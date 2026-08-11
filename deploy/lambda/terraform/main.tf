# One project on Lambda: a bucket that is the database, a function that
# is postgres and the api, and an http api in front of them.
#
# Nothing here is a database service. The bucket below holds every page
# and every wal byte, and the function is the only thing that reads or
# writes it, which is why the role can reach one prefix and nothing else
# and why there is no security group, no subnet and no vpc anywhere in
# this file.
#
# See README.md next to this for the order to run it in, and
# docs/serverless.md for what the one writer rule means for a fleet of
# functions.

# The store. No versioning on purpose: everything zou writes under wal/
# and chk/ is immutable and named by content or by sequence, so a
# version of a key that is never overwritten is a copy of the same
# bytes, and the objects that do get overwritten are the manifest and
# the page layer, which have their own history in the chain. Versioning
# a zou bucket doubles the bill and protects nothing the chain does not.
resource "aws_s3_bucket" "store" {
  bucket = var.bucket
}

resource "aws_s3_bucket_public_access_block" "store" {
  bucket                  = aws_s3_bucket.store.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The image. It is built and pushed before the function can be made,
# which is why the README applies this resource on its own first.
resource "aws_ecr_repository" "zou" {
  name                 = var.name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "fn" {
  name               = "${var.name}-fn"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

# Four actions on one prefix, and the list is scoped by condition
# rather than by resource because ListBucket is granted on the bucket
# itself. A role that can read the whole bucket can read every other
# project in it, and a deployment serves one.
data "aws_iam_policy_document" "store" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["${aws_s3_bucket.store.arn}/${var.prefix}/*"]
  }

  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.store.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.prefix}/*"]
    }
  }
}

resource "aws_iam_role_policy" "store" {
  name   = "${var.name}-store"
  role   = aws_iam_role.fn.id
  policy = data.aws_iam_policy_document.store.json
}

resource "aws_iam_role_policy_attachment" "logs" {
  role       = aws_iam_role.fn.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Made here rather than left to the first invocation, because a log
# group Lambda creates has no retention and keeps everything forever.
resource "aws_cloudwatch_log_group" "fn" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "fn" {
  function_name = var.name
  role          = aws_iam_role.fn.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.zou.repository_url}:${var.image_tag}"
  architectures = [var.architecture]
  memory_size   = var.memory_mb
  timeout       = var.timeout_seconds

  # One project has one writer. A second environment attaches, finds
  # the writer lease held by the first, and stops itself rather than
  # serve writes it cannot land, so this is not a tuning knob: without
  # it a burst of traffic makes environments that shut down.
  #
  # It also means one request at a time. That is the shape of a
  # database in a function, and the answer for anything busier is a
  # node with a port rather than a bigger reservation here.
  reserved_concurrent_executions = 1

  environment {
    variables = {
      ZOU_TARGET = "s3://${var.bucket}/${var.prefix}"
      ZOU_REF    = var.ref
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.fn,
    aws_iam_role_policy.store,
    aws_iam_role_policy_attachment.logs,
  ]
}

# An http api rather than a rest api: the payload is 2.0, which the
# adapter reads, and it bills at a fifth of what a rest api does for
# the same request.
resource "aws_apigatewayv2_api" "api" {
  name          = var.name
  protocol_type = "HTTP"

  # No cors_configuration block on purpose. zou answers preflights
  # itself, with the origins the project allows, and an api that
  # answered OPTIONS on its behalf would be a second policy in front of
  # the first with no way to agree with it.
}

resource "aws_apigatewayv2_integration" "fn" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.fn.invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = var.timeout_seconds * 1000
}

# Every path, because the project's own router is what decides them:
# /rest/v1/, /auth/v1/, /storage/v1/ and the health check are one
# surface and a gateway that knew about them would be a copy of the
# router that has to be kept in step with it.
resource "aws_apigatewayv2_route" "all" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.fn.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "api" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fn.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}
