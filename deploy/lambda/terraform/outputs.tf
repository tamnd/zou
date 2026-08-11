output "api_url" {
  description = "What a supabase-js client is pointed at, with no path prefix under it: /rest/v1/, /auth/v1/ and /storage/v1/ are where a hosted project puts them."
  value       = aws_apigatewayv2_api.api.api_endpoint
}

output "repository_url" {
  description = "Where to push the function image."
  value       = aws_ecr_repository.zou.repository_url
}

output "store" {
  description = "The store, in the form every zou command takes."
  value       = "s3://${var.bucket}/${var.prefix}"
}

output "function_name" {
  value = aws_lambda_function.fn.function_name
}

output "log_group" {
  description = "Where the function says what it did. `aws logs tail <group> --follow` is the whole of watching it."
  value       = aws_cloudwatch_log_group.fn.name
}
