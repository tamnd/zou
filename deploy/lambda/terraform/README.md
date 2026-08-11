# One project on Lambda, from nothing

An http api, a function, and a bucket that is the whole database.
Everything a supabase-js app talks to is in these four files, and the parts that are not are the two things Terraform cannot do for you: build the image, and make the project.

Terraform or OpenTofu, either one.
`tofu` below is `terraform` if that is what you have.

## 1. The repository first

The function is a container image, so the repository has to exist and have an image in it before the function can be made.
That is one targeted apply rather than a second config:

```bash
tofu init
tofu apply -target=aws_ecr_repository.zou -var bucket=my-zou-bucket
```

## 2. Build and push

The architecture has to match `var.architecture`, which is `arm64` by default because it is cheaper per GB second and zou is the same speed on it:

```bash
REPO=$(tofu output -raw repository_url)
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO%%/*}"

docker build --platform linux/arm64 -t zou .
docker build --platform linux/arm64 -f deploy/lambda/Dockerfile -t "$REPO:latest" .
docker push "$REPO:latest"
```

Those two builds are from the root of this repository, not from this directory.

## 3. The rest of it

```bash
tofu apply -var bucket=my-zou-bucket
```

That makes the bucket, the role, the log group, the function with a reserved concurrency of one, and an http api whose `$default` route is the function.

## 4. Make the project, once, from here

A function is the wrong place for an initdb.
The first attach of a project that has never run makes a cluster and captures it, which is around thirty seconds, and everything after it is an attach of tens of milliseconds.
So do it once from a laptop, against the same bucket:

```bash
STORE=$(tofu output -raw store)
zou tenant "$STORE" create demo
zou serve "$STORE" --ref demo --http 54321    # wait for it to say it is up, then stop it
```

That second line also installs the auth schema and builds the rest surface's schema cache, which are once in a project's life rather than once per environment.
It needs AWS credentials that can write the prefix, the same ones you ran Terraform with.

## 5. Point an app at it

```bash
tofu output -raw api_url
zou tenant "$STORE" keys demo --env
```

The url has no path prefix under it, so `/rest/v1/` and `/auth/v1/` are where a hosted project puts them, and a supabase-js client takes the url and the anon key with nothing else changed:

```js
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY)
const { data } = await supabase.from('notes').select()
```

The demo app the conformance suite drives is a supabase-js app of exactly that shape, see [zou-conformance](https://github.com/tamnd/zou-conformance), and pointing it here rather than at a local server is those two variables.

## What it costs

`scripts/zou-lambda-cost.sh` measures a project's store ops and invocation times on a laptop and prices them at the published rates, so a bill can be worked out before there is one.
An app doing 100,000 reads, 10,000 writes and 96 cold starts a day comes out at $11.69 a month, of which the gateway is $3.35 and the S3 puts are $6.17, see [docs/serverless.md](../../../docs/serverless.md).
For a deployment that is already up it prints the `aws cloudwatch` and `aws s3 ls` commands that produce the real numbers, and takes them back in with `--invocations-per-day` and friends.

## What one instance means

`reserved_concurrent_executions = 1` is not a cost control.
A project has one writer, held as a lease in its manifest, and a second environment that attaches finds the lease held and stops itself rather than serve writes it cannot land.
One instance also means one request at a time, and the answer for anything busier than an app with a handful of users is `zou serve` on a node with a port, see [docs/operations.md](../../../docs/operations.md).

## Tearing it down

`tofu destroy` takes the function, the api, the role and the repository.
It leaves the bucket if anything is in it, which is every object of the database, so emptying it is a decision you make on purpose:

```bash
aws s3 rm "s3://my-zou-bucket/projects" --recursive
tofu destroy -var bucket=my-zou-bucket
```
