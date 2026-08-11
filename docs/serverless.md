# Serverless

How to run one project on something that starts a process when a request arrives and stops it when nobody asks.

Everything here is one project per deployment.
A node that serves a thousand of them is the other shape, and it is the same binary: see [operations.md](operations.md).

## One project, no routing

`zou serve <store> --ref demo` serves the project `demo` at every url the node answers.

A fleet node reads the project out of the request, from a hostname like `demo.zou.example.com` or from the first path segment, `/demo/rest/v1/`.
Neither of those exists on a function url or a Cloud Run url: the hostname is assigned by the platform and there is nothing left in it to name a project with, and a path prefix would mean every client of a serverless deployment writes urls no other deployment uses.
So `--ref` takes the routing out rather than configuring it: the project is decided before the process starts, nothing is resolved per request, and `/rest/v1/` is `/rest/v1/` exactly as a hosted Supabase project spells it.
`ZOU_REF` sets the same thing, and `ZOU_TARGET` sets the store, because a container is configured with variables and an image that has to have its command rewritten to point at another bucket is an image per bucket.

The project comes up before the door opens.
A node with a port binds the listener first and attaches while the socket is already accepting, so a request that arrives during the attach waits in the accept queue instead of being refused.
A function has no listener, and Lambda gives an initialisation window before the first event, which is where the attach goes.
Either way the first request pays for nothing it did not ask for, and the log says so:

```
demo is up before the first request, in 118.2 ms
```

## One writer

A project has one writer at a time, held as a lease in its manifest with a 15 second TTL, see [operations.md](operations.md).
This is the whole of what a serverless deployment has to get right.

Two instances of the same project are safe and pointless: the second one attaches, finds the lease held, and its wal pusher waits for it rather than serving writes.
So cap the deployment at one instance.
On Cloud Run that is `maxScale: 1`, on Fly it is one machine, and on Lambda it is reserved concurrency of 1.

A deployment that ends cleanly puts the lease back.
`zou serve` and `zou lambda` both stop their postmaster on SIGINT and SIGTERM, and stopping clears the lease from the manifest, so the next start attaches immediately.
A deployment that is killed outright does not, and the next start's first write waits out the rest of the 15 seconds while reads answer normally.
That is the difference between a stop and a kill, and it is worth setting up the platform so it sends a signal.

## Freeze and thaw

A third thing can happen to a serverless process, and it is neither a stop nor a kill.
Lambda between invocations and a suspended Fly machine are both a freeze: every thread stops where it stands, the heartbeat included, and the PUTs the wal pusher had in flight are neither sent nor cancelled.
Minutes later the machine thaws with no idea any time passed, and lets all of them go at once.

Nothing it does then can cost an acked write.
The lease it was holding expired while it was frozen, so a successor took the store, sealed the chain at the head it found, and swept what was above.
Every one of those late PUTs lands at a seq that is either taken, in which case it loses the creation race, or free and past the successor's own writes, in which case it is litter the successor clears when it gets there.
A reader crossing that litter ends the chain rather than choking on it, and a break below the last seal is still an error, because that is corruption rather than a thaw.

What the thawed process itself does is give up.
Its next append fails, its pusher restarts, and it finds the lease held by a machine that is renewing it.
After `ZOU_LEASE_WAIT_SECS`, 60 seconds by default and four times the TTL, it stops the cluster instead of waiting on.
The alternative is worse than it sounds: a postmaster that answers on its port and hangs every commit forever, because a backend past its flush holds interrupts and cannot even be cancelled.

[scripts/zou-freeze-thaw.sh](../scripts/zou-freeze-thaw.sh) is the drill: SIGSTOP a cluster under load, hand the store to a successor, SIGCONT the first one, and check that every acked commit is in a third cluster restored from nothing but the objects.

## Lambda

`zou lambda <store> --ref demo` is a custom runtime: a loop over `GET /2018-06-01/runtime/invocation/next`, the project's own router, and `POST /2018-06-01/runtime/invocation/<id>/response`.
There is no port and no listener anywhere in it.
An initialisation that fails posts to `/2018-06-01/runtime/init/error` and stops, which is Lambda's signal to destroy the environment rather than send it work.

Both API Gateway payload formats are read, 2.0 and 1.0, along with function url events, which are 2.0.
They disagree about where the method lives, how the query string is spelled, whether repeated headers are a list, and whether cookies are separate from headers, and the adapter translates all four differences in each direction.
An answer carries `statusCode`, `headers`, `multiValueHeaders` for anything repeated, `cookies`, and `body`, base64 encoded only when the body is not text.

The image is the server image with a different entrypoint, see [deploy/lambda/Dockerfile](../deploy/lambda/Dockerfile):

```bash
docker build -t zou .
docker build -f deploy/lambda/Dockerfile -t zou-fn .
docker tag zou-fn <account>.dkr.ecr.<region>.amazonaws.com/zou-fn:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/zou-fn:latest

aws lambda create-function \
  --function-name zou-demo \
  --package-type Image \
  --code ImageUri=<account>.dkr.ecr.<region>.amazonaws.com/zou-fn:latest \
  --role arn:aws:iam::<account>:role/zou-demo \
  --architectures arm64 \
  --memory-size 2048 \
  --timeout 30 \
  --environment 'Variables={ZOU_TARGET=s3://my-bucket/projects,ZOU_REF=demo}'

aws lambda put-function-concurrency \
  --function-name zou-demo --reserved-concurrent-executions 1

aws lambda create-function-url-config \
  --function-name zou-demo --auth-type NONE
```

Build the image for the architecture the function runs on.
The role needs `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:ListBucket` on the prefix, and nothing else.
Its credentials arrive in the environment as a key, a secret and a session token, and zou signs with the token when it is there, which is what makes a role work at all.
`--auth-type NONE` is the right answer for a supabase-js app: the anon key is the gate, exactly as it is in front of a hosted project, and IAM auth would mean signing every browser request.

Make the project before the first function runs.
The first attach of a project that has never run is an initdb and a genesis capture, around 30 seconds, and a function is not where that should happen:

```bash
zou tenant s3://my-bucket/projects create demo
zou serve s3://my-bucket/projects --ref demo --http 54321   # once, then stop it
```

That second line is worth doing even though the function would do it itself.
It also installs the auth schema, which the first request that needs it does, once in a project's life, and it takes about three seconds.

Lambda sends SIGTERM before destroying an environment only when the function has an extension registered, so a function with no extension is always killed.
The cost of that is the paragraph above: reads are unaffected, and the first write in the next environment waits out what is left of the 15 second lease.

### What it costs

`scripts/zou-lambda-smoke.sh` runs the whole adapter against a fake runtime api on a laptop: it hands out function url events, the real binary answers them, and every answer is checked.
Two environments, because the interesting one is the second, which is what every cold start after a project's first does.
On an M-series laptop against a store on the local disk:

| | |
| --- | --- |
| exec to the runtime api loop | 0.3 ms |
| attach, before the first event | 25 to 57 ms |
| a health check | 0.7 ms |
| the first request to the rest surface | 97 ms |
| a signup, the first write of a project's life | 3.5 s |
| the same signup again | 55.8 ms |

The rest surface's first request builds the schema cache, and the first signup installs the auth schema, and both are once per project rather than once per environment.
A project that was warmed once, as above, starts answering in tens of milliseconds.

The cold attach against object storage is the number that matters more and it is measured separately, with the wire latency of S3 simulated, in [benchmarks.md](benchmarks.md).

## Cloud Run

Cloud Run is a container on a port, so this is `zou serve` with `--ref` and nothing else unusual, see [deploy/cloudrun/service.yaml](../deploy/cloudrun/service.yaml).

```bash
gcloud run services replace deploy/cloudrun/service.yaml --region europe-west1
```

Two settings in that file are load bearing.
`maxScale: 1` is the one writer rule.
`run.googleapis.com/cpu-throttling: "false"` is because the wal pusher and the lease heartbeat are threads rather than request handlers: an instance throttled to no cpu between requests stops renewing a lease it still holds, and then loses it to nobody.
Cloud Run stops an idle instance with SIGTERM, which zou answers by shutting the postmaster down and clearing the lease.

The store is `gs://bucket/prefix`, signed as S3 against `storage.googleapis.com` with an HMAC key for the service account rather than with the service account's own credentials:

```bash
gcloud storage hmac create zou-demo@<project>.iam.gserviceaccount.com
```

## Fly

Fly runs the server image directly, see [deploy/fly/fly.toml](../deploy/fly/fly.toml):

```bash
fly launch --no-deploy --copy-config --config deploy/fly/fly.toml
fly storage create        # a Tigris bucket, and the AWS variables for it
fly deploy
```

One machine, for the one writer rule, and `auto_stop_machines = "stop"` rather than `"suspend"`.
A stop is a signal and zou puts the lease back on the way out, so the next start has the store to itself immediately.
A suspend freezes the machine mid lease, heartbeat included, and the thaw is safe but not free: the lease expired while the machine was down, another one may have taken the store, and a machine that comes back to that has been replaced.
It works out what happened, refuses to write, and stops itself, which costs a start rather than the data.
A suspend is still the wrong setting for a single machine, because the thing it saves is the start a stop pays and the thing it risks is that stop happening anyway.

The postgres port and the pooler are off in that config because an http app does not need them.
Add a `[[services]]` block on 5432 to get psql and a connection string, and remember that the same one writer rule covers it.

## What is not here yet

A Terraform example that stands the whole thing up, and a cost readout against a real bill, are the next two lines of [issue #3](https://github.com/tamnd/zou/issues/3).
