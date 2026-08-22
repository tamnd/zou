# Reporting a vulnerability

Report it privately, at [github.com/tamnd/zou/security/advisories/new](https://github.com/tamnd/zou/security/advisories/new).
That is a private draft advisory on this repository and it is the preferred address, because it keeps the report, the fix and the eventual publication in one place.

Please do not open a public issue for something that would let somebody read or write data they should not.
Everything else, including a hardening idea that is not exploitable, is welcome as an ordinary issue with the `security` label.

## What to send

What version or commit, what the deployment looked like, and the smallest sequence that shows it.
A reproduction against `zou dev` or `make demo` is worth more than a description, since both are a few seconds to start.
If it needs a particular configuration, say which, because a hole that only exists with the ops port on the internet is a documentation bug and one that exists with the defaults is not.

## What happens next

An acknowledgement within three working days, and an assessment of whether it is a vulnerability and how bad within seven.
After that, a fix on a branch, a note on the draft advisory as it goes, and a release.
The advisory is published when the fix is released, with credit to the reporter unless they would rather not be named.

There is no bounty.
This is an Apache 2.0 project with no company behind it and it would be dishonest to imply otherwise.

## What counts

Anything that lets somebody reach data or an action that the [security model](docs/security.md) says they should not.
Some examples of what that means here:

- an `apikey` or a project key being accepted when it should not be, or a token being verified with the wrong key material
- reading or writing rows that row level security should have stopped, including through the REST surface, the storage surface, a realtime channel or the postgres port
- reaching another project on the same node, in any direction
- an S3 signature being accepted for a request it was not computed over
- a function reading a file outside its own `static_files`, or reaching the host in a way the runtime is supposed to refuse
- a project's function secrets being readable without the node's root key
- a crash or a hang reachable by an unauthenticated request to the http front door

## What is out of scope

Everything in the "what is deliberately not defended against" section of the [security model](docs/security.md) is known and written down, so a report that restates one of those is not a vulnerability.
The short version:

- the object store credential reaching every project on the store, and the database's own bytes not being encrypted at rest
- a service role key being the whole project, which is upstream's design too
- the ops port being unauthenticated, and `zou serve` binding it on every interface
- the postgres ports having no TLS when no certificate was given
- the S3 surface not checking the date and not rehashing the body, both of which match the reference on purpose
- outbound calls from webhooks, `net.http_*` and cron going wherever the project's own SQL says
- anything that needs a local account on the node, which already has the node's credentials
- load, in the sense of somebody with a valid key making expensive requests

If you think one of those is worse than the page makes it sound, say so.
That is a useful report and it will be treated as one, even though it is not an advisory.
