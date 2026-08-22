# Security model

What an attacker is assumed to be able to do, where the boundaries are, what is checked and by what, and what is not defended against.

The decisions themselves are written where they were made, in the module that makes them, and this page does not restate them.
It draws the lines between them and links out, because a fact copied into a second place is a fact that stops being true in one of them.

## The assumption

The attacker can reach any port the deployment exposes and can send anything on it.
They can hold a valid anon key, because an anon key is public: it ships in browser bundles and there is no deployment where it is a secret.
They can be a signed in user of the project, holding a token this server issued, and they can be a user of a different project on the same node.
They can read and write the object store if they have its credential, and the whole of this design assumes that credential is the thing being protected, because it is.

What they are not assumed to be is a process on the node.
A local user who can read `/proc`, the runtime directory or the node's environment has the node's object store credential and every project's secret, and nothing here is a defence against that.

## The doors

A node opens several listeners and they are not equally safe to expose.
The ports and their flags are in [operations.md](operations.md); what matters here is which side of the boundary each one belongs on.

**The http front door is public.**
It is the only listener meant to face the internet.
Every request on it names a project, by host under a wildcard domain or by the first path segment, unless the node was started with `--ref` and serves one project at every url it answers.
Almost everything on it sits behind a gate wanting an `apikey`, which is a JWT signed with that project's secret.
The exceptions are the handful a browser or a mail client navigates to with no headers at all, the JWKS document, a followed confirmation link, and both halves of a social sign in, and they are outside the gate here because they are outside it on the hosted gateway too.
Each of them carries its own proof instead: a one time token, a signed state, or nothing to prove because the document is public.

**The postgres port and the transaction pooler are private unless a certificate was given.**
With `--pg-tls-cert` they take `sslmode=require` and above and nothing else, and a startup packet sent in the clear is refused rather than read, because the packet after it carries the project key.
Without one, an `SSLRequest` is declined and the key crosses in the clear, so the port belongs on a private network or behind a terminator.
There is no channel binding either way, which matters when the tenant database is across a network the node does not own.

**The ops port is an admin port and has no authentication at all.**
`zou dev --ops` binds it on loopback.
`zou serve --ops` binds it on every interface, so on a node it is a firewall's job and not the server's.
It is a separate listener rather than two routes on the api port because a scrape is the operational state of a node and is not something to hand to whoever holds an anon key for one of the projects on it.

**The functions inspector is loopback and nothing else.**
A debugger session evaluates arbitrary javascript inside an isolate holding the project's secrets, so it is a shell on the process rather than a view of it, and [functions.md](functions.md) says so where the flag is documented.

The last door is not a listener at all.
It is the object store, and it is dealt with below.

## What is verified, and where

**An `apikey` is verified before anything is attached.**
The project's JWT secret lives in its registry entry, one object per tenant, which is a point GET the router already has to make.
That is deliberate, and `crates/zou-store/src/registry.rs` says why: if the secret lived in the project's own database, finding out that a request was signed with the wrong key would first require taking a lease, hydrating a manifest and starting a postmaster, and an unauthenticated request would be a lever for making a node do all of that.

**The algorithm branch is taken from the header exactly once and each branch only accepts its own key material.**
That is what kills `alg: none` and the confusion attack where an ES256 public key is replayed as an HMAC secret, and it is in `crates/zou-server/src/jwt.rs` with both key formats Supabase runs today.
Apikeys stay HS256 only, because the legacy key format is the only JWT shaped apikey Supabase has ever issued.

**On the postgres ports the password is the project key.**
The `role` claim in it has to be the role the connection asked for, so a key for anon cannot open a service_role session.
The check happens before the attach, for the same reason the http one does, and the tenant's own postgres never sees the key: the connection this node then opens carries the dsn credential, which belongs to the node and is not something a client should hold.

**On the S3 protocol there is no token to check at all.**
The authorization header carries a signature over the request, computed from a key pair the project was configured with, and a signature that verifies is the project's own key pair, which is the whole project.
So those statements run as the service role, for the same reason the reference runs them with its service key: an S3 client has no user, no session and nothing for a policy to be about.
Two things that surface does not check are recorded in `crates/zou-server/src/s3.rs` and are named again below, because they are behaviour kept on purpose rather than gaps nobody noticed.

## Roles, and where the rules are actually enforced

There are four roles and they are the ones a Supabase project already has.
`anon` and `authenticated` are what an application carries, `service_role` bypasses row level security and is the whole project, and `postgres` is the cluster superuser a project's own migrations run as.
A key for each is minted from the project's secret the same way.

Nothing above the database decides who may read a row.
Every REST and auth request runs in a transaction that begins with `set_config('role', ..., true)` and the verified claims in `request.jwt.claims`, all of it local to that transaction, and then the query runs as that role with the project's policies deciding what it returns.
Storage is the same story against `storage.objects`, and a signed url is the one shape that has already said everything it is going to say before postgres is asked.

Private realtime channels are the sharpest case, and `crates/zou-server/src/policy.rs` is worth reading in full.
There is no permission model for them, on purpose: the rules are ordinary row level security policies on `realtime.messages`, so the way to find out whether somebody may read a room is to become them and ask postgres to try, in a transaction that is always rolled back.
A model of our own would answer differently from upstream for policies written to be read by upstream, which is the failure worth avoiding.

## What is encrypted, and what is not

The store is trusted to be durable and not trusted to be private.
That asymmetry is the whole of the encryption story here.

One thing is sealed: a project's function secrets, at `tenants/<ref>/functions/SECRETS`, because a bucket is a thing that can be copied without the copy being noticed and secrets written in the clear next to the data they unlock would make that copy worth having.
The cipher is ChaCha20-Poly1305, so a fleet has no security property that depends on whether the box has AES-NI.
The root key encrypts nothing itself: every project gets its own, derived as `HMAC-SHA256(root, "zou/functions/secrets/1/<ref>")`, and the same label goes in as the associated data, so a ciphertext lifted out of one project's prefix does not open in another's even if somebody rewrites the key it sits under.
The root key arrives as `ZOU_SECRET_KEY` or `ZOU_SECRET_KEY_FILE` and there is deliberately no flag for it, because an argument is in `ps` output and in the shell history of whoever ran it.
`crates/zou/src/secrets.rs` is the source for all of that.

Nothing else is.
The pages, the WAL, the checkpoint runs, the user files and the registry entry holding the project's JWT secret and its S3 key pair are written as they are.
The registry module states the consequence plainly: anyone who can read that object can already read the database it belongs to.

## One node, many projects

A node keeps projects apart with postgres and with the layout, not with a kernel sandbox.

Each attached project gets its own postmaster, its own runtime directory restored from its own prefix, its own port on loopback and its own socket directory, and a request is routed to one of them by the ref in the host or the path.
A project's data is a prefix and the prefix is self contained, which is what makes a branch and a copy cheap and also what bounds the blast radius of a mistake in the layout.

What is shared is the process and the machine.
The postmasters are children of one node process running as one operating system user, so a hole that yields code execution inside one project's postgres is a hole in every project on that node.
Postgres superuser is the same statement: `postgres` is a project owner's credential and it is the superuser of that cluster, so it is not a role an application should carry and not a boundary a fleet should lean on.

Function isolates are the other shared surface, and they are the one place someone else's code runs by design.
A function may read the files its project listed in `static_files` and nothing else, symlinks are not followed, and a name is tidied lexically first so it cannot walk out and back in again.
It may open tcp and not unix sockets, because a unix socket is a file on the machine rather than somewhere on the network.
`crates/zou-functions/src/statics.rs` draws that line and says why: a function is somebody else's javascript in a process that holds a database superuser connection and a JWT secret.

## What is deliberately not defended against

None of these is a bug and all of them are things to know before a customer's data goes behind it.

**The database's own bytes are not encrypted at rest.**
If the object store is readable by somebody it should not be, the project is readable, including the registry entry that holds its secret and its S3 key pair.
Encryption at rest is the store's own, whatever the bucket is configured with.

**The object store credential is the fleet.**
A node holds one and it reaches every project on that store.
There is no per project credential for the node's own access to the store.

**A node holds the dsn credential for every postgres it started.**
That is what keeps the credential away from clients, and it means the node is the thing worth attacking.

**A service role key is the whole project.**
It bypasses row level security by definition, upstream too, and a function that uses it is deciding who may do what by itself.
`SUPABASE_SERVICE_ROLE_KEY` is in every function's environment for compatibility, so a project that deploys a function is trusting that function with its data.

**The S3 signature has no date window and the body is not rehashed.**
A signature correctly computed for a date in the past is accepted today, and `x-amz-content-sha256` is taken as the payload hash rather than checked against the bytes.
Both are what the reference does, and a client that works against Supabase has to work here; anything stricter would show up as somebody's upload failing against zou and nowhere else.

**Outbound calls go where the project says.**
A database webhook, a `net.http_*` call and a cron job all make the node fetch a url the project chose, and there is no allowlist and no refusal of an address on the node's own network.
Writing one of those needs SQL on the project, so this is the project owner's own reach rather than a stranger's, but on a shared node it is the node's network position being lent out.

**There is no rate limit on the front door itself.**
The auth surface has upstream's own per endpoint limits, and they are keyed on an address the platform tells the server about rather than on the socket, so a deployment that sets no forwarding header does not limit anybody.
That is upstream's behaviour and `crates/zou-server/src/limit.rs` says it in those words.
Nothing there and nothing elsewhere is a defence against somebody holding a valid anon key and deciding to be expensive, and a public deployment wants something in front of it.

**The ops port is unauthenticated and `zou serve` binds it on every interface.**

**There is no channel binding on the postgres ports**, and without a certificate there is no TLS on them at all.

## Reporting a hole

[SECURITY.md](../SECURITY.md) at the root of the repository says where to send one, what will be treated as a vulnerability, and what happens after.
