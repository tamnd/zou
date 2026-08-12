# Quickstart

From clone to a Postgres whose every page and WAL byte lives on a store, in about ten minutes, most of them spent compiling Postgres once.

## Prerequisites

A recent stable Rust toolchain for everything, and for the full tour the usual Postgres build tools: meson, ninja, a C compiler, flex, bison, and the readline, zlib, and icu libraries.

- Debian and Ubuntu: `apt-get install meson ninja-build flex bison libreadline-dev zlib1g-dev libicu-dev liblz4-dev libzstd-dev libssl-dev libxml2-dev libxslt1-dev pkg-config uuid-dev`
- macOS: `brew install meson ninja icu4c pkg-config`

## Act one, the object layer

```bash
git clone https://github.com/tamnd/zou && cd zou
make demo
```

That runs in seconds and needs nothing but Rust. It plays the storage engine on a local directory: the genesis manifest, the writer lease with epoch fencing, group committed WAL, sealed segments, and the manifest tail, printing each object as it lands. The directory is kept so you can look around.

Without the vendored Postgres built yet the demo stops there and tells you so.

## Act two, the real Postgres

```bash
make pg-build   # once, this is the slow part
make demo       # now both acts play
```

`make pg-build` fetches the pinned Postgres submodule, applies the zou patch series, and builds it with the storage manager shim linked in, see docs/postgres.md. With that in place `make demo` continues past act one: it starts `zou dev` on a fresh store, writes rows through plain `psql`, writes and checkpoints until the fold packs a page capture down, stops the server, prints what the store holds with `zou info`, takes a branch with `zou branch` which costs one small manifest and copies no data, and then restarts Postgres from nothing but the store and reads the rows back.

## Your own targets

`zou dev <target>` accepts more than a directory:

```bash
zou dev /tmp/mydb                      # a directory of objects
zou dev s3://bucket/prefix             # any S3 compatible endpoint, see below
zou dev sqlite:///tmp/mydb.db          # the whole store in one SQLite database
```

S3 style targets read `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the environment, plus `ZOU_S3_ENDPOINT` for a non AWS endpoint like a local MinIO and `ZOU_S3_REGION` when it matters. `gs://bucket/prefix` speaks the GCS dialect with HMAC interop keys.

The cluster superuser is `postgres`, whatever the account running the node is called, because that is the role a project's own migrations run as on a hosted Supabase project and the owner of a database should not depend on which account started it.
So connect by naming it, `psql -h 127.0.0.1 -p 5432 -U postgres -d postgres`, and a client that leaves the user out is told there is no role by that name.

## A project that already has a config.toml

A Supabase project keeps its ports, its auth switches and its provider credentials in `supabase/config.toml`, and `zou dev` reads that file rather than asking for the same settings a second time.
Run it from anywhere inside the project and it finds the file by walking up, takes `[api] port` as the http port and `[db] port` as the postgres port, serves the schemas `[api] schemas` names in that order, and turns the rest into the environment variables this binary already reads.

```bash
zou dev ./store                        # ports and settings from supabase/config.toml
zou dev ./store --config other/config.toml
zou dev ./store --no-config            # read nothing, the flags and the defaults only
```

A flag beats the file and anything already in the environment beats it too, so pinning `ZOU_MAILER_AUTOCONFIRM` for one run does not mean editing the project's file.
Nothing is served over http unless the file or a flag asks for a port, which keeps `zou dev` on its own a postmaster and nothing else.

`zou status` prints what a client should be pointed at, in the shape `supabase status` prints it, and exits non zero when nothing is listening, so a script can wait on it.

```bash
$ zou status
         API URL: http://127.0.0.1:54321
  S3 Storage URL: http://127.0.0.1:54321/storage/v1/s3
          DB URL: postgresql://postgres:postgres@127.0.0.1:54322/postgres
      JWT secret: ...
        anon key: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
service_role key: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
   S3 Access Key: 625729a08b95bf1b7ff351a663f3a23c
   S3 Secret Key: 850181e4652dd023b7a98c58ae0d2d34bd487ee0cc3254aed6eda37307425907
       S3 Region: local
          config: /home/me/app/supabase/config.toml
    not read yet: api.max_rows, db.shadow_port, studio.port
$ eval "$(zou status -o env)"          # API_URL, DB_URL, ANON_KEY, SERVICE_ROLE_KEY, S3_PROTOCOL_ACCESS_KEY_ID
$ zou status -o json                   # the same, for a tool that parses it
```

The keys are minted from `ZOU_JWT_SECRET`, so pin one, otherwise `zou dev` generates a secret it logs and no other process can know what it signed.
The S3 pair is the fixed one a local Supabase project answers to, so an S3 client that already has it in a `.env` keeps working, and `ZOU_S3_ACCESS_KEY`, `ZOU_S3_SECRET_KEY` and `ZOU_S3_REGION` replace it for a dev loop that is reachable from further away than loopback.
A project whose file says `[storage.s3_protocol] enabled = false` gets no pair at all, and then the endpoint answers every signature with the access key not being one this project has.
The last line is the honest part: every setting in the file that zou has no answer for yet is named rather than ignored in silence.

A path ending `.zou` is the single file backend. Every sequential tool works over it today, `zou info`, `zou branch`, `zou-bootstrap`, `zou-restore`, while `zou dev` needs the multi process postmaster and waits on the in process engine, see the note in docs/storage-engine.md.

## The project's own migrations

The schema lives in `supabase/migrations` as `<timestamp>_<name>.sql`, and what has been applied is recorded in `supabase_migrations.schema_migrations`, which is the table the Supabase CLI reads, so a project can go back and forth between the two while it is deciding.

```bash
zou migration new "add users table"    # supabase/migrations/20240102030405_add_users_table.sql
zou db push --dry-run                  # what would be applied, in name order
zou db push                            # apply it, and write the ledger
zou db reset                           # drop the project's schemas, replay everything, seed
zou db diff                            # what the database has that no migration accounts for
zou db diff -f "studio changes"        # the same, written as the next migration
```

The database is the one `[db] port` names, or `--db-url`, or `ZOU_DB_URL`, in that order.
Each file is applied as one transaction with its ledger row written inside it, so a file that fails half way leaves neither its tables nor its version behind, and the next `zou db push` starts again from the same place.

A reset drops every schema the project owns and puts `public` back the way initdb leaves one, then replays the migrations and runs the seed that `[db.seed] sql_paths` names, `./seed.sql` by default.
It leaves the schemas this server runs on alone, `auth`, `storage`, `extensions` and `zou` among them, because here those belong to the running server rather than to a container somebody can throw away.
That is also why it refuses a database that is not on this machine unless you say `--force`.

`zou db diff` answers the other question, which is what somebody changed in Studio or in psql and never wrote down.
It starts a postgres of its own on a socket in a temporary directory, gives it the same bootstrap a zou database gets so that `auth.uid()` in a policy and a foreign key to `auth.users` both resolve, replays the migrations into it, and prints the statements that would turn that database into the real one.
Give it `-f <name>` and those statements become the next migration instead, `--schema <name>` to look at some schemas rather than all of them, and `--pg-bin <dir>` if the postgres binaries are not where `zou dev` finds them.
Read only on the real database, so there is no `--force` to think about.

It compares schemas, enums, tables and their columns, constraints, indexes, views, functions, triggers, row level security and its policies, table and schema grants, and comments.
It does not compare default privileges, column privileges, ownership, extensions, publications, event triggers, domains, composite types, standalone sequences or foreign tables, and it says so on every run rather than letting "no changes" mean "nothing I looked at".
Grants on a table it is already creating are left out too, because a new table arrives with whatever the default privileges give it.
A database nobody has made a request against yet does not have `anon`, `authenticated` or `service_role` in it, because the server creates those on its first request, so on that one the grants to those three roles are left alone and the run says so.

## A database per pull request

A branch is a new prefix holding manifests that point at the parent's objects, so one costs a couple of small writes whatever the database weighs, and `zou dev` serves any ref in the store rather than only the default one.

```bash
zou branch ./store create local pr-142   # the branch, at the parent's last published state
zou dev ./store --ref pr-142             # serve it, on its own ports if you like
zou branch ./store delete pr-142         # and take it back when the work lands
```

Writes on a branch stay on the branch, the parent never sees them, and `zou branch ./store list` prints what is out there and where each one came from.
A ref that has no database refuses rather than quietly bootstrapping an empty one under a name that was probably a typo, so `--ref` on a laptop means the branch you took and nothing else.

In CI the composite action in `actions/branch` does the same two calls off the pull request event, taking the branch when one opens and removing it when it closes.

```yaml
- uses: tamnd/zou/actions/branch@main
  with:
    target: s3://mybucket/tenants
```

It names the branch `pr-<number>` unless `ref:` says otherwise, `source:` is the ref it branches from and defaults to `local`, and `zou:` is the binary if it is not on `PATH`.
Every push to the pull request runs it again, and a branch that is already there is left alone, because the database from the first run is the one with the test data in it.
Give the workflow the `closed` type as well as the usual ones and the same step deletes the branch, or set `delete: create` and `delete: delete` to say which half runs where.

One thing to know before wiring it up: a branch can only read pages the parent has already folded into page runs, and the capture a database is bootstrapped with is not one of those.
A fold packs one down after a few checkpoints of writes, so a project that has been running for a while has one and a store somebody made this morning may not.
`zou branch create` checks before it returns and refuses a source the child could not read, rather than handing back a database that fails on its first query.
`ZOU_FOLD_DOWN_FACTOR=0` in the server's environment brings the fold forward, the second one packs a full instead of the fifth, which is how the branch smoke test gets one out of a database that has only just started.

## Mail on a laptop

`zou dev <target> --http 54321` starts the API front door next to the postmaster and logs the anon and service_role keys the way `supabase start` does, so a client is pointed at it by copying two lines.

Signups are confirmed on the spot by default, which is the same thing the Supabase CLI does locally. Set `ZOU_MAILER_AUTOCONFIRM=false` and the dev loop mails its confirmations instead. Nothing carries them anywhere: with no mail server configured, zou keeps what it sends in memory, logs the link, and serves the last hundred messages to the service role. `zou inbox` prints them.

```bash
ZOU_JWT_SECRET=$(openssl rand -hex 32) ZOU_MAILER_AUTOCONFIRM=false zou dev /tmp/mydb --http 54321
zou inbox                              # who it went to, the subject, the link
zou inbox --clear                      # start the next flow with an empty mailbox
```

`zou inbox` mints the service_role key from `ZOU_JWT_SECRET`, the same variable `zou dev` asks to be pinned, or takes `ZOU_SERVICE_KEY` when there is one to hand. It talks to 127.0.0.1 and nowhere else. Recovery, magic link, reauthentication and email change codes all arrive in the same place, so the whole of the email surface can be walked through without a mail catcher container or a second port.

## Mail that leaves the machine

Set `ZOU_SMTP_HOST` and the mail goes to a real server instead. The names are GoTrue's with `GOTRUE_` swapped for `ZOU_`, so a project migrating across brings its own values.

```bash
ZOU_SMTP_HOST=smtp.example.com \
ZOU_SMTP_PORT=587 \
ZOU_SMTP_USER=postmaster@example.com \
ZOU_SMTP_PASS=... \
ZOU_SMTP_ADMIN_EMAIL=noreply@example.com \
ZOU_SMTP_SENDER_NAME="My Project" \
  zou dev /tmp/mydb --http 54321
```

Port 465 is TLS from the first byte and everything else is plain TCP upgraded with STARTTLS, which is what the ports mean everywhere else. `ZOU_SMTP_SECURITY=starttls|tls|none` says so explicitly when a server disagrees with its port, and `none` is what a mail catcher on 127.0.0.1 needs.

Two rules here are stricter than GoTrue's. A server that offers no STARTTLS is refused rather than talked to in the clear, and the password is never sent unencrypted unless the server is on the loopback address. There is no knob for skipping certificate verification, because a transport that can be told not to check is one that ends up not checking.

Once something is carrying the mail there is nothing left in the process, so `zou inbox` has no messages to print. It still prints the texted codes when there is no SMS provider, and `/dev/inbox` is only gone once both media have somewhere to go.

## Signing in with a phone number

Phone sign in is off by default, the same as GoTrue, because a project that has not asked for it should refuse it by name rather than half serve it. Turn it on and the codes behave exactly like the mail does on a laptop: nothing carries them anywhere, they are kept in the process, and `zou inbox` prints them next to the number they went to.

```bash
ZOU_JWT_SECRET=$(openssl rand -hex 32) \
ZOU_EXTERNAL_PHONE_ENABLED=true \
  zou dev /tmp/mydb --http 54321
zou inbox                              # the number, and the six digits that went to it
```

That is enough for `supabase.auth.signInWithOtp({ phone })` and `supabase.auth.verifyOtp({ phone, token, type: 'sms' })` to work end to end with no account anywhere. A number nobody has signed up with is signed up by the first code that goes to it, which is how a project with no passwords at all registers people. `POST /auth/v1/otp` with `create_user: false` refuses a number nobody holds instead.

Set `ZOU_SMS_PROVIDER` and the codes go out for real. The credentials are GoTrue's names with `GOTRUE_` swapped for `ZOU_`, and half a set is refused at startup rather than at the first send.

```bash
ZOU_EXTERNAL_PHONE_ENABLED=true \
ZOU_SMS_PROVIDER=twilio \
ZOU_SMS_TWILIO_ACCOUNT_SID=AC... \
ZOU_SMS_TWILIO_AUTH_TOKEN=... \
ZOU_SMS_TWILIO_MESSAGE_SERVICE_SID=MG... \
  zou dev /tmp/mydb --http 54321
```

`messagebird` is the other one, on `ZOU_SMS_MESSAGEBIRD_ACCESS_KEY` and `ZOU_SMS_MESSAGEBIRD_ORIGINATOR`. WhatsApp is Twilio's alone, through `ZOU_SMS_TWILIO_CONTENT_SID`, and asking any other provider for the `whatsapp` channel is refused by name. `ZOU_SMS_AUTOCONFIRM=true` takes a number at its word the way `ZOU_MAILER_AUTOCONFIRM` takes an address, which is what a project wants while it is still being written.

A number is held in E.164 with the plus taken off, so somebody who typed `+1 555 010 0000` and somebody who typed `15550100000` are one account. Changing a number through `updateUser({ phone })` stages it and texts the new one, and the account keeps the old number until that code comes back with `type: 'phone_change'`.

## Signing in with Google, Github, or Apple

Give a provider a client id and a secret and it turns up in the flow. The names are GoTrue's again with `GOTRUE_EXTERNAL_` swapped for `ZOU_EXTERNAL_`, so the values a project already has keep working.

```bash
ZOU_JWT_SECRET=$(openssl rand -hex 32) \
ZOU_EXTERNAL_GOOGLE_CLIENT_ID=... \
ZOU_EXTERNAL_GOOGLE_SECRET=... \
ZOU_EXTERNAL_GITHUB_CLIENT_ID=... \
ZOU_EXTERNAL_GITHUB_SECRET=... \
  zou dev /tmp/mydb --http 54321
```

Half a credential is refused at startup rather than at the first sign in, because a provider that is configured wrong is worth hearing about before somebody clicks the button. Register `http://127.0.0.1:54321/auth/v1/callback` with the provider, or set `ZOU_EXTERNAL_GOOGLE_REDIRECT_URI` when it has to be something else, such as a proxy in front.

The client library needs nothing new. `supabase.auth.signInWithOAuth({ provider: 'google' })` sends the browser to `/auth/v1/authorize`, the provider sends it back to `/auth/v1/callback`, and the session arrives either in the fragment or, when the client sent a PKCE challenge, as a code that `POST /auth/v1/token?grant_type=pkce` trades for one. Both of those endpoints are outside the apikey gate, because a browser following a redirect has no way to carry a header.

An address a provider will not vouch for never links to an account that already holds it. Signing in at a provider with somebody else's address gets a new account with no address at all, which is what GoTrue does and the reason it does it.

Github does not have to be github.com. `ZOU_EXTERNAL_GITHUB_URL` points the whole flow at a GitHub Enterprise install, which serves the sign in pages at the host itself and the api under `/api/v3`, and zou works both addresses out from the one variable the way GoTrue does. It is the only provider that reads a url: Google and Apple are found through their issuer, so a url set for either of them is logged and ignored rather than used, which is also what upstream does.

```bash
ZOU_EXTERNAL_GITHUB_CLIENT_ID=... \
ZOU_EXTERNAL_GITHUB_SECRET=... \
ZOU_EXTERNAL_GITHUB_URL=https://git.example.com \
  zou dev /tmp/mydb --http 54321
```

Apple is the same three variables and one extra way to configure it. Its client secret is a JWT with a five minute life rather than a string, so a project can either mint one itself and put it in `ZOU_EXTERNAL_APPLE_SECRET` the way GoTrue takes it, or hand over the key and let zou mint one per exchange:

```bash
ZOU_EXTERNAL_APPLE_CLIENT_ID=com.example.service \
ZOU_EXTERNAL_APPLE_TEAM_ID=A1B2C3D4E5 \
ZOU_EXTERNAL_APPLE_KEY_ID=F6G7H8I9J0 \
ZOU_EXTERNAL_APPLE_PRIVATE_KEY="$(cat AuthKey_F6G7H8I9J0.p8)" \
  zou dev /tmp/mydb --http 54321
```

The three key variables go together or not at all, and the key is signed once at startup so a bad one is a refusal to start rather than a sign in that fails later. Apple also posts the callback as a form instead of redirecting to it, because zou asks for a name and an address, so `/auth/v1/callback` answers POST as well as GET. The name arrives once, on the very first sign in, and is kept.

## Linking a second provider to an account

Off by default, the same as GoTrue. Set `ZOU_SECURITY_MANUAL_LINKING_ENABLED=true` and somebody who is signed in can attach another provider to the account they already have, and detach one again.

```js
await supabase.auth.linkIdentity({ provider: 'github' })
await supabase.auth.unlinkIdentity(identity)
```

These are the only two places where what a provider says about an address does not decide anything. The person is already signed in to the account, so the identity joins it whatever address it names, and the account keeps the address it has. An account that has no address of its own, which is what an anonymous sign in leaves, takes one from the identity that joins it and then has to prove it like any other.

The last identity cannot be unlinked, because an account with none is one nobody can sign in to again.

## Signing in as nobody

Off by default, the same as GoTrue. Set `ZOU_EXTERNAL_ANONYMOUS_USERS_ENABLED=true` and a signup carrying neither an address nor a number gets a real account with a real session, and no way to sign in to it a second time.

```js
await supabase.auth.signInAnonymously()
```

The account has no address, no identity, and an empty `app_metadata`, and its token carries `is_anonymous`. Row level security policies can read that claim, which is the point: an anonymous visitor gets to write a cart or a draft without being asked who they are first.

The account becomes a permanent one by taking an address, either through `updateUser({ email })` or by linking a provider to it. The id does not change, so everything already written against it stays where it is. With `ZOU_MAILER_AUTOCONFIRM=true` the address is taken on the spot and the account stops being anonymous immediately; otherwise the ordinary email change mail goes out and the account stops being anonymous when the link is followed. Setting a password on an account that still has no address or number is refused, because there would be nothing to sign in with.

## A second factor

An authenticator app, on by default, the same as GoTrue. Enrolling draws a secret and hands back the QR code to point a phone at, and nothing has changed yet: the factor is unverified and the session is still `aal1`.

```js
const { data: factor } = await supabase.auth.mfa.enroll({
  factorType: 'totp',
  friendlyName: 'phone',
})
// factor.totp.qr_code is an svg, factor.totp.secret is the same secret in base32

const { data: challenge } = await supabase.auth.mfa.challenge({ factorId: factor.id })
await supabase.auth.mfa.verify({
  factorId: factor.id,
  challengeId: challenge.id,
  code: '123456',
})
```

The verify answers with a fresh token pair, and that pair is the point of the whole exchange. The access token says `aal2` and carries an `amr` array naming what the session has passed and when, most recent first, so a policy can read `auth.jwt()->>'aal'` and decide whether this session may see a table at all. The refresh token the session was holding is swapped for a new one, and every other session on the account is deleted, because a session that only ever typed a password should not still be sitting there once the account has a second factor.

The level lives on the session rather than in the token, so the token a session was carrying before it verified is `aal2` too. What that buys is a client that does not have to chase its own tokens around: the session is what was lifted.

A challenge is good for five minutes, can be spent once, and has to be verified from the address it was created from. A project may ask for less and will still get five minutes, which is the floor upstream applies. An account may hold ten factors, and adding a second one to an account that already has a working factor needs an `aal2` session, so somebody who learned the password cannot quietly add their own phone.

```js
await supabase.auth.mfa.getAuthenticatorAssuranceLevel() // { currentLevel, nextLevel }
await supabase.auth.mfa.listFactors()
await supabase.auth.mfa.unenroll({ factorId: factor.id })
```

Taking a verified factor away needs an `aal2` session as well, and it puts every session it had lifted back down to `aal1`. A half finished enrollment nobody ever proved can be taken away from where it stands, and one that has been sitting there five minutes is cleared out on its own the next time the account enrolls anything.

`ZOU_MFA_TOTP_VERIFY_ENABLED=false` is how a project turns MFA off without deleting anybody's factors: what is enrolled stops being usable, and the challenge is refused before it is written. `ZOU_MFA_TOTP_ENROLL_ENABLED=false` closes the other end. Phone and WebAuthn factors are not built, and are refused the way an unconfigured GoTrue refuses them.

## Asking the project what it offers

A sign in screen has to know which buttons to draw before anybody clicks one. `GET /auth/v1/settings` answers that, with no token beyond the anon key, and it is the same document GoTrue serves: every provider zou knows as a boolean under `external`, whether signups are open, whether an address or a number is taken at its word, and which provider carries the text messages.

```js
const settings = await fetch(`${url}/auth/v1/settings`, {
  headers: { apikey: anonKey },
}).then((r) => r.json())

settings.external.google      // true once a client id and a secret are set
settings.disable_signup       // false unless the project closed the door
settings.sms_provider         // "twilio", "messagebird", or ""
```

A provider nobody configured is `false` rather than missing, so a client can read the whole set without guarding every name.

`GET /auth/v1/health` names the service and its version, which is what a health check and a client version negotiation both read.

## Two shapes of refusal

Every refusal from `/auth/v1/` carries a machine readable code as well as a sentence, and there are two shapes of it because GoTrue has two. Which one comes back is decided by the `X-Supabase-Api-Version` request header, exactly as it is upstream.

```
# no header, or anything before 2024-01-01: the shape that was always there
{"code": 422, "error_code": "signup_disabled", "msg": "Signups not allowed for this instance"}

# X-Supabase-Api-Version: 2024-01-01, echoed back on the response
{"code": "signup_disabled", "message": "Signups not allowed for this instance"}
```

A header that is not a date is the older shape, and a date later than the newest one there is gets the newest one. The older shape also sends the code in an `x-sb-error-code` response header, and fills in `error_id` on a failure of the server's own so a report can be matched to a line in the log. The supabase-js clients send the header themselves, so most projects never see the older shape and no project has to choose.

## Closing the door

Two knobs decide who may make an account, and they are GoTrue's with `GOTRUE_` swapped for `ZOU_`.

`ZOU_DISABLE_SIGNUP=true` stops new accounts everywhere at once: a signup with a password, a magic link or a one time code to somebody nobody knows, an anonymous sign in, and a first sign in through Google or Github are all refused with `signup_disabled`. Signing in still works, so a project that has finished taking on people keeps serving the ones it has, and a provider sign in by somebody who already has an account still lands and still links the identity.

`ZOU_EXTERNAL_EMAIL_ENABLED=false` takes the address away as a way in, for a project that signs people in by phone or by provider alone. Signup, the password grant, magic link, recovery, a one time code by mail and a resend all refuse with `email_provider_disabled` and say which of the two it is: "Email signups are disabled" for the one that would make an account, "Email logins are disabled" for the ones that would not.

Both show up in `/auth/v1/settings`, which is how a client knows not to draw the form in the first place.

## Reading and throwing away a session

`GET /auth/v1/user` describes the account the token names, and refuses with `user_not_found` or `session_not_found` when the account or the session behind the token is gone. That is what makes a logout mean something: the token stays signed and inside its hour, but what it names is no longer there.

```js
await supabase.auth.signOut()                    // this session
await supabase.auth.signOut({ scope: 'global' }) // every session on the account
await supabase.auth.signOut({ scope: 'others' }) // every session except this one
```

`POST /auth/v1/logout` takes `global`, `local`, and `others`, defaults to `global` when the scope is missing, and answers 204 with no body. Any other scope is a 400 that names back what it was given. `POST /auth/v1/resend` sends a signup confirmation or an email change pair again, draws fresh codes rather than repeating the ones already mailed, waits out the same minute the first send started, and answers with an empty object whether or not there was anything to send, so it says nothing about who has an account here.

## Managing accounts from a server

`supabase.auth.admin` works against the same service_role key `zou dev` prints, and nothing else gets in. A request with no bearer token, with the anon key, or with an ordinary person's own access token is refused before it touches the database.

```js
const admin = createClient(url, serviceRoleKey).auth.admin

await admin.createUser({ email: 'ada@example.com', password: '...', email_confirm: true })
await admin.listUsers({ page: 1, perPage: 50 })
await admin.getUserById(id)
await admin.updateUserById(id, { ban_duration: '24h' })
await admin.deleteUser(id)
await admin.deleteUser(id, true)   // keep the row, take everything out of it
```

An account created this way needs no confirmation mail: `email_confirm` is the admin asserting the address, and the same is true of an address changed with `updateUserById`. With no password at all the account exists and cannot be signed in to until somebody sets one. `ban_duration` takes Go's duration spellings, `24h` or `1h30m`, and `none` lifts a ban.

The second argument to `deleteUser` is a soft delete. The row stays, so an application's own rows pointing at `auth.users` still resolve, and the address, the number, the password, the metadata and every identity are replaced with a one way hash of themselves. Sessions and factors go for good either way.

## Sending the mail yourself

A project with its own templates and its own mail provider does not want zou posting anything. `generateLink` writes down everything the flow would have written down and hands the link back instead of sending it.

```js
const { data } = await admin.generateLink({
  type: 'magiclink',                       // signup, invite, recovery, email_change_current, email_change_new
  email: 'ada@example.com',
  options: { redirectTo: 'https://app.example.com/welcome' },
})
// data.properties.action_link, .email_otp, .hashed_token, .verification_type
```

Nothing goes out, so the dev inbox stays empty and a real SMTP server is never touched. The six digit code comes back next to the link, so a project that verifies codes rather than links has both. A `magiclink` for an address nobody has signed up with turns into a signup with a password nobody knows, which is upstream's way of inviting somebody without saying so, and the answer says `signup` back. A `recovery` or an email change for an address with no account is a 404 rather than a silence, because the caller here is the project itself and not a stranger. `redirectTo` is dropped unless it is somewhere this project owns, the same rule the rest of the mail follows.

`admin.inviteUserByEmail(email)` is the other half: it makes the account and does post the mail.

```js
await admin.inviteUserByEmail('ada@example.com', { data: { team: 'core' } })
```

The account it leaves has no password at all, so the invitation is the only way in until whoever takes it sets one, and it is marked `invited_at` so an application can tell an invited account from one that signed itself up. An address somebody has already confirmed is refused with `email_exists`; an account that never confirmed is invited where it stands, and the code it was holding stops working.

## Putting your own claims in every token

The claims in an access token are what `auth.jwt()` reads back inside every RLS policy, so a project that wants a policy to say `auth.jwt()->>'plan' = 'gold'` needs the plan in the token. The custom access token hook is a function in your own database that gets the claims this server was about to sign and hands back the claims it wants signed instead.

```sql
create function public.custom_access_token_hook(event jsonb) returns jsonb
language plpgsql stable as $$
declare
  claims jsonb := event->'claims';
  plan text;
begin
  select tier into plan from public.subscriptions where user_id = (event->>'user_id')::uuid;
  claims := jsonb_set(claims, '{plan}', to_jsonb(coalesce(plan, 'free')));
  return jsonb_set(event, '{claims}', claims);
end;
$$;
```

```
ZOU_HOOK_CUSTOM_ACCESS_TOKEN_URI=pg-functions://postgres/public/custom_access_token_hook
ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED=true
```

Every grant goes through it: password, refresh, magic link, phone code, social provider, anonymous, and the token pair a second factor hands back. The function is told which one it is in `event->>'authentication_method'`, and a refresh says `token_refresh` rather than repeating whatever the session was first proved with. It also gets `event->'metadata'` naming the call, the moment, and the address the request came from.

The claims it returns replace the whole set rather than merging into it, so a hook that reads `event->'claims'` and puts it back changed is the shape to copy. What comes back still has to carry the claims a Supabase client and an RLS policy need, and a hook that drops one gets a 500 that names it rather than a token nothing downstream can use.

The function runs inside the sign in's own transaction, which is the part worth knowing. What it writes commits with the sign in and rolls back with it, so a hook that keeps its own log of who signed in never records a sign in that did not happen. It gets two seconds. A hook that raises is a 500 and the sign in never happened.

It can also refuse, which is how a project puts its own rule in front of every sign in:

```sql
return jsonb_build_object('error', jsonb_build_object(
  'http_code', 403, 'message', 'only members of the company may sign in'));
```

The request fails with that message at that status, and the signup or sign in behind it goes back with it. A refusal on a refresh leaves the refresh token the client presented working, so a hook that breaks does not log everybody out.

Leaving the URI set and `ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED` unset wires the hook up and leaves it switched off, which is how upstream lets an operator put the plumbing in place first. Only `pg-functions://` URIs work here so far. The HTTP variant of a hook is not built, and a URI naming an endpoint is refused at startup rather than quietly ignored.

## Rate limits

Every auth endpoint has a budget, and they are GoTrue's own numbers: 150 token grants and 30 of everything else per caller per five minutes, 30 anonymous sign ins an hour, 15 factor challenges and verifications a minute, 30 emails and 30 text messages an hour for the whole project. A caller over budget gets a 429 with `over_request_rate_limit`.

None of the per caller budgets do anything until this server can tell one caller from another, which is upstream's behaviour and the thing to know before relying on any of it. Behind a proxy or a load balancer, name the header it sets:

```
ZOU_RATE_LIMIT_HEADER=x-forwarded-for
```

Listening on a socket yourself, with nothing forwarding, count callers by where they connected from:

```
ZOU_RATE_LIMIT_PEER=true
```

Set neither and nobody is limited by endpoint, because there is nothing to count against. Do not set `ZOU_RATE_LIMIT_PEER` behind a proxy: every request arrives from the proxy, so every caller would share one bucket. The platform's own `Sb-Forwarded-For` is read first when `ZOU_SECURITY_SB_FORWARDED_FOR_ENABLED=true`, and it wins over the header above.

The numbers themselves move with the same names GoTrue uses:

```
ZOU_RATE_LIMIT_TOKEN_REFRESH=150
ZOU_RATE_LIMIT_VERIFY=30
ZOU_RATE_LIMIT_OTP=30
ZOU_RATE_LIMIT_ANONYMOUS_USERS=30
ZOU_MFA_RATE_LIMIT_CHALLENGE_AND_VERIFY=15
ZOU_RATE_LIMIT_EMAIL_SENT=30
ZOU_RATE_LIMIT_SMS_SENT=30
```

`ZOU_RATE_LIMIT_OTP` is one number for six endpoints: signup, recover, magiclink, otp, resend, and updating a user. Each of them gets its own budget of that size rather than sharing one. All the endpoint budgets allow a burst of 30 whatever their refill is, except the anonymous one, which allows its whole hourly number at once.

The two send limits are different in kind. They are not per caller, they are the whole project's, because what they protect is the mail and SMS account everybody shares. They refuse with `over_email_send_rate_limit` and `over_sms_send_rate_limit`, and the flow they refuse goes back with them, so an account is never left holding a code that was never posted. Both accept `events/duration` as well as a bare number, so `ZOU_RATE_LIMIT_EMAIL_SENT=10/1m` is a bucket of ten with one back every minute, while the bare `30` is thirty in an hour and then nothing until the hour turns over. A project that confirms its own signups does not spend either budget, which is upstream's behaviour today.

## The audit trail

Every auth event writes a row to `auth.audit_log_entries`, on the same connection and inside the same transaction as the thing it describes. A signup whose transaction rolled back has no signup entry. Nothing has to be switched on and there is nothing to configure.

A row is mostly one json payload:

```sql
select payload ->> 'action'         as action,
       payload ->> 'actor_username' as who,
       created_at
  from auth.audit_log_entries
 where payload ->> 'actor_id' = '0f8fad5b-d9cb-469f-a165-70867728950e'
 order by created_at desc
 limit 20;
```

`action` is one of `login`, `logout`, `user_signedup`, `user_invited`, `user_deleted`, `user_modified`, `user_recovery_requested`, `user_reauthenticate_requested`, `user_confirmation_requested`, `user_repeated_signup`, `user_updated_password`, `token_refreshed`, `token_revoked`, `identity_unlinked`, `factor_in_progress`, `challenge_created`, `verification_attempted`, or `factor_unenrolled`. Alongside it, `log_type` puts each of those in one of six families, `account`, `team`, `token`, `user`, `factor`, and `recovery_codes`, which is what a dashboard groups by. The names do not always line up with the families: `user_signedup` is a `team` event and `login` is an `account` one.

`actor_id`, `actor_username` and `actor_via_sso` say who did it, and `actor_name` appears when the account carries a `full_name` in its metadata. Most events also carry a `traits` object with whatever the event has to say for itself, the provider on a login, the factor and challenge on a verification, the account acted on when an admin did the acting.

Three things about this table surprise people, and all three are GoTrue's behaviour rather than choices made here:

- The `ip_address` column is empty on almost every row. Only the four factor events fill it in. The address is in the request either way.
- An admin acting through the service key is not a person. `actor_id` is the nil uuid and `actor_username` is the role name, so every service key action reads as `service_role` rather than naming whoever holds the key. What the admin acted on is in the traits.
- An anonymous sign in writes nothing at all. Neither does a signup link generated through `/auth/v1/admin/generate_link`.

The table is not swept. Rows accumulate for as long as the project keeps them, and deciding on a retention policy is the operator's job.

## Types for the client

`supabase-js` is generic over a `Database` type, and everything it knows about your tables comes from a file generated out of the catalog. `zou gen types typescript` writes that file.

```bash
zou gen types typescript --db-url postgresql://postgres@127.0.0.1:5432/postgres > database.types.ts
zou gen types typescript --schema public,shop -o src/database.types.ts
```

The url can come from `--db-url`, `ZOU_DB_URL`, or `DATABASE_URL`, in that order. `--schema` takes a name, a comma separated list, or the flag repeated, and defaults to `public`. With no `--output` the file goes to stdout, so it pipes.

The file is byte for byte the file `supabase gen types typescript` writes from the same schema, line breaks included. That matters because the file is checked into your repository: a generator that produced an equivalent file laid out differently would show up as a whole file diff the first time you switched, and again every time you switched back. The test that keeps it that way is a byte comparison against a file the supabase generator produced, over a fixture holding one instance of every shape that has ever been awkward.

It only ever reads. There is a test that runs the whole command against a database set `default_transaction_read_only`, so pointing it at production is a question about your connection limit and nothing else.

## What answers where

A Supabase client is pointed at one url and reaches four surfaces under it. All four are routed today, rest and auth are built, storage is built as far as buckets and objects go, and realtime is built as far as its socket, broadcast and presence go.

| prefix | today |
| --- | --- |
| `/rest/v1` | PostgREST's grammar, the OpenAPI document at `/rest/v1/`, and `/rest/v1/rpc/<function>` |
| `/auth/v1` | the GoTrue endpoints this guide describes |
| `/storage/v1` | buckets under `/storage/v1/bucket`, objects under `/storage/v1/object`, and 501 for the rest |
| `/realtime/v1` | the websocket at `/realtime/v1/websocket`, channels, broadcast and presence on it, and 501 for the rest |

What is not built yet answers 501 rather than 404 on purpose, for any method and for every path under the prefix including the prefix itself, because a 404 reads as a wrong url and sends somebody looking for a typo they did not make. An endpoint under `/auth/v1` that is not served yet answers the same way. Anything outside all four prefixes is a 404 in the words the hosted gateway uses, `no Route matched with those values`, so a client that gets one is looking at the same sentence it would get from Supabase.

Four auth routes sit outside the apikey check, and every one of them is outside it because whoever calls it cannot be holding a key: `/auth/v1/.well-known/jwks.json` is fetched by whatever verifies a token, `/auth/v1/verify` is the link in a confirmation email opened in a mail client, and `/auth/v1/authorize` and `/auth/v1/callback` are the two halves of a social sign in, which are browser navigations rather than fetches.

The storage routes are outside it too, for a reason of their own. Storage reads its token from the `Authorization` header alone and answers a request that has none in its own words, and it does that behind the same gateway everything else sits behind. zou is the gateway as well as the server, so the only way to give that request the answer a Supabase project gives it is to let it past the check and refuse it in the handler. Everything else, the stubs included, wants an `apikey`.

The bytes of an object go where the pages go. `zou dev <target>` hands the storage surface the same target the engine runs on, under a prefix of its own, so a store on a directory keeps its objects in that directory and a store on a bucket keeps them in that bucket. Something embedding the server sets `objects` in the config instead, and a server that was given nothing answers the bucket surface and refuses the routes that carry bytes rather than writing files somewhere nobody asked for.

## Where to go next

- docs/compatibility.md for what a project moving off Supabase will notice, and what is still missing
- docs/architecture.md for the shape of the whole system
- docs/storage-engine.md for the manifest, lease, WAL, checkpoint, and branching design
- docs/postgres.md for the patch series and the storage manager shim
- docs/branching.md for branches, point in time, and a database per pull request
- docs/embedded.md for running the whole thing inside another process
- docs/operations.md for leases, retention, and recovery in operation
- docs/perf.md and docs/benchmarks.md for how the numbers are measured
