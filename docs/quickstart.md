# Quickstart

From clone to a Postgres whose every page and WAL byte lives on a store, in about ten minutes, most of them spent compiling Postgres once.

## Prerequisites

A recent stable Rust toolchain for everything, and for the full tour the usual Postgres build tools: meson, ninja, a C compiler, flex, bison, and the readline, zlib, and icu libraries.

- Debian and Ubuntu: `apt-get install meson ninja-build flex bison libreadline-dev zlib1g-dev libicu-dev pkg-config uuid-dev`
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

`make pg-build` fetches the pinned Postgres submodule, applies the zou patch series, and builds it with the storage manager shim linked in, see docs/postgres.md. With that in place `make demo` continues past act one: it starts `zou dev` on a fresh store, writes rows through plain `psql`, stops the server, prints what the store holds with `zou info`, takes a branch with `zou branch` which costs one small manifest and copies no data, and then restarts Postgres from nothing but the store and reads the rows back.

## Your own targets

`zou dev <target>` accepts more than a directory:

```bash
zou dev /tmp/mydb                      # a directory of objects
zou dev s3://bucket/prefix             # any S3 compatible endpoint, see below
zou dev sqlite:///tmp/mydb.db          # the whole store in one SQLite database
```

S3 style targets read `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the environment, plus `ZOU_S3_ENDPOINT` for a non AWS endpoint like a local MinIO and `ZOU_S3_REGION` when it matters. `gs://bucket/prefix` speaks the GCS dialect with HMAC interop keys.

A path ending `.zou` is the single file backend. Every sequential tool works over it today, `zou info`, `zou branch`, `zou-bootstrap`, `zou-restore`, while `zou dev` needs the multi process postmaster and waits on the in process engine, see the note in docs/storage-engine.md.

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

## Where to go next

- docs/architecture.md for the shape of the whole system
- docs/storage-engine.md for the manifest, lease, WAL, checkpoint, and branching design
- docs/postgres.md for the patch series and the storage manager shim
- docs/operations.md for leases, retention, and recovery in operation
- docs/perf.md and docs/benchmarks.md for how the numbers are measured
