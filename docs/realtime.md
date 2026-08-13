# Realtime

A socket at `/realtime/v1/websocket`, channels on it, and broadcast and presence between them.

`postgres_changes` is built as well, so a channel can subscribe to a table and be sent the rows that changed in it.
That is a smaller surface than the one Supabase Realtime serves, and the rest of this page is as much about the line as about what is on this side of it.

## Connecting

Nothing about the client is special, because there is nothing for it to be special about.

```js
import { createClient } from '@supabase/supabase-js'

const supabase = createClient('http://127.0.0.1:54321', ANON_KEY)

const room = supabase.channel('room')
room.on('broadcast', { event: 'cursor' }, ({ payload }) => draw(payload))
room.subscribe()

room.send({ type: 'broadcast', event: 'cursor', payload: { x: 12, y: 40 } })
```

The url is the project's url, the same one the rest of the client already has, and the key is the same anon key.
`supabase.channel(name)` opens the socket on the first channel and reuses it for the rest, and every channel on it is a topic of the form `realtime:<name>`.

Two protocol versions are spoken, and the client picks with `?vsn=`.
`2.0.0` is the current one and is what a current client sends without being asked.
`1.0.0` is the shape that predates it, and it is here because a client that pins an old version is a client that would otherwise get a socket that closes on the first frame.
Anything else is refused at the handshake with a sentence saying which two are spoken, rather than accepted and then answered in a shape the client cannot parse.

## What a channel does

`broadcast` sends a payload to the other sockets on the topic, and the two options in the join payload are honoured.
`self` decides whether the sender hears its own message back, and it is off unless asked for, because most of the time the sender already knows what it just sent.
`ack` decides whether the push is answered at all, and a client that asked for one gets a reply it can await.

A push carrying an event name is sent as bytes by the current client rather than as json, which is a detail nobody using the client ever sees and the one thing a server has to get right or it will look like it works and deliver nothing.
Both encodings are read, and a delivery goes out in whichever encoding the receiving socket can read: bytes to a `2.0.0` socket, json to a `1.0.0` one.

A broadcast reaches the sockets on the topic and nothing else.
Another topic does not hear it, and neither does another project: a node serving a thousand projects gives each one its own router and its own set of topics, so `realtime:room` on one project and `realtime:room` on another are two rooms that share a name.

## Presence

Presence is who is on a topic right now, kept by the server so that a socket arriving late is told rather than left to guess.

```js
const room = supabase.channel('room', { config: { presence: { key: user.id } } })

room.on('presence', { event: 'sync' }, () => render(room.presenceState()))
room.subscribe(async (status) => {
  if (status === 'SUBSCRIBED') await room.track({ typing: false })
})
```

`track` puts a payload on the topic under the channel's presence key, `untrack` takes it off, and both are answered so the client can await them.
A socket that names no key is known by the name the server gave it, which is enough to tell two anonymous sockets apart and nothing more.
Two tabs signed in as the same person are one key with two metas under it, and one tab closing takes one meta rather than the person.

Every change goes out to the topic as a `presence_diff` carrying `joins` and `leaves`, and a socket that has just joined gets a `presence_state` with the whole topic in it.
The tracker sees its own change through the same diff everybody else does, so there is one path into a client's state and not two.
A meta carries a `phx_ref` the client renames to `presence_ref`, which is how it tells one of a key's metas from another.

`presence: { enabled: true }` in the join decides whether this socket is *sent* presence, not whether it can be *seen*: a socket that tracks without asking for presence is in everybody else's state and gets no diffs itself.
That is the client's own rule, and it is the one that matters in practice, because the client sets the flag for you the moment a channel has a presence binding on it.

All of that is asked of zou through the real client rather than only over a raw socket, in the presence suite in [tamnd/zou-conformance](https://github.com/tamnd/zou-conformance), which runs against a real `supabase start` as well so that the questions are the reference's answers rather than ours.
The http broadcast below is asked there the same way, through both of the client's own ways of sending.

State is per topic and per project, and it is held where the sockets are.
A socket that goes away, whether it untracks, leaves the channel or just disconnects, leaves the topic, and the last socket off a topic takes the topic's state with it.
Nothing about presence is written down: it is the set of sockets that are connected now, so a node restart is an empty room and the clients that reconnect fill it back in.

## Sending without a socket

A room can be sent to over http, which is what anything that is not a browser does: a trigger, a worker, a cron job, or a client whose socket is not up yet.

```bash
curl -X POST "$URL/realtime/v1/api/broadcast" \
  -H "apikey: $ANON_KEY" -H 'content-type: application/json' \
  -d '{"messages":[{"topic":"room","event":"cursor","payload":{"x":12,"y":40}}]}'
```

That is the batch shape, and it is what `channel.send()` posts when the channel has no socket to push down.
The single message shape is `POST /realtime/v1/api/broadcast/{topic}/events/{event}` with the payload as the whole body, which is what `channel.httpSend(event, payload)` calls, and a body sent as `application/octet-stream` is carried as bytes the whole way, so the other client reads it as an ArrayBuffer rather than as text.
The topic in both is the channel's name, without the `realtime:` the socket topic carries, which is the same name the client passes to `supabase.channel()`.

The answer is 202 with an empty body.
It says the messages were taken, not that anybody heard them: a broadcast is not stored, nobody may be on the topic, and accepted is the strongest true thing there is to say.
A message missing its topic, its event or its payload is 422 and a list of what is wrong with each message in the batch, and a batch with one bad message in it sends none of them, because half a batch delivered and then complained about is an answer nobody can act on.

## Sending from sql

A room can also be sent to from inside the database, which is how a project broadcasts a row it has just written without an http client anywhere in the transaction.

```sql
select realtime.send(
  jsonb_build_object('x', 12),  -- payload
  'cursor',                     -- event
  'room',                       -- topic
  true                          -- private
);
```

```sql
create trigger orders_changed after insert or update on public.orders
for each row execute function public.orders_changed();

create or replace function public.orders_changed() returns trigger
language plpgsql as $$
begin
  perform realtime.broadcast_changes(
    'orders', tg_op, tg_op, tg_table_name, tg_table_schema, new, old);
  return null;
end;
$$;
```

Both are upstream's functions with upstream's signatures, down to the argument order, because a trigger somebody already wrote is written against them.
`realtime.send()` defaults to private, so a send that says nothing about it goes to the private room and a public channel of that name hears nothing.
`realtime.send_binary()` is the same thing with a `bytea` payload, which the other client reads as an ArrayBuffer.
`realtime.broadcast_changes()` builds the payload Supabase documents, `record`, `old_record`, `operation`, `table` and `schema`, and hands it to `realtime.send()`.

All three insert a row into `realtime.messages` and swallow whatever goes wrong, upstream included, so a send the policies refused is a warning in the log and a message nobody hears rather than a failed statement.
That is worth knowing before writing a trigger that relies on one: the insert is checked by the same policies as everything else on that table, and the trigger will not tell you when it was refused.

How the row reaches a socket is not upstream's.
Upstream reads it out of a logical replication slot, which means a slot, a publication and a decoder held open per project, and that is standing machinery a server whose whole point is to have none while nothing is happening cannot hold.
So the row announces itself: an after insert trigger calls `pg_notify` and the server listens on one connection.
The message rides inside the notification when it fits, and anything bigger, or binary, is announced by id and read back on the same connection.
Notifications are transactional, which is what makes this safe next to the policy probes: a probe rolls back, and a notification from a transaction that rolled back is never delivered, so a join never broadcasts itself.

Rows written this way are kept for three days and then deleted, which is upstream's retention done with a delete instead of by dropping a day's partition.

## Tokens

Three places a token can arrive, and all three are checked by the same verifier every http request goes through.

The `apikey` on the connect url is the project key, and a socket without one does not get past the handshake.
The `access_token` in a join payload is the user, and it is how a channel runs as the person rather than as the key the socket was opened with.
The `access_token` event is the same thing sent again on a live connection, which is what a client does when the token it opened with is about to expire.

A refresh that does not verify takes the socket's channels down rather than leaving them running on the claims that were about to stop being true.
The client sees an error on the push and then a `phx_error` on each channel, which is what it needs to resubscribe once it has a token that works.

## Private channels

A private channel is one whose rules are written in sql rather than in the server.

```js
const room = supabase.channel('room', { config: { private: true } })
```

```sql
create policy "read the rooms you are in" on realtime.messages
for select to authenticated
using (
  exists (
    select 1 from memberships m
    where m.room = realtime.topic() and m.person = auth.uid()
  )
);

create policy "write to the rooms you are in" on realtime.messages
for insert to authenticated
with check (
  exists (
    select 1 from memberships m
    where m.room = realtime.topic() and m.person = auth.uid()
  )
);
```

Those are ordinary row level security policies on `realtime.messages`, which is Supabase's convention and is worth reading twice, because nothing in the server knows the word room.
`realtime.topic()` is the channel's name without the `realtime:` a socket topic carries, and `auth.uid()` is whoever the token on the channel says it is.
A project that already has policies written for Supabase Realtime does not rewrite them to run here.

The way the server finds out is to ask postgres to try.
Reading is two rows written into `realtime.messages`, one for `broadcast` and one for `presence`, and then selected back as the user: whatever the select policies let through is what this person may read.
Writing is an insert tried as the user, where a refusal for insufficient privilege is a no and anything else is a yes.
Both run in a transaction that is rolled back whatever it found, so nothing is ever kept and the table stays empty.
That is upstream's own method, down to the two extensions and the rollback, because a check that worked differently would answer differently for the same policies.

A private channel and a public channel of the same name are two rooms rather than one.
`supabase.channel('room', { config: { private: true } })` and `supabase.channel('room')` do not hear each other, and they have to not: a public channel is joined by name with no policy read, so one room shared between the two would mean anybody could join `room` in a line and hear everything the policies were keeping them out of.
The same split runs through http, where `?private=true` picks which of the two a send goes to.

Reading is checked at the join, and a refusal names the room.

```
You do not have permissions to read from this Channel topic: room
```

Writing is checked the first time the channel sends something and then remembered for as long as the channel is up, which keeps a chatty room to one check rather than one per message.
A new token puts every private channel on the socket back to the policies, because the person the policies were answered for has just changed.
A channel that may no longer be read gets a `phx_error`, which is the signal a client resubscribes on.

Over http the same policies apply and the answers are upstream's.
`POST /realtime/v1/api/broadcast/{topic}/events/{event}?private=true` is 403 with `{"message":"Unauthorized"}` when the write policies say no.
The batch endpoint drops the private messages that were refused, sends the rest, and still answers 202, which is not much of an answer but it is the one clients are written against.
A service key is not stopped by any of this, because `service_role` has `bypassrls` and the policies never see it, which is how a project's backend reaches every room without being named in every policy.

Two things here are deliberately not upstream's.
A push a write policy refused is answered with an error on the push rather than dropped in silence, because silence is indistinguishable from a message that was sent and heard by nobody.
And `realtime.messages` is created unpartitioned rather than partitioned by day with a janitor keeping tomorrow's partition ready, because the only rows that outlive their statement are the ones `realtime.send()` wrote and they are deleted after three days, which is the same three days upstream keeps and one statement rather than a moving part.

A server running without a database refuses a private channel by saying that, rather than by saying 403.
403 is an answer about a project's policies, and sending somebody to read policies that were never consulted is worse than telling them what is actually missing.

## What a project is allowed

A project has a budget, the same five numbers upstream keeps on the tenant row, and the defaults are upstream's too.

| what | default | environment | what a client sees over it |
| --- | --- | --- | --- |
| sockets at once | 200 | `ZOU_REALTIME_MAX_CONCURRENT_USERS` | 429 at the handshake, `{"error":"Too many connected users"}` |
| joins a second | 100 | `ZOU_REALTIME_MAX_JOINS_PER_SECOND` | `ClientJoinRateLimitReached: Too many joins per second` on the join, then the socket is closed |
| channels per socket | 100 | `ZOU_REALTIME_MAX_CHANNELS_PER_CLIENT` | `ChannelRateLimitReached: Too many channels` on the join |
| messages a second | 100 | `ZOU_REALTIME_MAX_EVENTS_PER_SECOND` | the channel is sent `Too many messages per second` on its system event and closed |
| bytes in a message | 3000 kb | `ZOU_REALTIME_MAX_PAYLOAD_SIZE_IN_KB` | `payload_size_exceeded` when the channel asked for acks, and nothing when it did not |

Set any of them to `0` and that one is off, which is what a server running its own project usually wants and is not something upstream's tenant row can say.

The three rates are averages and not ceilings on a moment.
What is counted goes into a bucket per five seconds, twelve of them are kept, and the number compared with the limit is the average per second across that minute, so a burst is forgiven and a sustained rate is not.
A message costs the send and every delivery of it, so one broadcast to a hundred sockets is a hundred and one messages, which is the arithmetic that makes a hundred a second a real number rather than a generous one.

The http broadcast endpoints report the same budget on every answer.

```
x-rate-rolling: 12
x-rate-limit: 100
x-rate-limit-remaining: 88
```

A post that arrives when the project is already over its budget is 429 with `{"message":"Too many requests"}` and the same three headers, so a caller posting on a loop can read how much room is left without waiting to be refused.
A project with the events budget off reports none of this, because a header saying zero of zero left reads as refused.

Presence has two limits of its own upstream, on presence events a second and on how often one client may track, and neither is built here yet.

## Subscribing to a table

Nothing about this is special either:

```js
supabase
  .channel('todos')
  .on('postgres_changes', { event: '*', schema: 'public', table: 'todos' }, (change) => {
    console.log(change.eventType, change.new)
  })
  .subscribe()
```

A channel can carry several of these, filtered or not, and each callback is run for the subscriptions it belongs to:

```js
  .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'todos', filter: 'user_id=eq.' + id }, mine)
```

A filter is one column compared with one value, in the operators PostgREST spells `eq`, `neq`, `lt`, `lte`, `gt`, `gte` and `in`, and it is compared in the column's own type rather than as text.
A subscription with a filter and no table is refused, because a column of no table in particular is a subscription that would quietly match nothing.

The whole channel is refused when any of its subscriptions cannot be made, which the client reports as `CHANNEL_ERROR` with the reason on it.
Nothing is half subscribed.

## What the database has to be for postgres changes

Two things have to be true of a database before any of this delivers anything, and both are true of one this server started.

`wal_level` must be `logical`.
It is a postmaster setting and a restart, so `zou dev` and `zou serve` start postgres with it rather than turning it on when somebody first subscribes.
A database you brought yourself is whatever you set it to, and a tap against one below logical says so rather than reporting that nothing changed.

The table has to be in the `supabase_realtime` publication, which the bootstrap contract creates empty:

```sql
alter publication supabase_realtime add table todos;
```

That is the same line a Supabase project runs, and it is what makes changes opt in.
A publication with nothing in it publishes nothing, and a table nobody added is not read, decoded, or sent.

An update tells you what the row became.
Whether it also tells you what the row was depends on the table, and by default it does not:

```sql
alter table todos replica identity full;
```

Without that, postgres publishes the primary key of a changed row and not the rest of it, which is why `old_record` on a Supabase project is usually a key and a disappointment.
The cost of `full` is write ahead log volume on every update and delete, which is why it is not the default anywhere.

The slot a tap holds is temporary.
It lives exactly as long as the connection holding it, so a server nobody is subscribed to retains no write ahead log and leaves nothing behind to clean up, and a tap sees what happened after it opened rather than a replay of what it missed.
That is upstream's trade too.

## What a change looks like

What arrives is what Supabase sends:

```json
{
  "schema": "public",
  "table": "todos",
  "commit_timestamp": "2021-11-05T17:20:51.524Z",
  "type": "INSERT",
  "columns": [
    { "name": "id", "type": "int4" },
    { "name": "details", "type": "text" }
  ],
  "record": { "id": 12, "details": "wash up" },
  "errors": null
}
```

An insert has a `record`, a delete has an `old_record`, and an update has both.
The client renames them to `new` and `old` before your handler sees them.
The commit timestamp is when the transaction committed rather than when the row was written, because a row has no time of its own until then, and it is the same on every change in one transaction.

Values are json rather than the text postgres prints: an integer is a number, a boolean is a boolean, a `jsonb` column is the json it holds, an array is an array, and a timestamp is `2021-11-05T17:20:51.524+00:00` rather than the space separated form.
That is postgres's own `to_jsonb` of the value, which is what upstream calls per column, and it is checked against `to_jsonb` in the tests rather than against a description of it.
A `bytea` is hex with no `\x` on the front, which is upstream's shape and not postgres's.

Two rules worth knowing before you read a payload.

A column stored out of line that an update did not write is not sent by postgres at all.
If the table publishes its old rows it is filled in from there, and if it does not the key is simply absent from `record`, which is not the same as null.

A change larger than a megabyte arrives with every value over sixty four bytes left out and `errors` reading `["Error 413: Payload Too Large"]`.
The id survives, so a client can go and read the row.

## Who a change is sent to

A subscription is a read, so a subscriber is sent a row only if the database would have shown it to them.
That is asked of the database rather than decided by this server: it becomes the subscriber's role, puts their claims where `auth.uid()` reads them, and selects the changed row back by its primary key.
Your policies therefore mean here what they mean through `/rest/v1`, and there is nothing extra to write for realtime beyond turning row level security on.

Column privileges are part of the answer.
A subscriber holding `grant select (id, title)` gets those columns and no others, in both `record` and `columns`, so a client is never told about a column it may not read.

Three cases are worth knowing before you rely on this.

A delete is sent to every subscriber, because the row is gone and no policy can be asked about it.
On a table with row level security on, its `old_record` is cut down to the primary key, so what a subscriber learns is that a row with that key is gone rather than what was in it.
That holds on a table with `replica identity full` as well, where postgres publishes every column of the deleted row and marks all of them as part of the identity: the cut is made on the key the catalog has rather than on what the change says about itself, because the other way round is a table publishing its deleted rows to everybody who subscribed.

A table with no primary key cannot be checked, since there is nothing to select the row back by, and its changes arrive carrying `["Error 400: Bad Request, no primary key"]` instead of a record.
A subscriber who may not select the key columns gets `["Error 401: Unauthorized"]`, which is the same problem from the other side.

The check runs against the table as it is now, not as it was at the moment of the change.
An update that moves a row out of a policy's view is checked in its new state, and a row deleted immediately afterwards is already gone.
Upstream behaves the same way, and a project that needs an audit trail of who saw what should write one rather than infer it from this.

## What is not built

| asked for | what happens |
| --- | --- |
| `postgres_changes` on a server with no database | the join is refused, naming what is missing |
| presence rate limits | presence is not counted separately from the rest |

That is M4, tracked in [tamnd/zou#4](https://github.com/tamnd/zou/issues/4).

The refusal is worded rather than silent on purpose.
A server can accept any of these joins, reply `SUBSCRIBED` and then send nothing, and every client in the world will sit there waiting, which is the worst answer available: the thing that is missing looks exactly like the thing that is working and idle.

## On a fleet

A project's sockets live on the node holding that project.
A node that is asked to upgrade a socket for a project another node holds refuses it with 503 and a sentence, rather than serving it locally, because serving it locally would put half the room on one node and half on the other with no way for either half to hear the other.

A fan out tier that serves sockets away from the holder is part of M4 and is not built.
Until it is, the topology is the simple one: the holder serves the project, sockets included.
