# Realtime

A socket at `/realtime/v1/websocket`, channels on it, and broadcast between them.

That is a smaller surface than the one Supabase Realtime serves, and the rest of this page is as much about the line as about what is on this side of it.
Presence, `postgres_changes` and private channels are not built.
A join asking for one of them is answered with an error naming what is missing, which is the difference between a client that reports a failure and a client that waits forever for rows nobody is sending.

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

## Tokens

Three places a token can arrive, and all three are checked by the same verifier every http request goes through.

The `apikey` on the connect url is the project key, and a socket without one does not get past the handshake.
The `access_token` in a join payload is the user, and it is how a channel runs as the person rather than as the key the socket was opened with.
The `access_token` event is the same thing sent again on a live connection, which is what a client does when the token it opened with is about to expire.

A refresh that does not verify takes the socket's channels down rather than leaving them running on the claims that were about to stop being true.
The client sees an error on the push and then a `phx_error` on each channel, which is what it needs to resubscribe once it has a token that works.

## What is not built

| asked for | what happens |
| --- | --- |
| `postgres_changes` in a join | the join is refused, naming what is missing |
| `presence` enabled in a join, or a presence event | the same |
| a private channel | the same |
| `POST /realtime/v1/api/broadcast` | 501 |

All four are M4, tracked in [tamnd/zou#4](https://github.com/tamnd/zou/issues/4).

The refusals are worded rather than silent on purpose.
A server can accept any of these joins, reply `SUBSCRIBED` and then send nothing, and every client in the world will sit there waiting, which is the worst answer available: the thing that is missing looks exactly like the thing that is working and idle.

## On a fleet

A project's sockets live on the node holding that project.
A node that is asked to upgrade a socket for a project another node holds refuses it with 503 and a sentence, rather than serving it locally, because serving it locally would put half the room on one node and half on the other with no way for either half to hear the other.

A fan out tier that serves sockets away from the holder is part of M4 and is not built.
Until it is, the topology is the simple one: the holder serves the project, sockets included.
