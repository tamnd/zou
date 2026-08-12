//! The socket end of the realtime surface.
//!
//! `zou_realtime` is the protocol with no io in it: a message goes in
//! and a list of things to do comes out. This is the part that has the
//! io, and it is deliberately small, because everything that can be
//! decided without a socket has been decided somewhere a test can
//! reach without opening one.
//!
//! A connection is one task with two things to wait on: the next
//! message from the client, and the next broadcast from a topic this
//! socket is on. Whichever arrives first is handled and the loop goes
//! round again, so a client that is only listening costs a parked
//! future and a client that is only sending never blocks on a reader.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::sync::broadcast::error::RecvError;
use zou_realtime::{
    About, Action, Ask, BinaryBroadcast, Delivery, Encoding, Fanout, Grant, Hub, Identity, Session,
    SocketId, Tokens, Vsn,
};

use crate::{App, AuthContext, json_body, policy};

/// What checks a token for a socket: the project's own verifier, the
/// same one every http request goes through.
struct ProjectTokens {
    app: Arc<App>,
    anon_role: String,
}

impl Tokens for ProjectTokens {
    fn verify(&self, token: &str) -> Result<Identity, String> {
        match crate::jwt::verify_any(token, &self.app.cfg.jwt_secret, self.app.jwks.as_ref()) {
            Ok(verified) => {
                let role = match verified.role.as_deref() {
                    None | Some("") => self.anon_role.clone(),
                    Some(role) => role.to_string(),
                };
                Ok(Identity {
                    role,
                    claims: verified.claims,
                })
            }
            // GoTrue's words for a token that will not verify, which is
            // what a client shows when a channel refuses its token.
            Err(_) => Err("invalid claim: token is expired or malformed".into()),
        }
    }
}

/// The websocket endpoint, `/realtime/v1/websocket`.
///
/// The apikey gate has already run, so a request that gets here proved
/// a project key. Who the socket is on top of that comes from the
/// bearer token when there was one, which for a browser client there
/// never is: a websocket cannot carry headers, which is exactly why
/// the protocol has an `access_token` in the join payload and an
/// `access_token` event after it.
pub async fn websocket(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(auth): axum::Extension<AuthContext>,
    uri: Uri,
    upgrade: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Response {
    let Ok(upgrade) = upgrade else {
        // A plain GET of a websocket url. Upstream's gateway answers
        // 426 for this, and a browser that lands on the address bar
        // version of a realtime url gets exactly that.
        return json_body(
            axum::http::StatusCode::UPGRADE_REQUIRED,
            serde_json::json!({"message": "this endpoint is a websocket, upgrade required"}),
        );
    };
    let vsn = query(&uri, "vsn");
    let Some(vsn) = Vsn::parse(vsn.as_deref()) else {
        return json_body(
            axum::http::StatusCode::BAD_REQUEST,
            serde_json::json!({
                "message": "this server speaks the 1.0.0 and 2.0.0 phoenix protocols",
            }),
        );
    };
    let identity = Identity {
        role: auth.role.clone(),
        claims: (*auth.claims).clone(),
    };
    let anon_role = app.cfg.anon_role.clone();
    upgrade.on_upgrade(move |socket| async move {
        // A router can be built outside a runtime, so what listens for
        // database sends starts on the first socket rather than at
        // boot.
        app.sending
            .get_or_init(|| async { deliver(Arc::clone(&app)) })
            .await;
        let tokens = ProjectTokens {
            app: Arc::clone(&app),
            anon_role,
        };
        run(socket, Session::new(vsn, identity), &app, &tokens).await;
    })
}

/// One connection, until it goes.
async fn run(mut socket: WebSocket, mut session: Session, app: &Arc<App>, tokens: &dyn Tokens) {
    let hub = &app.hub;
    let me = hub.socket();
    // The topics this socket is on, each with the receiver it is
    // hearing them through. A join adds one and a leave drops one,
    // which is what stops a channel the client left from being
    // polled.
    let mut carrying: HashMap<String, tokio::sync::broadcast::Receiver<Delivery>> = HashMap::new();
    loop {
        let heard = if carrying.is_empty() {
            // Nothing to fan in, so this is just the client's next
            // message. Waiting on an empty set of receivers would
            // otherwise be a future that never wakes.
            Heard::Client(socket.recv().await)
        } else {
            tokio::select! {
                message = socket.recv() => Heard::Client(message),
                fanned = next_delivery(&mut carrying) => fanned,
            }
        };
        let actions = match heard {
            Heard::Client(None) => break,
            Heard::Client(Some(Err(e))) => {
                log::debug!("realtime: the socket went, {e}");
                break;
            }
            Heard::Client(Some(Ok(Message::Text(text)))) => session.text(text.as_str(), tokens),
            Heard::Client(Some(Ok(Message::Binary(bytes)))) => session.binary(&bytes),
            // Ping and pong are the transport's own and axum answers
            // them itself. A close is the client leaving.
            Heard::Client(Some(Ok(Message::Close(_)))) => break,
            Heard::Client(Some(Ok(_))) => Vec::new(),
            Heard::Fanned(delivery) => {
                let (from, fan) = &*delivery;
                match session.deliver(fan, *from == me) {
                    Some(action) => vec![action],
                    None => Vec::new(),
                }
            }
            // A socket so far behind that the hub gave up holding its
            // backlog. Carrying on would be a client missing messages
            // and not knowing it, so the socket is closed and the
            // client reconnects and resubscribes.
            Heard::Lagged(topic, missed) => {
                log::warn!("realtime: a socket missed {missed} messages on {topic}, closing it");
                break;
            }
        };
        if !act(&mut socket, actions, &mut session, app, me, &mut carrying).await {
            break;
        }
    }
    let topics: Vec<String> = carrying.keys().cloned().collect();
    // The presence goes before the receivers do, so the diff saying
    // this socket left is fanned while the topic is still up.
    for topic in &topics {
        hub.untrack(me, topic);
    }
    // Dropping the receivers is what actually takes this socket off
    // the topics; releasing after that is the bookkeeping that lets
    // the hub forget a topic nobody is on any more.
    drop(carrying);
    for topic in &topics {
        hub.released(topic);
    }
}

/// What woke the loop.
enum Heard {
    Client(Option<Result<Message, axum::Error>>),
    Fanned(Delivery),
    Lagged(String, u64),
}

/// The next broadcast from any topic this socket is on.
///
/// A linear poll over the receivers, because a socket is on a handful
/// of channels rather than thousands, and because the alternative is a
/// task and a queue per socket to merge them, which is more moving
/// parts than a browser tab's worth of channels deserves.
async fn next_delivery(
    carrying: &mut HashMap<String, tokio::sync::broadcast::Receiver<Delivery>>,
) -> Heard {
    let mut waiting: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Heard> + Send>>> =
        Vec::with_capacity(carrying.len());
    for (topic, receiver) in carrying.iter_mut() {
        let topic = topic.clone();
        waiting.push(Box::pin(async move {
            match receiver.recv().await {
                Ok(delivery) => Heard::Fanned(delivery),
                Err(RecvError::Lagged(missed)) => Heard::Lagged(topic, missed),
                // The last sender went, which the hub only does when
                // the topic is empty. Nothing more will arrive here,
                // so this arm parks rather than spinning.
                Err(RecvError::Closed) => std::future::pending().await,
            }
        }));
    }
    first_of(waiting).await
}

/// The first of several futures to finish, without a combinator crate
/// for it. Every future here is a receiver's `recv`, and dropping the
/// losers cancels nothing that matters: a broadcast receiver that is
/// polled and dropped has not consumed anything.
async fn first_of(
    mut waiting: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Heard> + Send + '_>>>,
) -> Heard {
    std::future::poll_fn(move |cx| {
        for future in waiting.iter_mut() {
            if let std::task::Poll::Ready(heard) = future.as_mut().poll(cx) {
                return std::task::Poll::Ready(heard);
            }
        }
        std::task::Poll::Pending
    })
    .await
}

/// Do what the session asked for. False means the socket is finished.
async fn act(
    socket: &mut WebSocket,
    actions: Vec<Action>,
    session: &mut Session,
    app: &Arc<App>,
    me: SocketId,
    carrying: &mut HashMap<String, tokio::sync::broadcast::Receiver<Delivery>>,
) -> bool {
    let hub = &app.hub;
    for action in actions {
        match action {
            Action::Text(text) => {
                if socket.send(Message::Text(text.into())).await.is_err() {
                    return false;
                }
            }
            Action::Binary(bytes) => {
                if socket.send(Message::Binary(bytes.into())).await.is_err() {
                    return false;
                }
            }
            Action::Carry(topic) => {
                carrying.insert(topic.clone(), hub.carry(&topic));
            }
            Action::Drop(topic) => {
                carrying.remove(&topic);
                hub.released(&topic);
            }
            Action::Fan(fan) => hub.fan(me, fan),
            Action::Track {
                topic,
                key,
                payload,
            } => hub.track(me, &topic, key, payload),
            Action::Untrack(topic) => hub.untrack(me, &topic),
            Action::State(topic) => {
                let state = session.state(&topic, hub.state(&topic));
                if let Action::Text(text) = state
                    && socket.send(Message::Text(text.into())).await.is_err()
                {
                    return false;
                }
            }
            // The one thing this loop stops for. Nothing else the
            // socket asked for is done until the project's own
            // policies have answered, which is what makes a private
            // channel private.
            Action::Ask(ask) => {
                let granted = answer(app, session.identity(), &ask).await;
                let next = session.authorized(&ask, granted);
                if !Box::pin(act(socket, next, session, app, me, carrying)).await {
                    return false;
                }
            }
        }
    }
    true
}

/// Go and find out, which is the io a private channel needs and the
/// reason the session cannot decide one on its own.
async fn answer(app: &App, who: &Identity, ask: &Ask) -> Result<Grant, String> {
    let Some(pool) = app.pool.as_ref() else {
        return Err(
            "this server has no database, which is what a private channel is checked against"
                .into(),
        );
    };
    let who = policy::Who {
        role: &who.role,
        claims: &who.claims,
    };
    match ask.about {
        About::Join => policy::reads(pool, &who, ask.name()).await,
        _ => policy::writes(pool, &who, ask.name()).await,
    }
}

/// A batch of broadcasts over http, `POST /realtime/v1/api/broadcast`.
///
/// This is the endpoint the client falls back to when it has a channel
/// object and no usable socket, and it is also how anything that is
/// not a browser sends to a room: a cron job, a trigger, a worker that
/// has an http client and no websocket in it.
///
/// The answer is 202 and an empty body, which says the messages were
/// taken rather than that anybody heard them. Nobody may be on the
/// topic at all, and a broadcast is not stored, so accepted is the
/// strongest true thing there is to say.
pub async fn broadcast(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(auth): axum::Extension<AuthContext>,
    body: Bytes,
) -> Response {
    let Ok(Value::Object(sent)) = serde_json::from_slice::<Value>(&body) else {
        return unprocessable(json!({"messages": ["is invalid"]}));
    };
    let Some(messages) = sent.get("messages") else {
        return unprocessable(json!({"messages": ["can't be blank"]}));
    };
    let Some(messages) = messages.as_array() else {
        return unprocessable(json!({"messages": ["is invalid"]}));
    };
    // Every message is checked before any of them is sent, so a batch
    // with a bad one in it is refused whole rather than half delivered
    // and then complained about.
    let faults: Vec<Value> = messages.iter().map(faults_in).collect();
    if faults.iter().any(|fault| fault != &json!({})) {
        return unprocessable(json!({"messages": faults}));
    }
    // One check per topic rather than per message, since the policies
    // are about the room and a batch is usually one room several
    // times.
    let mut asked: HashMap<String, bool> = HashMap::new();
    for message in messages {
        let topic = message["topic"].as_str().unwrap_or_default();
        let event = message["event"].as_str().unwrap_or_default();
        let payload = message.get("payload").cloned().unwrap_or(json!({}));
        let private = private_message(message);
        if private {
            let allowed = match asked.get(topic) {
                Some(allowed) => *allowed,
                None => {
                    let allowed = may_write(&app, &auth, topic).await.unwrap_or(false);
                    asked.insert(topic.to_string(), allowed);
                    allowed
                }
            };
            if !allowed {
                // Upstream drops a private message the policies
                // refused and answers 202 for the batch anyway. A
                // caller sending a mixed batch cannot be told which
                // half went without being told what its policies say,
                // which is a thing the batch shape has no room for.
                log::debug!("realtime: a private broadcast to {topic} was refused by the policies");
                continue;
            }
        }
        fan(&app.hub, topic, event, private, json_payload(&payload));
    }
    accepted()
}

/// One broadcast over http, `POST /realtime/v1/api/broadcast/{topic}/events/{event}`.
///
/// The same delivery as the batch above with the names in the url and
/// the payload as the whole body, which is what `httpSend` on a channel
/// calls. A body that is `application/octet-stream` is carried as bytes
/// all the way to the other client, which reads it as an ArrayBuffer,
/// the same as bytes that came in over a socket.
pub async fn broadcast_one(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(auth): axum::Extension<AuthContext>,
    Path((topic, event)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let private = query(&uri, "private").as_deref() == Some("true");
    if private {
        match may_write(&app, &auth, &topic).await {
            Ok(true) => {}
            // The caller's own policies said no, which is what 403
            // means here and the one place this surface says it.
            Ok(false) => {
                return json_body(StatusCode::FORBIDDEN, json!({"message": "Unauthorized"}));
            }
            Err(why) => {
                return json_body(StatusCode::UNPROCESSABLE_ENTITY, json!({"message": why}));
            }
        }
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.split(';').next().unwrap_or("").trim().to_string())
        .unwrap_or_else(|| "application/json".into());
    let payload = match content_type.as_str() {
        "application/octet-stream" => (Encoding::Binary, body.to_vec()),
        "application/json" | "" => match serde_json::from_slice::<Value>(&body) {
            Ok(payload) => json_payload(&payload),
            Err(_) => return unprocessable(json!({"payload": ["is invalid"]})),
        },
        other => {
            return json_body(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                json!({
                    "message": format!("{other} is not a content type this endpoint reads"),
                }),
            );
        }
    };
    fan(&app.hub, &topic, &event, private, payload);
    accepted()
}

/// A payload on its way to the sockets, as json.
fn json_payload(payload: &Value) -> (Encoding, Vec<u8>) {
    (Encoding::Json, payload.to_string().into_bytes())
}

/// Hand one message to the topic it names.
///
/// The topic in the url is the channel's name and the topic on the
/// socket is that with `realtime:` in front, which is the same rule the
/// client follows when it joins. The sender is a socket id nobody
/// holds, so nothing is suppressed as its own echo: an http caller is
/// not on the topic and has nothing to hear back.
///
/// `private` picks which of the two rooms of that name it goes to. A
/// message sent as private reaches the sockets the policies let onto
/// the private channel, and one sent without it reaches the public
/// channel, which is the whole point of there being two.
fn fan(
    hub: &Hub,
    topic: &str,
    event: &str,
    private: bool,
    (encoding, payload): (Encoding, Vec<u8>),
) {
    hub.fan(
        hub.socket(),
        Fanout {
            topic: zou_realtime::room(&format!("realtime:{topic}"), private),
            push: BinaryBroadcast {
                join_ref: String::new(),
                reference: String::new(),
                topic: format!("realtime:{topic}"),
                event: event.to_string(),
                meta: String::new(),
                encoding,
                payload,
            },
            to_self: false,
        },
    );
}

/// What is wrong with one message in a batch, in the shape upstream
/// answers with: a map per message, empty when there is nothing wrong
/// with it, so the caller can line the answers up with what it sent.
fn faults_in(message: &Value) -> Value {
    let mut faults = serde_json::Map::new();
    for field in ["topic", "event"] {
        match message.get(field).and_then(Value::as_str) {
            Some(value) if !value.is_empty() => {}
            _ => {
                faults.insert(field.into(), json!(["can't be blank"]));
            }
        }
    }
    if message.get("payload").is_none() {
        faults.insert("payload".into(), json!(["can't be blank"]));
    }
    Value::Object(faults)
}

fn private_message(message: &Value) -> bool {
    message.get("private").and_then(Value::as_bool) == Some(true)
}

/// Taken, and nothing else said.
fn accepted() -> Response {
    (StatusCode::ACCEPTED, ()).into_response()
}

fn unprocessable(errors: Value) -> Response {
    json_body(StatusCode::UNPROCESSABLE_ENTITY, json!({"errors": errors}))
}

/// Whether this caller may send to this room, which is the same
/// question a socket on a private channel asks and the same policies
/// answer it.
///
/// The error is the sentence to hand back rather than a no: a caller
/// told no goes and reads its policies, and it should only do that
/// when the policies are what refused it.
async fn may_write(app: &App, auth: &AuthContext, topic: &str) -> Result<bool, String> {
    let Some(pool) = app.pool.as_ref() else {
        return Err(
            "this server has no database, which is what a private broadcast is checked against"
                .into(),
        );
    };
    let who = policy::Who {
        role: &auth.role,
        claims: &auth.claims,
    };
    policy::writes(pool, &who, topic)
        .await
        .map(|granted| granted.broadcast)
}

/// How long a row written by `realtime.send()` is kept, which is
/// upstream's three days. Upstream drops a day's partition; this table
/// is one table, so the equivalent is a delete.
const KEEP: &str = "72 hours";

/// How often that sweep runs. An hour is often enough that the table
/// never has more than an hour of expired rows in it and rare enough
/// that a server nobody is using is not doing anything.
const SWEEP: std::time::Duration = std::time::Duration::from_secs(3600);

/// How long to wait before dialling again after the listening
/// connection died.
const REDIAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Carry what `realtime.send()` writes to the sockets, until the
/// process ends.
///
/// Upstream reads these rows out of a logical replication slot. That
/// means a slot, a publication and a decoder held open per project,
/// which is standing machinery a server that scales to zero cannot
/// have, so the row announces itself instead: the trigger in
/// `realtime-send.sql` calls `pg_notify` and this is what is listening.
///
/// A dropped connection loses whatever was sent while it was down, and
/// that is the right answer rather than a gap to fill: a broadcast is
/// something happening now, not a log, and a client that was not there
/// to hear it was not going to be told either way.
pub fn deliver(app: Arc<App>) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = deliver_once(&app).await {
                log::warn!("realtime: the database send listener stopped: {e}");
            }
            tokio::time::sleep(REDIAL).await;
        }
    });
}

/// One connection's worth of delivering, until it dies.
async fn deliver_once(app: &App) -> Result<(), crate::sql::Error> {
    let Some(pool) = &app.pool else {
        // Nothing to listen to, and nothing that will ever change
        // that, so this task is over rather than retrying forever.
        return Ok(());
    };
    let (client, mut notes) = pool.listening("zou_realtime").await?;
    // The first tick an hour in rather than immediately, since a
    // server that has just started is not the one with three days of
    // rows behind it, and a reconnect loop should not mean a delete
    // loop.
    let mut sweep = tokio::time::interval_at(tokio::time::Instant::now() + SWEEP, SWEEP);
    loop {
        let note = tokio::select! {
            note = notes.recv() => note,
            _ = sweep.tick() => {
                // The rows are only ever read back by the branch below
                // and only ever moments after they were written, so
                // what this deletes is what nobody was going to ask
                // for. A database that will not let this server delete
                // them says so once an hour and keeps working.
                if let Err(e) = client
                    .execute(
                        &format!(
                            "delete from realtime.messages where inserted_at < now() - interval '{KEEP}'"
                        ),
                        &[],
                    )
                    .await
                {
                    log::debug!("realtime: the messages table was not swept: {e}");
                }
                continue;
            }
        };
        let Some(note) = note else { return Ok(()) };
        match sent_message(&client, &note).await {
            Ok(Some(sent)) => fan(
                &app.hub,
                &sent.topic,
                &sent.event,
                sent.private,
                sent.payload,
            ),
            Ok(None) => {}
            Err(e) => log::warn!("realtime: a database send was not delivered: {e}"),
        }
    }
}

/// One message on its way from the table to a room.
struct Sent {
    topic: String,
    event: String,
    private: bool,
    payload: (Encoding, Vec<u8>),
}

/// What a notification turned out to be saying.
enum Announced {
    /// The whole message, which is the ordinary case and costs no
    /// round trip.
    Message(Sent),
    /// The id of a row to read it out of, which is what arrives for
    /// anything too big for a notification and anything binary.
    Row(String),
    /// Nothing this server can act on, which no version of the trigger
    /// writes and something else on the same channel might.
    Nothing,
}

/// Read one notification.
fn announced(note: &str) -> Announced {
    let Ok(Value::Object(note)) = serde_json::from_str::<Value>(note) else {
        return Announced::Nothing;
    };
    if let Some(topic) = note.get("topic").and_then(Value::as_str) {
        return Announced::Message(Sent {
            topic: topic.to_string(),
            event: note
                .get("event")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            // Private unless the row said otherwise, which is the
            // default on `realtime.send()` itself.
            private: note.get("private").and_then(Value::as_bool).unwrap_or(true),
            payload: json_payload(&note.get("payload").cloned().unwrap_or(json!({}))),
        });
    }
    match note.get("id").and_then(Value::as_str) {
        Some(id) => Announced::Row(id.to_string()),
        None => Announced::Nothing,
    }
}

/// What one notification is about, reading the row back when the
/// notification was only an id.
///
/// The read back happens on the connection the notification arrived
/// on, which is one the server is already holding, so it costs a round
/// trip and not a connection.
async fn sent_message(
    client: &tokio_postgres::Client,
    note: &str,
) -> Result<Option<Sent>, crate::sql::Error> {
    let id = match announced(note) {
        Announced::Message(sent) => return Ok(Some(sent)),
        Announced::Row(id) => id,
        Announced::Nothing => return Ok(None),
    };
    let rows = client
        .query(
            // Through text on the way to uuid, because a bare
            // `$1::uuid` would have postgres decide the parameter is
            // a uuid and this one is a string.
            "select topic, event, private, payload::text, binary_payload \
             from realtime.messages where id = $1::text::uuid",
            &[&id],
        )
        .await?;
    // The row may be gone already, which a sweep in the same second is
    // enough to do, and a message nobody can read is a message nobody
    // hears.
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let event: Option<String> = row.get(1);
    let private: Option<bool> = row.get(2);
    let payload: Option<String> = row.get(3);
    let binary: Option<Vec<u8>> = row.get(4);
    Ok(Some(Sent {
        topic: row.get(0),
        event: event.unwrap_or_default(),
        private: private.unwrap_or(true),
        payload: match binary {
            Some(bytes) => (Encoding::Binary, bytes),
            None => (
                Encoding::Json,
                payload.unwrap_or_else(|| "{}".into()).into_bytes(),
            ),
        },
    }))
}

/// One parameter out of the connect url.
fn query(uri: &Uri, name: &str) -> Option<String> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| value.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_version_comes_off_the_connect_url() {
        let uri: Uri = "/realtime/v1/websocket?apikey=k&vsn=1.0.0".parse().unwrap();
        assert_eq!(query(&uri, "vsn").as_deref(), Some("1.0.0"));
        assert_eq!(query(&uri, "apikey").as_deref(), Some("k"));
        assert_eq!(query(&uri, "log_level"), None);
        let bare: Uri = "/realtime/v1/websocket".parse().unwrap();
        assert_eq!(query(&bare, "vsn"), None);
    }

    /// The notification the trigger writes when the message fits in
    /// one, which is every ordinary send.
    #[test]
    fn a_whole_message_in_a_notification_needs_no_row() {
        let note = r#"{"id":"1","topic":"lobby","event":"greeting","private":true,
                       "payload":{"hello":"room"}}"#;
        let Announced::Message(sent) = announced(note) else {
            panic!("the whole message was there");
        };
        assert_eq!(sent.topic, "lobby");
        assert_eq!(sent.event, "greeting");
        assert!(sent.private);
        assert!(matches!(sent.payload.0, Encoding::Json));
        assert!(String::from_utf8_lossy(&sent.payload.1).contains("hello"));
    }

    /// A send that said it was not private is for the public room of
    /// that name, and a notification with nothing to say about it is
    /// for the private one, because that is what `realtime.send()`
    /// defaults to.
    #[test]
    fn a_send_is_private_unless_the_row_says_otherwise() {
        let public = r#"{"id":"1","topic":"lobby","event":"e","private":false,"payload":{}}"#;
        let Announced::Message(sent) = announced(public) else {
            panic!("a message");
        };
        assert!(!sent.private);

        let quiet = r#"{"id":"1","topic":"lobby","event":"e","payload":{}}"#;
        let Announced::Message(sent) = announced(quiet) else {
            panic!("a message");
        };
        assert!(sent.private);
    }

    #[test]
    fn an_id_on_its_own_is_a_row_to_go_and_read() {
        let Announced::Row(id) = announced(r#"{"id":"a3e1"}"#) else {
            panic!("an id was all there was");
        };
        assert_eq!(id, "a3e1");
    }

    #[test]
    fn a_notification_this_server_cannot_read_is_left_alone() {
        assert!(matches!(announced("not json at all"), Announced::Nothing));
        assert!(matches!(announced("{}"), Announced::Nothing));
        assert!(matches!(announced("[1,2,3]"), Announced::Nothing));
    }
}
