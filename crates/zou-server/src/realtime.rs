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

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::Uri;
use axum::response::Response;
use tokio::sync::broadcast::error::RecvError;
use zou_realtime::{Action, Delivery, Hub, Identity, Session, SocketId, Tokens, Vsn};

use crate::{App, AuthContext, json_body};

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
        let tokens = ProjectTokens {
            app: Arc::clone(&app),
            anon_role,
        };
        run(socket, Session::new(vsn, identity), &app.hub, &tokens).await;
    })
}

/// One connection, until it goes.
async fn run(mut socket: WebSocket, mut session: Session, hub: &Hub, tokens: &dyn Tokens) {
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
        if !act(&mut socket, actions, &session, hub, me, &mut carrying).await {
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
    session: &Session,
    hub: &Hub,
    me: SocketId,
    carrying: &mut HashMap<String, tokio::sync::broadcast::Receiver<Delivery>>,
) -> bool {
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
        }
    }
    true
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
}
