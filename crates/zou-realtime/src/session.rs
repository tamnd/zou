//! One socket's side of the conversation, with no socket in it.
//!
//! Everything a connection does is here as a function from a frame to
//! a list of actions, so the whole protocol can be tested without a
//! port, a runtime, or a client. The transport reads a message, hands
//! it over, and does what comes back: send these bytes, start or stop
//! carrying a topic, hand this broadcast to the hub, close.
//!
//! What this refuses is as much of the point as what it answers. A
//! join asking for postgres changes is told they are not built rather
//! than joined quietly and left silent, because a channel that says
//! SUBSCRIBED and never delivers a row is worse to debug than one that
//! failed.

use std::collections::HashMap;

use serde_json::{Value, json};

use crate::frame::{BinaryBroadcast, Encoding, Frame, Vsn};

/// Who the socket is, once a token has been read.
#[derive(Debug, Clone, PartialEq)]
pub struct Identity {
    /// The postgres role the claims name, `anon` when they name none.
    pub role: String,
    /// The whole claim set, for the authorization this does not do yet.
    pub claims: Value,
}

/// What can check a token. The realtime tier does not know how a
/// project signs, and should not: it is handed something that does.
pub trait Tokens: Send + Sync {
    /// The identity in `token`, or why it was refused. The message
    /// reaches the client, so it should say what a client can act on.
    fn verify(&self, token: &str) -> Result<Identity, String>;
}

/// What a client asked for in its join.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Config {
    /// Whether the sender of a broadcast hears its own message back.
    pub broadcast_self: bool,
    /// Whether a broadcast push is answered at all.
    pub broadcast_ack: bool,
    /// Whether this socket hears about presence on this channel. It
    /// does not decide whether this socket can be seen: a client that
    /// tracks is visible to everyone whatever it asked for, this only
    /// says whether the state and the diffs come back to it.
    pub presence: bool,
    /// What this socket asked to be known by on this channel, if it
    /// asked for anything. Nothing means the socket's own name.
    pub presence_key: Option<String>,
    /// Whether joining is subject to the project's own authorization.
    pub private: bool,
    /// The postgres changes the client wants, unread beyond its
    /// length, since this refuses them all.
    pub postgres_changes: usize,
}

impl Config {
    /// Read the `config` object out of a join payload. Everything in
    /// it is optional and everything missing takes realtime-js's own
    /// default, which is a public channel with broadcast on, no echo,
    /// no ack and no presence.
    pub fn of(payload: &Value) -> Config {
        let config = payload.get("config");
        let flag = |group: &str, name: &str| -> bool {
            config
                .and_then(|c| c.get(group))
                .and_then(|g| g.get(name))
                .and_then(Value::as_bool)
                .unwrap_or(false)
        };
        Config {
            broadcast_self: flag("broadcast", "self"),
            broadcast_ack: flag("broadcast", "ack"),
            presence: flag("presence", "enabled"),
            presence_key: config
                .and_then(|c| c.get("presence"))
                .and_then(|p| p.get("key"))
                .and_then(Value::as_str)
                .filter(|key| !key.is_empty())
                .map(str::to_string),
            private: config
                .and_then(|c| c.get("private"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            postgres_changes: config
                .and_then(|c| c.get("postgres_changes"))
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0),
        }
    }
}

/// A message a broadcast turns into, on its way to the other members
/// of a topic.
#[derive(Debug, Clone, PartialEq)]
pub struct Fanout {
    pub topic: String,
    pub push: BinaryBroadcast,
    /// Whether the socket that sent it hears it back, which is the
    /// sender's `broadcast.self` and nobody else's.
    pub to_self: bool,
}

/// What goes down a topic to the sockets on it.
#[derive(Debug, Clone, PartialEq)]
pub enum Sent {
    /// A broadcast from one socket to the others.
    Broadcast(Fanout),
    /// A change to who is on the topic, which everyone on it hears,
    /// the socket that caused it included: a client applies its own
    /// track through the same diff everybody else does, so there is
    /// one code path keeping every copy of the state level.
    Diff { topic: String, payload: Value },
}

impl Sent {
    pub fn topic(&self) -> &str {
        match self {
            Sent::Broadcast(fan) => &fan.topic,
            Sent::Diff { topic, .. } => topic,
        }
    }
}

/// What the transport should do next.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Down this socket, as text.
    Text(String),
    /// Down this socket, as bytes.
    Binary(Vec<u8>),
    /// Start carrying this topic's broadcasts to this socket.
    Carry(String),
    /// Stop.
    Drop(String),
    /// Hand this to everyone carrying the topic.
    Fan(Fanout),
    /// Put this socket on the topic's presence, or move it if it is
    /// already there, and tell the topic.
    Track {
        topic: String,
        key: Option<String>,
        payload: Value,
    },
    /// Take it off, and tell the topic.
    Untrack(String),
    /// Send this socket everyone who is on the topic right now, which
    /// only the hub knows.
    State(String),
}

/// One connection.
pub struct Session {
    vsn: Vsn,
    /// Who the socket said it was on connect. A join with no token of
    /// its own runs as this.
    identity: Identity,
    /// The channels this socket is on and what each asked for.
    channels: HashMap<String, Config>,
}

impl Session {
    /// A socket that has just finished its handshake. `identity` is
    /// whatever the connect url and headers proved, which the http
    /// layer has already checked, since a socket that gets this far
    /// passed the same apikey gate every other request does.
    pub fn new(vsn: Vsn, identity: Identity) -> Session {
        Session {
            vsn,
            identity,
            channels: HashMap::new(),
        }
    }

    /// The role this socket is currently acting as, which changes when
    /// a client refreshes its token mid connection.
    pub fn role(&self) -> &str {
        &self.identity.role
    }

    /// Whether this socket is on `topic`.
    pub fn on(&self, topic: &str) -> bool {
        self.channels.contains_key(topic)
    }

    /// A text message arrived.
    pub fn text(&mut self, text: &str, tokens: &dyn Tokens) -> Vec<Action> {
        let Some(frame) = Frame::decode(text) else {
            // Phoenix has nowhere to put this: there is no ref to
            // reply to and no topic to error on. Upstream logs and
            // carries on, and so does this.
            log::debug!(
                "realtime: a message that is not a frame, {} bytes",
                text.len()
            );
            return Vec::new();
        };
        self.frame(frame, tokens)
    }

    /// A binary message arrived, which is a broadcast push and nothing
    /// else. Every other event is text.
    pub fn binary(&mut self, bytes: &[u8]) -> Vec<Action> {
        let Some(push) = BinaryBroadcast::decode(bytes) else {
            log::debug!(
                "realtime: a binary message that is not a push, {} bytes",
                bytes.len()
            );
            return Vec::new();
        };
        let topic = push.topic.clone();
        let Some(config) = self.channels.get(&topic) else {
            // A push to a channel this socket never joined. There is a
            // ref on it, so the client is owed an answer rather than
            // silence.
            let asked = Frame {
                join_ref: Some(push.join_ref.clone()),
                reference: Some(push.reference.clone()),
                topic,
                event: "broadcast".into(),
                payload: json!({}),
            };
            return vec![self.send(Frame::error(
                &asked,
                "this socket has not joined that topic",
            ))];
        };
        let ack = config.broadcast_ack;
        let to_self = config.broadcast_self;
        let asked = Frame {
            join_ref: Some(push.join_ref.clone()),
            reference: Some(push.reference.clone()),
            topic: topic.clone(),
            event: "broadcast".into(),
            payload: json!({}),
        };
        let mut actions = vec![Action::Fan(Fanout {
            topic,
            push,
            to_self,
        })];
        if ack {
            actions.push(self.send(Frame::ok(&asked)));
        }
        actions
    }

    /// Everything a decoded frame can be.
    fn frame(&mut self, frame: Frame, tokens: &dyn Tokens) -> Vec<Action> {
        match frame.event.as_str() {
            // The socket's own keepalive, which belongs to no channel.
            "heartbeat" => vec![self.send(Frame::ok(&frame))],
            "phx_join" => self.join(frame, tokens),
            "phx_leave" => self.leave(frame),
            "access_token" => self.refresh(frame, tokens),
            "broadcast" => self.broadcast(frame),
            "presence" => self.presence(frame),
            other => {
                log::debug!("realtime: nothing answers {other} on {}", frame.topic);
                vec![self.send(Frame::error(
                    &frame,
                    format!("{other} is not an event this server answers"),
                ))]
            }
        }
    }

    fn join(&mut self, frame: Frame, tokens: &dyn Tokens) -> Vec<Action> {
        if !frame.topic.starts_with("realtime:") {
            return vec![self.send(Frame::error(
                &frame,
                "a channel topic is realtime: and then a name",
            ))];
        }
        // A join may carry a token of its own, and usually does: the
        // client puts the user's access token there so the channel
        // runs as the user rather than as the key the socket opened
        // with.
        if let Some(token) = frame.payload.get("access_token").and_then(Value::as_str) {
            match tokens.verify(token) {
                Ok(identity) => self.identity = identity,
                Err(why) => return vec![self.send(Frame::error(&frame, why))],
            }
        }
        let config = Config::of(&frame.payload);
        if config.private {
            return vec![self.send(Frame::error(
                &frame,
                "private channels are not implemented yet, tracked in tamnd/zou#4",
            ))];
        }
        if config.postgres_changes > 0 {
            return vec![self.send(Frame::error(
                &frame,
                "postgres changes are not implemented yet, tracked in tamnd/zou#4",
            ))];
        }
        let topic = frame.topic.clone();
        let wants_presence = config.presence;
        self.channels.insert(topic.clone(), config);
        // An ok with nothing in it, and the nothing matters: a reply
        // carrying a postgres_changes list is checked against the
        // client's own bindings, and one carrying none is read as
        // subscribed.
        let mut actions = vec![Action::Carry(topic.clone()), self.send(Frame::ok(&frame))];
        if wants_presence {
            // After the reply and after the topic is being carried, in
            // that order: the state is a snapshot and the diffs that
            // follow it are only complete if the socket was already
            // listening when it was taken.
            actions.push(Action::State(topic));
        }
        actions
    }

    fn leave(&mut self, frame: Frame) -> Vec<Action> {
        if self.channels.remove(&frame.topic).is_none() {
            return vec![self.send(Frame::error(&frame, "this socket is not on that topic"))];
        }
        // Untracking before the drop, because the diff has to go out
        // while this socket is still on the topic for the others to be
        // told it left.
        vec![
            Action::Untrack(frame.topic.clone()),
            Action::Drop(frame.topic.clone()),
            self.send(Frame::ok(&frame)),
        ]
    }

    /// `track` and `untrack`, which is a client saying it is here and
    /// then saying it is not.
    ///
    /// Both are answered, because the client awaits the reply, and
    /// both are visible to the whole topic whatever this socket asked
    /// for in its join: `presence.enabled` decides what comes back to
    /// this socket, not whether this socket can be seen.
    fn presence(&mut self, frame: Frame) -> Vec<Action> {
        let Some(config) = self.channels.get(&frame.topic) else {
            return vec![self.send(Frame::error(
                &frame,
                "this socket has not joined that topic",
            ))];
        };
        let event = frame
            .payload
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        match event.as_str() {
            "track" => {
                let payload = frame
                    .payload
                    .get("payload")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                if !payload.is_object() {
                    return vec![self.send(Frame::error(
                        &frame,
                        "a presence track carries an object as its payload",
                    ))];
                }
                let track = Action::Track {
                    topic: frame.topic.clone(),
                    key: config.presence_key.clone(),
                    payload,
                };
                vec![track, self.send(Frame::ok(&frame))]
            }
            "untrack" => vec![
                Action::Untrack(frame.topic.clone()),
                self.send(Frame::ok(&frame)),
            ],
            other => vec![self.send(Frame::error(
                &frame,
                format!("{other} is not a presence event, which is track or untrack"),
            ))],
        }
    }

    /// The topic's presence as this socket should see it, which the
    /// hub had to be asked for because it is the only thing that knows
    /// who else is here.
    pub fn state(&self, topic: &str, state: Value) -> Action {
        self.send(Frame::push(topic, "presence_state", state))
    }

    /// A client whose access token was about to expire has a new one.
    ///
    /// Refusing it takes the socket's channels down rather than
    /// leaving them running on the old claims, because the old token
    /// is the one that was about to stop being true.
    fn refresh(&mut self, frame: Frame, tokens: &dyn Tokens) -> Vec<Action> {
        let token = frame
            .payload
            .get("access_token")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match tokens.verify(token) {
            Ok(identity) => {
                self.identity = identity;
                vec![self.send(Frame::ok(&frame))]
            }
            Err(why) => {
                let topics: Vec<String> = self.channels.keys().cloned().collect();
                let mut actions = vec![self.send(Frame::error(&frame, why))];
                for topic in topics {
                    self.channels.remove(&topic);
                    actions.push(self.send(Frame::channel_error(&topic)));
                    actions.push(Action::Untrack(topic.clone()));
                    actions.push(Action::Drop(topic));
                }
                actions
            }
        }
    }

    /// A broadcast that arrived as json, which is what a client that
    /// is sending bytes rather than an event name does, and what every
    /// client did before the binary encoding existed.
    fn broadcast(&mut self, frame: Frame) -> Vec<Action> {
        let Some(config) = self.channels.get(&frame.topic) else {
            return vec![self.send(Frame::error(
                &frame,
                "this socket has not joined that topic",
            ))];
        };
        let ack = config.broadcast_ack;
        let to_self = config.broadcast_self;
        let Some(push) = BinaryBroadcast::from_frame(&frame) else {
            return vec![self.send(Frame::error(
                &frame,
                "a broadcast carries an event name and a payload",
            ))];
        };
        let mut actions = vec![Action::Fan(Fanout {
            topic: frame.topic.clone(),
            push,
            to_self,
        })];
        if ack {
            actions.push(self.send(Frame::ok(&frame)));
        }
        actions
    }

    /// Something from the hub, on its way down this socket.
    ///
    /// `mine` is whether this socket is the one that caused it, which
    /// is the only thing `broadcast.self` decides. A presence diff
    /// ignores it: the socket that tracked applies its own change out
    /// of the diff like everybody else does.
    pub fn deliver(&self, sent: &Sent, mine: bool) -> Option<Action> {
        match sent {
            Sent::Broadcast(fan) => self.broadcast_down(fan, mine),
            Sent::Diff { topic, payload } => {
                // A socket that did not ask for presence is not sent
                // any: it is still visible to the rest of the topic,
                // it just has nothing bound that would read this.
                self.channels.get(topic).filter(|c| c.presence)?;
                Some(self.send(Frame::push(topic, "presence_diff", payload.clone())))
            }
        }
    }

    fn broadcast_down(&self, fan: &Fanout, mine: bool) -> Option<Action> {
        if mine && !fan.to_self {
            return None;
        }
        if !self.channels.contains_key(&fan.topic) {
            return None;
        }
        match self.vsn {
            // The binary encoding is the current client's, and it
            // carries bytes as bytes.
            Vsn::V2 => Some(Action::Binary(fan.push.encode())),
            // A socket on the old version has a serializer that has
            // never seen a binary frame, so it gets the json shape,
            // and a payload that is not json cannot be sent to it at
            // all.
            Vsn::V1 => match fan.push.encoding {
                Encoding::Json => fan.push.as_frame().map(|f| self.send(f)),
                Encoding::Binary => {
                    log::debug!(
                        "realtime: a binary broadcast has nowhere to go on a 1.0.0 socket, {}",
                        fan.topic
                    );
                    None
                }
            },
        }
    }

    fn send(&self, frame: Frame) -> Action {
        Action::Text(frame.encode(self.vsn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A verifier that says yes to one token and no to everything
    /// else, which is every case this needs.
    struct OneToken;

    impl Tokens for OneToken {
        fn verify(&self, token: &str) -> Result<Identity, String> {
            if token == "good" {
                Ok(Identity {
                    role: "authenticated".into(),
                    claims: json!({"role": "authenticated", "sub": "u1"}),
                })
            } else {
                Err("invalid claim: missing sub claim".into())
            }
        }
    }

    fn socket() -> Session {
        Session::new(
            Vsn::V2,
            Identity {
                role: "anon".into(),
                claims: json!({"role": "anon"}),
            },
        )
    }

    fn text_of(action: &Action) -> Frame {
        match action {
            Action::Text(text) => Frame::decode(text).expect("a frame"),
            other => panic!("{other:?} is not text"),
        }
    }

    fn join(session: &mut Session, config: &str) -> Vec<Action> {
        session.text(
            &format!(r#"["1","1","realtime:room","phx_join",{config}]"#),
            &OneToken,
        )
    }

    #[test]
    fn a_join_is_carried_and_answered_in_that_order() {
        let mut session = socket();
        let actions = join(&mut session, r#"{"config":{}}"#);
        assert_eq!(actions[0], Action::Carry("realtime:room".into()));
        let reply = text_of(&actions[1]);
        assert_eq!(reply.event, "phx_reply");
        assert_eq!(reply.payload, json!({"status": "ok", "response": {}}));
        assert!(session.on("realtime:room"));
    }

    #[test]
    fn a_join_carrying_a_token_runs_as_that_token() {
        let mut session = socket();
        assert_eq!(session.role(), "anon");
        join(&mut session, r#"{"config":{},"access_token":"good"}"#);
        assert_eq!(session.role(), "authenticated");
    }

    #[test]
    fn a_join_carrying_a_token_that_does_not_verify_is_refused() {
        let mut session = socket();
        let actions = join(&mut session, r#"{"config":{},"access_token":"stale"}"#);
        let reply = text_of(&actions[0]);
        assert_eq!(reply.payload["status"], "error");
        assert_eq!(
            reply.payload["response"]["reason"],
            "invalid claim: missing sub claim"
        );
        assert!(!session.on("realtime:room"));
    }

    #[test]
    fn what_is_not_built_says_so_rather_than_joining_and_going_quiet() {
        for (config, said) in [
            (
                r#"{"config":{"postgres_changes":[{"event":"*"}]}}"#,
                "postgres changes",
            ),
            (r#"{"config":{"private":true}}"#, "private channels"),
        ] {
            let mut session = socket();
            let actions = join(&mut session, config);
            let reply = text_of(&actions[0]);
            assert_eq!(reply.payload["status"], "error", "{config}");
            let reason = reply.payload["response"]["reason"].as_str().unwrap();
            assert!(reason.starts_with(said), "{reason}");
            assert!(reason.contains("tamnd/zou#4"), "{reason}");
            assert!(!session.on("realtime:room"), "{config}");
        }
    }

    #[test]
    fn a_topic_that_is_not_a_channel_is_refused() {
        let mut session = socket();
        let actions = session.text(r#"["1","1","room","phx_join",{}]"#, &OneToken);
        assert_eq!(text_of(&actions[0]).payload["status"], "error");
    }

    #[test]
    fn a_heartbeat_is_answered_and_joins_nothing() {
        let mut session = socket();
        let actions = session.text(r#"[null,"9","phoenix","heartbeat",{}]"#, &OneToken);
        assert_eq!(actions.len(), 1);
        let reply = text_of(&actions[0]);
        assert_eq!(reply.topic, "phoenix");
        assert_eq!(reply.reference.as_deref(), Some("9"));
        assert_eq!(reply.payload["status"], "ok");
    }

    #[test]
    fn a_leave_takes_the_topic_off_the_socket() {
        let mut session = socket();
        join(&mut session, r#"{"config":{}}"#);
        let actions = session.text(r#"["1","2","realtime:room","phx_leave",{}]"#, &OneToken);
        assert_eq!(actions[0], Action::Untrack("realtime:room".into()));
        assert_eq!(actions[1], Action::Drop("realtime:room".into()));
        assert_eq!(text_of(&actions[2]).payload["status"], "ok");
        assert!(!session.on("realtime:room"));
        // And a second leave has nothing to take off.
        let again = session.text(r#"["1","3","realtime:room","phx_leave",{}]"#, &OneToken);
        assert_eq!(text_of(&again[0]).payload["status"], "error");
    }

    #[test]
    fn a_refreshed_token_changes_who_the_socket_is() {
        let mut session = socket();
        join(&mut session, r#"{"config":{}}"#);
        let actions = session.text(
            r#"["1","4","realtime:room","access_token",{"access_token":"good"}]"#,
            &OneToken,
        );
        assert_eq!(text_of(&actions[0]).payload["status"], "ok");
        assert_eq!(session.role(), "authenticated");
        assert!(session.on("realtime:room"));
    }

    #[test]
    fn a_refresh_that_does_not_verify_takes_the_channels_down() {
        let mut session = socket();
        join(&mut session, r#"{"config":{}}"#);
        let actions = session.text(
            r#"["1","4","realtime:room","access_token",{"access_token":"expired"}]"#,
            &OneToken,
        );
        assert_eq!(text_of(&actions[0]).payload["status"], "error");
        assert_eq!(text_of(&actions[1]).event, "phx_error");
        assert_eq!(actions[2], Action::Untrack("realtime:room".into()));
        assert_eq!(actions[3], Action::Drop("realtime:room".into()));
        assert!(!session.on("realtime:room"));
    }

    #[test]
    fn a_broadcast_goes_to_the_topic_and_is_not_acked_unless_asked() {
        let mut session = socket();
        join(&mut session, r#"{"config":{}}"#);
        let actions = session.text(
            r#"["1","5","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":1}}]"#,
            &OneToken,
        );
        assert_eq!(actions.len(), 1);
        let Action::Fan(fan) = &actions[0] else {
            panic!("{:?} is not a fan out", actions[0])
        };
        assert_eq!(fan.topic, "realtime:room");
        assert_eq!(fan.push.event, "cursor");
        assert!(!fan.to_self);
    }

    #[test]
    fn an_ack_is_sent_when_the_join_asked_for_one() {
        let mut session = socket();
        join(
            &mut session,
            r#"{"config":{"broadcast":{"ack":true,"self":true}}}"#,
        );
        let actions = session.text(
            r#"["1","5","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{}}]"#,
            &OneToken,
        );
        assert_eq!(actions.len(), 2);
        let Action::Fan(fan) = &actions[0] else {
            panic!("not a fan out")
        };
        assert!(fan.to_self);
        assert_eq!(text_of(&actions[1]).payload["status"], "ok");
    }

    #[test]
    fn a_broadcast_to_a_topic_nobody_joined_is_refused_rather_than_dropped() {
        let mut session = socket();
        let actions = session.text(
            r#"["1","5","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{}}]"#,
            &OneToken,
        );
        assert_eq!(text_of(&actions[0]).payload["status"], "error");
    }

    #[test]
    fn a_binary_push_fans_out_the_same_way_a_json_one_does() {
        let mut session = socket();
        join(&mut session, r#"{"config":{}}"#);
        let mut push = vec![3u8, 1, 1, 13, 6, 0, 1];
        push.extend_from_slice(b"11realtime:roomcursor");
        push.extend_from_slice(br#"{"x":1}"#);
        let actions = session.binary(&push);
        let Action::Fan(fan) = &actions[0] else {
            panic!("{:?} is not a fan out", actions[0])
        };
        assert_eq!(fan.push.event, "cursor");
        assert_eq!(fan.push.payload, br#"{"x":1}"#);
    }

    #[test]
    fn a_delivery_reaches_the_others_and_the_sender_only_if_it_asked() {
        let mut sender = socket();
        join(&mut sender, r#"{"config":{}}"#);
        let mut other = socket();
        join(&mut other, r#"{"config":{}}"#);
        let mut third = socket();
        join(&mut third, r#"{"config":{}}"#);
        let actions = sender.text(
            r#"["1","5","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":1}}]"#,
            &OneToken,
        );
        let Action::Fan(fan) = &actions[0] else {
            panic!("not a fan out")
        };
        let sent = Sent::Broadcast(fan.clone());
        assert!(sender.deliver(&sent, true).is_none());
        assert!(matches!(
            other.deliver(&sent, false),
            Some(Action::Binary(_))
        ));
        // A socket that left hears nothing more.
        third.text(r#"["1","6","realtime:room","phx_leave",{}]"#, &OneToken);
        assert!(third.deliver(&sent, false).is_none());
    }

    #[test]
    fn an_old_socket_is_delivered_the_shape_its_serializer_reads() {
        let mut old = Session::new(
            Vsn::V1,
            Identity {
                role: "anon".into(),
                claims: json!({}),
            },
        );
        old.text(
            r#"{"join_ref":"1","ref":"1","topic":"realtime:room","event":"phx_join","payload":{"config":{}}}"#,
            &OneToken,
        );
        let mut sender = socket();
        join(&mut sender, r#"{"config":{}}"#);
        let actions = sender.text(
            r#"["1","5","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":1}}]"#,
            &OneToken,
        );
        let Action::Fan(fan) = &actions[0] else {
            panic!("not a fan out")
        };
        let Some(Action::Text(text)) = old.deliver(&Sent::Broadcast(fan.clone()), false) else {
            panic!("a 1.0.0 socket takes text")
        };
        let down = Frame::decode(&text).unwrap();
        assert_eq!(down.event, "broadcast");
        assert_eq!(down.payload["event"], "cursor");
        assert_eq!(down.payload["payload"], json!({"x": 1}));
    }

    #[test]
    fn a_join_that_wants_presence_asks_for_the_state_and_one_that_does_not_does_not() {
        let mut session = socket();
        let actions = join(&mut session, r#"{"config":{"presence":{"enabled":true}}}"#);
        assert_eq!(actions[0], Action::Carry("realtime:room".into()));
        assert_eq!(text_of(&actions[1]).payload["status"], "ok");
        assert_eq!(actions[2], Action::State("realtime:room".into()));

        let mut quiet = socket();
        let actions = join(&mut quiet, r#"{"config":{}}"#);
        assert_eq!(actions.len(), 2);
    }

    #[test]
    fn a_track_carries_the_key_the_client_asked_for_and_nothing_when_it_did_not() {
        let mut named = socket();
        join(
            &mut named,
            r#"{"config":{"presence":{"enabled":true,"key":"u1"}}}"#,
        );
        let actions = named.text(
            r#"["1","5","realtime:room","presence",{"type":"presence","event":"track","payload":{"at":"now"}}]"#,
            &OneToken,
        );
        assert_eq!(
            actions[0],
            Action::Track {
                topic: "realtime:room".into(),
                key: Some("u1".into()),
                payload: json!({"at": "now"}),
            }
        );
        assert_eq!(text_of(&actions[1]).payload["status"], "ok");

        // An empty key is what the client sends when nobody set one,
        // and it means the socket names itself rather than everybody
        // sharing a key of "".
        let mut anonymous = socket();
        join(
            &mut anonymous,
            r#"{"config":{"presence":{"enabled":true,"key":""}}}"#,
        );
        let actions = anonymous.text(
            r#"["1","5","realtime:room","presence",{"type":"presence","event":"track","payload":{}}]"#,
            &OneToken,
        );
        assert!(matches!(&actions[0], Action::Track { key: None, .. }));
    }

    #[test]
    fn a_socket_can_be_seen_without_asking_to_see() {
        // presence.enabled is off, which realtime-js sets when the
        // client has no presence bindings, and track still works: the
        // flag decides what comes back, not whether this is visible.
        let mut session = socket();
        join(&mut session, r#"{"config":{}}"#);
        let actions = session.text(
            r#"["1","5","realtime:room","presence",{"type":"presence","event":"track","payload":{"at":"now"}}]"#,
            &OneToken,
        );
        assert!(matches!(&actions[0], Action::Track { .. }));
        // And the diff that comes of it has nowhere to go on this
        // socket, since nothing here is bound to presence.
        let diff = Sent::Diff {
            topic: "realtime:room".into(),
            payload: json!({"joins": {}, "leaves": {}}),
        };
        assert!(session.deliver(&diff, true).is_none());
    }

    #[test]
    fn a_diff_reaches_the_sockets_that_asked_for_presence_including_the_one_that_moved() {
        let mut session = socket();
        join(&mut session, r#"{"config":{"presence":{"enabled":true}}}"#);
        let diff = Sent::Diff {
            topic: "realtime:room".into(),
            payload: json!({"joins": {"u1": {"metas": [{"phx_ref": "3"}]}}, "leaves": {}}),
        };
        let Some(Action::Text(text)) = session.deliver(&diff, true) else {
            panic!("a socket that asked for presence hears its own track back")
        };
        let down = Frame::decode(&text).unwrap();
        assert_eq!(down.event, "presence_diff");
        assert_eq!(down.topic, "realtime:room");
        assert_eq!(down.payload["joins"]["u1"]["metas"][0]["phx_ref"], "3");
        // And a topic this socket is not on is not delivered at all.
        let elsewhere = Sent::Diff {
            topic: "realtime:lobby".into(),
            payload: json!({}),
        };
        assert!(session.deliver(&elsewhere, false).is_none());
    }

    #[test]
    fn a_presence_push_that_makes_no_sense_is_answered_rather_than_dropped() {
        let mut session = socket();
        // A topic this socket never joined.
        let actions = session.text(
            r#"["1","5","realtime:room","presence",{"type":"presence","event":"track","payload":{}}]"#,
            &OneToken,
        );
        assert_eq!(text_of(&actions[0]).payload["status"], "error");

        join(&mut session, r#"{"config":{"presence":{"enabled":true}}}"#);
        for (push, said) in [
            (
                r#"{"type":"presence","event":"wave"}"#,
                "is not a presence event",
            ),
            (
                r#"{"type":"presence","event":"track","payload":"here"}"#,
                "carries an object",
            ),
        ] {
            let actions = session.text(
                &format!(r#"["1","6","realtime:room","presence",{push}]"#),
                &OneToken,
            );
            let reply = text_of(&actions[0]);
            assert_eq!(reply.payload["status"], "error", "{push}");
            assert!(
                reply.payload["response"]["reason"]
                    .as_str()
                    .unwrap()
                    .contains(said),
                "{push}"
            );
        }
    }

    #[test]
    fn an_untrack_is_answered_and_asks_for_the_removal() {
        let mut session = socket();
        join(&mut session, r#"{"config":{"presence":{"enabled":true}}}"#);
        let actions = session.text(
            r#"["1","7","realtime:room","presence",{"type":"presence","event":"untrack"}]"#,
            &OneToken,
        );
        assert_eq!(actions[0], Action::Untrack("realtime:room".into()));
        assert_eq!(text_of(&actions[1]).payload["status"], "ok");
    }

    #[test]
    fn an_event_nobody_answers_is_said_out_loud() {
        let mut session = socket();
        let actions = session.text(r#"["1","7","realtime:room","dance",{}]"#, &OneToken);
        let reply = text_of(&actions[0]);
        assert_eq!(reply.payload["status"], "error");
        assert!(
            reply.payload["response"]["reason"]
                .as_str()
                .unwrap()
                .contains("dance")
        );
    }

    #[test]
    fn a_message_that_is_not_a_frame_is_ignored_rather_than_answered() {
        let mut session = socket();
        assert!(session.text("{", &OneToken).is_empty());
        assert!(session.binary(&[9, 9, 9]).is_empty());
    }
}
