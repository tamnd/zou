//! One socket's side of the conversation, with no socket in it.
//!
//! Everything a connection does is here as a function from a frame to
//! a list of actions, so the whole protocol can be tested without a
//! port, a runtime, or a client. The transport reads a message, hands
//! it over, and does what comes back: send these bytes, start or stop
//! carrying a topic, hand this broadcast to the hub, close.
//!
//! Two of those actions are questions rather than instructions: whether
//! a private channel may be joined, and what ids the database side gave
//! the subscriptions a join asked for. Both come back in through a
//! method, and in both cases the join waits, because a reply sent before
//! the answer would be a client told it is subscribed to something
//! nobody has arranged yet.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};

use crate::frame::{BinaryBroadcast, Encoding, Frame, Vsn};
use crate::limit::{Counters, Limits, Unlimited};

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
    /// The postgres changes the client asked for, exactly as it wrote
    /// them.
    ///
    /// Held as the client's own json rather than as anything parsed,
    /// because the join reply has to hand each one back the way it
    /// arrived. realtime-js compares the reply's list with its own
    /// bindings field by field and unsubscribes the channel when any of
    /// them differ, so a server that tidied a missing filter into a null
    /// on the way through would be a server whose subscriptions all
    /// fail. What the entries mean is somebody else's to work out.
    pub postgres_changes: Vec<Value>,
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
                .cloned()
                .unwrap_or_default(),
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
    /// Go and find out whether this socket is allowed to do this, and
    /// come back through [`Session::authorized`]. Nothing else this
    /// socket asked for happens until the answer arrives.
    Ask(Ask),
    /// Ask whatever is reading the database for these subscriptions,
    /// and come back through [`Session::watching`] with the id it gave
    /// each of them. The join is not answered until that arrives,
    /// because the reply is what carries the ids.
    Watch { topic: String, wants: Vec<Value> },
    /// Those subscriptions are finished, because the channel they were
    /// asked for on has gone.
    Unwatch(Vec<u64>),
    /// This socket is somebody else now, so whatever is checking its
    /// rows against a policy should be checking them against the new
    /// claims. Sent when a token is refreshed on a socket that is
    /// watching something, since a row is only allowed to reach whoever
    /// the client currently is.
    Rewatch,
    /// The socket itself is finished, once whatever is in front of
    /// this has been sent. Upstream reaches this by telling the
    /// transport to disconnect, and it is one refusal only: a project
    /// over its joins per second budget.
    Close,
}

/// A question about a private channel that only the project's own
/// database can answer.
///
/// The convention is Supabase's: a private channel is allowed or
/// refused by the policies the project wrote on `realtime.messages`,
/// with the room name in `realtime.topic()`. Reading them is io, which
/// this crate does not do, so the question comes out here and the
/// answer goes back in.
#[derive(Debug, Clone, PartialEq)]
pub struct Ask {
    /// The channel topic, `realtime:` and the name, which is what
    /// everything else here is keyed by. The name on its own is what
    /// the policies see.
    pub topic: String,
    /// What the answer is needed for.
    pub about: About,
}

impl Ask {
    /// The room name the policies are asked about, which is the topic
    /// without the prefix the socket carries.
    pub fn name(&self) -> &str {
        name_of(&self.topic)
    }
}

/// What a channel's `postgres_changes` list turned into, which is the
/// answer to an [`Action::Watch`].
///
/// The ids are one per entry the client asked for, in the order it
/// asked, because that is what the reply hands back and what the client
/// compares against its own bindings.
///
/// `refused` is the reason nothing is subscribed, in the wording
/// upstream uses, and it is a list that had one entry in it that could
/// not be subscribed rather than a list that failed. Upstream refuses
/// all of them together and says so once, naming the entry: a channel
/// that asked for two tables and is missing one hears about neither,
/// which is the same bargain as a join that cannot be half made. The
/// ids still go back, because the client checks the reply against its
/// bindings before it looks at anything else, and they name nothing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Watched {
    pub ids: Vec<u64>,
    pub refused: Option<String>,
}

/// What an [`Ask`] is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum About {
    /// Whether this socket may be on the topic at all, which is the
    /// read policies and is asked once at the join.
    Join,
    /// Whether it may send to the topic, which is the broadcast write
    /// policy.
    Broadcast,
    /// Whether it may be seen on the topic, which is the presence
    /// write policy.
    Presence,
}

/// What the policies said, one answer per extension, which is how
/// Supabase's own check reports them: a channel can be readable for
/// presence and not for broadcast, or the other way round.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Grant {
    pub broadcast: bool,
    pub presence: bool,
}

/// What the policies have said so far about one private channel.
#[derive(Debug, Clone, Copy, Default)]
struct Rights {
    /// Whether the presence read policy said yes, which decides
    /// whether this socket is sent the topic's presence at all.
    presence_read: bool,
    /// What the write policies said, once anything has needed them.
    /// Nothing means nobody has asked yet, which is the same laziness
    /// upstream has: a socket that only listens never costs a write
    /// check.
    writes: Option<Grant>,
}

/// Something held while the transport goes and asks.
#[derive(Debug)]
enum Pending {
    /// A join, waiting to know whether it is allowed to happen.
    Join(Frame),
    /// Something the socket asked to do, already turned into the
    /// actions that would do it, so that an allowed answer is a replay
    /// rather than a second pass through the same code.
    Do { asked: Frame, actions: Vec<Action> },
    /// A recheck after a new token, which nothing is waiting on and
    /// which only matters if the answer is no.
    Recheck,
    /// A join that is allowed to happen and is waiting for the ids of
    /// the postgres changes it asked for, because the reply is what
    /// hands them back.
    Watch(Frame),
}

/// The room name inside a channel topic.
fn name_of(topic: &str) -> &str {
    topic.strip_prefix("realtime:").unwrap_or(topic)
}

/// Which room in the hub a channel is on: its topic, and whether it
/// was joined as private.
///
/// A private channel and a public channel of the same name are two
/// rooms rather than one, which is upstream's rule and has to be. A
/// public channel is joined without any policy being read, so if the
/// two shared a room then anybody could join `room` publicly and hear
/// everything the policies on the private one were keeping them out
/// of. Two rooms is also what makes the private flag on a send mean
/// something: a message sent as private reaches the sockets that were
/// let in by the policies and nobody else.
pub fn room(topic: &str, private: bool) -> String {
    match private {
        true => format!("private:{topic}"),
        false => topic.to_string(),
    }
}

/// The channel topic a room is for, which is the room itself for a
/// public one. Unambiguous because a channel topic always starts with
/// `realtime:`, so nothing public can look private.
fn topic_of(room: &str) -> &str {
    room.strip_prefix("private:").unwrap_or(room)
}

/// What this project is allowed and what has been spent of it.
///
/// The numbers are the whole project's, so every session on one server
/// is handed the same one of these: two sockets sharing a project
/// share its budget, which is what makes a limit about the project
/// rather than about whoever connected first.
#[derive(Clone)]
pub struct Budget {
    pub limits: Limits,
    pub counters: Arc<dyn Counters>,
}

impl Default for Budget {
    /// The numbers a project takes when nothing set any, with nothing
    /// counting the two that need the whole server to count them. A
    /// session built this way still refuses a socket that holds too
    /// many channels or sends too big a message, since those it can
    /// answer alone.
    fn default() -> Budget {
        Budget {
            limits: Limits::default(),
            counters: Arc::new(Unlimited),
        }
    }
}

impl Budget {
    /// A project with nothing counted at all, which is what a test and
    /// an embedded server both want.
    pub fn none() -> Budget {
        Budget {
            limits: Limits::none(),
            counters: Arc::new(Unlimited),
        }
    }
}

/// One connection.
pub struct Session {
    vsn: Vsn,
    /// Who the socket said it was on connect. A join with no token of
    /// its own runs as this.
    identity: Identity,
    /// The channels this socket is on and what each asked for.
    channels: HashMap<String, Config>,
    /// The private ones among them, with whatever the policies have
    /// said so far. A public channel is not in here at all, so this
    /// map is also how the rest of this knows which channels are
    /// private.
    rights: HashMap<String, Rights>,
    /// What is waiting on an answer, at most one per topic: the
    /// transport asks one question and comes back with it before it
    /// reads the socket again, so a second question about the same
    /// topic cannot arrive in between.
    pending: HashMap<String, Pending>,
    /// The postgres change subscriptions each channel holds, by the ids
    /// whatever is reading the database gave them. This is both how a
    /// change that arrives finds the channel to go down and how a
    /// channel that leaves takes its own subscriptions with it.
    watched: HashMap<String, Vec<u64>>,
    /// What the project is allowed and what is counting it.
    budget: Budget,
}

impl Session {
    /// A socket that has just finished its handshake. `identity` is
    /// whatever the connect url and headers proved, which the http
    /// layer has already checked, since a socket that gets this far
    /// passed the same apikey gate every other request does.
    pub fn new(vsn: Vsn, identity: Identity) -> Session {
        Session::budgeted(vsn, identity, Budget::default())
    }

    /// The same, on a project whose limits somebody is counting.
    pub fn budgeted(vsn: Vsn, identity: Identity, budget: Budget) -> Session {
        Session {
            vsn,
            identity,
            channels: HashMap::new(),
            rights: HashMap::new(),
            pending: HashMap::new(),
            watched: HashMap::new(),
            budget,
        }
    }

    /// The role this socket is currently acting as, which changes when
    /// a client refreshes its token mid connection.
    pub fn role(&self) -> &str {
        &self.identity.role
    }

    /// Who the socket is now, which is what a policy check runs as.
    pub fn identity(&self) -> &Identity {
        &self.identity
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
        let room = room(&topic, config.private);
        let asked = Frame {
            join_ref: Some(push.join_ref.clone()),
            reference: Some(push.reference.clone()),
            topic: topic.clone(),
            event: "broadcast".into(),
            payload: json!({}),
        };
        if self.too_big(&push) {
            return self.oversized(&asked, ack);
        }
        let mut actions = vec![Action::Fan(Fanout {
            topic: room,
            push,
            to_self,
        })];
        if ack {
            actions.push(self.send(Frame::ok(&asked)));
        }
        self.gated(&topic, About::Broadcast, asked, actions)
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
        // The two budgets a join spends, before the token is read and
        // before the policies are asked: a socket that is already
        // holding too many channels and a project that is joining them
        // too fast are both refused whoever they turn out to be.
        let held = self.channels.len() as u64;
        let allowed = self.budget.limits.channels_per_client;
        if allowed > 0 && held >= allowed {
            return vec![self.send(Frame::error(
                &frame,
                "ChannelRateLimitReached: Too many channels",
            ))];
        }
        if !self.budget.counters.join() {
            // Upstream answers this one and then tells the transport
            // to disconnect, so the client is told why before the
            // socket goes rather than being dropped in silence.
            return vec![
                self.send(Frame::error(
                    &frame,
                    "ClientJoinRateLimitReached: Too many joins per second",
                )),
                Action::Close,
            ];
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
            // Nothing about this join happens until the project's own
            // policies have been read, so the whole frame is held and
            // the question goes out.
            let topic = frame.topic.clone();
            self.pending.insert(topic.clone(), Pending::Join(frame));
            return vec![Action::Ask(Ask {
                topic,
                about: About::Join,
            })];
        }
        self.joined(frame)
    }

    /// The join itself, once there is nothing left to check.
    fn joined(&mut self, frame: Frame) -> Vec<Action> {
        let config = Config::of(&frame.payload);
        let topic = frame.topic.clone();
        let room = room(&topic, config.private);
        let wants = config.postgres_changes.clone();
        self.channels.insert(topic.clone(), config);
        let mut actions = vec![Action::Carry(room)];
        if !wants.is_empty() {
            // The reply has to carry an id for every subscription and
            // nothing here knows one yet, so the frame waits the same
            // way a private join waits for its policies. Everything the
            // reply is followed by waits with it.
            self.pending.insert(topic.clone(), Pending::Watch(frame));
            actions.push(Action::Watch { topic, wants });
            return actions;
        }
        actions.push(self.send(Frame::ok(&frame)));
        actions.extend(self.after_reply(&topic));
        actions
    }

    /// What follows a join reply, once the reply itself has gone.
    ///
    /// The presence state is a snapshot and the diffs after it are only
    /// complete if the socket was already being sent the topic when it
    /// was taken, which is why this is after the carry and not before
    /// it. A private channel is sent presence only if the presence read
    /// policy said so, whatever the client asked for.
    fn after_reply(&self, topic: &str) -> Option<Action> {
        let config = self.channels.get(topic)?;
        let wanted = config.presence && self.readable_presence(topic);
        wanted.then(|| Action::State(room(topic, config.private)))
    }

    /// The ids whatever reads the database gave this channel's
    /// subscriptions, or why it gave none.
    ///
    /// The reply hands each one back to the client with the entry it
    /// came from, in the order the client asked, because that is what
    /// realtime-js checks it against: it walks its own bindings and the
    /// reply's list together, and a channel whose entries do not match
    /// field for field unsubscribes itself with a mismatch error. So
    /// what goes back is the client's own json with an id added and
    /// nothing else touched.
    pub fn watching(&mut self, topic: &str, made: Result<Watched, String>) -> Vec<Action> {
        let frame = match self.pending.remove(topic) {
            Some(Pending::Watch(frame)) => frame,
            // Something else is waiting on this topic, which is not
            // this answer's to take away.
            Some(other) => {
                self.pending.insert(topic.to_string(), other);
                return Vec::new();
            }
            None => {
                log::debug!("realtime: subscriptions for {topic} that nothing was waiting for");
                return Vec::new();
            }
        };
        let wants = match self.channels.get(topic) {
            Some(config) => config.postgres_changes.clone(),
            None => return Vec::new(),
        };
        let made = match made {
            Ok(made) if made.ids.len() == wants.len() => made,
            Ok(_) => {
                // Not a client's mistake and not something a client can
                // do anything about, but a reply that did not line up
                // would fail on the client as a mismatch, so it is said
                // plainly here instead.
                return self.unsubscribed(&frame, "the subscriptions do not match what was asked");
            }
            Err(why) => return self.unsubscribed(&frame, why),
        };
        let echoed: Vec<Value> = wants
            .iter()
            .zip(&made.ids)
            .map(|(want, id)| {
                let mut entry = want.clone();
                match entry.as_object_mut() {
                    Some(object) => {
                        object.insert("id".into(), json!(id));
                        entry
                    }
                    // A client that sent something that is not an object
                    // gets it back as it sent it, which will not match
                    // and will be told so by its own comparison.
                    None => entry,
                }
            })
            .collect();
        // A list with an entry that could not be subscribed leaves this
        // channel with none, so there is nothing here to take back out
        // when it goes.
        if made.refused.is_none() {
            self.watched.insert(topic.to_string(), made.ids);
        }
        let mut actions = vec![self.send(Frame::reply(
            &frame,
            "ok",
            json!({"postgres_changes": echoed}),
        ))];
        // And the line upstream says after it, which is where a
        // subscription that could not be made is said. The reply is ok
        // either way and the channel stays joined either way, because
        // the reply is about what the client asked for and this is
        // about what became of it. Nothing in realtime-js acts on it,
        // but it is on the socket and an application can bind `system`.
        actions.push(match &made.refused {
            Some(why) => self.send(Frame::system(&frame, "postgres_changes", "error", why)),
            None => self.send(Frame::system(
                &frame,
                "postgres_changes",
                "ok",
                "Subscribed to PostgreSQL",
            )),
        });
        actions.extend(self.after_reply(topic));
        actions
    }

    /// A join that got as far as being carried and then could not have
    /// what it asked for. The topic goes back down with the refusal,
    /// since a client that is told its join failed is not going to send
    /// a leave for it.
    fn unsubscribed(&mut self, frame: &Frame, why: impl Into<String>) -> Vec<Action> {
        let topic = frame.topic.clone();
        let room = self.room(&topic);
        self.channels.remove(&topic);
        self.rights.remove(&topic);
        let mut actions = vec![self.send(Frame::error(frame, why.into()))];
        actions.extend(self.unwatch(&topic));
        actions.push(Action::Drop(room));
        actions
    }

    /// The subscriptions a channel held, on their way out of whatever
    /// is reading the database.
    fn unwatch(&mut self, topic: &str) -> Option<Action> {
        let ids = self.watched.remove(topic)?;
        (!ids.is_empty()).then_some(Action::Unwatch(ids))
    }

    /// A row this socket asked about, on its way down the channel that
    /// asked.
    ///
    /// The ids are what says which channel: two channels on one socket
    /// can be subscribed to the same table, and each is owed the change
    /// with its own ids on it rather than one message carrying both.
    /// The project's events budget is spent here for the same reason it
    /// is spent on a broadcast, since a change delivered down a socket
    /// costs the project what any other message does.
    pub fn changed(&mut self, ids: &[u64], data: &Value) -> Vec<Action> {
        let mine: Vec<(String, Vec<u64>)> = self
            .watched
            .iter()
            .filter_map(|(topic, held)| {
                let ids: Vec<u64> = held.iter().copied().filter(|id| ids.contains(id)).collect();
                (!ids.is_empty()).then(|| (topic.clone(), ids))
            })
            .collect();
        let mut actions = Vec::new();
        for (topic, ids) in mine {
            if self.budget.counters.over_events() {
                actions.extend(self.overrun(&topic));
                continue;
            }
            actions.push(self.send(Frame::push(
                &topic,
                "postgres_changes",
                json!({"ids": ids, "data": data}),
            )));
        }
        actions
    }

    /// The room a topic this socket is on is in. A topic it is not on
    /// is a public room, which is the harmless direction to guess in:
    /// nothing is delivered to a room this socket is not carrying.
    fn room(&self, topic: &str) -> String {
        room(topic, self.channels.get(topic).is_some_and(|c| c.private))
    }

    /// Whether the topic's presence may be sent down this socket,
    /// which is a question only a private channel has.
    fn readable_presence(&self, topic: &str) -> bool {
        self.rights
            .get(topic)
            .is_none_or(|rights| rights.presence_read)
    }

    /// The answer to an [`Ask`], and what the socket does now it has
    /// one.
    ///
    /// `granted` is what the policies said, or the reason nobody could
    /// find out, which the client is told rather than left guessing
    /// about.
    pub fn authorized(&mut self, ask: &Ask, granted: Result<Grant, String>) -> Vec<Action> {
        let Some(pending) = self.pending.remove(&ask.topic) else {
            log::debug!(
                "realtime: an answer about {} that nothing was waiting for",
                ask.topic
            );
            return Vec::new();
        };
        let granted = match granted {
            Ok(granted) => granted,
            Err(why) => return self.refuse(pending, &ask.topic, why),
        };
        let name = ask.name().to_string();
        let rights = self.rights.entry(ask.topic.clone()).or_default();
        match ask.about {
            About::Join => {
                rights.presence_read = granted.presence;
                if !granted.broadcast && !granted.presence {
                    let why = format!(
                        "You do not have permissions to read from this Channel topic: {name}"
                    );
                    return self.refuse(pending, &ask.topic, why);
                }
                match pending {
                    Pending::Join(frame) => self.joined(frame),
                    // A recheck that came back fine, which is the
                    // quiet case: the socket carries on.
                    _ => Vec::new(),
                }
            }
            About::Broadcast | About::Presence => {
                rights.writes = Some(granted);
                let allowed = match ask.about {
                    About::Presence => granted.presence,
                    _ => granted.broadcast,
                };
                match (allowed, pending) {
                    (true, Pending::Do { actions, .. }) => actions,
                    (true, _) => Vec::new(),
                    (false, pending) => {
                        let why = format!(
                            "You do not have permissions to write to this Channel topic: {name}"
                        );
                        self.refuse(pending, &ask.topic, why)
                    }
                }
            }
        }
    }

    /// What a no looks like, which depends on what was waiting.
    fn refuse(&mut self, pending: Pending, topic: &str, why: String) -> Vec<Action> {
        match pending {
            Pending::Join(frame) => vec![self.send(Frame::error(&frame, why))],
            Pending::Do { asked, .. } => vec![self.send(Frame::error(&asked, why))],
            // A join that was already allowed and was waiting for its
            // subscriptions, which cannot happen while the transport
            // asks one question at a time and is answered here anyway
            // rather than left as a channel with no reply coming.
            Pending::Watch(frame) => self.unsubscribed(&frame, why),
            // A socket that was already on the topic and is not
            // allowed on it any more. The channel goes down the same
            // way a refused token takes one down, which is what the
            // client needs to stop trusting what it has and
            // resubscribe.
            Pending::Recheck => {
                let room = self.room(topic);
                self.channels.remove(topic);
                self.rights.remove(topic);
                let mut actions = vec![self.send(Frame::channel_error(topic))];
                actions.extend(self.unwatch(topic));
                actions.push(Action::Untrack(room.clone()));
                actions.push(Action::Drop(room));
                actions
            }
        }
    }

    /// Everything a private channel does goes through here first.
    ///
    /// A public channel goes straight through, a private one whose
    /// policies have already answered goes through or is refused with
    /// what it may not do, and one nobody has asked about yet holds
    /// what it was going to do until somebody has.
    fn gated(
        &mut self,
        topic: &str,
        about: About,
        asked: Frame,
        actions: Vec<Action>,
    ) -> Vec<Action> {
        let Some(rights) = self.rights.get(topic) else {
            return actions;
        };
        let known = rights.writes.map(|writes| match about {
            About::Presence => writes.presence,
            _ => writes.broadcast,
        });
        match known {
            Some(true) => actions,
            Some(false) => {
                let why = format!(
                    "You do not have permissions to write to this Channel topic: {}",
                    name_of(topic)
                );
                vec![self.send(Frame::error(&asked, why))]
            }
            None => {
                self.pending
                    .insert(topic.to_string(), Pending::Do { asked, actions });
                vec![Action::Ask(Ask {
                    topic: topic.to_string(),
                    about,
                })]
            }
        }
    }

    fn leave(&mut self, frame: Frame) -> Vec<Action> {
        let room = self.room(&frame.topic);
        if self.channels.remove(&frame.topic).is_none() {
            return vec![self.send(Frame::error(&frame, "this socket is not on that topic"))];
        }
        self.rights.remove(&frame.topic);
        self.pending.remove(&frame.topic);
        // Untracking before the drop, because the diff has to go out
        // while this socket is still on the topic for the others to be
        // told it left.
        let mut actions = vec![Action::Untrack(room.clone()), Action::Drop(room)];
        actions.extend(self.unwatch(&frame.topic));
        actions.push(self.send(Frame::ok(&frame)));
        actions
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
                    topic: room(&frame.topic, config.private),
                    key: config.presence_key.clone(),
                    payload,
                };
                let actions = vec![track, self.send(Frame::ok(&frame))];
                let topic = frame.topic.clone();
                self.gated(&topic, About::Presence, frame, actions)
            }
            "untrack" => vec![
                Action::Untrack(room(&frame.topic, config.private)),
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
    ///
    /// `room` is what the hub was asked about and the frame carries
    /// the topic, which are the same string for a public channel and
    /// two for a private one.
    pub fn state(&self, room: &str, state: Value) -> Action {
        self.send(Frame::push(topic_of(room), "presence_state", state))
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
                let mut actions = vec![self.send(Frame::ok(&frame))];
                // A row is only allowed to reach whoever this socket
                // currently is, and it is somebody else now, so whatever
                // checks its changes against the policies is told before
                // the next one is read.
                if !self.watched.is_empty() {
                    actions.push(Action::Rewatch);
                }
                // Every private channel this socket is on was allowed
                // on to it as somebody else. What the old token could
                // read says nothing about what this one can, so the
                // answers are thrown away and asked again, and a
                // channel the new token may not read is taken down
                // rather than left running.
                let private: Vec<String> = self.rights.keys().cloned().collect();
                for topic in private {
                    self.rights.insert(topic.clone(), Rights::default());
                    self.pending.insert(topic.clone(), Pending::Recheck);
                    actions.push(Action::Ask(Ask {
                        topic,
                        about: About::Join,
                    }));
                }
                actions
            }
            Err(why) => {
                let topics: Vec<String> = self.channels.keys().cloned().collect();
                let mut actions = vec![self.send(Frame::error(&frame, why))];
                for topic in topics {
                    let room = self.room(&topic);
                    self.channels.remove(&topic);
                    self.rights.remove(&topic);
                    self.pending.remove(&topic);
                    actions.push(self.send(Frame::channel_error(&topic)));
                    actions.extend(self.unwatch(&topic));
                    actions.push(Action::Untrack(room.clone()));
                    actions.push(Action::Drop(room));
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
        let room = room(&frame.topic, config.private);
        let Some(push) = BinaryBroadcast::from_frame(&frame) else {
            return vec![self.send(Frame::error(
                &frame,
                "a broadcast carries an event name and a payload",
            ))];
        };
        if self.too_big(&push) {
            return self.oversized(&frame, ack);
        }
        let mut actions = vec![Action::Fan(Fanout {
            topic: room,
            push,
            to_self,
        })];
        if ack {
            actions.push(self.send(Frame::ok(&frame)));
        }
        let topic = frame.topic.clone();
        self.gated(&topic, About::Broadcast, frame, actions)
    }

    /// Whether one push is more than the project allows a message to
    /// be.
    ///
    /// Upstream measures the erlang term it is about to hand to the
    /// pubsub and compares it with `max_payload_size_in_kb` and five
    /// hundred bytes of slack. There is no erlang term here, so what
    /// is measured is what will go over the wire: the payload and the
    /// event name it is sent under.
    fn too_big(&self, push: &BinaryBroadcast) -> bool {
        let Some(allowed) = self.budget.limits.payload_bytes() else {
            return false;
        };
        (push.payload.len() + push.event.len()) as u64 > allowed
    }

    /// What a message too big for the project gets: an error to the
    /// client that asked to be told, and silence for one that did not,
    /// which is upstream's answer to both.
    fn oversized(&mut self, asked: &Frame, ack: bool) -> Vec<Action> {
        if !ack {
            log::debug!(
                "realtime: a push to {} is over the payload budget and was dropped",
                asked.topic
            );
            return Vec::new();
        }
        vec![self.send(Frame::reply(asked, "error", json!("payload_size_exceeded")))]
    }

    /// The project has moved more messages a second than it may, and
    /// this channel is one that noticed.
    ///
    /// Upstream tells the client on the channel's own system event and
    /// then stops the channel, which the client sees as a close on
    /// that topic and not on the socket: its other channels carry on
    /// and this one is resubscribed by the client's own retry.
    fn overrun(&mut self, topic: &str) -> Vec<Action> {
        let room = self.room(topic);
        self.channels.remove(topic);
        self.rights.remove(topic);
        self.pending.remove(topic);
        let mut actions = vec![
            self.send(Frame::push(
                topic,
                "system",
                json!({
                    "extension": "system",
                    "status": "error",
                    "message": "Too many messages per second",
                    "channel": name_of(topic),
                }),
            )),
            self.send(Frame::push(topic, "phx_close", json!({}))),
        ];
        actions.extend(self.unwatch(topic));
        actions.push(Action::Untrack(room.clone()));
        actions.push(Action::Drop(room));
        actions
    }

    /// Something from the hub, on its way down this socket.
    ///
    /// `mine` is whether this socket is the one that caused it, which
    /// is the only thing `broadcast.self` decides. A presence diff
    /// ignores it: the socket that tracked applies its own change out
    /// of the diff like everybody else does.
    ///
    /// This is also where a project that is moving more messages than
    /// it may is noticed, because it is the only place that sees every
    /// message: upstream counts on delivery too, and the channel that
    /// was about to be handed one is the channel it takes down.
    pub fn deliver(&mut self, sent: &Sent, mine: bool) -> Vec<Action> {
        if self.budget.counters.over_events()
            && let Some(topic) = self.on_room(sent.topic()).map(str::to_string)
        {
            return self.overrun(&topic);
        }
        self.delivered(sent, mine).into_iter().collect()
    }

    fn delivered(&self, sent: &Sent, mine: bool) -> Option<Action> {
        match sent {
            Sent::Broadcast(fan) => self.broadcast_down(fan, mine),
            Sent::Diff { topic, payload } => {
                let topic = self.on_room(topic)?;
                // A socket that did not ask for presence is not sent
                // any: it is still visible to the rest of the topic,
                // it just has nothing bound that would read this. On a
                // private channel the presence read policy has the
                // last word, whatever was asked for.
                self.channels.get(topic).filter(|c| c.presence)?;
                if !self.readable_presence(topic) {
                    return None;
                }
                Some(self.send(Frame::push(topic, "presence_diff", payload.clone())))
            }
        }
    }

    /// The topic this socket is on that `room` is for, or nothing if
    /// this socket is not on it. A private room and a public room of
    /// the same name are two rooms, so a socket on one of them is not
    /// on the other and hears nothing from it.
    fn on_room<'a>(&self, room: &'a str) -> Option<&'a str> {
        let topic = topic_of(room);
        let config = self.channels.get(topic)?;
        (self::room(topic, config.private) == room).then_some(topic)
    }

    fn broadcast_down(&self, fan: &Fanout, mine: bool) -> Option<Action> {
        if mine && !fan.to_self {
            return None;
        }
        self.on_room(&fan.topic)?;
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

    /// What one delivery came to. Everything but an overrun is a
    /// single action or nothing at all, so a test that expects a
    /// delivery says so by taking one.
    fn only(actions: Vec<Action>) -> Option<Action> {
        assert!(actions.len() <= 1, "one delivery, {actions:?}");
        actions.into_iter().next()
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

    /// Subscriptions that were made, which is the ordinary answer to a
    /// watch.
    fn made(ids: Vec<u64>) -> Watched {
        Watched { ids, refused: None }
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

    /// A join with two subscriptions on it, one of them filtered.
    fn watching_join(session: &mut Session) -> Vec<Action> {
        join(
            session,
            r#"{"config":{"postgres_changes":[
                 {"event":"*","schema":"public","table":"todos"},
                 {"event":"INSERT","schema":"public","table":"todos","filter":"id=eq.1"}]}}"#,
        )
    }

    /// The reply is what carries the ids, so there is no reply until
    /// there are ids to carry.
    #[test]
    fn a_join_asking_for_postgres_changes_is_answered_once_it_has_them() {
        let mut session = socket();
        let actions = watching_join(&mut session);
        assert_eq!(actions[0], Action::Carry("realtime:room".into()));
        let Action::Watch { topic, wants } = &actions[1] else {
            panic!("{:?} is not a watch", actions[1]);
        };
        assert_eq!(topic, "realtime:room");
        assert_eq!(wants.len(), 2);
        assert_eq!(
            actions.len(),
            2,
            "nothing was answered, because a client told it is subscribed before anything is \
             reading the database is a client that will wait forever for its first row"
        );

        let actions = session.watching("realtime:room", Ok(made(vec![7, 8])));
        let reply = text_of(&actions[0]);
        assert_eq!(reply.payload["status"], "ok");
        assert_eq!(
            reply.payload["response"]["postgres_changes"],
            json!([
                {"event": "*", "schema": "public", "table": "todos", "id": 7},
                {"event": "INSERT", "schema": "public", "table": "todos",
                 "filter": "id=eq.1", "id": 8},
            ]),
            "each entry is the client's own, in the order it asked, with an id added: realtime-js \
             walks its bindings and this list together and unsubscribes the channel when any field \
             differs, so a filter this server tidied into a null would fail every subscription \
             that did not have one"
        );

        let system = text_of(&actions[1]);
        assert_eq!(system.event, "system");
        assert_eq!(system.topic, "realtime:room");
        assert_eq!(system.join_ref.as_deref(), Some("1"));
        assert_eq!(system.reference, None);
        assert_eq!(
            system.payload,
            json!({
                "message": "Subscribed to PostgreSQL",
                "status": "ok",
                "extension": "postgres_changes",
                "channel": "room",
            }),
            "the line upstream says after the reply, which is on the socket whether or not the \
             client's own library does anything with it"
        );
    }

    #[test]
    fn a_change_goes_down_the_channel_that_asked_for_it_with_its_own_ids() {
        let mut session = socket();
        watching_join(&mut session);
        session.watching("realtime:room", Ok(made(vec![7, 8])));

        let data = json!({"type": "INSERT", "table": "todos", "record": {"id": 1}});
        let actions = session.changed(&[8], &data);
        let push = text_of(&actions[0]);
        assert_eq!(push.topic, "realtime:room");
        assert_eq!(push.event, "postgres_changes");
        assert_eq!(
            push.payload,
            json!({"ids": [8], "data": data}),
            "the ids are the client's own, which is how it knows which of its subscriptions this \
             answers and which callback to run"
        );
        assert!(
            session.changed(&[99], &data).is_empty(),
            "and an id this socket never asked for goes nowhere"
        );
    }

    #[test]
    fn a_channel_that_leaves_takes_its_subscriptions_with_it() {
        let mut session = socket();
        watching_join(&mut session);
        session.watching("realtime:room", Ok(made(vec![7, 8])));

        let actions = session.text(r#"["1","2","realtime:room","phx_leave",{}]"#, &OneToken);
        assert!(
            actions.contains(&Action::Unwatch(vec![7, 8])),
            "a subscription nobody is on the other end of is a row checked against a policy for \
             nothing, {actions:?}"
        );
        assert!(
            session.changed(&[7], &json!({"type": "INSERT"})).is_empty(),
            "and a change that arrives after the leave has nowhere to go"
        );
    }

    /// A row is only allowed to reach whoever the socket currently is,
    /// so the thing checking the rows has to hear about a new token.
    #[test]
    fn a_refreshed_token_is_told_to_whatever_is_checking_the_rows() {
        let mut session = socket();
        watching_join(&mut session);
        session.watching("realtime:room", Ok(made(vec![7, 8])));

        let actions = session.text(
            r#"["1","3","realtime:room","access_token",{"access_token":"good"}]"#,
            &OneToken,
        );
        assert!(actions.contains(&Action::Rewatch), "{actions:?}");
        assert_eq!(session.role(), "authenticated");
    }

    /// A list with a table nobody published in it, which is a joined
    /// channel with nothing subscribed rather than a failed join.
    ///
    /// Upstream draws the line here rather than at the reply: the
    /// client asked for something well formed and got an answer, and
    /// what could not be arranged is said on the frame that says what
    /// became of the subscriptions. A channel that also broadcasts
    /// keeps working, which it would not if the join had been refused.
    #[test]
    fn a_subscription_nobody_could_make_is_said_on_the_system_frame() {
        let mut session = socket();
        watching_join(&mut session);
        let why = "Unable to subscribe to changes with given parameters.";
        let actions = session.watching(
            "realtime:room",
            Ok(Watched {
                ids: vec![7, 8],
                refused: Some(why.into()),
            }),
        );

        let reply = text_of(&actions[0]);
        assert_eq!(reply.payload["status"], "ok");
        assert_eq!(
            reply.payload["response"]["postgres_changes"][0]["id"], 7,
            "the ids go back whatever became of them, because realtime-js compares the reply with              its own bindings before it reads anything else and a shorter list is a mismatch"
        );
        let system = text_of(&actions[1]);
        assert_eq!(system.event, "system");
        assert_eq!(
            system.payload,
            json!({
                "message": why,
                "status": "error",
                "extension": "postgres_changes",
                "channel": "room",
            })
        );
        assert!(
            session.on("realtime:room"),
            "the channel is joined, which is what upstream leaves it as"
        );
        assert!(
            session.changed(&[7], &json!({"type": "INSERT"})).is_empty(),
            "and the ids name nothing, so a change that somehow carried one goes nowhere"
        );
        let actions = session.text(r#"["1","2","realtime:room","phx_leave",{}]"#, &OneToken);
        assert!(
            !actions.iter().any(|a| matches!(a, Action::Unwatch(_))),
            "nothing was bound, so there is nothing to take back out, {actions:?}"
        );
    }

    #[test]
    fn subscriptions_that_could_not_be_made_take_the_channel_back_down() {
        let mut session = socket();
        watching_join(&mut session);
        let actions = session.watching("realtime:room", Err("a filter needs a table".into()));
        let reply = text_of(&actions[0]);
        assert_eq!(reply.payload["status"], "error");
        assert_eq!(
            reply.payload["response"]["reason"],
            "a filter needs a table"
        );
        assert!(
            actions.contains(&Action::Drop("realtime:room".into())),
            "the join was carried before the ids were asked for, so a join that failed after that \
             is a topic to stop carrying, {actions:?}"
        );
        assert!(!session.on("realtime:room"));
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
        assert!(only(sender.deliver(&sent, true)).is_none());
        assert!(matches!(
            only(other.deliver(&sent, false)),
            Some(Action::Binary(_))
        ));
        // A socket that left hears nothing more.
        third.text(r#"["1","6","realtime:room","phx_leave",{}]"#, &OneToken);
        assert!(only(third.deliver(&sent, false)).is_none());
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
        let Some(Action::Text(text)) = only(old.deliver(&Sent::Broadcast(fan.clone()), false))
        else {
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
        assert!(only(session.deliver(&diff, true)).is_none());
    }

    #[test]
    fn a_diff_reaches_the_sockets_that_asked_for_presence_including_the_one_that_moved() {
        let mut session = socket();
        join(&mut session, r#"{"config":{"presence":{"enabled":true}}}"#);
        let diff = Sent::Diff {
            topic: "realtime:room".into(),
            payload: json!({"joins": {"u1": {"metas": [{"phx_ref": "3"}]}}, "leaves": {}}),
        };
        let Some(Action::Text(text)) = only(session.deliver(&diff, true)) else {
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
        assert!(only(session.deliver(&elsewhere, false)).is_none());
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

    const PRIVATE: &str = r#"{"config":{"private":true}}"#;

    fn asked(actions: &[Action]) -> Ask {
        match actions.first() {
            Some(Action::Ask(ask)) => ask.clone(),
            other => panic!("{other:?} is not a question"),
        }
    }

    /// Everything the policies said yes to.
    fn yes() -> Grant {
        Grant {
            broadcast: true,
            presence: true,
        }
    }

    /// A private channel joined with the policies saying yes, which is
    /// where the pushing tests start from.
    fn private_socket() -> Session {
        let mut session = socket();
        let ask = asked(&join(&mut session, PRIVATE));
        session.authorized(&ask, Ok(yes()));
        session
    }

    #[test]
    fn a_private_join_waits_for_the_policies_rather_than_deciding_itself() {
        let mut session = socket();
        let actions = join(&mut session, PRIVATE);
        assert_eq!(actions.len(), 1);
        let ask = asked(&actions);
        assert_eq!(ask.about, About::Join);
        assert_eq!(ask.topic, "realtime:room");
        // The name the policies are asked about is the room, not the
        // topic the socket carries.
        assert_eq!(ask.name(), "room");
        // Nothing has happened yet: no reply, and the socket is not on
        // the topic.
        assert!(!session.on("realtime:room"));

        let actions = session.authorized(&ask, Ok(yes()));
        // The room a private channel is carried on is not the room a
        // public channel of the same name is on.
        assert_eq!(actions[0], Action::Carry("private:realtime:room".into()));
        assert_eq!(text_of(&actions[1]).payload["status"], "ok");
        assert!(session.on("realtime:room"));
    }

    #[test]
    fn a_private_room_and_a_public_room_of_the_same_name_are_two_rooms() {
        let mut private = private_socket();
        let mut public = socket();
        join(&mut public, r#"{"config":{}}"#);

        let from_private = private.text(
            r#"["1","5","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":1}}]"#,
            &OneToken,
        );
        // The push is held while the write policy is asked, and the
        // answer is what turns it into a fan out.
        let allowed = private.authorized(&asked(&from_private), Ok(yes()));
        let Action::Fan(fan) = &allowed[0] else {
            panic!("not a fan out")
        };
        assert_eq!(fan.topic, "private:realtime:room");
        // A public channel of that name is a room the policies were
        // never asked about, so nothing the private one says reaches
        // it. Anything else would make the policies decoration: anyone
        // could join room without them and hear the lot.
        assert!(only(public.deliver(&Sent::Broadcast(fan.clone()), false)).is_none());

        let from_public = public.text(
            r#"["1","5","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":2}}]"#,
            &OneToken,
        );
        let Action::Fan(fan) = &from_public[0] else {
            panic!("not a fan out")
        };
        assert_eq!(fan.topic, "realtime:room");
        assert!(only(private.deliver(&Sent::Broadcast(fan.clone()), false)).is_none());
    }

    #[test]
    fn presence_on_a_private_room_is_not_presence_on_the_public_one() {
        let mut session = socket();
        let ask = asked(&join(
            &mut session,
            r#"{"config":{"private":true,"presence":{"enabled":true}}}"#,
        ));
        let actions = session.authorized(&ask, Ok(yes()));
        // The state a private join is sent is the private room's.
        assert_eq!(actions[2], Action::State("private:realtime:room".into()));
        // And it is sent down naming the topic the client joined,
        // which is the same channel whichever room it is on.
        let state = session.state("private:realtime:room", json!({}));
        assert_eq!(text_of(&state).topic, "realtime:room");

        let track = session.text(
            r#"["1","6","realtime:room","presence",{"event":"track","payload":{"typing":true}}]"#,
            &OneToken,
        );
        let allowed = session.authorized(&asked(&track), Ok(yes()));
        assert_eq!(
            allowed[0],
            Action::Track {
                topic: "private:realtime:room".into(),
                key: None,
                payload: json!({"typing": true}),
            }
        );
        // A diff from the public room of that name is not this
        // socket's, the same way a broadcast from it is not.
        assert!(
            only(session.deliver(
                &Sent::Diff {
                    topic: "realtime:room".into(),
                    payload: json!({"joins": {}, "leaves": {}}),
                },
                false,
            ))
            .is_none()
        );
    }

    #[test]
    fn a_private_join_the_policies_refuse_is_told_what_it_may_not_read() {
        let mut session = socket();
        let ask = asked(&join(&mut session, PRIVATE));
        let actions = session.authorized(&ask, Ok(Grant::default()));
        let reply = text_of(&actions[0]);
        assert_eq!(reply.payload["status"], "error");
        assert_eq!(
            reply.payload["response"]["reason"],
            "You do not have permissions to read from this Channel topic: room"
        );
        assert!(!session.on("realtime:room"));
    }

    #[test]
    fn a_join_nobody_could_check_says_why_rather_than_saying_no() {
        let mut session = socket();
        let ask = asked(&join(&mut session, PRIVATE));
        let actions = session.authorized(&ask, Err("the database is not answering".into()));
        assert_eq!(
            text_of(&actions[0]).payload["response"]["reason"],
            "the database is not answering"
        );
        assert!(!session.on("realtime:room"));
    }

    #[test]
    fn a_push_to_a_private_channel_is_checked_once_and_then_remembered() {
        let mut session = private_socket();
        let push = r#"["1","7","realtime:room","broadcast",{"event":"cursor","payload":{"x":1}}]"#;
        let actions = session.text(push, &OneToken);
        let ask = asked(&actions);
        assert_eq!(ask.about, About::Broadcast);
        // The fan out was held, not sent, and comes back whole.
        let actions = session.authorized(&ask, Ok(yes()));
        assert!(matches!(actions[0], Action::Fan(_)));
        // The second push is the same socket asking the same question,
        // which has already been answered.
        let actions = session.text(push, &OneToken);
        assert!(matches!(actions[0], Action::Fan(_)));
    }

    #[test]
    fn a_push_the_write_policies_refuse_is_answered_rather_than_sent() {
        let mut session = private_socket();
        let actions = session.text(
            r#"["1","7","realtime:room","broadcast",{"event":"cursor","payload":{}}]"#,
            &OneToken,
        );
        let ask = asked(&actions);
        let actions = session.authorized(&ask, Ok(Grant::default()));
        let reply = text_of(&actions[0]);
        assert_eq!(reply.payload["status"], "error");
        assert_eq!(
            reply.payload["response"]["reason"],
            "You do not have permissions to write to this Channel topic: room"
        );
        assert!(!actions.iter().any(|a| matches!(a, Action::Fan(_))));
    }

    #[test]
    fn a_track_on_a_private_channel_asks_about_presence_rather_than_broadcast() {
        let mut session = private_socket();
        let actions = session.text(
            r#"["1","7","realtime:room","presence",{"event":"track","payload":{"typing":true}}]"#,
            &OneToken,
        );
        let ask = asked(&actions);
        assert_eq!(ask.about, About::Presence);
        let refused = session.authorized(
            &ask,
            Ok(Grant {
                broadcast: true,
                presence: false,
            }),
        );
        assert!(!refused.iter().any(|a| matches!(a, Action::Track { .. })));
        assert_eq!(text_of(&refused[0]).payload["status"], "error");
    }

    #[test]
    fn presence_the_read_policy_refused_is_not_sent_down_the_socket() {
        let mut session = socket();
        let ask = asked(&join(
            &mut session,
            r#"{"config":{"private":true,"presence":{"enabled":true}}}"#,
        ));
        let actions = session.authorized(
            &ask,
            Ok(Grant {
                broadcast: true,
                presence: false,
            }),
        );
        // Joined, because broadcast is readable, but with no state
        // pushed after the reply and no diffs to follow.
        assert!(session.on("realtime:room"));
        assert!(!actions.iter().any(|a| matches!(a, Action::State(_))));
        let diff = Sent::Diff {
            topic: "realtime:room".into(),
            payload: json!({"joins": {}, "leaves": {}}),
        };
        assert_eq!(only(session.deliver(&diff, false)), None);
    }

    #[test]
    fn a_new_token_puts_the_private_channels_back_to_the_policies() {
        let mut session = private_socket();
        let actions = session.text(
            r#"["1","8",  "phoenix","access_token",{"access_token":"good"}]"#,
            &OneToken,
        );
        assert_eq!(text_of(&actions[0]).payload["status"], "ok");
        let ask = match &actions[1] {
            Action::Ask(ask) => ask.clone(),
            other => panic!("{other:?} is not a question"),
        };
        assert_eq!(ask.about, About::Join);
        // The new token may not read what the old one could, and the
        // channel goes down rather than carrying on.
        let actions = session.authorized(&ask, Ok(Grant::default()));
        assert_eq!(text_of(&actions[0]).event, "phx_error");
        assert_eq!(actions[1], Action::Untrack("private:realtime:room".into()));
        assert_eq!(actions[2], Action::Drop("private:realtime:room".into()));
        assert!(!session.on("realtime:room"));
    }

    #[test]
    fn a_new_token_that_can_still_read_leaves_the_channel_alone() {
        let mut session = private_socket();
        let actions = session.text(
            r#"["1","8","phoenix","access_token",{"access_token":"good"}]"#,
            &OneToken,
        );
        let ask = match &actions[1] {
            Action::Ask(ask) => ask.clone(),
            other => panic!("{other:?} is not a question"),
        };
        assert!(session.authorized(&ask, Ok(yes())).is_empty());
        assert!(session.on("realtime:room"));
        // The write answer was thrown away with the old token, so the
        // next push is checked again as the new one.
        let actions = session.text(
            r#"["1","9","realtime:room","broadcast",{"event":"cursor","payload":{}}]"#,
            &OneToken,
        );
        assert_eq!(asked(&actions).about, About::Broadcast);
    }

    #[test]
    fn a_public_channel_asks_nobody_anything() {
        let mut session = socket();
        join(&mut session, r#"{"config":{"presence":{"enabled":true}}}"#);
        for push in [
            r#"["1","7","realtime:room","broadcast",{"event":"cursor","payload":{}}]"#,
            r#"["1","8","realtime:room","presence",{"event":"track","payload":{}}]"#,
        ] {
            let actions = session.text(push, &OneToken);
            assert!(
                !actions.iter().any(|a| matches!(a, Action::Ask(_))),
                "{push}"
            );
        }
    }

    /// A project that has no room for another join, which is what the
    /// transport's counter says when the whole server is joining
    /// faster than it may.
    struct NoJoins;

    impl Counters for NoJoins {
        fn join(&self) -> bool {
            false
        }

        fn over_events(&self) -> bool {
            false
        }
    }

    /// A project that is already moving more messages a second than it
    /// is allowed.
    struct Overrun;

    impl Counters for Overrun {
        fn join(&self) -> bool {
            true
        }

        fn over_events(&self) -> bool {
            true
        }
    }

    fn on_budget(limits: Limits, counters: Arc<dyn Counters>) -> Session {
        Session::budgeted(
            Vsn::V2,
            Identity {
                role: "anon".into(),
                claims: json!({"role": "anon"}),
            },
            Budget { limits, counters },
        )
    }

    #[test]
    fn a_socket_holding_every_channel_it_may_is_refused_the_next_one() {
        let mut session = on_budget(
            Limits {
                channels_per_client: 1,
                ..Limits::none()
            },
            Arc::new(Unlimited),
        );
        join(&mut session, r#"{"config":{}}"#);
        let actions = session.text(
            r#"["1","2","realtime:other","phx_join",{"config":{}}]"#,
            &OneToken,
        );
        assert_eq!(
            text_of(&actions[0]).payload,
            json!({
                "status": "error",
                "response": {"reason": "ChannelRateLimitReached: Too many channels"},
            })
        );
        assert!(!session.on("realtime:other"));
        // The channel it already had is untouched, since the refusal
        // is about the next one and not about the socket.
        assert!(session.on("realtime:room"));
    }

    /// Upstream answers the join and then disconnects, which is the
    /// one refusal here that takes the whole socket with it.
    #[test]
    fn a_project_joining_faster_than_it_may_refuses_the_join_and_the_socket() {
        let mut session = on_budget(Limits::none(), Arc::new(NoJoins));
        let actions = join(&mut session, r#"{"config":{}}"#);
        assert_eq!(
            text_of(&actions[0]).payload,
            json!({
                "status": "error",
                "response": {"reason": "ClientJoinRateLimitReached: Too many joins per second"},
            })
        );
        assert_eq!(actions[1], Action::Close);
        assert!(!session.on("realtime:room"));
    }

    #[test]
    fn a_push_over_the_payload_budget_is_told_so_only_if_it_asked_to_be() {
        let kb = Limits {
            payload_size_kb: 1,
            ..Limits::none()
        };
        let big = "x".repeat(2000);

        let mut quiet = on_budget(kb, Arc::new(Unlimited));
        join(&mut quiet, r#"{"config":{}}"#);
        let dropped = quiet.text(
            &format!(
                r#"["1","5","realtime:room","broadcast",{{"event":"cursor","payload":{{"blob":"{big}"}}}}]"#
            ),
            &OneToken,
        );
        assert!(dropped.is_empty(), "{dropped:?}");

        let mut acked = on_budget(kb, Arc::new(Unlimited));
        join(&mut acked, r#"{"config":{"broadcast":{"ack":true}}}"#);
        let refused = acked.text(
            &format!(
                r#"["1","5","realtime:room","broadcast",{{"event":"cursor","payload":{{"blob":"{big}"}}}}]"#
            ),
            &OneToken,
        );
        assert_eq!(
            text_of(&refused[0]).payload,
            json!({"status": "error", "response": "payload_size_exceeded"})
        );

        // A message under the budget goes as it always did, so this is
        // a ceiling and not a switch.
        let small = acked.text(
            r#"["1","6","realtime:room","broadcast",{"event":"cursor","payload":{"x":1}}]"#,
            &OneToken,
        );
        assert!(matches!(small[0], Action::Fan(_)), "{small:?}");
    }

    #[test]
    fn a_binary_push_is_measured_the_same_way() {
        let mut session = on_budget(
            Limits {
                payload_size_kb: 1,
                ..Limits::none()
            },
            Arc::new(Unlimited),
        );
        join(&mut session, r#"{"config":{"broadcast":{"ack":true}}}"#);
        let mut push = vec![3u8, 1, 1, 13, 6, 0, 1];
        push.extend_from_slice(b"11realtime:roomcursor");
        push.extend_from_slice(&vec![b'x'; 2000]);
        let refused = session.binary(&push);
        assert_eq!(
            text_of(&refused[0]).payload,
            json!({"status": "error", "response": "payload_size_exceeded"})
        );
    }

    /// The project is over its messages a second, so the channel that
    /// was about to be handed one is told and taken down, and the
    /// socket itself carries on.
    #[test]
    fn a_channel_on_a_project_over_its_message_budget_is_taken_down() {
        let mut sender = socket();
        join(&mut sender, r#"{"config":{}}"#);
        let actions = sender.text(
            r#"["1","5","realtime:room","broadcast",{"event":"cursor","payload":{"x":1}}]"#,
            &OneToken,
        );
        let Action::Fan(fan) = &actions[0] else {
            panic!("not a fan out")
        };
        let sent = Sent::Broadcast(fan.clone());

        let mut over = on_budget(Limits::none(), Arc::new(Overrun));
        join(&mut over, r#"{"config":{}}"#);
        let told = over.deliver(&sent, false);
        let system = text_of(&told[0]);
        assert_eq!(system.event, "system");
        assert_eq!(
            system.payload,
            json!({
                "extension": "system",
                "status": "error",
                "message": "Too many messages per second",
                "channel": "room",
            })
        );
        assert_eq!(text_of(&told[1]).event, "phx_close");
        assert_eq!(told[2], Action::Untrack("realtime:room".into()));
        assert_eq!(told[3], Action::Drop("realtime:room".into()));
        assert!(!over.on("realtime:room"));
        // Nothing more is said about a topic this socket has been
        // taken off, so the channel is told once and not on every
        // message that follows.
        assert!(only(over.deliver(&sent, false)).is_none());
    }
}
