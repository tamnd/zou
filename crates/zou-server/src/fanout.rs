//! The link between a node with the sockets and the node with the
//! tenant.
//!
//! A tenant is written by one node, because the lease is one node's.
//! That is right for writes and wrong for sockets: a hundred thousand
//! idle listeners cost memory and file descriptors and nothing else,
//! and tying them to the node that owns the write path means the write
//! path is sized by how many browser tabs are open. So sockets get to
//! live somewhere else, and what they need from the tenant crosses one
//! link per tenant per node.
//!
//! Two ends live here. [`Away`] is the node's, which has the sockets
//! and no database, and [`link`] is the holder's, which is an ordinary
//! websocket route on the port it already serves. What crosses is in
//! [`Wire`]: a json header with its length in front of it and, for a
//! broadcast, the payload bytes after it, so a message a client sent as
//! bytes is not parsed and reprinted by anything on the way.
//!
//! One link per tenant and one ordered stream on it. Two streams would
//! be two orderings and a client that saw a presence diff before the
//! join it belongs to. Ordered has a price, which is that a broadcast
//! between two sockets on the same node goes to the holder and comes
//! back: the alternative is a local copy racing the copy every other
//! node sees, which is the same message arriving in two orders on two
//! screens.
//!
//! Every frame the holder sends is numbered. A node that receives a
//! number it did not expect has missed something, and the one honest
//! thing to do with that is to tell the sockets the same news the
//! change reader tells a subscriber it dropped, which is that there is
//! a gap. A client that hears a gap reconnects and resubscribes; a
//! client that missed messages quietly cannot.
//!
//! Which is the last resort rather than the first. A link that drops is
//! usually a link that comes back a second later, and gapping a hundred
//! thousand sockets because two servers lost a tcp connection is a
//! reconnect storm this end asked for. So the holder keeps the last
//! [`KEPT`] frames it sent per link and everything that link's sockets
//! held, for [`GRACE`]; a node names its link and says which number it
//! got to; and a link that comes back inside both is given what it
//! missed, in order, with nothing closed. Past either, it is told, and
//! then the gap is the honest answer again.
//!
//! What does not cross this link is a row the socket may not see. The
//! two questions only the holder can answer, whether a private
//! channel's policies allow this socket and what a subscriber may see
//! of a changed row, are answered where the database is, so a fan out
//! node is handed conclusions rather than rows to filter.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::Response;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use zou_realtime::{
    About, Ask, BinaryBroadcast, Delivery, Encoding, Fanout, Grant, Identity, Sent, SocketId,
    Watched,
};

use crate::reader::{Heard as Feed, Listening, Shared};
use crate::realtime::{Heard, next_delivery};
use crate::{App, AuthContext, json_body};

/// What this end speaks. Both ends say it and neither guesses: a node
/// and a holder on different releases is an ordinary thing during a
/// rollout, and the honest failure is a link that refuses to open
/// rather than one that opens and misreads a frame.
const VERSION: u64 = 1;

/// What the other end said it speaks, or `Err` with the number it said
/// when that is not this one.
///
/// A hello with no version in it at all is the case worth naming: it
/// is what a very old end sends, and reading it as agreement would
/// open the link that is least likely to work. It counts as zero,
/// which is not this version, so it refuses like any other mismatch.
fn agrees(frame: &Wire) -> Result<(), u64> {
    match frame.number("version").unwrap_or_default() {
        theirs if theirs == VERSION => Ok(()),
        theirs => Err(theirs),
    }
}

/// How many frames may be waiting to go up before the link is treated
/// as broken.
///
/// Bounded on purpose, and the bound is what stops a holder nobody can
/// reach from being this node's memory problem. Over it, the sockets
/// here are told there is a gap, which is the same thing they would be
/// told if the link had dropped, because as far as they are concerned
/// it has.
const QUEUE: usize = 1024;

/// How long a question waits for its answer before the socket that
/// asked is told it could not be answered.
///
/// Inside realtime-js's own ten second join timeout, so a client that
/// joined a private channel hears why rather than timing out with
/// nothing said.
const ASKING: Duration = Duration::from_secs(5);

/// How long the node waits before dialling the holder again, and the
/// most it will ever wait.
///
/// A handover moves the lease in seconds, so the first retry is quick,
/// and a holder that is down for an hour should not be dialled a
/// thousand times a second by every node in the fleet.
const FIRST: Duration = Duration::from_millis(250);
const MOST: Duration = Duration::from_secs(30);

/// How deep one socket's feed is on this side of the link.
///
/// The same depth the change reader gives a subscriber of its own, since
/// this is the same queue in the same place, holding rows for one client
/// that has not read them yet. A gap goes down it too, and a socket that
/// cannot even be given one is a socket whose feed is closed instead,
/// which its loop answers the same way.
const FEED: usize = 256;

/// How deep the one queue all of a link's subscribers share on the
/// holder.
///
/// Bigger than a socket's, because it is every subscriber on the node
/// rather than one, and bounded for the same reason: a node that has
/// stopped draining its link cannot be the holder's memory problem. Over
/// it the link is told there is a gap, once, however many rows did not
/// fit.
const CHANGES: usize = 4096;

/// How long a subscription waits for its answer.
///
/// Longer than an ordinary question, because the holder's own answer
/// waits for a tap on the database before it says a subscription is
/// live, and that wait is five seconds. A node that gave up before the
/// holder did would tell a client its subscription failed while it was
/// being set up.
const WATCHING: Duration = Duration::from_secs(8);

/// How many frames the holder keeps per link, so that a link which
/// comes back can be given what it missed rather than every socket on
/// the far end being closed.
///
/// Bounded on purpose, and this is the one number: a node nobody can
/// reach cannot be the holder's memory problem, which is the same
/// bargain a slow subscriber already gets. Four times one socket's own
/// feed, because what fills this is a whole node's worth of sockets
/// rather than one, and small enough that a fleet of them is megabytes.
/// A frame is kept as the bytes that went out, so keeping one costs a
/// refcount rather than a copy.
const KEPT: usize = 1024;

/// How long the holder waits for a link that dropped before giving back
/// what it was holding for it.
///
/// A redial starts at 250 ms and a handover moves a lease in seconds, so
/// a link that is coming back is back well inside this. Past it the
/// presence its sockets were in is given up and their subscriptions are
/// let go, because a room full of people who are not there is worse for
/// everybody else on the channel than a reconnect is for them.
const GRACE: Duration = Duration::from_secs(30);

/// One message on the link.
///
/// A four byte length, a json header, and the bytes the header
/// describes. The bytes are there for one thing, which is a broadcast
/// payload: half of those are not json at all, and the half that are do
/// not need to be understood by anything between the two clients, so a
/// link that parsed and reprinted them would be spending cpu to change
/// nothing but the whitespace.
#[derive(Debug, Clone, PartialEq)]
pub struct Wire {
    pub head: Value,
    pub body: Vec<u8>,
}

impl Wire {
    /// A frame with nothing but a header, which is all of them except a
    /// broadcast.
    pub fn of(head: Value) -> Wire {
        Wire {
            head,
            body: Vec::new(),
        }
    }

    pub fn with(head: Value, body: Vec<u8>) -> Wire {
        Wire { head, body }
    }

    pub fn encode(&self) -> Vec<u8> {
        let head = self.head.to_string();
        let mut out = Vec::with_capacity(4 + head.len() + self.body.len());
        out.extend_from_slice(&(head.len() as u32).to_be_bytes());
        out.extend_from_slice(head.as_bytes());
        out.extend_from_slice(&self.body);
        out
    }

    /// Read one. None for anything that is not a frame, a truncated
    /// one included, because there is nothing useful to do with half a
    /// message and the caller's answer to both is the same.
    pub fn decode(bytes: &[u8]) -> Option<Wire> {
        if bytes.len() < 4 {
            return None;
        }
        let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let rest = bytes.get(4..)?;
        let head = rest.get(..len)?;
        Some(Wire {
            head: serde_json::from_slice(head).ok()?,
            body: rest[len..].to_vec(),
        })
    }

    /// What kind of frame this is, going up.
    fn up(&self) -> &str {
        self.head.get("up").and_then(Value::as_str).unwrap_or("")
    }

    /// And going down.
    fn down(&self) -> &str {
        self.head.get("down").and_then(Value::as_str).unwrap_or("")
    }

    fn number(&self, field: &str) -> Option<u64> {
        self.head.get(field).and_then(Value::as_u64)
    }

    fn text(&self, field: &str) -> Option<&str> {
        self.head.get(field).and_then(Value::as_str)
    }
}

/// A broadcast as a header and its payload, which is the one thing on
/// this link that is read the same way in both directions.
fn broadcast_head(fan: &Fanout) -> Value {
    json!({
        "topic": fan.topic,
        "push": {
            "join_ref": fan.push.join_ref,
            "ref": fan.push.reference,
            "topic": fan.push.topic,
            "event": fan.push.event,
            "meta": fan.push.meta,
            "encoding": match fan.push.encoding {
                Encoding::Json => "json",
                Encoding::Binary => "binary",
            },
        },
    })
}

fn broadcast_of(head: &Value, body: Vec<u8>) -> Option<Fanout> {
    let push = head.get("push")?;
    let text = |field: &str| {
        push.get(field)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    Some(Fanout {
        topic: head.get("topic").and_then(Value::as_str)?.to_string(),
        push: BinaryBroadcast {
            join_ref: text("join_ref"),
            reference: text("ref"),
            topic: text("topic"),
            event: text("event"),
            meta: text("meta"),
            encoding: match push.get("encoding").and_then(Value::as_str) {
                Some("binary") => Encoding::Binary,
                _ => Encoding::Json,
            },
            payload: body,
        },
    })
}

/// The holder, from the node that has the sockets.
///
/// One of these per tenant on a node that does not hold it, holding
/// everything the sockets here share: the queue up, the questions
/// waiting on an answer, and every socket's way of being told there is
/// a gap.
pub struct Away {
    /// Where the holder answers, as the http url it serves on. The link
    /// is a websocket on the same port, so there is no second listener
    /// and no second port for an operator to open.
    endpoint: String,
    /// What this node presents at the holder's door, which is a service
    /// key minted from the project's own secret. The link is the
    /// project's infrastructure talking to itself, and it goes through
    /// the same apikey gate every other request does rather than
    /// through a door of its own.
    key: String,
    /// Frames on their way up, drained by the link task.
    up: mpsc::Sender<Vec<u8>>,
    /// The other end of that, taken by the task when it starts. Held
    /// here rather than passed in because a router can be built outside
    /// a runtime, so the link is dialled by the first socket rather
    /// than at boot.
    queue: Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
    dialling: tokio::sync::OnceCell<()>,
    /// What is waiting on an answer, by the number it asked under.
    asked: Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>,
    next: AtomicU64,
    /// Every socket here, so that a gap can reach all of them.
    sockets: Mutex<HashMap<u64, mpsc::Sender<Feed>>>,
    /// How many sockets here are on each topic, so the holder is told
    /// once per topic rather than once per socket, and told it is done
    /// with one when the last socket here leaves it.
    topics: Mutex<HashMap<String, usize>>,
    /// What this node calls its link to the holder, said on every
    /// hello, so that a link which dropped and dialled again is known
    /// on the other end to be the same one and can be given what it
    /// missed. One per tenant per process, and drawn from the system
    /// random source rather than counted, because every node in a fleet
    /// counting from one would be a fleet of link ones.
    link: u64,
    /// The last numbered frame from the holder that this node has
    /// processed, which is what a redial asks to carry on after.
    ///
    /// Here rather than in the link task because it outlives the task:
    /// it is the whole of what a reconnect has to say for itself.
    seen: AtomicU64,
}

impl Away {
    pub fn new(endpoint: &str, secret: &[u8]) -> Away {
        let (up, queue) = mpsc::channel(QUEUE);
        let mut raw = [0u8; 8];
        getrandom::fill(&mut raw).expect("the os rng never fails");
        Away {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            key: crate::jwt::mint(&crate::jwt::key_claims("service_role"), secret),
            up,
            queue: Mutex::new(Some(queue)),
            dialling: tokio::sync::OnceCell::new(),
            asked: Mutex::new(HashMap::new()),
            next: AtomicU64::new(0),
            sockets: Mutex::new(HashMap::new()),
            topics: Mutex::new(HashMap::new()),
            // Never zero, because zero is what the holder reads as a
            // node old enough not to name its link at all.
            link: u64::from_be_bytes(raw).max(1),
            seen: AtomicU64::new(0),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The url the link is opened on.
    fn url(&self) -> String {
        let endpoint = match self.endpoint.split_once("://") {
            Some(("http", rest)) => format!("ws://{rest}"),
            Some(("https", rest)) => format!("wss://{rest}"),
            _ => self.endpoint.clone(),
        };
        format!("{endpoint}/realtime/v1/link?apikey={}", self.key)
    }

    /// Start the link, if this is the first socket to need it.
    async fn started(self: &Arc<Self>, app: &Arc<App>) {
        let away = Arc::clone(self);
        let app = Arc::downgrade(app);
        self.dialling
            .get_or_init(|| async move {
                // The queue is the task's from here on. Taking it is
                // what makes a second call a no op even if two sockets
                // arrive at once.
                let Some(queue) = away.queue.lock().expect("the link").take() else {
                    return;
                };
                let dialled = Arc::clone(&away);
                tokio::spawn(async move { dialling(app, dialled, queue).await });
            })
            .await;
    }

    /// A socket here, with the way it hears rows and is told there is a
    /// gap.
    ///
    /// Every socket on this node has one from the moment it connects,
    /// subscriber or not, because a gap is news for all of them and not
    /// only for the ones watching a table. The rows come down the same
    /// queue, so a socket that subscribes to a table needs nothing new
    /// and hears its changes in the same place it hears everything else.
    pub async fn joined(self: &Arc<Self>, app: &Arc<App>, me: SocketId) -> Listening {
        let (to, heard) = mpsc::channel(FEED);
        // Registered before the link is dialled rather than after, so
        // that a socket that arrived while the holder was unreachable is
        // one of the sockets the failed dial tells.
        self.sockets.lock().expect("the link").insert(me.raw(), to);
        self.started(app).await;
        self.tell(Wire::of(json!({"up": "socket", "socket": me.raw()})))
            .await;
        Listening {
            id: me.raw(),
            heard,
        }
    }

    /// A socket here has gone.
    pub async fn gone(&self, me: SocketId) {
        self.sockets.lock().expect("the link").remove(&me.raw());
        self.tell(Wire::of(json!({"up": "gone", "socket": me.raw()})))
            .await;
    }

    /// Somebody here is on a topic. The holder is told once, when the
    /// first of them joins it.
    pub async fn carry(self: &Arc<Self>, app: &Arc<App>, topic: &str) {
        self.started(app).await;
        self.on_topic(topic).await;
    }

    async fn on_topic(&self, topic: &str) {
        let first = {
            let mut topics = self.topics.lock().expect("the link");
            let on = topics.entry(topic.to_string()).or_insert(0);
            *on += 1;
            *on == 1
        };
        if first {
            self.tell(Wire::of(json!({"up": "carry", "topic": topic})))
                .await;
        }
    }

    /// And the last of them has left it.
    pub async fn released(&self, topic: &str) {
        let last = {
            let mut topics = self.topics.lock().expect("the link");
            match topics.get_mut(topic) {
                Some(on) => {
                    *on = on.saturating_sub(1);
                    if *on == 0 {
                        topics.remove(topic);
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        };
        if last {
            self.tell(Wire::of(json!({"up": "leave", "topic": topic})))
                .await;
        }
    }

    /// A broadcast, which goes up and comes back down to every socket
    /// on the topic here, this node's included.
    ///
    /// Nothing is fanned locally on the way past. A local copy would
    /// reach the sockets here before the holder's copy reached anybody
    /// else, which is one message in two orders on two screens, and it
    /// would also have to be de duplicated against the copy that comes
    /// back. The count is zero because nothing has been delivered yet.
    pub async fn fan(&self, me: SocketId, fan: Fanout) -> usize {
        let mut head = broadcast_head(&fan);
        if let Some(head) = head.as_object_mut() {
            head.insert("up".into(), json!("fan"));
            head.insert("socket".into(), json!(me.raw()));
        }
        self.tell(Wire::with(head, fan.push.payload)).await;
        0
    }

    /// Presence, which lives on the holder and nowhere else.
    ///
    /// A joiner has to be told who is on the channel across every node
    /// rather than who is on it here, so this node keeps none of it:
    /// the track goes up and the diff it causes comes back down like
    /// any other message on the topic.
    pub async fn track(&self, me: SocketId, topic: &str, key: Option<String>, payload: Value) {
        self.tell(Wire::of(json!({
            "up": "track",
            "socket": me.raw(),
            "topic": topic,
            "key": key,
            "payload": payload,
        })))
        .await;
    }

    pub async fn untrack(&self, me: SocketId, topic: &str) {
        self.tell(Wire::of(
            json!({"up": "untrack", "socket": me.raw(), "topic": topic}),
        ))
        .await;
    }

    /// Everyone who is on a topic right now.
    ///
    /// An empty object for a link that could not answer, which is what
    /// the hub says about a topic nobody is on. A join that was told
    /// nobody is there and then hears the diffs is a client whose copy
    /// is right from its second message on, and there is nothing truer
    /// to send it.
    pub async fn state(&self, topic: &str) -> Value {
        match self.asking(json!({"up": "state", "topic": topic})).await {
            Ok(state) => state,
            Err(why) => {
                log::warn!("realtime: the presence of {topic} could not be read, {why}");
                json!({})
            }
        }
    }

    /// What the project's own policies say about a private channel.
    ///
    /// The token goes up rather than the claims out of it, and the
    /// holder verifies it with the project's own verifier exactly as it
    /// would for a socket of its own. A node asserting a claim set
    /// would be a node that can read any row in the project by claiming
    /// to be anybody; carrying the token means a link is worth no more
    /// than the tokens on it.
    pub async fn ask(&self, token: Option<&str>, ask: &Ask) -> Result<Grant, String> {
        let answered = self
            .asking(json!({
                "up": "ask",
                "token": token,
                "topic": ask.topic,
                "about": match ask.about {
                    About::Join => "join",
                    About::Broadcast => "broadcast",
                    About::Presence => "presence",
                },
            }))
            .await?;
        Ok(Grant {
            broadcast: answered
                .get("broadcast")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            presence: answered
                .get("presence")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }

    /// What one client's `postgres_changes` list turns into, asked of
    /// the holder.
    ///
    /// The list goes up as the client wrote it and the holder reads it
    /// with the same code it reads a local one with, so a filter it
    /// would refuse here is refused there in the same words. What comes
    /// back is the id per entry, which is what the join reply carries.
    ///
    /// The subscriber itself lives on the holder, against the claims in
    /// the token carried here, because deciding what a subscriber may
    /// see of a row is selecting that row back as them. A node that
    /// filtered rows itself would be a node that had already been sent
    /// them.
    pub async fn watch(
        &self,
        me: SocketId,
        token: Option<&str>,
        wants: &[Value],
    ) -> Result<Watched, String> {
        let answered = self
            .asking_for(
                json!({
                    "up": "watch",
                    "socket": me.raw(),
                    "token": token,
                    "wants": wants,
                }),
                WATCHING,
            )
            .await?;
        Ok(Watched {
            ids: answered
                .get("ids")
                .and_then(Value::as_array)
                .map(|ids| ids.iter().filter_map(Value::as_u64).collect())
                .unwrap_or_default(),
            refused: answered
                .get("refused")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    /// Those subscriptions are finished, because the channel they were
    /// asked for on has gone.
    pub async fn unwatch(&self, me: SocketId, ids: Vec<u64>) {
        self.tell(Wire::of(
            json!({"up": "unwatch", "socket": me.raw(), "ids": ids}),
        ))
        .await;
    }

    /// This socket is somebody else now.
    ///
    /// The holder has to be told, unlike every other question on this
    /// link, and for a reason worth writing down: the rest carry the
    /// token they are asked under, but a subscriber is asked nothing. It
    /// sits on the holder and rows are checked against it, so the claims
    /// it is checked against are kept there and a refreshed token has to
    /// reach them or a client would keep seeing what its old token could
    /// see.
    pub async fn became(&self, me: SocketId, token: Option<&str>) {
        self.tell(Wire::of(
            json!({"up": "became", "socket": me.raw(), "token": token}),
        ))
        .await;
    }

    /// One frame up, with no answer expected.
    ///
    /// A queue that is full is a link that is not moving, and the
    /// sockets here are told so rather than left waiting on it. Nothing
    /// blocks: a socket that stopped to wait on the link would be a
    /// socket that stopped reading its client.
    async fn tell(&self, frame: Wire) {
        if self.up.try_send(frame.encode()).is_err() {
            log::warn!("realtime: the link to {} is not moving", self.endpoint);
            self.gapped();
        }
    }

    /// One frame up, and the answer to it.
    async fn asking(&self, head: Value) -> Result<Value, String> {
        self.asking_for(head, ASKING).await
    }

    /// The same, waiting as long as the question deserves.
    async fn asking_for(&self, mut head: Value, wait: Duration) -> Result<Value, String> {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(head) = head.as_object_mut() {
            head.insert("id".into(), json!(id));
        }
        let (answer, waiting) = oneshot::channel();
        self.asked.lock().expect("the link").insert(id, answer);
        self.tell(Wire::of(head)).await;
        match tokio::time::timeout(wait, waiting).await {
            Ok(Ok(answered)) => answered,
            // The link went while this was waiting, or the holder took
            // longer than a client will.
            _ => {
                self.asked.lock().expect("the link").remove(&id);
                Err("the node holding this project did not answer".to_string())
            }
        }
    }

    /// An answer that has arrived, on its way to whoever asked.
    fn answered(&self, id: u64, answer: Result<Value, String>) {
        if let Some(waiting) = self.asked.lock().expect("the link").remove(&id) {
            let _ = waiting.send(answer);
        }
    }

    /// One row, or one gap, for the one socket it belongs to.
    ///
    /// The fan out is here rather than on the holder because a row on
    /// this link has already been decided for exactly one subscriber:
    /// the holder selected it back as that socket's claims before it
    /// crossed. So this end is a lookup and a queue and no filtering at
    /// all.
    fn feed(&self, theirs: u64, heard: Feed) {
        let mut sockets = self.sockets.lock().expect("the link");
        if let Some(to) = sockets.get(&theirs)
            && to.try_send(heard).is_err()
        {
            // Its queue is full, which is a client that has stopped
            // reading its own socket, or the socket has gone. There is no
            // room to tell it there is a gap either, so what it is told
            // is that its feed closed, which the socket loop answers by
            // going. Dropping the sender here is what says it.
            log::warn!("realtime: a socket stopped reading its changes, dropping it");
            sockets.remove(&theirs);
        }
    }

    /// Tell every socket here that it has missed something.
    ///
    /// The same news the change reader gives a subscriber it dropped,
    /// and the socket loop answers it the same way, by closing. A
    /// client that reconnects rejoins its channels and reads whatever
    /// it needs back; a client that carried on would have a hole in its
    /// stream and no way to know.
    fn gapped(&self) {
        let sockets = self.sockets.lock().expect("the link");
        for to in sockets.values() {
            // Try, because a socket that already has one queued has
            // already been told and a second says nothing new.
            let _ = to.try_send(Feed::Gap);
        }
        // And nothing that was waiting on an answer is getting one.
        let waiting: Vec<u64> = self
            .asked
            .lock()
            .expect("the link")
            .keys()
            .copied()
            .collect();
        drop(sockets);
        for id in waiting {
            self.answered(id, Err("the link to this project went".to_string()));
        }
    }

    /// Everything this end of the link holds, said again, because the
    /// holder that answered is not the one that was told it.
    ///
    /// A link that could not be resumed is a holder with nothing on it:
    /// no sockets, so a broadcast from one of them is dropped for
    /// naming a socket that link never announced, and no topics, so
    /// nothing on them ever comes back down. Both are this end's to
    /// say, and saying them again is cheap and idempotent.
    ///
    /// Not the subscriptions. A subscription asked for again comes back
    /// under new ids while the client is still holding the ids its join
    /// reply carried, so what a socket that lost its subscriber gets is
    /// the gap, and it resubscribes for itself.
    async fn again(&self) {
        let sockets: Vec<u64> = self
            .sockets
            .lock()
            .expect("the link")
            .keys()
            .copied()
            .collect();
        for socket in sockets {
            self.tell(Wire::of(json!({"up": "socket", "socket": socket})))
                .await;
        }
        let topics: Vec<String> = self
            .topics
            .lock()
            .expect("the link")
            .keys()
            .cloned()
            .collect();
        for topic in topics {
            self.tell(Wire::of(json!({"up": "carry", "topic": topic})))
                .await;
        }
    }

    /// How many sockets here, for a test and for a metric later.
    pub fn sockets(&self) -> usize {
        self.sockets.lock().expect("the link").len()
    }
}

/// The link, dialled and redialled for as long as this node is up.
///
/// A link that broke is not a gap on its own. The holder keeps what it
/// sent and what this node's sockets held for [`GRACE`], so a link back
/// inside that is handed the frames it missed and nothing here notices.
/// What is a gap is a link that stays down past it, because then what
/// was being kept has been given back, and a gap is also what a holder
/// says when it cannot resume: the change stream over there is a
/// temporary slot which retains nothing, so past the ring there is no
/// log to replay out of and the client is the one that reconnects and
/// reads.
async fn dialling(app: Weak<App>, away: Arc<Away>, mut queue: mpsc::Receiver<Vec<u8>>) {
    let mut wait = FIRST;
    // Whether there has ever been a link at all, since when this
    // outage started, and whether the sockets here have been told about
    // it. The last two are reset by a connection that opens, because a
    // link that opens either resumes or says it could not, and either
    // way the answer comes from the holder rather than a clock here.
    let mut ever = false;
    let mut went: Option<Instant> = None;
    let mut told = false;
    loop {
        if app.upgrade().is_none() {
            // The tenant this link belonged to has gone, which on a
            // fleet node is an eviction and on a single project node is
            // the process shutting down.
            return;
        }
        match tokio_tungstenite::connect_async(away.url()).await {
            Ok((socket, _)) => {
                log::info!("realtime: linked to {}", away.endpoint);
                wait = FIRST;
                ever = true;
                went = None;
                told = false;
                carried(socket, &app, &away, &mut queue).await;
                log::warn!("realtime: the link to {} ended", away.endpoint);
            }
            Err(e) => log::warn!(
                "realtime: the link to {} would not open, {e}",
                away.endpoint
            ),
        }
        // A first dial that fails is not an outage to be waited out.
        // Nothing has ever been kept for this node, so there is nothing
        // for a grace to protect, and a socket whose join went nowhere
        // is told at once rather than half a minute later.
        if !ever {
            away.gapped();
        } else {
            let since = *went.get_or_insert_with(Instant::now);
            if !told && since.elapsed() >= GRACE {
                log::warn!(
                    "realtime: the link to {} has been down {}s, past the {}s the holder keeps a link's {KEPT} frames, so the sockets here are told there is a gap",
                    away.endpoint,
                    since.elapsed().as_secs(),
                    GRACE.as_secs(),
                );
                away.gapped();
                told = true;
            }
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(MOST);
    }
}

type Linked =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// One connection to the holder, until it ends.
async fn carried(
    socket: Linked,
    app: &Weak<App>,
    away: &Arc<Away>,
    queue: &mut mpsc::Receiver<Vec<u8>>,
) {
    // Split, so that sending a frame does not wait on the next one
    // arriving and the other way round.
    let (mut writer, mut reader) = socket.split();
    // Which link this is and where its numbering got to, which is the
    // whole of what a reconnect has to say for itself: everything the
    // holder needs to work out whether it can carry on.
    let hello = Wire::of(json!({
        "up": "hello",
        "version": VERSION,
        "link": away.link,
        "seen": away.seen.load(Ordering::Relaxed),
    }))
    .encode();
    if writer
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            hello.into(),
        ))
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            going = queue.recv() => {
                let Some(bytes) = going else { return };
                if writer
                    .send(tokio_tungstenite::tungstenite::Message::Binary(bytes.into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            coming = reader.next() => {
                let Some(Ok(message)) = coming else { return };
                let bytes = match message {
                    tokio_tungstenite::tungstenite::Message::Binary(bytes) => bytes,
                    // Ping and pong are the transport's and tungstenite
                    // answers them itself. A close is the holder going.
                    tokio_tungstenite::tungstenite::Message::Close(_) => return,
                    _ => continue,
                };
                let Some(frame) = Wire::decode(&bytes) else {
                    log::warn!("realtime: the holder sent something that is not a frame");
                    return;
                };
                // The handshake sits outside the numbering, because it
                // is the frame that says where the numbering starts.
                // Everything after it is numbered from there, resumed
                // or not, so the check below holds either way.
                if frame.down() == "hello" {
                    if !greeting(away, &frame).await {
                        return;
                    }
                    continue;
                }
                let Some(seq) = frame.number("seq") else {
                    log::warn!("realtime: the holder sent a frame with no number on it");
                    return;
                };
                let seen = away.seen.load(Ordering::Relaxed);
                if seq != seen + 1 {
                    log::warn!("realtime: the link missed frame {} of {}", seen + 1, away.endpoint);
                    return;
                }
                away.seen.store(seq, Ordering::Relaxed);
                let Some(app) = app.upgrade() else { return };
                if !took(&app, away, frame).await {
                    return;
                }
            }
        }
    }
}

/// The holder's answer to the hello, which says whether it still had
/// this link. False ends the link.
async fn greeting(away: &Arc<Away>, frame: &Wire) -> bool {
    if let Err(theirs) = agrees(frame) {
        log::warn!(
            "realtime: {} speaks link version {theirs} and this node speaks {VERSION}",
            away.endpoint
        );
        return false;
    }
    if frame.head.get("resumed").and_then(Value::as_bool) == Some(true) {
        log::info!(
            "realtime: the link to {} carried on after frame {}",
            away.endpoint,
            away.seen.load(Ordering::Relaxed),
        );
        return true;
    }
    // A link starting over. Whatever went down the old one while this
    // node was away is not coming and cannot be named, so the sockets
    // here are told there is a gap, exactly once and only if there was
    // an old one: a first dial has missed nothing.
    if away.seen.swap(0, Ordering::Relaxed) > 0 {
        log::warn!(
            "realtime: {} could not carry the link on, so the sockets here are told there is a gap",
            away.endpoint,
        );
        away.gapped();
    }
    // And a holder that could not carry it on is a holder that knows
    // nothing about this node, which is this node's to fix.
    away.again().await;
    true
}

/// One frame from the holder. False ends the link.
async fn took(app: &Arc<App>, away: &Arc<Away>, frame: Wire) -> bool {
    match frame.down() {
        "reply" => {
            let Some(id) = frame.number("for") else {
                return true;
            };
            let answer = match frame.text("error") {
                Some(why) => Err(why.to_string()),
                None => Ok(frame.head.get("ok").cloned().unwrap_or(Value::Null)),
            };
            away.answered(id, answer);
        }
        // Everything a topic this node carries has sent, on its way to
        // the sockets here that are on it.
        "sent" => {
            let from = match frame.number("from") {
                // One of this node's own, which is how a broadcast that
                // asked not to be echoed is kept from its own sender.
                Some(mine) => SocketId::of(mine),
                None => SocketId::elsewhere(),
            };
            let sent = if let Some(diff) = frame.head.get("diff") {
                let Some(topic) = diff.get("topic").and_then(Value::as_str) else {
                    return true;
                };
                Sent::Diff {
                    topic: topic.to_string(),
                    payload: diff.get("payload").cloned().unwrap_or(Value::Null),
                }
            } else if let Some(fan) = frame.head.get("broadcast") {
                match broadcast_of(fan, frame.body) {
                    Some(fan) => Sent::Broadcast(fan),
                    None => return true,
                }
            } else {
                return true;
            };
            let reached = app.hub.relay(from, sent);
            if reached > 0 {
                // Counted here because this is the only node that knows
                // it, and told to the holder because the budget is the
                // project's rather than this node's.
                app.quota.spent(reached as u64);
                away.tell(Wire::of(json!({"up": "spent", "events": reached})))
                    .await;
            }
        }
        // A row for one subscriber here, already checked against that
        // socket's own claims by the node with the database.
        "changed" => {
            let Some(theirs) = frame.number("socket") else {
                return true;
            };
            let ids: Vec<u64> = frame
                .head
                .get("ids")
                .and_then(Value::as_array)
                .map(|ids| ids.iter().filter_map(Value::as_u64).collect())
                .unwrap_or_default();
            away.feed(
                theirs,
                Feed::Change {
                    ids,
                    data: Arc::new(frame.head.get("data").cloned().unwrap_or(Value::Null)),
                    commit_ts: frame
                        .head
                        .get("commit_ts")
                        .and_then(Value::as_i64)
                        .unwrap_or_default(),
                    // When the holder read it, said as how long ago that
                    // was, because two nodes have two clocks and neither
                    // can be told in the other's. What the metric wants
                    // is the whole journey, so the crossing counts: this
                    // is that instant moved back by the time the row has
                    // already spent travelling.
                    read: Instant::now()
                        - Duration::from_micros(frame.number("waited").unwrap_or_default()),
                    // The same, for the stage that starts where the
                    // holder stopped deciding. A holder on an older
                    // release does not send it, and the fallback is
                    // this instant, which reads as a stage that started
                    // when the frame arrived rather than as a negative.
                    queued: Instant::now()
                        - Duration::from_micros(frame.number("decided").unwrap_or_default()),
                },
            );
        }
        // One subscriber here missed rows, which is this node not
        // draining its own end fast enough for that one socket.
        "gapped" => {
            if let Some(theirs) = frame.number("socket") {
                away.feed(theirs, Feed::Gap);
            }
        }
        "gap" => away.gapped(),
        other => log::debug!("realtime: the holder sent a {other} frame, which this node ignores"),
    }
    true
}

/// The holder's end, `/realtime/v1/link`.
///
/// An ordinary websocket route on the port this node already serves, so
/// there is no second listener and nothing new for an operator to open.
/// The apikey gate has already run, which is what authorizes the link
/// itself: it is the project's own infrastructure and it presents the
/// project's service key. What it may do on behalf of one socket is a
/// separate question with a separate answer, which is the token that
/// socket is running on, carried on the frame that asks.
pub async fn link(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(auth): axum::Extension<AuthContext>,
    upgrade: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Response {
    let Ok(upgrade) = upgrade else {
        return json_body(
            StatusCode::UPGRADE_REQUIRED,
            json!({"message": "this endpoint is a websocket, upgrade required"}),
        );
    };
    if auth.role != "service_role" {
        return json_body(
            StatusCode::FORBIDDEN,
            json!({"message": "this endpoint is for the nodes serving this project"}),
        );
    }
    // A node that does not hold the tenant cannot answer for it, and a
    // link to one would be a link to a link. The lease moved, which the
    // node dialling this will find out for itself when it reads it.
    if !app.source.held() {
        return json_body(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"message": "this node does not hold this project"}),
        );
    }
    upgrade.on_upgrade(move |socket| async move {
        app.sending
            .get_or_init(|| async { crate::realtime::deliver(Arc::clone(&app)) })
            .await;
        holding(socket, &app).await;
    })
}

/// One socket on the far end of a link, which is a whole node.
struct Ashore {
    /// What the node calls this link, off its hello. Zero for a node
    /// old enough not to name one, which is a link that cannot be
    /// resumed and is not asking to be.
    link: u64,
    /// What each socket on the node is called here. The node names its
    /// own with numbers that mean nothing off its link, so presence and
    /// the sender check need a name of this hub's own.
    sockets: HashMap<u64, Held>,
    /// And the way back, for saying who a message came from in the
    /// numbers that link uses.
    theirs: HashMap<SocketId, u64>,
    /// The topics that node is carrying, each with the receiver this
    /// end hears them through. One per topic however many sockets the
    /// node has on it, because the node fans out its own.
    carrying: HashMap<String, tokio::sync::broadcast::Receiver<Delivery>>,
    /// The one queue every subscriber on this link shares, and what the
    /// change reader was handed when each of them was made.
    ///
    /// One queue rather than one per socket, because a link is a hundred
    /// thousand sockets and a hundred thousand receivers is not
    /// something to wake up and poll. Each row on it names the socket it
    /// was decided for, and the node it goes to does the fan out, which
    /// is where the sockets are.
    changes: Arc<Shared>,
    /// What has gone down this link, so that a hole in it is something
    /// the other end can see.
    seq: u64,
    /// The last [`KEPT`] frames that went down it, exactly as they went,
    /// so that a link which comes back can be given the ones it missed
    /// rather than every socket behind it being closed. The bytes are
    /// shared with what was sent, so keeping one is a refcount.
    kept: VecDeque<(u64, Bytes)>,
}

/// What a link that dropped left behind, waiting for it to dial again.
struct Kept {
    node: Ashore,
    /// The one queue its subscribers share, still being filled by the
    /// change reader while nobody is draining it. That is the point: a
    /// subscriber whose node blinked reads its rows when the node is
    /// back, and a queue that fills says there was a gap the same way
    /// it would with the link up.
    rows: mpsc::Receiver<(u64, Feed)>,
}

/// Every link that dropped and might come back, by the id its node
/// gave it.
///
/// One per project rather than one per node, because the node it
/// belongs to is by definition not connected. Bounded by the fleet: one
/// entry per node that dropped, each let go [`GRACE`] after it did.
#[derive(Default)]
pub struct Dropped(Mutex<HashMap<u64, Kept>>);

impl Dropped {
    fn keep(&self, link: u64, kept: Kept) {
        self.0.lock().expect("the dropped links").insert(link, kept);
    }

    fn take(&self, link: u64) -> Option<Kept> {
        self.0.lock().expect("the dropped links").remove(&link)
    }

    /// How many links are being held, for a test and for a metric
    /// later.
    pub fn len(&self) -> usize {
        self.0.lock().expect("the dropped links").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One socket on the far end.
struct Held {
    /// What it is called on this hub.
    id: SocketId,
    /// The topics it is on the presence of, so that a node going takes
    /// its sockets off every topic they were on and nobody else's.
    tracking: HashSet<String>,
    /// The change reader's name for it, once it has asked to watch a
    /// table. None until then, because a socket that only broadcasts is
    /// not a subscriber the reader has to consider for every changed
    /// row, on this side of a link exactly as on the other.
    watching: Option<u64>,
}

/// What woke the holder's end.
///
/// The hub's own enum plus the changes, which the socket loop cannot use
/// as it stands: a row here is for one of the sockets on the far end, so
/// it arrives with that node's name for it on it.
enum Woke {
    Hub(Heard),
    Changed(Option<(u64, Feed)>),
}

async fn holding(mut socket: WebSocket, app: &Arc<App>) {
    // The hello comes first and decides everything: whether this is a
    // new link or the one that dropped, and in the second case what it
    // is owed. Nothing else is read until it has been answered.
    let Some((mut node, mut rows)) = greeted(&mut socket, app).await else {
        return;
    };
    loop {
        let woke = if node.carrying.is_empty() {
            // Waiting on an empty set of receivers would be a future
            // that never wakes, which is a node that has said hello and
            // has nobody on a topic yet.
            tokio::select! {
                message = socket.recv() => Woke::Hub(Heard::Client(message)),
                row = rows.recv() => Woke::Changed(row),
            }
        } else {
            tokio::select! {
                message = socket.recv() => Woke::Hub(Heard::Client(message)),
                fanned = next_delivery(&mut node.carrying) => Woke::Hub(fanned),
                row = rows.recv() => Woke::Changed(row),
            }
        };
        let carry_on = match woke {
            // A row for one of that node's sockets. The check already
            // happened, against the claims in the token that socket is
            // running on, so what crosses is a row decided for exactly
            // one subscriber and the node it goes to filters nothing.
            Woke::Changed(Some((
                named,
                Feed::Change {
                    ids,
                    data,
                    commit_ts,
                    read,
                    queued,
                },
            ))) => {
                let head = json!({
                    "down": "changed",
                    "socket": named,
                    "ids": ids,
                    "data": *data,
                    "commit_ts": commit_ts,
                    // How long ago this node read it, rather than when,
                    // because the other end has its own clock and no way
                    // to read this one. It is the crossing that this
                    // makes countable: the latency a client sees is
                    // measured from the read and the read happened here.
                    "waited": read.elapsed().as_micros() as u64,
                    // And how long ago the reader here finished deciding
                    // it, said the same way and for the same reason. The
                    // stage it starts is everything after the decision,
                    // which away is a queue, a link, a crossing and a
                    // socket rather than only the first and the last, so
                    // it has to carry the crossing to mean anything.
                    "decided": queued.elapsed().as_micros() as u64,
                });
                down(&mut node, &mut socket, head, Vec::new()).await
            }
            // The reader has stopped sending rows to one subscriber, or
            // to all of them.
            Woke::Changed(Some((named, Feed::Gap))) => {
                down(
                    &mut node,
                    &mut socket,
                    json!({"down": "gapped", "socket": named}),
                    Vec::new(),
                )
                .await
            }
            // This end holds a sender for as long as the link is up, so
            // this is not something that happens. Ending the link is the
            // safe answer to it anyway: the node redials and its sockets
            // are told there was a gap.
            Woke::Changed(None) => false,
            Woke::Hub(Heard::Client(None) | Heard::Client(Some(Ok(Message::Close(_))))) => false,
            Woke::Hub(Heard::Client(Some(Err(e)))) => {
                log::debug!("realtime: a link went, {e}");
                false
            }
            Woke::Hub(Heard::Client(Some(Ok(Message::Binary(bytes))))) => {
                match Wire::decode(&bytes) {
                    Some(frame) => asked(app, &mut node, frame, &mut socket).await,
                    None => {
                        log::warn!("realtime: a link sent something that is not a frame");
                        false
                    }
                }
            }
            Woke::Hub(Heard::Client(Some(Ok(_)))) => true,
            Woke::Hub(Heard::Fanned(delivery)) => {
                let (from, sent) = &*delivery;
                let mine = node.theirs.get(from).copied();
                down(&mut node, &mut socket, sent_head(sent, mine), payload(sent)).await
            }
            // A node so far behind that the hub gave up holding its
            // backlog. It is told, and it tells its own sockets, which
            // is the same bargain a socket here gets and for the same
            // reason: one wedged node cannot be everybody's problem.
            Woke::Hub(Heard::Lagged(topic, missed)) => {
                log::warn!("realtime: a link missed {missed} messages on {topic}");
                down(&mut node, &mut socket, json!({"down": "gap"}), Vec::new()).await
            }
            // The changes on this end arrive as `Woke::Changed`, because
            // they are for one of the far node's sockets rather than for
            // a socket of this node's own.
            Woke::Hub(Heard::Changed(_)) => true,
        };
        if !carry_on {
            break;
        }
        // And whatever the reader could not fit in the shared queue,
        // which is this link not being drained. One gap for the whole
        // link however many rows it was, because there is no way to say
        // which they were, and the far end tells every socket on it.
        if node.changes.behind() {
            log::warn!("realtime: a link is not draining its changes, telling it there is a gap");
            if !down(&mut node, &mut socket, json!({"down": "gap"}), Vec::new()).await {
                break;
            }
        }
    }
    // The link went, which is almost always the link coming back. What
    // it held is kept under its name for the grace rather than given
    // back now, so that a node which redials in a second finds its
    // sockets still in the rooms they were in and its subscribers still
    // subscribed. A node that never says its name has nothing to come
    // back as, so its end is its end.
    match node.link {
        0 => release(app, node),
        link => keep(app, link, Kept { node, rows }),
    }
}

/// The handshake, which is where a link finds out whether it is the one
/// that dropped. None ends it.
async fn greeted(
    socket: &mut WebSocket,
    app: &Arc<App>,
) -> Option<(Ashore, mpsc::Receiver<(u64, Feed)>)> {
    let Some(Ok(Message::Binary(bytes))) = socket.recv().await else {
        return None;
    };
    let frame = Wire::decode(&bytes)?;
    if frame.up() != "hello" {
        log::warn!(
            "realtime: a link sent a {} frame before it said hello",
            frame.up()
        );
        return None;
    }
    if let Err(theirs) = agrees(&frame) {
        log::warn!("realtime: a node speaks link version {theirs} and this one {VERSION}");
        return None;
    }
    let link = frame.number("link").unwrap_or_default();
    let seen = frame.number("seen").unwrap_or_default();
    // What was kept for this link, if it is still here and if what it
    // is asking for is still in the ring.
    let resumed = match link {
        0 => None,
        link => match app.links.take(link) {
            Some(kept) if replayable(&kept.node, seen) => Some(kept),
            Some(kept) => {
                log::warn!(
                    "realtime: link {link} came back asking for frame {}, and this end has {} frames ending at {}, so it starts over",
                    seen + 1,
                    kept.node.kept.len(),
                    kept.node.seq,
                );
                release(app, kept.node);
                None
            }
            None => None,
        },
    };
    // Answered before anything is replayed, so that a node which is
    // starting over knows it before a numbered frame reaches it.
    let hello = Wire::of(json!({
        "down": "hello",
        "version": VERSION,
        "resumed": resumed.is_some(),
    }));
    let greeted = socket
        .send(Message::Binary(hello.encode().into()))
        .await
        .is_ok();
    let Some(Kept { mut node, rows }) = resumed else {
        if !greeted {
            return None;
        }
        let (changes, rows) = Shared::new(CHANGES);
        return Some((
            Ashore {
                link,
                sockets: HashMap::new(),
                theirs: HashMap::new(),
                carrying: HashMap::new(),
                changes,
                seq: 0,
                kept: VecDeque::new(),
            },
            rows,
        ));
    };
    // What went down the old link after the number the node got to, in
    // the order it went and under the numbers it went under. That is
    // the whole of the resume: the node's own check that each frame is
    // the one after the last holds straight through it, and everything
    // sent from here on carries on from where the old link stopped.
    let again: Vec<Bytes> = node
        .kept
        .iter()
        .filter(|(seq, _)| *seq > seen)
        .map(|(_, bytes)| bytes.clone())
        .collect();
    log::info!(
        "realtime: link {link} came back at frame {seen} and is being given the {} it missed, up to {}",
        again.len(),
        node.seq,
    );
    let mut carried = greeted;
    for bytes in again {
        carried = carried && socket.send(Message::Binary(bytes)).await.is_ok();
    }
    if !carried {
        // It went again mid replay. Which is the same thing that just
        // happened, so it is kept again on the same terms rather than
        // thrown away for having bad luck twice.
        keep(app, link, Kept { node, rows });
        return None;
    }
    node.link = link;
    Some((node, rows))
}

/// Whether a link asking to carry on after `seen` can be given what it
/// missed out of what is still kept.
///
/// Three cases, and only the middle one needs the ring: a node that
/// says it saw more than was ever sent is not the link this end was
/// holding, one that is exactly up to date needs nothing replayed, and
/// one that is behind needs the ring to still reach back to the frame
/// after the last it saw.
fn replayable(node: &Ashore, seen: u64) -> bool {
    match seen {
        seen if seen > node.seq => false,
        seen if seen == node.seq => true,
        seen => matches!(node.kept.front(), Some((oldest, _)) if *oldest <= seen + 1),
    }
}

/// Hold what a link left behind, and give it back if it does not come
/// for it.
fn keep(app: &Arc<App>, link: u64, kept: Kept) {
    app.links.keep(link, kept);
    let app = Arc::downgrade(app);
    tokio::spawn(async move {
        tokio::time::sleep(GRACE).await;
        let Some(app) = app.upgrade() else { return };
        // Gone already means it came back and took it, which is the
        // ordinary ending.
        let Some(kept) = app.links.take(link) else {
            return;
        };
        log::warn!(
            "realtime: link {link} has been gone {}s, so what its sockets held is given back",
            GRACE.as_secs(),
        );
        release(&app, kept.node);
    });
}

/// Everything a node's sockets held, given back. A node that went and
/// left its presence behind would be a room full of people who are not
/// there, and one that left its subscribers behind would be a policy
/// check per changed row for nobody.
fn release(app: &Arc<App>, node: Ashore) {
    for socket in node.sockets.values() {
        for topic in &socket.tracking {
            app.hub.untrack(socket.id, topic);
        }
        if let Some(listener) = socket.watching {
            app.changes.hung_up(listener);
        }
    }
    // The receivers go before the hub is told, so that a topic nobody
    // is left on is a topic with nothing still listening to it.
    let topics: Vec<String> = node.carrying.keys().cloned().collect();
    drop(node.carrying);
    for topic in &topics {
        app.hub.released(topic);
    }
}

/// What a message going down looks like, and who it came from.
fn sent_head(sent: &Sent, from: Option<u64>) -> Value {
    let mut head = json!({"down": "sent", "from": from});
    if let Some(head) = head.as_object_mut() {
        match sent {
            Sent::Broadcast(fan) => head.insert("broadcast".into(), broadcast_head(fan)),
            Sent::Diff { topic, payload } => {
                head.insert("diff".into(), json!({"topic": topic, "payload": payload}))
            }
        };
    }
    head
}

fn payload(sent: &Sent) -> Vec<u8> {
    match sent {
        Sent::Broadcast(fan) => fan.push.payload.clone(),
        Sent::Diff { .. } => Vec::new(),
    }
}

/// One frame down the link, numbered and kept. False ends it.
async fn down(node: &mut Ashore, socket: &mut WebSocket, mut head: Value, body: Vec<u8>) -> bool {
    node.seq += 1;
    if let Some(head) = head.as_object_mut() {
        head.insert("seq".into(), json!(node.seq));
    }
    let bytes = Bytes::from(Wire::with(head, body).encode());
    // Kept as it goes, so that a link which drops and comes back inside
    // the grace is handed what it missed instead of every socket behind
    // it being closed. The oldest goes when the ring is full, which is
    // what keeps a node nobody can reach to a bounded cost here.
    if node.kept.len() == KEPT {
        node.kept.pop_front();
    }
    node.kept.push_back((node.seq, bytes.clone()));
    socket.send(Message::Binary(bytes)).await.is_ok()
}

/// One frame from a node. False ends the link.
async fn asked(app: &Arc<App>, node: &mut Ashore, frame: Wire, socket: &mut WebSocket) -> bool {
    match frame.up() {
        // The handshake happened before this loop started. A second one
        // is a node saying hello twice, which changes nothing here.
        "hello" => {}
        "socket" => {
            let Some(theirs) = frame.number("socket") else {
                return true;
            };
            // Idempotent, because a node whose link could not be
            // resumed says its sockets again and a holder that made a
            // second name for one would leave the first in the hub with
            // nothing pointing at it.
            if node.sockets.contains_key(&theirs) {
                return true;
            }
            let id = app.hub.socket();
            node.sockets.insert(
                theirs,
                Held {
                    id,
                    tracking: HashSet::new(),
                    watching: None,
                },
            );
            node.theirs.insert(id, theirs);
        }
        "gone" => {
            let Some(theirs) = frame.number("socket") else {
                return true;
            };
            if let Some(held) = node.sockets.remove(&theirs) {
                for topic in &held.tracking {
                    app.hub.untrack(held.id, topic);
                }
                if let Some(listener) = held.watching {
                    app.changes.hung_up(listener);
                }
                node.theirs.remove(&held.id);
            }
        }
        "carry" => {
            let Some(topic) = frame.text("topic") else {
                return true;
            };
            node.carrying
                .entry(topic.to_string())
                .or_insert_with(|| app.hub.carry(topic));
        }
        "leave" => {
            let Some(topic) = frame.text("topic") else {
                return true;
            };
            // A topic this link never carried is a frame from before a
            // reconnect, queued on the node while the link was down and
            // sent to a holder that knows nothing about it. Releasing
            // it would be giving back something somebody else is
            // holding, so an unknown one is dropped.
            if node.carrying.remove(topic).is_some() {
                app.hub.released(topic);
            }
        }
        "fan" => {
            let Some(id) = frame
                .number("socket")
                .and_then(|theirs| named(node, theirs))
            else {
                return true;
            };
            let Some(fan) = broadcast_of(&frame.head, frame.body) else {
                return true;
            };
            // What the message cost the project is what it reached,
            // which is the sockets here plus the links it crossed. A
            // link is one message this server moved however many
            // sockets are behind it, and what it cost on the other side
            // is counted there and reported back.
            app.quota.sent(app.hub.fan(id, fan));
        }
        "track" => {
            let Some(theirs) = frame.number("socket") else {
                return true;
            };
            let Some(topic) = frame.text("topic").map(str::to_string) else {
                return true;
            };
            let Some(held) = node.sockets.get_mut(&theirs) else {
                return true;
            };
            let key = frame.text("key").map(str::to_string);
            let payload = frame.head.get("payload").cloned().unwrap_or(Value::Null);
            held.tracking.insert(topic.clone());
            app.hub.track(held.id, &topic, key, payload);
        }
        "untrack" => {
            let Some(theirs) = frame.number("socket") else {
                return true;
            };
            let Some(topic) = frame.text("topic").map(str::to_string) else {
                return true;
            };
            let Some(held) = node.sockets.get_mut(&theirs) else {
                return true;
            };
            held.tracking.remove(&topic);
            app.hub.untrack(held.id, &topic);
        }
        "state" => {
            let Some(id) = frame.number("id") else {
                return true;
            };
            let state = frame
                .text("topic")
                .map_or_else(|| json!({}), |topic| app.hub.state(topic));
            return answer(node, socket, id, Ok(state)).await;
        }
        "ask" => {
            let Some(id) = frame.number("id") else {
                return true;
            };
            let asked = Ask {
                topic: frame.text("topic").unwrap_or_default().to_string(),
                about: match frame.text("about") {
                    Some("join") => About::Join,
                    Some("presence") => About::Presence,
                    _ => About::Broadcast,
                },
            };
            let answered = match who(app, frame.text("token")) {
                Ok(who) => crate::realtime::answer(app, &who, &asked)
                    .await
                    .map(|grant| json!({"broadcast": grant.broadcast, "presence": grant.presence})),
                Err(why) => Err(why),
            };
            return answer(node, socket, id, answered).await;
        }
        // What one of that node's sockets wants to be told about a
        // table. The subscriber is made here, against the claims in the
        // token on this frame, because the thing that decides what it
        // may see of a row is a select of that row as them.
        //
        // Answered in line, like every other question on this link, so
        // the reply is in front of any row for it: the client is told
        // which id belongs to which subscription before it is sent
        // anything under one. The cost of that is the link waiting on a
        // database while it is set up, which is a publication read, and
        // on the very first subscription a wait for the tap.
        "watch" => {
            let Some(id) = frame.number("id") else {
                return true;
            };
            let Some(theirs) = frame.number("socket") else {
                return true;
            };
            let wants: Vec<Value> = frame
                .head
                .get("wants")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let answered = match who(app, frame.text("token")) {
                Ok(who) => match subscriber(app, node, theirs, &who) {
                    Some(listener) => crate::realtime::subscribed_on(app, listener, &wants)
                        .await
                        .map(|watched| json!({"ids": watched.ids, "refused": watched.refused})),
                    // A frame for a socket this link never announced,
                    // which is one queued on the node before a reconnect
                    // and sent to a holder that knows nothing about it.
                    None => Err("this link does not have that socket".to_string()),
                },
                Err(why) => Err(why),
            };
            return answer(node, socket, id, answered).await;
        }
        "unwatch" => {
            let ids = frame.head.get("ids").and_then(Value::as_array);
            for id in ids.into_iter().flatten().filter_map(Value::as_u64) {
                app.changes.unbind(id);
            }
        }
        // A socket over there refreshed its token, so the claims its
        // rows are checked against here are the new ones from the next
        // row on.
        "became" => {
            let Some(held) = frame
                .number("socket")
                .and_then(|theirs| node.sockets.get(&theirs))
            else {
                return true;
            };
            let Some(listener) = held.watching else {
                return true;
            };
            match who(app, frame.text("token")) {
                Ok(who) => app.changes.became(listener, crate::realtime::asker(&who)),
                // A token that will not verify is not a reason to change
                // what this subscriber may see. The socket's own node
                // refuses it too, and this frame is the one after that.
                Err(why) => {
                    log::debug!("realtime: a link sent a token that will not verify, {why}")
                }
            }
        }
        // What a node spent of the project's budget on the messages it
        // was handed, which is the one number it knows and this one
        // does not.
        "spent" => app.quota.spent(frame.number("events").unwrap_or_default()),
        other => log::debug!("realtime: a node sent a {other} frame, which the holder ignores"),
    }
    true
}

/// Who a question is asked as.
///
/// The token is verified here rather than taken on the node's word for
/// it, so a link is worth no more than the tokens carried on it. A
/// frame with no token at all is the project's anonymous role, which is
/// what a socket that connected with nothing but the project's own key
/// runs as.
fn who(app: &Arc<App>, token: Option<&str>) -> Result<Identity, String> {
    let Some(token) = token else {
        return Ok(Identity {
            role: app.cfg.anon_role.clone(),
            claims: Value::Null,
        });
    };
    match crate::jwt::verify_any(token, &app.cfg.jwt_secret, app.jwks.as_ref()) {
        Ok(verified) => {
            let role = match verified.role.as_deref() {
                None | Some("") => app.cfg.anon_role.clone(),
                Some(role) => role.to_string(),
            };
            // The hub asks its questions as this role, so it is held
            // to the project's set the way a socket's own token is.
            // See #92.
            if !app.cfg.exposes(&role) {
                return Err(format!("role \"{role}\" is not exposed"));
            }
            Ok(Identity {
                role,
                claims: verified.claims,
            })
        }
        Err(_) => Err("invalid claim: token is expired or malformed".to_string()),
    }
}

/// What this hub calls one of a node's sockets.
fn named(node: &Ashore, theirs: u64) -> Option<SocketId> {
    node.sockets.get(&theirs).map(|held| held.id)
}

/// The change reader's name for one of a node's sockets, made on the
/// first table it asks to watch.
///
/// On the first, because that is when there is something to check rows
/// against and not before: a socket that only broadcasts is not a
/// subscriber the reader walks for every row, on this side of a link
/// exactly as on the other. On the ones after it, the claims are
/// refreshed, since a second channel on one socket may have joined with
/// a newer token than the first did.
fn subscriber(app: &Arc<App>, node: &mut Ashore, theirs: u64, who: &Identity) -> Option<u64> {
    let changes = Arc::clone(&node.changes);
    let held = node.sockets.get_mut(&theirs)?;
    match held.watching {
        Some(listener) => {
            app.changes.became(listener, crate::realtime::asker(who));
            Some(listener)
        }
        None => {
            let listener = app
                .changes
                .listening_on(crate::realtime::asker(who), theirs, changes);
            held.watching = Some(listener);
            Some(listener)
        }
    }
}

async fn answer(
    node: &mut Ashore,
    socket: &mut WebSocket,
    id: u64,
    answered: Result<Value, String>,
) -> bool {
    let head = match answered {
        Ok(ok) => json!({"down": "reply", "for": id, "ok": ok}),
        Err(why) => json!({"down": "reply", "for": id, "error": why}),
    };
    down(node, socket, head, Vec::new()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_fan() -> Fanout {
        Fanout {
            topic: "realtime:room".into(),
            push: BinaryBroadcast {
                join_ref: "1".into(),
                reference: "2".into(),
                topic: "realtime:room".into(),
                event: "cursor".into(),
                meta: r#""at":1"#.into(),
                encoding: Encoding::Binary,
                payload: vec![0, 159, 146, 150],
            },
        }
    }

    /// A node and a holder on different releases is what a rolling
    /// restart is made of, so the hello is where the two of them find
    /// out. What matters most is the third case: a hello that carries
    /// no version is an end old enough not to have sent one, and the
    /// safe reading of silence is disagreement.
    #[test]
    fn a_link_opens_only_between_ends_that_speak_the_same_version() {
        assert_eq!(
            agrees(&Wire::of(json!({"up": "hello", "version": VERSION}))),
            Ok(())
        );
        assert_eq!(
            agrees(&Wire::of(json!({"up": "hello", "version": VERSION + 1}))),
            Err(VERSION + 1)
        );
        assert_eq!(agrees(&Wire::of(json!({"up": "hello"}))), Err(0));
    }

    #[test]
    fn a_frame_is_its_header_and_its_bytes() {
        let frame = Wire::with(json!({"up": "fan", "socket": 7}), vec![1, 2, 3]);
        let bytes = frame.encode();
        assert_eq!(Wire::decode(&bytes), Some(frame));
    }

    #[test]
    fn half_a_frame_is_nothing_rather_than_half_a_message() {
        let bytes = Wire::with(json!({"up": "fan"}), vec![1, 2, 3]).encode();
        for cut in 0..bytes.len() - 3 {
            assert_eq!(Wire::decode(&bytes[..cut]), None, "{cut} bytes of it");
        }
        // And a length that says more header than there is message.
        assert_eq!(Wire::decode(&[0, 0, 255, 0, b'{', b'}']), None);
    }

    #[test]
    fn a_broadcast_crosses_as_it_was_sent() {
        // The payload is not json and is not touched, which is the
        // whole reason the bytes are outside the header.
        let fan = a_fan();
        let frame = Wire::with(broadcast_head(&fan), fan.push.payload.clone());
        let bytes = frame.encode();
        let read = Wire::decode(&bytes).expect("a frame");
        assert_eq!(broadcast_of(&read.head, read.body), Some(fan));
    }

    #[test]
    fn a_link_url_is_the_holders_own_port() {
        let away = Away::new("http://10.0.0.4:8000/", b"a-secret");
        assert!(
            away.url()
                .starts_with("ws://10.0.0.4:8000/realtime/v1/link?apikey="),
            "{}",
            away.url()
        );
        assert!(
            Away::new("https://holder.example", b"a-secret")
                .url()
                .starts_with("wss://holder.example/realtime/v1/link"),
        );
    }

    /// The one number that decides whether a link can carry on, read
    /// three ways: from a link that is up to date, one that is behind
    /// but still inside the ring, and one that fell off the back of it.
    #[test]
    fn a_link_can_carry_on_only_from_a_frame_the_ring_still_reaches() {
        let (changes, _rows) = Shared::new(CHANGES);
        let mut node = Ashore {
            link: 7,
            sockets: HashMap::new(),
            theirs: HashMap::new(),
            carrying: HashMap::new(),
            changes,
            seq: 0,
            kept: VecDeque::new(),
        };
        // Nothing sent yet, so a link that has seen nothing is up to
        // date and one that claims to have seen something is not the
        // link this end was holding.
        assert!(replayable(&node, 0));
        assert!(!replayable(&node, 1));

        // Twice the ring, so the first half of it has been dropped.
        for _ in 0..KEPT * 2 {
            node.seq += 1;
            if node.kept.len() == KEPT {
                node.kept.pop_front();
            }
            node.kept.push_back((node.seq, Bytes::new()));
        }
        assert!(replayable(&node, node.seq), "it is up to date");
        let oldest = node.seq - KEPT as u64;
        assert!(replayable(&node, oldest), "the ring reaches back to it");
        assert!(!replayable(&node, oldest - 1), "it fell off the back");
        assert!(!replayable(&node, node.seq + 1), "it saw more than went");
    }

    #[tokio::test]
    async fn a_node_tells_the_holder_about_a_topic_once() {
        // However many sockets here join it, because the node fans out
        // its own copy: the holder is sending one message per node and
        // not one per socket.
        let away = Arc::new(Away::new("http://10.0.0.4:8000", b"a-secret"));
        let mut queue = away
            .queue
            .lock()
            .expect("the link")
            .take()
            .expect("a queue");
        away.on_topic("realtime:room").await;
        away.on_topic("realtime:room").await;
        let frame = Wire::decode(&queue.recv().await.expect("a frame")).expect("a frame");
        assert_eq!(frame.up(), "carry");
        assert_eq!(frame.text("topic"), Some("realtime:room"));
        assert!(queue.try_recv().is_err(), "the second join said nothing");
        // And the leave is the last socket leaving, not the first.
        away.released("realtime:room").await;
        assert!(queue.try_recv().is_err(), "somebody is still on it");
        away.released("realtime:room").await;
        assert_eq!(
            Wire::decode(&queue.recv().await.expect("a frame"))
                .expect("a frame")
                .up(),
            "leave"
        );
    }
}
