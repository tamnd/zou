//! Where a broadcast goes between one socket and the others.
//!
//! One tokio broadcast channel per topic, made when the first socket
//! joins and dropped when the last one leaves, so a server that has
//! seen a million topic names is holding the ones that are in use
//! rather than the ones that ever were.
//!
//! A channel is bounded. A socket that stops reading is a socket that
//! is gone or wedged, and the choice is between holding messages for
//! it forever and telling it that it missed some: this holds a
//! backlog and then tells it, which is what `RecvError::Lagged` is.
//! What the transport does with that is its own business, and the
//! honest thing is to close the socket so the client reconnects and
//! resyncs rather than carrying on with a hole in the stream.
//!
//! Presence is the one thing here that is state rather than a pipe. A
//! broadcast is gone the moment it has been handed out, but who is on
//! a topic has to be answerable to a socket that joins later, so the
//! hub keeps a map per topic of which socket is tracking what. Every
//! change to it goes out as a diff on the same pipe the broadcasts
//! use, which is how a client's copy stays level with the server's
//! without either of them asking the other anything.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};
use tokio::sync::broadcast;

use crate::session::{Fanout, Sent};

/// How many messages a slow socket may fall behind before it is told
/// it has missed some.
const BACKLOG: usize = 256;

/// Which socket a broadcast came from, so a sender can be told apart
/// from everybody else without comparing payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SocketId(u64);

impl std::fmt::Display for SocketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What comes out of a subscription: the message and who caused it.
/// Shared rather than cloned, because one push goes to every socket on
/// the topic and the payload is the big part of it.
pub type Delivery = Arc<(SocketId, Sent)>;

/// One socket's presence on one topic.
#[derive(Debug, Clone)]
struct Meta {
    /// What the client is known by, which is its own if it asked for
    /// one and the socket's name if it did not.
    key: String,
    /// What tells two tracks apart, including two by the same key,
    /// which is how a client with two tabs open counts as two.
    phx_ref: String,
    /// Whatever the client said about itself.
    payload: Value,
}

impl Meta {
    /// The shape a client reads: the payload with the ref in it. The
    /// client renames it to `presence_ref` on the way in, and uses it
    /// to work out which metas a diff added and which it took away.
    fn as_json(&self) -> Value {
        let mut out = match &self.payload {
            Value::Object(fields) => fields.clone(),
            _ => Map::new(),
        };
        out.insert("phx_ref".into(), json!(self.phx_ref));
        Value::Object(out)
    }
}

/// The topics this server is carrying.
#[derive(Default)]
pub struct Hub {
    topics: Mutex<HashMap<String, broadcast::Sender<Delivery>>>,
    /// Who is on each topic. Ordered by socket so the state a joiner
    /// is sent comes out the same way twice.
    presence: Mutex<HashMap<String, BTreeMap<SocketId, Meta>>>,
    next: AtomicU64,
    refs: AtomicU64,
}

impl Hub {
    pub fn new() -> Hub {
        Hub::default()
    }

    /// A name for a socket that has just connected.
    pub fn socket(&self) -> SocketId {
        SocketId(self.next.fetch_add(1, Ordering::Relaxed))
    }

    /// Start hearing `topic`. The receiver only carries messages sent
    /// after this call, which is the join semantics phoenix has: a
    /// channel is a live stream and not a log.
    pub fn carry(&self, topic: &str) -> broadcast::Receiver<Delivery> {
        let mut topics = self.topics.lock().expect("the hub");
        if let Some(sender) = topics.get(topic) {
            return sender.subscribe();
        }
        let (sender, receiver) = broadcast::channel(BACKLOG);
        topics.insert(topic.to_string(), sender);
        receiver
    }

    /// Hand a broadcast to everyone on its topic, and say how many
    /// sockets that was.
    ///
    /// A topic nobody is on is dropped here rather than sent, and its
    /// entry goes with it, which is how the map stays the size of the
    /// live topics.
    ///
    /// The count is what a message costs the project: upstream counts
    /// a send and every delivery of it against the same budget, so one
    /// broadcast to a thousand sockets is a thousand events and not
    /// one.
    pub fn fan(&self, from: SocketId, fan: Fanout) -> usize {
        self.send(from, Sent::Broadcast(fan))
    }

    fn send(&self, from: SocketId, sent: Sent) -> usize {
        let mut topics = self.topics.lock().expect("the hub");
        let Some(sender) = topics.get(sent.topic()) else {
            return 0;
        };
        let topic = sent.topic().to_string();
        match sender.send(Arc::new((from, sent))) {
            Ok(reached) => reached,
            Err(_) => {
                topics.remove(&topic);
                0
            }
        }
    }

    /// Say that a socket is on a topic, and tell the topic about it.
    ///
    /// `key` is what the client asked to be known by, and a client
    /// that asked for nothing is known by its socket, which is what
    /// upstream does with a per connection id. Tracking twice on one
    /// topic replaces what was there and the diff says so, with the
    /// old meta leaving in the same message the new one joins in.
    pub fn track(&self, who: SocketId, topic: &str, key: Option<String>, payload: Value) {
        let meta = Meta {
            key: key.unwrap_or_else(|| who.to_string()),
            phx_ref: self.refs.fetch_add(1, Ordering::Relaxed).to_string(),
            payload,
        };
        let replaced = {
            let mut presence = self.presence.lock().expect("the hub");
            presence
                .entry(topic.to_string())
                .or_default()
                .insert(who, meta.clone())
        };
        self.diff(who, topic, Some(&meta), replaced.as_ref());
    }

    /// Take a socket off a topic's presence, which is what an untrack,
    /// a leave and a closed connection all come to.
    pub fn untrack(&self, who: SocketId, topic: &str) {
        let gone = {
            let mut presence = self.presence.lock().expect("the hub");
            let Some(on_topic) = presence.get_mut(topic) else {
                return;
            };
            let gone = on_topic.remove(&who);
            if on_topic.is_empty() {
                presence.remove(topic);
            }
            gone
        };
        if gone.is_some() {
            self.diff(who, topic, None, gone.as_ref());
        }
    }

    /// Everyone on a topic, in the shape a client keeps its own copy
    /// in: keyed by presence key, each with the list of metas that key
    /// has, since one key can be two tabs.
    pub fn state(&self, topic: &str) -> Value {
        let presence = self.presence.lock().expect("the hub");
        let mut out = Map::new();
        if let Some(on_topic) = presence.get(topic) {
            for meta in on_topic.values() {
                let entry = out
                    .entry(meta.key.clone())
                    .or_insert_with(|| json!({"metas": []}));
                if let Some(metas) = entry.get_mut("metas").and_then(Value::as_array_mut) {
                    metas.push(meta.as_json());
                }
            }
        }
        Value::Object(out)
    }

    /// One change to a topic's presence, on its way to everyone on it.
    fn diff(&self, who: SocketId, topic: &str, joined: Option<&Meta>, left: Option<&Meta>) {
        let entry = |meta: Option<&Meta>| match meta {
            Some(meta) => json!({meta.key.clone(): {"metas": [meta.as_json()]}}),
            None => json!({}),
        };
        self.send(
            who,
            Sent::Diff {
                topic: topic.to_string(),
                payload: json!({"joins": entry(joined), "leaves": entry(left)}),
            },
        );
    }

    /// The last socket on `topic` has gone, so the topic can go too.
    ///
    /// Called on leave and on close. The check is the receiver count
    /// rather than the caller's word for it, since two sockets leaving
    /// at once would otherwise take the topic away from a third that
    /// is still on it.
    pub fn released(&self, topic: &str) {
        let mut topics = self.topics.lock().expect("the hub");
        if let Some(sender) = topics.get(topic)
            && sender.receiver_count() == 0
        {
            topics.remove(topic);
            self.presence.lock().expect("the hub").remove(topic);
        }
    }

    /// How many topics are being carried, for the tests and for a
    /// metric later.
    pub fn topics(&self) -> usize {
        self.topics.lock().expect("the hub").len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{BinaryBroadcast, Encoding};

    fn a_fan(topic: &str) -> Fanout {
        Fanout {
            topic: topic.into(),
            push: BinaryBroadcast {
                join_ref: "1".into(),
                reference: "2".into(),
                topic: topic.into(),
                event: "cursor".into(),
                meta: String::new(),
                encoding: Encoding::Json,
                payload: b"{}".to_vec(),
            },
            to_self: false,
        }
    }

    #[tokio::test]
    async fn everyone_on_a_topic_gets_it_and_knows_who_sent_it() {
        let hub = Hub::new();
        let one = hub.socket();
        let two = hub.socket();
        let mut first = hub.carry("realtime:room");
        let mut second = hub.carry("realtime:room");
        hub.fan(one, a_fan("realtime:room"));
        let heard = first.recv().await.unwrap();
        assert_eq!(heard.0, one);
        let Sent::Broadcast(fan) = &second.recv().await.unwrap().1 else {
            panic!("not a broadcast")
        };
        assert_eq!(fan.push.event, "cursor");
        assert_ne!(one, two);
    }

    #[tokio::test]
    async fn a_topic_nobody_is_on_carries_nothing_and_is_not_kept() {
        let hub = Hub::new();
        let who = hub.socket();
        let receiver = hub.carry("realtime:room");
        assert_eq!(hub.topics(), 1);
        drop(receiver);
        hub.fan(who, a_fan("realtime:room"));
        assert_eq!(hub.topics(), 0);
        // And one that was never joined is not made by sending to it.
        hub.fan(who, a_fan("realtime:elsewhere"));
        assert_eq!(hub.topics(), 0);
    }

    #[tokio::test]
    async fn a_topic_stays_while_anyone_is_still_on_it() {
        let hub = Hub::new();
        let first = hub.carry("realtime:room");
        let _second = hub.carry("realtime:room");
        drop(first);
        hub.released("realtime:room");
        assert_eq!(hub.topics(), 1);
    }

    /// The next thing on a receiver, as a presence diff or a panic.
    fn diff_of(delivery: &Delivery) -> Value {
        match &delivery.1 {
            Sent::Diff { payload, .. } => payload.clone(),
            other => panic!("{other:?} is not a diff"),
        }
    }

    #[tokio::test]
    async fn who_is_on_a_topic_is_answerable_to_a_socket_that_joins_later() {
        let hub = Hub::new();
        let first = hub.socket();
        let second = hub.socket();
        let _carrying = hub.carry("realtime:room");
        hub.track(first, "realtime:room", Some("u1".into()), json!({"at": 1}));
        hub.track(second, "realtime:room", Some("u2".into()), json!({"at": 2}));

        let state = hub.state("realtime:room");
        assert_eq!(state["u1"]["metas"][0]["at"], 1);
        assert_eq!(state["u2"]["metas"][0]["at"], 2);
        // Every meta carries the ref the client tells them apart by,
        // and no two are the same.
        let one = state["u1"]["metas"][0]["phx_ref"].as_str().unwrap();
        let two = state["u2"]["metas"][0]["phx_ref"].as_str().unwrap();
        assert_ne!(one, two);
        // A topic nobody is on has nobody on it, which is an object
        // and not a null, because the client iterates it.
        assert_eq!(hub.state("realtime:lobby"), json!({}));
    }

    #[tokio::test]
    async fn one_key_in_two_tabs_is_two_metas_and_going_takes_one_of_them() {
        let hub = Hub::new();
        let tab = hub.socket();
        let other_tab = hub.socket();
        let _carrying = hub.carry("realtime:room");
        hub.track(tab, "realtime:room", Some("u1".into()), json!({"tab": 1}));
        hub.track(
            other_tab,
            "realtime:room",
            Some("u1".into()),
            json!({"tab": 2}),
        );
        assert_eq!(
            hub.state("realtime:room")["u1"]["metas"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        hub.untrack(tab, "realtime:room");
        let left = hub.state("realtime:room");
        assert_eq!(left["u1"]["metas"].as_array().unwrap().len(), 1);
        assert_eq!(left["u1"]["metas"][0]["tab"], 2);
        hub.untrack(other_tab, "realtime:room");
        assert_eq!(hub.state("realtime:room"), json!({}));
    }

    #[tokio::test]
    async fn every_change_goes_out_as_a_diff_and_a_retrack_is_both_halves_at_once() {
        let hub = Hub::new();
        let who = hub.socket();
        let mut watching = hub.carry("realtime:room");
        hub.track(who, "realtime:room", Some("u1".into()), json!({"at": 1}));
        let joined = diff_of(&watching.recv().await.unwrap());
        assert_eq!(joined["joins"]["u1"]["metas"][0]["at"], 1);
        assert_eq!(joined["leaves"], json!({}));

        hub.track(who, "realtime:room", Some("u1".into()), json!({"at": 2}));
        let moved = diff_of(&watching.recv().await.unwrap());
        assert_eq!(moved["joins"]["u1"]["metas"][0]["at"], 2);
        assert_eq!(moved["leaves"]["u1"]["metas"][0]["at"], 1);
        // And only one meta is left behind, not two.
        assert_eq!(
            hub.state("realtime:room")["u1"]["metas"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        hub.untrack(who, "realtime:room");
        let gone = diff_of(&watching.recv().await.unwrap());
        assert_eq!(gone["joins"], json!({}));
        assert_eq!(gone["leaves"]["u1"]["metas"][0]["at"], 2);
        // Untracking what is not tracked says nothing at all.
        hub.untrack(who, "realtime:room");
        assert!(watching.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_socket_that_asked_for_no_key_is_known_by_its_own_name() {
        let hub = Hub::new();
        let who = hub.socket();
        let _carrying = hub.carry("realtime:room");
        hub.track(who, "realtime:room", None, json!({}));
        let state = hub.state("realtime:room");
        assert_eq!(
            state.as_object().unwrap().keys().collect::<Vec<_>>(),
            vec![&who.to_string()]
        );
    }

    #[tokio::test]
    async fn a_topic_nobody_is_left_on_forgets_who_was_there() {
        let hub = Hub::new();
        let who = hub.socket();
        let carrying = hub.carry("realtime:room");
        hub.track(who, "realtime:room", Some("u1".into()), json!({}));
        drop(carrying);
        hub.released("realtime:room");
        assert_eq!(hub.topics(), 0);
        assert_eq!(hub.state("realtime:room"), json!({}));
    }

    /// What a client keeps: the state it was sent when it joined, and
    /// every diff since, folded in the way `@supabase/phoenix` folds
    /// them. A join replaces the metas whose ref it carries and adds
    /// the ones it does not, a leave takes metas out by ref, and a key
    /// with no metas left is gone rather than empty.
    #[derive(Default)]
    struct Client {
        state: Map<String, Value>,
    }

    impl Client {
        fn apply(&mut self, diff: &Value) {
            for (key, entry) in diff["joins"].as_object().expect("an object") {
                let metas = self.state.entry(key.clone()).or_insert(json!([]));
                let arriving = refs(entry);
                let metas = metas.as_array_mut().expect("an array");
                metas.retain(|meta| !arriving.contains(&as_ref(meta)));
                metas.extend(entry["metas"].as_array().expect("an array").iter().cloned());
            }
            for (key, entry) in diff["leaves"].as_object().expect("an object") {
                let leaving = refs(entry);
                let Some(metas) = self.state.get_mut(key).and_then(Value::as_array_mut) else {
                    continue;
                };
                metas.retain(|meta| !leaving.contains(&as_ref(meta)));
                if metas.is_empty() {
                    self.state.remove(key);
                }
            }
        }

        /// The same shape the server keeps, so the two can be compared
        /// as one value rather than field by field.
        fn state(&self) -> Value {
            let mut out = Map::new();
            for (key, metas) in &self.state {
                out.insert(key.clone(), json!({"metas": sorted(metas)}));
            }
            Value::Object(out)
        }
    }

    fn as_ref(meta: &Value) -> String {
        meta["phx_ref"].as_str().expect("a ref").to_string()
    }

    fn refs(entry: &Value) -> Vec<String> {
        entry["metas"]
            .as_array()
            .expect("an array")
            .iter()
            .map(as_ref)
            .collect()
    }

    /// One key's metas by ref, because the server holds them by socket
    /// and a client holds them in the order it heard about them, and
    /// neither order is part of the protocol.
    fn sorted(metas: &Value) -> Vec<Value> {
        let mut metas: Vec<Value> = metas.as_array().expect("an array").clone();
        metas.sort_by_key(|meta| as_ref(meta).parse::<u64>().expect("a number"));
        metas
    }

    fn normalised(state: &Value) -> Value {
        let mut out = Map::new();
        for (key, entry) in state.as_object().expect("an object") {
            out.insert(key.clone(), json!({"metas": sorted(&entry["metas"])}));
        }
        Value::Object(out)
    }

    /// splitmix64, so a run that fails is a seed somebody can paste
    /// back rather than a shrug.
    fn next(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = *seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    #[tokio::test]
    async fn a_client_that_only_ever_sees_diffs_ends_up_where_the_server_is() {
        // The property presence rests on: a client is sent the state
        // once and diffs after that, so if the diffs are wrong its
        // copy drifts and nothing says so. Six sockets sharing three
        // keys, tracking, retracking and going, checked after every
        // single change rather than at the end, so a failure names the
        // step that caused it.
        let hub = Hub::new();
        let topic = "realtime:room";
        let mut watching = hub.carry(topic);
        let sockets: Vec<SocketId> = (0..6).map(|_| hub.socket()).collect();
        let keys = ["u1", "u2", "u3"];
        let mut client = Client::default();
        let mut seed = 0x2112;
        for step in 0..500u64 {
            let who = sockets[next(&mut seed) as usize % sockets.len()];
            if next(&mut seed).is_multiple_of(3) {
                hub.untrack(who, topic);
            } else {
                let key = keys[next(&mut seed) as usize % keys.len()];
                hub.track(who, topic, Some(key.into()), json!({"at": step}));
            }
            while let Ok(delivery) = watching.try_recv() {
                client.apply(&diff_of(&delivery));
            }
            assert_eq!(
                client.state(),
                normalised(&hub.state(topic)),
                "the client drifted at step {step}"
            );
        }
        // And it was not a run of nothing happening.
        assert!(!hub.state(topic).as_object().expect("an object").is_empty());
    }

    #[tokio::test]
    async fn a_socket_that_stops_reading_is_told_it_missed_some() {
        let hub = Hub::new();
        let who = hub.socket();
        let mut slow = hub.carry("realtime:room");
        for _ in 0..BACKLOG + 1 {
            hub.fan(who, a_fan("realtime:room"));
        }
        assert!(matches!(
            slow.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
    }
}
