//! The loop that owns a tap, and what a slow subscriber costs.
//!
//! Everything before this is a function: the decoder turns bytes into
//! rows, the matcher says who asked, the payload says what to send and
//! the visibility check says who may see it. This is the part that runs
//! forever, and the questions it answers are the ones the other modules
//! could not: how often to ask postgres for changes, what to do about a
//! socket that is not reading them, and what happens when the tap dies.
//!
//! A poll rather than a push, because that is what the tap is, and the
//! cadence is the only latency this server adds to a change. Idle it
//! asks every hundred milliseconds, which is upstream's
//! `poll_interval_ms` and its default; busy it asks again immediately,
//! because a batch that came back full is a database with more waiting
//! and sleeping on it would be adding latency to the case that can
//! least afford it.
//!
//! No subscribers, no tap. A logical slot pins the write ahead log from
//! the moment it exists until somebody reads past it, so a slot nobody
//! is reading is a disk filling up on a database whose owner did
//! nothing wrong. The reader drops its tap when the last subscriber
//! goes and takes a new one when the first arrives, which is also why
//! it can be started on a server that never has a subscriber and cost
//! nothing.
//!
//! ## What a slow socket costs
//!
//! Everybody, if it is allowed to. A tap is one connection reading one
//! slot for the whole project, so a reader that waited for a socket to
//! drain would be a reader that had stopped reading the slot, and a
//! slot that is not being read is write ahead log that is not being
//! freed. One browser tab on a train would then be a database running
//! out of disk.
//!
//! So a subscriber has a queue of its own, bounded, and the reader
//! never waits on it. A subscriber that fills its queue is dropped, its
//! sender goes with it, and the socket sees a closed channel and hangs
//! up so the client reconnects and resubscribes. That is the same
//! bargain the hub makes with a lagging broadcast receiver, for the
//! same reason and with the same reasoning behind it: a client that
//! missed changes and knows it can go and read the table, and a client
//! that missed changes quietly cannot.
//!
//! ## What resuming can and cannot mean
//!
//! The slot is temporary, so it dies with the connection that made it,
//! and a tap that had to be reopened has no way to ask for what
//! happened while it was away. That is upstream's trade as well and the
//! reason it is worth stating rather than hiding: the alternative is a
//! permanent slot, which is exactly the disk hazard above with nobody
//! at all on the other end of it.
//!
//! Within one tap's life resuming is real, since the tap consumes from
//! the slot and postgres remembers where it got to. Across a reopen it
//! is a gap, and a gap is told rather than smoothed over: every
//! subscriber alive across one hears [`Heard::Gap`], which is the same
//! shape of news as a lagging socket and gets the same answer from the
//! client.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::{Notify, mpsc};

use crate::binding::{Binding, Subscriptions};
use crate::cdc::{PUBLICATION, Tap};
use crate::payload::{self, Types};
use crate::pgoutput::Change;
use crate::sql::Pool;
use crate::visible::{Asker, seen};

/// How long the reader waits before asking again when the database had
/// nothing for it, which is upstream's `poll_interval_ms` default.
const IDLE: Duration = Duration::from_millis(100);

/// How many messages one poll takes, which is postgres's own counting:
/// a transaction's begin, its relations and its commit are messages, so
/// this is a bound on how much is read rather than on how many rows
/// come out of it.
const BATCH: i32 = 1000;

/// How far behind a subscriber may fall before it is dropped.
///
/// The same depth the hub gives a broadcast receiver. A number here is
/// a guess about how long a socket may be busy and how big a change is,
/// and the honest thing to say about it is that it is bounded on
/// purpose and that the bound is what stops one socket from being
/// everybody's problem.
const QUEUE: usize = 256;

/// What a subscriber hears.
#[derive(Debug, Clone, PartialEq)]
pub enum Heard {
    /// A change this subscriber asked for, with the ids of the
    /// bindings it matched, which is how a client knows which of its
    /// own subscriptions this is an answer to.
    Change {
        ids: Vec<u64>,
        data: Value,
        /// When the transaction that made it committed, by the
        /// database's clock, in microseconds.
        commit_ts: i64,
        /// When the tap read it, by this process's clock. The two
        /// together are what the delivery latency is measured from,
        /// and they are two clocks because there is no one clock this
        /// question has an answer on.
        read: Instant,
    },
    /// Changes were missed, because the tap had to be reopened or this
    /// subscriber was not reading. There is no way to say which
    /// changes, which is the whole point of telling somebody.
    Gap,
}

/// A subscriber's end of the feed.
pub struct Listening {
    /// What this subscriber is called when it binds or hangs up.
    pub id: u64,
    /// Where its changes arrive. A closed channel is the reader saying
    /// this subscriber is finished, which a socket answers by going.
    pub heard: mpsc::Receiver<Heard>,
}

struct Listener {
    asker: Asker,
    to: mpsc::Sender<Heard>,
    /// The bindings this subscriber has, so that a socket hanging up
    /// takes its own out of the index and nobody else's.
    bindings: Vec<u64>,
}

#[derive(Default)]
struct Inner {
    subs: Subscriptions,
    listeners: HashMap<u64, Listener>,
    /// Which subscriber a binding belongs to, which is what turns the
    /// matcher's answer into a list of queues.
    owner: HashMap<u64, u64>,
    next: u64,
}

/// Who is listening for postgres changes, and what they asked for.
///
/// One per server, next to the hub, and for the same reason: a
/// subscription is the project's rather than a connection's, and the
/// one thing reading the database has to be able to find all of them.
#[derive(Default)]
pub struct Changes {
    inner: Mutex<Inner>,
    /// Woken when the first subscriber arrives, so a reader with
    /// nothing to do is parked rather than polling a database nobody is
    /// listening to.
    wake: Notify,
}

impl Changes {
    pub fn new() -> Changes {
        Changes::default()
    }

    /// A subscriber, with the claims its rows will be checked against.
    ///
    /// The queue is made here rather than on the first binding, so that
    /// a socket which joined and has not finished asking for anything
    /// yet is already in a state where nothing it later asks for can be
    /// lost between the two.
    pub fn listen(&self, asker: Asker) -> Listening {
        let (to, heard) = mpsc::channel(QUEUE);
        let mut inner = self.inner.lock().expect("changes");
        inner.next += 1;
        let id = inner.next;
        inner.listeners.insert(
            id,
            Listener {
                asker,
                to,
                bindings: Vec::new(),
            },
        );
        drop(inner);
        // Whether this is the first subscriber or the thousandth, since
        // a wake nobody was waiting for is free.
        self.wake.notify_one();
        Listening { id, heard }
    }

    /// One thing a subscriber asked for, which comes back as the id a
    /// matching change carries.
    ///
    /// Nothing for a subscriber that has gone, which is a socket that
    /// hung up between the join and the answer to it.
    pub fn bind(&self, listener: u64, binding: Binding) -> Option<u64> {
        let mut inner = self.inner.lock().expect("changes");
        inner.listeners.get(&listener)?;
        inner.next += 1;
        let id = inner.next;
        inner.subs.add(id, binding);
        inner.owner.insert(id, listener);
        if let Some(who) = inner.listeners.get_mut(&listener) {
            who.bindings.push(id);
        }
        drop(inner);
        // This is what a reader waits for rather than the subscriber
        // itself, since a subscriber that has asked for nothing is
        // nothing to read a database for.
        self.wake.notify_one();
        Some(id)
    }

    /// One thing a subscriber is finished with, which is the channel it
    /// was asked for on going away while the socket stays.
    pub fn unbind(&self, id: u64) {
        let mut inner = self.inner.lock().expect("changes");
        inner.subs.remove(id);
        if let Some(listener) = inner.owner.remove(&id)
            && let Some(who) = inner.listeners.get_mut(&listener)
        {
            who.bindings.retain(|held| *held != id);
        }
    }

    /// This subscriber is somebody else now.
    ///
    /// A client refreshes its token on a socket it is keeping, and the
    /// rows it may see are the new token's rather than the old one's.
    /// Nothing already in its queue is taken back: those were allowed
    /// when they were sent, which is the same thing upstream can say and
    /// the only thing anybody can.
    pub fn became(&self, listener: u64, asker: Asker) {
        let mut inner = self.inner.lock().expect("changes");
        if let Some(who) = inner.listeners.get_mut(&listener) {
            who.asker = asker;
        }
    }

    /// A subscriber that is finished, and everything it asked for.
    pub fn hung_up(&self, listener: u64) {
        let mut inner = self.inner.lock().expect("changes");
        let Some(who) = inner.listeners.remove(&listener) else {
            return;
        };
        for id in who.bindings {
            inner.subs.remove(id);
            inner.owner.remove(&id);
        }
    }

    /// How many subscribers have asked for something, which is what
    /// decides whether there is a tap at all.
    ///
    /// A subscriber with no bindings is not one: a socket that joined
    /// and has not asked yet, or one whose channels have all gone while
    /// the socket stays open. Neither is a reason to hold a replication
    /// slot, and the second is a client that could otherwise pin a
    /// database's write ahead log by subscribing once and leaving.
    pub fn listening(&self) -> usize {
        self.inner
            .lock()
            .expect("changes")
            .listeners
            .values()
            .filter(|who| !who.bindings.is_empty())
            .count()
    }

    /// Wait until somebody is listening. Returns immediately when
    /// somebody already is, because the alternative is a reader that
    /// sleeps through the subscriber who arrived while it was checking.
    async fn woken(&self) {
        loop {
            let waiting = self.wake.notified();
            if self.listening() > 0 {
                return;
            }
            waiting.await;
        }
    }

    /// Who wanted this change, and as whom.
    ///
    /// One entry per subscriber rather than per binding: a client with
    /// three subscriptions on the same table is owed one message
    /// carrying three ids, which is what upstream sends and what its
    /// clients expect.
    fn wanted(&self, change: &Change) -> Vec<(u64, Asker, mpsc::Sender<Heard>, Vec<u64>)> {
        let inner = self.inner.lock().expect("changes");
        let mut by_listener: HashMap<u64, Vec<u64>> = HashMap::new();
        for id in inner.subs.matching(change) {
            if let Some(listener) = inner.owner.get(&id) {
                by_listener.entry(*listener).or_default().push(id);
            }
        }
        by_listener
            .into_iter()
            .filter_map(|(listener, mut ids)| {
                let who = inner.listeners.get(&listener)?;
                // Ascending, so that a client comparing what it got
                // with what it asked for sees the same order twice.
                ids.sort_unstable();
                Some((listener, who.asker.clone(), who.to.clone(), ids))
            })
            .collect()
    }

    /// Everybody, for the news that is not about one change.
    fn everybody(&self) -> Vec<(u64, mpsc::Sender<Heard>)> {
        let inner = self.inner.lock().expect("changes");
        inner
            .listeners
            .iter()
            .map(|(id, who)| (*id, who.to.clone()))
            .collect()
    }
}

/// The loop itself: a tap, a cadence, and the subscribers.
pub struct Reader {
    dsn: String,
    pool: Pool,
    changes: std::sync::Arc<Changes>,
    every: Duration,
    most: i32,
    /// The types the payloads needed, kept across taps because a type
    /// oid means the same thing in the same database whichever
    /// connection asked.
    types: Types,
    /// Where the last change came from, which is what the log says when
    /// a tap dies so that somebody can tell how much of the write ahead
    /// log went unread.
    at: u64,
}

impl Reader {
    pub fn new(dsn: &str, pool: Pool, changes: std::sync::Arc<Changes>) -> Reader {
        Reader {
            dsn: dsn.to_string(),
            pool,
            changes,
            every: IDLE,
            most: BATCH,
            types: Types::new(),
            at: 0,
        }
    }

    /// How long to wait after a poll that found nothing, for a test
    /// that would rather not wait a hundred milliseconds a change.
    pub fn every(mut self, every: Duration) -> Reader {
        self.every = every;
        self
    }

    /// Read until the process goes.
    ///
    /// There is no stop, on purpose: the thing that ends this loop is
    /// the server ending, and a reader that could be stopped would be a
    /// reader somebody could stop while sockets were still subscribed.
    pub async fn run(mut self) {
        let mut tap: Option<Tap> = None;
        // Whether the subscribers have already been told about the
        // trouble this reader is in, so a database that is missing a
        // publication is one warning and one gap rather than ten a
        // second of each.
        let mut told = false;
        loop {
            if self.changes.listening() == 0 {
                // The slot goes with the tap, which is the point:
                // nobody is listening, so nothing should be pinning
                // this database's write ahead log.
                tap = None;
                told = false;
                self.changes.woken().await;
                continue;
            }
            let open = match tap {
                Some(open) => open,
                None => match Tap::open(&self.dsn, PUBLICATION).await {
                    Ok(open) => {
                        told = false;
                        open
                    }
                    Err(why) => {
                        self.trouble(&mut told, &why.to_string()).await;
                        tokio::time::sleep(self.every).await;
                        continue;
                    }
                },
            };
            tap = Some(open);
            let read = match tap.as_mut().expect("a tap").changes(self.most).await {
                Ok(read) => read,
                Err(why) => {
                    // The connection went, and with it the slot, so
                    // what follows is a new tap and a gap in front of
                    // it rather than the rest of this one.
                    log::warn!("realtime: the tap stopped at {:x}, {why}", self.at);
                    self.gap().await;
                    told = true;
                    tap = None;
                    tokio::time::sleep(self.every).await;
                    continue;
                }
            };
            if read.is_empty() {
                tokio::time::sleep(self.every).await;
                continue;
            }
            let client = tap.as_ref().expect("a tap").client();
            self.deliver(client, &read).await;
            // Straight back round without sleeping: a batch that came
            // back with something in it is a database that may have
            // more, and the cadence is for an idle one.
        }
    }

    /// Everything one batch is owed, in the order postgres wrote it.
    async fn deliver(&mut self, client: &tokio_postgres::Client, batch: &[Change]) {
        // When this batch came back, which is where the half of the
        // delivery latency that is this server's own starts.
        let read = Instant::now();
        for change in batch {
            self.at = change.lsn;
            let wanted = self.changes.wanted(change);
            if wanted.is_empty() {
                continue;
            }
            let missing = self.types.missing(&change.relation);
            if !missing.is_empty()
                && let Err(why) = self.types.learn(client, &missing).await
            {
                // A payload can be built without the catalog: every
                // value falls back to the text postgres printed, which
                // is never wrong about what it says, only about what
                // type it is. Sending that beats sending nothing.
                log::warn!("realtime: the types of a changed table would not load, {why}");
            }
            // One visibility check and one payload per set of claims
            // rather than per subscriber, since two sockets signed in
            // as the same person are owed the same object, and the
            // check is the expensive half of this loop.
            let mut built: HashMap<String, Option<Value>> = HashMap::new();
            for (listener, asker, to, ids) in wanted {
                let key = format!("{}\u{0}{}", asker.role, asker.claims);
                let data = match built.get(&key) {
                    Some(data) => data.clone(),
                    None => {
                        let data = self.building(&asker, change).await;
                        built.insert(key, data.clone());
                        data
                    }
                };
                let Some(data) = data else { continue };
                match to.try_send(Heard::Change {
                    ids,
                    data,
                    commit_ts: change.commit_ts,
                    read,
                }) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Not a wait and not a drop of this one
                        // message: a subscriber that is this far behind
                        // is one whose next messages would not arrive
                        // either, and one that is told it missed
                        // something can go and read the table.
                        log::warn!("realtime: a subscriber stopped reading, dropping it");
                        self.changes.hung_up(listener);
                    }
                    // The socket went between the match and the send,
                    // which is ordinary and is cleaned up by whoever
                    // owned it.
                    Err(mpsc::error::TrySendError::Closed(_)) => {}
                }
            }
        }
    }

    /// The payload one subscriber is owed of one change, or nothing
    /// when they are owed none.
    async fn building(&self, asker: &Asker, change: &Change) -> Option<Value> {
        match seen(&self.pool, asker, change).await {
            Ok(may) if !may.row => None,
            Ok(may) => Some(payload::data(change, &self.types, &may)),
            Err(why) => {
                // Nobody could find out what this subscriber may see,
                // so they are told nothing about the row. A change feed
                // that guessed yes here would be one where a database
                // hiccup is a data leak.
                log::warn!("realtime: {why}");
                None
            }
        }
    }

    /// Tell everybody there is a gap, once.
    async fn trouble(&self, told: &mut bool, why: &str) {
        if *told {
            return;
        }
        *told = true;
        log::warn!("realtime: no changes are being read, {why}");
        self.gap().await;
    }

    /// Tell every subscriber that what they have is not everything.
    async fn gap(&self) {
        for (listener, to) in self.changes.everybody() {
            if to.try_send(Heard::Gap).is_err() {
                self.changes.hung_up(listener);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn asker(sub: &str) -> Asker {
        Asker {
            role: "authenticated".into(),
            claims: json!({"sub": sub}),
        }
    }

    fn binding(table: &str) -> Binding {
        Binding::of(&json!({"event": "*", "schema": "public", "table": table})).expect("a binding")
    }

    fn change(table: &str) -> Change {
        use crate::pgoutput::{Cell, Column, Op, Relation, Replica};
        use std::sync::Arc;
        Change {
            relation: Arc::new(Relation {
                oid: 16_384,
                schema: "public".into(),
                table: table.into(),
                replica: Replica::Default,
                columns: vec![Column {
                    name: "id".into(),
                    type_oid: 23,
                    key: true,
                }],
            }),
            op: Op::Insert,
            record: vec![Cell::Text("1".into())],
            old: None,
            old_key: false,
            commit_ts: 0,
            lsn: 1,
        }
    }

    #[test]
    fn a_change_goes_to_the_subscribers_who_asked_for_that_table() {
        let changes = Changes::new();
        let ana = changes.listen(asker("ana"));
        let ben = changes.listen(asker("ben"));
        changes.bind(ana.id, binding("todos")).expect("a binding");
        changes.bind(ben.id, binding("orders")).expect("a binding");

        let wanted = changes.wanted(&change("todos"));
        assert_eq!(wanted.len(), 1, "one of them asked about todos");
        assert_eq!(wanted[0].0, ana.id);
        assert_eq!(
            changes.wanted(&change("invoices")).len(),
            0,
            "a table nobody asked about costs nothing and reaches nobody"
        );
    }

    #[test]
    fn a_subscriber_with_two_bindings_on_a_table_is_owed_one_message_with_both_ids() {
        let changes = Changes::new();
        let ana = changes.listen(asker("ana"));
        let one = changes.bind(ana.id, binding("todos")).expect("a binding");
        let two = changes
            .bind(
                ana.id,
                Binding::of(&json!({"event": "INSERT", "schema": "public", "table": "todos"}))
                    .expect("a binding"),
            )
            .expect("a binding");

        let wanted = changes.wanted(&change("todos"));
        assert_eq!(wanted.len(), 1, "one subscriber, not one per binding");
        assert_eq!(
            wanted[0].3,
            vec![one, two],
            "and it carries both ids, which is how a client knows which of its own subscriptions \
             this answers"
        );
    }

    #[test]
    fn a_subscriber_that_hung_up_takes_its_bindings_with_it() {
        let changes = Changes::new();
        let ana = changes.listen(asker("ana"));
        changes.bind(ana.id, binding("todos")).expect("a binding");
        assert_eq!(changes.listening(), 1);

        changes.hung_up(ana.id);
        assert_eq!(changes.listening(), 0);
        assert!(
            changes.wanted(&change("todos")).is_empty(),
            "a socket that went is not still costing every change a comparison"
        );
        assert_eq!(
            changes.bind(ana.id, binding("todos")),
            None,
            "and nothing can be asked for on its behalf afterwards"
        );
    }

    #[tokio::test]
    async fn a_subscriber_that_stops_reading_is_dropped_rather_than_waited_for() {
        let changes = std::sync::Arc::new(Changes::new());
        let slow = changes.listen(asker("slow"));
        changes.bind(slow.id, binding("todos")).expect("a binding");
        let (_, _, to, _) = changes.wanted(&change("todos")).remove(0);
        for _ in 0..QUEUE {
            to.try_send(Heard::Gap).expect("a queue with room in it");
        }
        assert!(
            to.try_send(Heard::Gap).is_err(),
            "the queue is bounded, which is the whole point of it"
        );

        // What the reader does about it, which is the property that
        // matters: a socket on a train does not get to stop the tap
        // reading the slot, because a slot nobody reads is a disk
        // filling up.
        changes.hung_up(slow.id);
        assert_eq!(changes.listening(), 0);
    }

    #[tokio::test]
    async fn a_reader_with_nobody_listening_waits_rather_than_polling() {
        let changes = std::sync::Arc::new(Changes::new());
        let waiting = changes.woken();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), waiting)
                .await
                .is_err(),
            "nobody is subscribed, so there is nothing to hold a slot open for"
        );
        let listening = std::sync::Arc::clone(&changes);
        let ana = changes.listen(asker("ana"));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), changes.woken())
                .await
                .is_err(),
            "a socket that has joined and asked for nothing is not a subscription either"
        );
        tokio::spawn(async move {
            listening.bind(ana.id, binding("todos"));
        });
        tokio::time::timeout(Duration::from_secs(5), changes.woken())
            .await
            .expect("the first subscription wakes the reader");
    }

    /// A slot is held for whoever is reading it, and a socket with no
    /// subscriptions left is not.
    #[test]
    fn a_socket_that_gave_up_its_last_subscription_is_not_holding_a_tap_open() {
        let changes = Changes::new();
        let ana = changes.listen(asker("ana"));
        let one = changes.bind(ana.id, binding("todos")).expect("a binding");
        assert_eq!(changes.listening(), 1);
        changes.unbind(one);
        assert_eq!(
            changes.listening(),
            0,
            "the socket is still open and there is nothing on it to read a database for"
        );
    }
}
