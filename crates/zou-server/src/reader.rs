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
//! ## Why a join waits for one
//!
//! A slot sees what was written after it existed and nothing before, so
//! a client told it was subscribed while the tap was still opening
//! would lose every row written in between. That is not a rare window:
//! an application that subscribes and then writes, which is what a demo
//! and half the tutorials do, hits it every time on the first
//! subscription, and what it sees is a change that never arrives rather
//! than an error.
//!
//! So the tap has a state ([`Tapped`]) and a join waits on it. The
//! reader says `Open` once it has one and `Failed` when it could not
//! take one, and the join is answered on either, which means the only
//! thing a subscriber waits for is the answer to the question it just
//! asked. Upstream has nothing to wait for here because its slot is
//! permanent and always being read, which is the trade this made in the
//! other direction.
//!
//! Within one tap's life resuming is real, since the tap consumes from
//! the slot and postgres remembers where it got to. Across a reopen it
//! is a gap, and a gap is told rather than smoothed over: every
//! subscriber alive across one hears [`Heard::Gap`], which is the same
//! shape of news as a lagging socket and gets the same answer from the
//! client.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
    to: To,
    /// The bindings this subscriber has, so that a socket hanging up
    /// takes its own out of the index and nobody else's.
    bindings: Vec<u64>,
}

/// Where one subscriber's changes go.
///
/// A socket on this node has a queue of its own, which is the right
/// shape for a browser tab's worth of channels: the socket's own loop
/// waits on that queue along with everything else it is waiting on.
///
/// A node on the far end of a link is a hundred thousand of those, and
/// a hundred thousand receivers is not something to wake up and poll
/// every time one of them has a row in it. So the subscribers on one
/// link share the one queue that link drains and each row carries the
/// name of the socket it is for, which puts the fan out back on the
/// node where the socket is. Nothing is given away by that: the check
/// happened here either way, so what crosses is a row already decided
/// for exactly one subscriber.
#[derive(Clone)]
enum To {
    /// A socket on this node.
    Own(mpsc::Sender<Heard>),
    /// A socket on the other end of a link, as that link names it.
    Link { named: u64, link: Arc<Shared> },
}

impl To {
    /// Hand one message over, without ever waiting: a reader that
    /// waited on a subscriber would be a reader that had stopped
    /// reading the slot. False is somebody who is not keeping up.
    fn send(&self, heard: Heard) -> bool {
        match self {
            To::Own(to) => match to.try_send(heard) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => false,
                // The socket went between the match and the send, which
                // is ordinary and is cleaned up by whoever owned it.
                Err(mpsc::error::TrySendError::Closed(_)) => true,
            },
            To::Link { named, link } => link.send(*named, heard),
        }
    }

    /// Whether one that is not keeping up is dropped here.
    ///
    /// A socket of this node's own is: a subscriber this far behind is
    /// one whose next message would not have arrived either, and one
    /// that is told it missed something can go and read the table.
    ///
    /// A socket on a link is not, because what is behind is the link
    /// rather than that one subscription, and the link's own end tells
    /// every socket on it. Dropping subscriptions one at a time from
    /// here would be taking the project's changes away from a node that
    /// is about to reconnect and ask for them again.
    fn dropped_when_behind(&self) -> bool {
        matches!(self, To::Own(_))
    }
}

/// The one queue every subscriber on one link shares.
pub struct Shared {
    to: mpsc::Sender<(u64, Heard)>,
    /// Set when something did not fit, which is a node that is not
    /// draining its link. Its own end reads this and tells every socket
    /// on it there is a gap, which is the same news it would give them
    /// if the link had broken, because from where they are sitting it
    /// has.
    behind: AtomicBool,
}

impl Shared {
    /// One, as deep as the end that drains it asked for.
    pub fn new(depth: usize) -> (Arc<Shared>, mpsc::Receiver<(u64, Heard)>) {
        let (to, heard) = mpsc::channel(depth);
        (
            Arc::new(Shared {
                to,
                behind: AtomicBool::new(false),
            }),
            heard,
        )
    }

    fn send(&self, named: u64, heard: Heard) -> bool {
        if self.to.try_send((named, heard)).is_err() {
            self.behind.store(true, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Whether anything was missed since the last time this was asked.
    ///
    /// Taken rather than read, because the link tells its sockets once
    /// for however many rows did not fit: there is no way to say which
    /// they were, so saying it twice says nothing new.
    pub fn behind(&self) -> bool {
        self.behind.swap(false, Ordering::Relaxed)
    }
}

/// Whether there is a tap on the database right now.
///
/// What a join needs to know, and the reason it is three states rather
/// than a flag: a join that waited for `Open` on a database this server
/// cannot tap would wait for something that is not coming, so the
/// reader says which of the two happened and the join stops waiting
/// either way.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tapped {
    /// Nobody has asked for one, or the reader is taking one now.
    #[default]
    Waiting,
    /// Open, and reading from a point before this moment, which is what
    /// makes a row written after this one visible to whoever asked.
    Open,
    /// The last attempt did not work, and the reason has already gone
    /// to the subscribers as a gap.
    Failed,
}

#[derive(Default)]
struct Inner {
    subs: Subscriptions,
    /// Where the tap is, which is the reader's news and everybody
    /// else's answer.
    tap: Tapped,
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
    /// Woken the other way, when the reader has news about its tap, so
    /// that a join is answered as soon as there is one rather than on a
    /// timer.
    taken: Notify,
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
                to: To::Own(to),
                bindings: Vec::new(),
            },
        );
        drop(inner);
        // Whether this is the first subscriber or the thousandth, since
        // a wake nobody was waiting for is free.
        self.wake.notify_one();
        Listening { id, heard }
    }

    /// A subscriber on the other end of a link, whose changes go down
    /// the one queue that link drains with itself named on each of them.
    ///
    /// Everything else about it is a subscriber like any other: it is
    /// the reader that decides what this one may see of a row, against
    /// the claims in the token its socket is running on, which is why
    /// there is a subscriber here at all rather than a node asking for
    /// rows and filtering them itself.
    pub fn listening_on(&self, asker: Asker, named: u64, link: Arc<Shared>) -> u64 {
        let mut inner = self.inner.lock().expect("changes");
        inner.next += 1;
        let id = inner.next;
        inner.listeners.insert(
            id,
            Listener {
                asker,
                to: To::Link { named, link },
                bindings: Vec::new(),
            },
        );
        drop(inner);
        self.wake.notify_one();
        id
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

    /// An id that names no subscription, which is what the reply hands
    /// back for a list that could not be subscribed.
    ///
    /// The client checks the reply against its own bindings before it
    /// looks at anything else, so there has to be one per entry, and a
    /// change can never carry one of these because nothing was bound
    /// to it. It comes off the same counter as a real one so it can
    /// never be a real one later.
    pub fn unnamed(&self) -> u64 {
        let mut inner = self.inner.lock().expect("changes");
        inner.next += 1;
        inner.next
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

    /// The reader, saying where its tap is.
    pub fn tapped(&self, state: Tapped) {
        self.inner.lock().expect("changes").tap = state;
        // Everybody rather than one, since every join waiting on this
        // is waiting for the same tap.
        self.taken.notify_waiters();
    }

    /// Where it is, for a test and for the wait below.
    pub fn tap(&self) -> Tapped {
        self.inner.lock().expect("changes").tap
    }

    /// Wait until the reader has taken a tap, or has said it could not.
    ///
    /// This is what a join holds for after its bindings are in, and it
    /// is the whole of the fix for a subscription that is answered
    /// before there is anything reading the write ahead log: a client
    /// that has been told it is subscribed can write a row and expect
    /// to hear about it.
    ///
    /// It returns rather than refusing on `Failed`, because what is
    /// wrong with the database has already gone to this subscriber as a
    /// gap and a join that hung on it would turn a database missing its
    /// publication into a client that never gets an answer at all.
    pub async fn tapping(&self) {
        loop {
            let waiting = self.taken.notified();
            if self.tap() != Tapped::Waiting {
                return;
            }
            waiting.await;
        }
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
    fn wanted(&self, change: &Change) -> Vec<(u64, Asker, To, Vec<u64>)> {
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
    fn everybody(&self) -> Vec<(u64, To)> {
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
                // And the next join waits for the next tap rather than
                // being answered on the strength of this one.
                self.changes.tapped(Tapped::Waiting);
                self.changes.woken().await;
                continue;
            }
            let open = match tap {
                Some(open) => open,
                None => match Tap::open(&self.dsn, PUBLICATION).await {
                    Ok(open) => {
                        told = false;
                        // Said before the first poll, because what is
                        // waiting on it is a join that will write as
                        // soon as it is answered.
                        self.changes.tapped(Tapped::Open);
                        open
                    }
                    Err(why) => {
                        self.trouble(&mut told, &why.to_string()).await;
                        self.changes.tapped(Tapped::Failed);
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
                    // A join that arrives now waits for the tap that
                    // replaces this one, since the one it would have
                    // been answered on has gone.
                    self.changes.tapped(Tapped::Waiting);
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
                let sent = to.send(Heard::Change {
                    ids,
                    data,
                    commit_ts: change.commit_ts,
                    read,
                });
                // Not a wait and not a drop of this one message: a
                // subscriber that is this far behind is one whose next
                // messages would not arrive either, and one that is
                // told it missed something can go and read the table.
                if !sent && to.dropped_when_behind() {
                    log::warn!("realtime: a subscriber stopped reading, dropping it");
                    self.changes.hung_up(listener);
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
            if !to.send(Heard::Gap) && to.dropped_when_behind() {
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
            assert!(to.send(Heard::Gap), "a queue with room in it");
        }
        assert!(
            !to.send(Heard::Gap),
            "the queue is bounded, which is the whole point of it"
        );
        assert!(
            to.dropped_when_behind(),
            "and a socket on this node is the kind that is dropped for it"
        );

        // What the reader does about it, which is the property that
        // matters: a socket on a train does not get to stop the tap
        // reading the slot, because a slot nobody reads is a disk
        // filling up.
        changes.hung_up(slow.id);
        assert_eq!(changes.listening(), 0);
    }

    #[tokio::test]
    async fn a_subscriber_on_a_link_hears_its_rows_on_the_queue_that_link_drains() {
        // A row for a socket on another node goes down the one queue that
        // link drains with that node's own name for the socket on it, so
        // the fan out happens where the sockets are and the check happens
        // here.
        let changes = Changes::new();
        let (link, mut rows) = Shared::new(4);
        let ana = changes.listening_on(asker("ana"), 7, Arc::clone(&link));
        let bound = changes.bind(ana, binding("todos")).expect("a binding");

        let wanted = changes.wanted(&change("todos"));
        assert_eq!(wanted.len(), 1);
        assert_eq!(wanted[0].0, ana);
        assert_eq!(wanted[0].3, vec![bound]);
        assert!(wanted[0].2.send(Heard::Gap));
        assert_eq!(
            rows.recv().await,
            Some((7, Heard::Gap)),
            "named for the socket the other end knows, since a link's numbers are its own"
        );
    }

    #[tokio::test]
    async fn a_link_that_stops_draining_is_told_once_rather_than_losing_a_subscriber() {
        // What is behind is the link and not one subscription, so the news
        // goes to the link, which tells every socket on it. Dropping
        // subscribers from here would be taking a project's changes away
        // from a node that is about to reconnect and ask for them again.
        let changes = Changes::new();
        let (link, _rows) = Shared::new(1);
        let ana = changes.listening_on(asker("ana"), 7, Arc::clone(&link));
        changes.bind(ana, binding("todos")).expect("a binding");
        let (_, _, to, _) = changes.wanted(&change("todos")).remove(0);

        assert!(to.send(Heard::Gap), "one fits");
        assert!(!to.send(Heard::Gap), "and the queue is bounded on purpose");
        assert!(!to.dropped_when_behind());
        assert!(link.behind(), "the link is what is told");
        assert!(
            !link.behind(),
            "once, however many rows did not fit, because there is no way to say which they were"
        );
        assert_eq!(
            changes.listening(),
            1,
            "and the subscriber is still there for when that node drains its link"
        );
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

    /// The other half of the same idea. A slot is taken when the first
    /// subscriber arrives, and until it has been taken there is nothing
    /// to answer that subscriber's join on: a row written in the
    /// meantime is written before the slot existed and is gone.
    #[tokio::test]
    async fn a_join_waits_for_the_tap_that_will_carry_what_it_asked_for() {
        let changes = std::sync::Arc::new(Changes::new());
        assert_eq!(changes.tap(), Tapped::Waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), changes.tapping())
                .await
                .is_err(),
            "there is no tap yet, so the client is not told it is subscribed yet"
        );

        let opening = std::sync::Arc::clone(&changes);
        tokio::spawn(async move { opening.tapped(Tapped::Open) });
        tokio::time::timeout(Duration::from_secs(5), changes.tapping())
            .await
            .expect("the join goes through once there is a tap");

        // And a database this server cannot tap lets the join go as
        // well, because what is wrong with it has already reached this
        // subscriber as a gap and waiting longer tells nobody anything.
        changes.tapped(Tapped::Waiting);
        let refusing = std::sync::Arc::clone(&changes);
        tokio::spawn(async move { refusing.tapped(Tapped::Failed) });
        tokio::time::timeout(Duration::from_secs(5), changes.tapping())
            .await
            .expect("a tap that could not be taken is an answer too");
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
