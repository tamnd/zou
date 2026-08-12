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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::session::Fanout;

/// How many messages a slow socket may fall behind before it is told
/// it has missed some.
const BACKLOG: usize = 256;

/// Which socket a broadcast came from, so a sender can be told apart
/// from everybody else without comparing payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SocketId(u64);

/// What comes out of a subscription: the broadcast and who sent it.
/// Shared rather than cloned, because one push goes to every socket on
/// the topic and the payload is the big part of it.
pub type Delivery = Arc<(SocketId, Fanout)>;

/// The topics this server is carrying.
#[derive(Default)]
pub struct Hub {
    topics: Mutex<HashMap<String, broadcast::Sender<Delivery>>>,
    next: AtomicU64,
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

    /// Hand a broadcast to everyone on its topic.
    ///
    /// A topic nobody is on is dropped here rather than sent, and its
    /// entry goes with it, which is how the map stays the size of the
    /// live topics.
    pub fn fan(&self, from: SocketId, fan: Fanout) {
        let mut topics = self.topics.lock().expect("the hub");
        let Some(sender) = topics.get(&fan.topic) else {
            return;
        };
        let topic = fan.topic.clone();
        if sender.send(Arc::new((from, fan))).is_err() {
            topics.remove(&topic);
        }
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
        assert_eq!(second.recv().await.unwrap().1.push.event, "cursor");
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
