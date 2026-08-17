//! Where a socket's topics, changes and answers come from.
//!
//! The realtime protocol is a state machine with no io in it: a frame
//! arrives and a list of things to do comes back. Reading that list is
//! how this seam was found rather than argued about, because the list
//! is exhaustive, and it falls into three groups.
//!
//! The socket's own: send text, send bytes, close. Those never leave
//! the node the client is connected to.
//!
//! The tenant's, shared by every socket on it: carry a topic, drop it,
//! fan a broadcast, track presence, untrack, read the state. Those have
//! to happen in one place or two sockets on one channel stop hearing
//! each other.
//!
//! The database's: whether a private channel's policies allow this
//! socket, and what a subscriber may see of a changed row. Those are
//! postgres answering a question about itself, so they happen where the
//! database is and nowhere else.
//!
//! A tenant is written by one node, because the lease is one node's. So
//! the last two groups belong to the holder, and this is the one thing
//! a socket asks for all of them: [`Source::Held`] when the holder is
//! this node, and [`Source::Away`] when the sockets are here and the
//! tenant is not.
//!
//! Everything below is the same call under both, which is the point. A
//! client cannot tell which kind of node it reached, and the same
//! conformance suite is run against both.

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::broadcast::Receiver;
use zou_realtime::{Ask, Delivery, Fanout, Grant, Identity, SocketId, Watched};

use crate::App;
use crate::fanout::Away;
use crate::reader::Listening;

/// Where the tenant this socket belongs to actually is.
pub enum Source {
    /// Here. The hub, the change reader and the database are all on
    /// this node, which is every single node deployment and every node
    /// serving a tenant it holds.
    Held,
    /// Somewhere else. This node has the sockets and the holder has
    /// everything they need, over one ordered link per tenant.
    Away(Arc<Away>),
}

impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::Held => write!(f, "held"),
            Source::Away(_) => write!(f, "away"),
        }
    }
}

impl Source {
    /// Whether this node holds the tenant, which decides what else it
    /// starts: a node with the sockets and none of the tenant has no
    /// reader to run and no database to listen to.
    pub fn held(&self) -> bool {
        matches!(self, Source::Held)
    }

    /// A socket has just connected: its name, and where it is told it
    /// has missed something.
    ///
    /// The name is the local hub's either way, because the local hub is
    /// what fans a message out to the sockets here under both. What the
    /// holder is told is that number and what it says back is addressed
    /// to it, so two nodes using the same number for two sockets is not
    /// a collision: a link's numbers mean nothing off that link.
    ///
    /// Held, a socket has nothing to hear until it subscribes to a
    /// table, so it is given nothing. Away, it has one from the moment
    /// it connects, because a link that breaks is a gap and a gap is
    /// news for every socket on the node rather than only for the ones
    /// watching a table.
    pub async fn joined(&self, app: &Arc<App>) -> (SocketId, Option<Listening>) {
        let me = app.hub.socket();
        match self {
            Source::Held => (me, None),
            Source::Away(away) => (me, Some(away.joined(app, me).await)),
        }
    }

    /// Start hearing a topic.
    ///
    /// Away, this is two things at once: the local hub carries the
    /// topic so that one message from the holder reaches every socket
    /// here that is on it, and the holder is told there is somebody on
    /// it so that it sends one.
    pub async fn carry(&self, app: &Arc<App>, topic: &str) -> Receiver<Delivery> {
        if let Source::Away(away) = self {
            away.carry(app, topic).await;
        }
        app.hub.carry(topic)
    }

    /// A socket here has left a topic.
    pub async fn released(&self, app: &Arc<App>, topic: &str) {
        app.hub.released(topic);
        if let Source::Away(away) = self {
            away.released(topic).await;
        }
    }

    /// Hand a broadcast to everyone on its topic, and say how many
    /// sockets that was, which is what the message costs the project.
    ///
    /// Away, this is none of them yet: the message goes up and comes
    /// back down, and what it reaches here is counted when it arrives.
    /// Fanning a copy locally on the way past would put this node's
    /// sockets ahead of every other node's, which is one message in two
    /// orders on two screens.
    pub async fn fan(&self, app: &Arc<App>, me: SocketId, fan: Fanout) -> usize {
        match self {
            Source::Held => app.hub.fan(me, fan),
            Source::Away(away) => away.fan(me, fan).await,
        }
    }

    /// Put this socket on a topic's presence, or move it, and tell the
    /// topic.
    ///
    /// Away, this node keeps none of it. Presence is state rather than
    /// a pipe and it lives in one place, so the track goes up and the
    /// diff it causes comes back down like any other message.
    pub async fn track(
        &self,
        app: &Arc<App>,
        me: SocketId,
        topic: &str,
        key: Option<String>,
        payload: Value,
    ) {
        match self {
            Source::Held => app.hub.track(me, topic, key, payload),
            Source::Away(away) => away.track(me, topic, key, payload).await,
        }
    }

    /// Take it off, and tell the topic.
    pub async fn untrack(&self, app: &Arc<App>, me: SocketId, topic: &str) {
        match self {
            Source::Held => app.hub.untrack(me, topic),
            Source::Away(away) => away.untrack(me, topic).await,
        }
    }

    /// Everyone who is on a topic right now.
    ///
    /// One place holds this, always: a joiner has to be told who is on
    /// the channel across every node rather than who is on it here.
    pub async fn state(&self, app: &Arc<App>, topic: &str) -> Value {
        match self {
            Source::Held => app.hub.state(topic),
            Source::Away(away) => away.state(topic).await,
        }
    }

    /// What the project's own policies say about a private channel.
    ///
    /// Away, the token goes up rather than the claims out of it, and
    /// the holder verifies it for itself: a node asserting a claim set
    /// would be a node that can read any row in the project by claiming
    /// to be anybody.
    pub async fn ask(
        &self,
        app: &Arc<App>,
        token: Option<&str>,
        who: &Identity,
        ask: &Ask,
    ) -> Result<Grant, String> {
        match self {
            Source::Held => crate::realtime::answer(app, who, ask).await,
            Source::Away(away) => away.ask(token, ask).await,
        }
    }

    /// This socket is somebody else now, which is a client refreshing
    /// its token on a connection it is keeping.
    ///
    /// Held, the thing that cares is whatever checks rows against a
    /// policy. Away, nothing has to be told: every question that
    /// depends on who the socket is carries the token it is running on
    /// at the moment it is asked, so there is no state to keep level.
    pub async fn became(&self, app: &Arc<App>, who: &Identity, watching: &Option<Listening>) {
        if let Source::Held = self
            && let Some(listening) = watching.as_ref()
        {
            app.changes
                .became(listening.id, crate::realtime::asker(who));
        }
    }

    /// What one client's `postgres_changes` list turns into: the id
    /// each entry was given, in the order it was asked for, or the
    /// reason there are none.
    ///
    /// Away, the reason is that this node cannot do it yet. Changes
    /// over a link are the next piece of this and they are not this
    /// one, so a client that asks for them is refused in words on the
    /// system frame rather than joined to a channel that will never say
    /// anything.
    pub async fn watch(
        &self,
        app: &Arc<App>,
        who: &Identity,
        watching: &mut Option<Listening>,
        wants: &[Value],
    ) -> Result<Watched, String> {
        match self {
            Source::Held => crate::realtime::subscribed(app, who, watching, wants).await,
            Source::Away(_) => {
                Err("this node does not serve database changes for this project".to_string())
            }
        }
    }

    /// Those subscriptions are finished, because the channel they were
    /// asked for on has gone.
    pub async fn unwatch(&self, app: &Arc<App>, ids: Vec<u64>) {
        if let Source::Held = self {
            for id in ids {
                app.changes.unbind(id);
            }
        }
    }

    /// The socket is finished, and so is everything it asked for.
    ///
    /// A socket that went and left its subscriptions behind would be a
    /// policy check per changed row for nobody.
    pub async fn hung_up(&self, app: &Arc<App>, me: SocketId, watching: Option<Listening>) {
        match self {
            Source::Held => {
                if let Some(listening) = watching {
                    app.changes.hung_up(listening.id);
                }
            }
            Source::Away(away) => away.gone(me).await,
        }
    }
}
