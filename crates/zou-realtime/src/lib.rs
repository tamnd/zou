//! The realtime tier: the protocol realtime-js speaks, and the fan out
//! behind it.
//!
//! Supabase's realtime is a Phoenix application, so its wire protocol
//! is Phoenix Channels: one websocket carrying many topics, each topic
//! joined with `phx_join` and answered with `phx_reply`, a `heartbeat`
//! on the socket itself, and `phx_error` when a channel goes wrong.
//! What is on top of that is Supabase's: the topic naming, the join
//! payload's `config`, and the three things a channel can do, which
//! are broadcast, presence and postgres changes.
//!
//! This crate is the protocol and nothing else. There is no socket in
//! it, no http, and no runtime beyond the one channel type the fan out
//! is built from, so the whole thing is testable as a function from a
//! message to a list of things to do. [`session::Session`] is that
//! function, [`hub::Hub`] is where a broadcast goes between sockets,
//! and [`frame`] is what goes over the wire in the two json shapes and
//! the binary one that current clients use for broadcasts.
//!
//! What is built so far is the socket, the channels on it, tokens on
//! connect and mid connection, broadcast and presence. Postgres
//! changes and private channels are refused by name rather than
//! joined and left silent, which is the difference between a client
//! that reports an error and a client that waits forever.

pub mod frame;
pub mod hub;
pub mod session;

pub use frame::{BinaryBroadcast, Encoding, Frame, Vsn};
pub use hub::{Delivery, Hub, SocketId};
pub use session::{Action, Config, Fanout, Identity, Sent, Session, Tokens};
