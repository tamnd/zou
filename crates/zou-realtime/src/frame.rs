//! What goes over the socket, in the three shapes realtime-js sends
//! and reads.
//!
//! A frame is always the same five fields: the ref of the join that
//! opened the channel, the ref of this message, the topic, the event,
//! and the payload. Only the encoding differs.
//!
//! Version 1.0.0 is a json object with those five names in it, which
//! is what phoenix has always done and what realtime-js sent until it
//! moved its default. Version 2.0.0 is a json array in that order,
//! which is the same message with the names taken out. A client says
//! which it is speaking with `vsn` on the connect url, and a server
//! that answers in the other one is talking to nobody.
//!
//! The third shape is a binary broadcast, and it is not optional
//! either: realtime-js encodes every broadcast push that carries an
//! event name into it, which is every broadcast a client sends through
//! `channel.send`. A server that only reads json hears silence from a
//! current client and has no way to tell that is what happened.

use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::{Value, json};
use std::sync::OnceLock;

/// Which encoding this socket is speaking, from `vsn` on the connect
/// url.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vsn {
    /// A json object with named fields.
    V1,
    /// A json array, the order of the fields and nothing else.
    V2,
}

impl Vsn {
    /// What a `vsn` parameter means, with realtime-js's own default
    /// for a url that carries none. Anything else is not a version
    /// this speaks, and the caller should refuse the connection rather
    /// than guess.
    pub fn parse(vsn: Option<&str>) -> Option<Vsn> {
        match vsn {
            None | Some("") | Some("2.0.0") => Some(Vsn::V2),
            Some("1.0.0") => Some(Vsn::V1),
            Some(_) => None,
        }
    }
}

/// One message, either direction.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    /// The ref of the join that opened this channel. Null on the
    /// socket's own messages, like the heartbeat.
    pub join_ref: Option<String>,
    /// The ref of this message, which a reply carries back. Null on
    /// anything the server sends unprompted.
    pub reference: Option<String>,
    pub topic: String,
    pub event: String,
    pub payload: Value,
}

impl Frame {
    /// A reply to `to`, in phoenix's shape: the event is always
    /// phx_reply and the status lives inside the payload rather than
    /// in the event name.
    pub fn reply(to: &Frame, status: &str, response: Value) -> Frame {
        Frame {
            join_ref: to.join_ref.clone(),
            reference: to.reference.clone(),
            topic: to.topic.clone(),
            event: "phx_reply".into(),
            payload: json!({"status": status, "response": response}),
        }
    }

    /// An ok reply with nothing in it, which is what a heartbeat and a
    /// leave both get.
    pub fn ok(to: &Frame) -> Frame {
        Frame::reply(to, "ok", json!({}))
    }

    /// A refusal. The client joins the values of this object with
    /// commas to make the message on the error it hands the caller, so
    /// one field with the whole sentence in it reads better than four
    /// fields with a word each.
    pub fn error(to: &Frame, reason: impl Into<String>) -> Frame {
        Frame::reply(to, "error", json!({"reason": reason.into()}))
    }

    /// What the server says on a channel about the channel itself, and
    /// there is one of these: the line that follows a join that asked
    /// for postgres changes, once the subscriptions behind it exist.
    ///
    /// realtime-js reads it and does nothing with it, which is not a
    /// reason to leave it out. It is on the socket, an application that
    /// listens for `system` is handed it, and a recording of what
    /// upstream sends has it in it, so a server without it is a server
    /// whose frames are not the same frames.
    ///
    /// It carries the join's own ref and no ref of its own, the same
    /// way phoenix pushes anything on a joined channel, and the channel
    /// name in it is the topic without the prefix.
    /// The status is the whole of what the frame says: `ok` is the
    /// subscriptions being live, and `error` is the same frame carrying
    /// the reason there are none, which is how upstream answers a
    /// subscription to a table nobody published.
    pub fn system(to: &Frame, extension: &str, status: &str, message: &str) -> Frame {
        Frame {
            join_ref: to.join_ref.clone(),
            reference: None,
            topic: to.topic.clone(),
            event: "system".into(),
            payload: json!({
                "message": message,
                "status": status,
                "extension": extension,
                "channel": to.topic.strip_prefix("realtime:").unwrap_or(&to.topic),
            }),
        }
    }

    /// Something the server is saying on a topic without being asked,
    /// which is what a presence message is. There is no ref because
    /// nothing is being replied to, and the client matches it to a
    /// channel by topic and hands it to whatever is bound to the event.
    pub fn push(topic: &str, event: &str, payload: Value) -> Frame {
        Frame {
            join_ref: None,
            reference: None,
            topic: topic.into(),
            event: event.into(),
            payload,
        }
    }

    /// The channel has gone wrong and is not coming back. The client
    /// tears the channel down and retries the join.
    pub fn channel_error(topic: &str) -> Frame {
        Frame {
            join_ref: None,
            reference: None,
            topic: topic.into(),
            event: "phx_error".into(),
            payload: json!({}),
        }
    }

    /// Decode a text frame. Both versions are accepted whichever the
    /// socket said it speaks, because the cost of being wrong is a
    /// client that hears nothing, and an array is not a valid v1
    /// message nor an object a valid v2 one, so there is nothing to be
    /// ambiguous about.
    ///
    /// Read through `zou_json` rather than `serde_json`, because this
    /// is a client's text and `serde_json` reads an object whose first
    /// key is one particular string as something other than an object.
    pub fn decode(text: &str) -> Option<Frame> {
        let value = zou_json::from_str(text).ok()?;
        match value {
            Value::Array(parts) => {
                if parts.len() != 5 {
                    return None;
                }
                Some(Frame {
                    join_ref: as_ref(&parts[0]),
                    reference: as_ref(&parts[1]),
                    topic: parts[2].as_str()?.to_string(),
                    event: parts[3].as_str()?.to_string(),
                    payload: parts[4].clone(),
                })
            }
            Value::Object(map) => Some(Frame {
                join_ref: map.get("join_ref").and_then(as_ref),
                reference: map.get("ref").and_then(as_ref),
                topic: map.get("topic")?.as_str()?.to_string(),
                event: map.get("event")?.as_str()?.to_string(),
                payload: map.get("payload").cloned().unwrap_or(json!({})),
            }),
            // A bare number or string is valid json and is not a
            // message, which is what a client sending its own
            // keepalive text looks like.
            _ => None,
        }
    }

    /// Encode for a socket speaking `vsn`.
    ///
    /// Written straight out of the five fields rather than through a
    /// `Value` built from them. The old way of this built one and
    /// printed it, which walked and allocated the whole payload again
    /// to print it once, and the payload of a changed row goes to every
    /// socket watching the table: at a hundred thousand of them that
    /// copy is the most expensive thing on the path. Borrowing costs
    /// nothing and says the same bytes.
    pub fn encode(&self, vsn: Vsn) -> String {
        encoded(
            vsn,
            &self.join_ref,
            &self.reference,
            &self.topic,
            &self.event,
            &Ordered(&self.payload),
        )
    }

    /// The changed rows one channel is owed, encoded without a frame.
    ///
    /// This is the one message on the socket that is worth saying twice
    /// in the source. Every other frame is built once and sent once, so
    /// building it and encoding it is the same work either way. A change
    /// is one row going to every socket watching the table, and the ids
    /// are the only part of it that belongs to one of them, so a
    /// `Frame` here would be a copy of the whole row per socket to print
    /// something that already exists.
    pub fn changed(vsn: Vsn, topic: &str, ids: &[u64], data: &Value) -> String {
        encoded(
            vsn,
            &None,
            &None,
            topic,
            "postgres_changes",
            &Change { data, ids },
        )
    }
}

/// One socket's part of a change: the ids of its own subscriptions this
/// answers, and the row, which is everybody's.
///
/// The two names are in the order they are written in, which is the
/// order a struct is always serialized in. The row inside goes through
/// `Ordered` for the reason that type gives.
#[derive(Serialize)]
struct Change<'a> {
    #[serde(serialize_with = "ordered")]
    data: &'a Value,
    ids: &'a [u64],
}

fn ordered<S: Serializer>(value: &&Value, serializer: S) -> Result<S::Ok, S::Error> {
    Ordered(value).serialize(serializer)
}

/// A json value whose object keys come out in one order whatever the
/// build is doing.
///
/// `serde_json` has a `preserve_order` feature. With it off a
/// `Value::Object` is a `BTreeMap` and the keys are written sorted, and
/// with it on it is an `IndexMap` and they are written in the order
/// something happened to insert them. Nothing about a json object's
/// meaning changes either way and no client reads one differently, but
/// the feature is not ours to decide: anything else in the workspace
/// can turn it on, and cargo turns it on for everybody when it does.
/// `zou-deno` does, so `--features zou-deno/isolate` used to change the
/// bytes this crate puts on a socket, which makes a recording of our
/// frames a fact about a build rather than about the source.
///
/// So the frames are written sorted on purpose rather than by accident.
/// That is also the order upstream has: phoenix builds these payloads
/// as maps with atom keys, and a small map in erlang iterates in term
/// order, which for atoms is their name, so a recording of a real
/// `phx_reply` has `response` before `status` the same way this does.
///
/// In the build we ship the keys are sorted before this looks at them,
/// so it hands the value straight to serde and costs one atomic load.
struct Ordered<'a>(&'a Value);

impl Serialize for Ordered<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if sorted_already() {
            return self.0.serialize(serializer);
        }
        match self.0 {
            Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort_unstable();
                let mut out = serializer.serialize_map(Some(keys.len()))?;
                for key in keys {
                    out.serialize_entry(key, &Ordered(&map[key]))?;
                }
                out.end()
            }
            Value::Array(items) => serializer.collect_seq(items.iter().map(Ordered)),
            plain => plain.serialize(serializer),
        }
    }
}

/// A value printed with its keys in that order, for the places that
/// carry json as text rather than serializing it into something.
fn json_text(value: &Value) -> String {
    serde_json::to_string(&Ordered(value)).expect("a value is json")
}

/// Whether the `serde_json` in this build writes an object's keys
/// sorted, which is what asking it to write one says.
fn sorted_already() -> bool {
    static SORTED: OnceLock<bool> = OnceLock::new();
    *SORTED.get_or_init(|| json!({"b": 0, "a": 0}).to_string().starts_with(r#"{"a""#))
}

/// The five fields, in the shape `vsn` says, with nothing copied.
///
/// The old way of this built a `Value` of the five and printed it,
/// which walked and allocated the whole payload again to print it once.
/// Borrowing costs nothing and says the same bytes.
fn encoded<P: Serialize>(
    vsn: Vsn,
    join_ref: &Option<String>,
    reference: &Option<String>,
    topic: &str,
    event: &str,
    payload: &P,
) -> String {
    let encoded = match vsn {
        Vsn::V2 => serde_json::to_string(&(join_ref, reference, topic, event, payload)),
        Vsn::V1 => serde_json::to_string(&Named {
            event,
            join_ref,
            payload,
            reference,
            topic,
        }),
    };
    // A frame is json already: the payload is a `Value` and the rest
    // are strings, so there is nothing here serde can refuse.
    encoded.expect("a frame is json")
}

/// The five fields as version 1.0.0 names them, borrowed.
///
/// The names are written in the order they are declared in, which is
/// what a struct does whatever `serde_json` has been built with. They
/// are declared sorted so that this says the same bytes as the `Value`
/// this replaced.
#[derive(Serialize)]
struct Named<'a, P> {
    event: &'a str,
    join_ref: &'a Option<String>,
    payload: &'a P,
    #[serde(rename = "ref")]
    reference: &'a Option<String>,
    topic: &'a str,
}

/// A ref is a string on the wire, but phoenix has sent numbers in the
/// past and json has no opinion, so a number is read as the string it
/// prints as rather than dropped.
fn as_ref(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// The first byte of a binary frame, which says what the rest of it is.
mod kind {
    /// A broadcast a client is pushing up.
    pub const USER_BROADCAST_PUSH: u8 = 3;
    /// A broadcast the server is handing down.
    pub const USER_BROADCAST: u8 = 4;
}

/// How the payload at the end of a binary frame is to be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// Bytes, handed to the application as they are.
    Binary,
    /// A json document, parsed before the application sees it.
    Json,
}

impl Encoding {
    fn byte(self) -> u8 {
        match self {
            Encoding::Binary => 0,
            Encoding::Json => 1,
        }
    }
}

/// A broadcast in the binary encoding, either direction.
///
/// The payload is carried as bytes rather than as parsed json on
/// purpose. Half of these are not json at all, and the half that are
/// do not need to be understood by anything between the two clients,
/// so a server that parses and reprints them is spending cpu to change
/// nothing except the whitespace.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryBroadcast {
    pub join_ref: String,
    pub reference: String,
    pub topic: String,
    /// The application's own event name, the one the other client
    /// listens for. Not the phoenix event, which is always broadcast.
    pub event: String,
    /// The push's extra fields, json, empty when there were none.
    pub meta: String,
    pub encoding: Encoding,
    pub payload: Vec<u8>,
}

/// One byte for the kind and six for the five lengths and the
/// encoding, which is where a push's names start.
const PUSH_HEADER: usize = 7;

/// The same for a broadcast going down, which carries no refs and so
/// has two fewer lengths in front of it.
const BROADCAST_HEADER: usize = 5;

impl BinaryBroadcast {
    /// Read a push a client sent. None for anything that is not one,
    /// including a truncated frame, because there is nothing useful to
    /// do with half a message.
    pub fn decode(bytes: &[u8]) -> Option<BinaryBroadcast> {
        if bytes.len() < PUSH_HEADER || bytes[0] != kind::USER_BROADCAST_PUSH {
            return None;
        }
        let join_ref_len = bytes[1] as usize;
        let ref_len = bytes[2] as usize;
        let topic_len = bytes[3] as usize;
        let event_len = bytes[4] as usize;
        let meta_len = bytes[5] as usize;
        let encoding = match bytes[6] {
            0 => Encoding::Binary,
            1 => Encoding::Json,
            _ => return None,
        };
        let mut at = PUSH_HEADER;
        let mut take = |n: usize| -> Option<String> {
            let end = at.checked_add(n)?;
            let slice = bytes.get(at..end)?;
            at = end;
            String::from_utf8(slice.to_vec()).ok()
        };
        let join_ref = take(join_ref_len)?;
        let reference = take(ref_len)?;
        let topic = take(topic_len)?;
        let event = take(event_len)?;
        let meta = take(meta_len)?;
        let payload = bytes.get(at..)?.to_vec();
        Some(BinaryBroadcast {
            join_ref,
            reference,
            topic,
            event,
            meta,
            encoding,
            payload,
        })
    }

    /// Write this out as the broadcast the other clients receive.
    ///
    /// The refs are gone: they belonged to the sender's push and mean
    /// nothing to anyone else, and the decoder on the other side reads
    /// them as null.
    pub fn encode(&self) -> Vec<u8> {
        let topic = self.topic.as_bytes();
        let event = self.event.as_bytes();
        let meta = self.meta.as_bytes();
        let mut out = Vec::with_capacity(
            BROADCAST_HEADER + topic.len() + event.len() + meta.len() + self.payload.len(),
        );
        out.push(kind::USER_BROADCAST);
        out.push(topic.len() as u8);
        out.push(event.len() as u8);
        out.push(meta.len() as u8);
        out.push(self.encoding.byte());
        out.extend_from_slice(topic);
        out.extend_from_slice(event);
        out.extend_from_slice(meta);
        out.extend_from_slice(&self.payload);
        out
    }

    /// The same broadcast as a text frame, for a client that sent its
    /// push as json and expects the answer in kind.
    pub fn as_frame(&self) -> Option<Frame> {
        let payload: Value = match self.encoding {
            Encoding::Json => zou_json::from_slice(&self.payload).ok()?,
            Encoding::Binary => return None,
        };
        let mut body = json!({"type": "broadcast", "event": self.event, "payload": payload});
        if !self.meta.is_empty()
            && let Ok(meta) = zou_json::from_str(&self.meta)
        {
            body["meta"] = meta;
        }
        Some(Frame {
            join_ref: None,
            reference: None,
            topic: self.topic.clone(),
            event: "broadcast".into(),
            payload: body,
        })
    }

    /// A push that arrived as json, put into the same shape as one
    /// that arrived as bytes, so the fan out has one thing to carry.
    pub fn from_frame(frame: &Frame) -> Option<BinaryBroadcast> {
        let event = frame.payload.get("event")?.as_str()?.to_string();
        let payload = frame.payload.get("payload").cloned().unwrap_or(json!({}));
        let mut meta = frame.payload.clone();
        if let Some(map) = meta.as_object_mut() {
            map.remove("type");
            map.remove("event");
            map.remove("payload");
        }
        // Through `Ordered`, so a push that came up as json goes down
        // as the same bytes in every build rather than in whichever
        // order this one happened to keep the client's fields in.
        let meta = match meta.as_object() {
            Some(map) if !map.is_empty() => json_text(&meta),
            _ => String::new(),
        };
        Some(BinaryBroadcast {
            join_ref: frame.join_ref.clone().unwrap_or_default(),
            reference: frame.reference.clone().unwrap_or_default(),
            topic: frame.topic.clone(),
            event,
            meta,
            encoding: Encoding::Json,
            payload: json_text(&payload).into_bytes(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_array_form_is_the_five_fields_in_order() {
        let frame = Frame::decode(r#"["3","4","realtime:room","phx_join",{"config":{}}]"#).unwrap();
        assert_eq!(frame.join_ref.as_deref(), Some("3"));
        assert_eq!(frame.reference.as_deref(), Some("4"));
        assert_eq!(frame.topic, "realtime:room");
        assert_eq!(frame.event, "phx_join");
        assert_eq!(frame.payload, json!({"config": {}}));
    }

    #[test]
    fn the_object_form_is_the_same_message_with_names_on_it() {
        let one = Frame::decode(r#"["3","4","realtime:room","phx_join",{}]"#).unwrap();
        let other = Frame::decode(
            r#"{"join_ref":"3","ref":"4","topic":"realtime:room","event":"phx_join","payload":{}}"#,
        )
        .unwrap();
        assert_eq!(one, other);
    }

    #[test]
    fn a_heartbeat_carries_no_refs_of_its_own() {
        let frame = Frame::decode(r#"[null,"7","phoenix","heartbeat",{}]"#).unwrap();
        assert_eq!(frame.join_ref, None);
        assert_eq!(frame.reference.as_deref(), Some("7"));
        assert_eq!(
            Frame::ok(&frame).encode(Vsn::V2),
            r#"[null,"7","phoenix","phx_reply",{"response":{},"status":"ok"}]"#
        );
    }

    // The payloads below are written with their keys out of order on
    // purpose. In a build without `preserve_order` there is no way to
    // hold an object unsorted and these say nothing, and in a build
    // with it they are the whole point: they are what a client's own
    // ordering, or the order `json!` was written in, looks like by the
    // time it reaches the socket.
    #[test]
    fn a_frame_says_the_same_bytes_whatever_serde_json_was_built_with() {
        let frame = Frame::push(
            "realtime:room",
            "presence_state",
            json!({"u2": {"metas": []}, "u1": {"metas": [{"phx_ref": "1", "at": "noon"}]}}),
        );
        assert_eq!(
            frame.encode(Vsn::V2),
            r#"[null,null,"realtime:room","presence_state",{"u1":{"metas":[{"at":"noon","phx_ref":"1"}]},"u2":{"metas":[]}}]"#
        );
        assert_eq!(
            frame.encode(Vsn::V1),
            r#"{"event":"presence_state","join_ref":null,"payload":{"u1":{"metas":[{"at":"noon","phx_ref":"1"}]},"u2":{"metas":[]}},"ref":null,"topic":"realtime:room"}"#
        );
    }

    #[test]
    fn a_changed_row_is_written_the_same_way() {
        assert_eq!(
            Frame::changed(
                Vsn::V2,
                "realtime:room",
                &[8],
                &json!({"type": "INSERT", "record": {"id": 1}, "table": "todos"}),
            ),
            r#"[null,null,"realtime:room","postgres_changes",{"data":{"record":{"id":1},"table":"todos","type":"INSERT"},"ids":[8]}]"#
        );
    }

    // Found by the fuzzer. `serde_json` reads an object whose first key
    // is its own private one as a raw value rather than as an object,
    // and since keys are sorted on the way out, a dollar sign lands
    // first: a payload that arrived beside an ordinary key left in the
    // one position that means something else, and the frame no longer
    // read back. Nothing about the key is special to a client, so it
    // has to survive the round trip like any other.
    #[test]
    fn a_payload_key_serde_json_reserves_for_itself_is_just_a_key() {
        let token = "$serde_json::private::RawValue";
        for payload in [
            json!({ token: "0" }),
            json!({ token: 0, "a": 1 }),
            json!({"nested": { token: ["x"] }}),
        ] {
            let frame = Frame::push("realtime:room", "broadcast", payload.clone());
            for vsn in [Vsn::V1, Vsn::V2] {
                let written = frame.encode(vsn);
                let again = Frame::decode(&written)
                    .unwrap_or_else(|| panic!("{written} was written here and did not read back"));
                assert_eq!(again.payload, payload);
            }
        }
    }

    #[test]
    fn a_broadcast_carrying_that_key_still_reaches_a_json_client() {
        let token = "$serde_json::private::RawValue";
        let frame = Frame::decode(&format!(
            r#"["5","6","realtime:room","broadcast",{{"type":"broadcast","event":"cursor","payload":{{"{token}":"0"}},"{token}":"m"}}]"#
        ))
        .expect("a push");
        let push = BinaryBroadcast::from_frame(&frame).expect("a broadcast");
        let out = push.as_frame().expect("a json client reads it back");
        assert_eq!(out.payload["payload"][token], json!("0"));
        assert_eq!(out.payload["meta"][token], json!("m"));
    }

    #[test]
    fn a_json_push_carries_the_same_bytes_into_the_binary_broadcast() {
        let frame = Frame::decode(
            r#"["5","6","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"y":2,"x":1},"seq":3,"id":"a"}]"#,
        )
        .unwrap();
        let push = BinaryBroadcast::from_frame(&frame).unwrap();
        assert_eq!(push.payload, br#"{"x":1,"y":2}"#);
        assert_eq!(push.meta, r#"{"id":"a","seq":3}"#);
    }

    #[test]
    fn what_is_not_a_frame_is_not_read_as_one() {
        for text in [
            "",
            "not json",
            "[1,2,3]",
            r#"["1","2","realtime:room"]"#,
            r#"{"event":"phx_join"}"#,
        ] {
            assert!(Frame::decode(text).is_none(), "{text} was read as a frame");
        }
    }

    #[test]
    fn a_version_nobody_speaks_is_not_guessed_at() {
        assert_eq!(Vsn::parse(None), Some(Vsn::V2));
        assert_eq!(Vsn::parse(Some("2.0.0")), Some(Vsn::V2));
        assert_eq!(Vsn::parse(Some("1.0.0")), Some(Vsn::V1));
        assert_eq!(Vsn::parse(Some("3.0.0")), None);
    }

    /// The bytes a current realtime-js puts on the wire for
    /// `channel.send({type: 'broadcast', event: 'cursor', payload:
    /// {x: 1}})`, built the way its serializer builds them.
    fn a_push() -> Vec<u8> {
        let mut bytes = vec![3u8, 1, 1, 13, 6, 0, 1];
        bytes.extend_from_slice(b"5");
        bytes.extend_from_slice(b"6");
        bytes.extend_from_slice(b"realtime:room");
        bytes.extend_from_slice(b"cursor");
        bytes.extend_from_slice(br#"{"x":1}"#);
        bytes
    }

    #[test]
    fn a_binary_push_is_read_field_by_field() {
        let push = BinaryBroadcast::decode(&a_push()).unwrap();
        assert_eq!(push.join_ref, "5");
        assert_eq!(push.reference, "6");
        assert_eq!(push.topic, "realtime:room");
        assert_eq!(push.event, "cursor");
        assert_eq!(push.meta, "");
        assert_eq!(push.encoding, Encoding::Json);
        assert_eq!(push.payload, br#"{"x":1}"#);
    }

    #[test]
    fn what_goes_down_is_what_came_up_without_the_refs() {
        let push = BinaryBroadcast::decode(&a_push()).unwrap();
        let out = push.encode();
        assert_eq!(out[0], 4);
        assert_eq!(out[1] as usize, push.topic.len());
        assert_eq!(out[2] as usize, push.event.len());
        assert_eq!(out[3], 0);
        assert_eq!(out[4], 1);
        let at = 5 + push.topic.len() + push.event.len();
        assert_eq!(&out[5..5 + push.topic.len()], push.topic.as_bytes());
        assert_eq!(&out[at..], br#"{"x":1}"#);
    }

    #[test]
    fn half_a_binary_frame_is_not_half_a_broadcast() {
        let push = a_push();
        for cut in 0..PUSH_HEADER + 3 {
            assert!(
                BinaryBroadcast::decode(&push[..cut]).is_none(),
                "{cut} bytes was read as a push"
            );
        }
        let mut wrong_kind = push.clone();
        wrong_kind[0] = 4;
        assert!(BinaryBroadcast::decode(&wrong_kind).is_none());
    }

    #[test]
    fn a_json_push_and_a_binary_one_carry_the_same_thing() {
        let frame = Frame::decode(
            r#"["5","6","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{"x":1}}]"#,
        )
        .unwrap();
        let push = BinaryBroadcast::from_frame(&frame).unwrap();
        assert_eq!(push.event, "cursor");
        assert_eq!(push.topic, "realtime:room");
        assert_eq!(push.payload, br#"{"x":1}"#);
        let down = push.as_frame().unwrap();
        assert_eq!(down.event, "broadcast");
        assert_eq!(down.join_ref, None);
        assert_eq!(
            down.payload,
            json!({"type": "broadcast", "event": "cursor", "payload": {"x": 1}})
        );
    }

    #[test]
    fn what_the_push_carried_beyond_the_event_and_the_payload_is_kept() {
        let frame = Frame::decode(
            r#"["5","6","realtime:room","broadcast",{"type":"broadcast","event":"cursor","payload":{},"id":"a"}]"#,
        )
        .unwrap();
        let push = BinaryBroadcast::from_frame(&frame).unwrap();
        assert_eq!(push.meta, r#"{"id":"a"}"#);
        assert_eq!(push.as_frame().unwrap().payload["meta"], json!({"id": "a"}));
    }
}
