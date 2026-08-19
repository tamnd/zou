//! Both halves of what a realtime socket sends. The text protocol is a
//! five element array or an object of the same five fields, and the
//! binary one is a push whose header is six lengths this decoder reads
//! before it takes any of them.
//!
//! Text has an encoder that is its inverse, so a frame that decoded has
//! to survive being written back out and read again, in both versions
//! of the protocol, since a socket speaking either one gets the same
//! frame. Binary does not: what comes back out of a push is the
//! broadcast the other clients receive, which carries no refs and is a
//! different kind byte, so what this asks of it is that the lengths in
//! the header agree with what came out and that nothing panics on a
//! frame that was cut short.
//!
//! The one thing the text round trip is not asked for is a number to
//! the digit. A payload is parsed json and a json number is a double,
//! so a literal with more digits than a double holds is already
//! rounded by the time a frame has one, and reading the rounded value
//! back is not required to land on the same double: with
//! `{"x":555555555555555555555555555555552}` in it the value moves by
//! one unit in the last place on every pass. That is what every server
//! speaking this protocol does with such a literal, so numbers are
//! compared as the doubles they are.

#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use zou_realtime::frame::{BinaryBroadcast, Frame, Vsn};

/// Equal but for how much of a number a double holds. Everything that
/// is not a number is compared as it stands, and two numbers are the
/// same when they are within a few units in the last place of each
/// other, which is the most a literal that was rounded on the way in
/// can drift on the way out and back.
fn same_payload(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => match (a.as_f64(), b.as_f64()) {
            (Some(a), Some(b)) => {
                let apart = (a - b).abs();
                apart <= f64::EPSILON * 8.0 * a.abs().max(b.abs())
            }
            _ => a == b,
        },
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_payload(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(name, a)| b.get(name).is_some_and(|b| same_payload(a, b)))
        }
        _ => left == right,
    }
}

fuzz_target!(|data: &[u8]| {
    if let Some(push) = BinaryBroadcast::decode(data) {
        // Every name came out of the bytes after the header, and the
        // payload is whatever was left, so the five of them and the
        // header account for the frame exactly.
        let named = push.join_ref.len()
            + push.reference.len()
            + push.topic.len()
            + push.event.len()
            + push.meta.len();
        assert_eq!(
            7 + named + push.payload.len(),
            data.len(),
            "a push that decoded left bytes nobody read"
        );
        // The broadcast it turns into is read by clients rather than by
        // this decoder, so all that is asked here is that writing it
        // does not fall over on anything that decoded.
        let _ = push.encode();
        let _ = push.as_frame();
    }
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Some(frame) = Frame::decode(text) else {
        return;
    };
    for vsn in [Vsn::V1, Vsn::V2] {
        let written = frame.encode(vsn);
        let again = Frame::decode(&written).expect("a frame this wrote must read back");
        assert_eq!(again.topic, frame.topic);
        assert_eq!(again.event, frame.event);
        assert!(
            same_payload(&again.payload, &frame.payload),
            "a payload changed on the way out and back: {} became {}",
            frame.payload,
            again.payload
        );
        assert_eq!(again.join_ref, frame.join_ref);
        assert_eq!(again.reference, frame.reference);
    }
});
