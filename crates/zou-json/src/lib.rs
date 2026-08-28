//! Reading json somebody else wrote into a [`serde_json::Value`].
//!
//! `serde_json`'s own `Value` does not do this safely. Its deserializer
//! classifies the first key of every object, and with the `raw_value`
//! feature on, an object whose first key is
//! `$serde_json::private::RawValue` is not an object at all: the value
//! is read as a string and parsed again as json. So
//! `{"$serde_json::private::RawValue":"0"}` comes back as the number
//! zero, and the same key with anything but a string under it comes
//! back as a parse error about a raw value nobody asked for.
//!
//! That feature is not ours to turn off. `axum` turns it on for
//! anything that serves json, which is this whole server, so the
//! binary we ship has it. And the key is reachable: it is a column
//! name in a `jsonb` value, a field in a request body, a key in a
//! payload a client broadcasts. Sorting an object's keys on the way
//! out, which is what this workspace does so a recording of a frame is
//! a fact about the source rather than about the build, puts a dollar
//! sign first, so a payload that arrived with that key anywhere in an
//! object leaves with it in the one position that means something
//! else.
//!
//! Refusing the token would be wrong twice over: a string value may
//! carry those characters and mean nothing by them, and an object key
//! is data rather than an instruction. Escaping it does not work
//! either, since the key is unescaped before it is compared.
//!
//! What works is not asking `serde_json` for a `Value`. The tree is
//! built by a visitor of our own, where every nested value is
//! [`Plain`] rather than a `Value`, so the classifier is never reached
//! and a key in that namespace is an ordinary key with an ordinary
//! value under it. Numbers, strings, arrays and the rest are read the
//! way `serde_json` reads them, because they go through the same
//! deserializer.

use std::fmt;

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};

/// Parse text into a value, with no key meaning anything but itself.
pub fn from_str(text: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str::<Plain>(text).map(|parsed| parsed.0)
}

/// The same for bytes, for a body that arrived as bytes.
pub fn from_slice(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    serde_json::from_slice::<Plain>(bytes).map(|parsed| parsed.0)
}

/// A [`Value`] read without the private key classifier.
///
/// Public because a `#[derive(Deserialize)]` struct with a json field
/// in it needs a type to name, and naming `Value` there is the bug
/// this crate exists for.
pub struct Plain(pub Value);

impl<'de> Deserialize<'de> for Plain {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(PlainVisitor).map(Plain)
    }
}

struct PlainVisitor;

impl<'de> Visitor<'de> for PlainVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any json value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    /// A double that is not finite is not json. `serde_json` writes one
    /// as null, so reading one back as null is the same answer.
    fn visit_f64<E>(self, v: f64) -> Result<Value, E> {
        Ok(Number::from_f64(v).map_or(Value::Null, Value::Number))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.to_owned()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(Plain(item)) = seq.next_element()? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    /// The keys are read as strings and nothing is compared against
    /// them, which is the whole point of this file.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut out = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let Plain(value) = map.next_value()?;
            out.insert(key, value);
        }
        Ok(Value::Object(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The name `serde_json` reserves for itself, spelled here rather
    /// than imported because the point is that it has no meaning.
    const TOKEN: &str = "$serde_json::private::RawValue";

    #[test]
    fn the_private_key_is_an_ordinary_key() {
        let text = format!(r#"{{"{TOKEN}":"0"}}"#);
        let value = from_str(&text).expect("an object");
        assert_eq!(
            value.get(TOKEN).and_then(Value::as_str),
            Some("0"),
            "serde_json reads this one as the number zero"
        );
    }

    #[test]
    fn the_private_key_is_ordinary_nested_and_beside_others_too() {
        let text = format!(r#"{{"payload":{{"{TOKEN}":0,"other":"x"}}}}"#);
        let value = from_str(&text).expect("an object");
        let inner = value.get("payload").expect("the payload");
        assert_eq!(inner.get(TOKEN).and_then(Value::as_u64), Some(0));
        assert_eq!(inner.get("other").and_then(Value::as_str), Some("x"));
    }

    #[test]
    fn what_this_reads_it_reads_again_after_it_is_written() {
        let text = format!(r#"{{"{TOKEN}":0,"a":1}}"#);
        let once = from_str(&text).expect("an object");
        let written = serde_json::to_string(&once).expect("json");
        let twice = from_str(&written).expect("the same object again");
        assert_eq!(once, twice);
    }

    #[test]
    fn everything_else_reads_the_way_serde_json_reads_it() {
        for text in [
            "null",
            "true",
            "-3",
            "3.5",
            r#""a string""#,
            "[]",
            "[1,[2,{\"k\":null}],\"x\"]",
            "{}",
            r#"{"a":{"b":[1,2,3]},"c":false}"#,
            "18446744073709551615",
            "1e308",
        ] {
            assert_eq!(
                from_str(text).expect("it parses"),
                serde_json::from_str::<Value>(text).expect("it parses"),
                "{text} read differently"
            );
        }
    }

    #[test]
    fn what_is_not_json_is_still_not_json() {
        for text in ["", "{", "{\"a\"}", "[1,]", "nope"] {
            assert!(from_str(text).is_err(), "{text} should not parse");
        }
    }

    #[test]
    fn bytes_read_the_same_as_text() {
        let text = format!(r#"{{"{TOKEN}":"0","a":[1,2]}}"#);
        assert_eq!(
            from_slice(text.as_bytes()).expect("an object"),
            from_str(&text).expect("an object")
        );
    }
}
