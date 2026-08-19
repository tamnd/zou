//! Run with: cargo +nightly fuzz run jwt_verify
//!
//! Every request that arrives carrying an apikey or a bearer token has
//! that token split, base64 decoded and json parsed before anything
//! has checked who sent it, so this parser is reachable by anyone who
//! can open a connection and no input at all may panic it.
//!
//! The other half is that a token this server writes is a token this
//! server reads. Whatever claims come out of one, minting them again
//! has to give a token that verifies, name the same role, and carry
//! the same claim set back.

#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use zou_server::jwt::{self, Reject};

/// The one every zou dev instance ships with, which is as good a
/// secret as any and better than one the fuzzer can guess at.
const SECRET: &[u8] = b"super-secret-jwt-token-with-at-least-32-characters-long";

fuzz_target!(|data: &str| {
    // The apikey path and the bearer path both reach split and accept,
    // and neither may fall over on anything.
    let _ = jwt::verify_any(data, SECRET, None);
    match jwt::verify(data, SECRET) {
        Ok(verified) => round_trip(&verified.claims, verified.role.as_deref()),
        // Not a token this secret signed. Read the input as a claim set
        // instead, so bytes the fuzzer has shaped into json still reach
        // the half of this that decides what claims mean.
        Err(_) => {
            if let Ok(claims) = serde_json::from_str::<Value>(data) {
                let role = claims.get("role").and_then(Value::as_str);
                round_trip(&claims, role);
            }
        }
    }
});

fn round_trip(claims: &Value, role: Option<&str>) {
    // A claim set with serde_json's own escape hatch in it is not one.
    //
    // `$serde_json::private::RawValue` is the key serde_json uses to
    // smuggle a raw json document through its data model, and its
    // reader acts on it: a map whose first key is that one is read as
    // the document its value spells rather than as a map. Its writer
    // does not act on it, it writes the key out like any other, and it
    // sorts the keys, which puts that one first. So a map holding the
    // key somewhere other than first is written back out with it first
    // and then reads as something else, or as nothing.
    //
    // That is serde_json disagreeing with itself and there is nothing
    // here that could fix it. Nor is there anything here that can meet
    // it: every door into this server parses a body before anything
    // reaches a claim set, and a body carrying the key is refused as
    // json that will not parse. What is left is the fuzzer, which found
    // it by writing the key out by hand.
    if holds_the_raw_value_key(claims) {
        return;
    }
    let back = match jwt::verify(&jwt::mint(claims, SECRET), SECRET) {
        Ok(back) => back,
        // The three honest refusals of a token that was signed a moment
        // ago, all of them a claim about time that the claim set itself
        // carries: an exp already behind us, an nbf still ahead of us,
        // and an iat ahead of us. Minting says nothing about when, it
        // writes back whatever the claim set said, so a claim set the
        // fuzzer shaped with any of the three in it verifies to exactly
        // the refusal that claim asks for.
        Err(Reject::Expired | Reject::TooEarly | Reject::IssuedLater) => return,
        Err(why) => panic!("a token minted here did not verify here: {why:?}"),
    };
    assert_eq!(back.role.as_deref(), role, "the role did not round trip");
    if !holds_a_float(claims) {
        assert_eq!(&back.claims, claims, "the claims did not round trip");
    }
}

/// Whether the key serde_json reserves for itself is anywhere in here.
fn holds_the_raw_value_key(v: &Value) -> bool {
    const RESERVED: &str = "$serde_json::private::RawValue";
    match v {
        Value::Array(items) => items.iter().any(holds_the_raw_value_key),
        Value::Object(fields) => {
            fields.contains_key(RESERVED) || fields.values().any(holds_the_raw_value_key)
        }
        _ => false,
    }
}

/// Whether anything in here is a json number serde_json is holding as
/// an f64.
///
/// Writing an f64 out and reading it back is not the identity.
/// serde_json prints the shortest decimal that names the value and its
/// own parser does not always round that decimal back to the value it
/// names, so `0.22222222222211021576` arrives, is written as
/// `0.22222222222211022`, and reads back one ulp along; that one then
/// settles. `7340E047` does not settle at all, it flips between
/// `7.34e50` and `7.3400000000000004e50` on every trip. Both are
/// serde_json's rounding rather than anything here, so a claim set
/// carrying a float is checked for its role and its shape and not
/// compared value for value.
fn holds_a_float(v: &Value) -> bool {
    match v {
        Value::Number(n) => n.is_f64(),
        Value::Array(items) => items.iter().any(holds_a_float),
        Value::Object(fields) => fields.values().any(holds_a_float),
        _ => false,
    }
}
