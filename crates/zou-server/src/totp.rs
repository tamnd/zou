//! The six digits an authenticator app shows, and the string a phone
//! camera turns into an account in one.
//!
//! RFC 6238 over RFC 4226: HMAC-SHA1, thirty second steps, six digits,
//! one step of slack either side. None of those is configurable, here
//! or upstream, because every authenticator ever shipped has already
//! agreed to them and a project that changed one would be handing out
//! secrets its users' apps cannot read.
//!
//! The secret is twenty bytes from the os rng, written down in base32
//! without padding, which is the only encoding the otpauth url has.

use hmac::{Hmac, KeyInit, Mac};
use sha1::Sha1;

/// Seconds in a step. The counter is the unix second divided by this.
const PERIOD: i64 = 30;

/// How many steps either side of now are still accepted, which is what
/// covers a phone whose clock has drifted and a person who types
/// slowly.
const SKEW: i64 = 1;

/// Digits in a code. Six, and exactly six: a code of any other length
/// is refused without being compared, the way hotp.ValidateCustom
/// refuses it.
const DIGITS: usize = 6;

/// Bytes of secret. pquerna/otp's default, so the base32 is thirty two
/// characters and needs no padding.
const SECRET_BYTES: usize = 20;

const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// A fresh secret, base32 without padding, ready to be shown to a
/// person and stored as it stands.
pub fn secret() -> String {
    let mut raw = [0u8; SECRET_BYTES];
    getrandom::fill(&mut raw).expect("the os rng never fails");
    encode(&raw)
}

/// base32 as RFC 4648 writes it, without the padding Go's
/// StdEncoding.WithPadding(NoPadding) leaves off.
fn encode(raw: &[u8]) -> String {
    let mut out = String::with_capacity(raw.len().div_ceil(5) * 8);
    let mut bits = 0u32;
    let mut held = 0u32;
    for byte in raw {
        bits = (bits << 8) | *byte as u32;
        held += 8;
        while held >= 5 {
            held -= 5;
            out.push(ALPHABET[((bits >> held) & 0x1f) as usize] as char);
        }
    }
    if held > 0 {
        out.push(ALPHABET[((bits << (5 - held)) & 0x1f) as usize] as char);
    }
    out
}

/// The other direction, tolerant in the two ways Go's is: padding is
/// optional and case does not matter. Anything else is not base32 and
/// there is no secret to compare against.
fn decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let mut bits = 0u32;
    let mut held = 0u32;
    for c in s.trim().chars().take_while(|c| *c != '=') {
        let index = ALPHABET
            .iter()
            .position(|a| *a == c.to_ascii_uppercase() as u8)?;
        bits = (bits << 5) | index as u32;
        held += 5;
        if held >= 8 {
            held -= 8;
            out.push((bits >> held) as u8);
        }
    }
    Some(out)
}

/// The otpauth url, byte for byte what pquerna/otp's Key.URL() writes:
/// the label is issuer and account joined by a colon, and the query is
/// Go's url.Values.Encode, which sorts its keys.
pub fn uri(issuer: &str, account: &str, secret: &str) -> String {
    let query = crate::auth::encoded(&[
        ("algorithm", "SHA1".to_string()),
        ("digits", DIGITS.to_string()),
        ("issuer", issuer.to_string()),
        ("period", PERIOD.to_string()),
        ("secret", secret.to_string()),
    ]);
    format!(
        "otpauth://totp/{}?{query}",
        path_escape(&format!("{issuer}:{account}"))
    )
}

/// Go's url escaping for a path segment, which leaves the sub delimiters
/// and the colon and the at sign alone. It is why an address in a label
/// stays readable in the url an app shows back to a person.
fn path_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Whether a code is one this secret produces around this instant.
///
/// The order the steps are tried in is upstream's: now, then one ahead,
/// then one behind. It matters only for how long a wrong code takes to
/// be refused, and it is kept because keeping it costs nothing.
pub fn valid(code: &str, secret: &str, at: i64) -> bool {
    let code = code.trim();
    if code.len() != DIGITS {
        return false;
    }
    let Some(key) = decode(secret) else {
        return false;
    };
    let step = at.div_euclid(PERIOD);
    let mut counters = vec![step];
    for i in 1..=SKEW {
        counters.push(step + i);
        counters.push(step - i);
    }
    counters
        .into_iter()
        .any(|counter| same(&digits(&key, counter as u64), code))
}

/// The six digits an app holding this secret would be showing at this
/// instant, or None when the secret is not base32.
///
/// This is the phone's half of the exchange rather than the server's.
/// It is here because the server hands the secret out at enrollment and
/// something on this side has to be able to act as the app: the live
/// tests do, and so does anything embedding zou that wants to check a
/// factor works before telling a person it does.
pub fn code(secret: &str, at: i64) -> Option<String> {
    let key = decode(secret)?;
    Some(digits(&key, at.div_euclid(PERIOD) as u64))
}

/// The code this secret produces for one step, which is RFC 4226's
/// dynamic truncation of an HMAC over the counter.
fn digits(key: &[u8], counter: u64) -> String {
    let mut mac = Hmac::<Sha1>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(&counter.to_be_bytes());
    let sum = mac.finalize().into_bytes();
    let offset = (sum[sum.len() - 1] & 0xf) as usize;
    let value = (((sum[offset] & 0x7f) as u32) << 24)
        | ((sum[offset + 1] as u32) << 16)
        | ((sum[offset + 2] as u32) << 8)
        | (sum[offset + 3] as u32);
    format!("{:0width$}", value % 1_000_000, width = DIGITS)
}

/// A comparison that does not stop at the first wrong digit, because
/// how long a refusal takes is a thing an attacker can measure.
fn same(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// The size of one module of the QR code in the svg, and the margin
/// around it. Both are goqrsvg's, which is what GoTrue draws with.
const MODULE: i32 = 3;
const MARGIN: i32 = 5;

/// The url as an svg document, which is what the enroll response
/// carries so a client can drop it straight into the page. None when
/// the url is too long to encode, which cannot happen for an otpauth
/// url but is not worth panicking over.
///
/// The correction level is the high one upstream picks, so a code
/// printed small or photographed badly still reads.
pub fn qr_svg(text: &str) -> Option<String> {
    let qr = qrcodegen::QrCode::encode_text(text, qrcodegen::QrCodeEcc::High).ok()?;
    let side = qr.size() * MODULE + MARGIN * 2;
    let mut out = String::with_capacity(1 << 13);
    out.push_str("<?xml version=\"1.0\"?>\n");
    out.push_str(
        "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \
         \"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">\n",
    );
    out.push_str(&format!(
        "<svg width=\"{side}\" height=\"{side}\"\n \
         xmlns=\"http://www.w3.org/2000/svg\" \
         xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n"
    ));
    out.push_str(&format!(
        "<rect x=\"0\" y=\"0\" width=\"{side}\" height=\"{side}\" style=\"fill:white\"/>\n"
    ));
    for x in 0..qr.size() {
        for y in 0..qr.size() {
            if qr.get_module(x, y) {
                out.push_str(&format!(
                    "<rect x=\"{}\" y=\"{}\" width=\"{MODULE}\" height=\"{MODULE}\" \
                     style=\"fill:black;stroke:none\"/>\n",
                    x * MODULE + MARGIN,
                    y * MODULE + MARGIN,
                ));
            }
        }
    }
    out.push_str("</svg>\n");
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The secret RFC 6238's test vectors are written against, base32
    /// encoded: twenty ascii bytes, "12345678901234567890".
    const RFC: &str = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ";

    #[test]
    fn the_rfc_6238_vectors_come_out() {
        // The table in appendix B, truncated to the six digits an
        // authenticator app actually shows.
        for (at, expected) in [
            (59, "287082"),
            (1111111109, "081804"),
            (1111111111, "050471"),
            (1234567890, "005924"),
            (2000000000, "279037"),
            (20000000000i64, "353130"),
        ] {
            let key = decode(RFC).expect("the vector secret is base32");
            assert_eq!(digits(&key, (at / PERIOD) as u64), expected, "at {at}");
            assert!(valid(expected, RFC, at), "at {at}");
        }
    }

    #[test]
    fn the_code_an_app_would_show_is_the_one_the_server_accepts() {
        // The same appendix B table, from the phone's side. Checking it
        // against valid() alone would not notice a step of drift,
        // because valid() forgives one.
        for (at, expected) in [
            (59, "287082"),
            (1111111109, "081804"),
            (1234567890, "005924"),
            (2000000000, "279037"),
        ] {
            let shown = code(RFC, at).expect("the vector secret is base32");
            assert_eq!(shown, expected, "at {at}");
            assert!(valid(&shown, RFC, at), "at {at}");
        }
        assert!(code("not base 32!", 59).is_none());
    }

    #[test]
    fn a_code_is_good_for_one_step_either_side_and_no_further() {
        // The code the app showed at this instant, checked against a
        // server whose clock is a step out either way, and then two.
        let at = 1111111109;
        assert!(valid("081804", RFC, at));
        assert!(valid("081804", RFC, at - 30), "one step behind");
        assert!(valid("081804", RFC, at + 30), "one step ahead");
        assert!(!valid("081804", RFC, at - 60), "two steps behind");
        assert!(!valid("081804", RFC, at + 60), "two steps ahead");
    }

    #[test]
    fn a_code_that_is_not_six_digits_is_never_right() {
        for code in ["", "28708", "2870820", "abcdef", "287 082"] {
            assert!(!valid(code, RFC, 59), "{code:?}");
        }
        // Whitespace around the right code is trimmed, which is what a
        // paste out of an app carries.
        assert!(valid(" 287082 ", RFC, 59));
    }

    #[test]
    fn a_secret_that_is_not_base32_refuses_rather_than_matching() {
        assert!(!valid("287082", "not base 32!", 59));
        assert!(decode("!!!!").is_none());
    }

    #[test]
    fn a_fresh_secret_is_thirty_two_characters_of_base32() {
        let s = secret();
        assert_eq!(s.len(), 32, "{s}");
        assert!(
            s.bytes().all(|b| ALPHABET.contains(&b)),
            "{s} is not base32"
        );
        assert_ne!(s, secret(), "two secrets are not the same secret");
        assert_eq!(decode(&s).expect("round trips").len(), SECRET_BYTES);
    }

    #[test]
    fn base32_round_trips_every_length_of_input() {
        for len in 0..16 {
            let raw: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            let text = encode(&raw);
            assert_eq!(decode(&text).expect("round trips"), raw, "{len} bytes");
        }
    }

    #[test]
    fn padding_and_case_are_both_tolerated() {
        // Go pads the secret back out to a multiple of eight and
        // uppercases it before decoding, so all three of these are the
        // same secret.
        let expected = decode(RFC).unwrap();
        assert_eq!(decode(&RFC.to_lowercase()).unwrap(), expected);
        assert_eq!(
            decode("GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ====").unwrap(),
            expected
        );
    }

    #[test]
    fn the_url_is_the_one_an_authenticator_expects() {
        assert_eq!(
            uri("zou.test", "person@zou.test", "ABCDEF"),
            "otpauth://totp/zou.test:person@zou.test\
             ?algorithm=SHA1&digits=6&issuer=zou.test&period=30&secret=ABCDEF"
        );
    }

    #[test]
    fn a_label_with_a_space_in_it_is_escaped_the_way_go_escapes_it() {
        // The path keeps the colon and the at sign and percent encodes
        // the space, and the query turns the space into a plus.
        assert_eq!(
            uri("My App", "person@zou.test", "ABCDEF"),
            "otpauth://totp/My%20App:person@zou.test\
             ?algorithm=SHA1&digits=6&issuer=My+App&period=30&secret=ABCDEF"
        );
    }

    #[test]
    fn the_qr_code_is_an_svg_of_the_url() {
        let url = uri("zou.test", "person@zou.test", RFC);
        let svg = qr_svg(&url).expect("an otpauth url always fits");
        assert!(
            svg.starts_with("<?xml version=\"1.0\"?>\n<!DOCTYPE svg"),
            "{svg}"
        );
        assert!(svg.ends_with("</svg>\n"));
        // A version 1 code is 21 modules and this url needs more than
        // that, so the drawing is at least that big and square.
        let side: i32 = svg
            .split("width=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .and_then(|s| s.parse().ok())
            .expect("the svg declares a width");
        assert!(side > 21 * MODULE, "{side}");
        assert!(svg.contains(&format!("height=\"{side}\"")), "not square");
        assert!(
            svg.contains("style=\"fill:black;stroke:none\""),
            "no modules"
        );
    }
}
