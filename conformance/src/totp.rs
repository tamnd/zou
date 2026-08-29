//! The six digits an authenticator app would be showing, so that a case
//! can verify a factor it has just enrolled.
//!
//! Written here rather than called out of `zou_server::totp`, which this
//! crate already links and which does the same arithmetic. A harness
//! that generated its codes with the implementation under test would
//! agree with itself: a zou that computed the wrong digits would be
//! asked with the wrong digits, accept them, and pass, while the
//! reference next to it refused the same request for the right reason.
//! Two implementations that agree is the whole of what this proves, so
//! there have to be two.
//!
//! RFC 6238 over RFC 4226, at the parameters every authenticator has
//! already agreed to: HMAC-SHA1, thirty second steps, six digits. There
//! is nothing to configure because there is nothing either end would
//! accept configured differently.

use hmac::{Hmac, KeyInit, Mac};
use sha1::Sha1;

/// The step length in seconds.
const PERIOD: i64 = 30;

/// How many digits a code has.
const DIGITS: usize = 6;

/// RFC 4648's base32 alphabet, which is the only encoding an otpauth url
/// carries a secret in.
const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// The code this secret produces at this instant, or None when the
/// secret is not base32.
pub fn code(secret: &str, at: i64) -> Option<String> {
    let key = decode(secret)?;
    let counter = at.div_euclid(PERIOD) as u64;
    let mut mac = Hmac::<Sha1>::new_from_slice(&key).expect("hmac accepts any key length");
    mac.update(&counter.to_be_bytes());
    let sum = mac.finalize().into_bytes();
    // RFC 4226's dynamic truncation: the low nibble of the last byte
    // says where in the digest to read the number from.
    let offset = (sum[sum.len() - 1] & 0xf) as usize;
    let value = (((sum[offset] & 0x7f) as u32) << 24)
        | ((sum[offset + 1] as u32) << 16)
        | ((sum[offset + 2] as u32) << 8)
        | (sum[offset + 3] as u32);
    Some(format!("{:0width$}", value % 1_000_000, width = DIGITS))
}

/// Now, as a count of seconds, which is the only instant a case ever
/// wants a code for.
pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

/// The bytes behind a base32 string, padded or not, in either case.
///
/// A secret is written without padding in an otpauth url and with it in
/// some of the places a secret gets pasted, and a harness that refused
/// one of the two would be refusing over the transport rather than over
/// the key.
fn decode(text: &str) -> Option<Vec<u8>> {
    let mut bits: u32 = 0;
    let mut held = 0u32;
    let mut out = Vec::with_capacity(text.len() * 5 / 8);
    for byte in text.trim_end_matches('=').bytes() {
        let upper = byte.to_ascii_uppercase();
        let value = ALPHABET.iter().position(|c| *c == upper)? as u32;
        bits = (bits << 5) | value;
        held += 5;
        if held >= 8 {
            held -= 8;
            out.push((bits >> held) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 6238's own secret, which is the ascii digits 1 to 0 twice
    /// over written in base32.
    const RFC: &str = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ";

    /// The first three rows of RFC 6238's SHA1 table, which is the
    /// point of writing this out again: if it did not match these it
    /// would be a second implementation of the wrong thing.
    #[test]
    fn the_rfc_vectors() {
        assert_eq!(code(RFC, 59).as_deref(), Some("287082"));
        assert_eq!(code(RFC, 1111111109).as_deref(), Some("081804"));
        assert_eq!(code(RFC, 1111111111).as_deref(), Some("050471"));
    }

    /// Every second inside a step is the same code, and the one after
    /// it is not.
    #[test]
    fn a_code_lasts_a_step() {
        let inside = code(RFC, 60).unwrap();
        assert_eq!(code(RFC, 89).as_deref(), Some(inside.as_str()));
        assert_ne!(code(RFC, 90).as_deref(), Some(inside.as_str()));
    }

    #[test]
    fn padding_is_allowed_and_so_is_lower_case() {
        let plain = code("JBSWY3DPEHPK3PXP", 0);
        assert!(plain.is_some());
        assert_eq!(code("JBSWY3DPEHPK3PXP======", 0), plain);
        assert_eq!(code("jbswy3dpehpk3pxp", 0), plain);
    }

    #[test]
    fn a_secret_that_is_not_base32_has_no_code() {
        assert_eq!(code("not base32!", 0), None);
    }
}
