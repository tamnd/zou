//! `crypto`: randomness and hashes, neither of which is javascript's.
//!
//! Random bytes come from the operating system, and a hash written in
//! javascript would be slow and would be a fourth implementation of
//! sha256 in this repository. Both are already linked in here: the
//! server signs its own tokens with `hmac` and hashes with `sha2`, so
//! what a function reaches through `crypto` is the same code the rest
//! of this process trusts.
//!
//! What is here is the part of `SubtleCrypto` a function actually uses:
//! a digest, and HMAC signing and verifying. Encryption, key
//! derivation, key generation and the asymmetric algorithms are not,
//! and each is refused by name in the prelude rather than being
//! undefined.

use deno_core::op2;
use deno_error::JsErrorBox;
use hmac::{KeyInit, Mac, SimpleHmac};
use sha1::Sha1;
use sha2::digest::common::BlockSizeUser;
use sha2::{Digest, Sha256, Sha384, Sha512};

/// Bytes from the operating system, into the array javascript handed
/// over, which is where `crypto.getRandomValues` puts them.
#[op2(fast)]
pub fn op_zou_random(#[buffer] into: &mut [u8]) -> Result<(), JsErrorBox> {
    getrandom::fill(into)
        .map_err(|e| JsErrorBox::type_error(format!("the host has no randomness to give: {e}")))
}

#[op2]
#[buffer]
pub fn op_zou_digest(
    #[string] algorithm: &str,
    #[buffer] data: &[u8],
) -> Result<Vec<u8>, JsErrorBox> {
    digest(algorithm, data).ok_or_else(|| unknown(algorithm))
}

#[op2]
#[buffer]
pub fn op_zou_sign(
    #[string] algorithm: &str,
    #[buffer] key: &[u8],
    #[buffer] data: &[u8],
) -> Result<Vec<u8>, JsErrorBox> {
    signed(algorithm, key, data).ok_or_else(|| unknown(algorithm))
}

/// The comparison is here rather than in the prelude because a
/// comparison in javascript stops at the first byte that differs, and
/// how long a wrong answer took is how a signature is guessed.
#[op2(fast)]
pub fn op_zou_verify(
    #[string] algorithm: &str,
    #[buffer] key: &[u8],
    #[buffer] data: &[u8],
    #[buffer] signature: &[u8],
) -> Result<bool, JsErrorBox> {
    verified(algorithm, key, data, signature).ok_or_else(|| unknown(algorithm))
}

fn unknown(algorithm: &str) -> JsErrorBox {
    JsErrorBox::type_error(format!("Unrecognized algorithm name: {algorithm}"))
}

/// The hash of some bytes, or nothing when the name is not one of the
/// four hashes web crypto has.
///
/// The names arrive as the spec spells them, uppercase and hyphenated,
/// because the prelude settles that before the crossing: a name this
/// does not know is a name nobody should have sent.
fn digest(algorithm: &str, data: &[u8]) -> Option<Vec<u8>> {
    match algorithm {
        "SHA-1" => Some(Sha1::digest(data).to_vec()),
        "SHA-256" => Some(Sha256::digest(data).to_vec()),
        "SHA-384" => Some(Sha384::digest(data).to_vec()),
        "SHA-512" => Some(Sha512::digest(data).to_vec()),
        _ => None,
    }
}

/// An HMAC over some bytes with some key, named for the hash under it.
fn signed(algorithm: &str, key: &[u8], data: &[u8]) -> Option<Vec<u8>> {
    fn mac<H>(key: &[u8], data: &[u8]) -> Vec<u8>
    where
        H: Digest + BlockSizeUser,
    {
        // `SimpleHmac` rather than `Hmac`, because it is the one that
        // takes a plain `Digest` and every hash here is one. A key
        // longer than the block is hashed first, which is the spec's
        // own rule and this does it.
        let mut mac = SimpleHmac::<H>::new_from_slice(key).expect("any length of key");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }
    match algorithm {
        "SHA-1" => Some(mac::<Sha1>(key, data)),
        "SHA-256" => Some(mac::<Sha256>(key, data)),
        "SHA-384" => Some(mac::<Sha384>(key, data)),
        "SHA-512" => Some(mac::<Sha512>(key, data)),
        _ => None,
    }
}

/// Whether a signature is the one those bytes and that key make, told
/// in a time that does not depend on where the first difference is.
fn verified(algorithm: &str, key: &[u8], data: &[u8], signature: &[u8]) -> Option<bool> {
    let held = signed(algorithm, key, data)?;
    if held.len() != signature.len() {
        return Some(false);
    }
    let mut same = 0u8;
    for (one, two) in held.iter().zip(signature) {
        same |= one ^ two;
    }
    Some(same == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::new(), |mut said, byte| {
            use std::fmt::Write;
            let _ = write!(said, "{byte:02x}");
            said
        })
    }

    #[test]
    fn the_four_hashes_are_the_four_hashes() {
        assert_eq!(
            hex(&digest("SHA-1", b"zou").expect("a hash")),
            "138c4434ce6b0de777e96966217455e122753986"
        );
        assert_eq!(
            hex(&digest("SHA-256", b"zou").expect("a hash")),
            "b20a7d254bdab4ee822c1973b2dca94197261860c5ad468b401c430a9d2c6ca4"
        );
        assert_eq!(
            hex(&digest("SHA-384", b"zou").expect("a hash")),
            "40c98c7cedcf7a474f65d0d3648bbd85128b898dd50d82354b0c7f6ee11c6c61c57a708216355db4015e9ff1a33284fd"
        );
        assert_eq!(
            hex(&digest("SHA-512", b"zou").expect("a hash")),
            "cbc2065695af4c488931ed168e8d5da7f043ce9a2502b250b48b34ab2c39d930322652bcd6940be01d12595daa624072688654ebd20f2fd96b28f708da946fb7"
        );
    }

    /// The second test case of RFC 4231, which is the one every HMAC
    /// implementation is checked against.
    #[test]
    fn an_hmac_is_the_one_the_rfc_says_it_is() {
        let said =
            signed("SHA-256", b"Jefe", b"what do ya want for nothing?").expect("a signature");
        assert_eq!(
            hex(&said),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    /// A key longer than the hash's block is hashed down to one, which
    /// is a rule an implementation can miss and still pass the short
    /// key cases.
    #[test]
    fn a_key_longer_than_the_block_is_still_a_key() {
        let key = vec![0xaa; 131];
        let said = signed(
            "SHA-256",
            &key,
            b"Test Using Larger Than Block-Size Key - Hash Key First",
        )
        .expect("a signature");
        assert_eq!(
            hex(&said),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn a_signature_is_verified_and_a_wrong_one_is_not() {
        let signature = signed("SHA-256", b"key", b"message").expect("a signature");
        assert_eq!(
            verified("SHA-256", b"key", b"message", &signature),
            Some(true)
        );
        assert_eq!(
            verified("SHA-256", b"other", b"message", &signature),
            Some(false)
        );
        assert_eq!(
            verified("SHA-256", b"key", b"other message", &signature),
            Some(false)
        );
        // A signature of the wrong length is wrong, and is not an index
        // out of bounds.
        assert_eq!(
            verified("SHA-256", b"key", b"message", b"short"),
            Some(false)
        );
    }

    #[test]
    fn a_hash_this_runtime_does_not_have_is_nothing_rather_than_a_guess() {
        for name in ["MD5", "sha-256", "SHA256", ""] {
            assert!(digest(name, b"zou").is_none(), "{name}");
            assert!(signed(name, b"key", b"zou").is_none(), "{name}");
            assert!(verified(name, b"key", b"zou", b"tag").is_none(), "{name}");
        }
    }
}
