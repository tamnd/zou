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
//! a digest, HMAC signing and verifying, and AES in the two modes a
//! session cookie or a sealed payload is written in, CBC and GCM. Key
//! derivation and the asymmetric algorithms are not, and each is
//! refused by name in the prelude rather than being undefined.

use aes::cipher::{BlockModeDecrypt, BlockModeEncrypt, KeyIvInit, block_padding::Pkcs7};
use aes::{Aes128, Aes192, Aes256};
use aes_gcm::aead::{Aead, KeyInit as AeadKeyInit, Payload};
use aes_gcm::{AesGcm, Nonce};
use deno_core::op2;
use deno_error::JsErrorBox;
use hmac::{Mac, SimpleHmac};
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

/// The two directions of AES, in the two modes web crypto has for it.
///
/// The parameters are flat rather than a struct because they cross from
/// javascript one at a time: the mode by name, the key and the iv and
/// the additional data as bytes, and the tag length in bits, which is
/// what `AesGcmParams` calls it and is zero for CBC.
#[op2]
#[buffer]
pub fn op_zou_encrypt(
    #[string] algorithm: &str,
    #[buffer] key: &[u8],
    #[buffer] iv: &[u8],
    #[buffer] extra: &[u8],
    #[smi] tag: u32,
    #[buffer] data: &[u8],
) -> Result<Vec<u8>, JsErrorBox> {
    match algorithm {
        "AES-CBC" => cbc_encrypt(key, iv, data),
        "AES-GCM" => gcm(key, iv, tag).and_then(|gcm| gcm.encrypt(extra, data)),
        _ => Err(unknown(algorithm)),
    }
}

/// The other direction, where a wrong key is a refusal rather than
/// nonsense.
///
/// GCM says so because the tag does not check out and CBC says so
/// because the padding does not, and neither of those is a difference
/// the caller gets to see: both are `OperationError`, which is what the
/// specification calls the one thing decryption is allowed to say.
#[op2]
#[buffer]
pub fn op_zou_decrypt(
    #[string] algorithm: &str,
    #[buffer] key: &[u8],
    #[buffer] iv: &[u8],
    #[buffer] extra: &[u8],
    #[smi] tag: u32,
    #[buffer] data: &[u8],
) -> Result<Vec<u8>, JsErrorBox> {
    match algorithm {
        "AES-CBC" => cbc_decrypt(key, iv, data),
        "AES-GCM" => gcm(key, iv, tag).and_then(|gcm| gcm.decrypt(extra, data)),
        _ => Err(unknown(algorithm)),
    }
}

/// What a failed decryption says, and the whole of what it says.
///
/// The prelude turns this one sentence into the `OperationError` the
/// specification asks for, and it is one sentence rather than several
/// because which part of the ciphertext was wrong is not something the
/// party holding the wrong key should be told.
pub const FAILED: &str = "Decryption failed";

fn failed() -> JsErrorBox {
    JsErrorBox::type_error(FAILED)
}

fn unknown(algorithm: &str) -> JsErrorBox {
    JsErrorBox::type_error(format!("Unrecognized algorithm name: {algorithm}"))
}

/// The iv both modes of AES take, at the one length each of them takes
/// it at, because an iv of the wrong length is the caller's mistake and
/// not something to pad or cut.
fn iv<const N: usize>(iv: &[u8], mode: &str) -> Result<[u8; N], JsErrorBox> {
    iv.try_into().map_err(|_| {
        JsErrorBox::type_error(format!(
            "an {mode} iv is {N} bytes and this one is {}",
            iv.len()
        ))
    })
}

/// CBC over the three key lengths, with PKCS#7 padding, which is the
/// padding web crypto's AES-CBC is defined with and not a choice.
///
/// The key length picks the cipher rather than being checked against a
/// cipher somebody named, because web crypto gets the length off the
/// key: `AES-CBC` is one algorithm name for three of them.
fn cbc_encrypt(key: &[u8], nonce: &[u8], data: &[u8]) -> Result<Vec<u8>, JsErrorBox> {
    let nonce: [u8; 16] = iv(nonce, "AES-CBC")?;
    fn out<A>(key: &[u8], nonce: &[u8; 16], data: &[u8]) -> Result<Vec<u8>, JsErrorBox>
    where
        A: aes::cipher::BlockCipherEncrypt
            + aes::cipher::BlockSizeUser<BlockSize = aes::cipher::consts::U16>,
        cbc::Encryptor<A>: KeyIvInit + BlockModeEncrypt,
    {
        let mode =
            cbc::Encryptor::<A>::new_from_slices(key, nonce).map_err(|_| sized(key.len()))?;
        Ok(mode.encrypt_padded_vec::<Pkcs7>(data))
    }
    match key.len() {
        16 => out::<Aes128>(key, &nonce, data),
        24 => out::<Aes192>(key, &nonce, data),
        32 => out::<Aes256>(key, &nonce, data),
        _ => Err(sized(key.len())),
    }
}

fn cbc_decrypt(key: &[u8], nonce: &[u8], data: &[u8]) -> Result<Vec<u8>, JsErrorBox> {
    let nonce: [u8; 16] = iv(nonce, "AES-CBC")?;
    fn out<A>(key: &[u8], nonce: &[u8; 16], data: &[u8]) -> Result<Vec<u8>, JsErrorBox>
    where
        A: aes::cipher::BlockCipherDecrypt
            + aes::cipher::BlockSizeUser<BlockSize = aes::cipher::consts::U16>,
        cbc::Decryptor<A>: KeyIvInit + BlockModeDecrypt,
    {
        let mode =
            cbc::Decryptor::<A>::new_from_slices(key, nonce).map_err(|_| sized(key.len()))?;
        mode.decrypt_padded_vec::<Pkcs7>(data).map_err(|_| failed())
    }
    match key.len() {
        16 => out::<Aes128>(key, &nonce, data),
        24 => out::<Aes192>(key, &nonce, data),
        32 => out::<Aes256>(key, &nonce, data),
        _ => Err(sized(key.len())),
    }
}

fn sized(bytes: usize) -> JsErrorBox {
    JsErrorBox::type_error(format!(
        "an AES key is 128, 192 or 256 bits and this one is {}",
        bytes * 8
    ))
}

/// GCM with the key and the iv settled, ready to be handed the data and
/// whatever is authenticated beside it.
///
/// The iv is twelve bytes and the tag is the full 128 bits, which is
/// narrower than the specification allows and is what everything
/// writes. An iv of another length makes the counter out of a hash of
/// the iv rather than out of the iv, and a cut tag is a tag an attacker
/// has fewer guesses to make against, so both are refused by name here
/// rather than served in a way somebody has to go and check.
enum Gcm {
    Aes128(AesGcm<Aes128, aes_gcm::aead::consts::U12>, [u8; 12]),
    Aes192(AesGcm<Aes192, aes_gcm::aead::consts::U12>, [u8; 12]),
    Aes256(AesGcm<Aes256, aes_gcm::aead::consts::U12>, [u8; 12]),
}

fn gcm(key: &[u8], nonce: &[u8], tag: u32) -> Result<Gcm, JsErrorBox> {
    let held: [u8; 12] = iv(nonce, "AES-GCM")?;
    if tag != 0 && tag != 128 {
        return Err(JsErrorBox::type_error(format!(
            "an AES-GCM tag is 128 bits here and this one is {tag}"
        )));
    }
    let wrong = || sized(key.len());
    match key.len() {
        16 => Ok(Gcm::Aes128(
            AesGcm::new_from_slice(key).map_err(|_| wrong())?,
            held,
        )),
        24 => Ok(Gcm::Aes192(
            AesGcm::new_from_slice(key).map_err(|_| wrong())?,
            held,
        )),
        32 => Ok(Gcm::Aes256(
            AesGcm::new_from_slice(key).map_err(|_| wrong())?,
            held,
        )),
        _ => Err(wrong()),
    }
}

impl Gcm {
    /// The ciphertext with the tag on the end of it, which is where web
    /// crypto puts it and where the caller will look for it.
    fn encrypt(&self, extra: &[u8], data: &[u8]) -> Result<Vec<u8>, JsErrorBox> {
        let payload = Payload {
            msg: data,
            aad: extra,
        };
        match self {
            Gcm::Aes128(gcm, iv) => gcm.encrypt(&Nonce::from(*iv), payload),
            Gcm::Aes192(gcm, iv) => gcm.encrypt(&Nonce::from(*iv), payload),
            Gcm::Aes256(gcm, iv) => gcm.encrypt(&Nonce::from(*iv), payload),
        }
        .map_err(|_| failed())
    }

    /// The plaintext, or the one refusal.
    fn decrypt(&self, extra: &[u8], data: &[u8]) -> Result<Vec<u8>, JsErrorBox> {
        let payload = Payload {
            msg: data,
            aad: extra,
        };
        match self {
            Gcm::Aes128(gcm, iv) => gcm.decrypt(&Nonce::from(*iv), payload),
            Gcm::Aes192(gcm, iv) => gcm.decrypt(&Nonce::from(*iv), payload),
            Gcm::Aes256(gcm, iv) => gcm.decrypt(&Nonce::from(*iv), payload),
        }
        .map_err(|_| failed())
    }
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

    fn unhex(said: &str) -> Vec<u8> {
        (0..said.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&said[at..at + 2], 16).expect("two hex digits"))
            .collect()
    }

    /// The first CBC vector of NIST SP 800-38A, whose plaintext is two
    /// whole blocks, with the third block PKCS#7 adds on the end of it.
    #[test]
    fn a_cbc_ciphertext_is_the_one_the_publication_says_it_is() {
        let key = unhex("2b7e151628aed2a6abf7158809cf4f3c");
        let iv = unhex("000102030405060708090a0b0c0d0e0f");
        let plain = unhex("6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e51");
        let said = cbc_encrypt(&key, &iv, &plain).expect("a ciphertext");
        assert_eq!(
            hex(&said[..32]),
            "7649abac8119b246cee98e9b12e9197d5086cb9b507219ee95db113a917678b2"
        );
        assert_eq!(cbc_decrypt(&key, &iv, &said).expect("the plaintext"), plain);
    }

    /// The three key lengths, and the fourth thing that is not a key.
    #[test]
    fn every_aes_key_length_works_and_nothing_else_is_a_key() {
        for length in [16usize, 24, 32] {
            let key = vec![7u8; length];
            let iv = vec![9u8; 16];
            let said = cbc_encrypt(&key, &iv, b"zou").expect("a ciphertext");
            assert_eq!(
                cbc_decrypt(&key, &iv, &said).expect("the plaintext"),
                b"zou"
            );
            let iv = vec![9u8; 12];
            let said = gcm(&key, &iv, 128)
                .expect("a cipher")
                .encrypt(b"", b"zou")
                .expect("a ciphertext");
            assert_eq!(
                gcm(&key, &iv, 0)
                    .expect("a cipher")
                    .decrypt(b"", &said)
                    .expect("the plaintext"),
                b"zou"
            );
        }
        for length in [0usize, 8, 20, 64] {
            assert!(
                cbc_encrypt(&vec![7u8; length], &[9u8; 16], b"zou").is_err(),
                "{length}"
            );
            assert!(
                gcm(&vec![7u8; length], &[9u8; 12], 128).is_err(),
                "{length}"
            );
        }
    }

    /// The one thing a decryption is allowed to say, said for each of
    /// the ways it can fail: the wrong key, and in GCM the right key
    /// over bytes somebody changed.
    #[test]
    fn a_decryption_that_fails_says_the_one_sentence() {
        let iv = [9u8; 16];
        let said = cbc_encrypt(&[7u8; 32], &iv, b"zou").expect("a ciphertext");
        let out = cbc_decrypt(&[8u8; 32], &iv, &said).expect_err("a refusal");
        assert_eq!(out.to_string(), FAILED);

        let iv = [9u8; 12];
        let mut said = gcm(&[7u8; 32], &iv, 128)
            .expect("a cipher")
            .encrypt(b"", b"zou")
            .expect("a ciphertext");
        assert_eq!(
            gcm(&[8u8; 32], &iv, 128)
                .expect("a cipher")
                .decrypt(b"", &said)
                .expect_err("a refusal")
                .to_string(),
            FAILED
        );
        said[0] ^= 1;
        assert_eq!(
            gcm(&[7u8; 32], &iv, 128)
                .expect("a cipher")
                .decrypt(b"", &said)
                .expect_err("a refusal")
                .to_string(),
            FAILED
        );
    }

    /// What is authenticated beside the ciphertext is authenticated,
    /// which is the whole of what additional data is for.
    #[test]
    fn additional_data_is_part_of_what_a_gcm_tag_covers() {
        let iv = [9u8; 12];
        let said = gcm(&[7u8; 32], &iv, 128)
            .expect("a cipher")
            .encrypt(b"one", b"zou")
            .expect("a ciphertext");
        assert_eq!(
            gcm(&[7u8; 32], &iv, 128)
                .expect("a cipher")
                .decrypt(b"one", &said)
                .expect("the plaintext"),
            b"zou"
        );
        assert!(
            gcm(&[7u8; 32], &iv, 128)
                .expect("a cipher")
                .decrypt(b"two", &said)
                .is_err()
        );
    }

    /// An iv of the wrong length is the caller's mistake and is told so
    /// rather than being padded or cut into one that fits.
    #[test]
    fn an_iv_of_the_wrong_length_is_refused_by_length() {
        let said = cbc_encrypt(&[7u8; 32], &[9u8; 12], b"zou").expect_err("a refusal");
        assert_eq!(
            said.to_string(),
            "an AES-CBC iv is 16 bytes and this one is 12"
        );
        let said = gcm(&[7u8; 32], &[9u8; 16], 128).err().expect("a refusal");
        assert_eq!(
            said.to_string(),
            "an AES-GCM iv is 12 bytes and this one is 16"
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
