//! SCRAM-SHA-256, the client half.
//!
//! The postgres port opens a connection behind it with the dsn's own
//! credential, and a postmaster this node started asks for trust,
//! cleartext or md5. A deployment pointing a project at a postgres it
//! did not start hits SCRAM-SHA-256 instead, which is what every
//! cluster initialised this decade asks for, so the door has to be able
//! to answer it.
//!
//! It is arithmetic and nothing else: a nonce, PBKDF2-HMAC-SHA256 down
//! to the salted password, and two HMACs to the proof. hmac, sha2,
//! base64ct and getrandom were all already here, so nothing was added
//! to the supply chain for it.
//!
//! No channel binding. The gs2 header says `n`, which is a client that
//! does not support it rather than one that thinks the server does not,
//! so a server offering SCRAM-SHA-256-PLUS still gets an honest answer
//! and a downgrade cannot be hidden from it. That is enough while this
//! connection is on loopback or a private network, which is where it is
//! today, and channel binding is worth having the moment it crosses one.
//!
//! No SASLprep either. The password here is the one in the project's
//! dsn, which this node wrote, and normalising it would only matter for
//! a password with a non ascii space in it.

use base64ct::{Base64, Encoding};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

/// How many rounds of PBKDF2 this client will do before deciding the
/// server is not asking, it is spending its time. Postgres asks for
/// 4096 and the RFC's own floor is 4096, so a million is far enough
/// above anything real to only ever catch a server that is wrong or
/// unfriendly.
const MAX_ITERATIONS: u32 = 1_000_000;

/// One exchange, from the first message to the server's own proof.
pub struct Scram {
    /// The client first message without the gs2 header, which the
    /// auth message is built out of.
    first_bare: String,
    nonce: String,
    /// What the server's own proof is checked against, known once the
    /// client's final message has been built: the server key and the
    /// message it has to sign with it.
    expect: Option<([u8; 32], String)>,
}

impl Scram {
    /// A fresh exchange, with a nonce nobody has used before.
    pub fn new() -> Scram {
        let mut bytes = [0u8; 18];
        getrandom::fill(&mut bytes).expect("the system random source");
        Scram::with(&Base64::encode_string(&bytes))
    }

    fn with(nonce: &str) -> Scram {
        Scram {
            // The username is empty because postgres takes it from the
            // startup packet and the RFC says to leave it out when the
            // protocol carries it.
            first_bare: format!("n=,r={nonce}"),
            nonce: nonce.to_string(),
            expect: None,
        }
    }

    /// The client first message, gs2 header and all.
    pub fn first(&self) -> String {
        format!("n,,{}", self.first_bare)
    }

    /// The client final message: the proof that this side knows the
    /// password, without the password crossing.
    pub fn last(&mut self, password: &[u8], server_first: &str) -> Result<String, String> {
        let (nonce, salt, iterations) = server(server_first)?;
        if !nonce.starts_with(&self.nonce) {
            return Err("the server's nonce does not begin with the one it was sent".to_string());
        }
        let salted = pbkdf2(password, &salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();

        // biws is the base64 of the same gs2 header the first message
        // carried, which is what the server checks it against.
        let without_proof = format!("c=biws,r={nonce}");
        let auth = format!("{},{},{}", self.first_bare, server_first, without_proof);
        let signature = hmac(&stored_key, auth.as_bytes());
        let mut proof = client_key;
        for (byte, mask) in proof.iter_mut().zip(signature) {
            *byte ^= mask;
        }

        self.expect = Some((hmac(&salted, b"Server Key"), auth));
        Ok(format!(
            "{without_proof},p={}",
            Base64::encode_string(&proof)
        ))
    }

    /// The server's own proof, which is what says the thing that
    /// answered knows the password too and is not something in the
    /// middle repeating what it heard.
    pub fn done(&self, server_final: &str) -> Result<(), String> {
        let (server_key, auth) = self
            .expect
            .as_ref()
            .ok_or("the server finished before this side had asked anything")?;
        let mut verifier = None;
        for field in server_final.split(',') {
            match field.split_once('=') {
                Some(("v", value)) => verifier = Some(value),
                Some(("e", why)) => return Err(format!("the database refused the proof: {why}")),
                _ => {}
            }
        }
        let verifier = verifier.ok_or("the server's last message carries no proof")?;
        let given = Base64::decode_vec(verifier).map_err(|_| "the server's proof is not base64")?;
        // verify_slice rather than a comparison, because it is a
        // comparison against a secret and it is constant time.
        let mut mac =
            Hmac::<Sha256>::new_from_slice(server_key).expect("hmac accepts any key length");
        mac.update(auth.as_bytes());
        mac.verify_slice(&given)
            .map_err(|_| "the database's own proof is not the one this side computed".to_string())
    }
}

impl Default for Scram {
    fn default() -> Scram {
        Scram::new()
    }
}

/// The server first message, taken apart: its nonce, the salt, and how
/// many rounds it wants.
fn server(message: &str) -> Result<(String, Vec<u8>, u32), String> {
    let (mut nonce, mut salt, mut iterations) = (None, None, None);
    for field in message.split(',') {
        match field.split_once('=') {
            Some(("r", value)) => nonce = Some(value.to_string()),
            Some(("s", value)) => salt = Some(value),
            Some(("i", value)) => iterations = Some(value),
            _ => {}
        }
    }
    let nonce = nonce.ok_or("the server's first message carries no nonce")?;
    let salt = Base64::decode_vec(salt.ok_or("the server's first message carries no salt")?)
        .map_err(|_| "the server's salt is not base64")?;
    let iterations: u32 = iterations
        .ok_or("the server's first message carries no iteration count")?
        .parse()
        .map_err(|_| "the server's iteration count is not a number")?;
    if iterations == 0 || iterations > MAX_ITERATIONS {
        return Err(format!(
            "the server asked for {iterations} rounds of pbkdf2"
        ));
    }
    Ok((nonce, salt, iterations))
}

/// PBKDF2-HMAC-SHA256 down to one block, which is all SHA-256 needs
/// since the key is the size of the hash.
fn pbkdf2(password: &[u8], salt: &[u8], rounds: u32) -> [u8; 32] {
    let mut block = Vec::with_capacity(salt.len() + 4);
    block.extend_from_slice(salt);
    block.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac(password, &block);
    let mut out = u;
    for _ in 1..rounds {
        u = hmac(password, &u);
        for (byte, mask) in out.iter_mut().zip(u) {
            *byte ^= mask;
        }
    }
    out
}

fn hmac(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677's own exchange, byte for byte, which is what says the
    /// arithmetic here is the arithmetic everybody else does.
    #[test]
    fn the_rfc_vector_comes_out_of_this_the_way_it_went_in() {
        const SERVER_FIRST: &str = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let mut scram = Scram::with("rOprNGfwEbeRWgbNEkqO");
        // The vector's client names a user, which postgres does not,
        // so the auth message is built with the name in it here.
        scram.first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO".to_string();
        let last = scram
            .last(b"pencil", SERVER_FIRST)
            .expect("a final message");
        assert_eq!(
            last,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        scram
            .done("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .expect("the server's proof is the one the rfc prints");
    }

    #[test]
    fn the_first_message_says_this_client_does_not_do_channel_binding() {
        let scram = Scram::new();
        assert!(scram.first().starts_with("n,,n=,r="), "{}", scram.first());
        assert!(
            scram.first().len() > 12,
            "the nonce is not nothing: {}",
            scram.first()
        );
    }

    #[test]
    fn two_exchanges_do_not_share_a_nonce() {
        assert_ne!(Scram::new().first(), Scram::new().first());
    }

    /// A server that echoes something else as the nonce is either
    /// broken or somebody else, and either way this side stops before
    /// it sends a proof.
    #[test]
    fn a_nonce_that_does_not_begin_with_this_ones_is_refused() {
        let mut scram = Scram::with("mine");
        let err = scram
            .last(b"pencil", "r=yours,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
            .expect_err("a nonce that is not this one's");
        assert!(err.contains("nonce"), "{err}");
    }

    #[test]
    fn a_server_that_wants_more_rounds_than_anybody_asks_for_is_refused() {
        let mut scram = Scram::with("mine");
        let err = scram
            .last(b"pencil", "r=mine,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=999999999")
            .expect_err("a count that is a denial of service");
        assert!(err.contains("999999999"), "{err}");
    }

    #[test]
    fn a_proof_that_is_not_the_computed_one_is_refused() {
        const SERVER_FIRST: &str = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let mut scram = Scram::with("rOprNGfwEbeRWgbNEkqO");
        scram
            .last(b"pencil", SERVER_FIRST)
            .expect("a final message");
        let err = scram
            .done("v=AAAATRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .expect_err("a proof from something that does not know the password");
        assert!(err.contains("proof"), "{err}");
        assert!(
            scram.done("e=invalid-proof").is_err(),
            "and a server that says no is a no"
        );
    }

    /// The password never crosses, which is the whole point of the
    /// exchange and worth a test that says so.
    #[test]
    fn nothing_that_goes_out_carries_the_password() {
        const SERVER_FIRST: &str = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let mut scram = Scram::with("rOprNGfwEbeRWgbNEkqO");
        let last = scram
            .last(b"pencil", SERVER_FIRST)
            .expect("a final message");
        assert!(!scram.first().contains("pencil"));
        assert!(!last.contains("pencil"));
    }
}
