//! Password hashing and the strength rules, GoTrue's both.
//!
//! bcrypt at cost 10, which is what GoTrue writes, so a project moving
//! its rows from hosted Supabase to zou or back keeps every password: a
//! hash written by either side verifies on the other. The version
//! marker is written as $2a$ for the same reason, which is the format
//! Go's bcrypt produces.
//!
//! Hashing at cost 10 is deliberately expensive, tens of milliseconds
//! of pure cpu, so both entry points are meant to be called from a
//! blocking thread rather than on a runtime worker.

/// Go's bcrypt.DefaultCost, which is what GoTrue hashes at.
const COST: u32 = 10;

/// bcrypt only reads the first 72 bytes of a password, so GoTrue
/// refuses anything longer instead of silently ignoring the tail.
pub const MAX_LENGTH: usize = 72;

/// GoTrue's PASSWORD_MIN_LENGTH default.
pub const MIN_LENGTH: usize = 6;

/// Hash a password the way GoTrue does. The salt is drawn here rather
/// than by the bcrypt crate so every random value in this server comes
/// from the same place.
pub fn hash(password: &str) -> String {
    let mut salt = [0u8; 16];
    getrandom::fill(&mut salt).expect("the os rng never fails");
    bcrypt::hash_with_salt(password, COST, salt)
        .expect("cost 10 is in range")
        .format_for_version(bcrypt::Version::TwoA)
}

/// Whether a password matches a stored hash. A hash this end cannot
/// parse is not a match: an argon2 or firebase scrypt hash imported
/// from elsewhere lands here, and letting it through would be worse
/// than refusing a login that a later release will serve.
pub fn matches(password: &str, hash: &str) -> bool {
    bcrypt::verify(password, hash).unwrap_or(false)
}

/// Why a password was refused. `message` is GoTrue's own wording and
/// `reasons` is the machine readable list that rides along in the
/// weak_password field of the error body.
#[derive(Debug)]
pub struct Weak {
    pub message: String,
    pub reasons: Vec<&'static str>,
}

/// GoTrue's password strength check, minus the HaveIBeenPwned lookup
/// and the required character sets, which are config this end does not
/// take yet. Over the bcrypt limit is a plain validation failure rather
/// than a weak password, because it is not weak, it is unusable.
pub fn strength(password: &str) -> Result<(), Weak> {
    if password.len() < MIN_LENGTH {
        return Err(Weak {
            message: format!("Password should be at least {MIN_LENGTH} characters."),
            reasons: vec!["length"],
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hash_verifies_and_a_wrong_password_does_not() {
        let hash = hash("correct horse");
        assert!(matches("correct horse", &hash));
        assert!(!matches("correct horse ", &hash));
        assert!(!matches("", &hash));
    }

    #[test]
    fn the_format_is_the_one_gotrue_writes() {
        let hash = hash("correct horse");
        assert!(hash.starts_with("$2a$10$"), "not gotrue's format: {hash}");
        assert_eq!(hash.len(), 60);
        assert_ne!(hash, super::hash("correct horse"), "the salt is fresh");
    }

    #[test]
    fn a_hash_written_by_go_verifies_here() {
        // Produced by golang.org/x/crypto/bcrypt at DefaultCost, which
        // is the library and the cost GoTrue hashes with. A project
        // whose rows came from hosted Supabase logs in on hashes that
        // look exactly like this one, so this end has to accept it.
        let go = "$2a$10$9mSfsZp2ozwmHOV6.8fl.OtVThXLxCXzN7X26Qou1r28iLAb3odY.";
        assert!(matches("correct horse", go));
        assert!(!matches("correct horses", go));
    }

    #[test]
    fn a_hash_this_end_cannot_parse_is_not_a_match() {
        assert!(!matches(
            "anything",
            "$argon2id$v=19$m=65536,t=2,p=1$c2FsdA$aGFzaA"
        ));
        assert!(!matches("anything", ""));
        assert!(!matches("anything", "x"));
    }

    #[test]
    fn short_passwords_are_weak_and_carry_the_reason() {
        let weak = strength("12345").expect_err("five is short");
        assert_eq!(weak.message, "Password should be at least 6 characters.");
        assert_eq!(weak.reasons, vec!["length"]);
        assert!(strength("123456").is_ok());
    }
}
