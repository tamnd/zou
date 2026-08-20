//! Reading a value somebody set, and saying so when it cannot be read.
//!
//! The failure this module exists to remove is not a bad message, it is
//! no message. A setting read as
//!
//! ```ignore
//! std::env::var("ZOU_WARM_BLOCKS").ok().and_then(|v| v.parse().ok()).unwrap_or(65536)
//! ```
//!
//! turns `ZOU_WARM_BLOCKS=64k` into the default, in silence. The person
//! turned the knob, the knob does nothing, and there is no way to learn
//! that from the outside. A terse error would at least have said the
//! value was refused.
//!
//! So every reader here says three things when it refuses a value: what
//! was set, what would have been accepted, and what is being used
//! instead. The last one is the part that is easy to leave out and the
//! part that tells the reader whether they are looking at their tuning
//! or at the built in default.
//!
//! These warn and carry on rather than failing. That is deliberate and
//! it is about where they are read. A flag on a command line is refused
//! before anything has started and should stop the run. These are read
//! from inside a running postgres, and taking a database down over a
//! typo in a tuning knob is a worse outcome for the person than running
//! with the default and telling them about it.

use std::fmt::Display;
use std::str::FromStr;

/// What a person is told when the value they set is not usable. Three
/// clauses, in the order a reader needs them: what they set, why it was
/// not taken, and what is being used in its place.
fn ignored(name: &str, raw: &str, shape: &str, instead: &str) -> String {
    format!("{name} is set to {raw:?}, which is not {shape}, so it is being ignored and {instead}")
}

/// The raw text of a setting, or None if it is unset or empty.
///
/// Empty counts as unset and says nothing, because `ZOU_X=` is how a
/// shell script turns a setting off when it does not know whether it is
/// on, and warning about it would make the quiet path noisy.
fn raw(name: &str) -> Option<String> {
    let value = std::env::var(name).ok()?;
    let trimmed = value.trim();
    match trimmed.is_empty() {
        true => None,
        false => Some(trimmed.to_string()),
    }
}

/// A number, or None when the setting is unset or unreadable. `shape`
/// completes "which is not ...", so write it as a noun phrase: "a whole
/// number of megabytes", "a number of milliseconds".
pub fn number<T: FromStr>(name: &str, shape: &str) -> Option<T> {
    let raw = raw(name)?;
    match raw.parse() {
        Ok(value) => Some(value),
        Err(_) => {
            log::warn!(
                "{}",
                ignored(
                    name,
                    &raw,
                    shape,
                    "the built in default is being used instead"
                )
            );
            None
        }
    }
}

/// The same, for a caller that has a default to name. Naming it is the
/// whole point: a reader who sees the number zou fell back to can tell
/// at a glance how far their intent was from what is running.
pub fn number_or<T: FromStr + Display>(name: &str, shape: &str, default: T) -> T {
    let Some(raw) = raw(name) else { return default };
    match raw.parse() {
        Ok(value) => value,
        Err(_) => {
            log::warn!(
                "{}",
                ignored(
                    name,
                    &raw,
                    shape,
                    &format!("the default of {default} is being used")
                )
            );
            default
        }
    }
}

/// One of a short list of words, or None when the setting is unset or
/// is not on the list. Compared with case and surrounding space
/// ignored, so `NORMAL` is `normal`, which widens what is accepted and
/// cannot break a value that already worked.
///
/// The returned word is the one from `accepted`, so a caller can match
/// on the spelling it wrote rather than on whatever the person typed.
pub fn word(name: &str, accepted: &[&'static str]) -> Option<&'static str> {
    let raw = raw(name)?;
    if let Some(hit) = accepted.iter().find(|a| a.eq_ignore_ascii_case(&raw)) {
        return Some(hit);
    }
    log::warn!(
        "{}",
        ignored(
            name,
            &raw,
            &format!("one of {}", accepted.join(" ")),
            "the built in default is being used instead"
        )
    );
    None
}

/// On or off, or None when the setting is unset or is neither.
///
/// The list is long on purpose. A setting that takes only `1` turns a
/// person who writes `true` into somebody whose flag quietly does
/// nothing, and there is no reading of `true` where they meant off.
///
/// It is also a superset of what Go's `strconv.ParseBool` takes, which
/// is why `t` and `f` are here. The auth settings carry GoTrue's names
/// and anything written against GoTrue may well have been using them.
pub fn flag(name: &str) -> Option<bool> {
    let raw = raw(name)?;
    match raw.to_ascii_lowercase().as_str() {
        "1" | "t" | "true" | "on" | "yes" => Some(true),
        "0" | "f" | "false" | "off" | "no" => Some(false),
        _ => {
            log::warn!(
                "{}",
                ignored(
                    name,
                    &raw,
                    "one of 1 0 t f true false on off yes no",
                    "the built in default is being used instead"
                )
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three clauses, in one message, on the reader that is most
    /// likely to be hit: a number with a unit suffix that does not
    /// parse. Asserting the text rather than the return value is the
    /// point, since the return value was already right before this
    /// module existed and the message is what was missing.
    #[test]
    fn a_refused_number_says_what_was_set_what_was_wanted_and_what_ran() {
        let said = ignored(
            "ZOU_WARM_BLOCKS",
            "64k",
            "a whole number of blocks",
            "the default of 65536 is being used",
        );
        assert!(said.contains("ZOU_WARM_BLOCKS"), "{said}");
        assert!(said.contains("\"64k\""), "{said}");
        assert!(said.contains("a whole number of blocks"), "{said}");
        assert!(said.contains("65536"), "{said}");
    }

    /// A word off the list has to be told the list. Without this the
    /// person is left guessing at a synonym, which is how `normal` and
    /// `NORMAL` became different settings in the first place.
    #[test]
    fn a_word_off_the_list_is_told_the_list() {
        let said = ignored(
            "ZOU_SQLITE_SYNC",
            "fsync",
            &format!("one of {}", ["full", "normal"].join(" ")),
            "the built in default is being used instead",
        );
        assert!(said.contains("full normal"), "{said}");
    }

    /// Case and surrounding space are not a reason to refuse a value.
    /// These would each have been silently ignored before.
    #[test]
    fn a_word_is_matched_without_case_and_a_flag_takes_the_usual_synonyms() {
        // Safe here: these tests only read what they just set, and the
        // names are unique to this test file.
        unsafe {
            std::env::set_var("ZOU_TEST_SYNC", " NORMAL ");
            std::env::set_var("ZOU_TEST_STEAL", "TRUE");
            std::env::set_var("ZOU_TEST_EMPTY", "  ");
            std::env::set_var("ZOU_TEST_JUNK", "later");
        }
        assert_eq!(word("ZOU_TEST_SYNC", &["full", "normal"]), Some("normal"));
        assert_eq!(flag("ZOU_TEST_STEAL"), Some(true));
        assert_eq!(flag("ZOU_TEST_EMPTY"), None);
        assert_eq!(flag("ZOU_TEST_JUNK"), None);
        assert_eq!(number_or("ZOU_TEST_JUNK", "a number of seconds", 30u64), 30);
        assert_eq!(number::<u64>("ZOU_TEST_MISSING", "a number"), None);
        unsafe {
            std::env::remove_var("ZOU_TEST_SYNC");
            std::env::remove_var("ZOU_TEST_STEAL");
            std::env::remove_var("ZOU_TEST_EMPTY");
            std::env::remove_var("ZOU_TEST_JUNK");
        }
    }
}
