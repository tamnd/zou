//! The order every list in the generated file comes out in.
//!
//! supabase's generator sorts with JavaScript's localeCompare, which is
//! not the order bytes are in: it compares letters first and ignores
//! case until nothing else can separate two names, and it puts
//! punctuation before digits and digits before letters. That is why
//! `post_stats` comes before `posts` and why a table named `Orders`
//! would land next to `orders` rather than before every lowercase name
//! in the schema.
//!
//! This is that order for ascii, which is what a database name is in
//! almost every case. Names with characters outside ascii are compared
//! by codepoint after all ascii, where the root collation would file an
//! accented letter next to its plain one instead.

use std::cmp::Ordering;

/// Punctuation and symbols in the order the root collation puts them,
/// which is not the order they appear in the ascii table.
const SYMBOLS: &str = " \t\n_-,;:!?.'\"()[]{}@*/\\&#%`^+<=>|~$";

/// A group and a place in it, compared in that order: symbols first,
/// then digits, then letters with their case set aside, then whatever
/// is left.
fn primary(c: char) -> (u8, u32) {
    if let Some(at) = SYMBOLS.find(c) {
        return (0, at as u32);
    }
    if c.is_ascii_digit() {
        return (1, c as u32);
    }
    if c.is_ascii_alphabetic() {
        return (2, c.to_ascii_lowercase() as u32);
    }
    (3, c as u32)
}

/// Upper case loses to lower case, but only once the letters
/// themselves have come out equal.
fn case(c: char) -> u8 {
    u8::from(c.is_uppercase())
}

pub fn locale_cmp(a: &str, b: &str) -> Ordering {
    let primaries = a.chars().map(primary).cmp(b.chars().map(primary));
    if primaries != Ordering::Equal {
        return primaries;
    }
    let cases = a.chars().map(case).cmp(b.chars().map(case));
    if cases != Ordering::Equal {
        return cases;
    }
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted(names: &[&str]) -> Vec<String> {
        let mut names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        names.sort_by(|a, b| locale_cmp(a, b));
        names
    }

    #[test]
    fn an_underscore_sorts_before_a_letter() {
        assert_eq!(sorted(&["posts", "post_stats"]), ["post_stats", "posts"]);
    }

    #[test]
    fn a_space_sorts_before_an_underscore() {
        assert_eq!(
            sorted(&["gift_cards", "gift cards"]),
            ["gift cards", "gift_cards"]
        );
    }

    #[test]
    fn punctuation_comes_before_digits_and_digits_before_letters() {
        assert_eq!(sorted(&["fa", "2fa", "_fa"]), ["_fa", "2fa", "fa"]);
    }

    #[test]
    fn digits_run_in_the_order_they_count_in() {
        assert_eq!(sorted(&["9a", "2a", "10a"]), ["10a", "2a", "9a"]);
    }

    /// Two names that differ nowhere in ascii are compared by
    /// codepoint, and everything outside ascii files after everything
    /// in it, which is where this differs from the root collation.
    #[test]
    fn anything_outside_ascii_comes_after_all_of_it() {
        assert_eq!(
            sorted(&["élan", "zebra", "apple"]),
            ["apple", "zebra", "élan"]
        );
        assert_eq!(sorted(&["ω", "é"]), ["é", "ω"]);
    }

    #[test]
    fn case_only_decides_when_the_letters_do_not() {
        assert_eq!(
            sorted(&["Orders", "orders", "people"]),
            ["orders", "Orders", "people"]
        );
        assert_eq!(sorted(&["aB", "Ab"]), ["aB", "Ab"]);
    }

    #[test]
    fn a_shorter_name_that_is_a_prefix_comes_first() {
        assert_eq!(sorted(&["users_view", "users"]), ["users", "users_view"]);
    }

    #[test]
    fn two_names_that_differ_nowhere_are_equal() {
        assert_eq!(locale_cmp("users", "users"), Ordering::Equal);
    }
}
