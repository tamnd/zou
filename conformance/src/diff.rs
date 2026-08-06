//! What counts as the same answer.
//!
//! Three outcomes rather than two. A body that is byte identical and a
//! body that is the same json written differently are both usable by a
//! client, but they are not the same thing to somebody whose test
//! suite compares strings, so the second is a pass that says so rather
//! than a pass that hides it.
//!
//! Status and headers are compared exactly. There is no normalizing of
//! ids or timestamps anywhere in here, and there is not meant to be: a
//! case whose answer moves is a case that should be pinned down in
//! setup.sql instead.

use crate::suite::Answer;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Same,
    /// The same json, different bytes.
    Written,
    Different,
}

#[derive(Clone, Debug)]
pub struct Difference {
    pub verdict: Verdict,
    /// One line per thing that differs, in the order they were
    /// compared, so the first line is the most structural.
    pub lines: Vec<String>,
}

/// `expected` is the reference, `found` is what is being judged.
pub fn compare(expected: &Answer, found: &Answer) -> Difference {
    let mut lines = Vec::new();
    if expected.status != found.status {
        lines.push(format!("status {} not {}", found.status, expected.status));
    }
    for name in header_names(expected, found) {
        let want = expected.headers.get(&name);
        let got = found.headers.get(&name);
        if want != got {
            lines.push(format!(
                "{name} {} not {}",
                shown(got.map(String::as_str)),
                shown(want.map(String::as_str))
            ));
        }
    }
    if expected.body != found.body {
        lines.push(format!(
            "body {} not {}",
            snippet(&found.body),
            snippet(&expected.body)
        ));
    }
    // A parsed body compares 1200.50 and 1200.5 equal, because a double
    // cannot tell them apart. A client reading a price can, so the
    // literals are compared as they were written. Sorted, because two
    // bodies with their keys in a different order are the same body and
    // this must not be the thing that says otherwise.
    let want = numbers(&expected.text());
    let got = numbers(&found.text());
    if want != got {
        lines.push(format!(
            "numbers written as {} not {}",
            got.join(","),
            want.join(",")
        ));
    }
    if !lines.is_empty() {
        return Difference {
            verdict: Verdict::Different,
            lines,
        };
    }
    // Same status, same headers, same json. The bytes are all that is
    // left, and they are worth a line but not a failure.
    //
    // Through text() rather than off raw, because raw is only kept when
    // it differs from re-serializing the body and a side that has none
    // is a side whose bytes were the re-serialization. Saying "absent"
    // there would name the recording's storage instead of what went
    // over the wire.
    let want = expected.text();
    let got = found.text();
    if want != got {
        return Difference {
            verdict: Verdict::Written,
            lines: vec![format!(
                "the same json, written as {:?} not {:?}",
                cut(&got),
                cut(&want)
            )],
        };
    }
    Difference {
        verdict: Verdict::Same,
        lines: Vec::new(),
    }
}

/// Every number in a body, as it was written, in sorted order.
///
/// Numbers only appear outside strings, and outside a string the only
/// thing a digit or a minus can start is a number, so the scan is
/// nothing more than knowing whether it is inside a string.
fn numbers(text: &str) -> Vec<String> {
    let mut numbers = Vec::new();
    let mut chars = text.chars().peekable();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        if in_string {
            match c {
                '\\' => {
                    chars.next();
                }
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '-' | '0'..='9' => {
                let mut number = String::from(c);
                while let Some(c) = chars.peek() {
                    match c {
                        '0'..='9' | '.' | 'e' | 'E' | '+' | '-' => number.push(*c),
                        _ => break,
                    }
                    chars.next();
                }
                numbers.push(number);
            }
            _ => {}
        }
    }
    numbers.sort();
    numbers
}

fn header_names(expected: &Answer, found: &Answer) -> Vec<String> {
    let mut names: Vec<String> = expected.headers.keys().cloned().collect();
    for name in found.headers.keys() {
        if !names.contains(name) {
            names.push(name.clone());
        }
    }
    names.sort();
    names
}

fn shown(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("{value:?}"),
        None => "absent".to_string(),
    }
}

/// Long bodies are cut down, because a report is read in a terminal and
/// a five hundred row difference is still a difference at eighty
/// characters.
fn snippet(value: &serde_json::Value) -> String {
    cut(&serde_json::to_string(value).unwrap_or_else(|_| "?".to_string()))
}

fn cut(text: &str) -> String {
    match text.char_indices().nth(200) {
        Some((at, _)) => format!("{}...", &text[..at]),
        None => text.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn answer(status: u16, headers: &[(&str, &str)], body: serde_json::Value) -> Answer {
        Answer {
            name: "n".to_string(),
            status,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<String, String>>(),
            body,
            raw: None,
        }
    }

    #[test]
    fn the_same_answer_is_the_same_answer() {
        let a = answer(200, &[("content-type", "application/json")], json());
        let b = answer(200, &[("content-type", "application/json")], json());
        assert_eq!(compare(&a, &b).verdict, Verdict::Same);
    }

    fn json() -> serde_json::Value {
        serde_json::json!([{"id": 1, "title": "walk"}])
    }

    #[test]
    fn a_different_status_is_the_first_thing_said() {
        let a = answer(200, &[], json());
        let b = answer(404, &[], json());
        let difference = compare(&a, &b);
        assert_eq!(difference.verdict, Verdict::Different);
        assert_eq!(difference.lines[0], "status 404 not 200");
    }

    #[test]
    fn a_header_the_other_one_does_not_have_is_a_difference() {
        let a = answer(200, &[("content-range", "0-1/*")], json());
        let b = answer(200, &[], json());
        let difference = compare(&a, &b);
        assert_eq!(difference.verdict, Verdict::Different);
        assert_eq!(difference.lines[0], "content-range absent not \"0-1/*\"");
    }

    #[test]
    fn an_extra_header_is_a_difference_too() {
        let a = answer(200, &[], json());
        let b = answer(200, &[("content-range", "0-1/*")], json());
        assert_eq!(compare(&a, &b).verdict, Verdict::Different);
    }

    #[test]
    fn a_different_body_is_a_difference() {
        let a = answer(200, &[], serde_json::json!([{"id": 1}]));
        let b = answer(200, &[], serde_json::json!([{"id": 2}]));
        let difference = compare(&a, &b);
        assert_eq!(difference.verdict, Verdict::Different);
        assert!(difference.lines[0].starts_with("body [{\"id\":2}] not"));
    }

    /// The whole reason there are three outcomes.
    #[test]
    fn the_same_json_written_differently_passes_and_says_so() {
        let a = answer(200, &[], json());
        let mut b = answer(200, &[], json());
        b.raw = Some("[ {\"id\": 1, \"title\": \"walk\"} ]".to_string());
        let difference = compare(&a, &b);
        assert_eq!(difference.verdict, Verdict::Written);
        assert_eq!(difference.lines.len(), 1);
    }

    /// A side with no raw is a side whose bytes were the
    /// re-serialization, so the line has to say what it sent rather
    /// than say that nothing was kept for it.
    #[test]
    fn a_side_that_kept_no_bytes_still_says_what_it_wrote() {
        let a = answer(200, &[], json());
        let mut b = answer(200, &[], json());
        b.raw = Some("[ {\"id\": 1, \"title\": \"walk\"} ]".to_string());
        let line = &compare(&b, &a).lines[0];
        assert!(line.contains(r#"written as "[{\"id\":1"#), "{line}");
        assert!(!line.contains("absent"), "{line}");
    }

    /// numeric is not a double. A column holding 1200.50 that comes
    /// back as 1200.5 is a price that lost its pennies, and the parsed
    /// bodies cannot tell the difference, so the literals are compared.
    #[test]
    fn a_number_that_lost_its_trailing_zero_is_a_difference() {
        let mut a = answer(200, &[], serde_json::json!([{"budget": 1200.5}]));
        a.raw = Some(r#"[{"budget":1200.50}]"#.to_string());
        let mut b = answer(200, &[], serde_json::json!([{"budget": 1200.5}]));
        b.raw = Some(r#"[{"budget":1200.5}]"#.to_string());
        let difference = compare(&a, &b);
        assert_eq!(difference.verdict, Verdict::Different);
        assert_eq!(difference.lines[0], "numbers written as 1200.5 not 1200.50");
    }

    /// The same numbers in a different place is the same body, and key
    /// order is already allowed to differ, so the comparison must not
    /// be the one thing that turns it back into a failure.
    #[test]
    fn the_same_numbers_in_another_order_are_the_same_numbers() {
        let mut a = answer(200, &[], serde_json::json!([{"a": 1, "b": 2}]));
        a.raw = Some(r#"[{"a":1,"b":2}]"#.to_string());
        let mut b = answer(200, &[], serde_json::json!([{"a": 1, "b": 2}]));
        b.raw = Some(r#"[{"b":2,"a":1}]"#.to_string());
        assert_eq!(compare(&a, &b).verdict, Verdict::Written);
    }

    #[test]
    fn digits_inside_a_string_are_not_numbers() {
        assert_eq!(numbers(r#"{"a":"12","b":3}"#), ["3"]);
        assert_eq!(numbers(r#"{"a":"say \"7\"","b":-1.5e3}"#), ["-1.5e3"]);
        assert!(numbers("[]").is_empty());
    }

    #[test]
    fn a_long_body_is_cut_down_rather_than_printed_whole() {
        let long: Vec<serde_json::Value> = (0..200).map(|n| serde_json::json!({"id": n})).collect();
        let a = answer(200, &[], serde_json::json!([]));
        let b = answer(200, &[], serde_json::Value::Array(long));
        let difference = compare(&a, &b);
        assert!(difference.lines[0].len() < 300, "{}", difference.lines[0]);
        assert!(difference.lines[0].contains("..."));
    }
}
