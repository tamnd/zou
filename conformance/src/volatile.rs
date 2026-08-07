//! The values nobody can predict, replaced by what they looked like.
//!
//! `diff` compares exactly and is meant to. A suite over a table keeps
//! that honest by pinning its rows down in setup.sql, so the same
//! question really does have the same answer twice. A suite over an
//! auth server has no such option: a sign in answers with a token that
//! was signed a moment ago and a session row that was made a moment
//! ago, and the only two choices are to name those values or to stop
//! comparing the answer.
//!
//! So they are named, in the case, and replaced here before the answer
//! reaches the recording or the comparison. What replaces them is not a
//! blank: it is the name of the shape the value had. A `<uuid>` that
//! comes back as a `<number>` is still a difference, and so is a token
//! with two segments where the reference sent three. What is given up
//! is the value itself, which is the only thing that could not have
//! been kept.
//!
//! Both sides are redacted by the same case, so this cannot make one
//! target look better than another: a path that is volatile is
//! volatile for the reference too.

use std::collections::BTreeMap;

use serde_json::Value;

/// What a volatile path says before the name of a header, when it means
/// a header rather than a place in the body.
///
/// The resumable protocol needs this and nothing else does. A creation
/// answers 201 with no body at all and puts the url of the upload it
/// made in `location`, and that url carries the host the request
/// arrived on and an id that was generated a moment ago. Naming it in
/// the body would name nothing, because there is no body.
const HEADER: &str = "header:";

/// What a volatile path says before the name of an xml element, when
/// the body is not json and a place in it cannot be pointed at.
///
/// The S3 surface answers xml, and a listing of buckets carries the day
/// each one was made on. Two of them were made by setup.sql and are
/// pinned there; the one the suite just made was made a moment ago. A
/// body like that is one string as far as everything else here is
/// concerned, so naming it by a path would name the whole listing and
/// give up the names, the order and the shape of the document with it.
const ELEMENT: &str = "element:";

/// Replace every value `paths` names with the name of its shape.
///
/// A path that matches nothing is left alone rather than reported. The
/// answer to a request that failed does not carry the keys the answer
/// to one that worked does, and a case that names both is a case that
/// asks one question rather than two.
pub fn redact(body: &mut Value, paths: &[String]) {
    for path in paths {
        if let Some(name) = path.strip_prefix(ELEMENT) {
            if let Value::String(text) = body {
                *text = elements(text, name.trim());
            }
            continue;
        }
        if path.starts_with(HEADER) {
            continue;
        }
        walk(body, &steps(path));
    }
}

/// What every `<name>...</name>` holds, replaced by the name of its
/// shape, with the tags and everything around them left as they are.
///
/// Read by finding the tags rather than by parsing the document,
/// because the comparison is on the bytes and a parse that rewrote them
/// would be comparing this crate's idea of the document instead. An
/// element with attributes on it is not matched, and nothing the S3
/// surface answers has any.
fn elements(text: &str, name: &str) -> String {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(&open) {
        let (before, after) = rest.split_at(at + open.len());
        out.push_str(before);
        let Some(end) = after.find(&close) else {
            rest = after;
            break;
        };
        out.push_str(&string_shape(&after[..end]));
        out.push_str(&close);
        rest = &after[end + close.len()..];
    }
    out.push_str(rest);
    out
}

/// The same, for the headers a case named instead of a place in the
/// body. Header names are compared lowercased, which is how they are
/// kept.
pub fn redact_headers(headers: &mut BTreeMap<String, String>, paths: &[String]) {
    for path in paths {
        let Some(name) = path.strip_prefix(HEADER) else {
            continue;
        };
        if let Some(value) = headers.get_mut(&name.trim().to_ascii_lowercase()) {
            *value = string_shape(value);
        }
    }
}

/// A pointer split into its steps. Written with slashes, leading one
/// optional, and `*` for every element of an array or every value of an
/// object, which is what `identities` and `amr` need.
fn steps(path: &str) -> Vec<&str> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect()
}

fn walk(value: &mut Value, steps: &[&str]) {
    let Some((step, rest)) = steps.split_first() else {
        *value = Value::String(shape(value));
        return;
    };
    match value {
        Value::Object(map) if *step == "*" => {
            for (_, child) in map.iter_mut() {
                walk(child, rest);
            }
        }
        Value::Object(map) => {
            if let Some(child) = map.get_mut(*step) {
                walk(child, rest);
            }
        }
        Value::Array(items) if *step == "*" => {
            for child in items.iter_mut() {
                walk(child, rest);
            }
        }
        Value::Array(items) => {
            if let Ok(at) = step.parse::<usize>()
                && let Some(child) = items.get_mut(at)
            {
                walk(child, rest);
            }
        }
        _ => {}
    }
}

/// What a value looked like, in as much detail as can be checked
/// without knowing what it was.
///
/// The string shapes are the things an answer carries that move: the id
/// of a row, the time it was made, a token, and a url with a token on
/// the end. Each is told apart by its own spelling rather than by the
/// key it sat under, so a uuid that arrives where a timestamp belongs
/// says so.
fn shape(value: &Value) -> String {
    match value {
        Value::Null => "<null>".to_string(),
        Value::Bool(_) => "<bool>".to_string(),
        Value::Number(_) => "<number>".to_string(),
        Value::Array(items) => format!("<array of {}>", items.len()),
        Value::Object(_) => "<object>".to_string(),
        Value::String(text) => string_shape(text),
    }
}

fn string_shape(text: &str) -> String {
    if is_uuid(text) {
        return "<uuid>".to_string();
    }
    if is_jwt(text) {
        return "<jwt>".to_string();
    }
    if is_timestamp(text) {
        return "<timestamp>".to_string();
    }
    // A signed url is a path with a token on the end, and only the
    // token moves. Naming the whole of it `<string>` would give up the
    // route it points at, the bucket, and the name, which is most of
    // what the answer was for. So the token is named and the rest is
    // compared, and a url that starts pointing somewhere else is a
    // difference again.
    if let Some((path, token)) = text.split_once("?token=")
        && is_jwt(token)
    {
        return format!("{path}?token=<jwt>");
    }
    // A url a server built out of the host the request arrived on and
    // an id it made a moment ago, which is what a resumable creation
    // answers with. The host cannot be compared at all: the reference
    // is asked on one port and zou on another, so an absolute url
    // differs on every target by construction. What is worth keeping is
    // the route in the middle, since that is what a client sends the
    // bytes to, so the host goes, the last segment is named, and
    // everything between them is compared.
    //
    // The whole string has to be the url and it has to carry nothing
    // but a path. A scheme somewhere in the middle is a scheme inside a
    // query string, which is what a link with a `redirect_to` on it
    // looks like, and taking that apart the same way would keep half a
    // query and name the other half.
    if let Some(rest) = text
        .strip_prefix("http://")
        .or_else(|| text.strip_prefix("https://"))
        && !text.contains('?')
        && let Some((_, path)) = rest.split_once('/')
        && let Some((route, last)) = path.rsplit_once('/')
    {
        return format!("/{route}/{}", string_shape(last));
    }
    // A sentence with an id in the middle of it, which is what a
    // refusal that names the thing it could not find says: "Part 7 is
    // missing for upload id 6e125931-...". The same trade as the two
    // urls above and for the same reason. The id was made a moment ago
    // and cannot be compared; the words around it are what a client
    // reads and what a compatible server owes, so they still are.
    //
    // Word by word rather than by scanning, so an id has to stand on
    // its own with spaces around it. A run of hex inside a longer token
    // is not an id that was named, it is part of a value, and a rule
    // that reached into one would be redacting halves of things nobody
    // asked about.
    if text.split(' ').any(|word| named(word).is_some()) {
        let words: Vec<String> = text
            .split(' ')
            .map(|word| named(word).unwrap_or_else(|| word.to_string()))
            .collect();
        return words.join(" ");
    }
    "<string>".to_string()
}

/// One word of a sentence with its id named, or nothing if the word is
/// not one.
///
/// What is trimmed off first is whatever cannot be part of an id, so a
/// sentence that ends in one keeps its full stop.
fn named(word: &str) -> Option<String> {
    let core = word.trim_matches(|c: char| !c.is_ascii_hexdigit() && c != '-');
    if !is_uuid(core) {
        return None;
    }
    let at = word.find(core)?;
    Some(format!("{}<uuid>{}", &word[..at], &word[at + core.len()..]))
}

/// 8-4-4-4-12 hex, which is the one spelling postgres and Go both use.
fn is_uuid(text: &str) -> bool {
    let groups = [8, 4, 4, 4, 12];
    let mut parts = text.split('-');
    for want in groups {
        match parts.next() {
            Some(part) if part.len() == want && part.bytes().all(|b| b.is_ascii_hexdigit()) => {}
            _ => return false,
        }
    }
    parts.next().is_none()
}

/// Three base64url segments, which is enough to tell a signed token
/// from a random string without verifying it. Verifying it here would
/// mean holding the secret, and a recording is compared on a machine
/// that has no reason to.
fn is_jwt(text: &str) -> bool {
    let parts: Vec<&str> = text.split('.').collect();
    parts.len() == 3
        && parts.iter().all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        })
}

/// A date and a time, in either of the two spellings the two servers
/// send: postgres writes a space and Go writes a `T`.
fn is_timestamp(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() < 19 {
        return false;
    }
    let digits = [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18];
    let dashes = [4, 7];
    let colons = [13, 16];
    digits.iter().all(|at| bytes[*at].is_ascii_digit())
        && dashes.iter().all(|at| bytes[*at] == b'-')
        && colons.iter().all(|at| bytes[*at] == b':')
        && (bytes[10] == b'T' || bytes[10] == b' ')
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn redacted(mut body: Value, paths: &[&str]) -> Value {
        let paths: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
        redact(&mut body, &paths);
        body
    }

    #[test]
    fn a_named_value_becomes_the_name_of_its_shape() {
        let body = json!({
            "access_token": "aaa.bbb.ccc",
            "user": {"id": "e7b3f6b0-1c2d-4e5f-8a9b-0c1d2e3f4a5b"},
            "token_type": "bearer"
        });
        assert_eq!(
            redacted(body, &["/access_token", "/user/id"]),
            json!({
                "access_token": "<jwt>",
                "user": {"id": "<uuid>"},
                "token_type": "bearer"
            })
        );
    }

    /// The point of naming the shape rather than blanking the value.
    #[test]
    fn a_value_of_the_wrong_shape_is_still_a_difference() {
        let want = redacted(
            json!({"id": "e7b3f6b0-1c2d-4e5f-8a9b-0c1d2e3f4a5b"}),
            &["/id"],
        );
        let got = redacted(json!({"id": 7}), &["/id"]);
        assert_ne!(want, got);
        assert_eq!(got["id"], "<number>");
    }

    #[test]
    fn a_star_reaches_every_element_of_a_list() {
        let body = json!({"identities": [
            {"id": "1", "created_at": "2026-08-06T04:18:47Z"},
            {"id": "2", "created_at": "2026-08-06 04:18:47+00"}
        ]});
        assert_eq!(
            redacted(body, &["/identities/*/created_at"]),
            json!({"identities": [
                {"id": "1", "created_at": "<timestamp>"},
                {"id": "2", "created_at": "<timestamp>"}
            ]})
        );
    }

    #[test]
    fn a_star_reaches_every_value_of_an_object_too() {
        let body = json!({"claims": {"session_id": "a", "sub": "b"}});
        assert_eq!(
            redacted(body, &["/claims/*"]),
            json!({"claims": {"session_id": "<string>", "sub": "<string>"}})
        );
    }

    /// An error body does not carry the keys a success body does, and a
    /// case that names both is one case rather than two.
    #[test]
    fn a_path_that_matches_nothing_changes_nothing() {
        let body = json!({"msg": "no"});
        assert_eq!(redacted(body.clone(), &["/access_token"]), body);
    }

    #[test]
    fn a_whole_list_can_be_named_by_its_length() {
        let body = json!({"factors": [{"id": "a"}, {"id": "b"}]});
        assert_eq!(
            redacted(body, &["/factors"]),
            json!({"factors": "<array of 2>"})
        );
    }

    #[test]
    fn the_spellings_are_told_apart() {
        assert_eq!(
            string_shape("e7b3f6b0-1c2d-4e5f-8a9b-0c1d2e3f4a5b"),
            "<uuid>"
        );
        assert_eq!(string_shape("e7b3f6b01c2d4e5f8a9b0c1d2e3f4a5b"), "<string>");
        assert_eq!(string_shape("aaa.bbb.ccc"), "<jwt>");
        assert_eq!(string_shape("aaa.bbb"), "<string>");
        assert_eq!(string_shape("2026-08-06T04:18:47Z"), "<timestamp>");
        assert_eq!(string_shape("2026-08-06 04:18:47.123+00"), "<timestamp>");
        assert_eq!(string_shape("2026-08-06"), "<string>");
        assert_eq!(string_shape("bearer"), "<string>");
    }

    /// The path a signed url points at is not volatile and is most of
    /// what the answer said.
    #[test]
    fn a_signed_url_keeps_everything_but_its_token() {
        assert_eq!(
            string_shape("/object/sign/notes/hello.txt?token=aaa.bbb.ccc"),
            "/object/sign/notes/hello.txt?token=<jwt>"
        );
        // Not a token, so not a signed url, so nothing to keep.
        assert_eq!(
            string_shape("/object/sign/notes/hello.txt?token="),
            "<string>"
        );
    }

    /// The host an absolute url was built out of differs between
    /// targets by construction, and the id on the end of it differs
    /// between runs. The route is neither.
    #[test]
    fn a_url_keeps_the_route_and_gives_up_the_host_and_the_id() {
        assert_eq!(
            string_shape("http://127.0.0.1:54321/storage/v1/upload/resumable/bm90ZXM"),
            "/storage/v1/upload/resumable/<string>"
        );
        assert_eq!(
            string_shape("https://project.supabase.co/storage/v1/upload/resumable/bm90ZXM"),
            "/storage/v1/upload/resumable/<string>"
        );
    }

    /// A link with a `redirect_to` on it carries a second url inside
    /// its query, and the whole thing is one value that moves. Reading
    /// the scheme in the middle as the start of the url would keep the
    /// token, which is the part that moves most.
    #[test]
    fn a_scheme_inside_a_query_is_not_the_start_of_a_url() {
        assert_eq!(
            string_shape("/auth/v1/verify?token=ad7fff13&type=magiclink&redirect_to=http://a/b"),
            "<string>"
        );
        assert_eq!(
            string_shape("http://h/auth/v1/verify?token=ad7fff13&redirect_to=http://a/b"),
            "<string>"
        );
    }

    #[test]
    fn a_header_a_case_named_is_named_the_same_way_a_field_is() {
        let mut headers = BTreeMap::new();
        headers.insert("location".to_string(), "http://h/upload/x".to_string());
        headers.insert("tus-resumable".to_string(), "1.0.0".to_string());
        redact_headers(
            &mut headers,
            &["header:location".to_string(), "/id".to_string()],
        );
        assert_eq!(headers["location"], "/upload/<string>");
        assert_eq!(headers["tus-resumable"], "1.0.0", "not named, not touched");
    }

    /// The two live in one list, so each has to leave the other alone.
    #[test]
    fn a_header_path_is_not_read_as_a_path_into_the_body() {
        let body = json!({"header:location": "keep me"});
        assert_eq!(redacted(body.clone(), &["header:location"]), body);
    }

    /// The names and the order of a listing are what the case was for,
    /// and only the day one of them was made on moves.
    #[test]
    fn an_element_of_an_xml_body_is_named_and_the_document_is_not() {
        let body = Value::String(
            "<Buckets><Bucket><Name>photos</Name>\
             <CreationDate>2024-01-02T03:04:05.000Z</CreationDate></Bucket>\
             <Bucket><Name>made</Name>\
             <CreationDate>2026-08-07T03:49:38.782Z</CreationDate></Bucket></Buckets>"
                .to_string(),
        );
        assert_eq!(
            redacted(body, &["element:CreationDate"]),
            Value::String(
                "<Buckets><Bucket><Name>photos</Name>\
                 <CreationDate><timestamp></CreationDate></Bucket>\
                 <Bucket><Name>made</Name>\
                 <CreationDate><timestamp></CreationDate></Bucket></Buckets>"
                    .to_string()
            )
        );
    }

    /// A tag whose name merely starts the same way is a different tag,
    /// and an element nothing closed is left where it was rather than
    /// swallowing the rest of the document.
    #[test]
    fn an_element_is_matched_whole_and_a_broken_one_is_left_alone() {
        let body = Value::String("<Buckets><Bucket>a</Bucket></Buckets>".to_string());
        assert_eq!(
            redacted(body.clone(), &["element:Bucket"]),
            Value::String("<Buckets><Bucket><string></Bucket></Buckets>".to_string())
        );
        assert_eq!(redacted(body.clone(), &["element:Name"]), body);
        assert_eq!(
            redacted(Value::String("<Name>a".to_string()), &["element:Name"]),
            Value::String("<Name>a".to_string())
        );
    }

    /// A json body has places that can be pointed at, so an element
    /// path has nothing to say about one.
    #[test]
    fn an_element_path_leaves_a_json_body_alone() {
        let body = json!({"CreationDate": "2026-08-07T03:49:38.782Z"});
        assert_eq!(redacted(body.clone(), &["element:CreationDate"]), body);
    }

    /// A refusal that names the row it could not find is worth
    /// comparing except for the name, which is a value the server made
    /// up a moment ago.
    #[test]
    fn a_sentence_keeps_its_words_and_gives_up_its_ids() {
        let body = Value::String(
            "<Message>Part 7 is missing for upload id 6e125931-f378-4e33-90e0-dfe3700e4c65</Message>"
                .to_string(),
        );
        assert_eq!(
            redacted(body, &["element:Message"]),
            Value::String("<Message>Part 7 is missing for upload id <uuid></Message>".to_string())
        );
    }

    /// The id has to be a word of its own. Half of a longer token is
    /// not something anybody named.
    #[test]
    fn a_sentence_with_no_id_in_it_is_a_string() {
        assert_eq!(string_shape("Object not found"), "<string>");
        assert_eq!(
            string_shape("id 6e125931-f378-4e33-90e0-dfe3700e4c65ff"),
            "<string>"
        );
        assert_eq!(
            string_shape("try 6e125931-f378-4e33-90e0-dfe3700e4c65."),
            "try <uuid>."
        );
    }

    /// An index reaches one element, which is what an answer with a
    /// list of exactly one thing in it wants.
    #[test]
    fn an_index_reaches_the_element_it_names() {
        let body = json!([{"id": "a"}, {"id": "b"}]);
        assert_eq!(
            redacted(body, &["/1/id"]),
            json!([{"id": "a"}, {"id": "<string>"}])
        );
    }
}
