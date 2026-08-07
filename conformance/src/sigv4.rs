//! Signing a request the way an S3 client signs one.
//!
//! The S3 surface is the one part of Supabase that is not asked with a
//! JWT. A client sends a signature over the request itself, computed
//! from a key pair, and the server recomputes it and compares. So a
//! case aimed at `/storage/v1/s3` cannot be a case with a token on it:
//! the harness has to be the client, and this is that.
//!
//! Written here rather than taken from zou, on purpose. A conformance
//! harness that signed with the server's own code would agree with the
//! server about anything both of them got wrong, which is the one thing
//! it exists to catch. This is the algorithm from the specification,
//! and the reference is what says whether it was read correctly.
//!
//! Only the header flavour, which is what every client sends by
//! default. A presigned url puts the same fields in the query string
//! and is a separate question the suite asks separately.

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

/// The region a Supabase project's S3 endpoint is asked in. Local
/// projects answer to this one and the hosted ones are asked in their
/// own, which is a case rather than a constant.
pub const REGION: &str = "us-east-1";

/// Where a local Supabase project says it is.
///
/// The answer to GetBucketLocation, and the second of the two regions a
/// signature may be computed in: the endpoint takes one made here and
/// one made in [`REGION`], which is what a client that never asked
/// where it was signs in, and refuses a third. All three are recorded.
/// The target zou is started with is put in this region so that it is
/// answering the same question the reference was.
pub const PROJECT: &str = "local";

/// The signature version, which is in three places in every request and
/// is worth having one name for.
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// The pair a request is signed with.
#[derive(Clone, Debug)]
pub struct Credentials {
    pub access: String,
    pub secret: String,
}

/// What one signed request needs to know about itself. The headers are
/// every header that will be sent and that starts with `x-amz-`, plus
/// host, which is the set a client signs and the set a server expects
/// to be told about in `SignedHeaders`.
pub struct Request<'a> {
    pub method: &'a str,
    /// Path only, already percent encoded, no query string.
    pub path: &'a str,
    /// The query string without its `?`, empty when there is none.
    pub query: &'a str,
    /// Lowercased names, sorted, with `host` and the `x-amz-` headers
    /// in it. Values are compared as they are sent. Host is in here
    /// rather than beside them because the signature does not treat it
    /// specially: it is one of the headers a client chose to sign.
    pub headers: Vec<(String, String)>,
    /// Hex sha256 of the body, or `UNSIGNED-PAYLOAD` for a client that
    /// declines to hash what it is about to stream.
    pub payload: &'a str,
    /// `20260807T032800Z`, which is also what `x-amz-date` carries.
    pub stamp: &'a str,
    pub region: &'a str,
}

/// The value of the `authorization` header, ready to send.
pub fn authorization(request: &Request, credentials: &Credentials) -> String {
    let day = &request.stamp[..8];
    let scope = format!("{day}/{}/s3/aws4_request", request.region);
    let signed: Vec<&str> = request.headers.iter().map(|(n, _)| n.as_str()).collect();
    let signed = signed.join(";");
    let canonical = canonical_request(request, &signed);
    let to_sign = format!(
        "{ALGORITHM}\n{}\n{scope}\n{}",
        request.stamp,
        hex(&sha256(canonical.as_bytes()))
    );
    let signature = hex(&sign(
        &signing_key(credentials, day, request.region),
        &to_sign,
    ));
    format!(
        "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
        credentials.access
    )
}

/// The request as the signature sees it. Every field of it is compared
/// by the server against the same field rebuilt from what arrived, so
/// a difference anywhere here is a 403 rather than a bad request.
fn canonical_request(request: &Request, signed: &str) -> String {
    let mut canonical = String::new();
    canonical.push_str(request.method);
    canonical.push('\n');
    canonical.push_str(request.path);
    canonical.push('\n');
    canonical.push_str(&canonical_query(request.query));
    canonical.push('\n');
    for (name, value) in &request.headers {
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(value.trim());
        canonical.push('\n');
    }
    canonical.push('\n');
    canonical.push_str(signed);
    canonical.push('\n');
    canonical.push_str(request.payload);
    canonical
}

/// Sorted by name, and a parameter with no value keeps its `=`. The
/// sort is on the encoded name, which for everything a suite asks is
/// the name itself.
fn canonical_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((name, value)) => (name.to_string(), value.to_string()),
            None => (pair.to_string(), String::new()),
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The four nested HMACs, which is the whole reason a leaked signature
/// is not a leaked key: each one throws away what it was given.
fn signing_key(credentials: &Credentials, day: &str, region: &str) -> Vec<u8> {
    let start = format!("AWS4{}", credentials.secret);
    let key = sign(start.as_bytes(), day);
    let key = sign(&key, region);
    let key = sign(&key, "s3");
    sign(&key, "aws4_request")
}

fn sign(key: &[u8], message: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac takes any key length");
    mac.update(message.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

pub fn sha256(bytes: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().to_vec()
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `20260807T032800Z` from a unix second, which is the only format any
/// of this is written in.
///
/// Written out rather than taken from a date library, because the one
/// thing it has to do is agree with the server about which day it is
/// and the only input is a count of seconds. Leap seconds do not exist
/// in unix time and there are no time zones here.
pub fn stamp(unix: u64) -> String {
    let (year, month, day) = civil(unix / 86_400);
    let seconds = unix % 86_400;
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        seconds / 3600,
        (seconds % 3600) / 60,
        seconds % 60
    )
}

/// The date `days` after 1970-01-01, by the algorithm that counts from
/// March so that the leap day is the last day of the year and the month
/// lengths repeat.
fn civil(days: u64) -> (u64, u64, u64) {
    let days = days + 719_468;
    let era = days / 146_097;
    let of_era = days % 146_097;
    let year_of_era = (of_era - of_era / 1460 + of_era / 36_524 - of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let of_year = of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * of_year + 2) / 153;
    let day = of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example in Amazon's own documentation, which is the only
    /// thing that says this was read correctly rather than plausibly.
    /// A GET of an object with no query, signed on a fixed day with the
    /// documented key pair.
    #[test]
    fn the_documented_example_signs_the_documented_way() {
        let credentials = Credentials {
            access: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        };
        let request = Request {
            method: "GET",
            path: "/test.txt",
            query: "",
            headers: vec![
                (
                    "host".to_string(),
                    "examplebucket.s3.amazonaws.com".to_string(),
                ),
                ("range".to_string(), "bytes=0-9".to_string()),
                (
                    "x-amz-content-sha256".to_string(),
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
                ),
                ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
            ],
            payload: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            stamp: "20130524T000000Z",
            region: "us-east-1",
        };
        assert_eq!(
            authorization(&request, &credentials),
            "AWS4-HMAC-SHA256 \
             Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// Sorted by name rather than left as sent, which is the only part
    /// of a query string the signature cares about.
    #[test]
    fn a_query_is_signed_in_order_and_keeps_an_empty_value() {
        assert_eq!(
            canonical_query("prefix=a&delimiter=/"),
            "delimiter=/&prefix=a"
        );
        assert_eq!(canonical_query("list-type=2&x"), "list-type=2&x=");
        assert_eq!(canonical_query(""), "");
    }

    #[test]
    fn a_stamp_is_the_day_and_the_time_of_a_unix_second() {
        assert_eq!(stamp(0), "19700101T000000Z");
        assert_eq!(stamp(1_369_353_600), "20130524T000000Z");
        // A leap day, because the whole reason the conversion is
        // written out is that this is the one it would get wrong.
        assert_eq!(stamp(1_709_164_800), "20240229T000000Z");
        assert_eq!(stamp(1_754_537_280), "20250807T032800Z");
    }
}
