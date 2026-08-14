//! `URL`, parsed by the crate that already parses urls here.
//!
//! Writing a WHATWG url parser in the prelude would be a few hundred
//! lines of javascript that is wrong in the corners: percent encoding
//! per component, IDNA, the special schemes, what a relative reference
//! means against a base. The `url` crate is that parser, it is already
//! in this build because `deno_core` and the HTTP client both use it,
//! and it is the same one Deno's own `URL` is built on. So the parsing
//! is here and the shape a handler sees is in the prelude.
//!
//! Two ops: one that parses, one that changes a component. Both hand
//! back every component of the result rather than one, because a url
//! is read a piece at a time and a round trip per piece would be worse
//! than a struct per parse.

use deno_core::op2;
use deno_core::url::Url;

/// Every component of a parsed url, spelled the way the property is.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Parts {
    href: String,
    origin: String,
    protocol: String,
    username: String,
    password: String,
    host: String,
    hostname: String,
    port: String,
    pathname: String,
    search: String,
    hash: String,
}

impl From<&Url> for Parts {
    fn from(url: &Url) -> Parts {
        Parts {
            href: url.as_str().to_string(),
            origin: url.origin().ascii_serialization(),
            protocol: format!("{}:", url.scheme()),
            username: url.username().to_string(),
            password: url.password().unwrap_or_default().to_string(),
            host: host(url),
            hostname: url.host_str().unwrap_or_default().to_string(),
            port: url.port().map(|it| it.to_string()).unwrap_or_default(),
            pathname: url.path().to_string(),
            // Empty rather than a lone `?` or `#`, which is what the
            // property is when there is nothing after the mark.
            search: url
                .query()
                .filter(|it| !it.is_empty())
                .map_or_else(String::new, |it| format!("?{it}")),
            hash: url
                .fragment()
                .filter(|it| !it.is_empty())
                .map_or_else(String::new, |it| format!("#{it}")),
        }
    }
}

/// `host` is `hostname` with the port on it, when there is one, which
/// is the one component that is two.
fn host(url: &Url) -> String {
    match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_string(),
        (None, _) => String::new(),
    }
}

/// A url, or nothing, which the prelude turns into the `TypeError` the
/// constructor throws. An empty base means there is none: a relative
/// reference without one is not a url.
#[op2]
#[serde]
pub fn op_zou_url_parse(#[string] input: &str, #[string] base: &str) -> Option<Parts> {
    parse(input, base)
}

/// The op's own work, spelled as a function so it can be tested: what
/// `#[op2]` generates is reachable from javascript and not from Rust.
fn parse(input: &str, base: &str) -> Option<Parts> {
    Some(Parts::from(&parsed(input, base)?))
}

fn parsed(input: &str, base: &str) -> Option<Url> {
    if base.is_empty() {
        return Url::parse(input).ok();
    }
    let base = Url::parse(base).ok()?;
    base.join(input).ok()
}

/// One component changed, and every component as it is afterwards.
///
/// A setter that cannot do what it was asked leaves the url alone
/// rather than throwing, which is what the spec says and is why this
/// returns the parts either way.
#[op2]
#[serde]
pub fn op_zou_url_set(
    #[string] href: &str,
    #[string] part: &str,
    #[string] value: &str,
) -> Option<Parts> {
    set(href, part, value)
}

fn set(href: &str, part: &str, value: &str) -> Option<Parts> {
    let mut url = Url::parse(href).ok()?;
    match part {
        "href" => return parse(value, ""),
        "protocol" => {
            let _ = url.set_scheme(value.trim_end_matches(':'));
        }
        "username" => {
            let _ = url.set_username(value);
        }
        "password" => {
            let _ = url.set_password(Some(value).filter(|it| !it.is_empty()));
        }
        "host" => {
            let (name, port) = match value.rsplit_once(':') {
                Some((name, port)) => (name, port.parse::<u16>().ok()),
                None => (value, None),
            };
            if url.set_host(Some(name)).is_ok() {
                let _ = url.set_port(port);
            }
        }
        "hostname" => {
            let _ = url.set_host(Some(value));
        }
        "port" => {
            let _ = url.set_port(value.parse::<u16>().ok());
        }
        "pathname" => url.set_path(value),
        "search" => {
            let search = value.trim_start_matches('?');
            url.set_query(Some(search).filter(|it| !it.is_empty()));
        }
        "hash" => {
            let hash = value.trim_start_matches('#');
            url.set_fragment(Some(hash).filter(|it| !it.is_empty()));
        }
        _ => return None,
    }
    Some(Parts::from(&url))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(input: &str) -> Parts {
        Parts::from(&Url::parse(input).expect("a url"))
    }

    #[test]
    fn every_component_of_an_ordinary_url() {
        let url = parts("https://ana:secret@example.com:8443/one/two?a=1&b=2#top");
        assert_eq!(url.protocol, "https:");
        assert_eq!(url.username, "ana");
        assert_eq!(url.password, "secret");
        assert_eq!(url.hostname, "example.com");
        assert_eq!(url.port, "8443");
        assert_eq!(url.host, "example.com:8443");
        assert_eq!(url.pathname, "/one/two");
        assert_eq!(url.search, "?a=1&b=2");
        assert_eq!(url.hash, "#top");
        assert_eq!(url.origin, "https://example.com:8443");
    }

    #[test]
    fn a_default_port_is_not_a_port() {
        let url = parts("https://example.com/");
        assert_eq!(url.port, "");
        assert_eq!(url.host, "example.com");
        assert_eq!(url.origin, "https://example.com");
    }

    #[test]
    fn nothing_after_the_mark_is_nothing_and_not_the_mark() {
        let url = parts("https://example.com/a?#");
        assert_eq!(url.search, "");
        assert_eq!(url.hash, "");
    }

    #[test]
    fn a_relative_reference_needs_a_base_and_is_joined_to_it() {
        assert!(parse("/two", "").is_none());
        let joined = parse("/two", "https://example.com/one/three").expect("a url");
        assert_eq!(joined.href, "https://example.com/two");
        let beside = parse("./two", "https://example.com/one/three").expect("a url");
        assert_eq!(beside.href, "https://example.com/one/two");
    }

    #[test]
    fn what_is_not_a_url_is_nothing() {
        for input in ["", "not a url", "://example.com", "http://"] {
            assert!(parse(input, "").is_none(), "{input}");
        }
    }

    #[test]
    fn a_component_can_be_changed_and_the_rest_follows() {
        let changed =
            |part, value| set("https://example.com/one?a=1#top", part, value).expect("a url");
        assert_eq!(
            changed("pathname", "/two").href,
            "https://example.com/two?a=1#top"
        );
        assert_eq!(
            changed("search", "b=2").href,
            "https://example.com/one?b=2#top"
        );
        assert_eq!(
            changed("search", "?b=2").href,
            "https://example.com/one?b=2#top"
        );
        assert_eq!(
            changed("hash", "bottom").href,
            "https://example.com/one?a=1#bottom"
        );
        assert_eq!(changed("port", "8443").host, "example.com:8443");
        assert_eq!(
            changed("host", "other.example:1234").host,
            "other.example:1234"
        );
        assert_eq!(
            changed("hostname", "other.example").hostname,
            "other.example"
        );
        assert_eq!(changed("protocol", "http:").protocol, "http:");
        assert_eq!(
            changed("href", "https://elsewhere.example/").href,
            "https://elsewhere.example/"
        );
    }

    #[test]
    fn emptying_a_component_empties_it() {
        let changed =
            |part, value| set("https://example.com/one?a=1#top", part, value).expect("a url");
        assert_eq!(changed("search", "").search, "");
        assert_eq!(changed("hash", "").hash, "");
        assert_eq!(changed("port", "").port, "");
    }

    /// The spec's answer to a setter it cannot honour is to do nothing,
    /// which is not the same as an exception and not the same as a url
    /// that half changed.
    #[test]
    fn a_change_that_cannot_be_made_leaves_the_url_alone() {
        let url = set("https://example.com/one", "protocol", "not a scheme").expect("a url");
        assert_eq!(url.href, "https://example.com/one");
        let port = set("https://example.com/one", "port", "nonsense").expect("a url");
        assert_eq!(port.port, "");
    }
}
