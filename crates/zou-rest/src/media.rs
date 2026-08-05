//! Media type names.
//!
//! Two headers name a media type and both are read here: Accept says
//! what the answer may be written as, Content-Type says what the body
//! was written as. PostgREST keeps one enumeration for both and
//! spells every name back out of it, which is why a 406 lists
//! `application/vnd.pgrst.plan+text; for="application/json"` when the
//! request only wrote `application/vnd.pgrst.plan`: the list is the
//! names it understood, not the strings it was sent.
//!
//! A type this surface has no name for keeps the string it arrived
//! as, parameters and all, since there is nothing to canonicalize it
//! into.

/// A media type, as PostgREST enumerates them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaType {
    Json,
    /// The vendored array name with nulls=stripped on it. Without the
    /// parameter it is plain json and decodes as such.
    ArrayStripped,
    Single {
        stripped: bool,
    },
    Csv,
    Text,
    Xml,
    GeoJson,
    OpenApi,
    Form,
    Bytes,
    Any,
    /// The plan name, which carries the media type the plan is for
    /// and the explain options asked for.
    Plan {
        of: Box<MediaType>,
        json: bool,
        options: Vec<&'static str>,
    },
    Other(String),
}

/// The explain options a plan name may carry, in the order the name
/// spells them back out whatever order it was written in.
const OPTIONS: [&str; 5] = ["analyze", "verbose", "settings", "buffers", "wal"];

/// One media type name into the type it names.
///
/// The type and subtype decide, the parameters only refine: a name
/// with a parameter nobody reads is the same type as the name
/// without it, which is how `application/json;charset=utf-8` is
/// json. A name that does not parse at all is not an error, it is a
/// type this surface has no name for, and it keeps what it was sent.
pub fn decode(raw: &str) -> MediaType {
    let Tokens(main, sub, params) = match tokenize(raw) {
        Some(parts) => parts,
        None => Tokens(raw.to_ascii_lowercase(), String::new(), Vec::new()),
    };
    let param = |name: &str| {
        params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    };
    let stripped = param("nulls") == Some("stripped");
    let plan = |json: bool| MediaType::Plan {
        of: Box::new(decode(param("for").unwrap_or("application/json"))),
        json,
        options: {
            let asked: Vec<&str> = param("options").unwrap_or("").split('|').collect();
            OPTIONS
                .into_iter()
                .filter(|o| asked.contains(o))
                .collect::<Vec<_>>()
        },
    };
    match (main.as_str(), sub.as_str()) {
        ("application", "json") => MediaType::Json,
        ("application", "geo+json") => MediaType::GeoJson,
        ("text", "csv") => MediaType::Csv,
        ("text", "plain") => MediaType::Text,
        ("text", "xml") => MediaType::Xml,
        ("application", "openapi+json") => MediaType::OpenApi,
        ("application", "x-www-form-urlencoded") => MediaType::Form,
        ("application", "octet-stream") => MediaType::Bytes,
        ("application", "vnd.pgrst.plan" | "vnd.pgrst.plan+text") => plan(false),
        ("application", "vnd.pgrst.plan+json") => plan(true),
        ("application", "vnd.pgrst.object+json" | "vnd.pgrst.object") => {
            MediaType::Single { stripped }
        }
        ("application", "vnd.pgrst.array+json" | "vnd.pgrst.array") if stripped => {
            MediaType::ArrayStripped
        }
        ("application", "vnd.pgrst.array+json" | "vnd.pgrst.array") => MediaType::Json,
        ("*", "*") => MediaType::Any,
        _ => MediaType::Other(raw.to_string()),
    }
}

/// The name of a media type, which is what a 406 lists and what a
/// Content-Type header carries once the charset is on it.
pub fn mime(m: &MediaType) -> String {
    match m {
        MediaType::Json => "application/json".to_string(),
        MediaType::ArrayStripped => "application/vnd.pgrst.array+json;nulls=stripped".to_string(),
        MediaType::Single { stripped: false } => "application/vnd.pgrst.object+json".to_string(),
        MediaType::Single { stripped: true } => {
            "application/vnd.pgrst.object+json;nulls=stripped".to_string()
        }
        MediaType::Csv => "text/csv".to_string(),
        MediaType::Text => "text/plain".to_string(),
        MediaType::Xml => "text/xml".to_string(),
        MediaType::GeoJson => "application/geo+json".to_string(),
        MediaType::OpenApi => "application/openapi+json".to_string(),
        MediaType::Form => "application/x-www-form-urlencoded".to_string(),
        MediaType::Bytes => "application/octet-stream".to_string(),
        MediaType::Any => "*/*".to_string(),
        MediaType::Plan { of, json, options } => {
            let format = if *json { "json" } else { "text" };
            let opts = if options.is_empty() {
                String::new()
            } else {
                format!("; options={}", options.join("|"))
            };
            format!(
                "application/vnd.pgrst.plan+{format}; for=\"{}\"{opts}",
                mime(of)
            )
        }
        MediaType::Other(raw) => raw.clone(),
    }
}

/// The Content-Type header for a media type. Everything this surface
/// writes is utf-8 and says so, except the two that are not text:
/// bytes, and a type nobody here named.
pub fn content_type(m: &MediaType) -> String {
    match m {
        MediaType::Bytes | MediaType::Other(_) => mime(m),
        _ => format!("{}; charset=utf-8", mime(m)),
    }
}

/// A name taken apart: type, subtype, parameters.
struct Tokens(String, String, Vec<(String, String)>);

/// A name into its type, subtype and parameters, or none when it is
/// not a media type name at all.
///
/// The parameter list stops at the first thing that is not one
/// rather than failing, which is deliberate: `text/csv; q=0.5, junk`
/// is still csv, and only the type and subtype ever decide.
fn tokenize(raw: &str) -> Option<Tokens> {
    let chars: Vec<char> = raw.chars().collect();
    let mut i = 0;
    let take = |i: &mut usize, ok: fn(char) -> bool| {
        let start = *i;
        while *i < chars.len() && ok(chars[*i]) {
            *i += 1;
        }
        chars[start..*i].iter().collect::<String>()
    };
    let main = take(&mut i, |c| c.is_alphanumeric() || matches!(c, '.' | '*'));
    if main.is_empty() || i >= chars.len() || chars[i] != '/' {
        return None;
    }
    i += 1;
    let sub = take(&mut i, |c| {
        c.is_alphanumeric() || matches!(c, '.' | '*' | '+' | '-')
    });
    if sub.is_empty() {
        return None;
    }
    let mut params = Vec::new();
    loop {
        let mut j = i;
        let space = |j: &mut usize| {
            while *j < chars.len() && chars[*j] == ' ' {
                *j += 1;
            }
        };
        space(&mut j);
        if j >= chars.len() || chars[j] != ';' {
            break;
        }
        j += 1;
        space(&mut j);
        let key = take(&mut j, |c| c.is_alphanumeric() || c == '-');
        if key.is_empty() {
            break;
        }
        space(&mut j);
        if j >= chars.len() || chars[j] != '=' {
            break;
        }
        j += 1;
        space(&mut j);
        let value = if chars.get(j) == Some(&'"') {
            j += 1;
            let v = take(&mut j, |c| c != '"');
            if j >= chars.len() {
                break;
            }
            j += 1;
            v
        } else {
            let v = take(&mut j, |c| c.is_alphanumeric() || matches!(c, '|' | '-'));
            if v.is_empty() {
                break;
            }
            v
        };
        params.push((key.to_ascii_lowercase(), value));
        i = j;
    }
    Some(Tokens(
        main.to_ascii_lowercase(),
        sub.to_ascii_lowercase(),
        params,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_parameter_nobody_reads_does_not_change_the_type() {
        assert_eq!(decode("application/json"), MediaType::Json);
        assert_eq!(decode("application/json;charset=UTF-8"), MediaType::Json);
        assert_eq!(
            decode("ApplicatIon/vnd.PgRsT.object+json"),
            MediaType::Single { stripped: false }
        );
        assert_eq!(decode("*/*"), MediaType::Any);
    }

    #[test]
    fn the_array_name_is_json_unless_the_nulls_parameter_rides_along() {
        assert_eq!(decode("application/vnd.pgrst.array+json"), MediaType::Json);
        assert_eq!(
            decode("application/vnd.pgrst.array+json;nulls=stripped"),
            MediaType::ArrayStripped
        );
        assert_eq!(
            decode("application/vnd.pgrst.object+json;nulls=stripped"),
            MediaType::Single { stripped: true }
        );
    }

    #[test]
    fn a_plan_says_what_it_is_a_plan_of_even_when_the_request_did_not() {
        assert_eq!(
            mime(&decode("application/vnd.pgrst.plan")),
            "application/vnd.pgrst.plan+text; for=\"application/json\""
        );
        assert_eq!(
            mime(&decode("application/vnd.pgrst.plan+json;for=\"text/csv\"")),
            "application/vnd.pgrst.plan+json; for=\"text/csv\""
        );
        assert_eq!(
            mime(&decode(
                "application/vnd.pgrst.plan+text; for=\"text/xml\"; options=wal|analyze"
            )),
            "application/vnd.pgrst.plan+text; for=\"text/xml\"; options=analyze|wal"
        );
    }

    #[test]
    fn a_name_nobody_here_has_keeps_the_string_it_arrived_as() {
        assert_eq!(
            decode("application/vnd.twkb"),
            MediaType::Other("application/vnd.twkb".to_string())
        );
        assert_eq!(
            decode("audio/mpeg3"),
            MediaType::Other("audio/mpeg3".to_string())
        );
        assert_eq!(mime(&decode("audio/mpeg3")), "audio/mpeg3");
        assert_eq!(decode("nonsense"), MediaType::Other("nonsense".to_string()));
    }

    #[test]
    fn only_what_is_not_text_goes_out_without_a_charset() {
        assert_eq!(
            content_type(&MediaType::Json),
            "application/json; charset=utf-8"
        );
        assert_eq!(content_type(&MediaType::Text), "text/plain; charset=utf-8");
        assert_eq!(content_type(&MediaType::Bytes), "application/octet-stream");
        assert_eq!(
            content_type(&MediaType::Other("audio/mpeg3".to_string())),
            "audio/mpeg3"
        );
    }
}
