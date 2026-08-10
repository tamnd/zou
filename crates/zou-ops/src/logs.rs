//! The same log records, spelled as json.
//!
//! A line of prose is the right output for a terminal and the wrong one
//! for a log pipeline, which will either parse it with a regex that
//! breaks the first time a message contains a colon, or index the whole
//! line as one string and give up on querying it. So the format is a
//! switch, off by default because the default reader is a person
//! running `zou dev`, and on where something else is doing the reading.
//!
//! Nothing else changes: the same records, the same `RUST_LOG` filter,
//! the same stderr. What is written is one object per line, with the
//! fields flat and the names boring, because every collector already
//! knows what `level` and `msg` mean and none of them knows what ours
//! would have been.

use std::io::Write;

/// Install the process logger, filtered by `RUST_LOG` and falling back
/// to `default`.
///
/// `ZOU_LOG_FORMAT=json` picks json lines. Anything else, including
/// nothing, keeps the human format. An environment variable rather than
/// a flag because the thing that wants json is a container runtime,
/// which sets environment and does not get to rewrite the command line
/// of what it runs.
pub fn init(default: &str) {
    let env = env_logger::Env::default().default_filter_or(default);
    let mut builder = env_logger::Builder::from_env(env);
    if std::env::var("ZOU_LOG_FORMAT").is_ok_and(|v| v.eq_ignore_ascii_case("json")) {
        builder.format(|buf, record| {
            let at = record.file().map(|file| (file, record.line().unwrap_or(0)));
            writeln!(
                buf,
                "{}",
                line(
                    &buf.timestamp_millis().to_string(),
                    record.level(),
                    record.target(),
                    &record.args().to_string(),
                    at,
                    crate::trace::current(),
                )
            )
        });
    }
    builder.init();
}

/// One record as one json object.
///
/// The source location rides along only when the record carries one,
/// which is every record built by the macros and not every record built
/// by hand, and it is two fields rather than one string so that a
/// collector can group by file without splitting anything.
///
/// `ids` is the trace this record was written under, when there is one.
/// It is the field that makes a log line and a span the same story: a
/// slow trace opens a query for its lines and a suspicious line opens
/// the trace it came from.
pub fn line(
    ts: &str,
    level: log::Level,
    target: &str,
    message: &str,
    at: Option<(&str, u32)>,
    ids: Option<crate::trace::Ids>,
) -> String {
    let mut fields = serde_json::Map::new();
    fields.insert("ts".to_string(), ts.into());
    fields.insert("level".to_string(), level.as_str().to_lowercase().into());
    fields.insert("target".to_string(), target.into());
    fields.insert("msg".to_string(), message.into());
    if let Some((file, at)) = at {
        fields.insert("file".to_string(), file.into());
        fields.insert("line".to_string(), at.into());
    }
    if let Some(ids) = ids {
        fields.insert("trace_id".to_string(), ids.trace_hex().into());
        fields.insert("span_id".to_string(), ids.span_hex().into());
    }
    serde_json::Value::Object(fields).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn a_record_is_one_object_on_one_line() {
        let out = line(
            "2026-08-10T09:14:02.117Z",
            log::Level::Warn,
            "zou_server::gateway",
            "attach acme-prod: the store could not be read",
            Some(("crates/zou-server/src/gateway.rs", 73)),
            None,
        );
        assert!(!out.contains('\n'), "one line: {out}");
        let parsed: Value = serde_json::from_str(&out).expect("it parses");
        assert_eq!(parsed["ts"], "2026-08-10T09:14:02.117Z");
        assert_eq!(parsed["level"], "warn");
        assert_eq!(parsed["target"], "zou_server::gateway");
        assert_eq!(
            parsed["msg"],
            "attach acme-prod: the store could not be read"
        );
        assert_eq!(parsed["file"], "crates/zou-server/src/gateway.rs");
        assert_eq!(parsed["line"], 73);
    }

    #[test]
    fn a_record_without_a_source_location_leaves_the_fields_out() {
        let out = line("t", log::Level::Info, "zou", "listening", None, None);
        let parsed: Value = serde_json::from_str(&out).expect("it parses");
        assert!(parsed.get("file").is_none(), "{out}");
        assert!(parsed.get("line").is_none(), "{out}");
    }

    #[test]
    fn a_message_that_would_break_the_line_is_escaped() {
        // The reason json is worth having: a message with a quote, a
        // brace and a newline in it is still one parseable record.
        let out = line(
            "t",
            log::Level::Error,
            "zou_store",
            "put \"a/b\" failed\n{\"code\":500}",
            None,
            None,
        );
        assert_eq!(out.lines().count(), 1, "{out}");
        let parsed: Value = serde_json::from_str(&out).expect("it parses");
        assert_eq!(parsed["msg"], "put \"a/b\" failed\n{\"code\":500}");
    }

    #[test]
    fn a_record_under_a_trace_carries_its_ids() {
        let ids =
            crate::trace::Ids::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .expect("it parses");
        let out = line("t", log::Level::Info, "zou", "attaching", None, Some(ids));
        let parsed: Value = serde_json::from_str(&out).expect("it parses");
        assert_eq!(parsed["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(parsed["span_id"], "00f067aa0ba902b7");
    }

    #[test]
    fn a_record_outside_a_trace_says_nothing_about_one() {
        // A trace id no exporter will ever see is a field that costs
        // bytes and answers nothing.
        let out = line("t", log::Level::Info, "zou", "listening", None, None);
        let parsed: Value = serde_json::from_str(&out).expect("it parses");
        assert!(parsed.get("trace_id").is_none(), "{out}");
    }

    #[test]
    fn every_level_has_a_lowercase_name() {
        for (level, name) in [
            (log::Level::Error, "error"),
            (log::Level::Warn, "warn"),
            (log::Level::Info, "info"),
            (log::Level::Debug, "debug"),
            (log::Level::Trace, "trace"),
        ] {
            let parsed: Value =
                serde_json::from_str(&line("t", level, "zou", "x", None, None)).expect("it parses");
            assert_eq!(parsed["level"], name);
        }
    }
}
