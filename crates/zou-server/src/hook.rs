//! GoTrue's auth hooks: the points where a project puts its own code
//! in the middle of a flow this server runs.
//!
//! One is here so far, the custom access token hook, and only its
//! postgres half. It is the one with the longest reach, because the
//! claims it hands back are the claims that get signed, and those are
//! the claims auth.jwt() reads out again inside every RLS policy.
//!
//! The contract is upstream's, and the whole of it matters because a
//! project writes SQL against it:
//!
//! - The point is a URI, `pg-functions://<database>/<schema>/<function>`.
//!   The database part is parsed and then ignored, upstream too: the
//!   function runs on the connection the request is already holding.
//! - The function takes one jsonb argument and returns jsonb. It is
//!   called inside the grant's own transaction, so what it writes
//!   commits with the grant and rolls back with it.
//! - It is given `{metadata, user_id, claims, authentication_method}`
//!   and has to return `{"claims": {...}}`. Those claims replace the
//!   whole set, they are not merged, so a hook that drops a claim
//!   drops it from the token.
//! - It may refuse, by returning `{"error": {"message", "http_code"}}`,
//!   and the request fails with that message at that status.
//! - It gets two seconds, as a statement_timeout on the transaction.
//! - Whatever it returns still has to carry the claims a Supabase
//!   client and an RLS policy need, which is upstream's minimum viable
//!   token schema, checked here after the hook rather than trusted.
//!
//! The one part of upstream not built here is the HTTP variant of a
//! hook. A URI with any other scheme is refused at startup rather than
//! quietly ignored, so a project that points this at an endpoint finds
//! out immediately instead of at the first sign in.

use axum::http::StatusCode;

use crate::auth::Error;
use crate::sql;

/// One extensibility point, GoTrue's ExtensibilityPointConfiguration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Point {
    /// `GOTRUE_HOOK_<POINT>_ENABLED`. A point with a URI and this unset
    /// is configured and dormant, which is how upstream lets an
    /// operator leave the wiring in place and switch it off.
    pub enabled: bool,
    /// The URI as configured, kept so a log line can say what was
    /// pointed at.
    pub uri: String,
    /// What the URI names, quoted the way upstream quotes it:
    /// `"schema"."function"`. Empty when no URI was configured.
    pub name: String,
}

/// The project's hooks.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Settings {
    pub custom_access_token: Point,
}

impl Point {
    /// A point nobody configured.
    pub const fn off() -> Point {
        Point {
            enabled: false,
            uri: String::new(),
            name: String::new(),
        }
    }

    /// Whether this point should be run, which takes both halves: a
    /// URI that named something and the switch that turns it on.
    pub fn live(&self) -> bool {
        self.enabled && !self.name.is_empty()
    }
}

impl Default for Point {
    fn default() -> Point {
        Point::off()
    }
}

impl Settings {
    /// A project with no hooks at all, which is the usual project.
    pub const fn none() -> Settings {
        Settings {
            custom_access_token: Point::off(),
        }
    }
}

/// The hooks the environment asks for, GOTRUE_ swapped for ZOU_.
pub fn from_env() -> Result<Settings, String> {
    configured(&|name| std::env::var(name).unwrap_or_default())
}

pub fn configured(var: &dyn Fn(&str) -> String) -> Result<Settings, String> {
    Ok(Settings {
        custom_access_token: point(var, "ZOU_HOOK_CUSTOM_ACCESS_TOKEN")?,
    })
}

fn point(var: &dyn Fn(&str) -> String, prefix: &str) -> Result<Point, String> {
    let uri = var(&format!("{prefix}_URI"));
    let uri = uri.trim().to_string();
    // Upstream validates the URI of every point whether or not the
    // point is enabled, so a project that switches a hook on later
    // finds out at the same restart it wrote the URI in.
    let name = match uri.is_empty() {
        true => String::new(),
        false => names(&uri).map_err(|why| format!("{prefix}_URI is {uri:?}, {why}"))?,
    };
    let switch = format!("{prefix}_ENABLED");
    let enabled = match var(&switch).trim() {
        "" | "false" | "0" => false,
        "true" | "1" => true,
        other => return Err(format!("{switch} is {other:?}, which is not true or false")),
    };
    Ok(Point { enabled, uri, name })
}

/// GoTrue's ValidateExtensibilityPoint and PopulateExtensibilityPoint
/// in one: what the URI names, or why it names nothing this end can
/// call.
///
/// The name that comes back is interpolated into the select. The
/// quoting is not what makes that safe, the name rule below it is, and
/// it is postgres's own rule for an unquoted identifier.
fn names(uri: &str) -> Result<String, String> {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return Err(NOT_PG.to_string());
    };
    if !scheme.eq_ignore_ascii_case("pg-functions") {
        return Err(NOT_PG.to_string());
    }
    // What follows the scheme is the database, which upstream reads
    // and never uses. The path is everything from the first slash.
    let path = match rest.find('/') {
        Some(at) => &rest[at..],
        None => "",
    };
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 3 {
        return Err("and its path does not contain enough parts".to_string());
    }
    let (schema, function) = (parts[1], parts[2]);
    if !is_name(schema) {
        return Err(format!("and {schema:?} is not a schema name"));
    }
    if !is_name(function) {
        return Err(format!("and {function:?} is not a function name"));
    }
    Ok(format!("\"{schema}\".\"{function}\""))
}

const NOT_PG: &str = "and only pg-functions hooks are implemented here so far, not http ones";

/// Postgres's rule for an unquoted name, which is the rule upstream
/// holds the schema and the function to.
fn is_name(word: &str) -> bool {
    let mut chars = word.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && word.len() <= 63
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The two seconds upstream gives a hook, spent as a statement_timeout
/// on the transaction the grant is already inside. A hook that sits
/// there takes the sign in down with it, so the timeout is what keeps
/// a slow hook from holding a connection for the whole request.
const TIMEOUT_MS: i64 = 2000;

/// What a hook is handed, GoTrue's CustomAccessTokenInput.
///
/// The claims are the ones this server was about to sign. The uuid
/// stamps this one call so a hook that writes its own log can tell two
/// calls apart, and the time is the wall clock, in UTC rather than
/// upstream's local zone, which is the same instant either way.
pub(crate) fn input(claims: &serde_json::Value, method: &str, ip: &str) -> serde_json::Value {
    let mut metadata = serde_json::json!({
        "uuid": uuid4(),
        "time": stamp(crate::auth::now()),
        "name": "customize-access-token",
    });
    if !ip.is_empty() {
        metadata["ip_address"] = ip.into();
    }
    serde_json::json!({
        "metadata": metadata,
        "user_id": claims["sub"],
        "claims": claims,
        "authentication_method": method,
    })
}

/// Run the project's custom access token hook and hand back the claims
/// it wants signed, or the refusal it answered with.
pub(crate) async fn customize(
    sess: &sql::Session,
    point: &Point,
    input: &serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let Some(raw) = call(sess, &point.name, input).await? else {
        // The function answered SQL null, which upstream never
        // unmarshals into anything, so the claims it goes on to check
        // are the null it started with.
        return Err(broken(&schema_error(&[
            "(root): Invalid type. Expected: object, but got: null".to_string(),
        ])));
    };
    let Ok(out) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return Err(broken(UNREADABLE));
    };
    if let Some(refusal) = refused(&out) {
        return Err(refusal);
    }
    let claims = match &out {
        // A json null parses into nothing, and nothing has no claims
        // field, which is the same complaint as an empty object.
        serde_json::Value::Null => return Err(broken(MISSING)),
        serde_json::Value::Object(map) => match map.get("claims") {
            None => return Err(broken(MISSING)),
            Some(serde_json::Value::Null) => {
                return Err(broken(&schema_error(&[
                    "(root): Invalid type. Expected: object, but got: null".to_string(),
                ])));
            }
            Some(claims @ serde_json::Value::Object(_)) => claims.clone(),
            Some(_) => return Err(broken(UNREADABLE)),
        },
        _ => return Err(broken(UNREADABLE)),
    };
    let complaints = conforms(&claims);
    match complaints.is_empty() {
        true => Ok(claims),
        false => Err(broken(&schema_error(&complaints))),
    }
}

/// GoTrue's runPostgresHook: the timeout, the call, the timeout put
/// back. All three run on the grant's own transaction, so a hook that
/// writes is part of the same commit, and a hook that raises takes the
/// grant down with it.
async fn call(
    sess: &sql::Session,
    name: &str,
    input: &serde_json::Value,
) -> Result<Option<String>, Error> {
    sess.execute(
        &format!("set local statement_timeout to '{TIMEOUT_MS}'"),
        &[],
    )
    .await?;
    let rows = sess
        .query(&format!("select {name}($1::jsonb)::text"), &[input])
        .await?;
    let out: Option<String> = rows.first().and_then(|row| row.get(0));
    sess.execute("set local statement_timeout to default", &[])
        .await?;
    Ok(out)
}

/// GoTrue's hookserrors.Check. A hook says it refused by putting an
/// error object in what it returns, and anything that is not shaped
/// like one is not a refusal: a claim called error, a message that is
/// not a string, an empty message. Upstream reads all of those as a
/// hook that did not refuse, and so does this.
fn refused(out: &serde_json::Value) -> Option<Error> {
    let error = out.get("error")?.as_object()?;
    let message = match error.get("message") {
        None => "",
        Some(serde_json::Value::String(m)) => m,
        Some(_) => return None,
    };
    let code = match error.get("http_code") {
        None => 0,
        Some(serde_json::Value::Number(n)) if n.is_i64() => n.as_i64().unwrap_or(0),
        Some(_) => return None,
    };
    if message.is_empty() {
        return None;
    }
    // A hook that refuses without saying at what status gets 500,
    // which is upstream's default and a deliberate one: it is a hook
    // that broke, not a request that was wrong.
    let status = u16::try_from(code)
        .ok()
        .and_then(|code| StatusCode::from_u16(code).ok())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    Some(Error::Hook {
        status,
        code: named(status),
        msg: message.to_string(),
    })
}

/// A hook that broke rather than refused: always a 500, and always
/// with the message that says which way it broke, because the only
/// person who can fix it is the one who wrote the function.
fn broken(msg: &str) -> Error {
    Error::Hook {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        code: named(StatusCode::INTERNAL_SERVER_ERROR),
        msg: msg.to_string(),
    }
}

/// The error_code a client sees. GoTrue fills one in for an error that
/// carries none of its own, and it fills in a different one for a 500.
fn named(status: StatusCode) -> &'static str {
    match status == StatusCode::INTERNAL_SERVER_ERROR {
        true => "unexpected_failure",
        false => "unknown",
    }
}

const MISSING: &str = "output claims field is missing";
const UNREADABLE: &str = "Error unmarshaling JSON output.";

fn schema_error(complaints: &[String]) -> String {
    let mut msg = "output claims do not conform to the expected schema: \n".to_string();
    for complaint in complaints {
        msg.push_str(&format!("- {complaint}\n"));
    }
    msg
}

/// The claims a token still has to carry after the hook has had its
/// way with it, GoTrue's required list in its own order.
const REQUIRED: [&str; 10] = [
    "aud",
    "exp",
    "iat",
    "sub",
    "email",
    "phone",
    "role",
    "aal",
    "session_id",
    "is_anonymous",
];

/// The claims the schema puts a type on, in the order it lists them.
/// `is_anonymous` is required and typeless upstream, so a hook can
/// hand back anything at all under it, and this end does not invent a
/// rule upstream does not have. Anything not listed is a claim the
/// project made up, which is the whole point of the hook.
const TYPED: [(&str, &[&str]); 16] = [
    ("aud", &["string", "array"]),
    ("exp", &["integer"]),
    ("jti", &["string"]),
    ("iat", &["integer"]),
    ("iss", &["string"]),
    ("nbf", &["integer"]),
    ("sub", &["string"]),
    ("email", &["string"]),
    ("phone", &["string"]),
    ("app_metadata", &["object"]),
    ("user_metadata", &["object"]),
    ("role", &["string"]),
    ("aal", &["string"]),
    ("amr", &["array"]),
    ("session_id", &["string"]),
    ("client_id", &["string"]),
];

/// GoTrue's MinimumViableTokenSchema, checked the way the json schema
/// library upstream uses reports it: what is missing first, then what
/// is the wrong type, each in the order the schema lists them.
fn conforms(claims: &serde_json::Value) -> Vec<String> {
    let mut complaints = Vec::new();
    for name in REQUIRED {
        if claims.get(name).is_none() {
            complaints.push(format!("(root): {name} is required"));
        }
    }
    for (name, allowed) in TYPED {
        let Some(value) = claims.get(name) else {
            continue;
        };
        let given = kind(value);
        // A whole number satisfies integer and number both, which is
        // what json schema says and what the library upstream uses
        // does.
        let ok = allowed.contains(&given) || (given == "integer" && allowed.contains(&"number"));
        if !ok {
            complaints.push(format!(
                "{name}: Invalid type. Expected: {}, but got: {given}",
                allowed.join(", ")
            ));
        } else if name == "amr" {
            // The amr entries are strings or objects, nothing else.
            // Upstream reports the branch that came closest as well,
            // which is a second line this end does not repeat.
            for (at, entry) in value.as_array().into_iter().flatten().enumerate() {
                if !matches!(
                    entry,
                    serde_json::Value::String(_) | serde_json::Value::Object(_)
                ) {
                    complaints.push(format!(
                        "amr.{at}: Must validate at least one schema (anyOf)"
                    ));
                }
            }
        }
    }
    complaints
}

/// What json schema calls the type of a value. A number with nothing
/// after the point is an integer, which is the distinction the exp and
/// iat claims are held to.
fn kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(n) => match n.as_f64().is_some_and(|f| f.fract() != 0.0) {
            true => "number",
            false => "integer",
        },
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// A version 4 uuid, which is what upstream stamps a hook call with.
fn uuid4() -> String {
    let mut raw = [0u8; 16];
    getrandom::fill(&mut raw).expect("the os rng never fails");
    raw[6] = (raw[6] & 0x0f) | 0x40;
    raw[8] = (raw[8] & 0x3f) | 0x80;
    let mut out = String::with_capacity(36);
    for (at, byte) in raw.iter().enumerate() {
        if matches!(at, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// An instant as RFC 3339 in UTC, the shape Go marshals a time.Time
/// into and therefore the shape a hook's own parser expects.
fn stamp(unix: i64) -> String {
    const DAY: i64 = 86_400;
    let (year, month, day) = crate::smtp::civil(unix.div_euclid(DAY));
    let secs = unix.rem_euclid(DAY);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An environment made of pairs, so a test says only what it set.
    fn env(pairs: &[(&str, &str)]) -> Settings {
        read(pairs).expect("these settings are readable")
    }

    fn read(pairs: &[(&str, &str)]) -> Result<Settings, String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        configured(&|name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        })
    }

    #[test]
    fn a_project_with_nothing_set_has_no_hooks() {
        assert_eq!(env(&[]), Settings::none());
        assert!(!Settings::none().custom_access_token.live());
    }

    #[test]
    fn a_uri_names_a_schema_and_a_function() {
        let point = env(&[
            (
                "ZOU_HOOK_CUSTOM_ACCESS_TOKEN_URI",
                "pg-functions://postgres/public/custom_access_token_hook",
            ),
            ("ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED", "true"),
        ])
        .custom_access_token;
        assert_eq!(point.name, "\"public\".\"custom_access_token_hook\"");
        assert!(point.live());
        // The uri is kept as written, because the log line that says
        // what is wired up is more useful in the operator's own words.
        assert_eq!(
            point.uri,
            "pg-functions://postgres/public/custom_access_token_hook"
        );
    }

    #[test]
    fn a_hook_with_no_switch_is_wired_up_and_dormant() {
        let point = env(&[(
            "ZOU_HOOK_CUSTOM_ACCESS_TOKEN_URI",
            "pg-functions://postgres/auth/mine",
        )])
        .custom_access_token;
        assert_eq!(point.name, "\"auth\".\"mine\"");
        assert!(!point.enabled);
        assert!(!point.live());
    }

    #[test]
    fn a_switch_with_no_uri_stays_off() {
        // There is nothing to call, so the switch cannot turn anything
        // on however it is set.
        let point = env(&[("ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED", "true")]).custom_access_token;
        assert!(point.enabled);
        assert!(!point.live());
    }

    #[test]
    fn the_switch_takes_the_words_an_operator_writes() {
        for (word, on) in [
            ("true", true),
            ("1", true),
            ("false", false),
            ("0", false),
            ("", false),
            ("  true  ", true),
        ] {
            let point = env(&[("ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED", word)]).custom_access_token;
            assert_eq!(point.enabled, on, "{word:?}");
        }
        assert_eq!(
            read(&[("ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED", "yes")]),
            Err(
                "ZOU_HOOK_CUSTOM_ACCESS_TOKEN_ENABLED is \"yes\", which is not true or false"
                    .to_string()
            )
        );
    }

    #[test]
    fn a_uri_this_end_cannot_call_is_refused_at_startup() {
        // The http variant of a hook is the one part of upstream not
        // built here, and a project that points at one is told so at
        // the restart it wrote the URI in rather than at the first sign
        // in.
        assert_eq!(
            read(&[(
                "ZOU_HOOK_CUSTOM_ACCESS_TOKEN_URI",
                "https://example.com/hook"
            )]),
            Err(format!(
                "ZOU_HOOK_CUSTOM_ACCESS_TOKEN_URI is \"https://example.com/hook\", {NOT_PG}"
            ))
        );
    }

    #[test]
    fn a_uri_that_names_nothing_callable_says_which_part_is_wrong() {
        for (uri, why) in [
            ("public/f", NOT_PG.to_string()),
            (
                "pg-functions://postgres/public",
                "and its path does not contain enough parts".to_string(),
            ),
            (
                "pg-functions://postgres",
                "and its path does not contain enough parts".to_string(),
            ),
            (
                "pg-functions://postgres//f",
                "and \"\" is not a schema name".to_string(),
            ),
            (
                "pg-functions://postgres/public/drop table users",
                "and \"drop table users\" is not a function name".to_string(),
            ),
            (
                "pg-functions://postgres/public/f\"; drop table users; --",
                "and \"f\\\"; drop table users; --\" is not a function name".to_string(),
            ),
        ] {
            assert_eq!(
                read(&[("ZOU_HOOK_CUSTOM_ACCESS_TOKEN_URI", uri)]),
                Err(format!(
                    "ZOU_HOOK_CUSTOM_ACCESS_TOKEN_URI is {uri:?}, {why}"
                )),
                "{uri}"
            );
        }
    }

    #[test]
    fn the_database_in_the_uri_is_read_and_ignored() {
        // Upstream parses it and never uses it: the function runs on
        // the connection the request is already holding, whatever the
        // URI says.
        assert_eq!(
            names("pg-functions://somewhere-else/public/f"),
            Ok("\"public\".\"f\"".to_string())
        );
        assert_eq!(
            names("PG-Functions://postgres/public/f"),
            Ok("\"public\".\"f\"".to_string())
        );
    }

    #[test]
    fn a_name_is_what_postgres_calls_one() {
        assert!(is_name("public"));
        assert!(is_name("_private"));
        assert!(is_name("f1"));
        assert!(is_name(&"a".repeat(63)));
        assert!(!is_name(""));
        assert!(!is_name("1f"));
        assert!(!is_name("with space"));
        assert!(!is_name("quote\""));
        assert!(!is_name("dash-ed"));
        assert!(!is_name(&"a".repeat(64)));
    }

    fn refusal(out: serde_json::Value) -> Option<(u16, &'static str, String)> {
        match refused(&out) {
            Some(Error::Hook { status, code, msg }) => Some((status.as_u16(), code, msg)),
            Some(other) => panic!("not a hook error: {other:?}"),
            None => None,
        }
    }

    #[test]
    fn a_hook_refuses_by_returning_an_error() {
        assert_eq!(
            refusal(serde_json::json!({
                "error": {"http_code": 403, "message": "only members may sign in"}
            })),
            Some((403, "unknown", "only members may sign in".to_string()))
        );
    }

    #[test]
    fn a_refusal_with_no_status_of_its_own_is_a_500() {
        assert_eq!(
            refusal(serde_json::json!({"error": {"message": "not today"}})),
            Some((500, "unexpected_failure", "not today".to_string()))
        );
        // A status no client could read is the same case.
        assert_eq!(
            refusal(serde_json::json!({"error": {"http_code": 99, "message": "eh"}}))
                .expect("a refusal")
                .0,
            500
        );
    }

    #[test]
    fn what_is_not_shaped_like_a_refusal_is_not_one() {
        // Every one of these is a hook that returned claims with
        // something called error among them, and upstream signs them.
        for out in [
            serde_json::json!({"claims": {}}),
            serde_json::json!({"error": "no"}),
            serde_json::json!({"error": {}}),
            serde_json::json!({"error": {"message": ""}}),
            serde_json::json!({"error": {"message": 42}}),
            serde_json::json!({"error": {"message": "no", "http_code": "403"}}),
        ] {
            assert_eq!(refusal(out.clone()), None, "{out}");
        }
    }

    /// The claims of a token this server would have signed anyway,
    /// which is what a hook is handed and what it usually hands back.
    fn claims() -> serde_json::Value {
        serde_json::json!({
            "aud": "authenticated",
            "exp": 1_700_003_600,
            "iat": 1_700_000_000,
            "sub": "3f333df6-90a4-4fda-8dd3-9485d27cee36",
            "email": "person@zou.test",
            "phone": "",
            "role": "authenticated",
            "aal": "aal1",
            "session_id": "0b3c9b0e-6f3a-4c8a-9c1a-2f0b1d6a7c11",
            "is_anonymous": false,
            "app_metadata": {"provider": "email"},
            "user_metadata": {},
            "amr": [{"method": "password", "timestamp": 1_700_000_000}],
        })
    }

    #[test]
    fn the_claims_this_server_signs_conform() {
        assert_eq!(conforms(&claims()), Vec::<String>::new());
    }

    #[test]
    fn a_claim_the_token_cannot_do_without_is_named() {
        let mut claims = claims();
        claims.as_object_mut().expect("an object").remove("role");
        assert_eq!(conforms(&claims), vec!["(root): role is required"]);
    }

    #[test]
    fn everything_wrong_is_said_at_once_and_in_the_schemas_order() {
        // One pass, one message, in the order the schema lists them:
        // what is missing first, then what is the wrong type.
        let mut claims = claims();
        let map = claims.as_object_mut().expect("an object");
        map.remove("aal");
        map.remove("sub");
        map.insert("exp".to_string(), serde_json::json!("tomorrow"));
        map.insert("aud".to_string(), serde_json::json!(1));
        assert_eq!(
            conforms(&claims),
            vec![
                "(root): sub is required",
                "(root): aal is required",
                "aud: Invalid type. Expected: string, array, but got: integer",
                "exp: Invalid type. Expected: integer, but got: string",
            ]
        );
    }

    #[test]
    fn a_claim_the_schema_says_nothing_about_is_left_alone() {
        // A claim the project made up is the whole point of the hook,
        // and is_anonymous is required and typeless upstream, which
        // this end does not tighten.
        let mut claims = claims();
        let map = claims.as_object_mut().expect("an object");
        map.insert("plan".to_string(), serde_json::json!({"tier": "gold"}));
        map.insert("is_anonymous".to_string(), serde_json::json!("maybe"));
        map.remove("user_metadata");
        assert_eq!(conforms(&claims), Vec::<String>::new());
    }

    #[test]
    fn a_whole_number_is_an_integer_however_it_was_written() {
        let mut claims = claims();
        claims["exp"] = serde_json::json!(1_700_003_600.0);
        assert_eq!(conforms(&claims), Vec::<String>::new());
        claims["exp"] = serde_json::json!(1_700_003_600.5);
        assert_eq!(
            conforms(&claims),
            vec!["exp: Invalid type. Expected: integer, but got: number"]
        );
    }

    #[test]
    fn an_amr_entry_is_a_method_or_the_name_of_one() {
        let mut claims = claims();
        claims["amr"] = serde_json::json!(["password", {"method": "totp"}]);
        assert_eq!(conforms(&claims), Vec::<String>::new());
        claims["amr"] = serde_json::json!(["password", 1, null]);
        assert_eq!(
            conforms(&claims),
            vec![
                "amr.1: Must validate at least one schema (anyOf)",
                "amr.2: Must validate at least one schema (anyOf)",
            ]
        );
        claims["amr"] = serde_json::json!("password");
        assert_eq!(
            conforms(&claims),
            vec!["amr: Invalid type. Expected: array, but got: string"]
        );
    }

    #[test]
    fn what_a_hook_is_handed_is_what_upstream_hands_it() {
        let claims = claims();
        let input = input(&claims, "password", "203.0.113.7");
        assert_eq!(input["claims"], claims);
        assert_eq!(input["user_id"], claims["sub"]);
        assert_eq!(input["authentication_method"], "password");
        assert_eq!(input["metadata"]["name"], "customize-access-token");
        assert_eq!(input["metadata"]["ip_address"], "203.0.113.7");
        assert_eq!(
            input["metadata"]["uuid"].as_str().expect("a uuid").len(),
            36
        );
    }

    #[test]
    fn an_address_nobody_knows_is_left_out_rather_than_made_up() {
        let input = input(&claims(), "token_refresh", "");
        assert_eq!(input["metadata"].get("ip_address"), None);
        assert_eq!(input["authentication_method"], "token_refresh");
    }

    #[test]
    fn the_time_is_the_shape_go_marshals_one_into() {
        assert_eq!(stamp(0), "1970-01-01T00:00:00Z");
        assert_eq!(stamp(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(stamp(1_700_000_000 + 86_399), "2023-11-15T22:13:19Z");
    }

    #[test]
    fn the_stamp_on_a_call_is_a_version_4_uuid() {
        let uuid = uuid4();
        assert_eq!(uuid.len(), 36);
        let dashes: Vec<usize> = uuid.match_indices('-').map(|(at, _)| at).collect();
        assert_eq!(dashes, vec![8, 13, 18, 23]);
        assert_eq!(uuid.chars().nth(14), Some('4'), "{uuid}");
        assert!(
            "89ab".contains(uuid.chars().nth(19).expect("a variant")),
            "{uuid}"
        );
        assert!(uuid.chars().all(|c| c == '-' || c.is_ascii_hexdigit()));
        // Two calls stamp two different calls, which is the only thing
        // the uuid is for.
        assert_ne!(uuid, uuid4());
    }
}
