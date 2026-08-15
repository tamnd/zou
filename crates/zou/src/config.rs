//! `supabase/config.toml`: the file a Supabase project already has,
//! read for the settings this server can honour.
//!
//! A project that runs `supabase start` today keeps its ports, its auth
//! switches and its provider credentials in one file at the top of the
//! repository. `zou dev` reads the same file, so pointing a project at
//! zou is a change of command and not a second copy of the settings.
//! Most of what is in there becomes an environment variable this binary
//! already reads, which keeps one way of configuring the server rather
//! than two: the file fills in what the environment has not already
//! said, so an explicit `ZOU_` variable still wins and a flag wins over
//! both.
//!
//! Everything the file says that zou has no answer for is collected
//! rather than ignored in silence, and `zou status` prints the list. A
//! project should be able to see which of its settings arrived and
//! which did not.
//!
//! The reader is a small TOML subset: tables, dotted keys, strings,
//! integers, floats, booleans and arrays, which is all a config.toml
//! is. `env(NAME)` values are read from the environment, the way the
//! Supabase CLI reads them.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    List(Vec<Value>),
}

impl Value {
    fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }
}

/// A comment runs to the end of the line, unless the `#` is inside a
/// string, which is why this is not a `split('#')`.
fn strip_comment(line: &str) -> &str {
    let mut quote = None;
    let mut escaped = false;
    for (i, c) in line.char_indices() {
        match quote {
            Some(q) => {
                if escaped {
                    escaped = false;
                } else if c == '\\' && q == '"' {
                    escaped = true;
                } else if c == q {
                    quote = None;
                }
            }
            None => match c {
                '"' | '\'' => quote = Some(c),
                '#' => return &line[..i],
                _ => {}
            },
        }
    }
    line
}

/// Split on top level commas, so a list of lists survives.
fn commas(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quote = None;
    let mut escaped = false;
    let mut item = String::new();
    for c in body.chars() {
        match quote {
            Some(q) => {
                if escaped {
                    escaped = false;
                } else if c == '\\' && q == '"' {
                    escaped = true;
                } else if c == q {
                    quote = None;
                }
                item.push(c);
            }
            None => match c {
                '"' | '\'' => {
                    quote = Some(c);
                    item.push(c);
                }
                '[' => {
                    depth += 1;
                    item.push(c);
                }
                ']' => {
                    depth -= 1;
                    item.push(c);
                }
                ',' if depth == 0 => {
                    out.push(std::mem::take(&mut item));
                }
                _ => item.push(c),
            },
        }
    }
    if !item.trim().is_empty() {
        out.push(item);
    }
    out
}

fn unescape(body: &str) -> String {
    let mut out = String::new();
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

fn value(raw: &str) -> Result<Value, String> {
    let raw = raw.trim();
    if let Some(rest) = raw.strip_prefix('"') {
        let body = rest
            .strip_suffix('"')
            .ok_or_else(|| format!("unterminated string {raw:?}"))?;
        return Ok(Value::Str(unescape(body)));
    }
    if let Some(rest) = raw.strip_prefix('\'') {
        let body = rest
            .strip_suffix('\'')
            .ok_or_else(|| format!("unterminated string {raw:?}"))?;
        return Ok(Value::Str(body.to_string()));
    }
    if let Some(rest) = raw.strip_prefix('[') {
        let body = rest
            .strip_suffix(']')
            .ok_or_else(|| format!("unterminated list {raw:?}"))?;
        let mut items = Vec::new();
        for item in commas(body) {
            if item.trim().is_empty() {
                continue;
            }
            items.push(value(&item)?);
        }
        return Ok(Value::List(items));
    }
    match raw {
        "true" => return Ok(Value::Bool(true)),
        "false" => return Ok(Value::Bool(false)),
        _ => {}
    }
    let digits: String = raw.chars().filter(|c| *c != '_').collect();
    if let Ok(i) = digits.parse::<i64>() {
        return Ok(Value::Int(i));
    }
    if let Ok(f) = digits.parse::<f64>() {
        return Ok(Value::Float(f));
    }
    Err(format!("value {raw:?} is not something this reader knows"))
}

/// A table header or a dotted key splits on `.`, but not on a `.`
/// inside a quoted segment.
fn segments(key: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote = None;
    for c in key.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                } else {
                    cur.push(c);
                }
            }
            None => match c {
                '"' | '\'' => quote = Some(c),
                '.' => out.push(std::mem::take(&mut cur).trim().to_string()),
                _ => cur.push(c),
            },
        }
    }
    out.push(cur.trim().to_string());
    out
}

/// Every setting in the file, by its dotted path. A repeated table,
/// `[[x]]`, numbers its entries `x[0]`, `x[1]` and so on, which no
/// setting read here uses but which keeps the reader from losing them.
pub fn parse(text: &str) -> Result<BTreeMap<String, Value>, String> {
    let mut out = BTreeMap::new();
    let mut prefix = String::new();
    let mut repeats: BTreeMap<String, usize> = BTreeMap::new();
    let mut pending: Option<(String, String)> = None;
    for (n, line) in text.lines().enumerate() {
        let no = n + 1;
        let line = strip_comment(line).trim();
        if let Some((key, mut body)) = pending.take() {
            body.push(' ');
            body.push_str(line);
            if body.matches('[').count() > body.matches(']').count() {
                pending = Some((key, body));
                continue;
            }
            let v = value(&body).map_err(|e| format!("line {no}: {e}"))?;
            out.insert(key, v);
            continue;
        }
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("[[") {
            let name = rest
                .strip_suffix("]]")
                .ok_or_else(|| format!("line {no}: unterminated table header"))?;
            let name = segments(name).join(".");
            let seen = repeats.entry(name.clone()).or_insert(0);
            prefix = format!("{name}[{seen}]");
            *seen += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix('[') {
            let name = rest
                .strip_suffix(']')
                .ok_or_else(|| format!("line {no}: unterminated table header"))?;
            prefix = segments(name).join(".");
            continue;
        }
        let (key, body) = line
            .split_once('=')
            .ok_or_else(|| format!("line {no}: {line:?} is neither a header nor a setting"))?;
        let key = segments(key).join(".");
        let key = if prefix.is_empty() {
            key
        } else {
            format!("{prefix}.{key}")
        };
        let body = body.trim().to_string();
        if body.matches('[').count() > body.matches(']').count() {
            pending = Some((key, body));
            continue;
        }
        let v = value(&body).map_err(|e| format!("line {no}: {e}"))?;
        out.insert(key, v);
    }
    if let Some((key, _)) = pending {
        return Err(format!("{key} never finished its list"));
    }
    Ok(out)
}

/// `env(NAME)` is the Supabase CLI's way of keeping a secret out of a
/// file that is committed, and it means the same thing here.
fn expand(v: &str, var: &dyn Fn(&str) -> Option<String>) -> String {
    match v.strip_prefix("env(").and_then(|r| r.strip_suffix(')')) {
        Some(name) => var(name.trim()).unwrap_or_default(),
        None => v.to_string(),
    }
}

/// A boolean setting, the variable it becomes, and whether the file
/// says the opposite of what the variable says.
const BOOLS: &[(&str, &str, bool)] = &[
    ("auth.enable_signup", "ZOU_DISABLE_SIGNUP", true),
    (
        "auth.enable_anonymous_sign_ins",
        "ZOU_EXTERNAL_ANONYMOUS_USERS_ENABLED",
        false,
    ),
    (
        "auth.enable_manual_linking",
        "ZOU_SECURITY_MANUAL_LINKING_ENABLED",
        false,
    ),
    (
        "auth.email.enable_signup",
        "ZOU_EXTERNAL_EMAIL_ENABLED",
        false,
    ),
    (
        "auth.email.enable_confirmations",
        "ZOU_MAILER_AUTOCONFIRM",
        true,
    ),
    (
        "auth.sms.enable_signup",
        "ZOU_EXTERNAL_PHONE_ENABLED",
        false,
    ),
    ("auth.sms.enable_confirmations", "ZOU_SMS_AUTOCONFIRM", true),
    (
        "auth.mfa.totp.enroll_enabled",
        "ZOU_MFA_TOTP_ENROLL_ENABLED",
        false,
    ),
    (
        "auth.mfa.totp.verify_enabled",
        "ZOU_MFA_TOTP_VERIFY_ENABLED",
        false,
    ),
];

/// A number, and the variable it becomes. GoTrue's budgets are per
/// hour except the two the CLI names per minute, and the CLI's numbers
/// are the ones a project already tuned, so they go over as they are.
const INTS: &[(&str, &str)] = &[
    (
        "auth.mfa.max_enrolled_factors",
        "ZOU_MFA_MAX_ENROLLED_FACTORS",
    ),
    (
        "auth.mfa.max_verified_factors",
        "ZOU_MFA_MAX_VERIFIED_FACTORS",
    ),
    ("auth.rate_limit.email_sent", "ZOU_RATE_LIMIT_EMAIL_SENT"),
    ("auth.rate_limit.sms_sent", "ZOU_RATE_LIMIT_SMS_SENT"),
    (
        "auth.rate_limit.anonymous_users",
        "ZOU_RATE_LIMIT_ANONYMOUS_USERS",
    ),
    (
        "auth.rate_limit.token_refresh",
        "ZOU_RATE_LIMIT_TOKEN_REFRESH",
    ),
    (
        "auth.rate_limit.token_verifications",
        "ZOU_RATE_LIMIT_VERIFY",
    ),
];

/// A string, and the variable it becomes.
const STRINGS: &[(&str, &str)] = &[
    ("auth.sms.twilio.account_sid", "ZOU_SMS_TWILIO_ACCOUNT_SID"),
    ("auth.sms.twilio.auth_token", "ZOU_SMS_TWILIO_AUTH_TOKEN"),
    (
        "auth.sms.twilio.message_service_sid",
        "ZOU_SMS_TWILIO_MESSAGE_SERVICE_SID",
    ),
    (
        "auth.sms.messagebird.access_key",
        "ZOU_SMS_MESSAGEBIRD_ACCESS_KEY",
    ),
    (
        "auth.sms.messagebird.originator",
        "ZOU_SMS_MESSAGEBIRD_ORIGINATOR",
    ),
];

/// Settings that are read somewhere other than the environment, so
/// finding them in the file is not finding them unread.
const DIRECT: &[&str] = &[
    "project_id",
    "api.enabled",
    "api.port",
    "api.schemas",
    "auth.site_url",
    "db.port",
    "db.seed.enabled",
    "db.seed.sql_paths",
    "storage.s3_protocol.enabled",
];

/// The `[functions.<name>]` blocks and the `[edge_runtime]` one, and
/// every key they accounted for.
///
/// A key inside a block this reader does not know goes unread and is
/// printed by `zou status`, the same as anywhere else in the file, so a
/// project asking for something that is not honoured hears about it
/// rather than watching it be ignored.
fn layout(
    table: &BTreeMap<String, Value>,
    var: &dyn Fn(&str) -> Option<String>,
) -> (zou_functions::Layout, Vec<String>) {
    let mut out = zou_functions::Layout::default();
    let mut read = Vec::new();
    if let Some(word) = table.get("edge_runtime.policy").and_then(Value::as_str)
        && let Some(policy) = zou_functions::Policy::named(word)
    {
        out.policy = policy;
        read.push("edge_runtime.policy".to_string());
    }
    if let Some(port) = table
        .get("edge_runtime.inspector_port")
        .and_then(Value::as_int)
        .and_then(|i| u16::try_from(i).ok())
    {
        out.inspector_port = Some(port);
        read.push("edge_runtime.inspector_port".to_string());
    }
    // `deno_version` is which Deno the CLI's runtime is, and it is one
    // of two numbers: 1 pulls the older image and 2 is what the current
    // one already runs. This server has one runtime and it is the
    // second, so 2 is read and anything else is left unread and named
    // by `zou status`, the same as a policy nothing knows. What a
    // function is told the runtime is, `Deno.version`, says the same
    // thing from the other side.
    if table
        .get("edge_runtime.deno_version")
        .and_then(Value::as_int)
        == Some(2)
    {
        read.push("edge_runtime.deno_version".to_string());
    }
    // `enabled` under `[edge_runtime]` is the container the CLI starts,
    // which this server is instead of, so it is read and then nothing
    // happens: a project that switched the runtime off should not be
    // told the setting went unheard.
    if table.contains_key("edge_runtime.enabled") {
        read.push("edge_runtime.enabled".to_string());
    }
    // `[edge_runtime.secrets]` is what a project's functions see on top
    // of the four this server sets, and it is where `env(NAME)` earns
    // its keep: the name of a variable is committed and the secret is
    // not. A value that resolves to nothing is a variable the
    // environment does not have, and upstream leaves that one out
    // rather than handing a function an empty string that looks like a
    // key.
    for (key, value) in table {
        let Some(name) = key.strip_prefix("edge_runtime.secrets.") else {
            continue;
        };
        let Some(raw) = value.as_str() else {
            continue;
        };
        read.push(key.clone());
        let secret = expand(raw, var);
        if !secret.is_empty() {
            out.secrets.insert(name.to_string(), secret);
        }
    }
    for (key, value) in table {
        let Some(rest) = key.strip_prefix("functions.") else {
            continue;
        };
        let Some((name, leaf)) = rest.rsplit_once('.') else {
            continue;
        };
        // A dotted key deeper than one level is not a setting this
        // reader has, and treating it as one would mean inventing a
        // function called `hello.something`.
        if name.contains('.') {
            continue;
        }
        let settings = out.settings.entry(name.to_string()).or_default();
        let taken = match leaf {
            "enabled" => value.as_bool().map(|b| settings.enabled = b).is_some(),
            "verify_jwt" => value.as_bool().map(|b| settings.verify_jwt = b).is_some(),
            "import_map" => value
                .as_str()
                .map(|s| settings.import_map = Some(s.to_string()))
                .is_some(),
            "entrypoint" => value
                .as_str()
                .map(|s| settings.entrypoint = Some(s.to_string()))
                .is_some(),
            "static_files" => match value {
                Value::List(items) => {
                    settings.static_files = items
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect();
                    true
                }
                _ => false,
            },
            _ => false,
        };
        if taken {
            read.push(key.clone());
        }
    }
    (out, read)
}

/// A Supabase project, as far as this server is concerned.
#[derive(Debug, Default)]
pub struct Project {
    /// The file this came from.
    pub path: PathBuf,
    pub id: Option<String>,
    /// The api port, and None when the file switches the api off.
    pub api: Option<u16>,
    pub db: Option<u16>,
    /// What PostgREST is told to serve, in the file's order, because
    /// the first is what a request that names no schema gets.
    pub schemas: Vec<String>,
    pub site_url: Option<String>,
    /// Whether the S3 protocol endpoint answers, which the file calls
    /// `[storage.s3_protocol] enabled`. On unless the file says
    /// otherwise, the way the CLI generates it.
    pub s3: bool,
    /// Settings that become environment variables, in the order they
    /// are listed above.
    pub env: Vec<(String, String)>,
    /// Settings the file has and this server has no answer for.
    pub unread: Vec<String>,
    /// What `zou db reset` runs after the migrations, relative to the
    /// directory the config lives in, and empty when the file switches
    /// seeding off.
    pub seed: Vec<String>,
    /// What the file says about edge functions: a block per function
    /// and the `[edge_runtime]` settings over all of them.
    ///
    /// This one does not become an environment variable, because the
    /// thing that reads it is a directory listing rather than a
    /// server: which functions are served is a question about the disk
    /// beside the file, and the answer is a list rather than a setting.
    pub functions: zou_functions::Layout,
}

impl Project {
    /// The directory the file lives in, which is what a relative path
    /// inside it is relative to.
    pub fn dir(&self) -> PathBuf {
        self.path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    }

    /// Where a project keeps its migrations, which is next to the
    /// config file whatever the file is called.
    pub fn migrations(&self) -> PathBuf {
        self.dir().join("migrations")
    }

    /// Read a config.toml, taking `env(NAME)` values from the process
    /// environment.
    pub fn read(path: &Path) -> Result<Project, String> {
        let text =
            std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let var = |name: &str| std::env::var(name).ok();
        let mut project = Project::from_table(&parse(&text)?, &var);
        project.path = path.to_path_buf();
        Ok(project)
    }

    fn from_table(
        table: &BTreeMap<String, Value>,
        var: &dyn Fn(&str) -> Option<String>,
    ) -> Project {
        let mut env = Vec::new();
        let mut read: Vec<String> = DIRECT.iter().map(|k| k.to_string()).collect();
        for (key, name, flip) in BOOLS {
            if let Some(b) = table.get(*key).and_then(Value::as_bool) {
                read.push(key.to_string());
                env.push((name.to_string(), (b != *flip).to_string()));
            }
        }
        for (key, name) in INTS {
            if let Some(i) = table.get(*key).and_then(Value::as_int) {
                read.push(key.to_string());
                env.push((name.to_string(), i.to_string()));
            }
        }
        for (key, name) in STRINGS {
            if let Some(s) = table.get(*key).and_then(Value::as_str) {
                read.push(key.to_string());
                let s = expand(s, var);
                if !s.is_empty() {
                    env.push((name.to_string(), s));
                }
            }
        }
        // A text provider is named rather than switched on, so a file
        // with credentials for one that is off stays off.
        for provider in ["twilio", "messagebird"] {
            let key = format!("auth.sms.{provider}.enabled");
            if let Some(true) = table.get(&key).and_then(Value::as_bool) {
                env.push(("ZOU_SMS_PROVIDER".into(), provider.into()));
            }
            if table.contains_key(&key) {
                read.push(key);
            }
        }
        // Social providers are whatever the file names, so a provider
        // added upstream arrives here without a code change, as long as
        // this server knows how to talk to it.
        let mut providers: Vec<String> = Vec::new();
        for key in table.keys() {
            if let Some(rest) = key.strip_prefix("auth.external.")
                && let Some((name, _)) = rest.split_once('.')
                && !providers.iter().any(|p| p == name)
            {
                providers.push(name.to_string());
            }
        }
        for name in providers {
            let prefix = format!("auth.external.{name}");
            let known = zou_server::oauth::Provider::named(&name).is_some();
            let on = table
                .get(&format!("{prefix}.enabled"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            for (key, v) in table.range(prefix.clone()..).take_while(|(k, _)| {
                k.strip_prefix(&prefix)
                    .is_some_and(|r| r.starts_with('.') && !r[1..].contains('.'))
            }) {
                let leaf = key.rsplit('.').next().unwrap_or_default();
                let upper = name.to_uppercase();
                let var_name = match leaf {
                    "client_id" => format!("ZOU_EXTERNAL_{upper}_CLIENT_ID"),
                    "secret" => format!("ZOU_EXTERNAL_{upper}_SECRET"),
                    "url" => format!("ZOU_EXTERNAL_{upper}_URL"),
                    "enabled" => {
                        if known {
                            read.push(key.clone());
                        }
                        continue;
                    }
                    _ => continue,
                };
                if !known || !on {
                    continue;
                }
                read.push(key.clone());
                if let Some(s) = v.as_str() {
                    let s = expand(s, var);
                    if !s.is_empty() {
                        env.push((var_name, s));
                    }
                }
            }
        }
        let api_on = table
            .get("api.enabled")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let port = |key: &str| {
            table
                .get(key)
                .and_then(Value::as_int)
                .and_then(|i| u16::try_from(i).ok())
        };
        let schemas = match table.get("api.schemas") {
            Some(Value::List(items)) => items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            _ => Vec::new(),
        };
        // Seeding is on unless the file says otherwise, and the CLI's
        // own default path is the one a project that never touched the
        // setting has.
        let seed = match table.get("db.seed.enabled").and_then(Value::as_bool) {
            Some(false) => Vec::new(),
            _ => match table.get("db.seed.sql_paths") {
                Some(Value::List(items)) => items
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect(),
                _ => vec!["./seed.sql".to_string()],
            },
        };
        let (functions, function_keys) = layout(table, var);
        read.extend(function_keys);
        let unread = table
            .keys()
            .filter(|k| !read.iter().any(|r| r == *k))
            .cloned()
            .collect();
        Project {
            path: PathBuf::new(),
            id: table
                .get("project_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            api: api_on.then(|| port("api.port").unwrap_or(54321)),
            db: port("db.port"),
            schemas,
            site_url: table
                .get("auth.site_url")
                .and_then(Value::as_str)
                .map(str::to_string),
            // On unless the file switches it off, which is what a file
            // that never mentions it means and what the generated one
            // says.
            s3: table
                .get("storage.s3_protocol.enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            env,
            unread,
            seed,
            functions,
        }
    }

    /// Put the file's settings in the environment, without stepping on
    /// anything the caller set there first. Returns what it set, for
    /// the log line.
    ///
    /// Safety: called before this process starts a thread, the same
    /// place every other set_var in this binary is called from.
    pub fn export(&self) -> Vec<&str> {
        let mut set = Vec::new();
        for (name, v) in &self.env {
            if std::env::var_os(name).is_some_and(|v| !v.is_empty()) {
                continue;
            }
            unsafe { std::env::set_var(name, v) };
            set.push(name.as_str());
        }
        set
    }
}

/// The project a command was run inside, or the one it was pointed
/// at. A file that was named and is not there is an error, a file
/// nobody named and nobody has is simply None.
pub fn locate(explicit: Option<&Path>) -> Result<Option<Project>, String> {
    let path = match explicit {
        Some(path) => Some(path.to_path_buf()),
        None => find(&std::env::current_dir().map_err(|e| format!("cwd: {e}"))?),
    };
    match path {
        Some(path) => Project::read(&path).map(Some),
        None => Ok(None),
    }
}

/// The url a client on this machine reaches a `zou dev` cluster with.
/// Local connections are trust, so the password is there for the
/// clients that insist on one and is not a secret.
pub fn local_db_url(port: u16) -> String {
    format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres")
}

/// The S3 pair a `supabase start` project answers to, which is fixed
/// rather than generated and is the same on every machine.
///
/// It is a fixture and not a secret, the same way the local anon key
/// printed in every Supabase tutorial is: it opens a database on
/// loopback that a person started themselves. `zou dev` answers to it
/// so that a project which already has these three in an `.env` keeps
/// working when the command in front of it changes.
pub const LOCAL_S3_ACCESS_KEY: &str = "625729a08b95bf1b7ff351a663f3a23c";
pub const LOCAL_S3_SECRET_KEY: &str =
    "850181e4652dd023b7a98c58ae0d2d34bd487ee0cc3254aed6eda37307425907";
/// Where a local project says it is, which a client signs into and
/// which a bucket answers when asked for its location.
pub const LOCAL_S3_REGION: &str = "local";

/// The pair the dev loop's S3 endpoint is asked with: the local one
/// above, unless the environment names another.
///
/// Overridable because the pair being public is only harmless while the
/// thing behind it is on loopback, and a `zou dev` reachable from
/// anywhere else is a `zou dev` that wants its own.
pub fn local_s3() -> zou_server::s3::Credentials {
    let var = |name: &str, fallback: &str| match std::env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => fallback.to_string(),
    };
    zou_server::s3::Credentials {
        access: var("ZOU_S3_ACCESS_KEY", LOCAL_S3_ACCESS_KEY),
        secret: var("ZOU_S3_SECRET_KEY", LOCAL_S3_SECRET_KEY),
        region: var("ZOU_S3_REGION", LOCAL_S3_REGION),
    }
}

/// Look for a project config, starting at `from` and walking up. Both
/// the top of a project, which holds `supabase/config.toml`, and the
/// `supabase` directory itself are places a person runs a command from.
pub fn find(from: &Path) -> Option<PathBuf> {
    let mut dir = Some(from);
    while let Some(here) = dir {
        for candidate in [here.join("supabase/config.toml"), here.join("config.toml")] {
            if candidate.is_file() {
                return Some(candidate);
            }
        }
        dir = here.parent();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What `supabase init` writes, trimmed to the settings this file
    /// has something to say about.
    const SAMPLE: &str = r#"
# A comment, and a [bracket] inside one.
project_id = "demo"

[api]
enabled = true
port = 54321
schemas = ["conformance", "public"]
extra_search_path = ["public", "extensions"]
max_rows = 1000

[db]
port = 54322
shadow_port = 54320

[auth]
enabled = true
site_url = "http://127.0.0.1:3000"
additional_redirect_urls = [
  "https://127.0.0.1:3000",
]
jwt_expiry = 3600
enable_signup = true
enable_anonymous_sign_ins = true

[auth.email]
enable_signup = true
enable_confirmations = true

[auth.sms]
enable_signup = true
enable_confirmations = false

[auth.sms.twilio]
enabled = true
account_sid = "AC123"
auth_token = "env(TWILIO_TOKEN)"

[auth.mfa]
max_enrolled_factors = 7

[auth.mfa.totp]
enroll_enabled = false
verify_enabled = true

[auth.rate_limit]
email_sent = 4

[auth.external.github]
enabled = true
client_id = "gh-client"
secret = "env(GH_SECRET)"

[auth.external.gitlab]
enabled = true
client_id = "gl-client"

[studio]
port = 54323

[edge_runtime]
enabled = true
policy = "oneshot"
inspector_port = 8083
deno_version = 2

[functions.hello]
verify_jwt = false
import_map = "./functions/import_map.json"

[functions.quiet]
enabled = false

[functions.other]
entrypoint = "./functions/other/main.ts"
static_files = ["./functions/other/index.html"]
"#;

    fn table() -> BTreeMap<String, Value> {
        parse(SAMPLE).unwrap()
    }

    fn project() -> Project {
        let var = |name: &str| match name {
            "TWILIO_TOKEN" => Some("shhh".to_string()),
            "GH_SECRET" => Some("hunter2".to_string()),
            _ => None,
        };
        Project::from_table(&table(), &var)
    }

    fn env_of(p: &Project, name: &str) -> Option<String> {
        p.env
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    }

    #[test]
    fn the_reader_takes_a_config_toml_apart() {
        let t = table();
        assert_eq!(t.get("project_id"), Some(&Value::Str("demo".into())));
        assert_eq!(t.get("api.port"), Some(&Value::Int(54321)));
        assert_eq!(
            t.get("auth.email.enable_confirmations"),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            t.get("api.schemas"),
            Some(&Value::List(vec![
                Value::Str("conformance".into()),
                Value::Str("public".into())
            ]))
        );
        assert_eq!(
            t.get("auth.additional_redirect_urls"),
            Some(&Value::List(vec![Value::Str(
                "https://127.0.0.1:3000".into()
            )])),
            "a list that runs over several lines is still one list"
        );
        assert!(!t.contains_key("A comment"), "comments say nothing");
    }

    #[test]
    fn the_functions_blocks_become_a_layout() {
        let p = project();
        assert_eq!(p.functions.policy, zou_functions::Policy::OneShot);
        assert_eq!(p.functions.inspector_port, Some(8083));
        let hello = p.functions.settings("hello");
        assert!(!hello.verify_jwt, "one block does not move the others");
        assert!(hello.enabled, "a block that says nothing about it is on");
        assert_eq!(
            hello.import_map.as_deref(),
            Some("./functions/import_map.json")
        );
        assert!(!p.functions.settings("quiet").enabled);
        let other = p.functions.settings("other");
        assert_eq!(
            other.entrypoint.as_deref(),
            Some("./functions/other/main.ts")
        );
        assert_eq!(other.static_files, ["./functions/other/index.html"]);
        // A function the file never mentions gets upstream's defaults,
        // which is served and verified.
        let never = p.functions.settings("never-written-down");
        assert!(never.enabled && never.verify_jwt);
    }

    #[test]
    fn a_setting_the_functions_blocks_have_and_this_reader_does_not_is_unread() {
        let table = parse(
            "[functions.hello]\nverify_jwt = false\nsomething_else = 1\n[edge_runtime]\npolicy = \"per_worker\"\n",
        )
        .unwrap();
        let p = Project::from_table(&table, &|_| None);
        assert!(!p.functions.settings("hello").verify_jwt);
        assert_eq!(p.unread, ["functions.hello.something_else"]);
    }

    #[test]
    fn a_policy_that_is_not_one_of_the_two_is_not_taken() {
        let table = parse("[edge_runtime]\npolicy = \"whenever\"\n").unwrap();
        let p = Project::from_table(&table, &|_| None);
        assert_eq!(
            p.functions.policy,
            zou_functions::Policy::PerWorker,
            "the default, rather than a word nothing knows"
        );
        assert_eq!(p.unread, ["edge_runtime.policy"]);
    }

    #[test]
    fn the_deno_this_runtime_is_is_the_only_one_it_can_be_told_to_be() {
        let two = parse("[edge_runtime]\ndeno_version = 2\n").unwrap();
        assert!(
            !Project::from_table(&two, &|_| None)
                .unread
                .iter()
                .any(|k| k == "edge_runtime.deno_version"),
            "2 is what this runtime is"
        );
        for asked in ["1", "3", "\"two\""] {
            let table = parse(&format!("[edge_runtime]\ndeno_version = {asked}\n")).unwrap();
            assert_eq!(
                Project::from_table(&table, &|_| None).unread,
                ["edge_runtime.deno_version"],
                "deno_version = {asked} is not something this server can be"
            );
        }
    }

    #[test]
    fn the_secrets_block_is_read_and_env_is_resolved() {
        let table = parse(
            "[edge_runtime.secrets]\nplain = \"in the file\"\ntoken = \"env(TWILIO_TOKEN)\"\nabsent = \"env(NOT_SET)\"\n",
        )
        .unwrap();
        let p = Project::from_table(&table, &|name| match name {
            "TWILIO_TOKEN" => Some("shhh".to_string()),
            _ => None,
        });
        assert_eq!(
            p.functions.secrets.get("plain").map(String::as_str),
            Some("in the file")
        );
        assert_eq!(
            p.functions.secrets.get("token").map(String::as_str),
            Some("shhh"),
            "the name is in the file and the secret is in the environment"
        );
        assert!(
            !p.functions.secrets.contains_key("absent"),
            "a variable the environment does not have is left out, not set empty"
        );
        assert!(
            p.unread.is_empty(),
            "all three arrived, including the one with nothing behind it: {:?}",
            p.unread
        );
    }

    #[test]
    fn a_broken_file_says_which_line() {
        assert!(parse("[api\nport = 1").unwrap_err().contains("line 1"));
        assert!(parse("[api]\nport 1").unwrap_err().contains("line 2"));
        assert!(
            parse("[api]\nport = \"open")
                .unwrap_err()
                .contains("line 2")
        );
        assert!(parse("x = [1, 2").unwrap_err().contains("never finished"));
    }

    #[test]
    fn ports_and_schemas_come_over_directly() {
        let p = project();
        assert_eq!(p.id.as_deref(), Some("demo"));
        assert_eq!(p.api, Some(54321));
        assert_eq!(p.db, Some(54322));
        assert_eq!(p.schemas, ["conformance", "public"]);
        assert_eq!(p.site_url.as_deref(), Some("http://127.0.0.1:3000"));
    }

    #[test]
    fn an_api_that_is_off_has_no_port() {
        let mut t = table();
        t.insert("api.enabled".into(), Value::Bool(false));
        let p = Project::from_table(&t, &|_| None);
        assert_eq!(p.api, None);
    }

    #[test]
    fn switches_arrive_as_the_variables_this_binary_reads() {
        let p = project();
        assert_eq!(env_of(&p, "ZOU_DISABLE_SIGNUP").as_deref(), Some("false"));
        assert_eq!(
            env_of(&p, "ZOU_EXTERNAL_ANONYMOUS_USERS_ENABLED").as_deref(),
            Some("true")
        );
        assert_eq!(
            env_of(&p, "ZOU_MAILER_AUTOCONFIRM").as_deref(),
            Some("false"),
            "asking for confirmations is asking for autoconfirm to stop"
        );
        assert_eq!(env_of(&p, "ZOU_SMS_AUTOCONFIRM").as_deref(), Some("true"));
        assert_eq!(
            env_of(&p, "ZOU_MFA_TOTP_ENROLL_ENABLED").as_deref(),
            Some("false")
        );
        assert_eq!(
            env_of(&p, "ZOU_MFA_MAX_ENROLLED_FACTORS").as_deref(),
            Some("7")
        );
        assert_eq!(
            env_of(&p, "ZOU_RATE_LIMIT_EMAIL_SENT").as_deref(),
            Some("4")
        );
    }

    #[test]
    fn secrets_come_from_the_environment_the_file_names() {
        let p = project();
        assert_eq!(env_of(&p, "ZOU_SMS_PROVIDER").as_deref(), Some("twilio"));
        assert_eq!(
            env_of(&p, "ZOU_SMS_TWILIO_AUTH_TOKEN").as_deref(),
            Some("shhh")
        );
        assert_eq!(
            env_of(&p, "ZOU_EXTERNAL_GITHUB_SECRET").as_deref(),
            Some("hunter2")
        );
        assert_eq!(
            env_of(&p, "ZOU_EXTERNAL_GITHUB_CLIENT_ID").as_deref(),
            Some("gh-client")
        );
    }

    #[test]
    fn a_provider_this_server_cannot_talk_to_is_left_unread() {
        let p = project();
        assert_eq!(env_of(&p, "ZOU_EXTERNAL_GITLAB_CLIENT_ID"), None);
        assert!(
            p.unread
                .iter()
                .any(|k| k == "auth.external.gitlab.client_id"),
            "and it says so rather than dropping it, {:?}",
            p.unread
        );
    }

    #[test]
    fn a_provider_that_is_off_keeps_its_credentials_to_itself() {
        let mut t = table();
        t.insert("auth.external.github.enabled".into(), Value::Bool(false));
        let p = Project::from_table(&t, &|_| Some("x".into()));
        assert_eq!(env_of(&p, "ZOU_EXTERNAL_GITHUB_CLIENT_ID"), None);
    }

    #[test]
    fn a_project_that_never_mentioned_the_seed_still_has_the_one_the_cli_gives_it() {
        let none = |_: &str| None;
        let project = Project::from_table(&parse("project_id = \"x\"").unwrap(), &none);
        assert_eq!(project.seed, ["./seed.sql"]);
        let table = parse("[db.seed]\nsql_paths = [\"./a.sql\", \"./b.sql\"]").unwrap();
        assert_eq!(
            Project::from_table(&table, &none).seed,
            ["./a.sql", "./b.sql"]
        );
        let off = parse("[db.seed]\nenabled = false\nsql_paths = [\"./a.sql\"]").unwrap();
        assert!(
            Project::from_table(&off, &none).seed.is_empty(),
            "switched off means nothing runs, whatever the paths say"
        );
    }

    #[test]
    fn migrations_sit_next_to_the_file_that_names_the_project() {
        let project = Project {
            path: PathBuf::from("/home/me/app/supabase/config.toml"),
            ..Project::from_table(&parse("project_id = \"x\"").unwrap(), &|_: &str| None)
        };
        assert_eq!(project.dir(), PathBuf::from("/home/me/app/supabase"));
        assert_eq!(
            project.migrations(),
            PathBuf::from("/home/me/app/supabase/migrations")
        );
    }

    #[test]
    fn what_is_not_read_is_named() {
        let p = project();
        for key in [
            "api.max_rows",
            "auth.jwt_expiry",
            "auth.additional_redirect_urls",
            "studio.port",
            "db.shadow_port",
        ] {
            assert!(p.unread.iter().any(|k| k == key), "{key} is not read yet");
        }
        for key in ["api.port", "auth.email.enable_signup", "project_id"] {
            assert!(!p.unread.iter().any(|k| k == key), "{key} is read");
        }
    }

    #[test]
    fn the_s3_endpoint_is_on_unless_the_file_turns_it_off() {
        // What `supabase init` writes, which is the section and the one
        // key under it.
        let on = Project::from_table(
            &parse("[storage.s3_protocol]\nenabled = true\n").unwrap(),
            &|_| None,
        );
        assert!(on.s3);
        assert!(
            !on.unread.iter().any(|k| k == "storage.s3_protocol.enabled"),
            "and it is read rather than named as a setting with no answer"
        );
        let off = Project::from_table(
            &parse("[storage.s3_protocol]\nenabled = false\n").unwrap(),
            &|_| None,
        );
        assert!(!off.s3);
        assert!(
            Project::from_table(&parse("project_id = \"demo\"\n").unwrap(), &|_| None).s3,
            "a file that never mentions it has one"
        );
    }

    #[test]
    fn a_repeated_table_keeps_every_entry() {
        let t = parse("[[bucket]]\nname = \"a\"\n[[bucket]]\nname = \"b\"\n").unwrap();
        assert_eq!(t.get("bucket[0].name"), Some(&Value::Str("a".into())));
        assert_eq!(t.get("bucket[1].name"), Some(&Value::Str("b".into())));
    }

    #[test]
    fn find_walks_up_from_where_a_person_stands() {
        let dir = tempfile::tempdir().unwrap();
        let supabase = dir.path().join("supabase");
        std::fs::create_dir_all(supabase.join("migrations")).unwrap();
        std::fs::write(supabase.join("config.toml"), "project_id = \"x\"\n").unwrap();
        let want = supabase.join("config.toml");
        assert_eq!(find(dir.path()), Some(want.clone()));
        assert_eq!(find(&supabase), Some(want.clone()));
        assert_eq!(find(&supabase.join("migrations")), Some(want));
    }
}
