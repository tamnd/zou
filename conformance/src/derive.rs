//! Turning PostgREST's own test tree into a suite.
//!
//! The starter REST suite is eighty two questions somebody thought of.
//! That is a fine start and a bad finish, because the questions somebody
//! thinks of are the ones they already know the answer to. PostgREST has
//! a thousand of its own, written by the people who wrote the thing, and
//! they are the questions that matter.
//!
//! What is taken from upstream is the questions and the fixtures, never
//! the expectations. Upstream's expectations are written for upstream's
//! configuration, which rolls every transaction back and sets app
//! settings this harness does not, so they are answers to a slightly
//! different question. The answers come from recording the real binary
//! under the configuration in versions.json, the same as every other
//! suite here.
//!
//! This runs by hand, not in CI. Bumping PostgREST in versions.json
//! means running it again, and the diff it produces is upstream changing
//! its test suite, which is worth reading.

use std::collections::BTreeMap;
use std::path::Path;

use crate::suite::{Case, Cases};

/// The spec files that run against upstream's default configuration,
/// read off the `specs` list in `test/spec/Main.hs`. A file that needs a
/// server flag is answering a question about that flag rather than about
/// the REST surface, and a target configured differently would differ
/// for a reason that is not interesting.
///
/// `only` is for the files that export more than one spec, where just
/// one of them runs on the default app.
struct Spec {
    file: &'static str,
    feature: &'static str,
    only: Option<&'static str>,
}

const SPECS: &[Spec] = &[
    spec("Feature/Query/QuerySpec.hs", "query"),
    spec("Feature/Query/AndOrParamsSpec.hs", "andor"),
    spec("Feature/Query/ComputedRelsSpec.hs", "computed"),
    spec("Feature/Query/DeleteSpec.hs", "delete"),
    spec("Feature/Query/EmbedDisambiguationSpec.hs", "embed"),
    spec("Feature/Query/EmbedInnerJoinSpec.hs", "embed"),
    spec("Feature/Query/InsertSpec.hs", "insert"),
    spec("Feature/Query/JsonOperatorSpec.hs", "json"),
    spec("Feature/Query/NullsStripSpec.hs", "nulls"),
    spec("Feature/Query/PreferencesSpec.hs", "prefer"),
    spec("Feature/Query/RangeSpec.hs", "range"),
    spec("Feature/Query/RawOutputTypesSpec.hs", "raw"),
    spec("Feature/Query/RelatedQueriesSpec.hs", "related"),
    spec("Feature/Query/RpcSpec.hs", "rpc"),
    spec("Feature/Query/SingularSpec.hs", "single"),
    spec("Feature/Query/SpreadQueriesSpec.hs", "spread"),
    spec("Feature/Query/UpdateSpec.hs", "update"),
    spec("Feature/Query/UpsertSpec.hs", "upsert"),
    spec("Feature/OptionsSpec.hs", "options"),
    Spec {
        file: "Feature/Query/ErrorSpec.hs",
        feature: "error",
        only: Some("pgErrorCodeMapping"),
    },
    // What the surface does with the feature turned off, which is how
    // every Supabase project runs.
    Spec {
        file: "Feature/Query/PlanSpec.hs",
        feature: "plan",
        only: Some("disabledSpec"),
    },
    Spec {
        file: "Feature/Query/PgSafeUpdateSpec.hs",
        feature: "safeupdate",
        only: Some("disabledSpec"),
    },
];

const fn spec(file: &'static str, feature: &'static str) -> Spec {
    Spec {
        file,
        feature,
        only: None,
    }
}

/// The fixture files, in the order `fixtures/load.sql` includes them.
const FIXTURES: [&str; 6] = [
    "database.sql",
    "roles.sql",
    "schema.sql",
    "jwt.sql",
    "jsonschema.sql",
    "privileges.sql",
];

pub struct Derived {
    pub setup: String,
    /// The rows on their own, so a case that writes can be asked
    /// against the same rows the one before it was.
    pub reset: String,
    pub cases: Cases,
    /// What the scanner did not understand, so the number is in the
    /// report rather than in somebody's head.
    pub skipped: Vec<String>,
}

/// Everything, out of a PostgREST source tree.
pub fn derive(from: &Path, suite: &str) -> Result<Derived, String> {
    let spec_dir = from.join("test/spec");
    if !spec_dir.is_dir() {
        return Err(format!(
            "{} does not look like a PostgREST checkout, no test/spec in it",
            from.display()
        ));
    }
    let setup = setup(&spec_dir.join("fixtures"))?;
    let data = read(&spec_dir.join("fixtures/data.sql"))?;
    let reset = format!("{}\n{}", TRUNCATE, sql(&data));
    let mut cases = Vec::new();
    let mut skipped = Vec::new();
    let mut guessed = 0;
    let mut taken: BTreeMap<String, usize> = BTreeMap::new();
    for spec in SPECS {
        let text = read(&spec_dir.join(spec.file))?;
        let (found, missed) = scan(&text, spec);
        for mut case in found {
            if a_guess(&case) {
                guessed += 1;
                continue;
            }
            // Two `it` blocks in one file can be worded the same, and
            // one `it` block can make four requests, so the name gets a
            // number when it has to and reads as prose when it does not.
            let count = taken.entry(case.name.clone()).or_insert(0);
            *count += 1;
            if *count > 1 {
                case.name = format!("{} ({})", case.name, count);
            }
            cases.push(case);
        }
        skipped.extend(missed);
    }
    Ok(Derived {
        setup,
        reset,
        cases: Cases {
            suite: suite.to_string(),
            note: note(&skipped, guessed),
            schemas: vec!["test".to_string()],
            anon_role: "postgrest_test_anonymous".to_string(),
            // Upstream's fixtures pin every row down, so nothing a
            // derived case reads moves and there is nobody to sign a
            // token for.
            user: None,
            cases,
        },
        skipped,
    })
}

/// A request whose answer is the planner's guess rather than a fact.
///
/// `count=planned` puts the planner's row estimate in Content-Range, and
/// `count=estimated` is the same number whenever it is over the
/// threshold. That estimate is a property of the physical table at the
/// moment of the query: it moves with the page count, so a table that a
/// few writing cases have churned answers differently from the same
/// table an autovacuum has just been over, and postgres 17 and 18 need
/// not agree either.
///
/// Upstream can ask it because upstream runs against a database made
/// seconds earlier in a fixed order. Here the answer is recorded on one
/// machine and compared on another, days apart, so the case fails or
/// passes on the weather. Four of them flipped between two runs of the
/// same commit, which is the worst kind of case to have in a ratchet:
/// it teaches everybody that a red conformance job means nothing.
///
/// So the suite does not ask. Not excused, not known, not asked, and the
/// score does not count them either way.
fn a_guess(case: &Case) -> bool {
    case.headers.get("Prefer").is_some_and(|prefer| {
        prefer.contains("count=planned") || prefer.contains("count=estimated")
    })
}

fn note(skipped: &[String], guessed: usize) -> Vec<String> {
    vec![
        "Generated. Run `zou-conformance derive --from <postgrest checkout>`".to_string(),
        "rather than editing this by hand, and read the diff when you do.".to_string(),
        String::new(),
        "The questions are PostgREST's own, taken from the spec files that run".to_string(),
        "against its default configuration. The answers are not: upstream runs".to_string(),
        "with every transaction rolled back and with app settings this harness".to_string(),
        "does not set, so what a case is expected to answer there is not what".to_string(),
        "it answers here. recorded.json is what the pinned binary actually said.".to_string(),
        String::new(),
        format!(
            "{} requests in the spec files were not understood by the scanner and",
            skipped.len()
        ),
        "are not here. They are the ones built out of a helper or a variable".to_string(),
        "rather than written out, and they are listed by the deriver when it runs.".to_string(),
        String::new(),
        format!("{guessed} more are left out on purpose: they ask for count=planned or"),
        "count=estimated, and the answer to those is the planner's row estimate,".to_string(),
        "which moves with the page count of the table and so is not a function".to_string(),
        "of the request. A recording of a guess is not something to compare.".to_string(),
    ]
}

/// The fixtures, concatenated and with psql taken out of them.
///
/// `load.sql` is a psql script: `\ir` includes, `:"PGUSER"` variables.
/// The harness applies setup over a plain connection, because a target
/// is a url and a dsn and not a machine somebody can run psql on, so the
/// includes are done here and the variables are turned into SQL that
/// asks the server the same question.
fn setup(dir: &Path) -> Result<String, String> {
    let mut out = String::new();
    out.push_str(HEADER);
    for name in FIXTURES {
        let text = read(&dir.join(name))?;
        out.push_str(&format!("\n-- {name}\n\n"));
        out.push_str(&sql(&text));
    }
    let data = read(&dir.join("data.sql"))?;
    out.push_str("\n-- data.sql\n\n");
    out.push_str(&sql(&data));
    out.push_str(FOOTER);
    Ok(out)
}

const HEADER: &str = "\
-- Generated from PostgREST's own fixtures by `zou-conformance derive`.
-- The order is the order fixtures/load.sql includes them in.
--
-- Four things are not upstream's. The psql variables are gone, since
-- this is applied over a connection rather than by psql. Rows that
-- arrived on stdin are inserts, for the same reason. PostGIS is gone,
-- since it is not in the postgres CI runs and the specs that need it
-- are not in this suite. And the last line tells PostgREST to reload,
-- since it has just been told the schema moved.

set client_min_messages to warning;

-- schema.sql creates two schemas whose names are made of the characters
-- a url has opinions about, and database.sql does not drop them again.
-- Nothing depends on that upstream, where the database is new; here the
-- roles they grant to are dropped a few lines down, and a role cannot
-- be dropped while a schema still names it.
drop schema if exists \"SPECIAL \"\"@/\\#~_-\", \"EXTRA \"\"@/\\#~_-\" cascade;

-- schema.sql rewrites a function's oid in pg_proc on purpose, to build
-- the oid collision from PostgREST issue 4052. What it leaves behind is
-- a pg_depend row naming an oid no function has, and the next `drop
-- schema test cascade` reads that row and fails looking the function
-- up. Upstream never meets it, because upstream loads the fixtures into
-- a database it created a moment earlier. Here the same file is applied
-- once per target, so the row is swept first.
delete from pg_depend d
where d.classid = 'pg_proc'::regclass
  and not exists (select 1 from pg_proc p where p.oid = d.objid);
";

const FOOTER: &str = "\n\nnotify pgrst, 'reload schema';\n";

/// Empty every fixture table and put the rows back, which is what
/// upstream gets for free by rolling every transaction back.
const TRUNCATE: &str = "\
-- Generated. The rows only, so a case that writes starts where the
-- case before it started rather than where it left off.

do $$
declare
  tables text;
begin
  select string_agg(format('%I.%I', schemaname, tablename), ', ')
    into tables
    from pg_tables
   where schemaname in ('test', 'private', 'v1', 'v2', 'postgrest', 'تست',
                        'SPECIAL \"@/\\#~_-', 'EXTRA \"@/\\#~_-');
  if tables is not null then
    execute 'truncate ' || tables || ' restart identity cascade';
  end if;
end $$;
";

/// One fixture file with the psql taken out.
fn sql(text: &str) -> String {
    let mut out = String::new();
    let lines: Vec<&str> = text.lines().collect();
    let mut at = 0;
    while at < lines.len() {
        let line = lines[at];
        at += 1;
        let trimmed = line.trim_start();
        // Includes are done by the caller, in load.sql's order.
        if trimmed.starts_with("\\ir") || trimmed.starts_with("\\set") {
            continue;
        }
        // Rows that arrive on the wire rather than in the statement.
        // psql feeds them to a copy; there is no wire here, so they
        // become the inserts they would have been.
        if let Some(copy) = copy_at(&lines, at - 1) {
            out.push_str(&copy.inserts);
            at = copy.after;
            continue;
        }
        // A setting on the database only reaches connections opened
        // after it, and both targets have a pool up before setup runs,
        // so this would be in force on some requests and not others.
        // Nothing in the suites reads it: it is there for the pre
        // request function, which needs a server flag this harness does
        // not set.
        if trimmed.starts_with("ALTER DATABASE") {
            out.push_str("-- dropped, a database setting is not in force on a pool that is\n");
            out.push_str("-- already open: ");
            out.push_str(line);
            out.push('\n');
            continue;
        }
        // Which leaves one variable, and it is asking the server
        // something the server can answer itself.
        let line = line.replace(":\"PGUSER\"", "current_user");
        out.push_str(&line);
        out.push('\n');
    }
    without_postgis(&out)
}

struct Copied {
    inserts: String,
    /// The line after the `\.` that ended it.
    after: usize,
}

/// A `copy ... from stdin` and its rows, turned into inserts.
///
/// The two in the fixtures are both `csv delimiter '|'` and neither has
/// a quoted field or an embedded delimiter, so the split is a split.
/// Whitespace is left exactly where it is, because the rows are there
/// to be read back through the api and a column called
/// `"  col  w  space  "` holding `" space-1"` is the whole point of
/// them.
fn copy_at(lines: &[&str], at: usize) -> Option<Copied> {
    let head = lines[at];
    let lowered = head.to_lowercase();
    if !lowered.starts_with("copy ") || !lowered.contains("from stdin") {
        return None;
    }
    let into = head["copy ".len()..].trim_start();
    let into = into[..into.to_lowercase().find("from stdin")?].trim_end();
    let delimiter = match lowered.find("delimiter '") {
        Some(found) => head[found + "delimiter '".len()..].chars().next()?,
        None => '\t',
    };
    let mut inserts = String::new();
    let mut row = at + 1;
    while row < lines.len() && lines[row] != "\\." {
        let values: Vec<String> = lines[row].split(delimiter).map(literal).collect();
        inserts.push_str(&format!(
            "INSERT INTO {into} VALUES ({});\n",
            values.join(", ")
        ));
        row += 1;
    }
    Some(Copied {
        inserts,
        after: row + 1,
    })
}

/// One value, quoted the way postgres quotes one.
fn literal(value: &str) -> String {
    match value {
        // What csv means by a field with nothing in it.
        "" => "null".to_string(),
        value => format!("'{}'", value.replace('\'', "''")),
    }
}

/// Anything that needs PostGIS, dropped a whole statement at a time.
///
/// Commenting out the `create extension` line is not enough. There is a
/// geometry column under it, a table under that, functions returning
/// the table, aggregates over the functions, and custom media type
/// handlers over the aggregates. So the rule is by reference rather
/// than by name: a statement that mentions any of these goes, and since
/// everything that mentions them is downstream of the extension, that
/// is the whole of the dependency.
///
/// The match is against the statement with its function bodies taken
/// out, because `$$ ... multiple lines$$` is a comment about wrapping
/// and not a reference to the table called lines.
fn without_postgis(text: &str) -> String {
    const POSTGIS: [&str; 8] = [
        "postgis",
        "extensions.geometry",
        "extensions.st_",
        "shops",
        "shop_bles",
        "lines",
        "twkb",
        "geo+json",
    ];
    let mut out = String::new();
    for statement in statements(text) {
        let code = outside_bodies(&statement).to_lowercase();
        let needs = POSTGIS.iter().any(|name| match *name {
            // The only one of these that is also an English word.
            "lines" => code
                .split(|c: char| !c.is_alphanumeric() && c != '_')
                .any(|w| w == "lines"),
            name => code.contains(name),
        });
        match needs {
            true => {
                for line in statement.lines() {
                    out.push_str("-- dropped, needs postgis: ");
                    out.push_str(line);
                    out.push('\n');
                }
            }
            false => out.push_str(&statement),
        }
    }
    out
}

/// The text cut at every semicolon that ends a statement, keeping
/// everything, so that joining the pieces gives the file back.
fn statements(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut statement = String::new();
    let mut at = 0;
    while at < chars.len() {
        let c = chars[at];
        match c {
            '-' if chars.get(at + 1) == Some(&'-') => {
                while at < chars.len() && chars[at] != '\n' {
                    statement.push(chars[at]);
                    at += 1;
                }
            }
            '\'' => {
                statement.push(c);
                at += 1;
                while at < chars.len() {
                    statement.push(chars[at]);
                    at += 1;
                    if chars[at - 1] == '\'' {
                        break;
                    }
                }
            }
            '$' => match tag_at(&chars, at) {
                Some(tag) => {
                    let end = find(&chars, at + tag.len(), &tag).unwrap_or(chars.len());
                    let stop = (end + tag.len()).min(chars.len());
                    statement.extend(&chars[at..stop]);
                    at = stop;
                }
                None => {
                    statement.push(c);
                    at += 1;
                }
            },
            ';' => {
                statement.push(c);
                at += 1;
                // The newline after a statement belongs to it, so that
                // the pieces still read like the file they came from.
                while at < chars.len() && (chars[at] == '\n' || chars[at] == '\r') {
                    statement.push(chars[at]);
                    at += 1;
                }
                out.push(std::mem::take(&mut statement));
            }
            _ => {
                statement.push(c);
                at += 1;
            }
        }
    }
    if !statement.is_empty() {
        out.push(statement);
    }
    out
}

/// A dollar quote opening here, tag and all, when there is one.
///
/// A dollar sign is allowed inside an identifier, and postgres takes
/// the longest identifier it can, so `create table do$llar$s` is a
/// table and not a quote that never closes. That is exactly what the
/// fixtures have a table for, so the rule is here: a quote only opens
/// where an identifier could not be carrying on.
fn tag_at(chars: &[char], at: usize) -> Option<String> {
    if chars.get(at) != Some(&'$') {
        return None;
    }
    if at > 0 && (chars[at - 1].is_alphanumeric() || chars[at - 1] == '_' || chars[at - 1] == '$') {
        return None;
    }
    let mut tag = String::from("$");
    let mut look = at + 1;
    while let Some(c) = chars.get(look) {
        tag.push(*c);
        look += 1;
        match c {
            '$' => return Some(tag),
            c if c.is_alphanumeric() || *c == '_' => continue,
            _ => return None,
        }
    }
    None
}

fn find(chars: &[char], from: usize, what: &str) -> Option<usize> {
    let what: Vec<char> = what.chars().collect();
    (from..chars.len().saturating_sub(what.len() - 1))
        .find(|at| chars[*at..*at + what.len()] == what[..])
}

/// The statement with everything between dollar quotes taken out.
fn outside_bodies(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    let mut at = 0;
    while at < chars.len() {
        match tag_at(&chars, at) {
            Some(tag) => {
                let end = find(&chars, at + tag.len(), &tag).unwrap_or(chars.len());
                at = (end + tag.len()).min(chars.len());
                out.push(' ');
            }
            None => {
                out.push(chars[at]);
                at += 1;
            }
        }
    }
    out
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}

/// Every request one spec file makes, and what could not be read.
fn scan(text: &str, spec: &Spec) -> (Vec<Case>, Vec<String>) {
    // `import Protolude hiding (get)` is not a request, and it is the
    // only place in these files where one of the keywords turns up
    // inside brackets. Blanked rather than dropped, so the line numbers
    // in a skip still point at the file.
    let text: String = text
        .lines()
        .map(|line| match line.starts_with("import") {
            true => "",
            false => line,
        })
        .collect::<Vec<&str>>()
        .join("\n");
    let bytes: Vec<char> = text.chars().collect();
    let mut cases = Vec::new();
    let mut skipped = Vec::new();
    let mut it = String::new();
    let mut binding = String::new();
    let mut at = 0;
    let mut line = 1;
    while at < bytes.len() {
        if bytes[at] == '\n' {
            line += 1;
            at += 1;
            // A name in the first column starts a new top level
            // definition, which is how the files that export two specs
            // say where one ends.
            if let Some(name) = top_level(&bytes, at) {
                binding = name;
            }
            continue;
        }
        let word = word_at(&bytes, at);
        if word.is_empty() {
            at += 1;
            continue;
        }
        let after = at + word.chars().count();
        // Read past the description rather than through it. One of them
        // ends with the words "applied to it", and scanning on from
        // there took the next request's path for the next description.
        if word == "it"
            && let Ok((text, end)) = string_at(&bytes, space(&bytes, after))
        {
            it = text;
            line += bytes[at..end].iter().filter(|c| **c == '\n').count();
            at = end;
            continue;
        }
        let wanted = spec.only.is_none_or(|only| binding == only);
        let method = match word.as_str() {
            "get" => Some("GET"),
            "post" => Some("POST"),
            "put" => Some("PUT"),
            "patch" => Some("PATCH"),
            "delete" => Some("DELETE"),
            "request" => Some(""),
            _ => None,
        };
        match (wanted, method) {
            (true, Some(method)) if starts_a_request(&bytes, at) => {
                match request(&bytes, after, method, spec, &it) {
                    Ok(case) => cases.push(case),
                    Err(why) => skipped.push(format!("{}:{line}: {why}", spec.file)),
                }
            }
            _ => {}
        }
        at = after;
    }
    (cases, skipped)
}

/// A word is only a request when something that can precede one does.
/// `delete` turns up in prose and in `shouldRespondWith` chains, and
/// `get` is part of half the function names in the fixtures.
fn starts_a_request(bytes: &[char], at: usize) -> bool {
    let mut back = at;
    while back > 0 {
        back -= 1;
        match bytes[back] {
            ' ' | '\t' => continue,
            '\n' | '$' | '(' | '>' => return true,
            _ => return false,
        }
    }
    true
}

/// The name being defined, when this is the start of a top level
/// definition. Anything indented belongs to the one above it.
fn top_level(bytes: &[char], at: usize) -> Option<String> {
    let word = word_at(bytes, at);
    if word.is_empty() {
        return None;
    }
    let after = at + word.chars().count();
    let mut look = after;
    while look < bytes.len() && (bytes[look] == ' ' || bytes[look] == '\t') {
        look += 1;
    }
    match bytes.get(look) {
        Some(':') | Some('=') => Some(word),
        _ => None,
    }
}

fn word_at(bytes: &[char], at: usize) -> String {
    let mut out = String::new();
    let mut look = at;
    while look < bytes.len() && (bytes[look].is_alphanumeric() || bytes[look] == '_') {
        out.push(bytes[look]);
        look += 1;
    }
    out
}

/// One request, from just after the keyword that named it.
fn request(bytes: &[char], at: usize, method: &str, spec: &Spec, it: &str) -> Result<Case, String> {
    let mut at = at;
    let method = match method.is_empty() {
        // `request methodPost "/path" headers body`
        false => method.to_string(),
        true => {
            let (word, next) = word_arg(bytes, at)?;
            at = next;
            match word.strip_prefix("method") {
                Some(name) => name.to_uppercase(),
                None => return Err(format!("a method spelled {word:?}")),
            }
        }
    };
    let (path, next) = match arg(bytes, at)? {
        (Arg::Text(path), next) => (path, next),
        _ => return Err("a path that is not written out".to_string()),
    };
    at = next;
    let mut headers = BTreeMap::new();
    let mut body = None;
    // `request` takes headers and a body, the shorthands take a body
    // when the method has one and nothing when it does not.
    let wants_headers = method != "GET" || bytes.get(at.saturating_sub(1)).is_some();
    let _ = wants_headers;
    if let Ok((Arg::Headers(list), next)) = arg(bytes, at) {
        for (name, value) in list {
            headers.insert(name, value);
        }
        at = next;
    }
    if let Ok((Arg::Text(text), _)) = arg(bytes, at)
        && method != "GET"
        && method != "HEAD"
    {
        body = Some(text);
    }
    let writes = !matches!(method.as_str(), "GET" | "HEAD" | "OPTIONS");
    Ok(Case {
        name: match it.is_empty() {
            true => format!("{}: {}", spec.feature, path),
            false => format!("{}: {it}", spec.feature),
        },
        feature: spec.feature.to_string(),
        method,
        path: format!("/rest/v1{path}"),
        key: crate::suite::Key::Anon,
        headers,
        body,
        note: None,
        writes,
        chained: false,
        volatile: Vec::new(),
    })
}

enum Arg {
    Text(String),
    Headers(Vec<(String, String)>),
}

fn word_arg(bytes: &[char], at: usize) -> Result<(String, usize), String> {
    let at = space(bytes, at);
    let word = word_at(bytes, at);
    match word.is_empty() {
        true => Err("nothing where a word should be".to_string()),
        false => Ok((word.clone(), at + word.chars().count())),
    }
}

/// One argument, or an error naming what it was instead.
fn arg(bytes: &[char], at: usize) -> Result<(Arg, usize), String> {
    let at = space(bytes, at);
    match bytes.get(at) {
        Some('"') => {
            let (text, next) = string_at(bytes, at)?;
            Ok((Arg::Text(text), next))
        }
        Some('[') if looks_like(bytes, at, "[json|") => {
            let (text, next) = quasi(bytes, at)?;
            Ok((Arg::Text(text), next))
        }
        Some('[') => headers(bytes, at),
        Some('(') => helper(bytes, at),
        Some(_) => {
            let word = word_at(bytes, at);
            match word.as_str() {
                "mempty" => Ok((Arg::Text(String::new()), at + word.chars().count())),
                "" => Err("something the scanner cannot read".to_string()),
                _ => Err(format!("an argument written as {word}")),
            }
        }
        None => Err("the end of the file".to_string()),
    }
}

fn looks_like(bytes: &[char], at: usize, what: &str) -> bool {
    what.chars()
        .enumerate()
        .all(|(n, c)| bytes.get(at + n) == Some(&c))
}

fn space(bytes: &[char], at: usize) -> usize {
    let mut at = at;
    while at < bytes.len() && bytes[at].is_whitespace() {
        at += 1;
    }
    at
}

/// A Haskell string literal, unescaped.
fn string_at(bytes: &[char], at: usize) -> Result<(String, usize), String> {
    if bytes.get(at) != Some(&'"') {
        return Err("not a string".to_string());
    }
    let mut out = String::new();
    let mut look = at + 1;
    while look < bytes.len() {
        match bytes[look] {
            '"' => return Ok((out, look + 1)),
            '\\' => {
                look += 1;
                match bytes.get(look) {
                    // A string gap: a backslash, whitespace, and a
                    // backslash, which is how a long path is written
                    // over two lines.
                    Some(c) if c.is_whitespace() => {
                        look = space(bytes, look);
                        if bytes.get(look) == Some(&'\\') {
                            look += 1;
                        }
                    }
                    Some('n') => {
                        out.push('\n');
                        look += 1;
                    }
                    Some('t') => {
                        out.push('\t');
                        look += 1;
                    }
                    Some('r') => {
                        out.push('\r');
                        look += 1;
                    }
                    Some(c) => {
                        out.push(*c);
                        look += 1;
                    }
                    None => return Err("a string that never ends".to_string()),
                }
            }
            c => {
                out.push(c);
                look += 1;
            }
        }
    }
    Err("a string that never ends".to_string())
}

/// The text between `[json|` and `|]`.
fn quasi(bytes: &[char], at: usize) -> Result<(String, usize), String> {
    let mut look = at + "[json|".chars().count();
    let mut out = String::new();
    while look < bytes.len() {
        if bytes[look] == '|' && bytes.get(look + 1) == Some(&']') {
            return Ok((out.trim().to_string(), look + 2));
        }
        out.push(bytes[look]);
        look += 1;
    }
    Err("a json block that never ends".to_string())
}

/// A list of header pairs, written out.
fn headers(bytes: &[char], at: usize) -> Result<(Arg, usize), String> {
    let mut look = space(bytes, at + 1);
    let mut list = Vec::new();
    if bytes.get(look) == Some(&']') {
        return Ok((Arg::Headers(list), look + 1));
    }
    loop {
        look = space(bytes, look);
        if bytes.get(look) != Some(&'(') {
            return Err("a header list with something other than a pair in it".to_string());
        }
        look = space(bytes, look + 1);
        let name = match bytes.get(look) {
            Some('"') => {
                let (name, next) = string_at(bytes, look)?;
                look = next;
                name
            }
            _ => {
                let word = word_at(bytes, look);
                look += word.chars().count();
                match word.as_str() {
                    "hAccept" => "Accept".to_string(),
                    "" => return Err("a header name the scanner cannot read".to_string()),
                    other => return Err(format!("a header named {other}")),
                }
            }
        };
        look = space(bytes, look);
        if bytes.get(look) != Some(&',') {
            return Err("a header pair with no comma in it".to_string());
        }
        let (value, next) = match arg(bytes, look + 1)? {
            (Arg::Text(value), next) => (value, next),
            _ => return Err("a header value that is not written out".to_string()),
        };
        look = space(bytes, next);
        if bytes.get(look) != Some(&')') {
            return Err("a header pair that does not close".to_string());
        }
        list.push((name, value));
        look = space(bytes, look + 1);
        match bytes.get(look) {
            Some(',') => look += 1,
            Some(']') => return Ok((Arg::Headers(list), look + 1)),
            _ => return Err("a header list that does not close".to_string()),
        }
    }
}

/// The three helpers in SpecHelper that build headers, resolved here
/// rather than skipped, because between them they carry every Accept
/// and every Range in the suite.
fn helper(bytes: &[char], at: usize) -> Result<(Arg, usize), String> {
    let (inside, next) = balanced(bytes, at)?;
    let inside = inside.trim();
    if let Some(rest) = inside.strip_prefix("acceptHdrs") {
        let mime = quoted(rest)?;
        return Ok((Arg::Headers(vec![("Accept".to_string(), mime)]), next));
    }
    for (name, counted) in [("rangeHdrsWithCount", true), ("rangeHdrs", false)] {
        if let Some(rest) = inside.strip_prefix(name) {
            let mut list = vec![
                ("Range-Unit".to_string(), "items".to_string()),
                ("Range".to_string(), byte_range(rest)?),
            ];
            if counted {
                list.push(("Prefer".to_string(), "count=exact".to_string()));
            }
            return Ok((Arg::Headers(list), next));
        }
    }
    Err(format!(
        "headers built by {}",
        inside.split_whitespace().next().unwrap_or("something")
    ))
}

/// `(ByteRangeFromTo 0 1)` is `0-1`, `(ByteRangeFrom 0)` is `0-`, and
/// `(ByteRangeSuffix 1)` is `-1`, which is how http-types renders them
/// and what PostgREST reads out of the Range header.
fn byte_range(text: &str) -> Result<String, String> {
    let text = text.trim().trim_start_matches('(').trim_end_matches(')');
    let mut parts = text.split_whitespace();
    let kind = parts.next().unwrap_or_default();
    let numbers: Vec<&str> = parts.collect();
    match (kind, numbers.as_slice()) {
        ("ByteRangeFromTo", [from, to]) => Ok(format!("{from}-{to}")),
        ("ByteRangeFrom", [from]) => Ok(format!("{from}-")),
        ("ByteRangeSuffix", [to]) => Ok(format!("-{to}")),
        _ => Err(format!("a range written as {text:?}")),
    }
}

fn quoted(text: &str) -> Result<String, String> {
    let chars: Vec<char> = text.trim().chars().collect();
    let (text, _) = string_at(&chars, 0)?;
    Ok(text)
}

/// What is between a `(` and the `)` that closes it.
fn balanced(bytes: &[char], at: usize) -> Result<(String, usize), String> {
    let mut depth = 0;
    let mut out = String::new();
    let mut look = at;
    while look < bytes.len() {
        match bytes[look] {
            '(' => {
                depth += 1;
                if depth > 1 {
                    out.push('(');
                }
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok((out, look + 1));
                }
                out.push(')');
            }
            c => out.push(c),
        }
        look += 1;
    }
    Err("brackets that never close".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(source: &str) -> Case {
        let spec = spec("F.hs", "f");
        let (cases, skipped) = scan(source, &spec);
        assert!(skipped.is_empty(), "{skipped:?}");
        assert_eq!(cases.len(), 1, "{cases:?}");
        cases.into_iter().next().expect("one")
    }

    #[test]
    fn a_get_is_a_path_and_nothing_else() {
        let case = one("  it \"matches\" $\n    get \"/items?id=eq.5\"\n");
        assert_eq!(case.method, "GET");
        assert_eq!(case.path, "/rest/v1/items?id=eq.5");
        assert_eq!(case.name, "f: matches");
        assert!(case.headers.is_empty());
        assert!(case.body.is_none());
        assert!(!case.writes);
    }

    #[test]
    fn a_post_carries_the_json_block_after_it() {
        let case = one("  it \"inserts\" $\n    post \"/items\" [json| {\"id\":1} |]\n");
        assert_eq!(case.method, "POST");
        assert_eq!(case.body.as_deref(), Some("{\"id\":1}"));
        assert!(case.writes, "a post changes the rows");
    }

    #[test]
    fn a_request_names_its_own_method_and_headers() {
        let case = one(
            "  it \"patches\" $\n    request methodPatch \"/items?id=eq.1\"\n\
             \x20     [(\"Prefer\", \"return=representation\")]\n      [json| {\"id\":2} |]\n",
        );
        assert_eq!(case.method, "PATCH");
        assert_eq!(case.headers["Prefer"], "return=representation");
        assert_eq!(case.body.as_deref(), Some("{\"id\":2}"));
    }

    #[test]
    fn the_two_header_helpers_are_read_rather_than_skipped() {
        let case = one(
            "  it \"accepts\" $\n    request methodGet \"/items\" (acceptHdrs \"text/csv\") \"\"\n",
        );
        assert_eq!(case.headers["Accept"], "text/csv");

        let case = one("  it \"ranges\" $\n    request methodGet \"/items\"\n\
             \x20     (rangeHdrsWithCount (ByteRangeFromTo 0 1)) \"\"\n");
        assert_eq!(case.headers["Range"], "0-1");
        assert_eq!(case.headers["Range-Unit"], "items");
        assert_eq!(case.headers["Prefer"], "count=exact");
    }

    /// The scanner still reads them. They are dropped afterwards, in
    /// `derive`, so that the count of what was left out is a number
    /// rather than a silence.
    #[test]
    fn a_case_that_asks_for_a_guess_is_not_asked() {
        let mut case = one("  it \"counts\" $\n    get \"/items\"\n");
        assert!(!a_guess(&case));
        case.headers
            .insert("Prefer".to_string(), "count=exact".to_string());
        assert!(!a_guess(&case));
        case.headers
            .insert("Prefer".to_string(), "count=planned".to_string());
        assert!(a_guess(&case));
        case.headers.insert(
            "Prefer".to_string(),
            "count=estimated, return=representation".to_string(),
        );
        assert!(a_guess(&case));
    }

    /// A head has no body even though the call writes one.
    #[test]
    fn a_head_keeps_no_body() {
        let case = one("  it \"heads\" $\n    request methodHead \"/items\" [] mempty\n");
        assert_eq!(case.method, "HEAD");
        assert!(case.body.is_none());
        assert!(!case.writes);
    }

    /// The word turns up in prose and in function names, and neither is
    /// a request.
    #[test]
    fn a_keyword_in_the_middle_of_something_else_is_not_a_request() {
        let spec = spec("F.hs", "f");
        let (cases, _) = scan(
            "  it \"uses get_lines\" $\n    x <- pure (foo get_lines)\n",
            &spec,
        );
        assert!(cases.is_empty(), "{cases:?}");
    }

    #[test]
    fn a_request_the_scanner_cannot_read_is_counted_and_not_guessed() {
        let spec = spec("F.hs", "f");
        let (cases, skipped) = scan("  it \"builds it\" $\n    get (\"/items?\" <> q)\n", &spec);
        assert!(cases.is_empty());
        assert_eq!(skipped.len(), 1);
        assert!(skipped[0].starts_with("F.hs:2:"), "{skipped:?}");
    }

    #[test]
    fn only_takes_the_binding_it_was_asked_for() {
        let spec = Spec {
            file: "F.hs",
            feature: "f",
            only: Some("wanted"),
        };
        let source = "\nunwanted :: Spec\nunwanted = do\n  it \"a\" $\n    get \"/a\"\n\
                      \nwanted :: Spec\nwanted = do\n  it \"b\" $\n    get \"/b\"\n";
        let (cases, _) = scan(source, &spec);
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].path, "/rest/v1/b");
    }

    #[test]
    fn a_string_written_over_two_lines_is_one_string() {
        let case = one("  it \"long\" $\n    get \"/items?select=a,\\\n      \\b\"\n");
        assert_eq!(case.path, "/rest/v1/items?select=a,b");
    }

    #[test]
    fn psql_variables_become_something_the_server_can_answer() {
        let out = sql("GRANT a TO :\"PGUSER\";\n");
        assert!(out.contains("GRANT a TO current_user;"), "{out}");
    }

    /// It would be in force on connections opened after it and not on
    /// the ones the pool already has, which is worse than not being
    /// there at all.
    #[test]
    fn a_setting_on_the_database_is_dropped() {
        let out = sql("ALTER DATABASE :DBNAME SET x = '1';\n");
        assert!(!out.contains("\nALTER DATABASE"), "{out}");
        assert!(out.contains("-- dropped"), "{out}");
    }

    /// psql feeds these rows to a copy over the wire. There is no wire
    /// here, and the spaces are part of the value.
    #[test]
    fn rows_that_arrive_on_stdin_become_inserts() {
        let out =
            sql("COPY t (a, b) FROM STDIN CSV DELIMITER '|';\n1 | x \n2 | y \n\\.\nselect 1;\n");
        assert!(
            out.contains("INSERT INTO t (a, b) VALUES ('1 ', ' x ');"),
            "{out}"
        );
        assert!(
            out.contains("INSERT INTO t (a, b) VALUES ('2 ', ' y ');"),
            "{out}"
        );
        assert!(!out.contains("COPY"), "{out}");
        assert!(out.contains("select 1;"), "{out}");
    }

    /// A dollar sign is allowed inside an identifier, so this is a table
    /// and not a quote that swallows the rest of the file.
    #[test]
    fn a_dollar_inside_an_identifier_does_not_open_a_quote() {
        let out = sql("create table do$llar$s (a$num$ numeric);\ncreate table shops (id int);\n");
        assert!(out.contains("create table do$llar$s"), "{out}");
        assert!(!out.contains("\ncreate table shops"), "{out}");
    }

    #[test]
    fn the_postgis_tables_go_out_whole_and_not_just_their_extension() {
        let out =
            sql("create table shops (\n  id int primary key\n);\ncreate table keep (id int);\n");
        assert!(!out.contains("\ncreate table shops"), "{out}");
        assert!(out.contains("create table keep (id int);"), "{out}");
    }

    #[test]
    fn the_ranges_are_rendered_the_way_http_types_renders_them() {
        assert_eq!(
            byte_range(" (ByteRangeFromTo 0 1)").expect("a range"),
            "0-1"
        );
        assert_eq!(byte_range(" (ByteRangeFrom 2)").expect("a range"), "2-");
        assert_eq!(byte_range(" (ByteRangeSuffix 3)").expect("a range"), "-3");
        assert!(byte_range(" (Whatever 1)").is_err());
    }
}
