//! One shot SQL against a store, with no server in between.
//!
//! `postgres --single` is the backend without the postmaster: one
//! session, one process, statements read from stdin and answers written
//! to stdout. Nothing listens, nothing forks, and nothing has to be
//! waited for. Against a real zou store the M1 spike measured it at
//! about 40 ms from nothing to a first answered query where a
//! postmaster took about 150. That measurement is in
//! docs/architecture.md next to the decision it settled.
//!
//! A session is read only unless the caller asks for otherwise, and
//! [`Session::writable`] says why: without a postmaster nothing
//! publishes, so a write reaches the block objects and no further, and
//! a store with a checkpoint in it shadows those objects on the next
//! attach. The spike's write survived because it ran against a store
//! that had never been captured.
//!
//! It is an internal tool and never a serving path, which is the other
//! half of that decision. One session per process is the design rather
//! than a limit somebody could raise, a FATAL is the process exiting
//! rather than an error handed back, and there is no protocol at all,
//! so everything that makes it cheap also makes it useless for
//! anybody's connection. What it is for is the operation that needs SQL
//! once and is then finished: an integrity check over a store that was
//! just restored, a catalog question, a fixup during bootstrap.
//!
//! Two things about what the backend prints are worth knowing before
//! reading the parser, because both of them are quiet.
//!
//! A null column is not printed. The row for `select 1, null, 'x'`
//! carries fields 1 and 3 and says nothing at all about 2, so a row is
//! read by the column numbers that arrived rather than by counting
//! them, and a parser that zipped fields against headers in order would
//! silently move every value after a null one column to the left.
//!
//! A statement that fails does not stop the session and does not change
//! the exit status. `select nosuch;` prints ERROR on stderr, the next
//! statement runs, and the process exits 0. So the exit status answers
//! whether the backend started, and stderr answers whether the SQL
//! worked, and a caller that checked only the first would read an empty
//! result as a clean bill of health.

use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The database a session opens when the caller does not name one.
const DEFAULT_DATABASE: &str = "postgres";

/// A one shot backend: the process to run, what to run it on, and the
/// environment the storage manager reads out of.
pub struct Session {
    postgres: PathBuf,
    pgdata: PathBuf,
    database: String,
    env: Vec<(OsString, OsString)>,
    writable: bool,
}

/// One statement's answer, columns and rows, every value as the text
/// the backend printed. `None` is a null.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl Rows {
    /// The value at `row` under `column`, or `None` for a null, a row
    /// that is not there, or a column this answer does not have.
    pub fn get(&self, row: usize, column: &str) -> Option<&str> {
        let at = self.columns.iter().position(|c| c == column)?;
        self.rows.get(row)?.get(at)?.as_deref()
    }

    /// The one value of a one by one answer, which is the shape most
    /// maintenance questions come back in.
    pub fn scalar(&self) -> Option<&str> {
        self.rows.first()?.first()?.as_deref()
    }
}

impl Session {
    /// A session over `pgdata`, run by the `postgres` in `pg_bin`.
    pub fn new(pg_bin: &Path, pgdata: &Path) -> Self {
        Self {
            postgres: pg_bin.join(if cfg!(windows) {
                "postgres.exe"
            } else {
                "postgres"
            }),
            pgdata: pgdata.to_path_buf(),
            database: DEFAULT_DATABASE.to_string(),
            env: Vec::new(),
            writable: false,
        }
    }

    /// Open a database other than `postgres`.
    pub fn database(mut self, name: &str) -> Self {
        self.database = name.to_string();
        self
    }

    /// Let this session write, which it may not by default.
    ///
    /// The default is read only because of where a write from here
    /// goes. There is no postmaster, so there is no wal pusher and no
    /// checkpointer of the kind that publishes: the pages land in the
    /// block objects and the wal never reaches the shared log, so the
    /// manifest does not learn that anything happened. On a store with
    /// a checkpoint in it, which is every store an ordinary database
    /// lives in, a later attach rebuilds its pages out of that
    /// checkpoint and the run after it, and the block objects a session
    /// here wrote are shadowed by it. The statement succeeded, the
    /// process exited 0, and the change is not in the database. See
    /// #548.
    ///
    /// Where it does hold is a store with no checkpoint yet, which is
    /// the bootstrap window between initdb and the first capture, and
    /// that is what this is for. Anywhere else, write through a server.
    pub fn writable(mut self) -> Self {
        self.writable = true;
        self
    }

    /// Set a variable for the backend, which is how it is told which
    /// store and which tenant its pages are in.
    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Run `sql` and return one [`Rows`] per statement that answered
    /// with a result set. Statements that answer with nothing, an
    /// insert or a `create index`, contribute no entry, so the position
    /// of an answer in the returned list is not the position of the
    /// statement that produced it and a caller that needs to know which
    /// is which should ask one thing at a time.
    ///
    /// An `ERROR` from any statement fails the whole call, including
    /// one the backend recovered from and carried on past, because a
    /// maintenance run that half worked is not a run that worked.
    pub fn run(&self, sql: &str) -> Result<Vec<Rows>, String> {
        let mut cmd = Command::new(&self.postgres);
        cmd.arg("--single").arg("-D").arg(&self.pgdata);
        if !self.writable {
            // A guard rather than a policy: a write from here is lost
            // quietly, see writable(), and an error at the statement is
            // the only way a caller finds out at all.
            cmd.arg("-c").arg("default_transaction_read_only=on");
        }
        cmd.arg(&self.database)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in &self.env {
            cmd.env(key, value);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", self.postgres.display()))?;
        // The backend reads until stdin closes, so the pipe is dropped
        // here rather than after the wait, which would be a session
        // waiting for a statement and a parent waiting for the session.
        {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| "the backend took no stdin".to_string())?;
            stdin
                .write_all(sql.as_bytes())
                .map_err(|e| format!("write sql to the backend: {e}"))?;
            if !sql.ends_with('\n') {
                stdin
                    .write_all(b"\n")
                    .map_err(|e| format!("write sql to the backend: {e}"))?;
            }
        }
        let out = child
            .wait_with_output()
            .map_err(|e| format!("wait for the backend: {e}"))?;
        let stderr = String::from_utf8_lossy(&out.stderr);
        if let Some(complaint) = complaint(&stderr) {
            return Err(complaint);
        }
        if !out.status.success() {
            return Err(format!(
                "the single user backend exited with {} and said nothing about why, its output was:\n{}",
                out.status,
                stderr.trim_end()
            ));
        }
        Ok(parse(&String::from_utf8_lossy(&out.stdout)))
    }
}

/// The ERROR, FATAL and PANIC lines out of a backend's stderr, without
/// the timestamp and pid every line is stamped with, or `None` when the
/// session had nothing to complain about.
///
/// FATAL is here as well as in the exit status because the two do not
/// always arrive together and a message is worth more than a code.
fn complaint(stderr: &str) -> Option<String> {
    let said: Vec<&str> = stderr
        .lines()
        .filter_map(|line| {
            let message = line.split_once("] ").map_or(line, |(_, rest)| rest);
            let bad = message.starts_with("ERROR:")
                || message.starts_with("FATAL:")
                || message.starts_with("PANIC:");
            bad.then_some(message)
        })
        .collect();
    (!said.is_empty()).then(|| said.join("\n"))
}

/// The prompt the backend writes before each statement's output, which
/// is what separates one answer from the next.
const PROMPT: &str = "backend> ";

/// The line that ends the header block and every row after it.
const RULE: &str = "\t----";

/// Turn what the backend printed into result sets.
///
/// The shape per statement is a prompt, one line per column, a rule,
/// then a rule terminated block per row. Headers are told from values
/// by where they are rather than by how they look, because a column
/// named by a quoted identifier can be shaped like anything.
fn parse(stdout: &str) -> Vec<Rows> {
    let mut sets = Vec::new();
    let mut current = Rows::default();
    let mut pending: Vec<(usize, String)> = Vec::new();
    let mut in_header = true;

    let flush_row = |current: &mut Rows, pending: &mut Vec<(usize, String)>| {
        if pending.is_empty() {
            return;
        }
        let mut row = vec![None; current.columns.len()];
        for (at, value) in pending.drain(..) {
            // The backend numbers columns from one, and a field for a
            // column no header announced is not a row this understands.
            if let Some(slot) = at.checked_sub(1).and_then(|i| row.get_mut(i)) {
                *slot = Some(value);
            }
        }
        current.rows.push(row);
    };

    for line in stdout.lines() {
        let mut rest = line;
        let mut prompted = false;
        while let Some(after) = rest.strip_prefix(PROMPT) {
            rest = after;
            prompted = true;
        }
        if prompted {
            flush_row(&mut current, &mut pending);
            if !current.columns.is_empty() {
                sets.push(std::mem::take(&mut current));
            }
            current = Rows::default();
            in_header = true;
        }
        if rest == RULE {
            if in_header {
                in_header = false;
            } else {
                flush_row(&mut current, &mut pending);
            }
            continue;
        }
        let Some((at, name, value)) = field(rest) else {
            continue;
        };
        match value {
            None if in_header => {
                // Columns arrive in order and numbered from one, so a
                // gap would mean this is not a header block after all.
                if at == current.columns.len() + 1 {
                    current.columns.push(name.to_string());
                }
            }
            Some(value) if !in_header => pending.push((at, value.to_string())),
            _ => {}
        }
    }
    flush_row(&mut current, &mut pending);
    if !current.columns.is_empty() {
        sets.push(current);
    }
    sets
}

/// Split one header or value line into its column number, its name, and
/// its value if it carries one.
///
/// A line is `\t N: name\t(typeid = ...)` in a header block and
/// `\t N: name = "value"\t(typeid = ...)` in a row. Nothing in the
/// value is escaped, so a value with a quote in it prints its quote and
/// the only way to find the end of one is to work in from the trailing
/// type description rather than out from the first quote.
fn field(line: &str) -> Option<(usize, &str, Option<&str>)> {
    let body = line.strip_prefix('\t')?;
    let body = body[..body.rfind("\t(typeid = ")?].trim_end();
    let (at, rest) = body.split_once(':')?;
    let at = at.trim().parse().ok()?;
    // Not trimmed before the split: a column with no name prints
    // `1:  = "v"`, whose separator is the second of those two spaces,
    // and trimming first would leave `= "v"` with no separator in it at
    // all and read the whole thing as a header.
    match rest.strip_suffix('"').and_then(|r| r.split_once(" = \"")) {
        Some((name, value)) => Some((at, name.trim(), Some(value))),
        None => Some((at, rest.trim(), None)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the backend prints for
    /// `select 1 as one, null::text as n, 'x y' as s;`, verbatim.
    const WITH_A_NULL: &str = "PostgreSQL stand-alone backend 18.4\n\
        backend> \t 1: one\t(typeid = 23, len = 4, typmod = -1, byval = t)\n\
        \t 2: n\t(typeid = 25, len = -1, typmod = -1, byval = f)\n\
        \t 3: s\t(typeid = 25, len = -1, typmod = -1, byval = f)\n\
        \t----\n\
        \t 1: one = \"1\"\t(typeid = 23, len = 4, typmod = -1, byval = t)\n\
        \t 3: s = \"x y\"\t(typeid = 25, len = -1, typmod = -1, byval = f)\n\
        \t----\n\
        backend> \n";

    #[test]
    fn a_null_column_is_absent_from_the_row_rather_than_empty_in_it() {
        let sets = parse(WITH_A_NULL);
        assert_eq!(sets.len(), 1);
        let rows = &sets[0];
        assert_eq!(rows.columns, ["one", "n", "s"]);
        assert_eq!(rows.rows.len(), 1);
        // The value that follows the null is the assertion. Zipping the
        // two fields that arrived against the three headers would put
        // "x y" under n and leave s empty, and both would look like
        // data rather than like a bug.
        assert_eq!(rows.get(0, "one"), Some("1"));
        assert_eq!(rows.get(0, "n"), None);
        assert_eq!(rows.get(0, "s"), Some("x y"));
        assert_eq!(rows.scalar(), Some("1"));
    }

    /// Two statements, the first of them two rows, one value carrying
    /// the quote character the backend does not escape.
    const TWO_STATEMENTS: &str = "PostgreSQL stand-alone backend 18.4\n\
        backend> \t 1: a\t(typeid = 23, len = 4, typmod = -1, byval = t)\n\
        \t 2: b\t(typeid = 25, len = -1, typmod = -1, byval = f)\n\
        \t----\n\
        \t 1:  = \"1\"\t(typeid = 23, len = 4, typmod = -1, byval = t)\n\
        \t 2:  = \"q\"uote\"\t(typeid = 25, len = -1, typmod = -1, byval = f)\n\
        \t----\n\
        \t 1:  = \"2\"\t(typeid = 23, len = 4, typmod = -1, byval = t)\n\
        \t 2:  = \"plain\"\t(typeid = 25, len = -1, typmod = -1, byval = f)\n\
        \t----\n\
        backend> \t 1: after\t(typeid = 23, len = 4, typmod = -1, byval = t)\n\
        \t----\n\
        \t 1: after = \"3\"\t(typeid = 23, len = 4, typmod = -1, byval = t)\n\
        \t----\n\
        backend> \n";

    #[test]
    fn every_statement_that_answered_is_its_own_result_set() {
        let sets = parse(TWO_STATEMENTS);
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0].rows.len(), 2);
        assert_eq!(sets[0].get(0, "a"), Some("1"));
        // The value's own quote, kept whole. Reading the value out to
        // the first quote after `= "` would cut this to `q`.
        assert_eq!(sets[0].get(0, "b"), Some("q\"uote"));
        assert_eq!(sets[0].get(1, "b"), Some("plain"));
        assert_eq!(sets[1].columns, ["after"]);
        assert_eq!(sets[1].scalar(), Some("3"));
    }

    #[test]
    fn a_statement_that_answers_with_nothing_contributes_no_result_set() {
        let printed = "PostgreSQL stand-alone backend 18.4\nbackend> backend> \n";
        assert!(parse(printed).is_empty());
    }

    #[test]
    fn an_error_the_backend_carried_on_past_is_still_an_error() {
        // The whole of the failing case: the backend logs ERROR, runs
        // the next statement, and exits 0.
        let said = "2026-08-20 16:04:21.384 +07 [23531] ERROR:  column \"nosuch\" does not exist at character 8\n\
            2026-08-20 16:04:21.384 +07 [23531] STATEMENT:  select nosuch;\n";
        assert_eq!(
            complaint(said).as_deref(),
            Some("ERROR:  column \"nosuch\" does not exist at character 8")
        );
    }

    #[test]
    fn an_ordinary_log_line_is_not_a_complaint() {
        let said =
            "2026-08-20 16:03:58.052 +07 [22808] LOG:  checkpoint starting: shutdown immediate\n";
        assert_eq!(complaint(said), None);
        assert_eq!(complaint(""), None);
    }

    #[test]
    fn a_fatal_is_reported_by_what_it_said_and_not_only_by_the_exit_code() {
        let said =
            "2026-08-20 16:04:21.420 +07 [23541] FATAL:  database \"nosuchdb\" does not exist\n";
        assert_eq!(
            complaint(said).as_deref(),
            Some("FATAL:  database \"nosuchdb\" does not exist")
        );
    }
}
