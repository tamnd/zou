//! A suite: the schema it needs and the requests it makes.
//!
//! A suite is a directory in a checkout of tamnd/zou-conformance.
//! `setup.sql` is applied to whatever database a target reads,
//! `cases.json` is the requests, and `recorded.json` is what the
//! reference answered the last time somebody recorded it.
//!
//! Cases are data rather than code because the same list has to reach
//! three targets, and because a suite derived from upstream's fixtures
//! arrives as generated files. Nothing in a case is computed: no
//! timestamps, no generated ids, no ordering left to the planner. A
//! case whose answer is not the same twice is a case that cannot be
//! diffed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

/// Which key a request is sent with. `anon` and `service` are the two
/// keys a Supabase project hands out, `authenticated` is a key signed
/// with the same secret carrying the role a signed in person gets,
/// `user` is an access token for the person a suite seeded, and `none`
/// sends neither, which is how the gate itself gets tested.
///
/// The five `s3` kinds are not keys in the same sense. That surface is
/// asked with a signature over the request rather than with a token, so
/// a case naming one of them is signed by the harness on its way out.
/// `s3` signs with the pair the target was configured with, and three
/// of the others sign every bit as correctly with one field of the
/// signature wrong: a secret that is not it, an access key id nobody
/// has, or a region the project is not in. Those exist because a case
/// about a wrong signature cannot be a hand written header, whose date
/// would have to be today. A case about a malformed header writes its
/// own `authorization` under `none` instead.
///
/// `s3.project-region` is the odd one. Every other signature is made
/// in `us-east-1`, which is what a client signs in when nobody told it
/// otherwise, and this one is made in the region the project says it
/// is in when it is asked where it is. Whether that is a second region
/// the endpoint takes or the only one and `us-east-1` is the alias is
/// a question one recording answers and no documentation does.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Key {
    #[default]
    Anon,
    Authenticated,
    Service,
    User,
    S3,
    #[serde(rename = "s3.wrong-secret")]
    S3WrongSecret,
    #[serde(rename = "s3.wrong-key")]
    S3WrongId,
    #[serde(rename = "s3.wrong-region")]
    S3WrongRegion,
    #[serde(rename = "s3.project-region")]
    S3ProjectRegion,
    None,
}

impl Key {
    /// Whether the harness signs this request rather than putting a
    /// token on it.
    pub fn signs(self) -> bool {
        matches!(
            self,
            Key::S3
                | Key::S3WrongSecret
                | Key::S3WrongId
                | Key::S3WrongRegion
                | Key::S3ProjectRegion
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Case {
    /// Unique within the suite, and the thing a report names. Prose
    /// rather than an identifier, since it is read far more often than
    /// it is typed.
    pub name: String,
    /// What the case is about, and the row it counts towards on the
    /// scoreboard. Dotted, coarse on the left: `select.embed`.
    pub feature: String,
    #[serde(default = "get")]
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub key: Key,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// A token minted for this case alone, with some of its time claims
    /// moved off now. Each entry is a claim and an offset in seconds,
    /// so `{"nbf": 3600}` is a token that does not start working for an
    /// hour and `{"exp": -10}` is one that ran out ten seconds ago.
    ///
    /// Everything else a case sends is fixed text in a file, and a case
    /// about `nbf`, `iat` or the window around `exp` cannot be: a token
    /// that is an hour early is an hour early relative to the run
    /// asking it, and one written down last week is only expired. The
    /// suite says how far off and the run works out when that is.
    ///
    /// The token is otherwise the seeded person's, the same claims the
    /// `user` key carries, so a suite with no `user` has nobody to mint
    /// one for.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub token: BTreeMap<String, i64>,
    /// Sent as is, so a case can test what a malformed body does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// A body that is bytes, named as a file in the suite's directory.
    ///
    /// The render routes are why this exists. Transforming an image
    /// means having uploaded one, an image is not text, and a case is a
    /// line in a json file. The alternative was base64 in the case,
    /// which is a body nobody can look at and a diff nobody can read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// What `file` held, read once when the suite was loaded.
    ///
    /// Never written back out: it is the contents of a file that is
    /// already in the repository, and a case carrying its own copy of
    /// one would be two things to keep in step.
    #[serde(skip)]
    pub bytes: Option<Vec<u8>>,
    /// Why this case exists, when that is not obvious from the name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Whether the rows are different afterwards. A suite with a
    /// `reset.sql` puts them back before every case that says yes, so
    /// that a case is asked against the rows it was written against
    /// rather than against whatever the case before it left.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub writes: bool,
    /// Asked against what the case before it left, rather than against
    /// the fixture. It means something on any case, reading or writing,
    /// because the rows go back in front of every unchained case that
    /// something has written before.
    ///
    /// Some questions could not exist without it. A fixture writes rows,
    /// and the object surface is about bytes that live somewhere no
    /// fixture can reach: the only way to ask what a copy does is to
    /// upload something and then copy it, and the only way to ask what a
    /// download of a real object answers is to have uploaded it first.
    /// Both of those are two cases where the second one has to see what
    /// the first one did.
    ///
    /// A chain has to be contiguous. An unchained case sitting between a
    /// write and something that depends on it puts the rows back and
    /// takes the chain with them, so the cases in one chain sit together
    /// in the file in the order they are asked.
    ///
    /// The cost is that a chain is order dependent in a way the rest of
    /// a suite is not, so a chain should be short and should start with
    /// a case that resets like any other.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub chained: bool,
    /// Values out of this case's answer that the cases after it name,
    /// keyed by the name they write in braces.
    ///
    /// Each one says where to read it: `header:etag` or
    /// `element:UploadId`. A multipart upload is the reason this exists.
    /// The server mints an id nobody can know, every request after the
    /// creation carries it, and the id is in the body rather than in a
    /// header, so a suite that could not hold one could ask a creation
    /// and nothing after it.
    ///
    /// `location` is held out of every answer that has one without any
    /// case asking, which is what the resumable cases were written
    /// against and still name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub holds: BTreeMap<String, String>,
    /// The values in this answer that no two runs agree on, as json
    /// pointers into the body. Each one is replaced by the name of what
    /// it looked like before anything is compared, so a token still has
    /// to be a token and an id still has to be an id.
    ///
    /// A suite over a table can pin its answers down in setup.sql and
    /// none of these are needed. A suite over an auth server cannot: it
    /// answers with a token it just signed and a row it just made, and
    /// the alternative to naming those values is not comparing the
    /// answer at all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volatile: Vec<String>,
    /// The arrays in this answer whose order upstream does not promise,
    /// as json pointers into the body. Each is put into an order of the
    /// harness's own before anything is compared.
    ///
    /// One thing needs it and it is the factors on a user. GoTrue reads
    /// them with no order in the query and neither does zou, so the
    /// order is the order the rows are sitting in, and a case that
    /// compared it would be asking how postgres last reused the space
    /// in a page. What is in the list is still compared exactly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sorted: Vec<String>,
}

fn get() -> String {
    "GET".to_string()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Cases {
    pub suite: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub note: Vec<String>,
    /// The schemas a target has to expose for these cases to mean
    /// anything, first one first, since the first is the one a request
    /// that names no schema gets.
    #[serde(default = "conformance_schemas")]
    pub schemas: Vec<String>,
    /// The role the anon key carries. Upstream's own fixtures grant to
    /// a role of their own, so a suite derived from them asks under
    /// that name rather than under Supabase's.
    #[serde(default = "anon")]
    pub anon_role: String,
    /// The person the suite seeded, when it seeded one. The `user` key
    /// is an access token for them, minted from the same secret every
    /// target is configured with, so a case can ask an endpoint that
    /// wants a signed in caller without a case before it having signed
    /// anybody in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<User>,
    pub cases: Vec<Case>,
}

/// Enough of a seeded person to sign a token for them. Every field is
/// a row setup.sql wrote, so what the token claims and what the
/// database holds are the same thing by construction.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct User {
    pub id: String,
    pub email: String,
    /// The session the token belongs to. GoTrue writes one on every
    /// sign in and reads it back on the endpoints that can end a
    /// session, so a token without one is a token half the surface
    /// refuses.
    pub session_id: String,
}

fn conformance_schemas() -> Vec<String> {
    vec!["conformance".to_string(), "public".to_string()]
}

fn anon() -> String {
    "anon".to_string()
}

/// One answer, kept in the shape it is compared in rather than the
/// shape it arrived in.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Answer {
    pub name: String,
    pub status: u16,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// The parsed body when it was json, the text when it was not, and
    /// null when there was none. Parsed, because the recording is read
    /// by people and an escaped json string is not readable.
    pub body: serde_json::Value,
    /// The bytes as they arrived, kept only when they differ from
    /// re-serializing `body`, which is how a whitespace or key order
    /// difference stays visible without making every recording twice
    /// the size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>,
}

impl Answer {
    /// The body as it went over the wire, which is the kept bytes when
    /// there are any and the parsed body written back out when there
    /// are not.
    pub fn text(&self) -> String {
        match &self.raw {
            Some(raw) => raw.clone(),
            None => serde_json::to_string(&self.body).unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Recording {
    pub suite: String,
    /// What answered, in the words of the versions file.
    pub recorded_from: String,
    pub answers: Vec<Answer>,
}

/// A case zou is known to answer differently, and why. Checked in, so
/// that the difference arrives in a pull request somebody read rather
/// than in a CI run somebody ignored.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Known {
    pub name: String,
    pub why: String,
    /// What this difference is known against, when it is known against
    /// one comparison and not the others. The name a report prints
    /// after "compared with", so `supabase start` for the live stack
    /// and `postgrest 16.1 (recorded)` for the recording.
    ///
    /// The suites are recorded from a reference at a pinned version and
    /// diffed against the stack the CLI brings up, and those two are
    /// not always the same build. A case where they answer differently
    /// is a case zou can only match one of, and the field says which
    /// one it was excused against so that the other still holds it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub against: Option<String>,
}

pub struct Suite {
    pub name: String,
    pub dir: PathBuf,
    pub setup: String,
    /// The rows on their own, when the suite has them apart from the
    /// schema. Absent means the cases are asked in the order they are
    /// written and live with what the ones before them did.
    pub reset: Option<String>,
    pub cases: Cases,
    pub known: Vec<Known>,
}

static SUITES: OnceLock<PathBuf> = OnceLock::new();

/// Point the harness at a checkout of the suites, from `--suites`.
pub fn use_suites(dir: PathBuf) {
    let _ = SUITES.set(dir);
}

/// Where the suites live, which is not in this repository.
///
/// The questions and the recordings are in tamnd/zou-conformance: they
/// are megabytes of upstream's fixtures and upstream's answers rather
/// than zou's code, and they change when upstream changes rather than
/// when zou does. So the path comes from `--suites`, or from
/// `ZOU_CONFORMANCE_SUITES` for a shell that has a checkout already,
/// and the directory next to the crate is the fallback for anybody who
/// keeps one there.
pub fn suites_dir() -> PathBuf {
    if let Some(dir) = SUITES.get() {
        return dir.clone();
    }
    match std::env::var("ZOU_CONFORMANCE_SUITES") {
        Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => Path::new(env!("CARGO_MANIFEST_DIR")).join("suites"),
    }
}

/// Whether there is a checkout to read at all, which the tests over the
/// suites ask before they assert anything about them.
#[cfg(test)]
pub fn suites_present() -> bool {
    suites_dir().is_dir()
}

impl Suite {
    pub fn load(name: &str) -> Result<Suite, String> {
        let dir = suites_dir().join(name);
        if !dir.is_dir() {
            return Err(format!(
                "no suite named {name} in {}. The suites live in \
                 tamnd/zou-conformance, so clone it and pass --suites \
                 <checkout>/suites",
                suites_dir().display()
            ));
        }
        let setup = read(&dir.join("setup.sql"))?;
        let path = dir.join("reset.sql");
        let reset = match path.is_file() {
            true => Some(read(&path)?),
            false => None,
        };
        let mut cases: Cases = serde_json::from_str(&read(&dir.join("cases.json"))?)
            .map_err(|e| format!("{name}/cases.json: {e}"))?;
        // Now rather than when the case is sent, because a suite that
        // names a file that is not there should say so before it asks
        // anything, and because the same case is sent to two targets
        // and both of them should be sent the same bytes.
        for case in &mut cases.cases {
            if let Some(file) = &case.file {
                if outside(file) {
                    return Err(format!(
                        "{name}/cases.json: {:?} names {file:?}, which is outside the suite",
                        case.name
                    ));
                }
                let path = dir.join(file);
                case.bytes = Some(
                    std::fs::read(&path)
                        .map_err(|e| format!("{name}/cases.json: {}: {e}", path.display()))?,
                );
            }
        }
        if cases.suite != name {
            return Err(format!(
                "{name}/cases.json says it is the {} suite",
                cases.suite
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for case in &cases.cases {
            if !seen.insert(case.name.as_str()) {
                return Err(format!(
                    "{name}/cases.json has two cases named {:?}",
                    case.name
                ));
            }
        }
        let path = dir.join("known.json");
        let known: Vec<Known> = match path.is_file() {
            true => serde_json::from_str(&read(&path)?)
                .map_err(|e| format!("{}: {e}", path.display()))?,
            false => Vec::new(),
        };
        for known in &known {
            if !seen.contains(known.name.as_str()) {
                return Err(format!(
                    "{name}/known.json names {:?}, which is not a case",
                    known.name
                ));
            }
        }
        Ok(Suite {
            name: name.to_string(),
            dir,
            setup,
            reset,
            cases,
            known,
        })
    }

    /// The known list in the shape a report reads it, for a run
    /// comparing against the thing named.
    ///
    /// An entry that names what it is known against is left out of
    /// every other comparison, and a run that leaves one out is a run
    /// that expects the case to agree. That is the point of the field:
    /// a difference against one reference is not a licence to differ
    /// from the other.
    pub fn known(&self, compared_with: &str) -> BTreeMap<String, String> {
        self.known
            .iter()
            .filter(|k| match &k.against {
                Some(against) => against == compared_with,
                None => true,
            })
            .map(|k| (k.name.clone(), k.why.clone()))
            .collect()
    }

    pub fn recording_path(&self) -> PathBuf {
        self.dir.join("recorded.json")
    }

    pub fn known_path(&self) -> PathBuf {
        self.dir.join("known.json")
    }

    pub fn recording(&self) -> Result<Recording, String> {
        let path = self.recording_path();
        let text = read(&path)?;
        let recording: Recording =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(recording)
    }

    /// Every suite in the directory, in name order.
    pub fn all() -> Result<Vec<String>, String> {
        let dir = suites_dir();
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&dir).map_err(|e| format!("{}: {e}", dir.display()))? {
            let entry = entry.map_err(|e| format!("{}: {e}", dir.display()))?;
            if entry.path().is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                names.push(name.to_string());
            }
        }
        names.sort();
        Ok(names)
    }
}

/// Whether a named file reaches out of the suite's own directory.
///
/// A suite is data, and data that can name /etc/passwd as a request
/// body is data that reads a machine it was only meant to ask questions
/// of. The suites are pinned to a commit and read by people, so this is
/// a guard rather than a defence, but it is the difference between a
/// mistake that fails at load and one that gets uploaded somewhere.
fn outside(file: &str) -> bool {
    file.starts_with('/') || file.split('/').any(|part| part == "..")
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A suite with nothing in it but a known list, which is all the
    /// filter reads.
    fn with_known(known: Vec<Known>) -> Suite {
        Suite {
            name: "rest".to_string(),
            dir: PathBuf::new(),
            setup: String::new(),
            reset: None,
            cases: serde_json::from_str(r#"{"suite": "rest", "cases": []}"#).expect("parses"),
            known,
        }
    }

    #[test]
    fn a_difference_known_against_one_thing_is_not_excused_against_another() {
        let suite = with_known(vec![
            Known {
                name: "everywhere".to_string(),
                why: "zou does it differently and always will".to_string(),
                against: None,
            },
            Known {
                name: "the live stack only".to_string(),
                why: "the CLI ships an older build than the one this was recorded from".to_string(),
                against: Some("supabase start".to_string()),
            },
        ]);
        let stack = suite.known("supabase start");
        assert_eq!(stack.len(), 2);
        // And the recording, which is a different comparison, holds
        // the case to what was recorded.
        let recorded = suite.known("postgrest 16.1 (recorded)");
        assert_eq!(recorded.len(), 1);
        assert!(recorded.contains_key("everywhere"));
    }

    #[test]
    fn a_case_is_a_get_unless_it_says_otherwise() {
        let case: Case =
            serde_json::from_str(r#"{"name": "n", "feature": "f", "path": "/rest/v1/todos"}"#)
                .expect("parses");
        assert_eq!(case.method, "GET");
        assert_eq!(case.key, Key::Anon);
        assert!(case.headers.is_empty());
    }

    /// The bytes are never written back out, so a case that was loaded
    /// from a file and re-serialized still names the file and nothing
    /// else.
    #[test]
    fn a_body_that_is_a_file_is_named_rather_than_carried() {
        let mut case: Case = serde_json::from_str(
            r#"{"name": "n", "feature": "f", "path": "/p", "file": "fixtures/cat.jpg"}"#,
        )
        .expect("parses");
        assert_eq!(case.file.as_deref(), Some("fixtures/cat.jpg"));
        case.bytes = Some(vec![0xff, 0xd8, 0xff]);
        let written = serde_json::to_string(&case).expect("serializes");
        assert!(written.contains(r#""file":"fixtures/cat.jpg""#));
        assert!(!written.contains("bytes"));
    }

    #[test]
    fn a_file_that_reaches_out_of_the_suite_is_not_read() {
        assert!(outside("/etc/passwd"));
        assert!(outside("../../../etc/passwd"));
        assert!(outside("fixtures/../../secrets"));
        assert!(!outside("fixtures/cat.jpg"));
        assert!(!outside("cat.jpg"));
    }

    #[test]
    fn a_case_is_asked_against_the_fixture_unless_it_chains() {
        let plain: Case =
            serde_json::from_str(r#"{"name": "n", "feature": "f", "path": "/p", "writes": true}"#)
                .expect("parses");
        assert!(!plain.chained);
        let chained: Case = serde_json::from_str(
            r#"{"name": "n", "feature": "f", "path": "/p", "writes": true, "chained": true}"#,
        )
        .expect("parses");
        assert!(chained.chained);
    }

    /// The two tests below read the suites, and the suites are a
    /// checkout of another repository rather than files in this one, so
    /// they have nothing to say when there is no checkout. CI points
    /// `ZOU_CONFORMANCE_SUITES` at one before it runs them, which is
    /// where they are meant to run.
    #[test]
    fn every_suite_in_the_directory_loads() {
        if !suites_present() {
            return;
        }
        for name in Suite::all().expect("suites") {
            let suite = Suite::load(&name).expect(&name);
            assert!(!suite.cases.cases.is_empty(), "{name} has no cases");
            assert!(!suite.setup.trim().is_empty(), "{name} has no setup");
        }
    }

    /// The recording is what CI compares against, so a suite without
    /// one is a suite nobody is checking.
    #[test]
    fn every_case_has_an_answer_recorded_for_it() {
        if !suites_present() {
            return;
        }
        for name in Suite::all().expect("suites") {
            let suite = Suite::load(&name).expect(&name);
            let recording = suite.recording().expect(&name);
            let recorded: std::collections::BTreeSet<&str> =
                recording.answers.iter().map(|a| a.name.as_str()).collect();
            for case in &suite.cases.cases {
                assert!(
                    recorded.contains(case.name.as_str()),
                    "{name}: nothing recorded for {:?}",
                    case.name
                );
            }
            assert_eq!(
                recorded.len(),
                recording.answers.len(),
                "{name}: the recording has a name twice"
            );
        }
    }
}
