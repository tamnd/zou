//! What the run says when it is over.
//!
//! Two readers. A person wants the failures and nothing else, in the
//! order they would fix them. The scoreboard wants a number per feature
//! and no prose at all. Both come out of the same list of results, and
//! neither is allowed to round: a feature with three cases and one
//! failure is two thirds, not "mostly working".
//!
//! A case that is known to differ is still counted as a failure. It is
//! only excused from the exit code, so that CI fails on the day
//! something changes rather than on the day somebody writes a case for
//! a thing that is not built yet. The list is checked in, so the
//! difference is in the diff of the pull request that introduced it,
//! and a known case that starts passing fails the run too, because a
//! list nobody prunes stops meaning anything.

use std::collections::BTreeMap;

use crate::diff::{Difference, Verdict};

pub struct Result {
    pub name: String,
    pub feature: String,
    pub difference: Difference,
}

impl Result {
    fn differs(&self) -> bool {
        self.difference.verdict == Verdict::Different
    }
}

pub struct Report {
    pub suite: String,
    pub against: String,
    pub compared_with: String,
    pub results: Vec<Result>,
    /// Cases that never got an answer, which is a target that broke
    /// rather than a target that differs.
    pub errors: Vec<(String, String)>,
    /// Case name to why it is known to differ.
    pub known: BTreeMap<String, String>,
}

pub struct Counts {
    pub same: usize,
    pub written: usize,
    pub different: usize,
}

impl Counts {
    pub fn passed(&self) -> usize {
        self.same + self.written
    }

    pub fn total(&self) -> usize {
        self.passed() + self.different
    }

    /// Whole percent, rounded down, so 99.6% of a thousand cases reads
    /// as 99 and nobody claims a hundred while four cases fail.
    pub fn percent(&self) -> usize {
        match self.total() {
            0 => 100,
            total => self.passed() * 100 / total,
        }
    }
}

impl Report {
    pub fn counts(&self) -> Counts {
        let mut counts = Counts {
            same: 0,
            written: 0,
            different: 0,
        };
        for result in &self.results {
            match result.difference.verdict {
                Verdict::Same => counts.same += 1,
                Verdict::Written => counts.written += 1,
                Verdict::Different => counts.different += 1,
            }
        }
        counts
    }

    /// Per feature, in name order.
    pub fn by_feature(&self) -> Vec<(String, Counts)> {
        let mut features: BTreeMap<&str, Counts> = BTreeMap::new();
        for result in &self.results {
            let counts = features.entry(result.feature.as_str()).or_insert(Counts {
                same: 0,
                written: 0,
                different: 0,
            });
            match result.difference.verdict {
                Verdict::Same => counts.same += 1,
                Verdict::Written => counts.written += 1,
                Verdict::Different => counts.different += 1,
            }
        }
        features
            .into_iter()
            .map(|(name, counts)| (name.to_string(), counts))
            .collect()
    }

    /// Differences nobody wrote down, which is what a run is looking
    /// for.
    pub fn regressions(&self) -> Vec<&Result> {
        self.results
            .iter()
            .filter(|r| r.differs() && !self.known.contains_key(&r.name))
            .collect()
    }

    /// Cases the known list still names although they now agree. Worth
    /// failing over: an excuse that outlives the thing it excused hides
    /// the next difference in the same case.
    pub fn stale(&self) -> Vec<&Result> {
        self.results
            .iter()
            .filter(|r| !r.differs() && self.known.contains_key(&r.name))
            .collect()
    }

    pub fn failed(&self) -> bool {
        !self.errors.is_empty() || !self.regressions().is_empty() || !self.stale().is_empty()
    }

    /// The failures first and the summary last, because the summary is
    /// what stays on screen when the output is longer than the window.
    pub fn text(&self) -> String {
        let mut out = String::new();
        for (name, message) in &self.errors {
            out.push_str(&format!("error {name}\n  {message}\n"));
        }
        for result in &self.results {
            if !result.differs() {
                continue;
            }
            let head = match self.known.get(&result.name) {
                Some(why) => format!("known {}\n  {why}\n", result.name),
                None => format!("fail {}\n", result.name),
            };
            out.push_str(&head);
            for line in &result.difference.lines {
                out.push_str(&format!("  {line}\n"));
            }
        }
        for result in self.stale() {
            out.push_str(&format!(
                "fail {}\n  it agrees now, take it off the known list\n",
                result.name
            ));
        }
        let written: Vec<&Result> = self
            .results
            .iter()
            .filter(|r| r.difference.verdict == Verdict::Written)
            .collect();
        for result in &written {
            out.push_str(&format!("note {}\n", result.name));
            for line in &result.difference.lines {
                out.push_str(&format!("  {line}\n"));
            }
        }
        out.push_str(&format!(
            "\n{} against {}, compared with {}\n",
            self.suite, self.against, self.compared_with
        ));
        for (feature, counts) in self.by_feature() {
            out.push_str(&format!(
                "  {feature}: {}/{} ({}%)\n",
                counts.passed(),
                counts.total(),
                counts.percent()
            ));
        }
        let counts = self.counts();
        out.push_str(&format!(
            "  all: {}/{} ({}%), {} written differently, {} known, {} errors\n",
            counts.passed(),
            counts.total(),
            counts.percent(),
            counts.written,
            self.results.len() - self.regressions().len() - self.passing(),
            self.errors.len()
        ));
        out
    }

    fn passing(&self) -> usize {
        self.results.iter().filter(|r| !r.differs()).count()
    }

    /// The same run as data, which is what the scoreboard is generated
    /// from and what a CI job keeps as an artifact.
    pub fn json(&self) -> serde_json::Value {
        let features: Vec<serde_json::Value> = self
            .by_feature()
            .into_iter()
            .map(|(feature, counts)| {
                serde_json::json!({
                    "feature": feature,
                    "passed": counts.passed(),
                    "total": counts.total(),
                    "percent": counts.percent(),
                })
            })
            .collect();
        let cases: Vec<serde_json::Value> = self
            .results
            .iter()
            .map(|result| {
                serde_json::json!({
                    "name": result.name,
                    "feature": result.feature,
                    "verdict": match result.difference.verdict {
                        Verdict::Same => "same",
                        Verdict::Written => "written",
                        Verdict::Different => "different",
                    },
                    "known": self.known.get(&result.name),
                    "lines": result.difference.lines,
                })
            })
            .collect();
        let counts = self.counts();
        serde_json::json!({
            "suite": self.suite,
            "against": self.against,
            "compared_with": self.compared_with,
            "passed": counts.passed(),
            "total": counts.total(),
            "percent": counts.percent(),
            "written": counts.written,
            "errors": self.errors.iter().map(|(name, message)| serde_json::json!({
                "name": name,
                "message": message,
            })).collect::<Vec<_>>(),
            "features": features,
            "cases": cases,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(name: &str, feature: &str, verdict: Verdict) -> Result {
        Result {
            name: name.to_string(),
            feature: feature.to_string(),
            difference: Difference {
                verdict,
                lines: match verdict {
                    Verdict::Same => Vec::new(),
                    _ => vec!["status 500 not 200".to_string()],
                },
            },
        }
    }

    fn report(results: Vec<Result>) -> Report {
        Report {
            suite: "rest".to_string(),
            against: "zou".to_string(),
            compared_with: "postgrest 14.15".to_string(),
            results,
            errors: Vec::new(),
            known: BTreeMap::new(),
        }
    }

    #[test]
    fn a_body_written_differently_still_counts_as_a_pass() {
        let report = report(vec![
            result("a", "select", Verdict::Same),
            result("b", "select", Verdict::Written),
        ]);
        assert_eq!(report.counts().percent(), 100);
        assert!(!report.failed());
    }

    #[test]
    fn a_percentage_is_rounded_down_so_nobody_claims_all_of_it() {
        let mut results = vec![result("bad", "select", Verdict::Different)];
        for n in 0..999 {
            results.push(result(&format!("good {n}"), "select", Verdict::Same));
        }
        let report = report(results);
        assert_eq!(report.counts().percent(), 99);
        assert!(report.failed());
    }

    #[test]
    fn features_are_counted_apart() {
        let report = report(vec![
            result("a", "select", Verdict::Same),
            result("b", "insert", Verdict::Different),
            result("c", "insert", Verdict::Same),
        ]);
        let features = report.by_feature();
        assert_eq!(features[0].0, "insert");
        assert_eq!(features[0].1.percent(), 50);
        assert_eq!(features[1].0, "select");
        assert_eq!(features[1].1.percent(), 100);
    }

    /// A target that stopped answering is not a target that agrees.
    #[test]
    fn a_case_that_errored_fails_the_run() {
        let mut report = report(vec![result("a", "select", Verdict::Same)]);
        report
            .errors
            .push(("b".to_string(), "connection refused".to_string()));
        assert!(report.failed());
        assert!(report.text().contains("connection refused"));
    }

    #[test]
    fn nothing_to_compare_is_not_a_failure() {
        let report = report(Vec::new());
        assert_eq!(report.counts().percent(), 100);
        assert!(!report.failed());
    }

    #[test]
    fn the_text_puts_the_failures_before_the_summary() {
        let report = report(vec![
            result("a", "select", Verdict::Same),
            result("b", "select", Verdict::Different),
        ]);
        let text = report.text();
        let fail = text.find("fail b").expect("the failure");
        let summary = text.find("all: ").expect("the summary");
        assert!(fail < summary);
    }

    /// A written down difference is still a difference on the
    /// scoreboard. It is only excused from the exit code.
    #[test]
    fn a_known_difference_counts_against_the_score_and_not_against_the_run() {
        let mut report = report(vec![
            result("a", "select", Verdict::Same),
            result("b", "embed", Verdict::Different),
        ]);
        report
            .known
            .insert("b".to_string(), "embedding is not built yet".to_string());
        assert_eq!(report.counts().percent(), 50);
        assert!(!report.failed());
        assert!(report.text().contains("known b"));
        assert!(report.text().contains("embedding is not built yet"));
    }

    #[test]
    fn a_difference_nobody_wrote_down_fails_the_run() {
        let mut report = report(vec![
            result("a", "select", Verdict::Different),
            result("b", "embed", Verdict::Different),
        ]);
        report
            .known
            .insert("b".to_string(), "not built yet".to_string());
        assert_eq!(report.regressions().len(), 1);
        assert!(report.failed());
    }

    /// Otherwise the list grows old and the next difference in that
    /// case is excused by an entry about something else.
    #[test]
    fn a_known_case_that_started_agreeing_fails_the_run() {
        let mut report = report(vec![result("a", "select", Verdict::Same)]);
        report
            .known
            .insert("a".to_string(), "not built yet".to_string());
        assert!(report.failed());
        assert!(report.text().contains("take it off the known list"));
    }

    #[test]
    fn the_json_carries_every_case_and_not_just_the_failures() {
        let report = report(vec![
            result("a", "select", Verdict::Same),
            result("b", "select", Verdict::Different),
        ]);
        let json = report.json();
        assert_eq!(json["cases"].as_array().expect("cases").len(), 2);
        assert_eq!(json["percent"], 50);
        assert_eq!(json["features"][0]["feature"], "select");
    }
}
