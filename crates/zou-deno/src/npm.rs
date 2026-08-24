//! What a version is on npm, and what a range of them means.
//!
//! A specifier is `npm:@supabase/supabase-js@^2.39.0`, and the part
//! after the last `@` is not a version: it is a range, written in a
//! language with a caret, a tilde, an `x`, a hyphen, comparators and an
//! `||`. Picking which of the versions a registry lists that specifier
//! meant is the first thing a resolver does and the last thing anybody
//! wants to be wrong about, since being wrong is a package that is
//! nearly the one that was asked for.
//!
//! Written here rather than taken from a crate because the language is
//! npm's and not cargo's, and the two differ in the places that matter:
//! `~1.2` and `1.2.x` are ranges npm writes all the time and cargo's
//! syntax does not have, and a prerelease is only reachable by a range
//! that names one, which is a rule about the range as a whole rather
//! than about a single comparator.
//!
//! Nothing here is on the network. What a registry lists is #596's next
//! piece; this is the arithmetic that piece needs.

// Nothing calls this yet, since the caller is the registry client that
// comes next. It is checked in ahead of that caller because it is the
// part that is worth being sure about on its own, and it is: the table
// in tests/fixtures is node's own answers to seventeen hundred cases.
#![allow(dead_code)]

use std::cmp::Ordering;
use std::fmt;

/// A version, as npm writes them: three numbers, an optional prerelease
/// and a build that is carried and never compared.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Version {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    /// The dot separated parts after a `-`, empty for a release.
    pub pre: Vec<Part>,
}

/// One dot separated piece of a prerelease, which is a number when it
/// reads as one and a string otherwise, because that is how the two
/// sort against each other.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Part {
    Number(u64),
    Text(String),
}

impl Version {
    /// A version, or nothing at all.
    ///
    /// Leading `v` and surrounding space are taken off first, because a
    /// packument and a package.json both contain them and a caller that
    /// has to trim before asking is a caller that will forget to.
    pub(crate) fn parse(said: &str) -> Option<Version> {
        let said = said.trim().trim_start_matches(['v', '=']).trim();
        // Build metadata is not part of what a version is worth, and
        // two versions differing only in it are the same version.
        let said = said.split('+').next().unwrap_or(said);
        let (numbers, pre) = match said.split_once('-') {
            Some((numbers, pre)) => (numbers, pre),
            None => (said, ""),
        };
        let mut numbers = numbers.split('.');
        let major = number(numbers.next()?)?;
        let minor = number(numbers.next()?)?;
        let patch = number(numbers.next()?)?;
        if numbers.next().is_some() {
            return None;
        }
        Some(Version {
            major,
            minor,
            patch,
            pre: parts(pre),
        })
    }

    /// Whether this version was published as a prerelease, which is the
    /// thing a range has to name before it can reach one.
    pub(crate) fn prerelease(&self) -> bool {
        !self.pre.is_empty()
    }

    /// The three numbers, without the prerelease.
    fn numbers(&self) -> (u64, u64, u64) {
        (self.major, self.minor, self.patch)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        for (at, part) in self.pre.iter().enumerate() {
            let separator = if at == 0 { '-' } else { '.' };
            match part {
                Part::Number(n) => write!(f, "{separator}{n}")?,
                Part::Text(t) => write!(f, "{separator}{t}")?,
            }
        }
        Ok(())
    }
}

impl Ord for Version {
    /// Numbers first, and then the prerelease, where having one at all
    /// is lower than not having one: `1.0.0-rc.1` is before `1.0.0`.
    fn cmp(&self, other: &Version) -> Ordering {
        self.numbers().cmp(&other.numbers()).then_with(|| {
            match (self.pre.is_empty(), other.pre.is_empty()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => self.pre.cmp(&other.pre),
            }
        })
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Version) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Part {
    /// A number is worth less than a name, and two of a kind compare as
    /// what they are: `1.0.0-1` is before `1.0.0-alpha`.
    fn cmp(&self, other: &Part) -> Ordering {
        match (self, other) {
            (Part::Number(a), Part::Number(b)) => a.cmp(b),
            (Part::Text(a), Part::Text(b)) => a.cmp(b),
            (Part::Number(_), Part::Text(_)) => Ordering::Less,
            (Part::Text(_), Part::Number(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for Part {
    fn partial_cmp(&self, other: &Part) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn parts(said: &str) -> Vec<Part> {
    match said.is_empty() {
        true => Vec::new(),
        false => said
            .split('.')
            .map(|part| match number(part) {
                // A part with a leading zero is not a number, it is a
                // name that looks like one, and npm compares it as one.
                Some(n) if !(part.len() > 1 && part.starts_with('0')) => Part::Number(n),
                _ => Part::Text(part.to_string()),
            })
            .collect(),
    }
}

fn number(said: &str) -> Option<u64> {
    match said.is_empty() || !said.bytes().all(|b| b.is_ascii_digit()) {
        true => None,
        false => said.parse().ok(),
    }
}

/// A range, which is an `||` of sets, each of which is an `and` of
/// comparators. `>=1.2.3 <2 || 3.x` is two sets, of two and of two.
#[derive(Clone, Debug)]
pub(crate) struct Range {
    sets: Vec<Vec<Comparator>>,
}

/// One bound, as an operator and the version it is about.
#[derive(Clone, Debug)]
struct Comparator {
    op: Op,
    at: Version,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Op {
    Lt,
    Lte,
    Gt,
    Gte,
    Eq,
}

impl Range {
    /// A range, or nothing when it is written in a language this does
    /// not read.
    ///
    /// A url, a git url and a local path are all legal in a dependency
    /// and none of them is a range. Answering nothing rather than
    /// guessing is what lets the caller say so by name.
    pub(crate) fn parse(said: &str) -> Option<Range> {
        let said = said.trim();
        // The empty range, `*` and `x` all mean any version there is.
        //
        // `latest` is not one of them and is not a range at all: it is
        // a dist tag, and which version a registry has under a tag is
        // something only the registry knows. A caller reading a tag
        // gets nothing here and asks the packument, which is the same
        // answer node's own semver gives.
        if said.is_empty() || said == "*" || said == "x" || said == "X" {
            return Some(Range { sets: vec![vec![]] });
        }
        let mut sets = Vec::new();
        for one in said.split("||") {
            sets.push(set(one)?);
        }
        Some(Range { sets })
    }

    /// Whether a version is in this range.
    ///
    /// The prerelease rule is the subtle one and it is a rule about a
    /// set rather than about a comparator: `>=1.0.0` does not reach
    /// `2.0.0-rc.1`, because somebody asking for at least one point
    /// nothing did not ask to be given an unreleased two. A set that
    /// names a prerelease of the same three numbers did ask, and gets
    /// it. This is npm's rule and the reason a caret range is safe to
    /// leave in a package.json for years.
    pub(crate) fn allows(&self, version: &Version) -> bool {
        self.sets.iter().any(|set| allows(set, version))
    }

    /// The best of what a registry lists, which is the highest version
    /// this range allows.
    pub(crate) fn best<'a>(
        &self,
        listed: impl IntoIterator<Item = &'a Version>,
    ) -> Option<Version> {
        listed
            .into_iter()
            .filter(|it| self.allows(it))
            .max()
            .cloned()
    }
}

fn allows(set: &[Comparator], version: &Version) -> bool {
    if version.prerelease()
        && !set
            .iter()
            .any(|c| c.at.prerelease() && c.at.numbers() == version.numbers())
    {
        return false;
    }
    set.iter().all(|c| c.allows(version))
}

impl Comparator {
    fn allows(&self, version: &Version) -> bool {
        let how = version.cmp(&self.at);
        match self.op {
            Op::Lt => how == Ordering::Less,
            Op::Lte => how != Ordering::Greater,
            Op::Gt => how == Ordering::Greater,
            Op::Gte => how != Ordering::Less,
            Op::Eq => how == Ordering::Equal,
        }
    }
}

/// One `||` separated piece, which is comparators separated by space.
fn set(said: &str) -> Option<Vec<Comparator>> {
    // A hyphen range is two versions with a dash between them, and it
    // is read before the split on space because that is where its own
    // spaces are: `1.2.3 - 2.3.4`.
    if let Some(hyphen) = hyphen(said) {
        return Some(hyphen);
    }
    let mut comparators = Vec::new();
    for piece in said.split_whitespace() {
        comparators.extend(one(piece)?);
    }
    Some(comparators)
}

fn hyphen(said: &str) -> Option<Vec<Comparator>> {
    let (from, to) = said.split_once(" - ")?;
    let from = loose(from.trim())?;
    let to = loose(to.trim())?;
    let mut comparators = vec![Comparator {
        op: Op::Gte,
        at: at_least(&from),
    }];
    // An open right hand side is a bound on whatever it named: `- 2`
    // is everything before three, and `- 2.3` everything before 2.4.
    comparators.push(match (to.minor, to.patch) {
        (None, _) => Comparator {
            op: Op::Lt,
            at: version(to.major? + 1, 0, 0),
        },
        (Some(minor), None) => Comparator {
            op: Op::Lt,
            at: version(to.major?, minor + 1, 0),
        },
        (Some(minor), Some(patch)) => Comparator {
            op: Op::Lte,
            at: Version {
                major: to.major?,
                minor,
                patch,
                pre: to.pre,
            },
        },
    });
    Some(comparators)
}

/// One comparator, which can be worth two: `^1.2` is a lower bound and
/// an upper one.
fn one(said: &str) -> Option<Vec<Comparator>> {
    let (op, rest) = match said {
        _ if said.starts_with(">=") => (Some(Op::Gte), &said[2..]),
        _ if said.starts_with("<=") => (Some(Op::Lte), &said[2..]),
        _ if said.starts_with('>') => (Some(Op::Gt), &said[1..]),
        _ if said.starts_with('<') => (Some(Op::Lt), &said[1..]),
        _ if said.starts_with('=') => (Some(Op::Eq), &said[1..]),
        _ if said.starts_with('^') => return caret(&said[1..]),
        _ if said.starts_with('~') => return tilde(said[1..].trim_start_matches('>')),
        _ => (None, said),
    };
    let loose = loose(rest)?;
    match op {
        // A bare `1.2` or `1.x` is a range and not a version, and it is
        // the same range a tilde on the same words is.
        None => partial(&loose),
        Some(op) => Some(bound(op, &loose)),
    }
}

fn caret(said: &str) -> Option<Vec<Comparator>> {
    let loose = loose(said)?;
    let from = at_least(&loose);
    // The caret keeps the leftmost nonzero number, which is what makes
    // it different from a tilde on a zero major: `^0.2.3` is under
    // `0.3.0` and `~0.2.3` is too, but `^0.0.3` is under `0.0.4`.
    let to = match (from.major, loose.minor, loose.patch) {
        (0, Some(0), Some(_)) => version(0, 0, from.patch + 1),
        (0, Some(minor), _) => version(0, minor + 1, 0),
        (0, None, _) => version(1, 0, 0),
        (major, _, _) => version(major + 1, 0, 0),
    };
    Some(vec![
        Comparator {
            op: Op::Gte,
            at: from,
        },
        Comparator { op: Op::Lt, at: to },
    ])
}

fn tilde(said: &str) -> Option<Vec<Comparator>> {
    let loose = loose(said)?;
    let from = at_least(&loose);
    // A tilde allows the last number named to move, so a tilde on a
    // major alone is that whole major.
    let to = match (loose.minor, loose.patch) {
        (None, _) => version(from.major + 1, 0, 0),
        (Some(minor), _) => version(from.major, minor + 1, 0),
    };
    Some(vec![
        Comparator {
            op: Op::Gte,
            at: from,
        },
        Comparator { op: Op::Lt, at: to },
    ])
}

/// A bare `1`, `1.2` or `1.2.x`, as the two bounds it stands for.
fn partial(loose: &Loose) -> Option<Vec<Comparator>> {
    let major = match loose.major {
        // `x`, `x.y` and anything else with nothing on the left is
        // every version there is.
        None => return Some(Vec::new()),
        Some(major) => major,
    };
    Some(match (loose.minor, loose.patch) {
        (Some(minor), Some(patch)) => vec![Comparator {
            op: Op::Eq,
            at: Version {
                major,
                minor,
                patch,
                pre: loose.pre.clone(),
            },
        }],
        (Some(minor), None) => vec![
            Comparator {
                op: Op::Gte,
                at: version(major, minor, 0),
            },
            Comparator {
                op: Op::Lt,
                at: version(major, minor + 1, 0),
            },
        ],
        (None, _) => vec![
            Comparator {
                op: Op::Gte,
                at: version(major, 0, 0),
            },
            Comparator {
                op: Op::Lt,
                at: version(major + 1, 0, 0),
            },
        ],
    })
}

/// A version with holes in it, which is what every one of these words
/// is before it is turned into bounds: `1.2` has no patch, `1.x` has
/// neither a minor nor a patch.
struct Loose {
    major: Option<u64>,
    minor: Option<u64>,
    patch: Option<u64>,
    pre: Vec<Part>,
}

fn loose(said: &str) -> Option<Loose> {
    let said = said.trim().trim_start_matches('v').trim();
    let said = said.split('+').next().unwrap_or(said);
    let (numbers, pre) = match said.split_once('-') {
        Some((numbers, pre)) => (numbers, pre),
        None => (said, ""),
    };
    let mut read = [None, None, None];
    let mut pieces = numbers.split('.');
    for slot in &mut read {
        let Some(piece) = pieces.next() else { break };
        match piece {
            "" => return None,
            "x" | "X" | "*" => break,
            _ => *slot = Some(number(piece)?),
        }
    }
    if pieces.count() > 3 {
        return None;
    }
    Some(Loose {
        major: read[0],
        minor: read[1],
        patch: read[2],
        pre: parts(pre),
    })
}

/// The lowest version something with holes in it could mean.
fn at_least(loose: &Loose) -> Version {
    Version {
        major: loose.major.unwrap_or(0),
        minor: loose.minor.unwrap_or(0),
        patch: loose.patch.unwrap_or(0),
        pre: loose.pre.clone(),
    }
}

/// A comparator against a version with holes in it, where the holes
/// move the bound and sometimes the operator with it.
///
/// The four that move are the ones where filling the hole with a zero
/// would say the wrong thing. `>1.x` is not `>1.0.0`, which would let
/// `1.5.0` in when what was asked for was everything after the ones:
/// it is `>=2.0.0`. `<=1.x` is `<2.0.0` for the same reason from the
/// other side. `<1.x` and `>=1.x` are already right with a zero in the
/// hole, so they keep theirs.
fn bound(op: Op, loose: &Loose) -> Vec<Comparator> {
    let Some(major) = loose.major else {
        // A bound against nothing in particular, `>=x`, is no bound.
        return Vec::new();
    };
    let comparator = match (op, loose.minor, loose.patch) {
        (Op::Gt, None, _) => Comparator {
            op: Op::Gte,
            at: version(major + 1, 0, 0),
        },
        (Op::Gt, Some(minor), None) => Comparator {
            op: Op::Gte,
            at: version(major, minor + 1, 0),
        },
        (Op::Lte, None, _) => Comparator {
            op: Op::Lt,
            at: version(major + 1, 0, 0),
        },
        (Op::Lte, Some(minor), None) => Comparator {
            op: Op::Lt,
            at: version(major, minor + 1, 0),
        },
        _ => Comparator {
            op,
            at: at_least(loose),
        },
    };
    vec![comparator]
}

fn version(major: u64, minor: u64, patch: u64) -> Version {
    Version {
        major,
        minor,
        patch,
        pre: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{Range, Version};

    fn v(said: &str) -> Version {
        Version::parse(said).unwrap_or_else(|| panic!("{said} is a version"))
    }

    fn allows(range: &str, version: &str) -> bool {
        Range::parse(range)
            .unwrap_or_else(|| panic!("{range} is a range"))
            .allows(&v(version))
    }

    #[test]
    fn a_version_is_three_numbers_and_whatever_follows_the_dash() {
        assert_eq!(v("1.2.3").to_string(), "1.2.3");
        assert_eq!(v(" v1.2.3 ").to_string(), "1.2.3");
        assert_eq!(v("1.2.3-rc.1").to_string(), "1.2.3-rc.1");
        // Build metadata is carried by nobody, because two versions
        // differing only in it are one version.
        assert_eq!(v("1.2.3+build.7").to_string(), "1.2.3");
        assert_eq!(Version::parse("1.2"), None);
        assert_eq!(Version::parse("1.2.3.4"), None);
        assert_eq!(Version::parse("1.2.x"), None);
        assert_eq!(Version::parse(""), None);
    }

    /// The order npm sorts by, which decides what the best of a list
    /// is. A prerelease is below the release it is a prerelease of, a
    /// number is below a name, and more parts beat fewer.
    #[test]
    fn a_prerelease_is_below_the_release_it_leads_to() {
        let mut sorted = [
            v("1.0.0"),
            v("1.0.0-rc.1"),
            v("1.0.0-beta.11"),
            v("1.0.0-beta.2"),
            v("1.0.0-alpha"),
            v("1.0.0-alpha.1"),
            v("1.0.0-1"),
            v("0.9.9"),
        ];
        sorted.sort();
        let said: Vec<String> = sorted.iter().map(ToString::to_string).collect();
        assert_eq!(
            said,
            [
                "0.9.9",
                "1.0.0-1",
                "1.0.0-alpha",
                "1.0.0-alpha.1",
                "1.0.0-beta.2",
                "1.0.0-beta.11",
                "1.0.0-rc.1",
                "1.0.0",
            ]
        );
    }

    #[test]
    fn a_caret_keeps_the_leftmost_number_that_is_not_zero() {
        assert!(allows("^1.2.3", "1.2.3"));
        assert!(allows("^1.2.3", "1.9.0"));
        assert!(!allows("^1.2.3", "1.2.2"));
        assert!(!allows("^1.2.3", "2.0.0"));
        // The zero major, where the caret is tighter than it looks.
        assert!(allows("^0.2.3", "0.2.9"));
        assert!(!allows("^0.2.3", "0.3.0"));
        assert!(allows("^0.0.3", "0.0.3"));
        assert!(!allows("^0.0.3", "0.0.4"));
        // And the shapes with holes in them, which the corpus is full
        // of: `^2` and `^1.0` are both real lines in a package.json.
        assert!(allows("^2", "2.39.7"));
        assert!(!allows("^2", "3.0.0"));
        assert!(allows("^0", "0.9.9"));
        assert!(!allows("^0", "1.0.0"));
    }

    #[test]
    fn a_tilde_lets_the_last_number_named_move() {
        assert!(allows("~1.2.3", "1.2.9"));
        assert!(!allows("~1.2.3", "1.3.0"));
        assert!(allows("~1.2", "1.2.9"));
        assert!(!allows("~1.2", "1.3.0"));
        assert!(allows("~1", "1.9.9"));
        assert!(!allows("~1", "2.0.0"));
    }

    #[test]
    fn a_bare_number_with_holes_in_it_is_a_range() {
        assert!(allows("1.2.3", "1.2.3"));
        assert!(!allows("1.2.3", "1.2.4"));
        assert!(allows("1.2", "1.2.9"));
        assert!(!allows("1.2", "1.3.0"));
        assert!(allows("1.x", "1.9.9"));
        assert!(!allows("1.x", "2.0.0"));
        assert!(allows("1.2.x", "1.2.9"));
        assert!(allows("=1.2.3", "1.2.3"));
    }

    #[test]
    fn comparators_and_the_things_between_them() {
        assert!(allows(">=1.2.3 <2.0.0", "1.9.9"));
        assert!(!allows(">=1.2.3 <2.0.0", "2.0.0"));
        assert!(allows(">1.2.3", "1.2.4"));
        assert!(!allows(">1.2.3", "1.2.3"));
        assert!(allows("<=1.2.3", "1.2.3"));
        // An or, where being in either side is being in the range.
        assert!(allows("^1 || ^2", "2.3.4"));
        assert!(allows("^1 || ^2", "1.3.4"));
        assert!(!allows("^1 || ^2", "3.0.0"));
        // A hyphen, whose right hand side widens when it has holes.
        assert!(allows("1.2.3 - 2.3.4", "2.3.4"));
        assert!(!allows("1.2.3 - 2.3.4", "2.3.5"));
        assert!(allows("1.2.3 - 2.3", "2.3.9"));
        assert!(allows("1.2.3 - 2", "2.9.9"));
        assert!(!allows("1.2.3 - 2", "3.0.0"));
        // A bound against a partial version fills the hole with a zero
        // when that says the right thing and moves when it does not.
        assert!(!allows("<1.2.x", "1.2.9"), "under the twos is under 1.2.0");
        assert!(allows("<1.2.x", "1.1.9"));
        assert!(allows("<=1.2.x", "1.2.9"), "and through the twos is not");
        assert!(!allows("<=1.2.x", "1.3.0"));
        assert!(
            !allows(">1.x", "1.9.9"),
            "after the ones is not in the ones"
        );
        assert!(allows(">1.x", "2.0.0"));
        assert!(allows(">=1.x", "1.0.0"));
        assert!(!allows("<3", "3.0.0"));
        assert!(allows("<3", "2.9.9"));
    }

    #[test]
    fn everything_and_nothing_in_particular() {
        assert!(allows("*", "1.2.3"));
        assert!(allows("", "1.2.3"));
        assert!(allows("x", "1.2.3"));
        // A dist tag is not a range, and answering nothing is what
        // sends the caller to the packument that knows what it means.
        assert!(Range::parse("latest").is_none());
        assert!(Range::parse("next").is_none());
        // A url, a git ref and a path are all legal in a dependency and
        // none of them is a range, so they are refused by name rather
        // than read as something.
        assert!(Range::parse("github:someone/thing").is_none());
        assert!(Range::parse("file:../beside-me").is_none());
        assert!(Range::parse("https://example.invalid/a.tgz").is_none());
    }

    /// The rule that keeps a caret range safe to leave in a
    /// package.json: a prerelease is only reachable by a range that
    /// named a prerelease of the same three numbers.
    #[test]
    fn a_prerelease_is_only_reached_by_a_range_that_asked_for_one() {
        assert!(!allows(">=1.0.0", "2.0.0-rc.1"));
        assert!(!allows("^1.0.0", "1.5.0-rc.1"));
        assert!(!allows("*", "1.0.0-rc.1"));
        assert!(allows(">=1.0.0-rc.1", "1.0.0-rc.2"));
        assert!(allows("^1.0.0-rc.1", "1.0.0-rc.2"));
        assert!(!allows(">=1.0.0-rc.1", "1.5.0-rc.1"));
        // And a release of those same numbers is reached the ordinary
        // way, since the rule is about prereleases and not about the
        // range having one in it.
        assert!(allows(">=1.0.0-rc.1", "1.0.0"));
    }

    /// What a resolver actually calls: the highest of what a registry
    /// listed that the range allows.
    #[test]
    fn the_best_of_a_list_is_the_highest_one_the_range_allows() {
        let listed: Vec<Version> = ["1.9.9", "2.0.0", "2.39.7", "2.40.0-rc.1", "3.0.0", "2.39.8"]
            .iter()
            .map(|it| v(it))
            .collect();
        let best = |range: &str| {
            Range::parse(range)
                .unwrap()
                .best(&listed)
                .map(|it| it.to_string())
        };
        assert_eq!(best("^2").as_deref(), Some("2.39.8"));
        assert_eq!(best("^2.39.7").as_deref(), Some("2.39.8"));
        assert_eq!(best("*").as_deref(), Some("3.0.0"));
        assert_eq!(best("~2.0.0").as_deref(), Some("2.0.0"));
        assert_eq!(best("^4").as_deref(), None);
        // The prerelease in the list is not the answer to a range that
        // did not ask for one, and is the answer to one that did.
        assert_eq!(best(">=2.40.0-rc.1 <3").as_deref(), Some("2.40.0-rc.1"));
    }
}

#[cfg(test)]
mod recorded {
    use super::{Range, Version};

    /// Every case in the table, against what node's own semver said
    /// about it.
    ///
    /// The table was recorded rather than reasoned about, which is the
    /// only way to be sure about a language with this many corners:
    /// `>1.x` is `>=2.0.0` and `<1.2.x` is `<1.2.0`, and both of those
    /// were wrong here until the table said so. A case that disagrees
    /// is this reading npm's language differently from npm, and the
    /// consequence of that is a package that is nearly the one that
    /// was asked for.
    ///
    /// Regenerating it needs node and the semver package, which is why
    /// what is checked in is the answers rather than the script: this
    /// suite runs with neither.
    #[test]
    fn the_table_node_semver_produced() {
        let raw = include_str!("../tests/fixtures/npm-ranges.tsv");
        let mut wrong = Vec::new();
        let mut read = 0;
        for line in raw
            .lines()
            .filter(|it| !it.starts_with('#') && !it.is_empty())
        {
            let mut said = line.split('\t');
            let (range, version, satisfies) = (
                said.next().unwrap_or_default(),
                said.next().unwrap_or_default(),
                said.next().unwrap_or_default(),
            );
            let version = Version::parse(version).expect("the table holds versions");
            let mine = Range::parse(range).is_some_and(|it| it.allows(&version));
            read += 1;
            if mine.to_string() != satisfies {
                wrong.push(format!(
                    "{range} and {version}: node {satisfies}, here {mine}"
                ));
            }
        }
        assert!(read > 1000, "the table is there and was read: {read} cases");
        assert!(
            wrong.is_empty(),
            "{} of {read} read differently:\n{}",
            wrong.len(),
            wrong.join("\n")
        );
    }
}
