//! A bounded exhaustive check of what gc is allowed to delete.
//!
//! Every other test of the collector is one hand written scenario, and
//! a scenario is a proof about itself. The property the two phase
//! design exists to hold is not local to any scenario or to any
//! function: it is a property of interleavings, that no ordering of
//! publishes, folds, branch creations, crashes and gc runs ever leaves
//! a live manifest naming an object that is not in the store. That is
//! what this file checks, by walking the reachable state graph.
//!
//! It drives the real [`gc::run_with`] against a real store rather than
//! a model of the pinning rules. A reimplementation of those rules
//! could agree with itself perfectly and disagree with the collector,
//! which is the one thing a check like this must not be able to do. A
//! state is the store's contents, so two paths that wrote the same
//! bytes are the same state however they got there, and the walk
//! terminates because the clock is bounded.
//!
//! What is modelled is checkpoints, history snapshots and branches,
//! which is where the pinning is subtle: owner tags, a parent's capture
//! held alive by a child, retention expiry, and a candidate that a
//! branch published between two runs takes back off the list. Layers
//! pin through the same shape one loop further down and are covered by
//! the scenario tests next to the collector.
//!
//! ## The preconditions
//!
//! The collector's safety rests on two things it cannot enforce, and
//! the walk runs once per hazard so that each is shown to be load
//! bearing rather than asserted to be.
//!
//! The first is that a fold does not leave a capture uploaded and
//! unreferenced for longer than the safety window. Two runs a window
//! apart both looking at an object that genuinely nothing references
//! will delete it, and no pinning rule can prevent that, because at
//! both observations the object was garbage by every definition
//! available.
//!
//! The second is the same bound on branch creation, which is a publish
//! of a different object. A branch reads the parent's manifest and
//! writes its own naming what it read; between those two writes it pins
//! nothing, so a parent that supersedes a capture in that gap leaves
//! the branch naming bytes the collector was free to take.
//!
//! Under [`Mode::Documented`] both bounds hold and there must be no
//! violation. Under each of the other two one bound is dropped and
//! there must be a violation, with the shortest trace to it printed.
//! That half is the half that matters: a checker that reports nothing
//! is only worth something once it has been shown to report the
//! failures it exists to find.

use std::collections::{BTreeMap, HashMap, VecDeque};

use zou_pg::gc::{self, Policy};
use zou_store::layout::TenantLayout;
use zou_store::manifest::{BranchOf, CheckpointKind, CheckpointRef, Manifest};
use zou_store::{CasStore, Lsn, MemStore};

/// The parent, and the branch that can be made from it.
const PARENT: &str = "p";
const CHILD: &str = "c";

/// The captures a fold can produce. Two, because one is not enough to
/// supersede anything and three is the same shapes again more slowly.
const IDS: [&str; 2] = ["a", "b"];

/// How far the clock is allowed to run. The graph is finite because of
/// this and nothing else.
const HORIZON: u64 = 4;

/// The policy the walk runs under. Small numbers on purpose: the window
/// and the retention have to be crossable inside the horizon or the
/// states that depend on crossing them are never reached.
const WINDOW: u64 = 1;
const RETENTION: u64 = 2;

/// How many steps deep the walk goes. The clock bounds the graph but
/// not the depth, since the epoch in a history key rises with every
/// publish and makes otherwise identical stores different states.
const DEPTH: usize = 10;

/// Which of the two preconditions is in force. Dropping one at a time
/// is what makes each of them demonstrably necessary, rather than a
/// pair of guards that might both be doing nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Both publishers finish inside the window, which is what the
    /// collector documents as the operator's side of the bargain.
    Documented,
    /// A fold takes longer than the window to name what it uploaded.
    SlowFold,
    /// Branch creation takes longer than the window to write the
    /// manifest naming what it inherited.
    SlowBranch,
}

impl Mode {
    fn folds_finish_in_time(self) -> bool {
        self != Mode::SlowFold
    }

    fn branches_finish_in_time(self) -> bool {
        self != Mode::SlowBranch
    }
}

/// One reachable state. The store is the whole of it apart from the
/// clock and the bookkeeping the guards need, and that is deliberate:
/// anything the model remembered that the store does not would be a
/// fact the collector cannot see and must not depend on.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct World {
    now: u64,
    store: BTreeMap<String, Vec<u8>>,
    /// Captures uploaded and not yet published or given up on, and when
    /// their upload started. Not in the store because it is not a fact
    /// about the store: an object in flight and an object a crash
    /// abandoned look identical from outside, which is the whole reason
    /// deletion takes two runs.
    inflight: BTreeMap<String, u64>,
    /// A branch that has read the parent and not yet written itself:
    /// when it started, and what it read. Nothing of it is in the store
    /// yet, which is exactly the problem.
    branching: Option<(u64, Vec<String>)>,
    /// How many manifest writes have happened, which is the epoch the
    /// next history snapshot is named with.
    epoch: u64,
}

impl World {
    /// A parent with an empty manifest and nothing else.
    fn new() -> World {
        let mut world = World {
            now: 0,
            store: BTreeMap::new(),
            inflight: BTreeMap::new(),
            branching: None,
            epoch: 0,
        };
        world.publish_manifest(PARENT, Manifest::new(PARENT, 18));
        world
    }

    fn manifest(&self, r: &str) -> Option<Manifest> {
        let data = self.store.get(&TenantLayout::new(r).manifest())?;
        Some(Manifest::from_json(data).expect("this model wrote it"))
    }

    /// Write a manifest and the history snapshot every state changing
    /// publish leaves behind, which is what the real publisher does and
    /// what makes retention part of the graph rather than a footnote.
    fn publish_manifest(&mut self, r: &str, m: Manifest) {
        let layout = TenantLayout::new(r);
        let json = m.to_json();
        self.epoch += 1;
        self.store
            .insert(layout.manifest_history(self.epoch, self.now), json.clone());
        self.store.insert(layout.manifest(), json);
    }

    /// Where a capture's bytes live. One object per capture is enough:
    /// the collector decides per checkpoint id and deletes every key
    /// under it, so a second object under the same id is a second copy
    /// of the same question.
    fn capture(owner: &str, id: &str) -> String {
        TenantLayout::new(owner).chk_index(id)
    }
}

/// What can happen next, and what it does.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    /// A fold uploads a capture. Nothing references it yet.
    Upload(&'static str),
    /// The manifest gains the reference, and the fold is done.
    Publish(&'static str),
    /// A later fold supersedes it, or a branch drops it.
    Supersede(&'static str),
    /// The uploader died. The bytes stay and nothing will ever name
    /// them, which is exactly what the collector is for.
    Abandon(&'static str),
    /// A branch reads the parent's manifest. Nothing is written.
    BranchStart,
    /// The branch writes its own manifest, copying what it read with
    /// the owner tags branch creation writes, so the child names the
    /// parent's bytes without copying any.
    BranchFinish,
    /// The process creating the branch died before writing anything.
    BranchAbandon,
    /// The branch is deleted.
    DropBranch,
    /// A collector run.
    Gc,
    /// A second passes.
    Tick,
}

/// Everything enabled in this state, in a fixed order so the walk is
/// the same walk every time it runs.
fn steps(world: &World, mode: Mode) -> Vec<Step> {
    let mut out = Vec::new();
    let parent = world.manifest(PARENT).expect("the parent always exists");
    let named: Vec<&str> = parent.checkpoints.iter().map(|c| c.id.as_str()).collect();
    for id in IDS {
        let exists = world.store.contains_key(&World::capture(PARENT, id));
        let in_flight = world.inflight.contains_key(id);
        if !exists {
            out.push(Step::Upload(id));
        }
        if in_flight {
            out.push(Step::Publish(id));
            out.push(Step::Abandon(id));
        }
        if named.contains(&id) {
            out.push(Step::Supersede(id));
        }
    }
    match (world.branching.is_some(), world.manifest(CHILD).is_some()) {
        (true, _) => {
            out.push(Step::BranchFinish);
            out.push(Step::BranchAbandon);
        }
        (false, true) => out.push(Step::DropBranch),
        // A branch of nothing is a branch with nothing to pin, which is
        // a state the walk reaches anyway by branching and superseding.
        (false, false) if !named.is_empty() => out.push(Step::BranchStart),
        (false, false) => {}
    }
    // The guards, and the whole difference between the modes: a run may
    // not observe a publisher that has been unfinished for a window,
    // because finishing inside one is the operator's promise rather
    // than the collector's, and the collector cannot keep it for them.
    let overdue = |since: u64| world.now.saturating_sub(since) >= WINDOW;
    let slow_fold = world.inflight.values().any(|since| overdue(*since));
    let slow_branch = world.branching.iter().any(|(since, _)| overdue(*since));
    let blocked = (slow_fold && mode.folds_finish_in_time())
        || (slow_branch && mode.branches_finish_in_time());
    if !blocked {
        out.push(Step::Gc);
    }
    if world.now < HORIZON {
        out.push(Step::Tick);
    }
    out
}

fn apply(world: &World, step: &Step) -> World {
    let mut next = world.clone();
    match step {
        Step::Upload(id) => {
            next.store
                .insert(World::capture(PARENT, id), b"a capture".to_vec());
            next.inflight.insert((*id).to_string(), next.now);
        }
        Step::Publish(id) => {
            next.inflight.remove(*id);
            let mut m = next.manifest(PARENT).expect("the parent always exists");
            m.checkpoints.push(CheckpointRef {
                id: (*id).to_string(),
                lsn: Lsn(0x100),
                kind: CheckpointKind::Full,
                owner: None,
            });
            next.publish_manifest(PARENT, m);
        }
        Step::Supersede(id) => {
            let mut m = next.manifest(PARENT).expect("the parent always exists");
            m.checkpoints.retain(|c| c.id != *id);
            next.publish_manifest(PARENT, m);
        }
        Step::Abandon(id) => {
            next.inflight.remove(*id);
        }
        Step::BranchStart => {
            let parent = next.manifest(PARENT).expect("the parent always exists");
            let read = parent.checkpoints.iter().map(|c| c.id.clone()).collect();
            next.branching = Some((next.now, read));
        }
        Step::BranchFinish => {
            let (_, read) = next.branching.take().expect("only offered when branching");
            let mut child = Manifest::new(CHILD, 18);
            // What branch creation does: inherit the refs and tag every
            // untagged one with the tenant whose prefix holds it, so a
            // grandchild still reaches the bytes when branch_of only
            // names its direct parent.
            child.checkpoints = read
                .into_iter()
                .map(|id| CheckpointRef {
                    id,
                    lsn: Lsn(0x100),
                    kind: CheckpointKind::Full,
                    owner: Some(PARENT.to_string()),
                })
                .collect();
            child.branch_of = Some(BranchOf {
                tenant_ref: PARENT.to_string(),
                at_lsn: Lsn(0x100),
            });
            next.publish_manifest(CHILD, child);
        }
        Step::BranchAbandon => {
            next.branching = None;
        }
        Step::DropBranch => {
            let layout = TenantLayout::new(CHILD);
            next.store.remove(&layout.manifest());
        }
        Step::Gc => {
            let store = MemStore::new();
            for (key, data) in &next.store {
                store.put(key, data).expect("the model store takes writes");
            }
            gc::run_with(
                &store,
                next.now,
                Policy {
                    window_secs: WINDOW,
                    retention_secs: RETENTION,
                    ..Policy::default()
                },
            )
            .expect("the collector does not fail on anything this writes");
            next.store = store
                .list("")
                .expect("list")
                .into_iter()
                .map(|key| {
                    let (data, _) = store.get(&key).expect("get").expect("just listed");
                    (key, data)
                })
                .collect();
        }
        Step::Tick => next.now += 1,
    }
    next
}

/// A history key's tenant and the second it was written, or `None` if
/// the key is not one. The collector reads the same two things out of
/// the same name, and reads them the same way.
fn history_of(key: &str) -> Option<(String, u64)> {
    let rest = key.strip_prefix("tenants/")?;
    let (r, under) = rest.split_once('/')?;
    let name = under.strip_prefix("manifests/")?;
    let (_, unix) = name.strip_suffix(".json")?.split_once('-')?;
    Some((r.to_string(), unix.parse().ok()?))
}

/// The property. Everything a reader could still open has to be there:
/// every capture a live manifest names, and every capture a history
/// snapshot inside the retention window names, since that snapshot is
/// the promise point in time recovery makes.
///
/// A snapshot past retention is allowed to dangle. It is garbage
/// itself, and whatever only it referenced follows it out.
fn broken(world: &World) -> Option<String> {
    for r in [PARENT, CHILD] {
        let Some(m) = world.manifest(r) else { continue };
        for c in &m.checkpoints {
            let owner = c.owner.clone().unwrap_or_else(|| r.to_string());
            let key = World::capture(&owner, &c.id);
            if !world.store.contains_key(&key) {
                return Some(format!(
                    "the live manifest of {r} names {key}, which is gone"
                ));
            }
        }
    }
    for (key, data) in &world.store {
        let Some((r, stamp)) = history_of(key) else {
            continue;
        };
        if world.now.saturating_sub(stamp) >= RETENTION {
            continue;
        }
        let m = Manifest::from_json(data).expect("this model wrote it");
        for c in &m.checkpoints {
            let owner = c.owner.clone().unwrap_or_else(|| r.clone());
            let capture = World::capture(&owner, &c.id);
            if !world.store.contains_key(&capture) {
                return Some(format!(
                    "the snapshot {key} is inside retention and names {capture}, which is gone"
                ));
            }
        }
    }
    None
}

/// What the walk saw, which is as much of the result as the numbers
/// that say the walk was worth running.
struct Report {
    states: usize,
    /// The shortest trace to a broken state, if there is one.
    counterexample: Option<(Vec<Step>, String)>,
    /// Whether any run in the walk actually deleted something. A walk
    /// where nothing is ever collected holds the safety property for
    /// the least interesting reason there is.
    collected: bool,
    /// States where a child names a capture its parent has already
    /// dropped, which is the owner tag doing the only job it has.
    orphan_pins: usize,
    /// States where the only thing naming a capture is a snapshot
    /// inside the retention window.
    snapshot_pins: usize,
}

/// Walk every reachable state, breadth first, so the first violation
/// found is reached by the shortest path there is to it.
fn walk(mode: Mode) -> Report {
    let start = World::new();
    let mut seen: HashMap<World, (Option<World>, Option<Step>)> = HashMap::new();
    let mut queue = VecDeque::new();
    seen.insert(start.clone(), (None, None));
    queue.push_back((start, 0usize));
    let mut report = Report {
        states: 0,
        counterexample: None,
        collected: false,
        orphan_pins: 0,
        snapshot_pins: 0,
    };
    while let Some((world, depth)) = queue.pop_front() {
        let (orphan, snapshot) = interesting(&world);
        report.orphan_pins += usize::from(orphan);
        report.snapshot_pins += usize::from(snapshot);
        if let Some(why) = broken(&world) {
            report.states = seen.len();
            report.counterexample = Some((trace(&seen, &world), why));
            return report;
        }
        if depth == DEPTH {
            continue;
        }
        for step in steps(&world, mode) {
            let next = apply(&world, &step);
            if step == Step::Gc && next.store.len() < world.store.len() {
                report.collected = true;
            }
            if seen.contains_key(&next) {
                continue;
            }
            seen.insert(next.clone(), (Some(world.clone()), Some(step)));
            queue.push_back((next, depth + 1));
        }
    }
    report.states = seen.len();
    report
}

/// Whether this state is one of the two the pinning rules exist for.
/// Counted so that a walk which stopped reaching them fails loudly
/// instead of passing on the easy states alone.
fn interesting(world: &World) -> (bool, bool) {
    let parent = world.manifest(PARENT);
    let child = world.manifest(CHILD);
    let named = |m: &Option<Manifest>, id: &str| {
        m.as_ref()
            .map(|m| m.checkpoints.iter().any(|c| c.id == id))
            .unwrap_or(false)
    };
    let orphan = child
        .as_ref()
        .map(|m| {
            m.checkpoints
                .iter()
                .any(|c| c.owner.as_deref() == Some(PARENT) && !named(&parent, &c.id))
        })
        .unwrap_or(false);
    let mut snapshot = false;
    for (key, data) in &world.store {
        let Some((_, stamp)) = history_of(key) else {
            continue;
        };
        if world.now.saturating_sub(stamp) >= RETENTION {
            continue;
        }
        let m = Manifest::from_json(data).expect("this model wrote it");
        snapshot |= m
            .checkpoints
            .iter()
            .any(|c| !named(&parent, &c.id) && !named(&child, &c.id));
    }
    (orphan, snapshot)
}

/// The steps that led to a state, oldest first.
fn trace(seen: &HashMap<World, (Option<World>, Option<Step>)>, at: &World) -> Vec<Step> {
    let mut out = Vec::new();
    let mut here = at.clone();
    while let Some((Some(prev), Some(step))) = seen.get(&here).cloned() {
        out.push(step);
        here = prev;
    }
    out.reverse();
    out
}

/// A violation, said the way a person reading a failure wants it.
fn readout(mode: Mode, report: &Report) -> String {
    let Some((trace, why)) = &report.counterexample else {
        return format!("no violation in {} states under {mode:?}", report.states);
    };
    let steps: Vec<String> = trace.iter().map(|s| format!("{s:?}")).collect();
    format!(
        "{why}\nunder {mode:?}, reached in {} steps: {}",
        trace.len(),
        steps.join(" -> ")
    )
}

#[test]
fn nothing_a_live_manifest_names_is_ever_collected() {
    let walked = walk(Mode::Documented);
    assert!(
        walked.counterexample.is_none(),
        "{}",
        readout(Mode::Documented, &walked)
    );
    assert!(
        walked.collected,
        "no run in {} states ever deleted anything, so this held for the wrong reason",
        walked.states,
    );
    // None of these is an assertion about the number, which will move
    // with the bounds. They are here so that a walk which stopped
    // reaching the states the pinning rules exist for fails rather than
    // passing quietly on the easy half of the graph.
    assert!(
        walked.orphan_pins > 0,
        "no state had a branch holding a capture its parent had dropped",
    );
    assert!(
        walked.snapshot_pins > 0,
        "no state had a snapshot as the only thing naming a capture",
    );
    assert!(
        walked.states > 1000,
        "only {} states, the walk collapsed",
        walked.states,
    );
}

#[test]
fn a_fold_slower_than_the_window_loses_what_it_uploaded() {
    let walked = walk(Mode::SlowFold);
    let Some((trace, why)) = &walked.counterexample else {
        panic!(
            "no violation in {} states, so the safe walk proves nothing",
            walked.states
        );
    };
    println!("{}", readout(Mode::SlowFold, &walked));
    // The shape is the point, so it is asserted rather than only
    // printed: an upload, then two runs far enough apart to stamp it
    // and then take it, and only then the publish that names it.
    assert!(
        trace.iter().any(|s| matches!(s, Step::Upload(_))),
        "the thing lost has to have been uploaded: {trace:?}"
    );
    assert!(
        trace.iter().filter(|s| **s == Step::Gc).count() >= 2,
        "one run can never delete, so a trace with one is a different bug: {trace:?}"
    );
    assert!(
        why.contains("live manifest"),
        "a fold that publishes late dangles its own manifest: {why}"
    );
}

#[test]
fn a_branch_slower_than_the_window_loses_what_it_inherited() {
    let walked = walk(Mode::SlowBranch);
    let Some((trace, why)) = &walked.counterexample else {
        panic!(
            "no violation in {} states, so the safe walk proves nothing",
            walked.states
        );
    };
    println!("{}", readout(Mode::SlowBranch, &walked));
    // This one has a shape the fold case cannot produce: the capture is
    // published and superseded by its owner, and the only thing left
    // naming it is the branch that read the parent before the supersede
    // and wrote itself after the collector had been past.
    assert!(
        trace.contains(&Step::BranchStart) && trace.contains(&Step::BranchFinish),
        "the loss is between the two halves of a branch: {trace:?}"
    );
    assert!(
        trace.iter().filter(|s| **s == Step::Gc).count() >= 2,
        "one run can never delete, so a trace with one is a different bug: {trace:?}"
    );
    assert!(
        why.contains(CHILD),
        "it is the child that ends up naming nothing: {why}"
    );
}

/// A prefix with no live manifest is not scanned at all, so nothing
/// under it is collected. That is worth a test of its own because it is
/// the opposite of what the two phase design reads like it should do,
/// and because it is the reason a half deleted branch leaks rather than
/// tidying itself up.
#[test]
fn a_prefix_with_no_manifest_is_neither_pinned_nor_collected() {
    let store = MemStore::new();
    let parent = TenantLayout::new(PARENT);
    store
        .put(&parent.manifest(), &Manifest::new(PARENT, 18).to_json())
        .expect("put");
    // A branch that wrote its captures and never wrote its manifest.
    let stray = TenantLayout::new(CHILD).chk_index("a");
    store.put(&stray, b"orphaned bytes").expect("put");

    for now in [100, 200, 300] {
        gc::run(&store, now, 10, 1000).expect("the collector runs");
    }
    assert!(
        store.get(&stray).expect("get").is_some(),
        "a prefix with no manifest was collected, which would make branch creation racy"
    );
}
