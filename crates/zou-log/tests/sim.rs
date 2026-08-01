//! Deterministic simulation of the chain protocol.
//!
//! One scenario is a scripted life of a WAL shard: a sequencer commits
//! frames for a few tenants, consolidation and GC run in between, and
//! at one chosen store operation the node is killed, either before the
//! operation applied or after it applied but before the node learned
//! the outcome. A successor takes over, writers retry everything
//! unacked, the script finishes, and the ledger verifier recovers the
//! shard from the store alone and holds it to the honest rules:
//!
//! - Every acked frame is durable, exactly once after consolidation.
//! - Every recovered frame was issued by a writer, so a fenced zombie
//!   never smuggles anything in.
//! - Per tenant the recovered lsn stream is continuous, no gaps, no
//!   reorders.
//! - Takeover always succeeds, the shard is never wedged.
//!
//! The sweep test kills the first node at every store operation the
//! clean run performs, in both modes, which is the "kill the sequencer
//! at every await point" box: in this synchronous implementation every
//! await point is a store call. The seed test then randomizes the
//! script and the kill across many seeds; run the full count with
//! `ZOU_SIM_SEEDS=100000 cargo test --release -p zou-log --test sim`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use zou_log::{
    MediaSink, Sequencer, SequencerConfig, TeeFilter, WalMedia, catch_up, consolidate, gc_landing,
    take_over,
};
use zou_store::{CasError, CasStore, Frame2, Lsn, MemStore, Version};

/// SplitMix64, the whole rng the sim needs: deterministic per seed and
/// good enough to scatter scripts and kill points.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillMode {
    /// The node dies before the operation reaches the store.
    BeforeApply,
    /// The operation lands durably but the node dies before it learns
    /// the outcome, the zombie case.
    AfterApply,
}

/// A store handle belonging to one node. Operations count up, and at
/// `kill_at` the node dies: that operation and every later one on this
/// handle returns an io error, with the kill mode deciding whether the
/// fatal operation itself applied.
struct KillStore {
    inner: Arc<dyn CasStore>,
    ops: Arc<AtomicU64>,
    kill_at: u64,
    mode: KillMode,
    dead: Arc<AtomicBool>,
}

impl KillStore {
    fn call<T>(
        &self,
        key: &str,
        f: impl FnOnce(&dyn CasStore) -> Result<T, CasError>,
    ) -> Result<T, CasError> {
        let died = |key: &str| CasError::Io {
            key: key.to_string(),
            source: std::io::Error::other("node killed"),
        };
        if self.dead.load(Ordering::SeqCst) {
            return Err(died(key));
        }
        let op = self.ops.fetch_add(1, Ordering::SeqCst);
        if op == self.kill_at {
            self.dead.store(true, Ordering::SeqCst);
            if self.mode == KillMode::AfterApply {
                let _ = f(self.inner.as_ref());
            }
            return Err(died(key));
        }
        f(self.inner.as_ref())
    }
}

impl CasStore for KillStore {
    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, CasError> {
        self.call(key, |s| s.get(key))
    }

    fn put_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected: Option<&Version>,
    ) -> Result<Version, CasError> {
        self.call(key, |s| s.put_if_match(key, data, expected))
    }

    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<Version, CasError> {
        self.call(key, |s| s.put_if_absent(key, data))
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, CasError> {
        self.call(key, |s| s.get_range(key, offset, len))
    }

    fn delete(&self, key: &str) -> Result<(), CasError> {
        self.call(key, |s| s.delete(key))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CasError> {
        self.call(prefix, |s| s.list(prefix))
    }
}

#[derive(Debug, Clone, Copy)]
enum Action {
    Append(u128),
    Consolidate,
    Gc,
}

/// What every writer ever handed to a sequencer, and how much of it
/// was acked. The verifier holds the recovered shard to this.
struct Ledger {
    issued: HashMap<u128, Vec<Frame2>>,
    acked: HashMap<u128, usize>,
}

impl Ledger {
    fn new(tenants: &[u128]) -> Self {
        Self {
            issued: tenants.iter().map(|t| (*t, Vec::new())).collect(),
            acked: tenants.iter().map(|t| (*t, 0)).collect(),
        }
    }

    /// Issue the next frame of a tenant's continuous lsn stream.
    fn issue(&mut self, tenant: u128) -> Frame2 {
        let stream = self.issued.get_mut(&tenant).unwrap();
        let start = stream.last().map(|f| f.end_lsn.0).unwrap_or(100);
        let payload = format!("t{tenant}:{start}").into_bytes();
        let frame = Frame2 {
            tenant,
            writer_epoch: 1,
            start_lsn: Lsn(start),
            end_lsn: Lsn(start + payload.len() as u64),
            contains_commit: true,
            first_of_epoch: false,
            hints: Vec::new(),
            payload,
        };
        stream.push(frame.clone());
        frame
    }

    fn ack(&mut self, tenant: u128) {
        *self.acked.get_mut(&tenant).unwrap() += 1;
    }

    fn unacked(&self, tenant: u128) -> Vec<Frame2> {
        self.issued[&tenant][self.acked[&tenant]..].to_vec()
    }
}

fn sequencer(media: &Arc<WalMedia>, shard: u32, t: zou_log::Takeover) -> Sequencer {
    let sink = Arc::new(MediaSink::new(Arc::clone(media), shard));
    let config = SequencerConfig {
        window: Duration::ZERO,
        ..SequencerConfig::default()
    };
    Sequencer::resume(shard, sink as _, config, t.next_seq, t.prev_digest)
}

/// Run one scripted shard life with node A killed at `kill_at`, verify
/// the ledger, and return how many store operations node A performed,
/// which is what the sweep uses to enumerate every await point.
fn run_scenario(actions: &[Action], tenants: &[u128], kill_at: u64, mode: KillMode) -> u64 {
    let shard = 1;
    let mem = Arc::new(MemStore::new());
    let mut ledger = Ledger::new(tenants);
    let ctx = format!("kill_at={kill_at} mode={mode:?}");

    // Node A, the one that dies.
    let a_ops = Arc::new(AtomicU64::new(0));
    let a_dead = Arc::new(AtomicBool::new(false));
    let a_store: Arc<dyn CasStore> = Arc::new(KillStore {
        inner: Arc::clone(&mem) as Arc<dyn CasStore>,
        ops: Arc::clone(&a_ops),
        kill_at,
        mode,
        dead: Arc::clone(&a_dead),
    });
    let media_a = Arc::new(WalMedia::single(a_store));

    let mut idx = 0;
    let seq_a = match take_over(&media_a, shard, "node-a") {
        Ok(t) => {
            let seq = sequencer(&media_a, shard, t);
            while idx < actions.len() {
                let outcome = match actions[idx] {
                    Action::Append(tenant) => {
                        let frame = ledger.issue(tenant);
                        match seq.append(vec![frame]).and_then(|t| t.wait()) {
                            Ok(_) => {
                                ledger.ack(tenant);
                                Ok(())
                            }
                            Err(e) => Err(format!("{e}")),
                        }
                    }
                    Action::Consolidate => consolidate(&media_a, shard)
                        .map(drop)
                        .map_err(|e| format!("{e}")),
                    Action::Gc => gc_landing(&media_a, shard, Duration::ZERO)
                        .map(drop)
                        .map_err(|e| format!("{e}")),
                };
                match outcome {
                    Ok(()) => idx += 1,
                    Err(e) => {
                        assert!(
                            a_dead.load(Ordering::SeqCst),
                            "{ctx}: node A failed while alive at action {idx}: {e}"
                        );
                        break;
                    }
                }
            }
            Some(seq)
        }
        Err(e) => {
            assert!(
                a_dead.load(Ordering::SeqCst),
                "{ctx}: takeover by node A failed while alive: {e}"
            );
            None
        }
    };

    // Node B, the successor, never killed. It fences A, retries what A
    // never acked, and finishes the script.
    let media_b = Arc::new(WalMedia::single(Arc::clone(&mem) as Arc<dyn CasStore>));
    let t = take_over(&media_b, shard, "node-b")
        .unwrap_or_else(|e| panic!("{ctx}: the shard is wedged, node B cannot take over: {e}"));
    let seq_b = sequencer(&media_b, shard, t);

    // The zombie poke: whatever state A died in, an append through it
    // must fail now, because B sealed A's next chain position.
    if let Some(seq_a) = seq_a {
        let cursor = ledger.issued[&tenants[0]]
            .last()
            .map(|f| f.end_lsn.0)
            .unwrap_or(100);
        let probe = Frame2 {
            tenant: tenants[0],
            writer_epoch: 1,
            start_lsn: Lsn(cursor),
            end_lsn: Lsn(cursor + 6),
            contains_commit: true,
            first_of_epoch: false,
            hints: Vec::new(),
            payload: b"zombie".to_vec(),
        };
        let refused = match seq_a.append(vec![probe]) {
            Ok(ticket) => ticket.wait().is_err(),
            Err(_) => true,
        };
        assert!(refused, "{ctx}: a fenced sequencer acked an append");
        drop(seq_a);
    }

    for &tenant in tenants {
        for frame in ledger.unacked(tenant) {
            seq_b
                .append(vec![frame])
                .and_then(|t| t.wait())
                .unwrap_or_else(|e| panic!("{ctx}: retry on node B failed: {e}"));
            ledger.ack(tenant);
        }
    }
    while idx < actions.len() {
        match actions[idx] {
            Action::Append(tenant) => {
                let frame = ledger.issue(tenant);
                seq_b
                    .append(vec![frame])
                    .and_then(|t| t.wait())
                    .unwrap_or_else(|e| panic!("{ctx}: append on node B failed: {e}"));
                ledger.ack(tenant);
            }
            Action::Consolidate => {
                consolidate(&media_b, shard)
                    .unwrap_or_else(|e| panic!("{ctx}: consolidate on node B failed: {e}"));
            }
            Action::Gc => {
                gc_landing(&media_b, shard, Duration::ZERO)
                    .unwrap_or_else(|e| panic!("{ctx}: gc on node B failed: {e}"));
            }
        }
        idx += 1;
    }
    drop(seq_b);

    // The verifier recovers the shard from the store alone: take over,
    // fold everything, and compare against the ledger.
    let media_v = Arc::new(WalMedia::single(Arc::clone(&mem) as Arc<dyn CasStore>));
    take_over(&media_v, shard, "verifier")
        .unwrap_or_else(|e| panic!("{ctx}: the verifier cannot take over: {e}"));
    let mut folds = 0;
    while consolidate(&media_v, shard)
        .unwrap_or_else(|e| panic!("{ctx}: the verifier cannot consolidate: {e}"))
        .is_some()
    {
        folds += 1;
        assert!(folds < 8, "{ctx}: consolidation does not converge");
    }
    for &tenant in tenants {
        let recovered = catch_up(&media_v, shard, &TeeFilter::Tenant(tenant), Lsn(0))
            .unwrap_or_else(|e| panic!("{ctx}: recovery read failed: {e}"));
        let got: Vec<(u64, Vec<u8>)> = recovered
            .iter()
            .map(|f| (f.start_lsn.0, f.payload.clone()))
            .collect();
        let want: Vec<(u64, Vec<u8>)> = ledger.issued[&tenant]
            .iter()
            .map(|f| (f.start_lsn.0, f.payload.clone()))
            .collect();
        assert_eq!(
            got, want,
            "{ctx}: tenant {tenant} recovered stream differs from the ledger"
        );
        for pair in recovered.windows(2) {
            assert_eq!(
                pair[0].end_lsn, pair[1].start_lsn,
                "{ctx}: tenant {tenant} recovered stream has a gap"
            );
        }
    }

    a_ops.load(Ordering::SeqCst)
}

/// The fixed script for the sweep: appends for two tenants around a
/// consolidation and a GC, so the kill lands inside takeover, landing
/// PUTs, chain reads, the sealed PUT, the round PUT, the manifest CAS,
/// and the GC deletes, depending on the operation index.
const SWEEP_TENANTS: [u128; 2] = [7, 9];

fn sweep_script() -> Vec<Action> {
    vec![
        Action::Append(7),
        Action::Append(9),
        Action::Append(7),
        Action::Consolidate,
        Action::Append(9),
        Action::Gc,
        Action::Append(7),
        Action::Consolidate,
        Action::Append(9),
        Action::Gc,
        Action::Append(7),
    ]
}

#[test]
fn the_first_node_dies_at_every_store_operation_and_the_ledger_holds() {
    let script = sweep_script();
    let total = run_scenario(&script, &SWEEP_TENANTS, u64::MAX, KillMode::BeforeApply);
    assert!(total > 20, "the sweep script is too small to mean anything");
    for kill_at in 0..total {
        for mode in [KillMode::BeforeApply, KillMode::AfterApply] {
            run_scenario(&script, &SWEEP_TENANTS, kill_at, mode);
        }
    }
}

#[test]
fn random_scripts_and_kills_hold_the_ledger_over_many_seeds() {
    let seeds: u64 = std::env::var("ZOU_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000);
    for seed in 0..seeds {
        let mut rng = Rng(seed);
        let tenants: Vec<u128> = (0..1 + rng.below(2)).map(|i| 7 + 2 * i as u128).collect();
        let actions: Vec<Action> = (0..4 + rng.below(10))
            .map(|_| match rng.below(12) {
                0..=7 => Action::Append(tenants[rng.below(tenants.len() as u64) as usize]),
                8 | 9 => Action::Consolidate,
                _ => Action::Gc,
            })
            .collect();
        let kill_at = rng.below(64);
        let mode = if rng.below(2) == 0 {
            KillMode::BeforeApply
        } else {
            KillMode::AfterApply
        };
        run_scenario(&actions, &tenants, kill_at, mode);
    }
}
