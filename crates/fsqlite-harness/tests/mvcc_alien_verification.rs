use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use asupersync::lab::{DporExplorer, ExplorerConfig, LabConfig, LabRuntime};
use asupersync::runtime::yield_now;
use parking_lot::Mutex;
use proptest::prelude::*;

use fsqlite_harness::tla::{MvccStateSnapshot, MvccTlaExporter, TlaValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnOutcome {
    Committed,
    Aborted,
}

#[derive(Debug, Default)]
struct ModelState {
    // Global commit clock.
    commit_seq_hi: u64,
    // commit_index[page] = latest commit_seq that wrote page.
    commit_index: BTreeMap<u32, u64>,
    // Recently committed transactions' read evidence (committed pivots are possible).
    committed_readers: Vec<CommittedReaderEntry>,
    // Trace snapshots for TLA+ export/debug.
    trace: Vec<MvccStateSnapshot>,
}

#[derive(Debug, Clone)]
struct CommittedReaderEntry {
    txn_id: u64,
    begin_seq: u64,
    commit_seq: u64,
    has_in_rw: bool,
    read_pages: Vec<u32>,
}

impl ModelState {
    fn snapshot(&mut self, label: impl Into<String>, bucket: &HotBucket) {
        let mut vars = BTreeMap::new();
        vars.insert(
            "commit_seq_hi".to_string(),
            TlaValue::Nat(self.commit_seq_hi),
        );
        vars.insert(
            "hot_epoch".to_string(),
            TlaValue::Nat(u64::from(bucket.hot_epoch())),
        );
        vars.insert(
            "bucket".to_string(),
            TlaValue::Record(bucket.to_tla_record()),
        );
        vars.insert(
            "committed_readers".to_string(),
            TlaValue::Seq(
                self.committed_readers
                    .iter()
                    .map(|e| {
                        let mut r = BTreeMap::new();
                        r.insert("txn_id".to_string(), TlaValue::Nat(e.txn_id));
                        r.insert("begin_seq".to_string(), TlaValue::Nat(e.begin_seq));
                        r.insert("commit_seq".to_string(), TlaValue::Nat(e.commit_seq));
                        r.insert("has_in_rw".to_string(), TlaValue::Bool(e.has_in_rw));
                        r.insert(
                            "read_pages".to_string(),
                            TlaValue::Seq(
                                e.read_pages
                                    .iter()
                                    .copied()
                                    .map(|p| TlaValue::Nat(u64::from(p)))
                                    .collect(),
                            ),
                        );
                        TlaValue::Record(r)
                    })
                    .collect(),
            ),
        );
        self.trace.push(MvccStateSnapshot {
            label: label.into(),
            vars,
        });
    }
}

/// A minimal, SHM-shaped hot witness bucket with double-buffered epochs and a spinlock.
///
/// This is a *model* used for DPOR/injection testing. It is intentionally small:
/// one bucket entry, readers-only, 128 slots (two u64 words).
struct HotBucket {
    hot_epoch: AtomicU32,

    epoch_lock: AtomicU32,

    epoch_a: AtomicU32,
    readers_a: [AtomicU64; 2],

    epoch_b: AtomicU32,
    readers_b: [AtomicU64; 2],
}

impl HotBucket {
    fn new(initial_epoch: u32) -> Self {
        Self {
            hot_epoch: AtomicU32::new(initial_epoch),
            epoch_lock: AtomicU32::new(0),
            epoch_a: AtomicU32::new(0),
            readers_a: [AtomicU64::new(0), AtomicU64::new(0)],
            epoch_b: AtomicU32::new(0),
            readers_b: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }

    fn hot_epoch(&self) -> u32 {
        self.hot_epoch.load(Ordering::Acquire)
    }

    fn to_tla_record(&self) -> BTreeMap<String, TlaValue> {
        let mut r = BTreeMap::new();
        r.insert(
            "epoch_a".to_string(),
            TlaValue::Nat(u64::from(self.epoch_a())),
        );
        r.insert(
            "epoch_b".to_string(),
            TlaValue::Nat(u64::from(self.epoch_b())),
        );
        r.insert(
            "readers_a".to_string(),
            TlaValue::Set(
                self.readers_bits(self.epoch_a())
                    .into_iter()
                    .map(TlaValue::Nat)
                    .collect(),
            ),
        );
        r.insert(
            "readers_b".to_string(),
            TlaValue::Set(
                self.readers_bits(self.epoch_b())
                    .into_iter()
                    .map(TlaValue::Nat)
                    .collect(),
            ),
        );
        r
    }

    fn epoch_a(&self) -> u32 {
        self.epoch_a.load(Ordering::Acquire)
    }

    fn epoch_b(&self) -> u32 {
        self.epoch_b.load(Ordering::Acquire)
    }

    fn readers_bits(&self, e: u32) -> Vec<u64> {
        let mut out = Vec::new();
        let (epoch, readers) = self.match_epoch(e);
        if !epoch {
            return out;
        }
        for (word_idx, w) in readers.iter().enumerate() {
            let mut bits = w.load(Ordering::Acquire);
            while bits != 0 {
                let lsb = bits & bits.wrapping_neg();
                let bit = lsb.trailing_zeros();
                let Ok(word) = u64::try_from(word_idx) else {
                    continue;
                };
                out.push(word * 64 + u64::from(bit));
                bits ^= lsb;
            }
        }
        out
    }

    fn match_epoch(&self, target: u32) -> (bool, &[AtomicU64; 2]) {
        if self.epoch_a.load(Ordering::Acquire) == target {
            return (true, &self.readers_a);
        }
        if self.epoch_b.load(Ordering::Acquire) == target {
            return (true, &self.readers_b);
        }
        (false, &self.readers_a)
    }

    fn set_reader_bit(&self, target_epoch: u32, slot_id: u32) {
        let (ok, readers) = self.match_epoch(target_epoch);
        assert!(ok, "epoch buffer must be installed before setting bit");
        let Ok(word) = usize::try_from(slot_id / 64) else {
            return;
        };
        if word >= readers.len() {
            return;
        }
        let bit = slot_id % 64;
        let mask = 1_u64 << bit;
        readers[word].fetch_or(mask, Ordering::Relaxed);
    }

    /// Install `target_epoch` into a stale buffer (clear-then-publish) and return which buffer.
    fn install_epoch(&self, target_epoch: u32, cur: u32, prev: u32) -> Option<Buffer> {
        // Acquire spinlock.
        while self
            .epoch_lock
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }

        // Re-check now that we hold the lock.
        if self.epoch_a.load(Ordering::Acquire) == target_epoch {
            self.epoch_lock.store(0, Ordering::Release);
            return Some(Buffer::A);
        }
        if self.epoch_b.load(Ordering::Acquire) == target_epoch {
            self.epoch_lock.store(0, Ordering::Release);
            return Some(Buffer::B);
        }

        let a_epoch = self.epoch_a.load(Ordering::Acquire);
        let b_epoch = self.epoch_b.load(Ordering::Acquire);

        let chosen = if a_epoch != cur && a_epoch != prev {
            Buffer::A
        } else if b_epoch != cur && b_epoch != prev {
            Buffer::B
        } else {
            // With 2 buffers and at most {cur, prev} live, this should be unreachable.
            self.epoch_lock.store(0, Ordering::Release);
            return None;
        };

        match chosen {
            Buffer::A => {
                for w in &self.readers_a {
                    w.store(0, Ordering::Relaxed);
                }
                self.epoch_a.store(target_epoch, Ordering::Release);
            }
            Buffer::B => {
                for w in &self.readers_b {
                    w.store(0, Ordering::Relaxed);
                }
                self.epoch_b.store(target_epoch, Ordering::Release);
            }
        }

        self.epoch_lock.store(0, Ordering::Release);
        Some(chosen)
    }

    async fn register_read(&self, slot_id: u32, target_epoch: u32) {
        let cur = self.hot_epoch();
        let prev = cur.wrapping_sub(1);

        // Fast path: buffer already tagged.
        if self.epoch_a.load(Ordering::Acquire) == target_epoch {
            self.set_reader_bit(target_epoch, slot_id);
            return;
        }
        if self.epoch_b.load(Ordering::Acquire) == target_epoch {
            self.set_reader_bit(target_epoch, slot_id);
            return;
        }

        // Slow path: install + clear is serialized. Setting the bit is outside the lock.
        let installed = self.install_epoch(target_epoch, cur, prev);
        assert!(installed.is_some(), "epoch install failed unexpectedly");

        // Intentional yield point: another task may observe the installed epoch and set its bit.
        yield_now().await;

        self.set_reader_bit(target_epoch, slot_id);
    }

    fn readers_for_epoch(&self, target_epoch: u32) -> Vec<u32> {
        let mut out = Vec::new();
        let (ok, readers) = self.match_epoch(target_epoch);
        if !ok {
            return out;
        }
        for (word_idx, w) in readers.iter().enumerate() {
            let mut bits = w.load(Ordering::Acquire);
            while bits != 0 {
                let lsb = bits & bits.wrapping_neg();
                let bit = lsb.trailing_zeros();
                let Ok(word) = u32::try_from(word_idx) else {
                    continue;
                };
                out.push(word * 64 + bit);
                bits ^= lsb;
            }
        }
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Buffer {
    A,
    B,
}

fn build_tla_snapshots(n: usize) -> Vec<MvccStateSnapshot> {
    let mut snapshots = Vec::with_capacity(n);
    for step in 0..n {
        let mut vars = BTreeMap::new();
        vars.insert(
            "step".to_string(),
            TlaValue::Nat(u64::try_from(step).expect("step index should fit into u64")),
        );
        vars.insert("phase".to_string(), TlaValue::Str(format!("s{step}")));
        snapshots.push(MvccStateSnapshot {
            label: format!("state_{step}"),
            vars,
        });
    }
    snapshots
}

#[test]
fn tla_exporter_emits_a_module() {
    let snapshots = build_tla_snapshots(2);
    let exporter = MvccTlaExporter::from_snapshots(snapshots);
    let m = exporter.export_behavior("MvccTest");
    assert!(m.source.contains("---- MODULE MvccTest ----"));
    assert!(m.source.contains("States =="));
    assert!(m.source.contains("Spec =="));
}

#[test]
fn test_tla_export_concrete_behavior() {
    let mut vars0 = BTreeMap::new();
    vars0.insert("commit_seq_hi".to_string(), TlaValue::Nat(0));
    let mut vars1 = BTreeMap::new();
    vars1.insert("commit_seq_hi".to_string(), TlaValue::Nat(1));

    let exporter = MvccTlaExporter::from_snapshots(vec![
        MvccStateSnapshot {
            label: "init".to_string(),
            vars: vars0,
        },
        MvccStateSnapshot {
            label: "commit".to_string(),
            vars: vars1,
        },
    ]);
    let module = exporter.export_behavior("MvccConcreteBehavior");

    assert!(
        module
            .source
            .contains("---- MODULE MvccConcreteBehavior ----")
    );
    assert!(module.source.contains("States =="));
    assert!(module.source.contains("Init =="));
    assert!(module.source.contains("Next =="));
    assert!(module.source.contains("Spec =="));
}

#[test]
fn test_tla_export_spec_skeleton() {
    let exporter = MvccTlaExporter::from_snapshots(Vec::new());
    let module = exporter.export_parametric_spec_skeleton(
        "MvccSkeleton",
        &["txn_state", "commit_index", "gc_horizon"],
        &["InvNoDirtyRead", "InvGcSafe"],
    );

    assert!(module.source.contains("---- MODULE MvccSkeleton ----"));
    assert!(
        module
            .source
            .contains("VARIABLES txn_state, commit_index, gc_horizon")
    );
    assert!(module.source.contains("Init =="));
    assert!(module.source.contains("Next =="));
    assert!(module.source.contains("Spec =="));
    assert!(module.source.contains("InvNoDirtyRead == TRUE"));
    assert!(module.source.contains("InvGcSafe == TRUE"));
}

#[test]
fn test_tla_export_deterministic() {
    let mut vars0 = BTreeMap::new();
    vars0.insert("commit_seq_hi".to_string(), TlaValue::Nat(2));
    let mut vars1 = BTreeMap::new();
    vars1.insert("commit_seq_hi".to_string(), TlaValue::Nat(3));
    vars1.insert("rw_flag".to_string(), TlaValue::Bool(true));

    let snapshots = vec![
        MvccStateSnapshot {
            label: "before".to_string(),
            vars: vars0,
        },
        MvccStateSnapshot {
            label: "after".to_string(),
            vars: vars1,
        },
    ];

    let exporter_a = MvccTlaExporter::from_snapshots(snapshots.clone());
    let exporter_b = MvccTlaExporter::from_snapshots(snapshots);

    let out_a = exporter_a.export_behavior("DeterministicTrace");
    let out_b = exporter_b.export_behavior("DeterministicTrace");
    assert_eq!(out_a.source, out_b.source);
}

#[test]
fn test_tla_mvcc_commit_scenario() {
    let bucket = HotBucket::new(10);
    let mut state = ModelState::default();
    state.snapshot("begin", &bucket);
    state.commit_seq_hi = 1;
    state.commit_index.insert(7, 1);
    state.snapshot("commit", &bucket);
    state.snapshot("abort", &bucket);

    let module = MvccTlaExporter::from_snapshots(state.trace).export_behavior("MvccCommitScenario");
    assert!(module.source.contains("\"begin\""));
    assert!(module.source.contains("\"commit\""));
    assert!(module.source.contains("\"abort\""));
}

#[test]
fn test_e2e_tla_export_roundtrip() {
    let mut vars = BTreeMap::new();
    vars.insert("step".to_string(), TlaValue::Nat(0));
    let snapshots = vec![MvccStateSnapshot {
        label: "single".to_string(),
        vars,
    }];

    let module = MvccTlaExporter::from_snapshots(snapshots).export_behavior("Roundtrip");
    assert!(module.source.starts_with("---- MODULE Roundtrip ----"));
    assert!(module.source.contains("Spec =="));
    assert!(module.source.trim_end().ends_with("===="));
    assert!(module.source.contains("\"single\""));
}

#[test]
fn tla_export_concrete_behavior_has_expected_transitions() {
    let snapshots = build_tla_snapshots(10);
    let exporter = MvccTlaExporter::from_snapshots(snapshots);
    let module = exporter.export_behavior("MvccDeterministic10");

    assert!(
        module
            .source
            .contains("---- MODULE MvccDeterministic10 ----")
    );
    assert_eq!(
        module.source.matches("\\/ /\\ step = ").count(),
        9,
        "10 snapshots should produce 9 Next transitions"
    );
    assert!(module.source.contains("/\\ step' = 10"));
}

#[test]
fn tla_export_is_deterministic_for_same_trace() {
    let snapshots = build_tla_snapshots(10);
    let exporter_a = MvccTlaExporter::from_snapshots(snapshots.clone());
    let exporter_b = MvccTlaExporter::from_snapshots(snapshots);

    let behavior_a = exporter_a.export_behavior("MvccDeterministic");
    let behavior_b = exporter_b.export_behavior("MvccDeterministic");
    assert_eq!(
        behavior_a.source, behavior_b.source,
        "same snapshots must export bit-identical concrete modules"
    );

    let skeleton_a = exporter_a.export_spec_skeleton("MvccSkeleton");
    let skeleton_b = exporter_b.export_spec_skeleton("MvccSkeleton");
    assert_eq!(
        skeleton_a.source, skeleton_b.source,
        "skeleton export must be deterministic for a given module name"
    );
}

#[test]
fn tla_export_spec_skeleton_contains_core_sections() {
    let exporter = MvccTlaExporter::from_snapshots(Vec::new());
    let module = exporter.export_spec_skeleton("MvccSpecSkeleton");

    assert!(module.source.contains("---- MODULE MvccSpecSkeleton ----"));
    assert!(module.source.contains("CONSTANTS Txns, Pages"));
    assert!(
        module
            .source
            .contains("VARIABLES commitSeq, snapshots, readSet, writeSet, gcHorizon")
    );
    assert!(module.source.contains("Init =="));
    assert!(module.source.contains("Next =="));
    assert!(module.source.contains("InvariantSI =="));
    assert!(module.source.contains("Spec =="));
}

#[test]
fn dpor_hot_witness_epoch_install_has_no_lost_bits() {
    let mut explorer = DporExplorer::new(ExplorerConfig::new(0, 64).max_steps(50_000));

    let report = explorer.explore(|rt| {
        let root = rt
            .state
            .create_root_region(asupersync::types::Budget::INFINITE);

        let bucket = Arc::new(HotBucket::new(10));
        let state = Arc::new(Mutex::new(ModelState {
            commit_seq_hi: 10,
            ..ModelState::default()
        }));

        {
            let mut s = state.lock();
            s.snapshot("init", &bucket);
        }

        // Two concurrent registrations into the same epoch.
        let bucket_a = Arc::clone(&bucket);
        let state_a = Arc::clone(&state);
        let (t_a, _) = rt
            .state
            .create_task(root, asupersync::types::Budget::INFINITE, async move {
                bucket_a.register_read(1, 10).await;
                let mut s = state_a.lock();
                s.snapshot("after A", &bucket_a);
            })
            .expect("spawn A");

        let bucket_b = Arc::clone(&bucket);
        let state_b = Arc::clone(&state);
        let (t_b, _) = rt
            .state
            .create_task(root, asupersync::types::Budget::INFINITE, async move {
                bucket_b.register_read(2, 10).await;
                let mut s = state_b.lock();
                s.snapshot("after B", &bucket_b);
            })
            .expect("spawn B");

        let mut sched = rt
            .scheduler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        sched.schedule(t_a, 0);
        sched.schedule(t_b, 0);
        drop(sched);

        rt.run_until_quiescent();

        // Invariant: both bits are present in the installed epoch buffer.
        let bits = bucket.readers_for_epoch(10);
        assert!(bits.contains(&1), "slot 1 missing");
        assert!(bits.contains(&2), "slot 2 missing");

        // Export trace to a behavior module (debug artifact; not written to disk here).
        let exporter = {
            let s = state.lock();
            MvccTlaExporter::from_snapshots(s.trace.clone())
        };
        let m = exporter.export_behavior("HotWitnessInstall");
        assert!(m.source.contains("States =="));
    });

    assert!(
        report.violations.is_empty(),
        "DPOR exploration found violations: {:#?}",
        report.violations
    );
}

#[test]
fn dpor_outgoing_edges_cover_committed_and_freed_writers() {
    let mut explorer = DporExplorer::new(ExplorerConfig::new(7, 64).max_steps(100_000));

    let report = explorer.explore(|rt| {
        let root = rt
            .state
            .create_root_region(asupersync::types::Budget::INFINITE);

        let bucket = Arc::new(HotBucket::new(100));
        let state = Arc::new(Mutex::new(ModelState {
            commit_seq_hi: 100,
            ..ModelState::default()
        }));

        // Reader transaction T.
        let state_t = Arc::clone(&state);
        let bucket_t = Arc::clone(&bucket);
        let (t_t, _) = rt
            .state
            .create_task(root, asupersync::types::Budget::INFINITE, async move {
                let begin_seq = {
                    let s = state_t.lock();
                    s.commit_seq_hi
                };

                // Simulate reading page 1 under the snapshot.
                yield_now().await;
                let read_pages = vec![1_u32];

                // Commit-time outgoing edge discovery:
                yield_now().await;
                let (out_edges, w_commit_seq_at_check) = {
                    let s = state_t.lock();
                    let out_edges =
                        discover_outgoing_edges(begin_seq, &read_pages, &s.commit_index);
                    let w_commit_seq = s.commit_index.get(&1).copied();
                    drop(s);
                    (out_edges, w_commit_seq)
                };
                state_t.lock().snapshot("T commit", &bucket_t);

                // If a writer committed after our begin and touched page 1 before we committed,
                // out_edges must be non-empty (no false negatives).
                if let Some(w_commit_seq) = w_commit_seq_at_check {
                    if w_commit_seq > begin_seq {
                        assert!(
                            out_edges.contains(&w_commit_seq),
                            "missing outgoing edge to committed writer"
                        );
                    }
                }
            })
            .expect("spawn T");

        // Writer W commits page 1 and then "frees its slot" (becomes invisible to hot plane).
        let state_w = Arc::clone(&state);
        let bucket_w = Arc::clone(&bucket);
        let (t_w, _) = rt
            .state
            .create_task(root, asupersync::types::Budget::INFINITE, async move {
                yield_now().await;
                let commit_seq = {
                    let mut s = state_w.lock();
                    s.commit_seq_hi += 1;
                    let commit_seq = s.commit_seq_hi;
                    s.commit_index.insert(1, commit_seq);
                    commit_seq
                };
                state_w
                    .lock()
                    .snapshot(format!("W committed {commit_seq}"), &bucket_w);
            })
            .expect("spawn W");

        {
            let mut sched = rt.scheduler.lock().expect("scheduler lock");
            sched.schedule(t_t, 0);
            sched.schedule(t_w, 0);
        }
        rt.run_until_quiescent();
    });

    assert!(
        report.violations.is_empty(),
        "DPOR exploration found violations: {:#?}",
        report.violations
    );
}

#[test]
fn chaos_cancel_does_not_leak_hot_witness_epoch_lock() {
    // This is not a correctness proof: it is a liveness/safety guardrail.
    // Under cancellation injection we should never leave a per-bucket lock held.
    for seed in 0_u64..16 {
        let mut rt = LabRuntime::new(
            LabConfig::new(seed)
                .with_light_chaos()
                .worker_count(2)
                .max_steps(50_000),
        );
        let root = rt
            .state
            .create_root_region(asupersync::types::Budget::INFINITE);

        let bucket = Arc::new(HotBucket::new(10));

        let bucket_a = Arc::clone(&bucket);
        let (t_a, _) = rt
            .state
            .create_task(root, asupersync::types::Budget::INFINITE, async move {
                bucket_a.register_read(1, 10).await;
            })
            .expect("spawn A");

        let bucket_b = Arc::clone(&bucket);
        let (t_b, _) = rt
            .state
            .create_task(root, asupersync::types::Budget::INFINITE, async move {
                bucket_b.register_read(2, 10).await;
            })
            .expect("spawn B");

        let mut sched = rt
            .scheduler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        sched.schedule(t_a, 0);
        sched.schedule(t_b, 0);
        drop(sched);

        rt.run_until_quiescent();

        assert_eq!(
            bucket.epoch_lock.load(Ordering::Acquire),
            0,
            "epoch lock leaked under chaos cancellation (seed={seed})"
        );
    }
}

#[test]
fn dpor_incoming_edges_cover_committed_pivots_via_committed_readers_index() {
    let mut explorer = DporExplorer::new(ExplorerConfig::new(99, 64).max_steps(150_000));

    let report = explorer.explore(|rt| {
        let root = rt
            .state
            .create_root_region(asupersync::types::Budget::INFINITE);

        let bucket = Arc::new(HotBucket::new(100));
        let state = Arc::new(Mutex::new(ModelState {
            commit_seq_hi: 100,
            ..ModelState::default()
        }));

        // Transaction R: reads page 1, commits, and is already known to have has_in_rw = true
        // (i.e., it has an incoming rw edge from some earlier txn X).
        let state_r = Arc::clone(&state);
        let bucket_r = Arc::clone(&bucket);
        let (t_r, _) = rt
            .state
            .create_task(root, asupersync::types::Budget::INFINITE, async move {
                let begin_seq = { state_r.lock().commit_seq_hi };
                let read_pages = vec![1_u32];

                yield_now().await;

                let commit_seq = {
                    let mut s = state_r.lock();
                    s.commit_seq_hi += 1;
                    s.commit_seq_hi
                };
                let entry = CommittedReaderEntry {
                    txn_id: 1,
                    begin_seq,
                    commit_seq,
                    has_in_rw: true,
                    read_pages,
                };
                let mut s = state_r.lock();
                s.committed_readers.push(entry);
                s.snapshot(format!("R committed {commit_seq}"), &bucket_r);
                drop(s);
            })
            .expect("spawn R");

        // Transaction T: begins before R commits, writes page 1, then attempts to commit.
        let state_t = Arc::clone(&state);
        let bucket_t = Arc::clone(&bucket);
        let (t_t, _) = rt
            .state
            .create_task(root, asupersync::types::Budget::INFINITE, async move {
                let begin_seq = { state_t.lock().commit_seq_hi };

                // Wait for R to possibly commit.
                yield_now().await;

                // Simulate a write to page 1.
                let write_pages = vec![1_u32];

                // Commit-time incoming edge discovery:
                yield_now().await;
                let (in_edges, r_committed_after_snapshot) = {
                    let s = state_t.lock();
                    let in_edges =
                        discover_incoming_edges(begin_seq, &write_pages, &s.committed_readers);
                    let r_after = s
                        .committed_readers
                        .iter()
                        .find(|e| e.txn_id == 1)
                        .is_some_and(|r| r.commit_seq > begin_seq);
                    drop(s);
                    (in_edges, r_after)
                };

                // Apply the committed-pivot T3 rule: if any committed edge source has has_in_rw,
                // this commit must abort.
                let mut outcome = TxnOutcome::Committed;
                for c in &in_edges {
                    if c.has_in_rw {
                        outcome = TxnOutcome::Aborted;
                    }
                }

                state_t
                    .lock()
                    .snapshot(format!("T outcome {outcome:?}"), &bucket_t);

                // If R committed after our snapshot and read a page we wrote, T must abort
                // because R is a committed pivot (X -rw-> R -rw-> T).
                if r_committed_after_snapshot {
                    assert!(
                        in_edges.iter().any(|c| c.txn_id == 1),
                        "missing incoming edge source txn_id=1"
                    );
                    assert_eq!(
                        outcome,
                        TxnOutcome::Aborted,
                        "committed pivot must force abort"
                    );
                }
            })
            .expect("spawn T");

        {
            let mut sched = rt.scheduler.lock().expect("scheduler lock");
            sched.schedule(t_r, 0);
            sched.schedule(t_t, 0);
        }
        rt.run_until_quiescent();
    });

    assert!(
        report.violations.is_empty(),
        "DPOR exploration found violations: {:#?}",
        report.violations
    );
}

fn discover_outgoing_edges(
    begin_seq: u64,
    read_pages: &[u32],
    commit_index: &BTreeMap<u32, u64>,
) -> Vec<u64> {
    // Outgoing edge T -rw-> W exists if W committed after T's snapshot and wrote a page T read.
    let mut out = Vec::new();
    for &p in read_pages {
        if let Some(&latest) = commit_index.get(&p) {
            if latest > begin_seq {
                out.push(latest);
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

#[derive(Debug, Clone)]
struct IncomingCandidate {
    txn_id: u64,
    has_in_rw: bool,
}

fn discover_incoming_edges(
    begin_seq: u64,
    write_pages: &[u32],
    committed_readers: &[CommittedReaderEntry],
) -> Vec<IncomingCandidate> {
    let mut out = Vec::new();
    for e in committed_readers {
        if e.commit_seq <= begin_seq {
            continue;
        }
        if !pages_overlap(write_pages, &e.read_pages) {
            continue;
        }
        out.push(IncomingCandidate {
            txn_id: e.txn_id,
            has_in_rw: e.has_in_rw,
        });
    }
    out
}

fn pages_overlap(a: &[u32], b: &[u32]) -> bool {
    // Small, deterministic overlap check (sortedness not required).
    for &x in a {
        if b.contains(&x) {
            return true;
        }
    }
    false
}

proptest! {
    #[test]
    fn prop_outgoing_edges_covers_committed_writer(begin_seq in 0_u64..1_000, w_commit_seq in 0_u64..1_000) {
        let mut commit_index = BTreeMap::new();
        commit_index.insert(1_u32, w_commit_seq);

        let out = discover_outgoing_edges(begin_seq, &[1_u32], &commit_index);

        if w_commit_seq > begin_seq {
            prop_assert!(out.contains(&w_commit_seq));
        } else {
            prop_assert!(!out.contains(&w_commit_seq));
        }
    }

    #[test]
    fn prop_incoming_edges_covers_committed_reader(begin_seq in 0_u64..1_000, r_commit_seq in 0_u64..1_000, has_in_rw in any::<bool>()) {
        let committed = CommittedReaderEntry {
            txn_id: 1,
            begin_seq,
            commit_seq: r_commit_seq,
            has_in_rw,
            read_pages: vec![1_u32],
        };

        let in_edges = discover_incoming_edges(begin_seq, &[1_u32], &[committed]);

        if r_commit_seq > begin_seq {
            prop_assert!(in_edges.iter().any(|c| c.txn_id == 1));
        } else {
            prop_assert!(!in_edges.iter().any(|c| c.txn_id == 1));
        }
    }
}
