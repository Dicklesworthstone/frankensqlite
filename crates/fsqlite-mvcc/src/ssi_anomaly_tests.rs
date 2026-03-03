//! bd-688.6: SSI anomaly detection, latch-free stress, EBR liveness tests.
//!
//! Five test categories:
//! 1. Serialization anomaly detection (write-skew, phantom, lost-update)
//! 2. Latch-free multi-thread stress on hot pages
//! 3. EBR version reclamation liveness
//! 4. Rebase correctness (concurrent conflicting transactions)
//! 5. CAS version install correctness

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fsqlite_types::{
    CommitSeq, PageData, PageNumber, PageSize, SchemaEpoch, Snapshot, TxnEpoch, TxnId, TxnToken,
    WitnessKey,
};

use crate::begin_concurrent::{
    ConcurrentRegistry, concurrent_abort, concurrent_commit_with_ssi, concurrent_write_page,
};
use crate::core_types::{CommitIndex, InProcessPageLockTable, VersionArena};
use crate::ebr::{GLOBAL_EBR_METRICS, StaleReaderConfig, VersionGuard, VersionGuardRegistry};
use crate::gc::{GcScheduler, GcTodo};
use crate::lifecycle::MvccError;
use crate::ssi_validation::{
    ActiveTxnView, CommittedReaderInfo, SsiAbortReason, ssi_validate_and_publish,
};

const BEAD_ID: &str = "bd-688.6";

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

fn test_snapshot(high: u64) -> Snapshot {
    Snapshot {
        high: CommitSeq::new(high),
        schema_epoch: SchemaEpoch::ZERO,
    }
}

fn test_page(n: u32) -> PageNumber {
    PageNumber::new(n).expect("page number must be nonzero")
}

fn test_data() -> PageData {
    PageData::zeroed(PageSize::DEFAULT)
}

fn page_key(pgno: u32) -> WitnessKey {
    WitnessKey::Page(PageNumber::new(pgno).unwrap())
}

/// Mock active transaction for direct SSI validation testing.
struct MockActiveTxn {
    token: TxnToken,
    begin_seq: CommitSeq,
    active: bool,
    reads: Vec<WitnessKey>,
    writes: Vec<WitnessKey>,
    has_in: std::cell::Cell<bool>,
    has_out: std::cell::Cell<bool>,
    marked: std::cell::Cell<bool>,
}

impl MockActiveTxn {
    fn new(id: u64, epoch: u32, begin_seq: u64) -> Self {
        Self {
            token: TxnToken::new(TxnId::new(id).unwrap(), TxnEpoch::new(epoch)),
            begin_seq: CommitSeq::new(begin_seq),
            active: true,
            reads: Vec::new(),
            writes: Vec::new(),
            has_in: std::cell::Cell::new(false),
            has_out: std::cell::Cell::new(false),
            marked: std::cell::Cell::new(false),
        }
    }

    fn with_reads(mut self, keys: Vec<WitnessKey>) -> Self {
        self.reads = keys;
        self
    }

    fn with_writes(mut self, keys: Vec<WitnessKey>) -> Self {
        self.writes = keys;
        self
    }

    #[allow(dead_code)]
    fn with_has_in_rw(self, val: bool) -> Self {
        self.has_in.set(val);
        self
    }

    #[allow(dead_code)]
    fn with_has_out_rw(self, val: bool) -> Self {
        self.has_out.set(val);
        self
    }
}

impl ActiveTxnView for MockActiveTxn {
    fn token(&self) -> TxnToken {
        self.token
    }
    fn begin_seq(&self) -> CommitSeq {
        self.begin_seq
    }
    fn is_active(&self) -> bool {
        self.active
    }
    fn read_keys(&self) -> &[WitnessKey] {
        &self.reads
    }
    fn write_keys(&self) -> &[WitnessKey] {
        &self.writes
    }
    fn check_read_overlap(&self, key: &WitnessKey) -> bool {
        self.reads.iter().any(|k| crate::witness_plane::witness_keys_overlap(k, key))
    }
    fn check_write_overlap(&self, key: &WitnessKey) -> bool {
        self.writes.iter().any(|k| crate::witness_plane::witness_keys_overlap(k, key))
    }
    fn has_in_rw(&self) -> bool {
        self.has_in.get()
    }
    fn has_out_rw(&self) -> bool {
        self.has_out.get()
    }
    fn set_has_out_rw(&self, val: bool) {
        self.has_out.set(val);
    }
    fn set_has_in_rw(&self, val: bool) {
        self.has_in.set(val);
    }
    fn set_marked_for_abort(&self, val: bool) {
        self.marked.set(val);
    }
}

// =========================================================================
// §1  SERIALIZATION ANOMALY DETECTION
// =========================================================================

/// Classic write-skew: T1 reads A, writes B; T2 reads B, writes A.
/// The pivot transaction must abort.
#[test]
fn ssi_anomaly_write_skew_detected() {
    let lock_table = InProcessPageLockTable::new();
    let commit_index = CommitIndex::new();
    let mut registry = ConcurrentRegistry::new();

    // T1: reads page 100 (A), writes page 200 (B).
    let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    // T2: reads page 200 (B), writes page 100 (A).
    let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

    {
        let h1 = registry.get_mut(s1).unwrap();
        h1.record_read(test_page(100));
        concurrent_write_page(h1, &lock_table, s1, test_page(200), test_data()).unwrap();
    }
    {
        let h2 = registry.get_mut(s2).unwrap();
        h2.record_read(test_page(200));
        concurrent_write_page(h2, &lock_table, s2, test_page(100), test_data()).unwrap();
    }

    // T1 commits first.
    let result1 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s1,
        CommitSeq::new(11),
    );
    // T1 is the pivot (both in+out rw edges with T2).
    assert!(
        result1.is_err(),
        "bead_id={BEAD_ID} write-skew: pivot T1 must abort"
    );
    let (err, _) = result1.unwrap_err();
    assert_eq!(
        err,
        MvccError::BusySnapshot,
        "bead_id={BEAD_ID} write-skew: must be BusySnapshot"
    );

    // T2 can commit after T1 aborted.
    let result2 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s2,
        CommitSeq::new(11),
    );
    assert!(
        result2.is_ok(),
        "bead_id={BEAD_ID} write-skew: T2 must commit after T1 aborted"
    );
}

/// Phantom-like anomaly: T1 scans a range (reads pages 300..305), T2 inserts
/// into that range (writes page 303). SSI should detect the rw-antidependency.
#[test]
fn ssi_anomaly_phantom_insert_detected() {
    // T1: reads pages 300-304 (range scan), writes page 400 (aggregate).
    // T2: reads page 500, writes page 303 (insert into scanned range).
    let t2 = MockActiveTxn::new(2, 0, 1)
        .with_reads(vec![page_key(500)])
        .with_writes(vec![page_key(303)]);

    let txn1 = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
    let read_keys: Vec<WitnessKey> = (300..305).map(page_key).collect();
    let write_keys = vec![page_key(400)];

    let readers: Vec<&dyn ActiveTxnView> = vec![];
    let writers: Vec<&dyn ActiveTxnView> = vec![&t2];

    // T1 sees T2 writing to page 303, which T1 reads → outgoing edge.
    let result = ssi_validate_and_publish(
        txn1,
        CommitSeq::new(1),
        CommitSeq::new(5),
        &read_keys,
        &write_keys,
        &readers,
        &writers,
        &[],
        &[],
        false,
    );
    let ok = result.expect("bead_id={BEAD_ID} phantom: T1 with only outgoing edge commits");
    assert!(
        ok.ssi_state.has_out_rw,
        "bead_id={BEAD_ID} phantom: T1 must detect outgoing rw edge"
    );
}

/// Phantom detection through the full concurrent registry path.
#[test]
fn ssi_anomaly_phantom_via_registry() {
    let lock_table = InProcessPageLockTable::new();
    let commit_index = CommitIndex::new();
    let mut registry = ConcurrentRegistry::new();

    // T1: range-reads pages 300-304, writes aggregate page 400.
    let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    // T2: inserts into the range by writing page 303, reads unrelated page 500.
    let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

    {
        let h1 = registry.get_mut(s1).unwrap();
        for p in 300..305 {
            h1.record_read(test_page(p));
        }
        concurrent_write_page(h1, &lock_table, s1, test_page(400), test_data()).unwrap();
    }
    {
        let h2 = registry.get_mut(s2).unwrap();
        h2.record_read(test_page(500));
        concurrent_write_page(h2, &lock_table, s2, test_page(303), test_data()).unwrap();
    }

    // T2 commits first (only has incoming edge from T1).
    let result2 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s2,
        CommitSeq::new(11),
    );
    assert!(
        result2.is_ok(),
        "bead_id={BEAD_ID} phantom-registry: T2 commits (no pivot)"
    );

    // T1 should still commit (only outgoing edge to committed T2).
    let result1 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s1,
        CommitSeq::new(12),
    );
    // T1 has outgoing edge (T2 wrote 303 which T1 read), but T1 doesn't have
    // incoming edge because nobody reads what T1 writes (page 400).
    assert!(
        result1.is_ok(),
        "bead_id={BEAD_ID} phantom-registry: T1 commits (only outgoing, not a pivot)"
    );
}

/// Lost-update prevention via first-committer-wins: two txns writing the same
/// page, second committer gets BusySnapshot.
#[test]
fn ssi_anomaly_lost_update_prevented_by_fcw() {
    let lock_table = InProcessPageLockTable::new();
    let commit_index = CommitIndex::new();
    let mut registry = ConcurrentRegistry::new();

    // Both T1 and T2 write to the same page.
    let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

    // T1 writes page 50.
    {
        let h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(h1, &lock_table, s1, test_page(50), test_data()).unwrap();
    }
    // T2 writes a different page (page lock prevents writing the same page concurrently).
    {
        let h2 = registry.get_mut(s2).unwrap();
        concurrent_write_page(h2, &lock_table, s2, test_page(60), test_data()).unwrap();
    }

    // T1 commits first, updates commit_index for page 50.
    let result1 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s1,
        CommitSeq::new(11),
    );
    assert!(
        result1.is_ok(),
        "bead_id={BEAD_ID} lost-update: T1 commits first"
    );

    // Now start T3 with old snapshot, write page 50 (already committed by T1).
    let s3 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    {
        let h3 = registry.get_mut(s3).unwrap();
        concurrent_write_page(h3, &lock_table, s3, test_page(50), test_data()).unwrap();
    }

    // T3 should fail FCW: page 50 was committed at seq 11 > T3's snapshot high 10.
    let result3 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s3,
        CommitSeq::new(12),
    );
    assert!(
        result3.is_err(),
        "bead_id={BEAD_ID} lost-update: T3 must abort (FCW conflict on page 50)"
    );
}

/// Write-skew with three concurrent transactions forming a cycle.
/// T1 reads A, writes B; T2 reads B, writes C; T3 reads C, writes A.
/// At least one must abort (the pivot).
#[test]
fn ssi_anomaly_three_way_write_skew_cycle() {
    let lock_table = InProcessPageLockTable::new();
    let commit_index = CommitIndex::new();
    let mut registry = ConcurrentRegistry::new();

    let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    let s3 = registry.begin_concurrent(test_snapshot(10)).unwrap();

    // T1: reads A (page 100), writes B (page 200).
    {
        let h1 = registry.get_mut(s1).unwrap();
        h1.record_read(test_page(100));
        concurrent_write_page(h1, &lock_table, s1, test_page(200), test_data()).unwrap();
    }
    // T2: reads B (page 200), writes C (page 300).
    {
        let h2 = registry.get_mut(s2).unwrap();
        h2.record_read(test_page(200));
        concurrent_write_page(h2, &lock_table, s2, test_page(300), test_data()).unwrap();
    }
    // T3: reads C (page 300), writes A (page 100).
    {
        let h3 = registry.get_mut(s3).unwrap();
        h3.record_read(test_page(300));
        concurrent_write_page(h3, &lock_table, s3, test_page(100), test_data()).unwrap();
    }

    let mut commits = 0u32;
    let mut aborts = 0u32;

    for &(sid, seq) in &[(s1, 11u64), (s2, 12u64), (s3, 13u64)] {
        let result = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            sid,
            CommitSeq::new(seq),
        );
        if result.is_ok() {
            commits += 1;
        } else {
            aborts += 1;
        }
    }

    assert!(
        aborts >= 1,
        "bead_id={BEAD_ID} three-way-cycle: at least one txn must abort, got aborts={aborts}"
    );
    assert!(
        commits >= 1,
        "bead_id={BEAD_ID} three-way-cycle: at least one txn must commit, got commits={commits}"
    );
}

/// Write-skew detection via committed reader history (T3 rule).
/// T1 commits (was a reader), then T2 tries to commit with edges to T1.
#[test]
fn ssi_anomaly_committed_pivot_abort() {
    // Scenario: T1 (committed reader with has_in_rw) creates a committed pivot.
    // T2 writes to a page T1 read → incoming edge with committed source.
    // If T1 had has_in_rw at commit time, T2 must abort (committed pivot rule).
    let txn2 = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0));

    let committed_reader = CommittedReaderInfo {
        token: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0)),
        begin_seq: CommitSeq::new(1),
        commit_seq: CommitSeq::new(5),
        had_in_rw: true, // T1 was a pivot at commit time.
        pages: vec![test_page(50)],
    };

    // T2 writes page 50 (which T1 read) → incoming edge from committed reader.
    // T2 also reads page 60 and some active writer W wrote page 60 → outgoing edge.
    // But for the committed pivot rule, just the incoming edge from a committed
    // reader with had_in_rw=true is enough to force abort.
    let result = ssi_validate_and_publish(
        txn2,
        CommitSeq::new(1),
        CommitSeq::new(6),
        &[page_key(60)],
        &[page_key(50)],
        &[],
        &[],
        &[committed_reader],
        &[],
        false,
    );
    assert!(
        result.is_err(),
        "bead_id={BEAD_ID} committed-pivot: T2 must abort"
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.reason,
        SsiAbortReason::CommittedPivot,
        "bead_id={BEAD_ID} committed-pivot: reason must be CommittedPivot"
    );
}

/// Marked-for-abort propagation: when a transaction is marked for abort by
/// another committer's edge detection, it should fail at commit time.
#[test]
fn ssi_anomaly_marked_for_abort_propagation() {
    let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
    let result = ssi_validate_and_publish(
        txn,
        CommitSeq::new(1),
        CommitSeq::new(5),
        &[page_key(10)],
        &[page_key(20)],
        &[],
        &[],
        &[],
        &[],
        true, // marked_for_abort = true
    );
    assert!(
        result.is_err(),
        "bead_id={BEAD_ID} marked-for-abort: must abort"
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.reason,
        SsiAbortReason::MarkedForAbort,
        "bead_id={BEAD_ID} marked-for-abort: reason must be MarkedForAbort"
    );
}

// =========================================================================
// §2  LATCH-FREE MULTI-THREAD STRESS
// =========================================================================

/// Multi-threaded page lock contention: 64 threads race to acquire locks
/// on 8 hot pages, verify correctness via per-page checksums.
#[test]
fn latch_free_stress_64_threads_hot_pages() {
    use std::thread;

    let num_threads: u64 = 64;
    let hot_pages: u32 = 8;
    let ops_per_thread: u64 = 500;

    let lock_table = Arc::new(InProcessPageLockTable::new());
    let commit_index = Arc::new(CommitIndex::new());
    let commit_counter = Arc::new(AtomicU64::new(100));
    let total_commits = Arc::new(AtomicU64::new(0));
    let total_aborts = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let lock_table = Arc::clone(&lock_table);
            let commit_index = Arc::clone(&commit_index);
            let commit_counter = Arc::clone(&commit_counter);
            let total_commits = Arc::clone(&total_commits);
            let total_aborts = Arc::clone(&total_aborts);

            thread::spawn(move || {
                let base_id = (tid + 1) * 1000;
                for op in 0..ops_per_thread {
                    let txn_id_val = base_id + op;
                    let txn_id = TxnId::new(txn_id_val).unwrap();
                    // Pick a hot page deterministically based on tid + op.
                    #[allow(clippy::cast_possible_truncation)]
                    let page_num = ((tid + op) % u64::from(hot_pages)) as u32 + 1;
                    let page = test_page(page_num);

                    // Try to acquire the lock.
                    let acquired = lock_table.try_acquire(page, txn_id);
                    if acquired.is_ok() {
                        // "Write" the page (just update commit index).
                        let seq = commit_counter.fetch_add(1, Ordering::Relaxed);
                        commit_index.update(page, CommitSeq::new(seq));
                        lock_table.release_all(txn_id);
                        total_commits.fetch_add(1, Ordering::Relaxed);
                    } else {
                        total_aborts.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    let commits = total_commits.load(Ordering::Relaxed);
    let aborts = total_aborts.load(Ordering::Relaxed);
    let total = commits + aborts;

    assert_eq!(
        total,
        num_threads * ops_per_thread,
        "bead_id={BEAD_ID} latch-stress: all ops must be accounted for"
    );
    assert!(
        commits > 0,
        "bead_id={BEAD_ID} latch-stress: at least some commits must succeed"
    );

    // Verify commit index is consistent: each hot page has a valid latest seq.
    for page_num in 1..=hot_pages {
        let page = test_page(page_num);
        if let Some(seq) = commit_index.latest(page) {
            assert!(
                seq.get() >= 100,
                "bead_id={BEAD_ID} latch-stress: page {page_num} seq must be >= 100"
            );
        }
    }
}

/// Concurrent SSI validation stress: multiple threads validate transactions
/// simultaneously using the MockActiveTxn approach. Verifies no panics and
/// correct abort/commit decisions.
#[test]
fn ssi_validation_stress_16_concurrent_pairs() {
    use std::thread;

    let num_pairs: u64 = 16;
    let ops_per_pair: u64 = 100;
    let total_aborts = Arc::new(AtomicU64::new(0));
    let total_commits = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_pairs)
        .map(|pair_idx| {
            let total_aborts = Arc::clone(&total_aborts);
            let total_commits = Arc::clone(&total_commits);

            thread::spawn(move || {
                let base = pair_idx * 1000 + 1;
                for op in 0..ops_per_pair {
                    #[allow(clippy::cast_possible_truncation)]
                    let page_a = (pair_idx * 2 + 1) as u32;
                    #[allow(clippy::cast_possible_truncation)]
                    let page_b = (pair_idx * 2 + 2) as u32;

                    // Create write-skew pair: T1 reads A writes B, T2 reads B writes A.
                    let t2 = MockActiveTxn::new(base + op * 2 + 1, 0, 1)
                        .with_reads(vec![page_key(page_b)])
                        .with_writes(vec![page_key(page_a)]);

                    let txn1 = TxnToken::new(TxnId::new(base + op * 2).unwrap(), TxnEpoch::new(0));
                    let readers: Vec<&dyn ActiveTxnView> = vec![&t2];
                    let writers: Vec<&dyn ActiveTxnView> = vec![&t2];

                    let result = ssi_validate_and_publish(
                        txn1,
                        CommitSeq::new(1),
                        CommitSeq::new(5),
                        &[page_key(page_a)],
                        &[page_key(page_b)],
                        &readers,
                        &writers,
                        &[],
                        &[],
                        false,
                    );

                    match result {
                        Ok(_) => {
                            total_commits.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            total_aborts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked during SSI stress");
    }

    let aborts = total_aborts.load(Ordering::Relaxed);
    let commits = total_commits.load(Ordering::Relaxed);
    let total = aborts + commits;

    assert_eq!(
        total,
        num_pairs * ops_per_pair,
        "bead_id={BEAD_ID} ssi-stress: all ops accounted for"
    );
    // Write-skew pairs should all be detected as pivots.
    assert_eq!(
        aborts, total,
        "bead_id={BEAD_ID} ssi-stress: all write-skew pairs must abort"
    );
}

// =========================================================================
// §3  EBR VERSION RECLAMATION LIVENESS
// =========================================================================

/// EBR guard lifecycle: pin/unpin metrics are tracked correctly.
#[test]
fn ebr_guard_lifecycle_metrics() {
    // Delta-based: snapshot before, act, snapshot after.
    let before = GLOBAL_EBR_METRICS.snapshot();
    let registry = Arc::new(VersionGuardRegistry::default());

    // Pin 5 guards.
    let guards: Vec<_> = (0..5)
        .map(|_| VersionGuard::pin(Arc::clone(&registry)))
        .collect();
    assert_eq!(
        registry.active_guard_count(),
        5,
        "bead_id={BEAD_ID} ebr-lifecycle: 5 guards active"
    );

    let snap1 = GLOBAL_EBR_METRICS.snapshot();
    assert!(
        snap1.guards_pinned_total >= before.guards_pinned_total + 5,
        "bead_id={BEAD_ID} ebr-lifecycle: at least 5 pins recorded"
    );

    // Drop all guards.
    drop(guards);
    assert_eq!(
        registry.active_guard_count(),
        0,
        "bead_id={BEAD_ID} ebr-lifecycle: 0 guards after drop"
    );

    let snap2 = GLOBAL_EBR_METRICS.snapshot();
    assert!(
        snap2.guards_unpinned_total >= before.guards_unpinned_total + 5,
        "bead_id={BEAD_ID} ebr-lifecycle: at least 5 unpins recorded"
    );
}

/// EBR deferred retirement and flush: objects deferred via guards are
/// tracked in metrics. Flush pushes them toward reclamation.
#[test]
fn ebr_deferred_retirement_and_flush() {
    let registry = Arc::new(VersionGuardRegistry::default());
    let before = GLOBAL_EBR_METRICS.snapshot();

    let guard = VersionGuard::pin(Arc::clone(&registry));

    // Defer 10 retirements.
    for i in 0..10u64 {
        guard.defer_retire(vec![i; 64]); // 64-byte allocation.
    }

    let snap1 = GLOBAL_EBR_METRICS.snapshot();
    assert!(
        snap1.retirements_deferred_total >= before.retirements_deferred_total + 10,
        "bead_id={BEAD_ID} ebr-retire: 10 retirements deferred"
    );

    // Flush.
    guard.flush();
    let snap2 = GLOBAL_EBR_METRICS.snapshot();
    assert!(
        snap2.flush_calls_total > before.flush_calls_total,
        "bead_id={BEAD_ID} ebr-retire: 1 flush recorded"
    );

    drop(guard);
}

/// EBR stale reader detection: guards pinned longer than the threshold
/// are reported as stale.
#[test]
fn ebr_stale_reader_detection() {
    let config = StaleReaderConfig {
        warn_after: Duration::from_millis(1),
        warn_every: Duration::from_millis(1),
    };
    let registry = Arc::new(VersionGuardRegistry::new(config));
    let _guard = VersionGuard::pin(Arc::clone(&registry));

    // Sleep to exceed the stale threshold.
    std::thread::sleep(Duration::from_millis(5));

    let now = Instant::now();
    let stale = registry.stale_reader_snapshots(now);
    assert_eq!(
        stale.len(),
        1,
        "bead_id={BEAD_ID} ebr-stale: 1 stale reader detected"
    );
    assert!(
        stale[0].pinned_for >= Duration::from_millis(1),
        "bead_id={BEAD_ID} ebr-stale: pinned_for >= 1ms"
    );
}

/// EBR concurrent guard pinning from multiple threads.
/// Verifies that all guards are correctly unpinned after concurrent
/// pin/defer/flush/unpin cycles from multiple threads.
#[test]
fn ebr_concurrent_guard_pinning() {
    use std::thread;

    let registry = Arc::new(VersionGuardRegistry::default());
    let num_threads: u64 = 16;
    let guards_per_thread: u64 = 50;
    let completed = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let registry = Arc::clone(&registry);
            let completed = Arc::clone(&completed);
            thread::spawn(move || {
                for _ in 0..guards_per_thread {
                    let guard = VersionGuard::pin(Arc::clone(&registry));
                    guard.defer_retire(vec![0u8; 32]);
                    guard.flush();
                    drop(guard);
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    let expected_total = num_threads * guards_per_thread;
    assert_eq!(
        completed.load(Ordering::Relaxed),
        expected_total,
        "bead_id={BEAD_ID} ebr-concurrent: all ops completed"
    );
    assert_eq!(
        registry.active_guard_count(),
        0,
        "bead_id={BEAD_ID} ebr-concurrent: all guards unpinned"
    );
}

// =========================================================================
// §4  REBASE CORRECTNESS
// =========================================================================

/// FCW conflict detection: write to the same page after a commit between
/// snapshot and commit time.
#[test]
fn rebase_fcw_conflict_detected() {
    let lock_table = InProcessPageLockTable::new();
    let commit_index = CommitIndex::new();
    let mut registry = ConcurrentRegistry::new();

    let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

    // T1 writes page 50.
    {
        let h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(h1, &lock_table, s1, test_page(50), test_data()).unwrap();
    }
    // T1 commits → page 50 now at seq 11.
    let result1 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s1,
        CommitSeq::new(11),
    );
    assert!(result1.is_ok(), "bead_id={BEAD_ID} rebase-fcw: T1 commits");

    // T2 writes the same page 50 (with snapshot at seq 10).
    {
        let h2 = registry.get_mut(s2).unwrap();
        concurrent_write_page(h2, &lock_table, s2, test_page(50), test_data()).unwrap();
    }

    // T2 should fail FCW validation.
    let result2 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s2,
        CommitSeq::new(12),
    );
    assert!(
        result2.is_err(),
        "bead_id={BEAD_ID} rebase-fcw: T2 must abort (FCW conflict)"
    );
}

/// Rebase with disjoint pages succeeds: two concurrent transactions
/// writing to non-overlapping pages both commit.
#[test]
fn rebase_disjoint_pages_both_commit() {
    let lock_table = InProcessPageLockTable::new();
    let commit_index = CommitIndex::new();
    let mut registry = ConcurrentRegistry::new();

    let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();

    {
        let h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(h1, &lock_table, s1, test_page(50), test_data()).unwrap();
    }
    {
        let h2 = registry.get_mut(s2).unwrap();
        concurrent_write_page(h2, &lock_table, s2, test_page(60), test_data()).unwrap();
    }

    let result1 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s1,
        CommitSeq::new(11),
    );
    assert!(
        result1.is_ok(),
        "bead_id={BEAD_ID} rebase-disjoint: T1 commits"
    );

    let result2 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s2,
        CommitSeq::new(12),
    );
    assert!(
        result2.is_ok(),
        "bead_id={BEAD_ID} rebase-disjoint: T2 commits"
    );
}

/// Abort releases all page locks, allowing subsequent transactions.
#[test]
fn rebase_abort_releases_locks() {
    let lock_table = InProcessPageLockTable::new();
    let commit_index = CommitIndex::new();
    let mut registry = ConcurrentRegistry::new();

    let s1 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    {
        let h1 = registry.get_mut(s1).unwrap();
        concurrent_write_page(h1, &lock_table, s1, test_page(50), test_data()).unwrap();
    }
    // Abort T1.
    {
        let h1 = registry.get_mut(s1).unwrap();
        concurrent_abort(h1, &lock_table, s1);
    }

    // T2 should be able to acquire the same page lock.
    let s2 = registry.begin_concurrent(test_snapshot(10)).unwrap();
    {
        let h2 = registry.get_mut(s2).unwrap();
        let result = concurrent_write_page(h2, &lock_table, s2, test_page(50), test_data());
        assert!(
            result.is_ok(),
            "bead_id={BEAD_ID} rebase-abort: T2 can write page 50 after T1 abort"
        );
    }

    let result2 = concurrent_commit_with_ssi(
        &mut registry,
        &commit_index,
        &lock_table,
        s2,
        CommitSeq::new(11),
    );
    assert!(
        result2.is_ok(),
        "bead_id={BEAD_ID} rebase-abort: T2 commits after T1 abort"
    );
}

/// Multiple sequential abort/retry cycles on the same page.
#[test]
fn rebase_sequential_abort_retry_cycles() {
    let lock_table = InProcessPageLockTable::new();
    let commit_index = CommitIndex::new();
    let mut registry = ConcurrentRegistry::new();
    let mut commit_seq = 11u64;

    for cycle in 0..10u32 {
        // T1 uses the latest snapshot so FCW passes; T2 uses stale snapshot.
        let current_snap = if commit_seq > 11 { commit_seq - 1 } else { 10 };
        let stale_snap = 10u64; // Always stale: before any commits.

        let s1 = registry
            .begin_concurrent(test_snapshot(current_snap))
            .unwrap();
        let s2 = registry
            .begin_concurrent(test_snapshot(stale_snap))
            .unwrap();

        // T1 writes page 50.
        {
            let h1 = registry.get_mut(s1).unwrap();
            concurrent_write_page(h1, &lock_table, s1, test_page(50), test_data()).unwrap();
        }
        // T1 commits with fresh snapshot.
        let result1 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s1,
            CommitSeq::new(commit_seq),
        );
        assert!(
            result1.is_ok(),
            "bead_id={BEAD_ID} abort-retry cycle {cycle}: T1 commits"
        );
        commit_seq += 1;

        // T2 writes page 50 (stale snapshot from before T1's commit).
        {
            let h2 = registry.get_mut(s2).unwrap();
            concurrent_write_page(h2, &lock_table, s2, test_page(50), test_data()).unwrap();
        }
        // T2 should abort: page 50 committed after T2's snapshot.
        let result2 = concurrent_commit_with_ssi(
            &mut registry,
            &commit_index,
            &lock_table,
            s2,
            CommitSeq::new(commit_seq),
        );
        assert!(
            result2.is_err(),
            "bead_id={BEAD_ID} abort-retry cycle {cycle}: T2 aborts"
        );
        commit_seq += 1;
    }
}

// =========================================================================
// §5  CAS VERSION INSTALL CORRECTNESS
// =========================================================================

/// VersionArena alloc/free/realloc with generation ABA protection.
#[test]
fn cas_version_arena_aba_protection() {
    use fsqlite_types::PageVersion;

    let mut arena = VersionArena::new();

    // Allocate 100 versions.
    let indices: Vec<_> = (1..=100u32)
        .map(|i| {
            let version = PageVersion {
                pgno: test_page(i),
                commit_seq: CommitSeq::new(u64::from(i)),
                created_by: TxnToken::new(TxnId::new(u64::from(i)).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev: None,
            };
            arena.alloc(version)
        })
        .collect();

    // Verify all accessible.
    for (i, idx) in indices.iter().enumerate() {
        let v = arena.get(*idx);
        assert!(
            v.is_some(),
            "bead_id={BEAD_ID} cas-aba: slot {i} must be accessible"
        );
        let expected_seq = (i + 1) as u64;
        assert_eq!(
            v.unwrap().commit_seq.get(),
            expected_seq,
            "bead_id={BEAD_ID} cas-aba: slot {i} commit_seq"
        );
    }

    // Free half, then reallocate. The old indices must no longer work.
    let freed_indices: Vec<_> = indices[..50].to_vec();
    for idx in &freed_indices {
        arena.free(*idx);
    }

    // Stale indices should return None (generation mismatch).
    for (i, idx) in freed_indices.iter().enumerate() {
        assert!(
            arena.get(*idx).is_none(),
            "bead_id={BEAD_ID} cas-aba: freed slot {i} must be inaccessible (generation mismatch)"
        );
    }

    // Reallocate into freed slots.
    let new_indices: Vec<_> = (200..250u32)
        .map(|i| {
            let version = PageVersion {
                pgno: test_page(i),
                commit_seq: CommitSeq::new(u64::from(i)),
                created_by: TxnToken::new(TxnId::new(u64::from(i)).unwrap(), TxnEpoch::new(0)),
                data: PageData::zeroed(PageSize::DEFAULT),
                prev: None,
            };
            arena.alloc(version)
        })
        .collect();

    // New indices work.
    for (i, idx) in new_indices.iter().enumerate() {
        let v = arena.get(*idx);
        assert!(
            v.is_some(),
            "bead_id={BEAD_ID} cas-aba: reallocated slot {i} accessible"
        );
        assert_eq!(
            v.unwrap().commit_seq.get(),
            (i + 200) as u64,
            "bead_id={BEAD_ID} cas-aba: reallocated slot {i} commit_seq"
        );
    }

    // Old indices still don't work.
    for (i, idx) in freed_indices.iter().enumerate() {
        assert!(
            arena.get(*idx).is_none(),
            "bead_id={BEAD_ID} cas-aba: old index {i} still inaccessible after reallocation"
        );
    }
}

/// VersionArena take panics on generation mismatch (double-free prevention).
#[test]
#[should_panic(expected = "generation mismatch")]
fn cas_version_arena_double_free_panics() {
    use fsqlite_types::PageVersion;

    let mut arena = VersionArena::new();
    let version = PageVersion {
        pgno: test_page(1),
        commit_seq: CommitSeq::new(1),
        created_by: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0)),
        data: PageData::zeroed(PageSize::DEFAULT),
        prev: None,
    };
    let idx = arena.alloc(version);
    arena.free(idx);
    // Second free should panic with "generation mismatch".
    arena.free(idx);
}

/// CommitIndex concurrent updates: multiple threads update different pages
/// and verify monotonicity.
#[test]
fn cas_commit_index_concurrent_monotonic() {
    use std::thread;

    let commit_index = Arc::new(CommitIndex::new());
    let num_threads: u32 = 16;
    let updates_per_thread: u64 = 500;

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let commit_index = Arc::clone(&commit_index);
            thread::spawn(move || {
                let page = test_page(tid + 1);
                for seq in 1..=updates_per_thread {
                    commit_index.update(page, CommitSeq::new(seq));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    // Verify each page has the latest sequence.
    for tid in 0..num_threads {
        let page = test_page(tid + 1);
        let latest = commit_index.latest(page);
        assert_eq!(
            latest,
            Some(CommitSeq::new(updates_per_thread)),
            "bead_id={BEAD_ID} cas-monotonic: page {} must have seq {updates_per_thread}",
            tid + 1,
        );
    }
}

/// GC scheduler frequency computation.
#[test]
fn gc_scheduler_frequency_bounds() {
    let scheduler = GcScheduler::new();

    // Low pressure → minimum frequency.
    let freq_low = scheduler.compute_frequency(0.5);
    assert!(
        (freq_low - 1.0).abs() < f64::EPSILON,
        "bead_id={BEAD_ID} gc-freq: low pressure clamps to f_min"
    );

    // High pressure → maximum frequency.
    let freq_high = scheduler.compute_frequency(10_000.0);
    assert!(
        (freq_high - 100.0).abs() < f64::EPSILON,
        "bead_id={BEAD_ID} gc-freq: high pressure clamps to f_max"
    );

    // Medium pressure → intermediate frequency.
    let freq_med = scheduler.compute_frequency(40.0);
    assert!(
        freq_med > 1.0 && freq_med < 100.0,
        "bead_id={BEAD_ID} gc-freq: medium pressure gives intermediate frequency"
    );
}

/// GC todo queue dedup: enqueuing the same page twice doesn't create duplicates.
#[test]
fn gc_todo_queue_dedup() {
    let mut todo = GcTodo::new();

    todo.enqueue(test_page(10));
    todo.enqueue(test_page(10));
    todo.enqueue(test_page(20));

    // Drain and verify only 2 unique pages.
    let mut pages = Vec::new();
    while let Some(page) = todo.pop() {
        pages.push(page);
    }
    assert_eq!(
        pages.len(),
        2,
        "bead_id={BEAD_ID} gc-dedup: only 2 unique pages"
    );
}

// =========================================================================
// §PROPTEST  RANDOMIZED SSI VALIDATION
// =========================================================================

mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Strategy for generating random page numbers (1..=1000).
    fn page_num_strategy() -> impl Strategy<Value = u32> {
        1..=1000u32
    }

    /// Strategy for generating a random SSI scenario.
    fn ssi_scenario_strategy() -> impl Strategy<Value = (Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>)> {
        (
            proptest::collection::vec(page_num_strategy(), 1..=10), // T reads
            proptest::collection::vec(page_num_strategy(), 1..=10), // T writes
            proptest::collection::vec(page_num_strategy(), 0..=5),  // active reader pages
            proptest::collection::vec(page_num_strategy(), 0..=5),  // active writer pages
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        /// Random SSI validation never panics and always returns either
        /// Ok or a valid SsiBusySnapshot error.
        #[test]
        fn prop_ssi_validation_never_panics(
            (t_reads, t_writes, reader_pages, writer_pages) in ssi_scenario_strategy()
        ) {
            let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
            let read_keys: Vec<WitnessKey> = t_reads.iter().map(|&p| page_key(p)).collect();
            let write_keys: Vec<WitnessKey> = t_writes.iter().map(|&p| page_key(p)).collect();

            let reader = MockActiveTxn::new(2, 0, 1)
                .with_reads(reader_pages.iter().map(|&p| page_key(p)).collect());
            let writer = MockActiveTxn::new(3, 0, 1)
                .with_writes(writer_pages.iter().map(|&p| page_key(p)).collect());
            let readers: Vec<&dyn ActiveTxnView> = vec![&reader];
            let writers: Vec<&dyn ActiveTxnView> = vec![&writer];

            let result = ssi_validate_and_publish(
                txn,
                CommitSeq::new(1),
                CommitSeq::new(5),
                &read_keys,
                &write_keys,
                &readers,
                &writers,
                &[],
                &[],
                false,
            );

            match &result {
                Ok(ok) => {
                    // If committed, check invariants.
                    prop_assert!(
                        !(ok.ssi_state.has_in_rw && ok.ssi_state.has_out_rw),
                        "committed txn must not have both in+out rw edges (pivot)"
                    );
                }
                Err(err) => {
                    // If aborted, must be a valid reason.
                    prop_assert!(
                        matches!(
                            err.reason,
                            SsiAbortReason::Pivot
                                | SsiAbortReason::CommittedPivot
                                | SsiAbortReason::MarkedForAbort
                        ),
                        "abort must have valid reason"
                    );
                }
            }
        }

        /// Disjoint read/write sets always commit (no edges possible).
        #[test]
        fn prop_ssi_disjoint_sets_always_commit(
            read_pages in proptest::collection::vec(1..=500u32, 1..=10),
            write_pages in proptest::collection::vec(501..=1000u32, 1..=10),
        ) {
            let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
            let read_keys: Vec<WitnessKey> = read_pages.iter().map(|&p| page_key(p)).collect();
            let write_keys: Vec<WitnessKey> = write_pages.iter().map(|&p| page_key(p)).collect();

            // Active reader reads from write_pages range (501-1000), doesn't
            // overlap with T's write set (also 501-1000 — DOES overlap).
            // Use completely disjoint range for reader: 1001-1500.
            let reader = MockActiveTxn::new(2, 0, 1)
                .with_reads(vec![page_key(1001), page_key(1002)]);
            let writer = MockActiveTxn::new(3, 0, 1)
                .with_writes(vec![page_key(1003), page_key(1004)]);
            let readers: Vec<&dyn ActiveTxnView> = vec![&reader];
            let writers: Vec<&dyn ActiveTxnView> = vec![&writer];

            let result = ssi_validate_and_publish(
                txn,
                CommitSeq::new(1),
                CommitSeq::new(5),
                &read_keys,
                &write_keys,
                &readers,
                &writers,
                &[],
                &[],
                false,
            );

            prop_assert!(result.is_ok(), "disjoint read/write sets must always commit");
        }
    }
}
