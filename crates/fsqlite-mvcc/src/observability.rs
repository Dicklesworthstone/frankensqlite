//! MVCC observability integration.
//!
//! This module wires the `fsqlite-observability` event types into the MVCC
//! layer. It provides helper functions that emit conflict events through
//! both `tracing` (for structured logging) and an optional observer callback
//! (for programmatic access via PRAGMAs).
//!
//! **Invariant:** All functions in this module are non-blocking. They must
//! never acquire page locks or block writers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use fsqlite_observability::{ConflictEvent, ConflictObserver, SsiAbortCategory};
use fsqlite_types::{CommitSeq, PageNumber, TxnId, TxnToken};

/// Optional observer handle. When `None`, no callback overhead.
pub type SharedObserver = Option<Arc<dyn ConflictObserver>>;

/// Histogram buckets for `fsqlite_mvcc_versions_traversed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VersionsTraversedHistogram {
    pub le_1: u64,
    pub le_2: u64,
    pub le_4: u64,
    pub le_8: u64,
    pub le_16: u64,
    pub gt_16: u64,
}

/// Snapshot of MVCC snapshot-read metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotReadMetricsSnapshot {
    /// Histogram of versions traversed during snapshot reads.
    pub fsqlite_mvcc_versions_traversed: VersionsTraversedHistogram,
    /// Number of recorded snapshot-read samples.
    pub versions_traversed_samples: u64,
    /// Sum of traversed-version counts across samples.
    pub versions_traversed_sum: u64,
    /// Gauge of active snapshot-bearing transactions.
    pub fsqlite_mvcc_active_snapshots: u64,
}

static MVCC_VERSIONS_TRAVERSED_LE_1: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_LE_2: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_LE_4: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_LE_8: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_LE_16: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_GT_16: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_SAMPLES: AtomicU64 = AtomicU64::new(0);
static MVCC_VERSIONS_TRAVERSED_SUM: AtomicU64 = AtomicU64::new(0);
static MVCC_ACTIVE_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);

/// Monotonic nanosecond timestamp relative to process start.
fn now_ns() -> u64 {
    // Use a single, consistent epoch for all events in this process.
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    #[allow(clippy::cast_possible_truncation)] // clamped to u64::MAX
    {
        epoch.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }
}

/// Emit to observer if present.
#[inline]
fn emit(observer: &SharedObserver, event: &ConflictEvent) {
    if let Some(obs) = observer {
        obs.on_event(event);
    }
}

/// Record one snapshot-read traversal into the
/// `fsqlite_mvcc_versions_traversed` histogram.
pub fn record_snapshot_read_versions_traversed(versions_traversed: u64) {
    MVCC_VERSIONS_TRAVERSED_SAMPLES.fetch_add(1, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_SUM.fetch_add(versions_traversed, Ordering::Relaxed);

    let bucket = match versions_traversed {
        0 | 1 => &MVCC_VERSIONS_TRAVERSED_LE_1,
        2 => &MVCC_VERSIONS_TRAVERSED_LE_2,
        3 | 4 => &MVCC_VERSIONS_TRAVERSED_LE_4,
        5..=8 => &MVCC_VERSIONS_TRAVERSED_LE_8,
        9..=16 => &MVCC_VERSIONS_TRAVERSED_LE_16,
        _ => &MVCC_VERSIONS_TRAVERSED_GT_16,
    };
    bucket.fetch_add(1, Ordering::Relaxed);
}

/// Increment the `fsqlite_mvcc_active_snapshots` gauge.
pub fn mvcc_snapshot_established() {
    MVCC_ACTIVE_SNAPSHOTS.fetch_add(1, Ordering::Relaxed);
}

/// Decrement the `fsqlite_mvcc_active_snapshots` gauge (saturating at zero).
pub fn mvcc_snapshot_released() {
    loop {
        let current = MVCC_ACTIVE_SNAPSHOTS.load(Ordering::Relaxed);
        if current == 0 {
            return;
        }
        if MVCC_ACTIVE_SNAPSHOTS
            .compare_exchange_weak(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}

/// Snapshot MVCC snapshot-read metrics.
#[must_use]
pub fn mvcc_snapshot_metrics_snapshot() -> SnapshotReadMetricsSnapshot {
    SnapshotReadMetricsSnapshot {
        fsqlite_mvcc_versions_traversed: VersionsTraversedHistogram {
            le_1: MVCC_VERSIONS_TRAVERSED_LE_1.load(Ordering::Relaxed),
            le_2: MVCC_VERSIONS_TRAVERSED_LE_2.load(Ordering::Relaxed),
            le_4: MVCC_VERSIONS_TRAVERSED_LE_4.load(Ordering::Relaxed),
            le_8: MVCC_VERSIONS_TRAVERSED_LE_8.load(Ordering::Relaxed),
            le_16: MVCC_VERSIONS_TRAVERSED_LE_16.load(Ordering::Relaxed),
            gt_16: MVCC_VERSIONS_TRAVERSED_GT_16.load(Ordering::Relaxed),
        },
        versions_traversed_samples: MVCC_VERSIONS_TRAVERSED_SAMPLES.load(Ordering::Relaxed),
        versions_traversed_sum: MVCC_VERSIONS_TRAVERSED_SUM.load(Ordering::Relaxed),
        fsqlite_mvcc_active_snapshots: MVCC_ACTIVE_SNAPSHOTS.load(Ordering::Relaxed),
    }
}

/// Reset MVCC snapshot-read metrics.
pub fn reset_mvcc_snapshot_metrics() {
    MVCC_VERSIONS_TRAVERSED_LE_1.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_LE_2.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_LE_4.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_LE_8.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_LE_16.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_GT_16.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_SAMPLES.store(0, Ordering::Relaxed);
    MVCC_VERSIONS_TRAVERSED_SUM.store(0, Ordering::Relaxed);
    MVCC_ACTIVE_SNAPSHOTS.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Emit helpers for each event kind
// ---------------------------------------------------------------------------

/// Emit a page lock contention event.
///
/// Called when a page lock is held by another transaction and the requester
/// receives `Busy`.
pub fn emit_page_lock_contention(
    observer: &SharedObserver,
    page: PageNumber,
    requester: TxnId,
    holder: TxnId,
) {
    let event = ConflictEvent::PageLockContention {
        page,
        requester,
        holder,
        timestamp_ns: now_ns(),
    };
    tracing::info!(
        page = page.get(),
        requester = %requester,
        holder = %holder,
        "mvcc::page_lock_contention"
    );
    emit(observer, &event);
}

/// Emit a first-committer-wins base drift event.
///
/// Called from `concurrent_commit` when FCW validation detects that another
/// transaction committed to the same page after the snapshot.
pub fn emit_fcw_base_drift(
    observer: &SharedObserver,
    page: PageNumber,
    loser: TxnId,
    winner_commit_seq: CommitSeq,
    merge_attempted: bool,
    merge_succeeded: bool,
) {
    let event = ConflictEvent::FcwBaseDrift {
        page,
        loser,
        winner_commit_seq,
        merge_attempted,
        merge_succeeded,
        timestamp_ns: now_ns(),
    };
    tracing::warn!(
        page = page.get(),
        loser = %loser,
        winner_seq = winner_commit_seq.get(),
        merge_attempted,
        merge_succeeded,
        "mvcc::fcw_base_drift"
    );
    emit(observer, &event);
}

/// Emit an SSI abort event.
///
/// Called when SSI validation detects a dangerous structure (write skew)
/// and the transaction must abort.
pub fn emit_ssi_abort(
    observer: &SharedObserver,
    txn: TxnToken,
    reason: SsiAbortCategory,
    in_edge_count: usize,
    out_edge_count: usize,
) {
    let reason_str = match reason {
        SsiAbortCategory::Pivot => "pivot",
        SsiAbortCategory::CommittedPivot => "committed_pivot",
        SsiAbortCategory::MarkedForAbort => "marked_for_abort",
    };
    let event = ConflictEvent::SsiAbort {
        txn,
        reason,
        in_edge_count,
        out_edge_count,
        timestamp_ns: now_ns(),
    };
    tracing::warn!(
        txn_id = txn.id.get(),
        reason = reason_str,
        in_edges = in_edge_count,
        out_edges = out_edge_count,
        "mvcc::ssi_abort"
    );
    emit(observer, &event);
}

/// Emit a conflict-resolved event (merge succeeded).
pub fn emit_conflict_resolved(
    observer: &SharedObserver,
    txn: TxnId,
    pages_merged: usize,
    commit_seq: CommitSeq,
) {
    let event = ConflictEvent::ConflictResolved {
        txn,
        pages_merged,
        commit_seq,
        timestamp_ns: now_ns(),
    };
    tracing::info!(
        txn = %txn,
        pages_merged,
        commit_seq = commit_seq.get(),
        "mvcc::conflict_resolved"
    );
    emit(observer, &event);
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_observability::MetricsObserver;
    use fsqlite_types::TxnEpoch;

    fn make_page(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    fn make_txn(n: u64) -> TxnId {
        TxnId::new(n).unwrap()
    }

    fn make_token(n: u64) -> TxnToken {
        TxnToken::new(TxnId::new(n).unwrap(), TxnEpoch::new(1))
    }

    #[test]
    fn emit_fcw_records_to_observer() {
        let obs = Arc::new(MetricsObserver::new(100));
        let shared: SharedObserver = Some(obs.clone() as Arc<dyn ConflictObserver>);

        emit_fcw_base_drift(
            &shared,
            make_page(10),
            make_txn(2),
            CommitSeq::new(5),
            false,
            false,
        );

        let snap = obs.metrics().snapshot();
        assert_eq!(snap.fcw_drifts, 1);
        assert_eq!(snap.conflicts_total, 1);

        let events = obs.log().snapshot();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ConflictEvent::FcwBaseDrift { page, loser, .. }
                if page.get() == 10 && loser.get() == 2
        ));
    }

    #[test]
    fn emit_ssi_abort_records_to_observer() {
        let obs = Arc::new(MetricsObserver::new(100));
        let shared: SharedObserver = Some(obs.clone() as Arc<dyn ConflictObserver>);

        emit_ssi_abort(&shared, make_token(3), SsiAbortCategory::Pivot, 1, 1);

        let snap = obs.metrics().snapshot();
        assert_eq!(snap.ssi_aborts, 1);
    }

    #[test]
    fn emit_contention_records_to_observer() {
        let obs = Arc::new(MetricsObserver::new(100));
        let shared: SharedObserver = Some(obs.clone() as Arc<dyn ConflictObserver>);

        emit_page_lock_contention(&shared, make_page(42), make_txn(1), make_txn(2));

        let snap = obs.metrics().snapshot();
        assert_eq!(snap.page_contentions, 1);
    }

    #[test]
    fn emit_conflict_resolved_records_to_observer() {
        let obs = Arc::new(MetricsObserver::new(100));
        let shared: SharedObserver = Some(obs.clone() as Arc<dyn ConflictObserver>);

        emit_conflict_resolved(&shared, make_txn(1), 2, CommitSeq::new(10));

        let snap = obs.metrics().snapshot();
        assert_eq!(snap.conflicts_resolved, 1);
        // ConflictResolved is not a conflict, so total should stay 0.
        assert_eq!(snap.conflicts_total, 0);
    }

    #[test]
    fn no_observer_no_panic() {
        let shared: SharedObserver = None;
        emit_fcw_base_drift(
            &shared,
            make_page(1),
            make_txn(1),
            CommitSeq::new(1),
            false,
            false,
        );
        emit_ssi_abort(
            &shared,
            make_token(1),
            SsiAbortCategory::MarkedForAbort,
            0,
            0,
        );
        emit_page_lock_contention(&shared, make_page(1), make_txn(1), make_txn(2));
        emit_conflict_resolved(&shared, make_txn(1), 0, CommitSeq::new(1));
    }

    #[test]
    fn snapshot_metrics_record_histogram_and_gauge() {
        let before = mvcc_snapshot_metrics_snapshot();

        mvcc_snapshot_established();
        mvcc_snapshot_established();
        record_snapshot_read_versions_traversed(1);
        record_snapshot_read_versions_traversed(4);
        record_snapshot_read_versions_traversed(20);
        mvcc_snapshot_released();

        let after = mvcc_snapshot_metrics_snapshot();
        assert!(after.versions_traversed_samples >= before.versions_traversed_samples + 3);
        assert!(after.versions_traversed_sum >= before.versions_traversed_sum + 25);
        assert!(
            after.fsqlite_mvcc_versions_traversed.le_1
                >= before.fsqlite_mvcc_versions_traversed.le_1 + 1
        );
        assert!(
            after.fsqlite_mvcc_versions_traversed.le_4
                >= before.fsqlite_mvcc_versions_traversed.le_4 + 1
        );
        assert!(
            after.fsqlite_mvcc_versions_traversed.gt_16
                >= before.fsqlite_mvcc_versions_traversed.gt_16 + 1
        );
        assert!(after.fsqlite_mvcc_active_snapshots >= 1);
    }

    #[test]
    fn snapshot_gauge_release_saturates() {
        // Saturating release must never underflow/panic, even when gauge is zero.
        mvcc_snapshot_released();
    }
}
