//! §12.17 Time Travel Queries — `FOR SYSTEM_TIME AS OF` support.
//!
//! Enables reading historical database state via the immutable commit stream.
//! Two syntaxes are supported:
//!
//! - `FOR SYSTEM_TIME AS OF COMMITSEQ N` — exact commit sequence reference.
//! - `FOR SYSTEM_TIME AS OF '<timestamp>'` — resolved to commit sequence via
//!   binary search of the marker/commit-log space.
//!
//! A synthetic **read-only** snapshot `S` is created with `S.high = target_commit_seq`
//! and queries execute using normal MVCC resolution: `resolve(P, S)` returns the
//! newest committed version with `version.commit_seq <= S.high`.
//!
//! Time travel is strictly read-only; INSERT/UPDATE/DELETE/DDL in a time-travel
//! context MUST fail with an appropriate error.

use std::fmt;

use tracing::{debug, info, warn};

use fsqlite_types::{CommitSeq, PageNumber, SchemaEpoch, Snapshot};

use crate::VersionIdx;
use crate::core_types::CommitLog;
use crate::invariants::VersionStore;
use crate::witness_publication::CommitMarkerStore;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors specific to time-travel query resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeTravelError {
    /// The requested commit sequence has been pruned by GC.
    HistoryNotRetained {
        requested: CommitSeq,
        gc_horizon: CommitSeq,
    },
    /// The requested commit sequence does not exist (future or never committed).
    CommitSeqNotFound { requested: CommitSeq },
    /// No commit marker matches the requested timestamp.
    TimestampNotResolvable { target_unix_ns: u64 },
    /// DML (INSERT/UPDATE/DELETE) attempted in a time-travel context.
    ReadOnlyViolation { attempted_op: &'static str },
    /// DDL (CREATE/ALTER/DROP) attempted in a time-travel context.
    DdlBlocked { attempted_op: &'static str },
    /// The commit log is empty; no historical state exists.
    EmptyCommitLog,
}

impl fmt::Display for TimeTravelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HistoryNotRetained {
                requested,
                gc_horizon,
            } => write!(
                f,
                "history not retained: requested commit_seq={} but gc_horizon={}",
                requested.get(),
                gc_horizon.get()
            ),
            Self::CommitSeqNotFound { requested } => {
                write!(f, "commit_seq {} not found in commit log", requested.get())
            }
            Self::TimestampNotResolvable { target_unix_ns } => {
                write!(
                    f,
                    "no commit marker found for timestamp_unix_ns={target_unix_ns}"
                )
            }
            Self::ReadOnlyViolation { attempted_op } => {
                write!(
                    f,
                    "time-travel queries are read-only: {attempted_op} is not permitted"
                )
            }
            Self::DdlBlocked { attempted_op } => {
                write!(
                    f,
                    "DDL not permitted in time-travel context: {attempted_op}"
                )
            }
            Self::EmptyCommitLog => write!(f, "commit log is empty; no historical state"),
        }
    }
}

// ---------------------------------------------------------------------------
// Time-travel target specification
// ---------------------------------------------------------------------------

/// How the user specifies the historical point to query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeTravelTarget {
    /// `FOR SYSTEM_TIME AS OF COMMITSEQ <n>`.
    CommitSequence(CommitSeq),
    /// `FOR SYSTEM_TIME AS OF '<timestamp>'` — unix nanoseconds.
    TimestampUnixNs(u64),
}

// ---------------------------------------------------------------------------
// Time-travel snapshot
// ---------------------------------------------------------------------------

/// A read-only synthetic snapshot pinned to a historical commit sequence.
///
/// Wraps a normal [`Snapshot`] but carries a `read_only` flag that **must**
/// be checked before any write operation is dispatched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeTravelSnapshot {
    /// The underlying MVCC snapshot (S.high = target_commit_seq).
    snapshot: Snapshot,
    /// The commit sequence this snapshot is pinned to.
    target_commit_seq: CommitSeq,
    /// Always `true` for time-travel snapshots.
    read_only: bool,
}

impl TimeTravelSnapshot {
    /// The underlying snapshot for MVCC resolution.
    #[must_use]
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// The target commit sequence this snapshot is pinned to.
    #[must_use]
    pub fn target_commit_seq(&self) -> CommitSeq {
        self.target_commit_seq
    }

    /// Whether this snapshot is read-only (always `true`).
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Validate that a DML operation is permitted (it never is).
    ///
    /// # Errors
    ///
    /// Always returns [`TimeTravelError::ReadOnlyViolation`].
    pub fn check_dml(&self, op: &'static str) -> Result<(), TimeTravelError> {
        Err(TimeTravelError::ReadOnlyViolation { attempted_op: op })
    }

    /// Validate that a DDL operation is permitted (it never is).
    ///
    /// # Errors
    ///
    /// Always returns [`TimeTravelError::DdlBlocked`].
    pub fn check_ddl(&self, op: &'static str) -> Result<(), TimeTravelError> {
        Err(TimeTravelError::DdlBlocked { attempted_op: op })
    }

    /// Resolve a page version visible at this historical snapshot using the
    /// given [`VersionStore`].
    #[must_use]
    pub fn resolve_page(
        &self,
        version_store: &VersionStore,
        page: PageNumber,
    ) -> Option<VersionIdx> {
        version_store.resolve(page, &self.snapshot)
    }
}

// ---------------------------------------------------------------------------
// Timestamp → CommitSeq resolution via CommitMarkerStore
// ---------------------------------------------------------------------------

/// Binary-search the marker store to find the greatest commit sequence
/// whose `commit_time_unix_ns <= target_unix_ns`.
///
/// CommitMarkers are stored in a `BTreeMap` keyed by `commit_seq`, and their
/// `commit_time_unix_ns` field is monotonically non-decreasing (per spec:
/// `max(now_unix_ns(), prev + 1)`). This monotonicity guarantee enables a
/// linear scan from the end of the ordered map — efficient for small stores
/// and correct because timestamps never go backward.
///
/// For larger stores the `BTreeMap` iterator provides O(n) worst case, but
/// in practice commit marker stores are bounded by retention policy and the
/// scan terminates early.
///
/// # Errors
///
/// Returns [`TimeTravelError::TimestampNotResolvable`] if no marker has a
/// timestamp at or before the target.
pub fn resolve_timestamp_via_markers(
    marker_store: &CommitMarkerStore,
    target_unix_ns: u64,
) -> Result<CommitSeq, TimeTravelError> {
    debug!(
        target_unix_ns,
        "resolving timestamp to commit_seq via marker store"
    );

    if let Some(seq) = marker_store.resolve_seq_at_or_before_timestamp(target_unix_ns) {
        info!(
            commit_seq = seq.get(),
            target_unix_ns, "timestamp resolved to commit_seq via marker store"
        );
        Ok(seq)
    } else {
        warn!(
            target_unix_ns,
            "no commit marker found at or before target timestamp"
        );
        Err(TimeTravelError::TimestampNotResolvable { target_unix_ns })
    }
}

/// Binary-search the commit log to find the greatest commit sequence whose
/// `timestamp_unix_ns <= target_unix_ns`.
///
/// The [`CommitLog`] stores [`CommitRecord`](crate::core_types::CommitRecord) entries with a `timestamp_unix_ns`
/// field that is monotonically non-decreasing (timestamps are assigned at
/// commit time). This allows a standard binary search over the contiguous
/// record array.
///
/// # Errors
///
/// - [`TimeTravelError::EmptyCommitLog`] if the log has no records.
/// - [`TimeTravelError::TimestampNotResolvable`] if all records have
///   timestamps strictly greater than `target_unix_ns`.
pub fn resolve_timestamp_via_commit_log(
    commit_log: &CommitLog,
    target_unix_ns: u64,
) -> Result<CommitSeq, TimeTravelError> {
    debug!(
        target_unix_ns,
        "resolving timestamp to commit_seq via commit log"
    );

    if commit_log.is_empty() {
        return Err(TimeTravelError::EmptyCommitLog);
    }

    // Binary search: find the largest commit_seq where timestamp_unix_ns <= target.
    //
    // CommitLog provides O(1) access by CommitSeq. We know the range is
    // [base_seq .. base_seq + len). Extract base and len to drive the search.
    let latest = commit_log
        .latest_seq()
        .ok_or(TimeTravelError::EmptyCommitLog)?;
    let first_seq = CommitSeq::new(latest.get() + 1 - commit_log.len() as u64);

    let mut lo = 0_u64;
    let mut hi = commit_log.len() as u64;
    let mut result_seq: Option<CommitSeq> = None;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let seq = CommitSeq::new(first_seq.get() + mid);

        if let Some(record) = commit_log.get(seq) {
            if record.timestamp_unix_ns <= target_unix_ns {
                result_seq = Some(seq);
                lo = mid + 1;
            } else {
                hi = mid;
            }
        } else {
            // Should not happen with contiguous log, but be defensive.
            hi = mid;
        }
    }

    if let Some(seq) = result_seq {
        info!(
            commit_seq = seq.get(),
            target_unix_ns, "timestamp resolved to commit_seq"
        );
        Ok(seq)
    } else {
        warn!(
            target_unix_ns,
            "no commit record found at or before target timestamp"
        );
        Err(TimeTravelError::TimestampNotResolvable { target_unix_ns })
    }
}

// ---------------------------------------------------------------------------
// Snapshot construction
// ---------------------------------------------------------------------------

/// Create a time-travel snapshot for the given target.
///
/// # Arguments
///
/// * `target` — The historical point to query (commit seq or timestamp).
/// * `commit_log` — The commit log for sequence validation and timestamp resolution.
/// * `gc_horizon` — The current GC horizon; requests below this fail.
/// * `schema_epoch` — The schema epoch to embed in the snapshot.
///
/// # Errors
///
/// - [`TimeTravelError::EmptyCommitLog`] if the log is empty.
/// - [`TimeTravelError::HistoryNotRetained`] if the target is below the GC horizon.
/// - [`TimeTravelError::CommitSeqNotFound`] if the exact sequence doesn't exist.
/// - [`TimeTravelError::TimestampNotResolvable`] if no record matches the timestamp.
pub fn create_time_travel_snapshot(
    target: TimeTravelTarget,
    commit_log: &CommitLog,
    gc_horizon: CommitSeq,
    schema_epoch: SchemaEpoch,
) -> Result<TimeTravelSnapshot, TimeTravelError> {
    let target_seq = match target {
        TimeTravelTarget::CommitSequence(seq) => seq,
        TimeTravelTarget::TimestampUnixNs(ts) => resolve_timestamp_via_commit_log(commit_log, ts)?,
    };

    // Validate: commit_seq must exist in the log.
    if commit_log.get(target_seq).is_none() {
        return Err(TimeTravelError::CommitSeqNotFound {
            requested: target_seq,
        });
    }

    // Validate: commit_seq must be at or above the GC horizon.
    if target_seq < gc_horizon {
        return Err(TimeTravelError::HistoryNotRetained {
            requested: target_seq,
            gc_horizon,
        });
    }

    info!(
        target_commit_seq = target_seq.get(),
        gc_horizon = gc_horizon.get(),
        "time-travel snapshot created"
    );

    Ok(TimeTravelSnapshot {
        snapshot: Snapshot::new(target_seq, schema_epoch),
        target_commit_seq: target_seq,
        read_only: true,
    })
}

// ---------------------------------------------------------------------------
// Convenience: resolve a page at a historical commit
// ---------------------------------------------------------------------------

/// Resolve a single page version at a historical commit sequence.
///
/// This is a convenience wrapper combining snapshot creation and resolution.
///
/// # Errors
///
/// Propagates any [`TimeTravelError`] from snapshot creation.
pub fn resolve_page_at_commit(
    version_store: &VersionStore,
    commit_log: &CommitLog,
    page: PageNumber,
    target: TimeTravelTarget,
    gc_horizon: CommitSeq,
    schema_epoch: SchemaEpoch,
) -> Result<Option<VersionIdx>, TimeTravelError> {
    let tt_snapshot = create_time_travel_snapshot(target, commit_log, gc_horizon, schema_epoch)?;
    Ok(tt_snapshot.resolve_page(version_store, page))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use fsqlite_types::{
        CommitMarker, CommitSeq, ObjectId, PageData, PageNumber, PageSize, PageVersion,
        SchemaEpoch, TxnEpoch, TxnId, TxnToken, VersionPointer,
    };

    use crate::core_types::CommitRecord;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn make_txn_token(id: u64) -> TxnToken {
        TxnToken::new(TxnId::new(id).unwrap(), TxnEpoch::new(1))
    }

    fn make_page_version(
        pgno: u32,
        commit_seq: u64,
        data_byte: u8,
        prev: Option<VersionPointer>,
    ) -> PageVersion {
        PageVersion {
            pgno: PageNumber::new(pgno).unwrap(),
            commit_seq: CommitSeq::new(commit_seq),
            created_by: make_txn_token(commit_seq),
            data: PageData::from_vec(vec![data_byte; 4096]),
            prev,
        }
    }

    fn make_commit_record(seq: u64, timestamp_ns: u64) -> CommitRecord {
        CommitRecord {
            txn_id: TxnId::new(seq).unwrap(),
            commit_seq: CommitSeq::new(seq),
            pages: smallvec::smallvec![PageNumber::new(1).unwrap()],
            timestamp_unix_ns: timestamp_ns,
        }
    }

    fn make_commit_marker(seq: u64, timestamp_ns: u64) -> CommitMarker {
        CommitMarker {
            commit_seq: CommitSeq::new(seq),
            commit_time_unix_ns: timestamp_ns,
            capsule_object_id: ObjectId::from_bytes([1_u8; 16]),
            proof_object_id: ObjectId::from_bytes([2_u8; 16]),
            prev_marker: None,
            integrity_hash: [0_u8; 16],
        }
    }

    /// Build a commit log with N sequential records at 1-second intervals.
    fn build_commit_log(count: u64) -> CommitLog {
        let mut log = CommitLog::new(CommitSeq::new(1));
        let base_ts = 1_700_000_000_000_000_000_u64; // ~2023-11-14
        for i in 0..count {
            let seq = i + 1;
            log.append(make_commit_record(seq, base_ts + i * 1_000_000_000));
        }
        log
    }

    /// Build a `VersionStore` with a version chain for a page.
    fn build_version_store_with_chain(pgno: u32, commit_seqs: &[u64]) -> VersionStore {
        let store = VersionStore::new(PageSize::DEFAULT);

        let mut prev_ptr: Option<VersionPointer> = None;
        for &seq in commit_seqs {
            #[allow(clippy::cast_possible_truncation)]
            let version = make_page_version(pgno, seq, (seq & 0xFF) as u8, prev_ptr);
            let idx = store.publish(version);
            prev_ptr = Some(crate::invariants::idx_to_version_pointer(idx));
        }

        store
    }

    // -----------------------------------------------------------------------
    // §12.17 Unit Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_time_travel_as_of_commitseq() {
        let commit_log = build_commit_log(10);
        let gc_horizon = CommitSeq::new(1);
        let schema_epoch = SchemaEpoch::new(1);

        let snapshot = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(5)),
            &commit_log,
            gc_horizon,
            schema_epoch,
        )
        .expect("snapshot creation should succeed");

        assert_eq!(snapshot.target_commit_seq(), CommitSeq::new(5));
        assert_eq!(snapshot.snapshot().high, CommitSeq::new(5));
        assert!(snapshot.is_read_only());
    }

    #[test]
    fn test_time_travel_as_of_timestamp() {
        let commit_log = build_commit_log(10);
        let base_ts = 1_700_000_000_000_000_000_u64;
        let gc_horizon = CommitSeq::new(1);
        let schema_epoch = SchemaEpoch::new(1);

        // Target: 4.5 seconds after base → should resolve to commit_seq=5
        // (seq=5 has timestamp base + 4*1e9, seq=6 has base + 5*1e9)
        let target_ts = base_ts + 4_500_000_000;
        let snapshot = create_time_travel_snapshot(
            TimeTravelTarget::TimestampUnixNs(target_ts),
            &commit_log,
            gc_horizon,
            schema_epoch,
        )
        .expect("timestamp resolution should succeed");

        assert_eq!(snapshot.target_commit_seq(), CommitSeq::new(5));
    }

    #[test]
    fn test_time_travel_timestamp_to_commitseq_resolution() {
        let commit_log = build_commit_log(20);
        let base_ts = 1_700_000_000_000_000_000_u64;

        // Exact match: timestamp of commit 10 (base + 9*1e9)
        let seq = resolve_timestamp_via_commit_log(&commit_log, base_ts + 9_000_000_000)
            .expect("exact timestamp should resolve");
        assert_eq!(seq, CommitSeq::new(10));

        // Between commits 15 and 16: should return 15
        let seq = resolve_timestamp_via_commit_log(&commit_log, base_ts + 14_500_000_000)
            .expect("between-commits timestamp should resolve");
        assert_eq!(seq, CommitSeq::new(15));

        // At the very first commit
        let seq = resolve_timestamp_via_commit_log(&commit_log, base_ts)
            .expect("first commit timestamp should resolve");
        assert_eq!(seq, CommitSeq::new(1));

        // At the very last commit (base + 19*1e9)
        let seq = resolve_timestamp_via_commit_log(&commit_log, base_ts + 19_000_000_000)
            .expect("last commit timestamp should resolve");
        assert_eq!(seq, CommitSeq::new(20));
    }

    #[test]
    fn test_time_travel_timestamp_to_commitseq_resolution_via_markers() {
        let base_ts = 1_700_000_000_000_000_000_u64;
        let mut marker_store = CommitMarkerStore::new();

        for seq in 1..=5 {
            marker_store.publish(make_commit_marker(seq, base_ts + (seq - 1) * 1_000_000_000));
        }

        let seq = resolve_timestamp_via_markers(&marker_store, base_ts + 2_500_000_000)
            .expect("between-commit marker timestamp should resolve");
        assert_eq!(seq, CommitSeq::new(3));

        let seq = resolve_timestamp_via_markers(&marker_store, base_ts + 4_000_000_000)
            .expect("exact marker timestamp should resolve");
        assert_eq!(seq, CommitSeq::new(5));
    }

    #[test]
    fn test_time_travel_timestamp_to_commitseq_resolution_via_markers_not_found() {
        let base_ts = 1_700_000_000_000_000_000_u64;
        let mut marker_store = CommitMarkerStore::new();
        marker_store.publish(make_commit_marker(10, base_ts + 1_000_000_000));

        let result = resolve_timestamp_via_markers(&marker_store, base_ts);
        assert!(matches!(
            result.unwrap_err(),
            TimeTravelError::TimestampNotResolvable { .. }
        ));
    }

    #[test]
    fn test_time_travel_read_only() {
        let commit_log = build_commit_log(5);
        let snapshot = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(3)),
            &commit_log,
            CommitSeq::new(1),
            SchemaEpoch::new(1),
        )
        .unwrap();

        let err = snapshot.check_dml("INSERT").unwrap_err();
        assert_eq!(
            err,
            TimeTravelError::ReadOnlyViolation {
                attempted_op: "INSERT"
            }
        );

        let err = snapshot.check_dml("UPDATE").unwrap_err();
        assert_eq!(
            err,
            TimeTravelError::ReadOnlyViolation {
                attempted_op: "UPDATE"
            }
        );

        let err = snapshot.check_dml("DELETE").unwrap_err();
        assert_eq!(
            err,
            TimeTravelError::ReadOnlyViolation {
                attempted_op: "DELETE"
            }
        );
    }

    #[test]
    fn test_time_travel_ddl_blocked() {
        let commit_log = build_commit_log(5);
        let snapshot = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(3)),
            &commit_log,
            CommitSeq::new(1),
            SchemaEpoch::new(1),
        )
        .unwrap();

        let err = snapshot.check_ddl("CREATE TABLE").unwrap_err();
        assert_eq!(
            err,
            TimeTravelError::DdlBlocked {
                attempted_op: "CREATE TABLE"
            }
        );

        let err = snapshot.check_ddl("ALTER TABLE").unwrap_err();
        assert_eq!(
            err,
            TimeTravelError::DdlBlocked {
                attempted_op: "ALTER TABLE"
            }
        );

        let err = snapshot.check_ddl("DROP TABLE").unwrap_err();
        assert_eq!(
            err,
            TimeTravelError::DdlBlocked {
                attempted_op: "DROP TABLE"
            }
        );
    }

    #[test]
    fn test_time_travel_snapshot_isolation() {
        let commit_log = build_commit_log(10);
        let schema_epoch = SchemaEpoch::new(1);

        // Two time-travel snapshots at the same commit should be equivalent
        let s1 = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(5)),
            &commit_log,
            CommitSeq::new(1),
            schema_epoch,
        )
        .unwrap();

        let s2 = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(5)),
            &commit_log,
            CommitSeq::new(1),
            schema_epoch,
        )
        .unwrap();

        assert_eq!(s1.snapshot(), s2.snapshot());
        assert_eq!(s1.target_commit_seq(), s2.target_commit_seq());
    }

    #[test]
    fn test_time_travel_mvcc_resolution() {
        let commit_log = build_commit_log(10);

        // Build a version store with versions at commits 2, 5, 8
        let store = build_version_store_with_chain(1, &[2, 5, 8]);

        // Snapshot at commit 6 should see version at commit 5
        let snapshot = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(6)),
            &commit_log,
            CommitSeq::new(1),
            SchemaEpoch::new(1),
        )
        .unwrap();

        let idx = snapshot
            .resolve_page(&store, PageNumber::new(1).unwrap())
            .expect("should resolve to commit 5 version");

        let version = store.get_version(idx).expect("version should exist");
        assert_eq!(version.commit_seq, CommitSeq::new(5));
    }

    #[test]
    fn test_time_travel_pruned_history() {
        let commit_log = build_commit_log(10);

        // GC horizon at 5 means commits 1-4 are pruned
        let result = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(3)),
            &commit_log,
            CommitSeq::new(5),
            SchemaEpoch::new(1),
        );

        assert_eq!(
            result.unwrap_err(),
            TimeTravelError::HistoryNotRetained {
                requested: CommitSeq::new(3),
                gc_horizon: CommitSeq::new(5),
            }
        );
    }

    #[test]
    fn test_time_travel_multiple_commits() {
        let commit_log = build_commit_log(10);

        // Build version store with page 1 updated at commits 1, 3, 5, 7, 9
        let store = build_version_store_with_chain(1, &[1, 3, 5, 7, 9]);

        // Query at commit 4 → should see version 3
        let s4 = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(4)),
            &commit_log,
            CommitSeq::new(1),
            SchemaEpoch::new(1),
        )
        .unwrap();

        let idx = s4
            .resolve_page(&store, PageNumber::new(1).unwrap())
            .unwrap();
        assert_eq!(
            store.get_version(idx).unwrap().commit_seq,
            CommitSeq::new(3)
        );

        // Query at commit 8 → should see version 7
        let s8 = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(8)),
            &commit_log,
            CommitSeq::new(1),
            SchemaEpoch::new(1),
        )
        .unwrap();

        let idx = s8
            .resolve_page(&store, PageNumber::new(1).unwrap())
            .unwrap();
        assert_eq!(
            store.get_version(idx).unwrap().commit_seq,
            CommitSeq::new(7)
        );
    }

    #[test]
    fn test_time_travel_table_before_creation() {
        let commit_log = build_commit_log(10);

        // Page 99 has versions only at commits 5 and 8
        let store = build_version_store_with_chain(99, &[5, 8]);

        // Query at commit 3 → page 99 didn't exist yet
        let s3 = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(3)),
            &commit_log,
            CommitSeq::new(1),
            SchemaEpoch::new(1),
        )
        .unwrap();

        let result = s3.resolve_page(&store, PageNumber::new(99).unwrap());
        assert!(
            result.is_none(),
            "page should not be visible before its creation commit"
        );
    }

    #[test]
    fn test_time_travel_with_joins() {
        let commit_log = build_commit_log(10);

        // Two "tables" (different page numbers) with different version histories
        let store_a = build_version_store_with_chain(1, &[2, 5, 8]);
        let store_b = build_version_store_with_chain(2, &[3, 6, 9]);

        // Snapshot at commit 7 should see:
        // - Page 1 at commit 5
        // - Page 2 at commit 6
        let snapshot = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(7)),
            &commit_log,
            CommitSeq::new(1),
            SchemaEpoch::new(1),
        )
        .unwrap();

        let idx_a = snapshot
            .resolve_page(&store_a, PageNumber::new(1).unwrap())
            .unwrap();
        let idx_b = snapshot
            .resolve_page(&store_b, PageNumber::new(2).unwrap())
            .unwrap();

        assert_eq!(
            store_a.get_version(idx_a).unwrap().commit_seq,
            CommitSeq::new(5)
        );
        assert_eq!(
            store_b.get_version(idx_b).unwrap().commit_seq,
            CommitSeq::new(6)
        );
    }

    #[test]
    fn test_time_travel_commitseq_not_found() {
        let commit_log = build_commit_log(5);

        // Request commit_seq=99 which doesn't exist
        let result = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(99)),
            &commit_log,
            CommitSeq::new(1),
            SchemaEpoch::new(1),
        );

        assert_eq!(
            result.unwrap_err(),
            TimeTravelError::CommitSeqNotFound {
                requested: CommitSeq::new(99),
            }
        );
    }

    #[test]
    fn test_time_travel_empty_commit_log() {
        let commit_log = CommitLog::new(CommitSeq::new(1));

        let result = resolve_timestamp_via_commit_log(&commit_log, 1_700_000_000_000_000_000);

        assert_eq!(result.unwrap_err(), TimeTravelError::EmptyCommitLog);
    }

    #[test]
    fn test_time_travel_timestamp_before_all_commits() {
        let commit_log = build_commit_log(5);

        // Timestamp before any commit
        let result = resolve_timestamp_via_commit_log(&commit_log, 1_000_000_000);

        assert!(matches!(
            result.unwrap_err(),
            TimeTravelError::TimestampNotResolvable { .. }
        ));
    }

    #[test]
    fn test_time_travel_at_gc_horizon_boundary() {
        let commit_log = build_commit_log(10);

        // Exactly at GC horizon should succeed
        let result = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(5)),
            &commit_log,
            CommitSeq::new(5),
            SchemaEpoch::new(1),
        );
        assert!(result.is_ok());

        // One below GC horizon should fail
        let result = create_time_travel_snapshot(
            TimeTravelTarget::CommitSequence(CommitSeq::new(4)),
            &commit_log,
            CommitSeq::new(5),
            SchemaEpoch::new(1),
        );
        assert!(matches!(
            result.unwrap_err(),
            TimeTravelError::HistoryNotRetained { .. }
        ));
    }
}
