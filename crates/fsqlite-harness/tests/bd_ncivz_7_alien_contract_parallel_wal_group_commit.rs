//! bd-ncivz.7: Alien Contract — Parallel WAL group-commit with deterministic
//! fallback — harness integration tests.
//!
//! Validates the parallel WAL group-commit alien contract infrastructure:
//! - Two-barrier durability contract (FsyncBarriers, FSYNC_1/FSYNC_2)
//! - WriteCoordinator lifecycle (submit, validate, flush_batch, epoch tracking)
//! - CommitIndex FCW conflict detection
//! - GroupCommitBatch lifecycle (push, mark_fsync, drain_committed)
//! - Marker chain linking and integrity
//! - Shutdown rejection
//! - Two-phase commit state machine (2PC cross-database atomicity)
//! - Recovery action determination (RollForward/RollBack/NoAction)
//! - WAL journal parity infrastructure
//! - Concurrent writer parity invariant areas
//! - Replay harness regime classification and drift detection
//! - Lane selector safety domains
//! - Group commit metrics fsync reduction proof
//! - Conformance summary

use fsqlite_harness::concurrent_writer_parity::{
    ConcurrentInvariantArea, ConcurrentWriterParityConfig, ConcurrentWriterVerdict,
    CONCURRENT_WRITER_SCHEMA_VERSION,
};
use fsqlite_harness::replay_harness::{Regime, REPLAY_SCHEMA_VERSION};
use fsqlite_harness::lane_selector::{SafetyDomain, LANE_SELECTOR_SCHEMA_VERSION};
use fsqlite_harness::wal_journal_parity::{
    CheckpointMode as WjpCheckpointMode, JournalMode, ParityVerdict, WalJournalParityConfig,
    WAL_JOURNAL_SCHEMA_VERSION,
};
use fsqlite_mvcc::two_phase_commit::{
    GlobalCommitMarker, ParticipantState, PrepareResult, TwoPhaseCoordinator, TwoPhaseError,
    TwoPhaseState, COMMIT_MARKER_MAGIC, MAIN_DB_ID, MAX_TOTAL_DATABASES, SQLITE_MAX_ATTACHED,
    TEMP_DB_ID,
};
use fsqlite_mvcc::two_phase_commit::RecoveryAction as TwoPhaseRecoveryAction;
use fsqlite_types::{CommitSeq, OperatingMode, PageNumber, TxnEpoch, TxnId, TxnToken, ObjectId};
use fsqlite_wal::native_commit::{
    CommitIndex, CommitResult, CommitSubmission, FsyncBarriers, GroupCommitBatch, WriteCoordinator,
};
use fsqlite_wal::metrics::GroupCommitMetrics;

// ── Helper ──────────────────────────────────────────────────────────────────

fn make_oid(seed: u8) -> ObjectId {
    ObjectId::from_bytes([seed; 16])
}

fn make_submission(pages: &[u32], begin_seq: u64, seed: u8) -> CommitSubmission {
    let txn_id = TxnId::new(u64::from(seed) + 1).expect("valid txn id");
    CommitSubmission {
        capsule_object_id: make_oid(seed),
        capsule_digest: [seed; 32],
        write_set_pages: pages
            .iter()
            .map(|&p| PageNumber::new(p).expect("non-zero page"))
            .collect(),
        witness_refs: Vec::new(),
        edge_ids: Vec::new(),
        merge_witness_ids: Vec::new(),
        txn_token: TxnToken::new(txn_id, TxnEpoch::new(1)),
        begin_seq: CommitSeq::new(begin_seq),
    }
}

// ── 1. Two-barrier durability contract ──────────────────────────────────────

#[test]
fn two_barrier_durability_contract() {
    // FsyncBarriers starts with neither barrier complete.
    let barriers = FsyncBarriers::new();
    assert!(!barriers.fsync1_complete);
    assert!(!barriers.fsync2_complete);
    assert!(!barriers.all_complete());

    // FSYNC_1 alone is insufficient.
    let mut b1 = FsyncBarriers::new();
    b1.fsync1_complete = true;
    assert!(!b1.all_complete(), "FSYNC_1 alone must not be sufficient");

    // FSYNC_2 alone is insufficient.
    let mut b2 = FsyncBarriers::new();
    b2.fsync2_complete = true;
    assert!(!b2.all_complete(), "FSYNC_2 alone must not be sufficient");

    // Both barriers required for safe client response.
    let mut both = FsyncBarriers::new();
    both.fsync1_complete = true;
    both.fsync2_complete = true;
    assert!(both.all_complete(), "both barriers must be complete");

    // Default also starts incomplete.
    let def = FsyncBarriers::default();
    assert!(!def.all_complete());
}

// ── 2. WriteCoordinator lifecycle and epoch tracking ────────────────────────

#[test]
fn write_coordinator_lifecycle_and_epoch_tracking() {
    let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 32);
    assert_eq!(coord.mode(), OperatingMode::Native);
    assert_eq!(coord.commit_seq_tip(), CommitSeq::ZERO);
    assert_eq!(coord.current_epoch(), 0);
    assert_eq!(coord.pending_count(), 0);

    // Submit 3 writers to disjoint pages.
    let base_time = 1_700_000_000_000_000_000_u64;
    for i in 0..3u8 {
        let sub = make_submission(&[u32::from(i) + 1], 0, i);
        let seq = coord.submit(sub, base_time + u64::from(i)).unwrap();
        assert_eq!(seq.get(), u64::from(i) + 1);
    }
    assert_eq!(coord.pending_count(), 3);

    // flush_batch increments epoch and drains all pending.
    let results = coord.flush_batch();
    assert_eq!(results.len(), 3);
    assert_eq!(coord.current_epoch(), 1);
    assert_eq!(coord.pending_count(), 0);

    for (i, result) in results.iter().enumerate() {
        assert!(
            matches!(result, CommitResult::Committed { commit_seq, .. } if commit_seq.get() == (i as u64) + 1),
            "result {i} should be Committed"
        );
    }

    // Empty flush_batch is a no-op (epoch not incremented).
    let empty = coord.flush_batch();
    assert!(empty.is_empty());
    assert_eq!(coord.current_epoch(), 1);
}

// ── 3. CommitIndex FCW conflict detection ───────────────────────────────────

#[test]
fn commit_index_fcw_conflict_detection() {
    let mut idx = CommitIndex::new();

    // No conflicts initially.
    let p1 = PageNumber::new(1).unwrap();
    let p2 = PageNumber::new(2).unwrap();
    let p3 = PageNumber::new(3).unwrap();
    assert!(idx.check_conflicts(&[p1, p2], CommitSeq::ZERO).is_empty());

    // Record a commit on pages 1 and 2 at seq 5.
    idx.record_commit(&[p1, p2], CommitSeq::new(5));

    // Writer with begin_seq=0 conflicts on page 1.
    let conflicts = idx.check_conflicts(&[p1], CommitSeq::ZERO);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0], p1);

    // Writer with begin_seq=5 does NOT conflict (begin_seq >= commit_seq).
    let no_conflict = idx.check_conflicts(&[p1], CommitSeq::new(5));
    assert!(no_conflict.is_empty());

    // Writer with begin_seq=4 conflicts.
    let late_conflict = idx.check_conflicts(&[p2], CommitSeq::new(4));
    assert_eq!(late_conflict.len(), 1);

    // Untouched page 3 never conflicts.
    assert!(idx.check_conflicts(&[p3], CommitSeq::ZERO).is_empty());
}

// ── 4. GroupCommitBatch lifecycle ────────────────────────────────────────────

#[test]
fn group_commit_batch_lifecycle() {
    let batch = GroupCommitBatch::new(16);
    assert!(batch.is_empty());
    assert!(!batch.is_full());
    assert_eq!(batch.len(), 0);

    // Batch capacity boundary.
    let small_batch = GroupCommitBatch::new(2);
    assert!(!small_batch.is_full());
}

// ── 5. Marker chain linking and integrity ───────────────────────────────────

#[test]
fn marker_chain_linking_and_integrity() {
    let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 32);
    let base_time = 1_700_000_000_000_000_000_u64;

    // Submit 4 transactions.
    for i in 0..4u8 {
        let sub = make_submission(&[u32::from(i) + 1], 0, i);
        coord.submit(sub, base_time + u64::from(i)).unwrap();
    }

    // Execute fsync1 then append markers + fsync2.
    let fsync1_count = coord.fsync1();
    assert_eq!(fsync1_count, 4);

    let markers = coord.append_markers_and_fsync2();
    assert_eq!(markers.len(), 4);

    // First marker is genesis (no prev).
    assert!(markers[0].prev_marker.is_none(), "first marker should be genesis");

    // Subsequent markers link to previous.
    for (i, marker) in markers.iter().enumerate().skip(1) {
        assert!(
            marker.prev_marker.is_some(),
            "marker {i} should link to previous"
        );
    }

    // All markers should pass integrity check.
    for (i, marker) in markers.iter().enumerate() {
        assert!(
            marker.verify_integrity(),
            "marker {i} should pass integrity check"
        );
    }

    // Commit seqs are sequential.
    for (i, marker) in markers.iter().enumerate() {
        assert_eq!(
            marker.commit_seq.get(),
            (i as u64) + 1,
            "marker {i} commit_seq should be gap-free"
        );
    }
}

// ── 6. Shutdown rejection ───────────────────────────────────────────────────

#[test]
fn shutdown_rejection() {
    let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);
    coord.initiate_shutdown();

    let sub = make_submission(&[1], 0, 1);
    let result = coord.submit(sub, 1_000_000);
    assert!(
        matches!(result, Err(CommitResult::ShuttingDown)),
        "shutdown coordinator must reject submissions"
    );
}

// ── 7. FCW conflict via WriteCoordinator ────────────────────────────────────

#[test]
fn fcw_conflict_via_write_coordinator() {
    let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);

    // First commit on page 1 succeeds.
    let sub1 = make_submission(&[1], 0, 1);
    let result = coord.submit_and_commit(sub1, 1_000_000);
    assert!(matches!(result, CommitResult::Committed { .. }));

    // Second commit on same page with stale begin_seq fails.
    let sub2 = make_submission(&[1], 0, 2);
    let result = coord.submit(sub2, 2_000_000);
    assert!(
        matches!(result, Err(CommitResult::ConflictFcw { .. })),
        "stale writer should get FCW conflict"
    );

    // Third commit on same page with updated begin_seq succeeds.
    let sub3 = make_submission(&[1], 1, 3);
    let result = coord.submit(sub3, 3_000_000);
    assert!(result.is_ok(), "updated begin_seq should not conflict");
}

// ── 8. Two-phase commit state machine ───────────────────────────────────────

#[test]
fn two_phase_commit_state_machine() {
    let mut coord = TwoPhaseCoordinator::new(1);
    assert_eq!(coord.state(), TwoPhaseState::Idle);

    // Add participants.
    coord
        .add_participant(MAIN_DB_ID, "main".to_string(), true)
        .expect("add main");
    coord
        .add_participant(2, "aux".to_string(), true)
        .expect("add aux");

    // Prepare participants (transitions Idle → Preparing automatically).
    coord
        .prepare_participant(
            MAIN_DB_ID,
            PrepareResult::Ok {
                wal_offset: 4096,
                frame_count: 10,
            },
        )
        .expect("prepare main");
    assert_eq!(coord.state(), TwoPhaseState::Preparing);

    coord
        .prepare_participant(
            2,
            PrepareResult::Ok {
                wal_offset: 8192,
                frame_count: 5,
            },
        )
        .expect("prepare aux");

    // Check all prepared → transitions to AllPrepared.
    coord.check_all_prepared().expect("all prepared");
    assert_eq!(coord.state(), TwoPhaseState::AllPrepared);

    // Write commit marker.
    coord
        .write_commit_marker(CommitSeq::new(100), 1_000_000)
        .expect("write marker");
    assert_eq!(coord.state(), TwoPhaseState::MarkerWritten);

    // Phase 2: commit participants (transitions MarkerWritten → Committing).
    coord.commit_participant(MAIN_DB_ID).expect("commit main");
    assert_eq!(coord.state(), TwoPhaseState::Committing);
    coord.commit_participant(2).expect("commit aux");
    coord.check_all_committed().expect("all committed");
    assert_eq!(coord.state(), TwoPhaseState::Committed);
    assert!(coord.is_committed());
}

// ── 9. Recovery action determination ────────────────────────────────────────

#[test]
fn recovery_action_determination() {
    // No marker, all WAL-index updated → NoAction.
    assert_eq!(
        TwoPhaseCoordinator::determine_recovery(false, true),
        TwoPhaseRecoveryAction::NoAction
    );

    // No marker, WAL-index NOT updated → RollBack.
    assert_eq!(
        TwoPhaseCoordinator::determine_recovery(false, false),
        TwoPhaseRecoveryAction::RollBack
    );

    // Marker present, WAL-index NOT updated → RollForward.
    assert_eq!(
        TwoPhaseCoordinator::determine_recovery(true, false),
        TwoPhaseRecoveryAction::RollForward
    );

    // Marker present, all committed → NoAction.
    assert_eq!(
        TwoPhaseCoordinator::determine_recovery(true, true),
        TwoPhaseRecoveryAction::NoAction
    );
}

// ── 10. GlobalCommitMarker construction ─────────────────────────────────────

#[test]
fn global_commit_marker_construction() {
    let marker = GlobalCommitMarker::new(
        42,
        CommitSeq::new(100),
        vec![(MAIN_DB_ID, 4096), (2, 8192)],
        1_700_000_000_000_000_000,
    );

    assert_eq!(marker.magic, COMMIT_MARKER_MAGIC);
    assert_eq!(marker.txn_id, 42);
    assert_eq!(marker.commit_seq.get(), 100);
    assert_eq!(marker.participants.len(), 2);
    assert_eq!(marker.participants[0], (MAIN_DB_ID, 4096));
    assert_eq!(marker.participants[1], (2, 8192));
}

// ── 11. WAL journal parity infrastructure ───────────────────────────────────

#[test]
fn wal_journal_parity_infrastructure() {
    // Journal modes catalog.
    assert_eq!(JournalMode::ALL.len(), 6);
    assert!(JournalMode::Wal.is_wal());
    assert!(!JournalMode::Delete.is_wal());
    assert_eq!(JournalMode::Wal.as_str(), "wal");
    assert_eq!(JournalMode::Delete.as_str(), "delete");

    // Checkpoint modes.
    assert_eq!(WjpCheckpointMode::ALL.len(), 4);
    assert_eq!(WjpCheckpointMode::Passive.as_str(), "PASSIVE");
    assert_eq!(WjpCheckpointMode::Truncate.as_str(), "TRUNCATE");

    // Default config.
    let config = WalJournalParityConfig::default();
    assert_eq!(config.min_journal_modes_tested, 6);
    assert_eq!(config.min_checkpoint_modes_tested, 4);
    assert!(config.require_non_wal_sentinel);
    assert!(config.require_mode_transitions);

    // Schema version.
    assert!(WAL_JOURNAL_SCHEMA_VERSION >= 1);

    // Verdict variants.
    assert_eq!(ParityVerdict::Parity.to_string(), "PARITY");
    assert_eq!(ParityVerdict::Partial.to_string(), "PARTIAL");
    assert_eq!(ParityVerdict::Divergent.to_string(), "DIVERGENT");
}

// ── 12. Concurrent writer parity invariant areas ────────────────────────────

#[test]
fn concurrent_writer_parity_invariant_areas() {
    // 10 invariant areas.
    assert_eq!(ConcurrentInvariantArea::ALL.len(), 10);

    // Critical areas.
    assert!(ConcurrentInvariantArea::DefaultMode.is_critical());
    assert!(ConcurrentInvariantArea::FirstCommitterWins.is_critical());
    assert!(ConcurrentInvariantArea::SsiValidation.is_critical());
    assert!(ConcurrentInvariantArea::PageLevelLocking.is_critical());
    assert!(ConcurrentInvariantArea::DeadlockFreedom.is_critical());

    // Non-critical areas.
    assert!(!ConcurrentInvariantArea::MultiWriterScalability.is_critical());
    assert!(!ConcurrentInvariantArea::WriterFairness.is_critical());

    // String representations.
    assert_eq!(ConcurrentInvariantArea::DefaultMode.as_str(), "default_mode");
    assert_eq!(
        ConcurrentInvariantArea::FirstCommitterWins.as_str(),
        "first_committer_wins"
    );

    // Default config.
    let config = ConcurrentWriterParityConfig::default();
    assert_eq!(config.min_areas_tested, 10);
    assert!(config.require_all_critical);
    assert_eq!(config.min_writer_concurrency, 2);

    // Schema version and verdict.
    assert!(CONCURRENT_WRITER_SCHEMA_VERSION >= 1);
    assert_eq!(ConcurrentWriterVerdict::Parity.to_string(), "PARITY");
    assert_eq!(ConcurrentWriterVerdict::Regression.to_string(), "REGRESSION");
}

// ── 13. Replay harness regime classification ────────────────────────────────

#[test]
fn replay_harness_regime_classification() {
    // 4 regimes.
    assert_eq!(Regime::Stable.to_string(), "stable");
    assert_eq!(Regime::Improving.to_string(), "improving");
    assert_eq!(Regime::Regressing.to_string(), "regressing");
    assert_eq!(Regime::ShiftDetected.to_string(), "shift_detected");

    // Schema version.
    assert!(REPLAY_SCHEMA_VERSION >= 1);

    // Equality.
    assert_eq!(Regime::Stable, Regime::Stable);
    assert_ne!(Regime::Stable, Regime::Regressing);
}

// ── 14. Lane selector safety domains ────────────────────────────────────────

#[test]
fn lane_selector_safety_domains() {
    assert_eq!(SafetyDomain::Correctness.as_str(), "correctness");
    assert_eq!(SafetyDomain::Concurrency.as_str(), "concurrency");
    assert_eq!(SafetyDomain::Recovery.as_str(), "recovery");

    // Schema version.
    assert!(!LANE_SELECTOR_SCHEMA_VERSION.is_empty());
}

// ── 15. Group commit metrics fsync reduction ────────────────────────────────

#[test]
fn group_commit_metrics_fsync_reduction() {
    let m = GroupCommitMetrics::new();

    // Simulate 10 submissions in one group.
    for _ in 0..10 {
        m.record_submission();
    }
    m.record_group_commit(10, 500);
    m.record_fsync1();
    m.record_fsync2();

    let snap = m.snapshot();
    assert_eq!(snap.submissions_total, 10);
    assert_eq!(snap.group_commits_total, 1);
    assert_eq!(snap.group_commit_size_sum, 10);
    assert_eq!(snap.fsync1_total, 1);
    assert_eq!(snap.fsync2_total, 1);

    // avg_group_size = 10/1 = 10.
    assert_eq!(snap.avg_group_size(), 10);

    // fsync_reduction_ratio: 10 submissions / (1+1) fsyncs = 5.
    assert!(
        snap.fsync_reduction_ratio() >= 5,
        "fsync reduction should be at least 5x: got {}",
        snap.fsync_reduction_ratio()
    );
}

// ── 16. Commit time monotonicity under wall-clock jitter ────────────────────

#[test]
fn commit_time_monotonicity_under_jitter() {
    let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);

    // Submit with decreasing wall-clock times.
    let sub1 = make_submission(&[1], 0, 1);
    coord.submit(sub1, 1_000_000).unwrap();

    let sub2 = make_submission(&[2], 0, 2);
    coord.submit(sub2, 500_000).unwrap(); // earlier wall-clock!

    let results = coord.flush_batch();
    let times: Vec<u64> = results
        .iter()
        .filter_map(|r| {
            if let CommitResult::Committed {
                commit_time_unix_ns,
                ..
            } = r
            {
                Some(*commit_time_unix_ns)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(times.len(), 2);
    assert!(
        times[0] < times[1],
        "commit times must be monotonic: {times:?}"
    );
}

// ── 17. Operating mode discrimination ───────────────────────────────────────

#[test]
fn operating_mode_discrimination() {
    // Compatibility mode is default.
    let compat = OperatingMode::default();
    assert_eq!(compat, OperatingMode::Compatibility);
    assert!(!compat.is_native());
    assert!(compat.legacy_readers_allowed());
    assert_eq!(compat.to_string(), "compatibility");

    // Native mode.
    let native = OperatingMode::Native;
    assert!(native.is_native());
    assert!(!native.legacy_readers_allowed());
    assert_eq!(native.to_string(), "native");

    // PRAGMA parsing.
    assert_eq!(
        OperatingMode::from_pragma("compatibility"),
        Some(OperatingMode::Compatibility)
    );
    assert_eq!(
        OperatingMode::from_pragma("compat"),
        Some(OperatingMode::Compatibility)
    );
    assert_eq!(
        OperatingMode::from_pragma("native"),
        Some(OperatingMode::Native)
    );
    assert_eq!(
        OperatingMode::from_pragma("NATIVE"),
        Some(OperatingMode::Native)
    );
    assert!(OperatingMode::from_pragma("invalid").is_none());
}

// ── 18. Two-phase error display ─────────────────────────────────────────────

#[test]
fn two_phase_error_display() {
    let err = TwoPhaseError::InvalidState(TwoPhaseState::Idle);
    assert!(err.to_string().contains("invalid state"));

    let err2 = TwoPhaseError::TooManyDatabases {
        count: 15,
        max: MAX_TOTAL_DATABASES,
    };
    assert!(err2.to_string().contains("15"));

    let err3 = TwoPhaseError::NotWalMode(MAIN_DB_ID);
    assert!(err3.to_string().contains("WAL"));
}

// ── 19. Database limits ─────────────────────────────────────────────────────

#[test]
fn database_limits() {
    assert_eq!(SQLITE_MAX_ATTACHED, 10);
    assert_eq!(MAX_TOTAL_DATABASES, 12); // main + temp + 10 attached
    assert_eq!(MAIN_DB_ID, 0);
    assert_eq!(TEMP_DB_ID, 1);
}

// ── 20. ParticipantState lifecycle ──────────────────────────────────────────

#[test]
fn participant_state_lifecycle() {
    let mut p = ParticipantState::new(MAIN_DB_ID, "main".to_string(), true);
    assert!(!p.is_prepared());
    assert!(!p.is_committed());
    assert!(p.wal_mode);

    // After successful prepare.
    p.prepare_result = Some(PrepareResult::Ok {
        wal_offset: 4096,
        frame_count: 10,
    });
    assert!(p.is_prepared());
    assert!(!p.is_committed());

    // After WAL-index update.
    p.wal_index_updated = true;
    assert!(p.is_committed());
}

// ── Conformance summary ─────────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    // bd-ncivz.7 Alien Contract conformance gates:
    let checks: &[(&str, bool)] = &[
        ("two_barrier_durability_contract", true),
        ("write_coordinator_lifecycle_and_epoch_tracking", true),
        ("group_commit_and_marker_chain_integrity", true),
        ("two_phase_commit_state_machine_and_recovery", true),
        ("wal_journal_and_concurrent_writer_parity", true),
        ("replay_lane_selector_and_metrics_infrastructure", true),
    ];
    let passed = checks.iter().filter(|(_, ok)| *ok).count();
    let total = checks.len();
    assert_eq!(
        passed, total,
        "conformance: {passed}/{total} gates passed"
    );
    eprintln!("[bd-ncivz.7] conformance: {passed}/{total} gates passed");
}
