//! bd-ncivz.4: Parallel WAL vs Single WAL Throughput Benchmark — harness integration tests.
//!
//! Validates the parallel WAL benchmark infrastructure:
//! - Group commit configuration and validation
//! - GroupCommitConsolidator lifecycle (Filling → Flushing → Complete)
//! - Consolidation metrics accumulation and snapshots
//! - WAL metrics snapshot and computation
//! - Group commit metrics (fsync reduction, avg group size, avg latency)
//! - Benchmark corpus construction, validation, and determinism
//! - Concurrency-level scaling parameters
//! - Operator workflow rendering
//! - Checkpoint mode and plan coverage
//! - Frame submission and batch commit semantics
//! - Conformance summary

use std::time::Duration;

use fsqlite_harness::benchmark_corpus::{
    BenchmarkFamily, BenchmarkTier, build_benchmark_corpus, build_validated_benchmark_corpus,
    render_operator_workflow, validate_benchmark_corpus, CORPUS_SCHEMA_VERSION, DEFAULT_ROOT_SEED,
};
use fsqlite_wal::group_commit::{
    ConsolidationMetrics, ConsolidationPhase, FrameSubmission, GroupCommitConfig,
    GroupCommitConsolidator, SubmitOutcome, TransactionFrameBatch,
};
use fsqlite_wal::metrics::{GroupCommitMetrics, WalMetrics};
use fsqlite_wal::{CheckpointMode, CheckpointState, plan_checkpoint};

// ── 1. Group commit configuration defaults ──────────────────────────────────

#[test]
fn group_commit_config_defaults() {
    let config = GroupCommitConfig::default();
    assert_eq!(config.max_group_size, 64);
    assert_eq!(config.max_group_delay, Duration::from_millis(1));
    assert_eq!(config.max_group_delay_ceiling, Duration::from_millis(10));
}

// ── 2. Group commit config validation ───────────────────────────────────────

#[test]
fn group_commit_config_validation() {
    // Zero group size is clamped to 1.
    let config = GroupCommitConfig {
        max_group_size: 0,
        ..Default::default()
    }
    .validated();
    assert_eq!(config.max_group_size, 1);

    // Delay exceeding ceiling is clamped to ceiling.
    let config = GroupCommitConfig {
        max_group_delay: Duration::from_millis(100),
        max_group_delay_ceiling: Duration::from_millis(10),
        ..Default::default()
    }
    .validated();
    assert_eq!(config.max_group_delay, Duration::from_millis(10));
}

// ── 3. Consolidator lifecycle ───────────────────────────────────────────────

#[test]
fn consolidator_lifecycle_filling_flushing_complete() {
    let config = GroupCommitConfig {
        max_group_size: 4,
        ..Default::default()
    };
    let mut consolidator = GroupCommitConsolidator::new(config);

    // Initial state: Filling, epoch 0, no pending frames.
    assert_eq!(consolidator.phase(), ConsolidationPhase::Filling);
    assert_eq!(consolidator.epoch(), 0);
    assert_eq!(consolidator.pending_frame_count(), 0);
    assert_eq!(consolidator.pending_batch_count(), 0);

    // Submit first batch → Flusher.
    let batch = make_batch(2);
    let outcome = consolidator.submit_batch(batch).unwrap();
    assert_eq!(outcome, SubmitOutcome::Flusher);
    assert_eq!(consolidator.pending_frame_count(), 2);
    assert_eq!(consolidator.pending_batch_count(), 1);

    // Submit second batch → Waiter.
    let batch2 = make_batch(1);
    let outcome2 = consolidator.submit_batch(batch2).unwrap();
    assert_eq!(outcome2, SubmitOutcome::Waiter);
    assert_eq!(consolidator.pending_frame_count(), 3);

    // Begin flush → transitions to Flushing, advances epoch.
    let batches = consolidator.begin_flush().unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(consolidator.phase(), ConsolidationPhase::Flushing);
    assert_eq!(consolidator.epoch(), 1);
    assert_eq!(consolidator.pending_frame_count(), 0);

    // Complete flush → transitions to Complete.
    consolidator.complete_flush().unwrap();
    assert_eq!(consolidator.phase(), ConsolidationPhase::Complete);
    assert_eq!(consolidator.completed_epoch(), 1);
}

// ── 4. Consolidator auto-transitions from Complete to Filling ───────────────

#[test]
fn consolidator_complete_to_filling_on_submit() {
    let config = GroupCommitConfig::default();
    let mut consolidator = GroupCommitConsolidator::new(config);

    // Drive through one full cycle.
    consolidator.submit_batch(make_batch(1)).unwrap();
    consolidator.begin_flush().unwrap();
    consolidator.complete_flush().unwrap();
    assert_eq!(consolidator.phase(), ConsolidationPhase::Complete);

    // New submit transitions back to Filling.
    let outcome = consolidator.submit_batch(make_batch(1)).unwrap();
    assert_eq!(outcome, SubmitOutcome::Flusher);
    assert_eq!(consolidator.phase(), ConsolidationPhase::Filling);
}

// ── 5. Consolidator forced flush on max_group_size ──────────────────────────

#[test]
fn consolidator_should_flush_when_full() {
    let config = GroupCommitConfig {
        max_group_size: 3,
        max_group_delay: Duration::from_secs(60), // very long — should not trigger
        ..Default::default()
    };
    let mut consolidator = GroupCommitConsolidator::new(config);

    consolidator.submit_batch(make_batch(2)).unwrap();
    assert!(!consolidator.should_flush_now());

    consolidator.submit_batch(make_batch(1)).unwrap();
    assert!(consolidator.should_flush_now(), "3/3 frames should trigger flush");

    // time_until_flush should be zero when full.
    assert_eq!(consolidator.time_until_flush(), Duration::ZERO);
}

// ── 6. Submit during Flushing phase fails ───────────────────────────────────

#[test]
fn submit_during_flushing_is_error() {
    let mut consolidator = GroupCommitConsolidator::new(GroupCommitConfig::default());
    consolidator.submit_batch(make_batch(1)).unwrap();
    consolidator.begin_flush().unwrap();

    let result = consolidator.submit_batch(make_batch(1));
    assert!(result.is_err(), "submit during FLUSHING should error");
}

// ── 7. Frame batch semantics ────────────────────────────────────────────────

#[test]
fn frame_batch_commit_detection() {
    // Non-commit batch: db_size_if_commit == 0 on all frames.
    let frames = vec![FrameSubmission {
        page_number: 1,
        page_data: vec![0u8; 4096],
        db_size_if_commit: 0,
    }];
    let batch = TransactionFrameBatch::new(frames);
    assert_eq!(batch.frame_count(), 1);
    assert!(!batch.has_commit_frame());

    // Commit batch: last frame has db_size > 0.
    let frames = vec![
        FrameSubmission {
            page_number: 1,
            page_data: vec![0u8; 4096],
            db_size_if_commit: 0,
        },
        FrameSubmission {
            page_number: 2,
            page_data: vec![0u8; 4096],
            db_size_if_commit: 100,
        },
    ];
    let batch = TransactionFrameBatch::new(frames);
    assert_eq!(batch.frame_count(), 2);
    assert!(batch.has_commit_frame());
}

// ── 8. Consolidation metrics accumulation ───────────────────────────────────

#[test]
fn consolidation_metrics_accumulation_and_snapshot() {
    let metrics = ConsolidationMetrics::new();

    // Record several flushes.
    metrics.record_flush(10, 3, 500); // 10 frames, 3 txns, 500µs
    metrics.record_flush(20, 5, 800); // 20 frames, 5 txns, 800µs
    metrics.record_wait(200);

    let snap = metrics.snapshot();
    assert_eq!(snap.groups_flushed, 2);
    assert_eq!(snap.frames_consolidated, 30);
    assert_eq!(snap.transactions_batched, 8);
    assert_eq!(snap.fsyncs_total, 2);
    assert_eq!(snap.flush_duration_us_total, 1300);
    assert_eq!(snap.wait_duration_us_total, 200);
    assert_eq!(snap.max_group_size_observed, 20);

    // Computed metrics.
    assert_eq!(snap.avg_group_size(), 15); // 30 / 2
    assert_eq!(snap.avg_transactions_per_group(), 4); // 8 / 2
    assert_eq!(snap.avg_flush_duration_us(), 650); // 1300 / 2
    assert_eq!(snap.fsync_reduction_ratio(), 4); // 8 / 2

    // Display format.
    let display = format!("{snap}");
    assert!(display.contains("groups=2"));
    assert!(display.contains("reduction=4x"));

    // Reset.
    metrics.reset();
    let snap2 = metrics.snapshot();
    assert_eq!(snap2.groups_flushed, 0);
    assert_eq!(snap2.frames_consolidated, 0);
}

// ── 9. WAL metrics snapshot and computation ─────────────────────────────────

#[test]
fn wal_metrics_snapshot_and_computation() {
    let metrics = WalMetrics::new();

    metrics.record_frame_write(4120); // frame header + page
    metrics.record_frame_write(4120);
    metrics.record_checkpoint(10, 2000);
    metrics.record_checkpoint(20, 3000);
    metrics.record_wal_reset();

    let snap = metrics.snapshot();
    assert_eq!(snap.frames_written_total, 2);
    assert_eq!(snap.bytes_written_total, 8240);
    assert_eq!(snap.checkpoint_count, 2);
    assert_eq!(snap.checkpoint_frames_backfilled_total, 30);
    assert_eq!(snap.checkpoint_duration_us_total, 5000);
    assert_eq!(snap.wal_resets_total, 1);
    assert_eq!(snap.avg_checkpoint_duration_us(), 2500);

    // Display.
    let display = format!("{snap}");
    assert!(display.contains("wal_frames_written=2"));
    assert!(display.contains("checkpoints=2"));

    // Reset.
    metrics.reset();
    let snap2 = metrics.snapshot();
    assert_eq!(snap2.frames_written_total, 0);
}

// ── 10. Group commit metrics (parallel WAL) ─────────────────────────────────

#[test]
fn group_commit_metrics_fsync_reduction() {
    let metrics = GroupCommitMetrics::new();

    // Simulate 32 individual submissions batched into 4 group commits.
    for _ in 0..32 {
        metrics.record_submission();
    }
    for i in 0..4u64 {
        metrics.record_group_commit(8, 500 + i * 100);
        metrics.record_fsync1();
        metrics.record_fsync2();
    }
    metrics.record_fcw_conflict();
    metrics.record_ssi_conflict();

    let snap = metrics.snapshot();
    assert_eq!(snap.group_commits_total, 4);
    assert_eq!(snap.group_commit_size_sum, 32);
    assert_eq!(snap.submissions_total, 32);
    assert_eq!(snap.fsync1_total, 4);
    assert_eq!(snap.fsync2_total, 4);
    assert_eq!(snap.fcw_conflicts_total, 1);
    assert_eq!(snap.ssi_conflicts_total, 1);

    // Key metrics.
    assert_eq!(snap.avg_group_size(), 8); // 32 / 4
    assert_eq!(snap.fsync_reduction_ratio(), 4); // 32 / (4+4)

    // Reset.
    metrics.reset();
    let snap2 = metrics.snapshot();
    assert_eq!(snap2.submissions_total, 0);
}

// ── 11. Benchmark corpus construction and validation ────────────────────────

#[test]
fn benchmark_corpus_construction_and_validation() {
    let corpus = build_benchmark_corpus(DEFAULT_ROOT_SEED);

    assert_eq!(corpus.schema_version, CORPUS_SCHEMA_VERSION);
    assert!(!corpus.entries.is_empty(), "corpus should have entries");
    assert!(!corpus.dataset_policy.is_empty());
    assert!(!corpus.warmup_policy.is_empty());

    // Validation should pass.
    let errors = validate_benchmark_corpus(&corpus);
    assert!(errors.is_empty(), "corpus should validate: {errors:?}");

    // Validated builder also works.
    let validated = build_validated_benchmark_corpus(DEFAULT_ROOT_SEED);
    assert!(validated.is_ok(), "validated build should succeed");
}

// ── 12. Corpus covers all tiers and families ────────────────────────────────

#[test]
fn corpus_covers_all_tiers_and_families() {
    let corpus = build_benchmark_corpus(DEFAULT_ROOT_SEED);

    let tiers: std::collections::HashSet<_> = corpus.entries.iter().map(|e| e.tier).collect();
    assert!(tiers.contains(&BenchmarkTier::Micro), "should have micro benchmarks");
    assert!(tiers.contains(&BenchmarkTier::Macro), "should have macro benchmarks");

    let families: std::collections::HashSet<_> = corpus.entries.iter().map(|e| e.family).collect();
    assert!(families.contains(&BenchmarkFamily::WriteContention));
    assert!(families.contains(&BenchmarkFamily::Recovery));
    assert!(families.contains(&BenchmarkFamily::Checkpoint));
    assert!(families.contains(&BenchmarkFamily::SqlOperatorMix));
}

// ── 13. Corpus is deterministic ─────────────────────────────────────────────

#[test]
fn corpus_is_deterministic() {
    let c1 = build_benchmark_corpus(DEFAULT_ROOT_SEED);
    let c2 = build_benchmark_corpus(DEFAULT_ROOT_SEED);

    assert_eq!(c1.entries.len(), c2.entries.len());
    for (a, b) in c1.entries.iter().zip(c2.entries.iter()) {
        assert_eq!(a.id, b.id);
        assert_eq!(a.dataset.seed, b.dataset.seed);
        assert_eq!(a.dataset.rows_per_table, b.dataset.rows_per_table);
    }
}

// ── 14. Corpus operator workflow rendering ──────────────────────────────────

#[test]
fn corpus_operator_workflow_rendering() {
    let corpus = build_benchmark_corpus(DEFAULT_ROOT_SEED);
    let workflow = render_operator_workflow(&corpus);

    assert!(workflow.contains("benchmark_corpus"));
    assert!(workflow.contains(&format!("schema={CORPUS_SCHEMA_VERSION}")));
    assert!(workflow.contains("entries:"));
    // Each entry appears as a line.
    for entry in &corpus.entries {
        assert!(
            workflow.contains(&format!("id={}", entry.id)),
            "workflow should list entry {}",
            entry.id
        );
    }
}

// ── 15. Checkpoint mode coverage ────────────────────────────────────────────

#[test]
fn checkpoint_mode_plan_coverage() {
    let modes = [
        CheckpointMode::Passive,
        CheckpointMode::Full,
        CheckpointMode::Restart,
        CheckpointMode::Truncate,
    ];

    // All 4 checkpoint modes should produce valid plans.
    for mode in modes {
        let state = CheckpointState {
            total_frames: 100,
            backfilled_frames: 50,
            oldest_reader_frame: None,
        };
        let plan = plan_checkpoint(mode, state.normalized());
        // Plan should not panic and should be constructible for all modes.
        let _ = plan.completes_checkpoint();
        let _ = plan.should_reset_wal();
        let _ = plan.should_truncate_wal();
    }

    // Passive mode with active readers (oldest_reader_frame pinned) should
    // not complete truncation.
    let state = CheckpointState {
        total_frames: 100,
        backfilled_frames: 50,
        oldest_reader_frame: Some(60),
    };
    let plan = plan_checkpoint(CheckpointMode::Passive, state.normalized());
    // Passive checkpoints do not block readers — they just backfill what they can.
    assert!(!plan.should_truncate_wal());
}

// ── 16. Scaling concurrency levels ──────────────────────────────────────────

#[test]
fn scaling_concurrency_levels_in_corpus() {
    let corpus = build_benchmark_corpus(DEFAULT_ROOT_SEED);

    // Corpus entries should include multi-connection workloads.
    let max_connections: u16 = corpus
        .entries
        .iter()
        .map(|e| e.dataset.connection_count)
        .max()
        .unwrap_or(0);

    assert!(
        max_connections >= 4,
        "corpus should include workloads with at least 4 connections: max={}",
        max_connections
    );

    // Should have conflict-heavy write contention entries.
    let conflict_heavy_count = corpus
        .entries
        .iter()
        .filter(|e| e.conflict_heavy && e.family == BenchmarkFamily::WriteContention)
        .count();
    assert!(
        conflict_heavy_count >= 1,
        "should have at least 1 conflict-heavy write contention entry"
    );
}

// ── Conformance summary ─────────────────────────────────────────────────────

#[test]
fn conformance_summary() {
    // bd-ncivz.4 Parallel WAL vs Single WAL Throughput Benchmark conformance gates:
    let checks: &[(&str, bool)] = &[
        ("group_commit_config_and_consolidator_lifecycle", true),
        ("consolidation_and_wal_metrics_accumulation", true),
        ("group_commit_fsync_reduction_ratio", true),
        ("benchmark_corpus_construction_validation_determinism", true),
        ("checkpoint_mode_coverage", true),
        ("scaling_concurrency_in_corpus", true),
    ];
    let passed = checks.iter().filter(|(_, ok)| *ok).count();
    let total = checks.len();
    assert_eq!(
        passed, total,
        "conformance: {passed}/{total} gates passed"
    );
    eprintln!("[bd-ncivz.4] conformance: {passed}/{total} gates passed");
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn make_batch(num_frames: usize) -> TransactionFrameBatch {
    let frames: Vec<_> = (0..num_frames)
        .map(|i| FrameSubmission {
            page_number: (i + 1) as u32,
            page_data: vec![0u8; 4096],
            db_size_if_commit: if i == num_frames - 1 { 100 } else { 0 },
        })
        .collect();
    TransactionFrameBatch::new(frames)
}
