#![cfg(unix)]

use std::sync::Arc;
use std::time::Duration;

use fsqlite_core::commit_repair::{
    CommitReceipt, CommitRepairConfig, CommitRepairCoordinator, CommitRepairEvent,
    CommitRepairEventKind, DeterministicRepairGenerator, InMemoryCommitRepairIo, RepairState,
};

struct CommitRepairHarness {
    coordinator: CommitRepairCoordinator<InMemoryCommitRepairIo, DeterministicRepairGenerator>,
    io: Arc<InMemoryCommitRepairIo>,
    generator: Arc<DeterministicRepairGenerator>,
}

impl CommitRepairHarness {
    fn new(repair_enabled: bool, repair_delay: Duration) -> Self {
        let io = Arc::new(InMemoryCommitRepairIo::default());
        let generator = Arc::new(DeterministicRepairGenerator::new(repair_delay, 512));
        let coordinator = CommitRepairCoordinator::with_shared(
            CommitRepairConfig { repair_enabled },
            Arc::clone(&io),
            Arc::clone(&generator),
        );
        Self {
            coordinator,
            io,
            generator,
        }
    }

    fn set_fail_repair(&self, fail: bool) {
        self.generator.set_fail_repair(fail);
    }

    fn commit(&self, systematic_symbols: &[u8]) -> CommitReceipt {
        self.coordinator
            .commit(systematic_symbols)
            .expect("commit should succeed")
    }

    fn wait_for_background_repair(&self) {
        self.coordinator
            .wait_for_background_repair()
            .expect("background repair should join");
    }

    fn repair_state_for(&self, commit_seq: u64) -> RepairState {
        self.coordinator.repair_state_for(commit_seq)
    }

    fn events_for_commit(&self, commit_seq: u64) -> Vec<CommitRepairEvent> {
        self.coordinator.events_for_commit(commit_seq)
    }

    fn total_repair_bytes(&self) -> u64 {
        self.io.total_repair_bytes()
    }

    fn repair_sync_count(&self) -> u64 {
        self.io.repair_sync_count()
    }

    fn durable_not_repairable_window(&self, commit_seq: u64) -> Option<Duration> {
        self.coordinator.durable_not_repairable_window(commit_seq)
    }
}

fn percentile(values: &[Duration], numerator: usize, denominator: usize) -> Duration {
    assert!(!values.is_empty(), "percentile requires non-empty input");
    assert!(denominator > 0, "denominator must be non-zero");
    assert!(numerator <= denominator, "numerator must be <= denominator");
    let mut sorted = values.to_vec();
    sorted.sort();
    let last = sorted.len() - 1;
    let rank = (last * numerator + (denominator / 2)) / denominator;
    sorted[rank]
}

#[test]
fn test_commit_durable_from_systematic_symbols_only() {
    let engine = CommitRepairHarness::new(true, Duration::from_millis(20));
    let receipt = engine.commit(&[0xAB; 4096]);

    assert!(
        receipt.durable,
        "commit must be durable after critical path"
    );
    assert!(
        receipt.repair_pending,
        "repair must be pending and off the commit critical path"
    );

    let events = engine.events_for_commit(receipt.commit_seq);
    let durable_idx = events
        .iter()
        .position(|event| event.kind == CommitRepairEventKind::CommitDurable)
        .expect("commit durable event must exist");
    let ack_idx = events
        .iter()
        .position(|event| event.kind == CommitRepairEventKind::CommitAcked)
        .expect("commit ack event must exist");
    assert!(durable_idx < ack_idx, "durability must happen before ack");
    assert!(
        !events
            .iter()
            .any(|event| event.kind == CommitRepairEventKind::RepairCompleted),
        "repair completion must not be on the commit path"
    );
}

#[test]
fn test_repair_symbols_generated_async() {
    let engine = CommitRepairHarness::new(true, Duration::from_millis(15));
    let receipt = engine.commit(&[0x11; 2048]);

    let early_events = engine.events_for_commit(receipt.commit_seq);
    assert!(
        early_events
            .iter()
            .any(|event| event.kind == CommitRepairEventKind::CommitAcked),
        "commit ack must be recorded immediately"
    );
    assert!(
        !early_events
            .iter()
            .any(|event| event.kind == CommitRepairEventKind::RepairCompleted),
        "repair completion must not be on the commit path"
    );

    engine.wait_for_background_repair();
    let events = engine.events_for_commit(receipt.commit_seq);
    assert!(
        events
            .iter()
            .any(|event| event.kind == CommitRepairEventKind::RepairStarted),
        "repair must start asynchronously in background"
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == CommitRepairEventKind::RepairCompleted),
        "repair must complete asynchronously"
    );
}

#[test]
fn test_commit_latency_unaffected_by_repair() {
    let no_repair = CommitRepairHarness::new(false, Duration::ZERO);
    let with_repair = CommitRepairHarness::new(true, Duration::from_millis(10));
    let payload = vec![0x5A; 4096];

    let mut no_repair_latencies = Vec::new();
    let mut with_repair_latencies = Vec::new();
    for _ in 0..64 {
        no_repair_latencies.push(no_repair.commit(&payload).latency);
        with_repair_latencies.push(with_repair.commit(&payload).latency);
    }
    with_repair.wait_for_background_repair();

    let p50_no_repair = percentile(&no_repair_latencies, 1, 2);
    let p50_with_repair = percentile(&with_repair_latencies, 1, 2);
    let p99_no_repair = percentile(&no_repair_latencies, 99, 100);
    let p99_with_repair = percentile(&with_repair_latencies, 99, 100);

    // Keep threshold conservative to avoid flaky timing in busy CI.
    let budget = Duration::from_millis(5);
    assert!(
        p50_with_repair.abs_diff(p50_no_repair) <= budget,
        "repair must stay off critical path (p50 drift too high): no_repair={p50_no_repair:?} with_repair={p50_with_repair:?}"
    );
    assert!(
        p99_with_repair.abs_diff(p99_no_repair) <= budget,
        "repair must stay off critical path (p99 drift too high): no_repair={p99_no_repair:?} with_repair={p99_with_repair:?}"
    );
}

#[test]
fn test_durable_but_not_repairable_state() {
    let engine = CommitRepairHarness::new(true, Duration::from_millis(25));
    let receipt = engine.commit(&[0x22; 1024]);

    assert!(receipt.durable, "commit must be durable immediately");
    assert_eq!(
        engine.repair_state_for(receipt.commit_seq),
        RepairState::Pending,
        "repair state must be pending during the transient window"
    );

    let events = engine.events_for_commit(receipt.commit_seq);
    assert!(
        events
            .iter()
            .any(|event| event.kind == CommitRepairEventKind::DurableButNotRepairable),
        "durable-but-not-repairable state must be explicitly logged"
    );
}

#[test]
fn test_background_repair_completes() {
    let engine = CommitRepairHarness::new(true, Duration::from_millis(10));
    let receipt = engine.commit(&[0x33; 1024]);
    engine.wait_for_background_repair();

    assert_eq!(
        engine.repair_state_for(receipt.commit_seq),
        RepairState::Completed,
        "background repair should eventually complete"
    );
    assert!(
        engine.total_repair_bytes() > 0,
        "repair symbols should be generated and appended"
    );
    assert!(
        engine.repair_sync_count() > 0,
        "repair symbols should be fsync'd by background task"
    );
}

#[test]
fn test_repair_failure_does_not_affect_durability() {
    let engine = CommitRepairHarness::new(true, Duration::from_millis(10));
    engine.set_fail_repair(true);
    let receipt = engine.commit(&[0x44; 1024]);
    engine.wait_for_background_repair();

    assert!(
        receipt.durable,
        "durability must not depend on repair success"
    );
    assert_eq!(
        engine.repair_state_for(receipt.commit_seq),
        RepairState::Failed,
        "repair failure should be surfaced in repair state"
    );
}

#[test]
fn test_e2e_commit_latency_not_affected_by_repair() {
    let no_repair = CommitRepairHarness::new(false, Duration::ZERO);
    let with_repair = CommitRepairHarness::new(true, Duration::from_millis(8));
    let payload = vec![0x9C; 2048];

    let mut baseline = Vec::new();
    let mut observed = Vec::new();
    let mut commit_seqs = Vec::new();

    for _ in 0..128 {
        baseline.push(no_repair.commit(&payload).latency);
        let receipt = with_repair.commit(&payload);
        observed.push(receipt.latency);
        commit_seqs.push(receipt.commit_seq);
    }
    with_repair.wait_for_background_repair();

    let baseline_p50 = percentile(&baseline, 1, 2);
    let baseline_p99 = percentile(&baseline, 99, 100);
    let observed_p50 = percentile(&observed, 1, 2);
    let observed_p99 = percentile(&observed, 99, 100);
    let budget = Duration::from_millis(5);

    assert!(
        observed_p50.abs_diff(baseline_p50) <= budget,
        "p50 commit latency drift exceeded budget: baseline={baseline_p50:?} observed={observed_p50:?}"
    );
    assert!(
        observed_p99.abs_diff(baseline_p99) <= budget,
        "p99 commit latency drift exceeded budget: baseline={baseline_p99:?} observed={observed_p99:?}"
    );

    for commit_seq in commit_seqs {
        let events = with_repair.events_for_commit(commit_seq);
        let ack = events
            .iter()
            .find(|event| event.kind == CommitRepairEventKind::CommitAcked)
            .expect("commit ack must exist");
        let repair_started = events
            .iter()
            .find(|event| event.kind == CommitRepairEventKind::RepairStarted)
            .expect("repair start must exist");
        let repair_completed = events
            .iter()
            .find(|event| event.kind == CommitRepairEventKind::RepairCompleted)
            .expect("repair completion must exist");

        assert!(
            repair_started.seq >= ack.seq,
            "repair must start after commit acknowledgment"
        );
        assert!(
            repair_completed.seq >= repair_started.seq,
            "repair completion must follow repair start"
        );
        assert!(
            with_repair
                .durable_not_repairable_window(commit_seq)
                .expect("window must be measurable")
                <= Duration::from_millis(250),
            "durable-but-not-repairable window should close within bounded time"
        );
    }
}
