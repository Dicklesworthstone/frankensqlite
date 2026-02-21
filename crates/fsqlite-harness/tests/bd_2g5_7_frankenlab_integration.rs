//! bd-2g5.7: FrankenLab deterministic testing integration.
//!
//! Validates integration of FsLab deterministic scheduling with the concurrency
//! primitives (seqlock, left-right, RCU/QSBR, flat combining):
//!   1. Deterministic replay reproduces same results
//!   2. Concurrent primitive scenarios run under FsLab scheduling
//!   3. Schedule exploration across multiple seeds
//!   4. Chaos injection tolerance
//!   5. Trace certificates and oracle verification
//!   6. Metrics: schedules explored counter
//!   7. Machine-readable conformance output

use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite_harness::fslab::FsLab;
use fsqlite_mvcc::{
    FlatCombiner, LeftRight, LeftRightPair, QsbrRegistry, RcuCell,
    SeqLock,
};

/// Track how many distinct schedule seeds we've explored.
static SCHEDULES_EXPLORED: AtomicU64 = AtomicU64::new(0);

fn record_schedule_explored() {
    SCHEDULES_EXPLORED.fetch_add(1, Ordering::Relaxed);
}

fn reset_schedules_explored() {
    SCHEDULES_EXPLORED.store(0, Ordering::Relaxed);
}

fn schedules_explored() -> u64 {
    SCHEDULES_EXPLORED.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Test 1: Deterministic replay with seqlock
// ---------------------------------------------------------------------------

#[test]
fn test_deterministic_replay_seqlock() {
    let lab = FsLab::new(0xA1B2_C3D4).worker_count(2).max_steps(50_000);

    lab.assert_deterministic(|runtime, root| {
        let (t1, _) = FsLab::spawn_named(runtime, root, "seqlock_writer", async {
            let sl = SeqLock::new(0);
            for i in 1..=20 {
                sl.write(i);
            }
            sl.read("lab_check").unwrap_or(0)
        });
        let mut sched = runtime.scheduler.lock();
        sched.schedule(t1, 0);
    });

    record_schedule_explored();
    println!("[PASS] deterministic replay with seqlock — identical trace fingerprints");
}

// ---------------------------------------------------------------------------
// Test 2: Deterministic replay with left-right
// ---------------------------------------------------------------------------

#[test]
fn test_deterministic_replay_left_right() {
    let lab = FsLab::new(0xE5F6_0718).worker_count(2).max_steps(50_000);

    lab.assert_deterministic(|runtime, root| {
        let (t1, _) = FsLab::spawn_named(runtime, root, "lr_rw", async {
            let lr = LeftRight::new(0);
            for i in 1..=20 {
                lr.write(i);
            }
            lr.read("lab_check")
        });
        let mut sched = runtime.scheduler.lock();
        sched.schedule(t1, 0);
    });

    record_schedule_explored();
    println!("[PASS] deterministic replay with left-right — identical trace fingerprints");
}

// ---------------------------------------------------------------------------
// Test 3: Deterministic replay with RCU
// ---------------------------------------------------------------------------

#[test]
fn test_deterministic_replay_rcu() {
    let lab = FsLab::new(0x1234_5678).worker_count(2).max_steps(50_000);

    lab.assert_deterministic(|runtime, root| {
        let (t1, _) = FsLab::spawn_named(runtime, root, "rcu_rw", async {
            let reg = QsbrRegistry::new();
            let h = reg.register().unwrap();
            let cell = RcuCell::new(0);

            for i in 1..=20 {
                cell.publish(i);
                h.quiescent();
            }
            cell.read()
        });
        let mut sched = runtime.scheduler.lock();
        sched.schedule(t1, 0);
    });

    record_schedule_explored();
    println!("[PASS] deterministic replay with RCU — identical trace fingerprints");
}

// ---------------------------------------------------------------------------
// Test 4: Deterministic replay with flat combining
// ---------------------------------------------------------------------------

#[test]
fn test_deterministic_replay_flat_combining() {
    let lab = FsLab::new(0xDEAD_CAFE).worker_count(2).max_steps(50_000);

    lab.assert_deterministic(|runtime, root| {
        let (t1, _) = FsLab::spawn_named(runtime, root, "fc_ops", async {
            let fc = FlatCombiner::new(0);
            let h = fc.register().unwrap();
            for _ in 0..20 {
                h.add(1);
            }
            h.read()
        });
        let mut sched = runtime.scheduler.lock();
        sched.schedule(t1, 0);
    });

    record_schedule_explored();
    println!("[PASS] deterministic replay with flat combining — identical trace fingerprints");
}

// ---------------------------------------------------------------------------
// Test 5: Multi-task concurrent primitive scenario
// ---------------------------------------------------------------------------

#[test]
fn test_multi_task_concurrent_scenario() {
    let lab = FsLab::new(0xBEEF_F00D).worker_count(4).max_steps(100_000);

    let report = lab.run_with_setup(|runtime, root| {
        // Task 1: Seqlock writer
        let (t1, _) = FsLab::spawn_named(runtime, root, "sl_writer", async {
            let sl = SeqLock::new(0);
            for i in 1..=50 {
                sl.write(i);
            }
            sl.read("final").unwrap_or(0)
        });

        // Task 2: LeftRight writer
        let (t2, _) = FsLab::spawn_named(runtime, root, "lr_writer", async {
            let lr = LeftRight::new(100);
            for _ in 0..50 {
                lr.update(|v| v + 1);
            }
            lr.read("final")
        });

        // Task 3: RCU publisher
        let (t3, _) = FsLab::spawn_named(runtime, root, "rcu_pub", async {
            let reg = QsbrRegistry::new();
            let h = reg.register().unwrap();
            let cell = RcuCell::new(0);
            for i in 1..=50 {
                cell.publish(i);
                h.quiescent();
            }
            cell.read()
        });

        // Task 4: Flat combiner
        let (t4, _) = FsLab::spawn_named(runtime, root, "fc_add", async {
            let fc = FlatCombiner::new(0);
            let h = fc.register().unwrap();
            for _ in 0..50 {
                h.add(1);
            }
            h.read()
        });

        let mut sched = runtime.scheduler.lock();
        sched.schedule(t1, 0);
        sched.schedule(t2, 1);
        sched.schedule(t3, 2);
        sched.schedule(t4, 3);
    });

    assert!(
        report.oracle_report.all_passed(),
        "oracle failures: {:?}",
        report.oracle_report
    );
    assert!(report.quiescent, "runtime not quiescent");
    assert!(report.steps_total > 0, "no steps executed");

    record_schedule_explored();
    println!(
        "[PASS] multi-task concurrent scenario: steps={} fingerprint={}",
        report.steps_total, report.trace_fingerprint
    );
}

// ---------------------------------------------------------------------------
// Test 6: Chaos injection tolerance
// ---------------------------------------------------------------------------

#[test]
fn test_chaos_injection_tolerance() {
    let lab = FsLab::new(0xC0FF_EE42)
        .worker_count(2)
        .max_steps(50_000)
        .with_light_chaos();

    let report = lab.run_with_setup(|runtime, root| {
        let (t1, _) = FsLab::spawn_named(runtime, root, "chaos_sl", async {
            let sl = SeqLock::new(0);
            for i in 1..=10 {
                sl.write(i);
            }
            let v = sl.read("chaos_check").unwrap_or(0);
            assert!(v <= 10, "seqlock value out of range: {v}");
            v
        });

        let (t2, _) = FsLab::spawn_named(runtime, root, "chaos_lr", async {
            let lr = LeftRightPair::new(0, 0);
            for i in 1..=10 {
                lr.write(i, i);
            }
            let (a, b) = lr.read("chaos_check");
            assert_eq!(a, b, "torn read in left-right: a={a} b={b}");
            a
        });

        let mut sched = runtime.scheduler.lock();
        sched.schedule(t1, 0);
        sched.schedule(t2, 1);
    });

    assert!(
        report.invariant_violations.is_empty(),
        "invariant violations under chaos: {:?}",
        report.invariant_violations
    );

    record_schedule_explored();
    println!("[PASS] chaos injection tolerance — no invariant violations");
}

// ---------------------------------------------------------------------------
// Test 7: Schedule exploration across seeds
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_exploration() {
    reset_schedules_explored();

    let seeds: Vec<u64> = (0..20).map(|i| 0xFACE_0000 + i).collect();
    let mut all_fingerprints = Vec::new();

    for &seed in &seeds {
        let lab = FsLab::new(seed).worker_count(2).max_steps(20_000);

        let report = lab.run_with_setup(|runtime, root| {
            let (t1, _) = FsLab::spawn_named(runtime, root, "explore_a", async {
                let sl = SeqLock::new(0);
                for i in 1..=5 {
                    sl.write(i);
                }
                sl.read("done").unwrap_or(0)
            });

            let (t2, _) = FsLab::spawn_named(runtime, root, "explore_b", async {
                let lr = LeftRight::new(0);
                for _ in 0..5 {
                    lr.update(|v| v + 1);
                }
                lr.read("done")
            });

            let mut sched = runtime.scheduler.lock();
            sched.schedule(t1, 0);
            sched.schedule(t2, 1);
        });

        assert!(
            report.oracle_report.all_passed(),
            "oracle failure at seed={seed:#x}"
        );
        all_fingerprints.push(report.trace_fingerprint);
        record_schedule_explored();
    }

    let explored = schedules_explored();
    assert!(
        explored >= 20,
        "expected >= 20 schedules explored, got {explored}"
    );

    // Count distinct fingerprints (different seeds may produce different schedules).
    let mut unique = all_fingerprints.clone();
    unique.sort_unstable();
    unique.dedup();

    tracing::info!(
        target: "fsqlite.lab_schedule",
        schedule_seed = 0u64,
        thread_count = 2u32,
        interleaving_id = explored,
        "lab_schedule exploration complete"
    );

    println!(
        "[PASS] schedule exploration: seeds={} explored={explored} distinct_fingerprints={}",
        seeds.len(),
        unique.len()
    );
}

// ---------------------------------------------------------------------------
// Test 8: Trace certificate validation
// ---------------------------------------------------------------------------

#[test]
fn test_trace_certificates() {
    let lab = FsLab::new(0xABCD_EF01).worker_count(2).max_steps(50_000);

    let report = lab.run_with_setup(|runtime, root| {
        let (t1, _) = FsLab::spawn_named(runtime, root, "cert_task", async {
            let fc = FlatCombiner::new(100);
            let h = fc.register().unwrap();
            h.add(1);
            h.add(2);
            h.add(3);
            h.read()
        });
        let mut sched = runtime.scheduler.lock();
        sched.schedule(t1, 0);
    });

    // Trace certificate should be populated.
    assert!(
        report.trace_certificate.schedule_hash != 0,
        "schedule_hash should be non-zero"
    );
    assert!(report.trace_fingerprint != 0, "fingerprint should be non-zero");

    // Replay same seed — should produce identical certificate.
    let report2 = lab.run_with_setup(|runtime, root| {
        let (t1, _) = FsLab::spawn_named(runtime, root, "cert_task", async {
            let fc = FlatCombiner::new(100);
            let h = fc.register().unwrap();
            h.add(1);
            h.add(2);
            h.add(3);
            h.read()
        });
        let mut sched = runtime.scheduler.lock();
        sched.schedule(t1, 0);
    });

    assert_eq!(
        report.trace_certificate.schedule_hash,
        report2.trace_certificate.schedule_hash,
        "trace certificates differ on replay"
    );

    record_schedule_explored();
    println!(
        "[PASS] trace certificates: hash={} fingerprint={}",
        report.trace_certificate.schedule_hash, report.trace_fingerprint
    );
}

// ---------------------------------------------------------------------------
// Test 9: Conformance summary (JSON)
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::too_many_lines)]
fn test_conformance_summary() {
    struct TestResult {
        name: &'static str,
        pass: bool,
        detail: String,
    }

    let mut results = Vec::new();

    // 1. FsLab smoke — trivial task completes
    {
        let lab = FsLab::new(1).worker_count(1).max_steps(5_000);
        let report = lab.run_single_task(async { 42_u64 });
        let pass = report.oracle_report.all_passed() && report.quiescent;
        results.push(TestResult {
            name: "fslab_smoke",
            pass,
            detail: format!("quiescent={} steps={}", report.quiescent, report.steps_total),
        });
    }

    // 2. Deterministic replay
    {
        let lab = FsLab::new(0x42).worker_count(2).max_steps(10_000);
        let r1 = lab.run_single_task(async {
            let sl = SeqLock::new(0);
            sl.write(99);
            sl.read("det").unwrap_or(0)
        });
        let r2 = lab.run_single_task(async {
            let sl = SeqLock::new(0);
            sl.write(99);
            sl.read("det").unwrap_or(0)
        });
        let pass = r1.trace_fingerprint == r2.trace_fingerprint;
        results.push(TestResult {
            name: "deterministic_replay",
            pass,
            detail: format!("fp1={} fp2={}", r1.trace_fingerprint, r2.trace_fingerprint),
        });
    }

    // 3. Multi-primitive correctness
    {
        let lab = FsLab::new(0x99).worker_count(2).max_steps(20_000);
        let report = lab.run_with_setup(|runtime, root| {
            let (t1, _) = FsLab::spawn_named(runtime, root, "mp_sl", async {
                let sl = SeqLock::new(0);
                sl.write(5);
                sl.read("mp").unwrap_or(0)
            });
            let (t2, _) = FsLab::spawn_named(runtime, root, "mp_lr", async {
                let lr = LeftRight::new(0);
                lr.write(5);
                lr.read("mp")
            });
            let mut sched = runtime.scheduler.lock();
            sched.schedule(t1, 0);
            sched.schedule(t2, 1);
        });
        let pass = report.oracle_report.all_passed() && report.quiescent;
        results.push(TestResult {
            name: "multi_primitive_correctness",
            pass,
            detail: format!("steps={}", report.steps_total),
        });
    }

    // 4. Chaos tolerance
    {
        let lab = FsLab::new(0xCC)
            .worker_count(2)
            .max_steps(10_000)
            .with_light_chaos();
        let report = lab.run_single_task(async {
            let fc = FlatCombiner::new(0);
            let h = fc.register().unwrap();
            h.add(10);
            h.read()
        });
        let pass = report.invariant_violations.is_empty();
        results.push(TestResult {
            name: "chaos_tolerance",
            pass,
            detail: format!(
                "violations={}",
                report.invariant_violations.len()
            ),
        });
    }

    // 5. Schedule exploration (10 seeds)
    {
        let mut explored = 0u32;
        let mut all_pass = true;
        for seed in 0..10 {
            let lab = FsLab::new(seed).worker_count(2).max_steps(5_000);
            let report = lab.run_single_task(async move {
                let lr = LeftRight::new(0);
                lr.write(seed);
                lr.read("explore")
            });
            if !report.oracle_report.all_passed() {
                all_pass = false;
            }
            explored += 1;
        }
        results.push(TestResult {
            name: "schedule_exploration",
            pass: all_pass && explored >= 10,
            detail: format!("explored={explored}"),
        });
    }

    // 6. Trace certificate non-zero
    {
        let lab = FsLab::new(0xAA).worker_count(1).max_steps(5_000);
        let report = lab.run_single_task(async { 1_u64 });
        let pass =
            report.trace_certificate.schedule_hash != 0 && report.trace_fingerprint != 0;
        results.push(TestResult {
            name: "trace_certificate",
            pass,
            detail: format!(
                "hash={} fp={}",
                report.trace_certificate.schedule_hash, report.trace_fingerprint
            ),
        });
    }

    // Summary
    let total = results.len();
    let passed = results.iter().filter(|r| r.pass).count();
    let failed = total - passed;

    println!("\n=== bd-2g5.7: FrankenLab Integration Conformance Summary ===");
    println!("{{");
    println!("  \"bead\": \"bd-2g5.7\",");
    println!("  \"suite\": \"frankenlab_integration\",");
    println!("  \"total\": {total},");
    println!("  \"passed\": {passed},");
    println!("  \"failed\": {failed},");
    println!(
        "  \"pass_rate\": \"{:.1}%\",",
        passed as f64 / total as f64 * 100.0
    );
    println!("  \"cases\": [");
    for (i, r) in results.iter().enumerate() {
        let comma = if i + 1 < total { "," } else { "" };
        let status = if r.pass { "PASS" } else { "FAIL" };
        println!(
            "    {{ \"name\": \"{}\", \"status\": \"{status}\", \"detail\": \"{}\" }}{comma}",
            r.name, r.detail
        );
    }
    println!("  ]");
    println!("}}");

    assert_eq!(
        failed, 0,
        "{failed}/{total} FrankenLab integration conformance tests failed"
    );

    println!("[PASS] all {total} FrankenLab integration conformance tests passed");
}
