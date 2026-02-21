//! bd-2g5.2: RCU/QSBR correctness tests — zero-overhead metadata reads.
//!
//! Validates the RCU/QSBR protocol for lock-free metadata hot paths:
//!   1. Registry register/unregister lifecycle
//!   2. RcuCell single-value read/publish
//!   3. RcuPair consistent snapshot (no torn reads)
//!   4. RcuTriple consistent snapshot (no torn reads)
//!   5. Grace period waits for readers
//!   6. Metrics and tracing integration
//!   7. Stress test (readers never observe torn state)
//!   8. Machine-readable conformance output

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use fsqlite_mvcc::{
    QsbrRegistry, RcuCell, RcuPair, RcuTriple, rcu_metrics, record_rcu_reclaimed,
    reset_rcu_metrics,
};

// ---------------------------------------------------------------------------
// Test 1: Registry lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_registry_lifecycle() {
    let reg = QsbrRegistry::new();
    assert_eq!(reg.active_threads(), 0);
    assert_eq!(reg.global_epoch(), 1);

    let h1 = reg.register().unwrap();
    assert_eq!(reg.active_threads(), 1);
    assert_eq!(h1.slot(), 0);

    let h2 = reg.register().unwrap();
    assert_eq!(reg.active_threads(), 2);

    drop(h1);
    assert_eq!(reg.active_threads(), 1);

    drop(h2);
    assert_eq!(reg.active_threads(), 0);

    println!("[PASS] registry lifecycle: register/unregister/active_threads");
}

// ---------------------------------------------------------------------------
// Test 2: RcuCell read/publish
// ---------------------------------------------------------------------------

#[test]
fn test_rcu_cell_read_publish() {
    let cell = RcuCell::new(0);
    assert_eq!(cell.read(), 0);

    for i in 1..=100 {
        cell.publish(i);
        assert_eq!(cell.read(), i);
    }

    println!("[PASS] rcu_cell read/publish correctness");
}

// ---------------------------------------------------------------------------
// Test 3: RcuPair consistent snapshot
// ---------------------------------------------------------------------------

#[test]
fn test_rcu_pair_consistent_snapshot() {
    let reg = QsbrRegistry::new();
    let h = reg.register().unwrap();
    let pair = RcuPair::new(1, 2);
    assert_eq!(pair.read(), (1, 2));

    h.quiescent();
    pair.publish(10, 20, &h);
    h.quiescent();
    assert_eq!(pair.read(), (10, 20));

    pair.publish(100, 200, &h);
    h.quiescent();
    assert_eq!(pair.read(), (100, 200));

    drop(h);
    println!("[PASS] rcu_pair consistent snapshot");
}

// ---------------------------------------------------------------------------
// Test 4: RcuTriple consistent snapshot
// ---------------------------------------------------------------------------

#[test]
fn test_rcu_triple_consistent_snapshot() {
    let reg = QsbrRegistry::new();
    let h = reg.register().unwrap();
    let triple = RcuTriple::new(1, 2, 3);
    assert_eq!(triple.read(), (1, 2, 3));

    h.quiescent();
    triple.publish(10, 20, 30, &h);
    h.quiescent();
    assert_eq!(triple.read(), (10, 20, 30));

    drop(h);
    println!("[PASS] rcu_triple consistent snapshot");
}

// ---------------------------------------------------------------------------
// Test 5: Grace period waits for reader
// ---------------------------------------------------------------------------

#[test]
fn test_grace_period_waits_for_reader() {
    let reg = Arc::new(QsbrRegistry::new());

    let reg_r = Arc::clone(&reg);
    let ready = Arc::new(Barrier::new(2));
    let ready_r = Arc::clone(&ready);
    let do_quiescent = Arc::new(AtomicBool::new(false));
    let do_q_r = Arc::clone(&do_quiescent);

    let reader = thread::spawn(move || {
        let h = reg_r.register().unwrap();
        h.quiescent();
        ready_r.wait();
        // Simulate being in a read critical section.
        while !do_q_r.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        h.quiescent();
        thread::sleep(Duration::from_millis(10));
        drop(h);
    });

    ready.wait();

    let reg_w = Arc::clone(&reg);
    let do_q_w = Arc::clone(&do_quiescent);
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        do_q_w.store(true, Ordering::Release);
        reg_w.synchronize();
    });

    writer.join().unwrap();
    reader.join().unwrap();

    println!("[PASS] grace period waits for reader before completing");
}

// ---------------------------------------------------------------------------
// Test 6: Metrics integration
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_integration() {
    reset_rcu_metrics();

    let reg = QsbrRegistry::new();
    let h = reg.register().unwrap();

    h.synchronize_as_writer();
    h.synchronize_as_writer();
    h.synchronize_as_writer();

    record_rcu_reclaimed(5);

    let m = rcu_metrics();
    assert_eq!(
        m.fsqlite_rcu_grace_periods_total, 3,
        "expected 3 grace periods"
    );
    assert!(
        m.fsqlite_rcu_grace_period_duration_ns_total > 0,
        "expected non-zero total duration"
    );
    assert!(
        m.fsqlite_rcu_grace_period_duration_ns_max > 0,
        "expected non-zero max duration"
    );
    assert_eq!(
        m.fsqlite_rcu_reclaimed_total, 5,
        "expected 5 reclaimed"
    );

    // Verify serialization.
    let json = serde_json::to_string(&m).unwrap();
    assert!(json.contains("fsqlite_rcu_grace_periods_total"));
    assert!(json.contains("fsqlite_rcu_reclaimed_total"));

    drop(h);

    println!(
        "[PASS] metrics: grace_periods={} reclaimed={} max_ns={}",
        m.fsqlite_rcu_grace_periods_total,
        m.fsqlite_rcu_reclaimed_total,
        m.fsqlite_rcu_grace_period_duration_ns_max
    );
}

// ---------------------------------------------------------------------------
// Test 7: Stress test (no torn reads under contention)
// ---------------------------------------------------------------------------

#[test]
fn test_stress_no_torn_reads() {
    let reg = Arc::new(QsbrRegistry::new());
    let pair = Arc::new(RcuPair::new(0, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let global_reads = Arc::new(AtomicU64::new(0));
    let global_writes = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(4)); // 1 writer + 3 readers

    // Writer thread
    let w_reg = Arc::clone(&reg);
    let w_pair = Arc::clone(&pair);
    let w_stop = Arc::clone(&stop);
    let w_writes = Arc::clone(&global_writes);
    let w_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        let h = w_reg.register().unwrap();
        h.quiescent();
        w_barrier.wait();
        let mut val = 0u64;
        while !w_stop.load(Ordering::Relaxed) {
            val += 1;
            w_pair.publish(val, val, &h);
        }
        w_writes.store(val, Ordering::Relaxed);
        drop(h);
    });

    // 3 reader threads
    let mut readers = Vec::new();
    for _ in 0..3 {
        let r_reg = Arc::clone(&reg);
        let r_pair = Arc::clone(&pair);
        let r_stop = Arc::clone(&stop);
        let r_reads = Arc::clone(&global_reads);
        let r_barrier = Arc::clone(&barrier);
        readers.push(thread::spawn(move || {
            let h = r_reg.register().unwrap();
            h.quiescent();
            r_barrier.wait();
            let mut local_reads = 0u64;
            while !r_stop.load(Ordering::Relaxed) {
                let (a, b) = r_pair.read();
                assert_eq!(a, b, "TORN READ: a={a} b={b}");
                local_reads += 1;
                if local_reads % 1000 == 0 {
                    h.quiescent();
                }
            }
            h.quiescent();
            r_reads.fetch_add(local_reads, Ordering::Relaxed);
            drop(h);
        }));
    }

    thread::sleep(Duration::from_millis(500));
    stop.store(true, Ordering::Release);

    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }

    let total_writes = global_writes.load(Ordering::Relaxed);
    let reads = global_reads.load(Ordering::Relaxed);

    assert!(total_writes > 0, "writer must complete some publishes");
    assert!(reads > 0, "readers must complete some reads");

    println!("[PASS] stress no-torn-reads: writes={total_writes} reads={reads}");
}

// ---------------------------------------------------------------------------
// Test 8: Conformance summary (JSON)
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

    // 1. Registry lifecycle
    {
        let reg = QsbrRegistry::new();
        let h = reg.register().unwrap();
        let active = reg.active_threads();
        drop(h);
        let after = reg.active_threads();
        let pass = active == 1 && after == 0;
        results.push(TestResult {
            name: "registry_lifecycle",
            pass,
            detail: format!("active={active} after_drop={after}"),
        });
    }

    // 2. RcuCell correctness
    {
        let cell = RcuCell::new(0);
        cell.publish(42);
        let v = cell.read();
        let pass = v == 42;
        results.push(TestResult {
            name: "rcu_cell_correctness",
            pass,
            detail: format!("read={v}"),
        });
    }

    // 3. RcuPair consistency
    {
        let reg = QsbrRegistry::new();
        let h = reg.register().unwrap();
        let pair = RcuPair::new(0, 0);
        h.quiescent();
        pair.publish(7, 7, &h);
        h.quiescent();
        let (a, b) = pair.read();
        let pass = a == 7 && b == 7;
        results.push(TestResult {
            name: "rcu_pair_consistency",
            pass,
            detail: format!("a={a} b={b}"),
        });
        drop(h);
    }

    // 4. RcuTriple consistency
    {
        let reg = QsbrRegistry::new();
        let h = reg.register().unwrap();
        let triple = RcuTriple::new(0, 0, 0);
        h.quiescent();
        triple.publish(5, 5, 5, &h);
        h.quiescent();
        let (a, b, c) = triple.read();
        let pass = a == 5 && b == 5 && c == 5;
        results.push(TestResult {
            name: "rcu_triple_consistency",
            pass,
            detail: format!("a={a} b={b} c={c}"),
        });
        drop(h);
    }

    // 5. Grace period metrics
    {
        reset_rcu_metrics();
        let reg = QsbrRegistry::new();
        let h = reg.register().unwrap();
        h.synchronize_as_writer();
        let m = rcu_metrics();
        let pass = m.fsqlite_rcu_grace_periods_total == 1;
        results.push(TestResult {
            name: "grace_period_metrics",
            pass,
            detail: format!("grace_periods={}", m.fsqlite_rcu_grace_periods_total),
        });
        drop(h);
    }

    // 6. No torn reads under contention (quick)
    {
        let reg = Arc::new(QsbrRegistry::new());
        let pair = Arc::new(RcuPair::new(0, 0));
        let stop = Arc::new(AtomicBool::new(false));
        let torn = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(2));

        let w_reg = Arc::clone(&reg);
        let w_pair = Arc::clone(&pair);
        let w_stop = Arc::clone(&stop);
        let wb = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            let h = w_reg.register().unwrap();
            h.quiescent();
            wb.wait();
            let mut v = 0u64;
            while !w_stop.load(Ordering::Relaxed) {
                v += 1;
                w_pair.publish(v, v, &h);
            }
            drop(h);
        });

        let r_reg = Arc::clone(&reg);
        let r_pair = Arc::clone(&pair);
        let r_stop = Arc::clone(&stop);
        let rt = Arc::clone(&torn);
        let rb = Arc::clone(&barrier);
        let reader = thread::spawn(move || {
            let h = r_reg.register().unwrap();
            h.quiescent();
            rb.wait();
            let mut reads = 0u64;
            while !r_stop.load(Ordering::Relaxed) {
                let (a, b) = r_pair.read();
                if a != b {
                    rt.store(true, Ordering::Relaxed);
                }
                reads += 1;
                if reads % 1000 == 0 {
                    h.quiescent();
                }
            }
            h.quiescent();
            drop(h);
        });

        thread::sleep(Duration::from_millis(100));
        stop.store(true, Ordering::Release);
        writer.join().unwrap();
        reader.join().unwrap();

        let pass = !torn.load(Ordering::Relaxed);
        results.push(TestResult {
            name: "no_torn_reads",
            pass,
            detail: format!("torn={}", !pass),
        });
    }

    // Summary
    let total = results.len();
    let passed = results.iter().filter(|r| r.pass).count();
    let failed = total - passed;

    println!("\n=== bd-2g5.2: RCU/QSBR Conformance Summary ===");
    println!("{{");
    println!("  \"bead\": \"bd-2g5.2\",");
    println!("  \"suite\": \"rcu_correctness\",");
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
        "{failed}/{total} RCU/QSBR conformance tests failed"
    );

    println!("[PASS] all {total} RCU/QSBR conformance tests passed");
}
