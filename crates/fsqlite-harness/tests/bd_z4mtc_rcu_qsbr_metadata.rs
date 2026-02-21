//! bd-z4mtc: RCU/QSBR for hot metadata paths (§14.8) integration tests.
//!
//! Validates RCU + QSBR primitives for lock-free metadata hot paths:
//!   1. QSBR registration and slot management (MAX_RCU_THREADS=64)
//!   2. Grace period latency under contention
//!   3. RcuCell zero-overhead reads under concurrent writer
//!   4. RcuPair consistent snapshot (no torn reads)
//!   5. RcuTriple consistent snapshot (no torn reads)
//!   6. Epoch monotonicity across grace periods
//!   7. Read-dominated workload (schema cache simulation)
//!   8. Metrics fidelity (delta-based)
//!   9. Slot reuse after handle drop
//!  10. Machine-readable conformance output

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite_mvcc::{
    QsbrRegistry, RcuCell, RcuPair, RcuTriple, rcu_metrics,
    record_rcu_reclaimed, MAX_RCU_THREADS,
};

// ---------------------------------------------------------------------------
// Test 1: QSBR registration fills all 64 slots
// ---------------------------------------------------------------------------

#[test]
fn test_qsbr_registration_capacity() {
    let reg = QsbrRegistry::new();
    let mut handles = Vec::new();

    // Register MAX_RCU_THREADS handles.
    for i in 0..MAX_RCU_THREADS {
        let h = reg.register();
        assert!(h.is_some(), "slot {i} should be available");
        handles.push(h.unwrap());
    }
    assert_eq!(reg.active_threads(), MAX_RCU_THREADS);

    // 65th registration should fail.
    let overflow = reg.register();
    assert!(overflow.is_none(), "should reject 65th registration");

    // Drop one handle and verify slot is freed.
    let slot_idx = handles[0].slot();
    drop(handles.remove(0));
    assert_eq!(reg.active_threads(), MAX_RCU_THREADS - 1);

    // Re-register should succeed, reusing the freed slot.
    let reused = reg.register().expect("freed slot should be reusable");
    assert_eq!(reused.slot(), slot_idx, "should reuse slot {slot_idx}");
    assert_eq!(reg.active_threads(), MAX_RCU_THREADS);

    println!(
        "[PASS] QSBR registration capacity: {MAX_RCU_THREADS} slots, overflow rejected, reuse works"
    );
}

// ---------------------------------------------------------------------------
// Test 2: Grace period latency under contention
// ---------------------------------------------------------------------------

#[test]
fn test_grace_period_latency() {
    let reg = Arc::new(QsbrRegistry::new());
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(5)); // 4 readers + 1 writer

    // Spawn 4 reader threads that quiesce periodically.
    let mut readers = Vec::new();
    for _ in 0..4 {
        let r_reg = Arc::clone(&reg);
        let r_stop = Arc::clone(&stop);
        let r_bar = Arc::clone(&barrier);
        readers.push(thread::spawn(move || {
            let h = r_reg.register().unwrap();
            h.quiescent();
            r_bar.wait();
            let mut qs = 0u64;
            while !r_stop.load(Ordering::Relaxed) {
                h.quiescent();
                qs += 1;
                if qs % 100 == 0 {
                    thread::yield_now();
                }
            }
            h.quiescent();
            drop(h);
            qs
        }));
    }

    // Writer measures grace period latency.
    let w_reg = Arc::clone(&reg);
    let w_bar = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        let h = w_reg.register().unwrap();
        h.quiescent();
        w_bar.wait();

        let mut latencies = Vec::new();
        for _ in 0..20 {
            let start = Instant::now();
            h.synchronize_as_writer();
            latencies.push(start.elapsed());
        }
        drop(h);
        latencies
    });

    let latencies = writer.join().unwrap();
    stop.store(true, Ordering::Release);
    for r in readers {
        r.join().unwrap();
    }

    // Grace periods should complete within 50ms (generous bound).
    let max_lat = latencies.iter().max().unwrap();
    assert!(
        *max_lat < Duration::from_millis(50),
        "max grace period latency too high: {max_lat:?}"
    );

    println!(
        "[PASS] Grace period latency: 20 rounds, max={max_lat:?}, 4 reader threads"
    );
}

// ---------------------------------------------------------------------------
// Test 3: RcuCell zero-overhead reads under concurrent writer
// ---------------------------------------------------------------------------

#[test]
fn test_rcu_cell_concurrent_reads() {
    let reg = Arc::new(QsbrRegistry::new());
    let cell = Arc::new(RcuCell::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(9)); // 8 readers + 1 writer

    // 8 readers
    let mut readers = Vec::new();
    for _ in 0..8 {
        let r_reg = Arc::clone(&reg);
        let r_cell = Arc::clone(&cell);
        let r_stop = Arc::clone(&stop);
        let r_bar = Arc::clone(&barrier);
        readers.push(thread::spawn(move || {
            let h = r_reg.register().unwrap();
            h.quiescent();
            r_bar.wait();
            let mut reads = 0u64;
            let mut max_val = 0u64;
            while !r_stop.load(Ordering::Relaxed) {
                let v = r_cell.read();
                if v > max_val {
                    max_val = v;
                }
                reads += 1;
                if reads % 1000 == 0 {
                    h.quiescent();
                }
            }
            h.quiescent();
            drop(h);
            (reads, max_val)
        }));
    }

    // Writer publishes monotonically increasing values.
    let w_cell = Arc::clone(&cell);
    let w_stop = Arc::clone(&stop);
    let w_bar = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        w_bar.wait();
        let mut val = 0u64;
        while !w_stop.load(Ordering::Relaxed) {
            val += 1;
            w_cell.publish(val);
        }
        val
    });

    thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Release);

    let writes = writer.join().unwrap();
    let mut total_reads = 0u64;
    for r in readers {
        let (reads, _max) = r.join().unwrap();
        total_reads += reads;
    }

    assert!(writes > 0, "writer should have published values");
    assert!(total_reads > 0, "readers should have read values");

    // Final value should be visible.
    let final_val = cell.read();
    assert_eq!(final_val, writes, "final value mismatch");

    println!(
        "[PASS] RcuCell concurrent: writes={writes} total_reads={total_reads} (8 readers)"
    );
}

// ---------------------------------------------------------------------------
// Test 4: RcuPair no torn reads under contention
// ---------------------------------------------------------------------------

#[test]
fn test_rcu_pair_no_torn_reads() {
    let reg = Arc::new(QsbrRegistry::new());
    let pair = Arc::new(RcuPair::new(0, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(5)); // 4 readers + 1 writer

    let mut readers = Vec::new();
    for _ in 0..4 {
        let r_reg = Arc::clone(&reg);
        let r_pair = Arc::clone(&pair);
        let r_stop = Arc::clone(&stop);
        let r_torn = Arc::clone(&torn);
        let r_bar = Arc::clone(&barrier);
        readers.push(thread::spawn(move || {
            let h = r_reg.register().unwrap();
            h.quiescent();
            r_bar.wait();
            let mut reads = 0u64;
            while !r_stop.load(Ordering::Relaxed) {
                let (a, b) = r_pair.read();
                if a != b {
                    r_torn.fetch_add(1, Ordering::Relaxed);
                }
                reads += 1;
                if reads % 500 == 0 {
                    h.quiescent();
                }
            }
            h.quiescent();
            drop(h);
            reads
        }));
    }

    let w_reg = Arc::clone(&reg);
    let w_pair = Arc::clone(&pair);
    let w_stop = Arc::clone(&stop);
    let w_bar = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        let h = w_reg.register().unwrap();
        h.quiescent();
        w_bar.wait();
        let mut val = 0u64;
        while !w_stop.load(Ordering::Relaxed) {
            val += 1;
            w_pair.publish(val, val, &h);
        }
        drop(h);
        val
    });

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    let writes = writer.join().unwrap();
    let mut total_reads = 0u64;
    for r in readers {
        total_reads += r.join().unwrap();
    }

    let torn_count = torn.load(Ordering::Relaxed);
    assert_eq!(torn_count, 0, "TORN READS DETECTED: {torn_count}");
    assert!(writes > 0);
    assert!(total_reads > 0);

    println!(
        "[PASS] RcuPair no torn reads: writes={writes} reads={total_reads} torn=0"
    );
}

// ---------------------------------------------------------------------------
// Test 5: RcuTriple no torn reads under contention
// ---------------------------------------------------------------------------

#[test]
fn test_rcu_triple_no_torn_reads() {
    let reg = Arc::new(QsbrRegistry::new());
    let triple = Arc::new(RcuTriple::new(0, 0, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(5)); // 4 readers + 1 writer

    let mut readers = Vec::new();
    for _ in 0..4 {
        let r_reg = Arc::clone(&reg);
        let r_triple = Arc::clone(&triple);
        let r_stop = Arc::clone(&stop);
        let r_torn = Arc::clone(&torn);
        let r_bar = Arc::clone(&barrier);
        readers.push(thread::spawn(move || {
            let h = r_reg.register().unwrap();
            h.quiescent();
            r_bar.wait();
            let mut reads = 0u64;
            while !r_stop.load(Ordering::Relaxed) {
                let (a, b, c) = r_triple.read();
                if a != b || b != c {
                    r_torn.fetch_add(1, Ordering::Relaxed);
                }
                reads += 1;
                if reads % 500 == 0 {
                    h.quiescent();
                }
            }
            h.quiescent();
            drop(h);
            reads
        }));
    }

    let w_reg = Arc::clone(&reg);
    let w_triple = Arc::clone(&triple);
    let w_stop = Arc::clone(&stop);
    let w_bar = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        let h = w_reg.register().unwrap();
        h.quiescent();
        w_bar.wait();
        let mut val = 0u64;
        while !w_stop.load(Ordering::Relaxed) {
            val += 1;
            w_triple.publish(val, val, val, &h);
        }
        drop(h);
        val
    });

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    let writes = writer.join().unwrap();
    let mut total_reads = 0u64;
    for r in readers {
        total_reads += r.join().unwrap();
    }

    let torn_count = torn.load(Ordering::Relaxed);
    assert_eq!(torn_count, 0, "TORN READS DETECTED: {torn_count}");
    assert!(writes > 0);
    assert!(total_reads > 0);

    println!(
        "[PASS] RcuTriple no torn reads: writes={writes} reads={total_reads} torn=0"
    );
}

// ---------------------------------------------------------------------------
// Test 6: Epoch monotonicity across grace periods
// ---------------------------------------------------------------------------

#[test]
fn test_epoch_monotonicity() {
    let reg = QsbrRegistry::new();
    let h = reg.register().unwrap();

    let initial_epoch = reg.global_epoch();
    assert!(initial_epoch >= 1, "initial epoch should be >= 1");

    let mut prev = initial_epoch;
    for i in 0..10 {
        h.synchronize_as_writer();
        let current = reg.global_epoch();
        assert!(
            current > prev,
            "epoch should increase: round {i}, prev={prev}, current={current}"
        );
        prev = current;
    }

    assert_eq!(
        reg.global_epoch(),
        initial_epoch + 10,
        "epoch should advance by exactly 10"
    );

    drop(h);

    println!(
        "[PASS] Epoch monotonicity: {} -> {}, 10 grace periods",
        initial_epoch,
        initial_epoch + 10
    );
}

// ---------------------------------------------------------------------------
// Test 7: Read-dominated workload (schema cache simulation)
// ---------------------------------------------------------------------------

#[test]
fn test_read_dominated_schema_cache() {
    let reg = Arc::new(QsbrRegistry::new());
    // Simulate schema version + commit_seq metadata.
    let pair = Arc::new(RcuPair::new(1, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(9)); // 8 readers + 1 writer

    let read_count = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));

    // 8 readers simulating query threads checking schema version.
    let mut readers = Vec::new();
    for _ in 0..8 {
        let r_reg = Arc::clone(&reg);
        let r_pair = Arc::clone(&pair);
        let r_stop = Arc::clone(&stop);
        let r_bar = Arc::clone(&barrier);
        let r_count = Arc::clone(&read_count);
        readers.push(thread::spawn(move || {
            let h = r_reg.register().unwrap();
            h.quiescent();
            r_bar.wait();
            let mut local_reads = 0u64;
            while !r_stop.load(Ordering::Relaxed) {
                let (schema_ver, commit_seq) = r_pair.read();
                assert!(schema_ver > 0, "schema version should be > 0");
                let _ = commit_seq; // read but don't assert (can be 0)
                local_reads += 1;
                if local_reads % 1000 == 0 {
                    h.quiescent();
                }
            }
            h.quiescent();
            r_count.fetch_add(local_reads, Ordering::Relaxed);
            drop(h);
        }));
    }

    // Writer: schema updates (rare — every 10ms).
    let w_reg = Arc::clone(&reg);
    let w_pair = Arc::clone(&pair);
    let w_stop = Arc::clone(&stop);
    let w_bar = Arc::clone(&barrier);
    let w_count = Arc::clone(&write_count);
    let writer = thread::spawn(move || {
        let h = w_reg.register().unwrap();
        h.quiescent();
        w_bar.wait();
        let mut schema_ver = 1u64;
        let mut commit_seq = 0u64;
        while !w_stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(10));
            schema_ver += 1;
            commit_seq += 100;
            w_pair.publish(schema_ver, commit_seq, &h);
            w_count.fetch_add(1, Ordering::Relaxed);
        }
        drop(h);
    });

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }

    let reads = read_count.load(Ordering::Relaxed);
    let writes = write_count.load(Ordering::Relaxed);

    // Read-dominated: should have >> 100x more reads than writes.
    let ratio = if writes > 0 { reads / writes } else { reads };
    assert!(
        ratio > 100,
        "read/write ratio should be > 100:1, got {ratio}:1 (reads={reads} writes={writes})"
    );

    println!(
        "[PASS] Read-dominated schema cache: reads={reads} writes={writes} ratio={ratio}:1"
    );
}

// ---------------------------------------------------------------------------
// Test 8: Metrics fidelity (delta-based)
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_fidelity() {
    let m_before = rcu_metrics();

    let reg = QsbrRegistry::new();
    let h = reg.register().unwrap();

    // 5 grace periods.
    for _ in 0..5 {
        h.synchronize_as_writer();
    }

    // Record some reclamations.
    record_rcu_reclaimed(3);

    let m_after = rcu_metrics();
    drop(h);

    let delta_gp = m_after.fsqlite_rcu_grace_periods_total
        - m_before.fsqlite_rcu_grace_periods_total;
    let delta_ns = m_after.fsqlite_rcu_grace_period_duration_ns_total
        - m_before.fsqlite_rcu_grace_period_duration_ns_total;
    let delta_reclaimed = m_after.fsqlite_rcu_reclaimed_total
        - m_before.fsqlite_rcu_reclaimed_total;

    assert_eq!(delta_gp, 5, "should record 5 grace periods");
    assert!(delta_ns > 0, "total grace period duration should be > 0");
    assert!(delta_reclaimed >= 3, "should record at least 3 reclamations");
    assert!(
        m_after.fsqlite_rcu_grace_period_duration_ns_max > 0,
        "max grace period should be > 0"
    );

    println!(
        "[PASS] Metrics fidelity: delta_gp={delta_gp} delta_ns={delta_ns} reclaimed={delta_reclaimed}"
    );
}

// ---------------------------------------------------------------------------
// Test 9: Slot reuse after handle drop
// ---------------------------------------------------------------------------

#[test]
fn test_slot_reuse() {
    let reg = QsbrRegistry::new();

    // Register, drop, re-register — slot should be reused.
    let h1 = reg.register().unwrap();
    let slot1 = h1.slot();
    drop(h1);
    assert_eq!(reg.active_threads(), 0);

    let h2 = reg.register().unwrap();
    let slot2 = h2.slot();
    assert_eq!(slot2, slot1, "should reuse slot {slot1}");
    assert_eq!(reg.active_threads(), 1);

    // Register more, drop middle, verify middle slot is reused.
    let h3 = reg.register().unwrap();
    let slot3 = h3.slot();
    let h4 = reg.register().unwrap();
    let _slot4 = h4.slot();
    assert_eq!(reg.active_threads(), 3);

    drop(h3); // free middle slot
    assert_eq!(reg.active_threads(), 2);

    let h5 = reg.register().unwrap();
    assert_eq!(h5.slot(), slot3, "should reuse freed middle slot {slot3}");
    assert_eq!(reg.active_threads(), 3);

    println!(
        "[PASS] Slot reuse: slot {slot1} reused, middle slot {slot3} reused after drop"
    );
}

// ---------------------------------------------------------------------------
// Test 10: Machine-readable conformance output
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    // Property 1: QSBR registration capacity = MAX_RCU_THREADS.
    let reg = QsbrRegistry::new();
    let h = reg.register().unwrap();
    let cap_ok = MAX_RCU_THREADS == 64;
    drop(h);

    // Property 2: RcuCell read/write consistency.
    let cell = RcuCell::new(42);
    assert_eq!(cell.read(), 42);
    cell.publish(99);
    let cell_ok = cell.read() == 99;

    // Property 3: RcuPair consistent snapshot.
    let reg2 = QsbrRegistry::new();
    let h2 = reg2.register().unwrap();
    let pair = RcuPair::new(1, 2);
    pair.publish(10, 20, &h2);
    h2.quiescent();
    let (a, b) = pair.read();
    let pair_ok = a == 10 && b == 20;
    drop(h2);

    // Property 4: RcuTriple consistent snapshot.
    let reg3 = QsbrRegistry::new();
    let h3 = reg3.register().unwrap();
    let triple = RcuTriple::new(1, 2, 3);
    triple.publish(10, 20, 30, &h3);
    h3.quiescent();
    let (x, y, z) = triple.read();
    let triple_ok = x == 10 && y == 20 && z == 30;
    drop(h3);

    // Property 5: Grace period advances epoch.
    let reg4 = QsbrRegistry::new();
    let h4 = reg4.register().unwrap();
    let e_before = reg4.global_epoch();
    h4.synchronize_as_writer();
    let epoch_ok = reg4.global_epoch() == e_before + 1;
    drop(h4);

    // Property 6: Metrics track grace periods.
    let m = rcu_metrics();
    let metrics_ok = m.fsqlite_rcu_grace_periods_total > 0;

    println!();
    println!("=== Conformance Summary ===");
    println!("  [CONFORM] QSBR capacity: MAX_RCU_THREADS={MAX_RCU_THREADS}");
    println!("  [CONFORM] RcuCell read/write: publish(99) -> read()=99");
    println!("  [CONFORM] RcuPair snapshot: publish(10,20) -> read()=({a},{b})");
    println!("  [CONFORM] RcuTriple snapshot: publish(10,20,30) -> read()=({x},{y},{z})");
    println!("  [CONFORM] Epoch monotonicity: {e_before} -> {}", e_before + 1);
    println!("  [CONFORM] Metrics: grace_periods={}", m.fsqlite_rcu_grace_periods_total);
    println!("  Conformance: 6 / 6 (100.0%)");

    assert!(cap_ok, "QSBR capacity should be 64");
    assert!(cell_ok, "RcuCell read failed");
    assert!(pair_ok, "RcuPair snapshot failed");
    assert!(triple_ok, "RcuTriple snapshot failed");
    assert!(epoch_ok, "epoch did not advance");
    assert!(metrics_ok, "metrics not tracked");
}
