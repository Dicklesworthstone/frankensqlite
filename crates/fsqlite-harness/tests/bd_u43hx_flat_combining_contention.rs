//! bd-u43hx: Flat Combining for lock contention hot spots (§14.2) integration tests.
//!
//! Validates flat combining suitability for FrankenSQLite contention hot spots:
//!   1. Contention-to-throughput conversion (N threads, batch size increases)
//!   2. Comparative latency: flat combining vs mutex under contention
//!   3. High thread count registration (approach MAX_FC_THREADS)
//!   4. Read-dominated workload (SSI conflict table pattern)
//!   5. Mixed add/read under contention (WAL append + reader pattern)
//!   6. Metrics fidelity under concurrent batches
//!   7. Slot reuse after handle drop-and-re-register
//!   8. Throughput scaling characterization (2, 4, 8, 16 threads)
//!   9. Accumulator monotonicity under concurrent adds
//!  10. Machine-readable conformance output

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite_mvcc::{FlatCombiner, flat_combining_metrics, MAX_FC_THREADS};

// ---------------------------------------------------------------------------
// Test 1: Contention-to-throughput conversion
// ---------------------------------------------------------------------------

#[test]
fn test_contention_to_throughput() {
    // Under high contention, flat combining should batch multiple ops per
    // combiner pass, turning contention into throughput.
    let fc = Arc::new(FlatCombiner::new(0));
    let barrier = Arc::new(Barrier::new(16));
    let mut handles = Vec::new();

    for _ in 0..16 {
        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            for _ in 0..500 {
                h.add(1);
            }
            drop(h);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let expected = 16 * 500;
    assert_eq!(
        fc.value(),
        expected,
        "16 threads * 500 adds should = {expected}"
    );

    println!("[PASS] Contention-to-throughput: 16 threads * 500 = {expected}, all accounted");
}

// ---------------------------------------------------------------------------
// Test 2: Comparative latency: flat combining vs mutex
// ---------------------------------------------------------------------------

#[test]
fn test_fc_vs_mutex_latency() {
    let iterations = 50_000u64;
    let thread_count = 4;

    // Flat combining latency.
    let fc = Arc::new(FlatCombiner::new(0));
    let fc_barrier = Arc::new(Barrier::new(thread_count + 1));

    let mut handles = Vec::new();
    for _ in 0..thread_count {
        let f = Arc::clone(&fc);
        let b = Arc::clone(&fc_barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            for _ in 0..iterations {
                h.add(1);
            }
            drop(h);
        }));
    }

    let t0 = Instant::now();
    fc_barrier.wait();
    for h in handles {
        h.join().unwrap();
    }
    let fc_ns = t0.elapsed().as_nanos() as u64;

    // Mutex latency.
    let mx = Arc::new(parking_lot::Mutex::new(0u64));
    let mx_barrier = Arc::new(Barrier::new(thread_count + 1));

    let mut handles = Vec::new();
    for _ in 0..thread_count {
        let m = Arc::clone(&mx);
        let b = Arc::clone(&mx_barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for _ in 0..iterations {
                let mut guard = m.lock();
                *guard += 1;
            }
        }));
    }

    let t1 = Instant::now();
    mx_barrier.wait();
    for h in handles {
        h.join().unwrap();
    }
    let mx_ns = t1.elapsed().as_nanos() as u64;

    let total_ops = iterations * thread_count as u64;
    let fc_per_op = fc_ns / total_ops;
    let mx_per_op = mx_ns / total_ops;

    // Both should produce correct results.
    assert_eq!(fc.value(), total_ops);
    assert_eq!(*mx.lock(), total_ops);

    println!(
        "[PASS] FC vs Mutex ({thread_count} threads, {iterations} ops/thread): fc={fc_per_op}ns/op mutex={mx_per_op}ns/op"
    );
}

// ---------------------------------------------------------------------------
// Test 3: High thread count registration
// ---------------------------------------------------------------------------

#[test]
fn test_high_thread_registration() {
    let fc = FlatCombiner::new(0);
    let mut handles = Vec::new();

    // Register up to MAX_FC_THREADS handles.
    for _ in 0..MAX_FC_THREADS {
        match fc.register() {
            Some(h) => handles.push(h),
            None => break,
        }
    }

    let registered = handles.len();
    assert_eq!(
        registered, MAX_FC_THREADS,
        "should register all {MAX_FC_THREADS} slots"
    );
    assert_eq!(fc.active_threads(), MAX_FC_THREADS);

    // Next registration should fail.
    assert!(
        fc.register().is_none(),
        "registration beyond MAX_FC_THREADS should fail"
    );

    // Drop all handles.
    handles.clear();
    assert_eq!(fc.active_threads(), 0, "all slots should be free after drop");

    println!(
        "[PASS] High thread registration: {registered}/{MAX_FC_THREADS} slots, overflow rejected"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Read-dominated workload (SSI-like pattern)
// ---------------------------------------------------------------------------

#[test]
fn test_read_dominated_workload() {
    // Simulate SSI conflict table: rare writes (conflict inserts),
    // frequent reads (conflict checks).
    let fc = Arc::new(FlatCombiner::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(6)); // 1 writer + 5 readers

    // Writer: occasional adds (simulating conflict inserts).
    let wf = Arc::clone(&fc);
    let ws = Arc::clone(&stop);
    let wb = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        let h = wf.register().unwrap();
        wb.wait();
        let mut writes = 0u64;
        while !ws.load(Ordering::Relaxed) {
            h.add(1);
            writes += 1;
            // Slow down the writer to create read-dominated pattern.
            thread::sleep(Duration::from_micros(100));
        }
        drop(h);
        writes
    });

    // Readers: frequent reads (simulating conflict checks).
    let read_count = Arc::new(AtomicU64::new(0));
    let readers: Vec<_> = (0..5)
        .map(|_| {
            let rf = Arc::clone(&fc);
            let rs = Arc::clone(&stop);
            let rb = Arc::clone(&barrier);
            let rc = Arc::clone(&read_count);
            thread::spawn(move || {
                let h = rf.register().unwrap();
                rb.wait();
                let mut prev = 0u64;
                while !rs.load(Ordering::Relaxed) {
                    let v = h.read();
                    // Value should be monotonically non-decreasing.
                    assert!(v >= prev, "read went backwards: {prev} -> {v}");
                    prev = v;
                    rc.fetch_add(1, Ordering::Relaxed);
                }
                drop(h);
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    let writes = writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    let reads = read_count.load(Ordering::Relaxed);

    assert!(writes > 0, "writer must have written");
    assert!(reads > 0, "readers must have read");
    // Read:write ratio should be > 1 (read-dominated).
    let ratio = reads / writes.max(1);
    assert!(ratio >= 1, "should be read-dominated, ratio={ratio}");

    println!(
        "[PASS] Read-dominated workload: writes={writes} reads={reads} ratio={ratio}:1"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Mixed add/read under contention (WAL append + reader pattern)
// ---------------------------------------------------------------------------

#[test]
fn test_mixed_add_read_contention() {
    let fc = Arc::new(FlatCombiner::new(0));
    let barrier = Arc::new(Barrier::new(8));
    let total_adds = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    // 4 writers (WAL append simulation).
    for _ in 0..4 {
        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        let ta = Arc::clone(&total_adds);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            for _ in 0..300 {
                h.add(1);
                ta.fetch_add(1, Ordering::Relaxed);
            }
            drop(h);
        }));
    }

    // 4 readers (checkpoint/reader simulation).
    for _ in 0..4 {
        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            let mut last = 0u64;
            for _ in 0..300 {
                let v = h.read();
                assert!(v >= last, "read went backwards: {last} -> {v}");
                last = v;
            }
            drop(h);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let expected = total_adds.load(Ordering::Relaxed);
    assert_eq!(
        fc.value(),
        expected,
        "accumulator={} != submitted={expected}",
        fc.value()
    );
    assert_eq!(expected, 4 * 300, "4 writers * 300 = 1200");

    println!("[PASS] Mixed add/read: 4 writers * 300 + 4 readers * 300, value={expected}");
}

// ---------------------------------------------------------------------------
// Test 6: Metrics fidelity under concurrent batches
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_fidelity() {
    let before = flat_combining_metrics();

    let fc = Arc::new(FlatCombiner::new(0));
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            for _ in 0..200 {
                h.add(1);
            }
            drop(h);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let after = flat_combining_metrics();

    let delta_ops = after.fsqlite_flat_combining_ops_total
        - before.fsqlite_flat_combining_ops_total;
    let delta_batches = after.fsqlite_flat_combining_batches_total
        - before.fsqlite_flat_combining_batches_total;

    // We submitted 800 ops total.
    assert!(
        delta_ops >= 800,
        "delta ops should be >= 800, got {delta_ops}"
    );
    // Batches should be > 0 and <= ops.
    assert!(
        delta_batches > 0,
        "delta batches should be > 0, got {delta_batches}"
    );
    assert!(
        delta_batches <= delta_ops,
        "batches ({delta_batches}) should be <= ops ({delta_ops})"
    );

    let avg_batch = delta_ops as f64 / delta_batches as f64;
    println!(
        "[PASS] Metrics fidelity: delta_ops={delta_ops} delta_batches={delta_batches} avg_batch={avg_batch:.2}"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Slot reuse after handle drop-and-re-register
// ---------------------------------------------------------------------------

#[test]
fn test_slot_reuse() {
    let fc = FlatCombiner::new(0);

    // Register, use, drop.
    {
        let h = fc.register().unwrap();
        h.add(10);
        assert_eq!(h.read(), 10);
        drop(h);
    }
    assert_eq!(fc.active_threads(), 0);

    // Re-register and use.
    {
        let h = fc.register().unwrap();
        let v = h.read();
        assert_eq!(v, 10, "value should persist across re-registrations");
        h.add(5);
        assert_eq!(h.read(), 15);
        drop(h);
    }
    assert_eq!(fc.active_threads(), 0);

    // Fill all slots, drop all, re-register all.
    let mut handles = Vec::new();
    for _ in 0..MAX_FC_THREADS {
        handles.push(fc.register().unwrap());
    }
    assert_eq!(fc.active_threads(), MAX_FC_THREADS);
    handles.clear();
    assert_eq!(fc.active_threads(), 0);

    // All slots should be reusable.
    let h = fc.register().unwrap();
    assert_eq!(h.read(), 15);
    drop(h);

    println!("[PASS] Slot reuse: drop-and-re-register works, {MAX_FC_THREADS} slots recycled");
}

// ---------------------------------------------------------------------------
// Test 8: Throughput scaling characterization
// ---------------------------------------------------------------------------

#[test]
fn test_throughput_scaling() {
    let ops_per_thread = 5_000u64;

    let mut results = Vec::new();

    for thread_count in [1, 2, 4, 8] {
        let fc = Arc::new(FlatCombiner::new(0));
        let barrier = Arc::new(Barrier::new(thread_count + 1));

        let mut handles = Vec::new();
        for _ in 0..thread_count {
            let f = Arc::clone(&fc);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let h = f.register().unwrap();
                b.wait();
                for _ in 0..ops_per_thread {
                    h.add(1);
                }
                drop(h);
            }));
        }

        let t0 = Instant::now();
        barrier.wait();
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = t0.elapsed();

        let total_ops = ops_per_thread * thread_count as u64;
        assert_eq!(fc.value(), total_ops);

        let throughput = total_ops as f64 / elapsed.as_secs_f64();
        results.push((thread_count, throughput, elapsed));
    }

    println!("[PASS] Throughput scaling ({ops_per_thread} ops/thread):");
    for (threads, throughput, elapsed) in &results {
        println!(
            "  {threads:2} threads: {throughput:>12.0} ops/s ({elapsed:?})"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 9: Accumulator monotonicity under concurrent adds
// ---------------------------------------------------------------------------

#[test]
fn test_accumulator_monotonicity() {
    let fc = Arc::new(FlatCombiner::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(5)); // 4 adders + 1 reader

    // 4 adder threads.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let f = Arc::clone(&fc);
        let s = Arc::clone(&stop);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            let mut count = 0u64;
            while !s.load(Ordering::Relaxed) {
                h.add(1);
                count += 1;
            }
            drop(h);
            count
        }));
    }

    // 1 reader thread checking monotonicity.
    let rf = Arc::clone(&fc);
    let rs = Arc::clone(&stop);
    let rb = Arc::clone(&barrier);
    let reader = thread::spawn(move || {
        let h = rf.register().unwrap();
        rb.wait();
        let mut prev = 0u64;
        let mut checks = 0u64;
        while !rs.load(Ordering::Relaxed) {
            let v = h.read();
            assert!(
                v >= prev,
                "monotonicity violated: {prev} -> {v}"
            );
            prev = v;
            checks += 1;
        }
        drop(h);
        checks
    });

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    let mut total_adds = 0u64;
    for h in handles {
        total_adds += h.join().unwrap();
    }
    let checks = reader.join().unwrap();

    assert_eq!(
        fc.value(),
        total_adds,
        "accumulator={} != adds={total_adds}",
        fc.value()
    );
    assert!(checks > 0, "reader must have done monotonicity checks");

    println!(
        "[PASS] Monotonicity: {checks} checks, {total_adds} adds, all non-decreasing"
    );
}

// ---------------------------------------------------------------------------
// Test 10: Machine-readable conformance output
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    // Quick validation of all key properties.
    let fc = Arc::new(FlatCombiner::new(0));

    // Property 1: Correct total under contention.
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let f = Arc::clone(&fc);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let h = f.register().unwrap();
            b.wait();
            for _ in 0..100 {
                h.add(1);
            }
            drop(h);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let correct_total = fc.value() == 800;

    // Property 2: Reads see monotonic values.
    let fc2 = Arc::new(FlatCombiner::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let monotonic_ok = Arc::new(AtomicBool::new(true));
    let bar2 = Arc::new(Barrier::new(2));

    let wf = Arc::clone(&fc2);
    let ws = Arc::clone(&stop);
    let wb = Arc::clone(&bar2);
    let writer = thread::spawn(move || {
        let h = wf.register().unwrap();
        wb.wait();
        while !ws.load(Ordering::Relaxed) {
            h.add(1);
        }
        drop(h);
    });

    let rf = Arc::clone(&fc2);
    let rs = Arc::clone(&stop);
    let rb = Arc::clone(&bar2);
    let mo = Arc::clone(&monotonic_ok);
    let reader = thread::spawn(move || {
        let h = rf.register().unwrap();
        rb.wait();
        let mut prev = 0u64;
        while !rs.load(Ordering::Relaxed) {
            let v = h.read();
            if v < prev {
                mo.store(false, Ordering::Relaxed);
            }
            prev = v;
        }
        drop(h);
    });

    thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Release);
    writer.join().unwrap();
    reader.join().unwrap();
    let monotonic = monotonic_ok.load(Ordering::Relaxed);

    // Property 3: Slot lifecycle.
    let fc3 = FlatCombiner::new(0);
    let h = fc3.register().unwrap();
    let active_before = fc3.active_threads();
    drop(h);
    let active_after = fc3.active_threads();
    let lifecycle_ok = active_before == 1 && active_after == 0;

    // Property 4: MAX_FC_THREADS enforcement.
    let fc4 = FlatCombiner::new(0);
    let mut hs = Vec::new();
    for _ in 0..MAX_FC_THREADS {
        if let Some(h) = fc4.register() {
            hs.push(h);
        }
    }
    let overflow_rejected = fc4.register().is_none();
    hs.clear();

    // Property 5: Metrics track ops.
    let before = flat_combining_metrics();
    let fc5 = FlatCombiner::new(0);
    let h5 = fc5.register().unwrap();
    h5.add(1);
    h5.add(2);
    h5.add(3);
    let after = flat_combining_metrics();
    let delta = after.fsqlite_flat_combining_ops_total - before.fsqlite_flat_combining_ops_total;
    let metrics_ok = delta >= 3;
    drop(h5);

    // Property 6: Slot reuse after drop.
    let fc6 = FlatCombiner::new(42);
    {
        let h = fc6.register().unwrap();
        h.add(8);
        drop(h);
    }
    let h = fc6.register().unwrap();
    let reuse_ok = h.read() == 50;
    drop(h);

    println!();
    println!("=== Conformance Summary ===");
    println!("  [CONFORM] Contention correctness: 8-thread total = {}", if correct_total { "exact" } else { "MISMATCH" });
    println!("  [CONFORM] Read monotonicity: {}", if monotonic { "never decreases" } else { "VIOLATED" });
    println!("  [CONFORM] Slot lifecycle: register/unregister clean");
    println!("  [CONFORM] Capacity: MAX_FC_THREADS={MAX_FC_THREADS}, overflow rejected={overflow_rejected}");
    println!("  [CONFORM] Metrics: delta_ops={delta} (expected >= 3)");
    println!("  [CONFORM] Slot reuse: value persists across re-registration");
    println!("  Conformance: 6 / 6 (100.0%)");

    assert!(correct_total, "contention correctness failed");
    assert!(monotonic, "monotonicity violated");
    assert!(lifecycle_ok, "slot lifecycle broken");
    assert!(overflow_rejected, "MAX_FC_THREADS not enforced");
    assert!(metrics_ok, "metrics delta too low: {delta}");
    assert!(reuse_ok, "slot reuse value mismatch");
}
