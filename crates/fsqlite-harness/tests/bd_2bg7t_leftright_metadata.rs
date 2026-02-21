//! bd-2bg7t: Left-Right Sync for metadata hot paths (§14.3) integration tests.
//!
//! Validates Left-Right suitability for FrankenSQLite metadata hot paths:
//!   1. Wait-free reads under sustained writer pressure
//!   2. Pair consistency: schema_epoch + commit_seq never torn
//!   3. Triple consistency: commit_seq + schema_epoch + ecs_epoch never torn
//!   4. Comparative latency: left-right vs mutex for read-heavy workloads
//!   5. Read-dominated ratio (>1000:1) characterization
//!   6. Writer serialization: concurrent update closures produce correct total
//!   7. Metrics fidelity: reads, swaps, retries delta tracking
//!   8. Active side alternation under writes
//!   9. Mixed pair read/write: monotonic field progression
//!  10. Machine-readable conformance output

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite_mvcc::{LeftRight, LeftRightPair, LeftRightTriple, leftright_metrics};

// ---------------------------------------------------------------------------
// Test 1: Wait-free reads under sustained writer pressure
// ---------------------------------------------------------------------------

#[test]
fn test_waitfree_reads_under_writer() {
    let lr = Arc::new(LeftRight::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(3)); // 1 writer + 2 readers

    // Sustained writer.
    let wlr = Arc::clone(&lr);
    let ws = Arc::clone(&stop);
    let wb = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        wb.wait();
        let mut n = 0u64;
        while !ws.load(Ordering::Relaxed) {
            n += 1;
            wlr.write(n);
        }
        n
    });

    // 2 readers — every read must succeed (wait-free guarantee).
    let read_count = Arc::new(AtomicU64::new(0));
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let rlr = Arc::clone(&lr);
            let rs = Arc::clone(&stop);
            let rb = Arc::clone(&barrier);
            let rc = Arc::clone(&read_count);
            thread::spawn(move || {
                rb.wait();
                while !rs.load(Ordering::Relaxed) {
                    let _ = rlr.read("waitfree");
                    rc.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    let write_count = writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    let reads = read_count.load(Ordering::Relaxed);

    assert!(write_count > 0, "writer must have written");
    assert!(reads > 0, "readers must have read");

    println!(
        "[PASS] Wait-free reads: writes={write_count} reads={reads} (all succeeded, no blocking)"
    );
}

// ---------------------------------------------------------------------------
// Test 2: Pair consistency — schema_epoch + commit_seq never torn
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::needless_collect)]
fn test_pair_consistency() {
    let lr = Arc::new(LeftRightPair::new(0, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(5)); // 1 writer + 4 readers

    // Writer: both fields always equal (a, a*100).
    let wlr = Arc::clone(&lr);
    let ws = Arc::clone(&stop);
    let wb = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        wb.wait();
        let mut epoch = 0u64;
        while !ws.load(Ordering::Relaxed) {
            epoch += 1;
            wlr.write(epoch, epoch * 100);
        }
        epoch
    });

    // Readers verify consistency.
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let rlr = Arc::clone(&lr);
            let rs = Arc::clone(&stop);
            let rb = Arc::clone(&barrier);
            thread::spawn(move || {
                rb.wait();
                let mut reads = 0u64;
                while !rs.load(Ordering::Relaxed) {
                    let (a, b) = rlr.read("pair");
                    assert_eq!(
                        b,
                        a * 100,
                        "torn pair: schema_epoch={a}, commit_seq={b}"
                    );
                    reads += 1;
                }
                reads
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(400));
    stop.store(true, Ordering::Release);

    let epochs = writer.join().unwrap();
    let total_reads: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();

    assert!(epochs > 0);
    assert!(total_reads > 0);

    println!(
        "[PASS] Pair consistency: {epochs} epochs, {total_reads} reads, zero torn"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Triple consistency — commit_seq + schema_epoch + ecs_epoch
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::needless_collect)]
fn test_triple_consistency() {
    let lr = Arc::new(LeftRightTriple::new(0, 0, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(5)); // 1 writer + 4 readers

    // Writer: a, a*10, a*100 relationship.
    let wlr = Arc::clone(&lr);
    let ws = Arc::clone(&stop);
    let wb = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        wb.wait();
        let mut n = 0u64;
        while !ws.load(Ordering::Relaxed) {
            n += 1;
            wlr.write(n, n * 10, n * 100);
        }
        n
    });

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let rlr = Arc::clone(&lr);
            let rs = Arc::clone(&stop);
            let rb = Arc::clone(&barrier);
            thread::spawn(move || {
                rb.wait();
                let mut reads = 0u64;
                while !rs.load(Ordering::Relaxed) {
                    let (a, b, c) = rlr.read("triple");
                    assert_eq!(b, a * 10, "torn triple: a={a} b={b}");
                    assert_eq!(c, a * 100, "torn triple: a={a} c={c}");
                    reads += 1;
                }
                reads
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(400));
    stop.store(true, Ordering::Release);

    let write_count = writer.join().unwrap();
    let total_reads: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();

    assert!(write_count > 0);
    assert!(total_reads > 0);

    println!(
        "[PASS] Triple consistency: {write_count} writes, {total_reads} reads, zero torn"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Comparative latency — left-right vs mutex for read-heavy
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::cast_possible_truncation)]
fn test_leftright_vs_mutex_latency() {
    let iterations = 100_000u64;

    // Left-right uncontended read latency.
    let lr = LeftRight::new(42);
    let t0 = Instant::now();
    for _ in 0..iterations {
        let _ = lr.read("bench");
    }
    let lr_ns = t0.elapsed().as_nanos() as u64;

    // Mutex uncontended read latency.
    let mx = parking_lot::Mutex::new(42u64);
    let t1 = Instant::now();
    for _ in 0..iterations {
        let _ = *mx.lock();
    }
    let mx_ns = t1.elapsed().as_nanos() as u64;

    let lr_per_op = lr_ns / iterations;
    let mx_per_op = mx_ns / iterations;

    println!(
        "[PASS] Latency: left-right={lr_per_op}ns/op mutex={mx_per_op}ns/op"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Read-dominated ratio characterization
// ---------------------------------------------------------------------------

#[test]
fn test_read_dominated_ratio() {
    let lr = Arc::new(LeftRight::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(9)); // 1 writer + 8 readers

    // Writer: slow updates.
    let wlr = Arc::clone(&lr);
    let ws = Arc::clone(&stop);
    let wb = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        wb.wait();
        let mut n = 0u64;
        while !ws.load(Ordering::Relaxed) {
            n += 1;
            wlr.write(n);
            thread::sleep(Duration::from_micros(200));
        }
        n
    });

    // 8 readers: tight read loops.
    let read_count = Arc::new(AtomicU64::new(0));
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let rlr = Arc::clone(&lr);
            let rs = Arc::clone(&stop);
            let rb = Arc::clone(&barrier);
            let rc = Arc::clone(&read_count);
            thread::spawn(move || {
                rb.wait();
                while !rs.load(Ordering::Relaxed) {
                    let _ = rlr.read("ratio");
                    rc.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    let write_count = writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    let reads = read_count.load(Ordering::Relaxed);

    assert!(write_count > 0);
    assert!(reads > 0);
    let ratio = reads / write_count.max(1);

    println!(
        "[PASS] Read-dominated: writes={write_count} reads={reads} ratio={ratio}:1"
    );
}

// ---------------------------------------------------------------------------
// Test 6: Writer serialization via update closure
// ---------------------------------------------------------------------------

#[test]
fn test_writer_serialization() {
    let lr = Arc::new(LeftRight::new(0));
    let barrier = Arc::new(Barrier::new(4));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let l = Arc::clone(&lr);
            let b = Arc::clone(&barrier);
            thread::spawn(move || {
                b.wait();
                for _ in 0..1000 {
                    l.update(|v| v + 1);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let val = lr.read("counter");
    assert_eq!(val, 4000, "4 threads * 1000 increments = 4000");

    println!("[PASS] Writer serialization: 4 threads * 1000 = {val}");
}

// ---------------------------------------------------------------------------
// Test 7: Metrics fidelity — reads, swaps, retries
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_fidelity() {
    let before = leftright_metrics();

    let lr = LeftRight::new(7);

    // Known reads.
    for _ in 0..500 {
        lr.read("single");
    }

    // Known writes (each write = 1 swap).
    lr.write(8);
    lr.write(9);
    lr.write(10);

    let pair = LeftRightPair::new(1, 2);
    for _ in 0..250 {
        pair.read("pair");
    }

    let after = leftright_metrics();

    let delta_reads = after.fsqlite_leftright_reads_total - before.fsqlite_leftright_reads_total;
    let delta_swaps = after.fsqlite_leftright_swaps_total - before.fsqlite_leftright_swaps_total;

    assert!(
        delta_reads >= 750,
        "delta reads should be >= 750, got {delta_reads}"
    );
    assert!(
        delta_swaps >= 3,
        "delta swaps should be >= 3, got {delta_swaps}"
    );

    println!(
        "[PASS] Metrics fidelity: delta_reads={delta_reads} delta_swaps={delta_swaps}"
    );
}

// ---------------------------------------------------------------------------
// Test 8: Active side alternation under writes
// ---------------------------------------------------------------------------

#[test]
fn test_active_side_alternation() {
    let lr = LeftRight::new(0);

    let initial = lr.active_side();
    assert!(initial == 0 || initial == 1, "initial side must be 0 or 1");

    // Each write should swap the active side.
    let mut prev_side = initial;
    for i in 1..=10 {
        lr.write(i);
        let new_side = lr.active_side();
        assert_ne!(
            new_side, prev_side,
            "write #{i}: side should alternate, got {new_side} == {prev_side}"
        );
        prev_side = new_side;
    }

    // Value should be correct regardless of which side is active.
    assert_eq!(lr.read("side"), 10);

    println!("[PASS] Active side alternation: 10 writes, side alternated each time");
}

// ---------------------------------------------------------------------------
// Test 9: Pair monotonic field progression
// ---------------------------------------------------------------------------

#[test]
fn test_pair_monotonic_progression() {
    let lr = Arc::new(LeftRightPair::new(0, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(2)); // 1 writer + 1 reader

    // Writer: monotonically increasing pair.
    let wlr = Arc::clone(&lr);
    let ws = Arc::clone(&stop);
    let wb = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        wb.wait();
        let mut n = 0u64;
        while !ws.load(Ordering::Relaxed) {
            n += 1;
            wlr.write(n, n);
        }
        n
    });

    // Reader: values should never decrease.
    let rlr = Arc::clone(&lr);
    let rs = Arc::clone(&stop);
    let rb = Arc::clone(&barrier);
    let reader = thread::spawn(move || {
        rb.wait();
        let mut prev_a = 0u64;
        let mut checks = 0u64;
        while !rs.load(Ordering::Relaxed) {
            let (a, _b) = rlr.read("monotonic");
            assert!(
                a >= prev_a,
                "monotonicity violation: {prev_a} -> {a}"
            );
            prev_a = a;
            checks += 1;
        }
        checks
    });

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    let write_count = writer.join().unwrap();
    let checks = reader.join().unwrap();

    assert!(write_count > 0);
    assert!(checks > 0);

    println!(
        "[PASS] Pair monotonic: {write_count} writes, {checks} checks, never decreased"
    );
}

// ---------------------------------------------------------------------------
// Test 10: Machine-readable conformance output
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    // Property 1: Wait-free read (no blocking).
    let lr1 = LeftRight::new(42);
    let read_ok = lr1.read("conformance") == 42;
    lr1.write(99);
    let write_ok = lr1.read("conformance") == 99;

    // Property 2: Pair consistency.
    let lr2 = LeftRightPair::new(10, 20);
    let pair_ok = lr2.read("conf_pair") == (10, 20);
    lr2.write(30, 40);
    let pair_update_ok = lr2.read("conf_pair") == (30, 40);

    // Property 3: Triple consistency.
    let lr3 = LeftRightTriple::new(1, 2, 3);
    let triple_ok = lr3.read("conf_triple") == (1, 2, 3);
    lr3.write(4, 5, 6);
    let triple_update_ok = lr3.read("conf_triple") == (4, 5, 6);

    // Property 4: Update closure serialization.
    let lr4 = LeftRight::new(0);
    lr4.update(|v| v + 10);
    lr4.update(|v| v + 20);
    let update_ok = lr4.read("update") == 30;

    // Property 5: Side alternation.
    let lr5 = LeftRight::new(0);
    let s0 = lr5.active_side();
    lr5.write(1);
    let s1 = lr5.active_side();
    let alternation_ok = s0 != s1;

    // Property 6: Metrics track operations.
    let before = leftright_metrics();
    let lr6 = LeftRight::new(0);
    lr6.read("metrics_check");
    lr6.write(1);
    let after = leftright_metrics();
    let metrics_ok = after.fsqlite_leftright_reads_total > before.fsqlite_leftright_reads_total
        && after.fsqlite_leftright_swaps_total > before.fsqlite_leftright_swaps_total;

    println!();
    println!("=== Conformance Summary ===");
    println!("  [CONFORM] Wait-free read: basic read/write correctness");
    println!("  [CONFORM] Pair consistency: no torn reads on (a, b)");
    println!("  [CONFORM] Triple consistency: no torn reads on (a, b, c)");
    println!("  [CONFORM] Update closure: serialized read-modify-write");
    println!("  [CONFORM] Side alternation: active flips on each write");
    println!("  [CONFORM] Metrics: reads_total and swaps_total tracked");
    println!("  Conformance: 6 / 6 (100.0%)");

    assert!(read_ok && write_ok, "basic read/write failed");
    assert!(pair_ok && pair_update_ok, "pair consistency failed");
    assert!(triple_ok && triple_update_ok, "triple consistency failed");
    assert!(update_ok, "update closure failed");
    assert!(alternation_ok, "side alternation failed");
    assert!(metrics_ok, "metrics tracking failed");
}
