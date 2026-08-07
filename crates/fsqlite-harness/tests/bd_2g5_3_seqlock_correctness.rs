//! bd-2g5.3: Seqlock correctness tests — optimistic metadata reads.
//!
//! Validates the seqlock protocol for rarely-changing metadata:
//!   1. Single-value read/write correctness
//!   2. Pair consistency (no torn reads)
//!   3. Triple consistency (no torn reads)
//!   4. Writer serialization (concurrent updates)
//!   5. Metrics and tracing integration
//!   6. ThreadSanitizer-clean stress test
//!   7. Machine-readable conformance output

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use fsqlite_mvcc::{SeqLock, SeqLockPair, SeqLockTriple, seqlock_metrics};

// ---------------------------------------------------------------------------
// Test 1: Single-value read/write
// ---------------------------------------------------------------------------

#[test]
fn test_single_value_read_write() {
    let sl = SeqLock::new(0);
    assert_eq!(sl.read("init"), Some(0));

    for i in 1..=100 {
        sl.write(i);
        assert_eq!(sl.read("iter"), Some(i));
    }

    assert_eq!(sl.sequence(), 200); // 100 writes * 2 increments each

    sl.update(|v| v + 1);
    assert_eq!(sl.read("after_update"), Some(101));

    println!("[PASS] single-value read/write correctness");
}

// ---------------------------------------------------------------------------
// Test 2: Pair no-torn-read stress
// ---------------------------------------------------------------------------

#[test]
fn test_pair_no_torn_reads() {
    let sl = Arc::new(SeqLockPair::new(0, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(4)); // 1 writer + 3 readers

    let writer_sl = Arc::clone(&sl);
    let writer_stop = Arc::clone(&stop);
    let writer_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        writer_barrier.wait();
        let mut val = 0u64;
        while !writer_stop.load(Ordering::Relaxed) {
            val += 1;
            // Invariant: a == b
            writer_sl.write(val, val);
        }
        val
    });

    let mut readers = Vec::new();
    for _ in 0..3 {
        let rsl = Arc::clone(&sl);
        let rs = Arc::clone(&stop);
        let rb = Arc::clone(&barrier);
        readers.push(thread::spawn(move || {
            rb.wait();
            let mut reads = 0u64;
            let mut max_val = 0u64;
            while !rs.load(Ordering::Relaxed) {
                if let Some((a, b)) = rsl.read("pair_stress") {
                    assert_eq!(a, b, "TORN READ: a={a} b={b}");
                    reads += 1;
                    max_val = max_val.max(a);
                }
            }
            (reads, max_val)
        }));
    }

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    let writes = writer.join().unwrap();
    let mut total_reads = 0u64;
    let mut max_observed = 0u64;
    for r in readers {
        let (reads, max_val) = r.join().unwrap();
        total_reads += reads;
        max_observed = max_observed.max(max_val);
    }

    assert!(writes > 0, "writer must complete some writes");
    assert!(total_reads > 0, "readers must complete some reads");

    println!(
        "[PASS] pair no-torn-reads: writes={writes} reads={total_reads} max_observed={max_observed}"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Triple no-torn-read stress
// ---------------------------------------------------------------------------

#[test]
fn test_triple_no_torn_reads() {
    let sl = Arc::new(SeqLockTriple::new(0, 0, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(4));

    let writer_sl = Arc::clone(&sl);
    let writer_stop = Arc::clone(&stop);
    let writer_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        writer_barrier.wait();
        let mut val = 0u64;
        while !writer_stop.load(Ordering::Relaxed) {
            val += 1;
            // Invariant: a == b == c
            writer_sl.write(val, val, val);
        }
        val
    });

    let mut readers = Vec::new();
    for _ in 0..3 {
        let rsl = Arc::clone(&sl);
        let rs = Arc::clone(&stop);
        let rb = Arc::clone(&barrier);
        readers.push(thread::spawn(move || {
            rb.wait();
            let mut reads = 0u64;
            while !rs.load(Ordering::Relaxed) {
                if let Some((a, b, c)) = rsl.read("triple_stress") {
                    assert!(a == b && b == c, "TORN READ: a={a} b={b} c={c}");
                    reads += 1;
                }
            }
            reads
        }));
    }

    thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Release);

    let writes = writer.join().unwrap();
    let mut total_reads = 0u64;
    for r in readers {
        total_reads += r.join().unwrap();
    }

    assert!(writes > 0);
    assert!(total_reads > 0);

    println!("[PASS] triple no-torn-reads: writes={writes} reads={total_reads}");
}

// ---------------------------------------------------------------------------
// Test 4: Writer serialization
// ---------------------------------------------------------------------------

#[test]
fn test_writer_serialization() {
    let sl = Arc::new(SeqLock::new(0));
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let s = Arc::clone(&sl);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for _ in 0..500 {
                s.update(|v| v + 1);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let final_val = sl.read("final").unwrap();
    assert_eq!(final_val, 2000, "4 writers * 500 updates = 2000");

    let seq = sl.sequence();
    assert_eq!(seq % 2, 0, "sequence must be even after writes: {seq}");
    assert_eq!(seq, 4000, "2000 updates * 2 increments each");

    println!("[PASS] writer serialization: final={final_val} seq={seq}");
}

// ---------------------------------------------------------------------------
// Test 5: Metrics integration
// ---------------------------------------------------------------------------

#[test]
fn test_metrics_integration() {
    let before = seqlock_metrics();

    let sl = SeqLock::new(42);
    for _ in 0..10 {
        sl.read("metrics_test");
    }

    let after = seqlock_metrics();
    let delta_reads = after.fsqlite_seqlock_reads_total - before.fsqlite_seqlock_reads_total;
    assert!(
        delta_reads >= 10,
        "expected at least 10 reads recorded, got delta={delta_reads}"
    );

    // Verify serialization works.
    let json = serde_json::to_string(&after).unwrap();
    assert!(json.contains("fsqlite_seqlock_reads_total"));
    assert!(json.contains("fsqlite_seqlock_retries_total"));

    println!(
        "[PASS] metrics: reads_delta={delta_reads} retries={}",
        after.fsqlite_seqlock_retries_total
    );
}

// ---------------------------------------------------------------------------
// Test 6: Stress test (reader+writer contention)
// ---------------------------------------------------------------------------

#[test]
fn test_stress_concurrent_rw() {
    let sl = Arc::new(SeqLock::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let global_reads = Arc::new(AtomicU64::new(0));
    let global_writes = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(6)); // 2 writers + 4 readers

    // 2 writer threads
    let mut handles = Vec::new();
    for _ in 0..2 {
        let s = Arc::clone(&sl);
        let st = Arc::clone(&stop);
        let gw = Arc::clone(&global_writes);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            let mut local_writes = 0u64;
            while !st.load(Ordering::Relaxed) {
                s.update(|v| v.wrapping_add(1));
                local_writes += 1;
            }
            gw.fetch_add(local_writes, Ordering::Relaxed);
        }));
    }

    // 4 reader threads
    for _ in 0..4 {
        let s = Arc::clone(&sl);
        let st = Arc::clone(&stop);
        let gr = Arc::clone(&global_reads);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            let mut local_reads = 0u64;
            while !st.load(Ordering::Relaxed) {
                // Just read — value monotonicity isn't guaranteed because
                // we have concurrent writers, but reads must succeed.
                let _ = s.read("stress");
                local_reads += 1;
            }
            gr.fetch_add(local_reads, Ordering::Relaxed);
        }));
    }

    thread::sleep(Duration::from_millis(500));
    stop.store(true, Ordering::Release);

    for h in handles {
        h.join().unwrap();
    }

    let writes = global_writes.load(Ordering::Relaxed);
    let reads = global_reads.load(Ordering::Relaxed);
    let seq = sl.sequence();

    assert!(writes > 0);
    assert!(reads > 0);
    assert_eq!(seq % 2, 0, "sequence must be even: {seq}");
    assert_eq!(
        seq,
        writes * 2,
        "seq={seq} but expected writes*2={}",
        writes * 2
    );

    println!("[PASS] stress: writes={writes} reads={reads} seq={seq}");
}

// ---------------------------------------------------------------------------
// Test 7: Conformance summary (JSON)
// ---------------------------------------------------------------------------

#[test]
fn test_conformance_summary() {
    struct TestResult {
        name: &'static str,
        pass: bool,
        detail: String,
    }

    let mut results = Vec::new();

    // 1. Single value correctness
    {
        let sl = SeqLock::new(0);
        sl.write(42);
        let pass = sl.read("c1") == Some(42);
        results.push(TestResult {
            name: "single_value_correctness",
            pass,
            detail: format!("read={:?}", sl.read("c1")),
        });
    }

    // 2. Pair consistency
    {
        let sl = SeqLockPair::new(0, 0);
        sl.write(7, 7);
        let (a, b) = sl.read("c2").unwrap_or((0, 0));
        let pass = a == 7 && b == 7;
        results.push(TestResult {
            name: "pair_consistency",
            pass,
            detail: format!("a={a} b={b}"),
        });
    }

    // 3. Triple consistency
    {
        let sl = SeqLockTriple::new(0, 0, 0);
        sl.write(5, 5, 5);
        let (a, b, c) = sl.read("c3").unwrap_or((0, 0, 0));
        let pass = a == 5 && b == 5 && c == 5;
        results.push(TestResult {
            name: "triple_consistency",
            pass,
            detail: format!("a={a} b={b} c={c}"),
        });
    }

    // 4. Sequence counter parity
    {
        let sl = SeqLock::new(0);
        for i in 0..10 {
            sl.write(i);
        }
        let seq = sl.sequence();
        let pass = seq == 20 && seq.is_multiple_of(2);
        results.push(TestResult {
            name: "sequence_parity",
            pass,
            detail: format!("seq={seq}"),
        });
    }

    // 5. Metrics increment
    {
        let before = seqlock_metrics();
        let sl = SeqLock::new(0);
        for _ in 0..5 {
            sl.read("c5");
        }
        let after = seqlock_metrics();
        let delta = after.fsqlite_seqlock_reads_total - before.fsqlite_seqlock_reads_total;
        let pass = delta >= 5;
        results.push(TestResult {
            name: "metrics_increment",
            pass,
            detail: format!("reads_delta={delta}"),
        });
    }

    // 6. No torn reads under contention (quick)
    {
        let sl = Arc::new(SeqLockPair::new(0, 0));
        let stop = Arc::new(AtomicBool::new(false));
        let torn = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(2));

        let ws = Arc::clone(&sl);
        let wst = Arc::clone(&stop);
        let wb = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            wb.wait();
            let mut v = 0u64;
            while !wst.load(Ordering::Relaxed) {
                v += 1;
                ws.write(v, v);
            }
        });

        let rs = Arc::clone(&sl);
        let rst = Arc::clone(&stop);
        let rt = Arc::clone(&torn);
        let rb = Arc::clone(&barrier);
        let reader = thread::spawn(move || {
            rb.wait();
            while !rst.load(Ordering::Relaxed) {
                if let Some((a, b)) = rs.read("c6")
                    && a != b
                {
                    rt.store(true, Ordering::Relaxed);
                }
            }
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

    println!("\n=== bd-2g5.3: Seqlock Conformance Summary ===");
    println!("{{");
    println!("  \"bead\": \"bd-2g5.3\",");
    println!("  \"suite\": \"seqlock_correctness\",");
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
        "{failed}/{total} seqlock conformance tests failed"
    );

    println!("[PASS] all {total} seqlock conformance tests passed");
}
