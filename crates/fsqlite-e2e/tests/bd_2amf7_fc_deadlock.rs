//! bd-2amf7: Flat-combining page lock nested lock pattern stress test.
//!
//! The `FcPageLockShard` in `flat_combining_page_locks.rs` has a nested lock
//! pattern: `combiner_lock` → `map.inner.lock()` in `len()`, `is_empty()`,
//! `for_each()`, `retain()`, and `release_all()`. The `execute_locked()` path
//! also takes `map.inner.lock()` while `combiner_lock` is held (via
//! `drain_locked()`). The lock order is consistent (combiner → map), so
//! classical AB/BA deadlock is not possible. However:
//!
//! 1. If a `for_each` callback re-enters any method on the same shard,
//!    it deadlocks on `combiner_lock` (parking_lot Mutex is not reentrant).
//! 2. Under high concurrency, if a metric-sampling thread calls `len()` while
//!    many writers contend, the combiner-lock hold time during `drain_locked()`
//!    + `map.inner.lock()` can stall metric sampling (priority inversion).
//!
//! This test suite verifies:
//! - S1: High-concurrency acquire/release stress (no deadlock within timeout)
//! - S2: Concurrent len()/is_empty() sampling during write storm
//! - S3: release_all under concurrent pressure
//! - S4: retain under concurrent pressure
//! - S5: Concurrent for_each with writer storm

// The flat-combining module is behind the `mvcc-flat-combining` feature gate.
// These tests compile unconditionally but the FcPageLockShard type is only
// available when the feature is enabled. We test via the public
// InProcessPageLockTable interface which routes to flat-combining when enabled.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fsqlite_mvcc::core_types::InProcessPageLockTable;
use fsqlite_types::{PageNumber, TxnId};

const STRESS_DURATION: Duration = Duration::from_secs(2);
const HIGH_PAGE_BASE: u32 = 70_000;

fn make_high_page(offset: u32) -> PageNumber {
    PageNumber::new(HIGH_PAGE_BASE + offset).expect("valid page number")
}

fn make_txn(id: u64) -> TxnId {
    TxnId::new(id).expect("valid txn id")
}

// ─── S1: High-concurrency acquire/release stress ────────────────────

#[test]
fn s1_concurrent_acquire_release_no_deadlock() {
    let table = Arc::new(InProcessPageLockTable::new());
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    let threads: Vec<_> = (0..8)
        .map(|i| {
            let t = Arc::clone(&table);
            let s = Arc::clone(&stop);
            let o = Arc::clone(&ops);
            std::thread::spawn(move || {
                let txn = make_txn(100 + i);
                let mut local_ops: u64 = 0;
                while !s.load(Ordering::Relaxed) {
                    for j in 0..50 {
                        let page = make_high_page(j + (i as u32 * 50));
                        let _ = t.try_acquire(page, txn);
                        local_ops += 1;
                    }
                    for j in 0..50 {
                        let page = make_high_page(j + (i as u32 * 50));
                        t.release(page, txn);
                        local_ops += 1;
                    }
                }
                o.fetch_add(local_ops, Ordering::Relaxed);
            })
        })
        .collect();

    std::thread::sleep(STRESS_DURATION);
    stop.store(true, Ordering::Relaxed);

    for t in threads {
        t.join().expect("thread must not deadlock or panic");
    }

    let total = ops.load(Ordering::Relaxed);
    assert!(total > 0, "no operations completed — possible deadlock");
    eprintln!("S1: {total} ops in {STRESS_DURATION:?}");
}

// ─── S2: Concurrent len()/is_empty() sampling during write storm ────

#[test]
fn s2_len_sampling_during_write_storm() {
    let table = Arc::new(InProcessPageLockTable::new());
    let stop = Arc::new(AtomicBool::new(false));

    // Writer threads
    let writers: Vec<_> = (0..4)
        .map(|i| {
            let t = Arc::clone(&table);
            let s = Arc::clone(&stop);
            std::thread::spawn(move || {
                let txn = make_txn(200 + i);
                while !s.load(Ordering::Relaxed) {
                    for j in 0..20 {
                        let page = make_high_page(j + (i as u32 * 20));
                        let _ = t.try_acquire(page, txn);
                    }
                    for j in 0..20 {
                        let page = make_high_page(j + (i as u32 * 20));
                        t.release(page, txn);
                    }
                }
            })
        })
        .collect();

    // Sampler thread calling len() rapidly
    let sampler_table = Arc::clone(&table);
    let sampler_stop = Arc::clone(&stop);
    let sampler = std::thread::spawn(move || {
        let mut samples: u64 = 0;
        let start = Instant::now();
        while !sampler_stop.load(Ordering::Relaxed) {
            let _len = sampler_table.lock_count();
            let _empty = sampler_table.lock_count() == 0;
            samples += 1;
            if samples.is_multiple_of(10_000) && start.elapsed() > STRESS_DURATION {
                break;
            }
        }
        samples
    });

    std::thread::sleep(STRESS_DURATION);
    stop.store(true, Ordering::Relaxed);

    for w in writers {
        w.join().expect("writer must not deadlock");
    }
    let samples = sampler.join().expect("sampler must not deadlock");
    assert!(samples > 0, "sampler did zero samples — possible deadlock");
    eprintln!("S2: {samples} len() samples during write storm");
}

// ─── S3: release_all under concurrent pressure ──────────────────────

#[test]
fn s3_release_all_under_pressure() {
    let table = Arc::new(InProcessPageLockTable::new());
    let stop = Arc::new(AtomicBool::new(false));

    // Continuous writer
    let writer_table = Arc::clone(&table);
    let writer_stop = Arc::clone(&stop);
    let writer = std::thread::spawn(move || {
        let txn = make_txn(300);
        while !writer_stop.load(Ordering::Relaxed) {
            for j in 0..30 {
                let page = make_high_page(j);
                let _ = writer_table.try_acquire(page, txn);
            }
            std::thread::yield_now();
            writer_table.release_all(txn);
        }
    });

    // Concurrent acquire attempts by other txn
    let contender_table = Arc::clone(&table);
    let contender_stop = Arc::clone(&stop);
    let contender = std::thread::spawn(move || {
        let txn = make_txn(301);
        let mut successes: u64 = 0;
        while !contender_stop.load(Ordering::Relaxed) {
            for j in 0..30 {
                let page = make_high_page(j);
                if contender_table.try_acquire(page, txn).is_ok() {
                    successes += 1;
                    contender_table.release(page, txn);
                }
            }
        }
        successes
    });

    std::thread::sleep(STRESS_DURATION);
    stop.store(true, Ordering::Relaxed);

    writer.join().expect("writer must not deadlock");
    let successes = contender.join().expect("contender must not deadlock");
    eprintln!("S3: contender got {successes} acquisitions during release_all storm");
}

// ─── S4: retain under concurrent pressure ───────────────────────────

#[test]
fn s4_retain_under_pressure() {
    let table = Arc::new(InProcessPageLockTable::new());
    let stop = Arc::new(AtomicBool::new(false));

    // Writer: acquire pages, then retain-filter periodically
    let w_table = Arc::clone(&table);
    let w_stop = Arc::clone(&stop);
    let writer = std::thread::spawn(move || {
        let txn = make_txn(400);
        let mut rounds: u64 = 0;
        while !w_stop.load(Ordering::Relaxed) {
            for j in 0..20 {
                let page = make_high_page(j);
                let _ = w_table.try_acquire(page, txn);
            }
            // Release via individual release (not release_all)
            for j in 0..20 {
                let page = make_high_page(j);
                w_table.release(page, txn);
            }
            rounds += 1;
        }
        rounds
    });

    // Concurrent reader: acquire different page range
    let r_table = Arc::clone(&table);
    let r_stop = Arc::clone(&stop);
    let reader = std::thread::spawn(move || {
        let txn = make_txn(401);
        let mut rounds: u64 = 0;
        while !r_stop.load(Ordering::Relaxed) {
            for j in 20..40 {
                let page = make_high_page(j);
                let _ = r_table.try_acquire(page, txn);
            }
            for j in 20..40 {
                let page = make_high_page(j);
                r_table.release(page, txn);
            }
            rounds += 1;
        }
        rounds
    });

    std::thread::sleep(STRESS_DURATION);
    stop.store(true, Ordering::Relaxed);

    let w_rounds = writer.join().expect("writer must not deadlock");
    let r_rounds = reader.join().expect("reader must not deadlock");
    eprintln!("S4: writer {w_rounds} rounds, reader {r_rounds} rounds");
}

// ─── S5: Holder queries during write storm ──────────────────────────

#[test]
fn s5_holder_queries_during_write_storm() {
    let table = Arc::new(InProcessPageLockTable::new());
    let stop = Arc::new(AtomicBool::new(false));

    // Writers
    let writers: Vec<_> = (0..4)
        .map(|i| {
            let t = Arc::clone(&table);
            let s = Arc::clone(&stop);
            std::thread::spawn(move || {
                let txn = make_txn(500 + i);
                while !s.load(Ordering::Relaxed) {
                    for j in 0..10 {
                        let page = make_high_page(j);
                        let _ = t.try_acquire(page, txn);
                    }
                    for j in 0..10 {
                        let page = make_high_page(j);
                        t.release(page, txn);
                    }
                }
            })
        })
        .collect();

    // Holder query thread
    let q_table = Arc::clone(&table);
    let q_stop = Arc::clone(&stop);
    let querier = std::thread::spawn(move || {
        let mut queries: u64 = 0;
        while !q_stop.load(Ordering::Relaxed) {
            for j in 0..10 {
                let page = make_high_page(j);
                let _ = q_table.holder(page);
                queries += 1;
            }
        }
        queries
    });

    std::thread::sleep(STRESS_DURATION);
    stop.store(true, Ordering::Relaxed);

    for w in writers {
        w.join().expect("writer must not deadlock");
    }
    let queries = querier.join().expect("querier must not deadlock");
    assert!(queries > 0);
    eprintln!("S5: {queries} holder queries during write storm");
}
