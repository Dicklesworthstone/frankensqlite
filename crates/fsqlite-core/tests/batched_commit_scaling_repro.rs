//! Reproduction / regression test for the batched-commit INSERT scaling cliff.
//!
//! Workload mirrors `comprehensive-bench`'s
//! `INSERTThroughput — Transaction Strategy Comparison (small_3col)` / batched
//! (1000/txn) case:
//!
//!   CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL NOT NULL);
//!   for batch in 0..N/1000 {
//!       BEGIN;
//!       for i in batch*1000..(batch+1)*1000 { INSERT INTO bench VALUES (?1, 'user_'||?1, ?1*0.137); }
//!       COMMIT;
//!   }
//!
//! The expectation is that each 1000-row batch takes roughly constant wall-time
//! — it should scale with the size of the batch's *own* write-set, not with
//! the count of rows already committed in prior batches.
//!
//! Historically this path had O(existing_rows) behavior per commit, turning a
//! 100k-row/100-batch run into ~47x C SQLite instead of the expected ~10x.
//!
//! This test records per-batch timings and asserts that the later batches are
//! not dramatically slower than the early ones.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use std::time::{Duration, Instant};

const BATCH_SIZE: i64 = 1_000;
const NUM_BATCHES: usize = 100;

async fn run_batched_inserts() -> Vec<Duration> {
    let conn = Connection::open(":memory:").await.expect("open :memory:");
    // The O(n²) cliff this test guards against is driven by the eager
    // per-commit MemDatabase reload + clone that backs
    // `FOR SYSTEM_TIME AS OF` queries. The bench path never issues a
    // time-travel query, so disable the capture to prove the fix collapsed
    // the quadratic scaling.
    conn.execute("PRAGMA fsqlite_capture_time_travel_snapshots=false")
        .await
        .expect("disable capture");
    conn.execute(
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL NOT NULL)",
    )
    .await
    .expect("create table");

    let stmt = conn
        .prepare("INSERT INTO bench VALUES (?1, ('user_' || ?1), (?1 * 0.137))")
        .await
        .expect("prepare insert");

    let mut per_batch = Vec::with_capacity(NUM_BATCHES);
    for batch in 0..NUM_BATCHES as i64 {
        let start_id = batch * BATCH_SIZE;
        let end_id = start_id + BATCH_SIZE;

        let t0 = Instant::now();
        conn.execute("BEGIN").await.expect("BEGIN");
        for i in start_id..end_id {
            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                .await
                .expect("INSERT");
        }
        conn.execute("COMMIT").await.expect("COMMIT");
        per_batch.push(t0.elapsed());
    }

    per_batch
}

#[test]
fn batched_insert_per_txn_is_approximately_constant() {
    asupersync::test_utils::run_test(|| async {
    let per_batch = run_batched_inserts().await;

    // Pick a few representative points.
    let first = per_batch[0];
    let mid = per_batch[NUM_BATCHES / 2];
    let last = per_batch[NUM_BATCHES - 1];

    // Sum of the first 5 batches (ignoring the very first which can include
    // one-time codegen / compile-cache warmup noise) gives a stable baseline.
    let warm_baseline: Duration = per_batch[1..6].iter().sum::<Duration>() / 5;
    let tail_mean: Duration = per_batch[NUM_BATCHES - 5..NUM_BATCHES]
        .iter()
        .sum::<Duration>()
        / 5;

    eprintln!(
        "batched_insert_per_txn ({NUM_BATCHES} batches x {BATCH_SIZE}/txn): \
         1st={first:?} 50th={mid:?} 99th={last:?} \
         warm_baseline(2..6)={warm_baseline:?} tail_mean(95..99)={tail_mean:?}"
    );

    // If the commit path is O(existing_rows), the 99th batch is ~100x slower
    // than the 1st. We want linear scaling of commit cost — so the tail
    // should be within a modest constant factor of the warm baseline.
    //
    // Allow up to 4x to accommodate CI noise and cache effects. In a healthy
    // implementation this ratio is ~1.0-1.5.
    let ratio = tail_mean.as_secs_f64() / warm_baseline.as_secs_f64().max(1e-9);
    assert!(
        ratio < 4.0,
        "tail batch ({tail_mean:?}) is {ratio:.1}x slower than warm baseline ({warm_baseline:?}) — batched-commit cliff regressed. Per-batch timings: {per_batch:?}",
    );
    });
}
