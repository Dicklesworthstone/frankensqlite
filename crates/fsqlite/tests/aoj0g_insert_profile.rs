//! bd-aoj0g: measurement harness for the :memory: secondary-index INSERT
//! cost curve. Not a pass/fail keeper — run explicitly with `--ignored` to
//! print ns/insert per 1000-row batch at rising table depth for three
//! shapes:
//!   A) no secondary index, k = i % 100      (table-only baseline)
//!   B) index on k,        k = i             (sequential/monotonic keys)
//!   C) index on k,        k = i % 100       (the bd-aoj0g O(n) ramp shape)
//! July 2026 receipts (BlackThrush): A ~5us flat, B flat, C ramps with
//! table size => cache locality of scattered index-leaf writes.

use fsqlite::Connection;
use std::time::Instant;

const ROWS: usize = 10_000;
const BATCH: usize = 1_000;

async fn run_case(name: &str, with_index: bool, key_mod: Option<usize>) {
    let conn = Connection::open(":memory:").await.unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);")
        .await
        .unwrap();
    if with_index {
        conn.execute("CREATE INDEX idx_t_k ON t(k);").await.unwrap();
    }
    conn.execute("BEGIN;").await.unwrap();
    let mut batch_times = Vec::new();
    for batch_start in (0..ROWS).step_by(BATCH) {
        let start = Instant::now();
        for i in batch_start..batch_start + BATCH {
            let k = key_mod.map_or(i, |m| i % m);
            conn.execute(&format!(
                "INSERT INTO t (id, k, v) VALUES ({}, {}, 'payload-{}');",
                i + 1,
                k,
                i
            ))
            .await
            .unwrap();
        }
        let ns_per_insert = start.elapsed().as_nanos() / BATCH as u128;
        batch_times.push(ns_per_insert);
    }
    conn.execute("COMMIT;").await.unwrap();
    let first = batch_times.first().copied().unwrap_or(0);
    let last = batch_times.last().copied().unwrap_or(0);
    let ramp = if first > 0 {
        format!("{:.2}x", last as f64 / first as f64)
    } else {
        "n/a".to_owned()
    };
    println!(
        "[aoj0g] {name}: per-batch ns/insert = {batch_times:?} | first {first} last {last} ramp {ramp}"
    );
}

#[test]
#[ignore = "bd-aoj0g measurement harness, run explicitly with --ignored; prints timings"]
fn aoj0g_insert_phase_profile() {
    asupersync::test_utils::run_test(|| async {
        run_case("A no-index    k=i%100", false, Some(100)).await;
        run_case("B with-index  k=i     ", true, None).await;
        run_case("C with-index  k=i%100", true, Some(100)).await;
        // bd-aoj0g duplicate-run hypothesis probes: if the seek walks the
        // duplicate run linearly, D (one key, all rows duplicates) should
        // ramp hardest and E (10 dups/key at 10k rows) mildly — while a pure
        // cache-locality cause would rank C >= E > D (D touches one hot
        // subtree with perfect locality).
        run_case("D with-index  k=1     ", true, Some(1)).await;
        run_case("E with-index  k=i%1000", true, Some(1000)).await;
    });
}
