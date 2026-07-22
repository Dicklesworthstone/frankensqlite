//! bd-5310l: profile `SELECT DISTINCT k` / `GROUP BY k` on an indexed column with FEW distinct values
//! (50 distinct across 20k rows). Both frank and C SQLite return these in ascending (index) order
//! (feasibility probe confirmed), so streaming distinct values off the pre-sorted index is byte-exact.
//! Question: is frank's current path a full scan + sorter/hash (O(n log n)), so an index stream (or a
//! loose/skip scan) would win? If DISTINCT(indexed) ~= DISTINCT(not indexed), the index is unused.
//!
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test distinct_groupby_profile -- --ignored --nocapture

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::time::Instant;

use fsqlite::Connection;

fn measure(conn: &Connection, sql: &str, n: u64) -> f64 {
    for _ in 0..20 {
        let _ = conn.query(sql).unwrap();
    }
    let t = Instant::now();
    for _ in 0..n {
        let _ = black_box(conn.query(black_box(sql)).unwrap());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

/// Like `measure`, but returns per-batch means so the caller can report a CV
/// (coefficient of variation) alongside the pooled mean. bd-distinct-loose-scan
/// keep-gate discipline: a delta only counts with CV < 5%.
fn measure_batched(conn: &Connection, sql: &str, batches: u64, per_batch: u64) -> (f64, f64) {
    for _ in 0..20 {
        let _ = conn.query(sql).unwrap();
    }
    let mut means = Vec::new();
    for _ in 0..batches {
        let t = Instant::now();
        for _ in 0..per_batch {
            let _ = black_box(conn.query(black_box(sql)).unwrap());
        }
        means.push(t.elapsed().as_nanos() as f64 / per_batch as f64);
    }
    let mean = means.iter().sum::<f64>() / means.len() as f64;
    let var = means.iter().map(|m| (m - mean) * (m - mean)).sum::<f64>() / means.len() as f64;
    (mean, var.sqrt() / mean * 100.0)
}

/// Freshness gate (rch stale-artifact hazard): report whether the binary under
/// test actually routes `SELECT DISTINCT k` through the loose scan (no sorter),
/// so a benched delta is never attributed to a stale build.
fn distinct_routing(conn: &Connection, sql: &str) -> &'static str {
    let uses_sorter = conn
        .query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .any(|r| {
            matches!(r.values().get(1), Some(fsqlite_types::SqliteValue::Text(o)) if o.to_string() == "SorterOpen")
        });
    if uses_sorter {
        "sorter"
    } else {
        "loose-scan/index"
    }
}

#[test]
#[ignore = "profile; run under --profile release-perf"]
fn distinct_groupby_indexed_or_scan() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, u INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_k ON t(k);").unwrap();
    // k = few distinct (50) indexed; u = few distinct (50) NOT indexed. Pseudo-shuffled.
    for i in 1..=20_000_i64 {
        let key = (i.wrapping_mul(2_654_435_761) >> 8) % 50;
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {key}, {key});"))
            .unwrap();
    }
    let n = 2_000u64;
    let cases = [
        ("scan baseline (WHERE u=7)", "SELECT id FROM t WHERE u = 7"),
        ("DISTINCT k (indexed)", "SELECT DISTINCT k FROM t"),
        ("DISTINCT u (NOT indexed)", "SELECT DISTINCT u FROM t"),
        ("GROUP BY k (indexed)", "SELECT k FROM t GROUP BY k"),
        ("GROUP BY u (NOT indexed)", "SELECT u FROM t GROUP BY u"),
        (
            "GROUP BY k COUNT (indexed)",
            "SELECT k, COUNT(*) FROM t GROUP BY k",
        ),
        (
            "GROUP BY u COUNT (NOT indexed)",
            "SELECT u, COUNT(*) FROM t GROUP BY u",
        ),
    ];
    eprintln!("\n########## bd-5310l DISTINCT / GROUP BY: index stream vs scan+sort ##########");
    eprintln!(
        "  routing: DISTINCT k -> {}   (freshness gate: 'sorter' here means the loose-scan \
         lever is NOT in this binary)",
        distinct_routing(&conn, "SELECT DISTINCT k FROM t")
    );
    for (label, sql) in cases {
        eprintln!("  [{label:32}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    // Interleaved A/B with CV for the loose-scan keep gate: lever case (DISTINCT k,
    // indexed) vs null control (DISTINCT u, sorter path) alternating batch by batch
    // on the same connection.
    let mut k_stats = Vec::new();
    let mut u_stats = Vec::new();
    for _ in 0..5 {
        k_stats.push(measure_batched(&conn, "SELECT DISTINCT k FROM t", 1, 200));
        u_stats.push(measure_batched(&conn, "SELECT DISTINCT u FROM t", 1, 200));
    }
    let pool = |s: &[(f64, f64)]| {
        let means: Vec<f64> = s.iter().map(|(m, _)| *m).collect();
        let mean = means.iter().sum::<f64>() / means.len() as f64;
        let var = means.iter().map(|m| (m - mean) * (m - mean)).sum::<f64>() / means.len() as f64;
        (mean, var.sqrt() / mean * 100.0)
    };
    let (k_mean, k_cv) = pool(&k_stats);
    let (u_mean, u_cv) = pool(&u_stats);
    eprintln!(
        "  A/B interleaved x5: DISTINCT k (lever) {k_mean:9.1} ns/query CV {k_cv:4.1}% | \
         DISTINCT u (null control) {u_mean:9.1} ns/query CV {u_cv:4.1}% | ratio u/k {:6.2}x",
        u_mean / k_mean
    );
    eprintln!(
        "  -> if DISTINCT/GROUP BY k (indexed) ~= u (NOT indexed), the pre-sorted index is unused\n     \
         (full scan + sorter/hash) -> a byte-exact index-stream / loose-scan lever."
    );
    eprintln!("########## end distinct/groupby profile ##########\n");
}
