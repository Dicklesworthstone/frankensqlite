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
    for (label, sql) in cases {
        eprintln!("  [{label:32}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!(
        "  -> if DISTINCT/GROUP BY k (indexed) ~= u (NOT indexed), the pre-sorted index is unused\n     \
         (full scan + sorter/hash) -> a byte-exact index-stream / loose-scan lever."
    );
    eprintln!("########## end distinct/groupby profile ##########\n");
}
