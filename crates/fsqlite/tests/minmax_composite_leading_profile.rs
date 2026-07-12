//! bd-minmax-index-seek follow-on: MAX(a)/MIN(a) where `a` leads a composite `(a,b)` index now seeks
//! the index end (O(log n)) instead of full-scanning. Confirms the measured win vs the unindexed
//! baseline (a column with no leading-term index).
//!
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test minmax_composite_leading_profile -- --ignored --nocapture

#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::time::Instant;

use fsqlite::Connection;

fn measure(conn: &Connection, sql: &str, n: u64) -> f64 {
    for _ in 0..50 {
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
fn minmax_composite_leading_seek_or_scan() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, u INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_ab ON t(a, b);").unwrap(); // a leads a composite index
    // u has NO index -> full-scan baseline.
    for i in 1..=20_000_i64 {
        let a = (i.wrapping_mul(2_654_435_761) >> 8) & 0xffff;
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, {a}, {}, {a});",
            i % 100
        ))
        .unwrap();
    }
    let n = 5_000u64;
    let cases = [
        ("MAX(a) [(a,b) leading]", "SELECT MAX(a) FROM t"),
        ("MIN(a) [(a,b) leading]", "SELECT MIN(a) FROM t"),
        ("MAX(u) NOT indexed (scan)", "SELECT MAX(u) FROM t"),
        ("MIN(u) NOT indexed (scan)", "SELECT MIN(u) FROM t"),
    ];
    eprintln!("\n########## bd-minmax composite-leading MIN/MAX: seek vs scan ##########");
    for (label, sql) in cases {
        eprintln!("  [{label:28}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!("########## end composite-leading profile ##########\n");
}
