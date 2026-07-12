//! bd-eq-seek-fallback-zero-match: COUNT(*)/SUM WHERE a=<absent int> now O(log n) (no fallback scan).
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test eq_seek_exact_profile -- --ignored --nocapture
#![allow(clippy::cast_precision_loss)]
use fsqlite::Connection;
use std::hint::black_box;
use std::time::Instant;
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
fn eq_seek_absent_key() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, u INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_a ON t(a);").unwrap();
    for i in 1..=20_000_i64 {
        let a = (i.wrapping_mul(2_654_435_761) >> 8) & 0xffff;
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, {a}, {}, {a});",
            i % 100
        ))
        .unwrap();
    }
    let n = 5_000u64;
    for (label, sql) in [
        (
            "COUNT(*) WHERE a=absent [exact]",
            "SELECT COUNT(*) FROM t WHERE a = 999999",
        ),
        (
            "SUM(b) WHERE a=absent [exact]",
            "SELECT SUM(b) FROM t WHERE a = 999999",
        ),
        (
            "COUNT(*) WHERE u=absent [no index, scan]",
            "SELECT COUNT(*) FROM t WHERE u = 999999",
        ),
    ] {
        eprintln!("  [{label:42}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!("########## end eq-seek profile ##########");
}
