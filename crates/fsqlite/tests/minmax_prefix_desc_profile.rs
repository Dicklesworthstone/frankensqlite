//! bd-minmax-prefix-seek: MAX(b) WHERE a=? on (a, b DESC) via SeekGE (O(log n)) vs the group scan.
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test minmax_prefix_desc_profile -- --ignored --nocapture
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
fn minmax_prefix_desc_seek_or_scan() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_ab_desc ON t(a, b DESC);")
        .unwrap();
    for i in 1..=20_000_i64 {
        let a = i % 20;
        let b = (i.wrapping_mul(2_654_435_761) >> 8) & 0xffff;
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {a}, {b});"))
            .unwrap();
    }
    let n = 5_000u64;
    for (label, sql) in [
        (
            "MAX(b) WHERE a=7 [(a,b DESC)]",
            "SELECT MAX(b) FROM t WHERE a = 7",
        ),
        (
            "COUNT(*) WHERE a=7 (group scan)",
            "SELECT COUNT(*) FROM t WHERE a = 7",
        ),
    ] {
        eprintln!("  [{label:34}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!("########## end desc-prefix profile ##########");
}
