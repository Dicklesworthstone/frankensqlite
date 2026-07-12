//! bd-minmax-pair-seek: SELECT MIN(v), MAX(v) via two index-end seeks vs a full scan.
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test minmax_pair_profile -- --ignored --nocapture
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
fn minmax_pair_seek_or_scan() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, u INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_v ON t(v);").unwrap();
    for i in 1..=20_000_i64 {
        let v = (i.wrapping_mul(2_654_435_761) >> 8) & 0xffff;
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {v}, {v});"))
            .unwrap();
    }
    let n = 5_000u64;
    for (label, sql) in [
        ("MIN(v),MAX(v) [indexed]", "SELECT MIN(v), MAX(v) FROM t"),
        ("MIN(u),MAX(u) [scan]", "SELECT MIN(u), MAX(u) FROM t"),
    ] {
        eprintln!("  [{label:24}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!("########## end pair profile ##########");
}
