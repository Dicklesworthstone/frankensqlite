//! bd-minmax-index-seek follow-on: MAX(v)/MIN(v) on a DESC index seeks the index end vs full scan.
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test minmax_desc_index_profile -- --ignored --nocapture
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
fn minmax_desc_seek_or_scan() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, u INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_v_desc ON t(v DESC);")
        .unwrap();
    for i in 1..=20_000_i64 {
        let v = (i.wrapping_mul(2_654_435_761) >> 8) & 0xffff;
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {v}, {v});"))
            .unwrap();
    }
    let n = 5_000u64;
    for (label, sql) in [
        ("MAX(v) [DESC index]", "SELECT MAX(v) FROM t"),
        ("MIN(v) [DESC index]", "SELECT MIN(v) FROM t"),
        ("MAX(u) NOT indexed", "SELECT MAX(u) FROM t"),
        ("MIN(u) NOT indexed", "SELECT MIN(u) FROM t"),
    ] {
        eprintln!("  [{label:24}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!("########## end desc profile ##########");
}
