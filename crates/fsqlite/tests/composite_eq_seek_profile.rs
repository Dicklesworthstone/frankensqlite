//! bd-composite-eq-seek: SELECT id WHERE a=? AND b=? on (a,b) now seeks vs full scan.
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test composite_eq_seek_profile -- --ignored --nocapture
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
fn composite_eq_seek_or_scan() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, u INTEGER, v INTEGER);",
    )
    .unwrap();
    conn.execute("CREATE INDEX idx_ab ON t(a, b);").unwrap();
    for i in 1..=20_000_i64 {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, {}, {}, {}, {});",
            i % 140,
            i % 140,
            i % 140,
            i % 140
        ))
        .unwrap();
    }
    let n = 5_000u64;
    for (label, sql) in [
        (
            "id WHERE a=5 AND b=3 [(a,b) seek]",
            "SELECT id FROM t WHERE a = 5 AND b = 3",
        ),
        (
            "id WHERE u=5 AND v=3 [no index, scan]",
            "SELECT id FROM t WHERE u = 5 AND v = 3",
        ),
    ] {
        eprintln!("  [{label:40}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!("########## end composite-eq profile ##########");
}
