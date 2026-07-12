//! Profile-first: find the next O(n)-that-should-be-O(log n) gap among common shapes. A point rowid
//! lookup (~µs) is the "already fast" baseline; any shape in the milliseconds is a scan → a candidate
//! lever.
//!
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test misc_shapes_profile -- --ignored --nocapture

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
fn misc_shapes() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_a ON t(a);").unwrap();
    for i in 1..=20_000_i64 {
        let a = (i.wrapping_mul(2_654_435_761) >> 8) & 0xffff;
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {a}, {});", i % 100))
            .unwrap();
    }
    let n = 5_000u64;
    let cases = [
        (
            "point rowid lookup (fast baseline)",
            "SELECT a FROM t WHERE id = 12345",
        ),
        ("COUNT(*) whole table", "SELECT COUNT(*) FROM t"),
        (
            "COUNT(*) WHERE a = 7 (indexed)",
            "SELECT COUNT(*) FROM t WHERE a = 7",
        ),
        (
            "EXISTS WHERE a = 7 (indexed)",
            "SELECT EXISTS(SELECT 1 FROM t WHERE a = 7)",
        ),
        (
            "1 WHERE a=7 LIMIT 1 (indexed)",
            "SELECT 1 FROM t WHERE a = 7 LIMIT 1",
        ),
        (
            "MIN(a) WHERE a > 30000 (indexed range)",
            "SELECT MIN(a) FROM t WHERE a > 30000",
        ),
        (
            "MAX(a) WHERE a < 30000 (indexed range)",
            "SELECT MAX(a) FROM t WHERE a < 30000",
        ),
        (
            "SUM(b) WHERE a = 7 (indexed group)",
            "SELECT SUM(b) FROM t WHERE a = 7",
        ),
    ];
    eprintln!("\n########## misc shapes: fast (µs) vs scan (ms) ##########");
    for (label, sql) in cases {
        eprintln!("  [{label:38}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!("########## end misc shapes ##########\n");
}
