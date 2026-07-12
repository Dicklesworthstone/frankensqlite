//! bd-5310l: `SELECT MAX(b) FROM t WHERE a = ?` (or MIN) with a composite index `(a,b)`. The extremum
//! of `b` within the `a=?` group is the LAST (MAX) / FIRST (MIN) index entry of that prefix — a single
//! seek, O(log n). The existing aggregate index-eq seek positions at `a=?` then SCANS the whole group
//! accumulating, O(group size). Profile-first: if MAX(b) WHERE a=? on a composite index ~ scales with
//! the group size, the prefix-extremum seek is MISSING -> a byte-exact lever (extends bd-minmax-index-seek).
//!
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test minmax_prefix_profile -- --ignored --nocapture

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
fn minmax_prefix_seek_or_scan() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);")
        .unwrap();
    // (a,b) composite; a2 single-column so MAX(b) WHERE a2=? cannot use a b-ordered prefix.
    conn.execute("CREATE INDEX idx_ab ON t(a, b);").unwrap();
    conn.execute("CREATE INDEX idx_a2 ON t(c);").unwrap();
    // 20 distinct a values -> each a-group has ~1000 rows (large group so a group-scan is visibly O(n)).
    for i in 1..=20_000_i64 {
        let a = i % 20;
        let b = (i.wrapping_mul(2_654_435_761) >> 8) & 0xffff;
        let c = i % 20;
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {a}, {b}, {c});"))
            .unwrap();
    }
    let n = 5_000u64;
    let cases = [
        (
            "MAX(b) WHERE a=7 [(a,b) idx]",
            "SELECT MAX(b) FROM t WHERE a = 7",
        ),
        (
            "MIN(b) WHERE a=7 [(a,b) idx]",
            "SELECT MIN(b) FROM t WHERE a = 7",
        ),
        (
            "COUNT(*) WHERE a=7 [(a,b) idx]",
            "SELECT COUNT(*) FROM t WHERE a = 7",
        ),
        // Control: MAX over the single-col-indexed c grouped by an eq on c itself (degenerate) and a
        // plain full-column MAX(b) (no WHERE) which already seeks via bd-minmax-index-seek only if b is
        // indexed alone (it is not here) -> full scan baseline.
        (
            "MAX(b) no WHERE [b not sole-indexed]",
            "SELECT MAX(b) FROM t",
        ),
        (
            "SUM(b) WHERE a=7 [(a,b) idx]",
            "SELECT SUM(b) FROM t WHERE a = 7",
        ),
    ];
    eprintln!(
        "\n########## bd-5310l MAX/MIN(b) WHERE a=? on (a,b): prefix seek vs group scan ##########"
    );
    for (label, sql) in cases {
        eprintln!("  [{label:38}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!(
        "  -> if MAX(b) WHERE a=7 ~ COUNT(*)/SUM(b) WHERE a=7 (both scan the ~1000-row a=7 group),\n     \
         the prefix-extremum seek is MISSING; if MAX(b) << COUNT(*), it already seeks."
    );
    eprintln!("########## end minmax prefix profile ##########\n");
}
