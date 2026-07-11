//! bd-5310l fresh lane: MIN/MAX via a SECONDARY index. `SELECT MIN(v)`/`MAX(v)` where `v` carries a
//! (non-rowid) index should be a single seek to the first-non-NULL / last index entry — O(1), not an
//! O(n) full scan + running aggregate. The existing fast path (`minmax_rowid_seek_plan`) fires ONLY
//! for MIN/MAX over the rowid, so a secondary-indexed column still full-scans. Profile-first: if
//! `SELECT MIN(v)` (v indexed) ~= a full scan and >> `SELECT MIN(id)` (rowid, already O(1)), the
//! index MIN/MAX lever is MISSING (a byte-exact opportunity).
//!
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test minmax_index_profile -- --ignored --nocapture

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
fn minmax_index_bounded_or_scan() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, w TEXT, u INTEGER);")
        .unwrap();
    // v indexed (int), w indexed (text). u NOT indexed (control). Pseudo-shuffled so nothing is
    // pre-sorted by insert order.
    conn.execute("CREATE INDEX idx_t_v ON t(v);").unwrap();
    conn.execute("CREATE INDEX idx_t_w ON t(w);").unwrap();
    for i in 1..=20_000_i64 {
        let key = (i.wrapping_mul(2_654_435_761)) & 0xffff;
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, {key}, 'w{key:05}', {key});"
        ))
        .unwrap();
    }
    let n = 5_000u64;
    let cases = [
        ("MIN(id) rowid (already O(1))", "SELECT MIN(id) FROM t"),
        ("MAX(id) rowid (already O(1))", "SELECT MAX(id) FROM t"),
        ("MIN(v) INT indexed", "SELECT MIN(v) FROM t"),
        ("MAX(v) INT indexed", "SELECT MAX(v) FROM t"),
        ("MIN(w) TEXT indexed", "SELECT MIN(w) FROM t"),
        ("MAX(w) TEXT indexed", "SELECT MAX(w) FROM t"),
        ("MIN(u) NOT indexed (scan)", "SELECT MIN(u) FROM t"),
        ("MAX(u) NOT indexed (scan)", "SELECT MAX(u) FROM t"),
    ];
    eprintln!("\n########## bd-5310l MIN/MAX via secondary index: seek vs scan ##########");
    for (label, sql) in cases {
        eprintln!("  [{label:30}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!(
        "  -> if MIN(v)/MAX(v) (indexed) ~= MIN(u)/MAX(u) (scan) and >> MIN(id) (rowid O(1)),\n     \
         the index MIN/MAX seek is MISSING -> byte-exact lever."
    );
    eprintln!("########## end minmax index profile ##########\n");
}
