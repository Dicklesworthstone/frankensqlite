//! bd-5310l fresh lane: the ORDER BY sorter. A classic clean lever is `ORDER BY ... LIMIT N`: it
//! should keep only a BOUNDED top-N heap (O(n log N), N rows of memory), not a FULL sort then
//! truncate (O(n log n), n rows). Profile-first: if `... ORDER BY x LIMIT 10` costs ~the same as the
//! full `... ORDER BY x`, the bounded-sorter optimization is MISSING -> a byte-exact lever.
//!
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test sorter_limit_profile -- --ignored --nocapture

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
fn sorter_limit_bounded_or_full() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);")
        .unwrap();
    // v is NOT indexed -> ORDER BY v must sort. Pseudo-random-ish text so it isn't pre-sorted.
    for i in 1..=10_000_i64 {
        let key = (i.wrapping_mul(2_654_435_761)) & 0xffff;
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'v{key:05}', {});",
            i % 100
        ))
        .unwrap();
    }
    let n = 2_000u64;
    let cases = [
        ("scan no sort (baseline)", "SELECT id FROM t WHERE k = 7"),
        ("FULL sort ORDER BY v", "SELECT id FROM t ORDER BY v"),
        ("sort + LIMIT 10", "SELECT id FROM t ORDER BY v LIMIT 10"),
        ("sort + LIMIT 100", "SELECT id FROM t ORDER BY v LIMIT 100"),
        (
            "sort DESC + LIMIT 10",
            "SELECT id FROM t ORDER BY v DESC LIMIT 10",
        ),
        // bd-5310l lever: LIMIT+OFFSET within the cap should now be BOUNDED (~= LIMIT, not full sort).
        (
            "LIMIT 10 OFFSET 100 (bounded)",
            "SELECT id FROM t ORDER BY v LIMIT 10 OFFSET 100",
        ),
        (
            "LIMIT 10 OFFSET 500 (bounded)",
            "SELECT id FROM t ORDER BY v LIMIT 10 OFFSET 500",
        ),
        // Over the cap -> full-sort fallback (~= FULL sort).
        (
            "LIMIT 10 OFFSET 5000 (over cap)",
            "SELECT id FROM t ORDER BY v LIMIT 10 OFFSET 5000",
        ),
    ];
    eprintln!("\n########## bd-5310l ORDER BY sorter: bounded-LIMIT vs full-sort ##########");
    for (label, sql) in cases {
        eprintln!("  [{label:26}] {:9.1} ns/query", measure(&conn, sql, n));
    }
    eprintln!(
        "  -> if LIMIT 10 ~= FULL sort, the bounded top-N sorter is MISSING (a byte-exact lever);\n     \
         if LIMIT 10 << FULL sort, it is already bounded (optimized)."
    );
    eprintln!("########## end sorter profile ##########\n");
}
