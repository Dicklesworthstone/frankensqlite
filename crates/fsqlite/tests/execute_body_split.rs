//! bd-5310l follow-on: is execute_body (~biggest phase, the residual gap vs C SQLite) dominated by
//! the SEEK/cursor-open (systemic MVCC/btree — no clean lever) or by ROW-PROCESSING (Column reads +
//! Row materialization — potentially reducible)? A point-lookup HIT vs MISS split answers it: MISS =
//! cursor open + seek(not found); HIT = the same + Column x2 + ResultRow + materialize. HIT-MISS
//! isolates the row-processing. Runs in `fsqlite` (no criterion dev-dep) so it builds light/reliable.
//!
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test execute_body_split -- --ignored --nocapture

#![allow(clippy::cast_precision_loss)]

use std::time::Instant;

use fsqlite::Connection;
use fsqlite_core::connection::{
    hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};

fn measure(conn: &Connection, sql: &str, n: u64) -> (f64, f64) {
    for _ in 0..200 {
        let _ = conn.query(sql).unwrap();
    }
    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();
    let t = Instant::now();
    for _ in 0..n {
        let _ = conn.query(sql).unwrap();
    }
    let wall = t.elapsed().as_nanos() as f64 / n as f64;
    let snap = hot_path_profile_snapshot();
    reset_hot_path_profile();
    set_hot_path_profile_enabled(false);
    (wall, snap.execute_body_time_ns as f64 / n as f64)
}

#[test]
#[ignore = "profile; run under --profile release-perf"]
fn execute_body_hit_vs_miss() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_t_k ON t(k);").unwrap();
    for i in 1..=5000_i64 {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'row{i}', {});",
            i % 100
        ))
        .unwrap();
    }
    // Tiny 1-row table: a MISS here does cursor-open + a ~1-level seek, isolating the fixed
    // cursor-open cost from the per-level B-tree descent that the 5000-row MISS pays.
    conn.execute("CREATE TABLE s (id INTEGER PRIMARY KEY, v TEXT);")
        .unwrap();
    conn.execute("INSERT INTO s VALUES (1, 'only');").unwrap();
    let n = 200_000u64;
    let cases = [
        ("SELECT 1 (no cursor)", "SELECT 1"),
        (
            "MISS tiny table (1 row)",
            "SELECT id, v FROM s WHERE id = 9999999",
        ),
        (
            "point MISS (5000 rows)",
            "SELECT id, v FROM t WHERE id = 9999999",
        ),
        (
            "point HIT (id present)",
            "SELECT id, v FROM t WHERE id = 2500",
        ),
        ("point HIT covering id", "SELECT id FROM t WHERE id = 2500"),
    ];
    eprintln!("\n########## bd-5310l execute_body HIT vs MISS split ##########");
    for (label, sql) in cases {
        let (wall, exec) = measure(&conn, sql, n);
        eprintln!("  [{label:24}] wall = {wall:8.1} ns   execute_body = {exec:8.1} ns");
    }
    eprintln!(
        "  -> (HIT execute_body) - (MISS execute_body) = the Column-read + Row-materialize cost;\n     \
         if ~0, the ~14us is seek/cursor-open (systemic MVCC/btree, no clean lever)."
    );
    eprintln!("########## end execute_body split ##########\n");
}
