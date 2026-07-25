//! bd-eq-seek-fallback-zero-match (non-agg): SELECT id WHERE a=<absent int> now O(log n).
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test eq_seek_nonagg_profile -- --ignored --nocapture
#![allow(clippy::cast_precision_loss)]
use fsqlite::Connection;
use std::hint::black_box;
use std::time::Instant;
async fn measure(conn: &Connection, sql: &str, n: u64) -> f64 {
    for _ in 0..50 {
        let _ = conn.query(sql).await.unwrap();
    }
    let t = Instant::now();
    for _ in 0..n {
        let _ = black_box(conn.query(black_box(sql)).await.unwrap());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}
#[test]
#[ignore = "profile; run under --profile release-perf"]
fn eq_seek_nonagg_absent() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, u INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_a ON t(a);").await.unwrap();
        for i in 1..=20_000_i64 {
            let a = (i.wrapping_mul(2_654_435_761) >> 8) & 0xffff;
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {a}, {a});"))
                .await
                .unwrap();
        }
        let n = 5_000u64;
        for (label, sql) in [
            (
                "id WHERE a=absent LIMIT 1 [exact]",
                "SELECT id FROM t WHERE a = 999999 LIMIT 1",
            ),
            (
                "EXISTS a=absent [exact]",
                "SELECT EXISTS(SELECT 1 FROM t WHERE a = 999999)",
            ),
            (
                "id WHERE u=absent [no index, scan]",
                "SELECT id FROM t WHERE u = 999999 LIMIT 1",
            ),
        ] {
            eprintln!(
                "  [{label:38}] {:9.1} ns/query",
                measure(&conn, sql, n).await
            );
        }
        eprintln!("########## end nonagg profile ##########");
    });
}
