//! bd-minmax-range-seek: MIN(a) WHERE a>c / MAX(a) WHERE a<c via one bound seek vs a full scan.
//! Run: RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR rch exec -- \
//!   cargo test --profile release-perf -p fsqlite --test minmax_range_profile -- --ignored --nocapture
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
fn minmax_range_seek_or_scan() {
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
                "MIN(a) WHERE a > 30000 [indexed]",
                "SELECT MIN(a) FROM t WHERE a > 30000",
            ),
            (
                "MAX(a) WHERE a < 30000 [indexed]",
                "SELECT MAX(a) FROM t WHERE a < 30000",
            ),
            (
                "MIN(u) WHERE u > 30000 [scan]",
                "SELECT MIN(u) FROM t WHERE u > 30000",
            ),
        ] {
            eprintln!(
                "  [{label:34}] {:9.1} ns/query",
                measure(&conn, sql, n).await
            );
        }
        eprintln!("########## end range profile ##########");
    });
}
