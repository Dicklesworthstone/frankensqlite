//! Profile-first: which aggregate/existence shapes still SCAN (Rewind) vs SEEK after the recent
//! eq/range/IN/composite-prefix-range work? Run:
//! cargo test -p fsqlite --test gap_probe2 -- --ignored --nocapture

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

async fn shape(conn: &Connection, sql: &str) -> (bool, bool) {
    let ops: Vec<String> = conn
        .query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .filter_map(|r| match r.values().get(1) {
            Some(SqliteValue::Text(op)) => Some(op.to_string()),
            _ => None,
        })
        .collect();
    (
        ops.iter().any(|o| o.starts_with("Seek")),
        ops.iter().any(|o| o == "Rewind"),
    )
}

#[test]
#[ignore = "probe; run with --ignored --nocapture"]
fn gap2() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        for ddl in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
            "CREATE INDEX idx_a ON t(a);",
        ] {
            c.execute(ddl).await.unwrap();
        }
        for i in 1..=300_i64 {
            c.execute(&format!(
                "INSERT INTO t VALUES ({i}, {}, {}, {});",
                i % 15,
                i % 30,
                i % 50
            ))
            .await
            .unwrap();
        }
        let cases = [
            "SELECT MIN(b) FROM t WHERE a = 7 AND b > 2",
            "SELECT MAX(b) FROM t WHERE a = 7 AND b > 2",
            "SELECT COUNT(*) FROM t WHERE a = 7 AND b > 3",
            "SELECT COUNT(*) FROM t WHERE a = 7 AND x = 5",
            "SELECT EXISTS(SELECT 1 FROM t WHERE a = 7 AND b = 3)",
            "SELECT a, COUNT(*) FROM t GROUP BY a",
            "SELECT MIN(b) FROM t WHERE a = 7",
            "SELECT AVG(b) FROM t WHERE a = 7 AND b > 3",
            "SELECT COUNT(*) FROM t WHERE a = 7 AND b BETWEEN 3 AND 9",
            "SELECT MIN(a) FROM t WHERE a > 5",
            // Pure single-column range aggregates (no residual): guarded by agg_range_shadowed_index_oracle.
            "SELECT COUNT(*) FROM t WHERE a < 5",
            "SELECT COUNT(*) FROM t WHERE a >= 5 AND a <= 9",
            // Still-scanning shapes worth a future lever: GROUP BY, COUNT(DISTINCT), bare MIN/MAX walk.
            "SELECT COUNT(DISTINCT a) FROM t",
            // Non-aggregate single-column range scans (codegen_select paths 1906/2088) — do these seek
            // the index under the composite-shadow schema, or shadow-fail like the aggregate range did?
            "SELECT id FROM t WHERE a < 5",
            "SELECT id FROM t WHERE a < 5 ORDER BY a",
            "SELECT id, b FROM t WHERE a >= 5 AND a <= 9",
            "SELECT id FROM t WHERE a >= 5 AND a <= 9 ORDER BY a",
        ];
        eprintln!("\n########## gap2: SEEK vs SCAN ##########");
        for sql in cases {
            let (seek, rewind) = shape(&c, sql).await;
            let tag = if seek { "SEEK " } else { "SCAN!" };
            eprintln!("  [{tag}] seek={seek} rewind={rewind}  {sql}");
        }
        eprintln!("########## end ##########\n");
    });
}
