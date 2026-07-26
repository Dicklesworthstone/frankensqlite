//! Probe: SELECT * FROM t WHERE a=? AND b=? on a composite (a,b) index — 2-field seek, or seek a + scan?
use fsqlite::Connection;
use fsqlite_types::SqliteValue;
#[test]
#[ignore = "probe"]
fn composite_eq_plan() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);")
            .await
            .unwrap();
        c.execute("CREATE INDEX idx_ab ON t(a, b);").await.unwrap();
        for i in 1..=500_i64 {
            c.execute(&format!(
                "INSERT INTO t VALUES ({i}, {}, {}, {i});",
                i % 20,
                i % 10
            ))
            .await
            .unwrap();
        }
        for sql in [
            "SELECT id FROM t WHERE a = 5 AND b = 3",
            "SELECT x FROM t WHERE a = 5 AND b = 3 LIMIT 1",
        ] {
            let ops: Vec<String> = c
                .query(&format!("EXPLAIN {sql}"))
                .await
                .unwrap()
                .iter()
                .filter_map(|r| match r.values().get(1) {
                    Some(SqliteValue::Text(o)) => Some(o.to_string()),
                    _ => None,
                })
                .collect();
            let makerecords = ops.iter().filter(|o| *o == "MakeRecord").count();
            eprintln!(
                "\n[{sql}]\n  MakeRecord count={makerecords} (2+ key probe => composite seek)\n  ops={ops:?}"
            );
        }
    });
}
