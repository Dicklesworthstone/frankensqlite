//! bd-gh-compound-order-by-collate-clks3 (GH #217/#218): a compound SELECT
//! ORDER BY term wrapped in COLLATE must resolve to its SELECT-list column
//! (by name or position) instead of erroring "ORDER BY expression not found in
//! SELECT list". This keeper pins UNION / UNION ALL / positional / table-backed
//! forms against the rusqlite oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

const SEED: &[&str] = &["CREATE TABLE t(a TEXT)", "INSERT INTO t VALUES ('b')"];

const QUERIES: &[&str] = &[
    "SELECT 'b' AS x UNION SELECT 'A' ORDER BY x COLLATE NOCASE",
    "SELECT 'b' AS x UNION SELECT 'A' ORDER BY 1 COLLATE NOCASE",
    "SELECT 'b' AS x UNION ALL SELECT 'A' ORDER BY x COLLATE NOCASE",
    "SELECT a FROM t UNION SELECT 'A' ORDER BY a COLLATE NOCASE",
    // Discriminating: NOCASE flips the binary order of 'B' vs 'a'.
    "SELECT 'B' AS x UNION SELECT 'a' ORDER BY x COLLATE NOCASE",
    // Control: no COLLATE still works.
    "SELECT 'b' AS x UNION SELECT 'A' ORDER BY x",
];

fn oracle(sql: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in SEED {
        conn.execute(s, []).unwrap();
    }
    conn.prepare(sql)
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn compound_order_by_collate_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for s in SEED {
            conn.execute(s).await.expect("seed");
        }
        for sql in QUERIES {
            let expected = oracle(sql);
            let rows = conn
                .query(sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
            let got: Vec<String> = rows
                .iter()
                .map(|r| match r.values()[0] {
                    SqliteValue::Text(ref s) => s.as_ref().to_owned(),
                    ref other => panic!("`{sql}` not text: {other:?}"),
                })
                .collect();
            assert_eq!(
                got, expected,
                "compound ORDER BY COLLATE `{sql}` diverged from the C SQLite oracle"
            );
        }
    });
}
