//! bd-hbv4e: a scalar subquery whose FROM is an inline `(VALUES ...)` (or any
//! unaliased derived table) referencing the auto-generated `columnN` names was
//! wrongly rejected with "no such column in result expression". The FROM-less
//! outer `SELECT (subquery)` runs the correlation detector, which computed an
//! EMPTY `inner_tables` for the anonymous derived source (it contributed no
//! alias), so the subquery's own `column1` was misclassified as a bare outer
//! column. Anonymous derived tables now register an unnamed sentinel, mirroring
//! how a NAMED derived table already suppresses that classification. Oracle:
//! bundled rusqlite.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn frank_opt_int(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(sql).await.unwrap_or_else(|e| panic!("frank error on `{sql}`: {e:?}"));
    match &rows[0].values()[0] {
        SqliteValue::Integer(n) => Some(*n),
        SqliteValue::Null => None,
        other => panic!("expected INTEGER/NULL for `{sql}`, got {other:?}"),
    }
}

fn stock_opt_int(sql: &str) -> Option<i64> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.query_row(sql, [], |r| r.get::<_, Option<i64>>(0))
        .unwrap()
}

#[test]
fn values_columnn_in_scalar_subquery_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        let cases = [
            // The bead's confirmed repros — previously "no such column".
            "SELECT (SELECT count(column1) FROM (VALUES (1),(2)))",
            "SELECT (SELECT sum(column1) FROM (VALUES (1),(2),(3)))",
            "SELECT (SELECT column1 FROM (VALUES (10),(20)) ORDER BY column1 DESC LIMIT 1)",
            "SELECT (SELECT max(column1) FROM (VALUES (5),(2),(9),(1)))",
            // Multi-column VALUES + column2 in projection and WHERE.
            "SELECT (SELECT count(column2) FROM (VALUES (1,'a'),(2,'b'),(3,'c')))",
            "SELECT (SELECT sum(column1) FROM (VALUES (1,'x'),(2,'y'),(4,'y')) WHERE column2='y')",
            // Unaliased nested derived table (subquery, not VALUES).
            "SELECT (SELECT sum(column1) FROM (SELECT column1 FROM (VALUES (1),(2),(3)) WHERE column1 > 1))",
            // Anonymous COMPOUND and ORDER-BY derived tables with a named column
            // `x` — the same anonymous-subquery path, so the sentinel covers them.
            "SELECT (SELECT count(*) FROM (SELECT 1 UNION SELECT 2 UNION SELECT 1))",
            "SELECT (SELECT sum(x) FROM (SELECT 1 x UNION ALL SELECT 2 UNION ALL SELECT 3))",
            "SELECT (SELECT max(x) FROM (SELECT 5 x UNION SELECT 9 EXCEPT SELECT 1))",
            "SELECT (SELECT sum(x) FROM (SELECT 2 x UNION SELECT 1 ORDER BY x))",
            // Regression guards — these already worked and must stay correct.
            "SELECT (SELECT count(*) FROM (VALUES (1),(2)))",
            "WITH t(x) AS (VALUES (1),(2),(3)) SELECT (SELECT sum(x) FROM t)",
            "SELECT count(column1) FROM (VALUES (1),(2))",
        ];

        for sql in cases {
            let got = frank_opt_int(&conn, sql).await;
            let stock = stock_opt_int(sql);
            assert_eq!(got, stock, "scalar subquery over VALUES diverges from stock: {sql}");
        }
    });
}
