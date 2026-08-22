//! bd-t7oeo: the interpreted evaluator (`eval_join_expr`, the FROM-less / CTE
//! routed lane) had no `Expr::RowValue` arm, so a row-value comparison whose
//! operands are subqueries (which arrive pre-evaluated as
//! `RowValue([BoundOuterValue, …])`) errored NotImplemented. Pure-literal
//! `(1,2)=(1,2)` already worked via the compiled/VDBE path; this gap was
//! specifically the interpreted path. The BinaryOp arm now compares row values
//! element-wise with SQLite's row-value semantics.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn frank_opt_int(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(sql).await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Integer(n) => Some(*n),
        SqliteValue::Null => None,
        other => panic!("expected INTEGER/NULL, got {other:?}"),
    }
}

fn stock_opt_int(sql: &str) -> Option<i64> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.query_row(sql, [], |r| r.get::<_, Option<i64>>(0))
        .unwrap()
}

/// Every case is routed through the interpreted evaluator by using subquery
/// operands, and checked against the rusqlite oracle (including NULL results).
#[test]
fn rowvalue_subquery_comparisons_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        let cases = [
            // The bead's confirmed repros.
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c)=(SELECT a,b FROM c)",
            "SELECT (SELECT 1,2)=(SELECT 1,2)",
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c)=(1,2)",
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c)=(1,3)",
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c)<(1,3)",
            // Ordering operators, all six.
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c)>(1,3)",
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c)<=(1,2)",
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c)>=(1,2)",
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c)<>(1,2)",
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c)<>(1,3)",
            // Lexicographic: first element decides even though the second differs.
            "WITH c(a,b) AS (SELECT 1,9) SELECT (SELECT * FROM c)<(2,0)",
            // NULL semantics: NULL at the deciding position -> NULL order.
            "WITH c(a,b) AS (SELECT 1,NULL) SELECT (SELECT * FROM c)<(1,3)",
            // NULL not reached because element 0 already decides -> 1.
            "WITH c(a,b) AS (SELECT 1,NULL) SELECT (SELECT * FROM c)<(2,3)",
            // NULL with `=`: a NULL element and no decisive inequality -> NULL.
            "WITH c(a,b) AS (SELECT 1,NULL) SELECT (SELECT * FROM c)=(1,3)",
            // NULL with `=` but a decisive unequal element -> 0 (not NULL).
            "WITH c(a,b) AS (SELECT 1,NULL) SELECT (SELECT * FROM c)=(2,3)",
            // Text affinity per element.
            "WITH c(a,b) AS (SELECT 'x','y') SELECT (SELECT * FROM c)=('x','y')",
            "WITH c(a,b) AS (SELECT 'x','y') SELECT (SELECT * FROM c)<('x','z')",
        ];

        for sql in cases {
            let got = frank_opt_int(&conn, sql).await;
            let stock = stock_opt_int(sql);
            assert_eq!(
                got, stock,
                "row-value comparison diverges from stock: {sql}"
            );
        }
    });
}
