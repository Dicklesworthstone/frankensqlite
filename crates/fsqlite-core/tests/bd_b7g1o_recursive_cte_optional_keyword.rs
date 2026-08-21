//! bd-b7g1o: SQLite treats the `RECURSIVE` keyword as optional — a
//! self-referencing CTE (`WITH cte AS (... FROM cte ...)`) is recursive whether
//! or not `RECURSIVE` was written. Before the fix, frank required the keyword and
//! a keyword-less self-reference failed with `no such table`.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.unwrap();
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer, got {other:?}"),
    }
}

async fn scalar_text(conn: &Connection, sql: &str) -> String {
    let rows = conn.query(sql).await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Text(t) => t.to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

#[test]
fn bd_b7g1o_recursive_cte_without_keyword() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // Self-referencing CTE WITHOUT the RECURSIVE keyword → 1+2+3 = 6.
        assert_eq!(
            scalar_i64(
                &conn,
                "WITH r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) \
                 SELECT sum(n) FROM r;",
            )
            .await,
            6
        );

        // A non-recursive CTE precedes the recursive one, still no keyword.
        assert_eq!(
            scalar_i64(
                &conn,
                "WITH base(x) AS (SELECT 10), \
                      r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) \
                 SELECT (SELECT x FROM base) + sum(n) FROM r;",
            )
            .await,
            16
        );

        // Recursive tree walk over a JOIN, no keyword.
        conn.execute("CREATE TABLE org(id INT, mgr INT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO org VALUES(1,NULL),(2,1),(3,2),(4,3);")
            .await
            .unwrap();
        assert_eq!(
            scalar_text(
                &conn,
                "WITH p(id, path) AS ( \
                    SELECT id, ''||id FROM org WHERE mgr IS NULL \
                    UNION ALL \
                    SELECT o.id, p.path||'/'||o.id FROM org o JOIN p ON o.mgr=p.id) \
                 SELECT group_concat(path ORDER BY id) FROM p;",
            )
            .await,
            "1,1/2,1/2/3,1/2/3/4"
        );

        // Regression: the explicit RECURSIVE keyword still works.
        assert_eq!(
            scalar_i64(
                &conn,
                "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) \
                 SELECT sum(n) FROM r;",
            )
            .await,
            6
        );

        // Regression: a genuinely non-recursive plain WITH is unaffected.
        assert_eq!(
            scalar_i64(
                &conn,
                "WITH a(x) AS (SELECT 1), b(y) AS (SELECT 2) SELECT a.x + b.y FROM a, b;",
            )
            .await,
            3
        );

        conn.close().await.unwrap();
    });
}
