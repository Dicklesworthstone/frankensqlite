#![recursion_limit = "512"]

//! GH #175 (bd-gh-datetime-statement-now): `'now'` (and the `CURRENT_*` values)
//! must be read from the wall clock exactly once per statement and reused for
//! every evaluation within it — so `julianday('now')` is identical across all
//! rows a single statement produces, exactly like C SQLite. Before the fix every
//! evaluation called `SystemTime::now()`, so a many-row statement saw many
//! distinct `'now'` values. The value itself is time-dependent, so these assert
//! the STABILITY property rather than a specific instant.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    match &rows[0].values()[0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("{sql}: expected integer, got {other:?}"),
    }
}

#[test]
fn julianday_now_is_constant_across_one_statements_rows() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        // A single statement producing many rows: 'now' must be identical for
        // every row, so COUNT(DISTINCT ...) is exactly 1.
        assert_eq!(
            scalar_i64(
                &c,
                "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x < 4000) \
                 SELECT count(DISTINCT julianday('now')) FROM r",
            )
            .await,
            1
        );
    });
}

#[test]
fn multiple_now_reads_in_one_statement_are_equal() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        // Two direct reads and two subquery reads in one statement all agree.
        assert_eq!(scalar_i64(&c, "SELECT julianday('now') = julianday('now')").await, 1);
        assert_eq!(
            scalar_i64(&c, "SELECT (SELECT julianday('now')) = (SELECT julianday('now'))").await,
            1
        );
        // strftime/datetime built on 'now' share the same captured instant.
        assert_eq!(
            scalar_i64(&c, "SELECT strftime('%J', 'now') = strftime('%J', 'now')").await,
            1
        );
    });
}
