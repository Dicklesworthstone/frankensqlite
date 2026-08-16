//! GH #249 (bd-gh-fts5-column-match): a column-restricted FTS5 `<col> MATCH <q>`
//! must search only the named column, like stock SQLite.
//!
//! Before the fix, `SELECT rowid FROM t WHERE a MATCH 'red'` searched every
//! column (returned 1,2,3) because the column restriction was dropped when the
//! query reached the live-vtab scan — only the whole-table colon syntax
//! (`t MATCH 'a: red'`) worked. The planner now rewrites a column-restricted
//! match to that same `<col> : (<q>)` form, so both agree.
//!
//! Requires `--features ext-fts5`.

#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn rowids(conn: &Connection, sql: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {e}"))
        .iter()
        .map(|row| match row.values().first() {
            Some(SqliteValue::Integer(n)) => *n,
            other => panic!("expected integer rowid, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn fts5_column_restricted_match_respects_the_column() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(a, b)")
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO t(rowid,a,b) VALUES \
             (1,'red apple','blue sky'),(2,'green grass','red rose'),(3,'red car','red door')",
        )
        .await
        .unwrap();

        // Column-restricted MATCH searches only the named column (the fix).
        assert_eq!(
            rowids(&conn, "SELECT rowid FROM t WHERE a MATCH 'red'").await,
            vec![1, 3]
        );
        assert_eq!(
            rowids(&conn, "SELECT rowid FROM t WHERE b MATCH 'red'").await,
            vec![2, 3]
        );

        // Whole-table MATCH still searches every column.
        assert_eq!(
            rowids(&conn, "SELECT rowid FROM t WHERE t MATCH 'red'").await,
            vec![1, 2, 3]
        );

        // The pre-existing in-query colon column-filter syntax is unaffected.
        assert_eq!(
            rowids(&conn, "SELECT rowid FROM t WHERE t MATCH 'a: red'").await,
            vec![1, 3]
        );
        assert_eq!(
            rowids(&conn, "SELECT rowid FROM t WHERE t MATCH 'b: red'").await,
            vec![2, 3]
        );
    });
}
