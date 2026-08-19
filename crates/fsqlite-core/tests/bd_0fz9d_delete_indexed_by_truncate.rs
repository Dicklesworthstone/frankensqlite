//! bd-0fz9d (regression of 350af08c3 / bd-wlo29 L10).
//!
//! `validate_dml_target_partial_index` demanded that a forced `INDEXED BY`
//! partial index be covered by the statement WHERE for every UPDATE/DELETE. But
//! stock SQLite's DELETE truncate optimization (OP_Clear) bypasses the index
//! entirely for a WHERE-less, trigger-less, RETURNING-less, FK-parent-less
//! DELETE, so it never plans the WHERE loop that would reject an uncovered
//! forced partial index — a bare `DELETE FROM t INDEXED BY pi` simply empties the
//! table. The unconditional gate regressed this to a "no query solution" error.
//! Stock only opens the index (and errors) when the truncate opt is disabled.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn table_count(conn: &Connection) -> i64 {
    let rows = conn.query("SELECT count(*) FROM t;").await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("count(*) returned {other:?}"),
    }
}

/// A bare `DELETE FROM t INDEXED BY <partial>` (no WHERE, no trigger) must take
/// stock's truncate path and empty the table, not error.
#[test]
fn bare_delete_indexed_by_partial_truncates_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a INTEGER, b INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX pi ON t(a) WHERE b > 0;")
            .await
            .unwrap();
        for (a, b) in [(1, 1), (2, -1), (3, 5), (4, 0)] {
            conn.execute(&format!("INSERT INTO t VALUES({a}, {b});"))
                .await
                .unwrap();
        }
        assert_eq!(table_count(&conn).await, 4);

        // Before the fix this returned Err("no query solution"); stock empties t.
        conn.execute("DELETE FROM t INDEXED BY pi;")
            .await
            .expect("bare DELETE INDEXED BY partial index must truncate like stock");
        assert_eq!(table_count(&conn).await, 0);

        // rusqlite oracle: stock accepts and empties the table too.
        let oracle = rusqlite::Connection::open_in_memory().unwrap();
        oracle
            .execute_batch(
                "CREATE TABLE t(a INTEGER, b INTEGER);
                 CREATE INDEX pi ON t(a) WHERE b > 0;
                 INSERT INTO t VALUES(1,1),(2,-1),(3,5),(4,0);
                 DELETE FROM t INDEXED BY pi;",
            )
            .expect("stock accepts bare DELETE INDEXED BY partial index");
        let n: i64 = oracle
            .query_row("SELECT count(*) FROM t;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "stock must also empty the table");
    });
}

/// A DELETE whose forced partial index IS covered by the WHERE still works
/// (the truncate opt does not apply once a WHERE is present).
#[test]
fn delete_indexed_by_partial_with_covering_where_still_deletes() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a INTEGER, b INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX pi ON t(a) WHERE b > 0;")
            .await
            .unwrap();
        for (a, b) in [(1, 1), (2, -1), (3, 5)] {
            conn.execute(&format!("INSERT INTO t VALUES({a}, {b});"))
                .await
                .unwrap();
        }
        // WHERE b > 0 implies the partial predicate, so the gate is satisfied.
        conn.execute("DELETE FROM t INDEXED BY pi WHERE b > 0 AND a = 1;")
            .await
            .expect("covered forced partial index DELETE must be accepted");
        assert_eq!(table_count(&conn).await, 2);
    });
}

/// With a BEFORE DELETE trigger the truncate opt is disabled, so stock opens the
/// index and rejects the uncovered forced partial index. frank must keep erroring
/// there (the gate is not skipped).
#[test]
fn bare_delete_indexed_by_partial_with_trigger_still_errors() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a INTEGER, b INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX pi ON t(a) WHERE b > 0;")
            .await
            .unwrap();
        conn.execute("CREATE TABLE log(x);").await.unwrap();
        conn.execute(
            "CREATE TRIGGER trg BEFORE DELETE ON t BEGIN INSERT INTO log VALUES(OLD.a); END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 1);").await.unwrap();

        // Truncate opt disabled by the trigger -> the uncovered forced partial
        // index must still be rejected (matches stock).
        let err = conn.execute("DELETE FROM t INDEXED BY pi;").await;
        assert!(
            err.is_err(),
            "DELETE INDEXED BY uncovered partial index with a trigger must error like stock"
        );
    });
}
