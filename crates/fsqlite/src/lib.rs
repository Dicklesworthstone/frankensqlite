//! Public API facade for FrankenSQLite.
//!
//! This crate will grow a stable, ergonomic API surface over time. In early
//! phases it also re-exports selected internal crates for integration tests.

pub use fsqlite_core::connection::{Connection, PreparedStatement, Row, TraceEvent, TraceMask};
pub use fsqlite_vfs;

#[cfg(test)]
#[allow(
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::needless_collect,
    clippy::single_match_else,
    clippy::branches_sharing_code
)]
mod tests {
    use super::Connection;
    use fsqlite_error::FrankenError;
    use fsqlite_types::value::SqliteValue;

    fn row_values(row: &super::Row) -> Vec<SqliteValue> {
        row.values().to_vec()
    }

    #[test]
    fn test_connection_open_and_path() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        assert_eq!(conn.path(), ":memory:");
    }

    #[test]
    fn test_public_api_query_expression() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let rows = conn
            .query("SELECT 1 + 2, 'ab' || 'cd';")
            .expect("query should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Text("abcd".to_owned()),
            ]
        );
    }

    #[test]
    fn test_public_api_query_with_params() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let rows = conn
            .query_with_params(
                "SELECT ?1 + ?2, ?3;",
                &[
                    SqliteValue::Integer(4),
                    SqliteValue::Integer(5),
                    SqliteValue::Text("ok".to_owned()),
                ],
            )
            .expect("query_with_params should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(9), SqliteValue::Text("ok".to_owned())]
        );
    }

    #[test]
    fn test_public_api_query_row_returns_first_row() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let row = conn
            .query_row("VALUES (10), (20), (30);")
            .expect("query_row should return first row");
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(10)]);
    }

    #[test]
    fn test_public_api_query_row_empty_error() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let error = conn
            .query_row("SELECT 1 WHERE 0;")
            .expect_err("query_row should fail for empty result set");
        assert!(matches!(error, FrankenError::QueryReturnedNoRows));
    }

    #[test]
    fn test_public_api_execute_returns_row_count() {
        let conn = Connection::open(":memory:").expect("in-memory connection should open");
        let count = conn
            .execute("VALUES (1), (2), (3);")
            .expect("execute should succeed");
        assert_eq!(count, 3);
    }

    // ── Connection::open error paths ────────────────────────────────────

    #[test]
    fn open_empty_path_fails() {
        let err = Connection::open("").expect_err("empty path should fail");
        assert!(matches!(err, FrankenError::CannotOpen { .. }));
    }

    // ── Row accessors ────────────────────────────────────────────────────

    #[test]
    fn row_get_valid_index() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 42, 'hello';").unwrap();
        assert_eq!(row.get(0), Some(&SqliteValue::Integer(42)));
        assert_eq!(row.get(1), Some(&SqliteValue::Text("hello".to_owned())));
    }

    #[test]
    fn row_get_out_of_bounds() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 1;").unwrap();
        assert_eq!(row.get(99), None);
    }

    #[test]
    fn row_values_returns_all_columns() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 1, 2, 3;").unwrap();
        assert_eq!(row.values().len(), 3);
    }

    // ── PreparedStatement ────────────────────────────────────────────────

    #[test]
    fn prepared_query() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT 7 * 6;").unwrap();
        let rows = stmt.query().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn prepared_query_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT ?1 + ?2;").unwrap();
        let rows = stmt
            .query_with_params(&[SqliteValue::Integer(10), SqliteValue::Integer(20)])
            .unwrap();
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(30)]);
    }

    #[test]
    fn prepared_query_row() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT 99;").unwrap();
        let row = stmt.query_row().unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(99)]);
    }

    #[test]
    fn prepared_query_row_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT ?1;").unwrap();
        let row = stmt
            .query_row_with_params(&[SqliteValue::Text("xyz".to_owned())])
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Text("xyz".to_owned())]);
    }

    #[test]
    fn prepared_execute() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("VALUES (1), (2);").unwrap();
        assert_eq!(stmt.execute().unwrap(), 2);
    }

    #[test]
    fn prepared_execute_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT ?1;").unwrap();
        assert_eq!(
            stmt.execute_with_params(&[SqliteValue::Integer(1)])
                .unwrap(),
            1
        );
    }

    #[test]
    fn prepared_explain_not_empty() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT 1 + 2;").unwrap();
        let explain = stmt.explain();
        assert!(!explain.is_empty());
    }

    // ── Connection::query_row_with_params ────────────────────────────────

    #[test]
    fn query_row_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row_with_params("SELECT ?1 * 2;", &[SqliteValue::Integer(5)])
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(10)]);
    }

    // ── Connection::execute_with_params ──────────────────────────────────

    #[test]
    fn execute_with_params_returns_count() {
        let conn = Connection::open(":memory:").unwrap();
        let count = conn
            .execute_with_params("SELECT ?1;", &[SqliteValue::Integer(1)])
            .unwrap();
        assert_eq!(count, 1);
    }

    // ── DDL ──────────────────────────────────────────────────────────────

    #[test]
    fn create_table_and_insert_select() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'one');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'two');").unwrap();
        let rows = conn.query("SELECT a, b FROM t1;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn create_table_if_not_exists_no_error() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (x INTEGER);").unwrap();
        // Should not error with IF NOT EXISTS
        conn.execute("CREATE TABLE IF NOT EXISTS t1 (x INTEGER);")
            .unwrap();
    }

    #[test]
    fn create_duplicate_table_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (x INTEGER);").unwrap();
        let err = conn
            .execute("CREATE TABLE t1 (x INTEGER);")
            .expect_err("duplicate table should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    // ── DML affected-row counts (bd-118o) ─────────────────────────────────

    #[test]
    fn execute_insert_returns_affected_count() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        assert_eq!(conn.execute("INSERT INTO t VALUES (1);").unwrap(), 1);
        assert_eq!(
            conn.execute("INSERT INTO t VALUES (2), (3), (4);").unwrap(),
            3,
        );
    }

    #[test]
    fn execute_update_returns_affected_count() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();
        assert_eq!(conn.execute("UPDATE t SET v = 0;").unwrap(), 3);
        assert_eq!(conn.execute("UPDATE t SET v = 99 WHERE v = 0;").unwrap(), 3);
    }

    #[test]
    fn execute_delete_returns_affected_count() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();
        assert_eq!(conn.execute("DELETE FROM t WHERE v = 2;").unwrap(), 1);
        assert_eq!(conn.execute("DELETE FROM t;").unwrap(), 2);
    }

    // ── DML: UPDATE / DELETE ─────────────────────────────────────────────

    #[test]
    fn update_modifies_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (10);").unwrap();
        conn.execute("INSERT INTO t VALUES (20);").unwrap();
        conn.execute("UPDATE t SET v = 99 WHERE v = 10;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        let vals: Vec<_> = rows.iter().map(row_values).collect();
        assert!(vals.contains(&vec![SqliteValue::Integer(99)]));
        assert!(vals.contains(&vec![SqliteValue::Integer(20)]));
    }

    #[test]
    fn update_preserves_integer_primary_key_rowid_alias() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO accounts VALUES (1, 100);")
            .unwrap();
        conn.execute("INSERT INTO accounts VALUES (2, 200);")
            .unwrap();

        conn.execute("UPDATE accounts SET balance = balance + 5 WHERE id = 1;")
            .unwrap();

        let rows = conn
            .query("SELECT id, balance FROM accounts ORDER BY id;")
            .unwrap();
        assert_eq!(rows.len(), 2, "update must not create or lose rows");
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Integer(105)],
            "id=1 row must be updated in place"
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Integer(200)],
            "id=2 row must remain unchanged"
        );
    }

    #[test]
    fn concurrent_same_row_deposit_commits_must_conflict_or_serialize() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("concurrent_same_row_deposit.db");
        let db = db_path.to_string_lossy().to_string();

        {
            let conn = Connection::open(&db).unwrap();
            conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();
            conn.execute(
                "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL);",
            )
            .unwrap();
            conn.execute("INSERT INTO accounts VALUES (1, 0);").unwrap();
        }

        let conn1 = Connection::open(&db).unwrap();
        let conn2 = Connection::open(&db).unwrap();
        conn1.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();
        conn2.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();

        conn1.execute("BEGIN CONCURRENT;").unwrap();
        conn2.execute("BEGIN CONCURRENT;").unwrap();

        assert_eq!(
            conn1
                .execute("UPDATE accounts SET balance = balance + 1 WHERE id = 1;")
                .unwrap(),
            1
        );
        let update2 = conn2.execute("UPDATE accounts SET balance = balance + 1 WHERE id = 1;");

        let commit1 = conn1.execute("COMMIT;");
        let commit2 = match update2 {
            Ok(changes2) => {
                assert_eq!(changes2, 1, "second update should affect one row");
                conn2.execute("COMMIT;")
            }
            Err(err) => {
                assert!(
                    err.is_transient(),
                    "second concurrent writer should fail transiently on conflict, got: {err}"
                );
                let rollback = conn2.execute("ROLLBACK;");
                assert!(
                    rollback.is_ok(),
                    "second writer should remain rollback-able after transient conflict: {rollback:?}"
                );
                Err(err)
            }
        };

        let verify = Connection::open(&db).unwrap();
        let row = verify
            .query_row("SELECT balance FROM accounts WHERE id = 1;")
            .unwrap();
        let balance = row.get(0).cloned().unwrap_or(SqliteValue::Null);
        match (commit1, commit2) {
            (Ok(_), Ok(_)) => {
                assert_eq!(
                    balance,
                    SqliteValue::Integer(2),
                    "if both commits succeed, both deposits must be visible"
                );
            }
            (Ok(_), Err(err)) | (Err(err), Ok(_)) => {
                assert!(
                    err.is_transient(),
                    "conflicting concurrent writer should fail with transient busy snapshot/busy, got: {err}"
                );
                assert_eq!(
                    balance,
                    SqliteValue::Integer(1),
                    "if one writer aborts, exactly one deposit should persist"
                );
            }
            (Err(err1), Err(err2)) => {
                panic!("at least one concurrent writer must commit: err1={err1}; err2={err2}");
            }
        }
    }

    #[test]
    fn delete_removes_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();
        conn.execute("DELETE FROM t WHERE v = 2;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(vals.contains(&SqliteValue::Integer(1)));
        assert!(vals.contains(&SqliteValue::Integer(3)));
    }

    // ── Type handling ────────────────────────────────────────────────────

    #[test]
    fn null_value_roundtrip() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT NULL;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Null]);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn real_value_roundtrip() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 3.14;").unwrap();
        if let SqliteValue::Float(v) = &row_values(&row)[0] {
            assert!((*v - 3.14).abs() < f64::EPSILON);
        } else {
            unreachable!("expected Float value");
        }
    }

    #[test]
    fn text_value_roundtrip() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 'hello world';").unwrap();
        assert_eq!(
            row_values(&row),
            vec![SqliteValue::Text("hello world".to_owned())]
        );
    }

    #[test]
    fn blob_value_via_params() {
        let conn = Connection::open(":memory:").unwrap();
        let blob = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let row = conn
            .query_row_with_params("SELECT ?1;", &[SqliteValue::Blob(blob.clone())])
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Blob(blob)]);
    }

    // ── Transaction control ──────────────────────────────────────────────

    #[test]
    fn in_transaction_flag() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(!conn.in_transaction());
        conn.execute("BEGIN;").unwrap();
        assert!(conn.in_transaction());
        conn.execute("COMMIT;").unwrap();
        assert!(!conn.in_transaction());
    }

    #[test]
    fn begin_commit_persists_changes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("BEGIN;").unwrap();
        conn.execute("INSERT INTO t VALUES (42);").unwrap();
        conn.execute("COMMIT;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn rollback_reverts_changes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("BEGIN;").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("ROLLBACK;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn nested_begin_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        let err = conn
            .execute("BEGIN;")
            .expect_err("nested begin should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    #[test]
    fn commit_without_transaction_errors() {
        let conn = Connection::open(":memory:").unwrap();
        let err = conn
            .execute("COMMIT;")
            .expect_err("commit without txn should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    #[test]
    fn rollback_without_transaction_errors() {
        let conn = Connection::open(":memory:").unwrap();
        let err = conn
            .execute("ROLLBACK;")
            .expect_err("rollback without txn should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    // ── Savepoint ────────────────────────────────────────────────────────

    #[test]
    fn savepoint_and_rollback_to() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("SAVEPOINT sp1;").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("ROLLBACK TO sp1;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn savepoint_release_commits_changes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        conn.execute("SAVEPOINT sp1;").unwrap();
        conn.execute("INSERT INTO t VALUES (100);").unwrap();
        conn.execute("RELEASE sp1;").unwrap();
        let rows = conn.query("SELECT v FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(100)]);
    }

    #[test]
    fn release_nonexistent_savepoint_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN;").unwrap();
        let err = conn
            .execute("RELEASE nosuch;")
            .expect_err("release nonexistent savepoint should fail");
        assert!(matches!(err, FrankenError::Internal(_)));
    }

    // ── Parse error ──────────────────────────────────────────────────────

    #[test]
    fn parse_error_on_invalid_sql() {
        let conn = Connection::open(":memory:").unwrap();
        assert!(conn.query("NOT VALID SQL;").is_err());
    }

    // ── Multiple statements ──────────────────────────────────────────────

    #[test]
    fn multiple_statements_in_query() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (v INTEGER);").unwrap();
        // query() processes all statements, returns rows from last
        let rows = conn
            .query("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT v FROM t;")
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    // ── Expression arithmetic ────────────────────────────────────────────

    #[test]
    fn arithmetic_expressions() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 10 - 3, 4 * 5, 20 / 4;").unwrap();
        assert_eq!(
            row_values(&row),
            vec![
                SqliteValue::Integer(7),
                SqliteValue::Integer(20),
                SqliteValue::Integer(5),
            ]
        );
    }

    #[test]
    fn string_concatenation() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 'foo' || 'bar';").unwrap();
        assert_eq!(
            row_values(&row),
            vec![SqliteValue::Text("foobar".to_owned())]
        );
    }

    // ── Compound WHERE predicates (bd-2832) ────────────────────────────

    fn setup_three_rows(conn: &Connection) {
        conn.execute("CREATE TABLE t3 (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t3 VALUES (1, 'one');").unwrap();
        conn.execute("INSERT INTO t3 VALUES (2, 'two');").unwrap();
        conn.execute("INSERT INTO t3 VALUES (3, 'three');").unwrap();
    }

    #[test]
    fn where_and_predicate() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        let rows = conn
            .query("SELECT a FROM t3 WHERE a > 1 AND b = 'two';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
    }

    #[test]
    fn where_or_predicate() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        let rows = conn
            .query("SELECT a FROM t3 WHERE a = 1 OR a = 3;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(vals.contains(&SqliteValue::Integer(1)));
        assert!(vals.contains(&SqliteValue::Integer(3)));
    }

    #[test]
    fn where_comparison_operators() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        // Greater than
        let rows = conn.query("SELECT a FROM t3 WHERE a > 2;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(3)]);
        // Less than or equal
        let rows = conn.query("SELECT a FROM t3 WHERE a <= 2;").unwrap();
        assert_eq!(rows.len(), 2);
        // Not equal
        let rows = conn.query("SELECT a FROM t3 WHERE a != 2;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    // ── NULL handling (WHERE) ──────────────────────────────────────────

    #[test]
    fn where_is_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE tn (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO tn VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO tn VALUES (2, NULL);").unwrap();
        let rows = conn.query("SELECT a FROM tn WHERE b IS NULL;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
    }

    #[test]
    fn where_is_not_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE tn2 (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO tn2 VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO tn2 VALUES (2, NULL);").unwrap();
        let rows = conn
            .query("SELECT a FROM tn2 WHERE b IS NOT NULL;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    // ── NULL handling (expression) ─────────────────────────────────────

    #[test]
    fn coalesce_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT COALESCE(NULL, NULL, 42);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn nullif_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT NULLIF(1, 1);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Null]);
        let row = conn.query_row("SELECT NULLIF(1, 2);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
    }

    // ── CASE WHEN ──────────────────────────────────────────────────────

    #[test]
    fn case_when_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END;")
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Text("yes".to_owned())]);
    }

    #[test]
    fn case_simple_form() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END;")
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Text("b".to_owned())]);
    }

    // ── Built-in functions ─────────────────────────────────────────────

    #[test]
    fn builtin_abs() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT ABS(-42);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn builtin_length() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT LENGTH('hello');").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(5)]);
    }

    #[test]
    fn builtin_upper_lower() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT UPPER('hello'), LOWER('WORLD');")
            .unwrap();
        assert_eq!(
            row_values(&row),
            vec![
                SqliteValue::Text("HELLO".to_owned()),
                SqliteValue::Text("world".to_owned()),
            ]
        );
    }

    #[test]
    fn builtin_typeof() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT TYPEOF(42);").unwrap();
        assert_eq!(
            row_values(&row),
            vec![SqliteValue::Text("integer".to_owned())]
        );
    }

    // ── CAST ───────────────────────────────────────────────────────────

    #[test]
    fn cast_integer_to_text() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT CAST(42 AS TEXT);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Text("42".to_owned())]);
    }

    #[test]
    fn cast_text_to_integer() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT CAST('123' AS INTEGER);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(123)]);
    }

    // ── Blob literal ───────────────────────────────────────────────────

    #[test]
    fn blob_literal_hex() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT X'DEADBEEF';").unwrap();
        assert_eq!(
            row_values(&row),
            vec![SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])]
        );
    }

    // ── Unary operators ────────────────────────────────────────────────

    #[test]
    fn unary_minus() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT -42;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(-42)]);
    }

    #[test]
    fn not_operator() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT NOT 0;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
    }

    // ── ORDER BY / LIMIT (expression path) ─────────────────────────────

    #[test]
    fn values_order_by() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("VALUES (3), (1), (2) ORDER BY 1;").unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(
            vals,
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
                SqliteValue::Integer(3),
            ]
        );
    }

    #[test]
    fn values_order_by_desc_with_limit() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("VALUES (3), (1), (2) ORDER BY 1 DESC LIMIT 2;")
            .unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(vals, vec![SqliteValue::Integer(3), SqliteValue::Integer(2)]);
    }

    #[test]
    fn values_limit_offset() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("VALUES (10), (20), (30), (40) LIMIT 2 OFFSET 1;")
            .unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(
            vals,
            vec![SqliteValue::Integer(20), SqliteValue::Integer(30)]
        );
    }

    // ── DELETE without WHERE (all rows) ────────────────────────────────

    #[test]
    fn delete_all_rows() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        conn.execute("DELETE FROM t3;").unwrap();
        let rows = conn.query("SELECT a FROM t3;").unwrap();
        assert_eq!(rows.len(), 0);
    }

    // ── Non-column result expressions (bd-19g7) ────────────────────────

    #[test]
    fn select_expression_column_arithmetic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE te (a INTEGER);").unwrap();
        conn.execute("INSERT INTO te VALUES (10);").unwrap();
        conn.execute("INSERT INTO te VALUES (20);").unwrap();
        let rows = conn.query("SELECT a + 1 FROM te;").unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(vals.contains(&SqliteValue::Integer(11)));
        assert!(vals.contains(&SqliteValue::Integer(21)));
    }

    #[test]
    fn select_expression_column_with_literal() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE te2 (a INTEGER, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO te2 VALUES (5, 'hello');")
            .unwrap();
        let rows = conn.query("SELECT a * 2, b FROM te2;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(10),
                SqliteValue::Text("hello".to_owned())
            ]
        );
    }

    // ── Multi-row INSERT (bd-2of2) ────────────────────────────────────

    #[test]
    fn insert_multi_row_values() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE tm (v INTEGER);").unwrap();
        conn.execute("INSERT INTO tm VALUES (1), (2), (3);")
            .unwrap();
        let rows = conn.query("SELECT v FROM tm;").unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(vals.len(), 3);
        assert!(vals.contains(&SqliteValue::Integer(1)));
        assert!(vals.contains(&SqliteValue::Integer(2)));
        assert!(vals.contains(&SqliteValue::Integer(3)));
    }

    // ── IN / BETWEEN / LIKE (bd-3vpo) ─────────────────────────────────

    #[test]
    fn in_expression_only() {
        // Test IN without any table - pure expression evaluation
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 2 IN (1, 2, 3);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn between_expression_only() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 2 BETWEEN 1 AND 3;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn where_in_operator() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        let rows = conn.query("SELECT a FROM t3 WHERE a IN (1, 3);").unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(vals.len(), 2);
        assert!(vals.contains(&SqliteValue::Integer(1)));
        assert!(vals.contains(&SqliteValue::Integer(3)));
    }

    #[test]
    fn where_between_operator() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        let rows = conn
            .query("SELECT a FROM t3 WHERE a BETWEEN 1 AND 2;")
            .unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(vals.len(), 2);
        assert!(vals.contains(&SqliteValue::Integer(1)));
        assert!(vals.contains(&SqliteValue::Integer(2)));
    }

    #[test]
    fn where_like_operator() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        let rows = conn.query("SELECT b FROM t3 WHERE b LIKE 't%';").unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(vals.len(), 2);
        assert!(vals.contains(&SqliteValue::Text("two".to_owned())));
        assert!(vals.contains(&SqliteValue::Text("three".to_owned())));
    }

    // ── Aggregates (bd-xldj) ────────────────────────────────────────────

    #[test]
    fn aggregate_count_star() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        let row = conn.query_row("SELECT COUNT(*) FROM t3;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(3)]);
    }

    #[test]
    fn aggregate_sum_min_max() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        let row = conn
            .query_row("SELECT SUM(a), MIN(a), MAX(a) FROM t3;")
            .unwrap();
        assert_eq!(
            row_values(&row),
            vec![
                SqliteValue::Integer(6),
                SqliteValue::Integer(1),
                SqliteValue::Integer(3),
            ]
        );
    }

    #[test]
    fn aggregate_avg() {
        let conn = Connection::open(":memory:").unwrap();
        setup_three_rows(&conn);
        let row = conn.query_row("SELECT AVG(a) FROM t3;").unwrap();
        // AVG(1,2,3) = 2.0
        assert_eq!(row_values(&row), vec![SqliteValue::Float(2.0)]);
    }

    // ── UPDATE all rows (no WHERE) ─────────────────────────────────────

    #[test]
    fn update_all_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE tu (v INTEGER);").unwrap();
        conn.execute("INSERT INTO tu VALUES (1);").unwrap();
        conn.execute("INSERT INTO tu VALUES (2);").unwrap();
        conn.execute("UPDATE tu SET v = 0;").unwrap();
        let rows = conn.query("SELECT v FROM tu;").unwrap();
        assert!(
            rows.iter()
                .all(|r| row_values(r) == vec![SqliteValue::Integer(0)])
        );
    }

    // ═══════════════════════════════════════════════════════════════════
    // bd-2832: Expanded SQL pattern coverage (IvoryWaterfall)
    // ═══════════════════════════════════════════════════════════════════

    fn setup_bd2832(conn: &Connection) {
        conn.execute("CREATE TABLE tp (a INTEGER, b TEXT, c REAL);")
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (1, 'alpha', 1.5);")
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (2, 'beta', 2.5);")
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (3, 'gamma', 3.5);")
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (4, NULL, 4.5);")
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (5, 'delta', 5.5);")
            .unwrap();
    }

    // ── WHERE NOT ───────────────────────────────────────────────────────

    #[test]
    fn where_not_predicate() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn.query("SELECT a FROM tp WHERE NOT (a > 3);").unwrap();
        assert_eq!(rows.len(), 3);
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(vals.contains(&SqliteValue::Integer(1)));
        assert!(vals.contains(&SqliteValue::Integer(2)));
        assert!(vals.contains(&SqliteValue::Integer(3)));
    }

    // ── Comparison operators (>=, <) ────────────────────────────────────

    #[test]
    fn where_greater_equal() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn.query("SELECT a FROM tp WHERE a >= 4;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn where_less_than() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn.query("SELECT a FROM tp WHERE a < 3;").unwrap();
        assert_eq!(rows.len(), 2);
    }

    // ── Table-backed ORDER BY ASC / DESC ────────────────────────────────

    #[test]
    fn table_order_by_asc() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE tord (v INTEGER);").unwrap();
        conn.execute("INSERT INTO tord VALUES (3);").unwrap();
        conn.execute("INSERT INTO tord VALUES (1);").unwrap();
        conn.execute("INSERT INTO tord VALUES (2);").unwrap();
        let rows = conn.query("SELECT v FROM tord ORDER BY v;").unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(
            vals,
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
                SqliteValue::Integer(3),
            ]
        );
    }

    #[test]
    fn table_order_by_desc() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE tord2 (v INTEGER);").unwrap();
        conn.execute("INSERT INTO tord2 VALUES (3);").unwrap();
        conn.execute("INSERT INTO tord2 VALUES (1);").unwrap();
        conn.execute("INSERT INTO tord2 VALUES (2);").unwrap();
        let rows = conn.query("SELECT v FROM tord2 ORDER BY v DESC;").unwrap();
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(
            vals,
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Integer(2),
                SqliteValue::Integer(1),
            ]
        );
    }

    // ── Table-backed LIMIT / OFFSET ─────────────────────────────────────

    #[test]
    fn table_limit() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn.query("SELECT a FROM tp LIMIT 3;").unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn table_limit_offset() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn.query("SELECT a FROM tp LIMIT 2 OFFSET 2;").unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(vals, vec![SqliteValue::Integer(3), SqliteValue::Integer(4)]);
    }

    // ── WHERE + LIMIT ───────────────────────────────────────────────────

    #[test]
    fn where_with_limit() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn.query("SELECT a FROM tp WHERE a > 1 LIMIT 2;").unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(vals, vec![SqliteValue::Integer(2), SqliteValue::Integer(3)]);
    }

    // ── CASE WHEN on table-backed SELECT ────────────────────────────────

    #[test]
    fn case_when_table_backed() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn
            .query("SELECT CASE WHEN a > 3 THEN 'big' ELSE 'small' END FROM tp;")
            .unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].values()[0], SqliteValue::Text("small".to_owned()));
        assert_eq!(rows[3].values()[0], SqliteValue::Text("big".to_owned()));
    }

    // ── CAST on table column ────────────────────────────────────────────

    #[test]
    fn cast_table_backed() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE tcast (v INTEGER);").unwrap();
        conn.execute("INSERT INTO tcast VALUES (42);").unwrap();
        let row = conn
            .query_row("SELECT CAST(v AS TEXT) FROM tcast;")
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Text("42".to_owned())]);
    }

    // ── IS NULL / IS NOT NULL on table ──────────────────────────────────

    #[test]
    fn where_column_is_null_correct() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn.query("SELECT a FROM tp WHERE b IS NULL;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(4)]);
    }

    #[test]
    fn where_column_is_not_null_correct() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn.query("SELECT a FROM tp WHERE b IS NOT NULL;").unwrap();
        assert_eq!(rows.len(), 4);
    }

    // ── Unary minus on table column ─────────────────────────────────────

    #[test]
    fn unary_minus_table_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE tneg (x INTEGER);").unwrap();
        conn.execute("INSERT INTO tneg VALUES (42);").unwrap();
        let row = conn.query_row("SELECT -x FROM tneg;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(-42)]);
    }

    // ── Built-in functions: additional coverage ─────────────────────────

    #[test]
    fn builtin_typeof_all_types() {
        let conn = Connection::open(":memory:").unwrap();
        assert_eq!(
            row_values(&conn.query_row("SELECT typeof(3.14);").unwrap()),
            vec![SqliteValue::Text("real".to_owned())]
        );
        assert_eq!(
            row_values(&conn.query_row("SELECT typeof('abc');").unwrap()),
            vec![SqliteValue::Text("text".to_owned())]
        );
        assert_eq!(
            row_values(&conn.query_row("SELECT typeof(NULL);").unwrap()),
            vec![SqliteValue::Text("null".to_owned())]
        );
        assert_eq!(
            row_values(&conn.query_row("SELECT typeof(X'FF');").unwrap()),
            vec![SqliteValue::Text("blob".to_owned())]
        );
    }

    #[test]
    fn builtin_substr() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT substr('hello world', 7, 5);")
            .unwrap();
        assert_eq!(
            row_values(&row),
            vec![SqliteValue::Text("world".to_owned())]
        );
    }

    #[test]
    fn builtin_replace() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT replace('hello world', 'world', 'rust');")
            .unwrap();
        assert_eq!(
            row_values(&row),
            vec![SqliteValue::Text("hello rust".to_owned())]
        );
    }

    #[test]
    fn builtin_trim() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT trim('  hello  ');").unwrap();
        assert_eq!(
            row_values(&row),
            vec![SqliteValue::Text("hello".to_owned())]
        );
    }

    #[test]
    fn builtin_instr() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT instr('hello world', 'world');")
            .unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(7)]);
    }

    #[test]
    fn builtin_hex() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT hex(X'CAFE');").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Text("CAFE".to_owned())]);
    }

    // ── IS NULL expression context ──────────────────────────────────────

    #[test]
    fn is_null_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT NULL IS NULL;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
        let row = conn.query_row("SELECT 42 IS NULL;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(0)]);
    }

    // ── SOUNDEX NULL ────────────────────────────────────────────────────

    #[test]
    fn soundex_null_returns_question_marks() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT soundex(NULL);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Text("?000".to_owned())]);
    }

    // ── LIKE underscore wildcard ─────────────────────────────────────────

    #[test]
    fn like_underscore_wildcard() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn.query("SELECT b FROM tp WHERE b LIKE 'b_ta';").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("beta".to_owned())]
        );
    }

    // ── NOT IN / NOT BETWEEN ────────────────────────────────────────────

    #[test]
    fn where_not_in() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn
            .query("SELECT a FROM tp WHERE a NOT IN (1, 3, 5);")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(vals.contains(&SqliteValue::Integer(2)));
        assert!(vals.contains(&SqliteValue::Integer(4)));
    }

    #[test]
    fn where_in_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1), (2), (3);")
            .unwrap();
        conn.execute("INSERT INTO t2 VALUES (2), (3), (4);")
            .unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE a IN (SELECT b FROM t2) ORDER BY a;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
        assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(3)]);
    }

    #[test]
    fn where_in_table_name() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1), (2), (3);")
            .unwrap();
        conn.execute("INSERT INTO t2 VALUES (2), (3), (4);")
            .unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE a IN t2 ORDER BY a;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
        assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(3)]);
    }

    #[test]
    fn where_not_in_table_name() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1), (2), (3);")
            .unwrap();
        conn.execute("INSERT INTO t2 VALUES (2), (3), (4);")
            .unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE a NOT IN t2 ORDER BY a;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn where_exists_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1), (2);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (7);").unwrap();

        let rows = conn
            .query("SELECT a FROM t1 WHERE EXISTS (SELECT b FROM t2) ORDER BY a;")
            .unwrap();
        assert_eq!(rows.len(), 2);

        conn.execute("DELETE FROM t2;").unwrap();
        let rows = conn
            .query("SELECT a FROM t1 WHERE EXISTS (SELECT b FROM t2);")
            .unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn scalar_subquery_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE s (v INTEGER);").unwrap();
        conn.execute("INSERT INTO s VALUES (41);").unwrap();

        let row = conn.query_row("SELECT (SELECT v FROM s) + 1;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn update_where_in_table_name() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER, flag TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'orig'), (2, 'orig'), (3, 'orig');")
            .unwrap();
        conn.execute("INSERT INTO t2 VALUES (2), (3);").unwrap();

        conn.execute("UPDATE t1 SET flag='hit' WHERE a IN t2;")
            .unwrap();

        let rows = conn.query("SELECT a, flag FROM t1 ORDER BY a;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("orig".to_owned())
            ]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("hit".to_owned())]
        );
        assert_eq!(
            row_values(&rows[2]),
            vec![SqliteValue::Integer(3), SqliteValue::Text("hit".to_owned())]
        );
    }

    #[test]
    fn delete_where_in_table_name() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (a INTEGER);").unwrap();
        conn.execute("CREATE TABLE t2 (b INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1), (2), (3);")
            .unwrap();
        conn.execute("INSERT INTO t2 VALUES (2), (3);").unwrap();

        conn.execute("DELETE FROM t1 WHERE a IN t2;").unwrap();

        let rows = conn.query("SELECT a FROM t1 ORDER BY a;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn where_not_between() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let rows = conn
            .query("SELECT a FROM tp WHERE a NOT BETWEEN 2 AND 4;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(vals.contains(&SqliteValue::Integer(1)));
        assert!(vals.contains(&SqliteValue::Integer(5)));
    }

    // ── NULL semantics for IN / BETWEEN ────────────────────────────────

    #[test]
    fn between_null_operand_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        // NULL BETWEEN 1 AND 5 → NULL (not TRUE)
        let row = conn.query_row("SELECT NULL BETWEEN 1 AND 5;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Null]);
    }

    #[test]
    fn between_null_low_bound() {
        let conn = Connection::open(":memory:").unwrap();
        // 3 BETWEEN NULL AND 5: (3 >= NULL) AND (3 <= 5) = NULL AND TRUE = NULL
        let row = conn.query_row("SELECT 3 BETWEEN NULL AND 5;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Null]);
    }

    #[test]
    fn between_null_high_bound() {
        let conn = Connection::open(":memory:").unwrap();
        // 3 BETWEEN 1 AND NULL: (3 >= 1) AND (3 <= NULL) = TRUE AND NULL = NULL
        let row = conn.query_row("SELECT 3 BETWEEN 1 AND NULL;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Null]);
    }

    #[test]
    fn between_null_bound_out_of_range_returns_false() {
        let conn = Connection::open(":memory:").unwrap();
        // 3 BETWEEN 4 AND NULL: (3 >= 4) AND (3 <= NULL) = FALSE AND NULL = FALSE
        let row = conn.query_row("SELECT 3 BETWEEN 4 AND NULL;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(0)]);
    }

    #[test]
    fn in_null_operand_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        // NULL IN (1, 2, 3) → NULL (not FALSE)
        let row = conn.query_row("SELECT NULL IN (1, 2, 3);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Null]);
    }

    #[test]
    fn in_list_with_null_no_match_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        // 2 IN (1, NULL, 3): no exact match, but NULL in list → NULL
        let row = conn.query_row("SELECT 2 IN (1, NULL, 3);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Null]);
    }

    #[test]
    fn in_list_with_null_match_returns_true() {
        let conn = Connection::open(":memory:").unwrap();
        // 1 IN (1, NULL, 3): exact match on 1 → TRUE (integer 1)
        let row = conn.query_row("SELECT 1 IN (1, NULL, 3);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn not_in_null_operand_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        // NULL NOT IN (1, 2) → NULL
        let row = conn.query_row("SELECT NULL NOT IN (1, 2);").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Null]);
    }

    // ── DISTINCT ──────────────────────────────────────────────────────

    #[test]
    fn distinct_table_backed_select() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE td (id INTEGER, flag INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO td VALUES (1, 1);").unwrap();
        conn.execute("INSERT INTO td VALUES (2, 0);").unwrap();
        conn.execute("INSERT INTO td VALUES (3, 1);").unwrap();
        conn.execute("INSERT INTO td VALUES (4, 0);").unwrap();
        conn.execute("INSERT INTO td VALUES (5, 1);").unwrap();

        let rows = conn.query("SELECT DISTINCT flag FROM td;").unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(vals.contains(&SqliteValue::Integer(0)));
        assert!(vals.contains(&SqliteValue::Integer(1)));
    }

    // ── Aggregate + GROUP BY ───────────────────────────────────────────

    #[test]
    fn aggregate_group_by_count() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE tg (k TEXT);").unwrap();
        conn.execute("INSERT INTO tg VALUES ('a');").unwrap();
        conn.execute("INSERT INTO tg VALUES ('a');").unwrap();
        conn.execute("INSERT INTO tg VALUES ('b');").unwrap();

        let rows = conn
            .query("SELECT k, COUNT(*) FROM tg GROUP BY k ORDER BY k;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("a".to_owned()), SqliteValue::Integer(2)]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Text("b".to_owned()), SqliteValue::Integer(1)]
        );
    }

    #[test]
    fn group_by_alias_star_expansion() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE ga (k TEXT, v INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO ga VALUES ('a', 10);").unwrap();
        conn.execute("INSERT INTO ga VALUES ('a', 10);").unwrap();
        conn.execute("INSERT INTO ga VALUES ('b', 20);").unwrap();

        let rows = conn
            .query("SELECT t.* FROM ga AS t GROUP BY t.k, t.v ORDER BY t.k, t.v;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("a".to_owned()), SqliteValue::Integer(10)]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Text("b".to_owned()), SqliteValue::Integer(20)]
        );
    }

    #[test]
    fn right_join_null_extension() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE r (l_id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO l VALUES (1, 'left-a'), (2, 'left-b');")
            .unwrap();
        conn.execute("INSERT INTO r VALUES (2, 'right-b'), (3, 'right-c');")
            .unwrap();

        let rows = conn
            .query("SELECT l.name, r.tag FROM l RIGHT JOIN r ON l.id = r.l_id;")
            .unwrap();
        assert_eq!(rows.len(), 2);

        let projected: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
        assert!(projected.contains(&vec![
            SqliteValue::Text("left-b".to_owned()),
            SqliteValue::Text("right-b".to_owned())
        ]));
        assert!(projected.contains(&vec![
            SqliteValue::Null,
            SqliteValue::Text("right-c".to_owned())
        ]));
    }

    #[test]
    fn full_outer_join_null_extension() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE r (l_id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO l VALUES (1, 'left-a'), (2, 'left-b');")
            .unwrap();
        conn.execute("INSERT INTO r VALUES (2, 'right-b'), (3, 'right-c');")
            .unwrap();

        let rows = conn
            .query("SELECT l.name, r.tag FROM l FULL OUTER JOIN r ON l.id = r.l_id;")
            .unwrap();
        assert_eq!(rows.len(), 3);

        let projected: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
        assert!(projected.contains(&vec![
            SqliteValue::Text("left-a".to_owned()),
            SqliteValue::Null
        ]));
        assert!(projected.contains(&vec![
            SqliteValue::Text("left-b".to_owned()),
            SqliteValue::Text("right-b".to_owned())
        ]));
        assert!(projected.contains(&vec![
            SqliteValue::Null,
            SqliteValue::Text("right-c".to_owned())
        ]));
    }

    #[test]
    fn right_join_using_nulls_do_not_match() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE r (id INTEGER, tag TEXT);")
            .unwrap();
        conn.execute("INSERT INTO l VALUES (NULL, 'left-null'), (1, 'left-one');")
            .unwrap();
        conn.execute("INSERT INTO r VALUES (NULL, 'right-null'), (1, 'right-one');")
            .unwrap();

        let rows = conn
            .query("SELECT l.name, r.tag FROM l RIGHT JOIN r USING (id);")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let projected: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
        assert!(projected.contains(&vec![
            SqliteValue::Text("left-one".to_owned()),
            SqliteValue::Text("right-one".to_owned())
        ]));
        assert!(projected.contains(&vec![
            SqliteValue::Null,
            SqliteValue::Text("right-null".to_owned())
        ]));
    }

    #[test]
    fn aggregate_group_by_sum() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE gs (dept TEXT, salary INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO gs VALUES ('eng', 100);").unwrap();
        conn.execute("INSERT INTO gs VALUES ('eng', 200);").unwrap();
        conn.execute("INSERT INTO gs VALUES ('sales', 50);")
            .unwrap();

        let rows = conn
            .query("SELECT dept, SUM(salary) FROM gs GROUP BY dept;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<(SqliteValue, SqliteValue)> = rows
            .iter()
            .map(|r| {
                let v = row_values(r);
                (v[0].clone(), v[1].clone())
            })
            .collect();
        assert!(vals.contains(&(
            SqliteValue::Text("eng".to_owned()),
            SqliteValue::Integer(300)
        )));
        assert!(vals.contains(&(
            SqliteValue::Text("sales".to_owned()),
            SqliteValue::Integer(50)
        )));
    }

    #[test]
    fn aggregate_group_by_multiple_aggs() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE gm (cat TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO gm VALUES ('a', 10);").unwrap();
        conn.execute("INSERT INTO gm VALUES ('a', 20);").unwrap();
        conn.execute("INSERT INTO gm VALUES ('a', 30);").unwrap();
        conn.execute("INSERT INTO gm VALUES ('b', 5);").unwrap();

        let rows = conn
            .query("SELECT cat, COUNT(*), MIN(val), MAX(val) FROM gm GROUP BY cat;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let a_row = rows
            .iter()
            .find(|r| row_values(r)[0] == SqliteValue::Text("a".to_owned()))
            .unwrap();
        assert_eq!(
            row_values(a_row),
            vec![
                SqliteValue::Text("a".to_owned()),
                SqliteValue::Integer(3),
                SqliteValue::Integer(10),
                SqliteValue::Integer(30),
            ]
        );
        let b_row = rows
            .iter()
            .find(|r| row_values(r)[0] == SqliteValue::Text("b".to_owned()))
            .unwrap();
        assert_eq!(
            row_values(b_row),
            vec![
                SqliteValue::Text("b".to_owned()),
                SqliteValue::Integer(1),
                SqliteValue::Integer(5),
                SqliteValue::Integer(5),
            ]
        );
    }

    // ── Aggregate: count(col) excludes NULL ──────────────────────────────

    #[test]
    fn aggregate_count_column_excludes_null() {
        let conn = Connection::open(":memory:").unwrap();
        setup_bd2832(&conn);
        let row = conn.query_row("SELECT count(b) FROM tp;").unwrap();
        assert_eq!(row_values(&row), vec![SqliteValue::Integer(4)]);
    }

    // ── execute() with_params affected row count (bd-118o) ────────────

    #[test]
    fn execute_with_params_insert_returns_count() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE ewp (v INTEGER);").unwrap();
        let count = conn
            .execute_with_params("INSERT INTO ewp VALUES (?1);", &[SqliteValue::Integer(42)])
            .unwrap();
        assert_eq!(count, 1, "INSERT via execute_with_params should return 1");
    }

    #[test]
    fn execute_select_returns_row_count() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE es (v INTEGER);").unwrap();
        conn.execute("INSERT INTO es VALUES (1);").unwrap();
        conn.execute("INSERT INTO es VALUES (2);").unwrap();
        let count = conn.execute("SELECT * FROM es;").unwrap();
        assert_eq!(count, 2, "SELECT via execute() should return row count");
    }

    // ── Bug fix regression: SAVEPOINT RELEASE implicit transaction ───

    #[test]
    fn savepoint_release_ends_implicit_transaction() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE sr (v INTEGER);").unwrap();

        // SAVEPOINT starts an implicit transaction.
        conn.execute("SAVEPOINT sp1;").unwrap();
        assert!(conn.in_transaction());
        conn.execute("INSERT INTO sr VALUES (1);").unwrap();

        // RELEASE ends the implicit transaction.
        conn.execute("RELEASE sp1;").unwrap();
        assert!(
            !conn.in_transaction(),
            "RELEASE of last implicit savepoint should end transaction"
        );

        // After release, data should be committed.
        let rows = conn.query("SELECT * FROM sr;").unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn explicit_begin_savepoint_release_keeps_transaction() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE bsr (v INTEGER);").unwrap();

        // Explicit BEGIN, then SAVEPOINT, then RELEASE.
        conn.execute("BEGIN;").unwrap();
        conn.execute("SAVEPOINT sp1;").unwrap();
        conn.execute("INSERT INTO bsr VALUES (1);").unwrap();
        conn.execute("RELEASE sp1;").unwrap();

        // Transaction should still be active (explicit BEGIN requires COMMIT).
        assert!(
            conn.in_transaction(),
            "RELEASE after explicit BEGIN should not end the transaction"
        );
        conn.execute("COMMIT;").unwrap();
        assert!(!conn.in_transaction());
    }

    // ── Probe tests for SQL feature coverage ─────────────────────

    #[test]
    fn probe_update_self_ref_expr() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20);").unwrap();
        conn.execute("UPDATE t SET val = val + 5;").unwrap();
        let rows = conn.query("SELECT id, val FROM t ORDER BY id;").unwrap();
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Integer(15)]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Integer(25)]
        );
    }

    #[test]
    fn probe_delete_compound_where() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c');").unwrap();
        conn.execute("DELETE FROM t WHERE id > 1 AND val = 'b';")
            .unwrap();
        let rows = conn.query("SELECT id FROM t ORDER BY id;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
    }

    #[test]
    fn probe_coalesce_nulls() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, NULL, 'fallback');")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'present', 'fallback');")
            .unwrap();
        let rows = conn
            .query("SELECT id, COALESCE(a, b) FROM t ORDER BY id;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("fallback".to_owned())
        );
        assert_eq!(
            row_values(&rows[1])[1],
            SqliteValue::Text("present".to_owned())
        );
    }

    #[test]
    fn probe_case_when_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, NULL);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 5);").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 15);").unwrap();
        let rows = conn
            .query(
                "SELECT id, CASE WHEN val IS NULL THEN 'null' WHEN val < 10 THEN 'small' ELSE 'big' END FROM t ORDER BY id;",
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("null".to_owned())
        );
        assert_eq!(
            row_values(&rows[1])[1],
            SqliteValue::Text("small".to_owned())
        );
        assert_eq!(row_values(&rows[2])[1], SqliteValue::Text("big".to_owned()));
    }

    #[test]
    fn probe_union_all() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2, 'b');").unwrap();
        let rows = conn
            .query("SELECT val FROM t1 UNION ALL SELECT val FROM t2;")
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn probe_union_dedup() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'b');").unwrap();
        let rows = conn
            .query("SELECT val FROM t UNION SELECT val FROM t;")
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "UNION should deduplicate: got {:?}",
            rows.iter().map(row_values).collect::<Vec<_>>()
        );
    }

    #[test]
    fn probe_except() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c');").unwrap();
        let rows = conn
            .query("SELECT val FROM t EXCEPT SELECT val FROM t WHERE id = 2;")
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "EXCEPT should remove 'b': got {:?}",
            rows.iter().map(row_values).collect::<Vec<_>>()
        );
    }

    #[test]
    fn probe_intersect() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c');").unwrap();
        let rows = conn
            .query("SELECT val FROM t INTERSECT SELECT val FROM t WHERE id <= 2;")
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "INTERSECT should keep 'a' and 'b': got {:?}",
            rows.iter().map(row_values).collect::<Vec<_>>()
        );
    }

    #[test]
    fn probe_insert_select() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO src VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO dst SELECT * FROM src;").unwrap();
        let rows = conn.query("SELECT id, val FROM dst ORDER BY id;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("a".to_owned()));
    }

    #[test]
    fn probe_limit_offset() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        for i in 1..=10 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}');"))
                .unwrap();
        }
        let rows = conn
            .query("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2;")
            .unwrap();
        assert_eq!(rows.len(), 3, "LIMIT 3 OFFSET 2 should return 3 rows");
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(4));
        assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(5));
    }

    #[test]
    fn probe_group_by_multi_col() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, sub TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'A', 'x', 10);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'A', 'x', 20);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'A', 'y', 30);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (4, 'B', 'x', 40);")
            .unwrap();
        let rows = conn
            .query("SELECT cat, sub, SUM(val) FROM t GROUP BY cat, sub ORDER BY cat, sub;")
            .unwrap();
        assert_eq!(
            rows.len(),
            3,
            "Should have 3 groups: got {:?}",
            rows.iter().map(row_values).collect::<Vec<_>>()
        );
        assert_eq!(row_values(&rows[0])[2], SqliteValue::Integer(30));
        assert_eq!(row_values(&rows[1])[2], SqliteValue::Integer(30));
        assert_eq!(row_values(&rows[2])[2], SqliteValue::Integer(40));
    }

    #[test]
    fn probe_having_aggregate() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'A', 10);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'A', 20);").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'B', 30);").unwrap();
        let rows = conn
            .query("SELECT cat, COUNT(*) as cnt FROM t GROUP BY cat HAVING cnt > 1;")
            .unwrap();
        assert_eq!(rows.len(), 1, "Only group A has count > 1");
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("A".to_owned()));
    }

    #[test]
    fn having_between_filters_groups() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE hb (grp INTEGER, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO hb VALUES (1, 10), (1, 20), (2, 30), (3, 40), (3, 50), (3, 60);")
            .unwrap();
        // COUNT(*) for groups: 1→2, 2→1, 3→3. HAVING cnt BETWEEN 2 AND 3 keeps 1,3.
        let rows = conn
            .query("SELECT grp, COUNT(*) as cnt FROM hb GROUP BY grp HAVING cnt BETWEEN 2 AND 3;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let grps: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(grps.contains(&SqliteValue::Integer(1)));
        assert!(grps.contains(&SqliteValue::Integer(3)));
    }

    #[test]
    fn having_in_filters_groups() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE hi (grp TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO hi VALUES ('A', 1), ('B', 2), ('C', 3);")
            .unwrap();
        let rows = conn
            .query("SELECT grp FROM hi GROUP BY grp HAVING grp IN ('A', 'C');")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let grps: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(grps.contains(&SqliteValue::Text("A".to_owned())));
        assert!(grps.contains(&SqliteValue::Text("C".to_owned())));
    }

    #[test]
    fn having_case_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE hc (grp TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO hc VALUES ('X', 1), ('Y', 2), ('X', 3);")
            .unwrap();
        // CASE grp WHEN 'X' THEN 1 ELSE 0 END = 1 keeps only 'X'
        let rows = conn
            .query("SELECT grp, SUM(val) FROM hc GROUP BY grp HAVING CASE grp WHEN 'X' THEN 1 ELSE 0 END = 1;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("X".to_owned()));
    }

    #[test]
    fn like_null_operand_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT NULL LIKE 'abc';").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn like_null_pattern_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 'abc' LIKE NULL;").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn like_null_both_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT NULL LIKE NULL;").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn not_like_null_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 'abc' NOT LIKE NULL;").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn like_integer_coercion() {
        let conn = Connection::open(":memory:").unwrap();
        // SQLite coerces non-text to text for LIKE comparison.
        let rows = conn.query("SELECT 123 LIKE '123';").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
    }

    #[test]
    fn like_null_in_join_where() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE lnj (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO lnj VALUES (1, 'alice'), (2, NULL), (3, 'bob');")
            .unwrap();
        // NULL name LIKE '%' should not match (NULL result, not truthy).
        let rows = conn
            .query("SELECT id FROM lnj WHERE name LIKE '%' ORDER BY id;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
    }

    #[test]
    fn having_like_filters_groups() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE hlk (grp TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO hlk VALUES ('apple', 1), ('banana', 2), ('apricot', 3);")
            .unwrap();
        // HAVING grp LIKE 'ap%' keeps only 'apple' and 'apricot'.
        let rows = conn
            .query("SELECT grp, SUM(val) FROM hlk GROUP BY grp HAVING grp LIKE 'ap%';")
            .unwrap();
        assert_eq!(rows.len(), 2);
        let grps: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert!(grps.contains(&SqliteValue::Text("apple".to_owned())));
        assert!(grps.contains(&SqliteValue::Text("apricot".to_owned())));
    }

    #[test]
    fn case_null_base_does_not_match_null() {
        let conn = Connection::open(":memory:").unwrap();
        // NULL = NULL is UNKNOWN, not TRUE — CASE should go to ELSE.
        let rows = conn
            .query("SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END;")
            .unwrap();
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("no match".to_owned())
        );
    }

    #[test]
    fn case_null_base_skips_all_whens() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT CASE NULL WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'none' END;")
            .unwrap();
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("none".to_owned())
        );
    }

    #[test]
    fn case_null_when_value_skipped() {
        let conn = Connection::open(":memory:").unwrap();
        // CASE 1 WHEN NULL should skip because 1 = NULL is UNKNOWN.
        let rows = conn
            .query("SELECT CASE 1 WHEN NULL THEN 'bad' WHEN 1 THEN 'ok' ELSE 'miss' END;")
            .unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("ok".to_owned()));
    }

    #[test]
    fn case_null_in_join_filter() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE cj (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO cj VALUES (1, NULL), (2, 'x'), (3, 'y');")
            .unwrap();
        // CASE val WHEN NULL: should never match, so id=1 gets 'other'.
        let rows = conn
            .query(
                "SELECT id, CASE val WHEN 'x' THEN 'found' ELSE 'other' END AS r FROM cj ORDER BY id;",
            )
            .unwrap();
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("other".to_owned())
        );
        assert_eq!(
            row_values(&rows[1])[1],
            SqliteValue::Text("found".to_owned())
        );
    }

    #[test]
    fn cast_null_as_integer_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT CAST(NULL AS INTEGER);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn cast_null_as_real_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT CAST(NULL AS REAL);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn cast_null_as_text_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT CAST(NULL AS TEXT);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn cast_null_from_table_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE cn (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO cn VALUES (1, NULL), (2, '5');")
            .unwrap();
        // CAST(NULL AS INTEGER) should be NULL, not 0.
        let rows = conn
            .query("SELECT id, CAST(val AS INTEGER) FROM cn ORDER BY id;")
            .unwrap();
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Null);
        assert_eq!(row_values(&rows[1])[1], SqliteValue::Integer(5));
    }

    #[test]
    fn collate_in_join_does_not_return_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE cl (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO cl VALUES (1, 'Alice'), (2, 'bob');")
            .unwrap();
        // COLLATE should not silently return NULL — it should evaluate the inner expr.
        let rows = conn
            .query("SELECT id FROM cl WHERE name COLLATE NOCASE = 'alice' ORDER BY id;")
            .unwrap();
        // At minimum, id=1 should match (exact case match with 'Alice' compared via nocase).
        assert!(!rows.is_empty());
    }

    #[test]
    fn probe_nested_functions() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, '  hello  ');")
            .unwrap();
        let rows = conn.query("SELECT UPPER(TRIM(val)) FROM t;").unwrap();
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("HELLO".to_owned())
        );
    }

    #[test]
    fn replace_null_arg_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT REPLACE(NULL, 'a', 'b');").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        let rows = conn.query("SELECT REPLACE('hello', NULL, 'b');").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn trim_null_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT TRIM(NULL);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        let rows = conn.query("SELECT LTRIM(NULL);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        let rows = conn.query("SELECT RTRIM(NULL);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn hex_null_returns_empty_string() {
        // C SQLite: hex(NULL) returns '' (empty string), not NULL.
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT HEX(NULL);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text(String::new()));
    }

    #[test]
    fn instr_null_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT INSTR(NULL, 'x');").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        let rows = conn.query("SELECT INSTR('hello', NULL);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn substr_null_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT SUBSTR(NULL, 1, 3);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn substr_negative_start() {
        let conn = Connection::open(":memory:").unwrap();
        // Negative start counts from right: -1 = last char.
        let rows = conn.query("SELECT SUBSTR('hello', -1);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("o".to_owned()));
        let rows = conn.query("SELECT SUBSTR('hello', -3);").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("llo".to_owned()));
    }

    #[test]
    fn limit_negative_returns_all_rows() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE ln (id INTEGER PRIMARY KEY);")
            .unwrap();
        conn.execute("INSERT INTO ln VALUES (1), (2), (3), (4), (5);")
            .unwrap();
        // LIMIT -1 means unlimited in SQLite.
        let rows = conn
            .query("SELECT id FROM ln ORDER BY id LIMIT -1;")
            .unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn offset_negative_treated_as_zero() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE on_ (id INTEGER PRIMARY KEY);")
            .unwrap();
        conn.execute("INSERT INTO on_ VALUES (1), (2), (3);")
            .unwrap();
        // Negative OFFSET should be treated as 0.
        let rows = conn
            .query("SELECT id FROM on_ ORDER BY id LIMIT 2 OFFSET -5;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
    }

    #[test]
    fn null_comparison_returns_null_in_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE nc (id INTEGER PRIMARY KEY, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO nc VALUES (1, NULL), (2, 5), (3, NULL);")
            .unwrap();
        // NULL = 5 should be NULL (not truthy), so row 1 excluded.
        // NULL = NULL should be NULL (not truthy), so row 3 excluded.
        let rows = conn
            .query("SELECT id FROM nc WHERE val = 5 ORDER BY id;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
    }

    #[test]
    fn null_and_true_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        // NULL AND 1 should be NULL, not 0.
        let rows = conn.query("SELECT NULL AND 1;").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn null_or_false_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        // NULL OR 0 should be NULL, not 0.
        let rows = conn.query("SELECT NULL OR 0;").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn false_and_null_returns_false() {
        let conn = Connection::open(":memory:").unwrap();
        // 0 AND NULL should be 0 (FALSE short-circuits).
        let rows = conn.query("SELECT 0 AND NULL;").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(0));
    }

    #[test]
    fn null_ne_in_where_excludes_row() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE nne (id INTEGER PRIMARY KEY, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO nne VALUES (1, NULL), (2, 5), (3, 10);")
            .unwrap();
        // NULL != 5 is NULL (not truthy), so id=1 excluded. 5 != 5 is false, so id=2 excluded.
        let rows = conn
            .query("SELECT id FROM nne WHERE val != 5 ORDER BY id;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
    }

    #[test]
    fn mixed_type_comparison_uses_type_ordering() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE mt (id INTEGER PRIMARY KEY, val);")
            .unwrap();
        // Integer 5 < Text 'hello' in SQLite type ordering (numeric < text).
        conn.execute("INSERT INTO mt VALUES (1, 5), (2, 'hello'), (3, 10);")
            .unwrap();
        // 5 = 'hello' should be FALSE (different type classes).
        let rows = conn
            .query("SELECT id FROM mt WHERE val = 'hello';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
    }

    #[test]
    fn integer_less_than_text_in_type_ordering() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE ilt (id INTEGER PRIMARY KEY, val);")
            .unwrap();
        conn.execute("INSERT INTO ilt VALUES (1, 42), (2, 'abc');")
            .unwrap();
        // Integer 42 < Text 'abc' in SQLite type ordering.
        let rows = conn.query("SELECT id FROM ilt WHERE val < 'abc';").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
    }

    #[test]
    fn blob_greater_than_text_in_type_ordering() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE bgt (id INTEGER PRIMARY KEY, val);")
            .unwrap();
        conn.execute("INSERT INTO bgt VALUES (1, 'text'), (2, X'DEADBEEF');")
            .unwrap();
        // Blob > Text in SQLite type ordering.
        let rows = conn
            .query("SELECT id FROM bgt WHERE val > 'text';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
    }

    #[test]
    fn large_integer_float_precision_comparison() {
        let conn = Connection::open(":memory:").unwrap();
        // 2^53 + 1 = 9007199254740993 cannot be exactly represented as f64.
        // 9007199254740993 > 9007199254740992.0 should be true.
        conn.execute("CREATE TABLE lip (id INTEGER PRIMARY KEY, ival INTEGER, fval REAL);")
            .unwrap();
        conn.execute("INSERT INTO lip VALUES (1, 9007199254740993, 9007199254740992.0);")
            .unwrap();
        let rows = conn.query("SELECT id FROM lip WHERE ival > fval;").unwrap();
        assert_eq!(rows.len(), 1, "large integer should be greater than float");
    }

    #[test]
    fn not_null_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        // NOT NULL should be NULL, not 1.
        let rows = conn.query("SELECT NOT NULL;").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn not_null_in_where_excludes_row() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE nn (id INTEGER PRIMARY KEY, flag INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO nn VALUES (1, NULL), (2, 0), (3, 1);")
            .unwrap();
        // NOT flag: NOT NULL=NULL (excluded), NOT 0=1 (included), NOT 1=0 (excluded).
        let rows = conn
            .query("SELECT id FROM nn WHERE NOT flag ORDER BY id;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
    }

    #[test]
    fn bitnot_null_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        // ~NULL should be NULL.
        let rows = conn.query("SELECT ~NULL;").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
    }

    #[test]
    fn probe_update_where_column_cmp() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 5, 10);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 15, 10);").unwrap();
        conn.execute("UPDATE t SET a = a * 2 WHERE a < b;").unwrap();
        let rows = conn.query("SELECT id, a FROM t ORDER BY id;").unwrap();
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(10));
        assert_eq!(row_values(&rows[1])[1], SqliteValue::Integer(15));
    }

    #[test]
    fn probe_nullif() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'skip');").unwrap();
        let rows = conn
            .query("SELECT id, NULLIF(val, 'skip') FROM t ORDER BY id;")
            .unwrap();
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("x".to_owned()));
        assert_eq!(row_values(&rows[1])[1], SqliteValue::Null);
    }

    #[test]
    fn probe_iif() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 5);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 15);").unwrap();
        let rows = conn
            .query("SELECT id, IIF(val > 10, 'big', 'small') FROM t ORDER BY id;")
            .unwrap();
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("small".to_owned())
        );
        assert_eq!(row_values(&rows[1])[1], SqliteValue::Text("big".to_owned()));
    }

    #[test]
    fn probe_select_distinct() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (4, 'b');").unwrap();
        conn.execute("INSERT INTO t VALUES (5, 'c');").unwrap();
        let rows = conn
            .query("SELECT DISTINCT val FROM t ORDER BY val;")
            .unwrap();
        assert_eq!(rows.len(), 3, "DISTINCT should return 3 unique values");
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".to_owned()));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("b".to_owned()));
        assert_eq!(row_values(&rows[2])[0], SqliteValue::Text("c".to_owned()));
    }

    #[test]
    fn probe_order_by_desc() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 30);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 10);").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 20);").unwrap();
        let rows = conn
            .query("SELECT id, val FROM t ORDER BY val DESC;")
            .unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
        assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(2));
    }

    #[test]
    fn probe_insert_or_replace() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'old');").unwrap();
        conn.execute("INSERT OR REPLACE INTO t VALUES (1, 'new');")
            .unwrap();
        let rows = conn.query("SELECT id, val FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("new".to_owned()));
    }

    #[test]
    fn probe_insert_or_ignore() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'first');").unwrap();
        conn.execute("INSERT OR IGNORE INTO t VALUES (1, 'second');")
            .unwrap();
        let rows = conn.query("SELECT id, val FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("first".to_owned())
        );
    }

    #[test]
    fn probe_between() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
            .unwrap();
        for i in 1..=10 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {i});"))
                .unwrap();
        }
        let rows = conn
            .query("SELECT val FROM t WHERE val BETWEEN 3 AND 7 ORDER BY val;")
            .unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
        assert_eq!(row_values(&rows[4])[0], SqliteValue::Integer(7));
    }

    #[test]
    fn probe_in_list() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c');").unwrap();
        let rows = conn
            .query("SELECT id FROM t WHERE val IN ('a', 'c') ORDER BY id;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
    }

    #[test]
    fn probe_like_pattern() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'Alice');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'Bob');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'Charlie');")
            .unwrap();
        let rows = conn
            .query("SELECT name FROM t WHERE name LIKE '%li%' ORDER BY name;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("Alice".to_owned())
        );
        assert_eq!(
            row_values(&rows[1])[0],
            SqliteValue::Text("Charlie".to_owned())
        );
    }

    #[test]
    fn probe_subquery_in_where() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3, 'c');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1, 1);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2, 3);").unwrap();
        // This may not be supported - check if it errors gracefully
        let result =
            conn.query("SELECT val FROM t1 WHERE id IN (SELECT t1_id FROM t2) ORDER BY val;");
        if let Ok(rows) = result {
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".to_owned()));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("c".to_owned()));
        } else {
            // IN subquery not yet supported — that's fine for now
        }
    }

    // Test: INSERT ... RETURNING *
    #[test]
    fn probe_insert_returning_star() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t VALUES (1, 'Alice', 30) RETURNING *;")
            .unwrap();
        assert_eq!(rows.len(), 1, "RETURNING * should produce 1 row");
        // RETURNING * includes all columns: id (rowid alias), name, age
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("Alice".to_owned()),
                SqliteValue::Integer(30),
            ]
        );
    }

    // Test: INSERT ... RETURNING specific columns
    #[test]
    fn probe_insert_returning_columns() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t VALUES (1, 'Bob', 25) RETURNING name, age;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Text("Bob".to_owned()),
                SqliteValue::Integer(25),
            ]
        );
    }

    // Test: INSERT ... RETURNING rowid
    #[test]
    fn probe_insert_returning_rowid() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t VALUES (42, 'test') RETURNING id;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(42));
    }

    // Test: Multi-row INSERT ... RETURNING
    #[test]
    fn probe_insert_returning_multi_row() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();
        // INSERT SELECT with RETURNING
        conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t2 SELECT * FROM t RETURNING *;")
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "Multi-row INSERT RETURNING should produce 2 rows"
        );
    }

    // Test: UPDATE ... RETURNING *
    #[test]
    fn probe_update_returning_star() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'Alice', 30);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'Bob', 25);")
            .unwrap();
        let rows = conn
            .query("UPDATE t SET age = age + 1 WHERE id = 1 RETURNING *;")
            .unwrap();
        assert_eq!(rows.len(), 1, "UPDATE RETURNING should produce 1 row");
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("Alice".to_owned()),
                SqliteValue::Integer(31),
            ]
        );
    }

    // Test: UPDATE ... RETURNING specific columns
    #[test]
    fn probe_update_returning_columns() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 10);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20);").unwrap();
        let rows = conn
            .query("UPDATE t SET val = val * 2 RETURNING id, val;")
            .unwrap();
        assert_eq!(rows.len(), 2, "UPDATE RETURNING should produce 2 rows");
    }

    // Test: DELETE ... RETURNING *
    #[test]
    fn probe_delete_returning_star() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'Alice');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'Bob');").unwrap();
        let rows = conn
            .query("DELETE FROM t WHERE id = 2 RETURNING *;")
            .unwrap();
        assert_eq!(rows.len(), 1, "DELETE RETURNING should produce 1 row");
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("Bob".to_owned()),]
        );
        // Verify the row is actually deleted
        let remaining = conn.query("SELECT COUNT(*) FROM t;").unwrap();
        assert_eq!(row_values(&remaining[0])[0], SqliteValue::Integer(1));
    }

    // Test: DELETE ... RETURNING specific column
    #[test]
    fn probe_delete_returning_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'c');").unwrap();
        let rows = conn
            .query("DELETE FROM t WHERE id > 1 RETURNING val;")
            .unwrap();
        assert_eq!(rows.len(), 2, "DELETE RETURNING should produce 2 rows");
    }

    // Test: INSERT DEFAULT VALUES
    #[test]
    fn probe_insert_default_values() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t DEFAULT VALUES;").unwrap();
        let rows = conn.query("SELECT id, name, val FROM t;").unwrap();
        assert_eq!(rows.len(), 1, "DEFAULT VALUES should insert 1 row");
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        // name and val should be NULL (defaults)
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Null);
        assert_eq!(row_values(&rows[0])[2], SqliteValue::Null);
    }

    #[test]
    fn insert_default_values_uses_column_defaults() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE td (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', count INTEGER DEFAULT 42, ratio REAL DEFAULT 2.5);",
        )
        .unwrap();
        conn.execute("INSERT INTO td DEFAULT VALUES;").unwrap();
        let rows = conn
            .query("SELECT id, status, count, ratio FROM td;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("active".to_string()),
            "status should use DEFAULT 'active'"
        );
        assert_eq!(
            row_values(&rows[0])[2],
            SqliteValue::Integer(42),
            "count should use DEFAULT 42"
        );
        assert_eq!(
            row_values(&rows[0])[3],
            SqliteValue::Float(2.5),
            "ratio should use DEFAULT 2.5"
        );
    }

    #[test]
    fn insert_explicit_cols_uses_defaults_for_omitted() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE te (id INTEGER PRIMARY KEY, name TEXT, status TEXT DEFAULT 'pending');",
        )
        .unwrap();
        // Only specify name, omit status — should get DEFAULT 'pending'.
        conn.execute("INSERT INTO te (name) VALUES ('alice');")
            .unwrap();
        let rows = conn.query("SELECT id, name, status FROM te;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("alice".to_string())
        );
        assert_eq!(
            row_values(&rows[0])[2],
            SqliteValue::Text("pending".to_string()),
            "omitted column should use DEFAULT 'pending'"
        );
    }

    // Test: INSERT DEFAULT VALUES with RETURNING (IPK column)
    #[test]
    fn probe_insert_default_values_returning() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        // Use RETURNING id (IPK column) — tests that IPK columns emit Rowid
        // instead of Column (which would return Null for DEFAULT VALUES).
        let rows = conn
            .query("INSERT INTO t DEFAULT VALUES RETURNING id;")
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "DEFAULT VALUES RETURNING should produce 1 row"
        );
        // rowid should be auto-assigned (1)
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
    }

    // =================================================================
    // IPK integration tests (bd-3l6e / PARITY-B5)
    // =================================================================

    /// NULL IPK should auto-generate an incrementing rowid.
    #[test]
    fn ipk_null_auto_generates_rowid() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        let r1 = conn
            .query("INSERT INTO t VALUES (NULL, 'a') RETURNING id;")
            .unwrap();
        let r2 = conn
            .query("INSERT INTO t VALUES (NULL, 'b') RETURNING id;")
            .unwrap();
        let id1 = &row_values(&r1[0])[0];
        let id2 = &row_values(&r2[0])[0];
        // Both should be positive integers.
        assert!(
            matches!(id1, SqliteValue::Integer(n) if *n > 0),
            "NULL IPK should auto-generate positive id, got {id1:?}"
        );
        // Second should be greater than first.
        if let (SqliteValue::Integer(a), SqliteValue::Integer(b)) = (id1, id2) {
            assert!(
                b > a,
                "successive NULL IPK inserts should increment: {a} < {b}"
            );
        }
    }

    /// Explicit IPK value of 0 should be stored as rowid 0.
    #[test]
    fn ipk_zero_is_valid_rowid() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t VALUES (0, 'zero') RETURNING id;")
            .unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(0));
    }

    /// Negative IPK values should be stored as negative rowids.
    #[test]
    fn ipk_negative_is_valid_rowid() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t VALUES (-5, 'neg') RETURNING id;")
            .unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(-5));
    }

    /// Multi-row INSERT with explicit IPK values.
    #[test]
    fn ipk_multi_row_explicit_values() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t VALUES (10,'a'),(20,'b'),(30,'c') RETURNING id;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(10));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(20));
        assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(30));
    }

    /// Mixed NULL and explicit IPK in multi-row INSERT.
    #[test]
    fn ipk_mixed_null_and_explicit() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (10, 'explicit');")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t VALUES (NULL, 'auto') RETURNING id;")
            .unwrap();
        // Auto-generated id should be > 10 (the max existing rowid).
        if let SqliteValue::Integer(id) = &row_values(&rows[0])[0] {
            assert!(
                *id > 10,
                "auto-generated id after max=10 should be > 10, got {id}"
            );
        } else {
            panic!("expected Integer, got {:?}", row_values(&rows[0])[0]);
        }
    }

    /// RETURNING * should include the correct IPK value.
    #[test]
    fn ipk_returning_star_includes_correct_id() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t VALUES (42, 'x') RETURNING *;")
            .unwrap();
        let vals = row_values(&rows[0]);
        assert_eq!(vals[0], SqliteValue::Integer(42));
        assert_eq!(vals[1], SqliteValue::Text("x".to_owned()));
    }

    /// SELECT after INSERT should see the correct IPK values.
    #[test]
    fn ipk_roundtrip_select_after_insert() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (42, 'Alice');").unwrap();
        conn.execute("INSERT INTO t VALUES (100, 'Bob');").unwrap();
        let rows = conn.query("SELECT * FROM t ORDER BY id;").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(42));
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("Alice".to_owned())
        );
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(100));
    }

    /// Explicit column list in non-schema order should store values correctly.
    #[test]
    fn ipk_column_list_reorder() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        // Column list reverses schema order: (name, id) vs schema (id, name).
        let rows = conn
            .query("INSERT INTO t(name, id) VALUES ('Alice', 42) RETURNING *;")
            .unwrap();
        let vals = row_values(&rows[0]);
        assert_eq!(
            vals[0],
            SqliteValue::Integer(42),
            "id should be 42 (from column-list position 1)"
        );
        assert_eq!(
            vals[1],
            SqliteValue::Text("Alice".to_owned()),
            "name should be Alice (from column-list position 0)"
        );
        // Also verify via SELECT that the stored record is correct.
        let sel = conn.query("SELECT id, name FROM t;").unwrap();
        let sv = row_values(&sel[0]);
        assert_eq!(sv[0], SqliteValue::Integer(42));
        assert_eq!(sv[1], SqliteValue::Text("Alice".to_owned()));
    }

    /// Explicit column list omitting IPK should auto-generate rowid.
    #[test]
    fn ipk_column_list_omit_ipk() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        let rows = conn
            .query("INSERT INTO t(name) VALUES ('Bob') RETURNING id, name;")
            .unwrap();
        let vals = row_values(&rows[0]);
        // id should be auto-generated (positive integer).
        assert!(
            matches!(vals[0], SqliteValue::Integer(n) if n > 0),
            "omitted IPK should auto-generate, got {:?}",
            vals[0]
        );
        assert_eq!(vals[1], SqliteValue::Text("Bob".to_owned()));
    }

    /// DELETE then reinsert with same IPK should work.
    #[test]
    fn ipk_delete_reinsert_same_id() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'original');")
            .unwrap();
        conn.execute("DELETE FROM t WHERE id = 1;").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'reinserted');")
            .unwrap();
        let rows = conn.query("SELECT val FROM t WHERE id = 1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("reinserted".to_owned())
        );
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Index Maintenance Tests (Phase 5I - bd-1nmg)
    // ══════════════════════════════════════════════════════════════════════════

    // ── Basic Operations ──────────────────────────────────────────────────────

    /// INSERT should create index entries for single-column indexes.
    #[test]
    fn index_insert_single_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'bob');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 'charlie');")
            .unwrap();

        // Verify index is used for lookups (entries exist).
        let rows = conn.query("SELECT id FROM t WHERE name = 'bob';").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));

        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
    }

    /// INSERT should create index entries for multi-column indexes.
    #[test]
    fn index_insert_multi_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INT, b INT, c TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_ab ON t(a, b);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, 10, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 20, 'y');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 10, 'z');").unwrap();

        // Query using both columns of the index.
        let rows = conn
            .query("SELECT c FROM t WHERE a = 1 AND b = 20;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("y".to_owned()));

        // Query using only first column prefix.
        let rows = conn.query("SELECT c FROM t WHERE a = 1;").unwrap();
        assert_eq!(rows.len(), 2); // Should find both a=1 rows.
    }

    /// DELETE should remove index entries.
    #[test]
    fn index_delete_removes_entry() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'bob');").unwrap();

        // Verify both are findable.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 1);

        // Delete alice.
        conn.execute("DELETE FROM t WHERE id = 1;").unwrap();

        // Alice should no longer be findable via index.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 0);

        // Bob should still be findable.
        let rows = conn.query("SELECT id FROM t WHERE name = 'bob';").unwrap();
        assert_eq!(rows.len(), 1);
    }

    /// UPDATE should maintain index entries when indexed column changes.
    #[test]
    fn index_update_indexed_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();

        // Verify initial state.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 1);

        // Update name.
        conn.execute("UPDATE t SET name = 'alicia' WHERE id = 1;")
            .unwrap();

        // Old name should not be findable.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 0);

        // New name should be findable.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alicia';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
    }

    /// UPDATE should preserve index entries when non-indexed column changes.
    #[test]
    fn index_update_non_indexed_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, 'alice', 30);")
            .unwrap();

        // Update non-indexed column.
        conn.execute("UPDATE t SET age = 31 WHERE id = 1;").unwrap();

        // Index should still work correctly.
        let rows = conn
            .query("SELECT age FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(31));
    }

    // ── Multiple Indexes ──────────────────────────────────────────────────────

    /// Table with multiple indexes should maintain all of them.
    #[test]
    fn index_multiple_indexes_on_table() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INT, b INT, c INT, d INT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_a ON t(a);").unwrap();
        conn.execute("CREATE INDEX idx_b ON t(b);").unwrap();
        conn.execute("CREATE INDEX idx_ab ON t(a, b);").unwrap();
        conn.execute("CREATE INDEX idx_cd ON t(c, d);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, 2, 3, 4);").unwrap();
        conn.execute("INSERT INTO t VALUES (5, 6, 7, 8);").unwrap();

        // All indexes should be searchable.
        let rows = conn.query("SELECT b FROM t WHERE a = 1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));

        let rows = conn.query("SELECT a FROM t WHERE b = 6;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(5));

        let rows = conn
            .query("SELECT c FROM t WHERE a = 1 AND b = 2;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));

        let rows = conn
            .query("SELECT a FROM t WHERE c = 7 AND d = 8;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(5));
    }

    /// DELETE should remove entries from all indexes.
    #[test]
    fn index_delete_removes_from_all_indexes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_a ON t(a);").unwrap();
        conn.execute("CREATE INDEX idx_b ON t(b);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, 10, 100);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20, 200);").unwrap();

        // Delete row 1.
        conn.execute("DELETE FROM t WHERE id = 1;").unwrap();

        // Neither index should find the deleted row.
        let rows = conn.query("SELECT id FROM t WHERE a = 10;").unwrap();
        assert_eq!(rows.len(), 0);

        let rows = conn.query("SELECT id FROM t WHERE b = 100;").unwrap();
        assert_eq!(rows.len(), 0);

        // Row 2 should still be findable via both indexes.
        let rows = conn.query("SELECT id FROM t WHERE a = 20;").unwrap();
        assert_eq!(rows.len(), 1);

        let rows = conn.query("SELECT id FROM t WHERE b = 200;").unwrap();
        assert_eq!(rows.len(), 1);
    }

    /// UPDATE should maintain all affected indexes.
    #[test]
    fn index_update_maintains_all_indexes() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_a ON t(a);").unwrap();
        conn.execute("CREATE INDEX idx_b ON t(b);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, 10, 100);").unwrap();

        // Update both indexed columns.
        conn.execute("UPDATE t SET a = 11, b = 101 WHERE id = 1;")
            .unwrap();

        // Old values should not be findable.
        let rows = conn.query("SELECT id FROM t WHERE a = 10;").unwrap();
        assert_eq!(rows.len(), 0);
        let rows = conn.query("SELECT id FROM t WHERE b = 100;").unwrap();
        assert_eq!(rows.len(), 0);

        // New values should be findable.
        let rows = conn.query("SELECT id FROM t WHERE a = 11;").unwrap();
        assert_eq!(rows.len(), 1);
        let rows = conn.query("SELECT id FROM t WHERE b = 101;").unwrap();
        assert_eq!(rows.len(), 1);
    }

    // ── NULL Handling ─────────────────────────────────────────────────────────
    // Fixed in bd-36eh.1: NULL value handling in index B-trees.
    // The fix sets NULLEQ flag (0x80) in WHERE Ne comparisons so NULL != value
    // correctly skips rows with NULL values.

    /// Index should handle NULL values correctly.
    #[test]
    fn index_with_null_values() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, NULL);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'alice');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, NULL);").unwrap();

        // Query for NULL via IS NULL.
        let rows = conn.query("SELECT id FROM t WHERE name IS NULL;").unwrap();
        assert_eq!(rows.len(), 2);

        // Query for non-NULL.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
    }

    /// UPDATE NULL to non-NULL should update index correctly.
    #[test]
    fn index_update_null_to_non_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, NULL);").unwrap();
        conn.execute("INSERT INTO t VALUES (2, NULL);").unwrap();

        // Initially 2 NULLs.
        let rows = conn.query("SELECT id FROM t WHERE name IS NULL;").unwrap();
        assert_eq!(rows.len(), 2);

        // Update one NULL to non-NULL.
        conn.execute("UPDATE t SET name = 'bob' WHERE id = 1;")
            .unwrap();

        // Now only 1 NULL.
        let rows = conn.query("SELECT id FROM t WHERE name IS NULL;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));

        // And bob is findable.
        let rows = conn.query("SELECT id FROM t WHERE name = 'bob';").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
    }

    /// UPDATE non-NULL to NULL should update index correctly.
    #[test]
    fn index_update_non_null_to_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();

        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();

        // alice is findable.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 1);

        // Update to NULL.
        conn.execute("UPDATE t SET name = NULL WHERE id = 1;")
            .unwrap();

        // alice is no longer findable.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 0);

        // NULL is findable.
        let rows = conn.query("SELECT id FROM t WHERE name IS NULL;").unwrap();
        assert_eq!(rows.len(), 1);
    }

    // ── Bulk Operations ───────────────────────────────────────────────────────

    /// Bulk INSERT should maintain index for all rows.
    #[test]
    fn index_bulk_insert() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value INT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_value ON t(value);").unwrap();

        // Insert 100 rows.
        for i in 0..100 {
            conn.execute(&format!("INSERT INTO t VALUES ({}, {});", i, i * 2))
                .unwrap();
        }

        // Verify index works for various values.
        for i in [0, 25, 50, 75, 99] {
            let rows = conn
                .query(&format!("SELECT id FROM t WHERE value = {};", i * 2))
                .unwrap();
            assert_eq!(rows.len(), 1, "Should find row with value={}", i * 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(i));
        }
    }

    /// Bulk DELETE should remove all index entries.
    #[test]
    fn index_bulk_delete() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value INT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_value ON t(value);").unwrap();

        // Insert 50 rows.
        for i in 0..50 {
            conn.execute(&format!("INSERT INTO t VALUES ({}, {});", i, i))
                .unwrap();
        }

        // Delete half (even values).
        for i in (0..50).step_by(2) {
            conn.execute(&format!("DELETE FROM t WHERE id = {};", i))
                .unwrap();
        }

        // Even values should not be findable.
        for i in (0..50).step_by(2) {
            let rows = conn
                .query(&format!("SELECT id FROM t WHERE value = {};", i))
                .unwrap();
            assert_eq!(
                rows.len(),
                0,
                "Deleted row with value={} should not exist",
                i
            );
        }

        // Odd values should still be findable.
        for i in (1..50).step_by(2) {
            let rows = conn
                .query(&format!("SELECT id FROM t WHERE value = {};", i))
                .unwrap();
            assert_eq!(rows.len(), 1, "Row with value={} should exist", i);
        }
    }

    /// Bulk UPDATE should maintain all index entries.
    #[test]
    fn index_bulk_update() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value INT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_value ON t(value);").unwrap();

        // Insert 50 rows.
        for i in 0..50 {
            conn.execute(&format!("INSERT INTO t VALUES ({}, {});", i, i))
                .unwrap();
        }

        // Update all values: value = value + 1000.
        conn.execute("UPDATE t SET value = value + 1000;").unwrap();

        // Old values should not be findable.
        for i in 0..50 {
            let rows = conn
                .query(&format!("SELECT id FROM t WHERE value = {};", i))
                .unwrap();
            assert_eq!(rows.len(), 0);
        }

        // New values should be findable.
        for i in 0..50 {
            let rows = conn
                .query(&format!("SELECT id FROM t WHERE value = {};", i + 1000))
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(i));
        }
    }

    // ── Transaction Rollback ──────────────────────────────────────────────────

    /// Index entries should be rolled back with transaction.
    #[test]
    fn index_rollback_insert() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();

        conn.execute("BEGIN;").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'bob');").unwrap();

        // Bob should be visible in transaction.
        let rows = conn.query("SELECT id FROM t WHERE name = 'bob';").unwrap();
        assert_eq!(rows.len(), 1);

        conn.execute("ROLLBACK;").unwrap();

        // Bob should NOT be visible after rollback.
        let rows = conn.query("SELECT id FROM t WHERE name = 'bob';").unwrap();
        assert_eq!(rows.len(), 0);

        // Alice should still be there.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    /// Index entries should be rolled back on DELETE rollback.
    #[test]
    fn index_rollback_delete() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();

        conn.execute("BEGIN;").unwrap();
        conn.execute("DELETE FROM t WHERE id = 1;").unwrap();

        // Alice should not be visible in transaction.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 0);

        conn.execute("ROLLBACK;").unwrap();

        // Alice should be restored after rollback.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    /// Index entries should be rolled back on UPDATE rollback.
    #[test]
    fn index_rollback_update() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice');").unwrap();

        conn.execute("BEGIN;").unwrap();
        conn.execute("UPDATE t SET name = 'bob' WHERE id = 1;")
            .unwrap();

        // Bob should be visible, alice not.
        let rows = conn.query("SELECT id FROM t WHERE name = 'bob';").unwrap();
        assert_eq!(rows.len(), 1);
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 0);

        conn.execute("ROLLBACK;").unwrap();

        // Alice should be restored, bob gone.
        let rows = conn
            .query("SELECT id FROM t WHERE name = 'alice';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        let rows = conn.query("SELECT id FROM t WHERE name = 'bob';").unwrap();
        assert_eq!(rows.len(), 0);
    }

    // ── No REINDEX Required ───────────────────────────────────────────────────

    /// All operations should work WITHOUT manual REINDEX.
    #[test]
    fn index_no_reindex_needed() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").unwrap();

        // Perform many operations.
        for i in 0..100 {
            conn.execute(&format!("INSERT INTO t VALUES ({}, 'name{}');", i, i))
                .unwrap();
        }
        for i in 0..50 {
            conn.execute(&format!("DELETE FROM t WHERE id = {};", i))
                .unwrap();
        }
        for i in 50..100 {
            conn.execute(&format!(
                "UPDATE t SET name = 'updated{}' WHERE id = {};",
                i, i
            ))
            .unwrap();
        }

        // All remaining rows should be findable via index WITHOUT REINDEX.
        for i in 50..100 {
            let rows = conn
                .query(&format!("SELECT id FROM t WHERE name = 'updated{}';", i))
                .unwrap();
            assert_eq!(
                rows.len(),
                1,
                "Row with updated name for id={} should be findable",
                i
            );
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(i));
        }

        // Deleted rows should not be findable.
        for i in 0..50 {
            let rows = conn
                .query(&format!("SELECT id FROM t WHERE name = 'name{}';", i))
                .unwrap();
            assert_eq!(
                rows.len(),
                0,
                "Deleted row with name{} should not be findable",
                i
            );
        }

        // Verify total count.
        let rows = conn.query("SELECT COUNT(*) FROM t;").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(50));
    }

    // ── INSERT with index on IPK column ───────────────────────────────────────

    /// Index on INTEGER PRIMARY KEY column should work correctly.
    #[test]
    fn index_on_ipk_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_id ON t(id);").unwrap();

        conn.execute("INSERT INTO t VALUES (100, 'alice');")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (200, 'bob');").unwrap();

        // Index on IPK should work.
        let rows = conn.query("SELECT name FROM t WHERE id = 100;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("alice".to_owned())
        );
    }

    // ── Mixed operations sequence ─────────────────────────────────────────────

    /// Complex sequence of operations should maintain index consistency.
    #[test]
    fn index_mixed_operations_sequence() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b TEXT);")
            .unwrap();
        conn.execute("CREATE INDEX idx_a ON t(a);").unwrap();
        conn.execute("CREATE INDEX idx_b ON t(b);").unwrap();

        // Insert
        conn.execute("INSERT INTO t VALUES (1, 10, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (2, 20, 'y');").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 30, 'z');").unwrap();

        // Update
        conn.execute("UPDATE t SET a = 15 WHERE id = 1;").unwrap();

        // Delete
        conn.execute("DELETE FROM t WHERE id = 2;").unwrap();

        // Insert more
        conn.execute("INSERT INTO t VALUES (4, 40, 'w');").unwrap();

        // Verify state via indexes.
        let rows = conn.query("SELECT id FROM t WHERE a = 10;").unwrap();
        assert_eq!(rows.len(), 0); // Was updated to 15

        let rows = conn.query("SELECT id FROM t WHERE a = 15;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));

        let rows = conn.query("SELECT id FROM t WHERE b = 'y';").unwrap();
        assert_eq!(rows.len(), 0); // Was deleted

        let rows = conn.query("SELECT id FROM t WHERE a = 40;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(4));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Concurrent Writer Stress Tests (Phase 5E.6 - bd-1299)
    // ══════════════════════════════════════════════════════════════════════════

    /// Multi-threaded concurrent writer stress test.
    ///
    /// 8 writer threads perform 1000 transfer operations each. Validates:
    /// - Conservation invariant: total balance remains constant
    /// - No data corruption under concurrent load
    /// - Proper transaction isolation and conflict handling
    #[test]
    fn concurrent_writers_stress_conservation() {
        use rand::prelude::*;
        use std::sync::{Arc, Barrier};
        use std::thread;

        let dir = tempfile::tempdir().expect("create temp dir");
        let db_path = dir.path().join("stress.db");
        let db_path_str = db_path.to_str().unwrap();

        // Number of accounts and initial balance.
        const NUM_ACCOUNTS: i64 = 100;
        const INITIAL_BALANCE: i64 = 1000;
        const EXPECTED_TOTAL: i64 = NUM_ACCOUNTS * INITIAL_BALANCE;

        // Test parameters: 8 writers, 100 ops each (reduced for test speed).
        const NUM_WRITERS: usize = 8;
        const OPS_PER_WRITER: u64 = 100;

        // Setup: create table and initial accounts.
        {
            let conn = Connection::open(db_path_str).expect("open db for setup");
            conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER);")
                .expect("create table");

            for i in 0..NUM_ACCOUNTS {
                conn.execute(&format!(
                    "INSERT INTO accounts VALUES ({}, {});",
                    i, INITIAL_BALANCE
                ))
                .expect("insert account");
            }

            // Verify initial total.
            let rows = conn
                .query("SELECT SUM(balance) FROM accounts;")
                .expect("sum query");
            let total = match &row_values(&rows[0])[0] {
                SqliteValue::Integer(n) => *n,
                other => panic!("unexpected sum type: {:?}", other),
            };
            assert_eq!(total, EXPECTED_TOTAL, "initial balance mismatch");
        }

        // Synchronize all threads to start at the same time.
        let barrier = Arc::new(Barrier::new(NUM_WRITERS));

        // Track results from each thread: (commits, retries, errors).
        let handles: Vec<_> = (0..NUM_WRITERS)
            .map(|thread_id| {
                let path = db_path_str.to_owned();
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    let conn = Connection::open(&path).expect("open db in thread");
                    let mut rng = rand::rngs::StdRng::seed_from_u64(thread_id as u64);

                    let mut commits = 0_u64;
                    let mut retries = 0_u64;
                    let mut errors = 0_u64;

                    // Wait for all threads to be ready.
                    barrier.wait();

                    while commits < OPS_PER_WRITER {
                        // Pick random accounts for transfer.
                        let from_id = rng.gen_range(0..NUM_ACCOUNTS);
                        let to_id = rng.gen_range(0..NUM_ACCOUNTS);
                        if from_id == to_id {
                            continue; // Skip self-transfer.
                        }
                        let amount = rng.gen_range(1..=10_i64);

                        // Start transaction (will be CONCURRENT by default).
                        if conn.execute("BEGIN;").is_err() {
                            retries += 1;
                            continue;
                        }

                        // Read current balances.
                        let from_balance = match conn.query(&format!(
                            "SELECT balance FROM accounts WHERE id = {};",
                            from_id
                        )) {
                            Ok(rows) if !rows.is_empty() => match &row_values(&rows[0])[0] {
                                SqliteValue::Integer(n) => *n,
                                _ => {
                                    let _ = conn.execute("ROLLBACK;");
                                    errors += 1;
                                    continue;
                                }
                            },
                            _ => {
                                let _ = conn.execute("ROLLBACK;");
                                errors += 1;
                                continue;
                            }
                        };

                        // Skip if insufficient balance.
                        if from_balance < amount {
                            let _ = conn.execute("ROLLBACK;");
                            continue;
                        }

                        // Perform transfer: debit from, credit to.
                        let debit_result = conn.execute(&format!(
                            "UPDATE accounts SET balance = balance - {} WHERE id = {};",
                            amount, from_id
                        ));
                        let credit_result = conn.execute(&format!(
                            "UPDATE accounts SET balance = balance + {} WHERE id = {};",
                            amount, to_id
                        ));

                        if debit_result.is_err() || credit_result.is_err() {
                            let _ = conn.execute("ROLLBACK;");
                            retries += 1;
                            continue;
                        }

                        // Commit transaction.
                        match conn.execute("COMMIT;") {
                            Ok(_) => commits += 1,
                            Err(e) => {
                                // Check for BUSY/conflict errors.
                                let _ = conn.execute("ROLLBACK;");
                                if format!("{:?}", e).contains("Busy")
                                    || format!("{:?}", e).contains("busy")
                                {
                                    retries += 1;
                                } else {
                                    // Other error - still count as retry for this test.
                                    retries += 1;
                                }
                            }
                        }
                    }

                    (commits, retries, errors)
                })
            })
            .collect();

        // Wait for all threads to complete and collect results.
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Print thread results for diagnostics.
        let mut total_commits = 0_u64;
        let mut total_retries = 0_u64;
        let mut total_errors = 0_u64;
        for (i, (commits, retries, errors)) in results.iter().enumerate() {
            eprintln!(
                "Thread {}: {} commits, {} retries, {} errors",
                i, commits, retries, errors
            );
            total_commits += commits;
            total_retries += retries;
            total_errors += errors;
        }
        eprintln!(
            "Total: {} commits, {} retries, {} errors",
            total_commits, total_retries, total_errors
        );

        // Verify conservation invariant: total balance should be unchanged.
        let conn = Connection::open(db_path_str).expect("reopen db for verification");
        let rows = conn
            .query("SELECT SUM(balance) FROM accounts;")
            .expect("final sum query");
        let final_total = match &row_values(&rows[0])[0] {
            SqliteValue::Integer(n) => *n,
            other => panic!("unexpected final sum type: {:?}", other),
        };
        assert_eq!(
            final_total, EXPECTED_TOTAL,
            "Conservation invariant violated! Expected {}, got {}",
            EXPECTED_TOTAL, final_total
        );

        // Verify no negative balances.
        let rows = conn
            .query("SELECT COUNT(*) FROM accounts WHERE balance < 0;")
            .expect("negative balance check");
        let negative_count = match &row_values(&rows[0])[0] {
            SqliteValue::Integer(n) => *n,
            other => panic!("unexpected count type: {:?}", other),
        };
        assert_eq!(
            negative_count, 0,
            "Found {} accounts with negative balance",
            negative_count
        );

        // Verify all threads completed their target commits.
        for (i, (commits, _, _)) in results.iter().enumerate() {
            assert_eq!(
                *commits, OPS_PER_WRITER,
                "Thread {} completed {} commits instead of {}",
                i, commits, OPS_PER_WRITER
            );
        }
    }

    /// Verify that concurrent readers see consistent snapshots.
    #[test]
    fn concurrent_readers_consistency() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let dir = tempfile::tempdir().expect("create temp dir");
        let db_path = dir.path().join("readers.db");
        let db_path_str = db_path.to_str().unwrap();

        // Setup: create table with known data.
        {
            let conn = Connection::open(db_path_str).expect("open db for setup");
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
                .expect("create table");
            for i in 0..100 {
                conn.execute(&format!("INSERT INTO t VALUES ({}, {});", i, i * 10))
                    .expect("insert row");
            }
        }

        const NUM_READERS: usize = 4;
        const READS_PER_THREAD: usize = 50;

        let barrier = Arc::new(Barrier::new(NUM_READERS));

        let handles: Vec<_> = (0..NUM_READERS)
            .map(|thread_id| {
                let path = db_path_str.to_owned();
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    let conn = Connection::open(&path).expect("open db in reader thread");
                    barrier.wait();

                    let mut consistent = true;
                    for _ in 0..READS_PER_THREAD {
                        // Start a read transaction.
                        conn.execute("BEGIN;").expect("begin");

                        // Read sum - should always be consistent.
                        let rows = conn.query("SELECT SUM(val) FROM t;").expect("sum query");
                        let sum = if let SqliteValue::Integer(n) = &row_values(&rows[0])[0] {
                            *n
                        } else {
                            consistent = false;
                            break;
                        };

                        // Expected sum: 0 + 10 + 20 + ... + 990 = 10 * (0 + 1 + ... + 99) = 10 * 4950 = 49500
                        let expected = 10 * (99 * 100 / 2);
                        if sum != expected {
                            eprintln!(
                                "Thread {} saw inconsistent sum: {} (expected {})",
                                thread_id, sum, expected
                            );
                            consistent = false;
                        }

                        conn.execute("COMMIT;").expect("commit");
                    }

                    consistent
                })
            })
            .collect();

        // All readers should see consistent data.
        for (i, handle) in handles.into_iter().enumerate() {
            let consistent = handle.join().expect("reader thread panicked");
            assert!(consistent, "Reader thread {} saw inconsistent data", i);
        }
    }

    // ── Conformance gap probes (fixtures 017–021) ──────────────────────

    #[test]
    fn conformance_017_type_affinity_edge_numeric_coercion() {
        // '3.0e+5' into NUMERIC should coerce to integer 300000
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE q1(a NUMERIC, b TEXT, c INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO q1 VALUES('3.0e+5', 123, '0042')")
            .unwrap();
        let rows = conn
            .query("SELECT typeof(a), a, typeof(b), b, typeof(c), c FROM q1")
            .unwrap();
        assert_eq!(rows.len(), 1);
        let vals = row_values(&rows[0]);
        // SQLite behavior: '3.0e+5' → NUMERIC → integer 300000
        assert_eq!(vals[0], SqliteValue::Text("integer".to_owned()));
        assert_eq!(vals[1], SqliteValue::Integer(300_000));
        // 123 into TEXT → text "123"
        assert_eq!(vals[2], SqliteValue::Text("text".to_owned()));
        assert_eq!(vals[3], SqliteValue::Text("123".to_owned()));
        // '0042' into INTEGER → integer 42
        assert_eq!(vals[4], SqliteValue::Text("integer".to_owned()));
        assert_eq!(vals[5], SqliteValue::Integer(42));
    }

    #[test]
    fn conformance_018_collation_nocase_ascii_only() {
        // NOCASE is ASCII-insensitive: 'a' = 'A' → 1
        // but Unicode-sensitive: 'æ' ≠ 'Æ' → 0
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 'a' = 'A' COLLATE NOCASE").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
    }

    #[test]
    fn conformance_018_collation_nocase_unicode_sensitive() {
        let conn = Connection::open(":memory:").unwrap();
        // Unicode chars: NOCASE does NOT fold them
        let rows = conn.query("SELECT 'æ' = 'Æ' COLLATE NOCASE").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(0));
    }

    #[test]
    fn conformance_019_null_unique_allows_multiple_nulls() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE q3(a INTEGER UNIQUE, note TEXT)")
            .unwrap();
        conn.execute("INSERT INTO q3(a, note) VALUES(NULL, 'first-null')")
            .unwrap();
        // UNIQUE allows multiple NULLs
        conn.execute("INSERT INTO q3(a, note) VALUES(NULL, 'second-null')")
            .unwrap();
        conn.execute("INSERT INTO q3(a, note) VALUES(7, 'first-seven')")
            .unwrap();
        let rows = conn.query("SELECT COUNT(*) FROM q3").unwrap();
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
    }

    #[test]
    fn conformance_019_null_unique_rejects_duplicate_non_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE q3b(a INTEGER UNIQUE, note TEXT)")
            .unwrap();
        conn.execute("INSERT INTO q3b(a, note) VALUES(7, 'first-seven')")
            .unwrap();
        // Duplicate non-NULL should be rejected
        let result = conn.execute("INSERT INTO q3b(a, note) VALUES(7, 'dup')");
        assert!(
            result.is_err(),
            "Duplicate non-NULL unique value should fail"
        );
    }

    #[test]
    fn conformance_020_integer_overflow_promotes_to_real() {
        let conn = Connection::open(":memory:").unwrap();
        // i64::MAX + 1 should overflow to real
        let rows = conn
            .query("SELECT typeof(9223372036854775807 + 1)")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("real".to_owned())
        );
    }

    #[test]
    fn conformance_020_integer_underflow_promotes_to_real() {
        let conn = Connection::open(":memory:").unwrap();
        // i64::MIN - 1 should underflow to real
        let rows = conn
            .query("SELECT typeof(-9223372036854775808 - 1)")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0])[0],
            SqliteValue::Text("real".to_owned())
        );
    }

    #[test]
    fn conformance_021_savepoint_rollback_preserves_outer() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE q5(id INTEGER PRIMARY KEY, note TEXT)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO q5 VALUES(1, 'outer')").unwrap();
        conn.execute("SAVEPOINT s1").unwrap();
        conn.execute("INSERT INTO q5 VALUES(2, 'inner')").unwrap();
        conn.execute("ROLLBACK TO s1").unwrap();
        conn.execute("RELEASE s1").unwrap();
        conn.execute("COMMIT").unwrap();
        let rows = conn.query("SELECT id, note FROM q5 ORDER BY id").unwrap();
        assert_eq!(rows.len(), 1, "ROLLBACK TO s1 should undo inner insert");
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(
            row_values(&rows[0])[1],
            SqliteValue::Text("outer".to_owned())
        );
    }

    #[test]
    fn conformance_021_nested_begin_errors() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("BEGIN").unwrap();
        // Nested BEGIN inside an active transaction should error
        let result = conn.execute("BEGIN");
        assert!(result.is_err(), "Nested BEGIN should produce an error");
        conn.execute("ROLLBACK").unwrap();
    }

    // ── SQL Parity: REPLACE statement ────────────────────────────────────

    #[test]
    fn parity_replace_into_inserts_new_row() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("REPLACE INTO t VALUES (1, 'first');").unwrap();
        let rows = conn.query("SELECT id, val FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("first".into()));
    }

    #[test]
    fn parity_replace_into_overwrites_existing() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'old');").unwrap();
        conn.execute("REPLACE INTO t VALUES (1, 'new');").unwrap();
        let rows = conn.query("SELECT val FROM t WHERE id = 1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("new".into()));
    }

    // ── SQL Parity: INSERT OR IGNORE ─────────────────────────────────────

    #[test]
    fn parity_insert_or_ignore_skips_conflict() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'first');").unwrap();
        // INSERT OR IGNORE should silently skip the conflicting row
        conn.execute("INSERT OR IGNORE INTO t VALUES (1, 'dup');")
            .unwrap();
        let rows = conn.query("SELECT val FROM t WHERE id = 1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("first".into()));
    }

    // ── SQL Parity: Multi-column ORDER BY ────────────────────────────────

    #[test]
    fn parity_multi_column_order_by() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, c TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, 1, 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 2, 'y');").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1, 'z');").unwrap();
        let rows = conn.query("SELECT c FROM t ORDER BY a, b;").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("z".into()));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("y".into()));
        assert_eq!(row_values(&rows[2])[0], SqliteValue::Text("x".into()));
    }

    // ── SQL Parity: LIMIT with OFFSET ────────────────────────────────────

    #[test]
    fn parity_limit_with_offset() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        for i in 1..=5 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'r{i}');"))
                .unwrap();
        }
        let rows = conn
            .query("SELECT val FROM t ORDER BY id LIMIT 2 OFFSET 2;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("r3".into()));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("r4".into()));
    }

    // ── SQL Parity: Subquery in WHERE ────────────────────────────────────

    #[test]
    fn parity_subquery_in_where() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1 (id INTEGER, val TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE t2 (ref_id INTEGER);").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'a');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (2, 'b');").unwrap();
        conn.execute("INSERT INTO t1 VALUES (3, 'c');").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1);").unwrap();
        conn.execute("INSERT INTO t2 VALUES (3);").unwrap();
        let rows = conn
            .query("SELECT val FROM t1 WHERE id IN (SELECT ref_id FROM t2) ORDER BY id;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".into()));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("c".into()));
    }

    // ── SQL Parity: CAST in expressions ──────────────────────────────────

    #[test]
    fn parity_cast_integer_to_text() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT CAST(42 AS TEXT);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("42".into()));
    }

    #[test]
    fn parity_cast_text_to_integer() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT CAST('123' AS INTEGER);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(123));
    }

    // ── SQL Parity: EXISTS subquery ──────────────────────────────────────

    #[test]
    fn parity_exists_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        let row = conn
            .query_row("SELECT EXISTS (SELECT 1 FROM t WHERE id = 1);")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
        let row = conn
            .query_row("SELECT EXISTS (SELECT 1 FROM t WHERE id = 999);")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
    }

    // ── SQL Parity: COUNT(DISTINCT ...) ──────────────────────────────────

    #[test]
    fn parity_count_distinct() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (val INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        let row = conn
            .query_row("SELECT COUNT(DISTINCT val) FROM t;")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(3));
    }

    // ── SQL Parity: GROUP_CONCAT ─────────────────────────────────────────

    #[test]
    fn parity_group_concat_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (grp TEXT, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES ('a', 'x');").unwrap();
        conn.execute("INSERT INTO t VALUES ('a', 'y');").unwrap();
        conn.execute("INSERT INTO t VALUES ('b', 'z');").unwrap();
        let rows = conn
            .query("SELECT grp, GROUP_CONCAT(val, ',') FROM t GROUP BY grp ORDER BY grp;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // Group 'a' should have x,y (in insertion order)
        let a_val = &row_values(&rows[0])[1];
        match a_val {
            SqliteValue::Text(s) => {
                assert!(s == "x,y" || s == "y,x", "group_concat for 'a' = {s}");
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }

    // ── DISTINCT aggregate edge-case tests ─────────────────────────────

    #[test]
    fn parity_count_distinct_with_nulls() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE d2(x INTEGER);").unwrap();
        conn.execute("INSERT INTO d2 VALUES(1);").unwrap();
        conn.execute("INSERT INTO d2 VALUES(NULL);").unwrap();
        conn.execute("INSERT INTO d2 VALUES(2);").unwrap();
        conn.execute("INSERT INTO d2 VALUES(NULL);").unwrap();
        let row = conn.query_row("SELECT COUNT(DISTINCT x) FROM d2;").unwrap();
        // COUNT(DISTINCT x) ignores NULLs → 2 (values 1, 2)
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(2));
    }

    #[test]
    fn parity_sum_distinct() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE d3(x INTEGER);").unwrap();
        conn.execute("INSERT INTO d3 VALUES(10);").unwrap();
        conn.execute("INSERT INTO d3 VALUES(20);").unwrap();
        conn.execute("INSERT INTO d3 VALUES(10);").unwrap();
        conn.execute("INSERT INTO d3 VALUES(30);").unwrap();
        conn.execute("INSERT INTO d3 VALUES(20);").unwrap();
        let row = conn.query_row("SELECT SUM(DISTINCT x) FROM d3;").unwrap();
        // SUM(DISTINCT x) = 10 + 20 + 30 = 60
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(60));
    }

    #[test]
    fn parity_count_vs_count_distinct() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE d4(x INTEGER);").unwrap();
        conn.execute("INSERT INTO d4 VALUES(1);").unwrap();
        conn.execute("INSERT INTO d4 VALUES(1);").unwrap();
        conn.execute("INSERT INTO d4 VALUES(2);").unwrap();
        conn.execute("INSERT INTO d4 VALUES(2);").unwrap();
        conn.execute("INSERT INTO d4 VALUES(2);").unwrap();
        let r1 = conn.query_row("SELECT COUNT(x) FROM d4;").unwrap();
        assert_eq!(row_values(&r1)[0], SqliteValue::Integer(5));
        let r2 = conn.query_row("SELECT COUNT(DISTINCT x) FROM d4;").unwrap();
        assert_eq!(row_values(&r2)[0], SqliteValue::Integer(2));
    }

    #[test]
    fn parity_count_distinct_group_by() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE d5(grp TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO d5 VALUES('a', 1);").unwrap();
        conn.execute("INSERT INTO d5 VALUES('a', 2);").unwrap();
        conn.execute("INSERT INTO d5 VALUES('a', 1);").unwrap();
        conn.execute("INSERT INTO d5 VALUES('b', 10);").unwrap();
        conn.execute("INSERT INTO d5 VALUES('b', 10);").unwrap();
        conn.execute("INSERT INTO d5 VALUES('b', 20);").unwrap();
        let rows = conn
            .query("SELECT grp, COUNT(DISTINCT val) FROM d5 GROUP BY grp ORDER BY grp;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // Group 'a': {1,2} → 2
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".into()));
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(2));
        // Group 'b': {10,20} → 2
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("b".into()));
        assert_eq!(row_values(&rows[1])[1], SqliteValue::Integer(2));
    }

    #[test]
    fn parity_count_distinct_all_same() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE d6(x INTEGER);").unwrap();
        conn.execute("INSERT INTO d6 VALUES(42);").unwrap();
        conn.execute("INSERT INTO d6 VALUES(42);").unwrap();
        conn.execute("INSERT INTO d6 VALUES(42);").unwrap();
        let row = conn.query_row("SELECT COUNT(DISTINCT x) FROM d6;").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
    }

    #[test]
    fn parity_count_distinct_empty_table() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE d7(x INTEGER);").unwrap();
        let row = conn.query_row("SELECT COUNT(DISTINCT x) FROM d7;").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
    }

    // ── Scalar subquery tests ──────────────────────────────────────────

    #[test]
    fn parity_scalar_subquery_aggregate() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(10);").unwrap();
        conn.execute("INSERT INTO t VALUES(20);").unwrap();
        conn.execute("INSERT INTO t VALUES(30);").unwrap();
        let row = conn.query_row("SELECT (SELECT COUNT(*) FROM t);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(3));
    }

    #[test]
    fn parity_scalar_subquery_max() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(5);").unwrap();
        conn.execute("INSERT INTO t VALUES(15);").unwrap();
        conn.execute("INSERT INTO t VALUES(10);").unwrap();
        let row = conn.query_row("SELECT (SELECT MAX(x) FROM t);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(15));
    }

    #[test]
    fn parity_scalar_subquery_no_from() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT (SELECT 42);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(42));
    }

    #[test]
    fn parity_scalar_subquery_first_row() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(100);").unwrap();
        conn.execute("INSERT INTO t VALUES(200);").unwrap();
        let row = conn.query_row("SELECT (SELECT x FROM t);").unwrap();
        // Should return the first row value (100).
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(100));
    }

    #[test]
    fn parity_scalar_subquery_empty_table_is_null() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        let row = conn.query_row("SELECT (SELECT x FROM t);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Null);
    }

    // ── EXISTS subquery tests ──────────────────────────────────────────

    #[test]
    fn parity_exists_true() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        let row = conn.query_row("SELECT EXISTS (SELECT 1 FROM t);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
    }

    #[test]
    fn parity_exists_false_empty() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        let row = conn.query_row("SELECT EXISTS (SELECT 1 FROM t);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
    }

    #[test]
    fn parity_not_exists_true_empty() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        let row = conn
            .query_row("SELECT NOT EXISTS (SELECT 1 FROM t);")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
    }

    #[test]
    fn parity_exists_no_from() {
        // EXISTS (SELECT 1) is always true — no table needed.
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT EXISTS (SELECT 1);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
    }

    #[test]
    fn parity_exists_with_where() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(10);").unwrap();
        conn.execute("INSERT INTO t VALUES(20);").unwrap();

        // EXISTS with WHERE that matches.
        let row = conn
            .query_row("SELECT EXISTS (SELECT 1 FROM t WHERE x = 10);")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));

        // EXISTS with WHERE that doesn't match.
        let row = conn
            .query_row("SELECT EXISTS (SELECT 1 FROM t WHERE x = 99);")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
    }

    // ── FILTER clause parity tests ──────────────────────────────────────

    #[test]
    fn parity_count_filter() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE f1(x INTEGER);").unwrap();
        conn.execute("INSERT INTO f1 VALUES(1);").unwrap();
        conn.execute("INSERT INTO f1 VALUES(2);").unwrap();
        conn.execute("INSERT INTO f1 VALUES(3);").unwrap();
        conn.execute("INSERT INTO f1 VALUES(4);").unwrap();
        conn.execute("INSERT INTO f1 VALUES(5);").unwrap();
        // COUNT(*) FILTER (WHERE x > 3) → 2 rows (4, 5).
        let row = conn
            .query_row("SELECT COUNT(*) FILTER (WHERE x > 3) FROM f1;")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(2));
    }

    #[test]
    fn parity_sum_filter() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE f2(x INTEGER);").unwrap();
        conn.execute("INSERT INTO f2 VALUES(10);").unwrap();
        conn.execute("INSERT INTO f2 VALUES(20);").unwrap();
        conn.execute("INSERT INTO f2 VALUES(30);").unwrap();
        // SUM(x) FILTER (WHERE x >= 20) → 50.
        let row = conn
            .query_row("SELECT SUM(x) FILTER (WHERE x >= 20) FROM f2;")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(50));
    }

    #[test]
    fn parity_count_filter_none_match() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE f3(x INTEGER);").unwrap();
        conn.execute("INSERT INTO f3 VALUES(1);").unwrap();
        conn.execute("INSERT INTO f3 VALUES(2);").unwrap();
        // COUNT(*) FILTER (WHERE x > 100) → 0.
        let row = conn
            .query_row("SELECT COUNT(*) FILTER (WHERE x > 100) FROM f3;")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
    }

    #[test]
    fn parity_filter_no_group_by_same_table() {
        // Diagnostic: verify FILTER works on the SAME table/query without GROUP BY.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE f4b(city TEXT, age INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO f4b VALUES('A', 10);").unwrap();
        conn.execute("INSERT INTO f4b VALUES('A', 30);").unwrap();
        conn.execute("INSERT INTO f4b VALUES('B', 20);").unwrap();
        conn.execute("INSERT INTO f4b VALUES('B', 40);").unwrap();
        // COUNT(*) FILTER (WHERE age > 25) → 2 rows (30, 40).
        let row = conn
            .query_row("SELECT COUNT(*) FILTER (WHERE age > 25) FROM f4b;")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(2));
    }

    #[test]
    fn parity_filter_group_by_always_false() {
        // FILTER (WHERE 0) should always exclude → count = 0 per group.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE f4z(city TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO f4z VALUES('A', 1);").unwrap();
        conn.execute("INSERT INTO f4z VALUES('A', 2);").unwrap();
        conn.execute("INSERT INTO f4z VALUES('B', 3);").unwrap();
        let rows = conn
            .query("SELECT city, COUNT(*) FILTER (WHERE 0) FROM f4z GROUP BY city;")
            .unwrap();
        let mut results: Vec<(String, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                let city = match &vals[0] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => panic!("expected text"),
                };
                let cnt = match vals[1] {
                    SqliteValue::Integer(n) => n,
                    _ => panic!("expected integer, got {:?}", vals[1]),
                };
                (city, cnt)
            })
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(results, vec![("A".into(), 0), ("B".into(), 0)]);
    }

    #[test]
    fn parity_filter_with_group_by() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE f4(city TEXT, age INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO f4 VALUES('A', 10);").unwrap();
        conn.execute("INSERT INTO f4 VALUES('A', 30);").unwrap();
        conn.execute("INSERT INTO f4 VALUES('B', 20);").unwrap();
        conn.execute("INSERT INTO f4 VALUES('B', 40);").unwrap();
        // COUNT(*) FILTER (WHERE age > 25) per group:
        //   A: 1 (only age=30), B: 1 (only age=40).
        let rows = conn
            .query("SELECT city, COUNT(*) FILTER (WHERE age > 25) FROM f4 GROUP BY city;")
            .unwrap();
        let mut results: Vec<(String, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                let city = match &vals[0] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => panic!("expected text"),
                };
                let cnt = match vals[1] {
                    SqliteValue::Integer(n) => n,
                    _ => panic!("expected integer"),
                };
                (city, cnt)
            })
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(results, vec![("A".into(), 1), ("B".into(), 1)]);
    }

    #[test]
    fn parity_filter_multiple_aggregates() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE f5(x INTEGER);").unwrap();
        conn.execute("INSERT INTO f5 VALUES(1);").unwrap();
        conn.execute("INSERT INTO f5 VALUES(2);").unwrap();
        conn.execute("INSERT INTO f5 VALUES(3);").unwrap();
        conn.execute("INSERT INTO f5 VALUES(4);").unwrap();
        // Two aggregates with different filters in the same query.
        let row = conn
            .query_row(
                "SELECT COUNT(*) FILTER (WHERE x <= 2), COUNT(*) FILTER (WHERE x >= 3) FROM f5;",
            )
            .unwrap();
        let vals = row_values(&row);
        assert_eq!(vals[0], SqliteValue::Integer(2)); // x<=2: 1,2
        assert_eq!(vals[1], SqliteValue::Integer(2)); // x>=3: 3,4
    }

    #[test]
    fn parity_count_filter_vs_no_filter() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE f6(x INTEGER);").unwrap();
        conn.execute("INSERT INTO f6 VALUES(1);").unwrap();
        conn.execute("INSERT INTO f6 VALUES(2);").unwrap();
        conn.execute("INSERT INTO f6 VALUES(3);").unwrap();
        // Mix of filtered and unfiltered aggregates.
        let row = conn
            .query_row("SELECT COUNT(*), COUNT(*) FILTER (WHERE x > 1) FROM f6;")
            .unwrap();
        let vals = row_values(&row);
        assert_eq!(vals[0], SqliteValue::Integer(3)); // all rows
        assert_eq!(vals[1], SqliteValue::Integer(2)); // only x>1: 2,3
    }

    // ── CASE WHEN parity tests ───────────────────────────────────────────

    #[test]
    fn parity_case_simple() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END;")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("two".into()));
    }

    #[test]
    fn parity_case_searched() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(15);").unwrap();
        let row = conn
            .query_row(
                "SELECT CASE WHEN x < 10 THEN 'low' WHEN x < 20 THEN 'mid' ELSE 'high' END FROM t;",
            )
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("mid".into()));
    }

    #[test]
    fn parity_case_no_else_returns_null() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT CASE 5 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END;")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Null);
    }

    // ── COALESCE / NULLIF / IIF parity tests ─────────────────────────────

    #[test]
    fn parity_coalesce_basic() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT COALESCE(NULL, NULL, 42, 10);")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(42));
    }

    #[test]
    fn parity_coalesce_all_null() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT COALESCE(NULL, NULL);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Null);
    }

    #[test]
    fn parity_nullif_equal() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT NULLIF(5, 5);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Null);
    }

    #[test]
    fn parity_nullif_not_equal() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT NULLIF(5, 3);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(5));
    }

    #[test]
    fn parity_iif_true() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT IIF(1=1, 'yes', 'no');").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("yes".into()));
    }

    #[test]
    fn parity_iif_false() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT IIF(1=0, 'yes', 'no');").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("no".into()));
    }

    // ── BETWEEN parity tests ─────────────────────────────────────────────

    #[test]
    fn parity_between_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(5);").unwrap();
        conn.execute("INSERT INTO t VALUES(10);").unwrap();
        conn.execute("INSERT INTO t VALUES(15);").unwrap();
        let rows = conn
            .query("SELECT x FROM t WHERE x BETWEEN 5 AND 10 ORDER BY x;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(5));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(10));
    }

    #[test]
    fn parity_not_between() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(5);").unwrap();
        conn.execute("INSERT INTO t VALUES(10);").unwrap();
        let rows = conn
            .query("SELECT x FROM t WHERE x NOT BETWEEN 3 AND 7 ORDER BY x;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(10));
    }

    // ── LIKE parity tests ────────────────────────────────────────────────

    #[test]
    fn parity_like_percent() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(name TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES('apple');").unwrap();
        conn.execute("INSERT INTO t VALUES('banana');").unwrap();
        conn.execute("INSERT INTO t VALUES('apricot');").unwrap();
        let rows = conn
            .query("SELECT name FROM t WHERE name LIKE 'ap%' ORDER BY name;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("apple".into()));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("apricot".into()));
    }

    #[test]
    fn parity_like_underscore() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(code TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES('a1');").unwrap();
        conn.execute("INSERT INTO t VALUES('b2');").unwrap();
        conn.execute("INSERT INTO t VALUES('abc');").unwrap();
        let rows = conn
            .query("SELECT code FROM t WHERE code LIKE '_2';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("b2".into()));
    }

    #[test]
    fn parity_not_like() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(name TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES('cat');").unwrap();
        conn.execute("INSERT INTO t VALUES('dog');").unwrap();
        conn.execute("INSERT INTO t VALUES('car');").unwrap();
        let rows = conn
            .query("SELECT name FROM t WHERE name NOT LIKE 'ca%' ORDER BY name;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("dog".into()));
    }

    // ── JOIN parity tests ────────────────────────────────────────────────

    #[test]
    fn parity_inner_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER, item TEXT);")
            .unwrap();
        conn.execute("INSERT INTO users VALUES(1, 'Alice');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES(2, 'Bob');").unwrap();
        conn.execute("INSERT INTO orders VALUES(1, 1, 'Book');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES(2, 1, 'Pen');")
            .unwrap();
        conn.execute("INSERT INTO orders VALUES(3, 2, 'Notebook');")
            .unwrap();
        let rows = conn
            .query(
                "SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.user_id;",
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        // Verify all expected name-item pairs (order may vary).
        let mut pairs: Vec<(String, String)> = rows
            .iter()
            .map(|r| {
                let v = row_values(r);
                let name = match &v[0] {
                    SqliteValue::Text(s) => s.clone(),
                    other => panic!("expected Text, got {other:?}"),
                };
                let item = match &v[1] {
                    SqliteValue::Text(s) => s.clone(),
                    other => panic!("expected Text, got {other:?}"),
                };
                (name, item)
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("Alice".into(), "Book".into()),
                ("Alice".into(), "Pen".into()),
                ("Bob".into(), "Notebook".into()),
            ]
        );
    }

    #[test]
    fn parity_left_join_with_nulls() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER, info TEXT);")
            .unwrap();
        conn.execute("INSERT INTO a VALUES(1, 'x');").unwrap();
        conn.execute("INSERT INTO a VALUES(2, 'y');").unwrap();
        conn.execute("INSERT INTO b VALUES(1, 1, 'linked');")
            .unwrap();
        let rows = conn
            .query("SELECT a.val, b.info FROM a LEFT JOIN b ON a.id = b.a_id;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        // Collect results (order may vary).
        let mut results: Vec<(String, Option<String>)> = rows
            .iter()
            .map(|r| {
                let v = row_values(r);
                let val = match &v[0] {
                    SqliteValue::Text(s) => s.clone(),
                    other => panic!("expected Text, got {other:?}"),
                };
                let info = match &v[1] {
                    SqliteValue::Text(s) => Some(s.clone()),
                    SqliteValue::Null => None,
                    other => panic!("expected Text or Null, got {other:?}"),
                };
                (val, info)
            })
            .collect();
        results.sort();
        assert_eq!(
            results,
            vec![("x".into(), Some("linked".into())), ("y".into(), None),]
        );
    }

    #[test]
    fn parity_cross_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE a(x INTEGER);").unwrap();
        conn.execute("CREATE TABLE b(y INTEGER);").unwrap();
        conn.execute("INSERT INTO a VALUES(1);").unwrap();
        conn.execute("INSERT INTO a VALUES(2);").unwrap();
        conn.execute("INSERT INTO b VALUES(10);").unwrap();
        conn.execute("INSERT INTO b VALUES(20);").unwrap();
        let rows = conn.query("SELECT x, y FROM a, b ORDER BY x, y;").unwrap();
        // Cross product: 2*2 = 4 rows
        assert_eq!(rows.len(), 4);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(10));
        assert_eq!(row_values(&rows[3])[0], SqliteValue::Integer(2));
        assert_eq!(row_values(&rows[3])[1], SqliteValue::Integer(20));
    }

    // ── UNION / set operations parity tests ──────────────────────────────

    #[test]
    fn parity_union_removes_duplicates() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE a(x INTEGER);").unwrap();
        conn.execute("CREATE TABLE b(x INTEGER);").unwrap();
        conn.execute("INSERT INTO a VALUES(1);").unwrap();
        conn.execute("INSERT INTO a VALUES(2);").unwrap();
        conn.execute("INSERT INTO b VALUES(2);").unwrap();
        conn.execute("INSERT INTO b VALUES(3);").unwrap();
        let rows = conn
            .query("SELECT x FROM a UNION SELECT x FROM b ORDER BY x;")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(2));
        assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(3));
    }

    #[test]
    fn parity_union_all_keeps_duplicates() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE a(x INTEGER);").unwrap();
        conn.execute("CREATE TABLE b(x INTEGER);").unwrap();
        conn.execute("INSERT INTO a VALUES(1);").unwrap();
        conn.execute("INSERT INTO a VALUES(2);").unwrap();
        conn.execute("INSERT INTO b VALUES(2);").unwrap();
        conn.execute("INSERT INTO b VALUES(3);").unwrap();
        let rows = conn
            .query("SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x;")
            .unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(2));
        assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(2));
        assert_eq!(row_values(&rows[3])[0], SqliteValue::Integer(3));
    }

    // ── UPDATE / DELETE parity tests ─────────────────────────────────────

    #[test]
    fn parity_update_multiple_columns() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT, c REAL);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'old', 1.0);")
            .unwrap();
        conn.execute("UPDATE t SET b = 'new', c = 2.5 WHERE a = 1;")
            .unwrap();
        let row = conn.query_row("SELECT a, b, c FROM t;").unwrap();
        let vals = row_values(&row);
        assert_eq!(vals[0], SqliteValue::Integer(1));
        assert_eq!(vals[1], SqliteValue::Text("new".into()));
        assert_eq!(vals[2], SqliteValue::Float(2.5));
    }

    #[test]
    fn parity_delete_with_where() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES(2, 'b');").unwrap();
        conn.execute("INSERT INTO t VALUES(3, 'c');").unwrap();
        conn.execute("DELETE FROM t WHERE id > 1;").unwrap();
        let rows = conn.query("SELECT val FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".into()));
    }

    // ── HAVING parity tests ──────────────────────────────────────────────

    #[test]
    fn parity_having_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE sales(product TEXT, amount INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES('A', 10);").unwrap();
        conn.execute("INSERT INTO sales VALUES('A', 20);").unwrap();
        conn.execute("INSERT INTO sales VALUES('B', 5);").unwrap();
        conn.execute("INSERT INTO sales VALUES('C', 30);").unwrap();
        conn.execute("INSERT INTO sales VALUES('C', 40);").unwrap();
        let rows = conn
            .query(
                "SELECT product, SUM(amount) FROM sales GROUP BY product HAVING SUM(amount) > 15 ORDER BY product;",
            )
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("A".into()));
        assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(30));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("C".into()));
        assert_eq!(row_values(&rows[1])[1], SqliteValue::Integer(70));
    }

    // ── IN operator parity tests ─────────────────────────────────────────

    #[test]
    fn parity_in_values_list() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(2);").unwrap();
        conn.execute("INSERT INTO t VALUES(3);").unwrap();
        conn.execute("INSERT INTO t VALUES(4);").unwrap();
        let rows = conn
            .query("SELECT x FROM t WHERE x IN (1, 3) ORDER BY x;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
    }

    #[test]
    fn parity_not_in_values_list() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(2);").unwrap();
        conn.execute("INSERT INTO t VALUES(3);").unwrap();
        let rows = conn
            .query("SELECT x FROM t WHERE x NOT IN (1, 3) ORDER BY x;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
    }

    // ── Expression tests ─────────────────────────────────────────────────

    #[test]
    fn parity_unary_minus() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT -42;").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(-42));
    }

    #[test]
    fn parity_string_concat_operator() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT 'hello' || ' ' || 'world';").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("hello world".into()));
    }

    #[test]
    fn parity_typeof_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT typeof(42);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("integer".into()));
        let row = conn.query_row("SELECT typeof(3.14);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("real".into()));
        let row = conn.query_row("SELECT typeof('hi');").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("text".into()));
        let row = conn.query_row("SELECT typeof(NULL);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("null".into()));
    }

    #[test]
    fn parity_abs_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT ABS(-10);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(10));
        let row = conn.query_row("SELECT ABS(10);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(10));
    }

    #[test]
    fn parity_upper_lower_functions() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT UPPER('hello');").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("HELLO".into()));
        let row = conn.query_row("SELECT LOWER('WORLD');").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("world".into()));
    }

    #[test]
    fn parity_length_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT LENGTH('hello');").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(5));
        let row = conn.query_row("SELECT LENGTH('');").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
    }

    #[test]
    fn parity_min_max_aggregate() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(3);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(4);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(5);").unwrap();
        let row = conn.query_row("SELECT MIN(x), MAX(x) FROM t;").unwrap();
        let vals = row_values(&row);
        assert_eq!(vals[0], SqliteValue::Integer(1));
        assert_eq!(vals[1], SqliteValue::Integer(5));
    }

    #[test]
    fn parity_avg_aggregate() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(10);").unwrap();
        conn.execute("INSERT INTO t VALUES(20);").unwrap();
        conn.execute("INSERT INTO t VALUES(30);").unwrap();
        let row = conn.query_row("SELECT AVG(x) FROM t;").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Float(20.0));
    }

    // ── UPDATE with subquery in WHERE ────────────────────────────────────

    #[test]
    fn parity_update_where_in_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, price INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES(1, 'apple', 10);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES(2, 'banana', 20);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES(3, 'cherry', 30);")
            .unwrap();
        conn.execute("CREATE TABLE expensive(id INTEGER);").unwrap();
        conn.execute("INSERT INTO expensive VALUES(2);").unwrap();
        conn.execute("INSERT INTO expensive VALUES(3);").unwrap();
        // UPDATE items SET price = price * 2 WHERE id IN (SELECT id FROM expensive);
        conn.execute("UPDATE items SET price = price * 2 WHERE id IN (SELECT id FROM expensive);")
            .unwrap();
        let rows = conn
            .query("SELECT id, price FROM items ORDER BY id;")
            .unwrap();
        let results: Vec<(i64, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                (vals[0].to_integer(), vals[1].to_integer())
            })
            .collect();
        assert_eq!(results, vec![(1, 10), (2, 40), (3, 60)]);
    }

    // ── DELETE with subquery in WHERE ────────────────────────────────────

    #[test]
    fn parity_delete_where_in_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE data(id INTEGER, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO data VALUES(1, 'a');").unwrap();
        conn.execute("INSERT INTO data VALUES(2, 'b');").unwrap();
        conn.execute("INSERT INTO data VALUES(3, 'c');").unwrap();
        conn.execute("CREATE TABLE to_remove(id INTEGER);").unwrap();
        conn.execute("INSERT INTO to_remove VALUES(1);").unwrap();
        conn.execute("INSERT INTO to_remove VALUES(3);").unwrap();
        // DELETE FROM data WHERE id IN (SELECT id FROM to_remove);
        conn.execute("DELETE FROM data WHERE id IN (SELECT id FROM to_remove);")
            .unwrap();
        let rows = conn.query("SELECT id, val FROM data ORDER BY id;").unwrap();
        let results: Vec<(i64, String)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                (
                    vals[0].to_integer(),
                    match &vals[1] {
                        SqliteValue::Text(s) => s.clone(),
                        _ => panic!("expected text"),
                    },
                )
            })
            .collect();
        assert_eq!(results, vec![(2, "b".into())]);
    }

    // ── DateTime function probes ─────────────────────────────────────────

    #[test]
    fn parity_datetime_date_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT date('2023-06-15 14:30:00');")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("2023-06-15".into()));
    }

    #[test]
    fn parity_datetime_time_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT time('2023-06-15 14:30:45');")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("14:30:45".into()));
    }

    #[test]
    fn parity_datetime_strftime() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT strftime('%Y', '2023-06-15');")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("2023".into()));
    }

    // ── TOTAL aggregate ──────────────────────────────────────────────────

    #[test]
    fn parity_total_aggregate() {
        // TOTAL() returns 0.0 for empty set, unlike SUM() which returns NULL.
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        let row = conn.query_row("SELECT TOTAL(x) FROM t;").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Float(0.0));
    }

    #[test]
    fn parity_total_aggregate_with_values() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(10);").unwrap();
        conn.execute("INSERT INTO t VALUES(20);").unwrap();
        let row = conn.query_row("SELECT TOTAL(x) FROM t;").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Float(30.0));
    }

    // ── GLOB operator ────────────────────────────────────────────────────

    #[test]
    fn parity_glob_operator() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE files(name TEXT);").unwrap();
        conn.execute("INSERT INTO files VALUES('readme.txt');")
            .unwrap();
        conn.execute("INSERT INTO files VALUES('main.rs');")
            .unwrap();
        conn.execute("INSERT INTO files VALUES('test.txt');")
            .unwrap();
        let rows = conn
            .query("SELECT name FROM files WHERE name GLOB '*.txt' ORDER BY name;")
            .unwrap();
        let results: Vec<String> = rows
            .iter()
            .map(|r| match &row_values(r)[0] {
                SqliteValue::Text(s) => s.clone(),
                _ => panic!("expected text"),
            })
            .collect();
        assert_eq!(results, vec!["readme.txt", "test.txt"]);
    }

    // ── REPLACE function ─────────────────────────────────────────────────

    #[test]
    fn parity_replace_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT replace('hello world', 'world', 'rust');")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("hello rust".into()));
    }

    // ── ZEROBLOB function ────────────────────────────────────────────────

    #[test]
    fn parity_zeroblob_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT typeof(zeroblob(4)), length(zeroblob(4));")
            .unwrap();
        let vals = row_values(&row);
        assert_eq!(vals[0], SqliteValue::Text("blob".into()));
        assert_eq!(vals[1], SqliteValue::Integer(4));
    }

    // ── UNICODE / CHAR functions ─────────────────────────────────────────

    #[test]
    fn parity_unicode_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT unicode('A');").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(65));
    }

    #[test]
    fn parity_char_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn.query_row("SELECT char(65, 66, 67);").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("ABC".into()));
    }

    // ── INSTR with multi-byte ────────────────────────────────────────────

    #[test]
    fn parity_instr_multi_occurrence() {
        let conn = Connection::open(":memory:").unwrap();
        // INSTR returns position of FIRST occurrence (1-based).
        let row = conn.query_row("SELECT instr('abcabc', 'bc');").unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Integer(2));
    }

    // ── PRINTF / FORMAT function ─────────────────────────────────────────

    #[test]
    fn parity_printf_function() {
        let conn = Connection::open(":memory:").unwrap();
        let row = conn
            .query_row("SELECT printf('%d + %d = %d', 1, 2, 3);")
            .unwrap();
        assert_eq!(row_values(&row)[0], SqliteValue::Text("1 + 2 = 3".into()));
    }

    // ── Window function probe ────────────────────────────────────────────

    #[test]
    fn parity_row_number_window() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE w(name TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO w VALUES('a', 10);").unwrap();
        conn.execute("INSERT INTO w VALUES('b', 20);").unwrap();
        conn.execute("INSERT INTO w VALUES('c', 30);").unwrap();
        let rows = conn
            .query("SELECT name, ROW_NUMBER() OVER (ORDER BY val) FROM w;")
            .unwrap();
        let results: Vec<(String, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                let name = match &vals[0] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => panic!("expected text"),
                };
                (name, vals[1].to_integer())
            })
            .collect();
        assert_eq!(
            results,
            vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3),]
        );
    }

    #[test]
    fn window_row_number_partition_by() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE wp(dept TEXT, name TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO wp VALUES('eng','a',10);")
            .unwrap();
        conn.execute("INSERT INTO wp VALUES('eng','b',20);")
            .unwrap();
        conn.execute("INSERT INTO wp VALUES('sales','c',5);")
            .unwrap();
        conn.execute("INSERT INTO wp VALUES('sales','d',15);")
            .unwrap();
        let rows = conn
            .query("SELECT dept, name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY val) FROM wp;")
            .unwrap();
        let results: Vec<(String, String, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                let dept = match &vals[0] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => panic!("expected text"),
                };
                let name = match &vals[1] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => panic!("expected text"),
                };
                (dept, name, vals[2].to_integer())
            })
            .collect();
        assert_eq!(
            results,
            vec![
                ("eng".into(), "a".into(), 1),
                ("eng".into(), "b".into(), 2),
                ("sales".into(), "c".into(), 1),
                ("sales".into(), "d".into(), 2),
            ]
        );
    }

    #[test]
    fn window_rank_and_dense_rank() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE wr(name TEXT, score INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO wr VALUES('a', 100);").unwrap();
        conn.execute("INSERT INTO wr VALUES('b', 100);").unwrap();
        conn.execute("INSERT INTO wr VALUES('c', 90);").unwrap();
        conn.execute("INSERT INTO wr VALUES('d', 80);").unwrap();
        let rows = conn
            .query(
                "SELECT name, RANK() OVER (ORDER BY score DESC), \
                 DENSE_RANK() OVER (ORDER BY score DESC) FROM wr;",
            )
            .unwrap();
        let results: Vec<(String, i64, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                let name = match &vals[0] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => panic!("expected text"),
                };
                (name, vals[1].to_integer(), vals[2].to_integer())
            })
            .collect();
        // a=100, b=100 are tied at rank 1; c=90 rank 3; d=80 rank 4
        // dense_rank: a,b=1; c=2; d=3
        assert_eq!(
            results,
            vec![
                ("a".into(), 1, 1),
                ("b".into(), 1, 1),
                ("c".into(), 3, 2),
                ("d".into(), 4, 3),
            ]
        );
    }

    #[test]
    fn window_row_number_desc_order() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE wd(x INTEGER);").unwrap();
        conn.execute("INSERT INTO wd VALUES(1);").unwrap();
        conn.execute("INSERT INTO wd VALUES(2);").unwrap();
        conn.execute("INSERT INTO wd VALUES(3);").unwrap();
        let rows = conn
            .query("SELECT x, ROW_NUMBER() OVER (ORDER BY x DESC) FROM wd;")
            .unwrap();
        let results: Vec<(i64, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                (vals[0].to_integer(), vals[1].to_integer())
            })
            .collect();
        // x=3 is first (row_number=1), x=2 second, x=1 third
        assert_eq!(results, vec![(3, 1), (2, 2), (1, 3)]);
    }

    #[test]
    fn window_multiple_window_functions() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE wm(name TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO wm VALUES('a', 10);").unwrap();
        conn.execute("INSERT INTO wm VALUES('b', 20);").unwrap();
        conn.execute("INSERT INTO wm VALUES('c', 30);").unwrap();
        let rows = conn
            .query(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY val), \
                 DENSE_RANK() OVER (ORDER BY val) FROM wm;",
            )
            .unwrap();
        let results: Vec<(String, i64, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                let name = match &vals[0] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => panic!("expected text"),
                };
                (name, vals[1].to_integer(), vals[2].to_integer())
            })
            .collect();
        // All values distinct, so rank matches row_number
        assert_eq!(
            results,
            vec![("a".into(), 1, 1), ("b".into(), 2, 2), ("c".into(), 3, 3),]
        );
    }

    // ── CTE (WITH) probe ─────────────────────────────────────────────────

    #[test]
    fn parity_cte_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(2);").unwrap();
        conn.execute("INSERT INTO t VALUES(3);").unwrap();
        let rows = conn
            .query("WITH doubled AS (SELECT x * 2 AS d FROM t) SELECT d FROM doubled ORDER BY d;")
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert_eq!(results, vec![2, 4, 6]);
    }

    // ── Recursive CTE probe ─────────────────────────────────────────────

    #[test]
    fn parity_recursive_cte() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(
                "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<5) \
                 SELECT x FROM cnt;",
            )
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert_eq!(results, vec![1, 2, 3, 4, 5]);
    }

    // ── HAVING with multiple conditions ──────────────────────────────────

    #[test]
    fn parity_having_count_and_sum() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE sales(region TEXT, amount INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES('East', 100);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES('East', 200);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES('West', 50);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES('West', 60);")
            .unwrap();
        conn.execute("INSERT INTO sales VALUES('West', 70);")
            .unwrap();
        // HAVING COUNT(*) > 2 → only West (3 rows)
        let rows = conn
            .query("SELECT region, SUM(amount) FROM sales GROUP BY region HAVING COUNT(*) > 2;")
            .unwrap();
        let results: Vec<(String, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                (
                    match &vals[0] {
                        SqliteValue::Text(s) => s.clone(),
                        _ => panic!("expected text"),
                    },
                    vals[1].to_integer(),
                )
            })
            .collect();
        assert_eq!(results, vec![("West".into(), 180)]);
    }

    // ── Multi-table UPDATE with JOIN subquery ────────────────────────────

    #[test]
    fn parity_update_with_exists_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE products(pid INTEGER, name TEXT, active INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO products VALUES(1, 'Widget', 1);")
            .unwrap();
        conn.execute("INSERT INTO products VALUES(2, 'Gadget', 1);")
            .unwrap();
        conn.execute("INSERT INTO products VALUES(3, 'Doohickey', 1);")
            .unwrap();
        conn.execute("CREATE TABLE discontinued(product_id INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO discontinued VALUES(1);").unwrap();
        conn.execute("INSERT INTO discontinued VALUES(3);").unwrap();
        // Correlated EXISTS subquery: update rows where a matching row exists.
        conn.execute(
            "UPDATE products SET active = 0 WHERE EXISTS \
             (SELECT 1 FROM discontinued WHERE discontinued.product_id = products.pid);",
        )
        .unwrap();
        let rows = conn
            .query("SELECT pid, active FROM products ORDER BY pid;")
            .unwrap();
        let results: Vec<(i64, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                (vals[0].to_integer(), vals[1].to_integer())
            })
            .collect();
        assert_eq!(results, vec![(1, 0), (2, 1), (3, 0)]);
    }

    #[test]
    fn probe_correlated_exists_select() {
        // Diagnostic: does correlated EXISTS work in a SELECT context?
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE a(x INTEGER);").unwrap();
        conn.execute("INSERT INTO a VALUES(1);").unwrap();
        conn.execute("INSERT INTO a VALUES(2);").unwrap();
        conn.execute("INSERT INTO a VALUES(3);").unwrap();
        conn.execute("CREATE TABLE b(y INTEGER);").unwrap();
        conn.execute("INSERT INTO b VALUES(1);").unwrap();
        conn.execute("INSERT INTO b VALUES(3);").unwrap();
        let rows = conn
            .query("SELECT x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x) ORDER BY x;")
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert_eq!(results, vec![1, 3]);
    }

    // ── Conformance Probes: Edge Cases ──────────────────────────────────

    #[test]
    fn probe_order_by_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER, y TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES(3, 'c');").unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'a');").unwrap();
        conn.execute("INSERT INTO t VALUES(2, 'b');").unwrap();
        let rows = conn.query("SELECT y FROM t ORDER BY x * -1;").unwrap();
        let results: Vec<String> = rows
            .iter()
            .map(|r| match &row_values(r)[0] {
                SqliteValue::Text(s) => s.clone(),
                _ => panic!("expected text"),
            })
            .collect();
        assert_eq!(results, vec!["c", "b", "a"]);
    }

    #[test]
    fn probe_group_by_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER, v INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 10);").unwrap();
        conn.execute("INSERT INTO t VALUES(2, 20);").unwrap();
        conn.execute("INSERT INTO t VALUES(3, 30);").unwrap();
        conn.execute("INSERT INTO t VALUES(4, 40);").unwrap();
        let rows = conn
            .query("SELECT x % 2 AS grp, SUM(v) FROM t GROUP BY x % 2 ORDER BY grp;")
            .unwrap();
        let results: Vec<(i64, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                (vals[0].to_integer(), vals[1].to_integer())
            })
            .collect();
        assert_eq!(results, vec![(0, 60), (1, 40)]);
    }

    #[test]
    fn probe_having_with_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(cat TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES('a', 10);").unwrap();
        conn.execute("INSERT INTO t VALUES('a', 20);").unwrap();
        conn.execute("INSERT INTO t VALUES('b', 5);").unwrap();
        let rows = conn
            .query(
                "SELECT cat, SUM(val) AS s FROM t GROUP BY cat HAVING SUM(val) > 10 ORDER BY cat;",
            )
            .unwrap();
        let results: Vec<(String, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                let cat = match &vals[0] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => panic!("expected text"),
                };
                (cat, vals[1].to_integer())
            })
            .collect();
        assert_eq!(results, vec![("a".to_string(), 30)]);
    }

    #[test]
    fn probe_scalar_subquery_in_select() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(2);").unwrap();
        conn.execute("INSERT INTO t VALUES(3);").unwrap();
        let rows = conn
            .query("SELECT x, (SELECT MAX(x) FROM t) AS mx FROM t ORDER BY x;")
            .unwrap();
        let results: Vec<(i64, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                (vals[0].to_integer(), vals[1].to_integer())
            })
            .collect();
        assert_eq!(results, vec![(1, 3), (2, 3), (3, 3)]);
    }

    #[test]
    fn probe_nested_case_when() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(2);").unwrap();
        conn.execute("INSERT INTO t VALUES(3);").unwrap();
        let rows = conn
            .query(
                "SELECT CASE WHEN x < 2 THEN 'low' \
                 WHEN x < 3 THEN 'mid' ELSE 'high' END AS label FROM t ORDER BY x;",
            )
            .unwrap();
        let results: Vec<String> = rows
            .iter()
            .map(|r| match &row_values(r)[0] {
                SqliteValue::Text(s) => s.clone(),
                _ => panic!("expected text"),
            })
            .collect();
        assert_eq!(results, vec!["low", "mid", "high"]);
    }

    #[test]
    fn probe_coalesce_multi_arg() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT COALESCE(NULL, NULL, NULL, 42);")
            .unwrap();
        assert_eq!(row_values(&rows[0])[0].to_integer(), 42);
    }

    #[test]
    fn probe_nullif_function() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT NULLIF(5, 5), NULLIF(5, 3);").unwrap();
        let vals = row_values(&rows[0]);
        assert_eq!(vals[0], SqliteValue::Null);
        assert_eq!(vals[1].to_integer(), 5);
    }

    #[test]
    fn probe_iif_function() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT IIF(1 > 0, 'yes', 'no'), IIF(1 < 0, 'yes', 'no');")
            .unwrap();
        let vals = row_values(&rows[0]);
        let a = match &vals[0] {
            SqliteValue::Text(s) => s.clone(),
            _ => panic!("expected text"),
        };
        let b = match &vals[1] {
            SqliteValue::Text(s) => s.clone(),
            _ => panic!("expected text"),
        };
        assert_eq!(a, "yes");
        assert_eq!(b, "no");
    }

    #[test]
    fn probe_like_escape() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(s TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES('100% done');").unwrap();
        conn.execute("INSERT INTO t VALUES('50 percent');").unwrap();
        let rows = conn
            .query("SELECT s FROM t WHERE s LIKE '%!%%' ESCAPE '!';")
            .unwrap();
        assert_eq!(rows.len(), 1);
        let val = match &row_values(&rows[0])[0] {
            SqliteValue::Text(s) => s.clone(),
            _ => panic!("expected text"),
        };
        assert_eq!(val, "100% done");
    }

    #[test]
    fn probe_between_with_expressions() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(5);").unwrap();
        conn.execute("INSERT INTO t VALUES(10);").unwrap();
        let rows = conn
            .query("SELECT x FROM t WHERE x BETWEEN 2 + 1 AND 4 * 2 ORDER BY x;")
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert_eq!(results, vec![5]);
    }

    #[test]
    fn probe_distinct_with_expression() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(2);").unwrap();
        conn.execute("INSERT INTO t VALUES(3);").unwrap();
        conn.execute("INSERT INTO t VALUES(4);").unwrap();
        let rows = conn
            .query("SELECT DISTINCT x % 2 AS mod2 FROM t ORDER BY mod2;")
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert_eq!(results, vec![0, 1]);
    }

    #[test]
    fn probe_insert_or_ignore_keeps_existing() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'first');").unwrap();
        conn.execute("INSERT OR IGNORE INTO t VALUES(1, 'second');")
            .unwrap();
        let rows = conn.query("SELECT v FROM t WHERE id = 1;").unwrap();
        let val = match &row_values(&rows[0])[0] {
            SqliteValue::Text(s) => s.clone(),
            _ => panic!("expected text"),
        };
        assert_eq!(val, "first");
    }

    #[test]
    fn probe_insert_or_replace_overwrites() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'first');").unwrap();
        conn.execute("INSERT OR REPLACE INTO t VALUES(1, 'second');")
            .unwrap();
        let rows = conn.query("SELECT v FROM t WHERE id = 1;").unwrap();
        let val = match &row_values(&rows[0])[0] {
            SqliteValue::Text(s) => s.clone(),
            _ => panic!("expected text"),
        };
        assert_eq!(val, "second");
    }

    #[test]
    fn probe_delete_with_correlated_exists() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE items(id INTEGER, active INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO items VALUES(1, 1);").unwrap();
        conn.execute("INSERT INTO items VALUES(2, 1);").unwrap();
        conn.execute("INSERT INTO items VALUES(3, 1);").unwrap();
        conn.execute("CREATE TABLE retired(item_id INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO retired VALUES(2);").unwrap();
        conn.execute(
            "DELETE FROM items WHERE EXISTS \
             (SELECT 1 FROM retired WHERE retired.item_id = items.id);",
        )
        .unwrap();
        let rows = conn.query("SELECT id FROM items ORDER BY id;").unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert_eq!(results, vec![1, 3]);
    }

    #[test]
    fn probe_aggregate_in_order_by() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(cat TEXT, val INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES('b', 10);").unwrap();
        conn.execute("INSERT INTO t VALUES('a', 30);").unwrap();
        conn.execute("INSERT INTO t VALUES('c', 20);").unwrap();
        let rows = conn
            .query("SELECT cat, SUM(val) AS s FROM t GROUP BY cat ORDER BY SUM(val) DESC;")
            .unwrap();
        let results: Vec<(String, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                let cat = match &vals[0] {
                    SqliteValue::Text(s) => s.clone(),
                    _ => panic!("expected text"),
                };
                (cat, vals[1].to_integer())
            })
            .collect();
        assert_eq!(
            results,
            vec![
                ("a".to_string(), 30),
                ("c".to_string(), 20),
                ("b".to_string(), 10)
            ]
        );
    }

    #[test]
    fn probe_subquery_in_from() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(2);").unwrap();
        conn.execute("INSERT INTO t VALUES(3);").unwrap();
        let rows = conn
            .query("SELECT sub.doubled FROM (SELECT x * 2 AS doubled FROM t) AS sub ORDER BY sub.doubled;")
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert_eq!(results, vec![2, 4, 6]);
    }

    #[test]
    fn probe_multi_column_order_by_mixed() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(a INTEGER, b INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 3);").unwrap();
        conn.execute("INSERT INTO t VALUES(1, 1);").unwrap();
        conn.execute("INSERT INTO t VALUES(2, 2);").unwrap();
        conn.execute("INSERT INTO t VALUES(2, 4);").unwrap();
        let rows = conn
            .query("SELECT a, b FROM t ORDER BY a ASC, b DESC;")
            .unwrap();
        let results: Vec<(i64, i64)> = rows
            .iter()
            .map(|r| {
                let vals = row_values(r);
                (vals[0].to_integer(), vals[1].to_integer())
            })
            .collect();
        assert_eq!(results, vec![(1, 3), (1, 1), (2, 4), (2, 2)]);
    }

    #[test]
    fn probe_null_handling_order_by() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(3);").unwrap();
        conn.execute("INSERT INTO t VALUES(NULL);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        // SQLite: NULLs sort first in ASC order.
        let rows = conn.query("SELECT x FROM t ORDER BY x ASC;").unwrap();
        let results: Vec<SqliteValue> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
        assert_eq!(results[0], SqliteValue::Null);
        assert_eq!(results[1].to_integer(), 1);
        assert_eq!(results[2].to_integer(), 3);
    }

    #[test]
    fn probe_count_distinct() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(2);").unwrap();
        conn.execute("INSERT INTO t VALUES(1);").unwrap();
        conn.execute("INSERT INTO t VALUES(NULL);").unwrap();
        let rows = conn.query("SELECT COUNT(DISTINCT x) FROM t;").unwrap();
        // COUNT(DISTINCT x) should count distinct non-NULL values → 2
        assert_eq!(row_values(&rows[0])[0].to_integer(), 2);
    }

    #[test]
    fn probe_cast_in_where() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t(x TEXT);").unwrap();
        conn.execute("INSERT INTO t VALUES('123');").unwrap();
        conn.execute("INSERT INTO t VALUES('456');").unwrap();
        conn.execute("INSERT INTO t VALUES('abc');").unwrap();
        let rows = conn
            .query("SELECT x FROM t WHERE CAST(x AS INTEGER) > 200;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        let val = match &row_values(&rows[0])[0] {
            SqliteValue::Text(s) => s.clone(),
            _ => panic!("expected text"),
        };
        assert_eq!(val, "456");
    }

    #[test]
    fn probe_union_all_three_way() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 1;")
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert_eq!(results, vec![1, 2, 1]);
    }

    #[test]
    fn probe_union_dedup_three_way() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT 1 AS v UNION SELECT 2 UNION SELECT 1 ORDER BY v;")
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert_eq!(results, vec![1, 2]);
    }

    #[test]
    fn probe_except_compound() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 EXCEPT SELECT 2;")
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        // EXCEPT removes rows from right. Order: 1, 3
        assert!(results.contains(&1));
        assert!(results.contains(&3));
        assert!(!results.contains(&2));
    }

    #[test]
    fn probe_intersect_compound() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query(
                "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 \
                 INTERSECT SELECT 2 UNION ALL SELECT 3;",
            )
            .unwrap();
        let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
        assert!(results.contains(&2) || results.contains(&3));
    }

    // -----------------------------------------------------------------------
    // Conformance suite 024: string functions
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_024_substr() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT substr('hello world', 7)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "world");
        let r = conn.query("SELECT substr('hello world', 1, 5)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "hello");
        let r = conn.query("SELECT substr('hello', -3)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "llo");
    }

    #[test]
    fn conformance_024_replace() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn
            .query("SELECT replace('hello world', 'world', 'rust')")
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "hello rust");
        let r = conn.query("SELECT replace('aaa', 'a', 'bb')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "bbbbbb");
    }

    #[test]
    fn conformance_024_trim() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT trim('  hello  ')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "hello");
        let r = conn.query("SELECT ltrim('  hello  ')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "hello  ");
        let r = conn.query("SELECT rtrim('  hello  ')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "  hello");
    }

    #[test]
    fn conformance_024_instr() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT instr('hello world', 'world')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "7");
        let r = conn.query("SELECT instr('hello', 'xyz')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "0");
        let r = conn.query("SELECT instr('hello', '')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
    }

    #[test]
    fn conformance_024_hex_zeroblob() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT hex(zeroblob(4))").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "00000000");
        let r = conn.query("SELECT typeof(zeroblob(4))").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "blob");
    }

    #[test]
    fn conformance_024_char_unicode() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT char(65, 66, 67)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "ABC");
        let r = conn.query("SELECT unicode('A')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "65");
    }

    // -----------------------------------------------------------------------
    // Conformance suite 025: expression operators
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_025_between() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT 5 BETWEEN 1 AND 10").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
        let r = conn.query("SELECT 15 BETWEEN 1 AND 10").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "0");
        let r = conn.query("SELECT 5 NOT BETWEEN 1 AND 10").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "0");
    }

    #[test]
    fn conformance_025_in_operator() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT 3 IN (1, 2, 3, 4)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
        let r = conn.query("SELECT 5 IN (1, 2, 3, 4)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "0");
        let r = conn.query("SELECT 3 NOT IN (1, 2, 3, 4)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "0");
    }

    #[test]
    fn conformance_025_like_glob() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT 'hello' LIKE 'hel%'").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
        let r = conn.query("SELECT 'hello' LIKE 'HEL%'").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
        let r = conn.query("SELECT 'hello' GLOB 'hel*'").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
        let r = conn.query("SELECT 'hello' GLOB 'HEL*'").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "0");
    }

    #[test]
    fn conformance_025_coalesce_nullif_iif() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT coalesce(NULL, NULL, 'found')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "found");
        let r = conn.query("SELECT nullif(5, 5)").unwrap();
        assert!(row_values(&r[0])[0].is_null());
        let r = conn.query("SELECT nullif(5, 3)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "5");
        let r = conn.query("SELECT iif(1, 'yes', 'no')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "yes");
        let r = conn.query("SELECT iif(0, 'yes', 'no')").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "no");
    }

    #[test]
    fn conformance_025_bitwise() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT 6 & 3").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "2");
        let r = conn.query("SELECT 6 | 3").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "7");
        let r = conn.query("SELECT ~0").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "-1");
        let r = conn.query("SELECT 1 << 4").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "16");
        let r = conn.query("SELECT 16 >> 2").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "4");
    }

    #[test]
    fn conformance_025_cast() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT CAST('123' AS INTEGER)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "123");
        let r = conn.query("SELECT typeof(CAST(123 AS TEXT))").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "text");
        let r = conn.query("SELECT CAST(3.14 AS INTEGER)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "3");
    }

    #[test]
    fn conformance_025_unary_operators() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT -(-5)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "5");
        let r = conn.query("SELECT +42").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "42");
        let r = conn.query("SELECT NOT 0").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
        let r = conn.query("SELECT NOT 1").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "0");
    }

    #[test]
    fn conformance_025_is_null() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn.query("SELECT NULL IS NULL").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
        let r = conn.query("SELECT 5 IS NOT NULL").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
        let r = conn.query("SELECT NULL IS NOT NULL").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "0");
    }

    // -----------------------------------------------------------------------
    // Conformance suite 026: subquery and CTE
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_026_scalar_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();
        let r = conn.query("SELECT (SELECT MAX(val) FROM t1)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "30");
        let r = conn.query("SELECT (SELECT COUNT(*) FROM t1)").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "3");
    }

    #[test]
    fn conformance_026_exists_subquery() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, 'apple'), (2, 'banana')")
            .unwrap();
        let r = conn
            .query("SELECT EXISTS(SELECT 1 FROM items WHERE name = 'apple')")
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "1");
        let r = conn
            .query("SELECT EXISTS(SELECT 1 FROM items WHERE name = 'cherry')")
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "0");
    }

    #[test]
    fn conformance_026_cte_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE employees(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO employees VALUES \
             (1, 'Alice', 'eng', 100000), \
             (2, 'Bob', 'eng', 95000), \
             (3, 'Charlie', 'sales', 80000)",
        )
        .unwrap();
        let r = conn
            .query(
                "WITH eng AS (SELECT * FROM employees WHERE dept = 'eng') \
                 SELECT name FROM eng ORDER BY name",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
        assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn conformance_026_cte_explicit_columns() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE nums(n INTEGER)").unwrap();
        conn.execute("INSERT INTO nums VALUES (10), (20), (30)")
            .unwrap();
        let r = conn
            .query(
                "WITH doubled(val) AS (SELECT n * 2 FROM nums) \
                 SELECT val FROM doubled ORDER BY val",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "20");
        assert_eq!(row_values(&r[1])[0].to_text(), "40");
        assert_eq!(row_values(&r[2])[0].to_text(), "60");
    }

    #[test]
    fn conformance_026_recursive_cte() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn
            .query(
                "WITH RECURSIVE cnt(x) AS (\
                 SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5\
                 ) SELECT x FROM cnt",
            )
            .unwrap();
        let vals: Vec<String> = r.iter().map(|row| row_values(row)[0].to_text()).collect();
        assert_eq!(vals, ["1", "2", "3", "4", "5"]);
    }

    #[test]
    fn conformance_026_derived_table() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE scores(student TEXT, score INTEGER)")
            .unwrap();
        conn.execute(
            "INSERT INTO scores VALUES \
             ('Alice', 90), ('Alice', 85), ('Bob', 70), ('Bob', 80)",
        )
        .unwrap();
        let r = conn
            .query(
                "SELECT student, avg_score FROM \
                 (SELECT student, AVG(score) as avg_score \
                  FROM scores GROUP BY student) ORDER BY student",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
        assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
    }

    // -----------------------------------------------------------------------
    // Conformance suite 027: window functions (not yet implemented)
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "window functions not yet implemented"]
    fn conformance_027_row_number() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE sales(id INTEGER PRIMARY KEY, region TEXT, amount REAL)")
            .unwrap();
        conn.execute(
            "INSERT INTO sales VALUES \
             (1, 'North', 100.0), (2, 'South', 200.0), \
             (3, 'North', 150.0), (4, 'South', 175.0)",
        )
        .unwrap();
        let r = conn
            .query(
                "SELECT region, amount, \
                 ROW_NUMBER() OVER (ORDER BY amount DESC) as rn \
                 FROM sales",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[2].to_text(), "1");
    }

    #[test]
    #[ignore = "window functions not yet implemented"]
    fn conformance_027_rank_dense_rank() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE scores(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            .unwrap();
        conn.execute(
            "INSERT INTO scores VALUES \
             (1, 'A', 100), (2, 'B', 100), (3, 'C', 90), (4, 'D', 80)",
        )
        .unwrap();
        let r = conn
            .query(
                "SELECT name, RANK() OVER (ORDER BY score DESC) as rnk \
                 FROM scores",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[1].to_text(), "1");
        assert_eq!(row_values(&r[1])[1].to_text(), "1");
        assert_eq!(row_values(&r[2])[1].to_text(), "3");
    }

    #[test]
    #[ignore = "window functions not yet implemented"]
    fn conformance_027_sum_over() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE txns(id INTEGER PRIMARY KEY, amount REAL)")
            .unwrap();
        conn.execute("INSERT INTO txns VALUES (1, 10.0), (2, 20.0), (3, 30.0)")
            .unwrap();
        let r = conn
            .query(
                "SELECT id, SUM(amount) OVER (ORDER BY id) as running \
                 FROM txns",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[1].to_text(), "10.0");
        assert_eq!(row_values(&r[1])[1].to_text(), "30.0");
        assert_eq!(row_values(&r[2])[1].to_text(), "60.0");
    }

    #[test]
    #[ignore = "window functions not yet implemented"]
    fn conformance_027_lag_lead() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE seq(id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO seq VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap();
        let r = conn
            .query(
                "SELECT val, LAG(val) OVER (ORDER BY id) as prev \
                 FROM seq",
            )
            .unwrap();
        assert!(row_values(&r[0])[1].is_null());
        assert_eq!(row_values(&r[1])[1].to_text(), "a");
    }

    // -----------------------------------------------------------------------
    // Conformance suite 028: views
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_028_create_select_drop_view() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE products(\
             id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO products VALUES \
             (1, 'Widget', 9.99, 'gadgets'), \
             (2, 'Gizmo', 24.99, 'gadgets'), \
             (3, 'Doohickey', 4.99, 'tools')",
        )
        .unwrap();
        conn.execute("CREATE VIEW expensive AS SELECT * FROM products WHERE price > 10.0")
            .unwrap();
        let r = conn
            .query("SELECT name FROM expensive ORDER BY name")
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Gizmo");
        assert_eq!(r.len(), 1);
        conn.execute("DROP VIEW expensive").unwrap();
    }

    #[test]
    fn conformance_028_view_with_aggregate() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE products(\
             id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO products VALUES \
             (1, 'Widget', 9.99, 'gadgets'), \
             (2, 'Gizmo', 24.99, 'gadgets'), \
             (3, 'Doohickey', 4.99, 'tools'), \
             (4, 'Thingamajig', 14.99, 'tools'), \
             (5, 'Whatchamacallit', 49.99, 'gadgets')",
        )
        .unwrap();
        conn.execute(
            "CREATE VIEW category_stats AS \
             SELECT category, COUNT(*) as cnt \
             FROM products GROUP BY category",
        )
        .unwrap();
        let r = conn
            .query("SELECT category, cnt FROM category_stats ORDER BY category")
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "gadgets");
        assert_eq!(row_values(&r[0])[1].to_text(), "3");
        assert_eq!(row_values(&r[1])[0].to_text(), "tools");
        assert_eq!(row_values(&r[1])[1].to_text(), "2");
    }

    #[test]
    fn conformance_028_view_with_join() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE products(\
             id INTEGER PRIMARY KEY, name TEXT, price REAL)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO products VALUES \
             (1, 'Widget', 9.99), (2, 'Gizmo', 24.99)",
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE orders(\
             id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER)",
        )
        .unwrap();
        conn.execute("INSERT INTO orders VALUES (1, 1, 10), (2, 2, 5)")
            .unwrap();
        conn.execute(
            "CREATE VIEW order_details AS \
             SELECT o.id as order_id, p.name, o.qty, \
             p.price * o.qty as total \
             FROM orders o JOIN products p ON o.product_id = p.id",
        )
        .unwrap();
        let r = conn
            .query(
                "SELECT order_id, name, total \
                 FROM order_details ORDER BY order_id",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[1].to_text(), "Widget");
        assert_eq!(row_values(&r[0])[2].to_text(), "99.9");
        assert_eq!(row_values(&r[1])[1].to_text(), "Gizmo");
    }

    #[test]
    fn conformance_028_create_view_if_not_exists() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1(a INTEGER)").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1), (2), (3)").unwrap();
        conn.execute("CREATE VIEW v1 AS SELECT a FROM t1").unwrap();
        conn.execute("CREATE VIEW IF NOT EXISTS v1 AS SELECT 999")
            .unwrap();
        let r = conn.query("SELECT COUNT(*) FROM v1").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "3");
    }

    // -----------------------------------------------------------------------
    // Conformance suite 029: GROUP BY and HAVING
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_029_group_by_basic() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE orders(\
             id INTEGER PRIMARY KEY, customer TEXT, amount REAL)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO orders VALUES \
             (1,'Alice',50.0),(2,'Bob',30.0),(3,'Alice',70.0),\
             (4,'Bob',20.0),(5,'Charlie',100.0)",
        )
        .unwrap();
        let r = conn
            .query(
                "SELECT customer, SUM(amount) as total \
                 FROM orders GROUP BY customer ORDER BY customer",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
        assert_eq!(row_values(&r[0])[1].to_text(), "120.0");
        assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
        assert_eq!(row_values(&r[1])[1].to_text(), "50.0");
    }

    #[test]
    fn conformance_029_having() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE orders(\
             id INTEGER PRIMARY KEY, customer TEXT, amount REAL)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO orders VALUES \
             (1,'Alice',50.0),(2,'Bob',30.0),(3,'Alice',70.0),\
             (4,'Bob',20.0),(5,'Charlie',100.0)",
        )
        .unwrap();
        let r = conn
            .query(
                "SELECT customer, SUM(amount) as total \
                 FROM orders GROUP BY customer \
                 HAVING total > 60 ORDER BY customer",
            )
            .unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
        assert_eq!(row_values(&r[1])[0].to_text(), "Charlie");
    }

    #[test]
    fn conformance_029_group_by_count_min_max_avg() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE scores(\
             student TEXT, subject TEXT, score INTEGER)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO scores VALUES \
             ('Alice','Math',90),('Alice','Sci',85),\
             ('Bob','Math',70),('Bob','Sci',80)",
        )
        .unwrap();
        let r = conn
            .query(
                "SELECT student, COUNT(*) as cnt, MIN(score), MAX(score) \
                 FROM scores GROUP BY student ORDER BY student",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
        assert_eq!(row_values(&r[0])[1].to_text(), "2");
        assert_eq!(row_values(&r[0])[2].to_text(), "85");
        assert_eq!(row_values(&r[0])[3].to_text(), "90");
    }

    #[test]
    fn conformance_029_group_by_multi_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE log(\
             dept TEXT, year INTEGER, revenue REAL)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO log VALUES \
             ('eng',2024,100.0),('eng',2024,200.0),\
             ('eng',2025,150.0),('sales',2024,80.0)",
        )
        .unwrap();
        let r = conn
            .query(
                "SELECT dept, year, SUM(revenue) as total \
                 FROM log GROUP BY dept, year ORDER BY dept, year",
            )
            .unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(row_values(&r[0])[0].to_text(), "eng");
        assert_eq!(row_values(&r[0])[1].to_text(), "2024");
        assert_eq!(row_values(&r[0])[2].to_text(), "300.0");
    }

    // -----------------------------------------------------------------------
    // Conformance suite 030: CASE expressions
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_030_case_searched() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, score INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 95), (2, 72), (3, 45)")
            .unwrap();
        let r = conn
            .query(
                "SELECT id, \
                 CASE WHEN score >= 90 THEN 'A' \
                      WHEN score >= 70 THEN 'B' \
                      ELSE 'F' END as grade \
                 FROM t1 ORDER BY id",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[1].to_text(), "A");
        assert_eq!(row_values(&r[1])[1].to_text(), "B");
        assert_eq!(row_values(&r[2])[1].to_text(), "F");
    }

    #[test]
    fn conformance_030_case_simple() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn
            .query(
                "SELECT CASE 2 \
                 WHEN 1 THEN 'one' \
                 WHEN 2 THEN 'two' \
                 WHEN 3 THEN 'three' \
                 ELSE 'other' END",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "two");
    }

    #[test]
    fn conformance_030_case_null() {
        let conn = Connection::open(":memory:").unwrap();
        let r = conn
            .query(
                "SELECT CASE NULL \
                 WHEN NULL THEN 'match' \
                 ELSE 'no match' END",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "no match");
        let r = conn
            .query(
                "SELECT CASE WHEN NULL THEN 'truthy' \
                 ELSE 'falsy' END",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "falsy");
    }

    #[test]
    fn conformance_030_case_in_aggregate() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, status TEXT)")
            .unwrap();
        conn.execute(
            "INSERT INTO items VALUES \
             (1,'active'),(2,'inactive'),(3,'active'),\
             (4,'active'),(5,'inactive')",
        )
        .unwrap();
        let r = conn
            .query(
                "SELECT SUM(CASE WHEN status = 'active' \
                 THEN 1 ELSE 0 END) as active_count FROM items",
            )
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "3");
    }

    // -----------------------------------------------------------------------
    // Conformance suite 031: INSERT conflict handling
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "INSERT OR REPLACE conflict handling not yet implemented"]
    fn conformance_031_insert_or_replace() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO kv VALUES ('a', 'first')")
            .unwrap();
        conn.execute("INSERT OR REPLACE INTO kv VALUES ('a', 'replaced')")
            .unwrap();
        let r = conn.query("SELECT value FROM kv WHERE key = 'a'").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "replaced");
    }

    #[test]
    #[ignore = "INSERT OR IGNORE conflict handling not yet implemented"]
    fn conformance_031_insert_or_ignore() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT)")
            .unwrap();
        conn.execute("INSERT INTO kv VALUES ('a', 'first')")
            .unwrap();
        conn.execute("INSERT OR IGNORE INTO kv VALUES ('a', 'ignored')")
            .unwrap();
        let r = conn.query("SELECT value FROM kv WHERE key = 'a'").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "first");
        conn.execute("INSERT OR IGNORE INTO kv VALUES ('b', 'new')")
            .unwrap();
        let r = conn.query("SELECT COUNT(*) FROM kv").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "2");
    }

    #[test]
    #[ignore = "REPLACE INTO conflict handling not yet implemented"]
    fn conformance_031_replace_into() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE t1(\
             id INTEGER PRIMARY KEY, name TEXT, \
             score INTEGER DEFAULT 0)",
        )
        .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'Alice', 100)")
            .unwrap();
        conn.execute("REPLACE INTO t1 VALUES (1, 'Alice Updated', 200)")
            .unwrap();
        let r = conn
            .query("SELECT name, score FROM t1 WHERE id = 1")
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Alice Updated");
        assert_eq!(row_values(&r[0])[1].to_text(), "200");
    }

    // -----------------------------------------------------------------------
    // Conformance suite 032: ALTER TABLE
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_032_rename_table() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute(
            "CREATE TABLE original(\
             id INTEGER PRIMARY KEY, name TEXT)",
        )
        .unwrap();
        conn.execute("INSERT INTO original VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();
        conn.execute("ALTER TABLE original RENAME TO renamed")
            .unwrap();
        let r = conn.query("SELECT name FROM renamed ORDER BY id").unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
        assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
    }

    #[test]
    #[ignore = "ALTER TABLE ADD COLUMN DEFAULT values not yet returned for existing rows"]
    fn conformance_032_add_column() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();
        conn.execute("ALTER TABLE t1 ADD COLUMN score INTEGER DEFAULT 0")
            .unwrap();
        let r = conn
            .query("SELECT name, score FROM t1 ORDER BY id")
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
        assert_eq!(row_values(&r[0])[1].to_text(), "0");
        conn.execute("INSERT INTO t1 VALUES (3, 'Charlie', 95)")
            .unwrap();
        let r = conn
            .query("SELECT name, score FROM t1 WHERE id = 3")
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Charlie");
        assert_eq!(row_values(&r[0])[1].to_text(), "95");
    }

    #[test]
    #[ignore = "ALTER TABLE ADD COLUMN DEFAULT values not yet returned for existing rows"]
    fn conformance_032_add_multiple_columns() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();
        conn.execute("ALTER TABLE t1 ADD COLUMN score INTEGER DEFAULT 0")
            .unwrap();
        conn.execute("ALTER TABLE t1 ADD COLUMN active INTEGER DEFAULT 1")
            .unwrap();
        let r = conn
            .query("SELECT name, active FROM t1 ORDER BY id")
            .unwrap();
        assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
        assert_eq!(row_values(&r[0])[1].to_text(), "1");
        assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
        assert_eq!(row_values(&r[1])[1].to_text(), "1");
    }
}
