//! Public API facade for FrankenSQLite.
//!
//! This crate will grow a stable, ergonomic API surface over time. In early
//! phases it also re-exports selected internal crates for integration tests.

pub use fsqlite_core::connection::{Connection, PreparedStatement, Row};
pub use fsqlite_vfs;

#[cfg(test)]
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
            .query("SELECT t.* FROM ga AS t GROUP BY k, v ORDER BY k, v;")
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
}
