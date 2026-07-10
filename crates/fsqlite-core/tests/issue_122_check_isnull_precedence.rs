//! Regression tests for issue #122: `CHECK ((a IS NULL) = (b IS NULL))`
//! mis-evaluated because schema normalization stripped the parentheses and
//! the re-parse regrouped the expression.
//!
//! Two defects interacted:
//!
//! 1. The AST serializer (`fsqlite-ast` Display) only parenthesized
//!    `BinaryOp`/`UnaryOp` operands, so `Eq(IsNull(a), IsNull(b))` rendered
//!    as `a IS NULL = b IS NULL`. The null-test and `=` share one
//!    left-associative precedence level (verified against the C SQLite CLI:
//!    `SELECT 200 IS NULL = 'ok' IS NULL` yields 0), so the stored text
//!    re-parsed as `((a IS NULL) = b) IS NULL` — a semantically different
//!    expression that inverted the constraint.
//! 2. The parser greedily consumed `NULL` after `IS`, so operators binding
//!    tighter than `IS` no longer attached to the NULL literal
//!    (`x IS NULL < 2` must parse as `x IS (NULL < 2)`, matching C SQLite's
//!    binaryToUnaryIfNull fold).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const CREATE_RUNS: &str = "CREATE TABLE runs (
    id INTEGER PRIMARY KEY,
    t_end INTEGER,
    outcome TEXT,
    CHECK ((t_end IS NULL) = (outcome IS NULL))
) STRICT";

/// Assert the CHECK constraint from issue #122 behaves like C SQLite on a
/// given (already-created) connection: rows where both columns are NULL or
/// both are non-NULL pass; mixed rows fail.
fn assert_check_semantics(conn: &Connection, id_base: i64) {
    // Both non-NULL: satisfies the constraint. Was REJECTED before the fix.
    conn.execute(&format!(
        "INSERT INTO runs (id, t_end, outcome) VALUES ({}, 200, 'ok')",
        id_base
    ))
    .expect("row with both columns non-NULL must satisfy the CHECK");

    // Both NULL: satisfies the constraint.
    conn.execute(&format!("INSERT INTO runs (id) VALUES ({})", id_base + 1))
        .expect("row with both columns NULL must satisfy the CHECK");

    // t_end set, outcome NULL: VIOLATES the constraint. Was ACCEPTED before
    // the fix.
    let err = conn.execute(&format!(
        "INSERT INTO runs (id, t_end) VALUES ({}, 300)",
        id_base + 2
    ));
    assert!(
        err.is_err(),
        "row with t_end set and outcome NULL must violate the CHECK, got {err:?}"
    );

    // outcome set, t_end NULL: also violates.
    let err = conn.execute(&format!(
        "INSERT INTO runs (id, outcome) VALUES ({}, 'late')",
        id_base + 3
    ));
    assert!(
        err.is_err(),
        "row with outcome set and t_end NULL must violate the CHECK, got {err:?}"
    );
}

#[test]
fn test_issue_122_check_isnull_eq_isnull_in_memory() {
    let conn = Connection::open(":memory:").expect("open in-memory db");
    conn.execute(CREATE_RUNS).expect("create table");
    assert_check_semantics(&conn, 1);
}

/// The stored schema text must preserve the semantically necessary
/// parentheses, and the constraint must keep working after the schema is
/// re-loaded from disk by a fresh connection.
#[test]
fn test_issue_122_check_survives_schema_round_trip_through_file() {
    let tmp = tempfile::NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_str().expect("utf-8 temp path");

    {
        let conn = Connection::open(path).expect("open file db");
        conn.execute(CREATE_RUNS).expect("create table");
        assert_check_semantics(&conn, 1);
    }

    // Reopen: the schema is re-parsed from the stored (normalized) text.
    let conn = Connection::open(path).expect("reopen file db");

    let rows = conn
        .query("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'runs'")
        .expect("read stored schema text");
    assert_eq!(rows.len(), 1, "expected exactly one schema row");
    let stored_sql = match &rows[0].values()[0] {
        SqliteValue::Text(s) => s.clone(),
        other => panic!("expected TEXT schema sql, got {other:?}"),
    };
    assert!(
        stored_sql.contains("(t_end IS NULL) = (outcome IS NULL)"),
        "stored schema text must keep the grouping parentheses, got: {stored_sql}"
    );

    assert_check_semantics(&conn, 11);
}

/// Expression-level oracle: fsqlite must agree with C SQLite on how the
/// unparenthesized forms group. Expected values were captured verbatim from
/// the sqlite3 CLI (3.46.1) and are re-checked here against rusqlite.
#[test]
fn test_issue_122_null_test_precedence_matches_c_sqlite() {
    // (sql, expected result from the sqlite3 CLI)
    let cases: &[(&str, i64)] = &[
        // Explicit grouping: (0) = (0) -> 1.
        ("SELECT (200 IS NULL) = ('ok' IS NULL)", 1),
        // Unparenthesized: ((200 IS NULL) = 'ok') IS NULL -> 0.
        ("SELECT 200 IS NULL = 'ok' IS NULL", 0),
        // ((300 IS NULL) = NULL) IS NULL -> 1 (NOT (0 = 1) -> 0).
        ("SELECT 300 IS NULL = NULL IS NULL", 1),
        // Single-token postfix form groups the same way.
        ("SELECT 200 ISNULL = 'ok' ISNULL", 0),
        // Tighter operator after NULL attaches to NULL: 1 IS (NULL < 2) -> 0.
        ("SELECT 1 IS NULL < 2", 0),
        ("SELECT 1 IS NULL + 1", 0),
        // Parenthesized NULL still folds to a null-test.
        ("SELECT 0 IS (NULL)", 0),
    ];

    let fconn = Connection::open(":memory:").expect("open fsqlite");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");

    for (sql, expected) in cases {
        let rows = fconn.query(sql).expect("fsqlite query");
        assert_eq!(rows.len(), 1, "{sql}: expected one row");
        let got = match rows[0].values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("{sql}: expected INTEGER, got {other:?}"),
        };
        assert_eq!(got, *expected, "fsqlite disagrees with sqlite3 CLI: {sql}");

        let oracle: i64 = rconn
            .query_row(sql, [], |row| row.get(0))
            .expect("rusqlite query");
        assert_eq!(got, oracle, "fsqlite disagrees with rusqlite oracle: {sql}");
    }
}
