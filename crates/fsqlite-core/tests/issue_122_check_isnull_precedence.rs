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
use fsqlite_error::FrankenError;
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
async fn assert_check_semantics(conn: &Connection, id_base: i64) {
    // Both non-NULL: satisfies the constraint. Was REJECTED before the fix.
    conn.execute(&format!(
        "INSERT INTO runs (id, t_end, outcome) VALUES ({}, 200, 'ok')",
        id_base
    ))
    .await
    .expect("row with both columns non-NULL must satisfy the CHECK");

    // Both NULL: satisfies the constraint.
    conn.execute(&format!("INSERT INTO runs (id) VALUES ({})", id_base + 1))
        .await
        .expect("row with both columns NULL must satisfy the CHECK");

    // t_end set, outcome NULL: VIOLATES the constraint. Was ACCEPTED before
    // the fix.
    let err = conn
        .execute(&format!(
            "INSERT INTO runs (id, t_end) VALUES ({}, 300)",
            id_base + 2
        ))
        .await;
    assert!(
        err.is_err(),
        "row with t_end set and outcome NULL must violate the CHECK, got {err:?}"
    );

    // outcome set, t_end NULL: also violates.
    let err = conn
        .execute(&format!(
            "INSERT INTO runs (id, outcome) VALUES ({}, 'late')",
            id_base + 3
        ))
        .await;
    assert!(
        err.is_err(),
        "row with outcome set and t_end NULL must violate the CHECK, got {err:?}"
    );
}

const WIDE_CHECK_TERMS: usize = 32;

fn wide_check_fixture() -> (String, String) {
    let columns = (0..WIDE_CHECK_TERMS)
        .map(|index| format!("v{index} INTEGER NOT NULL"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut right_nested_all_one = format!("v{} = 1", WIDE_CHECK_TERMS - 1);
    let mut right_nested_any_zero = format!("v{} = 0", WIDE_CHECK_TERMS - 1);
    for index in (0..WIDE_CHECK_TERMS - 1).rev() {
        right_nested_all_one = format!("v{index} = 1 AND ({right_nested_all_one})");
        right_nested_any_zero = format!("v{index} = 0 OR ({right_nested_any_zero})");
    }

    // bd-67tdh oracle-reconciliation: stock sqlite3 3.46.1 stores the CHECK
    // expression byte-for-byte as written -- it does NOT flatten the associative
    // AND/OR chains -- and ALTER TABLE ADD COLUMN splices new columns in ahead of
    // the CHECK without touching it. So the stored / reopened / post-ADD-COLUMN
    // schema must contain the *verbatim* nested CHECK, not a flattened form.
    let expected_check =
        format!("({right_nested_all_one}) OR (({right_nested_any_zero}) AND guard = 0)");
    let create_sql = format!(
        "CREATE TABLE logic (\
         id INTEGER PRIMARY KEY, {columns}, guard INTEGER NOT NULL, \
         CHECK(({right_nested_all_one}) OR (({right_nested_any_zero}) AND guard = 0))\
         ) STRICT"
    );
    (create_sql, expected_check)
}

fn logic_insert_sql(id: i64, zero_at: Option<usize>, guard: i64) -> String {
    let columns = (0..WIDE_CHECK_TERMS)
        .map(|index| format!("v{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let values = (0..WIDE_CHECK_TERMS)
        .map(|index| if zero_at == Some(index) { "0" } else { "1" })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO logic (id, {columns}, guard) VALUES ({id}, {values}, {guard})")
}

async fn assert_wide_check_semantics(conn: &Connection, id_base: i64) {
    conn.execute(&logic_insert_sql(id_base, None, 1))
        .await
        .expect("the all-one branch must satisfy the wide CHECK");
    conn.execute(&logic_insert_sql(id_base + 1, Some(7), 0))
        .await
        .expect("the any-zero branch with guard zero must satisfy the wide CHECK");

    let rejected = conn
        .execute(&logic_insert_sql(id_base + 2, Some(7), 1))
        .await
        .expect_err("a zero term with a nonzero guard must violate the wide CHECK");
    match rejected {
        FrankenError::CheckViolation { .. } => {}
        other => panic!("wide CHECK returned the wrong FrankenSQLite error: {other:?}"),
    }
    let rejected_rows = conn
        .query(&format!(
            "SELECT count(*) FROM logic WHERE id = {}",
            id_base + 2
        ))
        .await
        .expect("count rejected FrankenSQLite row");
    assert_eq!(
        rejected_rows[0].values()[0],
        SqliteValue::Integer(0),
        "a CHECK-rejected row must not remain visible"
    );
}

async fn stored_logic_schema_sql(conn: &Connection) -> String {
    let rows = conn
        .query("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'logic'")
        .await
        .expect("read stored logic schema text");
    assert_eq!(rows.len(), 1, "expected exactly one logic schema row");
    match &rows[0].values()[0] {
        SqliteValue::Text(sql) => sql.to_string(),
        other => panic!("expected TEXT schema sql, got {other:?}"),
    }
}

#[test]
fn test_issue_122_check_isnull_eq_isnull_in_memory() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:")
            .await
            .expect("open in-memory db");
        conn.execute(CREATE_RUNS).await.expect("create table");
        assert_check_semantics(&conn, 1).await;
    });
}

/// The stored schema text must preserve the semantically necessary
/// parentheses, and the constraint must keep working after the schema is
/// re-loaded from disk by a fresh connection.
#[test]
fn test_issue_122_check_survives_schema_round_trip_through_file() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_str().expect("utf-8 temp path");

        {
            let conn = Connection::open(path).await.expect("open file db");
            conn.execute(CREATE_RUNS).await.expect("create table");
            assert_check_semantics(&conn, 1).await;
        }

        // Reopen: the schema is re-parsed from the stored (normalized) text.
        let conn = Connection::open(path).await.expect("reopen file db");

        let rows = conn
            .query("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'runs'")
            .await
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

        assert_check_semantics(&conn, 11).await;
    });
}

/// Repeated ALTER/reopen cycles used to amplify formatter-added parentheses in
/// migration-scale boolean constraints. The stored SQL may legitimately gain
/// each newly added column, but its pre-existing CHECK expression must be
/// canonical, byte-stable across reopen, and bounded in size.
#[test]
fn test_wide_boolean_check_alter_reopen_keeps_schema_bounded_and_semantic() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_str().expect("utf-8 temp path");
        let (create_sql, expected_check) = wide_check_fixture();

        let mut previous_sql = {
            let conn = Connection::open(path).await.expect("open file db");
            conn.execute(&create_sql).await.expect("create logic table");
            let stored_sql = stored_logic_schema_sql(&conn).await;
            assert!(
                stored_sql.contains(&expected_check),
                "CREATE must store the verbatim CHECK expression (sqlite3 does not \
                 flatten it): {stored_sql}"
            );
            assert_wide_check_semantics(&conn, 1).await;
            stored_sql
        };
        let baseline_len = previous_sql.len();
        let baseline_parentheses = previous_sql.matches('(').count();

        for cycle in 0..2 {
            let conn = Connection::open(path).await.expect("reopen before ALTER");
            let reopened_sql = stored_logic_schema_sql(&conn).await;
            assert_eq!(
                reopened_sql, previous_sql,
                "schema bytes changed merely by reopening at cycle {cycle}"
            );

            conn.execute(&format!(
                "ALTER TABLE logic ADD COLUMN added_{cycle} INTEGER DEFAULT {cycle}"
            ))
            .await
            .expect("ALTER must add a plain defaulted column");

            let altered_sql = stored_logic_schema_sql(&conn).await;
            assert!(
                altered_sql.contains(&expected_check),
                "ALTER cycle {cycle} changed the verbatim CHECK expression: {altered_sql}"
            );
            assert_eq!(
                altered_sql.matches('(').count(),
                baseline_parentheses,
                "ALTER cycle {cycle} amplified schema parentheses"
            );
            assert!(
                altered_sql.len() <= baseline_len + (cycle + 1) * 64,
                "ALTER cycle {cycle} grew schema text beyond the added column: \
                 baseline={baseline_len}, current={}, sql={altered_sql}",
                altered_sql.len()
            );
            let cycle_id = i64::try_from(cycle).expect("test cycle fits i64");
            assert_wide_check_semantics(&conn, 100 + cycle_id * 10).await;
            previous_sql = altered_sql;
        }

        {
            let conn = Connection::open(path)
                .await
                .expect("final FrankenSQLite reopen");
            assert_eq!(
                stored_logic_schema_sql(&conn).await,
                previous_sql,
                "final FrankenSQLite reopen changed stored schema bytes"
            );
            assert_wide_check_semantics(&conn, 1_000).await;
        }

        let sqlite = rusqlite::Connection::open(path).expect("stock SQLite final reopen");
        let integrity: String = sqlite
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("stock SQLite integrity_check");
        assert_eq!(integrity, "ok");
        let stock_sql: String = sqlite
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'logic'",
                [],
                |row| row.get(0),
            )
            .expect("stock SQLite must parse and expose the logic schema");
        assert_eq!(
            stock_sql, previous_sql,
            "stock SQLite observed different stored schema bytes"
        );
        sqlite
            .execute_batch(&logic_insert_sql(2_000, None, 1))
            .expect("stock SQLite must accept the all-one branch");
        sqlite
            .execute_batch(&logic_insert_sql(2_001, Some(7), 0))
            .expect("stock SQLite must accept the guarded any-zero branch");
        let rejected = sqlite
            .execute_batch(&logic_insert_sql(2_002, Some(7), 1))
            .expect_err("stock SQLite must enforce the failing CHECK branch");
        match rejected {
            rusqlite::Error::SqliteFailure(error, _) => {
                assert_eq!(
                    error.extended_code,
                    rusqlite::ffi::SQLITE_CONSTRAINT_CHECK,
                    "stock SQLite failure must be SQLITE_CONSTRAINT_CHECK"
                );
            }
            other => panic!("stock SQLite returned a non-SQLite CHECK error: {other}"),
        }
        let rejected_count: i64 = sqlite
            .query_row(
                "SELECT count(*) FROM logic WHERE id = ?1",
                [2_002_i64],
                |row| row.get(0),
            )
            .expect("count rejected stock SQLite row");
        assert_eq!(
            rejected_count, 0,
            "stock SQLite must not retain a CHECK-rejected row"
        );
    });
}

/// Expression-level oracle: fsqlite must agree with C SQLite on how the
/// unparenthesized forms group. Expected values were captured verbatim from
/// the sqlite3 CLI (3.46.1) and are re-checked here against rusqlite.
#[test]
fn test_issue_122_null_test_precedence_matches_c_sqlite() {
    asupersync::test_utils::run_test(|| async {
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

        let fconn = Connection::open(":memory:").await.expect("open fsqlite");
        let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");

        for (sql, expected) in cases {
            let rows = fconn.query(sql).await.expect("fsqlite query");
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
    });
}
