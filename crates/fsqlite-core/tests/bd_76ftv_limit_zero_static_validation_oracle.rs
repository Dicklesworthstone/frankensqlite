//! bd-76ftv group C: `LIMIT 0` must not hide an error stock raises while it
//! prepares the statement.
//!
//! Stock SQLite resolves every name of a SELECT at prepare, whatever its
//! LIMIT, and only then jumps past the row source when the LIMIT is 0
//! (`computeLimitRegisters`). The bd-ct8zm series (0635b1f33) made fsqlite
//! return zero rows for a constant or bound `LIMIT 0` before a SELECT without
//! FROM reached the executor that checks its names, so it answered `[]` where
//! stock refuses: `SELECT missing_column, count(*) LIMIT 0`,
//! `SELECT count(*) WHERE missing_column LIMIT 0`,
//! `SELECT count(*) LIMIT 0 OFFSET missing_column`,
//! `SELECT count(*) GROUP BY 1 LIMIT 0`,
//! `SELECT count(*) AS c WHERE c = 1 LIMIT 0`, the same with `LIMIT 1 - 1`,
//! `LIMIT -0` and a bound `LIMIT ?` of 0, and the non-aggregate
//! `SELECT missing_column LIMIT 0` / `SELECT 1 WHERE missing_column LIMIT 0`.
//! A window SELECT without FROM skipped its window checks the same way:
//! `SELECT 1 COLLATE nope AS x WINDOW w AS (ORDER BY x) ORDER BY row_number()
//! OVER w LIMIT 0` answered `[]` (stock: "no such collation sequence: nope").
//!
//! Every statement runs on bundled SQLite 3.54.1 (its outcome is pinned, so
//! the comparison cannot pass vacuously) and on fsqlite, on `:memory:` and on
//! a file, executed directly and prepared then stepped. Since bd-22yt1, every
//! statement — including the FROM-less column/alias/aggregate/window errors
//! with `LIMIT 0`, `LIMIT 1`, bound `LIMIT ?`, and no `LIMIT` — must match
//! stock SQLite 3.54.1's exact outcome text.
//!
//! Negative cases a naive "validate everything eagerly" fix fails: LIMIT 0
//! still evaluates no row expression (an `abs(-9223372036854775808)` in the
//! result, WHERE, GROUP BY, HAVING, ORDER BY or OFFSET returns `[]`, as in
//! stock, while `LIMIT 1` raises the overflow), a counting function is never
//! called, a SELECT-list alias used in WHERE or HAVING stays valid, and a
//! correlated subquery without FROM that reads its outer row under LIMIT 0
//! stays valid.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_message(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message))
        | rusqlite::Error::SqlInputError { msg: message, .. } => message.clone(),
        other => other.to_string(),
    }
}

/// Stock's outcome, prepared then stepped with `params` bound: rows, or which
/// of the two failed.
fn stock_outcome(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> String {
    let mut statement = match conn.prepare(sql) {
        Ok(statement) => statement,
        Err(error) => return format!("prepare error: {}", stock_message(&error)),
    };
    for (index, value) in params.iter().enumerate() {
        statement.raw_bind_parameter(index + 1, value).unwrap();
    }
    let columns = statement.column_count();
    let mut cursor = statement.raw_query();
    let mut rows = Vec::new();
    loop {
        match cursor.next() {
            Ok(Some(row)) => rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect::<Vec<_>>(),
            ),
            Ok(None) => return format!("rows={rows:?}"),
            Err(error) => return format!("step error: {}", stock_message(&error)),
        }
    }
}

fn render_rows(rows: &[fsqlite_core::connection::Row]) -> String {
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect();
    format!("rows={rows:?}")
}

/// fsqlite's outcome: executed directly (`error: ...`, which cannot tell
/// prepare from step) or prepared then stepped.
async fn fsqlite_outcome(conn: &Connection, sql: &str, params: &[i64], prepared: bool) -> String {
    let values: Vec<SqliteValue> = params.iter().map(|v| SqliteValue::Integer(*v)).collect();
    if !prepared {
        let result = if values.is_empty() {
            conn.query(sql).await
        } else {
            conn.query_with_params(sql, &values).await
        };
        return match result {
            Ok(rows) => render_rows(&rows),
            Err(error) => format!("error: {error}"),
        };
    }
    let statement = match conn.prepare(sql).await {
        Ok(statement) => statement,
        Err(error) => return format!("prepare error: {error}"),
    };
    let result = if values.is_empty() {
        statement.query().await
    } else {
        statement.query_with_params(&values).await
    };
    match result {
        Ok(rows) => render_rows(&rows),
        Err(error) => format!("step error: {error}"),
    }
}

fn untimed(outcome: &str) -> String {
    outcome
        .strip_prefix("prepare ")
        .or_else(|| outcome.strip_prefix("step "))
        .unwrap_or(outcome)
        .to_owned()
}

const SETUP: &str = "CREATE TABLE t(a, b); INSERT INTO t VALUES (1, 2), (3, 4);";

/// (sql, bound parameters, stock 3.54.1's exact outcome)
const STATEMENTS: &[(&str, &[i64], &str)] = &[
    // Refused by stock at prepare; fsqlite answered [] under LIMIT 0 (bd-76ftv)
    // and previously used nameless FROM-less wording (bd-22yt1).
    (
        "SELECT missing_column, count(*) LIMIT 0",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) WHERE missing_column LIMIT 0",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) LIMIT 0 OFFSET missing_column",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) GROUP BY 1 LIMIT 0",
        &[],
        "prepare error: aggregate functions are not allowed in the GROUP BY clause",
    ),
    (
        "SELECT count(*) AS c WHERE c = 1 LIMIT 0",
        &[],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT count(*) AS c, c + 1 AS d WHERE d = 2 LIMIT 0",
        &[],
        "prepare error: no such column: c",
    ),
    (
        "SELECT missing_column LIMIT 0",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT 1 WHERE missing_column LIMIT 0",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT 1 LIMIT 0 OFFSET missing_column",
        &[],
        "prepare error: no such column: missing_column",
    ),
    // LIMIT given by an expression whose value is 0, or a bound 0.
    (
        "SELECT missing_column, count(*) LIMIT 1 - 1",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) AS c WHERE c = 1 LIMIT -0",
        &[],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT count(*) GROUP BY 1 LIMIT abs(0)",
        &[],
        "prepare error: aggregate functions are not allowed in the GROUP BY clause",
    ),
    (
        "SELECT count(*) WHERE missing_column LIMIT (SELECT 0)",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT missing_column, count(*) LIMIT ?",
        &[0],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) WHERE missing_column LIMIT ?",
        &[0],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) GROUP BY 1 LIMIT ?",
        &[0],
        "prepare error: aggregate functions are not allowed in the GROUP BY clause",
    ),
    (
        "SELECT count(*) AS c WHERE c = 1 LIMIT ?",
        &[0],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT 1 WHERE missing_column LIMIT ?",
        &[0],
        "prepare error: no such column: missing_column",
    ),
    // bd-22yt1: the same FROM-less statements with LIMIT 1, bound LIMIT 1,
    // and no LIMIT must produce the exact same stock error text.
    (
        "SELECT missing_column, count(*) LIMIT 1",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT missing_column, count(*)",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) WHERE missing_column LIMIT 1",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) WHERE missing_column",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) LIMIT 1 OFFSET missing_column",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) AS c WHERE c = 1 LIMIT 1",
        &[],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT count(*) AS c WHERE c = 1",
        &[],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT count(*) AS c, c + 1 AS d WHERE d = 2 LIMIT 1",
        &[],
        "prepare error: no such column: c",
    ),
    (
        "SELECT count(*) AS c, c + 1 AS d WHERE d = 2",
        &[],
        "prepare error: no such column: c",
    ),
    (
        "SELECT missing_column LIMIT 1",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT missing_column",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT 1 WHERE missing_column LIMIT 1",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT 1 WHERE missing_column",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT 1 LIMIT 1 OFFSET missing_column",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT missing_column, count(*) LIMIT ?",
        &[1],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) WHERE missing_column LIMIT ?",
        &[1],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) AS c WHERE c = 1 LIMIT ?",
        &[1],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT 1 WHERE missing_column LIMIT ?",
        &[1],
        "prepare error: no such column: missing_column",
    ),
    // bd-22yt1: adjacent FROM-less clauses, qualified names, duplicate-alias
    // precedence, multi-aggregate alias misuse, and aliased window misuse.
    (
        "SELECT t.missing LIMIT 0",
        &[],
        "prepare error: no such column: t.missing",
    ),
    (
        "SELECT t.missing",
        &[],
        "prepare error: no such column: t.missing",
    ),
    (
        "SELECT 1 WHERE t.missing LIMIT 0",
        &[],
        "prepare error: no such column: t.missing",
    ),
    (
        "SELECT 1 LIMIT 0 OFFSET t.missing",
        &[],
        "prepare error: no such column: t.missing",
    ),
    (
        "SELECT count(*) AS c, sum(1) AS d WHERE c = 1 AND d = 1 LIMIT 0",
        &[],
        "prepare error: misuse of aggregate: sum()",
    ),
    (
        "SELECT count(*) AS c, sum(1) AS d WHERE c = 1 AND d = 1",
        &[],
        "prepare error: misuse of aggregate: sum()",
    ),
    (
        "SELECT count(*) AS c WHERE missing_col AND c = 1 LIMIT 0",
        &[],
        "prepare error: no such column: missing_col",
    ),
    (
        "SELECT count(*) AS c WHERE c = 1 AND missing_col",
        &[],
        "prepare error: no such column: missing_col",
    ),
    (
        "SELECT count(*) AS c WHERE (SELECT c) LIMIT 0",
        &[],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT count(*) AS c WHERE (SELECT c)",
        &[],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT 1 AS c WHERE (SELECT c)",
        &[],
        "rows=[[\"1\"]]",
    ),
    (
        "SELECT 1 AS c, (SELECT c)",
        &[],
        "prepare error: no such column: c",
    ),
    (
        "SELECT row_number() OVER () AS r WHERE (SELECT r)",
        &[],
        "prepare error: misuse of aliased window function r",
    ),
    (
        "SELECT count(*) AS x, 1 AS x WHERE x = 1 LIMIT 0",
        &[],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT count(*) AS x, 1 AS x WHERE x = 1",
        &[],
        "prepare error: misuse of aggregate: count()",
    ),
    (
        "SELECT 1 AS x, count(*) AS x WHERE x = 1",
        &[],
        "rows=[[\"1\", \"1\"]]",
    ),
    (
        "SELECT row_number() OVER () AS r WHERE r = 1 LIMIT 0",
        &[],
        "prepare error: misuse of aliased window function r",
    ),
    (
        "SELECT row_number() OVER () AS r WHERE r = 1",
        &[],
        "prepare error: misuse of aliased window function r",
    ),
    (
        "SELECT row_number() OVER () AS r GROUP BY r",
        &[],
        "prepare error: misuse of aliased window function r",
    ),
    (
        "SELECT row_number() OVER () AS r GROUP BY 1",
        &[],
        "prepare error: misuse of window function row_number()",
    ),
    (
        "SELECT count(*) GROUP BY missing_g",
        &[],
        "prepare error: no such column: missing_g",
    ),
    (
        "SELECT count(*) HAVING missing_h",
        &[],
        "prepare error: no such column: missing_h",
    ),
    (
        "SELECT 1 ORDER BY missing_o",
        &[],
        "prepare error: no such column: missing_o",
    ),
    (
        "VALUES (missing_v)",
        &[],
        "prepare error: no such column: missing_v",
    ),
    (
        "SELECT row_number() OVER (ORDER BY missing_w)",
        &[],
        "prepare error: no such column: missing_w",
    ),
    // A window SELECT without FROM: its windows' keys, reached through a
    // SELECT-list alias, resolve their collation at prepare.
    (
        "SELECT 1 COLLATE nope AS x WINDOW w AS (ORDER BY x) ORDER BY row_number() OVER w LIMIT 0",
        &[],
        "prepare error: no such collation sequence: nope",
    ),
    (
        "SELECT 1 COLLATE nope AS x WINDOW w AS (ORDER BY x) \
         ORDER BY row_number() OVER w LIMIT 1 - 1",
        &[],
        "prepare error: no such collation sequence: nope",
    ),
    (
        "SELECT 1 COLLATE nope AS x ORDER BY row_number() OVER (ORDER BY x) LIMIT ?",
        &[0],
        "prepare error: no such collation sequence: nope",
    ),
    // Refused under LIMIT 0 before this change too; must stay refused.
    (
        "SELECT count(*) AS c GROUP BY c LIMIT 0",
        &[],
        "prepare error: aggregate functions are not allowed in the GROUP BY clause",
    ),
    (
        "SELECT count(*) HAVING missing_column LIMIT 0",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*) ORDER BY missing_column LIMIT 0",
        &[],
        "prepare error: no such column: missing_column",
    ),
    (
        "SELECT count(*), no_such_fn(1) LIMIT 0",
        &[],
        "prepare error: no such function: no_such_fn",
    ),
    (
        "SELECT count(*), abs(1, 2) LIMIT 0",
        &[],
        "prepare error: wrong number of arguments to function abs()",
    ),
    (
        "SELECT a, (SELECT nosuch, count(*) LIMIT 0) FROM t ORDER BY a",
        &[],
        "prepare error: no such column: nosuch",
    ),
    (
        "SELECT a, (SELECT t.nosuch LIMIT 0) FROM t ORDER BY a",
        &[],
        "prepare error: no such column: t.nosuch",
    ),
    // Valid: LIMIT 0 evaluates no row expression (the overflow is never
    // raised) and a SELECT-list alias stays visible.
    (
        "SELECT count(*), abs(-9223372036854775808) LIMIT 0",
        &[],
        "rows=[]",
    ),
    (
        "SELECT abs(-9223372036854775808) LIMIT 0",
        &[],
        "rows=[]",
    ),
    (
        "SELECT count(*), abs(-9223372036854775808) LIMIT 1 - 1",
        &[],
        "rows=[]",
    ),
    (
        "SELECT count(*), abs(-9223372036854775808) LIMIT ?",
        &[0],
        "rows=[]",
    ),
    (
        "SELECT max(1) WHERE abs(-9223372036854775808) LIMIT 0",
        &[],
        "rows=[]",
    ),
    (
        "SELECT count(*) GROUP BY abs(-9223372036854775808) LIMIT 0",
        &[],
        "rows=[]",
    ),
    (
        "SELECT count(*) HAVING abs(-9223372036854775808) LIMIT 0",
        &[],
        "rows=[]",
    ),
    (
        "SELECT count(*), abs(-9223372036854775808) ORDER BY 1 LIMIT 0",
        &[],
        "rows=[]",
    ),
    (
        "SELECT count(*) LIMIT 0 OFFSET abs(-9223372036854775808)",
        &[],
        "rows=[]",
    ),
    (
        "SELECT row_number() OVER (), abs(-9223372036854775808) LIMIT 0",
        &[],
        "rows=[]",
    ),
    (
        "SELECT sum(abs(-9223372036854775808)) OVER () LIMIT 0",
        &[],
        "rows=[]",
    ),
    (
        "SELECT 1 AS x ORDER BY row_number() OVER (ORDER BY x) LIMIT 0",
        &[],
        "rows=[]",
    ),
    ("SELECT 1 AS c WHERE c = 1 LIMIT 0", &[], "rows=[]"),
    ("SELECT count(*) AS c HAVING c > 0 LIMIT 0", &[], "rows=[]"),
    ("SELECT 1 AS c, count(*) WHERE c = 1 LIMIT 0", &[], "rows=[]"),
    // Valid: a subquery without FROM reads its outer row under LIMIT 0.
    (
        "SELECT a, (SELECT t.a LIMIT 0) FROM t ORDER BY a",
        &[],
        "rows=[[\"1\", \"NULL\"], [\"3\", \"NULL\"]]",
    ),
    (
        "SELECT a, (SELECT count(*) + t.a LIMIT 0) FROM t ORDER BY a",
        &[],
        "rows=[[\"1\", \"NULL\"], [\"3\", \"NULL\"]]",
    ),
    (
        "SELECT a, (SELECT b WHERE a > 1 LIMIT 0) FROM t ORDER BY a",
        &[],
        "rows=[[\"1\", \"NULL\"], [\"3\", \"NULL\"]]",
    ),
    (
        "SELECT a FROM t WHERE NOT EXISTS (SELECT 1 WHERE t.b > 0 LIMIT 0) ORDER BY a",
        &[],
        "rows=[[\"1\"], [\"3\"]]",
    ),
    (
        "SELECT a, (SELECT count(*) HAVING t.a > 0 LIMIT 0) FROM t ORDER BY a",
        &[],
        "rows=[[\"1\", \"NULL\"], [\"3\", \"NULL\"]]",
    ),
    // Controls: with LIMIT 1 the same expressions do run.
    (
        "SELECT count(*), abs(-9223372036854775808) LIMIT 1",
        &[],
        "step error: integer overflow",
    ),
    (
        "SELECT row_number() OVER (), abs(-9223372036854775808) LIMIT 1",
        &[],
        "step error: integer overflow",
    ),
    (
        "SELECT 1 AS x ORDER BY row_number() OVER (ORDER BY x) LIMIT 1",
        &[],
        "rows=[[\"1\"]]",
    ),
    (
        "SELECT a, (SELECT count(*) + t.a LIMIT 1) FROM t ORDER BY a",
        &[],
        "rows=[[\"1\", \"2\"], [\"3\", \"4\"]]",
    ),
];

#[test]
fn limit_zero_keeps_stock_prepare_errors_and_evaluates_no_row() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock.execute_batch(SETUP).unwrap();
    for (sql, params, pinned) in STATEMENTS {
        assert_eq!(
            stock_outcome(&stock, sql, params),
            *pinned,
            "stock on `{sql}` {params:?}"
        );
    }
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path().join("bd_76ftv_c.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let conn = Connection::open(&path).await.unwrap();
            conn.execute_batch(SETUP).await.unwrap();
            for prepared in [false, true] {
                for (sql, params, pinned) in STATEMENTS {
                    let got = fsqlite_outcome(&conn, sql, params, prepared).await;
                    let ok = untimed(&got) == untimed(pinned);
                    if !ok {
                        mismatches.push(format!(
                            "file_backed={file_backed} prepared={prepared} `{sql}` {params:?}:\n    \
                             got   {got}\n    stock {pinned}"
                        ));
                    }
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} outcome(s) differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}

struct CountCalls(Arc<AtomicUsize>);

impl fsqlite_func::ScalarFunction for CountCalls {
    fn name(&self) -> &'static str {
        "count_calls"
    }

    fn num_args(&self) -> i32 {
        0
    }

    fn invoke(&self, _args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(SqliteValue::Integer(1))
    }
}

/// LIMIT 0 resolves names but calls no function of a row: a counting
/// function in every clause of a FROM-less SELECT is never invoked, direct or
/// prepared with a bound 0, while LIMIT 1 does invoke it.
#[test]
fn limit_zero_calls_no_function_of_a_row() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                let file = dir.path().join("bd_76ftv_c_calls.db");
                file.to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let conn = Connection::open(&path).await.unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            conn.register_nondeterministic_scalar_function(CountCalls(Arc::clone(&calls)));
            for sql in [
                "SELECT count(*), count_calls() LIMIT 0",
                "SELECT count_calls() LIMIT 0",
                "SELECT count(*) WHERE count_calls() LIMIT 0",
                "SELECT 1 WHERE count_calls() LIMIT 0",
                "SELECT count(*) GROUP BY count_calls() LIMIT 0",
                "SELECT count(*) HAVING count_calls() LIMIT 0",
                "SELECT count(*) ORDER BY count_calls() LIMIT 0",
                "SELECT count(*) LIMIT 0 OFFSET count_calls()",
                "SELECT count(*), count_calls() LIMIT 1 - 1",
                "SELECT row_number() OVER (), count_calls() LIMIT 0",
                "SELECT sum(count_calls()) OVER () LIMIT 0",
            ] {
                let rows = conn.query(sql).await.unwrap();
                assert!(rows.is_empty(), "file_backed={file_backed} `{sql}`: {rows:?}");
                let statement = conn.prepare(sql).await.unwrap();
                let rows = statement.query().await.unwrap();
                assert!(rows.is_empty(), "prepared `{sql}`: {rows:?}");
            }
            for sql in [
                "SELECT count(*), count_calls() LIMIT ?",
                "SELECT count(*) WHERE count_calls() LIMIT ?",
                "SELECT 1 WHERE count_calls() LIMIT ?",
            ] {
                let zero = [SqliteValue::Integer(0)];
                let rows = conn.query_with_params(sql, &zero).await.unwrap();
                assert!(rows.is_empty(), "`{sql}` with 0: {rows:?}");
                let statement = conn.prepare(sql).await.unwrap();
                let rows = statement.query_with_params(&zero).await.unwrap();
                assert!(rows.is_empty(), "prepared `{sql}` with 0: {rows:?}");
            }
            assert_eq!(
                calls.load(Ordering::Relaxed),
                0,
                "file_backed={file_backed}: LIMIT 0 called a function of a row"
            );
            // Not vacuous: the same function runs when a row is produced.
            let rows = conn
                .query("SELECT count(*), count_calls() LIMIT 1")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert!(
                calls.load(Ordering::Relaxed) >= 1,
                "file_backed={file_backed}: LIMIT 1 never called it"
            );
        }
    });
}
