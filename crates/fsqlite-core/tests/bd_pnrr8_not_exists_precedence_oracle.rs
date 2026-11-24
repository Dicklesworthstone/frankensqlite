//! bd-pnrr8: `NOT EXISTS (...)` takes prefix NOT's precedence, as in stock
//! SQLite 3.54.1, so `NOT EXISTS (SELECT 1) + 10` is `NOT (EXISTS (...) + 10)`
//! (0), not `(NOT EXISTS (...)) + 10` (10). fsqlite parsed `NOT EXISTS` as one
//! tight unit and gave a silent wrong answer wherever an operator that binds
//! tighter than NOT followed it.
//!
//! `core_sql_rusqlite_conformance::bd_pnrr8_not_exists_operator_precedence_matches_rusqlite`
//! covers the operator matrix on one `:memory:` connection, queried directly.
//! This keeper covers the statement contexts that matrix does not: the
//! expression in WHERE, ORDER BY, GROUP BY, CASE, UPDATE SET and DELETE
//! WHERE; stored SQL that is re-parsed (a view and a trigger's WHEN and body,
//! read again after the file is reopened); bound parameters; and each statement
//! on `:memory:` and on a file, executed directly and prepared. Every outcome
//! (its rows, or its error text) must equal stock's, and is pinned to stock's
//! so the comparison cannot pass vacuously.
//!
//! Negative cases a naive fix fails: explicit grouping must survive
//! (`(NOT EXISTS (...)) + 10` is 10 and `NOT (EXISTS (...)) + 10` is 0), a
//! boolean operator still binds outside the NOT (`NOT EXISTS (...) AND 0`,
//! `OR 1`), plain `NOT 0 + 10` keeps its meaning, and repeated NOTs group
//! their whole operand (`NOT NOT EXISTS (...) + 10` is 1).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

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

fn stock_outcome(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> String {
    let result = (|| -> rusqlite::Result<Vec<Vec<String>>> {
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows = Vec::new();
        let mut cursor = statement.query(rusqlite::params_from_iter(params))?;
        while let Some(row) = cursor.next()? {
            rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect(),
            );
        }
        Ok(rows)
    })();
    match result {
        Ok(rows) => format!("rows={rows:?}"),
        Err(
            rusqlite::Error::SqliteFailure(_, Some(message))
            | rusqlite::Error::SqlInputError { msg: message, .. },
        ) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

fn render_rows(rows: &[fsqlite_core::connection::Row]) -> String {
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect();
    format!("rows={rows:?}")
}

/// `sql` on fsqlite, run directly or prepared first: a SELECT is queried,
/// any other statement executed.
async fn fsqlite_outcome(conn: &Connection, sql: &str, params: &[i64], prepared: bool) -> String {
    let params: Vec<SqliteValue> = params.iter().map(|&n| SqliteValue::Integer(n)).collect();
    let returns_rows = sql.starts_with("SELECT");
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement) if returns_rows => statement.query_with_params(&params).await,
            Ok(statement) => statement
                .execute_with_params(&params)
                .await
                .map(|_| Vec::new()),
            Err(error) => Err(error),
        }
    } else if returns_rows {
        conn.query_with_params(sql, &params).await
    } else {
        conn.execute_with_params(sql, &params)
            .await
            .map(|_| Vec::new())
    };
    match result {
        Ok(rows) => render_rows(&rows),
        Err(error) => format!("error: {error}"),
    }
}

const NO_ROWS: &str = "rows=[]";
const SETUP: &[&str] = &[
    "CREATE TABLE t(k INTEGER PRIMARY KEY, n INT, c TEXT)",
    "INSERT INTO t VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 30, 'z')",
    "CREATE TABLE s(k, v)",
    "INSERT INTO s VALUES (1, 7), (1, 73), (2, 80)",
    "CREATE TABLE log(k, e)",
    "CREATE VIEW v AS SELECT k, NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k) + 10 AS e FROM t",
    "CREATE TRIGGER t_ai AFTER INSERT ON t \
     WHEN NOT EXISTS (SELECT 1 FROM s WHERE s.k = new.k) + 1 \
     BEGIN INSERT INTO log VALUES (new.k, 'when'); END",
    "CREATE TRIGGER t_au AFTER UPDATE ON t \
     BEGIN INSERT INTO log VALUES (new.k, NOT EXISTS (SELECT 1 FROM s WHERE s.k = new.k) + 10); END",
];

/// Run in order after `SETUP`; each pinned to stock's outcome.
const STATEMENTS: &[(&str, &str)] = &[
    // The operand of NOT extends over every operator tighter than NOT.
    (
        "SELECT NOT EXISTS (SELECT 1) + 10, NOT EXISTS (SELECT 1 WHERE 0) + 10",
        "rows=[[\"0\", \"0\"]]",
    ),
    (
        "SELECT NOT EXISTS (SELECT 1) - 1, NOT EXISTS (SELECT 1) * 0, \
         NOT EXISTS (SELECT 1) / 2, NOT EXISTS (SELECT 1) || 'x'",
        "rows=[[\"1\", \"1\", \"1\", \"0\"]]",
    ),
    (
        "SELECT NOT EXISTS (SELECT 1) = 2, NOT EXISTS (SELECT 1) < 2, \
         NOT EXISTS (SELECT 1) IS NULL, NOT EXISTS (SELECT 1) NOTNULL",
        "rows=[[\"1\", \"0\", \"1\", \"0\"]]",
    ),
    (
        "SELECT NOT EXISTS (SELECT 1) BETWEEN 0 AND 2, NOT EXISTS (SELECT 1) IN (0, 1), \
         NOT EXISTS (SELECT 1) LIKE '%', NOT EXISTS (SELECT 'A') COLLATE NOCASE = 'a'",
        "rows=[[\"0\", \"0\", \"0\", \"1\"]]",
    ),
    // AND and OR bind looser than NOT.
    (
        "SELECT NOT EXISTS (SELECT 1) AND 0, NOT EXISTS (SELECT 1) OR 1, \
         NOT EXISTS (SELECT 1 WHERE 0) AND 1",
        "rows=[[\"0\", \"1\", \"1\"]]",
    ),
    // NOT EXISTS on the right of an operator.
    (
        "SELECT 10 + NOT EXISTS (SELECT 1) + 5, 'a' || NOT EXISTS (SELECT 1) || 'b', \
         2 = NOT EXISTS (SELECT 1) + 2",
        "rows=[[\"10\", \"'a0'\", \"0\"]]",
    ),
    (
        "SELECT NOT NOT EXISTS (SELECT 1) + 10, NOT NOT NOT EXISTS (SELECT 1) + 10",
        "rows=[[\"1\", \"0\"]]",
    ),
    // Explicit grouping, plain NOT, and a bare NOT EXISTS keep their meaning.
    (
        "SELECT (NOT EXISTS (SELECT 1)) + 10, NOT (EXISTS (SELECT 1)) + 10, NOT 0 + 10, \
         NOT EXISTS (SELECT 1 WHERE 0)",
        "rows=[[\"10\", \"0\", \"0\", \"1\"]]",
    ),
    (
        "SELECT CASE WHEN NOT EXISTS (SELECT 1) + 10 THEN 'y' ELSE 'n' END",
        "rows=[[\"'n'\"]]",
    ),
    // Correlated, in each clause.
    (
        "SELECT k, NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k) + 10 FROM t ORDER BY k",
        "rows=[[\"1\", \"0\"], [\"2\", \"0\"], [\"3\", \"0\"]]",
    ),
    (
        "SELECT k FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k) + 1 ORDER BY k",
        NO_ROWS,
    ),
    (
        "SELECT k FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k) AND k > 0 ORDER BY k",
        "rows=[[\"3\"]]",
    ),
    (
        "SELECT k FROM t ORDER BY NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k) + k DESC, k",
        "rows=[[\"1\"], [\"2\"], [\"3\"]]",
    ),
    // A correlated subquery in a GROUP BY key needs bd-scbik (f5dfadb7a).
    (
        "SELECT count(*) FROM t GROUP BY NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k) + k",
        "rows=[[\"3\"]]",
    ),
    // A view's stored SELECT and a trigger's WHEN and body.
    (
        "SELECT * FROM v ORDER BY k",
        "rows=[[\"1\", \"0\"], [\"2\", \"0\"], [\"3\", \"0\"]]",
    ),
    ("INSERT INTO t VALUES (4, 4, 'w')", NO_ROWS),
    (
        "UPDATE t SET n = NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k) + 100 WHERE k <= 2",
        NO_ROWS,
    ),
    (
        "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k) + 1",
        NO_ROWS,
    ),
    (
        "SELECT * FROM t ORDER BY k",
        "rows=[[\"1\", \"0\", \"'x'\"], [\"2\", \"0\", \"'y'\"], [\"3\", \"30\", \"'z'\"], \
         [\"4\", \"4\", \"'w'\"]]",
    ),
    (
        "SELECT * FROM log ORDER BY rowid",
        "rows=[[\"1\", \"0\"], [\"2\", \"0\"]]",
    ),
];

/// Run after `STATEMENTS`; on a file, after closing and reopening it, so the
/// view and the triggers are parsed again from their stored SQL.
const AFTER_REOPEN: &[(&str, &str)] = &[
    (
        "SELECT * FROM v ORDER BY k",
        "rows=[[\"1\", \"0\"], [\"2\", \"0\"], [\"3\", \"0\"], [\"4\", \"0\"]]",
    ),
    ("INSERT INTO t VALUES (6, 6, 'v')", NO_ROWS),
    ("UPDATE t SET c = 'u' WHERE k = 1", NO_ROWS),
    (
        "SELECT * FROM log ORDER BY rowid",
        "rows=[[\"1\", \"0\"], [\"2\", \"0\"], [\"1\", \"0\"]]",
    ),
];

/// Run after `SETUP` with bound parameters.
const PARAMETER_STATEMENTS: &[(&str, &[i64], &str)] = &[
    (
        "SELECT NOT EXISTS (SELECT 1 WHERE ?1) + ?2",
        &[1, 10],
        "rows=[[\"0\"]]",
    ),
    (
        "SELECT NOT EXISTS (SELECT 1 WHERE ?1) + ?2",
        &[0, 10],
        "rows=[[\"0\"]]",
    ),
    (
        "SELECT k FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k AND s.v > ?1) + ?2 \
         ORDER BY k",
        &[100, 1],
        NO_ROWS,
    ),
    (
        "SELECT k FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.k = t.k AND s.v > ?1) + ?2 \
         ORDER BY k",
        &[100, 0],
        "rows=[[\"1\"], [\"2\"], [\"3\"]]",
    ),
];

fn stock_with_setup() -> rusqlite::Connection {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    stock
}

#[test]
fn not_exists_takes_prefix_not_precedence_in_every_statement_context_like_stock() {
    let stock = stock_with_setup();
    let expected: Vec<String> = STATEMENTS
        .iter()
        .chain(AFTER_REOPEN)
        .map(|(sql, _)| stock_outcome(&stock, sql, &[]))
        .collect();
    // Not vacuous: stock gives each pinned outcome.
    for ((sql, pinned), stock_gave) in STATEMENTS.iter().chain(AFTER_REOPEN).zip(&expected) {
        assert_eq!(stock_gave, pinned, "stock on `{sql}`");
    }
    let (expected_before, expected_after) = expected.split_at(STATEMENTS.len());
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_pnrr8.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let mut conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, _), expected) in STATEMENTS.iter().zip(expected_before) {
                    let got = fsqlite_outcome(&conn, sql, &[], prepared).await;
                    if &got != expected {
                        mismatches.push(format!(
                            "file_backed={file_backed} prepared={prepared} `{sql}`:\n    \
                             got  {got}\n    want {expected}"
                        ));
                    }
                }
                if file_backed {
                    conn.close().await.unwrap();
                    conn = Connection::open(&path).await.unwrap();
                }
                for ((sql, _), expected) in AFTER_REOPEN.iter().zip(expected_after) {
                    let got = fsqlite_outcome(&conn, sql, &[], prepared).await;
                    if &got != expected {
                        mismatches.push(format!(
                            "file_backed={file_backed} prepared={prepared} after reopen `{sql}`:\n    \
                             got  {got}\n    want {expected}"
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

#[test]
fn not_exists_takes_prefix_not_precedence_with_bound_parameters_like_stock() {
    let stock = stock_with_setup();
    let expected: Vec<String> = PARAMETER_STATEMENTS
        .iter()
        .map(|(sql, params, _)| stock_outcome(&stock, sql, params))
        .collect();
    for ((sql, _, pinned), stock_gave) in PARAMETER_STATEMENTS.iter().zip(&expected) {
        assert_eq!(stock_gave, pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_pnrr8_params.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, params, _), expected) in PARAMETER_STATEMENTS.iter().zip(&expected) {
                    let got = fsqlite_outcome(&conn, sql, params, prepared).await;
                    if &got != expected {
                        mismatches.push(format!(
                            "file_backed={file_backed} prepared={prepared} `{sql}` {params:?}:\n    \
                             got  {got}\n    want {expected}"
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
