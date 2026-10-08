#![recursion_limit = "512"]

//! bd-tjvwc: when `main.users` and `TEMP users` both exist, an INSERT, UPDATE
//! or DELETE whose target is one of them must read the other one wherever the
//! statement names it: in a scalar or EXISTS subquery, in UPDATE ... FROM, and
//! in a correlated reference back to the target. FrankenSQLite's DML codegen
//! resolves tables by bare name, so naming one database's `users` made every
//! `users` in the statement read that database. Compared with stock SQLite
//! (rusqlite, bundled): the statement's outcome and changed-row count, then
//! both tables' rows. Every statement runs on a fresh pair of databases,
//! through both `Connection::execute` and a prepared statement, on `:memory:`
//! and on a file. The SELECT forms that the DELETE lane for a correlated
//! EXISTS runs internally are compared too, and so are RETURNING subqueries
//! naming a schema-qualified table, which read NULL (bd-8sawz).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// `main.users` holds 'm' and `temp.users` holds 't'; `t2` (MAIN only) holds
/// one row and `tonly` (TEMP only) holds 7.
const SETUP: &[&str] = &[
    "CREATE TABLE main.users(name)",
    "INSERT INTO main.users VALUES ('m')",
    "CREATE TEMP TABLE users(name)",
    "INSERT INTO temp.users VALUES ('t')",
    "CREATE TABLE t2(a, b)",
    "INSERT INTO t2 VALUES ('a', 'b')",
    "CREATE TEMP TABLE tonly(x)",
    "INSERT INTO tonly VALUES (7)",
];

/// The shapes reported in bd-tjvwc and its review comment.
const REPORTED: &[&str] = &[
    "UPDATE main.users SET name = (SELECT name FROM temp.users)",
    "UPDATE main.users AS m SET name = (SELECT m.name || u.name FROM temp.users AS u)",
    "UPDATE temp.users SET name = name || '?' WHERE EXISTS (SELECT 1 FROM main.users AS mu WHERE mu.name = users.name)",
    "UPDATE temp.users AS tu SET name = name || '?' WHERE EXISTS (SELECT 1 FROM main.users AS mu WHERE mu.name = tu.name)",
    "DELETE FROM temp.users WHERE EXISTS (SELECT 1 FROM main.users AS mu WHERE users.name = 't' AND mu.name <> 'x')",
    "DELETE FROM temp.users AS tu WHERE EXISTS (SELECT 1 FROM main.users AS mu WHERE tu.name = 't' AND mu.name <> 'x')",
    "UPDATE main.users SET name = u.name FROM temp.users AS u",
    "UPDATE main.users AS m SET name = m.name || u.name FROM temp.users AS u",
    "UPDATE temp.users SET name = mu.name FROM main.users AS mu",
    "UPDATE main.users SET name = temp.users.name FROM temp.users",
    "UPDATE temp.users SET name = main.users.name FROM main.users",
];

/// The same mix with the other target, an unqualified target (TEMP), DELETE
/// with a scalar subquery, INSERT ... SELECT and INSERT ... VALUES with a
/// subquery, and RETURNING.
const MORE: &[&str] = &[
    "UPDATE users SET name = (SELECT name FROM main.users)",
    "UPDATE users SET name = u.name FROM main.users AS u",
    "UPDATE temp.users SET name = (SELECT name FROM main.users) || name",
    "UPDATE main.users SET name = name || (SELECT name FROM temp.users WHERE temp.users.name = 't')",
    "DELETE FROM main.users WHERE name = (SELECT name FROM temp.users)",
    "DELETE FROM main.users WHERE EXISTS (SELECT 1 FROM temp.users WHERE temp.users.name = 't')",
    "DELETE FROM main.users WHERE name IN (SELECT name FROM temp.users)",
    "DELETE FROM temp.users WHERE name = (SELECT name FROM main.users)",
    "DELETE FROM users WHERE name <> (SELECT name FROM main.users)",
    "INSERT INTO main.users SELECT name || '+' FROM temp.users",
    "INSERT INTO temp.users SELECT name || '+' FROM main.users",
    "INSERT INTO main.users VALUES ((SELECT name FROM temp.users))",
    "INSERT INTO temp.users VALUES ((SELECT name FROM main.users))",
    "INSERT INTO t2 SELECT m.name, u.name FROM main.users AS m, temp.users AS u",
    "INSERT INTO main.users SELECT name || '!' FROM temp.users WHERE EXISTS (SELECT 1 FROM main.users WHERE main.users.name = 'm')",
    "UPDATE temp.users SET name = 'q' WHERE EXISTS (SELECT 1 FROM main.users WHERE main.users.name = 'm') RETURNING name",
    // A target of a third table whose subqueries read both databases' `users`.
    "UPDATE t2 SET a = (SELECT name FROM temp.users), b = (SELECT name FROM main.users)",
    "UPDATE t2 SET a = (SELECT name FROM users) || (SELECT name FROM main.users)",
    "DELETE FROM t2 WHERE (SELECT name FROM main.users) = 'm' AND (SELECT name FROM users) = 't'",
    "DELETE FROM t2 WHERE EXISTS (SELECT 1 FROM main.users AS mu, temp.users AS tu WHERE mu.name = 'm' AND tu.name = 't')",
];

/// bd-8sawz: RETURNING subqueries naming a schema-qualified table, for a
/// table in both databases, only in MAIN (`t2`) and only in TEMP (`tonly`).
const RETURNING_SUBQUERIES: &[&str] = &[
    "UPDATE main.users SET name = 'q' RETURNING name, (SELECT name FROM temp.users)",
    "UPDATE t2 SET a = 'z' RETURNING (SELECT name FROM main.users), (SELECT name FROM temp.users), (SELECT x FROM tonly)",
    "UPDATE t2 SET b = 'y' RETURNING (SELECT a FROM main.t2), (SELECT x FROM temp.tonly)",
    "INSERT INTO t2 VALUES ('n', 'o') RETURNING (SELECT name FROM main.users), (SELECT x FROM temp.tonly)",
    "DELETE FROM t2 RETURNING (SELECT name FROM temp.users), (SELECT name FROM main.users)",
];

/// The SELECT forms of DML shapes above, which DML lanes run internally.
const SELECTS: &[&str] = &[
    "SELECT * FROM temp.users WHERE EXISTS (SELECT 1 FROM main.users AS mu WHERE users.name = 't' AND mu.name <> 'x')",
    "SELECT * FROM temp.users AS tu WHERE EXISTS (SELECT 1 FROM main.users AS mu WHERE tu.name = 't' AND mu.name <> 'x')",
    "SELECT name, (SELECT name FROM temp.users) FROM main.users",
    "SELECT name, (SELECT name FROM main.users) FROM temp.users",
];

const STATE: &[&str] = &[
    "SELECT name FROM main.users ORDER BY name",
    "SELECT name FROM temp.users ORDER BY name",
    "SELECT a, b FROM main.t2",
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn stock_message(error: rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(m)) | rusqlite::Error::SqlInputError { msg: m, .. } => m,
        other => other.to_string(),
    }
}

/// Rows of a stock query, or its error message.
fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut statement = r.prepare(sql).map_err(stock_message)?;
    let n = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .map_err(stock_message)
}

fn frank_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn returns_rows(sql: &str) -> bool {
    sql.starts_with("SELECT") || sql.contains("RETURNING")
}

/// Stock's outcome: the changed-row count, or the rows of a SELECT or a
/// RETURNING clause, or the error.
fn stock_outcome(r: &rusqlite::Connection, sql: &str) -> String {
    if returns_rows(sql) {
        return match stock_rows(r, sql) {
            Ok(rows) => format!("rows {rows:?}"),
            Err(m) => format!("error: {m}"),
        };
    }
    match r.prepare(sql).and_then(|mut statement| statement.execute([])) {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {}", stock_message(e)),
    }
}

async fn frank_outcome(f: &Connection, sql: &str, prepared: bool) -> String {
    if returns_rows(sql) {
        let rows = if prepared {
            match f.prepare(sql).await {
                Ok(statement) => statement.query().await,
                Err(e) => Err(e),
            }
        } else {
            f.query(sql).await
        };
        return match rows {
            Ok(rows) => format!("rows {:?}", frank_rows(&rows)),
            Err(e) => format!("error: {e}"),
        };
    }
    let changed = if prepared {
        match f.prepare(sql).await {
            Ok(statement) => statement.execute().await,
            Err(e) => Err(e),
        }
    } else {
        f.execute(sql).await
    };
    match changed {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {e}"),
    }
}

/// Runs `sql` on a fresh pair and returns every difference from stock.
async fn check_one(sql: &str, file_backed: bool, prepared: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("tjvwc.db").to_str().expect("utf-8 path").to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    let mut failures = Vec::new();
    for setup in SETUP {
        f.execute(setup).await.expect("frank setup");
        r.execute(setup, []).expect("stock setup");
    }
    let label = format!("[file_backed={file_backed} prepared={prepared}] `{sql}`");
    let fo = frank_outcome(&f, sql, prepared).await;
    let so = stock_outcome(&r, sql);
    if fo != so {
        failures.push(format!("{label}: frank {fo:?} vs stock {so:?}"));
    }
    for state in STATE {
        let fv = match f.query(state).await {
            Ok(rows) => Ok(frank_rows(&rows)),
            Err(e) => Err(e.to_string()),
        };
        let sv = stock_rows(&r, state);
        if fv != sv {
            failures.push(format!("{label}, then `{state}`: frank {fv:?} vs stock {sv:?}"));
        }
    }
    f.close().await.expect("close");
    failures
}

fn check(label: &str, statements: &'static [&'static str]) {
    asupersync::test_utils::run_test(|| async move {
        let mut failures = Vec::new();
        for sql in statements {
            // A prepared DML statement's RETURNING rows are not readable
            // through `PreparedStatement::query` (DML runs through
            // `Connection::execute_prepared`), so those run directly only.
            let modes: &[bool] = if sql.contains("RETURNING") {
                &[false]
            } else {
                &[false, true]
            };
            for file_backed in [false, true] {
                for &prepared in modes {
                    failures.extend(check_one(sql, file_backed, prepared).await);
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{label}: {} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

#[test]
fn reported_same_name_main_temp_dml_matches_stock() {
    check("reported", REPORTED);
}

#[test]
fn more_same_name_main_temp_dml_matches_stock() {
    check("more", MORE);
}

#[test]
fn same_name_main_temp_select_forms_match_stock() {
    check("selects", SELECTS);
}

#[test]
fn returning_subqueries_over_qualified_tables_match_stock() {
    check("returning", RETURNING_SUBQUERIES);
}
