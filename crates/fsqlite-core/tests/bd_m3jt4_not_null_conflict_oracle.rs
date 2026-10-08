#![recursion_limit = "512"]

//! bd-m3jt4: a NULL for a NOT NULL column resolves its conflict as stock
//! SQLite does. REPLACE stores the column's DEFAULT (and aborts when there is
//! no DEFAULT, or the DEFAULT is NULL too); IGNORE skips the row. FrankenSQLite
//! raised "NOT NULL constraint failed" for REPLACE, and its prepared
//! direct-insert / direct-update lanes raised it for IGNORE as well. Each
//! statement runs on a fresh database, with its parameters bound both through
//! `execute_with_params` and a prepared statement; compared with stock
//! (rusqlite, bundled): the outcome (changed rows or the error), then the table.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

struct Case {
    setup: &'static [&'static str],
    statement: &'static str,
    params: &'static [Option<i64>],
}

const WITH_DEFAULT: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b NOT NULL DEFAULT 5)",
    "INSERT INTO t VALUES (1, 7)",
];
const NO_DEFAULT: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b NOT NULL)",
    "INSERT INTO t VALUES (1, 7)",
];
const NULL_DEFAULT: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b NOT NULL DEFAULT NULL)",
    "INSERT INTO t VALUES (1, 7)",
];
const COLUMN_REPLACE: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b NOT NULL ON CONFLICT REPLACE DEFAULT 9)",
    "INSERT INTO t VALUES (1, 7)",
];
const COLUMN_IGNORE: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b NOT NULL ON CONFLICT IGNORE)",
    "INSERT INTO t VALUES (1, 7)",
];
const TEXT_DEFAULT: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT NOT NULL DEFAULT 'dflt', c INT)",
    "INSERT INTO t VALUES (1, 'x', 1)",
];

const CASES: &[Case] = &[
    // The reported shapes.
    Case { setup: WITH_DEFAULT, statement: "INSERT OR REPLACE INTO t VALUES (2, NULL)", params: &[] },
    Case { setup: WITH_DEFAULT, statement: "UPDATE OR REPLACE t SET b = NULL", params: &[] },
    Case { setup: NO_DEFAULT, statement: "INSERT OR IGNORE INTO t VALUES (?1, ?2)", params: &[Some(2), None] },
    // REPLACE: with a DEFAULT, without one, a NULL DEFAULT, the column clause.
    Case { setup: WITH_DEFAULT, statement: "INSERT OR REPLACE INTO t VALUES (?1, ?2)", params: &[Some(2), None] },
    Case { setup: WITH_DEFAULT, statement: "INSERT OR REPLACE INTO t VALUES (1, NULL)", params: &[] },
    Case { setup: WITH_DEFAULT, statement: "UPDATE OR REPLACE t SET b = ?1 WHERE a = ?2", params: &[None, Some(1)] },
    Case { setup: NO_DEFAULT, statement: "INSERT OR REPLACE INTO t VALUES (2, NULL)", params: &[] },
    Case { setup: NO_DEFAULT, statement: "UPDATE OR REPLACE t SET b = NULL", params: &[] },
    Case { setup: NULL_DEFAULT, statement: "INSERT OR REPLACE INTO t VALUES (2, NULL)", params: &[] },
    Case { setup: COLUMN_REPLACE, statement: "INSERT INTO t VALUES (2, NULL)", params: &[] },
    Case { setup: COLUMN_REPLACE, statement: "INSERT INTO t VALUES (?1, ?2)", params: &[Some(2), None] },
    Case { setup: COLUMN_REPLACE, statement: "UPDATE t SET b = ?1", params: &[None] },
    Case { setup: COLUMN_REPLACE, statement: "INSERT OR ABORT INTO t VALUES (2, NULL)", params: &[] },
    Case { setup: TEXT_DEFAULT, statement: "INSERT OR REPLACE INTO t(a, b, c) VALUES (?1, ?2, ?3)", params: &[Some(2), None, Some(3)] },
    // IGNORE: the statement clause and the column clause, INSERT and UPDATE.
    Case { setup: NO_DEFAULT, statement: "INSERT OR IGNORE INTO t VALUES (2, NULL)", params: &[] },
    Case { setup: NO_DEFAULT, statement: "INSERT OR IGNORE INTO t VALUES (?1, ?2)", params: &[Some(3), Some(4)] },
    Case { setup: NO_DEFAULT, statement: "UPDATE OR IGNORE t SET b = ?1", params: &[None] },
    Case { setup: COLUMN_IGNORE, statement: "INSERT INTO t VALUES (?1, ?2)", params: &[Some(2), None] },
    Case { setup: COLUMN_IGNORE, statement: "UPDATE t SET b = NULL", params: &[] },
    // The default ABORT still errors.
    Case { setup: NO_DEFAULT, statement: "INSERT INTO t VALUES (?1, ?2)", params: &[Some(2), None] },
    Case { setup: WITH_DEFAULT, statement: "UPDATE t SET b = NULL", params: &[] },
];

const STATE: &str = "SELECT * FROM t ORDER BY a";

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

fn frank_params(params: &[Option<i64>]) -> Vec<SqliteValue> {
    params
        .iter()
        .map(|value| value.map_or(SqliteValue::Null, SqliteValue::Integer))
        .collect()
}

async fn check_case(case: &Case, file_backed: bool, prepared: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("m3jt4.db").to_str().expect("utf-8 path").to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in case.setup {
        f.execute(sql).await.expect("frank setup");
        r.execute(sql, []).expect("stock setup");
    }
    let params = frank_params(case.params);
    let changed = if prepared {
        match f.prepare(case.statement).await {
            Ok(statement) => f.execute_prepared_with_params(&statement, &params).await,
            Err(e) => Err(e),
        }
    } else {
        f.execute_with_params(case.statement, &params).await
    };
    let fo = match changed {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {e}"),
    };
    let so = match r
        .prepare(case.statement)
        .and_then(|mut statement| statement.execute(rusqlite::params_from_iter(case.params.iter())))
    {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {}", stock_message(e)),
    };
    let label = format!(
        "[file_backed={file_backed} prepared={prepared}] {:?} `{}` {:?}",
        case.setup[0], case.statement, case.params
    );
    let mut failures = Vec::new();
    if fo != so {
        failures.push(format!("{label}: frank {fo:?} vs stock {so:?}"));
    }
    let fv = match f.query(STATE).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
            .collect::<Vec<_>>()),
        Err(e) => Err(e.to_string()),
    };
    let sv = stock_rows(&r, STATE);
    if fv != sv {
        failures.push(format!("{label}, then `{STATE}`: frank {fv:?} vs stock {sv:?}"));
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn not_null_conflict_resolution_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for case in CASES {
            for file_backed in [false, true] {
                for prepared in [false, true] {
                    failures.extend(check_case(case, file_backed, prepared).await);
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
