#![recursion_limit = "512"]

//! bd-45puh / bd-r4g3y: UPSERT against a partial UNIQUE index behaves as in
//! stock SQLite.
//!
//! - bd-45puh: with the conflict target omitted, the probe skipped partial
//!   UNIQUE indexes, so a row their predicate admits raised "UNIQUE constraint
//!   failed" instead of taking DO NOTHING / DO UPDATE.
//! - bd-r4g3y: the probe evaluated a table-qualified index predicate
//!   (`q.value > 0`, `q.rowid > 0`) under the INSERT's alias, where `q.` read
//!   NULL, so an aliased UPSERT missed the conflict and raised UNIQUE.
//!
//! Each statement runs on a fresh database for each index predicate spelling;
//! compared with stock (rusqlite, bundled): the outcome (changed rows or the
//! error), then the table. `:memory:` and file, execute and prepared.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Partial UNIQUE index predicates, unqualified and table-qualified, on a
/// column and on the rowid / its alias.
const PREDICATES: &[&str] = &["value > 0", "q.value > 0", "q.rowid > 0", "id > 0"];

const STATEMENTS: &[&str] = &[
    // Omitted conflict target (bd-45puh).
    "INSERT INTO q(value, note) VALUES (5, 'b') ON CONFLICT DO NOTHING",
    "INSERT INTO q(value, note) VALUES (5, 'b') ON CONFLICT DO UPDATE SET note = excluded.note",
    "INSERT INTO q(value, note) VALUES (6, 'c') ON CONFLICT DO NOTHING",
    "INSERT INTO q(id, value, note) VALUES (1, 9, 'pk') ON CONFLICT DO UPDATE SET note = 'pk-hit'",
    "INSERT INTO q(value, note) VALUES (-1, 'outside') ON CONFLICT DO NOTHING",
    // Aliased INSERT with a target (bd-r4g3y).
    "INSERT INTO q AS x(value, note) VALUES (5, 'c') ON CONFLICT(value) WHERE value > 0 DO NOTHING",
    "INSERT INTO q AS x(value, note) VALUES (5, 'c') ON CONFLICT(value) WHERE value > 0 DO UPDATE SET note = 'aliased'",
    "INSERT INTO q AS x(value, note) VALUES (5, 'd') ON CONFLICT DO UPDATE SET note = x.note || '+'",
    // Unaliased with a target, for comparison.
    "INSERT INTO q(value, note) VALUES (5, 'e') ON CONFLICT(value) WHERE value > 0 DO UPDATE SET note = excluded.note",
];

const STATE: &str = "SELECT id, value, note FROM q ORDER BY id";

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

fn stock_outcome(r: &rusqlite::Connection, sql: &str) -> String {
    match r.prepare(sql).and_then(|mut statement| statement.execute([])) {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {}", stock_message(e)),
    }
}

async fn frank_outcome(f: &Connection, sql: &str, prepared: bool) -> String {
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

async fn check_one(predicate: &str, sql: &str, file_backed: bool, prepared: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("upsert.db").to_str().expect("utf-8 path").to_owned()
    } else {
        ":memory:".to_owned()
    };
    let setup = [
        "CREATE TABLE q(id INTEGER PRIMARY KEY, value INT, note TEXT)".to_owned(),
        format!("CREATE UNIQUE INDEX q_partial ON q(value) WHERE {predicate}"),
        "INSERT INTO q(id, value, note) VALUES (1, 5, 'a'), (2, -1, 'neg')".to_owned(),
    ];
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in &setup {
        f.execute(sql).await.expect("frank setup");
        r.execute(sql, []).expect("stock setup");
    }
    let label = format!("[WHERE {predicate}; file_backed={file_backed} prepared={prepared}] `{sql}`");
    let mut failures = Vec::new();
    let fo = frank_outcome(&f, sql, prepared).await;
    let so = stock_outcome(&r, sql);
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
fn upsert_against_partial_unique_indexes_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for predicate in PREDICATES {
            for sql in STATEMENTS {
                for file_backed in [false, true] {
                    for prepared in [false, true] {
                        failures.extend(check_one(predicate, sql, file_backed, prepared).await);
                    }
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
