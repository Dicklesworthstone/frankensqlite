#![recursion_limit = "512"]

//! bd-x4g7x: `name.*` expands every FROM item addressed as `name`, whatever
//! database the item lives in, as SQLite's `selectExpander` does. An aliased
//! item is addressed by its alias alone. FrankenSQLite required the star's
//! database to equal the item's, so `SELECT users.* FROM temp.users` failed
//! with "no such table: users", and over a main/temp join `users.*` expanded
//! one item. Compared with stock SQLite (rusqlite, bundled): rows and error
//! messages, `:memory:` and file, through `Connection::query` and
//! `Connection::prepare(..).query()`. (Stock rejects a three-part
//! `db.table.*` as a syntax error; FrankenSQLite accepts it: bd-oh98k.)

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// `main.users` holds 'm', `temp.users` holds 't', `aux.users` holds 'a';
/// `solo` exists only in TEMP.
const SETUP: &[&str] = &[
    "CREATE TABLE main.users(name)",
    "INSERT INTO main.users VALUES ('m')",
    "CREATE TEMP TABLE users(name)",
    "INSERT INTO temp.users VALUES ('t')",
    "CREATE TEMP TABLE solo(x)",
    "INSERT INTO solo VALUES (7)",
    "ATTACH ':memory:' AS aux",
    "CREATE TABLE aux.users(name)",
    "INSERT INTO aux.users VALUES ('a')",
];

const QUERIES: &[&str] = &[
    // The reported shapes.
    "SELECT users.* FROM temp.users",
    "SELECT users.* FROM main.users JOIN temp.users",
    // One item, each database, with filters and ordering.
    "SELECT users.* FROM main.users",
    "SELECT users.* FROM aux.users",
    "SELECT users.* FROM temp.users WHERE name = 't'",
    "SELECT users.*, 1 FROM temp.users ORDER BY 1",
    "SELECT solo.* FROM temp.solo",
    "SELECT solo.* FROM solo",
    // Aliased items answer only to their alias.
    "SELECT users.* FROM temp.users AS x",
    "SELECT x.* FROM temp.users AS x",
    // Joins: every item addressed as `users` expands.
    "SELECT users.* FROM temp.users JOIN aux.users",
    "SELECT users.* FROM main.users JOIN aux.users ORDER BY 1",
    "SELECT users.*, solo.* FROM temp.users JOIN solo",
    "SELECT users.name, solo.x FROM temp.users JOIN solo",
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

fn frank_rows(
    result: fsqlite_error::Result<Vec<fsqlite_core::connection::Row>>,
) -> Vec<Vec<String>> {
    match result {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(
            rusqlite::Error::SqliteFailure(_, Some(m))
            | rusqlite::Error::SqlInputError { msg: m, .. },
        ) => return vec![vec![format!("<ERR {m}>")]],
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    match st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
    {
        Ok(rows) => rows,
        Err(rusqlite::Error::SqliteFailure(_, Some(m))) => vec![vec![format!("<ERR {m}>")]],
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

async fn run(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("x4g7x.db").to_str().expect("utf-8 path").to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in SETUP {
        f.execute(sql).await.expect("frank setup");
        r.execute_batch(sql).expect("stock setup");
    }
    let mut failures = Vec::new();
    for sql in QUERIES {
        let stock = stock_rows(&r, sql);
        let direct = frank_rows(f.query(sql).await);
        if direct != stock {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (query):\n  frank {direct:?}\n  stock {stock:?}"
            ));
        }
        let prepared = match f.prepare(sql).await {
            Ok(statement) => frank_rows(statement.query().await),
            Err(e) => frank_rows(Err(e)),
        };
        if prepared != stock {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (prepare):\n  frank {prepared:?}\n  stock {stock:?}"
            ));
        }
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn table_star_over_database_qualified_items_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            failures.extend(run(file_backed).await);
        }
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
