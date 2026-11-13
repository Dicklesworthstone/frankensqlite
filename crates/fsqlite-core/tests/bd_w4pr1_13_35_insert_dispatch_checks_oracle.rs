#![recursion_limit = "512"]

//! bd-w4pr1.13.35 — an INSERT builds the boxed future of a dispatch-time check
//! only when that check can act on it, and every check still catches the
//! statements it exists for.
//!
//! The dispatcher used to heap-allocate one future per check for every INSERT:
//! delegation to an attached database, INSTEAD OF triggers on a view, the two
//! FTS5 command forms, inbound foreign keys of rows a REPLACE deleted, and the
//! AUTOINCREMENT high-water. Each is now built behind that check's own first
//! test. This file runs one statement per check, and a look-alike that must
//! NOT be caught, on FrankenSQLite and on stock SQLite (rusqlite), each on
//! file-backed databases, in autocommit and inside `BEGIN ... COMMIT`, and
//! compares every statement's outcome and the resulting contents.
//!
//! A gate keyed on the wrong property fails a named case:
//! * `attached`: a gate that skipped delegation for every INSERT writes
//!   `aux.t`'s row into main's `t` (or fails), which the dumps of both show.
//! * `view`: a gate that skipped the INSTEAD OF check for INSERT fails with
//!   "cannot modify v because it is a view" where stock runs the trigger.
//! * `fts5_command`: the `'delete'` command names three columns, so a gate on a
//!   one-column list keeps the deleted document in the index. `'rebuild'` and
//!   `'optimize'` run in the setup, which must succeed, and no document may
//!   match the command's name afterwards. They are not among the compared
//!   statements because FrankenSQLite reports `changes()` 0 after them where
//!   stock reports 1, a difference main 0f8b7c7f4 already has and that this
//!   lever does not touch.
//! * `name_column`: an ordinary table whose first column bears the table's
//!   name passes the FTS5 gate and must store `'delete'` as data.
//! * `replace_cascade`: a gate that never ran the REPLACE-victim pass leaves
//!   the child row of the replaced parent in place.
//! * `autoincrement`: a gate that skipped the high-water refresh leaves
//!   `sqlite_sequence` behind, and the next implicit id reuses a deleted one.
#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// (case, setup, statements whose outcome is compared, dump queries)
type Case = (
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
    &'static [&'static str],
);

const CASES: &[Case] = &[
    (
        "attached",
        &["CREATE TABLE t(x)", "CREATE TABLE aux.t(x)"],
        &[
            "INSERT INTO aux.t VALUES (1)",
            "INSERT INTO t VALUES (2)",
            "INSERT INTO main.t VALUES (3)",
            "INSERT INTO aux.t(x) VALUES (4)",
        ],
        &[
            "SELECT x FROM main.t ORDER BY x",
            "SELECT x FROM aux.t ORDER BY x",
        ],
    ),
    (
        "view",
        &[
            "CREATE TABLE base(id INTEGER PRIMARY KEY, v TEXT)",
            "CREATE VIEW v AS SELECT id, v FROM base",
            "CREATE TRIGGER v_ins INSTEAD OF INSERT ON v BEGIN \
             INSERT INTO base VALUES (new.id + 100, new.v || '!'); END",
        ],
        &[
            "INSERT INTO v VALUES (1, 'via view')",
            "INSERT INTO v(id, v) VALUES (2, 'again')",
            "INSERT INTO base VALUES (3, 'direct')",
        ],
        &[
            "SELECT id, v FROM base ORDER BY id",
            "SELECT id, v FROM v ORDER BY id",
        ],
    ),
    (
        "fts5_command",
        &[
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)",
            "CREATE VIRTUAL TABLE ft USING fts5(body, content='docs', content_rowid='id')",
            "INSERT INTO docs VALUES (1, 'hello world'), (2, 'hello there'), (3, 'goodbye world')",
            "INSERT INTO ft(ft) VALUES ('rebuild')",
            "INSERT INTO ft(ft) VALUES ('optimize')",
        ],
        &[
            "INSERT INTO ft(ft, rowid, body) VALUES ('delete', 2, 'hello there')",
            "DELETE FROM docs WHERE id = 2",
            "INSERT INTO docs VALUES (4, 'hello again')",
            "INSERT INTO ft(rowid, body) VALUES (4, 'hello again')",
        ],
        &[
            "SELECT rowid FROM ft WHERE ft MATCH 'hello' ORDER BY rowid",
            "SELECT rowid FROM ft WHERE ft MATCH 'there' ORDER BY rowid",
            "SELECT rowid FROM ft WHERE ft MATCH 'world OR again' ORDER BY rowid",
            "SELECT rowid FROM ft WHERE ft MATCH 'optimize OR rebuild' ORDER BY rowid",
        ],
    ),
    (
        "name_column",
        &["CREATE TABLE c(c TEXT, d INTEGER)"],
        &[
            "INSERT INTO c(c, d) VALUES ('delete', 1)",
            "INSERT INTO c(c) VALUES ('optimize')",
            "INSERT INTO c(c, d) VALUES ('rebuild', 3)",
        ],
        &["SELECT c, d FROM c ORDER BY rowid"],
    ),
    (
        "replace_cascade",
        &[
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT)",
            "CREATE TABLE ch(pid INTEGER REFERENCES p(id) ON DELETE CASCADE, w TEXT)",
            "INSERT INTO p VALUES (1, 'a'), (2, 'b')",
            "INSERT INTO ch VALUES (1, 'x'), (2, 'y')",
        ],
        &[
            "INSERT OR REPLACE INTO p VALUES (1, 'a2')",
            "INSERT INTO p VALUES (3, 'c')",
            "REPLACE INTO p VALUES (2, 'b2')",
        ],
        &[
            "SELECT id, v FROM p ORDER BY id",
            "SELECT pid, w FROM ch ORDER BY pid",
        ],
    ),
    (
        "autoincrement",
        &["CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"],
        &[
            "INSERT INTO a(v) VALUES ('one')",
            "INSERT INTO a(id, v) VALUES (10, 'ten')",
            "DELETE FROM a WHERE id = 10",
            "INSERT INTO a(v) VALUES ('after')",
        ],
        &[
            "SELECT id, v FROM a ORDER BY id",
            "SELECT name, seq FROM sqlite_sequence ORDER BY name",
        ],
    ),
];

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Integer(v) => format!("i:{v}"),
        SqliteValue::Float(v) => format!("f:{v}"),
        SqliteValue::Text(v) => format!("t:{v}"),
        SqliteValue::Blob(v) => format!("b:{:?}", v.to_vec()),
        SqliteValue::Null => "null".to_owned(),
    }
}

fn render_rusqlite(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
        rusqlite::types::ValueRef::Real(v) => format!("f:{v}"),
        rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
        rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
        rusqlite::types::ValueRef::Null => "null".to_owned(),
    }
}

/// Whether the statement succeeded, then `changes()` after it. Statements run
/// through `query`, the entry point the shell uses, so a recognized literal
/// INSERT takes the parameterized path as it does there.
async fn fsqlite_outcome(conn: &Connection, sql: &str) -> String {
    let status = match conn.query(sql).await {
        Ok(_) => "ok",
        Err(_) => "error",
    };
    format!(
        "{status} {:?}",
        fsqlite_dump(conn, "SELECT changes()").await
    )
}

fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let status = match conn.execute_batch(sql) {
        Ok(()) => "ok",
        Err(_) => "error",
    };
    format!("{status} {:?}", stock_dump(conn, "SELECT changes()"))
}

async fn fsqlite_dump(conn: &Connection, sql: &str) -> Vec<String> {
    match conn.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(render_fsqlite)
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        Err(error) => vec![format!("error: {error}")],
    }
}

fn stock_dump(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut stmt = match conn.prepare(sql) {
        Ok(stmt) => stmt,
        Err(error) => return vec![format!("error: {error}")],
    };
    let width = stmt.column_count();
    let rows = stmt.query_map([], |row| {
        Ok((0..width)
            .map(|i| render_rusqlite(row.get_ref_unwrap(i)))
            .collect::<Vec<_>>()
            .join("|"))
    });
    match rows {
        Ok(rows) => rows.map(|row| row.expect("stock row")).collect(),
        Err(error) => vec![format!("error: {error}")],
    }
}

/// Run one case on both engines, in autocommit or in one transaction, and
/// return its divergences.
async fn run_case(case: &Case, in_transaction: bool) -> Vec<String> {
    let (name, setup, statements, dumps) = *case;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = |file: &str| dir.path().join(file).to_string_lossy().into_owned();
    let fsqlite = Connection::open(path("fsqlite.db"))
        .await
        .expect("open fsqlite");
    let stock = rusqlite::Connection::open(path("stock.db")).expect("open stock");
    fsqlite
        .execute(&format!("ATTACH '{}' AS aux", path("fsqlite_aux.db")))
        .await
        .expect("fsqlite attach");
    stock
        .execute(&format!("ATTACH '{}' AS aux", path("stock_aux.db")), [])
        .expect("stock attach");
    for sql in setup {
        fsqlite.execute(sql).await.expect("fsqlite setup");
        stock.execute_batch(sql).expect("stock setup");
    }

    let mut diffs = Vec::new();
    let mode = if in_transaction { "txn" } else { "autocommit" };
    if in_transaction {
        fsqlite.execute("BEGIN").await.expect("fsqlite begin");
        stock.execute_batch("BEGIN").expect("stock begin");
    }
    for sql in statements {
        let ours = fsqlite_outcome(&fsqlite, sql).await;
        let theirs = stock_outcome(&stock, sql);
        if ours != theirs {
            diffs.push(format!(
                "[{name} {mode}] {sql}\n  fsqlite {ours}\n  stock   {theirs}"
            ));
        }
    }
    if in_transaction {
        fsqlite.execute("COMMIT").await.expect("fsqlite commit");
        stock.execute_batch("COMMIT").expect("stock commit");
    }
    for sql in dumps {
        let ours = fsqlite_dump(&fsqlite, sql).await;
        let theirs = stock_dump(&stock, sql);
        if ours != theirs {
            diffs.push(format!(
                "[{name} {mode}] {sql}\n  fsqlite {ours:?}\n  stock   {theirs:?}"
            ));
        }
    }
    diffs
}

#[test]
fn each_insert_dispatch_check_still_catches_its_statements() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for case in CASES {
            for in_transaction in [false, true] {
                diffs.extend(run_case(case, in_transaction).await);
            }
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) from stock:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
