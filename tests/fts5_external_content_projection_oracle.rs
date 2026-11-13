#![recursion_limit = "512"]

//! GH#211 (bd-c6jre): what an external-content FTS5 table (`content='docs'`)
//! returns for its columns, against stock SQLite (rusqlite).
//!
//! The table's index holds only postings; stock reads the column values from
//! the content table, by name: a full scan is a scan of the content table
//! (so `count(*)` and the rows follow it, indexed or not) and a MATCH looks
//! each hit up there by `content_rowid`. FrankenSQLite projected NULL (or
//! empty text), so `highlight()` and `snippet()` came back empty too, and its
//! `'rebuild'` paired the content table's columns with the FTS5 columns by
//! position. Each case seeds a database, runs its statements and compares
//! every outcome with stock: in memory, in a file FrankenSQLite seeded with
//! another connection, and in a file stock seeded. Stock then checks the
//! file, and its checks must agree with the ones it runs on its own file.
#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const DOCS: &[&str] = &[
    "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
    "INSERT INTO docs VALUES (1, 'hello', 'alpha beta'), (2, 'world', 'gamma beta')",
    "CREATE VIRTUAL TABLE ft USING fts5(title, body, content='docs', content_rowid='id')",
    "INSERT INTO ft(ft) VALUES ('rebuild')",
];

const DOCS_WITH_TRIGGERS: &[&str] = &[
    "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
    "INSERT INTO docs VALUES (1, 'hello', 'alpha beta'), (2, 'world', 'gamma beta')",
    "CREATE VIRTUAL TABLE ft USING fts5(title, body, content='docs', content_rowid='id')",
    "INSERT INTO ft(ft) VALUES ('rebuild')",
    "CREATE TRIGGER docs_ai AFTER INSERT ON docs BEGIN \
     INSERT INTO ft(rowid, title, body) VALUES (new.id, new.title, new.body); END",
    "CREATE TRIGGER docs_ad AFTER DELETE ON docs BEGIN \
     INSERT INTO ft(ft, rowid, title, body) VALUES ('delete', old.id, old.title, old.body); END",
    "CREATE TRIGGER docs_au AFTER UPDATE ON docs BEGIN \
     INSERT INTO ft(ft, rowid, title, body) VALUES ('delete', old.id, old.title, old.body); \
     INSERT INTO ft(rowid, title, body) VALUES (new.id, new.title, new.body); END",
];

/// (name, schema and seed, statements after the open)
const CASES: &[(&str, &[&str], &[&str])] = &[
    (
        "projections",
        DOCS,
        &[
            "SELECT rowid, * FROM ft",
            "SELECT rowid, title, body FROM ft WHERE ft MATCH 'beta'",
            "SELECT body FROM ft WHERE ft MATCH 'title:world'",
            "SELECT title FROM ft WHERE rowid = 2",
            "SELECT count(*) FROM ft",
            "SELECT rowid, highlight(ft, 1, '[', ']') FROM ft WHERE ft MATCH 'gamma'",
            "SELECT snippet(ft, 1, '[', ']', '..', 3) FROM ft WHERE ft MATCH 'alpha'",
            "SELECT rowid, highlight(ft, 0, '<', '>') FROM ft WHERE ft MATCH 'beta OR hello' \
             ORDER BY rank",
            "SELECT d.id, ft.title FROM docs AS d JOIN ft ON ft.rowid = d.id \
             WHERE ft MATCH 'beta' ORDER BY d.id",
        ],
    ),
    // The content table's columns are matched to the FTS5 columns by name.
    (
        "columns_by_name",
        &[
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, extra TEXT, body TEXT, title TEXT)",
            "INSERT INTO docs VALUES (1, 'x1', 'alpha beta', 'hello'), (2, 'x2', 'gamma beta', 'world')",
            "CREATE VIRTUAL TABLE ft USING fts5(title, body, content='docs', content_rowid='id')",
            "INSERT INTO ft(ft) VALUES ('rebuild')",
        ],
        &[
            "SELECT rowid, * FROM ft",
            "SELECT rowid FROM ft WHERE ft MATCH 'hello'",
            "SELECT rowid FROM ft WHERE ft MATCH 'x1'",
            "SELECT rowid, title FROM ft WHERE ft MATCH 'body:alpha'",
        ],
    ),
    // The values keep the content table's types.
    (
        "typed_unindexed_column",
        &[
            "CREATE TABLE d(id INTEGER PRIMARY KEY, n INTEGER, body TEXT)",
            "INSERT INTO d VALUES (1, 42, 'red fish'), (2, 7, 'blue fish')",
            "CREATE VIRTUAL TABLE ft USING fts5(n UNINDEXED, body, content='d', content_rowid='id')",
            "INSERT INTO ft(ft) VALUES ('rebuild')",
        ],
        &[
            "SELECT rowid, n, typeof(n), body FROM ft WHERE ft MATCH 'fish'",
            "SELECT n FROM ft ORDER BY n",
            "SELECT rowid FROM ft WHERE ft MATCH '42'",
        ],
    ),
    (
        "implicit_rowid",
        &[
            "CREATE TABLE d(body TEXT)",
            "INSERT INTO d VALUES ('red fish'), ('blue fish')",
            "CREATE VIRTUAL TABLE ft USING fts5(body, content='d')",
            "INSERT INTO ft(ft) VALUES ('rebuild')",
        ],
        &[
            "SELECT rowid, * FROM ft WHERE ft MATCH 'fish'",
            "SELECT rowid, * FROM ft WHERE rowid > 1",
        ],
    ),
    (
        "content_rowid_not_an_alias",
        &[
            "CREATE TABLE d(k INTEGER, body TEXT)",
            "INSERT INTO d VALUES (10, 'red fish'), (20, 'blue fish')",
            "CREATE VIRTUAL TABLE ft USING fts5(body, content='d', content_rowid='k')",
            "INSERT INTO ft(ft) VALUES ('rebuild')",
        ],
        &[
            "SELECT rowid, * FROM ft WHERE ft MATCH 'blue'",
            "SELECT rowid, * FROM ft ORDER BY rowid DESC",
        ],
    ),
    (
        "view_content",
        &[
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
            "INSERT INTO docs VALUES (1, 'hello', 'alpha beta'), (2, 'world', 'gamma beta')",
            "CREATE VIEW v AS SELECT id, upper(body) AS body FROM docs",
            "CREATE VIRTUAL TABLE ft USING fts5(body, content='v', content_rowid='id')",
            "INSERT INTO ft(ft) VALUES ('rebuild')",
        ],
        &[
            "SELECT rowid, * FROM ft WHERE ft MATCH 'gamma'",
            "SELECT rowid, * FROM ft",
        ],
    ),
    // Scans follow the content table, MATCH follows the index.
    (
        "content_outside_the_index",
        DOCS,
        &[
            "INSERT INTO docs VALUES (3, 'new', 'delta')",
            "SELECT rowid, * FROM ft",
            "SELECT count(*) FROM ft",
            "SELECT rowid FROM ft WHERE ft MATCH 'delta'",
            "DELETE FROM docs WHERE id = 1",
            "SELECT rowid FROM ft WHERE ft MATCH 'alpha'",
            "SELECT rowid, * FROM ft",
        ],
    ),
    (
        "sync_triggers",
        DOCS_WITH_TRIGGERS,
        &[
            "INSERT INTO docs VALUES (3, 'third', 'delta beta')",
            "UPDATE docs SET body = 'epsilon' WHERE id = 1",
            "DELETE FROM docs WHERE id = 2",
            "SELECT rowid, * FROM ft WHERE ft MATCH 'beta'",
            "SELECT rowid, * FROM ft WHERE ft MATCH 'epsilon'",
            "SELECT rowid, * FROM ft",
            "SELECT rowid, highlight(ft, 1, '[', ']') FROM ft WHERE ft MATCH 'delta'",
        ],
    ),
    (
        "rebuild_in_a_transaction",
        DOCS,
        &[
            "BEGIN",
            "INSERT INTO docs VALUES (3, 'third', 'delta')",
            "INSERT INTO ft(ft) VALUES ('rebuild')",
            "SELECT rowid, title FROM ft WHERE ft MATCH 'delta'",
            "COMMIT",
            "SELECT rowid, title FROM ft WHERE ft MATCH 'delta'",
        ],
    ),
    (
        "delete_all",
        DOCS,
        &[
            "INSERT INTO ft(ft) VALUES ('delete-all')",
            "SELECT rowid, * FROM ft",
            "SELECT count(*) FROM ft",
            "SELECT rowid FROM ft WHERE ft MATCH 'beta'",
            "INSERT INTO ft(ft) VALUES ('rebuild')",
            "SELECT rowid, title FROM ft WHERE ft MATCH 'beta'",
        ],
    ),
    // RENAME does not follow into the content= option. Stock reads a hit's
    // content row only when a column of it is read, so MATCH by rowid still
    // works; a full scan reads the content table and fails.
    (
        "content_table_renamed",
        DOCS,
        &[
            "ALTER TABLE docs RENAME TO docs2",
            "SELECT rowid FROM ft WHERE ft MATCH 'beta' ORDER BY rowid",
            "SELECT count(*) FROM ft WHERE ft MATCH 'beta'",
            "SELECT rowid FROM ft WHERE ft MATCH 'beta OR hello' ORDER BY rank",
            "SELECT * FROM ft",
            "SELECT count(*) FROM ft",
        ],
    ),
    (
        "content_column_missing",
        &[
            "CREATE TABLE d(id INTEGER PRIMARY KEY, title TEXT)",
            "INSERT INTO d VALUES (1, 'one')",
            "CREATE VIRTUAL TABLE ft USING fts5(title, body, content='d', content_rowid='id')",
        ],
        &["SELECT * FROM ft", "INSERT INTO ft(ft) VALUES ('rebuild')"],
    ),
];

/// The `rank = 1` form compares the index with the content table.
const STOCK_CHECKS: &[&str] = &[
    "INSERT INTO ft(ft) VALUES ('integrity-check')",
    "INSERT INTO ft(ft, rank) VALUES ('integrity-check', 1)",
    "PRAGMA integrity_check",
];

/// Who wrote the seed the statements run against.
#[derive(Clone, Copy, Debug)]
enum Seed {
    /// The same in-memory connection.
    Memory,
    /// Another FrankenSQLite connection, closed before the open.
    Fsqlite,
    /// Stock.
    Stock,
}

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

fn render_rows(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(render_fsqlite)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn stock_query(conn: &rusqlite::Connection, sql: &str) -> String {
    let run = || -> rusqlite::Result<Vec<String>> {
        let mut stmt = conn.prepare(sql)?;
        let width = stmt.column_count();
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(
                (0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
        Ok(out)
    };
    match run() {
        Ok(rows) => format!("{sql} => ok {rows:?}"),
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => format!("{sql} => error:{message}"),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

async fn fsqlite_run(conn: &Connection, sql: &str) -> String {
    if sql.starts_with("SELECT") {
        match conn.query(sql).await {
            Ok(rows) => format!("{sql} => ok {:?}", render_rows(&rows)),
            Err(error) => format!("{sql} => error:{error}"),
        }
    } else {
        match conn.execute(sql).await {
            Ok(_) => format!("{sql} => ok []"),
            Err(error) => format!("{sql} => error:{error}"),
        }
    }
}

/// Every statement's outcome in one stock connection after the seed one
/// closed, and stock's checks of its own file.
fn stock_expected(path: &str, seed: &[&str], steps: &[&str]) -> (Vec<String>, Vec<String>) {
    {
        let conn = rusqlite::Connection::open(path).expect("stock open");
        for sql in seed {
            conn.execute_batch(sql).expect("stock seed");
        }
    }
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    let transcript = steps.iter().map(|sql| stock_query(&conn, sql)).collect();
    let checks = STOCK_CHECKS
        .iter()
        .map(|sql| stock_query(&conn, sql))
        .collect();
    (transcript, checks)
}

async fn fsqlite_transcript(path: &str, seed_by: Seed, seed: &[&str], steps: &[&str]) -> Vec<String> {
    let conn = match seed_by {
        Seed::Memory => {
            let conn = Connection::open(":memory:").await.expect("fsqlite open");
            for sql in seed {
                conn.execute(sql).await.expect("fsqlite seed");
            }
            conn
        }
        Seed::Fsqlite => {
            let seeder = Connection::open(path).await.expect("fsqlite open");
            for sql in seed {
                seeder.execute(sql).await.expect("fsqlite seed");
            }
            seeder.close().await.expect("fsqlite close");
            Connection::open(path).await.expect("fsqlite reopen")
        }
        Seed::Stock => {
            let stock = rusqlite::Connection::open(path).expect("stock open");
            for sql in seed {
                stock.execute_batch(sql).expect("stock seed");
            }
            drop(stock);
            Connection::open(path).await.expect("fsqlite open")
        }
    };
    let mut transcript = Vec::new();
    for sql in steps {
        transcript.push(fsqlite_run(&conn, sql).await);
    }
    conn.close().await.expect("fsqlite close");
    transcript
}

#[test]
fn external_content_columns_match_stock() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dir_path = dir.path().to_owned();
    let path_of = |name: &str| {
        dir_path
            .join(format!("{name}.db"))
            .to_str()
            .expect("utf-8 path")
            .to_owned()
    };
    let expected: Vec<(Vec<String>, Vec<String>)> = CASES
        .iter()
        .map(|(name, seed, steps)| stock_expected(&path_of(&format!("stock_{name}")), seed, steps))
        .collect();
    // The oracle itself: the content table's text, by name.
    assert_eq!(
        expected[1].0[0],
        "SELECT rowid, * FROM ft => ok [\"i:1|t:hello|t:alpha beta\", \"i:2|t:world|t:gamma beta\"]",
        "stock: {:#?}",
        expected[1].0
    );
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for ((name, seed, steps), (want, want_checks)) in CASES.iter().zip(&expected) {
            for seed_by in [Seed::Memory, Seed::Fsqlite, Seed::Stock] {
                let path = path_of(&format!("{name}_{seed_by:?}"));
                let got = fsqlite_transcript(&path, seed_by, seed, steps).await;
                if &got != want {
                    mismatches.push(format!(
                        "{name} ({seed_by:?})\n  fsqlite: {got:#?}\n  stock:   {want:#?}"
                    ));
                }
                if !matches!(seed_by, Seed::Memory) {
                    let stock = rusqlite::Connection::open(&path).expect("stock reopen");
                    let checks: Vec<String> = STOCK_CHECKS
                        .iter()
                        .map(|sql| stock_query(&stock, sql))
                        .collect();
                    if &checks != want_checks {
                        mismatches.push(format!(
                            "{name} ({seed_by:?}): stock checking the fsqlite file\n  \
                             fsqlite file: {checks:#?}\n  stock file:   {want_checks:#?}"
                        ));
                    }
                }
            }
        }
        for mismatch in &mismatches {
            eprintln!("event=fts5_external_content_projection_mismatch {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} mismatches; first: {}",
            mismatches.len(),
            mismatches[0]
        );
    });
}
