#![recursion_limit = "512"]

//! GH#211: writes to an external-content FTS5 table (`content='docs'`) whose
//! index the connection did not build itself, against stock SQLite
//! (rusqlite).
//!
//! An external-content table keeps no `%_content` rows, so when the connection
//! binds an index someone else wrote (a reopened file, a file stock created,
//! or the segments a `'rebuild'` just persisted) its postings exist only in
//! `%_data`. The first INSERT or `'delete'` used to rebuild the in-memory index
//! from the (empty) `%_content` rows, so MATCH then saw only the rows written
//! since. Each case runs the same setup and writes through the documented
//! sync triggers (or explicit FTS5 commands) and compares every write, the
//! MATCH results in the writing connection and after a reopen, and stock's
//! reading of the file (MATCH, both `'integrity-check'` forms and
//! `PRAGMA integrity_check`).
#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)",
    "CREATE VIRTUAL TABLE ft USING fts5(body, content='docs', content_rowid='id')",
    "INSERT INTO docs VALUES (1, 'hello world'), (2, 'hello there'), (3, 'goodbye world')",
    "INSERT INTO ft(ft) VALUES('rebuild')",
];

/// The external-content sync triggers from the FTS5 documentation.
const TRIGGERS: &[&str] = &[
    "CREATE TRIGGER docs_ai AFTER INSERT ON docs BEGIN \
     INSERT INTO ft(rowid, body) VALUES (new.id, new.body); END",
    "CREATE TRIGGER docs_ad AFTER DELETE ON docs BEGIN \
     INSERT INTO ft(ft, rowid, body) VALUES ('delete', old.id, old.body); END",
    "CREATE TRIGGER docs_au AFTER UPDATE ON docs BEGIN \
     INSERT INTO ft(ft, rowid, body) VALUES ('delete', old.id, old.body); \
     INSERT INTO ft(rowid, body) VALUES (new.id, new.body); END",
];

/// (name, uses the sync triggers, writes)
const CASES: &[(&str, bool, &[&str])] = &[
    ("insert", true, &["INSERT INTO docs VALUES (4, 'hello again')"]),
    ("delete", true, &["DELETE FROM docs WHERE id = 2"]),
    (
        "update",
        true,
        &["UPDATE docs SET body = 'goodbye there' WHERE id = 3"],
    ),
    (
        "two_inserts",
        true,
        &[
            "INSERT INTO docs VALUES (4, 'hello again')",
            "INSERT INTO docs VALUES (5, 'there again')",
        ],
    ),
    (
        "explicit_insert",
        false,
        &[
            "INSERT INTO docs VALUES (4, 'hello again')",
            "INSERT INTO ft(rowid, body) VALUES (4, 'hello again')",
        ],
    ),
    (
        "explicit_delete",
        false,
        &[
            "INSERT INTO ft(ft, rowid, body) VALUES ('delete', 2, 'hello there')",
            "DELETE FROM docs WHERE id = 2",
        ],
    ),
    // Stock re-indexes the source from scratch, so row 1 must leave the index.
    (
        "rebuild_after_source_delete",
        false,
        &[
            "DELETE FROM docs WHERE id = 1",
            "INSERT INTO ft(ft) VALUES('rebuild')",
        ],
    ),
];

const TERMS: &[&str] = &["hello", "world", "there", "goodbye", "again", "hello OR goodbye"];

const STOCK_CHECKS: &[&str] = &[
    "INSERT INTO ft(ft) VALUES('integrity-check')",
    "INSERT INTO ft(ft, rank) VALUES('integrity-check', 1)",
    "PRAGMA integrity_check",
];

/// Who builds the index the writes land on.
#[derive(Clone, Copy, Debug)]
enum Setup {
    /// FrankenSQLite, in the writing connection (bound by the `'rebuild'`).
    SameConnection,
    /// FrankenSQLite, then the file is reopened.
    Reopened,
    /// Stock creates the file.
    Stock,
}

fn setup_sql(triggers: bool) -> Vec<&'static str> {
    let mut sql = SETUP.to_vec();
    if triggers {
        sql.extend_from_slice(TRIGGERS);
    }
    sql
}

fn queries() -> Vec<String> {
    TERMS
        .iter()
        .map(|term| format!("SELECT rowid FROM ft WHERE ft MATCH '{term}' ORDER BY rowid"))
        .collect()
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

async fn fsqlite_query(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => format!("{sql} => ok {:?}", render_rows(&rows)),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

async fn fsqlite_execute(conn: &Connection, sql: &str) -> String {
    match conn.execute(sql).await {
        Ok(_) => format!("{sql} => ok []"),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

fn stock_setup(path: &str, triggers: bool) {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    for sql in setup_sql(triggers) {
        conn.execute_batch(sql).expect("stock setup");
    }
}

/// The writes' outcomes followed by the MATCH results, all in one stock
/// connection, and stock's checks of its own file.
fn stock_expected(path: &str, triggers: bool, writes: &[&str]) -> (Vec<String>, Vec<String>) {
    stock_setup(path, triggers);
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    let mut transcript: Vec<String> = writes.iter().map(|sql| stock_query(&conn, sql)).collect();
    transcript.extend(queries().iter().map(|sql| stock_query(&conn, sql)));
    let checks = STOCK_CHECKS
        .iter()
        .map(|sql| stock_query(&conn, sql))
        .collect();
    (transcript, checks)
}

/// One case under one setup: the writing connection's transcript, a fresh
/// FrankenSQLite connection's MATCH results, and stock reading the file.
async fn run_case(
    path: &str,
    setup: Setup,
    triggers: bool,
    writes: &[&str],
    want: &[String],
    want_checks: &[String],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    let conn = match setup {
        Setup::Stock => {
            stock_setup(path, triggers);
            Connection::open(path).await.expect("fsqlite open")
        }
        Setup::SameConnection | Setup::Reopened => {
            let conn = Connection::open(path).await.expect("fsqlite open");
            for sql in setup_sql(triggers) {
                conn.execute(sql).await.expect("fsqlite setup");
            }
            if matches!(setup, Setup::Reopened) {
                conn.close().await.expect("fsqlite close");
                Connection::open(path).await.expect("fsqlite reopen")
            } else {
                conn
            }
        }
    };
    let mut got = Vec::new();
    for sql in writes {
        got.push(fsqlite_execute(&conn, sql).await);
    }
    for sql in queries() {
        got.push(fsqlite_query(&conn, &sql).await);
    }
    conn.close().await.expect("fsqlite close");
    if got != want {
        mismatches.push(format!(
            "writing connection\n  fsqlite: {got:#?}\n  stock:   {want:#?}"
        ));
    }
    let want_matches = &want[writes.len()..];
    let reopened = Connection::open(path).await.expect("fsqlite reopen");
    let mut got_reopened = Vec::new();
    for sql in queries() {
        got_reopened.push(fsqlite_query(&reopened, &sql).await);
    }
    reopened.close().await.expect("fsqlite close");
    if got_reopened != want_matches {
        mismatches.push(format!(
            "fsqlite after a reopen\n  fsqlite: {got_reopened:#?}\n  stock:   {want_matches:#?}"
        ));
    }
    // Stock must read the index FrankenSQLite left behind.
    let stock = rusqlite::Connection::open(path).expect("stock reopen");
    let stock_matches: Vec<String> = queries()
        .iter()
        .map(|sql| stock_query(&stock, sql))
        .collect();
    let stock_checks: Vec<String> = STOCK_CHECKS
        .iter()
        .map(|sql| stock_query(&stock, sql))
        .collect();
    if stock_matches != want_matches || stock_checks != want_checks {
        mismatches.push(format!(
            "stock reading the fsqlite file\n  {stock_matches:#?}\n  {stock_checks:#?}"
        ));
    }
    mismatches
}

#[test]
fn external_content_writes_after_open_match_stock() {
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
        .map(|(name, triggers, writes)| {
            stock_expected(&path_of(&format!("stock_{name}")), *triggers, writes)
        })
        .collect();
    // The oracle itself: every write succeeds and its checks pass.
    for ((name, _, writes), (transcript, checks)) in CASES.iter().zip(&expected) {
        assert!(
            transcript[..writes.len()].iter().all(|line| line.ends_with("=> ok []")),
            "stock writes for {name}: {transcript:#?}"
        );
        assert!(
            checks.iter().all(|line| line.ends_with("=> ok []") || line.ends_with("=> ok [\"t:ok\"]")),
            "stock checks for {name}: {checks:#?}"
        );
    }
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for ((name, triggers, writes), (want, want_checks)) in CASES.iter().zip(&expected) {
            for setup in [Setup::SameConnection, Setup::Reopened, Setup::Stock] {
                let path = path_of(&format!("{name}_{setup:?}"));
                let label = format!("{name} ({setup:?})");
                for mismatch in run_case(&path, setup, *triggers, writes, want, want_checks).await {
                    mismatches.push(format!("{label}: {mismatch}"));
                }
            }
        }
        for mismatch in &mismatches {
            eprintln!("event=fts5_external_content_mismatch {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} mismatches; first: {}",
            mismatches.len(),
            mismatches[0]
        );
    });
}
