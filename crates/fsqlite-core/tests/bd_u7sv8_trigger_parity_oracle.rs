#![recursion_limit = "512"]

//! bd-u7sv8: trigger parity gaps against stock SQLite.
//!
//! - A trigger's NEW row is assembled from the statement's values (an
//!   INSERT's row, or the OLD row with the SET applied), so its generated
//!   columns read NULL in INSERT triggers and kept the OLD values in UPDATE
//!   triggers. Stock computes them from the new base columns, VIRTUAL and
//!   STORED alike, before any trigger reads NEW.
//!
//! Every case is compared against stock SQLite (rusqlite), in memory and
//! file-backed.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE g(id INTEGER PRIMARY KEY, a INTEGER, s TEXT, \
    b INTEGER GENERATED ALWAYS AS (a * 10) VIRTUAL, \
    c TEXT GENERATED ALWAYS AS ('c' || a) STORED, \
    d GENERATED ALWAYS AS (b + 1) VIRTUAL, \
    e TEXT GENERATED ALWAYS AS (a + 0.5) STORED, \
    j GENERATED ALWAYS AS (json_object('s', s, 'b', b)) VIRTUAL);\
CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
CREATE TRIGGER g_bi BEFORE INSERT ON g BEGIN INSERT INTO log(msg) VALUES \
    ('bi ' || quote(NEW.a) || ' ' || quote(NEW.b) || ' ' || quote(NEW.c) || ' ' || quote(NEW.d) \
     || ' ' || quote(NEW.e) || ' ' || NEW.j); END;\
CREATE TRIGGER g_ai AFTER INSERT ON g BEGIN INSERT INTO log(msg) VALUES \
    ('ai ' || quote(NEW.a) || ' ' || quote(NEW.b) || ' ' || quote(NEW.c) || ' ' || quote(NEW.d) \
     || ' ' || quote(NEW.e) || ' ' || NEW.j); END;\
CREATE TRIGGER g_bu BEFORE UPDATE ON g BEGIN INSERT INTO log(msg) VALUES \
    ('bu ' || quote(OLD.b) || '>' || quote(NEW.b) || ' ' || quote(OLD.c) || '>' || quote(NEW.c) \
     || ' ' || quote(NEW.d) || ' ' || quote(NEW.e) || ' ' || NEW.j); END;\
CREATE TRIGGER g_au AFTER UPDATE ON g BEGIN INSERT INTO log(msg) VALUES \
    ('au ' || quote(OLD.b) || '>' || quote(NEW.b) || ' ' || quote(OLD.c) || '>' || quote(NEW.c) \
     || ' ' || quote(NEW.d) || ' ' || quote(NEW.e) || ' ' || NEW.j); END;\
CREATE TRIGGER g_ad AFTER DELETE ON g BEGIN INSERT INTO log(msg) VALUES \
    ('ad ' || quote(OLD.b) || ' ' || quote(OLD.c) || ' ' || quote(OLD.d) || ' ' || OLD.j); END;\
CREATE TRIGGER g_bu_when BEFORE UPDATE OF a ON g WHEN NEW.b > 1000 BEGIN \
    INSERT INTO log(msg) VALUES ('when ' || NEW.id || ' ' || NEW.b); END;\
CREATE TABLE src(k INTEGER PRIMARY KEY, delta INTEGER);\
INSERT INTO src VALUES (1, 5), (3, 7);";

const STATEMENTS: &[&str] = &[
    "INSERT INTO g(id, a, s) VALUES (1, 5, 'x')",
    "INSERT INTO g(id, a, s) VALUES (2, 6, 'y'), (3, 7, 'z')",
    "INSERT INTO g(id, a, s) SELECT k + 10, delta, 'src' FROM src",
    "UPDATE g SET a = a + 1 WHERE id = 1",
    "UPDATE g SET a = a + 100",
    "UPDATE g SET s = s || '!' WHERE id = 2",
    "UPDATE g SET a = g.a + src.delta FROM src WHERE src.k = g.id",
    "INSERT INTO g(id, a, s) VALUES (2, 9, 'dup') ON CONFLICT(id) DO UPDATE SET a = excluded.a * 2",
    "INSERT OR REPLACE INTO g(id, a, s) VALUES (3, 1, 'replaced')",
    "DELETE FROM g WHERE id = 2",
    "DELETE FROM g WHERE id > 10",
];

const DUMPS: &[&str] = &[
    "SELECT seq, msg FROM log ORDER BY seq",
    "SELECT id, a, s, b, c, d, e, j FROM g ORDER BY id",
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

fn stock_transcript() -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(SCHEMA).expect("stock schema");
    let query = |sql: &str| -> Result<Vec<String>, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let width = stmt.column_count();
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            out.push(
                (0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
        Ok(out)
    };
    let mut transcript = Vec::new();
    for sql in STATEMENTS {
        let outcome = match query(sql) {
            Ok(rows) => format!("ok {rows:?}"),
            Err(message) => format!("error:{message}"),
        };
        transcript.push(format!("{sql} => {outcome}"));
    }
    for sql in DUMPS {
        transcript.push(format!("{sql} => {:?}", query(sql).expect("stock dump")));
    }
    transcript
}

async fn fsqlite_transcript(conn: &Connection) -> Vec<String> {
    conn.execute_batch(SCHEMA).await.expect("schema");
    let mut transcript = Vec::new();
    for sql in STATEMENTS {
        let outcome = match conn.query(sql).await {
            Ok(rows) => format!("ok {:?}", render_rows(&rows)),
            Err(error) => format!("error:{error}"),
        };
        transcript.push(format!("{sql} => {outcome}"));
    }
    for sql in DUMPS {
        let rows = conn.query(sql).await.expect("dump");
        transcript.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    transcript
}

fn assert_matches_stock(label: &str, got: &[String], want: &[String]) {
    for (line, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "bd-u7sv8 {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-u7sv8 {label} transcript length");
}

#[test]
fn trigger_new_generated_columns_match_stock_in_memory() {
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn trigger_new_generated_columns_match_stock_file_backed() {
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("u7sv8.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(&path_str).await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("file-backed", &got, &expected);
    });
    let checked = rusqlite::Connection::open(&path).expect("stock open of fsqlite file");
    let verdict: String = checked
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(verdict, "ok", "stock integrity_check of the fsqlite file");
}
