#![recursion_limit = "512"]

//! bd-8a8pr: with no other writer, a statement inside a transaction that
//! fails part-way and rolls back leaves no rowid gap, as in stock (which
//! recomputes `max(rowid) + 1`).
//!
//! fsqlite's writers take implicit rowids from a shared allocator that never
//! handed a reservation back on a statement rollback, so the next INSERT in
//! the transaction skipped the rolled-back rowids (5 where stock gives 3). A
//! statement now marks the allocator at its first reservation and gives the
//! rowids back on rollback when no other writer allocated after them. (A
//! failed autocommit statement on a file-backed database, a ROLLBACK, and a
//! statement another writer allocated behind still leave gaps; the README
//! lists them under Limitations.)
//!
//! Every case is compared against stock SQLite (rusqlite), in memory and
//! file-backed.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, v NOT NULL);\
INSERT INTO t(v) VALUES (1), (2);\
CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT, v NOT NULL);\
CREATE TABLE h(v NOT NULL);\
CREATE TABLE log(id INTEGER PRIMARY KEY, x);\
CREATE TABLE g(id INTEGER PRIMARY KEY, v NOT NULL);\
CREATE TRIGGER g_ai AFTER INSERT ON g BEGIN INSERT INTO log(x) VALUES ('g ' || NEW.v); END;";

const STATEMENTS: &[&str] = &[
    "BEGIN",
    "INSERT INTO t(v) VALUES (3), (NULL)",
    "INSERT INTO t(v) VALUES (4)",
    "INSERT INTO t(v) SELECT value FROM generate_series(1, 3) UNION ALL SELECT NULL",
    "INSERT INTO t(v) VALUES (5)",
    "SAVEPOINT s1",
    "INSERT INTO t(v) VALUES (8), (NULL)",
    "INSERT INTO t(v) VALUES (9)",
    "ROLLBACK TO s1",
    "INSERT INTO t(v) VALUES (10)",
    "RELEASE s1",
    "INSERT INTO a(v) VALUES (1), (NULL)",
    "INSERT INTO a(v) VALUES (2)",
    "INSERT INTO h(v) VALUES (1), (NULL)",
    "INSERT INTO h(v) VALUES (2)",
    // The trigger's own inserts roll back with the statement.
    "INSERT INTO g(v) VALUES (1), (NULL)",
    "INSERT INTO g(v) VALUES (2)",
    "COMMIT",
];

const DUMPS: &[&str] = &[
    "SELECT id, v FROM t ORDER BY id",
    "SELECT id, v FROM a ORDER BY id",
    "SELECT name, seq FROM sqlite_sequence ORDER BY name",
    "SELECT rowid, v FROM h ORDER BY rowid",
    "SELECT id, v FROM g ORDER BY id",
    "SELECT id, x FROM log ORDER BY id",
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
        // The rowids are under test, not the error wording.
        let outcome = match query(sql) {
            Ok(rows) => format!("ok {rows:?}"),
            Err(_) => "error".to_owned(),
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
            Err(_) => "error".to_owned(),
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
        assert_eq!(got, want, "bd-8a8pr {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-8a8pr {label} transcript length");
}

#[test]
fn rowids_after_statement_rollback_match_stock_in_memory() {
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn rowids_after_statement_rollback_match_stock_file_backed() {
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("8a8pr.db");
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

/// The allocator the rewind leaves behind is the one every connection to the
/// database shares: a second connection's next rowid follows the rows the
/// first one committed, with no duplicate and no gap.
#[test]
fn a_second_connection_allocates_after_the_rewound_statement_like_stock() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("8a8pr_two_connections.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async move {
        let a = Connection::open(&path_str).await.expect("open a");
        a.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v NOT NULL)")
            .await
            .expect("schema");
        let b = Connection::open(&path_str).await.expect("open b");
        a.execute("BEGIN CONCURRENT").await.expect("a begin");
        a.query("INSERT INTO t(v) VALUES ('a1')")
            .await
            .expect("a insert");
        assert!(
            a.query("INSERT INTO t(v) VALUES ('a2'), (NULL)")
                .await
                .is_err()
        );
        a.query("INSERT INTO t(v) VALUES ('a3')")
            .await
            .expect("a insert");
        a.execute("COMMIT").await.expect("a commit");
        b.query("INSERT INTO t(v) VALUES ('b1')")
            .await
            .expect("b insert");
        let rows = b
            .query("SELECT id, v FROM t ORDER BY id")
            .await
            .expect("dump");
        assert_eq!(
            render_rows(&rows),
            ["i:1|t:a1", "i:2|t:a3", "i:3|t:b1"],
            "stock gives 1, 2, 3"
        );
    });
    let checked = rusqlite::Connection::open(&path).expect("stock open of fsqlite file");
    let verdict: String = checked
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(verdict, "ok", "stock integrity_check of the fsqlite file");
}
