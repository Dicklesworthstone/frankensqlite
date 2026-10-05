#![recursion_limit = "512"]

//! bd-yb70u: an UPDATE that keeps each row's rowid no longer bumps the
//! concurrent rowid allocator's floor once per row (the bump walked the table
//! cursor to its last row every time). The rowids a later INSERT allocates
//! must still be stock's: after same-rowid rewrites of every size, after
//! rowid-changing UPDATEs (which still bump), with AUTOINCREMENT and a lowered
//! `sqlite_sequence`, without an INTEGER PRIMARY KEY, and inside explicit
//! transactions.
//!
//! Every case is compared against stock SQLite (rusqlite), in memory and
//! file-backed (where writers use the concurrent allocator).

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, v);\
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300) \
    INSERT INTO t(id, v) SELECT i, i FROM n;\
CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT, v);\
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 50) \
    INSERT INTO a(v) SELECT i FROM n;\
CREATE TABLE h(v);\
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 120) \
    INSERT INTO h(v) SELECT i FROM n;";

const STATEMENTS: &[&str] = &[
    // Same-size in-place rewrites, then an allocation.
    "UPDATE t SET v = v + 1",
    "INSERT INTO t(v) VALUES ('after same-size')",
    // Growing and shrinking rewrites (delete + insert at the same position).
    "UPDATE t SET v = printf('%0200d', v) WHERE id % 3 = 0",
    "UPDATE t SET v = 1 WHERE id % 3 = 0",
    "INSERT INTO t(v) VALUES ('after resize')",
    // Rowid-changing UPDATE still raises the floor.
    "UPDATE t SET id = id + 1000 WHERE id > 290",
    "INSERT INTO t(v) VALUES ('after rowid change')",
    // Inside an explicit transaction.
    "BEGIN",
    "UPDATE t SET v = v || 'x' WHERE id < 50",
    "INSERT INTO t(v) VALUES ('in txn')",
    "COMMIT",
    // AUTOINCREMENT: the top rows deleted, then rewrites, then allocations.
    "DELETE FROM a WHERE id > 40",
    "UPDATE a SET v = v * 10",
    "INSERT INTO a(v) VALUES ('after delete + update')",
    "UPDATE sqlite_sequence SET seq = 5 WHERE name = 'a'",
    "UPDATE a SET v = v + 1",
    "INSERT INTO a(v) VALUES ('after lowered seq')",
    // Hidden rowid.
    "DELETE FROM h WHERE rowid > 100",
    "UPDATE h SET v = v * 2",
    "INSERT INTO h(v) VALUES ('hidden')",
];

const DUMPS: &[&str] = &[
    "SELECT count(*), max(id), sum(length(v)) FROM t",
    "SELECT id, v FROM t WHERE typeof(v) = 'text' AND v NOT GLOB '0*' ORDER BY id",
    "SELECT id, v FROM a ORDER BY id",
    "SELECT name, seq FROM sqlite_sequence ORDER BY name",
    "SELECT rowid, v FROM h WHERE rowid > 95 ORDER BY rowid",
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
        let last_rowid = conn.last_insert_rowid();
        transcript.push(format!("{sql} => {outcome} last_insert_rowid={last_rowid}"));
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
        let last_rowid = conn.last_insert_rowid();
        transcript.push(format!("{sql} => {outcome} last_insert_rowid={last_rowid}"));
    }
    for sql in DUMPS {
        let rows = conn.query(sql).await.expect("dump");
        transcript.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    transcript
}

fn assert_matches_stock(label: &str, got: &[String], want: &[String]) {
    for (line, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "bd-yb70u {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-yb70u {label} transcript length");
}

#[test]
fn rowid_allocation_after_same_rowid_updates_matches_stock_in_memory() {
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn rowid_allocation_after_same_rowid_updates_matches_stock_file_backed() {
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("yb70u_alloc.db");
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
