#![recursion_limit = "512"]

//! DML subqueries were evaluated without the statement's parameters, and
//! their results were frozen into programs that were reused.
//!
//! `compile_table_insert` resolves VALUES subqueries to literals before
//! codegen, because VDBE codegen compiles VALUES rows without a scan context.
//! It ran them with no parameters, so `?2 IN (SELECT x FROM t WHERE g = ?3)`
//! compared against NULL, and the resulting program was cached under the
//! statement's text, so a later execution inside the same transaction reused
//! a stale result. Preparation did the same to every INSERT, UPDATE and
//! DELETE subquery: it folded them once, with no parameters, into the
//! prepared program, so later executions never saw new data. Every case is
//! compared against stock SQLite.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE t(x INTEGER PRIMARY KEY, g);\
INSERT INTO t VALUES (1, 1), (2, 1), (3, 2);\
CREATE TABLE plain(k, c, n);\
CREATE TABLE trig(k INTEGER PRIMARY KEY, c, n);\
CREATE TABLE side(m);\
CREATE TRIGGER trig_ai AFTER INSERT ON trig BEGIN \
    INSERT INTO side VALUES (NEW.k || ':' || quote(NEW.c) || ':' || quote(NEW.n)); END;\
CREATE TABLE up(k INTEGER PRIMARY KEY, c, hits DEFAULT 0);";

/// (sql, params). Each statement runs once per parameter row.
const CASES: &[(&str, &[[i64; 3]])] = &[
    (
        "INSERT INTO plain VALUES (?1, ?2 IN (SELECT x FROM t WHERE g = ?3), \
         (SELECT count(*) FROM t WHERE g = ?3))",
        &[[10, 1, 1], [11, 3, 1], [12, 3, 2], [13, 1, 2], [14, 0, 1]],
    ),
    (
        "INSERT INTO trig VALUES (?1, ?2 IN (SELECT x FROM t WHERE g = ?3), \
         (SELECT count(*) FROM t WHERE g = ?3))",
        &[[20, 1, 1], [21, 3, 1], [22, 3, 2]],
    ),
    (
        "INSERT INTO plain VALUES (?, ? IN (SELECT x FROM t WHERE g = ?), \
         (SELECT count(*) FROM plain))",
        &[[30, 2, 1], [31, 3, 2], [32, 9, 1]],
    ),
    (
        "INSERT INTO plain VALUES (?1, ?2 NOT IN (SELECT x FROM t WHERE g = ?3), 0), \
         (?1 + 1, ?3 IN (SELECT g FROM t WHERE x > ?2), 1)",
        &[[40, 1, 1], [42, 2, 2]],
    ),
    (
        "INSERT INTO up VALUES (?1 % 2, ?2 IN (SELECT x FROM t WHERE g = ?3), 0) \
         ON CONFLICT(k) DO UPDATE SET c = excluded.c, hits = hits + 1",
        &[[50, 1, 1], [51, 3, 1], [52, 3, 2], [53, 2, 1]],
    ),
];

const DUMPS: &[&str] = &[
    "SELECT * FROM plain ORDER BY rowid",
    "SELECT * FROM trig ORDER BY rowid",
    "SELECT * FROM side ORDER BY rowid",
    "SELECT * FROM up ORDER BY rowid",
    "SELECT * FROM log ORDER BY rowid",
];

/// Statements repeated inside one explicit transaction, with a write to the
/// subquery's table between them: the second and third must see it.
const IN_TXN: &[&str] = &[
    "BEGIN",
    "INSERT INTO log VALUES ('txn', (SELECT count(*) FROM t), 4 IN (SELECT x FROM t))",
    "INSERT INTO t VALUES (4, 3)",
    "INSERT INTO log VALUES ('txn', (SELECT count(*) FROM t), 4 IN (SELECT x FROM t))",
    "INSERT INTO t VALUES (5, 3)",
    "INSERT INTO log VALUES ('txn', (SELECT count(*) FROM t), 4 IN (SELECT x FROM t))",
    "COMMIT",
];

/// One prepared statement executed three times around a write.
const PREPARED: &str = "INSERT INTO log VALUES (?, (SELECT count(*) FROM t), \
    (SELECT max(x) FROM t WHERE g = ?) IN (SELECT x FROM t WHERE g = 1))";

/// Prepared UPDATE and DELETE whose subqueries read data, executed around
/// writes to that data: preparation folded these once as well.
const PREPARED_UPDATE: &str = "UPDATE log SET b = (SELECT count(*) FROM t WHERE g = ?) \
    WHERE a = ?";
const PREPARED_DELETE: &str = "DELETE FROM log WHERE a = ? \
    AND b < (SELECT count(*) FROM t WHERE x > ?)";

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
    conn.execute_batch("CREATE TABLE log(a, b, c);")
        .expect("stock log");
    for (sql, rows) in CASES {
        for row in *rows {
            conn.execute(sql, rusqlite::params_from_iter(row.iter()))
                .expect("stock insert");
        }
    }
    for sql in IN_TXN {
        conn.execute_batch(sql).expect("stock txn statement");
    }
    {
        let mut stmt = conn.prepare(PREPARED).expect("stock prepare");
        stmt.execute(rusqlite::params![1, 1]).expect("stock prepared 1");
        conn.execute_batch("INSERT INTO t VALUES (9, 1)")
            .expect("stock write");
        stmt.execute(rusqlite::params![2, 1]).expect("stock prepared 2");
        stmt.execute(rusqlite::params![3, 2]).expect("stock prepared 3");
        let mut update = conn.prepare(PREPARED_UPDATE).expect("stock prepare update");
        let mut delete = conn.prepare(PREPARED_DELETE).expect("stock prepare delete");
        update.execute(rusqlite::params![1, 1]).expect("stock update 1");
        delete.execute(rusqlite::params![2, 0]).expect("stock delete 1");
        conn.execute_batch("INSERT INTO t VALUES (10, 1), (11, 2)")
            .expect("stock write 2");
        update.execute(rusqlite::params![1, 3]).expect("stock update 2");
        delete.execute(rusqlite::params![2, 0]).expect("stock delete 2");
    }
    let mut transcript = Vec::new();
    for sql in DUMPS {
        let mut stmt = conn.prepare(sql).expect("stock dump prepare");
        let width = stmt.column_count();
        let rows = stmt
            .query_map([], |row| {
                Ok((0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"))
            })
            .expect("stock dump")
            .collect::<Result<Vec<_>, _>>()
            .expect("stock rows");
        transcript.push(format!("{sql} => {rows:?}"));
    }
    transcript
}

async fn fsqlite_transcript(conn: &Connection) -> Vec<String> {
    conn.execute_batch(SCHEMA).await.expect("schema");
    conn.execute_batch("CREATE TABLE log(a, b, c);")
        .await
        .expect("log");
    for (sql, rows) in CASES {
        for row in *rows {
            let params: Vec<SqliteValue> = row.iter().copied().map(SqliteValue::Integer).collect();
            conn.execute_with_params(sql, &params)
                .await
                .unwrap_or_else(|error| panic!("{sql} {row:?}: {error}"));
        }
    }
    for sql in IN_TXN {
        conn.execute(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    {
        let stmt = conn.prepare(PREPARED).await.expect("prepare");
        stmt.execute_with_params(&[SqliteValue::Integer(1), SqliteValue::Integer(1)])
            .await
            .expect("prepared 1");
        conn.execute("INSERT INTO t VALUES (9, 1)")
            .await
            .expect("write");
        stmt.execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(1)])
            .await
            .expect("prepared 2");
        stmt.execute_with_params(&[SqliteValue::Integer(3), SqliteValue::Integer(2)])
            .await
            .expect("prepared 3");
        let update = conn.prepare(PREPARED_UPDATE).await.expect("prepare update");
        let delete = conn.prepare(PREPARED_DELETE).await.expect("prepare delete");
        update
            .execute_with_params(&[SqliteValue::Integer(1), SqliteValue::Integer(1)])
            .await
            .expect("update 1");
        delete
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(0)])
            .await
            .expect("delete 1");
        conn.execute("INSERT INTO t VALUES (10, 1), (11, 2)")
            .await
            .expect("write 2");
        update
            .execute_with_params(&[SqliteValue::Integer(1), SqliteValue::Integer(3)])
            .await
            .expect("update 2");
        delete
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Integer(0)])
            .await
            .expect("delete 2");
    }
    let mut transcript = Vec::new();
    for sql in DUMPS {
        let rows = conn.query(sql).await.expect("dump");
        transcript.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    transcript
}

fn assert_matches_stock(label: &str, got: &[String], want: &[String]) {
    for (line, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "{label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "{label} transcript length");
}

#[test]
fn insert_values_subqueries_see_parameters_in_memory() {
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn insert_values_subqueries_see_parameters_file_backed() {
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("values_subquery.db");
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
