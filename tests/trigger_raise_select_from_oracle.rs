#![recursion_limit = "512"]

//! A trigger body's `SELECT RAISE(...) FROM u WHERE ...` never fired.
//!
//! Only the FROM-less RAISE shapes (`SELECT RAISE(...) [WHERE c]` and
//! `SELECT CASE WHEN c THEN RAISE(...) END`) were evaluated; every other
//! RAISE-shaped SELECT — the common constraint idiom `SELECT RAISE(ABORT,
//! 'dup') FROM u WHERE u.k = NEW.k`, or one with GROUP BY / HAVING, ORDER BY
//! or LIMIT — was skipped as if its condition were false, so the trigger
//! silently enforced nothing. Stock evaluates the RAISE for each row the
//! SELECT yields. Every case is compared against stock SQLite.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE u(k INTEGER, tag TEXT);\
INSERT INTO u VALUES (1, 'a'), (2, 'b'), (2, 'bb'), (5, 'e');\
CREATE TABLE t(k INTEGER, v TEXT);\
CREATE TABLE log(msg);\
CREATE TRIGGER t_bi BEFORE INSERT ON t BEGIN \
  SELECT RAISE(ABORT, 'dup k') FROM u WHERE u.k = NEW.k AND NEW.v = 'abort'; \
  SELECT RAISE(IGNORE) FROM u WHERE u.k = NEW.k AND NEW.v = 'ignore'; \
  SELECT RAISE(FAIL, 'fail k') FROM u WHERE u.k = NEW.k AND NEW.v = 'fail'; \
  SELECT CASE WHEN u.tag = 'bb' THEN RAISE(ABORT, 'case bb') END FROM u \
      WHERE u.k = NEW.k AND NEW.v = 'case'; \
  SELECT RAISE(ABORT, 'grouped') FROM u WHERE NEW.v = 'group' \
      GROUP BY u.k HAVING count(*) > 1 AND u.k = NEW.k; \
  SELECT RAISE(ABORT, 'limited') FROM u WHERE NEW.v = 'limit' LIMIT 0; \
  SELECT RAISE(ABORT, 'ordered') FROM u WHERE u.k = NEW.k AND NEW.v = 'order' ORDER BY u.tag DESC; \
  SELECT RAISE(ROLLBACK, 'rolled back') FROM u AS x JOIN u AS y ON x.k = y.k AND x.tag <> y.tag \
      WHERE x.k = NEW.k AND NEW.v = 'rollback'; \
  INSERT INTO log VALUES ('passed ' || NEW.k || ' ' || NEW.v); \
END;\
CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN \
  SELECT RAISE(ABORT, 'update clash') FROM u WHERE u.k = NEW.k AND u.tag = NEW.v; \
END;\
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN \
  SELECT RAISE(FAIL, 'delete guarded') FROM u WHERE u.k = OLD.k AND u.tag = 'e'; \
END;";

const STATEMENTS: &[&str] = &[
    "INSERT INTO t VALUES (1, 'abort')",
    "INSERT INTO t VALUES (3, 'abort')",
    "INSERT INTO t VALUES (2, 'ignore')",
    "INSERT INTO t VALUES (4, 'ignore')",
    "INSERT INTO t VALUES (5, 'fail')",
    "INSERT INTO t VALUES (1, 'case')",
    "INSERT INTO t VALUES (2, 'case')",
    "INSERT INTO t VALUES (2, 'group')",
    "INSERT INTO t VALUES (1, 'group')",
    "INSERT INTO t VALUES (2, 'limit')",
    "INSERT INTO t VALUES (2, 'order')",
    "INSERT INTO t SELECT k, 'ignore' FROM u",
    "INSERT INTO t VALUES (7, 'x'), (8, 'y'), (5, 'fail'), (9, 'z')",
    "BEGIN",
    "INSERT INTO t VALUES (10, 'in txn')",
    "INSERT INTO t VALUES (2, 'rollback')",
    "INSERT INTO t VALUES (11, 'after rollback')",
    "UPDATE t SET v = 'b' WHERE k = 3",
    "UPDATE t SET v = 'b', k = 2 WHERE k = 3",
    "UPDATE t SET v = 'bb!' WHERE k = 1",
    "DELETE FROM t WHERE k IN (7, 8)",
    "INSERT INTO t VALUES (5, 'del me')",
    "DELETE FROM t WHERE k >= 5",
];

const DUMPS: &[&str] = &[
    "SELECT k, v FROM t ORDER BY rowid",
    "SELECT msg FROM log ORDER BY rowid",
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

/// `changes()` is compared after DML only (it is not the subject here).
fn is_dml(sql: &str) -> bool {
    ["INSERT", "UPDATE", "DELETE"]
        .iter()
        .any(|verb| sql.starts_with(verb))
}

/// The RAISE message is what both engines agree on; the error-code
/// decoration around it is not the subject of this keeper.
fn outcome_error(message: &str) -> String {
    for known in [
        "dup k",
        "fail k",
        "case bb",
        "grouped",
        "limited",
        "ordered",
        "rolled back",
        "update clash",
        "delete guarded",
    ] {
        if message.contains(known) {
            return format!("error:{known}");
        }
    }
    format!("error:{message}")
}

fn stock_transcript() -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(SCHEMA).expect("stock schema");
    let mut transcript = Vec::new();
    for sql in STATEMENTS {
        let outcome = match conn.execute_batch(sql) {
            Ok(()) if is_dml(sql) => {
                let changes: i64 = conn
                    .query_row("SELECT changes()", [], |row| row.get(0))
                    .expect("stock changes");
                format!("ok changes={changes}")
            }
            Ok(()) => "ok".to_owned(),
            Err(error) => outcome_error(&error.to_string()),
        };
        transcript.push(format!("{sql} => {outcome} autocommit={}", conn.is_autocommit()));
    }
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
    let mut transcript = Vec::new();
    for sql in STATEMENTS {
        let outcome = match conn.execute(sql).await {
            Ok(_) if is_dml(sql) => {
                let changes = render_rows(&conn.query("SELECT changes()").await.expect("changes"));
                format!("ok changes={}", changes.join(",").trim_start_matches("i:"))
            }
            Ok(_) => "ok".to_owned(),
            Err(error) => outcome_error(&error.to_string()),
        };
        transcript.push(format!(
            "{sql} => {outcome} autocommit={}",
            !conn.in_transaction()
        ));
    }
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
fn raise_select_with_from_fires_like_stock_in_memory() {
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn raise_select_with_from_fires_like_stock_file_backed() {
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("raise_from.db");
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
