#![recursion_limit = "512"]

//! bd-yb70u: a multi-row UPDATE / DELETE fires its triggers in row-key order.
//!
//! Stock collects the rows such a statement changes into a table keyed by the
//! row key (an ephemeral rowid table, or one keyed like the WITHOUT ROWID
//! PRIMARY KEY) before changing any of them, so its triggers see the rows in
//! ascending rowid / primary-key order even when the WHERE clause is answered
//! by an index. fsqlite visited them in the WHERE scan's order (index order),
//! both in the row-by-row replay and in the batched DELETE trigger path.
//!
//! Every case is compared against stock SQLite (rusqlite), in memory and
//! file-backed.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
CREATE TABLE t(a INTEGER PRIMARY KEY, b, c);\
CREATE INDEX tb ON t(b);\
INSERT INTO t VALUES (1, 30, 0), (2, 20, 0), (3, 10, 0), (4, 25, 0), (5, 5, 0), \
    (-7, 15, 0), (9000000000, 12, 0);\
CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN INSERT INTO log(msg) VALUES ('t bu ' || OLD.a); END;\
CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN INSERT INTO log(msg) VALUES \
    ('t au ' || NEW.a || ' done ' || (SELECT count(*) FROM t WHERE c > 0)); END;\
CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT INTO log(msg) VALUES ('t ad ' || OLD.a); END;\
CREATE TABLE h(b, c);\
CREATE INDEX hb ON h(b);\
INSERT INTO h(rowid, b, c) VALUES (10, 30, 0), (20, 10, 0), (30, 20, 0);\
CREATE TRIGGER h_ad AFTER DELETE ON h BEGIN INSERT INTO log(msg) VALUES ('h ad ' || OLD.rowid); END;\
CREATE TABLE wd(k TEXT PRIMARY KEY DESC, v) WITHOUT ROWID;\
CREATE INDEX wdv ON wd(v);\
INSERT INTO wd VALUES ('c', 1), ('a', 2), ('b', 3), ('d', 0);\
CREATE TRIGGER wd_au AFTER UPDATE ON wd BEGIN INSERT INTO log(msg) VALUES ('wd au ' || NEW.k); END;\
CREATE TRIGGER wd_bd BEFORE DELETE ON wd BEGIN INSERT INTO log(msg) VALUES ('wd bd ' || OLD.k); END;\
CREATE TRIGGER wd_ad AFTER DELETE ON wd BEGIN INSERT INTO log(msg) VALUES ('wd ad ' || OLD.k); END;\
CREATE TABLE wn(k TEXT COLLATE NOCASE, j INT, v, PRIMARY KEY(k, j DESC)) WITHOUT ROWID;\
CREATE INDEX wnv ON wn(v);\
INSERT INTO wn VALUES ('B', 1, 1), ('a', 2, 2), ('A', 3, 3), ('c', 0, 4), ('b', 5, 5);\
CREATE TRIGGER wn_bd BEFORE DELETE ON wn BEGIN INSERT INTO log(msg) VALUES ('wn bd ' || OLD.k || OLD.j); END;\
CREATE TABLE g(id INTEGER PRIMARY KEY, b);\
CREATE INDEX gb ON g(b);\
INSERT INTO g VALUES (1, 3), (2, 1), (3, 2), (4, 9);\
CREATE TRIGGER g_bu BEFORE UPDATE ON g WHEN OLD.id = 3 BEGIN SELECT RAISE(IGNORE); END;\
CREATE TRIGGER g_au AFTER UPDATE ON g BEGIN INSERT INTO log(msg) VALUES ('g au ' || NEW.id); END;\
CREATE TABLE p(id INTEGER PRIMARY KEY, b);\
CREATE INDEX pb ON p(b);\
CREATE TABLE ch(pid REFERENCES p(id) ON UPDATE CASCADE ON DELETE CASCADE);\
INSERT INTO p VALUES (1, 3), (2, 1), (3, 2);\
INSERT INTO ch VALUES (1), (2), (3);\
CREATE TRIGGER p_au AFTER UPDATE ON p BEGIN INSERT INTO log(msg) VALUES ('p au ' || NEW.id); END;\
CREATE TRIGGER p_bd BEFORE DELETE ON p BEGIN INSERT INTO log(msg) VALUES ('p bd ' || OLD.id); END;\
CREATE TRIGGER ch_ad AFTER DELETE ON ch BEGIN INSERT INTO log(msg) VALUES ('ch ad ' || OLD.pid); END;";

const STATEMENTS: &[&str] = &[
    // Rowid table, WHERE answered by index tb: BEFORE + AFTER replay.
    "UPDATE t SET c = c + 1 WHERE b > 6",
    // The indexed column itself changes.
    "UPDATE t SET b = b + 1 WHERE b BETWEEN 6 AND 40",
    "UPDATE t SET c = c + 1 WHERE b IN (31, 11, 21)",
    // Batched AFTER-only DELETE triggers: rowid and hidden rowid.
    "DELETE FROM t WHERE b > 13",
    "DELETE FROM h WHERE b > 0",
    // WITHOUT ROWID, PRIMARY KEY DESC: UPDATE replay, BEFORE + AFTER DELETE replay.
    "UPDATE wd SET v = v + 10 WHERE v > 0",
    "DELETE FROM wd WHERE v > 0",
    // WITHOUT ROWID, (k NOCASE, j DESC): batched BEFORE-only DELETE.
    "DELETE FROM wn WHERE v > 0",
    // RAISE(IGNORE) skips one row of the replay.
    "UPDATE g SET b = b + 10 WHERE b > 0",
    // FK parent with cascades.
    "PRAGMA foreign_keys = ON",
    "UPDATE p SET b = b + 1 WHERE b > 0",
    "DELETE FROM p WHERE b > 0",
];

const DUMPS: &[&str] = &[
    "SELECT seq, msg FROM log ORDER BY seq",
    "SELECT a, b, c FROM t ORDER BY a",
    "SELECT rowid, b, c FROM h ORDER BY rowid",
    "SELECT k, v FROM wd ORDER BY k",
    "SELECT k, j, v FROM wn ORDER BY k, j",
    "SELECT id, b FROM g ORDER BY id",
    "SELECT id, b FROM p ORDER BY id",
    "SELECT pid FROM ch ORDER BY pid",
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
        assert_eq!(got, want, "bd-yb70u {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-yb70u {label} transcript length");
}

#[test]
fn dml_trigger_row_order_matches_stock_in_memory() {
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn dml_trigger_row_order_matches_stock_file_backed() {
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("yb70u.db");
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
