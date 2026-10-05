#![recursion_limit = "512"]

//! bd-f5s4w: two name-resolution gaps in trigger bodies and WHEN clauses.
//!
//! - The ORDER BY and LIMIT of a SELECT with its own FROM clause bound bare
//!   names to the trigger row, so `k` in `SELECT k FROM g ORDER BY
//!   abs(k - NEW.k)` became NEW.k: every sort key was equal (a WHEN guard
//!   missed rows) and a plain `ORDER BY ..., k` became a column position
//!   ("ORDER BY term out of range"). Only explicit NEW./OLD. references are
//!   the trigger row there, as in the same SELECT's WHERE.
//! - A non-TEMP trigger's unqualified table names resolved to a same-named
//!   TEMP table. Stock resolves them in the trigger's own schema (MAIN); a
//!   TEMP trigger still sees TEMP first.
//!
//! Every case is compared against stock SQLite (rusqlite).

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE g(k INTEGER, tag TEXT);\
INSERT INTO g VALUES (1, 'one'), (4, 'four'), (6, 'six'), (9, 'nine'), (15, 'fifteen');\
CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, n INTEGER);\
CREATE TABLE log(msg);\
CREATE TRIGGER t_when AFTER INSERT ON t \
    WHEN NEW.k IN (SELECT k FROM g ORDER BY abs(k - NEW.k) LIMIT NEW.n) BEGIN \
    INSERT INTO log VALUES ('when ' || NEW.id); END;\
CREATE TRIGGER t_exists AFTER INSERT ON t \
    WHEN EXISTS (SELECT 1 FROM (SELECT k FROM g ORDER BY abs(k - NEW.k), k LIMIT 1) WHERE k > NEW.k) BEGIN \
    INSERT INTO log VALUES ('exists ' || NEW.id); END;\
CREATE TRIGGER t_body AFTER INSERT ON t BEGIN \
    INSERT INTO log SELECT 'near ' || NEW.id || ' ' || group_concat(tag, ',') \
        FROM (SELECT tag FROM g ORDER BY abs(k - NEW.k), k LIMIT NEW.n); \
    INSERT INTO log SELECT 'ord ' || NEW.id || ' ' || group_concat(k, ',') \
        FROM (SELECT k FROM g ORDER BY k DESC LIMIT NEW.n OFFSET 1); \
    INSERT INTO log SELECT 'cmp ' || NEW.id || ' ' || group_concat(k, ',') \
        FROM (SELECT k FROM g WHERE k < NEW.k UNION SELECT k FROM g WHERE k > 8 ORDER BY k DESC); \
    INSERT INTO log SELECT 'new ' || NEW.id || ' ' || group_concat(x, ',') \
        FROM (SELECT NEW.k AS x UNION ALL SELECT NEW.n ORDER BY 1); \
END;\
CREATE TABLE m(id INTEGER PRIMARY KEY, v);\
CREATE TABLE cfg(k, v);\
INSERT INTO cfg VALUES ('mode', 'main');\
CREATE TABLE audit(msg);\
CREATE TRIGGER m_ai AFTER INSERT ON m \
    WHEN (SELECT v FROM cfg WHERE k = 'mode') LIKE 'main%' BEGIN \
    INSERT INTO audit VALUES ('main trigger ' || NEW.id || ' cfg=' || (SELECT v FROM cfg WHERE k = 'mode')); \
    UPDATE cfg SET v = v || '+' WHERE k = 'mode'; \
    DELETE FROM audit WHERE msg = 'stale'; \
END;";

const STATEMENTS: &[&str] = &[
    "INSERT INTO t VALUES (1, 5, 2)",
    "INSERT INTO t VALUES (2, 9, 1)",
    "INSERT INTO t VALUES (3, 14, 3)",
    "INSERT INTO t VALUES (4, 6, 1)",
    "INSERT INTO m VALUES (1, 'a')",
    "CREATE TEMP TABLE audit(msg)",
    "CREATE TEMP TABLE cfg(k, v)",
    "INSERT INTO temp.cfg VALUES ('mode', 'temp')",
    "INSERT INTO temp.audit VALUES ('stale')",
    "INSERT INTO main.audit VALUES ('stale')",
    "INSERT INTO m VALUES (2, 'b')",
    "CREATE TEMP TRIGGER tm_ai AFTER INSERT ON m BEGIN \
         INSERT INTO audit VALUES ('temp trigger ' || NEW.id || ' cfg=' || (SELECT v FROM cfg WHERE k = 'mode')); END",
    "INSERT INTO m VALUES (3, 'c')",
];

const DUMPS: &[&str] = &[
    "SELECT rowid, msg FROM log ORDER BY rowid",
    "SELECT 'main', msg FROM main.audit ORDER BY rowid",
    "SELECT 'temp', msg FROM temp.audit ORDER BY rowid",
    "SELECT 'main', v FROM main.cfg",
    "SELECT 'temp', v FROM temp.cfg",
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
    let mut transcript = Vec::new();
    for sql in STATEMENTS {
        let outcome = match conn.execute_batch(sql) {
            Ok(()) => "ok".to_owned(),
            Err(error) => format!("error:{error}"),
        };
        transcript.push(format!("{sql} => {outcome}"));
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
            Ok(_) => "ok".to_owned(),
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
        assert_eq!(got, want, "bd-f5s4w {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-f5s4w {label} transcript length");
}

#[test]
fn trigger_name_resolution_matches_stock_in_memory() {
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn trigger_name_resolution_matches_stock_file_backed() {
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("f5s4w.db");
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
