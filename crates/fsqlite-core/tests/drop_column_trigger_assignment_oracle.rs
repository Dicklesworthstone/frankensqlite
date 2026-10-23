#![recursion_limit = "512"]

//! `ALTER TABLE t DROP COLUMN c` with a trigger that mentions `c`, against
//! stock SQLite (rusqlite).
//!
//! Stock re-resolves only a trigger's expressions after the drop
//! (`renameResolveTrigger`), so a step that merely assigns the column
//! (`UPDATE t SET c = ...`, `INSERT INTO t(c) ...`, an upsert's `SET c`) does
//! not block it; the trigger stays in the schema and fails when it fires. A
//! step that reads `c` in an expression does block it. Each case compares the
//! DROP's outcome, the schema rows left behind, and firing the trigger
//! afterwards, in memory and in a file stock then reads.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

/// (table the trigger is on, trigger step)
const CASES: &[(&str, &str)] = &[
    ("o", "UPDATE t SET c = 1"),
    ("o", "UPDATE t SET (a, c) = (1, 2)"),
    ("o", "INSERT INTO t(a, c) VALUES (1, 2)"),
    (
        "o",
        "INSERT INTO t(a, c) VALUES (1, 2) ON CONFLICT DO UPDATE SET c = 3",
    ),
    ("o", "INSERT INTO t(a, c) SELECT a, a FROM t"),
    ("t", "UPDATE t SET c = 1"),
    ("o", "UPDATE o SET x = 1 WHERE x = 0"),
    ("o", "UPDATE t SET a = c"),
    ("o", "UPDATE t SET c = 1 WHERE c > 0"),
    ("o", "INSERT INTO t(a) SELECT c FROM t"),
    ("o", "DELETE FROM t WHERE c = 1"),
    ("o", "INSERT INTO t(a, c) VALUES (1, 2) ON CONFLICT(c) DO NOTHING"),
    ("t", "SELECT new.c"),
];

fn script(on: &str, body: &str) -> Vec<String> {
    let fire = if on == "o" {
        "INSERT INTO o VALUES (5)"
    } else {
        "INSERT INTO t VALUES (7)"
    };
    vec![
        "CREATE TABLE t(a UNIQUE, c)".to_owned(),
        "CREATE TABLE o(x)".to_owned(),
        "INSERT INTO t VALUES (1, 1)".to_owned(),
        format!("CREATE TRIGGER tr AFTER INSERT ON {on} BEGIN {body}; END"),
        "ALTER TABLE t DROP COLUMN c".to_owned(),
        "SELECT type, name, sql FROM sqlite_schema WHERE name IN ('t', 'tr') ORDER BY name"
            .to_owned(),
        fire.to_owned(),
        "SELECT * FROM t ORDER BY 1".to_owned(),
    ]
}

/// Stock names the trigger row in the message (`new.c`); FrankenSQLite names
/// the column (`c`), a difference the README lists under Limitations.
fn normalize(line: String) -> String {
    line.replace("no such column: new.c", "no such column: c")
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
    let line = match run() {
        Ok(rows) => format!("{sql} => ok {rows:?}"),
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => {
            format!("{sql} => error:{message}")
        }
        Err(error) => format!("{sql} => error:{error}"),
    };
    normalize(line)
}

async fn fsqlite_query(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => format!("{sql} => ok {:?}", render_rows(&rows)),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

fn stock_transcript(on: &str, body: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch("PRAGMA legacy_alter_table = OFF")
        .expect("stock pragma");
    script(on, body)
        .iter()
        .map(|sql| stock_query(&conn, sql))
        .collect()
}

#[test]
fn drop_column_with_trigger_references_matches_stock() {
    let expected: Vec<Vec<String>> = CASES
        .iter()
        .map(|(on, body)| stock_transcript(on, body))
        .collect();
    // The oracle itself: the assigning steps let the DROP through, the
    // reading ones refuse it.
    let dropped = |transcript: &[String]| transcript[4].ends_with("=> ok []");
    assert_eq!(
        expected.iter().map(|t| dropped(t)).collect::<Vec<_>>(),
        [
            true, true, true, true, true, true, true, false, false, false, false, false, false
        ],
        "stock outcomes: {expected:#?}"
    );
    let dir = tempfile::tempdir().expect("temp dir");
    let dir_path = dir.path().to_owned();
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for (index, ((on, body), want)) in CASES.iter().zip(&expected).enumerate() {
            for file_backed in [false, true] {
                let path = if file_backed {
                    dir_path
                        .join(format!("case{index}.db"))
                        .to_str()
                        .expect("utf-8 path")
                        .to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.expect("fsqlite open");
                let mut got = Vec::new();
                for sql in script(on, body) {
                    got.push(fsqlite_query(&conn, &sql).await);
                }
                conn.close().await.expect("fsqlite close");
                if &got != want {
                    mismatches.push(format!(
                        "{on}: {body} (file_backed={file_backed})\n  fsqlite: {got:#?}\n  stock:   {want:#?}"
                    ));
                }
                if file_backed {
                    // Stock must read the schema FrankenSQLite left behind.
                    let stock = rusqlite::Connection::open(&path).expect("stock reopen");
                    let check = stock_query(&stock, "PRAGMA integrity_check");
                    let schema = stock_query(&stock, &script(on, body)[5]);
                    if check != "PRAGMA integrity_check => ok [\"t:ok\"]" || schema != want[5] {
                        mismatches.push(format!(
                            "{on}: {body}: stock reading the fsqlite file\n  {check}\n  {schema}\n  want {}",
                            want[5]
                        ));
                    }
                }
            }
        }
        for mismatch in &mismatches {
            eprintln!("event=drop_column_trigger_mismatch {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} mismatches; first: {}",
            mismatches.len(),
            mismatches[0]
        );
    });
}
