#![recursion_limit = "512"]
//! GH#516: RETURNING stays part of the outer triggered INSERT.
//!
//! The conformance cases run on the unchanged engine too. The explicitly
//! ignored dispatch probe is an acceptance test for the compiled-row candidate,
//! not a requirement that the unmodified main branch already has that candidate.
//! All SQLite oracle connections use separate databases from FrankenSQLite.

use std::sync::{Arc, Mutex};

use fsqlite_core::connection::{Connection, Row, TraceEvent, TraceMask};
use fsqlite_types::value::SqliteValue;
use rusqlite::types::ValueRef;
use serde::Deserialize;

fn stock_value(value: ValueRef<'_>) -> SqliteValue {
    match value {
        ValueRef::Null => SqliteValue::Null,
        ValueRef::Integer(value) => SqliteValue::Integer(value),
        ValueRef::Real(value) => SqliteValue::Float(value),
        ValueRef::Text(value) => {
            SqliteValue::Text(std::str::from_utf8(value).expect("UTF-8 fixture").into())
        }
        ValueRef::Blob(value) => SqliteValue::Blob(value.to_vec().into()),
    }
}

fn stock_rows(db: &rusqlite::Connection, sql: &str) -> rusqlite::Result<Vec<Vec<SqliteValue>>> {
    let mut statement = db.prepare(sql)?;
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width).map(|i| row.get_ref(i).map(stock_value)).collect()
        })?
        .collect()
}

fn assert_rows(actual: &[Row], expected: &[Vec<SqliteValue>], label: &str) {
    let actual: Vec<_> = actual.iter().map(|row| row.values().to_vec()).collect();
    let types = |rows: &[Vec<SqliteValue>]| {
        rows.iter()
            .map(|row| row.iter().map(SqliteValue::typeof_str).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    assert_eq!(types(&actual), types(expected), "storage classes: {label}");
    assert_eq!(actual, expected, "values and order: {label}");
}

#[derive(Deserialize)]
struct Case {
    name: String,
    setup: Vec<String>,
    insert: String,
    succeeds: bool,
    after: Vec<String>,
}

fn cases() -> Vec<Case> {
    serde_json::from_str(include_str!("gh516_returning_cases.json"))
        .expect("RETURNING conformance cases")
}

async fn snapshots(native: &Connection, stock: &rusqlite::Connection, label: &str) {
    for sql in [
        "SELECT changes(),total_changes(),last_insert_rowid()",
        "SELECT * FROM t ORDER BY id",
        "SELECT * FROM audit ORDER BY rowid",
    ] {
        assert_rows(
            &native.query(sql).await.expect("native snapshot"),
            &stock_rows(stock, sql).expect("stock snapshot"),
            &format!("{label}: {sql}"),
        );
    }
}

async fn differential_returning(wal: bool) {
    for case in cases() {
        let directory = tempfile::tempdir().expect("separate database directory");
        let path = if wal {
            directory.path().join("native.db").to_str().unwrap().to_owned()
        } else {
            ":memory:".to_owned()
        };
        let native = Connection::open(&path).await.expect("native open");
        let stock = if wal {
            rusqlite::Connection::open(directory.path().join("stock.db")).unwrap()
        } else {
            rusqlite::Connection::open_in_memory().unwrap()
        };
        if wal {
            native.execute("PRAGMA journal_mode=WAL").await.unwrap();
            stock.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        }
        for sql in &case.setup {
            stock.execute_batch(sql).unwrap_or_else(|e| panic!("{} stock setup: {sql}: {e}", case.name));
            native.execute(sql).await.unwrap_or_else(|e| panic!("{} native setup: {sql}: {e}", case.name));
        }
        let expected = stock_rows(&stock, &case.insert);
        let actual = native.query(&case.insert).await;
        assert_eq!(expected.is_ok(), case.succeeds, "invalid fixture {}: {expected:?}", case.name);
        assert_eq!(actual.is_ok(), expected.is_ok(), "{} wal={wal}: native={actual:?}, stock={expected:?}", case.name);
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => assert_rows(&actual, &expected, &case.name),
            (Err(actual), Err(expected)) => {
                if case.name.starts_with("raise/") {
                    let expected_msg = expected.to_string();
                    let actual_reason = match &actual {
                        fsqlite_error::FrankenError::FunctionError(msg)
                        | fsqlite_error::FrankenError::RaiseFail(msg)
                        | fsqlite_error::FrankenError::TransactionRolledBack { reason: msg } => {
                            msg.as_str()
                        }
                        other => panic!("{} unexpected raise error variant: {other:?}", case.name),
                    };
                    assert!(
                        expected_msg.ends_with(actual_reason),
                        "{} raise message mismatch: actual={actual:?}, expected={expected_msg}",
                        case.name
                    );
                } else {
                    let code = expected.sqlite_error().expect("SQLite constraint error");
                    assert_eq!(
                        actual.error_code() as i32,
                        code.extended_code & 0xff,
                        "{} error code",
                        case.name
                    );
                    assert_eq!(
                        actual.to_string(),
                        expected.to_string(),
                        "{} error message",
                        case.name
                    );
                }
            }
            _ => unreachable!(),
        }
        snapshots(&native, &stock, &case.name).await;
        for sql in &case.after {
            stock.execute_batch(sql).unwrap();
            native.execute(sql).await.unwrap();
            snapshots(&native, &stock, &format!("{} after {sql}", case.name)).await;
        }
        let contents = stock_rows(&stock, "SELECT * FROM t ORDER BY id").unwrap();
        let audit = stock_rows(&stock, "SELECT * FROM audit ORDER BY rowid").unwrap();
        native.close().await.unwrap();
        drop(stock);
        if wal {
            // Stock reads the actual candidate file only after native close.
            let persisted = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(stock_rows(&persisted, "SELECT * FROM t ORDER BY id").unwrap(), contents, "{} persisted t", case.name);
            assert_eq!(stock_rows(&persisted, "SELECT * FROM audit ORDER BY rowid").unwrap(), audit, "{} persisted audit", case.name);
            let integrity: String = persisted.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
            assert_eq!(integrity, "ok", "{} persisted integrity", case.name);
        }
    }
}

#[test]
fn returning_conformance_memory() {
    asupersync::test_utils::run_test(|| async { differential_returning(false).await; });
}

#[test]
fn returning_conformance_wal() {
    asupersync::test_utils::run_test(|| async { differential_returning(true).await; });
}

#[test]
fn returning_original_parameters_survive_repeated_preparation() {
    asupersync::test_utils::run_test(|| async {
        let native = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)",
            "CREATE TABLE audit(id INTEGER,v TEXT)",
            "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(NEW.id,NEW.v); END",
        ] {
            native.execute(sql).await.unwrap();
            stock.execute_batch(sql).unwrap();
        }
        // RETURNING's anonymous placeholder is slot 5 in the original SQL,
        // not a slot accidentally reused for the synthetic single-row VALUES.
        let sql = "INSERT INTO t VALUES(?2,?1),(?4,?3) RETURNING id,v,?";
        let prepared = native.prepare(sql).await.unwrap();
        for offset in [0, 100] {
            let parameters = [
                SqliteValue::Text("a".into()), SqliteValue::Integer(offset + 1),
                SqliteValue::Text("b".into()), SqliteValue::Integer(offset + 2),
                SqliteValue::Text(format!("marker-{offset}").into()),
            ];
            let actual = prepared.query_with_params(&parameters).await.unwrap();
            let mut statement = stock.prepare(sql).unwrap();
            let expected: Vec<Vec<SqliteValue>> = statement.query_map(
                rusqlite::params!["a", offset + 1, "b", offset + 2, format!("marker-{offset}")],
                |row| (0..3).map(|i| row.get_ref(i).map(stock_value)).collect(),
            ).unwrap().collect::<rusqlite::Result<_>>().unwrap();
            assert_rows(&actual, &expected, "original parameter numbering");
        }
        drop(prepared);
        snapshots(&native, &stock, "bound parameters").await;
        native.close().await.unwrap();
    });
}

#[test]
fn returning_compiled_candidate_avoids_row_redispatch() {
    asupersync::test_utils::run_test(|| async {
        for timing in ["BEFORE", "AFTER"] {
            for projection in ["*", "id,v,id+1,CASE WHEN v IS NULL THEN 'missing' ELSE v END"] {
                let native = Connection::open(":memory:").await.unwrap();
                native.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)").await.unwrap();
                native.execute("CREATE TABLE audit(v TEXT)").await.unwrap();
                native.execute(&format!("CREATE TRIGGER tr {timing} INSERT ON t WHEN NEW.v IS NULL BEGIN INSERT INTO audit VALUES(NEW.v); END")).await.unwrap();
                let events = Arc::new(Mutex::new(Vec::new()));
                let captured = Arc::clone(&events);
                native.trace_v2(TraceMask::STMT, Some(Arc::new(move |event| {
                    if let TraceEvent::Statement { sql } = event {
                        captured.lock().unwrap().push(sql);
                    }
                })));
                let sql = format!("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c') RETURNING {projection}");
                let rows = native.query(&sql).await.unwrap();
                native.trace_v2(TraceMask::STMT, None);
                assert_eq!(rows.len(), 3);
                assert_eq!(events.lock().unwrap().len(), 1, "{timing}: synthetic complete row INSERTs remain");
                assert_eq!(native.query("SELECT count(*) FROM audit").await.unwrap()[0].values()[0], SqliteValue::Integer(0));
                native.close().await.unwrap();
            }
        }
    });
}
