#![recursion_limit = "512"]
//! GH#516 differential semantics and an opt-in release throughput experiment.
//! Oracle and candidate always use DIFFERENT database files (including WAL).
//! The timed region excludes setup, commit, assertions, and tracing. The exact
//! same binary source is run before and after the engine source transformation.
use std::sync::{Arc, Mutex};
use std::time::Instant;

use fsqlite_core::connection::{Connection, TraceEvent, TraceMask};
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;
use rusqlite::types::ValueRef;
use serde::Deserialize;

#[derive(Deserialize)]
struct Scenario {
    name: String,
    setup: Vec<String>,
    steps: Vec<Step>,
}

#[derive(Deserialize)]
struct Step {
    sql: String,
    query: bool,
    ok: bool,
}

fn sqlite_value(value: ValueRef<'_>) -> SqliteValue {
    match value {
        ValueRef::Null => SqliteValue::Null,
        ValueRef::Integer(v) => SqliteValue::Integer(v),
        ValueRef::Real(v) => SqliteValue::Float(v),
        ValueRef::Text(v) => SqliteValue::Text(std::str::from_utf8(v).unwrap().into()),
        ValueRef::Blob(v) => SqliteValue::Blob(v.to_vec().into()),
    }
}

fn sqlite_rows(db: &rusqlite::Connection, sql: &str) -> rusqlite::Result<Vec<Vec<SqliteValue>>> {
    let mut statement = db.prepare(sql)?;
    let width = statement.column_count();
    let rows = statement.query_map([], |row| {
        (0..width).map(|i| row.get_ref(i).map(sqlite_value)).collect()
    })?;
    rows.collect()
}

fn assert_error_parity(native: &FrankenError, stock: &rusqlite::Error, label: &str) {
    let stock_code = stock.sqlite_error().unwrap_or_else(|| panic!("{label}: non-SQLite oracle error: {stock}"));
    assert_eq!(native.error_code() as i32, stock_code.extended_code & 0xff,
        "{label}: native={native}; sqlite={stock}");
}

async fn differential(wal: bool) {
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!("gh516_cases.json")).unwrap();
    for scenario in scenarios {
        let dir = tempfile::tempdir().unwrap();
        let path = if wal {
            dir.path().join("candidate.db").to_str().unwrap().to_owned()
        } else {
            ":memory:".to_owned()
        };
        let native = Connection::open(&path).await.unwrap();
        let stock = if wal {
            rusqlite::Connection::open(dir.path().join("oracle.db")).unwrap()
        } else {
            rusqlite::Connection::open_in_memory().unwrap()
        };
        if wal {
            native.execute("PRAGMA journal_mode=WAL").await.unwrap();
            stock.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        }
        for sql in &scenario.setup {
            stock.execute_batch(sql).unwrap_or_else(|e| panic!("{} oracle setup `{sql}`: {e}", scenario.name));
            native.execute(sql).await.unwrap_or_else(|e| panic!("{} native setup `{sql}`: {e}", scenario.name));
        }
        for (index, step) in scenario.steps.iter().enumerate() {
            let label = format!("{} wal={wal} step={index}: {}", scenario.name, step.sql);
            if step.query {
                let expected = sqlite_rows(&stock, &step.sql);
                let actual = native.query(&step.sql).await;
                assert_eq!(expected.is_ok(), step.ok, "invalid oracle fixture: {label}: {expected:?}");
                assert_eq!(actual.is_ok(), expected.is_ok(), "{label}: native={actual:?}; sqlite={expected:?}");
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) => {
                        let actual: Vec<Vec<SqliteValue>> = actual.iter().map(|r| r.values().to_vec()).collect();
                        let types = |rows: &[Vec<SqliteValue>]| rows.iter().map(|r| r.iter().map(SqliteValue::typeof_str).collect::<Vec<_>>()).collect::<Vec<_>>();
                        assert_eq!(types(&actual), types(&expected), "storage classes: {label}");
                        assert_eq!(actual, expected, "values/order: {label}");
                    }
                    (Err(actual), Err(expected)) => assert_error_parity(&actual, &expected, &label),
                    _ => unreachable!(),
                }
            } else {
                let expected = stock.execute(&step.sql, []);
                let actual = native.execute(&step.sql).await;
                assert_eq!(expected.is_ok(), step.ok, "invalid oracle fixture: {label}: {expected:?}");
                assert_eq!(actual.is_ok(), expected.is_ok(), "{label}: native={actual:?}; sqlite={expected:?}");
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) if ["INSERT", "UPDATE", "DELETE"].iter().any(|p| step.sql.starts_with(p)) => {
                        assert_eq!(actual, expected, "affected rows: {label}");
                    }
                    (Err(actual), Err(expected)) => assert_error_parity(&actual, &expected, &label),
                    _ => {}
                }
            }
        }
        let expected = sqlite_rows(&stock, "PRAGMA integrity_check").unwrap();
        let actual = native.query("PRAGMA integrity_check").await.unwrap();
        assert_eq!(actual.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>(), expected, "{} integrity", scenario.name);
        let mut persisted = Vec::new();
        if wal {
            for row in sqlite_rows(&stock, "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").unwrap() {
                let name = row[0].as_text_str().unwrap();
                let sql = format!("SELECT * FROM \"{}\" ORDER BY rowid", name.replace('"', "\"\""));
                persisted.push((sql.clone(), sqlite_rows(&stock, &sql).unwrap()));
            }
        }
        native.close().await.unwrap();
        drop(stock);
        // Persistence evidence is read only after clean close, never by opening
        // stock SQLite against a live FrankenSQLite WAL connection.
        if wal {
            let reopened = Connection::open(&path).await.unwrap();
            assert_eq!(reopened.query("PRAGMA integrity_check").await.unwrap()[0].values()[0], SqliteValue::Text("ok".into()));
            for (sql, expected) in persisted {
                let actual = reopened.query(&sql).await.unwrap();
                assert_eq!(actual.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>(), expected, "{} persisted {sql}", scenario.name);
            }
            reopened.close().await.unwrap();
        }
    }
}

#[test]
fn differential_memory() {
    asupersync::test_utils::run_test(|| async { differential(false).await; });
}

#[test]
fn differential_wal() {
    asupersync::test_utils::run_test(|| async { differential(true).await; });
}


#[test]
fn differential_parameters() {
    asupersync::test_utils::run_test(|| async {
        let native = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)",
            "CREATE TABLE audit(id INTEGER,v TEXT)",
            "CREATE TRIGGER ai AFTER INSERT ON t WHEN NEW.id>0 BEGIN INSERT INTO audit VALUES(NEW.id,NEW.v); END",
        ] {
            native.execute(sql).await.unwrap();
            stock.execute_batch(sql).unwrap();
        }
        let sql = "INSERT INTO t VALUES(?2,?1),(?4,?3),(?6,?5)";
        let prepared = native.prepare(sql).await.unwrap();
        for offset in [0, 100] {
            let parameters = [SqliteValue::Text("a".into()),SqliteValue::Integer(offset+11),
                SqliteValue::Text("b".into()),SqliteValue::Integer(offset+22),
                SqliteValue::Text("c".into()),SqliteValue::Integer(offset+33)];
            assert_eq!(prepared.execute_with_params(&parameters).await.unwrap(), 3);
            stock.execute(sql,rusqlite::params!["a",offset+11,"b",offset+22,"c",offset+33]).unwrap();
        }
        drop(prepared);
        for sql in ["SELECT * FROM t ORDER BY id","SELECT * FROM audit ORDER BY rowid","SELECT changes(),total_changes(),last_insert_rowid()"] {
            let actual=native.query(sql).await.unwrap();
            assert_eq!(actual.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>(),sqlite_rows(&stock,sql).unwrap(),"bound parameters: {sql}");
        }
        native.close().await.unwrap();
    });
}

#[test]
fn compiled_batch_has_one_outer_statement() {
    asupersync::test_utils::run_test(|| async {
        for timing in ["BEFORE", "AFTER"] {
            let db = Connection::open(":memory:").await.unwrap();
            db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)").await.unwrap();
            db.execute("CREATE TABLE audit(v INTEGER)").await.unwrap();
            db.execute(&format!("CREATE TRIGGER tr {timing} INSERT ON t WHEN NEW.v<0 BEGIN INSERT INTO audit VALUES(NEW.v); END")).await.unwrap();
            let events = Arc::new(Mutex::new(Vec::new()));
            let captured = Arc::clone(&events);
            db.trace_v2(TraceMask::STMT, Some(Arc::new(move |event| {
                if let TraceEvent::Statement { sql } = event {
                    captured.lock().unwrap().push(sql);
                }
            })));
            let changed = db.execute("INSERT INTO t(v) VALUES(1),(2),(3),(4),(5),(6),(7),(8)").await.unwrap();
            db.trace_v2(TraceMask::STMT, None);
            assert_eq!(changed, 8);
            let events = events.lock().unwrap().clone();
            assert_eq!(events.len(), 1, "{timing}: replayed complete SQL statements: {events:?}");
            assert_eq!(db.query("SELECT count(*) FROM audit").await.unwrap()[0].values()[0], SqliteValue::Integer(0));
            assert_eq!(db.query("SELECT count(*) FROM t").await.unwrap()[0].values()[0], SqliteValue::Integer(8));
            db.close().await.unwrap();
        }
    });
}

async fn measure(wal: bool, batch: usize, kind: &str) -> Vec<f64> {
    let dir = tempfile::tempdir().unwrap();
    let path = if wal { dir.path().join("bench.db").to_str().unwrap().to_owned() } else { ":memory:".to_owned() };
    let db = Connection::open(&path).await.unwrap();
    if wal { db.execute("PRAGMA journal_mode=WAL").await.unwrap(); }
    db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)").await.unwrap();
    db.execute("CREATE TABLE audit(v INTEGER)").await.unwrap();
    if kind != "none" {
        let timing = if kind.starts_with("before") { "BEFORE" } else { "AFTER" };
        let predicate = if kind.ends_with("false") { "NEW.v<0" } else { "NEW.v>=0" };
        db.execute(&format!("CREATE TRIGGER tr {timing} INSERT ON t WHEN {predicate} BEGIN INSERT INTO audit VALUES(NEW.v); END")).await.unwrap();
    }
    let values = (1..=batch).map(|v| format!("({v})")).collect::<Vec<_>>().join(",");
    let sql = format!("INSERT INTO t(v) VALUES{values}");
    let rows = 4096;
    let batches = rows / batch;
    let mut samples = Vec::new();
    for round in 0..9 {
        db.execute("BEGIN").await.unwrap();
        let start = Instant::now();
        for _ in 0..batches { db.execute(&sql).await.unwrap(); }
        let elapsed = start.elapsed().as_nanos() as f64 / (batches * batch) as f64;
        // Outside the clock: an optimization that drops writes is not speed.
        let count = db.query("SELECT count(*) FROM t").await.unwrap();
        assert_eq!(count[0].values()[0], SqliteValue::Integer((batches * batch) as i64));
        let audit_count = db.query("SELECT count(*) FROM audit").await.unwrap();
        let expected_audit = if kind.ends_with("true") { (batches * batch) as i64 } else { 0 };
        assert_eq!(audit_count[0].values()[0], SqliteValue::Integer(expected_audit));
        db.execute("ROLLBACK").await.unwrap();
        if round >= 2 { samples.push(elapsed); }
    }
    db.close().await.unwrap();
    samples
}

#[test]
#[ignore = "release-only before/after experiment; invoked by GH516 native validation"]
fn release_throughput() {
    assert!(!cfg!(debug_assertions), "performance measurements require --release");
    asupersync::test_utils::run_test(|| async {
        for wal in [false, true] {
            for batch in [8, 64, 256] {
                for kind in ["none", "before_false", "after_false", "after_true"] {
                    let samples = measure(wal, batch, kind).await;
                    println!("GH516_BENCH {}", serde_json::json!({"wal":wal,"batch":batch,"kind":kind,"ns_per_row":samples}));
                }
            }
        }
    });
}
