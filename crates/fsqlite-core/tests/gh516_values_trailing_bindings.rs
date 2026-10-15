#![recursion_limit = "512"]
//! GH#516: original UPSERT/RETURNING bind slots must survive VALUES replay.
//! Each insert and side effect is compared to an independent stock SQLite DB.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;
use rusqlite::types::{Value, ValueRef};

fn fvalue(value: &Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Integer(v) => SqliteValue::Integer(*v),
        Value::Real(v) => SqliteValue::Float(*v),
        Value::Text(v) => SqliteValue::Text(v.clone().into()),
        Value::Blob(v) => SqliteValue::Blob(v.clone().into()),
    }
}

fn stock_value(value: ValueRef<'_>) -> SqliteValue {
    match value {
        ValueRef::Null => SqliteValue::Null,
        ValueRef::Integer(v) => SqliteValue::Integer(v),
        ValueRef::Real(v) => SqliteValue::Float(v),
        ValueRef::Text(v) => SqliteValue::Text(std::str::from_utf8(v).unwrap().into()),
        ValueRef::Blob(v) => SqliteValue::Blob(v.to_vec().into()),
    }
}

fn stock_rows(db: &rusqlite::Connection, sql: &str, params: &[Value]) -> Vec<Vec<SqliteValue>> {
    let mut statement = db.prepare(sql).unwrap();
    let width = statement.column_count();
    statement.query_map(rusqlite::params_from_iter(params.iter()), |row| {
        (0..width).map(|i| row.get_ref(i).map(stock_value)).collect()
    }).unwrap().collect::<rusqlite::Result<_>>().unwrap()
}

fn rows(rows: &[Row]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(|row| row.values().to_vec()).collect()
}

async fn compare_state(native: &Connection, stock: &rusqlite::Connection, label: &str) {
    for sql in [
        "SELECT id,v FROM t ORDER BY id",
        "SELECT phase,id,v FROM audit ORDER BY rowid",
        "SELECT changes(),total_changes(),last_insert_rowid()",
    ] {
        assert_eq!(rows(&native.query(sql).await.unwrap()), stock_rows(stock, sql, &[]), "{label}: {sql}");
    }
}

async fn one_case(wal: bool, upsert: bool, original_slot: bool) {
    let dir = tempfile::tempdir().unwrap();
    let native_path = dir.path().join("native.db");
    let stock_path = dir.path().join("stock.db");
    let native = Connection::open(if wal { native_path.to_str().unwrap() } else { ":memory:" })
        .await.unwrap();
    let stock = if wal {
        rusqlite::Connection::open(&stock_path).unwrap()
    } else {
        rusqlite::Connection::open_in_memory().unwrap()
    };
    if wal {
        native.execute("PRAGMA journal_mode=WAL").await.unwrap();
        stock.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    }
    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT UNIQUE)",
        "CREATE TABLE audit(phase TEXT,id INTEGER,v TEXT)",
        "CREATE TRIGGER bi BEFORE INSERT ON t BEGIN INSERT INTO audit VALUES('B',NEW.id,NEW.v); END",
        "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit VALUES('A',NEW.id,NEW.v); END",
        "CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO audit VALUES('U',NEW.id,NEW.v); END",
    ] {
        native.execute(sql).await.unwrap();
        stock.execute_batch(sql).unwrap();
    }
    if upsert {
        native.execute("INSERT INTO t VALUES(1,'old')").await.unwrap();
        stock.execute_batch("INSERT INTO t VALUES(1,'old')").unwrap();
    }
    let sql = match (upsert, original_slot) {
        (false, false) => "INSERT INTO t(id,v) VALUES(?2,?1),(?4,?3) RETURNING id,v,?5",
        (false, true) => "INSERT INTO t(id,v) VALUES(?2,?1),(?4,?3) RETURNING id,v,?3,?1",
        (true, false) => "INSERT INTO t(id,v) VALUES(?2,?1),(?4,?3) ON CONFLICT(id) DO UPDATE SET v=excluded.v || ?5 RETURNING id,v,?6",
        (true, true) => "INSERT INTO t(id,v) VALUES(?2,?1),(?4,?3) ON CONFLICT(id) DO UPDATE SET v=excluded.v || ?5 RETURNING id,v,?1,?3",
    };
    let prepared = native.prepare(sql).await.unwrap();
    for offset in [0_i64, 10] {
        let mut params = vec![
            Value::Text(format!("first-{offset}")),
            Value::Integer(if upsert { 1 } else { offset + 1 }),
            Value::Text(format!("second-{offset}")),
            Value::Integer(offset + 2),
        ];
        if upsert {
            params.push(Value::Text("+updated".to_owned()));
        }
        if !original_slot {
            params.push(Value::Text(format!("marker-{offset}")));
        }
        let bound: Vec<_> = params.iter().map(fvalue).collect();
        let expected = stock_rows(&stock, sql, &params);
        let actual = prepared.query_with_params(&bound).await.unwrap();
        assert_eq!(rows(&actual), expected,
            "wal={wal} upsert={upsert} original_slot={original_slot} offset={offset}");
        compare_state(&native, &stock, "after VALUES").await;
    }
    drop(prepared);
    native.close().await.unwrap();
    drop(stock);
    if wal {
        let reopened = rusqlite::Connection::open(&native_path).unwrap();
        let verdict: String = reopened.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
        assert_eq!(verdict, "ok");
    }
}

#[test]
fn values_original_trailing_bindings_memory() {
    asupersync::test_utils::run_test(|| async {
        for upsert in [false, true] {
            for original_slot in [false, true] {
                one_case(false, upsert, original_slot).await;
            }
        }
    });
}

#[test]
fn values_original_trailing_bindings_wal() {
    asupersync::test_utils::run_test(|| async {
        for upsert in [false, true] {
            for original_slot in [false, true] {
                one_case(true, upsert, original_slot).await;
            }
        }
    });
}
