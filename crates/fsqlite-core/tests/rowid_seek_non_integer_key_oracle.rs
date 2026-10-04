#![recursion_limit = "512"]

//! A rowid range seek (`SeekGE`/`SeekGT`/`SeekLE`/`SeekLT` on a table cursor)
//! truncated its key with `to_integer()`. With a bound TEXT or REAL key that
//! returned wrong rows on file-backed connections: `a >= 'y'` (text sorts above
//! every integer) returned the whole table instead of nothing, and `a >= '2.5'`
//! or `a >= 2.5` (bound) included row 2. Stock's `OP_SeekGE` applies NUMERIC
//! affinity to a text key, treats a non-numeric text or blob key as above every
//! rowid, and rounds a non-integral REAL toward the seek direction by switching
//! the operator. rusqlite is the oracle, in memory and file-backed, unprepared
//! and prepared.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use rusqlite::types::Value;

fn from_stock(value: Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Integer(v) => SqliteValue::Integer(v),
        Value::Real(v) => SqliteValue::Float(v),
        Value::Text(v) => SqliteValue::from(v.as_str()),
        Value::Blob(v) => SqliteValue::Blob(v.into()),
    }
}

fn to_stock(value: &SqliteValue) -> Value {
    match value {
        SqliteValue::Null => Value::Null,
        SqliteValue::Integer(v) => Value::Integer(*v),
        SqliteValue::Float(v) => Value::Real(*v),
        SqliteValue::Text(v) => Value::Text(v.to_string()),
        SqliteValue::Blob(v) => Value::Blob(v.to_vec()),
    }
}

fn stock_rows(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn.prepare(sql).expect("stock prepare");
    let params: Vec<Value> = params.iter().map(to_stock).collect();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params.iter()))
        .expect("stock query");
    let mut out = Vec::new();
    while let Some(row) = rows.next().expect("stock next") {
        let width = row.as_ref().column_count();
        out.push(
            (0..width)
                .map(|i| from_stock(row.get::<_, Value>(i).expect("stock value")))
                .collect(),
        );
    }
    out
}

const SCHEMA: &str = "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT);\
                      INSERT INTO t VALUES (-3,'m'),(1,'x'),(2,'y'),(3,'z'),(10,'w');";

fn cases() -> Vec<(&'static str, SqliteValue)> {
    let text = |v: &str| SqliteValue::from(v);
    let mut cases = Vec::new();
    let keys = [
        text("y"),
        text("2.5"),
        text("-2.5"),
        text(" 2 "),
        text("3.0"),
        text("1e30"),
        text("-1e30"),
        SqliteValue::Float(2.5),
        SqliteValue::Float(-2.5),
        SqliteValue::Float(3.0),
        SqliteValue::Float(1e30),
        SqliteValue::Float(-1e30),
        SqliteValue::Float(9_223_372_036_854_775_808.0),
        SqliteValue::Float(-9_223_372_036_854_775_808.0),
        SqliteValue::Blob(vec![0x31].into()),
        SqliteValue::Integer(2),
    ];
    for key in keys {
        for sql in [
            "SELECT a FROM t WHERE a >= ?1 ORDER BY a",
            "SELECT a FROM t WHERE a > ?1 ORDER BY a",
            "SELECT a FROM t WHERE a <= ?1 ORDER BY a DESC",
            "SELECT a FROM t WHERE a < ?1 ORDER BY a DESC",
            "SELECT a FROM t WHERE a <= ?1 ORDER BY a",
            "SELECT a FROM t WHERE a > ?1 ORDER BY a DESC",
            "SELECT a FROM t WHERE a BETWEEN ?1 AND 10 ORDER BY a",
            "SELECT a FROM t WHERE a BETWEEN -5 AND ?1 ORDER BY a DESC",
            "SELECT count(*) FROM t WHERE rowid >= ?1",
        ] {
            cases.push((sql, key.clone()));
        }
    }
    cases
}

#[test]
fn rowid_range_seek_with_text_real_and_blob_keys_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [true, false] {
            let dir = tempfile::tempdir().expect("dir");
            let path = if file_backed {
                dir.path().join("t.db").to_string_lossy().into_owned()
            } else {
                ":memory:".to_owned()
            };
            let conn = Connection::open(&path).await.expect("open");
            conn.execute_batch(SCHEMA).await.expect("schema");
            let stock = rusqlite::Connection::open_in_memory().expect("stock");
            stock.execute_batch(SCHEMA).expect("stock schema");
            let mut mismatches = Vec::new();
            for (sql, key) in cases() {
                let params = [key];
                let expected = stock_rows(&stock, sql, &params);
                let ours = conn
                    .query_with_params(sql, &params)
                    .await
                    .map(|rows| rows.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>())
                    .map_err(|e| e.to_string());
                if ours.as_ref().ok() != Some(&expected) {
                    mismatches.push(format!(
                        "file={file_backed} {sql} {:?}: ours {ours:?}, stock {expected:?}",
                        params[0]
                    ));
                }
                let stmt = conn.prepare(sql).await.expect("prepare");
                for _ in 0..2 {
                    let ours = stmt
                        .query_with_params(&params)
                        .await
                        .map(|rows| rows.iter().map(|r| r.values().to_vec()).collect::<Vec<_>>())
                        .map_err(|e| e.to_string());
                    if ours.as_ref().ok() != Some(&expected) {
                        mismatches.push(format!(
                            "prepared file={file_backed} {sql} {:?}: ours {ours:?}, stock {expected:?}",
                            params[0]
                        ));
                    }
                }
            }
            assert!(
                mismatches.is_empty(),
                "{} mismatches:\n{}",
                mismatches.len(),
                mismatches.join("\n")
            );
        }
    });
}

#[test]
fn rowid_range_dml_with_text_key_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("d.db").to_string_lossy().into_owned();
        let conn = Connection::open(&path).await.expect("open");
        conn.execute_batch(SCHEMA).await.expect("schema");
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        stock.execute_batch(SCHEMA).expect("stock schema");
        for (sql, key) in [
            (
                "UPDATE t SET b = b || '!' WHERE a >= ?1",
                SqliteValue::from("y"),
            ),
            (
                "UPDATE t SET b = b || '?' WHERE a > ?1",
                SqliteValue::Float(2.5),
            ),
            ("DELETE FROM t WHERE a < ?1", SqliteValue::from("-2.5")),
        ] {
            conn.execute_with_params(sql, std::slice::from_ref(&key))
                .await
                .expect("ours dml");
            stock
                .execute(sql, rusqlite::params![to_stock(&key)])
                .expect("stock dml");
            let check = "SELECT a, b FROM t ORDER BY a";
            let ours: Vec<Vec<SqliteValue>> = conn
                .query(check)
                .await
                .expect("check")
                .iter()
                .map(|r| r.values().to_vec())
                .collect();
            assert_eq!(ours, stock_rows(&stock, check, &[]), "after {sql} {key:?}");
        }
    });
}
