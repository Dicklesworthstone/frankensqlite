//! GH493's transparent single-use read lane. General materialized/recursive
//! CTE regressions remain in gh493_schema_only_contract; do not replace them.
#![recursion_limit = "512"]
#![cfg(feature = "native")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::path::Path;

const BULK_ROWS: i64 = 4096;

fn seed(path: &Path) {
    let mut db = rusqlite::Connection::open(path).unwrap();
    db.execute_batch(
        "PRAGMA journal_mode=DELETE; \
         CREATE TABLE bulk(id INTEGER PRIMARY KEY, body TEXT NOT NULL); \
         CREATE TABLE a(id TEXT PRIMARY KEY COLLATE NOCASE, v INTEGER); \
         CREATE TABLE b(id TEXT PRIMARY KEY, v INTEGER);",
    ).unwrap();
    let tx = db.transaction().unwrap();
    {
        let body = "x".repeat(2048);
        let mut insert = tx.prepare("INSERT INTO bulk VALUES (?1,?2)").unwrap();
        for id in 0..BULK_ROWS {
            insert.execute(rusqlite::params![id, &body]).unwrap();
        }
    }
    for id in 0..100_i64 {
        let key = format!("k{id:03}");
        tx.execute("INSERT INTO a VALUES (?1,?2)", rusqlite::params![&key, id]).unwrap();
        tx.execute("INSERT INTO b VALUES (?1,?2)", rusqlite::params![&key, id * 2]).unwrap();
    }
    tx.commit().unwrap();
    let bytes = std::fs::metadata(path).unwrap().len();
    assert!((8 * 1024 * 1024..=24 * 1024 * 1024).contains(&bytes));
}

fn stock_rows(db: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut statement = db.prepare(sql).unwrap();
    let width = statement.column_count();
    statement.query_map([], |row| {
        (0..width).map(|column| {
            row.get::<_, rusqlite::types::Value>(column).map(|value| match value {
                rusqlite::types::Value::Null => SqliteValue::Null,
                rusqlite::types::Value::Integer(value) => SqliteValue::Integer(value),
                rusqlite::types::Value::Real(value) => SqliteValue::Float(value),
                rusqlite::types::Value::Text(value) => SqliteValue::Text(value.into()),
                rusqlite::types::Value::Blob(value) => SqliteValue::Blob(value.into()),
            })
        }).collect::<rusqlite::Result<Vec<_>>>()
    }).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap()
}

#[test]
fn projection_cte_public_reads_are_correct_and_never_hydrate_unrelated_rows() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("projection.db");
        seed(&path);
        let stock = rusqlite::Connection::open_with_flags(
            &path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        ).unwrap();
        for sql in [
            "WITH pick AS (SELECT id FROM a WHERE id='k042') SELECT a.v FROM a,pick WHERE a.id=pick.id",
            "WITH c AS (SELECT id,v FROM a) SELECT c.id,c.v+b.v FROM c JOIN b ON b.id=c.id ORDER BY c.id",
            "WITH c(key,val) AS (SELECT id,v FROM a WHERE v<3) SELECT p.key,p.val+b.v FROM c p JOIN b ON b.id=p.key ORDER BY p.key",
            "WITH c AS (SELECT id,v FROM a WHERE v<3) SELECT c.* FROM c ORDER BY c.id",
            "WITH c AS (SELECT id FROM a WHERE v<3) SELECT b.id,c.id FROM b LEFT JOIN c ON b.id=c.id WHERE b.v<10 ORDER BY b.id",
            "WITH c AS NOT MATERIALIZED (SELECT id,v FROM a WHERE v BETWEEN 2 AND 5) SELECT id,v FROM c ORDER BY id DESC LIMIT 2 OFFSET 1",
            "WITH c AS (SELECT id FROM a WHERE v IN (1,2,3)) SELECT DISTINCT id FROM c ORDER BY id",
            "WITH c AS (SELECT id FROM a) SELECT id FROM c WHERE id='K042'",
            "WITH c AS (SELECT id AS key,v AS val FROM a WHERE v<2) SELECT key,val FROM c ORDER BY key",
            "WITH c AS (SELECT main.a.id,main.a.v FROM main.a WHERE main.a.v<3) SELECT c.id,main.b.v FROM c JOIN main.b ON c.id=main.b.id ORDER BY c.id",
        ] {
            let expected = stock_rows(&stock, sql);
            // Each shape and API starts on a fresh connection. A prior query
            // cannot mask a hydration or an accidentally populated mirror.
            for prepared in [false, true] {
                let conn = Connection::open_existing_schema_only(path.to_string_lossy().into_owned()).await.unwrap();
                assert_eq!(conn.memdb_row_hydration_count(), 0);
                let rows = if prepared {
                    conn.prepare(sql).await.unwrap().query().await.unwrap()
                } else {
                    conn.query(sql).await.unwrap()
                };
                let actual: Vec<_> = rows.iter().map(|row| row.values().to_vec()).collect();
                assert_eq!(actual, expected, "prepared={prepared}: {sql}");
                assert_eq!(conn.memdb_row_hydration_count(), 0, "prepared={prepared}: {sql}");
                conn.close_without_checkpoint().await.unwrap();
            }
        }
        drop(stock);
        let conn = Connection::open_existing_schema_only(path.to_string_lossy().into_owned()).await.unwrap();
        let sql = "WITH c(key,val) AS (SELECT id,v FROM a WHERE v>=?2) SELECT ?1,p.key,p.val FROM c p WHERE p.key=?3";
        let params = [SqliteValue::Integer(123), SqliteValue::Integer(40), SqliteValue::Text("k042".into())];
        let expected = vec![SqliteValue::Integer(123), SqliteValue::Text("k042".into()), SqliteValue::Integer(42)];
        for rows in [
            conn.query_with_params(sql, &params).await.unwrap(),
            conn.prepare(sql).await.unwrap().query_with_params(&params).await.unwrap(),
        ] {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values(), expected.as_slice());
        }
        assert_eq!(conn.memdb_row_hydration_count(), 0, "numbered parameter read");
        // The new lane does not alter strict materialization refusal.
        conn.set_reject_mem_fallback(true);
        conn.set_strict_mem_fallback_rejection(true);
        let error = conn.query("WITH c AS (SELECT id FROM a) SELECT id FROM c").await.unwrap_err();
        assert!(error.to_string().contains("with_clause_materialization"), "{error}");
        assert_eq!(conn.memdb_row_hydration_count(), 0, "strict refusal");
        conn.set_strict_mem_fallback_rejection(false);
        conn.set_reject_mem_fallback(false);
        conn.close_without_checkpoint().await.unwrap();
    });
}
