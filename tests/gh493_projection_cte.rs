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

#[test]
fn projection_cte_forests_public_reads_remain_pager_backed() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("forest.db");
        seed(&path);
        let stock = rusqlite::Connection::open_with_flags(
            &path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        ).unwrap();
        for sql in [
            "WITH c AS (SELECT id,v FROM a), d AS (SELECT id,v FROM b) SELECT c.id,c.v+d.v FROM c JOIN d USING(id) ORDER BY c.id",
            "WITH c AS (SELECT id,v FROM a WHERE v>=40), d AS (SELECT id,v FROM c WHERE v<45) SELECT id,v FROM d ORDER BY id",
            "WITH d(key,val) AS (SELECT p.id,p.v FROM c p WHERE p.v<3), c AS (SELECT id,v FROM a) SELECT key,val FROM d ORDER BY key",
            "WITH c AS (SELECT id,v FROM a), d AS (SELECT id,v FROM c WHERE v<3), e AS (SELECT id,v FROM b), f AS (SELECT id,v FROM e WHERE v<6) SELECT d.id,d.v+f.v FROM d JOIN f ON d.id=f.id ORDER BY d.id",
            "WITH c(key,val) AS (SELECT id,v FROM a), d(k,n) AS (SELECT p.key,p.val FROM c p WHERE p.val<3) SELECT q.k,q.n+b.v FROM d q JOIN b ON b.id=q.k ORDER BY q.k",
            "WITH c AS (SELECT id,v FROM a), d AS (SELECT id,v FROM c) SELECT id,v FROM d WHERE id='K042'",
            "WITH c AS (SELECT id,v FROM a WHERE v<3), d AS (SELECT id,v FROM b WHERE v<0) SELECT c.id,d.v FROM c LEFT JOIN d ON c.id=d.id ORDER BY c.id",
            "WITH c AS (SELECT id AS v,v AS id FROM a), d AS (SELECT id,v FROM c WHERE id<3) SELECT v AS id,id AS v FROM d ORDER BY v DESC",
            "WITH c AS (SELECT id,v FROM a WHERE v IN (1,2,3)), d AS (SELECT id,v FROM c WHERE v>1) SELECT id,v FROM d ORDER BY id",
            "WITH c AS NOT MATERIALIZED (SELECT id,v FROM a), d AS NOT MATERIALIZED (SELECT id,v FROM c WHERE v<4) SELECT DISTINCT id,v FROM d ORDER BY id DESC LIMIT 2 OFFSET 1",
            "WITH StageOne AS (SELECT id,v FROM a), StageTwo AS (SELECT p.id,p.v FROM sTaGeOnE p WHERE p.v<3) SELECT q.id,q.v FROM sTaGeTwO q ORDER BY q.id",
            "WITH c AS (SELECT id,v FROM a WHERE v<3), d AS (SELECT id,v FROM b WHERE v<6) SELECT * FROM c JOIN d USING(id) ORDER BY id",
        ] {
            let expected = stock_rows(&stock, sql);
            for prepared in [false, true] {
                let conn = Connection::open_existing_schema_only(
                    path.to_string_lossy().into_owned(),
                ).await.unwrap();
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
    });
}

#[test]
fn projection_cte_forests_preserve_prepared_rebinding_and_strict_refusal() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("forest-params.db");
        seed(&path);
        let sql = "WITH d(key,val) AS (SELECT p.id,p.v FROM c p WHERE p.v>=?2), \
            c AS (SELECT id,v FROM a WHERE v<?4) \
            SELECT ?1,q.key,q.val FROM d q WHERE q.key=?3";
        for prepared in [false, true] {
            let conn = Connection::open_existing_schema_only(
                path.to_string_lossy().into_owned(),
            ).await.unwrap();
            {
                let statement = if prepared { Some(conn.prepare(sql).await.unwrap()) } else { None };
                for (key, low, high, value) in [
                    ("k042", 40, 50, Some(42)),
                    ("k003", 0, 10, Some(3)),
                    ("k042", 43, 50, None),
                    ("k003", 0, 3, None),
                    ("K042", 40, 50, Some(42)),
                ] {
                    let params = [
                        SqliteValue::Integer(123), SqliteValue::Integer(low),
                        SqliteValue::Text(key.into()), SqliteValue::Integer(high),
                    ];
                    let rows = if let Some(statement) = &statement {
                        statement.query_with_params(&params).await.unwrap()
                    } else {
                        conn.query_with_params(sql, &params).await.unwrap()
                    };
                    let actual: Vec<_> = rows.iter().map(|row| row.values().to_vec()).collect();
                    let expected: Vec<_> = value.into_iter().map(|value| vec![
                        SqliteValue::Integer(123),
                        SqliteValue::Text(key.to_ascii_lowercase().into()),
                        SqliteValue::Integer(value),
                    ]).collect();
                    assert_eq!(actual, expected, "prepared={prepared}, {key}, [{low},{high})");
                    assert_eq!(conn.memdb_row_hydration_count(), 0);
                }
            }
            conn.set_reject_mem_fallback(true);
            conn.set_strict_mem_fallback_rejection(true);
            let error = conn.query(
                "WITH c AS (SELECT id FROM a), d AS (SELECT id FROM c) SELECT id FROM d",
            ).await.expect_err("strict mode still refuses the WITH materialization boundary");
            assert!(error.to_string().contains("with_clause_materialization"), "{error}");
            assert_eq!(conn.memdb_row_hydration_count(), 0);
            conn.set_strict_mem_fallback_rejection(false);
            conn.set_reject_mem_fallback(false);
            conn.close_without_checkpoint().await.unwrap();
        }
    });
}

#[test]
fn projection_cte_forests_keep_the_readers_wal_snapshot() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("forest-wal.db");
        seed(&path);
        let writer = rusqlite::Connection::open(&path).unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        let conn = Connection::open_existing_schema_only(
            path.to_string_lossy().into_owned(),
        ).await.unwrap();
        conn.execute("BEGIN DEFERRED").await.unwrap();
        let pinned = conn.query("SELECT v FROM a WHERE id='k042'").await.unwrap();
        assert_eq!(pinned[0].values(), &[SqliteValue::Integer(42)]);
        writer.execute("UPDATE a SET v=4242 WHERE id='k042'", []).unwrap();
        let sql = "WITH c AS (SELECT id,v FROM a), d AS (SELECT id,v FROM c) \
            SELECT v FROM d WHERE id='k042'";
        assert_eq!(stock_rows(&writer, sql), vec![vec![SqliteValue::Integer(4242)]]);
        for rows in [
            conn.query(sql).await.unwrap(),
            conn.prepare(sql).await.unwrap().query().await.unwrap(),
        ] {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(42)]);
        }
        assert_eq!(conn.memdb_row_hydration_count(), 0, "pinned CTE read");
        conn.execute("ROLLBACK").await.unwrap();
        let rows = conn.query(sql).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values(), &[SqliteValue::Integer(4242)]);
        assert_eq!(conn.memdb_row_hydration_count(), 0, "fresh CTE snapshot");
        conn.close_without_checkpoint().await.unwrap();
    });
}
