//! GH493 integration regressions for the pager-backed CTE candidate.
//!
//! This file is installed as a nonignored integration target by the candidate
//! patch. Merely retaining it under artifacts/gh493 does not run these tests.
//! Counter assertions cover work outside the VDBE and must not be replaced by
//! generous elapsed-time/RSS thresholds.

#![cfg(feature = "native")]
#![recursion_limit = "512"]

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const BULK_ROWS: i64 = 4096;
const CTE: &str = "WITH c AS (SELECT id, v FROM a WHERE id = 'k042') \
    SELECT c.id, c.v + b.v FROM c JOIN b ON b.id = c.id";
const PARAM_CTE: &str = "WITH c AS (SELECT id, v FROM a WHERE id = ?1) \
    SELECT c.id, c.v + b.v + ?2 FROM c JOIN b ON b.id = c.id";

fn seed(path: &Path) {
    let mut db = rusqlite::Connection::open(path).unwrap();
    db.execute_batch(
        "PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; \
         CREATE TABLE bulk(id INTEGER PRIMARY KEY, body TEXT NOT NULL); \
         CREATE TABLE a(id TEXT PRIMARY KEY, v INTEGER NOT NULL); \
         CREATE TABLE b(id TEXT PRIMARY KEY, v INTEGER NOT NULL); \
         CREATE TABLE nodes(id INTEGER PRIMARY KEY, parent INTEGER, label TEXT); \
         INSERT INTO nodes VALUES (1,NULL,'root'),(2,1,'branch'),(3,2,'left'),(4,2,'right');",
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

fn text(value: &str) -> SqliteValue {
    SqliteValue::Text(value.to_owned().into())
}

fn one(key: &str, value: i64) -> Vec<Vec<SqliteValue>> {
    vec![vec![text(key), SqliteValue::Integer(value)]]
}

fn assert_bounded(conn: &Connection, label: &str) {
    assert_eq!(conn.memdb_row_hydration_count(), 0, "{label}: persistent row hydration");
}

async fn query(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql).await.unwrap_or_else(|error| panic!("{sql}: {error}"))
        .iter().map(|row| row.values().to_vec()).collect()
}

fn stock_query(path: &Path, sql: &str) -> Vec<Vec<SqliteValue>> {
    use rusqlite::types::ValueRef;
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width).map(|index| {
            Ok(match row.get_ref(index)? {
                ValueRef::Null => SqliteValue::Null,
                ValueRef::Integer(value) => SqliteValue::Integer(value),
                ValueRef::Real(value) => SqliteValue::Float(value),
                ValueRef::Text(value) => text(std::str::from_utf8(value).unwrap()),
                ValueRef::Blob(value) => SqliteValue::Blob(value.to_vec().into()),
            })
        }).collect::<rusqlite::Result<Vec<_>>>()
    }).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap()
}

#[test]
fn schema_only_cte_direct_and_prepared_apis_stay_bounded() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("apis.db");
        seed(&path);
        for api in ["query", "query_row", "params", "prepared"] {
            let conn = Connection::open_existing_schema_only(path.to_string_lossy().as_ref()).await.unwrap();
            assert_bounded(&conn, "open");
            let params = [text("k042"), SqliteValue::Integer(7)];
            match api {
                "query" => assert_eq!(query(&conn, CTE).await, one("k042", 126)),
                "query_row" => assert_eq!(conn.query_row(CTE).await.unwrap().values(), &one("k042", 126)[0]),
                "params" => {
                    let rows = conn.query_with_params(PARAM_CTE, &params).await.unwrap();
                    assert_eq!(rows[0].values(), &one("k042", 133)[0]);
                    assert_eq!(rows.len(), 1);
                }
                "prepared" => {
                    let stmt = conn.prepare(PARAM_CTE).await.unwrap();
                    assert_bounded(&conn, "prepare");
                    for id in [42, 7, 42] {
                        let key = format!("k{id:03}");
                        let rows = stmt.query_with_params(&[text(&key), SqliteValue::Integer(7)]).await.unwrap();
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].values(), &one(&key, id * 3 + 7)[0]);
                    }
                }
                _ => unreachable!(),
            }
            assert_bounded(&conn, api);
            conn.close_without_checkpoint().await.unwrap();
        }
    });
}

#[test]
fn schema_only_cte_shapes_match_stock_without_bulk_hydration() {
    const CASES: &[&str] = &[
        "WITH c AS MATERIALIZED (SELECT id,v FROM a WHERE v<3) SELECT c.id,c.v+b.v FROM c JOIN b USING(id) ORDER BY c.id",
        "WITH c AS NOT MATERIALIZED (SELECT id,v FROM a WHERE v<3) SELECT c.id,c.v+b.v FROM c JOIN b USING(id) ORDER BY c.id",
        "WITH second AS (SELECT id,v+1 AS v FROM first), first AS (SELECT id,v FROM a WHERE v<3) SELECT second.id,second.v+b.v FROM second JOIN b USING(id) ORDER BY second.id",
        "WITH c AS (WITH d AS (SELECT id,v FROM a WHERE v<3) SELECT * FROM d) SELECT c.id,c.v+b.v FROM c JOIN b USING(id) ORDER BY c.id",
        "WITH c AS (SELECT id,v FROM a WHERE v<0) SELECT b.id,c.v FROM b LEFT JOIN c USING(id) WHERE b.v<6 ORDER BY b.id",
        "WITH c(x) AS (SELECT v FROM a WHERE v<3 UNION SELECT v FROM a WHERE v<3) SELECT x FROM c ORDER BY x",
        "WITH c(x) AS (SELECT v FROM a WHERE v<3 UNION ALL SELECT v FROM a WHERE v<3) SELECT x FROM c ORDER BY x",
        "WITH c AS (SELECT id,v FROM a WHERE v<3) SELECT (SELECT sum(v) FROM c), count(*) FROM b WHERE id IN (SELECT id FROM c)",
        "WITH c AS (SELECT id,v FROM a WHERE v<3) SELECT id,sum(v) OVER (ORDER BY id) FROM c ORDER BY id",
        "WITH c AS (SELECT id,v FROM a WHERE v<6) SELECT v%2,sum(v) FROM c GROUP BY v%2 ORDER BY 1",
        "WITH c AS (SELECT a.id,a.v FROM a JOIN sqlite_master m ON m.name='a' WHERE m.type='table' AND a.v<3) SELECT c.id,c.v+b.v FROM c JOIN b USING(id) ORDER BY c.id",
        "WITH RECURSIVE r(id) AS (SELECT id FROM nodes WHERE id=1 UNION ALL SELECT n.id FROM nodes n JOIN r ON n.parent=r.id) SELECT r.id,n.label FROM r JOIN nodes n ON n.id=r.id ORDER BY r.id",
        "WITH RECURSIVE r(x) AS (SELECT 1 UNION SELECT CASE x WHEN 3 THEN 1 ELSE x+1 END FROM r JOIN nodes n ON n.id=r.x WHERE n.id<=3) SELECT x FROM r ORDER BY x",
        "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r) SELECT x FROM r LIMIT 5 OFFSET 2",
    ];
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shapes.db");
        seed(&path);
        // No stock handle remains open while FrankenSQLite owns the file.
        let expected: Vec<_> = CASES.iter().map(|sql| stock_query(&path, sql)).collect();
        for (sql, expected) in CASES.iter().zip(expected) {
            let conn = Connection::open_existing_schema_only(path.to_string_lossy().as_ref()).await.unwrap();
            assert_eq!(query(&conn, sql).await, expected, "{sql}");
            assert_bounded(&conn, sql);
            conn.close_without_checkpoint().await.unwrap();
        }
    });
}

#[test]
fn schema_only_cte_error_cleanup_preserves_temp_shadow_and_rowid_values() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cleanup.db");
        seed(&path);
        let conn = Connection::open_existing_schema_only(path.to_string_lossy().as_ref()).await.unwrap();
        conn.execute("CREATE TEMP TABLE c(x INTEGER)").await.unwrap();
        conn.execute("INSERT INTO temp.c VALUES (41)").await.unwrap();
        for _ in 0..3 {
            assert!(conn.query("WITH c(x) AS (SELECT v FROM a WHERE id='k042'), d(y) AS (SELECT abs(-9223372036854775808) FROM c) SELECT y FROM d").await.is_err());
            assert_eq!(query(&conn, "SELECT x FROM temp.c").await, vec![vec![SqliteValue::Integer(41)]]);
            assert_eq!(query(&conn, CTE).await, one("k042", 126));
            assert_bounded(&conn, "after failed WITH and reused CTE name");
        }
        // nodes has an IPK. Its name-keyed alias metadata must not replace the
        // CTE's computed id with a synthetic materialization rowid.
        let sql = "WITH nodes(id,parent,label) AS (SELECT id+100,parent,label FROM main.nodes WHERE id<3) SELECT c.id,b.v FROM nodes c JOIN b ON b.id='k000' ORDER BY c.id";
        assert_eq!(query(&conn, sql).await, vec![vec![SqliteValue::Integer(101), SqliteValue::Integer(0)], vec![SqliteValue::Integer(102), SqliteValue::Integer(0)]]);
        assert_bounded(&conn, "CTE IPK shadow");
        conn.close_without_checkpoint().await.unwrap();
    });
}

#[test]
fn schema_only_cte_uses_own_writes_and_savepoint_rollback() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("local.db");
        seed(&path);
        let conn = Connection::open_existing_schema_only(path.to_string_lossy().as_ref()).await.unwrap();
        conn.execute("BEGIN IMMEDIATE").await.unwrap();
        conn.execute("UPDATE a SET v=900 WHERE id='k042'").await.unwrap();
        assert_eq!(query(&conn, CTE).await, one("k042", 984));
        conn.execute("SAVEPOINT s").await.unwrap();
        conn.execute("UPDATE a SET v=901 WHERE id='k042'").await.unwrap();
        assert_eq!(query(&conn, CTE).await, one("k042", 985));
        conn.execute("ROLLBACK TO s").await.unwrap();
        assert_eq!(query(&conn, CTE).await, one("k042", 984));
        conn.execute("RELEASE s").await.unwrap();
        conn.execute("ROLLBACK").await.unwrap();
        assert_eq!(query(&conn, CTE).await, one("k042", 126));
        assert_bounded(&conn, "writes/savepoints/rollback");
        conn.close().await.unwrap();
        assert_eq!(stock_query(&path, "SELECT id,v FROM a WHERE id='k042'"), one("k042", 42));
    });
}

#[test]
fn schema_only_cte_keeps_pinned_wal_visibility() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("visibility.db");
        seed(&path);
        let writer = Connection::open_existing_schema_only(path.to_string_lossy().as_ref()).await.unwrap();
        writer.execute("PRAGMA journal_mode=WAL").await.unwrap();
        let reader = Connection::open_existing_schema_only(path.to_string_lossy().as_ref()).await.unwrap();
        reader.execute("BEGIN DEFERRED").await.unwrap();
        assert_eq!(query(&reader, CTE).await, one("k042", 126));
        writer.execute("UPDATE a SET v=77 WHERE id='k042'").await.unwrap();
        assert_eq!(query(&reader, CTE).await, one("k042", 126));
        reader.execute("ROLLBACK").await.unwrap();
        assert_eq!(query(&reader, CTE).await, one("k042", 161));
        assert_bounded(&reader, "pinned reader");
        assert_bounded(&writer, "writer");
        reader.close().await.unwrap();
        writer.close().await.unwrap();
    });
}

#[test]
fn attached_cte_dml_preserves_returning_upsert_and_savepoints() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("main.db");
        let aux = dir.path().join("aux.db");
        seed(&path);
        {
            let stock = rusqlite::Connection::open(&aux).unwrap();
            stock.execute_batch("CREATE TABLE sink(id TEXT PRIMARY KEY, v INTEGER NOT NULL)").unwrap();
        }
        let conn = Connection::open_existing_schema_only(path.to_string_lossy().as_ref()).await.unwrap();
        conn.execute(&format!("ATTACH DATABASE '{}' AS aux", aux.to_string_lossy().replace('\'', "''"))).await.unwrap();
        let insert = "WITH c AS (SELECT id,v FROM a WHERE id='k042') INSERT INTO aux.sink(id,v) SELECT id,v FROM c RETURNING id,v";
        assert_eq!(query(&conn, insert).await, one("k042", 42));
        let upsert = "WITH c AS (SELECT id,v+5 AS v FROM a WHERE id='k042') INSERT INTO aux.sink(id,v) SELECT id,v FROM c WHERE true ON CONFLICT(id) DO UPDATE SET v=excluded.v+1 RETURNING id,v";
        assert_eq!(query(&conn, upsert).await, one("k042", 48));
        let update = "WITH c AS (SELECT v FROM a WHERE id='k042') UPDATE aux.sink SET v=(SELECT v FROM c) WHERE id='k042' RETURNING id,v";
        assert_eq!(query(&conn, update).await, one("k042", 42));
        conn.execute("BEGIN").await.unwrap();
        conn.execute("SAVEPOINT s").await.unwrap();
        let delete = "WITH c AS (SELECT id FROM a WHERE id='k042') DELETE FROM aux.sink WHERE id IN (SELECT id FROM c) RETURNING id,v";
        assert_eq!(query(&conn, delete).await, one("k042", 42));
        assert!(query(&conn, "SELECT id,v FROM aux.sink").await.is_empty());
        conn.execute("ROLLBACK TO s").await.unwrap();
        conn.execute("RELEASE s").await.unwrap();
        assert_eq!(query(&conn, "SELECT id,v FROM aux.sink").await, one("k042", 42));
        let provisional = "WITH c AS (SELECT id,v FROM a WHERE id='k043') INSERT INTO aux.sink(id,v) SELECT id,v FROM c RETURNING id,v";
        assert_eq!(query(&conn, provisional).await, one("k043", 43));
        conn.execute("ROLLBACK").await.unwrap();
        // The unreferenced WITH must not shadow aux.sink or force a full child
        // reload. The child's dispatcher owns visibility of retained writes.
        assert_eq!(query(&conn, "WITH sink AS (SELECT id,v FROM a) SELECT id,v FROM aux.sink ORDER BY id").await, one("k042", 42));
        assert_bounded(&conn, "attached CTE DML parent");
        conn.close().await.unwrap();
        assert_eq!(stock_query(&aux, "SELECT id,v FROM sink ORDER BY id"), one("k042", 42));
        assert_eq!(stock_query(&path, "SELECT count(*) FROM bulk"), vec![vec![SqliteValue::Integer(BULK_ROWS)]]);
    });
}

#[test]
fn schema_only_cte_keeps_strict_fallback_denial() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("strict.db");
        seed(&path);
        let conn = Connection::open_existing_schema_only(path.to_string_lossy().as_ref()).await.unwrap();
        conn.set_reject_mem_fallback(true);
        conn.set_strict_mem_fallback_rejection(true);
        let error = conn.query(CTE).await.expect_err("strict certification still rejects CTE materialization");
        assert!(error.to_string().contains("with_clause_materialization"), "{error}");
        assert_bounded(&conn, "strict refusal");
        conn.set_strict_mem_fallback_rejection(false);
        assert_eq!(query(&conn, CTE).await, one("k042", 126));
        assert_bounded(&conn, "after strict refusal");
        conn.close_without_checkpoint().await.unwrap();
    });
}
