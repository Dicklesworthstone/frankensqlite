//! GH493: schema-only WITH statements must not hydrate persistent table rows.
//!
//! These are ordinary Cargo integration tests, not the ignored performance
//! profile and not an artifact-only candidate. The unresolved implementation
//! is expected to fail their zero-hydration assertions. Keep the assertions
//! intact when fixing execution; a fast query is not evidence of bounded IO.
//! Stock SQLite supplies the row-semantics oracle, never the hydration oracle.

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

/// bd-yk032: an ORDINARY connection on a small (<8 MiB, where GH#492 does NOT
/// defer MemDB hydration) file-backed database must NOT re-hydrate unrelated
/// persistent tables when executing WITH statements that read persistent tables,
/// views, WITHOUT ROWID tables, or recursive joins — across both DELETE and WAL
/// journal modes, autocommit and explicit transactions/savepoints, SELECT and
/// DML, direct and prepared execution.
#[test]
fn bd_yk032_ordinary_connection_with_persistent_reads_never_hydrates_unrelated_tables() {
    asupersync::test_utils::run_test(|| async {
        for journal_mode in ["DELETE", "WAL"] {
            let dir = tempfile::tempdir().unwrap();
            let frank_path = dir.path().join(format!("yk032-frank-{journal_mode}.db"));
            let stock_path = dir.path().join(format!("yk032-stock-{journal_mode}.db"));
            let setup = "
                CREATE TABLE unrelated(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
                CREATE VIEW tv AS SELECT id, v FROM t;
                CREATE TABLE wr(k TEXT PRIMARY KEY, v INTEGER NOT NULL) WITHOUT ROWID;
                INSERT INTO wr VALUES ('a', 10), ('b', 20), ('c', 30);
            ";

            let frank = Connection::open(frank_path.to_string_lossy().as_ref())
                .await
                .unwrap();
            let stock = rusqlite::Connection::open(&stock_path).unwrap();
            frank
                .execute(&format!("PRAGMA journal_mode={journal_mode}"))
                .await
                .unwrap();
            stock
                .execute_batch(&format!("PRAGMA journal_mode={journal_mode};"))
                .unwrap();
            frank.execute_batch(setup).await.unwrap();
            stock.execute_batch(setup).unwrap();

            // Seed 1,500 rows in `unrelated` (~250 KiB, well under GH#492's
            // 8 MiB hydration deferral cap so `defer_memdb_row_hydration()` is
            // guaranteed false on this ordinary connection).
            let seed_unrelated = "WITH RECURSIVE u(i) AS ( \
                SELECT 1 UNION ALL SELECT i + 1 FROM u WHERE i < 1500 \
            ) INSERT INTO unrelated(id, payload) SELECT i, 'payload_' || i FROM u";
            frank.execute(seed_unrelated).await.unwrap();
            stock.execute(seed_unrelated, []).unwrap();
            let db_bytes = std::fs::metadata(&frank_path).unwrap().len();
            assert!(
                db_bytes < 4 * 1024 * 1024,
                "{journal_mode}: fixture must stay under GH#492 8 MiB cap (got {db_bytes} bytes)"
            );
            assert_eq!(
                frank.memdb_row_hydration_count(),
                0,
                "{journal_mode}: CTE-only seed should not hydrate"
            );

            // Seed initial rows in `t`, leaving `memdb_requires_active_txn_reload`
            // set on an ordinary connection.
            let cte_insert = "WITH RECURSIVE s(i) AS ( \
                SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 20 \
            ) INSERT INTO t(v) SELECT i FROM s";
            frank.execute(cte_insert).await.unwrap();
            stock.execute(cte_insert, []).unwrap();

            let read_queries = [
                "WITH c(k) AS (SELECT 1) SELECT count(*), sum(t.v) FROM t, c",
                "WITH c(k) AS (SELECT 2) SELECT count(*) * max(k), sum(tv.v) FROM tv, c",
                "WITH RECURSIVE r(n) AS ( \
                     SELECT 1 UNION ALL \
                     SELECT n + 1 FROM r, t WHERE t.id = r.n AND n < 10 \
                 ) SELECT count(*), max(n) FROM r",
                "WITH c(k) AS (SELECT 'b') SELECT wr.k, wr.v FROM wr JOIN c ON wr.k = c.k",
            ];

            for sql in read_queries {
                let expected = stock_query(&stock_path, sql);
                let direct = query(&frank, sql).await;
                assert_eq!(direct, expected, "{journal_mode} direct: {sql}");
                let prepared: Vec<Vec<SqliteValue>> = frank
                    .prepare(sql)
                    .await
                    .unwrap()
                    .query()
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().to_vec())
                    .collect();
                assert_eq!(prepared, expected, "{journal_mode} prepared: {sql}");
            }
            assert_eq!(
                frank.memdb_row_hydration_count(),
                0,
                "{journal_mode}: WITH reads over persistent tables/views/WITHOUT ROWID must hydrate 0 rows"
            );

            // Explicit transaction + uncommitted writes + savepoint rollback +
            // WITH UPDATE / WITH DELETE / WITH INSERT reading persistent `t`.
            frank.execute("BEGIN IMMEDIATE").await.unwrap();
            stock.execute("BEGIN IMMEDIATE", []).unwrap();
            for dml in [
                "WITH c AS (SELECT id FROM t WHERE id <= 5) \
                 UPDATE t SET v = v + 100 WHERE id IN (SELECT id FROM c)",
                "WITH c AS (SELECT id FROM t WHERE id = 1) \
                 DELETE FROM t WHERE id IN (SELECT id FROM c)",
                "WITH c AS (SELECT v FROM t WHERE id <= 3) \
                 INSERT INTO t(v) SELECT v + 1000 FROM c",
            ] {
                frank.execute(dml).await.unwrap();
                stock.execute(dml, []).unwrap();
            }
            frank.execute("SAVEPOINT sp").await.unwrap();
            stock.execute("SAVEPOINT sp", []).unwrap();
            frank
                .execute("INSERT INTO t(v) VALUES (9999)")
                .await
                .unwrap();
            stock
                .execute("INSERT INTO t(v) VALUES (9999)", [])
                .unwrap();
            let check_in_sp = "WITH c(k) AS (SELECT 1) SELECT count(*), sum(t.v) FROM t, c";
            let sp_actual = query(&frank, check_in_sp).await;
            let sp_expected: Vec<Vec<SqliteValue>> = {
                let mut stmt = stock.prepare(check_in_sp).unwrap();
                stmt.query_map([], |row| {
                    Ok(vec![
                        SqliteValue::Integer(row.get(0)?),
                        SqliteValue::Integer(row.get(1)?),
                    ])
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
            };
            assert_eq!(sp_actual, sp_expected, "{journal_mode} in-savepoint WITH");

            frank.execute("ROLLBACK TO sp").await.unwrap();
            stock.execute("ROLLBACK TO sp", []).unwrap();
            frank.execute("RELEASE sp").await.unwrap();
            stock.execute("RELEASE sp", []).unwrap();
            frank.execute("COMMIT").await.unwrap();
            stock.execute("COMMIT", []).unwrap();

            for sql in read_queries {
                assert_eq!(
                    query(&frank, sql).await,
                    stock_query(&stock_path, sql),
                    "{journal_mode} post-commit: {sql}"
                );
            }
            assert_eq!(
                frank.memdb_row_hydration_count(),
                0,
                "{journal_mode}: full mixed WITH SELECT/INSERT/UPDATE/DELETE + savepoint sequence must hydrate 0 persistent rows"
            );
            frank.close().await.unwrap();
        }
    });
}

/// bd-yk032 gate: mixed workload (CTE insert + WITH read of the same table +
/// mixed persistent/working-table recursive join) at N=20/40/80 alongside an
/// unrelated table on an ordinary <8 MiB file-backed connection. Must hydrate
/// 0 MemDatabase rows at every scale and match live stock rusqlite 3.54.1.
#[test]
fn bd_yk032_mixed_cte_insert_and_with_read_scaling_n20_n40_n80() {
    use std::time::Instant;

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let mut report = Vec::new();
        for n in [20_usize, 40, 80] {
            let frank_path = dir.path().join(format!("yk032-scale-frank-{n}.db"));
            let stock_path = dir.path().join(format!("yk032-scale-stock-{n}.db"));
            let frank = Connection::open(frank_path.to_string_lossy().as_ref())
                .await
                .unwrap();
            let stock = rusqlite::Connection::open(&stock_path).unwrap();
            let schema = "
                PRAGMA journal_mode=WAL;
                CREATE TABLE unrelated(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
                CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);
            ";
            frank.execute_batch(schema).await.unwrap();
            stock.execute_batch(schema).unwrap();

            let seed_unrelated = "WITH RECURSIVE u(i) AS ( \
                SELECT 1 UNION ALL SELECT i + 1 FROM u WHERE i < 1500 \
            ) INSERT INTO unrelated(id, body) SELECT i, 'unrelated_row_' || i FROM u";
            frank.execute(seed_unrelated).await.unwrap();
            stock.execute(seed_unrelated, []).unwrap();

            let insert_sql = "WITH RECURSIVE s(i) AS ( \
                SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 10 \
            ) INSERT INTO t(v) SELECT i FROM s";
            let read_join_sql = "WITH c(k) AS (SELECT 2) SELECT count(*) * max(k) FROM t, c";
            let read_rec_sql = "WITH RECURSIVE r(n) AS ( \
                SELECT 1 UNION ALL \
                SELECT n + 1 FROM r, t WHERE t.id = r.n AND n < 5 \
            ) SELECT count(*) FROM r";

            let hydrated_before = frank.memdb_row_hydration_count();
            let f_start = Instant::now();
            for step in 1..=n {
                frank.execute(insert_sql).await.unwrap();
                let count_rows = query(&frank, read_join_sql).await;
                let expected_double = i64::try_from(step).unwrap() * 10 * 2;
                assert_eq!(
                    count_rows,
                    vec![vec![SqliteValue::Integer(expected_double)]],
                    "N={n} step={step}: join count"
                );
                let rec_rows = query(&frank, read_rec_sql).await;
                assert_eq!(
                    rec_rows,
                    vec![vec![SqliteValue::Integer(5)]],
                    "N={n} step={step}: recursive join count"
                );
            }
            let f_ms = f_start.elapsed().as_secs_f64() * 1000.0;
            let hydrated_delta = frank.memdb_row_hydration_count() - hydrated_before;
            assert_eq!(
                hydrated_delta, 0,
                "N={n}: mixed CTE insert + WITH persistent read hydrated {hydrated_delta} rows (expected 0)"
            );

            let s_start = Instant::now();
            for _ in 1..=n {
                stock.execute(insert_sql, []).unwrap();
                let _: i64 = stock
                    .query_row(read_join_sql, [], |row| row.get(0))
                    .unwrap();
                let _: i64 = stock
                    .query_row(read_rec_sql, [], |row| row.get(0))
                    .unwrap();
            }
            let s_ms = s_start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(
                query(&frank, "SELECT count(*), sum(v) FROM t").await,
                stock_query(&stock_path, "SELECT count(*), sum(v) FROM t"),
                "N={n}: final table state must match stock 3.54.1"
            );
            report.push(format!(
                "N={n}: frank={f_ms:.1}ms stock={s_ms:.1}ms hydrated={hydrated_delta}"
            ));
            frank.close().await.unwrap();
        }
        eprintln!("bd-yk032 scaling receipt: {}", report.join(" | "));
    });
}
