//! GH#503 at freelist-trunk scale, beyond the single-page index reproducer.
//!
//! Drop a BLOB index owning more pages than one freelist trunk can represent,
//! reuse those pages in a larger overflow chain, then allocate beyond EOF.
//! Rollback and savepoint rollback must not recycle the committed chain. The
//! existing uninterrupted six-statement reproduction remains a separate guard.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const PAGE_BYTES: usize = 4096;
const OVERFLOW_BYTES: usize = PAGE_BYTES - 4;
const SEED_ROWS: usize = 160;
const SEED_BYTES: usize = 32_768;
const VICTIM: i64 = 1_000_000;
const VICTIM_BYTES: usize = 8 * 1024 * 1024;
const CREATE_U: &str = "CREATE TABLE u(k TEXT PRIMARY KEY,v INTEGER)";

fn pattern(len: usize, key: i64) -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..len)
        .map(|offset| u8::try_from(offset % 251).expect("pattern byte"))
        .collect();
    for (index, page) in bytes.chunks_mut(OVERFLOW_BYTES).enumerate() {
        let tag = key
            .to_le_bytes()
            .into_iter()
            .chain(u64::try_from(index).expect("pattern page index").to_le_bytes());
        for (byte, value) in page.iter_mut().zip(tag) {
            *byte = value;
        }
    }
    bytes
}

fn assert_blob(actual: &[u8], expected: &[u8], key: i64, engine: &str) {
    assert_eq!(actual.len(), expected.len(), "{engine}: length at id={key}");
    let mismatch = actual.iter().zip(expected).position(|(a, b)| a != b);
    assert!(
        mismatch.is_none(),
        "{engine}: payload at id={key} differs at byte {mismatch:?}"
    );
}

async fn execute_pair(conn: &Connection, stock: &rusqlite::Connection, sql: &str) {
    stock.execute_batch(sql).expect(sql);
    conn.execute(sql).await.expect(sql);
}

async fn insert_pair(
    conn: &Connection,
    stock: &rusqlite::Connection,
    key: i64,
    blob: &[u8],
) {
    let sql = "INSERT INTO t(id,a,b) VALUES(?1,?2,?3)";
    assert_eq!(
        stock
            .execute(sql, rusqlite::params![key, key * 17, blob])
            .unwrap(),
        1
    );
    assert_eq!(
        conn.execute_with_params(
            sql,
            &[
                SqliteValue::Integer(key),
                SqliteValue::Integer(key * 17),
                SqliteValue::Blob(blob.into()),
            ],
        )
        .await
        .expect("insert patterned row"),
        1
    );
}

fn stock_int(stock: &rusqlite::Connection, sql: &str) -> i64 {
    stock.query_row(sql, [], |row| row.get(0)).expect(sql)
}

async fn our_int(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect(sql);
    assert_eq!(rows.len(), 1, "{sql}");
    match rows[0].values() {
        [SqliteValue::Integer(value)] => *value,
        other => panic!("expected integer for {sql}, got {other:?}"),
    }
}

const ROWS_SQL: &str = "SELECT id,a,b FROM t ORDER BY id";
const SCHEMA_SQL: &str = "SELECT type,name,tbl_name FROM sqlite_master ORDER BY name";

type StoredRow = (i64, i64, Vec<u8>);
type SchemaRow = (String, String, String);

fn checked_stock_rows(
    stock: &rusqlite::Connection,
    has_victim: bool,
    outer_committed: bool,
) -> Vec<StoredRow> {
    let rows = stock
        .prepare(ROWS_SQL)
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<StoredRow>>>()
        .unwrap();
    assert_eq!(rows.len(), SEED_ROWS + usize::from(has_victim));
    for (index, (key, value, blob)) in rows.iter().enumerate() {
        let expected_key = if index < SEED_ROWS {
            i64::try_from(index + 1).unwrap()
        } else {
            VICTIM
        };
        assert_eq!(*key, expected_key, "stock row identity");
        assert_eq!(*value, key * 17 + i64::from(outer_committed && *key == 1));
        let len = if *key == VICTIM {
            VICTIM_BYTES
        } else {
            SEED_BYTES
        };
        assert_blob(blob, &pattern(len, *key), *key, "stock");
    }
    let integrity = stock
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(integrity, vec!["ok".to_owned()], "stock integrity");
    rows
}

fn stock_schema(stock: &rusqlite::Connection) -> Vec<SchemaRow> {
    stock
        .prepare(SCHEMA_SQL)
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

async fn check(
    conn: &Connection,
    stock: &rusqlite::Connection,
    has_victim: bool,
    outer_committed: bool,
) {
    let expected = checked_stock_rows(stock, has_victim, outer_committed);
    let actual = conn.query(ROWS_SQL).await.unwrap();
    assert_eq!(actual.len(), expected.len(), "FrankenSQLite row count");
    for (row, (key, value, blob)) in actual.iter().zip(&expected) {
        match row.values() {
            [SqliteValue::Integer(id), SqliteValue::Integer(a), SqliteValue::Blob(b)] => {
                assert_eq!((*id, *a), (*key, *value));
                assert_blob(b, blob, *key, "FrankenSQLite");
            }
            _ => panic!("unexpected row types at id={key}"),
        }
    }
    let schema = conn.query(SCHEMA_SQL).await.unwrap();
    let schema: Vec<SchemaRow> = schema
        .iter()
        .map(|row| match row.values() {
            [SqliteValue::Text(kind), SqliteValue::Text(name), SqliteValue::Text(table)] => {
                (kind.to_string(), name.to_string(), table.to_string())
            }
            _ => panic!("unexpected schema row types"),
        })
        .collect();
    assert_eq!(
        schema,
        stock_schema(stock),
        "schema after allocation/rollback"
    );
    let integrity = conn.query("PRAGMA integrity_check").await.unwrap();
    assert_eq!(integrity.len(), 1);
    match integrity[0].values() {
        [SqliteValue::Text(result)] => assert_eq!(result.to_string(), "ok"),
        other => panic!("unexpected integrity result {other:?}"),
    }
}

async fn assert_multiple_trunks(conn: &Connection, stock: &rusqlite::Connection) -> (i64, i64) {
    // Even a completely full trunk holds at most (page_size / 4 - 2)
    // leaves plus itself. Do not assume SQLite's more conservative writer
    // occupancy, nor infer the exercised path merely from the fixture size.
    let one_trunk = i64::try_from(PAGE_BYTES / 4 - 1).unwrap();
    let ours = our_int(conn, "PRAGMA freelist_count").await;
    let theirs = stock_int(stock, "PRAGMA freelist_count");
    assert!(ours > one_trunk, "FrankenSQLite freed only {ours} pages");
    assert!(theirs > one_trunk, "stock freed only {theirs} pages");
    (ours, theirs)
}

async fn run_case(file_backed: bool, disable_retention: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = if file_backed {
        directory
            .path()
            .join("fsqlite.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.unwrap();
    let stock = if file_backed {
        rusqlite::Connection::open(directory.path().join("stock.db")).unwrap()
    } else {
        rusqlite::Connection::open_in_memory().unwrap()
    };
    stock.execute_batch("PRAGMA page_size=4096").unwrap();
    execute_pair(&conn, &stock, "PRAGMA journal_mode=DELETE").await;
    assert_eq!(our_int(&conn, "PRAGMA page_size").await, 4096);
    assert_eq!(stock_int(&stock, "PRAGMA page_size"), 4096);
    execute_pair(
        &conn,
        &stock,
        "CREATE TABLE t(id INTEGER PRIMARY KEY,a INTEGER,b BLOB)",
    )
    .await;
    execute_pair(&conn, &stock, "CREATE INDEX ta ON t(b)").await;
    execute_pair(&conn, &stock, "BEGIN IMMEDIATE").await;
    for key in 1..=i64::try_from(SEED_ROWS).unwrap() {
        insert_pair(&conn, &stock, key, &pattern(SEED_BYTES, key)).await;
    }
    execute_pair(&conn, &stock, "COMMIT").await;
    if disable_retention {
        conn.execute("PRAGMA fsqlite.autocommit_retain=OFF")
            .await
            .unwrap();
    }
    check(&conn, &stock, false, false).await;
    execute_pair(&conn, &stock, "DROP INDEX ta").await;
    let (our_free, stock_free) = assert_multiple_trunks(&conn, &stock).await;
    // Require the large row to exhaust even a full freelist before allocating
    // fresh EOF pages. This is a fixture precondition, not a speed assertion.
    let minimum_overflow = i64::try_from(VICTIM_BYTES / OVERFLOW_BYTES - 2).unwrap();
    assert!(
        our_free < minimum_overflow,
        "increase overflow fixture: {our_free}"
    );
    assert!(stock_free < minimum_overflow);
    let victim_blob = pattern(VICTIM_BYTES, VICTIM);
    insert_pair(&conn, &stock, VICTIM, &victim_blob).await;
    execute_pair(&conn, &stock, CREATE_U).await;
    assert!(
        our_int(&conn, "PRAGMA freelist_count").await < our_free,
        "reuse must stay enabled"
    );
    assert!(stock_int(&stock, "PRAGMA freelist_count") < stock_free);
    check(&conn, &stock, true, false).await;

    // Force speculative allocation beyond the committed overflow chain, then
    // rewind it. The following schema publication must not expose old roots or
    // hand committed pages back to another owner.
    let abandoned = pattern(VICTIM_BYTES + 3 * OVERFLOW_BYTES, VICTIM + 1);
    for savepoint in [false, true] {
        execute_pair(&conn, &stock, "BEGIN IMMEDIATE").await;
        if savepoint {
            execute_pair(&conn, &stock, "UPDATE t SET a=a+1 WHERE id=1").await;
            execute_pair(&conn, &stock, "SAVEPOINT reuse").await;
        }
        execute_pair(&conn, &stock, "DELETE FROM t WHERE id=1000000").await;
        insert_pair(&conn, &stock, VICTIM + 1, &abandoned).await;
        execute_pair(
            &conn,
            &stock,
            "CREATE TABLE abandoned(k TEXT PRIMARY KEY,v INTEGER)",
        )
        .await;
        if savepoint {
            for sql in ["ROLLBACK TO reuse", "RELEASE reuse", "COMMIT"] {
                execute_pair(&conn, &stock, sql).await;
            }
        } else {
            execute_pair(&conn, &stock, "ROLLBACK").await;
        }
        execute_pair(
            &conn,
            &stock,
            "CREATE TABLE boundary(k TEXT PRIMARY KEY,v INTEGER)",
        )
        .await;
        execute_pair(&conn, &stock, "DROP TABLE boundary").await;
        check(&conn, &stock, true, savepoint).await;
    }

    let high_water = our_int(&conn, "PRAGMA page_count").await;
    let stock_high_water = stock_int(&stock, "PRAGMA page_count");
    for cycle in 0..3 {
        execute_pair(&conn, &stock, "DELETE FROM t WHERE id=1000000").await;
        execute_pair(&conn, &stock, "DROP TABLE u").await;
        assert_multiple_trunks(&conn, &stock).await;
        insert_pair(&conn, &stock, VICTIM, &victim_blob).await;
        execute_pair(&conn, &stock, CREATE_U).await;
        check(&conn, &stock, true, true).await;
        // Private-memory reuse must fit the established extent. File-backed
        // pagers may retain allocation slack; their raw persisted image is
        // checked below instead of requiring SQLite's physical page layout.
        if !file_backed {
            assert!(
                our_int(&conn, "PRAGMA page_count").await <= high_water,
                "leaked pages on cycle {cycle}"
            );
        }
        assert!(stock_int(&stock, "PRAGMA page_count") <= stock_high_water);
    }
    conn.close().await.unwrap();
    if file_backed {
        // Inspect the actual FrankenSQLite bytes with stock before reopening
        // through FrankenSQLite, which might otherwise conceal a flush defect.
        let persisted = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        drop(checked_stock_rows(&persisted, true, true));
        assert_eq!(stock_schema(&persisted), stock_schema(&stock));
        drop(persisted);
        let reopened = Connection::open(&path).await.unwrap();
        check(&reopened, &stock, true, true).await;
        reopened.close().await.unwrap();
    }
}

#[test]
fn gh503_multitrunk_memory_reuse_with_retention_off_matches_sqlite() {
    asupersync::test_utils::run_test(|| async { run_case(false, true).await });
}

#[test]
fn gh503_multitrunk_default_retention_matches_sqlite() {
    asupersync::test_utils::run_test(|| async { run_case(false, false).await });
}

#[test]
fn gh503_multitrunk_file_reuse_and_reopen_match_sqlite() {
    asupersync::test_utils::run_test(|| async { run_case(true, true).await });
}
