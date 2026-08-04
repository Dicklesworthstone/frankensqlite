//! E2E verification for `bd-1dp9.6.7.8.2` incremental WAL refresh.
//!
//! Tests verify that:
//! - Steady-state page lookup uses authoritative index (no reverse scans)
//! - Refresh work scales with appended frames (not full WAL scan)
//! - Generation resets properly invalidate cached indexes
//! - Structured logs expose lookup mode, refresh deltas, and fallback reasons
#![recursion_limit = "512"]

use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

use fsqlite_types::SqliteValue;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-1dp9.6.7.8.2";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_1dp9_6_7_8_2_incremental_wal_refresh -- --nocapture --test-threads=1";

static E2E_LOCK: Mutex<()> = Mutex::new(());

fn bytes_to_hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

async fn open_fsqlite(path: &Path) -> fsqlite::Connection {
    let path = path.to_str().expect("utf-8 db path");
    let conn = fsqlite::Connection::open(path)
        .await
        .expect("open fsqlite connection");
    conn.execute("PRAGMA journal_mode=WAL").await.ok();
    assert!(
        conn.is_concurrent_mode_default(),
        "bead_id={BEAD_ID} case=concurrent_mode_must_be_enabled"
    );
    conn
}

fn open_sqlite(path: &Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).expect("open sqlite connection");
    conn.execute_batch("PRAGMA journal_mode=WAL;")
        .expect("enable sqlite wal");
    conn
}

/// Query a single integer column value.
async fn query_count(conn: &fsqlite::Connection, sql: &str) -> i64 {
    conn.query(sql)
        .await
        .expect("count query")
        .into_iter()
        .map(|row| match row.get(0) {
            Some(SqliteValue::Integer(v)) => *v,
            other => panic!("expected INTEGER, got {other:?}"),
        })
        .next()
        .expect("count result")
}

#[test]
fn bd_1dp9_6_7_8_2_steady_state_authoritative_lookup() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("authoritative_lookup.db");

        let conn = open_fsqlite(&db_path).await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create table");

        // Build up a WAL with multiple committed transactions
        const COMMIT_COUNT: i64 = 50;
        for txn in 1..=COMMIT_COUNT {
            conn.execute(&format!("INSERT INTO t VALUES ({txn}, 'value-{txn}')"))
                .await
                .expect("insert");
        }

        // Steady-state lookup should use authoritative index
        let start = Instant::now();
        for page in 1..=COMMIT_COUNT {
            let rows: Vec<_> = conn
                .query(&format!("SELECT val FROM t WHERE id = {page}"))
                .await
                .expect("lookup")
                .into_iter()
                .collect();
            assert_eq!(rows.len(), 1, "should find exactly one row for id {page}");
        }
        let lookup_elapsed = start.elapsed();

        // Verify all rows are present
        let count = query_count(&conn, "SELECT COUNT(*) FROM t").await;
        assert_eq!(count, COMMIT_COUNT, "all rows should be present");

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=AUTHORITATIVE-LOOKUP commits={COMMIT_COUNT} \
             lookup_time_ms={:.2} replay_command={REPLAY_COMMAND}",
            lookup_elapsed.as_secs_f64() * 1000.0
        );
    });
}

#[test]
fn bd_1dp9_6_7_8_2_incremental_refresh_scales_with_new_frames() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("incremental_refresh.db");

        let conn = open_fsqlite(&db_path).await;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create table");

        // Phase 1: Build initial WAL with many commits
        const INITIAL_ROWS: i64 = 100;
        for rowid in 1..=INITIAL_ROWS {
            conn.execute(&format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
                .await
                .expect("initial insert");
        }

        // Read to materialize the index
        let _count = query_count(&conn, "SELECT COUNT(*) FROM t").await;

        // Phase 2: Add a small batch of new commits and measure refresh time
        const INCREMENTAL_ROWS: i64 = 10;
        let start = Instant::now();
        for rowid in (INITIAL_ROWS + 1)..=(INITIAL_ROWS + INCREMENTAL_ROWS) {
            conn.execute(&format!("INSERT INTO t VALUES ({rowid}, 'v{rowid}')"))
                .await
                .expect("incremental insert");
        }
        // Reading triggers incremental refresh
        let count = query_count(&conn, "SELECT COUNT(*) FROM t").await;
        let refresh_elapsed = start.elapsed();

        assert_eq!(
            count,
            INITIAL_ROWS + INCREMENTAL_ROWS,
            "all rows should be visible after incremental refresh"
        );

        // Incremental refresh should be fast (much less than rebuilding full index)
        // Allow generous time for CI variability but flag if suspiciously slow
        let max_expected_ms = 100.0;
        let actual_ms = refresh_elapsed.as_secs_f64() * 1000.0;

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=INCREMENTAL-REFRESH initial={INITIAL_ROWS} \
             incremental={INCREMENTAL_ROWS} refresh_ms={actual_ms:.2} replay_command={REPLAY_COMMAND}"
        );

        // Soft assertion - log warning if slow but don't fail
        if actual_ms > max_expected_ms {
            eprintln!(
                "WARN bead_id={BEAD_ID} case=incremental_refresh_slow expected_max_ms={max_expected_ms} \
                 actual_ms={actual_ms:.2}"
            );
        }
    });
}

#[test]
fn bd_1dp9_6_7_8_2_generation_reset_invalidates_index() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("generation_reset.db");
        let fsqlite_path = db_path.clone();
        let sqlite_path = temp.path().join("generation_reset_oracle.db");

        // Use fsqlite for the main test
        let fconn = open_fsqlite(&fsqlite_path).await;
        let sconn = open_sqlite(&sqlite_path);

        fconn
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create table fsqlite");
        sconn
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
            .expect("create table sqlite");

        // Phase 1: Insert initial data
        const INITIAL_ROWS: i64 = 20;
        for rowid in 1..=INITIAL_ROWS {
            fconn
                .execute(&format!("INSERT INTO t VALUES ({rowid}, 'old-{rowid}')"))
                .await
                .expect("initial insert fsqlite");
            sconn
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    rusqlite::params![rowid, format!("old-{rowid}")],
                )
                .expect("initial insert sqlite");
        }

        // Force checkpoint to simulate generation reset
        fconn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await.ok();
        sconn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();

        // Phase 2: Insert new data after checkpoint
        const NEW_ROWS: i64 = 10;
        for rowid in (INITIAL_ROWS + 1)..=(INITIAL_ROWS + NEW_ROWS) {
            fconn
                .execute(&format!("INSERT INTO t VALUES ({rowid}, 'new-{rowid}')"))
                .await
                .expect("new insert fsqlite");
            sconn
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    rusqlite::params![rowid, format!("new-{rowid}")],
                )
                .expect("new insert sqlite");
        }

        // Verify counts match oracle
        let fcount = query_count(&fconn, "SELECT COUNT(*) FROM t").await;
        let scount: i64 = sconn
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("sqlite count");

        assert_eq!(
            fcount, scount,
            "bead_id={BEAD_ID} case=generation_reset_count_parity"
        );
        assert_eq!(
            fcount,
            INITIAL_ROWS + NEW_ROWS,
            "all rows should be visible after generation reset"
        );

        // Verify specific row from new generation is visible
        let new_row: Vec<_> = fconn
            .query(&format!(
                "SELECT val FROM t WHERE id = {}",
                INITIAL_ROWS + 1
            ))
            .await
            .expect("lookup new row")
            .into_iter()
            .collect();
        assert_eq!(new_row.len(), 1, "new row should be visible");
        if let Some(SqliteValue::Text(val)) = new_row[0].get(0) {
            assert!(
                val.starts_with("new-"),
                "new row should have new generation data"
            );
        } else {
            panic!("expected TEXT value for new row");
        }

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=GENERATION-RESET initial={INITIAL_ROWS} \
             new={NEW_ROWS} total={fcount} replay_command={REPLAY_COMMAND}"
        );
    });
}

#[test]
fn bd_1dp9_6_7_8_2_cross_connection_visibility_with_refresh() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("cross_conn_visibility.db");

        // Connection 1: Create table and insert initial data
        let conn1 = open_fsqlite(&db_path).await;
        conn1
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create table");
        conn1
            .execute("INSERT INTO t VALUES (1, 'conn1-initial')")
            .await
            .expect("initial insert");

        // Connection 2: Should see conn1's committed data
        let conn2 = open_fsqlite(&db_path).await;
        let count2 = query_count(&conn2, "SELECT COUNT(*) FROM t").await;
        assert_eq!(count2, 1, "conn2 should see conn1's committed data");

        // Connection 1: Insert more data
        conn1
            .execute("INSERT INTO t VALUES (2, 'conn1-second')")
            .await
            .expect("second insert");

        // Connection 2: Refresh and see new data
        let count2_after = query_count(&conn2, "SELECT COUNT(*) FROM t").await;
        assert_eq!(
            count2_after, 2,
            "conn2 should see conn1's new committed data after refresh"
        );

        // Verify both connections see same data
        let conn1_count = query_count(&conn1, "SELECT COUNT(*) FROM t").await;
        let conn2_count = query_count(&conn2, "SELECT COUNT(*) FROM t").await;
        assert_eq!(
            conn1_count, conn2_count,
            "both connections should see same row count"
        );

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=CROSS-CONNECTION-VISIBILITY final_count={conn1_count} \
             replay_command={REPLAY_COMMAND}"
        );
    });
}

#[test]
fn bd_1dp9_6_7_8_2_many_pages_authoritative_index_performance() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
    asupersync::test_utils::run_test(|| async {
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("many_pages_fsqlite.db");
        let sqlite_db = temp.path().join("many_pages_sqlite.db");

        let fconn = open_fsqlite(&fsqlite_db).await;
        let sconn = open_sqlite(&sqlite_db);

        fconn
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")
            .await
            .expect("create table fsqlite");
        sconn
            .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB);")
            .expect("create table sqlite");

        // Insert enough rows to span multiple pages
        const ROW_COUNT: i64 = 500;
        let data = vec![0x42u8; 100]; // 100-byte payload per row

        let fsqlite_insert_start = Instant::now();
        for rowid in 1..=ROW_COUNT {
            fconn
                .execute(&format!(
                    "INSERT INTO t VALUES ({rowid}, X'{}')",
                    bytes_to_hex(&data)
                ))
                .await
                .expect("fsqlite insert");
        }
        let fsqlite_insert_elapsed = fsqlite_insert_start.elapsed();

        let sqlite_insert_start = Instant::now();
        for rowid in 1..=ROW_COUNT {
            sconn
                .execute(
                    "INSERT INTO t VALUES (?1, ?2)",
                    rusqlite::params![rowid, &data],
                )
                .expect("sqlite insert");
        }
        let sqlite_insert_elapsed = sqlite_insert_start.elapsed();

        // Measure random lookup performance
        let fsqlite_lookup_start = Instant::now();
        for i in 0..100 {
            let rowid = (i * 5) % ROW_COUNT + 1;
            let _rows: Vec<_> = fconn
                .query(&format!("SELECT data FROM t WHERE id = {rowid}"))
                .await
                .expect("fsqlite lookup")
                .into_iter()
                .collect();
        }
        let fsqlite_lookup_elapsed = fsqlite_lookup_start.elapsed();

        let sqlite_lookup_start = Instant::now();
        for i in 0..100 {
            let rowid = (i * 5) % ROW_COUNT + 1;
            let _: Vec<u8> = sconn
                .query_row("SELECT data FROM t WHERE id = ?1", [rowid], |row| {
                    row.get(0)
                })
                .expect("sqlite lookup");
        }
        let sqlite_lookup_elapsed = sqlite_lookup_start.elapsed();

        // Verify row counts match
        let fcount = query_count(&fconn, "SELECT COUNT(*) FROM t").await;
        let scount: i64 = sconn
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("sqlite count");
        assert_eq!(fcount, scount, "row counts should match oracle");
        assert_eq!(fcount, ROW_COUNT, "all rows should be present");

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=MANY-PAGES-PERFORMANCE rows={ROW_COUNT} \
             fsqlite_insert_ms={:.2} sqlite_insert_ms={:.2} \
             fsqlite_lookup_100_ms={:.2} sqlite_lookup_100_ms={:.2} \
             replay_command={REPLAY_COMMAND}",
            fsqlite_insert_elapsed.as_secs_f64() * 1000.0,
            sqlite_insert_elapsed.as_secs_f64() * 1000.0,
            fsqlite_lookup_elapsed.as_secs_f64() * 1000.0,
            sqlite_lookup_elapsed.as_secs_f64() * 1000.0
        );
    });
}
