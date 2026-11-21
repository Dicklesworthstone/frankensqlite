#![recursion_limit = "512"]

//! bd-dwjnq.3 / bd-dwjnq.4: Independent acceptance proof and stock-SQLite
//! oracle suite for `PRAGMA mmap_size` governing actual VFS database-file
//! mapping admission.
//!
//! Verifies:
//! 1. Public `PRAGMA mmap_size` readback and actual OS `MAP_SHARED | PROT_READ`
//!    admission across `0`, small (partial-file), large (full-file), huge
//!    (clamped to `0x7fff_0000`), and negative (reset to `0`) values, matching
//!    stock C SQLite 3.46.1 (`rusqlite`) and proving real mmap read hits and
//!    past-limit `pread` fallbacks.
//! 2. Dynamic file growth (lazy remap expands `mapped_bytes`), file truncation
//!    via `DELETE` + `VACUUM` (safe pre-truncate unmap + shrink remap with zero
//!    SIGBUS), and strictly bounded VMA lifetime across `close_in_place` and
//!    reopen (verified via both per-connection `unmap_count` and process-wide
//!    `active_database_mmap_count`).
//! 3. Concurrent multi-thread `SnapshotPageReader` reads sharing the bounded
//!    read-only mapping alongside concurrent MVCC WAL writers (concurrent-writer
//!    mode remains ON by default).
//! 4. Mapping failure fallback (`inject_mmap_failure` degrades cleanly to
//!    `pread` without errors and recovers when reconfigured) and encrypted
//!    database (`PRAGMA key`) compatibility over mmap reads.
//! 5. Explicit unsupported reporting on `:memory:` (readback stays `0` and
//!    `mmap_status().supported == false`) plus a planted stored-only negative
//!    control that fails the acceptance predicate when no mapping is admitted.

use std::fs;
use std::path::Path;
use std::sync::{Arc, Barrier, RwLock};
use std::thread;

/// Coordinates process-wide `active_database_mmap_count()` observation against
/// sibling tests in this binary so exact VMA lifetime assertions remain
/// deterministic under parallel `cargo test` execution.
static GLOBAL_MMAP_TEST_GUARD: RwLock<()> = RwLock::new(());

use asupersync::test_utils::run_test;
use fsqlite_core::connection::Connection;
use fsqlite_types::PageNumber;
use fsqlite_types::value::SqliteValue;
use fsqlite_vfs::{MAX_MMAP_SIZE, VfsMmapStatus, active_database_mmap_count};
use tempfile::TempDir;

fn sqlite_val_from_rusqlite(val: rusqlite::types::Value) -> SqliteValue {
    match val {
        rusqlite::types::Value::Null => SqliteValue::Null,
        rusqlite::types::Value::Integer(i) => SqliteValue::Integer(i),
        rusqlite::types::Value::Real(f) => SqliteValue::Float(f),
        rusqlite::types::Value::Text(s) => SqliteValue::Text(s.into()),
        rusqlite::types::Value::Blob(b) => SqliteValue::Blob(b.into()),
    }
}

fn rusqlite_query(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("rusqlite prepare `{sql}`: {e}"));
    let col_count = stmt.column_count();
    stmt.query_map([], |row| {
        let mut vals = Vec::with_capacity(col_count);
        for i in 0..col_count {
            vals.push(sqlite_val_from_rusqlite(row.get(i)?));
        }
        Ok(vals)
    })
    .unwrap_or_else(|e| panic!("rusqlite query `{sql}`: {e}"))
    .map(|r| r.expect("rusqlite row"))
    .collect()
}

async fn fsqlite_query(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite query `{sql}`: {e}"))
        .into_iter()
        .map(|row| row.values().to_vec())
        .collect()
}

/// Acceptance predicate for live mmap admission (`bd-dwjnq.4`).
/// A stored-only / readback-only implementation fails this predicate whenever
/// the platform reports `supported == true`.
fn check_live_mmap_acceptance(
    status: VfsMmapStatus,
    requested_limit: u64,
    expected_mapped_bytes: u64,
) -> std::result::Result<(), String> {
    if !status.supported {
        return Err("platform explicitly reported mmap unsupported".to_owned());
    }
    if status.configured_limit_bytes != requested_limit {
        return Err(format!(
            "configured_limit_bytes mismatch: got {}, expected {requested_limit}",
            status.configured_limit_bytes
        ));
    }
    if status.mapped_bytes < expected_mapped_bytes {
        return Err(format!(
            "stored-only/inert mmap: mapped_bytes={} < expected {expected_mapped_bytes}",
            status.mapped_bytes
        ));
    }
    if status.map_attempts == 0 {
        return Err("stored-only/inert mmap: map_attempts == 0".to_owned());
    }
    if status.mmap_read_hits == 0 {
        return Err("stored-only/inert mmap: mmap_read_hits == 0 after workload".to_owned());
    }
    Ok(())
}

async fn seed_multi_page_db(db_path: &Path, stock_path: &Path, row_count: i64) {
    let conn = Connection::open(db_path.to_str().unwrap())
        .await
        .expect("open seed fsqlite");
    let stock = rusqlite::Connection::open(stock_path).expect("open seed rusqlite");

    let ddl = "CREATE TABLE items(id INTEGER PRIMARY KEY, grp INTEGER, payload TEXT);";
    conn.execute("PRAGMA journal_mode = WAL;")
        .await
        .expect("fsqlite wal");
    stock
        .execute_batch("PRAGMA journal_mode = WAL;")
        .expect("stock wal");
    conn.execute(ddl).await.expect("fsqlite ddl");
    stock.execute(ddl, []).expect("stock ddl");

    conn.execute("BEGIN;").await.expect("fsqlite begin");
    stock.execute("BEGIN;", []).expect("stock begin");
    for i in 1..=row_count {
        let payload = format!("item-{i:04}-{}", "M".repeat(600));
        let sql = format!("INSERT INTO items VALUES ({i}, {}, '{payload}');", i % 17);
        conn.execute(&sql).await.expect("fsqlite insert");
        stock.execute(&sql, []).expect("stock insert");
    }
    conn.execute("COMMIT;").await.expect("fsqlite commit");
    stock.execute("COMMIT;", []).expect("stock commit");

    // Checkpoint WAL frames into the main DB file so the main file spans dozens
    // of 4 KiB pages on disk.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        .await
        .expect("fsqlite checkpoint");
    stock
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("stock checkpoint");
    conn.close().await.expect("close seed fsqlite");
    stock.close().expect("close seed rusqlite");
}

#[test]
fn test_mmap_size_zero_small_large_negative_and_stock_oracle() {
    let _shared_guard = GLOBAL_MMAP_TEST_GUARD
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_test(|| async {
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("oracle.db");
        let stock_path = dir.path().join("stock.db");
        seed_multi_page_db(&db_path, &stock_path, 350).await;

        let file_len = fs::metadata(&db_path).expect("db metadata").len();
        assert!(
            file_len >= 64 * 1024,
            "seeded DB must span >= 16 pages so partial/full mmap caps are distinct (got {file_len} B)"
        );

        let conn = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open reader");
        let stock = rusqlite::Connection::open(&stock_path).expect("open stock reader");

        // 1. Default is 0 (disabled): matches stock, zero mapped bytes.
        assert_eq!(
            fsqlite_query(&conn, "PRAGMA mmap_size;").await,
            rusqlite_query(&stock, "PRAGMA mmap_size;")
        );
        let init_status = conn.mmap_status();
        assert!(init_status.supported, "Linux/Unix file DB must support mmap");
        assert_eq!(init_status.configured_limit_bytes, 0);
        assert_eq!(init_status.mapped_bytes, 0);
        assert!(!init_status.is_active());

        // 2. Small cap (16 KiB < file_len): maps only first 16 KiB; full scan
        //    produces BOTH mmap hits (prefix pages) AND pread fallbacks (tail pages).
        assert_eq!(
            fsqlite_query(&conn, "PRAGMA mmap_size = 16384;").await,
            rusqlite_query(&stock, "PRAGMA mmap_size = 16384;")
        );
        let s_small = conn.mmap_status();
        assert_eq!(s_small.configured_limit_bytes, 16_384);
        assert_eq!(s_small.mapped_bytes, 16_384);
        assert!(s_small.is_active());
        assert!(s_small.map_attempts >= 1);

        let scan_sql = "SELECT id, grp, length(payload) FROM items ORDER BY id;";
        assert_eq!(
            fsqlite_query(&conn, scan_sql).await,
            rusqlite_query(&stock, scan_sql),
            "partial-mmap scan must match stock SQLite byte-for-byte"
        );
        let after_small = conn.mmap_status();
        assert!(
            after_small.mmap_read_hits > 0,
            "small-cap scan must serve prefix pages via mmap: {after_small:?}"
        );
        assert!(
            after_small.pread_fallbacks > 0,
            "small-cap scan must fall back to pread for pages past 16 KiB: {after_small:?}"
        );
        check_live_mmap_acceptance(after_small, 16_384, 16_384)
            .expect("small-cap live mmap must pass acceptance");

        // 3. Large cap (4 MiB > file_len) on a cold reader: maps entire file,
        //    serves every cold page read from mmap with ZERO pread fallbacks.
        let cold = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open cold reader");
        assert_eq!(
            fsqlite_query(&cold, "PRAGMA mmap_size = 4194304;").await,
            rusqlite_query(&stock, "PRAGMA mmap_size = 4194304;")
        );
        let s_large = cold.mmap_status();
        assert_eq!(s_large.configured_limit_bytes, 4_194_304);
        assert_eq!(s_large.mapped_bytes, file_len);
        assert_eq!(
            fsqlite_query(&cold, scan_sql).await,
            rusqlite_query(&stock, scan_sql),
            "full-file mmap scan must match stock SQLite byte-for-byte"
        );
        let after_large = cold.mmap_status();
        assert!(
            after_large.mmap_read_hits >= 10,
            "cold full-file scan must record many mmap hits: {after_large:?}"
        );
        assert_eq!(
            after_large.pread_fallbacks, 0,
            "full-file mmap scan should not fall back to pread: {after_large:?}"
        );
        check_live_mmap_acceptance(after_large, 4_194_304, file_len)
            .expect("full-file live mmap must pass acceptance");

        // 4. Huge value clamps to MAX_MMAP_SIZE (0x7fff_0000), matching stock SQLite!
        assert_eq!(
            fsqlite_query(&cold, "PRAGMA mmap_size = 9223372036854775807;").await,
            rusqlite_query(&stock, "PRAGMA mmap_size = 9223372036854775807;")
        );
        assert_eq!(cold.mmap_status().configured_limit_bytes, MAX_MMAP_SIZE);

        // 5. Negative value resets to 0 and immediately unmaps VMA.
        let unmaps_before = cold.mmap_status().unmap_count;
        assert_eq!(
            fsqlite_query(&cold, "PRAGMA mmap_size = -1;").await,
            rusqlite_query(&stock, "PRAGMA mmap_size = -1;")
        );
        let s_neg = cold.mmap_status();
        assert_eq!(s_neg.configured_limit_bytes, 0);
        assert_eq!(s_neg.mapped_bytes, 0);
        assert!(!s_neg.is_active());
        assert!(s_neg.unmap_count > unmaps_before);

        // 6. Explicit 0 on the first connection also unmaps cleanly.
        let unmaps_conn = conn.mmap_status().unmap_count;
        assert_eq!(
            fsqlite_query(&conn, "PRAGMA mmap_size = 0;").await,
            rusqlite_query(&stock, "PRAGMA mmap_size = 0;")
        );
        assert_eq!(conn.mmap_status().mapped_bytes, 0);
        assert!(conn.mmap_status().unmap_count > unmaps_conn);

        cold.close().await.expect("close cold");
        conn.close().await.expect("close conn");
        stock.close().expect("close stock");
    });
}

#[test]
fn test_mmap_size_file_growth_truncate_vacuum_and_close_reopen_lifetime() {
    let _exclusive_guard = GLOBAL_MMAP_TEST_GUARD
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_test(|| async {
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("growth_trunc.db");
        let stock_path = dir.path().join("growth_trunc_stock.db");
        seed_multi_page_db(&db_path, &stock_path, 30).await;

        let initial_len = fs::metadata(&db_path).expect("initial meta").len();

        // Keep reader open with a large mmap limit BEFORE growth so it starts
        // with a small mapping and must dynamically remap as the DB grows!
        let mut reader = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open reader");
        reader
            .execute("PRAGMA mmap_size = 4194304;")
            .await
            .expect("set reader mmap");
        let s_init = reader.mmap_status();
        assert_eq!(s_init.mapped_bytes, initial_len);
        let init_gen = s_init.mapping_generation;

        // Grow DB dramatically via a second writer connection and checkpoint into main file.
        let writer = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open writer");
        writer.execute("BEGIN;").await.expect("begin grow");
        for i in 31..=500_i64 {
            let payload = format!("grown-{i:04}-{}", "G".repeat(700));
            writer
                .execute(&format!("INSERT INTO items VALUES ({i}, {}, '{payload}');", i % 19))
                .await
                .expect("insert grown");
        }
        writer.execute("COMMIT;").await.expect("commit grow");
        writer
            .execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("checkpoint grow");
        let grown_len = fs::metadata(&db_path).expect("grown meta").len();
        assert!(
            grown_len > initial_len * 4,
            "DB must grow significantly: initial={initial_len} grown={grown_len}"
        );

        // Reader scans all 500 rows: lazy growth remap expands `mapped_bytes` to `grown_len`!
        let count_rows = fsqlite_query(&reader, "SELECT COUNT(*), SUM(id) FROM items;").await;
        assert_eq!(
            count_rows,
            vec![vec![
                SqliteValue::Integer(500),
                SqliteValue::Integer(500 * 501 / 2),
            ]]
        );
        let s_grown = reader.mmap_status();
        assert_eq!(
            s_grown.mapped_bytes, grown_len,
            "reader must lazily remap to grown file size: {s_grown:?}"
        );
        assert!(
            s_grown.mapping_generation > init_gen,
            "growth remap must advance mapping_generation: init={init_gen} grown={s_grown:?}"
        );
        assert!(s_grown.mmap_read_hits > 0);
        writer.close().await.expect("close writer");

        // Close reader via `close_in_place`: verify bounded lifetime unmaps VMA
        // BEFORE `close_in_place` returns!
        let active_before_close = active_database_mmap_count();
        assert!(active_before_close >= 1);
        let unmaps_before_close = reader.mmap_status().unmap_count;
        reader.close_in_place().await.expect("close_in_place reader");
        assert_eq!(reader.mmap_status().mapped_bytes, 0);
        assert!(reader.mmap_status().unmap_count > unmaps_before_close);
        assert_eq!(
            active_database_mmap_count(),
            active_before_close - 1,
            "close_in_place must release OS database mmap immediately"
        );

        // Reopen, enable mmap, DELETE 96% of rows, and VACUUM (truncates DB file!).
        // Verifies pre-truncate unmap prevents SIGBUS and remaps to shrunk size.
        let mut vac_conn = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("reopen for vacuum");
        assert_eq!(
            fsqlite_query(&vac_conn, "PRAGMA mmap_size;").await,
            vec![vec![SqliteValue::Integer(0)]],
            "reopened connection must start at default mmap_size=0"
        );
        vac_conn
            .execute("PRAGMA mmap_size = 4194304;")
            .await
            .expect("enable mmap before vacuum");
        assert_eq!(vac_conn.mmap_status().mapped_bytes, grown_len);

        vac_conn
            .execute("DELETE FROM items WHERE id > 20;")
            .await
            .expect("delete most rows");
        vac_conn.execute("VACUUM;").await.expect("vacuum shrink");

        let shrunk_len = fs::metadata(&db_path).expect("shrunk meta").len();
        assert!(
            shrunk_len < grown_len / 2,
            "VACUUM must shrink file on disk: grown={grown_len} shrunk={shrunk_len}"
        );
        let s_shrunk = vac_conn.mmap_status();
        assert!(
            s_shrunk.mapped_bytes <= shrunk_len,
            "mapping after truncate/VACUUM must never exceed shrunk file len (SIGBUS safety): {s_shrunk:?} shrunk={shrunk_len}"
        );
        assert_eq!(
            fsqlite_query(&vac_conn, "SELECT COUNT(*), SUM(id) FROM items;").await,
            vec![vec![SqliteValue::Integer(20), SqliteValue::Integer(210)]]
        );

        vac_conn.close_in_place().await.expect("close vac_conn");
        assert_eq!(vac_conn.mmap_status().mapped_bytes, 0);
    });
}

#[test]
fn test_mmap_size_concurrent_wal_snapshots_and_mvcc_writers() {
    let _shared_guard = GLOBAL_MMAP_TEST_GUARD
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_test(|| async {
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("concurrent_snap.db");
        let stock_path = dir.path().join("concurrent_snap_stock.db");
        seed_multi_page_db(&db_path, &stock_path, 250).await;

        let file_len = fs::metadata(&db_path).expect("meta").len();

        // Reader pins a WAL read snapshot with mmap active.
        let snap_conn = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open snap_conn");
        snap_conn
            .execute("PRAGMA mmap_size = 4194304;")
            .await
            .expect("set snap mmap");
        assert_eq!(snap_conn.mmap_status().mapped_bytes, file_len);

        snap_conn.execute("BEGIN;").await.expect("begin read snap");
        let snap_count = fsqlite_query(&snap_conn, "SELECT COUNT(*) FROM items;").await;
        assert_eq!(snap_count, vec![vec![SqliteValue::Integer(250)]]);

        let snapshot_reader = snap_conn
            .active_snapshot_page_reader()
            .expect("pinned read-only WAL transaction must offer SnapshotPageReader");
        assert_eq!(
            snapshot_reader.mapped_bytes() as u64,
            file_len,
            "SnapshotPageReader must inherit active read-only mmap view"
        );
        let db_size = snapshot_reader.db_size();
        assert!(db_size >= 16);

        // Capture golden page bytes once on main thread.
        let mut golden_pages = Vec::with_capacity(db_size as usize);
        let mut golden_buf = snapshot_reader.new_buffer();
        for pgno in 1..=db_size {
            let page = snapshot_reader
                .read_page(PageNumber::new(pgno).unwrap(), &mut golden_buf)
                .expect("golden read_page");
            golden_pages.push(page.to_vec());
        }
        let golden_pages = Arc::new(golden_pages);

        // Concurrent MVCC writer commits new rows to WAL while 4 reader threads
        // read every page concurrently through the shared mmap-backed SnapshotPageReader!
        let writer = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open concurrent writer");
        assert!(
            writer.is_concurrent_mode_default(),
            "concurrent-writer mode MUST remain ON by default"
        );

        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();
        for tid in 0..4_usize {
            let reader_clone = snapshot_reader.clone();
            let golden_clone = Arc::clone(&golden_pages);
            let bar = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut buf = reader_clone.new_buffer();
                bar.wait();
                for round in 0..4_usize {
                    for pgno in 1..=db_size {
                        let page = reader_clone
                            .read_page(PageNumber::new(pgno).unwrap(), &mut buf)
                            .unwrap_or_else(|e| panic!("thread {tid} round {round} page {pgno}: {e}"));
                        assert_eq!(
                            page,
                            golden_clone[(pgno - 1) as usize].as_slice(),
                            "thread {tid} observed inconsistent snapshot page {pgno}"
                        );
                    }
                }
            }));
        }

        barrier.wait();
        writer.execute("BEGIN CONCURRENT;").await.expect("begin concurrent");
        for i in 251..=300_i64 {
            writer
                .execute(&format!(
                    "INSERT INTO items VALUES ({i}, 99, 'concurrent-wal-{i}-{}');",
                    "W".repeat(256)
                ))
                .await
                .expect("concurrent insert");
        }
        writer.execute("COMMIT;").await.expect("commit concurrent");

        for h in handles {
            h.join().expect("snapshot reader thread panicked");
        }
        drop(snapshot_reader);

        // Pinned snapshot still sees 250 rows (MVCC/WAL isolation preserved),
        // while writer sees 300 rows, and mmap hits grew significantly.
        assert_eq!(
            fsqlite_query(&snap_conn, "SELECT COUNT(*) FROM items;").await,
            vec![vec![SqliteValue::Integer(250)]]
        );
        assert_eq!(
            fsqlite_query(&writer, "SELECT COUNT(*) FROM items;").await,
            vec![vec![SqliteValue::Integer(300)]]
        );
        let hits = snap_conn.mmap_status().mmap_read_hits;
        assert!(
            hits >= u64::from(db_size) * 4,
            "concurrent snapshot threads must read through shared mmap: hits={hits}, db_size={db_size}"
        );

        snap_conn.execute("COMMIT;").await.expect("end snap txn");
        snap_conn.close().await.expect("close snap_conn");
        writer.close().await.expect("close writer");
    });
}

#[test]
fn test_mmap_size_mapping_failure_fallback_and_encrypted_db_compatibility() {
    let _shared_guard = GLOBAL_MMAP_TEST_GUARD
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_test(|| async {
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("fallback.db");
        let stock_path = dir.path().join("fallback_stock.db");
        seed_multi_page_db(&db_path, &stock_path, 180).await;

        let file_len = fs::metadata(&db_path).expect("meta").len();

        // 1. Mapping failure fallback: inject failure -> `PRAGMA mmap_size`
        //    degrades gracefully to pread without failing the pragma or queries.
        let conn = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open fallback conn");
        conn.inject_mmap_failure(1).expect("inject mmap failure");
        assert_eq!(
            fsqlite_query(&conn, "PRAGMA mmap_size = 4194304;").await,
            vec![vec![SqliteValue::Integer(4_194_304)]]
        );
        let s_fail = conn.mmap_status();
        assert_eq!(s_fail.mapped_bytes, 0);
        assert_eq!(s_fail.map_failures, 1);
        assert_eq!(s_fail.map_attempts, 1);

        let expected = vec![vec![
            SqliteValue::Integer(180),
            SqliteValue::Integer(180 * 181 / 2),
        ]];
        assert_eq!(
            fsqlite_query(&conn, "SELECT COUNT(*), SUM(id) FROM items;").await,
            expected,
            "query must succeed identically via pread fallback after mmap failure"
        );
        let after_fallback = conn.mmap_status();
        assert_eq!(after_fallback.mapped_bytes, 0);
        assert_eq!(after_fallback.mmap_read_hits, 0);
        assert!(
            after_fallback.pread_fallbacks > 0,
            "reads after mmap failure must increment pread_fallbacks: {after_fallback:?}"
        );

        // Re-issuing PRAGMA mmap_size recovers cleanly once OS mapping succeeds again.
        conn.execute("PRAGMA mmap_size = 4194304;")
            .await
            .expect("recover mmap");
        let s_recovered = conn.mmap_status();
        assert_eq!(s_recovered.mapped_bytes, file_len);
        assert!(s_recovered.is_active());
        conn.close().await.expect("close fallback conn");

        // 2. Encrypted database (`PRAGMA key`) compatibility: mmap reads raw
        //    ciphertext pages and pager decrypts/authenticates them identically!
        let enc_path = dir.path().join("encrypted_mmap.db");
        {
            let enc = Connection::open(enc_path.to_str().unwrap())
                .await
                .expect("open enc create");
            enc.execute("PRAGMA fsqlite.cipher_argon2_m_cost = 1024;")
                .await
                .expect("fast argon2 m");
            enc.execute("PRAGMA fsqlite.cipher_argon2_t_cost = 1;")
                .await
                .expect("fast argon2 t");
            enc.execute("PRAGMA fsqlite.cipher_argon2_p_cost = 1;")
                .await
                .expect("fast argon2 p");
            enc.execute("PRAGMA key = 'mmap-secret-passphrase';")
                .await
                .expect("set key");
            enc.execute("CREATE TABLE secrets(id INTEGER PRIMARY KEY, body TEXT);")
                .await
                .expect("create secrets");
            enc.execute("BEGIN;").await.expect("begin enc");
            for i in 1..=120_i64 {
                enc.execute(&format!(
                    "INSERT INTO secrets VALUES ({i}, 'secret-{i:04}-{}');",
                    "S".repeat(512)
                ))
                .await
                .expect("insert secret");
            }
            enc.execute("COMMIT;").await.expect("commit enc");
            enc.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                .await
                .expect("checkpoint enc");
            enc.close().await.expect("close enc writer");
        }
        let enc_len = fs::metadata(&enc_path).expect("enc meta").len();
        {
            let enc_reader = Connection::open(enc_path.to_str().unwrap())
                .await
                .expect("reopen enc");
            enc_reader
                .execute("PRAGMA key = 'mmap-secret-passphrase';")
                .await
                .expect("unlock enc");
            enc_reader
                .execute("PRAGMA mmap_size = 4194304;")
                .await
                .expect("set enc mmap");
            assert_eq!(enc_reader.mmap_status().mapped_bytes, enc_len);
            let rows = fsqlite_query(&enc_reader, "SELECT COUNT(*), SUM(id) FROM secrets;").await;
            assert_eq!(
                rows,
                vec![vec![
                    SqliteValue::Integer(120),
                    SqliteValue::Integer(120 * 121 / 2),
                ]]
            );
            assert!(
                enc_reader.mmap_status().mmap_read_hits > 0,
                "encrypted DB reads must work cleanly over OS mmap + AEAD decrypt"
            );
            enc_reader.close().await.expect("close enc reader");
        }
    });
}

#[test]
fn test_mmap_size_unsupported_memory_and_planted_stored_only_negative_control() {
    let _shared_guard = GLOBAL_MMAP_TEST_GUARD
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_test(|| async {
        // 1. `:memory:` explicitly reports unsupported; readback NEVER implies
        //    an active mapping.
        let mem = Connection::open(":memory:").await.expect("open :memory:");
        assert_eq!(
            fsqlite_query(&mem, "PRAGMA mmap_size;").await,
            vec![vec![SqliteValue::Integer(0)]]
        );
        assert_eq!(
            fsqlite_query(&mem, "PRAGMA mmap_size = 65536;").await,
            vec![vec![SqliteValue::Integer(0)]],
            ":memory: PRAGMA mmap_size readback must stay 0 (must not imply active mapping)"
        );
        assert_eq!(
            fsqlite_query(&mem, "PRAGMA mmap_size;").await,
            vec![vec![SqliteValue::Integer(0)]]
        );
        let mem_status = mem.mmap_status();
        assert!(!mem_status.supported);
        assert_eq!(mem_status.configured_limit_bytes, 0);
        assert_eq!(mem_status.mapped_bytes, 0);
        assert!(!mem_status.is_active());
        assert!(
            check_live_mmap_acceptance(mem_status, 65_536, 4096).is_err(),
            "unsupported backend must never satisfy supported-platform live acceptance"
        );
        mem.close().await.expect("close mem");

        // 2. Planted negative control: a stored-only knob on a supported platform
        //    that reports `configured_limit_bytes = 262144` with zero mapping
        //    admission (`mapped_bytes == 0`, `map_attempts == 0`, `mmap_read_hits == 0`)
        //    MUST fail acceptance.
        let stored_only_fake = VfsMmapStatus {
            supported: true,
            configured_limit_bytes: 262_144,
            mapped_bytes: 0,
            mapping_generation: 0,
            mmap_read_hits: 0,
            pread_fallbacks: 0,
            map_attempts: 0,
            map_failures: 0,
            unmap_count: 0,
        };
        let neg_err = check_live_mmap_acceptance(stored_only_fake, 262_144, 4096)
            .expect_err("planted stored-only mmap_size MUST fail acceptance");
        assert!(
            neg_err.contains("stored-only/inert"),
            "unexpected negative-control error: {neg_err}"
        );

        // Positive control on the exact same predicate with a real file-backed DB.
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("pos.db");
        let stock_path = dir.path().join("pos_stock.db");
        seed_multi_page_db(&db_path, &stock_path, 40).await;
        let file_len = fs::metadata(&db_path).expect("meta").len();

        let pos = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open pos");
        pos.execute("PRAGMA mmap_size = 262144;")
            .await
            .expect("set pos mmap");
        let _ = fsqlite_query(&pos, "SELECT COUNT(*) FROM items;").await;
        check_live_mmap_acceptance(pos.mmap_status(), 262_144, file_len.min(262_144))
            .expect("live file-backed DB must satisfy acceptance predicate");
        pos.close().await.expect("close pos");
    });
}
