//! bd-1q134 — End-to-end stock SQLite oracle tests for `PRAGMA auto_vacuum`,
//! `PRAGMA incremental_vacuum`, pointer-map maintenance, root-page compaction,
//! mode transitions (`FULL <-> INCREMENTAL` live, `NONE <-> FULL/INCREMENTAL` via
//! `VACUUM`), multi-ptrmap 512-byte databases, concurrent WAL mode, and planted
//! corrupt-ptrmap negative controls.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

fn query_i64(rows: &[fsqlite_core::connection::Row]) -> i64 {
    match rows
        .first()
        .and_then(|r| r.values().first())
        .expect("expected single integer row")
    {
        SqliteValue::Integer(n) => *n,
        other => panic!("expected Integer, got {other:?}"),
    }
}

fn query_text(rows: &[fsqlite_core::connection::Row]) -> String {
    match rows
        .first()
        .and_then(|r| r.values().first())
        .expect("expected single text row")
    {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected Text, got {other:?}"),
    }
}

fn stock_check(path: &Path) -> (String, i64, i64, i64) {
    let conn = rusqlite::Connection::open(path).expect("rusqlite open");
    let integrity: String = conn
        .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
        .expect("stock integrity_check");
    let auto_vacuum: i64 = conn
        .query_row("PRAGMA auto_vacuum;", [], |r| r.get(0))
        .expect("stock auto_vacuum");
    let page_count: i64 = conn
        .query_row("PRAGMA page_count;", [], |r| r.get(0))
        .expect("stock page_count");
    let freelist_count: i64 = conn
        .query_row("PRAGMA freelist_count;", [], |r| r.get(0))
        .expect("stock freelist_count");
    (integrity, auto_vacuum, page_count, freelist_count)
}

#[test]
fn test_fsqlite_auto_vacuum_full_shrinks_physical_file_and_passes_stock_oracle() {
    asupersync::test_utils::run_test(|| async {
        for journal_mode in ["DELETE", "WAL"] {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join(format!("av_full_{journal_mode}.db"));
            let path_str = path.to_string_lossy().into_owned();

            let conn = Connection::open(&path_str).await.expect("open");
            conn.execute(&format!("PRAGMA journal_mode = {journal_mode};"))
                .await
                .expect("set journal_mode");
            conn.execute("PRAGMA auto_vacuum = FULL;")
                .await
                .expect("enable auto_vacuum FULL");
            assert_eq!(
                query_i64(&conn.query("PRAGMA auto_vacuum;").await.unwrap()),
                1
            );

            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, payload BLOB);")
                .await
                .expect("create table");
            conn.execute("CREATE INDEX t_k ON t(k);")
                .await
                .expect("create index");

            conn.execute("BEGIN;").await.unwrap();
            for i in 1..=350_i64 {
                conn.execute(&format!(
                    "INSERT INTO t(id, k, payload) VALUES ({i}, 'key-{i:04}', zeroblob(1800));"
                ))
                .await
                .expect("insert");
            }
            conn.execute("COMMIT;").await.unwrap();
            if journal_mode == "WAL" {
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                    .await
                    .expect("checkpoint peak");
            }

            let pages_peak = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
            let bytes_peak = std::fs::metadata(&path).expect("peak metadata").len();
            assert!(
                pages_peak > 120,
                "{journal_mode}: expected grown DB > 120 pages, got {pages_peak}"
            );

            // Delete 95% of rows; auto_vacuum = FULL must reclaim pages immediately on COMMIT.
            conn.execute("DELETE FROM t WHERE id > 15;")
                .await
                .expect("mass delete");
            if journal_mode == "WAL" {
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                    .await
                    .expect("checkpoint shrunk");
            }

            let pages_shrunk = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
            let freelist_shrunk = query_i64(&conn.query("PRAGMA freelist_count;").await.unwrap());
            let bytes_shrunk = std::fs::metadata(&path).expect("shrunk metadata").len();
            assert_eq!(
                freelist_shrunk, 0,
                "{journal_mode}: FULL mode must leave freelist_count == 0"
            );
            assert!(
                pages_shrunk < pages_peak / 3,
                "{journal_mode}: page_count must shrink substantially ({pages_peak} -> {pages_shrunk})"
            );
            assert!(
                bytes_shrunk < bytes_peak / 2,
                "{journal_mode}: physical file size must shrink ({bytes_peak} -> {bytes_shrunk})"
            );
            assert_eq!(
                query_text(&conn.query("PRAGMA integrity_check;").await.unwrap()),
                "ok",
                "{journal_mode}: fsqlite integrity_check after shrink"
            );
            conn.close().await.expect("close");

            let (integrity, av, pc, fc) = stock_check(&path);
            assert_eq!(
                integrity, "ok",
                "{journal_mode}: stock integrity_check after FULL shrink"
            );
            assert_eq!(av, 1);
            assert_eq!(pc, pages_shrunk);
            assert_eq!(fc, 0);
            let stock = rusqlite::Connection::open(&path).unwrap();
            let count: i64 = stock
                .query_row("SELECT count(*) FROM t;", [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, 15);
            drop(stock);

            // Now drop the only table under FULL and verify the DB shrinks back to 1 page (4096B),
            // removing pointer-map page 2 while preserving auto_vacuum = 1.
            let conn2 = Connection::open(&path_str).await.expect("reopen");
            conn2.execute("DROP TABLE t;").await.expect("drop only table");
            if journal_mode == "WAL" {
                conn2
                    .execute("PRAGMA wal_checkpoint(TRUNCATE);")
                    .await
                    .expect("checkpoint empty");
            }
            assert_eq!(
                query_i64(&conn2.query("PRAGMA page_count;").await.unwrap()),
                1,
                "{journal_mode}: empty FULL DB should truncate back to 1 page"
            );
            assert_eq!(
                query_i64(&conn2.query("PRAGMA freelist_count;").await.unwrap()),
                0
            );
            assert_eq!(
                query_i64(&conn2.query("PRAGMA auto_vacuum;").await.unwrap()),
                1
            );
            assert_eq!(
                query_text(&conn2.query("PRAGMA integrity_check;").await.unwrap()),
                "ok"
            );
            conn2.close().await.expect("close empty");

            let (integrity_empty, av_empty, pc_empty, fc_empty) = stock_check(&path);
            assert_eq!(integrity_empty, "ok");
            assert_eq!(av_empty, 1);
            assert_eq!(pc_empty, 1);
            assert_eq!(fc_empty, 0);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), 4096);
        }
    });
}

#[test]
fn test_fsqlite_auto_vacuum_512b_multi_ptrmap_and_root_compaction() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("av_512b.db");
        let path_str = path.to_string_lossy().into_owned();

        // Seed a 512-byte auto_vacuum = FULL database using stock SQLite so
        // pointer-map page 2 covers pages 3..=104 and page 105 is the second ptrmap page.
        {
            let stock = rusqlite::Connection::open(&path).expect("rusqlite open");
            stock
                .execute_batch(
                    "PRAGMA page_size = 512;
                     PRAGMA auto_vacuum = FULL;
                     CREATE TABLE t1(id INTEGER PRIMARY KEY, v BLOB);
                     CREATE INDEX i1 ON t1(v);
                     CREATE TABLE t2(id INTEGER PRIMARY KEY, label TEXT, body BLOB);
                     CREATE INDEX i2 ON t2(label);
                     CREATE TABLE t3(id INTEGER PRIMARY KEY, note TEXT);",
                )
                .expect("stock 512B init");
        }

        let conn = Connection::open(&path_str).await.expect("fsqlite open");
        assert_eq!(
            query_i64(&conn.query("PRAGMA auto_vacuum;").await.unwrap()),
            1
        );

        // Grow past the 2nd ptrmap page (page 105) with inline + overflowing rows.
        conn.execute("BEGIN;").await.unwrap();
        for i in 1..=90_i64 {
            conn.execute(&format!(
                "INSERT INTO t1 VALUES ({i}, zeroblob(300));"
            ))
            .await
            .unwrap();
            conn.execute(&format!(
                "INSERT INTO t2 VALUES ({i}, 'lbl-{i:03}', zeroblob(900));"
            ))
            .await
            .unwrap();
            conn.execute(&format!(
                "INSERT INTO t3 VALUES ({i}, 'note-{i:03}');"
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();

        let pc_grown = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
        assert!(
            pc_grown > 250,
            "expected 512B DB to cross multiple ptrmap pages (>250 pages), got {pc_grown}"
        );
        assert_eq!(
            query_text(&conn.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );

        // DROP TABLE t1 (roots 3 and 4): forces t3 / i2 to compact down into vacated root slots.
        conn.execute("DROP TABLE t1;").await.expect("drop t1");
        assert_eq!(
            query_text(&conn.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );

        // CREATE TABLE t4 after t2 already has child/overflow pages directly above largest_root_page:
        // forces relocating a live non-root page out of the newly claimed root slot.
        conn.execute("CREATE TABLE t4(id INTEGER PRIMARY KEY, x TEXT);")
            .await
            .expect("create t4");
        for i in 1..=25_i64 {
            conn.execute(&format!("INSERT INTO t4 VALUES ({i}, 'x-{i}');"))
                .await
                .unwrap();
        }

        // Delete 85% of t2 to trigger multi-ptrmap-group FULL truncation across page 105.
        conn.execute("DELETE FROM t2 WHERE id > 12;")
            .await
            .expect("delete t2");
        let pc_after = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
        assert!(
            pc_after < pc_grown / 2,
            "expected 512B DB to shrink across ptrmap boundary ({pc_grown} -> {pc_after})"
        );
        assert_eq!(
            query_text(&conn.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );
        conn.close().await.expect("close");

        let (integrity, av, pc, fc) = stock_check(&path);
        assert_eq!(integrity, "ok", "stock integrity_check on 512B multi-ptrmap DB");
        assert_eq!(av, 1);
        assert_eq!(pc, pc_after);
        assert_eq!(fc, 0);

        let stock = rusqlite::Connection::open(&path).unwrap();
        let c2: i64 = stock
            .query_row("SELECT count(*) FROM t2;", [], |r| r.get(0))
            .unwrap();
        let c3: i64 = stock
            .query_row("SELECT count(*) FROM t3;", [], |r| r.get(0))
            .unwrap();
        let c4: i64 = stock
            .query_row("SELECT count(*) FROM t4;", [], |r| r.get(0))
            .unwrap();
        assert_eq!((c2, c3, c4), (12, 90, 25));
    });
}

#[test]
fn test_fsqlite_auto_vacuum_incremental_stepwise_reclaim_and_savepoint() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("av_incremental.db");
        let path_str = path.to_string_lossy().into_owned();

        let conn = Connection::open(&path_str).await.expect("open");
        conn.execute("PRAGMA journal_mode = DELETE;")
            .await
            .unwrap();
        conn.execute("PRAGMA auto_vacuum = INCREMENTAL;")
            .await
            .unwrap();
        assert_eq!(
            query_i64(&conn.query("PRAGMA auto_vacuum;").await.unwrap()),
            2
        );

        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, v BLOB);")
            .await
            .unwrap();
        conn.execute("BEGIN;").await.unwrap();
        for i in 1..=200_i64 {
            conn.execute(&format!(
                "INSERT INTO items VALUES ({i}, zeroblob(1800));"
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();

        let pc_peak = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
        conn.execute("DELETE FROM items WHERE id > 10;")
            .await
            .unwrap();
        let pc_after_delete = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
        let fc_after_delete = query_i64(&conn.query("PRAGMA freelist_count;").await.unwrap());
        assert_eq!(
            pc_after_delete, pc_peak,
            "INCREMENTAL mode must NOT shrink before PRAGMA incremental_vacuum"
        );
        assert!(
            fc_after_delete > 40,
            "expected > 40 free pages after deleting 95%, got {fc_after_delete}"
        );

        // Step 1: reclaim exactly 5 pages.
        conn.execute("PRAGMA incremental_vacuum(5);").await.unwrap();
        let pc_step1 = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
        let fc_step1 = query_i64(&conn.query("PRAGMA freelist_count;").await.unwrap());
        assert!(
            pc_step1 <= pc_after_delete - 5,
            "incremental_vacuum(5) must reduce page_count by >= 5 ({pc_after_delete} -> {pc_step1})"
        );
        assert_eq!(fc_step1, fc_after_delete - 5);
        assert_eq!(
            query_text(&conn.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );

        // Step 2: incremental_vacuum inside a transaction with savepoint rollback.
        conn.execute("BEGIN;").await.unwrap();
        conn.execute("PRAGMA incremental_vacuum(4);").await.unwrap();
        let pc_before_sp = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
        conn.execute("SAVEPOINT sp1;").await.unwrap();
        conn.execute("PRAGMA incremental_vacuum(4);").await.unwrap();
        let pc_in_sp = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
        assert!(pc_in_sp < pc_before_sp);
        conn.execute("ROLLBACK TO sp1;").await.unwrap();
        conn.execute("RELEASE sp1;").await.unwrap();
        conn.execute("COMMIT;").await.unwrap();
        assert_eq!(
            query_i64(&conn.query("PRAGMA page_count;").await.unwrap()),
            pc_before_sp,
            "savepoint rollback must undo the nested incremental_vacuum(4)"
        );
        assert_eq!(
            query_text(&conn.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );

        // Step 3: drain all remaining free pages (no-arg form).
        conn.execute("PRAGMA incremental_vacuum;").await.unwrap();
        let pc_final = query_i64(&conn.query("PRAGMA page_count;").await.unwrap());
        let fc_final = query_i64(&conn.query("PRAGMA freelist_count;").await.unwrap());
        assert_eq!(fc_final, 0);
        assert!(pc_final < pc_peak / 3);
        assert_eq!(
            query_text(&conn.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );
        conn.close().await.unwrap();

        let (integrity, av, pc, fc) = stock_check(&path);
        assert_eq!(integrity, "ok");
        assert_eq!(av, 2);
        assert_eq!(pc, pc_final);
        assert_eq!(fc, 0);
    });
}

#[test]
fn test_auto_vacuum_mode_transitions_live_and_via_vacuum() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("av_transitions.db");
        let path_str = path.to_string_lossy().into_owned();

        let conn = Connection::open(&path_str).await.expect("open");
        conn.execute("PRAGMA journal_mode = DELETE;")
            .await
            .unwrap();
        assert_eq!(
            query_i64(&conn.query("PRAGMA auto_vacuum;").await.unwrap()),
            0
        );

        // Non-empty DB starting in NONE (0).
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT, b BLOB);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX t_s ON t(s);").await.unwrap();
        for i in 1..=120_i64 {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 's-{i:03}', zeroblob(1600));"
            ))
            .await
            .unwrap();
        }

        // NONE -> FULL on non-empty DB must stay 0 until VACUUM.
        conn.execute("PRAGMA auto_vacuum = FULL;").await.unwrap();
        assert_eq!(
            query_i64(&conn.query("PRAGMA auto_vacuum;").await.unwrap()),
            0,
            "NONE -> FULL must not activate until VACUUM"
        );
        conn.execute("VACUUM;").await.expect("VACUUM NONE -> FULL");
        assert_eq!(
            query_i64(&conn.query("PRAGMA auto_vacuum;").await.unwrap()),
            1,
            "VACUUM must activate pending FULL mode"
        );
        assert_eq!(
            query_text(&conn.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );

        // FULL -> INCREMENTAL switches LIVE without VACUUM.
        conn.execute("PRAGMA auto_vacuum = INCREMENTAL;")
            .await
            .unwrap();
        assert_eq!(
            query_i64(&conn.query("PRAGMA auto_vacuum;").await.unwrap()),
            2,
            "FULL -> INCREMENTAL must switch live immediately"
        );
        conn.close().await.unwrap();

        let (integrity_inc, av_inc, _, _) = stock_check(&path);
        assert_eq!(integrity_inc, "ok");
        assert_eq!(av_inc, 2);

        // Reopen, switch INCREMENTAL -> FULL live, delete most rows and verify shrink,
        // then switch FULL -> NONE via VACUUM.
        let conn2 = Connection::open(&path_str).await.expect("reopen");
        assert_eq!(
            query_i64(&conn2.query("PRAGMA auto_vacuum;").await.unwrap()),
            2
        );
        conn2.execute("PRAGMA auto_vacuum = FULL;").await.unwrap();
        assert_eq!(
            query_i64(&conn2.query("PRAGMA auto_vacuum;").await.unwrap()),
            1
        );

        let pc_before = query_i64(&conn2.query("PRAGMA page_count;").await.unwrap());
        conn2.execute("DELETE FROM t WHERE id > 10;").await.unwrap();
        let pc_after = query_i64(&conn2.query("PRAGMA page_count;").await.unwrap());
        assert!(pc_after < pc_before / 2);

        // FULL -> NONE is deferred until VACUUM.
        conn2.execute("PRAGMA auto_vacuum = NONE;").await.unwrap();
        assert_eq!(
            query_i64(&conn2.query("PRAGMA auto_vacuum;").await.unwrap()),
            1,
            "FULL -> NONE must not deactivate until VACUUM"
        );
        conn2.execute("VACUUM;").await.expect("VACUUM FULL -> NONE");
        assert_eq!(
            query_i64(&conn2.query("PRAGMA auto_vacuum;").await.unwrap()),
            0
        );
        assert_eq!(
            query_text(&conn2.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );
        conn2.close().await.unwrap();

        let (integrity_none, av_none, _, _) = stock_check(&path);
        assert_eq!(integrity_none, "ok");
        assert_eq!(av_none, 0);
        let stock = rusqlite::Connection::open(&path).unwrap();
        let remaining: i64 = stock
            .query_row("SELECT count(*) FROM t;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 10);
    });
}

#[test]
fn test_planted_corrupt_ptrmap_negative_control_caught_by_both_fsqlite_and_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("av_corrupt_control.db");
        let path_str = path.to_string_lossy().into_owned();

        let conn = Connection::open(&path_str).await.expect("open");
        conn.execute("PRAGMA journal_mode = DELETE;")
            .await
            .unwrap();
        conn.execute("PRAGMA auto_vacuum = FULL;").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB);")
            .await
            .unwrap();
        for i in 1..=20_i64 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, zeroblob(1500));"))
                .await
                .unwrap();
        }
        assert_eq!(
            query_text(&conn.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );
        conn.close().await.unwrap();

        let (integrity_clean, av_clean, _, _) = stock_check(&path);
        assert_eq!(integrity_clean, "ok");
        assert_eq!(av_clean, 1);

        // Plant 1: corrupt page 2's ptrmap entry for page 3 (offset 4096 + 0:
        // change PtrMapType::RootPage (1) to PtrMapType::FreePage (2)).
        let original_bytes = std::fs::read(&path).expect("read clean db");
        {
            let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
            let mut b = [0u8; 1];
            f.seek(SeekFrom::Start(4096)).unwrap();
            f.read_exact(&mut b).unwrap();
            assert_eq!(b[0], 1, "page 3 ptrmap entry must start as RootPage (1)");
            f.seek(SeekFrom::Start(4096)).unwrap();
            f.write_all(&[2u8]).unwrap();
            f.sync_all().unwrap();
        }

        let (stock_err, _, _, _) = stock_check(&path);
        assert_ne!(
            stock_err, "ok",
            "stock must catch planted corrupt ptrmap entry"
        );
        {
            let conn_bad = Connection::open(&path_str).await.expect("open corrupt");
            let fsqlite_ic = query_text(&conn_bad.query("PRAGMA integrity_check;").await.unwrap());
            assert_ne!(
                fsqlite_ic, "ok",
                "fsqlite PRAGMA integrity_check must catch planted corrupt ptrmap entry"
            );
            conn_bad.close().await.unwrap();
        }

        // Plant 2: restore clean bytes, then corrupt header largest_root_page (bytes 52..56).
        std::fs::write(&path, &original_bytes).unwrap();
        {
            let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(52)).unwrap();
            f.write_all(&999u32.to_be_bytes()).unwrap();
            f.sync_all().unwrap();
        }

        let (stock_lrp_err, _, _, _) = stock_check(&path);
        assert_ne!(
            stock_lrp_err, "ok",
            "stock must catch planted largest_root_page header mismatch"
        );
        {
            let conn_bad2 = Connection::open(&path_str).await.expect("open lrp corrupt");
            let fsqlite_ic2 =
                query_text(&conn_bad2.query("PRAGMA integrity_check;").await.unwrap());
            assert_ne!(
                fsqlite_ic2, "ok",
                "fsqlite PRAGMA integrity_check must catch planted largest_root_page mismatch"
            );
            conn_bad2.close().await.unwrap();
        }
    });
}

#[test]
fn test_auto_vacuum_concurrent_writers_default_mode_and_wal_truncate() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("av_wal_concurrent.db");
        let path_str = path.to_string_lossy().into_owned();

        let c1 = Connection::open(&path_str).await.expect("open c1");
        assert!(
            c1.is_concurrent_mode_default(),
            "concurrent_mode_default must stay ON by default"
        );
        c1.execute("PRAGMA auto_vacuum = FULL;").await.unwrap();
        c1.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, b BLOB);")
            .await
            .unwrap();
        c1.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, b BLOB);")
            .await
            .unwrap();

        let c2 = Connection::open(&path_str).await.expect("open c2");
        assert!(c2.is_concurrent_mode_default());

        // Interleaved concurrent writes across two connections in WAL mode.
        for i in 1..=80_i64 {
            c1.execute(&format!("INSERT INTO a VALUES ({i}, zeroblob(1500));"))
                .await
                .unwrap();
            c2.execute(&format!("INSERT INTO b VALUES ({i}, zeroblob(1500));"))
                .await
                .unwrap();
        }
        c1.execute("PRAGMA wal_checkpoint(TRUNCATE);").await.unwrap();
        let bytes_peak = std::fs::metadata(&path).unwrap().len();

        // Both connections delete most of their respective tables under FULL auto-vacuum.
        c1.execute("DELETE FROM a WHERE id > 5;").await.unwrap();
        c2.execute("DELETE FROM b WHERE id > 5;").await.unwrap();
        c2.close().await.unwrap();

        c1.execute("PRAGMA wal_checkpoint(TRUNCATE);").await.unwrap();
        let bytes_shrunk = std::fs::metadata(&path).unwrap().len();
        assert!(
            bytes_shrunk < bytes_peak / 2,
            "WAL TRUNCATE checkpoint must physically shrink DB after FULL auto-vacuum ({bytes_peak} -> {bytes_shrunk})"
        );
        assert_eq!(
            query_text(&c1.query("PRAGMA integrity_check;").await.unwrap()),
            "ok"
        );
        c1.close().await.unwrap();

        let (integrity, av, _, fc) = stock_check(&path);
        assert_eq!(integrity, "ok");
        assert_eq!(av, 1);
        assert_eq!(fc, 0);
        let stock = rusqlite::Connection::open(&path).unwrap();
        let ca: i64 = stock
            .query_row("SELECT count(*) FROM a;", [], |r| r.get(0))
            .unwrap();
        let cb: i64 = stock
            .query_row("SELECT count(*) FROM b;", [], |r| r.get(0))
            .unwrap();
        assert_eq!((ca, cb), (5, 5));
    });
}
