//! GH #302 (bd-gh302-continuous-overlap-freelist-reuse-i5tx4): snapshot-safe
//! committed-freelist reuse under continuous reader/writer overlap.
//!
//! Three keepers:
//! 1. `test_gh302_page_count_bounded_under_continuous_overlap` — a reader
//!    transaction stays open across the whole churn run while a writer
//!    repeatedly deletes and reinserts a fixed-size working set. The database
//!    must reach a bounded page_count after warm-up (committed-free pages are
//!    reused instead of forcing unbounded EOF growth), the held reader must
//!    keep its original snapshot, and the final file must pass stock SQLite
//!    integrity_check.
//! 2. `test_gh302_external_stock_reader_keeps_bytes_while_pages_reused` — a
//!    stock SQLite (rusqlite) reader holds a read transaction at an old WAL
//!    mark while the fsqlite writer frees pages and reuses them below
//!    db_size. The external reader must keep observing its original bytes.
//! 3. `test_gh302_page_count_bounded_under_racing_writer_churn` — four writer
//!    connections race delete/reinsert cycles over disjoint ranges of a
//!    fixed-size working set while a reader snapshot stays held. Racing
//!    begins routinely miss the sole-current-snapshot fast path, so this is
//!    the shape where denied freelist reuse turns into unbounded EOF growth.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

async fn query_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.unwrap_or_else(|error| {
        panic!("query failed: {sql}: {error:?}");
    });
    match rows.first().map(|row| row.values()[0].clone()) {
        Some(SqliteValue::Integer(value)) => value,
        other => panic!("expected integer result for {sql}, got {other:?}"),
    }
}

const ROWS: i64 = 200;
const PAYLOAD_LEN: usize = 200;

async fn seed_database(conn: &Connection) {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;\n         PRAGMA busy_timeout=5000;\n         CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT);",
    )
    .await
    .expect("schema setup");
    conn.execute("BEGIN CONCURRENT;").await.expect("seed begin");
    for id in 0..ROWS {
        let payload = format!("seed_{id}_{}", "x".repeat(PAYLOAD_LEN));
        conn.execute(&format!("INSERT INTO t VALUES ({id}, '{payload}');"))
            .await
            .expect("seed insert");
    }
    conn.execute("COMMIT;").await.expect("seed commit");
}

#[test]
fn test_gh302_page_count_bounded_under_continuous_overlap() {
    asupersync::test_utils::run_test(|| async {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("gh302_overlap.db");
        let db = db_path.to_string_lossy().into_owned();

        let writer = Connection::open(&db).await.expect("open writer");
        seed_database(&writer).await;

        // Reader binds a snapshot BEFORE any churn and holds it for the whole
        // run — the sustained-overlap condition from the issue.
        let reader = Connection::open(&db).await.expect("open reader");
        reader
            .execute_batch("PRAGMA busy_timeout=5000;")
            .await
            .expect("reader pragmas");
        reader
            .execute("BEGIN CONCURRENT;")
            .await
            .expect("reader begin");
        let reader_count_before = query_i64(&reader, "SELECT COUNT(*) FROM t;").await;
        assert_eq!(reader_count_before, ROWS, "reader must see the seed rows");

        const CYCLES: usize = 50;
        const WARMUP: usize = 10;
        // Allow small drift for freelist-trunk reshaping; growth beyond this
        // means committed-free pages were not reused.
        const SLACK_PAGES: i64 = 6;

        let mut warmup_page_count = 0_i64;
        for cycle in 0..CYCLES {
            // Delete half the working set, commit (frees pages durably)...
            let deleted_and_reinserted = async |low: i64, high: i64| {
                loop {
                    match writer.execute("BEGIN CONCURRENT;").await {
                        Ok(_) => {}
                        Err(error) if error.is_transient() => continue,
                        Err(error) => panic!("begin failed: {error:?}"),
                    }
                    let outcome = async {
                        writer
                            .execute(&format!("DELETE FROM t WHERE id >= {low} AND id < {high};"))
                            .await?;
                        writer.execute("COMMIT;").await
                    }
                    .await;
                    match outcome {
                        Ok(_) => break,
                        Err(error) if error.is_transient() => {
                            if writer.in_transaction() {
                                let _ = writer.execute("ROLLBACK;").await;
                            }
                        }
                        Err(error) => panic!("delete cycle failed: {error:?}"),
                    }
                }
                loop {
                    match writer.execute("BEGIN CONCURRENT;").await {
                        Ok(_) => {}
                        Err(error) if error.is_transient() => continue,
                        Err(error) => panic!("begin failed: {error:?}"),
                    }
                    let outcome = async {
                        for id in low..high {
                            let payload = format!("cycle_{id}_{}", "y".repeat(PAYLOAD_LEN));
                            writer
                                .execute(&format!("INSERT INTO t VALUES ({id}, '{payload}');"))
                                .await?;
                        }
                        writer.execute("COMMIT;").await
                    }
                    .await;
                    match outcome {
                        Ok(_) => break,
                        Err(error) if error.is_transient() => {
                            if writer.in_transaction() {
                                let _ = writer.execute("ROLLBACK;").await;
                            }
                        }
                        Err(error) => panic!("insert cycle failed: {error:?}"),
                    }
                }
            };
            let (low, high) = if cycle % 2 == 0 {
                (0, ROWS / 2)
            } else {
                (ROWS / 2, ROWS)
            };
            deleted_and_reinserted(low, high).await;

            let page_count = query_i64(&writer, "PRAGMA page_count;").await;
            let freelist_count = query_i64(&writer, "PRAGMA freelist_count;").await;
            eprintln!(
                "gh302 cycle={cycle} page_count={page_count} freelist_count={freelist_count}"
            );
            if cycle + 1 == WARMUP {
                warmup_page_count = page_count;
            }
        }

        let final_page_count = query_i64(&writer, "PRAGMA page_count;").await;
        assert!(
            warmup_page_count > 0,
            "warm-up page count must have been sampled"
        );
        assert!(
            final_page_count <= warmup_page_count + SLACK_PAGES,
            "page_count must stay bounded under continuous overlap: \
             warmup={warmup_page_count} final={final_page_count} (slack {SLACK_PAGES})"
        );

        // The held reader keeps its original snapshot across the entire run.
        let reader_count_after = query_i64(&reader, "SELECT COUNT(*) FROM t;").await;
        assert_eq!(
            reader_count_after, ROWS,
            "held reader must keep observing its original snapshot"
        );
        let reader_seed_rows =
            query_i64(&reader, "SELECT COUNT(*) FROM t WHERE data LIKE 'seed_%';").await;
        assert_eq!(
            reader_seed_rows, ROWS,
            "held reader must still see the ORIGINAL (seed) row bytes"
        );
        reader.execute("ROLLBACK;").await.expect("reader end");

        let writer_count = query_i64(&writer, "SELECT COUNT(*) FROM t;").await;
        assert_eq!(writer_count, ROWS, "writer sees the churned working set");

        writer.close().await.expect("close writer");
        reader.close().await.expect("close reader");

        // Stock SQLite is the integrity oracle for the durable image.
        let oracle = rusqlite::Connection::open(&db_path).expect("oracle open");
        let integrity: String = oracle
            .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
            .expect("oracle integrity_check");
        assert_eq!(integrity, "ok", "stock integrity_check must pass");
        let oracle_rows: i64 = oracle
            .query_row("SELECT COUNT(*) FROM t;", [], |row| row.get(0))
            .expect("oracle count");
        assert_eq!(oracle_rows, ROWS);
    });
}

#[test]
fn test_gh302_external_stock_reader_keeps_bytes_while_pages_reused() {
    asupersync::test_utils::run_test(|| async {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("gh302_external_reader.db");
        let db = db_path.to_string_lossy().into_owned();

        let writer = Connection::open(&db).await.expect("open writer");
        seed_database(&writer).await;
        // Make sure the seed is visible to an external stock reader.
        let seeded = query_i64(&writer, "SELECT COUNT(*) FROM t;").await;
        assert_eq!(seeded, ROWS);

        // External stock SQLite reader binds a read snapshot at the pre-churn
        // WAL mark and holds it.
        let external = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("external reader open");
        external
            .execute_batch("BEGIN;")
            .expect("external reader begin");
        let external_before: (i64, i64) = external
            .query_row(
                "SELECT COUNT(*), COUNT(CASE WHEN data LIKE 'seed_%' THEN 1 END) FROM t;",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("external reader initial read");
        assert_eq!(external_before, (ROWS, ROWS));

        // Free pages durably...
        writer.execute("BEGIN CONCURRENT;").await.expect("begin");
        writer
            .execute(&format!("DELETE FROM t WHERE id < {};", ROWS / 2))
            .await
            .expect("delete");
        writer.execute("COMMIT;").await.expect("commit delete");
        let page_count_after_delete = query_i64(&writer, "PRAGMA page_count;").await;
        let freelist_after_delete = query_i64(&writer, "PRAGMA freelist_count;").await;
        assert!(
            freelist_after_delete > 0,
            "delete must free pages: freelist_count={freelist_after_delete}"
        );

        // ...then reinsert an equal volume: the allocator must reuse the
        // committed-free pages below db_size instead of growing the file.
        writer.execute("BEGIN CONCURRENT;").await.expect("begin");
        for id in 0..(ROWS / 2) {
            let payload = format!("reuse_{id}_{}", "z".repeat(PAYLOAD_LEN));
            writer
                .execute(&format!("INSERT INTO t VALUES ({id}, '{payload}');"))
                .await
                .expect("reinsert");
        }
        writer.execute("COMMIT;").await.expect("commit reinsert");
        let page_count_after_reuse = query_i64(&writer, "PRAGMA page_count;").await;
        eprintln!(
            "gh302 external: after_delete={page_count_after_delete} \
             freelist={freelist_after_delete} after_reuse={page_count_after_reuse}"
        );
        assert!(
            page_count_after_reuse <= page_count_after_delete,
            "reinsert must reuse committed-free pages, not grow the file: \
             {page_count_after_delete} -> {page_count_after_reuse}"
        );

        // The external reader at the old mark still observes its ORIGINAL
        // bytes even though the pages holding them were freed and reused.
        let external_after: (i64, i64) = external
            .query_row(
                "SELECT COUNT(*), COUNT(CASE WHEN data LIKE 'seed_%' THEN 1 END) FROM t;",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("external reader re-read");
        assert_eq!(
            external_after,
            (ROWS, ROWS),
            "external stock reader must keep its original snapshot bytes"
        );
        external.execute_batch("COMMIT;").expect("external end");
        drop(external);

        writer.close().await.expect("close writer");

        let oracle = rusqlite::Connection::open(&db_path).expect("oracle open");
        let integrity: String = oracle
            .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
            .expect("oracle integrity_check");
        assert_eq!(integrity, "ok", "stock integrity_check must pass");
    });
}

#[test]
fn test_gh302_page_count_bounded_under_racing_writer_churn() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("gh302_racing_churn.db");
    let db = db_path.to_string_lossy().into_owned();

    asupersync::test_utils::run_test(|| async {
        let setup = Connection::open(&db).await.expect("open setup");
        seed_database(&setup).await;
        setup.close().await.expect("close setup");
    });

    const WRITERS: i64 = 4;
    const CYCLES: usize = 25;
    let rows_per_writer = ROWS / WRITERS;

    // Reader holds a snapshot across the entire racing run on its own OS
    // thread (its runtime stays alive while the writers churn).
    let reader_thread = {
        let db = db.clone();
        let (bound_tx, bound_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let reader = Connection::open(&db).await.expect("open reader");
                reader
                    .execute("BEGIN CONCURRENT;")
                    .await
                    .expect("reader begin");
                let before = query_i64(&reader, "SELECT COUNT(*) FROM t;").await;
                assert_eq!(before, ROWS, "reader must bind the seed snapshot");
                bound_tx.send(()).expect("signal snapshot bound");
                done_rx.recv().expect("await writer completion");
                let after = query_i64(&reader, "SELECT COUNT(*) FROM t;").await;
                assert_eq!(
                    after, ROWS,
                    "held reader must keep its original snapshot across racing churn"
                );
                reader.execute("ROLLBACK;").await.expect("reader end");
                reader.close().await.expect("close reader");
            });
        });
        bound_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("reader snapshot must bind");
        (handle, done_tx)
    };

    let mut writer_handles = Vec::new();
    for writer_id in 0..WRITERS {
        let db = db.clone();
        writer_handles.push(std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&db).await.expect("open writer");
                conn.execute_batch("PRAGMA busy_timeout=5000;")
                    .await
                    .expect("writer pragmas");
                let low = writer_id * rows_per_writer;
                let high = low + rows_per_writer;
                for cycle in 0..CYCLES {
                    let run_txn = async |sql_batch: Vec<String>| {
                        loop {
                            if conn.in_transaction() {
                                let _ = conn.execute("ROLLBACK;").await;
                            }
                            match conn.execute("BEGIN CONCURRENT;").await {
                                Ok(_) => {}
                                Err(error) if error.is_transient() => continue,
                                Err(error) => panic!("begin failed: {error:?}"),
                            }
                            let mut failed_transient = false;
                            for sql in &sql_batch {
                                match conn.execute(sql).await {
                                    Ok(_) => {}
                                    Err(error) if error.is_transient() => {
                                        failed_transient = true;
                                        break;
                                    }
                                    Err(error) => panic!("statement failed: {error:?}"),
                                }
                            }
                            if failed_transient {
                                continue;
                            }
                            match conn.execute("COMMIT;").await {
                                Ok(_) => break,
                                Err(error) if error.is_transient() => {}
                                Err(error) => panic!("commit failed: {error:?}"),
                            }
                        }
                    };
                    run_txn(vec![format!(
                        "DELETE FROM t WHERE id >= {low} AND id < {high};"
                    )])
                    .await;
                    let inserts: Vec<String> = (low..high)
                        .map(|id| {
                            format!(
                                "INSERT INTO t VALUES ({id}, 'w{writer_id}_c{cycle}_{}');",
                                "q".repeat(PAYLOAD_LEN)
                            )
                        })
                        .collect();
                    run_txn(inserts).await;
                }
                conn.close().await.expect("close writer");
            });
        }));
    }
    for handle in writer_handles {
        handle.join().expect("writer thread");
    }

    let (reader_handle, done_tx) = reader_thread;
    done_tx.send(()).expect("release reader");
    reader_handle.join().expect("reader thread");

    asupersync::test_utils::run_test(|| async {
        let verify = Connection::open(&db).await.expect("open verify");
        let page_count = query_i64(&verify, "PRAGMA page_count;").await;
        let freelist_count = query_i64(&verify, "PRAGMA freelist_count;").await;
        let rows = query_i64(&verify, "SELECT COUNT(*) FROM t;").await;
        eprintln!(
            "gh302 racing: final page_count={page_count} freelist_count={freelist_count} rows={rows}"
        );
        assert_eq!(rows, ROWS, "the working set is fixed-size");
        // The steady-state working set needs ~19 pages (measured by the
        // sequential keeper above). Denied reuse under racing churn grows the
        // file far beyond that; bounded reuse keeps it in the same ballpark.
        // The bound is deliberately generous: it fails on unbounded growth,
        // not on modest transient bloat.
        assert!(
            page_count <= 60,
            "page_count must stay bounded under racing overlap churn: {page_count}"
        );
        verify.close().await.expect("close verify");
    });

    let oracle = rusqlite::Connection::open(&db_path).expect("oracle open");
    let integrity: String = oracle
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("oracle integrity_check");
    assert_eq!(integrity, "ok", "stock integrity_check must pass");
}

/// GH#302 acceptance #3: committed-free pages must stay reusable across a
/// CRASH/REOPEN boundary — recovery rebuilds the freelist from durable state,
/// and a post-recovery writer reuses those pages instead of growing the file.
#[test]
fn test_gh302_freelist_reuse_survives_crash_reopen() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("gh302_crash_reopen.db");
    let db = db_path.to_string_lossy().into_owned();

    // Phase 1: seed, free pages durably, then simulate a crash by DROPPING
    // the connection without close() — no checkpoint, no clean shutdown; the
    // committed state lives in the WAL.
    let page_count_after_delete = {
        let db = db.clone();
        let mut result = 0_i64;
        asupersync::test_utils::run_test(|| async {
            let writer = Connection::open(&db).await.expect("open writer");
            seed_database(&writer).await;
            writer
                .execute("BEGIN CONCURRENT;")
                .await
                .expect("begin delete");
            writer
                .execute(&format!("DELETE FROM t WHERE id < {};", ROWS / 2))
                .await
                .expect("delete");
            writer.execute("COMMIT;").await.expect("commit delete");
            let freelist_count = query_i64(&writer, "PRAGMA freelist_count;").await;
            assert!(
                freelist_count > 0,
                "delete must free pages durably before the crash: {freelist_count}"
            );
            result = query_i64(&writer, "PRAGMA page_count;").await;
            drop(writer); // crash: no close(), no checkpoint
        });
        result
    };

    // Phase 2: reopen (recovery), reinsert the same volume — the recovered
    // freelist must satisfy the allocation without growing the file.
    asupersync::test_utils::run_test(|| async {
        let writer = Connection::open(&db).await.expect("reopen after crash");
        let recovered_rows = query_i64(&writer, "SELECT COUNT(*) FROM t;").await;
        assert_eq!(
            recovered_rows,
            ROWS / 2,
            "recovery must keep committed state"
        );
        writer
            .execute("BEGIN CONCURRENT;")
            .await
            .expect("begin reinsert");
        for id in 0..(ROWS / 2) {
            let payload = format!("recover_{id}_{}", "r".repeat(PAYLOAD_LEN));
            writer
                .execute(&format!("INSERT INTO t VALUES ({id}, '{payload}');"))
                .await
                .expect("reinsert");
        }
        writer.execute("COMMIT;").await.expect("commit reinsert");
        let page_count_after_reuse = query_i64(&writer, "PRAGMA page_count;").await;
        eprintln!(
            "gh302 crash-reopen: before={page_count_after_delete} after={page_count_after_reuse}"
        );
        assert!(
            page_count_after_reuse <= page_count_after_delete,
            "post-recovery reinsert must reuse recovered free pages, not grow: \
             {page_count_after_delete} -> {page_count_after_reuse}"
        );
        writer.close().await.expect("close writer");
    });

    let oracle = rusqlite::Connection::open(&db_path).expect("oracle open");
    let integrity: String = oracle
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("oracle integrity_check");
    assert_eq!(
        integrity, "ok",
        "stock integrity_check must pass after crash-reopen reuse"
    );
}

/// GH#302 acceptance #3: freelist reuse must be exact across a CHECKPOINT
/// boundary — pages freed before the checkpoint land in the main file's
/// durable freelist metadata and stay reusable after it, with the stock
/// oracle green on the checkpointed image.
#[test]
fn test_gh302_freelist_reuse_across_checkpoint_boundary() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("gh302_checkpoint_boundary.db");
    let db = db_path.to_string_lossy().into_owned();

    asupersync::test_utils::run_test(|| async {
        let writer = Connection::open(&db).await.expect("open writer");
        seed_database(&writer).await;

        writer
            .execute("BEGIN CONCURRENT;")
            .await
            .expect("begin delete");
        writer
            .execute(&format!("DELETE FROM t WHERE id < {};", ROWS / 2))
            .await
            .expect("delete");
        writer.execute("COMMIT;").await.expect("commit delete");
        let freelist_before = query_i64(&writer, "PRAGMA freelist_count;").await;
        assert!(
            freelist_before > 0,
            "delete must free pages: {freelist_before}"
        );

        // Checkpoint the freed state into the main database file. The pragma
        // returns a result row (busy/log/checkpointed), so drive it as a
        // query like the prepared-pragma dispatch tests do.
        let _ = writer
            .query("PRAGMA wal_checkpoint(FULL);")
            .await
            .expect("checkpoint after delete");
        let page_count_at_checkpoint = query_i64(&writer, "PRAGMA page_count;").await;
        let freelist_at_checkpoint = query_i64(&writer, "PRAGMA freelist_count;").await;
        assert_eq!(
            freelist_at_checkpoint, freelist_before,
            "checkpoint must carry the durable freelist across the boundary"
        );

        // Reinsert the same volume: reuse must come from the checkpointed
        // freelist without growing the file.
        writer
            .execute("BEGIN CONCURRENT;")
            .await
            .expect("begin reinsert");
        for id in 0..(ROWS / 2) {
            let payload = format!("ckpt_{id}_{}", "c".repeat(PAYLOAD_LEN));
            writer
                .execute(&format!("INSERT INTO t VALUES ({id}, '{payload}');"))
                .await
                .expect("reinsert");
        }
        writer.execute("COMMIT;").await.expect("commit reinsert");
        let page_count_after_reuse = query_i64(&writer, "PRAGMA page_count;").await;
        eprintln!(
            "gh302 checkpoint-boundary: at_ckpt={page_count_at_checkpoint} \
             freelist_at_ckpt={freelist_at_checkpoint} after={page_count_after_reuse}"
        );
        assert!(
            page_count_after_reuse <= page_count_at_checkpoint,
            "post-checkpoint reinsert must reuse checkpointed free pages, not grow: \
             {page_count_at_checkpoint} -> {page_count_after_reuse}"
        );

        // Second checkpoint so the stock oracle reads a fully materialized
        // main file as well as the WAL tail.
        let _ = writer
            .query("PRAGMA wal_checkpoint(FULL);")
            .await
            .expect("checkpoint after reuse");
        writer.close().await.expect("close writer");
    });

    let oracle = rusqlite::Connection::open(&db_path).expect("oracle open");
    let integrity: String = oracle
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("oracle integrity_check");
    assert_eq!(
        integrity, "ok",
        "stock integrity_check must pass on the checkpointed image"
    );
    let rows: i64 = oracle
        .query_row("SELECT COUNT(*) FROM t;", [], |row| row.get(0))
        .expect("oracle count");
    assert_eq!(rows, ROWS);
}
