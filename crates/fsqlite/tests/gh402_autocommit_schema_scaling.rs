//! GH#402: file-backed autocommit statement cost grows super-linearly with
//! schema size (8 ms -> 320 ms per CREATE at 600 objects; 340 ms per
//! single-row autocommit INSERT; flat-cheap inside a transaction; :memory:
//! unaffected; cliff between 200 and 300 objects).
//!
//! Two surfaces live here:
//!   * `gh402_measure_autocommit_schema_scaling` — measurement harness
//!     (`--ignored`): prints per-window timings plus hot-path counter deltas
//!     so the super-linear term is attributed, not guessed.
//!   * keeper tests — bounded-work assertions that fail if the per-autocommit
//!     cost regresses back to O(schema) re-materialization.

use fsqlite::{Connection, SqliteValue};
use fsqlite_core::connection::{
    hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use std::path::Path;
use std::time::Instant;

const DEFAULT_TABLES: usize = 300; // 600 schema objects: past the reported 200-300 object cliff.
const WINDOW: usize = 50;

fn table_count() -> usize {
    std::env::var("FSQLITE_GH402_TABLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_TABLES)
}

fn file_kb(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len() / 1024).unwrap_or(0)
}

#[derive(Clone, Copy, Default)]
struct Deltas {
    memdb_refresh: u64,
    schema_full_scans: u64,
    parse_ns: u64,
    compile_ns: u64,
    schema_full_reloads: u64,
    schema_light: u64,
    schema_refresh_ns: u64,
    pager_pub: u64,
    begin_setup_ns: u64,
    execute_body_ns: u64,
    commit_pre_txn_ns: u64,
    commit_roundtrip_ns: u64,
    commit_finalize_seq_ns: u64,
    commit_handle_finalize_ns: u64,
    commit_post_maint_ns: u64,
    finalize_post_publish_ns: u64,
    wal_frames_written: u64,
    checkpoint_frames_backfilled: u64,
    checkpoint_duration_us: u64,
}

fn snapshot_deltas() -> Deltas {
    let snap = hot_path_profile_snapshot();
    let wal = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
    Deltas {
        memdb_refresh: snap.memdb_refresh_count,
        schema_full_scans: snap.memdb_txn_schema_full_scans,
        parse_ns: snap.parser.parse_time_ns,
        compile_ns: snap.parser.compile_time_ns,
        schema_full_reloads: snap.prepared_schema_full_reloads,
        schema_light: snap.prepared_schema_lightweight_refreshes,
        schema_refresh_ns: snap.prepared_schema_refresh_time_ns,
        pager_pub: snap.pager_publication_refreshes,
        begin_setup_ns: snap.begin_setup_time_ns,
        execute_body_ns: snap.execute_body_time_ns,
        commit_pre_txn_ns: snap.commit_pre_txn_time_ns,
        commit_roundtrip_ns: snap.commit_txn_roundtrip_time_ns,
        commit_finalize_seq_ns: snap.commit_finalize_seq_time_ns,
        commit_handle_finalize_ns: snap.commit_handle_finalize_time_ns,
        commit_post_maint_ns: snap.commit_post_write_maintenance_time_ns,
        finalize_post_publish_ns: snap.finalize_post_publish_time_ns,
        wal_frames_written: wal.frames_written_total,
        checkpoint_frames_backfilled: wal.checkpoint_frames_backfilled_total,
        checkpoint_duration_us: wal.checkpoint_duration_us_total,
    }
}

#[allow(clippy::cast_precision_loss)]
fn window_report(
    label: &str,
    window_tables: usize,
    elapsed_ms: u128,
    before: &Deltas,
    after: &Deltas,
    db_path: &Path,
) {
    let ms = |a: u64, b: u64| (a - b) / 1_000_000;
    let wal = db_path.with_extension("db-wal");
    println!(
        "[gh402] {label} tables={window_tables:>4} window_ms={elapsed_ms:>7} \
         refresh={}/{}fs parse_ms={} compile_ms={} sched_reload={}f/{}l refresh_ms={} pub={} \
         begin_ms={} body_ms={} commit_ms={}p/{}r/{}s/{}h/{}m/{}pp db_kb={} wal_kb={}",
        after.memdb_refresh - before.memdb_refresh,
        after.schema_full_scans - before.schema_full_scans,
        ms(after.parse_ns, before.parse_ns),
        ms(after.compile_ns, before.compile_ns),
        after.schema_full_reloads - before.schema_full_reloads,
        after.schema_light - before.schema_light,
        ms(after.schema_refresh_ns, before.schema_refresh_ns),
        after.pager_pub - before.pager_pub,
        ms(after.begin_setup_ns, before.begin_setup_ns),
        ms(after.execute_body_ns, before.execute_body_ns),
        ms(after.commit_pre_txn_ns, before.commit_pre_txn_ns),
        ms(after.commit_roundtrip_ns, before.commit_roundtrip_ns),
        ms(after.commit_finalize_seq_ns, before.commit_finalize_seq_ns),
        ms(
            after.commit_handle_finalize_ns,
            before.commit_handle_finalize_ns
        ),
        ms(after.commit_post_maint_ns, before.commit_post_maint_ns),
        ms(
            after.finalize_post_publish_ns,
            before.finalize_post_publish_ns
        ),
        file_kb(db_path),
        file_kb(&wal),
    );
    println!(
        "[gh402] {label} tables={window_tables} wal_frames_written={} checkpoint_frames_backfilled={} checkpoint_us={}",
        after.wal_frames_written - before.wal_frames_written,
        after.checkpoint_frames_backfilled - before.checkpoint_frames_backfilled,
        after.checkpoint_duration_us - before.checkpoint_duration_us,
    );
}

async fn create_schema_autocommit(
    conn: &Connection,
    tables: usize,
    label: &str,
    db_path: &Path,
) -> Vec<u128> {
    let mut window_times = Vec::new();
    let mut window_start = Instant::now();
    let mut before = snapshot_deltas();
    let mut parses_before = conn.schema_reload_parse_count();
    for i in 0..tables {
        conn.execute(&format!(
            "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b REAL, c BLOB);"
        ))
        .await
        .expect("create table");
        conn.execute(&format!("CREATE INDEX idx_t{i}_a ON t{i}(a);"))
            .await
            .expect("create index");
        if (i + 1) % WINDOW == 0 {
            let elapsed = window_start.elapsed().as_millis();
            let after = snapshot_deltas();
            window_report(label, i + 1, elapsed, &before, &after, db_path);
            let parses_after = conn.schema_reload_parse_count();
            println!(
                "[gh402] {label} tables={} stored_schema_parse_delta={}",
                i + 1,
                parses_after - parses_before
            );
            parses_before = parses_after;
            window_times.push(elapsed);
            before = after;
            window_start = Instant::now();
        }
    }
    window_times
}

/// Measurement harness for GH#402. Run explicitly:
/// `cargo test -p fsqlite --test gh402_autocommit_schema_scaling -- --ignored --nocapture`
#[test]
#[ignore = "GH#402 measurement harness; prints timings and counter deltas"]
fn gh402_measure_autocommit_schema_scaling() {
    asupersync::test_utils::run_test(|| async {
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh402_seq.db");
        let conn = Connection::open(path.to_str().unwrap())
            .await
            .expect("open");
        let windows = create_schema_autocommit(&conn, table_count(), "seq", &path).await;
        println!("[gh402] seq window_ms trace: {windows:?}");
        println!(
            "[gh402] seq hydration_count={} after schema build",
            conn.memdb_row_hydration_count()
        );

        // 20 autocommit single-row INSERTs after the schema exists.
        let before = snapshot_deltas();
        let t = Instant::now();
        for _ in 0..20 {
            conn.execute("INSERT INTO t0 (a) VALUES ('x');")
                .await
                .expect("insert");
        }
        let after = snapshot_deltas();
        window_report(
            "autocommit-inserts",
            20,
            t.elapsed().as_millis(),
            &before,
            &after,
            &path,
        );

        // Same 20 INSERTs inside one transaction.
        let before = snapshot_deltas();
        let t = Instant::now();
        conn.execute("BEGIN IMMEDIATE;").await.expect("begin");
        for _ in 0..20 {
            conn.execute("INSERT INTO t0 (a) VALUES ('y');")
                .await
                .expect("insert");
        }
        conn.execute("COMMIT;").await.expect("commit");
        let after = snapshot_deltas();
        window_report(
            "txn-inserts",
            20,
            t.elapsed().as_millis(),
            &before,
            &after,
            &path,
        );

        // Reopen + first statement.
        let t = Instant::now();
        let c2 = Connection::open(path.to_str().unwrap())
            .await
            .expect("reopen");
        let _ = c2.execute("SELECT 1;").await;
        println!(
            "[gh402] reopen+first statement: {} ms",
            t.elapsed().as_millis()
        );

        set_hot_path_profile_enabled(false);
    });
}

/// Current residual matrix: keep storage mode, schema size and transaction
/// shape separate. Timings are observations, never machine-specific pass bars.
#[test]
#[ignore = "GH#402 residual measurement matrix; run explicitly with --nocapture"]
fn gh402_measure_residual_schema_matrix() {
    asupersync::test_utils::run_test(|| async {
        set_hot_path_profile_enabled(true);
        let maximum = table_count().max(6);
        for tables in [maximum / 6, maximum / 2, maximum] {
            for file_backed in [false, true] {
                for ddl_mode in ["seq", "batch", "txn"] {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let path = dir.path().join("residual.db");
                    let target = if file_backed {
                        path.to_str().unwrap()
                    } else {
                        ":memory:"
                    };
                    let label = format!(
                        "residual storage={} mode={ddl_mode} tables={tables}",
                        if file_backed { "file" } else { "memory" },
                    );
                    let conn = Connection::open(target).await.expect("open");
                    reset_hot_path_profile();
                    let started = Instant::now();
                    if ddl_mode == "txn" {
                        conn.execute("BEGIN IMMEDIATE;").await.expect("begin DDL");
                    }
                    if ddl_mode == "batch" {
                        let mut sql = String::new();
                        for i in 0..tables {
                            sql.push_str(&format!(
                                "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b REAL, c BLOB);\n\
                                 CREATE INDEX idx_t{i}_a ON t{i}(a);\n"
                            ));
                        }
                        let before = snapshot_deltas();
                        conn.execute_batch(&sql).await.expect("batch DDL");
                        window_report(
                            &label,
                            tables,
                            started.elapsed().as_millis(),
                            &before,
                            &snapshot_deltas(),
                            &path,
                        );
                    } else {
                        create_schema_autocommit(&conn, tables, &label, &path).await;
                    }
                    if ddl_mode == "txn" {
                        conn.execute("COMMIT;").await.expect("commit DDL");
                    }
                    println!(
                        "[gh402] {label} schema_total_us={} stored_schema_parses={}",
                        started.elapsed().as_micros(),
                        conn.schema_reload_parse_count()
                    );

                    let stock = rusqlite::Connection::open_in_memory().unwrap();
                    for i in 0..tables {
                        stock
                            .execute_batch(&format!(
                                "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b REAL, c BLOB);
                                 CREATE INDEX idx_t{i}_a ON t{i}(a);"
                            ))
                            .unwrap();
                    }
                    for (in_transaction, value) in [(false, "x"), (true, "y")] {
                        let before = snapshot_deltas();
                        let started = Instant::now();
                        if in_transaction {
                            conn.execute("BEGIN IMMEDIATE;")
                                .await
                                .expect("begin INSERTs");
                        }
                        for _ in 0..20 {
                            conn.execute(&format!("INSERT INTO t0 (a) VALUES ('{value}');"))
                                .await
                                .expect("insert");
                        }
                        if in_transaction {
                            conn.execute("COMMIT;").await.expect("commit INSERTs");
                        }
                        window_report(
                            &format!("{label} inserts_txn={in_transaction}"),
                            tables,
                            started.elapsed().as_millis(),
                            &before,
                            &snapshot_deltas(),
                            &path,
                        );
                        for _ in 0..20 {
                            stock
                                .execute("INSERT INTO t0 (a) VALUES (?1)", [value])
                                .unwrap();
                        }
                    }
                    let sql = "SELECT id, a FROM t0 ORDER BY id;";
                    let expected: Vec<(i64, String)> = stock
                        .prepare(sql)
                        .unwrap()
                        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                        .unwrap()
                        .collect::<rusqlite::Result<_>>()
                        .unwrap();
                    let actual = conn.query(sql).await.unwrap();
                    assert_eq!(actual.len(), expected.len());
                    for (actual, (id, value)) in actual.iter().zip(&expected) {
                        assert_eq!(
                            actual.values(),
                            &[
                                SqliteValue::Integer(*id),
                                SqliteValue::Text(value.clone().into())
                            ]
                        );
                    }
                    let count = conn
                        .query_row(
                            "SELECT count(*) FROM sqlite_schema WHERE type IN ('table', 'index');",
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        count.values(),
                        &[SqliteValue::Integer(i64::try_from(tables * 2).unwrap())]
                    );

                    for sample in 0..3 {
                        let started = Instant::now();
                        let stats = conn.memory_stats().expect("memory_stats");
                        println!(
                            "[gh402] {label} sample={sample} memory_stats_us={} cached_pages={} page_size={}",
                            started.elapsed().as_micros(),
                            stats.page_cache.cached_pages,
                            stats.page_size_bytes
                        );
                        if file_backed {
                            let before = snapshot_deltas();
                            let started = Instant::now();
                            let peer = Connection::open(target).await.expect("peer open");
                            let open_us = started.elapsed().as_micros();
                            let started = Instant::now();
                            let row = peer.query_row("SELECT count(*) FROM t0;").await.unwrap();
                            let first_us = started.elapsed().as_micros();
                            assert_eq!(row.values(), &[SqliteValue::Integer(40)]);
                            println!(
                                "[gh402] {label} sample={sample} reopen_us={open_us} first_statement_us={first_us} stored_schema_parses={}",
                                peer.schema_reload_parse_count()
                            );
                            window_report(
                                &format!("{label} reopen-profile"),
                                tables,
                                0,
                                &before,
                                &snapshot_deltas(),
                                &path,
                            );
                            peer.close().await.expect("close peer");
                        }
                    }
                    conn.close().await.expect("close");
                }
            }
        }
        set_hot_path_profile_enabled(false);
    });
}

/// GH#402 keeper: total checkpoint backfill work across a file-backed
/// autocommit DDL loop must stay linear in the frames actually written.
///
/// Pre-fix, every post-commit autocheckpoint restarted from WAL frame 0
/// (`SimplePager::checkpoint` passed `backfilled_frames = 0`) and the trigger
/// keyed on raw WAL length, so once the WAL crossed the adaptive target every
/// autocommit statement re-walked the whole WAL: backfilled-frames grew
/// quadratically (observed ratio >40x at this scale). Post-fix the adapter's
/// generation-tagged watermark resumes where the last checkpoint stopped, so
/// cumulative backfill stays within a small multiple of frames written.
///
/// The counters are process-global, so concurrent tests can only ADD linear
/// noise to both sides of the inequality; the quadratic signature this guards
/// against exceeds the bound by more than an order of magnitude.
#[test]
fn gh402_autocommit_checkpoint_backfill_work_is_linear() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh402_keeper.db");
        let conn = Connection::open(path.to_str().unwrap())
            .await
            .expect("open");

        let before = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();
        const KEEPER_TABLES: usize = 250; // 500 objects: safely past the cliff.
        for i in 0..KEEPER_TABLES {
            conn.execute(&format!(
                "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b REAL, c BLOB);"
            ))
            .await
            .expect("create table");
            conn.execute(&format!("CREATE INDEX idx_t{i}_a ON t{i}(a);"))
                .await
                .expect("create index");
        }
        let after = fsqlite_wal::GLOBAL_WAL_METRICS.snapshot();

        let written = after
            .frames_written_total
            .saturating_sub(before.frames_written_total);
        let backfilled = after
            .checkpoint_frames_backfilled_total
            .saturating_sub(before.checkpoint_frames_backfilled_total);
        println!(
            "[gh402-keeper] frames_written_delta={written} checkpoint_backfilled_delta={backfilled}"
        );
        assert!(
            backfilled <= written.saturating_mul(3).saturating_add(4_000),
            "checkpoint backfill work is super-linear again (GH#402): \
             {backfilled} frames backfilled for {written} frames written — \
             autocheckpoints are re-walking the whole WAL per autocommit statement"
        );
        conn.close().await.expect("close");
    });
}

/// GH#402 keeper: the checkpoint-scheduling change must not weaken
/// cross-connection schema visibility. Connection A holds warm caches; B (a
/// separate connection on the same file) commits DDL + a row; A must observe
/// both without any manual refresh — the schema-cookie / visible-commit-seq
/// staleness check is the guard that any skip-refresh fast path has to pass.
#[test]
fn gh402_cross_connection_schema_change_remains_visible() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh402_visibility.db");
        let path = path.to_str().unwrap();

        let conn_a = Connection::open(path).await.expect("open A");
        conn_a
            .execute("CREATE TABLE seed (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("seed table");
        conn_a
            .execute("INSERT INTO seed (v) VALUES ('warm');")
            .await
            .expect("seed row");
        // Warm A's prepared/schema caches.
        let rows = conn_a
            .query("SELECT v FROM seed;")
            .await
            .expect("warm read");
        assert_eq!(rows.len(), 1);

        let conn_b = Connection::open(path).await.expect("open B");
        conn_b
            .execute("CREATE TABLE from_b (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .expect("B ddl");
        conn_b
            .execute("INSERT INTO from_b (v) VALUES ('peer');")
            .await
            .expect("B row");

        // A must see B's committed schema object and its row.
        let rows = conn_a
            .query("SELECT v FROM from_b;")
            .await
            .expect("A must see B's new table");
        assert_eq!(rows.len(), 1, "A must see B's committed row");
        conn_b.close().await.expect("close B");
        conn_a.close().await.expect("close A");
    });
}

/// PR#401 invariant keeper (reimplemented in-house, GH#402 companion): a
/// schema-only open never bulk-hydrates file rows into `MemDatabase` through
/// the prepared-query MemDB fast path, and the read-only variant still
/// refuses writes. The gate derives from the open mode (the schema-only
/// family), not a caller-set flag.
#[test]
fn gh402_schema_only_prepared_reads_stay_pager_backed() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("gh402_schema_only.db");
        let path = path.to_str().unwrap();

        // Build a small canonical table with an index through a normal open.
        let writer = Connection::open(path).await.expect("open writer");
        writer
            .execute("CREATE TABLE canon (id INTEGER PRIMARY KEY, k TEXT NOT NULL, v TEXT);")
            .await
            .expect("ddl");
        writer
            .execute("CREATE INDEX idx_canon_k ON canon(k);")
            .await
            .expect("index");
        writer.execute("BEGIN;").await.expect("begin");
        for i in 0..64 {
            writer
                .execute(&format!(
                    "INSERT INTO canon (k, v) VALUES ('k{i}', 'v{i}');"
                ))
                .await
                .expect("insert");
        }
        writer.execute("COMMIT;").await.expect("commit");
        writer.close().await.expect("close writer");

        // Read-only schema-only open: parameterized prepared lookup must
        // answer from the pager without hydrating MemDatabase rows.
        let reader = Connection::open_schema_only(path)
            .await
            .expect("schema-only open");
        let stmt = reader
            .prepare("SELECT v FROM canon WHERE k = ?1;")
            .await
            .expect("prepare");
        for i in [3_usize, 41, 3] {
            let rows = stmt
                .query_with_params(&[SqliteValue::from(format!("k{i}"))])
                .await
                .expect("prepared lookup");
            assert_eq!(rows.len(), 1, "lookup k{i} must find its row");
        }
        assert_eq!(
            reader.memdb_row_hydration_count(),
            0,
            "schema-only prepared reads must stay pager-backed \
             (PR#401 invariant): the MemDB fast path bulk-hydrated the file"
        );
        assert!(
            reader
                .execute("INSERT INTO canon (k, v) VALUES ('nope', 'nope');")
                .await
                .is_err(),
            "read-only schema-only open must refuse writes"
        );
        reader.close().await.expect("close reader");
    });
}
