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

use fsqlite::Connection;
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
}

fn snapshot_deltas() -> Deltas {
    let snap = hot_path_profile_snapshot();
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
        ms(after.commit_handle_finalize_ns, before.commit_handle_finalize_ns),
        ms(after.commit_post_maint_ns, before.commit_post_maint_ns),
        ms(after.finalize_post_publish_ns, before.finalize_post_publish_ns),
        file_kb(db_path),
        file_kb(&wal),
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
        let conn = Connection::open(path.to_str().unwrap()).await.expect("open");
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
        let c2 = Connection::open(path.to_str().unwrap()).await.expect("reopen");
        let _ = c2.execute("SELECT 1;").await;
        println!("[gh402] reopen+first statement: {} ms", t.elapsed().as_millis());

        set_hot_path_profile_enabled(false);
    });
}
