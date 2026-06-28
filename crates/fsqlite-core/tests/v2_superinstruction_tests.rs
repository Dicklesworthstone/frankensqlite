//! V2 superinstruction fusion + ceremony reduction regression tests.
//!
//! Verifies correctness after V2.1 (FusedAppendInsert), V2.2
//! (FusedOpenWriteLast, currently disabled), and V2.3 (bounds check +
//! metrics gating) optimizations.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use fsqlite_vdbe::engine::{
    VdbeMetricsSnapshot, reset_vdbe_metrics, set_vdbe_metrics_enabled, vdbe_metrics_snapshot,
};
use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::tempdir;

fn new_mem_conn() -> Connection {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("PRAGMA journal_mode=WAL").ok();
    conn
}

fn new_file_conn(path: &str) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode=WAL").ok();
    conn
}

fn explicit_row_insert_sql(table: &str, rowids: &[i64]) -> String {
    let values = rowids
        .iter()
        .map(|rowid| format!("({rowid}, 'v{rowid}')"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} VALUES {values}")
}

fn v2_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn v2_test_guard() -> MutexGuard<'static, ()> {
    v2_test_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn capture_vdbe_metrics<T>(
    f: impl FnOnce() -> T,
) -> (T, fsqlite_vdbe::engine::VdbeMetricsSnapshot) {
    set_vdbe_metrics_enabled(true);
    reset_vdbe_metrics();
    let result = f();
    let snapshot = vdbe_metrics_snapshot();
    reset_vdbe_metrics();
    set_vdbe_metrics_enabled(false);
    (result, snapshot)
}

fn log_track_t_metrics(scenario: &str, metrics: &VdbeMetricsSnapshot) {
    eprintln!(
        "INFO track=T scenario={scenario} append_count={} seek_count={} append_hint_clear_count={} make_record_calls_total={}",
        metrics.insert_append_count,
        metrics.insert_seek_count,
        metrics.insert_append_hint_clear_count,
        metrics.make_record_calls_total,
    );
}

// ── V2.1: FusedAppendInsert correctness ─────────────────────────────────

#[test]
fn test_v2_fused_insert_simple_1col() {
    let _guard = v2_test_guard();
    // Simple INSERT that should trigger the FusedAppendInsert peephole
    // (NewRowid + MakeRecord + Insert with ABORT conflict, no indexes)
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=1000 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 1000);

    let max = conn.query_row("SELECT MAX(id) FROM t").unwrap();
    assert_eq!(max.values()[0].to_integer(), 1000);

    let min = conn.query_row("SELECT MIN(id) FROM t").unwrap();
    assert_eq!(min.values()[0].to_integer(), 1);
}

#[test]
fn test_v2_fused_insert_multicol_varied_types() {
    let _guard = v2_test_guard();
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=500 {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'user_{i}', {}.5, X'DEADBEEF')",
            i
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 500);

    let row = conn
        .query_row("SELECT name, score FROM t WHERE id = 250")
        .unwrap();
    assert_eq!(row.values()[0].to_text(), "user_250");
    assert!((row.values()[1].to_float() - 250.5).abs() < 0.001);
}

#[test]
fn test_v2_fused_insert_autoincrement() {
    let _guard = v2_test_guard();
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=100 {
        conn.execute(&format!("INSERT INTO t (val) VALUES ('item_{i}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 100);

    // Verify monotonically increasing rowids
    let max = conn.query_row("SELECT MAX(id) FROM t").unwrap();
    assert!(max.values()[0].to_integer() >= 100);
}

#[test]
fn test_v2_fused_insert_empty_table_first_row() {
    let _guard = v2_test_guard();
    // First INSERT into empty table — no cached last_alloc_rowid
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'first')").unwrap();

    let row = conn.query_row("SELECT id, val FROM t").unwrap();
    assert_eq!(row.values()[0].to_integer(), 1);
    assert_eq!(row.values()[1].to_text(), "first");
}

#[test]
fn test_v2_sequential_explicit_rowid_inserts_keep_append_path_hot() {
    let _guard = v2_test_guard();
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    let rowids: Vec<i64> = (1..=128).collect();
    let insert_sql = explicit_row_insert_sql("t", &rowids);

    let (_result, metrics) = capture_vdbe_metrics(|| {
        conn.execute(&insert_sql).unwrap();
    });

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 128);
    log_track_t_metrics("sequential_explicit_rowids", &metrics);

    assert!(
        metrics.insert_append_count >= 120,
        "sequential explicit-rowid inserts should stay on the append path after the initial seed insert, got {:?}",
        metrics
    );
    assert!(
        metrics.insert_seek_count <= 8,
        "sequential explicit-rowid inserts should avoid repeated existence seeks, got {:?}",
        metrics
    );
    assert_eq!(
        metrics.insert_append_hint_clear_count, 0,
        "sequential explicit-rowid inserts should not clear the append hint, got {:?}",
        metrics
    );
}

#[test]
fn test_v2_plain_execute_sequential_inserts_keep_append_path_hot_across_statements() {
    let _guard = v2_test_guard();
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")
        .unwrap();

    let (_result, metrics) = capture_vdbe_metrics(|| {
        conn.execute("BEGIN").unwrap();
        for rowid in 1..=128_i64 {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({rowid}, lower('V{rowid}'), abs(-{rowid}))"
            ))
            .unwrap();
        }
        conn.execute("COMMIT").unwrap();
    });

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 128);
    log_track_t_metrics("plain_execute_sequential_across_statements", &metrics);

    assert!(
        metrics.insert_append_count >= 120,
        "repeated reusable-lane INSERT statements should reuse the append path across statements, got {:?}",
        metrics
    );
    assert!(
        metrics.insert_seek_count <= 8,
        "repeated reusable-lane INSERT statements should avoid repeated existence seeks, got {:?}",
        metrics
    );
    assert_eq!(
        metrics.insert_append_hint_clear_count, 0,
        "repeated reusable-lane INSERT statements should preserve the append hint across statements, got {:?}",
        metrics
    );
}

#[test]
fn test_v2_midstream_insert_clears_append_hint_until_right_edge_reestablished() {
    let _guard = v2_test_guard();
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let (_result, metrics) = capture_vdbe_metrics(|| {
        conn.execute(
            "INSERT INTO t VALUES \
             (10, 'ten'), \
             (30, 'thirty'), \
             (20, 'twenty'), \
             (21, 'twenty_one')",
        )
        .unwrap();
    });

    let rows = conn.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.values()[0].to_integer())
            .collect::<Vec<_>>(),
        vec![10, 20, 21, 30]
    );
    log_track_t_metrics("midstream_gap_fallback", &metrics);

    assert_eq!(
        metrics.insert_append_count, 1,
        "only the proven right-edge insert should use the append no-seek path; midstream inserts must clear the hint, got {:?}",
        metrics
    );
    assert!(
        metrics.insert_seek_count >= 3,
        "midstream inserts should force conservative seeks until the right edge is proven again, got {:?}",
        metrics
    );
    assert!(
        metrics.insert_append_hint_clear_count >= 1,
        "midstream inserts should clear the cached append hint, got {:?}",
        metrics
    );
}

#[test]
fn test_v2_append_path_with_concurrent_mode_on_disjoint_tables() {
    let _guard = v2_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("track_t_append_concurrent.db");
    let db_path = db_path.to_string_lossy().into_owned();

    let setup = new_file_conn(&db_path);
    setup
        .execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    setup
        .execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    drop(setup);

    let conn1 = new_file_conn(&db_path);
    let conn2 = new_file_conn(&db_path);
    conn1.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();
    conn2.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();

    let rowids: Vec<i64> = (1..=128).collect();
    let insert_t1 = explicit_row_insert_sql("t1", &rowids);
    let insert_t2 = explicit_row_insert_sql("t2", &rowids);

    let (_result, metrics) = capture_vdbe_metrics(|| {
        conn1.execute("BEGIN CONCURRENT;").unwrap();
        conn2.execute("BEGIN CONCURRENT;").unwrap();
        conn1.execute(&insert_t1).unwrap();
        conn2.execute(&insert_t2).unwrap();
        conn1.execute("COMMIT;").unwrap();
        conn2.execute("COMMIT;").unwrap();
    });
    log_track_t_metrics("concurrent_mode_disjoint_tables", &metrics);

    let verify = new_file_conn(&db_path);
    let count_t1 = verify.query_row("SELECT COUNT(*) FROM t1").unwrap();
    let count_t2 = verify.query_row("SELECT COUNT(*) FROM t2").unwrap();
    assert_eq!(count_t1.values()[0].to_integer(), 128);
    assert_eq!(count_t2.values()[0].to_integer(), 128);
    assert!(
        metrics.insert_append_count >= 240,
        "two disjoint concurrent writers should both stay on the append path after seeding, got {:?}",
        metrics
    );
    assert!(
        metrics.insert_seek_count <= 16,
        "disjoint concurrent writers should not fall back to repeated seeks, got {:?}",
        metrics
    );
}

#[test]
fn test_v2_fused_insert_after_delete() {
    let _guard = v2_test_guard();
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .unwrap();

    // Insert some rows
    for i in 1..=10 {
        conn.execute(&format!("INSERT INTO t (val) VALUES ('item_{i}')"))
            .unwrap();
    }
    // Delete some
    conn.execute("DELETE FROM t WHERE id > 5").unwrap();

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 5);

    // Insert more — AUTOINCREMENT should not reuse deleted rowids
    for i in 11..=15 {
        conn.execute(&format!("INSERT INTO t (val) VALUES ('new_{i}')"))
            .unwrap();
    }

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 10);

    let min_new = conn
        .query_row("SELECT MIN(id) FROM t WHERE val LIKE 'new_%'")
        .unwrap();
    assert!(
        min_new.values()[0].to_integer() > 10,
        "AUTOINCREMENT should not reuse deleted rowids"
    );
}

#[test]
fn test_v2_insert_not_fused_with_index() {
    let _guard = v2_test_guard();
    // Table with secondary index — should NOT use FusedAppendInsert
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=100 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 'unique_{i}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 100);

    // Verify UNIQUE constraint works
    let result = conn.execute("INSERT INTO t VALUES (101, 'unique_1')");
    assert!(
        result.is_err(),
        "UNIQUE constraint should prevent duplicate"
    );
}

#[test]
fn test_v2_insert_not_fused_with_conflict() {
    let _guard = v2_test_guard();
    // INSERT OR REPLACE — should NOT use FusedAppendInsert
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'original')")
        .unwrap();
    conn.execute("INSERT OR REPLACE INTO t VALUES (1, 'replaced')")
        .unwrap();

    let row = conn.query_row("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(row.values()[0].to_text(), "replaced");
}

// ── V2.3: Ceremony reduction correctness ────────────────────────────────

#[test]
fn test_v2_halt_sentinel_terminates() {
    let _guard = v2_test_guard();
    // Programs must terminate via Halt — verify no infinite loop
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let row = conn.query_row("SELECT * FROM t").unwrap();
    assert_eq!(row.values()[0].to_integer(), 1);
}

#[test]
fn test_v2_prepared_insert_matches_adhoc() {
    let _guard = v2_test_guard();
    // Verify prepared INSERT produces same results as ad-hoc INSERT
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    // Ad-hoc inserts into t1
    conn.execute("BEGIN").unwrap();
    for i in 1..=100 {
        conn.execute(&format!("INSERT INTO t1 VALUES ({i}, {})", i * 10))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    // Prepared inserts into t2
    let stmt = conn.prepare("INSERT INTO t2 VALUES (?1, ?2)").unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=100_i64 {
        stmt.execute_with_params(&[SqliteValue::Integer(i), SqliteValue::Integer(i * 10)])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    // Compare results
    let sum1 = conn.query_row("SELECT SUM(val) FROM t1").unwrap();
    let sum2 = conn.query_row("SELECT SUM(val) FROM t2").unwrap();
    assert_eq!(sum1.values()[0].to_integer(), sum2.values()[0].to_integer());

    let count1 = conn.query_row("SELECT COUNT(*) FROM t1").unwrap();
    let count2 = conn.query_row("SELECT COUNT(*) FROM t2").unwrap();
    assert_eq!(
        count1.values()[0].to_integer(),
        count2.values()[0].to_integer()
    );
}

// ── Mixed fused and normal operations ───────────────────────────────────

#[test]
fn test_v2_mixed_insert_update_in_transaction() {
    let _guard = v2_test_guard();
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    // Fused INSERT path (no index, ABORT mode)
    for i in 1..=50 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    // UPDATE (not fused — different opcode path)
    conn.execute("UPDATE t SET val = val * 2 WHERE id <= 25")
        .unwrap();
    // More INSERTs
    for i in 51..=100 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 100);

    // Verify UPDATE applied
    let row = conn.query_row("SELECT val FROM t WHERE id = 10").unwrap();
    assert_eq!(row.values()[0].to_integer(), 20); // 10 * 2
    let row = conn.query_row("SELECT val FROM t WHERE id = 30").unwrap();
    assert_eq!(row.values()[0].to_integer(), 30); // not doubled
}

#[test]
fn test_v2_large_insert_page_splits() {
    let _guard = v2_test_guard();
    // Enough data to trigger multiple B-tree page splits during fused insert
    let conn = new_mem_conn();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=5000 {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'data_padding_{i}_xxxxxxxxxxxxxxxxxx')"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), 5000);

    // Verify B-tree integrity via spot checks
    let first = conn.query_row("SELECT data FROM t WHERE id = 1").unwrap();
    assert!(first.values()[0].to_text().starts_with("data_padding_1_"));
    let last = conn
        .query_row("SELECT data FROM t WHERE id = 5000")
        .unwrap();
    assert!(last.values()[0].to_text().starts_with("data_padding_5000_"));
}

#[test]
fn test_v2_file_backed_sequential_append_preserves_integrity_and_persists() {
    // bd-udl9m (Track G): a file-backed (real pager + B-tree) large sequential
    // append must engage the zero-seek append fast path, leave the on-disk
    // B-tree structurally valid (PRAGMA integrity_check == "ok"), and survive a
    // reopen with the exact same row set. This is the strongest "zero behavior
    // drift" guard for the Track G append optimization because it exercises page
    // splits and persistence — not just the in-memory execution image. Explicit
    // rowids keep these inserts on the counted Insert lane (auto-rowid inserts
    // take the separate, equally zero-seek FusedAppendInsert lane).
    let _guard = v2_test_guard();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("track_g_append_integrity.db");
    let db_path = db_path.to_string_lossy().into_owned();

    let conn = new_file_conn(&db_path);
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();

    let rows: i64 = 4000;
    let (_result, metrics) = capture_vdbe_metrics(|| {
        conn.execute("BEGIN").unwrap();
        for id in 1..=rows {
            conn.execute(&format!(
                "INSERT INTO t VALUES ({id}, 'payload_{id}_padpadpadpadpadpadpad')"
            ))
            .unwrap();
        }
        conn.execute("COMMIT").unwrap();
    });
    log_track_t_metrics("file_backed_sequential_append_integrity", &metrics);

    // The append fast path must carry the right edge across the page splits this
    // workload triggers; only the seed insert (plus at most a few cold-cursor
    // re-entries) should fall back to a full existence seek.
    let min_appends = u64::try_from(rows - 16).expect("append threshold fits in u64");
    assert!(
        metrics.insert_append_count >= min_appends,
        "file-backed sequential inserts should stay on the append no-seek path across page splits, got {:?}",
        metrics
    );
    assert!(
        metrics.insert_seek_count <= 16,
        "file-backed sequential inserts should not fall back to repeated existence seeks, got {:?}",
        metrics
    );

    let count = conn.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), rows);

    // Structural proof: the append fast path must not corrupt the on-disk B-tree
    // (no orphaned cells, valid interior dividers, intact freelist).
    let integrity = conn.query("PRAGMA integrity_check").unwrap();
    assert_eq!(
        integrity[0].values()[0].to_text(),
        "ok",
        "append fast path must leave the B-tree structurally valid, got {:?}",
        integrity[0].values()
    );
    drop(conn);

    // Persistence proof: reopen from disk and confirm the exact row set and the
    // boundary rows survive a fresh pager/B-tree hydration.
    let reopened = new_file_conn(&db_path);
    let count = reopened.query_row("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.values()[0].to_integer(), rows);
    let min = reopened.query_row("SELECT MIN(id) FROM t").unwrap();
    assert_eq!(min.values()[0].to_integer(), 1);
    let max = reopened.query_row("SELECT MAX(id) FROM t").unwrap();
    assert_eq!(max.values()[0].to_integer(), rows);
    let mid = reopened
        .query_row("SELECT data FROM t WHERE id = 2000")
        .unwrap();
    assert_eq!(
        mid.values()[0].to_text(),
        "payload_2000_padpadpadpadpadpadpad"
    );
    let integrity = reopened.query("PRAGMA integrity_check").unwrap();
    assert_eq!(
        integrity[0].values()[0].to_text(),
        "ok",
        "reopened database must remain structurally valid after an append-only load"
    );
}
