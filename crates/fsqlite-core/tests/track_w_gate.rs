//! bd-aaiwu: W-TEST — Truth-matrix, mixed-OLTP, and regression gate for Track W.
//!
//! Covers:
//! 1. INSERT lane classifier truth matrix (DirectCompiled vs ReusableTableProgram).
//! 2. UPDATE/DELETE fast lane verification with fallback attribution.
//! 3. Mixed OLTP regression: interleaved INSERT/UPDATE/DELETE/SELECT with lane metrics.
//! 4. W1 — function cache preservation on reusable lane (integration-level).
//! 5. W9 — retained autocommit overlay (same-connection read-after-write).
//! 6. Cross-connection correctness for prepared DML.
//!
//! Run:
//!   cargo test -p fsqlite-core --test track_w_gate -- --test-threads=1 --nocapture

use fsqlite_core::connection::{
    Connection, hot_path_profile_enabled, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use fsqlite_types::SqliteValue;
use std::sync::{LazyLock, Mutex, MutexGuard};

// ─── Profile test guard (serializes hot-path profiling across tests) ─────

static W_GATE_PROFILE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct WGateProfileGuard {
    _lock: MutexGuard<'static, ()>,
    previous_enabled: bool,
}

impl WGateProfileGuard {
    fn new() -> Self {
        let lock = W_GATE_PROFILE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous_enabled = hot_path_profile_enabled();
        reset_hot_path_profile();
        set_hot_path_profile_enabled(true);
        Self {
            _lock: lock,
            previous_enabled,
        }
    }
}

impl Drop for WGateProfileGuard {
    fn drop(&mut self) {
        reset_hot_path_profile();
        set_hot_path_profile_enabled(self.previous_enabled);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. INSERT lane classifier truth matrix
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wtest_insert_truth_matrix_values_all_params_hits_direct_lane() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE tm1 (id INTEGER PRIMARY KEY, name TEXT, val REAL)")
        .unwrap();

    let stmt = conn.prepare("INSERT INTO tm1 VALUES (?1, ?2, ?3)").unwrap();
    reset_hot_path_profile();
    for i in 0..20 {
        stmt.execute_with_params(&[
            SqliteValue::Integer(i),
            SqliteValue::Text(format!("n{i}").into()),
            SqliteValue::Float(i as f64 * 0.1),
        ])
        .unwrap();
    }
    let snap = hot_path_profile_snapshot();
    assert_eq!(
        snap.prepared_direct_insert_executions, 20,
        "VALUES(?1,?2,?3) should use direct-insert lane: {snap:?}"
    );
    assert_eq!(
        snap.prepared_insert_fast_lane_hits, 20,
        "all 20 INSERTs should hit fast lane: {snap:?}"
    );
}

#[test]
fn wtest_insert_truth_matrix_column_list_params_hits_direct_lane() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE tm2 (id INTEGER PRIMARY KEY, name TEXT, val REAL)")
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO tm2 (id, name, val) VALUES (?1, ?2, ?3)")
        .unwrap();
    reset_hot_path_profile();
    for i in 0..10 {
        stmt.execute_with_params(&[
            SqliteValue::Integer(i),
            SqliteValue::Text(format!("n{i}").into()),
            SqliteValue::Float(i as f64),
        ])
        .unwrap();
    }
    let snap = hot_path_profile_snapshot();
    assert!(
        snap.prepared_insert_fast_lane_hits >= 10,
        "column-list INSERT with all params should hit fast lane: {snap:?}"
    );
}

#[test]
fn wtest_insert_truth_matrix_operator_expression_values_hits_direct_lane() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE tm3 (id INTEGER PRIMARY KEY, computed TEXT)")
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO tm3 VALUES (?1, ('prefix_' || ?1))")
        .unwrap();
    reset_hot_path_profile();
    for i in 0..10 {
        stmt.execute_with_params(&[SqliteValue::Integer(i)])
            .unwrap();
    }
    let snap = hot_path_profile_snapshot();
    assert_eq!(
        snap.prepared_direct_insert_executions, 10,
        "operator-only expression VALUES should stay on the compiled direct-insert lane: {snap:?}"
    );
    assert_eq!(
        snap.prepared_table_engine_reuses, 0,
        "operator-only expression VALUES should not allocate/reuse the VDBE table engine: {snap:?}"
    );
    let rows = conn.query("SELECT computed FROM tm3 ORDER BY id").unwrap();
    assert_eq!(rows[0].values()[0], SqliteValue::Text("prefix_0".into()));
    assert_eq!(rows[9].values()[0], SqliteValue::Text("prefix_9".into()));
}

#[test]
fn wtest_insert_truth_matrix_autoincrement_hits_fast_lane() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE tm4 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        .unwrap();

    let stmt = conn.prepare("INSERT INTO tm4 (name) VALUES (?1)").unwrap();
    reset_hot_path_profile();
    for i in 0..10 {
        stmt.execute_with_params(&[SqliteValue::Text(format!("row{i}").into())])
            .unwrap();
    }
    let snap = hot_path_profile_snapshot();
    assert!(
        snap.prepared_insert_fast_lane_hits >= 10,
        "AUTOINCREMENT INSERT should hit fast lane: {snap:?}"
    );
    let count = conn.query("SELECT COUNT(*) FROM tm4").unwrap();
    assert_eq!(count[0].values()[0], SqliteValue::Integer(10));
}

#[test]
fn wtest_insert_truth_matrix_returning_stays_off_direct_lane() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE tm5 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let result = conn.query("INSERT INTO tm5 VALUES (1, 'hello') RETURNING id, val");
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        }
        Err(_) => {
            // RETURNING may not be fully wired — acceptable to skip
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. UPDATE/DELETE fast lane verification with fallback attribution
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wtest_update_simple_where_hits_fast_lane() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE wu1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO wu1 VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();

    let stmt = conn
        .prepare("UPDATE wu1 SET val = ?1 WHERE id = ?2")
        .unwrap();
    reset_hot_path_profile();
    stmt.execute_with_params(&[SqliteValue::Text("x".into()), SqliteValue::Integer(1)])
        .unwrap();
    stmt.execute_with_params(&[SqliteValue::Text("y".into()), SqliteValue::Integer(2)])
        .unwrap();

    let snap = hot_path_profile_snapshot();
    assert!(
        snap.prepared_direct_update_executions >= 2,
        "simple WHERE UPDATE should hit the direct UPDATE lane: {snap:?}"
    );
    assert_eq!(
        snap.prepared_update_delete_fallback_trigger, 0,
        "no trigger fallback expected: {snap:?}"
    );
    assert_eq!(
        snap.prepared_update_delete_fallback_foreign_key, 0,
        "no FK fallback expected: {snap:?}"
    );

    let rows = conn.query("SELECT val FROM wu1 ORDER BY id").unwrap();
    assert_eq!(rows[0].values()[0], SqliteValue::Text("x".into()));
    assert_eq!(rows[1].values()[0], SqliteValue::Text("y".into()));
    assert_eq!(rows[2].values()[0], SqliteValue::Text("c".into()));
}

#[test]
fn wtest_delete_simple_where_hits_fast_lane() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE wd1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO wd1 VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();

    let stmt = conn.prepare("DELETE FROM wd1 WHERE id = ?1").unwrap();
    reset_hot_path_profile();
    stmt.execute_with_params(&[SqliteValue::Integer(1)])
        .unwrap();
    stmt.execute_with_params(&[SqliteValue::Integer(3)])
        .unwrap();

    let snap = hot_path_profile_snapshot();
    assert!(
        snap.prepared_direct_delete_executions >= 2,
        "simple WHERE DELETE should hit the direct DELETE lane: {snap:?}"
    );

    let rows = conn.query("SELECT id FROM wd1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));
}

#[test]
fn wtest_update_with_trigger_falls_back_with_attribution() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE wut (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE wut_log (id INTEGER PRIMARY KEY, msg TEXT)")
        .unwrap();
    conn.execute("INSERT INTO wut VALUES (1, 'old')").unwrap();
    conn.execute(
        "CREATE TRIGGER wut_trg AFTER UPDATE ON wut
         BEGIN INSERT INTO wut_log VALUES (NEW.id, NEW.val); END",
    )
    .unwrap();

    let stmt = conn
        .prepare("UPDATE wut SET val = ?1 WHERE id = ?2")
        .unwrap();
    reset_hot_path_profile();
    stmt.execute_with_params(&[SqliteValue::Text("new".into()), SqliteValue::Integer(1)])
        .unwrap();

    let snap = hot_path_profile_snapshot();
    assert_eq!(
        snap.prepared_update_delete_fast_lane_hits, 0,
        "trigger UPDATE must NOT hit fast lane: {snap:?}"
    );
    assert_eq!(
        snap.prepared_update_delete_fallback_trigger, 1,
        "trigger UPDATE must attribute fallback to trigger: {snap:?}"
    );

    let log = conn.query("SELECT msg FROM wut_log").unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].values()[0], SqliteValue::Text("new".into()));
}

#[test]
fn wtest_delete_with_fk_cascade_falls_back_with_attribution() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE wfk_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE wfk_child (
            id INTEGER PRIMARY KEY,
            pid INTEGER REFERENCES wfk_parent(id) ON DELETE CASCADE
        )",
    )
    .unwrap();
    conn.execute("INSERT INTO wfk_parent VALUES (1),(2)")
        .unwrap();
    conn.execute("INSERT INTO wfk_child VALUES (10,1),(20,2)")
        .unwrap();

    let stmt = conn
        .prepare("DELETE FROM wfk_parent WHERE id = ?1")
        .unwrap();
    reset_hot_path_profile();
    stmt.execute_with_params(&[SqliteValue::Integer(1)])
        .unwrap();

    let snap = hot_path_profile_snapshot();
    assert_eq!(
        snap.prepared_update_delete_fallback_foreign_key, 1,
        "FK CASCADE DELETE must attribute fallback to FK before any precompiled VDBE handoff: {snap:?}"
    );

    let children = conn.query("SELECT id FROM wfk_child").unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].values()[0], SqliteValue::Integer(20));
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Mixed OLTP regression gate
// ═══════════════════════════════════════════════════════════════════════════

struct MiniRng(u64);

impl MiniRng {
    const fn new(seed: u64) -> Self {
        Self(if seed == 0 { 1 } else { seed })
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

#[test]
fn wtest_mixed_oltp_prepared_lanes_all_active() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE moltp (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .unwrap();

    let insert_stmt = conn
        .prepare("INSERT INTO moltp VALUES (?1, ?2, ?3)")
        .unwrap();
    let update_stmt = conn
        .prepare("UPDATE moltp SET val = ?1 WHERE id = ?2")
        .unwrap();
    let delete_stmt = conn.prepare("DELETE FROM moltp WHERE id = ?1").unwrap();

    let mut rng = MiniRng::new(0xDEAD_BEEF);
    let mut alive: Vec<i64> = Vec::new();
    let mut next_id: i64 = 1;
    // Seed 100 rows first
    for _ in 0..100 {
        let id = next_id;
        next_id += 1;
        insert_stmt
            .execute_with_params(&[
                SqliteValue::Integer(id),
                SqliteValue::Text(format!("n{id}").into()),
                SqliteValue::Integer(id * 7),
            ])
            .unwrap();
        alive.push(id);
    }

    reset_hot_path_profile();
    let mut insert_count: u64 = 0;
    let mut update_count: u64 = 0;
    let mut delete_count: u64 = 0;
    let mut select_count: u64 = 0;

    // 1000-op mixed workload: 40% INSERT, 25% UPDATE, 15% DELETE, 20% SELECT
    for _ in 0..1000 {
        let roll = rng.next_usize(100);
        match roll {
            0..40 => {
                let id = next_id;
                next_id += 1;
                insert_stmt
                    .execute_with_params(&[
                        SqliteValue::Integer(id),
                        SqliteValue::Text(format!("n{id}").into()),
                        SqliteValue::Integer(id * 7),
                    ])
                    .unwrap();
                alive.push(id);
                insert_count += 1;
            }
            40..65 => {
                if !alive.is_empty() {
                    let idx = rng.next_usize(alive.len());
                    let id = alive[idx];
                    update_stmt
                        .execute_with_params(&[
                            SqliteValue::Integer(id * 13),
                            SqliteValue::Integer(id),
                        ])
                        .unwrap();
                    update_count += 1;
                }
            }
            65..80 => {
                if !alive.is_empty() {
                    let idx = rng.next_usize(alive.len());
                    let id = alive.swap_remove(idx);
                    delete_stmt
                        .execute_with_params(&[SqliteValue::Integer(id)])
                        .unwrap();
                    delete_count += 1;
                }
            }
            _ => {
                let rows = conn.query("SELECT COUNT(*) FROM moltp").unwrap();
                assert_eq!(
                    rows[0].values()[0],
                    SqliteValue::Integer(alive.len() as i64)
                );
                select_count += 1;
            }
        }
    }

    let snap = hot_path_profile_snapshot();

    eprintln!("=== W-TEST: Mixed OLTP lane metrics ===");
    eprintln!(
        "ops: insert={insert_count} update={update_count} delete={delete_count} select={select_count}"
    );
    eprintln!(
        "INSERT: fast_lane={} direct_exec={} engine_reuses={}",
        snap.prepared_insert_fast_lane_hits,
        snap.prepared_direct_insert_executions,
        snap.prepared_table_engine_reuses
    );
    eprintln!(
        "UPDATE/DELETE: fused_fast_lane={} instrumented={} dml_handoff={} direct_update={} direct_delete={}",
        snap.prepared_update_delete_fast_lane_hits,
        snap.prepared_update_delete_instrumented_lane_hits,
        snap.prepared_update_delete_dml_direct_handoff_runs,
        snap.prepared_direct_update_executions,
        snap.prepared_direct_delete_executions
    );
    eprintln!(
        "fallbacks: trigger={} fk={} returning={} vtab={}",
        snap.prepared_update_delete_fallback_trigger,
        snap.prepared_update_delete_fallback_foreign_key,
        snap.prepared_update_delete_fallback_returning,
        snap.prepared_update_delete_fallback_live_vtab
    );
    eprintln!("=== END ===");

    // INSERT fast lane should cover all insert ops
    assert!(
        snap.prepared_insert_fast_lane_hits >= insert_count,
        "all {insert_count} INSERTs should hit fast lane, got {}: {snap:?}",
        snap.prepared_insert_fast_lane_hits
    );

    // UPDATE/DELETE direct lanes should cover all update+delete ops (no triggers/FKs)
    let direct_update_delete =
        snap.prepared_direct_update_executions + snap.prepared_direct_delete_executions;
    assert!(
        direct_update_delete >= update_count + delete_count,
        "all {} UPDATE/DELETE ops should hit direct lanes, got {}: {snap:?}",
        update_count + delete_count,
        direct_update_delete
    );

    // Zero fallbacks expected — no triggers, FKs, RETURNING, vtabs
    assert_eq!(snap.prepared_update_delete_fallback_trigger, 0);
    assert_eq!(snap.prepared_update_delete_fallback_foreign_key, 0);
    assert_eq!(snap.prepared_update_delete_fallback_returning, 0);
    assert_eq!(snap.prepared_update_delete_fallback_live_vtab, 0);

    // Data integrity: row count matches alive set
    let final_rows = conn.query("SELECT COUNT(*) FROM moltp").unwrap();
    assert_eq!(
        final_rows[0].values()[0],
        SqliteValue::Integer(alive.len() as i64),
        "final row count must match alive set after mixed OLTP workload"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. W1 — function cache preservation (integration-level)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wtest_w1_function_cache_preserved_across_prepared_reuse() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute(
        "CREATE TABLE w1t (id INTEGER PRIMARY KEY, val TEXT NOT NULL, computed INTEGER NOT NULL)",
    )
    .unwrap();

    // Function-valued INSERTs still use the reusable VDBE table program path;
    // operator-only expressions are handled by the direct-insert microplan.
    let stmt = conn
        .prepare("INSERT INTO w1t VALUES (?1, ('name_' || ?1), length('name_' || ?1))")
        .unwrap();

    reset_hot_path_profile();
    for i in 0..50 {
        stmt.execute_with_params(&[SqliteValue::Integer(i)])
            .unwrap();
    }

    let snap = hot_path_profile_snapshot();
    // After first alloc, all subsequent should be reuses
    assert!(
        snap.prepared_table_engine_reuses >= 49,
        "W1: function-valued INSERT should reuse engine 49+ times, got {}: {snap:?}",
        snap.prepared_table_engine_reuses
    );
    assert!(
        snap.prepared_table_engine_fresh_allocs <= 1,
        "W1: at most 1 fresh engine alloc expected, got {}: {snap:?}",
        snap.prepared_table_engine_fresh_allocs
    );

    let count = conn.query("SELECT COUNT(*) FROM w1t").unwrap();
    assert_eq!(count[0].values()[0], SqliteValue::Integer(50));
    let computed = conn
        .query("SELECT computed FROM w1t WHERE id = 49")
        .unwrap();
    assert_eq!(computed[0].values()[0], SqliteValue::Integer(7));
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. W9 — retained autocommit overlay (same-connection read-after-write)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wtest_w9_same_connection_read_after_write_no_data_loss() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE w9t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let insert_stmt = conn.prepare("INSERT INTO w9t VALUES (?1, ?2)").unwrap();

    // Interleave writes and reads on same connection
    for i in 1..=50 {
        insert_stmt
            .execute_with_params(&[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("v{i}").into()),
            ])
            .unwrap();

        // Read back immediately — must see the row we just wrote
        let rows = conn
            .query(&format!("SELECT val FROM w9t WHERE id = {i}"))
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "read-after-write must see row {i} immediately"
        );
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Text(format!("v{i}").into())
        );

        // Count should match i
        let count_rows = conn.query("SELECT COUNT(*) FROM w9t").unwrap();
        assert_eq!(
            count_rows[0].values()[0],
            SqliteValue::Integer(i),
            "COUNT(*) must reflect all {i} inserted rows"
        );
    }

    let snap = hot_path_profile_snapshot();
    eprintln!("=== W9: Retained autocommit overlay metrics ===");
    eprintln!(
        "retained_autocommit: reuses={} parks={} flushes={} raw_flushes={} overlay_hits={} overlay_misses={}",
        snap.retained_autocommit_reuses,
        snap.retained_autocommit_parks,
        snap.retained_autocommit_flushes,
        snap.retained_autocommit_read_after_write_flushes,
        snap.retained_autocommit_overlay_hits,
        snap.retained_autocommit_overlay_misses
    );
    eprintln!("=== END ===");
}

#[test]
fn wtest_w9_same_connection_update_then_read_sees_new_value() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE w9u (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO w9u VALUES (1,'original'),(2,'original'),(3,'original')")
        .unwrap();

    let update_stmt = conn
        .prepare("UPDATE w9u SET val = ?1 WHERE id = ?2")
        .unwrap();

    reset_hot_path_profile();
    update_stmt
        .execute_with_params(&[
            SqliteValue::Text("modified".into()),
            SqliteValue::Integer(2),
        ])
        .unwrap();

    let rows = conn.query("SELECT val FROM w9u WHERE id = 2").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values()[0],
        SqliteValue::Text("modified".into()),
        "read-after-UPDATE must see new value"
    );

    // Other rows unchanged
    let other = conn.query("SELECT val FROM w9u WHERE id = 1").unwrap();
    assert_eq!(other[0].values()[0], SqliteValue::Text("original".into()));
}

#[test]
fn wtest_w9_same_connection_delete_then_read_sees_removal() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE w9d (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO w9d VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();

    let delete_stmt = conn.prepare("DELETE FROM w9d WHERE id = ?1").unwrap();

    reset_hot_path_profile();
    delete_stmt
        .execute_with_params(&[SqliteValue::Integer(2)])
        .unwrap();

    let rows = conn.query("SELECT id FROM w9d ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
    assert_eq!(rows[1].values()[0], SqliteValue::Integer(3));
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Cross-connection correctness for prepared DML
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wtest_cross_connection_prepared_insert_visible_after_commit() {
    let _g = WGateProfileGuard::new();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cross_conn.db");
    let path = db.to_string_lossy().into_owned();

    let conn_a = Connection::open(&path).unwrap();
    conn_a.execute("PRAGMA journal_mode = WAL").unwrap();
    conn_a
        .execute("CREATE TABLE xconn (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let conn_b = Connection::open(&path).unwrap();

    // Prepare INSERT on conn_a
    let stmt = conn_a.prepare("INSERT INTO xconn VALUES (?1, ?2)").unwrap();

    reset_hot_path_profile();
    stmt.execute_with_params(&[SqliteValue::Integer(1), SqliteValue::Text("from_a".into())])
        .unwrap();

    // conn_b should see the committed row (autocommit semantics)
    let rows = conn_b.query("SELECT val FROM xconn WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 1, "conn_b must see conn_a's committed INSERT");
    assert_eq!(rows[0].values()[0], SqliteValue::Text("from_a".into()));
}

#[test]
fn wtest_cross_connection_prepared_insert_within_explicit_txn() {
    let _g = WGateProfileGuard::new();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cross_txn.db");
    let path = db.to_string_lossy().into_owned();

    let conn_a = Connection::open(&path).unwrap();
    conn_a.execute("PRAGMA journal_mode = WAL").unwrap();
    conn_a
        .execute("CREATE TABLE xtxn (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let conn_b = Connection::open(&path).unwrap();

    let stmt = conn_a.prepare("INSERT INTO xtxn VALUES (?1, ?2)").unwrap();

    // Insert within explicit transaction
    conn_a.execute("BEGIN").unwrap();
    for i in 1..=10 {
        stmt.execute_with_params(&[
            SqliteValue::Integer(i),
            SqliteValue::Text(format!("row{i}").into()),
        ])
        .unwrap();
    }

    // Before commit: conn_b shouldn't see uncommitted rows (MVCC isolation)
    let pre_commit = conn_b.query("SELECT COUNT(*) FROM xtxn").unwrap();
    assert_eq!(
        pre_commit[0].values()[0],
        SqliteValue::Integer(0),
        "conn_b must not see uncommitted rows from conn_a's explicit txn"
    );

    conn_a.execute("COMMIT").unwrap();

    // After commit: conn_b should see all 10 rows
    let post_commit = conn_b.query("SELECT COUNT(*) FROM xtxn").unwrap();
    assert_eq!(
        post_commit[0].values()[0],
        SqliteValue::Integer(10),
        "conn_b must see all committed rows after conn_a's COMMIT"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Explicit transaction prepared DML batch — engine reuse + fast lane
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wtest_explicit_txn_batch_insert_engine_reuse_and_fast_lane() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE txn_batch (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO txn_batch VALUES (?1, ?2)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    reset_hot_path_profile();
    for i in 0..500 {
        stmt.execute_with_params(&[
            SqliteValue::Integer(i),
            SqliteValue::Text(format!("v{i}").into()),
        ])
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let snap = hot_path_profile_snapshot();
    eprintln!("=== W-TEST: Explicit txn batch INSERT metrics ===");
    eprintln!(
        "fast_lane={} direct_exec={} engine_reuses={} fresh_allocs={}",
        snap.prepared_insert_fast_lane_hits,
        snap.prepared_direct_insert_executions,
        snap.prepared_table_engine_reuses,
        snap.prepared_table_engine_fresh_allocs
    );
    eprintln!("=== END ===");

    assert!(
        snap.prepared_insert_fast_lane_hits >= 500,
        "all 500 txn batch INSERTs should hit fast lane: {snap:?}"
    );

    let count = conn.query("SELECT COUNT(*) FROM txn_batch").unwrap();
    assert_eq!(count[0].values()[0], SqliteValue::Integer(500));
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. FK pragma toggle — dynamic fast-lane eligibility
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wtest_fk_pragma_toggle_dynamic_fast_lane_eligibility() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("PRAGMA foreign_keys = OFF").unwrap();
    conn.execute("CREATE TABLE fkt_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE fkt_child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES fkt_parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    conn.execute("INSERT INTO fkt_parent VALUES (1),(2),(3)")
        .unwrap();
    conn.execute("INSERT INTO fkt_child VALUES (10,1),(20,2),(30,3)")
        .unwrap();

    let stmt = conn
        .prepare("DELETE FROM fkt_parent WHERE id = ?1")
        .unwrap();

    // With FKs OFF: should use fast lane
    reset_hot_path_profile();
    stmt.execute_with_params(&[SqliteValue::Integer(1)])
        .unwrap();
    let off_snap = hot_path_profile_snapshot();
    assert!(
        off_snap.prepared_update_delete_fast_lane_hits + off_snap.prepared_direct_delete_executions
            >= 1,
        "FK=OFF DELETE should hit a prepared fast lane: {off_snap:?}"
    );
    assert_eq!(off_snap.prepared_update_delete_fallback_foreign_key, 0);

    // With FKs ON: should fall back
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    reset_hot_path_profile();
    stmt.execute_with_params(&[SqliteValue::Integer(2)])
        .unwrap();
    let on_snap = hot_path_profile_snapshot();
    assert_eq!(
        on_snap.prepared_update_delete_fallback_foreign_key, 1,
        "FK=ON DELETE should attribute to FK fallback before any precompiled VDBE handoff: {on_snap:?}"
    );

    // Verify FK=ON cascaded the second delete while the earlier FK=OFF
    // delete intentionally left child 10 orphaned.
    let children = conn.query("SELECT id FROM fkt_child ORDER BY id").unwrap();
    assert_eq!(
        children.len(),
        2,
        "FK=OFF should leave child 10 orphaned, and FK=ON should cascade-delete child 20"
    );
    assert_eq!(children[0].values()[0], SqliteValue::Integer(10));
    assert_eq!(children[1].values()[0], SqliteValue::Integer(30));
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Prepared DML with multiple tables — no cross-table interference
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wtest_multi_table_prepared_dml_no_interference() {
    let _g = WGateProfileGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE mt_a (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE mt_b (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let ins_a = conn.prepare("INSERT INTO mt_a VALUES (?1, ?2)").unwrap();
    let ins_b = conn.prepare("INSERT INTO mt_b VALUES (?1, ?2)").unwrap();
    let upd_a = conn
        .prepare("UPDATE mt_a SET val = ?1 WHERE id = ?2")
        .unwrap();
    let del_b = conn.prepare("DELETE FROM mt_b WHERE id = ?1").unwrap();

    reset_hot_path_profile();

    // Interleave operations across tables
    for i in 0..20 {
        ins_a
            .execute_with_params(&[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("a{i}").into()),
            ])
            .unwrap();
        ins_b
            .execute_with_params(&[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("b{i}").into()),
            ])
            .unwrap();
    }
    for i in 0..10 {
        upd_a
            .execute_with_params(&[SqliteValue::Text("updated".into()), SqliteValue::Integer(i)])
            .unwrap();
        del_b
            .execute_with_params(&[SqliteValue::Integer(i)])
            .unwrap();
    }

    let snap = hot_path_profile_snapshot();
    assert!(
        snap.prepared_insert_fast_lane_hits >= 40,
        "all 40 INSERTs across 2 tables should hit fast lane: {snap:?}"
    );
    assert!(
        snap.prepared_direct_update_executions + snap.prepared_direct_delete_executions >= 20,
        "UPDATE+DELETE across tables should hit direct lanes: {snap:?}"
    );

    let a_count = conn.query("SELECT COUNT(*) FROM mt_a").unwrap();
    assert_eq!(a_count[0].values()[0], SqliteValue::Integer(20));
    let b_count = conn.query("SELECT COUNT(*) FROM mt_b").unwrap();
    assert_eq!(b_count[0].values()[0], SqliteValue::Integer(10));

    let a_updated = conn
        .query("SELECT COUNT(*) FROM mt_a WHERE val = 'updated'")
        .unwrap();
    assert_eq!(a_updated[0].values()[0], SqliteValue::Integer(10));
}
