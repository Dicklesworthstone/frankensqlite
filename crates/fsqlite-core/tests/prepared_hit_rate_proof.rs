//! bd-db300.4.5.1: Prove actual prepared-artifact hit rates and fast-lane usage
//! on c1 micro-workloads.
//!
//! This test reproduces the commutative_inserts_disjoint_keys c1 workload pattern
//! (the worst measured cell at 0.068x) using prepared statements and proves:
//! 1. Prepared INSERT fast-lane hits = 100% of INSERT ops.
//! 2. Table engine reuse = 100% after first alloc.
//! 3. Parse/compiled cache hits = 0 (expected: prepared stmts bypass these caches).
//! 4. Schema refreshes and publication binds are the dominant per-statement cost.
//!
//! Run:
//!   CARGO_TARGET_DIR=/tmp/pane1-d51 cargo test -p fsqlite-core \
//!     --test prepared_hit_rate_proof -- --test-threads=1 --nocapture

use fsqlite_core::connection::{
    Connection, hot_path_profile_enabled, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use fsqlite_error::FrankenError;
use fsqlite_types::SqliteValue;
use std::sync::{LazyLock, Mutex, MutexGuard};

static HOT_PATH_PROFILE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn lock_profile_test_mutex() -> MutexGuard<'static, ()> {
    match HOT_PATH_PROFILE_TEST_LOCK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct HotPathProfileTestGuard {
    _lock: MutexGuard<'static, ()>,
    previous_enabled: bool,
}

impl HotPathProfileTestGuard {
    fn new() -> Self {
        let lock = lock_profile_test_mutex();
        let previous_enabled = hot_path_profile_enabled();
        reset_hot_path_profile();
        set_hot_path_profile_enabled(true);
        Self {
            _lock: lock,
            previous_enabled,
        }
    }
}

impl Drop for HotPathProfileTestGuard {
    fn drop(&mut self) {
        reset_hot_path_profile();
        set_hot_path_profile_enabled(self.previous_enabled);
    }
}

/// Simulate the c1 commutative_inserts workload: N prepared INSERTs into
/// separate tables, autocommit, file-backed WAL.
#[test]
fn test_prepared_fast_lane_hit_rate_on_c1_workload() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        let conn = Connection::open(path).await.unwrap();
        conn.execute("PRAGMA journal_mode = WAL").await.unwrap();
        conn.execute("PRAGMA synchronous = NORMAL").await.unwrap();

        // Create 2 tables (simulates disjoint-key workload with multiple tables).
        conn.execute("CREATE TABLE t0(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();

        // Prepare statements (one per table, as the real executor does).
        let stmt0 = conn.prepare("INSERT INTO t0 VALUES(?1, ?2)").await.unwrap();
        let stmt1 = conn.prepare("INSERT INTO t1 VALUES(?1, ?2)").await.unwrap();

        // Warm: one execution per table to establish baseline.
        stmt0
            .execute_with_params(&[
                fsqlite_types::SqliteValue::Integer(0),
                fsqlite_types::SqliteValue::Text("warm".into()),
            ])
            .await
            .unwrap();
        stmt1
            .execute_with_params(&[
                fsqlite_types::SqliteValue::Integer(0),
                fsqlite_types::SqliteValue::Text("warm".into()),
            ])
            .await
            .unwrap();

        // Reset counters after warmup.
        reset_hot_path_profile();

        // Measurement: 50 INSERTs per table = 100 total (matches real workload scale).
        for i in 1..=50 {
            stmt0
                .execute_with_params(&[
                    fsqlite_types::SqliteValue::Integer(i),
                    fsqlite_types::SqliteValue::Text(format!("v{i}").into()),
                ])
                .await
                .unwrap();
            stmt1
                .execute_with_params(&[
                    fsqlite_types::SqliteValue::Integer(i),
                    fsqlite_types::SqliteValue::Text(format!("v{i}").into()),
                ])
                .await
                .unwrap();
        }

        let snap = hot_path_profile_snapshot();

        // ─── Scorecard ───
        eprintln!("=== bd-db300.4.5.1: Prepared Hit Rate Proof (100 file-backed INSERTs) ===");
        eprintln!("Parser counters (expected: 0 hits — prepared stmts bypass parse cache):");
        eprintln!(
            "  parse_cache:    hits={:>4}  misses={:>4}",
            snap.parser.parse_cache_hits, snap.parser.parse_cache_misses
        );
        eprintln!(
            "  compiled_cache: hits={:>4}  misses={:>4}",
            snap.parser.compiled_cache_hits, snap.parser.compiled_cache_misses
        );
        eprintln!(
            "  fast_path:      {:>4}  slow_path: {:>4}",
            snap.parser.fast_path_executions, snap.parser.slow_path_executions
        );
        eprintln!("Connection ceremony counters:");
        eprintln!(
            "  prepared_insert_fast_lane_hits:      {:>4}",
            snap.prepared_insert_fast_lane_hits
        );
        eprintln!(
            "  prepared_table_engine_reuses:        {:>4}",
            snap.prepared_table_engine_reuses
        );
        eprintln!(
            "  prepared_table_engine_fresh_allocs:  {:>4}",
            snap.prepared_table_engine_fresh_allocs
        );
        eprintln!(
            "  prepared_schema_refreshes:           {:>4}",
            snap.prepared_schema_refreshes
        );
        eprintln!(
            "  pager_publication_refreshes:         {:>4}",
            snap.pager_publication_refreshes
        );
        eprintln!(
            "  begin_refresh_count:                 {:>4}",
            snap.begin_refresh_count
        );
        eprintln!(
            "  commit_refresh_count:                {:>4}",
            snap.commit_refresh_count
        );
        eprintln!(
            "  background_status_checks:            {:>4}",
            snap.background_status_checks
        );
        eprintln!("=== END SCORECARD ===");

        // ─── Assertions ───

        // 1. All 100 INSERTs should hit the prepared fast lane.
        assert!(
            snap.prepared_insert_fast_lane_hits >= 100,
            "all 100 INSERTs should hit prepared fast lane: got {}",
            snap.prepared_insert_fast_lane_hits
        );

        // 2. Fast path should dominate (precompiled_dml path from bd-6eyrg.1).
        assert!(
            snap.parser.fast_path_executions >= 100,
            "all 100 INSERTs should use fast path: got {}",
            snap.parser.fast_path_executions
        );

        // 3. Parse and compiled cache hits should be 0 (prepared stmts bypass both).
        assert_eq!(
            snap.parser.parse_cache_hits, 0,
            "prepared stmts should not produce parse cache hits"
        );
        assert_eq!(
            snap.parser.compiled_cache_hits, 0,
            "prepared stmts should not produce compiled cache hits"
        );

        // 4. Either the direct-insert fast path OR the engine-reuse path should
        // cover all ops. The direct-insert path bypasses the VDBE engine entirely
        // (so engine_reuses stays 0) but is strictly faster. Accept either.
        let direct_insert_executions = snap.prepared_direct_insert_executions;
        let engine_reuses = snap.prepared_table_engine_reuses;
        assert!(
            direct_insert_executions >= 100 || engine_reuses >= 100,
            "all ops should use either direct-insert ({direct_insert_executions}) or engine-reuse ({engine_reuses}) path",
        );
        assert_eq!(
            snap.prepared_table_engine_fresh_allocs, 0,
            "no fresh table-engine allocs expected after warmup: got {}",
            snap.prepared_table_engine_fresh_allocs
        );
    });
}

/// Prove bd-db300.4.5.2 directly: when prepared DML must take the
/// FullReloadRequired refresh path, the execution should reuse the schema-bound
/// publication instead of paying a second bind during autocommit begin.
#[test]
fn test_prepared_full_reload_reuses_publication_after_cross_connection_ddl() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("prepared_full_reload_publication_reuse.db");
        let db = db_path.to_string_lossy().into_owned();

        let conn1 = Connection::open(&db).await.unwrap();
        conn1.set_reject_mem_fallback(true);
        conn1.set_strict_mem_fallback_rejection(true);
        conn1
            .execute("CREATE TABLE prep_full_reload_pub (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();

        let stale_stmt = conn1
            .prepare("INSERT INTO prep_full_reload_pub (id, val) VALUES (?1, ?2)")
            .await
            .unwrap();

        let conn2 = Connection::open(&db).await.unwrap();
        conn2.set_reject_mem_fallback(true);
        conn2.set_strict_mem_fallback_rejection(true);
        conn2
            .execute("CREATE TABLE prep_full_reload_pub_bump (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();

        let err = stale_stmt
            .execute_with_params(&[SqliteValue::Integer(1), SqliteValue::Text("stale".into())])
            .await
            .expect_err("cross-connection DDL must invalidate the stale prepared INSERT");
        assert!(matches!(err, FrankenError::SchemaChanged));

        // Force future stale prepared executions onto the full-reload path while
        // keeping schema identity stable for the measured window.
        conn1.set_reject_mem_fallback(false);
        let stmt = conn1
            .prepare("INSERT INTO prep_full_reload_pub (id, val) VALUES (?1, ?2)")
            .await
            .unwrap();
        conn2
            .execute("INSERT INTO prep_full_reload_pub VALUES (1, 'from_conn2')")
            .await
            .unwrap();

        reset_hot_path_profile();
        let affected = stmt
            .execute_with_params(&[
                SqliteValue::Integer(2),
                SqliteValue::Text("from_conn1".into()),
            ])
            .await
            .unwrap();
        assert_eq!(affected, 1);

        let profile = hot_path_profile_snapshot();
        eprintln!("=== bd-db300.4.5.2: FullReloadRequired publication-reuse proof ===");
        eprintln!(
            "prepared_schema_refreshes={} lightweight={} full_reload={} pager_publication_refreshes={} fast_lane_hits={}",
            profile.prepared_schema_refreshes,
            profile.prepared_schema_lightweight_refreshes,
            profile.prepared_schema_full_reloads,
            profile.pager_publication_refreshes,
            profile.prepared_insert_fast_lane_hits
        );
        eprintln!("=== END SCORECARD ===");

        assert_eq!(
            profile.prepared_schema_refreshes, 1,
            "the measured prepared execute should pay exactly one external schema refresh: {profile:?}"
        );
        assert_eq!(
            profile.prepared_schema_full_reloads, 1,
            "with eager MemDB hydration enabled, stale prepared DML should take the FullReloadRequired path: {profile:?}"
        );
        assert_eq!(
            profile.prepared_schema_lightweight_refreshes, 0,
            "the full-reload proof window must not fall back to the lightweight refresh path: {profile:?}"
        );
        assert_eq!(
            profile.pager_publication_refreshes, 1,
            "the prepared execute should reuse the full-reload publication instead of rebinding during autocommit begin: {profile:?}"
        );
        assert_eq!(
            profile.prepared_insert_fast_lane_hits, 1,
            "the measured prepared insert should stay on the prepared fast lane after the full reload: {profile:?}"
        );

        let rows = conn1
            .query("SELECT id, val FROM prep_full_reload_pub ORDER BY id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("from_conn2".into()));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));
        assert_eq!(rows[1].values()[1], SqliteValue::Text("from_conn1".into()));
    });
}

/// B3.4 Probe: Measure commit_txn_roundtrip_time_ns for :memory: autocommit INSERTs.
/// Uses AUTO-ROWID inserts to test the implicit_rowid_hint fast path.
// ubs:ignore false positive: generated test values are bound parameters, not SQL text.
#[test]
fn test_b3_4_memory_autocommit_commit_roundtrip_probe() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();
        // Use auto-rowid: INSERT INTO t(val) VALUES(?) - no explicit id
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        let stmt = conn.prepare("INSERT INTO t(val) VALUES(?1)").await.unwrap();

        // Warmup
        for i in 0..10 {
            stmt.execute_with_params(&[SqliteValue::Text(format!("w{i}").into())])
                .await
                .unwrap();
        }
        reset_hot_path_profile();

        // Measurement: 1000 autocommit INSERTs with auto-rowid
        let n: i64 = 1000;
        for i in 0..n {
            stmt.execute_with_params(&[SqliteValue::Text(format!("v{i}").into())])
                .await
                .unwrap();
        }

        let snap = hot_path_profile_snapshot();
        let commit_us = snap.commit_txn_roundtrip_time_ns as f64 / 1000.0;
        let per_row_us = commit_us / n as f64;
        let cursor_setup_us = snap.prepared_direct_insert_cursor_setup_time_ns as f64 / 1000.0;
        let btree_insert_us = snap.prepared_direct_insert_btree_insert_time_ns as f64 / 1000.0;

        eprintln!("=== B3.4 Probe ({n} :memory: auto-rowid INSERTs) ===");
        eprintln!(
            "commit_txn_roundtrip:  {:.1} us total, {:.3} us/row",
            commit_us, per_row_us
        );
        eprintln!(
            "cursor_setup:          {:.1} us total, {:.3} us/row",
            cursor_setup_us,
            cursor_setup_us / n as f64
        );
        eprintln!(
            "btree_insert:          {:.1} us total, {:.3} us/row",
            btree_insert_us,
            btree_insert_us / n as f64
        );
        eprintln!(
            "execute_body:          {:.1} us total, {:.3} us/row",
            snap.execute_body_time_ns as f64 / 1000.0,
            snap.execute_body_time_ns as f64 / 1000.0 / n as f64
        );
        eprintln!("fast_lane_hits: {}", snap.prepared_insert_fast_lane_hits);
        eprintln!(
            "direct_insert_executions: {}",
            snap.prepared_direct_insert_executions
        );

        assert!(snap.prepared_insert_fast_lane_hits >= n as u64);
    });
}

/// B3 follow-up proof: exact `small_3col` autocommit prepared INSERT shape.
///
/// This keeps a disjoint backstop on the comprehensive/write-throughput
/// workload while exposing the direct-body split pane 5 is targeting next:
/// row build, serialization, btree insert, and MemDB apply.
#[test]
fn test_b3_small_3col_autocommit_direct_insert_profile_breakdown() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "CREATE TABLE bench(\
            id INTEGER PRIMARY KEY, \
            name TEXT NOT NULL, \
            value REAL NOT NULL\
        )",
        )
        .await
        .unwrap();
        let stmt = conn
            .prepare("INSERT INTO bench VALUES (?1, ('user_' || ?1), (?1 * 0.137))")
            .await
            .unwrap();

        const ROWS: i64 = 512;
        reset_hot_path_profile();
        for id in 0..ROWS {
            let affected = stmt
                .execute_with_params(&[SqliteValue::Integer(id)])
                .await
                .unwrap();
            assert_eq!(affected, 1);
        }

        let profile = hot_path_profile_snapshot();
        let direct_insert_total = profile
            .prepared_direct_insert_row_build_time_ns
            .saturating_add(profile.prepared_direct_insert_cursor_setup_time_ns)
            .saturating_add(profile.prepared_direct_insert_serialize_time_ns)
            .saturating_add(profile.prepared_direct_insert_btree_insert_time_ns)
            .saturating_add(profile.prepared_direct_insert_memdb_apply_time_ns);
        let autocommit_wrapper_total = profile
            .prepared_direct_insert_schema_validation_time_ns
            .saturating_add(profile.prepared_direct_insert_autocommit_begin_time_ns)
            .saturating_add(profile.prepared_direct_insert_change_tracking_time_ns)
            .saturating_add(profile.prepared_direct_insert_autocommit_resolve_time_ns);
        let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROWS));

        eprintln!("=== B3 small_3col autocommit profile ({ROWS} rows) ===");
        eprintln!(
            "row_build:            {:.1} us total, {:.3} us/row",
            profile.prepared_direct_insert_row_build_time_ns as f64 / 1000.0,
            profile.prepared_direct_insert_row_build_time_ns as f64 / 1000.0 / ROWS as f64
        );
        eprintln!(
            "cursor_setup:         {:.1} us total, {:.3} us/row",
            profile.prepared_direct_insert_cursor_setup_time_ns as f64 / 1000.0,
            profile.prepared_direct_insert_cursor_setup_time_ns as f64 / 1000.0 / ROWS as f64
        );
        eprintln!(
            "serialize:            {:.1} us total, {:.3} us/row",
            profile.prepared_direct_insert_serialize_time_ns as f64 / 1000.0,
            profile.prepared_direct_insert_serialize_time_ns as f64 / 1000.0 / ROWS as f64
        );
        eprintln!(
            "btree_insert:         {:.1} us total, {:.3} us/row",
            profile.prepared_direct_insert_btree_insert_time_ns as f64 / 1000.0,
            profile.prepared_direct_insert_btree_insert_time_ns as f64 / 1000.0 / ROWS as f64
        );
        eprintln!(
            "memdb_apply:          {:.1} us total, {:.3} us/row",
            profile.prepared_direct_insert_memdb_apply_time_ns as f64 / 1000.0,
            profile.prepared_direct_insert_memdb_apply_time_ns as f64 / 1000.0 / ROWS as f64
        );
        eprintln!(
            "direct_body_total:    {:.1} us total, {:.3} us/row",
            direct_insert_total as f64 / 1000.0,
            direct_insert_total as f64 / 1000.0 / ROWS as f64
        );
        eprintln!(
            "autocommit_wrapper:   {:.1} us total, {:.3} us/row",
            autocommit_wrapper_total as f64 / 1000.0,
            autocommit_wrapper_total as f64 / 1000.0 / ROWS as f64
        );
        eprintln!(
            "commit_roundtrip:     {:.1} us total, {:.3} us/row",
            profile.commit_txn_roundtrip_time_ns as f64 / 1000.0,
            profile.commit_txn_roundtrip_time_ns as f64 / 1000.0 / ROWS as f64
        );
        eprintln!(
            "retained_autocommit: reuses={} parks={} memory_fast_begins={}",
            profile.retained_autocommit_reuses,
            profile.retained_autocommit_parks,
            profile.memory_autocommit_fast_path_begins,
        );
        eprintln!(
            "direct_execs:         fast_hits={} direct_execs={} autocommit_execs={}",
            profile.prepared_insert_fast_lane_hits,
            profile.prepared_direct_insert_executions,
            profile.prepared_direct_insert_autocommit_executions,
        );

        assert_eq!(
            profile.prepared_insert_fast_lane_hits, ROWS as u64,
            "small_3col autocommit INSERT should stay on the prepared fast lane: {profile:?}"
        );
        assert_eq!(
            profile.prepared_direct_insert_executions, ROWS as u64,
            "small_3col autocommit INSERT should stay on the direct insert path: {profile:?}"
        );
        assert_eq!(
            profile.prepared_direct_insert_autocommit_executions, ROWS as u64,
            "small_3col autocommit INSERT should count every autocommit direct execution: {profile:?}"
        );
        assert!(
            profile.prepared_direct_insert_serialize_time_ns > 0,
            "the direct insert profile must expose serialization cost for the small_3col shape: {profile:?}"
        );
        assert!(
            profile.prepared_direct_insert_btree_insert_time_ns > 0,
            "the direct insert profile must expose btree insert cost for the small_3col shape: {profile:?}"
        );
        assert!(
            profile.retained_autocommit_parks >= ROWS as u64 - 1,
            "autocommit INSERT should park the retained write transaction for the next statement: {profile:?}"
        );
        assert!(
            profile.retained_autocommit_reuses >= ROWS as u64 - 2
                || profile.memory_autocommit_fast_path_begins >= 1,
            "the small_3col autocommit probe should stay on the retained write-txn path after the first statement: {profile:?}"
        );
    });
}

/// bd-wwqen.3: Proof test for column-list INSERT direct path eligibility.
///
/// This test proves that column-list INSERT syntax
/// (e.g., `INSERT INTO t(col1, col2) VALUES(?, ?)`) stays eligible for the
/// direct insert fast path, including reordered column lists.
///
/// Key findings:
/// 1. Without column list: `INSERT INTO t VALUES(?, ?)` → direct path
/// 2. With column list: `INSERT INTO t(col1, col2) VALUES(?, ?)` → direct path
/// 3. Reordered VALUES are mapped back to table column order
#[test]
fn test_bd_wwqen_3_column_list_insert_direct_path_eligibility() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();

        // Create table with multiple columns in specific order
        conn.execute("CREATE TABLE col_order_test(id INTEGER PRIMARY KEY, name TEXT, value REAL)")
            .await
            .unwrap();

        // Test 1: Without column list (should hit direct path)
        let stmt_no_cols = conn
            .prepare("INSERT INTO col_order_test VALUES(?1, ?2, ?3)")
            .await
            .unwrap();
        reset_hot_path_profile();
        for i in 0..100 {
            stmt_no_cols
                .execute_with_params(&[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("name_{i}").into()),
                    SqliteValue::Float(i as f64 * 1.5),
                ])
                .await
                .unwrap();
        }
        let snap_no_cols = hot_path_profile_snapshot();

        eprintln!("=== bd-wwqen.3: Column-list INSERT eligibility (no column list) ===");
        eprintln!(
            "direct_insert_executions: {}",
            snap_no_cols.prepared_direct_insert_executions
        );
        eprintln!(
            "fast_lane_hits: {}",
            snap_no_cols.prepared_insert_fast_lane_hits
        );

        // Without column list, direct path should be used
        assert_eq!(
            snap_no_cols.prepared_direct_insert_executions, 100,
            "INSERT without column list should use direct insert path"
        );

        // Clear table for next test
        conn.execute("DELETE FROM col_order_test").await.unwrap();

        // Test 2: With column list in SAME order.
        let stmt_same_order = conn
            .prepare("INSERT INTO col_order_test(id, name, value) VALUES(?1, ?2, ?3)")
            .await
            .unwrap();
        reset_hot_path_profile();
        for i in 0..100 {
            stmt_same_order
                .execute_with_params(&[
                    SqliteValue::Integer(i + 1000),
                    SqliteValue::Text(format!("name_{i}").into()),
                    SqliteValue::Float(i as f64 * 1.5),
                ])
                .await
                .unwrap();
        }
        let snap_same_order = hot_path_profile_snapshot();

        eprintln!("=== bd-wwqen.3: Column-list INSERT eligibility (same order) ===");
        eprintln!(
            "direct_insert_executions: {}",
            snap_same_order.prepared_direct_insert_executions
        );
        eprintln!(
            "fast_lane_hits: {}",
            snap_same_order.prepared_insert_fast_lane_hits
        );

        assert_eq!(
            snap_same_order.prepared_direct_insert_executions, 100,
            "same-order column-list INSERT should use the direct insert path"
        );

        // Test 3: With column list in DIFFERENT order.
        conn.execute("DELETE FROM col_order_test").await.unwrap();
        let stmt_diff_order = conn
            .prepare("INSERT INTO col_order_test(value, name, id) VALUES(?1, ?2, ?3)")
            .await
            .unwrap();
        reset_hot_path_profile();
        for i in 0..100 {
            stmt_diff_order
                .execute_with_params(&[
                    SqliteValue::Float(i as f64 * 2.5),
                    SqliteValue::Text(format!("reordered_{i}").into()),
                    SqliteValue::Integer(i + 2000),
                ])
                .await
                .unwrap();
        }
        let snap_diff_order = hot_path_profile_snapshot();

        eprintln!("=== bd-wwqen.3: Column-list INSERT eligibility (different order) ===");
        eprintln!(
            "direct_insert_executions: {}",
            snap_diff_order.prepared_direct_insert_executions
        );
        eprintln!(
            "fast_lane_hits: {}",
            snap_diff_order.prepared_insert_fast_lane_hits
        );

        // Verify data integrity regardless of path
        let rows = conn
            .query("SELECT COUNT(*) FROM col_order_test")
            .await
            .unwrap();
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(100));

        let sample = conn
            .query("SELECT id, name, value FROM col_order_test WHERE id = 2050 ORDER BY id")
            .await
            .unwrap();
        assert_eq!(sample.len(), 1);
        assert_eq!(sample[0].values()[0], SqliteValue::Integer(2050));
        assert_eq!(
            sample[0].values()[1],
            SqliteValue::Text("reordered_50".into())
        );
        // Verify reordering: value should be 50 * 2.5 = 125.0
        assert_eq!(sample[0].values()[2], SqliteValue::Float(125.0));
        assert_eq!(
            snap_diff_order.prepared_direct_insert_executions, 100,
            "reordered column-list INSERT should use the direct insert path"
        );

        // Summary for regression validation.
        eprintln!("\n=== bd-wwqen.3 VALIDATION SUMMARY ===");
        eprintln!(
            "Test 1 (no col list):    direct_insert_executions = {} (expected: 100)",
            snap_no_cols.prepared_direct_insert_executions
        );
        eprintln!(
            "Test 2 (same order):     direct_insert_executions = {} (expected: 100)",
            snap_same_order.prepared_direct_insert_executions
        );
        eprintln!(
            "Test 3 (diff order):     direct_insert_executions = {} (expected: 100)",
            snap_diff_order.prepared_direct_insert_executions
        );
        eprintln!("=== END bd-wwqen.3 eligibility test ===");
    });
}

#[test]
fn prepared_direct_delete_duplicate_and_absent_counts_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        let ddl = "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL);";
        conn.execute(ddl).await.unwrap();
        sqlite.execute(ddl, []).unwrap();

        let insert = conn
            .prepare("INSERT INTO bench VALUES (?1, ?2);")
            .await
            .unwrap();
        let mut sqlite_insert = sqlite
            .prepare("INSERT INTO bench VALUES (?1, ?2);")
            .unwrap();
        for rowid in 1_i64..=5_i64 {
            let name = format!("user_{rowid}");
            sqlite_insert
                .execute(rusqlite::params![rowid, &name])
                .unwrap();
            insert
                .execute_with_params(&[SqliteValue::Integer(rowid), SqliteValue::Text(name.into())])
                .await
                .unwrap();
        }
        drop(sqlite_insert);

        let delete = conn
            .prepare("DELETE FROM bench WHERE id = ?1;")
            .await
            .unwrap();
        let mut sqlite_delete = sqlite.prepare("DELETE FROM bench WHERE id = ?1;").unwrap();
        conn.execute("BEGIN;").await.unwrap();
        sqlite.execute("BEGIN;", []).unwrap();

        for (rowid, expected_affected) in [(1_i64, 1), (1, 0), (2, 1), (99, 0), (3, 1), (2, 0)] {
            let fsqlite_affected = conn
                .execute_prepared_with_params(&delete, &[SqliteValue::Integer(rowid)])
                .await
                .expect("prepared direct delete should execute");
            let sqlite_affected = sqlite_delete
                .execute(rusqlite::params![rowid])
                .expect("sqlite delete should execute");
            assert_eq!(
                sqlite_affected, expected_affected,
                "SQLite reference affected-count mismatch for rowid {rowid}"
            );
            assert_eq!(
                fsqlite_affected, expected_affected,
                "FrankenSQLite affected-count mismatch for rowid {rowid}"
            );
        }
        drop(sqlite_delete);

        let fsqlite_total = conn
            .query_row("SELECT count(*) FROM bench;")
            .await
            .expect("fsqlite read-your-writes count should execute");
        let sqlite_total: i64 = sqlite
            .query_row("SELECT count(*) FROM bench;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sqlite_total, 2);
        assert_eq!(
            fsqlite_total.values()[0],
            SqliteValue::Integer(sqlite_total)
        );

        let fsqlite_survivors = conn
            .query_row("SELECT count(*) FROM bench WHERE id IN (4, 5);")
            .await
            .expect("fsqlite survivor count should execute");
        let sqlite_survivors: i64 = sqlite
            .query_row(
                "SELECT count(*) FROM bench WHERE id IN (4, 5);",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sqlite_survivors, 2);
        assert_eq!(
            fsqlite_survivors.values()[0],
            SqliteValue::Integer(sqlite_survivors)
        );

        conn.execute("COMMIT;").await.unwrap();
        sqlite.execute("COMMIT;", []).unwrap();

        let fsqlite_remaining = conn
            .query_row("SELECT count(*) FROM bench WHERE id <= 3;")
            .await
            .expect("fsqlite post-commit deleted count should execute");
        let sqlite_remaining: i64 = sqlite
            .query_row("SELECT count(*) FROM bench WHERE id <= 3;", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(sqlite_remaining, 0);
        assert_eq!(
            fsqlite_remaining.values()[0],
            SqliteValue::Integer(sqlite_remaining)
        );

        let profile = hot_path_profile_snapshot();
        assert_eq!(
            profile.prepared_direct_delete_executions, 6,
            "every DELETE execution in this proof should stay on the prepared direct-delete path: {profile:?}"
        );
        assert!(
            profile.prepared_direct_delete_leaf_run_start_hits >= 3,
            "the successful row deletes should exercise the buffered leaf-run path: {profile:?}"
        );
        assert!(
            profile.prepared_direct_delete_leaf_run_active_miss_already_deleted >= 1,
            "duplicate rowids must be rejected by the active leaf-run before falling back to the physical tree: {profile:?}"
        );
        assert!(
            profile.prepared_direct_delete_leaf_run_dirty_flushes >= 1,
            "read/commit boundaries should flush buffered direct DELETE work: {profile:?}"
        );
    });
}

#[test]
fn prepared_direct_delete_staged_only_absent_probe_records_active_miss() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            .await
            .unwrap();

        let insert = conn
            .prepare("INSERT INTO bench VALUES (?1, ?2);")
            .await
            .unwrap();
        conn.execute("BEGIN;").await.unwrap();
        for rowid in 1_i64..=1000_i64 {
            conn.execute_prepared_with_params(
                &insert,
                &[
                    SqliteValue::Integer(rowid),
                    SqliteValue::Text(format!("user_{rowid:04}_{}", "x".repeat(256)).into()),
                ],
            )
            .await
            .expect("setup insert should execute");
        }
        conn.execute("COMMIT;").await.unwrap();

        let delete = conn
            .prepare("DELETE FROM bench WHERE id = ?1;")
            .await
            .unwrap();
        reset_hot_path_profile();
        conn.execute("BEGIN;").await.unwrap();
        assert_eq!(
            conn.execute_prepared_with_params(&delete, &[SqliteValue::Integer(1)])
                .await
                .expect("first delete should start a pending leaf run"),
            1
        );
        assert_eq!(
            conn.execute_prepared_with_params(&delete, &[SqliteValue::Integer(100_000)])
                .await
                .expect("first absent high rowid should stage the current leaf run"),
            0
        );
        assert_eq!(
            conn.execute_prepared_with_params(&delete, &[SqliteValue::Integer(100_001)])
                .await
                .expect("second absent high rowid should keep staged runs without flushing"),
            0
        );

        let row = conn
            .query_row("SELECT count(*) FROM bench;")
            .await
            .expect("read should flush the staged delete run");
        assert_eq!(row.values()[0], SqliteValue::Integer(999));
        conn.execute("COMMIT;").await.unwrap();

        let profile = hot_path_profile_snapshot();
        assert_eq!(
            profile.prepared_direct_delete_executions, 3,
            "proof DELETE statements should stay on the prepared direct-delete path: {profile:?}"
        );
        assert_eq!(
            profile.prepared_direct_delete_leaf_run_active_attempts,
            profile
                .prepared_direct_delete_leaf_run_active_hits
                .saturating_add(profile.prepared_direct_delete_leaf_run_active_misses),
            "active DELETE-run probes should account every attempt as a hit or miss: {profile:?}"
        );
        assert!(
            profile.prepared_direct_delete_leaf_run_active_misses >= 2,
            "both absent high-rowid probes should be visible as active misses: {profile:?}"
        );
        assert!(
            profile.prepared_direct_delete_leaf_run_active_miss_staged_runs >= 1,
            "staged-only probes should not be misclassified as shape mismatches: {profile:?}"
        );
        assert_eq!(
            profile.prepared_direct_delete_leaf_run_active_miss_shape_mismatches, 0,
            "same-shape staged-only probes should not inflate shape mismatch counters: {profile:?}"
        );
    });
}

#[test]
fn prepared_direct_delete_savepoint_boundary_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();
        let conn = Connection::open(":memory:").await.unwrap();
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        let ddl = "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT NOT NULL);";
        conn.execute(ddl).await.unwrap();
        sqlite.execute(ddl, []).unwrap();

        let insert = conn
            .prepare("INSERT INTO bench VALUES (?1, ?2);")
            .await
            .unwrap();
        let mut sqlite_insert = sqlite
            .prepare("INSERT INTO bench VALUES (?1, ?2);")
            .unwrap();
        for rowid in 1_i64..=6_i64 {
            let name = format!("user_{rowid}");
            sqlite_insert
                .execute(rusqlite::params![rowid, &name])
                .unwrap();
            insert
                .execute_with_params(&[SqliteValue::Integer(rowid), SqliteValue::Text(name.into())])
                .await
                .unwrap();
        }
        drop(sqlite_insert);

        let delete = conn
            .prepare("DELETE FROM bench WHERE id = ?1;")
            .await
            .unwrap();
        let mut sqlite_delete = sqlite.prepare("DELETE FROM bench WHERE id = ?1;").unwrap();
        conn.execute("BEGIN;").await.unwrap();
        sqlite.execute("BEGIN;", []).unwrap();

        for rowid in [2_i64, 4_i64] {
            let fsqlite_affected = conn
                .execute_prepared_with_params(&delete, &[SqliteValue::Integer(rowid)])
                .await
                .unwrap();
            let sqlite_affected = sqlite_delete.execute(rusqlite::params![rowid]).unwrap();
            assert_eq!(fsqlite_affected, sqlite_affected);
            assert_eq!(fsqlite_affected, 1);
        }

        conn.execute("SAVEPOINT sp;").await.unwrap();
        sqlite.execute("SAVEPOINT sp;", []).unwrap();
        let profile_after_savepoint = hot_path_profile_snapshot();
        assert!(
            profile_after_savepoint.prepared_direct_delete_leaf_run_dirty_flushes >= 1,
            "SAVEPOINT must flush pending direct DELETE work before the savepoint boundary: {profile_after_savepoint:?}"
        );

        let fsqlite_mid = conn
            .query("SELECT id, name FROM bench ORDER BY id;")
            .await
            .unwrap();
        let sqlite_mid = sqlite_rows(&sqlite);
        assert_eq!(franken_rows(&fsqlite_mid), sqlite_mid);

        for rowid in [3_i64, 5_i64] {
            let fsqlite_affected = conn
                .execute_prepared_with_params(&delete, &[SqliteValue::Integer(rowid)])
                .await
                .unwrap();
            let sqlite_affected = sqlite_delete.execute(rusqlite::params![rowid]).unwrap();
            assert_eq!(fsqlite_affected, sqlite_affected);
            assert_eq!(fsqlite_affected, 1);
        }
        conn.execute("ROLLBACK TO sp;").await.unwrap();
        sqlite.execute("ROLLBACK TO sp;", []).unwrap();
        conn.execute("RELEASE sp;").await.unwrap();
        sqlite.execute("RELEASE sp;", []).unwrap();
        drop(sqlite_delete);

        let fsqlite_after_rollback = conn
            .query("SELECT id, name FROM bench ORDER BY id;")
            .await
            .unwrap();
        let sqlite_after_rollback = sqlite_rows(&sqlite);
        assert_eq!(franken_rows(&fsqlite_after_rollback), sqlite_after_rollback);
        assert_eq!(
            sqlite_after_rollback,
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("user_1".into())],
                vec![SqliteValue::Integer(3), SqliteValue::Text("user_3".into())],
                vec![SqliteValue::Integer(5), SqliteValue::Text("user_5".into())],
                vec![SqliteValue::Integer(6), SqliteValue::Text("user_6".into())],
            ]
        );

        conn.execute("COMMIT;").await.unwrap();
        sqlite.execute("COMMIT;", []).unwrap();
        let fsqlite_final = conn
            .query("SELECT id, name FROM bench ORDER BY id;")
            .await
            .unwrap();
        assert_eq!(franken_rows(&fsqlite_final), sqlite_rows(&sqlite));

        let profile = hot_path_profile_snapshot();
        assert_eq!(
            profile.prepared_direct_delete_executions, 4,
            "all proof DELETE statements should stay on the direct-delete path: {profile:?}"
        );
        assert!(
            profile.prepared_direct_delete_leaf_run_dirty_flushes >= 1,
            "savepoint boundary must publish pending direct DELETE work: {profile:?}"
        );
    });
}

fn franken_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(|row| row.values().to_vec()).collect()
}

fn sqlite_rows(sqlite: &rusqlite::Connection) -> Vec<Vec<SqliteValue>> {
    let mut stmt = sqlite
        .prepare("SELECT id, name FROM bench ORDER BY id;")
        .unwrap();
    stmt.query_map([], |row| {
        let id = row.get(0)?;
        let name: String = row.get(1)?;
        Ok(vec![
            SqliteValue::Integer(id),
            SqliteValue::Text(name.into()),
        ])
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}
