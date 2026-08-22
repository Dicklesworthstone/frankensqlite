//! bd-db300.4.2.3: Statement cache invalidation correctness verification.
//!
//! Proves:
//! 1. Parse/compiled/prepared caches produce hits on repeated identical SQL.
//! 2. DDL (schema_cookie change) invalidates schema-bound prepared state.
//! 3. Results remain correct after invalidation or transparent re-prepare.
//! 4. Rollback to savepoint does NOT spuriously invalidate caches when
//!    schema_cookie is unchanged.
//! 5. Schema generation bump (connection-local DDL) transparently re-prepares
//!    prepared statements (GH #239), matching stock SQLite, instead of
//!    surfacing a permanent SchemaChanged error.
//! 6. File-backed databases share the same correctness/recovery behavior.
//! 7. Warm-loop churn measurement scorecard with structured output.
//!
//! NOTE: Tests T1–T3 and T5–T9 use global hot-path counters. This file
//! serializes those tests with a mutex so they do not leak profiling state or
//! race each other under the default Rust test runner.
//!
//! Run:
//!   cargo test -p fsqlite-core --test statement_cache_invalidation \
//!     -- --test-threads=1 --nocapture
//!
//! Structured log capture:
//!   RUST_LOG="fsqlite.statement_reuse=info" cargo test -p fsqlite-core \
//!     --test statement_cache_invalidation -- --test-threads=1 --nocapture \
//!     2>&1 | grep "fsqlite.statement_reuse"

use fsqlite_core::connection::{
    Connection, hot_path_profile_enabled, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
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

/// Helper: snapshot the parser cache counters.
fn cache_snapshot() -> (u64, u64, u64, u64, u64, u64) {
    let s = hot_path_profile_snapshot();
    (
        s.parser.parse_cache_hits,
        s.parser.parse_cache_misses,
        s.parser.compiled_cache_hits,
        s.parser.compiled_cache_misses,
        s.parser.prepared_cache_hits,
        s.parser.prepared_cache_misses,
    )
}

/// T1: Repeated identical SELECT produces parse cache hit delta.
#[test]
fn test_parse_cache_hits_on_repeated_select() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'a')").await.unwrap();

        // First execution — warms cache.
        let rows1 = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let (ph1, _, _, _, _, _) = cache_snapshot();

        // Second execution — delta should show a hit.
        let rows2 = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let (ph2, _, _, _, _, _) = cache_snapshot();

        assert!(
            ph2 > ph1,
            "parse cache hits should increase on repeated SQL: before={ph1}, after={ph2}"
        );

        // Results must be identical.
        assert_eq!(
            rows1, rows2,
            "repeated query must produce identical results"
        );
    });
}

/// T2: DDL invalidates schema-bound prepared state; subsequent query still correct.
#[test]
fn test_ddl_invalidates_schema_bound_reuse_and_recovers() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'before')")
            .await
            .unwrap();
        let stmt = conn
            .prepare("SELECT val FROM t WHERE id = ?1")
            .await
            .unwrap();

        // Warm the cache with two identical queries.
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let (ph_pre, _, _, _, _, _) = cache_snapshot();
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let (ph_warm, _, _, _, _, _) = cache_snapshot();
        assert!(
            ph_warm > ph_pre,
            "cache should be warm: ph_pre={ph_pre}, ph_warm={ph_warm}"
        );

        // DDL: add a column (changes schema_cookie).
        conn.execute("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0")
            .await
            .unwrap();

        // GH #239: a same-connection ADD COLUMN must transparently re-prepare
        // (no SchemaChanged), returning the unchanged row.
        let rows = stmt
            .query_with_params(&[fsqlite_types::SqliteValue::Integer(1)])
            .await
            .expect("prepared SELECT must transparently re-prepare after ADD COLUMN");
        assert_eq!(rows.len(), 1, "transparent re-prepare should return 1 row");
        assert_eq!(
            rows[0].get(0),
            Some(&fsqlite_types::SqliteValue::Text("before".into())),
            "transparent re-prepare should preserve the original column"
        );

        // Query through the new schema and prove the connection-local caches recover.
        let rows = conn
            .query("SELECT val, extra FROM t WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "query should return the row after ALTER TABLE"
        );
        assert_eq!(
            rows[0].get(0),
            Some(&fsqlite_types::SqliteValue::Text("before".into())),
            "existing column should remain readable after ALTER TABLE"
        );
        assert_eq!(
            rows[0].get(1),
            Some(&fsqlite_types::SqliteValue::Integer(0)),
            "new column should expose its DEFAULT value"
        );

        let (ph_before_reuse, _, _, _, _, _) = cache_snapshot();
        let _ = conn
            .query("SELECT val, extra FROM t WHERE id = 1")
            .await
            .unwrap();
        let (ph_after_reuse, _, _, _, _, _) = cache_snapshot();
        assert!(
            ph_after_reuse > ph_before_reuse,
            "cache should recover after DDL: {ph_before_reuse} -> {ph_after_reuse}"
        );
    });
}

/// T3: Rollback to savepoint does NOT invalidate caches when schema unchanged.
#[test]
fn test_rollback_savepoint_preserves_cache() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'original')")
            .await
            .unwrap();

        // Warm parse cache.
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let (ph_pre, _, _, _, _, _) = cache_snapshot();

        // Savepoint + DML + rollback (no DDL = no schema_cookie change).
        conn.execute("SAVEPOINT sp1").await.unwrap();
        conn.execute("INSERT INTO t VALUES(2, 'temp')")
            .await
            .unwrap();
        conn.execute("ROLLBACK TO sp1").await.unwrap();
        conn.execute("RELEASE sp1").await.unwrap();

        // Query again — should still hit parse cache (schema unchanged).
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let (ph_post, _, _, _, _, _) = cache_snapshot();

        assert!(
            ph_post > ph_pre,
            "parse cache hits should increase after rollback with unchanged schema: before={ph_pre}, after={ph_post}"
        );

        // Result correctness: rollback undid the INSERT.
        let rows = conn.query("SELECT COUNT(*) FROM t").await.unwrap();
        let count = rows[0].get(0).unwrap();
        assert_eq!(
            count,
            &fsqlite_types::SqliteValue::Integer(1),
            "rollback should undo the INSERT, leaving 1 row"
        );
    });
}

/// T4: Schema generation bump invalidates prepared statements.
#[test]
fn test_schema_generation_invalidates_prepared() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'v1')").await.unwrap();

        // Prepare and execute.
        let stmt = conn
            .prepare("SELECT val FROM t WHERE id = ?1")
            .await
            .unwrap();
        let rows1 = stmt
            .query_with_params(&[fsqlite_types::SqliteValue::Integer(1)])
            .await
            .unwrap();
        assert!(!rows1.is_empty(), "prepared statement should return rows");

        // DDL changes schema_generation.
        conn.execute("CREATE TABLE t2(x INTEGER)").await.unwrap();

        // GH #239: the prepared statement must transparently re-prepare on a
        // same-connection schema change and still return the correct rows.
        let rows = stmt
            .query_with_params(&[fsqlite_types::SqliteValue::Integer(1)])
            .await
            .expect("prepared SELECT must transparently re-prepare after DDL");
        assert!(
            !rows.is_empty(),
            "re-prepared result should still be correct"
        );
    });
}

/// T5: Repeated identical INSERT uses prepared cache.
#[test]
fn test_prepared_cache_hits_on_repeated_insert() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();

        // Two identical INSERTs.
        conn.execute("INSERT INTO t VALUES(1, 'a')").await.unwrap();
        let (_, _, _, _, ph1, _) = cache_snapshot();
        conn.execute("INSERT INTO t VALUES(1, 'a')")
            .await
            .unwrap_or_default(); // PK conflict OK
        let (_, _, _, _, ph2, _) = cache_snapshot();

        // The prepared-template cache now owns identical ad-hoc DML reuse; a hit here
        // bypasses the compiled-program cache entirely.
        assert!(
            ph2 > ph1,
            "identical SQL should produce a prepared cache hit: before={ph1}, after={ph2}"
        );
    });
}

/// T6: File-backed database has same invalidation behavior.
#[test]
fn test_file_backed_cache_invalidation() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();

        let conn = Connection::open(path).await.unwrap();
        conn.execute("PRAGMA journal_mode = WAL").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'file-backed')")
            .await
            .unwrap();
        let stmt = conn
            .prepare("SELECT val FROM t WHERE id = ?1")
            .await
            .unwrap();

        // Warm cache.
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let (ph1, _, _, _, _, _) = cache_snapshot();

        // Repeat — hit delta.
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let (ph2, _, _, _, _, _) = cache_snapshot();
        assert!(
            ph2 > ph1,
            "file-backed: parse cache should hit on repeated query: {ph1} -> {ph2}"
        );

        // DDL invalidation.
        conn.execute("CREATE TABLE t2(x INTEGER)").await.unwrap();
        // GH #239: same-connection DDL transparently re-prepares (file-backed).
        let rows = stmt
            .query_with_params(&[fsqlite_types::SqliteValue::Integer(1)])
            .await
            .expect("file-backed prepared SELECT must transparently re-prepare after DDL");
        assert_eq!(
            rows.len(),
            1,
            "file-backed: transparent re-prepare should return 1 row"
        );
        assert_eq!(
            rows[0].get(0),
            Some(&fsqlite_types::SqliteValue::Text("file-backed".into())),
            "file-backed: transparent re-prepare should preserve row data"
        );

        let rows = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        assert_eq!(
            rows.len(),
            1,
            "file-backed: result must be correct after DDL"
        );
        assert_eq!(
            rows[0].get(0),
            Some(&fsqlite_types::SqliteValue::Text("file-backed".into())),
            "file-backed: row data must survive DDL"
        );

        let (ph_pre_reuse, _, _, _, _, _) = cache_snapshot();
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let (ph_post_reuse, _, _, _, _, _) = cache_snapshot();
        assert!(
            ph_post_reuse > ph_pre_reuse,
            "file-backed: cache should recover after DDL: {ph_pre_reuse} -> {ph_post_reuse}"
        );
    });
}

/// T7: Churn measurement scorecard — the authoritative artifact for bd-db300.4.2.3.
#[test]
fn test_churn_measurement_scorecard() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE bench(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            .await
            .unwrap();

        let insert_sql = "INSERT INTO bench VALUES(1, 'test', 42)";
        let select_sql = "SELECT name, score FROM bench WHERE id = 1";
        let delete_sql = "DELETE FROM bench WHERE id = 1";

        // Cold iteration.
        conn.execute(insert_sql).await.unwrap();
        let _ = conn.query(select_sql).await.unwrap();
        let snap_cold = hot_path_profile_snapshot();
        conn.execute(delete_sql).await.unwrap();

        // Warm loop (100 iterations).
        reset_hot_path_profile();
        for _ in 0..100 {
            conn.execute(insert_sql).await.unwrap();
            let _ = conn.query(select_sql).await.unwrap();
            conn.execute(delete_sql).await.unwrap();
        }
        let snap_warm = hot_path_profile_snapshot();

        // ─── Scorecard output ─────────────────────────────────────
        eprintln!("=== bd-db300.4.2.3 Churn Measurement Scorecard ===");
        eprintln!("Cold (first iteration):");
        eprintln!(
            "  parse:    hits={:>4}  misses={:>4}",
            snap_cold.parser.parse_cache_hits, snap_cold.parser.parse_cache_misses
        );
        eprintln!(
            "  compiled: hits={:>4}  misses={:>4}",
            snap_cold.parser.compiled_cache_hits, snap_cold.parser.compiled_cache_misses
        );
        eprintln!(
            "  prepared: hits={:>4}  misses={:>4}",
            snap_cold.parser.prepared_cache_hits, snap_cold.parser.prepared_cache_misses
        );
        eprintln!("Warm (100 iterations, 3 statements each = 300 statement dispatches):");
        eprintln!(
            "  parse:    hits={:>4}  misses={:>4}  hit_rate={:.1}%",
            snap_warm.parser.parse_cache_hits,
            snap_warm.parser.parse_cache_misses,
            100.0 * snap_warm.parser.parse_cache_hits as f64
                / (snap_warm.parser.parse_cache_hits + snap_warm.parser.parse_cache_misses).max(1)
                    as f64,
        );
        eprintln!(
            "  compiled: hits={:>4}  misses={:>4}  hit_rate={:.1}%",
            snap_warm.parser.compiled_cache_hits,
            snap_warm.parser.compiled_cache_misses,
            100.0 * snap_warm.parser.compiled_cache_hits as f64
                / (snap_warm.parser.compiled_cache_hits + snap_warm.parser.compiled_cache_misses)
                    .max(1) as f64,
        );
        eprintln!(
            "  prepared: hits={:>4}  misses={:>4}",
            snap_warm.parser.prepared_cache_hits, snap_warm.parser.prepared_cache_misses
        );
        eprintln!(
            "  parse_time_ns={}, compile_time_ns={}",
            snap_warm.parser.parse_time_ns, snap_warm.parser.compile_time_ns
        );
        eprintln!("=== END SCORECARD ===");

        // bd-6eyrg.1 fast-path counters.
        eprintln!(
            "  fast_path={}, slow_path={}",
            snap_warm.parser.fast_path_executions, snap_warm.parser.slow_path_executions
        );

        // Assertion: warm loop cache hits should dominate misses.
        assert!(
            snap_warm.parser.parse_cache_hits > snap_warm.parser.parse_cache_misses,
            "warm loop: parse cache hits ({}) should exceed misses ({})",
            snap_warm.parser.parse_cache_hits,
            snap_warm.parser.parse_cache_misses
        );
    });
}

/// T8: Full transaction rollback with DDL preserves cache correctness.
///
/// `BEGIN; CREATE TABLE ...; ROLLBACK` should leave the schema_cookie
/// unchanged (the DDL was rolled back). Same-connection schema_generation
/// may still conservatively invalidate local prepared state, so this test
/// proves post-rollback correctness rather than a mandatory cache hit.
#[test]
fn test_rollback_transaction_with_ddl_preserves_cache() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'stable')")
            .await
            .unwrap();

        // Warm the cache.
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        let _ = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();

        // DDL inside a rolled-back transaction.
        conn.execute("BEGIN").await.unwrap();
        conn.execute("CREATE TABLE t_temp(x INTEGER)")
            .await
            .unwrap();
        conn.execute("ROLLBACK").await.unwrap();

        // Query after rollback — schema_cookie is restored, but same-connection
        // schema_generation may have bumped conservatively. Accept either a hit
        // or a miss here; the contract is correctness after rollback.
        let rows = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        assert_eq!(rows.len(), 1, "rolled-back DDL should not affect data");
        assert_eq!(
            rows[0].get(0),
            Some(&fsqlite_types::SqliteValue::Text("stable".into())),
            "result must be correct after rolled-back DDL"
        );

        // t_temp should not exist.
        let result = conn.query("SELECT * FROM t_temp").await;
        assert!(result.is_err(), "t_temp should not exist after ROLLBACK");
    });
}

/// T9: Rapid multi-DDL schema churn — cache invalidation and recovery.
///
/// Exercises: CREATE TABLE, ALTER TABLE, DROP TABLE in rapid sequence,
/// verifying that queries against surviving tables return correct results
/// and that the cache recovers to hit state after the churn settles.
#[test]
fn test_rapid_schema_churn_invalidation_and_recovery() {
    asupersync::test_utils::run_test(|| async {
        let _profile_guard = HotPathProfileTestGuard::new();

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE stable(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO stable VALUES(1, 'anchor')")
            .await
            .unwrap();

        // Warm cache on the stable table.
        let _ = conn
            .query("SELECT val FROM stable WHERE id = 1")
            .await
            .unwrap();
        let _ = conn
            .query("SELECT val FROM stable WHERE id = 1")
            .await
            .unwrap();

        // Schema churn storm.
        for i in 0..5 {
            conn.execute(&format!("CREATE TABLE churn_{i}(x INTEGER)"))
                .await
                .unwrap();
        }
        for i in 0..5 {
            conn.execute(&format!("DROP TABLE churn_{i}"))
                .await
                .unwrap();
        }
        conn.execute("ALTER TABLE stable ADD COLUMN extra INTEGER DEFAULT 0")
            .await
            .unwrap();

        // After churn, verify the stable table is still correct.
        let rows = conn
            .query("SELECT val FROM stable WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(0),
            Some(&fsqlite_types::SqliteValue::Text("anchor".into())),
            "stable table data must survive schema churn"
        );

        // Verify cache recovers: two identical queries should produce a hit.
        let (ph_pre, _, _, _, _, _) = cache_snapshot();
        let _ = conn
            .query("SELECT val FROM stable WHERE id = 1")
            .await
            .unwrap();
        let (ph_post, _, _, _, _, _) = cache_snapshot();
        assert!(
            ph_post > ph_pre,
            "cache should recover after schema churn: {ph_pre} -> {ph_post}"
        );
    });
}

/// T10: Re-prepare after SchemaChanged returns correct results.
///
/// After a prepared statement gets SchemaChanged, the caller must be able
/// to re-prepare the same SQL and get correct results for the new schema.
#[test]
fn test_reprepare_after_schema_changed_returns_correct_results() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1, 'original')")
            .await
            .unwrap();

        // Prepare against old schema.
        let stmt = conn
            .prepare("SELECT val FROM t WHERE id = ?1")
            .await
            .unwrap();
        let rows = stmt
            .query_with_params(&[fsqlite_types::SqliteValue::Integer(1)])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(0),
            Some(&fsqlite_types::SqliteValue::Text("original".into())),
            "prepared query should read the original value before schema churn"
        );

        // DDL changes the schema.
        conn.execute("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 42")
            .await
            .unwrap();

        // Old prepared stmt should fail or transparently re-prepare.
        let result = stmt
            .query_with_params(&[fsqlite_types::SqliteValue::Integer(1)])
            .await;
        let schema_changed = matches!(result, Err(fsqlite_error::FrankenError::SchemaChanged));
        // GH #239: FrankenSQLite now transparently re-prepares a same-connection
        // schema change, so the caller never observes SchemaChanged here. The
        // `if schema_changed` branch documents the historical manual-reprepare
        // fallback and is retained only as a reference.
        assert!(
            !schema_changed,
            "prepared statement must transparently re-prepare after same-connection DDL"
        );

        if schema_changed {
            // Re-prepare with the NEW schema.
            let stmt2 = conn
                .prepare("SELECT val, extra FROM t WHERE id = ?1")
                .await
                .unwrap();
            let rows2 = stmt2
                .query_with_params(&[fsqlite_types::SqliteValue::Integer(1)])
                .await
                .unwrap();
            assert_eq!(rows2.len(), 1, "re-prepared query should return 1 row");
            assert_eq!(
                rows2[0].get(0),
                Some(&fsqlite_types::SqliteValue::Text("original".into())),
                "re-prepared query should preserve the original column value"
            );
            // The new column should have the default value.
            assert_eq!(
                rows2[0].get(1),
                Some(&fsqlite_types::SqliteValue::Integer(42)),
                "new column should have DEFAULT 42"
            );
        } else {
            // Transparent re-prepare — verify it returned the correct row.
            let rows = result.unwrap();
            assert_eq!(rows.len(), 1, "transparent re-prepare should return 1 row");
            assert_eq!(
                rows[0].get(0),
                Some(&fsqlite_types::SqliteValue::Text("original".into())),
                "transparent re-prepare should preserve the original value"
            );
        }
    });
}
