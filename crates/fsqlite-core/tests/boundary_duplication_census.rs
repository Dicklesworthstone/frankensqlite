//! bd-db300.5.2.2.1: Boundary-duplication census for prepared DML.
//!
//! Instruments and attributes the common prepared-DML path to expose how many
//! times each bookkeeping phase fires per statement execution:
//! - schema_refresh (via refresh_prepared_schema_state)
//! - publication_bind (via bind_pager_publication)
//! - memdb_refresh (via refresh_memdb_if_stale — actual refresh, not skip)
//! - begin_refresh (via ensure_autocommit_txn — file-backed begin work)
//! - commit_refresh (via resolve_autocommit_txn — commit publication work)
//!
//! Run:
//!   cargo test -p fsqlite-core --test boundary_duplication_census \
//!     -- --test-threads=1 --nocapture
//!
//! Structured log capture:
//!   RUST_LOG="fsqlite.execute_path=debug,fsqlite.statement_reuse=info" \
//!     cargo test -p fsqlite-core --test boundary_duplication_census \
//!     -- --test-threads=1 --nocapture

use std::sync::{Mutex, MutexGuard};

use fsqlite_core::connection::{
    Connection, hot_path_profile_enabled, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};

static CENSUS_TEST_LOCK: Mutex<()> = Mutex::new(());

struct CensusProfileGuard {
    _lock: MutexGuard<'static, ()>,
    previous_enabled: bool,
}

impl CensusProfileGuard {
    fn new() -> Self {
        let lock = CENSUS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let previous_enabled = hot_path_profile_enabled();
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        Self {
            _lock: lock,
            previous_enabled,
        }
    }
}

impl Drop for CensusProfileGuard {
    fn drop(&mut self) {
        reset_hot_path_profile();
        set_hot_path_profile_enabled(self.previous_enabled);
    }
}

/// Snapshot the census-relevant counters.
fn census_snapshot() -> CensusCounters {
    let s = hot_path_profile_snapshot();
    CensusCounters {
        schema_refresh: s.prepared_schema_refreshes,
        schema_lightweight: s.prepared_schema_lightweight_refreshes,
        schema_full_reload: s.prepared_schema_full_reloads,
        publication_bind: s.pager_publication_refreshes,
        begin_refresh: s.begin_refresh_count,
        commit_refresh: s.commit_refresh_count,
        memdb_refresh: s.memdb_refresh_count,
        fast_path: s.parser.fast_path_executions,
        slow_path: s.parser.slow_path_executions,
        bg_status: s.background_status_checks,
        prepared_insert_fast_lane_hits: s.prepared_insert_fast_lane_hits,
    }
}

#[derive(Debug, Clone)]
struct CensusCounters {
    schema_refresh: u64,
    schema_lightweight: u64,
    schema_full_reload: u64,
    publication_bind: u64,
    begin_refresh: u64,
    commit_refresh: u64,
    memdb_refresh: u64,
    fast_path: u64,
    slow_path: u64,
    bg_status: u64,
    prepared_insert_fast_lane_hits: u64,
}

impl CensusCounters {
    fn delta(&self, before: &Self) -> Self {
        Self {
            schema_refresh: self.schema_refresh.saturating_sub(before.schema_refresh),
            schema_lightweight: self
                .schema_lightweight
                .saturating_sub(before.schema_lightweight),
            schema_full_reload: self
                .schema_full_reload
                .saturating_sub(before.schema_full_reload),
            publication_bind: self
                .publication_bind
                .saturating_sub(before.publication_bind),
            begin_refresh: self.begin_refresh.saturating_sub(before.begin_refresh),
            commit_refresh: self.commit_refresh.saturating_sub(before.commit_refresh),
            memdb_refresh: self.memdb_refresh.saturating_sub(before.memdb_refresh),
            fast_path: self.fast_path.saturating_sub(before.fast_path),
            slow_path: self.slow_path.saturating_sub(before.slow_path),
            bg_status: self.bg_status.saturating_sub(before.bg_status),
            prepared_insert_fast_lane_hits: self
                .prepared_insert_fast_lane_hits
                .saturating_sub(before.prepared_insert_fast_lane_hits),
        }
    }

    fn print(&self, label: &str) {
        eprintln!("  [{label}]");
        eprintln!(
            "    schema_refresh={} (lightweight={}, full_reload={})",
            self.schema_refresh, self.schema_lightweight, self.schema_full_reload
        );
        eprintln!("    publication_bind={}", self.publication_bind);
        eprintln!("    begin_refresh={}", self.begin_refresh);
        eprintln!("    commit_refresh={}", self.commit_refresh);
        eprintln!("    memdb_refresh={}", self.memdb_refresh);
        eprintln!(
            "    fast_path={}, slow_path={}",
            self.fast_path, self.slow_path
        );
        eprintln!("    bg_status={}", self.bg_status);
        eprintln!(
            "    prepared_insert_fast_lane_hits={}",
            self.prepared_insert_fast_lane_hits
        );
    }
}

/// C1: Single prepared INSERT in autocommit mode — census per statement.
#[test]
fn test_census_single_prepared_insert_autocommit() {
    let _profile_guard = CensusProfileGuard::new();

    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let stmt = conn.prepare("INSERT INTO t VALUES(?1, ?2)").unwrap();

    // Steady-state: execute 10 identical prepared INSERTs.
    let before = census_snapshot();
    for i in 0..10 {
        stmt.execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(i),
            fsqlite_types::SqliteValue::Text(format!("v{i}").into()),
        ])
        .unwrap();
    }
    let after = census_snapshot();
    let delta = after.delta(&before);

    eprintln!("=== C1: Single prepared INSERT ×10, :memory: autocommit ===");
    delta.print("per-10-stmts");

    // The direct prepared INSERT fast lane should be taken for all 10. The
    // broader parser fast-path counter also includes internal helper work.
    assert_eq!(
        delta.prepared_insert_fast_lane_hits, 10,
        "all 10 INSERTs should use the prepared INSERT fast lane"
    );
    assert_eq!(
        delta.slow_path, 0,
        "prepared INSERT should avoid slow fallback"
    );

    // :memory: autocommit should NOT trigger begin_refresh (uses fast path).
    // begin_refresh only fires for file-backed databases.
    assert_eq!(
        delta.begin_refresh, 0,
        ":memory: should not trigger begin_refresh (uses memory fast path)"
    );
}

/// C2: File-backed prepared INSERT — exposes duplication.
#[test]
fn test_census_file_backed_prepared_insert() {
    let _profile_guard = CensusProfileGuard::new();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();

    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute("PRAGMA synchronous = NORMAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let stmt = conn.prepare("INSERT INTO t VALUES(?1, ?2)").unwrap();

    // Warm: one execution to establish baseline.
    stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(0),
        fsqlite_types::SqliteValue::Text("warm".into()),
    ])
    .unwrap();

    // Census: 10 steady-state prepared INSERTs.
    let before = census_snapshot();
    for i in 1..=10 {
        stmt.execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(i),
            fsqlite_types::SqliteValue::Text(format!("v{i}").into()),
        ])
        .unwrap();
    }
    let after = census_snapshot();
    let delta = after.delta(&before);

    eprintln!("=== C2: File-backed prepared INSERT ×10, WAL autocommit ===");
    delta.print("per-10-stmts");

    // All 10 should take the direct prepared INSERT fast lane. The broader
    // parser fast-path counter also includes internal helper statements.
    assert_eq!(
        delta.prepared_insert_fast_lane_hits, 10,
        "all 10 INSERTs should use the prepared INSERT fast lane"
    );
    assert_eq!(
        delta.slow_path, 0,
        "prepared INSERT should avoid slow fallback"
    );
    assert_eq!(
        delta.memdb_refresh, 0,
        "file-backed WAL path should not pay memdb refresh work in this census"
    );

    // Key census output: how many begin + commit refreshes per 10 statements?
    eprintln!(
        "  DUPLICATION RATIO: begin_refresh/stmt = {:.1}, commit_refresh/stmt = {:.1}",
        delta.begin_refresh as f64 / 10.0,
        delta.commit_refresh as f64 / 10.0,
    );
    eprintln!(
        "  publication_bind/stmt = {:.1}",
        delta.publication_bind as f64 / 10.0
    );
}

/// C3: Explicit transaction — begin/commit work should happen once, not per statement.
#[test]
fn test_census_explicit_transaction_prepared_insert() {
    let _profile_guard = CensusProfileGuard::new();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();

    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let stmt = conn.prepare("INSERT INTO t VALUES(?1, ?2)").unwrap();

    // Explicit transaction wrapping 10 INSERTs.
    conn.execute("BEGIN").unwrap();
    let before = census_snapshot();
    for i in 0..10 {
        stmt.execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(i),
            fsqlite_types::SqliteValue::Text(format!("v{i}").into()),
        ])
        .unwrap();
    }
    let after = census_snapshot();
    conn.execute("COMMIT").unwrap();
    let delta = after.delta(&before);

    eprintln!("=== C3: Explicit txn, file-backed INSERT ×10 ===");
    delta.print("per-10-stmts");

    // The direct prepared INSERT fast lane should still handle every user
    // statement. Global parser fast-path counters include internal helper
    // traffic and are not one-to-one with user statements anymore.
    assert_eq!(
        delta.prepared_insert_fast_lane_hits, 10,
        "explicit txn should still use the prepared INSERT fast lane"
    );
    assert_eq!(
        delta.slow_path, 0,
        "explicit txn should avoid slow fallback"
    );

    // Explicit transactions should materially reduce begin/commit refresh work
    // versus per-statement autocommit, even if some internal helper activity
    // still contributes to the global counters.
    assert!(
        delta.begin_refresh < 10,
        "explicit txn begin_refresh should be below one-per-statement duplication: {delta:?}"
    );
    assert!(
        delta.commit_refresh < 10,
        "explicit txn commit_refresh should be below one-per-statement duplication: {delta:?}"
    );
}

/// C4: Schema invalidation during census — one DDL, verify counters spike then recover.
#[test]
fn test_census_schema_invalidation_spike() {
    let _profile_guard = CensusProfileGuard::new();

    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let stmt = conn.prepare("INSERT INTO t VALUES(?1, ?2)").unwrap();

    // 5 steady-state executions.
    let before = census_snapshot();
    for i in 0..5 {
        stmt.execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(i),
            fsqlite_types::SqliteValue::Text("pre".into()),
        ])
        .unwrap();
    }
    let mid = census_snapshot();
    let delta_pre = mid.delta(&before);

    // DDL — invalidates caches.
    conn.execute("CREATE TABLE t2(x INTEGER)").unwrap();

    // The old prepared stmt will get SchemaChanged.
    let result = stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(99),
        fsqlite_types::SqliteValue::Text("post-ddl".into()),
    ]);
    let after = census_snapshot();
    let delta_post = after.delta(&mid);

    eprintln!("=== C4: Schema invalidation spike ===");
    eprintln!("Pre-DDL (5 stmts):");
    delta_pre.print("pre");
    eprintln!("Post-DDL (1 stmt attempt):");
    delta_post.print("post");
    eprintln!(
        "  stmt_result: {:?}",
        result.as_ref().map(|_| "ok").unwrap_or("err")
    );
}

/// C5: Full census scorecard — the authoritative artifact for bd-db300.5.2.2.1.
#[test]
fn test_census_full_scorecard() {
    let _profile_guard = CensusProfileGuard::new();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();

    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute("PRAGMA synchronous = NORMAL").unwrap();
    conn.execute("CREATE TABLE bench(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
        .unwrap();

    let ins = conn
        .prepare("INSERT INTO bench VALUES(?1, ?2, ?3)")
        .unwrap();

    // Warm.
    ins.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(0),
        fsqlite_types::SqliteValue::Text("warm".into()),
        fsqlite_types::SqliteValue::Integer(0),
    ])
    .unwrap();

    // Census: 100 iterations.
    reset_hot_path_profile();
    for i in 1..=100 {
        ins.execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(i),
            fsqlite_types::SqliteValue::Text(format!("r{i}").into()),
            fsqlite_types::SqliteValue::Integer(i * 7),
        ])
        .unwrap();
    }
    let snap = census_snapshot();

    eprintln!("=== bd-db300.5.2.2.1 Full Census Scorecard (100 file-backed prepared INSERTs) ===");
    eprintln!(
        "  schema_refresh:     {:>4}  ({:.2}/stmt)",
        snap.schema_refresh,
        snap.schema_refresh as f64 / 100.0
    );
    eprintln!(
        "  publication_bind:   {:>4}  ({:.2}/stmt)",
        snap.publication_bind,
        snap.publication_bind as f64 / 100.0
    );
    eprintln!(
        "  begin_refresh:      {:>4}  ({:.2}/stmt)",
        snap.begin_refresh,
        snap.begin_refresh as f64 / 100.0
    );
    eprintln!(
        "  commit_refresh:     {:>4}  ({:.2}/stmt)",
        snap.commit_refresh,
        snap.commit_refresh as f64 / 100.0
    );
    eprintln!(
        "  memdb_refresh:      {:>4}  ({:.2}/stmt)",
        snap.memdb_refresh,
        snap.memdb_refresh as f64 / 100.0
    );
    eprintln!(
        "  fast_path:          {:>4}  ({:.2}/stmt)",
        snap.fast_path,
        snap.fast_path as f64 / 100.0
    );
    eprintln!(
        "  slow_path:          {:>4}  ({:.2}/stmt)",
        snap.slow_path,
        snap.slow_path as f64 / 100.0
    );
    eprintln!(
        "  bg_status:          {:>4}  ({:.2}/stmt)",
        snap.bg_status,
        snap.bg_status as f64 / 100.0
    );
    eprintln!("=== END SCORECARD ===");

    // Assertion: fast path should dominate.
    assert!(
        snap.fast_path >= 100,
        "all 100 prepared INSERTs should use fast path: fast={}, slow={}",
        snap.fast_path,
        snap.slow_path
    );
}
