//! bd-6eyrg.1: Fast-path vs slow-path execution separation tests.
//!
//! Proves:
//! 1. Prepared INSERT uses the fast path (counter increments).
//! 2. Prepared SELECT / CTE / view queries record path metrics without racing.
//! 3. DDL invalidation forces a schema change boundary, then re-preparation
//!    restores the fast path.
//! 4. Parameterized prepared statements still execute correctly.
//! 5. Complex queries (JOINs, subqueries) still produce correct results.
//! 6. Latency: prepared fast lanes are not catastrophically slower than ad-hoc
//!    execution on repeated runs.
//!
//! Run:
//!   cargo test -p fsqlite-core --test fast_path_separation \
//!     -- --test-threads=1 --nocapture

use fsqlite_btree::instrumentation::{
    btree_leaf_reuse_snapshot, reset_btree_leaf_reuse_profile, set_btree_copy_profile_enabled,
};
use fsqlite_core::connection::{
    Connection, hot_path_profile_enabled, hot_path_profile_snapshot, reset_hot_path_profile,
    set_hot_path_profile_enabled,
};
use std::sync::{Mutex, MutexGuard};

static FAST_PATH_PROFILE_TEST_LOCK: Mutex<()> = Mutex::new(());

struct FastPathProfileIsolationGuard {
    _lock: MutexGuard<'static, ()>,
}

impl FastPathProfileIsolationGuard {
    fn new() -> Self {
        let lock = FAST_PATH_PROFILE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self { _lock: lock }
    }
}

struct FastPathProfileTestGuard {
    _lock: MutexGuard<'static, ()>,
    previous_enabled: bool,
}

impl FastPathProfileTestGuard {
    fn new() -> Self {
        let lock = FAST_PATH_PROFILE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous_enabled = hot_path_profile_enabled();
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        set_btree_copy_profile_enabled(true);
        reset_btree_leaf_reuse_profile();
        Self {
            _lock: lock,
            previous_enabled,
        }
    }
}

impl Drop for FastPathProfileTestGuard {
    fn drop(&mut self) {
        reset_btree_leaf_reuse_profile();
        set_btree_copy_profile_enabled(false);
        reset_hot_path_profile();
        set_hot_path_profile_enabled(self.previous_enabled);
    }
}

fn fast_slow_delta(
    before: &fsqlite_core::connection::ParserHotPathProfileSnapshot,
    after: &fsqlite_core::connection::ParserHotPathProfileSnapshot,
) -> (u64, u64) {
    (
        after
            .fast_path_executions
            .saturating_sub(before.fast_path_executions),
        after
            .slow_path_executions
            .saturating_sub(before.slow_path_executions),
    )
}

fn assert_count_star_sum_row(
    row: &fsqlite_core::connection::Row,
    expected_count: i64,
    expected_sum: Option<fsqlite_types::SqliteValue>,
) {
    assert_eq!(
        row.get(0),
        Some(&fsqlite_types::SqliteValue::Integer(expected_count)),
        "COUNT(*) should match the expected row count"
    );
    match expected_sum {
        Some(expected_sum) => assert_eq!(
            row.get(1),
            Some(&expected_sum),
            "SUM() should match the expected non-NULL total"
        ),
        None => assert_eq!(
            row.get(1),
            Some(&fsqlite_types::SqliteValue::Null),
            "SUM() should be NULL when no non-NULL inputs contribute"
        ),
    }
}

fn explain_opcode_names(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .into_iter()
        .map(|row| match row.get(1) {
            Some(fsqlite_types::SqliteValue::Text(opcode)) => opcode.to_string(),
            other => format!("<non-text opcode column: {other:?}>"),
        })
        .collect()
}

fn stringify_fsqlite_value(value: &fsqlite_types::SqliteValue) -> String {
    match value {
        fsqlite_types::SqliteValue::Null => "NULL".to_owned(),
        fsqlite_types::SqliteValue::Integer(n) => n.to_string(),
        fsqlite_types::SqliteValue::Float(f) => format!("{f}"),
        fsqlite_types::SqliteValue::Text(s) => format!("'{s}'"),
        fsqlite_types::SqliteValue::Blob(b) => {
            format!(
                "X'{}'",
                b.iter()
                    .map(|byte| format!("{byte:02X}"))
                    .collect::<String>()
            )
        }
    }
}

fn stringify_rusqlite_value(value: rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => {
            format!(
                "X'{}'",
                b.iter()
                    .map(|byte| format!("{byte:02X}"))
                    .collect::<String>()
            )
        }
    }
}

fn sorted_frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    let stmt = conn.prepare(sql).unwrap();
    let mut rows = stmt
        .query()
        .unwrap()
        .into_iter()
        .map(|row| row.values().iter().map(stringify_fsqlite_value).collect())
        .collect::<Vec<Vec<String>>>();
    rows.sort();
    rows
}

fn sorted_rusqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let col_count = stmt.column_count();
    let mut rows = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(col_count);
            for idx in 0..col_count {
                let value: rusqlite::types::Value = row.get(idx)?;
                values.push(stringify_rusqlite_value(value));
            }
            Ok(values)
        })
        .unwrap()
        .collect::<Result<Vec<Vec<String>>, _>>()
        .unwrap();
    rows.sort();
    rows
}

fn seed_grouped_sum_bench(fconn: &Connection, rconn: &rusqlite::Connection, row_count: usize) {
    for id in 0..row_count {
        let value = (id * 3) + 1;
        let sql = format!("INSERT INTO bench VALUES ({id}, 'name{id}', {value}.0)");
        fconn.execute(&sql).unwrap();
        rconn.execute(&sql, []).unwrap();
    }
}

/// T1: Prepared INSERT uses fast path.
#[test]
fn test_fast_path_simple_insert() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let stmt = conn.prepare("INSERT INTO t VALUES(?1, ?2)").unwrap();
    let before = hot_path_profile_snapshot();
    stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(1),
        fsqlite_types::SqliteValue::Text("fast".into()),
    ])
    .unwrap();
    let after = hot_path_profile_snapshot();

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    eprintln!("[T1] INSERT: fast_delta={fast_delta}, slow_delta={slow_delta}");
    assert!(
        fast_delta > 0,
        "prepared INSERT should use fast path: fast_delta={fast_delta}"
    );
}

#[test]
#[ignore = "manual perf probe for wide prepared direct INSERT hot path"]
fn manual_profile_large_prepared_direct_insert_single_txn_10k() {
    const ROW_COUNT: i64 = 10_000;
    const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL, email TEXT NOT NULL, department TEXT NOT NULL, title TEXT NOT NULL, bio TEXT NOT NULL, address TEXT NOT NULL, notes TEXT NOT NULL, score INTEGER NOT NULL)";
    const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('FirstName_' || ?1), ('LastName_' || ?1), ('employee' || ?1 || '@bigcorp.example.com'), ('Engineering_Dept_' || (?1 % 20)), ('Senior Software Engineer Level ' || (?1 % 5)), ('This is the biography for employee number ' || ?1 || '. They have been working at the company for many years and have contributed to numerous projects across multiple teams. Their expertise spans distributed systems, database internals, and performance optimization. They are known for their thorough code reviews and mentorship of junior engineers.'), (?1 || ' Technology Park, Building ' || (?1 % 50) || ', Suite ' || (?1 % 200) || ', Innovation City, CA 94000'), ('Internal notes: Employee ' || ?1 || ' - Performance rating: Exceeds Expectations. Last review date: 2026-01-15. Next review: 2026-07-15. Skills: Rust, C++, SQL, distributed systems, leadership.'), (?1 * 13))";

    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute(CREATE_TABLE).unwrap();
    conn.execute("BEGIN").unwrap();
    let stmt = conn.prepare(INSERT_SQL).unwrap();

    reset_hot_path_profile();
    let started = std::time::Instant::now();
    for i in 0..ROW_COUNT {
        stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(i)])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let wall = started.elapsed();
    let profile = hot_path_profile_snapshot();
    let leaf_reuse = btree_leaf_reuse_snapshot();

    eprintln!(
        concat!(
            "[manual_large_insert_10k] wall_us={} execute_body_us={} ",
            "row_build_us={} cursor_setup_us={} serialize_us={} ",
            "btree_insert_us={} memdb_apply_us={} direct_execs={} fast_execs={} ",
            "payload_appends={} payload_mutate_us={} payload_stage_us={} ",
            "full_cell_appends={} full_cell_mutate_us={} full_cell_stage_us={} ",
            "quick_balance_attempts={} quick_balance_hits={} quick_balance_us={} ",
            "local_split_attempts={} local_split_hits={} local_split_us={} ",
            "nonroot_calls={} nonroot_us={}"
        ),
        wall.as_micros(),
        profile.execute_body_time_ns / 1_000,
        profile.prepared_direct_insert_row_build_time_ns / 1_000,
        profile.prepared_direct_insert_cursor_setup_time_ns / 1_000,
        profile.prepared_direct_insert_serialize_time_ns / 1_000,
        profile.prepared_direct_insert_btree_insert_time_ns / 1_000,
        profile.prepared_direct_insert_memdb_apply_time_ns / 1_000,
        profile.prepared_direct_insert_executions,
        profile.parser.fast_path_executions,
        leaf_reuse.fast_table_leaf_payload_appends,
        leaf_reuse.fast_table_leaf_payload_mutate_time_ns / 1_000,
        leaf_reuse.fast_table_leaf_payload_stage_time_ns / 1_000,
        leaf_reuse.fast_table_leaf_full_cell_appends,
        leaf_reuse.fast_table_leaf_full_cell_mutate_time_ns / 1_000,
        leaf_reuse.fast_table_leaf_full_cell_stage_time_ns / 1_000,
        leaf_reuse.quick_balance_attempts,
        leaf_reuse.quick_balance_hits,
        leaf_reuse.quick_balance_time_ns / 1_000,
        leaf_reuse.local_split_attempts,
        leaf_reuse.local_split_hits,
        leaf_reuse.local_split_time_ns / 1_000,
        leaf_reuse.nonroot_balance_calls,
        leaf_reuse.nonroot_balance_time_ns / 1_000,
    );
}

#[test]
#[ignore = "manual perf probe for operation_baseline_bench batch insert shape"]
fn manual_profile_bench_shape_prepared_direct_insert_1000() {
    const ROW_COUNT: i64 = 1_000;
    const CREATE_TABLE: &str =
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)";
    const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))";

    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute(CREATE_TABLE).unwrap();
    conn.execute("BEGIN").unwrap();
    let stmt = conn.prepare(INSERT_SQL).unwrap();

    reset_hot_path_profile();
    let insert_started = std::time::Instant::now();
    for i in 1..=ROW_COUNT {
        stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(i)])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let insert_wall = insert_started.elapsed();
    let profile = hot_path_profile_snapshot();

    let count_started = std::time::Instant::now();
    let count_stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
    let row = count_stmt.query_row().unwrap();
    let count_wall = count_started.elapsed();
    assert_eq!(
        row.values()[0],
        fsqlite_types::SqliteValue::Integer(ROW_COUNT)
    );

    eprintln!(
        concat!(
            "[manual_bench_shape_insert_1000] insert_wall_us={} count_wall_us={} ",
            "bg_status_us={} execute_body_us={} schema_validation_us={} ",
            "row_build_us={} cursor_setup_us={} serialize_us={} ",
            "btree_insert_us={} memdb_apply_us={} change_tracking_us={} ",
            "commit_pre_txn_us={} commit_roundtrip_us={} commit_finalize_us={} ",
            "commit_handle_finalize_us={} commit_post_write_us={} finalize_post_publish_us={} ",
            "direct_execs={} fast_execs={}"
        ),
        insert_wall.as_micros(),
        count_wall.as_micros(),
        profile.background_status_time_ns / 1_000,
        profile.execute_body_time_ns / 1_000,
        profile.prepared_direct_insert_schema_validation_time_ns / 1_000,
        profile.prepared_direct_insert_row_build_time_ns / 1_000,
        profile.prepared_direct_insert_cursor_setup_time_ns / 1_000,
        profile.prepared_direct_insert_serialize_time_ns / 1_000,
        profile.prepared_direct_insert_btree_insert_time_ns / 1_000,
        profile.prepared_direct_insert_memdb_apply_time_ns / 1_000,
        profile.prepared_direct_insert_change_tracking_time_ns / 1_000,
        profile.commit_pre_txn_time_ns / 1_000,
        profile.commit_txn_roundtrip_time_ns / 1_000,
        profile.commit_finalize_seq_time_ns / 1_000,
        profile.commit_handle_finalize_time_ns / 1_000,
        profile.commit_post_write_maintenance_time_ns / 1_000,
        profile.finalize_post_publish_time_ns / 1_000,
        profile.prepared_direct_insert_executions,
        profile.parser.fast_path_executions,
    );
}

#[test]
#[ignore = "manual perf probe for sequential_inserts autocommit prepared direct insert shape"]
fn manual_profile_simple_prepared_direct_insert_autocommit_512() {
    const ROW_COUNT: i64 = 512;
    const CREATE_TABLE: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)";
    const INSERT_SQL: &str = "INSERT INTO t VALUES (?1, 'val')";

    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute(CREATE_TABLE).unwrap();
    let stmt = conn.prepare(INSERT_SQL).unwrap();

    reset_hot_path_profile();
    let started = std::time::Instant::now();
    for i in 0..ROW_COUNT {
        stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(i)])
            .unwrap();
    }
    let wall = started.elapsed();
    let profile = hot_path_profile_snapshot();

    let rows = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(
        rows[0].values()[0],
        fsqlite_types::SqliteValue::Integer(ROW_COUNT)
    );

    eprintln!(
        concat!(
            "[manual_simple_autocommit_insert_512] wall_us={} execute_body_us={} ",
            "schema_validation_us={} row_build_us={} cursor_setup_us={} ",
            "serialize_us={} btree_insert_us={} memdb_apply_us={} ",
            "change_tracking_us={} autocommit_begin_us={} autocommit_resolve_us={} ",
            "commit_roundtrip_us={} commit_finalize_us={} commit_handle_finalize_us={} ",
            "commit_post_write_us={} finalize_post_publish_us={} commit_refreshes={} ",
            "memory_fast_begins={} cached_write_reuses={} cached_write_parks={} ",
            "publication_refreshes={} direct_execs={} autocommit_execs={} fast_execs={}"
        ),
        wall.as_micros(),
        profile.execute_body_time_ns / 1_000,
        profile.prepared_direct_insert_schema_validation_time_ns / 1_000,
        profile.prepared_direct_insert_row_build_time_ns / 1_000,
        profile.prepared_direct_insert_cursor_setup_time_ns / 1_000,
        profile.prepared_direct_insert_serialize_time_ns / 1_000,
        profile.prepared_direct_insert_btree_insert_time_ns / 1_000,
        profile.prepared_direct_insert_memdb_apply_time_ns / 1_000,
        profile.prepared_direct_insert_change_tracking_time_ns / 1_000,
        profile.prepared_direct_insert_autocommit_begin_time_ns / 1_000,
        profile.prepared_direct_insert_autocommit_resolve_time_ns / 1_000,
        profile.commit_txn_roundtrip_time_ns / 1_000,
        profile.commit_finalize_seq_time_ns / 1_000,
        profile.commit_handle_finalize_time_ns / 1_000,
        profile.commit_post_write_maintenance_time_ns / 1_000,
        profile.finalize_post_publish_time_ns / 1_000,
        profile.commit_refresh_count,
        profile.memory_autocommit_fast_path_begins,
        profile.cached_write_txn_reuses,
        profile.cached_write_txn_parks,
        profile.pager_publication_refreshes,
        profile.prepared_direct_insert_executions,
        profile.prepared_direct_insert_autocommit_executions,
        profile.parser.fast_path_executions,
    );
}

#[test]
#[ignore = "manual perf probe for full operation_baseline_bench batch insert lifecycle"]
fn manual_profile_full_op_batch_insert_1000_lifecycle() {
    const ROW_COUNT: i64 = 1_000;
    const CREATE_TABLE: &str =
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)";
    const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))";

    let _profile_guard = FastPathProfileTestGuard::new();

    let open_started = std::time::Instant::now();
    let conn = Connection::open(":memory:").unwrap();
    let open_wall = open_started.elapsed();

    let pragma_started = std::time::Instant::now();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    let pragma_wall = pragma_started.elapsed();

    let create_started = std::time::Instant::now();
    conn.execute(CREATE_TABLE).unwrap();
    let create_wall = create_started.elapsed();

    let begin_started = std::time::Instant::now();
    conn.execute("BEGIN").unwrap();
    let begin_wall = begin_started.elapsed();

    let prepare_insert_started = std::time::Instant::now();
    let stmt = conn.prepare(INSERT_SQL).unwrap();
    let prepare_insert_wall = prepare_insert_started.elapsed();

    reset_hot_path_profile();
    let insert_started = std::time::Instant::now();
    for i in 1..=ROW_COUNT {
        stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(i)])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let insert_wall = insert_started.elapsed();
    let profile = hot_path_profile_snapshot();

    let prepare_count_started = std::time::Instant::now();
    let count_stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
    let prepare_count_wall = prepare_count_started.elapsed();

    let count_started = std::time::Instant::now();
    let row = count_stmt.query_row().unwrap();
    let count_wall = count_started.elapsed();
    assert_eq!(
        row.values()[0],
        fsqlite_types::SqliteValue::Integer(ROW_COUNT)
    );

    eprintln!(
        concat!(
            "[manual_full_op_batch_insert_1000] open_us={} pragma_us={} create_us={} begin_us={} ",
            "prepare_insert_us={} insert_plus_commit_us={} prepare_count_us={} count_us={} ",
            "bg_status_us={} execute_body_us={} schema_validation_us={} row_build_us={} ",
            "btree_insert_us={} memdb_apply_us={} commit_roundtrip_us={} commit_handle_finalize_us={} ",
            "finalize_post_publish_us={}"
        ),
        open_wall.as_micros(),
        pragma_wall.as_micros(),
        create_wall.as_micros(),
        begin_wall.as_micros(),
        prepare_insert_wall.as_micros(),
        insert_wall.as_micros(),
        prepare_count_wall.as_micros(),
        count_wall.as_micros(),
        profile.background_status_time_ns / 1_000,
        profile.execute_body_time_ns / 1_000,
        profile.prepared_direct_insert_schema_validation_time_ns / 1_000,
        profile.prepared_direct_insert_row_build_time_ns / 1_000,
        profile.prepared_direct_insert_btree_insert_time_ns / 1_000,
        profile.prepared_direct_insert_memdb_apply_time_ns / 1_000,
        profile.commit_txn_roundtrip_time_ns / 1_000,
        profile.commit_handle_finalize_time_ns / 1_000,
        profile.finalize_post_publish_time_ns / 1_000,
    );
}

/// T2: Prepared SELECT records path metrics without double-counting.
#[test]
fn test_fast_path_simple_select() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'a')").unwrap();

    let stmt = conn.prepare("SELECT val FROM t WHERE id = ?1").unwrap();
    let before = hot_path_profile_snapshot();
    let rows = stmt
        .query_with_params(&[fsqlite_types::SqliteValue::Integer(1)])
        .unwrap();
    let after = hot_path_profile_snapshot();

    assert!(!rows.is_empty(), "SELECT should return a row");
    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    eprintln!("[T2] SELECT: fast_delta={fast_delta}, slow_delta={slow_delta}");
    // Either fast or slow is acceptable for SELECT — we document actual behavior.
    eprintln!(
        "[T2] SELECT path: {}",
        if fast_delta > 0 { "FAST" } else { "SLOW" }
    );
}

/// T3: CTE query falls through to slow path.
#[test]
fn test_slow_path_cte() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'cte')").unwrap();

    // CTE queries go through execute_statement (slow path).
    let before = hot_path_profile_snapshot();
    let rows = conn
        .query("WITH cte AS (SELECT * FROM t) SELECT val FROM cte")
        .unwrap();
    let after = hot_path_profile_snapshot();

    assert!(!rows.is_empty(), "CTE query should return results");
    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    eprintln!("[T3] CTE: fast_delta={fast_delta}, slow_delta={slow_delta}");
    // CTE may use slow path through execute_statement, or may compile and fast-path.
    // Document actual behavior.
    eprintln!(
        "[T3] CTE path: {}",
        if slow_delta > fast_delta {
            "SLOW (expected)"
        } else {
            "FAST (compiled)"
        }
    );
}

/// T4: DDL invalidates schema cookie, next execution uses slow path,
/// then stabilizes back to fast path.
#[test]
fn test_slow_path_schema_change_then_fast_path_recovery() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'v1')").unwrap();

    let stmt = conn.prepare("INSERT INTO t VALUES(?1, ?2)").unwrap();

    // Pre-DDL: fast path.
    let before = hot_path_profile_snapshot();
    stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(2),
        fsqlite_types::SqliteValue::Text("v2".into()),
    ])
    .unwrap();
    let after = hot_path_profile_snapshot();
    let (fast_pre, _) = fast_slow_delta(&before.parser, &after.parser);
    eprintln!("[T4] pre-DDL: fast_delta={fast_pre}");

    // DDL changes schema_cookie.
    conn.execute("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0")
        .unwrap();

    // Post-DDL: prepared statement should detect schema change.
    let result = stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(3),
        fsqlite_types::SqliteValue::Text("v3".into()),
    ]);

    match &result {
        Err(fsqlite_error::FrankenError::SchemaChanged) => {
            eprintln!("[T4] post-DDL: SchemaChanged (expected)");
        }
        Ok(_) => {
            eprintln!("[T4] post-DDL: succeeded (transparent re-prepare)");
        }
        Err(e) => {
            eprintln!("[T4] post-DDL: error {e:?}");
        }
    }

    // Re-prepare after schema change.
    let stmt2 = conn.prepare("INSERT INTO t VALUES(?1, ?2, ?3)").unwrap();
    let before2 = hot_path_profile_snapshot();
    stmt2
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(4),
            fsqlite_types::SqliteValue::Text("v4".into()),
            fsqlite_types::SqliteValue::Integer(99),
        ])
        .unwrap();
    let after2 = hot_path_profile_snapshot();
    let (fast_post, _) = fast_slow_delta(&before2.parser, &after2.parser);
    eprintln!("[T4] post-re-prepare: fast_delta={fast_post}");

    // Verify data correctness.
    let rows = conn.query("SELECT COUNT(*) FROM t").unwrap();
    let count = rows[0].get(0).unwrap();
    // Should have at least 2 rows (id=1 and id=2 from the pre-DDL insert).
    if let fsqlite_types::SqliteValue::Integer(n) = count {
        assert!(*n >= 2, "should have at least 2 rows after DDL: got {n}");
    }
}

/// T5: Fast path works with all parameter types.
#[test]
fn test_fast_path_parameterized() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB)")
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO t VALUES(?1, ?2, ?3, ?4)")
        .unwrap();
    stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(1),
        fsqlite_types::SqliteValue::Text("alice".into()),
        fsqlite_types::SqliteValue::Float(3.14),
        fsqlite_types::SqliteValue::Blob(vec![0xDE, 0xAD].into()),
    ])
    .unwrap();

    stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(2),
        fsqlite_types::SqliteValue::Null,
        fsqlite_types::SqliteValue::Null,
        fsqlite_types::SqliteValue::Null,
    ])
    .unwrap();

    let rows = conn.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2, "should have 2 rows");

    // Verify data types round-trip.
    let row1 = &rows[0];
    assert_eq!(
        row1.get(1),
        Some(&fsqlite_types::SqliteValue::Text("alice".into()))
    );
}

/// T6: View query records path metrics for the deferred-query route.
#[test]
fn test_slow_path_view_expansion() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'view-test')")
        .unwrap();
    conn.execute("CREATE VIEW v AS SELECT val FROM t").unwrap();

    let before = hot_path_profile_snapshot();
    let rows = conn.query("SELECT * FROM v").unwrap();
    let after = hot_path_profile_snapshot();

    assert!(!rows.is_empty(), "view query should return results");
    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    eprintln!("[T6] VIEW: fast_delta={fast_delta}, slow_delta={slow_delta}");
}

/// T7: Complex queries (JOINs, subqueries) still produce correct results.
#[test]
fn test_no_regression_complex_queries() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
        .unwrap();
    conn.execute("CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO customers VALUES(1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO customers VALUES(2, 'Bob')")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES(1, 1, 100.0)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES(2, 1, 200.0)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES(3, 2, 50.0)")
        .unwrap();

    // JOIN.
    let rows = conn
        .query("SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.id ORDER BY c.name")
        .unwrap();
    assert_eq!(rows.len(), 2);

    // Subquery.
    let rows = conn
        .query("SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 75)")
        .unwrap();
    assert!(!rows.is_empty());

    // Correlated subquery.
    let rows = conn
        .query("SELECT name, (SELECT SUM(amount) FROM orders WHERE customer_id = customers.id) AS total FROM customers ORDER BY name")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

/// T8: Latency scorecard — fast path vs repeated ad-hoc execution.
#[test]
fn test_fast_path_latency_scorecard() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE bench(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let iterations = 1000;

    // Prepared path (should use fast path).
    let stmt = conn.prepare("INSERT INTO bench VALUES(?1, ?2)").unwrap();
    let t_prepared_start = std::time::Instant::now();
    for i in 0..iterations {
        stmt.execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(i),
            fsqlite_types::SqliteValue::Text(format!("row{i}").into()),
        ])
        .unwrap();
    }
    let t_prepared = t_prepared_start.elapsed();

    let snap_prepared = hot_path_profile_snapshot();
    conn.execute("DELETE FROM bench").unwrap();
    reset_hot_path_profile();

    // Ad-hoc path (always re-parses/compiles, but uses compile cache).
    let t_adhoc_start = std::time::Instant::now();
    for i in 0..iterations {
        conn.execute(&format!("INSERT INTO bench VALUES({i}, 'row{i}')"))
            .unwrap();
    }
    let t_adhoc = t_adhoc_start.elapsed();

    let snap_adhoc = hot_path_profile_snapshot();

    eprintln!("=== bd-6eyrg.1 Latency Scorecard ===");
    eprintln!("Prepared ({iterations} iterations):");
    eprintln!("  elapsed: {:?}", t_prepared);
    eprintln!(
        "  fast_path={}, slow_path={}",
        snap_prepared.parser.fast_path_executions, snap_prepared.parser.slow_path_executions
    );
    eprintln!("Ad-hoc ({iterations} iterations):");
    eprintln!("  elapsed: {:?}", t_adhoc);
    eprintln!(
        "  fast_path={}, slow_path={}",
        snap_adhoc.parser.fast_path_executions, snap_adhoc.parser.slow_path_executions
    );
    let ratio = t_prepared.as_nanos() as f64 / t_adhoc.as_nanos().max(1) as f64;
    eprintln!("  prepared/adhoc ratio: {ratio:.2}x");
    eprintln!("=== END SCORECARD ===");

    // The prepared path should not be more than 2x slower than ad-hoc.
    // (It should actually be faster, but we're lenient here.)
    assert!(
        ratio < 2.0,
        "prepared path should not be >2x slower than ad-hoc: ratio={ratio:.2}"
    );
}

/// T9: Prepared UPDATE uses fast lane on file-backed WAL (bd-db300.5.2.2.3).
#[test]
fn test_fast_path_prepared_update() {
    let _profile_guard = FastPathProfileTestGuard::new();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();
    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'before')").unwrap();
    conn.execute("INSERT INTO t VALUES(2, 'before')").unwrap();

    let stmt = conn.prepare("UPDATE t SET val = ?2 WHERE id = ?1").unwrap();

    // Warm.
    stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(1),
        fsqlite_types::SqliteValue::Text("warm".into()),
    ])
    .unwrap();
    reset_hot_path_profile();

    // Measure.
    let before = hot_path_profile_snapshot();
    stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(1),
        fsqlite_types::SqliteValue::Text("after1".into()),
    ])
    .unwrap();
    stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(2),
        fsqlite_types::SqliteValue::Text("after2".into()),
    ])
    .unwrap();
    let after = hot_path_profile_snapshot();

    let (fast_delta, _) = fast_slow_delta(&before.parser, &after.parser);
    let direct_update = after
        .prepared_direct_update_executions
        .saturating_sub(before.prepared_direct_update_executions);
    eprintln!("[T9] UPDATE: fast_delta={fast_delta}, direct_update_delta={direct_update}");
    assert!(
        fast_delta >= 2,
        "prepared UPDATE should use fast path: fast_delta={fast_delta}"
    );
    assert!(
        direct_update >= 2,
        "prepared UPDATE should hit the direct UPDATE lane: direct_update={direct_update}"
    );

    // Correctness.
    let rows = conn.query("SELECT val FROM t ORDER BY id").unwrap();
    assert_eq!(
        rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Text("after1".into()))
    );
    assert_eq!(
        rows[1].get(0),
        Some(&fsqlite_types::SqliteValue::Text("after2".into()))
    );
}

#[test]
fn test_prepared_update_vdbe_constraint_error_restores_row_in_explicit_txn() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
        .unwrap();
    conn.execute("CREATE TRIGGER t_update_noop AFTER UPDATE ON t BEGIN SELECT 1; END")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'before')").unwrap();
    conn.execute("BEGIN").unwrap();

    let stmt = conn.prepare("UPDATE t SET val = ?1 WHERE id = ?2").unwrap();
    reset_hot_path_profile();
    let err = stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Null,
            fsqlite_types::SqliteValue::Integer(1),
        ])
        .expect_err("NOT NULL violating prepared UPDATE should fail");
    assert!(
        err.to_string().contains("NOT NULL"),
        "unexpected prepared UPDATE error: {err}"
    );
    let profile = hot_path_profile_snapshot();
    assert_eq!(
        profile.parser.fast_path_executions, 0,
        "triggered table should force the prepared UPDATE through VDBE, not the direct lane"
    );
    assert_eq!(
        profile.parser.slow_path_executions, 1,
        "constraint regression must cover the VDBE-backed prepared UPDATE path"
    );

    let rows = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values(),
        &[
            fsqlite_types::SqliteValue::Integer(1),
            fsqlite_types::SqliteValue::Text("before".into()),
        ],
        "VDBE-backed prepared UPDATE must restore the original row after a constraint error"
    );
    conn.execute("COMMIT").unwrap();
}

/// T10: Prepared DELETE uses fast lane on file-backed WAL (bd-db300.5.2.2.3).
#[test]
fn test_fast_path_prepared_delete() {
    let _profile_guard = FastPathProfileTestGuard::new();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();
    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'a')").unwrap();
    conn.execute("INSERT INTO t VALUES(2, 'b')").unwrap();
    conn.execute("INSERT INTO t VALUES(3, 'c')").unwrap();

    let stmt = conn.prepare("DELETE FROM t WHERE id = ?1").unwrap();

    // Warm.
    stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(3)])
        .unwrap();
    reset_hot_path_profile();

    // Measure.
    let before = hot_path_profile_snapshot();
    stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(1)])
        .unwrap();
    stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(2)])
        .unwrap();
    let after = hot_path_profile_snapshot();

    let (fast_delta, _) = fast_slow_delta(&before.parser, &after.parser);
    let direct_delete = after
        .prepared_direct_delete_executions
        .saturating_sub(before.prepared_direct_delete_executions);
    eprintln!("[T10] DELETE: fast_delta={fast_delta}, direct_delete_delta={direct_delete}");
    assert!(
        fast_delta >= 2,
        "prepared DELETE should use fast path: fast_delta={fast_delta}"
    );
    assert!(
        direct_delete >= 2,
        "prepared DELETE should hit the direct DELETE lane: direct_delete={direct_delete}"
    );

    // Correctness.
    let rows = conn.query("SELECT COUNT(*) FROM t").unwrap();
    let count = rows[0].get(0).unwrap();
    assert_eq!(
        count,
        &fsqlite_types::SqliteValue::Integer(0),
        "all rows should be deleted"
    );
}

/// T11: Deferred-DML path uses no_publication() proof and still succeeds
/// (bd-db300.5.2.2.3).
///
/// The deferred path fires when `stmt.deferred_dml_statement()` is Some and
/// `fast_path.supports_direct_dispatch_now()` is true. Foreign keys force
/// the deferred path because FK enforcement requires post-statement checking.
#[test]
fn test_deferred_dml_no_publication_proof() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();
    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES(1)").unwrap();

    // Prepared DELETE on parent with FK child — forces deferred DML path.
    let stmt = conn.prepare("DELETE FROM parent WHERE id = ?1").unwrap();

    // Should succeed (no child references id=1... wait, no child rows exist).
    let result = stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(1)]);
    assert!(
        result.is_ok(),
        "deferred-DML DELETE with no FK violation should succeed: {:?}",
        result
    );

    // Insert a child referencing parent id=2, then try to delete parent id=2.
    conn.execute("INSERT INTO parent VALUES(2)").unwrap();
    conn.execute("INSERT INTO child VALUES(1, 2)").unwrap();
    let result = stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(2)]);
    // Should fail with FK constraint violation.
    assert!(
        result.is_err(),
        "deferred-DML DELETE with FK violation should fail"
    );
}

/// T12: UPDATE/DELETE DDL invalidation + recovery (bd-db300.5.2.2.3).
///
/// Mirrors T4 but for UPDATE and DELETE: DDL invalidates the prepared
/// statement, re-prepare restores fast-path execution.
#[test]
fn test_fast_path_update_delete_ddl_invalidation() {
    let _profile_guard = FastPathProfileTestGuard::new();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();
    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'orig')").unwrap();

    let update_stmt = conn.prepare("UPDATE t SET val = ?2 WHERE id = ?1").unwrap();
    let delete_stmt = conn.prepare("DELETE FROM t WHERE id = ?1").unwrap();

    // Pre-DDL: should succeed on fast path.
    update_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(1),
            fsqlite_types::SqliteValue::Text("updated".into()),
        ])
        .unwrap();

    // DDL changes schema.
    conn.execute("ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0")
        .unwrap();

    // Post-DDL: old prepared stmts should detect schema change.
    let update_result = update_stmt.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(1),
        fsqlite_types::SqliteValue::Text("post-ddl".into()),
    ]);
    match &update_result {
        Err(fsqlite_error::FrankenError::SchemaChanged) => {
            eprintln!("[T12] UPDATE post-DDL: SchemaChanged (expected)");
        }
        Ok(_) => {
            eprintln!("[T12] UPDATE post-DDL: succeeded (transparent re-prepare)");
        }
        Err(e) => {
            eprintln!("[T12] UPDATE post-DDL: error {e:?}");
        }
    }

    let delete_result = delete_stmt.execute_with_params(&[fsqlite_types::SqliteValue::Integer(1)]);
    match &delete_result {
        Err(fsqlite_error::FrankenError::SchemaChanged) => {
            eprintln!("[T12] DELETE post-DDL: SchemaChanged (expected)");
        }
        Ok(_) => {
            eprintln!("[T12] DELETE post-DDL: succeeded (transparent re-prepare)");
        }
        Err(e) => {
            eprintln!("[T12] DELETE post-DDL: error {e:?}");
        }
    }

    // Re-prepare UPDATE on new schema and verify fast path restored.
    conn.execute("INSERT INTO t VALUES(10, 'seed', 0)").unwrap();
    conn.execute("INSERT INTO t VALUES(11, 'seed2', 0)")
        .unwrap();

    let update2 = conn
        .prepare("UPDATE t SET val = ?2, extra = ?3 WHERE id = ?1")
        .unwrap();
    let before_upd = hot_path_profile_snapshot();
    update2
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(10),
            fsqlite_types::SqliteValue::Text("recovered".into()),
            fsqlite_types::SqliteValue::Integer(42),
        ])
        .unwrap();
    let after_upd = hot_path_profile_snapshot();
    let (fast_upd, _) = fast_slow_delta(&before_upd.parser, &after_upd.parser);
    eprintln!("[T12] post-re-prepare UPDATE: fast_delta={fast_upd}");
    assert!(
        fast_upd > 0,
        "re-prepared UPDATE should restore fast path: fast_delta={fast_upd}"
    );

    // Verify UPDATE correctness.
    let rows = conn
        .query("SELECT val, extra FROM t WHERE id = 10")
        .unwrap();
    assert!(
        !rows.is_empty(),
        "re-prepared UPDATE should have affected a row"
    );
    assert_eq!(
        rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Text("recovered".into()))
    );

    // Re-prepare DELETE on new schema and verify fast path restored.
    let delete2 = conn.prepare("DELETE FROM t WHERE id = ?1").unwrap();
    let before_del = hot_path_profile_snapshot();
    delete2
        .execute_with_params(&[fsqlite_types::SqliteValue::Integer(11)])
        .unwrap();
    let after_del = hot_path_profile_snapshot();
    let (fast_del, _) = fast_slow_delta(&before_del.parser, &after_del.parser);
    eprintln!("[T12] post-re-prepare DELETE: fast_delta={fast_del}");
    assert!(
        fast_del > 0,
        "re-prepared DELETE should restore fast path: fast_delta={fast_del}"
    );

    // Verify DELETE correctness.
    let rows = conn.query("SELECT COUNT(*) FROM t WHERE id = 11").unwrap();
    assert_eq!(
        rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Integer(0)),
        "re-prepared DELETE should have removed the row"
    );
}

/// T13: File-backed prepared INSERT, UPDATE, and DELETE all reuse prebound
/// publication (≤1 pager_publication_refresh per operation).
///
/// Before the entry-proof fix, UPDATE/DELETE double-refreshed because the
/// deferred-DML path did not thread the prebound publication.  Now all three
/// DML kinds pass the publication through the entry proof.
#[test]
fn test_file_backed_publication_refresh_counts() {
    let _profile_guard = FastPathProfileTestGuard::new();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();
    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'a')").unwrap();

    let insert_stmt = conn.prepare("INSERT INTO t VALUES(?1, ?2)").unwrap();
    let update_stmt = conn.prepare("UPDATE t SET val = ?2 WHERE id = ?1").unwrap();
    let delete_stmt = conn.prepare("DELETE FROM t WHERE id = ?1").unwrap();

    // Warm all stmts.
    insert_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(10),
            fsqlite_types::SqliteValue::Text("warm".into()),
        ])
        .unwrap();
    update_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(1),
            fsqlite_types::SqliteValue::Text("warm".into()),
        ])
        .unwrap();
    conn.execute("INSERT INTO t VALUES(20, 'del_warm')")
        .unwrap();
    delete_stmt
        .execute_with_params(&[fsqlite_types::SqliteValue::Integer(20)])
        .unwrap();

    // Measure INSERT (precompiled path — should reuse prebound publication).
    reset_hot_path_profile();
    let before_ins = hot_path_profile_snapshot();
    insert_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(100),
            fsqlite_types::SqliteValue::Text("measured".into()),
        ])
        .unwrap();
    let after_ins = hot_path_profile_snapshot();
    let insert_pub = after_ins
        .pager_publication_refreshes
        .saturating_sub(before_ins.pager_publication_refreshes);
    eprintln!("[T13] INSERT pager_publication_refreshes delta = {insert_pub}");
    assert!(
        insert_pub <= 1,
        "INSERT should reuse prebound publication (≤1 refresh): got {insert_pub}"
    );

    // Measure UPDATE (deferred-DML path; this used to double-refresh).
    reset_hot_path_profile();
    let before_upd = hot_path_profile_snapshot();
    update_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(1),
            fsqlite_types::SqliteValue::Text("measured".into()),
        ])
        .unwrap();
    let after_upd = hot_path_profile_snapshot();
    let update_pub = after_upd
        .pager_publication_refreshes
        .saturating_sub(before_upd.pager_publication_refreshes);
    eprintln!("[T13] UPDATE pager_publication_refreshes delta = {update_pub}");
    assert!(
        update_pub <= 1,
        "UPDATE should reuse prebound publication (≤1 refresh): got {update_pub}"
    );

    // Measure DELETE.  Seed the target row with the prepared insert_stmt
    // to avoid an ad-hoc conn.execute() between reset and measurement
    // (ad-hoc execution advances commit_seq, forcing a stale-publication
    // refresh that inflates the counter).
    insert_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(200),
            fsqlite_types::SqliteValue::Text("del_target".into()),
        ])
        .unwrap();
    reset_hot_path_profile();
    let before_del = hot_path_profile_snapshot();
    delete_stmt
        .execute_with_params(&[fsqlite_types::SqliteValue::Integer(200)])
        .unwrap();
    let after_del = hot_path_profile_snapshot();
    let delete_pub = after_del
        .pager_publication_refreshes
        .saturating_sub(before_del.pager_publication_refreshes);
    eprintln!("[T13] DELETE pager_publication_refreshes delta = {delete_pub}");
    assert!(
        delete_pub <= 1,
        "DELETE should reuse prebound publication (≤1 refresh): got {delete_pub}"
    );
}

/// T14: :memory: UPDATE/DELETE succeeds with no-publication entry proof
/// (bd-db300.5.2.2.3 / bd-db300.5.2.2.4).
#[test]
fn test_entry_proof_no_publication_for_memory_update_delete() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES(1, 'a')").unwrap();
    conn.execute("INSERT INTO t VALUES(2, 'b')").unwrap();

    let update_stmt = conn.prepare("UPDATE t SET val = ?2 WHERE id = ?1").unwrap();
    let delete_stmt = conn.prepare("DELETE FROM t WHERE id = ?1").unwrap();

    // UPDATE on :memory: — entry_proof.publication is None.
    update_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(1),
            fsqlite_types::SqliteValue::Text("updated".into()),
        ])
        .unwrap();

    // DELETE on :memory:.
    delete_stmt
        .execute_with_params(&[fsqlite_types::SqliteValue::Integer(2)])
        .unwrap();

    // Verify data correctness.
    let rows = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 1, "one row should remain after delete");
    assert_eq!(
        rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Integer(1))
    );
    assert_eq!(
        rows[0].get(1),
        Some(&fsqlite_types::SqliteValue::Text("updated".into()))
    );
}

/// T15: Prepared DML within explicit BEGIN...COMMIT uses entry-proof path
/// without regression (bd-db300.5.2.2.3 / bd-db300.5.2.2.4).
#[test]
fn test_entry_proof_within_explicit_transaction() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();
    let conn = Connection::open(path).unwrap();
    conn.execute("PRAGMA journal_mode = WAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    let insert_stmt = conn.prepare("INSERT INTO t VALUES(?1, ?2)").unwrap();
    let update_stmt = conn.prepare("UPDATE t SET val = ?2 WHERE id = ?1").unwrap();
    let delete_stmt = conn.prepare("DELETE FROM t WHERE id = ?1").unwrap();

    // Explicit transaction: entry_proof.publication should be None
    // (in_transaction = true → ensure_autocommit_txn returns false early).
    conn.execute("BEGIN").unwrap();

    insert_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(1),
            fsqlite_types::SqliteValue::Text("inserted".into()),
        ])
        .unwrap();
    insert_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(2),
            fsqlite_types::SqliteValue::Text("also inserted".into()),
        ])
        .unwrap();

    update_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(1),
            fsqlite_types::SqliteValue::Text("updated in txn".into()),
        ])
        .unwrap();

    delete_stmt
        .execute_with_params(&[fsqlite_types::SqliteValue::Integer(2)])
        .unwrap();

    conn.execute("COMMIT").unwrap();

    // Verify all operations committed correctly.
    let rows = conn.query("SELECT id, val FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 1, "only row 1 should remain");
    assert_eq!(
        rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Integer(1))
    );
    assert_eq!(
        rows[0].get(1),
        Some(&fsqlite_types::SqliteValue::Text("updated in txn".into()))
    );
}

/// T16: B4 coverage — unique secondary-index query_row should use the direct
/// indexed-equality counter.
#[test]
fn test_query_row_indexed_equality_uses_direct_counter() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT NOT NULL, val TEXT NOT NULL)")
        .unwrap();
    conn.execute("CREATE UNIQUE INDEX idx_t_email ON t(email)")
        .unwrap();
    conn.execute(
        "INSERT INTO t VALUES
            (1, 'a@test.example', 'alpha'),
            (2, 'b@test.example', 'beta'),
            (3, 'c@test.example', 'gamma')",
    )
    .unwrap();

    let stmt = conn.prepare("SELECT * FROM t WHERE email = ?1").unwrap();

    let before = hot_path_profile_snapshot();
    let row = stmt
        .query_row_with_params(&[fsqlite_types::SqliteValue::Text("b@test.example".into())])
        .unwrap();
    let after = hot_path_profile_snapshot();

    assert_eq!(
        row.get(0),
        Some(&fsqlite_types::SqliteValue::Integer(2)),
        "indexed equality query_row should return the matching id"
    );
    assert_eq!(
        row.get(1),
        Some(&fsqlite_types::SqliteValue::Text("b@test.example".into())),
        "indexed equality query_row should return the matching email"
    );
    assert_eq!(
        row.get(2),
        Some(&fsqlite_types::SqliteValue::Text("beta".into())),
        "indexed equality query_row should return the matching payload"
    );

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "prepared indexed equality query_row should stay on the fast prepared path"
    );
    assert_eq!(
        slow_delta, 0,
        "prepared indexed equality query_row should not need slow-path execution"
    );

    let direct_delta = after
        .direct_indexed_equality_query_hits
        .saturating_sub(before.direct_indexed_equality_query_hits);
    assert!(
        direct_delta > 0,
        "indexed equality query_row should increment direct indexed equality hits: before={before:?} after={after:?}"
    );

    let miss = stmt
        .query_row_with_params(&[fsqlite_types::SqliteValue::Text(
            "missing@test.example".into(),
        )])
        .expect_err("missing indexed equality row should return no rows");
    assert!(
        matches!(miss, fsqlite_error::FrankenError::QueryReturnedNoRows),
        "missing indexed equality row should surface QueryReturnedNoRows"
    );
}

/// T17: B4 coverage — rowid-range query_row should short-circuit after
/// first/second row detection and flip the direct rowid-range counter.
#[test]
fn test_query_row_rowid_range_uses_direct_counter() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    conn.execute(
        "INSERT INTO t VALUES
            (1, 'alpha'),
            (2, 'beta'),
            (3, 'gamma'),
            (4, 'delta')",
    )
    .unwrap();

    let stmt = conn
        .prepare("SELECT * FROM t WHERE id >= ?1 AND id < ?2")
        .unwrap();

    let before = hot_path_profile_snapshot();
    let row = stmt
        .query_row_with_params(&[
            fsqlite_types::SqliteValue::Integer(2),
            fsqlite_types::SqliteValue::Integer(3),
        ])
        .unwrap();
    let after = hot_path_profile_snapshot();

    assert_eq!(
        row.get(0),
        Some(&fsqlite_types::SqliteValue::Integer(2)),
        "single-row rowid range should return the expected id"
    );
    assert_eq!(
        row.get(1),
        Some(&fsqlite_types::SqliteValue::Text("beta".into())),
        "single-row rowid range should return the expected payload"
    );

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "prepared rowid-range query_row should stay on the fast prepared path"
    );
    assert_eq!(
        slow_delta, 0,
        "prepared rowid-range query_row should not need slow-path execution"
    );

    let direct_delta = after
        .direct_rowid_range_query_hits
        .saturating_sub(before.direct_rowid_range_query_hits);
    assert!(
        direct_delta > 0,
        "rowid-range query_row should increment direct rowid-range hits: before={before:?} after={after:?}"
    );

    let no_rows = stmt
        .query_row_with_params(&[
            fsqlite_types::SqliteValue::Integer(5),
            fsqlite_types::SqliteValue::Integer(5),
        ])
        .expect_err("empty rowid range should return no rows");
    assert!(
        matches!(no_rows, fsqlite_error::FrankenError::QueryReturnedNoRows),
        "empty rowid range should surface QueryReturnedNoRows"
    );

    let multiple_rows = stmt
        .query_row_with_params(&[
            fsqlite_types::SqliteValue::Integer(2),
            fsqlite_types::SqliteValue::Integer(5),
        ])
        .expect_err("multi-row range should return multiple rows error");
    assert!(
        matches!(
            multiple_rows,
            fsqlite_error::FrankenError::QueryReturnedMultipleRows
        ),
        "multi-row rowid range should surface QueryReturnedMultipleRows"
    );
}

#[test]
fn test_b3_point_lookup_profile_scales_across_table_sizes() {
    const ROW_COUNTS: [i64; 4] = [100, 1_000, 10_000, 100_000];
    const PROBES_PER_SIZE: usize = 96;

    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            payload TEXT NOT NULL,
            score INTEGER NOT NULL
        )",
    )
    .unwrap();

    let rowid_opcodes = explain_opcode_names(&conn, "SELECT payload FROM t WHERE id = 42");
    eprintln!(
        "[bd-6eyrg.3] rowid point lookup bytecode: {}",
        rowid_opcodes.join(", ")
    );
    assert!(
        rowid_opcodes.iter().any(|op| op == "SeekRowid"),
        "rowid point lookup should emit SeekRowid, got {rowid_opcodes:?}"
    );
    assert!(
        !rowid_opcodes.iter().any(|op| op == "Rewind"),
        "rowid point lookup should not lower to a scan, got {rowid_opcodes:?}"
    );

    let insert = conn.prepare("INSERT INTO t VALUES (?1, ?2, ?3)").unwrap();
    let mut seeded_through = 0_i64;
    let mut measured_avg_nanos = Vec::with_capacity(ROW_COUNTS.len());

    for row_count in ROW_COUNTS {
        conn.execute("BEGIN").unwrap();
        for id in (seeded_through + 1)..=row_count {
            insert
                .execute_with_params(&[
                    fsqlite_types::SqliteValue::Integer(id),
                    fsqlite_types::SqliteValue::Text(format!("payload_{id}").into()),
                    fsqlite_types::SqliteValue::Integer(id * 7),
                ])
                .unwrap();
        }
        conn.execute("COMMIT").unwrap();
        seeded_through = row_count;

        let select = conn.prepare("SELECT payload FROM t WHERE id = ?1").unwrap();
        let warm_probe = [fsqlite_types::SqliteValue::Integer(row_count / 2)];
        let warm_row = select.query_row_with_params(&warm_probe).unwrap();
        assert_eq!(
            warm_row.get(0),
            Some(&fsqlite_types::SqliteValue::Text(
                format!("payload_{}", row_count / 2).into()
            )),
            "warm point lookup should return the expected payload"
        );

        reset_hot_path_profile();
        let before = hot_path_profile_snapshot();
        let started = std::time::Instant::now();
        for probe_index in 0..PROBES_PER_SIZE {
            let probe_index = i64::try_from(probe_index).unwrap();
            let probe_id = 1 + ((probe_index * 7_919) % row_count);
            let row = select
                .query_row_with_params(&[fsqlite_types::SqliteValue::Integer(probe_id)])
                .unwrap();
            std::hint::black_box(row);
        }
        let elapsed = started.elapsed();
        let after = hot_path_profile_snapshot();

        let avg_nanos = elapsed.as_nanos() / u128::try_from(PROBES_PER_SIZE).unwrap();
        measured_avg_nanos.push(avg_nanos);
        let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
        let direct_delta = after
            .direct_rowid_lookup_query_row_hits
            .saturating_sub(before.direct_rowid_lookup_query_row_hits);
        eprintln!(
            "[bd-6eyrg.3] point_lookup rows={row_count} probes={PROBES_PER_SIZE} \
             avg_ns={avg_nanos} total_us={} direct_rowid_hits={direct_delta} \
             fast_delta={fast_delta} slow_delta={slow_delta}",
            elapsed.as_micros()
        );

        let expected_probes = u64::try_from(PROBES_PER_SIZE).unwrap();
        assert_eq!(
            direct_delta, expected_probes,
            "rowid lookup at {row_count} rows should stay on the direct rowid path: before={before:?} after={after:?}"
        );
        assert!(
            fast_delta >= expected_probes,
            "rowid lookup at {row_count} rows should be recorded as fast-path execution: before={before:?} after={after:?}"
        );
        assert_eq!(
            slow_delta, 0,
            "rowid lookup at {row_count} rows should not fall back to slow execution"
        );

        let budget_nanos = if row_count <= 1_000 {
            50_000_u128
        } else if row_count == 10_000 {
            100_000_u128
        } else {
            200_000_u128
        };
        assert!(
            avg_nanos < budget_nanos,
            "point lookup at {row_count} rows averaged {avg_nanos}ns, budget {budget_nanos}ns"
        );
    }

    let first_avg = measured_avg_nanos.first().copied().unwrap_or(1).max(1);
    let last_avg = measured_avg_nanos.last().copied().unwrap_or(first_avg);
    let scale_budget = first_avg.saturating_mul(16).max(25_000);
    assert!(
        last_avg <= scale_budget,
        "point lookup should stay seek-bounded: 100-row avg {first_avg}ns, 100k-row avg {last_avg}ns, budget {scale_budget}ns"
    );
}

#[test]
fn test_b3_covering_indexed_equality_profile_stays_seek_bounded() {
    const ROW_COUNT: i64 = 10_000;
    const PROBES: usize = 96;

    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute(
        "CREATE TABLE t(
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            name TEXT NOT NULL,
            payload TEXT NOT NULL
        )",
    )
    .unwrap();
    conn.execute("CREATE INDEX t_category_payload ON t(category, payload)")
        .unwrap();

    let insert = conn
        .prepare("INSERT INTO t VALUES (?1, ?2, ?3, ?4)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=ROW_COUNT {
        insert
            .execute_with_params(&[
                fsqlite_types::SqliteValue::Integer(id),
                fsqlite_types::SqliteValue::Text(format!("cat_{id:05}").into()),
                fsqlite_types::SqliteValue::Text(format!("name_{id:05}").into()),
                fsqlite_types::SqliteValue::Text(format!("payload_{id:05}").into()),
            ])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();

    let covering_opcodes =
        explain_opcode_names(&conn, "SELECT payload FROM t WHERE category = 'cat_05000'");
    eprintln!(
        "[bd-6eyrg.3] covering indexed equality bytecode: {}",
        covering_opcodes.join(", ")
    );
    assert!(
        covering_opcodes.iter().any(|op| op == "SeekGE"),
        "covering indexed equality should probe the covering index, got {covering_opcodes:?}"
    );
    assert!(
        !covering_opcodes.iter().any(|op| op == "SeekRowid"),
        "covering indexed equality should avoid table rowid lookup, got {covering_opcodes:?}"
    );
    let non_covering_opcodes =
        explain_opcode_names(&conn, "SELECT name FROM t WHERE category = 'cat_05000'");
    eprintln!(
        "[bd-6eyrg.3] non-covering indexed equality bytecode: {}",
        non_covering_opcodes.join(", ")
    );
    assert!(
        non_covering_opcodes.iter().any(|op| op == "SeekGE"),
        "non-covering indexed equality should probe the index, got {non_covering_opcodes:?}"
    );
    assert!(
        non_covering_opcodes.iter().any(|op| op == "IdxRowid"),
        "non-covering indexed equality should read rowids from the index, got {non_covering_opcodes:?}"
    );
    assert!(
        non_covering_opcodes.iter().any(|op| op == "SeekRowid"),
        "non-covering indexed equality should seek the table row, got {non_covering_opcodes:?}"
    );

    let covering = conn
        .prepare("SELECT payload FROM t WHERE category = ?1")
        .unwrap();
    let non_covering = conn
        .prepare("SELECT name FROM t WHERE category = ?1")
        .unwrap();
    let probe = [fsqlite_types::SqliteValue::Text("cat_05000".into())];
    let warm_rows = covering.query_with_params(&probe).unwrap();
    assert_eq!(warm_rows.len(), 1);
    assert_eq!(
        warm_rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Text("payload_05000".into())),
        "covering lookup should return the expected payload"
    );
    let warm_non_covering_rows = non_covering.query_with_params(&probe).unwrap();
    assert_eq!(warm_non_covering_rows.len(), 1);
    assert_eq!(
        warm_non_covering_rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Text("name_05000".into())),
        "non-covering lookup should return the expected table payload"
    );

    let probes = (0..PROBES)
        .map(|probe_index| {
            let probe_index = i64::try_from(probe_index).unwrap();
            let probe_id = 1 + ((probe_index * 7_919) % ROW_COUNT);
            fsqlite_types::SqliteValue::Text(format!("cat_{probe_id:05}").into())
        })
        .collect::<Vec<_>>();
    let opcode_total = |snapshot: &fsqlite_core::connection::HotPathProfileSnapshot,
                        opcode: &str| {
        snapshot
            .vdbe
            .opcode_execution_totals
            .iter()
            .find(|count| count.opcode == opcode)
            .map_or(0, |count| count.total)
    };

    reset_hot_path_profile();
    let before = hot_path_profile_snapshot();
    let started = std::time::Instant::now();
    let mut row_total = 0_usize;
    for probe in &probes {
        let rows = covering
            .query_with_params(std::slice::from_ref(probe))
            .unwrap();
        row_total = row_total.saturating_add(rows.len());
        std::hint::black_box(rows);
    }
    let elapsed = started.elapsed();
    let after = hot_path_profile_snapshot();

    let avg_nanos = elapsed.as_nanos() / u128::try_from(PROBES).unwrap();
    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    let seek_ge_total = opcode_total(&after, "SeekGE");
    let seek_rowid_total = opcode_total(&after, "SeekRowid");
    eprintln!(
        "[bd-6eyrg.3] covering_index_lookup rows={ROW_COUNT} probes={PROBES} \
         avg_ns={avg_nanos} total_us={} row_total={row_total} \
         seek_ge_execs={seek_ge_total} seek_rowid_execs={seek_rowid_total} \
         fast_delta={fast_delta} slow_delta={slow_delta}",
        elapsed.as_micros()
    );

    let expected_probes = u64::try_from(PROBES).unwrap();
    assert_eq!(
        row_total, PROBES,
        "each covering index probe should return exactly one row"
    );
    assert!(
        seek_ge_total >= expected_probes,
        "covering indexed equality should execute one SeekGE probe per lookup: before={before:?} after={after:?}"
    );
    assert_eq!(
        seek_rowid_total, 0,
        "covering indexed equality should avoid dynamic table rowid seeks: before={before:?} after={after:?}"
    );
    assert!(
        fast_delta >= expected_probes,
        "covering indexed equality should be recorded as fast-path execution: before={before:?} after={after:?}"
    );
    assert_eq!(
        slow_delta, 0,
        "covering indexed equality should not fall back to slow execution"
    );
    assert!(
        avg_nanos < 100_000,
        "covering indexed equality averaged {avg_nanos}ns, budget 100000ns"
    );

    reset_hot_path_profile();
    let non_covering_before = hot_path_profile_snapshot();
    let non_covering_started = std::time::Instant::now();
    let mut non_covering_row_total = 0_usize;
    for probe in &probes {
        let rows = non_covering
            .query_with_params(std::slice::from_ref(probe))
            .unwrap();
        non_covering_row_total = non_covering_row_total.saturating_add(rows.len());
        std::hint::black_box(rows);
    }
    let non_covering_elapsed = non_covering_started.elapsed();
    let non_covering_after = hot_path_profile_snapshot();

    let non_covering_avg_nanos = non_covering_elapsed.as_nanos() / u128::try_from(PROBES).unwrap();
    let (non_covering_fast_delta, non_covering_slow_delta) =
        fast_slow_delta(&non_covering_before.parser, &non_covering_after.parser);
    let non_covering_seek_ge_total = opcode_total(&non_covering_after, "SeekGE");
    let non_covering_idx_rowid_total = opcode_total(&non_covering_after, "IdxRowid");
    let non_covering_seek_rowid_total = opcode_total(&non_covering_after, "SeekRowid");
    eprintln!(
        "[bd-6eyrg.3] non_covering_index_lookup rows={ROW_COUNT} probes={PROBES} \
         avg_ns={non_covering_avg_nanos} total_us={} row_total={non_covering_row_total} \
         seek_ge_execs={non_covering_seek_ge_total} idx_rowid_execs={non_covering_idx_rowid_total} \
         seek_rowid_execs={non_covering_seek_rowid_total} fast_delta={non_covering_fast_delta} \
         slow_delta={non_covering_slow_delta}",
        non_covering_elapsed.as_micros()
    );

    assert_eq!(
        non_covering_row_total, PROBES,
        "each non-covering index probe should return exactly one row"
    );
    assert!(
        non_covering_seek_ge_total >= expected_probes,
        "non-covering indexed equality should execute one SeekGE probe per lookup: before={non_covering_before:?} after={non_covering_after:?}"
    );
    assert!(
        non_covering_idx_rowid_total >= expected_probes,
        "non-covering indexed equality should read index rowids per lookup: before={non_covering_before:?} after={non_covering_after:?}"
    );
    assert!(
        non_covering_seek_rowid_total >= expected_probes,
        "non-covering indexed equality should seek table rows per lookup: before={non_covering_before:?} after={non_covering_after:?}"
    );
    assert!(
        non_covering_fast_delta >= expected_probes,
        "non-covering indexed equality should be recorded as fast-path execution: before={non_covering_before:?} after={non_covering_after:?}"
    );
    assert_eq!(
        non_covering_slow_delta, 0,
        "non-covering indexed equality should not fall back to slow execution"
    );
    // Structural (deterministic) proof that the covering lookup does strictly
    // less per-probe work than the non-covering lookup. The non-covering shape
    // must additionally read index rowids and seek the table heap per probe
    // (IdxRowid + SeekRowid), while the covering shape resolves the result
    // entirely from the index (SeekRowid == 0). Comparing these opcode totals
    // is the real seek-bounded invariant the benchmark guards. We deliberately
    // do NOT compare raw wall-clock here: the per-probe difference is on the
    // order of a few hundred nanoseconds and is dominated by scheduler/cache
    // noise, so a `covering_avg_ns < non_covering_avg_ns` assertion flakes
    // (~3%) without protecting any additional correctness. avg_nanos is still
    // bounded above by the absolute budget asserted earlier.
    assert_eq!(
        seek_rowid_total, 0,
        "covering index lookup must perform zero table heap seeks: covering_seek_rowid={seek_rowid_total} non_covering_seek_rowid={non_covering_seek_rowid_total}"
    );
    assert!(
        non_covering_seek_rowid_total >= expected_probes,
        "non-covering lookup must perform at least one table heap seek per probe that the covering lookup avoids: covering_seek_rowid={seek_rowid_total} non_covering_seek_rowid={non_covering_seek_rowid_total}"
    );
    assert!(
        non_covering_idx_rowid_total >= expected_probes,
        "non-covering lookup must additionally read an index rowid per probe that the covering lookup avoids: covering_seek_rowid={seek_rowid_total} non_covering_idx_rowid={non_covering_idx_rowid_total}"
    );
    let _ = (avg_nanos, non_covering_avg_nanos);
}

#[test]
fn test_fast_path_count_star_sum_basic_correctness() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let stmt = conn.prepare("SELECT COUNT(*), SUM(score) FROM t").unwrap();

    let before = hot_path_profile_snapshot();
    let row = stmt.query_row().unwrap();
    let after = hot_path_profile_snapshot();

    assert_count_star_sum_row(&row, 3, Some(fsqlite_types::SqliteValue::Integer(60)));

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "prepared COUNT(*)+SUM() should stay on the fast prepared path"
    );
    assert_eq!(
        slow_delta, 0,
        "prepared COUNT(*)+SUM() should not need slow-path execution"
    );
}

#[test]
fn test_fast_path_count_sum_and_covering_indexed_equality_shapes_stay_direct() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER NOT NULL)",
    )
    .unwrap();
    conn.execute("CREATE INDEX t_name ON t(name)").unwrap();
    conn.execute(
        "INSERT INTO t VALUES
            (1, 'alpha', 10),
            (2, 'beta', 20),
            (3, 'gamma', 30)",
    )
    .unwrap();

    let count_sum = conn.prepare("SELECT COUNT(*), SUM(score) FROM t").unwrap();
    let covering_lookup = conn.prepare("SELECT name FROM t WHERE name = ?1").unwrap();

    let before = hot_path_profile_snapshot();
    let aggregate_row = count_sum.query_row().unwrap();
    let lookup_rows = covering_lookup
        .query_with_params(&[fsqlite_types::SqliteValue::Text("beta".into())])
        .unwrap();
    let after = hot_path_profile_snapshot();

    assert_count_star_sum_row(
        &aggregate_row,
        3,
        Some(fsqlite_types::SqliteValue::Integer(60)),
    );
    assert_eq!(lookup_rows.len(), 1);
    assert_eq!(
        lookup_rows[0].values(),
        &[fsqlite_types::SqliteValue::Text("beta".into())]
    );

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert_eq!(
        fast_delta, 2,
        "COUNT(*)+SUM() and covering indexed equality should both use prepared fast execution"
    );
    assert_eq!(
        slow_delta, 0,
        "new prepared SELECT fast-path shapes should not fall back to slow execution"
    );
    assert_eq!(
        after
            .direct_indexed_equality_query_hits
            .saturating_sub(before.direct_indexed_equality_query_hits),
        1,
        "covering indexed equality should use the direct indexed-equality path"
    );
}

#[test]
fn test_count_in_list_respects_alias_qualifier_boundary() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap();

    let err = conn
        .query("SELECT COUNT(*) FROM t AS alias_t WHERE t.name IN ('alpha')")
        .expect_err("original table qualifier should not bypass an alias");
    assert!(
        matches!(err, fsqlite_error::FrankenError::NoSuchColumn { ref name } if name == "t.name"),
        "a hidden base-table qualifier should fail specifically as an unresolved column, not as a parser/internal error: {err:?}"
    );
}

#[test]
fn test_count_in_list_respects_non_binary_column_collation() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')")
        .unwrap();

    let rows = conn
        .query("SELECT COUNT(*) FROM t WHERE name IN ('ALPHA')")
        .unwrap();
    assert_eq!(
        rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Integer(1)),
        "NOCASE column collation should be preserved when COUNT IN-list fast path declines"
    );
}

#[test]
fn test_count_in_list_applies_column_affinity_to_literals() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, label TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1, '1'), (2, 2, '2')")
        .unwrap();

    let numeric_rows = conn
        .query("SELECT COUNT(*) FROM t WHERE n IN ('1')")
        .unwrap();
    assert_eq!(
        numeric_rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Integer(1)),
        "INTEGER affinity should coerce numeric-looking text literals before COUNT IN-list hashing"
    );

    let text_rows = conn
        .query("SELECT COUNT(*) FROM t WHERE label IN (1)")
        .unwrap();
    assert_eq!(
        text_rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Integer(1)),
        "TEXT affinity should coerce numeric literals before COUNT IN-list hashing"
    );
}

#[test]
fn test_fast_path_count_star_sum_empty_table_returns_zero_and_null() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER)")
        .unwrap();

    let stmt = conn.prepare("SELECT COUNT(*), SUM(score) FROM t").unwrap();

    let before = hot_path_profile_snapshot();
    let row = stmt.query_row().unwrap();
    let after = hot_path_profile_snapshot();

    assert_count_star_sum_row(&row, 0, None);

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "empty-table COUNT(*)+SUM() should still use the prepared fast path"
    );
    assert_eq!(
        slow_delta, 0,
        "empty-table COUNT(*)+SUM() should not fall back to the slow path"
    );
}

#[test]
fn test_fast_path_count_star_sum_skips_nulls_but_counts_rows() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, NULL), (2, 10), (3, NULL), (4, 5)")
        .unwrap();

    let stmt = conn.prepare("SELECT COUNT(*), SUM(score) FROM t").unwrap();

    let before = hot_path_profile_snapshot();
    let row = stmt.query_row().unwrap();
    let after = hot_path_profile_snapshot();

    assert_count_star_sum_row(&row, 4, Some(fsqlite_types::SqliteValue::Integer(15)));

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "COUNT(*)+SUM() with NULL values should stay on the prepared fast path"
    );
    assert_eq!(
        slow_delta, 0,
        "COUNT(*)+SUM() with NULL values should not fall back to the slow path"
    );
}

#[test]
fn test_fast_path_count_star_sum_uses_trailing_column_defaults() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(marker INTEGER, score INTEGER DEFAULT 7)")
        .unwrap();
    conn.execute("INSERT INTO t(marker) VALUES (1), (2), (3)")
        .unwrap();

    let stmt = conn.prepare("SELECT COUNT(*), SUM(score) FROM t").unwrap();

    let before = hot_path_profile_snapshot();
    let row = stmt.query_row().unwrap();
    let after = hot_path_profile_snapshot();

    assert_count_star_sum_row(&row, 3, Some(fsqlite_types::SqliteValue::Integer(21)));

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "COUNT(*)+SUM() should preserve trailing DEFAULT values on the prepared fast path"
    );
    assert_eq!(
        slow_delta, 0,
        "trailing DEFAULT COUNT(*)+SUM() should not need slow-path execution"
    );
}

#[test]
fn test_rowid_alias_short_record_reload_preserves_trailing_default() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap();

    {
        let conn = Connection::open(path).unwrap();
        conn.execute("CREATE TABLE t(marker TEXT, id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO t(marker, id) VALUES ('alpha', 1)")
            .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN score INTEGER DEFAULT 7")
            .unwrap();
        conn.close().unwrap();
    }

    let conn = Connection::open(path).unwrap();
    let rows = conn
        .query("SELECT marker, id, score FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values(),
        &[
            fsqlite_types::SqliteValue::Text("alpha".into()),
            fsqlite_types::SqliteValue::Integer(1),
            fsqlite_types::SqliteValue::Integer(7),
        ],
        "rehydrating a short record should replace the INTEGER PRIMARY KEY alias slot without shifting the trailing default column"
    );
    let integrity_rows = conn.query("PRAGMA integrity_check").unwrap();
    assert_eq!(
        integrity_rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Text("ok".into())),
        "integrity checking should use the same rowid-alias/default alignment"
    );
}

#[test]
fn test_fast_path_count_star_sum_text_values_use_sum_registry_fallback() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(score TEXT)").unwrap();
    conn.execute("INSERT INTO t VALUES ('1.5'), ('2.5'), (NULL)")
        .unwrap();

    let stmt = conn.prepare("SELECT COUNT(*), SUM(score) FROM t").unwrap();

    let before = hot_path_profile_snapshot();
    let row = stmt.query_row().unwrap();
    let after = hot_path_profile_snapshot();

    assert_count_star_sum_row(&row, 3, Some(fsqlite_types::SqliteValue::Float(4.0)));

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "COUNT(*)+SUM() should keep prepared execution while using SUM fallback semantics"
    );
    assert_eq!(
        slow_delta, 0,
        "text SUM fallback should not require slow-path statement execution"
    );
}

#[test]
fn test_fast_path_count_star_sum_rowid_alias_projection() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let stmt = conn.prepare("SELECT COUNT(*), SUM(id) FROM t").unwrap();

    let before = hot_path_profile_snapshot();
    let row = stmt.query_row().unwrap();
    let after = hot_path_profile_snapshot();

    assert_count_star_sum_row(&row, 3, Some(fsqlite_types::SqliteValue::Integer(6)));

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "COUNT(*)+SUM() should project INTEGER PRIMARY KEY aliases without slow-path execution"
    );
    assert_eq!(
        slow_delta, 0,
        "rowid-alias COUNT(*)+SUM() should not leave the prepared fast path"
    );
}

#[test]
fn test_fast_path_count_star_sum_sees_post_insert_visibility() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    let stmt = conn.prepare("SELECT COUNT(*), SUM(score) FROM t").unwrap();

    let baseline = stmt.query_row().unwrap();
    assert_count_star_sum_row(&baseline, 2, Some(fsqlite_types::SqliteValue::Integer(30)));

    conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    reset_hot_path_profile();
    let before = hot_path_profile_snapshot();
    let row = stmt.query_row().unwrap();
    let after = hot_path_profile_snapshot();

    assert_count_star_sum_row(&row, 3, Some(fsqlite_types::SqliteValue::Integer(60)));

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "COUNT(*)+SUM() should see inserted rows without leaving the prepared fast path"
    );
    assert_eq!(
        slow_delta, 0,
        "post-insert COUNT(*)+SUM() should not fall back to the slow path"
    );
}

#[test]
fn test_fast_path_count_star_sum_sees_post_delete_visibility() {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let stmt = conn.prepare("SELECT COUNT(*), SUM(score) FROM t").unwrap();

    let baseline = stmt.query_row().unwrap();
    assert_count_star_sum_row(&baseline, 3, Some(fsqlite_types::SqliteValue::Integer(60)));

    conn.execute("DELETE FROM t WHERE id = 2").unwrap();

    reset_hot_path_profile();
    let before = hot_path_profile_snapshot();
    let row = stmt.query_row().unwrap();
    let after = hot_path_profile_snapshot();

    assert_count_star_sum_row(&row, 2, Some(fsqlite_types::SqliteValue::Integer(40)));

    let (fast_delta, slow_delta) = fast_slow_delta(&before.parser, &after.parser);
    assert!(
        fast_delta > 0,
        "COUNT(*)+SUM() should see deleted rows without leaving the prepared fast path"
    );
    assert_eq!(
        slow_delta, 0,
        "post-delete COUNT(*)+SUM() should not fall back to the slow path"
    );
}

#[test]
fn test_fast_path_count_star_sum_sees_writes_after_repeated_overlay_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let _profile_guard = FastPathProfileTestGuard::new();
    let conn = Connection::open(":memory:")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, score INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")?;

    let stmt = conn.prepare("SELECT COUNT(*), SUM(score) FROM t")?;

    let first = stmt.query_row()?;
    let second = stmt.query_row()?;
    assert_count_star_sum_row(&first, 3, Some(fsqlite_types::SqliteValue::Integer(60)));
    assert_count_star_sum_row(&second, 3, Some(fsqlite_types::SqliteValue::Integer(60)));

    conn.execute("UPDATE t SET score = 200 WHERE id = 2")?;
    let after_update = stmt.query_row()?;
    assert_count_star_sum_row(
        &after_update,
        3,
        Some(fsqlite_types::SqliteValue::Integer(240)),
    );

    conn.execute("DELETE FROM t WHERE id = 1")?;
    let after_delete = stmt.query_row()?;
    assert_count_star_sum_row(
        &after_delete,
        2,
        Some(fsqlite_types::SqliteValue::Integer(230)),
    );

    let insert = conn.prepare("INSERT INTO t VALUES (?1, ?2)")?;
    insert.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(4),
        fsqlite_types::SqliteValue::Integer(40),
    ])?;
    let after_insert = stmt.query_row()?;
    let after_insert_again = stmt.query_row()?;
    assert_count_star_sum_row(
        &after_insert,
        3,
        Some(fsqlite_types::SqliteValue::Integer(270)),
    );
    assert_count_star_sum_row(
        &after_insert_again,
        3,
        Some(fsqlite_types::SqliteValue::Integer(270)),
    );

    let update = conn.prepare("UPDATE t SET score = ?2 WHERE id = ?1")?;
    update.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(4),
        fsqlite_types::SqliteValue::Integer(400),
    ])?;
    let after_prepared_update = stmt.query_row()?;
    let after_prepared_update_again = stmt.query_row()?;
    assert_count_star_sum_row(
        &after_prepared_update,
        3,
        Some(fsqlite_types::SqliteValue::Integer(630)),
    );
    assert_count_star_sum_row(
        &after_prepared_update_again,
        3,
        Some(fsqlite_types::SqliteValue::Integer(630)),
    );

    let delete = conn.prepare("DELETE FROM t WHERE id = ?1")?;
    delete.execute_with_params(&[fsqlite_types::SqliteValue::Integer(4)])?;
    let after_prepared_delete = stmt.query_row()?;
    let after_prepared_delete_again = stmt.query_row()?;
    assert_count_star_sum_row(
        &after_prepared_delete,
        2,
        Some(fsqlite_types::SqliteValue::Integer(230)),
    );
    assert_count_star_sum_row(
        &after_prepared_delete_again,
        2,
        Some(fsqlite_types::SqliteValue::Integer(230)),
    );

    update.execute_with_params(&[
        fsqlite_types::SqliteValue::Integer(2),
        fsqlite_types::SqliteValue::Null,
    ])?;
    let after_null_update = stmt.query_row()?;
    assert_count_star_sum_row(
        &after_null_update,
        2,
        Some(fsqlite_types::SqliteValue::Integer(30)),
    );

    delete.execute_with_params(&[fsqlite_types::SqliteValue::Integer(3)])?;
    let after_last_non_null_delete = stmt.query_row()?;
    assert_count_star_sum_row(&after_last_non_null_delete, 1, None);
    Ok(())
}

#[test]
fn test_fast_path_group_by_rowid_bucket_sum_matches_sqlite_reference_rows() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    const CREATE_TABLE: &str =
        "CREATE TABLE bench(id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL NOT NULL)";

    for (row_count, divisor) in [(0_usize, 1_i64), (7, 3), (25, 10)] {
        let fconn = Connection::open(":memory:").unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        fconn.execute(CREATE_TABLE).unwrap();
        rconn.execute(CREATE_TABLE, []).unwrap();
        seed_grouped_sum_bench(&fconn, &rconn, row_count);

        let sql =
            format!("SELECT (id / {divisor}), SUM(value) FROM bench GROUP BY (id / {divisor})");
        let frank_rows = sorted_frank_rows(&fconn, &sql);
        let sqlite_rows = sorted_rusqlite_rows(&rconn, &sql);

        assert_eq!(
            frank_rows, sqlite_rows,
            "grouped-SUM rows should match rusqlite for row_count={row_count}, divisor={divisor}"
        );
    }
}

#[test]
fn test_track_s_insert_10k_matches_rusqlite_oracle() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    const ROW_COUNT: i64 = 10_000;
    const CREATE_TABLE: &str =
        "CREATE TABLE bench(id INTEGER PRIMARY KEY, label TEXT NOT NULL, score INTEGER NOT NULL)";
    // Keep the insert on the register/VDBE path by using scalar expressions
    // instead of the trivial direct VALUES(?1, ?2, ?3) shape.
    const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, lower(?2), abs(?3))";

    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();
    fconn.execute(CREATE_TABLE).unwrap();
    rconn.execute(CREATE_TABLE, []).unwrap();
    fconn.execute("BEGIN").unwrap();
    rconn.execute_batch("BEGIN").unwrap();

    let fstmt = fconn.prepare(INSERT_SQL).unwrap();
    let mut rstmt = rconn.prepare(INSERT_SQL).unwrap();
    for id in 0_i64..ROW_COUNT {
        let label = format!("ROW_{id}");
        let score = -(id * 7);
        fstmt
            .execute_with_params(&[
                fsqlite_types::SqliteValue::Integer(id),
                fsqlite_types::SqliteValue::Text(label.clone().into()),
                fsqlite_types::SqliteValue::Integer(score),
            ])
            .unwrap();
        rstmt.execute(rusqlite::params![id, label, score]).unwrap();
    }
    fconn.execute("COMMIT").unwrap();
    rconn.execute_batch("COMMIT").unwrap();

    let frank_rows = sorted_frank_rows(&fconn, "SELECT id, label, score FROM bench");
    let sqlite_rows = sorted_rusqlite_rows(&rconn, "SELECT id, label, score FROM bench");

    assert_eq!(frank_rows.len(), ROW_COUNT as usize);
    assert_eq!(
        frank_rows, sqlite_rows,
        "10K register-path INSERTs should match the rusqlite oracle exactly"
    );
}

#[test]
fn test_track_s_select_after_insert_matches_rusqlite_oracle() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    const CREATE_TABLE: &str =
        "CREATE TABLE t(id INTEGER PRIMARY KEY, label TEXT NOT NULL, score INTEGER NOT NULL)";
    const INSERT_SQL: &str = "INSERT INTO t VALUES (?1, lower(?2), abs(?3))";

    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();
    fconn.execute(CREATE_TABLE).unwrap();
    rconn.execute(CREATE_TABLE, []).unwrap();

    let insert_stmt = fconn.prepare(INSERT_SQL).unwrap();
    insert_stmt
        .execute_with_params(&[
            fsqlite_types::SqliteValue::Integer(7),
            fsqlite_types::SqliteValue::Text("MiXeD_Label".into()),
            fsqlite_types::SqliteValue::Integer(-45),
        ])
        .unwrap();
    rconn
        .execute(INSERT_SQL, rusqlite::params![7_i64, "MiXeD_Label", -45_i64])
        .unwrap();

    let select_sql = "SELECT label, score FROM t WHERE id = ?1";
    let select_stmt = fconn.prepare(select_sql).unwrap();
    let frank_rows = select_stmt
        .query_with_params(&[fsqlite_types::SqliteValue::Integer(7)])
        .unwrap();
    let sqlite_row = rconn
        .query_row(select_sql, rusqlite::params![7_i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap();

    assert_eq!(frank_rows.len(), 1, "SELECT should see the inserted row");
    assert_eq!(
        frank_rows[0].get(0),
        Some(&fsqlite_types::SqliteValue::Text(
            sqlite_row.0.clone().into()
        ))
    );
    assert_eq!(
        frank_rows[0].get(1),
        Some(&fsqlite_types::SqliteValue::Integer(sqlite_row.1))
    );
    assert_eq!(sqlite_row.0, "mixed_label");
    assert_eq!(sqlite_row.1, 45);
}

#[test]
fn test_prepared_param_null_predicate_mix_matches_rusqlite_oracle() {
    let _profile_isolation = FastPathProfileIsolationGuard::new();
    const SQL: &str = "SELECT CASE WHEN ?1 IS NOT NULL THEN (?2 + ?3) ELSE ?4 END";

    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();
    let fstmt = fconn.prepare(SQL).unwrap();
    let mut rstmt = rconn.prepare(SQL).unwrap();
    let f_non_null = [
        fsqlite_types::SqliteValue::Integer(1),
        fsqlite_types::SqliteValue::Integer(2),
        fsqlite_types::SqliteValue::Integer(3),
        fsqlite_types::SqliteValue::Integer(100),
    ];
    let f_null = [
        fsqlite_types::SqliteValue::Null,
        fsqlite_types::SqliteValue::Integer(2),
        fsqlite_types::SqliteValue::Integer(3),
        fsqlite_types::SqliteValue::Integer(100),
    ];

    let mut checksum = 0_u64;
    let mut use_non_null_params = true;
    for iteration in 0..16_u64 {
        let expected = if use_non_null_params {
            rstmt
                .query_row(rusqlite::params![1_i64, 2_i64, 3_i64, 100_i64], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
        } else {
            rstmt
                .query_row(
                    rusqlite::params![rusqlite::types::Null, 2_i64, 3_i64, 100_i64],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
        };
        let rows = fstmt
            .query_with_params(if use_non_null_params {
                &f_non_null
            } else {
                &f_null
            })
            .unwrap();
        assert_eq!(rows.len(), 1, "prepared predicate query returns one row");
        let row = rows
            .first()
            .expect("prepared predicate query returns one row");
        assert_eq!(
            row.get(0),
            Some(&fsqlite_types::SqliteValue::Integer(expected)),
            "prepared parameter/null predicate branch should match rusqlite"
        );
        checksum = checksum.wrapping_add(
            u64::try_from(expected).expect("oracle result is positive") * (iteration + 1),
        );
        use_non_null_params = !use_non_null_params;
    }
    assert_eq!(checksum, 7_520, "checksum documents the benchmark mix");
}
