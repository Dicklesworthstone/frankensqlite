//! bd-e6zfc: Non-atomic schema cache reloads across 3 RefCells (views,
//! triggers, schema_by_name) — concurrent DDL during reload can diverge
//! caches.
//!
//! ## Bug hypothesis
//!
//! Schema cache is stored in 3 separate RefCells: views, triggers, and
//! schema_by_name. When the cache is reloaded (e.g., after DDL from another
//! connection), each RefCell is updated independently. If a concurrent DDL
//! occurs between updates to these RefCells, the caches can diverge:
//! - views might reflect the new schema while triggers reflects the old
//! - schema_by_name might contain a table that triggers doesn't know about
//!
//! ## Test approach
//!
//! Exercise concurrent DDL + DML to detect divergence between schema caches.
//! Since this is a single-connection issue (RefCells are per-connection),
//! the test uses multiple connections with concurrent DDL.
//!
//! - S1: Concurrent CREATE VIEW + SELECT through view
//! - S2: Concurrent CREATE TRIGGER + trigger-firing DML
//! - S3: Rapid schema changes (ALTER TABLE) with cached queries
//! - S4: View + trigger interaction under concurrent DDL
//! - S5: Schema cache coherence after concurrent DDL storm
#![recursion_limit = "512"]

use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, process::Command, process::Stdio};

use fsqlite::{Connection, FrankenError, Row, SqliteValue};

const STRESS_DURATION: Duration = Duration::from_secs(2);
// This bounds retry admission and backoff, not an in-flight async operation.
// The supervised child process below provides the hard wall-clock deadline.
const SCHEMA_STORM_RETRY_BUDGET: Duration = Duration::from_secs(5);
const SCHEMA_STORM_CHILD_ENV: &str = "FSQLITE_SCHEMA_STORM_CHILD";
const SCHEMA_STORM_RECEIPT_ENV: &str = "FSQLITE_SCHEMA_STORM_RECEIPT";
const SCHEMA_STORM_RECEIPT_PREFIX: &str = "FSQLITE_SCHEMA_STORM_COMPLETE:";

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

fn supervise_schema_storm_test() -> bool {
    const TEST_NAME: &str = "s5_schema_coherence_after_storm";
    const TIMEOUT: Duration = Duration::from_secs(90);

    match (
        env::var_os(SCHEMA_STORM_CHILD_ENV),
        env::var_os(SCHEMA_STORM_RECEIPT_ENV),
    ) {
        (Some(child_token), Some(receipt_token)) if child_token == receipt_token => return false,
        (None, None) => {}
        _ => panic!("inconsistent inherited schema-storm supervision environment"),
    }

    let receipt_token = format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos()
    );
    let expected_receipt = format!("{SCHEMA_STORM_RECEIPT_PREFIX}{receipt_token}");
    let test_binary = env::current_exe().expect("resolve current test binary");
    let mut child = Command::new(test_binary)
        .args(["--exact", TEST_NAME, "--include-ignored", "--nocapture"])
        .env(SCHEMA_STORM_CHILD_ENV, &receipt_token)
        .env(SCHEMA_STORM_RECEIPT_ENV, &receipt_token)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn supervised schema-storm child");
    let child_stdout = child
        .stdout
        .take()
        .expect("capture supervised schema-storm child stdout");
    let mut receipt_reader = Some(std::thread::spawn(move || {
        let mut found = false;
        for line in BufReader::new(child_stdout).lines() {
            if line.expect("read supervised schema-storm child stdout") == expected_receipt {
                found = true;
            }
        }
        found
    }));
    let deadline = Instant::now() + TIMEOUT;

    loop {
        match child.try_wait().expect("poll schema-storm child") {
            Some(status) => {
                let receipt_found = receipt_reader
                    .take()
                    .expect("receipt reader must be present")
                    .join()
                    .expect("schema-storm receipt reader must not panic");
                assert!(
                    status.success(),
                    "supervised schema-storm child failed with {status}"
                );
                assert!(
                    receipt_found,
                    "supervised schema-storm child exited without its completion receipt"
                );
                return true;
            }
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                child.wait().expect("reap timed-out schema-storm child");
                receipt_reader
                    .take()
                    .expect("receipt reader must be present")
                    .join()
                    .expect("schema-storm receipt reader must not panic");
                panic!("supervised schema-storm test exceeded {TIMEOUT:?}");
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

fn is_schema_storm_transient(error: &FrankenError) -> bool {
    matches!(
        error,
        FrankenError::Busy
            | FrankenError::BusyRecovery
            | FrankenError::BusySnapshot { .. }
            | FrankenError::DatabaseLocked { .. }
            | FrankenError::SchemaChanged
    )
}

struct SchemaStormClosePermit<'a> {
    lock: &'a AtomicBool,
}

impl Drop for SchemaStormClosePermit<'_> {
    fn drop(&mut self) {
        self.lock.store(false, Ordering::Release);
    }
}

fn acquire_schema_storm_close_permit(lock: &AtomicBool) -> SchemaStormClosePermit<'_> {
    while lock
        .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        std::thread::yield_now();
    }
    SchemaStormClosePermit { lock }
}

async fn execute_schema_storm_step(
    conn: &Connection,
    sql: &str,
    transient_retries: &AtomicU64,
) -> Result<usize, FrankenError> {
    let deadline = Instant::now() + SCHEMA_STORM_RETRY_BUDGET;
    let mut retries = 0_u32;
    loop {
        match conn.execute(sql).await {
            Ok(changed) => return Ok(changed),
            Err(error) if is_schema_storm_transient(&error) => {
                transient_retries.fetch_add(1, Ordering::Relaxed);
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                let backoff = Duration::from_millis(1_u64 << retries.min(4));
                std::thread::sleep(backoff.min(remaining));
                if Instant::now() >= deadline {
                    return Err(error);
                }
                retries = retries.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn query_schema_storm_step(
    conn: &Connection,
    sql: &str,
    transient_retries: &AtomicU64,
) -> Result<Vec<Row>, FrankenError> {
    let deadline = Instant::now() + SCHEMA_STORM_RETRY_BUDGET;
    let mut retries = 0_u32;
    loop {
        match conn.query(sql).await {
            Ok(rows) => return Ok(rows),
            Err(error) if is_schema_storm_transient(&error) => {
                transient_retries.fetch_add(1, Ordering::Relaxed);
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                let backoff = Duration::from_millis(1_u64 << retries.min(4));
                std::thread::sleep(backoff.min(remaining));
                if Instant::now() >= deadline {
                    return Err(error);
                }
                retries = retries.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn open_schema_storm_connection(
    path: &str,
    transient_retries: &AtomicU64,
) -> Result<Connection, FrankenError> {
    let deadline = Instant::now() + SCHEMA_STORM_RETRY_BUDGET;
    let mut retries = 0_u32;
    loop {
        match Connection::open(path).await {
            Ok(conn) => return Ok(conn),
            Err(error) if is_schema_storm_transient(&error) => {
                transient_retries.fetch_add(1, Ordering::Relaxed);
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                let backoff = Duration::from_millis(1_u64 << retries.min(4));
                std::thread::sleep(backoff.min(remaining));
                if Instant::now() >= deadline {
                    return Err(error);
                }
                retries = retries.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn assert_schema_storm_object_absent(
    conn: &Connection,
    sql: &str,
    expected_name: &str,
    transient_retries: &AtomicU64,
) {
    match query_schema_storm_step(conn, sql, transient_retries).await {
        Err(FrankenError::NoSuchTable { name }) => {
            assert_eq!(
                name, expected_name,
                "missing-object error named the wrong schema object"
            );
        }
        Err(error) => {
            panic!("dropped schema object `{expected_name}` returned an unexpected error: {error}");
        }
        Ok(rows) => {
            panic!(
                "dropped schema object `{expected_name}` remained queryable with {} row(s)",
                rows.len()
            );
        }
    }
}

fn assert_anchor_sentinel(rows: &[Row], context: &str) {
    assert_eq!(rows.len(), 1, "{context}: anchor row count changed");
    assert_eq!(
        rows[0].values(),
        &[
            SqliteValue::Integer(7),
            SqliteValue::Text("sentinel".into()),
        ],
        "{context}: anchor sentinel changed"
    );
}

// ─── S1: Concurrent CREATE VIEW + SELECT ───────────────────────────

#[test]
fn s1_concurrent_create_view_select() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("s1.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn.execute(&format!("INSERT INTO base VALUES ({i}, 'row_{i}')"))
                    .await
                    .expect("seed");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // DDL: creates and drops views
        let d_path = path_str.to_string();
        let d_stop = Arc::clone(&stop);
        let ddl = std::thread::spawn(move || {
            let mut ops = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&d_path).await.expect("d open");
                while !d_stop.load(Ordering::Relaxed) {
                    let vname = format!("v_{}", ops % 5);
                    conn.execute(&format!(
                        "CREATE VIEW IF NOT EXISTS {vname} AS SELECT * FROM base WHERE id <= {}",
                        (ops % 100) + 1
                    ))
                    .await
                    .ok();
                    conn.execute(&format!("DROP VIEW IF EXISTS {vname}"))
                        .await
                        .ok();
                    ops += 1;
                }
            });
            ops
        });

        // Reader: queries views and base table
        let r_path = path_str.to_string();
        let r_stop = Arc::clone(&stop);
        let reader = std::thread::spawn(move || {
            let mut reads = 0u64;
            let mut errors = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&r_path).await.expect("r open");
                while !r_stop.load(Ordering::Relaxed) {
                    // Try to read through view (may not exist)
                    let vname = format!("v_{}", reads % 5);
                    match conn.query(&format!("SELECT * FROM {vname}")).await {
                        Ok(_) => reads += 1,
                        Err(_) => errors += 1,
                    }
                    // Base table should always work
                    if let Ok(rows) = conn.query("SELECT * FROM base").await {
                        assert!(
                            rows.len() >= 100,
                            "base table corrupted: {} rows",
                            rows.len()
                        );
                    }
                }
            });
            (reads, errors)
        });

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let ddl_ops = ddl.join().expect("DDL must not panic");
        let (reads, errors) = reader.join().expect("reader must not panic");

        eprintln!("S1: {ddl_ops} DDL ops, {reads} view reads, {errors} expected errors");
    });
}

// ─── S2: Concurrent CREATE TRIGGER + DML ───────────────────────────

#[test]
fn s2_concurrent_create_trigger_dml() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("s2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("create events");
            conn.execute("CREATE TABLE log (event_id INTEGER)")
                .await
                .expect("create log");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // DDL: creates and drops triggers
        let d_path = path_str.to_string();
        let d_stop = Arc::clone(&stop);
        let ddl = std::thread::spawn(move || {
            let mut ops = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&d_path).await.expect("d open");
                while !d_stop.load(Ordering::Relaxed) {
                    conn.execute(
                        "CREATE TRIGGER IF NOT EXISTS t_log AFTER INSERT ON events \
                         BEGIN INSERT INTO log VALUES (NEW.id); END",
                    )
                    .await
                    .ok();
                    std::thread::sleep(Duration::from_millis(50));
                    conn.execute("DROP TRIGGER IF EXISTS t_log").await.ok();
                    std::thread::sleep(Duration::from_millis(50));
                    ops += 1;
                }
            });
            ops
        });

        // Writer: inserts into events (may or may not fire trigger)
        let w_path = path_str.to_string();
        let w_stop = Arc::clone(&stop);
        let writer = std::thread::spawn(move || {
            let mut inserted = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&w_path).await.expect("w open");
                while !w_stop.load(Ordering::Relaxed) {
                    if conn.execute("BEGIN").await.is_ok() {
                        conn.execute(&format!(
                            "INSERT INTO events VALUES ({inserted}, 'e_{inserted}')"
                        ))
                        .await
                        .ok();
                        if conn.execute("COMMIT").await.is_err() {
                            conn.execute("ROLLBACK").await.ok();
                        }
                        inserted += 1;
                    }
                }
            });
            inserted
        });

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let ddl_ops = ddl.join().expect("DDL must not panic");
        let inserted = writer.join().expect("writer must not panic");

        // The key assertion: no panics from schema cache divergence
        let verify = Connection::open(path_str).await.expect("verify");
        let events = verify
            .query("SELECT * FROM events")
            .await
            .expect("events")
            .len();
        let logs = verify.query("SELECT * FROM log").await.expect("log").len();

        // Logs can be less than events (trigger was sometimes dropped)
        assert!(
            logs <= events,
            "more logs ({logs}) than events ({events}) — trigger fired extra times?"
        );
        eprintln!(
            "S2: {ddl_ops} trigger DDL ops, {inserted} inserts, {events} events, {logs} logs"
        );
    });
}

// ─── S3: Rapid ALTER TABLE with cached queries ─────────────────────

#[test]
fn s3_alter_table_cached_queries() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("s3.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE evolving (id INTEGER PRIMARY KEY, base_col TEXT)")
                .await
                .expect("create");
            conn.execute("INSERT INTO evolving VALUES (1, 'initial')")
                .await
                .expect("seed");
        }

        let stop = Arc::new(AtomicBool::new(false));

        // DDL: adds columns
        let d_path = path_str.to_string();
        let d_stop = Arc::clone(&stop);
        let ddl = std::thread::spawn(move || {
            let mut ops = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&d_path).await.expect("d open");
                while !d_stop.load(Ordering::Relaxed) {
                    let col = format!("col_{ops}");
                    conn.execute(&format!(
                        "ALTER TABLE evolving ADD COLUMN {col} TEXT DEFAULT 'def'"
                    ))
                    .await
                    .ok();
                    ops += 1;
                    if ops > 50 {
                        break; // Don't add infinite columns
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            });
            ops
        });

        // Reader: continuously queries with SELECT *
        let r_path = path_str.to_string();
        let r_stop = Arc::clone(&stop);
        let reader = std::thread::spawn(move || {
            let mut reads = 0u64;
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&r_path).await.expect("r open");
                while !r_stop.load(Ordering::Relaxed) {
                    if conn.query("SELECT * FROM evolving").await.is_ok() {
                        reads += 1;
                    }
                }
            });
            reads
        });

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        let alter_ops = ddl.join().expect("DDL must not panic");
        let reads = reader.join().expect("reader must not panic");

        eprintln!("S3: {alter_ops} ALTER TABLE ADD COLUMN ops, {reads} reads — no crash");
    });
}

// ─── S4: View + trigger interaction under DDL ──────────────────────

#[test]
fn s4_view_trigger_interaction_ddl() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");

        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create data");
        conn.execute("CREATE TABLE audit (data_id INTEGER, action TEXT)")
            .await
            .expect("create audit");

        // Cycle: create view → create trigger → insert (fires trigger, visible via view) →
        //        drop trigger → drop view → verify cleanup
        for round in 0..10 {
            conn.execute("CREATE VIEW v_audit AS SELECT data_id, action FROM audit")
                .await
                .expect("create view");
            conn.execute(
                "CREATE TRIGGER t_audit AFTER INSERT ON data \
                 BEGIN INSERT INTO audit VALUES (NEW.id, 'added'); END",
            )
            .await
            .expect("create trigger");

            let id = round + 1;
            conn.execute(&format!("INSERT INTO data VALUES ({id}, 'round_{round}')"))
                .await
                .expect("insert");

            // View should show the trigger's insertion
            let view_rows = conn.query("SELECT * FROM v_audit").await.expect("view");
            assert_eq!(
                view_rows.len(),
                id as usize,
                "round {round}: view should show {id} rows"
            );

            conn.execute("DROP TRIGGER t_audit")
                .await
                .expect("drop trigger");
            conn.execute("DROP VIEW v_audit").await.expect("drop view");

            // Audit table should still have data (trigger was dropped, not rolled back)
            let audit_rows = conn.query("SELECT * FROM audit").await.expect("audit");
            assert_eq!(
                audit_rows.len(),
                id as usize,
                "round {round}: audit should still have {id} rows after trigger/view drop"
            );
        }
        eprintln!("S4: 10 rounds of view+trigger create/drop cycles — schema coherent");
    });
}

// ─── S5: Schema cache coherence after DDL storm ────────────────────

#[test]
fn s5_schema_coherence_after_storm() {
    if supervise_schema_storm_test() {
        return;
    }

    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("s5.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE anchor (id INTEGER PRIMARY KEY, marker TEXT NOT NULL)")
                .await
                .expect("create anchor");
            conn.execute("INSERT INTO anchor VALUES (7, 'sentinel')")
                .await
                .expect("seed anchor sentinel");
            conn.close().await.expect("close setup connection");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let total_rounds = Arc::new(AtomicU64::new(0));
        let transient_retries = Arc::new(AtomicU64::new(0));
        let workers_with_progress = Arc::new(AtomicU64::new(0));
        let close_in_progress = Arc::new(AtomicBool::new(false));
        let start = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = mpsc::channel();

        // Four DDL workers repeatedly create, exercise, and drop thread-owned
        // tables and views. Every step is checked: transient contention is
        // retried, while every non-transient error fails the worker.
        let threads: Vec<_> = (0_u64..4)
            .map(|tid| {
                let path = path_str.to_string();
                let s = Arc::clone(&stop);
                let rounds = Arc::clone(&total_rounds);
                let retries = Arc::clone(&transient_retries);
                let progress = Arc::clone(&workers_with_progress);
                let worker_close_lock = Arc::clone(&close_in_progress);
                let worker_start = Arc::clone(&start);
                let worker_ready = ready_tx.clone();
                std::thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = open_schema_storm_connection(&path, retries.as_ref())
                            .await
                            .expect("open DDL worker connection");
                        worker_ready.send(()).expect("report DDL worker readiness");
                        drop(worker_ready);
                        while !worker_start.load(Ordering::Acquire) {
                            if s.load(Ordering::Acquire) {
                                let _close_permit =
                                    acquire_schema_storm_close_permit(&worker_close_lock);
                                conn.close_without_checkpoint()
                                    .await
                                    .expect("close cancelled DDL worker connection");
                                return;
                            }
                            std::thread::yield_now();
                        }
                        if s.load(Ordering::Acquire) {
                            let _close_permit =
                                acquire_schema_storm_close_permit(&worker_close_lock);
                            conn.close_without_checkpoint()
                                .await
                                .expect("close cancelled DDL worker connection");
                            return;
                        }
                        let mut completed_rounds = 0u64;
                        while !s.load(Ordering::Relaxed) {
                            let name = format!("obj_{tid}_{}", completed_rounds % 3);
                            execute_schema_storm_step(
                                &conn,
                                &format!(
                                    "CREATE TABLE IF NOT EXISTS {name} (id INTEGER PRIMARY KEY, v TEXT)"
                                ),
                                retries.as_ref(),
                            )
                            .await
                            .expect("create storm table");
                            execute_schema_storm_step(
                                &conn,
                                &format!(
                                    "CREATE VIEW IF NOT EXISTS v_{name} AS SELECT * FROM {name}"
                                ),
                                retries.as_ref(),
                            )
                            .await
                            .expect("create storm view");
                            execute_schema_storm_step(
                                &conn,
                                &format!(
                                    "INSERT OR REPLACE INTO {name} VALUES ({completed_rounds}, 'data')"
                                ),
                                retries.as_ref(),
                            )
                            .await
                            .expect("insert storm row");

                            let visible = query_schema_storm_step(
                                &conn,
                                &format!(
                                    "SELECT id FROM v_{name} WHERE id = {completed_rounds}"
                                ),
                                retries.as_ref(),
                            )
                            .await
                            .expect("query storm row through view");
                            assert_eq!(
                                visible.len(),
                                1,
                                "worker {tid} round {completed_rounds}: inserted row not visible through owned view"
                            );

                            execute_schema_storm_step(
                                &conn,
                                &format!("DROP VIEW IF EXISTS v_{name}"),
                                retries.as_ref(),
                            )
                            .await
                            .expect("drop storm view");
                            execute_schema_storm_step(
                                &conn,
                                &format!("DROP TABLE IF EXISTS {name}"),
                                retries.as_ref(),
                            )
                            .await
                            .expect("drop storm table");

                            let table_sql = format!("SELECT * FROM {name}");
                            let view_name = format!("v_{name}");
                            let view_sql = format!("SELECT * FROM {view_name}");
                            if (completed_rounds + tid).is_multiple_of(2) {
                                assert_schema_storm_object_absent(
                                    &conn,
                                    &view_sql,
                                    &view_name,
                                    retries.as_ref(),
                                )
                                .await;
                                assert_schema_storm_object_absent(
                                    &conn,
                                    &table_sql,
                                    &name,
                                    retries.as_ref(),
                                )
                                .await;
                            } else {
                                assert_schema_storm_object_absent(
                                    &conn,
                                    &table_sql,
                                    &name,
                                    retries.as_ref(),
                                )
                                .await;
                                assert_schema_storm_object_absent(
                                    &conn,
                                    &view_sql,
                                    &view_name,
                                    retries.as_ref(),
                                )
                                .await;
                            }
                            let residual_objects = query_schema_storm_step(
                                &conn,
                                &format!(
                                    "SELECT name FROM sqlite_master WHERE name IN ('{name}', '{view_name}')"
                                ),
                                retries.as_ref(),
                            )
                            .await
                            .expect("verify storm objects were dropped from the catalog");
                            assert!(
                                residual_objects.is_empty(),
                                "worker {tid} round {completed_rounds}: dropped table or view remains visible"
                            );
                            completed_rounds += 1;
                        }

                        let anchor_rows = query_schema_storm_step(
                            &conn,
                            "SELECT id, marker FROM anchor",
                            retries.as_ref(),
                        )
                        .await
                        .expect("hot worker cache retains anchor table");
                        assert_anchor_sentinel(&anchor_rows, "hot worker cache");
                        assert!(
                            completed_rounds > 0,
                            "schema-storm worker {tid} completed no verified rounds"
                        );
                        rounds.fetch_add(completed_rounds, Ordering::Relaxed);
                        progress.fetch_add(1, Ordering::Relaxed);
                        // The worker's job is complete, and the final verifier below owns
                        // the single checkpoint after all workers have joined. Avoid making
                        // concurrent worker shutdown contend on an unrelated checkpoint.
                        let _close_permit =
                            acquire_schema_storm_close_permit(&worker_close_lock);
                        conn.close_without_checkpoint()
                            .await
                            .expect("close DDL worker connection without checkpoint");
                    });
                })
            })
            .collect();

        drop(ready_tx);
        let readiness_deadline = Instant::now() + Duration::from_secs(30);
        let readiness = (0..4).try_for_each(|_| {
            ready_rx
                .recv_timeout(readiness_deadline.saturating_duration_since(Instant::now()))
                .map(|_| ())
        });
        if let Err(error) = readiness {
            stop.store(true, Ordering::Release);
            start.store(true, Ordering::Release);
            let mut panicked_workers = 0_u64;
            for thread in threads {
                if thread.join().is_err() {
                    panicked_workers += 1;
                }
            }
            panic!(
                "schema-storm workers did not all become ready: {error}; panicked_workers={panicked_workers}"
            );
        }
        start.store(true, Ordering::Release);

        std::thread::sleep(STRESS_DURATION);
        stop.store(true, Ordering::Relaxed);

        for t in threads {
            t.join()
                .expect("thread must not panic (schema cache divergence?)");
        }

        let completed_rounds = total_rounds.load(Ordering::Relaxed);
        assert_eq!(
            workers_with_progress.load(Ordering::Relaxed),
            4,
            "every schema-storm worker must complete a verified round"
        );

        // Anchor table must survive
        let verify = open_schema_storm_connection(path_str, transient_retries.as_ref())
            .await
            .expect("open verification connection");
        let anchor_rows = query_schema_storm_step(
            &verify,
            "SELECT id, marker FROM anchor",
            transient_retries.as_ref(),
        )
        .await
        .expect("anchor table missing after DDL storm");
        assert_anchor_sentinel(&anchor_rows, "final verifier");
        verify.close().await.expect("close verification connection");
        eprintln!(
            "S5: {completed_rounds} verified DDL rounds across 4 threads with {} transient retries, anchor table intact",
            transient_retries.load(Ordering::Relaxed)
        );
    });

    let receipt = env::var(SCHEMA_STORM_RECEIPT_ENV)
        .expect("supervised schema-storm child must receive a completion receipt token");
    println!("{SCHEMA_STORM_RECEIPT_PREFIX}{receipt}");
}
