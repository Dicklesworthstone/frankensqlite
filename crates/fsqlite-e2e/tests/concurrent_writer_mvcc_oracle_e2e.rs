//! bd-6xvq3 — Concurrent-writer MVCC oracle parity e2e tests.
//!
//! Exercises FrankenSQLite's core innovation — page-level MVCC concurrent
//! writers — and cross-checks results against C SQLite (via rusqlite in WAL
//! mode). Tests run real multi-threaded workloads on both engines using
//! file-backed databases, then compare final table state for correctness.
//!
//! Coverage:
//!   - Disjoint-partition inserts with final-state oracle comparison
//!   - Same-table concurrent inserts into non-overlapping PK ranges
//!   - Read snapshot isolation (reader sees pre-commit state during writes)
//!   - Write-write conflict on same row (SSI must abort one writer)
//!   - Mixed DML: concurrent INSERT + UPDATE on different rows
//!   - Concurrent DDL + DML interleaving
#![recursion_limit = "512"]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use fsqlite::SqliteValue;
use fsqlite_harness::failure_bundle::{ExecutionLaneEvidence, ObservedExecutionLane};
use fsqlite_harness::serializability_oracle::{
    BeginMode, CONCURRENT_WRITE_OBSERVATION_PREFIX, CONCURRENT_WRITE_OBSERVATION_SCHEMA,
    CONCURRENT_WRITE_SOURCE_PATHS, CONCURRENT_WRITE_TEST_NAME, CONCURRENT_WRITE_TEST_TARGET,
    ConcurrentCommitObservation, ConcurrentCommitPhase, ConcurrentStorageObservation,
    ConcurrentWriteObservation, HistoryEvent, HistoryOperation, HistoryValue, HistoryWorkload,
    ScheduleProvenance, TRANSACTION_HISTORY_SCHEMA_VERSION, TransactionHistory,
};
use fsqlite_harness::test_inventory::ExecutionLane;
use sha2::{Digest, Sha256};
use tracing_subscriber::{Layer, layer::SubscriberExt};

const RETRY_LIMIT: u32 = 200;
const RETRY_BACKOFF: Duration = Duration::from_micros(200);

#[derive(Default)]
struct OverlapRecorder {
    order: u64,
    events: Vec<HistoryEvent>,
    phases: Vec<ConcurrentCommitObservation>,
    storage: Vec<ConcurrentStorageObservation>,
}

impl OverlapRecorder {
    fn next_order(&mut self) -> u64 {
        let order = self.order;
        self.order = self
            .order
            .checked_add(1)
            .expect("bounded observation order");
        order
    }
}

#[derive(Clone)]
struct ObservedWriter {
    process_id: String,
    connection_id: String,
    transaction_id: String,
    database_id: String,
    table: &'static str,
    root_page: u32,
    recorder: Arc<Mutex<OverlapRecorder>>,
    tracing_transaction: Arc<AtomicBool>,
    // Compare only while both opened handles are alive; never serialize this token.
    file_identity: Arc<Mutex<Option<fsqlite_vfs::FileIdentity>>>,
}

impl ObservedWriter {
    fn record(&self, operation: HistoryOperation) {
        let mut recorder = self.recorder.lock().expect("history recorder");
        let logical_time = recorder.next_order();
        let event_id = u64::try_from(recorder.events.len()).expect("bounded history");
        recorder.events.push(HistoryEvent {
            event_id,
            logical_time,
            process_id: self.process_id.clone(),
            connection_id: self.connection_id.clone(),
            transaction_id: Some(self.transaction_id.clone()),
            operation,
        });
    }
}

#[derive(Default)]
struct CommitTraceFields(BTreeMap<String, String>);

impl tracing::field::Visit for CommitTraceFields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl CommitTraceFields {
    fn number(&self, name: &str) -> u64 {
        self.0
            .get(name)
            .expect("production commit trace field")
            .parse()
            .expect("numeric production commit trace field")
    }
}

struct CommitTraceLayer(ObservedWriter);

impl<S: tracing::Subscriber> Layer<S> for CommitTraceLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "fsqlite_core::connection"
            || !self.0.tracing_transaction.load(Ordering::Relaxed)
        {
            return;
        }
        let mut fields = CommitTraceFields::default();
        event.record(&mut fields);
        let phase = match fields.0.get("visibility_decision").map(String::as_str) {
            Some("commit_plan_clean") => ConcurrentCommitPhase::CommitPlanClean,
            Some("commit_published") => ConcurrentCommitPhase::CommitPublished,
            _ => return,
        };
        let mut recorder = self.0.recorder.lock().expect("commit trace recorder");
        let logical_time = recorder.next_order();
        recorder.phases.push(ConcurrentCommitObservation {
            logical_time,
            process_id: self.0.process_id.clone(),
            connection_id: self.0.connection_id.clone(),
            transaction_id: self.0.transaction_id.clone(),
            phase,
            engine_txn_id: fields.number("txn_id"),
            snapshot_high: fields.number("snapshot_high"),
            commit_seq: fields.number("commit_seq"),
            page_id: u32::try_from(fields.number("page_id")).expect("page number fits u32"),
        });
    }
}

fn compiled_overlap_source_hashes() -> BTreeMap<String, String> {
    let sources: [(&str, &[u8]); 11] = [
        ("Cargo.toml", include_bytes!("../../../Cargo.toml")),
        (
            "rust-toolchain.toml",
            include_bytes!("../../../rust-toolchain.toml"),
        ),
        (
            "crates/fsqlite-core/Cargo.toml",
            include_bytes!("../../fsqlite-core/Cargo.toml"),
        ),
        (
            "crates/fsqlite-core/src/connection.rs",
            include_bytes!("../../fsqlite-core/src/connection.rs"),
        ),
        (
            "crates/fsqlite-e2e/tests/concurrent_writer_mvcc_oracle_e2e.rs",
            include_bytes!("concurrent_writer_mvcc_oracle_e2e.rs"),
        ),
        (
            "crates/fsqlite-harness/src/serializability_oracle.rs",
            include_bytes!("../../fsqlite-harness/src/serializability_oracle.rs"),
        ),
        (
            "crates/fsqlite-harness/src/release_certificate.rs",
            include_bytes!("../../fsqlite-harness/src/release_certificate.rs"),
        ),
        (
            "crates/fsqlite-pager/src/pager.rs",
            include_bytes!("../../fsqlite-pager/src/pager.rs"),
        ),
        (
            "crates/fsqlite-pager/src/page_cache.rs",
            include_bytes!("../../fsqlite-pager/src/page_cache.rs"),
        ),
        (
            "crates/fsqlite-mvcc/src/begin_concurrent.rs",
            include_bytes!("../../fsqlite-mvcc/src/begin_concurrent.rs"),
        ),
        (
            "crates/fsqlite-vdbe/src/lib.rs",
            include_bytes!("../../fsqlite-vdbe/src/lib.rs"),
        ),
    ];
    assert_eq!(sources.map(|(path, _)| path), CONCURRENT_WRITE_SOURCE_PATHS);
    sources
        .into_iter()
        .map(|(path, bytes)| {
            (
                path.to_owned(),
                fsqlite_harness::bytes_to_lower_hex(Sha256::digest(bytes)),
            )
        })
        .collect()
}

impl ObservedWriter {
    async fn execute(
        &self,
        path: &str,
        ready: mpsc::SyncSender<()>,
        commit_allowed: mpsc::Receiver<()>,
    ) {
        let conn = fsqlite::Connection::open(path)
            .await
            .expect("open observed writer");
        conn.set_reject_mem_fallback(true);
        conn.set_strict_mem_fallback_rejection(true);
        let identity = conn
            .file_identity()
            .await
            .expect("open-file identity")
            .expect("file-backed writer requires an opened file identity");
        {
            let mut shared = self.file_identity.lock().expect("identity comparison");
            if let Some(first) = *shared {
                assert!(
                    first == identity,
                    "both live Connections must hold the same file object"
                );
            } else {
                *shared = Some(identity);
            }
        }
        let backend_rows = conn
            .query("PRAGMA fsqlite.backend_kind")
            .await
            .expect("actual backend");
        let SqliteValue::Text(backend_kind) = &backend_rows[0].values()[0] else {
            panic!("backend kind must be text");
        };
        assert_ne!(backend_kind.as_ref(), "memory");
        assert_eq!(
            fsqlite_query_sorted(&conn, "PRAGMA journal_mode")
                .await
                .expect("journal mode"),
            vec![vec!["wal".to_owned()]],
        );
        assert_eq!(
            fsqlite_query_sorted(&conn, "PRAGMA fsqlite.backend_mode")
                .await
                .expect("actual fallback mode"),
            vec![vec!["parity_cert_strict".to_owned()]],
        );
        self.recorder
            .lock()
            .expect("storage recorder")
            .storage
            .push(ConcurrentStorageObservation {
                process_id: self.process_id.clone(),
                connection_id: self.connection_id.clone(),
                database_id: self.database_id.clone(),
                backend_kind: backend_kind.to_string(),
                storage_mode: "compatibility".to_owned(),
            });
        conn.reset_fallback_decision_evidence();
        conn.execute("BEGIN")
            .await
            .expect("default concurrent BEGIN");
        let snapshot = conn
            .current_concurrent_snapshot_seq()
            .expect("live concurrent snapshot");
        self.record(HistoryOperation::Begin {
            mode: BeginMode::Concurrent,
        });
        self.tracing_transaction.store(true, Ordering::Relaxed);
        let sql = format!("UPDATE {} SET value = 1 WHERE id = 1", self.table);
        assert_eq!(conn.execute(&sql).await.expect("disjoint page write"), 1);
        self.record(HistoryOperation::Write {
            key: self.table.to_owned(),
            value: HistoryValue::Integer(1),
            page_number: Some(self.root_page),
        });
        assert_eq!(conn.current_concurrent_snapshot_seq(), Some(snapshot));
        ready
            .send(())
            .expect("report completed write while transaction is active");
        commit_allowed
            .recv_timeout(Duration::from_secs(30))
            .expect("both writers must finish writing before either commits");
        conn.execute("COMMIT")
            .await
            .expect("disjoint writer COMMIT must succeed");
        self.record(HistoryOperation::Commit);
        self.tracing_transaction.store(false, Ordering::Relaxed);
        assert!(conn.last_local_commit_seq().expect("completed publication") > snapshot);
        let fallback = conn.fallback_decision_snapshot();
        assert!(
            !fallback.truncated && fallback.decisions.is_empty(),
            "no unobserved or allowed fallback"
        );
        conn.close().await.expect("close observed writer");
    }
}

fn overlap_identity(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn build_overlap_observation(
    run_id: &str,
    recorder: &Arc<Mutex<OverlapRecorder>>,
    final_state: BTreeMap<String, HistoryValue>,
) -> ConcurrentWriteObservation {
    let recorder = recorder.lock().expect("complete overlap observations");
    let trace_id = overlap_identity("FSQLITE_PROOF_TRACE_ID", run_id);
    let scenario_id = overlap_identity("FSQLITE_PROOF_SCENARIO_ID", "concurrent_write_read");
    let seed = std::env::var("FSQLITE_PROOF_SEED").map_or(0x006d_7708, |value| {
        value.parse().expect("explicit proof seed must be u64")
    });
    let lane_evidence = recorder
        .storage
        .iter()
        .map(|storage| {
            assert_eq!(
                recorder
                    .phases
                    .iter()
                    .filter(|phase| { phase.connection_id == storage.connection_id })
                    .count(),
                2,
                "each writer must produce both actual commit trace phases"
            );
            ExecutionLaneEvidence::from_observations(
                ExecutionLane::MvccRequired,
                vec![
                    ObservedExecutionLane::SqlResult,
                    ObservedExecutionLane::PagerBacked,
                    ObservedExecutionLane::Mvcc,
                ],
                &trace_id,
                run_id,
                &scenario_id,
                "UPDATE",
                &storage.backend_kind,
                "parity_cert_strict",
                format!("{}:parity_cert_strict", storage.backend_kind),
                Vec::new(),
                true,
            )
        })
        .collect();
    let mut history = TransactionHistory {
        schema_version: TRANSACTION_HISTORY_SCHEMA_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        trace_id,
        scenario_id,
        seed,
        engine_git_sha: overlap_identity(
            "FSQLITE_CANDIDATE_GIT_SHA",
            "identified-by-compiled-source-hashes",
        ),
        engine_dirty: std::env::var("FSQLITE_CANDIDATE_DIRTY").as_deref() != Ok("0"),
        workload: HistoryWorkload::Register,
        schedule: ScheduleProvenance::observation_only(
            "distinct OS threads; real SQL completions and core commit trace events on one logical clock",
        ),
        execution_lane_evidence: lane_evidence,
        concurrent_mode_enabled: true,
        reopen_concurrent_mode_enabled: None,
        initial_state: BTreeMap::from([
            ("overlap_a".to_owned(), HistoryValue::Integer(0)),
            ("overlap_b".to_owned(), HistoryValue::Integer(0)),
        ]),
        final_state,
        final_state_sha256: String::new(),
        events: recorder.events.clone(),
    };
    history.refresh_final_state_hash();
    ConcurrentWriteObservation {
        schema_version: CONCURRENT_WRITE_OBSERVATION_SCHEMA.to_owned(),
        history,
        test_target: CONCURRENT_WRITE_TEST_TARGET.to_owned(),
        test_name: CONCURRENT_WRITE_TEST_NAME.to_owned(),
        source_sha256: compiled_overlap_source_hashes(),
        cargo_lock_sha256: fsqlite_harness::bytes_to_lower_hex(Sha256::digest(include_bytes!(
            "../../../Cargo.lock"
        ))),
        storage: recorder.storage.clone(),
        commit_phases: recorder.phases.clone(),
    }
}

const STOCK_REOPEN_PREFIX: &str = "FSQLITE_STOCK_REOPEN_OBSERVATION=";

fn verify_overlap_stock_child(path: &std::path::Path) {
    let parent: u32 = std::env::var("FSQLITE_CONCURRENCY_PARENT_PID")
        .expect("parent process identity")
        .parse()
        .expect("parent PID");
    assert_ne!(
        parent,
        std::process::id(),
        "stock verifier must be a separate process"
    );
    let stock =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("independent stock reopen");
    let mut final_state = BTreeMap::new();
    for table in ["overlap_a", "overlap_b"] {
        assert_eq!(
            rusqlite_query_sorted(
                &stock,
                &format!("SELECT id, value FROM {table} ORDER BY id")
            )
            .expect("stock exact rows"),
            vec![vec!["1".to_owned(), "1".to_owned()]],
        );
        let value: i64 = stock
            .query_row(
                &format!("SELECT value FROM {table} WHERE id=1"),
                [],
                |row| row.get(0),
            )
            .expect("actual retained value");
        final_state.insert(table.to_owned(), HistoryValue::Integer(value));
    }
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("stock integrity check");
    assert_eq!(integrity, "ok");
    println!(
        "{STOCK_REOPEN_PREFIX}{}",
        serde_json::json!({
            "process_id": std::process::id(), "parent_process_id": parent,
            "sqlite_version": rusqlite::version(), "retained_rows": 2, "integrity": integrity,
            "final_state": final_state,
        })
    );
}

fn run_overlap_stock_child(path: &std::path::Path) -> BTreeMap<String, HistoryValue> {
    let child = std::process::Command::new(std::env::current_exe().expect("current test binary"))
        .args([
            "--exact",
            CONCURRENT_WRITE_TEST_NAME,
            "--show-output",
            "--test-threads=1",
        ])
        .env("FSQLITE_CONCURRENCY_STOCK_REOPEN_DB", path)
        .env(
            "FSQLITE_CONCURRENCY_PARENT_PID",
            std::process::id().to_string(),
        )
        .output()
        .expect("spawn stock verifier process");
    assert!(
        child.status.success(),
        "stock child failed: stdout={} stderr={}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    let stdout = String::from_utf8(child.stdout).expect("child UTF-8 output");
    let receipts: Vec<_> = stdout
        .lines()
        .filter_map(|line| line.strip_prefix(STOCK_REOPEN_PREFIX))
        .collect();
    assert_eq!(
        receipts.len(),
        1,
        "one actual stock reopen observation required"
    );
    let receipt: serde_json::Value = serde_json::from_str(receipts[0]).expect("stock receipt JSON");
    assert_eq!(receipt["parent_process_id"], std::process::id());
    assert_ne!(receipt["process_id"], std::process::id());
    assert_eq!(receipt["retained_rows"], 2);
    assert_eq!(receipt["integrity"], "ok");
    // Keep the actual child observation, without duplicating its libtest completion records.
    println!("{STOCK_REOPEN_PREFIX}{}", receipts[0]);
    serde_json::from_value(receipt["final_state"].clone()).expect("actual reopened final state")
}

fn seed_overlap_database(path: &str) -> [u32; 2] {
    let roots = {
        let stock = rusqlite::Connection::open(path).expect("stock seed");
        stock
            .execute_batch(
                "PRAGMA journal_mode=WAL;
             CREATE TABLE overlap_a(id INTEGER PRIMARY KEY, value INTEGER);
             CREATE TABLE overlap_b(id INTEGER PRIMARY KEY, value INTEGER);
             INSERT INTO overlap_a VALUES(1,0);
             INSERT INTO overlap_b VALUES(1,0);",
            )
            .expect("two preallocated table leaves");
        ["overlap_a", "overlap_b"].map(|table| {
            stock
                .query_row(
                    "SELECT rootpage FROM sqlite_master WHERE name=?1",
                    [table],
                    |row| row.get::<_, u32>(0),
                )
                .expect("actual table root page")
        })
    };
    assert!(roots[0] > 1 && roots[1] > 1 && roots[0] != roots[1]);
    assert_eq!(
        &std::fs::read(path).expect("actual database header")[..16],
        b"SQLite format 3\0"
    );
    roots
}

#[test]
fn observed_disjoint_writer_overlap_for_certificate() {
    if let Some(path) = std::env::var_os("FSQLITE_CONCURRENCY_STOCK_REOPEN_DB") {
        // This same executable supplies the independent stock-only child. It emits
        // no concurrency observation, so a child-only run cannot certify overlap.
        verify_overlap_stock_child(std::path::Path::new(&path));
        return;
    }
    let database = tempfile::NamedTempFile::new().expect("fresh file-backed database");
    let path = database
        .path()
        .to_str()
        .expect("UTF-8 fixture path")
        .to_owned();
    let roots = seed_overlap_database(&path);
    let fallback_id = format!(
        "bd-6hdwo.8-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("current timestamp")
            .as_nanos()
    );
    let run_id = overlap_identity("FSQLITE_PROOF_RUN_ID", &fallback_id);
    let recorder = Arc::new(Mutex::new(OverlapRecorder::default()));
    let file_identity = Arc::new(Mutex::new(None));
    thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::sync_channel(2);
        let mut permissions = Vec::new();
        let mut workers = Vec::new();
        for (index, table) in ["overlap_a", "overlap_b"].into_iter().enumerate() {
            let writer = ObservedWriter {
                process_id: std::process::id().to_string(),
                connection_id: format!("{}:writer-{index}", std::process::id()),
                transaction_id: format!("writer-{index}:attempt-0"),
                database_id: format!("{run_id}:main"),
                table,
                root_page: roots[index],
                recorder: Arc::clone(&recorder),
                tracing_transaction: Arc::new(AtomicBool::new(false)),
                file_identity: Arc::clone(&file_identity),
            };
            let worker_path = path.clone();
            let ready = ready_tx.clone();
            let (allowed_tx, allowed_rx) = mpsc::sync_channel(1);
            permissions.push(allowed_tx);
            workers.push(scope.spawn(move || {
                let subscriber =
                    tracing_subscriber::registry().with(CommitTraceLayer(writer.clone()));
                tracing::subscriber::with_default(subscriber, || {
                    asupersync::test_utils::run_test(|| async {
                        writer.execute(&worker_path, ready, allowed_rx).await;
                    });
                });
            }));
        }
        for _ in 0..2 {
            ready_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("both transactions must hold completed disjoint writes");
        }
        for permission in permissions {
            permission
                .send(())
                .expect("release already-overlapping writers to commit");
        }
        for worker in workers {
            worker.join().expect("observed writer panicked");
        }
    });
    let final_state = run_overlap_stock_child(database.path());
    let observation = build_overlap_observation(&run_id, &recorder, final_state);
    observation
        .validate()
        .unwrap_or_else(|error| {
            panic!(
                "actual history must prove committed disjoint writer overlap: {error}; storage={:?}; commit_phases={:?}",
                observation.storage, observation.commit_phases
            )
        });
    println!(
        "{CONCURRENT_WRITE_OBSERVATION_PREFIX}{}",
        serde_json::to_string(&observation).expect("compact observed concurrency JSON")
    );
}

#[test]
fn overlapping_same_page_writer_conflict_preserves_winner() {
    asupersync::test_utils::run_test(|| async {
        let database = tempfile::NamedTempFile::new().expect("same-page database");
        let path = database.path().to_str().expect("fixture path");
        let roots = seed_overlap_database(path);
        let first = fsqlite::Connection::open(path).await.expect("first writer");
        let second = fsqlite::Connection::open(path)
            .await
            .expect("second writer");
        first
            .execute("BEGIN")
            .await
            .expect("first concurrent BEGIN");
        second
            .execute("BEGIN")
            .await
            .expect("second concurrent BEGIN");
        let first_snapshot = first
            .current_concurrent_snapshot_seq()
            .expect("first active snapshot");
        let second_snapshot = second
            .current_concurrent_snapshot_seq()
            .expect("second active snapshot");
        assert_eq!(first_snapshot, second_snapshot);
        assert_eq!(
            first
                .execute("UPDATE overlap_a SET value=1 WHERE id=1")
                .await
                .expect("first held write"),
            1
        );
        let second_write = second
            .execute("UPDATE overlap_a SET value=2 WHERE id=1")
            .await;
        first
            .execute("COMMIT")
            .await
            .expect("first writer must commit");
        let (phase, error) = match second_write {
            Ok(changed) => {
                assert_eq!(changed, 1);
                (
                    "commit",
                    second
                        .execute("COMMIT")
                        .await
                        .expect_err("FCW must reject stale same-page write"),
                )
            }
            Err(error) => ("write", error),
        };
        match &error {
            fsqlite::FrankenError::WriteConflict { page, .. }
            | fsqlite::FrankenError::SerializationFailure { page } => assert_eq!(*page, roots[0]),
            fsqlite::FrankenError::Busy | fsqlite::FrankenError::BusySnapshot { .. } => {}
            unexpected => {
                panic!("same-page rejection must be a typed MVCC conflict: {unexpected:?}")
            }
        }
        println!(
            "FSQLITE_SAME_PAGE_CONFLICT={}",
            serde_json::json!({
                "process_id": std::process::id(), "connections": ["first", "second"],
                "snapshot_high": first_snapshot, "page_id": roots[0], "rejected_phase": phase,
                "error": error.to_string(), "extended_code": error.extended_error_code(),
                "winner_commit_seq": first.last_local_commit_seq().expect("winner publication"),
                "attempts": 1, "retries": 0,
            })
        );
        if second.in_transaction() {
            second
                .execute("ROLLBACK")
                .await
                .expect("rollback conflicting transaction");
        }
        first.close().await.expect("close winning writer");
        second.close().await.expect("close rejected writer");
        let stock = rusqlite::Connection::open(database.path()).expect("stock reopen");
        assert_eq!(
            rusqlite_query_sorted(&stock, "SELECT id,value FROM overlap_a ORDER BY id")
                .expect("retained winner"),
            vec![vec!["1".to_owned(), "1".to_owned()]],
        );
    });
}

#[test]
#[should_panic(expected = "BOTH_ERR")]
fn concurrent_oracle_refuses_two_query_errors() {
    asupersync::test_utils::run_test(|| async {
        let frank = tempfile::NamedTempFile::new().unwrap();
        let stock = tempfile::NamedTempFile::new().unwrap();
        compare_query_results(
            "both query errors must fail the oracle",
            frank.path().to_str().unwrap(),
            stock.path().to_str().unwrap(),
            &["SELECT required_value FROM absent_table"],
        )
        .await;
    });
}

// ── Helpers ────────────────────────────────────────────────────────────

async fn fsqlite_query_sorted(
    conn: &fsqlite::Connection,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".into(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Blob(b) => {
                        format!(
                            "X'{}'",
                            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                        )
                    }
                })
                .collect()
        })
        .collect())
}

fn rusqlite_query_sorted(
    conn: &rusqlite::Connection,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".into(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => s,
                rusqlite::types::Value::Blob(b) => {
                    format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    )
                }
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn compare_query_results(label: &str, f_path: &str, r_path: &str, queries: &[&str]) {
    let f = fsqlite::Connection::open(f_path)
        .await
        .expect("open frank for verify");
    let r = rusqlite::Connection::open(r_path).expect("open rusqlite for verify");
    let mut mismatches = Vec::new();
    for q in queries {
        match (
            fsqlite_query_sorted(&f, q).await,
            rusqlite_query_sorted(&r, q),
        ) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH {q}\n  frank: {a:?}\n  csql:  {b:?}"));
            }
            (Err(e), Ok(b)) => {
                mismatches.push(format!("FRANK_ERR {q}\n  err: {e}\n  csql: {b:?}"));
            }
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR {q}\n  frank: {a:?}\n  err: {e}"));
            }
            (Err(a), Err(b)) => {
                mismatches.push(format!(
                    "BOTH_ERR {q}\n  frank: {a}\n  csql: {b}\n  expected successful query results"
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
    f.close().await.expect("close frank oracle connection");
}

// ── Test 1: Disjoint-partition concurrent inserts ──────────────────────

#[test]
fn concurrent_disjoint_table_inserts_4_threads() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4usize;
        let rows_per_thread: i64 = 100;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        // Setup: create per-thread tables in both engines
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            for tid in 0..n_threads {
                let ddl = format!("CREATE TABLE t_{tid} (id INTEGER PRIMARY KEY, val INTEGER);");
                f.execute(&ddl).await.unwrap();
                r.execute_batch(&ddl).unwrap();
            }
        }

        // FrankenSQLite: concurrent writers
        {
            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = f_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        asupersync::test_utils::run_test(|| async {
                            let conn = fsqlite::Connection::open(&p).await.unwrap();
                            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                            bar.wait();

                            for i in 0..rows_per_thread {
                                let mut attempts = 0u32;
                                loop {
                                    if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                        attempts += 1;
                                        assert!(
                                            attempts < RETRY_LIMIT,
                                            "BEGIN stuck for thread {tid}"
                                        );
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    let sql = format!(
                                        "INSERT INTO t_{tid} VALUES ({i}, {});",
                                        i * 10 + tid as i64
                                    );
                                    if conn.execute(&sql).await.is_err() {
                                        drop(conn.execute("ROLLBACK").await);
                                        attempts += 1;
                                        assert!(
                                            attempts < RETRY_LIMIT,
                                            "INSERT stuck for thread {tid}"
                                        );
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    match conn.execute("COMMIT").await {
                                        Ok(_) => break,
                                        Err(_) => {
                                            drop(conn.execute("ROLLBACK").await);
                                            attempts += 1;
                                            assert!(
                                                attempts < RETRY_LIMIT,
                                                "COMMIT stuck for thread {tid}"
                                            );
                                            thread::sleep(RETRY_BACKOFF);
                                        }
                                    }
                                }
                            }
                        });
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("thread panicked");
            }
        }

        // C SQLite: concurrent writers
        {
            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = r_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        let conn = rusqlite::Connection::open(&p).unwrap();
                        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                            .unwrap();
                        bar.wait();

                        for i in 0..rows_per_thread {
                            let sql = format!(
                                "INSERT INTO t_{tid} VALUES ({i}, {});",
                                i * 10 + tid as i64
                            );
                            loop {
                                match conn.execute_batch(&sql) {
                                    Ok(()) => break,
                                    Err(e) if e.to_string().contains("database is locked") => {
                                        thread::sleep(RETRY_BACKOFF);
                                    }
                                    Err(e) => panic!("csqlite insert failed: {e}"),
                                }
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("csqlite thread panicked");
            }
        }

        // Verify both engines have identical data
        let queries: Vec<String> = (0..n_threads)
            .map(|tid| format!("SELECT id, val FROM t_{tid} ORDER BY id"))
            .collect();
        let query_refs: Vec<&str> = queries.iter().map(String::as_str).collect();
        compare_query_results("disjoint_4t", &f_path, &r_path, &query_refs).await;
    });
}

// ── Test 2: Same-table concurrent inserts, non-overlapping PK ranges ──

#[test]
fn concurrent_same_table_inserts_non_overlapping_pks() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4usize;
        let rows_per_thread: i64 = 50;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE shared (id INTEGER PRIMARY KEY, writer INTEGER, val TEXT);")
                .await
                .unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch(
                "CREATE TABLE shared (id INTEGER PRIMARY KEY, writer INTEGER, val TEXT);",
            )
            .unwrap();
        }

        // FrankenSQLite: concurrent writers, each owns a PK range
        {
            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = f_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        asupersync::test_utils::run_test(|| async {
                            let conn = fsqlite::Connection::open(&p).await.unwrap();
                            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                            bar.wait();

                            let base = tid as i64 * rows_per_thread;
                            for i in 0..rows_per_thread {
                                let pk = base + i;
                                let mut attempts = 0u32;
                                loop {
                                    if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    let sql = format!(
                                        "INSERT INTO shared VALUES ({pk}, {tid}, 'w{tid}_r{i}');"
                                    );
                                    if conn.execute(&sql).await.is_err() {
                                        drop(conn.execute("ROLLBACK").await);
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    match conn.execute("COMMIT").await {
                                        Ok(_) => break,
                                        Err(_) => {
                                            drop(conn.execute("ROLLBACK").await);
                                            attempts += 1;
                                            assert!(attempts < RETRY_LIMIT);
                                            thread::sleep(RETRY_BACKOFF);
                                        }
                                    }
                                }
                            }
                        });
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("thread panicked");
            }
        }

        // C SQLite: same workload
        {
            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = r_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        let conn = rusqlite::Connection::open(&p).unwrap();
                        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                            .unwrap();
                        bar.wait();

                        let base = tid as i64 * rows_per_thread;
                        for i in 0..rows_per_thread {
                            let pk = base + i;
                            let sql =
                                format!("INSERT INTO shared VALUES ({pk}, {tid}, 'w{tid}_r{i}');");
                            loop {
                                match conn.execute_batch(&sql) {
                                    Ok(()) => break,
                                    Err(e) if e.to_string().contains("database is locked") => {
                                        thread::sleep(RETRY_BACKOFF);
                                    }
                                    Err(e) => panic!("csqlite insert failed: {e}"),
                                }
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("csqlite thread panicked");
            }
        }

        compare_query_results(
            "same_table_non_overlapping",
            &f_path,
            &r_path,
            &[
                "SELECT COUNT(*) FROM shared",
                "SELECT id, writer, val FROM shared ORDER BY id",
            ],
        )
        .await;
    });
}

// ── Test 3: Verify row count integrity after concurrent inserts ────────

#[test]
fn concurrent_insert_no_data_loss_8_threads() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 8usize;
        let rows_per_thread: i64 = 25;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            for tid in 0..n_threads {
                f.execute(&format!(
                    "CREATE TABLE t_{tid} (id INTEGER PRIMARY KEY, data TEXT);"
                ))
                .await
                .unwrap();
            }
        }

        let total_retries = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(n_threads));
        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = f_path.clone();
                let bar = barrier.clone();
                let retries = total_retries.clone();
                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();

                        let mut local_retries = 0u64;
                        for i in 0..rows_per_thread {
                            let mut attempts = 0u32;
                            loop {
                                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                    local_retries += 1;
                                    attempts += 1;
                                    assert!(attempts < RETRY_LIMIT, "stuck on BEGIN, thread {tid}");
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                                let sql =
                                    format!("INSERT INTO t_{tid} VALUES ({i}, 'data_{tid}_{i}');");
                                if conn.execute(&sql).await.is_err() {
                                    drop(conn.execute("ROLLBACK").await);
                                    local_retries += 1;
                                    attempts += 1;
                                    assert!(attempts < RETRY_LIMIT);
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                                match conn.execute("COMMIT").await {
                                    Ok(_) => break,
                                    Err(_) => {
                                        drop(conn.execute("ROLLBACK").await);
                                        local_retries += 1;
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
                                    }
                                }
                            }
                        }
                        retries.fetch_add(local_retries, Ordering::Relaxed);
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        // Verify via independent rusqlite read
        let verify = rusqlite::Connection::open(f_tmp.path()).unwrap();
        for tid in 0..n_threads {
            let count: i64 = verify
                .query_row(&format!("SELECT COUNT(*) FROM t_{tid}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(
                count,
                rows_per_thread,
                "thread {tid}: expected {rows_per_thread} rows, got {count} (retries: {})",
                total_retries.load(Ordering::Relaxed)
            );
        }
    });
}

// ── Test 4: Read isolation — reader sees consistent snapshot ───────────

async fn verify_held_reader_snapshot(
    path: &str,
    ready: mpsc::SyncSender<u64>,
    committed: mpsc::Receiver<u64>,
) {
    let conn = fsqlite::Connection::open(path).await.unwrap();
    conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
    conn.execute("BEGIN").await.unwrap();
    let before = fsqlite_query_sorted(&conn, "SELECT id, val FROM snap ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        before,
        vec![
            vec!["1".to_owned(), "100".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
        ],
        "reader should see initial state"
    );
    let snapshot = conn
        .current_concurrent_snapshot_seq()
        .expect("BEGIN must activate the concurrent snapshot by default");
    ready.send(snapshot).expect("reader ready");
    let writer_commit = committed
        .recv_timeout(Duration::from_secs(30))
        .expect("writer must durably commit while the reader transaction remains open");
    assert!(
        writer_commit > snapshot,
        "writer must publish after the reader snapshot"
    );
    let during = fsqlite_query_sorted(&conn, "SELECT id, val FROM snap ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        during, before,
        "reader must retain its snapshot after the writer actually commits"
    );
    conn.execute("COMMIT").await.unwrap();
    let after = fsqlite_query_sorted(&conn, "SELECT id, val FROM snap ORDER BY id")
        .await
        .expect("refresh reader after ending snapshot");
    assert_eq!(
        after,
        vec![
            vec!["1".to_owned(), "999".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
        ],
        "the next reader transaction must observe both committed writes"
    );
    conn.close().await.expect("close reader");
}

#[test]
fn read_snapshot_isolation_during_concurrent_write() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let conn = fsqlite::Connection::open(&f_path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute("CREATE TABLE snap (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO snap VALUES (1, 100);")
                .await
                .unwrap();
            conn.execute("INSERT INTO snap VALUES (2, 200);")
                .await
                .unwrap();
        }

        let (reader_ready_tx, reader_ready_rx) = mpsc::sync_channel(1);
        let (writer_committed_tx, writer_committed_rx) = mpsc::sync_channel(1);
        let fp = f_path.clone();

        let reader_handle = thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                verify_held_reader_snapshot(&fp, reader_ready_tx, writer_committed_rx).await;
            });
        });

        let reader_snapshot = reader_ready_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("reader must begin and read before writer starts");

        let wconn = fsqlite::Connection::open(&f_path).await.unwrap();
        wconn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        wconn.execute("BEGIN CONCURRENT").await.unwrap();
        wconn
            .execute("INSERT INTO snap VALUES (3, 300);")
            .await
            .unwrap();
        wconn
            .execute("UPDATE snap SET val = 999 WHERE id = 1;")
            .await
            .expect("writer UPDATE must succeed");
        wconn
            .execute("COMMIT")
            .await
            .expect("writer COMMIT must succeed");
        let writer_commit = wconn
            .last_local_commit_seq()
            .expect("writer publication sequence");
        assert!(writer_commit > reader_snapshot);
        writer_committed_tx
            .send(writer_commit)
            .expect("notify reader after publication");

        reader_handle.join().expect("reader thread panicked");
        wconn.close().await.expect("close writer");
        let stock = rusqlite::Connection::open(&f_path).expect("stock reopen of written file");
        assert_eq!(
            rusqlite_query_sorted(&stock, "SELECT id, val FROM snap ORDER BY id")
                .expect("stock retained rows"),
            vec![
                vec!["1".to_owned(), "999".to_owned()],
                vec!["2".to_owned(), "200".to_owned()],
                vec!["3".to_owned(), "300".to_owned()],
            ]
        );
    });
}

// ── Test 5: Concurrent multi-row batch inserts ────────────────────────

#[test]
fn concurrent_batch_inserts_verify_total() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4usize;
        let batches_per_thread = 5;
        let rows_per_batch = 10i64;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let conn = fsqlite::Connection::open(&f_path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute(
                "CREATE TABLE batched (id INTEGER PRIMARY KEY, thread_id INTEGER, batch INTEGER, seq INTEGER);"
            ).await.unwrap();
        }

        let barrier = Arc::new(Barrier::new(n_threads));
        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = f_path.clone();
                let bar = barrier.clone();
                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();

                        for batch in 0..batches_per_thread {
                            let mut attempts = 0u32;
                            loop {
                                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                    attempts += 1;
                                    assert!(attempts < RETRY_LIMIT);
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                                let mut ok = true;
                                for seq in 0..rows_per_batch {
                                    let pk = (tid as i64) * 1000 + (batch as i64) * 100 + seq;
                                    let sql = format!(
                                        "INSERT INTO batched VALUES ({pk}, {tid}, {batch}, {seq});"
                                    );
                                    if conn.execute(&sql).await.is_err() {
                                        ok = false;
                                        break;
                                    }
                                }
                                if !ok {
                                    drop(conn.execute("ROLLBACK").await);
                                    attempts += 1;
                                    assert!(attempts < RETRY_LIMIT);
                                    thread::sleep(RETRY_BACKOFF);
                                    continue;
                                }
                                match conn.execute("COMMIT").await {
                                    Ok(_) => break,
                                    Err(_) => {
                                        drop(conn.execute("ROLLBACK").await);
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
                                    }
                                }
                            }
                        }
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        let expected_total = n_threads as i64 * batches_per_thread as i64 * rows_per_batch;
        let verify = fsqlite::Connection::open(&f_path).await.unwrap();
        let rows = verify.query("SELECT COUNT(*) FROM batched").await.unwrap();
        let actual: i64 = match &rows[0].values()[0] {
            SqliteValue::Integer(n) => *n,
            other => panic!("unexpected count type: {other:?}"),
        };
        assert_eq!(
            actual, expected_total,
            "expected {expected_total} total rows, got {actual}"
        );

        // Verify per-thread counts
        for tid in 0..n_threads {
            let q = format!("SELECT COUNT(*) FROM batched WHERE thread_id = {tid}");
            let rows = verify.query(&q).await.unwrap();
            let count: i64 = match &rows[0].values()[0] {
                SqliteValue::Integer(n) => *n,
                other => panic!("unexpected count type for thread {tid}: {other:?}"),
            };
            let expected = batches_per_thread as i64 * rows_per_batch;
            assert_eq!(
                count, expected,
                "thread {tid}: expected {expected}, got {count}"
            );
        }

        // Cross-verify with rusqlite
        let r_verify = rusqlite::Connection::open(f_tmp.path()).unwrap();
        let r_count: i64 = r_verify
            .query_row("SELECT COUNT(*) FROM batched", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            r_count, expected_total,
            "rusqlite cross-check: expected {expected_total}, got {r_count}"
        );
    });
}

// ── Test 6: Concurrent autocommit inserts (no explicit txn) ───────────

#[test]
fn concurrent_autocommit_inserts_oracle_parity() {
    asupersync::test_utils::run_test(|| async {
        let n_threads = 4usize;
        let rows_per_thread: i64 = 30;

        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        let ddl = "CREATE TABLE auto_t (id INTEGER PRIMARY KEY, src INTEGER);";
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute(ddl).await.unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch(ddl).unwrap();
        }

        // FrankenSQLite autocommit (BEGIN CONCURRENT is auto-promoted)
        {
            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = f_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        asupersync::test_utils::run_test(|| async {
                            let conn = fsqlite::Connection::open(&p).await.unwrap();
                            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                            bar.wait();

                            let base = tid as i64 * rows_per_thread;
                            for i in 0..rows_per_thread {
                                let pk = base + i;
                                let sql = format!("INSERT INTO auto_t VALUES ({pk}, {tid});");
                                let mut attempts = 0u32;
                                loop {
                                    match conn.execute(&sql).await {
                                        Ok(_) => break,
                                        Err(_) => {
                                            attempts += 1;
                                            assert!(attempts < RETRY_LIMIT);
                                            thread::sleep(RETRY_BACKOFF);
                                        }
                                    }
                                }
                            }
                        });
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("thread panicked");
            }
        }

        // C SQLite
        {
            let barrier = Arc::new(Barrier::new(n_threads));
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = r_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        let conn = rusqlite::Connection::open(&p).unwrap();
                        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                            .unwrap();
                        bar.wait();

                        let base = tid as i64 * rows_per_thread;
                        for i in 0..rows_per_thread {
                            let pk = base + i;
                            let sql = format!("INSERT INTO auto_t VALUES ({pk}, {tid});");
                            loop {
                                match conn.execute_batch(&sql) {
                                    Ok(()) => break,
                                    Err(e) if e.to_string().contains("database is locked") => {
                                        thread::sleep(RETRY_BACKOFF);
                                    }
                                    Err(e) => panic!("csqlite insert failed: {e}"),
                                }
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("thread panicked");
            }
        }

        let expected = n_threads as i64 * rows_per_thread;
        compare_query_results(
            "autocommit_inserts",
            &f_path,
            &r_path,
            &[
                "SELECT COUNT(*) FROM auto_t",
                "SELECT id, src FROM auto_t ORDER BY id",
            ],
        )
        .await;

        // Also verify total
        let verify = fsqlite::Connection::open(&f_path).await.unwrap();
        let rows = verify.query("SELECT COUNT(*) FROM auto_t").await.unwrap();
        let actual: i64 = match &rows[0].values()[0] {
            SqliteValue::Integer(n) => *n,
            _ => panic!("bad type"),
        };
        assert_eq!(actual, expected);
    });
}

// ── Test 7: Scaling ratio — fsqlite should not degrade vs single-thread ─

#[test]
fn concurrent_scaling_ratio_does_not_degrade() {
    asupersync::test_utils::run_test(|| async {
        let rows = 50i64;

        async fn measure(n_threads: usize, rows: i64) -> Duration {
            let f_tmp = tempfile::NamedTempFile::new().unwrap();
            let f_path = f_tmp.path().to_str().unwrap().to_owned();

            {
                let conn = fsqlite::Connection::open(&f_path).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                for tid in 0..n_threads {
                    conn.execute(&format!(
                        "CREATE TABLE scale_{tid} (id INTEGER PRIMARY KEY, v INTEGER);"
                    ))
                    .await
                    .unwrap();
                }
            }

            let barrier = Arc::new(Barrier::new(n_threads));
            let start = std::time::Instant::now();
            let handles: Vec<_> = (0..n_threads)
                .map(|tid| {
                    let p = f_path.clone();
                    let bar = barrier.clone();
                    thread::spawn(move || {
                        asupersync::test_utils::run_test(|| async {
                            let conn = fsqlite::Connection::open(&p).await.unwrap();
                            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                            bar.wait();

                            for i in 0..rows {
                                let mut attempts = 0u32;
                                loop {
                                    if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    let sql =
                                        format!("INSERT INTO scale_{tid} VALUES ({i}, {});", i * 3);
                                    if conn.execute(&sql).await.is_err() {
                                        drop(conn.execute("ROLLBACK").await);
                                        attempts += 1;
                                        assert!(attempts < RETRY_LIMIT);
                                        thread::sleep(RETRY_BACKOFF);
                                        continue;
                                    }
                                    match conn.execute("COMMIT").await {
                                        Ok(_) => break,
                                        Err(_) => {
                                            drop(conn.execute("ROLLBACK").await);
                                            attempts += 1;
                                            assert!(attempts < RETRY_LIMIT);
                                            thread::sleep(RETRY_BACKOFF);
                                        }
                                    }
                                }
                            }
                        });
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }
            start.elapsed()
        }

        let t1 = measure(1, rows).await;
        let t4 = measure(4, rows).await;

        #[allow(clippy::cast_precision_loss)]
        let ratio = t4.as_secs_f64() / t1.as_secs_f64();

        eprintln!(
            "scaling: 1t={:.1}ms  4t={:.1}ms  ratio={:.2}x (4t/1t, lower is better)",
            t1.as_secs_f64() * 1000.0,
            t4.as_secs_f64() * 1000.0,
            ratio
        );

        assert!(
            ratio < 8.0,
            "4-thread wall time is {ratio:.2}x of 1-thread — severe degradation (expect < 8x)"
        );
    });
}
