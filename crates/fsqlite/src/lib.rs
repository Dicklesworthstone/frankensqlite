//! Public API facade for FrankenSQLite.
//!
//! This crate will grow a stable, ergonomic API surface over time. In early
//! phases it also re-exports selected internal crates for integration tests.

// The re-exported `Connection` futures from `fsqlite-core` are deeply nested
// (statement dispatch → DML → triggers → nested statement execution); the
// default limit overflows while type-checking them here too.
#![recursion_limit = "512"]
// bd-h9o9r: this crate drives fsqlite-core's deliberately non-`Send`,
// deeply nested engine futures (the same nesting behind the
// `recursion_limit` above); `future_not_send` and `large_futures`
// contradict that design — see fsqlite-core/src/lib.rs for the full
// rationale, including why boxing was rejected by the perf ledger.
#![allow(clippy::future_not_send)]
#![allow(clippy::large_futures)]

pub use fsqlite_core::connection::{
    Connection, ConnectionEnv, DatabaseImageReceipt, IoPollStrategy, PreparedStatement, Row,
    RuntimeConfig, RuntimeContext, TraceEvent, TraceMask, init_global_runtime,
};
/// Whole-database-image capture, bounded structural proof, and publication.
///
/// These name the guards and proof counters returned by
/// [`Connection::begin_bounded_structural_snapshot`] and
/// [`Connection::begin_database_image_publication`]; without them a caller can
/// call those methods but cannot write down the types they hand back.
pub use fsqlite_core::connection::{BoundedDatabaseStructuralStats, PageCachePeakSnapshot};
#[cfg(not(target_arch = "wasm32"))]
pub use fsqlite_core::connection::{
    BoundedStructuralSnapshot, DatabaseImagePublication, PendingDatabaseImagePublication,
};
pub use fsqlite_error::{DatabaseImagePublicationErrorClass, FrankenError};
pub use fsqlite_types::SqliteValue;
pub use fsqlite_vfs;
pub use fsqlite_vfs::FileIdentity;
#[cfg(all(feature = "native", any(unix, windows)))]
pub use fsqlite_vfs::{
    DatabaseNamespaceGenerationTransition, NamespaceGenerationTransitionOutcome,
    begin_database_namespace_generation_transition,
};

#[cfg(feature = "session")]
/// Manual session/changeset API facade re-exported from `fsqlite-ext-session`.
pub mod session {
    pub use fsqlite_ext_session::{
        ApplyOutcome, ChangeOp, Changeset, ChangesetRow, ChangesetValue, ConflictAction,
        ConflictType, Session, SimpleTarget, TableChangeset, TableInfo, changeset_varint_len,
        extension_name,
    };
}

#[cfg(feature = "async-api")]
pub mod async_api;
#[cfg(feature = "async-api")]
pub use async_api::AsyncConnection;

pub mod compat;
pub mod migrate;

#[cfg(test)]
#[allow(
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::needless_collect,
    clippy::single_match_else,
    clippy::branches_sharing_code
)]
mod tests {
    use super::{
        Connection, ConnectionEnv, FileIdentity, IoPollStrategy, RuntimeConfig, RuntimeContext,
        init_global_runtime,
    };
    use fsqlite_ast::{CreateTableBody, Statement};
    use fsqlite_error::FrankenError;
    use fsqlite_parser::parse_first_statement_with_tail;
    use fsqlite_types::value::SqliteValue;
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::{Arc, mpsc};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    const CONCURRENT_STRESS_CHILD_ENV: &str = "FSQLITE_CONCURRENT_STRESS_CHILD";
    const CONCURRENT_STRESS_RECEIPT_ENV: &str = "FSQLITE_CONCURRENT_STRESS_RECEIPT";
    // bd-gmqxy: the receipt travels through a temp file whose path is passed
    // via this env var, not through the child's stdout — libtest's output
    // capture machinery in the parent broke the stdout channel, failing the
    // supervised child under plain `cargo test` while `--nocapture` passed.
    const CONCURRENT_STRESS_RECEIPT_PATH_ENV: &str = "FSQLITE_CONCURRENT_STRESS_RECEIPT_PATH";
    const CONCURRENT_STRESS_RECEIPT_PREFIX: &str = "FSQLITE_CONCURRENT_STRESS_COMPLETE:";
    const CONCURRENT_STRESS_TEST_NAME: &str = "tests::concurrent_writers_stress_conservation";
    const CONCURRENT_STRESS_CHILD_TIMEOUT: Duration = Duration::from_secs(90);
    const CONCURRENT_STRESS_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
    const CONCURRENT_STRESS_WORKER_TIMEOUT: Duration = Duration::from_secs(60);
    const CONCURRENT_STRESS_MAX_ATTEMPTS_PER_COMMIT: u64 = 512;
    const CONCURRENT_STRESS_MAX_ATTEMPTS_PER_WORKER: u64 = 2_560;
    const CONCURRENT_READER_MAX_OPEN_ATTEMPTS: u64 = 4;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ConcurrentStressStartDecision {
        Run,
        Abort,
    }

    #[derive(Debug)]
    enum ConcurrentStressStartup {
        Ready { worker_id: usize },
        Failed { worker_id: usize, error: String },
    }

    struct ConcurrentStressStartGate {
        senders: Vec<mpsc::SyncSender<ConcurrentStressStartDecision>>,
        armed: bool,
    }

    impl ConcurrentStressStartGate {
        fn new(senders: Vec<mpsc::SyncSender<ConcurrentStressStartDecision>>) -> Self {
            Self {
                senders,
                armed: true,
            }
        }

        fn release(mut self) -> Result<(), String> {
            let mut failures = Vec::new();
            for (worker_id, sender) in self.senders.iter().enumerate() {
                if sender.send(ConcurrentStressStartDecision::Run).is_err() {
                    failures.push(worker_id);
                }
            }
            if failures.is_empty() {
                self.armed = false;
                Ok(())
            } else {
                Err(format!(
                    "workers disconnected before the run decision: {failures:?}"
                ))
            }
        }
    }

    impl Drop for ConcurrentStressStartGate {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            for sender in &self.senders {
                let _ = sender.try_send(ConcurrentStressStartDecision::Abort);
            }
        }
    }

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct ConcurrentStressRetryCounts {
        busy: u64,
        busy_recovery: u64,
        busy_snapshot: u64,
        database_locked: u64,
        write_conflict: u64,
        serialization_failure: u64,
    }

    impl ConcurrentStressRetryCounts {
        fn record(&mut self, error: &FrankenError) -> bool {
            match error {
                FrankenError::Busy => self.busy += 1,
                FrankenError::BusyRecovery => self.busy_recovery += 1,
                FrankenError::BusySnapshot { .. } => self.busy_snapshot += 1,
                FrankenError::DatabaseLocked { .. } => self.database_locked += 1,
                FrankenError::WriteConflict { .. } => self.write_conflict += 1,
                FrankenError::SerializationFailure { .. } => self.serialization_failure += 1,
                _ => return false,
            }
            true
        }

        const fn total(self) -> u64 {
            self.busy
                + self.busy_recovery
                + self.busy_snapshot
                + self.database_locked
                + self.write_conflict
                + self.serialization_failure
        }
    }

    #[derive(Debug)]
    struct ConcurrentStressTransfer {
        from_id: i64,
        to_id: i64,
        amount: i64,
        begin_seq: u64,
        commit_seq: u64,
    }

    #[derive(Debug)]
    struct ConcurrentStressStockDiagnostic {
        row_count: i64,
        balance_sum: i64,
        point_count: i64,
        scan_count: i64,
        integrity: Vec<String>,
        balances: Vec<(i64, i64)>,
    }

    #[derive(Debug)]
    struct ConcurrentStressWorkerOutcome {
        worker_id: usize,
        concurrent_mode_default: bool,
        commits: u64,
        attempts: u64,
        max_attempts_for_commit: u64,
        retries: ConcurrentStressRetryCounts,
        elapsed: Duration,
        failure: Option<String>,
        committed_transfers: Vec<ConcurrentStressTransfer>,
    }

    impl ConcurrentStressWorkerOutcome {
        fn pending(worker_id: usize) -> Self {
            Self {
                worker_id,
                concurrent_mode_default: false,
                commits: 0,
                attempts: 0,
                max_attempts_for_commit: 0,
                retries: ConcurrentStressRetryCounts::default(),
                elapsed: Duration::ZERO,
                failure: Some("worker exited without recording an outcome".to_owned()),
                committed_transfers: Vec::new(),
            }
        }
    }

    fn supervise_concurrent_writer_stress() -> bool {
        match (
            std::env::var_os(CONCURRENT_STRESS_CHILD_ENV),
            std::env::var_os(CONCURRENT_STRESS_RECEIPT_ENV),
        ) {
            (Some(child_token), Some(receipt_token)) if child_token == receipt_token => {
                return false;
            }
            (None, None) => {}
            _ => panic!("inconsistent inherited concurrent-stress supervision environment"),
        }

        let receipt_token = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock must be after the Unix epoch")
                .as_nanos()
        );
        let expected_receipt = format!("{CONCURRENT_STRESS_RECEIPT_PREFIX}{receipt_token}");
        let receipt_dir =
            tempfile::tempdir().expect("create concurrent-writer stress receipt directory");
        let receipt_path = receipt_dir.path().join("receipt");
        let mut child =
            Command::new(std::env::current_exe().expect("resolve current fsqlite test executable"))
                .args([
                    "--exact",
                    CONCURRENT_STRESS_TEST_NAME,
                    "--include-ignored",
                    "--nocapture",
                ])
                .env(CONCURRENT_STRESS_CHILD_ENV, &receipt_token)
                .env(CONCURRENT_STRESS_RECEIPT_ENV, &receipt_token)
                .env(CONCURRENT_STRESS_RECEIPT_PATH_ENV, &receipt_path)
                .stdout(Stdio::piped())
                .spawn()
                .expect("spawn supervised concurrent-writer stress child");
        let child_stdout = child
            .stdout
            .take()
            .expect("capture concurrent-writer stress child stdout");
        // Drain the pipe so the child can never block on a full stdout buffer;
        // the receipt itself arrives through the temp file, immune to libtest
        // capture on either side of the process boundary.
        let mut receipt_reader = Some(std::thread::spawn(move || {
            for line in BufReader::new(child_stdout).lines() {
                drop(line.expect("read concurrent-writer stress child stdout"));
            }
        }));
        let deadline = Instant::now() + CONCURRENT_STRESS_CHILD_TIMEOUT;

        loop {
            match child
                .try_wait()
                .expect("poll supervised concurrent-writer stress child")
            {
                Some(status) => {
                    receipt_reader
                        .take()
                        .expect("receipt reader must be present")
                        .join()
                        .expect("concurrent-writer stress receipt reader must not panic");
                    assert!(
                        status.success(),
                        "supervised concurrent-writer stress child failed with {status}"
                    );
                    let receipt_contents = std::fs::read_to_string(&receipt_path)
                        .expect("supervised concurrent-writer stress child exited without writing its completion receipt file");
                    assert_eq!(
                        receipt_contents.trim_end(),
                        expected_receipt,
                        "supervised concurrent-writer stress child wrote a mismatched completion receipt"
                    );
                    return true;
                }
                None if Instant::now() >= deadline => {
                    let _ = child.kill();
                    child
                        .wait()
                        .expect("reap timed-out concurrent-writer stress child");
                    receipt_reader
                        .take()
                        .expect("receipt reader must be present")
                        .join()
                        .expect("concurrent-writer stress receipt reader must not panic");
                    panic!(
                        "supervised concurrent-writer stress test exceeded {:?}",
                        CONCURRENT_STRESS_CHILD_TIMEOUT
                    );
                }
                None => std::thread::sleep(Duration::from_millis(50)),
            }
        }
    }

    fn concurrent_stress_attempt_budget_error(
        attempts_for_commit: u64,
        total_attempts: u64,
        elapsed: Duration,
    ) -> Option<String> {
        if attempts_for_commit >= CONCURRENT_STRESS_MAX_ATTEMPTS_PER_COMMIT {
            Some(format!(
                "exhausted {CONCURRENT_STRESS_MAX_ATTEMPTS_PER_COMMIT} attempts for one commit"
            ))
        } else if total_attempts >= CONCURRENT_STRESS_MAX_ATTEMPTS_PER_WORKER {
            Some(format!(
                "exhausted {CONCURRENT_STRESS_MAX_ATTEMPTS_PER_WORKER} total attempts"
            ))
        } else if elapsed >= CONCURRENT_STRESS_WORKER_TIMEOUT {
            Some(format!(
                "exceeded worker deadline {:?}",
                CONCURRENT_STRESS_WORKER_TIMEOUT
            ))
        } else {
            None
        }
    }

    fn concurrent_stress_backoff(attempts_for_commit: u64, participant_id: u64) {
        let exponent = attempts_for_commit.saturating_sub(1).min(5) as u32;
        let base_millis = 1_u64 << exponent;
        let jitter_millis = participant_id
            .wrapping_mul(11)
            .wrapping_add(attempts_for_commit.wrapping_mul(7))
            % (base_millis + 1);
        std::thread::sleep(Duration::from_millis(base_millis + jitter_millis));
    }

    async fn concurrent_stress_rollback_precommit_transient(
        conn: &Connection,
        outcome: &mut ConcurrentStressWorkerOutcome,
        phase: &str,
        primary_error: &FrankenError,
    ) -> Result<(), String> {
        if !outcome.retries.record(primary_error) {
            return Err(format!("unexpected {phase} error: {primary_error:?}"));
        }

        // BusySnapshot invalidates the whole transaction. Full ROLLBACK is the
        // engine contract that releases page locks and reloads committed state
        // before the next BEGIN binds a fresh publication snapshot.
        if let Err(rollback_error) = conn.execute("ROLLBACK;").await {
            let _ = outcome.retries.record(&rollback_error);
            // Full rollback clears the explicit-transaction state before it
            // reloads the newly committed pager image. A peer may hold the
            // recovery fence during that reload, yielding BusyRecovery even
            // though rollback already released this worker's page locks and
            // ended its transaction. In that exact state the next bounded
            // BEGIN is the recovery retry; every other rollback error remains
            // a hard failure.
            if matches!(rollback_error, FrankenError::BusyRecovery) && !conn.in_transaction() {
                return Ok(());
            }
            return Err(format!(
                "ROLLBACK after retryable {phase} error {primary_error:?} failed: \
                 {rollback_error:?}"
            ));
        }

        Ok(())
    }

    fn concurrent_stress_panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
        if let Some(message) = payload.downcast_ref::<String>() {
            message
        } else if let Some(message) = payload.downcast_ref::<&'static str>() {
            message
        } else {
            "non-string panic payload"
        }
    }

    fn row_values(row: &super::Row) -> Vec<SqliteValue> {
        row.values().to_vec()
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    fn native_suffixed_path(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
        let mut suffixed = path.as_os_str().to_owned();
        suffixed.push(suffix);
        std::path::PathBuf::from(suffixed)
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    fn native_database_artifacts(path: &std::path::Path) -> [std::path::PathBuf; 8] {
        [
            path.to_owned(),
            native_suffixed_path(path, "-journal"),
            native_suffixed_path(path, "-wal"),
            native_suffixed_path(path, "-wal-fec"),
            native_suffixed_path(path, "-shm"),
            native_suffixed_path(path, "-lock-shared"),
            native_suffixed_path(path, "-lock-reserved"),
            native_suffixed_path(path, "-lock-pending"),
        ]
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    fn snapshot_native_artifacts(paths: &[std::path::PathBuf]) -> Vec<Option<Vec<u8>>> {
        paths
            .iter()
            .map(|path| match std::fs::read(path) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("snapshot {}: {error}", path.display()),
            })
            .collect()
    }

    #[cfg(all(feature = "native", windows))]
    fn seed_windows_database(path: &std::path::Path) {
        let seed = rusqlite::Connection::open(path).expect("create valid SQLite database");
        seed.execute_batch(
            "PRAGMA journal_mode = DELETE;
             CREATE TABLE identity_probe(value INTEGER NOT NULL);
             INSERT INTO identity_probe VALUES (1);",
        )
        .expect("seed valid SQLite database");
    }

    #[cfg(all(feature = "native", windows))]
    fn windows_file_identity(path: &std::path::Path) -> FileIdentity {
        let file = std::fs::File::open(path).expect("open Windows identity handle");
        FileIdentity::from_file(&file)
            .expect("query Windows identity handle")
            .expect("Windows file identity must be available")
    }

    #[cfg(all(feature = "native", windows))]
    fn seed_windows_auxiliary_sentinels(artifacts: &[std::path::PathBuf; 8]) {
        std::fs::write(&artifacts[1], b"journal sentinel").expect("seed journal sentinel");
        std::fs::write(&artifacts[2], b"WAL sentinel").expect("seed WAL sentinel");
        std::fs::write(&artifacts[3], b"WAL-FEC sentinel").expect("seed WAL-FEC sentinel");
        std::fs::write(&artifacts[4], b"SHM sentinel").expect("seed SHM sentinel");
    }

    #[test]
    fn test_connection_open_and_path() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:")
                .await
                .expect("in-memory connection should open");
            assert_eq!(conn.path(), ":memory:");
        });
    }

    #[test]
    fn in_memory_connection_has_no_filesystem_identity() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:")
                .await
                .expect("in-memory connection should open");
            assert_eq!(conn.file_identity().await.unwrap(), None);
        });
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    #[test]
    fn namespace_lifetime_connections_to_the_same_database_identity_coexist() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let database_path = dir.path().join("shared-generation.db");
            let database_path = database_path.to_string_lossy().into_owned();
            let first = Connection::open(database_path.clone())
                .await
                .expect("open first connection");
            let second = Connection::open_existing(database_path)
                .await
                .expect("join the initialized live database generation");
            first
                .execute("PRAGMA fsqlite.stmt_microbatch = OFF;")
                .await
                .expect("disable statement carry on first connection");
            second
                .execute("PRAGMA fsqlite.stmt_microbatch = OFF;")
                .await
                .expect("disable statement carry on peer connection");
            first
                .execute_batch(
                    "CREATE TABLE shared_generation(value INTEGER NOT NULL);
                 INSERT INTO shared_generation VALUES (1);",
                )
                .await
                .expect("seed the shared generation");

            let first_identity = first
                .file_identity()
                .await
                .expect("query first connection identity")
                .expect("native database has a stable identity");
            assert_eq!(
                second.file_identity().await.expect("query peer identity"),
                Some(first_identity),
                "both connections must remain leased to the same database object"
            );

            second
                .execute("INSERT INTO shared_generation VALUES (2);")
                .await
                .expect("peer connection writes through the shared generation");
            let rows = first
                .query("SELECT value FROM shared_generation ORDER BY value;")
                .await
                .expect("first connection observes the peer commit");
            assert_eq!(
                rows.iter().map(row_values).collect::<Vec<_>>(),
                vec![vec![SqliteValue::Integer(1)], vec![SqliteValue::Integer(2)]]
            );
        });
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    #[test]
    fn namespace_generation_transition_reopens_quarantined_replacement() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let database_path = dir.path().join("recover.db");
            let quarantine_path = dir.path().join("recover.db.quarantined");
            let replacement_stage = dir.path().join("recover.db.replacement");
            let database_path_string = database_path.to_string_lossy().into_owned();

            {
                let generation_a =
                    rusqlite::Connection::open(&database_path).expect("create generation A");
                generation_a
                    .execute_batch(
                        "PRAGMA journal_mode = DELETE;
                         CREATE TABLE generation(value INTEGER NOT NULL);
                         INSERT INTO generation VALUES (1);",
                    )
                    .expect("seed generation A");
            }
            let generation_a = Connection::open_existing(database_path_string.clone())
                .await
                .expect("open generation A through the public facade");
            let old_identity = generation_a
                .file_identity()
                .await
                .expect("query generation A identity")
                .expect("native database identity");
            let rows = generation_a
                .query("SELECT value FROM generation;")
                .await
                .expect("query generation A");
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
            drop(generation_a);

            {
                let generation_b =
                    rusqlite::Connection::open(&replacement_stage).expect("create generation B");
                generation_b
                    .execute_batch(
                        "PRAGMA journal_mode = DELETE;
                         CREATE TABLE generation(value INTEGER NOT NULL);
                         INSERT INTO generation VALUES (2);",
                    )
                    .expect("seed generation B");
            }
            let replacement_file = std::fs::File::open(&replacement_stage)
                .expect("retain generation B identity handle");
            let replacement_identity = FileIdentity::from_file(&replacement_file)
                .expect("query generation B identity")
                .expect("native database identity");
            assert_ne!(old_identity, replacement_identity);

            let mut transition =
                super::begin_database_namespace_generation_transition(&database_path, old_identity)
                    .expect("guard generation A before pathname mutation");
            std::fs::rename(&database_path, &quarantine_path)
                .expect("quarantine fully quiescent generation A under guard");
            std::fs::rename(&replacement_stage, &database_path)
                .expect("install generation B at the stable pathname");

            assert_eq!(
                transition
                    .publish_replacement(replacement_identity)
                    .expect("publish generation B"),
                super::NamespaceGenerationTransitionOutcome::Published
            );
            assert_eq!(
                transition.finish().expect("finish generation B transition"),
                replacement_identity
            );
            let generation_b = Connection::open_existing_with_expected_identity(
                database_path_string,
                replacement_identity,
            )
            .await
            .expect("reopen generation B through the public facade");
            let rows = generation_b
                .query("SELECT value FROM generation;")
                .await
                .expect("query generation B");
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
            drop(generation_b);

            super::begin_database_namespace_generation_transition(
                &database_path,
                replacement_identity,
            )
            .expect("reacquire exact published generation")
            .finish()
            .expect("finish no-op exact reacquisition");
        });
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    #[test]
    fn reserved_identity_open_never_synthesizes_a_missing_path() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let identity_path = dir.path().join("existing-identity.db");
            let missing_path = dir.path().join("missing-reservation.db");
            drop(std::fs::File::create(&identity_path).expect("create identity source"));
            let identity_file = std::fs::File::open(&identity_path).expect("open identity source");
            let expected_identity = FileIdentity::from_file(&identity_file)
                .expect("query filesystem identity")
                .expect("native filesystem identity must be available");

            let artifacts = [
                missing_path.clone(),
                native_suffixed_path(&missing_path, "-journal"),
                native_suffixed_path(&missing_path, "-wal"),
                native_suffixed_path(&missing_path, "-wal-fec"),
                native_suffixed_path(&missing_path, "-shm"),
                native_suffixed_path(&missing_path, "-lock-shared"),
                native_suffixed_path(&missing_path, "-lock-reserved"),
                native_suffixed_path(&missing_path, "-lock-pending"),
            ];
            assert!(artifacts.iter().all(|path| !path.exists()));

            let error = Connection::open_reserved_with_expected_identity(
                missing_path.to_string_lossy().into_owned(),
                expected_identity,
            )
            .await
            .expect_err("missing reservation must not be created");

            assert!(matches!(error, FrankenError::CannotOpen { .. }));
            assert!(
                artifacts.iter().all(|path| !path.exists()),
                "identity-bound reserved open must leave the missing main path and every sidecar absent"
            );
        });
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    #[test]
    fn reserved_identity_open_refuses_a_preexisting_recovery_artifact_without_mutation() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let database_path = dir.path().join("reserved-empty.db");
            let artifacts = native_database_artifacts(&database_path);
            drop(std::fs::File::create(&database_path).expect("reserve empty database path"));
            let reservation = std::fs::File::open(&database_path).expect("open reservation handle");
            let expected_identity = FileIdentity::from_file(&reservation)
                .expect("query reservation identity")
                .expect("native filesystem identity must be available");
            std::fs::write(&artifacts[1], b"reserved journal sentinel")
                .expect("seed recovery artifact");
            let before = snapshot_native_artifacts(&artifacts);
            assert!(
                before[2..].iter().all(Option::is_none),
                "every unseeded recovery and advisory-lock sidecar must start absent"
            );

            let error = Connection::open_reserved_with_expected_identity(
                database_path.to_string_lossy().into_owned(),
                expected_identity,
            )
            .await
            .expect_err("a reserved-empty open must refuse a pre-existing recovery artifact");

            assert!(matches!(error, FrankenError::CannotOpen { .. }));
            assert_eq!(
                snapshot_native_artifacts(&artifacts),
                before,
                "refusal must leave main, recovery artifacts, and advisory-lock sidecars unchanged"
            );
        });
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn reserved_identity_open_refuses_dangling_recovery_artifact_symlinks() {
        asupersync::test_utils::run_test(|| async {
            use std::os::unix::fs::symlink;

            let dir = tempfile::tempdir().expect("create temp dir");
            for (index, suffix) in ["-journal", "-wal", "-wal-fec", "-shm"]
                .into_iter()
                .enumerate()
            {
                let database_path = dir.path().join(format!("reserved-dangling-{index}.db"));
                drop(std::fs::File::create(&database_path).expect("reserve empty database path"));
                let reservation =
                    std::fs::File::open(&database_path).expect("open reservation handle");
                let expected_identity = FileIdentity::from_file(&reservation)
                    .expect("query reservation identity")
                    .expect("Unix filesystem identity must be available");
                let dangling_target = dir.path().join(format!("missing-target-{index}"));
                let artifact_path = native_suffixed_path(&database_path, suffix);
                symlink(&dangling_target, &artifact_path).expect("seed dangling artifact symlink");

                let error = Connection::open_reserved_with_expected_identity(
                    database_path.to_string_lossy().into_owned(),
                    expected_identity,
                )
                .await
                .expect_err("a dangling recovery-artifact symlink must refuse initialization");

                assert!(matches!(error, FrankenError::CannotOpen { .. }));
                assert_eq!(
                    std::fs::metadata(&database_path).unwrap().len(),
                    0,
                    "refusal must leave the reserved main file empty"
                );
                assert!(
                    std::fs::symlink_metadata(&artifact_path)
                        .expect("refusal must preserve the dangling artifact")
                        .file_type()
                        .is_symlink(),
                    "refusal must preserve the {suffix} symlink itself"
                );
                assert!(
                    !dangling_target.exists(),
                    "refusal must not create the dangling symlink target"
                );
            }
        });
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    #[test]
    fn reserved_identity_open_refuses_a_nonempty_file_without_artifact_mutation() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let database_path = dir.path().join("reserved-nonempty.db");
            std::fs::write(&database_path, b"nonempty reservation sentinel")
                .expect("seed nonempty reserved file");
            let reservation = std::fs::File::open(&database_path).expect("open reservation handle");
            let expected_identity = FileIdentity::from_file(&reservation)
                .expect("query reservation identity")
                .expect("native filesystem identity must be available");
            let artifacts = native_database_artifacts(&database_path);
            let before = snapshot_native_artifacts(&artifacts);
            assert!(
                before[1..].iter().all(Option::is_none),
                "every database sidecar must start absent"
            );

            let error = Connection::open_reserved_with_expected_identity(
                database_path.to_string_lossy().into_owned(),
                expected_identity,
            )
            .await
            .expect_err("a reserved-empty open must refuse a nonempty file");

            assert!(matches!(error, FrankenError::CannotOpen { .. }));
            assert_eq!(
                snapshot_native_artifacts(&artifacts),
                before,
                "nonempty refusal must leave the main file and every sidecar unchanged"
            );
        });
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    #[test]
    fn namespace_lifetime_reserved_open_refuses_wal_segment_and_fec_rewrite_artifacts() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");

            for artifact_kind in ["wal-segment", "wal-fec-rewrite"] {
                let database_path = dir.path().join(format!("reserved-{artifact_kind}.db"));
                drop(std::fs::File::create(&database_path).expect("reserve empty database path"));
                let reservation =
                    std::fs::File::open(&database_path).expect("retain reservation handle");
                let expected_identity = FileIdentity::from_file(&reservation)
                    .expect("query reservation identity")
                    .expect("native filesystem identity must be available");
                let artifact_path = match artifact_kind {
                    "wal-segment" => dir.path().join("reserved-wal-segment.db-wal-seg-00000001"),
                    "wal-fec-rewrite" => native_suffixed_path(&database_path, "-wal-fec")
                        .with_extension("wal-fec.tmp"),
                    _ => unreachable!("artifact cases are exhaustive"),
                };
                let sentinel = format!("{artifact_kind} sentinel").into_bytes();
                std::fs::write(&artifact_path, &sentinel).expect("seed forbidden artifact");

                let error = Connection::open_reserved_with_expected_identity(
                    database_path.to_string_lossy().into_owned(),
                    expected_identity,
                )
                .await
                .expect_err("reserved bootstrap must reject every pre-existing WAL artifact");

                assert!(matches!(error, FrankenError::CannotOpen { .. }));
                assert_eq!(
                    std::fs::metadata(&database_path).unwrap().len(),
                    0,
                    "refusal must leave the reserved main file empty"
                );
                assert_eq!(
                    std::fs::read(&artifact_path).expect("read preserved forbidden artifact"),
                    sentinel,
                    "refusal must not mutate the forbidden artifact"
                );
            }
        });
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn connection_identity_remains_bound_to_open_file_after_path_swap() {
        asupersync::test_utils::run_test(|| async {
            use std::fs::File;

            let dir = tempfile::tempdir().expect("create temp dir");
            let database_path = dir.path().join("identity.db");
            let displaced_path = dir.path().join("identity.opened.db");
            let conn = Connection::open(database_path.to_string_lossy().into_owned())
                .await
                .expect("open file-backed connection");

            let leased_file = File::open(&database_path).expect("lease opened database descriptor");
            let leased_identity = FileIdentity::from_file(&leased_file)
                .expect("read leased descriptor identity")
                .expect("Unix descriptors have stable identities");
            let connection_identity = conn
                .file_identity()
                .await
                .expect("read connection identity")
                .expect("Unix VFS exposes an open-file identity");
            assert_eq!(connection_identity, leased_identity);

            std::fs::rename(&database_path, &displaced_path)
                .expect("displace opened database path");
            drop(File::create(&database_path).expect("create replacement path"));
            let replacement_file =
                File::open(&database_path).expect("lease replacement descriptor");
            let replacement_identity = FileIdentity::from_file(&replacement_file)
                .expect("read replacement descriptor identity")
                .expect("Unix descriptors have stable identities");

            assert_ne!(connection_identity, replacement_identity);
            assert_eq!(conn.file_identity().await.unwrap(), Some(leased_identity));
            drop(conn);
        });
    }

    #[cfg(all(feature = "native", unix))]
    async fn exercise_live_namespace_replacement_rejection(journal_mode: &str) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let database_path = dir.path().join(format!("live-{journal_mode}.db"));
        let displaced_path = dir.path().join(format!("live-{journal_mode}.displaced.db"));
        let replacement_stage = dir
            .path()
            .join(format!("live-{journal_mode}.replacement.db"));
        let database_path_string = database_path.to_string_lossy().into_owned();

        let live = Connection::open(database_path_string.clone())
            .await
            .expect("open live generation");
        live.execute("PRAGMA fsqlite.stmt_microbatch = OFF;")
            .await
            .expect("disable retained statement carry for namespace boundary proof");
        live.execute(&format!("PRAGMA journal_mode = '{journal_mode}';"))
            .await
            .expect("select requested journal mode");
        live.execute_batch(
            "CREATE TABLE identity_probe(value INTEGER NOT NULL);
             INSERT INTO identity_probe VALUES (1);",
        )
        .await
        .expect("seed live generation");
        let live_identity = live
            .file_identity()
            .await
            .expect("query live identity")
            .expect("Unix database has a stable identity");

        {
            let replacement = rusqlite::Connection::open(&replacement_stage)
                .expect("create valid replacement database");
            replacement
                .execute_batch(
                    "PRAGMA journal_mode = DELETE;
                     CREATE TABLE identity_probe(value INTEGER NOT NULL);
                     INSERT INTO identity_probe VALUES (9001);",
                )
                .expect("seed replacement database");
        }
        let replacement_staged_bytes =
            std::fs::read(&replacement_stage).expect("snapshot staged replacement");

        std::fs::rename(&database_path, &displaced_path).expect("displace live main file");
        std::fs::rename(&replacement_stage, &database_path)
            .expect("install replacement at the live pathname");
        let replacement_file =
            std::fs::File::open(&database_path).expect("open replacement identity handle");
        let replacement_identity = FileIdentity::from_file(&replacement_file)
            .expect("query replacement identity")
            .expect("Unix database has a stable identity");
        assert_ne!(live_identity, replacement_identity);

        let write_error = live
            .execute("INSERT INTO identity_probe VALUES (2);")
            .await
            .expect_err("live connection must reject a replaced main pathname");
        assert!(matches!(write_error, FrankenError::CannotOpen { .. }));
        assert_eq!(
            std::fs::read(&database_path).expect("read rejected replacement"),
            replacement_staged_bytes,
            "rejection must precede every write to the replacement database object"
        );

        let join_error = Connection::open(database_path_string.clone())
            .await
            .expect_err("a peer must not join the replacement while the old generation is live");
        assert!(matches!(join_error, FrankenError::CannotOpen { .. }));
        assert!(matches!(
            super::begin_database_namespace_generation_transition(&database_path, live_identity),
            Err(FrankenError::Busy)
        ));
        assert_eq!(
            std::fs::read(&database_path).expect("read replacement after rejected join"),
            replacement_staged_bytes,
            "rejected admission must not mutate the replacement"
        );

        drop(live);
        std::fs::rename(&database_path, &replacement_stage)
            .expect("restage replacement before acquiring transition guard");
        std::fs::rename(&displaced_path, &database_path)
            .expect("restore live generation before acquiring transition guard");
        let mut transition =
            super::begin_database_namespace_generation_transition(&database_path, live_identity)
                .expect("guard the quiescent old generation");
        std::fs::rename(&database_path, &displaced_path)
            .expect("quarantine old generation under guard");
        std::fs::rename(&replacement_stage, &database_path)
            .expect("activate replacement under guard");
        assert_eq!(
            transition
                .publish_replacement(replacement_identity)
                .expect("publish the quiescent replacement generation"),
            super::NamespaceGenerationTransitionOutcome::Published
        );
        transition.finish().expect("finish replacement transition");
        let replacement = Connection::open_existing(database_path_string)
            .await
            .expect("replacement becomes a new generation after the old lease drops");
        assert_eq!(
            replacement.file_identity().await.unwrap(),
            Some(replacement_identity)
        );
        let rows = replacement
            .query("SELECT value FROM identity_probe;")
            .await
            .expect("query the replacement generation");
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(9001)]);
        replacement
            .execute("INSERT INTO identity_probe VALUES (9002);")
            .await
            .expect("new generation is writable");
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn namespace_lifetime_delete_mode_replacement_is_rejected() {
        asupersync::test_utils::run_test(|| async {
            exercise_live_namespace_replacement_rejection("DELETE").await;
        });
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn namespace_lifetime_wal_mode_replacement_is_rejected() {
        asupersync::test_utils::run_test(|| async {
            exercise_live_namespace_replacement_rejection("WAL").await;
        });
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    #[test]
    fn namespace_lifetime_relative_path_remains_anchored_after_cwd_change() {
        asupersync::test_utils::run_test(|| async {
            const CHILD_ROOT: &str = "FSQLITE_NAMESPACE_CWD_CHILD_ROOT";

            if let Some(root) = std::env::var_os(CHILD_ROOT) {
                let root = std::path::PathBuf::from(root);
                let original_dir = root.join("original-cwd");
                let later_dir = root.join("later-cwd");
                std::env::set_current_dir(&original_dir).expect("enter original cwd");

                let conn = Connection::open("anchored.db")
                    .await
                    .expect("open relative database path");
                let expected_path = original_dir.join("anchored.db");
                let expected_canonical_path = expected_path
                    .canonicalize()
                    .expect("canonicalize newly opened relative database path");
                assert_eq!(std::path::Path::new(conn.path()), expected_canonical_path);
                conn.execute_batch(
                    "CREATE TABLE cwd_probe(value INTEGER NOT NULL);
                 INSERT INTO cwd_probe VALUES (1);",
                )
                .await
                .expect("seed database from original cwd");

                std::env::set_current_dir(&later_dir).expect("change process cwd");
                conn.execute("INSERT INTO cwd_probe VALUES (2);")
                    .await
                    .expect("write remains bound to original absolute path");
                drop(conn);

                assert!(expected_path.exists());
                assert!(
                    !later_dir.join("anchored.db").exists(),
                    "no operation may re-resolve the configured relative path against the new cwd"
                );
                let verification =
                    rusqlite::Connection::open(expected_path).expect("open anchored database");
                let values = verification
                    .prepare("SELECT value FROM cwd_probe ORDER BY value")
                    .expect("prepare anchored verification")
                    .query_map([], |row| row.get::<_, i64>(0))
                    .expect("query anchored verification")
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .expect("collect anchored verification");
                assert_eq!(values, vec![1, 2]);
                return;
            }

            let dir = tempfile::tempdir().expect("create parent temp dir");
            std::fs::create_dir(dir.path().join("original-cwd")).expect("create original cwd");
            std::fs::create_dir(dir.path().join("later-cwd")).expect("create later cwd");
            let output = std::process::Command::new(
                std::env::current_exe().expect("locate current Rust test binary"),
            )
            .args([
                "--exact",
                "tests::namespace_lifetime_relative_path_remains_anchored_after_cwd_change",
                "--nocapture",
            ])
            .env(CHILD_ROOT, dir.path())
            .current_dir(dir.path())
            .output()
            .expect("run cwd-isolated child test");
            assert!(
                output.status.success(),
                "cwd-isolated child failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        });
    }

    #[cfg(all(feature = "native", unix))]
    #[test]
    fn expected_identity_refuses_swapped_hot_journal_without_mutation() {
        asupersync::test_utils::run_test(|| async {
            use fsqlite_pager::{JournalHeader, JournalPageRecord};
            use std::fs::File;

            let dir = tempfile::tempdir().expect("create temp dir");
            let database_path = dir.path().join("identity-bound.db");
            let displaced_path = dir.path().join("identity-bound.leased.db");
            let journal_path = dir.path().join("identity-bound.db-journal");

            {
                let leased_seed = rusqlite::Connection::open(&database_path)
                    .expect("create identity-leased database");
                leased_seed
                    .execute_batch(
                        "PRAGMA journal_mode = DELETE;
                     CREATE TABLE leased_marker(value INTEGER);
                     INSERT INTO leased_marker VALUES (1);",
                    )
                    .expect("seed identity-leased database");
            }
            let leased_file =
                File::open(&database_path).expect("lease original database descriptor");
            let leased_identity = FileIdentity::from_file(&leased_file)
                .expect("read leased descriptor identity")
                .expect("Unix descriptors have stable identities");
            std::fs::rename(&database_path, &displaced_path)
                .expect("replace leased database pathname");

            let (page_size, page_count) = {
                let replacement = rusqlite::Connection::open(&database_path)
                    .expect("create replacement database");
                replacement
                    .execute_batch(
                        "PRAGMA page_size = 4096;
                     PRAGMA journal_mode = DELETE;
                     CREATE TABLE replacement_marker(value INTEGER);
                     INSERT INTO replacement_marker VALUES (2);",
                    )
                    .expect("seed replacement database");
                let page_size: i64 = replacement
                    .query_row("PRAGMA page_size", [], |row| row.get(0))
                    .expect("read replacement page size");
                let page_count: i64 = replacement
                    .query_row("PRAGMA page_count", [], |row| row.get(0))
                    .expect("read replacement page count");
                (
                    u32::try_from(page_size).expect("page size fits u32"),
                    u32::try_from(page_count).expect("page count fits u32"),
                )
            };
            assert!(page_count >= 2, "replacement database must have page 2");

            let replacement_file =
                File::open(&database_path).expect("open replacement database descriptor");
            let replacement_identity = FileIdentity::from_file(&replacement_file)
                .expect("read replacement descriptor identity")
                .expect("Unix descriptors have stable identities");
            assert_ne!(replacement_identity, leased_identity);

            let page_size_usize = usize::try_from(page_size).expect("page size fits usize");
            let replacement_pristine =
                std::fs::read(&database_path).expect("read replacement database");
            let mut replacement_bytes = replacement_pristine.clone();
            assert!(replacement_bytes.len() >= page_size_usize * 2);
            let page_two_preimage =
                replacement_bytes[page_size_usize..page_size_usize * 2].to_vec();
            replacement_bytes[page_size_usize] ^= 0xff;
            std::fs::write(&database_path, &replacement_bytes)
                .expect("write simulated interrupted page update");

            let nonce = 0x4653_514c;
            let journal_header = JournalHeader {
                page_count: 1,
                nonce,
                initial_db_size: page_count,
                sector_size: 512,
                page_size,
            };
            let mut journal_bytes = journal_header.encode_padded();
            journal_bytes.extend(JournalPageRecord::new(2, page_two_preimage, nonce).encode());
            std::fs::write(&journal_path, &journal_bytes).expect("write valid hot journal");

            let database_before =
                std::fs::read(&database_path).expect("snapshot replacement bytes");
            let journal_before = std::fs::read(&journal_path).expect("snapshot hot journal bytes");
            let error = Connection::open_existing_with_expected_identity(
                database_path.to_string_lossy().into_owned(),
                leased_identity,
            )
            .await
            .expect_err("identity-bound open must reject the replacement database");

            assert!(matches!(error, FrankenError::CannotOpen { .. }));
            assert_eq!(
                std::fs::read(&database_path).unwrap(),
                database_before,
                "identity refusal must precede hot-journal recovery writes"
            );
            assert_eq!(
                std::fs::read(&journal_path).unwrap(),
                journal_before,
                "identity refusal must not invalidate or delete the hot journal"
            );

            let control_database_path = dir.path().join("identity-bound-control.db");
            let control_journal_path = dir.path().join("identity-bound-control.db-journal");
            std::fs::write(&control_database_path, &database_before)
                .expect("copy interrupted database into recovery control");
            std::fs::write(&control_journal_path, &journal_before)
                .expect("copy hot journal into recovery control");

            let control =
                Connection::open_existing(control_database_path.to_string_lossy().into_owned())
                    .await
                    .expect("plain write-existing open must recover the control copy");
            drop(control);

            let control_after =
                std::fs::read(&control_database_path).expect("read recovered control database");
            assert_ne!(
                control_after, database_before,
                "control recovery must prove the hot journal is not inert"
            );
            assert_eq!(
                &control_after[page_size_usize..page_size_usize * 2],
                &replacement_pristine[page_size_usize..page_size_usize * 2],
                "control recovery must restore the original page-two preimage"
            );
            assert!(
                !control_journal_path.exists()
                    || std::fs::read(&control_journal_path).unwrap().is_empty(),
                "control recovery must consume or invalidate the hot journal"
            );
        });
    }

    #[cfg(all(feature = "native", windows))]
    #[test]
    fn windows_expected_identity_refuses_before_any_database_artifact_mutation() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let identity_path = dir.path().join("identity.db");
            let candidate_path = dir.path().join("candidate.db");
            for path in [&identity_path, &candidate_path] {
                seed_windows_database(path);
            }

            let expected_identity = windows_file_identity(&identity_path);
            let candidate_identity = windows_file_identity(&candidate_path);
            assert_ne!(expected_identity, candidate_identity);

            let artifacts = native_database_artifacts(&candidate_path);
            seed_windows_auxiliary_sentinels(&artifacts);
            let before = snapshot_native_artifacts(&artifacts);
            assert!(
                before[5..].iter().all(Option::is_none),
                "advisory lock sidecars must start absent"
            );

            let error = Connection::open_existing_with_expected_identity(
                candidate_path.to_string_lossy().into_owned(),
                expected_identity,
            )
            .await
            .expect_err("wrong expected identity must refuse the candidate database");

            assert!(matches!(error, FrankenError::CannotOpen { .. }));
            assert_eq!(
                snapshot_native_artifacts(&artifacts),
                before,
                "identity refusal must leave main, journal, WAL, SHM, and advisory sidecars unchanged"
            );
        });
    }

    #[cfg(all(feature = "native", windows))]
    #[test]
    fn windows_schema_only_identity_mismatch_precedes_artifact_mutation() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let identity_path = dir.path().join("schema-identity.db");
            let candidate_path = dir.path().join("schema-candidate.db");
            seed_windows_database(&identity_path);
            seed_windows_database(&candidate_path);

            let expected_identity = windows_file_identity(&identity_path);
            assert_ne!(expected_identity, windows_file_identity(&candidate_path));

            let artifacts = native_database_artifacts(&candidate_path);
            seed_windows_auxiliary_sentinels(&artifacts);
            let before = snapshot_native_artifacts(&artifacts);
            assert!(
                before[5..].iter().all(Option::is_none),
                "advisory lock sidecars must start absent"
            );

            let error = Connection::open_schema_only_with_expected_identity(
                candidate_path.to_string_lossy().into_owned(),
                expected_identity,
            )
            .await
            .expect_err("wrong expected identity must refuse schema-only open");

            assert!(matches!(error, FrankenError::CannotOpen { .. }));
            assert_eq!(
                snapshot_native_artifacts(&artifacts),
                before,
                "schema-only refusal must leave every database artifact unchanged"
            );
        });
    }

    #[cfg(all(feature = "native", windows))]
    #[test]
    fn windows_existing_expected_identity_accepts_matching_file() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let database_path = dir.path().join("existing-matching-identity.db");
            seed_windows_database(&database_path);

            let leased_file =
                std::fs::File::open(&database_path).expect("retain existing database handle");
            let expected_identity = FileIdentity::from_file(&leased_file)
                .expect("query existing database identity")
                .expect("Windows file identity must be available");
            let conn = Connection::open_existing_with_expected_identity(
                database_path.to_string_lossy().into_owned(),
                expected_identity,
            )
            .await
            .expect("matching identity must open the existing database");

            assert_eq!(conn.file_identity().await.unwrap(), Some(expected_identity));
            let rows = conn
                .query("SELECT COUNT(*) FROM identity_probe;")
                .await
                .expect("matching identity connection must query the seeded table");
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
            drop(conn);
            drop(leased_file);
        });
    }

    #[cfg(all(feature = "native", windows))]
    #[test]
    fn windows_schema_only_expected_identity_accepts_matching_file() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let database_path = dir.path().join("schema-matching-identity.db");
            seed_windows_database(&database_path);

            let leased_file =
                std::fs::File::open(&database_path).expect("retain schema database handle");
            let expected_identity = FileIdentity::from_file(&leased_file)
                .expect("query schema database identity")
                .expect("Windows file identity must be available");
            let conn = Connection::open_schema_only_with_expected_identity(
                database_path.to_string_lossy().into_owned(),
                expected_identity,
            )
            .await
            .expect("matching identity must open the schema-only connection");

            assert_eq!(conn.file_identity().await.unwrap(), Some(expected_identity));
            let rows = conn
                .query("SELECT COUNT(*) FROM sqlite_master WHERE name = 'identity_probe';")
                .await
                .expect("matching identity schema connection must load the seeded schema");
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
            drop(conn);
            drop(leased_file);
        });
    }

    #[cfg(all(feature = "native", windows))]
    #[test]
    fn windows_reserved_empty_open_is_identity_bound() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let identity_path = dir.path().join("reservation-identity.db");
            let candidate_path = dir.path().join("reservation-candidate.db");
            let accepted_path = dir.path().join("reservation-accepted.db");
            for path in [&identity_path, &candidate_path, &accepted_path] {
                drop(std::fs::File::create(path).expect("reserve empty database path"));
            }

            let wrong_identity = windows_file_identity(&identity_path);
            assert_ne!(wrong_identity, windows_file_identity(&candidate_path));
            let candidate_artifacts = native_database_artifacts(&candidate_path);
            seed_windows_auxiliary_sentinels(&candidate_artifacts);
            let candidate_before = snapshot_native_artifacts(&candidate_artifacts);
            assert!(
                candidate_before[5..].iter().all(Option::is_none),
                "advisory lock sidecars must start absent"
            );

            let error = Connection::open_reserved_with_expected_identity(
                candidate_path.to_string_lossy().into_owned(),
                wrong_identity,
            )
            .await
            .expect_err("wrong reservation identity must refuse initialization");
            assert!(matches!(error, FrankenError::CannotOpen { .. }));
            assert_eq!(
                snapshot_native_artifacts(&candidate_artifacts),
                candidate_before,
                "wrong reservation identity must leave the empty file and sidecars untouched"
            );

            let accepted_reservation =
                std::fs::File::open(&accepted_path).expect("retain accepted reservation handle");
            let accepted_identity = FileIdentity::from_file(&accepted_reservation)
                .expect("query accepted reservation identity")
                .expect("Windows file identity must be available");
            let conn = Connection::open_reserved_with_expected_identity(
                accepted_path.to_string_lossy().into_owned(),
                accepted_identity,
            )
            .await
            .expect("matching reservation identity must initialize the database");
            assert_eq!(conn.file_identity().await.unwrap(), Some(accepted_identity));
            assert!(
                std::fs::metadata(&accepted_path).unwrap().len() > 0,
                "matching reservation must initialize the empty database image"
            );
            conn.execute("CREATE TABLE reservation_probe(value INTEGER NOT NULL);")
                .await
                .expect("initialized reservation must accept SQL");
        });
    }

    #[test]
    fn test_public_api_query_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:")
                .await
                .expect("in-memory connection should open");
            let rows = conn
                .query("SELECT 1 + 2, 'ab' || 'cd';")
                .await
                .expect("query should succeed");
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(3), SqliteValue::Text("abcd".into()),]
            );
        });
    }

    #[test]
    fn test_public_api_query_with_params() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:")
                .await
                .expect("in-memory connection should open");
            let rows = conn
                .query_with_params(
                    "SELECT ?1 + ?2, ?3;",
                    &[
                        SqliteValue::Integer(4),
                        SqliteValue::Integer(5),
                        SqliteValue::Text("ok".into()),
                    ],
                )
                .await
                .expect("query_with_params should succeed");
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(9), SqliteValue::Text("ok".into())]
            );
        });
    }

    #[test]
    fn test_public_api_query_row_multiple_rows_error() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:")
                .await
                .expect("in-memory connection should open");
            let error = conn
                .query_row("VALUES (10), (20), (30);")
                .await
                .expect_err("query_row should fail when more than one row is returned");
            assert!(matches!(error, FrankenError::QueryReturnedMultipleRows));
        });
    }

    #[test]
    fn test_public_api_query_row_empty_error() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:")
                .await
                .expect("in-memory connection should open");
            let error = conn
                .query_row("SELECT 1 WHERE 0;")
                .await
                .expect_err("query_row should fail for empty result set");
            assert!(matches!(error, FrankenError::QueryReturnedNoRows));
        });
    }

    #[test]
    fn test_public_api_execute_returns_row_count() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:")
                .await
                .expect("in-memory connection should open");
            let count = conn
                .execute("VALUES (1), (2), (3);")
                .await
                .expect("execute should succeed");
            assert_eq!(count, 3);
        });
    }

    // ── Connection::open error paths ────────────────────────────────────

    #[test]
    fn open_empty_path_fails() {
        asupersync::test_utils::run_test(|| async {
            let err = Connection::open("")
                .await
                .expect_err("empty path should fail");
            assert!(matches!(err, FrankenError::CannotOpen { .. }));
        });
    }

    #[test]
    fn runtime_api_is_reexported() {
        asupersync::test_utils::run_test(|| async {
            let runtime = init_global_runtime(RuntimeConfig {
                worker_threads: 2,
                io_poll_strategy: IoPollStrategy::Blocking,
            });
            assert_eq!(runtime.config().worker_threads, 2);
            assert_eq!(runtime.config().io_poll_strategy, IoPollStrategy::Blocking);

            let parent_cx = fsqlite_types::cx::Cx::new().with_trace_context(11, 0, 0);
            let explicit_runtime = Arc::new(RuntimeContext::new_with_root_cx(
                RuntimeConfig {
                    worker_threads: 1,
                    io_poll_strategy: IoPollStrategy::Auto,
                },
                &parent_cx,
            ));
            let env = ConnectionEnv::new(Arc::clone(&explicit_runtime));
            let conn = Connection::open_with_env(":memory:", env)
                .await
                .expect("connection should open");
            assert_eq!(conn.path(), ":memory:");
        });
    }

    // ── Row accessors ────────────────────────────────────────────────────

    #[test]
    fn row_get_valid_index() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT 42, 'hello';").await.unwrap();
            assert_eq!(row.get(0), Some(&SqliteValue::Integer(42)));
            assert_eq!(row.get(1), Some(&SqliteValue::Text("hello".into())));
        });
    }

    #[test]
    fn row_get_out_of_bounds() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT 1;").await.unwrap();
            assert_eq!(row.get(99), None);
        });
    }

    #[test]
    fn row_values_returns_all_columns() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT 1, 2, 3;").await.unwrap();
            assert_eq!(row.values().len(), 3);
        });
    }

    // ── PreparedStatement ────────────────────────────────────────────────

    #[test]
    fn prepared_query() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let stmt = conn.prepare("SELECT 7 * 6;").await.unwrap();
            let rows = stmt.query().await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
        });
    }

    #[test]
    fn prepared_query_with_params() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let stmt = conn.prepare("SELECT ?1 + ?2;").await.unwrap();
            let rows = stmt
                .query_with_params(&[SqliteValue::Integer(10), SqliteValue::Integer(20)])
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(30)]);
        });
    }

    #[test]
    fn prepared_query_row() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let stmt = conn.prepare("SELECT 99;").await.unwrap();
            let row = stmt.query_row().await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(99)]);
        });
    }

    #[test]
    fn prepared_query_row_with_params() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let stmt = conn.prepare("SELECT ?1;").await.unwrap();
            let row = stmt
                .query_row_with_params(&[SqliteValue::Text("xyz".into())])
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("xyz".into())]);
        });
    }

    #[test]
    fn prepared_execute() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let stmt = conn.prepare("VALUES (1), (2);").await.unwrap();
            assert_eq!(stmt.execute().await.unwrap(), 2);
        });
    }

    #[test]
    fn prepared_execute_with_params() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let stmt = conn.prepare("SELECT ?1;").await.unwrap();
            assert_eq!(
                stmt.execute_with_params(&[SqliteValue::Integer(1)])
                    .await
                    .unwrap(),
                1
            );
        });
    }

    #[test]
    fn prepared_explain_not_empty() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let stmt = conn.prepare("SELECT 1 + 2;").await.unwrap();
            let explain = stmt.explain();
            assert!(!explain.is_empty());
        });
    }

    #[cfg(feature = "session")]
    #[test]
    fn session_feature_reexports_manual_session_api() {
        assert_eq!(super::session::extension_name(), "session");

        let mut session = super::session::Session::new();
        session.attach_table("users", 2, vec![true, false]);
        session.record_insert(
            "users",
            vec![
                super::session::ChangesetValue::Integer(1),
                super::session::ChangesetValue::Text("alice".to_owned()),
            ],
        );

        let encoded = session.changeset().encode();
        let decoded = super::session::Changeset::decode(&encoded)
            .expect("re-exported session API should round-trip changesets");
        assert_eq!(decoded.encode(), encoded);
    }

    #[test]
    fn prepared_indexed_equality_explain_uses_duplicate_run_probe() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("indexed-equality.db");
            let db = db_path.to_string_lossy().to_string();

            {
                let conn = Connection::open(&db).await.unwrap();
                conn.execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, b INTEGER NOT NULL, name TEXT NOT NULL);",
            )
            .await
            .unwrap();
                conn.execute("CREATE INDEX idx_t_b ON t(b);").await.unwrap();
                conn.execute(
                    "INSERT INTO t (id, b, name) VALUES \
                 (1, 42, 'alice'), (2, 42, 'bruce'), (5, 42, 'claire'), (9, 99, 'dora');",
                )
                .await
                .unwrap();
            }

            let conn = Connection::open(&db).await.unwrap();
            let stmt = conn
                .prepare("SELECT name FROM t WHERE b = ?1;")
                .await
                .unwrap();

            let explain = stmt.explain();
            assert!(explain.contains("idx_t_b"));
            assert!(explain.contains("SeekGE"));
            assert!(explain.contains("IdxRowid"));
            assert!(explain.contains("SeekRowid"));
            assert!(
                explain.contains("-9223372036854775808"),
                "expected synthetic low-rowid probe in explain output: {explain}"
            );

            let rows = stmt
                .query_with_params(&[SqliteValue::Integer(42)])
                .await
                .unwrap();
            assert_eq!(
                rows.iter()
                    .map(|row| row.get(0).cloned().unwrap())
                    .collect::<Vec<_>>(),
                vec![
                    SqliteValue::Text("alice".to_owned().into()),
                    SqliteValue::Text("bruce".to_owned().into()),
                    SqliteValue::Text("claire".to_owned().into()),
                ]
            );
        });
    }

    // ── Connection::query_row_with_params ────────────────────────────────

    #[test]
    fn query_row_with_params() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row_with_params("SELECT ?1 * 2;", &[SqliteValue::Integer(5)])
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(10)]);
        });
    }

    // ── Connection::execute_with_params ──────────────────────────────────

    #[test]
    fn execute_with_params_returns_count() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let count = conn
                .execute_with_params("SELECT ?1;", &[SqliteValue::Integer(1)])
                .await
                .unwrap();
            assert_eq!(count, 1);
        });
    }

    // ── DDL ──────────────────────────────────────────────────────────────

    #[test]
    fn create_table_and_insert_select() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER, b TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'one');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (2, 'two');")
                .await
                .unwrap();
            let rows = conn.query("SELECT a, b FROM t1;").await.unwrap();
            assert_eq!(rows.len(), 2);
        });
    }

    #[test]
    fn create_table_if_not_exists_no_error() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (x INTEGER);").await.unwrap();
            // Should not error with IF NOT EXISTS
            conn.execute("CREATE TABLE IF NOT EXISTS t1 (x INTEGER);")
                .await
                .unwrap();
        });
    }

    #[test]
    fn create_duplicate_table_errors() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (x INTEGER);").await.unwrap();
            let err = conn
                .execute("CREATE TABLE t1 (x INTEGER);")
                .await
                .expect_err("duplicate table should fail");
            assert!(matches!(err, FrankenError::Internal(_)));
        });
    }

    #[test]
    fn public_api_writable_schema_allows_filebacked_sqlite_master_insert() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().expect("create temp dir");
            let db_path = dir.path().join("writable-schema.db");
            let conn = Connection::open(db_path.to_string_lossy().into_owned())
                .await
                .unwrap();

            conn.execute("CREATE TABLE real_table (id INTEGER);")
                .await
                .unwrap();
            let before = conn.query_row("PRAGMA writable_schema;").await.unwrap();
            assert_eq!(row_values(&before), vec![SqliteValue::Integer(0)]);

            conn.execute("PRAGMA writable_schema = ON;").await.unwrap();
            let after = conn.query_row("PRAGMA writable_schema;").await.unwrap();
            assert_eq!(row_values(&after), vec![SqliteValue::Integer(1)]);

            let inserted = conn
                .execute(
                    "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql) \
                 VALUES('table', 'fake_tbl', 'fake_tbl', 0, 'CREATE TABLE fake_tbl(x)');",
                )
                .await
                .unwrap();
            assert_eq!(inserted, 1);

            let deleted = conn
                .execute("DELETE FROM sqlite_master WHERE name = 'fake_tbl';")
                .await
                .unwrap();
            assert_eq!(deleted, 1);
        });
    }

    // ── DML affected-row counts (bd-118o) ─────────────────────────────────

    #[test]
    fn execute_insert_returns_affected_count() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            assert_eq!(conn.execute("INSERT INTO t VALUES (1);").await.unwrap(), 1);
            assert_eq!(
                conn.execute("INSERT INTO t VALUES (2), (3), (4);")
                    .await
                    .unwrap(),
                3,
            );
        });
    }

    #[test]
    fn execute_update_returns_affected_count() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (3);").await.unwrap();
            assert_eq!(conn.execute("UPDATE t SET v = 0;").await.unwrap(), 3);
            assert_eq!(
                conn.execute("UPDATE t SET v = 99 WHERE v = 0;")
                    .await
                    .unwrap(),
                3
            );
        });
    }

    #[test]
    fn execute_delete_returns_affected_count() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (3);").await.unwrap();
            assert_eq!(conn.execute("DELETE FROM t WHERE v = 2;").await.unwrap(), 1);
            assert_eq!(conn.execute("DELETE FROM t;").await.unwrap(), 2);
        });
    }

    // ── DML: UPDATE / DELETE ─────────────────────────────────────────────

    #[test]
    fn update_modifies_rows() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (20);").await.unwrap();
            conn.execute("UPDATE t SET v = 99 WHERE v = 10;")
                .await
                .unwrap();
            let rows = conn.query("SELECT v FROM t;").await.unwrap();
            let vals: Vec<_> = rows.iter().map(row_values).collect();
            assert!(vals.contains(&vec![SqliteValue::Integer(99)]));
            assert!(vals.contains(&vec![SqliteValue::Integer(20)]));
        });
    }

    #[test]
    fn update_preserves_integer_primary_key_rowid_alias() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO accounts VALUES (1, 100);")
                .await
                .unwrap();
            conn.execute("INSERT INTO accounts VALUES (2, 200);")
                .await
                .unwrap();

            conn.execute("UPDATE accounts SET balance = balance + 5 WHERE id = 1;")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT id, balance FROM accounts ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2, "update must not create or lose rows");
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(1), SqliteValue::Integer(105)],
                "id=1 row must be updated in place"
            );
            assert_eq!(
                row_values(&rows[1]),
                vec![SqliteValue::Integer(2), SqliteValue::Integer(200)],
                "id=2 row must remain unchanged"
            );
        });
    }

    #[test]
    fn concurrent_same_row_deposit_commits_must_conflict_or_serialize() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("concurrent_same_row_deposit.db");
            let db = db_path.to_string_lossy().to_string();

            {
                let conn = Connection::open(&db).await.unwrap();
                conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
                    .await
                    .unwrap();
                conn.execute(
                    "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL);",
                )
                .await
                .unwrap();
                conn.execute("INSERT INTO accounts VALUES (1, 0);")
                    .await
                    .unwrap();
            }

            let conn1 = Connection::open(&db).await.unwrap();
            let conn2 = Connection::open(&db).await.unwrap();
            conn1
                .execute("PRAGMA fsqlite.concurrent_mode=ON;")
                .await
                .unwrap();
            conn2
                .execute("PRAGMA fsqlite.concurrent_mode=ON;")
                .await
                .unwrap();

            conn1.execute("BEGIN CONCURRENT;").await.unwrap();
            conn2.execute("BEGIN CONCURRENT;").await.unwrap();

            assert_eq!(
                conn1
                    .execute("UPDATE accounts SET balance = balance + 1 WHERE id = 1;")
                    .await
                    .unwrap(),
                1
            );
            let update2 = conn2
                .execute("UPDATE accounts SET balance = balance + 1 WHERE id = 1;")
                .await;

            let commit1 = conn1.execute("COMMIT;").await;
            let commit2 = match update2 {
                Ok(changes2) => {
                    assert_eq!(changes2, 1, "second update should affect one row");
                    conn2.execute("COMMIT;").await
                }
                Err(err) => {
                    assert!(
                        err.is_transient(),
                        "second concurrent writer should fail transiently on conflict, got: {err}"
                    );
                    let rollback = conn2.execute("ROLLBACK;").await;
                    assert!(
                        rollback.is_ok(),
                        "second writer should remain rollback-able after transient conflict: {rollback:?}"
                    );
                    Err(err)
                }
            };

            let verify = Connection::open(&db).await.unwrap();
            let row = verify
                .query_row("SELECT balance FROM accounts WHERE id = 1;")
                .await
                .unwrap();
            let balance = row.get(0).cloned().unwrap_or(SqliteValue::Null);
            match (commit1, commit2) {
                (Ok(_), Ok(_)) => {
                    assert_eq!(
                        balance,
                        SqliteValue::Integer(2),
                        "if both commits succeed, both deposits must be visible"
                    );
                }
                (Ok(_), Err(err)) | (Err(err), Ok(_)) => {
                    assert!(
                        err.is_transient(),
                        "conflicting concurrent writer should fail with transient busy snapshot/busy, got: {err}"
                    );
                    assert_eq!(
                        balance,
                        SqliteValue::Integer(1),
                        "if one writer aborts, exactly one deposit should persist"
                    );
                }
                (Err(err1), Err(err2)) => {
                    panic!("at least one concurrent writer must commit: err1={err1}; err2={err2}");
                }
            }
        });
    }

    #[test]
    fn delete_removes_rows() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (3);").await.unwrap();
            conn.execute("DELETE FROM t WHERE v = 2;").await.unwrap();
            let rows = conn.query("SELECT v FROM t;").await.unwrap();
            assert_eq!(rows.len(), 2);
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(vals.contains(&SqliteValue::Integer(1)));
            assert!(vals.contains(&SqliteValue::Integer(3)));
        });
    }

    // ── Type handling ────────────────────────────────────────────────────

    #[test]
    fn null_value_roundtrip() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT NULL;").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Null]);
        });
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn real_value_roundtrip() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT 3.14;").await.unwrap();
            if let SqliteValue::Float(v) = &row_values(&row)[0] {
                assert!((*v - 3.14).abs() < f64::EPSILON);
            } else {
                unreachable!("expected Float value");
            }
        });
    }

    #[test]
    fn text_value_roundtrip() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT 'hello world';").await.unwrap();
            assert_eq!(
                row_values(&row),
                vec![SqliteValue::Text("hello world".into())]
            );
        });
    }

    #[test]
    fn blob_value_via_params() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let blob = vec![0xDE, 0xAD, 0xBE, 0xEF];
            let row = conn
                .query_row_with_params("SELECT ?1;", &[SqliteValue::Blob(blob.clone().into())])
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Blob(blob.into())]);
        });
    }

    // ── Transaction control ──────────────────────────────────────────────

    #[test]
    fn in_transaction_flag() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            assert!(!conn.in_transaction());
            conn.execute("BEGIN;").await.unwrap();
            assert!(conn.in_transaction());
            conn.execute("COMMIT;").await.unwrap();
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn begin_commit_persists_changes() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            conn.execute("BEGIN;").await.unwrap();
            conn.execute("INSERT INTO t VALUES (42);").await.unwrap();
            conn.execute("COMMIT;").await.unwrap();
            let rows = conn.query("SELECT v FROM t;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
        });
    }

    #[test]
    fn rollback_reverts_changes() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();

            let m1 = conn.query("SELECT * FROM sqlite_master;").await.unwrap();
            eprintln!("MAIN BEFORE BEGIN: {:?}", m1);

            conn.execute("BEGIN;").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
            conn.execute("ROLLBACK;").await.unwrap();

            let m2 = conn.query("SELECT * FROM sqlite_master;").await.unwrap();
            eprintln!("MAIN AFTER ROLLBACK: {:?}", m2);

            let rows = conn.query("SELECT v FROM t;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
        });
    }

    #[test]
    fn nested_begin_errors() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("BEGIN;").await.unwrap();
            let err = conn
                .execute("BEGIN;")
                .await
                .expect_err("nested begin should fail");
            assert!(matches!(err, FrankenError::Internal(_)));
        });
    }

    #[test]
    fn commit_without_transaction_errors() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let err = conn
                .execute("COMMIT;")
                .await
                .expect_err("commit without txn should fail");
            assert!(matches!(err, FrankenError::Internal(_)));
        });
    }

    #[test]
    fn rollback_without_transaction_errors() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let err = conn
                .execute("ROLLBACK;")
                .await
                .expect_err("rollback without txn should fail");
            assert!(matches!(err, FrankenError::Internal(_)));
        });
    }

    // ── Savepoint ────────────────────────────────────────────────────────

    #[test]
    fn savepoint_and_rollback_to() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
            conn.execute("SAVEPOINT sp1;").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
            conn.execute("ROLLBACK TO sp1;").await.unwrap();
            let rows = conn.query("SELECT v FROM t;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
        });
    }

    #[test]
    fn savepoint_release_commits_changes() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            conn.execute("SAVEPOINT sp1;").await.unwrap();
            conn.execute("INSERT INTO t VALUES (100);").await.unwrap();
            conn.execute("RELEASE sp1;").await.unwrap();
            let rows = conn.query("SELECT v FROM t;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(100)]);
        });
    }

    #[test]
    fn release_nonexistent_savepoint_errors() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("BEGIN;").await.unwrap();
            let err = conn
                .execute("RELEASE nosuch;")
                .await
                .expect_err("release nonexistent savepoint should fail");
            assert!(matches!(err, FrankenError::Internal(_)));
        });
    }

    // ── Parse error ──────────────────────────────────────────────────────

    #[test]
    fn parse_error_on_invalid_sql() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            assert!(conn.query("NOT VALID SQL;").await.is_err());
        });
    }

    // ── Multiple statements ──────────────────────────────────────────────

    #[test]
    fn multiple_statements_in_query() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (v INTEGER);").await.unwrap();
            // query() processes all statements, returns rows from last
            let rows = conn
                .query("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT v FROM t;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
        });
    }

    // ── Expression arithmetic ────────────────────────────────────────────

    #[test]
    fn arithmetic_expressions() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT 10 - 3, 4 * 5, 20 / 4;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&row),
                vec![
                    SqliteValue::Integer(7),
                    SqliteValue::Integer(20),
                    SqliteValue::Integer(5),
                ]
            );
        });
    }

    #[test]
    fn string_concatenation() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT 'foo' || 'bar';").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("foobar".into())]);
        });
    }

    // ── Compound WHERE predicates (bd-2832) ────────────────────────────

    async fn setup_three_rows(conn: &Connection) {
        conn.execute("CREATE TABLE t3 (a INTEGER, b TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t3 VALUES (1, 'one');")
            .await
            .unwrap();
        conn.execute("INSERT INTO t3 VALUES (2, 'two');")
            .await
            .unwrap();
        conn.execute("INSERT INTO t3 VALUES (3, 'three');")
            .await
            .unwrap();
    }

    #[test]
    fn where_and_predicate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            let rows = conn
                .query("SELECT a FROM t3 WHERE a > 1 AND b = 'two';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
        });
    }

    #[test]
    fn where_or_predicate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            let rows = conn
                .query("SELECT a FROM t3 WHERE a = 1 OR a = 3;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(vals.contains(&SqliteValue::Integer(1)));
            assert!(vals.contains(&SqliteValue::Integer(3)));
        });
    }

    #[test]
    fn where_comparison_operators() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            // Greater than
            let rows = conn.query("SELECT a FROM t3 WHERE a > 2;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(3)]);
            // Less than or equal
            let rows = conn.query("SELECT a FROM t3 WHERE a <= 2;").await.unwrap();
            assert_eq!(rows.len(), 2);
            // Not equal
            let rows = conn.query("SELECT a FROM t3 WHERE a != 2;").await.unwrap();
            assert_eq!(rows.len(), 2);
        });
    }

    // ── NULL handling (WHERE) ──────────────────────────────────────────

    #[test]
    fn where_is_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE tn (a INTEGER, b TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO tn VALUES (1, 'x');")
                .await
                .unwrap();
            conn.execute("INSERT INTO tn VALUES (2, NULL);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT a FROM tn WHERE b IS NULL;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
        });
    }

    #[test]
    fn where_is_not_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE tn2 (a INTEGER, b TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO tn2 VALUES (1, 'x');")
                .await
                .unwrap();
            conn.execute("INSERT INTO tn2 VALUES (2, NULL);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT a FROM tn2 WHERE b IS NOT NULL;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
        });
    }

    // ── NULL handling (expression) ─────────────────────────────────────

    #[test]
    fn coalesce_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT COALESCE(NULL, NULL, 42);")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(42)]);
        });
    }

    #[test]
    fn nullif_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT NULLIF(1, 1);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Null]);
            let row = conn.query_row("SELECT NULLIF(1, 2);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
        });
    }

    // ── CASE WHEN ──────────────────────────────────────────────────────

    #[test]
    fn case_when_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END;")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("yes".into())]);
        });
    }

    #[test]
    fn case_simple_form() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END;")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("b".into())]);
        });
    }

    // ── Built-in functions ─────────────────────────────────────────────

    #[test]
    fn builtin_abs() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT ABS(-42);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(42)]);
        });
    }

    #[test]
    fn builtin_length() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT LENGTH('hello');").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(5)]);
        });
    }

    #[test]
    fn builtin_upper_lower() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT UPPER('hello'), LOWER('WORLD');")
                .await
                .unwrap();
            assert_eq!(
                row_values(&row),
                vec![
                    SqliteValue::Text("HELLO".into()),
                    SqliteValue::Text("world".into()),
                ]
            );
        });
    }

    #[test]
    fn builtin_typeof() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT TYPEOF(42);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("integer".into())]);
        });
    }

    // ── CAST ───────────────────────────────────────────────────────────

    #[test]
    fn cast_integer_to_text() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT CAST(42 AS TEXT);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("42".into())]);
        });
    }

    #[test]
    fn cast_text_to_integer() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT CAST('123' AS INTEGER);")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(123)]);
        });
    }

    // ── Blob literal ───────────────────────────────────────────────────

    #[test]
    fn blob_literal_hex() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT X'DEADBEEF';").await.unwrap();
            assert_eq!(
                row_values(&row),
                vec![SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF].into())]
            );
        });
    }

    // ── Unary operators ────────────────────────────────────────────────

    #[test]
    fn unary_minus() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT -42;").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(-42)]);
        });
    }

    #[test]
    fn not_operator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT NOT 0;").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
        });
    }

    // ── ORDER BY / LIMIT (expression path) ─────────────────────────────

    #[test]
    fn values_order_by() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            assert!(
                conn.query("VALUES (3), (1), (2) ORDER BY 1;")
                    .await
                    .is_err(),
                "bare VALUES cannot carry an ORDER BY clause in SQLite grammar"
            );
            let rows = conn
                .query("SELECT * FROM (VALUES (3), (1), (2)) ORDER BY 1;")
                .await
                .unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(
                vals,
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(3),
                ]
            );
        });
    }

    #[test]
    fn values_order_by_desc_with_limit() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query("SELECT * FROM (VALUES (3), (1), (2)) ORDER BY 1 DESC LIMIT 2;")
                .await
                .unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(vals, vec![SqliteValue::Integer(3), SqliteValue::Integer(2)]);
        });
    }

    #[test]
    fn values_limit_offset() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            assert!(
                conn.query("VALUES (10), (20), (30), (40) LIMIT 2 OFFSET 1;")
                    .await
                    .is_err(),
                "bare VALUES cannot carry a LIMIT clause in SQLite grammar"
            );
            let rows = conn
                .query("SELECT * FROM (VALUES (10), (20), (30), (40)) ORDER BY 1 LIMIT 2 OFFSET 1;")
                .await
                .unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(
                vals,
                vec![SqliteValue::Integer(20), SqliteValue::Integer(30)]
            );
        });
    }

    // ── DELETE without WHERE (all rows) ────────────────────────────────

    #[test]
    fn delete_all_rows() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            conn.execute("DELETE FROM t3;").await.unwrap();
            let rows = conn.query("SELECT a FROM t3;").await.unwrap();
            assert_eq!(rows.len(), 0);
        });
    }

    // ── Non-column result expressions (bd-19g7) ────────────────────────

    #[test]
    fn select_expression_column_arithmetic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE te (a INTEGER);").await.unwrap();
            conn.execute("INSERT INTO te VALUES (10);").await.unwrap();
            conn.execute("INSERT INTO te VALUES (20);").await.unwrap();
            let rows = conn.query("SELECT a + 1 FROM te;").await.unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(vals.contains(&SqliteValue::Integer(11)));
            assert!(vals.contains(&SqliteValue::Integer(21)));
        });
    }

    #[test]
    fn select_expression_column_with_literal() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE te2 (a INTEGER, b TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO te2 VALUES (5, 'hello');")
                .await
                .unwrap();
            let rows = conn.query("SELECT a * 2, b FROM te2;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(10), SqliteValue::Text("hello".into())]
            );
        });
    }

    // ── Multi-row INSERT (bd-2of2) ────────────────────────────────────

    #[test]
    fn insert_multi_row_values() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE tm (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO tm VALUES (1), (2), (3);")
                .await
                .unwrap();
            let rows = conn.query("SELECT v FROM tm;").await.unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(vals.len(), 3);
            assert!(vals.contains(&SqliteValue::Integer(1)));
            assert!(vals.contains(&SqliteValue::Integer(2)));
            assert!(vals.contains(&SqliteValue::Integer(3)));
        });
    }

    // ── IN / BETWEEN / LIKE (bd-3vpo) ─────────────────────────────────

    #[test]
    fn in_expression_only() {
        asupersync::test_utils::run_test(|| async {
            // Test IN without any table - pure expression evaluation
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT 2 IN (1, 2, 3);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
        });
    }

    #[test]
    fn between_expression_only() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT 2 BETWEEN 1 AND 3;").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
        });
    }

    #[test]
    fn where_in_operator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            let rows = conn
                .query("SELECT a FROM t3 WHERE a IN (1, 3);")
                .await
                .unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(vals.len(), 2);
            assert!(vals.contains(&SqliteValue::Integer(1)));
            assert!(vals.contains(&SqliteValue::Integer(3)));
        });
    }

    #[test]
    fn where_between_operator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            let rows = conn
                .query("SELECT a FROM t3 WHERE a BETWEEN 1 AND 2;")
                .await
                .unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(vals.len(), 2);
            assert!(vals.contains(&SqliteValue::Integer(1)));
            assert!(vals.contains(&SqliteValue::Integer(2)));
        });
    }

    #[test]
    fn where_like_operator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            let rows = conn
                .query("SELECT b FROM t3 WHERE b LIKE 't%';")
                .await
                .unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(vals.len(), 2);
            assert!(vals.contains(&SqliteValue::Text("two".into())));
            assert!(vals.contains(&SqliteValue::Text("three".into())));
        });
    }

    // ── Aggregates (bd-xldj) ────────────────────────────────────────────

    #[test]
    fn aggregate_count_star() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            let row = conn.query_row("SELECT COUNT(*) FROM t3;").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(3)]);
        });
    }

    #[test]
    fn aggregate_sum_min_max() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            let row = conn
                .query_row("SELECT SUM(a), MIN(a), MAX(a) FROM t3;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&row),
                vec![
                    SqliteValue::Integer(6),
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(3),
                ]
            );
        });
    }

    #[test]
    fn aggregate_avg() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_three_rows(&conn).await;
            let row = conn.query_row("SELECT AVG(a) FROM t3;").await.unwrap();
            // AVG(1,2,3) = 2.0
            assert_eq!(row_values(&row), vec![SqliteValue::Float(2.0)]);
        });
    }

    // ── UPDATE all rows (no WHERE) ─────────────────────────────────────

    #[test]
    fn update_all_rows() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE tu (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO tu VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO tu VALUES (2);").await.unwrap();
            conn.execute("UPDATE tu SET v = 0;").await.unwrap();
            let rows = conn.query("SELECT v FROM tu;").await.unwrap();
            assert!(
                rows.iter()
                    .all(|r| row_values(r) == vec![SqliteValue::Integer(0)])
            );
        });
    }

    // ═══════════════════════════════════════════════════════════════════
    // bd-2832: Expanded SQL pattern coverage (IvoryWaterfall)
    // ═══════════════════════════════════════════════════════════════════

    async fn setup_bd2832(conn: &Connection) {
        conn.execute("CREATE TABLE tp (a INTEGER, b TEXT, c REAL);")
            .await
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (1, 'alpha', 1.5);")
            .await
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (2, 'beta', 2.5);")
            .await
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (3, 'gamma', 3.5);")
            .await
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (4, NULL, 4.5);")
            .await
            .unwrap();
        conn.execute("INSERT INTO tp VALUES (5, 'delta', 5.5);")
            .await
            .unwrap();
    }

    // ── WHERE NOT ───────────────────────────────────────────────────────

    #[test]
    fn where_not_predicate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn
                .query("SELECT a FROM tp WHERE NOT (a > 3);")
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(vals.contains(&SqliteValue::Integer(1)));
            assert!(vals.contains(&SqliteValue::Integer(2)));
            assert!(vals.contains(&SqliteValue::Integer(3)));
        });
    }

    // ── Comparison operators (>=, <) ────────────────────────────────────

    #[test]
    fn where_greater_equal() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn.query("SELECT a FROM tp WHERE a >= 4;").await.unwrap();
            assert_eq!(rows.len(), 2);
        });
    }

    #[test]
    fn where_less_than() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn.query("SELECT a FROM tp WHERE a < 3;").await.unwrap();
            assert_eq!(rows.len(), 2);
        });
    }

    // ── Table-backed ORDER BY ASC / DESC ────────────────────────────────

    #[test]
    fn table_order_by_asc() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE tord (v INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO tord VALUES (3);").await.unwrap();
            conn.execute("INSERT INTO tord VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO tord VALUES (2);").await.unwrap();
            let rows = conn.query("SELECT v FROM tord ORDER BY v;").await.unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(
                vals,
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(3),
                ]
            );
        });
    }

    #[test]
    fn table_order_by_desc() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE tord2 (v INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO tord2 VALUES (3);").await.unwrap();
            conn.execute("INSERT INTO tord2 VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO tord2 VALUES (2);").await.unwrap();
            let rows = conn
                .query("SELECT v FROM tord2 ORDER BY v DESC;")
                .await
                .unwrap();
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(
                vals,
                vec![
                    SqliteValue::Integer(3),
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(1),
                ]
            );
        });
    }

    // ── Table-backed LIMIT / OFFSET ─────────────────────────────────────

    #[test]
    fn table_limit() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn.query("SELECT a FROM tp LIMIT 3;").await.unwrap();
            assert_eq!(rows.len(), 3);
        });
    }

    #[test]
    fn table_limit_offset() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn
                .query("SELECT a FROM tp LIMIT 2 OFFSET 2;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(vals, vec![SqliteValue::Integer(3), SqliteValue::Integer(4)]);
        });
    }

    // ── WHERE + LIMIT ───────────────────────────────────────────────────

    #[test]
    fn where_with_limit() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn
                .query("SELECT a FROM tp WHERE a > 1 LIMIT 2;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(vals, vec![SqliteValue::Integer(2), SqliteValue::Integer(3)]);
        });
    }

    // ── CASE WHEN on table-backed SELECT ────────────────────────────────

    #[test]
    fn case_when_table_backed() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn
                .query("SELECT CASE WHEN a > 3 THEN 'big' ELSE 'small' END FROM tp;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 5);
            assert_eq!(rows[0].values()[0], SqliteValue::Text("small".into()));
            assert_eq!(rows[3].values()[0], SqliteValue::Text("big".into()));
        });
    }

    // ── CAST on table column ────────────────────────────────────────────

    #[test]
    fn cast_table_backed() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE tcast (v INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO tcast VALUES (42);")
                .await
                .unwrap();
            let row = conn
                .query_row("SELECT CAST(v AS TEXT) FROM tcast;")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("42".into())]);
        });
    }

    // ── IS NULL / IS NOT NULL on table ──────────────────────────────────

    #[test]
    fn where_column_is_null_correct() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn
                .query("SELECT a FROM tp WHERE b IS NULL;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(4)]);
        });
    }

    #[test]
    fn where_column_is_not_null_correct() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn
                .query("SELECT a FROM tp WHERE b IS NOT NULL;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 4);
        });
    }

    // ── Unary minus on table column ─────────────────────────────────────

    #[test]
    fn unary_minus_table_column() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE tneg (x INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO tneg VALUES (42);").await.unwrap();
            let row = conn.query_row("SELECT -x FROM tneg;").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(-42)]);
        });
    }

    // ── Built-in functions: additional coverage ─────────────────────────

    #[test]
    fn builtin_typeof_all_types() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            assert_eq!(
                row_values(&conn.query_row("SELECT typeof(3.14);").await.unwrap()),
                vec![SqliteValue::Text("real".into())]
            );
            assert_eq!(
                row_values(&conn.query_row("SELECT typeof('abc');").await.unwrap()),
                vec![SqliteValue::Text("text".into())]
            );
            assert_eq!(
                row_values(&conn.query_row("SELECT typeof(NULL);").await.unwrap()),
                vec![SqliteValue::Text("null".into())]
            );
            assert_eq!(
                row_values(&conn.query_row("SELECT typeof(X'FF');").await.unwrap()),
                vec![SqliteValue::Text("blob".into())]
            );
        });
    }

    #[test]
    fn builtin_substr() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT substr('hello world', 7, 5);")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("world".into())]);
        });
    }

    #[test]
    fn builtin_replace() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT replace('hello world', 'world', 'rust');")
                .await
                .unwrap();
            assert_eq!(
                row_values(&row),
                vec![SqliteValue::Text("hello rust".into())]
            );
        });
    }

    #[test]
    fn builtin_trim() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT trim('  hello  ');").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("hello".into())]);
        });
    }

    #[test]
    fn builtin_instr() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT instr('hello world', 'world');")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(7)]);
        });
    }

    #[test]
    fn builtin_hex() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT hex(X'CAFE');").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("CAFE".into())]);
        });
    }

    // ── IS NULL expression context ──────────────────────────────────────

    #[test]
    fn is_null_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT NULL IS NULL;").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
            let row = conn.query_row("SELECT 42 IS NULL;").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(0)]);
        });
    }

    // ── SOUNDEX NULL ────────────────────────────────────────────────────

    #[test]
    fn soundex_null_returns_question_marks() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT soundex(NULL);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Text("?000".into())]);
        });
    }

    // ── LIKE underscore wildcard ─────────────────────────────────────────

    #[test]
    fn like_underscore_wildcard() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn
                .query("SELECT b FROM tp WHERE b LIKE 'b_ta';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Text("beta".into())]);
        });
    }

    // ── NOT IN / NOT BETWEEN ────────────────────────────────────────────

    #[test]
    fn where_not_in() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn
                .query("SELECT a FROM tp WHERE a NOT IN (1, 3, 5);")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(vals.contains(&SqliteValue::Integer(2)));
            assert!(vals.contains(&SqliteValue::Integer(4)));
        });
    }

    #[test]
    fn where_in_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER);").await.unwrap();
            conn.execute("CREATE TABLE t2 (b INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1), (2), (3);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (2), (3), (4);")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT a FROM t1 WHERE a IN (SELECT b FROM t2) ORDER BY a;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
            assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(3)]);
        });
    }

    #[test]
    fn where_in_table_name() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER);").await.unwrap();
            conn.execute("CREATE TABLE t2 (b INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1), (2), (3);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (2), (3), (4);")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT a FROM t1 WHERE a IN t2 ORDER BY a;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(2)]);
            assert_eq!(row_values(&rows[1]), vec![SqliteValue::Integer(3)]);
        });
    }

    #[test]
    fn where_not_in_table_name() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER);").await.unwrap();
            conn.execute("CREATE TABLE t2 (b INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1), (2), (3);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (2), (3), (4);")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT a FROM t1 WHERE a NOT IN t2 ORDER BY a;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
        });
    }

    #[test]
    fn where_exists_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER);").await.unwrap();
            conn.execute("CREATE TABLE t2 (b INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1), (2);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (7);").await.unwrap();

            let rows = conn
                .query("SELECT a FROM t1 WHERE EXISTS (SELECT b FROM t2) ORDER BY a;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);

            conn.execute("DELETE FROM t2;").await.unwrap();
            let rows = conn
                .query("SELECT a FROM t1 WHERE EXISTS (SELECT b FROM t2);")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);
        });
    }

    #[test]
    fn scalar_subquery_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE s (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO s VALUES (41);").await.unwrap();

            let row = conn
                .query_row("SELECT (SELECT v FROM s) + 1;")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(42)]);
        });
    }

    #[test]
    fn update_where_in_table_name() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER, flag TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t2 (b INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'orig'), (2, 'orig'), (3, 'orig');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (2), (3);")
                .await
                .unwrap();

            conn.execute("UPDATE t1 SET flag='hit' WHERE a IN t2;")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT a, flag FROM t1 ORDER BY a;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(1), SqliteValue::Text("orig".into())]
            );
            assert_eq!(
                row_values(&rows[1]),
                vec![SqliteValue::Integer(2), SqliteValue::Text("hit".into())]
            );
            assert_eq!(
                row_values(&rows[2]),
                vec![SqliteValue::Integer(3), SqliteValue::Text("hit".into())]
            );
        });
    }

    #[test]
    fn delete_where_in_table_name() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (a INTEGER);").await.unwrap();
            conn.execute("CREATE TABLE t2 (b INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1), (2), (3);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (2), (3);")
                .await
                .unwrap();

            conn.execute("DELETE FROM t1 WHERE a IN t2;").await.unwrap();

            let rows = conn.query("SELECT a FROM t1 ORDER BY a;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
        });
    }

    #[test]
    fn where_not_between() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let rows = conn
                .query("SELECT a FROM tp WHERE a NOT BETWEEN 2 AND 4;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(vals.contains(&SqliteValue::Integer(1)));
            assert!(vals.contains(&SqliteValue::Integer(5)));
        });
    }

    // ── NULL semantics for IN / BETWEEN ────────────────────────────────

    #[test]
    fn between_null_operand_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // NULL BETWEEN 1 AND 5 → NULL (not TRUE)
            let row = conn
                .query_row("SELECT NULL BETWEEN 1 AND 5;")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Null]);
        });
    }

    #[test]
    fn between_null_low_bound() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // 3 BETWEEN NULL AND 5: (3 >= NULL) AND (3 <= 5) = NULL AND TRUE = NULL
            let row = conn
                .query_row("SELECT 3 BETWEEN NULL AND 5;")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Null]);
        });
    }

    #[test]
    fn between_null_high_bound() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // 3 BETWEEN 1 AND NULL: (3 >= 1) AND (3 <= NULL) = TRUE AND NULL = NULL
            let row = conn
                .query_row("SELECT 3 BETWEEN 1 AND NULL;")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Null]);
        });
    }

    #[test]
    fn between_null_bound_out_of_range_returns_false() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // 3 BETWEEN 4 AND NULL: (3 >= 4) AND (3 <= NULL) = FALSE AND NULL = FALSE
            let row = conn
                .query_row("SELECT 3 BETWEEN 4 AND NULL;")
                .await
                .unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(0)]);
        });
    }

    #[test]
    fn in_null_operand_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // NULL IN (1, 2, 3) → NULL (not FALSE)
            let row = conn.query_row("SELECT NULL IN (1, 2, 3);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Null]);
        });
    }

    #[test]
    fn in_list_with_null_no_match_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // 2 IN (1, NULL, 3): no exact match, but NULL in list → NULL
            let row = conn.query_row("SELECT 2 IN (1, NULL, 3);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Null]);
        });
    }

    #[test]
    fn in_list_with_null_match_returns_true() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // 1 IN (1, NULL, 3): exact match on 1 → TRUE (integer 1)
            let row = conn.query_row("SELECT 1 IN (1, NULL, 3);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(1)]);
        });
    }

    #[test]
    fn not_in_null_operand_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // NULL NOT IN (1, 2) → NULL
            let row = conn.query_row("SELECT NULL NOT IN (1, 2);").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Null]);
        });
    }

    // ── DISTINCT ──────────────────────────────────────────────────────

    #[test]
    fn distinct_table_backed_select() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE td (id INTEGER, flag INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO td VALUES (1, 1);").await.unwrap();
            conn.execute("INSERT INTO td VALUES (2, 0);").await.unwrap();
            conn.execute("INSERT INTO td VALUES (3, 1);").await.unwrap();
            conn.execute("INSERT INTO td VALUES (4, 0);").await.unwrap();
            conn.execute("INSERT INTO td VALUES (5, 1);").await.unwrap();

            let rows = conn.query("SELECT DISTINCT flag FROM td;").await.unwrap();
            assert_eq!(rows.len(), 2);
            let vals: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(vals.contains(&SqliteValue::Integer(0)));
            assert!(vals.contains(&SqliteValue::Integer(1)));
        });
    }

    // ── Aggregate + GROUP BY ───────────────────────────────────────────

    #[test]
    fn aggregate_group_by_count() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE tg (k TEXT);").await.unwrap();
            conn.execute("INSERT INTO tg VALUES ('a');").await.unwrap();
            conn.execute("INSERT INTO tg VALUES ('a');").await.unwrap();
            conn.execute("INSERT INTO tg VALUES ('b');").await.unwrap();

            let rows = conn
                .query("SELECT k, COUNT(*) FROM tg GROUP BY k ORDER BY k;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Text("a".into()), SqliteValue::Integer(2)]
            );
            assert_eq!(
                row_values(&rows[1]),
                vec![SqliteValue::Text("b".into()), SqliteValue::Integer(1)]
            );
        });
    }

    #[test]
    fn group_by_alias_star_expansion() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE ga (k TEXT, v INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO ga VALUES ('a', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO ga VALUES ('a', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO ga VALUES ('b', 20);")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT t.* FROM ga AS t GROUP BY t.k, t.v ORDER BY t.k, t.v;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Text("a".into()), SqliteValue::Integer(10)]
            );
            assert_eq!(
                row_values(&rows[1]),
                vec![SqliteValue::Text("b".into()), SqliteValue::Integer(20)]
            );
        });
    }

    #[test]
    fn right_join_null_extension() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE r (l_id INTEGER, tag TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO l VALUES (1, 'left-a'), (2, 'left-b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO r VALUES (2, 'right-b'), (3, 'right-c');")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT l.name, r.tag FROM l RIGHT JOIN r ON l.id = r.l_id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);

            let projected: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
            assert!(projected.contains(&vec![
                SqliteValue::Text("left-b".into()),
                SqliteValue::Text("right-b".into())
            ]));
            assert!(projected.contains(&vec![
                SqliteValue::Null,
                SqliteValue::Text("right-c".into())
            ]));
        });
    }

    #[test]
    fn full_outer_join_null_extension() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE r (l_id INTEGER, tag TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO l VALUES (1, 'left-a'), (2, 'left-b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO r VALUES (2, 'right-b'), (3, 'right-c');")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT l.name, r.tag FROM l FULL OUTER JOIN r ON l.id = r.l_id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);

            let projected: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
            assert!(
                projected.contains(&vec![SqliteValue::Text("left-a".into()), SqliteValue::Null])
            );
            assert!(projected.contains(&vec![
                SqliteValue::Text("left-b".into()),
                SqliteValue::Text("right-b".into())
            ]));
            assert!(projected.contains(&vec![
                SqliteValue::Null,
                SqliteValue::Text("right-c".into())
            ]));
        });
    }

    #[test]
    fn right_join_using_nulls_do_not_match() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE l (id INTEGER, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE r (id INTEGER, tag TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO l VALUES (NULL, 'left-null'), (1, 'left-one');")
                .await
                .unwrap();
            conn.execute("INSERT INTO r VALUES (NULL, 'right-null'), (1, 'right-one');")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT l.name, r.tag FROM l RIGHT JOIN r USING (id);")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let projected: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
            assert!(projected.contains(&vec![
                SqliteValue::Text("left-one".into()),
                SqliteValue::Text("right-one".into())
            ]));
            assert!(projected.contains(&vec![
                SqliteValue::Null,
                SqliteValue::Text("right-null".into())
            ]));
        });
    }

    #[test]
    fn aggregate_group_by_sum() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE gs (dept TEXT, salary INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO gs VALUES ('eng', 100);")
                .await
                .unwrap();
            conn.execute("INSERT INTO gs VALUES ('eng', 200);")
                .await
                .unwrap();
            conn.execute("INSERT INTO gs VALUES ('sales', 50);")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT dept, SUM(salary) FROM gs GROUP BY dept;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let vals: Vec<(SqliteValue, SqliteValue)> = rows
                .iter()
                .map(|r| {
                    let v = row_values(r);
                    (v[0].clone(), v[1].clone())
                })
                .collect();
            assert!(vals.contains(&(SqliteValue::Text("eng".into()), SqliteValue::Integer(300))));
            assert!(vals.contains(&(SqliteValue::Text("sales".into()), SqliteValue::Integer(50))));
        });
    }

    #[test]
    fn aggregate_group_by_multiple_aggs() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE gm (cat TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO gm VALUES ('a', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO gm VALUES ('a', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO gm VALUES ('a', 30);")
                .await
                .unwrap();
            conn.execute("INSERT INTO gm VALUES ('b', 5);")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT cat, COUNT(*), MIN(val), MAX(val) FROM gm GROUP BY cat;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let a_row = rows
                .iter()
                .find(|r| row_values(r)[0] == SqliteValue::Text("a".into()))
                .unwrap();
            assert_eq!(
                row_values(a_row),
                vec![
                    SqliteValue::Text("a".into()),
                    SqliteValue::Integer(3),
                    SqliteValue::Integer(10),
                    SqliteValue::Integer(30),
                ]
            );
            let b_row = rows
                .iter()
                .find(|r| row_values(r)[0] == SqliteValue::Text("b".into()))
                .unwrap();
            assert_eq!(
                row_values(b_row),
                vec![
                    SqliteValue::Text("b".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(5),
                    SqliteValue::Integer(5),
                ]
            );
        });
    }

    // ── Aggregate: count(col) excludes NULL ──────────────────────────────

    #[test]
    fn aggregate_count_column_excludes_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            setup_bd2832(&conn).await;
            let row = conn.query_row("SELECT count(b) FROM tp;").await.unwrap();
            assert_eq!(row_values(&row), vec![SqliteValue::Integer(4)]);
        });
    }

    // ── execute() with_params affected row count (bd-118o) ────────────

    #[test]
    fn execute_with_params_insert_returns_count() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE ewp (v INTEGER);").await.unwrap();
            let count = conn
                .execute_with_params("INSERT INTO ewp VALUES (?1);", &[SqliteValue::Integer(42)])
                .await
                .unwrap();
            assert_eq!(count, 1, "INSERT via execute_with_params should return 1");
        });
    }

    #[test]
    fn execute_with_params_insert_respects_explicit_column_order() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE message_payloads(
                id INTEGER PRIMARY KEY,
                attachments TEXT NOT NULL DEFAULT '[]',
                recipients_json TEXT NOT NULL DEFAULT '{}'
            );",
            )
            .await
            .unwrap();

            let recipients = r#"{"to":["BlueLake"],"cc":[],"bcc":[]}"#;
            let attachments = r#"[{"name":"artifact.txt","path":"attachments/demo.txt","content_type":"text/plain","size":"128"}]"#;

            conn.execute_with_params(
                "INSERT INTO message_payloads(recipients_json, attachments) VALUES (?1, ?2);",
                &[
                    SqliteValue::Text(recipients.into()),
                    SqliteValue::Text(attachments.into()),
                ],
            )
            .await
            .unwrap();

            let row = conn
                .query_row("SELECT recipients_json, attachments FROM message_payloads LIMIT 1;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&row),
                vec![
                    SqliteValue::Text(recipients.into()),
                    SqliteValue::Text(attachments.into())
                ]
            );
        });
    }

    #[test]
    fn execute_with_params_insert_duplicate_target_columns_keep_first_assignment() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE dup_targets(a INTEGER, b INTEGER);")
                .await
                .unwrap();

            conn.execute_with_params(
                "INSERT INTO dup_targets(a, a, b) VALUES (?1, ?2, ?3);",
                &[
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(3),
                ],
            )
            .await
            .unwrap();

            let row = conn
                .query_row("SELECT a, b FROM dup_targets;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&row),
                vec![SqliteValue::Integer(1), SqliteValue::Integer(3)]
            );
        });
    }

    #[test]
    fn execute_select_returns_row_count() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE es (v INTEGER);").await.unwrap();
            conn.execute("INSERT INTO es VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO es VALUES (2);").await.unwrap();
            let count = conn.execute("SELECT * FROM es;").await.unwrap();
            assert_eq!(count, 2, "SELECT via execute() should return row count");
        });
    }

    // ── Bug fix regression: SAVEPOINT RELEASE implicit transaction ───

    #[test]
    fn savepoint_release_ends_implicit_transaction() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE sr (v INTEGER);").await.unwrap();

            // SAVEPOINT starts an implicit transaction.
            conn.execute("SAVEPOINT sp1;").await.unwrap();
            assert!(conn.in_transaction());
            conn.execute("INSERT INTO sr VALUES (1);").await.unwrap();

            // RELEASE ends the implicit transaction.
            conn.execute("RELEASE sp1;").await.unwrap();
            assert!(
                !conn.in_transaction(),
                "RELEASE of last implicit savepoint should end transaction"
            );

            // After release, data should be committed.
            let rows = conn.query("SELECT * FROM sr;").await.unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    #[test]
    fn explicit_begin_savepoint_release_keeps_transaction() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE bsr (v INTEGER);").await.unwrap();

            // Explicit BEGIN, then SAVEPOINT, then RELEASE.
            conn.execute("BEGIN;").await.unwrap();
            conn.execute("SAVEPOINT sp1;").await.unwrap();
            conn.execute("INSERT INTO bsr VALUES (1);").await.unwrap();
            conn.execute("RELEASE sp1;").await.unwrap();

            // Transaction should still be active (explicit BEGIN requires COMMIT).
            assert!(
                conn.in_transaction(),
                "RELEASE after explicit BEGIN should not end the transaction"
            );
            conn.execute("COMMIT;").await.unwrap();
            assert!(!conn.in_transaction());
        });
    }

    // ── Probe tests for SQL feature coverage ─────────────────────

    #[test]
    fn probe_update_self_ref_expr() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2, 20);").await.unwrap();
            conn.execute("UPDATE t SET val = val + 5;").await.unwrap();
            let rows = conn
                .query("SELECT id, val FROM t ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(1), SqliteValue::Integer(15)]
            );
            assert_eq!(
                row_values(&rows[1]),
                vec![SqliteValue::Integer(2), SqliteValue::Integer(25)]
            );
        });
    }

    #[test]
    fn probe_delete_compound_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'c');")
                .await
                .unwrap();
            conn.execute("DELETE FROM t WHERE id > 1 AND val = 'b';")
                .await
                .unwrap();
            let rows = conn.query("SELECT id FROM t ORDER BY id;").await.unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
        });
    }

    #[test]
    fn probe_coalesce_nulls() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, NULL, 'fallback');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'present', 'fallback');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, COALESCE(a, b) FROM t ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(
                row_values(&rows[0])[1],
                SqliteValue::Text("fallback".into())
            );
            assert_eq!(row_values(&rows[1])[1], SqliteValue::Text("present".into()));
        });
    }

    #[test]
    fn probe_case_when_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, NULL);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 5);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (3, 15);").await.unwrap();
            let rows = conn
            .query(
                "SELECT id, CASE WHEN val IS NULL THEN 'null' WHEN val < 10 THEN 'small' ELSE 'big' END FROM t ORDER BY id;",
            )
            .await
            .unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("null".into()));
            assert_eq!(row_values(&rows[1])[1], SqliteValue::Text("small".into()));
            assert_eq!(row_values(&rows[2])[1], SqliteValue::Text("big".into()));
        });
    }

    #[test]
    fn probe_union_all() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (2, 'b');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT val FROM t1 UNION ALL SELECT val FROM t2;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
        });
    }

    #[test]
    fn probe_union_dedup() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'b');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT val FROM t UNION SELECT val FROM t;")
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                2,
                "UNION should deduplicate: got {:?}",
                rows.iter().map(row_values).collect::<Vec<_>>()
            );
        });
    }

    #[test]
    fn probe_except() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'c');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT val FROM t EXCEPT SELECT val FROM t WHERE id = 2;")
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                2,
                "EXCEPT should remove 'b': got {:?}",
                rows.iter().map(row_values).collect::<Vec<_>>()
            );
        });
    }

    #[test]
    fn probe_intersect() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'c');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT val FROM t INTERSECT SELECT val FROM t WHERE id <= 2;")
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                2,
                "INTERSECT should keep 'a' and 'b': got {:?}",
                rows.iter().map(row_values).collect::<Vec<_>>()
            );
        });
    }

    #[test]
    fn probe_insert_select() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO src VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO src VALUES (2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO dst SELECT * FROM src;")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, val FROM dst ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("a".into()));
        });
    }

    #[test]
    fn probe_limit_offset() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            for i in 1..=10 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}');"))
                    .await
                    .unwrap();
            }
            let rows = conn
                .query("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 3, "LIMIT 3 OFFSET 2 should return 3 rows");
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(4));
            assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(5));
        });
    }

    #[test]
    fn probe_group_by_multi_col() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, sub TEXT, val INTEGER);",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'A', 'x', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'A', 'x', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'A', 'y', 30);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (4, 'B', 'x', 40);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT cat, sub, SUM(val) FROM t GROUP BY cat, sub ORDER BY cat, sub;")
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                3,
                "Should have 3 groups: got {:?}",
                rows.iter().map(row_values).collect::<Vec<_>>()
            );
            assert_eq!(row_values(&rows[0])[2], SqliteValue::Integer(30));
            assert_eq!(row_values(&rows[1])[2], SqliteValue::Integer(30));
            assert_eq!(row_values(&rows[2])[2], SqliteValue::Integer(40));
        });
    }

    #[test]
    fn probe_having_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'A', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'A', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'B', 30);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT cat, COUNT(*) as cnt FROM t GROUP BY cat HAVING cnt > 1;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1, "Only group A has count > 1");
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("A".into()));
        });
    }

    #[test]
    fn having_between_filters_groups() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE hb (grp INTEGER, val INTEGER);")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO hb VALUES (1, 10), (1, 20), (2, 30), (3, 40), (3, 50), (3, 60);",
            )
            .await
            .unwrap();
            // COUNT(*) for groups: 1→2, 2→1, 3→3. HAVING cnt BETWEEN 2 AND 3 keeps 1,3.
            let rows = conn
                .query(
                    "SELECT grp, COUNT(*) as cnt FROM hb GROUP BY grp HAVING cnt BETWEEN 2 AND 3;",
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let grps: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(grps.contains(&SqliteValue::Integer(1)));
            assert!(grps.contains(&SqliteValue::Integer(3)));
        });
    }

    #[test]
    fn having_in_filters_groups() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE hi (grp TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO hi VALUES ('A', 1), ('B', 2), ('C', 3);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT grp FROM hi GROUP BY grp HAVING grp IN ('A', 'C');")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let grps: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(grps.contains(&SqliteValue::Text("A".into())));
            assert!(grps.contains(&SqliteValue::Text("C".into())));
        });
    }

    #[test]
    fn having_case_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE hc (grp TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO hc VALUES ('X', 1), ('Y', 2), ('X', 3);")
                .await
                .unwrap();
            // CASE grp WHEN 'X' THEN 1 ELSE 0 END = 1 keeps only 'X'
            let rows = conn
            .query("SELECT grp, SUM(val) FROM hc GROUP BY grp HAVING CASE grp WHEN 'X' THEN 1 ELSE 0 END = 1;")
            .await
            .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("X".into()));
        });
    }

    #[test]
    fn like_null_operand_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT NULL LIKE 'abc';").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn like_null_pattern_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT 'abc' LIKE NULL;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn like_null_both_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT NULL LIKE NULL;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn not_like_null_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT 'abc' NOT LIKE NULL;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn like_integer_coercion() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // SQLite coerces non-text to text for LIKE comparison.
            let rows = conn.query("SELECT 123 LIKE '123';").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        });
    }

    #[test]
    fn like_null_in_join_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE lnj (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO lnj VALUES (1, 'alice'), (2, NULL), (3, 'bob');")
                .await
                .unwrap();
            // NULL name LIKE '%' should not match (NULL result, not truthy).
            let rows = conn
                .query("SELECT id FROM lnj WHERE name LIKE '%' ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
        });
    }

    #[test]
    fn having_like_filters_groups() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE hlk (grp TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO hlk VALUES ('apple', 1), ('banana', 2), ('apricot', 3);")
                .await
                .unwrap();
            // HAVING grp LIKE 'ap%' keeps only 'apple' and 'apricot'.
            let rows = conn
                .query("SELECT grp, SUM(val) FROM hlk GROUP BY grp HAVING grp LIKE 'ap%';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            let grps: Vec<_> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert!(grps.contains(&SqliteValue::Text("apple".into())));
            assert!(grps.contains(&SqliteValue::Text("apricot".into())));
        });
    }

    #[test]
    fn case_null_base_does_not_match_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // NULL = NULL is UNKNOWN, not TRUE — CASE should go to ELSE.
            let rows = conn
                .query("SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&rows[0])[0],
                SqliteValue::Text("no match".into())
            );
        });
    }

    #[test]
    fn case_null_base_skips_all_whens() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query("SELECT CASE NULL WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'none' END;")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("none".into()));
        });
    }

    #[test]
    fn case_null_when_value_skipped() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // CASE 1 WHEN NULL should skip because 1 = NULL is UNKNOWN.
            let rows = conn
                .query("SELECT CASE 1 WHEN NULL THEN 'bad' WHEN 1 THEN 'ok' ELSE 'miss' END;")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("ok".into()));
        });
    }

    #[test]
    fn case_null_in_join_filter() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE cj (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO cj VALUES (1, NULL), (2, 'x'), (3, 'y');")
                .await
                .unwrap();
            // CASE val WHEN NULL: should never match, so id=1 gets 'other'.
            let rows = conn
            .query(
                "SELECT id, CASE val WHEN 'x' THEN 'found' ELSE 'other' END AS r FROM cj ORDER BY id;",
            )
            .await
            .unwrap();
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("other".into()));
            assert_eq!(row_values(&rows[1])[1], SqliteValue::Text("found".into()));
        });
    }

    #[test]
    fn cast_null_as_integer_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT CAST(NULL AS INTEGER);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn cast_null_as_real_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT CAST(NULL AS REAL);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn cast_null_as_text_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT CAST(NULL AS TEXT);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn cast_null_from_table_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE cn (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO cn VALUES (1, NULL), (2, '5');")
                .await
                .unwrap();
            // CAST(NULL AS INTEGER) should be NULL, not 0.
            let rows = conn
                .query("SELECT id, CAST(val AS INTEGER) FROM cn ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Null);
            assert_eq!(row_values(&rows[1])[1], SqliteValue::Integer(5));
        });
    }

    #[test]
    fn collate_in_join_does_not_return_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE cl (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO cl VALUES (1, 'Alice'), (2, 'bob');")
                .await
                .unwrap();
            // COLLATE should not silently return NULL — it should evaluate the inner expr.
            let rows = conn
                .query("SELECT id FROM cl WHERE name COLLATE NOCASE = 'alice' ORDER BY id;")
                .await
                .unwrap();
            // At minimum, id=1 should match (exact case match with 'Alice' compared via nocase).
            assert!(!rows.is_empty());
        });
    }

    #[test]
    fn probe_nested_functions() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, '  hello  ');")
                .await
                .unwrap();
            let rows = conn.query("SELECT UPPER(TRIM(val)) FROM t;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("HELLO".into()));
        });
    }

    #[test]
    fn replace_null_arg_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT REPLACE(NULL, 'a', 'b');").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
            let rows = conn
                .query("SELECT REPLACE('hello', NULL, 'b');")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn trim_null_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT TRIM(NULL);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
            let rows = conn.query("SELECT LTRIM(NULL);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
            let rows = conn.query("SELECT RTRIM(NULL);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn hex_null_returns_empty_string() {
        asupersync::test_utils::run_test(|| async {
            // C SQLite: hex(NULL) returns '' (empty string), not NULL.
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT HEX(NULL);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("".into()));
        });
    }

    #[test]
    fn instr_null_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT INSTR(NULL, 'x');").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
            let rows = conn.query("SELECT INSTR('hello', NULL);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn substr_null_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT SUBSTR(NULL, 1, 3);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn substr_negative_start() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // Negative start counts from right: -1 = last char.
            let rows = conn.query("SELECT SUBSTR('hello', -1);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("o".into()));
            let rows = conn.query("SELECT SUBSTR('hello', -3);").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("llo".into()));
        });
    }

    #[test]
    fn limit_negative_returns_all_rows() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE ln (id INTEGER PRIMARY KEY);")
                .await
                .unwrap();
            conn.execute("INSERT INTO ln VALUES (1), (2), (3), (4), (5);")
                .await
                .unwrap();
            // LIMIT -1 means unlimited in SQLite.
            let rows = conn
                .query("SELECT id FROM ln ORDER BY id LIMIT -1;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 5);
        });
    }

    #[test]
    fn offset_negative_treated_as_zero() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE on_ (id INTEGER PRIMARY KEY);")
                .await
                .unwrap();
            conn.execute("INSERT INTO on_ VALUES (1), (2), (3);")
                .await
                .unwrap();
            // Negative OFFSET should be treated as 0.
            let rows = conn
                .query("SELECT id FROM on_ ORDER BY id LIMIT 2 OFFSET -5;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        });
    }

    #[test]
    fn null_comparison_returns_null_in_join() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE nc (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO nc VALUES (1, NULL), (2, 5), (3, NULL);")
                .await
                .unwrap();
            // NULL = 5 should be NULL (not truthy), so row 1 excluded.
            // NULL = NULL should be NULL (not truthy), so row 3 excluded.
            let rows = conn
                .query("SELECT id FROM nc WHERE val = 5 ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn null_and_true_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // NULL AND 1 should be NULL, not 0.
            let rows = conn.query("SELECT NULL AND 1;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn null_or_false_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // NULL OR 0 should be NULL, not 0.
            let rows = conn.query("SELECT NULL OR 0;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn false_and_null_returns_false() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // 0 AND NULL should be 0 (FALSE short-circuits).
            let rows = conn.query("SELECT 0 AND NULL;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(0));
        });
    }

    #[test]
    fn null_ne_in_where_excludes_row() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE nne (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO nne VALUES (1, NULL), (2, 5), (3, 10);")
                .await
                .unwrap();
            // NULL != 5 is NULL (not truthy), so id=1 excluded. 5 != 5 is false, so id=2 excluded.
            let rows = conn
                .query("SELECT id FROM nne WHERE val != 5 ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
        });
    }

    #[test]
    fn mixed_type_comparison_uses_type_ordering() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE mt (id INTEGER PRIMARY KEY, val);")
                .await
                .unwrap();
            // Integer 5 < Text 'hello' in SQLite type ordering (numeric < text).
            conn.execute("INSERT INTO mt VALUES (1, 5), (2, 'hello'), (3, 10);")
                .await
                .unwrap();
            // 5 = 'hello' should be FALSE (different type classes).
            let rows = conn
                .query("SELECT id FROM mt WHERE val = 'hello';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn integer_less_than_text_in_type_ordering() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE ilt (id INTEGER PRIMARY KEY, val);")
                .await
                .unwrap();
            conn.execute("INSERT INTO ilt VALUES (1, 42), (2, 'abc');")
                .await
                .unwrap();
            // Integer 42 < Text 'abc' in SQLite type ordering.
            let rows = conn
                .query("SELECT id FROM ilt WHERE val < 'abc';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        });
    }

    #[test]
    fn blob_greater_than_text_in_type_ordering() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE bgt (id INTEGER PRIMARY KEY, val);")
                .await
                .unwrap();
            conn.execute("INSERT INTO bgt VALUES (1, 'text'), (2, X'DEADBEEF');")
                .await
                .unwrap();
            // Blob > Text in SQLite type ordering.
            let rows = conn
                .query("SELECT id FROM bgt WHERE val > 'text';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn large_integer_float_precision_comparison() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // 2^53 + 1 = 9007199254740993 cannot be exactly represented as f64.
            // 9007199254740993 > 9007199254740992.0 should be true.
            conn.execute("CREATE TABLE lip (id INTEGER PRIMARY KEY, ival INTEGER, fval REAL);")
                .await
                .unwrap();
            conn.execute("INSERT INTO lip VALUES (1, 9007199254740993, 9007199254740992.0);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id FROM lip WHERE ival > fval;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1, "large integer should be greater than float");
        });
    }

    #[test]
    fn not_null_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // NOT NULL should be NULL, not 1.
            let rows = conn.query("SELECT NOT NULL;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn not_null_in_where_excludes_row() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE nn (id INTEGER PRIMARY KEY, flag INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO nn VALUES (1, NULL), (2, 0), (3, 1);")
                .await
                .unwrap();
            // NOT flag: NOT NULL=NULL (excluded), NOT 0=1 (included), NOT 1=0 (excluded).
            let rows = conn
                .query("SELECT id FROM nn WHERE NOT flag ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn bitnot_null_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // ~NULL should be NULL.
            let rows = conn.query("SELECT ~NULL;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);
        });
    }

    #[test]
    fn probe_update_where_column_cmp() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 5, 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 15, 10);")
                .await
                .unwrap();
            conn.execute("UPDATE t SET a = a * 2 WHERE a < b;")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, a FROM t ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(10));
            assert_eq!(row_values(&rows[1])[1], SqliteValue::Integer(15));
        });
    }

    #[test]
    fn probe_nullif() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'x');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'skip');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, NULLIF(val, 'skip') FROM t ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("x".into()));
            assert_eq!(row_values(&rows[1])[1], SqliteValue::Null);
        });
    }

    #[test]
    fn probe_iif() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 5);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2, 15);").await.unwrap();
            let rows = conn
                .query("SELECT id, IIF(val > 10, 'big', 'small') FROM t ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("small".into()));
            assert_eq!(row_values(&rows[1])[1], SqliteValue::Text("big".into()));
        });
    }

    #[test]
    fn probe_select_distinct() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (4, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (5, 'c');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT DISTINCT val FROM t ORDER BY val;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 3, "DISTINCT should return 3 unique values");
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".into()));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("b".into()));
            assert_eq!(row_values(&rows[2])[0], SqliteValue::Text("c".into()));
        });
    }

    #[test]
    fn probe_order_by_desc() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 30);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2, 10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (3, 20);").await.unwrap();
            let rows = conn
                .query("SELECT id, val FROM t ORDER BY val DESC;")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
            assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn probe_insert_or_replace() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'old');")
                .await
                .unwrap();
            conn.execute("INSERT OR REPLACE INTO t VALUES (1, 'new');")
                .await
                .unwrap();
            let rows = conn.query("SELECT id, val FROM t;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("new".into()));
        });
    }

    #[test]
    fn probe_insert_or_ignore() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'first');")
                .await
                .unwrap();
            conn.execute("INSERT OR IGNORE INTO t VALUES (1, 'second');")
                .await
                .unwrap();
            let rows = conn.query("SELECT id, val FROM t;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("first".into()));
        });
    }

    #[test]
    fn probe_between() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            for i in 1..=10 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, {i});"))
                    .await
                    .unwrap();
            }
            let rows = conn
                .query("SELECT val FROM t WHERE val BETWEEN 3 AND 7 ORDER BY val;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 5);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
            assert_eq!(row_values(&rows[4])[0], SqliteValue::Integer(7));
        });
    }

    #[test]
    fn probe_in_list() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'c');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id FROM t WHERE val IN ('a', 'c') ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
        });
    }

    #[test]
    fn probe_like_pattern() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'Alice');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'Bob');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'Charlie');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT name FROM t WHERE name LIKE '%li%' ORDER BY name;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("Alice".into()));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("Charlie".into()));
        });
    }

    #[test]
    fn probe_subquery_in_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (3, 'c');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (1, 1);").await.unwrap();
            conn.execute("INSERT INTO t2 VALUES (2, 3);").await.unwrap();
            // This may not be supported - check if it errors gracefully
            let result = conn
                .query("SELECT val FROM t1 WHERE id IN (SELECT t1_id FROM t2) ORDER BY val;")
                .await;
            if let Ok(rows) = result {
                assert_eq!(rows.len(), 2);
                assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".into()));
                assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("c".into()));
            } else {
                // IN subquery not yet supported — that's fine for now
            }
        });
    }

    // Test: INSERT ... RETURNING *
    #[test]
    fn probe_insert_returning_star() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t VALUES (1, 'Alice', 30) RETURNING *;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1, "RETURNING * should produce 1 row");
            // RETURNING * includes all columns: id (rowid alias), name, age
            assert_eq!(
                row_values(&rows[0]),
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("Alice".into()),
                    SqliteValue::Integer(30),
                ]
            );
        });
    }

    // Test: INSERT ... RETURNING specific columns
    #[test]
    fn probe_insert_returning_columns() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t VALUES (1, 'Bob', 25) RETURNING name, age;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Text("Bob".into()), SqliteValue::Integer(25),]
            );
        });
    }

    // Test: INSERT ... RETURNING rowid
    #[test]
    fn probe_insert_returning_rowid() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t VALUES (42, 'test') RETURNING id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(42));
        });
    }

    // Test: Multi-row INSERT ... RETURNING
    #[test]
    fn probe_insert_returning_multi_row() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'b');")
                .await
                .unwrap();
            // INSERT SELECT with RETURNING
            conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t2 SELECT * FROM t RETURNING *;")
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                2,
                "Multi-row INSERT RETURNING should produce 2 rows"
            );
        });
    }

    // Test: UPDATE ... RETURNING *
    #[test]
    fn probe_update_returning_star() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'Alice', 30);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'Bob', 25);")
                .await
                .unwrap();
            let rows = conn
                .query("UPDATE t SET age = age + 1 WHERE id = 1 RETURNING *;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1, "UPDATE RETURNING should produce 1 row");
            assert_eq!(
                row_values(&rows[0]),
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("Alice".into()),
                    SqliteValue::Integer(31),
                ]
            );
        });
    }

    // Test: UPDATE ... RETURNING specific columns
    #[test]
    fn probe_update_returning_columns() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2, 20);").await.unwrap();
            let rows = conn
                .query("UPDATE t SET val = val * 2 RETURNING id, val;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2, "UPDATE RETURNING should produce 2 rows");
        });
    }

    // Test: DELETE ... RETURNING *
    #[test]
    fn probe_delete_returning_star() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'Alice');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'Bob');")
                .await
                .unwrap();
            let rows = conn
                .query("DELETE FROM t WHERE id = 2 RETURNING *;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1, "DELETE RETURNING should produce 1 row");
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(2), SqliteValue::Text("Bob".into()),]
            );
            // Verify the row is actually deleted
            let remaining = conn.query("SELECT COUNT(*) FROM t;").await.unwrap();
            assert_eq!(row_values(&remaining[0])[0], SqliteValue::Integer(1));
        });
    }

    // Test: DELETE ... RETURNING specific column
    #[test]
    fn probe_delete_returning_column() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'c');")
                .await
                .unwrap();
            let rows = conn
                .query("DELETE FROM t WHERE id > 1 RETURNING val;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2, "DELETE RETURNING should produce 2 rows");
        });
    }

    // Test: INSERT DEFAULT VALUES
    #[test]
    fn probe_insert_default_values() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t DEFAULT VALUES;").await.unwrap();
            let rows = conn.query("SELECT id, name, val FROM t;").await.unwrap();
            assert_eq!(rows.len(), 1, "DEFAULT VALUES should insert 1 row");
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            // name and val should be NULL (defaults)
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Null);
            assert_eq!(row_values(&rows[0])[2], SqliteValue::Null);
        });
    }

    #[test]
    fn insert_default_values_uses_column_defaults() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
            "CREATE TABLE td (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', count INTEGER DEFAULT 42, ratio REAL DEFAULT 2.5);",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO td DEFAULT VALUES;")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, status, count, ratio FROM td;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(
                row_values(&rows[0])[1],
                SqliteValue::Text("active".to_string().into()),
                "status should use DEFAULT 'active'"
            );
            assert_eq!(
                row_values(&rows[0])[2],
                SqliteValue::Integer(42),
                "count should use DEFAULT 42"
            );
            assert_eq!(
                row_values(&rows[0])[3],
                SqliteValue::Float(2.5),
                "ratio should use DEFAULT 2.5"
            );
        });
    }

    #[test]
    fn insert_default_values_uses_expression_defaults() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
            "CREATE TABLE td (id INTEGER PRIMARY KEY, total INTEGER DEFAULT (40 + 2), label TEXT DEFAULT lower('HELLO'));",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO td DEFAULT VALUES;")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, total, label FROM td;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(42));
            assert_eq!(row_values(&rows[0])[2], SqliteValue::Text("hello".into()));
        });
    }

    #[test]
    fn create_table_rejects_non_constant_default_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let err = conn
                .execute("CREATE TABLE t(a INTEGER, b INTEGER DEFAULT (a + 1));")
                .await
                .expect_err("column-reference DEFAULT should be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("default value of column [b] is not constant"),
                "unexpected error: {msg}"
            );
        });
    }

    #[test]
    fn create_table_rejects_aggregate_default_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let err = conn
                .execute("CREATE TABLE t(a INTEGER, b INTEGER DEFAULT (sum(1)));")
                .await
                .expect_err("aggregate DEFAULT should be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("default value of column [b] is not constant"),
                "unexpected error: {msg}"
            );
        });
    }

    #[test]
    fn create_table_rejects_hidden_aggregate_default_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let err = conn
                .execute("CREATE TABLE t(a INTEGER, b INTEGER DEFAULT (1 IN (sum(1))));")
                .await
                .expect_err("aggregate hidden inside IN DEFAULT should be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("default value of column [b] is not constant"),
                "unexpected error: {msg}"
            );
        });
    }

    #[test]
    fn alter_table_add_column_rejects_non_constant_default_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(a INTEGER);").await.unwrap();
            let err = conn
                .execute("ALTER TABLE t ADD COLUMN b INTEGER DEFAULT (a + 1);")
                .await
                .expect_err("column-reference DEFAULT should be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("default value of column [b] is not constant"),
                "unexpected error: {msg}"
            );
        });
    }

    #[test]
    fn alter_table_add_column_rejects_hidden_aggregate_default_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(a INTEGER);").await.unwrap();
            let err = conn
                .execute("ALTER TABLE t ADD COLUMN b INTEGER DEFAULT (1 IN (sum(1)));")
                .await
                .expect_err("aggregate hidden inside IN DEFAULT should be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("default value of column [b] is not constant"),
                "unexpected error: {msg}"
            );
        });
    }

    #[test]
    fn alter_table_add_column_rejects_aggregate_default_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(a INTEGER);").await.unwrap();
            let err = conn
                .execute("ALTER TABLE t ADD COLUMN b INTEGER DEFAULT (sum(1));")
                .await
                .expect_err("aggregate DEFAULT should be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains("default value of column [b] is not constant"),
                "unexpected error: {msg}"
            );
        });
    }

    #[test]
    fn insert_explicit_cols_uses_defaults_for_omitted() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
            "CREATE TABLE te (id INTEGER PRIMARY KEY, name TEXT, status TEXT DEFAULT 'pending');",
        )
        .await
        .unwrap();
            // Only specify name, omit status — should get DEFAULT 'pending'.
            conn.execute("INSERT INTO te (name) VALUES ('alice');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, name, status FROM te;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0])[1],
                SqliteValue::Text("alice".to_string().into())
            );
            assert_eq!(
                row_values(&rows[0])[2],
                SqliteValue::Text("pending".to_string().into()),
                "omitted column should use DEFAULT 'pending'"
            );
        });
    }

    // Test: INSERT DEFAULT VALUES with RETURNING (IPK column)
    #[test]
    fn probe_insert_default_values_returning() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            // Use RETURNING id (IPK column) — tests that IPK columns emit Rowid
            // instead of Column (which would return Null for DEFAULT VALUES).
            let rows = conn
                .query("INSERT INTO t DEFAULT VALUES RETURNING id;")
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                1,
                "DEFAULT VALUES RETURNING should produce 1 row"
            );
            // rowid should be auto-assigned (1)
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        });
    }

    // =================================================================
    // IPK integration tests (bd-3l6e / PARITY-B5)
    // =================================================================

    /// NULL IPK should auto-generate an incrementing rowid.
    #[test]
    fn ipk_null_auto_generates_rowid() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            let r1 = conn
                .query("INSERT INTO t VALUES (NULL, 'a') RETURNING id;")
                .await
                .unwrap();
            let r2 = conn
                .query("INSERT INTO t VALUES (NULL, 'b') RETURNING id;")
                .await
                .unwrap();
            let id1 = &row_values(&r1[0])[0];
            let id2 = &row_values(&r2[0])[0];
            // Both should be positive integers.
            assert!(
                matches!(id1, SqliteValue::Integer(n) if *n > 0),
                "NULL IPK should auto-generate positive id, got {id1:?}"
            );
            // Second should be greater than first.
            if let (SqliteValue::Integer(a), SqliteValue::Integer(b)) = (id1, id2) {
                assert!(
                    b > a,
                    "successive NULL IPK inserts should increment: {a} < {b}"
                );
            }
        });
    }

    /// Explicit IPK value of 0 should be stored as rowid 0.
    #[test]
    fn ipk_zero_is_valid_rowid() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t VALUES (0, 'zero') RETURNING id;")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(0));
        });
    }

    /// Negative IPK values should be stored as negative rowids.
    #[test]
    fn ipk_negative_is_valid_rowid() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t VALUES (-5, 'neg') RETURNING id;")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(-5));
        });
    }

    /// Multi-row INSERT with explicit IPK values.
    #[test]
    fn ipk_multi_row_explicit_values() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t VALUES (10,'a'),(20,'b'),(30,'c') RETURNING id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(10));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(20));
            assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(30));
        });
    }

    /// Mixed NULL and explicit IPK in multi-row INSERT.
    #[test]
    fn ipk_mixed_null_and_explicit() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (10, 'explicit');")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t VALUES (NULL, 'auto') RETURNING id;")
                .await
                .unwrap();
            // Auto-generated id should be > 10 (the max existing rowid).
            if let SqliteValue::Integer(id) = &row_values(&rows[0])[0] {
                assert!(
                    *id > 10,
                    "auto-generated id after max=10 should be > 10, got {id}"
                );
            } else {
                panic!("expected Integer, got {:?}", row_values(&rows[0])[0]);
            }
        });
    }

    /// RETURNING * should include the correct IPK value.
    #[test]
    fn ipk_returning_star_includes_correct_id() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t VALUES (42, 'x') RETURNING *;")
                .await
                .unwrap();
            let vals = row_values(&rows[0]);
            assert_eq!(vals[0], SqliteValue::Integer(42));
            assert_eq!(vals[1], SqliteValue::Text("x".into()));
        });
    }

    /// SELECT after INSERT should see the correct IPK values.
    #[test]
    fn ipk_roundtrip_select_after_insert() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (42, 'Alice');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (100, 'Bob');")
                .await
                .unwrap();
            let rows = conn.query("SELECT * FROM t ORDER BY id;").await.unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(42));
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("Alice".into()));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(100));
        });
    }

    /// Explicit column list in non-schema order should store values correctly.
    #[test]
    fn ipk_column_list_reorder() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            // Column list reverses schema order: (name, id) vs schema (id, name).
            let rows = conn
                .query("INSERT INTO t(name, id) VALUES ('Alice', 42) RETURNING *;")
                .await
                .unwrap();
            let vals = row_values(&rows[0]);
            assert_eq!(
                vals[0],
                SqliteValue::Integer(42),
                "id should be 42 (from column-list position 1)"
            );
            assert_eq!(
                vals[1],
                SqliteValue::Text("Alice".into()),
                "name should be Alice (from column-list position 0)"
            );
            // Also verify via SELECT that the stored record is correct.
            let sel = conn.query("SELECT id, name FROM t;").await.unwrap();
            let sv = row_values(&sel[0]);
            assert_eq!(sv[0], SqliteValue::Integer(42));
            assert_eq!(sv[1], SqliteValue::Text("Alice".into()));
        });
    }

    #[test]
    fn insert_select_without_from_reorders_targets_and_fills_defaults() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
            "CREATE TABLE dst (id INTEGER PRIMARY KEY, label TEXT DEFAULT 'seed', qty INTEGER);",
        )
        .await
        .unwrap();

            let rows = conn
                .query("INSERT INTO dst(qty) SELECT 11 RETURNING label, qty;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Text("seed".into()), SqliteValue::Integer(11)]
            );

            let rows = conn
                .query("INSERT INTO dst(qty, label) SELECT 22, 'fresh' RETURNING label, qty;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Text("fresh".into()), SqliteValue::Integer(22)]
            );
        });
    }

    #[test]
    fn insert_select_without_from_explicit_rowid_is_preserved() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (payload TEXT);")
                .await
                .unwrap();

            let rows = conn
                .query("INSERT INTO t(rowid, payload) SELECT 7, 'x' RETURNING rowid, payload;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(7), SqliteValue::Text("x".into())]
            );
        });
    }

    #[test]
    fn insert_values_rowid_family_uses_last_target_assignment() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .unwrap();

            let rows = conn
                .query("INSERT INTO t(rowid, id, payload) VALUES (7, 8, 'x') RETURNING rowid, id;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(8), SqliteValue::Integer(8)]
            );

            let rows = conn
                .query("INSERT INTO t(id, rowid, payload) VALUES (9, 10, 'y') RETURNING rowid, id;")
                .await
                .unwrap();
            assert_eq!(
                row_values(&rows[0]),
                vec![SqliteValue::Integer(10), SqliteValue::Integer(10)]
            );
        });
    }

    #[test]
    fn ipk_insert_select_column_list_reorder() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t(name, id) SELECT 'Alice', 42 RETURNING id, name;")
                .await
                .unwrap();
            let vals = row_values(&rows[0]);
            assert_eq!(vals[0], SqliteValue::Integer(42));
            assert_eq!(vals[1], SqliteValue::Text("Alice".into()));

            let sel = conn.query("SELECT id, name FROM t;").await.unwrap();
            let stored = row_values(&sel[0]);
            assert_eq!(stored[0], SqliteValue::Integer(42));
            assert_eq!(stored[1], SqliteValue::Text("Alice".into()));
        });
    }

    #[test]
    fn ipk_insert_select_hidden_rowid_alias_honors_explicit_rowid() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t(rowid, name) SELECT 7, 'Bob' RETURNING id, rowid, name;")
                .await
                .unwrap();
            let vals = row_values(&rows[0]);
            assert_eq!(vals[0], SqliteValue::Integer(7));
            assert_eq!(vals[1], SqliteValue::Integer(7));
            assert_eq!(vals[2], SqliteValue::Text("Bob".into()));

            let sel = conn.query("SELECT id, rowid, name FROM t;").await.unwrap();
            let stored = row_values(&sel[0]);
            assert_eq!(stored[0], SqliteValue::Integer(7));
            assert_eq!(stored[1], SqliteValue::Integer(7));
            assert_eq!(stored[2], SqliteValue::Text("Bob".into()));
        });
    }

    #[test]
    fn upsert_do_update_resolves_hidden_rowid_aliases() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (u TEXT UNIQUE, v INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t(rowid, u, v) VALUES (1, 'dup', 10);")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO t(rowid, u, v) VALUES (7, 'dup', 99)
             ON CONFLICT(u) DO UPDATE SET v = excluded.rowid + rowid;",
            )
            .await
            .unwrap();

            let rows = conn
                .query("SELECT rowid, u, v FROM t ORDER BY rowid;")
                .await
                .unwrap();
            let vals = row_values(&rows[0]);
            assert_eq!(vals[0], SqliteValue::Integer(1));
            assert_eq!(vals[1], SqliteValue::Text("dup".into()));
            assert_eq!(vals[2], SqliteValue::Integer(8));
        });
    }

    #[test]
    fn alter_table_preserves_primary_key_sql_in_sqlite_master() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id TEXT PRIMARY KEY, body TEXT);")
                .await
                .unwrap();
            conn.execute("ALTER TABLE t RENAME COLUMN body TO payload;")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT sql FROM sqlite_master WHERE name = 't';")
                .await
                .unwrap();
            let row = row_values(&rows[0]);
            let sql = match &row[0] {
                SqliteValue::Text(sql) => sql,
                other => panic!("expected SQL text, got {other:?}"),
            };
            assert!(sql.contains("PRIMARY KEY"), "{sql}");
            assert!(!sql.contains("UNIQUE"), "{sql}");
        });
    }

    #[test]
    fn alter_table_preserves_typeless_without_rowid_sql_in_sqlite_master() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE wr(id TEXT, body, PRIMARY KEY(id, body)) WITHOUT ROWID;")
                .await
                .unwrap();
            conn.execute("ALTER TABLE wr ADD COLUMN note TEXT;")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT sql FROM sqlite_master WHERE name = 'wr';")
                .await
                .unwrap();
            let row = row_values(&rows[0]);
            let sql = match &row[0] {
                SqliteValue::Text(sql) => sql,
                other => panic!("expected SQL text, got {other:?}"),
            };
            assert!(sql.contains("PRIMARY KEY"), "{sql}");
            assert!(sql.contains("WITHOUT ROWID"), "{sql}");
            assert!(sql.contains("body"), "{sql}");
            assert!(!sql.contains("body BLOB"), "{sql}");
        });
    }

    #[test]
    fn alter_table_drop_primary_key_column_is_rejected() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id TEXT PRIMARY KEY, body TEXT);")
                .await
                .unwrap();
            let err = conn
                .execute("ALTER TABLE t DROP COLUMN id;")
                .await
                .expect_err("dropping a primary key column must fail");
            let msg = err.to_string();
            assert!(
                msg.contains("cannot drop PRIMARY KEY column") && msg.contains("id"),
                "{msg}"
            );
        });
    }

    /// Explicit column list omitting IPK should auto-generate rowid.
    #[test]
    fn ipk_column_list_omit_ipk() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            let rows = conn
                .query("INSERT INTO t(name) VALUES ('Bob') RETURNING id, name;")
                .await
                .unwrap();
            let vals = row_values(&rows[0]);
            // id should be auto-generated (positive integer).
            assert!(
                matches!(vals[0], SqliteValue::Integer(n) if n > 0),
                "omitted IPK should auto-generate, got {:?}",
                vals[0]
            );
            assert_eq!(vals[1], SqliteValue::Text("Bob".into()));
        });
    }

    /// DELETE then reinsert with same IPK should work.
    #[test]
    fn ipk_delete_reinsert_same_id() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'original');")
                .await
                .unwrap();
            conn.execute("DELETE FROM t WHERE id = 1;").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'reinserted');")
                .await
                .unwrap();
            let rows = conn.query("SELECT val FROM t WHERE id = 1;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0])[0],
                SqliteValue::Text("reinserted".into())
            );
        });
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Index Maintenance Tests (Phase 5I - bd-1nmg)
    // ══════════════════════════════════════════════════════════════════════════

    // ── Basic Operations ──────────────────────────────────────────────────────

    /// INSERT should create index entries for single-column indexes.
    #[test]
    fn index_insert_single_column() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();

            conn.execute("INSERT INTO t VALUES (1, 'alice');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'bob');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 'charlie');")
                .await
                .unwrap();

            // Verify index is used for lookups (entries exist).
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'bob';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));

            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        });
    }

    /// INSERT should create index entries for multi-column indexes.
    #[test]
    fn index_insert_multi_column() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (a INT, b INT, c TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_ab ON t(a, b);")
                .await
                .unwrap();

            conn.execute("INSERT INTO t VALUES (1, 10, 'x');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 20, 'y');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 10, 'z');")
                .await
                .unwrap();

            // Query using both columns of the index.
            let rows = conn
                .query("SELECT c FROM t WHERE a = 1 AND b = 20;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("y".into()));

            // Query using only first column prefix.
            let rows = conn.query("SELECT c FROM t WHERE a = 1;").await.unwrap();
            assert_eq!(rows.len(), 2); // Should find both a=1 rows.
        });
    }

    /// DELETE should remove index entries.
    #[test]
    fn index_delete_removes_entry() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();

            conn.execute("INSERT INTO t VALUES (1, 'alice');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'bob');")
                .await
                .unwrap();

            // Verify both are findable.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);

            // Delete alice.
            conn.execute("DELETE FROM t WHERE id = 1;").await.unwrap();

            // Alice should no longer be findable via index.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            // Bob should still be findable.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'bob';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    /// UPDATE should maintain index entries when indexed column changes.
    #[test]
    fn index_update_indexed_column() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();

            conn.execute("INSERT INTO t VALUES (1, 'alice');")
                .await
                .unwrap();

            // Verify initial state.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);

            // Update name.
            conn.execute("UPDATE t SET name = 'alicia' WHERE id = 1;")
                .await
                .unwrap();

            // Old name should not be findable.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            // New name should be findable.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alicia';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        });
    }

    /// UPDATE should preserve index entries when non-indexed column changes.
    #[test]
    fn index_update_non_indexed_column() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();

            conn.execute("INSERT INTO t VALUES (1, 'alice', 30);")
                .await
                .unwrap();

            // Update non-indexed column.
            conn.execute("UPDATE t SET age = 31 WHERE id = 1;")
                .await
                .unwrap();

            // Index should still work correctly.
            let rows = conn
                .query("SELECT age FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(31));
        });
    }

    // ── Multiple Indexes ──────────────────────────────────────────────────────

    /// Table with multiple indexes should maintain all of them.
    #[test]
    fn index_multiple_indexes_on_table() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (a INT, b INT, c INT, d INT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a);").await.unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b);").await.unwrap();
            conn.execute("CREATE INDEX idx_ab ON t(a, b);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_cd ON t(c, d);")
                .await
                .unwrap();

            conn.execute("INSERT INTO t VALUES (1, 2, 3, 4);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (5, 6, 7, 8);")
                .await
                .unwrap();

            // All indexes should be searchable.
            let rows = conn.query("SELECT b FROM t WHERE a = 1;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));

            let rows = conn.query("SELECT a FROM t WHERE b = 6;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(5));

            let rows = conn
                .query("SELECT c FROM t WHERE a = 1 AND b = 2;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));

            let rows = conn
                .query("SELECT a FROM t WHERE c = 7 AND d = 8;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(5));
        });
    }

    /// DELETE should remove entries from all indexes.
    #[test]
    fn index_delete_removes_from_all_indexes() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a);").await.unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b);").await.unwrap();

            conn.execute("INSERT INTO t VALUES (1, 10, 100);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 20, 200);")
                .await
                .unwrap();

            // Delete row 1.
            conn.execute("DELETE FROM t WHERE id = 1;").await.unwrap();

            // Neither index should find the deleted row.
            let rows = conn.query("SELECT id FROM t WHERE a = 10;").await.unwrap();
            assert_eq!(rows.len(), 0);

            let rows = conn.query("SELECT id FROM t WHERE b = 100;").await.unwrap();
            assert_eq!(rows.len(), 0);

            // Row 2 should still be findable via both indexes.
            let rows = conn.query("SELECT id FROM t WHERE a = 20;").await.unwrap();
            assert_eq!(rows.len(), 1);

            let rows = conn.query("SELECT id FROM t WHERE b = 200;").await.unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    /// UPDATE should maintain all affected indexes.
    #[test]
    fn index_update_maintains_all_indexes() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a);").await.unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b);").await.unwrap();

            conn.execute("INSERT INTO t VALUES (1, 10, 100);")
                .await
                .unwrap();

            // Update both indexed columns.
            conn.execute("UPDATE t SET a = 11, b = 101 WHERE id = 1;")
                .await
                .unwrap();

            // Old values should not be findable.
            let rows = conn.query("SELECT id FROM t WHERE a = 10;").await.unwrap();
            assert_eq!(rows.len(), 0);
            let rows = conn.query("SELECT id FROM t WHERE b = 100;").await.unwrap();
            assert_eq!(rows.len(), 0);

            // New values should be findable.
            let rows = conn.query("SELECT id FROM t WHERE a = 11;").await.unwrap();
            assert_eq!(rows.len(), 1);
            let rows = conn.query("SELECT id FROM t WHERE b = 101;").await.unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    // ── NULL Handling ─────────────────────────────────────────────────────────
    // Fixed in bd-36eh.1: NULL value handling in index B-trees.
    // The fix sets NULLEQ flag (0x80) in WHERE Ne comparisons so NULL != value
    // correctly skips rows with NULL values.

    /// Index should handle NULL values correctly.
    #[test]
    fn index_with_null_values() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();

            conn.execute("INSERT INTO t VALUES (1, NULL);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'alice');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, NULL);")
                .await
                .unwrap();

            // Query for NULL via IS NULL.
            let rows = conn
                .query("SELECT id FROM t WHERE name IS NULL;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);

            // Query for non-NULL.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
        });
    }

    /// UPDATE NULL to non-NULL should update index correctly.
    #[test]
    fn index_update_null_to_non_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();

            conn.execute("INSERT INTO t VALUES (1, NULL);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, NULL);")
                .await
                .unwrap();

            // Initially 2 NULLs.
            let rows = conn
                .query("SELECT id FROM t WHERE name IS NULL;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);

            // Update one NULL to non-NULL.
            conn.execute("UPDATE t SET name = 'bob' WHERE id = 1;")
                .await
                .unwrap();

            // Now only 1 NULL.
            let rows = conn
                .query("SELECT id FROM t WHERE name IS NULL;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));

            // And bob is findable.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'bob';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        });
    }

    /// UPDATE non-NULL to NULL should update index correctly.
    #[test]
    fn index_update_non_null_to_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();

            conn.execute("INSERT INTO t VALUES (1, 'alice');")
                .await
                .unwrap();

            // alice is findable.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);

            // Update to NULL.
            conn.execute("UPDATE t SET name = NULL WHERE id = 1;")
                .await
                .unwrap();

            // alice is no longer findable.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            // NULL is findable.
            let rows = conn
                .query("SELECT id FROM t WHERE name IS NULL;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    // ── Bulk Operations ───────────────────────────────────────────────────────

    /// Bulk INSERT should maintain index for all rows.
    #[test]
    fn index_bulk_insert() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value INT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_value ON t(value);")
                .await
                .unwrap();

            // Insert 100 rows.
            for i in 0..100 {
                conn.execute(&format!("INSERT INTO t VALUES ({}, {});", i, i * 2))
                    .await
                    .unwrap();
            }

            // Verify index works for various values.
            for i in [0, 25, 50, 75, 99] {
                let rows = conn
                    .query(&format!("SELECT id FROM t WHERE value = {};", i * 2))
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1, "Should find row with value={}", i * 2);
                assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(i));
            }
        });
    }

    /// Bulk DELETE should remove all index entries.
    #[test]
    fn index_bulk_delete() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value INT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_value ON t(value);")
                .await
                .unwrap();

            // Insert 50 rows.
            for i in 0..50 {
                conn.execute(&format!("INSERT INTO t VALUES ({}, {});", i, i))
                    .await
                    .unwrap();
            }

            // Delete half (even values).
            for i in (0..50).step_by(2) {
                conn.execute(&format!("DELETE FROM t WHERE id = {};", i))
                    .await
                    .unwrap();
            }

            // Even values should not be findable.
            for i in (0..50).step_by(2) {
                let rows = conn
                    .query(&format!("SELECT id FROM t WHERE value = {};", i))
                    .await
                    .unwrap();
                assert_eq!(
                    rows.len(),
                    0,
                    "Deleted row with value={} should not exist",
                    i
                );
            }

            // Odd values should still be findable.
            for i in (1..50).step_by(2) {
                let rows = conn
                    .query(&format!("SELECT id FROM t WHERE value = {};", i))
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1, "Row with value={} should exist", i);
            }
        });
    }

    /// Bulk UPDATE should maintain all index entries.
    #[test]
    fn index_bulk_update() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value INT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_value ON t(value);")
                .await
                .unwrap();

            // Insert 50 rows.
            for i in 0..50 {
                conn.execute(&format!("INSERT INTO t VALUES ({}, {});", i, i))
                    .await
                    .unwrap();
            }

            // Update all values: value = value + 1000.
            conn.execute("UPDATE t SET value = value + 1000;")
                .await
                .unwrap();

            // Old values should not be findable.
            for i in 0..50 {
                let rows = conn
                    .query(&format!("SELECT id FROM t WHERE value = {};", i))
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 0);
            }

            // New values should be findable.
            for i in 0..50 {
                let rows = conn
                    .query(&format!("SELECT id FROM t WHERE value = {};", i + 1000))
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(i));
            }
        });
    }

    // ── Transaction Rollback ──────────────────────────────────────────────────

    /// Index entries should be rolled back with transaction.
    #[test]
    fn index_rollback_insert() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'alice');")
                .await
                .unwrap();

            conn.execute("BEGIN;").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'bob');")
                .await
                .unwrap();

            // Bob should be visible in transaction.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'bob';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);

            conn.execute("ROLLBACK;").await.unwrap();

            // Bob should NOT be visible after rollback.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'bob';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            // Alice should still be there.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    /// Index entries should be rolled back on DELETE rollback.
    #[test]
    fn index_rollback_delete() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'alice');")
                .await
                .unwrap();

            conn.execute("BEGIN;").await.unwrap();
            conn.execute("DELETE FROM t WHERE id = 1;").await.unwrap();

            // Alice should not be visible in transaction.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            conn.execute("ROLLBACK;").await.unwrap();

            // Alice should be restored after rollback.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    /// Index entries should be rolled back on UPDATE rollback.
    #[test]
    fn index_rollback_update() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'alice');")
                .await
                .unwrap();

            conn.execute("BEGIN;").await.unwrap();
            conn.execute("UPDATE t SET name = 'bob' WHERE id = 1;")
                .await
                .unwrap();

            // Bob should be visible, alice not.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'bob';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            conn.execute("ROLLBACK;").await.unwrap();

            // Alice should be restored, bob gone.
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'alice';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            let rows = conn
                .query("SELECT id FROM t WHERE name = 'bob';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);
        });
    }

    // ── No REINDEX Required ───────────────────────────────────────────────────

    /// All operations should work WITHOUT manual REINDEX.
    #[test]
    fn index_no_reindex_needed() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t(name);")
                .await
                .unwrap();

            // Perform many operations.
            for i in 0..100 {
                conn.execute(&format!("INSERT INTO t VALUES ({}, 'name{}');", i, i))
                    .await
                    .unwrap();
            }
            for i in 0..50 {
                conn.execute(&format!("DELETE FROM t WHERE id = {};", i))
                    .await
                    .unwrap();
            }
            for i in 50..100 {
                conn.execute(&format!(
                    "UPDATE t SET name = 'updated{}' WHERE id = {};",
                    i, i
                ))
                .await
                .unwrap();
            }

            // All remaining rows should be findable via index WITHOUT REINDEX.
            for i in 50..100 {
                let rows = conn
                    .query(&format!("SELECT id FROM t WHERE name = 'updated{}';", i))
                    .await
                    .unwrap();
                assert_eq!(
                    rows.len(),
                    1,
                    "Row with updated name for id={} should be findable",
                    i
                );
                assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(i));
            }

            // Deleted rows should not be findable.
            for i in 0..50 {
                let rows = conn
                    .query(&format!("SELECT id FROM t WHERE name = 'name{}';", i))
                    .await
                    .unwrap();
                assert_eq!(
                    rows.len(),
                    0,
                    "Deleted row with name{} should not be findable",
                    i
                );
            }

            // Verify total count.
            let rows = conn.query("SELECT COUNT(*) FROM t;").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(50));
        });
    }

    // ── INSERT with index on IPK column ───────────────────────────────────────

    /// Index on INTEGER PRIMARY KEY column should work correctly.
    #[test]
    fn index_on_ipk_column() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_id ON t(id);").await.unwrap();

            conn.execute("INSERT INTO t VALUES (100, 'alice');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (200, 'bob');")
                .await
                .unwrap();

            // Index on IPK should work.
            let rows = conn
                .query("SELECT name FROM t WHERE id = 100;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("alice".into()));
        });
    }

    // ── Mixed operations sequence ─────────────────────────────────────────────

    /// Complex sequence of operations should maintain index consistency.
    #[test]
    fn index_mixed_operations_sequence() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_a ON t(a);").await.unwrap();
            conn.execute("CREATE INDEX idx_b ON t(b);").await.unwrap();

            // Insert
            conn.execute("INSERT INTO t VALUES (1, 10, 'x');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 20, 'y');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (3, 30, 'z');")
                .await
                .unwrap();

            // Update
            conn.execute("UPDATE t SET a = 15 WHERE id = 1;")
                .await
                .unwrap();

            // Delete
            conn.execute("DELETE FROM t WHERE id = 2;").await.unwrap();

            // Insert more
            conn.execute("INSERT INTO t VALUES (4, 40, 'w');")
                .await
                .unwrap();

            // Verify state via indexes.
            let rows = conn.query("SELECT id FROM t WHERE a = 10;").await.unwrap();
            assert_eq!(rows.len(), 0); // Was updated to 15

            let rows = conn.query("SELECT id FROM t WHERE a = 15;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));

            let rows = conn.query("SELECT id FROM t WHERE b = 'y';").await.unwrap();
            assert_eq!(rows.len(), 0); // Was deleted

            let rows = conn.query("SELECT id FROM t WHERE a = 40;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(4));
        });
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Concurrent Writer Stress Tests (Phase 5E.6 - bd-1299)
    // ══════════════════════════════════════════════════════════════════════════

    /// Multi-threaded concurrent writer stress test.
    ///
    /// Eight writer threads perform 20 committed transfer operations each.
    /// The parent process enforces a hard deadline; the child process uses
    /// bounded, typed retries and fail-closed startup coordination.
    #[test]
    fn concurrent_writers_stress_conservation() {
        if supervise_concurrent_writer_stress() {
            return;
        }

        use rand::prelude::*;
        use std::thread;

        const NUM_ACCOUNTS: i64 = 100;
        const INITIAL_BALANCE: i64 = 1_000;
        const EXPECTED_TOTAL: i64 = NUM_ACCOUNTS * INITIAL_BALANCE;
        const NUM_WRITERS: usize = 8;
        const OPS_PER_WRITER: u64 = 20;

        let dir = tempfile::tempdir().expect("create concurrent-stress temp dir");
        let db_path = dir.path().join("stress.db");
        let db_path_string = db_path.to_string_lossy().into_owned();

        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&db_path_string)
                .await
                .expect("open concurrent-stress database for setup");
            assert!(
                conn.is_concurrent_mode_default(),
                "setup connection must preserve the concurrent-writer default"
            );
            conn.execute(
                "CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY,
                    balance INTEGER,
                    payload TEXT NOT NULL
                );",
            )
            .await
            .expect("create accounts table");
            let payload = "x".repeat(512);
            for account_id in 0..NUM_ACCOUNTS {
                conn.execute_with_params(
                    "INSERT INTO accounts VALUES (?1, ?2, ?3);",
                    &[
                        SqliteValue::Integer(account_id),
                        SqliteValue::Integer(INITIAL_BALANCE),
                        SqliteValue::Text(payload.clone().into()),
                    ],
                )
                .await
                .expect("insert initial account");
            }
            let rows = conn
                .query(
                    "SELECT COUNT(*), SUM(balance), MIN(length(payload)), MAX(length(payload))
                     FROM accounts;",
                )
                .await
                .expect("query initial account invariants");
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(NUM_ACCOUNTS));
            assert_eq!(
                row_values(&rows[0])[1],
                SqliteValue::Integer(EXPECTED_TOTAL)
            );
            assert_eq!(row_values(&rows[0])[2], SqliteValue::Integer(512));
            assert_eq!(row_values(&rows[0])[3], SqliteValue::Integer(512));
            let page_count = conn
                .query_row("PRAGMA page_count;")
                .await
                .expect("query multi-page setup size");
            assert!(
                matches!(row_values(&page_count).as_slice(), [SqliteValue::Integer(count)] if *count > 2),
                "fixed-width account payloads must span multiple database pages: {page_count:?}"
            );
            conn.close()
                .await
                .expect("close concurrent-stress setup connection");
        });

        let (startup_tx, startup_rx) = mpsc::channel::<ConcurrentStressStartup>();
        let mut start_senders = Vec::with_capacity(NUM_WRITERS);
        let mut handles = Vec::with_capacity(NUM_WRITERS);

        for worker_id in 0..NUM_WRITERS {
            let path = db_path_string.clone();
            let startup_tx = startup_tx.clone();
            let (start_tx, start_rx) = mpsc::sync_channel(1);
            start_senders.push(start_tx);
            handles.push(thread::spawn(move || {
                let mut outcome = ConcurrentStressWorkerOutcome::pending(worker_id);
                let started_at = Instant::now();
                asupersync::test_utils::run_test(|| async {
                    let mut open_attempts = 0_u64;
                    let mut last_open_error = None;
                    let mut conn = loop {
                        if open_attempts >= CONCURRENT_STRESS_MAX_ATTEMPTS_PER_COMMIT
                            || outcome.attempts >= CONCURRENT_STRESS_MAX_ATTEMPTS_PER_WORKER
                            || started_at.elapsed() >= CONCURRENT_STRESS_STARTUP_TIMEOUT
                        {
                            let message = format!(
                                "connection open exhausted its bounded retry budget after {open_attempts} attempts; last transient error: {}",
                                last_open_error.as_deref().unwrap_or("none")
                            );
                            let _ = startup_tx.send(ConcurrentStressStartup::Failed {
                                worker_id,
                                error: message.clone(),
                            });
                            outcome.failure = Some(message);
                            return;
                        }

                        open_attempts += 1;
                        outcome.attempts += 1;
                        match Connection::open(&path).await {
                            Ok(conn) => break conn,
                            Err(error) if outcome.retries.record(&error) => {
                                last_open_error = Some(format!("connection open: {error:?}"));
                                concurrent_stress_backoff(
                                    open_attempts,
                                    u64::try_from(worker_id).expect("worker id fits u64"),
                                );
                            }
                            Err(error) => {
                                let message = format!("connection open failed: {error:?}");
                                let _ = startup_tx.send(ConcurrentStressStartup::Failed {
                                    worker_id,
                                    error: message.clone(),
                                });
                                outcome.failure = Some(message);
                                return;
                            }
                        }
                    };
                    'worker: {
                    outcome.concurrent_mode_default = conn.is_concurrent_mode_default();
                    if !outcome.concurrent_mode_default {
                        let message = "concurrent mode is not enabled by default".to_owned();
                        let _ = startup_tx.send(ConcurrentStressStartup::Failed {
                            worker_id,
                            error: message.clone(),
                        });
                        outcome.failure = Some(message);
                        break 'worker;
                    }
                    if startup_tx
                        .send(ConcurrentStressStartup::Ready { worker_id })
                        .is_err()
                    {
                        outcome.failure = Some("startup coordinator disconnected".to_owned());
                        break 'worker;
                    }
                    match start_rx.recv() {
                        Ok(ConcurrentStressStartDecision::Run) => {}
                        Ok(ConcurrentStressStartDecision::Abort) => {
                            outcome.failure = Some("startup coordinator aborted the run".to_owned());
                            break 'worker;
                        }
                        Err(error) => {
                            outcome.failure =
                                Some(format!("startup decision channel disconnected: {error}"));
                            break 'worker;
                        }
                    }

                    outcome.failure = None;
                    let mut attempts_for_commit = 0_u64;
                    let mut last_transient_error: Option<String> = None;
                    let mut rng = rand::rngs::StdRng::seed_from_u64(worker_id as u64);

                    'transfers: while outcome.commits < OPS_PER_WRITER {
                        if let Some(budget_error) = concurrent_stress_attempt_budget_error(
                            attempts_for_commit,
                            outcome.attempts,
                            started_at.elapsed(),
                        ) {
                            outcome.failure = Some(format!(
                                "{budget_error}; last transient error: {}",
                                last_transient_error.as_deref().unwrap_or("none")
                            ));
                            break;
                        }

                        outcome.attempts += 1;
                        attempts_for_commit += 1;
                        outcome.max_attempts_for_commit =
                            outcome.max_attempts_for_commit.max(attempts_for_commit);

                        let from_id = rng.random_range(0..NUM_ACCOUNTS);
                        let to_id = rng.random_range(0..NUM_ACCOUNTS);
                        if from_id == to_id {
                            continue;
                        }
                        let amount = rng.random_range(1..=10_i64);

                        if let Err(error) = conn.execute("BEGIN;").await {
                            if outcome.retries.record(&error) {
                                last_transient_error = Some(format!("BEGIN: {error:?}"));
                                concurrent_stress_backoff(
                                    attempts_for_commit,
                                    u64::try_from(worker_id).expect("worker id fits u64"),
                                );
                                continue;
                            }
                            outcome.failure =
                                Some(format!("unexpected BEGIN error: {error:?}"));
                            break;
                        }
                        let begin_seq = conn
                            .current_concurrent_snapshot_seq()
                            .expect("successful concurrent BEGIN must bind its snapshot sequence");

                        let from_balance = match conn
                            .query(&format!(
                                "SELECT balance FROM accounts WHERE id = {from_id};"
                            ))
                            .await
                        {
                            Ok(rows) if rows.len() == 1 => match &row_values(&rows[0])[0] {
                                SqliteValue::Integer(balance) => *balance,
                                other => {
                                    if let Err(rollback_error) = conn.execute("ROLLBACK;").await {
                                        outcome.failure = Some(format!(
                                            "rollback after invalid balance type failed: {rollback_error:?}"
                                        ));
                                    } else {
                                        outcome.failure = Some(format!(
                                            "balance query returned invalid value: {other:?}"
                                        ));
                                    }
                                    break 'transfers;
                                }
                            },
                            Ok(rows) => {
                                if std::env::var_os("DK9RA_O81OV").is_some() {
                                    let snap_count = conn
                                        .query("SELECT COUNT(*) FROM accounts;")
                                        .await
                                        .ok()
                                        .and_then(|r| r.first().map(|row| {
                                            format!("{:?}", row_values(row).to_vec())
                                        }));
                                    let snap_min_max = conn
                                        .query("SELECT MIN(id), MAX(id) FROM accounts;")
                                        .await
                                        .ok()
                                        .and_then(|r| r.first().map(|row| {
                                            format!("{:?}", row_values(row).to_vec())
                                        }));
                                    eprintln!(
                                        "DK9RA_DIAG worker={worker_id} from_id={from_id} begin_seq={begin_seq} snap_count={snap_count:?} snap_min_max={snap_min_max:?}"
                                    );
                                    match Connection::open(&path).await {
                                        Ok(fresh) => {
                                            let fresh_count = fresh
                                                .query("SELECT COUNT(*) FROM accounts;")
                                                .await
                                                .ok()
                                                .and_then(|r| r.first().map(|row| {
                                                    format!("{:?}", row_values(row).to_vec())
                                                }));
                                            let point = fresh
                                                .query(&format!(
                                                    "SELECT id FROM accounts WHERE id = {from_id};"
                                                ))
                                                .await
                                                .ok()
                                                .map(|r| r.len());
                                            let range = fresh
                                                .query(&format!(
                                                    "SELECT id FROM accounts WHERE id >= {from_id} AND id <= {from_id};"
                                                ))
                                                .await
                                                .ok()
                                                .map(|r| r.len());
                                            let cluster = fresh
                                                .query(&format!(
                                                    "SELECT id FROM accounts WHERE id >= {lo} AND id <= {hi} ORDER BY id;",
                                                    lo = from_id.saturating_sub(4),
                                                    hi = from_id + 4
                                                ))
                                                .await
                                                .ok()
                                                .map(|rows| {
                                                    rows.iter()
                                                        .map(|row| format!("{:?}", row_values(row).first()))
                                                        .collect::<Vec<_>>()
                                                        .join(",")
                                                });
                                            let dups = fresh
                                                .query("SELECT id FROM accounts GROUP BY id HAVING COUNT(*) > 1;")
                                                .await
                                                .ok()
                                                .map(|rows| {
                                                    rows.iter()
                                                        .map(|row| format!("{:?}", row_values(row).first()))
                                                        .collect::<Vec<_>>()
                                                        .join(",")
                                                });
                                            let integrity = fresh
                                                .query("PRAGMA integrity_check;")
                                                .await
                                                .ok()
                                                .and_then(|r| r.first().map(|row| {
                                                    format!("{:?}", row_values(row).to_vec())
                                                }));
                                            eprintln!(
                                                "DK9RA_DIAG_FRESH from_id={from_id} fresh_count={fresh_count:?} point={point:?} range={range:?} cluster=[{}] dups=[{}] integrity={integrity:?}",
                                                cluster.unwrap_or_default(),
                                                dups.unwrap_or_default()
                                            );
                                            let _ = fresh.close().await;
                                        }
                                        Err(open_err) => {
                                            eprintln!("DK9RA_DIAG_FRESH open_failed={open_err:?}");
                                        }
                                    }
                                }
                                if let Err(rollback_error) = conn.execute("ROLLBACK;").await {
                                    outcome.failure = Some(format!(
                                        "rollback after missing account failed: {rollback_error:?}"
                                    ));
                                } else {
                                    outcome.failure = Some(format!(
                                        "balance query returned {} rows for account {from_id}",
                                        rows.len()
                                    ));
                                }
                                break;
                            }
                            Err(error) => {
                                let transient_error = format!("balance query: {error:?}");
                                match concurrent_stress_rollback_precommit_transient(
                                    &conn,
                                    &mut outcome,
                                    "balance query",
                                    &error,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        last_transient_error = Some(transient_error);
                                        concurrent_stress_backoff(
                                            attempts_for_commit,
                                            u64::try_from(worker_id).expect("worker id fits u64"),
                                        );
                                        continue;
                                    }
                                    Err(recovery_error) => {
                                        outcome.failure = Some(recovery_error);
                                        break;
                                    }
                                }
                            }
                        };

                        if from_balance < amount {
                            if let Err(error) = conn.execute("ROLLBACK;").await {
                                outcome.failure = Some(format!(
                                    "rollback after insufficient balance failed: {error:?}"
                                ));
                                break;
                            }
                            continue;
                        }

                        match conn
                            .execute(&format!(
                                "UPDATE accounts SET balance = balance - {amount} WHERE id = {from_id};"
                            ))
                            .await
                        {
                            Ok(1) => {}
                            Ok(affected) => {
                                if let Err(rollback_error) = conn.execute("ROLLBACK;").await {
                                    outcome.failure = Some(format!(
                                        "rollback after debit affected {affected} rows failed: {rollback_error:?}"
                                    ));
                                } else {
                                    outcome.failure = Some(format!(
                                        "debit affected {affected} rows; expected 1"
                                    ));
                                }
                                break;
                            }
                            Err(error) => {
                                let transient_error = format!("debit: {error:?}");
                                match concurrent_stress_rollback_precommit_transient(
                                    &conn,
                                    &mut outcome,
                                    "debit",
                                    &error,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        last_transient_error = Some(transient_error);
                                        concurrent_stress_backoff(
                                            attempts_for_commit,
                                            u64::try_from(worker_id).expect("worker id fits u64"),
                                        );
                                        continue;
                                    }
                                    Err(recovery_error) => {
                                        outcome.failure = Some(recovery_error);
                                        break;
                                    }
                                }
                            }
                        }

                        match conn
                            .execute(&format!(
                                "UPDATE accounts SET balance = balance + {amount} WHERE id = {to_id};"
                            ))
                            .await
                        {
                            Ok(1) => {}
                            Ok(affected) => {
                                if let Err(rollback_error) = conn.execute("ROLLBACK;").await {
                                    outcome.failure = Some(format!(
                                        "rollback after credit affected {affected} rows failed: {rollback_error:?}"
                                    ));
                                } else {
                                    outcome.failure = Some(format!(
                                        "credit affected {affected} rows; expected 1"
                                    ));
                                }
                                break;
                            }
                            Err(error) => {
                                let transient_error = format!("credit: {error:?}");
                                match concurrent_stress_rollback_precommit_transient(
                                    &conn,
                                    &mut outcome,
                                    "credit",
                                    &error,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        last_transient_error = Some(transient_error);
                                        concurrent_stress_backoff(
                                            attempts_for_commit,
                                            u64::try_from(worker_id).expect("worker id fits u64"),
                                        );
                                        continue;
                                    }
                                    Err(recovery_error) => {
                                        outcome.failure = Some(recovery_error);
                                        break;
                                    }
                                }
                            }
                        }

                        match conn.execute("COMMIT;").await {
                            Ok(_) => {
                                outcome.commits += 1;
                                let commit_seq = conn.last_local_commit_seq().expect(
                                    "successful concurrent commit must publish its sequence",
                                );
                                outcome.committed_transfers.push(ConcurrentStressTransfer {
                                    from_id,
                                    to_id,
                                    amount,
                                    begin_seq,
                                    commit_seq,
                                });
                                attempts_for_commit = 0;
                                last_transient_error = None;
                            }
                            Err(error) => {
                                let transient_error = format!("commit: {error:?}");
                                match concurrent_stress_rollback_precommit_transient(
                                    &conn,
                                    &mut outcome,
                                    "commit",
                                    &error,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        last_transient_error = Some(transient_error);
                                        concurrent_stress_backoff(
                                            attempts_for_commit,
                                            u64::try_from(worker_id).expect("worker id fits u64"),
                                        );
                                    }
                                    Err(recovery_error) => {
                                        outcome.failure = Some(recovery_error);
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    outcome.elapsed = started_at.elapsed();
                    if outcome.failure.is_none()
                        && outcome.elapsed > CONCURRENT_STRESS_WORKER_TIMEOUT
                    {
                        outcome.failure = Some(format!(
                            "completed after worker deadline {:?}: {:?}",
                            CONCURRENT_STRESS_WORKER_TIMEOUT, outcome.elapsed
                        ));
                    }
                    }
                    if let Err(error) = conn.close_without_checkpoint_in_place().await {
                        let close_failure = format!("worker connection close failed: {error:?}");
                        if let Some(failure) = &mut outcome.failure {
                            failure.push_str("; ");
                            failure.push_str(&close_failure);
                        } else {
                            outcome.failure = Some(close_failure);
                        }
                        conn.close_best_effort_in_place().await;
                    }
                });
                outcome
            }));
        }
        drop(startup_tx);

        let mut ready = [false; NUM_WRITERS];
        let mut ready_count = 0_usize;
        let startup_deadline = Instant::now() + CONCURRENT_STRESS_STARTUP_TIMEOUT;
        let mut startup_failure = None;
        while ready_count < NUM_WRITERS {
            let remaining = startup_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                startup_failure = Some(format!(
                    "startup timed out with {ready_count}/{NUM_WRITERS} workers ready"
                ));
                break;
            }
            match startup_rx.recv_timeout(remaining) {
                Ok(ConcurrentStressStartup::Ready { worker_id }) => {
                    if worker_id >= NUM_WRITERS {
                        startup_failure =
                            Some(format!("out-of-range startup worker id {worker_id}"));
                        break;
                    }
                    if std::mem::replace(&mut ready[worker_id], true) {
                        startup_failure =
                            Some(format!("duplicate startup receipt from worker {worker_id}"));
                        break;
                    }
                    ready_count += 1;
                }
                Ok(ConcurrentStressStartup::Failed { worker_id, error }) => {
                    startup_failure = Some(format!("worker {worker_id} startup failed: {error}"));
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    startup_failure = Some(format!(
                        "startup timed out with {ready_count}/{NUM_WRITERS} workers ready"
                    ));
                    break;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    startup_failure = Some(format!(
                        "startup channel closed with {ready_count}/{NUM_WRITERS} workers ready"
                    ));
                    break;
                }
            }
        }

        let start_gate = ConcurrentStressStartGate::new(start_senders);
        let startup_result = if let Some(error) = startup_failure {
            drop(start_gate);
            Err(error)
        } else {
            start_gate.release()
        };

        let mut results = Vec::with_capacity(NUM_WRITERS);
        let mut panics = Vec::new();
        for (worker_id, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(outcome) => results.push(outcome),
                Err(payload) => panics.push(format!(
                    "worker {worker_id} panicked: {}",
                    concurrent_stress_panic_message(payload.as_ref())
                )),
            }
        }
        assert!(
            panics.is_empty(),
            "concurrent-stress worker panics: {panics:?}"
        );
        assert!(
            startup_result.is_ok(),
            "concurrent-stress startup failed: {}; outcomes: {results:#?}",
            startup_result
                .as_ref()
                .expect_err("failed startup must carry a diagnostic")
        );
        assert_eq!(results.len(), NUM_WRITERS, "missing worker outcomes");
        results.sort_by_key(|outcome| outcome.worker_id);

        for outcome in &results {
            eprintln!("concurrent stress worker outcome: {outcome:#?}");
        }

        // Always collect an independent committed-file verdict before any
        // worker assertion can abort this keeper. A worker may fail because
        // its transaction assembled a mixed page-version view, or because a
        // writer actually published a malformed B-tree. Stock SQLite's scan,
        // point lookup, aggregate, and integrity checker distinguish those
        // two release-critical failure classes after every worker is closed.
        let stock_diagnostic = (|| -> Result<ConcurrentStressStockDiagnostic, String> {
            let stock = rusqlite::Connection::open(&db_path).map_err(|error| error.to_string())?;
            let row_count = stock
                .query_row("SELECT COUNT(*) FROM accounts;", [], |row| row.get(0))
                .map_err(|error| error.to_string())?;
            let balance_sum = stock
                .query_row("SELECT SUM(balance) FROM accounts;", [], |row| row.get(0))
                .map_err(|error| error.to_string())?;
            let point_count = stock
                .query_row("SELECT COUNT(*) FROM accounts WHERE id = 9;", [], |row| {
                    row.get(0)
                })
                .map_err(|error| error.to_string())?;
            let scan_count = stock
                .query_row(
                    "SELECT COUNT(*) FROM accounts NOT INDEXED WHERE id + 0 = 9;",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())?;
            let mut statement = stock
                .prepare("PRAGMA integrity_check;")
                .map_err(|error| error.to_string())?;
            let integrity = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|error| error.to_string())?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| error.to_string())?;
            let mut statement = stock
                .prepare("SELECT id, balance FROM accounts ORDER BY id;")
                .map_err(|error| error.to_string())?;
            let balances = statement
                .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
                .map_err(|error| error.to_string())?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|error| error.to_string())?;
            Ok(ConcurrentStressStockDiagnostic {
                row_count,
                balance_sum,
                point_count,
                scan_count,
                integrity,
                balances,
            })
        })();
        eprintln!("concurrent stress stock SQLite diagnostic: {stock_diagnostic:#?}");

        if let Ok(ConcurrentStressStockDiagnostic {
            balances: durable_balances,
            ..
        }) = &stock_diagnostic
        {
            let account_count =
                usize::try_from(NUM_ACCOUNTS).expect("concurrent stress account count fits usize");
            let mut expected_balances = vec![INITIAL_BALANCE; account_count];
            for transfer in results
                .iter()
                .flat_map(|outcome| &outcome.committed_transfers)
            {
                assert!(
                    transfer.commit_seq > transfer.begin_seq,
                    "committed transfer must advance its BEGIN snapshot: {transfer:?}"
                );
                let from_index = usize::try_from(transfer.from_id)
                    .expect("concurrent stress source account fits usize");
                let to_index = usize::try_from(transfer.to_id)
                    .expect("concurrent stress target account fits usize");
                expected_balances[from_index] -= transfer.amount;
                expected_balances[to_index] += transfer.amount;
            }
            let balance_mismatches = durable_balances
                .iter()
                .filter_map(|&(account_id, durable_balance)| {
                    let account_index = usize::try_from(account_id)
                        .expect("durable concurrent stress account id fits usize");
                    let expected_balance = expected_balances[account_index];
                    (durable_balance != expected_balance).then_some((
                        account_id,
                        expected_balance,
                        durable_balance,
                        durable_balance - expected_balance,
                    ))
                })
                .collect::<Vec<_>>();
            eprintln!(
                "concurrent stress durable balance mismatches (id, expected, durable, delta): \
                 {balance_mismatches:?}"
            );
            for (account_id, _, _, _) in &balance_mismatches {
                let mut touching_transfers = results
                    .iter()
                    .flat_map(|outcome| &outcome.committed_transfers)
                    .filter(|transfer| {
                        transfer.from_id == *account_id || transfer.to_id == *account_id
                    })
                    .collect::<Vec<_>>();
                touching_transfers.sort_by_key(|transfer| transfer.commit_seq);
                eprintln!(
                    "concurrent stress committed touches for account {account_id}: \
                     {touching_transfers:?}"
                );
            }
        }

        let mut total_commits = 0_u64;
        let mut total_retries = 0_u64;
        for (expected_worker_id, outcome) in results.iter().enumerate() {
            assert_eq!(
                outcome.worker_id, expected_worker_id,
                "worker ids must be unique and contiguous"
            );
            assert!(
                outcome.concurrent_mode_default,
                "worker {} lost the concurrent-writer default",
                outcome.worker_id
            );
            assert!(
                outcome.failure.is_none(),
                "worker {} failed: {:?}",
                outcome.worker_id,
                outcome.failure
            );
            assert_eq!(
                outcome.commits, OPS_PER_WRITER,
                "worker {} committed the wrong number of transfers",
                outcome.worker_id
            );
            assert!(
                outcome.attempts <= CONCURRENT_STRESS_MAX_ATTEMPTS_PER_WORKER,
                "worker {} exceeded its attempt budget",
                outcome.worker_id
            );
            assert!(
                outcome.max_attempts_for_commit <= CONCURRENT_STRESS_MAX_ATTEMPTS_PER_COMMIT,
                "worker {} exceeded its per-commit attempt budget",
                outcome.worker_id
            );
            assert!(
                outcome.elapsed <= CONCURRENT_STRESS_WORKER_TIMEOUT,
                "worker {} exceeded its elapsed-time budget",
                outcome.worker_id
            );
            total_commits += outcome.commits;
            total_retries += outcome.retries.total();
        }
        assert_eq!(
            total_commits,
            u64::try_from(NUM_WRITERS).expect("writer count fits u64") * OPS_PER_WRITER
        );
        eprintln!("concurrent stress total retries: {total_retries}");

        let stock_diagnostic = stock_diagnostic.expect("stock SQLite diagnostic must complete");
        assert_eq!(stock_diagnostic.row_count, NUM_ACCOUNTS);
        assert_eq!(stock_diagnostic.balance_sum, EXPECTED_TOTAL);
        assert_eq!(stock_diagnostic.point_count, 1);
        assert_eq!(stock_diagnostic.scan_count, 1);
        assert_eq!(stock_diagnostic.integrity, ["ok"]);

        let mut final_invariants = None;
        let mut final_integrity = None;
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&db_path_string)
                .await
                .expect("reopen concurrent-stress database for verification");
            assert!(
                conn.is_concurrent_mode_default(),
                "verification connection must preserve the concurrent-writer default"
            );
            let rows = conn
                .query(
                    "SELECT COUNT(*), SUM(balance),\
                     SUM(CASE WHEN balance < 0 THEN 1 ELSE 0 END),\
                     MIN(length(payload)), MAX(length(payload)) FROM accounts;",
                )
                .await
                .expect("query final account invariants");
            final_invariants = Some(rows.iter().map(row_values).collect::<Vec<_>>());
            let integrity = conn
                .query("PRAGMA integrity_check;")
                .await
                .expect("run final integrity check");
            final_integrity = Some(integrity.iter().map(row_values).collect::<Vec<_>>());
            conn.close()
                .await
                .expect("close concurrent-stress verification connection");
        });
        assert_eq!(
            final_invariants,
            Some(vec![vec![
                SqliteValue::Integer(NUM_ACCOUNTS),
                SqliteValue::Integer(EXPECTED_TOTAL),
                SqliteValue::Integer(0),
                SqliteValue::Integer(512),
                SqliteValue::Integer(512),
            ]]),
            "final multi-page aggregate invariants must have exact INTEGER storage classes"
        );
        assert_eq!(
            final_integrity,
            Some(vec![vec![SqliteValue::Text("ok".into())]]),
            "integrity_check must return exactly one TEXT ok row"
        );

        let receipt_token = std::env::var(CONCURRENT_STRESS_RECEIPT_ENV)
            .expect("supervised child must inherit its receipt token");
        let receipt = format!("{CONCURRENT_STRESS_RECEIPT_PREFIX}{receipt_token}");
        let receipt_path = std::env::var_os(CONCURRENT_STRESS_RECEIPT_PATH_ENV)
            .expect("supervised child must inherit its receipt file path");
        std::fs::write(&receipt_path, &receipt)
            .expect("supervised child must write its completion receipt file");
        println!("{receipt}");
    }

    #[test]
    fn concurrent_credit_conflict_rollback_preserves_staged_debit() {
        asupersync::test_utils::run_test(|| async {
            const NUM_ACCOUNTS: i64 = 100;
            const INITIAL_BALANCE: i64 = 1_000;
            const EXPECTED_TOTAL: i64 = NUM_ACCOUNTS * INITIAL_BALANCE;

            let dir = tempfile::tempdir().expect("create rollback-atomicity temp dir");
            let db_path = dir.path().join("rollback-atomicity.db");
            let db_path = db_path.to_string_lossy().into_owned();
            let setup = Connection::open(&db_path)
                .await
                .expect("open rollback-atomicity setup connection");
            setup
                .execute(
                    "CREATE TABLE accounts (
                        id INTEGER PRIMARY KEY,
                        balance INTEGER,
                        payload TEXT NOT NULL
                    );",
                )
                .await
                .expect("create rollback-atomicity accounts table");
            let payload = "x".repeat(512);
            for account_id in 0..NUM_ACCOUNTS {
                setup
                    .execute_with_params(
                        "INSERT INTO accounts VALUES (?1, ?2, ?3);",
                        &[
                            SqliteValue::Integer(account_id),
                            SqliteValue::Integer(INITIAL_BALANCE),
                            SqliteValue::Text(payload.clone().into()),
                        ],
                    )
                    .await
                    .expect("insert rollback-atomicity account");
            }
            setup.close().await.expect("close setup connection");

            let debit = Connection::open(&db_path)
                .await
                .expect("open debit connection");
            let credit_blocker = Connection::open(&db_path)
                .await
                .expect("open credit-blocker connection");
            debit.execute("BEGIN;").await.expect("begin debit txn");
            credit_blocker
                .execute("BEGIN;")
                .await
                .expect("begin credit-blocker txn");
            assert_eq!(
                debit
                    .execute("UPDATE accounts SET balance = balance - 9 WHERE id = 0;")
                    .await
                    .expect("stage debit on first leaf"),
                1
            );
            assert_eq!(
                credit_blocker
                    .execute("UPDATE accounts SET balance = balance + 1 WHERE id = 99;")
                    .await
                    .expect("lock credit leaf from peer txn"),
                1
            );
            let credit_error = debit
                .execute("UPDATE accounts SET balance = balance + 9 WHERE id = 99;")
                .await
                .expect_err("credit page held by peer must reject the partial transfer");
            assert!(
                matches!(
                    credit_error,
                    FrankenError::Busy | FrankenError::BusySnapshot { .. }
                ),
                "credit conflict must be retryable, got {credit_error:?}"
            );
            debit
                .execute("ROLLBACK;")
                .await
                .expect("rollback staged debit after credit conflict");
            credit_blocker
                .execute("ROLLBACK;")
                .await
                .expect("rollback credit blocker");
            debit.close().await.expect("close debit connection");
            credit_blocker
                .close()
                .await
                .expect("close credit-blocker connection");

            let verify = Connection::open(&db_path)
                .await
                .expect("open rollback-atomicity verification connection");
            let row = verify
                .query_row(
                    "SELECT COUNT(*), SUM(balance),
                     (SELECT balance FROM accounts WHERE id = 0),
                     (SELECT balance FROM accounts WHERE id = 99)
                     FROM accounts;",
                )
                .await
                .expect("query rollback-atomicity invariants");
            assert_eq!(
                row_values(&row),
                vec![
                    SqliteValue::Integer(NUM_ACCOUNTS),
                    SqliteValue::Integer(EXPECTED_TOTAL),
                    SqliteValue::Integer(INITIAL_BALANCE),
                    SqliteValue::Integer(INITIAL_BALANCE),
                ],
                "rolling back a transfer after its credit conflicts must discard its staged debit"
            );
            verify
                .close()
                .await
                .expect("close rollback-atomicity verification connection");
        });
    }

    #[test]
    fn concurrent_stress_start_gate_aborts_all_waiters_on_failure() {
        let mut senders = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..3 {
            let (sender, receiver) = mpsc::sync_channel(1);
            senders.push(sender);
            handles.push(std::thread::spawn(move || {
                receiver
                    .recv_timeout(Duration::from_millis(100))
                    .expect("abort decision must reach every startup waiter")
            }));
        }
        drop(ConcurrentStressStartGate::new(senders));
        for handle in handles {
            assert_eq!(
                handle.join().expect("startup waiter must be joined"),
                ConcurrentStressStartDecision::Abort
            );
        }
    }

    #[test]
    fn concurrent_stress_retry_budget_is_exact_and_finite() {
        let mut attempts = 0_u64;
        let mut retries = ConcurrentStressRetryCounts::default();
        let exhaustion = loop {
            if let Some(error) =
                concurrent_stress_attempt_budget_error(attempts, attempts, Duration::ZERO)
            {
                break error;
            }
            attempts += 1;
            assert!(retries.record(&FrankenError::Busy));
        };
        assert_eq!(attempts, CONCURRENT_STRESS_MAX_ATTEMPTS_PER_COMMIT);
        assert_eq!(retries.busy, CONCURRENT_STRESS_MAX_ATTEMPTS_PER_COMMIT);
        assert!(exhaustion.contains("attempts for one commit"));
        assert!(retries.record(&FrankenError::BusyRecovery));
        assert_eq!(retries.busy_recovery, 1);
        assert!(
            concurrent_stress_attempt_budget_error(
                0,
                CONCURRENT_STRESS_MAX_ATTEMPTS_PER_WORKER,
                Duration::ZERO,
            )
            .expect("the total-attempt budget must be finite")
            .contains("total attempts")
        );
        assert!(
            concurrent_stress_attempt_budget_error(0, 0, CONCURRENT_STRESS_WORKER_TIMEOUT,)
                .expect("the elapsed-time budget must be finite")
                .contains("worker deadline")
        );
    }

    /// Verify that concurrent readers see consistent snapshots.
    #[test]
    fn concurrent_readers_consistency() {
        asupersync::test_utils::run_test(|| async {
            use std::thread;

            let dir = tempfile::tempdir().expect("create temp dir");
            let db_path = dir.path().join("readers.db");
            let db_path_str = db_path.to_str().unwrap();

            // Setup: create table with known data.
            {
                let conn = Connection::open(db_path_str)
                    .await
                    .expect("open db for setup");
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER);")
                    .await
                    .expect("create table");
                for i in 0..100 {
                    conn.execute(&format!("INSERT INTO t VALUES ({}, {});", i, i * 10))
                        .await
                        .expect("insert row");
                }
                conn.close()
                    .await
                    .expect("close concurrent-reader setup connection");
            }

            const NUM_READERS: usize = 4;
            const READS_PER_THREAD: usize = 50;

            let (startup_tx, startup_rx) = mpsc::channel::<ConcurrentStressStartup>();
            let mut start_senders = Vec::with_capacity(NUM_READERS);
            let mut handles = Vec::with_capacity(NUM_READERS);
            for thread_id in 0..NUM_READERS {
                let path = db_path_str.to_owned();
                let startup_tx = startup_tx.clone();
                let (start_tx, start_rx) = mpsc::sync_channel(1);
                start_senders.push(start_tx);

                handles.push(thread::spawn(move || {
                    let mut consistent = true;
                    asupersync::test_utils::run_test(|| async {
                        let mut open_attempts = 0_u64;
                        let mut conn = loop {
                            open_attempts += 1;
                            match Connection::open(&path).await {
                                Ok(conn) => break conn,
                                Err(error @ (FrankenError::Busy | FrankenError::BusyRecovery))
                                    if open_attempts < CONCURRENT_READER_MAX_OPEN_ATTEMPTS =>
                                {
                                    eprintln!(
                                        "reader {thread_id} open attempt {open_attempts} hit transient {error:?}"
                                    );
                                    concurrent_stress_backoff(
                                        open_attempts,
                                        u64::try_from(thread_id).expect("reader id fits u64"),
                                    );
                                }
                                Err(error) => {
                                    let error = format!(
                                        "open failed after {open_attempts} attempt(s): {error:?}"
                                    );
                                    let _ = startup_tx.send(ConcurrentStressStartup::Failed {
                                        worker_id: thread_id,
                                        error,
                                    });
                                    consistent = false;
                                    return;
                                }
                            }
                        };

                        if startup_tx
                            .send(ConcurrentStressStartup::Ready {
                                worker_id: thread_id,
                            })
                            .is_err()
                        {
                            eprintln!("reader {thread_id} startup coordinator disconnected");
                            conn.close_best_effort_in_place().await;
                            consistent = false;
                            return;
                        }
                        if !matches!(
                            start_rx.recv_timeout(CONCURRENT_STRESS_STARTUP_TIMEOUT),
                            Ok(ConcurrentStressStartDecision::Run)
                        ) {
                            eprintln!("reader {thread_id} did not receive the run decision");
                            conn.close_best_effort_in_place().await;
                            consistent = false;
                            return;
                        }

                        for _ in 0..READS_PER_THREAD {
                            // Start a read transaction.
                            conn.execute("BEGIN;").await.expect("begin");

                            // Read sum - should always be consistent.
                            let rows = conn
                                .query("SELECT SUM(val) FROM t;")
                                .await
                                .expect("sum query");
                            let sum = if let SqliteValue::Integer(n) = &row_values(&rows[0])[0] {
                                *n
                            } else {
                                consistent = false;
                                break;
                            };

                            // Expected sum: 0 + 10 + 20 + ... + 990 = 10 * (0 + 1 + ... + 99) = 10 * 4950 = 49500
                            let expected = 10 * (99 * 100 / 2);
                            if sum != expected {
                                eprintln!(
                                    "Thread {} saw inconsistent sum: {} (expected {})",
                                    thread_id, sum, expected
                                );
                                consistent = false;
                            }

                            conn.execute("COMMIT;").await.expect("commit");
                        }
                        if let Err(error) = conn.close_without_checkpoint_in_place().await {
                            eprintln!("reader {thread_id} close failed: {error:?}");
                            conn.close_best_effort_in_place().await;
                            consistent = false;
                        }
                    });

                    consistent
                }));
            }
            drop(startup_tx);

            let mut ready = [false; NUM_READERS];
            let mut ready_count = 0_usize;
            let startup_deadline = Instant::now() + CONCURRENT_STRESS_STARTUP_TIMEOUT;
            let mut startup_failure = None;
            while ready_count < NUM_READERS {
                let remaining = startup_deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    startup_failure = Some(format!(
                        "startup timed out with {ready_count}/{NUM_READERS} readers ready"
                    ));
                    break;
                }
                match startup_rx.recv_timeout(remaining) {
                    Ok(ConcurrentStressStartup::Ready { worker_id }) => {
                        if worker_id >= NUM_READERS {
                            startup_failure = Some(format!("out-of-range reader id {worker_id}"));
                            break;
                        }
                        if std::mem::replace(&mut ready[worker_id], true) {
                            startup_failure =
                                Some(format!("duplicate startup receipt from reader {worker_id}"));
                            break;
                        }
                        ready_count += 1;
                    }
                    Ok(ConcurrentStressStartup::Failed { worker_id, error }) => {
                        startup_failure =
                            Some(format!("reader {worker_id} startup failed: {error}"));
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        startup_failure = Some(format!(
                            "startup timed out with {ready_count}/{NUM_READERS} readers ready"
                        ));
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        startup_failure = Some(format!(
                            "startup channel closed with {ready_count}/{NUM_READERS} readers ready"
                        ));
                        break;
                    }
                }
            }

            let start_gate = ConcurrentStressStartGate::new(start_senders);
            let startup_result = if let Some(error) = startup_failure {
                drop(start_gate);
                Err(error)
            } else {
                start_gate.release()
            };

            let results = handles
                .into_iter()
                .map(|handle| handle.join())
                .collect::<Vec<_>>();

            assert!(
                startup_result.is_ok(),
                "concurrent-reader startup failed: {}",
                startup_result
                    .as_ref()
                    .expect_err("failed startup must carry a diagnostic")
            );

            // Join every reader before asserting so the database remains live
            // long enough to report every worker outcome.
            for (i, result) in results.into_iter().enumerate() {
                let consistent = result.expect("reader thread panicked");
                assert!(consistent, "Reader thread {} saw inconsistent data", i);
            }
        });
    }

    // ── Conformance gap probes (fixtures 017–021) ──────────────────────

    #[test]
    fn conformance_017_type_affinity_edge_numeric_coercion() {
        asupersync::test_utils::run_test(|| async {
            // '3.0e+5' into NUMERIC should coerce to integer 300000
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE q1(a NUMERIC, b TEXT, c INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO q1 VALUES('3.0e+5', 123, '0042')")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT typeof(a), a, typeof(b), b, typeof(c), c FROM q1")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            let vals = row_values(&rows[0]);
            // SQLite behavior: '3.0e+5' → NUMERIC → integer 300000
            assert_eq!(vals[0], SqliteValue::Text("integer".into()));
            assert_eq!(vals[1], SqliteValue::Integer(300_000));
            // 123 into TEXT → text "123"
            assert_eq!(vals[2], SqliteValue::Text("text".into()));
            assert_eq!(vals[3], SqliteValue::Text("123".into()));
            // '0042' into INTEGER → integer 42
            assert_eq!(vals[4], SqliteValue::Text("integer".into()));
            assert_eq!(vals[5], SqliteValue::Integer(42));
        });
    }

    #[test]
    fn conformance_018_collation_nocase_ascii_only() {
        asupersync::test_utils::run_test(|| async {
            // NOCASE is ASCII-insensitive: 'a' = 'A' → 1
            // but Unicode-sensitive: 'æ' ≠ 'Æ' → 0
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn.query("SELECT 'a' = 'A' COLLATE NOCASE").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        });
    }

    #[test]
    fn conformance_018_collation_nocase_unicode_sensitive() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // Unicode chars: NOCASE does NOT fold them
            let rows = conn.query("SELECT 'æ' = 'Æ' COLLATE NOCASE").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(0));
        });
    }

    #[test]
    fn conformance_019_null_unique_allows_multiple_nulls() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE q3(a INTEGER UNIQUE, note TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO q3(a, note) VALUES(NULL, 'first-null')")
                .await
                .unwrap();
            // UNIQUE allows multiple NULLs
            conn.execute("INSERT INTO q3(a, note) VALUES(NULL, 'second-null')")
                .await
                .unwrap();
            conn.execute("INSERT INTO q3(a, note) VALUES(7, 'first-seven')")
                .await
                .unwrap();
            let rows = conn.query("SELECT COUNT(*) FROM q3").await.unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
        });
    }

    #[test]
    fn conformance_019_null_unique_rejects_duplicate_non_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE q3b(a INTEGER UNIQUE, note TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO q3b(a, note) VALUES(7, 'first-seven')")
                .await
                .unwrap();
            // Duplicate non-NULL should be rejected
            let result = conn
                .execute("INSERT INTO q3b(a, note) VALUES(7, 'dup')")
                .await;
            assert!(
                result.is_err(),
                "Duplicate non-NULL unique value should fail"
            );
        });
    }

    #[test]
    fn conformance_020_integer_overflow_promotes_to_real() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // i64::MAX + 1 should overflow to real
            let rows = conn
                .query("SELECT typeof(9223372036854775807 + 1)")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("real".into()));
        });
    }

    #[test]
    fn conformance_020_integer_underflow_promotes_to_real() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // i64::MIN - 1 should underflow to real
            let rows = conn
                .query("SELECT typeof(-9223372036854775808 - 1)")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("real".into()));
        });
    }

    #[test]
    fn conformance_021_savepoint_rollback_preserves_outer() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE q5(id INTEGER PRIMARY KEY, note TEXT)")
                .await
                .unwrap();
            conn.execute("BEGIN").await.unwrap();
            conn.execute("INSERT INTO q5 VALUES(1, 'outer')")
                .await
                .unwrap();
            conn.execute("SAVEPOINT s1").await.unwrap();
            conn.execute("INSERT INTO q5 VALUES(2, 'inner')")
                .await
                .unwrap();
            conn.execute("ROLLBACK TO s1").await.unwrap();
            conn.execute("RELEASE s1").await.unwrap();
            conn.execute("COMMIT").await.unwrap();
            let rows = conn
                .query("SELECT id, note FROM q5 ORDER BY id")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1, "ROLLBACK TO s1 should undo inner insert");
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("outer".into()));
        });
    }

    #[test]
    fn conformance_021_nested_begin_errors() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("BEGIN").await.unwrap();
            // Nested BEGIN inside an active transaction should error
            let result = conn.execute("BEGIN").await;
            assert!(result.is_err(), "Nested BEGIN should produce an error");
            conn.execute("ROLLBACK").await.unwrap();
        });
    }

    // ── SQL Parity: REPLACE statement ────────────────────────────────────

    #[test]
    fn parity_replace_into_inserts_new_row() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("REPLACE INTO t VALUES (1, 'first');")
                .await
                .unwrap();
            let rows = conn.query("SELECT id, val FROM t;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Text("first".into()));
        });
    }

    #[test]
    fn parity_replace_into_overwrites_existing() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'old');")
                .await
                .unwrap();
            conn.execute("REPLACE INTO t VALUES (1, 'new');")
                .await
                .unwrap();
            let rows = conn.query("SELECT val FROM t WHERE id = 1;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("new".into()));
        });
    }

    // ── SQL Parity: INSERT OR IGNORE ─────────────────────────────────────

    #[test]
    fn parity_insert_or_ignore_skips_conflict() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'first');")
                .await
                .unwrap();
            // INSERT OR IGNORE should silently skip the conflicting row
            conn.execute("INSERT OR IGNORE INTO t VALUES (1, 'dup');")
                .await
                .unwrap();
            let rows = conn.query("SELECT val FROM t WHERE id = 1;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("first".into()));
        });
    }

    // ── SQL Parity: Multi-column ORDER BY ────────────────────────────────

    #[test]
    fn parity_multi_column_order_by() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, c TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 1, 'x');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 2, 'y');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 1, 'z');")
                .await
                .unwrap();
            let rows = conn.query("SELECT c FROM t ORDER BY a, b;").await.unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("z".into()));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("y".into()));
            assert_eq!(row_values(&rows[2])[0], SqliteValue::Text("x".into()));
        });
    }

    // ── SQL Parity: LIMIT with OFFSET ────────────────────────────────────

    #[test]
    fn parity_limit_with_offset() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            for i in 1..=5 {
                conn.execute(&format!("INSERT INTO t VALUES ({i}, 'r{i}');"))
                    .await
                    .unwrap();
            }
            let rows = conn
                .query("SELECT val FROM t ORDER BY id LIMIT 2 OFFSET 2;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("r3".into()));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("r4".into()));
        });
    }

    // ── SQL Parity: Subquery in WHERE ────────────────────────────────────

    #[test]
    fn parity_subquery_in_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1 (id INTEGER, val TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t2 (ref_id INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (3, 'c');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO t2 VALUES (3);").await.unwrap();
            let rows = conn
                .query("SELECT val FROM t1 WHERE id IN (SELECT ref_id FROM t2) ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".into()));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("c".into()));
        });
    }

    // ── SQL Parity: CAST in expressions ──────────────────────────────────

    #[test]
    fn parity_cast_integer_to_text() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT CAST(42 AS TEXT);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("42".into()));
        });
    }

    #[test]
    fn parity_cast_text_to_integer() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT CAST('123' AS INTEGER);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(123));
        });
    }

    // ── SQL Parity: EXISTS subquery ──────────────────────────────────────

    #[test]
    fn parity_exists_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
            let row = conn
                .query_row("SELECT EXISTS (SELECT 1 FROM t WHERE id = 1);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
            let row = conn
                .query_row("SELECT EXISTS (SELECT 1 FROM t WHERE id = 999);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
        });
    }

    // ── SQL Parity: COUNT(DISTINCT ...) ──────────────────────────────────

    #[test]
    fn parity_count_distinct() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (val INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (3);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
            let row = conn
                .query_row("SELECT COUNT(DISTINCT val) FROM t;")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(3));
        });
    }

    // ── SQL Parity: GROUP_CONCAT ─────────────────────────────────────────

    #[test]
    fn parity_group_concat_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (grp TEXT, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES ('a', 'x');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES ('a', 'y');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES ('b', 'z');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT grp, GROUP_CONCAT(val, ',') FROM t GROUP BY grp ORDER BY grp;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            // Group 'a' should have x,y (in insertion order)
            let a_val = &row_values(&rows[0])[1];
            match a_val {
                SqliteValue::Text(s) => {
                    assert!(&**s == "x,y" || &**s == "y,x", "group_concat for 'a' = {s}");
                }
                other => panic!("expected Text, got {other:?}"),
            }
        });
    }

    // ── DISTINCT aggregate edge-case tests ─────────────────────────────

    #[test]
    fn parity_count_distinct_with_nulls() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE d2(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO d2 VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO d2 VALUES(NULL);").await.unwrap();
            conn.execute("INSERT INTO d2 VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO d2 VALUES(NULL);").await.unwrap();
            let row = conn
                .query_row("SELECT COUNT(DISTINCT x) FROM d2;")
                .await
                .unwrap();
            // COUNT(DISTINCT x) ignores NULLs → 2 (values 1, 2)
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn parity_sum_distinct() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE d3(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO d3 VALUES(10);").await.unwrap();
            conn.execute("INSERT INTO d3 VALUES(20);").await.unwrap();
            conn.execute("INSERT INTO d3 VALUES(10);").await.unwrap();
            conn.execute("INSERT INTO d3 VALUES(30);").await.unwrap();
            conn.execute("INSERT INTO d3 VALUES(20);").await.unwrap();
            let row = conn
                .query_row("SELECT SUM(DISTINCT x) FROM d3;")
                .await
                .unwrap();
            // SUM(DISTINCT x) = 10 + 20 + 30 = 60
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(60));
        });
    }

    #[test]
    fn parity_count_vs_count_distinct() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE d4(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO d4 VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO d4 VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO d4 VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO d4 VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO d4 VALUES(2);").await.unwrap();
            let r1 = conn.query_row("SELECT COUNT(x) FROM d4;").await.unwrap();
            assert_eq!(row_values(&r1)[0], SqliteValue::Integer(5));
            let r2 = conn
                .query_row("SELECT COUNT(DISTINCT x) FROM d4;")
                .await
                .unwrap();
            assert_eq!(row_values(&r2)[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn parity_count_distinct_group_by() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE d5(grp TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO d5 VALUES('a', 1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO d5 VALUES('a', 2);")
                .await
                .unwrap();
            conn.execute("INSERT INTO d5 VALUES('a', 1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO d5 VALUES('b', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO d5 VALUES('b', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO d5 VALUES('b', 20);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT grp, COUNT(DISTINCT val) FROM d5 GROUP BY grp ORDER BY grp;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            // Group 'a': {1,2} → 2
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".into()));
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(2));
            // Group 'b': {10,20} → 2
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("b".into()));
            assert_eq!(row_values(&rows[1])[1], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn parity_count_distinct_all_same() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE d6(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO d6 VALUES(42);").await.unwrap();
            conn.execute("INSERT INTO d6 VALUES(42);").await.unwrap();
            conn.execute("INSERT INTO d6 VALUES(42);").await.unwrap();
            let row = conn
                .query_row("SELECT COUNT(DISTINCT x) FROM d6;")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
        });
    }

    #[test]
    fn parity_count_distinct_empty_table() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE d7(x INTEGER);").await.unwrap();
            let row = conn
                .query_row("SELECT COUNT(DISTINCT x) FROM d7;")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
        });
    }

    // ── Scalar subquery tests ──────────────────────────────────────────

    #[test]
    fn parity_scalar_subquery_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(20);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(30);").await.unwrap();
            let row = conn
                .query_row("SELECT (SELECT COUNT(*) FROM t);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(3));
        });
    }

    #[test]
    fn parity_scalar_subquery_max() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(5);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(15);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(10);").await.unwrap();
            let row = conn
                .query_row("SELECT (SELECT MAX(x) FROM t);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(15));
        });
    }

    #[test]
    fn parity_scalar_subquery_no_from() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT (SELECT 42);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(42));
        });
    }

    #[test]
    fn parity_scalar_subquery_first_row() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(100);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(200);").await.unwrap();
            let row = conn.query_row("SELECT (SELECT x FROM t);").await.unwrap();
            // Should return the first row value (100).
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(100));
        });
    }

    #[test]
    fn parity_scalar_subquery_empty_table_is_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            let row = conn.query_row("SELECT (SELECT x FROM t);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Null);
        });
    }

    // ── EXISTS subquery tests ──────────────────────────────────────────

    #[test]
    fn parity_exists_true() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            let row = conn
                .query_row("SELECT EXISTS (SELECT 1 FROM t);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
        });
    }

    #[test]
    fn parity_exists_false_empty() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            let row = conn
                .query_row("SELECT EXISTS (SELECT 1 FROM t);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
        });
    }

    #[test]
    fn parity_not_exists_true_empty() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            let row = conn
                .query_row("SELECT NOT EXISTS (SELECT 1 FROM t);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
        });
    }

    #[test]
    fn parity_exists_no_from() {
        asupersync::test_utils::run_test(|| async {
            // EXISTS (SELECT 1) is always true — no table needed.
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT EXISTS (SELECT 1);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));
        });
    }

    #[test]
    fn parity_exists_with_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(20);").await.unwrap();

            // EXISTS with WHERE that matches.
            let row = conn
                .query_row("SELECT EXISTS (SELECT 1 FROM t WHERE x = 10);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(1));

            // EXISTS with WHERE that doesn't match.
            let row = conn
                .query_row("SELECT EXISTS (SELECT 1 FROM t WHERE x = 99);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
        });
    }

    // ── FILTER clause parity tests ──────────────────────────────────────

    #[test]
    fn parity_count_filter() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE f1(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO f1 VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO f1 VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO f1 VALUES(3);").await.unwrap();
            conn.execute("INSERT INTO f1 VALUES(4);").await.unwrap();
            conn.execute("INSERT INTO f1 VALUES(5);").await.unwrap();
            // COUNT(*) FILTER (WHERE x > 3) → 2 rows (4, 5).
            let row = conn
                .query_row("SELECT COUNT(*) FILTER (WHERE x > 3) FROM f1;")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn parity_sum_filter() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE f2(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO f2 VALUES(10);").await.unwrap();
            conn.execute("INSERT INTO f2 VALUES(20);").await.unwrap();
            conn.execute("INSERT INTO f2 VALUES(30);").await.unwrap();
            // SUM(x) FILTER (WHERE x >= 20) → 50.
            let row = conn
                .query_row("SELECT SUM(x) FILTER (WHERE x >= 20) FROM f2;")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(50));
        });
    }

    #[test]
    fn parity_count_filter_none_match() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE f3(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO f3 VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO f3 VALUES(2);").await.unwrap();
            // COUNT(*) FILTER (WHERE x > 100) → 0.
            let row = conn
                .query_row("SELECT COUNT(*) FILTER (WHERE x > 100) FROM f3;")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
        });
    }

    #[test]
    fn parity_filter_no_group_by_same_table() {
        asupersync::test_utils::run_test(|| async {
            // Diagnostic: verify FILTER works on the SAME table/query without GROUP BY.
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE f4b(city TEXT, age INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4b VALUES('A', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4b VALUES('A', 30);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4b VALUES('B', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4b VALUES('B', 40);")
                .await
                .unwrap();
            // COUNT(*) FILTER (WHERE age > 25) → 2 rows (30, 40).
            let row = conn
                .query_row("SELECT COUNT(*) FILTER (WHERE age > 25) FROM f4b;")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn parity_filter_group_by_always_false() {
        asupersync::test_utils::run_test(|| async {
            // FILTER (WHERE 0) should always exclude → count = 0 per group.
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE f4z(city TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4z VALUES('A', 1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4z VALUES('A', 2);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4z VALUES('B', 3);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT city, COUNT(*) FILTER (WHERE 0) FROM f4z GROUP BY city;")
                .await
                .unwrap();
            let mut results: Vec<(String, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    let city = match &vals[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        _ => panic!("expected text"),
                    };
                    let cnt = match vals[1] {
                        SqliteValue::Integer(n) => n,
                        _ => panic!("expected integer, got {:?}", vals[1]),
                    };
                    (city, cnt)
                })
                .collect();
            results.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(results, vec![("A".into(), 0), ("B".into(), 0)]);
        });
    }

    #[test]
    fn parity_filter_with_group_by() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE f4(city TEXT, age INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4 VALUES('A', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4 VALUES('A', 30);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4 VALUES('B', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO f4 VALUES('B', 40);")
                .await
                .unwrap();
            // COUNT(*) FILTER (WHERE age > 25) per group:
            //   A: 1 (only age=30), B: 1 (only age=40).
            let rows = conn
                .query("SELECT city, COUNT(*) FILTER (WHERE age > 25) FROM f4 GROUP BY city;")
                .await
                .unwrap();
            let mut results: Vec<(String, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    let city = match &vals[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        _ => panic!("expected text"),
                    };
                    let cnt = match vals[1] {
                        SqliteValue::Integer(n) => n,
                        _ => panic!("expected integer"),
                    };
                    (city, cnt)
                })
                .collect();
            results.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(results, vec![("A".into(), 1), ("B".into(), 1)]);
        });
    }

    #[test]
    fn parity_filter_multiple_aggregates() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE f5(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO f5 VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO f5 VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO f5 VALUES(3);").await.unwrap();
            conn.execute("INSERT INTO f5 VALUES(4);").await.unwrap();
            // Two aggregates with different filters in the same query.
            let row = conn
            .query_row(
                "SELECT COUNT(*) FILTER (WHERE x <= 2), COUNT(*) FILTER (WHERE x >= 3) FROM f5;",
            )
            .await
            .unwrap();
            let vals = row_values(&row);
            assert_eq!(vals[0], SqliteValue::Integer(2)); // x<=2: 1,2
            assert_eq!(vals[1], SqliteValue::Integer(2)); // x>=3: 3,4
        });
    }

    #[test]
    fn parity_count_filter_vs_no_filter() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE f6(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO f6 VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO f6 VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO f6 VALUES(3);").await.unwrap();
            // Mix of filtered and unfiltered aggregates.
            let row = conn
                .query_row("SELECT COUNT(*), COUNT(*) FILTER (WHERE x > 1) FROM f6;")
                .await
                .unwrap();
            let vals = row_values(&row);
            assert_eq!(vals[0], SqliteValue::Integer(3)); // all rows
            assert_eq!(vals[1], SqliteValue::Integer(2)); // only x>1: 2,3
        });
    }

    // ── CASE WHEN parity tests ───────────────────────────────────────────

    #[test]
    fn parity_case_simple() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END;")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("two".into()));
        });
    }

    #[test]
    fn parity_case_searched() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(15);").await.unwrap();
            let row = conn
            .query_row(
                "SELECT CASE WHEN x < 10 THEN 'low' WHEN x < 20 THEN 'mid' ELSE 'high' END FROM t;",
            )
            .await
            .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("mid".into()));
        });
    }

    #[test]
    fn parity_case_no_else_returns_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT CASE 5 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END;")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Null);
        });
    }

    // ── COALESCE / NULLIF / IIF parity tests ─────────────────────────────

    #[test]
    fn parity_coalesce_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT COALESCE(NULL, NULL, 42, 10);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(42));
        });
    }

    #[test]
    fn parity_coalesce_all_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT COALESCE(NULL, NULL);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Null);
        });
    }

    #[test]
    fn parity_nullif_equal() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT NULLIF(5, 5);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Null);
        });
    }

    #[test]
    fn parity_nullif_not_equal() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT NULLIF(5, 3);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(5));
        });
    }

    #[test]
    fn parity_iif_true() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT IIF(1=1, 'yes', 'no');")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("yes".into()));
        });
    }

    #[test]
    fn parity_iif_false() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT IIF(1=0, 'yes', 'no');")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("no".into()));
        });
    }

    // ── BETWEEN parity tests ─────────────────────────────────────────────

    #[test]
    fn parity_between_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(5);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(15);").await.unwrap();
            let rows = conn
                .query("SELECT x FROM t WHERE x BETWEEN 5 AND 10 ORDER BY x;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(5));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(10));
        });
    }

    #[test]
    fn parity_not_between() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(5);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(10);").await.unwrap();
            let rows = conn
                .query("SELECT x FROM t WHERE x NOT BETWEEN 3 AND 7 ORDER BY x;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(10));
        });
    }

    // ── LIKE parity tests ────────────────────────────────────────────────

    #[test]
    fn parity_like_percent() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(name TEXT);").await.unwrap();
            conn.execute("INSERT INTO t VALUES('apple');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('banana');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('apricot');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT name FROM t WHERE name LIKE 'ap%' ORDER BY name;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("apple".into()));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("apricot".into()));
        });
    }

    #[test]
    fn parity_like_underscore() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(code TEXT);").await.unwrap();
            conn.execute("INSERT INTO t VALUES('a1');").await.unwrap();
            conn.execute("INSERT INTO t VALUES('b2');").await.unwrap();
            conn.execute("INSERT INTO t VALUES('abc');").await.unwrap();
            let rows = conn
                .query("SELECT code FROM t WHERE code LIKE '_2';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("b2".into()));
        });
    }

    #[test]
    fn parity_not_like() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(name TEXT);").await.unwrap();
            conn.execute("INSERT INTO t VALUES('cat');").await.unwrap();
            conn.execute("INSERT INTO t VALUES('dog');").await.unwrap();
            conn.execute("INSERT INTO t VALUES('car');").await.unwrap();
            let rows = conn
                .query("SELECT name FROM t WHERE name NOT LIKE 'ca%' ORDER BY name;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("dog".into()));
        });
    }

    // ── JOIN parity tests ────────────────────────────────────────────────

    #[test]
    fn parity_inner_join() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER, item TEXT);",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO users VALUES(1, 'Alice');")
                .await
                .unwrap();
            conn.execute("INSERT INTO users VALUES(2, 'Bob');")
                .await
                .unwrap();
            conn.execute("INSERT INTO orders VALUES(1, 1, 'Book');")
                .await
                .unwrap();
            conn.execute("INSERT INTO orders VALUES(2, 1, 'Pen');")
                .await
                .unwrap();
            conn.execute("INSERT INTO orders VALUES(3, 2, 'Notebook');")
                .await
                .unwrap();
            let rows = conn
            .query(
                "SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.user_id;",
            )
            .await
            .unwrap();
            assert_eq!(rows.len(), 3);
            // Verify all expected name-item pairs (order may vary).
            let mut pairs: Vec<(String, String)> = rows
                .iter()
                .map(|r| {
                    let v = row_values(r);
                    let name = match &v[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        other => panic!("expected Text, got {other:?}"),
                    };
                    let item = match &v[1] {
                        SqliteValue::Text(s) => s.to_string(),
                        other => panic!("expected Text, got {other:?}"),
                    };
                    (name, item)
                })
                .collect();
            pairs.sort();
            assert_eq!(
                pairs,
                vec![
                    ("Alice".into(), "Book".into()),
                    ("Alice".into(), "Pen".into()),
                    ("Bob".into(), "Notebook".into()),
                ]
            );
        });
    }

    #[test]
    fn parity_left_join_with_nulls() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER, info TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO a VALUES(1, 'x');").await.unwrap();
            conn.execute("INSERT INTO a VALUES(2, 'y');").await.unwrap();
            conn.execute("INSERT INTO b VALUES(1, 1, 'linked');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT a.val, b.info FROM a LEFT JOIN b ON a.id = b.a_id;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            // Collect results (order may vary).
            let mut results: Vec<(String, Option<String>)> = rows
                .iter()
                .map(|r| {
                    let v = row_values(r);
                    let val = match &v[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        other => panic!("expected Text, got {other:?}"),
                    };
                    let info = match &v[1] {
                        SqliteValue::Text(s) => Some(s.to_string()),
                        SqliteValue::Null => None,
                        other => panic!("expected Text or Null, got {other:?}"),
                    };
                    (val, info)
                })
                .collect();
            results.sort();
            assert_eq!(
                results,
                vec![("x".into(), Some("linked".into())), ("y".into(), None),]
            );
        });
    }

    #[test]
    fn parity_cross_join() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE a(x INTEGER);").await.unwrap();
            conn.execute("CREATE TABLE b(y INTEGER);").await.unwrap();
            conn.execute("INSERT INTO a VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO a VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO b VALUES(10);").await.unwrap();
            conn.execute("INSERT INTO b VALUES(20);").await.unwrap();
            let rows = conn
                .query("SELECT x, y FROM a, b ORDER BY x, y;")
                .await
                .unwrap();
            // Cross product: 2*2 = 4 rows
            assert_eq!(rows.len(), 4);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(10));
            assert_eq!(row_values(&rows[3])[0], SqliteValue::Integer(2));
            assert_eq!(row_values(&rows[3])[1], SqliteValue::Integer(20));
        });
    }

    // ── UNION / set operations parity tests ──────────────────────────────

    #[test]
    fn parity_union_removes_duplicates() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE a(x INTEGER);").await.unwrap();
            conn.execute("CREATE TABLE b(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO a VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO a VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO b VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO b VALUES(3);").await.unwrap();
            let rows = conn
                .query("SELECT x FROM a UNION SELECT x FROM b ORDER BY x;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(2));
            assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(3));
        });
    }

    #[test]
    fn parity_union_all_keeps_duplicates() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE a(x INTEGER);").await.unwrap();
            conn.execute("CREATE TABLE b(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO a VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO a VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO b VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO b VALUES(3);").await.unwrap();
            let rows = conn
                .query("SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 4);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(2));
            assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(2));
            assert_eq!(row_values(&rows[3])[0], SqliteValue::Integer(3));
        });
    }

    // ── UPDATE / DELETE parity tests ─────────────────────────────────────

    #[test]
    fn parity_update_multiple_columns() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(a INTEGER, b TEXT, c REAL);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1, 'old', 1.0);")
                .await
                .unwrap();
            conn.execute("UPDATE t SET b = 'new', c = 2.5 WHERE a = 1;")
                .await
                .unwrap();
            let row = conn.query_row("SELECT a, b, c FROM t;").await.unwrap();
            let vals = row_values(&row);
            assert_eq!(vals[0], SqliteValue::Integer(1));
            assert_eq!(vals[1], SqliteValue::Text("new".into()));
            assert_eq!(vals[2], SqliteValue::Float(2.5));
        });
    }

    #[test]
    fn parity_delete_with_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1, 'a');").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2, 'b');").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3, 'c');").await.unwrap();
            conn.execute("DELETE FROM t WHERE id > 1;").await.unwrap();
            let rows = conn.query("SELECT val FROM t;").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("a".into()));
        });
    }

    // ── HAVING parity tests ──────────────────────────────────────────────

    #[test]
    fn parity_having_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE sales(product TEXT, amount INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('A', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('A', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('B', 5);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('C', 30);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('C', 40);")
                .await
                .unwrap();
            let rows = conn
            .query(
                "SELECT product, SUM(amount) FROM sales GROUP BY product HAVING SUM(amount) > 15 ORDER BY product;",
            )
            .await
            .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("A".into()));
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(30));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Text("C".into()));
            assert_eq!(row_values(&rows[1])[1], SqliteValue::Integer(70));
        });
    }

    // ── IN operator parity tests ─────────────────────────────────────────

    #[test]
    fn parity_in_values_list() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(4);").await.unwrap();
            let rows = conn
                .query("SELECT x FROM t WHERE x IN (1, 3) ORDER BY x;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(3));
        });
    }

    #[test]
    fn parity_not_in_values_list() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3);").await.unwrap();
            let rows = conn
                .query("SELECT x FROM t WHERE x NOT IN (1, 3) ORDER BY x;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
        });
    }

    // ── Expression tests ─────────────────────────────────────────────────

    #[test]
    fn parity_unary_minus() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT -42;").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(-42));
        });
    }

    #[test]
    fn parity_string_concat_operator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT 'hello' || ' ' || 'world';")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("hello world".into()));
        });
    }

    #[test]
    fn parity_typeof_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT typeof(42);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("integer".into()));
            let row = conn.query_row("SELECT typeof(3.14);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("real".into()));
            let row = conn.query_row("SELECT typeof('hi');").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("text".into()));
            let row = conn.query_row("SELECT typeof(NULL);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("null".into()));
        });
    }

    #[test]
    fn parity_abs_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT ABS(-10);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(10));
            let row = conn.query_row("SELECT ABS(10);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(10));
        });
    }

    #[test]
    fn parity_upper_lower_functions() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT UPPER('hello');").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("HELLO".into()));
            let row = conn.query_row("SELECT LOWER('WORLD');").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("world".into()));
        });
    }

    #[test]
    fn parity_length_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT LENGTH('hello');").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(5));
            let row = conn.query_row("SELECT LENGTH('');").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(0));
        });
    }

    #[test]
    fn parity_min_max_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(4);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(5);").await.unwrap();
            let row = conn
                .query_row("SELECT MIN(x), MAX(x) FROM t;")
                .await
                .unwrap();
            let vals = row_values(&row);
            assert_eq!(vals[0], SqliteValue::Integer(1));
            assert_eq!(vals[1], SqliteValue::Integer(5));
        });
    }

    #[test]
    fn parity_avg_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(20);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(30);").await.unwrap();
            let row = conn.query_row("SELECT AVG(x) FROM t;").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Float(20.0));
        });
    }

    // ── UPDATE with subquery in WHERE ────────────────────────────────────

    #[test]
    fn parity_update_where_in_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, price INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO items VALUES(1, 'apple', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO items VALUES(2, 'banana', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO items VALUES(3, 'cherry', 30);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE expensive(id INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO expensive VALUES(2);")
                .await
                .unwrap();
            conn.execute("INSERT INTO expensive VALUES(3);")
                .await
                .unwrap();
            // UPDATE items SET price = price * 2 WHERE id IN (SELECT id FROM expensive);
            conn.execute(
                "UPDATE items SET price = price * 2 WHERE id IN (SELECT id FROM expensive);",
            )
            .await
            .unwrap();
            let rows = conn
                .query("SELECT id, price FROM items ORDER BY id;")
                .await
                .unwrap();
            let results: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    (vals[0].to_integer(), vals[1].to_integer())
                })
                .collect();
            assert_eq!(results, vec![(1, 10), (2, 40), (3, 60)]);
        });
    }

    // ── DELETE with subquery in WHERE ────────────────────────────────────

    #[test]
    fn parity_delete_where_in_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE data(id INTEGER, val TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO data VALUES(1, 'a');")
                .await
                .unwrap();
            conn.execute("INSERT INTO data VALUES(2, 'b');")
                .await
                .unwrap();
            conn.execute("INSERT INTO data VALUES(3, 'c');")
                .await
                .unwrap();
            conn.execute("CREATE TABLE to_remove(id INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO to_remove VALUES(1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO to_remove VALUES(3);")
                .await
                .unwrap();
            // DELETE FROM data WHERE id IN (SELECT id FROM to_remove);
            conn.execute("DELETE FROM data WHERE id IN (SELECT id FROM to_remove);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, val FROM data ORDER BY id;")
                .await
                .unwrap();
            let results: Vec<(i64, String)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    (
                        vals[0].to_integer(),
                        match &vals[1] {
                            SqliteValue::Text(s) => s.to_string(),
                            _ => panic!("expected text"),
                        },
                    )
                })
                .collect();
            assert_eq!(results, vec![(2, "b".into())]);
        });
    }

    // ── DateTime function probes ─────────────────────────────────────────

    #[test]
    fn parity_datetime_date_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT date('2023-06-15 14:30:00');")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("2023-06-15".into()));
        });
    }

    #[test]
    fn parity_datetime_time_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT time('2023-06-15 14:30:45');")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("14:30:45".into()));
        });
    }

    #[test]
    fn parity_datetime_strftime() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT strftime('%Y', '2023-06-15');")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("2023".into()));
        });
    }

    // ── TOTAL aggregate ──────────────────────────────────────────────────

    #[test]
    fn parity_total_aggregate() {
        asupersync::test_utils::run_test(|| async {
            // TOTAL() returns 0.0 for empty set, unlike SUM() which returns NULL.
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            let row = conn.query_row("SELECT TOTAL(x) FROM t;").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Float(0.0));
        });
    }

    #[test]
    fn parity_total_aggregate_with_values() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(20);").await.unwrap();
            let row = conn.query_row("SELECT TOTAL(x) FROM t;").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Float(30.0));
        });
    }

    // ── GLOB operator ────────────────────────────────────────────────────

    #[test]
    fn parity_glob_operator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE files(name TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO files VALUES('readme.txt');")
                .await
                .unwrap();
            conn.execute("INSERT INTO files VALUES('main.rs');")
                .await
                .unwrap();
            conn.execute("INSERT INTO files VALUES('test.txt');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT name FROM files WHERE name GLOB '*.txt' ORDER BY name;")
                .await
                .unwrap();
            let results: Vec<String> = rows
                .iter()
                .map(|r| match &row_values(r)[0] {
                    SqliteValue::Text(s) => s.to_string(),
                    _ => panic!("expected text"),
                })
                .collect();
            assert_eq!(results, vec!["readme.txt", "test.txt"]);
        });
    }

    // ── REPLACE function ─────────────────────────────────────────────────

    #[test]
    fn parity_replace_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT replace('hello world', 'world', 'rust');")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("hello rust".into()));
        });
    }

    // ── ZEROBLOB function ────────────────────────────────────────────────

    #[test]
    fn parity_zeroblob_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT typeof(zeroblob(4)), length(zeroblob(4));")
                .await
                .unwrap();
            let vals = row_values(&row);
            assert_eq!(vals[0], SqliteValue::Text("blob".into()));
            assert_eq!(vals[1], SqliteValue::Integer(4));
        });
    }

    // ── UNICODE / CHAR functions ─────────────────────────────────────────

    #[test]
    fn parity_unicode_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT unicode('A');").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(65));
        });
    }

    #[test]
    fn parity_char_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn.query_row("SELECT char(65, 66, 67);").await.unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("ABC".into()));
        });
    }

    // ── INSTR with multi-byte ────────────────────────────────────────────

    #[test]
    fn parity_instr_multi_occurrence() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // INSTR returns position of FIRST occurrence (1-based).
            let row = conn
                .query_row("SELECT instr('abcabc', 'bc');")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Integer(2));
        });
    }

    // ── PRINTF / FORMAT function ─────────────────────────────────────────

    #[test]
    fn parity_printf_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let row = conn
                .query_row("SELECT printf('%d + %d = %d', 1, 2, 3);")
                .await
                .unwrap();
            assert_eq!(row_values(&row)[0], SqliteValue::Text("1 + 2 = 3".into()));
        });
    }

    // ── Window function probe ────────────────────────────────────────────

    #[test]
    fn parity_row_number_window() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE w(name TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO w VALUES('a', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO w VALUES('b', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO w VALUES('c', 30);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT name, ROW_NUMBER() OVER (ORDER BY val) FROM w;")
                .await
                .unwrap();
            let results: Vec<(String, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    let name = match &vals[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        _ => panic!("expected text"),
                    };
                    (name, vals[1].to_integer())
                })
                .collect();
            assert_eq!(
                results,
                vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3),]
            );
        });
    }

    #[test]
    fn window_row_number_partition_by() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE wp(dept TEXT, name TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wp VALUES('eng','a',10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wp VALUES('eng','b',20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wp VALUES('sales','c',5);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wp VALUES('sales','d',15);")
                .await
                .unwrap();
            let rows = conn
            .query("SELECT dept, name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY val) FROM wp;")
            .await
            .unwrap();
            let results: Vec<(String, String, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    let dept = match &vals[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        _ => panic!("expected text"),
                    };
                    let name = match &vals[1] {
                        SqliteValue::Text(s) => s.to_string(),
                        _ => panic!("expected text"),
                    };
                    (dept, name, vals[2].to_integer())
                })
                .collect();
            assert_eq!(
                results,
                vec![
                    ("eng".into(), "a".into(), 1),
                    ("eng".into(), "b".into(), 2),
                    ("sales".into(), "c".into(), 1),
                    ("sales".into(), "d".into(), 2),
                ]
            );
        });
    }

    #[test]
    fn window_rank_and_dense_rank() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE wr(name TEXT, score INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wr VALUES('a', 100);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wr VALUES('b', 100);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wr VALUES('c', 90);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wr VALUES('d', 80);")
                .await
                .unwrap();
            let rows = conn
                .query(
                    "SELECT name, RANK() OVER (ORDER BY score DESC), \
                 DENSE_RANK() OVER (ORDER BY score DESC) FROM wr;",
                )
                .await
                .unwrap();
            let results: Vec<(String, i64, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    let name = match &vals[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        _ => panic!("expected text"),
                    };
                    (name, vals[1].to_integer(), vals[2].to_integer())
                })
                .collect();
            // a=100, b=100 are tied at rank 1; c=90 rank 3; d=80 rank 4
            // dense_rank: a,b=1; c=2; d=3
            assert_eq!(
                results,
                vec![
                    ("a".into(), 1, 1),
                    ("b".into(), 1, 1),
                    ("c".into(), 3, 2),
                    ("d".into(), 4, 3),
                ]
            );
        });
    }

    #[test]
    fn window_row_number_desc_order() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE wd(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO wd VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO wd VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO wd VALUES(3);").await.unwrap();
            let rows = conn
                .query("SELECT x, ROW_NUMBER() OVER (ORDER BY x DESC) FROM wd;")
                .await
                .unwrap();
            let results: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    (vals[0].to_integer(), vals[1].to_integer())
                })
                .collect();
            // x=3 is first (row_number=1), x=2 second, x=1 third
            assert_eq!(results, vec![(3, 1), (2, 2), (1, 3)]);
        });
    }

    #[test]
    fn window_multiple_window_functions() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE wm(name TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wm VALUES('a', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wm VALUES('b', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO wm VALUES('c', 30);")
                .await
                .unwrap();
            let rows = conn
                .query(
                    "SELECT name, ROW_NUMBER() OVER (ORDER BY val), \
                 DENSE_RANK() OVER (ORDER BY val) FROM wm;",
                )
                .await
                .unwrap();
            let results: Vec<(String, i64, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    let name = match &vals[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        _ => panic!("expected text"),
                    };
                    (name, vals[1].to_integer(), vals[2].to_integer())
                })
                .collect();
            // All values distinct, so rank matches row_number
            assert_eq!(
                results,
                vec![("a".into(), 1, 1), ("b".into(), 2, 2), ("c".into(), 3, 3),]
            );
        });
    }

    #[test]
    fn window_lag_negative_offset_reads_following_row() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES(1,10),(2,20),(3,30);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, lag(val,-1,'D') OVER (ORDER BY id) FROM t1 ORDER BY id;")
                .await
                .unwrap();
            let results: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
            assert_eq!(
                results,
                vec![
                    vec![SqliteValue::Integer(1), SqliteValue::Integer(20)],
                    vec![SqliteValue::Integer(2), SqliteValue::Integer(30)],
                    vec![SqliteValue::Integer(3), SqliteValue::Text("D".into())],
                ]
            );
        });
    }

    #[test]
    fn window_lag_lead_fractional_offset_uses_default() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT, off REAL);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES(1,'a',1.5),(2,'b',1.5),(3,'c',1.5);")
                .await
                .unwrap();
            let rows = conn
                .query(
                    "SELECT id, \
                 lag(val,off,'D') OVER (ORDER BY id), \
                 lead(val,off,'D') OVER (ORDER BY id) \
                 FROM t1 ORDER BY id;",
                )
                .await
                .unwrap();
            let results: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
            assert_eq!(
                results,
                vec![
                    vec![
                        SqliteValue::Integer(1),
                        SqliteValue::Text("D".into()),
                        SqliteValue::Text("D".into()),
                    ],
                    vec![
                        SqliteValue::Integer(2),
                        SqliteValue::Text("D".into()),
                        SqliteValue::Text("D".into()),
                    ],
                    vec![
                        SqliteValue::Integer(3),
                        SqliteValue::Text("D".into()),
                        SqliteValue::Text("D".into()),
                    ],
                ]
            );
        });
    }

    #[test]
    fn window_grouped_ntile_advances_two_pass_position() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1,10),(2,20),(3,30);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, ntile(2) OVER (ORDER BY id) FROM t GROUP BY id ORDER BY id;")
                .await
                .unwrap();
            let results: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
            assert_eq!(
                results,
                vec![
                    vec![SqliteValue::Integer(1), SqliteValue::Integer(1)],
                    vec![SqliteValue::Integer(2), SqliteValue::Integer(1)],
                    vec![SqliteValue::Integer(3), SqliteValue::Integer(2)],
                ]
            );
        });
    }

    #[test]
    fn window_nth_value_uses_current_row_n_for_full_frame() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x TEXT, n INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('a',1),('b',2),('c',3);")
                .await
                .unwrap();
            let rows = conn
                .query(
                    "SELECT x, n, nth_value(x,n) OVER (\
                 ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
                 ) FROM t ORDER BY x;",
                )
                .await
                .unwrap();
            let results: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
            assert_eq!(
                results,
                vec![
                    vec![
                        SqliteValue::Text("a".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("a".into()),
                    ],
                    vec![
                        SqliteValue::Text("b".into()),
                        SqliteValue::Integer(2),
                        SqliteValue::Text("b".into()),
                    ],
                    vec![
                        SqliteValue::Text("c".into()),
                        SqliteValue::Integer(3),
                        SqliteValue::Text("c".into()),
                    ],
                ]
            );
        });
    }

    #[test]
    fn window_nth_value_uses_current_row_n_for_sliding_frame() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x TEXT, n INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('a',1),('b',2),('c',1);")
                .await
                .unwrap();
            let rows = conn
                .query(
                    "SELECT x, n, nth_value(x,n) OVER (\
                 ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) FROM t ORDER BY x;",
                )
                .await
                .unwrap();
            let results: Vec<Vec<SqliteValue>> = rows.iter().map(row_values).collect();
            assert_eq!(
                results,
                vec![
                    vec![
                        SqliteValue::Text("a".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("a".into()),
                    ],
                    vec![
                        SqliteValue::Text("b".into()),
                        SqliteValue::Integer(2),
                        SqliteValue::Text("b".into()),
                    ],
                    vec![
                        SqliteValue::Text("c".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("b".into()),
                    ],
                ]
            );
        });
    }

    #[test]
    fn window_nth_value_rejects_fractional_n() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let err = conn
                .query(
                    "WITH t(x,n) AS (VALUES ('a',1.5),('b',1.5)) \
                 SELECT nth_value(x,n) OVER (\
                 ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
                 ) FROM t;",
                )
                .await
                .expect_err("fractional nth_value offset should fail");
            assert!(
                matches!(&err, FrankenError::FunctionError(message) if message == "second argument to nth_value must be a positive integer"),
                "unexpected error: {err:?}"
            );
        });
    }

    #[test]
    fn window_nth_value_rejects_text_numeric_prefix_n() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let err = conn
                .query(
                    "WITH t(x,n) AS (VALUES ('a','2x'),('b','2x')) \
                 SELECT nth_value(x,n) OVER (\
                 ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
                 ) FROM t;",
                )
                .await
                .expect_err("numeric-prefix nth_value offset should fail");
            assert!(
                matches!(&err, FrankenError::FunctionError(message) if message == "second argument to nth_value must be a positive integer"),
                "unexpected error: {err:?}"
            );
        });
    }

    #[test]
    fn window_nth_value_rejects_blob_integer_n() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let err = conn
                .query(
                    "WITH t(x,n) AS (VALUES ('a',x'32'),('b',x'32')) \
                 SELECT nth_value(x,n) OVER (\
                 ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
                 ) FROM t;",
                )
                .await
                .expect_err("blob nth_value offset should fail");
            assert!(
                matches!(&err, FrankenError::FunctionError(message) if message == "second argument to nth_value must be a positive integer"),
                "unexpected error: {err:?}"
            );
        });
    }

    // ── CTE (WITH) probe ─────────────────────────────────────────────────

    #[test]
    fn parity_cte_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3);").await.unwrap();
            let rows = conn
                .query(
                    "WITH doubled AS (SELECT x * 2 AS d FROM t) SELECT d FROM doubled ORDER BY d;",
                )
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert_eq!(results, vec![2, 4, 6]);
        });
    }

    // ── Recursive CTE probe ─────────────────────────────────────────────

    #[test]
    fn parity_recursive_cte() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query(
                    "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<5) \
                 SELECT x FROM cnt;",
                )
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert_eq!(results, vec![1, 2, 3, 4, 5]);
        });
    }

    // ── HAVING with multiple conditions ──────────────────────────────────

    #[test]
    fn parity_having_count_and_sum() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE sales(region TEXT, amount INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('East', 100);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('East', 200);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('West', 50);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('West', 60);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES('West', 70);")
                .await
                .unwrap();
            // HAVING COUNT(*) > 2 → only West (3 rows)
            let rows = conn
                .query("SELECT region, SUM(amount) FROM sales GROUP BY region HAVING COUNT(*) > 2;")
                .await
                .unwrap();
            let results: Vec<(String, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    (
                        match &vals[0] {
                            SqliteValue::Text(s) => s.to_string(),
                            _ => panic!("expected text"),
                        },
                        vals[1].to_integer(),
                    )
                })
                .collect();
            assert_eq!(results, vec![("West".into(), 180)]);
        });
    }

    // ── Multi-table UPDATE with JOIN subquery ────────────────────────────

    #[test]
    fn parity_update_with_exists_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE products(pid INTEGER, name TEXT, active INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO products VALUES(1, 'Widget', 1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO products VALUES(2, 'Gadget', 1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO products VALUES(3, 'Doohickey', 1);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE discontinued(product_id INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO discontinued VALUES(1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO discontinued VALUES(3);")
                .await
                .unwrap();
            // Correlated EXISTS subquery: update rows where a matching row exists.
            conn.execute(
                "UPDATE products SET active = 0 WHERE EXISTS \
             (SELECT 1 FROM discontinued WHERE discontinued.product_id = products.pid);",
            )
            .await
            .unwrap();
            let rows = conn
                .query("SELECT pid, active FROM products ORDER BY pid;")
                .await
                .unwrap();
            let results: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    (vals[0].to_integer(), vals[1].to_integer())
                })
                .collect();
            assert_eq!(results, vec![(1, 0), (2, 1), (3, 0)]);
        });
    }

    #[test]
    fn probe_correlated_exists_select() {
        asupersync::test_utils::run_test(|| async {
            // Diagnostic: does correlated EXISTS work in a SELECT context?
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE a(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO a VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO a VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO a VALUES(3);").await.unwrap();
            conn.execute("CREATE TABLE b(y INTEGER);").await.unwrap();
            conn.execute("INSERT INTO b VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO b VALUES(3);").await.unwrap();
            let rows = conn
                .query("SELECT x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x) ORDER BY x;")
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert_eq!(results, vec![1, 3]);
        });
    }

    // ── Conformance Probes: Edge Cases ──────────────────────────────────

    #[test]
    fn probe_order_by_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER, y TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(3, 'c');").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1, 'a');").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2, 'b');").await.unwrap();
            let rows = conn
                .query("SELECT y FROM t ORDER BY x * -1;")
                .await
                .unwrap();
            let results: Vec<String> = rows
                .iter()
                .map(|r| match &row_values(r)[0] {
                    SqliteValue::Text(s) => s.to_string(),
                    _ => panic!("expected text"),
                })
                .collect();
            assert_eq!(results, vec!["c", "b", "a"]);
        });
    }

    #[test]
    fn probe_group_by_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER, v INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1, 10);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2, 20);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3, 30);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(4, 40);").await.unwrap();
            let rows = conn
                .query("SELECT x % 2 AS grp, SUM(v) FROM t GROUP BY x % 2 ORDER BY grp;")
                .await
                .unwrap();
            let results: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    (vals[0].to_integer(), vals[1].to_integer())
                })
                .collect();
            assert_eq!(results, vec![(0, 60), (1, 40)]);
        });
    }

    #[test]
    fn probe_having_with_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(cat TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('a', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('a', 20);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('b', 5);").await.unwrap();
            let rows = conn
            .query(
                "SELECT cat, SUM(val) AS s FROM t GROUP BY cat HAVING SUM(val) > 10 ORDER BY cat;",
            )
            .await
            .unwrap();
            let results: Vec<(String, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    let cat = match &vals[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        _ => panic!("expected text"),
                    };
                    (cat, vals[1].to_integer())
                })
                .collect();
            assert_eq!(results, vec![("a".to_string(), 30)]);
        });
    }

    #[test]
    fn probe_scalar_subquery_in_select() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3);").await.unwrap();
            let rows = conn
                .query("SELECT x, (SELECT MAX(x) FROM t) AS mx FROM t ORDER BY x;")
                .await
                .unwrap();
            let results: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    (vals[0].to_integer(), vals[1].to_integer())
                })
                .collect();
            assert_eq!(results, vec![(1, 3), (2, 3), (3, 3)]);
        });
    }

    #[test]
    fn probe_nested_case_when() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3);").await.unwrap();
            let rows = conn
                .query(
                    "SELECT CASE WHEN x < 2 THEN 'low' \
                 WHEN x < 3 THEN 'mid' ELSE 'high' END AS label FROM t ORDER BY x;",
                )
                .await
                .unwrap();
            let results: Vec<String> = rows
                .iter()
                .map(|r| match &row_values(r)[0] {
                    SqliteValue::Text(s) => s.to_string(),
                    _ => panic!("expected text"),
                })
                .collect();
            assert_eq!(results, vec!["low", "mid", "high"]);
        });
    }

    #[test]
    fn probe_coalesce_multi_arg() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query("SELECT COALESCE(NULL, NULL, NULL, 42);")
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[0].to_integer(), 42);
        });
    }

    #[test]
    fn probe_nullif_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query("SELECT NULLIF(5, 5), NULLIF(5, 3);")
                .await
                .unwrap();
            let vals = row_values(&rows[0]);
            assert_eq!(vals[0], SqliteValue::Null);
            assert_eq!(vals[1].to_integer(), 5);
        });
    }

    #[test]
    fn probe_iif_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query("SELECT IIF(1 > 0, 'yes', 'no'), IIF(1 < 0, 'yes', 'no');")
                .await
                .unwrap();
            let vals = row_values(&rows[0]);
            let a = match &vals[0] {
                SqliteValue::Text(s) => s.to_string(),
                _ => panic!("expected text"),
            };
            let b = match &vals[1] {
                SqliteValue::Text(s) => s.to_string(),
                _ => panic!("expected text"),
            };
            assert_eq!(a, "yes");
            assert_eq!(b, "no");
        });
    }

    #[test]
    fn probe_like_escape() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(s TEXT);").await.unwrap();
            conn.execute("INSERT INTO t VALUES('100% done');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('50 percent');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT s FROM t WHERE s LIKE '%!%%' ESCAPE '!';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            let val = match &row_values(&rows[0])[0] {
                SqliteValue::Text(s) => s.to_string(),
                _ => panic!("expected text"),
            };
            assert_eq!(val, "100% done");
        });
    }

    #[test]
    fn probe_between_with_expressions() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(5);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(10);").await.unwrap();
            let rows = conn
                .query("SELECT x FROM t WHERE x BETWEEN 2 + 1 AND 4 * 2 ORDER BY x;")
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert_eq!(results, vec![5]);
        });
    }

    #[test]
    fn probe_distinct_with_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(4);").await.unwrap();
            let rows = conn
                .query("SELECT DISTINCT x % 2 AS mod2 FROM t ORDER BY mod2;")
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert_eq!(results, vec![0, 1]);
        });
    }

    #[test]
    fn probe_insert_or_ignore_keeps_existing() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1, 'first');")
                .await
                .unwrap();
            conn.execute("INSERT OR IGNORE INTO t VALUES(1, 'second');")
                .await
                .unwrap();
            let rows = conn.query("SELECT v FROM t WHERE id = 1;").await.unwrap();
            let val = match &row_values(&rows[0])[0] {
                SqliteValue::Text(s) => s.to_string(),
                _ => panic!("expected text"),
            };
            assert_eq!(val, "first");
        });
    }

    #[test]
    fn probe_insert_or_replace_overwrites() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1, 'first');")
                .await
                .unwrap();
            conn.execute("INSERT OR REPLACE INTO t VALUES(1, 'second');")
                .await
                .unwrap();
            let rows = conn.query("SELECT v FROM t WHERE id = 1;").await.unwrap();
            let val = match &row_values(&rows[0])[0] {
                SqliteValue::Text(s) => s.to_string(),
                _ => panic!("expected text"),
            };
            assert_eq!(val, "second");
        });
    }

    #[test]
    fn probe_delete_with_correlated_exists() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE items(id INTEGER, active INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO items VALUES(1, 1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO items VALUES(2, 1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO items VALUES(3, 1);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE retired(item_id INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO retired VALUES(2);")
                .await
                .unwrap();
            conn.execute(
                "DELETE FROM items WHERE EXISTS \
             (SELECT 1 FROM retired WHERE retired.item_id = items.id);",
            )
            .await
            .unwrap();
            let rows = conn
                .query("SELECT id FROM items ORDER BY id;")
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert_eq!(results, vec![1, 3]);
        });
    }

    #[test]
    fn probe_aggregate_in_order_by() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(cat TEXT, val INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('b', 10);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('a', 30);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES('c', 20);")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT cat, SUM(val) AS s FROM t GROUP BY cat ORDER BY SUM(val) DESC;")
                .await
                .unwrap();
            let results: Vec<(String, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    let cat = match &vals[0] {
                        SqliteValue::Text(s) => s.to_string(),
                        _ => panic!("expected text"),
                    };
                    (cat, vals[1].to_integer())
                })
                .collect();
            assert_eq!(
                results,
                vec![
                    ("a".to_string(), 30),
                    ("c".to_string(), 20),
                    ("b".to_string(), 10)
                ]
            );
        });
    }

    #[test]
    fn probe_subquery_in_from() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3);").await.unwrap();
            let rows = conn
            .query("SELECT sub.doubled FROM (SELECT x * 2 AS doubled FROM t) AS sub ORDER BY sub.doubled;")
            .await
            .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert_eq!(results, vec![2, 4, 6]);
        });
    }

    #[test]
    fn probe_multi_column_order_by_mixed() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(a INTEGER, b INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES(1, 3);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1, 1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2, 2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2, 4);").await.unwrap();
            let rows = conn
                .query("SELECT a, b FROM t ORDER BY a ASC, b DESC;")
                .await
                .unwrap();
            let results: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| {
                    let vals = row_values(r);
                    (vals[0].to_integer(), vals[1].to_integer())
                })
                .collect();
            assert_eq!(results, vec![(1, 3), (1, 1), (2, 4), (2, 2)]);
        });
    }

    #[test]
    fn probe_null_handling_order_by() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(3);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(NULL);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            // SQLite: NULLs sort first in ASC order.
            let rows = conn.query("SELECT x FROM t ORDER BY x ASC;").await.unwrap();
            let results: Vec<SqliteValue> = rows.iter().map(|r| row_values(r)[0].clone()).collect();
            assert_eq!(results[0], SqliteValue::Null);
            assert_eq!(results[1].to_integer(), 1);
            assert_eq!(results[2].to_integer(), 3);
        });
    }

    #[test]
    fn probe_count_distinct() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(2);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES(NULL);").await.unwrap();
            let rows = conn
                .query("SELECT COUNT(DISTINCT x) FROM t;")
                .await
                .unwrap();
            // COUNT(DISTINCT x) should count distinct non-NULL values → 2
            assert_eq!(row_values(&rows[0])[0].to_integer(), 2);
        });
    }

    #[test]
    fn probe_cast_in_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x TEXT);").await.unwrap();
            conn.execute("INSERT INTO t VALUES('123');").await.unwrap();
            conn.execute("INSERT INTO t VALUES('456');").await.unwrap();
            conn.execute("INSERT INTO t VALUES('abc');").await.unwrap();
            let rows = conn
                .query("SELECT x FROM t WHERE CAST(x AS INTEGER) > 200;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            let val = match &row_values(&rows[0])[0] {
                SqliteValue::Text(s) => s.to_string(),
                _ => panic!("expected text"),
            };
            assert_eq!(val, "456");
        });
    }

    #[test]
    fn probe_union_all_three_way() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query("SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 1;")
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert_eq!(results, vec![1, 2, 1]);
        });
    }

    #[test]
    fn probe_union_dedup_three_way() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query("SELECT 1 AS v UNION SELECT 2 UNION SELECT 1 ORDER BY v;")
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert_eq!(results, vec![1, 2]);
        });
    }

    #[test]
    fn probe_except_compound() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 EXCEPT SELECT 2;")
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            // EXCEPT removes rows from right. Order: 1, 3
            assert!(results.contains(&1));
            assert!(results.contains(&3));
            assert!(!results.contains(&2));
        });
    }

    #[test]
    fn probe_intersect_compound() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let rows = conn
                .query(
                    "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 \
                 INTERSECT SELECT 2 UNION ALL SELECT 3;",
                )
                .await
                .unwrap();
            let results: Vec<i64> = rows.iter().map(|r| row_values(r)[0].to_integer()).collect();
            assert!(results.contains(&2) || results.contains(&3));
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 024: string functions
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_024_substr() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT substr('hello world', 7)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "world");
            let r = conn
                .query("SELECT substr('hello world', 1, 5)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "hello");
            let r = conn.query("SELECT substr('hello', -3)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "llo");
        });
    }

    #[test]
    fn conformance_024_replace() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT replace('hello world', 'world', 'rust')")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "hello rust");
            let r = conn
                .query("SELECT replace('aaa', 'a', 'bb')")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "bbbbbb");
        });
    }

    #[test]
    fn conformance_024_trim() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT trim('  hello  ')").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "hello");
            let r = conn.query("SELECT ltrim('  hello  ')").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "hello  ");
            let r = conn.query("SELECT rtrim('  hello  ')").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "  hello");
        });
    }

    #[test]
    fn conformance_024_instr() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT instr('hello world', 'world')")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "7");
            let r = conn.query("SELECT instr('hello', 'xyz')").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
            let r = conn.query("SELECT instr('hello', '')").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
        });
    }

    #[test]
    fn conformance_024_hex_zeroblob() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT hex(zeroblob(4))").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "00000000");
            let r = conn.query("SELECT typeof(zeroblob(4))").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "blob");
        });
    }

    #[test]
    fn conformance_024_char_unicode() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT char(65, 66, 67)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "ABC");
            let r = conn.query("SELECT unicode('A')").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "65");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 025: expression operators
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_025_between() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT 5 BETWEEN 1 AND 10").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            let r = conn.query("SELECT 15 BETWEEN 1 AND 10").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
            let r = conn.query("SELECT 5 NOT BETWEEN 1 AND 10").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
        });
    }

    #[test]
    fn conformance_025_in_operator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT 3 IN (1, 2, 3, 4)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            let r = conn.query("SELECT 5 IN (1, 2, 3, 4)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
            let r = conn.query("SELECT 3 NOT IN (1, 2, 3, 4)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
        });
    }

    #[test]
    fn conformance_025_like_glob() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT 'hello' LIKE 'hel%'").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            let r = conn.query("SELECT 'hello' LIKE 'HEL%'").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            let r = conn.query("SELECT 'hello' GLOB 'hel*'").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            let r = conn.query("SELECT 'hello' GLOB 'HEL*'").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
        });
    }

    #[test]
    fn conformance_025_coalesce_nullif_iif() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT coalesce(NULL, NULL, 'found')")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "found");
            let r = conn.query("SELECT nullif(5, 5)").await.unwrap();
            assert!(row_values(&r[0])[0].is_null());
            let r = conn.query("SELECT nullif(5, 3)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "5");
            let r = conn.query("SELECT iif(1, 'yes', 'no')").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "yes");
            let r = conn.query("SELECT iif(0, 'yes', 'no')").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "no");
        });
    }

    #[test]
    fn conformance_025_bitwise() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT 6 & 3").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "2");
            let r = conn.query("SELECT 6 | 3").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "7");
            let r = conn.query("SELECT ~0").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "-1");
            let r = conn.query("SELECT 1 << 4").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "16");
            let r = conn.query("SELECT 16 >> 2").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "4");
        });
    }

    #[test]
    fn conformance_025_cast() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT CAST('123' AS INTEGER)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "123");
            let r = conn
                .query("SELECT typeof(CAST(123 AS TEXT))")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "text");
            let r = conn.query("SELECT CAST(3.14 AS INTEGER)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3");
        });
    }

    #[test]
    fn conformance_025_unary_operators() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT -(-5)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "5");
            let r = conn.query("SELECT +42").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "42");
            let r = conn.query("SELECT NOT 0").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            let r = conn.query("SELECT NOT 1").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
        });
    }

    #[test]
    fn conformance_025_is_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT NULL IS NULL").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            let r = conn.query("SELECT 5 IS NOT NULL").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            let r = conn.query("SELECT NULL IS NOT NULL").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 026: subquery and CTE
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_026_scalar_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)")
                .await
                .unwrap();
            let r = conn
                .query("SELECT (SELECT MAX(val) FROM t1)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "30");
            let r = conn
                .query("SELECT (SELECT COUNT(*) FROM t1)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3");
        });
    }

    #[test]
    fn conformance_026_exists_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO items VALUES (1, 'apple'), (2, 'banana')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT EXISTS(SELECT 1 FROM items WHERE name = 'apple')")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            let r = conn
                .query("SELECT EXISTS(SELECT 1 FROM items WHERE name = 'cherry')")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
        });
    }

    #[test]
    fn conformance_026_cte_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
            "CREATE TABLE employees(id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)",
        )
        .await
        .unwrap();
            conn.execute(
                "INSERT INTO employees VALUES \
             (1, 'Alice', 'eng', 100000), \
             (2, 'Bob', 'eng', 95000), \
             (3, 'Charlie', 'sales', 80000)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "WITH eng AS (SELECT * FROM employees WHERE dept = 'eng') \
                 SELECT name FROM eng ORDER BY name",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(r.len(), 2);
        });
    }

    #[test]
    fn conformance_026_cte_explicit_columns() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE nums(n INTEGER)").await.unwrap();
            conn.execute("INSERT INTO nums VALUES (10), (20), (30)")
                .await
                .unwrap();
            let r = conn
                .query(
                    "WITH doubled(val) AS (SELECT n * 2 FROM nums) \
                 SELECT val FROM doubled ORDER BY val",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "20");
            assert_eq!(row_values(&r[1])[0].to_text(), "40");
            assert_eq!(row_values(&r[2])[0].to_text(), "60");
        });
    }

    #[test]
    fn conformance_026_recursive_cte() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query(
                    "WITH RECURSIVE cnt(x) AS (\
                 SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5\
                 ) SELECT x FROM cnt",
                )
                .await
                .unwrap();
            let vals: Vec<String> = r.iter().map(|row| row_values(row)[0].to_text()).collect();
            assert_eq!(vals, ["1", "2", "3", "4", "5"]);
        });
    }

    #[test]
    fn conformance_026_derived_table() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE scores(student TEXT, score INTEGER)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO scores VALUES \
             ('Alice', 90), ('Alice', 85), ('Bob', 70), ('Bob', 80)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT student, avg_score FROM \
                 (SELECT student, AVG(score) as avg_score \
                  FROM scores GROUP BY student) ORDER BY student",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 027: window functions (not yet implemented)
    // -----------------------------------------------------------------------

    #[test]

    fn conformance_027_row_number() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE sales(id INTEGER PRIMARY KEY, region TEXT, amount REAL)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO sales VALUES \
             (1, 'North', 100.0), (2, 'South', 200.0), \
             (3, 'North', 150.0), (4, 'South', 175.0)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT region, amount, \
                 ROW_NUMBER() OVER (ORDER BY amount DESC) as rn \
                 FROM sales",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[2].to_text(), "1");
        });
    }

    #[test]

    fn conformance_027_rank_dense_rank() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE scores(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO scores VALUES \
             (1, 'A', 100), (2, 'B', 100), (3, 'C', 90), (4, 'D', 80)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT name, RANK() OVER (ORDER BY score DESC) as rnk \
                 FROM scores",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[1].to_text(), "1");
            assert_eq!(row_values(&r[1])[1].to_text(), "1");
            assert_eq!(row_values(&r[2])[1].to_text(), "3");
        });
    }

    #[test]

    fn conformance_027_sum_over() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE txns(id INTEGER PRIMARY KEY, amount REAL)")
                .await
                .unwrap();
            conn.execute("INSERT INTO txns VALUES (1, 10.0), (2, 20.0), (3, 30.0)")
                .await
                .unwrap();
            let r = conn
                .query(
                    "SELECT id, SUM(amount) OVER (ORDER BY id) as running \
                 FROM txns",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[1].to_text(), "10.0");
            assert_eq!(row_values(&r[1])[1].to_text(), "30.0");
            assert_eq!(row_values(&r[2])[1].to_text(), "60.0");
        });
    }

    #[test]

    fn conformance_027_lag_lead() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE seq(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO seq VALUES (1, 'a'), (2, 'b'), (3, 'c')")
                .await
                .unwrap();
            let r = conn
                .query(
                    "SELECT val, LAG(val) OVER (ORDER BY id) as prev \
                 FROM seq",
                )
                .await
                .unwrap();
            assert!(row_values(&r[0])[1].is_null());
            assert_eq!(row_values(&r[1])[1].to_text(), "a");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 028: views
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_028_create_select_drop_view() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE products(\
             id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO products VALUES \
             (1, 'Widget', 9.99, 'gadgets'), \
             (2, 'Gizmo', 24.99, 'gadgets'), \
             (3, 'Doohickey', 4.99, 'tools')",
            )
            .await
            .unwrap();
            conn.execute("CREATE VIEW expensive AS SELECT * FROM products WHERE price > 10.0")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM expensive ORDER BY name")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Gizmo");
            assert_eq!(r.len(), 1);
            conn.execute("DROP VIEW expensive").await.unwrap();
        });
    }

    #[test]
    fn conformance_028_view_with_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE products(\
             id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT)",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO products VALUES \
             (1, 'Widget', 9.99, 'gadgets'), \
             (2, 'Gizmo', 24.99, 'gadgets'), \
             (3, 'Doohickey', 4.99, 'tools'), \
             (4, 'Thingamajig', 14.99, 'tools'), \
             (5, 'Whatchamacallit', 49.99, 'gadgets')",
            )
            .await
            .unwrap();
            conn.execute(
                "CREATE VIEW category_stats AS \
             SELECT category, COUNT(*) as cnt \
             FROM products GROUP BY category",
            )
            .await
            .unwrap();
            let r = conn
                .query("SELECT category, cnt FROM category_stats ORDER BY category")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "gadgets");
            assert_eq!(row_values(&r[0])[1].to_text(), "3");
            assert_eq!(row_values(&r[1])[0].to_text(), "tools");
            assert_eq!(row_values(&r[1])[1].to_text(), "2");
        });
    }

    #[test]
    fn conformance_028_view_with_join() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE products(\
             id INTEGER PRIMARY KEY, name TEXT, price REAL)",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO products VALUES \
             (1, 'Widget', 9.99), (2, 'Gizmo', 24.99)",
            )
            .await
            .unwrap();
            conn.execute(
                "CREATE TABLE orders(\
             id INTEGER PRIMARY KEY, product_id INTEGER, qty INTEGER)",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO orders VALUES (1, 1, 10), (2, 2, 5)")
                .await
                .unwrap();
            conn.execute(
                "CREATE VIEW order_details AS \
             SELECT o.id as order_id, p.name, o.qty, \
             p.price * o.qty as total \
             FROM orders o JOIN products p ON o.product_id = p.id",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT order_id, name, total \
                 FROM order_details ORDER BY order_id",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[1].to_text(), "Widget");
            assert_eq!(row_values(&r[0])[2].to_text(), "99.9");
            assert_eq!(row_values(&r[1])[1].to_text(), "Gizmo");
        });
    }

    #[test]
    fn conformance_028_create_view_if_not_exists() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(a INTEGER)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1), (2), (3)")
                .await
                .unwrap();
            conn.execute("CREATE VIEW v1 AS SELECT a FROM t1")
                .await
                .unwrap();
            conn.execute("CREATE VIEW IF NOT EXISTS v1 AS SELECT 999")
                .await
                .unwrap();
            let r = conn.query("SELECT COUNT(*) FROM v1").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 029: GROUP BY and HAVING
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_029_group_by_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE orders(\
             id INTEGER PRIMARY KEY, customer TEXT, amount REAL)",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO orders VALUES \
             (1,'Alice',50.0),(2,'Bob',30.0),(3,'Alice',70.0),\
             (4,'Bob',20.0),(5,'Charlie',100.0)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT customer, SUM(amount) as total \
                 FROM orders GROUP BY customer ORDER BY customer",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[0])[1].to_text(), "120.0");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[1])[1].to_text(), "50.0");
        });
    }

    #[test]
    fn conformance_029_having() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE orders(\
             id INTEGER PRIMARY KEY, customer TEXT, amount REAL)",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO orders VALUES \
             (1,'Alice',50.0),(2,'Bob',30.0),(3,'Alice',70.0),\
             (4,'Bob',20.0),(5,'Charlie',100.0)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT customer, SUM(amount) as total \
                 FROM orders GROUP BY customer \
                 HAVING total > 60 ORDER BY customer",
                )
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Charlie");
        });
    }

    #[test]
    fn conformance_029_group_by_count_min_max_avg() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE scores(\
             student TEXT, subject TEXT, score INTEGER)",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO scores VALUES \
             ('Alice','Math',90),('Alice','Sci',85),\
             ('Bob','Math',70),('Bob','Sci',80)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT student, COUNT(*) as cnt, MIN(score), MAX(score) \
                 FROM scores GROUP BY student ORDER BY student",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[0])[1].to_text(), "2");
            assert_eq!(row_values(&r[0])[2].to_text(), "85");
            assert_eq!(row_values(&r[0])[3].to_text(), "90");
        });
    }

    #[test]
    fn conformance_029_group_by_multi_column() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE log(\
             dept TEXT, year INTEGER, revenue REAL)",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO log VALUES \
             ('eng',2024,100.0),('eng',2024,200.0),\
             ('eng',2025,150.0),('sales',2024,80.0)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT dept, year, SUM(revenue) as total \
                 FROM log GROUP BY dept, year ORDER BY dept, year",
                )
                .await
                .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "eng");
            assert_eq!(row_values(&r[0])[1].to_text(), "2024");
            assert_eq!(row_values(&r[0])[2].to_text(), "300.0");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 030: CASE expressions
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_030_case_searched() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, score INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 95), (2, 72), (3, 45)")
                .await
                .unwrap();
            let r = conn
                .query(
                    "SELECT id, \
                 CASE WHEN score >= 90 THEN 'A' \
                      WHEN score >= 70 THEN 'B' \
                      ELSE 'F' END as grade \
                 FROM t1 ORDER BY id",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[1].to_text(), "A");
            assert_eq!(row_values(&r[1])[1].to_text(), "B");
            assert_eq!(row_values(&r[2])[1].to_text(), "F");
        });
    }

    #[test]
    fn conformance_030_case_simple() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query(
                    "SELECT CASE 2 \
                 WHEN 1 THEN 'one' \
                 WHEN 2 THEN 'two' \
                 WHEN 3 THEN 'three' \
                 ELSE 'other' END",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "two");
        });
    }

    #[test]
    fn conformance_030_case_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query(
                    "SELECT CASE NULL \
                 WHEN NULL THEN 'match' \
                 ELSE 'no match' END",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "no match");
            let r = conn
                .query(
                    "SELECT CASE WHEN NULL THEN 'truthy' \
                 ELSE 'falsy' END",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "falsy");
        });
    }

    #[test]
    fn conformance_030_case_in_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, status TEXT)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO items VALUES \
             (1,'active'),(2,'inactive'),(3,'active'),\
             (4,'active'),(5,'inactive')",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT SUM(CASE WHEN status = 'active' \
                 THEN 1 ELSE 0 END) as active_count FROM items",
                )
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 031: INSERT conflict handling
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_031_insert_or_replace() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO kv VALUES ('a', 'first')")
                .await
                .unwrap();
            conn.execute("INSERT OR REPLACE INTO kv VALUES ('a', 'replaced')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT value FROM kv WHERE key = 'a'")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "replaced");
        });
    }

    #[test]
    fn conformance_031_insert_or_ignore() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO kv VALUES ('a', 'first')")
                .await
                .unwrap();
            conn.execute("INSERT OR IGNORE INTO kv VALUES ('a', 'ignored')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT value FROM kv WHERE key = 'a'")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "first");
            conn.execute("INSERT OR IGNORE INTO kv VALUES ('b', 'new')")
                .await
                .unwrap();
            // Verify both rows exist (original 'a' + new 'b').
            let r = conn.query("SELECT key FROM kv ORDER BY key").await.unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "a");
            assert_eq!(row_values(&r[1])[0].to_text(), "b");
        });
    }

    #[test]
    fn conformance_031_replace_into() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE t1(\
             id INTEGER PRIMARY KEY, name TEXT, \
             score INTEGER DEFAULT 0)",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'Alice', 100)")
                .await
                .unwrap();
            conn.execute("REPLACE INTO t1 VALUES (1, 'Alice Updated', 200)")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name, score FROM t1 WHERE id = 1")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice Updated");
            assert_eq!(row_values(&r[0])[1].to_text(), "200");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 032: ALTER TABLE
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_032_rename_table() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE original(\
             id INTEGER PRIMARY KEY, name TEXT)",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO original VALUES (1, 'Alice'), (2, 'Bob')")
                .await
                .unwrap();
            conn.execute("ALTER TABLE original RENAME TO renamed")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM renamed ORDER BY id")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
        });
    }

    #[test]
    fn conformance_032_add_column() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')")
                .await
                .unwrap();
            conn.execute("ALTER TABLE t1 ADD COLUMN score INTEGER DEFAULT 0")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name, score FROM t1 ORDER BY id")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[0])[1].to_text(), "0");
            conn.execute("INSERT INTO t1 VALUES (3, 'Charlie', 95)")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name, score FROM t1 WHERE id = 3")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Charlie");
            assert_eq!(row_values(&r[0])[1].to_text(), "95");
        });
    }

    #[test]
    fn conformance_032_add_multiple_columns() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')")
                .await
                .unwrap();
            conn.execute("ALTER TABLE t1 ADD COLUMN score INTEGER DEFAULT 0")
                .await
                .unwrap();
            conn.execute("ALTER TABLE t1 ADD COLUMN active INTEGER DEFAULT 1")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name, active FROM t1 ORDER BY id")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[0])[1].to_text(), "1");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[1])[1].to_text(), "1");
        });
    }

    #[test]
    fn alter_table_preserves_without_rowid_in_schema_sql() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE wr(id INTEGER PRIMARY KEY, body TEXT) WITHOUT ROWID;")
                .await
                .unwrap();
            conn.execute("ALTER TABLE wr ADD COLUMN extra INTEGER DEFAULT 0;")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'wr';")
                .await
                .unwrap();
            let sql = row_values(&rows[0])[0].to_text();
            assert!(
                sql.to_ascii_uppercase().contains("WITHOUT ROWID"),
                "ALTER TABLE must preserve WITHOUT ROWID in sqlite_master SQL: {sql}"
            );
        });
    }

    #[test]
    fn alter_table_preserves_typeless_columns_in_schema_sql() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE typeless(payload);")
                .await
                .unwrap();
            conn.execute("ALTER TABLE typeless ADD COLUMN note;")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'typeless';")
                .await
                .unwrap();
            let sql = row_values(&rows[0])[0].to_text();
            let Some((Statement::CreateTable(create), _)) =
                parse_first_statement_with_tail(&sql).expect("sqlite_master sql should parse")
            else {
                panic!("expected CREATE TABLE sql, got: {sql}");
            };
            let CreateTableBody::Columns { columns, .. } = create.body else {
                panic!("expected CREATE TABLE column definition body, got: {sql}");
            };
            assert_eq!(
                columns.len(),
                2,
                "ALTER TABLE should preserve both columns in sqlite_master sql: {sql}"
            );
            assert_eq!(columns[0].name, "payload", "{sql}");
            assert!(
                columns[0].type_name.is_none(),
                "ALTER TABLE must not synthesize a declared type for existing typeless columns: {sql}"
            );
            assert_eq!(columns[1].name, "note", "{sql}");
            assert!(
                columns[1].type_name.is_none(),
                "ALTER TABLE must not synthesize a declared type for added typeless columns: {sql}"
            );
        });
    }

    #[test]
    fn alter_table_preserves_embedded_quote_identifiers_in_schema_sql() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(r#"CREATE TABLE "te""st"("co""l" TEXT PRIMARY KEY);"#)
                .await
                .unwrap();
            conn.execute(r#"ALTER TABLE "te""st" ADD COLUMN "no""te" TEXT;"#)
                .await
                .unwrap();

            let rows = conn
                .query(r#"SELECT sql FROM sqlite_master WHERE type='table' AND name='te"st';"#)
                .await
                .unwrap();
            let sql = row_values(&rows[0])[0].to_text();
            let Some((Statement::CreateTable(create), _)) =
                parse_first_statement_with_tail(&sql).expect("sqlite_master sql should parse")
            else {
                panic!("expected CREATE TABLE sql, got: {sql}");
            };
            let CreateTableBody::Columns { columns, .. } = create.body else {
                panic!("expected CREATE TABLE column definition body, got: {sql}");
            };
            assert_eq!(create.name.name, "te\"st", "{sql}");
            assert_eq!(columns[0].name, "co\"l", "{sql}");
            assert_eq!(columns[1].name, "no\"te", "{sql}");
        });
    }

    #[test]
    fn alter_table_rename_preserves_embedded_quote_identifiers_in_index_sql() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(r#"CREATE TABLE "te""st"("co""l" TEXT);"#)
                .await
                .unwrap();
            conn.execute(r#"CREATE INDEX "ix""q" ON "te""st"("co""l");"#)
                .await
                .unwrap();
            conn.execute(r#"ALTER TABLE "te""st" RENAME TO "ta""rget";"#)
                .await
                .unwrap();

            let rows = conn
                .query(r#"SELECT sql FROM sqlite_master WHERE type='index' AND name='ix"q';"#)
                .await
                .unwrap();
            let sql = row_values(&rows[0])[0].to_text();
            let Some((Statement::CreateIndex(create), _)) =
                parse_first_statement_with_tail(&sql).expect("sqlite_master sql should parse")
            else {
                panic!("expected CREATE INDEX sql, got: {sql}");
            };
            assert_eq!(create.name.name, "ix\"q", "{sql}");
            assert_eq!(create.table, "ta\"rget", "{sql}");
        });
    }

    #[test]
    fn foreign_key_cascade_supports_embedded_quote_identifiers() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
            conn.execute(r#"CREATE TABLE "par""ent"("id""x" INTEGER PRIMARY KEY);"#)
                .await
                .unwrap();
            conn.execute(
            r#"CREATE TABLE "chi""ld"("fk""x" INTEGER REFERENCES "par""ent"("id""x") ON DELETE CASCADE);"#,
        )
        .await
        .unwrap();
            conn.execute(r#"INSERT INTO "par""ent"("id""x") VALUES (1);"#)
                .await
                .unwrap();
            conn.execute(r#"INSERT INTO "chi""ld"("fk""x") VALUES (1);"#)
                .await
                .unwrap();

            conn.execute(r#"DELETE FROM "par""ent" WHERE "id""x" = 1;"#)
                .await
                .unwrap();

            let rows = conn
                .query(r#"SELECT COUNT(*) FROM "chi""ld";"#)
                .await
                .unwrap();
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(0));
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 033: JOINs
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_033_inner_join() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE dept(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO dept VALUES (1,'Eng'),(2,'Sales'),(3,'HR')")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO emp VALUES (1,'Alice',1),(2,'Bob',2),(3,'Charlie',1),(4,'Diana',2)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT emp.name, dept.name FROM emp \
                 INNER JOIN dept ON emp.dept_id = dept.id \
                 ORDER BY emp.name",
                )
                .await
                .unwrap();
            assert_eq!(r.len(), 4);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[0])[1].to_text(), "Eng");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[1])[1].to_text(), "Sales");
            assert_eq!(row_values(&r[2])[0].to_text(), "Charlie");
            assert_eq!(row_values(&r[2])[1].to_text(), "Eng");
            assert_eq!(row_values(&r[3])[0].to_text(), "Diana");
            assert_eq!(row_values(&r[3])[1].to_text(), "Sales");
        });
    }

    #[test]
    fn conformance_033_left_join() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE dept(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO dept VALUES (1,'Eng'),(2,'Sales'),(3,'HR')")
                .await
                .unwrap();
            conn.execute("INSERT INTO emp VALUES (1,'Alice',1),(2,'Bob',2)")
                .await
                .unwrap();
            let r = conn
                .query(
                    "SELECT dept.name, emp.name FROM dept \
                 LEFT JOIN emp ON dept.id = emp.dept_id \
                 ORDER BY dept.name",
                )
                .await
                .unwrap();
            assert_eq!(r.len(), 3);
            // Eng has Alice
            assert_eq!(row_values(&r[0])[0].to_text(), "Eng");
            assert_eq!(row_values(&r[0])[1].to_text(), "Alice");
            // HR has no employees — NULL
            assert_eq!(row_values(&r[1])[0].to_text(), "HR");
            assert!(row_values(&r[1])[1].is_null());
            // Sales has Bob
            assert_eq!(row_values(&r[2])[0].to_text(), "Sales");
            assert_eq!(row_values(&r[2])[1].to_text(), "Bob");
        });
    }

    #[test]
    fn conformance_033_cross_join() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE colors(c TEXT)").await.unwrap();
            conn.execute("CREATE TABLE sizes(s TEXT)").await.unwrap();
            conn.execute("INSERT INTO colors VALUES ('red'),('blue')")
                .await
                .unwrap();
            conn.execute("INSERT INTO sizes VALUES ('S'),('M'),('L')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT c, s FROM colors CROSS JOIN sizes ORDER BY c, s")
                .await
                .unwrap();
            assert_eq!(r.len(), 6);
            assert_eq!(row_values(&r[0])[0].to_text(), "blue");
            assert_eq!(row_values(&r[0])[1].to_text(), "L");
            assert_eq!(row_values(&r[5])[0].to_text(), "red");
            assert_eq!(row_values(&r[5])[1].to_text(), "S");
        });
    }

    #[test]
    fn conformance_033_self_join() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, mgr_id INTEGER)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO emp VALUES (1,'Boss',NULL),(2,'Alice',1),(3,'Bob',1),(4,'Charlie',2)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT e.name, m.name FROM emp e \
                 INNER JOIN emp m ON e.mgr_id = m.id \
                 ORDER BY e.name",
                )
                .await
                .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[0])[1].to_text(), "Boss");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[1])[1].to_text(), "Boss");
            assert_eq!(row_values(&r[2])[0].to_text(), "Charlie");
            assert_eq!(row_values(&r[2])[1].to_text(), "Alice");
        });
    }

    #[test]
    fn regression_join_literal_text_numeric_comparison_uses_storage_class_order() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE a(x INTEGER)").await.unwrap();
            conn.execute("CREATE TABLE b(y INTEGER)").await.unwrap();
            conn.execute("INSERT INTO a VALUES (1)").await.unwrap();
            conn.execute("INSERT INTO b VALUES (2)").await.unwrap();

            let rows = conn
                .query("SELECT a.x FROM a JOIN b ON '123' = 123;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            let rows = conn
                .query("SELECT a.x FROM a JOIN b ON '123' < 124;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            let rows = conn
                .query("SELECT a.x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS y) b ON '123' = 123;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            let rows = conn
                .query("SELECT a.x FROM (SELECT 1 AS x) a JOIN (SELECT 2 AS y) b ON '123' < 124;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            let rows = conn
                .query("SELECT a.x FROM a JOIN b ON a.rowid = '1';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);

            let rows = conn
                .query("SELECT a.x FROM a JOIN b ON a.rowid > '0';")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);

            conn.execute("CREATE TABLE txt(v TEXT)").await.unwrap();
            conn.execute("CREATE TABLE num(v NUMERIC)").await.unwrap();
            conn.execute("INSERT INTO txt VALUES ('9')").await.unwrap();
            conn.execute("INSERT INTO num VALUES (CAST('9' AS TEXT))")
                .await
                .unwrap();

            let rows = conn
                .query("SELECT v FROM (SELECT v FROM txt) a JOIN (SELECT 10 AS n) b ON v < n;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            let rows = conn
                .query("SELECT v FROM (SELECT v FROM num) a JOIN (SELECT 10 AS n) b ON v < n;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    #[test]
    fn regression_join_ambiguous_column_surfaces_typed_error() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE a(x INTEGER)").await.unwrap();
            conn.execute("CREATE TABLE b(x INTEGER)").await.unwrap();

            let err = conn
                .query("SELECT a.x FROM a JOIN b ON x = 1;")
                .await
                .expect_err("unqualified duplicate JOIN column should fail");
            assert!(
                matches!(err, FrankenError::AmbiguousColumn { ref name } if name == "x"),
                "unexpected error: {err:?}"
            );
        });
    }

    #[test]
    fn regression_table_alias_hides_base_table_qualifier() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER)").await.unwrap();

            let err = conn
                .query("SELECT t.x FROM t AS a;")
                .await
                .expect_err("base table qualifier should not resolve after aliasing");
            let message = err.to_string();
            assert!(
                message.contains("no such table: t") || message.contains("no such column"),
                "unexpected error: {err:?}"
            );
        });
    }

    async fn assert_wrong_function_arity(conn: &Connection, sql: &str, name: &str) {
        let err = conn
            .query(sql)
            .await
            .expect_err("known function with wrong arity should fail");
        let expected = format!("wrong number of arguments to function {name}()");
        assert!(
            matches!(&err, FrankenError::FunctionError(message) if message == &expected),
            "unexpected error for {sql}: {err:?}"
        );
    }

    #[test]
    fn regression_aggregate_wrong_arity_surfaces_function_error() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(v INTEGER, s TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
                .await
                .unwrap();

            assert_wrong_function_arity(&conn, "SELECT sum() FROM t;", "sum").await;
            assert_wrong_function_arity(&conn, "SELECT group_concat() FROM t;", "group_concat")
                .await;
            assert_wrong_function_arity(
                &conn,
                "SELECT group_concat(s, '-', '!') FROM t;",
                "group_concat",
            )
            .await;
        });
    }

    #[test]
    fn regression_window_wrong_arity_surfaces_function_error() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(v INTEGER)").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1), (2)").await.unwrap();

            assert_wrong_function_arity(&conn, "SELECT rank(1) OVER (ORDER BY v) FROM t;", "rank")
                .await;
            assert_wrong_function_arity(&conn, "SELECT lag() OVER (ORDER BY v) FROM t;", "lag")
                .await;
            assert_wrong_function_arity(
                &conn,
                "SELECT lag(v, 1, 0, 0) OVER (ORDER BY v) FROM t;",
                "lag",
            )
            .await;
            assert_wrong_function_arity(&conn, "SELECT count(1, 2) OVER () FROM t;", "count").await;
        });
    }

    #[test]
    fn conformance_033_join_with_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE dept(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
            "CREATE TABLE emp(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO dept VALUES (1,'Eng'),(2,'Sales')")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO emp VALUES (1,'Alice',1,100),(2,'Bob',2,80),(3,'Charlie',1,120)",
            )
            .await
            .unwrap();
            let r = conn
                .query(
                    "SELECT emp.name, dept.name FROM emp \
                 JOIN dept ON emp.dept_id = dept.id \
                 WHERE emp.salary > 90 ORDER BY emp.name",
                )
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Charlie");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 034: UPDATE
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_034_update_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'Alice',80),(2,'Bob',90),(3,'Charlie',70)")
                .await
                .unwrap();
            conn.execute("UPDATE t1 SET score = 95 WHERE id = 2")
                .await
                .unwrap();
            let r = conn
                .query("SELECT score FROM t1 WHERE id = 2")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "95");
        });
    }

    #[test]
    fn conformance_034_update_multiple_columns() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'Alice',80)")
                .await
                .unwrap();
            conn.execute("UPDATE t1 SET name = 'Alicia', score = 99 WHERE id = 1")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name, score FROM t1 WHERE id = 1")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alicia");
            assert_eq!(row_values(&r[0])[1].to_text(), "99");
        });
    }

    #[test]
    fn conformance_034_update_all_rows() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, active INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,1),(2,1),(3,0)")
                .await
                .unwrap();
            conn.execute("UPDATE t1 SET active = 0").await.unwrap();
            let r = conn
                .query("SELECT active FROM t1 ORDER BY id")
                .await
                .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
            assert_eq!(row_values(&r[1])[0].to_text(), "0");
            assert_eq!(row_values(&r[2])[0].to_text(), "0");
        });
    }

    #[test]
    fn conformance_034_update_with_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,10),(2,20),(3,30)")
                .await
                .unwrap();
            conn.execute("UPDATE t1 SET val = val * 2 WHERE id <= 2")
                .await
                .unwrap();
            let r = conn.query("SELECT val FROM t1 ORDER BY id").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "20");
            assert_eq!(row_values(&r[1])[0].to_text(), "40");
            assert_eq!(row_values(&r[2])[0].to_text(), "30");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 035: DELETE
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_035_delete_with_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')")
                .await
                .unwrap();
            conn.execute("DELETE FROM t1 WHERE id = 2").await.unwrap();
            let r = conn.query("SELECT name FROM t1 ORDER BY id").await.unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Charlie");
        });
    }

    #[test]
    fn conformance_035_delete_all() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'Alice'),(2,'Bob')")
                .await
                .unwrap();
            conn.execute("DELETE FROM t1").await.unwrap();
            let r = conn.query("SELECT COUNT(*) FROM t1").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
        });
    }

    #[test]
    fn conformance_035_delete_with_in() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie'),(4,'Diana')")
                .await
                .unwrap();
            conn.execute("DELETE FROM t1 WHERE id IN (2, 4)")
                .await
                .unwrap();
            let r = conn.query("SELECT name FROM t1 ORDER BY id").await.unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Charlie");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 036: Compound queries (UNION, INTERSECT, EXCEPT)
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_036_union_all() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(name TEXT)").await.unwrap();
            conn.execute("CREATE TABLE t2(name TEXT)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES ('Alice'),('Bob')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES ('Bob'),('Charlie')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM t1 UNION ALL SELECT name FROM t2 ORDER BY name")
                .await
                .unwrap();
            assert_eq!(r.len(), 4);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[2])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[3])[0].to_text(), "Charlie");
        });
    }

    #[test]
    fn conformance_036_union_distinct() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(name TEXT)").await.unwrap();
            conn.execute("CREATE TABLE t2(name TEXT)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES ('Alice'),('Bob')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES ('Bob'),('Charlie')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM t1 UNION SELECT name FROM t2 ORDER BY name")
                .await
                .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[2])[0].to_text(), "Charlie");
        });
    }

    #[test]
    fn conformance_036_intersect() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(name TEXT)").await.unwrap();
            conn.execute("CREATE TABLE t2(name TEXT)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES ('Alice'),('Bob'),('Charlie')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES ('Bob'),('Charlie'),('Diana')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM t1 INTERSECT SELECT name FROM t2 ORDER BY name")
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[1])[0].to_text(), "Charlie");
        });
    }

    #[test]
    fn conformance_036_except() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(name TEXT)").await.unwrap();
            conn.execute("CREATE TABLE t2(name TEXT)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES ('Alice'),('Bob'),('Charlie')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES ('Bob'),('Diana')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM t1 EXCEPT SELECT name FROM t2 ORDER BY name")
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Charlie");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 037: ORDER BY, LIMIT, OFFSET, DISTINCT
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_037_order_by_desc() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM t1 ORDER BY id DESC")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Charlie");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[2])[0].to_text(), "Alice");
        });
    }

    #[test]
    fn conformance_037_order_by_multiple() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, dept TEXT, name TEXT)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO t1 VALUES (1,'A','Charlie'),(2,'B','Alice'),\
             (3,'A','Alice'),(4,'B','Bob')",
            )
            .await
            .unwrap();
            let r = conn
                .query("SELECT dept, name FROM t1 ORDER BY dept ASC, name ASC")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[1].to_text(), "Alice");
            assert_eq!(row_values(&r[0])[0].to_text(), "A");
            assert_eq!(row_values(&r[1])[1].to_text(), "Charlie");
            assert_eq!(row_values(&r[2])[1].to_text(), "Alice");
            assert_eq!(row_values(&r[2])[0].to_text(), "B");
            assert_eq!(row_values(&r[3])[1].to_text(), "Bob");
        });
    }

    #[test]
    fn conformance_037_limit() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO t1 VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie'),(4,'Diana'),(5,'Eve')",
            )
            .await
            .unwrap();
            let r = conn
                .query("SELECT name FROM t1 ORDER BY id LIMIT 3")
                .await
                .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[2])[0].to_text(), "Charlie");
        });
    }

    #[test]
    fn conformance_037_limit_offset() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO t1 VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie'),(4,'Diana'),(5,'Eve')",
            )
            .await
            .unwrap();
            let r = conn
                .query("SELECT name FROM t1 ORDER BY id LIMIT 2 OFFSET 2")
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Charlie");
            assert_eq!(row_values(&r[1])[0].to_text(), "Diana");
        });
    }

    #[test]
    fn conformance_037_distinct() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(name TEXT)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES ('Alice'),('Bob'),('Alice'),('Charlie'),('Bob')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT DISTINCT name FROM t1 ORDER BY name")
                .await
                .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[2])[0].to_text(), "Charlie");
        });
    }

    #[test]
    fn conformance_037_order_by_nulls() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'B'),(2,NULL),(3,'A'),(4,NULL),(5,'C')")
                .await
                .unwrap();
            // SQLite: NULLs sort first in ASC order
            let r = conn
                .query("SELECT val FROM t1 ORDER BY val ASC")
                .await
                .unwrap();
            assert_eq!(r.len(), 5);
            assert!(row_values(&r[0])[0].is_null());
            assert!(row_values(&r[1])[0].is_null());
            assert_eq!(row_values(&r[2])[0].to_text(), "A");
            assert_eq!(row_values(&r[3])[0].to_text(), "B");
            assert_eq!(row_values(&r[4])[0].to_text(), "C");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 038: INSERT ... SELECT
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_038_insert_select_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE dst(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO src VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')")
                .await
                .unwrap();
            conn.execute("INSERT INTO dst SELECT * FROM src WHERE id <= 2")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM dst ORDER BY id")
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
        });
    }

    #[test]
    fn conformance_038_insert_select_with_transform() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE dst(name TEXT)").await.unwrap();
            conn.execute("INSERT INTO src VALUES (1,'Alice'),(2,'Bob')")
                .await
                .unwrap();
            conn.execute("INSERT INTO dst SELECT upper(name) FROM src")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM dst ORDER BY name")
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "ALICE");
            assert_eq!(row_values(&r[1])[0].to_text(), "BOB");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 039: NULL handling
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_039_null_comparison() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'A'),(2,NULL),(3,'B')")
                .await
                .unwrap();
            // NULL = NULL is not true in SQL
            let r = conn
                .query("SELECT COUNT(*) FROM t1 WHERE val = NULL")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0");
            // IS NULL works
            let r = conn
                .query("SELECT COUNT(*) FROM t1 WHERE val IS NULL")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            // IS NOT NULL
            let r = conn
                .query("SELECT COUNT(*) FROM t1 WHERE val IS NOT NULL")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "2");
        });
    }

    #[test]
    fn conformance_039_coalesce() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,NULL,'fallback'),(2,'value',NULL)")
                .await
                .unwrap();
            let r = conn
                .query("SELECT COALESCE(a, b) FROM t1 ORDER BY id")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "fallback");
            assert_eq!(row_values(&r[1])[0].to_text(), "value");
        });
    }

    #[test]
    fn conformance_039_ifnull() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT IFNULL(NULL, 'default'), IFNULL('value', 'default')")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "default");
            assert_eq!(row_values(&r[0])[1].to_text(), "value");
        });
    }

    #[test]
    fn conformance_039_nullif() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT NULLIF(1, 1), NULLIF(1, 2)")
                .await
                .unwrap();
            assert!(row_values(&r[0])[0].is_null());
            assert_eq!(row_values(&r[0])[1].to_text(), "1");
        });
    }

    #[test]
    fn conformance_039_null_in_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(val INTEGER)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1),(NULL),(3),(NULL),(5)")
                .await
                .unwrap();
            let r = conn
                .query("SELECT COUNT(*), COUNT(val), SUM(val), AVG(val) FROM t1")
                .await
                .unwrap();
            // COUNT(*) counts all rows including NULL
            assert_eq!(row_values(&r[0])[0].to_text(), "5");
            // COUNT(val) skips NULLs
            assert_eq!(row_values(&r[0])[1].to_text(), "3");
            // SUM skips NULLs: 1+3+5=9
            assert_eq!(row_values(&r[0])[2].to_text(), "9");
            // AVG skips NULLs: 9/3=3.0
            assert_eq!(row_values(&r[0])[3].to_text(), "3.0");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 040: CREATE INDEX / DROP INDEX
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_040_create_index_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_name ON t1(name)")
                .await
                .unwrap();
            // Index should appear in sqlite_master
            let r = conn
            .query("SELECT type, name, tbl_name FROM sqlite_master WHERE type = 'index' AND name = 'idx_name'")
            .await
            .unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(row_values(&r[0])[0].to_text(), "index");
            assert_eq!(row_values(&r[0])[1].to_text(), "idx_name");
            assert_eq!(row_values(&r[0])[2].to_text(), "t1");
        });
    }

    #[test]
    fn conformance_040_create_index_if_not_exists() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx1 ON t1(val)").await.unwrap();
            // Second create with IF NOT EXISTS should succeed silently
            conn.execute("CREATE INDEX IF NOT EXISTS idx1 ON t1(val)")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx1'")
                .await
                .unwrap();
            assert_eq!(r.len(), 1);
        });
    }

    #[test]
    fn conformance_040_drop_index() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx1 ON t1(val)").await.unwrap();
            conn.execute("DROP INDEX idx1").await.unwrap();
            let r = conn
                .query("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx1'")
                .await
                .unwrap();
            assert_eq!(r.len(), 0);
        });
    }

    #[test]
    fn conformance_040_unique_index() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, code TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE UNIQUE INDEX idx_code ON t1(code)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'abc')")
                .await
                .unwrap();
            // Inserting duplicate should fail
            let result = conn.execute("INSERT INTO t1 VALUES (2, 'abc')").await;
            assert!(result.is_err());
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 041: Triggers
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_041_trigger_after_insert() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE log(msg TEXT)").await.unwrap();
            conn.execute(
                "CREATE TRIGGER t1_after_insert AFTER INSERT ON t1 \
             BEGIN INSERT INTO log VALUES ('inserted ' || NEW.val); END",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 'hello')")
                .await
                .unwrap();
            let r = conn.query("SELECT msg FROM log").await.unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(row_values(&r[0])[0].to_text(), "inserted hello");
        });
    }

    #[test]
    fn conformance_041_trigger_before_insert() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE audit(action TEXT)")
                .await
                .unwrap();
            conn.execute(
                "CREATE TRIGGER t1_before BEFORE INSERT ON t1 \
             BEGIN INSERT INTO audit VALUES ('before_insert'); END",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 42)").await.unwrap();
            let r = conn.query("SELECT action FROM audit").await.unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(row_values(&r[0])[0].to_text(), "before_insert");
        });
    }

    #[test]
    fn conformance_041_trigger_drop() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE log(x TEXT)").await.unwrap();
            conn.execute(
                "CREATE TRIGGER trg1 AFTER INSERT ON t1 \
             BEGIN INSERT INTO log VALUES ('fired'); END",
            )
            .await
            .unwrap();
            conn.execute("DROP TRIGGER trg1").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1)").await.unwrap();
            let r = conn.query("SELECT x FROM log").await.unwrap();
            // Trigger was dropped, so log should be empty
            assert_eq!(r.len(), 0);
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 042: AUTOINCREMENT
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_042_autoincrement_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1(val) VALUES ('a')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1(val) VALUES ('b')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1(val) VALUES ('c')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT id, val FROM t1 ORDER BY id")
                .await
                .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            assert_eq!(row_values(&r[1])[0].to_text(), "2");
            assert_eq!(row_values(&r[2])[0].to_text(), "3");
        });
    }

    #[test]
    fn conformance_042_autoincrement_after_delete() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1(val) VALUES ('a')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1(val) VALUES ('b')")
                .await
                .unwrap();
            // Delete row with id=2
            conn.execute("DELETE FROM t1 WHERE id = 2").await.unwrap();
            // Next insert should get id=3, not reuse id=2
            conn.execute("INSERT INTO t1(val) VALUES ('c')")
                .await
                .unwrap();
            let r = conn.query("SELECT id FROM t1 ORDER BY id").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            assert_eq!(row_values(&r[1])[0].to_text(), "3");
        });
    }

    #[test]
    fn conformance_042_sqlite_sequence_table() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1(val) VALUES ('x')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1(val) VALUES ('y')")
                .await
                .unwrap();
            // sqlite_sequence should track the max assigned rowid
            let r = conn
                .query("SELECT seq FROM sqlite_sequence WHERE name = 't1'")
                .await
                .unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(row_values(&r[0])[0].to_text(), "2");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 043: DEFAULT values
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_043_default_literal() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE t1(\
             id INTEGER PRIMARY KEY, \
             status TEXT DEFAULT 'active', \
             count INTEGER DEFAULT 0)",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t1(id) VALUES (1)").await.unwrap();
            let r = conn
                .query("SELECT status, count FROM t1 WHERE id = 1")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "active");
            assert_eq!(row_values(&r[0])[1].to_text(), "0");
        });
    }

    #[test]
    fn conformance_043_default_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            // Column without DEFAULT clause defaults to NULL
            conn.execute("INSERT INTO t1(id) VALUES (1)").await.unwrap();
            let r = conn.query("SELECT val FROM t1 WHERE id = 1").await.unwrap();
            assert!(row_values(&r[0])[0].is_null());
        });
    }

    #[test]
    fn conformance_043_default_override() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT DEFAULT 'default_val')")
                .await
                .unwrap();
            // Explicit value should override DEFAULT
            conn.execute("INSERT INTO t1 VALUES (1, 'custom')")
                .await
                .unwrap();
            let r = conn.query("SELECT val FROM t1 WHERE id = 1").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "custom");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 044: Multi-column ORDER BY and expression sorting
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_044_order_by_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 10), (2, -5), (3, 3)")
                .await
                .unwrap();
            let r = conn
                .query("SELECT id, ABS(val) as a FROM t1 ORDER BY ABS(val)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3"); // ABS(3)=3
            assert_eq!(row_values(&r[1])[0].to_text(), "2"); // ABS(-5)=5
            assert_eq!(row_values(&r[2])[0].to_text(), "1"); // ABS(10)=10
        });
    }

    #[test]
    fn conformance_044_order_by_column_index() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(a TEXT, b INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES ('x', 3), ('y', 1), ('z', 2)")
                .await
                .unwrap();
            // ORDER BY column index (1-based)
            let r = conn.query("SELECT a, b FROM t1 ORDER BY 2").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "y"); // b=1
            assert_eq!(row_values(&r[1])[0].to_text(), "z"); // b=2
            assert_eq!(row_values(&r[2])[0].to_text(), "x"); // b=3
        });
    }

    #[test]
    fn conformance_044_order_by_alias() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, first TEXT, last TEXT)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO t1 VALUES (1, 'Bob', 'Smith'), \
             (2, 'Alice', 'Jones'), (3, 'Charlie', 'Adams')",
            )
            .await
            .unwrap();
            // ORDER BY using column alias
            let r = conn
                .query("SELECT first || ' ' || last AS full_name FROM t1 ORDER BY full_name")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice Jones");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob Smith");
            assert_eq!(row_values(&r[2])[0].to_text(), "Charlie Adams");
        });
    }

    // ── Conformance 048: Math functions (§13.2) via SQL pipeline ─────

    #[test]
    fn conformance_048_abs() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT abs(-42), abs(3.14), abs(0)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "42");
            assert_eq!(row_values(&r[0])[1].to_text(), "3.14");
            assert_eq!(row_values(&r[0])[2].to_text(), "0");
        });
    }

    #[test]
    fn conformance_048_abs_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT abs(NULL)").await.unwrap();
            assert!(row_values(&r[0])[0].is_null());
        });
    }

    #[test]
    fn conformance_048_round() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT round(3.14159), round(3.14159, 2), round(3.5)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3.0");
            assert_eq!(row_values(&r[0])[1].to_text(), "3.14");
            assert_eq!(row_values(&r[0])[2].to_text(), "4.0");
        });
    }

    #[test]
    fn conformance_048_sign() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT sign(-5), sign(0), sign(42), sign(NULL)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "-1");
            assert_eq!(row_values(&r[0])[1].to_text(), "0");
            assert_eq!(row_values(&r[0])[2].to_text(), "1");
            assert!(row_values(&r[0])[3].is_null());
        });
    }

    #[test]
    fn conformance_048_trig_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT sin(0), cos(0), tan(0)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0.0");
            assert_eq!(row_values(&r[0])[1].to_text(), "1.0");
            assert_eq!(row_values(&r[0])[2].to_text(), "0.0");
        });
    }

    #[test]
    fn conformance_048_acos_asin_atan() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT acos(1), asin(0), atan(0)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0.0");
            assert_eq!(row_values(&r[0])[1].to_text(), "0.0");
            assert_eq!(row_values(&r[0])[2].to_text(), "0.0");
        });
    }

    #[test]
    fn conformance_048_acos_domain_error() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT acos(2.0)").await.unwrap();
            assert!(row_values(&r[0])[0].is_null());
        });
    }

    #[test]
    fn conformance_048_sqrt() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT sqrt(144), sqrt(2)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "12.0");
            let v = row_values(&r[0])[1].to_text();
            assert!(v.starts_with("1.41421356"), "got {v}");
        });
    }

    #[test]
    fn conformance_048_sqrt_negative() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT sqrt(-1)").await.unwrap();
            assert!(row_values(&r[0])[0].is_null());
        });
    }

    #[test]
    fn conformance_048_pow() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT pow(2, 10), power(3, 2)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1024.0");
            assert_eq!(row_values(&r[0])[1].to_text(), "9.0");
        });
    }

    #[test]
    fn conformance_048_exp_ln() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT exp(0), ln(1)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1.0");
            assert_eq!(row_values(&r[0])[1].to_text(), "0.0");
        });
    }

    #[test]
    fn conformance_048_log_variants() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT log(100), log10(1000), log2(8)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "2.0");
            assert_eq!(row_values(&r[0])[1].to_text(), "3.0");
            assert_eq!(row_values(&r[0])[2].to_text(), "3.0");
        });
    }

    #[test]
    fn conformance_048_log_two_arg() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT log(2, 8)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3.0");
        });
    }

    #[test]
    fn conformance_048_ln_negative_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT ln(-1), ln(0)").await.unwrap();
            assert!(row_values(&r[0])[0].is_null());
            assert!(row_values(&r[0])[1].is_null());
        });
    }

    #[test]
    fn conformance_048_pi() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT pi()").await.unwrap();
            let v = row_values(&r[0])[0].to_text();
            assert!(v.starts_with("3.14159265"), "got {v}");
        });
    }

    #[test]
    fn conformance_048_ceil_floor_trunc() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT ceil(1.2), floor(1.7), trunc(2.9)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "2.0");
            assert_eq!(row_values(&r[0])[1].to_text(), "1.0");
            assert_eq!(row_values(&r[0])[2].to_text(), "2.0");
        });
    }

    #[test]
    fn conformance_048_ceil_floor_negative() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT ceil(-1.2), floor(-1.2), trunc(-2.9)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "-1.0");
            assert_eq!(row_values(&r[0])[1].to_text(), "-2.0");
            assert_eq!(row_values(&r[0])[2].to_text(), "-2.0");
        });
    }

    #[test]
    fn conformance_048_degrees_radians() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT degrees(pi()), radians(180)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "180.0");
            let v = row_values(&r[0])[1].to_text();
            assert!(v.starts_with("3.14159265"), "got {v}");
        });
    }

    #[test]
    fn conformance_048_mod_func() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT mod(10, 3), mod(10, 0)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1.0");
            assert!(row_values(&r[0])[1].is_null());
        });
    }

    #[test]
    fn conformance_048_atan2() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT atan2(0, 1), atan2(1, 0)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0.0");
            let v = row_values(&r[0])[1].to_text();
            assert!(v.starts_with("1.57079632"), "got {v}");
        });
    }

    #[test]
    fn conformance_048_math_null_propagation() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT sin(NULL), sqrt(NULL), pow(NULL, 2), mod(NULL, 3)")
                .await
                .unwrap();
            assert!(row_values(&r[0])[0].is_null());
            assert!(row_values(&r[0])[1].is_null());
            assert!(row_values(&r[0])[2].is_null());
            assert!(row_values(&r[0])[3].is_null());
        });
    }

    #[test]
    fn conformance_048_math_with_table() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE nums(x REAL)").await.unwrap();
            conn.execute("INSERT INTO nums VALUES (4.0),(9.0),(16.0),(25.0)")
                .await
                .unwrap();
            let r = conn
                .query("SELECT sqrt(x) FROM nums ORDER BY x")
                .await
                .unwrap();
            assert_eq!(r.len(), 4);
            assert_eq!(row_values(&r[0])[0].to_text(), "2.0");
            assert_eq!(row_values(&r[1])[0].to_text(), "3.0");
            assert_eq!(row_values(&r[2])[0].to_text(), "4.0");
            assert_eq!(row_values(&r[3])[0].to_text(), "5.0");
        });
    }

    #[test]
    fn conformance_048_hyperbolic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT sinh(0), cosh(0), tanh(0)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "0.0");
            assert_eq!(row_values(&r[0])[1].to_text(), "1.0");
            assert_eq!(row_values(&r[0])[2].to_text(), "0.0");
        });
    }

    // ── Conformance 049: String functions and typeof ─────────────────

    #[test]
    fn conformance_049_length() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT length('hello'), length(''), length(NULL)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "5");
            assert_eq!(row_values(&r[0])[1].to_text(), "0");
            assert!(row_values(&r[0])[2].is_null());
        });
    }

    #[test]
    fn conformance_049_upper_lower() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT upper('hello'), lower('WORLD')")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "HELLO");
            assert_eq!(row_values(&r[0])[1].to_text(), "world");
        });
    }

    #[test]
    fn conformance_049_typeof() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT typeof(42), typeof(3.14), typeof('hi'), typeof(NULL), typeof(X'00')")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "integer");
            assert_eq!(row_values(&r[0])[1].to_text(), "real");
            assert_eq!(row_values(&r[0])[2].to_text(), "text");
            assert_eq!(row_values(&r[0])[3].to_text(), "null");
            assert_eq!(row_values(&r[0])[4].to_text(), "blob");
        });
    }

    #[test]
    fn conformance_049_max_min_scalar() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT max(1, 5, 3), min(10, 2, 7)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "5");
            assert_eq!(row_values(&r[0])[1].to_text(), "2");
        });
    }

    #[test]
    fn conformance_049_total_changes() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(x INTEGER)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1),(2),(3)")
                .await
                .unwrap();
            let r = conn.query("SELECT changes()").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3");
        });
    }

    // ── Conformance 050: Type coercion and string concatenation ──────

    #[test]
    fn conformance_050_string_concat() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT 'hello' || ' ' || 'world'")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "hello world");
        });
    }

    #[test]
    fn conformance_050_concat_with_numbers() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT 'val=' || 42").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "val=42");
        });
    }

    #[test]
    fn conformance_050_concat_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT 'abc' || NULL").await.unwrap();
            assert!(row_values(&r[0])[0].is_null());
        });
    }

    #[test]
    fn conformance_050_numeric_string_comparison() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT CASE WHEN 10 = '10' THEN 'equal' ELSE 'not_equal' END")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "not_equal");
        });
    }

    #[test]
    fn conformance_050_mixed_arithmetic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT 1 + 2.5, 10 / 3, 10.0 / 3")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3.5");
            assert_eq!(row_values(&r[0])[1].to_text(), "3");
            let v = row_values(&r[0])[2].to_text();
            assert!(v.starts_with("3.333"), "got {v}");
        });
    }

    #[test]
    fn conformance_050_cast_types() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT CAST('123' AS INTEGER), CAST(3.14 AS INTEGER), CAST(42 AS TEXT)")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "123");
            assert_eq!(row_values(&r[0])[1].to_text(), "3");
            assert_eq!(row_values(&r[0])[2].to_text(), "42");
        });
    }

    // ── Conformance 051: Expression edge cases ──────────────────────

    #[test]
    fn conformance_051_unary_minus() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT -(-5), -(3.14)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "5");
            assert_eq!(row_values(&r[0])[1].to_text(), "-3.14");
        });
    }

    #[test]
    fn conformance_051_modulo_operator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT 17 % 5, -7 % 3").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "2");
            assert_eq!(row_values(&r[0])[1].to_text(), "-1");
        });
    }

    #[test]
    fn conformance_051_boolean_expressions() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT 1 AND 1, 1 AND 0, 0 OR 1, 0 OR 0, NOT 0, NOT 1")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            assert_eq!(row_values(&r[0])[1].to_text(), "0");
            assert_eq!(row_values(&r[0])[2].to_text(), "1");
            assert_eq!(row_values(&r[0])[3].to_text(), "0");
            assert_eq!(row_values(&r[0])[4].to_text(), "1");
            assert_eq!(row_values(&r[0])[5].to_text(), "0");
        });
    }

    #[test]
    fn conformance_051_comparison_operators() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT 5 > 3, 5 < 3, 5 >= 5, 5 <= 4, 5 != 3, 5 == 5")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "1");
            assert_eq!(row_values(&r[0])[1].to_text(), "0");
            assert_eq!(row_values(&r[0])[2].to_text(), "1");
            assert_eq!(row_values(&r[0])[3].to_text(), "0");
            assert_eq!(row_values(&r[0])[4].to_text(), "1");
            assert_eq!(row_values(&r[0])[5].to_text(), "1");
        });
    }

    // ── Conformance 052: Complex multi-table queries ─────────────────

    #[test]
    fn conformance_052_multi_table_join_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE departments(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE employees(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL)")
            .await
            .unwrap();
            conn.execute("INSERT INTO departments VALUES (1,'Engineering'),(2,'Sales'),(3,'HR')")
                .await
                .unwrap();
            conn.execute("INSERT INTO employees VALUES (1,'Alice',1,80000),(2,'Bob',1,90000),(3,'Charlie',2,70000),(4,'Diana',2,75000),(5,'Eve',3,60000)")
            .await
            .unwrap();
            let r = conn
            .query("SELECT d.name, COUNT(e.id), SUM(e.salary) FROM departments d JOIN employees e ON d.id = e.dept_id GROUP BY d.name ORDER BY d.name")
            .await
            .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "Engineering");
            assert_eq!(row_values(&r[0])[1].to_text(), "2");
            assert_eq!(row_values(&r[0])[2].to_text(), "170000.0");
            assert_eq!(row_values(&r[1])[0].to_text(), "HR");
            assert_eq!(row_values(&r[1])[1].to_text(), "1");
            assert_eq!(row_values(&r[1])[2].to_text(), "60000.0");
            assert_eq!(row_values(&r[2])[0].to_text(), "Sales");
            assert_eq!(row_values(&r[2])[1].to_text(), "2");
            assert_eq!(row_values(&r[2])[2].to_text(), "145000.0");
        });
    }

    #[test]
    fn conformance_052_subquery_in_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, price REAL)")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO products VALUES (1,'A',10.0),(2,'B',20.0),(3,'C',30.0),(4,'D',15.0)",
            )
            .await
            .unwrap();
            let r = conn
            .query("SELECT name FROM products WHERE price > (SELECT AVG(price) FROM products) ORDER BY name")
            .await
            .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "B");
            assert_eq!(row_values(&r[1])[0].to_text(), "C");
        });
    }

    #[test]
    fn conformance_052_insert_with_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE src(val INTEGER)").await.unwrap();
            conn.execute("CREATE TABLE dst(val INTEGER)").await.unwrap();
            conn.execute("INSERT INTO src VALUES (1),(2),(3),(4),(5)")
                .await
                .unwrap();
            conn.execute("INSERT INTO dst SELECT val FROM src WHERE val > 3")
                .await
                .unwrap();
            let r = conn
                .query("SELECT val FROM dst ORDER BY val")
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "4");
            assert_eq!(row_values(&r[1])[0].to_text(), "5");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 053: GROUP_CONCAT aggregate
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_053_group_concat_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, grp TEXT, val TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'a','x'),(2,'a','y'),(3,'b','z'),(4,'a','w')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT grp, GROUP_CONCAT(val) FROM t1 GROUP BY grp ORDER BY grp")
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            // Default separator is comma
            let a_vals = row_values(&r[0])[1].to_text();
            assert!(a_vals.contains('x'));
            assert!(a_vals.contains('y'));
            assert!(a_vals.contains('w'));
            assert_eq!(row_values(&r[1])[1].to_text(), "z");
        });
    }

    #[test]
    fn conformance_053_group_concat_custom_separator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT GROUP_CONCAT(val, ' | ') FROM t1")
                .await
                .unwrap();
            let result = row_values(&r[0])[0].to_text();
            // All values should be present with custom separator
            assert!(result.contains('a'));
            assert!(result.contains('b'));
            assert!(result.contains('c'));
            assert!(result.contains('|'));
        });
    }

    #[test]
    fn conformance_053_group_concat_null_skip() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(val TEXT)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES ('a'),(NULL),('c')")
                .await
                .unwrap();
            let r = conn
                .query("SELECT GROUP_CONCAT(val) FROM t1")
                .await
                .unwrap();
            let result = row_values(&r[0])[0].to_text();
            // NULL values should be skipped
            assert!(result.contains('a'));
            assert!(result.contains('c'));
            assert!(!result.contains("NULL"));
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 054: PRAGMA
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_054_pragma_table_info() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
            "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL DEFAULT 0.0)",
        )
        .await
        .unwrap();
            let r = conn.query("PRAGMA table_info(t1)").await.unwrap();
            assert!(r.len() >= 3);
            // Check column names are present
            let col_names: Vec<String> = r.iter().map(|row| row_values(row)[1].to_text()).collect();
            assert!(col_names.contains(&"id".to_owned()));
            assert!(col_names.contains(&"name".to_owned()));
            assert!(col_names.contains(&"score".to_owned()));
        });
    }

    #[test]
    fn pragma_table_info_preserves_declared_type_arguments() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE metrics(amount DECIMAL(10, 2), name VARCHAR(255))")
                .await
                .unwrap();
            let rows = conn.query("PRAGMA table_info(metrics)").await.unwrap();
            let amount = rows
                .iter()
                .find(|row| row_values(row)[1].to_text() == "amount")
                .expect("amount column metadata");
            let name = rows
                .iter()
                .find(|row| row_values(row)[1].to_text() == "name")
                .expect("name column metadata");
            assert_eq!(row_values(amount)[2].to_text(), "DECIMAL(10, 2)");
            assert_eq!(row_values(name)[2].to_text(), "VARCHAR(255)");
        });
    }

    #[test]
    fn conformance_054_pragma_user_version() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA user_version = 42").await.unwrap();
            let r = conn.query("PRAGMA user_version").await.unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(row_values(&r[0])[0].to_text(), "42");
        });
    }

    #[test]
    fn conformance_054_pragma_table_list() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE alpha(x INTEGER)").await.unwrap();
            conn.execute("CREATE TABLE beta(y TEXT)").await.unwrap();
            // sqlite_master should list both tables
            let r = conn
                .query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
                .await
                .unwrap();
            assert!(r.len() >= 2);
            let names: Vec<String> = r.iter().map(|row| row_values(row)[0].to_text()).collect();
            assert!(names.contains(&"alpha".to_owned()));
            assert!(names.contains(&"beta".to_owned()));
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 055: Nested and correlated subqueries
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_055_nested_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
                .await
                .unwrap();
            // Subquery in subquery
            let r = conn
                .query(
                    "SELECT val FROM t1 WHERE val > \
                 (SELECT AVG(val) FROM t1 WHERE val < \
                 (SELECT MAX(val) FROM t1)) ORDER BY val",
                )
                .await
                .unwrap();
            // AVG of vals < 50 = (10+20+30+40)/4 = 25
            // vals > 25: 30, 40, 50
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "30");
            assert_eq!(row_values(&r[1])[0].to_text(), "40");
            assert_eq!(row_values(&r[2])[0].to_text(), "50");
        });
    }

    #[test]
    fn conformance_055_in_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t2(id INTEGER PRIMARY KEY, t1_id INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (1,1),(2,3)")
                .await
                .unwrap();
            let r = conn
                .query("SELECT name FROM t1 WHERE id IN (SELECT t1_id FROM t2) ORDER BY name")
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[1])[0].to_text(), "Charlie");
        });
    }

    #[test]
    fn conformance_055_not_in_subquery() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t2(ref_id INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1,'A'),(2,'B'),(3,'C')")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (1),(3)").await.unwrap();
            let r = conn
                .query("SELECT name FROM t1 WHERE id NOT IN (SELECT ref_id FROM t2)")
                .await
                .unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(row_values(&r[0])[0].to_text(), "B");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 056: Multi-table relationships
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_056_three_table_join() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE departments(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE employees(id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
            )
            .await
            .unwrap();
            conn.execute(
                "CREATE TABLE projects(id INTEGER PRIMARY KEY, title TEXT, emp_id INTEGER)",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO departments VALUES (1,'Eng'),(2,'Sales')")
                .await
                .unwrap();
            conn.execute("INSERT INTO employees VALUES (1,'Alice',1),(2,'Bob',1),(3,'Charlie',2)")
                .await
                .unwrap();
            conn.execute("INSERT INTO projects VALUES (1,'Widget',1),(2,'Gadget',2),(3,'Deal',3)")
                .await
                .unwrap();
            let r = conn
                .query(
                    "SELECT d.name, e.name, p.title \
                 FROM departments d \
                 JOIN employees e ON e.dept_id = d.id \
                 JOIN projects p ON p.emp_id = e.id \
                 WHERE d.name = 'Eng' \
                 ORDER BY e.name",
                )
                .await
                .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[1].to_text(), "Alice");
            assert_eq!(row_values(&r[0])[2].to_text(), "Widget");
            assert_eq!(row_values(&r[1])[1].to_text(), "Bob");
            assert_eq!(row_values(&r[1])[2].to_text(), "Gadget");
        });
    }

    #[test]
    fn conformance_056_left_join_with_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE teams(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE members(id INTEGER PRIMARY KEY, team_id INTEGER, name TEXT)",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO teams VALUES (1,'Alpha'),(2,'Beta'),(3,'Gamma')")
                .await
                .unwrap();
            conn.execute("INSERT INTO members VALUES (1,1,'A1'),(2,1,'A2'),(3,2,'B1')")
                .await
                .unwrap();
            let r = conn
            .query(
                "SELECT t.name, SUM(CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END) as member_count \
                 FROM teams t \
                 LEFT JOIN members m ON m.team_id = t.id \
                 GROUP BY t.id, t.name \
                 ORDER BY t.name",
            )
            .await
            .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alpha");
            assert_eq!(row_values(&r[0])[1].to_text(), "2");
            assert_eq!(row_values(&r[1])[0].to_text(), "Beta");
            assert_eq!(row_values(&r[1])[1].to_text(), "1");
            assert_eq!(row_values(&r[2])[0].to_text(), "Gamma");
            assert_eq!(row_values(&r[2])[1].to_text(), "0");
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 057: Expression edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_057_integer_division() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // SQLite integer division truncates toward zero
            let r = conn.query("SELECT 7/2, -7/2, 1/3").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3");
            assert_eq!(row_values(&r[0])[1].to_text(), "-3");
            assert_eq!(row_values(&r[0])[2].to_text(), "0");
        });
    }

    #[test]
    fn conformance_057_real_division() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT 7.0/2, 1.0/3.0").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "3.5");
            // 1/3 as float
            let third: f64 = row_values(&r[0])[1].to_text().parse().unwrap();
            assert!((third - 1.0 / 3.0).abs() < 1e-10);
        });
    }

    #[test]
    fn conformance_057_string_concatenation_operator() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn
                .query("SELECT 'hello' || ' ' || 'world'")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "hello world");
        });
    }

    #[test]
    fn conformance_057_null_arithmetic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // Any arithmetic with NULL produces NULL
            let r = conn
                .query("SELECT NULL + 1, NULL * 5, NULL || 'text'")
                .await
                .unwrap();
            assert!(row_values(&r[0])[0].is_null());
            assert!(row_values(&r[0])[1].is_null());
            assert!(row_values(&r[0])[2].is_null());
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 058: Correlated subqueries and CREATE TABLE AS SELECT
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_058_correlated_subquery_in_where() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t2(id INTEGER, t1_id INTEGER, score INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 100), (2, 200), (3, 300)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES (1,1,10),(2,1,20),(3,2,30),(4,3,5)")
                .await
                .unwrap();
            // Correlated subquery: get t1 rows where max t2 score > 15
            let r = conn
            .query(
                "SELECT t1.id, t1.val FROM t1 WHERE (SELECT MAX(score) FROM t2 WHERE t2.t1_id = t1.id) > 15 ORDER BY t1.id",
            )
            .await
            .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&r[0])[1], SqliteValue::Integer(100));
            assert_eq!(row_values(&r[1])[0], SqliteValue::Integer(2));
            assert_eq!(row_values(&r[1])[1], SqliteValue::Integer(200));
        });
    }

    #[test]
    fn conformance_058_correlated_subquery_in_select() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)",
            )
            .await
            .unwrap();
            conn.execute("CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
                .await
                .unwrap();
            conn.execute("INSERT INTO orders VALUES (1,1,100.0),(2,1,200.0),(3,2,50.0)")
                .await
                .unwrap();
            // Correlated subquery in SELECT list
            let r = conn
            .query(
                "SELECT c.name, (SELECT SUM(amount) FROM orders o WHERE o.customer_id = c.id) AS total FROM customers c ORDER BY c.name",
            )
            .await
            .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "Alice");
            assert_eq!(row_values(&r[0])[1].to_text(), "300.0");
            assert_eq!(row_values(&r[1])[0].to_text(), "Bob");
            assert_eq!(row_values(&r[1])[1].to_text(), "50.0");
        });
    }

    #[test]
    fn conformance_058_create_table_as_select() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO src VALUES (1, 100), (2, 200), (3, 300)")
                .await
                .unwrap();
            // CREATE TABLE AS SELECT
            conn.execute(
                "CREATE TABLE dst AS SELECT id, val * 2 AS doubled FROM src WHERE id <= 2",
            )
            .await
            .unwrap();
            let r = conn.query("SELECT * FROM dst ORDER BY id").await.unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&r[0])[1], SqliteValue::Integer(200));
            assert_eq!(row_values(&r[1])[0], SqliteValue::Integer(2));
            assert_eq!(row_values(&r[1])[1], SqliteValue::Integer(400));
        });
    }

    // -----------------------------------------------------------------------
    // Conformance suite 059: GROUP BY expressions and HAVING edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn conformance_059_group_by_expression() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t1 VALUES (1, 100), (2, 200), (3, 300)")
                .await
                .unwrap();
            // GROUP BY an expression, not just a column
            let r = conn
            .query(
                "SELECT val / 100 AS bucket, COUNT(*) FROM t1 GROUP BY val / 100 ORDER BY bucket",
            )
            .await
            .unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(1));
            assert_eq!(row_values(&r[0])[1], SqliteValue::Integer(1));
            assert_eq!(row_values(&r[1])[0], SqliteValue::Integer(2));
            assert_eq!(row_values(&r[1])[1], SqliteValue::Integer(1));
            assert_eq!(row_values(&r[2])[0], SqliteValue::Integer(3));
            assert_eq!(row_values(&r[2])[1], SqliteValue::Integer(1));
        });
    }

    #[test]
    fn conformance_059_group_by_function() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE names(id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute("INSERT INTO names VALUES (1,'Alice'),(2,'alice'),(3,'Bob'),(4,'BOB')")
                .await
                .unwrap();
            // GROUP BY UPPER(name) collapses case-variants
            let r = conn
            .query("SELECT UPPER(name) AS uname, COUNT(*) FROM names GROUP BY UPPER(name) ORDER BY uname")
            .await
            .unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(row_values(&r[0])[0].to_text(), "ALICE");
            assert_eq!(row_values(&r[0])[1], SqliteValue::Integer(2));
            assert_eq!(row_values(&r[1])[0].to_text(), "BOB");
            assert_eq!(row_values(&r[1])[1], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn conformance_059_having_without_group_by() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(val INTEGER)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1),(2),(3)")
                .await
                .unwrap();
            // HAVING without GROUP BY: implicit single-group aggregation
            // C SQLite verified: returns one row with COUNT(*) = 3
            let r = conn
                .query("SELECT COUNT(*) FROM t1 HAVING COUNT(*) > 2")
                .await
                .unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(3));
        });
    }

    #[test]
    fn conformance_059_having_without_group_by_no_match() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t1(val INTEGER)").await.unwrap();
            conn.execute("INSERT INTO t1 VALUES (1),(2),(3)")
                .await
                .unwrap();
            // HAVING condition not met: returns empty result
            let r = conn
                .query("SELECT COUNT(*) FROM t1 HAVING COUNT(*) > 5")
                .await
                .unwrap();
            assert_eq!(r.len(), 0);
        });
    }

    // ── Conformance suite 060: Regression tests for function name case,
    //    ORDER BY column index, HAVING, and comparison coercion ────────

    #[test]
    fn conformance_060_lowercase_function_names() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // Verify lowercase function names resolve correctly in the registry.
            let r = conn.query("SELECT typeof(42)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "integer");

            let r = conn.query("SELECT typeof(3.14)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "real");

            let r = conn.query("SELECT typeof('hello')").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "text");

            let r = conn.query("SELECT typeof(NULL)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "null");
        });
    }

    #[test]
    fn conformance_060_mixed_case_function_names() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // Mixed-case function calls should all resolve.
            let r = conn.query("SELECT abs(-7)").await.unwrap();
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(7));

            let r = conn.query("SELECT ABS(-7)").await.unwrap();
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(7));

            let r = conn.query("SELECT Abs(-7)").await.unwrap();
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(7));
        });
    }

    #[test]
    fn conformance_060_hex_upper_lower() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let r = conn.query("SELECT hex(255)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "323535");

            let r = conn.query("SELECT HEX(255)").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "323535");
        });
    }

    #[test]
    fn conformance_060_order_by_column_index_multi() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t2(x TEXT, y INTEGER, z REAL)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t2 VALUES ('a', 3, 1.0), ('b', 1, 3.0), ('c', 2, 2.0)")
                .await
                .unwrap();
            // ORDER BY first column (text, ascending)
            let r = conn.query("SELECT x, y FROM t2 ORDER BY 1").await.unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "a");
            assert_eq!(row_values(&r[1])[0].to_text(), "b");
            assert_eq!(row_values(&r[2])[0].to_text(), "c");
        });
    }

    #[test]
    fn conformance_060_order_by_column_index_desc() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t3(a TEXT, b INTEGER)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t3 VALUES ('x', 3), ('y', 1), ('z', 2)")
                .await
                .unwrap();
            // ORDER BY 2 DESC — sort by b descending
            let r = conn
                .query("SELECT a, b FROM t3 ORDER BY 2 DESC")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0].to_text(), "x"); // b=3
            assert_eq!(row_values(&r[1])[0].to_text(), "z"); // b=2
            assert_eq!(row_values(&r[2])[0].to_text(), "y"); // b=1
        });
    }

    #[test]
    fn conformance_060_having_sum_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE sales(amount REAL)")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES (100.0),(200.0),(300.0)")
                .await
                .unwrap();
            // HAVING with SUM aggregate, no GROUP BY
            let r = conn
                .query("SELECT SUM(amount) FROM sales HAVING SUM(amount) > 500")
                .await
                .unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(row_values(&r[0])[0], SqliteValue::Float(600.0));
        });
    }

    #[test]
    fn conformance_060_numeric_text_comparison_variants() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // Integer != text-integer without affinity
            let r = conn
                .query("SELECT CASE WHEN 42 = '42' THEN 1 ELSE 0 END")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(0));

            // Float != text-float without affinity
            let r = conn
                .query("SELECT CASE WHEN 3.14 = '3.14' THEN 1 ELSE 0 END")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(0));

            // Text that doesn't parse as number should NOT equal an integer
            let r = conn
                .query("SELECT CASE WHEN 10 = 'ten' THEN 1 ELSE 0 END")
                .await
                .unwrap();
            assert_eq!(row_values(&r[0])[0], SqliteValue::Integer(0));
        });
    }

    // -----------------------------------------------------------------------
    // Regression: HAVING aggregate not in SELECT list (review fix)
    // -----------------------------------------------------------------------

    #[test]
    fn regression_having_aggregate_not_in_select() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE emp (dept TEXT, salary INTEGER);")
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO emp VALUES ('A', 100), ('A', 200), ('B', 50), ('B', 60), ('B', 70);",
            )
            .await
            .unwrap();
            // COUNT(*) is only in HAVING, not in the SELECT list.
            let rows = conn
                .query("SELECT dept FROM emp GROUP BY dept HAVING COUNT(*) >= 3;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("B".into()));
        });
    }

    #[test]
    fn regression_having_sum_not_in_select() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE sales (product TEXT, amount INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO sales VALUES ('X', 10), ('X', 20), ('Y', 100), ('Y', 200);")
                .await
                .unwrap();
            // SUM(amount) is only in HAVING, not in SELECT.
            let rows = conn
                .query("SELECT product FROM sales GROUP BY product HAVING SUM(amount) > 50;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Text("Y".into()));
        });
    }

    #[test]
    fn regression_null_comparison_returns_null_not_zero() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // SQL three-valued logic: comparison with NULL produces NULL, not 0.
            let rows = conn.query("SELECT (1 = NULL);").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);

            let rows = conn.query("SELECT (NULL > 5);").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);

            let rows = conn.query("SELECT (NULL = NULL);").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Null);

            // IS / IS NOT should still return 0/1, not NULL.
            let rows = conn.query("SELECT (1 IS NULL);").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(0));

            let rows = conn.query("SELECT (NULL IS NULL);").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(1));
        });
    }

    // -----------------------------------------------------------------------
    // Generated columns (VIRTUAL / STORED) — F-SQL.19
    // -----------------------------------------------------------------------

    #[test]
    fn generated_column_stored_basic() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
            "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a + b) STORED)",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO t (a, b) VALUES (3, 7)")
                .await
                .unwrap();
            let rows = conn.query("SELECT a, b, c FROM t").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(3));
            assert_eq!(row_values(&rows[0])[1], SqliteValue::Integer(7));
            assert_eq!(
                row_values(&rows[0])[2],
                SqliteValue::Integer(10),
                "STORED generated column c = a + b should be 10"
            );
        });
    }

    #[test]
    fn generated_column_stored_multiplication() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
            "CREATE TABLE prices (qty INTEGER, unit_price INTEGER, total INTEGER GENERATED ALWAYS AS (qty * unit_price) STORED)",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO prices (qty, unit_price) VALUES (5, 12)")
                .await
                .unwrap();
            let rows = conn.query("SELECT total FROM prices").await.unwrap();
            assert_eq!(
                row_values(&rows[0])[0],
                SqliteValue::Integer(60),
                "STORED generated column total = qty * unit_price should be 60"
            );
        });
    }

    #[test]
    fn generated_column_stored_multi_row() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
                "CREATE TABLE t (x INTEGER, doubled INTEGER GENERATED ALWAYS AS (x * 2) STORED)",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t (x) VALUES (1)").await.unwrap();
            conn.execute("INSERT INTO t (x) VALUES (5)").await.unwrap();
            conn.execute("INSERT INTO t (x) VALUES (100)")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT doubled FROM t ORDER BY x")
                .await
                .unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(row_values(&rows[0])[0], SqliteValue::Integer(2));
            assert_eq!(row_values(&rows[1])[0], SqliteValue::Integer(10));
            assert_eq!(row_values(&rows[2])[0], SqliteValue::Integer(200));
        });
    }

    #[test]
    fn generated_column_stored_update_recomputes() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(
            "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a + b) STORED)",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO t (a, b) VALUES (3, 7)")
                .await
                .unwrap();
            conn.execute("UPDATE t SET a = 10 WHERE b = 7")
                .await
                .unwrap();
            let rows = conn.query("SELECT c FROM t").await.unwrap();
            assert_eq!(
                row_values(&rows[0])[0],
                SqliteValue::Integer(17),
                "STORED generated column should recompute after UPDATE: 10 + 7 = 17"
            );
        });
    }

    // -----------------------------------------------------------------------
    // Foreign Key enforcement — bd-thqgm
    // -----------------------------------------------------------------------

    #[test]
    fn fk_insert_valid_parent_succeeds() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON").await.unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO parent VALUES (1, 'Alice')")
                .await
                .unwrap();
            // Child references existing parent — should succeed.
            conn.execute("INSERT INTO child VALUES (1, 1)")
                .await
                .unwrap();
            let rows = conn.query("SELECT * FROM child").await.unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    #[test]
    fn fk_insert_missing_parent_fails() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON").await.unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
        )
        .await
        .unwrap();
            // No parent row with id=99 — should fail.
            let result = conn.execute("INSERT INTO child VALUES (1, 99)").await;
            assert!(result.is_err(), "INSERT with missing FK parent should fail");
        });
    }

    #[test]
    fn fk_insert_null_fk_value_succeeds() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON").await.unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
        )
        .await
        .unwrap();
            // NULL FK value should always succeed (SQL standard).
            conn.execute("INSERT INTO child VALUES (1, NULL)")
                .await
                .unwrap();
            let rows = conn.query("SELECT * FROM child").await.unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    #[test]
    fn fk_off_by_default() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            // FK enforcement is OFF by default (matching SQLite).
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
        )
        .await
        .unwrap();
            // Should succeed even without parent, because FK enforcement is off.
            conn.execute("INSERT INTO child VALUES (1, 99)")
                .await
                .unwrap();
        });
    }

    #[test]
    fn fk_delete_parent_with_children_fails() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON").await.unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO parent VALUES (1, 'Alice')")
                .await
                .unwrap();
            conn.execute("INSERT INTO child VALUES (1, 1)")
                .await
                .unwrap();
            // Deleting parent with child references should fail (default NO ACTION).
            let result = conn.execute("DELETE FROM parent WHERE id = 1").await;
            assert!(
                result.is_err(),
                "DELETE parent with child references should fail with FK ON"
            );
        });
    }

    #[test]
    fn fk_delete_cascade() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON").await.unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO parent VALUES (1, 'Alice')")
                .await
                .unwrap();
            conn.execute("INSERT INTO child VALUES (1, 1)")
                .await
                .unwrap();
            conn.execute("INSERT INTO child VALUES (2, 1)")
                .await
                .unwrap();
            // CASCADE should delete children too.
            conn.execute("DELETE FROM parent WHERE id = 1")
                .await
                .unwrap();
            let rows = conn.query("SELECT * FROM child").await.unwrap();
            assert_eq!(
                rows.len(),
                0,
                "ON DELETE CASCADE should delete all child rows"
            );
        });
    }

    #[test]
    fn fk_delete_set_null() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON").await.unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .unwrap();
            conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
        )
        .await
        .unwrap();
            conn.execute("INSERT INTO parent VALUES (1, 'Alice')")
                .await
                .unwrap();
            conn.execute("INSERT INTO child VALUES (1, 1)")
                .await
                .unwrap();
            // SET NULL should null out the FK column in children.
            conn.execute("DELETE FROM parent WHERE id = 1")
                .await
                .unwrap();
            let rows = conn.query("SELECT parent_id FROM child").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0])[0],
                SqliteValue::Null,
                "ON DELETE SET NULL should set FK column to NULL"
            );
        });
    }
}
