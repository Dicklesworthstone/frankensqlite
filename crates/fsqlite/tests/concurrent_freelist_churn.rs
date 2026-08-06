//! GH#302 e2e verification: default (concurrent-promoted) transactions must
//! reuse committed freelist pages without breaking an already-pinned reader.
//! Steady-state churn must plateau in `PRAGMA page_count`, and reused pages
//! must still expose their old bytes to an older WAL snapshot held in a truly
//! separate process, without relying on process-global engine state.
//!
//! Run: `cargo test -p fsqlite --test concurrent_freelist_churn`

#![allow(clippy::future_not_send, clippy::large_futures)]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use std::env;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SNAPSHOT_ROW_COUNT: usize = 160;
const SNAPSHOT_BATCH_SIZE: usize = 16;
const SNAPSHOT_PAYLOAD_BYTES: usize = 1_536;
const OLD_ID_BASE: i64 = 1_000;
const NEW_ID_BASE: i64 = 2_000;
const EXTERNAL_READER_HELPER_TEST: &str = "external_process_old_snapshot_reader_helper";
const EXTERNAL_READER_DATABASE_ENV: &str = "FSQLITE_GH302_EXTERNAL_READER_DATABASE";
const EXTERNAL_READER_TOKEN_ENV: &str = "FSQLITE_GH302_EXTERNAL_READER_TOKEN";
const EXTERNAL_READER_READY_PREFIX: &str = "FSQLITE_GH302_READER_READY:";
const EXTERNAL_READER_READ_PREFIX: &str = "FSQLITE_GH302_READER_READ:";
const EXTERNAL_READER_COMPLETE_PREFIX: &str = "FSQLITE_GH302_READER_COMPLETE:";
const EXTERNAL_READER_RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
const EXTERNAL_READER_EXIT_TIMEOUT: Duration = Duration::from_secs(10);
const EXTERNAL_READER_POLL_INTERVAL: Duration = Duration::from_millis(25);

async fn pragma_u64(conn: &Connection, pragma: &str) -> u64 {
    let rows = conn.query(pragma).await.expect("pragma query");
    match rows[0].values().first() {
        Some(SqliteValue::Integer(v)) => u64::try_from(*v).expect("non-negative pragma value"),
        other => panic!("unexpected pragma row shape for {pragma}: {other:?}"),
    }
}

async fn configure_default_wal_connection(conn: &Connection, role: &str) {
    conn.set_reject_mem_fallback(true);
    conn.set_strict_mem_fallback_rejection(true);
    assert!(
        conn.is_concurrent_mode_default(),
        "{role} must retain FrankenSQLite's concurrent-default contract"
    );

    let journal_mode = conn
        .query("PRAGMA journal_mode")
        .await
        .expect("read journal mode");
    assert!(
        matches!(
            journal_mode.first().and_then(|row| row.values().first()),
            Some(SqliteValue::Text(mode)) if mode.as_ref() == "wal"
        ),
        "{role} must report WAL mode, got {journal_mode:?}"
    );

    conn.execute("PRAGMA wal_autocheckpoint = 0")
        .await
        .expect("disable WAL autocheckpoint");
    assert_eq!(
        pragma_u64(conn, "PRAGMA wal_autocheckpoint").await,
        0,
        "{role} WAL autocheckpoint must remain disabled"
    );
}

fn generation_payload(generation: &str, filler: &str, ordinal: usize) -> String {
    let prefix = format!("{generation}-{ordinal:04}-");
    assert!(prefix.len() < SNAPSHOT_PAYLOAD_BYTES);
    format!(
        "{prefix}{}",
        filler.repeat(SNAPSHOT_PAYLOAD_BYTES - prefix.len())
    )
}

async fn insert_generation(conn: &Connection, id_base: i64, generation: &str, filler: &str) {
    for batch_start in (0..SNAPSHOT_ROW_COUNT).step_by(SNAPSHOT_BATCH_SIZE) {
        let batch_end = (batch_start + SNAPSHOT_BATCH_SIZE).min(SNAPSHOT_ROW_COUNT);
        let values = (batch_start..batch_end)
            .map(|ordinal| {
                let id = id_base + i64::try_from(ordinal).expect("snapshot row id fits i64");
                let payload = generation_payload(generation, filler, ordinal);
                format!("({id}, '{payload}')")
            })
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute(&format!(
            "INSERT INTO snapshot_churn(id, payload) VALUES {values}"
        ))
        .await
        .expect("insert snapshot generation batch");
    }
}

fn assert_generation(rows: &[fsqlite::Row], id_base: i64, generation: &str, filler: &str) {
    assert_eq!(
        rows.len(),
        SNAPSHOT_ROW_COUNT,
        "{generation} snapshot row count"
    );
    for (ordinal, row) in rows.iter().enumerate() {
        let expected_id = id_base + i64::try_from(ordinal).expect("snapshot row ordinal fits i64");
        let expected_payload = generation_payload(generation, filler, ordinal);
        match row.values() {
            [
                SqliteValue::Integer(actual_id),
                SqliteValue::Text(actual_payload),
            ] => {
                assert_eq!(
                    *actual_id, expected_id,
                    "{generation} snapshot id at ordinal {ordinal}"
                );
                assert_eq!(
                    actual_payload.as_ref(),
                    expected_payload.as_str(),
                    "{generation} snapshot payload at ordinal {ordinal}"
                );
            }
            values => {
                panic!("unexpected {generation} snapshot row at ordinal {ordinal}: {values:?}")
            }
        }
    }
}

async fn seed_snapshot_fixture(writer: &Connection) {
    writer
        .execute("BEGIN IMMEDIATE")
        .await
        .expect("begin deterministic seed transaction");
    assert!(
        !writer.is_concurrent_transaction(),
        "BEGIN IMMEDIATE fixture setup must not use concurrent page leases"
    );
    writer
        .execute(
            "CREATE TABLE snapshot_churn(\
                id INTEGER PRIMARY KEY, \
                payload TEXT NOT NULL\
            )",
        )
        .await
        .expect("create snapshot churn table");
    insert_generation(writer, OLD_ID_BASE, "old", "o").await;
    writer
        .execute("COMMIT")
        .await
        .expect("commit seed transaction");

    let seeded_page_count = pragma_u64(writer, "PRAGMA page_count").await;
    assert!(
        seeded_page_count > 2,
        "seed must span multiple pages, got page_count={seeded_page_count}"
    );
    assert_eq!(
        pragma_u64(writer, "PRAGMA freelist_count").await,
        0,
        "deterministic seed must not leave unused leased pages on the freelist"
    );
}

async fn commit_delete_and_replacement(writer: &Connection) {
    let freelist_before_delete = pragma_u64(writer, "PRAGMA freelist_count").await;
    assert_eq!(
        freelist_before_delete, 0,
        "reuse proof requires an empty baseline freelist"
    );

    writer
        .execute("BEGIN")
        .await
        .expect("begin delete transaction");
    assert!(
        writer.is_concurrent_transaction(),
        "delete transaction must exercise the concurrent-default allocator"
    );
    writer
        .execute("DELETE FROM snapshot_churn")
        .await
        .expect("delete old generation");
    writer
        .execute("COMMIT")
        .await
        .expect("commit old-generation delete");

    let page_count_after_delete = pragma_u64(writer, "PRAGMA page_count").await;
    let freelist_after_delete = pragma_u64(writer, "PRAGMA freelist_count").await;
    assert!(
        freelist_after_delete > freelist_before_delete,
        "committed delete must release pages: before={freelist_before_delete} \
         after={freelist_after_delete} page_count={page_count_after_delete}"
    );

    writer
        .execute("BEGIN")
        .await
        .expect("begin replacement transaction");
    assert!(
        writer.is_concurrent_transaction(),
        "replacement transaction must exercise the concurrent-default allocator"
    );
    insert_generation(writer, NEW_ID_BASE, "new", "n").await;
    writer
        .execute("COMMIT")
        .await
        .expect("commit replacement generation");

    let page_count_after_reuse = pragma_u64(writer, "PRAGMA page_count").await;
    let freelist_after_reuse = pragma_u64(writer, "PRAGMA freelist_count").await;
    assert_eq!(
        page_count_after_reuse, page_count_after_delete,
        "equal-sized replacements must reuse committed free pages instead of growing EOF; \
         freelist before={freelist_before_delete} after_delete={freelist_after_delete} \
         after_reuse={freelist_after_reuse}"
    );
    assert!(
        freelist_after_reuse < freelist_after_delete,
        "replacement commit must consume the committed freelist: \
         after_delete={freelist_after_delete} after_reuse={freelist_after_reuse} \
         page_count={page_count_after_reuse}"
    );
}

async fn assert_fresh_generation(conn: &Connection) {
    conn.execute("BEGIN")
        .await
        .expect("begin fresh reader transaction");
    assert!(
        conn.is_concurrent_transaction(),
        "fresh transaction must retain the concurrent-default contract"
    );
    let fresh_rows = conn
        .query("SELECT id, payload FROM snapshot_churn ORDER BY id")
        .await
        .expect("read replacement generation from fresh snapshot");
    assert_generation(&fresh_rows, NEW_ID_BASE, "new", "n");
    conn.execute("COMMIT")
        .await
        .expect("finish fresh reader transaction");
}

fn assert_stock_integrity_and_replacement(database_path: &Path) {
    let stock = rusqlite::Connection::open(database_path).expect("open with stock SQLite");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("stock SQLite integrity_check");
    assert_eq!(integrity, "ok", "stock SQLite must accept the final file");

    let mut statement = stock
        .prepare("SELECT id, payload FROM snapshot_churn ORDER BY id")
        .expect("prepare stock replacement query");
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query stock replacement generation")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("decode stock replacement generation");
    assert_eq!(
        rows.len(),
        SNAPSHOT_ROW_COUNT,
        "stock SQLite replacement row count"
    );
    for (ordinal, (actual_id, actual_payload)) in rows.iter().enumerate() {
        let expected_id = NEW_ID_BASE + i64::try_from(ordinal).expect("stock row ordinal fits i64");
        let expected_payload = generation_payload("new", "n", ordinal);
        assert_eq!(
            *actual_id, expected_id,
            "stock SQLite replacement id at ordinal {ordinal}"
        );
        assert_eq!(
            actual_payload, &expected_payload,
            "stock SQLite replacement payload at ordinal {ordinal}"
        );
    }
}

struct ExternalReaderChild {
    child: Child,
    input: Option<ChildStdin>,
    receipts: Receiver<String>,
    stdout_reader: Option<JoinHandle<()>>,
    transcript: Vec<String>,
    token: String,
    reaped: bool,
}

impl ExternalReaderChild {
    // Ownership transfers into this RAII guard; `wait_for_success` reaps the
    // normal path and `Drop` kills/reaps every early-return or panic path.
    #[allow(clippy::zombie_processes)]
    fn spawn(database_path: &Path) -> Self {
        let token = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        );
        let mut child = Command::new(env::current_exe().expect("resolve current test executable"))
            .args([
                "--exact",
                EXTERNAL_READER_HELPER_TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(EXTERNAL_READER_DATABASE_ENV, database_path.as_os_str())
            .env(EXTERNAL_READER_TOKEN_ENV, &token)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn external old-reader helper");
        let Some(input) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            panic!("capture helper stdin");
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill();
            let _ = child.wait();
            panic!("capture helper stdout");
        };
        let (receipt_tx, receipts) = mpsc::channel();
        let stdout_reader = match std::thread::Builder::new()
            .name("gh302-external-reader-stdout".to_owned())
            .spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let line = match line {
                        Ok(line) => line,
                        Err(error) => format!("<helper stdout read error: {error}>"),
                    };
                    if receipt_tx.send(line).is_err() {
                        break;
                    }
                }
            }) {
            Ok(reader) => reader,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("spawn helper stdout reader: {error}");
            }
        };

        Self {
            child,
            input: Some(input),
            receipts,
            stdout_reader: Some(stdout_reader),
            transcript: Vec::new(),
            token,
            reaped: false,
        }
    }

    fn drain_receipts(&mut self) {
        self.transcript.extend(self.receipts.try_iter());
    }

    fn join_stdout_reader(&mut self) {
        if let Some(reader) = self.stdout_reader.take() {
            reader.join().expect("helper stdout reader must not panic");
        }
        self.drain_receipts();
    }

    fn terminate_and_reap(&mut self) {
        self.input.take();
        if self.reaped {
            return;
        }
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.reaped = true;
    }

    fn wait_for_receipt(&mut self, prefix: &str, phase: &str) {
        let expected = format!("{prefix}{}", self.token);
        let deadline = Instant::now() + EXTERNAL_READER_RECEIPT_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.terminate_and_reap();
                self.join_stdout_reader();
                panic!(
                    "external reader timed out waiting for {phase} receipt {expected:?}; \
                     stdout={:?}",
                    self.transcript
                );
            }

            match self
                .receipts
                .recv_timeout(remaining.min(EXTERNAL_READER_POLL_INTERVAL))
            {
                Ok(line) => {
                    let matched = line.split_whitespace().any(|receipt| receipt == expected);
                    self.transcript.push(line);
                    if matched {
                        return;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    if let Some(status) = self.child.try_wait().unwrap_or_else(|error| {
                        panic!("poll external reader while waiting for {phase}: {error}")
                    }) {
                        self.reaped = true;
                        self.join_stdout_reader();
                        panic!(
                            "external reader exited with {status} before {phase} receipt \
                             {expected:?}; stdout={:?}",
                            self.transcript
                        );
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.terminate_and_reap();
                    self.join_stdout_reader();
                    panic!(
                        "external reader closed stdout before {phase} receipt {expected:?}; \
                         stdout={:?}",
                        self.transcript
                    );
                }
            }
        }
    }

    fn request_first_target_read(&mut self) {
        let command = format!("{EXTERNAL_READER_READ_PREFIX}{}", self.token);
        let mut input = self.input.take().expect("external reader stdin available");
        writeln!(input, "{command}").expect("send external reader read command");
        input.flush().expect("flush external reader read command");
    }

    fn wait_for_success(mut self) {
        let deadline = Instant::now() + EXTERNAL_READER_EXIT_TIMEOUT;
        let status = loop {
            if let Some(status) = self
                .child
                .try_wait()
                .expect("poll completed external reader")
            {
                self.reaped = true;
                break status;
            }
            if Instant::now() >= deadline {
                self.terminate_and_reap();
                self.join_stdout_reader();
                panic!(
                    "external reader did not exit within {:?}; stdout={:?}",
                    EXTERNAL_READER_EXIT_TIMEOUT, self.transcript
                );
            }
            std::thread::sleep(EXTERNAL_READER_POLL_INTERVAL);
        };

        self.join_stdout_reader();
        assert!(
            status.success(),
            "external reader failed with {status}; stdout={:?}",
            self.transcript
        );
    }
}

impl Drop for ExternalReaderChild {
    fn drop(&mut self) {
        self.terminate_and_reap();
        if let Some(reader) = self.stdout_reader.take() {
            let _ = reader.join();
        }
    }
}

#[test]
fn default_churn_page_count_plateaus_after_warmup() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("churn.db").to_string_lossy().into_owned();
        let conn = Connection::open(&path).await.expect("open churn db");
        conn.set_reject_mem_fallback(true);
        conn.set_strict_mem_fallback_rejection(true);

        let payload = "x".repeat(200);
        let mut page_counts = Vec::new();
        for _cycle in 0..6u32 {
            conn.execute(
                "CREATE TABLE churn(id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, payload TEXT NOT NULL)",
            )
            .await
            .expect("create churn table");
            conn.execute("CREATE INDEX churn_grp ON churn(grp, id)")
                .await
                .expect("create churn index");
            for batch in 0..8u32 {
                let values = (0..25u32)
                    .map(|i| format!("({}, '{payload}')", u64::from(batch * 25 + i) % 7))
                    .collect::<Vec<_>>()
                    .join(", ");
                conn.execute(&format!("INSERT INTO churn(grp, payload) VALUES {values}"))
                    .await
                    .expect("insert churn batch");
            }
            conn.execute("DROP TABLE churn").await.expect("drop churn");
            page_counts.push(pragma_u64(&conn, "PRAGMA page_count").await);
        }

        // Warm-up may grow the file; steady state must not. Allow the second
        // cycle as the high-water mark and require every later cycle to stay
        // at or below it.
        let high_water = page_counts[1];
        for (cycle, &count) in page_counts.iter().enumerate().skip(2) {
            assert!(
                count <= high_water,
                "page_count must plateau after warm-up: cycle={cycle} count={count} \
                 high_water={high_water} all={page_counts:?}"
            );
        }

        // The engine's own integrity check must pass on the churned database.
        let verdict = conn
            .query("PRAGMA integrity_check")
            .await
            .expect("integrity_check");
        let ok = matches!(
            verdict[0].values().first(),
            Some(SqliteValue::Text(s)) if s.as_ref() == "ok"
        );
        assert!(
            ok,
            "integrity_check must return ok, got {:?}",
            verdict[0].values()
        );
    });
}

#[test]
#[ignore = "subprocess helper for external-process GH#302 snapshot proof"]
fn external_process_old_snapshot_reader_helper() {
    let Some(database_path) = env::var_os(EXTERNAL_READER_DATABASE_ENV) else {
        assert!(
            env::var_os(EXTERNAL_READER_TOKEN_ENV).is_none(),
            "external reader token must not exist without its database path"
        );
        return;
    };
    let token = env::var(EXTERNAL_READER_TOKEN_ENV).expect("external reader token");
    let path = std::path::PathBuf::from(database_path)
        .to_string_lossy()
        .into_owned();

    asupersync::test_utils::run_test(|| async {
        let reader = Connection::open(&path).await.expect("external reader open");
        configure_default_wal_connection(&reader, "external reader").await;
        reader
            .execute("BEGIN")
            .await
            .expect("external reader begin old snapshot");
        assert!(
            reader.is_concurrent_transaction(),
            "external reader BEGIN must auto-promote in its independent process"
        );

        println!("{EXTERNAL_READER_READY_PREFIX}{token}");
        std::io::stdout()
            .flush()
            .expect("flush external reader ready receipt");

        let mut command = String::new();
        let bytes_read = std::io::stdin()
            .read_line(&mut command)
            .expect("read parent command");
        assert!(bytes_read > 0, "parent closed IPC before authorizing read");
        let expected_command = format!("{EXTERNAL_READER_READ_PREFIX}{token}");
        assert_eq!(
            command.trim_end_matches(&['\r', '\n'][..]),
            expected_command.as_str(),
            "external reader received an invalid or stale command"
        );

        // No target-table access occurs above this point. The parent sends the
        // command only after both the delete and replacement commits complete.
        let old_rows = reader
            .query("SELECT id, payload FROM snapshot_churn ORDER BY id")
            .await
            .expect("external reader first target access");
        assert_generation(&old_rows, OLD_ID_BASE, "old", "o");
        reader
            .execute("COMMIT")
            .await
            .expect("external reader finish old snapshot");
        reader.close().await.expect("external reader close");

        println!("{EXTERNAL_READER_COMPLETE_PREFIX}{token}");
        std::io::stdout()
            .flush()
            .expect("flush external reader completion receipt");
    });
}

#[test]
fn local_connections_old_snapshot_survives_committed_freelist_reuse() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let database_path = dir.path().join("snapshot-reuse.db");
        let path = database_path.to_string_lossy().into_owned();

        let writer = Connection::open(&path).await.expect("open writer");
        configure_default_wal_connection(&writer, "writer").await;
        seed_snapshot_fixture(&writer).await;

        let reader = Connection::open(&path).await.expect("open old reader");
        configure_default_wal_connection(&reader, "reader").await;
        reader
            .execute("BEGIN")
            .await
            .expect("begin old reader snapshot");
        assert!(
            reader.is_concurrent_transaction(),
            "plain BEGIN must auto-promote on the concurrent-default reader"
        );

        commit_delete_and_replacement(&writer).await;

        // This is the old reader's first access to any target-table page. It
        // must still resolve every deleted-and-reused page through its pinned
        // pre-delete snapshot and recover the exact old bytes.
        let old_rows = reader
            .query("SELECT id, payload FROM snapshot_churn ORDER BY id")
            .await
            .expect("read old generation from pinned snapshot");
        assert_generation(&old_rows, OLD_ID_BASE, "old", "o");
        reader
            .execute("COMMIT")
            .await
            .expect("finish old reader snapshot");

        assert_fresh_generation(&reader).await;

        reader.close().await.expect("close reader");
        writer.close().await.expect("close writer");

        assert_stock_integrity_and_replacement(&database_path);
    });
}

#[test]
fn external_process_old_snapshot_survives_committed_freelist_reuse() {
    assert!(
        env::var_os(EXTERNAL_READER_DATABASE_ENV).is_none()
            && env::var_os(EXTERNAL_READER_TOKEN_ENV).is_none(),
        "external parent arm must never run inside its helper environment"
    );
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let database_path = dir.path().join("external-snapshot-reuse.db");
        let path = database_path.to_string_lossy().into_owned();

        let writer = Connection::open(&path).await.expect("open parent writer");
        configure_default_wal_connection(&writer, "parent writer").await;
        seed_snapshot_fixture(&writer).await;

        let mut old_reader = ExternalReaderChild::spawn(&database_path);
        old_reader.wait_for_receipt(EXTERNAL_READER_READY_PREFIX, "ready");

        commit_delete_and_replacement(&writer).await;

        old_reader.request_first_target_read();
        old_reader.wait_for_receipt(EXTERNAL_READER_COMPLETE_PREFIX, "completion");
        old_reader.wait_for_success();

        assert_fresh_generation(&writer).await;
        writer.close().await.expect("close parent writer");

        assert_stock_integrity_and_replacement(&database_path);
    });
}
