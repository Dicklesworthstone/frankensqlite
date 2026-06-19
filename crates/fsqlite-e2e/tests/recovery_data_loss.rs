//! bd-yfdb6 (OPS-3-2 CRITICAL): data-loss fault-injection test for WAL
//! recovery with two connections in flight.
//!
//! # Scenario
//!
//! Two processes open the same database via FrankenSQLite:
//!
//! 1. **Producer (child)** opens the DB, creates a table, and inserts rows in
//!    batches of 50 inside explicit transactions. After each `COMMIT`, the
//!    child appends one line to a side-car "commit log" (a plain file on
//!    disk) recording the transaction id it just committed. The child
//!    continues committing batches until the parent sends `SIGKILL`.
//! 2. **Killer (parent)** waits for the child to commit at least
//!    `KILL_AFTER_COMMITS` batches, then sends `SIGKILL`. The kill point is
//!    seeded from a deterministic RNG so we can reproduce runs.
//! 3. **Verifier (parent)** opens the DB a second time, runs recovery, and
//!    reads back all rows. Every row that appears in the commit log MUST
//!    be visible; if any are missing, the bug has reproduced and the test
//!    fails loudly.
//!
//! The whole sequence is repeated 20 times with different seeds. The test
//! succeeds only if all 20 iterations recover without data loss.

use std::env;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tempfile::tempdir;

const BEAD_ID: &str = "bd-yfdb6";
const HELPER_MODE_ENV: &str = "FSQLITE_YFDB6_HELPER_MODE";
const HELPER_DB_PATH_ENV: &str = "FSQLITE_YFDB6_DB_PATH";
const HELPER_COMMIT_LOG_ENV: &str = "FSQLITE_YFDB6_COMMIT_LOG";
const HELPER_STOP_AFTER_ENV: &str = "FSQLITE_YFDB6_STOP_AFTER";
const HELPER_TEST_NAME: &str = "yfdb6_producer_helper";

/// Rows inserted per transaction.
const BATCH_SIZE: i64 = 25;
/// Minimum committed batches before the parent may kill the child.
const KILL_AFTER_MIN_COMMITS: u32 = 4;
/// Upper bound on committed batches we wait for.  Anything above keeps the
/// child from running forever if the kill signal somehow fails to reach it.
const KILL_AFTER_MAX_COMMITS: u32 = 12;
/// How long the parent will wait for the child to publish commit-log entries
/// before declaring the run inconclusive.
const COMMIT_LOG_WAIT: Duration = Duration::from_secs(10);

fn helper_is_active() -> bool {
    env::var_os(HELPER_MODE_ENV).is_some()
}

fn row_count(conn: &Connection) -> i64 {
    let row = conn
        .query_row("SELECT COUNT(*) FROM t;")
        .expect("count query");
    match row.get(0) {
        Some(SqliteValue::Integer(count)) => *count,
        other => panic!("expected integer COUNT(*), got {other:?}"),
    }
}

fn ordered_ids(conn: &Connection) -> Vec<i64> {
    conn.query("SELECT id FROM t ORDER BY id;")
        .expect("query ordered ids")
        .into_iter()
        .map(|row| match row.get(0) {
            Some(SqliteValue::Integer(value)) => *value,
            other => panic!("expected integer id, got {other:?}"),
        })
        .collect()
}

fn setup_table(conn: &Connection) {
    conn.execute("PRAGMA journal_mode = WAL;")
        .expect("enable WAL mode");
    conn.execute("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, payload TEXT);")
        .expect("create table");
}

fn read_commit_log(path: &Path) -> Vec<(i64, i64)> {
    let f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let reader = BufReader::new(f);
    let mut out = Vec::new();
    for line in reader.lines() {
        let Ok(line) = line else { continue };
        let Some((lo, hi)) = line.split_once(',') else {
            continue;
        };
        let Ok(lo) = lo.trim().parse::<i64>() else {
            continue;
        };
        let Ok(hi) = hi.trim().parse::<i64>() else {
            continue;
        };
        out.push((lo, hi));
    }
    out
}

fn expected_rows_from_commit_log(entries: &[(i64, i64)]) -> Vec<i64> {
    let mut rows: Vec<i64> = Vec::new();
    for (lo, hi) in entries {
        for v in *lo..*hi {
            rows.push(v);
        }
    }
    rows.sort_unstable();
    rows
}

fn wait_for_committed_batches(commit_log: &Path, min_batches: u32) -> bool {
    let deadline = Instant::now() + COMMIT_LOG_WAIT;
    loop {
        let entries = read_commit_log(commit_log);
        if u32::try_from(entries.len()).unwrap_or(u32::MAX) >= min_batches {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn yfdb6_producer_child(db_path: &Path, commit_log: &Path, stop_after: u32) -> ! {
    let conn = Connection::open(db_path.to_string_lossy().as_ref()).expect("producer: open db");
    setup_table(&conn);

    let mut commit_log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(commit_log)
        .expect("producer: open commit log");

    let mut next_id: i64 = 0;
    let mut batches_committed: u32 = 0;
    loop {
        conn.execute("BEGIN IMMEDIATE;")
            .expect("producer: begin txn");
        let lo = next_id;
        let hi = lo + BATCH_SIZE;
        for id in lo..hi {
            conn.execute_with_params(
                "INSERT INTO t(id, payload) VALUES (?1, ?2);",
                &[
                    SqliteValue::Integer(id),
                    SqliteValue::Text(format!("row-{id}").into()),
                ],
            )
            .expect("producer: insert");
        }
        conn.execute("COMMIT;").expect("producer: commit");

        // Record (inclusive lo, exclusive hi) into the side-car commit log.
        writeln!(commit_log_file, "{lo},{hi}").expect("producer: append commit log");
        commit_log_file
            .sync_all()
            .expect("producer: sync commit log");

        next_id = hi;
        batches_committed += 1;

        if batches_committed >= stop_after {
            // Busy-loop until the parent kills us. We deliberately keep
            // starting a new transaction so the parent has a fighting chance
            // of landing the SIGKILL mid-write (which is the whole point of
            // this test).
            loop {
                conn.execute("BEGIN IMMEDIATE;")
                    .expect("producer: begin tail txn");
                for id in next_id..(next_id + BATCH_SIZE) {
                    conn.execute_with_params(
                        "INSERT INTO t(id, payload) VALUES (?1, ?2);",
                        &[
                            SqliteValue::Integer(id),
                            SqliteValue::Text(format!("tail-{id}").into()),
                        ],
                    )
                    .expect("producer: tail insert");
                }
                // Deliberately leave the txn uncommitted and start another.
                conn.execute("ROLLBACK;").expect("producer: tail rollback");
                next_id += BATCH_SIZE;
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn spawn_producer(db_path: &Path, commit_log: &Path, stop_after: u32) -> std::process::Child {
    Command::new(env::current_exe().expect("current_exe"))
        .arg("--exact")
        .arg(HELPER_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .env(HELPER_MODE_ENV, "producer")
        .env(HELPER_DB_PATH_ENV, db_path.as_os_str())
        .env(HELPER_COMMIT_LOG_ENV, commit_log.as_os_str())
        .env(HELPER_STOP_AFTER_ENV, stop_after.to_string())
        .spawn()
        .expect("spawn producer helper")
}

fn run_one_iteration(seed: u64, iteration: u32) {
    let dir = tempdir().expect("tempdir");
    let db_path: PathBuf = dir.path().join(format!("yfdb6_iter_{iteration}.db"));
    let commit_log: PathBuf = dir.path().join(format!("yfdb6_iter_{iteration}.commits"));

    let mut rng = StdRng::seed_from_u64(seed);
    let stop_after: u32 = rng.random_range(KILL_AFTER_MIN_COMMITS..=KILL_AFTER_MAX_COMMITS);

    let mut child = spawn_producer(&db_path, &commit_log, stop_after);

    // Wait for the child to publish at least KILL_AFTER_MIN_COMMITS commits
    // before killing it. If the child never publishes enough commits within
    // COMMIT_LOG_WAIT, the run is inconclusive and we abort it cleanly.
    if !wait_for_committed_batches(&commit_log, KILL_AFTER_MIN_COMMITS) {
        let _ = child.kill();
        let _ = child.wait();
        panic!(
            "[{BEAD_ID}] iter={iteration} seed={seed:#x}: producer never published \
             {KILL_AFTER_MIN_COMMITS} commits within {:?}",
            COMMIT_LOG_WAIT
        );
    }

    // Randomize a tiny extra delay after the minimum commit count so the
    // kill lands at a different point in the commit path each iteration.
    let extra_ms: u64 = rng.random_range(0..50);
    std::thread::sleep(Duration::from_millis(extra_ms));

    // Child::kill on unix is SIGKILL (see docs); on non-unix this is also
    // sufficient for the test's "unclean shutdown" semantics.
    let _ = child.kill();
    let _ = child.wait();

    // Collect what was committed before the kill.
    let commit_entries = read_commit_log(&commit_log);
    let expected_rows = expected_rows_from_commit_log(&commit_entries);

    // Open a fresh connection: this runs the WAL recovery path.
    let verifier = Connection::open(db_path.to_string_lossy().as_ref()).expect("verifier: open db");
    assert!(
        verifier.is_concurrent_mode_default(),
        "[{BEAD_ID}] recovered connection must keep concurrent mode enabled",
    );

    let actual_count = row_count(&verifier);
    let actual_rows = ordered_ids(&verifier);

    assert_eq!(
        actual_count,
        i64::try_from(actual_rows.len()).expect("row count fits i64"),
        "[{BEAD_ID}] COUNT(*) disagreed with SELECT id ORDER BY id",
    );

    // Every row that was recorded as committed in the commit log MUST be
    // visible post-recovery. Missing rows == data loss == the OPS-3-2 bug.
    for row in &expected_rows {
        assert!(
            actual_rows.binary_search(row).is_ok(),
            "[{BEAD_ID}] DATA LOSS iter={iteration} seed={seed:#x}: committed row \
             id={row} is missing after recovery. committed={} recovered={}",
            expected_rows.len(),
            actual_rows.len(),
        );
    }

    eprintln!(
        "[{BEAD_ID}] iter={iteration} seed={seed:#x} stop_after={stop_after} \
         committed_batches={} committed_rows={} recovered_rows={}",
        commit_entries.len(),
        expected_rows.len(),
        actual_rows.len(),
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns child processes; not for miri")]
fn two_process_sigkill_recovery_loses_no_committed_writes() {
    if helper_is_active() {
        // When re-exec'd as the producer child, the helper entrypoint below
        // handles the work; the top-level test body should never run.
        return;
    }
    // Deterministic base seed bound to the bead id. Twenty iterations ×
    // different per-iteration seeds covers enough kill points to surface
    // the race reproducibly.
    let base_seed: u64 = 0x0000_0000_0000_BD6F_u64;
    let iterations: u32 = 20;
    for i in 0..iterations {
        let seed = base_seed
            .wrapping_mul(1 + u64::from(i))
            .wrapping_add(u64::from(i) * 7919);
        run_one_iteration(seed, i);
    }
    eprintln!(
        "[{BEAD_ID}] SIGKILL fault-injection test passed {iterations} iterations with \
         no data loss.",
    );
}

#[test]
#[ignore = "invoked via subprocess by two_process_sigkill_recovery_loses_no_committed_writes"]
fn yfdb6_producer_helper() {
    let Some(mode) = env::var_os(HELPER_MODE_ENV) else {
        return;
    };
    let Some(db_path) = env::var_os(HELPER_DB_PATH_ENV) else {
        return;
    };
    let Some(commit_log) = env::var_os(HELPER_COMMIT_LOG_ENV) else {
        return;
    };
    let stop_after: u32 = env::var(HELPER_STOP_AFTER_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(KILL_AFTER_MAX_COMMITS);

    let db_path = PathBuf::from(db_path);
    let commit_log = PathBuf::from(commit_log);
    match mode.to_string_lossy().as_ref() {
        "producer" => yfdb6_producer_child(&db_path, &commit_log, stop_after),
        other => panic!("unknown yfdb6 helper mode: {other}"),
    }
}
