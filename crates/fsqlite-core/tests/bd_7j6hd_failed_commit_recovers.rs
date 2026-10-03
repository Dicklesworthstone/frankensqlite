//! bd-7j6hd: a COMMIT whose group-commit callback fails after its durable
//! mutation started (WAL frames appended, sync or publication failed) leaves
//! the epoch in doubt and the pager retaining RESERVED until reconciliation.
//! That must be a transient state: later writers in the same process, on the
//! same connection or a fresh one, must proceed promptly, and the file must
//! stay consistent for stock SQLite.
//!
//! Requires `--features fault-injection` (the WAL hooks are compiled out
//! otherwise).

#![cfg(feature = "fault-injection")]
// The async engine futures nest deeply; match the other integration suites.
#![recursion_limit = "512"]

use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_wal::fault_hooks::{self, FaultHookArm};

/// Generous: a healthy commit takes milliseconds; the wedge never returns.
const STEP_DEADLINE: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug)]
enum Fault {
    /// The WAL frames (with the commit marker) are written, then fsync fails.
    SyncAfterAppend,
    /// The append itself reports failure after writing every frame.
    AfterAppend,
    /// The append is refused before any frame reaches the WAL.
    AppendBusy,
}

fn arm(fault: Fault) {
    let arm = FaultHookArm::new("bd-7j6hd", format!("{fault:?}"), "failed_commit_recovers");
    match fault {
        Fault::SyncAfterAppend => fault_hooks::arm_sync_failure(arm),
        Fault::AfterAppend => fault_hooks::arm_after_append(arm),
        Fault::AppendBusy => fault_hooks::arm_append_busy_countdown(arm, 1),
    }
}

fn count(rows: &[fsqlite_core::connection::Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("count(*) returned {other:?}"),
    }
}

/// Runs the scenario on its own thread so a wedge fails the test instead of
/// hanging the suite. Returns whether the faulted INSERT reported success and
/// the rows stock SQLite reads afterwards.
fn run_scenario(db: &Path, fault: Fault) -> (bool, Vec<i64>) {
    let path = db.to_str().expect("utf-8").to_owned();
    let (tx, rx) = mpsc::channel::<String>();
    let worker = std::thread::spawn(move || {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&path).await.expect("open");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute("PRAGMA synchronous=FULL").await.expect("sync full");
            conn.execute("CREATE TABLE t(x INTEGER)").await.expect("ddl");
            conn.execute("INSERT INTO t VALUES (1)").await.expect("seed");
            tx.send("seeded".into()).ok();

            fault_hooks::clear();
            arm(fault);
            let failed = conn.execute("INSERT INTO t VALUES (2)").await;
            let fired = fault_hooks::take_records();
            fault_hooks::clear();
            tx.send(format!(
                "faulted: result={failed:?} fired={}",
                fired.len()
            ))
            .ok();
            tx.send(format!("acknowledged={}", failed.is_ok())).ok();
            assert_eq!(fired.len(), 1, "{fault:?} hook must fire exactly once");

            // Same connection, next write.
            conn.execute("INSERT INTO t VALUES (3)")
                .await
                .expect("same-connection write after a failed commit");
            tx.send("same-conn write ok".into()).ok();

            // A fresh connection in the same process.
            let other = Connection::open(&path).await.expect("second open");
            other
                .execute("INSERT INTO t VALUES (4)")
                .await
                .expect("second-connection write after a failed commit");
            tx.send("second-conn write ok".into()).ok();

            // Both connections agree on what is committed.
            let a = count(&conn.query("SELECT count(*) FROM t").await.expect("read a"));
            let b = count(&other.query("SELECT count(*) FROM t").await.expect("read b"));
            assert_eq!(a, b, "connections disagree after recovery");
            tx.send(format!("counts ok: {a}")).ok();
            drop(other);
            drop(conn);
        });
        tx.send("done".into()).ok();
    });

    let mut log = Vec::new();
    loop {
        match rx.recv_timeout(STEP_DEADLINE) {
            Ok(step) if step == "done" => break,
            Ok(step) => log.push(step),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let panic = worker.join().err().map(|payload| {
                    payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                        .unwrap_or_default()
                });
                panic!("{fault:?}: scenario failed after {log:?}: {panic:?}");
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("{fault:?}: WEDGED for {STEP_DEADLINE:?} after steps {log:?}");
            }
        }
    }
    worker.join().expect("scenario thread");
    eprintln!("{fault:?}: {log:?}");
    let acknowledged = log.iter().any(|step| step == "acknowledged=true");

    let stock = rusqlite::Connection::open(db).expect("stock open");
    let check: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(check, "ok", "{fault:?}: stock integrity_check");
    let mut stmt = stock.prepare("SELECT x FROM t ORDER BY x").expect("prepare");
    let rows = stmt
        .query_map([], |row| row.get(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows");
    (acknowledged, rows)
}

fn assert_outcome(fault: Fault, acknowledged: bool, rows: &[i64]) {
    match fault {
        // Nothing reached the WAL, so the flusher's ordinary busy retry
        // commits the row and acknowledges it.
        Fault::AppendBusy => {
            assert!(acknowledged, "{fault:?}: a retried pre-write busy must commit");
            assert_eq!(rows, [1, 2, 3, 4], "{fault:?}: committed rows");
        }
        // The commit marker may be on disk: the caller gets an error, and
        // row 2's fate is reconciliation's durability verdict (present when
        // it proves the marker durable). Every other row is an ordinary
        // acknowledged commit and must be present.
        Fault::SyncAfterAppend | Fault::AfterAppend => {
            assert!(!acknowledged, "{fault:?}: an in-doubt commit must not report success");
            assert!(
                rows == [1, 3, 4] || rows == [1, 2, 3, 4],
                "{fault:?}: unexpected committed rows {rows:?}"
            );
        }
    }
}

#[test]
fn failed_commit_does_not_wedge_later_writers() {
    for fault in [Fault::SyncAfterAppend, Fault::AfterAppend, Fault::AppendBusy] {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("bd_7j6hd.db");
        let (acknowledged, rows) = run_scenario(&db, fault);
        assert_outcome(fault, acknowledged, &rows);
    }
}
