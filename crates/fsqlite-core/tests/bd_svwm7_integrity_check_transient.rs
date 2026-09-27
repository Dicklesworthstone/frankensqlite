#![recursion_limit = "512"]

//! bd-svwm7 — `PRAGMA integrity_check` and `PRAGMA quick_check` must surface a
//! transient lock conflict as an error (stock SQLite answers SQLITE_BUSY), never
//! as a verdict row. A caller that follows the convention "first row is `ok`, or
//! the database is damaged" otherwise reads a lock conflict as corruption.
//!
//! The checker runs both pragmas back to back against a peer that commits and
//! runs `wal_checkpoint(TRUNCATE)` without pause. Every outcome must be either
//! verdict rows free of transient-error text, or a transient error.

use fsqlite_core::connection::Connection;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn seed(path: &str) {
    let path = path.to_owned();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&path).await.expect("open seed");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("ddl");
        conn.execute("CREATE INDEX ix_v ON t(v)").await.expect("index");
        let payload = "d".repeat(600);
        for i in 0..400 {
            conn.execute(&format!("INSERT INTO t(id, v) VALUES({i}, '{payload}')"))
                .await
                .expect("seed insert");
        }
        conn.close().await.expect("close seed");
    });
}

/// Text a lock conflict renders as; it must never appear as a verdict row.
fn is_transient_text(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    text.contains("database is busy")
        || text.contains("database is locked")
        || text.contains("snapshot conflict")
}

#[test]
fn integrity_check_reports_a_transient_conflict_as_an_error_not_a_verdict() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("svwm7.db");
    let path = path.to_str().expect("utf-8 path").to_owned();
    seed(&path);

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let path = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&path).await.expect("open writer");
                while !stop.load(Ordering::Relaxed) {
                    let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;
                    let _ = conn.execute("INSERT INTO t(v) VALUES('w')").await;
                }
                conn.close().await.expect("close writer");
            });
        })
    };

    let checks = Arc::new(AtomicUsize::new(0));
    let transient_errors = Arc::new(AtomicUsize::new(0));
    let violation_count = Arc::new(AtomicUsize::new(0));
    let violations: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let violation_count = Arc::clone(&violation_count);
        let path = path.clone();
        let checks = Arc::clone(&checks);
        let transient_errors = Arc::clone(&transient_errors);
        let violations = Arc::clone(&violations);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&path).await.expect("open checker");
            // No waiting: expose every conflict the pragma meets.
            conn.execute("PRAGMA busy_timeout=0").await.expect("busy_timeout");
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                for pragma in ["PRAGMA integrity_check", "PRAGMA quick_check"] {
                    checks.fetch_add(1, Ordering::Relaxed);
                    match conn.query(pragma).await {
                        Ok(rows) => {
                            for row in &rows {
                                let text = format!("{:?}", row.values()[0]);
                                if is_transient_text(&text) {
                                    violation_count.fetch_add(1, Ordering::Relaxed);
                                    let mut kept = violations.lock().unwrap();
                                    if kept.len() < 10 {
                                        kept.push(format!("{pragma}: verdict row {text}"));
                                    }
                                }
                            }
                        }
                        Err(error) if error.is_transient() => {
                            transient_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => {
                            violation_count.fetch_add(1, Ordering::Relaxed);
                            let mut kept = violations.lock().unwrap();
                            if kept.len() < 10 {
                                kept.push(format!("{pragma}: non-transient error {error}"));
                            }
                        }
                    }
                }
            }
            conn.close().await.expect("close checker");
        });
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread");
    let checks = checks.load(Ordering::Relaxed);
    let transient_errors = transient_errors.load(Ordering::Relaxed);
    let violation_count = violation_count.load(Ordering::Relaxed);
    let first = violations.lock().unwrap().clone();
    println!(
        "bd-svwm7 keeper: {checks} checks, {transient_errors} transient errors, {violation_count} violations"
    );
    assert!(checks >= 10, "the checker must actually run ({checks} checks)");
    assert!(
        violation_count == 0,
        "{violation_count} violation(s) in {checks} checks; first {}: {first:#?}",
        first.len()
    );
}
