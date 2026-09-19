//! bd-udetu — is `PRAGMA busy_timeout` honoured on the `BEGIN CONCURRENT` path?
//!
//! The ioq6x livelock shows four writers, each with `busy_timeout=10000`, taking
//! 5001 `BEGIN CONCURRENT` attempts inside a 340-second run — about 68 ms per
//! attempt. A ten-second timeout cannot be in effect. That arithmetic is
//! suggestive but indirect, so this measures it head on.
//!
//! Shape: connection A takes an exclusive write lock and holds it; connection B,
//! with `busy_timeout=10000`, then tries to begin. If the timeout is honoured, B
//! blocks for roughly ten seconds before reporting busy. If it is not, B returns
//! almost immediately — which is what turns a zero-backoff retry loop into a spin.
//!
//! `BEGIN IMMEDIATE` is measured alongside as the control, because
//! `lifecycle.rs` does build a deadline from `busy_timeout_ms` for that route.
//! A large gap between the two is the finding.
//!
//! This is a characterisation probe: it prints timings rather than asserting a
//! threshold, so it cannot go red on a slow machine. Run it explicitly:
//!   cargo test -p fsqlite-core --test bd_udetu_busy_timeout_probe -- --ignored --nocapture

use fsqlite_core::connection::Connection;
use std::time::Instant;

const BUSY_TIMEOUT_MS: u128 = 10_000;

async fn open_writer(path: &str) -> Connection {
    let conn = Connection::open(path).await.expect("open");
    conn.execute("PRAGMA busy_timeout=10000")
        .await
        .expect("busy_timeout must apply");
    conn
}

/// Time how long `sql` takes on `conn`, and report what it returned.
async fn timed(conn: &Connection, sql: &str) -> (u128, Result<(), String>) {
    let started = Instant::now();
    let outcome = conn
        .execute(sql)
        .await
        .map(|_| ())
        .map_err(|error| format!("{error}"));
    (started.elapsed().as_millis(), outcome)
}

#[test]
#[ignore = "bd-udetu characterisation probe; prints timings, run explicitly"]
fn busy_timeout_is_honoured_on_begin_concurrent() {
    asupersync::test_utils::run_test(|| async {
        for blocker in ["BEGIN IMMEDIATE", "BEGIN CONCURRENT"] {
            for contender in ["BEGIN IMMEDIATE", "BEGIN CONCURRENT"] {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().join("udetu.db");
                let path = path.to_str().expect("utf-8 path").to_owned();

                let a = open_writer(&path).await;
                a.execute("PRAGMA journal_mode=WAL").await.expect("wal");
                a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
                    .await
                    .expect("ddl");

                // A takes a write lock and holds it for the whole measurement.
                let (_, began) = timed(&a, blocker).await;
                if began.is_err() {
                    println!("blocker={blocker:16} could not begin: {began:?} -- skipping");
                    continue;
                }
                let _ = a.execute("INSERT INTO t(v) VALUES(1)").await;

                let b = open_writer(&path).await;
                let (elapsed_ms, outcome) = timed(&b, contender).await;

                let verdict = match (&outcome, elapsed_ms) {
                    (Ok(()), _) => "PROCEEDED (no contention)".to_owned(),
                    (Err(_), ms) if ms >= BUSY_TIMEOUT_MS * 8 / 10 => {
                        "waited ~busy_timeout -> HONOURED".to_owned()
                    }
                    (Err(_), ms) if ms < 1_000 => {
                        format!("returned after {ms}ms -> busy_timeout NOT honoured")
                    }
                    (Err(_), ms) => format!("returned after {ms}ms -> partial/unclear"),
                };
                println!(
                    "blocker={blocker:16} contender={contender:16} elapsed={elapsed_ms:>6}ms  {verdict}"
                );
                if let Err(error) = &outcome {
                    println!("    error: {error}");
                }

                let _ = a.execute("ROLLBACK").await;
                let _ = b.execute("ROLLBACK").await;
                a.close().await.expect("close a");
                b.close().await.expect("close b");
            }
        }
        println!(
            "\nExpected if busy_timeout works: every contended row shows elapsed near {BUSY_TIMEOUT_MS}ms.\n\
             A contended row returning in tens of milliseconds is the bd-udetu defect."
        );
    });
}
