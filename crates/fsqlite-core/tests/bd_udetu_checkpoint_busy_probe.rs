//! bd-udetu — where does the instant `database is busy` come from?
//!
//! The previous probe established two things: `busy_timeout` IS honoured (an
//! `IMMEDIATE` contender waited 10002 ms against a 10000 ms setting), and
//! `BEGIN CONCURRENT` does not block on a competing writer at all. Yet the ioq6x
//! writers receive `database is busy` from `BEGIN CONCURRENT` roughly every 68 ms
//! with `busy_timeout=10000` applied. So that busy must arrive by a path that
//! returns immediately rather than waiting.
//!
//! The suspect is the checkpoint. ioq6x runs `wal_checkpoint(TRUNCATE)` between
//! phases — it needs exclusive access, and the writers resume right around it.
//! This puts a checkpointer in a loop on one connection and a `BEGIN CONCURRENT`
//! writer in a loop on another, and records the elapsed time of every attempt
//! that comes back busy.
//!
//! The finding, if it reproduces, is any busy returning in tens of milliseconds
//! while `busy_timeout` is 10000 — i.e. a busy that never consulted the deadline.
//!
//! Characterisation probe: prints a distribution, asserts nothing about timing so
//! it cannot go red on a slow machine.
//!   cargo test -p fsqlite-core --test bd_udetu_checkpoint_busy_probe -- --ignored --nocapture

use fsqlite_core::connection::Connection;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

const RUN_SECS: u64 = 25;

#[test]
#[ignore = "bd-udetu characterisation probe; prints a busy-latency distribution, run explicitly"]
fn checkpoint_contention_busy_latency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("udetu_ckpt.db");
    let path = path.to_str().expect("utf-8").to_owned();

    // Seed a database with enough data that a TRUNCATE checkpoint takes real time.
    {
        let p = path.clone();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&p).await.expect("open seed");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute("PRAGMA busy_timeout=10000").await.expect("bt");
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .await
                .expect("ddl");
            let payload = "d".repeat(1500);
            for i in 0..1500 {
                conn.execute(&format!("INSERT INTO t(id,v) VALUES({i},'{payload}')"))
                    .await
                    .expect("seed insert");
            }
            conn.close().await.expect("close seed");
        });
    }

    let stop = Arc::new(AtomicBool::new(false));

    // Checkpointer: hammer TRUNCATE checkpoints, which need exclusive access.
    let checkpointer = {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open ckpt");
                conn.execute("PRAGMA busy_timeout=10000").await.expect("bt");
                let mut done = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await.is_ok() {
                        done += 1;
                    }
                    let _ = conn.execute("INSERT INTO t(v) VALUES('x')").await;
                }
                println!("checkpointer: {done} TRUNCATE checkpoints completed");
                conn.close().await.expect("close ckpt");
            });
        })
    };

    // Writer: BEGIN CONCURRENT in a loop, timing every attempt.
    let writer = {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open writer");
                conn.execute("PRAGMA busy_timeout=10000").await.expect("bt");
                let deadline = Instant::now() + std::time::Duration::from_secs(RUN_SECS);
                let (mut ok, mut busy) = (0u64, 0u64);
                let mut busy_ms: Vec<u128> = Vec::new();
                let mut first_error = String::new();
                while Instant::now() < deadline {
                    let started = Instant::now();
                    match conn.execute("BEGIN CONCURRENT").await {
                        Ok(_) => {
                            ok += 1;
                            let _ = conn.execute("INSERT INTO t(v) VALUES('w')").await;
                            let _ = conn.execute("COMMIT").await;
                        }
                        Err(error) => {
                            busy += 1;
                            busy_ms.push(started.elapsed().as_millis());
                            if first_error.is_empty() {
                                first_error = format!("{error}");
                            }
                            if conn.in_transaction() {
                                let _ = conn.execute("ROLLBACK").await;
                            }
                        }
                    }
                }
                stop.store(true, Ordering::Relaxed);
                println!("\nwriter: {ok} began OK, {busy} refused");
                if !first_error.is_empty() {
                    println!("writer: first refusal: {first_error}");
                }
                if !busy_ms.is_empty() {
                    busy_ms.sort_unstable();
                    let n = busy_ms.len();
                    let sum: u128 = busy_ms.iter().sum();
                    println!(
                        "writer: refusal latency ms -> min={} p50={} p90={} max={} mean={}",
                        busy_ms[0],
                        busy_ms[n / 2],
                        busy_ms[n * 9 / 10],
                        busy_ms[n - 1],
                        sum / n as u128
                    );
                    let instant = busy_ms.iter().filter(|ms| **ms < 1_000).count();
                    println!(
                        "writer: {instant} of {n} refusals returned in under 1s despite busy_timeout=10000"
                    );
                    println!(
                        "VERDICT: {}",
                        if instant > 0 {
                            "at least one busy bypassed the busy_timeout deadline -- bd-udetu reproduced"
                        } else {
                            "every refusal waited out the timeout -- checkpoint is NOT the instant-busy source"
                        }
                    );
                } else {
                    println!("VERDICT: no refusals at all -- checkpoint contention did not reproduce it");
                }
                conn.close().await.expect("close writer");
            });
        })
    };

    writer.join().expect("writer thread");
    stop.store(true, Ordering::Relaxed);
    checkpointer.join().expect("checkpointer thread");
}
