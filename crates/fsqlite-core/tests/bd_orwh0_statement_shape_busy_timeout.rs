//! bd-orwh0 — every autocommit statement shape must honour `busy_timeout`, not
//! just the three that happened to get reported.
//!
//! bd-udetu looked like "BEGIN is broken". It was not. The transients involved
//! are raised by the pager publication bind, which runs *before* the pager
//! admission that owns the `busy_timeout` budget, and every autocommit statement
//! goes through that bind. The autocommit retry loop in
//! `execute_statement_after_background_status` was armed by a statement-shape
//! allowlist, so it covered exactly the shapes someone had complained about:
//! PRAGMA, SELECT, then BEGIN.
//!
//! Measured on the unfixed engine against a `wal_checkpoint(TRUNCATE)` loop with
//! `busy_timeout=4000`, four seconds per shape:
//!
//! ```text
//! CREATE TABLE   14305 ok    8164 refused   min/p50 = 0 ms
//! SAVEPOINT       9810 ok   45492 refused   min/p50 = 0 ms
//! CREATE INDEX       0 ok    7497 refused   min/p50 = 0 ms
//! ANALYZE            0 ok   53879 refused   min/p50 = 0 ms
//! ```
//!
//! `CREATE INDEX` and `ANALYZE` never succeeded once. That is an availability
//! bug, not a latency one: a schema migration against a database with an active
//! checkpointer failed immediately with SQLITE_BUSY and never consulted the
//! timeout the application had set.
//!
//! The fix extends the allowlist to `SAVEPOINT` and `ANALYZE`, and the keeper
//! below guards exactly those. DDL is deliberately NOT covered: arming every
//! shape except transaction completion made `CREATE INDEX` produce a malformed
//! index in 1 of 3 runs, where the unfixed engine was clean in 3 of 3 with more
//! successful `CREATE INDEX`es. That is bd-pa8e5, and it is why this stays an
//! allowlist instead of becoming an exclusion list.
//!
//! The keeper asserts the contract, not a throughput number: a busy returned to
//! the caller must mean the deadline was actually spent. It does not assert that
//! the statement eventually succeeds — against a checkpointer that never pauses,
//! refusing after the full timeout is the correct answer. The threshold is half
//! the timeout, which cannot trip on a slow machine, where waits only grow. The
//! `#[ignore]`d probe beside it prints the whole table, DDL included, for the
//! next person asking this about a shape not listed here.

use fsqlite_core::connection::Connection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BUSY_TIMEOUT_MS: u64 = 4_000;
/// A refusal is only contract-legal if it spent at least this long first.
const MIN_LEGAL_REFUSAL_MS: u128 = (BUSY_TIMEOUT_MS / 2) as u128;

/// One measured statement shape: a label, the SQL, and anything needed to put
/// the connection back where it started.
struct Shape {
    label: &'static str,
    sql: &'static str,
    cleanup: &'static [&'static str],
}

/// How many attempts succeeded, and `(elapsed_ms, message)` for each refusal.
type ShapeOutcome = (u64, Vec<(u128, String)>);

/// The shapes this fix newly covers: measured at 45_492 and 53_879 refusals
/// respectively, all at min / p50 = 0 ms, before the allowlist was extended.
/// DDL is absent on purpose — see bd-pa8e5 and the module docs.
const REGRESSED_SHAPES: &[Shape] = &[
    Shape {
        label: "SAVEPOINT",
        sql: "SAVEPOINT sp",
        cleanup: &["RELEASE sp"],
    },
    Shape {
        label: "ANALYZE",
        sql: "ANALYZE",
        cleanup: &[],
    },
];

/// Every shape worth looking at, for the characterisation probe.
const ALL_SHAPES: &[Shape] = &[
    Shape { label: "SELECT", sql: "SELECT count(*) FROM t", cleanup: &[] },
    Shape { label: "PRAGMA", sql: "PRAGMA user_version", cleanup: &[] },
    Shape { label: "BEGIN CONCURRENT", sql: "BEGIN CONCURRENT", cleanup: &["COMMIT"] },
    Shape { label: "BEGIN IMMEDIATE", sql: "BEGIN IMMEDIATE", cleanup: &["COMMIT"] },
    Shape { label: "INSERT", sql: "INSERT INTO t(v) VALUES('p')", cleanup: &[] },
    Shape { label: "UPDATE", sql: "UPDATE t SET v='u' WHERE id=1", cleanup: &[] },
    Shape { label: "DELETE", sql: "DELETE FROM t WHERE id=-1", cleanup: &[] },
    Shape { label: "CREATE TABLE", sql: "CREATE TABLE IF NOT EXISTS probe_ddl(a)", cleanup: &[] },
    Shape { label: "CREATE INDEX", sql: "CREATE INDEX IF NOT EXISTS probe_ix ON t(v)", cleanup: &[] },
    Shape { label: "SAVEPOINT", sql: "SAVEPOINT sp", cleanup: &["RELEASE sp"] },
    Shape { label: "ANALYZE", sql: "ANALYZE", cleanup: &[] },
];

/// Seed a database big enough that a TRUNCATE checkpoint holds exclusive access
/// for a measurable window rather than completing instantly.
fn seed(path: &str) {
    let path = path.to_owned();
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&path).await.expect("open seed");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("ddl");
        // The index matters: it makes every checkpointer INSERT churn a second
        // b-tree, which is what drives the contention these shapes were losing
        // to. Without it the measurement is far milder and the keeper can pass
        // on an engine that still has the defect.
        conn.execute("CREATE INDEX ix_v ON t(v)").await.expect("index");
        let payload = "d".repeat(1_500);
        for i in 0..1_200 {
            conn.execute(&format!("INSERT INTO t(id,v) VALUES({i},'{payload}')"))
                .await
                .expect("seed insert");
        }
        conn.close().await.expect("close seed");
    });
}

/// Run every `shape` for `secs` against a TRUNCATE-checkpoint loop on another
/// connection, and return each shape's outcome in order.
fn measure(path: &str, shapes: &'static [Shape], secs: u64) -> Vec<ShapeOutcome> {
    let stop = Arc::new(AtomicBool::new(false));
    let checkpointer = {
        let p = path.to_owned();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open checkpointer");
                conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                    .await
                    .expect("busy_timeout");
                while !stop.load(Ordering::Relaxed) {
                    let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;
                    let _ = conn.execute("INSERT INTO t(v) VALUES('c')").await;
                }
                conn.close().await.expect("close checkpointer");
            });
        })
    };

    // `run_test` requires the future to resolve to `()`, so results come back
    // through shared state rather than as a return value.
    let collected: Arc<Mutex<Vec<ShapeOutcome>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let p = path.to_owned();
        let stop = Arc::clone(&stop);
        let collected = Arc::clone(&collected);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&p).await.expect("open prober");
            conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                .await
                .expect("busy_timeout");
            for shape in shapes {
                let deadline = Instant::now() + Duration::from_secs(secs);
                let mut ok = 0u64;
                let mut refusals: Vec<(u128, String)> = Vec::new();
                while Instant::now() < deadline {
                    let started = Instant::now();
                    match conn.execute(shape.sql).await {
                        Ok(_) => {
                            ok += 1;
                            for sql in shape.cleanup {
                                let _ = conn.execute(sql).await;
                            }
                        }
                        Err(error) => {
                            refusals.push((started.elapsed().as_millis(), format!("{error}")));
                        }
                    }
                    // Never leave a transaction open across shapes.
                    if conn.in_transaction() {
                        let _ = conn.execute("ROLLBACK").await;
                    }
                }
                collected.lock().expect("collected").push((ok, refusals));
            }
            stop.store(true, Ordering::Relaxed);
            conn.close().await.expect("close prober");
        });
    }

    stop.store(true, Ordering::Relaxed);
    checkpointer.join().expect("checkpointer thread");
    let outcomes = collected.lock().expect("collected").clone();
    outcomes
}

#[test]
fn every_autocommit_shape_spends_busy_timeout_before_refusing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("orwh0.db");
    let path = path.to_str().expect("utf-8 path").to_owned();
    seed(&path);

    let outcomes = measure(&path, REGRESSED_SHAPES, 3);
    assert_eq!(outcomes.len(), REGRESSED_SHAPES.len(), "one outcome per shape");

    // Report every shape before asserting anything. A keeper that aborts on the
    // first failing shape hides what the others did, and the others are how you
    // tell a busy-timeout problem from something worse.
    for (shape, (ok, refusals)) in REGRESSED_SHAPES.iter().zip(&outcomes) {
        let first = refusals.first().map_or("-", |(_, message)| message.as_str());
        println!(
            "{:<14} ok={ok:<8} refused={:<8} first refusal: {first}",
            shape.label,
            refusals.len()
        );
    }

    for (shape, (ok, refusals)) in REGRESSED_SHAPES.iter().zip(&outcomes) {
        let label = shape.label;
        let instant: Vec<&(u128, String)> = refusals
            .iter()
            .filter(|(ms, _)| *ms < MIN_LEGAL_REFUSAL_MS)
            .collect();
        let fastest: Vec<&(u128, String)> = instant.iter().take(3).copied().collect();
        assert!(
            instant.is_empty(),
            "{label}: {} of {} refusals came back in under {MIN_LEGAL_REFUSAL_MS}ms despite \
             busy_timeout={BUSY_TIMEOUT_MS}ms ({ok} succeeded). A busy returned to the caller \
             must mean the deadline was actually spent. Fastest: {fastest:?}",
            instant.len(),
            refusals.len()
        );
    }
}

#[test]
#[ignore = "bd-orwh0 characterisation probe; prints the whole table, run explicitly"]
fn statement_shape_busy_table() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("orwh0_probe.db");
    let path = path.to_str().expect("utf-8 path").to_owned();
    seed(&path);

    let outcomes = measure(&path, ALL_SHAPES, 4);
    println!(
        "\n{:<20} {:>8} {:>8} {:>7} {:>7} {:>7}  first refusal",
        "shape", "ok", "refused", "min_ms", "p50_ms", "max_ms"
    );
    for (shape, (ok, refusals)) in ALL_SHAPES.iter().zip(&outcomes) {
        let mut latencies: Vec<u128> = refusals.iter().map(|(ms, _)| *ms).collect();
        latencies.sort_unstable();
        let (min, p50, max) = if latencies.is_empty() {
            (0, 0, 0)
        } else {
            (
                latencies[0],
                latencies[latencies.len() / 2],
                latencies[latencies.len() - 1],
            )
        };
        let first = refusals.first().map_or("-", |(_, message)| message.as_str());
        println!(
            "{:<20} {ok:>8} {:>8} {min:>7} {p50:>7} {max:>7}  {first}",
            shape.label,
            refusals.len()
        );
    }
    println!(
        "\nExpected if busy_timeout is honoured everywhere: refused=0, or refusals near \
         {BUSY_TIMEOUT_MS}ms.\nA shape with refusals at ~0ms is a bd-orwh0 instance."
    );
}
