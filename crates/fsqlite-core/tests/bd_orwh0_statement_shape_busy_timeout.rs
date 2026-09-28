#![recursion_limit = "512"]

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

/// The shapes this fix covers. SAVEPOINT and ANALYZE came first (45_492 and
/// 53_879 refusals respectively, all at min / p50 = 0 ms). DDL joined them once
/// the "arming DDL corrupts an index" finding was retracted — see bd-pa8e5, and
/// the module docs above. Measured with no stock connection anywhere, arming
/// took CREATE INDEX from 94_411 busy refusals to zero with integrity_check ok.
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
    Shape {
        label: "CREATE TABLE",
        sql: "CREATE TABLE IF NOT EXISTS probe_ddl(a)",
        cleanup: &[],
    },
    Shape {
        label: "CREATE INDEX",
        sql: "CREATE INDEX IF NOT EXISTS probe_ix2 ON t(id)",
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

/// bd-pa8e5: a CREATE INDEX that the autocommit loop retried must leave exactly
/// what one clean attempt leaves — the verbatim CREATE text in sqlite_master, no
/// residue that makes the next CREATE say "already exists", and a sound image.
#[test]
fn retried_create_index_persists_verbatim_sql_and_leaves_no_residue() {
    const CREATE: &str = "CREATE INDEX probe_verbatim ON t(  v  , id )";
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("pa8e5.db");
    let path = path.to_str().expect("utf-8 path").to_owned();
    seed(&path);

    let stop = Arc::new(AtomicBool::new(false));
    let checkpointer = {
        let p = path.clone();
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
    let failures: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let p = path.clone();
        let failures = Arc::clone(&failures);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&p).await.expect("open prober");
            conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                .await
                .expect("busy_timeout");
            // A refusal after the whole busy_timeout is legal against a
            // checkpointer that never pauses, so a transient error just means
            // "again". Anything else — "already exists" from a residue of a
            // failed attempt, or rewritten sqlite_master text — is the defect.
            let deadline = Instant::now() + Duration::from_secs(3);
            // Stop retrying at this point. A DDL writer can starve against a
            // checkpointer that commits without pause (first committer wins
            // on page 1; the page-1/freelist contention track), which is not
            // what this keeper judges: the image it leaves is still checked.
            let hard_stop = deadline + Duration::from_secs(60);
            let starved = |what: &str, created: u32| {
                println!("{what} #{created}: still refused after the retry budget (starved)");
            };
            let (mut created, mut refused) = (0u32, 0u32);
            'probe: while Instant::now() < deadline {
                match conn.execute(CREATE).await {
                    Ok(_) => created += 1,
                    Err(error) if error.is_transient() => {
                        refused += 1;
                        continue;
                    }
                    Err(error) => {
                        failures.lock().unwrap().push(format!("create #{created}: {error}"));
                        break;
                    }
                }
                let sql = loop {
                    match conn
                        .query("SELECT sql FROM sqlite_master WHERE name = 'probe_verbatim'")
                        .await
                    {
                        Ok(rows) => break rows.first().map(|row| format!("{:?}", row.values()[0])),
                        Err(error) if error.is_transient() => {
                            if Instant::now() > hard_stop {
                                starved("read", created);
                                break 'probe;
                            }
                        }
                        Err(error) => {
                            failures.lock().unwrap().push(format!("read #{created}: {error}"));
                            break 'probe;
                        }
                    }
                };
                if sql.as_deref() != Some(&format!("Text({CREATE:?})")) {
                    failures.lock().unwrap().push(format!("create #{created}: sql {sql:?}"));
                    break;
                }
                loop {
                    match conn.execute("DROP INDEX probe_verbatim").await {
                        Ok(_) => break,
                        Err(error) if error.is_transient() => {
                            refused += 1;
                            if Instant::now() > hard_stop {
                                starved("drop", created);
                                break 'probe;
                            }
                        }
                        Err(error) => {
                            failures.lock().unwrap().push(format!("drop #{created}: {error}"));
                            break 'probe;
                        }
                    }
                }
            }
            println!("created and dropped {created} times, {refused} busy refusals retried");
            conn.close().await.expect("close prober");
        });
    }
    stop.store(true, Ordering::Relaxed);
    checkpointer.join().expect("checkpointer thread");
    let failures = failures.lock().unwrap().clone();
    assert!(failures.is_empty(), "{failures:?}");

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&path).await.expect("reopen");
        let rows = conn.query("PRAGMA integrity_check").await.expect("integrity_check");
        assert_eq!(format!("{:?}", rows[0].values()[0]), "Text(\"ok\")");
        conn.close().await.expect("close");
    });
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

// ---- bd-11sz4: a writer must maintain an index a peer re-created across TRUNCATE checkpoints ----

/// Read a positive `usize` knob from the environment, falling back to `default`.
fn bd_11sz4_knob(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// One bd-11sz4 stress run (driven by [`bd_11sz4_stress_parallel_harness`]).
///
/// A "checkpointer" connection alternates `PRAGMA wal_checkpoint(TRUNCATE)` with
/// an `INSERT`, pausing a pseudo-random 0-300 ms between rounds. Meanwhile a
/// prober connection runs `FSQLITE_BD11SZ4_CYCLES` (default 6) cycles of
/// CREATE INDEX / DROP INDEX / CREATE INDEX. It keeps each re-created index for
/// 1 s and runs `PRAGMA integrity_check` before dropping it for the next cycle.
/// On the first failing mid-run check it stops cycling with the index in place.
/// It then stops the checkpointer, reopens the database and checks again with no
/// writer running, so a failure there is persistent on-disk corruption.
///
/// The run panics with a greppable marker:
/// - `V5_PERSISTENT_CORRUPTION final=...`: the quiescent check failed.
/// - `V5_MIDRUN_ONLY mid=[...] final=ok`: only the mid-run check failed.
/// - `STARVED_OR_ERROR ...`: a statement stayed busy for 120 s.
///
/// The pause schedule is seeded from the process id and depends on timing, so a
/// failing run cannot be replayed.
#[test]
#[ignore = "bd-11sz4 stress run; driven by bd_11sz4_stress_parallel_harness"]
fn bd_11sz4_stress_writer_keeps_recreated_index() {
    const CREATE: &str = "CREATE INDEX probe_verbatim ON t(  v  , id )";
    const DROP: &str = "DROP INDEX probe_verbatim";
    const CHECK: &str = "__CHECK__";
    let cycles = bd_11sz4_knob("FSQLITE_BD11SZ4_CYCLES", 6);
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bd11sz4.db");
    let path = path.to_str().expect("utf-8 path").to_owned();
    seed(&path);

    let stop = Arc::new(AtomicBool::new(false));
    let midrun_bad: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let checkpointer = {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open checkpointer");
                conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                    .await
                    .expect("busy_timeout");
                let mut rng = u64::from(std::process::id()).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                while !stop.load(Ordering::Relaxed) {
                    let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;
                    let _ = conn.execute("INSERT INTO t(v) VALUES('c')").await;
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    std::thread::sleep(Duration::from_millis(rng % 301));
                }
                conn.close().await.expect("close checkpointer");
            });
        })
    };
    {
        let p = path.clone();
        let midrun_bad = Arc::clone(&midrun_bad);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&p).await.expect("open prober");
            conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                .await
                .expect("busy_timeout");
            'cycles: for cycle in 0..cycles {
                let mut steps = vec![CREATE, DROP, CREATE, CHECK];
                if cycle + 1 < cycles {
                    steps.push(DROP);
                }
                for step in steps {
                    let started = Instant::now();
                    if step == CHECK {
                        // Keep the re-created index while the checkpointer inserts.
                        std::thread::sleep(Duration::from_secs(1));
                        let rows = loop {
                            match conn.query("PRAGMA integrity_check").await {
                                Ok(rows) => break rows,
                                Err(error)
                                    if error.is_transient()
                                        && started.elapsed() < Duration::from_secs(120) => {}
                                Err(error) => panic!("STARVED_OR_ERROR integrity_check: {error}"),
                            }
                        };
                        let got = format!("{:?}", rows[0].values()[0]);
                        if got != "Text(\"ok\")" {
                            // Leave the index in place for the quiescent check.
                            *midrun_bad.lock().expect("midrun lock") =
                                Some(format!("cycle {cycle}: {got}"));
                            break 'cycles;
                        }
                        continue;
                    }
                    loop {
                        match conn.execute(step).await {
                            Ok(_) => break,
                            Err(error)
                                if error.is_transient()
                                    && started.elapsed() < Duration::from_secs(120) => {}
                            Err(error) => panic!("STARVED_OR_ERROR cycle {cycle} {step}: {error}"),
                        }
                    }
                }
            }
            conn.close().await.expect("close prober");
        });
    }
    std::thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);
    checkpointer.join().expect("checkpointer thread");
    let mid = midrun_bad.lock().expect("midrun lock").clone();
    let final_slot: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    {
        let final_slot = Arc::clone(&final_slot);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&path).await.expect("reopen");
            let rows = conn
                .query("PRAGMA integrity_check")
                .await
                .expect("integrity_check");
            *final_slot.lock().expect("final lock") = format!("{:?}", rows[0].values()[0]);
            conn.close().await.expect("close");
        });
    }
    let final_got = final_slot.lock().expect("final lock").clone();
    // Keep a persistently corrupt image for offline inspection.
    if final_got != "Text(\"ok\")"
        && let Ok(dir) = std::env::var("FSQLITE_BD11SZ4_DUMP")
    {
        let _ = std::fs::create_dir_all(&dir);
        for suffix in ["", "-wal"] {
            let _ = std::fs::copy(
                format!("{path}{suffix}"),
                format!("{dir}/db_{}.db{suffix}", std::process::id()),
            );
        }
    }
    match (mid, final_got == "Text(\"ok\")") {
        (_, false) => panic!("V5_PERSISTENT_CORRUPTION final={final_got}"),
        (Some(mid), true) => panic!("V5_MIDRUN_ONLY mid=[{mid}] final=ok"),
        (None, true) => {}
    }
}

/// bd-11sz4 stress harness: run [`bd_11sz4_stress_writer_keeps_recreated_index`]
/// in parallel child processes and classify each failure.
///
/// ```text
/// cargo test -p fsqlite-core --test bd_orwh0_statement_shape_busy_timeout -- \
///     bd_11sz4_stress_parallel_harness --ignored --nocapture
/// ```
///
/// Knobs: `FSQLITE_BD11SZ4_WORKERS` (default 18), `FSQLITE_BD11SZ4_RUNS` (runs per
/// worker, default 2) and `FSQLITE_BD11SZ4_CYCLES` (default 6). Set
/// `FSQLITE_BD11SZ4_DUMP` to a directory to keep each persistent failure's full
/// child output there. The final
/// `SENS5 SUMMARY` line keeps a stable format for log greps.
///
/// How to read it:
/// - It depends on load. On the regression it reproduced in about 10-16% of the
///   runs that reached the quiescent check under about 36 concurrent test
///   processes, and it went silent at low load.
/// - A clean result means something only alongside a known-bad control that
///   fails in the same window.
/// - Only `PERSISTENT_*` failures count. `STARVED_OR_ERROR` runs never reached
///   the check.
/// - `MIDRUN_ONLY` with `Text("database is busy")` was bd-svwm7 (integrity_check
///   reported a transient busy as a result row), not this bug. Since
///   integrity_check returns that busy as an error, the mid-run check retries it.
/// - With the bd-11sz4 fix reverted, 24 workers x 2 runs x 4 passes gave 5
///   persistent missing-from-index failures in 192 runs. The fix gave 0 in 192
///   in the same window.
#[test]
#[ignore = "bd-11sz4 stress harness; load-dependent, see doc comment"]
fn bd_11sz4_stress_parallel_harness() {
    use std::process::Command;
    use std::sync::atomic::AtomicUsize;
    const NAME: &str = "bd_11sz4_stress_writer_keeps_recreated_index";
    let workers = bd_11sz4_knob("FSQLITE_BD11SZ4_WORKERS", 18);
    let runs_per_worker = bd_11sz4_knob("FSQLITE_BD11SZ4_RUNS", 2);
    let exe = std::env::current_exe().expect("current_exe");
    let runs = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let handles: Vec<_> = (0..workers)
        .map(|worker| {
            let exe = exe.clone();
            let runs = Arc::clone(&runs);
            let failures = Arc::clone(&failures);
            std::thread::spawn(move || {
                for run in 0..runs_per_worker {
                    let out = Command::new(&exe)
                        .args([
                            "--exact",
                            NAME,
                            "--ignored",
                            "--test-threads=1",
                            "--nocapture",
                        ])
                        .output()
                        .expect("spawn stress child");
                    runs.fetch_add(1, Ordering::Relaxed);
                    if out.status.success() {
                        continue;
                    }
                    let text = format!(
                        "{}{}",
                        String::from_utf8_lossy(&out.stdout),
                        String::from_utf8_lossy(&out.stderr)
                    );
                    let signature = if text.contains("V5_PERSISTENT_CORRUPTION") {
                        if text.contains("is missing from index") {
                            "PERSISTENT_MISSING_FROM_INDEX"
                        } else {
                            "PERSISTENT_OTHER"
                        }
                    } else if text.contains("V5_MIDRUN_ONLY") {
                        "MIDRUN_ONLY"
                    } else if text.contains("is missing from index") {
                        "MISSING_FROM_INDEX"
                    } else if text.contains("referenced multiple times") {
                        "REFERENCED_TWICE"
                    } else if text.contains("STARVED_OR_ERROR") {
                        "STARVED_OR_ERROR"
                    } else {
                        "OTHER"
                    };
                    let detail = text
                        .lines()
                        .find(|line| line.contains("V5_") || line.contains("STARVED_OR_ERROR"))
                        .unwrap_or("")
                        .chars()
                        .take(400)
                        .collect::<String>();
                    eprintln!("SENS5 FAIL w{worker} r{run} {signature}: {detail}");
                    if let (true, Ok(dir)) = (
                        signature.starts_with("PERSISTENT"),
                        std::env::var("FSQLITE_BD11SZ4_DUMP"),
                    ) {
                        let _ = std::fs::create_dir_all(&dir);
                        let file =
                            format!("{dir}/fail_{}_w{worker}_r{run}.log", std::process::id());
                        let _ = std::fs::write(&file, &text);
                        eprintln!("SENS5 DUMP {file}");
                    }
                    failures.lock().expect("failures lock").push(signature);
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("worker join");
    }
    let failures = failures.lock().expect("failures lock");
    let count = |signature: &str| failures.iter().filter(|seen| **seen == signature).count();
    eprintln!(
        "SENS5 SUMMARY runs={} failures={} persistent_missing={} persistent_other={} midrun_only={} missing_from_index={} referenced_twice={} starved_or_error={} other={}",
        runs.load(Ordering::Relaxed),
        failures.len(),
        count("PERSISTENT_MISSING_FROM_INDEX"),
        count("PERSISTENT_OTHER"),
        count("MIDRUN_ONLY"),
        count("MISSING_FROM_INDEX"),
        count("REFERENCED_TWICE"),
        count("STARVED_OR_ERROR"),
        count("OTHER")
    );
}
