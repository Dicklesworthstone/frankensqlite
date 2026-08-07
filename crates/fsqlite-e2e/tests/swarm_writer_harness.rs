//! Swarm-writer test harness for the multi-process concurrency contract.
//!
//! Issue: <https://github.com/Dicklesworthstone/frankensqlite/issues/79>
//! Contract: [`docs/concurrency-contract.md`].
//! Umbrella tracking: <https://github.com/Dicklesworthstone/frankensqlite/issues/70>.
//!
//! This test exercises the workload shape from #79:
//!
//! ```text
//! N = configurable, default 16
//! duration = configurable, default 30s in CI / 1h on demand
//! process lifetime = uniform [50ms, 500ms]
//! operations per process = uniform [1, 8] from {INSERT, UPDATE, SELECT_BY_PK, SELECT_RANGE}
//! key space = small enough that overlap is the common case (e.g. 0..1000)
//! ```
//!
//! The test is `#[ignore]`-gated. Run on demand via:
//!
//! ```sh
//! cargo test --profile release-perf -p fsqlite-e2e --test swarm_writer_harness \
//!     -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Knobs (env vars, all optional):
//!   - `FSQLITE_SWARM_WORKERS` (default 16)
//!   - `FSQLITE_SWARM_SECONDS` (default 30)
//!   - `FSQLITE_SWARM_KEYSPACE` (default 1000)
//!   - `FSQLITE_SWARM_BUSY_TIMEOUT_MS` (default 5000)
//!   - `FSQLITE_SWARM_SEED` (default 0xC0FF_EEC0_DEC0_FFEE)
//!   - `FSQLITE_SWARM_BACKEND` = `fsqlite` | `stock` | `both` (default `both`)
//!
//! Each child writes a per-process audit log (one JSON record per
//! committed transaction or asserted observation) to the shared run
//! directory. The parent collects them and verifies the seven
//! acceptance assertions from #79 against fsqlite — and against stock
//! SQLite, as a differential oracle.
//!
//! ## Why this test will likely fail today
//!
//! Issue #79 is explicit: this harness exists *to expose* the
//! corrupt-under-multi-process bugs cataloged under #70. Each known-
//! failing assertion carries an inline reference to the bug shape it
//! covers (`beads_rust#252`, `frankensqlite#56`, etc). DO NOT silence
//! a failing assertion — file an engineering bead under #70 instead.

#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]
#![allow(clippy::similar_names)]

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fsqlite::{Connection, FrankenError, SqliteValue};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Marker env var: when set on a child process, the test binary runs
/// as a swarm worker instead of running the test framework.
const CHILD_MARKER: &str = "FSQLITE_SWARM_CHILD";

/// Backend selector marker; one of `fsqlite` or `stock`.
const CHILD_BACKEND: &str = "FSQLITE_SWARM_CHILD_BACKEND";

/// Default workload configuration (issue #79 spec).
const DEFAULT_WORKERS: usize = 16;
const DEFAULT_SECONDS: u64 = 30;
const DEFAULT_KEYSPACE: i64 = 1_000;
const DEFAULT_BUSY_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_SEED: u64 = 0xC0FF_EEC0_DEC0_FFEE;

/// Per-process lifetime distribution, milliseconds.
const PROC_LIFETIME_MIN_MS: u64 = 50;
const PROC_LIFETIME_MAX_MS: u64 = 500;

/// Per-process operation count distribution.
const PROC_OPS_MIN: u32 = 1;
const PROC_OPS_MAX: u32 = 8;

/// Children get this many milliseconds beyond their requested lifetime
/// to finish committing and exit before the parent kills them.
const CHILD_GRACE_MS: u64 = 30_000;

/// Inline op codes.
#[derive(Debug, Clone, Copy)]
enum Op {
    Insert,
    Update,
    SelectByPk,
    SelectRange,
}

/// Audit-log record format. One line per record, JSON.
#[allow(clippy::too_many_arguments)]
fn audit_line(
    backend: &str,
    pid: u32,
    op: &str,
    key: i64,
    value: Option<&str>,
    expected: Option<&str>,
    observed: Option<&str>,
    latency_us: u128,
    err: Option<&str>,
) -> String {
    // We avoid serde_json to keep the test self-contained.
    let mut s = String::with_capacity(192);
    s.push('{');
    let _ = std::fmt::Write::write_fmt(
        &mut s,
        format_args!(
            r#""backend":"{backend}","pid":{pid},"op":"{op}","key":{key},"latency_us":{latency_us}"#
        ),
    );
    if let Some(v) = value {
        s.push_str(r#","value":""#);
        json_escape(&mut s, v);
        s.push('"');
    }
    if let Some(v) = expected {
        s.push_str(r#","expected":""#);
        json_escape(&mut s, v);
        s.push('"');
    }
    if let Some(v) = observed {
        s.push_str(r#","observed":""#);
        json_escape(&mut s, v);
        s.push('"');
    }
    if let Some(e) = err {
        s.push_str(r#","err":""#);
        json_escape(&mut s, e);
        s.push('"');
    }
    s.push('}');
    s.push('\n');
    s
}

fn json_escape(out: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = std::fmt::Write::write_fmt(out, format_args!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
}

#[derive(Debug, Clone)]
struct RunConfig {
    workers: usize,
    seconds: u64,
    keyspace: i64,
    busy_timeout_ms: u64,
    seed: u64,
    /// Backend the parent is currently exercising. The child reads it
    /// from the env so we don't need argv parsing in the test binary.
    backend: Backend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    Fsqlite,
    Stock,
}

impl Backend {
    fn as_str(self) -> &'static str {
        match self {
            Self::Fsqlite => "fsqlite",
            Self::Stock => "stock",
        }
    }
    fn from_env() -> Option<Self> {
        match env::var(CHILD_BACKEND).ok().as_deref() {
            Some("fsqlite") => Some(Self::Fsqlite),
            Some("stock") => Some(Self::Stock),
            _ => None,
        }
    }
}

fn cfg_from_env(backend: Backend) -> RunConfig {
    fn env_u64(k: &str, dflt: u64) -> u64 {
        env::var(k)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(dflt)
    }
    fn env_usize(k: &str, dflt: usize) -> usize {
        env::var(k)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(dflt)
    }
    fn env_i64(k: &str, dflt: i64) -> i64 {
        env::var(k)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(dflt)
    }
    RunConfig {
        workers: env_usize("FSQLITE_SWARM_WORKERS", DEFAULT_WORKERS),
        seconds: env_u64("FSQLITE_SWARM_SECONDS", DEFAULT_SECONDS),
        keyspace: env_i64("FSQLITE_SWARM_KEYSPACE", DEFAULT_KEYSPACE),
        busy_timeout_ms: env_u64("FSQLITE_SWARM_BUSY_TIMEOUT_MS", DEFAULT_BUSY_TIMEOUT_MS),
        seed: env_u64("FSQLITE_SWARM_SEED", DEFAULT_SEED),
        backend,
    }
}

fn parent_test_root() -> PathBuf {
    let base = env::var("FSQLITE_SWARM_RUN_DIR").map_or_else(|_| env::temp_dir(), PathBuf::from);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    base.join(format!(
        "fsqlite-swarm-harness-{stamp}-pid{}",
        std::process::id()
    ))
}

/// Top-level test entry point: orchestrates fsqlite + stock-sqlite
/// runs and emits per-criterion pass/fail diagnostics.
#[test]
#[ignore = "long-running multi-process harness; run with --ignored"]
fn swarm_writer_harness() {
    // If we are running as a child worker (re-spawned via current_exe),
    // skip the test-framework path and do worker work instead.
    if let Some(backend) = Backend::from_env() {
        run_as_child(backend);
        return;
    }

    asupersync::test_utils::run_test(|| async {
        let backends = match env::var("FSQLITE_SWARM_BACKEND").ok().as_deref() {
            Some("fsqlite") => vec![Backend::Fsqlite],
            Some("stock") => vec![Backend::Stock],
            _ => vec![Backend::Fsqlite, Backend::Stock],
        };

        let mut total_failures = 0_usize;
        for backend in backends {
            let cfg = cfg_from_env(backend);
            let run_root = parent_test_root().join(backend.as_str());
            fs::create_dir_all(&run_root).expect("create run root");
            eprintln!(
                "[swarm-harness] backend={} workers={} seconds={} keyspace=0..{} \
                 busy_timeout_ms={} seed={:#x} run_dir={}",
                backend.as_str(),
                cfg.workers,
                cfg.seconds,
                cfg.keyspace,
                cfg.busy_timeout_ms,
                cfg.seed,
                run_root.display()
            );
            let report = run_parent(&cfg, &run_root).await;
            for c in &report.criteria {
                eprintln!(
                    "[swarm-harness][{}] {} {}",
                    backend.as_str(),
                    if c.pass { "PASS" } else { "FAIL" },
                    c.name
                );
                if !c.pass {
                    total_failures += 1;
                    eprintln!("    detail: {}", c.detail);
                }
            }
        }

        // The harness is a regression net for #70 — known-failing assertions
        // are EXPECTED today. We surface failures as the test failure so
        // future fixes accrete to a real net.
        assert!(
            total_failures == 0,
            "swarm_writer_harness saw {total_failures} failed criteria — see stderr above. \
             Each failure corresponds to a #70 surface. DO NOT silence; \
             file an engineering bead instead."
        );
    });
}

// =====================================================================
// Parent process
// =====================================================================

#[derive(Debug)]
struct CriterionReport {
    name: &'static str,
    pass: bool,
    detail: String,
}

#[derive(Debug)]
struct ParentReport {
    criteria: Vec<CriterionReport>,
}

async fn run_parent(cfg: &RunConfig, run_dir: &Path) -> ParentReport {
    let db_path = run_dir.join("swarm.db");
    let audit_dir = run_dir.join("audit");
    fs::create_dir_all(&audit_dir).expect("audit dir");

    initialize_database(cfg, &db_path).await;

    // Run sequential opens to verify Connection::open semantics across
    // many short-lived processes (assertion #6). Spawn N opens.
    let open_check = run_sequential_open_check(cfg, run_dir).await;

    // Spawn the swarm.
    let mut criteria: Vec<CriterionReport> = Vec::new();
    criteria.push(open_check);

    let workers_failure = spawn_and_collect_workers(cfg, &db_path, &audit_dir);
    criteria.push(workers_failure);

    // Acceptance assertions 1 & 2: integrity + WAL shape.
    criteria.push(assert_integrity_passes(cfg, &db_path));
    criteria.push(assert_wal_shape_clean(&db_path));

    // VACUUM INTO copy + integrity check (per #79).
    criteria.push(assert_vacuum_into_passes(cfg, &db_path, run_dir));

    // Audit-log analysis: assertions 3, 4, 5, 7.
    let audit_records = collect_audit_records(&audit_dir);
    criteria.push(assert_no_silent_zero_row(cfg, &db_path, &audit_records));
    criteria.push(assert_no_wrong_row(&audit_records));
    criteria.push(assert_no_indefinite_hangs(&audit_records));

    ParentReport { criteria }
}

async fn initialize_database(cfg: &RunConfig, db_path: &Path) {
    match cfg.backend {
        Backend::Fsqlite => {
            let conn = Connection::open(db_path.to_string_lossy().to_string())
                .await
                .expect("fsqlite open for init");
            drop(
                conn.execute(&format!("PRAGMA busy_timeout={};", cfg.busy_timeout_ms))
                    .await,
            );
            drop(conn.execute("PRAGMA journal_mode=WAL;").await);
            drop(conn.execute("PRAGMA synchronous=NORMAL;").await);
            // Best-effort: enable concurrent mode on fsqlite — it's a
            // no-op on stock and may not be wired on every fsqlite
            // build, hence best-effort.
            drop(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await);
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS swarm_kv (
                    k INTEGER PRIMARY KEY,
                    v TEXT NOT NULL,
                    last_writer INTEGER NOT NULL,
                    rev INTEGER NOT NULL
                );",
            )
            .await
            .expect("create schema");
            drop(conn.close().await);
        }
        Backend::Stock => {
            let conn = rusqlite::Connection::open(db_path).expect("stock open for init");
            conn.busy_timeout(Duration::from_millis(cfg.busy_timeout_ms))
                .expect("stock busy_timeout");
            conn.pragma_update(None, "journal_mode", "WAL")
                .expect("stock journal_mode");
            conn.pragma_update(None, "synchronous", "NORMAL")
                .expect("stock synchronous");
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS swarm_kv (
                    k INTEGER PRIMARY KEY,
                    v TEXT NOT NULL,
                    last_writer INTEGER NOT NULL,
                    rev INTEGER NOT NULL
                );",
            )
            .expect("stock create schema");
        }
    }
}

async fn run_sequential_open_check(cfg: &RunConfig, run_dir: &Path) -> CriterionReport {
    // Acceptance assertion #6: Connection::open succeeds for the Nth
    // process with consistent semantics; no carry-over freelist state.
    // We do this in-process serially; the multi-process variant is
    // inherent in the swarm itself.
    let path = run_dir.join("sequential_open.db");
    if path.exists() {
        let _ = fs::remove_file(&path);
    }
    let mut last_err: Option<String> = None;
    for i in 0..cfg.workers {
        let result: Result<(), String> = async {
            match cfg.backend {
                Backend::Fsqlite => {
                    let conn = Connection::open(path.to_string_lossy().to_string())
                        .await
                        .map_err(|e| format!("open#{i}: {e}"))?;
                    drop(
                        conn.execute(&format!("PRAGMA busy_timeout={};", cfg.busy_timeout_ms))
                            .await,
                    );
                    drop(conn.execute("PRAGMA journal_mode=WAL;").await);
                    if i == 0 {
                        conn.execute(
                            "CREATE TABLE IF NOT EXISTS open_check (k INTEGER PRIMARY KEY, v TEXT)",
                        )
                        .await
                        .map_err(|e| format!("create#{i}: {e}"))?;
                    }
                    conn.execute_with_params(
                        "INSERT INTO open_check (k, v) VALUES (?1, ?2)",
                        &[
                            SqliteValue::Integer(i as i64),
                            SqliteValue::Text(format!("seq-{i}").into()),
                        ],
                    )
                    .await
                    .map_err(|e| format!("insert#{i}: {e}"))?;
                    conn.close().await.map_err(|e| format!("close#{i}: {e}"))
                }
                Backend::Stock => {
                    let conn =
                        rusqlite::Connection::open(&path).map_err(|e| format!("open#{i}: {e}"))?;
                    conn.busy_timeout(Duration::from_millis(cfg.busy_timeout_ms))
                        .map_err(|e| format!("busy_timeout#{i}: {e}"))?;
                    conn.pragma_update(None, "journal_mode", "WAL")
                        .map_err(|e| format!("journal_mode#{i}: {e}"))?;
                    if i == 0 {
                        conn.execute_batch(
                            "CREATE TABLE IF NOT EXISTS open_check (k INTEGER PRIMARY KEY, v TEXT)",
                        )
                        .map_err(|e| format!("create#{i}: {e}"))?;
                    }
                    conn.execute(
                        "INSERT INTO open_check (k, v) VALUES (?1, ?2)",
                        rusqlite::params![i as i64, format!("seq-{i}")],
                    )
                    .map_err(|e| format!("insert#{i}: {e}"))?;
                    Ok(())
                }
            }
        }
        .await;
        if let Err(e) = result {
            last_err = Some(e);
            break;
        }
    }
    match last_err {
        Some(e) => CriterionReport {
            name: "sequential_open",
            pass: false,
            detail: format!("Connection::open became inconsistent — {e}"),
        },
        None => CriterionReport {
            name: "sequential_open",
            pass: true,
            detail: format!("{} sequential opens succeeded", cfg.workers),
        },
    }
}

fn spawn_and_collect_workers(cfg: &RunConfig, db_path: &Path, audit_dir: &Path) -> CriterionReport {
    let exe = match env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            return CriterionReport {
                name: "workers_exit_clean",
                pass: false,
                detail: format!("could not resolve current_exe: {e}"),
            };
        }
    };
    let mut children = Vec::with_capacity(cfg.workers);
    for worker_id in 0..cfg.workers {
        let audit_path = audit_dir.join(format!("worker-{worker_id}.jsonl"));
        let child = Command::new(&exe)
            // The test binary's arg list is what cargo passes; we don't
            // intercept argv (it would require knowing cargo's internal
            // args). Instead, we use env to switch into child mode.
            .env(CHILD_MARKER, "1")
            .env(CHILD_BACKEND, cfg.backend.as_str())
            .env("FSQLITE_SWARM_DB_PATH", db_path)
            .env("FSQLITE_SWARM_AUDIT_PATH", &audit_path)
            .env("FSQLITE_SWARM_WORKER_ID", worker_id.to_string())
            .env("FSQLITE_SWARM_KEYSPACE", cfg.keyspace.to_string())
            .env(
                "FSQLITE_SWARM_BUSY_TIMEOUT_MS",
                cfg.busy_timeout_ms.to_string(),
            )
            .env("FSQLITE_SWARM_SEED", cfg.seed.to_string())
            .env("FSQLITE_SWARM_SECONDS", cfg.seconds.to_string())
            // The test binary will run a single test; pin it.
            .args([
                "--ignored",
                "--exact",
                "--test-threads=1",
                "swarm_writer_harness",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        match child {
            Ok(c) => children.push((worker_id, c, Instant::now())),
            Err(e) => {
                return CriterionReport {
                    name: "workers_exit_clean",
                    pass: false,
                    detail: format!("failed to spawn worker {worker_id}: {e}"),
                };
            }
        }
    }

    // Wait window: workers can run up to (cfg.seconds * 1000ms) of
    // wall-clock if they happen to draw the maximum lifetime; in
    // practice each one self-exits in PROC_LIFETIME_MAX_MS. We bound
    // the parent wait by cfg.seconds + grace.
    let parent_deadline =
        Instant::now() + Duration::from_millis(cfg.seconds * 1_000 + CHILD_GRACE_MS);
    let mut bad: Vec<String> = Vec::new();
    let mut launched = children.len();

    // Re-launch short-lived children for the duration window so the
    // workload matches #79's "swarm of short-lived processes" shape.
    while Instant::now() < parent_deadline {
        let mut still_running = Vec::new();
        for (id, mut child, started) in children {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        bad.push(format!(
                            "worker {id} exit code {:?} after {}ms",
                            status.code(),
                            started.elapsed().as_millis()
                        ));
                    }
                }
                Ok(None) => {
                    if started.elapsed() > Duration::from_millis(PROC_LIFETIME_MAX_MS + 5_000) {
                        let _ = child.kill();
                        bad.push(format!(
                            "worker {id} hung past {}ms; killed (assertion #7 failure: \
                             busy_timeout not honored across F_SETLK contention; \
                             see beads_rust#243, #247)",
                            started.elapsed().as_millis()
                        ));
                    } else {
                        still_running.push((id, child, started));
                    }
                }
                Err(e) => {
                    bad.push(format!("worker {id} try_wait err: {e}"));
                }
            }
        }
        children = still_running;
        if Instant::now() >= parent_deadline && children.is_empty() {
            break;
        }
        // Top up: keep ~cfg.workers children alive across the run.
        while children.len() < cfg.workers && Instant::now() < parent_deadline {
            let worker_id = launched;
            launched += 1;
            let audit_path = audit_dir.join(format!("worker-{worker_id}.jsonl"));
            let spawn = Command::new(&exe)
                .env(CHILD_MARKER, "1")
                .env(CHILD_BACKEND, cfg.backend.as_str())
                .env("FSQLITE_SWARM_DB_PATH", db_path)
                .env("FSQLITE_SWARM_AUDIT_PATH", &audit_path)
                .env("FSQLITE_SWARM_WORKER_ID", worker_id.to_string())
                .env("FSQLITE_SWARM_KEYSPACE", cfg.keyspace.to_string())
                .env(
                    "FSQLITE_SWARM_BUSY_TIMEOUT_MS",
                    cfg.busy_timeout_ms.to_string(),
                )
                .env("FSQLITE_SWARM_SEED", cfg.seed.to_string())
                .env("FSQLITE_SWARM_SECONDS", cfg.seconds.to_string())
                .args([
                    "--ignored",
                    "--exact",
                    "--test-threads=1",
                    "swarm_writer_harness",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn();
            match spawn {
                Ok(c) => children.push((worker_id, c, Instant::now())),
                Err(e) => {
                    bad.push(format!("respawn worker {worker_id}: {e}"));
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    // Reap stragglers.
    for (id, mut child, started) in children {
        if started.elapsed() > Duration::from_millis(PROC_LIFETIME_MAX_MS + CHILD_GRACE_MS) {
            let _ = child.kill();
        }
        match child.wait() {
            Ok(status) if status.success() => {}
            Ok(status) => bad.push(format!("worker {id} final exit {:?}", status.code())),
            Err(e) => bad.push(format!("worker {id} wait err: {e}")),
        }
    }

    if bad.is_empty() {
        CriterionReport {
            name: "workers_exit_clean",
            pass: true,
            detail: format!("{launched} child processes completed cleanly"),
        }
    } else {
        let head: Vec<_> = bad.iter().take(8).cloned().collect();
        CriterionReport {
            name: "workers_exit_clean",
            pass: false,
            detail: format!(
                "{} worker problems (showing first {}): {:?}",
                bad.len(),
                head.len(),
                head
            ),
        }
    }
}

fn assert_integrity_passes(cfg: &RunConfig, db_path: &Path) -> CriterionReport {
    // Acceptance #1: PRAGMA integrity_check passes when stock SQLite
    // opens the post-run file. Both engines: pass through stock.
    let messages: Result<Vec<String>, String> = (|| {
        // Make sure no fsqlite-specific lockers are still holding.
        // Sleep briefly to let WAL settle.
        std::thread::sleep(Duration::from_millis(100));
        let conn = rusqlite::Connection::open(db_path).map_err(|e| e.to_string())?;
        conn.busy_timeout(Duration::from_millis(cfg.busy_timeout_ms))
            .map_err(|e| e.to_string())?;
        let mut stmt = conn
            .prepare("PRAGMA integrity_check")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let s: String = row.get(0).map_err(|e| e.to_string())?;
            out.push(s);
        }
        Ok(out)
    })();
    match messages {
        Ok(ms) if ms.len() == 1 && ms[0] == "ok" => CriterionReport {
            name: "stock_integrity_check",
            pass: true,
            detail: "ok".into(),
        },
        Ok(ms) => CriterionReport {
            name: "stock_integrity_check",
            pass: false,
            detail: format!(
                "stock SQLite integrity_check returned: {ms:?} \
                 (assertion #1; see beads_rust#235, #242)"
            ),
        },
        Err(e) => CriterionReport {
            name: "stock_integrity_check",
            pass: false,
            detail: format!("stock SQLite open/integrity failed: {e}"),
        },
    }
}

fn assert_wal_shape_clean(db_path: &Path) -> CriterionReport {
    // Acceptance #2: no "WAL page index integrity failure," "short
    // header read," or frame-order anomaly. We do a structural check:
    // the WAL sidecar must be empty (post-checkpoint) or have a valid
    // 32-byte header.
    let wal_path = db_path.with_extension(
        format!(
            "{}-wal",
            db_path
                .extension()
                .map(|e| e.to_string_lossy().to_string())
                .unwrap_or_default()
        )
        .trim_start_matches('-'),
    );
    let real_wal: PathBuf = {
        // db_path.with_extension picks the wrong shape when path has
        // no extension; reconstruct cleanly.
        let mut s = db_path.as_os_str().to_owned();
        s.push("-wal");
        PathBuf::from(s)
    };
    let _ = wal_path;
    let path = real_wal;
    if !path.exists() {
        return CriterionReport {
            name: "wal_shape",
            pass: true,
            detail: "no -wal sidecar present (already checkpointed)".into(),
        };
    }
    let bytes = match fs::metadata(&path) {
        Ok(m) => m.len(),
        Err(e) => {
            return CriterionReport {
                name: "wal_shape",
                pass: false,
                detail: format!("stat {} failed: {e}", path.display()),
            };
        }
    };
    if bytes == 0 {
        return CriterionReport {
            name: "wal_shape",
            pass: true,
            detail: "wal sidecar is 0 bytes".into(),
        };
    }
    if bytes < 32 {
        return CriterionReport {
            name: "wal_shape",
            pass: false,
            detail: format!(
                "WAL sidecar is {bytes} bytes — short header read \
                 (#56-class; assertion #2)"
            ),
        };
    }
    // Read the magic; must be 0x377f0682 or 0x377f0683.
    let mut header = [0u8; 32];
    use std::io::Read as _;
    if let Ok(mut f) = File::open(&path)
        && f.read_exact(&mut header).is_ok()
    {
        let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        if magic != 0x377f_0682 && magic != 0x377f_0683 {
            return CriterionReport {
                name: "wal_shape",
                pass: false,
                detail: format!(
                    "WAL header magic {magic:#x} not a SQLite WAL marker \
                     (assertion #2)"
                ),
            };
        }
    }
    CriterionReport {
        name: "wal_shape",
        pass: true,
        detail: format!("wal sidecar {bytes} bytes; header magic OK"),
    }
}

fn assert_vacuum_into_passes(cfg: &RunConfig, db_path: &Path, run_dir: &Path) -> CriterionReport {
    // Per #79: also `vacuum into 'verify.db'` and integrity-check the
    // copy — silent corruption can hide behind a successful checkpoint.
    let copy_path = run_dir.join("verify.db");
    if copy_path.exists() {
        let _ = fs::remove_file(&copy_path);
    }
    let res: Result<Vec<String>, String> = (|| {
        let conn = rusqlite::Connection::open(db_path).map_err(|e| e.to_string())?;
        conn.busy_timeout(Duration::from_millis(cfg.busy_timeout_ms))
            .map_err(|e| e.to_string())?;
        // Use a parameterized form so we don't have to escape the path.
        // VACUUM INTO doesn't accept ?-bind in stock SQLite, so we
        // build the SQL by quoting (path comes from our own tempdir).
        let sql = format!("VACUUM INTO '{}'", copy_path.display());
        conn.execute_batch(&sql).map_err(|e| e.to_string())?;
        let copy = rusqlite::Connection::open(&copy_path).map_err(|e| e.to_string())?;
        let mut stmt = copy
            .prepare("PRAGMA integrity_check")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let s: String = row.get(0).map_err(|e| e.to_string())?;
            out.push(s);
        }
        Ok(out)
    })();
    match res {
        Ok(ms) if ms.len() == 1 && ms[0] == "ok" => CriterionReport {
            name: "vacuum_into_integrity",
            pass: true,
            detail: "ok".into(),
        },
        Ok(ms) => CriterionReport {
            name: "vacuum_into_integrity",
            pass: false,
            detail: format!(
                "VACUUM INTO copy integrity returned: {ms:?} \
                 (silent-corruption escape hatch; see #79 spec)"
            ),
        },
        Err(e) => CriterionReport {
            name: "vacuum_into_integrity",
            pass: false,
            detail: format!("VACUUM INTO failed: {e}"),
        },
    }
}

#[derive(Debug, Clone)]
struct AuditRecord {
    #[allow(dead_code)]
    backend: String,
    pid: u32,
    op: String,
    key: i64,
    expected: Option<String>,
    observed: Option<String>,
    err: Option<String>,
}

fn collect_audit_records(audit_dir: &Path) -> Vec<AuditRecord> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(audit_dir) {
        Ok(e) => e,
        Err(_) => return out,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let f = match File::open(&path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        for line in BufReader::new(f).lines().map_while(Result::ok) {
            if let Some(rec) = parse_audit_line(&line) {
                out.push(rec);
            }
        }
    }
    out
}

fn parse_audit_line(line: &str) -> Option<AuditRecord> {
    // Light JSON parser tailored to our shape — keeps the harness
    // self-contained without pulling serde_json.
    let body = line.trim();
    if !body.starts_with('{') || !body.ends_with('}') {
        return None;
    }
    let mut backend = None;
    let mut pid: u32 = 0;
    let mut op = None;
    let mut key: i64 = 0;
    let mut expected = None;
    let mut observed = None;
    let mut err = None;
    let inner = &body[1..body.len() - 1];
    let mut i = 0;
    let bytes = inner.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'"' {
            // read field name
            let start = i + 1;
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            let name = std::str::from_utf8(&bytes[start..i]).ok()?.to_owned();
            i += 1; // closing quote
            // expect colon
            while i < bytes.len() && bytes[i] != b':' {
                i += 1;
            }
            i += 1;
            while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
                i += 1;
            }
            // value: string or number
            if i < bytes.len() && bytes[i] == b'"' {
                let s_start = i + 1;
                i += 1;
                let mut buf = String::new();
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        match bytes[i + 1] {
                            b'"' => buf.push('"'),
                            b'\\' => buf.push('\\'),
                            b'n' => buf.push('\n'),
                            b'r' => buf.push('\r'),
                            b't' => buf.push('\t'),
                            _ => buf.push(bytes[i + 1] as char),
                        }
                        i += 2;
                    } else {
                        buf.push(bytes[i] as char);
                        i += 1;
                    }
                }
                let _ = s_start;
                i += 1; // closing
                match name.as_str() {
                    "backend" => backend = Some(buf),
                    "op" => op = Some(buf),
                    "expected" => expected = Some(buf),
                    "observed" => observed = Some(buf),
                    "err" => err = Some(buf),
                    _ => {}
                }
            } else {
                // number
                let n_start = i;
                while i < bytes.len() && bytes[i] != b',' && bytes[i] != b'}' {
                    i += 1;
                }
                let n = std::str::from_utf8(&bytes[n_start..i]).ok()?.trim();
                match name.as_str() {
                    "pid" => pid = n.parse().unwrap_or(0),
                    "key" => key = n.parse().unwrap_or(0),
                    _ => {}
                }
            }
        } else {
            i += 1;
        }
    }
    Some(AuditRecord {
        backend: backend.unwrap_or_default(),
        pid,
        op: op.unwrap_or_default(),
        key,
        expected,
        observed,
        err,
    })
}

fn assert_no_silent_zero_row(
    cfg: &RunConfig,
    db_path: &Path,
    records: &[AuditRecord],
) -> CriterionReport {
    // Acceptance #4: No silent zero-row returns for primary-key SELECT
    // against a row another process committed.
    //
    // After the swarm finishes, every key that appears as `op=commit`
    // in the audit log must be present in swarm_kv when a fresh
    // connection issues SELECT WHERE k=?. If not, that's the
    // beads_rust#252/#254/#255 class.
    let mut missing: Vec<i64> = Vec::new();
    let mut seen_keys: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for r in records {
        if r.op == "commit" && r.err.is_none() {
            seen_keys.insert(r.key);
        }
    }
    if seen_keys.is_empty() {
        return CriterionReport {
            name: "no_silent_zero_row",
            pass: false,
            detail: "no committed-write audit records found — \
                     workload did not produce any commits"
                .into(),
        };
    }
    let res: Result<(), String> = (|| {
        let conn = rusqlite::Connection::open(db_path).map_err(|e| e.to_string())?;
        conn.busy_timeout(Duration::from_millis(cfg.busy_timeout_ms))
            .map_err(|e| e.to_string())?;
        let mut stmt = conn
            .prepare("SELECT k FROM swarm_kv WHERE k = ?1")
            .map_err(|e| e.to_string())?;
        for k in &seen_keys {
            let n: Option<i64> = stmt.query_row(rusqlite::params![*k], |r| r.get(0)).ok();
            if n.is_none() {
                missing.push(*k);
                if missing.len() > 64 {
                    break;
                }
            }
        }
        Ok(())
    })();
    if let Err(e) = res {
        return CriterionReport {
            name: "no_silent_zero_row",
            pass: false,
            detail: format!("post-run readback failed: {e}"),
        };
    }
    if missing.is_empty() {
        CriterionReport {
            name: "no_silent_zero_row",
            pass: true,
            detail: format!(
                "all {} committed keys readable post-run by stock SQLite",
                seen_keys.len()
            ),
        }
    } else {
        CriterionReport {
            name: "no_silent_zero_row",
            pass: false,
            detail: format!(
                "{} committed keys NOT readable post-run \
                 (assertion #4 fail; class: beads_rust#252,#254,#255). first: {:?}",
                missing.len(),
                &missing[..missing.len().min(8)]
            ),
        }
    }
}

fn assert_no_wrong_row(records: &[AuditRecord]) -> CriterionReport {
    // Acceptance #5: No silent wrong-row returns. Children record any
    // mismatch between expected and observed under op="select_pk".
    let wrong: Vec<&AuditRecord> = records
        .iter()
        .filter(|r| {
            r.op == "select_pk"
                && r.expected.is_some()
                && r.observed.is_some()
                && r.expected != r.observed
        })
        .collect();
    if wrong.is_empty() {
        CriterionReport {
            name: "no_wrong_row_returns",
            pass: true,
            detail: "no wrong-row select_pk audit entries".into(),
        }
    } else {
        let head: Vec<String> = wrong
            .iter()
            .take(4)
            .map(|r| {
                format!(
                    "pid={} k={} expected={:?} observed={:?}",
                    r.pid, r.key, r.expected, r.observed
                )
            })
            .collect();
        CriterionReport {
            name: "no_wrong_row_returns",
            pass: false,
            detail: format!(
                "{} wrong-row returns (assertion #5 fail; class: beads_rust#252). first: {head:?}",
                wrong.len()
            ),
        }
    }
}

fn assert_no_indefinite_hangs(records: &[AuditRecord]) -> CriterionReport {
    // Acceptance #7: PRAGMA busy_timeout honored across F_SETLK
    // contention; no indefinite hangs and no zero-rows-committed exits.
    //
    // We approximate "no indefinite hangs" as "no worker reported the
    // busy_timeout exhaustion sentinel ('busy_budget_exhausted')". The
    // parent already reaps actual hangs in spawn_and_collect_workers.
    let exhausted: Vec<&AuditRecord> = records
        .iter()
        .filter(|r| r.op == "busy_budget_exhausted")
        .collect();
    if exhausted.is_empty() {
        return CriterionReport {
            name: "busy_timeout_honored",
            pass: true,
            detail: "no busy_budget_exhausted records".into(),
        };
    }
    CriterionReport {
        name: "busy_timeout_honored",
        pass: false,
        detail: format!(
            "{} busy_budget_exhausted entries (assertion #7 fail; class: beads_rust#243,#247)",
            exhausted.len()
        ),
    }
}

// =====================================================================
// Child process
// =====================================================================

fn run_as_child(backend: Backend) {
    // Children must NOT panic the test framework (cargo treats panics
    // as failures). We catch any failure and exit cleanly with code 1.
    // The async child body gets its own runtime *inside* the
    // `catch_unwind` boundary so panics still surface as exit code 2.
    let res = std::panic::catch_unwind(|| {
        let mut outcome: Result<(), String> = Ok(());
        asupersync::test_utils::run_test(|| async {
            outcome = child_main(backend).await;
        });
        outcome
    });
    match res {
        Ok(Ok(())) => std::process::exit(0),
        Ok(Err(e)) => {
            eprintln!(
                "[swarm-child] {} {} error: {}",
                backend.as_str(),
                std::process::id(),
                e
            );
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("[swarm-child] panic in worker {}", std::process::id());
            std::process::exit(2);
        }
    }
}

async fn child_main(backend: Backend) -> Result<(), String> {
    let db_path = env::var("FSQLITE_SWARM_DB_PATH").map_err(|e| format!("DB_PATH: {e}"))?;
    let audit_path =
        env::var("FSQLITE_SWARM_AUDIT_PATH").map_err(|e| format!("AUDIT_PATH: {e}"))?;
    let worker_id: u64 = env::var("FSQLITE_SWARM_WORKER_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let keyspace: i64 = env::var("FSQLITE_SWARM_KEYSPACE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_KEYSPACE);
    let busy_timeout_ms: u64 = env::var("FSQLITE_SWARM_BUSY_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BUSY_TIMEOUT_MS);
    let seed: u64 = env::var("FSQLITE_SWARM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_SEED);

    // Per-process RNG: seed depends on worker id, pid, and time so
    // different short-lived processes diverge.
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let rng_seed = seed
        ^ worker_id.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ u64::from(std::process::id())
        ^ now_ns;
    let mut rng = StdRng::seed_from_u64(rng_seed);

    let lifetime_ms = rng.random_range(PROC_LIFETIME_MIN_MS..=PROC_LIFETIME_MAX_MS);
    let ops_count = rng.random_range(PROC_OPS_MIN..=PROC_OPS_MAX);
    let deadline = Instant::now() + Duration::from_millis(lifetime_ms);

    // Open audit log (append).
    let mut audit = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&audit_path)
        .map_err(|e| format!("audit open: {e}"))?;

    let pid = std::process::id();

    match backend {
        Backend::Fsqlite => {
            child_run_fsqlite(
                &db_path,
                &mut audit,
                pid,
                worker_id,
                keyspace,
                busy_timeout_ms,
                ops_count,
                deadline,
                &mut rng,
            )
            .await
        }
        Backend::Stock => child_run_stock(
            &db_path,
            &mut audit,
            pid,
            worker_id,
            keyspace,
            busy_timeout_ms,
            ops_count,
            deadline,
            &mut rng,
        ),
    }
}

fn pick_op(rng: &mut StdRng) -> Op {
    match rng.random_range(0..4) {
        0 => Op::Insert,
        1 => Op::Update,
        2 => Op::SelectByPk,
        _ => Op::SelectRange,
    }
}

#[allow(clippy::too_many_arguments)]
async fn child_run_fsqlite(
    db_path: &str,
    audit: &mut File,
    pid: u32,
    worker_id: u64,
    keyspace: i64,
    busy_timeout_ms: u64,
    ops_count: u32,
    deadline: Instant,
    rng: &mut StdRng,
) -> Result<(), String> {
    let conn = match Connection::open(db_path.to_string()).await {
        Ok(c) => c,
        Err(e) => {
            audit
                .write_all(
                    audit_line(
                        "fsqlite",
                        pid,
                        "open_err",
                        -1,
                        None,
                        None,
                        None,
                        0,
                        Some(&format!("{e}")),
                    )
                    .as_bytes(),
                )
                .ok();
            return Err(format!("open: {e}"));
        }
    };
    drop(
        conn.execute(&format!("PRAGMA busy_timeout={busy_timeout_ms};"))
            .await,
    );
    drop(conn.execute("PRAGMA journal_mode=WAL;").await);
    drop(conn.execute("PRAGMA synchronous=NORMAL;").await);
    drop(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await);

    let busy_budget = Duration::from_millis(busy_timeout_ms.saturating_mul(2) + 2_000);

    let mut completed = 0_u32;
    while completed < ops_count && Instant::now() < deadline {
        let op = pick_op(rng);
        let key = rng.random_range(0..keyspace);
        let value = format!("w{worker_id}-pid{pid}-rev{completed}");
        let started = Instant::now();
        let res: Result<String, FrankenError> = match op {
            Op::Insert | Op::Update => {
                retry_busy_fsqlite(busy_budget, async || {
                    conn.begin_transaction().await?;
                    let r: Result<(), FrankenError> = async {
                        conn.execute_with_params(
                            "INSERT INTO swarm_kv (k, v, last_writer, rev) \
                             VALUES (?1, ?2, ?3, 1) \
                             ON CONFLICT(k) DO UPDATE SET \
                                v=excluded.v, \
                                last_writer=excluded.last_writer, \
                                rev=swarm_kv.rev+1",
                            &[
                                SqliteValue::Integer(key),
                                SqliteValue::Text(value.clone().into()),
                                SqliteValue::Integer(worker_id as i64),
                            ],
                        )
                        .await?;
                        Ok(())
                    }
                    .await;
                    match r {
                        Ok(()) => {
                            conn.commit_transaction().await?;
                            Ok("commit".to_owned())
                        }
                        Err(e) => {
                            drop(conn.rollback_transaction().await);
                            Err(e)
                        }
                    }
                })
                .await
            }
            Op::SelectByPk => {
                // Read-your-own-writes / cross-process visibility check.
                // We commit a known marker first, then immediately
                // SELECT it back. Mismatch is assertion #5/#3.
                let r = retry_busy_fsqlite(busy_budget, async || {
                    conn.begin_transaction().await?;
                    conn.execute_with_params(
                        "INSERT INTO swarm_kv (k, v, last_writer, rev) \
                         VALUES (?1, ?2, ?3, 1) \
                         ON CONFLICT(k) DO UPDATE SET \
                            v=excluded.v, \
                            last_writer=excluded.last_writer, \
                            rev=swarm_kv.rev+1",
                        &[
                            SqliteValue::Integer(key),
                            SqliteValue::Text(value.clone().into()),
                            SqliteValue::Integer(worker_id as i64),
                        ],
                    )
                    .await?;
                    conn.commit_transaction().await?;
                    let row = conn
                        .query_row_with_params(
                            "SELECT v FROM swarm_kv WHERE k = ?1",
                            &[SqliteValue::Integer(key)],
                        )
                        .await
                        .map(|row| match row.values().first() {
                            Some(SqliteValue::Text(s)) => s.to_string(),
                            _ => String::from("<non-text>"),
                        });
                    match row {
                        Ok(observed) => {
                            // Audit the readback.
                            let _ = audit.write_all(
                                audit_line(
                                    "fsqlite",
                                    pid,
                                    "select_pk",
                                    key,
                                    Some(&value),
                                    Some(&value),
                                    Some(&observed),
                                    started.elapsed().as_micros(),
                                    None,
                                )
                                .as_bytes(),
                            );
                            if observed == value {
                                Ok("select_pk_ok".to_owned())
                            } else {
                                Ok("select_pk_mismatch".to_owned())
                            }
                        }
                        Err(e) => {
                            // Zero-row return for a row we just
                            // committed: assertion #4.
                            let _ = audit.write_all(
                                audit_line(
                                    "fsqlite",
                                    pid,
                                    "select_pk",
                                    key,
                                    Some(&value),
                                    Some(&value),
                                    None,
                                    started.elapsed().as_micros(),
                                    Some(&format!("{e}")),
                                )
                                .as_bytes(),
                            );
                            Err(e)
                        }
                    }
                })
                .await;
                r
            }
            Op::SelectRange => {
                retry_busy_fsqlite(busy_budget, async || {
                    let lo = rng.random_range(0..keyspace);
                    let hi = (lo + rng.random_range(1..32)).min(keyspace);
                    let _rows = conn
                        .query_with_params(
                            "SELECT k, v FROM swarm_kv WHERE k BETWEEN ?1 AND ?2",
                            &[SqliteValue::Integer(lo), SqliteValue::Integer(hi)],
                        )
                        .await?;
                    Ok("select_range".to_owned())
                })
                .await
            }
        };
        let latency = started.elapsed().as_micros();
        match res {
            Ok(label) => {
                let _ = audit.write_all(
                    audit_line(
                        "fsqlite",
                        pid,
                        &label,
                        key,
                        Some(&value),
                        None,
                        None,
                        latency,
                        None,
                    )
                    .as_bytes(),
                );
            }
            Err(e) => {
                let label = if format!("{e}").contains("budget_exhausted") {
                    "busy_budget_exhausted"
                } else {
                    "op_err"
                };
                let _ = audit.write_all(
                    audit_line(
                        "fsqlite",
                        pid,
                        label,
                        key,
                        Some(&value),
                        None,
                        None,
                        latency,
                        Some(&format!("{e}")),
                    )
                    .as_bytes(),
                );
            }
        }
        completed = completed.saturating_add(1);
    }

    drop(conn.close().await);
    Ok(())
}

async fn retry_busy_fsqlite<T>(
    budget: Duration,
    mut op: impl AsyncFnMut() -> Result<T, FrankenError>,
) -> Result<T, FrankenError> {
    let started = Instant::now();
    let mut attempt = 0_u32;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let transient = matches!(
                    e,
                    FrankenError::Busy
                        | FrankenError::BusyRecovery
                        | FrankenError::BusySnapshot { .. }
                        | FrankenError::DatabaseLocked { .. }
                );
                if !transient {
                    return Err(e);
                }
                if started.elapsed() >= budget {
                    return Err(FrankenError::Internal(format!(
                        "busy_budget_exhausted: {e}"
                    )));
                }
                attempt = attempt.saturating_add(1);
                let backoff =
                    Duration::from_millis((5_u64.saturating_mul(1 << attempt.min(5))).min(250));
                std::thread::sleep(backoff);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn child_run_stock(
    db_path: &str,
    audit: &mut File,
    pid: u32,
    worker_id: u64,
    keyspace: i64,
    busy_timeout_ms: u64,
    ops_count: u32,
    deadline: Instant,
    rng: &mut StdRng,
) -> Result<(), String> {
    let conn = rusqlite::Connection::open(db_path).map_err(|e| format!("open: {e}"))?;
    conn.busy_timeout(Duration::from_millis(busy_timeout_ms))
        .map_err(|e| format!("busy_timeout: {e}"))?;
    let _ = conn.pragma_update(None, "journal_mode", "WAL");
    let _ = conn.pragma_update(None, "synchronous", "NORMAL");

    let mut completed = 0_u32;
    while completed < ops_count && Instant::now() < deadline {
        let op = pick_op(rng);
        let key = rng.random_range(0..keyspace);
        let value = format!("w{worker_id}-pid{pid}-rev{completed}");
        let started = Instant::now();
        let res: Result<String, rusqlite::Error> = match op {
            Op::Insert | Op::Update => conn
                .execute(
                    "INSERT INTO swarm_kv (k, v, last_writer, rev) \
                     VALUES (?1, ?2, ?3, 1) \
                     ON CONFLICT(k) DO UPDATE SET \
                        v=excluded.v, \
                        last_writer=excluded.last_writer, \
                        rev=swarm_kv.rev+1",
                    rusqlite::params![key, value, worker_id as i64],
                )
                .map(|_| "commit".to_owned()),
            Op::SelectByPk => (|| -> Result<String, rusqlite::Error> {
                conn.execute(
                    "INSERT INTO swarm_kv (k, v, last_writer, rev) \
                     VALUES (?1, ?2, ?3, 1) \
                     ON CONFLICT(k) DO UPDATE SET \
                        v=excluded.v, \
                        last_writer=excluded.last_writer, \
                        rev=swarm_kv.rev+1",
                    rusqlite::params![key, value, worker_id as i64],
                )?;
                let observed: Option<String> = conn
                    .query_row(
                        "SELECT v FROM swarm_kv WHERE k = ?1",
                        rusqlite::params![key],
                        |r| r.get::<_, String>(0),
                    )
                    .ok();
                let _ = audit.write_all(
                    audit_line(
                        "stock",
                        pid,
                        "select_pk",
                        key,
                        Some(&value),
                        Some(&value),
                        observed.as_deref(),
                        started.elapsed().as_micros(),
                        None,
                    )
                    .as_bytes(),
                );
                Ok("select_pk".to_owned())
            })(),
            Op::SelectRange => (|| -> Result<String, rusqlite::Error> {
                let lo = rng.random_range(0..keyspace);
                let hi = (lo + rng.random_range(1..32)).min(keyspace);
                let mut stmt =
                    conn.prepare("SELECT k, v FROM swarm_kv WHERE k BETWEEN ?1 AND ?2")?;
                let mut rows = stmt.query(rusqlite::params![lo, hi])?;
                while let Some(_r) = rows.next()? {}
                Ok("select_range".to_owned())
            })(),
        };
        let latency = started.elapsed().as_micros();
        match res {
            Ok(label) => {
                let _ = audit.write_all(
                    audit_line(
                        "stock",
                        pid,
                        &label,
                        key,
                        Some(&value),
                        None,
                        None,
                        latency,
                        None,
                    )
                    .as_bytes(),
                );
            }
            Err(e) => {
                let _ = audit.write_all(
                    audit_line(
                        "stock",
                        pid,
                        "op_err",
                        key,
                        Some(&value),
                        None,
                        None,
                        latency,
                        Some(&format!("{e}")),
                    )
                    .as_bytes(),
                );
            }
        }
        completed = completed.saturating_add(1);
    }
    Ok(())
}
