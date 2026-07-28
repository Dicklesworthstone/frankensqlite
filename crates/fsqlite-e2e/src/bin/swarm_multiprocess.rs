//! Multi-process swarm-writer stress harness for FrankenSQLite.
//!
//! The parent process initializes one WAL database, spawns N child processes
//! that all open that same file via `fsqlite`, then verifies the family of
//! concurrency invariants from frankensqlite#70.

#![allow(clippy::struct_excessive_bools, clippy::too_many_lines)]
// bd-mnlk2 / bd-zavyn: the hoisted timed bodies await fsqlite-core's
// deliberately large, deeply nested engine futures inside one runtime entry
// per transaction attempt; boxing them would put an allocation inside the
// timed window.
#![allow(clippy::large_futures)]
#![recursion_limit = "512"]

use std::env;
use std::fs::{self, File};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fsqlite::{Connection, FrankenError, SqliteValue};
use fsqlite_e2e::verify_csqlite::{verify_concurrency_artifact, write_artifact_bundle};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde::{Deserialize, Serialize};

const REPORT_SCHEMA_V1: &str = "fsqlite-e2e.swarm-multiprocess-report.v1";
const WORKER_REPORT_SCHEMA_V1: &str = "fsqlite-e2e.swarm-multiprocess-worker-report.v1";
const DEFAULT_WORKERS: usize = 8;
const DEFAULT_SECONDS: u64 = 60;
const DEFAULT_BUSY_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_SEED: u64 = 0x4653_514C_5357_4152;
const DEFAULT_HOT_ROWS: i64 = 32;
const ROW_ID_STRIDE: i64 = 1_000_000_000;
const HOT_ROW_BASE: i64 = -1_000_000;
const START_DELAY_MS: u64 = 1_500;
const PARENT_TIMEOUT_GRACE_MS: u64 = 20_000;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const MAX_WORKERS: usize = 1_024;
static GROUP_COMMIT_TRACE_ENABLED: OnceLock<bool> = OnceLock::new();

type HarnessResult<T> = Result<T, String>;

fn group_commit_trace_enabled() -> bool {
    *GROUP_COMMIT_TRACE_ENABLED.get_or_init(|| {
        env::var_os("FSQLITE_TRACE_GROUP_COMMIT").is_some_and(|value| {
            let value = value.to_string_lossy();
            !value.is_empty() && value != "0" && !value.eq_ignore_ascii_case("false")
        })
    })
}

#[derive(Debug, Clone)]
struct RunConfig {
    workers: usize,
    seconds: u64,
    iters: Option<u64>,
    busy_timeout_ms: u64,
    seed: u64,
    db_path: Option<PathBuf>,
    artifact_root: PathBuf,
    open_checks: Option<usize>,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            workers: DEFAULT_WORKERS,
            seconds: DEFAULT_SECONDS,
            iters: None,
            busy_timeout_ms: DEFAULT_BUSY_TIMEOUT_MS,
            seed: DEFAULT_SEED,
            db_path: None,
            artifact_root: PathBuf::from("crates/fsqlite-e2e/artifacts/swarm-multiprocess"),
            open_checks: None,
        }
    }
}

impl RunConfig {
    const fn resolved_open_checks(&self) -> usize {
        match self.open_checks {
            Some(value) => value,
            None => self.workers,
        }
    }
}

#[derive(Debug, Clone)]
struct ChildConfig {
    worker_id: usize,
    start_at_ms: u64,
    report_path: PathBuf,
    jsonl_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
enum Mode {
    Parent(RunConfig),
    Child { run: RunConfig, child: ChildConfig },
}

#[derive(Debug, Serialize)]
struct ReportConfig {
    workers: usize,
    seconds: u64,
    iters: Option<u64>,
    busy_timeout_ms: u64,
    seed: u64,
    db_path: String,
    artifact_root: String,
    open_checks: usize,
}

#[derive(Debug, Serialize)]
struct SwarmReport {
    schema: &'static str,
    success: bool,
    duration_ms: u64,
    run_dir: String,
    db_path: String,
    forensics_dir: Option<String>,
    config: ReportConfig,
    criteria: Vec<CriterionReport>,
    row_counts: RowCounts,
    workers: Vec<WorkerProcessReport>,
}

#[derive(Debug, Serialize)]
struct CriterionReport {
    name: &'static str,
    pass: bool,
    duration_ms: u64,
    detail: String,
}

#[derive(Debug, Serialize, Default)]
struct RowCounts {
    total_rows: Option<i64>,
    live_rows: Option<i64>,
    progress_rows: Option<i64>,
    committed_workers: Option<i64>,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct WorkerProcessReport {
    worker_id: usize,
    exit_code: Option<i32>,
    killed_for_timeout: bool,
    duration_ms: u64,
    report_path: String,
    stdout_snippet: String,
    stderr_snippet: String,
    report: Option<WorkerReport>,
    report_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerReport {
    schema: String,
    worker_id: usize,
    pid: u32,
    success: bool,
    duration_ms: u64,
    iterations: u64,
    transactions_committed: u64,
    inserts: u64,
    updates: u64,
    deletes: u64,
    pk_selects: u64,
    own_read_checks: u64,
    cross_process_read_checks: u64,
    wrong_row_checks: u64,
    busy_errors: u64,
    busy_retries: u64,
    bounded_retry_exhaustions: u64,
    read_your_own_write_pass: bool,
    cross_process_visibility_pass: bool,
    wrong_row_returns_pass: bool,
    busy_timeout_honored_pass: bool,
    failure: Option<String>,
}

#[derive(Debug, Default)]
struct WorkerCounters {
    iterations: u64,
    transactions_committed: u64,
    inserts: u64,
    updates: u64,
    deletes: u64,
    pk_selects: u64,
    own_read_checks: u64,
    cross_process_read_checks: u64,
    wrong_row_checks: u64,
    busy_errors: u64,
    busy_retries: u64,
    bounded_retry_exhaustions: u64,
}

#[derive(Debug)]
struct WorkerState {
    next_seq: i64,
    live_ids: Vec<i64>,
}

#[derive(Debug)]
struct CommittedWrite {
    id: i64,
    seq: i64,
    payload: String,
    deleted_id: Option<i64>,
}

#[derive(Debug)]
struct ObservedRow {
    id: i64,
    owner: i64,
    seq: i64,
    payload: String,
    deleted: bool,
}

#[derive(Debug)]
struct ProgressRow {
    worker_id: i64,
    last_id: i64,
    last_seq: i64,
    payload: String,
}

fn main() -> ExitCode {
    match real_main() {
        Ok(success) => {
            if success {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}

fn real_main() -> HarnessResult<bool> {
    match parse_args()? {
        Mode::Parent(config) => run_parent(config),
        Mode::Child { run, child } => run_child_and_write_report(&run, &child),
    }
}

fn run_parent(config: RunConfig) -> HarnessResult<bool> {
    validate_parent_config(&config)?;
    let started = Instant::now();
    let run_dir = config.artifact_root.join(timestamp_dir_name("run"));
    fs::create_dir_all(&run_dir).map_err(|err| {
        format!(
            "failed to create artifact directory `{}`: {err}",
            run_dir.display()
        )
    })?;

    let db_path = match &config.db_path {
        Some(path) => {
            if path.exists() {
                return Err(format!(
                    "refusing to run swarm harness against existing DB `{}`",
                    path.display()
                ));
            }
            path.clone()
        }
        None => run_dir.join("swarm.db"),
    };
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create DB parent directory `{}`: {err}",
                parent.display()
            )
        })?;
    }

    let open_check = timed_criterion("sequential_open", || {
        run_sequential_open_check(&run_dir, &config)
    });
    initialize_database(&db_path, &config)?;

    let start_at_ms = unix_time_ms().saturating_add(START_DELAY_MS);
    let workers = spawn_workers(&config, &db_path, &run_dir, start_at_ms)?;

    let heartbeat_stop = Arc::new(AtomicBool::new(false));
    let heartbeat_handle = {
        let stop = Arc::clone(&heartbeat_stop);
        let n_workers = config.workers;
        let seed = config.seed;
        thread::spawn(move || {
            let mut tick = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(HEARTBEAT_INTERVAL);
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                tick += 1;
                let elapsed_s = tick * HEARTBEAT_INTERVAL.as_secs();
                let _ = writeln!(
                    std::io::stderr(),
                    "[swarm-heartbeat] tick={tick} elapsed={elapsed_s}s workers={n_workers} seed={seed}"
                );
            }
        })
    };

    let worker_reports = collect_workers(workers, &config, &run_dir)?;
    heartbeat_stop.store(true, Ordering::Relaxed);
    let _ = heartbeat_handle.join();

    let wal_shape = timed_criterion("wal_shape", || validate_wal_shape(&db_path));
    let wal_checkpoint = timed_criterion("wal_checkpoint", || {
        run_fsqlite_checkpoint(&db_path, &config)
    });
    let fsqlite_integrity = timed_criterion("fsqlite_integrity_check", || {
        run_fsqlite_integrity_check(&db_path, &config)
    });
    let sqlite_integrity = timed_criterion("sqlite_integrity_check", || {
        run_sqlite_integrity_check(&db_path, &config)
    });
    let final_visibility = timed_criterion("final_cross_process_visibility", || {
        verify_final_progress_visibility(&db_path, &config)
    });
    let row_counts = collect_row_counts(&db_path, &config);

    let jsonl_check = timed_criterion("jsonl_schema_validation", || {
        validate_worker_jsonl(&run_dir, config.workers)
    });
    let wal_corruption = wal_corruption_criterion(&wal_shape, &wal_checkpoint);
    let csqlite_cross = timed_criterion("csqlite_cross_check", || {
        csqlite_cross_check(&db_path, &run_dir)
    });
    let criteria = vec![
        open_check,
        worker_process_criterion(&worker_reports),
        read_your_own_write_criterion(&worker_reports),
        cross_process_visibility_criterion(&worker_reports, &final_visibility, config.workers),
        wrong_row_criterion(&worker_reports),
        busy_timeout_criterion(&worker_reports),
        jsonl_check,
        wal_corruption,
        wal_shape,
        wal_checkpoint,
        fsqlite_integrity,
        sqlite_integrity,
        csqlite_cross,
        final_visibility,
    ];

    let success = criteria.iter().all(|criterion| criterion.pass);
    let forensics_dir = if success {
        None
    } else {
        Some(copy_forensics(&db_path, &run_dir)?)
    };

    let report = SwarmReport {
        schema: REPORT_SCHEMA_V1,
        success,
        duration_ms: duration_to_u64_ms(started.elapsed()),
        run_dir: run_dir.to_string_lossy().into_owned(),
        db_path: db_path.to_string_lossy().into_owned(),
        forensics_dir: forensics_dir
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned()),
        config: ReportConfig {
            workers: config.workers,
            seconds: config.seconds,
            iters: config.iters,
            busy_timeout_ms: config.busy_timeout_ms,
            seed: config.seed,
            db_path: db_path.to_string_lossy().into_owned(),
            artifact_root: config.artifact_root.to_string_lossy().into_owned(),
            open_checks: config.resolved_open_checks(),
        },
        criteria,
        row_counts,
        workers: worker_reports,
    };

    write_report_artifact(&run_dir, &report)?;
    print_json(&report)?;
    Ok(success)
}

fn run_child_and_write_report(config: &RunConfig, child: &ChildConfig) -> HarnessResult<bool> {
    if let Some(parent) = child.report_path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create child report directory `{}`: {err}",
                parent.display()
            )
        })?;
    }
    let report = run_child(config, child);
    let json = serde_json::to_string_pretty(&report)
        .map_err(|err| format!("failed to serialize worker report: {err}"))?;
    fs::write(&child.report_path, json).map_err(|err| {
        format!(
            "failed to write worker report `{}`: {err}",
            child.report_path.display()
        )
    })?;
    Ok(report.success)
}

fn run_child(config: &RunConfig, child: &ChildConfig) -> WorkerReport {
    let started = Instant::now();
    let mut counters = WorkerCounters::default();
    let result = run_child_workload(config, child, &mut counters);
    let success = result.is_ok();
    WorkerReport {
        schema: WORKER_REPORT_SCHEMA_V1.to_owned(),
        worker_id: child.worker_id,
        pid: std::process::id(),
        success,
        duration_ms: duration_to_u64_ms(started.elapsed()),
        iterations: counters.iterations,
        transactions_committed: counters.transactions_committed,
        inserts: counters.inserts,
        updates: counters.updates,
        deletes: counters.deletes,
        pk_selects: counters.pk_selects,
        own_read_checks: counters.own_read_checks,
        cross_process_read_checks: counters.cross_process_read_checks,
        wrong_row_checks: counters.wrong_row_checks,
        busy_errors: counters.busy_errors,
        busy_retries: counters.busy_retries,
        bounded_retry_exhaustions: counters.bounded_retry_exhaustions,
        read_your_own_write_pass: success && counters.own_read_checks > 0,
        // Per-worker peer checks are opportunistic coverage: under high
        // write-conflict contention, one process can legitimately finish its
        // time window before slower retried peers publish progress. Any actual
        // peer visibility mismatch makes `success` false; the parent process
        // runs the authoritative final visibility check after all workers exit.
        cross_process_visibility_pass: success,
        wrong_row_returns_pass: success,
        busy_timeout_honored_pass: counters.bounded_retry_exhaustions == 0,
        failure: result.err(),
    }
}

fn run_child_workload(
    config: &RunConfig,
    child: &ChildConfig,
    counters: &mut WorkerCounters,
) -> HarnessResult<()> {
    wait_until(child.start_at_ms);
    let db_path = config
        .db_path
        .as_ref()
        .ok_or_else(|| "child mode requires --db".to_owned())?;
    let conn = open_fsqlite(db_path)?;
    configure_fsqlite(&conn, config)?;

    let mut jsonl_writer = child
        .jsonl_path
        .as_ref()
        .map(|path| -> HarnessResult<std::io::BufWriter<File>> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|err| {
                    format!("failed to create jsonl dir `{}`: {err}", parent.display())
                })?;
            }
            let file = File::create(path).map_err(|err| {
                format!("failed to create jsonl file `{}`: {err}", path.display())
            })?;
            Ok(std::io::BufWriter::new(file))
        })
        .transpose()?;

    let worker_seed = config
        .seed
        .wrapping_add((child.worker_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut rng = StdRng::seed_from_u64(worker_seed);
    let mut state = WorkerState {
        next_seq: 1,
        live_ids: Vec::new(),
    };
    let deadline = Instant::now() + Duration::from_secs(config.seconds);
    let mut op_id: u64 = 0;

    while config.iters.is_none_or(|limit| counters.iterations < limit) && Instant::now() < deadline
    {
        let committed =
            commit_mixed_transaction(&conn, config, child, &mut state, counters, &mut rng)?;
        counters.iterations = counters.iterations.saturating_add(1);
        if let Some(writer) = jsonl_writer.as_mut() {
            emit_op_jsonl(
                writer,
                config,
                child,
                op_id,
                "mixed_commit",
                "ok",
                &committed,
            );
            op_id += 1;
        }
        verify_committed_own_row(&conn, config, child.worker_id, &committed, counters)?;
        verify_random_pk_lookup(&conn, config, &state, counters, &mut rng)?;
        verify_other_worker_visible(&conn, config, child.worker_id, counters, &mut rng)?;
        if let Some(deleted_id) = committed.deleted_id {
            verify_deleted_row_absent_or_matching(&conn, config, deleted_id, counters)?;
        }
    }

    fsqlite_e2e::block_on(conn.close())
        .map_err(|err| format!("worker connection close/checkpoint failed: {err}"))
}

fn commit_mixed_transaction(
    conn: &Connection,
    config: &RunConfig,
    child: &ChildConfig,
    state: &mut WorkerState,
    counters: &mut WorkerCounters,
    rng: &mut StdRng,
) -> HarnessResult<CommittedWrite> {
    let seq = state.next_seq;
    let id = row_id_for(child.worker_id, seq)?;
    let payload = random_payload(rng, child.worker_id, seq);
    let hot_id = HOT_ROW_BASE - rng.random_range(0..DEFAULT_HOT_ROWS);
    let update_id = state
        .live_ids
        .get(rng.random_range(0..state.live_ids.len().max(1)))
        .copied()
        .unwrap_or(hot_id);
    let deleted_id = choose_delete_id(&state.live_ids, rng);
    let update_payload = format!(
        "update:w{}:s{}:{}",
        child.worker_id,
        seq,
        rng.random::<u32>()
    );

    // bd-mnlk2 / bd-zavyn: one runtime entry per transaction attempt (the
    // deadline loop scores iterations/sec, so per-op entries inflated the
    // denominator). retry_fsqlite's backoff sleep already sits outside the
    // closure, i.e. outside the entered runtime.
    retry_fsqlite(config, counters, "mixed write transaction", || {
        fsqlite_e2e::block_on(async {
            conn.begin_transaction().await?;
            let mut in_txn = true;
            let result = async {
                let inserted = conn
                    .execute_with_params(
                        "INSERT INTO swarm_rows \
                         (id, owner, seq, payload, touched_by, generation, deleted) \
                         VALUES (?1, ?2, ?3, ?4, ?2, 0, 0)",
                        &[
                            SqliteValue::Integer(id),
                            SqliteValue::Integer(
                                i64::try_from(child.worker_id).unwrap_or(i64::MAX),
                            ),
                            SqliteValue::Integer(seq),
                            SqliteValue::Text(payload.clone().into()),
                        ],
                    )
                    .await?;
                if inserted != 1 {
                    return Err(FrankenError::Internal(format!(
                        "insert affected {inserted} rows for id={id}"
                    )));
                }

                let updated = conn
                    .execute_with_params(
                        "UPDATE swarm_rows \
                         SET payload = ?1, touched_by = ?2, generation = generation + 1 \
                         WHERE id = ?3",
                        &[
                            SqliteValue::Text(update_payload.clone().into()),
                            SqliteValue::Integer(
                                i64::try_from(child.worker_id).unwrap_or(i64::MAX),
                            ),
                            SqliteValue::Integer(update_id),
                        ],
                    )
                    .await?;
                if updated != 1 {
                    return Err(FrankenError::Internal(format!(
                        "update affected {updated} rows for id={update_id}"
                    )));
                }

                if let Some(delete_id) = deleted_id {
                    let deleted = conn
                        .execute_with_params(
                            "DELETE FROM swarm_rows WHERE id = ?1 AND owner = ?2",
                            &[
                                SqliteValue::Integer(delete_id),
                                SqliteValue::Integer(
                                    i64::try_from(child.worker_id).unwrap_or(i64::MAX),
                                ),
                            ],
                        )
                        .await?;
                    if deleted != 1 {
                        return Err(FrankenError::Internal(format!(
                            "delete affected {deleted} rows for id={delete_id}"
                        )));
                    }
                }

                let progressed = conn
                    .execute_with_params(
                        "UPDATE worker_progress \
                         SET last_id = ?1, last_seq = ?2, payload = ?3, observed_epoch = ?2 \
                         WHERE worker_id = ?4",
                        &[
                            SqliteValue::Integer(id),
                            SqliteValue::Integer(seq),
                            SqliteValue::Text(payload.clone().into()),
                            SqliteValue::Integer(
                                i64::try_from(child.worker_id).unwrap_or(i64::MAX),
                            ),
                        ],
                    )
                    .await?;
                if progressed != 1 {
                    return Err(FrankenError::Internal(format!(
                        "progress update affected {progressed} rows for worker={}",
                        child.worker_id
                    )));
                }

                conn.commit_transaction().await?;
                in_txn = false;
                Ok(())
            }
            .await;
            if result.is_err() && in_txn {
                drop(conn.rollback_transaction().await);
            }
            result
        })
    })?;
    trace_swarm_post_commit_visibility(conn, config, child.worker_id, id, seq, &payload);

    counters.transactions_committed = counters.transactions_committed.saturating_add(1);
    counters.inserts = counters.inserts.saturating_add(1);
    counters.updates = counters.updates.saturating_add(1);
    if let Some(delete_id) = deleted_id {
        state.live_ids.retain(|candidate| *candidate != delete_id);
        counters.deletes = counters.deletes.saturating_add(1);
    }
    state.live_ids.push(id);
    state.next_seq = state.next_seq.saturating_add(1);

    Ok(CommittedWrite {
        id,
        seq,
        payload,
        deleted_id,
    })
}

fn verify_committed_own_row(
    conn: &Connection,
    config: &RunConfig,
    worker_id: usize,
    committed: &CommittedWrite,
    counters: &mut WorkerCounters,
) -> HarnessResult<()> {
    let expected = ObservedRow {
        id: committed.id,
        owner: i64::try_from(worker_id).map_err(|err| format!("worker id overflow: {err}"))?,
        seq: committed.seq,
        payload: committed.payload.clone(),
        deleted: false,
    };
    let observed = query_pk(conn, config, counters, committed.id)?;
    counters.own_read_checks = counters.own_read_checks.saturating_add(1);
    match observed {
        Some(row) if rows_match(&row, &expected) => Ok(()),
        Some(row) => Err(format!(
            "read-your-own-write mismatch for id={}: expected {:?}, observed {:?}",
            committed.id, expected, row
        )),
        None => Err(format!(
            "read-your-own-write returned zero rows for committed id={}",
            committed.id
        )),
    }
}

fn verify_random_pk_lookup(
    conn: &Connection,
    config: &RunConfig,
    state: &WorkerState,
    counters: &mut WorkerCounters,
    rng: &mut StdRng,
) -> HarnessResult<()> {
    let lookup_id = if !state.live_ids.is_empty() && rng.random_ratio(3, 4) {
        state.live_ids[rng.random_range(0..state.live_ids.len())]
    } else {
        HOT_ROW_BASE - rng.random_range(0..DEFAULT_HOT_ROWS)
    };
    let _ = query_pk(conn, config, counters, lookup_id)?;
    Ok(())
}

fn verify_other_worker_visible(
    conn: &Connection,
    config: &RunConfig,
    worker_id: usize,
    counters: &mut WorkerCounters,
    rng: &mut StdRng,
) -> HarnessResult<()> {
    if config.workers <= 1 {
        return Ok(());
    }
    let start = rng.random_range(0..config.workers);
    for offset in 0..config.workers {
        let other = (start + offset) % config.workers;
        if other == worker_id {
            continue;
        }
        let expected_worker_id =
            i64::try_from(other).map_err(|err| format!("worker id overflow: {err}"))?;
        let read = with_consistent_read_transaction(
            conn,
            config,
            counters,
            "cross-process consistency snapshot",
            |counters| {
                let progress = query_progress(conn, config, counters, other)?;
                if progress.last_id == 0 {
                    return Ok(None);
                }
                if progress.worker_id != expected_worker_id {
                    return Err(format!(
                        "worker_progress wrong-row return: queried worker={other}, got worker={}",
                        progress.worker_id
                    ));
                }
                let observed = query_pk(conn, config, counters, progress.last_id)?;
                Ok(Some((progress, observed)))
            },
        )?;
        let Some((progress, observed)) = read else {
            continue;
        };
        counters.cross_process_read_checks = counters.cross_process_read_checks.saturating_add(1);
        match observed {
            Some(row)
                if row.owner == progress.worker_id
                    && row.seq == progress.last_seq
                    && row.payload == progress.payload
                    && !row.deleted =>
            {
                return Ok(());
            }
            Some(row) => {
                return Err(format!(
                    "cross-process visibility mismatch: progress={progress:?}, row={row:?}"
                ));
            }
            None => {
                return Err(format!(
                    "cross-process visibility returned zero rows for progress={progress:?}"
                ));
            }
        }
    }
    Ok(())
}

fn verify_deleted_row_absent_or_matching(
    conn: &Connection,
    config: &RunConfig,
    deleted_id: i64,
    counters: &mut WorkerCounters,
) -> HarnessResult<()> {
    let _ = query_pk(conn, config, counters, deleted_id)?;
    Ok(())
}

fn query_pk(
    conn: &Connection,
    config: &RunConfig,
    counters: &mut WorkerCounters,
    id: i64,
) -> HarnessResult<Option<ObservedRow>> {
    let rows = retry_fsqlite(config, counters, "pk lookup", || {
        fsqlite_e2e::block_on(conn.query_with_params(
            "SELECT id, owner, seq, payload, deleted FROM swarm_rows WHERE id = ?1",
            &[SqliteValue::Integer(id)],
        ))
    })?;
    counters.pk_selects = counters.pk_selects.saturating_add(1);
    counters.wrong_row_checks = counters.wrong_row_checks.saturating_add(1);
    if rows.len() > 1 {
        return Err(format!(
            "primary-key lookup for id={id} returned {} rows",
            rows.len()
        ));
    }
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let observed = observed_row(row)?;
    if observed.id != id {
        return Err(format!(
            "silent wrong-row return: queried id={id}, got id={}",
            observed.id
        ));
    }
    Ok(Some(observed))
}

fn query_progress(
    conn: &Connection,
    config: &RunConfig,
    counters: &mut WorkerCounters,
    worker_id: usize,
) -> HarnessResult<ProgressRow> {
    let worker = i64::try_from(worker_id).map_err(|err| format!("worker id overflow: {err}"))?;
    let rows = retry_fsqlite(config, counters, "worker progress lookup", || {
        fsqlite_e2e::block_on(conn.query_with_params(
            "SELECT worker_id, last_id, last_seq, payload \
             FROM worker_progress WHERE worker_id = ?1",
            &[SqliteValue::Integer(worker)],
        ))
    })?;
    if rows.len() != 1 {
        return Err(format!(
            "worker_progress lookup for worker={worker_id} returned {} rows",
            rows.len()
        ));
    }
    progress_row(&rows[0])
}

fn emit_op_jsonl(
    writer: &mut std::io::BufWriter<File>,
    config: &RunConfig,
    child: &ChildConfig,
    op_id: u64,
    op_type: &str,
    outcome: &str,
    committed: &CommittedWrite,
) {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let line = serde_json::json!({
        "ts_unix_nanos": ts,
        "level": "INFO",
        "target": "fsqlite_e2e::swarm",
        "run_id": format!("swarm-seed-{}", config.seed),
        "trace_id": config.seed,
        "workspace_id": "fsqlite-swarm",
        "host": hostname(),
        "process_id": child.worker_id,
        "op_id": op_id,
        "op_type": op_type,
        "outcome": outcome,
        "row_id": committed.id,
        "seq": committed.seq,
    });
    let _ = writeln!(writer, "{line}");
}

fn hostname() -> &'static str {
    static HOST: OnceLock<String> = OnceLock::new();
    HOST.get_or_init(|| {
        fs::read_to_string("/etc/hostname")
            .map(|h| h.trim().to_owned())
            .unwrap_or_else(|_| format!("pid-{}", std::process::id()))
    })
}

fn trace_swarm_post_commit_visibility(
    conn: &Connection,
    config: &RunConfig,
    worker_id: usize,
    id: i64,
    seq: i64,
    payload: &str,
) {
    if !group_commit_trace_enabled() {
        return;
    }

    let same_conn_count = count_rows_by_id(conn, id);
    let progress_last_id = count_progress_last_id(conn, worker_id);
    let fresh_conn_count = if let Some(db_path) = config.db_path.as_ref() {
        (|| -> HarnessResult<i64> {
            let fresh = open_fsqlite(db_path)?;
            configure_fsqlite(&fresh, config)?;
            count_rows_by_id(&fresh, id)
        })()
    } else {
        Err("missing db path".to_owned())
    };
    eprintln!(
        "[fsqlite_swarm_commit] worker_id={worker_id} seq={seq} id={id} payload={payload} same_conn_count={same_conn_count:?} progress_last_id={progress_last_id:?} fresh_conn_count={fresh_conn_count:?}"
    );
}

fn count_rows_by_id(conn: &Connection, id: i64) -> HarnessResult<i64> {
    let rows = fsqlite_e2e::block_on(conn.query_with_params(
        "SELECT COUNT(*) FROM swarm_rows WHERE id = ?1",
        &[SqliteValue::Integer(id)],
    ))
    .map_err(|err| format!("count by id failed for id={id}: {err}"))?;
    if rows.len() != 1 {
        return Err(format!(
            "count by id for id={id} returned {} rows",
            rows.len()
        ));
    }
    value_i64(&rows[0], 0)
}

fn count_progress_last_id(conn: &Connection, worker_id: usize) -> HarnessResult<Option<i64>> {
    let worker = i64::try_from(worker_id).map_err(|err| format!("worker id overflow: {err}"))?;
    let rows = fsqlite_e2e::block_on(conn.query_with_params(
        "SELECT last_id FROM worker_progress WHERE worker_id = ?1",
        &[SqliteValue::Integer(worker)],
    ))
    .map_err(|err| format!("progress last_id query failed for worker={worker_id}: {err}"))?;
    match rows.len() {
        0 => Ok(None),
        1 => value_i64(&rows[0], 0).map(Some),
        n => Err(format!(
            "progress last_id query for worker={worker_id} returned {n} rows"
        )),
    }
}

fn with_consistent_read_transaction<T, F>(
    conn: &Connection,
    config: &RunConfig,
    counters: &mut WorkerCounters,
    label: &str,
    read: F,
) -> HarnessResult<T>
where
    F: FnOnce(&mut WorkerCounters) -> HarnessResult<T>,
{
    retry_fsqlite(config, counters, label, || {
        fsqlite_e2e::block_on(conn.execute("BEGIN DEFERRED;")).map(|_| ())
    })?;
    match read(counters) {
        Ok(value) => {
            if let Err(error) = fsqlite_e2e::block_on(conn.commit_transaction()) {
                drop(fsqlite_e2e::block_on(conn.rollback_transaction()));
                return Err(format!("{label} commit failed: {error}"));
            }
            Ok(value)
        }
        Err(error) => {
            drop(fsqlite_e2e::block_on(conn.rollback_transaction()));
            Err(error)
        }
    }
}

fn retry_fsqlite<T, F>(
    config: &RunConfig,
    counters: &mut WorkerCounters,
    label: &str,
    mut op: F,
) -> HarnessResult<T>
where
    F: FnMut() -> Result<T, FrankenError>,
{
    let started = Instant::now();
    let budget = retry_budget(config);
    let mut attempt = 0_u32;
    loop {
        match op() {
            Ok(value) => return Ok(value),
            Err(error) if is_transient(&error) => {
                counters.busy_errors = counters.busy_errors.saturating_add(1);
                if started.elapsed() >= budget {
                    counters.bounded_retry_exhaustions =
                        counters.bounded_retry_exhaustions.saturating_add(1);
                    return Err(format!(
                        "{label} exhausted bounded busy retry budget after {} ms: {error}",
                        duration_to_u64_ms(started.elapsed())
                    ));
                }
                attempt = attempt.saturating_add(1);
                counters.busy_retries = counters.busy_retries.saturating_add(1);
                thread::sleep(backoff_duration(attempt));
            }
            Err(error) => return Err(format!("{label} failed: {error}")),
        }
    }
}

fn initialize_database(db_path: &Path, config: &RunConfig) -> HarnessResult<()> {
    let conn = open_fsqlite(db_path)?;
    configure_fsqlite(&conn, config)?;
    fsqlite_e2e::block_on(conn.execute_batch(
        "CREATE TABLE swarm_rows (
            id INTEGER PRIMARY KEY,
            owner INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            payload TEXT NOT NULL,
            touched_by INTEGER NOT NULL,
            generation INTEGER NOT NULL,
            deleted INTEGER NOT NULL
        );
        CREATE TABLE worker_progress (
            worker_id INTEGER PRIMARY KEY,
            last_id INTEGER NOT NULL,
            last_seq INTEGER NOT NULL,
            payload TEXT NOT NULL,
            observed_epoch INTEGER NOT NULL
        );",
    ))
    .map_err(|err| format!("failed to create swarm schema: {err}"))?;

    for worker_id in 0..config.workers {
        fsqlite_e2e::block_on(conn.execute_with_params(
            "INSERT INTO worker_progress \
             (worker_id, last_id, last_seq, payload, observed_epoch) \
             VALUES (?1, 0, 0, '', 0)",
            &[SqliteValue::Integer(
                i64::try_from(worker_id).map_err(|err| format!("worker id overflow: {err}"))?,
            )],
        ))
        .map_err(|err| format!("failed to seed progress row for worker {worker_id}: {err}"))?;
    }

    for offset in 0..DEFAULT_HOT_ROWS {
        let id = HOT_ROW_BASE - offset;
        fsqlite_e2e::block_on(conn.execute_with_params(
            "INSERT INTO swarm_rows \
             (id, owner, seq, payload, touched_by, generation, deleted) \
             VALUES (?1, -1, ?2, ?3, -1, 0, 0)",
            &[
                SqliteValue::Integer(id),
                SqliteValue::Integer(offset),
                SqliteValue::Text(format!("hot-row-{offset}").into()),
            ],
        ))
        .map_err(|err| format!("failed to seed hot row {id}: {err}"))?;
    }
    Ok(())
}

fn run_sequential_open_check(run_dir: &Path, config: &RunConfig) -> HarnessResult<String> {
    let path = run_dir.join("sequential_open_fresh.db");
    for index in 0..config.resolved_open_checks() {
        let conn = open_fsqlite(&path)?;
        configure_fsqlite(&conn, config)?;
        if !conn.is_concurrent_mode_default() {
            return Err(format!(
                "open #{index} did not preserve concurrent_mode default ON"
            ));
        }
        if index == 0 {
            fsqlite_e2e::block_on(
                conn.execute("CREATE TABLE open_check (id INTEGER PRIMARY KEY, v TEXT);"),
            )
            .map_err(|err| format!("sequential open schema create failed: {err}"))?;
        }
        fsqlite_e2e::block_on(conn.execute_with_params(
            "INSERT INTO open_check (id, v) VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(
                    i64::try_from(index).map_err(|err| format!("open index overflow: {err}"))?,
                ),
                SqliteValue::Text(format!("open-{index}").into()),
            ],
        ))
        .map_err(|err| format!("sequential open insert #{index} failed: {err}"))?;
    }
    Ok(format!(
        "{} sequential Connection::open calls succeeded on fresh file",
        config.resolved_open_checks()
    ))
}

fn spawn_workers(
    config: &RunConfig,
    db_path: &Path,
    run_dir: &Path,
    start_at_ms: u64,
) -> HarnessResult<Vec<RunningWorker>> {
    let exe = env::current_exe().map_err(|err| format!("failed to resolve current exe: {err}"))?;
    let direct_output = group_commit_trace_enabled();
    let mut workers = Vec::with_capacity(config.workers);
    for worker_id in 0..config.workers {
        let report_path = run_dir.join(format!("worker_{worker_id}.json"));
        let jsonl_path = run_dir.join(format!("worker_{worker_id}.jsonl"));
        let stdout_path = run_dir.join(format!("worker_{worker_id}.stdout.txt"));
        let stderr_path = run_dir.join(format!("worker_{worker_id}.stderr.txt"));
        let mut command = Command::new(&exe);
        command
            .arg("--child")
            .arg("--workers")
            .arg(config.workers.to_string())
            .arg("--seconds")
            .arg(config.seconds.to_string());
        if let Some(iters) = config.iters {
            command.arg("--iters").arg(iters.to_string());
        }
        command
            .arg("--busy-timeout-ms")
            .arg(config.busy_timeout_ms.to_string())
            .arg("--seed")
            .arg(config.seed.to_string())
            .arg("--db")
            .arg(db_path)
            .arg("--worker-id")
            .arg(worker_id.to_string())
            .arg("--start-at-ms")
            .arg(start_at_ms.to_string())
            .arg("--child-report")
            .arg(&report_path)
            .arg("--jsonl-path")
            .arg(&jsonl_path);
        if direct_output {
            let stdout_file = File::create(&stdout_path)
                .map_err(|err| format!("failed to create `{}`: {err}", stdout_path.display()))?;
            let stderr_file = File::create(&stderr_path)
                .map_err(|err| format!("failed to create `{}`: {err}", stderr_path.display()))?;
            command
                .stdout(Stdio::from(stdout_file))
                .stderr(Stdio::from(stderr_file));
        } else {
            command.stdout(Stdio::piped()).stderr(Stdio::piped());
        }
        let child = command
            .spawn()
            .map_err(|err| format!("failed to spawn worker {worker_id}: {err}"))?;
        workers.push(RunningWorker {
            worker_id,
            report_path,
            stdout_path,
            stderr_path,
            direct_output,
            child,
            started: Instant::now(),
        });
    }
    Ok(workers)
}

struct RunningWorker {
    worker_id: usize,
    report_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    direct_output: bool,
    child: std::process::Child,
    started: Instant,
}

fn collect_workers(
    workers: Vec<RunningWorker>,
    config: &RunConfig,
    run_dir: &Path,
) -> HarnessResult<Vec<WorkerProcessReport>> {
    let timeout = Duration::from_millis(
        config
            .seconds
            .saturating_mul(1_000)
            .saturating_add(config.busy_timeout_ms)
            .saturating_add(PARENT_TIMEOUT_GRACE_MS),
    );
    let mut reports = Vec::with_capacity(workers.len());
    for worker in workers {
        reports.push(collect_worker(worker, timeout, run_dir)?);
    }
    reports.sort_by_key(|report| report.worker_id);
    Ok(reports)
}

fn collect_worker(
    mut worker: RunningWorker,
    timeout: Duration,
    run_dir: &Path,
) -> HarnessResult<WorkerProcessReport> {
    let mut killed = false;
    loop {
        if worker.started.elapsed() >= timeout {
            killed = true;
            worker.child.kill().map_err(|err| {
                format!(
                    "failed to kill timed-out worker {}: {err}",
                    worker.worker_id
                )
            })?;
            break;
        }
        if worker
            .child
            .try_wait()
            .map_err(|err| format!("failed to poll worker {}: {err}", worker.worker_id))?
            .is_some()
        {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let (exit_code, stdout, stderr) = if worker.direct_output {
        let status = worker.child.wait().map_err(|err| {
            format!(
                "failed to collect worker {} status: {err}",
                worker.worker_id
            )
        })?;
        let stdout = read_lossy_file(&worker.stdout_path)?;
        let stderr = read_lossy_file(&worker.stderr_path)?;
        (status.code(), stdout, stderr)
    } else {
        let output = worker.child.wait_with_output().map_err(|err| {
            format!(
                "failed to collect worker {} output: {err}",
                worker.worker_id
            )
        })?;
        write_worker_process_output(run_dir, worker.worker_id, &output)?;
        (
            output.status.code(),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    };
    let duration_ms = duration_to_u64_ms(worker.started.elapsed());
    let (report, report_error) = read_worker_report(&worker.report_path);
    Ok(WorkerProcessReport {
        worker_id: worker.worker_id,
        exit_code,
        killed_for_timeout: killed,
        duration_ms,
        report_path: worker.report_path.to_string_lossy().into_owned(),
        stdout_snippet: truncate(&stdout, 4_096),
        stderr_snippet: truncate(&stderr, 4_096),
        report,
        report_error,
    })
}

fn read_lossy_file(path: &Path) -> HarnessResult<String> {
    let bytes =
        fs::read(path).map_err(|err| format!("failed to read `{}`: {err}", path.display()))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn read_worker_report(path: &Path) -> (Option<WorkerReport>, Option<String>) {
    match fs::read_to_string(path) {
        Ok(json) => match serde_json::from_str::<WorkerReport>(&json) {
            Ok(report) => (Some(report), None),
            Err(err) => (None, Some(format!("failed to parse worker report: {err}"))),
        },
        Err(err) => (None, Some(format!("failed to read worker report: {err}"))),
    }
}

fn write_worker_process_output(
    run_dir: &Path,
    worker_id: usize,
    output: &Output,
) -> HarnessResult<()> {
    let stdout_path = run_dir.join(format!("worker_{worker_id}.stdout.txt"));
    let stderr_path = run_dir.join(format!("worker_{worker_id}.stderr.txt"));
    fs::write(&stdout_path, &output.stdout)
        .map_err(|err| format!("failed to write `{}`: {err}", stdout_path.display()))?;
    fs::write(&stderr_path, &output.stderr)
        .map_err(|err| format!("failed to write `{}`: {err}", stderr_path.display()))?;
    Ok(())
}

fn validate_wal_shape(db_path: &Path) -> HarnessResult<String> {
    let wal_path = wal_path(db_path);
    if !wal_path.exists() {
        return Ok(
            "WAL file absent after workload; checkpoint/drop may have removed it".to_owned(),
        );
    }
    let metadata = fs::metadata(&wal_path)
        .map_err(|err| format!("failed to stat WAL `{}`: {err}", wal_path.display()))?;
    let len = metadata.len();
    if len == 0 {
        return Ok("WAL file exists and is empty".to_owned());
    }
    if len < 32 {
        return Err(format!(
            "short WAL header read: `{}` is {len} bytes",
            wal_path.display()
        ));
    }

    let mut file = File::open(&wal_path)
        .map_err(|err| format!("failed to open WAL `{}`: {err}", wal_path.display()))?;
    let mut header = [0_u8; 32];
    file.read_exact(&mut header)
        .map_err(|err| format!("short WAL header read from `{}`: {err}", wal_path.display()))?;
    let page_size = u64::from(read_u32_be(&header[8..12]));
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        return Err(format!(
            "invalid WAL page size {page_size} in `{}`",
            wal_path.display()
        ));
    }
    let frame_size = page_size.saturating_add(24);
    let payload_len = len.saturating_sub(32);
    if payload_len % frame_size != 0 {
        return Err(format!(
            "WAL frame-order/size anomaly: payload bytes {payload_len} not divisible by frame size {frame_size}"
        ));
    }
    let frames = payload_len / frame_size;
    for frame_index in 0..frames {
        let offset = 32 + frame_index.saturating_mul(frame_size);
        file.seek(SeekFrom::Start(offset))
            .map_err(|err| format!("failed to seek WAL frame {frame_index}: {err}"))?;
        let mut frame_header = [0_u8; 24];
        file.read_exact(&mut frame_header)
            .map_err(|err| format!("short WAL frame header read at frame {frame_index}: {err}"))?;
        let page_number = read_u32_be(&frame_header[0..4]);
        if page_number == 0 {
            return Err(format!("WAL frame {frame_index} has page number 0"));
        }
    }
    Ok(format!(
        "WAL shape valid: {len} bytes, page_size={page_size}, frames={frames}"
    ))
}

fn run_fsqlite_checkpoint(db_path: &Path, config: &RunConfig) -> HarnessResult<String> {
    let conn = open_fsqlite(db_path)?;
    configure_fsqlite(&conn, config)?;
    let rows = fsqlite_e2e::block_on(conn.query("PRAGMA wal_checkpoint(TRUNCATE);"))
        .map_err(|err| format!("fsqlite wal_checkpoint(TRUNCATE) failed: {err}"))?;
    Ok(format!("fsqlite checkpoint returned {} row(s)", rows.len()))
}

fn run_fsqlite_integrity_check(db_path: &Path, config: &RunConfig) -> HarnessResult<String> {
    let conn = open_fsqlite(db_path)?;
    configure_fsqlite(&conn, config)?;
    let rows = fsqlite_e2e::block_on(conn.query("PRAGMA integrity_check;"))
        .map_err(|err| format!("fsqlite integrity_check failed: {err}"))?;
    let messages = integrity_messages_from_fsqlite_rows(&rows)?;
    if messages == ["ok"] {
        Ok("fsqlite PRAGMA integrity_check returned ok".to_owned())
    } else {
        Err(format!(
            "fsqlite PRAGMA integrity_check returned diagnostics: {messages:?}"
        ))
    }
}

fn run_sqlite_integrity_check(db_path: &Path, config: &RunConfig) -> HarnessResult<String> {
    let conn = rusqlite::Connection::open(db_path)
        .map_err(|err| format!("stock SQLite open failed: {err}"))?;
    conn.execute_batch(&format!(
        "PRAGMA busy_timeout={}; PRAGMA wal_checkpoint(TRUNCATE);",
        config.busy_timeout_ms
    ))
    .map_err(|err| format!("stock SQLite checkpoint failed: {err}"))?;
    let message: String = conn
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .map_err(|err| format!("stock SQLite integrity_check failed: {err}"))?;
    if message == "ok" {
        Ok("stock SQLite PRAGMA integrity_check returned ok".to_owned())
    } else {
        Err(format!(
            "stock SQLite PRAGMA integrity_check returned `{message}`"
        ))
    }
}

fn csqlite_cross_check(db_path: &Path, run_dir: &Path) -> HarnessResult<String> {
    let (report, artifact) = verify_concurrency_artifact(db_path)
        .map_err(|err| format!("csqlite cross-check failed: {err}"))?;

    if let Some(ref art) = artifact {
        let artifact_dir = run_dir.join("verify_artifacts");
        if let Err(e) = write_artifact_bundle(art, &artifact_dir, "swarm") {
            eprintln!("[swarm] WARNING: failed to write verify artifact: {e}");
        }
    }

    if report.ok {
        Ok(format!(
            "csqlite cross-check PASSED: {} pages, {} tables, {:.1}ms",
            report.page_count, report.table_count, report.timings.total_ms
        ))
    } else {
        Err(format!(
            "csqlite cross-check FAILED: quick_check={}, integrity_check={}, wal_checkpoint={}",
            report.quick_check, report.integrity_check, report.wal_checkpoint
        ))
    }
}

fn verify_final_progress_visibility(db_path: &Path, config: &RunConfig) -> HarnessResult<String> {
    let conn = open_fsqlite(db_path)?;
    configure_fsqlite(&conn, config)?;
    let mut committed_workers = 0_usize;
    let mut counters = WorkerCounters::default();
    with_consistent_read_transaction(
        &conn,
        config,
        &mut counters,
        "final progress consistency snapshot",
        |counters| {
            let rows = fsqlite_e2e::block_on(
                conn.query("SELECT worker_id, last_id, last_seq, payload FROM worker_progress;"),
            )
            .map_err(|err| format!("failed to query final worker_progress: {err}"))?;
            if rows.len() != config.workers {
                return Err(format!(
                    "worker_progress has {} rows, expected {}",
                    rows.len(),
                    config.workers
                ));
            }

            for row in &rows {
                let progress = progress_row(row)?;
                if progress.last_id == 0 {
                    continue;
                }
                committed_workers = committed_workers.saturating_add(1);
                let observed = query_pk(&conn, config, counters, progress.last_id)?;
                match observed {
                    Some(found)
                        if found.owner == progress.worker_id
                            && found.seq == progress.last_seq
                            && found.payload == progress.payload
                            && !found.deleted => {}
                    Some(found) => {
                        return Err(format!(
                            "final progress visibility mismatch: progress={progress:?}, row={found:?}"
                        ));
                    }
                    None => {
                        return Err(format!(
                            "final progress row points at missing committed row: {progress:?}"
                        ));
                    }
                }
            }

            if committed_workers != config.workers {
                return Err(format!(
                    "only {committed_workers}/{} workers published committed progress",
                    config.workers
                ));
            }
            Ok(())
        },
    )?;

    Ok(format!(
        "all {committed_workers} worker progress rows point at visible committed rows"
    ))
}

fn collect_row_counts(db_path: &Path, config: &RunConfig) -> RowCounts {
    let mut counts = RowCounts::default();
    match open_fsqlite(db_path).and_then(|conn| {
        configure_fsqlite(&conn, config)?;
        Ok(conn)
    }) {
        Ok(conn) => {
            counts.total_rows = query_count(&conn, "SELECT COUNT(*) FROM swarm_rows;")
                .map_err(|err| counts.errors.push(err))
                .ok();
            counts.live_rows =
                query_count(&conn, "SELECT COUNT(*) FROM swarm_rows WHERE deleted = 0;")
                    .map_err(|err| counts.errors.push(err))
                    .ok();
            counts.progress_rows = query_count(&conn, "SELECT COUNT(*) FROM worker_progress;")
                .map_err(|err| counts.errors.push(err))
                .ok();
            counts.committed_workers = query_count(
                &conn,
                "SELECT COUNT(*) FROM worker_progress WHERE last_id != 0;",
            )
            .map_err(|err| counts.errors.push(err))
            .ok();
        }
        Err(err) => counts.errors.push(err),
    }
    counts
}

fn query_count(conn: &Connection, sql: &str) -> HarnessResult<i64> {
    let rows = fsqlite_e2e::block_on(conn.query(sql))
        .map_err(|err| format!("count query `{sql}` failed: {err}"))?;
    if rows.len() != 1 {
        return Err(format!("count query `{sql}` returned {} rows", rows.len()));
    }
    value_i64(&rows[0], 0)
}

fn timed_criterion<F>(name: &'static str, f: F) -> CriterionReport
where
    F: FnOnce() -> HarnessResult<String>,
{
    let started = Instant::now();
    match f() {
        Ok(detail) => CriterionReport {
            name,
            pass: true,
            duration_ms: duration_to_u64_ms(started.elapsed()),
            detail,
        },
        Err(error) => CriterionReport {
            name,
            pass: false,
            duration_ms: duration_to_u64_ms(started.elapsed()),
            detail: error,
        },
    }
}

fn worker_process_criterion(workers: &[WorkerProcessReport]) -> CriterionReport {
    let failures: Vec<String> = workers
        .iter()
        .filter_map(|worker| {
            if worker_process_passes(worker) {
                None
            } else {
                Some(format!(
                    "worker {} exit={:?} killed={} report_success={:?} report_worker_id={:?} report_schema={:?} report_error={:?}",
                    worker.worker_id,
                    worker.exit_code,
                    worker.killed_for_timeout,
                    worker.report.as_ref().map(|report| report.success),
                    worker.report.as_ref().map(|report| report.worker_id),
                    worker.report.as_ref().map(|report| report.schema.as_str()),
                    worker.report_error
                ))
            }
        })
        .collect();
    CriterionReport {
        name: "worker_processes",
        pass: failures.is_empty(),
        duration_ms: 0,
        detail: if failures.is_empty() {
            format!("{} worker processes exited successfully", workers.len())
        } else {
            failures.join("; ")
        },
    }
}

fn worker_report_identity_matches_process(worker: &WorkerProcessReport) -> bool {
    worker.report.as_ref().is_some_and(|report| {
        report.worker_id == worker.worker_id && report.schema == WORKER_REPORT_SCHEMA_V1
    })
}

fn worker_report_success_matches_process(worker: &WorkerProcessReport) -> bool {
    worker_report_identity_matches_process(worker)
        && worker.report.as_ref().is_some_and(|report| report.success)
}

fn worker_process_passes(worker: &WorkerProcessReport) -> bool {
    !worker.killed_for_timeout
        && worker.exit_code == Some(0)
        && worker_report_success_matches_process(worker)
}

fn read_your_own_write_criterion(workers: &[WorkerProcessReport]) -> CriterionReport {
    worker_bool_criterion(
        "read_your_own_write",
        workers,
        |report| report.read_your_own_write_pass && report.own_read_checks > 0,
        |report| report.own_read_checks,
    )
}

fn cross_process_visibility_criterion(
    workers: &[WorkerProcessReport],
    final_visibility: &CriterionReport,
    worker_count: usize,
) -> CriterionReport {
    let observed_workers = workers.len();
    let worker_pass = observed_workers == worker_count && workers.iter().all(worker_process_passes);
    if worker_count <= 1 {
        return CriterionReport {
            name: "cross_process_visibility",
            pass: worker_pass && final_visibility.pass,
            duration_ms: final_visibility.duration_ms,
            detail: format!(
                "single-worker run has no peer visibility checks; worker reports pass={worker_pass}; observed_workers={observed_workers}/{worker_count}; final visibility: {}",
                final_visibility.detail
            ),
        };
    }
    let peer_checks = workers
        .iter()
        .filter_map(|worker| worker.report.as_ref())
        .fold(0_u64, |total, report| {
            total.saturating_add(report.cross_process_read_checks)
        });
    CriterionReport {
        name: "cross_process_visibility",
        pass: worker_pass && final_visibility.pass,
        duration_ms: final_visibility.duration_ms,
        detail: format!(
            "worker peer checks pass={worker_pass}; observed_workers={observed_workers}/{worker_count}; peer_checks={peer_checks}; final visibility: {}",
            final_visibility.detail
        ),
    }
}

fn wrong_row_criterion(workers: &[WorkerProcessReport]) -> CriterionReport {
    worker_bool_criterion(
        "wrong_row_returns",
        workers,
        |report| report.wrong_row_returns_pass && report.wrong_row_checks > 0,
        |report| report.wrong_row_checks,
    )
}

fn busy_timeout_criterion(workers: &[WorkerProcessReport]) -> CriterionReport {
    let failures: Vec<String> = workers
        .iter()
        .filter_map(|worker| {
            let report_pass = worker
                .report
                .as_ref()
                .is_some_and(|report| {
                    worker_report_identity_matches_process(worker)
                        && report.busy_timeout_honored_pass
                });
            if !worker.killed_for_timeout && report_pass {
                None
            } else {
                Some(format!(
                    "worker {} killed={} busy_pass={report_pass} report_worker_id={:?} report_schema={:?}",
                    worker.worker_id,
                    worker.killed_for_timeout,
                    worker.report.as_ref().map(|report| report.worker_id),
                    worker.report.as_ref().map(|report| report.schema.as_str())
                ))
            }
        })
        .collect();
    let retries: u64 = workers
        .iter()
        .filter_map(|worker| worker.report.as_ref())
        .map(|report| report.busy_retries)
        .sum();
    CriterionReport {
        name: "busy_timeout",
        pass: failures.is_empty(),
        duration_ms: 0,
        detail: if failures.is_empty() {
            format!("bounded busy retry honored; total_busy_retries={retries}")
        } else {
            failures.join("; ")
        },
    }
}

fn wal_corruption_criterion(
    wal_shape: &CriterionReport,
    wal_checkpoint: &CriterionReport,
) -> CriterionReport {
    CriterionReport {
        name: "wal_corruption",
        pass: wal_shape.pass && wal_checkpoint.pass,
        duration_ms: wal_shape
            .duration_ms
            .saturating_add(wal_checkpoint.duration_ms),
        detail: format!(
            "shape_pass={} checkpoint_pass={}; {}; {}",
            wal_shape.pass, wal_checkpoint.pass, wal_shape.detail, wal_checkpoint.detail
        ),
    }
}

fn worker_bool_criterion<P, C>(
    name: &'static str,
    workers: &[WorkerProcessReport],
    predicate: P,
    count: C,
) -> CriterionReport
where
    P: Fn(&WorkerReport) -> bool,
    C: Fn(&WorkerReport) -> u64,
{
    let mut total_checks = 0_u64;
    let mut failures = Vec::new();
    for worker in workers {
        match &worker.report {
            Some(report) if !worker_report_identity_matches_process(worker) => {
                failures.push(format!(
                    "worker {} invalid report identity for {name}: report_worker_id={} report_schema={}",
                    worker.worker_id, report.worker_id, report.schema
                ));
            }
            Some(report) if predicate(report) => {
                total_checks = total_checks.saturating_add(count(report));
            }
            Some(report) => failures.push(format!(
                "worker {} failed {name}: checks={} failure={:?}",
                worker.worker_id,
                count(report),
                report.failure
            )),
            None => failures.push(format!(
                "worker {} missing report: {:?}",
                worker.worker_id, worker.report_error
            )),
        }
    }
    CriterionReport {
        name,
        pass: failures.is_empty(),
        duration_ms: 0,
        detail: if failures.is_empty() {
            format!("{total_checks} checks passed")
        } else {
            failures.join("; ")
        },
    }
}

fn validate_worker_jsonl(run_dir: &Path, workers: usize) -> HarnessResult<String> {
    let mut total_lines = 0_u64;
    let mut invalid_lines = 0_u64;
    let required_fields = [
        "ts_unix_nanos",
        "level",
        "target",
        "run_id",
        "trace_id",
        "workspace_id",
        "host",
        "process_id",
        "op_id",
        "op_type",
        "outcome",
    ];
    for worker_id in 0..workers {
        let jsonl_path = run_dir.join(format!("worker_{worker_id}.jsonl"));
        if !jsonl_path.exists() {
            continue;
        }
        let content = fs::read_to_string(&jsonl_path)
            .map_err(|err| format!("failed to read `{}`: {err}", jsonl_path.display()))?;
        for line in content.lines().filter(|l| !l.trim().is_empty()) {
            total_lines += 1;
            match serde_json::from_str::<serde_json::Value>(line) {
                Ok(obj) => {
                    for field in &required_fields {
                        if obj.get(*field).is_none() {
                            invalid_lines += 1;
                            break;
                        }
                    }
                }
                Err(_) => {
                    invalid_lines += 1;
                }
            }
        }
    }
    if invalid_lines > 0 {
        return Err(format!(
            "{invalid_lines}/{total_lines} JSONL lines failed schema validation"
        ));
    }
    Ok(format!("{total_lines} JSONL lines validated, zero invalid"))
}

fn copy_forensics(db_path: &Path, run_dir: &Path) -> HarnessResult<PathBuf> {
    let forensic_dir = run_dir
        .join("forensics")
        .join(timestamp_dir_name("failure"));
    fs::create_dir_all(&forensic_dir).map_err(|err| {
        format!(
            "failed to create forensics directory `{}`: {err}",
            forensic_dir.display()
        )
    })?;
    for source in [db_path.to_path_buf(), wal_path(db_path), shm_path(db_path)] {
        if source.exists() {
            let file_name = source
                .file_name()
                .ok_or_else(|| format!("source path `{}` has no file name", source.display()))?;
            let destination = forensic_dir.join(file_name);
            fs::copy(&source, &destination).map_err(|err| {
                format!(
                    "failed to copy `{}` to `{}`: {err}",
                    source.display(),
                    destination.display()
                )
            })?;
        }
    }
    // Copy per-worker JSONL logs and reports into forensics
    if let Ok(entries) = fs::read_dir(run_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".jsonl") || name_str.ends_with(".json") {
                let destination = forensic_dir.join(&name);
                let _ = fs::copy(entry.path(), destination);
            }
        }
    }
    Ok(forensic_dir)
}

fn write_report_artifact(run_dir: &Path, report: &SwarmReport) -> HarnessResult<()> {
    let path = run_dir.join("report.json");
    let json = serde_json::to_string_pretty(report)
        .map_err(|err| format!("failed to serialize parent report: {err}"))?;
    fs::write(&path, json).map_err(|err| format!("failed to write `{}`: {err}", path.display()))
}

fn print_json<T: Serialize>(value: &T) -> HarnessResult<()> {
    let json = serde_json::to_string_pretty(value)
        .map_err(|err| format!("failed to serialize JSON report: {err}"))?;
    println!("{json}");
    Ok(())
}

fn parse_args() -> HarnessResult<Mode> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut config = RunConfig::default();
    let mut child_mode = false;
    let mut worker_id = None;
    let mut start_at_ms = None;
    let mut child_report = None;
    let mut jsonl_path = None;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "--help" | "-h" => return Err(usage()),
            "--child" => child_mode = true,
            "--workers" => {
                index = index.saturating_add(1);
                config.workers = parse_required(&args, index, "--workers")?;
            }
            "--seconds" => {
                index = index.saturating_add(1);
                config.seconds = parse_required(&args, index, "--seconds")?;
            }
            "--iters" => {
                index = index.saturating_add(1);
                config.iters = Some(parse_required(&args, index, "--iters")?);
            }
            "--busy-timeout-ms" => {
                index = index.saturating_add(1);
                config.busy_timeout_ms = parse_required(&args, index, "--busy-timeout-ms")?;
            }
            "--seed" => {
                index = index.saturating_add(1);
                config.seed = parse_required(&args, index, "--seed")?;
            }
            "--db" => {
                index = index.saturating_add(1);
                config.db_path = Some(PathBuf::from(parse_required_string(&args, index, "--db")?));
            }
            "--artifact-root" => {
                index = index.saturating_add(1);
                config.artifact_root =
                    PathBuf::from(parse_required_string(&args, index, "--artifact-root")?);
            }
            "--open-checks" => {
                index = index.saturating_add(1);
                config.open_checks = Some(parse_required(&args, index, "--open-checks")?);
            }
            "--worker-id" => {
                index = index.saturating_add(1);
                worker_id = Some(parse_required(&args, index, "--worker-id")?);
            }
            "--start-at-ms" => {
                index = index.saturating_add(1);
                start_at_ms = Some(parse_required(&args, index, "--start-at-ms")?);
            }
            "--child-report" => {
                index = index.saturating_add(1);
                child_report = Some(PathBuf::from(parse_required_string(
                    &args,
                    index,
                    "--child-report",
                )?));
            }
            "--jsonl-path" => {
                index = index.saturating_add(1);
                jsonl_path = Some(PathBuf::from(parse_required_string(
                    &args,
                    index,
                    "--jsonl-path",
                )?));
            }
            _ if arg.starts_with("--workers=") => {
                config.workers = parse_value(arg, "--workers=")?;
            }
            _ if arg.starts_with("--seconds=") => {
                config.seconds = parse_value(arg, "--seconds=")?;
            }
            _ if arg.starts_with("--iters=") => {
                config.iters = Some(parse_value(arg, "--iters=")?);
            }
            _ if arg.starts_with("--busy-timeout-ms=") => {
                config.busy_timeout_ms = parse_value(arg, "--busy-timeout-ms=")?;
            }
            _ if arg.starts_with("--seed=") => {
                config.seed = parse_value(arg, "--seed=")?;
            }
            _ if arg.starts_with("--db=") => {
                config.db_path = Some(PathBuf::from(strip_prefix(arg, "--db=")?));
            }
            _ if arg.starts_with("--artifact-root=") => {
                config.artifact_root = PathBuf::from(strip_prefix(arg, "--artifact-root=")?);
            }
            _ if arg.starts_with("--open-checks=") => {
                config.open_checks = Some(parse_value(arg, "--open-checks=")?);
            }
            _ if arg.starts_with("--worker-id=") => {
                worker_id = Some(parse_value(arg, "--worker-id=")?);
            }
            _ if arg.starts_with("--start-at-ms=") => {
                start_at_ms = Some(parse_value(arg, "--start-at-ms=")?);
            }
            _ if arg.starts_with("--child-report=") => {
                child_report = Some(PathBuf::from(strip_prefix(arg, "--child-report=")?));
            }
            _ if arg.starts_with("--jsonl-path=") => {
                jsonl_path = Some(PathBuf::from(strip_prefix(arg, "--jsonl-path=")?));
            }
            _ => return Err(format!("unknown argument `{arg}`\n{}", usage())),
        }
        index = index.saturating_add(1);
    }

    validate_parent_config(&config)?;
    if child_mode {
        let child = ChildConfig {
            worker_id: worker_id.ok_or_else(|| "child mode requires --worker-id".to_owned())?,
            start_at_ms: start_at_ms
                .ok_or_else(|| "child mode requires --start-at-ms".to_owned())?,
            report_path: child_report
                .ok_or_else(|| "child mode requires --child-report".to_owned())?,
            jsonl_path,
        };
        if child.worker_id >= config.workers {
            return Err(format!(
                "worker_id {} out of range for workers={}",
                child.worker_id, config.workers
            ));
        }
        if config.db_path.is_none() {
            return Err("child mode requires --db".to_owned());
        }
        Ok(Mode::Child { run: config, child })
    } else {
        Ok(Mode::Parent(config))
    }
}

fn validate_parent_config(config: &RunConfig) -> HarnessResult<()> {
    if config.workers == 0 || config.workers > MAX_WORKERS {
        return Err(format!(
            "--workers must be in 1..={MAX_WORKERS}, got {}",
            config.workers
        ));
    }
    if config.seconds == 0 {
        return Err("--seconds must be greater than zero".to_owned());
    }
    if config.iters == Some(0) {
        return Err("--iters must be greater than zero when provided".to_owned());
    }
    if config.busy_timeout_ms == 0 {
        return Err("--busy-timeout-ms must be greater than zero".to_owned());
    }
    if config.resolved_open_checks() == 0 {
        return Err("--open-checks must be greater than zero".to_owned());
    }
    Ok(())
}

fn usage() -> String {
    "usage: swarm-multiprocess [--workers=N] [--seconds=N] \
     [--iters=N] [--busy-timeout-ms=N] [--seed=N] [--db PATH] [--artifact-root PATH]"
        .to_owned()
}

fn parse_required<T>(args: &[String], index: usize, flag: &str) -> HarnessResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    parse_required_string(args, index, flag)?
        .parse::<T>()
        .map_err(|err| {
            format!(
                "invalid value `{}` for {flag}: {err}",
                args.get(index).map_or("", String::as_str)
            )
        })
}

fn parse_required_string(args: &[String], index: usize, flag: &str) -> HarnessResult<String> {
    args.get(index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_value<T>(arg: &str, prefix: &str) -> HarnessResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw = strip_prefix(arg, prefix)?;
    raw.parse::<T>()
        .map_err(|err| format!("invalid value `{raw}` for {prefix}: {err}"))
}

fn strip_prefix<'a>(arg: &'a str, prefix: &str) -> HarnessResult<&'a str> {
    arg.strip_prefix(prefix)
        .ok_or_else(|| format!("argument `{arg}` is missing prefix `{prefix}`"))
}

fn open_fsqlite(db_path: &Path) -> HarnessResult<Connection> {
    let path = db_path
        .to_str()
        .ok_or_else(|| format!("path `{}` is not valid UTF-8", db_path.display()))?;
    fsqlite_e2e::block_on(Connection::open(path.to_owned()))
        .map_err(|err| format!("fsqlite open `{path}` failed: {err}"))
}

fn configure_fsqlite(conn: &Connection, config: &RunConfig) -> HarnessResult<()> {
    fsqlite_e2e::block_on(
        conn.execute(&format!("PRAGMA busy_timeout={};", config.busy_timeout_ms)),
    )
    .map_err(|err| format!("failed to set busy_timeout: {err}"))?;
    fsqlite_e2e::block_on(conn.execute("PRAGMA journal_mode=WAL;"))
        .map_err(|err| format!("failed to set journal_mode=WAL: {err}"))?;
    fsqlite_e2e::block_on(conn.execute("PRAGMA synchronous=NORMAL;"))
        .map_err(|err| format!("failed to set synchronous=NORMAL: {err}"))?;
    fsqlite_e2e::block_on(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;"))
        .map_err(|err| format!("failed to set concurrent_mode=ON: {err}"))?;
    if !conn.is_concurrent_mode_default() {
        return Err("concurrent_mode default is not ON after configuration".to_owned());
    }
    Ok(())
}

fn observed_row(row: &fsqlite::Row) -> HarnessResult<ObservedRow> {
    Ok(ObservedRow {
        id: value_i64(row, 0)?,
        owner: value_i64(row, 1)?,
        seq: value_i64(row, 2)?,
        payload: value_text(row, 3)?,
        deleted: value_i64(row, 4)? != 0,
    })
}

fn progress_row(row: &fsqlite::Row) -> HarnessResult<ProgressRow> {
    Ok(ProgressRow {
        worker_id: value_i64(row, 0)?,
        last_id: value_i64(row, 1)?,
        last_seq: value_i64(row, 2)?,
        payload: value_text(row, 3)?,
    })
}

fn value_i64(row: &fsqlite::Row, index: usize) -> HarnessResult<i64> {
    match row.values().get(index) {
        Some(SqliteValue::Integer(value)) => Ok(*value),
        other => Err(format!(
            "expected integer at column {index}, got {}",
            sqlite_value_debug(other)
        )),
    }
}

fn value_text(row: &fsqlite::Row, index: usize) -> HarnessResult<String> {
    match row.values().get(index) {
        Some(SqliteValue::Text(value)) => Ok(value.to_string()),
        other => Err(format!(
            "expected text at column {index}, got {}",
            sqlite_value_debug(other)
        )),
    }
}

fn sqlite_value_debug(value: Option<&SqliteValue>) -> String {
    match value {
        Some(value) => format!("{value:?}"),
        None => "missing column".to_owned(),
    }
}

fn integrity_messages_from_fsqlite_rows(rows: &[fsqlite::Row]) -> HarnessResult<Vec<String>> {
    rows.iter().map(|row| value_text(row, 0)).collect()
}

fn rows_match(observed: &ObservedRow, expected: &ObservedRow) -> bool {
    observed.id == expected.id
        && observed.owner == expected.owner
        && observed.seq == expected.seq
        && observed.payload == expected.payload
        && observed.deleted == expected.deleted
}

fn row_id_for(worker_id: usize, seq: i64) -> HarnessResult<i64> {
    let worker = i64::try_from(worker_id).map_err(|err| format!("worker id overflow: {err}"))?;
    worker
        .checked_mul(ROW_ID_STRIDE)
        .and_then(|base| base.checked_add(seq))
        .ok_or_else(|| format!("row id overflow for worker={worker_id}, seq={seq}"))
}

fn choose_delete_id(live_ids: &[i64], rng: &mut StdRng) -> Option<i64> {
    if live_ids.len() < 8 || !rng.random_ratio(1, 10) {
        return None;
    }
    let max_index = live_ids.len().saturating_sub(2);
    Some(live_ids[rng.random_range(0..max_index)])
}

fn random_payload(rng: &mut StdRng, worker_id: usize, seq: i64) -> String {
    format!(
        "payload:w{worker_id}:s{seq}:a{:016x}:b{:016x}",
        rng.random::<u64>(),
        rng.random::<u64>()
    )
}

fn is_transient(error: &FrankenError) -> bool {
    matches!(
        error,
        FrankenError::Busy
            | FrankenError::BusyRecovery
            | FrankenError::BusySnapshot { .. }
            | FrankenError::DatabaseLocked { .. }
    )
}

fn retry_budget(config: &RunConfig) -> Duration {
    Duration::from_millis(
        config
            .busy_timeout_ms
            .saturating_mul(2)
            .saturating_add(2_000),
    )
}

fn backoff_duration(attempt: u32) -> Duration {
    let shift = attempt.min(6);
    let millis = 5_u64.saturating_mul(1_u64 << shift).min(250);
    Duration::from_millis(millis)
}

fn wait_until(target_ms: u64) {
    let now = unix_time_ms();
    if target_ms > now {
        thread::sleep(Duration::from_millis(target_ms - now));
    }
}

fn unix_time_ms() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

fn timestamp_dir_name(prefix: &str) -> String {
    format!("{prefix}-{}-pid{}", unix_time_ms(), std::process::id())
}

fn duration_to_u64_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let mut truncated: String = value.chars().take(max_chars).collect();
    truncated.push_str("...");
    truncated
}

fn read_u32_be(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn wal_path(db_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", db_path.to_string_lossy()))
}

fn shm_path(db_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-shm", db_path.to_string_lossy()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn final_visibility(pass: bool) -> CriterionReport {
        CriterionReport {
            name: "final_cross_process_visibility",
            pass,
            duration_ms: 7,
            detail: "final detail".to_owned(),
        }
    }

    fn worker(
        worker_id: usize,
        success: bool,
        cross_process_read_checks: u64,
    ) -> WorkerProcessReport {
        worker_with_report(
            worker_id,
            worker_id,
            WORKER_REPORT_SCHEMA_V1,
            success,
            cross_process_read_checks,
        )
    }

    fn worker_with_report(
        worker_id: usize,
        report_worker_id: usize,
        report_schema: &str,
        success: bool,
        cross_process_read_checks: u64,
    ) -> WorkerProcessReport {
        WorkerProcessReport {
            worker_id,
            exit_code: Some(0),
            killed_for_timeout: false,
            duration_ms: 11,
            report_path: format!("worker_{worker_id}.json"),
            stdout_snippet: String::new(),
            stderr_snippet: String::new(),
            report: Some(WorkerReport {
                schema: report_schema.to_owned(),
                worker_id: report_worker_id,
                pid: 1234,
                success,
                duration_ms: 11,
                iterations: 1,
                transactions_committed: 1,
                inserts: 1,
                updates: 1,
                deletes: 0,
                pk_selects: 1,
                own_read_checks: 1,
                cross_process_read_checks,
                wrong_row_checks: 1,
                busy_errors: 0,
                busy_retries: 0,
                bounded_retry_exhaustions: 0,
                read_your_own_write_pass: true,
                cross_process_visibility_pass: success,
                wrong_row_returns_pass: true,
                busy_timeout_honored_pass: true,
                failure: None,
            }),
            report_error: None,
        }
    }

    #[test]
    fn worker_process_criterion_fails_mismatched_worker_report_identity() {
        let report =
            worker_process_criterion(&[worker_with_report(0, 1, WORKER_REPORT_SCHEMA_V1, true, 3)]);

        assert!(
            !report.pass,
            "worker process criterion must reject a successful report with the wrong worker_id"
        );
        assert!(
            report.detail.contains("report_worker_id=Some(1)"),
            "detail should expose the mismatched worker id: {}",
            report.detail
        );
    }

    #[test]
    fn worker_process_criterion_fails_wrong_worker_report_schema() {
        let report = worker_process_criterion(&[worker_with_report(0, 0, "old-schema", true, 3)]);

        assert!(
            !report.pass,
            "worker process criterion must reject a successful report with the wrong schema"
        );
        assert!(
            report.detail.contains("report_schema=Some(\"old-schema\")"),
            "detail should expose the mismatched schema: {}",
            report.detail
        );
    }

    #[test]
    fn worker_bool_criterion_fails_mismatched_worker_report_identity() {
        let report = read_your_own_write_criterion(&[worker_with_report(
            0,
            7,
            WORKER_REPORT_SCHEMA_V1,
            true,
            3,
        )]);

        assert!(
            !report.pass,
            "generic worker criteria must reject a successful report with the wrong worker_id"
        );
        assert!(
            report.detail.contains("invalid report identity"),
            "detail should expose the identity failure: {}",
            report.detail
        );
    }

    #[test]
    fn busy_timeout_criterion_fails_mismatched_worker_report_identity() {
        let report =
            busy_timeout_criterion(&[worker_with_report(0, 7, WORKER_REPORT_SCHEMA_V1, true, 3)]);

        assert!(
            !report.pass,
            "busy-timeout criterion must reject a report with the wrong worker_id"
        );
        assert!(
            report.detail.contains("report_worker_id=Some(7)"),
            "detail should expose the identity failure: {}",
            report.detail
        );
    }

    #[test]
    fn cross_process_visibility_fails_when_multi_worker_report_set_is_incomplete() {
        let report =
            cross_process_visibility_criterion(&[worker(0, true, 3)], &final_visibility(true), 2);

        assert!(
            !report.pass,
            "cross-process visibility must not pass with a missing worker report"
        );
        assert!(
            report.detail.contains("observed_workers=1/2"),
            "detail should expose missing worker count: {}",
            report.detail
        );
    }

    #[test]
    fn cross_process_visibility_fails_mismatched_worker_report_identity() {
        let report = cross_process_visibility_criterion(
            &[
                worker_with_report(0, 0, WORKER_REPORT_SCHEMA_V1, true, 3),
                worker_with_report(1, 0, WORKER_REPORT_SCHEMA_V1, true, 3),
            ],
            &final_visibility(true),
            2,
        );

        assert!(
            !report.pass,
            "cross-process visibility must reject stale or misattributed worker reports"
        );
        assert!(
            report.detail.contains("worker peer checks pass=false"),
            "detail should expose failed worker validation: {}",
            report.detail
        );
    }

    #[test]
    fn cross_process_visibility_single_worker_requires_successful_worker_report() {
        let report = cross_process_visibility_criterion(&[], &final_visibility(true), 1);

        assert!(
            !report.pass,
            "single-worker visibility must still require the expected worker report"
        );
        assert!(
            report.detail.contains("observed_workers=0/1"),
            "detail should expose missing single-worker report: {}",
            report.detail
        );
        assert!(
            report.detail.contains("final visibility: final detail"),
            "detail should preserve authoritative final visibility context: {}",
            report.detail
        );
    }
}
