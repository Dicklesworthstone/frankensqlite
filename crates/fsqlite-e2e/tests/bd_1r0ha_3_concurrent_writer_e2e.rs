//! Deterministic concurrent-writer e2e matrix with fairness/latency evidence.
//!
//! Bead: bd-1r0ha.3

use std::path::{Path, PathBuf};
use std::time::Duration;

use fsqlite_e2e::benchmark::{BenchmarkConfig, BenchmarkMeta, BenchmarkSummary, run_benchmark};
use fsqlite_e2e::fairness;
use fsqlite_e2e::fsqlite_executor::{FsqliteExecConfig, run_oplog_fsqlite};
use fsqlite_e2e::oplog::{
    ConcurrencyModel, ExpectedResult, OpKind, OpLog, OpLogHeader, OpRecord, RngSpec,
    preset_hot_page_contention,
};
use fsqlite_e2e::report::EngineRunReport;
use fsqlite_e2e::sqlite_executor::{SqliteExecConfig, run_oplog_sqlite};
use fsqlite_e2e::{E2eResult, HarnessSettings};
use fsqlite_types::SqliteValue;
use serde_json::json;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-1r0ha.3";
const REPLAY_COMMAND: &str =
    "cargo test -p fsqlite-e2e --test bd_1r0ha_3_concurrent_writer_e2e -- --nocapture --test-threads=1";

const READERS: u16 = 10;
const WRITERS: u16 = 10;
const TOTAL_WORKERS: u16 = READERS + WRITERS;
const DISJOINT_ROUNDS: u32 = 4;
const HOT_ROUNDS: u32 = 6;
const HOT_ROWS: u32 = 10;

const SCENARIO_RW_DISJOINT: &str = "MVCC-E2E-RW10W10-DISJOINT";
const SCENARIO_HOT_CONTENTION: &str = "MVCC-E2E-HOT-WRITE-CONTENTION";
const SCENARIO_NO_SERIALIZATION: &str = "MVCC-E2E-NO-GLOBAL-SERIALIZATION";

const SEED_RW_DISJOINT: u64 = 61_301;
const SEED_HOT_CONTENTION: u64 = 61_302;
const SEED_NO_SERIALIZATION: u64 = 61_303;

#[derive(Debug, Clone, Copy)]
enum EngineKind {
    Sqlite,
    Fsqlite,
}

impl EngineKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite3",
            Self::Fsqlite => "fsqlite",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct WriterProgress {
    min_commits: u64,
    max_commits: u64,
    total_commits: u64,
    writer_count: usize,
}

fn sqlite_exec_config() -> SqliteExecConfig {
    let settings = HarnessSettings {
        busy_timeout_ms: 0,
        ..fairness::benchmark_settings()
    };
    let mut pragmas = settings.to_sqlite3_pragmas();
    pragmas.extend(fairness::additional_benchmark_pragmas());

    SqliteExecConfig {
        pragmas,
        max_busy_retries: 1_000,
        busy_backoff: Duration::from_micros(200),
        busy_backoff_max: Duration::from_millis(2),
        run_integrity_check: true,
    }
}

fn fsqlite_exec_config() -> FsqliteExecConfig {
    let settings = fairness::benchmark_settings();
    let mut config = settings.to_fsqlite_exec_config();
    config.pragmas.extend(fairness::additional_benchmark_pragmas());
    config
}

fn benchmark_engine(engine: EngineKind, workload_name: &str, oplog: &OpLog) -> BenchmarkSummary {
    let benchmark_config = BenchmarkConfig {
        warmup_iterations: 0,
        min_iterations: 3,
        measurement_time_secs: 0,
    };
    let meta = BenchmarkMeta {
        engine: engine.as_str().to_owned(),
        workload: workload_name.to_owned(),
        fixture_id: BEAD_ID.to_owned(),
        concurrency: oplog.header.concurrency.worker_count,
        cargo_profile: "test".to_owned(),
    };

    match engine {
        EngineKind::Sqlite => {
            let config = sqlite_exec_config();
            run_benchmark(&benchmark_config, &meta, |_| -> E2eResult<EngineRunReport> {
                let temp = tempdir()?;
                let db_path = temp.path().join("sqlite3.db");
                run_oplog_sqlite(&db_path, oplog, &config)
            })
        }
        EngineKind::Fsqlite => {
            let config = fsqlite_exec_config();
            run_benchmark(&benchmark_config, &meta, |_| -> E2eResult<EngineRunReport> {
                let temp = tempdir()?;
                let db_path = temp.path().join("fsqlite.db");
                run_oplog_fsqlite(&db_path, oplog, &config)
            })
        }
    }
}

fn run_once_engine(
    engine: EngineKind,
    oplog: &OpLog,
    progress_writer_count: u16,
) -> (EngineRunReport, WriterProgress) {
    let temp = tempdir().expect("tempdir");
    let db_path: PathBuf = temp.path().join(format!("{}-once.db", engine.as_str()));

    let report = match engine {
        EngineKind::Sqlite => {
            run_oplog_sqlite(&db_path, oplog, &sqlite_exec_config()).expect("run sqlite scenario")
        }
        EngineKind::Fsqlite => {
            run_oplog_fsqlite(&db_path, oplog, &fsqlite_exec_config()).expect("run fsqlite scenario")
        }
    };

    let progress = match engine {
        EngineKind::Sqlite => writer_progress_sqlite(&db_path, progress_writer_count),
        EngineKind::Fsqlite => writer_progress_fsqlite(&db_path, progress_writer_count),
    };

    (report, progress)
}

fn writer_progress_table_name(writer_id: u16) -> String {
    format!("writer_progress_{writer_id}")
}

fn writer_progress_sqlite(db_path: &Path, writer_count: u16) -> WriterProgress {
    if writer_count == 0 {
        return WriterProgress::default();
    }
    let conn = match rusqlite::Connection::open(db_path) {
        Ok(conn) => conn,
        Err(_) => return WriterProgress::default(),
    };
    let mut commits = Vec::new();
    for writer_id in 0..writer_count {
        let table = writer_progress_table_name(writer_id);
        let sql = format!("SELECT COUNT(*) FROM {table};");
        let count = conn
            .query_row(&sql, [], |row| row.get::<_, i64>(0))
            .unwrap_or(0);
        let value = u64::try_from(count).unwrap_or(0);
        commits.push(value);
    }
    summarize_progress(&commits)
}

fn writer_progress_fsqlite(db_path: &Path, writer_count: u16) -> WriterProgress {
    if writer_count == 0 {
        return WriterProgress::default();
    }
    let Some(path) = db_path.to_str() else {
        return WriterProgress::default();
    };
    let conn = match fsqlite::Connection::open(path) {
        Ok(conn) => conn,
        Err(_) => return WriterProgress::default(),
    };
    let mut commits = Vec::new();
    for writer_id in 0..writer_count {
        let table = writer_progress_table_name(writer_id);
        let sql = format!("SELECT COUNT(*) FROM {table};");
        let rows = match conn.query(&sql) {
            Ok(rows) => rows,
            Err(_) => {
                commits.push(0);
                continue;
            }
        };
        let count = rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| match value {
                SqliteValue::Integer(v) => u64::try_from(*v).ok(),
                _ => None,
            })
            .unwrap_or(0);
        commits.push(count);
    }

    summarize_progress(&commits)
}

fn push_worker_progress_table_setup(
    records: &mut Vec<OpRecord>,
    op_id: &mut u64,
    worker: u16,
    writers: u16,
) {
    for writer_id in 0..writers {
        let table = writer_progress_table_name(writer_id);
        records.push(OpRecord {
            op_id: *op_id,
            worker,
            kind: OpKind::Sql {
                statement: format!(
                    "CREATE TABLE IF NOT EXISTS {table} (round INTEGER PRIMARY KEY, committed INTEGER NOT NULL)"
                ),
            },
            expected: None,
        });
        *op_id += 1;
    }
}

fn push_reader_round(records: &mut Vec<OpRecord>, op_id: &mut u64, worker: u16, hot_key: u32) {
    records.push(OpRecord {
        op_id: *op_id,
        worker,
        kind: OpKind::Begin,
        expected: None,
    });
    *op_id += 1;

    records.push(OpRecord {
        op_id: *op_id,
        worker,
        kind: OpKind::Sql {
            statement: "SELECT SUM(v) FROM rw_hot".to_owned(),
        },
        expected: Some(ExpectedResult::RowCount(1)),
    });
    *op_id += 1;

    records.push(OpRecord {
        op_id: *op_id,
        worker,
        kind: OpKind::Sql {
            statement: format!("SELECT v FROM rw_hot WHERE id = {hot_key}"),
        },
        expected: Some(ExpectedResult::RowCount(1)),
    });
    *op_id += 1;

    records.push(OpRecord {
        op_id: *op_id,
        worker,
        kind: OpKind::Commit,
        expected: None,
    });
    *op_id += 1;
}

fn push_writer_round(
    records: &mut Vec<OpRecord>,
    op_id: &mut u64,
    worker: u16,
    writer_id: u16,
    round: u32,
) {
    let table = writer_progress_table_name(writer_id);

    records.push(OpRecord {
        op_id: *op_id,
        worker,
        kind: OpKind::Begin,
        expected: None,
    });
    *op_id += 1;

    records.push(OpRecord {
        op_id: *op_id,
        worker,
        kind: OpKind::Sql {
            statement: format!("INSERT INTO {table} (round, committed) VALUES ({round}, 1)"),
        },
        expected: Some(ExpectedResult::AffectedRows(1)),
    });
    *op_id += 1;

    records.push(OpRecord {
        op_id: *op_id,
        worker,
        kind: OpKind::Commit,
        expected: None,
    });
    *op_id += 1;
}

fn build_writer_fanout_oplog(fixture_id: &str, seed: u64, writers: u16, rounds: u32) -> OpLog {
    let mut records = Vec::new();
    let mut op_id = 0_u64;

    for worker in 0..writers {
        push_worker_progress_table_setup(&mut records, &mut op_id, worker, writers);

        for round in 0..rounds {
            push_writer_round(&mut records, &mut op_id, worker, worker, round);
        }
    }

    OpLog {
        header: OpLogHeader {
            fixture_id: fixture_id.to_owned(),
            seed,
            rng: RngSpec::default(),
            concurrency: ConcurrencyModel {
                worker_count: writers,
                transaction_size: 1,
                commit_order_policy: "barrier".to_owned(),
            },
            preset: Some("writer_fanout".to_owned()),
        },
        records,
    }
}

fn summarize_progress(commits: &[u64]) -> WriterProgress {
    if commits.is_empty() {
        return WriterProgress::default();
    }

    let min_commits = commits.iter().copied().min().expect("at least one commit");
    let max_commits = commits.iter().copied().max().expect("at least one commit");
    let total_commits = commits.iter().sum();

    WriterProgress {
        min_commits,
        max_commits,
        total_commits,
        writer_count: commits.len(),
    }
}

fn total_retries(summary: &BenchmarkSummary) -> u64 {
    summary.iterations.iter().map(|iteration| iteration.retries).sum()
}

fn total_aborts(summary: &BenchmarkSummary) -> u64 {
    summary.iterations.iter().map(|iteration| iteration.aborts).sum()
}

fn conflict_like_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("busy")
        || lower.contains("locked")
        || lower.contains("snapshot")
        || lower.contains("conflict")
}

#[allow(clippy::cast_precision_loss)]
fn commit_throughput_commits_per_sec(summary: &BenchmarkSummary, commits_per_run: u64) -> f64 {
    let median_ms = summary.latency.median_ms;
    if median_ms <= 0.0 {
        0.0
    } else {
        commits_per_run as f64 / (median_ms / 1000.0)
    }
}

fn build_reader_writer_disjoint_oplog(
    fixture_id: &str,
    seed: u64,
    readers: u16,
    writers: u16,
    rounds: u32,
    hot_rows: u32,
) -> OpLog {
    let worker_count = readers + writers;
    let mut records = Vec::new();
    let mut op_id = 0_u64;

    // Idempotent setup duplicated per worker so executors do not need to enforce
    // a dedicated setup worker.
    for worker in 0..worker_count {
        records.push(OpRecord {
            op_id,
            worker,
            kind: OpKind::Sql {
                statement: "CREATE TABLE IF NOT EXISTS rw_hot (id INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0)".to_owned(),
            },
            expected: None,
        });
        op_id += 1;

        for hot_key in 0..hot_rows {
            records.push(OpRecord {
                op_id,
                worker,
                kind: OpKind::Sql {
                    statement: format!(
                        "INSERT OR IGNORE INTO rw_hot (id, v) VALUES ({hot_key}, 0)"
                    ),
                },
                expected: None,
            });
            op_id += 1;
        }
        push_worker_progress_table_setup(&mut records, &mut op_id, worker, writers);
    }

    for worker in 0..worker_count {
        if worker < readers {
            for round in 0..rounds {
                let hot_key = (u32::from(worker) + round) % hot_rows;
                push_reader_round(&mut records, &mut op_id, worker, hot_key);
            }
        } else {
            let writer_id = worker - readers;
            for round in 0..rounds {
                push_writer_round(&mut records, &mut op_id, worker, writer_id, round);
            }
        }
    }

    OpLog {
        header: OpLogHeader {
            fixture_id: fixture_id.to_owned(),
            seed,
            rng: RngSpec::default(),
            concurrency: ConcurrencyModel {
                worker_count,
                transaction_size: 2,
                commit_order_policy: "barrier".to_owned(),
            },
            preset: Some("reader_writer_disjoint".to_owned()),
        },
        records,
    }
}

fn emit_scenario_outcome(payload: serde_json::Value) {
    println!("SCENARIO_OUTCOME:{payload}");
}

#[test]
fn unit_scenario_contract_constants_are_stable() {
    assert_eq!(BEAD_ID, "bd-1r0ha.3");
    assert_eq!(READERS, 10);
    assert_eq!(WRITERS, 10);
    assert_eq!(TOTAL_WORKERS, 20);
    assert_eq!(SCENARIO_RW_DISJOINT, "MVCC-E2E-RW10W10-DISJOINT");
    assert_eq!(SCENARIO_HOT_CONTENTION, "MVCC-E2E-HOT-WRITE-CONTENTION");
    assert_eq!(SCENARIO_NO_SERIALIZATION, "MVCC-E2E-NO-GLOBAL-SERIALIZATION");
}

#[test]
fn scenario_rw10w10_disjoint_fairness_and_latency() {
    let oplog = build_reader_writer_disjoint_oplog(
        SCENARIO_RW_DISJOINT,
        SEED_RW_DISJOINT,
        READERS,
        WRITERS,
        DISJOINT_ROUNDS,
        HOT_ROWS,
    );

    let sqlite_summary = benchmark_engine(EngineKind::Sqlite, "rw10w10_disjoint", &oplog);
    let fsqlite_summary = benchmark_engine(EngineKind::Fsqlite, "rw10w10_disjoint", &oplog);

    let (sqlite_once, sqlite_progress) = run_once_engine(EngineKind::Sqlite, &oplog, WRITERS);
    let (fsqlite_once, fsqlite_progress) = run_once_engine(EngineKind::Fsqlite, &oplog, WRITERS);

    assert!(
        sqlite_once.error.is_none(),
        "sqlite rw10w10 disjoint should succeed without fatal errors: {:?}",
        sqlite_once.error
    );
    assert!(
        fsqlite_once.error.is_none(),
        "fsqlite rw10w10 disjoint should succeed without fatal errors: {:?}",
        fsqlite_once.error
    );

    let expected_commits_per_writer = u64::from(DISJOINT_ROUNDS);
    let sqlite_starvation_gap = sqlite_progress
        .max_commits
        .saturating_sub(sqlite_progress.min_commits);
    let fsqlite_starvation_gap = fsqlite_progress
        .max_commits
        .saturating_sub(fsqlite_progress.min_commits);
    let sqlite_bounded_starvation =
        sqlite_progress.min_commits >= expected_commits_per_writer && sqlite_starvation_gap <= 1;
    let fsqlite_bounded_starvation =
        fsqlite_progress.min_commits >= expected_commits_per_writer && fsqlite_starvation_gap <= 1;

    assert!(
        sqlite_bounded_starvation,
        "sqlite bounded-starvation violated: progress={sqlite_progress:?}"
    );
    assert!(
        fsqlite_bounded_starvation,
        "fsqlite bounded-starvation violated: progress={fsqlite_progress:?}"
    );

    let fsqlite_notes = fsqlite_once.correctness.notes.clone().unwrap_or_default();
    assert!(
        fsqlite_notes.contains("parallel worker execution"),
        "fsqlite run must execute workers in parallel to avoid global serialization: {fsqlite_notes}"
    );

    let sqlite_retries = total_retries(&sqlite_summary);
    assert!(
        sqlite_retries > 0,
        "sqlite should report lock retries under 10R/10W concurrency; got {sqlite_retries}"
    );

    emit_scenario_outcome(json!({
        "scenario_id": SCENARIO_RW_DISJOINT,
        "seed": SEED_RW_DISJOINT,
        "reader_workers": READERS,
        "writer_workers": WRITERS,
        "hot_rows": HOT_ROWS,
        "rounds": DISJOINT_ROUNDS,
        "bounded_starvation_sqlite": sqlite_bounded_starvation,
        "bounded_starvation_fsqlite": fsqlite_bounded_starvation,
        "sqlite_writer_progress": {
            "min_commits": sqlite_progress.min_commits,
            "max_commits": sqlite_progress.max_commits,
            "total_commits": sqlite_progress.total_commits,
            "writers": sqlite_progress.writer_count,
        },
        "fsqlite_writer_progress": {
            "min_commits": fsqlite_progress.min_commits,
            "max_commits": fsqlite_progress.max_commits,
            "total_commits": fsqlite_progress.total_commits,
            "writers": fsqlite_progress.writer_count,
        },
        "sqlite": {
            "median_ops_per_sec": sqlite_summary.throughput.median_ops_per_sec,
            "tail_latency_ms": {
                "p95": sqlite_summary.latency.p95_ms,
                "p99": sqlite_summary.latency.p99_ms,
            },
            "wait_retry_counts": {
                "retries": total_retries(&sqlite_summary),
                "aborts": total_aborts(&sqlite_summary),
            },
            "commit_throughput_commits_per_sec": commit_throughput_commits_per_sec(
                &sqlite_summary,
                sqlite_progress.total_commits,
            ),
        },
        "fsqlite": {
            "median_ops_per_sec": fsqlite_summary.throughput.median_ops_per_sec,
            "tail_latency_ms": {
                "p95": fsqlite_summary.latency.p95_ms,
                "p99": fsqlite_summary.latency.p99_ms,
            },
            "wait_retry_counts": {
                "retries": total_retries(&fsqlite_summary),
                "aborts": total_aborts(&fsqlite_summary),
            },
            "commit_throughput_commits_per_sec": commit_throughput_commits_per_sec(
                &fsqlite_summary,
                fsqlite_progress.total_commits,
            ),
        },
        "replay_command": REPLAY_COMMAND,
    }));
}

#[test]
fn scenario_hot_page_contention_conflict_outcomes() {
    let oplog = preset_hot_page_contention(
        SCENARIO_HOT_CONTENTION,
        SEED_HOT_CONTENTION,
        TOTAL_WORKERS,
        HOT_ROUNDS,
    );

    let sqlite_summary = benchmark_engine(EngineKind::Sqlite, "hot_page_contention", &oplog);
    let fsqlite_summary = benchmark_engine(EngineKind::Fsqlite, "hot_page_contention", &oplog);
    let (fsqlite_once, _) = run_once_engine(EngineKind::Fsqlite, &oplog, 0);

    let sqlite_retry_total = total_retries(&sqlite_summary);
    assert!(
        sqlite_retry_total > 0,
        "sqlite hot contention must show lock retries to prove contention path coverage"
    );

    let fsqlite_conflict_outcome_ok = match fsqlite_once.error.as_deref() {
        None => true,
        Some(message) => conflict_like_error(message),
    };
    assert!(
        fsqlite_conflict_outcome_ok,
        "fsqlite hot contention error must be conflict-like when present: {:?}",
        fsqlite_once.error
    );

    emit_scenario_outcome(json!({
        "scenario_id": SCENARIO_HOT_CONTENTION,
        "seed": SEED_HOT_CONTENTION,
        "reader_workers": 0,
        "writer_workers": TOTAL_WORKERS,
        "hot_rows": HOT_ROWS,
        "rounds": HOT_ROUNDS,
        "conflict_outcome_ok": fsqlite_conflict_outcome_ok,
        "sqlite": {
            "median_ops_per_sec": sqlite_summary.throughput.median_ops_per_sec,
            "tail_latency_ms": {
                "p95": sqlite_summary.latency.p95_ms,
                "p99": sqlite_summary.latency.p99_ms,
            },
            "wait_retry_counts": {
                "retries": sqlite_retry_total,
                "aborts": total_aborts(&sqlite_summary),
            },
        },
        "fsqlite": {
            "median_ops_per_sec": fsqlite_summary.throughput.median_ops_per_sec,
            "tail_latency_ms": {
                "p95": fsqlite_summary.latency.p95_ms,
                "p99": fsqlite_summary.latency.p99_ms,
            },
            "wait_retry_counts": {
                "retries": total_retries(&fsqlite_summary),
                "aborts": total_aborts(&fsqlite_summary),
            },
            "error": fsqlite_once.error,
        },
        "replay_command": REPLAY_COMMAND,
    }));
}

#[test]
fn scenario_no_global_writer_serialization_probe() {
    let oplog = build_writer_fanout_oplog(
        SCENARIO_NO_SERIALIZATION,
        SEED_NO_SERIALIZATION,
        TOTAL_WORKERS,
        DISJOINT_ROUNDS,
    );

    let sqlite_summary = benchmark_engine(EngineKind::Sqlite, "writer_fanout", &oplog);
    let fsqlite_summary = benchmark_engine(EngineKind::Fsqlite, "writer_fanout", &oplog);
    let (fsqlite_once, fsqlite_progress) = run_once_engine(EngineKind::Fsqlite, &oplog, TOTAL_WORKERS);

    let sqlite_retry_total = total_retries(&sqlite_summary);
    assert!(
        sqlite_retry_total > 0,
        "sqlite should surface lock retries in high-concurrency writer fanout"
    );
    assert!(
        fsqlite_once.error.is_none(),
        "fsqlite writer fanout should complete without fatal error: {:?}",
        fsqlite_once.error
    );

    let notes = fsqlite_once.correctness.notes.clone().unwrap_or_default();
    assert!(
        notes.contains("parallel worker execution"),
        "concurrency probe must run with parallel workers: {notes}"
    );
    assert!(
        fsqlite_progress.min_commits >= u64::from(DISJOINT_ROUNDS),
        "writer fanout must show bounded starvation in fsqlite: {fsqlite_progress:?}"
    );

    emit_scenario_outcome(json!({
        "scenario_id": SCENARIO_NO_SERIALIZATION,
        "seed": SEED_NO_SERIALIZATION,
        "concurrency": TOTAL_WORKERS,
        "no_global_writer_serialization": notes.contains("parallel worker execution") && sqlite_retry_total > 0,
        "sqlite": {
            "median_ops_per_sec": sqlite_summary.throughput.median_ops_per_sec,
            "tail_latency_ms": {
                "p95": sqlite_summary.latency.p95_ms,
                "p99": sqlite_summary.latency.p99_ms,
            },
            "wait_retry_counts": {
                "retries": sqlite_retry_total,
                "aborts": total_aborts(&sqlite_summary),
            },
        },
        "fsqlite": {
            "median_ops_per_sec": fsqlite_summary.throughput.median_ops_per_sec,
            "tail_latency_ms": {
                "p95": fsqlite_summary.latency.p95_ms,
                "p99": fsqlite_summary.latency.p99_ms,
            },
            "wait_retry_counts": {
                "retries": total_retries(&fsqlite_summary),
                "aborts": total_aborts(&fsqlite_summary),
            },
            "writer_progress": {
                "min_commits": fsqlite_progress.min_commits,
                "max_commits": fsqlite_progress.max_commits,
                "total_commits": fsqlite_progress.total_commits,
                "writers": fsqlite_progress.writer_count,
            },
        },
        "replay_command": REPLAY_COMMAND,
    }));
}
