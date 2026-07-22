//! E2E verification for `bd-3wop3.1.3` durability certificates and visibility publication.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fsqlite_e2e::logging::init_logging;
use fsqlite_types::SqliteValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-3wop3.1.3";
const SCENARIO_ID: &str = "parallel_wal_publication";
const COMPATIBILITY_SELECTOR: &str = "wal_invariant,integrity_check,row_level";
const EVIDENCE_OWNER: &str = "durability-combiner-owner";
const VERIFICATION_CLASS: &str = "COR";
const ORACLE: &str = "rusqlite";
const ALLOWED_DIFFERENCE_POLICY: &str = "none";
const BASELINE_COMPARATOR: &str = "sqlite3_c1_baseline";
const POLICY_ID: &str = "parallel_wal_control.v1";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_3wop3_1_3_parallel_wal_publication -- --nocapture --test-threads=1";
const CHILD_TEST_NAME: &str = "bd_3wop3_1_3_parallel_wal_publication_child_entrypoint";
const PERFORMANCE_CHILD_TEST_NAME: &str =
    "bd_3wop3_1_3_parallel_wal_publication_performance_child_entrypoint";
const CHILD_MODE_ENV: &str = "FSQLITE_BD_3WOP3_1_3_MODE";
const CHILD_RUN_DIR_ENV: &str = "FSQLITE_BD_3WOP3_1_3_RUN_DIR";
const PERFORMANCE_THREADS_ENV: &str = "FSQLITE_BD_3WOP3_1_3_PERFORMANCE_THREADS";
const PERFORMANCE_REPETITION_ENV: &str = "FSQLITE_BD_3WOP3_1_3_PERFORMANCE_REPETITION";
const REPORT_ARTIFACT_ENV: &str = "FSQLITE_BD_3WOP3_1_3_ARTIFACT";
const RUN_ROOT_ENV: &str = "FSQLITE_BD_3WOP3_1_3_RUN_ROOT";
const OPEN_RETRY_BUDGET: Duration = Duration::from_secs(10);
const DURABILITY_LOG_FILTER: &str = "warn,fsqlite::wal::durability_combiner=debug";
const MAX_THROUGHPUT_REGRESSION_PERCENT: u64 = 5;
const PERFORMANCE_REPETITIONS: usize = 4;

static E2E_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FinalRow {
    table_name: String,
    id: i64,
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublicationRunSummary {
    bead_id: String,
    claim_id: String,
    evidence_id: String,
    evidence_owner: String,
    verification_class: String,
    mode: String,
    scenario_id: String,
    compatibility_selector: String,
    oracle: String,
    allowed_difference_policy: String,
    baseline_comparator: String,
    policy_id: String,
    decision_id: String,
    budget_id: String,
    shadow_lineage: String,
    fallback_lineage: String,
    replay_command: String,
    log_path: String,
    trace_ids: Vec<u64>,
    final_rows: Vec<FinalRow>,
    integrity_check: String,
    publication_events: usize,
    durable_certificate_records: usize,
    commit_certificates: Vec<u64>,
    commit_seq_ranges: Vec<(u64, u64)>,
    durability_sequences: Vec<u64>,
    publication_generations: Vec<u64>,
    ordered_region_ns: Vec<u64>,
    batch_sizes: Vec<u64>,
    lookup_modes: Vec<String>,
    control_modes: Vec<String>,
    shadow_verdicts: Vec<String>,
    fallback_reasons: Vec<String>,
    missing_fields_by_event: BTreeMap<usize, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PerformanceRunSummary {
    claim_id: String,
    evidence_id: String,
    evidence_owner: String,
    mode: String,
    thread_count: usize,
    repetition: usize,
    transactions_per_thread: usize,
    committed_transactions: usize,
    retry_count: u64,
    row_count: usize,
    integrity_check: String,
    wall_time_ns: u64,
    throughput_transactions_per_second: u64,
    publication_events: usize,
    batch_sizes: Vec<u64>,
    ordered_region_ns: Vec<u64>,
    ordered_region_total_ns: u64,
    ordered_region_median_ns: u64,
    ordered_region_p95_ns: u64,
    ordered_region_ns_per_commit: u64,
    replay_command: String,
    oracle: String,
    allowed_difference_policy: String,
    baseline_comparator: String,
    policy_id: String,
    decision_id: String,
    fallback_lineage: String,
}

fn percentile(samples: &[u64], percentage: usize) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = sorted.len().saturating_mul(percentage).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

fn median(samples: &[u64]) -> u64 {
    assert!(!samples.is_empty(), "median requires at least one sample");
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        sorted[middle - 1]
            .saturating_add(sorted[middle])
            .div_ceil(2)
    } else {
        sorted[middle]
    }
}

fn evidence_id(mode: &str) -> String {
    format!("D1-COR-01:fsqlite_mvcc:baseline_unpinned:c2:{mode}:42")
}

fn is_lock_contention(error: &fsqlite::FrankenError) -> bool {
    matches!(
        error,
        fsqlite::FrankenError::Busy
            | fsqlite::FrankenError::BusyRecovery
            | fsqlite::FrankenError::BusySnapshot { .. }
            | fsqlite::FrankenError::DatabaseLocked { .. }
    )
}

fn retry_lock_contention<T>(
    context: &str,
    mut operation: impl FnMut() -> Result<T, fsqlite::FrankenError>,
) -> Result<T, String> {
    let started = Instant::now();
    let mut attempt = 0_u32;
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if is_lock_contention(&error) && started.elapsed() < OPEN_RETRY_BUDGET => {
                attempt = attempt.saturating_add(1);
                thread::sleep(Duration::from_millis(1_u64 << attempt.min(5)));
            }
            Err(error) => return Err(format!("{context} after {:?}: {error}", started.elapsed())),
        }
    }
}

fn try_open_connection(path: &Path) -> Result<fsqlite::Connection, String> {
    let path = path
        .to_str()
        .ok_or_else(|| format!("database path is not UTF-8: {}", path.display()))?;
    let conn = retry_lock_contention("open FrankenSQLite connection", || {
        fsqlite::Connection::open(path)
    })?;
    retry_lock_contention("enable WAL mode", || {
        conn.execute("PRAGMA journal_mode=WAL;").map(|_| ())
    })?;
    if !conn.is_concurrent_mode_default() {
        return Err(format!(
            "bead_id={BEAD_ID} case=concurrent_mode_default_guard"
        ));
    }
    Ok(conn)
}

fn open_connection(path: &Path) -> fsqlite::Connection {
    try_open_connection(path).unwrap_or_else(|error| panic!("{error}"))
}

fn insert_row(path: &Path, table_name: &'static str, id: i64, barrier: Arc<Barrier>) {
    let conn = try_open_connection(path);
    barrier.wait();
    let conn = conn.unwrap_or_else(|error| panic!("{error}"));
    conn.execute("BEGIN CONCURRENT;")
        .expect("begin concurrent transaction");
    conn.execute(&format!(
        "INSERT INTO {table_name} VALUES ({id}, '{table_name}-{id}')"
    ))
    .expect("insert row");
    conn.execute("COMMIT;").expect("commit transaction");
    conn.close_without_checkpoint()
        .expect("close writer without checkpoint");
}

fn query_single_text(conn: &fsqlite::Connection, sql: &str) -> String {
    let rows = conn
        .query(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    match rows.first().and_then(|row| row.get(0)) {
        Some(SqliteValue::Text(value)) => value.to_string(),
        other => panic!("expected one TEXT value for {sql}, got {other:?}"),
    }
}

fn fetch_final_rows(conn: &fsqlite::Connection) -> Vec<FinalRow> {
    conn.query(
        "SELECT table_name, id, value FROM (\
         SELECT 'a' AS table_name, id, value FROM a \
         UNION ALL \
         SELECT 'b' AS table_name, id, value FROM b\
         ) ORDER BY table_name, id",
    )
    .expect("query final rows")
    .into_iter()
    .map(|row| {
        let table_name = match row.get(0) {
            Some(SqliteValue::Text(value)) => value.to_string(),
            other => panic!("expected TEXT table_name, got {other:?}"),
        };
        let id = match row.get(1) {
            Some(SqliteValue::Integer(value)) => *value,
            other => panic!("expected INTEGER id, got {other:?}"),
        };
        let value = match row.get(2) {
            Some(SqliteValue::Text(value)) => value.to_string(),
            other => panic!("expected TEXT value, got {other:?}"),
        };
        FinalRow {
            table_name,
            id,
            value,
        }
    })
    .collect()
}

fn field_value<'a>(event: &'a Value, key: &str) -> Option<&'a Value> {
    event
        .get(key)
        .or_else(|| event.get("fields").and_then(|fields| fields.get(key)))
}

fn field_string(event: &Value, key: &str) -> Option<String> {
    field_value(event, key).map(|value| match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    })
}

fn field_u64(event: &Value, key: &str) -> Option<u64> {
    field_value(event, key).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn summarize_publication_logs(
    log_path: &Path,
    mode: &str,
    final_rows: Vec<FinalRow>,
    integrity_check: String,
    durable_certificate_records: usize,
) -> PublicationRunSummary {
    const REQUIRED_FIELDS: [&str; 15] = [
        "trace_id",
        "scenario_id",
        "commit_certificate",
        "certificate_epoch",
        "commit_seq_lo",
        "commit_seq_hi",
        "durability_seq",
        "publication_generation",
        "ordered_region_ns",
        "batch_size",
        "lookup_mode",
        "control_mode",
        "shadow_certificate_verdict",
        "compatibility_selector",
        "fallback_reason",
    ];

    let mut commit_certificates = Vec::new();
    let mut trace_ids = BTreeSet::new();
    let mut commit_seq_ranges = Vec::new();
    let mut durability_sequences = Vec::new();
    let mut publication_generations = Vec::new();
    let mut ordered_region_ns = Vec::new();
    let mut batch_sizes = Vec::new();
    let mut lookup_modes = BTreeSet::new();
    let mut control_modes = BTreeSet::new();
    let mut shadow_verdicts = BTreeSet::new();
    let mut fallback_reasons = BTreeSet::new();
    let mut missing_fields_by_event = BTreeMap::new();

    let log_text = fs::read_to_string(log_path).expect("read durability-combiner log");
    for line in log_text.lines().filter(|line| !line.trim().is_empty()) {
        let event: Value = serde_json::from_str(line).expect("parse json log event");
        if event.get("target").and_then(Value::as_str) != Some("fsqlite::wal::durability_combiner")
        {
            continue;
        }
        if field_string(&event, "scenario_id").as_deref() != Some(SCENARIO_ID) {
            continue;
        }

        let event_index = commit_certificates.len();
        let missing = REQUIRED_FIELDS
            .iter()
            .filter(|field| field_value(&event, field).is_none())
            .map(|field| (*field).to_owned())
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            missing_fields_by_event.insert(event_index, missing);
        }

        commit_certificates
            .push(field_u64(&event, "commit_certificate").expect("commit certificate field"));
        trace_ids.insert(field_u64(&event, "trace_id").expect("trace id field"));
        commit_seq_ranges.push((
            field_u64(&event, "commit_seq_lo").expect("commit seq lo field"),
            field_u64(&event, "commit_seq_hi").expect("commit seq hi field"),
        ));
        durability_sequences
            .push(field_u64(&event, "durability_seq").expect("durability sequence field"));
        publication_generations.push(
            field_u64(&event, "publication_generation").expect("publication generation field"),
        );
        ordered_region_ns
            .push(field_u64(&event, "ordered_region_ns").expect("ordered region field"));
        batch_sizes.push(field_u64(&event, "batch_size").expect("batch size field"));
        lookup_modes.insert(field_string(&event, "lookup_mode").expect("lookup mode field"));
        control_modes.insert(field_string(&event, "control_mode").expect("control mode field"));
        shadow_verdicts.insert(
            field_string(&event, "shadow_certificate_verdict").expect("shadow verdict field"),
        );
        fallback_reasons
            .insert(field_string(&event, "fallback_reason").expect("fallback reason field"));
        assert_eq!(
            field_string(&event, "compatibility_selector").as_deref(),
            Some(COMPATIBILITY_SELECTOR),
            "bead_id={BEAD_ID} case=compatibility_selector"
        );
    }

    PublicationRunSummary {
        bead_id: BEAD_ID.to_owned(),
        claim_id: BEAD_ID.to_owned(),
        evidence_id: evidence_id(mode),
        evidence_owner: EVIDENCE_OWNER.to_owned(),
        verification_class: VERIFICATION_CLASS.to_owned(),
        mode: mode.to_owned(),
        scenario_id: SCENARIO_ID.to_owned(),
        compatibility_selector: COMPATIBILITY_SELECTOR.to_owned(),
        oracle: ORACLE.to_owned(),
        allowed_difference_policy: ALLOWED_DIFFERENCE_POLICY.to_owned(),
        baseline_comparator: BASELINE_COMPARATOR.to_owned(),
        policy_id: POLICY_ID.to_owned(),
        decision_id: format!("{POLICY_ID}:{mode}:static-configuration"),
        budget_id: "none".to_owned(),
        shadow_lineage: if mode == "shadow_compare" {
            evidence_id("conservative")
        } else {
            "none".to_owned()
        },
        fallback_lineage: if mode == "conservative" {
            evidence_id("conservative")
        } else {
            "none".to_owned()
        },
        replay_command: REPLAY_COMMAND.to_owned(),
        log_path: log_path.display().to_string(),
        trace_ids: trace_ids.into_iter().collect(),
        final_rows,
        integrity_check,
        publication_events: commit_certificates.len(),
        durable_certificate_records,
        commit_certificates,
        commit_seq_ranges,
        durability_sequences,
        publication_generations,
        ordered_region_ns,
        batch_sizes,
        lookup_modes: lookup_modes.into_iter().collect(),
        control_modes: control_modes.into_iter().collect(),
        shadow_verdicts: shadow_verdicts.into_iter().collect(),
        fallback_reasons: fallback_reasons.into_iter().collect(),
        missing_fields_by_event,
    }
}

fn run_child_workload(run_dir: &Path, mode: &str) -> PublicationRunSummary {
    let _logging_guard = init_logging(run_dir, true).expect("initialize child logging");
    let db_path = run_dir.join("parallel_wal_publication.db");
    let log_path = run_dir.join("test.log.jsonl");

    {
        let conn = open_connection(&db_path);
        conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, value TEXT)")
            .expect("create table a");
        conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, value TEXT)")
            .expect("create table b");
        conn.close_without_checkpoint()
            .expect("close setup connection without resetting certificate-bound WAL");
    }

    for wave in 0..2_i64 {
        let barrier = Arc::new(Barrier::new(3));
        let writer_a_path = db_path.clone();
        let writer_b_path = db_path.clone();
        let writer_a_barrier = Arc::clone(&barrier);
        let writer_b_barrier = Arc::clone(&barrier);
        let writer_a = thread::spawn(move || {
            insert_row(&writer_a_path, "a", wave + 1, writer_a_barrier);
        });
        let writer_b = thread::spawn(move || {
            insert_row(&writer_b_path, "b", wave + 1, writer_b_barrier);
        });
        barrier.wait();
        writer_a.join().expect("join writer a");
        writer_b.join().expect("join writer b");
    }

    let conn = open_connection(&db_path);
    let rows_before_checkpoint = fetch_final_rows(&conn);
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("truncate checkpoint");
    conn.close_without_checkpoint()
        .expect("close explicit checkpoint connection");

    let post_checkpoint = open_connection(&db_path);
    post_checkpoint
        .execute("INSERT INTO a VALUES (3, 'a-3')")
        .expect("commit first transaction in reset WAL generation");
    post_checkpoint
        .close_without_checkpoint()
        .expect("close post-checkpoint writer without another reset");

    let reopened = open_connection(&db_path);
    let final_rows = fetch_final_rows(&reopened);
    let integrity_check = query_single_text(&reopened, "PRAGMA integrity_check;");
    assert_eq!(
        integrity_check, "ok",
        "integrity check after post-checkpoint commit"
    );
    reopened
        .close_without_checkpoint()
        .expect("close reopened connection without resetting post-checkpoint WAL");
    assert_eq!(
        final_rows.len(),
        rows_before_checkpoint.len() + 1,
        "bead_id={BEAD_ID} case=post_checkpoint_generation_adds_exactly_one_row"
    );

    let mut certificate_path = db_path.as_os_str().to_owned();
    certificate_path.push("-wal-cert");
    let certificate_path = PathBuf::from(certificate_path);
    let durable_records = fsqlite_wal::decode_parallel_wal_durable_certificate_records(
        &fs::read(&certificate_path).unwrap_or_else(|error| {
            panic!(
                "read durable certificate sidecar {}: {error}",
                certificate_path.display()
            )
        }),
    )
    .expect("strictly reconstruct durable certificate sidecar");
    assert!(
        !durable_records.is_empty(),
        "bead_id={BEAD_ID} case={mode}_durable_certificate_records_present"
    );

    summarize_publication_logs(
        &log_path,
        mode,
        final_rows,
        integrity_check,
        durable_records.len(),
    )
}

fn run_performance_writer(
    db_path: &Path,
    lane_id: usize,
    transactions: usize,
    barrier: Arc<Barrier>,
) -> u64 {
    let conn = open_connection(db_path);
    barrier.wait();
    let mut retries = 0_u64;
    for row_id in 1..=transactions {
        let mut attempt = 0_u32;
        loop {
            attempt = attempt.saturating_add(1);
            conn.execute("BEGIN CONCURRENT;")
                .expect("begin production matrix transaction");
            let insert = conn.execute(&format!(
                "INSERT INTO lane_{lane_id} VALUES ({row_id}, 'lane-{lane_id}-row-{row_id}')"
            ));
            let commit = insert.and_then(|_| conn.execute("COMMIT;"));
            match commit {
                Ok(_) => break,
                Err(error) if is_lock_contention(&error) && attempt < 64 => {
                    let _ = conn.execute("ROLLBACK;");
                    retries = retries.saturating_add(1);
                    thread::sleep(Duration::from_micros(u64::from(attempt).saturating_mul(50)));
                }
                Err(error) => panic!(
                    "production matrix lane {lane_id} row {row_id} failed after {attempt} attempts: {error}"
                ),
            }
        }
    }
    conn.close_without_checkpoint()
        .expect("close production matrix writer without checkpoint");
    retries
}

fn run_performance_workload(
    run_dir: &Path,
    mode: &str,
    thread_count: usize,
    repetition: usize,
) -> PerformanceRunSummary {
    const TRANSACTIONS_PER_THREAD: usize = 16;

    let _logging_guard = init_logging(run_dir, true).expect("initialize performance logging");
    let db_path = run_dir.join("parallel_wal_publication_performance.db");
    let log_path = run_dir.join("test.log.jsonl");
    let setup = open_connection(&db_path);
    for lane_id in 0..thread_count {
        setup
            .execute(&format!(
                "CREATE TABLE lane_{lane_id} (id INTEGER PRIMARY KEY, value TEXT)"
            ))
            .expect("create disjoint production matrix table");
    }
    setup
        .execute("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("reset WAL before measured production matrix");
    setup
        .close_without_checkpoint()
        .expect("close production matrix setup connection");

    let barrier = Arc::new(Barrier::new(thread_count.saturating_add(1)));
    let mut writers = Vec::with_capacity(thread_count);
    for lane_id in 0..thread_count {
        let writer_path = db_path.clone();
        let writer_barrier = Arc::clone(&barrier);
        writers.push(thread::spawn(move || {
            run_performance_writer(
                &writer_path,
                lane_id,
                TRANSACTIONS_PER_THREAD,
                writer_barrier,
            )
        }));
    }
    let wall_start = Instant::now();
    barrier.wait();
    let retry_count = writers
        .into_iter()
        .map(|writer| writer.join().expect("join production matrix writer"))
        .fold(0_u64, u64::saturating_add);
    let wall_time_ns = u64::try_from(wall_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let committed_transactions = thread_count.saturating_mul(TRANSACTIONS_PER_THREAD);

    let verification = open_connection(&db_path);
    let row_count = (0..thread_count)
        .map(|lane_id| {
            let rows = verification
                .query(&format!("SELECT COUNT(*) FROM lane_{lane_id}"))
                .expect("count production matrix rows");
            match rows.first().and_then(|row| row.get(0)) {
                Some(SqliteValue::Integer(count)) => usize::try_from(*count).expect("row count"),
                other => panic!("expected production matrix row count, got {other:?}"),
            }
        })
        .sum::<usize>();
    let integrity_check = query_single_text(&verification, "PRAGMA integrity_check;");
    verification
        .close_without_checkpoint()
        .expect("close production matrix verification connection");

    // Setup commits precede the measured writers in the same log. Select the
    // newest complete publication suffix whose certified batch cardinality
    // covers exactly the measured transactions.
    let log_text = fs::read_to_string(&log_path).expect("read production matrix log");
    let mut all_publications = Vec::new();
    for line in log_text.lines().filter(|line| !line.trim().is_empty()) {
        let event: Value = serde_json::from_str(line).expect("parse production matrix event");
        if event.get("target").and_then(Value::as_str) == Some("fsqlite::wal::durability_combiner")
            && field_string(&event, "scenario_id").as_deref() == Some(SCENARIO_ID)
        {
            all_publications.push((
                field_u64(&event, "ordered_region_ns").expect("ordered region metric"),
                field_u64(&event, "batch_size").expect("batch size metric"),
            ));
        }
    }
    let mut selected = Vec::new();
    let mut selected_commits = 0_u64;
    for publication in all_publications.into_iter().rev() {
        if selected_commits >= u64::try_from(committed_transactions).unwrap_or(u64::MAX) {
            break;
        }
        selected_commits = selected_commits.saturating_add(publication.1);
        selected.push(publication);
    }
    selected.reverse();
    assert_eq!(
        selected_commits,
        u64::try_from(committed_transactions).unwrap_or(u64::MAX),
        "bead_id={BEAD_ID} case=production_matrix_publication_coverage mode={mode} threads={thread_count}"
    );
    let ordered_region_ns = selected.iter().map(|sample| sample.0).collect::<Vec<_>>();
    let batch_sizes = selected.iter().map(|sample| sample.1).collect::<Vec<_>>();
    let ordered_region_total_ns = ordered_region_ns.iter().copied().sum::<u64>();
    let committed_transactions_u64 = u64::try_from(committed_transactions).unwrap_or(u64::MAX);
    let throughput_transactions_per_second = if wall_time_ns == 0 {
        0
    } else {
        committed_transactions_u64.saturating_mul(1_000_000_000) / wall_time_ns
    };
    let cell_evidence_id =
        format!("D1-PFA-01:fsqlite_mvcc:baseline_unpinned:c{thread_count}:{mode}:r{repetition}:42");

    PerformanceRunSummary {
        claim_id: BEAD_ID.to_owned(),
        evidence_id: cell_evidence_id.clone(),
        evidence_owner: EVIDENCE_OWNER.to_owned(),
        mode: mode.to_owned(),
        thread_count,
        repetition,
        transactions_per_thread: TRANSACTIONS_PER_THREAD,
        committed_transactions,
        retry_count,
        row_count,
        integrity_check,
        wall_time_ns,
        throughput_transactions_per_second,
        publication_events: ordered_region_ns.len(),
        batch_sizes,
        ordered_region_total_ns,
        ordered_region_median_ns: percentile(&ordered_region_ns, 50),
        ordered_region_p95_ns: percentile(&ordered_region_ns, 95),
        ordered_region_ns_per_commit: ordered_region_total_ns / committed_transactions_u64.max(1),
        ordered_region_ns,
        replay_command: format!(
            "FSQLITE_PARALLEL_WAL_MODE={mode} FSQLITE_BD_3WOP3_1_3_PERFORMANCE_THREADS={thread_count} FSQLITE_BD_3WOP3_1_3_PERFORMANCE_REPETITION={repetition} {REPLAY_COMMAND}"
        ),
        oracle: "row_count_and_integrity_check".to_owned(),
        allowed_difference_policy: ALLOWED_DIFFERENCE_POLICY.to_owned(),
        baseline_comparator: if mode == "auto" {
            format!(
                "D1-PFA-01:fsqlite_mvcc:baseline_unpinned:c{thread_count}:conservative:r{repetition}:42"
            )
        } else {
            "none".to_owned()
        },
        policy_id: POLICY_ID.to_owned(),
        decision_id: format!("{POLICY_ID}:{mode}:c{thread_count}:static-configuration"),
        fallback_lineage: if mode == "conservative" {
            cell_evidence_id
        } else {
            "none".to_owned()
        },
    }
}

fn run_root_for(label: &str) -> (PathBuf, Option<tempfile::TempDir>) {
    if let Ok(base) = env::var(RUN_ROOT_ENV) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        let dir = PathBuf::from(base).join(format!("{label}-{nanos}"));
        fs::create_dir_all(&dir).expect("create persistent run dir");
        (dir, None)
    } else {
        let temp = tempdir().expect("create temporary run dir");
        let dir = temp.path().join(label);
        fs::create_dir_all(&dir).expect("create temporary mode dir");
        (dir, Some(temp))
    }
}

fn spawn_mode_run(mode: &str) -> PublicationRunSummary {
    let (run_dir, _temp_guard) = run_root_for(mode);
    let mut command = Command::new(env::current_exe().expect("current test executable"));
    let status = command
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("RUST_LOG", DURABILITY_LOG_FILTER)
        .env(CHILD_MODE_ENV, mode)
        .env(CHILD_RUN_DIR_ENV, &run_dir)
        .env("FSQLITE_PARALLEL_WAL_MODE", mode)
        .env("FSQLITE_PARALLEL_WAL_LANES", "2")
        .status()
        .expect("spawn child publication run");
    assert!(
        status.success(),
        "bead_id={BEAD_ID} case={mode}_child_process_failed status={status}"
    );
    serde_json::from_slice(&fs::read(run_dir.join("summary.json")).expect("read child summary"))
        .expect("parse child summary")
}

fn spawn_performance_run(
    mode: &str,
    thread_count: usize,
    repetition: usize,
) -> PerformanceRunSummary {
    let (run_dir, _temp_guard) =
        run_root_for(&format!("performance-{mode}-{thread_count}t-r{repetition}"));
    let status = Command::new(env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg(PERFORMANCE_CHILD_TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("RUST_LOG", DURABILITY_LOG_FILTER)
        .env(CHILD_MODE_ENV, mode)
        .env(CHILD_RUN_DIR_ENV, &run_dir)
        .env(PERFORMANCE_THREADS_ENV, thread_count.to_string())
        .env(PERFORMANCE_REPETITION_ENV, repetition.to_string())
        .env("FSQLITE_PARALLEL_WAL_MODE", mode)
        .env("FSQLITE_PARALLEL_WAL_LANES", thread_count.to_string())
        .status()
        .expect("spawn production performance matrix run");
    assert!(
        status.success(),
        "bead_id={BEAD_ID} case={mode}_{thread_count}t_r{repetition}_performance_child_failed status={status}"
    );
    serde_json::from_slice(
        &fs::read(run_dir.join("performance-summary.json"))
            .expect("read production performance summary"),
    )
    .expect("parse production performance summary")
}

fn expected_final_rows() -> Vec<FinalRow> {
    vec![
        FinalRow {
            table_name: "a".to_owned(),
            id: 1,
            value: "a-1".to_owned(),
        },
        FinalRow {
            table_name: "a".to_owned(),
            id: 2,
            value: "a-2".to_owned(),
        },
        FinalRow {
            table_name: "a".to_owned(),
            id: 3,
            value: "a-3".to_owned(),
        },
        FinalRow {
            table_name: "b".to_owned(),
            id: 1,
            value: "b-1".to_owned(),
        },
        FinalRow {
            table_name: "b".to_owned(),
            id: 2,
            value: "b-2".to_owned(),
        },
    ]
}

fn rusqlite_oracle_rows() -> Vec<FinalRow> {
    let conn = rusqlite::Connection::open_in_memory().expect("open C SQLite oracle");
    conn.execute_batch(
        "CREATE TABLE a (id INTEGER PRIMARY KEY, value TEXT);\
         CREATE TABLE b (id INTEGER PRIMARY KEY, value TEXT);\
         INSERT INTO a VALUES (1, 'a-1');\
         INSERT INTO b VALUES (1, 'b-1');\
         INSERT INTO a VALUES (2, 'a-2');\
         INSERT INTO b VALUES (2, 'b-2');\
         INSERT INTO a VALUES (3, 'a-3');",
    )
    .expect("execute deterministic C SQLite oracle workload");
    let mut statement = conn
        .prepare(
            "SELECT table_name, id, value FROM (\
             SELECT 'a' AS table_name, id, value FROM a \
             UNION ALL \
             SELECT 'b' AS table_name, id, value FROM b\
             ) ORDER BY table_name, id",
        )
        .expect("prepare C SQLite oracle query");
    statement
        .query_map([], |row| {
            Ok(FinalRow {
                table_name: row.get(0)?,
                id: row.get(1)?,
                value: row.get(2)?,
            })
        })
        .expect("query C SQLite oracle rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect C SQLite oracle rows")
}

fn assert_monotonic_publication(summary: &PublicationRunSummary) {
    assert!(
        summary.publication_events >= 4,
        "bead_id={BEAD_ID} case={}_publication_event_count observed={}",
        summary.mode,
        summary.publication_events
    );
    assert_eq!(
        summary.publication_events, summary.durable_certificate_records,
        "bead_id={BEAD_ID} case={}_every_publication_has_one_reconstructible_durable_certificate",
        summary.mode
    );
    assert!(
        summary.missing_fields_by_event.is_empty(),
        "bead_id={BEAD_ID} case={}_missing_fields observed={:?}",
        summary.mode,
        summary.missing_fields_by_event
    );
    // The combiner deliberately drops its ordered lock before structured
    // logging, so independently scheduled threads may emit adjacent receipts
    // in either log order. Validate the certificate order, not logger timing.
    let mut commit_seq_ranges = summary.commit_seq_ranges.clone();
    commit_seq_ranges.sort_unstable();
    for pair in commit_seq_ranges.windows(2) {
        assert_eq!(
            pair[1].0,
            pair[0].1 + 1,
            "bead_id={BEAD_ID} case={}_commit_sequence_contiguous ranges={:?}",
            summary.mode,
            commit_seq_ranges
        );
    }
    for mut sequence in [
        summary.durability_sequences.clone(),
        summary.publication_generations.clone(),
    ] {
        sequence.sort_unstable();
        for pair in sequence.windows(2) {
            assert_eq!(
                pair[1],
                pair[0] + 1,
                "bead_id={BEAD_ID} case={}_publication_counter_contiguous sequence={sequence:?}",
                summary.mode
            );
        }
    }
    assert!(
        summary.batch_sizes.iter().all(|size| *size > 0),
        "bead_id={BEAD_ID} case={}_nonempty_certificates",
        summary.mode
    );
}

#[test]
fn bd_3wop3_1_3_parallel_wal_publication_modes_are_semantically_equivalent() {
    let _guard = E2E_LOCK.lock().expect("publication e2e lock");
    let auto = spawn_mode_run("auto");
    let conservative = spawn_mode_run("conservative");
    let shadow_compare = spawn_mode_run("shadow_compare");

    let oracle_rows = rusqlite_oracle_rows();
    let expected_rows = expected_final_rows();
    assert_eq!(
        oracle_rows, expected_rows,
        "bead_id={BEAD_ID} case=fixture_matches_rusqlite_oracle"
    );
    for summary in [&auto, &conservative, &shadow_compare] {
        assert_eq!(
            summary.final_rows, oracle_rows,
            "bead_id={BEAD_ID} case={}_exact_rusqlite_row_oracle",
            summary.mode
        );
        assert_eq!(summary.integrity_check, "ok");
        assert_monotonic_publication(summary);
        assert!(
            summary
                .control_modes
                .iter()
                .any(|mode| mode == &summary.mode),
            "bead_id={BEAD_ID} case={}_control_mode observed={:?}",
            summary.mode,
            summary.control_modes
        );
    }
    assert_eq!(auto.final_rows, conservative.final_rows);
    assert_eq!(auto.final_rows, shadow_compare.final_rows);
    assert!(
        auto.lookup_modes
            .iter()
            .any(|mode| mode == "authoritative_index")
    );
    assert!(
        conservative
            .lookup_modes
            .iter()
            .any(|mode| mode == "conservative_index")
    );
    assert!(
        conservative
            .fallback_reasons
            .iter()
            .any(|reason| reason == "operator_forced")
    );
    assert!(
        shadow_compare
            .shadow_verdicts
            .iter()
            .any(|verdict| verdict == "clean")
    );

    let mut performance_matrix = Vec::with_capacity(PERFORMANCE_REPETITIONS * 4);
    for thread_count in [8_usize, 16_usize] {
        for repetition in 0..PERFORMANCE_REPETITIONS {
            // Alternate pair order so warmup, scheduler, and host drift do not
            // systematically favor either operating mode.
            let mode_order = if repetition % 2 == 0 {
                ["conservative", "auto"]
            } else {
                ["auto", "conservative"]
            };
            for mode in mode_order {
                performance_matrix.push(spawn_performance_run(mode, thread_count, repetition));
            }
        }
    }
    for cell in &performance_matrix {
        assert_eq!(cell.row_count, cell.committed_transactions);
        assert_eq!(cell.integrity_check, "ok");
        assert!(cell.publication_events > 0);
        assert!(cell.ordered_region_total_ns > 0);
        assert!(cell.throughput_transactions_per_second > 0);
    }
    println!(
        "FSQLITE_D1_PERFORMANCE_CELLS_JSON={}",
        serde_json::to_string(&performance_matrix).expect("serialize performance cells")
    );
    let performance_comparisons = [8_usize, 16_usize].map(|thread_count| {
        let conservative_cells = performance_matrix
            .iter()
            .filter(|cell| cell.thread_count == thread_count && cell.mode == "conservative")
            .collect::<Vec<_>>();
        let auto_cells = performance_matrix
            .iter()
            .filter(|cell| cell.thread_count == thread_count && cell.mode == "auto")
            .collect::<Vec<_>>();
        assert_eq!(conservative_cells.len(), PERFORMANCE_REPETITIONS);
        assert_eq!(auto_cells.len(), PERFORMANCE_REPETITIONS);
        let conservative_ordered_ns_per_commit = conservative_cells
            .iter()
            .map(|cell| cell.ordered_region_ns_per_commit)
            .collect::<Vec<_>>();
        let auto_ordered_ns_per_commit = auto_cells
            .iter()
            .map(|cell| cell.ordered_region_ns_per_commit)
            .collect::<Vec<_>>();
        let conservative_throughput = conservative_cells
            .iter()
            .map(|cell| cell.throughput_transactions_per_second)
            .collect::<Vec<_>>();
        let auto_throughput = auto_cells
            .iter()
            .map(|cell| cell.throughput_transactions_per_second)
            .collect::<Vec<_>>();
        let conservative_ordered_median = median(&conservative_ordered_ns_per_commit);
        let auto_ordered_median = median(&auto_ordered_ns_per_commit);
        let conservative_throughput_median = median(&conservative_throughput);
        let auto_throughput_median = median(&auto_throughput);
        assert!(
            auto_ordered_median < conservative_ordered_median,
            "bead_id={BEAD_ID} case=centralized_publication_cost_shrank threads={thread_count} before_ns_per_commit={} after_ns_per_commit={}",
            conservative_ordered_median,
            auto_ordered_median
        );
        let minimum_accepted_throughput = conservative_throughput_median
            .saturating_mul(100 - MAX_THROUGHPUT_REGRESSION_PERCENT)
            .div_ceil(100);
        assert!(
            auto_throughput_median >= minimum_accepted_throughput,
            "bead_id={BEAD_ID} case=centralized_work_not_relocated threads={thread_count} before_tps={} after_tps={} minimum_after_tps={} tolerance_percent={MAX_THROUGHPUT_REGRESSION_PERCENT}",
            conservative_throughput_median,
            auto_throughput_median,
            minimum_accepted_throughput
        );
        assert!(
            conservative_cells.iter().chain(&auto_cells).all(|cell| {
                cell.row_count == cell.committed_transactions && cell.integrity_check == "ok"
            }),
            "bead_id={BEAD_ID} case=performance_matrix_semantics_preserved threads={thread_count}"
        );
        serde_json::json!({
            "thread_count": thread_count,
            "repetitions": PERFORMANCE_REPETITIONS,
            "pair_order": "alternating",
            "aggregation": "median",
            "before_evidence_ids": conservative_cells
                .iter()
                .map(|cell| &cell.evidence_id)
                .collect::<Vec<_>>(),
            "after_evidence_ids": auto_cells
                .iter()
                .map(|cell| &cell.evidence_id)
                .collect::<Vec<_>>(),
            "before_ordered_region_ns_per_commit_samples": conservative_ordered_ns_per_commit,
            "after_ordered_region_ns_per_commit_samples": auto_ordered_ns_per_commit,
            "before_ordered_region_ns_per_commit_median": conservative_ordered_median,
            "after_ordered_region_ns_per_commit_median": auto_ordered_median,
            "ordered_region_reduction_ns_per_commit": conservative_ordered_median
                .saturating_sub(auto_ordered_median),
            "ordered_region_shrank": auto_ordered_median < conservative_ordered_median,
            "before_throughput_transactions_per_second_samples": conservative_throughput,
            "after_throughput_transactions_per_second_samples": auto_throughput,
            "before_throughput_transactions_per_second_median": conservative_throughput_median,
            "after_throughput_transactions_per_second_median": auto_throughput_median,
            "maximum_accepted_throughput_regression_percent":
                MAX_THROUGHPUT_REGRESSION_PERCENT,
            "minimum_accepted_throughput_transactions_per_second":
                minimum_accepted_throughput,
            "throughput_within_regression_budget": auto_throughput_median
                >= minimum_accepted_throughput,
            "semantics_preserved": true,
        })
    });

    let artifact = serde_json::json!({
        "bead_id": BEAD_ID,
        "scenario_id": SCENARIO_ID,
        "compatibility_selector": COMPATIBILITY_SELECTOR,
        "selector_coverage": ["wal_invariant", "integrity_check", "row_level"],
        "claim_id": BEAD_ID,
        "evidence_owner": EVIDENCE_OWNER,
        "verification_class": VERIFICATION_CLASS,
        "oracle": ORACLE,
        "allowed_difference_policy": ALLOWED_DIFFERENCE_POLICY,
        "baseline_comparator": BASELINE_COMPARATOR,
        "policy_id": POLICY_ID,
        "oracle_rows": &oracle_rows,
        "oracle_verdict": "exact_match",
        "shadow_lineage": {
            "shadow_compare_evidence_id": &shadow_compare.evidence_id,
            "conservative_baseline_evidence_id": &conservative.evidence_id,
            "verdict": "clean",
        },
        "fallback_lineage": {
            "conservative_evidence_id": &conservative.evidence_id,
            "fallback_reason": "operator_forced",
        },
        "centralized_publication_cost_matrix": {
            "methodology": "production file-backed FrankenSQLite disjoint-table concurrent commits; conservative retains raw frame preparation inside the durability combiner and auto performs lane/fallback preparation before the tiny ordered residue; each 8-thread and 16-thread comparison uses four alternating-order pairs and the median, ordered-region cost comes only from lock-held production durability-combiner receipts, and a declared 5 percent median wall-throughput non-regression budget guards against opaque relocation",
            "cells": &performance_matrix,
            "comparisons": performance_comparisons,
        },
        "replay_command": REPLAY_COMMAND,
        "runs": {
            "auto": &auto,
            "conservative": &conservative,
            "shadow_compare": &shadow_compare,
        }
    });

    if let Ok(path) = env::var(REPORT_ARTIFACT_ENV) {
        let artifact_path = PathBuf::from(path);
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).expect("create report artifact parent");
        }
        fs::write(
            artifact_path,
            serde_json::to_vec_pretty(&artifact).expect("serialize report artifact"),
        )
        .expect("write report artifact");
    }
    println!(
        "FSQLITE_D1_REPORT_JSON={}",
        serde_json::to_string(&artifact).expect("serialize compact report artifact")
    );
}

#[test]
#[ignore = "helper entrypoint for the parent test harness; not meant to run directly"]
fn bd_3wop3_1_3_parallel_wal_publication_child_entrypoint() {
    let run_dir = PathBuf::from(
        env::var(CHILD_RUN_DIR_ENV)
            .expect("child run dir env must be present for child entrypoint"),
    );
    fs::create_dir_all(&run_dir).expect("create child run dir");
    let mode = env::var(CHILD_MODE_ENV).expect("child mode env must be present");
    let summary = run_child_workload(&run_dir, &mode);
    fs::write(
        run_dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary).expect("serialize child summary"),
    )
    .expect("write child summary");
}

#[test]
#[ignore = "helper entrypoint for the production performance matrix"]
fn bd_3wop3_1_3_parallel_wal_publication_performance_child_entrypoint() {
    let run_dir = PathBuf::from(
        env::var(CHILD_RUN_DIR_ENV).expect("performance child run dir env must be present"),
    );
    fs::create_dir_all(&run_dir).expect("create performance child run dir");
    let mode = env::var(CHILD_MODE_ENV).expect("performance child mode env must be present");
    let thread_count = env::var(PERFORMANCE_THREADS_ENV)
        .expect("performance thread-count env must be present")
        .parse::<usize>()
        .expect("performance thread count must be usize");
    let repetition = env::var(PERFORMANCE_REPETITION_ENV)
        .expect("performance repetition env must be present")
        .parse::<usize>()
        .expect("performance repetition must be usize");
    assert!(matches!(thread_count, 8 | 16));
    assert!(repetition < PERFORMANCE_REPETITIONS);
    let summary = run_performance_workload(&run_dir, &mode, thread_count, repetition);
    fs::write(
        run_dir.join("performance-summary.json"),
        serde_json::to_vec_pretty(&summary).expect("serialize performance child summary"),
    )
    .expect("write performance child summary");
}
