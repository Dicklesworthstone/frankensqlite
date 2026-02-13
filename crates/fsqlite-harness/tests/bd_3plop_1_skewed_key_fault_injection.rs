use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use serde::Serialize;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-3plop.1";
const HOT_KEY_COUNT: u64 = 100;
const COLD_KEY_COUNT: u64 = 10_000;
const DEFAULT_OPS_PER_CASE: usize = 80;
const CONCURRENT_THREADS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum KeyDistribution {
    ZipfS1,
    ZipfS2,
    Sequential,
    ReverseSequential,
    HotCold,
}

impl KeyDistribution {
    const ALL: [Self; 5] = [
        Self::ZipfS1,
        Self::ZipfS2,
        Self::Sequential,
        Self::ReverseSequential,
        Self::HotCold,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::ZipfS1 => "zipf_s_1_0",
            Self::ZipfS2 => "zipf_s_2_0",
            Self::Sequential => "sequential_monotonic",
            Self::ReverseSequential => "reverse_sequential",
            Self::HotCold => "hot_cold",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum WorkloadKind {
    Insert,
    Update,
    Mixed,
    Concurrent,
}

impl WorkloadKind {
    const ALL: [Self; 4] = [Self::Insert, Self::Update, Self::Mixed, Self::Concurrent];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Mixed => "mixed",
            Self::Concurrent => "concurrent_32_threads",
        }
    }
}

#[derive(Debug, Default)]
struct ThreadStats {
    committed_ops: usize,
    aborted_ops: usize,
    hot_hits: usize,
    cold_hits: usize,
    key_touches: HashMap<u64, usize>,
}

#[derive(Debug, Serialize)]
struct CaseArtifact {
    run_id: String,
    distribution: String,
    workload: String,
    seed: u64,
    requested_ops: usize,
    committed_ops: usize,
    aborted_ops: usize,
    row_count: u64,
    integrity_ok: bool,
    hot_hit_rate: f64,
    max_version_chain_len_proxy: usize,
    btree_order_check_ok: bool,
    elapsed_ms: u128,
}

#[derive(Debug, Serialize)]
struct SuiteArtifact {
    schema_version: u32,
    bead_id: String,
    run_id: String,
    ops_per_case: usize,
    cases: Vec<CaseArtifact>,
    distribution_analysis: Vec<DistributionSummary>,
    acceptance_checks: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DistributionSummary {
    distribution: String,
    workloads_covered: usize,
    total_committed_ops: usize,
    max_chain_proxy: usize,
    avg_hot_hit_rate: f64,
    expectation: String,
    expectation_met: bool,
    all_integrity_ok: bool,
    all_btree_order_ok: bool,
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    *state
}

fn ops_per_case() -> usize {
    std::env::var("BD_3PLOP_OPS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_OPS_PER_CASE)
}

fn key_space(distribution: KeyDistribution) -> u64 {
    match distribution {
        KeyDistribution::HotCold => HOT_KEY_COUNT + COLD_KEY_COUNT,
        _ => 8_192,
    }
}

fn sample_zipf_like(state: &mut u64, hot_ratio_pct: u64, hot_count: u64, total_count: u64) -> u64 {
    let pick = lcg_next(state) % 100;
    if pick < hot_ratio_pct {
        1 + (lcg_next(state) % hot_count.max(1))
    } else {
        hot_count + 1 + (lcg_next(state) % (total_count.saturating_sub(hot_count).max(1)))
    }
}

fn sample_key(distribution: KeyDistribution, state: &mut u64, seq_idx: usize) -> u64 {
    let space = key_space(distribution);
    match distribution {
        KeyDistribution::ZipfS1 => sample_zipf_like(state, 80, (space / 5).max(1), space),
        KeyDistribution::ZipfS2 => sample_zipf_like(state, 95, (space / 20).max(1), space),
        KeyDistribution::Sequential => {
            let idx = u64::try_from(seq_idx).expect("sequence index fits u64");
            1 + (idx % space.max(1))
        }
        KeyDistribution::ReverseSequential => {
            let idx = u64::try_from(seq_idx).expect("sequence index fits u64");
            space.saturating_sub(idx % space.max(1))
        }
        KeyDistribution::HotCold => sample_zipf_like(state, 99, HOT_KEY_COUNT, space),
    }
}

fn is_hot_key(distribution: KeyDistribution, key: u64) -> bool {
    match distribution {
        KeyDistribution::HotCold => key <= HOT_KEY_COUNT,
        KeyDistribution::ZipfS1 => key <= key_space(distribution) / 5,
        KeyDistribution::ZipfS2 => key <= key_space(distribution) / 20,
        KeyDistribution::Sequential | KeyDistribution::ReverseSequential => {
            key <= key_space(distribution) / 5
        }
    }
}

fn hot_hit_expectation(distribution: KeyDistribution) -> (f64, f64, &'static str) {
    match distribution {
        KeyDistribution::ZipfS1 => (0.60, 1.00, "zipf_s_1_0 should keep hot hit rate >= 0.60"),
        KeyDistribution::ZipfS2 => (0.85, 1.00, "zipf_s_2_0 should keep hot hit rate >= 0.85"),
        KeyDistribution::Sequential => (
            0.80,
            1.00,
            "sequential_monotonic should stay concentrated on a hot prefix",
        ),
        KeyDistribution::ReverseSequential => (
            0.00,
            0.20,
            "reverse_sequential should avoid the hot-prefix key range",
        ),
        KeyDistribution::HotCold => (
            0.95,
            1.00,
            "hot_cold should heavily favor the hot key set (>= 0.95)",
        ),
    }
}

fn execute_insert(conn: &Connection, key: u64) -> Result<(), String> {
    let sql = format!("INSERT OR IGNORE INTO kv (id, v) VALUES ({key}, 1)");
    conn.execute(&sql)
        .map_err(|error| format!("insert_failed key={key} error={error}"))?;
    let sql = format!("UPDATE kv SET v = v + 1 WHERE id = {key}");
    conn.execute(&sql)
        .map_err(|error| format!("post_insert_update_failed key={key} error={error}"))?;
    Ok(())
}

fn execute_update(conn: &Connection, key: u64) -> Result<(), String> {
    let sql = format!("UPDATE kv SET v = v + 1 WHERE id = {key}");
    let changed = conn
        .execute(&sql)
        .map_err(|error| format!("update_failed key={key} error={error}"))?;
    if changed == 0 {
        execute_insert(conn, key)?;
    }
    Ok(())
}

fn configure_connection(conn: &Connection) -> Result<(), String> {
    conn.execute("PRAGMA journal_mode = WAL")
        .map_err(|error| format!("pragma_wal_failed error={error}"))?;
    conn.execute("PRAGMA synchronous = NORMAL")
        .map_err(|error| format!("pragma_sync_failed error={error}"))?;
    conn.execute("PRAGMA busy_timeout = 25")
        .map_err(|error| format!("pragma_busy_timeout_failed error={error}"))?;
    Ok(())
}

fn setup_database(path: &str) -> Result<(), String> {
    let conn =
        Connection::open(path).map_err(|error| format!("open_failed path={path} {error}"))?;
    configure_connection(&conn)?;
    conn.execute("CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
        .map_err(|error| format!("create_table_failed error={error}"))?;
    Ok(())
}

fn prepopulate_for_update(
    path: &str,
    distribution: KeyDistribution,
    target_rows: u64,
) -> Result<(), String> {
    let conn =
        Connection::open(path).map_err(|error| format!("open_failed path={path} {error}"))?;
    configure_connection(&conn)?;
    let row_limit = target_rows.max(1).min(key_space(distribution));
    for key in 1..=row_limit {
        let sql = format!("INSERT OR IGNORE INTO kv (id, v) VALUES ({key}, 0)");
        conn.execute(&sql)
            .map_err(|error| format!("prepopulate_failed key={key} error={error}"))?;
    }
    Ok(())
}

fn row_count(conn: &Connection) -> Result<u64, String> {
    let rows = conn
        .query("SELECT count(*) FROM kv")
        .map_err(|error| format!("count_query_failed error={error}"))?;
    let Some(row) = rows.first() else {
        return Err("count_query_empty_result".to_owned());
    };
    match row.get(0) {
        Some(SqliteValue::Integer(value)) => {
            u64::try_from(*value).map_err(|_| format!("negative_row_count value={value}"))
        }
        Some(other) => Err(format!("unexpected_count_type value={other:?}")),
        None => Err("missing_count_column".to_owned()),
    }
}

fn integrity_ok(conn: &Connection) -> Result<bool, String> {
    let rows = conn
        .query("PRAGMA integrity_check")
        .map_err(|error| format!("integrity_check_failed error={error}"))?;
    let Some(row) = rows.first() else {
        return Err("integrity_check_empty".to_owned());
    };
    match row.get(0) {
        Some(SqliteValue::Text(value)) => Ok(value.eq_ignore_ascii_case("ok")),
        Some(other) => Err(format!("unexpected_integrity_type value={other:?}")),
        None => Err("missing_integrity_column".to_owned()),
    }
}

fn btree_order_check(conn: &Connection) -> Result<bool, String> {
    let rows = conn
        .query("SELECT id FROM kv ORDER BY id ASC")
        .map_err(|error| format!("order_query_failed error={error}"))?;
    let mut previous = None::<i64>;
    for row in &rows {
        let Some(SqliteValue::Integer(value)) = row.get(0) else {
            return Err("unexpected_order_column_type".to_owned());
        };
        if let Some(prev) = previous {
            if *value < prev {
                return Ok(false);
            }
        }
        previous = Some(*value);
    }
    Ok(true)
}

fn run_thread_workload(
    path: String,
    distribution: KeyDistribution,
    workload: WorkloadKind,
    seed: u64,
    ops: usize,
    thread_idx: usize,
) -> ThreadStats {
    let mut stats = ThreadStats::default();
    let mut state = seed ^ (u64::try_from(thread_idx).expect("thread index fits u64") << 32);
    let conn = match Connection::open(&path) {
        Ok(conn) => conn,
        Err(_) => {
            stats.aborted_ops = ops;
            return stats;
        }
    };
    let _ = configure_connection(&conn);

    for step in 0..ops {
        let seq = thread_idx
            .checked_mul(ops)
            .and_then(|base| base.checked_add(step))
            .unwrap_or(step);
        let key = sample_key(distribution, &mut state, seq);
        let op_result = match workload {
            WorkloadKind::Insert => execute_insert(&conn, key),
            WorkloadKind::Update => execute_update(&conn, key),
            WorkloadKind::Mixed | WorkloadKind::Concurrent => {
                if (lcg_next(&mut state) & 1) == 0 {
                    execute_insert(&conn, key)
                } else {
                    execute_update(&conn, key)
                }
            }
        };

        match op_result {
            Ok(()) => {
                stats.committed_ops += 1;
                *stats.key_touches.entry(key).or_insert(0) += 1;
                if is_hot_key(distribution, key) {
                    stats.hot_hits += 1;
                } else {
                    stats.cold_hits += 1;
                }
            }
            Err(_) => {
                stats.aborted_ops += 1;
            }
        }
    }

    stats
}

fn merge_thread_stats(aggregate: &mut ThreadStats, thread_stats: ThreadStats) {
    aggregate.committed_ops += thread_stats.committed_ops;
    aggregate.aborted_ops += thread_stats.aborted_ops;
    aggregate.hot_hits += thread_stats.hot_hits;
    aggregate.cold_hits += thread_stats.cold_hits;
    for (key, touches) in thread_stats.key_touches {
        *aggregate.key_touches.entry(key).or_insert(0) += touches;
    }
}

fn run_concurrent_db_workload(
    path: String,
    distribution: KeyDistribution,
    seed: u64,
    ops_per_thread: usize,
) -> Result<ThreadStats, String> {
    let mut handles = Vec::with_capacity(CONCURRENT_THREADS);

    for thread_idx in 0..CONCURRENT_THREADS {
        let thread_path = path.clone();
        handles.push(thread::spawn(move || {
            run_thread_workload(
                thread_path,
                distribution,
                WorkloadKind::Concurrent,
                seed,
                ops_per_thread,
                thread_idx,
            )
        }));
    }

    let mut aggregate = ThreadStats::default();
    for handle in handles {
        let thread_stats = handle
            .join()
            .map_err(|_| "thread_join_failed_possible_deadlock".to_owned())?;
        merge_thread_stats(&mut aggregate, thread_stats);
    }
    Ok(aggregate)
}

fn build_distribution_analysis(cases: &[CaseArtifact]) -> Result<Vec<DistributionSummary>, String> {
    let mut by_distribution = BTreeMap::<String, Vec<&CaseArtifact>>::new();
    for case in cases {
        by_distribution
            .entry(case.distribution.clone())
            .or_default()
            .push(case);
    }

    let mut summaries = Vec::with_capacity(by_distribution.len());
    for distribution in KeyDistribution::ALL {
        let dist_key = distribution.as_str().to_owned();
        let cases_for_dist = by_distribution
            .get(&dist_key)
            .ok_or_else(|| format!("missing_distribution_cases distribution={dist_key}"))?;

        let workloads_covered = cases_for_dist
            .iter()
            .map(|case| case.workload.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let total_committed_ops = cases_for_dist.iter().map(|case| case.committed_ops).sum();
        let max_chain_proxy = cases_for_dist
            .iter()
            .map(|case| case.max_version_chain_len_proxy)
            .max()
            .unwrap_or(0);
        let integrity_ok = cases_for_dist.iter().all(|case| case.integrity_ok);
        let btree_ok = cases_for_dist.iter().all(|case| case.btree_order_check_ok);

        #[allow(clippy::cast_precision_loss)]
        let avg_hot_hit_rate = if cases_for_dist.is_empty() {
            0.0
        } else {
            cases_for_dist
                .iter()
                .map(|case| case.hot_hit_rate)
                .sum::<f64>()
                / (cases_for_dist.len() as f64)
        };
        let (min_expected, max_expected, expectation) = hot_hit_expectation(distribution);
        let expectation_met = (min_expected..=max_expected).contains(&avg_hot_hit_rate);

        summaries.push(DistributionSummary {
            distribution: dist_key,
            workloads_covered,
            total_committed_ops,
            max_chain_proxy,
            avg_hot_hit_rate,
            expectation: expectation.to_owned(),
            expectation_met,
            all_integrity_ok: integrity_ok,
            all_btree_order_ok: btree_ok,
        });
    }

    Ok(summaries)
}

fn run_case(
    distribution: KeyDistribution,
    workload: WorkloadKind,
    suite_seed: u64,
    case_seed: u64,
    ops: usize,
) -> Result<CaseArtifact, String> {
    let temp_dir = tempdir().map_err(|error| format!("tempdir_failed error={error}"))?;
    let db_path = temp_dir.path().join("skewed-fault.db");
    let db_str = db_path
        .to_str()
        .ok_or_else(|| "db_path_utf8_failed".to_owned())?
        .to_owned();
    setup_database(&db_str)?;

    if matches!(workload, WorkloadKind::Update) {
        let prepopulate_rows =
            u64::try_from(ops.saturating_mul(2).max(256)).expect("prepopulate row count fits u64");
        prepopulate_for_update(&db_str, distribution, prepopulate_rows)?;
    }

    let started = Instant::now();
    let (aggregate, requested_ops) = if matches!(workload, WorkloadKind::Concurrent) {
        let ops_per_thread = (ops / CONCURRENT_THREADS).max(4);
        let requested_ops = ops_per_thread.saturating_mul(CONCURRENT_THREADS);
        let aggregate = run_concurrent_db_workload(
            db_str.clone(),
            distribution,
            suite_seed ^ case_seed,
            ops_per_thread,
        )?;
        (aggregate, requested_ops)
    } else {
        (
            run_thread_workload(
                db_str.clone(),
                distribution,
                workload,
                suite_seed ^ case_seed,
                ops,
                0,
            ),
            ops,
        )
    };

    let conn = Connection::open(&db_str).map_err(|error| format!("open_final_failed {error}"))?;
    let row_count = row_count(&conn)?;
    let integrity_ok = integrity_ok(&conn)?;
    let btree_order_check_ok = btree_order_check(&conn)?;

    let hot_total = aggregate.hot_hits + aggregate.cold_hits;
    #[allow(clippy::cast_precision_loss)]
    let hot_hit_rate = if hot_total == 0 {
        0.0
    } else {
        (aggregate.hot_hits as f64) / (hot_total as f64)
    };
    let max_version_chain_len_proxy = aggregate.key_touches.values().copied().max().unwrap_or(0);
    let elapsed_ms = started.elapsed().as_millis();

    Ok(CaseArtifact {
        run_id: format!(
            "{}-{}-{}-{}",
            BEAD_ID,
            distribution.as_str(),
            workload.as_str(),
            case_seed
        ),
        distribution: distribution.as_str().to_owned(),
        workload: workload.as_str().to_owned(),
        seed: case_seed,
        requested_ops,
        committed_ops: aggregate.committed_ops,
        aborted_ops: aggregate.aborted_ops,
        row_count,
        integrity_ok,
        hot_hit_rate,
        max_version_chain_len_proxy,
        btree_order_check_ok,
        elapsed_ms,
    })
}

fn write_suite_artifact(suite: &SuiteArtifact) -> Result<PathBuf, String> {
    let root = workspace_root()?;
    let output_dir = root.join("test-results").join("bd_3plop_1");
    fs::create_dir_all(&output_dir).map_err(|error| {
        format!(
            "artifact_dir_create_failed path={} error={error}",
            output_dir.display()
        )
    })?;
    let output_path = output_dir.join(format!("{}.json", suite.run_id));
    let payload = serde_json::to_string_pretty(suite)
        .map_err(|error| format!("artifact_serialize_failed error={error}"))?;
    fs::write(&output_path, payload).map_err(|error| {
        format!(
            "artifact_write_failed path={} error={error}",
            output_path.display()
        )
    })?;
    Ok(output_path)
}

#[test]
fn skewed_key_sampling_is_deterministic() {
    let mut first = BTreeMap::new();
    let mut second = BTreeMap::new();

    for distribution in KeyDistribution::ALL {
        let mut a_state = 0xDEAD_BEEF_u64;
        let mut b_state = 0xDEAD_BEEF_u64;
        let a = (0..128)
            .map(|idx| sample_key(distribution, &mut a_state, idx))
            .collect::<Vec<_>>();
        let b = (0..128)
            .map(|idx| sample_key(distribution, &mut b_state, idx))
            .collect::<Vec<_>>();
        first.insert(distribution.as_str(), a);
        second.insert(distribution.as_str(), b);
    }

    assert_eq!(
        first, second,
        "bead_id={BEAD_ID} sampling must be deterministic for fixed seed"
    );
}

#[test]
fn test_e2e_bd_3plop_1_skewed_key_fault_injection() {
    let suite_seed = 0x5EED_3A10_u64;
    let ops = ops_per_case();
    let mut artifacts = Vec::new();

    for (dist_idx, distribution) in KeyDistribution::ALL.into_iter().enumerate() {
        for (workload_idx, workload) in WorkloadKind::ALL.into_iter().enumerate() {
            let case_seed = suite_seed
                + (u64::try_from(dist_idx).expect("dist index fits u64") * 1_000)
                + (u64::try_from(workload_idx).expect("workload index fits u64") * 10);

            let artifact = run_case(distribution, workload, suite_seed, case_seed, ops)
                .unwrap_or_else(|error| {
                    panic!(
                        "bead_id={BEAD_ID} distribution={} workload={} case_seed={} failed: {error}",
                        distribution.as_str(),
                        workload.as_str(),
                        case_seed
                    )
                });

            assert!(
                artifact.integrity_ok,
                "bead_id={BEAD_ID} integrity_check failed distribution={} workload={}",
                artifact.distribution, artifact.workload
            );
            assert!(
                artifact.btree_order_check_ok,
                "bead_id={BEAD_ID} btree order check failed distribution={} workload={}",
                artifact.distribution, artifact.workload
            );

            eprintln!(
                "INFO bead_id={BEAD_ID} case=skewed_fault_run distribution={} workload={} seed={} requested_ops={} committed_ops={} aborted_ops={} row_count={} hot_hit_rate={:.4} max_chain_proxy={} elapsed_ms={}",
                artifact.distribution,
                artifact.workload,
                artifact.seed,
                artifact.requested_ops,
                artifact.committed_ops,
                artifact.aborted_ops,
                artifact.row_count,
                artifact.hot_hit_rate,
                artifact.max_version_chain_len_proxy,
                artifact.elapsed_ms,
            );

            artifacts.push(artifact);
        }
    }

    let distribution_analysis =
        build_distribution_analysis(&artifacts).expect("distribution analysis must be computed");
    for summary in &distribution_analysis {
        assert_eq!(
            summary.workloads_covered,
            WorkloadKind::ALL.len(),
            "bead_id={BEAD_ID} distribution={} must include all workloads",
            summary.distribution
        );
        assert!(
            summary.all_integrity_ok,
            "bead_id={BEAD_ID} distribution={} must preserve integrity_check",
            summary.distribution
        );
        assert!(
            summary.all_btree_order_ok,
            "bead_id={BEAD_ID} distribution={} must preserve btree ordering",
            summary.distribution
        );
        assert!(
            summary.expectation_met,
            "bead_id={BEAD_ID} distribution={} avg_hot_hit_rate={} violated expectation={}",
            summary.distribution, summary.avg_hot_hit_rate, summary.expectation
        );
        eprintln!(
            "INFO bead_id={BEAD_ID} case=distribution_analysis distribution={} workloads={} total_committed_ops={} max_chain_proxy={} avg_hot_hit_rate={:.4} expectation_met={}",
            summary.distribution,
            summary.workloads_covered,
            summary.total_committed_ops,
            summary.max_chain_proxy,
            summary.avg_hot_hit_rate,
            summary.expectation_met,
        );
    }

    let acceptance_checks = vec![
        "all 5 distributions executed across 4 workload types".to_owned(),
        "real concurrent workload executed with 32 writer threads".to_owned(),
        "integrity_check + btree ordering validated for every case".to_owned(),
        "distribution-specific hot-hit expectations validated".to_owned(),
        "version-chain pressure proxy captured per case/distribution".to_owned(),
    ];

    let run_id = format!("{BEAD_ID}-{}", suite_seed);
    let suite = SuiteArtifact {
        schema_version: 1,
        bead_id: BEAD_ID.to_owned(),
        run_id: run_id.clone(),
        ops_per_case: ops,
        cases: artifacts,
        distribution_analysis,
        acceptance_checks,
    };
    let output_path = write_suite_artifact(&suite).expect("suite artifact should be written");
    eprintln!(
        "INFO bead_id={BEAD_ID} case=suite_artifact path={} run_id={} cases={}",
        output_path.display(),
        run_id,
        suite.cases.len()
    );
}
