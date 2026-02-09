use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_mvcc::{
    AmsWindowCollector, AmsWindowCollectorConfig, AmsWindowEstimate, SpaceSavingEntry,
    dedup_write_set, mix64,
};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::{Value, json};

const BEAD_ID: &str = "bd-3u2v";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_3u2v_unit_compliance_gate",
    "prop_bd_3u2v_structure_compliance",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_3u2v_compliance", "test_e2e_bd_3u2v"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 9] = [
    "test_bd_3u2v_unit_compliance_gate",
    "prop_bd_3u2v_structure_compliance",
    "test_e2e_bd_3u2v_compliance",
    "test_e2e_bd_3u2v",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "bd-1fpm",
];

#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct ComplianceEvaluation {
    missing_unit_ids: Vec<&'static str>,
    missing_e2e_ids: Vec<&'static str>,
    missing_log_levels: Vec<&'static str>,
    missing_log_standard_ref: bool,
}

impl ComplianceEvaluation {
    fn is_compliant(&self) -> bool {
        self.missing_unit_ids.is_empty()
            && self.missing_e2e_ids.is_empty()
            && self.missing_log_levels.is_empty()
            && !self.missing_log_standard_ref
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
struct TraceDigest {
    top10_true: Vec<u64>,
    top10_estimated: Vec<u64>,
    overlap_count: usize,
    head_contrib_upper_bits: u64,
    head_contrib_lower_bits: u64,
    tail_contrib_hat_bits: u64,
    zipf_s_hat_bits: u64,
    zipf_window_n: u64,
    ledger_rows: Vec<(u64, u64, u64)>,
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
struct TraceOutcome {
    digest: TraceDigest,
    overlap_pct: f64,
    zipf_s_hat: f64,
    estimate: AmsWindowEstimate,
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn load_issue_description(issue_id: &str) -> Result<String, String> {
    let issues_path = workspace_root()?.join(ISSUES_JSONL);
    let raw = fs::read_to_string(&issues_path).map_err(|error| {
        format!(
            "issues_jsonl_read_failed path={} error={error}",
            issues_path.display()
        )
    })?;

    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line)
            .map_err(|error| format!("issues_jsonl_parse_failed error={error} line={line}"))?;
        if value.get("id").and_then(Value::as_str) == Some(issue_id) {
            let mut canonical = value
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();

            if let Some(comments) = value.get("comments").and_then(Value::as_array) {
                for comment in comments {
                    if let Some(text) = comment.get("text").and_then(Value::as_str) {
                        canonical.push_str("\n\n");
                        canonical.push_str(text);
                    }
                }
            }

            return Ok(canonical);
        }
    }

    Err(format!("bead_id={issue_id} not_found_in={ISSUES_JSONL}"))
}

fn contains_identifier(text: &str, expected_marker: &str) -> bool {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|part| part == expected_marker)
}

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !contains_identifier(description, id))
        .collect::<Vec<_>>();

    let missing_log_levels = LOG_LEVEL_MARKERS
        .into_iter()
        .filter(|level| !description.contains(level))
        .collect::<Vec<_>>();

    ComplianceEvaluation {
        missing_unit_ids,
        missing_e2e_ids,
        missing_log_levels,
        missing_log_standard_ref: !description.contains(LOG_STANDARD_REF),
    }
}

fn unique_runtime_dir(label: &str) -> Result<PathBuf, String> {
    let root = workspace_root()?.join("target").join("bd_3u2v_runtime");
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "runtime_dir_create_failed path={} error={error}",
            root.display()
        )
    })?;

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let path = root.join(format!("{label}_{}_{}", std::process::id(), stamp));
    fs::create_dir_all(&path).map_err(|error| {
        format!(
            "runtime_subdir_create_failed path={} error={error}",
            path.display()
        )
    })?;
    Ok(path)
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn zipf_page(seed: u64, page_count: u64, s: f64) -> u64 {
    debug_assert!(page_count > 0);
    debug_assert!(s > 0.0);
    let u = (mix64(seed) as f64 + 1.0) / (u64::MAX as f64 + 1.0);
    let normalization: f64 = (1..=page_count).map(|rank| (rank as f64).powf(-s)).sum();
    let mut cumulative = 0.0_f64;
    for rank in 1..=page_count {
        cumulative += (rank as f64).powf(-s) / normalization;
        if u <= cumulative {
            return rank - 1;
        }
    }
    page_count - 1
}

fn top10_exact(exact: &HashMap<u64, u64>) -> Vec<u64> {
    let mut entries = exact
        .iter()
        .map(|(&pgno, &count)| (pgno, count))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    entries.into_iter().take(10).map(|(pgno, _)| pgno).collect()
}

fn top10_estimated(entries: &[SpaceSavingEntry]) -> Vec<u64> {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| {
        right
            .count_hat
            .cmp(&left.count_hat)
            .then(left.pgno.cmp(&right.pgno))
    });
    sorted
        .into_iter()
        .take(10)
        .map(|entry| entry.pgno)
        .collect()
}

fn run_zipf_trace() -> TraceOutcome {
    let mut collector = AmsWindowCollector::new(
        AmsWindowCollectorConfig {
            r: 32,
            db_epoch: 1,
            regime_id: 18,
            window_width_ticks: 1_000,
            track_exact_m2: true,
            track_heavy_hitters: true,
            heavy_hitter_k: 64,
            estimate_zipf: true,
        },
        0,
    );

    let mut exact = HashMap::<u64, u64>::new();
    let page_count = 64u64;
    let zipf_s = 0.99;
    for txn_id in 0u64..300 {
        let writer = txn_id % 4;
        let mut write_set = Vec::with_capacity(8);
        for idx in 0u64..8 {
            let seed = txn_id.wrapping_mul(9_973) ^ writer.wrapping_mul(151) ^ idx;
            write_set.push(zipf_page(seed, page_count, zipf_s));
        }
        let dedup = dedup_write_set(&write_set);
        for &pgno in &dedup {
            *exact.entry(pgno).or_default() += 1;
        }
        let _closed = collector.observe_commit_attempt(txn_id, &dedup);
    }

    let estimate = collector.force_flush(300);
    let head_tail = estimate
        .head_tail
        .expect("head_tail decomposition must be present when heavy hitters are enabled");
    let zipf = estimate
        .zipf
        .expect("zipf estimate must be present when estimate_zipf=true");
    let ledger = estimate.to_evidence_ledger();

    let top_true = top10_exact(&exact);
    let top_estimated = top10_estimated(&estimate.heavy_hitters);
    let true_set: BTreeSet<u64> = top_true.iter().copied().collect();
    let estimated_set: BTreeSet<u64> = top_estimated.iter().copied().collect();
    let overlap_count = true_set.intersection(&estimated_set).count();
    let overlap_pct = overlap_count as f64 / 10.0;

    let digest = TraceDigest {
        top10_true: top_true,
        top10_estimated: top_estimated,
        overlap_count,
        head_contrib_upper_bits: head_tail.head_contrib_upper.to_bits(),
        head_contrib_lower_bits: head_tail.head_contrib_lower.to_bits(),
        tail_contrib_hat_bits: head_tail.tail_contrib_hat.to_bits(),
        zipf_s_hat_bits: zipf.s_hat.to_bits(),
        zipf_window_n: zipf.window_n,
        ledger_rows: ledger
            .heavy_hitters
            .iter()
            .map(|entry| (entry.pgno, entry.count_hat, entry.err))
            .collect(),
    };

    TraceOutcome {
        digest,
        overlap_pct,
        zipf_s_hat: zipf.s_hat,
        estimate,
    }
}

#[test]
fn test_bd_3u2v_unit_compliance_gate() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);
    if !evaluation.missing_unit_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=unit_ids_missing missing={:?}",
            evaluation.missing_unit_ids
        ));
    }
    if !evaluation.missing_e2e_ids.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_ids_missing missing={:?}",
            evaluation.missing_e2e_ids
        ));
    }
    if !evaluation.missing_log_levels.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=logging_levels_missing missing={:?}",
            evaluation.missing_log_levels
        ));
    }
    if evaluation.missing_log_standard_ref {
        return Err(format!(
            "bead_id={BEAD_ID} case=logging_standard_missing expected_ref={LOG_STANDARD_REF}"
        ));
    }
    Ok(())
}

proptest! {
    #[test]
    fn prop_bd_3u2v_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n## E2E Test\n- {}\n- {}\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
            LOG_STANDARD_REF,
        );

        synthetic = synthetic.replacen(REQUIRED_TOKENS[missing_index], "", 1);
        let evaluation = evaluate_description(&synthetic);
        if evaluation.is_compliant() {
            return Err(TestCaseError::fail(format!(
                "bead_id={} case=structure_compliance expected_non_compliant missing_index={} missing_marker={}",
                BEAD_ID,
                missing_index,
                REQUIRED_TOKENS[missing_index]
            )));
        }
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn test_e2e_bd_3u2v_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    let first = run_zipf_trace();
    let second = run_zipf_trace();
    if first.digest != second.digest {
        return Err(format!(
            "bead_id={BEAD_ID} case=deterministic_replay_failure first={:?} second={:?}",
            first.digest, second.digest
        ));
    }

    if first.overlap_pct < 0.80 {
        return Err(format!(
            "bead_id={BEAD_ID} case=top10_overlap_violation overlap_pct={} required=0.80 top10_true={:?} top10_estimated={:?}",
            first.overlap_pct, first.digest.top10_true, first.digest.top10_estimated
        ));
    }

    let head_tail = first
        .estimate
        .head_tail
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=head_tail_missing"))?;
    if head_tail.head_contrib_upper < 0.50 {
        return Err(format!(
            "bead_id={BEAD_ID} case=head_contrib_violation head_contrib_upper={} required=0.50",
            head_tail.head_contrib_upper
        ));
    }

    let zipf_error = (first.zipf_s_hat - 0.99).abs();
    if zipf_error > 0.20 {
        return Err(format!(
            "bead_id={BEAD_ID} case=zipf_accuracy_violation s_hat={} expected=0.99 abs_error={zipf_error}",
            first.zipf_s_hat
        ));
    }

    let ledger = first.estimate.to_evidence_ledger();
    if ledger.txn_count < 200 {
        return Err(format!(
            "bead_id={BEAD_ID} case=window_txn_count_too_small txn_count={}",
            ledger.txn_count
        ));
    }
    if ledger.heavy_hitters.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=ledger_heavy_hitters_missing"
        ));
    }
    if ledger.head_contrib_lower.is_none()
        || ledger.head_contrib_upper.is_none()
        || ledger.tail_contrib_hat.is_none()
    {
        return Err(format!(
            "bead_id={BEAD_ID} case=ledger_head_tail_fields_missing ledger={ledger:?}"
        ));
    }

    for pair in ledger.heavy_hitters.windows(2) {
        let prev = pair[0];
        let next = pair[1];
        if prev.count_hat < next.count_hat
            || (prev.count_hat == next.count_hat && prev.pgno > next.pgno)
        {
            return Err(format!(
                "bead_id={BEAD_ID} case=ledger_sort_order_violation prev={prev:?} next={next:?}"
            ));
        }
    }

    let runtime_dir = unique_runtime_dir("e2e")?;
    let artifact_path = runtime_dir.join("bd_3u2v_artifact.json");
    let artifact = json!({
        "bead_id": BEAD_ID,
        "overlap_pct": first.overlap_pct,
        "zipf_s_hat": first.zipf_s_hat,
        "top10_true": first.digest.top10_true,
        "top10_estimated": first.digest.top10_estimated,
        "head_contrib_lower": head_tail.head_contrib_lower,
        "head_contrib_upper": head_tail.head_contrib_upper,
        "tail_contrib_hat": head_tail.tail_contrib_hat,
        "missing_unit_ids": evaluation.missing_unit_ids,
        "missing_e2e_ids": evaluation.missing_e2e_ids,
        "missing_log_levels": evaluation.missing_log_levels,
        "missing_log_standard_ref": evaluation.missing_log_standard_ref
    });
    let artifact_pretty = serde_json::to_string_pretty(&artifact)
        .map_err(|error| format!("artifact_serialize_failed error={error}"))?;
    fs::write(&artifact_path, artifact_pretty).map_err(|error| {
        format!(
            "artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=artifact_written path={} size_bytes={}",
        artifact_path.display(),
        fs::metadata(&artifact_path)
            .map_err(|error| format!("artifact_metadata_failed error={error}"))?
            .len()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary overlap_pct={} zipf_s_hat={} head_contrib_upper={} tail_contrib_hat={} missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={}",
        first.overlap_pct,
        first.zipf_s_hat,
        head_tail.head_contrib_upper,
        head_tail.tail_contrib_hat,
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref
    );

    for id in &evaluation.missing_unit_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_unit_id id={id}");
    }
    for id in &evaluation.missing_e2e_ids {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_e2e_id id={id}");
    }
    for level in &evaluation.missing_log_levels {
        eprintln!("WARN bead_id={BEAD_ID} case=missing_log_level level={level}");
    }
    if evaluation.missing_log_standard_ref {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=missing_log_standard_ref expected={LOG_STANDARD_REF}"
        );
    }

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }
    Ok(())
}

#[test]
fn test_e2e_bd_3u2v() -> Result<(), String> {
    test_e2e_bd_3u2v_compliance()
}
