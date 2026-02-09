use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_mvcc::{AmsWindowCollector, AmsWindowCollectorConfig, mix64};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::{Value, json};

const BEAD_ID: &str = "bd-26be";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_26be_unit_compliance_gate",
    "prop_bd_26be_structure_compliance",
];
const E2E_TEST_IDS: [&str; 2] = ["test_e2e_bd_26be_compliance", "test_e2e_bd_26be"];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 9] = [
    "test_bd_26be_unit_compliance_gate",
    "prop_bd_26be_structure_compliance",
    "test_e2e_bd_26be_compliance",
    "test_e2e_bd_26be",
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
struct WindowDigest {
    window_id: u64,
    txn_count: u64,
    f2_hat: u128,
    m2_hat_bits: Option<u64>,
    exact_f2: Option<u128>,
    exact_m2_bits: Option<u64>,
    p_eff_hat_bits: u64,
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
    let root = workspace_root()?.join("target").join("bd_26be_runtime");
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
    debug_assert!(s > 0.0 && s < 1.0);
    let u = (mix64(seed) as f64 + 1.0) / (u64::MAX as f64 + 1.0);
    let one_minus_s = 1.0 - s;
    let cdf_scale = (page_count as f64).powf(one_minus_s) - 1.0;
    let rank = (u * cdf_scale + 1.0).powf(1.0 / one_minus_s).floor() as u64;
    rank.clamp(1, page_count) - 1
}

fn run_zipf_trace() -> Vec<WindowDigest> {
    let config = AmsWindowCollectorConfig {
        r: 32,
        db_epoch: 1,
        regime_id: 7,
        window_width_ticks: 40,
        track_exact_m2: true,
        track_heavy_hitters: false,
        heavy_hitter_k: 64,
        estimate_zipf: false,
    };
    let mut collector = AmsWindowCollector::new(config, 0);
    let mut closed = Vec::new();

    // 4 writers x 200 ticks over a 10k-page domain.
    let page_count = 10_000u64;
    let zipf_s = 0.99;
    for tick in 0u64..200 {
        for writer in 0u64..4 {
            let mut write_set = Vec::with_capacity(8);
            for idx in 0u64..8 {
                let seed = tick.wrapping_mul(10_000) ^ writer.wrapping_mul(97) ^ idx;
                write_set.push(zipf_page(seed, page_count, zipf_s));
            }
            closed.extend(collector.observe_commit_attempt(tick, &write_set));
        }
    }
    closed.push(collector.force_flush(200));

    closed
        .into_iter()
        .map(|snapshot| WindowDigest {
            window_id: snapshot.window_id,
            txn_count: snapshot.txn_count,
            f2_hat: snapshot.f2_hat,
            m2_hat_bits: snapshot.m2_hat.map(f64::to_bits),
            exact_f2: snapshot.exact_f2,
            exact_m2_bits: snapshot.exact_m2.map(f64::to_bits),
            p_eff_hat_bits: snapshot.p_eff_hat.to_bits(),
        })
        .collect()
}

#[test]
fn test_bd_26be_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_26be_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
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
fn test_e2e_bd_26be_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    let first = run_zipf_trace();
    let second = run_zipf_trace();
    if first != second {
        return Err(format!(
            "bead_id={BEAD_ID} case=deterministic_replay_failure first_len={} second_len={}",
            first.len(),
            second.len()
        ));
    }

    let mut f2_error_max = 0.0_f64;
    let mut m2_error_max = 0.0_f64;
    for (index, digest) in first.iter().enumerate() {
        if index < 2 {
            continue;
        }
        let Some(exact_f2) = digest.exact_f2 else {
            continue;
        };
        if exact_f2 == 0 {
            continue;
        }
        let f2_error = ((digest.f2_hat as f64 - exact_f2 as f64) / exact_f2 as f64).abs();
        f2_error_max = f2_error_max.max(f2_error);
        if f2_error > 0.50 {
            return Err(format!(
                "bead_id={BEAD_ID} case=f2_tolerance_violation window_id={} f2_hat={} exact_f2={} rel_error={f2_error}",
                digest.window_id, digest.f2_hat, exact_f2
            ));
        }

        let (Some(m2_bits), Some(exact_m2_bits)) = (digest.m2_hat_bits, digest.exact_m2_bits)
        else {
            continue;
        };
        let m2 = f64::from_bits(m2_bits);
        let exact_m2_value = f64::from_bits(exact_m2_bits);
        if exact_m2_value != 0.0 {
            let m2_error = ((m2 - exact_m2_value) / exact_m2_value).abs();
            m2_error_max = m2_error_max.max(m2_error);
            if m2_error > 0.50 {
                return Err(format!(
                    "bead_id={BEAD_ID} case=m2_tolerance_violation window_id={} m2_hat={m2} exact_m2={exact_m2_value} rel_error={m2_error}",
                    digest.window_id
                ));
            }
        }
    }

    let runtime_dir = unique_runtime_dir("e2e")?;
    let artifact_path = runtime_dir.join("bd_26be_artifact.json");
    let artifact = json!({
        "bead_id": BEAD_ID,
        "windows": first.len(),
        "max_f2_relative_error": f2_error_max,
        "max_m2_relative_error": m2_error_max,
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
        "INFO bead_id={BEAD_ID} case=e2e_summary windows={} max_f2_relative_error={} max_m2_relative_error={} missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={}",
        first.len(),
        f2_error_max,
        m2_error_max,
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
fn test_e2e_bd_26be() -> Result<(), String> {
    test_e2e_bd_26be_compliance()
}
