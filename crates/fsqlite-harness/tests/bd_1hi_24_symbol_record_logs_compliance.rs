use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_core::symbol_log::{
    SymbolSegmentHeader, append_symbol_record, ensure_symbol_segment, rebuild_object_locator,
    scan_symbol_segment, symbol_segment_path,
};
use fsqlite_types::{ObjectId, Oti, SymbolRecord, SymbolRecordFlags};
use proptest::prelude::proptest;
use proptest::test_runner::TestCaseError;
use serde_json::{Value, json};
use std::io::Write as _;

const BEAD_ID: &str = "bd-1hi.24";
const ISSUES_JSONL: &str = ".beads/issues.jsonl";
const UNIT_TEST_IDS: [&str; 2] = [
    "test_bd_1hi_24_unit_compliance_gate",
    "prop_bd_1hi_24_structure_compliance",
];
const E2E_TEST_IDS: [&str; 3] = [
    "test_e2e_bd_1hi_24_compliance",
    "test_e2e_symbol_log_lifecycle",
    "test_e2e_crash_recovery_symbol_logs",
];
const LOG_LEVEL_MARKERS: [&str; 4] = ["DEBUG", "INFO", "WARN", "ERROR"];
const LOG_STANDARD_REF: &str = "bd-1fpm";
const REQUIRED_TOKENS: [&str; 10] = [
    "test_bd_1hi_24_unit_compliance_gate",
    "prop_bd_1hi_24_structure_compliance",
    "test_e2e_bd_1hi_24_compliance",
    "test_e2e_symbol_log_lifecycle",
    "test_e2e_crash_recovery_symbol_logs",
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

fn evaluate_description(description: &str) -> ComplianceEvaluation {
    let missing_unit_ids = UNIT_TEST_IDS
        .into_iter()
        .filter(|id| !description.contains(id))
        .collect::<Vec<_>>();

    let missing_e2e_ids = E2E_TEST_IDS
        .into_iter()
        .filter(|id| !description.contains(id))
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
    let root = workspace_root()?.join("target").join("bd_1hi_24_runtime");
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "runtime_dir_create_failed path={} error={error}",
            root.as_path().display()
        )
    })?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let path = root.join(format!("{label}_{}_{}", std::process::id(), stamp));
    fs::create_dir_all(&path).map_err(|error| {
        format!(
            "runtime_subdir_create_failed path={} error={error}",
            path.as_path().display()
        )
    })?;
    Ok(path)
}

fn make_record(object_seed: u8, esi: u32, symbol_size: usize) -> Result<SymbolRecord, String> {
    let mut object_id_bytes = [0_u8; 16];
    object_id_bytes.fill(object_seed);
    let object_id = ObjectId::from_bytes(object_id_bytes);

    let symbol_size_u32 = u32::try_from(symbol_size)
        .map_err(|error| format!("symbol_size_convert_failed value={symbol_size} error={error}"))?;
    let oti = Oti {
        f: u64::from(symbol_size_u32),
        al: 4,
        t: symbol_size_u32,
        z: 1,
        n: 1,
    };

    let mut data = vec![0_u8; symbol_size];
    let esi_low = u8::try_from(esi & 0xFF).expect("masked esi fits u8");
    for (idx, byte) in data.iter_mut().enumerate() {
        let idx_low = u8::try_from(idx & 0xFF).expect("masked idx fits u8");
        *byte = object_seed ^ esi_low ^ idx_low;
    }
    let flags = if esi == 0 {
        SymbolRecordFlags::SYSTEMATIC_RUN_START
    } else {
        SymbolRecordFlags::empty()
    };

    Ok(SymbolRecord::new(object_id, oti, esi, data, flags))
}

#[test]
fn test_bd_1hi_24_unit_compliance_gate() -> Result<(), String> {
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
    fn prop_bd_1hi_24_structure_compliance(missing_index in 0usize..REQUIRED_TOKENS.len()) {
        let mut synthetic = format!(
            "## Unit Test Requirements\n- {}\n- {}\n\n## E2E Test\n- {}\n- {}\n- {}\n\n## Logging Requirements\n- DEBUG: stage progress\n- INFO: summary\n- WARN: degraded mode\n- ERROR: terminal failure\n- Reference: {}\n",
            UNIT_TEST_IDS[0],
            UNIT_TEST_IDS[1],
            E2E_TEST_IDS[0],
            E2E_TEST_IDS[1],
            E2E_TEST_IDS[2],
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
fn test_e2e_bd_1hi_24_compliance() -> Result<(), String> {
    let description = load_issue_description(BEAD_ID)?;
    let evaluation = evaluate_description(&description);

    let runtime_dir = unique_runtime_dir("e2e")?;
    let symbols_dir = runtime_dir.join("ecs").join("symbols");
    fs::create_dir_all(&symbols_dir).map_err(|error| {
        format!(
            "symbols_dir_create_failed path={} error={error}",
            symbols_dir.as_path().display()
        )
    })?;
    let header = SymbolSegmentHeader::new(1, 42, 1_700_000_000);
    let segment_path = symbol_segment_path(&symbols_dir, header.segment_id);
    ensure_symbol_segment(&segment_path, header)
        .map_err(|error| format!("segment_create_failed error={error}"))?;

    let record_a = make_record(1, 0, 128)?;
    let record_b = make_record(2, 0, 96)?;
    append_symbol_record(&symbols_dir, header, &record_a)
        .map_err(|error| format!("append_record_a_failed error={error}"))?;
    append_symbol_record(&symbols_dir, header, &record_b)
        .map_err(|error| format!("append_record_b_failed error={error}"))?;

    let crash_record = make_record(3, 0, 256)?.to_bytes();
    let cut = crash_record.len() / 3;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&segment_path)
        .map_err(|error| format!("segment_open_append_failed error={error}"))?;
    file.write_all(&crash_record[..cut])
        .map_err(|error| format!("partial_append_failed error={error}"))?;
    file.sync_data()
        .map_err(|error| format!("partial_append_sync_failed error={error}"))?;

    let scanned = scan_symbol_segment(&segment_path)
        .map_err(|error| format!("scan_symbol_segment_failed error={error}"))?;
    let locator = rebuild_object_locator(&symbols_dir)
        .map_err(|error| format!("rebuild_object_locator_failed error={error}"))?;

    let artifact_path = runtime_dir.join("bd_1hi_24_artifact.json");
    let artifact = json!({
        "bead_id": BEAD_ID,
        "compliant": evaluation.is_compliant(),
        "record_count": scanned.records.len(),
        "torn_tail": scanned.torn_tail,
        "locator_object_count": locator.len(),
    });
    fs::write(
        &artifact_path,
        serde_json::to_string_pretty(&artifact)
            .map_err(|error| format!("artifact_serialize_failed error={error}"))?,
    )
    .map_err(|error| {
        format!(
            "artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} case=artifact_written path={} record_count={} torn_tail={}",
        artifact_path.display(),
        scanned.records.len(),
        scanned.torn_tail
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} case=e2e_summary missing_unit_ids={} missing_e2e_ids={} missing_log_levels={} missing_log_standard_ref={} locator_object_count={}",
        evaluation.missing_unit_ids.len(),
        evaluation.missing_e2e_ids.len(),
        evaluation.missing_log_levels.len(),
        evaluation.missing_log_standard_ref,
        locator.len()
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
    if scanned.records.len() != 2 || !scanned.torn_tail {
        eprintln!(
            "ERROR bead_id={BEAD_ID} case=runtime_symbol_log_check_failed records={} torn_tail={}",
            scanned.records.len(),
            scanned.torn_tail
        );
    }

    if !evaluation.is_compliant() {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_compliance_failure evaluation={evaluation:?}"
        ));
    }
    if scanned.records.len() != 2 {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_record_count_mismatch actual={}",
            scanned.records.len()
        ));
    }
    if !scanned.torn_tail {
        return Err(format!(
            "bead_id={BEAD_ID} case=e2e_expected_torn_tail_missing locator={locator:?}"
        ));
    }

    Ok(())
}
