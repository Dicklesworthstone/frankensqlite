use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_harness::soak_executor::{
    LeakBudgetPolicy, LeakDetectorFinding, LeakSeverity, ResourceTelemetryRecord, SoakPhase,
    TelemetryBoundary, TrackedResource, detect_leak_budget_violations,
};
use serde_json::json;
use sha2::{Digest, Sha256};

const BEAD_ID: &str = "bd-mblr.7.7.3";
const GATE_ID: &str = "phase7.leak_budget_ci";
const LOG_STANDARD_REF: &str = "bd-1fpm";

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("bead_id={BEAD_ID} case=workspace_root_failed error={error}"))
}

fn runtime_dir(label: &str) -> Result<PathBuf, String> {
    let root = workspace_root()?
        .join("target")
        .join("bd_mblr_7_7_3_runtime");
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=runtime_root_create_failed path={} error={error}",
            root.display()
        )
    })?;

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let path = root.join(format!("{label}_{}_{}", std::process::id(), nanos));
    fs::create_dir_all(&path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=runtime_subdir_create_failed path={} error={error}",
            path.display()
        )
    })?;
    Ok(path)
}

fn make_record(
    sequence: u64,
    scenario_id: &str,
    boundary: TelemetryBoundary,
    heap_bytes: u64,
    wal_pages: u64,
) -> ResourceTelemetryRecord {
    ResourceTelemetryRecord {
        run_id: "soak-ci-leak-gate".to_owned(),
        scenario_id: scenario_id.to_owned(),
        profile_name: "ci-gate".to_owned(),
        run_seed: 0x7773_0001,
        sequence,
        boundary,
        phase: match boundary {
            TelemetryBoundary::Startup => SoakPhase::Warmup,
            TelemetryBoundary::SteadyState => SoakPhase::MainLoop,
            TelemetryBoundary::Teardown => SoakPhase::Complete,
        },
        transaction_count: sequence,
        elapsed_secs: sequence as f64 * 0.05,
        wal_pages,
        heap_bytes,
        active_transactions: 2,
        lock_table_size: 4,
        max_version_chain_len: 3,
        p99_latency_us: 500,
        ssi_aborts_since_last: 0,
        commits_since_last: sequence,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

fn actionable_diagnostic(finding: &LeakDetectorFinding) -> String {
    let severity = match finding.severity {
        LeakSeverity::Notice => "notice",
        LeakSeverity::Warning => "warning",
        LeakSeverity::Critical => "critical",
    };
    format!(
        "severity={severity} scenario={} resource={} action=inspect_resource_growth_and_fix_root_cause baseline={:.3} final={:.3} reason={}",
        finding.scenario_id,
        finding.resource.as_str(),
        finding.baseline_mean,
        finding.final_value,
        finding.reason
    )
}

fn gate_fail_decision(warning_count: usize, critical_count: usize) -> bool {
    warning_count > 0 || critical_count > 0
}

#[test]
fn ci_leak_gate_enforces_budget_and_emits_actionable_diagnostics() -> Result<(), String> {
    let records = vec![
        make_record(0, "HEALTHY", TelemetryBoundary::Startup, 1_000_000, 10),
        make_record(1, "HEALTHY", TelemetryBoundary::SteadyState, 1_010_000, 11),
        make_record(2, "HEALTHY", TelemetryBoundary::SteadyState, 1_008_000, 11),
        make_record(3, "HEALTHY", TelemetryBoundary::SteadyState, 1_009_000, 11),
        make_record(4, "HEALTHY", TelemetryBoundary::Teardown, 1_007_000, 11),
        make_record(5, "REAL-LEAK", TelemetryBoundary::Startup, 1_000_000, 10),
        make_record(
            6,
            "REAL-LEAK",
            TelemetryBoundary::SteadyState,
            1_050_000,
            15,
        ),
        make_record(
            7,
            "REAL-LEAK",
            TelemetryBoundary::SteadyState,
            1_120_000,
            21,
        ),
        make_record(
            8,
            "REAL-LEAK",
            TelemetryBoundary::SteadyState,
            1_220_000,
            28,
        ),
        make_record(
            9,
            "REAL-LEAK",
            TelemetryBoundary::SteadyState,
            1_360_000,
            36,
        ),
        make_record(10, "REAL-LEAK", TelemetryBoundary::Teardown, 1_520_000, 45),
    ];

    let policy = LeakBudgetPolicy::default();
    let report = detect_leak_budget_violations(&records, &policy);

    let warning_count = report.warning_count();
    let critical_count = report.critical_count();
    let action_items: Vec<String> = report
        .findings
        .iter()
        .filter(|finding| finding.severity >= LeakSeverity::Warning)
        .map(actionable_diagnostic)
        .collect();

    assert!(
        !action_items.is_empty(),
        "bead_id={BEAD_ID} gate_id={GATE_ID} case=action_items_present"
    );

    let should_fail = gate_fail_decision(warning_count, critical_count);
    assert!(
        should_fail,
        "bead_id={BEAD_ID} gate_id={GATE_ID} case=leak_failure_detected warnings={warning_count} critical={critical_count}"
    );

    let runtime = runtime_dir("ci_leak_gate")?;
    let artifact_path = runtime.join("ci_leak_gate_diagnostics.json");
    let artifact_payload = json!({
        "bead_id": BEAD_ID,
        "gate_id": GATE_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "decision": "fail",
        "records_analyzed": report.records_analyzed,
        "warning_count": warning_count,
        "critical_count": critical_count,
        "actionable_diagnostics": action_items,
        "findings": report.findings,
        "artifact_links": [artifact_path.display().to_string()],
    });
    let bytes = serde_json::to_vec_pretty(&artifact_payload).map_err(|error| {
        format!("bead_id={BEAD_ID} case=diagnostics_serialize_failed error={error}")
    })?;
    let artifact_sha256 = sha256_hex(&bytes);
    fs::write(&artifact_path, bytes).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=diagnostics_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    let raw = fs::read_to_string(&artifact_path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=diagnostics_read_failed path={} error={error}",
            artifact_path.display()
        )
    })?;
    let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(|error| {
        format!("bead_id={BEAD_ID} case=diagnostics_parse_failed error={error}")
    })?;
    assert_eq!(
        parsed["decision"],
        serde_json::Value::String("fail".to_owned()),
        "bead_id={BEAD_ID} case=decision_fail"
    );
    assert!(
        parsed["actionable_diagnostics"]
            .as_array()
            .is_some_and(|items| !items.is_empty()),
        "bead_id={BEAD_ID} case=actionable_diagnostics_non_empty"
    );
    assert!(
        parsed["actionable_diagnostics"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item
                .as_str()
                .is_some_and(|entry| entry.contains("scenario=REAL-LEAK")))),
        "bead_id={BEAD_ID} case=actionable_diagnostics_real_leak"
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} gate_id={GATE_ID} decision=fail warnings={warning_count} critical={critical_count} diagnostics_path={} diagnostics_sha256={artifact_sha256}",
        artifact_path.display()
    );

    Ok(())
}

#[test]
fn ci_leak_gate_allows_notice_only_patterns() {
    let records = vec![
        make_record(0, "WARMUP-ONLY", TelemetryBoundary::Startup, 1_000_000, 10),
        make_record(
            1,
            "WARMUP-ONLY",
            TelemetryBoundary::SteadyState,
            1_480_000,
            11,
        ),
        make_record(
            2,
            "WARMUP-ONLY",
            TelemetryBoundary::SteadyState,
            1_470_000,
            11,
        ),
        make_record(
            3,
            "WARMUP-ONLY",
            TelemetryBoundary::SteadyState,
            1_466_000,
            11,
        ),
        make_record(
            4,
            "WARMUP-ONLY",
            TelemetryBoundary::SteadyState,
            1_463_000,
            11,
        ),
        make_record(5, "WARMUP-ONLY", TelemetryBoundary::Teardown, 1_461_000, 11),
    ];

    let policy = LeakBudgetPolicy::default();
    let report = detect_leak_budget_violations(&records, &policy);
    let warning_count = report.warning_count();
    let critical_count = report.critical_count();

    assert!(
        warning_count == 0 && critical_count == 0,
        "bead_id={BEAD_ID} gate_id={GATE_ID} case=notice_only warnings={warning_count} critical={critical_count}"
    );
    assert!(
        !gate_fail_decision(warning_count, critical_count),
        "bead_id={BEAD_ID} gate_id={GATE_ID} case=decision_pass_for_notice_only"
    );
    assert!(
        report
            .findings
            .iter()
            .all(|finding| finding.severity == LeakSeverity::Notice),
        "bead_id={BEAD_ID} gate_id={GATE_ID} case=notice_only_findings"
    );
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.resource == TrackedResource::HeapBytes),
        "bead_id={BEAD_ID} gate_id={GATE_ID} case=heap_notice_present"
    );
}
