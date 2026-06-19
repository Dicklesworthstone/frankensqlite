use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_harness::soak_executor::{
    LeakBudgetPolicy, LeakSeverity, ResourceTelemetryRecord, SoakPhase, TelemetryBoundary,
    TrackedResource, detect_leak_budget_violations,
};
use serde_json::json;
use sha2::{Digest, Sha256};

const BEAD_ID: &str = "bd-mblr.7.7.2";
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
        .join("bd_mblr_7_7_2_runtime");
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

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    fsqlite_harness::bytes_to_lower_hex(digest)
}

fn make_record(
    sequence: u64,
    scenario_id: &str,
    boundary: TelemetryBoundary,
    heap_bytes: u64,
    wal_pages: u64,
) -> ResourceTelemetryRecord {
    ResourceTelemetryRecord {
        run_id: "soak-detector-integration".to_owned(),
        scenario_id: scenario_id.to_owned(),
        profile_name: "integration".to_owned(),
        run_seed: 0x7772_0001,
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

#[test]
#[allow(clippy::too_many_lines)]
fn leak_detector_classifies_synthetic_signatures_and_writes_report_artifact() -> Result<(), String>
{
    let records = vec![
        make_record(0, "WARMUP-ONLY", TelemetryBoundary::Startup, 1_000_000, 10),
        make_record(
            1,
            "WARMUP-ONLY",
            TelemetryBoundary::SteadyState,
            1_500_000,
            12,
        ),
        make_record(
            2,
            "WARMUP-ONLY",
            TelemetryBoundary::SteadyState,
            1_490_000,
            12,
        ),
        make_record(
            3,
            "WARMUP-ONLY",
            TelemetryBoundary::SteadyState,
            1_486_000,
            12,
        ),
        make_record(
            4,
            "WARMUP-ONLY",
            TelemetryBoundary::SteadyState,
            1_484_000,
            12,
        ),
        make_record(5, "WARMUP-ONLY", TelemetryBoundary::Teardown, 1_483_000, 12),
        make_record(6, "REAL-LEAK", TelemetryBoundary::Startup, 1_000_000, 10),
        make_record(
            7,
            "REAL-LEAK",
            TelemetryBoundary::SteadyState,
            1_040_000,
            14,
        ),
        make_record(
            8,
            "REAL-LEAK",
            TelemetryBoundary::SteadyState,
            1_100_000,
            19,
        ),
        make_record(
            9,
            "REAL-LEAK",
            TelemetryBoundary::SteadyState,
            1_190_000,
            25,
        ),
        make_record(
            10,
            "REAL-LEAK",
            TelemetryBoundary::SteadyState,
            1_310_000,
            32,
        ),
        make_record(11, "REAL-LEAK", TelemetryBoundary::Teardown, 1_470_000, 40),
    ];

    let policy = LeakBudgetPolicy::default();
    let report = detect_leak_budget_violations(&records, &policy);

    assert!(
        report.findings.iter().any(|finding| {
            finding.scenario_id == "WARMUP-ONLY"
                && finding.resource == TrackedResource::HeapBytes
                && finding.severity == LeakSeverity::Notice
                && finding.warmup_exempted
        }),
        "bead_id={BEAD_ID} case=warmup_notice_present"
    );
    assert!(
        report.findings.iter().any(|finding| {
            finding.scenario_id == "REAL-LEAK"
                && finding.resource == TrackedResource::HeapBytes
                && finding.severity >= LeakSeverity::Warning
        }),
        "bead_id={BEAD_ID} case=real_leak_warning_or_critical_present"
    );

    let scenario_set: BTreeSet<&str> = report
        .findings
        .iter()
        .map(|finding| finding.scenario_id.as_str())
        .collect();
    assert!(
        scenario_set.contains("WARMUP-ONLY") && scenario_set.contains("REAL-LEAK"),
        "bead_id={BEAD_ID} case=both_scenarios_present"
    );

    let triage_lines: Vec<String> = report
        .findings
        .iter()
        .map(fsqlite_harness::soak_executor::LeakDetectorFinding::triage_line)
        .collect();
    assert!(
        triage_lines
            .iter()
            .any(|line| line.contains("resource=heap_bytes")),
        "bead_id={BEAD_ID} case=triage_line_contains_resource"
    );

    let artifact = json!({
        "bead_id": BEAD_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "records_analyzed": report.records_analyzed,
        "critical_count": report.critical_count(),
        "warning_count": report.warning_count(),
        "findings": report.findings,
        "triage_lines": triage_lines
    });
    let artifact_bytes = serde_json::to_vec_pretty(&artifact).map_err(|error| {
        format!("bead_id={BEAD_ID} case=artifact_serialize_failed error={error}")
    })?;
    let artifact_sha256 = sha256_hex(&artifact_bytes);

    let runtime = runtime_dir("leak_policy_detection")?;
    let artifact_path = runtime.join("leak_policy_detection.json");
    fs::write(&artifact_path, artifact_bytes).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} phase=leak_policy_detection run_id=soak-detector-integration reference={LOG_STANDARD_REF} artifact_path={} artifact_sha256={artifact_sha256}",
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} phase=leak_policy_detection critical_count={} warning_count={} findings_total={}",
        report.critical_count(),
        report.warning_count(),
        report.findings.len()
    );

    Ok(())
}
