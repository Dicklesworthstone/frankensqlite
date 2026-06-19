use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_harness::soak_executor::{SoakRunReport, TelemetryBoundary, run_soak};
use fsqlite_harness::soak_profiles::{SoakWorkloadSpec, profile_light};
use serde_json::json;
use sha2::{Digest, Sha256};

const BEAD_ID: &str = "bd-mblr.7.7.1";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const RESOURCE_SEED: u64 = 0x7710_0001;

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("bead_id={BEAD_ID} case=workspace_root_failed error={error}"))
}

fn runtime_dir(label: &str) -> Result<PathBuf, String> {
    let root = workspace_root()?
        .join("target")
        .join("bd_mblr_7_7_1_runtime");
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

#[derive(Debug)]
struct ResourceTelemetrySummary {
    run_id: String,
    scenarios: BTreeSet<String>,
    startup_count: usize,
    steady_state_count: usize,
    teardown_count: usize,
}

fn summarize_resource_telemetry(
    report: &SoakRunReport,
) -> Result<ResourceTelemetrySummary, String> {
    assert!(
        !report.resource_telemetry.is_empty(),
        "bead_id={BEAD_ID} case=resource_telemetry_nonempty"
    );

    let boundaries: BTreeSet<TelemetryBoundary> = report
        .resource_telemetry
        .iter()
        .map(|record| record.boundary)
        .collect();
    assert!(
        boundaries.contains(&TelemetryBoundary::Startup),
        "bead_id={BEAD_ID} case=startup_boundary_present"
    );
    assert!(
        boundaries.contains(&TelemetryBoundary::SteadyState),
        "bead_id={BEAD_ID} case=steady_state_boundary_present"
    );
    assert!(
        boundaries.contains(&TelemetryBoundary::Teardown),
        "bead_id={BEAD_ID} case=teardown_boundary_present"
    );

    let run_ids: BTreeSet<&str> = report
        .resource_telemetry
        .iter()
        .map(|record| record.run_id.as_str())
        .collect();
    assert_eq!(
        run_ids.len(),
        1,
        "bead_id={BEAD_ID} case=single_deterministic_run_id"
    );
    let run_id = run_ids
        .first()
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=missing_run_id"))?
        .to_string();

    let scenarios: BTreeSet<String> = report
        .resource_telemetry
        .iter()
        .map(|record| record.scenario_id.clone())
        .collect();
    assert!(
        scenarios.contains("LEAK-HEAP"),
        "bead_id={BEAD_ID} case=scenario_leak_heap_present"
    );
    assert!(
        scenarios.contains("LEAK-WAL"),
        "bead_id={BEAD_ID} case=scenario_leak_wal_present"
    );

    let mut previous_sequence = None;
    for record in &report.resource_telemetry {
        if let Some(previous_sequence) = previous_sequence {
            assert!(
                record.sequence > previous_sequence,
                "bead_id={BEAD_ID} case=sequence_monotone prev={previous_sequence} cur={}",
                record.sequence
            );
        }
        previous_sequence = Some(record.sequence);
    }

    Ok(ResourceTelemetrySummary {
        run_id,
        scenarios,
        startup_count: report
            .resource_telemetry
            .iter()
            .filter(|record| record.boundary == TelemetryBoundary::Startup)
            .count(),
        steady_state_count: report
            .resource_telemetry
            .iter()
            .filter(|record| record.boundary == TelemetryBoundary::SteadyState)
            .count(),
        teardown_count: report
            .resource_telemetry
            .iter()
            .filter(|record| record.boundary == TelemetryBoundary::Teardown)
            .count(),
    })
}

fn write_resource_telemetry_artifact(
    report: &SoakRunReport,
    summary: &ResourceTelemetrySummary,
) -> Result<(PathBuf, String), String> {
    let artifact = json!({
        "bead_id": BEAD_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "run_id": summary.run_id,
        "seed": RESOURCE_SEED,
        "records_total": report.resource_telemetry.len(),
        "boundary_counts": {
            "startup": summary.startup_count,
            "steady_state": summary.steady_state_count,
            "teardown": summary.teardown_count
        },
        "scenario_ids": summary.scenarios,
        "records": report.resource_telemetry
    });
    let artifact_bytes = serde_json::to_vec_pretty(&artifact).map_err(|error| {
        format!("bead_id={BEAD_ID} case=artifact_serialize_failed error={error}")
    })?;
    let artifact_sha256 = sha256_hex(&artifact_bytes);

    let runtime = runtime_dir("resource_telemetry_soak_lane")?;
    let artifact_path = runtime.join("resource_telemetry_soak_lane.json");
    fs::write(&artifact_path, artifact_bytes).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    Ok((artifact_path, artifact_sha256))
}

#[test]
fn soak_lane_emits_normalized_resource_telemetry() -> Result<(), String> {
    let mut profile = profile_light();
    profile.target_transactions = 240;
    profile.invariant_check_interval = 60;
    profile.scenario_ids = vec!["LEAK-HEAP".to_owned(), "LEAK-WAL".to_owned()];
    let spec = SoakWorkloadSpec::from_profile(profile, RESOURCE_SEED);

    let report = run_soak(spec);
    let summary = summarize_resource_telemetry(&report)?;
    let (artifact_path, artifact_sha256) = write_resource_telemetry_artifact(&report, &summary)?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} phase=resource_telemetry_soak_lane run_id={} seed={RESOURCE_SEED} reference={LOG_STANDARD_REF} artifact_path={} artifact_sha256={artifact_sha256}",
        summary.run_id,
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} phase=resource_telemetry_soak_lane run_id={} records_total={} startup={} steady_state={} teardown={}",
        summary.run_id,
        report.resource_telemetry.len(),
        summary.startup_count,
        summary.steady_state_count,
        summary.teardown_count
    );

    Ok(())
}
