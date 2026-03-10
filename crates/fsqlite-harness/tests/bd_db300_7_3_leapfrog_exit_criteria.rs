use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use fsqlite_harness::leapfrog_exit_criteria::{BEAD_ID, LeapfrogExitCriteria};

const CONTRACT_PATH: &str = "leapfrog_exit_criteria.toml";
const REQUIRED_TESTS: [&str; 4] = [
    "test_bd_db300_7_3_contract_schema_and_links",
    "test_bd_db300_7_3_required_campaign_surface_exists",
    "test_bd_db300_7_3_cell_targets_are_monotone",
    "test_bd_db300_7_3_verification_plan_is_actionable",
];
const REQUIRED_LOG_FIELDS: [&str; 8] = [
    "throughput_ratio_vs_sqlite",
    "retry_rate",
    "cpu_utilization_pct",
    "p50_latency_ratio_vs_sqlite",
    "p95_latency_ratio_vs_sqlite",
    "p99_latency_ratio_vs_sqlite",
    "responsiveness_regression_ratio_vs_sqlite",
    "topology_reassignments",
];

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn load_contract() -> Result<LeapfrogExitCriteria, String> {
    let root = workspace_root()?;
    LeapfrogExitCriteria::load_from_workspace_root(&root)
}

#[test]
fn test_bd_db300_7_3_contract_schema_and_links() -> Result<(), String> {
    let root = workspace_root()?;
    let criteria = load_contract()?;
    criteria.validate(&root)?;

    if criteria.meta.bead_id != BEAD_ID {
        return Err(format!(
            "unexpected bead id actual={} expected={BEAD_ID}",
            criteria.meta.bead_id
        ));
    }

    let contract_path = root.join(CONTRACT_PATH);
    if !contract_path.exists() {
        return Err(format!(
            "contract path missing path={}",
            contract_path.display()
        ));
    }

    Ok(())
}

#[test]
fn test_bd_db300_7_3_required_campaign_surface_exists() -> Result<(), String> {
    let criteria = load_contract()?;

    let expected_scenarios = [
        "commutative_inserts_disjoint_keys_c1",
        "commutative_inserts_disjoint_keys_c4",
        "commutative_inserts_disjoint_keys_c8",
        "hot_page_contention_c1",
        "hot_page_contention_c4",
        "hot_page_contention_c8",
        "mixed_read_write_c1",
        "mixed_read_write_c4",
        "mixed_read_write_c8",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<BTreeSet<_>>();

    let actual_scenarios = criteria
        .verification_plan
        .e2e_scenarios
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if actual_scenarios != expected_scenarios {
        return Err(format!(
            "scenario set mismatch actual={actual_scenarios:?} expected={expected_scenarios:?}"
        ));
    }

    let expected_profiles = ["baseline_unpinned", "recommended_pinned", "adversarial_cross_node"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let actual_profiles = criteria
        .campaign
        .required_placement_profiles
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if actual_profiles != expected_profiles {
        return Err(format!(
            "placement profile mismatch actual={actual_profiles:?} expected={expected_profiles:?}"
        ));
    }

    Ok(())
}

#[test]
fn test_bd_db300_7_3_cell_targets_are_monotone() -> Result<(), String> {
    let criteria = load_contract()?;
    let c1 = criteria
        .cell_gate("c1")
        .ok_or_else(|| "missing c1 gate".to_owned())?;
    let c4 = criteria
        .cell_gate("c4")
        .ok_or_else(|| "missing c4 gate".to_owned())?;
    let c8 = criteria
        .cell_gate("c8")
        .ok_or_else(|| "missing c8 gate".to_owned())?;

    if !(c1.recommended_min_throughput_ratio_vs_sqlite
        < c4.recommended_min_throughput_ratio_vs_sqlite
        && c4.recommended_min_throughput_ratio_vs_sqlite
            < c8.recommended_min_throughput_ratio_vs_sqlite)
    {
        return Err("recommended throughput targets must strictly increase c1 < c4 < c8".to_owned());
    }

    if !(c1.baseline_catastrophic_floor_ratio_vs_sqlite
        > c4.baseline_catastrophic_floor_ratio_vs_sqlite
        && c4.baseline_catastrophic_floor_ratio_vs_sqlite
            > c8.baseline_catastrophic_floor_ratio_vs_sqlite)
    {
        return Err("baseline catastrophic floors must get looser as concurrency rises".to_owned());
    }

    if !(c1.adversarial_catastrophic_floor_ratio_vs_sqlite
        > c4.adversarial_catastrophic_floor_ratio_vs_sqlite
        && c4.adversarial_catastrophic_floor_ratio_vs_sqlite
            > c8.adversarial_catastrophic_floor_ratio_vs_sqlite)
    {
        return Err("adversarial catastrophic floors must get looser as concurrency rises".to_owned());
    }

    if !(c1.max_retry_rate < c4.max_retry_rate && c4.max_retry_rate < c8.max_retry_rate) {
        return Err("retry budget must widen monotonically c1 < c4 < c8".to_owned());
    }

    if !(c1.min_cpu_utilization_pct < c4.min_cpu_utilization_pct
        && c4.min_cpu_utilization_pct < c8.min_cpu_utilization_pct)
    {
        return Err("minimum CPU utilization must tighten monotonically".to_owned());
    }

    if !(c1.max_p99_latency_ratio_vs_sqlite < c4.max_p99_latency_ratio_vs_sqlite
        && c4.max_p99_latency_ratio_vs_sqlite < c8.max_p99_latency_ratio_vs_sqlite)
    {
        return Err("p99 latency guard must relax monotonically with concurrency".to_owned());
    }

    Ok(())
}

#[test]
fn test_bd_db300_7_3_verification_plan_is_actionable() -> Result<(), String> {
    let criteria = load_contract()?;

    let actual_tests = criteria
        .verification_plan
        .unit_tests
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected_tests = REQUIRED_TESTS.into_iter().collect::<BTreeSet<_>>();
    if actual_tests != expected_tests {
        return Err(format!(
            "verification unit test set mismatch actual={actual_tests:?} expected={expected_tests:?}"
        ));
    }

    let log_fields = criteria
        .verification_plan
        .required_log_fields
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for required_field in REQUIRED_LOG_FIELDS {
        if !log_fields.contains(required_field) {
            return Err(format!(
                "required log field missing field={required_field}"
            ));
        }
    }

    if !criteria.scorecard.claim_forbidden_when_any_fail {
        return Err("claim gate must be fail-able".to_owned());
    }
    if !criteria
        .scorecard
        .claim_language
        .contains("leapfrogs SQLite")
    {
        return Err("claim language must explicitly name the leapfrog claim".to_owned());
    }
    if !criteria
        .scorecard
        .claim_language
        .contains("recommended_pinned")
    {
        return Err("claim language must mention the recommended placement".to_owned());
    }

    let artifacts = criteria
        .verification_plan
        .logging_artifacts
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for required_artifact in [
        "artifacts/{bead_id}/{run_id}/events.jsonl",
        "artifacts/{bead_id}/{run_id}/manifest.json",
        "artifacts/{bead_id}/{run_id}/summary.md",
        "artifacts/{bead_id}/{run_id}/cell_metrics.jsonl",
        "artifacts/{bead_id}/{run_id}/retry_report.json",
        "artifacts/{bead_id}/{run_id}/topology.json",
    ] {
        if !artifacts.contains(required_artifact) {
            return Err(format!(
                "logging artifact missing artifact={required_artifact}"
            ));
        }
    }

    Ok(())
}
