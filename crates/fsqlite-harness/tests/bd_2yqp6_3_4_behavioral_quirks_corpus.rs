//! Adversarial behavioral-quirk corpus gate for bd-2yqp6.3.4.
//!
//! Coverage goals:
//! - type-affinity edge coercions,
//! - collation anomalies (ASCII NOCASE vs Unicode),
//! - NULL/UNIQUE behavior,
//! - integer overflow semantics,
//! - transaction edge behavior.
//!
//! On divergence, this test emits deterministic minimal-repro artifacts under:
//! `artifacts/bd-2yqp6.3.4/minimal-repros/`.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_harness::differential_v2::{
    CsqliteExecutor, DifferentialResult, ExecutionEnvelope, FsqliteExecutor, MismatchReduction,
    Outcome, minimize_mismatch_workload, run_differential,
};
use fsqlite_harness::oracle::{
    FixtureOp, FsqliteMode, TestFixture, find_sqlite3_binary, load_fixture, run_suite,
};
use serde_json::json;
use sha2::{Digest, Sha256};

const BEAD_ID: &str = "bd-2yqp6.3.4";
const BASE_SEED: u64 = 3_520;
const REPLAY_COMMAND: &str = "rch exec -- cargo test -p fsqlite-harness --test bd_2yqp6_3_4_behavioral_quirks_corpus -- --nocapture";
const FIXTURE_FILES: [&str; 5] = [
    "017_type_affinity_edge.json",
    "018_collation_ascii_unicode_nocase.json",
    "019_null_unique_edge.json",
    "020_integer_overflow_semantics.json",
    "021_transaction_edge_cases.json",
];
const EXPECTED_FIXTURE_IDS: [&str; 5] = [
    "017_type_affinity_edge",
    "018_collation_ascii_unicode_nocase",
    "019_null_unique_edge",
    "020_integer_overflow_semantics",
    "021_transaction_edge_cases",
];

fn conformance_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root canonicalize")
}

fn load_target_fixtures() -> Vec<(PathBuf, TestFixture)> {
    let base = conformance_dir();
    FIXTURE_FILES
        .iter()
        .map(|file_name| {
            let path = base.join(file_name);
            let fixture = load_fixture(&path).unwrap_or_else(|error| {
                panic!("failed to load fixture {}: {error}", path.display())
            });
            (path, fixture)
        })
        .collect()
}

fn fixture_sql_ops(fixture: &TestFixture) -> Vec<String> {
    fixture
        .ops
        .iter()
        .filter_map(|op| match op {
            FixtureOp::Open { .. } => None,
            FixtureOp::Exec { sql, .. } | FixtureOp::Query { sql, .. } => Some(sql.clone()),
        })
        .collect()
}

fn scenario_seed(index: usize) -> u64 {
    BASE_SEED + u64::try_from(index).expect("index should fit into u64")
}

fn envelope_for_fixture(fixture: &TestFixture, index: usize) -> ExecutionEnvelope {
    let seed = scenario_seed(index);
    ExecutionEnvelope::builder(seed)
        .run_id(format!("{BEAD_ID}-{}-{seed}", fixture.id))
        .scenario_id(format!("QUIRK-C4-{}", fixture.id))
        .workload(fixture_sql_ops(fixture))
        .build()
}

fn sha256_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    format!("{digest:x}")
}

fn write_minimal_repro_artifact(
    fixture_path: &Path,
    fixture: &TestFixture,
    envelope: &ExecutionEnvelope,
    result: &DifferentialResult,
    reduction: Option<&MismatchReduction>,
) -> Result<PathBuf, String> {
    let artifact_dir = workspace_root()
        .join("artifacts")
        .join(BEAD_ID)
        .join("minimal-repros");
    fs::create_dir_all(&artifact_dir).map_err(|error| {
        format!(
            "create_dir_failed path={} error={error}",
            artifact_dir.display()
        )
    })?;

    let fixture_bytes = fs::read(fixture_path).map_err(|error| {
        format!(
            "fixture_read_failed path={} error={error}",
            fixture_path.display()
        )
    })?;
    let fixture_sha256 = sha256_hex(&fixture_bytes);
    let envelope_hash = &result.artifact_hashes.envelope_id[..16];
    let artifact_path = artifact_dir.join(format!("{}-{envelope_hash}.json", fixture.id));

    let reduction_payload = reduction.map(|minimized| {
        json!({
            "original_workload_len": minimized.original_workload_len,
            "minimized_workload_len": minimized.minimized_workload_len,
            "removed_workload_indices": minimized.removed_workload_indices,
            "reduction_ratio": minimized.reduction_ratio(),
            "minimized_envelope": minimized.minimized_envelope,
            "minimized_result": minimized.minimized_result,
        })
    });

    let payload = json!({
        "bead_id": BEAD_ID,
        "trace_id": result.metadata.trace_id,
        "run_id": result.metadata.run_id,
        "scenario_id": result.metadata.scenario_id,
        "seed": result.metadata.seed,
        "fixture": {
            "path": fixture_path.display().to_string(),
            "id": fixture.id,
            "sha256": fixture_sha256,
        },
        "replay_command": REPLAY_COMMAND,
        "envelope": envelope,
        "differential_result": result,
        "minimal_reduction": reduction_payload,
    });

    let content = serde_json::to_vec_pretty(&payload)
        .map_err(|error| format!("artifact_serialize_failed: {error}"))?;
    fs::write(&artifact_path, content).map_err(|error| {
        format!(
            "artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    Ok(artifact_path)
}

#[test]
fn fixture_inventory_covers_all_required_quirk_categories() {
    let fixtures = load_target_fixtures();
    assert_eq!(
        fixtures.len(),
        FIXTURE_FILES.len(),
        "bead_id={BEAD_ID} fixture inventory mismatch"
    );

    let actual_ids = fixtures
        .iter()
        .map(|(_, fixture)| fixture.id.clone())
        .collect::<BTreeSet<_>>();
    let expected_ids = EXPECTED_FIXTURE_IDS
        .iter()
        .map(|id| (*id).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_ids, expected_ids,
        "bead_id={BEAD_ID} fixture IDs changed unexpectedly"
    );
}

#[test]
fn oracle_expectations_hold_for_behavioral_quirk_fixtures() {
    let Ok(sqlite3_path) = find_sqlite3_binary() else {
        eprintln!("bead_id={BEAD_ID} skipping oracle fixture gate: sqlite3 binary not found");
        return;
    };

    let fixtures = load_target_fixtures()
        .into_iter()
        .map(|(_, fixture)| fixture)
        .collect::<Vec<_>>();

    for mode in [FsqliteMode::Compatibility, FsqliteMode::Native] {
        let report = run_suite(&sqlite3_path, &fixtures, mode).unwrap_or_else(|error| {
            panic!("bead_id={BEAD_ID} mode={mode:?} run_suite failed: {error}")
        });
        assert!(
            report.all_passed(),
            "bead_id={BEAD_ID} mode={mode:?} oracle fixture mismatch: failed={} diffs={:?}",
            report.failed,
            report
                .reports
                .iter()
                .filter(|fixture| !fixture.passed)
                .flat_map(|fixture| fixture.diffs.iter())
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn differential_oracle_runs_and_emits_minimal_repro_artifacts_for_divergences() {
    let fixtures = load_target_fixtures();
    let mut pass_count = 0_usize;
    let mut divergence_summaries = Vec::new();

    for (index, (path, fixture)) in fixtures.iter().enumerate() {
        let envelope = envelope_for_fixture(fixture, index);
        let fsqlite = FsqliteExecutor::open_in_memory()
            .unwrap_or_else(|error| panic!("bead_id={BEAD_ID} failed to open fsqlite: {error}"));
        let csqlite = CsqliteExecutor::open_in_memory()
            .unwrap_or_else(|error| panic!("bead_id={BEAD_ID} failed to open csqlite: {error}"));
        let result = run_differential(&envelope, &fsqlite, &csqlite);

        if matches!(result.outcome, Outcome::Pass) {
            pass_count += 1;
            continue;
        }

        let reduction = minimize_mismatch_workload(
            &envelope,
            FsqliteExecutor::open_in_memory,
            CsqliteExecutor::open_in_memory,
        )
        .unwrap_or_else(|error| {
            panic!(
                "bead_id={BEAD_ID} fixture={} mismatch minimization failed: {error}",
                fixture.id
            )
        });

        let artifact_path =
            write_minimal_repro_artifact(path, fixture, &envelope, &result, reduction.as_ref())
                .unwrap_or_else(|error| {
                    panic!(
                        "bead_id={BEAD_ID} fixture={} repro artifact write failed: {error}",
                        fixture.id
                    )
                });

        assert!(
            artifact_path.is_file(),
            "bead_id={BEAD_ID} expected artifact path to exist: {}",
            artifact_path.display()
        );
        if let Some(minimized) = reduction.as_ref() {
            assert!(
                minimized.minimized_workload_len <= minimized.original_workload_len,
                "bead_id={BEAD_ID} fixture={} minimizer expanded workload",
                fixture.id
            );
        }

        divergence_summaries.push(format!(
            "fixture={} scenario_id={} outcome={} mismatched={} logical_state_matched={} artifact={}",
            fixture.id,
            result.metadata.scenario_id,
            result.outcome,
            result.statements_mismatched,
            result.logical_state_matched,
            artifact_path.display()
        ));
    }

    assert!(
        pass_count + divergence_summaries.len() == fixtures.len(),
        "bead_id={BEAD_ID} missing fixture outcomes: passes={} divergences={} total={}",
        pass_count,
        divergence_summaries.len(),
        fixtures.len()
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} case=differential_quirk_summary passes={} divergences={} replay='{}' divergence_artifacts={:?}",
        pass_count,
        divergence_summaries.len(),
        REPLAY_COMMAND,
        divergence_summaries
    );
}
