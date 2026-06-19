//! Certification-facing behavioral quirk suite for bd-2yqp6.5.3.
//!
//! This suite promotes high-risk SQLite quirks into deterministic differential
//! coverage with explicit corpus-scenario and feature-id mappings:
//! - type affinity and coercion,
//! - built-in collation behavior,
//! - NULL semantics,
//! - integer and aggregate overflow behavior.
//!
//! Divergences emit machine-readable artifacts and minimal reproducible SQL
//! scripts under `artifacts/bd-2yqp6.5.3/minimal-repros/`.

#![allow(clippy::too_many_lines)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use fsqlite_harness::differential_v2::{
    CsqliteExecutor, DifferentialResult, ExecutionEnvelope, FsqliteExecutor, MismatchReduction,
    Outcome, minimize_mismatch_workload, run_differential,
};
use fsqlite_harness::oracle::{
    ErrorCategory, FixtureOp, FsqliteMode, QueryExpectation, TestFixture, find_sqlite3_binary,
    load_fixture, run_suite,
};
use fsqlite_harness::parity_taxonomy::build_canonical_universe;
use serde_json::json;
use sha2::{Digest, Sha256};

const BEAD_ID: &str = "bd-2yqp6.5.3";
const BASE_SEED: u64 = 3_530;
const REPLAY_COMMAND: &str = "rch exec -- cargo test -p fsqlite-harness --test bd_2yqp6_5_3_behavioral_quirk_suite -- --nocapture";

#[derive(Debug, Clone)]
struct QuirkScenario {
    id: String,
    corpus_scenario_id: &'static str,
    category: &'static str,
    feature_titles: &'static [&'static str],
    fixture_path: Option<PathBuf>,
    fixture: TestFixture,
}

fn conformance_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root canonicalize")
}

fn feature_id_catalog() -> BTreeMap<String, String> {
    build_canonical_universe()
        .sorted_features()
        .into_iter()
        .map(|feature| (feature.title.clone(), feature.id.0.clone()))
        .collect()
}

fn feature_ids_for_titles(feature_titles: &[&str]) -> Vec<String> {
    let catalog = feature_id_catalog();
    let mut feature_ids = feature_titles
        .iter()
        .map(|title| {
            catalog
                .get(*title)
                .unwrap_or_else(|| panic!("missing feature title mapping for {title}"))
                .clone()
        })
        .collect::<Vec<_>>();
    feature_ids.sort();
    feature_ids.dedup();
    feature_ids
}

fn load_fixture_scenario(
    file_name: &'static str,
    corpus_scenario_id: &'static str,
    category: &'static str,
    feature_titles: &'static [&'static str],
) -> QuirkScenario {
    let path = conformance_dir().join(file_name);
    let fixture = load_fixture(&path)
        .unwrap_or_else(|error| panic!("failed to load fixture {}: {error}", path.display()));
    QuirkScenario {
        id: fixture.id.clone(),
        corpus_scenario_id,
        category,
        feature_titles,
        fixture_path: Some(path),
        fixture,
    }
}

fn inline_sum_overflow_fixture() -> TestFixture {
    TestFixture {
        id: "e3_sum_overflow_error".to_owned(),
        description: "SUM over integer inputs raises overflow instead of silently promoting"
            .to_owned(),
        ops: vec![
            FixtureOp::Open {
                path: ":memory:".to_owned(),
            },
            FixtureOp::Query {
                sql: "WITH nums(x) AS (VALUES(9223372036854775807), (1)) SELECT sum(x) FROM nums"
                    .to_owned(),
                expect: QueryExpectation {
                    expect_error: Some(ErrorCategory::Error),
                    ..QueryExpectation::default()
                },
            },
        ],
        fsqlite_modes: vec![FsqliteMode::Compatibility, FsqliteMode::Native],
        divergence: None,
    }
}

fn inline_empty_string_fixture() -> TestFixture {
    TestFixture {
        id: "e3_empty_string_not_null".to_owned(),
        description: "Empty strings remain distinct from NULL in boolean and length semantics"
            .to_owned(),
        ops: vec![
            FixtureOp::Open {
                path: ":memory:".to_owned(),
            },
            FixtureOp::Query {
                sql: "SELECT '' IS NULL, '' = '', length('')".to_owned(),
                expect: QueryExpectation {
                    rows: vec![vec!["0".to_owned(), "1".to_owned(), "0".to_owned()]],
                    ordered: true,
                    ..QueryExpectation::default()
                },
            },
        ],
        fsqlite_modes: vec![FsqliteMode::Compatibility, FsqliteMode::Native],
        divergence: None,
    }
}

fn inline_rtrim_collation_fixture() -> TestFixture {
    TestFixture {
        id: "e3_rtrim_collation".to_owned(),
        description:
            "RTRIM collation ignores trailing spaces while BINARY still distinguishes them"
                .to_owned(),
        ops: vec![
            FixtureOp::Open {
                path: ":memory:".to_owned(),
            },
            FixtureOp::Query {
                sql: "WITH vals(v) AS (VALUES('abc'), ('abc  '), ('abc\t')) \
                      SELECT COUNT(DISTINCT v COLLATE RTRIM), \
                             COUNT(DISTINCT v COLLATE BINARY), \
                             SUM(v = 'abc' COLLATE RTRIM) \
                      FROM vals"
                    .to_owned(),
                expect: QueryExpectation {
                    rows: vec![vec!["2".to_owned(), "3".to_owned(), "2".to_owned()]],
                    ordered: true,
                    ..QueryExpectation::default()
                },
            },
        ],
        fsqlite_modes: vec![FsqliteMode::Compatibility, FsqliteMode::Native],
        divergence: None,
    }
}

fn inline_composite_unique_null_fixture() -> TestFixture {
    TestFixture {
        id: "e3_composite_unique_nulls".to_owned(),
        description:
            "Composite UNIQUE constraints allow repeated keys whenever any constrained column is NULL"
                .to_owned(),
        ops: vec![
            FixtureOp::Open {
                path: ":memory:".to_owned(),
            },
            FixtureOp::Exec {
                sql: "CREATE TABLE qn(a INTEGER, b INTEGER, note TEXT, UNIQUE(a, b))".to_owned(),
                expect_error: None,
            },
            FixtureOp::Exec {
                sql: "INSERT INTO qn(a, b, note) VALUES(NULL, 1, 'null-a-1')".to_owned(),
                expect_error: None,
            },
            FixtureOp::Exec {
                sql: "INSERT INTO qn(a, b, note) VALUES(NULL, 1, 'null-a-2')".to_owned(),
                expect_error: None,
            },
            FixtureOp::Exec {
                sql: "INSERT INTO qn(a, b, note) VALUES(1, NULL, 'null-b-1')".to_owned(),
                expect_error: None,
            },
            FixtureOp::Exec {
                sql: "INSERT INTO qn(a, b, note) VALUES(1, NULL, 'null-b-2')".to_owned(),
                expect_error: None,
            },
            FixtureOp::Exec {
                sql: "INSERT INTO qn(a, b, note) VALUES(1, 1, 'nonnull')".to_owned(),
                expect_error: None,
            },
            FixtureOp::Query {
                sql: "SELECT COUNT(*), SUM(a IS NULL OR b IS NULL) FROM qn".to_owned(),
                expect: QueryExpectation {
                    rows: vec![vec!["5".to_owned(), "4".to_owned()]],
                    ordered: true,
                    ..QueryExpectation::default()
                },
            },
            FixtureOp::Exec {
                sql: "INSERT INTO qn(a, b, note) VALUES(1, 1, 'dup-nonnull')".to_owned(),
                expect_error: Some(ErrorCategory::Constraint),
            },
            FixtureOp::Query {
                sql: "SELECT COUNT(*), \
                             SUM(CASE WHEN a = 1 AND b = 1 THEN 1 ELSE 0 END) \
                      FROM qn"
                    .to_owned(),
                expect: QueryExpectation {
                    rows: vec![vec!["5".to_owned(), "1".to_owned()]],
                    ordered: true,
                    ..QueryExpectation::default()
                },
            },
        ],
        fsqlite_modes: vec![FsqliteMode::Compatibility, FsqliteMode::Native],
        divergence: None,
    }
}

fn inline_mul_div_overflow_fixture() -> TestFixture {
    TestFixture {
        id: "e3_mul_div_overflow_promotes_real".to_owned(),
        description:
            "Multiply and divide overflow promote integer expressions to REAL instead of wrapping"
                .to_owned(),
        ops: vec![
            FixtureOp::Open {
                path: ":memory:".to_owned(),
            },
            FixtureOp::Query {
                sql: "SELECT typeof(9223372036854775807 * 2), \
                             typeof(-9223372036854775808 / -1)"
                    .to_owned(),
                expect: QueryExpectation {
                    rows: vec![vec!["real".to_owned(), "real".to_owned()]],
                    ordered: true,
                    ..QueryExpectation::default()
                },
            },
        ],
        fsqlite_modes: vec![FsqliteMode::Compatibility, FsqliteMode::Native],
        divergence: None,
    }
}

fn inline_total_no_overflow_fixture() -> TestFixture {
    TestFixture {
        id: "e3_total_no_overflow".to_owned(),
        description:
            "TOTAL continues in REAL space for large integer inputs where SUM would overflow"
                .to_owned(),
        ops: vec![
            FixtureOp::Open {
                path: ":memory:".to_owned(),
            },
            FixtureOp::Query {
                sql: "WITH nums(x) AS (VALUES(9223372036854775807), (1)) \
                      SELECT typeof(total(x)), total(x) > 9e18 FROM nums"
                    .to_owned(),
                expect: QueryExpectation {
                    rows: vec![vec!["real".to_owned(), "1".to_owned()]],
                    ordered: true,
                    ..QueryExpectation::default()
                },
            },
        ],
        fsqlite_modes: vec![FsqliteMode::Compatibility, FsqliteMode::Native],
        divergence: None,
    }
}

fn inline_scalar_vs_aggregate_min_max_null_fixture() -> TestFixture {
    TestFixture {
        id: "e3_scalar_vs_aggregate_min_max_nulls".to_owned(),
        description:
            "Scalar min/max return NULL if any argument is NULL, while aggregate min/max ignore NULL rows"
                .to_owned(),
        ops: vec![
            FixtureOp::Open {
                path: ":memory:".to_owned(),
            },
            FixtureOp::Query {
                sql: "WITH vals(v) AS (VALUES(NULL), (1), (3)) \
                      SELECT max(1, NULL, 3) IS NULL, \
                             min(1, NULL, 3) IS NULL, \
                             max(v), \
                             min(v), \
                             count(v) \
                      FROM vals"
                    .to_owned(),
                expect: QueryExpectation {
                    rows: vec![vec![
                        "1".to_owned(),
                        "1".to_owned(),
                        "3".to_owned(),
                        "1".to_owned(),
                        "2".to_owned(),
                    ]],
                    ordered: true,
                    ..QueryExpectation::default()
                },
            },
        ],
        fsqlite_modes: vec![FsqliteMode::Compatibility, FsqliteMode::Native],
        divergence: None,
    }
}

fn quirk_scenarios() -> Vec<QuirkScenario> {
    vec![
        load_fixture_scenario(
            "004_type_affinity.json",
            "QUIRK-C4-004_type_affinity",
            "affinity",
            &["Type affinity rules"],
        ),
        load_fixture_scenario(
            "017_type_affinity_edge.json",
            "QUIRK-C4-017_type_affinity_edge",
            "affinity",
            &["Type affinity rules", "Type coercion"],
        ),
        load_fixture_scenario(
            "018_collation_ascii_unicode_nocase.json",
            "QUIRK-C4-018_collation_ascii_unicode_nocase",
            "collation",
            &["Collation sequences"],
        ),
        load_fixture_scenario(
            "019_null_unique_edge.json",
            "QUIRK-C4-019_null_unique_edge",
            "null",
            &["NULL semantics"],
        ),
        load_fixture_scenario(
            "020_integer_overflow_semantics.json",
            "QUIRK-C4-020_integer_overflow_semantics",
            "overflow",
            &[
                "Type coercion",
                "Integer storage classes",
                "Real (IEEE 754)",
            ],
        ),
        QuirkScenario {
            id: "e3_sum_overflow_error".to_owned(),
            corpus_scenario_id: "QUIRK-C4-sum_overflow_error",
            category: "overflow",
            feature_titles: &[
                "Type coercion",
                "Integer storage classes",
                "Real (IEEE 754)",
            ],
            fixture_path: None,
            fixture: inline_sum_overflow_fixture(),
        },
        QuirkScenario {
            id: "e3_empty_string_not_null".to_owned(),
            corpus_scenario_id: "QUIRK-C4-empty_string_not_null",
            category: "null",
            feature_titles: &["NULL semantics"],
            fixture_path: None,
            fixture: inline_empty_string_fixture(),
        },
        QuirkScenario {
            id: "e3_rtrim_collation".to_owned(),
            corpus_scenario_id: "QUIRK-C4-rtrim_collation",
            category: "collation",
            feature_titles: &["Collation sequences"],
            fixture_path: None,
            fixture: inline_rtrim_collation_fixture(),
        },
        QuirkScenario {
            id: "e3_composite_unique_nulls".to_owned(),
            corpus_scenario_id: "QUIRK-C4-composite_unique_nulls",
            category: "null",
            feature_titles: &["NULL semantics"],
            fixture_path: None,
            fixture: inline_composite_unique_null_fixture(),
        },
        QuirkScenario {
            id: "e3_mul_div_overflow_promotes_real".to_owned(),
            corpus_scenario_id: "QUIRK-C4-mul_div_overflow_promotes_real",
            category: "overflow",
            feature_titles: &[
                "Type coercion",
                "Integer storage classes",
                "Real (IEEE 754)",
            ],
            fixture_path: None,
            fixture: inline_mul_div_overflow_fixture(),
        },
        QuirkScenario {
            id: "e3_total_no_overflow".to_owned(),
            corpus_scenario_id: "QUIRK-C4-total_no_overflow",
            category: "overflow",
            feature_titles: &["Type coercion", "Real (IEEE 754)"],
            fixture_path: None,
            fixture: inline_total_no_overflow_fixture(),
        },
        QuirkScenario {
            id: "e3_scalar_vs_aggregate_min_max_nulls".to_owned(),
            corpus_scenario_id: "QUIRK-C4-scalar_vs_aggregate_min_max_nulls",
            category: "null",
            feature_titles: &["NULL semantics"],
            fixture_path: None,
            fixture: inline_scalar_vs_aggregate_min_max_null_fixture(),
        },
    ]
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
    BASE_SEED + u64::try_from(index).expect("scenario index should fit into u64")
}

fn envelope_for_scenario(scenario: &QuirkScenario, index: usize) -> ExecutionEnvelope {
    let seed = scenario_seed(index);
    ExecutionEnvelope::builder(seed)
        .run_id(format!("{BEAD_ID}-{}-{seed}", scenario.id))
        .scenario_id(scenario.corpus_scenario_id.to_owned())
        .workload(fixture_sql_ops(&scenario.fixture))
        .build()
}

fn sha256_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    fsqlite_harness::bytes_to_lower_hex(digest)
}

fn scenario_source_bytes(scenario: &QuirkScenario) -> Result<Vec<u8>, String> {
    if let Some(path) = &scenario.fixture_path {
        return fs::read(path)
            .map_err(|error| format!("fixture_read_failed path={} error={error}", path.display()));
    }

    serde_json::to_vec_pretty(&scenario.fixture).map_err(|error| {
        format!(
            "inline_fixture_serialize_failed id={} error={error}",
            scenario.id
        )
    })
}

fn canonical_sql_statement(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.ends_with(';') {
        trimmed.to_owned()
    } else {
        format!("{trimmed};")
    }
}

fn render_repro_sql_script(
    scenario: &QuirkScenario,
    envelope: &ExecutionEnvelope,
    reduction: Option<&MismatchReduction>,
) -> String {
    let feature_ids = feature_ids_for_titles(scenario.feature_titles);
    let workload = reduction
        .map(|value| &value.minimized_envelope.workload)
        .unwrap_or(&envelope.workload);

    let mut content = format!(
        "-- bead_id={BEAD_ID}\n\
         -- fixture_id={fixture_id}\n\
         -- corpus_scenario_id={corpus_scenario_id}\n\
         -- category={category}\n\
         -- feature_ids={feature_ids}\n\
         -- replay_command={REPLAY_COMMAND}\n\n",
        fixture_id = scenario.fixture.id,
        corpus_scenario_id = scenario.corpus_scenario_id,
        category = scenario.category,
        feature_ids = feature_ids.join(","),
    );

    for statement in workload {
        content.push_str(&canonical_sql_statement(statement));
        content.push('\n');
    }

    content
}

fn write_divergence_artifacts(
    scenario: &QuirkScenario,
    envelope: &ExecutionEnvelope,
    result: &DifferentialResult,
    reduction: Option<&MismatchReduction>,
) -> Result<(PathBuf, PathBuf), String> {
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

    let source_bytes = scenario_source_bytes(scenario)?;
    let source_sha256 = sha256_hex(&source_bytes);
    let feature_ids = feature_ids_for_titles(scenario.feature_titles);
    let envelope_hash = &result.artifact_hashes.envelope_id[..16];

    let repro_content = render_repro_sql_script(scenario, envelope, reduction);
    let repro_sha256 = sha256_hex(repro_content.as_bytes());
    let repro_path = artifact_dir.join(format!("{}-{envelope_hash}.sql", scenario.id));
    fs::write(&repro_path, repro_content.as_bytes()).map_err(|error| {
        format!(
            "repro_write_failed path={} error={error}",
            repro_path.display()
        )
    })?;

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

    let source_label = scenario
        .fixture_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| format!("inline://{}", scenario.fixture.id));
    let artifact_path = artifact_dir.join(format!("{}-{envelope_hash}.json", scenario.id));
    let payload = json!({
        "bead_id": BEAD_ID,
        "fixture_id": scenario.fixture.id,
        "category": scenario.category,
        "corpus_scenario_id": scenario.corpus_scenario_id,
        "feature_titles": scenario.feature_titles,
        "feature_ids": feature_ids,
        "fixture_source": {
            "label": source_label,
            "sha256": source_sha256,
        },
        "replay_command": REPLAY_COMMAND,
        "trace_id": result.metadata.trace_id,
        "run_id": result.metadata.run_id,
        "scenario_id": result.metadata.scenario_id,
        "seed": result.metadata.seed,
        "envelope": envelope,
        "differential_result": result,
        "minimal_reduction": reduction_payload,
        "minimal_repro_sql": {
            "path": repro_path.display().to_string(),
            "sha256": repro_sha256,
        },
    });
    let content = serde_json::to_vec_pretty(&payload)
        .map_err(|error| format!("artifact_serialize_failed: {error}"))?;
    fs::write(&artifact_path, content).map_err(|error| {
        format!(
            "artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    Ok((artifact_path, repro_path))
}

#[test]
fn scenario_inventory_maps_to_feature_ids_and_corpus_scenarios() {
    let scenarios = quirk_scenarios();
    let mut ids = BTreeSet::new();
    let mut corpus_ids = BTreeSet::new();
    let mut categories = BTreeSet::new();

    for scenario in &scenarios {
        assert!(
            ids.insert(scenario.id.clone()),
            "duplicate scenario id {}",
            scenario.id
        );
        assert!(
            corpus_ids.insert(scenario.corpus_scenario_id),
            "duplicate corpus scenario id {}",
            scenario.corpus_scenario_id
        );
        assert!(
            !feature_ids_for_titles(scenario.feature_titles).is_empty(),
            "scenario {} must resolve feature ids",
            scenario.id
        );
        assert!(
            !fixture_sql_ops(&scenario.fixture).is_empty(),
            "scenario {} must have replayable SQL",
            scenario.id
        );
        categories.insert(scenario.category);
    }

    assert!(categories.contains("affinity"));
    assert!(categories.contains("collation"));
    assert!(categories.contains("null"));
    assert!(categories.contains("overflow"));
}

#[test]
fn oracle_expectations_hold_for_behavioral_quirk_scenarios() {
    let Ok(sqlite3_path) = find_sqlite3_binary() else {
        eprintln!("bead_id={BEAD_ID} skipping oracle quirk gate: sqlite3 binary not found");
        return;
    };

    let fixtures = quirk_scenarios()
        .into_iter()
        .map(|scenario| scenario.fixture)
        .collect::<Vec<_>>();

    for mode in [FsqliteMode::Compatibility, FsqliteMode::Native] {
        let report = run_suite(&sqlite3_path, &fixtures, mode).unwrap_or_else(|error| {
            panic!("bead_id={BEAD_ID} mode={mode:?} run_suite failed: {error}")
        });
        assert!(
            report.all_passed(),
            "bead_id={BEAD_ID} mode={mode:?} oracle mismatch: failed={} diffs={:?}",
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
fn supported_behavioral_quirks_match_csqlite_differentially() {
    let scenarios = quirk_scenarios();
    let mut pass_count = 0_usize;
    let mut divergences = Vec::new();

    for (index, scenario) in scenarios.iter().enumerate() {
        let envelope = envelope_for_scenario(scenario, index);
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
                "bead_id={BEAD_ID} scenario={} minimization failed: {error}",
                scenario.id
            )
        });
        let (artifact_path, repro_path) =
            write_divergence_artifacts(scenario, &envelope, &result, reduction.as_ref())
                .unwrap_or_else(|error| {
                    panic!(
                        "bead_id={BEAD_ID} scenario={} artifact write failed: {error}",
                        scenario.id
                    )
                });

        divergences.push(format!(
            "scenario={} corpus_scenario_id={} feature_ids={:?} outcome={} artifact={} repro={}",
            scenario.id,
            scenario.corpus_scenario_id,
            feature_ids_for_titles(scenario.feature_titles),
            result.outcome,
            artifact_path.display(),
            repro_path.display()
        ));
    }

    assert_eq!(
        pass_count + divergences.len(),
        scenarios.len(),
        "bead_id={BEAD_ID} missing scenario outcomes"
    );
    assert!(
        divergences.is_empty(),
        "bead_id={BEAD_ID} behavioral quirk mismatches:\n{}",
        divergences.join("\n")
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} case=behavioral_quirk_summary passes={} divergences={} replay='{}'",
        pass_count,
        divergences.len(),
        REPLAY_COMMAND
    );
}

#[test]
fn repro_script_uses_minimized_workload_when_present() {
    let scenario = quirk_scenarios()
        .into_iter()
        .find(|scenario| scenario.id == "019_null_unique_edge")
        .expect("null unique scenario should exist");
    let envelope = envelope_for_scenario(&scenario, 0);
    let minimized_workload = vec![
        "CREATE TABLE q3(a INTEGER UNIQUE, note TEXT)".to_owned(),
        "INSERT INTO q3(a, note) VALUES(NULL, 'first-null')".to_owned(),
        "INSERT INTO q3(a, note) VALUES(NULL, 'second-null')".to_owned(),
        "SELECT COUNT(*) FROM q3".to_owned(),
    ];
    let minimized_envelope = ExecutionEnvelope::builder(9_999)
        .scenario_id("QUIRK-C4-019_null_unique_edge-minimized".to_owned())
        .workload(minimized_workload.clone())
        .build();
    let fsqlite = FsqliteExecutor::open_in_memory().expect("open fsqlite");
    let csqlite = CsqliteExecutor::open_in_memory().expect("open csqlite");
    let minimized_result = run_differential(&minimized_envelope, &fsqlite, &csqlite);
    let reduction = MismatchReduction {
        original_workload_len: envelope.workload.len(),
        minimized_workload_len: minimized_envelope.workload.len(),
        removed_workload_indices: vec![4],
        minimized_envelope,
        minimized_result,
    };

    let script = render_repro_sql_script(&scenario, &envelope, Some(&reduction));
    assert!(script.contains("SELECT COUNT(*) FROM q3;"));
    assert!(!script.contains("INSERT INTO q3(a, note) VALUES(7, 'first-seven');"));
    assert!(script.contains("feature_ids="));
    assert!(script.contains("QUIRK-C4-019_null_unique_edge"));
}
