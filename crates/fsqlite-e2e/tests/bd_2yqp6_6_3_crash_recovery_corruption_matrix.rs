//! Deterministic crash-recovery and corruption differential matrix.
//!
//! Bead: bd-2yqp6.6.3
//!
//! This suite executes a fixed corruption matrix across both:
//! - stock C SQLite (`rusqlite`) baseline behavior,
//! - FrankenSQLite recovery baseline behavior.
//!
//! It validates:
//! - deterministic replayability for each strategy,
//! - before/after corruption hash evidence presence,
//! - explicit failure-class assignment (`data_loss` vs `recoverable_divergence`).

#![allow(clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use fsqlite_e2e::FRANKEN_SEED;
use fsqlite_e2e::corruption::{
    CatalogEntry, CorruptionCategory, CorruptionReport, CorruptionSeverity,
    corruption_strategy_catalog,
};
use fsqlite_e2e::fsqlite_baseline::{
    FsqliteBaselineResult, FsqliteRecoveryTier, measure_fsqlite_baseline,
};
use fsqlite_e2e::sqlite3_baseline::{
    SqliteBaselineResult, SqliteOutcomeTier, measure_sqlite3_baseline,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

const BEAD_ID: &str = "bd-2yqp6.6.3";
const LOG_STANDARD_REF: &str = "AGENTS.md#cross-cutting-quality-contract";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_2yqp6_6_3_crash_recovery_corruption_matrix -- --nocapture --test-threads=1";
const STRATEGY_ENV_FILTER: &str = "FSQLITE_CRASH_MATRIX_ONLY";
const ARTIFACT_ENV_PATH: &str = "FSQLITE_CRASH_MATRIX_ARTIFACT";

const DEFAULT_STRATEGY_IDS: [&str; 6] = [
    "bitflip_db_single",
    "truncate_db_half",
    "wal_truncate_0",
    "wal_bitflip_frame0",
    "wal_torn_write_frame1",
    "header_zero",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureClass {
    None,
    RecoverableDivergence,
    DataLoss,
}

impl FailureClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::RecoverableDivergence => "recoverable_divergence",
            Self::DataLoss => "data_loss",
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::None => 0,
            Self::RecoverableDivergence => 1,
            Self::DataLoss => 2,
        }
    }
}

#[test]
fn bead_metadata_constants_are_stable_for_replay() {
    assert_eq!(BEAD_ID, "bd-2yqp6.6.3");
    assert_eq!(LOG_STANDARD_REF, "AGENTS.md#cross-cutting-quality-contract");
    assert_eq!(
        REPLAY_COMMAND,
        "cargo test -p fsqlite-e2e --test bd_2yqp6_6_3_crash_recovery_corruption_matrix -- --nocapture --test-threads=1"
    );
}

#[test]
fn bd_2yqp6_6_3_crash_recovery_corruption_differential_matrix() {
    let selected_ids = selected_strategy_ids();
    assert!(
        !selected_ids.is_empty(),
        "at least one strategy must be selected"
    );

    let catalog = corruption_strategy_catalog();
    let by_id: BTreeMap<String, CatalogEntry> = catalog
        .into_iter()
        .map(|entry| (entry.strategy_id.clone(), entry))
        .collect();

    let mut records = Vec::new();
    let mut first_failure: Option<String> = None;
    let mut saw_db_strategy = false;
    let mut saw_wal_strategy = false;

    for strategy_id in &selected_ids {
        let entry = by_id
            .get(strategy_id)
            .unwrap_or_else(|| panic!("missing strategy id in catalog: {strategy_id}"));

        saw_db_strategy |= entry.category == CorruptionCategory::DatabaseFile;
        saw_wal_strategy |= entry.category == CorruptionCategory::Wal;

        let seed = normalized_seed(entry.default_seed);
        let run_id = format!("{BEAD_ID}-{}-{seed:016x}", entry.strategy_id);
        let trace_id = format!("trace-{run_id}");

        let (sqlite_a, sqlite_ms, fsqlite_a, fsqlite_ms) = measure_pair(entry);
        let (sqlite_b, _, fsqlite_b, _) = measure_pair(entry);

        let determinism_errors = determinism_errors(&sqlite_a, &sqlite_b, &fsqlite_a, &fsqlite_b);
        let deterministic_replay = determinism_errors.is_empty();

        let sqlite_failure = classify_sqlite_failure(&sqlite_a);
        let fsqlite_failure = classify_fsqlite_failure(&fsqlite_a);
        let combined_failure = combine_failure_class(sqlite_failure, fsqlite_failure);

        let corruption_report = fsqlite_a
            .corruption_report
            .as_ref()
            .or(sqlite_a.corruption_report.as_ref());
        let (hashes_present, corruption_hashes) = collect_hash_evidence(corruption_report);

        let classification_is_known = matches!(
            combined_failure,
            FailureClass::None | FailureClass::RecoverableDivergence | FailureClass::DataLoss
        );

        let mut diagnostics = Vec::new();
        if !deterministic_replay {
            diagnostics.push(format!(
                "deterministic replay mismatch: {}",
                determinism_errors.join("; ")
            ));
        }
        if !hashes_present {
            diagnostics.push("missing before/after corruption hash evidence".to_owned());
        }
        if !classification_is_known {
            diagnostics.push("failure class was not assigned".to_owned());
        }

        let outcome = if diagnostics.is_empty() {
            "pass"
        } else {
            "fail"
        };
        let first_failure_for_record = diagnostics.first().cloned();
        if first_failure.is_none() && !diagnostics.is_empty() {
            first_failure = Some(format!(
                "{}: {}",
                entry.strategy_id,
                diagnostics.join(" | ")
            ));
        }

        let record = json!({
            "bead_id": BEAD_ID,
            "trace_id": trace_id,
            "run_id": run_id,
            "scenario_id": entry.strategy_id,
            "seed": seed,
            "scenario": {
                "strategy_name": entry.name,
                "description": entry.description,
                "category": category_str(entry.category),
                "severity": severity_str(entry.severity),
            },
            "timing_ms": {
                "sqlite3": sqlite_ms,
                "fsqlite": fsqlite_ms,
            },
            "outcome": outcome,
            "first_failure": first_failure_for_record,
            "checks": {
                "deterministic_replay": deterministic_replay,
                "before_after_hashes_present": hashes_present,
                "failure_classified": classification_is_known,
            },
            "failure_classification": {
                "sqlite3": sqlite_failure.as_str(),
                "fsqlite": fsqlite_failure.as_str(),
                "combined": combined_failure.as_str(),
            },
            "sqlite3": summarize_sqlite(&sqlite_a),
            "fsqlite": summarize_fsqlite(&fsqlite_a),
            "corruption_hashes": corruption_hashes,
            "log_standard_ref": LOG_STANDARD_REF,
            "replay_command": format!("{REPLAY_COMMAND} {strategy_id}"),
        });

        assert_outcome_schema_valid(&record);
        println!("SCENARIO_OUTCOME:{record}");
        records.push(record);
    }

    let filtered = env::var(STRATEGY_ENV_FILTER)
        .ok()
        .is_some_and(|raw| !raw.trim().is_empty());
    if !filtered {
        assert!(
            saw_db_strategy,
            "matrix must include at least one database-file corruption strategy"
        );
        assert!(
            saw_wal_strategy,
            "matrix must include at least one WAL corruption strategy"
        );
    }

    maybe_write_artifact(&records, first_failure.as_deref());
    if let Some(failure) = first_failure {
        panic!("crash/corruption matrix failed: {failure}");
    }
}

fn selected_strategy_ids() -> Vec<String> {
    let Ok(raw) = env::var(STRATEGY_ENV_FILTER) else {
        return DEFAULT_STRATEGY_IDS
            .iter()
            .map(|id| (*id).to_owned())
            .collect();
    };

    let ids: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .collect();

    if ids.is_empty() {
        DEFAULT_STRATEGY_IDS
            .iter()
            .map(|id| (*id).to_owned())
            .collect()
    } else {
        ids
    }
}

fn normalized_seed(seed: u64) -> u64 {
    if seed == 0 { FRANKEN_SEED } else { seed }
}

fn measure_pair(entry: &CatalogEntry) -> (SqliteBaselineResult, u64, FsqliteBaselineResult, u64) {
    let sqlite_dir = tempdir().unwrap_or_else(|e| panic!("create sqlite temp dir: {e}"));
    let started_sqlite = Instant::now();
    let sqlite = measure_sqlite3_baseline(entry, sqlite_dir.path()).unwrap_or_else(|e| {
        panic!(
            "sqlite baseline measurement failed for {}: {e}",
            entry.strategy_id
        )
    });
    let sqlite_ms = duration_to_ms(started_sqlite.elapsed().as_millis());

    let fsqlite_dir = tempdir().unwrap_or_else(|e| panic!("create fsqlite temp dir: {e}"));
    let started_fsqlite = Instant::now();
    let fsqlite = measure_fsqlite_baseline(entry, fsqlite_dir.path()).unwrap_or_else(|e| {
        panic!(
            "fsqlite baseline measurement failed for {}: {e}",
            entry.strategy_id
        )
    });
    let fsqlite_ms = duration_to_ms(started_fsqlite.elapsed().as_millis());

    (sqlite, sqlite_ms, fsqlite, fsqlite_ms)
}

fn determinism_errors(
    sqlite_a: &SqliteBaselineResult,
    sqlite_b: &SqliteBaselineResult,
    fsqlite_a: &FsqliteBaselineResult,
    fsqlite_b: &FsqliteBaselineResult,
) -> Vec<String> {
    let mut errors = Vec::new();

    if sqlite_a.outcome != sqlite_b.outcome {
        errors.push(format!(
            "sqlite outcome changed: {} -> {}",
            sqlite_a.outcome, sqlite_b.outcome
        ));
    }
    if sqlite_a.rows_after != sqlite_b.rows_after {
        errors.push(format!(
            "sqlite rows_after changed: {:?} -> {:?}",
            sqlite_a.rows_after, sqlite_b.rows_after
        ));
    }
    if sqlite_a.actual_dump_hash != sqlite_b.actual_dump_hash {
        errors.push("sqlite actual_dump_hash changed".to_owned());
    }
    if sqlite_a.expected_dump_hash != sqlite_b.expected_dump_hash {
        errors.push("sqlite expected_dump_hash changed".to_owned());
    }

    if fsqlite_a.recovery_tier != fsqlite_b.recovery_tier {
        errors.push(format!(
            "fsqlite recovery_tier changed: {} -> {}",
            fsqlite_a.recovery_tier, fsqlite_b.recovery_tier
        ));
    }
    if fsqlite_a.rows_after != fsqlite_b.rows_after {
        errors.push(format!(
            "fsqlite rows_after changed: {:?} -> {:?}",
            fsqlite_a.rows_after, fsqlite_b.rows_after
        ));
    }
    if fsqlite_a.actual_dump_hash != fsqlite_b.actual_dump_hash {
        errors.push("fsqlite actual_dump_hash changed".to_owned());
    }
    if fsqlite_a.expected_dump_hash != fsqlite_b.expected_dump_hash {
        errors.push("fsqlite expected_dump_hash changed".to_owned());
    }

    errors
}

fn classify_sqlite_failure(result: &SqliteBaselineResult) -> FailureClass {
    if result
        .rows_after
        .is_some_and(|rows| rows < result.rows_before)
    {
        return FailureClass::DataLoss;
    }

    match result.outcome {
        SqliteOutcomeTier::OpenFailed | SqliteOutcomeTier::IntegrityFailed => {
            FailureClass::DataLoss
        }
        SqliteOutcomeTier::LogicallyDiverged => FailureClass::RecoverableDivergence,
        SqliteOutcomeTier::OpenedAndMatches => FailureClass::None,
    }
}

fn classify_fsqlite_failure(result: &FsqliteBaselineResult) -> FailureClass {
    if result
        .rows_after
        .is_some_and(|rows| rows < result.rows_before)
    {
        return FailureClass::DataLoss;
    }

    match result.recovery_tier {
        FsqliteRecoveryTier::Lost => FailureClass::DataLoss,
        FsqliteRecoveryTier::Partial => FailureClass::RecoverableDivergence,
        FsqliteRecoveryTier::Recovered => FailureClass::None,
    }
}

fn combine_failure_class(left: FailureClass, right: FailureClass) -> FailureClass {
    if left.rank() >= right.rank() {
        left
    } else {
        right
    }
}

fn summarize_sqlite(result: &SqliteBaselineResult) -> Value {
    json!({
        "outcome": result.outcome.to_string(),
        "rows_before": result.rows_before,
        "rows_after": result.rows_after,
        "integrity_output": result.integrity_output,
        "open_error": result.open_error,
        "query_error": result.query_error,
        "expected_dump_hash": result.expected_dump_hash,
        "actual_dump_hash": result.actual_dump_hash,
    })
}

fn summarize_fsqlite(result: &FsqliteBaselineResult) -> Value {
    json!({
        "recovery_tier": result.recovery_tier.to_string(),
        "rows_before": result.rows_before,
        "rows_after": result.rows_after,
        "recovery_attempted": result.recovery_attempted,
        "recovery_succeeded": result.recovery_succeeded,
        "pages_recovered": result.pages_recovered,
        "integrity_output": result.integrity_output,
        "open_error": result.open_error,
        "expected_dump_hash": result.expected_dump_hash,
        "actual_dump_hash": result.actual_dump_hash,
    })
}

fn collect_hash_evidence(report: Option<&CorruptionReport>) -> (bool, Value) {
    let Some(report) = report else {
        return (false, json!({ "present": false }));
    };

    let modifications: Vec<Value> = report
        .modifications
        .iter()
        .map(|modification| {
            json!({
                "offset": modification.offset,
                "length": modification.length,
                "sha256_before": modification.sha256_before,
                "sha256_after": modification.sha256_after,
                "page_first": modification.page_first,
                "page_last": modification.page_last,
                "wal_frame_first": modification.wal_frame_first,
                "wal_frame_last": modification.wal_frame_last,
                "truncated": modification.sha256_after.is_none(),
            })
        })
        .collect();

    let before_hashes_present = report
        .modifications
        .iter()
        .all(|modification| !modification.sha256_before.is_empty());
    let after_hashes_or_truncate_present = report.modifications.iter().all(|modification| {
        modification
            .sha256_after
            .as_ref()
            .is_some_and(|hash| !hash.is_empty())
            || modification.sha256_after.is_none()
    });
    let hashes_present = !report.modifications.is_empty()
        && before_hashes_present
        && after_hashes_or_truncate_present;

    (
        hashes_present,
        json!({
            "present": true,
            "scenario_id": report.scenario_id,
            "original_sha256": report.original_sha256,
            "affected_bytes": report.affected_bytes,
            "affected_pages": report.affected_pages,
            "modifications": modifications,
        }),
    )
}

fn category_str(category: CorruptionCategory) -> &'static str {
    match category {
        CorruptionCategory::DatabaseFile => "database_file",
        CorruptionCategory::Wal => "wal",
        CorruptionCategory::Sidecar => "sidecar",
    }
}

fn severity_str(severity: CorruptionSeverity) -> &'static str {
    match severity {
        CorruptionSeverity::Subtle => "subtle",
        CorruptionSeverity::Moderate => "moderate",
        CorruptionSeverity::Severe => "severe",
    }
}

fn outcome_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": [
            "bead_id",
            "trace_id",
            "run_id",
            "scenario_id",
            "seed",
            "scenario",
            "timing_ms",
            "outcome",
            "checks",
            "failure_classification",
            "sqlite3",
            "fsqlite",
            "corruption_hashes",
            "log_standard_ref",
            "replay_command"
        ],
        "properties": {
            "bead_id": { "type": "string" },
            "trace_id": { "type": "string" },
            "run_id": { "type": "string" },
            "scenario_id": { "type": "string" },
            "seed": { "type": "integer", "minimum": 1 },
            "scenario": { "type": "object" },
            "timing_ms": {
                "type": "object",
                "required": ["sqlite3", "fsqlite"],
                "properties": {
                    "sqlite3": { "type": "integer", "minimum": 0 },
                    "fsqlite": { "type": "integer", "minimum": 0 }
                }
            },
            "outcome": { "type": "string", "enum": ["pass", "fail"] },
            "first_failure": { "type": ["string", "null"] },
            "checks": {
                "type": "object",
                "required": [
                    "deterministic_replay",
                    "before_after_hashes_present",
                    "failure_classified"
                ],
                "properties": {
                    "deterministic_replay": { "type": "boolean" },
                    "before_after_hashes_present": { "type": "boolean" },
                    "failure_classified": { "type": "boolean" }
                }
            },
            "failure_classification": {
                "type": "object",
                "required": ["sqlite3", "fsqlite", "combined"],
                "properties": {
                    "sqlite3": { "type": "string" },
                    "fsqlite": { "type": "string" },
                    "combined": { "type": "string" }
                }
            },
            "sqlite3": { "type": "object" },
            "fsqlite": { "type": "object" },
            "corruption_hashes": { "type": "object" },
            "log_standard_ref": { "type": "string" },
            "replay_command": { "type": "string" }
        }
    })
}

fn assert_outcome_schema_valid(record: &Value) {
    let schema = outcome_schema();
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .expect("build crash matrix outcome schema validator");

    let errors: Vec<String> = validator
        .iter_errors(record)
        .map(|err| err.to_string())
        .collect();
    assert!(
        errors.is_empty(),
        "scenario outcome failed schema validation:\n- {}",
        errors.join("\n- ")
    );
}

fn maybe_write_artifact(records: &[Value], first_failure: Option<&str>) {
    let Ok(path) = env::var(ARTIFACT_ENV_PATH) else {
        return;
    };

    let artifact_path = PathBuf::from(path);
    if let Some(parent) = artifact_path.parent() {
        fs::create_dir_all(parent).expect("create artifact parent directory");
    }

    let overall_status = if records.iter().all(|record| {
        record
            .get("outcome")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "pass")
    }) {
        "pass"
    } else {
        "fail"
    };

    let artifact = json!({
        "bead_id": BEAD_ID,
        "overall_status": overall_status,
        "run_count": records.len(),
        "first_failure": first_failure,
        "log_standard_ref": LOG_STANDARD_REF,
        "replay_command": REPLAY_COMMAND,
        "runs": records,
    });

    let payload = serde_json::to_vec_pretty(&artifact).expect("serialize artifact payload");
    let digest = Sha256::digest(&payload);
    let hash = format!("{digest:x}");

    fs::write(&artifact_path, payload).expect("write crash matrix artifact");
    eprintln!(
        "DEBUG bead_id={BEAD_ID} artifact_path={} sha256={} replay_command={REPLAY_COMMAND}",
        artifact_path.display(),
        hash
    );
}

fn duration_to_ms(ms: u128) -> u64 {
    u64::try_from(ms).unwrap_or(u64::MAX)
}
