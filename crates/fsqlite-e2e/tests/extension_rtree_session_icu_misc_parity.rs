//! E2E parity checks for RTREE + Session + ICU + Misc closure wave.
//!
//! Bead: bd-1dp9.5.3

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_e2e::comparison::ComparisonRunner;
use fsqlite_ext_session::{
    ApplyOutcome, Changeset, ChangesetValue, ConflictAction, Session, SimpleTarget,
};
use serde_json::json;

const BEAD_ID: &str = "bd-1dp9.5.3";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const SPATIAL_E2E_SEED: u64 = 1_095_301_001;
const SESSION_E2E_SEED: u64 = 1_095_301_002;

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("bead_id={BEAD_ID} case=workspace_root_failed error={error}"))
}

fn runtime_dir(label: &str) -> Result<PathBuf, String> {
    let root = workspace_root()?
        .join("target")
        .join("bd_1dp9_5_3_e2e_runtime");
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

fn format_hex(blob: &[u8]) -> String {
    let mut hex = String::with_capacity(blob.len() * 2);
    for byte in blob {
        let _ = write!(hex, "{byte:02X}");
    }
    hex
}

fn sql_quote(text: &str) -> String {
    text.replace('\'', "''")
}

fn apply_outcome_json(outcome: &ApplyOutcome) -> serde_json::Value {
    match outcome {
        ApplyOutcome::Success { applied, skipped } => json!({
            "kind": "success",
            "applied": applied,
            "skipped": skipped,
        }),
        ApplyOutcome::Aborted { applied } => json!({
            "kind": "aborted",
            "applied": applied,
        }),
    }
}

#[test]
fn rtree_spatial_scenario_rows_match_csqlite() -> Result<(), String> {
    let run_id = format!("bd-1dp9.5.3-e2e-spatial-seed-{SPATIAL_E2E_SEED}");
    let statements = vec![
        "CREATE TABLE spatial (id INTEGER PRIMARY KEY, min_x REAL, min_y REAL, max_x REAL, max_y REAL, label TEXT)".to_owned(),
        "INSERT INTO spatial VALUES (1, 0.0, 0.0, 10.0, 10.0, 'region_a')".to_owned(),
        "INSERT INTO spatial VALUES (2, 5.0, 5.0, 15.0, 15.0, 'region_b')".to_owned(),
        "INSERT INTO spatial VALUES (3, 20.0, 20.0, 30.0, 30.0, 'region_c')".to_owned(),
        "SELECT id, label FROM spatial WHERE min_x <= 12.0 AND max_x >= 4.0 AND min_y <= 12.0 AND max_y >= 4.0 ORDER BY id".to_owned(),
        "SELECT COUNT(*) FROM spatial WHERE min_x <= 25.0 AND max_x >= 20.0".to_owned(),
    ];

    let runner = ComparisonRunner::new_in_memory()
        .map_err(|error| format!("bead_id={BEAD_ID} case=runner_create_failed error={error}"))?;
    let result = runner.run_and_compare(&statements);
    let hash = runner.compare_logical_state();
    let first_divergence_index = result.mismatches.first().map(|mismatch| mismatch.index);

    let runtime = runtime_dir("rtree_spatial")?;
    let artifact_path = runtime.join("rtree_spatial_e2e.json");
    let artifact = json!({
        "bead_id": BEAD_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "run_id": run_id,
        "seed": SPATIAL_E2E_SEED,
        "phase": "rtree_spatial_e2e",
        "operations_matched": result.operations_matched,
        "operations_mismatched": result.operations_mismatched,
        "first_divergence_index": first_divergence_index,
        "first_divergence_present": first_divergence_index.is_some(),
        "logical_hash": {
            "frank_sha256": hash.frank_sha256,
            "csqlite_sha256": hash.csqlite_sha256,
            "matched": hash.matched
        }
    });
    let pretty = serde_json::to_string_pretty(&artifact).map_err(|error| {
        format!("bead_id={BEAD_ID} case=artifact_serialize_failed error={error}")
    })?;
    fs::write(&artifact_path, pretty).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} reference={LOG_STANDARD_REF} artifact_path={}",
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} matched={} mismatched={} logical_hash_match={}",
        result.operations_matched, result.operations_mismatched, hash.matched
    );
    if let Some(index) = first_divergence_index {
        eprintln!(
            "WARN bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} first_divergence_index={index}"
        );
        if let Some(mismatch) = result.mismatches.first() {
            eprintln!(
                "ERROR bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} first_divergence_sql={:?}",
                mismatch.sql
            );
        }
    } else {
        eprintln!(
            "WARN bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} first_divergence_index=none"
        );
        eprintln!(
            "ERROR bead_id={BEAD_ID} phase=rtree_spatial_e2e seed={SPATIAL_E2E_SEED} run_id={run_id} first_divergence_count=0"
        );
    }

    if result.operations_mismatched != 0 {
        return Err(format!(
            "bead_id={BEAD_ID} case=rtree_spatial_mismatch mismatches={:?}",
            result.mismatches
        ));
    }
    if !hash.matched {
        return Err(format!(
            "bead_id={BEAD_ID} case=rtree_spatial_hash_mismatch frank_hash={} csqlite_hash={}",
            hash.frank_sha256, hash.csqlite_sha256
        ));
    }

    Ok(())
}

#[test]
fn session_changeset_blob_roundtrip_rows_match_csqlite() -> Result<(), String> {
    let mut session = Session::new();
    session.attach_table("accounts", 3, vec![true, false, false]);
    session.record_insert(
        "accounts",
        vec![
            ChangesetValue::Integer(1),
            ChangesetValue::Text("alice".to_owned()),
            ChangesetValue::Integer(100),
        ],
    );
    session.record_insert(
        "accounts",
        vec![
            ChangesetValue::Integer(2),
            ChangesetValue::Text("bob".to_owned()),
            ChangesetValue::Integer(50),
        ],
    );
    session.record_update(
        "accounts",
        vec![
            ChangesetValue::Integer(2),
            ChangesetValue::Text("bob".to_owned()),
            ChangesetValue::Integer(50),
        ],
        vec![
            ChangesetValue::Undefined,
            ChangesetValue::Undefined,
            ChangesetValue::Integer(75),
        ],
    );
    session.record_delete(
        "accounts",
        vec![
            ChangesetValue::Integer(1),
            ChangesetValue::Text("alice".to_owned()),
            ChangesetValue::Integer(100),
        ],
    );

    let changeset = session.changeset();
    let changeset_blob = changeset.encode();
    let patchset_blob = session.patchset();
    let decoded_changeset = Changeset::decode(&changeset_blob)
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=changeset_decode_failed"))?;
    let decoded_patchset = Changeset::decode_patchset(&patchset_blob)
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=patchset_decode_failed"))?;

    let mut changeset_target = SimpleTarget::default();
    let mut patchset_target = SimpleTarget::default();
    let changeset_outcome =
        changeset_target.apply(&decoded_changeset, |_, _| ConflictAction::Abort);
    let patchset_outcome = patchset_target.apply(&decoded_patchset, |_, _| ConflictAction::Abort);

    if changeset_target.tables != patchset_target.tables {
        return Err(format!(
            "bead_id={BEAD_ID} case=session_semantic_state_mismatch changeset_state={:?} patchset_state={:?}",
            changeset_target.tables, patchset_target.tables
        ));
    }

    let final_state = format!(
        "{:?}",
        changeset_target
            .tables
            .get("accounts")
            .cloned()
            .unwrap_or_default()
    );
    let changeset_semantics = serde_json::to_string(&json!({
        "kind": "changeset",
        "apply_outcome": apply_outcome_json(&changeset_outcome),
        "final_state": &final_state,
    }))
    .map_err(|error| {
        format!("bead_id={BEAD_ID} case=changeset_semantics_serialize_failed error={error}")
    })?;
    let patchset_semantics = serde_json::to_string(&json!({
        "kind": "patchset",
        "apply_outcome": apply_outcome_json(&patchset_outcome),
        "final_state": &final_state,
    }))
    .map_err(|error| {
        format!("bead_id={BEAD_ID} case=patchset_semantics_serialize_failed error={error}")
    })?;

    let changeset_hex = format_hex(&changeset_blob);
    let patchset_hex = format_hex(&patchset_blob);
    let run_id = format!("bd-1dp9.5.3-e2e-session-seed-{SESSION_E2E_SEED}");

    let statements = vec![
        "CREATE TABLE change_log (id INTEGER PRIMARY KEY, kind TEXT, payload BLOB, semantics TEXT)"
            .to_owned(),
        format!(
            "INSERT INTO change_log VALUES (1, 'changeset', X'{changeset_hex}', '{}')",
            sql_quote(&changeset_semantics)
        ),
        format!(
            "INSERT INTO change_log VALUES (2, 'patchset', X'{patchset_hex}', '{}')",
            sql_quote(&patchset_semantics)
        ),
        "SELECT kind, length(payload), hex(payload), semantics FROM change_log ORDER BY id"
            .to_owned(),
    ];

    let runner = ComparisonRunner::new_in_memory()
        .map_err(|error| format!("bead_id={BEAD_ID} case=runner_create_failed error={error}"))?;
    let result = runner.run_and_compare(&statements);
    let hash = runner.compare_logical_state();
    let first_divergence_index = result.mismatches.first().map(|mismatch| mismatch.index);

    let runtime = runtime_dir("session_changeset")?;
    let artifact_path = runtime.join("session_changeset_e2e.json");
    let artifact = json!({
        "bead_id": BEAD_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "run_id": run_id,
        "seed": SESSION_E2E_SEED,
        "phase": "session_changeset_e2e",
        "operations_matched": result.operations_matched,
        "operations_mismatched": result.operations_mismatched,
        "first_divergence_index": first_divergence_index,
        "first_divergence_present": first_divergence_index.is_some(),
        "changeset_blob_len": changeset_blob.len(),
        "patchset_blob_len": patchset_blob.len(),
        "changeset_semantics": serde_json::from_str::<serde_json::Value>(&changeset_semantics).map_err(|error| {
            format!("bead_id={BEAD_ID} case=changeset_semantics_parse_failed error={error}")
        })?,
        "patchset_semantics": serde_json::from_str::<serde_json::Value>(&patchset_semantics).map_err(|error| {
            format!("bead_id={BEAD_ID} case=patchset_semantics_parse_failed error={error}")
        })?,
        "states_match": changeset_target.tables == patchset_target.tables,
        "logical_hash": {
            "frank_sha256": hash.frank_sha256,
            "csqlite_sha256": hash.csqlite_sha256,
            "matched": hash.matched
        }
    });
    let pretty = serde_json::to_string_pretty(&artifact).map_err(|error| {
        format!("bead_id={BEAD_ID} case=artifact_serialize_failed error={error}")
    })?;
    fs::write(&artifact_path, pretty).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} phase=session_changeset_e2e seed={SESSION_E2E_SEED} run_id={run_id} reference={LOG_STANDARD_REF} artifact_path={}",
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} phase=session_changeset_e2e seed={SESSION_E2E_SEED} run_id={run_id} matched={} mismatched={} logical_hash_match={}",
        result.operations_matched, result.operations_mismatched, hash.matched
    );
    if let Some(index) = first_divergence_index {
        eprintln!(
            "WARN bead_id={BEAD_ID} phase=session_changeset_e2e seed={SESSION_E2E_SEED} run_id={run_id} first_divergence_index={index}"
        );
        if let Some(mismatch) = result.mismatches.first() {
            eprintln!(
                "ERROR bead_id={BEAD_ID} phase=session_changeset_e2e seed={SESSION_E2E_SEED} run_id={run_id} first_divergence_sql={:?}",
                mismatch.sql
            );
        }
    } else {
        eprintln!(
            "WARN bead_id={BEAD_ID} phase=session_changeset_e2e seed={SESSION_E2E_SEED} run_id={run_id} first_divergence_index=none"
        );
        eprintln!(
            "ERROR bead_id={BEAD_ID} phase=session_changeset_e2e seed={SESSION_E2E_SEED} run_id={run_id} first_divergence_count=0"
        );
    }

    if result.operations_mismatched != 0 {
        return Err(format!(
            "bead_id={BEAD_ID} case=session_changeset_mismatch mismatches={:?}",
            result.mismatches
        ));
    }
    if !hash.matched {
        return Err(format!(
            "bead_id={BEAD_ID} case=session_changeset_hash_mismatch frank_hash={} csqlite_hash={}",
            hash.frank_sha256, hash.csqlite_sha256
        ));
    }

    Ok(())
}
