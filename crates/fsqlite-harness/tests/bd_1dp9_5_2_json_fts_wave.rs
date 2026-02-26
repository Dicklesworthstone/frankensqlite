use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_harness::differential_v2::{
    self, DifferentialResult, EngineIdentity, ExecutionEnvelope, FsqliteExecutor, NormalizedValue,
    Outcome, SqlExecutor,
};
use serde_json::json;

const BEAD_ID: &str = "bd-1dp9.5.2";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const JSON_WAVE_SEED: u64 = 1_095_200_001;
const JSON_WAVE_SEED_2: u64 = 1_095_200_002;

struct RusqliteExecutor {
    conn: rusqlite::Connection,
}

impl RusqliteExecutor {
    fn open_in_memory() -> Result<Self, String> {
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|error| format!("bead_id={BEAD_ID} case=sqlite_open_failed error={error}"))?;
        Ok(Self { conn })
    }
}

impl SqlExecutor for RusqliteExecutor {
    fn execute(&self, sql: &str) -> Result<usize, String> {
        self.conn.execute(sql.trim(), []).map_err(|error| {
            format!("bead_id={BEAD_ID} case=sqlite_execute_failed sql={sql:?} error={error}")
        })
    }

    fn query(&self, sql: &str) -> Result<Vec<Vec<NormalizedValue>>, String> {
        let mut stmt = self.conn.prepare(sql.trim()).map_err(|error| {
            format!("bead_id={BEAD_ID} case=sqlite_prepare_failed sql={sql:?} error={error}")
        })?;
        let column_count = stmt.column_count();
        let rows = stmt
            .query_map([], |row| {
                let mut values = Vec::with_capacity(column_count);
                for index in 0..column_count {
                    let value: rusqlite::types::Value =
                        row.get(index).unwrap_or(rusqlite::types::Value::Null);
                    let normalized = match value {
                        rusqlite::types::Value::Null => NormalizedValue::Null,
                        rusqlite::types::Value::Integer(number) => NormalizedValue::Integer(number),
                        rusqlite::types::Value::Real(number) => NormalizedValue::Real(number),
                        rusqlite::types::Value::Text(text) => NormalizedValue::Text(text),
                        rusqlite::types::Value::Blob(blob) => NormalizedValue::Blob(blob),
                    };
                    values.push(normalized);
                }
                Ok(values)
            })
            .map_err(|error| {
                format!("bead_id={BEAD_ID} case=sqlite_query_failed sql={sql:?} error={error}")
            })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            format!("bead_id={BEAD_ID} case=sqlite_collect_failed sql={sql:?} error={error}")
        })
    }

    fn engine_identity(&self) -> EngineIdentity {
        EngineIdentity::CSqliteOracle
    }
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("bead_id={BEAD_ID} case=workspace_root_failed error={error}"))
}

fn runtime_dir(label: &str) -> Result<PathBuf, String> {
    let root = workspace_root()?.join("target").join("bd_1dp9_5_2_runtime");
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

fn run_wave(envelope: &ExecutionEnvelope) -> Result<DifferentialResult, String> {
    let fsqlite = FsqliteExecutor::open_in_memory()
        .map_err(|error| format!("bead_id={BEAD_ID} case=fsqlite_open_failed error={error}"))?;
    let sqlite = RusqliteExecutor::open_in_memory()?;
    Ok(differential_v2::run_differential(
        envelope, &fsqlite, &sqlite,
    ))
}

fn log_wave_result(phase: &str, seed: u64, result: &DifferentialResult) {
    eprintln!(
        "DEBUG bead_id={BEAD_ID} phase={phase} seed={seed} run_id={:?} envelope_id={} reference={LOG_STANDARD_REF}",
        result.envelope.run_id, result.artifact_hashes.envelope_id
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} phase={phase} seed={seed} outcome={} matched={} mismatched={} total={} result_hash={}",
        result.outcome,
        result.statements_matched,
        result.statements_mismatched,
        result.statements_total,
        result.artifact_hashes.result_hash
    );

    if let Some(index) = result.first_divergence_index {
        eprintln!(
            "WARN bead_id={BEAD_ID} phase={phase} seed={seed} first_divergence_index={index}"
        );
        if let Some(divergence) = result.divergences.first() {
            eprintln!(
                "ERROR bead_id={BEAD_ID} phase={phase} seed={seed} first_divergence_sql={:?}",
                divergence.sql
            );
        }
    } else {
        eprintln!("WARN bead_id={BEAD_ID} phase={phase} seed={seed} first_divergence_index=none");
        eprintln!("ERROR bead_id={BEAD_ID} phase={phase} seed={seed} first_divergence_count=0");
    }
}

#[test]
fn test_json1_differential_wave_parity() -> Result<(), String> {
    let run_id = format!("bd-1dp9.5.2-json-wave-seed-{JSON_WAVE_SEED}");
    let envelope = ExecutionEnvelope::builder(JSON_WAVE_SEED)
        .run_id(run_id)
        .engines("fsqlite", "sqlite")
        .workload([
            r#"SELECT json_valid('{"a":1}')"#.to_owned(),
            r#"SELECT json_extract('{"a":[10,20]}', '$.a[1]')"#.to_owned(),
            r#"SELECT json_type('{"a":[10,20]}', '$.a')"#.to_owned(),
            r#"SELECT json_extract(json_set('{"a":1}', '$.b', 2), '$.b')"#.to_owned(),
            r#"SELECT json_extract(json_remove('{"a":1,"b":2}', '$.b'), '$.b')"#.to_owned(),
        ])
        .build();

    let result = run_wave(&envelope)?;
    log_wave_result("json1_differential", JSON_WAVE_SEED, &result);

    if result.outcome != Outcome::Pass {
        return Err(format!(
            "bead_id={BEAD_ID} case=json_wave_failed outcome={} first_divergence={:?} divergences={:?}",
            result.outcome, result.first_divergence_index, result.divergences
        ));
    }
    Ok(())
}

#[test]
fn test_json1_secondary_differential_wave() -> Result<(), String> {
    let run_id = format!("bd-1dp9.5.2-json-wave-seed-{JSON_WAVE_SEED_2}");
    let envelope = ExecutionEnvelope::builder(JSON_WAVE_SEED_2)
        .run_id(run_id)
        .engines("fsqlite", "sqlite")
        .workload([
            r"SELECT json_quote('alpha')".to_owned(),
            r"SELECT json_array(1, 'x', NULL)".to_owned(),
            r"SELECT json_extract(json_object('k', 'v', 'n', 2), '$.n')".to_owned(),
            r#"SELECT json_valid(json_patch('{"a":1}', '{"b":2}'))"#.to_owned(),
        ])
        .build();

    let result = run_wave(&envelope)?;
    log_wave_result("json1_secondary_differential", JSON_WAVE_SEED_2, &result);

    if result.outcome != Outcome::Pass {
        return Err(format!(
            "bead_id={BEAD_ID} case=json_wave_2_failed outcome={} first_divergence={:?} divergences={:?}",
            result.outcome, result.first_divergence_index, result.divergences
        ));
    }
    Ok(())
}

#[test]
fn test_wave_writes_structured_artifact() -> Result<(), String> {
    let run_id = format!("bd-1dp9.5.2-artifact-wave-seed-{JSON_WAVE_SEED}");
    let envelope = ExecutionEnvelope::builder(JSON_WAVE_SEED)
        .run_id(run_id.clone())
        .engines("fsqlite", "sqlite")
        .workload([
            r#"SELECT json_valid('{"artifact":true}')"#.to_owned(),
            r"SELECT json_quote('artifact')".to_owned(),
        ])
        .build();

    let result = run_wave(&envelope)?;
    let runtime = runtime_dir("artifact")?;
    let artifact_path = runtime.join("bd_1dp9_5_2_json_fts_wave.json");
    let artifact = json!({
        "bead_id": BEAD_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "run_id": run_id,
        "seed": JSON_WAVE_SEED,
        "phase": "json_fts_wave",
        "outcome": result.outcome.to_string(),
        "statements_total": result.statements_total,
        "statements_matched": result.statements_matched,
        "statements_mismatched": result.statements_mismatched,
        "first_divergence_index": result.first_divergence_index,
        "first_divergence_present": result.first_divergence_index.is_some(),
        "artifact_hashes": {
            "envelope_id": result.artifact_hashes.envelope_id,
            "result_hash": result.artifact_hashes.result_hash,
            "workload_hash": result.artifact_hashes.workload_hash
        }
    });
    let artifact_pretty = serde_json::to_string_pretty(&artifact).map_err(|error| {
        format!("bead_id={BEAD_ID} case=artifact_serialize_failed error={error}")
    })?;
    fs::write(&artifact_path, artifact_pretty).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    eprintln!(
        "DEBUG bead_id={BEAD_ID} phase=artifact_written run_id={run_id} path={} reference={LOG_STANDARD_REF}",
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} phase=artifact_summary run_id={run_id} result_hash={} workload_hash={}",
        result.artifact_hashes.result_hash, result.artifact_hashes.workload_hash
    );
    eprintln!(
        "WARN bead_id={BEAD_ID} phase=artifact_first_divergence run_id={run_id} present={}",
        result.first_divergence_index.is_some()
    );
    eprintln!(
        "ERROR bead_id={BEAD_ID} phase=artifact_terminal run_id={run_id} divergence_count={}",
        result.divergences.len()
    );

    Ok(())
}
