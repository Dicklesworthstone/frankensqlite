use std::cmp::Ordering;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fsqlite_ext_icu::{IcuCollation, IcuLocale, IcuLowerFunc, IcuUpperFunc};
use fsqlite_ext_misc::{DecimalAddFunc, DecimalCmpFunc, UuidBlobFunc, UuidStrFunc};
use fsqlite_ext_rtree::{MBoundingBox, RtreeConfig, RtreeCoordType, RtreeEntry, RtreeIndex};
use fsqlite_ext_session::{
    ApplyOutcome, ChangeOp, Changeset, ChangesetValue, ConflictAction, Session, SimpleTarget,
};
use fsqlite_func::collation::CollationFunction;
use fsqlite_func::scalar::ScalarFunction;
use fsqlite_harness::differential_v2::{
    self, DifferentialResult, EngineIdentity, ExecutionEnvelope, FsqliteExecutor, NormalizedValue,
    Outcome, SqlExecutor,
};
use fsqlite_types::SqliteValue;
use serde_json::json;

const BEAD_ID: &str = "bd-1dp9.5.3";
const LOG_STANDARD_REF: &str = "bd-1fpm";
const SPATIAL_WAVE_SEED: u64 = 1_095_300_001;
const SESSION_WAVE_SEED: u64 = 1_095_300_002;

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
    let root = workspace_root()?.join("target").join("bd_1dp9_5_3_runtime");
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

fn format_hex(blob: &[u8]) -> String {
    let mut hex = String::with_capacity(blob.len() * 2);
    for byte in blob {
        let _ = write!(hex, "{byte:02X}");
    }
    hex
}

fn write_structured_artifact(
    phase: &str,
    run_id: &str,
    seed: u64,
    result: &DifferentialResult,
) -> Result<PathBuf, String> {
    let runtime = runtime_dir(phase)?;
    let artifact_path = runtime.join(format!("{phase}.json"));
    let artifact = json!({
        "bead_id": BEAD_ID,
        "log_standard_ref": LOG_STANDARD_REF,
        "run_id": run_id,
        "seed": seed,
        "phase": phase,
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
    let pretty = serde_json::to_string_pretty(&artifact).map_err(|error| {
        format!("bead_id={BEAD_ID} case=artifact_serialize_failed error={error}")
    })?;
    fs::write(&artifact_path, pretty).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=artifact_write_failed path={} error={error}",
            artifact_path.display()
        )
    })?;

    Ok(artifact_path)
}

fn log_wave_result(
    phase: &str,
    seed: u64,
    result: &DifferentialResult,
    artifact_path: &Path,
    run_id: &str,
) {
    eprintln!(
        "DEBUG bead_id={BEAD_ID} phase={phase} seed={seed} run_id={run_id} envelope_id={} reference={LOG_STANDARD_REF} artifact_path={}",
        result.artifact_hashes.envelope_id,
        artifact_path.display()
    );
    eprintln!(
        "INFO bead_id={BEAD_ID} phase={phase} seed={seed} run_id={run_id} outcome={} matched={} mismatched={} total={} result_hash={}",
        result.outcome,
        result.statements_matched,
        result.statements_mismatched,
        result.statements_total,
        result.artifact_hashes.result_hash
    );

    if let Some(index) = result.first_divergence_index {
        eprintln!(
            "WARN bead_id={BEAD_ID} phase={phase} seed={seed} run_id={run_id} first_divergence_index={index}"
        );
        if let Some(divergence) = result.divergences.first() {
            eprintln!(
                "ERROR bead_id={BEAD_ID} phase={phase} seed={seed} run_id={run_id} first_divergence_sql={:?}",
                divergence.sql
            );
        }
    } else {
        eprintln!(
            "WARN bead_id={BEAD_ID} phase={phase} seed={seed} run_id={run_id} first_divergence_index=none"
        );
        eprintln!(
            "ERROR bead_id={BEAD_ID} phase={phase} seed={seed} run_id={run_id} first_divergence_count=0"
        );
    }
}

fn bbox(coords: Vec<f64>) -> Result<MBoundingBox, String> {
    MBoundingBox::new(coords).ok_or_else(|| format!("bead_id={BEAD_ID} case=invalid_bbox"))
}

#[test]
fn test_unit_rtree_geometry_invariants() -> Result<(), String> {
    let config = RtreeConfig::new(2, RtreeCoordType::Float32)
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=rtree_config_invalid"))?;
    let mut index = RtreeIndex::new(config);

    let entry_1 = RtreeEntry {
        id: 1,
        bbox: bbox(vec![0.0, 10.0, 0.0, 10.0])?,
    };
    let entry_2 = RtreeEntry {
        id: 2,
        bbox: bbox(vec![5.0, 15.0, 5.0, 15.0])?,
    };
    let entry_3 = RtreeEntry {
        id: 3,
        bbox: bbox(vec![20.0, 30.0, 20.0, 30.0])?,
    };

    assert!(index.insert(entry_1));
    assert!(index.insert(entry_2));
    assert!(index.insert(entry_3));
    assert_eq!(index.len(), 3);

    let query = bbox(vec![4.0, 12.0, 4.0, 12.0])?;
    let mut ids: Vec<i64> = index
        .range_query(&query)
        .iter()
        .map(|entry| entry.id)
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2]);

    assert!(index.update(3, bbox(vec![8.0, 11.0, 8.0, 11.0])?));
    let mut ids_after_update: Vec<i64> = index
        .range_query(&query)
        .iter()
        .map(|entry| entry.id)
        .collect();
    ids_after_update.sort_unstable();
    assert_eq!(ids_after_update, vec![1, 2, 3]);

    eprintln!(
        "INFO bead_id={BEAD_ID} phase=unit_rtree_geometry run_id=unit-rtree seed={SPATIAL_WAVE_SEED} reference={LOG_STANDARD_REF}"
    );
    Ok(())
}

#[test]
fn test_unit_session_changeset_apply_invariants() -> Result<(), String> {
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
    session.record_update(
        "accounts",
        vec![
            ChangesetValue::Integer(1),
            ChangesetValue::Text("alice".to_owned()),
            ChangesetValue::Integer(100),
        ],
        vec![
            ChangesetValue::Undefined,
            ChangesetValue::Undefined,
            ChangesetValue::Integer(125),
        ],
    );
    session.record_delete(
        "accounts",
        vec![
            ChangesetValue::Integer(2),
            ChangesetValue::Text("bob".to_owned()),
            ChangesetValue::Integer(50),
        ],
    );

    let changeset = session.changeset();
    let encoded = changeset.encode();
    let decoded = Changeset::decode(&encoded)
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=changeset_decode_failed"))?;
    assert_eq!(decoded.tables.len(), 1);
    assert_eq!(decoded.tables[0].rows.len(), 3);
    assert_eq!(decoded.tables[0].rows[0].op, ChangeOp::Insert);
    assert_eq!(decoded.tables[0].rows[1].op, ChangeOp::Update);
    assert_eq!(decoded.tables[0].rows[2].op, ChangeOp::Delete);

    let inverted = decoded.invert();
    assert_eq!(inverted.tables[0].rows[0].op, ChangeOp::Delete);
    assert_eq!(inverted.tables[0].rows[1].op, ChangeOp::Update);
    assert_eq!(inverted.tables[0].rows[2].op, ChangeOp::Insert);

    let mut target = SimpleTarget::default();
    let outcome = target.apply(&decoded, |_kind, _row| ConflictAction::OmitChange);
    match outcome {
        ApplyOutcome::Success { applied, skipped } => {
            assert_eq!(applied, 2);
            assert_eq!(skipped, 1);
        }
        ApplyOutcome::Aborted { applied } => {
            return Err(format!(
                "bead_id={BEAD_ID} case=session_apply_aborted applied={applied}"
            ));
        }
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} phase=unit_session_changeset run_id=unit-session seed={SESSION_WAVE_SEED} encoded_len={}",
        encoded.len()
    );
    Ok(())
}

#[test]
fn test_unit_icu_collation_and_case_invariants() -> Result<(), String> {
    let upper = IcuUpperFunc
        .invoke(&[
            SqliteValue::Text("stra\u{00DF}e".to_owned()),
            SqliteValue::Text("de_DE".to_owned()),
        ])
        .map_err(|error| format!("bead_id={BEAD_ID} case=icu_upper_failed error={error}"))?;
    assert_eq!(upper, SqliteValue::Text("STRASSE".to_owned()));

    let lower = IcuLowerFunc
        .invoke(&[
            SqliteValue::Text("I\u{0130}".to_owned()),
            SqliteValue::Text("tr_TR".to_owned()),
        ])
        .map_err(|error| format!("bead_id={BEAD_ID} case=icu_lower_failed error={error}"))?;
    assert_eq!(lower, SqliteValue::Text("\u{0131}i".to_owned()));

    let locale = IcuLocale::parse("de_DE")
        .map_err(|error| format!("bead_id={BEAD_ID} case=icu_locale_parse_failed error={error}"))?;
    let collation = IcuCollation::new("icu_de".to_owned(), locale);
    assert_eq!(
        collation.compare(b"ad", "\u{00E4}".as_bytes()),
        Ordering::Less
    );
    assert_eq!(
        collation.compare("\u{00E4}".as_bytes(), b"af"),
        Ordering::Less
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} phase=unit_icu_collation run_id=unit-icu seed={SPATIAL_WAVE_SEED} reference={LOG_STANDARD_REF}"
    );
    Ok(())
}

#[test]
fn test_unit_misc_decimal_and_uuid_conversion_invariants() -> Result<(), String> {
    let sum = DecimalAddFunc
        .invoke(&[
            SqliteValue::Text("123.450".to_owned()),
            SqliteValue::Text("0.55".to_owned()),
        ])
        .map_err(|error| format!("bead_id={BEAD_ID} case=decimal_add_failed error={error}"))?;
    assert_eq!(sum, SqliteValue::Text("124".to_owned()));

    let cmp = DecimalCmpFunc
        .invoke(&[
            SqliteValue::Text("10.00".to_owned()),
            SqliteValue::Text("9.99".to_owned()),
        ])
        .map_err(|error| format!("bead_id={BEAD_ID} case=decimal_cmp_failed error={error}"))?;
    assert_eq!(cmp, SqliteValue::Integer(1));

    let canonical_uuid = SqliteValue::Text("123e4567-e89b-12d3-a456-426614174000".to_owned());
    let blob = UuidBlobFunc
        .invoke(std::slice::from_ref(&canonical_uuid))
        .map_err(|error| format!("bead_id={BEAD_ID} case=uuid_blob_failed error={error}"))?;
    let SqliteValue::Blob(bytes) = blob else {
        return Err(format!("bead_id={BEAD_ID} case=uuid_blob_not_blob"));
    };
    let restored = UuidStrFunc
        .invoke(&[SqliteValue::Blob(bytes)])
        .map_err(|error| format!("bead_id={BEAD_ID} case=uuid_str_failed error={error}"))?;
    assert_eq!(restored, canonical_uuid);

    eprintln!(
        "INFO bead_id={BEAD_ID} phase=unit_misc_decimal_uuid run_id=unit-misc seed={SESSION_WAVE_SEED} reference={LOG_STANDARD_REF}"
    );
    Ok(())
}

#[test]
fn test_rtree_spatial_differential_wave_with_artifact() -> Result<(), String> {
    let run_id = format!("bd-1dp9.5.3-spatial-wave-seed-{SPATIAL_WAVE_SEED}");
    let envelope = ExecutionEnvelope::builder(SPATIAL_WAVE_SEED)
        .run_id(run_id.clone())
        .engines("fsqlite", "sqlite")
        .schema([
            "CREATE TABLE spatial (id INTEGER PRIMARY KEY, min_x REAL, min_y REAL, max_x REAL, max_y REAL, label TEXT)".to_owned(),
        ])
        .workload([
            "INSERT INTO spatial VALUES (1, 0.0, 0.0, 10.0, 10.0, 'region_a')".to_owned(),
            "INSERT INTO spatial VALUES (2, 5.0, 5.0, 15.0, 15.0, 'region_b')".to_owned(),
            "INSERT INTO spatial VALUES (3, 20.0, 20.0, 30.0, 30.0, 'region_c')".to_owned(),
            "SELECT id, label FROM spatial WHERE min_x <= 12.0 AND max_x >= 4.0 AND min_y <= 12.0 AND max_y >= 4.0 ORDER BY id".to_owned(),
            "SELECT COUNT(*) FROM spatial WHERE min_x <= 25.0 AND max_x >= 20.0".to_owned(),
        ])
        .build();

    let result = run_wave(&envelope)?;
    let artifact_path = write_structured_artifact(
        "rtree_spatial_differential",
        &run_id,
        SPATIAL_WAVE_SEED,
        &result,
    )?;
    log_wave_result(
        "rtree_spatial_differential",
        SPATIAL_WAVE_SEED,
        &result,
        &artifact_path,
        &run_id,
    );

    if result.outcome != Outcome::Pass {
        return Err(format!(
            "bead_id={BEAD_ID} case=spatial_wave_failed outcome={} first_divergence={:?} divergences={:?}",
            result.outcome, result.first_divergence_index, result.divergences
        ));
    }

    Ok(())
}

#[test]
fn test_session_changeset_differential_wave_with_artifact() -> Result<(), String> {
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
    let changeset = session.changeset();
    let blob = changeset.encode();
    let hex = format_hex(&blob);

    let run_id = format!("bd-1dp9.5.3-session-wave-seed-{SESSION_WAVE_SEED}");
    let insert_sql = format!("INSERT INTO changesets VALUES (1, X'{hex}')");
    let envelope = ExecutionEnvelope::builder(SESSION_WAVE_SEED)
        .run_id(run_id.clone())
        .engines("fsqlite", "sqlite")
        .schema(["CREATE TABLE changesets (id INTEGER PRIMARY KEY, payload BLOB)".to_owned()])
        .workload([
            insert_sql,
            "SELECT length(payload) FROM changesets WHERE id = 1".to_owned(),
            "SELECT hex(payload) FROM changesets WHERE id = 1".to_owned(),
        ])
        .build();

    let result = run_wave(&envelope)?;
    let artifact_path = write_structured_artifact(
        "session_changeset_differential",
        &run_id,
        SESSION_WAVE_SEED,
        &result,
    )?;
    log_wave_result(
        "session_changeset_differential",
        SESSION_WAVE_SEED,
        &result,
        &artifact_path,
        &run_id,
    );

    if result.outcome != Outcome::Pass {
        return Err(format!(
            "bead_id={BEAD_ID} case=session_wave_failed outcome={} first_divergence={:?} divergences={:?}",
            result.outcome, result.first_divergence_index, result.divergences
        ));
    }

    let decoded = Changeset::decode(&blob)
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=decoded_changeset_none"))?;
    assert_eq!(decoded.tables.len(), 1);
    assert_eq!(decoded.tables[0].rows.len(), 2);

    Ok(())
}
