use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use blake3::Hasher;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::vectorized::{Batch, ColumnData, ColumnSpec, ColumnVectorType};
use fsqlite_vdbe::vectorized_hash_join::{JoinType, hash_join_build, hash_join_probe};
use fsqlite_vdbe::vectorized_join::{TrieRelation, TrieRow, leapfrog_join};
use serde::{Deserialize, Serialize};

const BEAD_ID: &str = "bd-2qr3a.5";
const GOLDEN_MANIFEST_RELATIVE: &str = "conformance/leapfrog_join_golden_blake3.json";
const UPDATE_ENV_VAR: &str = "FSQLITE_UPDATE_GOLDEN";
const SCHEMA_VERSION: u32 = 1;
const HASH_ALGORITHM: &str = "blake3";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LeapfrogGoldenEntry {
    scenario_id: String,
    relation_count: usize,
    relation_sizes: Vec<usize>,
    hash_join_blake3: String,
    leapfrog_blake3: String,
    row_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LeapfrogGoldenManifest {
    schema_version: u32,
    hash_algorithm: String,
    entries: Vec<LeapfrogGoldenEntry>,
}

#[derive(Debug, Clone)]
struct JoinScenario {
    id: &'static str,
    relations: Vec<Vec<i64>>,
}

fn manifest_path() -> Result<PathBuf, String> {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let canonical_root = crate_root.canonicalize().map_err(|error| {
        format!("bead_id={BEAD_ID} case=manifest_root_canonicalize error={error}")
    })?;
    Ok(canonical_root.join(GOLDEN_MANIFEST_RELATIVE))
}

fn update_requested() -> bool {
    std::env::var(UPDATE_ENV_VAR).is_ok_and(|raw| {
        let normalized = raw.trim();
        normalized == "1" || normalized.eq_ignore_ascii_case("true")
    })
}

fn scenarios() -> Vec<JoinScenario> {
    vec![
        JoinScenario {
            id: "tpch_q2_sf1_like",
            relations: vec![
                (0_i64..1_000).collect(),
                (0_i64..500).map(|i| i * 2).collect(),
                (0_i64..400).map(|i| i * 3).collect(),
                (0_i64..200).map(|i| i * 5).collect(),
                (0_i64..100).map(|i| i * 7).collect(),
            ],
        },
        JoinScenario {
            id: "tpch_q7_sf1_like",
            relations: vec![
                (0_i64..2_000).collect(),
                (200_i64..1_800).step_by(2).collect(),
                (100_i64..1_600).step_by(3).collect(),
                (150_i64..1_700).step_by(5).collect(),
            ],
        },
        JoinScenario {
            id: "tpch_q9_sf1_like",
            relations: vec![
                (0_i64..500).collect(),
                (100_i64..400).collect(),
                (200_i64..600).collect(),
                (150_i64..450).collect(),
                (250_i64..550).collect(),
                (200_i64..500).collect(),
            ],
        },
    ]
}

fn build_trie(keys: &[i64]) -> TrieRelation {
    let mut rows: Vec<TrieRow> = keys
        .iter()
        .enumerate()
        .map(|(i, &key)| TrieRow::new(vec![SqliteValue::Integer(key)], i))
        .collect();
    rows.sort_by(|a, b| {
        a.key
            .partial_cmp(&b.key)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows.dedup_by(|a, b| a.key == b.key);
    TrieRelation::from_sorted_rows(rows).expect("build trie relation")
}

fn build_hash_batch(name: &str, keys: &[i64]) -> Batch {
    let specs = vec![
        ColumnSpec::new(format!("{name}_key"), ColumnVectorType::Int64),
        ColumnSpec::new(format!("{name}_payload"), ColumnVectorType::Int64),
    ];
    let rows: Vec<Vec<SqliteValue>> = keys
        .iter()
        .enumerate()
        .map(|(i, &key)| vec![SqliteValue::Integer(key), SqliteValue::Integer(i as i64)])
        .collect();
    Batch::from_rows(&rows, &specs, rows.len().max(1)).expect("build hash batch")
}

fn extract_int64_column_sorted(batch: &Batch, col_idx: usize) -> Result<Vec<i64>, String> {
    let column = batch
        .columns()
        .get(col_idx)
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=column_out_of_bounds column={col_idx}"))?;
    let data = match &column.data {
        ColumnData::Int64(values) => values,
        other => {
            return Err(format!(
                "bead_id={BEAD_ID} case=unexpected_column_type column={col_idx} type={other:?}"
            ));
        }
    };

    let mut output = Vec::with_capacity(batch.row_count());
    for &selected in batch.selection().as_slice() {
        let row = usize::from(selected);
        if !column.validity.is_valid(row) {
            return Err(format!(
                "bead_id={BEAD_ID} case=unexpected_null_key row={row} column={col_idx}"
            ));
        }
        output.push(data.as_slice()[row]);
    }
    output.sort_unstable();
    Ok(output)
}

fn run_pairwise_hash_join(relation_keys: &[Vec<i64>]) -> Result<Vec<i64>, String> {
    let (first, rest) = relation_keys
        .split_first()
        .ok_or_else(|| format!("bead_id={BEAD_ID} case=missing_relations"))?;
    if rest.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=not_enough_relations relation_count={}",
            relation_keys.len()
        ));
    }

    let mut current = build_hash_batch("r0", first);
    for (idx, keys) in rest.iter().enumerate() {
        let probe = build_hash_batch(&format!("r{}", idx + 1), keys);
        let table = hash_join_build(current, &[0]).map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=hash_join_build relation={} error={error}",
                idx
            )
        })?;
        current = hash_join_probe(&table, &probe, &[0], JoinType::Inner).map_err(|error| {
            format!(
                "bead_id={BEAD_ID} case=hash_join_probe relation={} error={error}",
                idx
            )
        })?;
    }

    extract_int64_column_sorted(&current, 0)
}

fn run_leapfrog_join(relation_keys: &[Vec<i64>]) -> Result<Vec<i64>, String> {
    if relation_keys.len() < 2 {
        return Err(format!(
            "bead_id={BEAD_ID} case=not_enough_relations relation_count={}",
            relation_keys.len()
        ));
    }

    let tries: Vec<TrieRelation> = relation_keys.iter().map(|keys| build_trie(keys)).collect();
    let trie_refs: Vec<&TrieRelation> = tries.iter().collect();
    let matches = leapfrog_join(&trie_refs)
        .map_err(|error| format!("bead_id={BEAD_ID} case=leapfrog_join error={error}"))?;

    let mut output = Vec::new();
    for join_match in matches {
        let key = join_match
            .key
            .first()
            .ok_or_else(|| format!("bead_id={BEAD_ID} case=empty_leapfrog_key"))?;
        let key_value = match key {
            SqliteValue::Integer(value) => *value,
            other => {
                return Err(format!(
                    "bead_id={BEAD_ID} case=non_integer_key value={other:?}"
                ));
            }
        };
        let multiplicity = usize::try_from(join_match.tuple_multiplicity()).map_err(|error| {
            format!("bead_id={BEAD_ID} case=tuple_multiplicity_overflow error={error}")
        })?;
        for _ in 0..multiplicity {
            output.push(key_value);
        }
    }
    output.sort_unstable();
    Ok(output)
}

fn blake3_keys(keys: &[i64]) -> String {
    let mut hasher = Hasher::new();
    for key in keys {
        hasher.update(key.to_string().as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().to_hex().to_string()
}

fn compute_manifest() -> Result<LeapfrogGoldenManifest, String> {
    let mut entries = Vec::new();
    for scenario in scenarios() {
        let hash_join_rows = run_pairwise_hash_join(&scenario.relations)?;
        let leapfrog_rows = run_leapfrog_join(&scenario.relations)?;

        if hash_join_rows != leapfrog_rows {
            let preview = hash_join_rows
                .iter()
                .zip(&leapfrog_rows)
                .take(10)
                .enumerate()
                .map(|(idx, (left, right))| format!("idx={idx} hash={left} leapfrog={right}"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "bead_id={BEAD_ID} case=row_mismatch scenario={} hash_rows={} leapfrog_rows={} preview=[{}]",
                scenario.id,
                hash_join_rows.len(),
                leapfrog_rows.len(),
                preview
            ));
        }

        entries.push(LeapfrogGoldenEntry {
            scenario_id: scenario.id.to_owned(),
            relation_count: scenario.relations.len(),
            relation_sizes: scenario.relations.iter().map(Vec::len).collect(),
            hash_join_blake3: blake3_keys(&hash_join_rows),
            leapfrog_blake3: blake3_keys(&leapfrog_rows),
            row_count: hash_join_rows.len(),
        });
    }

    entries.sort_by(|left, right| left.scenario_id.cmp(&right.scenario_id));
    Ok(LeapfrogGoldenManifest {
        schema_version: SCHEMA_VERSION,
        hash_algorithm: HASH_ALGORITHM.to_owned(),
        entries,
    })
}

fn write_manifest(path: &Path, manifest: &LeapfrogGoldenManifest) -> Result<(), String> {
    let encoded = serde_json::to_string_pretty(manifest)
        .map_err(|error| format!("bead_id={BEAD_ID} case=serialize_manifest error={error}"))?;
    fs::write(path, format!("{encoded}\n")).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=write_manifest path={} error={error}",
            path.display()
        )
    })
}

fn read_manifest(path: &Path) -> Result<LeapfrogGoldenManifest, String> {
    let raw = fs::read_to_string(path).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=read_manifest path={} error={error}",
            path.display()
        )
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=parse_manifest path={} error={error}",
            path.display()
        )
    })
}

fn diff_entries(expected: &LeapfrogGoldenManifest, actual: &LeapfrogGoldenManifest) -> Vec<String> {
    let expected_by_id: BTreeMap<&str, &LeapfrogGoldenEntry> = expected
        .entries
        .iter()
        .map(|entry| (entry.scenario_id.as_str(), entry))
        .collect();
    let actual_by_id: BTreeMap<&str, &LeapfrogGoldenEntry> = actual
        .entries
        .iter()
        .map(|entry| (entry.scenario_id.as_str(), entry))
        .collect();

    let mut scenario_ids = BTreeSet::new();
    scenario_ids.extend(expected_by_id.keys().copied());
    scenario_ids.extend(actual_by_id.keys().copied());

    let mut diff_lines = Vec::new();
    for scenario_id in scenario_ids {
        match (
            expected_by_id.get(scenario_id),
            actual_by_id.get(scenario_id),
        ) {
            (Some(expected_entry), Some(actual_entry)) => {
                if expected_entry != actual_entry {
                    diff_lines.push(format!("scenario={scenario_id} changed"));
                    if expected_entry.relation_sizes != actual_entry.relation_sizes {
                        diff_lines.push(format!(
                            "  relation_sizes expected={:?} actual={:?}",
                            expected_entry.relation_sizes, actual_entry.relation_sizes
                        ));
                    }
                    if expected_entry.hash_join_blake3 != actual_entry.hash_join_blake3 {
                        diff_lines.push(format!(
                            "  hash_join_blake3 expected={} actual={}",
                            expected_entry.hash_join_blake3, actual_entry.hash_join_blake3
                        ));
                    }
                    if expected_entry.leapfrog_blake3 != actual_entry.leapfrog_blake3 {
                        diff_lines.push(format!(
                            "  leapfrog_blake3 expected={} actual={}",
                            expected_entry.leapfrog_blake3, actual_entry.leapfrog_blake3
                        ));
                    }
                    if expected_entry.row_count != actual_entry.row_count {
                        diff_lines.push(format!(
                            "  row_count expected={} actual={}",
                            expected_entry.row_count, actual_entry.row_count
                        ));
                    }
                }
            }
            (Some(_), None) => {
                diff_lines.push(format!(
                    "scenario={scenario_id} missing from actual manifest"
                ));
            }
            (None, Some(_)) => {
                diff_lines.push(format!(
                    "scenario={scenario_id} missing from expected manifest"
                ));
            }
            (None, None) => {}
        }
    }

    diff_lines
}

#[test]
fn test_bd_2qr3a_5_leapfrog_golden_checksums() -> Result<(), String> {
    let manifest = compute_manifest()?;
    for entry in &manifest.entries {
        if entry.hash_join_blake3 != entry.leapfrog_blake3 {
            return Err(format!(
                "bead_id={BEAD_ID} case=checksum_divergence scenario={} hash_join={} leapfrog={}",
                entry.scenario_id, entry.hash_join_blake3, entry.leapfrog_blake3
            ));
        }
    }

    let path = manifest_path()?;
    if update_requested() {
        write_manifest(&path, &manifest)?;
        eprintln!(
            "INFO bead_id={BEAD_ID} case=manifest_updated path={} scenarios={}",
            path.display(),
            manifest.entries.len()
        );
        return Ok(());
    }

    if !path.exists() {
        return Err(format!(
            "bead_id={BEAD_ID} case=manifest_missing path={} hint='set {UPDATE_ENV_VAR}=1 to generate'",
            path.display()
        ));
    }

    let expected = read_manifest(&path)?;
    if expected.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "bead_id={BEAD_ID} case=schema_version_mismatch expected={} actual={}",
            SCHEMA_VERSION, expected.schema_version
        ));
    }
    if expected.hash_algorithm != HASH_ALGORITHM {
        return Err(format!(
            "bead_id={BEAD_ID} case=hash_algorithm_mismatch expected={} actual={}",
            HASH_ALGORITHM, expected.hash_algorithm
        ));
    }
    if expected.entries.is_empty() {
        return Err(format!("bead_id={BEAD_ID} case=manifest_empty"));
    }

    let diff = diff_entries(&expected, &manifest);
    if !diff.is_empty() {
        return Err(format!(
            "bead_id={BEAD_ID} case=checksum_mismatch\n{}\nupdate_command='{}=1 cargo test -p fsqlite-harness --test bd_2qr3a_5_leapfrog_golden_checksums'",
            diff.join("\n"),
            UPDATE_ENV_VAR
        ));
    }

    Ok(())
}
