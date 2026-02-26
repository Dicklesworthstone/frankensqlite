//! Contract + deterministic replay tests for bd-2yqp6.3.1.
//!
//! This test gate enforces:
//! - machine-readable corpus category floors,
//! - feature-id mapping for all in-scope corpus entries,
//! - deterministic shard replay metadata with stable bundle hashes,
//! - deterministic ingestion/normalization/execution replay behavior.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use fsqlite::Connection;
use fsqlite_harness::corpus_ingest::{
    CORPUS_SEED_BASE, CorpusBuilder, generate_seed_corpus, ingest_conformance_fixtures,
};
use fsqlite_types::value::SqliteValue;
use proptest::prelude::*;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const BEAD_ID: &str = "bd-2yqp6.3.1";

#[derive(Debug, Deserialize)]
struct CorpusManifestContract {
    meta: Meta,
    category_floors: Vec<CategoryFloor>,
    entries: Vec<CorpusEntry>,
    shards: Vec<Shard>,
}

#[derive(Debug, Deserialize)]
struct Meta {
    schema_version: String,
    bead_id: String,
    track_id: String,
    sqlite_target: String,
    generated_at: String,
    contract_owner: String,
    root_seed: u64,
}

#[derive(Debug, Deserialize)]
struct CategoryFloor {
    category: String,
    min_entries: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct CorpusEntry {
    entry_id: String,
    title: String,
    category: String,
    source: String,
    in_scope: bool,
    feature_ids: Vec<String>,
    shard_id: String,
    #[serde(default)]
    execution_required: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(clippy::struct_field_names)]
struct Shard {
    shard_id: String,
    scenario_id: String,
    seed: u64,
    run_id_template: String,
    trace_id_template: String,
    replay_command: String,
    entry_ids: Vec<String>,
    bundle_hash: String,
}

fn load_manifest() -> CorpusManifestContract {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../corpus_manifest.toml");
    let content = fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", path.display());
    });
    toml::from_str::<CorpusManifestContract>(&content).unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", path.display());
    })
}

fn feature_id_is_valid(feature_id: &str) -> bool {
    let Some((family, ordinal)) = feature_id
        .strip_prefix("F-")
        .and_then(|tail| tail.split_once('.'))
    else {
        return false;
    };

    !family.is_empty()
        && family.chars().all(|ch| ch.is_ascii_uppercase())
        && ordinal.len() >= 2
        && ordinal.chars().all(|ch| ch.is_ascii_digit())
}

fn sha256_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    format!("{digest:x}")
}

fn canonical_shard_hash(manifest: &CorpusManifestContract, shard: &Shard) -> String {
    let entries_by_id: BTreeMap<String, &CorpusEntry> = manifest
        .entries
        .iter()
        .map(|entry| (entry.entry_id.clone(), entry))
        .collect();

    let mut entry_ids = shard.entry_ids.clone();
    entry_ids.sort();

    let mut payload_entries = Vec::with_capacity(entry_ids.len());
    for entry_id in &entry_ids {
        let entry = entries_by_id.get(entry_id).unwrap_or_else(|| {
            panic!(
                "shard {} references unknown entry_id {}",
                shard.shard_id, entry_id
            )
        });
        let mut feature_ids = entry.feature_ids.clone();
        feature_ids.sort();
        payload_entries.push(serde_json::json!({
            "entry_id": entry.entry_id,
            "category": entry.category,
            "feature_ids": feature_ids,
            "source": entry.source,
            "in_scope": entry.in_scope,
        }));
    }

    let payload = serde_json::json!({
        "schema_version": manifest.meta.schema_version,
        "bead_id": manifest.meta.bead_id,
        "root_seed": manifest.meta.root_seed,
        "shard_id": shard.shard_id,
        "scenario_id": shard.scenario_id,
        "seed": shard.seed,
        "entry_ids": entry_ids,
        "entries": payload_entries,
    });

    sha256_hex(payload.to_string().as_bytes())
}

fn ingest_and_normalize(base_seed: u64) -> fsqlite_harness::corpus_ingest::CorpusManifest {
    let mut builder = CorpusBuilder::new(base_seed);
    let conformance_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance");
    ingest_conformance_fixtures(&conformance_dir, &mut builder).unwrap_or_else(|error| {
        panic!(
            "failed to ingest conformance fixtures from {}: {error}",
            conformance_dir.display()
        )
    });
    generate_seed_corpus(&mut builder);
    builder.build()
}

fn corpus_fingerprint(manifest: &fsqlite_harness::corpus_ingest::CorpusManifest) -> String {
    let mut entries = manifest
        .entries
        .iter()
        .map(|entry| {
            let mut features = entry.taxonomy_features.clone();
            features.sort();
            serde_json::json!({
                "id": entry.id,
                "family": entry.family.to_string(),
                "seed": entry.seed,
                "statements": entry.statements,
                "features": features,
                "content_hash": entry.content_hash(),
            })
        })
        .collect::<Vec<_>>();

    entries.sort_by(|left, right| {
        let left_id = left["id"].as_str().unwrap_or_default();
        let right_id = right["id"].as_str().unwrap_or_default();
        left_id.cmp(right_id)
    });

    let payload = serde_json::json!({
        "base_seed": manifest.base_seed,
        "entries": entries,
        "total_entries": manifest.entries.len(),
        "active_entries": manifest.coverage.active_entries,
        "skipped_entries": manifest.coverage.skipped_entries,
    });

    sha256_hex(payload.to_string().as_bytes())
}

#[derive(Debug, Clone, PartialEq)]
struct ExecutionProbe {
    rows: Vec<Vec<SqliteValue>>,
    aggregate_row: Vec<SqliteValue>,
    pragma_row: Vec<SqliteValue>,
    error_debug: String,
}

fn run_execution_probe(seed: u64) -> ExecutionProbe {
    let conn = Connection::open(":memory:").expect("open in-memory connection");

    conn.execute("CREATE TABLE replay_probe(id INTEGER PRIMARY KEY, amount INTEGER, label TEXT);")
        .expect("create replay_probe");

    let base = i64::try_from(seed % 97).expect("seed modulo fits into i64");
    conn.execute(&format!(
        "INSERT INTO replay_probe VALUES (1, {}, 'alpha');",
        base + 10
    ))
    .expect("insert row 1");
    conn.execute(&format!(
        "INSERT INTO replay_probe VALUES (2, {}, 'beta');",
        base + 20
    ))
    .expect("insert row 2");

    conn.execute("UPDATE replay_probe SET amount = amount + 5 WHERE id = 2;")
        .expect("update row 2");

    let rows = conn
        .query("SELECT id, amount, label FROM replay_probe ORDER BY id;")
        .expect("select deterministic rows")
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();

    let aggregate_row = conn
        .query_row("SELECT COUNT(*), SUM(amount) FROM replay_probe;")
        .expect("select aggregate row")
        .values()
        .to_vec();

    let pragma_row = conn
        .query_row("PRAGMA page_size;")
        .expect("pragma page_size")
        .values()
        .to_vec();

    let error_debug = format!(
        "{:?}",
        conn.query("SELECT FROM broken_sql")
            .expect_err("syntax error path should fail deterministically")
    );

    ExecutionProbe {
        rows,
        aggregate_row,
        pragma_row,
        error_debug,
    }
}

#[test]
fn manifest_meta_is_pinned_to_bd_2yqp6_3_1_contract() {
    let manifest = load_manifest();
    assert_eq!(manifest.meta.schema_version, "1.0.0");
    assert_eq!(manifest.meta.bead_id, BEAD_ID);
    assert_eq!(manifest.meta.track_id, "bd-2yqp6.3");
    assert_eq!(manifest.meta.sqlite_target, "3.52.0");
    assert!(!manifest.meta.generated_at.trim().is_empty());
    assert!(!manifest.meta.contract_owner.trim().is_empty());
    assert!(manifest.meta.root_seed > 0);
}

#[test]
fn category_floors_are_satisfied() {
    let manifest = load_manifest();
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();

    for entry in &manifest.entries {
        if entry.in_scope {
            counts
                .entry(entry.category.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }

    let mut floor_categories = BTreeSet::new();
    for floor in &manifest.category_floors {
        assert!(
            floor_categories.insert(floor.category.as_str()),
            "duplicate category floor: {}",
            floor.category
        );
        let observed = counts.get(&floor.category).copied().unwrap_or(0);
        assert!(
            observed >= floor.min_entries,
            "category floor miss for {}: observed={}, required={}",
            floor.category,
            observed,
            floor.min_entries
        );
    }
}

#[test]
fn entries_are_mapped_to_feature_ids_and_shards() {
    let manifest = load_manifest();

    let shard_ids: BTreeSet<String> = manifest
        .shards
        .iter()
        .map(|shard| shard.shard_id.clone())
        .collect();

    let floor_categories: BTreeSet<String> = manifest
        .category_floors
        .iter()
        .map(|floor| floor.category.clone())
        .collect();

    let mut entry_ids = BTreeSet::new();
    for entry in &manifest.entries {
        assert!(
            entry_ids.insert(entry.entry_id.as_str()),
            "duplicate entry_id: {}",
            entry.entry_id
        );
        assert!(
            !entry.title.trim().is_empty(),
            "empty title for {}",
            entry.entry_id
        );
        assert!(
            floor_categories.contains(&entry.category),
            "entry {} references unknown category {}",
            entry.entry_id,
            entry.category
        );
        assert!(
            shard_ids.contains(&entry.shard_id),
            "entry {} references unknown shard {}",
            entry.entry_id,
            entry.shard_id
        );
        if entry.in_scope {
            assert!(
                !entry.feature_ids.is_empty(),
                "in-scope entry {} must map to feature_ids",
                entry.entry_id
            );
            for feature_id in &entry.feature_ids {
                assert!(
                    feature_id_is_valid(feature_id),
                    "invalid feature_id {} on {}",
                    feature_id,
                    entry.entry_id
                );
            }
        }
    }
}

#[test]
fn shard_replay_metadata_is_complete_and_hashes_match() {
    let manifest = load_manifest();

    let entries_by_id: BTreeMap<String, &CorpusEntry> = manifest
        .entries
        .iter()
        .map(|entry| (entry.entry_id.clone(), entry))
        .collect();

    let mut shard_seen = BTreeSet::new();
    let mut covered_entries = BTreeSet::new();

    for shard in &manifest.shards {
        assert!(
            shard_seen.insert(shard.shard_id.as_str()),
            "duplicate shard_id: {}",
            shard.shard_id
        );
        assert!(!shard.scenario_id.trim().is_empty());
        assert!(shard.seed > 0);
        assert!(shard.run_id_template.contains("{seed}"));
        assert!(shard.trace_id_template.contains("{seed}"));
        assert!(
            shard
                .replay_command
                .contains("verify_bd_2yqp6_3_1_corpus_manifest.sh"),
            "replay command missing verifier script for shard {}",
            shard.shard_id
        );

        let mut shard_entry_ids = BTreeSet::new();
        for entry_id in &shard.entry_ids {
            assert!(
                shard_entry_ids.insert(entry_id.as_str()),
                "shard {} has duplicate entry id {}",
                shard.shard_id,
                entry_id
            );
            let entry = entries_by_id.get(entry_id).unwrap_or_else(|| {
                panic!(
                    "shard {} references unknown entry id {}",
                    shard.shard_id, entry_id
                )
            });
            assert_eq!(
                entry.shard_id, shard.shard_id,
                "entry {} shard mismatch: {} != {}",
                entry.entry_id, entry.shard_id, shard.shard_id
            );
            covered_entries.insert(entry.entry_id.as_str());
        }

        assert_eq!(
            shard.bundle_hash,
            canonical_shard_hash(&manifest, shard),
            "stable bundle hash mismatch for shard {}",
            shard.shard_id
        );
    }

    let in_scope_entry_ids: BTreeSet<&str> = manifest
        .entries
        .iter()
        .filter(|entry| entry.in_scope)
        .map(|entry| entry.entry_id.as_str())
        .collect();

    assert_eq!(
        covered_entries, in_scope_entry_ids,
        "shards must cover all in-scope entries exactly"
    );
}

#[test]
fn ingestion_and_normalization_replay_are_deterministic() {
    let first = ingest_and_normalize(CORPUS_SEED_BASE);
    let second = ingest_and_normalize(CORPUS_SEED_BASE);

    let hash_a = corpus_fingerprint(&first);
    let hash_b = corpus_fingerprint(&second);
    assert_eq!(hash_a, hash_b, "ingest/normalize fingerprint drifted");

    let third = ingest_and_normalize(CORPUS_SEED_BASE.wrapping_add(1));
    let hash_c = corpus_fingerprint(&third);
    assert_ne!(
        hash_a, hash_c,
        "changing base seed must change normalized corpus fingerprint"
    );
}

#[test]
fn execution_replay_probe_is_deterministic() {
    let manifest = load_manifest();
    let probe_a = run_execution_probe(manifest.meta.root_seed);
    let probe_b = run_execution_probe(manifest.meta.root_seed);
    assert_eq!(probe_a, probe_b, "execution replay probe drifted");

    let required_exec_entries = manifest
        .entries
        .iter()
        .filter(|entry| entry.in_scope && entry.execution_required)
        .count();
    assert!(
        required_exec_entries >= 8,
        "expected at least 8 execution-required entries, got {}",
        required_exec_entries
    );
}

proptest! {
    #[test]
    fn shard_hash_changes_with_seed(mutated_seed in 1_u64..u64::MAX) {
        let manifest = load_manifest();
        let base_shard = manifest
            .shards
            .first()
            .unwrap_or_else(|| panic!("manifest must contain at least one shard"));

        prop_assume!(mutated_seed != base_shard.seed);

        let mut mutated_shard = base_shard.clone();
        mutated_shard.seed = mutated_seed;
        let original_hash = canonical_shard_hash(&manifest, base_shard);
        let mutated_hash = canonical_shard_hash(&manifest, &mutated_shard);

        prop_assert_ne!(original_hash, mutated_hash);
    }
}
