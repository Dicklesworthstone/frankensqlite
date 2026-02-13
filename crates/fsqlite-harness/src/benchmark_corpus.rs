//! Concurrency/recovery benchmark corpus (bd-mblr.7.3.1).
//!
//! This module curates a deterministic benchmark corpus spanning:
//! - write contention hot paths (conflict-heavy concurrent writers),
//! - recovery behavior,
//! - checkpoint behavior,
//! - representative SQL/operator mixes.
//!
//! The corpus includes both micro-style and macro-style benchmark lanes and
//! records deterministic dataset/warmup recipes for reproducibility.

use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Bead identifier for log/assert correlation.
pub const BEAD_ID: &str = "bd-mblr.7.3.1";
/// Schema version for benchmark corpus serialization.
pub const CORPUS_SCHEMA_VERSION: u32 = 1;
/// Default deterministic root seed for corpus generation.
pub const DEFAULT_ROOT_SEED: u64 = 0xB731_71A0_0000_0001;

/// Benchmark granularity tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BenchmarkTier {
    /// Focused benchmark for page-level or primitive-level behavior.
    Micro,
    /// End-to-end benchmark using integrated workload mixes.
    Macro,
}

impl fmt::Display for BenchmarkTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Micro => f.write_str("micro"),
            Self::Macro => f.write_str("macro"),
        }
    }
}

/// Benchmark behavior family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BenchmarkFamily {
    WriteContention,
    Recovery,
    Checkpoint,
    SqlOperatorMix,
}

impl fmt::Display for BenchmarkFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteContention => f.write_str("write-contention"),
            Self::Recovery => f.write_str("recovery"),
            Self::Checkpoint => f.write_str("checkpoint"),
            Self::SqlOperatorMix => f.write_str("sql-operator-mix"),
        }
    }
}

/// Deterministic dataset generation and warmup recipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetRecipe {
    /// Dataset seed derived from `root_seed` + entry ID.
    pub seed: u64,
    /// Number of tables generated for the benchmark.
    pub table_count: u16,
    /// Rows generated per table.
    pub rows_per_table: u32,
    /// Page size used by the lane.
    pub page_size: u32,
    /// Warmup iterations before taking measurements.
    pub warmup_iterations: u16,
    /// Connection fanout for this workload.
    pub connection_count: u16,
    /// Checkpoint interval hint (transactions), if applicable.
    pub checkpoint_interval: Option<u32>,
}

/// One benchmark corpus entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkCorpusEntry {
    pub id: String,
    pub title: String,
    pub tier: BenchmarkTier,
    pub family: BenchmarkFamily,
    pub command: String,
    pub scenario_ids: Vec<String>,
    pub tags: Vec<String>,
    pub conflict_heavy: bool,
    pub dataset: DatasetRecipe,
}

/// Serialized benchmark corpus manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkCorpusManifest {
    pub bead_id: String,
    pub schema_version: u32,
    pub root_seed: u64,
    pub dataset_policy: String,
    pub warmup_policy: String,
    pub entries: Vec<BenchmarkCorpusEntry>,
}

struct EntrySpec<'a> {
    id: &'a str,
    title: &'a str,
    tier: BenchmarkTier,
    family: BenchmarkFamily,
    command: &'a str,
    scenario_ids: &'a [&'a str],
    tags: &'a [&'a str],
    base_rows: u32,
    connection_count: u16,
    checkpoint_interval: Option<u32>,
    conflict_heavy: bool,
}

/// Build the canonical deterministic benchmark corpus.
#[must_use]
pub fn build_benchmark_corpus(root_seed: u64) -> BenchmarkCorpusManifest {
    let specs = canonical_entry_specs();
    let mut entries = Vec::with_capacity(specs.len());
    for (index, spec) in specs.iter().enumerate() {
        let ordinal = u64::try_from(index + 1).unwrap_or(u64::MAX);
        entries.push(build_entry(root_seed, ordinal, spec));
    }
    entries.sort_by(|left, right| left.id.cmp(&right.id));

    BenchmarkCorpusManifest {
        bead_id: BEAD_ID.to_owned(),
        schema_version: CORPUS_SCHEMA_VERSION,
        root_seed,
        dataset_policy:
            "dataset_seed = sha256(root_seed || entry_id || ordinal); stable table/row/page shape"
                .to_owned(),
        warmup_policy:
            "micro: 5..7 iterations, macro: 10..12 iterations; deterministic from dataset seed"
                .to_owned(),
        entries,
    }
}

/// Build and validate the canonical corpus.
pub fn build_validated_benchmark_corpus(root_seed: u64) -> Result<BenchmarkCorpusManifest, String> {
    let corpus = build_benchmark_corpus(root_seed);
    let errors = validate_benchmark_corpus(&corpus);
    if errors.is_empty() {
        Ok(corpus)
    } else {
        Err(errors.join("; "))
    }
}

/// Validate benchmark corpus shape and deterministic coverage guarantees.
#[must_use]
pub fn validate_benchmark_corpus(corpus: &BenchmarkCorpusManifest) -> Vec<String> {
    let mut errors = Vec::new();
    if corpus.bead_id != BEAD_ID {
        errors.push(format!(
            "unexpected bead_id: {} (expected {BEAD_ID})",
            corpus.bead_id
        ));
    }
    if corpus.schema_version != CORPUS_SCHEMA_VERSION {
        errors.push(format!(
            "unexpected schema_version: {} (expected {CORPUS_SCHEMA_VERSION})",
            corpus.schema_version
        ));
    }
    if corpus.entries.is_empty() {
        errors.push("benchmark corpus must contain entries".to_owned());
        return errors;
    }

    let mut ids = BTreeSet::new();
    let mut tiers = BTreeSet::new();
    let mut families = BTreeSet::new();
    let mut has_page_primitive_micro = false;
    let mut has_query_mix_macro = false;
    let mut has_conflict_heavy_write = false;
    let mut previous_id: Option<&str> = None;

    for entry in &corpus.entries {
        if !ids.insert(entry.id.as_str()) {
            errors.push(format!("duplicate entry id: {}", entry.id));
        }
        if let Some(previous) = previous_id {
            if previous > entry.id.as_str() {
                errors.push("entries must be sorted by id".to_owned());
            }
        }
        previous_id = Some(&entry.id);
        tiers.insert(entry.tier);
        families.insert(entry.family);

        if entry.tier == BenchmarkTier::Micro
            && entry.tags.iter().any(|tag| tag == "page-primitive")
        {
            has_page_primitive_micro = true;
        }
        if entry.tier == BenchmarkTier::Macro && entry.tags.iter().any(|tag| tag == "query-mix") {
            has_query_mix_macro = true;
        }
        if entry.family == BenchmarkFamily::WriteContention && entry.conflict_heavy {
            has_conflict_heavy_write = true;
        }
        validate_entry(entry, &mut errors);
    }

    if !tiers.contains(&BenchmarkTier::Micro) || !tiers.contains(&BenchmarkTier::Macro) {
        errors.push("corpus must include both micro and macro entries".to_owned());
    }

    let required_families = [
        BenchmarkFamily::WriteContention,
        BenchmarkFamily::Recovery,
        BenchmarkFamily::Checkpoint,
        BenchmarkFamily::SqlOperatorMix,
    ];
    for family in required_families {
        if !families.contains(&family) {
            errors.push(format!("missing benchmark family: {family}"));
        }
    }

    if !has_page_primitive_micro {
        errors.push("missing micro page-primitive benchmark entry".to_owned());
    }
    if !has_query_mix_macro {
        errors.push("missing macro query-mix benchmark entry".to_owned());
    }
    if !has_conflict_heavy_write {
        errors.push("missing conflict-heavy write-contention benchmark entry".to_owned());
    }

    errors
}

/// Render an operator workflow showing deterministic run commands per entry.
#[must_use]
pub fn render_operator_workflow(corpus: &BenchmarkCorpusManifest) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "benchmark_corpus bead_id={} schema={} root_seed={}",
        corpus.bead_id, corpus.schema_version, corpus.root_seed
    ));
    lines.push(format!("dataset_policy={}", corpus.dataset_policy));
    lines.push(format!("warmup_policy={}", corpus.warmup_policy));
    lines.push("entries:".to_owned());
    for entry in &corpus.entries {
        lines.push(format!(
            "- id={} tier={} family={} warmup={} conn={} rows/table={} seed={} cmd={}",
            entry.id,
            entry.tier,
            entry.family,
            entry.dataset.warmup_iterations,
            entry.dataset.connection_count,
            entry.dataset.rows_per_table,
            entry.dataset.seed,
            entry.command
        ));
    }
    lines.join("\n")
}

/// Serialize a corpus to pretty JSON.
pub fn write_corpus_json(path: &Path, corpus: &BenchmarkCorpusManifest) -> Result<(), String> {
    let payload = serde_json::to_vec_pretty(corpus)
        .map_err(|error| format!("benchmark_corpus_json_serialize_failed: {error}"))?;
    std::fs::write(path, payload).map_err(|error| {
        format!(
            "benchmark_corpus_write_failed path={} error={error}",
            path.display()
        )
    })
}

fn validate_entry(entry: &BenchmarkCorpusEntry, errors: &mut Vec<String>) {
    if entry.id.trim().is_empty() {
        errors.push("entry id must not be empty".to_owned());
    }
    if entry.command.trim().is_empty() || !entry.command.starts_with("cargo ") {
        errors.push(format!("entry {} has invalid command", entry.id));
    }
    if entry.scenario_ids.is_empty() {
        errors.push(format!("entry {} must include scenario_ids", entry.id));
    }
    if entry.dataset.table_count == 0 {
        errors.push(format!("entry {} has zero table_count", entry.id));
    }
    if entry.dataset.rows_per_table == 0 {
        errors.push(format!("entry {} has zero rows_per_table", entry.id));
    }
    if entry.dataset.warmup_iterations == 0 {
        errors.push(format!("entry {} has zero warmup_iterations", entry.id));
    }
    if entry.dataset.connection_count == 0 {
        errors.push(format!("entry {} has zero connection_count", entry.id));
    }
}

fn build_entry(root_seed: u64, ordinal: u64, spec: &EntrySpec<'_>) -> BenchmarkCorpusEntry {
    let dataset_seed = derive_entry_seed(root_seed, spec.id, ordinal);
    let warmup_base = match spec.tier {
        BenchmarkTier::Micro => 5_u16,
        BenchmarkTier::Macro => 10_u16,
    };
    let warmup_bump = match dataset_seed % 3 {
        0 => 0_u16,
        1 => 1_u16,
        _ => 2_u16,
    };
    let row_jitter = match u16::try_from(dataset_seed % 257) {
        Ok(value) => u32::from(value),
        Err(_) => 0,
    };
    let table_count = match spec.tier {
        BenchmarkTier::Micro => 4_u16,
        BenchmarkTier::Macro => 12_u16,
    };
    let page_size = if dataset_seed & 1 == 0 {
        4_096_u32
    } else {
        8_192_u32
    };

    BenchmarkCorpusEntry {
        id: spec.id.to_owned(),
        title: spec.title.to_owned(),
        tier: spec.tier,
        family: spec.family,
        command: spec.command.to_owned(),
        scenario_ids: spec
            .scenario_ids
            .iter()
            .map(|id| (*id).to_owned())
            .collect(),
        tags: spec.tags.iter().map(|tag| (*tag).to_owned()).collect(),
        conflict_heavy: spec.conflict_heavy,
        dataset: DatasetRecipe {
            seed: dataset_seed,
            table_count,
            rows_per_table: spec.base_rows + row_jitter,
            page_size,
            warmup_iterations: warmup_base + warmup_bump,
            connection_count: spec.connection_count,
            checkpoint_interval: spec.checkpoint_interval,
        },
    }
}

fn derive_entry_seed(root_seed: u64, entry_id: &str, ordinal: u64) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(root_seed.to_le_bytes());
    hasher.update(ordinal.to_le_bytes());
    hasher.update(entry_id.as_bytes());
    hasher.update(BEAD_ID.as_bytes());
    let digest = hasher.finalize();

    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_le_bytes(bytes)
}

fn canonical_entry_specs() -> Vec<EntrySpec<'static>> {
    vec![
        EntrySpec {
            id: "bm-checkpoint-plan-micro",
            title: "Checkpoint planner primitive path",
            tier: BenchmarkTier::Micro,
            family: BenchmarkFamily::Checkpoint,
            command: "cargo test -p fsqlite-harness checkpoint_full_blocked_by_readers -- --nocapture",
            scenario_ids: &["INFRA-2", "PARITY-1"],
            tags: &["page-primitive", "checkpoint", "sanity"],
            base_rows: 2_048,
            connection_count: 4,
            checkpoint_interval: Some(64),
            conflict_heavy: false,
        },
        EntrySpec {
            id: "bm-mvcc-page-conflict-micro",
            title: "Conflict-heavy concurrent writer primitive path",
            tier: BenchmarkTier::Micro,
            family: BenchmarkFamily::WriteContention,
            command: "cargo test -p fsqlite-harness --test bd_2npr_mvcc_concurrent_writer_stress -- --nocapture",
            scenario_ids: &["CONC-1", "CONC-2"],
            tags: &["page-primitive", "conflict-heavy", "mvcc"],
            base_rows: 4_096,
            connection_count: 32,
            checkpoint_interval: Some(128),
            conflict_heavy: true,
        },
        EntrySpec {
            id: "bm-wal-checksum-recovery-micro",
            title: "WAL checksum chain recovery primitive path",
            tier: BenchmarkTier::Micro,
            family: BenchmarkFamily::Recovery,
            command: "cargo test -p fsqlite-harness --test bd_2fas_wal_checksum_chain_recovery_compliance -- --nocapture",
            scenario_ids: &["REC-1", "REC-2"],
            tags: &["page-primitive", "recovery", "wal"],
            base_rows: 2_560,
            connection_count: 8,
            checkpoint_interval: Some(96),
            conflict_heavy: false,
        },
        EntrySpec {
            id: "bm-checkpoint-transaction-mix-macro",
            title: "Transaction/checkpoint macro mix",
            tier: BenchmarkTier::Macro,
            family: BenchmarkFamily::Checkpoint,
            command: "cargo test -p fsqlite-e2e --test correctness_transactions -- --nocapture",
            scenario_ids: &["PARITY-1", "PARITY-2"],
            tags: &["query-mix", "checkpoint", "transaction"],
            base_rows: 20_000,
            connection_count: 24,
            checkpoint_interval: Some(512),
            conflict_heavy: false,
        },
        EntrySpec {
            id: "bm-recovery-crash-replay-macro",
            title: "Crash replay recovery macro lane",
            tier: BenchmarkTier::Macro,
            family: BenchmarkFamily::Recovery,
            command: "cargo test -p fsqlite-e2e --test recovery_crash_wal_replay -- --nocapture",
            scenario_ids: &["INFRA-3", "REC-4"],
            tags: &["query-mix", "recovery", "crash-replay"],
            base_rows: 24_000,
            connection_count: 16,
            checkpoint_interval: Some(768),
            conflict_heavy: false,
        },
        EntrySpec {
            id: "bm-sql-operator-mix-macro",
            title: "Representative SQL/operator macro mix",
            tier: BenchmarkTier::Macro,
            family: BenchmarkFamily::SqlOperatorMix,
            command: "cargo run -p fsqlite-harness --bin e2e_full_suite_runner -- --execute --root-seed 424242",
            scenario_ids: &["PARITY-1", "PARITY-2", "PARITY-3"],
            tags: &["query-mix", "sql-operator", "e2e"],
            base_rows: 32_000,
            connection_count: 20,
            checkpoint_interval: Some(1_024),
            conflict_heavy: false,
        },
        EntrySpec {
            id: "bm-write-contention-macro",
            title: "Conflict-heavy concurrent writer macro lane",
            tier: BenchmarkTier::Macro,
            family: BenchmarkFamily::WriteContention,
            command: "cargo test -p fsqlite-harness --test bd_yvhd_ssi_perf_validation_compliance -- --nocapture",
            scenario_ids: &["CONC-4", "CONC-5"],
            tags: &["query-mix", "conflict-heavy", "ssi"],
            base_rows: 28_000,
            connection_count: 64,
            checkpoint_interval: Some(2_048),
            conflict_heavy: true,
        },
    ]
}
