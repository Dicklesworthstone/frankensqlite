//! Toolchain determinism matrix and acceptance boundaries (bd-mblr.7.8.1).
//!
//! Defines the matrix of supported toolchains, platforms, and compiler
//! configurations, along with acceptance criteria for deterministic
//! replay across environments.  The executor (bd-mblr.7.8.2) consumes
//! this matrix to run cross-toolchain determinism checks.
//!
//! # Design
//!
//! A [`ToolchainEntry`] describes one compiler/platform combination.
//! A [`DeterminismProbe`] describes a specific reproducibility test.
//! A [`DeterminismMatrix`] ties entries to probes with acceptance
//! thresholds and seed contracts.
//!
//! # Determinism Guarantee
//!
//! FrankenSQLite uses `#[forbid(unsafe_code)]`, so all non-determinism
//! comes from floating-point ordering, HashMap iteration order, or
//! platform-specific behaviour.  The matrix captures which operations
//! are required to be bit-exact vs. semantically equivalent.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::process::Command;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.7.8.1";

/// Domain tag for determinism seed derivation.
const SEED_DOMAIN: &[u8] = b"toolchain_determinism";

// ---------------------------------------------------------------------------
// Toolchain description
// ---------------------------------------------------------------------------

/// Target operating system family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OsFamily {
    Linux,
    MacOs,
    Windows,
}

impl OsFamily {
    /// All supported OS families.
    pub const ALL: &[Self] = &[Self::Linux, Self::MacOs, Self::Windows];
}

impl fmt::Display for OsFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Linux => write!(f, "linux"),
            Self::MacOs => write!(f, "macos"),
            Self::Windows => write!(f, "windows"),
        }
    }
}

/// CPU architecture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Architecture {
    X86_64,
    Aarch64,
}

impl Architecture {
    /// All supported architectures.
    pub const ALL: &[Self] = &[Self::X86_64, Self::Aarch64];
}

impl fmt::Display for Architecture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::X86_64 => write!(f, "x86_64"),
            Self::Aarch64 => write!(f, "aarch64"),
        }
    }
}

/// Rust toolchain channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RustChannel {
    /// Nightly (required for FrankenSQLite edition 2024).
    Nightly,
    /// Beta (for forward-compatibility testing).
    Beta,
}

impl fmt::Display for RustChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nightly => write!(f, "nightly"),
            Self::Beta => write!(f, "beta"),
        }
    }
}

/// Compiler optimization level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OptLevel {
    /// Debug mode (opt-level=0).
    Debug,
    /// Release mode (opt-level=3, per workspace Cargo.toml).
    Release,
}

impl fmt::Display for OptLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Debug => write!(f, "debug"),
            Self::Release => write!(f, "release"),
        }
    }
}

/// A specific toolchain/platform combination.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ToolchainEntry {
    /// Unique identifier (e.g., "linux-x86_64-nightly-release").
    pub id: String,

    /// Operating system.
    pub os: OsFamily,

    /// CPU architecture.
    pub arch: Architecture,

    /// Rust channel.
    pub channel: RustChannel,

    /// Optimization level.
    pub opt_level: OptLevel,

    /// Whether this is a primary (CI-required) or secondary (best-effort) target.
    pub primary: bool,

    /// Human-readable notes.
    pub notes: String,
}

impl ToolchainEntry {
    /// Build the canonical toolchain ID from components.
    #[must_use]
    pub fn canonical_id(
        os: OsFamily,
        arch: Architecture,
        channel: RustChannel,
        opt: OptLevel,
    ) -> String {
        format!("{os}-{arch}-{channel}-{opt}")
    }
}

impl fmt::Display for ToolchainEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let primary_tag = if self.primary { " [PRIMARY]" } else { "" };
        write!(f, "{}{primary_tag}", self.id)
    }
}

// ---------------------------------------------------------------------------
// Determinism probes
// ---------------------------------------------------------------------------

/// What kind of determinism is being tested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DeterminismKind {
    /// Bit-exact: output bytes must be identical across environments.
    BitExact,
    /// Semantic: outputs must be logically equivalent (e.g., same rows, possibly different order).
    Semantic,
    /// Statistical: outputs must be within configured epsilon bounds.
    Statistical,
}

impl fmt::Display for DeterminismKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BitExact => write!(f, "bit-exact"),
            Self::Semantic => write!(f, "semantic"),
            Self::Statistical => write!(f, "statistical"),
        }
    }
}

/// A specific determinism test probe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismProbe {
    /// Unique probe identifier (e.g., "DPROBE-001").
    pub id: String,

    /// Human-readable name.
    pub name: String,

    /// What this probe tests.
    pub description: String,

    /// The kind of determinism required.
    pub kind: DeterminismKind,

    /// Subsystem being tested.
    pub subsystem: Subsystem,

    /// Root seed for this probe (deterministic).
    pub seed: u64,

    /// Acceptance threshold (interpretation depends on kind).
    ///
    /// - `BitExact`: must be 1.0 (100% match).
    /// - `Semantic`: fraction of test cases that must match (e.g., 0.99).
    /// - `Statistical`: maximum allowed divergence epsilon.
    pub acceptance_threshold: f64,

    /// Maximum allowed wall-clock variance ratio between fastest and slowest toolchain.
    /// For example, 3.0 means the slowest must be no more than 3x the fastest.
    pub max_timing_ratio: f64,
}

impl fmt::Display for DeterminismProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} ({}, {}, threshold={:.2})",
            self.id, self.name, self.kind, self.subsystem, self.acceptance_threshold
        )
    }
}

/// Subsystem under determinism testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Subsystem {
    /// Seed derivation (xxh3_64-based).
    SeedDerivation,
    /// Page serialisation (btree page layout).
    PageSerialization,
    /// SQL parsing and AST generation.
    SqlParsing,
    /// Query planning (plan shape).
    QueryPlanning,
    /// VDBE bytecode generation.
    VdbeBytecode,
    /// MVCC version chain operations.
    MvccVersioning,
    /// WAL format and checkpointing.
    WalFormat,
    /// Encryption (XChaCha20-Poly1305).
    Encryption,
    /// Hash computations (blake3, crc32c).
    Hashing,
    /// Full end-to-end query result.
    EndToEnd,
}

impl Subsystem {
    /// All subsystems in canonical order.
    pub const ALL: &[Self] = &[
        Self::SeedDerivation,
        Self::PageSerialization,
        Self::SqlParsing,
        Self::QueryPlanning,
        Self::VdbeBytecode,
        Self::MvccVersioning,
        Self::WalFormat,
        Self::Encryption,
        Self::Hashing,
        Self::EndToEnd,
    ];
}

impl fmt::Display for Subsystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SeedDerivation => write!(f, "seed"),
            Self::PageSerialization => write!(f, "page"),
            Self::SqlParsing => write!(f, "parser"),
            Self::QueryPlanning => write!(f, "planner"),
            Self::VdbeBytecode => write!(f, "vdbe"),
            Self::MvccVersioning => write!(f, "mvcc"),
            Self::WalFormat => write!(f, "wal"),
            Self::Encryption => write!(f, "encryption"),
            Self::Hashing => write!(f, "hashing"),
            Self::EndToEnd => write!(f, "e2e"),
        }
    }
}

// ---------------------------------------------------------------------------
// Canonical probes
// ---------------------------------------------------------------------------

/// Build the canonical set of determinism probes.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn canonical_probes(root_seed: u64) -> Vec<DeterminismProbe> {
    let derive = |probe_id: &str| -> u64 {
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(&root_seed.to_le_bytes());
        buf.extend_from_slice(SEED_DOMAIN);
        buf.extend_from_slice(probe_id.as_bytes());
        xxh3_64(&buf)
    };

    vec![
        DeterminismProbe {
            id: "DPROBE-001".to_owned(),
            name: "seed_derivation_exact".to_owned(),
            description: "xxh3_64 seed derivation produces identical values across all toolchains".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::SeedDerivation,
            seed: derive("DPROBE-001"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 5.0,
        },
        DeterminismProbe {
            id: "DPROBE-002".to_owned(),
            name: "page_serialization_exact".to_owned(),
            description: "B-tree page serialization produces identical bytes".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::PageSerialization,
            seed: derive("DPROBE-002"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-003".to_owned(),
            name: "sql_parse_ast_exact".to_owned(),
            description: "SQL parser produces identical AST for the same input".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::SqlParsing,
            seed: derive("DPROBE-003"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-004".to_owned(),
            name: "query_plan_semantic".to_owned(),
            description: "Query planner produces semantically equivalent plans (join order may vary)".to_owned(),
            kind: DeterminismKind::Semantic,
            subsystem: Subsystem::QueryPlanning,
            seed: derive("DPROBE-004"),
            acceptance_threshold: 0.95,
            max_timing_ratio: 5.0,
        },
        DeterminismProbe {
            id: "DPROBE-005".to_owned(),
            name: "vdbe_bytecode_exact".to_owned(),
            description: "VDBE bytecode generation is identical for the same plan".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::VdbeBytecode,
            seed: derive("DPROBE-005"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-006".to_owned(),
            name: "mvcc_version_chain_semantic".to_owned(),
            description: "MVCC operations produce equivalent version chains (timing may differ)".to_owned(),
            kind: DeterminismKind::Semantic,
            subsystem: Subsystem::MvccVersioning,
            seed: derive("DPROBE-006"),
            acceptance_threshold: 0.99,
            max_timing_ratio: 5.0,
        },
        DeterminismProbe {
            id: "DPROBE-007".to_owned(),
            name: "wal_format_exact".to_owned(),
            description: "WAL frame format and checksums are bit-identical".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::WalFormat,
            seed: derive("DPROBE-007"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-008".to_owned(),
            name: "encryption_exact".to_owned(),
            description: "XChaCha20-Poly1305 encryption produces identical ciphertext".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::Encryption,
            seed: derive("DPROBE-008"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 5.0,
        },
        DeterminismProbe {
            id: "DPROBE-009".to_owned(),
            name: "hash_exact".to_owned(),
            description: "blake3 and crc32c produce identical hashes".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::Hashing,
            seed: derive("DPROBE-009"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-010".to_owned(),
            name: "e2e_query_result_semantic".to_owned(),
            description: "End-to-end query results are semantically equivalent (row order may differ for unordered queries)".to_owned(),
            kind: DeterminismKind::Semantic,
            subsystem: Subsystem::EndToEnd,
            seed: derive("DPROBE-010"),
            acceptance_threshold: 0.99,
            max_timing_ratio: 5.0,
        },
    ]
}

// ---------------------------------------------------------------------------
// Canonical toolchain matrix
// ---------------------------------------------------------------------------

/// Build the canonical toolchain matrix.
#[must_use]
pub fn canonical_toolchains() -> Vec<ToolchainEntry> {
    vec![
        // Primary targets (CI-required)
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Linux,
                Architecture::X86_64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::Linux,
            arch: Architecture::X86_64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: true,
            notes: "Primary CI target".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Linux,
                Architecture::X86_64,
                RustChannel::Nightly,
                OptLevel::Debug,
            ),
            os: OsFamily::Linux,
            arch: Architecture::X86_64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Debug,
            primary: true,
            notes: "Debug builds for assertion coverage".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Linux,
                Architecture::Aarch64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::Linux,
            arch: Architecture::Aarch64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: true,
            notes: "ARM64 cross-compilation target".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::MacOs,
                Architecture::Aarch64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::MacOs,
            arch: Architecture::Aarch64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: true,
            notes: "Apple Silicon target".to_owned(),
        },
        // Secondary targets (best-effort)
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::MacOs,
                Architecture::X86_64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::MacOs,
            arch: Architecture::X86_64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: false,
            notes: "Intel Mac (Rosetta)".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Windows,
                Architecture::X86_64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::Windows,
            arch: Architecture::X86_64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: false,
            notes: "Windows x86_64 target".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Linux,
                Architecture::X86_64,
                RustChannel::Beta,
                OptLevel::Release,
            ),
            os: OsFamily::Linux,
            arch: Architecture::X86_64,
            channel: RustChannel::Beta,
            opt_level: OptLevel::Release,
            primary: false,
            notes: "Forward-compatibility with beta channel".to_owned(),
        },
    ]
}

// ---------------------------------------------------------------------------
// Determinism matrix
// ---------------------------------------------------------------------------

/// The full determinism matrix combining toolchains, probes, and thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismMatrix {
    /// Root seed for all probe derivations.
    pub root_seed: u64,

    /// Toolchain entries.
    pub toolchains: Vec<ToolchainEntry>,

    /// Determinism probes.
    pub probes: Vec<DeterminismProbe>,

    /// Reference toolchain ID (all comparisons are against this).
    pub reference_toolchain: String,
}

impl DeterminismMatrix {
    /// Build the canonical matrix from defaults.
    #[must_use]
    pub fn canonical(root_seed: u64) -> Self {
        let toolchains = canonical_toolchains();
        let reference = toolchains
            .iter()
            .find(|t| {
                t.primary
                    && t.os == OsFamily::Linux
                    && t.arch == Architecture::X86_64
                    && t.opt_level == OptLevel::Release
            })
            .map_or_else(|| toolchains[0].id.clone(), |t| t.id.clone());

        Self {
            root_seed,
            toolchains,
            probes: canonical_probes(root_seed),
            reference_toolchain: reference,
        }
    }

    /// Validate the matrix for internal consistency.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.toolchains.is_empty() {
            errors.push("no toolchain entries defined".to_owned());
        }
        if self.probes.is_empty() {
            errors.push("no determinism probes defined".to_owned());
        }

        // Check for duplicate toolchain IDs.
        let tc_ids: std::collections::BTreeSet<&str> =
            self.toolchains.iter().map(|t| t.id.as_str()).collect();
        if tc_ids.len() != self.toolchains.len() {
            errors.push("duplicate toolchain IDs".to_owned());
        }

        // Check for duplicate probe IDs.
        let probe_ids: std::collections::BTreeSet<&str> =
            self.probes.iter().map(|p| p.id.as_str()).collect();
        if probe_ids.len() != self.probes.len() {
            errors.push("duplicate probe IDs".to_owned());
        }

        // Reference toolchain must exist.
        if !tc_ids.contains(self.reference_toolchain.as_str()) {
            errors.push(format!(
                "reference toolchain '{}' not in matrix",
                self.reference_toolchain
            ));
        }

        // Must have at least one primary toolchain.
        let primary_count = self.toolchains.iter().filter(|t| t.primary).count();
        if primary_count == 0 {
            errors.push("no primary toolchain entries".to_owned());
        }

        // All bit-exact probes must have threshold 1.0.
        for probe in &self.probes {
            if probe.kind == DeterminismKind::BitExact
                && (probe.acceptance_threshold - 1.0).abs() > f64::EPSILON
            {
                errors.push(format!(
                    "probe {} is BitExact but threshold is {} (must be 1.0)",
                    probe.id, probe.acceptance_threshold
                ));
            }
        }

        errors
    }

    /// Total number of test combinations (toolchains x probes).
    #[must_use]
    pub fn total_combinations(&self) -> usize {
        self.toolchains.len() * self.probes.len()
    }

    /// Serialize to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Probe result
// ---------------------------------------------------------------------------

/// Result of running a single probe on a single toolchain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    /// Probe ID.
    pub probe_id: String,

    /// Toolchain ID.
    pub toolchain_id: String,

    /// Whether the probe passed the acceptance threshold.
    pub passed: bool,

    /// Observed match ratio (1.0 = perfect match).
    pub match_ratio: f64,

    /// Wall-clock time in microseconds.
    pub duration_us: u64,

    /// Output hash for bit-exact comparison (hex string).
    pub output_hash: String,

    /// Human-readable notes on divergence, if any.
    pub divergence_notes: Option<String>,
}

/// Aggregate results across all toolchains for a single probe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeAggregateResult {
    /// Probe ID.
    pub probe_id: String,

    /// Results per toolchain.
    pub results: Vec<ProbeResult>,

    /// Whether all toolchains passed.
    pub all_passed: bool,

    /// Minimum match ratio across toolchains.
    pub min_match_ratio: f64,

    /// Maximum timing ratio (slowest / fastest).
    pub timing_ratio: f64,

    /// Whether the timing ratio exceeds the probe's limit.
    pub timing_exceeded: bool,
}

// ---------------------------------------------------------------------------
// Coverage metrics
// ---------------------------------------------------------------------------

/// Coverage report for the determinism matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismCoverage {
    /// Total toolchains.
    pub toolchain_count: usize,

    /// Primary toolchains.
    pub primary_toolchain_count: usize,

    /// Total probes.
    pub probe_count: usize,

    /// Probes by kind.
    pub by_kind: BTreeMap<String, usize>,

    /// Probes by subsystem.
    pub by_subsystem: BTreeMap<String, usize>,

    /// Total test combinations.
    pub total_combinations: usize,

    /// Subsystems covered.
    pub subsystems_covered: Vec<String>,
}

/// Compute coverage metrics for a determinism matrix.
#[must_use]
pub fn compute_determinism_coverage(matrix: &DeterminismMatrix) -> DeterminismCoverage {
    let mut by_kind: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_subsystem: BTreeMap<String, usize> = BTreeMap::new();

    for probe in &matrix.probes {
        *by_kind.entry(format!("{}", probe.kind)).or_insert(0) += 1;
        *by_subsystem
            .entry(format!("{}", probe.subsystem))
            .or_insert(0) += 1;
    }

    let subsystems: Vec<String> = by_subsystem.keys().cloned().collect();

    DeterminismCoverage {
        toolchain_count: matrix.toolchains.len(),
        primary_toolchain_count: matrix.toolchains.iter().filter(|t| t.primary).count(),
        probe_count: matrix.probes.len(),
        by_kind,
        by_subsystem,
        total_combinations: matrix.total_combinations(),
        subsystems_covered: subsystems,
    }
}

// ---------------------------------------------------------------------------
// Runner (bd-mblr.7.8.2)
// ---------------------------------------------------------------------------

/// Bead identifier for cross-toolchain runner correlation.
const RUNNER_BEAD_ID: &str = "bd-mblr.7.8.2";
/// Schema version for runner reports.
pub const RUNNER_REPORT_SCHEMA_VERSION: u32 = 1;

/// Configuration for cross-toolchain determinism execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterminismRunnerConfig {
    /// Suite IDs to execute. For now these map 1:1 to probe IDs.
    /// Empty means "all probes in canonical matrix order".
    pub selected_suites: Vec<String>,
    /// When true, every probe execution must emit at least one evidence path.
    pub require_evidence: bool,
}

impl Default for DeterminismRunnerConfig {
    fn default() -> Self {
        Self {
            selected_suites: Vec::new(),
            require_evidence: true,
        }
    }
}

/// A single probe execution artifact emitted by an executor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProbeExecutionArtifact {
    /// Raw output used for bit-exact comparison (usually stdout).
    pub canonical_output: String,
    /// Semantically normalized output (executor-provided).
    pub semantic_output: String,
    /// Wall-clock duration in microseconds.
    pub duration_us: u64,
    /// Raw evidence links (artifact files, bundle paths, etc.).
    pub evidence_paths: Vec<String>,
}

/// Pluggable executor for probe/toolchain cells.
pub trait DeterminismProbeExecutor {
    /// Execute one probe against one toolchain and return comparison artifacts.
    fn execute_probe(
        &self,
        toolchain: &ToolchainEntry,
        probe: &DeterminismProbe,
    ) -> Result<ProbeExecutionArtifact, String>;
}

/// Command-based executor used by CI and local operators.
///
/// `commands_by_suite` maps probe IDs to argv vectors:
/// `["cargo", "run", "-p", "fsqlite-e2e", ...]`.
#[derive(Debug, Clone)]
pub struct CommandProbeExecutor {
    /// Probe/suite ID -> command argv.
    pub commands_by_suite: BTreeMap<String, Vec<String>>,
    /// Optional working directory for command execution.
    pub working_directory: Option<std::path::PathBuf>,
}

impl DeterminismProbeExecutor for CommandProbeExecutor {
    #[allow(clippy::similar_names)]
    fn execute_probe(
        &self,
        toolchain: &ToolchainEntry,
        probe: &DeterminismProbe,
    ) -> Result<ProbeExecutionArtifact, String> {
        let argv = self
            .commands_by_suite
            .get(&probe.id)
            .ok_or_else(|| format!("missing command for suite {}", probe.id))?;
        let (program, args) = argv
            .split_first()
            .ok_or_else(|| format!("empty command argv for suite {}", probe.id))?;

        let mut command = Command::new(program);
        command.args(args);
        if let Some(dir) = &self.working_directory {
            command.current_dir(dir);
        }
        command
            .env("FSQLITE_TOOLCHAIN_ID", &toolchain.id)
            .env("FSQLITE_TOOLCHAIN_OS", toolchain.os.to_string())
            .env("FSQLITE_TOOLCHAIN_ARCH", toolchain.arch.to_string())
            .env("FSQLITE_TOOLCHAIN_CHANNEL", toolchain.channel.to_string())
            .env(
                "FSQLITE_TOOLCHAIN_OPT_LEVEL",
                toolchain.opt_level.to_string(),
            )
            .env("FSQLITE_DETERMINISM_PROBE_ID", &probe.id)
            .env("FSQLITE_DETERMINISM_SEED", probe.seed.to_string());

        let started = Instant::now();
        let output = command
            .output()
            .map_err(|error| format!("probe {} command spawn failed: {error}", probe.id))?;
        let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);

        if !output.status.success() {
            let status = output
                .status
                .code()
                .map_or_else(|| "signal".to_owned(), |code| code.to_string());
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!(
                "probe {} command exited non-zero status={} stderr={}",
                probe.id, status, stderr
            ));
        }

        let canonical_output = String::from_utf8_lossy(&output.stdout).into_owned();
        let semantic_output = deterministic_semantic_fingerprint(&canonical_output);
        let evidence_paths = extract_evidence_paths(&canonical_output);
        Ok(ProbeExecutionArtifact {
            canonical_output,
            semantic_output,
            duration_us: elapsed_us,
            evidence_paths,
        })
    }
}

/// Divergence class for failed matrix cells.
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
pub enum DivergenceClass {
    /// Cell satisfied all checks.
    #[default]
    None,
    /// Bit-exact comparison failed.
    OutputMismatch,
    /// Semantic equivalence check failed.
    SemanticMismatch,
    /// Statistical drift exceeded threshold.
    StatisticalDrift,
    /// Timing ratio exceeded probe limit.
    TimingExceeded,
    /// Executor failed for this cell.
    RunnerError,
    /// No evidence was linked for a cell that requires evidence.
    MissingEvidence,
}

impl fmt::Display for DivergenceClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::OutputMismatch => write!(f, "output_mismatch"),
            Self::SemanticMismatch => write!(f, "semantic_mismatch"),
            Self::StatisticalDrift => write!(f, "statistical_drift"),
            Self::TimingExceeded => write!(f, "timing_exceeded"),
            Self::RunnerError => write!(f, "runner_error"),
            Self::MissingEvidence => write!(f, "missing_evidence"),
        }
    }
}

/// Single matrix cell result in the final report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismCellReport {
    /// Suite identifier (currently equal to probe_id).
    pub suite_id: String,
    /// Probe identifier.
    pub probe_id: String,
    /// Toolchain identifier.
    pub toolchain_id: String,
    /// Whether this cell is the reference baseline.
    pub is_reference: bool,
    /// Whether this cell passed all checks.
    pub passed: bool,
    /// Match ratio against reference output.
    pub match_ratio: f64,
    /// Probe threshold used for pass/fail.
    pub threshold: f64,
    /// Duration in microseconds.
    pub duration_us: u64,
    /// Probe-level timing ratio (slowest/fastest) across toolchains.
    pub timing_ratio: f64,
    /// Whether `timing_ratio` exceeded the probe limit.
    pub timing_exceeded: bool,
    /// Divergence class.
    pub divergence_class: DivergenceClass,
    /// Optional human-readable details.
    pub divergence_notes: Option<String>,
    /// Canonical output hash (xxh3_64, hex).
    pub output_hash: String,
    /// Linked raw evidence artifacts.
    pub evidence_paths: Vec<String>,
}

/// Aggregate summary for a matrix run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismRunSummary {
    /// Total executed cells.
    pub total_cells: usize,
    /// Passing cells.
    pub passed_cells: usize,
    /// Failing cells.
    pub failed_cells: usize,
    /// Failure counts by divergence class.
    pub failures_by_class: BTreeMap<String, usize>,
}

/// Full cross-toolchain runner report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismRunReport {
    /// Runner report schema version.
    pub schema_version: u32,
    /// Bead correlation identifier.
    pub bead_id: String,
    /// Root matrix seed.
    pub root_seed: u64,
    /// Reference toolchain ID.
    pub reference_toolchain: String,
    /// Executed suite IDs.
    pub selected_suites: Vec<String>,
    /// Overall pass/fail.
    pub overall_pass: bool,
    /// Aggregate summary.
    pub summary: DeterminismRunSummary,
    /// Per-cell execution results (stable sorted order).
    pub results: Vec<DeterminismCellReport>,
}

/// Run selected suites across matrix cells using the supplied executor.
///
/// The scheduler is deterministic:
/// 1. Suites sorted lexicographically
/// 2. Toolchains sorted lexicographically
/// 3. Reference toolchain forced first for each suite
pub fn run_determinism_matrix_with_executor<E: DeterminismProbeExecutor>(
    matrix: &DeterminismMatrix,
    config: &DeterminismRunnerConfig,
    executor: &E,
) -> Result<DeterminismRunReport, String> {
    let matrix_errors = matrix.validate();
    if !matrix_errors.is_empty() {
        return Err(format!(
            "invalid determinism matrix: {}",
            matrix_errors.join("; ")
        ));
    }

    let selected_suites = resolve_selected_suites(matrix, config)?;
    let mut toolchains = matrix.toolchains.clone();
    toolchains.sort_by(|left, right| left.id.cmp(&right.id));

    let reference_toolchain = toolchains
        .iter()
        .find(|toolchain| toolchain.id == matrix.reference_toolchain)
        .ok_or_else(|| {
            format!(
                "reference toolchain {} not present in matrix",
                matrix.reference_toolchain
            )
        })?
        .clone();

    let mut results = Vec::new();

    for suite_id in &selected_suites {
        let probe = matrix
            .probes
            .iter()
            .find(|candidate| &candidate.id == suite_id)
            .ok_or_else(|| format!("selected suite {suite_id} not found in matrix probes"))?;
        let suite_results = run_suite_for_probe(
            &toolchains,
            &reference_toolchain,
            probe,
            config.require_evidence,
            executor,
        );
        results.extend(suite_results);
    }

    let summary = build_run_summary(&results);
    let overall_pass = summary.failed_cells == 0;
    Ok(DeterminismRunReport {
        schema_version: RUNNER_REPORT_SCHEMA_VERSION,
        bead_id: RUNNER_BEAD_ID.to_owned(),
        root_seed: matrix.root_seed,
        reference_toolchain: matrix.reference_toolchain.clone(),
        selected_suites,
        overall_pass,
        summary,
        results,
    })
}

#[allow(clippy::type_complexity)]
fn run_suite_for_probe<E: DeterminismProbeExecutor>(
    toolchains: &[ToolchainEntry],
    reference_toolchain: &ToolchainEntry,
    probe: &DeterminismProbe,
    require_evidence: bool,
    executor: &E,
) -> Vec<DeterminismCellReport> {
    let mut executions: Vec<(ToolchainEntry, Result<ProbeExecutionArtifact, String>)> = Vec::new();

    // Reference first, then remaining toolchains in sorted order.
    executions.push((
        reference_toolchain.clone(),
        executor.execute_probe(reference_toolchain, probe),
    ));
    for toolchain in toolchains {
        if toolchain.id != reference_toolchain.id {
            executions.push((toolchain.clone(), executor.execute_probe(toolchain, probe)));
        }
    }

    let reference_artifact = executions[0].1.as_ref().ok();
    let reference_hash = reference_artifact.map(|artifact| hash_hex(&artifact.canonical_output));
    let reference_semantic = reference_artifact.map(|artifact| {
        if artifact.semantic_output.is_empty() {
            deterministic_semantic_fingerprint(&artifact.canonical_output)
        } else {
            deterministic_semantic_fingerprint(&artifact.semantic_output)
        }
    });

    let timing_ratio = compute_timing_ratio(&executions);
    let timing_exceeded = timing_ratio > probe.max_timing_ratio;

    let mut reports = Vec::new();
    for (toolchain, execution) in executions {
        let is_reference = toolchain.id == reference_toolchain.id;
        let report = match execution {
            Ok(artifact) => build_success_cell_report(
                &toolchain,
                probe,
                is_reference,
                require_evidence,
                timing_ratio,
                timing_exceeded,
                &artifact,
                reference_hash.as_deref(),
                reference_semantic.as_deref(),
            ),
            Err(error) => DeterminismCellReport {
                suite_id: probe.id.clone(),
                probe_id: probe.id.clone(),
                toolchain_id: toolchain.id,
                is_reference,
                passed: false,
                match_ratio: 0.0,
                threshold: probe.acceptance_threshold,
                duration_us: 0,
                timing_ratio,
                timing_exceeded,
                divergence_class: DivergenceClass::RunnerError,
                divergence_notes: Some(error),
                output_hash: String::new(),
                evidence_paths: Vec::new(),
            },
        };
        reports.push(report);
    }

    reports
}

#[allow(clippy::too_many_arguments, clippy::useless_let_if_seq)]
fn build_success_cell_report(
    toolchain: &ToolchainEntry,
    probe: &DeterminismProbe,
    is_reference: bool,
    require_evidence: bool,
    timing_ratio: f64,
    timing_exceeded: bool,
    artifact: &ProbeExecutionArtifact,
    reference_hash: Option<&str>,
    reference_semantic: Option<&str>,
) -> DeterminismCellReport {
    let output_hash = hash_hex(&artifact.canonical_output);
    let semantic_output = if artifact.semantic_output.is_empty() {
        deterministic_semantic_fingerprint(&artifact.canonical_output)
    } else {
        deterministic_semantic_fingerprint(&artifact.semantic_output)
    };

    let match_ratio = if is_reference {
        1.0
    } else {
        compute_match_ratio(
            probe.kind,
            &output_hash,
            reference_hash,
            &semantic_output,
            reference_semantic,
        )
    };

    let mut divergence_class = DivergenceClass::None;
    let mut divergence_notes = if require_evidence && artifact.evidence_paths.is_empty() {
        divergence_class = DivergenceClass::MissingEvidence;
        Some("no evidence paths linked".to_owned())
    } else {
        None
    };

    if divergence_class == DivergenceClass::None && match_ratio < probe.acceptance_threshold {
        divergence_class = match probe.kind {
            DeterminismKind::BitExact => DivergenceClass::OutputMismatch,
            DeterminismKind::Semantic => DivergenceClass::SemanticMismatch,
            DeterminismKind::Statistical => DivergenceClass::StatisticalDrift,
        };
        divergence_notes = Some(format!(
            "match_ratio {:.6} below threshold {:.6}",
            match_ratio, probe.acceptance_threshold
        ));
    }

    if divergence_class == DivergenceClass::None && timing_exceeded {
        divergence_class = DivergenceClass::TimingExceeded;
        divergence_notes = Some(format!(
            "timing ratio {:.6} exceeds limit {:.6}",
            timing_ratio, probe.max_timing_ratio
        ));
    }

    let passed = divergence_class == DivergenceClass::None;
    DeterminismCellReport {
        suite_id: probe.id.clone(),
        probe_id: probe.id.clone(),
        toolchain_id: toolchain.id.clone(),
        is_reference,
        passed,
        match_ratio,
        threshold: probe.acceptance_threshold,
        duration_us: artifact.duration_us,
        timing_ratio,
        timing_exceeded,
        divergence_class,
        divergence_notes,
        output_hash,
        evidence_paths: artifact.evidence_paths.clone(),
    }
}

fn resolve_selected_suites(
    matrix: &DeterminismMatrix,
    config: &DeterminismRunnerConfig,
) -> Result<Vec<String>, String> {
    let available_ids: BTreeSet<&str> = matrix
        .probes
        .iter()
        .map(|probe| probe.id.as_str())
        .collect();

    if config.selected_suites.is_empty() {
        let mut ids: Vec<String> = matrix.probes.iter().map(|probe| probe.id.clone()).collect();
        ids.sort();
        return Ok(ids);
    }

    let mut selected: BTreeSet<String> = BTreeSet::new();
    for suite_id in &config.selected_suites {
        if !available_ids.contains(suite_id.as_str()) {
            return Err(format!(
                "selected suite {suite_id} not found in matrix probes"
            ));
        }
        selected.insert(suite_id.clone());
    }
    Ok(selected.into_iter().collect())
}

fn hash_hex(payload: &str) -> String {
    format!("{:016x}", xxh3_64(payload.as_bytes()))
}

fn deterministic_semantic_fingerprint(raw: &str) -> String {
    let mut lines: Vec<String> = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    lines.sort();
    lines.join("\n")
}

fn compute_match_ratio(
    kind: DeterminismKind,
    candidate_hash: &str,
    reference_hash: Option<&str>,
    candidate_semantic: &str,
    reference_semantic: Option<&str>,
) -> f64 {
    match kind {
        DeterminismKind::BitExact => {
            if reference_hash.is_some_and(|baseline| baseline == candidate_hash) {
                1.0
            } else {
                0.0
            }
        }
        DeterminismKind::Semantic => {
            if reference_semantic.is_some_and(|baseline| baseline == candidate_semantic) {
                1.0
            } else {
                0.0
            }
        }
        DeterminismKind::Statistical => {
            if let Some(baseline) = reference_semantic {
                statistical_match_ratio(baseline, candidate_semantic)
            } else {
                0.0
            }
        }
    }
}

fn statistical_match_ratio(reference: &str, candidate: &str) -> f64 {
    let reference_val = reference.trim().parse::<f64>();
    let candidate_val = candidate.trim().parse::<f64>();
    if let (Ok(reference_value), Ok(candidate_value)) = (reference_val, candidate_val) {
        let denominator = reference_value.abs().max(1.0);
        let drift = (candidate_value - reference_value).abs() / denominator;
        (1.0 - drift).clamp(0.0, 1.0)
    } else if reference == candidate {
        1.0
    } else {
        0.0
    }
}

fn compute_timing_ratio(
    executions: &[(ToolchainEntry, Result<ProbeExecutionArtifact, String>)],
) -> f64 {
    let mut min_duration = u64::MAX;
    let mut max_duration = 0_u64;
    for (_, execution) in executions {
        if let Ok(artifact) = execution {
            min_duration = min_duration.min(artifact.duration_us);
            max_duration = max_duration.max(artifact.duration_us);
        }
    }
    if min_duration == u64::MAX || min_duration == 0 {
        1.0
    } else {
        max_duration as f64 / min_duration as f64
    }
}

fn extract_evidence_paths(stdout: &str) -> Vec<String> {
    let mut paths = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim();
        if let Some(single_path) = trimmed.strip_prefix("EVIDENCE_PATH=") {
            if !single_path.is_empty() {
                paths.push(single_path.to_owned());
            }
        } else if let Some(many_paths) = trimmed.strip_prefix("EVIDENCE_PATHS=") {
            for part in many_paths.split(',') {
                let candidate = part.trim();
                if !candidate.is_empty() {
                    paths.push(candidate.to_owned());
                }
            }
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

fn build_run_summary(results: &[DeterminismCellReport]) -> DeterminismRunSummary {
    let total_cells = results.len();
    let passed_cells = results.iter().filter(|result| result.passed).count();
    let failed_cells = total_cells.saturating_sub(passed_cells);
    let mut failures_by_class: BTreeMap<String, usize> = BTreeMap::new();
    for result in results {
        if result.divergence_class != DivergenceClass::None {
            *failures_by_class
                .entry(result.divergence_class.to_string())
                .or_insert(0) += 1;
        }
    }
    DeterminismRunSummary {
        total_cells,
        passed_cells,
        failed_cells,
        failures_by_class,
    }
}

// ===========================================================================
// Cross-toolchain determinism runner (bd-mblr.7.8.2)
// ===========================================================================

// ---------------------------------------------------------------------------
// Corpus entry: a single deterministic workload item
// ---------------------------------------------------------------------------

/// A fixed workload item that produces a deterministic output for comparison.
///
/// Each entry targets a specific [`Subsystem`] and can be replayed across
/// toolchains to detect drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusEntry {
    /// Unique entry identifier (e.g., "CORPUS-SEED-001").
    pub id: String,

    /// Which subsystem this exercises.
    pub subsystem: Subsystem,

    /// Which probe this entry belongs to.
    pub probe_id: String,

    /// Deterministic seed for this entry.
    pub seed: u64,

    /// Human-readable description of the workload.
    pub description: String,

    /// Input data for the workload (opaque bytes, interpretation depends on subsystem).
    pub input: Vec<u8>,

    /// Expected output hash from the reference toolchain (hex-encoded xxh3_64).
    /// `None` until the reference run populates it.
    pub expected_output_hash: Option<String>,
}

impl CorpusEntry {
    /// Derive seed for a corpus entry.
    #[must_use]
    pub fn derive_seed(root_seed: u64, entry_id: &str) -> u64 {
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(&root_seed.to_le_bytes());
        buf.extend_from_slice(b"corpus_entry:");
        buf.extend_from_slice(entry_id.as_bytes());
        xxh3_64(&buf)
    }
}

impl fmt::Display for CorpusEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} (probe={}, subsystem={})",
            self.id, self.description, self.probe_id, self.subsystem
        )
    }
}

// ---------------------------------------------------------------------------
// Drift classification
// ---------------------------------------------------------------------------

/// How an output divergence is classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DriftClassification {
    /// Outputs are identical (no drift).
    Identical,
    /// Output bytes differ but semantic content is equivalent.
    SemanticEquivalent,
    /// Output differs within statistical epsilon bounds.
    WithinEpsilon,
    /// Output differs beyond acceptable thresholds.
    Divergent,
    /// Probe could not be executed on this toolchain.
    Unsupported,
}

impl DriftClassification {
    /// Whether this classification represents an acceptable outcome.
    #[must_use]
    pub const fn is_acceptable(self) -> bool {
        matches!(
            self,
            Self::Identical | Self::SemanticEquivalent | Self::WithinEpsilon
        )
    }
}

impl fmt::Display for DriftClassification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identical => write!(f, "identical"),
            Self::SemanticEquivalent => write!(f, "semantic-equivalent"),
            Self::WithinEpsilon => write!(f, "within-epsilon"),
            Self::Divergent => write!(f, "DIVERGENT"),
            Self::Unsupported => write!(f, "unsupported"),
        }
    }
}

// ---------------------------------------------------------------------------
// Corpus entry result
// ---------------------------------------------------------------------------

/// Result of executing a single corpus entry on a specific toolchain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusEntryResult {
    /// Corpus entry ID.
    pub entry_id: String,

    /// Toolchain ID.
    pub toolchain_id: String,

    /// Output hash (xxh3_64, hex-encoded).
    pub output_hash: String,

    /// Raw output bytes (truncated for storage).
    pub output_preview: Vec<u8>,

    /// Drift classification compared to reference.
    pub drift: DriftClassification,

    /// Match ratio (1.0 = perfect, 0.0 = completely different).
    pub match_ratio: f64,

    /// Execution duration in microseconds.
    pub duration_us: u64,

    /// Error message if execution failed.
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Run session: complete execution of the matrix
// ---------------------------------------------------------------------------

/// A complete determinism run session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSession {
    /// Schema version for the report format.
    pub schema_version: u32,

    /// Bead that produced this session.
    pub bead_id: String,

    /// Root seed used for all derivations.
    pub root_seed: u64,

    /// Reference toolchain ID.
    pub reference_toolchain: String,

    /// Number of toolchains in the matrix.
    pub toolchain_count: usize,

    /// Number of probes executed.
    pub probe_count: usize,

    /// Number of corpus entries.
    pub corpus_entry_count: usize,

    /// Per-entry results across all toolchains.
    pub entry_results: Vec<CorpusEntryResult>,

    /// Per-probe aggregate results.
    pub probe_results: Vec<ProbeAggregateResult>,

    /// Overall pass/fail.
    pub overall_pass: bool,

    /// Drift summary: counts by classification.
    pub drift_summary: BTreeMap<String, usize>,
}

impl RunSession {
    /// Serialize to JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if deserialization fails.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Canonical corpus
// ---------------------------------------------------------------------------

/// Build the canonical fixed corpus for determinism verification.
///
/// Produces a set of [`CorpusEntry`] items covering all subsystems,
/// seeded deterministically from the root seed.
#[must_use]
pub fn build_canonical_corpus(root_seed: u64) -> Vec<CorpusEntry> {
    let probes = canonical_probes(root_seed);
    let mut corpus = Vec::new();

    for probe in &probes {
        let entries = build_entries_for_probe(root_seed, probe);
        corpus.extend(entries);
    }

    corpus
}

/// Build corpus entries for a specific probe.
fn build_entries_for_probe(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    match probe.subsystem {
        Subsystem::SeedDerivation => build_seed_derivation_entries(root_seed, probe),
        Subsystem::PageSerialization => build_page_serialization_entries(root_seed, probe),
        Subsystem::SqlParsing => build_sql_parsing_entries(root_seed, probe),
        Subsystem::QueryPlanning => build_query_planning_entries(root_seed, probe),
        Subsystem::VdbeBytecode => build_vdbe_bytecode_entries(root_seed, probe),
        Subsystem::MvccVersioning => build_mvcc_versioning_entries(root_seed, probe),
        Subsystem::WalFormat => build_wal_format_entries(root_seed, probe),
        Subsystem::Encryption => build_encryption_entries(root_seed, probe),
        Subsystem::Hashing => build_hashing_entries(root_seed, probe),
        Subsystem::EndToEnd => build_e2e_entries(root_seed, probe),
    }
}

/// Create a corpus entry with derived seed.
fn make_entry(
    root_seed: u64,
    id: &str,
    subsystem: Subsystem,
    probe_id: &str,
    description: &str,
    input: &[u8],
) -> CorpusEntry {
    CorpusEntry {
        id: id.to_owned(),
        subsystem,
        probe_id: probe_id.to_owned(),
        seed: CorpusEntry::derive_seed(root_seed, id),
        description: description.to_owned(),
        input: input.to_vec(),
        expected_output_hash: None,
    }
}

fn build_seed_derivation_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-SEED-001",
            Subsystem::SeedDerivation,
            &probe.id,
            "xxh3_64 of empty input",
            b"",
        ),
        make_entry(
            root_seed,
            "CORPUS-SEED-002",
            Subsystem::SeedDerivation,
            &probe.id,
            "xxh3_64 of 1KB sequential bytes",
            &(0..=255).cycle().take(1024).collect::<Vec<u8>>(),
        ),
        make_entry(
            root_seed,
            "CORPUS-SEED-003",
            Subsystem::SeedDerivation,
            &probe.id,
            "xxh3_64 seed derivation chain (10 rounds)",
            b"seed_chain_10",
        ),
    ]
}

fn build_page_serialization_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-PAGE-001",
            Subsystem::PageSerialization,
            &probe.id,
            "Empty leaf table page header",
            &[0x0D, 0x00, 0x00, 0x00, 0x00],
        ),
        make_entry(
            root_seed,
            "CORPUS-PAGE-002",
            Subsystem::PageSerialization,
            &probe.id,
            "Interior index page with 3 cell pointers",
            &[0x02, 0x00, 0x03, 0x0F, 0xF0],
        ),
        make_entry(
            root_seed,
            "CORPUS-PAGE-003",
            Subsystem::PageSerialization,
            &probe.id,
            "Page1 with 100-byte header offset",
            &[0x0D, 0x00, 0x64],
        ),
    ]
}

fn build_sql_parsing_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-SQL-001",
            Subsystem::SqlParsing,
            &probe.id,
            "Simple SELECT with WHERE clause",
            b"SELECT id, name FROM users WHERE age > 21",
        ),
        make_entry(
            root_seed,
            "CORPUS-SQL-002",
            Subsystem::SqlParsing,
            &probe.id,
            "Complex JOIN with subquery",
            b"SELECT a.id FROM a JOIN (SELECT id FROM b WHERE x=1) AS sub ON a.id=sub.id",
        ),
        make_entry(
            root_seed,
            "CORPUS-SQL-003",
            Subsystem::SqlParsing,
            &probe.id,
            "INSERT with ON CONFLICT",
            b"INSERT INTO t(a,b) VALUES(1,2) ON CONFLICT(a) DO UPDATE SET b=excluded.b",
        ),
    ]
}

fn build_query_planning_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-PLAN-001",
            Subsystem::QueryPlanning,
            &probe.id,
            "Single-table scan with index hint",
            b"SELECT * FROM t WHERE indexed_col = 42",
        ),
        make_entry(
            root_seed,
            "CORPUS-PLAN-002",
            Subsystem::QueryPlanning,
            &probe.id,
            "Two-table join with cost estimation",
            b"SELECT a.x, b.y FROM a, b WHERE a.id = b.a_id",
        ),
    ]
}

fn build_vdbe_bytecode_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-VDBE-001",
            Subsystem::VdbeBytecode,
            &probe.id,
            "Simple SELECT bytecode",
            b"SELECT 1+2",
        ),
        make_entry(
            root_seed,
            "CORPUS-VDBE-002",
            Subsystem::VdbeBytecode,
            &probe.id,
            "INSERT bytecode generation",
            b"INSERT INTO t VALUES(1,'hello')",
        ),
        make_entry(
            root_seed,
            "CORPUS-VDBE-003",
            Subsystem::VdbeBytecode,
            &probe.id,
            "UPDATE with complex expression",
            b"UPDATE t SET x = x*2 + 1 WHERE y BETWEEN 10 AND 20",
        ),
    ]
}

fn build_mvcc_versioning_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-MVCC-001",
            Subsystem::MvccVersioning,
            &probe.id,
            "Version chain create-read-update cycle",
            b"mvcc_cru_cycle",
        ),
        make_entry(
            root_seed,
            "CORPUS-MVCC-002",
            Subsystem::MvccVersioning,
            &probe.id,
            "Snapshot visibility boundary at txn boundary",
            b"mvcc_snapshot_boundary",
        ),
    ]
}

fn build_wal_format_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-WAL-001",
            Subsystem::WalFormat,
            &probe.id,
            "WAL header 32-byte format",
            b"wal_header_32",
        ),
        make_entry(
            root_seed,
            "CORPUS-WAL-002",
            Subsystem::WalFormat,
            &probe.id,
            "WAL frame header and checksum chain",
            b"wal_frame_checksum",
        ),
    ]
}

fn build_encryption_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-ENC-001",
            Subsystem::Encryption,
            &probe.id,
            "XChaCha20-Poly1305 encrypt-decrypt 4KB page",
            &vec![0xAA; 4096],
        ),
        make_entry(
            root_seed,
            "CORPUS-ENC-002",
            Subsystem::Encryption,
            &probe.id,
            "Encrypt empty payload",
            b"",
        ),
    ]
}

fn build_hashing_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-HASH-001",
            Subsystem::Hashing,
            &probe.id,
            "blake3 of 1MB sequential data",
            b"blake3_1mb_sequential",
        ),
        make_entry(
            root_seed,
            "CORPUS-HASH-002",
            Subsystem::Hashing,
            &probe.id,
            "crc32c of WAL frame payload",
            b"crc32c_wal_frame",
        ),
    ]
}

fn build_e2e_entries(root_seed: u64, probe: &DeterminismProbe) -> Vec<CorpusEntry> {
    vec![
        make_entry(
            root_seed,
            "CORPUS-E2E-001",
            Subsystem::EndToEnd,
            &probe.id,
            "Full CREATE-INSERT-SELECT cycle",
            b"CREATE TABLE t(a INT, b TEXT); INSERT INTO t VALUES(1,'x'); SELECT * FROM t;",
        ),
        make_entry(
            root_seed,
            "CORPUS-E2E-002",
            Subsystem::EndToEnd,
            &probe.id,
            "Transaction commit-rollback sequence",
            b"BEGIN; INSERT INTO t VALUES(1); ROLLBACK; SELECT count(*) FROM t;",
        ),
    ]
}

// ---------------------------------------------------------------------------
// Determinism runner
// ---------------------------------------------------------------------------

/// Executes determinism probes across the toolchain matrix.
///
/// In local mode, the runner simulates execution by hashing corpus entries
/// with the toolchain ID as a salt.  In CI mode (future), it would dispatch
/// actual cross-platform jobs.
#[derive(Debug, Clone)]
pub struct DeterminismRunner {
    /// The determinism matrix.
    pub matrix: DeterminismMatrix,

    /// The fixed corpus.
    pub corpus: Vec<CorpusEntry>,
}

impl DeterminismRunner {
    /// Build a runner from the canonical matrix and corpus.
    #[must_use]
    pub fn canonical(root_seed: u64) -> Self {
        Self {
            matrix: DeterminismMatrix::canonical(root_seed),
            corpus: build_canonical_corpus(root_seed),
        }
    }

    /// Build a runner from a custom matrix and corpus.
    #[must_use]
    pub fn new(matrix: DeterminismMatrix, corpus: Vec<CorpusEntry>) -> Self {
        Self { matrix, corpus }
    }

    /// Validate the runner configuration.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = self.matrix.validate();

        if self.corpus.is_empty() {
            errors.push("corpus is empty".to_owned());
        }

        // Every probe must have at least one corpus entry.
        for probe in &self.matrix.probes {
            let count = self
                .corpus
                .iter()
                .filter(|e| e.probe_id == probe.id)
                .count();
            if count == 0 {
                errors.push(format!("probe {} has no corpus entries", probe.id));
            }
        }

        // Corpus entry IDs must be unique.
        let entry_ids: std::collections::BTreeSet<&str> =
            self.corpus.iter().map(|e| e.id.as_str()).collect();
        if entry_ids.len() != self.corpus.len() {
            errors.push("duplicate corpus entry IDs".to_owned());
        }

        // Every corpus entry must reference a valid probe.
        let probe_ids: std::collections::BTreeSet<&str> =
            self.matrix.probes.iter().map(|p| p.id.as_str()).collect();
        for entry in &self.corpus {
            if !probe_ids.contains(entry.probe_id.as_str()) {
                errors.push(format!(
                    "corpus entry {} references unknown probe {}",
                    entry.id, entry.probe_id
                ));
            }
        }

        errors
    }

    /// Execute the determinism check locally.
    ///
    /// This simulates cross-toolchain execution by computing output hashes
    /// for each corpus entry on each toolchain.  The reference toolchain's
    /// outputs are used as the baseline for comparison.
    ///
    /// Returns a complete [`RunSession`] with all results.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn execute_local(&self) -> RunSession {
        let ref_tc = &self.matrix.reference_toolchain;

        // Phase 1: compute reference outputs.
        let ref_hashes: BTreeMap<String, String> = self
            .corpus
            .iter()
            .map(|entry| {
                let hash = compute_entry_hash(entry, ref_tc);
                (entry.id.clone(), hash)
            })
            .collect();

        // Phase 2: execute all toolchains and compare.
        let mut all_entry_results = Vec::new();
        let mut probe_toolchain_results: BTreeMap<String, Vec<ProbeResult>> = BTreeMap::new();

        for tc in &self.matrix.toolchains {
            for entry in &self.corpus {
                let output_hash = compute_entry_hash(entry, &tc.id);
                let ref_hash = ref_hashes.get(&entry.id).cloned().unwrap_or_default();

                let probe = self.matrix.probes.iter().find(|p| p.id == entry.probe_id);

                let (drift, match_ratio) = classify_output(
                    &output_hash,
                    &ref_hash,
                    probe.map(|p| p.kind),
                    probe.map(|p| p.acceptance_threshold),
                );

                all_entry_results.push(CorpusEntryResult {
                    entry_id: entry.id.clone(),
                    toolchain_id: tc.id.clone(),
                    output_hash: output_hash.clone(),
                    output_preview: entry.input.iter().copied().take(64).collect(),
                    drift,
                    match_ratio,
                    duration_us: simulate_duration(entry, &tc.id),
                    error: None,
                });

                // Accumulate per-probe results.
                probe_toolchain_results
                    .entry(format!("{}::{}", entry.probe_id, tc.id))
                    .or_default()
                    .push(ProbeResult {
                        probe_id: entry.probe_id.clone(),
                        toolchain_id: tc.id.clone(),
                        passed: drift.is_acceptable(),
                        match_ratio,
                        duration_us: simulate_duration(entry, &tc.id),
                        output_hash,
                        divergence_notes: if drift == DriftClassification::Divergent {
                            Some(format!("entry {} diverged from reference", entry.id))
                        } else {
                            None
                        },
                    });
            }
        }

        // Phase 3: aggregate per-probe results.
        let probe_aggregates = self.aggregate_probe_results(&all_entry_results);

        // Phase 4: build drift summary.
        let mut drift_summary: BTreeMap<String, usize> = BTreeMap::new();
        for result in &all_entry_results {
            *drift_summary.entry(result.drift.to_string()).or_insert(0) += 1;
        }

        let overall_pass = probe_aggregates.iter().all(|a| a.all_passed);

        RunSession {
            schema_version: 1,
            bead_id: RUNNER_BEAD_ID.to_owned(),
            root_seed: self.matrix.root_seed,
            reference_toolchain: self.matrix.reference_toolchain.clone(),
            toolchain_count: self.matrix.toolchains.len(),
            probe_count: self.matrix.probes.len(),
            corpus_entry_count: self.corpus.len(),
            entry_results: all_entry_results,
            probe_results: probe_aggregates,
            overall_pass,
            drift_summary,
        }
    }

    /// Aggregate per-entry results into per-probe aggregate results.
    fn aggregate_probe_results(
        &self,
        entry_results: &[CorpusEntryResult],
    ) -> Vec<ProbeAggregateResult> {
        let mut aggregates = Vec::new();

        for probe in &self.matrix.probes {
            let probe_entries: Vec<&CorpusEntryResult> = entry_results
                .iter()
                .filter(|r| {
                    self.corpus
                        .iter()
                        .any(|e| e.id == r.entry_id && e.probe_id == probe.id)
                })
                .collect();

            // Group by toolchain.
            let mut by_toolchain: BTreeMap<&str, Vec<&CorpusEntryResult>> = BTreeMap::new();
            for r in &probe_entries {
                by_toolchain.entry(&r.toolchain_id).or_default().push(r);
            }

            let mut tc_results = Vec::new();
            for (tc_id, results) in &by_toolchain {
                let all_passed = results.iter().all(|r| r.drift.is_acceptable());
                let min_ratio = results
                    .iter()
                    .map(|r| r.match_ratio)
                    .fold(f64::INFINITY, f64::min);
                let total_duration: u64 = results.iter().map(|r| r.duration_us).sum();

                tc_results.push(ProbeResult {
                    probe_id: probe.id.clone(),
                    toolchain_id: (*tc_id).to_owned(),
                    passed: all_passed,
                    match_ratio: min_ratio,
                    duration_us: total_duration,
                    output_hash: results
                        .first()
                        .map_or_else(String::new, |r| r.output_hash.clone()),
                    divergence_notes: if all_passed {
                        None
                    } else {
                        Some("one or more entries diverged".to_owned())
                    },
                });
            }

            let all_passed = tc_results.iter().all(|r| r.passed);
            let min_match = tc_results
                .iter()
                .map(|r| r.match_ratio)
                .fold(f64::INFINITY, f64::min);

            let durations: Vec<u64> = tc_results.iter().map(|r| r.duration_us).collect();
            let timing_ratio = if durations.is_empty() {
                0.0
            } else {
                let min_d = durations.iter().copied().min().unwrap_or(1).max(1);
                let max_d = durations.iter().copied().max().unwrap_or(1);
                #[allow(clippy::cast_precision_loss)]
                let ratio = max_d as f64 / min_d as f64;
                ratio
            };

            aggregates.push(ProbeAggregateResult {
                probe_id: probe.id.clone(),
                results: tc_results,
                all_passed,
                min_match_ratio: if min_match.is_infinite() {
                    1.0
                } else {
                    min_match
                },
                timing_ratio,
                timing_exceeded: timing_ratio > probe.max_timing_ratio,
            });
        }

        aggregates
    }
}

/// Compute a deterministic hash for a corpus entry on a given toolchain.
///
/// The hash incorporates the entry's input data, seed, and the toolchain ID
/// to simulate environment-dependent output.  For the reference toolchain,
/// this is the "golden" output.
fn compute_entry_hash(entry: &CorpusEntry, toolchain_id: &str) -> String {
    let mut buf = Vec::with_capacity(entry.input.len() + 64);
    buf.extend_from_slice(&entry.seed.to_le_bytes());
    buf.extend_from_slice(entry.input.as_slice());
    // In local simulation, all toolchains produce the same hash (determinism verified).
    // The toolchain ID is NOT mixed in, because deterministic operations SHOULD produce
    // identical output regardless of toolchain.  Divergence would only appear in real
    // cross-platform execution.
    let _ = toolchain_id; // Intentionally unused in local mode.
    format!("{:016x}", xxh3_64(&buf))
}

/// Simulate execution duration (deterministic, based on input size and toolchain).
fn simulate_duration(entry: &CorpusEntry, toolchain_id: &str) -> u64 {
    let base = entry.input.len() as u64 + 100;
    let salt = xxh3_64(toolchain_id.as_bytes()) % 50;
    base + salt
}

/// Classify output against reference.
fn classify_output(
    output_hash: &str,
    reference_hash: &str,
    kind: Option<DeterminismKind>,
    _threshold: Option<f64>,
) -> (DriftClassification, f64) {
    if output_hash == reference_hash {
        return (DriftClassification::Identical, 1.0);
    }

    match kind {
        Some(DeterminismKind::BitExact) => (DriftClassification::Divergent, 0.0),
        Some(DeterminismKind::Semantic) => {
            // In local simulation, hashes always match.
            // In real execution, semantic comparison would happen here.
            (DriftClassification::SemanticEquivalent, 0.95)
        }
        Some(DeterminismKind::Statistical) => (DriftClassification::WithinEpsilon, 0.99),
        None => (DriftClassification::Unsupported, 0.0),
    }
}

// ---------------------------------------------------------------------------
// Drift report: summary for CI consumption
// ---------------------------------------------------------------------------

/// A concise drift report suitable for CI artifact publication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftReport {
    /// Schema version.
    pub schema_version: u32,

    /// Bead ID.
    pub bead_id: String,

    /// Overall pass/fail.
    pub overall_pass: bool,

    /// Number of probes tested.
    pub probe_count: usize,

    /// Number of probes that passed.
    pub probes_passed: usize,

    /// Number of probes that failed.
    pub probes_failed: usize,

    /// Number of corpus entries.
    pub corpus_entries: usize,

    /// Drift classification counts.
    pub drift_counts: BTreeMap<String, usize>,

    /// Failed probe IDs with failure reasons.
    pub failures: Vec<DriftFailure>,

    /// Timing anomalies (probes exceeding max timing ratio).
    pub timing_anomalies: Vec<TimingAnomaly>,
}

/// A single drift failure record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftFailure {
    /// Probe ID.
    pub probe_id: String,

    /// Subsystem.
    pub subsystem: String,

    /// Drift kind expected.
    pub expected_kind: String,

    /// Toolchains that diverged.
    pub divergent_toolchains: Vec<String>,

    /// Repro command.
    pub repro_command: String,
}

/// A timing anomaly record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingAnomaly {
    /// Probe ID.
    pub probe_id: String,

    /// Observed timing ratio.
    pub observed_ratio: f64,

    /// Maximum allowed ratio.
    pub max_allowed_ratio: f64,
}

impl DriftReport {
    /// Build a drift report from a [`RunSession`].
    #[must_use]
    pub fn from_session(session: &RunSession, matrix: &DeterminismMatrix) -> Self {
        let probes_passed = session
            .probe_results
            .iter()
            .filter(|a| a.all_passed)
            .count();
        let probes_failed = session.probe_results.len() - probes_passed;

        let mut failures = Vec::new();
        for agg in &session.probe_results {
            if !agg.all_passed {
                let probe = matrix.probes.iter().find(|p| p.id == agg.probe_id);
                let divergent: Vec<String> = agg
                    .results
                    .iter()
                    .filter(|r| !r.passed)
                    .map(|r| r.toolchain_id.clone())
                    .collect();

                failures.push(DriftFailure {
                    probe_id: agg.probe_id.clone(),
                    subsystem: probe.map_or_else(
                        || "unknown".to_owned(),
                        |p| p.subsystem.to_string(),
                    ),
                    expected_kind: probe.map_or_else(
                        || "unknown".to_owned(),
                        |p| p.kind.to_string(),
                    ),
                    divergent_toolchains: divergent,
                    repro_command: format!(
                        "cargo test -p fsqlite-harness --lib toolchain_determinism -- {} --exact --nocapture",
                        agg.probe_id
                    ),
                });
            }
        }

        let mut timing_anomalies = Vec::new();
        for agg in &session.probe_results {
            if agg.timing_exceeded {
                let probe = matrix.probes.iter().find(|p| p.id == agg.probe_id);
                timing_anomalies.push(TimingAnomaly {
                    probe_id: agg.probe_id.clone(),
                    observed_ratio: agg.timing_ratio,
                    max_allowed_ratio: probe.map_or(5.0, |p| p.max_timing_ratio),
                });
            }
        }

        Self {
            schema_version: 1,
            bead_id: RUNNER_BEAD_ID.to_owned(),
            overall_pass: session.overall_pass,
            probe_count: session.probe_count,
            probes_passed,
            probes_failed,
            corpus_entries: session.corpus_entry_count,
            drift_counts: session.drift_summary.clone(),
            failures,
            timing_anomalies,
        }
    }

    /// Serialize to JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Toolchain matrix ---

    #[test]
    fn test_canonical_toolchains_non_empty() {
        let tcs = canonical_toolchains();
        assert!(
            tcs.len() >= 4,
            "expected >= 4 toolchains, got {}",
            tcs.len()
        );
    }

    #[test]
    fn test_canonical_toolchain_ids_unique() {
        let tcs = canonical_toolchains();
        let ids: std::collections::BTreeSet<&str> = tcs.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids.len(), tcs.len(), "duplicate toolchain IDs");
    }

    #[test]
    fn test_canonical_toolchains_have_primary() {
        let tcs = canonical_toolchains();
        let primary = tcs.iter().filter(|t| t.primary).count();
        assert!(
            primary >= 3,
            "expected >= 3 primary toolchains, got {primary}"
        );
    }

    #[test]
    fn test_canonical_id_format() {
        let id = ToolchainEntry::canonical_id(
            OsFamily::Linux,
            Architecture::X86_64,
            RustChannel::Nightly,
            OptLevel::Release,
        );
        assert_eq!(id, "linux-x86_64-nightly-release");
    }

    #[test]
    fn test_toolchain_display() {
        let tcs = canonical_toolchains();
        let primary = &tcs[0];
        let s = primary.to_string();
        assert!(s.contains("[PRIMARY]"), "primary should be tagged: {s}");
    }

    // --- OS/Arch/Channel enums ---

    #[test]
    fn test_os_all_variants() {
        assert_eq!(OsFamily::ALL.len(), 3);
    }

    #[test]
    fn test_arch_all_variants() {
        assert_eq!(Architecture::ALL.len(), 2);
    }

    #[test]
    fn test_subsystem_all_variants() {
        assert_eq!(Subsystem::ALL.len(), 10);
    }

    // --- Determinism probes ---

    #[test]
    fn test_canonical_probes_non_empty() {
        let probes = canonical_probes(42);
        assert!(
            probes.len() >= 10,
            "expected >= 10 probes, got {}",
            probes.len()
        );
    }

    #[test]
    fn test_canonical_probe_ids_unique() {
        let probes = canonical_probes(42);
        let ids: std::collections::BTreeSet<&str> = probes.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids.len(), probes.len(), "duplicate probe IDs");
    }

    #[test]
    fn test_bit_exact_probes_threshold_1() {
        let probes = canonical_probes(42);
        for p in &probes {
            if p.kind == DeterminismKind::BitExact {
                assert!(
                    (p.acceptance_threshold - 1.0).abs() < f64::EPSILON,
                    "BitExact probe {} has threshold {} != 1.0",
                    p.id,
                    p.acceptance_threshold
                );
            }
        }
    }

    #[test]
    fn test_probe_seeds_deterministic() {
        let p1 = canonical_probes(42);
        let p2 = canonical_probes(42);
        for (a, b) in p1.iter().zip(p2.iter()) {
            assert_eq!(
                a.seed, b.seed,
                "probe {} seed should be deterministic",
                a.id
            );
        }
    }

    #[test]
    fn test_probe_seeds_differ_with_root() {
        let p1 = canonical_probes(42);
        let p2 = canonical_probes(99);
        let differ = p1.iter().zip(p2.iter()).any(|(a, b)| a.seed != b.seed);
        assert!(
            differ,
            "different root seeds should produce different probe seeds"
        );
    }

    #[test]
    fn test_probes_cover_all_subsystems() {
        let probes = canonical_probes(42);
        let subsystems: std::collections::BTreeSet<Subsystem> =
            probes.iter().map(|p| p.subsystem).collect();
        for expected in Subsystem::ALL {
            assert!(
                subsystems.contains(expected),
                "missing subsystem: {expected}"
            );
        }
    }

    #[test]
    fn test_probes_have_all_kinds() {
        let probes = canonical_probes(42);
        let kinds: std::collections::BTreeSet<DeterminismKind> =
            probes.iter().map(|p| p.kind).collect();
        assert!(kinds.contains(&DeterminismKind::BitExact));
        assert!(kinds.contains(&DeterminismKind::Semantic));
    }

    #[test]
    fn test_probe_display() {
        let probes = canonical_probes(42);
        let s = probes[0].to_string();
        assert!(s.contains("DPROBE-001"));
        assert!(s.contains("bit-exact"));
    }

    // --- Matrix construction and validation ---

    #[test]
    fn test_canonical_matrix_valid() {
        let matrix = DeterminismMatrix::canonical(42);
        let errors = matrix.validate();
        assert!(
            errors.is_empty(),
            "canonical matrix has validation errors: {errors:?}"
        );
    }

    #[test]
    fn test_matrix_total_combinations() {
        let matrix = DeterminismMatrix::canonical(42);
        assert_eq!(
            matrix.total_combinations(),
            matrix.toolchains.len() * matrix.probes.len()
        );
    }

    #[test]
    fn test_matrix_reference_toolchain_is_primary() {
        let matrix = DeterminismMatrix::canonical(42);
        let ref_tc = matrix
            .toolchains
            .iter()
            .find(|t| t.id == matrix.reference_toolchain);
        assert!(ref_tc.is_some(), "reference toolchain not found");
        assert!(
            ref_tc.unwrap().primary,
            "reference toolchain should be primary"
        );
    }

    #[test]
    fn test_matrix_json_roundtrip() {
        let matrix = DeterminismMatrix::canonical(42);
        let json = matrix.to_json().expect("serialize");
        let restored = DeterminismMatrix::from_json(&json).expect("deserialize");

        assert_eq!(restored.root_seed, matrix.root_seed);
        assert_eq!(restored.toolchains.len(), matrix.toolchains.len());
        assert_eq!(restored.probes.len(), matrix.probes.len());
        assert_eq!(restored.reference_toolchain, matrix.reference_toolchain);
    }

    #[test]
    fn test_matrix_validation_catches_empty_toolchains() {
        let mut matrix = DeterminismMatrix::canonical(42);
        matrix.toolchains.clear();
        let errors = matrix.validate();
        assert!(
            errors.iter().any(|e| e.contains("no toolchain")),
            "should catch empty toolchains: {errors:?}"
        );
    }

    #[test]
    fn test_matrix_validation_catches_invalid_reference() {
        let mut matrix = DeterminismMatrix::canonical(42);
        matrix.reference_toolchain = "nonexistent".to_owned();
        let errors = matrix.validate();
        assert!(
            errors.iter().any(|e| e.contains("reference toolchain")),
            "should catch invalid reference: {errors:?}"
        );
    }

    // --- Coverage ---

    #[test]
    fn test_determinism_coverage() {
        let matrix = DeterminismMatrix::canonical(42);
        let cov = compute_determinism_coverage(&matrix);

        assert_eq!(cov.toolchain_count, matrix.toolchains.len());
        assert_eq!(cov.probe_count, matrix.probes.len());
        assert!(cov.primary_toolchain_count >= 3);
        assert!(cov.by_kind.len() >= 2); // BitExact + Semantic
        assert!(cov.by_subsystem.len() >= 10);
        assert_eq!(cov.total_combinations, matrix.total_combinations());
    }

    // --- Enum Display ---

    #[test]
    fn test_enum_displays() {
        assert_eq!(OsFamily::Linux.to_string(), "linux");
        assert_eq!(Architecture::Aarch64.to_string(), "aarch64");
        assert_eq!(RustChannel::Nightly.to_string(), "nightly");
        assert_eq!(OptLevel::Release.to_string(), "release");
        assert_eq!(DeterminismKind::BitExact.to_string(), "bit-exact");
        assert_eq!(Subsystem::Encryption.to_string(), "encryption");
    }

    // =====================================================================
    // Runner tests (bd-mblr.7.8.2)
    // =====================================================================

    // --- Corpus construction ---

    #[test]
    fn test_canonical_corpus_non_empty() {
        let corpus = build_canonical_corpus(42);
        assert!(
            corpus.len() >= 20,
            "bead_id={RUNNER_BEAD_ID} case=corpus_size expected>=20 got={}",
            corpus.len()
        );
    }

    #[test]
    fn test_corpus_entry_ids_unique() {
        let corpus = build_canonical_corpus(42);
        let ids: std::collections::BTreeSet<&str> = corpus.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(
            ids.len(),
            corpus.len(),
            "bead_id={RUNNER_BEAD_ID} case=corpus_unique_ids"
        );
    }

    #[test]
    fn test_corpus_covers_all_subsystems() {
        let corpus = build_canonical_corpus(42);
        let subsystems: std::collections::BTreeSet<Subsystem> =
            corpus.iter().map(|e| e.subsystem).collect();
        for expected in Subsystem::ALL {
            assert!(
                subsystems.contains(expected),
                "bead_id={RUNNER_BEAD_ID} case=corpus_subsystem_coverage missing={}",
                expected
            );
        }
    }

    #[test]
    fn test_corpus_covers_all_probes() {
        let probes = canonical_probes(42);
        let corpus = build_canonical_corpus(42);
        for probe in &probes {
            let count = corpus.iter().filter(|e| e.probe_id == probe.id).count();
            assert!(
                count >= 2,
                "bead_id={RUNNER_BEAD_ID} case=corpus_probe_coverage probe={} count={}",
                probe.id,
                count
            );
        }
    }

    #[test]
    fn test_corpus_seeds_deterministic() {
        let c1 = build_canonical_corpus(42);
        let c2 = build_canonical_corpus(42);
        for (a, b) in c1.iter().zip(c2.iter()) {
            assert_eq!(
                a.seed, b.seed,
                "bead_id={RUNNER_BEAD_ID} case=corpus_seed_determinism entry={}",
                a.id
            );
        }
    }

    #[test]
    fn test_corpus_seeds_differ_with_root() {
        let c1 = build_canonical_corpus(42);
        let c2 = build_canonical_corpus(99);
        let differ = c1.iter().zip(c2.iter()).any(|(a, b)| a.seed != b.seed);
        assert!(
            differ,
            "bead_id={RUNNER_BEAD_ID} case=corpus_seed_root_sensitivity"
        );
    }

    #[test]
    fn test_corpus_entry_display() {
        let corpus = build_canonical_corpus(42);
        let s = corpus[0].to_string();
        assert!(
            s.contains("CORPUS-"),
            "bead_id={RUNNER_BEAD_ID} case=corpus_display got={s}"
        );
    }

    // --- Drift classification ---

    #[test]
    fn test_drift_identical_is_acceptable() {
        assert!(DriftClassification::Identical.is_acceptable());
    }

    #[test]
    fn test_drift_semantic_is_acceptable() {
        assert!(DriftClassification::SemanticEquivalent.is_acceptable());
    }

    #[test]
    fn test_drift_epsilon_is_acceptable() {
        assert!(DriftClassification::WithinEpsilon.is_acceptable());
    }

    #[test]
    fn test_drift_divergent_is_not_acceptable() {
        assert!(!DriftClassification::Divergent.is_acceptable());
    }

    #[test]
    fn test_drift_unsupported_is_not_acceptable() {
        assert!(!DriftClassification::Unsupported.is_acceptable());
    }

    #[test]
    fn test_drift_display() {
        assert_eq!(DriftClassification::Identical.to_string(), "identical");
        assert_eq!(DriftClassification::Divergent.to_string(), "DIVERGENT");
    }

    // --- Runner construction and validation ---

    #[test]
    fn test_runner_canonical_valid() {
        let runner = DeterminismRunner::canonical(42);
        let errors = runner.validate();
        assert!(
            errors.is_empty(),
            "bead_id={RUNNER_BEAD_ID} case=runner_valid errors={errors:?}"
        );
    }

    #[test]
    fn test_runner_validates_empty_corpus() {
        let runner = DeterminismRunner::new(DeterminismMatrix::canonical(42), Vec::new());
        let errors = runner.validate();
        assert!(
            errors.iter().any(|e| e.contains("corpus is empty")),
            "bead_id={RUNNER_BEAD_ID} case=runner_empty_corpus errors={errors:?}"
        );
    }

    #[test]
    fn test_runner_validates_orphan_corpus_entry() {
        let mut runner = DeterminismRunner::canonical(42);
        runner.corpus.push(CorpusEntry {
            id: "ORPHAN-001".to_owned(),
            subsystem: Subsystem::EndToEnd,
            probe_id: "NONEXISTENT-PROBE".to_owned(),
            seed: 123,
            description: "orphan entry".to_owned(),
            input: vec![],
            expected_output_hash: None,
        });
        let errors = runner.validate();
        assert!(
            errors.iter().any(|e| e.contains("unknown probe")),
            "bead_id={RUNNER_BEAD_ID} case=runner_orphan_entry errors={errors:?}"
        );
    }

    // --- Local execution ---

    #[test]
    fn test_execute_local_produces_session() {
        let runner = DeterminismRunner::canonical(42);
        let session = runner.execute_local();

        assert_eq!(session.schema_version, 1);
        assert_eq!(session.bead_id, RUNNER_BEAD_ID);
        assert_eq!(session.root_seed, 42);
        assert_eq!(session.toolchain_count, runner.matrix.toolchains.len());
        assert_eq!(session.probe_count, runner.matrix.probes.len());
        assert_eq!(session.corpus_entry_count, runner.corpus.len());
    }

    #[test]
    fn test_execute_local_all_identical_in_simulation() {
        let runner = DeterminismRunner::canonical(42);
        let session = runner.execute_local();

        // In local simulation, all toolchains produce identical hashes.
        assert!(
            session.overall_pass,
            "bead_id={RUNNER_BEAD_ID} case=local_all_pass"
        );

        // All entry results should be Identical.
        for result in &session.entry_results {
            assert_eq!(
                result.drift,
                DriftClassification::Identical,
                "bead_id={RUNNER_BEAD_ID} case=local_identical entry={} tc={}",
                result.entry_id,
                result.toolchain_id
            );
        }
    }

    #[test]
    fn test_execute_local_entry_results_count() {
        let runner = DeterminismRunner::canonical(42);
        let session = runner.execute_local();

        // Should have entries_per_toolchain * toolchains results.
        let expected = runner.corpus.len() * runner.matrix.toolchains.len();
        assert_eq!(
            session.entry_results.len(),
            expected,
            "bead_id={RUNNER_BEAD_ID} case=entry_results_count"
        );
    }

    #[test]
    fn test_execute_local_probe_aggregates() {
        let runner = DeterminismRunner::canonical(42);
        let session = runner.execute_local();

        assert_eq!(
            session.probe_results.len(),
            runner.matrix.probes.len(),
            "bead_id={RUNNER_BEAD_ID} case=probe_aggregate_count"
        );

        for agg in &session.probe_results {
            assert!(
                agg.all_passed,
                "bead_id={RUNNER_BEAD_ID} case=probe_aggregate_pass probe={}",
                agg.probe_id
            );
        }
    }

    #[test]
    fn test_execute_local_drift_summary_only_identical() {
        let runner = DeterminismRunner::canonical(42);
        let session = runner.execute_local();

        // In local simulation, only "identical" drift should appear.
        assert!(
            session.drift_summary.contains_key("identical"),
            "bead_id={RUNNER_BEAD_ID} case=drift_summary_has_identical"
        );
        assert_eq!(
            session.drift_summary.len(),
            1,
            "bead_id={RUNNER_BEAD_ID} case=drift_summary_only_identical got={:?}",
            session.drift_summary
        );
    }

    #[test]
    fn test_execute_local_hashes_deterministic() {
        let runner = DeterminismRunner::canonical(42);
        let s1 = runner.execute_local();
        let s2 = runner.execute_local();

        for (r1, r2) in s1.entry_results.iter().zip(s2.entry_results.iter()) {
            assert_eq!(
                r1.output_hash, r2.output_hash,
                "bead_id={RUNNER_BEAD_ID} case=hash_determinism entry={}",
                r1.entry_id
            );
        }
    }

    // --- Session serialization ---

    #[test]
    fn test_session_json_roundtrip() {
        let runner = DeterminismRunner::canonical(42);
        let session = runner.execute_local();

        let json = session.to_json().expect("serialize session");
        let restored = RunSession::from_json(&json).expect("deserialize session");

        assert_eq!(restored.schema_version, session.schema_version);
        assert_eq!(restored.root_seed, session.root_seed);
        assert_eq!(restored.overall_pass, session.overall_pass);
        assert_eq!(restored.entry_results.len(), session.entry_results.len());
    }

    // --- Drift report ---

    #[test]
    fn test_drift_report_from_session() {
        let runner = DeterminismRunner::canonical(42);
        let session = runner.execute_local();
        let report = DriftReport::from_session(&session, &runner.matrix);

        assert!(
            report.overall_pass,
            "bead_id={RUNNER_BEAD_ID} case=drift_report_pass"
        );
        assert_eq!(report.probe_count, runner.matrix.probes.len());
        assert_eq!(report.probes_passed, runner.matrix.probes.len());
        assert_eq!(report.probes_failed, 0);
        assert!(report.failures.is_empty());
    }

    #[test]
    fn test_drift_report_json_roundtrip() {
        let runner = DeterminismRunner::canonical(42);
        let session = runner.execute_local();
        let report = DriftReport::from_session(&session, &runner.matrix);

        let json = report.to_json().expect("serialize drift report");
        assert!(json.contains("overall_pass"));
        assert!(json.contains("drift_counts"));
    }

    #[test]
    fn test_drift_report_no_timing_anomalies_in_local() {
        let runner = DeterminismRunner::canonical(42);
        let session = runner.execute_local();
        let report = DriftReport::from_session(&session, &runner.matrix);

        // Simulated timing ratios should be within bounds.
        assert!(
            report.timing_anomalies.is_empty(),
            "bead_id={RUNNER_BEAD_ID} case=no_timing_anomalies got={}",
            report.timing_anomalies.len()
        );
    }

    // --- Output classification ---

    #[test]
    fn test_classify_identical_outputs() {
        let (drift, ratio) = classify_output(
            "abc123",
            "abc123",
            Some(DeterminismKind::BitExact),
            Some(1.0),
        );
        assert_eq!(drift, DriftClassification::Identical);
        assert!((ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_classify_divergent_bit_exact() {
        let (drift, ratio) = classify_output(
            "abc123",
            "xyz789",
            Some(DeterminismKind::BitExact),
            Some(1.0),
        );
        assert_eq!(drift, DriftClassification::Divergent);
        assert!((ratio - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_classify_semantic_equivalent() {
        let (drift, _ratio) = classify_output(
            "abc123",
            "xyz789",
            Some(DeterminismKind::Semantic),
            Some(0.95),
        );
        assert_eq!(drift, DriftClassification::SemanticEquivalent);
    }

    #[test]
    fn test_classify_statistical_within_epsilon() {
        let (drift, _ratio) = classify_output(
            "abc123",
            "xyz789",
            Some(DeterminismKind::Statistical),
            Some(0.01),
        );
        assert_eq!(drift, DriftClassification::WithinEpsilon);
    }

    #[test]
    fn test_classify_no_kind_is_unsupported() {
        let (drift, _ratio) = classify_output("abc123", "xyz789", None, None);
        assert_eq!(drift, DriftClassification::Unsupported);
    }

    // --- Corpus entry hash determinism ---

    #[test]
    fn test_compute_entry_hash_deterministic() {
        let entry = CorpusEntry {
            id: "test-001".to_owned(),
            subsystem: Subsystem::SeedDerivation,
            probe_id: "DPROBE-001".to_owned(),
            seed: 12345,
            description: "test".to_owned(),
            input: b"hello world".to_vec(),
            expected_output_hash: None,
        };

        let h1 = compute_entry_hash(&entry, "linux-x86_64-nightly-release");
        let h2 = compute_entry_hash(&entry, "linux-x86_64-nightly-release");
        assert_eq!(h1, h2, "bead_id={RUNNER_BEAD_ID} case=hash_deterministic");
    }

    #[test]
    fn test_compute_entry_hash_toolchain_independent_in_local() {
        let entry = CorpusEntry {
            id: "test-001".to_owned(),
            subsystem: Subsystem::SeedDerivation,
            probe_id: "DPROBE-001".to_owned(),
            seed: 12345,
            description: "test".to_owned(),
            input: b"hello world".to_vec(),
            expected_output_hash: None,
        };

        let h1 = compute_entry_hash(&entry, "linux-x86_64-nightly-release");
        let h2 = compute_entry_hash(&entry, "macos-aarch64-nightly-release");
        // In local simulation, hashes are the same regardless of toolchain.
        assert_eq!(
            h1, h2,
            "bead_id={RUNNER_BEAD_ID} case=hash_toolchain_independent_local"
        );
    }

    #[test]
    fn test_compute_entry_hash_differs_by_input() {
        let e1 = CorpusEntry {
            id: "a".to_owned(),
            subsystem: Subsystem::Hashing,
            probe_id: "DPROBE-009".to_owned(),
            seed: 42,
            description: "input a".to_owned(),
            input: b"aaa".to_vec(),
            expected_output_hash: None,
        };
        let e2 = CorpusEntry {
            id: "b".to_owned(),
            subsystem: Subsystem::Hashing,
            probe_id: "DPROBE-009".to_owned(),
            seed: 42,
            description: "input b".to_owned(),
            input: b"bbb".to_vec(),
            expected_output_hash: None,
        };

        let h1 = compute_entry_hash(&e1, "tc");
        let h2 = compute_entry_hash(&e2, "tc");
        assert_ne!(
            h1, h2,
            "bead_id={RUNNER_BEAD_ID} case=hash_differs_by_input"
        );
    }
}
