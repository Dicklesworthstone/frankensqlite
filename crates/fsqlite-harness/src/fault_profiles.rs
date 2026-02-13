//! Deterministic fault profile suite (`bd-mblr.2.3.1`).
//!
//! Named, categorized fault profiles that compose [`FaultSpec`](crate::fault_vfs::FaultSpec)
//! sequences and bind them to expected invariant outcomes. Profiles are deterministic:
//! same profile + same seed → same fault sequence → same expected behavior.
//!
//! # Architecture
//!
//! ```text
//!  FaultProfileCatalog
//!    └── FaultProfile (named, categorized)
//!          ├── generate_specs(seed) → Vec<FaultSpec>
//!          └── expected_behavior()  → ExpectedBehavior
//!                ├── invariants_preserved: Vec<MvccInvariant>
//!                ├── committed_data_preserved: bool
//!                └── ...
//! ```
//!
//! # E2E Adoption Checklist (F-1..F-8)
//!
//! - **F-1**: Every fault profile has a unique stable ID (`FP-xxx`).
//! - **F-2**: Profile IDs are referenced in test assertions for traceability.
//! - **F-3**: `generate_specs()` is pure (no side effects, deterministic from seed).
//! - **F-4**: `ExpectedBehavior` is validated by at least one test per profile.
//! - **F-5**: Catalog iteration supports filtering by category and severity.
//! - **F-6**: Profiles compose with `FaultState` from `fault_vfs` without modification.
//! - **F-7**: Seed variation produces distinct but reproducible spec sequences.
//! - **F-8**: Profile metadata is machine-readable for CI triage integration.

use crate::eprocess::MvccInvariant;
use crate::fault_vfs::FaultSpec;

/// Bead identifier for tracing and log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.2.3.1";

// ---------------------------------------------------------------------------
// Fault category and severity
// ---------------------------------------------------------------------------

/// Broad classification of fault mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultCategory {
    /// Generic I/O read/write errors.
    IoError,
    /// Partial (torn) writes where only a prefix persists.
    TornWrite,
    /// Simulated power loss during operations.
    PowerLoss,
    /// Corruption of sidecar files (WAL, SHM, journal).
    SidecarCorruption,
}

impl FaultCategory {
    /// All categories in canonical order.
    #[must_use]
    pub fn all() -> &'static [Self] {
        &[
            Self::IoError,
            Self::TornWrite,
            Self::PowerLoss,
            Self::SidecarCorruption,
        ]
    }

    /// Human-readable label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::IoError => "I/O Error",
            Self::TornWrite => "Torn Write",
            Self::PowerLoss => "Power Loss",
            Self::SidecarCorruption => "Sidecar Corruption",
        }
    }
}

/// How severe is the expected impact on database operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultSeverity {
    /// Handled gracefully, no data loss expected.
    Benign,
    /// Some operations may fail transiently, but recovery is automatic.
    Degraded,
    /// Recovery required after restart; uncommitted data may be lost.
    Recoverable,
    /// May require explicit repair; committed data at risk.
    Catastrophic,
}

impl FaultSeverity {
    /// All severities in ascending order.
    #[must_use]
    pub fn all() -> &'static [Self] {
        &[
            Self::Benign,
            Self::Degraded,
            Self::Recoverable,
            Self::Catastrophic,
        ]
    }

    /// Human-readable label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Benign => "Benign",
            Self::Degraded => "Degraded",
            Self::Recoverable => "Recoverable",
            Self::Catastrophic => "Catastrophic",
        }
    }
}

// ---------------------------------------------------------------------------
// Expected behavior
// ---------------------------------------------------------------------------

/// What should happen when a fault profile fires during a test scenario.
#[derive(Debug, Clone)]
pub struct ExpectedBehavior {
    /// Database should recover to a consistent state after restart.
    pub recovery_expected: bool,
    /// Uncommitted data loss is acceptable (transactions in flight).
    pub uncommitted_loss_allowed: bool,
    /// Committed data must survive the fault.
    pub committed_data_preserved: bool,
    /// MVCC invariants that must hold during and after the fault.
    pub invariants_preserved: Vec<MvccInvariant>,
    /// Brief description of expected observable behavior.
    pub description: &'static str,
}

impl ExpectedBehavior {
    /// Standard recovery expectation: committed data survives, uncommitted may be lost,
    /// all core MVCC invariants preserved.
    #[must_use]
    pub fn standard_recovery() -> Self {
        Self {
            recovery_expected: true,
            uncommitted_loss_allowed: true,
            committed_data_preserved: true,
            invariants_preserved: MvccInvariant::ALL.to_vec(),
            description: "Standard recovery: committed data preserved, uncommitted lost",
        }
    }

    /// Graceful handling: no data loss, operation retries succeed.
    #[must_use]
    pub fn graceful_retry() -> Self {
        Self {
            recovery_expected: true,
            uncommitted_loss_allowed: false,
            committed_data_preserved: true,
            invariants_preserved: MvccInvariant::ALL.to_vec(),
            description: "Graceful handling: transient error, retry succeeds",
        }
    }

    /// Catastrophic: committed data may be at risk, recovery uncertain.
    #[must_use]
    pub fn catastrophic() -> Self {
        Self {
            recovery_expected: false,
            uncommitted_loss_allowed: true,
            committed_data_preserved: false,
            invariants_preserved: vec![
                MvccInvariant::Monotonicity,
                MvccInvariant::LockExclusivity,
                MvccInvariant::SerializedModeExclusivity,
            ],
            description: "Catastrophic: committed data at risk, hardware-enforced invariants only",
        }
    }
}

// ---------------------------------------------------------------------------
// Fault profile
// ---------------------------------------------------------------------------

/// A named, deterministic fault profile that generates [`FaultSpec`] sequences
/// and declares expected invariant behavior.
#[derive(Debug, Clone)]
pub struct FaultProfile {
    /// Stable identifier (e.g., `"FP-001"`). Never changes once assigned.
    pub id: &'static str,
    /// Human-readable name.
    pub name: &'static str,
    /// Broad fault mechanism.
    pub category: FaultCategory,
    /// Expected severity of impact.
    pub severity: FaultSeverity,
    /// Detailed description of what this profile simulates.
    pub description: &'static str,
    /// Expected behavior when this fault fires.
    pub expected: ExpectedBehavior,
    /// Which profile variant to use for spec generation.
    variant: ProfileVariant,
}

/// Internal: which spec generation strategy to use.
#[derive(Debug, Clone, Copy)]
enum ProfileVariant {
    TornWalFrame,
    TornJournalRecord,
    TornDbPage,
    PowerCutDuringCommit,
    PowerCutDuringCheckpoint,
    PowerCutMidTransaction,
    IoErrorOnRead,
    IoErrorOnWalSync,
    IoErrorOnDbWrite,
    CorruptWalHeader,
    CorruptShmRegion,
    CorruptJournalHeader,
}

impl FaultProfile {
    /// Generate deterministic [`FaultSpec`] sequences for this profile.
    ///
    /// The `seed` controls variation within the profile (different offsets,
    /// sync counts, etc.) while keeping behavior deterministic.
    #[must_use]
    pub fn generate_specs(&self, seed: u64) -> Vec<FaultSpec> {
        match self.variant {
            ProfileVariant::TornWalFrame => {
                // Tear a WAL frame write at a seed-dependent frame index.
                let frame_idx = (seed % 8) + 1; // frames 1-8
                let offset = 32 + (frame_idx - 1) * (24 + 4096); // WAL header + preceding frames
                let valid_bytes = usize::try_from((seed / 8) % 24).unwrap_or(0); // tear within frame header
                vec![
                    FaultSpec::torn_write("*.wal")
                        .at_offset_bytes(offset)
                        .valid_bytes(valid_bytes)
                        .build(),
                ]
            }

            ProfileVariant::TornJournalRecord => {
                // Tear a rollback journal record.
                let record_idx = (seed % 5) + 1;
                // Journal: 28-byte header + records of (4B page_no + page_size + 4B checksum).
                let page_size: u64 = 4096;
                let record_size = 4 + page_size + 4;
                let offset = 28 + (record_idx - 1) * record_size;
                let valid_bytes = usize::try_from((seed / 5) % 16).unwrap_or(0);
                vec![
                    FaultSpec::torn_write("*.journal")
                        .at_offset_bytes(offset)
                        .valid_bytes(valid_bytes)
                        .build(),
                ]
            }

            ProfileVariant::TornDbPage => {
                // Tear a direct database page write.
                let page_no = (seed % 10) + 1; // pages 1-10
                let page_size: u64 = 4096;
                let offset = (page_no - 1) * page_size;
                let valid_bytes = usize::try_from((seed / 10) % page_size).unwrap_or(0);
                vec![
                    FaultSpec::torn_write("*.db")
                        .at_offset_bytes(offset)
                        .valid_bytes(valid_bytes)
                        .build(),
                ]
            }

            ProfileVariant::PowerCutDuringCommit => {
                // Power cut after the Nth WAL sync (commit boundary).
                let sync_idx = u32::try_from((seed % 4) + 1).unwrap_or(1); // after 1-4 syncs
                vec![
                    FaultSpec::power_cut("*.wal")
                        .after_nth_sync(sync_idx)
                        .build(),
                ]
            }

            ProfileVariant::PowerCutDuringCheckpoint => {
                // Power cut during checkpoint (db file sync).
                let sync_idx = u32::try_from((seed % 3) + 1).unwrap_or(1);
                vec![
                    FaultSpec::power_cut("*.db")
                        .after_nth_sync(sync_idx)
                        .build(),
                ]
            }

            ProfileVariant::PowerCutMidTransaction => {
                // Power cut early (after first WAL sync) to simulate mid-transaction crash.
                vec![FaultSpec::power_cut("*.wal").after_nth_sync(0).build()]
            }

            ProfileVariant::IoErrorOnRead => {
                // I/O error on database file reads.
                vec![FaultSpec::io_error("*.db").build()]
            }

            ProfileVariant::IoErrorOnWalSync => {
                // I/O error on WAL sync.
                vec![FaultSpec::io_error("*.wal").build()]
            }

            ProfileVariant::IoErrorOnDbWrite => {
                // I/O error on database file writes.
                vec![FaultSpec::io_error("*.db").build()]
            }

            ProfileVariant::CorruptWalHeader => {
                // Torn write at WAL file header (first 32 bytes).
                let valid_bytes = usize::try_from(seed % 32).unwrap_or(0);
                vec![
                    FaultSpec::torn_write("*.wal")
                        .at_offset_bytes(0)
                        .valid_bytes(valid_bytes)
                        .build(),
                ]
            }

            ProfileVariant::CorruptShmRegion => {
                // Torn write to shared memory (WAL index) file.
                let valid_bytes = usize::try_from(seed % 64).unwrap_or(0);
                vec![
                    FaultSpec::torn_write("*.shm")
                        .at_offset_bytes(0)
                        .valid_bytes(valid_bytes)
                        .build(),
                ]
            }

            ProfileVariant::CorruptJournalHeader => {
                // Torn write at journal header (first 28 bytes).
                let valid_bytes = usize::try_from(seed % 28).unwrap_or(0);
                vec![
                    FaultSpec::torn_write("*.journal")
                        .at_offset_bytes(0)
                        .valid_bytes(valid_bytes)
                        .build(),
                ]
            }
        }
    }

    /// Summary line for triage reports.
    #[must_use]
    pub fn summary_line(&self) -> String {
        format!(
            "[{}] {} ({}/{}) — {}",
            self.id,
            self.name,
            self.category.label(),
            self.severity.label(),
            self.description,
        )
    }
}

// ---------------------------------------------------------------------------
// Profile catalog
// ---------------------------------------------------------------------------

/// Catalog of all defined fault profiles.
///
/// Provides iteration, filtering, and lookup by ID.
#[derive(Debug, Clone)]
pub struct FaultProfileCatalog {
    profiles: Vec<FaultProfile>,
}

impl FaultProfileCatalog {
    /// Build the default catalog with all standard profiles.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn default_catalog() -> Self {
        Self {
            profiles: vec![
                // --- Torn Write profiles ---
                FaultProfile {
                    id: "FP-001",
                    name: "Torn WAL frame write",
                    category: FaultCategory::TornWrite,
                    severity: FaultSeverity::Recoverable,
                    description: "Partial write of a WAL frame; torn at frame header boundary",
                    expected: ExpectedBehavior::standard_recovery(),
                    variant: ProfileVariant::TornWalFrame,
                },
                FaultProfile {
                    id: "FP-002",
                    name: "Torn journal record",
                    category: FaultCategory::TornWrite,
                    severity: FaultSeverity::Recoverable,
                    description: "Partial write of a rollback journal page record",
                    expected: ExpectedBehavior::standard_recovery(),
                    variant: ProfileVariant::TornJournalRecord,
                },
                FaultProfile {
                    id: "FP-003",
                    name: "Torn database page write",
                    category: FaultCategory::TornWrite,
                    severity: FaultSeverity::Recoverable,
                    description: "Partial write of a database page during checkpoint or direct I/O",
                    expected: ExpectedBehavior::standard_recovery(),
                    variant: ProfileVariant::TornDbPage,
                },
                // --- Power Loss profiles ---
                FaultProfile {
                    id: "FP-004",
                    name: "Power cut during WAL commit",
                    category: FaultCategory::PowerLoss,
                    severity: FaultSeverity::Recoverable,
                    description: "Power loss at WAL sync boundary; in-flight commit lost",
                    expected: ExpectedBehavior::standard_recovery(),
                    variant: ProfileVariant::PowerCutDuringCommit,
                },
                FaultProfile {
                    id: "FP-005",
                    name: "Power cut during checkpoint",
                    category: FaultCategory::PowerLoss,
                    severity: FaultSeverity::Recoverable,
                    description: "Power loss during WAL-to-DB checkpoint transfer",
                    expected: ExpectedBehavior::standard_recovery(),
                    variant: ProfileVariant::PowerCutDuringCheckpoint,
                },
                FaultProfile {
                    id: "FP-006",
                    name: "Power cut mid-transaction",
                    category: FaultCategory::PowerLoss,
                    severity: FaultSeverity::Recoverable,
                    description: "Immediate power loss before any WAL sync completes",
                    expected: ExpectedBehavior {
                        recovery_expected: true,
                        uncommitted_loss_allowed: true,
                        committed_data_preserved: true,
                        invariants_preserved: MvccInvariant::ALL.to_vec(),
                        description: "Mid-transaction crash: no data committed, clean recovery",
                    },
                    variant: ProfileVariant::PowerCutMidTransaction,
                },
                // --- I/O Error profiles ---
                FaultProfile {
                    id: "FP-007",
                    name: "I/O error on database read",
                    category: FaultCategory::IoError,
                    severity: FaultSeverity::Benign,
                    description: "Transient read I/O error on database file",
                    expected: ExpectedBehavior::graceful_retry(),
                    variant: ProfileVariant::IoErrorOnRead,
                },
                FaultProfile {
                    id: "FP-008",
                    name: "I/O error on WAL sync",
                    category: FaultCategory::IoError,
                    severity: FaultSeverity::Degraded,
                    description: "I/O error during WAL fsync; commit may fail",
                    expected: ExpectedBehavior {
                        recovery_expected: true,
                        uncommitted_loss_allowed: true,
                        committed_data_preserved: true,
                        invariants_preserved: MvccInvariant::ALL.to_vec(),
                        description: "WAL sync failure: transaction aborted, retry possible",
                    },
                    variant: ProfileVariant::IoErrorOnWalSync,
                },
                FaultProfile {
                    id: "FP-009",
                    name: "I/O error on database write",
                    category: FaultCategory::IoError,
                    severity: FaultSeverity::Degraded,
                    description: "I/O error during database page write (checkpoint path)",
                    expected: ExpectedBehavior {
                        recovery_expected: true,
                        uncommitted_loss_allowed: true,
                        committed_data_preserved: true,
                        invariants_preserved: MvccInvariant::ALL.to_vec(),
                        description: "DB write failure: checkpoint aborted, WAL data intact",
                    },
                    variant: ProfileVariant::IoErrorOnDbWrite,
                },
                // --- Sidecar Corruption profiles ---
                FaultProfile {
                    id: "FP-010",
                    name: "Corrupt WAL header",
                    category: FaultCategory::SidecarCorruption,
                    severity: FaultSeverity::Catastrophic,
                    description: "Torn write to WAL file header (magic/checksum region)",
                    expected: ExpectedBehavior::catastrophic(),
                    variant: ProfileVariant::CorruptWalHeader,
                },
                FaultProfile {
                    id: "FP-011",
                    name: "Corrupt SHM region",
                    category: FaultCategory::SidecarCorruption,
                    severity: FaultSeverity::Recoverable,
                    description: "Torn write to WAL-index shared memory file",
                    expected: ExpectedBehavior {
                        recovery_expected: true,
                        uncommitted_loss_allowed: true,
                        committed_data_preserved: true,
                        invariants_preserved: MvccInvariant::ALL.to_vec(),
                        description: "SHM corruption: rebuilt from WAL on next open",
                    },
                    variant: ProfileVariant::CorruptShmRegion,
                },
                FaultProfile {
                    id: "FP-012",
                    name: "Corrupt journal header",
                    category: FaultCategory::SidecarCorruption,
                    severity: FaultSeverity::Recoverable,
                    description: "Torn write to rollback journal header (magic/nonce region)",
                    expected: ExpectedBehavior::standard_recovery(),
                    variant: ProfileVariant::CorruptJournalHeader,
                },
            ],
        }
    }

    /// Number of profiles in the catalog.
    #[must_use]
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    /// Whether the catalog is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }

    /// Iterate over all profiles.
    pub fn iter(&self) -> impl Iterator<Item = &FaultProfile> {
        self.profiles.iter()
    }

    /// Look up a profile by its stable ID (e.g., `"FP-001"`).
    #[must_use]
    pub fn by_id(&self, id: &str) -> Option<&FaultProfile> {
        self.profiles.iter().find(|p| p.id == id)
    }

    /// Filter profiles by category.
    #[must_use]
    pub fn by_category(&self, cat: FaultCategory) -> Vec<&FaultProfile> {
        self.profiles.iter().filter(|p| p.category == cat).collect()
    }

    /// Filter profiles by severity (exact match).
    #[must_use]
    pub fn by_severity(&self, sev: FaultSeverity) -> Vec<&FaultProfile> {
        self.profiles.iter().filter(|p| p.severity == sev).collect()
    }

    /// Filter profiles by maximum severity (inclusive).
    #[must_use]
    pub fn up_to_severity(&self, max_sev: FaultSeverity) -> Vec<&FaultProfile> {
        self.profiles
            .iter()
            .filter(|p| p.severity <= max_sev)
            .collect()
    }

    /// Profiles that expect committed data to survive the fault.
    #[must_use]
    pub fn committed_data_preserved(&self) -> Vec<&FaultProfile> {
        self.profiles
            .iter()
            .filter(|p| p.expected.committed_data_preserved)
            .collect()
    }

    /// Profiles that expect full recovery after restart.
    #[must_use]
    pub fn recovery_expected(&self) -> Vec<&FaultProfile> {
        self.profiles
            .iter()
            .filter(|p| p.expected.recovery_expected)
            .collect()
    }

    /// Render a triage summary table.
    #[must_use]
    pub fn render_triage_table(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Fault Profile Catalog ({} profiles)",
            self.profiles.len()
        );
        let _ = writeln!(out, "{}", "=".repeat(80));

        for cat in FaultCategory::all() {
            let group = self.by_category(*cat);
            if group.is_empty() {
                continue;
            }
            let _ = writeln!(out, "\n## {}", cat.label());
            for p in &group {
                let _ = writeln!(out, "  {}", p.summary_line());
            }
        }

        let _ = writeln!(out, "\n{}", "=".repeat(80));
        let _ = writeln!(
            out,
            "Recovery expected: {}/{}",
            self.recovery_expected().len(),
            self.profiles.len()
        );
        let _ = writeln!(
            out,
            "Committed data preserved: {}/{}",
            self.committed_data_preserved().len(),
            self.profiles.len()
        );
        out
    }
}

// ---------------------------------------------------------------------------
// Seed variation helpers
// ---------------------------------------------------------------------------

/// Generate a matrix of (profile, seed) pairs for exhaustive testing.
///
/// Returns `profiles.len() * seeds_per_profile` test cases, each with a
/// unique (profile_id, seed) combination.
#[must_use]
pub fn generate_test_matrix(
    catalog: &FaultProfileCatalog,
    seeds_per_profile: u64,
) -> Vec<(&FaultProfile, u64)> {
    let mut matrix = Vec::new();
    for profile in catalog.iter() {
        for seed in 0..seeds_per_profile {
            matrix.push((profile, seed));
        }
    }
    matrix
}

/// Quick-check that a profile's specs are internally consistent.
///
/// Validates:
/// - At least one spec is generated per seed
/// - Specs have non-empty file globs
/// - Profile ID matches expected format
#[must_use]
pub fn validate_profile(profile: &FaultProfile, seed: u64) -> Vec<String> {
    let mut issues = Vec::new();

    if !profile.id.starts_with("FP-") {
        issues.push(format!(
            "Profile ID '{}' does not match FP-xxx format",
            profile.id
        ));
    }

    let specs = profile.generate_specs(seed);
    if specs.is_empty() {
        issues.push(format!(
            "Profile '{}' generated 0 specs for seed {seed}",
            profile.id
        ));
    }

    for (i, spec) in specs.iter().enumerate() {
        if spec.file_glob.is_empty() {
            issues.push(format!(
                "Profile '{}' spec[{i}] has empty file_glob",
                profile.id
            ));
        }
    }

    if profile.expected.invariants_preserved.is_empty() {
        issues.push(format!(
            "Profile '{}' declares no preserved invariants",
            profile.id
        ));
    }

    issues
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const TEST_BEAD: &str = "bd-mblr.2.3.1";

    #[test]
    fn catalog_has_twelve_profiles() {
        let catalog = FaultProfileCatalog::default_catalog();
        assert_eq!(
            catalog.len(),
            12,
            "bead_id={TEST_BEAD} expected 12 profiles in default catalog"
        );
    }

    #[test]
    fn all_profile_ids_are_unique() {
        let catalog = FaultProfileCatalog::default_catalog();
        let ids: Vec<&str> = catalog.iter().map(|p| p.id).collect();
        let unique: HashSet<&str> = ids.iter().copied().collect();
        assert_eq!(
            ids.len(),
            unique.len(),
            "bead_id={TEST_BEAD} duplicate profile IDs found"
        );
    }

    #[test]
    fn all_profile_ids_match_format() {
        let catalog = FaultProfileCatalog::default_catalog();
        for p in catalog.iter() {
            assert!(
                p.id.starts_with("FP-"),
                "bead_id={TEST_BEAD} profile ID '{}' does not match FP-xxx format",
                p.id
            );
        }
    }

    #[test]
    fn every_profile_generates_at_least_one_spec() {
        let catalog = FaultProfileCatalog::default_catalog();
        for p in catalog.iter() {
            for seed in 0..5 {
                let specs = p.generate_specs(seed);
                assert!(
                    !specs.is_empty(),
                    "bead_id={TEST_BEAD} profile '{}' generated 0 specs for seed {seed}",
                    p.id,
                );
            }
        }
    }

    #[test]
    fn specs_are_deterministic_across_calls() {
        let catalog = FaultProfileCatalog::default_catalog();
        for p in catalog.iter() {
            let specs_a = p.generate_specs(42);
            let specs_b = p.generate_specs(42);
            assert_eq!(
                specs_a.len(),
                specs_b.len(),
                "bead_id={TEST_BEAD} profile '{}' non-deterministic spec count",
                p.id,
            );
            for (i, (a, b)) in specs_a.iter().zip(specs_b.iter()).enumerate() {
                assert_eq!(
                    a.file_glob, b.file_glob,
                    "bead_id={TEST_BEAD} profile '{}' spec[{i}] glob differs",
                    p.id,
                );
                assert_eq!(
                    a.kind, b.kind,
                    "bead_id={TEST_BEAD} profile '{}' spec[{i}] kind differs",
                    p.id,
                );
                assert_eq!(
                    a.at_offset, b.at_offset,
                    "bead_id={TEST_BEAD} profile '{}' spec[{i}] offset differs",
                    p.id,
                );
                assert_eq!(
                    a.after_nth_sync, b.after_nth_sync,
                    "bead_id={TEST_BEAD} profile '{}' spec[{i}] sync differs",
                    p.id,
                );
            }
        }
    }

    #[test]
    fn different_seeds_produce_variation_for_torn_wal_frame() {
        let catalog = FaultProfileCatalog::default_catalog();
        let fp001 = catalog.by_id("FP-001").expect("FP-001 exists");

        let mut offsets = HashSet::new();
        for seed in 0..8 {
            let specs = fp001.generate_specs(seed);
            offsets.insert(specs[0].at_offset);
        }
        assert!(
            offsets.len() > 1,
            "bead_id={TEST_BEAD} FP-001 should produce different offsets for different seeds"
        );
    }

    #[test]
    fn category_filter_returns_correct_groups() {
        let catalog = FaultProfileCatalog::default_catalog();

        let torn = catalog.by_category(FaultCategory::TornWrite);
        assert_eq!(
            torn.len(),
            3,
            "bead_id={TEST_BEAD} expected 3 torn-write profiles"
        );

        let power = catalog.by_category(FaultCategory::PowerLoss);
        assert_eq!(
            power.len(),
            3,
            "bead_id={TEST_BEAD} expected 3 power-loss profiles"
        );

        let io = catalog.by_category(FaultCategory::IoError);
        assert_eq!(
            io.len(),
            3,
            "bead_id={TEST_BEAD} expected 3 I/O error profiles"
        );

        let corrupt = catalog.by_category(FaultCategory::SidecarCorruption);
        assert_eq!(
            corrupt.len(),
            3,
            "bead_id={TEST_BEAD} expected 3 sidecar corruption profiles"
        );
    }

    #[test]
    fn severity_filter_returns_correct_profiles() {
        let catalog = FaultProfileCatalog::default_catalog();

        let benign = catalog.by_severity(FaultSeverity::Benign);
        assert_eq!(
            benign.len(),
            1,
            "bead_id={TEST_BEAD} expected 1 benign profile"
        );
        assert_eq!(benign[0].id, "FP-007");

        let catastrophic = catalog.by_severity(FaultSeverity::Catastrophic);
        assert_eq!(
            catastrophic.len(),
            1,
            "bead_id={TEST_BEAD} expected 1 catastrophic profile"
        );
        assert_eq!(catastrophic[0].id, "FP-010");
    }

    #[test]
    fn up_to_severity_is_monotonically_inclusive() {
        let catalog = FaultProfileCatalog::default_catalog();

        let benign_only = catalog.up_to_severity(FaultSeverity::Benign);
        let up_to_degraded = catalog.up_to_severity(FaultSeverity::Degraded);
        let up_to_recoverable = catalog.up_to_severity(FaultSeverity::Recoverable);
        let all = catalog.up_to_severity(FaultSeverity::Catastrophic);

        assert!(
            benign_only.len() <= up_to_degraded.len(),
            "bead_id={TEST_BEAD} benign <= degraded"
        );
        assert!(
            up_to_degraded.len() <= up_to_recoverable.len(),
            "bead_id={TEST_BEAD} degraded <= recoverable"
        );
        assert!(
            up_to_recoverable.len() <= all.len(),
            "bead_id={TEST_BEAD} recoverable <= all"
        );
        assert_eq!(
            all.len(),
            catalog.len(),
            "bead_id={TEST_BEAD} catastrophic includes all"
        );
    }

    #[test]
    fn committed_data_preserved_filter() {
        let catalog = FaultProfileCatalog::default_catalog();
        let preserved = catalog.committed_data_preserved();
        // Only FP-010 (corrupt WAL header, catastrophic) does NOT preserve committed data.
        assert_eq!(
            preserved.len(),
            11,
            "bead_id={TEST_BEAD} 11 of 12 profiles should preserve committed data"
        );
    }

    #[test]
    fn recovery_expected_filter() {
        let catalog = FaultProfileCatalog::default_catalog();
        let recoverable = catalog.recovery_expected();
        // Only FP-010 does not expect recovery.
        assert_eq!(
            recoverable.len(),
            11,
            "bead_id={TEST_BEAD} 11 of 12 profiles should expect recovery"
        );
    }

    #[test]
    fn every_profile_declares_at_least_one_invariant() {
        let catalog = FaultProfileCatalog::default_catalog();
        for p in catalog.iter() {
            assert!(
                !p.expected.invariants_preserved.is_empty(),
                "bead_id={TEST_BEAD} profile '{}' has no invariants declared",
                p.id,
            );
        }
    }

    #[test]
    fn all_recovery_profiles_preserve_all_mvcc_invariants() {
        let catalog = FaultProfileCatalog::default_catalog();
        let all_invariants: HashSet<_> = MvccInvariant::ALL.iter().copied().collect();

        for p in catalog.recovery_expected() {
            if p.expected.committed_data_preserved {
                let preserved: HashSet<_> =
                    p.expected.invariants_preserved.iter().copied().collect();
                assert_eq!(
                    preserved, all_invariants,
                    "bead_id={TEST_BEAD} recovery profile '{}' should preserve all MVCC invariants",
                    p.id,
                );
            }
        }
    }

    #[test]
    fn catastrophic_profile_preserves_hardware_invariants_only() {
        let catalog = FaultProfileCatalog::default_catalog();
        let fp010 = catalog.by_id("FP-010").expect("FP-010 exists");

        let preserved: HashSet<_> = fp010
            .expected
            .invariants_preserved
            .iter()
            .copied()
            .collect();
        assert!(
            preserved.contains(&MvccInvariant::Monotonicity),
            "bead_id={TEST_BEAD} FP-010 should preserve INV-1"
        );
        assert!(
            preserved.contains(&MvccInvariant::LockExclusivity),
            "bead_id={TEST_BEAD} FP-010 should preserve INV-2"
        );
        assert!(
            preserved.contains(&MvccInvariant::SerializedModeExclusivity),
            "bead_id={TEST_BEAD} FP-010 should preserve INV-7"
        );
        assert_eq!(
            preserved.len(),
            3,
            "bead_id={TEST_BEAD} FP-010 should preserve exactly 3 hardware-enforced invariants"
        );
    }

    #[test]
    fn validate_all_profiles_pass() {
        let catalog = FaultProfileCatalog::default_catalog();
        for p in catalog.iter() {
            let issues = validate_profile(p, 0);
            assert!(
                issues.is_empty(),
                "bead_id={TEST_BEAD} profile '{}' validation failed: {issues:?}",
                p.id,
            );
        }
    }

    #[test]
    fn test_matrix_generates_correct_count() {
        let catalog = FaultProfileCatalog::default_catalog();
        let matrix = generate_test_matrix(&catalog, 3);
        assert_eq!(
            matrix.len(),
            36,
            "bead_id={TEST_BEAD} 12 profiles * 3 seeds = 36 test cases"
        );
    }

    #[test]
    fn by_id_returns_correct_profile() {
        let catalog = FaultProfileCatalog::default_catalog();

        let fp004 = catalog.by_id("FP-004").expect("FP-004 exists");
        assert_eq!(fp004.name, "Power cut during WAL commit");
        assert_eq!(fp004.category, FaultCategory::PowerLoss);

        assert!(catalog.by_id("FP-999").is_none());
    }

    #[test]
    fn triage_table_renders_all_categories() {
        let catalog = FaultProfileCatalog::default_catalog();
        let table = catalog.render_triage_table();

        assert!(
            table.contains("I/O Error"),
            "bead_id={TEST_BEAD} triage table should list I/O Error category"
        );
        assert!(
            table.contains("Torn Write"),
            "bead_id={TEST_BEAD} triage table should list Torn Write category"
        );
        assert!(
            table.contains("Power Loss"),
            "bead_id={TEST_BEAD} triage table should list Power Loss category"
        );
        assert!(
            table.contains("Sidecar Corruption"),
            "bead_id={TEST_BEAD} triage table should list Sidecar Corruption category"
        );
        assert!(
            table.contains("12 profiles"),
            "bead_id={TEST_BEAD} triage table should report 12 profiles"
        );
    }

    #[test]
    fn summary_line_contains_profile_id() {
        let catalog = FaultProfileCatalog::default_catalog();
        for p in catalog.iter() {
            let line = p.summary_line();
            assert!(
                line.contains(p.id),
                "bead_id={TEST_BEAD} summary line should contain profile ID"
            );
            assert!(
                line.contains(p.name),
                "bead_id={TEST_BEAD} summary line should contain profile name"
            );
        }
    }

    #[test]
    fn torn_wal_frame_seed_variation_covers_all_frames() {
        let catalog = FaultProfileCatalog::default_catalog();
        let fp001 = catalog.by_id("FP-001").expect("FP-001 exists");

        let mut offsets = HashSet::new();
        for seed in 0..8 {
            let specs = fp001.generate_specs(seed);
            if let Some(offset) = specs[0].at_offset {
                offsets.insert(offset);
            }
        }
        // Seeds 0-7 should target frames 1-8, producing 8 distinct offsets.
        assert_eq!(
            offsets.len(),
            8,
            "bead_id={TEST_BEAD} FP-001 seeds 0-7 should cover 8 distinct frame offsets"
        );
    }

    #[test]
    fn power_cut_profiles_use_wal_or_db_globs() {
        let catalog = FaultProfileCatalog::default_catalog();
        let power = catalog.by_category(FaultCategory::PowerLoss);

        for p in &power {
            let specs = p.generate_specs(0);
            for spec in &specs {
                assert!(
                    spec.file_glob == "*.wal" || spec.file_glob == "*.db",
                    "bead_id={TEST_BEAD} power-loss profile '{}' targets unexpected glob: {}",
                    p.id,
                    spec.file_glob,
                );
            }
        }
    }

    #[test]
    fn io_error_profiles_generate_io_error_kind() {
        let catalog = FaultProfileCatalog::default_catalog();
        let io = catalog.by_category(FaultCategory::IoError);

        for p in &io {
            let specs = p.generate_specs(0);
            for spec in &specs {
                assert_eq!(
                    spec.kind,
                    crate::fault_vfs::FaultKind::IoError,
                    "bead_id={TEST_BEAD} I/O profile '{}' should generate IoError kind",
                    p.id,
                );
            }
        }
    }

    #[test]
    fn sidecar_corruption_targets_correct_files() {
        let catalog = FaultProfileCatalog::default_catalog();
        let corrupt = catalog.by_category(FaultCategory::SidecarCorruption);

        let mut globs: Vec<&str> = corrupt
            .iter()
            .flat_map(|p| p.generate_specs(0))
            .map(|s| match s.file_glob.as_str() {
                "*.wal" => "*.wal",
                "*.shm" => "*.shm",
                "*.journal" => "*.journal",
                other => panic!("unexpected glob: {other}"),
            })
            .collect();
        globs.sort_unstable();

        assert_eq!(
            globs,
            vec!["*.journal", "*.shm", "*.wal"],
            "bead_id={TEST_BEAD} sidecar corruption should target WAL, SHM, and journal"
        );
    }
}
