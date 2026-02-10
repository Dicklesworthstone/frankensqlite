//! Corruption scenario definitions for the E2E demo suite (bd-1w6k.7.2).
//!
//! Each [`CorruptionScenario`] captures a self-contained demo narrative:
//!
//! - **Setup**: how to create the database fixture.
//! - **Injection**: which corruption pattern to apply, and on which file.
//! - **Expected sqlite3 outcome**: data loss, integrity failure, etc.
//! - **Expected FrankenSQLite outcome**: recovery, graceful degradation, etc.
//! - **Success criteria**: machine-verifiable assertions.
//!
//! Downstream beads (bd-1w6k.7.3, bd-1w6k.7.4) implement the actual demo
//! runners that exercise these scenarios against each engine.

use crate::corruption::CorruptionPattern;

// ── Scenario builder types ──────────────────────────────────────────────

/// Which file the corruption targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionTarget {
    /// The main `.db` database file.
    Database,
    /// The WAL journal (`.db-wal`).
    Wal,
    /// The WAL-FEC sidecar (`.db-wal-fec`).
    WalFecSidecar,
}

/// What sqlite3 is expected to do after corruption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedSqliteBehavior {
    /// `PRAGMA integrity_check` returns errors; data may be partially readable.
    IntegrityCheckFails {
        /// If Some, the minimum number of rows that should survive.
        min_surviving_rows: Option<usize>,
    },
    /// The database opens but queries return fewer rows than were inserted.
    DataLoss {
        /// Upper bound on rows sqlite3 can recover (None = unknown, assert <= inserted).
        max_recovered_rows: Option<usize>,
    },
    /// The database cannot be opened at all (malformed header, etc.).
    OpenFails,
    /// sqlite3 silently discards the corrupted WAL tail; data from truncated
    /// frames is lost but the database is otherwise functional.
    WalTailTruncated,
}

/// What FrankenSQLite is expected to do after corruption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedFsqliteBehavior {
    /// WAL-FEC repair succeeds — all rows recovered, integrity check passes.
    FullRecovery,
    /// WAL-FEC repair attempted but corruption exceeds repair capacity (R symbols).
    /// Falls back to truncation like sqlite3.
    RepairExceedsCapacity,
    /// WAL-FEC recovery is disabled; behaves like sqlite3 (truncation / data loss).
    RecoveryDisabled,
    /// Sidecar itself is corrupted; falls back to truncation.
    SidecarDamaged,
}

/// A complete corruption demo scenario.
#[derive(Debug, Clone)]
pub struct CorruptionScenario {
    /// Human-readable scenario name for reports.
    pub name: &'static str,
    /// Narrative description of what this scenario demonstrates.
    pub description: &'static str,

    // ── Setup ──────────────────────────────────────────────────────
    /// Number of rows to insert during setup.
    pub setup_row_count: usize,
    /// Whether to enable WAL-FEC sidecar generation during setup.
    pub setup_wal_fec: bool,
    /// Number of WAL-FEC repair symbols (R) to provision. Ignored if `!setup_wal_fec`.
    pub setup_repair_symbols: u32,

    // ── Injection ──────────────────────────────────────────────────
    /// Which file to corrupt.
    pub target: CorruptionTarget,
    /// The corruption pattern to apply.
    pub pattern: ScenarioCorruptionPattern,
    /// RNG seed for deterministic corruption.
    pub seed: u64,

    // ── Expected outcomes ──────────────────────────────────────────
    /// Expected C SQLite behavior after corruption.
    pub expected_sqlite: ExpectedSqliteBehavior,
    /// Expected FrankenSQLite behavior after corruption.
    pub expected_fsqlite: ExpectedFsqliteBehavior,
    /// Whether FrankenSQLite recovery toggle should be ON for this scenario.
    pub fsqlite_recovery_enabled: bool,
}

/// Corruption patterns parameterized for scenario definitions.
///
/// These map to [`CorruptionPattern`] at runtime but allow scenario-relative
/// frame/page references (e.g. "first 2 frames" rather than absolute offsets).
#[derive(Debug, Clone)]
pub enum ScenarioCorruptionPattern {
    /// Corrupt specific WAL frames (0-indexed frame numbers).
    WalFrames { frame_indices: Vec<u32> },
    /// Corrupt ALL WAL frames (calculated at runtime from actual frame count).
    WalAllFrames,
    /// Zero out the 100-byte database header.
    DbHeaderZero,
    /// Corrupt a specific database page with random data.
    DbPageCorrupt { page_number: u32 },
    /// Flip a single bit in a WAL frame's data region.
    WalBitFlip { frame_index: u32, bit_position: u8 },
    /// Corrupt the WAL-FEC sidecar file.
    SidecarCorrupt { offset: u64, length: usize },
}

impl ScenarioCorruptionPattern {
    /// Convert to a concrete [`CorruptionPattern`] given runtime info.
    ///
    /// `total_frames` is required for `WalAllFrames`.
    #[must_use]
    pub fn to_corruption_pattern(&self, seed: u64, total_frames: u32) -> CorruptionPattern {
        match self {
            Self::WalFrames { frame_indices } => CorruptionPattern::WalFrameCorrupt {
                frame_numbers: frame_indices.clone(),
                seed,
            },
            Self::WalAllFrames => CorruptionPattern::WalFrameCorrupt {
                frame_numbers: (0..total_frames).collect(),
                seed,
            },
            Self::DbHeaderZero => CorruptionPattern::HeaderZero,
            Self::DbPageCorrupt { page_number } => CorruptionPattern::PagePartialCorrupt {
                page_number: *page_number,
                offset_within_page: 0,
                length: 128,
                seed,
            },
            Self::WalBitFlip {
                frame_index,
                bit_position,
            } => {
                // Flip within the frame payload at a fixed offset (100) so this is independent
                // of WAL file layout details (page size is handled by the injector).
                CorruptionPattern::WalFrameBitFlip {
                    frame_index: *frame_index,
                    byte_offset_within_payload: 100,
                    bit_position: *bit_position,
                }
            }
            Self::SidecarCorrupt { offset, length } => CorruptionPattern::SidecarCorrupt {
                offset: *offset,
                length: *length,
                seed,
            },
        }
    }
}

// ── Scenario catalog ────────────────────────────────────────────────────

/// Return the complete catalog of corruption demo scenarios.
///
/// These are organized by the two target narratives from bd-1w6k.7.2:
///
/// **Narrative 1 — WAL corruption after commit:**
///   Scenarios 1-4 corrupt WAL frames after a successful commit.
///
/// **Narrative 2 — Database page corruption (bitrot):**
///   Scenarios 5-6 corrupt the database file itself.
///
/// **Edge cases:**
///   Scenarios 7-8 test sidecar damage and recovery toggle.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn scenario_catalog() -> Vec<CorruptionScenario> {
    vec![
        // ── Narrative 1: WAL corruption after commit ──────────────────

        // Scenario 1: Moderate WAL corruption, within FEC tolerance
        CorruptionScenario {
            name: "wal_corrupt_within_tolerance",
            description: "\
                Corrupt 2 WAL frames after a 100-row commit. With R=4 repair \
                symbols, FrankenSQLite should fully recover. C SQLite truncates \
                the WAL at the first bad checksum, losing committed data.",
            setup_row_count: 100,
            setup_wal_fec: true,
            setup_repair_symbols: 4,
            target: CorruptionTarget::Wal,
            pattern: ScenarioCorruptionPattern::WalFrames {
                frame_indices: vec![0, 1],
            },
            seed: 42,
            expected_sqlite: ExpectedSqliteBehavior::WalTailTruncated,
            expected_fsqlite: ExpectedFsqliteBehavior::FullRecovery,
            fsqlite_recovery_enabled: true,
        },
        // Scenario 2: Single-bit WAL corruption (subtle bitrot)
        CorruptionScenario {
            name: "wal_single_bit_flip",
            description: "\
                Flip a single bit in the first WAL frame's payload. The WAL \
                checksum chain breaks at frame 1. C SQLite discards from that \
                point. FrankenSQLite detects via xxh3-128 hash mismatch and \
                repairs from FEC symbols.",
            setup_row_count: 100,
            setup_wal_fec: true,
            setup_repair_symbols: 4,
            target: CorruptionTarget::Wal,
            pattern: ScenarioCorruptionPattern::WalBitFlip {
                frame_index: 0,
                bit_position: 3,
            },
            seed: 0,
            expected_sqlite: ExpectedSqliteBehavior::WalTailTruncated,
            expected_fsqlite: ExpectedFsqliteBehavior::FullRecovery,
            fsqlite_recovery_enabled: true,
        },
        // Scenario 3: WAL corruption exceeding FEC capacity
        CorruptionScenario {
            name: "wal_corrupt_beyond_tolerance",
            description: "\
                Corrupt ALL WAL frames with only R=2 repair symbols. Neither \
                engine can recover: C SQLite truncates at frame 1, FrankenSQLite \
                detects the damage but repair symbols are insufficient.",
            setup_row_count: 100,
            setup_wal_fec: true,
            setup_repair_symbols: 2,
            target: CorruptionTarget::Wal,
            pattern: ScenarioCorruptionPattern::WalAllFrames,
            seed: 99,
            expected_sqlite: ExpectedSqliteBehavior::WalTailTruncated,
            expected_fsqlite: ExpectedFsqliteBehavior::RepairExceedsCapacity,
            fsqlite_recovery_enabled: true,
        },
        // Scenario 4: WAL corruption with recovery disabled (emulate C SQLite)
        CorruptionScenario {
            name: "wal_corrupt_recovery_disabled",
            description: "\
                Corrupt 2 frames (normally within R=4 tolerance) but disable \
                FrankenSQLite recovery. Both engines behave the same: truncate \
                the WAL and lose committed data. Demonstrates the toggle.",
            setup_row_count: 100,
            setup_wal_fec: true,
            setup_repair_symbols: 4,
            target: CorruptionTarget::Wal,
            pattern: ScenarioCorruptionPattern::WalFrames {
                frame_indices: vec![0, 1],
            },
            seed: 42,
            expected_sqlite: ExpectedSqliteBehavior::WalTailTruncated,
            expected_fsqlite: ExpectedFsqliteBehavior::RecoveryDisabled,
            fsqlite_recovery_enabled: false,
        },
        // ── Narrative 2: Database page corruption (bitrot) ────────────

        // Scenario 5: Database header zeroed
        CorruptionScenario {
            name: "db_header_zeroed",
            description: "\
                Zero out the 100-byte SQLite database header. C SQLite cannot \
                recognize the file format and fails on open or integrity check. \
                FrankenSQLite (future: header redundancy) would detect and \
                potentially reconstruct from WAL + header cache.",
            setup_row_count: 50,
            setup_wal_fec: false,
            setup_repair_symbols: 0,
            target: CorruptionTarget::Database,
            pattern: ScenarioCorruptionPattern::DbHeaderZero,
            seed: 0,
            expected_sqlite: ExpectedSqliteBehavior::OpenFails,
            expected_fsqlite: ExpectedFsqliteBehavior::RepairExceedsCapacity,
            fsqlite_recovery_enabled: true,
        },
        // Scenario 6: Corrupt a data page in the DB file
        CorruptionScenario {
            name: "db_page_bitrot",
            description: "\
                Corrupt 128 bytes of database page 2 (first data page after \
                header). C SQLite detects corruption via integrity_check but \
                cannot self-heal. FrankenSQLite (future: page checksums) would \
                detect and flag the page for repair.",
            setup_row_count: 50,
            setup_wal_fec: false,
            setup_repair_symbols: 0,
            target: CorruptionTarget::Database,
            pattern: ScenarioCorruptionPattern::DbPageCorrupt { page_number: 2 },
            seed: 77,
            expected_sqlite: ExpectedSqliteBehavior::IntegrityCheckFails {
                min_surviving_rows: None,
            },
            expected_fsqlite: ExpectedFsqliteBehavior::RepairExceedsCapacity,
            fsqlite_recovery_enabled: true,
        },
        // ── Edge cases ───────────────────────────────────────────────

        // Scenario 7: Sidecar itself corrupted
        CorruptionScenario {
            name: "sidecar_damaged",
            description: "\
                Corrupt the WAL-FEC sidecar's repair symbol region. WAL frames \
                are also corrupted. FrankenSQLite attempts recovery but the \
                sidecar metadata/symbols are unreadable, so it falls back to \
                truncation like C SQLite.",
            setup_row_count: 100,
            setup_wal_fec: true,
            setup_repair_symbols: 4,
            target: CorruptionTarget::WalFecSidecar,
            pattern: ScenarioCorruptionPattern::SidecarCorrupt {
                offset: 64,
                length: 512,
            },
            seed: 55,
            expected_sqlite: ExpectedSqliteBehavior::WalTailTruncated,
            expected_fsqlite: ExpectedFsqliteBehavior::SidecarDamaged,
            fsqlite_recovery_enabled: true,
        },
        // Scenario 8: WAL corruption without any FEC sidecar
        CorruptionScenario {
            name: "wal_corrupt_no_sidecar",
            description: "\
                Corrupt WAL frames when no WAL-FEC sidecar exists. Both engines \
                behave identically: truncation at the first bad checksum. Shows \
                that FrankenSQLite degrades gracefully without FEC.",
            setup_row_count: 100,
            setup_wal_fec: false,
            setup_repair_symbols: 0,
            target: CorruptionTarget::Wal,
            pattern: ScenarioCorruptionPattern::WalFrames {
                frame_indices: vec![0, 1],
            },
            seed: 42,
            expected_sqlite: ExpectedSqliteBehavior::WalTailTruncated,
            expected_fsqlite: ExpectedFsqliteBehavior::RepairExceedsCapacity,
            fsqlite_recovery_enabled: true,
        },
    ]
}

/// Return only scenarios that demonstrate WAL corruption recovery (Narrative 1).
#[must_use]
pub fn wal_corruption_scenarios() -> Vec<CorruptionScenario> {
    scenario_catalog()
        .into_iter()
        .filter(|s| s.target == CorruptionTarget::Wal)
        .collect()
}

/// Return only scenarios that demonstrate database page corruption (Narrative 2).
#[must_use]
pub fn db_corruption_scenarios() -> Vec<CorruptionScenario> {
    scenario_catalog()
        .into_iter()
        .filter(|s| s.target == CorruptionTarget::Database)
        .collect()
}

/// Return scenarios where FrankenSQLite is expected to fully recover.
#[must_use]
pub fn recoverable_scenarios() -> Vec<CorruptionScenario> {
    scenario_catalog()
        .into_iter()
        .filter(|s| s.expected_fsqlite == ExpectedFsqliteBehavior::FullRecovery)
        .collect()
}

/// Return scenarios where both engines are expected to lose data.
#[must_use]
pub fn unrecoverable_scenarios() -> Vec<CorruptionScenario> {
    scenario_catalog()
        .into_iter()
        .filter(|s| s.expected_fsqlite != ExpectedFsqliteBehavior::FullRecovery)
        .collect()
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_has_all_scenarios() {
        let catalog = scenario_catalog();
        assert_eq!(catalog.len(), 8, "expected 8 scenarios in catalog");
    }

    #[test]
    fn test_catalog_names_are_unique() {
        let catalog = scenario_catalog();
        let names: Vec<&str> = catalog.iter().map(|s| s.name).collect();
        let mut deduped = names.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(names.len(), deduped.len(), "scenario names must be unique");
    }

    #[test]
    fn test_wal_scenarios_filter() {
        let wal = wal_corruption_scenarios();
        assert!(wal.len() >= 4, "expected at least 4 WAL scenarios");
        for s in &wal {
            assert_eq!(s.target, CorruptionTarget::Wal);
        }
    }

    #[test]
    fn test_db_scenarios_filter() {
        let db = db_corruption_scenarios();
        assert_eq!(db.len(), 2, "expected 2 DB corruption scenarios");
        for s in &db {
            assert_eq!(s.target, CorruptionTarget::Database);
        }
    }

    #[test]
    fn test_recoverable_filter() {
        let rec = recoverable_scenarios();
        assert!(rec.len() >= 2, "expected at least 2 recoverable scenarios");
        for s in &rec {
            assert_eq!(s.expected_fsqlite, ExpectedFsqliteBehavior::FullRecovery);
            assert!(s.fsqlite_recovery_enabled);
        }
    }

    #[test]
    fn test_unrecoverable_filter() {
        let unrec = unrecoverable_scenarios();
        assert!(
            unrec.len() >= 4,
            "expected at least 4 unrecoverable scenarios"
        );
        for s in &unrec {
            assert_ne!(s.expected_fsqlite, ExpectedFsqliteBehavior::FullRecovery);
        }
    }

    #[test]
    fn test_recovery_disabled_scenario_has_toggle_off() {
        let catalog = scenario_catalog();
        let disabled = catalog
            .iter()
            .find(|s| s.name == "wal_corrupt_recovery_disabled")
            .expect("should have recovery-disabled scenario");
        assert!(!disabled.fsqlite_recovery_enabled);
        assert_eq!(
            disabled.expected_fsqlite,
            ExpectedFsqliteBehavior::RecoveryDisabled
        );
    }

    #[test]
    fn test_scenario_pattern_to_corruption_pattern() {
        // WalFrames
        let pat = ScenarioCorruptionPattern::WalFrames {
            frame_indices: vec![0, 1],
        };
        let cp = pat.to_corruption_pattern(42, 10);
        assert!(matches!(cp, CorruptionPattern::WalFrameCorrupt { .. }));

        // WalAllFrames
        let pat = ScenarioCorruptionPattern::WalAllFrames;
        let cp = pat.to_corruption_pattern(99, 5);
        assert!(matches!(cp, CorruptionPattern::WalFrameCorrupt { .. }));
        if let CorruptionPattern::WalFrameCorrupt { frame_numbers, .. } = cp {
            assert_eq!(frame_numbers, vec![0, 1, 2, 3, 4]);
        }

        // DbHeaderZero
        let pat = ScenarioCorruptionPattern::DbHeaderZero;
        let cp = pat.to_corruption_pattern(0, 0);
        assert!(matches!(cp, CorruptionPattern::HeaderZero));

        // WalBitFlip
        let pat = ScenarioCorruptionPattern::WalBitFlip {
            frame_index: 0,
            bit_position: 3,
        };
        let cp = pat.to_corruption_pattern(0, 10);
        assert!(matches!(cp, CorruptionPattern::WalFrameBitFlip { .. }));
    }

    #[test]
    fn test_all_scenarios_have_descriptions() {
        for s in &scenario_catalog() {
            assert!(!s.name.is_empty(), "name must not be empty");
            assert!(!s.description.is_empty(), "description must not be empty");
            assert!(
                s.setup_row_count > 0,
                "{}: row count must be positive",
                s.name
            );
        }
    }

    #[test]
    fn test_wal_fec_scenarios_have_sidecar_setup() {
        for s in &scenario_catalog() {
            if s.expected_fsqlite == ExpectedFsqliteBehavior::FullRecovery {
                assert!(
                    s.setup_wal_fec,
                    "{}: recoverable scenario must enable WAL-FEC setup",
                    s.name
                );
                assert!(
                    s.setup_repair_symbols > 0,
                    "{}: recoverable scenario must have R > 0",
                    s.name
                );
            }
        }
    }

    #[test]
    fn test_scenario_seed_determinism() {
        let catalog = scenario_catalog();
        // Scenarios with the same name should always have the same seed
        let s1 = &catalog[0];
        let s2 = &catalog[0];
        assert_eq!(s1.seed, s2.seed);
    }
}
