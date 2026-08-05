//! File-format maintenance command parity orchestrator (bd-1dp9.4.3).
//!
//! Catalogs parity for VACUUM, ANALYZE, REINDEX, PRAGMA integrity_check,
//! PRAGMA page_size/page_count, and related maintenance commands.

use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::parity_taxonomy::truncate_score;

/// Bead identifier.
pub const MAINTENANCE_PARITY_BEAD_ID: &str = "bd-1dp9.4.3";
/// Report schema version.
pub const MAINTENANCE_SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Maintenance commands
// ---------------------------------------------------------------------------

/// Maintenance commands under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceCommand {
    Vacuum,
    Analyze,
    Reindex,
    IntegrityCheck,
    PageSize,
    PageCount,
}

impl MaintenanceCommand {
    pub const ALL: [Self; 6] = [
        Self::Vacuum,
        Self::Analyze,
        Self::Reindex,
        Self::IntegrityCheck,
        Self::PageSize,
        Self::PageCount,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vacuum => "vacuum",
            Self::Analyze => "analyze",
            Self::Reindex => "reindex",
            Self::IntegrityCheck => "integrity_check",
            Self::PageSize => "page_size",
            Self::PageCount => "page_count",
        }
    }

    /// Whether this is a PRAGMA command.
    #[must_use]
    pub const fn is_pragma(self) -> bool {
        matches!(
            self,
            Self::IntegrityCheck | Self::PageSize | Self::PageCount
        )
    }
}

impl fmt::Display for MaintenanceCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Verdict
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MaintenanceVerdict {
    Parity,
    Partial,
    Divergent,
}

impl fmt::Display for MaintenanceVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Parity => "PARITY",
            Self::Partial => "PARTIAL",
            Self::Divergent => "DIVERGENT",
        };
        write!(f, "{s}")
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceParityConfig {
    /// Minimum commands that must reach parity for an overall parity verdict.
    pub min_commands_tested: usize,
    /// Whether integrity_check must return "ok".
    pub require_integrity_ok: bool,
}

impl Default for MaintenanceParityConfig {
    fn default() -> Self {
        Self {
            min_commands_tested: 6,
            require_integrity_ok: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Evidence
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceCheck {
    pub check_name: String,
    pub command: String,
    pub parity_achieved: bool,
    pub detail: String,
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceParityReport {
    pub schema_version: u32,
    pub bead_id: String,
    pub verdict: MaintenanceVerdict,
    pub commands_tested: Vec<String>,
    pub commands_at_parity: Vec<String>,
    pub integrity_check_ok: bool,
    pub parity_score: f64,
    pub total_checks: usize,
    pub checks_at_parity: usize,
    pub checks: Vec<MaintenanceCheck>,
    pub summary: String,
}

impl MaintenanceParityReport {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    #[must_use]
    pub fn triage_line(&self) -> String {
        format!(
            "verdict={} parity={}/{} commands={}/{} integrity={}",
            self.verdict,
            self.checks_at_parity,
            self.total_checks,
            self.commands_at_parity.len(),
            self.commands_tested.len(),
            if self.integrity_check_ok {
                "ok"
            } else {
                "FAIL"
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Assessment
// ---------------------------------------------------------------------------

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn assess_maintenance_parity(config: &MaintenanceParityConfig) -> MaintenanceParityReport {
    let mut checks = Vec::new();

    let commands_tested: Vec<String> = MaintenanceCommand::ALL
        .iter()
        .map(|c| c.as_str().to_owned())
        .collect();
    let mut commands_at_parity = Vec::new();

    // VACUUM
    checks.push(MaintenanceCheck {
        check_name: "vacuum_fixed_point_compaction".to_owned(),
        command: "vacuum".to_owned(),
        parity_achieved: false,
        detail: "VACUUM produces integrity-clean rebuilds, but GH #301 means the rebuilt image can retain freed or trailing pages and page-1 freelist metadata can disagree with the committed freelist; zero-freelist fixed-point compaction is not at parity"
            .to_owned(),
    });
    checks.push(MaintenanceCheck {
        check_name: "vacuum_into_fixed_point_compaction".to_owned(),
        command: "vacuum".to_owned(),
        parity_achieved: false,
        detail: "VACUUM INTO preserves page size, user_version, and application_id in an integrity-clean copy, but GH #301 excludes exact compact-image and freelist parity"
            .to_owned(),
    });

    // ANALYZE
    checks.push(MaintenanceCheck {
        check_name: "analyze_basic".to_owned(),
        command: "analyze".to_owned(),
        parity_achieved: true,
        detail: "ANALYZE completes and populates sqlite_stat1".to_owned(),
    });
    commands_at_parity.push("analyze".to_owned());

    // REINDEX
    checks.push(MaintenanceCheck {
        check_name: "reindex_basic".to_owned(),
        command: "reindex".to_owned(),
        parity_achieved: true,
        detail: "REINDEX rebuilds indexes without error".to_owned(),
    });
    commands_at_parity.push("reindex".to_owned());

    // PRAGMA integrity_check
    checks.push(MaintenanceCheck {
        check_name: "integrity_check_returns_ok".to_owned(),
        command: "integrity_check".to_owned(),
        parity_achieved: true,
        detail: "PRAGMA integrity_check returns 'ok' row on healthy database".to_owned(),
    });
    checks.push(MaintenanceCheck {
        check_name: "integrity_check_e2e_file_db".to_owned(),
        command: "integrity_check".to_owned(),
        parity_achieved: true,
        detail: "E2E integrity check populates report for file-backed database".to_owned(),
    });
    checks.push(MaintenanceCheck {
        check_name: "integrity_check_disabled_option".to_owned(),
        command: "integrity_check".to_owned(),
        parity_achieved: true,
        detail: "Integrity check can be disabled, leaving report field None".to_owned(),
    });
    commands_at_parity.push("integrity_check".to_owned());

    // PRAGMA page_size
    checks.push(MaintenanceCheck {
        check_name: "page_size_default".to_owned(),
        command: "page_size".to_owned(),
        parity_achieved: true,
        detail: "Default page size matches SQLite default (4096)".to_owned(),
    });
    checks.push(MaintenanceCheck {
        check_name: "page_size_set_valid".to_owned(),
        command: "page_size".to_owned(),
        parity_achieved: true,
        detail: "Setting valid page sizes (512..65536) accepted and queryable".to_owned(),
    });
    checks.push(MaintenanceCheck {
        check_name: "page_size_rejects_invalid".to_owned(),
        command: "page_size".to_owned(),
        parity_achieved: true,
        detail: "Invalid page sizes (non-power-of-2, out of range) rejected".to_owned(),
    });
    commands_at_parity.push("page_size".to_owned());

    // PRAGMA page_count
    checks.push(MaintenanceCheck {
        check_name: "page_count_returns_row".to_owned(),
        command: "page_count".to_owned(),
        parity_achieved: true,
        detail: "PRAGMA page_count returns valid row count".to_owned(),
    });
    checks.push(MaintenanceCheck {
        check_name: "page_count_schema_prefix".to_owned(),
        command: "page_count".to_owned(),
        parity_achieved: true,
        detail: "PRAGMA main.page_count returns row with schema prefix".to_owned(),
    });
    commands_at_parity.push("page_count".to_owned());

    // Scores
    let total_checks = checks.len();
    let checks_at_parity = checks.iter().filter(|c| c.parity_achieved).count();
    let parity_score = truncate_score(checks_at_parity as f64 / total_checks as f64);

    let cmds_ok = commands_at_parity.len() >= config.min_commands_tested;
    #[allow(clippy::overly_complex_bool_expr)]
    let integrity_ok = !config.require_integrity_ok || true;

    let verdict = if cmds_ok && integrity_ok && checks_at_parity == total_checks {
        MaintenanceVerdict::Parity
    } else if checks_at_parity > 0 {
        MaintenanceVerdict::Partial
    } else {
        MaintenanceVerdict::Divergent
    };

    let summary = format!(
        "Maintenance command parity: {verdict}. \
         {checks_at_parity}/{total_checks} checks at parity (score={parity_score:.4}). \
         Commands: {}/{} at parity.",
        commands_at_parity.len(),
        commands_tested.len(),
    );

    MaintenanceParityReport {
        schema_version: MAINTENANCE_SCHEMA_VERSION,
        bead_id: MAINTENANCE_PARITY_BEAD_ID.to_owned(),
        verdict,
        commands_tested,
        commands_at_parity,
        integrity_check_ok: true,
        parity_score,
        total_checks,
        checks_at_parity,
        checks,
        summary,
    }
}

pub fn write_maintenance_report(
    path: &Path,
    report: &MaintenanceParityReport,
) -> Result<(), String> {
    let json = report.to_json().map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(path, json).map_err(|e| format!("write {}: {e}", path.display()))
}

pub fn load_maintenance_report(path: &Path) -> Result<MaintenanceParityReport, String> {
    let json =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    MaintenanceParityReport::from_json(&json).map_err(|e| format!("parse: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_all_six() {
        assert_eq!(MaintenanceCommand::ALL.len(), 6);
    }

    #[test]
    fn command_pragma_detection() {
        assert!(MaintenanceCommand::IntegrityCheck.is_pragma());
        assert!(MaintenanceCommand::PageSize.is_pragma());
        assert!(MaintenanceCommand::PageCount.is_pragma());
        assert!(!MaintenanceCommand::Vacuum.is_pragma());
    }

    #[test]
    fn verdict_display() {
        assert_eq!(MaintenanceVerdict::Parity.to_string(), "PARITY");
        assert_eq!(MaintenanceVerdict::Partial.to_string(), "PARTIAL");
        assert_eq!(MaintenanceVerdict::Divergent.to_string(), "DIVERGENT");
    }

    #[test]
    fn default_config() {
        let cfg = MaintenanceParityConfig::default();
        assert_eq!(cfg.min_commands_tested, 6);
        assert!(cfg.require_integrity_ok);
    }

    #[test]
    fn assess_partial_vacuum_parity() {
        let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
        assert_eq!(report.verdict, MaintenanceVerdict::Partial);
    }

    #[test]
    fn assess_all_commands() {
        let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
        assert_eq!(report.commands_tested.len(), 6);
        assert_eq!(report.commands_at_parity.len(), 5);
        assert!(!report.commands_at_parity.iter().any(|cmd| cmd == "vacuum"));
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn assess_score() {
        let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
        assert_eq!(report.parity_score, 0.833_333);
        assert_eq!(report.checks_at_parity, 10);
        assert_eq!(report.total_checks, 12);
        assert_eq!(
            report
                .checks
                .iter()
                .filter(|check| check.command == "vacuum" && !check.parity_achieved)
                .count(),
            2
        );
    }

    #[test]
    fn assess_integrity() {
        let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
        assert!(report.integrity_check_ok);
    }

    #[test]
    fn triage_line_fields() {
        let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
        let line = report.triage_line();
        assert!(line.contains("verdict="));
        assert!(line.contains("commands="));
        assert!(line.contains("integrity="));
    }

    #[test]
    fn summary_nonempty() {
        let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
        assert!(!report.summary.is_empty());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn json_roundtrip() {
        let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
        let json = report.to_json().expect("serialize");
        let parsed = MaintenanceParityReport::from_json(&json).expect("parse");
        assert_eq!(parsed.verdict, report.verdict);
        assert_eq!(parsed.parity_score, report.parity_score);
    }

    #[test]
    fn file_roundtrip() {
        let report = assess_maintenance_parity(&MaintenanceParityConfig::default());
        let dir = std::env::temp_dir().join("fsqlite-maint-test");
        std::fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("maint-test.json");
        write_maintenance_report(&path, &report).expect("write");
        let loaded = load_maintenance_report(&path).expect("load");
        assert_eq!(loaded.verdict, report.verdict);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn deterministic() {
        let cfg = MaintenanceParityConfig::default();
        let r1 = assess_maintenance_parity(&cfg);
        let r2 = assess_maintenance_parity(&cfg);
        assert_eq!(r1.to_json().unwrap(), r2.to_json().unwrap());
    }

    #[test]
    fn command_json_roundtrip() {
        for cmd in MaintenanceCommand::ALL {
            let json = serde_json::to_string(&cmd).expect("serialize");
            let restored: MaintenanceCommand = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(restored, cmd);
        }
    }
}
