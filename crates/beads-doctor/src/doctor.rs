//! Doctor report model, JSON parsing, and the [`DoctorRunner`] abstraction.
//!
//! The `br doctor --quick --json` schema (verified against the installed `br`)
//! is:
//!
//! ```json
//! {
//!   "ok": false,
//!   "workspace_health": "degraded",
//!   "reliability_audit": {
//!     "source": "doctor.inspect",
//!     "health": "degraded",
//!     "anomaly_count": 1,
//!     "anomalies": [
//!       { "code": "stale_recovery_artifacts", "severity": "degraded",
//!         "message": "stale recovery artifacts present" }
//!     ]
//!   },
//!   "checks": [ ... ]
//! }
//! ```
//!
//! We consume `reliability_audit.anomalies`; all other fields are ignored so a
//! future `br` schema addition never breaks parsing.

use std::process::Command;

use asupersync::Cx;
use serde::{Deserialize, Serialize};

use crate::config::Workspace;
use crate::error::DoctorError;

/// A single structured anomaly reported by `br doctor`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Anomaly {
    /// Stable machine code, e.g. `database_corrupt` or `stale_recovery_artifacts`.
    pub code: String,
    /// Reported severity, e.g. `degraded`, `warn`, `error`, `critical`.
    pub severity: String,
    /// Human-readable description.
    pub message: String,
    /// Optional remediation hint (present on some `br` findings).
    #[serde(default)]
    pub remediation: Option<String>,
}

/// The normalized result of a single doctor run over one workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorReport {
    /// Overall reliability health string, when present.
    pub health: Option<String>,
    /// All anomalies parsed from `reliability_audit.anomalies`.
    pub anomalies: Vec<Anomaly>,
}

// ---- serde shapes for the subset of the doctor JSON we consume -------------

#[derive(Debug, Deserialize)]
struct DoctorJson {
    #[serde(default)]
    reliability_audit: Option<ReliabilityAudit>,
}

#[derive(Debug, Deserialize)]
struct ReliabilityAudit {
    #[serde(default)]
    health: Option<String>,
    #[serde(default)]
    anomalies: Vec<Anomaly>,
}

/// Parses raw `br doctor --json` stdout into a [`DoctorReport`].
///
/// # Errors
///
/// Returns [`DoctorError::Json`] when the payload is not valid JSON matching the
/// expected shape. A payload with no `reliability_audit` parses to an empty
/// report (no anomalies), which is treated as healthy.
pub fn parse_doctor_report(stdout: &str) -> Result<DoctorReport, DoctorError> {
    let parsed: DoctorJson = serde_json::from_str(stdout)?;
    let (health, anomalies) = parsed
        .reliability_audit
        .map_or((None, Vec::new()), |audit| (audit.health, audit.anomalies));
    Ok(DoctorReport { health, anomalies })
}

/// Runs the doctor for a workspace and returns its normalized report.
///
/// Implementations receive `&Cx` first, matching the asupersync-native
/// convention even though the concrete `br` runner performs a blocking
/// subprocess call (acceptable at a 30-minute cadence). Tests substitute an
/// in-memory stub so no real `br` is required.
pub trait DoctorRunner: Send + Sync {
    /// Probes `ws` and returns its parsed report.
    ///
    /// # Errors
    ///
    /// Returns a [`DoctorError`] when the subprocess cannot be launched or its
    /// output cannot be parsed.
    fn run(&self, cx: &Cx, ws: &Workspace) -> Result<DoctorReport, DoctorError>;
}

/// The production runner: shells out to `br doctor --quick --json`.
#[derive(Debug, Default, Clone, Copy)]
pub struct BrDoctorRunner;

impl DoctorRunner for BrDoctorRunner {
    fn run(&self, _cx: &Cx, ws: &Workspace) -> Result<DoctorReport, DoctorError> {
        let output = Command::new("br")
            .args(["doctor", "--quick", "--json"])
            .current_dir(&ws.path)
            .output()
            .map_err(|source| DoctorError::Spawn {
                command: "br doctor --quick --json".to_string(),
                dir: ws.path.clone(),
                source,
            })?;

        // `br` returns exit status 0 even when the workspace is `degraded`; the
        // structured JSON is always on stdout, so we parse stdout regardless of
        // status and let the anomaly severities drive behavior.
        let stdout = std::str::from_utf8(&output.stdout).map_err(|_| DoctorError::NonUtf8Output)?;
        parse_doctor_report(stdout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEGRADED_FIXTURE: &str = r#"{
        "ok": false,
        "workspace_health": "degraded",
        "reliability_audit": {
            "source": "doctor.inspect",
            "health": "degraded",
            "anomaly_count": 1,
            "anomalies": [
                { "code": "stale_recovery_artifacts", "severity": "degraded",
                  "message": "stale recovery artifacts present" }
            ]
        },
        "checks": []
    }"#;

    const CORRUPT_FIXTURE: &str = r#"{
        "ok": false,
        "reliability_audit": {
            "health": "critical",
            "anomalies": [
                { "code": "database_corrupt", "severity": "error",
                  "message": "beads.db failed integrity check",
                  "remediation": "restore from JSONL and rebuild the db" },
                { "code": "orphaned_write_lock", "severity": "warn",
                  "message": "stale write_lock present" }
            ]
        }
    }"#;

    #[test]
    fn parses_anomalies_from_fixture() {
        // Acceptance 1: fixture JSON -> [{code, severity, message}].
        let report = parse_doctor_report(DEGRADED_FIXTURE).expect("valid");
        assert_eq!(report.health.as_deref(), Some("degraded"));
        assert_eq!(report.anomalies.len(), 1);
        let a = &report.anomalies[0];
        assert_eq!(a.code, "stale_recovery_artifacts");
        assert_eq!(a.severity, "degraded");
        assert_eq!(a.message, "stale recovery artifacts present");
    }

    #[test]
    fn parses_multiple_anomalies_with_remediation() {
        let report = parse_doctor_report(CORRUPT_FIXTURE).expect("valid");
        assert_eq!(report.anomalies.len(), 2);
        assert_eq!(report.anomalies[0].code, "database_corrupt");
        assert_eq!(
            report.anomalies[0].remediation.as_deref(),
            Some("restore from JSONL and rebuild the db")
        );
        assert_eq!(report.anomalies[1].code, "orphaned_write_lock");
    }

    #[test]
    fn missing_reliability_audit_is_empty_report() {
        let report = parse_doctor_report(r#"{"ok": true, "checks": []}"#).expect("valid");
        assert!(report.anomalies.is_empty());
        assert!(report.health.is_none());
    }

    #[test]
    fn invalid_json_is_an_error() {
        assert!(parse_doctor_report("not json").is_err());
    }
}
