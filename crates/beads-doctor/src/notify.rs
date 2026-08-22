//! Severity policy, the stable notification payload schema, and the
//! [`Notifier`] abstraction.
//!
//! HTTP is intentionally done by shelling out to `curl` — the workspace forbids
//! tokio/reqwest, and a 30-minute-cadence probe has no need for an async HTTP
//! client.

use std::process::Command;

use asupersync::Cx;
use serde::{Deserialize, Serialize};

use crate::config::Workspace;
use crate::doctor::Anomaly;
use crate::error::DoctorError;

/// Version of the [`Notification`] payload schema.
///
/// Downstream PagerDuty/Slack bridges pin to this; bump only on a breaking
/// shape change.
pub const NOTIFICATION_SCHEMA_VERSION: u32 = 1;

/// Codes that always notify regardless of the reported severity or the
/// workspace threshold — data-integrity failures must never be log-only.
pub const ALWAYS_NOTIFY_CODES: &[&str] = &["database_corrupt", "sqlite_integrity_failed"];

/// The structured, stable notification payload.
///
/// The field set and order are a contract: `{schema_version, workspace, code,
/// severity, message, remediation, timestamp}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notification {
    /// Payload schema version ([`NOTIFICATION_SCHEMA_VERSION`]).
    pub schema_version: u32,
    /// Workspace name the finding belongs to.
    pub workspace: String,
    /// The anomaly code.
    pub code: String,
    /// The anomaly severity.
    pub severity: String,
    /// The anomaly message.
    pub message: String,
    /// Remediation guidance (empty string when none is known).
    pub remediation: String,
    /// Timestamp in nanoseconds from `asupersync::time::wall_now()`.
    pub timestamp: u64,
}

/// Ranks a severity string into a comparable tier.
///
/// `info/ok/unknown = 0`, `degraded = 1`, `warn = 2`, `error = 3`,
/// `critical/fatal = 4`.
#[must_use]
pub fn severity_rank(severity: &str) -> u8 {
    match severity.to_ascii_lowercase().as_str() {
        "critical" | "fatal" => 4,
        "error" => 3,
        "warn" | "warning" => 2,
        "degraded" => 1,
        _ => 0,
    }
}

/// Decides whether an anomaly warrants a notification.
///
/// True when the code is on [`ALWAYS_NOTIFY_CODES`], or when the anomaly's
/// severity rank meets or exceeds the workspace threshold. With the default
/// `"error"` threshold, `degraded`/`warn` findings are log-only.
#[must_use]
pub fn is_notify_worthy(code: &str, severity: &str, threshold: &str) -> bool {
    if ALWAYS_NOTIFY_CODES.contains(&code) {
        return true;
    }
    severity_rank(severity) >= severity_rank(threshold)
}

/// Canonical remediation text for known always-notify codes.
#[must_use]
pub fn default_remediation(code: &str) -> String {
    match code {
        "database_corrupt" => {
            "Restore `.beads/beads.db` from the JSONL store and rebuild (`br doctor --repair`)."
                .to_string()
        }
        "sqlite_integrity_failed" => {
            "Run `PRAGMA integrity_check`; rebuild the beads db from JSONL if it fails.".to_string()
        }
        _ => String::new(),
    }
}

/// Builds a [`Notification`] from an anomaly and the owning workspace.
#[must_use]
pub fn build_notification(ws: &Workspace, anomaly: &Anomaly, timestamp_nanos: u64) -> Notification {
    let remediation = anomaly
        .remediation
        .clone()
        .filter(|r| !r.is_empty())
        .unwrap_or_else(|| default_remediation(&anomaly.code));
    Notification {
        schema_version: NOTIFICATION_SCHEMA_VERSION,
        workspace: ws.name.clone(),
        code: anomaly.code.clone(),
        severity: anomaly.severity.clone(),
        message: anomaly.message.clone(),
        remediation,
        timestamp: timestamp_nanos,
    }
}

/// Posts notifications for error-severity findings.
///
/// Implementations receive `&Cx` first per the asupersync-native convention.
pub trait Notifier: Send + Sync {
    /// Posts `payload` to the given ntfy `server`/`topic`.
    ///
    /// # Errors
    ///
    /// Returns [`DoctorError::Notify`] when the underlying transport fails.
    fn notify(
        &self,
        cx: &Cx,
        server: &str,
        topic: &str,
        payload: &Notification,
    ) -> Result<(), DoctorError>;
}

/// The production notifier: `curl -s -H "Title: ..." -d <json> <server>/<topic>`.
#[derive(Debug, Default, Clone, Copy)]
pub struct NtfyNotifier;

impl Notifier for NtfyNotifier {
    fn notify(
        &self,
        _cx: &Cx,
        server: &str,
        topic: &str,
        payload: &Notification,
    ) -> Result<(), DoctorError> {
        let body = serde_json::to_string(payload)?;
        let url = format!("{}/{topic}", server.trim_end_matches('/'));
        let title = format!(
            "beads-doctor: {} [{}] {}",
            payload.workspace, payload.severity, payload.code
        );

        let output = Command::new("curl")
            .args(["-s", "-S", "--fail", "-H"])
            .arg(format!("Title: {title}"))
            .arg("-H")
            .arg("Content-Type: application/json")
            .arg("-d")
            .arg(&body)
            .arg(&url)
            .output()
            .map_err(|source| DoctorError::Notify {
                command: "curl".to_string(),
                reason: source.to_string(),
            })?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            Err(DoctorError::Notify {
                command: format!("curl {url}"),
                reason: format!("exit {:?}: {stderr}", output.status.code()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anomaly(code: &str, severity: &str) -> Anomaly {
        Anomaly {
            code: code.to_string(),
            severity: severity.to_string(),
            message: format!("{code} occurred"),
            remediation: None,
        }
    }

    #[test]
    fn error_and_critical_are_notify_worthy_at_default_threshold() {
        assert!(is_notify_worthy("some_code", "error", "error"));
        assert!(is_notify_worthy("some_code", "critical", "error"));
    }

    #[test]
    fn degraded_and_warn_are_log_only_at_default_threshold() {
        // Acceptance 4: WARN/degraded (e.g. orphaned write_lock) is not notified.
        assert!(!is_notify_worthy("orphaned_write_lock", "warn", "error"));
        assert!(!is_notify_worthy(
            "stale_recovery_artifacts",
            "degraded",
            "error"
        ));
    }

    #[test]
    fn always_notify_codes_override_severity_and_threshold() {
        // Even a mislabeled low severity notifies for corruption codes.
        assert!(is_notify_worthy("database_corrupt", "degraded", "error"));
        assert!(is_notify_worthy(
            "sqlite_integrity_failed",
            "warn",
            "critical"
        ));
    }

    #[test]
    fn threshold_can_be_relaxed_to_warn() {
        assert!(is_notify_worthy("noisy", "warn", "warn"));
        assert!(!is_notify_worthy("noisy", "degraded", "warn"));
    }

    #[test]
    fn payload_schema_is_stable() {
        // Acceptance 3: assert exact JSON keys/shape.
        let ws = Workspace {
            name: "fsqlite".to_string(),
            path: "/data/projects/frankensqlite".to_string(),
            ntfy_topic: "beads-fsqlite".to_string(),
            ntfy_server: "https://ntfy.sh".to_string(),
            severity_threshold: "error".to_string(),
        };
        let n = build_notification(&ws, &anomaly("database_corrupt", "error"), 1234);
        let value: serde_json::Value = serde_json::to_value(&n).expect("serializes");
        let obj = value.as_object().expect("object");

        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "code",
                "message",
                "remediation",
                "schema_version",
                "severity",
                "timestamp",
                "workspace",
            ]
        );
        assert_eq!(obj["schema_version"], serde_json::json!(1));
        assert_eq!(obj["workspace"], serde_json::json!("fsqlite"));
        assert_eq!(obj["code"], serde_json::json!("database_corrupt"));
        assert_eq!(obj["severity"], serde_json::json!("error"));
        assert_eq!(obj["timestamp"], serde_json::json!(1234));
        // database_corrupt gets canned remediation when none is supplied.
        assert!(
            obj["remediation"]
                .as_str()
                .expect("string")
                .contains("Restore")
        );
    }

    #[test]
    fn supplied_remediation_wins_over_default() {
        let ws = Workspace {
            name: "w".to_string(),
            path: "/p".to_string(),
            ntfy_topic: "t".to_string(),
            ntfy_server: "https://ntfy.sh".to_string(),
            severity_threshold: "error".to_string(),
        };
        let mut a = anomaly("database_corrupt", "error");
        a.remediation = Some("call the DBA".to_string());
        let n = build_notification(&ws, &a, 7);
        assert_eq!(n.remediation, "call the DBA");
    }
}
