//! Structured log schema validator, redaction policy, and replay-decoder testpack (bd-1dp9.7.6).
//!
//! Builds on the unified E2E log schema from bd-1dp9.7.2 (`e2e_log_schema`) to provide:
//!
//! 1. **Batch schema validation** with CI-friendly diagnostic reports pinpointing violations.
//! 2. **Deterministic redaction policy** classifying fields by sensitivity and applying
//!    reproducible sanitization (same input always produces same redacted output).
//! 3. **Replay decoder** for JSONL event streams with round-trip consistency verification.
//!
//! # Schema Validation
//!
//! The [`validate_event_stream`] function processes a sequence of events and returns
//! a [`ValidationReport`] with per-event diagnostics, aggregate statistics, and
//! CI exit-code semantics (any error = non-zero).
//!
//! # Redaction Policy
//!
//! Fields are classified into [`FieldSensitivity`] levels. The [`redact_event`] function
//! applies deterministic redaction: sensitive values are replaced with stable placeholders
//! derived from a keyed hash, ensuring the same input always produces the same redacted
//! output while preventing information recovery.
//!
//! # Replay Decoder
//!
//! The [`decode_jsonl_stream`] function parses JSONL text into typed events. The
//! [`verify_roundtrip`] function confirms encode-decode-encode consistency.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as FmtWrite;

use serde::{Deserialize, Serialize};

use crate::e2e_log_schema::{self, LOG_SCHEMA_VERSION, LogEventSchema, LogEventType};

#[allow(dead_code)]
const BEAD_ID: &str = "bd-1dp9.7.6";

// ---- Field Sensitivity Classification ----

/// Sensitivity level for a schema field, governing redaction behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldSensitivity {
    /// Safe to emit in any log context (run_id, phase, event_type, scenario_id, seed).
    Safe,
    /// Internal operational data that may contain workspace-specific paths or identifiers.
    /// Redacted in external-facing logs, preserved in internal replay bundles.
    Internal,
    /// Potentially sensitive data (file paths with usernames, custom context values).
    /// Always redacted in exported logs.
    Sensitive,
}

/// Classification of a named field's sensitivity level.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldClassification {
    pub field_name: String,
    pub sensitivity: FieldSensitivity,
    pub redaction_strategy: RedactionStrategy,
}

/// How a field value is redacted when its sensitivity requires it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RedactionStrategy {
    /// Keep the value as-is (safe fields).
    Preserve,
    /// Replace with a deterministic placeholder derived from a keyed hash of the value.
    DeterministicHash,
    /// Replace with a fixed placeholder string.
    FixedPlaceholder,
    /// Redact file system paths by replacing the directory portion, keeping the basename.
    PathBasename,
}

/// Build the canonical field sensitivity classification for the log schema.
#[must_use]
pub fn build_field_classifications() -> Vec<FieldClassification> {
    vec![
        FieldClassification {
            field_name: "run_id".to_owned(),
            sensitivity: FieldSensitivity::Safe,
            redaction_strategy: RedactionStrategy::Preserve,
        },
        FieldClassification {
            field_name: "timestamp".to_owned(),
            sensitivity: FieldSensitivity::Safe,
            redaction_strategy: RedactionStrategy::Preserve,
        },
        FieldClassification {
            field_name: "phase".to_owned(),
            sensitivity: FieldSensitivity::Safe,
            redaction_strategy: RedactionStrategy::Preserve,
        },
        FieldClassification {
            field_name: "event_type".to_owned(),
            sensitivity: FieldSensitivity::Safe,
            redaction_strategy: RedactionStrategy::Preserve,
        },
        FieldClassification {
            field_name: "scenario_id".to_owned(),
            sensitivity: FieldSensitivity::Safe,
            redaction_strategy: RedactionStrategy::Preserve,
        },
        FieldClassification {
            field_name: "seed".to_owned(),
            sensitivity: FieldSensitivity::Safe,
            redaction_strategy: RedactionStrategy::Preserve,
        },
        FieldClassification {
            field_name: "backend".to_owned(),
            sensitivity: FieldSensitivity::Safe,
            redaction_strategy: RedactionStrategy::Preserve,
        },
        FieldClassification {
            field_name: "artifact_hash".to_owned(),
            sensitivity: FieldSensitivity::Safe,
            redaction_strategy: RedactionStrategy::Preserve,
        },
        // Context keys with potential sensitivity
        FieldClassification {
            field_name: "context.artifact_paths".to_owned(),
            sensitivity: FieldSensitivity::Internal,
            redaction_strategy: RedactionStrategy::PathBasename,
        },
        FieldClassification {
            field_name: "context.invariant_ids".to_owned(),
            sensitivity: FieldSensitivity::Safe,
            redaction_strategy: RedactionStrategy::Preserve,
        },
        // Catch-all for unknown context keys
        FieldClassification {
            field_name: "context.*".to_owned(),
            sensitivity: FieldSensitivity::Sensitive,
            redaction_strategy: RedactionStrategy::DeterministicHash,
        },
    ]
}

/// Look up the classification for a field name. Unknown fields default to Sensitive.
#[must_use]
pub fn classify_field(field_name: &str) -> FieldClassification {
    let classifications = build_field_classifications();
    // Exact match first
    if let Some(c) = classifications.iter().find(|c| c.field_name == field_name) {
        return c.clone();
    }
    // Wildcard match for context.*
    if field_name.starts_with("context.") {
        if let Some(c) = classifications.iter().find(|c| c.field_name == "context.*") {
            return FieldClassification {
                field_name: field_name.to_owned(),
                ..c.clone()
            };
        }
    }
    // Default: sensitive with deterministic hash
    FieldClassification {
        field_name: field_name.to_owned(),
        sensitivity: FieldSensitivity::Sensitive,
        redaction_strategy: RedactionStrategy::DeterministicHash,
    }
}

// ---- Deterministic Redaction ----

/// Deterministic hash-based redaction using xxhash for stability.
fn deterministic_redact(value: &str, salt: u64) -> String {
    let hash = xxhash_rust::xxh3::xxh3_64(value.as_bytes()).wrapping_add(salt);
    format!("[REDACTED:{hash:016x}]")
}

/// Redact a file path by keeping only the basename.
fn redact_path(value: &str) -> String {
    let parts: Vec<&str> = value.split(',').collect();
    parts
        .iter()
        .map(|p| {
            let trimmed = p.trim();
            if let Some(pos) = trimmed.rfind('/') {
                format!("[...]{}", &trimmed[pos..])
            } else {
                trimmed.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Apply a redaction strategy to a value.
fn apply_redaction(value: &str, strategy: RedactionStrategy, salt: u64) -> String {
    match strategy {
        RedactionStrategy::Preserve => value.to_owned(),
        RedactionStrategy::DeterministicHash => deterministic_redact(value, salt),
        RedactionStrategy::FixedPlaceholder => "[REDACTED]".to_owned(),
        RedactionStrategy::PathBasename => redact_path(value),
    }
}

/// Redact a log event according to the field sensitivity policy.
///
/// The `salt` parameter ensures deterministic output: the same event with the same
/// salt always produces the same redacted result.
#[must_use]
pub fn redact_event(event: &LogEventSchema, salt: u64) -> LogEventSchema {
    let mut redacted_context = BTreeMap::new();
    for (key, value) in &event.context {
        let field_name = format!("context.{key}");
        let classification = classify_field(&field_name);
        let redacted_value = apply_redaction(value, classification.redaction_strategy, salt);
        redacted_context.insert(key.clone(), redacted_value);
    }

    LogEventSchema {
        run_id: event.run_id.clone(),
        timestamp: event.timestamp.clone(),
        phase: event.phase,
        event_type: event.event_type,
        scenario_id: event.scenario_id.clone(),
        seed: event.seed,
        backend: event.backend.clone(),
        artifact_hash: event.artifact_hash.clone(),
        context: redacted_context,
    }
}

// ---- Replay Decoder ----

/// A single line parsed from a JSONL event stream.
#[derive(Debug, Clone)]
pub enum DecodedLine {
    /// Successfully parsed event with its 0-based line index.
    Event {
        line_index: usize,
        event: LogEventSchema,
    },
    /// Parse failure with the raw line text and error description.
    Error {
        line_index: usize,
        raw_line: String,
        error: String,
    },
}

/// Result of decoding a JSONL event stream.
#[derive(Debug, Clone)]
pub struct DecodedStream {
    pub events: Vec<LogEventSchema>,
    pub errors: Vec<DecodedLine>,
    pub total_lines: usize,
    pub blank_lines: usize,
}

impl DecodedStream {
    /// Number of successfully parsed events.
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Number of parse errors.
    #[must_use]
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Whether the stream was decoded without errors.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

/// Decode a JSONL text stream into typed log events.
///
/// Each non-blank line is treated as a JSON object conforming to [`LogEventSchema`].
/// Blank lines are counted but skipped. Parse failures are captured as errors.
#[must_use]
pub fn decode_jsonl_stream(jsonl: &str) -> DecodedStream {
    let mut events = Vec::new();
    let mut errors = Vec::new();
    let mut blank_lines = 0_usize;
    let mut total_lines = 0_usize;

    for (line_index, line) in jsonl.lines().enumerate() {
        total_lines += 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            blank_lines += 1;
            continue;
        }
        match serde_json::from_str::<LogEventSchema>(trimmed) {
            Ok(event) => events.push(event),
            Err(e) => errors.push(DecodedLine::Error {
                line_index,
                raw_line: trimmed.to_owned(),
                error: e.to_string(),
            }),
        }
    }

    DecodedStream {
        events,
        errors,
        total_lines,
        blank_lines,
    }
}

/// Encode a sequence of events as JSONL text (one JSON object per line).
///
/// # Errors
///
/// Returns an error if any event fails to serialize.
pub fn encode_jsonl_stream(events: &[LogEventSchema]) -> Result<String, serde_json::Error> {
    let mut output = String::new();
    for event in events {
        let line = serde_json::to_string(event)?;
        output.push_str(&line);
        output.push('\n');
    }
    Ok(output)
}

/// Verify encode-decode-encode round-trip consistency.
///
/// Encodes the events to JSONL, decodes back, and re-encodes. The two JSONL strings
/// must be byte-identical.
///
/// Returns `Ok(())` on success, or `Err(description)` with a diff summary on failure.
pub fn verify_roundtrip(events: &[LogEventSchema]) -> Result<(), String> {
    let encoded_1 = encode_jsonl_stream(events).map_err(|e| format!("encode pass 1: {e}"))?;
    let decoded = decode_jsonl_stream(&encoded_1);
    if !decoded.is_clean() {
        return Err(format!(
            "decode errors on pass 1: {} errors out of {} lines",
            decoded.error_count(),
            decoded.total_lines,
        ));
    }
    if decoded.events.len() != events.len() {
        return Err(format!(
            "event count mismatch: original {} vs decoded {}",
            events.len(),
            decoded.events.len(),
        ));
    }
    let encoded_2 =
        encode_jsonl_stream(&decoded.events).map_err(|e| format!("encode pass 2: {e}"))?;
    if encoded_1 != encoded_2 {
        let lines_1: Vec<&str> = encoded_1.lines().collect();
        let lines_2: Vec<&str> = encoded_2.lines().collect();
        let mut diff_summary = String::new();
        for (i, (l1, l2)) in lines_1.iter().zip(lines_2.iter()).enumerate() {
            if l1 != l2 {
                let _ = write!(diff_summary, "line {i}: first divergence\n");
                break;
            }
        }
        return Err(format!("round-trip mismatch:\n{diff_summary}"));
    }
    Ok(())
}

// ---- Batch Schema Validation ----

/// Diagnostic for a single validation issue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidationDiagnostic {
    /// 0-based index of the event in the stream.
    pub event_index: usize,
    /// The run_id of the event (for correlation).
    pub run_id: String,
    /// Severity of the issue.
    pub severity: DiagnosticSeverity,
    /// Human-readable description of the violation.
    pub message: String,
    /// The field that triggered the violation (if applicable).
    pub field: Option<String>,
}

/// Severity level for validation diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosticSeverity {
    /// Schema violation that must be fixed (missing required field, invalid value).
    Error,
    /// Quality issue that should be fixed (missing recommended field, suboptimal format).
    Warning,
    /// Informational note (e.g., deprecated field usage).
    Info,
}

/// Aggregate statistics from a validation run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidationStats {
    pub total_events: usize,
    pub valid_events: usize,
    pub invalid_events: usize,
    pub error_count: usize,
    pub warning_count: usize,
    pub info_count: usize,
    /// Unique run_ids observed.
    pub unique_run_ids: usize,
    /// Phases observed in the stream.
    pub phases_observed: Vec<String>,
    /// Event types observed in the stream.
    pub event_types_observed: Vec<String>,
}

/// Complete validation report for a batch of events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    pub bead_id: String,
    pub schema_version: String,
    pub diagnostics: Vec<ValidationDiagnostic>,
    pub stats: ValidationStats,
    /// CI-friendly: true if there are zero errors.
    pub passed: bool,
}

impl ValidationReport {
    /// Render a human-readable summary of the validation report.
    #[must_use]
    pub fn render_summary(&self) -> String {
        let mut out = String::new();
        let _ = write!(
            out,
            "Schema Validation Report (bd-1dp9.7.6)\n\
             Schema version: {}\n\
             Events: {} total, {} valid, {} invalid\n\
             Diagnostics: {} errors, {} warnings, {} info\n\
             Result: {}\n",
            self.schema_version,
            self.stats.total_events,
            self.stats.valid_events,
            self.stats.invalid_events,
            self.stats.error_count,
            self.stats.warning_count,
            self.stats.info_count,
            if self.passed { "PASS" } else { "FAIL" },
        );
        if !self.diagnostics.is_empty() {
            let _ = write!(out, "\nDiagnostics:\n");
            for diag in &self.diagnostics {
                let field_str = diag
                    .field
                    .as_deref()
                    .map_or(String::new(), |f| format!(" [{f}]"));
                let _ = write!(
                    out,
                    "  [{:?}] event[{}] run_id={}{}: {}\n",
                    diag.severity, diag.event_index, diag.run_id, field_str, diag.message,
                );
            }
        }
        out
    }
}

/// Validate a stream of events against the unified log schema.
///
/// Returns a [`ValidationReport`] with per-event diagnostics and aggregate statistics.
/// The report's `passed` field is `true` only if there are zero errors.
#[must_use]
pub fn validate_event_stream(events: &[LogEventSchema]) -> ValidationReport {
    let mut diagnostics = Vec::new();
    let mut run_ids = BTreeSet::new();
    let mut phases = BTreeSet::new();
    let mut event_types = BTreeSet::new();
    let mut invalid_events = BTreeSet::new();

    for (idx, event) in events.iter().enumerate() {
        run_ids.insert(event.run_id.clone());
        phases.insert(event.phase.as_str().to_owned());
        event_types.insert(event.event_type.as_str().to_owned());

        // Run the base schema validator
        let errors = e2e_log_schema::validate_log_event(event);
        for error_msg in &errors {
            invalid_events.insert(idx);
            diagnostics.push(ValidationDiagnostic {
                event_index: idx,
                run_id: event.run_id.clone(),
                severity: DiagnosticSeverity::Error,
                message: error_msg.clone(),
                field: extract_field_from_error(error_msg),
            });
        }

        // Extended validation: correlation ID format
        if !event.run_id.is_empty() && !event.run_id.contains('-') {
            diagnostics.push(ValidationDiagnostic {
                event_index: idx,
                run_id: event.run_id.clone(),
                severity: DiagnosticSeverity::Warning,
                message: "run_id should follow `{bead_id}-{timestamp}-{pid}` format".to_owned(),
                field: Some("run_id".to_owned()),
            });
        }

        // Extended validation: recommended fields for non-info events
        if event.event_type != LogEventType::Info
            && event.event_type != LogEventType::Start
            && event.scenario_id.is_none()
        {
            diagnostics.push(ValidationDiagnostic {
                event_index: idx,
                run_id: event.run_id.clone(),
                severity: DiagnosticSeverity::Warning,
                message: "scenario_id recommended for non-info/non-start events".to_owned(),
                field: Some("scenario_id".to_owned()),
            });
        }

        // Extended validation: seed recommended for all events
        if event.seed.is_none() {
            diagnostics.push(ValidationDiagnostic {
                event_index: idx,
                run_id: event.run_id.clone(),
                severity: DiagnosticSeverity::Info,
                message: "seed recommended for reproducibility".to_owned(),
                field: Some("seed".to_owned()),
            });
        }

        // Extended validation: first_divergence events should have context
        if event.event_type == LogEventType::FirstDivergence && event.context.is_empty() {
            diagnostics.push(ValidationDiagnostic {
                event_index: idx,
                run_id: event.run_id.clone(),
                severity: DiagnosticSeverity::Warning,
                message: "first_divergence events should include context with divergence details"
                    .to_owned(),
                field: Some("context".to_owned()),
            });
        }

        // Extended validation: artifact_generated events should have artifact_hash
        if event.event_type == LogEventType::ArtifactGenerated && event.artifact_hash.is_none() {
            diagnostics.push(ValidationDiagnostic {
                event_index: idx,
                run_id: event.run_id.clone(),
                severity: DiagnosticSeverity::Warning,
                message: "artifact_generated events should include artifact_hash".to_owned(),
                field: Some("artifact_hash".to_owned()),
            });
        }
    }

    let error_count = diagnostics
        .iter()
        .filter(|d| d.severity == DiagnosticSeverity::Error)
        .count();
    let warning_count = diagnostics
        .iter()
        .filter(|d| d.severity == DiagnosticSeverity::Warning)
        .count();
    let info_count = diagnostics
        .iter()
        .filter(|d| d.severity == DiagnosticSeverity::Info)
        .count();

    let stats = ValidationStats {
        total_events: events.len(),
        valid_events: events.len() - invalid_events.len(),
        invalid_events: invalid_events.len(),
        error_count,
        warning_count,
        info_count,
        unique_run_ids: run_ids.len(),
        phases_observed: phases.into_iter().collect(),
        event_types_observed: event_types.into_iter().collect(),
    };

    ValidationReport {
        bead_id: BEAD_ID.to_owned(),
        schema_version: LOG_SCHEMA_VERSION.to_owned(),
        diagnostics,
        stats,
        passed: error_count == 0,
    }
}

/// Extract the field name from a validation error message (best-effort).
fn extract_field_from_error(msg: &str) -> Option<String> {
    if msg.starts_with("required field '") {
        let rest = &msg["required field '".len()..];
        return rest.split('\'').next().map(str::to_owned);
    }
    if msg.starts_with("field '") {
        let rest = &msg["field '".len()..];
        return rest.split('\'').next().map(str::to_owned);
    }
    if msg.contains("scenario_id") {
        return Some("scenario_id".to_owned());
    }
    if msg.contains("artifact_hash") {
        return Some("artifact_hash".to_owned());
    }
    if msg.contains("timestamp") {
        return Some("timestamp".to_owned());
    }
    if msg.contains("seed") {
        return Some("seed".to_owned());
    }
    None
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e_log_schema::{LogEventType, LogPhase, canonical_event_examples};

    fn make_valid_event() -> LogEventSchema {
        let mut context = BTreeMap::new();
        context.insert("invariant_ids".to_owned(), "INV-1,INV-9".to_owned());
        context.insert(
            "artifact_paths".to_owned(),
            "/data/projects/test/artifacts/events.jsonl".to_owned(),
        );
        LogEventSchema {
            run_id: "bd-1dp9.7.6-20260213T090000Z-99999".to_owned(),
            timestamp: "2026-02-13T09:00:00.000Z".to_owned(),
            phase: LogPhase::Execute,
            event_type: LogEventType::Pass,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: Some(42),
            backend: Some("fsqlite".to_owned()),
            artifact_hash: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
            ),
            context,
        }
    }

    fn make_minimal_event(
        run_id: &str,
        phase: LogPhase,
        event_type: LogEventType,
    ) -> LogEventSchema {
        LogEventSchema {
            run_id: run_id.to_owned(),
            timestamp: "2026-02-13T09:00:00.000Z".to_owned(),
            phase,
            event_type,
            scenario_id: None,
            seed: None,
            backend: None,
            artifact_hash: None,
            context: BTreeMap::new(),
        }
    }

    // ---- Field Classification Tests ----

    #[test]
    fn field_classifications_cover_all_schema_fields() {
        let classifications = build_field_classifications();
        let expected_fields = [
            "run_id",
            "timestamp",
            "phase",
            "event_type",
            "scenario_id",
            "seed",
            "backend",
            "artifact_hash",
        ];
        for field in expected_fields {
            assert!(
                classifications.iter().any(|c| c.field_name == field),
                "missing classification for schema field '{field}'",
            );
        }
    }

    #[test]
    fn required_fields_are_safe() {
        for field in e2e_log_schema::REQUIRED_EVENT_FIELDS {
            let c = classify_field(field);
            assert_eq!(
                c.sensitivity,
                FieldSensitivity::Safe,
                "required field '{field}' should be Safe",
            );
        }
    }

    #[test]
    fn unknown_fields_default_sensitive() {
        let c = classify_field("context.user_email");
        assert_eq!(c.sensitivity, FieldSensitivity::Sensitive);
        assert_eq!(c.redaction_strategy, RedactionStrategy::DeterministicHash);
    }

    #[test]
    fn known_safe_context_key_preserved() {
        let c = classify_field("context.invariant_ids");
        assert_eq!(c.sensitivity, FieldSensitivity::Safe);
        assert_eq!(c.redaction_strategy, RedactionStrategy::Preserve);
    }

    #[test]
    fn artifact_paths_classified_internal() {
        let c = classify_field("context.artifact_paths");
        assert_eq!(c.sensitivity, FieldSensitivity::Internal);
        assert_eq!(c.redaction_strategy, RedactionStrategy::PathBasename);
    }

    // ---- Redaction Tests ----

    #[test]
    fn redaction_is_deterministic() {
        let event = make_valid_event();
        let salt = 12345_u64;
        let r1 = redact_event(&event, salt);
        let r2 = redact_event(&event, salt);
        assert_eq!(r1, r2, "redaction must be deterministic with same salt");
    }

    #[test]
    fn redaction_different_salt_different_output() {
        let mut event = make_valid_event();
        event
            .context
            .insert("secret".to_owned(), "my-password".to_owned());
        let r1 = redact_event(&event, 1);
        let r2 = redact_event(&event, 2);
        // The "secret" field should be redacted differently with different salts
        assert_ne!(
            r1.context.get("secret"),
            r2.context.get("secret"),
            "different salt should produce different redacted values",
        );
    }

    #[test]
    fn safe_fields_preserved_after_redaction() {
        let event = make_valid_event();
        let redacted = redact_event(&event, 42);
        assert_eq!(redacted.run_id, event.run_id);
        assert_eq!(redacted.timestamp, event.timestamp);
        assert_eq!(redacted.phase, event.phase);
        assert_eq!(redacted.event_type, event.event_type);
        assert_eq!(redacted.scenario_id, event.scenario_id);
        assert_eq!(redacted.seed, event.seed);
        assert_eq!(redacted.artifact_hash, event.artifact_hash);
    }

    #[test]
    fn invariant_ids_preserved_after_redaction() {
        let event = make_valid_event();
        let redacted = redact_event(&event, 42);
        assert_eq!(
            redacted.context.get("invariant_ids"),
            event.context.get("invariant_ids"),
            "invariant_ids (safe) should be preserved",
        );
    }

    #[test]
    fn artifact_paths_redacted_to_basename() {
        let event = make_valid_event();
        let redacted = redact_event(&event, 42);
        let redacted_paths = redacted.context.get("artifact_paths").unwrap();
        assert!(
            redacted_paths.contains("[...]"),
            "artifact_paths should have directory portion redacted, got: {redacted_paths}",
        );
        assert!(
            redacted_paths.contains("events.jsonl"),
            "artifact_paths should preserve basename, got: {redacted_paths}",
        );
    }

    #[test]
    fn sensitive_context_keys_redacted() {
        let mut event = make_valid_event();
        event
            .context
            .insert("user_data".to_owned(), "sensitive-info".to_owned());
        let redacted = redact_event(&event, 42);
        let redacted_value = redacted.context.get("user_data").unwrap();
        assert!(
            redacted_value.starts_with("[REDACTED:"),
            "sensitive context key should be hash-redacted, got: {redacted_value}",
        );
    }

    #[test]
    fn path_redaction_handles_multiple_paths() {
        let result = redact_path("/home/user/data/file1.json, /opt/logs/file2.jsonl");
        assert!(result.contains("[...]/file1.json"));
        assert!(result.contains("[...]/file2.jsonl"));
    }

    #[test]
    fn path_redaction_handles_no_directory() {
        let result = redact_path("file.json");
        assert_eq!(result, "file.json");
    }

    // ---- Replay Decoder Tests ----

    #[test]
    fn decode_valid_jsonl() {
        let event = make_valid_event();
        let jsonl = serde_json::to_string(&event).unwrap();
        let stream = decode_jsonl_stream(&jsonl);
        assert!(stream.is_clean());
        assert_eq!(stream.event_count(), 1);
        assert_eq!(stream.events[0], event);
    }

    #[test]
    fn decode_multiple_events() {
        let events = canonical_event_examples();
        let jsonl = encode_jsonl_stream(&events).unwrap();
        let stream = decode_jsonl_stream(&jsonl);
        assert!(stream.is_clean());
        assert_eq!(stream.event_count(), events.len());
    }

    #[test]
    fn decode_skips_blank_lines() {
        let event = make_valid_event();
        let line = serde_json::to_string(&event).unwrap();
        let jsonl = format!("\n{line}\n\n{line}\n");
        let stream = decode_jsonl_stream(&jsonl);
        assert!(stream.is_clean());
        assert_eq!(stream.event_count(), 2);
        assert_eq!(stream.blank_lines, 3); // leading + 2 between/trailing blanks
    }

    #[test]
    fn decode_captures_errors() {
        let jsonl = "not-valid-json\n{\"also\": \"bad\"}\n";
        let stream = decode_jsonl_stream(jsonl);
        assert!(!stream.is_clean());
        assert_eq!(stream.error_count(), 2);
        assert_eq!(stream.event_count(), 0);
    }

    #[test]
    fn decode_mixed_valid_and_invalid() {
        let event = make_valid_event();
        let valid_line = serde_json::to_string(&event).unwrap();
        let jsonl = format!("{valid_line}\nnot-json\n{valid_line}\n");
        let stream = decode_jsonl_stream(&jsonl);
        assert!(!stream.is_clean());
        assert_eq!(stream.event_count(), 2);
        assert_eq!(stream.error_count(), 1);
    }

    #[test]
    fn roundtrip_canonical_events() {
        let events = canonical_event_examples();
        let result = verify_roundtrip(&events);
        assert!(result.is_ok(), "roundtrip failed: {}", result.unwrap_err());
    }

    #[test]
    fn roundtrip_single_event() {
        let events = vec![make_valid_event()];
        let result = verify_roundtrip(&events);
        assert!(result.is_ok(), "roundtrip failed: {}", result.unwrap_err());
    }

    #[test]
    fn roundtrip_empty_stream() {
        let events: Vec<LogEventSchema> = Vec::new();
        let result = verify_roundtrip(&events);
        assert!(result.is_ok(), "roundtrip failed: {}", result.unwrap_err());
    }

    #[test]
    fn encode_produces_valid_jsonl() {
        let events = vec![make_valid_event(), make_valid_event()];
        let jsonl = encode_jsonl_stream(&events).unwrap();
        let lines: Vec<&str> = jsonl.lines().collect();
        assert_eq!(lines.len(), 2);
        for line in lines {
            let parsed: LogEventSchema = serde_json::from_str(line).unwrap();
            assert_eq!(parsed.run_id, events[0].run_id);
        }
    }

    // ---- Batch Validation Tests ----

    #[test]
    fn validate_canonical_examples_pass() {
        let events = canonical_event_examples();
        let report = validate_event_stream(&events);
        assert!(
            report.passed,
            "canonical examples should pass: {}",
            report.render_summary(),
        );
        assert_eq!(report.stats.total_events, events.len());
        assert_eq!(report.stats.valid_events, events.len());
    }

    #[test]
    fn validate_empty_stream() {
        let report = validate_event_stream(&[]);
        assert!(report.passed);
        assert_eq!(report.stats.total_events, 0);
    }

    #[test]
    fn validate_detects_empty_run_id() {
        let event = LogEventSchema {
            run_id: String::new(),
            timestamp: "2026-02-13T09:00:00Z".to_owned(),
            phase: LogPhase::Execute,
            event_type: LogEventType::Pass,
            scenario_id: Some("MVCC-1".to_owned()),
            seed: Some(42),
            backend: None,
            artifact_hash: None,
            context: BTreeMap::new(),
        };
        let report = validate_event_stream(&[event]);
        assert!(!report.passed);
        assert!(report.stats.error_count > 0);
    }

    #[test]
    fn validate_detects_bad_timestamp() {
        let event = LogEventSchema {
            run_id: "test-run-1".to_owned(),
            timestamp: "not-a-timestamp".to_owned(),
            phase: LogPhase::Execute,
            event_type: LogEventType::Info,
            scenario_id: None,
            seed: None,
            backend: None,
            artifact_hash: None,
            context: BTreeMap::new(),
        };
        let report = validate_event_stream(&[event]);
        assert!(!report.passed);
        let ts_errors: Vec<_> = report
            .diagnostics
            .iter()
            .filter(|d| d.field.as_deref() == Some("timestamp"))
            .collect();
        assert!(!ts_errors.is_empty(), "should flag bad timestamp");
    }

    #[test]
    fn validate_detects_missing_seed_for_failure() {
        let event = LogEventSchema {
            run_id: "test-run-1".to_owned(),
            timestamp: "2026-02-13T09:00:00Z".to_owned(),
            phase: LogPhase::Validate,
            event_type: LogEventType::Fail,
            scenario_id: None, // missing for fail event
            seed: None,        // missing for fail event
            backend: None,
            artifact_hash: None,
            context: BTreeMap::new(),
        };
        let report = validate_event_stream(&[event]);
        assert!(!report.passed);
        let seed_errors: Vec<_> = report
            .diagnostics
            .iter()
            .filter(|d| d.field.as_deref() == Some("seed"))
            .collect();
        assert!(
            !seed_errors.is_empty(),
            "should flag missing seed for fail events"
        );
    }

    #[test]
    fn validate_warns_on_missing_scenario_id() {
        let event = make_minimal_event("test-run-1", LogPhase::Validate, LogEventType::Pass);
        let report = validate_event_stream(&[event]);
        let scenario_warnings: Vec<_> = report
            .diagnostics
            .iter()
            .filter(|d| {
                d.severity == DiagnosticSeverity::Warning
                    && d.field.as_deref() == Some("scenario_id")
            })
            .collect();
        assert!(
            !scenario_warnings.is_empty(),
            "should warn about missing scenario_id for pass events",
        );
    }

    #[test]
    fn validate_warns_artifact_generated_without_hash() {
        let event = LogEventSchema {
            run_id: "test-run-1".to_owned(),
            timestamp: "2026-02-13T09:00:00Z".to_owned(),
            phase: LogPhase::Report,
            event_type: LogEventType::ArtifactGenerated,
            scenario_id: Some("INFRA-6".to_owned()),
            seed: Some(1),
            backend: None,
            artifact_hash: None, // should have hash
            context: BTreeMap::new(),
        };
        let report = validate_event_stream(&[event]);
        let hash_warnings: Vec<_> = report
            .diagnostics
            .iter()
            .filter(|d| {
                d.severity == DiagnosticSeverity::Warning
                    && d.field.as_deref() == Some("artifact_hash")
            })
            .collect();
        assert!(
            !hash_warnings.is_empty(),
            "should warn about missing artifact_hash for artifact_generated events",
        );
    }

    #[test]
    fn validate_warns_on_run_id_without_dashes() {
        let event = make_minimal_event("nodashes", LogPhase::Setup, LogEventType::Start);
        let report = validate_event_stream(&[event]);
        let warnings: Vec<_> = report
            .diagnostics
            .iter()
            .filter(|d| {
                d.severity == DiagnosticSeverity::Warning && d.field.as_deref() == Some("run_id")
            })
            .collect();
        assert!(
            !warnings.is_empty(),
            "should warn about run_id format without dashes",
        );
    }

    #[test]
    fn validate_report_tracks_unique_run_ids() {
        let events = vec![
            make_minimal_event("run-a", LogPhase::Setup, LogEventType::Start),
            make_minimal_event("run-a", LogPhase::Execute, LogEventType::Info),
            make_minimal_event("run-b", LogPhase::Setup, LogEventType::Start),
        ];
        let report = validate_event_stream(&events);
        assert_eq!(report.stats.unique_run_ids, 2);
    }

    #[test]
    fn validate_report_tracks_phases_and_types() {
        let events = vec![
            make_minimal_event("run-a", LogPhase::Setup, LogEventType::Start),
            make_minimal_event("run-a", LogPhase::Execute, LogEventType::Info),
            make_minimal_event("run-a", LogPhase::Validate, LogEventType::Pass),
        ];
        let report = validate_event_stream(&events);
        assert_eq!(report.stats.phases_observed.len(), 3);
        assert_eq!(report.stats.event_types_observed.len(), 3);
    }

    #[test]
    fn validate_report_render_summary() {
        let events = canonical_event_examples();
        let report = validate_event_stream(&events);
        let summary = report.render_summary();
        assert!(summary.contains("Schema Validation Report"));
        assert!(summary.contains(LOG_SCHEMA_VERSION));
        assert!(summary.contains("PASS") || summary.contains("FAIL"));
    }

    #[test]
    fn validate_report_json_roundtrip() {
        let events = canonical_event_examples();
        let report = validate_event_stream(&events);
        let json = serde_json::to_string_pretty(&report).unwrap();
        let deserialized: ValidationReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.passed, report.passed);
        assert_eq!(deserialized.stats, report.stats);
    }

    // ---- Integration: Decode + Validate + Redact Pipeline ----

    #[test]
    fn full_pipeline_decode_validate_redact() {
        let events = canonical_event_examples();

        // Step 1: Encode to JSONL
        let jsonl = encode_jsonl_stream(&events).unwrap();

        // Step 2: Decode back
        let decoded = decode_jsonl_stream(&jsonl);
        assert!(decoded.is_clean());
        assert_eq!(decoded.event_count(), events.len());

        // Step 3: Validate
        let report = validate_event_stream(&decoded.events);
        assert!(
            report.passed,
            "decoded events should validate: {}",
            report.render_summary(),
        );

        // Step 4: Redact
        let salt = 7777_u64;
        let redacted: Vec<LogEventSchema> = decoded
            .events
            .iter()
            .map(|e| redact_event(e, salt))
            .collect();

        // Step 5: Verify redacted events still validate (safe fields intact)
        let redacted_report = validate_event_stream(&redacted);
        assert!(
            redacted_report.passed,
            "redacted events should still validate: {}",
            redacted_report.render_summary(),
        );

        // Step 6: Verify round-trip on redacted events
        let result = verify_roundtrip(&redacted);
        assert!(
            result.is_ok(),
            "redacted roundtrip failed: {}",
            result.unwrap_err()
        );
    }

    #[test]
    fn full_pipeline_determinism() {
        let events = canonical_event_examples();
        let salt = 42_u64;

        // Two complete pipeline runs should produce identical results
        let run = |evts: &[LogEventSchema]| -> (String, ValidationReport) {
            let jsonl = encode_jsonl_stream(evts).unwrap();
            let decoded = decode_jsonl_stream(&jsonl);
            let redacted: Vec<LogEventSchema> = decoded
                .events
                .iter()
                .map(|e| redact_event(e, salt))
                .collect();
            let report = validate_event_stream(&redacted);
            let re_encoded = encode_jsonl_stream(&redacted).unwrap();
            (re_encoded, report)
        };

        let (jsonl_1, report_1) = run(&events);
        let (jsonl_2, report_2) = run(&events);

        assert_eq!(jsonl_1, jsonl_2, "pipeline output must be deterministic");
        assert_eq!(report_1.passed, report_2.passed);
        assert_eq!(report_1.stats, report_2.stats);
    }
}
