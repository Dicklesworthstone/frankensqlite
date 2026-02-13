//! E2E test for bd-1dp9.7.6: Structured log schema validator, redaction policy, and replay decoder.
//!
//! Exercises the full pipeline: emit structured log events → encode to JSONL → decode →
//! validate against schema → redact sensitive fields → re-validate → verify round-trip.
//!
//! This test simulates a realistic E2E scenario where multiple runs produce structured
//! log events across all phases and event types.

use std::collections::BTreeMap;

use fsqlite_harness::e2e_log_schema::{LOG_SCHEMA_VERSION, LogEventSchema, LogEventType, LogPhase};
use fsqlite_harness::log_schema_validator::{
    self, DiagnosticSeverity, FieldSensitivity, decode_jsonl_stream, encode_jsonl_stream,
    redact_event, validate_event_stream, verify_roundtrip,
};

const BEAD_ID: &str = "bd-1dp9.7.6";
const SEED: u64 = 20260213;

/// Build a realistic E2E event stream simulating a multi-phase parity test run.
fn build_realistic_event_stream() -> Vec<LogEventSchema> {
    let run_id = format!("{BEAD_ID}-20260213T090000Z-{SEED}");
    let mut events = Vec::new();

    // Phase 1: Setup
    events.push(LogEventSchema {
        run_id: run_id.clone(),
        timestamp: "2026-02-13T09:00:00.000Z".to_owned(),
        phase: LogPhase::Setup,
        event_type: LogEventType::Start,
        scenario_id: Some("INFRA-6".to_owned()),
        seed: Some(SEED),
        backend: Some("both".to_owned()),
        artifact_hash: None,
        context: {
            let mut ctx = BTreeMap::new();
            ctx.insert("invariant_ids".to_owned(), "INV-1,INV-9".to_owned());
            ctx
        },
    });

    // Phase 2: Execute with multiple scenarios
    for (i, scenario) in ["MVCC-3", "SSI-1", "TXN-1", "WAL-1"].iter().enumerate() {
        events.push(LogEventSchema {
            run_id: run_id.clone(),
            timestamp: format!("2026-02-13T09:00:0{}.000Z", i + 1),
            phase: LogPhase::Execute,
            event_type: LogEventType::Info,
            scenario_id: Some((*scenario).to_owned()),
            seed: Some(SEED),
            backend: Some("fsqlite".to_owned()),
            artifact_hash: None,
            context: BTreeMap::new(),
        });
    }

    // Phase 3: Validate with pass/fail
    events.push(LogEventSchema {
        run_id: run_id.clone(),
        timestamp: "2026-02-13T09:00:05.000Z".to_owned(),
        phase: LogPhase::Validate,
        event_type: LogEventType::Pass,
        scenario_id: Some("MVCC-3".to_owned()),
        seed: Some(SEED),
        backend: Some("fsqlite".to_owned()),
        artifact_hash: Some(
            "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned(),
        ),
        context: BTreeMap::new(),
    });

    // Phase 3b: First divergence detected
    events.push(LogEventSchema {
        run_id: run_id.clone(),
        timestamp: "2026-02-13T09:00:06.000Z".to_owned(),
        phase: LogPhase::Validate,
        event_type: LogEventType::FirstDivergence,
        scenario_id: Some("WAL-1".to_owned()),
        seed: Some(SEED),
        backend: Some("both".to_owned()),
        artifact_hash: None,
        context: {
            let mut ctx = BTreeMap::new();
            ctx.insert("divergence_point".to_owned(), "row 42 column 3".to_owned());
            ctx.insert(
                "artifact_paths".to_owned(),
                "/data/projects/frankensqlite/test-results/wal_divergence.json".to_owned(),
            );
            ctx
        },
    });

    // Phase 4: Teardown
    events.push(LogEventSchema {
        run_id: run_id.clone(),
        timestamp: "2026-02-13T09:00:07.000Z".to_owned(),
        phase: LogPhase::Teardown,
        event_type: LogEventType::Info,
        scenario_id: Some("INFRA-6".to_owned()),
        seed: Some(SEED),
        backend: None,
        artifact_hash: None,
        context: BTreeMap::new(),
    });

    // Phase 5: Report with artifact
    events.push(LogEventSchema {
        run_id: run_id.clone(),
        timestamp: "2026-02-13T09:00:08.000Z".to_owned(),
        phase: LogPhase::Report,
        event_type: LogEventType::ArtifactGenerated,
        scenario_id: Some("INFRA-6".to_owned()),
        seed: Some(SEED),
        backend: None,
        artifact_hash: Some(
            "deadbeefcafebabedeadbeefcafebabedeadbeefcafebabedeadbeefcafebabe".to_owned(),
        ),
        context: {
            let mut ctx = BTreeMap::new();
            ctx.insert(
                "artifact_paths".to_owned(),
                "/data/projects/frankensqlite/test-results/report.jsonl".to_owned(),
            );
            ctx
        },
    });

    events
}

#[test]
fn e2e_full_pipeline_realistic_stream() {
    let events = build_realistic_event_stream();

    // Step 1: Encode to JSONL
    let jsonl = encode_jsonl_stream(&events).expect("bead_id={BEAD_ID} case=encode");
    assert!(!jsonl.is_empty(), "bead_id={BEAD_ID} case=encode_nonempty");

    // Step 2: Decode from JSONL
    let decoded = decode_jsonl_stream(&jsonl);
    assert!(
        decoded.is_clean(),
        "bead_id={BEAD_ID} case=decode_clean errors={}",
        decoded.error_count(),
    );
    assert_eq!(
        decoded.event_count(),
        events.len(),
        "bead_id={BEAD_ID} case=decode_count",
    );

    // Step 3: Validate against schema
    let report = validate_event_stream(&decoded.events);
    assert!(
        report.passed,
        "bead_id={BEAD_ID} case=validate_pass summary={}",
        report.render_summary(),
    );
    assert_eq!(report.schema_version, LOG_SCHEMA_VERSION);
    assert_eq!(report.stats.total_events, events.len());
    assert_eq!(report.stats.valid_events, events.len());
    assert_eq!(report.stats.unique_run_ids, 1);

    // Verify all phases observed
    assert!(
        report.stats.phases_observed.len() >= 4,
        "bead_id={BEAD_ID} case=phases_observed expected>=4 got={}",
        report.stats.phases_observed.len(),
    );

    // Step 4: Redact
    let salt = SEED;
    let redacted: Vec<LogEventSchema> = decoded
        .events
        .iter()
        .map(|e| redact_event(e, salt))
        .collect();

    // Step 5: Verify redacted events still validate
    let redacted_report = validate_event_stream(&redacted);
    assert!(
        redacted_report.passed,
        "bead_id={BEAD_ID} case=redacted_validate summary={}",
        redacted_report.render_summary(),
    );

    // Step 6: Verify round-trip consistency
    let roundtrip_result = verify_roundtrip(&decoded.events);
    assert!(
        roundtrip_result.is_ok(),
        "bead_id={BEAD_ID} case=roundtrip error={}",
        roundtrip_result.unwrap_err(),
    );

    // Step 7: Verify redacted round-trip
    let redacted_roundtrip = verify_roundtrip(&redacted);
    assert!(
        redacted_roundtrip.is_ok(),
        "bead_id={BEAD_ID} case=redacted_roundtrip error={}",
        redacted_roundtrip.unwrap_err(),
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=report event_type=pass \
         run_id={BEAD_ID}-20260213T090000Z-{SEED} seed={SEED} \
         total_events={} valid={} phases={} event_types={} \
         schema_version={LOG_SCHEMA_VERSION} result=PASS",
        report.stats.total_events,
        report.stats.valid_events,
        report.stats.phases_observed.len(),
        report.stats.event_types_observed.len(),
    );
}

#[test]
fn e2e_redaction_preserves_safe_fields() {
    let events = build_realistic_event_stream();
    let salt = 42_u64;

    for event in &events {
        let redacted = redact_event(event, salt);

        // All safe fields must be identical
        assert_eq!(
            redacted.run_id, event.run_id,
            "bead_id={BEAD_ID} case=redact_run_id",
        );
        assert_eq!(
            redacted.timestamp, event.timestamp,
            "bead_id={BEAD_ID} case=redact_timestamp",
        );
        assert_eq!(
            redacted.phase, event.phase,
            "bead_id={BEAD_ID} case=redact_phase",
        );
        assert_eq!(
            redacted.event_type, event.event_type,
            "bead_id={BEAD_ID} case=redact_event_type",
        );
        assert_eq!(
            redacted.scenario_id, event.scenario_id,
            "bead_id={BEAD_ID} case=redact_scenario_id",
        );
        assert_eq!(
            redacted.seed, event.seed,
            "bead_id={BEAD_ID} case=redact_seed",
        );

        // invariant_ids (safe) must be preserved
        if let Some(original) = event.context.get("invariant_ids") {
            let redacted_val = redacted
                .context
                .get("invariant_ids")
                .expect("invariant_ids should be in redacted context");
            assert_eq!(
                redacted_val, original,
                "bead_id={BEAD_ID} case=redact_invariant_ids_preserved",
            );
        }

        // artifact_paths (internal) must be redacted to basename
        if let Some(_original) = event.context.get("artifact_paths") {
            let redacted_val = redacted
                .context
                .get("artifact_paths")
                .expect("artifact_paths should be in redacted context");
            assert!(
                !redacted_val.contains("/data/projects"),
                "bead_id={BEAD_ID} case=redact_artifact_paths directory should be stripped",
            );
        }
    }

    eprintln!(
        "bead_id={BEAD_ID} phase=validate event_type=pass \
         run_id={BEAD_ID}-redaction-{SEED} seed={SEED} \
         events_checked={} result=PASS",
        events.len(),
    );
}

#[test]
fn e2e_redaction_determinism() {
    let events = build_realistic_event_stream();
    let salt = 999_u64;

    let redacted_1: Vec<LogEventSchema> = events.iter().map(|e| redact_event(e, salt)).collect();
    let redacted_2: Vec<LogEventSchema> = events.iter().map(|e| redact_event(e, salt)).collect();

    let jsonl_1 = encode_jsonl_stream(&redacted_1).unwrap();
    let jsonl_2 = encode_jsonl_stream(&redacted_2).unwrap();

    assert_eq!(
        jsonl_1, jsonl_2,
        "bead_id={BEAD_ID} case=determinism redaction must be deterministic",
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=validate event_type=pass \
         run_id={BEAD_ID}-determinism-{SEED} seed={SEED} \
         jsonl_len={} result=PASS",
        jsonl_1.len(),
    );
}

#[test]
fn e2e_validation_diagnostics_ci_output() {
    // Create a stream with intentional quality issues to verify CI diagnostics
    let events = vec![
        // Good event
        LogEventSchema {
            run_id: "bd-1dp9.7.6-20260213T090000Z-1".to_owned(),
            timestamp: "2026-02-13T09:00:00Z".to_owned(),
            phase: LogPhase::Setup,
            event_type: LogEventType::Start,
            scenario_id: Some("INFRA-6".to_owned()),
            seed: Some(1),
            backend: None,
            artifact_hash: None,
            context: BTreeMap::new(),
        },
        // Missing seed for fail event (should be error)
        LogEventSchema {
            run_id: "bd-1dp9.7.6-20260213T090000Z-1".to_owned(),
            timestamp: "2026-02-13T09:00:01Z".to_owned(),
            phase: LogPhase::Validate,
            event_type: LogEventType::Fail,
            scenario_id: Some("MVCC-3".to_owned()),
            seed: None,
            backend: None,
            artifact_hash: None,
            context: BTreeMap::new(),
        },
    ];

    let report = validate_event_stream(&events);

    // Should fail due to missing seed for fail event
    assert!(
        !report.passed,
        "bead_id={BEAD_ID} case=ci_diagnostics should fail with missing seed for fail event",
    );

    // Should have errors
    let errors: Vec<_> = report
        .diagnostics
        .iter()
        .filter(|d| d.severity == DiagnosticSeverity::Error)
        .collect();
    assert!(
        !errors.is_empty(),
        "bead_id={BEAD_ID} case=ci_errors should have errors",
    );

    // Verify render_summary produces parseable CI output
    let summary = report.render_summary();
    assert!(
        summary.contains("FAIL"),
        "bead_id={BEAD_ID} case=ci_summary should contain FAIL",
    );
    assert!(
        summary.contains("Diagnostics:"),
        "bead_id={BEAD_ID} case=ci_diagnostics_section should have diagnostics section",
    );

    // Verify JSON serialization for CI artifact publishing
    let json = serde_json::to_string_pretty(&report).expect("report should serialize to JSON");
    assert!(
        json.contains("bd-1dp9.7.6"),
        "bead_id={BEAD_ID} case=ci_json should contain bead_id",
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=validate event_type=pass \
         run_id={BEAD_ID}-ci-diagnostics-{SEED} seed={SEED} \
         error_count={} warning_count={} result=PASS",
        report.stats.error_count, report.stats.warning_count,
    );
}

#[test]
fn e2e_field_sensitivity_policy_complete() {
    // Verify all standard schema fields have explicit sensitivity classifications
    let classifications = log_schema_validator::build_field_classifications();

    // Required fields must all be Safe
    for field in fsqlite_harness::e2e_log_schema::REQUIRED_EVENT_FIELDS {
        let c = log_schema_validator::classify_field(field);
        assert_eq!(
            c.sensitivity,
            FieldSensitivity::Safe,
            "bead_id={BEAD_ID} case=field_policy required field '{field}' must be Safe",
        );
    }

    // Known context keys must have explicit classifications
    let known_context_keys = ["context.invariant_ids", "context.artifact_paths"];
    for key in known_context_keys {
        let c = log_schema_validator::classify_field(key);
        assert_ne!(
            c.field_name, "context.*",
            "bead_id={BEAD_ID} case=field_policy known key '{key}' should have explicit classification",
        );
    }

    // Unknown context keys must default to Sensitive
    let c = log_schema_validator::classify_field("context.unknown_field");
    assert_eq!(
        c.sensitivity,
        FieldSensitivity::Sensitive,
        "bead_id={BEAD_ID} case=field_policy unknown context key should be Sensitive",
    );

    eprintln!(
        "bead_id={BEAD_ID} phase=validate event_type=pass \
         run_id={BEAD_ID}-field-policy-{SEED} seed={SEED} \
         classifications={} result=PASS",
        classifications.len(),
    );
}
