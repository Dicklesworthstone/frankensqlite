//! Golden/contract tests for bd-mblr.5.4.1 — log quality and field completeness.
//!
//! These tests enforce three guarantees:
//! 1. Required schema fields and value contracts remain intact.
//! 2. Event ordering/correlation remains reproducible (run_id + event_sequence).
//! 3. Failure events contain actionable triage context (reason + replay + artifacts).
//!
//! Coverage spans both runner entrypoints:
//! - `e2e-runner` via `e2e_logging_init`
//! - `realdb-e2e` via `realdb_e2e_logging`

use std::collections::BTreeSet;

use fsqlite_harness::e2e_log_schema::{LogEventSchema, LogEventType, LogPhase};
use fsqlite_harness::e2e_logging_init::{
    E2eLoggingConfig, RunContext, emit_completion_event, emit_phase_event,
    init_e2e_logging_with_context,
};
use fsqlite_harness::log_schema_validator::{
    DecodedLine, DiagnosticSeverity, decode_jsonl_stream, validate_event_stream,
};
use fsqlite_harness::realdb_e2e_logging::{
    RealdbCommand, RealdbCommandMeta, emit_command_completion, emit_command_event,
    init_realdb_logging,
};

const BEAD_ID: &str = "bd-mblr.5.4.1";
const SEED: u64 = 20260213;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ContractViolation {
    path: String,
    message: String,
}

fn run_context(run_id: &str) -> RunContext {
    RunContext::new(
        run_id,
        BEAD_ID,
        Some("INFRA-6"),
        Some(SEED),
        Some("fsqlite"),
    )
}

fn test_config() -> E2eLoggingConfig {
    E2eLoggingConfig::test()
}

fn add_failure_context(event: &mut LogEventSchema, reason: &str) {
    event
        .context
        .insert("failure_reason".to_owned(), reason.to_owned());
    event.context.insert(
        "replay_command".to_owned(),
        format!(
            "cargo test -p fsqlite-harness --test bd_mblr_5_4_1_log_quality_golden -- --exact {reason}"
        ),
    );
    event.context.insert(
        "artifact_paths".to_owned(),
        format!("/tmp/{BEAD_ID}/events.jsonl,/tmp/{BEAD_ID}/failure_bundle.json"),
    );
}

fn validate_log_contract(events: &[LogEventSchema]) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    if events.is_empty() {
        violations.push(ContractViolation {
            path: "stream".to_owned(),
            message: "stream must contain at least one event".to_owned(),
        });
        return violations;
    }

    let base_report = validate_event_stream(events);
    for diag in base_report
        .diagnostics
        .iter()
        .filter(|diag| diag.severity == DiagnosticSeverity::Error)
    {
        let path = match diag.field.as_deref() {
            Some(field) => format!("event[{}].{field}", diag.event_index),
            None => format!("event[{}]", diag.event_index),
        };
        violations.push(ContractViolation {
            path,
            message: diag.message.clone(),
        });
    }

    if events[0].phase != LogPhase::Setup {
        violations.push(ContractViolation {
            path: "event[0].phase".to_owned(),
            message: "first event must be setup phase".to_owned(),
        });
    }
    if events[0].event_type != LogEventType::Start {
        violations.push(ContractViolation {
            path: "event[0].event_type".to_owned(),
            message: "first event must be start event".to_owned(),
        });
    }

    if !events.iter().any(|event| event.phase == LogPhase::Report) {
        violations.push(ContractViolation {
            path: "stream".to_owned(),
            message: "stream must include at least one report-phase event".to_owned(),
        });
    }

    let root_run_id = events[0].run_id.clone();
    let mut previous_sequence: Option<u64> = None;

    for (index, event) in events.iter().enumerate() {
        if event.run_id != root_run_id {
            violations.push(ContractViolation {
                path: format!("event[{index}].run_id"),
                message: format!("run_id mismatch with stream correlation root '{root_run_id}'"),
            });
        }

        match event.context.get("event_sequence") {
            Some(raw_sequence) => match raw_sequence.parse::<u64>() {
                Ok(sequence) => {
                    if let Some(previous) = previous_sequence {
                        if sequence <= previous {
                            violations.push(ContractViolation {
                                path: format!("event[{index}].context.event_sequence"),
                                message: format!(
                                    "event_sequence must be strictly increasing: previous={previous}, current={sequence}"
                                ),
                            });
                        }
                    }
                    previous_sequence = Some(sequence);
                }
                Err(parse_error) => violations.push(ContractViolation {
                    path: format!("event[{index}].context.event_sequence"),
                    message: format!("invalid u64 event_sequence: {parse_error}"),
                }),
            },
            None => violations.push(ContractViolation {
                path: format!("event[{index}].context.event_sequence"),
                message: "missing required correlation field event_sequence".to_owned(),
            }),
        }

        if matches!(
            event.event_type,
            LogEventType::Fail | LogEventType::Error | LogEventType::FirstDivergence
        ) {
            for key in ["failure_reason", "replay_command", "artifact_paths"] {
                let missing = event
                    .context
                    .get(key)
                    .is_none_or(|value| value.trim().is_empty());
                if missing {
                    violations.push(ContractViolation {
                        path: format!("event[{index}].context.{key}"),
                        message: "missing required failure context field".to_owned(),
                    });
                }
            }
        }

        if event.event_type == LogEventType::ArtifactGenerated {
            if event.artifact_hash.is_none() {
                violations.push(ContractViolation {
                    path: format!("event[{index}].artifact_hash"),
                    message: "artifact_generated events must include artifact_hash".to_owned(),
                });
            }
            let missing_paths = event
                .context
                .get("artifact_paths")
                .is_none_or(|value| value.trim().is_empty());
            if missing_paths {
                violations.push(ContractViolation {
                    path: format!("event[{index}].context.artifact_paths"),
                    message: "artifact_generated events must include artifact_paths".to_owned(),
                });
            }
        }

        if let Some(paths) = event.context.get("artifact_paths") {
            let has_non_empty_path = paths.split(',').any(|value| !value.trim().is_empty());
            if !has_non_empty_path {
                violations.push(ContractViolation {
                    path: format!("event[{index}].context.artifact_paths"),
                    message: "artifact_paths must contain at least one non-empty path".to_owned(),
                });
            }
        }
    }

    violations.sort();
    violations.dedup();
    violations
}

fn validate_jsonl_contract(jsonl: &str) -> Vec<ContractViolation> {
    let decoded = decode_jsonl_stream(jsonl);
    let mut violations = Vec::new();

    for decoded_line in &decoded.errors {
        if let DecodedLine::Error {
            line_index, error, ..
        } = decoded_line
        {
            violations.push(ContractViolation {
                path: format!("line[{line_index}]"),
                message: error.clone(),
            });
        }
    }

    violations.extend(validate_log_contract(&decoded.events));
    violations.sort();
    violations.dedup();
    violations
}

fn build_e2e_runner_success_events() -> Vec<LogEventSchema> {
    let context = run_context("bd-mblr.5.4.1-e2e-success");
    let init = init_e2e_logging_with_context(context.clone(), &test_config());

    vec![
        init.startup_event,
        emit_phase_event(
            &context,
            LogPhase::Execute,
            LogEventType::Info,
            "running e2e smoke suite",
        ),
        emit_completion_event(&context, true, 3, 3, 0),
    ]
}

fn build_e2e_runner_failure_events() -> Vec<LogEventSchema> {
    let context = run_context("bd-mblr.5.4.1-e2e-failure");
    let init = init_e2e_logging_with_context(context.clone(), &test_config());

    let mut execute_error = emit_phase_event(
        &context,
        LogPhase::Execute,
        LogEventType::Error,
        "differential mismatch",
    );
    add_failure_context(&mut execute_error, "e2e_execute_error");

    let mut completion_fail = emit_completion_event(&context, false, 3, 2, 1);
    add_failure_context(&mut completion_fail, "e2e_completion_fail");

    vec![init.startup_event, execute_error, completion_fail]
}

fn build_realdb_success_events() -> Vec<LogEventSchema> {
    let context = run_context("bd-mblr.5.4.1-realdb-success");
    let meta = RealdbCommandMeta::for_command(RealdbCommand::Run)
        .with_db_path("sample_sqlite_db_files/golden/project.db")
        .with_page_size(4096);
    let init = init_realdb_logging(context.clone(), &test_config(), &meta);

    vec![
        init.core.startup_event,
        init.command_startup_event,
        emit_command_event(
            &context,
            RealdbCommand::Run,
            LogEventType::Info,
            "running realdb scenario",
        ),
        emit_command_completion(
            &context,
            RealdbCommand::Run,
            true,
            &[("scenarios", "8"), ("failures", "0")],
        ),
    ]
}

fn build_realdb_failure_events() -> Vec<LogEventSchema> {
    let context = run_context("bd-mblr.5.4.1-realdb-failure");
    let meta = RealdbCommandMeta::for_command(RealdbCommand::Compare)
        .with_db_path("sample_sqlite_db_files/golden/project.db")
        .with_page_size(4096);
    let init = init_realdb_logging(context.clone(), &test_config(), &meta);

    let mut comparison_error = emit_command_event(
        &context,
        RealdbCommand::Compare,
        LogEventType::Error,
        "first divergence detected",
    );
    add_failure_context(&mut comparison_error, "realdb_compare_error");

    let mut completion_fail = emit_command_completion(
        &context,
        RealdbCommand::Compare,
        false,
        &[("mismatches", "2")],
    );
    add_failure_context(&mut completion_fail, "realdb_compare_completion_fail");

    vec![
        init.core.startup_event,
        init.command_startup_event,
        comparison_error,
        completion_fail,
    ]
}

#[test]
fn e2e_runner_contract_accepts_success_and_failure_flows() {
    let success_violations = validate_log_contract(&build_e2e_runner_success_events());
    assert!(
        success_violations.is_empty(),
        "bead_id={BEAD_ID} case=e2e_success violations={success_violations:#?}",
    );

    let failure_violations = validate_log_contract(&build_e2e_runner_failure_events());
    assert!(
        failure_violations.is_empty(),
        "bead_id={BEAD_ID} case=e2e_failure violations={failure_violations:#?}",
    );
}

#[test]
fn realdb_runner_contract_accepts_success_and_failure_flows() {
    let success_violations = validate_log_contract(&build_realdb_success_events());
    assert!(
        success_violations.is_empty(),
        "bead_id={BEAD_ID} case=realdb_success violations={success_violations:#?}",
    );

    let failure_violations = validate_log_contract(&build_realdb_failure_events());
    assert!(
        failure_violations.is_empty(),
        "bead_id={BEAD_ID} case=realdb_failure violations={failure_violations:#?}",
    );
}

#[test]
fn golden_contract_reports_precise_field_paths_for_broken_stream() {
    let mut events = build_e2e_runner_failure_events();

    events[0].timestamp = "invalid-timestamp".to_owned();
    events[1].run_id = "wrong-run-id".to_owned();
    events[1].context.remove("event_sequence");
    events[1].context.remove("replay_command");

    let violations = validate_log_contract(&events);
    let paths: BTreeSet<String> = violations.iter().map(|item| item.path.clone()).collect();

    let expected_paths = BTreeSet::from([
        "event[0].timestamp".to_owned(),
        "event[1].run_id".to_owned(),
        "event[1].context.event_sequence".to_owned(),
        "event[1].context.replay_command".to_owned(),
    ]);

    for expected in &expected_paths {
        assert!(
            paths.contains(expected),
            "bead_id={BEAD_ID} case=missing_expected_path expected={expected} actual={paths:?}",
        );
    }
}

#[test]
fn jsonl_contract_pinpoints_invalid_value_type_with_line_index() {
    let jsonl = concat!(
        r#"{"run_id":"bd-mblr.5.4.1-jsonl-1","timestamp":"2026-02-13T09:00:00.000Z","phase":"Setup","event_type":"Start","scenario_id":"INFRA-6","seed":"not-a-number","backend":"fsqlite","artifact_hash":null,"context":{"event_sequence":"0"}}"#,
        "\n",
        r#"{"run_id":"bd-mblr.5.4.1-jsonl-1","timestamp":"2026-02-13T09:00:01.000Z","phase":"Report","event_type":"Pass","scenario_id":"INFRA-6","seed":20260213,"backend":"fsqlite","artifact_hash":null,"context":{"event_sequence":"1"}}"#,
        "\n",
    );

    let violations = validate_jsonl_contract(jsonl);
    assert!(
        violations
            .iter()
            .any(|violation| violation.path == "line[0]"
                && violation.message.contains("invalid type")),
        "bead_id={BEAD_ID} case=jsonl_type_path violations={violations:#?}",
    );
}
