#![recursion_limit = "512"]

//! Temporary GH#533 stage diagnostics, shared verbatim by baseline/candidate.
//! Public metrics only. This is an instrumented diagnostic, not a release
//! benchmark; its cumulative timers can overlap and must not be summed.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_parser::{parse_metrics_snapshot, reset_parse_metrics, set_parse_metrics_enabled};
use serde_json::{Value, json};
use std::error::Error;
use std::io::{self, Write};
use std::time::Instant;

struct Case {
    name: &'static str,
    sql: String,
    expected: i64,
    reported_size: bool,
}

fn process_rss() -> Value {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return Value::Null;
    };
    let read = |name: &str| {
        status.lines().find_map(|line| {
            line.strip_prefix(name)?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })
    };
    json!({"rss_kib": read("VmRSS:"), "process_peak_rss_kib": read("VmHWM:")})
}

async fn run_case(case: &Case, label: &str) -> Result<Value, Box<dyn Error>> {
    // Exclude opening and closing the connection from the query's stage data.
    set_hot_path_profile_enabled(false);
    set_parse_metrics_enabled(false);
    let connection = Connection::open(":memory:").await?;
    connection.set_fallback_capture_enabled(true);
    connection.reset_fallback_decision_evidence();
    reset_hot_path_profile();
    reset_parse_metrics();
    let rss_before = process_rss();
    set_hot_path_profile_enabled(true);
    set_parse_metrics_enabled(true);
    let started = Instant::now();
    let result = connection.query(&case.sql).await;
    let query_wall_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    set_parse_metrics_enabled(false);
    set_hot_path_profile_enabled(false);
    let profile = hot_path_profile_snapshot();
    let parses = parse_metrics_snapshot();
    let fallback = connection.fallback_decision_snapshot();
    let execution = connection.fallback_execution_snapshot();
    let rss_after = process_rss();
    let correct = result.as_ref().is_ok_and(|rows| {
        rows.len() == 1
            && rows[0].values().len() == 1
            && rows[0].values()[0].as_integer() == Some(case.expected)
    });
    let rows = result.as_ref().ok().map(|rows| {
        rows.iter()
            .take(4)
            .map(|row| {
                row.values()
                    .iter()
                    .map(|value| format!("{value:?}"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    });
    let error = result.as_ref().err().map(ToString::to_string);
    let decisions = fallback
        .decisions
        .iter()
        .map(|decision| {
            json!({
                "statement_kind": decision.statement_kind,
                "boundary": decision.fallback_boundary,
                "reason": decision.decision_reason,
                "outcome": decision.decision_outcome,
                "source_touchpoint": decision.source_touchpoint,
                "occurrences": decision.occurrences,
                "first_failure": decision.first_failure_diagnostic,
            })
        })
        .collect::<Vec<_>>();
    let events = execution
        .events
        .iter()
        .map(|event| {
            json!({
                "event_id": event.event_id,
                "statement_id": event.statement_id,
                "statement_kind": event.statement_kind,
                "reason": event.decision_reason,
                "outcome": event.decision_outcome,
                "source_context": event.source_context,
                "storage_backend": event.storage_backend,
            })
        })
        .collect::<Vec<_>>();
    let mut report = json!({
        "schema": "issue533-stage-probe-v1",
        "label": label,
        "probe_binary": env!("CARGO_PKG_NAME"),
        "source_revision": option_env!("ISSUE533_SOURCE_REVISION").unwrap_or("unspecified"),
        "instrumented": true,
        "release_performance_measurement": false,
        "timers_overlap": true,
        "instrumentation_can_affect_execution_lanes": true,
        "fresh_connection": true,
        "database": ":memory:",
        "case": case.name,
        "sql": case.sql,
        "reported_size": case.reported_size,
        "expected_integer": case.expected,
        "correct": correct,
        "result_rows": rows,
        "error": error,
        "query_wall_ns": query_wall_ns,
        "rss_before_query": rss_before,
        "rss_after_query_before_close": rss_after,
        "rss_note": "VmHWM is cumulative for this process; use one --case per process for per-case peaks. RSS includes profiling overhead.",
        "parser": {
            "parsed_statements": parses.fsqlite_parse_statements_total,
            "parsed_tokens": parses.fsqlite_parse_tokens_total,
            "parse_errors": parses.fsqlite_parse_errors_total,
            "parse_single_calls": profile.parser.parse_single_calls,
            "parse_multi_calls": profile.parser.parse_multi_calls,
            "parse_cache_hits": profile.parser.parse_cache_hits,
            "parse_cache_misses": profile.parser.parse_cache_misses,
            "parsed_sql_bytes": profile.parser.parsed_sql_bytes,
            "parse_time_ns": profile.parser.parse_time_ns,
            "rewrite_calls": profile.parser.rewrite_calls,
            "rewrite_time_ns": profile.parser.rewrite_time_ns,
            "compile_time_ns": profile.parser.compile_time_ns,
            "compiled_cache_hits": profile.parser.compiled_cache_hits,
            "compiled_cache_misses": profile.parser.compiled_cache_misses,
            "prepared_cache_hits": profile.parser.prepared_cache_hits,
            "prepared_cache_misses": profile.parser.prepared_cache_misses,
            "fast_path_executions": profile.parser.fast_path_executions,
            "slow_path_executions": profile.parser.slow_path_executions,
        },
        "execution_scopes": {
            "execute_body_time_ns": profile.execute_body_time_ns,
            "begin_setup_time_ns": profile.begin_setup_time_ns,
            "background_status_time_ns": profile.background_status_time_ns,
            "background_status_checks": profile.background_status_checks,
            "statement_dispatch_background_gates": profile.statement_dispatch_background_gates,
            "connection_snapshots": profile.connection_snapshots,
            "prepared_lookup_time_ns": profile.prepared_lookup_time_ns,
            "prepared_schema_refresh_time_ns": profile.prepared_schema_refresh_time_ns,
            "memdb_refresh_count": profile.memdb_refresh_count,
            "memdb_pager_reloads": profile.memdb_pager_reloads,
            "arena_alloc_bytes": profile.arena_alloc_bytes,
        },
        "vdbe": {
            "opcodes_executed": profile.vdbe.opcodes_executed_total,
            "statements": profile.vdbe.statements_total,
            "statement_duration_us": profile.vdbe.statement_duration_us_total,
            "sort_rows": profile.vdbe.sort_rows_total,
            "sort_spill_pages": profile.vdbe.sort_spill_pages_total,
            "column_reads": profile.vdbe.column_reads_total,
            "record_decode_calls": profile.vdbe.record_decode_calls_total,
            "decoded_values": profile.vdbe.decoded_values_total,
            "decoded_value_heap_bytes": profile.vdbe.decoded_value_heap_bytes_total,
            "result_rows": profile.vdbe.result_rows_total,
            "result_values": profile.vdbe.result_values_total,
        },
        "btree_copy_kernels": profile.btree_copy_kernels,
        "record_decode": profile.record_decode,
        "fallback_decisions": decisions,
        "fallback_decisions_truncated": fallback.truncated,
        "recent_fallback_events": events,
        "fallback_events_dropped": execution.dropped_events,
        "full_hot_path_profile_debug": format!("{profile:#?}"),
    });
    report["close_error"] = match connection.close().await {
        Ok(()) => Value::Null,
        Err(error) => json!(error.to_string()),
    };
    Ok(report)
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut selected = "all".to_owned();
    let mut label = env!("CARGO_PKG_NAME").to_owned();
    let mut count_rows = 1_000_000_i64;
    let mut exists_rows = 10_000_i64;
    let mut args = std::env::args().skip(1);
    while let Some(option) = args.next() {
        if option == "--help" {
            println!(
                "--case all|direct_count|cte_count|cte_exists|direct_exists --count-rows N --exists-rows N --label LABEL"
            );
            return Ok(());
        }
        let value = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("missing value for {option}"),
            )
        })?;
        match option.as_str() {
            "--case" => selected = value,
            "--label" => label = value,
            "--count-rows" => count_rows = value.parse()?,
            "--exists-rows" => exists_rows = value.parse()?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown option {option}"),
                )
                .into());
            }
        }
    }
    if count_rows < 0 || exists_rows < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "row counts must be nonnegative",
        )
        .into());
    }
    let cases = [
        Case {
            name: "direct_count",
            sql: format!("SELECT count(*) FROM generate_series(1,{count_rows});"),
            expected: count_rows,
            reported_size: count_rows == 1_000_000,
        },
        Case {
            name: "cte_count",
            sql: format!(
                "WITH t AS (SELECT value FROM generate_series(1,{count_rows})) SELECT count(*) FROM t;"
            ),
            expected: count_rows,
            reported_size: count_rows == 1_000_000,
        },
        Case {
            name: "cte_exists",
            sql: format!(
                "WITH t AS (SELECT value FROM generate_series(1,{exists_rows})) SELECT count(*) FROM t WHERE EXISTS(SELECT 1 FROM t t1 WHERE t.value=t1.value);"
            ),
            expected: exists_rows,
            reported_size: exists_rows == 10_000,
        },
        Case {
            name: "direct_exists",
            sql: format!(
                "SELECT count(*) FROM generate_series(1,{exists_rows})t WHERE EXISTS(SELECT 1 FROM generate_series(1,{exists_rows}) t1 WHERE t.value=t1.value);"
            ),
            expected: exists_rows,
            reported_size: exists_rows == 10_000,
        },
    ];
    if selected != "all" && !cases.iter().any(|case| case.name == selected) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown case {selected}"),
        )
        .into());
    }
    let mut correct = true;
    for case in cases
        .iter()
        .filter(|case| selected == "all" || selected == case.name)
    {
        let report = futures_lite::future::block_on(run_case(case, &label))?;
        correct &= report["correct"] == true && report["close_error"].is_null();
        println!("{}", serde_json::to_string(&report)?);
        io::stdout().flush()?;
    }
    if !correct {
        return Err(io::Error::other(
            "one or more query results or closes failed; see JSON output",
        )
        .into());
    }
    Ok(())
}
