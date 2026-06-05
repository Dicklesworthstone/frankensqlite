use std::collections::BTreeSet;

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const FALLBACK_BOUNDARY_INVENTORY: &str =
    include_str!("../../../docs/contracts/fallback_boundary_inventory.toml");
const CONNECTION_SOURCE: &str = include_str!("../src/connection.rs");
const E2E_EXECUTOR_SOURCE: &str = include_str!("../../fsqlite-e2e/src/fsqlite_executor.rs");
const VDBE_ENGINE_SOURCE: &str = include_str!("../../fsqlite-vdbe/src/engine.rs");
const INVENTORY_REQUIRED_FIELDS: &[&str] = &[
    "boundary_id",
    "boundary_family",
    "statement_kind",
    "trigger_condition",
    "decision_reason",
    "source_file",
    "source_touchpoint",
    "source_anchor",
    "certifying_policy",
    "non_cert_policy",
    "expected_backend_identity",
    "decision_outcome",
    "first_failure_diag",
    "owner_bead",
    "covered_by_test",
    "notes",
];
const REQUIRED_BOUNDARY_IDS: &[&str] = &[
    "conn.select.with_clause_materialization",
    "conn.select.view_materialization",
    "conn.select.sqlite_schema_virtual_materialization",
    "conn.select.live_vtab_select_fallback",
    "conn.select.time_travel_snapshot",
    "conn.memdb.refresh_from_pager_publication",
    "conn.memdb.refresh_from_active_txn",
    "conn.prepared_update_delete.static_fallback_reason",
    "conn.virtual_table.live_vtab_scan",
    "conn.virtual_table.legacy_fts5_create_materialization",
    "e2e.file_backed_default_strict_parity",
    "vdbe.open_cursor.parity_cert_rejection",
];

struct FallbackEvent<'a> {
    event_seq: i64,
    statement_fingerprint: &'a str,
    plan_id: &'a str,
    table_name: &'a str,
    workload_lane: &'a str,
    fallback_surface: &'a str,
    fallback_reason: Option<&'a str>,
    supported_fast_path: bool,
    concurrency_impact: &'a str,
    durability_impact: &'a str,
    memory_impact: &'a str,
    latency_impact: &'a str,
    diagnostics_available: bool,
    first_failure_diag: &'a str,
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_optional_text(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), sql_text)
}

fn sql_bool(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}

fn install_fallback_schema(conn: &Connection) -> TestResult {
    conn.execute(
        "CREATE TABLE fsqlite_fallback_reason_codes_contract(
            reason_code TEXT NOT NULL PRIMARY KEY,
            fallback_surface TEXT NOT NULL,
            severity TEXT NOT NULL,
            retryable INTEGER NOT NULL,
            concurrency_impact TEXT NOT NULL,
            durability_impact TEXT NOT NULL,
            memory_impact TEXT NOT NULL,
            latency_impact TEXT NOT NULL,
            human_text TEXT NOT NULL
        );",
    )?;
    conn.execute(
        "CREATE TABLE fsqlite_fallback_events_contract(
            event_seq INTEGER NOT NULL PRIMARY KEY,
            event_ts_ms INTEGER NOT NULL,
            trace_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            scenario_id TEXT NOT NULL,
            statement_fingerprint TEXT NOT NULL,
            plan_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            workload_lane TEXT NOT NULL,
            fallback_surface TEXT NOT NULL,
            fallback_reason TEXT,
            supported_fast_path INTEGER NOT NULL,
            concurrency_impact TEXT NOT NULL,
            durability_impact TEXT NOT NULL,
            memory_impact TEXT NOT NULL,
            latency_impact TEXT NOT NULL,
            diagnostics_available INTEGER NOT NULL,
            first_failure_diag TEXT NOT NULL
        );",
    )?;
    conn.execute(
        "CREATE INDEX idx_fsqlite_fallback_events_contract_aggregate
            ON fsqlite_fallback_events_contract(
                statement_fingerprint,
                plan_id,
                table_name,
                workload_lane,
                fallback_reason
            );",
    )?;
    seed_fallback_reason_codes(conn)?;
    Ok(())
}

fn seed_fallback_reason_codes(conn: &Connection) -> TestResult {
    conn.execute(
        "INSERT INTO fsqlite_fallback_reason_codes_contract(
            reason_code,
            fallback_surface,
            severity,
            retryable,
            concurrency_impact,
            durability_impact,
            memory_impact,
            latency_impact,
            human_text
        ) VALUES
            (
                'unsupported_sql_shape',
                'planner',
                'notice',
                0,
                'may_reduce_parallelism',
                'unchanged',
                'bounded_extra_memory',
                'may_increase_latency',
                'Statement shape is not yet supported by the native fast path.'
            ),
            (
                'planner_bypass',
                'planner',
                'notice',
                0,
                'unchanged',
                'unchanged',
                'none',
                'may_increase_latency',
                'Planner deliberately bypassed a native lowering.'
            ),
            (
                'storage_fallback',
                'storage',
                'warning',
                1,
                'may_block_conflicting_pages',
                'unchanged',
                'bounded_extra_memory',
                'may_increase_latency',
                'Storage path used a compatibility fallback.'
            ),
            (
                'diagnostics_unavailable',
                'diagnostic',
                'warning',
                1,
                'unknown',
                'unknown',
                'unknown',
                'unknown',
                'Fallback classification was unavailable for this statement.'
            );",
    )?;
    Ok(())
}

fn record_fallback_event(conn: &Connection, event: FallbackEvent<'_>) -> TestResult {
    conn.execute(&format!(
        "INSERT INTO fsqlite_fallback_events_contract(
            event_seq,
            event_ts_ms,
            trace_id,
            run_id,
            scenario_id,
            statement_fingerprint,
            plan_id,
            table_name,
            workload_lane,
            fallback_surface,
            fallback_reason,
            supported_fast_path,
            concurrency_impact,
            durability_impact,
            memory_impact,
            latency_impact,
            diagnostics_available,
            first_failure_diag
        ) VALUES (
            {event_seq},
            {event_ts_ms},
            'trace-fallback-contract',
            'run-fallback-contract',
            'scenario-agent-swarm-fallback',
            {statement_fingerprint},
            {plan_id},
            {table_name},
            {workload_lane},
            {fallback_surface},
            {fallback_reason},
            {supported_fast_path},
            {concurrency_impact},
            {durability_impact},
            {memory_impact},
            {latency_impact},
            {diagnostics_available},
            {first_failure_diag}
        );",
        event_seq = event.event_seq,
        event_ts_ms = 1_000 + event.event_seq,
        statement_fingerprint = sql_text(event.statement_fingerprint),
        plan_id = sql_text(event.plan_id),
        table_name = sql_text(event.table_name),
        workload_lane = sql_text(event.workload_lane),
        fallback_surface = sql_text(event.fallback_surface),
        fallback_reason = sql_optional_text(event.fallback_reason),
        supported_fast_path = sql_bool(event.supported_fast_path),
        concurrency_impact = sql_text(event.concurrency_impact),
        durability_impact = sql_text(event.durability_impact),
        memory_impact = sql_text(event.memory_impact),
        latency_impact = sql_text(event.latency_impact),
        diagnostics_available = sql_bool(event.diagnostics_available),
        first_failure_diag = sql_text(event.first_failure_diag)
    ))?;
    trace_fallback_event(&event);
    Ok(())
}

fn trace_fallback_event(event: &FallbackEvent<'_>) {
    tracing::info!(
        target: "fsqlite.fallback_transparency_contract",
        statement_fingerprint = event.statement_fingerprint,
        plan_id = event.plan_id,
        table_name = event.table_name,
        workload_lane = event.workload_lane,
        fallback_surface = event.fallback_surface,
        fallback_reason = event.fallback_reason.unwrap_or("none"),
        supported_fast_path = event.supported_fast_path,
        concurrency_impact = event.concurrency_impact,
        durability_impact = event.durability_impact,
        memory_impact = event.memory_impact,
        latency_impact = event.latency_impact,
        diagnostics_available = event.diagnostics_available,
        first_failure_diag = event.first_failure_diag,
        "fallback transparency contract event"
    );
}

fn row_values(row: &Row) -> &[SqliteValue] {
    row.values()
}

fn inventory_boundary_sections() -> Vec<&'static str> {
    FALLBACK_BOUNDARY_INVENTORY
        .split("\n[[boundary]]")
        .skip(1)
        .collect()
}

fn inventory_field_value<'a>(section: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key} = ");
    section.lines().find_map(|line| {
        let raw = line.trim_start().strip_prefix(prefix.as_str())?.trim();
        raw.strip_prefix('"')?.strip_suffix('"')
    })
}

fn inventory_has_decision_reason(reason: &str) -> bool {
    FALLBACK_BOUNDARY_INVENTORY.contains(&format!("decision_reason = \"{reason}\""))
}

fn quoted_literals_after(source: &'static str, start: usize, max_len: usize) -> Vec<&'static str> {
    let end = source.len().min(start.saturating_add(max_len));
    let mut rest = &source[start..end];
    let mut literals = Vec::new();
    while let Some(open_quote) = rest.find('"') {
        let after_open = &rest[open_quote + 1..];
        let Some(close_quote) = after_open.find('"') else {
            break;
        };
        literals.push(&after_open[..close_quote]);
        rest = &after_open[close_quote + 1..];
    }
    literals
}

fn connection_second_argument_reasons(needle: &str) -> BTreeSet<&'static str> {
    let mut reasons = BTreeSet::new();
    for (index, _) in CONNECTION_SOURCE.match_indices(needle) {
        let literals = quoted_literals_after(CONNECTION_SOURCE, index, 320);
        if let Some(reason) = literals.get(1) {
            reasons.insert(*reason);
        }
    }
    reasons
}

fn connection_first_argument_reasons(needle: &str) -> BTreeSet<&'static str> {
    let mut reasons = BTreeSet::new();
    for (index, _) in CONNECTION_SOURCE.match_indices(needle) {
        let literals = quoted_literals_after(CONNECTION_SOURCE, index, 320);
        if let Some(reason) = literals.first() {
            reasons.insert(*reason);
        }
    }
    reasons
}

fn vdbe_open_storage_cursor_source() -> &'static str {
    let start = VDBE_ENGINE_SOURCE
        .find("fn open_storage_cursor(")
        .expect("VDBE source should define open_storage_cursor");
    let relative_end = VDBE_ENGINE_SOURCE[start..]
        .find("\n    fn trace_opcode(")
        .expect("VDBE source should define trace_opcode after open_storage_cursor");
    &VDBE_ENGINE_SOURCE[start..start + relative_end]
}

fn vdbe_open_storage_cursor_decision_reasons() -> BTreeSet<&'static str> {
    let source = vdbe_open_storage_cursor_source();
    let mut reasons = BTreeSet::new();
    for needle in ["decision_reason = \"", "mem_decision_reason = \""] {
        let mut rest = source;
        while let Some(offset) = rest.find(needle) {
            let after_prefix = &rest[offset + needle.len()..];
            let Some(close_quote) = after_prefix.find('"') else {
                break;
            };
            reasons.insert(&after_prefix[..close_quote]);
            rest = &after_prefix[close_quote + 1..];
        }
    }
    reasons
}

fn runtime_fallback_boundary_reasons() -> BTreeSet<&'static str> {
    let mut reasons = connection_second_argument_reasons("self.log_mem_execution_fallback(");
    reasons.extend(connection_second_argument_reasons(
        "self.disable_mem_fallback_rejection_for_internal_scope(",
    ));
    reasons.extend(connection_first_argument_reasons(
        "self.log_aggregate_window_storage_substrate_dispatch(",
    ));
    reasons.extend(connection_first_argument_reasons(
        "self.try_execute_group_by_storage_substrate(",
    ));
    reasons.extend([
        "join_vdbe_storage_cursors",
        "derived_source_flattened_vdbe_storage_cursors",
    ]);
    reasons.extend(vdbe_open_storage_cursor_decision_reasons());
    reasons
}

fn source_contains_anchor(source_file: &str, source_anchor: &str) -> bool {
    match source_file {
        "crates/fsqlite-core/src/connection.rs" => CONNECTION_SOURCE.contains(source_anchor),
        "crates/fsqlite-e2e/src/fsqlite_executor.rs" => E2E_EXECUTOR_SOURCE.contains(source_anchor),
        "crates/fsqlite-vdbe/src/engine.rs" => VDBE_ENGINE_SOURCE.contains(source_anchor),
        _ => false,
    }
}

fn fallback_count(conn: &Connection) -> TestResult<i64> {
    let rows = conn.query("SELECT count(*) FROM fsqlite_fallback_events_contract;")?;
    let row = rows.first().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "fallback event count query returned no rows",
        )
    })?;
    match row_values(row) {
        [SqliteValue::Integer(count)] => Ok(*count),
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("expected integer fallback event count, got {other:?}"),
        )
        .into()),
    }
}

#[test]
fn fallback_boundary_inventory_covers_runtime_reason_strings() {
    assert!(
        FALLBACK_BOUNDARY_INVENTORY.contains("logical_key = \"fallback_boundary_inventory.v1\""),
        "inventory must identify the stable contract key"
    );
    let sections = inventory_boundary_sections();
    assert!(
        sections.len() >= 30,
        "fallback inventory should cover connection and VDBE boundary families"
    );

    let mut boundary_ids = BTreeSet::new();
    for section in &sections {
        for field in INVENTORY_REQUIRED_FIELDS {
            assert!(
                section.contains(&format!("{field} = ")),
                "inventory boundary is missing required field {field}: {section}"
            );
        }

        let boundary_id = inventory_field_value(section, "boundary_id")
            .expect("boundary section should have boundary_id");
        assert!(
            boundary_ids.insert(boundary_id),
            "duplicate fallback boundary id {boundary_id}"
        );

        let source_file = inventory_field_value(section, "source_file")
            .expect("boundary section should have source_file");
        let source_anchor = inventory_field_value(section, "source_anchor")
            .expect("boundary section should have source_anchor");
        assert!(
            source_contains_anchor(source_file, source_anchor),
            "source anchor {source_anchor:?} was not found in {source_file:?}"
        );
    }

    for &boundary_id in REQUIRED_BOUNDARY_IDS {
        assert!(
            boundary_ids.contains(boundary_id),
            "required fallback boundary {boundary_id} is missing from inventory"
        );
    }

    for policy in [
        "certifying_policy = \"strict_deny\"",
        "certifying_policy = \"strict_real_backend_only\"",
        "certifying_policy = \"strict_refuse_invalid\"",
        "non_cert_policy = \"allow_with_event\"",
        "non_cert_policy = \"allow_transient_mem_read_only\"",
        "non_cert_policy = \"real_backend_only\"",
        "non_cert_policy = \"refuse_invalid\"",
    ] {
        assert!(
            FALLBACK_BOUNDARY_INVENTORY.contains(policy),
            "inventory should include policy assignment {policy}"
        );
    }

    let runtime_reasons = runtime_fallback_boundary_reasons();
    assert!(
        runtime_reasons.len() >= 25,
        "runtime reason scanner should find connection and VDBE decisions"
    );
    for reason in runtime_reasons {
        assert!(
            inventory_has_decision_reason(reason),
            "runtime fallback/storage decision reason {reason:?} is missing from inventory"
        );
    }
}

#[test]
fn fallback_reason_codes_are_stable_and_queryable() -> TestResult {
    let conn = Connection::open(":memory:")?;
    assert!(
        conn.is_concurrent_mode_default(),
        "fallback transparency must not disable concurrent-writer mode"
    );
    install_fallback_schema(&conn)?;

    let rows = conn.query(
        "SELECT reason_code, fallback_surface, severity, retryable,
                concurrency_impact, durability_impact, memory_impact,
                latency_impact
           FROM fsqlite_fallback_reason_codes_contract
          ORDER BY reason_code;",
    )?;
    assert_eq!(rows.len(), 4);
    assert_eq!(
        row_values(&rows[0]),
        &[
            SqliteValue::Text("diagnostics_unavailable".into()),
            SqliteValue::Text("diagnostic".into()),
            SqliteValue::Text("warning".into()),
            SqliteValue::Integer(1),
            SqliteValue::Text("unknown".into()),
            SqliteValue::Text("unknown".into()),
            SqliteValue::Text("unknown".into()),
            SqliteValue::Text("unknown".into()),
        ]
    );
    assert_eq!(
        row_values(&rows[1]),
        &[
            SqliteValue::Text("planner_bypass".into()),
            SqliteValue::Text("planner".into()),
            SqliteValue::Text("notice".into()),
            SqliteValue::Integer(0),
            SqliteValue::Text("unchanged".into()),
            SqliteValue::Text("unchanged".into()),
            SqliteValue::Text("none".into()),
            SqliteValue::Text("may_increase_latency".into()),
        ]
    );
    assert_eq!(
        row_values(&rows[2]),
        &[
            SqliteValue::Text("storage_fallback".into()),
            SqliteValue::Text("storage".into()),
            SqliteValue::Text("warning".into()),
            SqliteValue::Integer(1),
            SqliteValue::Text("may_block_conflicting_pages".into()),
            SqliteValue::Text("unchanged".into()),
            SqliteValue::Text("bounded_extra_memory".into()),
            SqliteValue::Text("may_increase_latency".into()),
        ]
    );
    assert_eq!(
        row_values(&rows[3]),
        &[
            SqliteValue::Text("unsupported_sql_shape".into()),
            SqliteValue::Text("planner".into()),
            SqliteValue::Text("notice".into()),
            SqliteValue::Integer(0),
            SqliteValue::Text("may_reduce_parallelism".into()),
            SqliteValue::Text("unchanged".into()),
            SqliteValue::Text("bounded_extra_memory".into()),
            SqliteValue::Text("may_increase_latency".into()),
        ]
    );

    Ok(())
}

#[test]
fn supported_fast_path_records_no_fallback_reason() -> TestResult {
    let conn = Connection::open(":memory:")?;
    assert!(
        conn.is_concurrent_mode_default(),
        "fallback transparency must not disable concurrent-writer mode"
    );
    install_fallback_schema(&conn)?;

    record_fallback_event(
        &conn,
        FallbackEvent {
            event_seq: 1,
            statement_fingerprint: "insert-fast-path",
            plan_id: "direct-insert-v1",
            table_name: "agent_jobs",
            workload_lane: "ingest",
            fallback_surface: "native",
            fallback_reason: None,
            supported_fast_path: true,
            concurrency_impact: "unchanged",
            durability_impact: "unchanged",
            memory_impact: "none",
            latency_impact: "none",
            diagnostics_available: true,
            first_failure_diag: "none",
        },
    )?;

    let aggregate = conn.query(
        "SELECT statement_fingerprint, plan_id, table_name, workload_lane,
                fallback_reason, supported_fast_path, diagnostics_available
           FROM fsqlite_fallback_events_contract
          WHERE statement_fingerprint = 'insert-fast-path';",
    )?;
    assert_eq!(aggregate.len(), 1);
    assert_eq!(
        row_values(&aggregate[0]),
        &[
            SqliteValue::Text("insert-fast-path".into()),
            SqliteValue::Text("direct-insert-v1".into()),
            SqliteValue::Text("agent_jobs".into()),
            SqliteValue::Text("ingest".into()),
            SqliteValue::Null,
            SqliteValue::Integer(1),
            SqliteValue::Integer(1),
        ],
        "fast-path statements must keep fallback_reason empty while diagnostics remain available"
    );

    let fallback_only = conn.query(
        "SELECT count(*)
           FROM fsqlite_fallback_events_contract
          WHERE supported_fast_path = 0;",
    )?;
    assert_eq!(
        row_values(&fallback_only[0]),
        &[SqliteValue::Integer(0)],
        "supported fast paths must not be counted as compatibility fallback work"
    );

    Ok(())
}

#[test]
fn mixed_workload_aggregates_frequency_by_statement_plan_table_and_lane() -> TestResult {
    let conn = Connection::open(":memory:")?;
    assert!(
        conn.is_concurrent_mode_default(),
        "fallback transparency must not disable concurrent-writer mode"
    );
    install_fallback_schema(&conn)?;

    for event in [
        FallbackEvent {
            event_seq: 1,
            statement_fingerprint: "select-join-window",
            plan_id: "compat-select-v1",
            table_name: "agent_jobs",
            workload_lane: "ingest",
            fallback_surface: "planner",
            fallback_reason: Some("unsupported_sql_shape"),
            supported_fast_path: false,
            concurrency_impact: "may_reduce_parallelism",
            durability_impact: "unchanged",
            memory_impact: "bounded_extra_memory",
            latency_impact: "may_increase_latency",
            diagnostics_available: true,
            first_failure_diag: "native windowed join lowering unavailable",
        },
        FallbackEvent {
            event_seq: 2,
            statement_fingerprint: "select-join-window",
            plan_id: "compat-select-v1",
            table_name: "agent_jobs",
            workload_lane: "ingest",
            fallback_surface: "planner",
            fallback_reason: Some("unsupported_sql_shape"),
            supported_fast_path: false,
            concurrency_impact: "may_reduce_parallelism",
            durability_impact: "unchanged",
            memory_impact: "bounded_extra_memory",
            latency_impact: "may_increase_latency",
            diagnostics_available: true,
            first_failure_diag: "native windowed join lowering unavailable",
        },
        FallbackEvent {
            event_seq: 3,
            statement_fingerprint: "update-with-trigger",
            plan_id: "compat-update-v1",
            table_name: "agent_jobs",
            workload_lane: "maintenance",
            fallback_surface: "planner",
            fallback_reason: Some("planner_bypass"),
            supported_fast_path: false,
            concurrency_impact: "unchanged",
            durability_impact: "unchanged",
            memory_impact: "none",
            latency_impact: "may_increase_latency",
            diagnostics_available: true,
            first_failure_diag: "trigger side effects require generic execution",
        },
        FallbackEvent {
            event_seq: 4,
            statement_fingerprint: "bulk-backfill",
            plan_id: "storage-compat-v1",
            table_name: "agent_ranges",
            workload_lane: "backfill",
            fallback_surface: "storage",
            fallback_reason: Some("storage_fallback"),
            supported_fast_path: false,
            concurrency_impact: "may_block_conflicting_pages",
            durability_impact: "unchanged",
            memory_impact: "bounded_extra_memory",
            latency_impact: "may_increase_latency",
            diagnostics_available: true,
            first_failure_diag: "range-local storage fast path unavailable",
        },
        FallbackEvent {
            event_seq: 5,
            statement_fingerprint: "opaque-extension",
            plan_id: "unknown-plan",
            table_name: "agent_extensions",
            workload_lane: "analysis",
            fallback_surface: "diagnostic",
            fallback_reason: Some("diagnostics_unavailable"),
            supported_fast_path: false,
            concurrency_impact: "unknown",
            durability_impact: "unknown",
            memory_impact: "unknown",
            latency_impact: "unknown",
            diagnostics_available: false,
            first_failure_diag: "fallback classifier did not receive planner context",
        },
    ] {
        record_fallback_event(&conn, event)?;
    }

    let rows = conn.query(
        "SELECT statement_fingerprint, plan_id, table_name, workload_lane,
                fallback_reason, count(*) AS fallback_count,
                min(concurrency_impact), min(durability_impact),
                min(memory_impact), min(latency_impact),
                min(diagnostics_available), min(first_failure_diag)
           FROM fsqlite_fallback_events_contract
          WHERE supported_fast_path = 0
          GROUP BY statement_fingerprint, plan_id, table_name, workload_lane,
                   fallback_reason
          ORDER BY fallback_count DESC, statement_fingerprint ASC;",
    )?;
    assert_eq!(rows.len(), 4);
    assert_eq!(
        row_values(&rows[0]),
        &[
            SqliteValue::Text("select-join-window".into()),
            SqliteValue::Text("compat-select-v1".into()),
            SqliteValue::Text("agent_jobs".into()),
            SqliteValue::Text("ingest".into()),
            SqliteValue::Text("unsupported_sql_shape".into()),
            SqliteValue::Integer(2),
            SqliteValue::Text("may_reduce_parallelism".into()),
            SqliteValue::Text("unchanged".into()),
            SqliteValue::Text("bounded_extra_memory".into()),
            SqliteValue::Text("may_increase_latency".into()),
            SqliteValue::Integer(1),
            SqliteValue::Text("native windowed join lowering unavailable".into()),
        ],
        "aggregate must group fallback frequency by fingerprint, plan, table, lane, and reason"
    );
    assert_eq!(
        row_values(&rows[1]),
        &[
            SqliteValue::Text("bulk-backfill".into()),
            SqliteValue::Text("storage-compat-v1".into()),
            SqliteValue::Text("agent_ranges".into()),
            SqliteValue::Text("backfill".into()),
            SqliteValue::Text("storage_fallback".into()),
            SqliteValue::Integer(1),
            SqliteValue::Text("may_block_conflicting_pages".into()),
            SqliteValue::Text("unchanged".into()),
            SqliteValue::Text("bounded_extra_memory".into()),
            SqliteValue::Text("may_increase_latency".into()),
            SqliteValue::Integer(1),
            SqliteValue::Text("range-local storage fast path unavailable".into()),
        ]
    );
    assert_eq!(
        row_values(&rows[2]),
        &[
            SqliteValue::Text("opaque-extension".into()),
            SqliteValue::Text("unknown-plan".into()),
            SqliteValue::Text("agent_extensions".into()),
            SqliteValue::Text("analysis".into()),
            SqliteValue::Text("diagnostics_unavailable".into()),
            SqliteValue::Integer(1),
            SqliteValue::Text("unknown".into()),
            SqliteValue::Text("unknown".into()),
            SqliteValue::Text("unknown".into()),
            SqliteValue::Text("unknown".into()),
            SqliteValue::Integer(0),
            SqliteValue::Text("fallback classifier did not receive planner context".into()),
        ]
    );
    assert_eq!(
        row_values(&rows[3]),
        &[
            SqliteValue::Text("update-with-trigger".into()),
            SqliteValue::Text("compat-update-v1".into()),
            SqliteValue::Text("agent_jobs".into()),
            SqliteValue::Text("maintenance".into()),
            SqliteValue::Text("planner_bypass".into()),
            SqliteValue::Integer(1),
            SqliteValue::Text("unchanged".into()),
            SqliteValue::Text("unchanged".into()),
            SqliteValue::Text("none".into()),
            SqliteValue::Text("may_increase_latency".into()),
            SqliteValue::Integer(1),
            SqliteValue::Text("trigger side effects require generic execution".into()),
        ]
    );

    Ok(())
}

#[test]
fn fallback_events_are_transactional_and_resettable() -> TestResult {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("fallback_events_transactional.db");
    let db = db_path.to_string_lossy().to_string();

    let conn = Connection::open(&db)?;
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")?;
    assert!(
        conn.is_concurrent_mode_default(),
        "fallback transparency must not disable concurrent-writer mode"
    );
    install_fallback_schema(&conn)?;

    conn.execute("BEGIN CONCURRENT;")?;
    record_fallback_event(
        &conn,
        FallbackEvent {
            event_seq: 1,
            statement_fingerprint: "rollback-fallback",
            plan_id: "compat-select-v1",
            table_name: "agent_jobs",
            workload_lane: "ingest",
            fallback_surface: "planner",
            fallback_reason: Some("unsupported_sql_shape"),
            supported_fast_path: false,
            concurrency_impact: "may_reduce_parallelism",
            durability_impact: "unchanged",
            memory_impact: "bounded_extra_memory",
            latency_impact: "may_increase_latency",
            diagnostics_available: true,
            first_failure_diag: "rolled-back fallback event",
        },
    )?;
    assert_eq!(fallback_count(&conn)?, 1);
    conn.execute("ROLLBACK;")?;
    assert_eq!(
        fallback_count(&conn)?,
        0,
        "rollback must discard fallback diagnostic rows from the transaction"
    );

    for event in [
        FallbackEvent {
            event_seq: 2,
            statement_fingerprint: "reset-fallback-a",
            plan_id: "compat-select-v1",
            table_name: "agent_jobs",
            workload_lane: "ingest",
            fallback_surface: "planner",
            fallback_reason: Some("unsupported_sql_shape"),
            supported_fast_path: false,
            concurrency_impact: "may_reduce_parallelism",
            durability_impact: "unchanged",
            memory_impact: "bounded_extra_memory",
            latency_impact: "may_increase_latency",
            diagnostics_available: true,
            first_failure_diag: "reset event a",
        },
        FallbackEvent {
            event_seq: 3,
            statement_fingerprint: "reset-fallback-b",
            plan_id: "compat-update-v1",
            table_name: "agent_jobs",
            workload_lane: "maintenance",
            fallback_surface: "planner",
            fallback_reason: Some("planner_bypass"),
            supported_fast_path: false,
            concurrency_impact: "unchanged",
            durability_impact: "unchanged",
            memory_impact: "none",
            latency_impact: "may_increase_latency",
            diagnostics_available: true,
            first_failure_diag: "reset event b",
        },
    ] {
        record_fallback_event(&conn, event)?;
    }
    assert_eq!(fallback_count(&conn)?, 2);
    conn.execute("DELETE FROM fsqlite_fallback_events_contract;")?;
    assert_eq!(
        fallback_count(&conn)?,
        0,
        "diagnostic reset must clear bounded fallback event state"
    );

    Ok(())
}
