use std::collections::BTreeSet;

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const FALLBACK_BOUNDARY_INVENTORY: &str =
    include_str!("../../../docs/contracts/fallback_boundary_inventory.toml");
const G9_FALLBACK_DENIAL_REPLAY_SCRIPT: &str =
    include_str!("../../../scripts/verify_g9_fallback_denial_replay.sh");
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
const FALLBACK_DECISION_REQUIRED_FIELDS: &[&str] = &[
    "trace_id",
    "run_id",
    "scenario_id",
    "statement_fingerprint",
    "statement_kind",
    "backend_identity",
    "fallback_boundary",
    "decision_reason",
    "certifying_mode",
    "strict_mode",
    "decision_outcome",
    "source_touchpoint",
    "first_failure_diag",
];
const FALLBACK_DECISION_ALLOWED_OUTCOMES: &[&str] = &[
    "denied",
    "allowed_compatibility_fallback",
    "real_backend_dispatch",
    "real_backend_refresh",
    "vdbe_mempage_fallback_rejected",
    "vdbe_mempage_fallback_allowed",
    "intentional_time_travel_snapshot",
    "refused",
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

#[derive(Clone, Copy)]
struct FallbackDecisionEvent<'a> {
    trace_id: &'a str,
    run_id: &'a str,
    scenario_id: &'a str,
    statement_fingerprint: &'a str,
    statement_kind: &'a str,
    backend_identity: &'a str,
    fallback_boundary: &'a str,
    decision_reason: &'a str,
    certifying_mode: bool,
    strict_mode: bool,
    decision_outcome: &'a str,
    source_touchpoint: &'a str,
    first_failure_diag: &'a str,
}

#[derive(Clone, Copy)]
enum ReplayOperation {
    Execute,
    Query,
}

#[derive(Clone, Copy)]
struct StrictSqlReplayScenario {
    scenario_id: &'static str,
    setup_sql: &'static [&'static str],
    operation: ReplayOperation,
    sql: &'static str,
    statement_kind: &'static str,
    decision_reason: &'static str,
    fallback_boundary: &'static str,
}

#[derive(Clone, Copy)]
struct ReplayMatrixScenario {
    scenario_id: &'static str,
    boundary_id: &'static str,
    statement_kind: &'static str,
    decision_reason: &'static str,
    decision_outcome: &'static str,
    certifying_mode: bool,
    strict_mode: bool,
    backend_identity: &'static str,
    source_touchpoint: &'static str,
}

const G9_REPLAY_COMMAND: &str = "rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-g9-fallback-denial-replay-target cargo test -p fsqlite-core --test agent_swarm_fallback_transparency_contract deterministic_fallback_denial_replay -- --nocapture";

const STRICT_SQL_REPLAY_SCENARIOS: &[StrictSqlReplayScenario] = &[
    StrictSqlReplayScenario {
        scenario_id: "g9-deny-select-cte",
        setup_sql: &[],
        operation: ReplayOperation::Query,
        sql: "WITH c(x) AS (SELECT 1) SELECT x FROM c;",
        statement_kind: "select",
        decision_reason: "with_clause_materialization",
        fallback_boundary: "conn.select.with_clause_materialization",
    },
    StrictSqlReplayScenario {
        scenario_id: "g9-deny-view-materialization",
        setup_sql: &[
            "CREATE TABLE g9_jobs(id INTEGER PRIMARY KEY, status TEXT);",
            "INSERT INTO g9_jobs(status) VALUES ('ready');",
            "CREATE VIEW g9_ready_jobs AS SELECT status FROM g9_jobs WHERE status = 'ready';",
        ],
        operation: ReplayOperation::Query,
        sql: "SELECT status FROM g9_ready_jobs;",
        statement_kind: "select",
        decision_reason: "view_materialization",
        fallback_boundary: "conn.select.view_materialization",
    },
    StrictSqlReplayScenario {
        scenario_id: "g9-deny-sqlite-schema",
        setup_sql: &["CREATE TABLE g9_schema_probe(id INTEGER PRIMARY KEY);"],
        operation: ReplayOperation::Query,
        sql: "SELECT name FROM sqlite_schema WHERE type = 'table';",
        statement_kind: "select",
        decision_reason: "sqlite_schema_virtual_materialization",
        fallback_boundary: "conn.select.sqlite_schema_virtual_materialization",
    },
    StrictSqlReplayScenario {
        scenario_id: "g9-deny-insert-cte",
        setup_sql: &["CREATE TABLE g9_insert_cte_dst(x INTEGER);"],
        operation: ReplayOperation::Execute,
        sql: "WITH c(x) AS (SELECT 1) INSERT INTO g9_insert_cte_dst(x) SELECT x FROM c;",
        statement_kind: "insert",
        decision_reason: "with_clause_materialization",
        fallback_boundary: "conn.insert.with_clause_materialization",
    },
    StrictSqlReplayScenario {
        scenario_id: "g9-deny-insert-select-replay",
        setup_sql: &[
            "CREATE TABLE g9_insert_select_src(x INTEGER);",
            "CREATE TABLE g9_insert_select_dst(x INTEGER);",
            "INSERT INTO g9_insert_select_src(x) VALUES (1), (2);",
        ],
        operation: ReplayOperation::Execute,
        sql: "INSERT INTO g9_insert_select_dst(x) SELECT x FROM g9_insert_select_src WHERE x > 0;",
        statement_kind: "insert_select",
        decision_reason: "insert_select_row_by_row_fallback",
        fallback_boundary: "conn.insert_select.insert_select_row_by_row_fallback",
    },
];

const REPLAY_MATRIX_SCENARIOS: &[ReplayMatrixScenario] = &[
    ReplayMatrixScenario {
        scenario_id: "g9-deny-select-cte",
        boundary_id: "conn.select.with_clause_materialization",
        statement_kind: "select",
        decision_reason: "with_clause_materialization",
        decision_outcome: "denied",
        certifying_mode: true,
        strict_mode: true,
        backend_identity: "memory:parity_cert_strict",
        source_touchpoint: "execute_statement_dispatch:select:with_clause_materialization",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-deny-view-materialization",
        boundary_id: "conn.select.view_materialization",
        statement_kind: "select",
        decision_reason: "view_materialization",
        decision_outcome: "denied",
        certifying_mode: true,
        strict_mode: true,
        backend_identity: "memory:parity_cert_strict",
        source_touchpoint: "execute_statement_dispatch:select:view_materialization",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-deny-sqlite-schema",
        boundary_id: "conn.select.sqlite_schema_virtual_materialization",
        statement_kind: "select",
        decision_reason: "sqlite_schema_virtual_materialization",
        decision_outcome: "denied",
        certifying_mode: true,
        strict_mode: true,
        backend_identity: "memory:parity_cert_strict",
        source_touchpoint: "execute_statement_dispatch:select:sqlite_schema_virtual_materialization",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-deny-insert-cte",
        boundary_id: "conn.insert.with_clause_materialization",
        statement_kind: "insert",
        decision_reason: "with_clause_materialization",
        decision_outcome: "denied",
        certifying_mode: true,
        strict_mode: true,
        backend_identity: "memory:parity_cert_strict",
        source_touchpoint: "execute_statement_dispatch:insert:with_clause_materialization",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-deny-update-cte",
        boundary_id: "conn.update.with_clause_materialization",
        statement_kind: "update",
        decision_reason: "with_clause_materialization",
        decision_outcome: "denied",
        certifying_mode: true,
        strict_mode: true,
        backend_identity: "memory:parity_cert_strict",
        source_touchpoint: "execute_statement_dispatch:update:with_clause_materialization",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-deny-delete-cte",
        boundary_id: "conn.delete.with_clause_materialization",
        statement_kind: "delete",
        decision_reason: "with_clause_materialization",
        decision_outcome: "denied",
        certifying_mode: true,
        strict_mode: true,
        backend_identity: "memory:parity_cert_strict",
        source_touchpoint: "execute_statement_dispatch:delete:with_clause_materialization",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-deny-insert-select-replay",
        boundary_id: "conn.insert_select.insert_select_row_by_row_fallback",
        statement_kind: "insert_select",
        decision_reason: "insert_select_row_by_row_fallback",
        decision_outcome: "denied",
        certifying_mode: true,
        strict_mode: true,
        backend_identity: "memory:parity_cert_strict",
        source_touchpoint: "execute_statement_dispatch:insert_select:insert_select_row_by_row_fallback",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-deny-materialized-temp-table",
        boundary_id: "conn.statement.materialized_temp_table_execution",
        statement_kind: "statement",
        decision_reason: "materialized_temp_table_execution",
        decision_outcome: "denied",
        certifying_mode: true,
        strict_mode: true,
        backend_identity: "memory:parity_cert_strict",
        source_touchpoint: "internal_mem_fallback_guard:statement:materialized_temp_table_execution",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-deny-vdbe-mempage",
        boundary_id: "vdbe.open_cursor.parity_cert_rejection",
        statement_kind: "vdbe_open_cursor",
        decision_reason: "parity_cert_rejection",
        decision_outcome: "vdbe_mempage_fallback_rejected",
        certifying_mode: true,
        strict_mode: false,
        backend_identity: "mem:parity_cert",
        source_touchpoint: "VdbeEngine::open_storage_cursor",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-control-real-backend",
        boundary_id: "vdbe.open_cursor.valid_btree_page",
        statement_kind: "vdbe_open_cursor",
        decision_reason: "valid_btree_page",
        decision_outcome: "real_backend_dispatch",
        certifying_mode: true,
        strict_mode: true,
        backend_identity: "txn:parity_cert_strict",
        source_touchpoint: "VdbeEngine::open_storage_cursor",
    },
    ReplayMatrixScenario {
        scenario_id: "g9-control-non-cert-view",
        boundary_id: "conn.select.view_materialization",
        statement_kind: "select",
        decision_reason: "view_materialization",
        decision_outcome: "allowed_compatibility_fallback",
        certifying_mode: false,
        strict_mode: false,
        backend_identity: "memory:fallback_allowed",
        source_touchpoint: "execute_statement_dispatch:select:view_materialization",
    },
];

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
    conn.execute(
        "CREATE TABLE fsqlite_fallback_decision_events_contract(
            event_seq INTEGER NOT NULL PRIMARY KEY,
            trace_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            scenario_id TEXT NOT NULL,
            statement_fingerprint TEXT NOT NULL,
            statement_kind TEXT NOT NULL,
            backend_identity TEXT NOT NULL,
            fallback_boundary TEXT NOT NULL,
            decision_reason TEXT NOT NULL,
            certifying_mode INTEGER NOT NULL,
            strict_mode INTEGER NOT NULL,
            decision_outcome TEXT NOT NULL,
            source_touchpoint TEXT NOT NULL,
            first_failure_diag TEXT NOT NULL
        );",
    )?;
    conn.execute(
        "CREATE INDEX idx_fsqlite_fallback_decision_events_contract_lookup
            ON fsqlite_fallback_decision_events_contract(
                statement_fingerprint,
                statement_kind,
                backend_identity,
                fallback_boundary,
                decision_outcome
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

fn fallback_decision_contract_error(message: impl Into<String>) -> Box<dyn std::error::Error> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

fn fallback_decision_required_text_fields<'a>(
    event: &'a FallbackDecisionEvent<'a>,
) -> [(&'static str, &'a str); 11] {
    [
        ("trace_id", event.trace_id),
        ("run_id", event.run_id),
        ("scenario_id", event.scenario_id),
        ("statement_fingerprint", event.statement_fingerprint),
        ("statement_kind", event.statement_kind),
        ("backend_identity", event.backend_identity),
        ("fallback_boundary", event.fallback_boundary),
        ("decision_reason", event.decision_reason),
        ("decision_outcome", event.decision_outcome),
        ("source_touchpoint", event.source_touchpoint),
        ("first_failure_diag", event.first_failure_diag),
    ]
}

fn validate_fallback_decision_event(event: &FallbackDecisionEvent<'_>) -> TestResult {
    for (field, value) in fallback_decision_required_text_fields(event) {
        if value.trim().is_empty() {
            return Err(fallback_decision_contract_error(format!(
                "fallback decision field {field} must not be empty"
            )));
        }
    }

    if !FALLBACK_DECISION_ALLOWED_OUTCOMES.contains(&event.decision_outcome) {
        return Err(fallback_decision_contract_error(format!(
            "unknown fallback decision outcome {}",
            event.decision_outcome
        )));
    }
    if !event.backend_identity.contains(':') {
        return Err(fallback_decision_contract_error(format!(
            "backend_identity {} must include backend kind and mode",
            event.backend_identity
        )));
    }
    for expected_diag_part in [
        event.statement_kind,
        event.fallback_boundary,
        event.source_touchpoint,
        event.decision_reason,
    ] {
        if !event.first_failure_diag.contains(expected_diag_part) {
            return Err(fallback_decision_contract_error(format!(
                "first_failure_diag {:?} must include {:?}",
                event.first_failure_diag, expected_diag_part
            )));
        }
    }

    match event.decision_outcome {
        "denied" => {
            if !event.certifying_mode || !event.strict_mode {
                return Err(fallback_decision_contract_error(
                    "strict denial must carry certifying_mode=true and strict_mode=true",
                ));
            }
        }
        "allowed_compatibility_fallback" | "intentional_time_travel_snapshot" => {
            if event.strict_mode {
                return Err(fallback_decision_contract_error(
                    "allowed compatibility fallback must not carry strict_mode=true",
                ));
            }
        }
        "real_backend_dispatch" | "real_backend_refresh" => {
            if event.backend_identity.starts_with("mem:") {
                return Err(fallback_decision_contract_error(format!(
                    "{} must not be reported with MemDatabase backend_identity {}",
                    event.decision_outcome, event.backend_identity
                )));
            }
        }
        "vdbe_mempage_fallback_rejected" => {
            if !event.certifying_mode {
                return Err(fallback_decision_contract_error(
                    "VDBE MemPageStore rejection must carry certifying_mode=true",
                ));
            }
        }
        "vdbe_mempage_fallback_allowed" | "refused" => {}
        other => {
            return Err(fallback_decision_contract_error(format!(
                "unhandled fallback decision outcome {other}"
            )));
        }
    }

    Ok(())
}

fn record_fallback_decision_event(
    conn: &Connection,
    event_seq: i64,
    event: FallbackDecisionEvent<'_>,
) -> TestResult {
    validate_fallback_decision_event(&event)?;
    conn.execute(&format!(
        "INSERT INTO fsqlite_fallback_decision_events_contract(
            event_seq,
            trace_id,
            run_id,
            scenario_id,
            statement_fingerprint,
            statement_kind,
            backend_identity,
            fallback_boundary,
            decision_reason,
            certifying_mode,
            strict_mode,
            decision_outcome,
            source_touchpoint,
            first_failure_diag
        ) VALUES (
            {event_seq},
            {trace_id},
            {run_id},
            {scenario_id},
            {statement_fingerprint},
            {statement_kind},
            {backend_identity},
            {fallback_boundary},
            {decision_reason},
            {certifying_mode},
            {strict_mode},
            {decision_outcome},
            {source_touchpoint},
            {first_failure_diag}
        );",
        event_seq = event_seq,
        trace_id = sql_text(event.trace_id),
        run_id = sql_text(event.run_id),
        scenario_id = sql_text(event.scenario_id),
        statement_fingerprint = sql_text(event.statement_fingerprint),
        statement_kind = sql_text(event.statement_kind),
        backend_identity = sql_text(event.backend_identity),
        fallback_boundary = sql_text(event.fallback_boundary),
        decision_reason = sql_text(event.decision_reason),
        certifying_mode = sql_bool(event.certifying_mode),
        strict_mode = sql_bool(event.strict_mode),
        decision_outcome = sql_text(event.decision_outcome),
        source_touchpoint = sql_text(event.source_touchpoint),
        first_failure_diag = sql_text(event.first_failure_diag)
    ))?;
    Ok(())
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

fn inventory_has_boundary_id(boundary_id: &str) -> bool {
    FALLBACK_BOUNDARY_INVENTORY.contains(&format!("boundary_id = \"{boundary_id}\""))
}

fn assert_strict_replay_denial(scenario: &StrictSqlReplayScenario) -> TestResult {
    let dir = tempfile::tempdir()?;
    let db_path = dir
        .path()
        .join(format!("{}.db", scenario.scenario_id.replace('-', "_")));
    let db = db_path.to_string_lossy();
    let conn = Connection::open(db.as_ref())?;
    assert!(
        conn.is_concurrent_mode_default(),
        "G9 replay scenarios must not disable concurrent-writer mode"
    );

    for setup in scenario.setup_sql {
        conn.execute(setup)?;
    }
    conn.execute("PRAGMA fsqlite.parity_cert_strict = ON;")?;

    let error = match scenario.operation {
        ReplayOperation::Execute => conn
            .execute(scenario.sql)
            .map(|_| ())
            .expect_err("strict replay scenario should deny compatibility fallback"),
        ReplayOperation::Query => conn
            .query(scenario.sql)
            .map(|_| ())
            .expect_err("strict replay scenario should deny compatibility fallback"),
    };
    let message = error.to_string();
    for expected in [
        "in-memory fallback disabled in strict parity-cert mode",
        &format!("statement_kind={}", scenario.statement_kind),
        &format!("decision_reason={}", scenario.decision_reason),
        "decision_outcome=denied",
        &format!("fallback_boundary={}", scenario.fallback_boundary),
        "source_touchpoint=",
        "first_failure_diag=",
    ] {
        assert!(
            message.contains(expected),
            "{} did not include {expected:?}: {message}",
            scenario.scenario_id
        );
    }
    Ok(())
}

fn replay_matrix_first_failure_diag(scenario: &ReplayMatrixScenario) -> String {
    format!(
        "statement_kind={}; fallback_boundary={}; source_touchpoint={}; decision_reason={}",
        scenario.statement_kind,
        scenario.boundary_id,
        scenario.source_touchpoint,
        scenario.decision_reason
    )
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
fn fallback_decision_contract_validates_schema_fields_and_outcomes() -> TestResult {
    let conn = Connection::open(":memory:")?;
    assert!(
        conn.is_concurrent_mode_default(),
        "fallback telemetry must not disable concurrent-writer mode"
    );
    install_fallback_schema(&conn)?;

    let decision_events = [
        FallbackDecisionEvent {
            trace_id: "trace-real-backend",
            run_id: "run-g9",
            scenario_id: "scenario-real-backend",
            statement_fingerprint: "select-real-table",
            statement_kind: "select",
            backend_identity: "txn:parity_cert_strict",
            fallback_boundary: "conn.select.real_backend_dispatch",
            decision_reason: "valid_btree_page",
            certifying_mode: true,
            strict_mode: true,
            decision_outcome: "real_backend_dispatch",
            source_touchpoint: "VdbeEngine::open_storage_cursor",
            first_failure_diag: "statement_kind=select; fallback_boundary=conn.select.real_backend_dispatch; source_touchpoint=VdbeEngine::open_storage_cursor; decision_reason=valid_btree_page",
        },
        FallbackDecisionEvent {
            trace_id: "trace-denied",
            run_id: "run-g9",
            scenario_id: "scenario-denied",
            statement_fingerprint: "select-with",
            statement_kind: "select",
            backend_identity: "memory:parity_cert_strict",
            fallback_boundary: "conn.select.with_clause_materialization",
            decision_reason: "with_clause_materialization",
            certifying_mode: true,
            strict_mode: true,
            decision_outcome: "denied",
            source_touchpoint: "execute_statement_dispatch:select:with_clause_materialization",
            first_failure_diag: "statement_kind=select; fallback_boundary=conn.select.with_clause_materialization; source_touchpoint=execute_statement_dispatch:select:with_clause_materialization; decision_reason=with_clause_materialization",
        },
        FallbackDecisionEvent {
            trace_id: "trace-allowed",
            run_id: "run-g9",
            scenario_id: "scenario-allowed",
            statement_fingerprint: "select-view",
            statement_kind: "select",
            backend_identity: "memory:fallback_allowed",
            fallback_boundary: "conn.select.view_materialization",
            decision_reason: "view_materialization",
            certifying_mode: false,
            strict_mode: false,
            decision_outcome: "allowed_compatibility_fallback",
            source_touchpoint: "execute_statement_dispatch:select:view_materialization",
            first_failure_diag: "statement_kind=select; fallback_boundary=conn.select.view_materialization; source_touchpoint=execute_statement_dispatch:select:view_materialization; decision_reason=view_materialization",
        },
        FallbackDecisionEvent {
            trace_id: "trace-vdbe-rejected",
            run_id: "run-g9",
            scenario_id: "scenario-vdbe",
            statement_fingerprint: "vdbe-open-storage-cursor:2:2:parity_cert_rejection",
            statement_kind: "vdbe_open_cursor",
            backend_identity: "mem:parity_cert",
            fallback_boundary: "vdbe.open_cursor.parity_cert_rejection",
            decision_reason: "parity_cert_rejection",
            certifying_mode: true,
            strict_mode: false,
            decision_outcome: "vdbe_mempage_fallback_rejected",
            source_touchpoint: "VdbeEngine::open_storage_cursor",
            first_failure_diag: "statement_kind=vdbe_open_cursor; fallback_boundary=vdbe.open_cursor.parity_cert_rejection; source_touchpoint=VdbeEngine::open_storage_cursor; decision_reason=parity_cert_rejection",
        },
        FallbackDecisionEvent {
            trace_id: "trace-refresh",
            run_id: "run-g9",
            scenario_id: "scenario-refresh",
            statement_fingerprint: "memdb-refresh",
            statement_kind: "statement",
            backend_identity: "txn:parity_cert",
            fallback_boundary: "conn.memdb.refresh_from_pager_publication",
            decision_reason: "refresh_from_pager_publication",
            certifying_mode: true,
            strict_mode: false,
            decision_outcome: "real_backend_refresh",
            source_touchpoint: "refresh_from_pager_publication",
            first_failure_diag: "statement_kind=statement; fallback_boundary=conn.memdb.refresh_from_pager_publication; source_touchpoint=refresh_from_pager_publication; decision_reason=refresh_from_pager_publication",
        },
    ];
    for (idx, event) in decision_events.into_iter().enumerate() {
        record_fallback_decision_event(
            &conn,
            i64::try_from(idx + 1).expect("test event count fits i64"),
            event,
        )?;
    }

    let rows = conn.query(
        "SELECT decision_outcome, count(*)
           FROM fsqlite_fallback_decision_events_contract
          GROUP BY decision_outcome
          ORDER BY decision_outcome;",
    )?;
    assert_eq!(
        rows.len(),
        5,
        "decision event contract should preserve every standardized outcome"
    );

    let mut empty_field = decision_events[1];
    empty_field.statement_fingerprint = "";
    assert!(
        validate_fallback_decision_event(&empty_field).is_err(),
        "empty required identifiers must fail schema validation"
    );

    let mut unknown_outcome = decision_events[1];
    unknown_outcome.decision_outcome = "maybe_fallback";
    assert!(
        validate_fallback_decision_event(&unknown_outcome).is_err(),
        "unknown decision outcomes must fail schema validation"
    );

    let mut inconsistent_backend = decision_events[0];
    inconsistent_backend.backend_identity = "mem:parity_cert_strict";
    assert!(
        validate_fallback_decision_event(&inconsistent_backend).is_err(),
        "real-backend dispatch must not validate as a MemDatabase backend"
    );

    Ok(())
}

#[test]
fn production_fallback_decision_sources_expose_required_schema_fields() {
    for field in FALLBACK_DECISION_REQUIRED_FIELDS {
        assert!(
            CONNECTION_SOURCE.contains(field),
            "connection fallback-decision logs must expose required field {field}"
        );
        assert!(
            VDBE_ENGINE_SOURCE.contains(field),
            "VDBE fallback-decision logs must expose required field {field}"
        );
    }
    for outcome in FALLBACK_DECISION_ALLOWED_OUTCOMES {
        assert!(
            FALLBACK_BOUNDARY_INVENTORY.contains(outcome)
                || CONNECTION_SOURCE.contains(outcome)
                || VDBE_ENGINE_SOURCE.contains(outcome),
            "fallback-decision outcome {outcome} must be documented or emitted"
        );
    }
    for target in [CONNECTION_SOURCE, VDBE_ENGINE_SOURCE] {
        assert!(
            target.contains("target: \"fsqlite.fallback_decision\""),
            "fallback decision telemetry must use the stable tracing target"
        );
    }
}

#[test]
fn strict_denial_error_names_fallback_boundary_and_outcome() -> TestResult {
    let conn = Connection::open(":memory:")?;
    conn.execute("PRAGMA fsqlite.parity_cert_strict = ON;")?;

    let err = conn
        .query("WITH c(x) AS (SELECT 1) SELECT x FROM c;")
        .expect_err("strict parity-cert must reject WITH-clause MemDatabase materialization");
    let message = err.to_string();
    for expected in [
        "in-memory fallback disabled in strict parity-cert mode",
        "statement_kind=select",
        "decision_reason=with_clause_materialization",
        "decision_outcome=denied",
        "fallback_boundary=conn.select.with_clause_materialization",
        "source_touchpoint=",
        "first_failure_diag=",
    ] {
        assert!(
            message.contains(expected),
            "strict fallback denial did not include {expected:?}: {message}"
        );
    }

    Ok(())
}

#[test]
fn deterministic_fallback_denial_replay_matrix_covers_inventory_and_controls() -> TestResult {
    assert!(
        REPLAY_MATRIX_SCENARIOS.len() >= 10,
        "G9.3 replay matrix must include strict denials plus controls"
    );

    let mut scenario_ids = BTreeSet::new();
    let mut strict_denials = 0;
    let mut non_cert_controls = 0;
    let mut real_backend_controls = 0;
    let conn = Connection::open(":memory:")?;
    install_fallback_schema(&conn)?;

    for (idx, scenario) in REPLAY_MATRIX_SCENARIOS.iter().copied().enumerate() {
        assert!(
            scenario_ids.insert(scenario.scenario_id),
            "duplicate G9 replay scenario id {}",
            scenario.scenario_id
        );
        assert!(
            inventory_has_boundary_id(scenario.boundary_id),
            "G9 replay scenario {} references boundary missing from inventory: {}",
            scenario.scenario_id,
            scenario.boundary_id
        );
        assert!(
            inventory_has_decision_reason(scenario.decision_reason),
            "G9 replay scenario {} references reason missing from inventory: {}",
            scenario.scenario_id,
            scenario.decision_reason
        );

        let first_failure_diag = replay_matrix_first_failure_diag(&scenario);
        let event = FallbackDecisionEvent {
            trace_id: "trace-g9-replay-matrix",
            run_id: "run-g9-replay-matrix",
            scenario_id: scenario.scenario_id,
            statement_fingerprint: scenario.scenario_id,
            statement_kind: scenario.statement_kind,
            backend_identity: scenario.backend_identity,
            fallback_boundary: scenario.boundary_id,
            decision_reason: scenario.decision_reason,
            certifying_mode: scenario.certifying_mode,
            strict_mode: scenario.strict_mode,
            decision_outcome: scenario.decision_outcome,
            source_touchpoint: scenario.source_touchpoint,
            first_failure_diag: first_failure_diag.as_str(),
        };
        record_fallback_decision_event(
            &conn,
            i64::try_from(idx + 1).expect("G9 replay matrix event count fits i64"),
            event,
        )?;

        let known_outcome = match scenario.decision_outcome {
            "denied" | "vdbe_mempage_fallback_rejected" => {
                assert!(
                    scenario.certifying_mode,
                    "strict-denial replay {} must be certifying",
                    scenario.scenario_id
                );
                strict_denials += 1;
                true
            }
            "allowed_compatibility_fallback" | "intentional_time_travel_snapshot" => {
                assert!(
                    !scenario.certifying_mode,
                    "compatibility control {} must not certify parity",
                    scenario.scenario_id
                );
                non_cert_controls += 1;
                true
            }
            "real_backend_dispatch" | "real_backend_refresh" => {
                assert!(
                    scenario.certifying_mode,
                    "real-backend control {} should run in certifying mode",
                    scenario.scenario_id
                );
                real_backend_controls += 1;
                true
            }
            _ => false,
        };
        assert!(
            known_outcome,
            "G9 replay matrix scenario {} used unsupported outcome {}",
            scenario.scenario_id, scenario.decision_outcome
        );
    }

    assert!(
        strict_denials >= 8,
        "G9.3 should cover a broad strict-denial matrix, saw {strict_denials}"
    );
    assert!(
        non_cert_controls >= 1,
        "G9.3 must keep non-cert compatibility controls separate"
    );
    assert!(
        real_backend_controls >= 1,
        "G9.3 must include at least one positive real-backend control"
    );

    let rows = conn.query(
        "SELECT decision_outcome, count(*)
           FROM fsqlite_fallback_decision_events_contract
          GROUP BY decision_outcome
          ORDER BY decision_outcome;",
    )?;
    assert!(
        rows.len() >= 4,
        "G9 replay matrix should persist denied, VDBE rejected, real-backend, and non-cert outcomes"
    );

    Ok(())
}

#[test]
fn deterministic_fallback_denial_replay_exercises_real_strict_sql_boundaries() -> TestResult {
    for scenario in STRICT_SQL_REPLAY_SCENARIOS {
        assert!(
            inventory_has_boundary_id(scenario.fallback_boundary),
            "strict SQL replay scenario {} references boundary missing from inventory: {}",
            scenario.scenario_id,
            scenario.fallback_boundary
        );
        assert!(
            inventory_has_decision_reason(scenario.decision_reason),
            "strict SQL replay scenario {} references reason missing from inventory: {}",
            scenario.scenario_id,
            scenario.decision_reason
        );
        assert_strict_replay_denial(scenario)?;
    }
    Ok(())
}

#[test]
fn deterministic_fallback_denial_replay_script_is_copyable_rch_gate() {
    for expected in [
        "BEAD_ID=\"bd-2yqp6.7.9.3\"",
        "FSQLITE_G9_REPLAY_ARTIFACT_ROOT",
        "fallback_denial_replay_summary.json",
        "fallback_denial_replay_test.log",
        "rch exec -- env CARGO_TARGET_DIR=",
        "cargo test -p fsqlite-core --test agent_swarm_fallback_transparency_contract deterministic_fallback_denial_replay -- --nocapture",
    ] {
        assert!(
            G9_FALLBACK_DENIAL_REPLAY_SCRIPT.contains(expected),
            "G9 replay script must contain {expected:?}"
        );
    }
    assert!(
        G9_REPLAY_COMMAND.contains("rch exec -- env CARGO_TARGET_DIR="),
        "documented G9 replay command must offload cargo through rch"
    );
}

#[test]
fn real_backend_strict_execution_and_non_cert_fallback_remain_distinguishable() -> TestResult {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("g9_decision_real_backend.db");
    let db = db_path.to_string_lossy();
    let conn = Connection::open(db.as_ref())?;
    conn.execute("CREATE TABLE agent_jobs(id INTEGER PRIMARY KEY, status TEXT);")?;
    conn.execute("INSERT INTO agent_jobs(status) VALUES ('ready');")?;
    conn.execute("PRAGMA fsqlite.parity_cert_strict = ON;")?;

    let rows = conn.query("SELECT status FROM agent_jobs WHERE id = 1;")?;
    assert_eq!(
        row_values(&rows[0]),
        &[SqliteValue::Text("ready".into())],
        "strict certifying mode should still execute real backend table reads"
    );

    let compat = Connection::open(":memory:")?;
    compat.execute("PRAGMA fsqlite.parity_cert = OFF;")?;
    let rows = compat.query("WITH c(x) AS (SELECT 7) SELECT x FROM c;")?;
    assert_eq!(
        row_values(&rows[0]),
        &[SqliteValue::Integer(7)],
        "non-cert compatibility fallback must remain allowed and distinguishable from strict denial"
    );

    Ok(())
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
