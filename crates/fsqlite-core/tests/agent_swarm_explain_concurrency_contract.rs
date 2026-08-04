use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct ConcurrencyDiagnostic<'a> {
    event_seq: i64,
    diagnostic_surface: &'a str,
    statement_fingerprint: &'a str,
    plan_id: &'a str,
    table_name: &'a str,
    index_name: Option<&'a str>,
    range_id: Option<&'a str>,
    queue_name: Option<&'a str>,
    lease_key: Option<&'a str>,
    worker_id: &'a str,
    transaction_id: &'a str,
    hotspot_kind: &'a str,
    page_number: Option<i64>,
    page_start: Option<i64>,
    page_end: Option<i64>,
    predicted_conflict_count: i64,
    observed_conflict_count: i64,
    retry_count: i64,
    abort_count: i64,
    busy_family: &'a str,
    conflict_reason: Option<&'a str>,
    fallback_reason: Option<&'a str>,
    external_wait: Option<&'a str>,
    coordination_strategy: &'a str,
    diagnostics_available: bool,
    suggested_next_inspection: &'a str,
    first_failure_diag: &'a str,
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_optional_text(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), sql_text)
}

fn sql_optional_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |number| number.to_string())
}

fn sql_bool(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}

fn row_values(row: &Row) -> Vec<SqliteValue> {
    row.values().to_vec()
}

fn rows_to_values(rows: &[Row]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(row_values).collect()
}

async fn install_concurrency_schema(conn: &Connection) -> TestResult {
    conn.execute(
        "CREATE TABLE fsqlite_concurrency_reason_codes_contract(
            reason_code TEXT NOT NULL PRIMARY KEY,
            hotspot_kind TEXT NOT NULL,
            severity TEXT NOT NULL,
            retryable INTEGER NOT NULL,
            contention_family TEXT NOT NULL,
            concurrency_impact TEXT NOT NULL,
            suggested_next_inspection TEXT NOT NULL,
            human_text TEXT NOT NULL
        );",
    )
    .await?;
    conn.execute(
        "CREATE TABLE fsqlite_explain_concurrency_contract(
            event_seq INTEGER NOT NULL PRIMARY KEY,
            event_ts_ms INTEGER NOT NULL,
            diagnostic_surface TEXT NOT NULL,
            trace_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            scenario_id TEXT NOT NULL,
            statement_fingerprint TEXT NOT NULL,
            plan_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            index_name TEXT,
            range_id TEXT,
            queue_name TEXT,
            lease_key TEXT,
            worker_id TEXT NOT NULL,
            transaction_id TEXT NOT NULL,
            hotspot_kind TEXT NOT NULL,
            page_number INTEGER,
            page_start INTEGER,
            page_end INTEGER,
            predicted_conflict_count INTEGER NOT NULL,
            observed_conflict_count INTEGER NOT NULL,
            retry_count INTEGER NOT NULL,
            abort_count INTEGER NOT NULL,
            busy_family TEXT NOT NULL,
            conflict_reason TEXT,
            fallback_reason TEXT,
            external_wait TEXT,
            coordination_strategy TEXT NOT NULL,
            diagnostics_available INTEGER NOT NULL,
            suggested_next_inspection TEXT NOT NULL,
            first_failure_diag TEXT NOT NULL
        );",
    )
    .await?;
    conn.execute(
        "CREATE INDEX idx_fsqlite_explain_concurrency_contract_join
            ON fsqlite_explain_concurrency_contract(
                statement_fingerprint,
                plan_id,
                table_name,
                hotspot_kind
            );",
    )
    .await?;
    seed_reason_codes(conn).await?;
    Ok(())
}

async fn seed_reason_codes(conn: &Connection) -> TestResult {
    conn.execute(
        "INSERT INTO fsqlite_concurrency_reason_codes_contract(
            reason_code,
            hotspot_kind,
            severity,
            retryable,
            contention_family,
            concurrency_impact,
            suggested_next_inspection,
            human_text
        ) VALUES
            (
                'ok_low_conflict',
                'none',
                'notice',
                0,
                'page_mvcc',
                'lock_free_expected',
                'none',
                'Statement is expected to stay on the low-conflict MVCC path.'
            ),
            (
                'hot_page_predicted',
                'page',
                'warning',
                1,
                'page_mvcc',
                'retry_may_increase',
                'inspect_page_heat',
                'Planner and observed writes point at a hot page.'
            ),
            (
                'coordination_wait',
                'coordination',
                'notice',
                1,
                'queue_lease_range',
                'ownership_wait_only',
                'inspect_coordination_owner',
                'Queue, lease, or range ownership explains the wait.'
            ),
            (
                'compatibility_fallback',
                'fallback',
                'warning',
                0,
                'compatibility',
                'may_reduce_parallelism',
                'inspect_fallback_reason',
                'Statement uses a compatibility-backed path.'
            ),
            (
                'external_resource_wait',
                'external',
                'warning',
                1,
                'external',
                'outside_mvcc',
                'inspect_vfs_or_runtime_wait',
                'The wait is outside page-level MVCC coordination.'
            ),
            (
                'diagnostics_unavailable',
                'unknown',
                'warning',
                1,
                'unknown',
                'unknown',
                'inspect_first_failure_diag',
                'Concurrency diagnostics were unavailable.'
            );",
    )
    .await?;
    Ok(())
}

async fn record_diagnostic(conn: &Connection, diagnostic: ConcurrencyDiagnostic<'_>) -> TestResult {
    conn.execute(&format!(
        "INSERT INTO fsqlite_explain_concurrency_contract(
            event_seq,
            event_ts_ms,
            diagnostic_surface,
            trace_id,
            run_id,
            scenario_id,
            statement_fingerprint,
            plan_id,
            table_name,
            index_name,
            range_id,
            queue_name,
            lease_key,
            worker_id,
            transaction_id,
            hotspot_kind,
            page_number,
            page_start,
            page_end,
            predicted_conflict_count,
            observed_conflict_count,
            retry_count,
            abort_count,
            busy_family,
            conflict_reason,
            fallback_reason,
            external_wait,
            coordination_strategy,
            diagnostics_available,
            suggested_next_inspection,
            first_failure_diag
        ) VALUES (
            {event_seq},
            {event_ts_ms},
            {diagnostic_surface},
            'trace-explain-concurrency-contract',
            'run-explain-concurrency-contract',
            'scenario-agent-swarm-concurrency',
            {statement_fingerprint},
            {plan_id},
            {table_name},
            {index_name},
            {range_id},
            {queue_name},
            {lease_key},
            {worker_id},
            {transaction_id},
            {hotspot_kind},
            {page_number},
            {page_start},
            {page_end},
            {predicted_conflict_count},
            {observed_conflict_count},
            {retry_count},
            {abort_count},
            {busy_family},
            {conflict_reason},
            {fallback_reason},
            {external_wait},
            {coordination_strategy},
            {diagnostics_available},
            {suggested_next_inspection},
            {first_failure_diag}
        );",
        event_seq = diagnostic.event_seq,
        event_ts_ms = 2_000 + diagnostic.event_seq,
        diagnostic_surface = sql_text(diagnostic.diagnostic_surface),
        statement_fingerprint = sql_text(diagnostic.statement_fingerprint),
        plan_id = sql_text(diagnostic.plan_id),
        table_name = sql_text(diagnostic.table_name),
        index_name = sql_optional_text(diagnostic.index_name),
        range_id = sql_optional_text(diagnostic.range_id),
        queue_name = sql_optional_text(diagnostic.queue_name),
        lease_key = sql_optional_text(diagnostic.lease_key),
        worker_id = sql_text(diagnostic.worker_id),
        transaction_id = sql_text(diagnostic.transaction_id),
        hotspot_kind = sql_text(diagnostic.hotspot_kind),
        page_number = sql_optional_i64(diagnostic.page_number),
        page_start = sql_optional_i64(diagnostic.page_start),
        page_end = sql_optional_i64(diagnostic.page_end),
        predicted_conflict_count = diagnostic.predicted_conflict_count,
        observed_conflict_count = diagnostic.observed_conflict_count,
        retry_count = diagnostic.retry_count,
        abort_count = diagnostic.abort_count,
        busy_family = sql_text(diagnostic.busy_family),
        conflict_reason = sql_optional_text(diagnostic.conflict_reason),
        fallback_reason = sql_optional_text(diagnostic.fallback_reason),
        external_wait = sql_optional_text(diagnostic.external_wait),
        coordination_strategy = sql_text(diagnostic.coordination_strategy),
        diagnostics_available = sql_bool(diagnostic.diagnostics_available),
        suggested_next_inspection = sql_text(diagnostic.suggested_next_inspection),
        first_failure_diag = sql_text(diagnostic.first_failure_diag),
    ))
    .await?;
    trace_concurrency_diagnostic(&diagnostic);
    Ok(())
}

fn trace_concurrency_diagnostic(diagnostic: &ConcurrencyDiagnostic<'_>) {
    tracing::info!(
        target: "fsqlite.explain_concurrency_contract",
        diagnostic_surface = diagnostic.diagnostic_surface,
        statement_fingerprint = diagnostic.statement_fingerprint,
        plan_id = diagnostic.plan_id,
        table_name = diagnostic.table_name,
        index_name = diagnostic.index_name.unwrap_or("none"),
        range_id = diagnostic.range_id.unwrap_or("none"),
        queue_name = diagnostic.queue_name.unwrap_or("none"),
        lease_key = diagnostic.lease_key.unwrap_or("none"),
        worker_id = diagnostic.worker_id,
        transaction_id = diagnostic.transaction_id,
        hotspot_kind = diagnostic.hotspot_kind,
        page_number = diagnostic.page_number.unwrap_or(-1),
        predicted_conflict_count = diagnostic.predicted_conflict_count,
        observed_conflict_count = diagnostic.observed_conflict_count,
        retry_count = diagnostic.retry_count,
        abort_count = diagnostic.abort_count,
        busy_family = diagnostic.busy_family,
        conflict_reason = diagnostic.conflict_reason.unwrap_or("none"),
        fallback_reason = diagnostic.fallback_reason.unwrap_or("none"),
        external_wait = diagnostic.external_wait.unwrap_or("none"),
        coordination_strategy = diagnostic.coordination_strategy,
        diagnostics_available = diagnostic.diagnostics_available,
        suggested_next_inspection = diagnostic.suggested_next_inspection,
        first_failure_diag = diagnostic.first_failure_diag,
    );
}

async fn seed_diagnostic_examples(conn: &Connection) -> TestResult {
    let examples = [
        ConcurrencyDiagnostic {
            event_seq: 1,
            diagnostic_surface: "EXPLAIN CONCURRENCY",
            statement_fingerprint: "fp-low-conflict-insert",
            plan_id: "plan-insert-disjoint-leaf",
            table_name: "jobs",
            index_name: Some("sqlite_autoindex_jobs_1"),
            range_id: Some("range-worker-a"),
            queue_name: None,
            lease_key: None,
            worker_id: "worker-a",
            transaction_id: "txn-100",
            hotspot_kind: "none",
            page_number: Some(40),
            page_start: Some(40),
            page_end: Some(40),
            predicted_conflict_count: 0,
            observed_conflict_count: 0,
            retry_count: 0,
            abort_count: 0,
            busy_family: "none",
            conflict_reason: Some("ok_low_conflict"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "page_mvcc",
            diagnostics_available: true,
            suggested_next_inspection: "none",
            first_failure_diag: "none",
        },
        ConcurrencyDiagnostic {
            event_seq: 2,
            diagnostic_surface: "EXPLAIN CONCURRENCY",
            statement_fingerprint: "fp-hot-page-update",
            plan_id: "plan-update-leaf-47",
            table_name: "jobs",
            index_name: None,
            range_id: None,
            queue_name: None,
            lease_key: None,
            worker_id: "worker-b",
            transaction_id: "txn-101",
            hotspot_kind: "page",
            page_number: Some(47),
            page_start: Some(47),
            page_end: Some(47),
            predicted_conflict_count: 3,
            observed_conflict_count: 2,
            retry_count: 1,
            abort_count: 1,
            busy_family: "busy_snapshot",
            conflict_reason: Some("hot_page_predicted"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "page_mvcc",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_page_heat",
            first_failure_diag: "same page had two concurrent writers",
        },
        ConcurrencyDiagnostic {
            event_seq: 3,
            diagnostic_surface: "PRAGMA fsqlite.concurrency_status",
            statement_fingerprint: "fp-queue-claim",
            plan_id: "plan-queue-claim",
            table_name: "fsqlite_queue",
            index_name: Some("idx_fsqlite_queue_claim"),
            range_id: None,
            queue_name: Some("replay-work"),
            lease_key: None,
            worker_id: "worker-c",
            transaction_id: "txn-102",
            hotspot_kind: "coordination",
            page_number: None,
            page_start: None,
            page_end: None,
            predicted_conflict_count: 1,
            observed_conflict_count: 1,
            retry_count: 1,
            abort_count: 0,
            busy_family: "busy",
            conflict_reason: Some("coordination_wait"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "coordination_row",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_coordination_owner",
            first_failure_diag: "queue item already claimed by worker-a",
        },
        ConcurrencyDiagnostic {
            event_seq: 4,
            diagnostic_surface: "PRAGMA fsqlite.concurrency_status",
            statement_fingerprint: "fp-lease-renew",
            plan_id: "plan-lease-renew",
            table_name: "fsqlite_lease",
            index_name: Some("sqlite_autoindex_fsqlite_lease_1"),
            range_id: None,
            queue_name: None,
            lease_key: Some("writer-slo-budget"),
            worker_id: "worker-d",
            transaction_id: "txn-103",
            hotspot_kind: "coordination",
            page_number: None,
            page_start: None,
            page_end: None,
            predicted_conflict_count: 1,
            observed_conflict_count: 1,
            retry_count: 0,
            abort_count: 0,
            busy_family: "none",
            conflict_reason: Some("coordination_wait"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "coordination_row",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_coordination_owner",
            first_failure_diag: "lease owner token mismatch",
        },
        ConcurrencyDiagnostic {
            event_seq: 5,
            diagnostic_surface: "EXPLAIN CONCURRENCY",
            statement_fingerprint: "fp-range-backfill",
            plan_id: "plan-range-disjoint",
            table_name: "events",
            index_name: Some("events_rowid"),
            range_id: Some("range-worker-e"),
            queue_name: None,
            lease_key: None,
            worker_id: "worker-e",
            transaction_id: "txn-104",
            hotspot_kind: "range",
            page_number: None,
            page_start: Some(100),
            page_end: Some(108),
            predicted_conflict_count: 0,
            observed_conflict_count: 0,
            retry_count: 0,
            abort_count: 0,
            busy_family: "none",
            conflict_reason: Some("ok_low_conflict"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "page_mvcc",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_range_balance",
            first_failure_diag: "none",
        },
        ConcurrencyDiagnostic {
            event_seq: 6,
            diagnostic_surface: "EXPLAIN CONCURRENCY",
            statement_fingerprint: "fp-window-fallback",
            plan_id: "plan-window-compat",
            table_name: "events",
            index_name: None,
            range_id: None,
            queue_name: None,
            lease_key: None,
            worker_id: "worker-f",
            transaction_id: "txn-105",
            hotspot_kind: "fallback",
            page_number: None,
            page_start: None,
            page_end: None,
            predicted_conflict_count: 0,
            observed_conflict_count: 0,
            retry_count: 0,
            abort_count: 0,
            busy_family: "none",
            conflict_reason: None,
            fallback_reason: Some("compatibility_fallback"),
            external_wait: None,
            coordination_strategy: "diagnostic_only",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_fallback_reason",
            first_failure_diag: "window frame still uses compatibility path",
        },
        ConcurrencyDiagnostic {
            event_seq: 7,
            diagnostic_surface: "PRAGMA fsqlite.concurrency_status",
            statement_fingerprint: "fp-vfs-wait",
            plan_id: "plan-checkpoint",
            table_name: "events",
            index_name: None,
            range_id: None,
            queue_name: None,
            lease_key: None,
            worker_id: "worker-g",
            transaction_id: "txn-106",
            hotspot_kind: "external",
            page_number: None,
            page_start: None,
            page_end: None,
            predicted_conflict_count: 0,
            observed_conflict_count: 0,
            retry_count: 2,
            abort_count: 0,
            busy_family: "busy_recovery",
            conflict_reason: Some("external_resource_wait"),
            fallback_reason: None,
            external_wait: Some("checkpoint_backpressure"),
            coordination_strategy: "external_wait",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_vfs_or_runtime_wait",
            first_failure_diag: "checkpoint backpressure delayed publication",
        },
    ];

    for example in examples {
        record_diagnostic(conn, example).await?;
    }
    Ok(())
}

#[test]
fn reason_codes_are_stable_and_dimensioned() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_concurrency_schema(&conn).await?;

            let rows = conn
                .query(
                    "SELECT reason_code,
                hotspot_kind,
                retryable,
                contention_family,
                concurrency_impact,
                suggested_next_inspection
           FROM fsqlite_concurrency_reason_codes_contract
          ORDER BY reason_code;",
                )
                .await?;

            assert_eq!(
                rows_to_values(&rows),
                vec![
                    vec![
                        SqliteValue::Text("compatibility_fallback".into()),
                        SqliteValue::Text("fallback".into()),
                        SqliteValue::Integer(0),
                        SqliteValue::Text("compatibility".into()),
                        SqliteValue::Text("may_reduce_parallelism".into()),
                        SqliteValue::Text("inspect_fallback_reason".into()),
                    ],
                    vec![
                        SqliteValue::Text("coordination_wait".into()),
                        SqliteValue::Text("coordination".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("queue_lease_range".into()),
                        SqliteValue::Text("ownership_wait_only".into()),
                        SqliteValue::Text("inspect_coordination_owner".into()),
                    ],
                    vec![
                        SqliteValue::Text("diagnostics_unavailable".into()),
                        SqliteValue::Text("unknown".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("unknown".into()),
                        SqliteValue::Text("unknown".into()),
                        SqliteValue::Text("inspect_first_failure_diag".into()),
                    ],
                    vec![
                        SqliteValue::Text("external_resource_wait".into()),
                        SqliteValue::Text("external".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("external".into()),
                        SqliteValue::Text("outside_mvcc".into()),
                        SqliteValue::Text("inspect_vfs_or_runtime_wait".into()),
                    ],
                    vec![
                        SqliteValue::Text("hot_page_predicted".into()),
                        SqliteValue::Text("page".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("page_mvcc".into()),
                        SqliteValue::Text("retry_may_increase".into()),
                        SqliteValue::Text("inspect_page_heat".into()),
                    ],
                    vec![
                        SqliteValue::Text("ok_low_conflict".into()),
                        SqliteValue::Text("none".into()),
                        SqliteValue::Integer(0),
                        SqliteValue::Text("page_mvcc".into()),
                        SqliteValue::Text("lock_free_expected".into()),
                        SqliteValue::Text("none".into()),
                    ],
                ]
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn diagnostic_rows_cover_operator_contention_questions() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_concurrency_schema(&conn).await?;
            seed_diagnostic_examples(&conn).await?;

            let rows = conn
                .query(
                    "SELECT hotspot_kind,
                statement_fingerprint,
                plan_id,
                table_name,
                page_number,
                busy_family,
                conflict_reason,
                fallback_reason,
                external_wait,
                suggested_next_inspection
           FROM fsqlite_explain_concurrency_contract
          ORDER BY event_seq;",
                )
                .await?;

            assert_eq!(
                rows_to_values(&rows),
                vec![
                    vec![
                        SqliteValue::Text("none".into()),
                        SqliteValue::Text("fp-low-conflict-insert".into()),
                        SqliteValue::Text("plan-insert-disjoint-leaf".into()),
                        SqliteValue::Text("jobs".into()),
                        SqliteValue::Integer(40),
                        SqliteValue::Text("none".into()),
                        SqliteValue::Text("ok_low_conflict".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("none".into()),
                    ],
                    vec![
                        SqliteValue::Text("page".into()),
                        SqliteValue::Text("fp-hot-page-update".into()),
                        SqliteValue::Text("plan-update-leaf-47".into()),
                        SqliteValue::Text("jobs".into()),
                        SqliteValue::Integer(47),
                        SqliteValue::Text("busy_snapshot".into()),
                        SqliteValue::Text("hot_page_predicted".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_page_heat".into()),
                    ],
                    vec![
                        SqliteValue::Text("coordination".into()),
                        SqliteValue::Text("fp-queue-claim".into()),
                        SqliteValue::Text("plan-queue-claim".into()),
                        SqliteValue::Text("fsqlite_queue".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("busy".into()),
                        SqliteValue::Text("coordination_wait".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_coordination_owner".into()),
                    ],
                    vec![
                        SqliteValue::Text("coordination".into()),
                        SqliteValue::Text("fp-lease-renew".into()),
                        SqliteValue::Text("plan-lease-renew".into()),
                        SqliteValue::Text("fsqlite_lease".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("none".into()),
                        SqliteValue::Text("coordination_wait".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_coordination_owner".into()),
                    ],
                    vec![
                        SqliteValue::Text("range".into()),
                        SqliteValue::Text("fp-range-backfill".into()),
                        SqliteValue::Text("plan-range-disjoint".into()),
                        SqliteValue::Text("events".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("none".into()),
                        SqliteValue::Text("ok_low_conflict".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_range_balance".into()),
                    ],
                    vec![
                        SqliteValue::Text("fallback".into()),
                        SqliteValue::Text("fp-window-fallback".into()),
                        SqliteValue::Text("plan-window-compat".into()),
                        SqliteValue::Text("events".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("none".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("compatibility_fallback".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_fallback_reason".into()),
                    ],
                    vec![
                        SqliteValue::Text("external".into()),
                        SqliteValue::Text("fp-vfs-wait".into()),
                        SqliteValue::Text("plan-checkpoint".into()),
                        SqliteValue::Text("events".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("busy_recovery".into()),
                        SqliteValue::Text("external_resource_wait".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("checkpoint_backpressure".into()),
                        SqliteValue::Text("inspect_vfs_or_runtime_wait".into()),
                    ],
                ]
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn summary_surface_distinguishes_mvcc_coordination_fallback_and_external_waits() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_concurrency_schema(&conn).await?;
            seed_diagnostic_examples(&conn).await?;

            let rows = conn
                .query(
                    "SELECT hotspot_kind,
                COUNT(*),
                SUM(predicted_conflict_count),
                SUM(observed_conflict_count),
                SUM(retry_count),
                SUM(abort_count)
           FROM fsqlite_explain_concurrency_contract
          GROUP BY hotspot_kind
          ORDER BY hotspot_kind;",
                )
                .await?;

            assert_eq!(
                rows_to_values(&rows),
                vec![
                    vec![
                        SqliteValue::Text("coordination".into()),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                    ],
                    vec![
                        SqliteValue::Text("external".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(0),
                    ],
                    vec![
                        SqliteValue::Text("fallback".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                    ],
                    vec![
                        SqliteValue::Text("none".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                    ],
                    vec![
                        SqliteValue::Text("page".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(3),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(1),
                    ],
                    vec![
                        SqliteValue::Text("range".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                    ],
                ]
            );

            let serialized_rows = conn
                .query(
                    "SELECT coordination_strategy
           FROM fsqlite_explain_concurrency_contract
          WHERE coordination_strategy = 'global_writer_lock';",
                )
                .await?;
            assert!(
                serialized_rows.is_empty(),
                "diagnostics must never prescribe a global writer lock"
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn rollback_removes_unpublished_diagnostics() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_concurrency_schema(&conn).await?;

            conn.execute("BEGIN;").await?;
            record_diagnostic(
                &conn,
                ConcurrencyDiagnostic {
                    event_seq: 99,
                    diagnostic_surface: "EXPLAIN CONCURRENCY",
                    statement_fingerprint: "fp-rolled-back",
                    plan_id: "plan-rolled-back",
                    table_name: "jobs",
                    index_name: None,
                    range_id: None,
                    queue_name: None,
                    lease_key: None,
                    worker_id: "worker-z",
                    transaction_id: "txn-rollback",
                    hotspot_kind: "page",
                    page_number: Some(55),
                    page_start: Some(55),
                    page_end: Some(55),
                    predicted_conflict_count: 1,
                    observed_conflict_count: 1,
                    retry_count: 0,
                    abort_count: 0,
                    busy_family: "busy_snapshot",
                    conflict_reason: Some("hot_page_predicted"),
                    fallback_reason: None,
                    external_wait: None,
                    coordination_strategy: "page_mvcc",
                    diagnostics_available: true,
                    suggested_next_inspection: "inspect_page_heat",
                    first_failure_diag: "temporary page hotspot",
                },
            )
            .await?;

            let inside_txn = conn
                .query(
                    "SELECT COUNT(*)
           FROM fsqlite_explain_concurrency_contract
          WHERE statement_fingerprint = 'fp-rolled-back';",
                )
                .await?;
            assert_eq!(
                rows_to_values(&inside_txn),
                vec![vec![SqliteValue::Integer(1)]]
            );

            conn.execute("ROLLBACK;").await?;

            let after_rollback = conn
                .query(
                    "SELECT COUNT(*)
           FROM fsqlite_explain_concurrency_contract
          WHERE statement_fingerprint = 'fp-rolled-back';",
                )
                .await?;
            assert_eq!(
                rows_to_values(&after_rollback),
                vec![vec![SqliteValue::Integer(0)]]
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}
