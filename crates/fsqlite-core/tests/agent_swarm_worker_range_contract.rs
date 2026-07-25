use std::collections::BTreeSet;

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const RANGE_NAME: &str = "bulk";
const TABLE_NAME: &str = "agent_jobs";

#[derive(Clone, Copy)]
struct RangeSpec<'a> {
    range_id: &'a str,
    start_key: i64,
    end_key: i64,
    page_start: i64,
    page_end: i64,
    split_parent: Option<&'a str>,
}

fn contract_error(message: impl Into<String>) -> Box<dyn std::error::Error> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

fn encoded_key(value: i64) -> String {
    format!("{value:016}")
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_optional_text(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), sql_text)
}

fn only_row_values<'a>(rows: &'a [Row], context: &str) -> TestResult<&'a [SqliteValue]> {
    if rows.len() != 1 {
        return Err(contract_error(format!(
            "{context} expected exactly one row, got {}",
            rows.len()
        )));
    }
    rows.first()
        .map(Row::values)
        .ok_or_else(|| contract_error(format!("{context} returned no rows")))
}

async fn install_range_schema(conn: &Connection) -> TestResult {
    conn.execute(
        "CREATE TABLE fsqlite_worker_range_reason_codes_contract(
            reason_code TEXT NOT NULL PRIMARY KEY,
            severity TEXT NOT NULL,
            retryable INTEGER NOT NULL,
            operator_hint TEXT NOT NULL
        );",
    )
    .await?;
    conn.execute(
        "CREATE TABLE fsqlite_worker_ranges_contract(
            range_name TEXT NOT NULL,
            range_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            index_name TEXT,
            range_start TEXT NOT NULL,
            range_end TEXT NOT NULL,
            owner_id TEXT,
            lease_token TEXT,
            generation INTEGER NOT NULL DEFAULT 1,
            mode TEXT NOT NULL,
            state TEXT NOT NULL,
            assigned_at_ms INTEGER,
            renewed_at_ms INTEGER,
            expires_at_ms INTEGER,
            split_parent TEXT,
            observed_conflict_count INTEGER NOT NULL DEFAULT 0,
            predicted_page_start INTEGER NOT NULL,
            predicted_page_end INTEGER NOT NULL,
            imbalance_reason TEXT NOT NULL,
            last_reason_code TEXT,
            PRIMARY KEY(range_name, range_id)
        );",
    )
    .await?;
    conn.execute(
        "CREATE INDEX idx_fsqlite_worker_ranges_contract_allocator
            ON fsqlite_worker_ranges_contract(
                range_name,
                table_name,
                index_name,
                state,
                range_start
            );",
    )
    .await?;
    conn.execute(
        "CREATE TABLE fsqlite_worker_range_conflict_samples_contract(
            strategy TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            page_number INTEGER NOT NULL
        );",
    )
    .await?;
    seed_range_reason_codes(conn).await?;
    Ok(())
}

async fn seed_range_reason_codes(conn: &Connection) -> TestResult {
    conn.execute(
        "INSERT INTO fsqlite_worker_range_reason_codes_contract(
            reason_code,
            severity,
            retryable,
            operator_hint
        ) VALUES
            ('ok', 'info', 0, 'range mutation succeeded'),
            ('range_exhausted', 'notice', 1, 'split or release a compatible range'),
            ('range_overlap', 'warning', 1, 'choose disjoint key bounds'),
            ('range_gap', 'warning', 0, 'merge only adjacent ranges'),
            ('range_owner_mismatch', 'warning', 1, 'refresh owner and generation'),
            ('range_invalid_bounds', 'error', 0, 'range_start must sort before range_end'),
            ('range_generation_conflict', 'warning', 1, 'retry after reading current generation'),
            ('range_invalid_state', 'error', 0, 'requested range transition is illegal'),
            ('range_enforced_unsupported', 'notice', 0, 'initial implementation is advisory');",
    )
    .await?;
    Ok(())
}

async fn seed_range(conn: &Connection, spec: RangeSpec<'_>) -> TestResult {
    conn.execute(&format!(
        "INSERT INTO fsqlite_worker_ranges_contract(
            range_name,
            range_id,
            table_name,
            index_name,
            range_start,
            range_end,
            owner_id,
            lease_token,
            generation,
            mode,
            state,
            assigned_at_ms,
            renewed_at_ms,
            expires_at_ms,
            split_parent,
            observed_conflict_count,
            predicted_page_start,
            predicted_page_end,
            imbalance_reason,
            last_reason_code
        ) VALUES (
            {range_name},
            {range_id},
            {table_name},
            NULL,
            {range_start},
            {range_end},
            NULL,
            NULL,
            1,
            'advisory',
            'available',
            NULL,
            NULL,
            NULL,
            {split_parent},
            0,
            {page_start},
            {page_end},
            'balanced',
            'ok'
        );",
        range_name = sql_text(RANGE_NAME),
        range_id = sql_text(spec.range_id),
        table_name = sql_text(TABLE_NAME),
        range_start = sql_text(&encoded_key(spec.start_key)),
        range_end = sql_text(&encoded_key(spec.end_key)),
        split_parent = sql_optional_text(spec.split_parent),
        page_start = spec.page_start,
        page_end = spec.page_end
    ))
    .await?;
    Ok(())
}

fn allocate_sql(worker_id: &str, lease_token: &str, now_ms: i64, ttl_ms: i64) -> String {
    format!(
        "UPDATE fsqlite_worker_ranges_contract
            SET state = 'assigned',
                owner_id = {worker_id},
                lease_token = {lease_token},
                generation = generation + 1,
                assigned_at_ms = {now_ms},
                renewed_at_ms = {now_ms},
                expires_at_ms = {now_ms} + {ttl_ms},
                last_reason_code = 'ok'
          WHERE range_name = {range_name}
            AND range_id = (
                SELECT range_id
                  FROM fsqlite_worker_ranges_contract
                 WHERE range_name = {range_name}
                   AND table_name = {table_name}
                   AND index_name IS NULL
                   AND state IN ('available', 'released')
                   AND mode = 'advisory'
                 ORDER BY range_start ASC, range_id ASC
                 LIMIT 1
            )
          RETURNING range_id, owner_id, lease_token, generation, range_start,
                    range_end, predicted_page_start, predicted_page_end,
                    last_reason_code;",
        worker_id = sql_text(worker_id),
        lease_token = sql_text(lease_token),
        range_name = sql_text(RANGE_NAME),
        table_name = sql_text(TABLE_NAME)
    )
}

fn renew_sql(
    range_id: &str,
    worker_id: &str,
    lease_token: &str,
    generation: i64,
    now_ms: i64,
    ttl_ms: i64,
) -> String {
    format!(
        "UPDATE fsqlite_worker_ranges_contract
            SET renewed_at_ms = {now_ms},
                expires_at_ms = {now_ms} + {ttl_ms},
                last_reason_code = 'ok'
          WHERE range_name = {range_name}
            AND range_id = {range_id}
            AND owner_id = {worker_id}
            AND lease_token = {lease_token}
            AND generation = {generation}
            AND state = 'assigned'
            AND expires_at_ms > {now_ms}
          RETURNING range_id, owner_id, generation, expires_at_ms,
                    renewed_at_ms, last_reason_code;",
        range_name = sql_text(RANGE_NAME),
        range_id = sql_text(range_id),
        worker_id = sql_text(worker_id),
        lease_token = sql_text(lease_token)
    )
}

fn release_sql(range_id: &str, worker_id: &str, generation: i64, now_ms: i64) -> String {
    format!(
        "UPDATE fsqlite_worker_ranges_contract
            SET state = 'released',
                owner_id = NULL,
                lease_token = NULL,
                expires_at_ms = NULL,
                renewed_at_ms = {now_ms},
                last_reason_code = 'ok'
          WHERE range_name = {range_name}
            AND range_id = {range_id}
            AND owner_id = {worker_id}
            AND generation = {generation}
            AND state = 'assigned'
          RETURNING range_id, state, generation, last_reason_code;",
        range_name = sql_text(RANGE_NAME),
        range_id = sql_text(range_id),
        worker_id = sql_text(worker_id)
    )
}

fn retire_owned_sql(range_id: &str, worker_id: &str, generation: i64) -> String {
    format!(
        "UPDATE fsqlite_worker_ranges_contract
            SET state = 'retired',
                owner_id = NULL,
                lease_token = NULL,
                expires_at_ms = NULL,
                last_reason_code = 'ok'
          WHERE range_name = {range_name}
            AND range_id = {range_id}
            AND owner_id = {worker_id}
            AND generation = {generation}
            AND state = 'assigned'
          RETURNING range_id, state, last_reason_code;",
        range_name = sql_text(RANGE_NAME),
        range_id = sql_text(range_id),
        worker_id = sql_text(worker_id)
    )
}

fn retire_available_sql(range_id: &str) -> String {
    format!(
        "UPDATE fsqlite_worker_ranges_contract
            SET state = 'retired',
                last_reason_code = 'ok'
          WHERE range_name = {range_name}
            AND range_id = {range_id}
            AND state IN ('available', 'released')
          RETURNING range_id, state, last_reason_code;",
        range_name = sql_text(RANGE_NAME),
        range_id = sql_text(range_id)
    )
}

fn insert_available_range_sql(spec: RangeSpec<'_>) -> String {
    let range_start = encoded_key(spec.start_key);
    let range_end = encoded_key(spec.end_key);
    format!(
        "INSERT INTO fsqlite_worker_ranges_contract(
            range_name,
            range_id,
            table_name,
            index_name,
            range_start,
            range_end,
            owner_id,
            lease_token,
            generation,
            mode,
            state,
            assigned_at_ms,
            renewed_at_ms,
            expires_at_ms,
            split_parent,
            observed_conflict_count,
            predicted_page_start,
            predicted_page_end,
            imbalance_reason,
            last_reason_code
        )
        SELECT
            {range_name},
            {range_id},
            {table_name},
            NULL,
            {range_start},
            {range_end},
            NULL,
            NULL,
            1,
            'advisory',
            'available',
            NULL,
            NULL,
            NULL,
            {split_parent},
            0,
            {page_start},
            {page_end},
            'balanced',
            'ok'
         WHERE {range_start} < {range_end}
           AND NOT EXISTS (
                SELECT 1
                  FROM fsqlite_worker_ranges_contract
                 WHERE range_name = {range_name}
                   AND table_name = {table_name}
                   AND index_name IS NULL
                   AND state <> 'retired'
                   AND NOT (range_end <= {range_start}
                            OR range_start >= {range_end})
           )
        RETURNING range_id, range_start, range_end, state, split_parent,
                  last_reason_code;",
        range_name = sql_text(RANGE_NAME),
        range_id = sql_text(spec.range_id),
        table_name = sql_text(TABLE_NAME),
        range_start = sql_text(&range_start),
        range_end = sql_text(&range_end),
        split_parent = sql_optional_text(spec.split_parent),
        page_start = spec.page_start,
        page_end = spec.page_end
    )
}

async fn record_conflict_sample(
    conn: &Connection,
    strategy: &str,
    worker_id: &str,
    page_number: i64,
) -> TestResult {
    conn.execute(&format!(
        "INSERT INTO fsqlite_worker_range_conflict_samples_contract(
            strategy,
            worker_id,
            page_number
        ) VALUES (
            {strategy},
            {worker_id},
            {page_number}
        );",
        strategy = sql_text(strategy),
        worker_id = sql_text(worker_id)
    ))
    .await?;
    Ok(())
}

async fn modeled_same_page_conflicts(conn: &Connection, strategy: &str) -> TestResult<i64> {
    let rows = conn
        .query(&format!(
            "SELECT worker_id, page_number
           FROM fsqlite_worker_range_conflict_samples_contract
          WHERE strategy = {strategy};",
            strategy = sql_text(strategy)
        ))
        .await?;
    let mut pages = BTreeSet::new();
    for row in &rows {
        match row.values() {
            [SqliteValue::Text(_), SqliteValue::Integer(page_number)] => {
                pages.insert(*page_number);
            }
            other => {
                return Err(contract_error(format!(
                    "conflict sample row had unexpected shape: {other:?}"
                )));
            }
        }
    }
    let worker_count = i64::try_from(rows.len())?;
    let distinct_pages = i64::try_from(pages.len())?;
    Ok(worker_count - distinct_pages)
}

fn trace_range_event(
    operation: &str,
    range_id: &str,
    worker_id: &str,
    reason_code: &str,
    imbalance_reason: &str,
) {
    tracing::info!(
        target: "fsqlite.worker_range_contract",
        range_name = RANGE_NAME,
        range_id,
        table_name = TABLE_NAME,
        worker_id,
        operation,
        reason_code,
        imbalance_reason,
        "worker range contract event"
    );
}

#[test]
fn range_reason_codes_and_introspection_fields_are_stable() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(
                conn.is_concurrent_mode_default(),
                "worker range coordination contract must not disable concurrent-writer mode"
            );
            install_range_schema(&conn).await?;
            seed_range(
                &conn,
                RangeSpec {
                    range_id: "range-a",
                    start_key: 0,
                    end_key: 1_000,
                    page_start: 0,
                    page_end: 3,
                    split_parent: None,
                },
            )
            .await?;

            let codes = conn
                .query(
                    "SELECT reason_code, severity, retryable, operator_hint
           FROM fsqlite_worker_range_reason_codes_contract
          ORDER BY reason_code;",
                )
                .await?;
            assert_eq!(codes.len(), 9);
            assert_eq!(
                only_row_values(
                    &conn
                        .query(
                            "SELECT reason_code, severity, retryable, operator_hint
                   FROM fsqlite_worker_range_reason_codes_contract
                  WHERE reason_code = 'range_overlap';",
                        )
                        .await?,
                    "range_overlap reason code",
                )?,
                &[
                    SqliteValue::Text("range_overlap".into()),
                    SqliteValue::Text("warning".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("choose disjoint key bounds".into()),
                ]
            );

            let introspection = conn
                .query(
                    "SELECT range_name, range_id, table_name, index_name, range_start,
                range_end, owner_id, mode, state, predicted_page_start,
                predicted_page_end, observed_conflict_count, imbalance_reason,
                last_reason_code
           FROM fsqlite_worker_ranges_contract
          WHERE range_name = 'bulk'
          ORDER BY table_name, index_name, range_start;",
                )
                .await?;
            assert_eq!(
                only_row_values(&introspection, "worker range introspection")?,
                &[
                    SqliteValue::Text(RANGE_NAME.into()),
                    SqliteValue::Text("range-a".into()),
                    SqliteValue::Text(TABLE_NAME.into()),
                    SqliteValue::Null,
                    SqliteValue::Text(encoded_key(0).into()),
                    SqliteValue::Text(encoded_key(1_000).into()),
                    SqliteValue::Null,
                    SqliteValue::Text("advisory".into()),
                    SqliteValue::Text("available".into()),
                    SqliteValue::Integer(0),
                    SqliteValue::Integer(3),
                    SqliteValue::Integer(0),
                    SqliteValue::Text("balanced".into()),
                    SqliteValue::Text("ok".into()),
                ],
                "introspection must expose stable range, planner, and diagnostic fields"
            );

            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn range_allocate_renew_release_and_exhausted_paths_are_deterministic() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(
                conn.is_concurrent_mode_default(),
                "worker range coordination contract must not disable concurrent-writer mode"
            );
            install_range_schema(&conn).await?;
            for spec in [
                RangeSpec {
                    range_id: "range-a",
                    start_key: 0,
                    end_key: 500,
                    page_start: 0,
                    page_end: 1,
                    split_parent: None,
                },
                RangeSpec {
                    range_id: "range-b",
                    start_key: 500,
                    end_key: 1_000,
                    page_start: 2,
                    page_end: 3,
                    split_parent: None,
                },
            ] {
                seed_range(&conn, spec).await?;
            }

            let allocated = conn
                .query(&allocate_sql("worker-a", "token-a", 100, 1_000))
                .await?;
            trace_range_event("allocate", "range-a", "worker-a", "ok", "balanced");
            assert_eq!(
                only_row_values(&allocated, "first worker range allocation")?,
                &[
                    SqliteValue::Text("range-a".into()),
                    SqliteValue::Text("worker-a".into()),
                    SqliteValue::Text("token-a".into()),
                    SqliteValue::Integer(2),
                    SqliteValue::Text(encoded_key(0).into()),
                    SqliteValue::Text(encoded_key(500).into()),
                    SqliteValue::Integer(0),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let stale_renew = conn
                .query(&renew_sql("range-a", "worker-a", "token-a", 1, 200, 1_000))
                .await?;
            assert!(
                stale_renew.is_empty(),
                "range renew must require the current generation"
            );

            let renewed = conn
                .query(&renew_sql("range-a", "worker-a", "token-a", 2, 300, 1_200))
                .await?;
            trace_range_event("renew", "range-a", "worker-a", "ok", "balanced");
            assert_eq!(
                only_row_values(&renewed, "range renewal")?,
                &[
                    SqliteValue::Text("range-a".into()),
                    SqliteValue::Text("worker-a".into()),
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(1_500),
                    SqliteValue::Integer(300),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let wrong_owner_release = conn
                .query(&release_sql("range-a", "worker-b", 2, 400))
                .await?;
            assert!(
                wrong_owner_release.is_empty(),
                "range release must require the current owner and generation"
            );

            let released = conn
                .query(&release_sql("range-a", "worker-a", 2, 500))
                .await?;
            trace_range_event("release", "range-a", "worker-a", "ok", "balanced");
            assert_eq!(
                only_row_values(&released, "range release")?,
                &[
                    SqliteValue::Text("range-a".into()),
                    SqliteValue::Text("released".into()),
                    SqliteValue::Integer(2),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let reassigned = conn
                .query(&allocate_sql("worker-b", "token-b", 600, 1_000))
                .await?;
            trace_range_event("allocate", "range-a", "worker-b", "ok", "balanced");
            assert_eq!(
                only_row_values(&reassigned, "released range reassignment")?,
                &[
                    SqliteValue::Text("range-a".into()),
                    SqliteValue::Text("worker-b".into()),
                    SqliteValue::Text("token-b".into()),
                    SqliteValue::Integer(3),
                    SqliteValue::Text(encoded_key(0).into()),
                    SqliteValue::Text(encoded_key(500).into()),
                    SqliteValue::Integer(0),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let second_assignment = conn
                .query(&allocate_sql("worker-c", "token-c", 700, 1_000))
                .await?;
            trace_range_event("allocate", "range-b", "worker-c", "ok", "balanced");
            assert_eq!(second_assignment.len(), 1);

            let exhausted = conn
                .query(&allocate_sql("worker-d", "token-d", 800, 1_000))
                .await?;
            trace_range_event(
                "allocate",
                "none",
                "worker-d",
                "range_exhausted",
                "split_or_release_required",
            );
            assert!(
                exhausted.is_empty(),
                "exhausted allocator must return no ownership row"
            );

            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn range_overlap_invalid_bounds_split_and_merge_are_enforced() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(
                conn.is_concurrent_mode_default(),
                "worker range coordination contract must not disable concurrent-writer mode"
            );
            install_range_schema(&conn).await?;
            seed_range(
                &conn,
                RangeSpec {
                    range_id: "range-parent",
                    start_key: 0,
                    end_key: 1_000,
                    page_start: 0,
                    page_end: 3,
                    split_parent: None,
                },
            )
            .await?;

            let allocated = conn
                .query(&allocate_sql("worker-split", "token-split", 100, 1_000))
                .await?;
            assert_eq!(allocated.len(), 1);
            let retired_parent = conn
                .query(&retire_owned_sql("range-parent", "worker-split", 2))
                .await?;
            trace_range_event(
                "split-retire-parent",
                "range-parent",
                "worker-split",
                "ok",
                "balanced",
            );
            assert_eq!(
                only_row_values(&retired_parent, "retired split parent")?,
                &[
                    SqliteValue::Text("range-parent".into()),
                    SqliteValue::Text("retired".into()),
                    SqliteValue::Text("ok".into()),
                ]
            );

            for spec in [
                RangeSpec {
                    range_id: "range-left",
                    start_key: 0,
                    end_key: 500,
                    page_start: 0,
                    page_end: 1,
                    split_parent: Some("range-parent"),
                },
                RangeSpec {
                    range_id: "range-right",
                    start_key: 500,
                    end_key: 1_000,
                    page_start: 2,
                    page_end: 3,
                    split_parent: Some("range-parent"),
                },
            ] {
                let split_child = conn.query(&insert_available_range_sql(spec)).await?;
                trace_range_event(
                    "split-child",
                    spec.range_id,
                    "worker-split",
                    "ok",
                    "balanced",
                );
                assert_eq!(split_child.len(), 1);
            }

            let overlapping = conn
                .query(&insert_available_range_sql(RangeSpec {
                    range_id: "range-overlap",
                    start_key: 400,
                    end_key: 600,
                    page_start: 1,
                    page_end: 2,
                    split_parent: Some("range-parent"),
                }))
                .await?;
            trace_range_event(
                "insert",
                "range-overlap",
                "worker-split",
                "range_overlap",
                "choose_disjoint_key_bounds",
            );
            assert!(
                overlapping.is_empty(),
                "overlapping active worker range must not be inserted"
            );

            let invalid_bounds = conn
                .query(&insert_available_range_sql(RangeSpec {
                    range_id: "range-invalid",
                    start_key: 700,
                    end_key: 600,
                    page_start: 2,
                    page_end: 2,
                    split_parent: Some("range-parent"),
                }))
                .await?;
            trace_range_event(
                "insert",
                "range-invalid",
                "worker-split",
                "range_invalid_bounds",
                "start_must_sort_before_end",
            );
            assert!(
                invalid_bounds.is_empty(),
                "range_start >= range_end must return no inserted range"
            );

            for range_id in ["range-left", "range-right"] {
                let retired = conn.query(&retire_available_sql(range_id)).await?;
                trace_range_event(
                    "merge-retire-source",
                    range_id,
                    "worker-merge",
                    "ok",
                    "balanced",
                );
                assert_eq!(retired.len(), 1);
            }
            let merged = conn
                .query(&insert_available_range_sql(RangeSpec {
                    range_id: "range-merged",
                    start_key: 0,
                    end_key: 1_000,
                    page_start: 0,
                    page_end: 3,
                    split_parent: None,
                }))
                .await?;
            trace_range_event(
                "merge-insert",
                "range-merged",
                "worker-merge",
                "ok",
                "balanced",
            );
            assert_eq!(
                only_row_values(&merged, "merged worker range")?,
                &[
                    SqliteValue::Text("range-merged".into()),
                    SqliteValue::Text(encoded_key(0).into()),
                    SqliteValue::Text(encoded_key(1_000).into()),
                    SqliteValue::Text("available".into()),
                    SqliteValue::Null,
                    SqliteValue::Text("ok".into()),
                ]
            );

            let active_ranges = conn
                .query(
                    "SELECT range_id, range_start, range_end
           FROM fsqlite_worker_ranges_contract
          WHERE state <> 'retired'
          ORDER BY range_start;",
                )
                .await?;
            assert_eq!(
                only_row_values(&active_ranges, "active merged range")?,
                &[
                    SqliteValue::Text("range-merged".into()),
                    SqliteValue::Text(encoded_key(0).into()),
                    SqliteValue::Text(encoded_key(1_000).into()),
                ],
                "merge must leave one active disjoint replacement range"
            );

            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn range_rollback_restores_available_assignment_state() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("range_rollback.db");
            let db = db_path.to_string_lossy().to_string();

            let conn = Connection::open(&db).await?;
            conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await?;
            assert!(
                conn.is_concurrent_mode_default(),
                "worker range rollback proof must keep concurrent-writer mode enabled"
            );
            install_range_schema(&conn).await?;
            seed_range(
                &conn,
                RangeSpec {
                    range_id: "range-rollback",
                    start_key: 0,
                    end_key: 1_000,
                    page_start: 0,
                    page_end: 3,
                    split_parent: None,
                },
            )
            .await?;

            conn.execute("BEGIN CONCURRENT;").await?;
            let allocated = conn
                .query(&allocate_sql(
                    "worker-rollback",
                    "token-rollback",
                    100,
                    1_000,
                ))
                .await?;
            trace_range_event(
                "allocate",
                "range-rollback",
                "worker-rollback",
                "ok",
                "balanced",
            );
            assert_eq!(allocated.len(), 1);
            conn.execute("ROLLBACK;").await?;

            let restored = conn
                .query(
                    "SELECT owner_id, lease_token, generation, state, expires_at_ms,
                last_reason_code
           FROM fsqlite_worker_ranges_contract
          WHERE range_name = 'bulk'
            AND range_id = 'range-rollback';",
                )
                .await?;
            assert_eq!(
                only_row_values(&restored, "rolled back range assignment")?,
                &[
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Integer(1),
                    SqliteValue::Text("available".into()),
                    SqliteValue::Null,
                    SqliteValue::Text("ok".into()),
                ],
                "rollback must restore worker range ownership and generation"
            );

            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn range_aware_assignment_reduces_modeled_same_page_conflicts() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(
                conn.is_concurrent_mode_default(),
                "worker range fairness proof must keep concurrent-writer mode enabled"
            );
            install_range_schema(&conn).await?;
            for spec in [
                RangeSpec {
                    range_id: "range-0",
                    start_key: 0,
                    end_key: 250,
                    page_start: 0,
                    page_end: 0,
                    split_parent: None,
                },
                RangeSpec {
                    range_id: "range-1",
                    start_key: 250,
                    end_key: 500,
                    page_start: 1,
                    page_end: 1,
                    split_parent: None,
                },
                RangeSpec {
                    range_id: "range-2",
                    start_key: 500,
                    end_key: 750,
                    page_start: 2,
                    page_end: 2,
                    split_parent: None,
                },
                RangeSpec {
                    range_id: "range-3",
                    start_key: 750,
                    end_key: 1_000,
                    page_start: 3,
                    page_end: 3,
                    split_parent: None,
                },
            ] {
                seed_range(&conn, spec).await?;
            }

            for worker_index in 0_i64..4 {
                let worker_id = format!("worker-{worker_index}");
                record_conflict_sample(&conn, "naive", &worker_id, 0).await?;
                let allocation = conn
                    .query(&allocate_sql(
                        &worker_id,
                        &format!("token-{worker_index}"),
                        100 + worker_index,
                        1_000,
                    ))
                    .await?;
                assert_eq!(allocation.len(), 1);
                record_conflict_sample(&conn, "range-aware", &worker_id, worker_index).await?;
            }

            let assigned = conn
                .query(
                    "SELECT range_id, owner_id, predicted_page_start, predicted_page_end,
                observed_conflict_count, imbalance_reason
           FROM fsqlite_worker_ranges_contract
          WHERE state = 'assigned'
          ORDER BY range_id;",
                )
                .await?;
            assert_eq!(assigned.len(), 4);
            for row in &assigned {
                match row.values() {
                    [
                        SqliteValue::Text(range_id),
                        SqliteValue::Text(owner_id),
                        SqliteValue::Integer(page_start),
                        SqliteValue::Integer(page_end),
                        SqliteValue::Integer(conflict_count),
                        SqliteValue::Text(imbalance_reason),
                    ] => {
                        assert_eq!(page_start, page_end);
                        assert_eq!(*conflict_count, 0);
                        assert_eq!(imbalance_reason.as_str(), "balanced");
                        trace_range_event("fairness", range_id, owner_id, "ok", imbalance_reason);
                    }
                    other => {
                        return Err(contract_error(format!(
                            "assigned range row had unexpected shape: {other:?}"
                        )));
                    }
                }
            }

            assert_eq!(
                modeled_same_page_conflicts(&conn, "naive").await?,
                3,
                "naive shared-key workers all target the same modeled page"
            );
            assert_eq!(
                modeled_same_page_conflicts(&conn, "range-aware").await?,
                0,
                "range-aware workers must model disjoint page ownership"
            );

            Ok(())
        }
        .await;
    });
    outcome
}
