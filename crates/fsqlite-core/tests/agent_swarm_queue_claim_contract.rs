use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn install_queue_schema(conn: &Connection) -> TestResult {
    conn.execute(
        "CREATE TABLE fsqlite_queue_contract(
            queue_name TEXT NOT NULL,
            item_key TEXT NOT NULL,
            status TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            available_at_ms INTEGER NOT NULL DEFAULT 0,
            owner_id TEXT,
            claim_attempt_id TEXT,
            claim_seq INTEGER NOT NULL DEFAULT 0,
            claimed_at_ms INTEGER,
            expires_at_ms INTEGER,
            abandoned_at_ms INTEGER,
            updated_at_ms INTEGER NOT NULL DEFAULT 0,
            last_reason_code TEXT,
            PRIMARY KEY(queue_name, item_key)
        );",
    )
    .await?;
    conn.execute(
        "CREATE INDEX idx_fsqlite_queue_contract_claim
            ON fsqlite_queue_contract(
                queue_name,
                status,
                priority DESC,
                available_at_ms,
                item_key
            );",
    )
    .await?;
    Ok(())
}

async fn enqueue(conn: &Connection, item_key: &str, priority: i64) -> TestResult {
    conn.execute(&format!(
        "INSERT INTO fsqlite_queue_contract(
            queue_name,
            item_key,
            status,
            priority,
            available_at_ms,
            updated_at_ms,
            last_reason_code
        ) VALUES (
            'coordination',
            '{item_key}',
            'ready',
            {priority},
            0,
            0,
            'ok'
        );"
    ))
    .await?;
    Ok(())
}

fn claim_sql(worker_id: &str, claim_attempt_id: &str, now_ms: i64) -> String {
    format!(
        "UPDATE fsqlite_queue_contract
            SET status = 'claimed',
                owner_id = '{worker_id}',
                claim_attempt_id = '{claim_attempt_id}',
                claim_seq = claim_seq + 1,
                claimed_at_ms = {now_ms},
                expires_at_ms = {now_ms} + 1000,
                updated_at_ms = {now_ms},
                last_reason_code = 'ok'
          WHERE queue_name = 'coordination'
            AND item_key = (
                SELECT item_key
                  FROM fsqlite_queue_contract
                 WHERE queue_name = 'coordination'
                   AND status = 'ready'
                   AND available_at_ms <= {now_ms}
                 ORDER BY priority DESC, available_at_ms ASC, item_key ASC
                 LIMIT 1
            )
          RETURNING item_key, owner_id, claim_attempt_id, claim_seq, last_reason_code;"
    )
}

fn claim_retry_sql(worker_id: &str, item_key: &str, claim_attempt_id: &str) -> String {
    format!(
        "SELECT item_key, owner_id, claim_attempt_id, claim_seq, last_reason_code
           FROM fsqlite_queue_contract
          WHERE queue_name = 'coordination'
            AND item_key = '{item_key}'
            AND status = 'claimed'
            AND owner_id = '{worker_id}'
            AND claim_attempt_id = '{claim_attempt_id}';"
    )
}

fn release_sql(worker_id: &str, item_key: &str, claim_seq: i64, now_ms: i64) -> String {
    format!(
        "UPDATE fsqlite_queue_contract
            SET status = 'ready',
                owner_id = NULL,
                claim_attempt_id = NULL,
                claimed_at_ms = NULL,
                expires_at_ms = NULL,
                updated_at_ms = {now_ms},
                last_reason_code = 'ok'
          WHERE queue_name = 'coordination'
            AND item_key = '{item_key}'
            AND status = 'claimed'
            AND owner_id = '{worker_id}'
            AND claim_seq = {claim_seq}
          RETURNING item_key, status, claim_seq, last_reason_code;"
    )
}

fn abandon_sql(item_key: &str, now_ms: i64) -> String {
    format!(
        "UPDATE fsqlite_queue_contract
            SET status = 'abandoned',
                owner_id = NULL,
                claim_attempt_id = NULL,
                claimed_at_ms = NULL,
                expires_at_ms = NULL,
                abandoned_at_ms = {now_ms},
                updated_at_ms = {now_ms},
                last_reason_code = 'ok'
          WHERE queue_name = 'coordination'
            AND item_key = '{item_key}'
            AND status IN ('ready', 'claimed')
          RETURNING item_key, status, abandoned_at_ms, last_reason_code;"
    )
}

fn row_values(row: &Row) -> &[SqliteValue] {
    row.values()
}

fn trace_claim_attempt(
    worker_id: &str,
    claim_attempt_id: &str,
    statement_fingerprint: &str,
    conflict_reason: &str,
    elapsed_ms: i64,
) {
    tracing::info!(
        target: "fsqlite.queue_contract",
        queue_name = "coordination",
        worker_id,
        claim_attempt_id,
        statement_fingerprint,
        conflict_reason,
        elapsed_ms,
        "queue claim contract attempt"
    );
}

#[test]
fn queue_claim_empty_and_release_paths_are_deterministic() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(
                conn.is_concurrent_mode_default(),
                "queue coordination contract must not disable concurrent-writer mode"
            );
            install_queue_schema(&conn).await?;

            let empty_claim = conn
                .query(&claim_sql("worker-a", "attempt-empty", 10))
                .await?;
            trace_claim_attempt(
                "worker-a",
                "attempt-empty",
                "queue-claim-contract",
                "empty_queue",
                0,
            );
            assert!(
                empty_claim.is_empty(),
                "empty queue claim must return no mutation rows"
            );

            enqueue(&conn, "job-1", 10).await?;
            let claim_rows = conn.query(&claim_sql("worker-a", "attempt-1", 20)).await?;
            assert_eq!(claim_rows.len(), 1);
            assert_eq!(
                row_values(&claim_rows[0]),
                &[
                    SqliteValue::Text("job-1".into()),
                    SqliteValue::Text("worker-a".into()),
                    SqliteValue::Text("attempt-1".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let retry_rows = conn
                .query(&claim_retry_sql("worker-a", "job-1", "attempt-1"))
                .await?;
            trace_claim_attempt("worker-a", "attempt-1", "queue-claim-contract", "ok", 0);
            assert_eq!(retry_rows.len(), 1);
            assert_eq!(
                row_values(&retry_rows[0]),
                row_values(&claim_rows[0]),
                "retrying the same claim attempt must return the existing ownership row"
            );

            let already_claimed = conn.query(&claim_sql("worker-b", "attempt-2", 30)).await?;
            trace_claim_attempt(
                "worker-b",
                "attempt-2",
                "queue-claim-contract",
                "already_claimed",
                0,
            );
            assert!(
                already_claimed.is_empty(),
                "already-claimed row must not be claimed by another worker"
            );

            let wrong_owner_release = conn.query(&release_sql("worker-b", "job-1", 1, 40)).await?;
            assert!(
                wrong_owner_release.is_empty(),
                "wrong owner release must return no mutation rows"
            );

            let release_rows = conn.query(&release_sql("worker-a", "job-1", 1, 50)).await?;
            assert_eq!(release_rows.len(), 1);
            assert_eq!(
                row_values(&release_rows[0]),
                &[
                    SqliteValue::Text("job-1".into()),
                    SqliteValue::Text("ready".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("ok".into()),
                ]
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn queue_claim_abandon_path_is_deterministic() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(
                conn.is_concurrent_mode_default(),
                "queue coordination contract must not disable concurrent-writer mode"
            );
            install_queue_schema(&conn).await?;
            enqueue(&conn, "job-abandon", 5).await?;

            let claim_rows = conn
                .query(&claim_sql("worker-a", "attempt-abandon", 100))
                .await?;
            trace_claim_attempt(
                "worker-a",
                "attempt-abandon",
                "queue-claim-contract",
                "ok",
                0,
            );
            assert_eq!(claim_rows.len(), 1);

            let abandon_rows = conn.query(&abandon_sql("job-abandon", 150)).await?;
            assert_eq!(abandon_rows.len(), 1);
            assert_eq!(
                row_values(&abandon_rows[0]),
                &[
                    SqliteValue::Text("job-abandon".into()),
                    SqliteValue::Text("abandoned".into()),
                    SqliteValue::Integer(150),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let retry_after_abandon = conn
                .query(&claim_retry_sql(
                    "worker-a",
                    "job-abandon",
                    "attempt-abandon",
                ))
                .await?;
            assert!(
                retry_after_abandon.is_empty(),
                "abandoning a claim must clear the idempotent retry row"
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn queue_claim_rollback_restores_ready_row() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("queue_claim_rollback.db");
            let db = db_path.to_string_lossy().to_string();

            let conn = Connection::open(&db).await?;
            conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await?;
            install_queue_schema(&conn).await?;
            enqueue(&conn, "job-rollback", 1).await?;

            conn.execute("BEGIN CONCURRENT;").await?;
            let claim_rows = conn
                .query(&claim_sql("worker-a", "attempt-rollback", 100))
                .await?;
            trace_claim_attempt(
                "worker-a",
                "attempt-rollback",
                "queue-claim-contract",
                "ok",
                0,
            );
            assert_eq!(claim_rows.len(), 1);
            conn.execute("ROLLBACK;").await?;

            let rows = conn
                .query(
                    "SELECT status, owner_id, claim_attempt_id, claim_seq
           FROM fsqlite_queue_contract
          WHERE queue_name = 'coordination'
            AND item_key = 'job-rollback';",
                )
                .await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0]),
                &[
                    SqliteValue::Text("ready".into()),
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Integer(0),
                ],
                "claim rollback must restore the row to claimable ready state"
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn queue_claim_race_publishes_one_owner() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("queue_claim_race.db");
            let db = db_path.to_string_lossy().to_string();

            {
                let setup = Connection::open(&db).await?;
                setup.execute("PRAGMA fsqlite.concurrent_mode=ON;").await?;
                install_queue_schema(&setup).await?;
                enqueue(&setup, "job-race", 100).await?;
            }

            let winner = Connection::open(&db).await?;
            let loser = Connection::open(&db).await?;
            for conn in [&winner, &loser] {
                conn.execute("PRAGMA busy_timeout=250;").await?;
                conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await?;
                assert!(
                    conn.is_concurrent_mode_default(),
                    "same-process queue race must run with concurrent mode enabled"
                );
            }

            winner.execute("BEGIN CONCURRENT;").await?;
            loser.execute("BEGIN CONCURRENT;").await?;

            let winner_rows = winner
                .query(&claim_sql("worker-winner", "attempt-winner", 200))
                .await?;
            trace_claim_attempt(
                "worker-winner",
                "attempt-winner",
                "queue-claim-contract",
                "ok",
                0,
            );
            assert_eq!(winner_rows.len(), 1);
            winner.execute("COMMIT;").await?;

            let loser_claim = loser
                .query(&claim_sql("worker-loser", "attempt-loser", 201))
                .await;
            match loser_claim {
                Ok(rows) if rows.is_empty() => {
                    trace_claim_attempt(
                        "worker-loser",
                        "attempt-loser",
                        "queue-claim-contract",
                        "queue_claim_conflict",
                        0,
                    );
                    loser.execute("ROLLBACK;").await?;
                }
                Ok(rows) => {
                    trace_claim_attempt(
                        "worker-loser",
                        "attempt-loser",
                        "queue-claim-contract",
                        "commit_retryable",
                        0,
                    );
                    assert_eq!(
                        rows.len(),
                        1,
                        "stale losing claim should return at most its attempted mutation row"
                    );
                    let loser_commit = loser.execute("COMMIT;").await;
                    assert!(
                        loser_commit
                            .as_ref()
                            .is_err_and(fsqlite_error::FrankenError::is_transient),
                        "stale losing queue claim must fail transiently at commit: {loser_commit:?}"
                    );
                }
                Err(err) => {
                    trace_claim_attempt(
                        "worker-loser",
                        "attempt-loser",
                        "queue-claim-contract",
                        "retryable_error",
                        0,
                    );
                    assert!(
                        err.is_transient(),
                        "stale losing queue claim must fail with a retryable error: {err}"
                    );
                    drop(loser.execute("ROLLBACK;").await);
                }
            }

            let rows = winner
                .query(
                    "SELECT item_key, status, owner_id, claim_attempt_id, claim_seq
           FROM fsqlite_queue_contract
          WHERE queue_name = 'coordination'
          ORDER BY item_key;",
                )
                .await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0]),
                &[
                    SqliteValue::Text("job-race".into()),
                    SqliteValue::Text("claimed".into()),
                    SqliteValue::Text("worker-winner".into()),
                    SqliteValue::Text("attempt-winner".into()),
                    SqliteValue::Integer(1),
                ],
                "only one owner may publish for the contended queue item"
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}
