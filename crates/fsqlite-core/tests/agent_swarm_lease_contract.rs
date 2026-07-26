use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn install_lease_schema(conn: &Connection) -> TestResult {
    conn.execute(
        "CREATE TABLE fsqlite_lease_contract(
            lease_key TEXT NOT NULL PRIMARY KEY,
            owner_id TEXT,
            lease_token TEXT,
            generation INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL,
            acquired_at_ms INTEGER,
            renewed_at_ms INTEGER,
            expires_at_ms INTEGER,
            renew_interval_ms INTEGER NOT NULL DEFAULT 0,
            released_at_ms INTEGER,
            metadata_ref TEXT,
            last_reason_code TEXT
        );",
    )
    .await?;
    conn.execute(
        "CREATE INDEX idx_fsqlite_lease_contract_expiration
            ON fsqlite_lease_contract(state, expires_at_ms, lease_key);",
    )
    .await?;
    Ok(())
}

fn acquire_missing_sql(
    lease_key: &str,
    owner_id: &str,
    lease_token: &str,
    now_ms: i64,
    ttl_ms: i64,
) -> String {
    format!(
        "INSERT INTO fsqlite_lease_contract(
            lease_key,
            owner_id,
            lease_token,
            generation,
            state,
            acquired_at_ms,
            renewed_at_ms,
            expires_at_ms,
            renew_interval_ms,
            metadata_ref,
            last_reason_code
        ) VALUES (
            '{lease_key}',
            '{owner_id}',
            '{lease_token}',
            1,
            'active',
            {now_ms},
            {now_ms},
            {now_ms} + {ttl_ms},
            {ttl_ms},
            'lease-contract',
            'ok'
        )
        RETURNING lease_key, owner_id, lease_token, generation, state,
                  expires_at_ms, renew_interval_ms, last_reason_code;"
    )
}

fn renew_sql(
    lease_key: &str,
    owner_id: &str,
    lease_token: &str,
    generation: i64,
    now_ms: i64,
    ttl_ms: i64,
) -> String {
    format!(
        "UPDATE fsqlite_lease_contract
            SET renewed_at_ms = {now_ms},
                expires_at_ms = {now_ms} + {ttl_ms},
                renew_interval_ms = {ttl_ms},
                last_reason_code = 'ok'
          WHERE lease_key = '{lease_key}'
            AND owner_id = '{owner_id}'
            AND lease_token = '{lease_token}'
            AND generation = {generation}
            AND state = 'active'
            AND expires_at_ms > {now_ms}
          RETURNING lease_key, owner_id, lease_token, generation,
                    expires_at_ms, renew_interval_ms, last_reason_code;"
    )
}

#[derive(Clone, Copy)]
struct LeaseOwner<'a> {
    owner_id: &'a str,
    lease_token: &'a str,
}

fn transfer_sql(
    lease_key: &str,
    current: LeaseOwner<'_>,
    generation: i64,
    next: LeaseOwner<'_>,
    now_ms: i64,
    ttl_ms: i64,
) -> String {
    format!(
        "UPDATE fsqlite_lease_contract
            SET owner_id = '{next_owner_id}',
                lease_token = '{next_lease_token}',
                generation = generation + 1,
                acquired_at_ms = {now_ms},
                renewed_at_ms = {now_ms},
                expires_at_ms = {now_ms} + {ttl_ms},
                renew_interval_ms = {ttl_ms},
                released_at_ms = NULL,
                last_reason_code = 'ok'
          WHERE lease_key = '{lease_key}'
            AND owner_id = '{current_owner_id}'
            AND lease_token = '{current_lease_token}'
            AND generation = {generation}
            AND state = 'active'
            AND expires_at_ms > {now_ms}
          RETURNING lease_key, owner_id, lease_token, generation,
                    expires_at_ms, renew_interval_ms, last_reason_code;",
        current_owner_id = current.owner_id,
        current_lease_token = current.lease_token,
        next_owner_id = next.owner_id,
        next_lease_token = next.lease_token
    )
}

fn release_sql(
    lease_key: &str,
    owner_id: &str,
    lease_token: &str,
    generation: i64,
    now_ms: i64,
) -> String {
    format!(
        "UPDATE fsqlite_lease_contract
            SET state = 'released',
                expires_at_ms = {now_ms},
                released_at_ms = {now_ms},
                last_reason_code = 'ok'
          WHERE lease_key = '{lease_key}'
            AND owner_id = '{owner_id}'
            AND lease_token = '{lease_token}'
            AND generation = {generation}
            AND state = 'active'
            AND expires_at_ms > {now_ms}
          RETURNING lease_key, owner_id, generation, state, released_at_ms,
                    last_reason_code;"
    )
}

fn expire_sql(lease_key: &str, now_ms: i64) -> String {
    format!(
        "UPDATE fsqlite_lease_contract
            SET state = 'expired',
                last_reason_code = 'lease_expired'
          WHERE lease_key = '{lease_key}'
            AND state = 'active'
            AND expires_at_ms <= {now_ms}
          RETURNING lease_key, owner_id, generation, state, expires_at_ms,
                    last_reason_code;"
    )
}

fn takeover_sql(
    lease_key: &str,
    owner_id: &str,
    lease_token: &str,
    now_ms: i64,
    ttl_ms: i64,
) -> String {
    format!(
        "UPDATE fsqlite_lease_contract
            SET owner_id = '{owner_id}',
                lease_token = '{lease_token}',
                generation = generation + 1,
                state = 'active',
                acquired_at_ms = {now_ms},
                renewed_at_ms = {now_ms},
                expires_at_ms = {now_ms} + {ttl_ms},
                renew_interval_ms = {ttl_ms},
                released_at_ms = NULL,
                last_reason_code = 'ok'
          WHERE lease_key = '{lease_key}'
            AND (state IN ('released', 'expired') OR expires_at_ms <= {now_ms})
          RETURNING lease_key, owner_id, lease_token, generation, state,
                    expires_at_ms, renew_interval_ms, last_reason_code;"
    )
}

fn row_values(row: &Row) -> &[SqliteValue] {
    row.values()
}

fn trace_lease_event(
    lease_key: &str,
    owner_id: &str,
    lease_token: &str,
    renew_interval_ms: i64,
    expiration_reason: &str,
    conflict_reason: &str,
    elapsed_ms: i64,
) {
    tracing::info!(
        target: "fsqlite.lease_contract",
        lease_key,
        owner_id,
        lease_token,
        renew_interval_ms,
        expiration_reason,
        conflict_reason,
        elapsed_ms,
        "lease contract mutation"
    );
}

#[test]
fn lease_acquire_renew_transfer_release_paths_are_deterministic() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(
                conn.is_concurrent_mode_default(),
                "lease coordination contract must not disable concurrent-writer mode"
            );
            install_lease_schema(&conn).await?;

            let acquire_rows = conn
                .query(&acquire_missing_sql(
                    "shard-a", "worker-a", "token-a", 100, 1000,
                ))
                .await?;
            trace_lease_event("shard-a", "worker-a", "token-a", 1000, "none", "ok", 0);
            assert_eq!(acquire_rows.len(), 1);
            assert_eq!(
                row_values(&acquire_rows[0]),
                &[
                    SqliteValue::Text("shard-a".into()),
                    SqliteValue::Text("worker-a".into()),
                    SqliteValue::Text("token-a".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("active".into()),
                    SqliteValue::Integer(1100),
                    SqliteValue::Integer(1000),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let still_active_takeover = conn
                .query(&takeover_sql("shard-a", "worker-b", "token-b", 200, 1000))
                .await?;
            trace_lease_event(
                "shard-a",
                "worker-b",
                "token-b",
                1000,
                "none",
                "lease_already_active",
                0,
            );
            assert!(
                still_active_takeover.is_empty(),
                "active non-expired lease must not transfer through takeover"
            );

            let wrong_token_renew = conn
                .query(&renew_sql(
                    "shard-a",
                    "worker-a",
                    "wrong-token",
                    1,
                    300,
                    1000,
                ))
                .await?;
            assert!(
                wrong_token_renew.is_empty(),
                "renew requires the current owner token and generation"
            );

            let renew_rows = conn
                .query(&renew_sql("shard-a", "worker-a", "token-a", 1, 500, 1200))
                .await?;
            trace_lease_event("shard-a", "worker-a", "token-a", 1200, "none", "ok", 0);
            assert_eq!(renew_rows.len(), 1);
            assert_eq!(
                row_values(&renew_rows[0]),
                &[
                    SqliteValue::Text("shard-a".into()),
                    SqliteValue::Text("worker-a".into()),
                    SqliteValue::Text("token-a".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(1700),
                    SqliteValue::Integer(1200),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let transfer_rows = conn
                .query(&transfer_sql(
                    "shard-a",
                    LeaseOwner {
                        owner_id: "worker-a",
                        lease_token: "token-a",
                    },
                    1,
                    LeaseOwner {
                        owner_id: "worker-b",
                        lease_token: "token-b",
                    },
                    600,
                    900,
                ))
                .await?;
            trace_lease_event("shard-a", "worker-b", "token-b", 900, "none", "ok", 0);
            assert_eq!(transfer_rows.len(), 1);
            assert_eq!(
                row_values(&transfer_rows[0]),
                &[
                    SqliteValue::Text("shard-a".into()),
                    SqliteValue::Text("worker-b".into()),
                    SqliteValue::Text("token-b".into()),
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(1500),
                    SqliteValue::Integer(900),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let stale_release = conn
                .query(&release_sql("shard-a", "worker-a", "token-a", 1, 650))
                .await?;
            assert!(
                stale_release.is_empty(),
                "release requires the current owner token and generation"
            );

            let release_rows = conn
                .query(&release_sql("shard-a", "worker-b", "token-b", 2, 700))
                .await?;
            trace_lease_event("shard-a", "worker-b", "token-b", 900, "none", "ok", 0);
            assert_eq!(release_rows.len(), 1);
            assert_eq!(
                row_values(&release_rows[0]),
                &[
                    SqliteValue::Text("shard-a".into()),
                    SqliteValue::Text("worker-b".into()),
                    SqliteValue::Integer(2),
                    SqliteValue::Text("released".into()),
                    SqliteValue::Integer(700),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let reacquire_rows = conn
                .query(&takeover_sql("shard-a", "worker-c", "token-c", 800, 500))
                .await?;
            trace_lease_event("shard-a", "worker-c", "token-c", 500, "released", "ok", 0);
            assert_eq!(reacquire_rows.len(), 1);
            assert_eq!(
                row_values(&reacquire_rows[0]),
                &[
                    SqliteValue::Text("shard-a".into()),
                    SqliteValue::Text("worker-c".into()),
                    SqliteValue::Text("token-c".into()),
                    SqliteValue::Integer(3),
                    SqliteValue::Text("active".into()),
                    SqliteValue::Integer(1300),
                    SqliteValue::Integer(500),
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
fn lease_expiration_clock_boundary_is_deterministic() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(
                conn.is_concurrent_mode_default(),
                "lease coordination contract must not disable concurrent-writer mode"
            );
            install_lease_schema(&conn).await?;

            let acquire_rows = conn
                .query(&acquire_missing_sql(
                    "clock-boundary",
                    "worker-a",
                    "token-a",
                    100,
                    50,
                ))
                .await?;
            assert_eq!(acquire_rows.len(), 1);

            let before_boundary = conn
                .query(&renew_sql(
                    "clock-boundary",
                    "worker-a",
                    "token-a",
                    1,
                    149,
                    100,
                ))
                .await?;
            trace_lease_event(
                "clock-boundary",
                "worker-a",
                "token-a",
                100,
                "before_boundary",
                "ok",
                0,
            );
            assert_eq!(
                row_values(&before_boundary[0]),
                &[
                    SqliteValue::Text("clock-boundary".into()),
                    SqliteValue::Text("worker-a".into()),
                    SqliteValue::Text("token-a".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(249),
                    SqliteValue::Integer(100),
                    SqliteValue::Text("ok".into()),
                ]
            );

            let exact_boundary_renew = conn
                .query(&renew_sql(
                    "clock-boundary",
                    "worker-a",
                    "token-a",
                    1,
                    249,
                    100,
                ))
                .await?;
            trace_lease_event(
                "clock-boundary",
                "worker-a",
                "token-a",
                100,
                "lease_expired",
                "lease_expired",
                0,
            );
            assert!(
                exact_boundary_renew.is_empty(),
                "renew at exactly expires_at_ms must fail deterministically"
            );

            let expire_rows = conn.query(&expire_sql("clock-boundary", 249)).await?;
            assert_eq!(expire_rows.len(), 1);
            assert_eq!(
                row_values(&expire_rows[0]),
                &[
                    SqliteValue::Text("clock-boundary".into()),
                    SqliteValue::Text("worker-a".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("expired".into()),
                    SqliteValue::Integer(249),
                    SqliteValue::Text("lease_expired".into()),
                ]
            );

            let takeover_rows = conn
                .query(&takeover_sql(
                    "clock-boundary",
                    "worker-b",
                    "token-b",
                    250,
                    75,
                ))
                .await?;
            trace_lease_event(
                "clock-boundary",
                "worker-b",
                "token-b",
                75,
                "lease_expired",
                "ok",
                0,
            );
            assert_eq!(takeover_rows.len(), 1);
            assert_eq!(
                row_values(&takeover_rows[0]),
                &[
                    SqliteValue::Text("clock-boundary".into()),
                    SqliteValue::Text("worker-b".into()),
                    SqliteValue::Text("token-b".into()),
                    SqliteValue::Integer(2),
                    SqliteValue::Text("active".into()),
                    SqliteValue::Integer(325),
                    SqliteValue::Integer(75),
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
fn lease_rollback_restores_prior_owner() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("lease_rollback.db");
            let db = db_path.to_string_lossy().to_string();

            let conn = Connection::open(&db).await?;
            conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await?;
            install_lease_schema(&conn).await?;
            let acquire_rows = conn
                .query(&acquire_missing_sql(
                    "rollback-shard",
                    "worker-a",
                    "token-a",
                    100,
                    1000,
                ))
                .await?;
            assert_eq!(acquire_rows.len(), 1);

            conn.execute("BEGIN CONCURRENT;").await?;
            let transfer_rows = conn
                .query(&transfer_sql(
                    "rollback-shard",
                    LeaseOwner {
                        owner_id: "worker-a",
                        lease_token: "token-a",
                    },
                    1,
                    LeaseOwner {
                        owner_id: "worker-b",
                        lease_token: "token-b",
                    },
                    200,
                    1000,
                ))
                .await?;
            trace_lease_event(
                "rollback-shard",
                "worker-b",
                "token-b",
                1000,
                "none",
                "ok",
                0,
            );
            assert_eq!(transfer_rows.len(), 1);
            conn.execute("ROLLBACK;").await?;

            let rows = conn
                .query(
                    "SELECT owner_id, lease_token, generation, state, expires_at_ms
           FROM fsqlite_lease_contract
          WHERE lease_key = 'rollback-shard';",
                )
                .await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(
                row_values(&rows[0]),
                &[
                    SqliteValue::Text("worker-a".into()),
                    SqliteValue::Text("token-a".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("active".into()),
                    SqliteValue::Integer(1100),
                ],
                "rollback must restore the previous lease owner and generation"
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn lease_takeover_race_publishes_one_owner() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("lease_takeover_race.db");
    let db = db_path.to_string_lossy().to_string();

    {
        let setup = Connection::open(&db).await?;
        setup.execute("PRAGMA fsqlite.concurrent_mode=ON;").await?;
        install_lease_schema(&setup).await?;
        let acquire_rows = setup.query(&acquire_missing_sql(
            "contended-shard",
            "worker-old",
            "token-old",
            100,
            10,
        )).await?;
        assert_eq!(acquire_rows.len(), 1);
    }

    let winner = Connection::open(&db).await?;
    let loser = Connection::open(&db).await?;
    for conn in [&winner, &loser] {
        conn.execute("PRAGMA busy_timeout=250;").await?;
        conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await?;
        assert!(
            conn.is_concurrent_mode_default(),
            "same-process lease race must run with concurrent mode enabled"
        );
    }

    winner.execute("BEGIN CONCURRENT;").await?;
    loser.execute("BEGIN CONCURRENT;").await?;

    let winner_rows = winner.query(&takeover_sql(
        "contended-shard",
        "worker-winner",
        "token-winner",
        200,
        1000,
    )).await?;
    trace_lease_event(
        "contended-shard",
        "worker-winner",
        "token-winner",
        1000,
        "lease_expired",
        "ok",
        0,
    );
    assert_eq!(winner_rows.len(), 1);
    winner.execute("COMMIT;").await?;

    let loser_takeover = loser.query(&takeover_sql(
        "contended-shard",
        "worker-loser",
        "token-loser",
        201,
        1000,
    )).await;
    match loser_takeover {
        Ok(rows) if rows.is_empty() => {
            trace_lease_event(
                "contended-shard",
                "worker-loser",
                "token-loser",
                1000,
                "lease_expired",
                "lease_generation_conflict",
                0,
            );
            loser.execute("ROLLBACK;").await?;
        }
        Ok(rows) => {
            trace_lease_event(
                "contended-shard",
                "worker-loser",
                "token-loser",
                1000,
                "lease_expired",
                "commit_retryable",
                0,
            );
            assert_eq!(
                rows.len(),
                1,
                "stale losing takeover should return at most its attempted mutation row"
            );
            let loser_commit = loser.execute("COMMIT;").await;
            assert!(
                loser_commit
                    .as_ref()
                    .is_err_and(fsqlite_error::FrankenError::is_transient),
                "stale losing lease takeover must fail transiently at commit: {loser_commit:?}"
            );
        }
        Err(err) => {
            trace_lease_event(
                "contended-shard",
                "worker-loser",
                "token-loser",
                1000,
                "lease_expired",
                "retryable_error",
                0,
            );
            assert!(
                err.is_transient(),
                "stale losing lease takeover must fail with a retryable error: {err}"
            );
            drop(loser.execute("ROLLBACK;").await);
        }
    }

    let rows = winner.query(
        "SELECT lease_key, owner_id, lease_token, generation, state
           FROM fsqlite_lease_contract
          WHERE lease_key = 'contended-shard';",
    ).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        row_values(&rows[0]),
        &[
            SqliteValue::Text("contended-shard".into()),
            SqliteValue::Text("worker-winner".into()),
            SqliteValue::Text("token-winner".into()),
            SqliteValue::Integer(2),
            SqliteValue::Text("active".into()),
        ],
        "only one owner may publish for the contended lease key"
    );

    Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}
