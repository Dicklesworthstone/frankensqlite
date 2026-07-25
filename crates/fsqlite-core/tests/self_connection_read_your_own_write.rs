// Integration tests are their own crate root and do not inherit the lib's
// `#![recursion_limit]`. The async engine futures nest deeply enough
// (statement dispatch -> DML -> trigger -> nested statement execution) that
// the trait solver overflows the default 128 while monomorphizing them.
// Matches the 512 used by fsqlite-core, fsqlite, fsqlite-e2e and fsqlite-harness.
#![recursion_limit = "512"]

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::SqliteValue;
use std::error::Error;

const DEFAULT_HOT_ROWS: i64 = 32;
const HOT_ROW_BASE: i64 = -1_000_000;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

async fn open_wal_db(path: &str) -> TestResult<Connection> {
    let conn = Connection::open(path).await?;
    conn.execute("PRAGMA busy_timeout=5000;").await?;
    conn.execute("PRAGMA journal_mode=WAL;").await?;
    conn.execute("PRAGMA synchronous=NORMAL;").await?;
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await?;
    Ok(conn)
}

async fn initialize_swarm_schema(conn: &Connection) -> TestResult {
    conn.execute_batch(
        "CREATE TABLE swarm_rows (
            id INTEGER PRIMARY KEY,
            owner INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            payload TEXT NOT NULL,
            touched_by INTEGER NOT NULL,
            generation INTEGER NOT NULL,
            deleted INTEGER NOT NULL
        );
        CREATE TABLE worker_progress (
            worker_id INTEGER PRIMARY KEY,
            last_id INTEGER NOT NULL,
            last_seq INTEGER NOT NULL,
            payload TEXT NOT NULL,
            observed_epoch INTEGER NOT NULL
        );",
    )
    .await?;

    conn.execute_with_params(
        "INSERT INTO worker_progress \
         (worker_id, last_id, last_seq, payload, observed_epoch) \
         VALUES (?1, 0, 0, '', 0)",
        &[SqliteValue::Integer(0)],
    )
    .await?;

    for offset in 0..DEFAULT_HOT_ROWS {
        let id = HOT_ROW_BASE - offset;
        conn.execute_with_params(
            "INSERT INTO swarm_rows \
             (id, owner, seq, payload, touched_by, generation, deleted) \
             VALUES (?1, -1, ?2, ?3, -1, 0, 0)",
            &[
                SqliteValue::Integer(id),
                SqliteValue::Integer(offset),
                SqliteValue::Text(format!("hot-row-{offset}").into()),
            ],
        )
        .await?;
    }

    Ok(())
}

fn text_at(row: &Row, column: usize) -> TestResult<String> {
    match row.get(column) {
        Some(SqliteValue::Text(value)) => Ok(value.to_string()),
        Some(other) => Err(format!("expected text at column {column}, got {other:?}").into()),
        None => Err(format!("missing column {column}").into()),
    }
}

fn integer_at(row: &Row, column: usize) -> TestResult<i64> {
    match row.get(column) {
        Some(SqliteValue::Integer(value)) => Ok(*value),
        Some(other) => Err(format!("expected integer at column {column}, got {other:?}").into()),
        None => Err(format!("missing column {column}").into()),
    }
}

async fn commit_seed_rows(conn: &Connection, start: i64, end: i64, payload: &str) -> TestResult {
    conn.begin_transaction().await?;
    for id in start..=end {
        let inserted = conn
            .execute_with_params(
                "INSERT INTO t(id, v) VALUES (?1, ?2)",
                &[
                    SqliteValue::Integer(id),
                    SqliteValue::Text(payload.to_owned().into()),
                ],
            )
            .await?;
        assert_eq!(
            inserted, 1,
            "seed insert affected {inserted} rows for id={id}"
        );
    }
    conn.commit_transaction().await?;
    Ok(())
}

async fn commit_delete_range(conn: &Connection, start: i64, end: i64) -> TestResult {
    conn.begin_transaction().await?;
    for id in start..=end {
        let deleted = conn
            .execute_with_params("DELETE FROM t WHERE id = ?1", &[SqliteValue::Integer(id)])
            .await?;
        assert_eq!(
            deleted, 1,
            "seed delete affected {deleted} rows for id={id}"
        );
    }
    conn.commit_transaction().await?;
    Ok(())
}

#[test]
fn same_connection_mixed_txn_read_your_own_write_never_returns_zero_rows() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("self-connection-read-your-own-write.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn = open_wal_db(&db_path).await?;
            initialize_swarm_schema(&conn).await?;

            let mut live_ids = Vec::new();
            for seq in 1_i64..=2_000 {
                let id = seq;
                let payload = format!("insert:{seq}");
                let update_payload = format!("update:{seq}");
                let hot_id = HOT_ROW_BASE - ((seq - 1) % DEFAULT_HOT_ROWS);
                let update_id = if live_ids.is_empty() {
                    hot_id
                } else {
                    live_ids[(seq as usize + 7) % live_ids.len()]
                };
                let delete_id = if live_ids.len() >= 8 && seq % 5 == 0 {
                    Some(live_ids[(seq as usize / 5) % live_ids.len()])
                } else {
                    None
                };

                conn.begin_transaction().await?;

                let inserted = conn
                    .execute_with_params(
                        "INSERT INTO swarm_rows \
                         (id, owner, seq, payload, touched_by, generation, deleted) \
                         VALUES (?1, ?2, ?3, ?4, ?2, 0, 0)",
                        &[
                            SqliteValue::Integer(id),
                            SqliteValue::Integer(0),
                            SqliteValue::Integer(seq),
                            SqliteValue::Text(payload.clone().into()),
                        ],
                    )
                    .await?;
                assert_eq!(inserted, 1, "seq={seq}: insert affected {inserted} rows");

                let updated = conn
                    .execute_with_params(
                        "UPDATE swarm_rows \
                         SET payload = ?1, touched_by = ?2, generation = generation + 1 \
                         WHERE id = ?3",
                        &[
                            SqliteValue::Text(update_payload.into()),
                            SqliteValue::Integer(0),
                            SqliteValue::Integer(update_id),
                        ],
                    )
                    .await?;
                assert_eq!(updated, 1, "seq={seq}: update affected {updated} rows");

                if let Some(delete_id) = delete_id {
                    let deleted = conn
                        .execute_with_params(
                            "DELETE FROM swarm_rows WHERE id = ?1 AND owner = ?2",
                            &[SqliteValue::Integer(delete_id), SqliteValue::Integer(0)],
                        )
                        .await?;
                    assert_eq!(
                        deleted, 1,
                        "seq={seq}: delete affected {deleted} rows for id={delete_id}"
                    );
                }

                let progressed = conn
                    .execute_with_params(
                        "UPDATE worker_progress \
                         SET last_id = ?1, last_seq = ?2, payload = ?3, observed_epoch = ?2 \
                         WHERE worker_id = ?4",
                        &[
                            SqliteValue::Integer(id),
                            SqliteValue::Integer(seq),
                            SqliteValue::Text(payload.clone().into()),
                            SqliteValue::Integer(0),
                        ],
                    )
                    .await?;
                assert_eq!(
                    progressed, 1,
                    "seq={seq}: worker_progress update affected {progressed} rows"
                );

                conn.commit_transaction().await?;

                let rows = conn
                    .query_with_params(
                        "SELECT id, owner, seq, payload, deleted FROM swarm_rows WHERE id = ?1",
                        &[SqliteValue::Integer(id)],
                    )
                    .await?;
                assert_eq!(
                    rows.len(),
                    1,
                    "seq={seq}: expected one row for freshly committed id={id}, observed {rows:?}"
                );

                let row = &rows[0];
                assert_eq!(integer_at(row, 0)?, id, "seq={seq}: wrong id returned");
                assert_eq!(integer_at(row, 1)?, 0, "seq={seq}: wrong owner returned");
                assert_eq!(integer_at(row, 2)?, seq, "seq={seq}: wrong seq returned");
                assert_eq!(
                    text_at(row, 3)?,
                    payload,
                    "seq={seq}: wrong payload returned for id={id}"
                );
                assert_eq!(integer_at(row, 4)?, 0, "seq={seq}: row marked deleted");

                if let Some(delete_id) = delete_id {
                    live_ids.retain(|candidate| *candidate != delete_id);
                }
                live_ids.push(id);
            }

            Ok::<(), Box<dyn Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn prepared_select_on_same_connection_survives_peer_commits_without_page_growth() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir
                .path()
                .join("self-connection-read-your-own-write-peer.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn_a = open_wal_db(&db_path).await?;
            conn_a
                .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
                .await?;

            let seed_payload = "seed-row-".repeat(16);
            commit_seed_rows(&conn_a, 1, 5_000, &seed_payload).await?;
            commit_delete_range(&conn_a, 2_001, 5_000).await?;

            let conn_b = open_wal_db(&db_path).await?;
            let select_a = conn_a.prepare("SELECT v FROM t WHERE id = ?1").await?;
            let insert_a = conn_a
                .prepare("INSERT INTO t(id, v) VALUES (?1, ?2)")
                .await?;

            let warm_rows = select_a
                .query_with_params(&[SqliteValue::Integer(1)])
                .await?;
            assert_eq!(warm_rows.len(), 1, "warmup SELECT must find seed row");
            assert_eq!(text_at(&warm_rows[0], 0)?, seed_payload);

            for iteration in 0_i64..2_000 {
                let inserted_id = 2_001 + iteration;
                let inserted_value = format!("worker-a:{iteration}");
                let peer_value = format!("worker-b:{iteration}");

                conn_a.begin_transaction().await?;
                let inserted = insert_a
                    .execute_with_params(&[
                        SqliteValue::Integer(inserted_id),
                        SqliteValue::Text(inserted_value.clone().into()),
                    ])
                    .await?;
                assert_eq!(
                    inserted, 1,
                    "iteration={iteration}: A insert affected {inserted} rows for id={inserted_id}"
                );
                conn_a.commit_transaction().await?;

                conn_b.begin_transaction().await?;
                let updated = conn_b
                    .execute_with_params(
                        "UPDATE t SET v = ?1 WHERE id = 1",
                        &[SqliteValue::Text(peer_value.into())],
                    )
                    .await?;
                assert_eq!(
                    updated, 1,
                    "iteration={iteration}: B update affected {updated} rows"
                );
                conn_b.commit_transaction().await?;

                let rows = select_a
                    .query_with_params(&[SqliteValue::Integer(inserted_id)])
                    .await?;
                assert_eq!(
                    rows.len(),
                    1,
                    "iteration={iteration}: prepared SELECT returned {rows:?} for freshly committed id={inserted_id}"
                );
                assert_eq!(
                    text_at(&rows[0], 0)?,
                    inserted_value,
                    "iteration={iteration}: wrong payload for id={inserted_id}"
                );
            }

            Ok::<(), Box<dyn Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn ad_hoc_query_on_same_connection_survives_peer_commits_without_page_growth() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir
                .path()
                .join("self-connection-read-your-own-write-ad-hoc-peer.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn_a = open_wal_db(&db_path).await?;
            conn_a
                .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
                .await?;

            let seed_payload = "seed-row-".repeat(16);
            commit_seed_rows(&conn_a, 1, 5_000, &seed_payload).await?;
            commit_delete_range(&conn_a, 2_001, 5_000).await?;

            let conn_b = open_wal_db(&db_path).await?;
            let warm_rows = conn_a
                .query_with_params("SELECT v FROM t WHERE id = ?1", &[SqliteValue::Integer(1)])
                .await?;
            assert_eq!(warm_rows.len(), 1, "warmup SELECT must find seed row");
            assert_eq!(text_at(&warm_rows[0], 0)?, seed_payload);

            for iteration in 0_i64..2_000 {
                let inserted_id = 2_001 + iteration;
                let inserted_value = format!("worker-a:{iteration}");
                let peer_value = format!("worker-b:{iteration}");

                conn_a.begin_transaction().await?;
                let inserted = conn_a
                    .execute_with_params(
                        "INSERT INTO t(id, v) VALUES (?1, ?2)",
                        &[
                            SqliteValue::Integer(inserted_id),
                            SqliteValue::Text(inserted_value.clone().into()),
                        ],
                    )
                    .await?;
                assert_eq!(
                    inserted, 1,
                    "iteration={iteration}: A insert affected {inserted} rows for id={inserted_id}"
                );
                conn_a.commit_transaction().await?;

                conn_b.begin_transaction().await?;
                let updated = conn_b
                    .execute_with_params(
                        "UPDATE t SET v = ?1 WHERE id = 1",
                        &[SqliteValue::Text(peer_value.into())],
                    )
                    .await?;
                assert_eq!(
                    updated, 1,
                    "iteration={iteration}: B update affected {updated} rows"
                );
                conn_b.commit_transaction().await?;

                let rows = conn_a
                    .query_with_params(
                        "SELECT v FROM t WHERE id = ?1",
                        &[SqliteValue::Integer(inserted_id)],
                    )
                    .await?;
                assert_eq!(
                    rows.len(),
                    1,
                    "iteration={iteration}: ad hoc SELECT returned {rows:?} for freshly committed id={inserted_id}"
                );
                assert_eq!(
                    text_at(&rows[0], 0)?,
                    inserted_value,
                    "iteration={iteration}: wrong payload for id={inserted_id}"
                );
            }

            Ok::<(), Box<dyn Error>>(())
        }
        .await;
    });
    outcome
}
