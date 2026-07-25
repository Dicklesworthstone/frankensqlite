use fsqlite_core::connection::{Connection, Row};
use fsqlite_error::FrankenError;
use fsqlite_types::SqliteValue;
use std::error::Error;
use std::sync::mpsc;
use std::thread;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

async fn open_wal_db(path: &str) -> TestResult<Connection> {
    let conn = Connection::open(path).await?;
    conn.execute("PRAGMA busy_timeout=5000;").await?;
    conn.execute("PRAGMA journal_mode=WAL;").await?;
    conn.execute("PRAGMA synchronous=NORMAL;").await?;
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await?;
    Ok(conn)
}

fn text_at(row: &Row, column: usize) -> TestResult<String> {
    match row.get(column) {
        Some(SqliteValue::Text(value)) => Ok(value.to_string()),
        Some(other) => Err(format!("expected text at column {column}, got {other:?}").into()),
        None => Err(format!("missing column {column}").into()),
    }
}

#[test]
fn same_connection_read_your_own_write_survives_peer_commit_races() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir
                .path()
                .join("self-connection-read-your-own-write-peer-races.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn_a = open_wal_db(&db_path).await?;
            conn_a
                .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
                .await?;
            conn_a
                .execute("CREATE TABLE peer(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
                .await?;
            conn_a.begin_transaction().await?;
            for id in 1_i64..=128 {
                conn_a
                    .execute_with_params(
                        "INSERT INTO t(id, payload) VALUES (?1, ?2)",
                        &[
                            SqliteValue::Integer(id),
                            SqliteValue::Text(format!("seed:{id}").into()),
                        ],
                    )
                    .await?;
            }
            for id in 1_i64..=32 {
                conn_a
                    .execute_with_params(
                        "INSERT INTO peer(id, payload) VALUES (?1, ?2)",
                        &[
                            SqliteValue::Integer(id),
                            SqliteValue::Text(format!("peer-seed:{id}").into()),
                        ],
                    )
                    .await?;
            }
            conn_a.commit_transaction().await?;

            let warm_rows = conn_a
                .query_with_params(
                    "SELECT payload FROM t WHERE id = ?1",
                    &[SqliteValue::Integer(1)],
                )
                .await?;
            assert_eq!(warm_rows.len(), 1, "warmup query must find the seed row");
            assert_eq!(text_at(&warm_rows[0], 0)?, "seed:1");

            let (peer_request_tx, peer_request_rx) = mpsc::channel::<i64>();
            let (peer_done_tx, peer_done_rx) = mpsc::channel::<Result<(), String>>();
            let db_path_b = db_path.clone();
            let worker = thread::spawn(move || -> Result<(), String> {
                let mut worker_outcome: Result<(), String> = Ok(());
                asupersync::test_utils::run_test(|| async {
                    worker_outcome = async {
                        let conn_b = open_wal_db(&db_path_b)
                            .await
                            .map_err(|err| err.to_string())?;
                        while let Ok(iteration) = peer_request_rx.recv() {
                            let hot_id = (iteration % 32) + 1;
                            loop {
                                let cycle: Result<(), FrankenError> = async {
                                    conn_b.begin_transaction().await?;
                                    conn_b
                                        .execute_with_params(
                                            "UPDATE peer SET payload = ?1 WHERE id = ?2",
                                            &[
                                                SqliteValue::Text(
                                                    format!("peer:{iteration}").into(),
                                                ),
                                                SqliteValue::Integer(hot_id),
                                            ],
                                        )
                                        .await?;
                                    conn_b.commit_transaction().await
                                }
                                .await;

                                match cycle {
                                    Ok(()) => {
                                        peer_done_tx
                                            .send(Ok(()))
                                            .map_err(|err| err.to_string())?;
                                        break;
                                    }
                                    Err(
                                        FrankenError::Busy | FrankenError::BusySnapshot { .. },
                                    ) => {
                                        drop(conn_b.rollback_transaction().await);
                                    }
                                    Err(err) => {
                                        peer_done_tx
                                            .send(Err(err.to_string()))
                                            .map_err(|send_err| send_err.to_string())?;
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        Ok::<(), String>(())
                    }
                    .await;
                });
                worker_outcome
            });

            for iteration in 0_i64..2_000 {
                let inserted_id = 10_000 + iteration;
                let inserted_payload = format!("self:{iteration}");

                conn_a.begin_transaction().await?;
                let inserted = conn_a
                    .execute_with_params(
                        "INSERT INTO t(id, payload) VALUES (?1, ?2)",
                        &[
                            SqliteValue::Integer(inserted_id),
                            SqliteValue::Text(inserted_payload.clone().into()),
                        ],
                    )
                    .await?;
                assert_eq!(
                    inserted, 1,
                    "iteration={iteration}: insert affected {inserted} rows for id={inserted_id}"
                );
                conn_a.commit_transaction().await?;
                peer_request_tx
                    .send(iteration)
                    .map_err(|err| err.to_string())?;
                let peer_outcome = peer_done_rx.recv().map_err(|err| err.to_string())?;
                if let Err(err) = peer_outcome {
                    return Err(err.into());
                }

                let rows = conn_a
                    .query_with_params(
                        "SELECT payload FROM t WHERE id = ?1",
                        &[SqliteValue::Integer(inserted_id)],
                    )
                    .await?;
                assert_eq!(
                    rows.len(),
                    1,
                    "iteration={iteration}: expected one row for freshly committed id={inserted_id}, observed {rows:?}"
                );
                assert_eq!(
                    text_at(&rows[0], 0)?,
                    inserted_payload,
                    "iteration={iteration}: wrong payload for id={inserted_id}"
                );
            }

            drop(peer_request_tx);
            let worker_result = worker.join().expect("peer worker thread must join cleanly");
            if let Err(err) = worker_result {
                return Err(Box::<dyn Error>::from(err));
            }
            Ok::<(), Box<dyn Error>>(())
        }
        .await;
    });
    outcome
}
