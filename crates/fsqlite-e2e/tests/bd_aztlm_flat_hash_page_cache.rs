//! Track Q flat-hash page-cache oracle and concurrent-writer evidence for `bd-aztlm`.
#![recursion_limit = "512"]

use std::path::Path;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use fsqlite_types::SqliteValue;
use serde_json::json;
use tempfile::tempdir;

const BEAD_ID: &str = "bd-aztlm";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_aztlm_flat_hash_page_cache -- --nocapture --test-threads=1";
const BUSY_TIMEOUT_MS: u64 = 5_000;
const RETRY_SLEEP_MS: u64 = 2;
const MAX_RETRIES_PER_TXN: usize = 256;
const ORACLE_ROWS: i64 = 10_000;
const WRITERS: usize = 4;
const ROUNDS_PER_WRITER: usize = 250;

static TRACK_Q_E2E_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Default, Clone, Copy)]
struct WriterStats {
    committed: usize,
    retries: u64,
}

fn emit_track_q_e2e_log(test_name: &str, phase: &str, payload: serde_json::Value) {
    eprintln!(
        "TRACK_Q_E2E:{}",
        json!({
            "bead_id": BEAD_ID,
            "test_name": test_name,
            "phase": phase,
            "replay_command": REPLAY_COMMAND,
            "payload": payload
        })
    );
}

async fn open_fsqlite(path: &Path) -> fsqlite::Connection {
    let path = path.to_str().expect("utf-8 fsqlite path");
    let conn = fsqlite::Connection::open(path)
        .await
        .expect("open fsqlite connection");
    conn.execute("PRAGMA journal_mode=WAL;")
        .await
        .expect("enable fsqlite wal");
    conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS};"))
        .await
        .expect("set fsqlite busy timeout");
    conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
        .await
        .expect("enable fsqlite concurrent mode");
    conn
}

fn open_sqlite(path: &Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).expect("open sqlite connection");
    conn.execute_batch(&format!(
        "PRAGMA journal_mode=WAL; PRAGMA busy_timeout={BUSY_TIMEOUT_MS};"
    ))
    .expect("configure sqlite connection");
    conn
}

fn payload_for_rowid(rowid: i64) -> String {
    let padding_len = usize::try_from((rowid.rem_euclid(31)) + 12).expect("padding length fits");
    format!("row_{rowid}_{}", "x".repeat(padding_len))
}

fn concurrent_row_id(writer_id: usize, round: usize) -> i64 {
    let writer = i64::try_from(writer_id).expect("writer id fits");
    let round = i64::try_from(round).expect("round fits");
    (writer * 100_000) + round + 1
}

fn concurrent_payload(writer_id: usize, round: usize) -> String {
    format!(
        "writer_{writer_id}_round_{round}_{}",
        "y".repeat((writer_id + round) % 19 + 8)
    )
}

async fn fetch_fsqlite_rows(conn: &fsqlite::Connection, table: &str) -> Vec<(i64, String, i64)> {
    let sql = format!("SELECT id, payload, writer FROM {table} ORDER BY id");
    conn.query(&sql)
        .await
        .expect("query fsqlite rows")
        .into_iter()
        .map(|row| {
            let id = match row.get(0) {
                Some(SqliteValue::Integer(value)) => *value,
                other => panic!("expected INTEGER id, got {other:?}"),
            };
            let payload = match row.get(1) {
                Some(SqliteValue::Text(value)) => value.to_string(),
                other => panic!("expected TEXT payload, got {other:?}"),
            };
            let writer = match row.get(2) {
                Some(SqliteValue::Integer(value)) => *value,
                other => panic!("expected INTEGER writer, got {other:?}"),
            };
            (id, payload, writer)
        })
        .collect()
}

fn fetch_sqlite_rows(conn: &rusqlite::Connection, table: &str) -> Vec<(i64, String, i64)> {
    let sql = format!("SELECT id, payload, writer FROM {table} ORDER BY id");
    let mut stmt = conn.prepare(&sql).expect("prepare sqlite select");
    stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })
    .expect("query sqlite rows")
    .map(|row| row.expect("sqlite row"))
    .collect()
}

async fn query_single_integer(conn: &fsqlite::Connection, sql: &str) -> i64 {
    let row = conn.query_row(sql).await.expect("query integer row");
    match row.get(0) {
        Some(SqliteValue::Integer(value)) => *value,
        Some(other) => panic!("expected integer result for `{sql}`, got {other:?}"),
        None => panic!("missing integer result for `{sql}`"),
    }
}

async fn query_single_text(conn: &fsqlite::Connection, sql: &str) -> String {
    let row = conn.query_row(sql).await.expect("query text row");
    match row.get(0) {
        Some(SqliteValue::Text(value)) => value.to_string(),
        Some(other) => panic!("expected text result for `{sql}`, got {other:?}"),
        None => panic!("missing text result for `{sql}`"),
    }
}

fn rows_per_sec(rows: usize, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return f64::from(u32::try_from(rows).expect("rows fit in u32"));
    }
    f64::from(u32::try_from(rows).expect("rows fit in u32")) / secs
}

#[test]
fn bd_aztlm_flat_hash_insert_10k_oracle_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let _guard = TRACK_Q_E2E_LOCK.lock().expect("track q e2e lock");

        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_q_oracle_fsqlite.db");
        let sqlite_db = temp.path().join("track_q_oracle_sqlite.db");

        let fconn = open_fsqlite(&fsqlite_db).await;
        let sconn = open_sqlite(&sqlite_db);
        fconn
            .execute(
                "CREATE TABLE page_cache_track_q (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, writer INTEGER NOT NULL);",
            )
            .await
            .expect("create fsqlite oracle table");
        sconn.execute_batch(
            "CREATE TABLE page_cache_track_q (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, writer INTEGER NOT NULL);",
        )
        .expect("create sqlite oracle table");

        let fsqlite_started = Instant::now();
        fconn.execute("BEGIN;").await.expect("fsqlite begin");
        for rowid in 1_i64..=ORACLE_ROWS {
            let payload = payload_for_rowid(rowid);
            let sql = format!(
                "INSERT INTO page_cache_track_q (id, payload, writer) VALUES ({rowid}, '{payload}', 0);"
            );
            let changes = fconn.execute(&sql).await.expect("fsqlite insert");
            assert_eq!(
                changes, 1,
                "fsqlite should insert exactly one row per statement"
            );
        }
        fconn.execute("COMMIT;").await.expect("fsqlite commit");
        let fsqlite_elapsed = fsqlite_started.elapsed();

        let sqlite_started = Instant::now();
        sconn.execute_batch("BEGIN;").expect("sqlite begin");
        for rowid in 1_i64..=ORACLE_ROWS {
            let payload = payload_for_rowid(rowid);
            sconn
                .execute(
                    "INSERT INTO page_cache_track_q (id, payload, writer) VALUES (?1, ?2, 0)",
                    rusqlite::params![rowid, payload],
                )
                .expect("sqlite insert");
        }
        sconn.execute_batch("COMMIT;").expect("sqlite commit");
        let sqlite_elapsed = sqlite_started.elapsed();

        let fsqlite_rows = fetch_fsqlite_rows(&fconn, "page_cache_track_q").await;
        let sqlite_rows = fetch_sqlite_rows(&sconn, "page_cache_track_q");
        assert_eq!(
            fsqlite_rows, sqlite_rows,
            "10K insert oracle rowset mismatch between fsqlite and sqlite"
        );

        let integrity = query_single_text(&fconn, "PRAGMA integrity_check;").await;
        assert_eq!(integrity, "ok", "fsqlite integrity_check should stay clean");

        emit_track_q_e2e_log(
            "bd_aztlm_flat_hash_insert_10k_oracle_matches_sqlite",
            "verify",
            json!({
                "rows": ORACLE_ROWS,
                "fsqlite_elapsed_ms": fsqlite_elapsed.as_millis(),
                "sqlite_elapsed_ms": sqlite_elapsed.as_millis(),
                "fsqlite_rows_per_sec": rows_per_sec(usize::try_from(ORACLE_ROWS).expect("row count fits"), fsqlite_elapsed),
                "sqlite_rows_per_sec": rows_per_sec(usize::try_from(ORACLE_ROWS).expect("row count fits"), sqlite_elapsed),
                "integrity_check": integrity
            }),
        );
    });
}

#[test]
fn bd_aztlm_flat_hash_four_concurrent_writers_no_data_loss() {
    asupersync::test_utils::run_test(|| async {
        let _guard = TRACK_Q_E2E_LOCK.lock().expect("track q e2e lock");

        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_q_concurrent_fsqlite.db");
        let sqlite_db = temp.path().join("track_q_concurrent_sqlite.db");

        {
            let conn = open_fsqlite(&fsqlite_db).await;
            conn.execute(
                "CREATE TABLE writer_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, writer INTEGER NOT NULL);",
            )
            .await
            .expect("create fsqlite concurrent table");
        }

        let barrier = Arc::new(Barrier::new(WRITERS));
        let mut handles = Vec::with_capacity(WRITERS);
        for writer_id in 0..WRITERS {
            let db = fsqlite_db.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || -> Result<WriterStats, String> {
                let mut outcome: Result<WriterStats, String> =
                    Err(format!("writer={writer_id}: writer body did not run"));
                asupersync::test_utils::run_test(|| async {
                    outcome = (async || -> Result<WriterStats, String> {
                        let conn = open_fsqlite(&db).await;
                        let mut stats = WriterStats::default();
                        barrier.wait();

                        for round in 0..ROUNDS_PER_WRITER {
                            let row_id = concurrent_row_id(writer_id, round);
                            let payload = concurrent_payload(writer_id, round);
                            let insert_sql = format!(
                                "INSERT INTO writer_rows (id, payload, writer) VALUES ({row_id}, '{payload}', {writer_id});"
                            );
                            let mut retries_this_row = 0_usize;
                            loop {
                                match conn.execute("BEGIN CONCURRENT;").await {
                                    Ok(_) => {}
                                    Err(err) if err.is_transient() => {
                                        retries_this_row += 1;
                                        stats.retries = stats.retries.saturating_add(1);
                                        if retries_this_row > MAX_RETRIES_PER_TXN {
                                            return Err(format!(
                                                "writer={writer_id} round={round}: exceeded retry budget at BEGIN"
                                            ));
                                        }
                                        thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                        continue;
                                    }
                                    Err(err) => {
                                        return Err(format!(
                                            "writer={writer_id} round={round}: non-transient BEGIN error: {err}"
                                        ));
                                    }
                                }

                                match conn.execute(&insert_sql).await {
                                    Ok(1) => {}
                                    Ok(changes) => {
                                        drop(conn.execute("ROLLBACK;").await);
                                        return Err(format!(
                                            "writer={writer_id} round={round}: expected 1 inserted row, got {changes}"
                                        ));
                                    }
                                    Err(err) if err.is_transient() => {
                                        retries_this_row += 1;
                                        stats.retries = stats.retries.saturating_add(1);
                                        drop(conn.execute("ROLLBACK;").await);
                                        if retries_this_row > MAX_RETRIES_PER_TXN {
                                            return Err(format!(
                                                "writer={writer_id} round={round}: exceeded retry budget at INSERT"
                                            ));
                                        }
                                        thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                        continue;
                                    }
                                    Err(err) => {
                                        drop(conn.execute("ROLLBACK;").await);
                                        return Err(format!(
                                            "writer={writer_id} round={round}: non-transient INSERT error: {err}"
                                        ));
                                    }
                                }

                                match conn.execute("COMMIT;").await {
                                    Ok(_) => {
                                        stats.committed += 1;
                                        break;
                                    }
                                    Err(err) if err.is_transient() => {
                                        retries_this_row += 1;
                                        stats.retries = stats.retries.saturating_add(1);
                                        drop(conn.execute("ROLLBACK;").await);
                                        if retries_this_row > MAX_RETRIES_PER_TXN {
                                            return Err(format!(
                                                "writer={writer_id} round={round}: exceeded retry budget at COMMIT"
                                            ));
                                        }
                                        thread::sleep(Duration::from_millis(RETRY_SLEEP_MS));
                                    }
                                    Err(err) => {
                                        drop(conn.execute("ROLLBACK;").await);
                                        return Err(format!(
                                            "writer={writer_id} round={round}: non-transient COMMIT error: {err}"
                                        ));
                                    }
                                }
                            }
                        }

                        Ok(stats)
                    })()
                    .await;
                });
                outcome
            }));
        }

        let started = Instant::now();
        let mut total_committed = 0_usize;
        let mut total_retries = 0_u64;
        for handle in handles {
            let stats = handle
                .join()
                .expect("track q writer thread must not panic")
                .unwrap_or_else(|message| panic!("{message}"));
            total_committed += stats.committed;
            total_retries = total_retries.saturating_add(stats.retries);
        }
        let elapsed = started.elapsed();

        let verifier = open_fsqlite(&fsqlite_db).await;
        let total_rows = query_single_integer(&verifier, "SELECT COUNT(*) FROM writer_rows;").await;
        assert_eq!(
            total_rows,
            i64::try_from(total_committed).expect("committed count fits"),
            "final writer row count should match committed transactions"
        );
        assert_eq!(
            total_rows,
            i64::try_from(WRITERS * ROUNDS_PER_WRITER).expect("expected row count fits"),
            "4-concurrent-writer workload should preserve every inserted row"
        );

        for writer_id in 0..WRITERS {
            let writer_rows = query_single_integer(
                &verifier,
                &format!("SELECT COUNT(*) FROM writer_rows WHERE writer = {writer_id};"),
            )
            .await;
            assert_eq!(
                writer_rows,
                i64::try_from(ROUNDS_PER_WRITER).expect("round count fits"),
                "writer {writer_id} should retain every committed row"
            );
        }

        let integrity = query_single_text(&verifier, "PRAGMA integrity_check;").await;
        assert_eq!(
            integrity, "ok",
            "concurrent writer integrity_check should stay clean"
        );

        let sqlite = open_sqlite(&sqlite_db);
        sqlite
            .execute_batch(
                "CREATE TABLE writer_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, writer INTEGER NOT NULL);",
            )
            .expect("create sqlite concurrent oracle table");
        sqlite.execute_batch("BEGIN;").expect("sqlite oracle begin");
        for writer_id in 0..WRITERS {
            for round in 0..ROUNDS_PER_WRITER {
                sqlite
                    .execute(
                        "INSERT INTO writer_rows (id, payload, writer) VALUES (?1, ?2, ?3)",
                        rusqlite::params![
                            concurrent_row_id(writer_id, round),
                            concurrent_payload(writer_id, round),
                            i64::try_from(writer_id).expect("writer id fits")
                        ],
                    )
                    .expect("sqlite oracle insert");
            }
        }
        sqlite
            .execute_batch("COMMIT;")
            .expect("sqlite oracle commit");

        let fsqlite_rows = fetch_fsqlite_rows(&verifier, "writer_rows").await;
        let sqlite_rows = fetch_sqlite_rows(&sqlite, "writer_rows");
        assert_eq!(
            fsqlite_rows, sqlite_rows,
            "concurrent writer rowset should match the sqlite oracle"
        );

        emit_track_q_e2e_log(
            "bd_aztlm_flat_hash_four_concurrent_writers_no_data_loss",
            "verify",
            json!({
                "writers": WRITERS,
                "rounds_per_writer": ROUNDS_PER_WRITER,
                "total_committed": total_committed,
                "total_retries": total_retries,
                "elapsed_ms": elapsed.as_millis(),
                "rows_per_sec": rows_per_sec(total_committed, elapsed),
                "integrity_check": integrity
            }),
        );
    });
}
