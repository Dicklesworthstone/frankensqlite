//! GH#525: scans must remain correct after exhausting snapshot-read retention.
//!
//! These SQL keepers complement the pager's exact retained-page bounds. The
//! fixture is larger than the eager-hydration threshold, and COUNT(id), unlike
//! COUNT(*), walks the table. The streaming callback consumes payloads without
//! collecting result rows, as CASS's lexical-index rebuild does.
//!
//! The two ignored entry points are also a baseline-compatible measurement
//! harness. Build this same file against both revisions. First create a fixture:
//!
//! GH525_DB=/tmp/gh525.db GH525_ROWS=200000 <test-binary> --ignored --exact \
//!     gh525_profile_fixture --nocapture
//!
//! Then run EACH engine/shape/budget combination in a fresh process:
//!
//! GH525_DB=/tmp/gh525.db GH525_ENGINE=fsqlite GH525_SHAPE=count \
//!     GH525_CACHE_SIZE=-2000 <test-binary> --ignored --exact \
//!     gh525_profile_child --nocapture
//!
//! Engines: fsqlite, sqlite. Shapes: count, stream, analytics, join, explicit.
//! GH525_PEER=1 opens an idle second connection to exercise shared-pager reads.
//! GH525_READ_ONLY=1 uses the physical READ_ONLY open path used by CASS, in
//! addition to query_only. By default both engines open an existing writable DB.
//! FSQLITE_PAGE_BUFFER_MAX remains available for the issue's pool-cap controls.
//! JSON records include a full-result digest, timing and Linux VmHWM. RSS is a
//! whole-process high-water mark, not an exact live-allocation measurement.

#![cfg(feature = "native")]
#![recursion_limit = "512"]

use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use fsqlite_core::connection::{Connection, Row};
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

const TEST_ROWS: i64 = 4096;
const BODY_BYTES: usize = 3000;
const COUNT: &str = "SELECT COUNT(id) FROM big";
const STREAM: &str = "SELECT id, sid, body FROM big ORDER BY id";
const ANALYTICS: &str = "SELECT COUNT(id), SUM(sid), MIN(id), MAX(id) FROM big";
const JOIN: &str = "SELECT COUNT(b.id), SUM(s.tid) FROM big b \
    JOIN small s ON s.id = b.sid LEFT JOIN tiny t ON t.id = s.tid \
    WHERE b.id > 0 AND t.id > 0";

fn fixture(path: &Path, rows: i64) {
    assert!(!path.exists(), "fixture creation must not overwrite a database");
    assert!(rows > 0);
    let mut db = rusqlite::Connection::open(path).expect("create SQLite fixture");
    db.execute_batch(
        "PRAGMA page_size=4096; PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; \
         CREATE TABLE tiny(id INTEGER PRIMARY KEY, name TEXT); \
         CREATE TABLE small(id INTEGER PRIMARY KEY, tid INTEGER); \
         CREATE TABLE big(id INTEGER PRIMARY KEY, sid INTEGER NOT NULL, body TEXT); \
         CREATE INDEX big_sid ON big(sid);",
    )
    .expect("fixture schema");
    let tx = db.transaction().expect("fixture transaction");
    for id in 1..=8 {
        tx.execute(
            "INSERT INTO tiny VALUES (?1, ?2)",
            rusqlite::params![id, format!("agent{id}")],
        )
        .unwrap();
    }
    for id in 1..=1000 {
        tx.execute(
            "INSERT INTO small VALUES (?1, ?2)",
            rusqlite::params![id, 1 + id % 8],
        )
        .unwrap();
    }
    {
        // Deterministic payload, with the same 3000-byte record width as the
        // issue's hex(randomblob(1500)); no compression is involved in either DB.
        let body = "x".repeat(BODY_BYTES);
        let mut insert = tx.prepare("INSERT INTO big VALUES (?1, ?2, ?3)").unwrap();
        for id in 1..=rows {
            insert
                .execute(rusqlite::params![id, 1 + id % 1000, &body])
                .unwrap();
        }
    }
    tx.commit().unwrap();
    db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    db.close().unwrap();
    if rows >= TEST_ROWS {
        assert!(
            std::fs::metadata(path).unwrap().len() > 8 * 1024 * 1024,
            "fixture must bypass eager file-backed row hydration"
        );
    }
}

#[derive(Default)]
struct Digest {
    rows: u64,
    hasher: blake3::Hasher,
}

impl Digest {
    fn value(&mut self, tag: u8, bytes: &[u8]) {
        self.hasher.update(&[tag]);
        self.hasher.update(&(bytes.len() as u64).to_le_bytes());
        self.hasher.update(bytes);
    }

    fn frank_row(&mut self, row: &Row) {
        self.rows += 1;
        self.hasher.update(&(row.values().len() as u64).to_le_bytes());
        for value in row.values() {
            match value {
                SqliteValue::Null => self.value(0, &[]),
                SqliteValue::Integer(value) => self.value(1, &value.to_le_bytes()),
                SqliteValue::Float(value) => self.value(2, &value.to_bits().to_le_bytes()),
                SqliteValue::Text(value) => self.value(3, value.as_bytes_direct()),
                SqliteValue::Blob(value) => self.value(4, value),
            }
        }
    }

    fn stock_row(&mut self, row: &rusqlite::Row<'_>) {
        use rusqlite::types::ValueRef;

        self.rows += 1;
        let columns = row.as_ref().column_count();
        self.hasher.update(&(columns as u64).to_le_bytes());
        for index in 0..columns {
            match row.get_ref(index).unwrap() {
                ValueRef::Null => self.value(0, &[]),
                ValueRef::Integer(value) => self.value(1, &value.to_le_bytes()),
                ValueRef::Real(value) => self.value(2, &value.to_bits().to_le_bytes()),
                ValueRef::Text(value) => self.value(3, value),
                ValueRef::Blob(value) => self.value(4, value),
            }
        }
    }

    fn result(&self) -> (u64, String) {
        (self.rows, self.hasher.finalize().to_hex().to_string())
    }
}

fn stock_digest(conn: &rusqlite::Connection, sql: &str) -> Digest {
    let mut digest = Digest::default();
    let mut statement = conn.prepare(sql).unwrap();
    let mut rows = statement.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        digest.stock_row(row);
    }
    digest
}

async fn frank_digest(conn: &Connection, sql: &str) -> Digest {
    let mut digest = Digest::default();
    let statement = conn.prepare(sql).await.expect("prepare scan");
    statement
        .query_with_params_for_each(&[], |row| {
            digest.frank_row(row);
            Ok(())
        })
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    digest
}

async fn open_reader(path: &str, physically_read_only: bool) -> Connection {
    if physically_read_only {
        // This is exactly the core path used by
        // fsqlite::compat::open_with_flags(SQLITE_OPEN_READ_ONLY).
        Connection::open_schema_only(path).await.unwrap()
    } else {
        Connection::open(path).await.unwrap()
    }
}

fn stock_concurrent_scan(path: &Path) {
    fixture(path, TEST_ROWS);
    let reader = rusqlite::Connection::open(path).unwrap();
    let writer = rusqlite::Connection::open(path).unwrap();
    reader
        .execute_batch("PRAGMA query_only=ON; PRAGMA cache_size=16; BEGIN")
        .unwrap();
    let expected_body = "x".repeat(BODY_BYTES);
    let mut emitted = 0_i64;
    {
        let mut statement = reader.prepare(STREAM).unwrap();
        let mut rows = statement.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            emitted += 1;
            assert_eq!(row.get::<_, i64>(0).unwrap(), emitted);
            assert_eq!(row.get::<_, String>(2).unwrap(), expected_body);
            if emitted == 128 {
                writer.execute_batch("BEGIN").unwrap();
                for target in [1, TEST_ROWS] {
                    assert_eq!(
                        writer
                            .execute(
                                "UPDATE big SET body=?1 WHERE id=?2",
                                rusqlite::params!["y".repeat(BODY_BYTES), target],
                            )
                            .unwrap(),
                        1
                    );
                }
                writer.execute_batch("COMMIT").unwrap();
            }
        }
    }
    assert_eq!(emitted, TEST_ROWS);
    let old_first: String = reader
        .query_row("SELECT body FROM big WHERE id=1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(old_first, expected_body);
    reader.execute_batch("PRAGMA cache_size=65536").unwrap();
    let old: String = reader
        .query_row("SELECT body FROM big WHERE id=?1", [TEST_ROWS], |row| row.get(0))
        .unwrap();
    assert_eq!(old, expected_body);
    reader.execute_batch("ROLLBACK").unwrap();
    let current: String = reader
        .query_row("SELECT body FROM big WHERE id=?1", [TEST_ROWS], |row| row.get(0))
        .unwrap();
    assert_eq!(current, "y".repeat(BODY_BYTES));
    let current_first: String = reader
        .query_row("SELECT body FROM big WHERE id=1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(current_first, "y".repeat(BODY_BYTES));
    println!(
        "GH525_CONFLICT {}",
        serde_json::json!({"engine": "sqlite", "cache_pages": 16,
            "commit_after_rows": 128, "rows_emitted": emitted, "busy_snapshot": false})
    );
}

#[test]
fn large_autocommit_scans_match_sqlite_beyond_cache_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("autocommit.db");
    fixture(&path, TEST_ROWS);
    let stock = rusqlite::Connection::open(&path).unwrap();
    let expected: Vec<_> = [COUNT, STREAM, ANALYTICS, JOIN]
        .iter()
        .map(|sql| stock_digest(&stock, sql).result())
        .collect();
    stock.close().unwrap();

    asupersync::test_utils::run_test(|| async {
        for read_only in [false, true] {
            let conn = open_reader(&path.to_string_lossy(), read_only).await;
            conn.execute("PRAGMA query_only=ON").await.unwrap();
            for with_peer in [false, true] {
                let peer = if with_peer {
                    Some(open_reader(&path.to_string_lossy(), read_only).await)
                } else {
                    None
                };
                for cache_size in [0, 8, -2000] {
                    conn.execute(&format!("PRAGMA cache_size={cache_size}"))
                        .await
                        .unwrap();
                    for (sql, oracle) in [COUNT, STREAM, ANALYTICS, JOIN].iter().zip(&expected) {
                        assert_eq!(
                            &frank_digest(&conn, sql).await.result(),
                            oracle,
                            "read_only={read_only}, idle_peer={with_peer}, cache_size={cache_size}, {sql}"
                        );
                    }
                }
                if let Some(peer) = peer {
                    peer.close_without_checkpoint().await.unwrap();
                }
            }
            conn.close_without_checkpoint().await.unwrap();
        }
    });
}

#[test]
fn long_explicit_transaction_repeats_scans_across_cache_resizes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("explicit.db");
    fixture(&path, TEST_ROWS);
    let stock = rusqlite::Connection::open(&path).unwrap();
    let expected = stock_digest(&stock, STREAM).result();
    stock.close().unwrap();

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(path.to_string_lossy().into_owned())
            .await
            .unwrap();
        conn.execute("PRAGMA query_only=ON").await.unwrap();
        conn.execute("PRAGMA cache_size=8").await.unwrap();
        conn.execute("BEGIN").await.unwrap();
        for cache_size in [8, 0, 64, 1] {
            conn.execute(&format!("PRAGMA cache_size={cache_size}"))
                .await
                .unwrap();
            assert_eq!(frank_digest(&conn, STREAM).await.result(), expected);
            let count = conn.query_row(COUNT).await.unwrap();
            assert_eq!(count.values(), &[SqliteValue::Integer(TEST_ROWS)]);
        }
        conn.execute("COMMIT").await.unwrap();
        assert_eq!(frank_digest(&conn, STREAM).await.result(), expected);
        conn.close_without_checkpoint().await.unwrap();
    });
}

#[test]
fn concurrent_commit_never_mixes_rows_after_retention_is_exhausted() {
    let dir = tempfile::tempdir().unwrap();
    stock_concurrent_scan(&dir.path().join("stock-concurrent.db"));
    let path = dir.path().join("concurrent.db");
    fixture(&path, TEST_ROWS);
    let path_string = path.to_string_lossy().into_owned();
    let (write_tx, write_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let writer_path = path_string.clone();
    let writer = std::thread::spawn(move || {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(writer_path).await.unwrap();
            result_tx.send(Ok::<(), String>(())).unwrap();
            if write_rx.recv_timeout(Duration::from_secs(30)).is_ok() {
                // One publication changes both an already-read retained page
                // and a page the scan has not reached. Re-reading row 1 must
                // now prove snapshot isolation, not merely read unchanged data.
                let outcome = async {
                    conn.execute("BEGIN").await?;
                    for target in [1, TEST_ROWS] {
                        assert_eq!(
                            conn.execute_with_params(
                                "UPDATE big SET body=?1 WHERE id=?2",
                                &[
                                    SqliteValue::Text("y".repeat(BODY_BYTES).into()),
                                    SqliteValue::Integer(target),
                                ],
                            )
                            .await?,
                            1
                        );
                    }
                    conn.execute("COMMIT").await?;
                    Ok::<(), FrankenError>(())
                }
                    .await
                    .map_err(|error| error.to_string());
                result_tx.send(outcome).unwrap();
                let _ = finish_rx.recv_timeout(Duration::from_secs(30));
            }
            conn.close_without_checkpoint().await.unwrap();
        });
    });
    result_rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap();

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&path_string).await.unwrap();
        conn.execute("PRAGMA query_only=ON").await.unwrap();
        conn.execute("PRAGMA cache_size=16").await.unwrap();
        conn.execute("BEGIN").await.unwrap();
        let first_sql = "SELECT body FROM big WHERE id=1";
        let first = conn.query_row(first_sql).await.unwrap();
        let expected_body = "x".repeat(BODY_BYTES);
        assert_eq!(first.values(), &[SqliteValue::Text(expected_body.clone().into())]);

        let mut emitted = 0_i64;
        let statement = conn.prepare(STREAM).await.unwrap();
        let outcome = statement
            .query_with_params_for_each(&[], |row| {
                emitted += 1;
                assert_eq!(row.values()[0], SqliteValue::Integer(emitted), "no duplicated prefix");
                let SqliteValue::Text(body) = &row.values()[2] else {
                    panic!("expected text body");
                };
                assert_eq!(body.as_str(), expected_body, "mixed snapshot at row {emitted}");
                if emitted == 128 {
                    write_tx.send(()).unwrap();
                    result_rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap();
                }
                Ok(())
            })
            .await;
        let conflicted = match outcome {
            Ok(()) => {
                assert_eq!(emitted, TEST_ROWS, "successful scan must be complete");
                false
            }
            Err(FrankenError::BusySnapshot { .. }) => {
                assert!(emitted >= 128);
                true
            }
            Err(error) => panic!("unexpected scan failure: {error}"),
        };
        drop(statement);
        assert_eq!(conn.query_row(first_sql).await.unwrap().values(), first.values());

        // Growing the cache after saturation must not re-authorize an image
        // from a newer publication. A historical resolver may still succeed,
        // but then it MUST return the old payload.
        conn.execute("PRAGMA cache_size=65536").await.unwrap();
        match conn
            .query_row(&format!("SELECT body FROM big WHERE id={TEST_ROWS}"))
            .await
        {
            Ok(row) => assert_eq!(row.values(), &[SqliteValue::Text(expected_body.into())]),
            Err(FrankenError::BusySnapshot { .. }) => {},
            Err(error) => panic!("unexpected post-publication read failure: {error}"),
        }
        conn.execute("ROLLBACK").await.unwrap();
        let current = conn
            .query_row(&format!("SELECT body FROM big WHERE id={TEST_ROWS}"))
            .await
            .unwrap();
        assert_eq!(current.values(), &[SqliteValue::Text("y".repeat(BODY_BYTES).into())]);
        let current_first = conn.query_row(first_sql).await.unwrap();
        assert_eq!(
            current_first.values(),
            &[SqliteValue::Text("y".repeat(BODY_BYTES).into())]
        );
        println!(
            "GH525_CONFLICT {}",
            serde_json::json!({"engine": "fsqlite", "cache_pages": 16, "commit_after_rows": 128,
                "rows_emitted": emitted, "busy_snapshot": conflicted})
        );
        conn.close_without_checkpoint().await.unwrap();
    });
    finish_tx.send(()).unwrap();
    writer.join().unwrap();
}

fn proc_kib(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(field)?.split_whitespace().next()?.parse().ok())
}

#[test]
#[ignore = "explicit large stock-SQLite fixture generation for isolated GH525 profiles"]
fn gh525_profile_fixture() {
    let path = std::env::var("GH525_DB").expect("GH525_DB is required");
    let rows = std::env::var("GH525_ROWS")
        .unwrap_or_else(|_| "200000".to_owned())
        .parse()
        .expect("GH525_ROWS must be a positive integer");
    fixture(Path::new(&path), rows);
    println!(
        "GH525_FIXTURE {}",
        serde_json::json!({"path": path, "rows": rows, "body_bytes": BODY_BYTES,
            "database_bytes": std::fs::metadata(&path).unwrap().len(),
            "sqlite_version": rusqlite::version()})
    );
}

#[test]
#[ignore = "isolated GH525 profile child; run separately for each engine/shape/cache budget"]
fn gh525_profile_child() {
    let path = std::env::var("GH525_DB").expect("GH525_DB is required");
    let engine = std::env::var("GH525_ENGINE").unwrap_or_else(|_| "fsqlite".to_owned());
    let shape = std::env::var("GH525_SHAPE").unwrap_or_else(|_| "count".to_owned());
    let cache_size: i32 = std::env::var("GH525_CACHE_SIZE")
        .unwrap_or_else(|_| "-2000".to_owned())
        .parse()
        .expect("GH525_CACHE_SIZE must be an i32");
    let with_peer = std::env::var("GH525_PEER").is_ok_and(|value| value == "1");
    let read_only = std::env::var("GH525_READ_ONLY").is_ok_and(|value| value == "1");
    let (sql, repetitions) = match shape.as_str() {
        "count" => (COUNT, 1),
        "stream" => (STREAM, 1),
        "analytics" => (ANALYTICS, 1),
        "join" => (JOIN, 1),
        "explicit" => (STREAM, 3),
        _ => panic!("unknown GH525_SHAPE={shape}"),
    };
    let started = Instant::now();
    let rss_before = proc_kib("VmRSS:");
    let mut results = Vec::new();
    let query_ns;
    let mut cache_stats = None;
    match engine.as_str() {
        "fsqlite" => {
            let mut elapsed = 0_u128;
            asupersync::test_utils::run_test(|| async {
                let conn = open_reader(&path, read_only).await;
                let peer = if with_peer {
                    Some(open_reader(&path, read_only).await)
                } else {
                    None
                };
                conn.execute("PRAGMA query_only=ON").await.unwrap();
                conn.execute(&format!("PRAGMA cache_size={cache_size}"))
                    .await
                    .unwrap();
                if shape == "explicit" {
                    conn.execute("BEGIN").await.unwrap();
                }
                let query_started = Instant::now();
                for _ in 0..repetitions {
                    results.push(frank_digest(&conn, sql).await.result());
                }
                elapsed = query_started.elapsed().as_nanos();
                cache_stats = Some(conn.memory_stats().unwrap().page_cache);
                if shape == "explicit" {
                    conn.execute("COMMIT").await.unwrap();
                }
                conn.close_without_checkpoint().await.unwrap();
                if let Some(peer) = peer {
                    peer.close_without_checkpoint().await.unwrap();
                }
            });
            query_ns = elapsed;
        }
        "sqlite" => {
            let flags = if read_only {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            } else {
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            };
            let conn = rusqlite::Connection::open_with_flags(&path, flags).unwrap();
            let _peer = with_peer.then(|| rusqlite::Connection::open_with_flags(&path, flags).unwrap());
            conn.execute_batch(&format!("PRAGMA query_only=ON; PRAGMA cache_size={cache_size}"))
                .unwrap();
            if shape == "explicit" {
                conn.execute_batch("BEGIN").unwrap();
            }
            let query_started = Instant::now();
            for _ in 0..repetitions {
                results.push(stock_digest(&conn, sql).result());
            }
            query_ns = query_started.elapsed().as_nanos();
            if shape == "explicit" {
                conn.execute_batch("COMMIT").unwrap();
            }
            conn.close().unwrap();
        }
        _ => panic!("unknown GH525_ENGINE={engine}"),
    }
    assert!(results.windows(2).all(|pair| pair[0] == pair[1]));
    println!(
        "GH525_MEASUREMENT {}",
        serde_json::json!({
            "engine": engine, "sqlite_version": rusqlite::version(), "shape": shape,
            "database_bytes": std::fs::metadata(&path).unwrap().len(),
            "cache_size": cache_size, "idle_peer": with_peer, "results": results,
            "physical_read_only": read_only,
            "page_buffer_max_env": std::env::var("FSQLITE_PAGE_BUFFER_MAX").ok(),
            "query_ns": query_ns, "total_ns": started.elapsed().as_nanos(),
            "rss_before_kib": rss_before, "rss_after_kib": proc_kib("VmRSS:"),
            "peak_rss_kib": proc_kib("VmHWM:"),
            "cached_pages": cache_stats.as_ref().map(|stats| stats.cached_pages),
            "cache_page_budget": cache_stats.as_ref().map(|stats| stats.cache_page_budget),
            "pool_capacity": cache_stats.as_ref().map(|stats| stats.pool_capacity),
        })
    );
}
