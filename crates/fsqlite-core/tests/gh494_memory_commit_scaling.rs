#![recursion_limit = "512"]
#![cfg(all(feature = "native", not(target_arch = "wasm32")))]

//! GH494: publication must not scale with unrelated committed resident rows.
//!
//! The ordinary tests compare commit, savepoint, rollback, trigger and index
//! semantics with stock SQLite. The ignored release-only probes independently
//! grow a resident table to 32 MiB while repeatedly updating ONE fixed-size row.
//! BEGIN, write, finalization, first reads and the full envelope are measured
//! separately so moving work out of COMMIT cannot disguise the regression.
//!
//! Run with `cargo test --release -p fsqlite-core --no-default-features
//! --features native --test gh494_memory_commit_scaling -- --include-ignored
//! --nocapture --test-threads=1`. Wall-clock thresholds are coarse guards, not
//! a proof of O(touched-state) work; internal publication counters and a native
//! profile are still needed. Do not disable time-travel capture to pass these
//! probes: also run `bd_zjocc_time_travel_history_semantics`.

use std::time::{Duration, Instant};

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const PAYLOAD_BYTES: usize = 4096;
const RESIDENT_STAGES: &[usize] = &[0, 128, 512, 2048, 8192];
const WARMUP_TRANSACTIONS: usize = 8;
const SAMPLES: usize = 101;

fn render(rows: &[Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "null".to_owned(),
                    SqliteValue::Integer(value) => format!("i:{value}"),
                    SqliteValue::Float(value) => format!("r:{value}"),
                    SqliteValue::Text(value) => format!("t:{value}"),
                    SqliteValue::Blob(value) => format!("b:{value:?}"),
                })
                .collect()
        })
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).expect("stock prepare");
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            (0..columns)
                .map(|column| {
                    Ok(match row.get_ref(column)? {
                        rusqlite::types::ValueRef::Null => "null".to_owned(),
                        rusqlite::types::ValueRef::Integer(value) => format!("i:{value}"),
                        rusqlite::types::ValueRef::Real(value) => format!("r:{value}"),
                        rusqlite::types::ValueRef::Text(value) => {
                            format!("t:{}", String::from_utf8_lossy(value))
                        }
                        rusqlite::types::ValueRef::Blob(value) => format!("b:{value:?}"),
                    })
                })
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("stock query")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("stock rows")
}

async fn execute_pair(conn: &Connection, stock: &rusqlite::Connection, sql: &str) {
    stock
        .execute_batch(sql)
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"));
    conn.execute(sql)
        .await
        .unwrap_or_else(|error| panic!("fsqlite `{sql}`: {error}"));
}

const CONTROL_SCHEMA: &[&str] = &[
    "CREATE TABLE resident(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
    "CREATE TABLE hot(id INTEGER PRIMARY KEY, k INTEGER NOT NULL)",
    "CREATE UNIQUE INDEX hot_k ON hot(k)",
    "CREATE TABLE audit(seq INTEGER PRIMARY KEY AUTOINCREMENT, id INTEGER, event INTEGER, k INTEGER)",
    "CREATE INDEX audit_id_event ON audit(id,event)",
    "CREATE TRIGGER hot_insert AFTER INSERT ON hot BEGIN INSERT INTO audit(id,event,k) VALUES(NEW.id,1,NEW.k); END",
    "CREATE TRIGGER hot_update AFTER UPDATE ON hot BEGIN INSERT INTO audit(id,event,k) VALUES(NEW.id,2,NEW.k); END",
    "CREATE TRIGGER hot_delete AFTER DELETE ON hot BEGIN INSERT INTO audit(id,event,k) VALUES(OLD.id,3,OLD.k); END",
    "CREATE TRIGGER hot_reject BEFORE INSERT ON hot WHEN NEW.k < 0 BEGIN SELECT RAISE(ABORT,'negative key'); END",
];

const CONTROL_READS: &[&str] = &[
    "SELECT id,k FROM hot ORDER BY id",
    "SELECT seq,id,event,k FROM audit ORDER BY seq",
    "SELECT count(*),sum(length(payload)) FROM resident",
    "SELECT seq,id,event,k FROM audit INDEXED BY audit_id_event WHERE id=1 ORDER BY event,seq",
    // INDEXED BY makes stale or missing secondary-index entries observable.
    "SELECT id,k FROM hot INDEXED BY hot_k ORDER BY k",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 10",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 20",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 30",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 40",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 50",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 60",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 80",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 99",
];

// The multi-row cases write a row AND an AFTER-trigger audit row before the
// second row fails. Both direct and indirect writes must be rolled back.
const CONTROL_FAILURES: &[&str] = &[
    "INSERT INTO hot VALUES(4,60)",
    "INSERT INTO hot VALUES(5,-1)",
    "INSERT INTO hot VALUES(4,80),(5,60)",
    "INSERT INTO hot VALUES(4,80),(5,-1)",
];

async fn compare_controls(conn: &Connection, stock: &rusqlite::Connection) {
    for sql in CONTROL_READS {
        let actual = conn
            .query(sql)
            .await
            .unwrap_or_else(|error| panic!("fsqlite query `{sql}`: {error}"));
        assert_eq!(render(&actual), stock_rows(stock, sql), "query: {sql}");
    }
}

async fn correctness_controls(file_backed: bool, wal: bool) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = if file_backed {
        directory
            .path()
            .join("controls.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.expect("open fsqlite");
    let stock = rusqlite::Connection::open_in_memory().expect("open stock");
    if wal {
        conn.execute("PRAGMA journal_mode=WAL")
            .await
            .expect("enable WAL");
    }
    for sql in CONTROL_SCHEMA {
        execute_pair(&conn, &stock, sql).await;
    }
    let blob = format!("X'{}'", "ab".repeat(PAYLOAD_BYTES));
    execute_pair(&conn, &stock, "BEGIN").await;
    for id in 1..=64 {
        execute_pair(
            &conn,
            &stock,
            &format!("INSERT INTO resident VALUES({id},{blob})"),
        )
        .await;
    }
    execute_pair(&conn, &stock, "COMMIT").await;
    // Retain a prepared reader across publication and rollback.
    let reader = conn
        .prepare("SELECT id,k FROM hot ORDER BY id")
        .await
        .expect("prepare reader");

    for sql in ["BEGIN", "INSERT INTO hot VALUES(1,10)", "COMMIT"] {
        execute_pair(&conn, &stock, sql).await;
    }
    compare_controls(&conn, &stock).await;
    assert_eq!(
        render(&reader.query().await.expect("prepared read after commit")),
        stock_rows(&stock, "SELECT id,k FROM hot ORDER BY id")
    );

    for sql in [
        "BEGIN",
        "UPDATE hot SET k=20 WHERE id=1",
        "INSERT INTO hot VALUES(2,30)",
        "SAVEPOINT nested",
        "UPDATE hot SET k=99 WHERE id=1",
        "INSERT INTO hot VALUES(3,40)",
        "DELETE FROM hot WHERE id=2",
    ] {
        execute_pair(&conn, &stock, sql).await;
    }
    compare_controls(&conn, &stock).await;
    for sql in ["ROLLBACK TO nested", "RELEASE nested"] {
        execute_pair(&conn, &stock, sql).await;
    }
    compare_controls(&conn, &stock).await;
    execute_pair(&conn, &stock, "COMMIT").await;
    compare_controls(&conn, &stock).await;

    for sql in [
        "BEGIN",
        "UPDATE hot SET k=50 WHERE id=1",
        "DELETE FROM hot WHERE id=2",
        "INSERT INTO hot VALUES(3,60)",
    ] {
        execute_pair(&conn, &stock, sql).await;
    }
    compare_controls(&conn, &stock).await;
    execute_pair(&conn, &stock, "ROLLBACK").await;
    compare_controls(&conn, &stock).await;
    assert_eq!(
        render(&reader.query().await.expect("prepared read after rollback")),
        stock_rows(&stock, "SELECT id,k FROM hot ORDER BY id")
    );

    // Both failure paths must leave the successful statement in the
    // same transaction intact and publish none of the failed writes.
    execute_pair(&conn, &stock, "BEGIN").await;
    execute_pair(&conn, &stock, "INSERT INTO hot VALUES(3,60)").await;
    for sql in CONTROL_FAILURES {
        let before: Vec<_> = CONTROL_READS
            .iter()
            .map(|query| stock_rows(&stock, query))
            .collect();
        assert!(stock.execute_batch(sql).is_err(), "stock must reject {sql}");
        let after: Vec<_> = CONTROL_READS
            .iter()
            .map(|query| stock_rows(&stock, query))
            .collect();
        assert_eq!(before, after, "stock statement atomicity: {sql}");
        assert!(conn.execute(sql).await.is_err(), "fsqlite must reject {sql}");
        compare_controls(&conn, &stock).await;
    }
    execute_pair(&conn, &stock, "COMMIT").await;
    compare_controls(&conn, &stock).await;
    drop(reader);
    if wal {
        let checkpoint = conn
            .query("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .expect("checkpoint WAL");
        assert_eq!(checkpoint.len(), 1);
        assert!(matches!(
            checkpoint[0].values().first(),
            Some(SqliteValue::Integer(0))
        ));
    }
    drop(conn);

    if file_backed {
        // Verify the persisted image BEFORE another engine open could repair it.
        let persisted = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("stock read-only persisted database");
        let integrity: String = persisted
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity check");
        assert_eq!(integrity, "ok");
        for sql in CONTROL_READS {
            assert_eq!(
                stock_rows(&persisted, sql),
                stock_rows(&stock, sql),
                "persisted: {sql}"
            );
        }
        drop(persisted);
        let reopened = Connection::open(&path).await.expect("reopen file");
        compare_controls(&reopened, &stock).await;
        // Check persisted trigger/index definitions, not just rows.
        for sql in ["BEGIN", "INSERT INTO hot VALUES(4,70)", "COMMIT"] {
            execute_pair(&reopened, &stock, sql).await;
        }
        compare_controls(&reopened, &stock).await;
    }
}

#[test]
fn gh494_memory_commit_rollback_trigger_index_controls() {
    asupersync::test_utils::run_test(|| async { correctness_controls(false, false).await });
}

#[test]
fn gh494_file_commit_rollback_trigger_index_controls() {
    asupersync::test_utils::run_test(|| async { correctness_controls(true, false).await });
}

#[test]
fn gh494_wal_file_commit_rollback_trigger_index_controls() {
    asupersync::test_utils::run_test(|| async { correctness_controls(true, true).await });
}

#[derive(Clone, Copy, Debug)]
enum Finish {
    Commit,
    Rollback,
}

impl Finish {
    const fn sql(self) -> &'static str {
        match self {
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
        }
    }
}

#[derive(Clone, Copy)]
struct Sample {
    begin: Duration,
    write: Duration,
    finish: Duration,
    first_reads: Duration,
    envelope: Duration,
}

fn quantile(
    values: impl Iterator<Item = Duration>,
    numerator: usize,
    denominator: usize,
) -> Duration {
    let mut values: Vec<_> = values.collect();
    values.sort_unstable();
    assert!(!values.is_empty());
    values[(values.len() - 1) * numerator / denominator]
}

fn median(samples: &[Sample], field: fn(&Sample) -> Duration) -> Duration {
    quantile(samples.iter().map(field), 1, 2)
}

fn within_timing_budget(baseline: Duration, observed: Duration) -> bool {
    // A deliberately coarse, explicitly non-deterministic guard against the
    // reported ~19x regression. Internal work counters are still required.
    let budget = (baseline * 4).max(baseline + Duration::from_millis(2));
    observed <= budget
}

async fn grow_resident(conn: &Connection, from: usize, to: usize, blob: &str) {
    if from == to {
        return;
    }
    conn.execute("BEGIN").await.expect("begin resident growth");
    for first in ((from + 1)..=to).step_by(16) {
        let last = (first + 15).min(to);
        let rows = (first..=last)
            .map(|id| format!("({id},{blob})"))
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(&format!("INSERT INTO resident VALUES {rows}"))
            .await
            .expect("grow unrelated resident table");
    }
    conn.execute("COMMIT").await.expect("commit resident growth");
}

async fn sample_one_row(
    conn: &Connection,
    finish: Finish,
    previous: i64,
    resident_rows: usize,
) -> Sample {
    let attempted = 1 - previous;
    let expected = match finish {
        Finish::Commit => attempted,
        Finish::Rollback => previous,
    };
    let write_sql = format!("UPDATE hot SET version={attempted} WHERE id=1");
    let resident_sql = format!(
        "SELECT length(payload) FROM resident WHERE id={}",
        resident_rows.max(1)
    );
    let start = Instant::now();
    conn.execute("BEGIN").await.expect("begin sample");
    let begin = start.elapsed();
    let tick = Instant::now();
    conn.execute(&write_sql).await.expect("one-row update");
    let write = tick.elapsed();
    let tick = Instant::now();
    conn.execute(finish.sql()).await.expect("finish sample");
    let finish_elapsed = tick.elapsed();
    let tick = Instant::now();
    let hot = conn
        .query("SELECT version,length(payload) FROM hot WHERE id=1")
        .await
        .expect("first read of touched table");
    let resident = conn.query(&resident_sql).await.expect("first unrelated read");
    let first_reads = tick.elapsed();
    let envelope = start.elapsed();
    assert_eq!(
        render(&hot),
        vec![vec![format!("i:{expected}"), format!("i:{PAYLOAD_BYTES}")]]
    );
    if resident_rows == 0 {
        assert!(resident.is_empty());
    } else {
        assert_eq!(render(&resident), vec![vec![format!("i:{PAYLOAD_BYTES}")]]);
    }
    Sample {
        begin,
        write,
        finish: finish_elapsed,
        first_reads,
        envelope,
    }
}

async fn unrelated_resident_scaling(file_backed: bool, finish: Finish) {
    assert!(!cfg!(debug_assertions), "run this timing test with --release");
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = if file_backed {
        directory
            .path()
            .join("scaling.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.expect("open probe");
    conn.execute("CREATE TABLE resident(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
        .await
        .expect("create resident");
    conn.execute("CREATE TABLE hot(id INTEGER PRIMARY KEY, version INTEGER, payload BLOB NOT NULL)")
        .await
        .expect("create hot");
    let blob = format!("X'{}'", "ab".repeat(PAYLOAD_BYTES));
    conn.execute(&format!("INSERT INTO hot VALUES(1,0,{blob})"))
        .await
        .expect("seed fixed-size hot table");
    let mut resident_rows = 0;
    let mut previous = 0;
    let mut measurements = Vec::new();
    for &size in RESIDENT_STAGES {
        grow_resident(&conn, resident_rows, size, &blob).await;
        resident_rows = size;
        // Traverse payloads outside the timed region. Memory-mode
        // storage is resident; the file case is a backend control.
        let warm = conn
            .query("SELECT payload FROM resident")
            .await
            .expect("warm resident");
        assert_eq!(warm.len(), size);
        drop(warm);
        let mut samples = Vec::with_capacity(SAMPLES);
        for iteration in 0..(WARMUP_TRANSACTIONS + SAMPLES) {
            let sample = sample_one_row(&conn, finish, previous, size).await;
            if matches!(finish, Finish::Commit) {
                previous = 1 - previous;
            }
            if iteration >= WARMUP_TRANSACTIONS {
                samples.push(sample);
            }
        }
        let finish_median = median(&samples, |sample| sample.finish);
        let envelope_median = median(&samples, |sample| sample.envelope);
        println!(
            "backend={} finish={finish:?} resident_rows={size} resident_bytes={} begin_p50_us={} write_p50_us={} finish_p50_us={} finish_p90_us={} first_reads_p50_us={} envelope_p50_us={}",
            if file_backed { "file" } else { "memory" },
            size * PAYLOAD_BYTES,
            median(&samples, |sample| sample.begin).as_micros(),
            median(&samples, |sample| sample.write).as_micros(),
            finish_median.as_micros(),
            quantile(samples.iter().map(|sample| sample.finish), 9, 10).as_micros(),
            median(&samples, |sample| sample.first_reads).as_micros(),
            envelope_median.as_micros(),
        );
        measurements.push((size, finish_median, envelope_median));
    }
    let (_, base_finish, base_envelope) = measurements[0];
    for &(size, observed_finish, observed_envelope) in &measurements[1..] {
        assert!(
            within_timing_budget(base_finish, observed_finish),
            "{finish:?} scales with unrelated rows: file={file_backed}, rows={size}, baseline={base_finish:?}, observed={observed_finish:?}"
        );
        assert!(
            within_timing_budget(base_envelope, observed_envelope),
            "work moved outside finalization: file={file_backed}, finish={finish:?}, rows={size}, baseline={base_envelope:?}, observed={observed_envelope:?}"
        );
    }
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_memory_unrelated_resident_commit_scaling() {
    asupersync::test_utils::run_test(|| async {
        unrelated_resident_scaling(false, Finish::Commit).await;
    });
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_memory_unrelated_resident_rollback_scaling() {
    asupersync::test_utils::run_test(|| async {
        unrelated_resident_scaling(false, Finish::Rollback).await;
    });
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_file_unrelated_resident_commit_scaling() {
    asupersync::test_utils::run_test(|| async {
        unrelated_resident_scaling(true, Finish::Commit).await;
    });
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_file_unrelated_resident_rollback_scaling() {
    asupersync::test_utils::run_test(|| async {
        unrelated_resident_scaling(true, Finish::Rollback).await;
    });
}

async fn original_single_insert_scaling(file_backed: bool) {
    assert!(!cfg!(debug_assertions), "run this timing test with --release");
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = if file_backed {
        directory
            .path()
            .join("original.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.expect("open original repro");
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
        .await
        .expect("create original table");
    let blob = format!("X'{}'", "ab".repeat(PAYLOAD_BYTES));
    let mut id = 0;
    let mut windows = Vec::new();
    for _ in 0..8 {
        let before = id;
        let mut insert_time = Duration::ZERO;
        let mut commit_time = Duration::ZERO;
        for _ in 0..200 {
            id += 1;
            // Keep SQL formatting outside the timed INSERT interval.
            let sql = format!("INSERT INTO t VALUES({id},{blob})");
            conn.execute("BEGIN").await.expect("begin insert transaction");
            let tick = Instant::now();
            conn.execute(&sql).await.expect("insert payload");
            insert_time += tick.elapsed();
            let tick = Instant::now();
            conn.execute("COMMIT").await.expect("commit payload");
            commit_time += tick.elapsed();
        }
        let mean_commit = commit_time / 200;
        println!(
            "original backend={} rows_before={before} insert_mean_us={} commit_mean_us={}",
            if file_backed { "file" } else { "memory" },
            (insert_time / 200).as_micros(),
            mean_commit.as_micros(),
        );
        windows.push(mean_commit);
    }
    let rows = conn
        .query("SELECT count(*),sum(length(payload)) FROM t")
        .await
        .expect("verify committed payloads");
    assert_eq!(
        render(&rows),
        vec![vec!["i:1600".to_owned(), "i:6553600".to_owned()]]
    );
    for (window, &observed) in windows.iter().enumerate().skip(1) {
        assert!(
            within_timing_budget(windows[0], observed),
            "original GH494 slope: file={file_backed}, window={window}, baseline={:?}, observed={observed:?}",
            windows[0]
        );
    }
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_original_memory_single_insert_commit_scaling() {
    asupersync::test_utils::run_test(|| async { original_single_insert_scaling(false).await });
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_original_file_single_insert_commit_scaling() {
    asupersync::test_utils::run_test(|| async { original_single_insert_scaling(true).await });
}
