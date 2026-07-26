//! Track R record-encoding oracle, roundtrip, and throughput coverage for `bd-gieaf`.
#![recursion_limit = "512"]

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use fsqlite_types::{SqliteValue, value::SmallText};
use rusqlite::{params_from_iter, types::Value as RusqliteValue};
use tempfile::tempdir;

const BEAD_ID: &str = "bd-gieaf";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_gieaf_track_r_record_encoding -- --nocapture --test-threads=1";
const INSERT_ROWS_ORACLE: i64 = 10_000;
const INSERT_ROWS_PERF: i64 = 10_000;
const TRACK_R_SCHEMA_SQL: &str = "CREATE TABLE record_track (\
id INTEGER PRIMARY KEY,\
ival INTEGER NOT NULL,\
rval REAL NOT NULL,\
tval TEXT NOT NULL,\
bval BLOB NOT NULL,\
nval NUMERIC\
)";
const TRACK_R_INSERT_SQL: &str = "INSERT INTO record_track VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
const TRACK_R_SELECT_SQL: &str =
    "SELECT id, ival, rval, tval, bval, nval FROM record_track ORDER BY id";

static TRACK_R_E2E_LOCK: Mutex<()> = Mutex::new(());

async fn open_fsqlite(path: &Path) -> fsqlite::Connection {
    let path = path.to_str().expect("utf-8 db path");
    let conn = fsqlite::Connection::open(path)
        .await
        .expect("open fsqlite connection");
    conn.execute("PRAGMA journal_mode=WAL").await.ok();
    conn
}

fn open_sqlite(path: &Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).expect("open sqlite connection");
    conn.execute_batch("PRAGMA journal_mode=WAL;")
        .expect("enable sqlite wal");
    conn
}

fn generated_record(rowid: i64) -> Vec<SqliteValue> {
    let seed = usize::try_from(rowid).unwrap_or(usize::MAX);
    let integer = rowid.saturating_mul(17).saturating_sub(5_000);
    let tail = match seed % 5 {
        0 => SqliteValue::Null,
        1 => SqliteValue::Integer(0),
        2 => SqliteValue::Integer(1),
        3 => SqliteValue::Integer(-1),
        _ => SqliteValue::Integer(i64::try_from(seed % 97).unwrap_or(0) - 48),
    };
    let text = match seed % 4 {
        0 => String::new(),
        1 => format!("row-{seed:05}"),
        2 => "x".repeat((seed % 32) + 1),
        _ => format!("record-{seed:05}-tail"),
    };
    let blob_len = match seed % 6 {
        0 => 0,
        1 => 1,
        2 => 7,
        3 => 19,
        4 => 33,
        _ => (seed % 48) + 1,
    };
    let blob = (0..blob_len)
        .map(|offset| {
            let byte = (seed.wrapping_mul(17)).wrapping_add(offset.wrapping_mul(29)) % 251;
            u8::try_from(byte).unwrap_or(0)
        })
        .collect::<Vec<_>>();

    vec![
        SqliteValue::Integer(rowid),
        SqliteValue::Integer(integer),
        SqliteValue::Float((seed as f64).mul_add(1.625, -777.25)),
        SqliteValue::Text(SmallText::from_string(text)),
        SqliteValue::Blob(Arc::<[u8]>::from(blob)),
        tail,
    ]
}

fn generated_records(row_count: i64) -> Vec<Vec<SqliteValue>> {
    (1..=row_count).map(generated_record).collect()
}

fn values_bitwise_eq(left: &SqliteValue, right: &SqliteValue) -> bool {
    match (left, right) {
        (SqliteValue::Null, SqliteValue::Null) => true,
        (SqliteValue::Integer(lhs), SqliteValue::Integer(rhs)) => lhs == rhs,
        (SqliteValue::Float(lhs), SqliteValue::Float(rhs)) => lhs.to_bits() == rhs.to_bits(),
        (SqliteValue::Text(lhs), SqliteValue::Text(rhs)) => lhs == rhs,
        (SqliteValue::Blob(lhs), SqliteValue::Blob(rhs)) => lhs == rhs,
        _ => false,
    }
}

fn assert_rows_eq(actual: &[Vec<SqliteValue>], expected: &[Vec<SqliteValue>], context: &str) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{context}: row count mismatch"
    );
    for (row_idx, (actual_row, expected_row)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            actual_row.len(),
            expected_row.len(),
            "{context}: column count mismatch at row {row_idx}"
        );
        for (col_idx, (actual_value, expected_value)) in
            actual_row.iter().zip(expected_row.iter()).enumerate()
        {
            assert!(
                values_bitwise_eq(actual_value, expected_value),
                "{context}: mismatch at row {row_idx} col {col_idx}: actual={actual_value:?} expected={expected_value:?}",
            );
        }
    }
}

fn to_rusqlite_value(value: &SqliteValue) -> RusqliteValue {
    match value {
        SqliteValue::Null => RusqliteValue::Null,
        SqliteValue::Integer(integer) => RusqliteValue::Integer(*integer),
        SqliteValue::Float(float) => RusqliteValue::Real(*float),
        SqliteValue::Text(text) => RusqliteValue::Text(text.as_str().to_owned()),
        SqliteValue::Blob(blob) => RusqliteValue::Blob(blob.to_vec()),
    }
}

fn from_rusqlite_value(value: RusqliteValue) -> SqliteValue {
    match value {
        RusqliteValue::Null => SqliteValue::Null,
        RusqliteValue::Integer(integer) => SqliteValue::Integer(integer),
        RusqliteValue::Real(float) => SqliteValue::Float(float),
        RusqliteValue::Text(text) => SqliteValue::Text(SmallText::from_string(text)),
        RusqliteValue::Blob(blob) => SqliteValue::Blob(Arc::<[u8]>::from(blob)),
    }
}

async fn insert_records_fsqlite(conn: &fsqlite::Connection, rows: &[Vec<SqliteValue>]) -> Duration {
    let insert_stmt = conn
        .prepare(TRACK_R_INSERT_SQL)
        .await
        .expect("prepare fsqlite insert");
    let start = Instant::now();
    conn.execute("BEGIN;").await.expect("fsqlite begin");
    for row in rows {
        insert_stmt
            .execute_with_params(row.as_slice())
            .await
            .expect("fsqlite insert");
    }
    conn.execute("COMMIT;").await.expect("fsqlite commit");
    start.elapsed()
}

fn insert_records_sqlite(conn: &rusqlite::Connection, rows: &[Vec<SqliteValue>]) -> Duration {
    let start = Instant::now();
    conn.execute_batch("BEGIN;").expect("sqlite begin");
    let mut insert = conn
        .prepare(TRACK_R_INSERT_SQL)
        .expect("prepare sqlite insert");
    for row in rows {
        let params = row.iter().map(to_rusqlite_value).collect::<Vec<_>>();
        insert
            .execute(params_from_iter(params))
            .expect("sqlite insert");
    }
    conn.execute_batch("COMMIT;").expect("sqlite commit");
    start.elapsed()
}

async fn fetch_fsqlite_rows(conn: &fsqlite::Connection) -> Vec<Vec<SqliteValue>> {
    conn.query(TRACK_R_SELECT_SQL)
        .await
        .expect("query fsqlite rows")
        .into_iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn fetch_sqlite_rows(conn: &rusqlite::Connection) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn
        .prepare(TRACK_R_SELECT_SQL)
        .expect("prepare sqlite select");
    stmt.query_map([], |row| {
        (0..6)
            .map(|idx| row.get::<_, RusqliteValue>(idx).map(from_rusqlite_value))
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .expect("query sqlite rows")
    .map(|row| row.expect("sqlite row"))
    .collect()
}

fn rows_per_sec(rows: i64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return rows as f64;
    }
    rows as f64 / secs
}

#[test]
fn bd_gieaf_track_r_prepared_insert_10k_matches_sqlite_oracle() {
    let _guard = TRACK_R_E2E_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    asupersync::test_utils::run_test(|| async {
        let expected_rows = generated_records(INSERT_ROWS_ORACLE);
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_r_oracle_fsqlite.db");
        let sqlite_db = temp.path().join("track_r_oracle_sqlite.db");

        let fconn = open_fsqlite(&fsqlite_db).await;
        let sconn = open_sqlite(&sqlite_db);

        assert!(
            fconn.is_concurrent_mode_default(),
            "Track R coverage must keep concurrent_mode_default enabled by default"
        );

        fconn
            .execute(TRACK_R_SCHEMA_SQL)
            .await
            .expect("create fsqlite table");
        sconn
            .execute_batch(&(TRACK_R_SCHEMA_SQL.to_owned() + ";"))
            .expect("create sqlite table");

        let fsqlite_elapsed = insert_records_fsqlite(&fconn, &expected_rows).await;
        let sqlite_elapsed = insert_records_sqlite(&sconn, &expected_rows);

        let fsqlite_rows = fetch_fsqlite_rows(&fconn).await;
        let sqlite_rows = fetch_sqlite_rows(&sconn);
        assert_rows_eq(
            &fsqlite_rows,
            &sqlite_rows,
            "Track R oracle rowset mismatch",
        );
        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=TRACK-R-ORACLE-10K rows={} fsqlite_rows_per_sec={:.1} sqlite_rows_per_sec={:.1} replay_command={REPLAY_COMMAND}",
            INSERT_ROWS_ORACLE,
            rows_per_sec(INSERT_ROWS_ORACLE, fsqlite_elapsed),
            rows_per_sec(INSERT_ROWS_ORACLE, sqlite_elapsed),
        );
    });
}

#[test]
fn bd_gieaf_track_r_roundtrip_10k_query_is_lossless() {
    let _guard = TRACK_R_E2E_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    asupersync::test_utils::run_test(|| async {
        let expected_rows = generated_records(INSERT_ROWS_ORACLE);
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_r_roundtrip_fsqlite.db");
        let fconn = open_fsqlite(&fsqlite_db).await;

        assert!(
            fconn.is_concurrent_mode_default(),
            "Track R coverage must keep concurrent_mode_default enabled by default"
        );

        fconn
            .execute(TRACK_R_SCHEMA_SQL)
            .await
            .expect("create fsqlite table");
        let _insert_elapsed = insert_records_fsqlite(&fconn, &expected_rows).await;
        let fsqlite_rows = fetch_fsqlite_rows(&fconn).await;
        assert_rows_eq(
            &fsqlite_rows,
            &expected_rows,
            "Track R roundtrip rowset mismatch",
        );

        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=TRACK-R-ROUNDTRIP-10K rows={} replay_command={REPLAY_COMMAND}",
            INSERT_ROWS_ORACLE,
        );
    });
}

#[test]
#[ignore = "manual perf probe; run via rch when validating Track R throughput"]
fn bd_gieaf_track_r_prepared_insert_10k_perf_probe_emits_metrics() {
    let _guard = TRACK_R_E2E_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    asupersync::test_utils::run_test(|| async {
        let rows = generated_records(INSERT_ROWS_PERF);
        let temp = tempdir().expect("tempdir");
        let fsqlite_db = temp.path().join("track_r_perf_fsqlite.db");
        let sqlite_db = temp.path().join("track_r_perf_sqlite.db");

        let fconn = open_fsqlite(&fsqlite_db).await;
        let sconn = open_sqlite(&sqlite_db);

        fconn
            .execute(TRACK_R_SCHEMA_SQL)
            .await
            .expect("create fsqlite table");
        sconn
            .execute_batch(&(TRACK_R_SCHEMA_SQL.to_owned() + ";"))
            .expect("create sqlite table");

        let fsqlite_elapsed = insert_records_fsqlite(&fconn, &rows).await;
        let sqlite_elapsed = insert_records_sqlite(&sconn, &rows);

        assert_eq!(
            fetch_fsqlite_rows(&fconn).await.len(),
            INSERT_ROWS_PERF as usize,
            "perf probe should persist the full Track R rowset"
        );
        assert_eq!(
            fetch_sqlite_rows(&sconn).len(),
            INSERT_ROWS_PERF as usize,
            "sqlite perf probe should persist the full Track R rowset"
        );
        eprintln!(
            "INFO bead_id={BEAD_ID} scenario=TRACK-R-PERF-10K rows={} fsqlite_rows_per_sec={:.1} sqlite_rows_per_sec={:.1} replay_command={REPLAY_COMMAND}",
            INSERT_ROWS_PERF,
            rows_per_sec(INSERT_ROWS_PERF, fsqlite_elapsed),
            rows_per_sec(INSERT_ROWS_PERF, sqlite_elapsed),
        );
    });
}
