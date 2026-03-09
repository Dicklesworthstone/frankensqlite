//! Benchmark: single-threaded sequential write throughput.
//!
//! Bead: bd-1dus
//!
//! Measures INSERT throughput (rows/sec) for both `FrankenSQLite` and C `SQLite`
//! across three transaction strategies:
//!
//! 1. **Autocommit**: each INSERT is its own implicit transaction.
//! 2. **Batched**: 10 batches of 1,000 INSERTs each, wrapped in explicit txns.
//! 3. **Single transaction**: all 10,000 INSERTs in one BEGIN…COMMIT.
//!
//! Both backends use identical PRAGMA settings (best-effort for in-memory) and
//! verify final row counts to confirm correctness.

use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_types::value::SqliteValue;

const ROW_COUNT: i64 = 10_000;
const BATCH_SIZE: i64 = 1000;
const NUM_BATCHES: i64 = 10;

// ─── PRAGMA helpers ─────────────────────────────────────────────────────

/// Apply normalised PRAGMA settings on C `SQLite` (best-effort for in-memory).
fn apply_pragmas_csqlite(conn: &rusqlite::Connection) {
    // page_size must be set before any table creation; journal_mode and
    // synchronous are no-ops for in-memory but included for parity.
    conn.execute_batch(
        "PRAGMA page_size = 4096;\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
    )
    .ok();
}

/// Apply normalised PRAGMA settings on `FrankenSQLite` (best-effort).
fn apply_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        let _ = conn.execute(pragma);
    }
}

// ─── Schema helper ──────────────────────────────────────────────────────

const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);";

// ─── Criterion configuration ────────────────────────────────────────────

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── Variant 1: Autocommit ──────────────────────────────────────────────

fn bench_write_autocommit(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_10k_autocommit");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137))")
                    .unwrap();
                for i in 0..ROW_COUNT {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                // Verify row count.
                let mut count_stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
                let count: i64 = count_stmt.query_row([], |r| r.get(0)).unwrap();
                assert_eq!(count, ROW_COUNT);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                // FrankenSQLite's prepare() currently supports SELECT only,
                // so we use conn.execute() with formatted SQL for INSERTs.
                for i in 0..ROW_COUNT {
                    #[allow(clippy::cast_possible_truncation)]
                    let val = f64::from(i as i32) * 0.137;
                    conn.execute_params(
                        "INSERT INTO bench VALUES (?, ?, ?)",
                        &[
                            fsqlite_types::value::SqliteValue::Integer(i),
                            fsqlite_types::value::SqliteValue::Text(format!("data_{i}")),
                            fsqlite_types::value::SqliteValue::Float(val),
                        ],
                    )
                    .unwrap();
                }
                // Verify row count.
                let rows = conn.query("SELECT COUNT(*) FROM bench").unwrap();
                assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── Variant 2: Batched (1,000 per transaction, 10 batches) ─────────────

fn bench_write_batched(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_10k_batched");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137))")
                    .unwrap();
                for batch in 0..NUM_BATCHES {
                    conn.execute_batch("BEGIN").unwrap();
                    let start = batch * BATCH_SIZE;
                    for i in start..start + BATCH_SIZE {
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                    conn.execute_batch("COMMIT").unwrap();
                }
                let mut count_stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
                let count: i64 = count_stmt.query_row([], |r| r.get(0)).unwrap();
                assert_eq!(count, ROW_COUNT);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                for batch in 0..NUM_BATCHES {
                    conn.execute("BEGIN").unwrap();
                    let start = batch * BATCH_SIZE;
                    for i in start..start + BATCH_SIZE {
                        #[allow(clippy::cast_possible_truncation)]
                        let val = f64::from(i as i32) * 0.137;
                        conn.execute_params(
                            "INSERT INTO bench VALUES (?, ?, ?)",
                            &[
                                fsqlite_types::value::SqliteValue::Integer(i),
                                fsqlite_types::value::SqliteValue::Text(format!("data_{i}")),
                                fsqlite_types::value::SqliteValue::Float(val),
                            ],
                        )
                        .unwrap();
                    }
                    conn.execute("COMMIT").unwrap();
                }
                let rows = conn.query("SELECT COUNT(*) FROM bench").unwrap();
                assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── Variant 3: Single transaction ──────────────────────────────────────

fn bench_write_single_txn(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_10k_single_txn");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137))")
                    .unwrap();
                for i in 0..ROW_COUNT {
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
                let mut count_stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
                let count: i64 = count_stmt.query_row([], |r| r.get(0)).unwrap();
                assert_eq!(count, ROW_COUNT);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                conn.execute("BEGIN").unwrap();
                for i in 0..ROW_COUNT {
                    #[allow(clippy::cast_possible_truncation)]
                    let val = f64::from(i as i32) * 0.137;
                    conn.execute_params(
                        "INSERT INTO bench VALUES (?, ?, ?)",
                        &[
                            fsqlite_types::value::SqliteValue::Integer(i),
                            fsqlite_types::value::SqliteValue::Text(format!("data_{i}")),
                            fsqlite_types::value::SqliteValue::Float(val),
                        ],
                    )
                    .unwrap();
                }
                conn.execute("COMMIT").unwrap();
                let rows = conn.query("SELECT COUNT(*) FROM bench").unwrap();
                assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(
    name = write_throughput;
    config = criterion_config();
    targets = bench_write_autocommit, bench_write_batched, bench_write_single_txn
);
criterion_main!(write_throughput);
