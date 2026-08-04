#![recursion_limit = "512"]
// bd-mnlk2 / bd-zavyn: the consolidated timed bodies await fsqlite-core's
// deliberately non-`Send`, deeply nested engine futures inside one runtime
// entry per sample; `future_not_send` and `large_futures` contradict that
// design (see fsqlite-core/src/lib.rs for the full rationale, including why
// boxing was rejected by the perf ledger).
#![allow(clippy::future_not_send)]
#![allow(clippy::large_futures)]

//! Benchmark: single-threaded sequential write throughput.
//!
//! Bead: bd-1dus
//!
//! Measures INSERT throughput (rows/sec) for both `FrankenSQLite` and C `SQLite`
//! across three transaction strategies and two statement modes:
//!
//! 1. **Autocommit**: each INSERT is its own implicit transaction.
//! 2. **Batched**: 10 batches of 1,000 INSERTs each, wrapped in explicit txns.
//! 3. **Single transaction**: all 10,000 INSERTs in one BEGIN…COMMIT.
//! 4. **Prepared vs ad-hoc**: each backend is measured both with a reused
//!    prepared DML statement and with per-call ad-hoc execution so the parser
//!    and codegen tax stays explicit instead of being accidentally folded into
//!    a backend comparison.
//!
//! Both backends require identical PRAGMA setup to succeed and
//! verify final row counts to confirm correctness.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_types::value::SqliteValue;

const ROW_COUNT: i64 = 10_000;
const BATCH_SIZE: i64 = 1000;
const NUM_BATCHES: i64 = 10;

// ─── PRAGMA helpers ─────────────────────────────────────────────────────

/// Apply normalised PRAGMA settings on C `SQLite`.
fn apply_pragmas_csqlite(conn: &rusqlite::Connection) {
    // page_size must be set before any table creation; journal_mode and
    // synchronous are no-ops for in-memory but included for parity.
    conn.execute_batch(
        "PRAGMA page_size = 4096;\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
    )
    .expect("failed to apply C SQLite benchmark pragmas");
}

/// Apply normalised PRAGMA settings on `FrankenSQLite`.
fn apply_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        fsqlite_e2e::block_on(conn.execute(pragma)).unwrap_or_else(|error| {
            panic!("failed to apply FrankenSQLite benchmark pragma `{pragma}`: {error}")
        });
    }
}

// ─── Schema helper ──────────────────────────────────────────────────────

const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);";
const INSERT_SQL: &str = "INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137));";

fn run_csqlite_prepared_inserts(conn: &rusqlite::Connection, start: i64, end: i64) {
    let mut stmt = conn.prepare(INSERT_SQL).unwrap();
    for i in start..end {
        black_box(stmt.execute(rusqlite::params![i]).unwrap());
    }
}

fn run_csqlite_ad_hoc_inserts(conn: &rusqlite::Connection, start: i64, end: i64) {
    for i in start..end {
        black_box(conn.execute(INSERT_SQL, rusqlite::params![i]).unwrap());
    }
}

async fn run_fsqlite_prepared_inserts(conn: &fsqlite::Connection, start: i64, end: i64) {
    let stmt = conn.prepare(INSERT_SQL).await.unwrap();
    for i in start..end {
        black_box(
            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                .await
                .unwrap(),
        );
    }
}

async fn run_fsqlite_ad_hoc_inserts(conn: &fsqlite::Connection, start: i64, end: i64) {
    for i in start..end {
        black_box(
            conn.execute_with_params(INSERT_SQL, &[SqliteValue::Integer(i)])
                .await
                .unwrap(),
        );
    }
}

fn verify_row_count_csqlite(conn: &rusqlite::Connection) {
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM bench", [], |row| row.get(0))
        .unwrap();
    black_box(count);
    assert_eq!(count, ROW_COUNT);
}

async fn verify_row_count_fsqlite(conn: &fsqlite::Connection) {
    let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
    black_box(&rows);
    assert_eq!(rows[0].values()[0], SqliteValue::Integer(ROW_COUNT));
}

// ─── Criterion configuration ────────────────────────────────────────────

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── Variant 1: Autocommit ──────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=csqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
fn bench_write_autocommit(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_10k_autocommit");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("csqlite_prepared", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                run_csqlite_prepared_inserts(&conn, 0, ROW_COUNT);
                verify_row_count_csqlite(&conn);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("csqlite_ad_hoc", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                run_csqlite_ad_hoc_inserts(&conn, 0, ROW_COUNT);
                verify_row_count_csqlite(&conn);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite_prepared", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(CREATE_TABLE)).unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    run_fsqlite_prepared_inserts(&conn, 0, ROW_COUNT).await;
                    verify_row_count_fsqlite(&conn).await;
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite_ad_hoc", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(CREATE_TABLE)).unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    run_fsqlite_ad_hoc_inserts(&conn, 0, ROW_COUNT).await;
                    verify_row_count_fsqlite(&conn).await;
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── Variant 2: Batched (1,000 per transaction, 10 batches) ─────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=csqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
fn bench_write_batched(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_10k_batched");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("csqlite_prepared", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                for batch in 0..NUM_BATCHES {
                    conn.execute_batch("BEGIN").unwrap();
                    let start = batch * BATCH_SIZE;
                    run_csqlite_prepared_inserts(&conn, start, start + BATCH_SIZE);
                    conn.execute_batch("COMMIT").unwrap();
                }
                verify_row_count_csqlite(&conn);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("csqlite_ad_hoc", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                for batch in 0..NUM_BATCHES {
                    conn.execute_batch("BEGIN").unwrap();
                    let start = batch * BATCH_SIZE;
                    run_csqlite_ad_hoc_inserts(&conn, start, start + BATCH_SIZE);
                    conn.execute_batch("COMMIT").unwrap();
                }
                verify_row_count_csqlite(&conn);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite_prepared", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(CREATE_TABLE)).unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    for batch in 0..NUM_BATCHES {
                        conn.execute("BEGIN").await.unwrap();
                        let start = batch * BATCH_SIZE;
                        run_fsqlite_prepared_inserts(&conn, start, start + BATCH_SIZE).await;
                        conn.execute("COMMIT").await.unwrap();
                    }
                    verify_row_count_fsqlite(&conn).await;
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite_ad_hoc", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(CREATE_TABLE)).unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    for batch in 0..NUM_BATCHES {
                        conn.execute("BEGIN").await.unwrap();
                        let start = batch * BATCH_SIZE;
                        run_fsqlite_ad_hoc_inserts(&conn, start, start + BATCH_SIZE).await;
                        conn.execute("COMMIT").await.unwrap();
                    }
                    verify_row_count_fsqlite(&conn).await;
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── Variant 3: Single transaction ──────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=csqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
fn bench_write_single_txn(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_10k_single_txn");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("csqlite_prepared", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN").unwrap();
                run_csqlite_prepared_inserts(&conn, 0, ROW_COUNT);
                conn.execute_batch("COMMIT").unwrap();
                verify_row_count_csqlite(&conn);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("csqlite_ad_hoc", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(CREATE_TABLE).unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN").unwrap();
                run_csqlite_ad_hoc_inserts(&conn, 0, ROW_COUNT);
                conn.execute_batch("COMMIT").unwrap();
                verify_row_count_csqlite(&conn);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite_prepared", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(CREATE_TABLE)).unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN").await.unwrap();
                    run_fsqlite_prepared_inserts(&conn, 0, ROW_COUNT).await;
                    conn.execute("COMMIT").await.unwrap();
                    verify_row_count_fsqlite(&conn).await;
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite_ad_hoc", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(CREATE_TABLE)).unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN").await.unwrap();
                    run_fsqlite_ad_hoc_inserts(&conn, 0, ROW_COUNT).await;
                    conn.execute("COMMIT").await.unwrap();
                    verify_row_count_fsqlite(&conn).await;
                });
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
