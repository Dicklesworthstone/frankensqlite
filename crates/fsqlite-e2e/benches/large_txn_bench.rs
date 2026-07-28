#![recursion_limit = "512"]

//! Benchmark: large transaction (100K row single-txn insert).
//!
//! Bead: bd-6d9v
//!
//! Measures performance of large transactions that stress the page cache and
//! version management.  A single BEGIN…COMMIT wrapping 100,000 INSERT
//! statements.  Verifies final row count for correctness.
//!
//! The 1M-row variant is omitted from Criterion (too slow for iterative
//! sampling) but is available as a standalone `#[test]` in the correctness
//! suite.

// bd-mnlk2 / bd-zavyn: the hoisted timed bodies await fsqlite-core's
// deliberately large, deeply nested engine futures inside one runtime entry
// per sample; boxing them would put an allocation inside the timed window.
#![allow(clippy::large_futures)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_types::value::SqliteValue;

const ROW_COUNT_10K: u64 = 10_000;
const ROW_COUNT_100K: u64 = 100_000;

// ─── PRAGMA helpers ─────────────────────────────────────────────────────

fn apply_pragmas_csqlite(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "PRAGMA page_size = 4096;\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
    )
    .expect("failed to apply C SQLite benchmark pragmas");
}

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

const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score INTEGER, created TEXT);";

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── 10K single-transaction INSERT ──────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_large_txn_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_txn_prepared_insert_10k");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(ROW_COUNT_10K));

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
                    .prepare(
                        "INSERT INTO bench VALUES (\
                             ?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), \
                             ('2026-01-' || ((?1 % 28) + 1))\
                         )",
                    )
                    .unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..ROW_COUNT_10K as i64 {
                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                }
                conn.execute_batch("COMMIT").unwrap();

                let mut count_stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
                let count: i64 = count_stmt.query_row([], |r| r.get(0)).unwrap();
                black_box(count);
                #[allow(clippy::cast_possible_wrap)]
                let expected = ROW_COUNT_10K as i64;
                assert_eq!(count, expected);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
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
                    let stmt = conn
                        .prepare(
                            "INSERT INTO bench VALUES (\
                             ?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), \
                             ('2026-01-' || ((?1 % 28) + 1))\
                         )",
                        )
                        .await
                        .unwrap();
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..ROW_COUNT_10K as i64 {
                        black_box(
                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    conn.execute("COMMIT").await.unwrap();

                    let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
                    black_box(&rows);
                    #[allow(clippy::cast_possible_wrap)]
                    let expected = ROW_COUNT_10K as i64;
                    assert_eq!(rows[0].values()[0], SqliteValue::Integer(expected));
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── 100K single-transaction INSERT ─────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_large_txn_100k(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_txn_100k");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));
    group.throughput(Throughput::Elements(ROW_COUNT_100K));

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
                    .prepare(
                        "INSERT INTO bench VALUES (\
                             ?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), \
                             ('2026-01-' || ((?1 % 28) + 1))\
                         )",
                    )
                    .unwrap();
                #[allow(clippy::cast_possible_wrap)]
                for i in 0..ROW_COUNT_100K as i64 {
                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                }
                conn.execute_batch("COMMIT").unwrap();

                let mut count_stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
                let count: i64 = count_stmt.query_row([], |r| r.get(0)).unwrap();
                black_box(count);
                #[allow(clippy::cast_possible_wrap)]
                let expected = ROW_COUNT_100K as i64;
                assert_eq!(count, expected);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
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
                    let stmt = conn
                        .prepare(
                            "INSERT INTO bench VALUES (\
                             ?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), \
                             ('2026-01-' || ((?1 % 28) + 1))\
                         )",
                        )
                        .await
                        .unwrap();
                    #[allow(clippy::cast_possible_wrap)]
                    for i in 0..ROW_COUNT_100K as i64 {
                        black_box(
                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    conn.execute("COMMIT").await.unwrap();

                    let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
                    black_box(&rows);
                    #[allow(clippy::cast_possible_wrap)]
                    let expected = ROW_COUNT_100K as i64;
                    assert_eq!(rows[0].values()[0], SqliteValue::Integer(expected));
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── 100K batched (10 × 10K) ────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_large_txn_100k_batched(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_txn_100k_batched_10k");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));
    group.throughput(Throughput::Elements(ROW_COUNT_100K));

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
                    .prepare(
                        "INSERT INTO bench VALUES (\
                             ?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), \
                             ('2026-01-' || ((?1 % 28) + 1))\
                         )",
                    )
                    .unwrap();
                for batch in 0..10_i64 {
                    conn.execute_batch("BEGIN").unwrap();
                    let base = batch * 10_000;
                    for i in base..base + 10_000 {
                        black_box(stmt.execute(rusqlite::params![i]).unwrap());
                    }
                    conn.execute_batch("COMMIT").unwrap();
                }
                let mut count_stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
                let count: i64 = count_stmt.query_row([], |r| r.get(0)).unwrap();
                black_box(count);
                #[allow(clippy::cast_possible_wrap)]
                let expected = ROW_COUNT_100K as i64;
                assert_eq!(count, expected);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
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
                    let stmt = conn
                        .prepare(
                            "INSERT INTO bench VALUES (\
                             ?1, ('name_' || ?1), ('user_' || ?1 || '@test.com'), (?1 * 7), \
                             ('2026-01-' || ((?1 % 28) + 1))\
                         )",
                        )
                        .await
                        .unwrap();
                    for batch in 0..10_i64 {
                        conn.execute("BEGIN").await.unwrap();
                        let base = batch * 10_000;
                        for i in base..base + 10_000 {
                            black_box(
                                stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                    .await
                                    .unwrap(),
                            );
                        }
                        conn.execute("COMMIT").await.unwrap();
                    }
                    let rows = conn.query("SELECT COUNT(*) FROM bench").await.unwrap();
                    black_box(&rows);
                    #[allow(clippy::cast_possible_wrap)]
                    let expected = ROW_COUNT_100K as i64;
                    assert_eq!(rows[0].values()[0], SqliteValue::Integer(expected));
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(
    name = large_txn;
    config = criterion_config();
    targets = bench_large_txn_10k, bench_large_txn_100k, bench_large_txn_100k_batched
);
criterion_main!(large_txn);
