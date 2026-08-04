//! Benchmark: mixed OLTP workload (80% read / 20% write).
//!
//! Bead: bd-1fez
//!
//! Simulates a realistic OLTP workload: predominantly reads with occasional
//! writes.  Uses a deterministic xorshift64 PRNG to select operations:
//!
//! - 80% SELECT (point lookups, range scans, aggregates)
//! - 15% INSERT
//! - 3% UPDATE
//! - 2% DELETE
//!
//! The benchmark measures throughput (ops/sec) over a fixed number of
//! operations on both backends.
//!
// bd-mnlk2 / bd-zavyn: the hoisted timed bodies await fsqlite-core's
// deliberately large, deeply nested engine futures inside one runtime entry
// per sample; boxing them would put an allocation inside the timed window.
#![allow(clippy::large_futures)]

use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite::{FrankenError, SqliteValue};
use rusqlite::OptionalExtension;

const SEED_ROWS: usize = 500;
const OPS_PER_ITERATION: u64 = 2000;

// ─── Deterministic PRNG ─────────────────────────────────────────────────

struct Rng64 {
    state: u64,
}

impl Rng64 {
    const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    #[allow(clippy::cast_possible_truncation)]
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % (bound as u64)) as usize
    }
}

// ─── PRAGMA helpers ─────────────────────────────────────────────────────

fn apply_pragmas_csqlite(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "PRAGMA page_size = 4096;\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
    )
    .expect("apply C SQLite benchmark PRAGMAs");
}

fn apply_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        fsqlite_e2e::block_on(conn.execute(pragma)).unwrap_or_else(|error| {
            panic!("failed to execute FrankenSQLite benchmark PRAGMA `{pragma}`: {error:?}")
        });
    }
}

const CREATE_TABLE: &str = "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);";

fn fsqlite_integer(row: &fsqlite::Row, index: usize, context: &str) -> i64 {
    match row.get(index) {
        Some(SqliteValue::Integer(value)) => *value,
        value => panic!("{context} column {index} was not an integer: {value:?}"),
    }
}

fn expected_post_workload_aggregate() -> (i64, i64) {
    #[allow(clippy::cast_possible_wrap)]
    let mut scores: BTreeMap<i64, i64> = (1..=SEED_ROWS as i64).map(|id| (id, id * 7)).collect();
    let mut rng = Rng64::new(42);
    #[allow(clippy::cast_possible_wrap)]
    let mut next_id = SEED_ROWS as i64 + 1;

    #[allow(clippy::cast_possible_wrap)]
    for _ in 0..OPS_PER_ITERATION {
        let roll = rng.next_usize(100);
        if roll < 40 {
            rng.next_usize(SEED_ROWS);
        } else if roll < 60 {
            rng.next_usize(SEED_ROWS - 50);
        } else if roll < 80 {
            // Aggregate reads do not consume another random value.
        } else if roll < 95 {
            scores.insert(next_id, next_id * 7);
            next_id += 1;
        } else if roll < 98 {
            let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
            if let Some(score) = scores.get_mut(&id) {
                *score = id * 99;
            }
        } else {
            let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
            scores.remove(&id);
        }
    }

    (
        i64::try_from(scores.len()).unwrap(),
        scores.values().copied().sum(),
    )
}

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── Setup helpers ──────────────────────────────────────────────────────

fn setup_csqlite() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    apply_pragmas_csqlite(&conn);
    conn.execute_batch(CREATE_TABLE).unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
            .unwrap();
        #[allow(clippy::cast_possible_wrap)]
        for i in 1..=SEED_ROWS as i64 {
            stmt.execute(rusqlite::params![i]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

fn setup_fsqlite() -> fsqlite::Connection {
    let conn =
        fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).expect("open FrankenSQLite");
    apply_pragmas_fsqlite(&conn);
    fsqlite_e2e::block_on(conn.execute(CREATE_TABLE))
        .expect("create FrankenSQLite benchmark table");
    fsqlite_e2e::block_on(conn.execute("BEGIN")).expect("begin FrankenSQLite seed transaction");
    for i in 1..=SEED_ROWS {
        fsqlite_e2e::block_on(conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, 'name_{i}', {})",
            i * 7,
        )))
        .expect("insert FrankenSQLite seed row");
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT")).expect("commit FrankenSQLite seed transaction");
    conn
}

// ─── C SQLite mixed OLTP ────────────────────────────────────────────────

#[allow(clippy::cast_possible_wrap)]
// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_mixed_oltp_csqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_oltp_80r_20w");
    let expected_aggregate = expected_post_workload_aggregate();
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.throughput(Throughput::Elements(OPS_PER_ITERATION));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            setup_csqlite,
            |conn| {
                let mut rng = Rng64::new(42);
                let mut next_id = SEED_ROWS as i64 + 1;
                let mut expected_row_count = SEED_ROWS as i64;

                let mut select_pt = conn.prepare("SELECT * FROM bench WHERE id = ?1").unwrap();
                let mut select_range = conn
                    .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
                    .unwrap();
                let mut select_agg = conn
                    .prepare("SELECT COUNT(*), SUM(score) FROM bench")
                    .unwrap();
                let mut insert = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
                    .unwrap();
                let mut update = conn
                    .prepare("UPDATE bench SET score = ?2 WHERE id = ?1")
                    .unwrap();
                let mut delete = conn.prepare("DELETE FROM bench WHERE id = ?1").unwrap();

                for _ in 0..OPS_PER_ITERATION {
                    let roll = rng.next_usize(100);
                    if roll < 40 {
                        let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                        let row = select_pt
                            .query_row(rusqlite::params![id], |row| {
                                Ok((
                                    row.get::<_, i64>(0)?,
                                    row.get::<_, String>(1)?,
                                    row.get::<_, i64>(2)?,
                                ))
                            })
                            .optional()
                            .expect("C SQLite point lookup");
                        drop(black_box(row));
                    } else if roll < 60 {
                        let start = (rng.next_usize(SEED_ROWS - 50) + 1) as i64;
                        let count: i64 = select_range
                            .query_row(rusqlite::params![start, start + 50], |r| r.get(0))
                            .unwrap();
                        black_box(count);
                    } else if roll < 80 {
                        let aggregate: (i64, i64) = select_agg
                            .query_row([], |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())))
                            .unwrap();
                        black_box(aggregate);
                    } else if roll < 95 {
                        let inserted = insert.execute(rusqlite::params![next_id]).unwrap();
                        assert_eq!(inserted, 1, "C SQLite INSERT affected-row count");
                        expected_row_count += i64::try_from(inserted).unwrap();
                        next_id += 1;
                    } else if roll < 98 {
                        let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                        let updated = update.execute(rusqlite::params![id, id * 99]).unwrap();
                        assert!(updated <= 1, "C SQLite UPDATE affected {updated} rows");
                    } else {
                        let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                        let deleted = delete.execute(rusqlite::params![id]).unwrap();
                        assert!(deleted <= 1, "C SQLite DELETE affected {deleted} rows");
                        expected_row_count -= i64::try_from(deleted).unwrap();
                    }
                }

                let aggregate: (i64, i64) = select_agg
                    .query_row([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
                    .expect("C SQLite post-workload aggregate");
                assert_eq!(
                    aggregate.0, expected_row_count,
                    "C SQLite post-workload row count"
                );
                assert_eq!(
                    aggregate, expected_aggregate,
                    "C SQLite post-workload aggregate"
                );
                black_box(aggregate);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── FrankenSQLite mixed OLTP ───────────────────────────────────────────

#[allow(clippy::cast_possible_wrap)]
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_mixed_oltp_fsqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_oltp_80r_20w");
    let expected_aggregate = expected_post_workload_aggregate();
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.throughput(Throughput::Elements(OPS_PER_ITERATION));

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            setup_fsqlite,
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let mut rng = Rng64::new(42);
                    let mut next_id = SEED_ROWS as i64 + 1;
                    let mut expected_row_count = SEED_ROWS as i64;
                    let select_pt = conn
                        .prepare("SELECT * FROM bench WHERE id = ?1")
                        .await
                        .expect("prepare FrankenSQLite point lookup");
                    let select_range = conn
                        .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
                        .await
                        .expect("prepare FrankenSQLite range lookup");
                    let select_agg = conn
                        .prepare("SELECT COUNT(*), SUM(score) FROM bench")
                        .await
                        .expect("prepare FrankenSQLite aggregate");
                    let insert = conn
                        .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
                        .await
                        .expect("prepare FrankenSQLite INSERT");
                    let update = conn
                        .prepare("UPDATE bench SET score = ?2 WHERE id = ?1")
                        .await
                        .expect("prepare FrankenSQLite UPDATE");
                    let delete = conn
                        .prepare("DELETE FROM bench WHERE id = ?1")
                        .await
                        .expect("prepare FrankenSQLite DELETE");

                    for _ in 0..OPS_PER_ITERATION {
                        let roll = rng.next_usize(100);
                        if roll < 40 {
                            let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                            let row = match select_pt
                                .query_row_with_params(&[SqliteValue::Integer(id)])
                                .await
                            {
                                Ok(row) => Some(row),
                                Err(FrankenError::QueryReturnedNoRows) => None,
                                Err(error) => {
                                    panic!("FrankenSQLite point lookup failed: {error:?}")
                                }
                            };
                            drop(black_box(row));
                        } else if roll < 60 {
                            let start = (rng.next_usize(SEED_ROWS - 50) + 1) as i64;
                            let row = select_range
                                .query_row_with_params(&[
                                    SqliteValue::Integer(start),
                                    SqliteValue::Integer(start + 50),
                                ])
                                .await
                                .expect("FrankenSQLite range lookup");
                            black_box(fsqlite_integer(&row, 0, "FrankenSQLite range lookup"));
                        } else if roll < 80 {
                            let row = select_agg
                                .query_row()
                                .await
                                .expect("FrankenSQLite aggregate");
                            black_box((
                                fsqlite_integer(&row, 0, "FrankenSQLite aggregate"),
                                fsqlite_integer(&row, 1, "FrankenSQLite aggregate"),
                            ));
                        } else if roll < 95 {
                            let inserted = insert
                                .execute_with_params(&[SqliteValue::Integer(next_id)])
                                .await
                                .expect("FrankenSQLite INSERT");
                            assert_eq!(inserted, 1, "FrankenSQLite INSERT affected-row count");
                            expected_row_count += i64::try_from(inserted).unwrap();
                            next_id += 1;
                        } else if roll < 98 {
                            let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                            let updated = update
                                .execute_with_params(&[
                                    SqliteValue::Integer(id),
                                    SqliteValue::Integer(id * 99),
                                ])
                                .await
                                .expect("FrankenSQLite UPDATE");
                            assert!(updated <= 1, "FrankenSQLite UPDATE affected {updated} rows");
                        } else {
                            let id = (rng.next_usize(SEED_ROWS) + 1) as i64;
                            let deleted = delete
                                .execute_with_params(&[SqliteValue::Integer(id)])
                                .await
                                .expect("FrankenSQLite DELETE");
                            assert!(deleted <= 1, "FrankenSQLite DELETE affected {deleted} rows");
                            expected_row_count -= i64::try_from(deleted).unwrap();
                        }
                    }

                    let row = select_agg
                        .query_row()
                        .await
                        .expect("FrankenSQLite post-workload aggregate");
                    let aggregate = (
                        fsqlite_integer(&row, 0, "FrankenSQLite post-workload aggregate"),
                        fsqlite_integer(&row, 1, "FrankenSQLite post-workload aggregate"),
                    );
                    assert_eq!(
                        aggregate.0, expected_row_count,
                        "FrankenSQLite post-workload row count"
                    );
                    assert_eq!(
                        aggregate, expected_aggregate,
                        "FrankenSQLite post-workload aggregate"
                    );
                    black_box(aggregate);
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(
    name = mixed_oltp;
    config = criterion_config();
    targets = bench_mixed_oltp_csqlite, bench_mixed_oltp_fsqlite
);
criterion_main!(mixed_oltp);
