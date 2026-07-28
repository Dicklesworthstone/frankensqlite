//! Benchmark: all 9 primary operations for baseline capture (bd-1lsfu.1).
//!
//! Measures latency and throughput for the 9 canonical database operations:
//! 1. Sequential scan (full table)
//! 2. Point lookup (WHERE id = ?)
//! 3. Range scan (WHERE id BETWEEN ? AND ?)
//! 4. Single-row insert
//! 5. Batch insert (1000 rows in one transaction)
//! 6. Single-row update
//! 7. Single-row delete
//! 8. 2-way equi-join
//! 9. Aggregation (COUNT/SUM/AVG)
//!
//! Both FrankenSQLite and C SQLite are benchmarked with identical PRAGMA
//! settings for fair comparison.

// bd-mnlk2 / bd-zavyn: the hoisted timed bodies await fsqlite-core's
// deliberately large, deeply nested engine futures inside one runtime entry
// per sample; boxing them would put an allocation inside the timed window.
#![allow(clippy::large_futures)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_types::value::SqliteValue;

const SEED_ROWS: i64 = 1000;

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

// ─── Setup helpers ──────────────────────────────────────────────────────

fn setup_csqlite_seeded() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    apply_pragmas_csqlite(&conn);
    conn.execute_batch(
        "CREATE TABLE bench (\
             id INTEGER PRIMARY KEY,\
             name TEXT NOT NULL,\
             category TEXT NOT NULL,\
             score INTEGER NOT NULL\
         );",
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare(
                "INSERT INTO bench VALUES (?1, ('name_' || ?1), ('cat_' || (?1 % 10)), (?1 * 7))",
            )
            .unwrap();
        for i in 1..=SEED_ROWS {
            stmt.execute(rusqlite::params![i]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

fn setup_csqlite_with_join_table() -> rusqlite::Connection {
    let conn = setup_csqlite_seeded();
    conn.execute_batch(
        "CREATE TABLE bench2 (\
             id INTEGER PRIMARY KEY,\
             bench_id INTEGER NOT NULL,\
             label TEXT NOT NULL\
         );",
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO bench2 VALUES (?1, ?2, ('label_' || ?1))")
            .unwrap();
        for i in 1..=500_i64 {
            // Join table has 500 rows matching a subset of bench.
            stmt.execute(rusqlite::params![i, i * 2]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

fn setup_fsqlite_seeded() -> fsqlite::Connection {
    let conn =
        fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).expect("open FrankenSQLite");
    apply_pragmas_fsqlite(&conn);
    fsqlite_e2e::block_on(conn.execute(
        "CREATE TABLE bench (\
             id INTEGER PRIMARY KEY,\
             name TEXT NOT NULL,\
             category TEXT NOT NULL,\
             score INTEGER NOT NULL\
         )",
    ))
    .expect("create FrankenSQLite benchmark table");
    fsqlite_e2e::block_on(conn.execute("BEGIN")).expect("begin FrankenSQLite seed transaction");
    for i in 1..=SEED_ROWS {
        let inserted = fsqlite_e2e::block_on(conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, 'name_{i}', 'cat_{}', {})",
            i % 10,
            i * 7,
        )))
        .expect("insert FrankenSQLite seed row");
        assert_eq!(inserted, 1, "FrankenSQLite seed INSERT affected-row count");
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT")).expect("commit FrankenSQLite seed transaction");
    conn
}

fn setup_fsqlite_with_join_table() -> fsqlite::Connection {
    let conn = setup_fsqlite_seeded();
    fsqlite_e2e::block_on(conn.execute(
        "CREATE TABLE bench2 (\
             id INTEGER PRIMARY KEY,\
             bench_id INTEGER NOT NULL,\
             label TEXT NOT NULL\
         )",
    ))
    .expect("create FrankenSQLite join table");
    fsqlite_e2e::block_on(conn.execute("BEGIN"))
        .expect("begin FrankenSQLite join-table seed transaction");
    for i in 1..=500_i64 {
        let inserted = fsqlite_e2e::block_on(conn.execute(&format!(
            "INSERT INTO bench2 VALUES ({i}, {}, 'label_{i}')",
            i * 2,
        )))
        .expect("insert FrankenSQLite join-table seed row");
        assert_eq!(
            inserted, 1,
            "FrankenSQLite join-table seed INSERT affected-row count"
        );
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT"))
        .expect("commit FrankenSQLite join-table seed transaction");
    conn
}

// ─── Criterion config ───────────────────────────────────────────────────

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── 1. Sequential scan ────────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_sequential_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_sequential_scan");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(SEED_ROWS as u64));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_seeded();
        let mut stmt = conn.prepare("SELECT * FROM bench").unwrap();
        b.iter(|| {
            let rows: Vec<(i64, String, String, i64)> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get(0).unwrap(),
                        row.get(1).unwrap(),
                        row.get(2).unwrap(),
                        row.get(3).unwrap(),
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(
                i64::try_from(rows.len()).expect("row count must fit i64"),
                SEED_ROWS
            );
            black_box(rows);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        let stmt = fsqlite_e2e::block_on(conn.prepare("SELECT * FROM bench"))
            .expect("prepare FrankenSQLite sequential scan");
        b.iter(|| {
            let rows =
                fsqlite_e2e::block_on(stmt.query()).expect("execute FrankenSQLite sequential scan");
            assert_eq!(
                i64::try_from(rows.len()).expect("row count must fit i64"),
                SEED_ROWS
            );
            black_box(rows);
        });
    });

    group.finish();
}

// ─── 2. Point lookup ───────────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_point_lookup");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_seeded();
        let mut stmt = conn.prepare("SELECT * FROM bench WHERE id = ?1").unwrap();
        let mut id = 1_i64;
        b.iter(|| {
            let row: (i64, String, String, i64) = stmt
                .query_row(rusqlite::params![id], |row| {
                    Ok((
                        row.get(0).unwrap(),
                        row.get(1).unwrap(),
                        row.get(2).unwrap(),
                        row.get(3).unwrap(),
                    ))
                })
                .unwrap();
            assert_eq!(row.0, id);
            black_box(row);
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        let stmt = fsqlite_e2e::block_on(conn.prepare("SELECT * FROM bench WHERE id = ?1"))
            .expect("prepare FrankenSQLite point lookup");
        let mut id = 1_i64;
        b.iter(|| {
            let row =
                fsqlite_e2e::block_on(stmt.query_row_with_params(&[SqliteValue::Integer(id)]))
                    .expect("execute FrankenSQLite point lookup");
            assert_eq!(row.values()[0], SqliteValue::Integer(id));
            black_box(row);
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.finish();
}

// ─── 3. Range scan ─────────────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_range_scan_100");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(100));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_seeded();
        let mut stmt = conn
            .prepare("SELECT * FROM bench WHERE id >= ?1 AND id < ?2")
            .unwrap();
        b.iter(|| {
            let rows: Vec<(i64, String, String, i64)> = stmt
                .query_map(rusqlite::params![100, 200], |row| {
                    Ok((
                        row.get::<_, i64>(0).unwrap(),
                        row.get::<_, String>(1).unwrap(),
                        row.get::<_, String>(2).unwrap(),
                        row.get::<_, i64>(3).unwrap(),
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 100);
            black_box(rows);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        let stmt =
            fsqlite_e2e::block_on(conn.prepare("SELECT * FROM bench WHERE id >= ?1 AND id < ?2"))
                .expect("prepare FrankenSQLite range scan");
        b.iter(|| {
            let rows = fsqlite_e2e::block_on(
                stmt.query_with_params(&[SqliteValue::Integer(100), SqliteValue::Integer(200)]),
            )
            .expect("execute FrankenSQLite range scan");
            assert_eq!(rows.len(), 100);
            black_box(rows);
        });
    });

    group.finish();
}

// ─── 4. Single-row insert ──────────────────────────────────────────────
//
// This is intentionally a cold operation benchmark. `iter_batched` excludes
// setup from the timed body, but each timed iteration still performs a
// one-shot INSERT, validates via COUNT(*), and then drops the freshly prepared
// connection state. Use this group for end-to-end "one row on a fresh
// database" latency, not for the steady-state prepared INSERT hot path.

// BENCH-META: engine=csqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
fn bench_single_row_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_single_row_insert");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
                )
                .unwrap();
                conn
            },
            |conn| {
                let inserted = conn
                    .execute("INSERT INTO bench VALUES (1, 'test_name', 42)", [])
                    .unwrap();
                assert_eq!(inserted, 1, "C SQLite INSERT affected-row count");
                let count: i64 = conn
                    .prepare("SELECT COUNT(*) FROM bench")
                    .unwrap()
                    .query_row([], |r| r.get(0))
                    .unwrap();
                assert_eq!(count, 1);
                black_box(count);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
                    .expect("open FrankenSQLite");
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
                ))
                .expect("create FrankenSQLite single-INSERT table");
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let inserted = conn
                        .execute("INSERT INTO bench VALUES (1, 'test_name', 42)")
                        .await
                        .expect("execute FrankenSQLite single INSERT");
                    assert_eq!(inserted, 1, "FrankenSQLite INSERT affected-row count");
                    let stmt = conn
                        .prepare("SELECT COUNT(*) FROM bench")
                        .await
                        .expect("prepare FrankenSQLite post-INSERT count");
                    let row = stmt
                        .query_row()
                        .await
                        .expect("query FrankenSQLite post-INSERT count");
                    assert_eq!(row.values()[0], SqliteValue::Integer(1));
                    black_box(row);
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ─── 5. Batch insert (1000 rows in one transaction) ────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_batch_insert_1000");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));
    group.throughput(Throughput::Elements(1000));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
                    .unwrap();
                for i in 1..=1000_i64 {
                    let inserted = stmt.execute(rusqlite::params![i]).unwrap();
                    assert_eq!(inserted, 1, "C SQLite batch INSERT affected-row count");
                }
                conn.execute_batch("COMMIT").unwrap();
                let count: i64 = conn
                    .prepare("SELECT COUNT(*) FROM bench")
                    .unwrap()
                    .query_row([], |r| r.get(0))
                    .unwrap();
                assert_eq!(count, 1000);
                black_box(count);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
                    .expect("open FrankenSQLite");
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
                ))
                .expect("create FrankenSQLite batch-INSERT table");
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN")
                        .await
                        .expect("begin FrankenSQLite batch-INSERT transaction");
                    let stmt = conn
                        .prepare("INSERT INTO bench VALUES (?1, ('name_' || ?1), (?1 * 7))")
                        .await
                        .expect("prepare FrankenSQLite batch INSERT");
                    for i in 1..=1000_i64 {
                        let inserted = stmt
                            .execute_with_params(&[SqliteValue::Integer(i)])
                            .await
                            .expect("execute FrankenSQLite batch INSERT");
                        assert_eq!(inserted, 1, "FrankenSQLite batch INSERT affected-row count");
                    }
                    conn.execute("COMMIT")
                        .await
                        .expect("commit FrankenSQLite batch-INSERT transaction");
                    let count_stmt = conn
                        .prepare("SELECT COUNT(*) FROM bench")
                        .await
                        .expect("prepare FrankenSQLite post-batch count");
                    let row = count_stmt
                        .query_row()
                        .await
                        .expect("query FrankenSQLite post-batch count");
                    assert_eq!(row.values()[0], SqliteValue::Integer(1000));
                    black_box(row);
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── 6. Single-row update ──────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_single_row_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_single_row_update");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_seeded();
        let mut stmt = conn
            .prepare("UPDATE bench SET score = ?1 WHERE id = ?2")
            .unwrap();
        let mut id = 1_i64;
        b.iter(|| {
            let updated = stmt.execute(rusqlite::params![id * 13, id]).unwrap();
            assert_eq!(updated, 1, "C SQLite UPDATE affected-row count");
            black_box(updated);
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        let stmt = fsqlite_e2e::block_on(conn.prepare("UPDATE bench SET score = ?1 WHERE id = ?2"))
            .expect("prepare FrankenSQLite UPDATE");
        let mut id = 1_i64;
        b.iter(|| {
            let updated =
                fsqlite_e2e::block_on(stmt.execute_with_params(&[
                    SqliteValue::Integer(id * 13),
                    SqliteValue::Integer(id),
                ]))
                .expect("execute FrankenSQLite UPDATE");
            assert_eq!(updated, 1, "FrankenSQLite UPDATE affected-row count");
            black_box(updated);
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.finish();
}

// ─── 7. Single-row delete ──────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
fn bench_single_row_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_single_row_delete");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    // Each iteration needs a fresh table since we delete from it.
    group.bench_function("csqlite", |b| {
        b.iter_batched(
            setup_csqlite_seeded,
            |conn| {
                let deleted = conn
                    .execute("DELETE FROM bench WHERE id = 500", [])
                    .unwrap();
                assert_eq!(deleted, 1, "C SQLite DELETE affected-row count");
                let count: i64 = conn
                    .prepare("SELECT COUNT(*) FROM bench")
                    .unwrap()
                    .query_row([], |r| r.get(0))
                    .unwrap();
                assert_eq!(count, SEED_ROWS - 1);
                black_box(count);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            setup_fsqlite_seeded,
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let deleted = conn
                        .execute("DELETE FROM bench WHERE id = 500")
                        .await
                        .expect("execute FrankenSQLite DELETE");
                    assert_eq!(deleted, 1, "FrankenSQLite DELETE affected-row count");
                    let stmt = conn
                        .prepare("SELECT COUNT(*) FROM bench")
                        .await
                        .expect("prepare FrankenSQLite post-DELETE count");
                    let row = stmt
                        .query_row()
                        .await
                        .expect("query FrankenSQLite post-DELETE count");
                    assert_eq!(row.values()[0], SqliteValue::Integer(SEED_ROWS - 1));
                    black_box(row);
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── 8. 2-way equi-join ───────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_two_way_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_two_way_equi_join");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_with_join_table();
        let mut stmt = conn
            .prepare(
                "SELECT bench.id, bench.name, bench2.label \
                 FROM bench INNER JOIN bench2 ON bench.id = bench2.bench_id",
            )
            .unwrap();
        b.iter(|| {
            let rows: Vec<(i64, String, String)> = stmt
                .query_map([], |row| {
                    Ok((
                        row.get(0).unwrap(),
                        row.get(1).unwrap(),
                        row.get(2).unwrap(),
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 500);
            black_box(rows);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_with_join_table();
        let stmt = fsqlite_e2e::block_on(conn.prepare(
            "SELECT bench.id, bench.name, bench2.label \
                 FROM bench INNER JOIN bench2 ON bench.id = bench2.bench_id",
        ))
        .expect("prepare FrankenSQLite two-way join");
        b.iter(|| {
            let rows =
                fsqlite_e2e::block_on(stmt.query()).expect("execute FrankenSQLite two-way join");
            assert_eq!(rows.len(), 500);
            black_box(rows);
        });
    });

    group.finish();
}

// ─── 9. Aggregation (COUNT/SUM/AVG) ────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_aggregation");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_seeded();
        let mut stmt = conn
            .prepare("SELECT COUNT(*), SUM(score), AVG(score) FROM bench")
            .unwrap();
        b.iter(|| {
            let (count, sum, avg): (i64, i64, f64) = stmt
                .query_row([], |r| {
                    Ok((r.get(0).unwrap(), r.get(1).unwrap(), r.get(2).unwrap()))
                })
                .unwrap();
            assert_eq!(count, SEED_ROWS);
            assert_eq!(sum, 3_503_500);
            assert_eq!(avg, 3_503.5);
            black_box((count, sum, avg));
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        let stmt = fsqlite_e2e::block_on(
            conn.prepare("SELECT COUNT(*), SUM(score), AVG(score) FROM bench"),
        )
        .expect("prepare FrankenSQLite aggregation");
        b.iter(|| {
            let row =
                fsqlite_e2e::block_on(stmt.query_row()).expect("execute FrankenSQLite aggregation");
            let vals = row.values();
            assert_eq!(vals[0], SqliteValue::Integer(SEED_ROWS));
            assert_eq!(vals[1], SqliteValue::Integer(3_503_500));
            assert_eq!(vals[2], SqliteValue::Float(3_503.5));
            black_box(row);
        });
    });

    group.finish();
}

// ─── bd-wwqen.3: Column-list vs no-column-list INSERT ───────────────────
//
// Documents the performance difference between:
// - `INSERT INTO t VALUES (?, ?, ?)` → direct insert fast path
// - `INSERT INTO t(a, b, c) VALUES (?, ?, ?)` → VDBE path (current behavior)
//
// The fix (tracked in bd-wwqen.3) should eliminate this gap by supporting
// column reordering in the direct insert path.

// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_column_list_insert_prepared(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_column_list_insert_prepared_1000");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));
    group.throughput(Throughput::Elements(1000));

    // FrankenSQLite: No column list (direct path)
    group.bench_function("frankensqlite_no_col_list", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
                    .expect("open FrankenSQLite");
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
                ))
                .expect("create FrankenSQLite no-column-list table");
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN")
                        .await
                        .expect("begin FrankenSQLite no-column-list transaction");
                    // No column list → should hit direct insert path
                    let stmt = conn
                        .prepare("INSERT INTO bench VALUES (?1, ?2, ?3)")
                        .await
                        .expect("prepare FrankenSQLite no-column-list INSERT");
                    for i in 1..=1000_i64 {
                        let inserted = stmt
                            .execute_with_params(&[
                                SqliteValue::Integer(i),
                                SqliteValue::Text(format!("name_{i}").into()),
                                SqliteValue::Integer(i * 7),
                            ])
                            .await
                            .expect("execute FrankenSQLite no-column-list INSERT");
                        assert_eq!(
                            inserted, 1,
                            "FrankenSQLite no-column-list INSERT affected-row count"
                        );
                    }
                    conn.execute("COMMIT")
                        .await
                        .expect("commit FrankenSQLite no-column-list transaction");
                });
            },
            BatchSize::LargeInput,
        );
    });

    // FrankenSQLite: With column list, same order (currently VDBE path)
    group.bench_function("frankensqlite_col_list_same_order", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
                    .expect("open FrankenSQLite");
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
                ))
                .expect("create FrankenSQLite same-order column-list table");
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN")
                        .await
                        .expect("begin FrankenSQLite same-order column-list transaction");
                    // Column list in same order → currently VDBE path, should be direct after fix
                    let stmt = conn
                        .prepare("INSERT INTO bench(id, name, score) VALUES (?1, ?2, ?3)")
                        .await
                        .expect("prepare FrankenSQLite same-order column-list INSERT");
                    for i in 1..=1000_i64 {
                        let inserted = stmt
                            .execute_with_params(&[
                                SqliteValue::Integer(i),
                                SqliteValue::Text(format!("name_{i}").into()),
                                SqliteValue::Integer(i * 7),
                            ])
                            .await
                            .expect("execute FrankenSQLite same-order column-list INSERT");
                        assert_eq!(
                            inserted, 1,
                            "FrankenSQLite same-order column-list INSERT affected-row count"
                        );
                    }
                    conn.execute("COMMIT")
                        .await
                        .expect("commit FrankenSQLite same-order column-list transaction");
                });
            },
            BatchSize::LargeInput,
        );
    });

    // FrankenSQLite: With column list, different order (reordering needed)
    group.bench_function("frankensqlite_col_list_diff_order", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
                    .expect("open FrankenSQLite");
                apply_pragmas_fsqlite(&conn);
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
                ))
                .expect("create FrankenSQLite reordered column-list table");
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN")
                        .await
                        .expect("begin FrankenSQLite reordered column-list transaction");
                    // Column list in different order → requires reordering
                    let stmt = conn
                        .prepare("INSERT INTO bench(score, name, id) VALUES (?1, ?2, ?3)")
                        .await
                        .expect("prepare FrankenSQLite reordered column-list INSERT");
                    for i in 1..=1000_i64 {
                        let inserted = stmt
                            .execute_with_params(&[
                                SqliteValue::Integer(i * 7),                   // score
                                SqliteValue::Text(format!("name_{i}").into()), // name
                                SqliteValue::Integer(i),                       // id
                            ])
                            .await
                            .expect("execute FrankenSQLite reordered column-list INSERT");
                        assert_eq!(
                            inserted, 1,
                            "FrankenSQLite reordered column-list INSERT affected-row count"
                        );
                    }
                    conn.execute("COMMIT")
                        .await
                        .expect("commit FrankenSQLite reordered column-list transaction");
                });
            },
            BatchSize::LargeInput,
        );
    });

    // C SQLite baselines for comparison
    group.bench_function("csqlite_no_col_list", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ?2, ?3)")
                    .unwrap();
                for i in 1..=1000_i64 {
                    let inserted = stmt
                        .execute(rusqlite::params![i, format!("name_{i}"), i * 7])
                        .unwrap();
                    assert_eq!(
                        inserted, 1,
                        "C SQLite no-column-list INSERT affected-row count"
                    );
                }
                conn.execute_batch("COMMIT").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("csqlite_col_list_same_order", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                apply_pragmas_csqlite(&conn);
                conn.execute_batch(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn
                    .prepare("INSERT INTO bench(id, name, score) VALUES (?1, ?2, ?3)")
                    .unwrap();
                for i in 1..=1000_i64 {
                    let inserted = stmt
                        .execute(rusqlite::params![i, format!("name_{i}"), i * 7])
                        .unwrap();
                    assert_eq!(
                        inserted, 1,
                        "C SQLite same-order column-list INSERT affected-row count"
                    );
                }
                conn.execute_batch("COMMIT").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(
    name = operation_baselines;
    config = criterion_config();
    targets =
        bench_sequential_scan,
        bench_point_lookup,
        bench_range_scan,
        bench_single_row_insert,
        bench_batch_insert,
        bench_single_row_update,
        bench_single_row_delete,
        bench_two_way_join,
        bench_aggregation,
        bench_column_list_insert_prepared
);
criterion_main!(operation_baselines);
