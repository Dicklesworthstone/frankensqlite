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
    .ok();
}

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
    let conn = fsqlite::Connection::open(":memory:").unwrap();
    apply_pragmas_fsqlite(&conn);
    conn.execute(
        "CREATE TABLE bench (\
             id INTEGER PRIMARY KEY,\
             name TEXT NOT NULL,\
             category TEXT NOT NULL,\
             score INTEGER NOT NULL\
         )",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=SEED_ROWS {
        conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, 'name_{i}', 'cat_{}', {})",
            i % 10,
            i * 7,
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn
}

fn setup_fsqlite_with_join_table() -> fsqlite::Connection {
    let conn = setup_fsqlite_seeded();
    conn.execute(
        "CREATE TABLE bench2 (\
             id INTEGER PRIMARY KEY,\
             bench_id INTEGER NOT NULL,\
             label TEXT NOT NULL\
         )",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=500_i64 {
        conn.execute(&format!(
            "INSERT INTO bench2 VALUES ({i}, {}, 'label_{i}')",
            i * 2,
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn
}

// ─── Criterion config ───────────────────────────────────────────────────

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── 1. Sequential scan ────────────────────────────────────────────────

fn bench_sequential_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_sequential_scan");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(SEED_ROWS as u64));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_seeded();
        let mut stmt = conn.prepare("SELECT * FROM bench").unwrap();
        b.iter(|| {
            let rows: Vec<(i64,)> = stmt
                .query_map([], |row| Ok((row.get(0).unwrap(),)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len() as i64, SEED_ROWS);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        b.iter(|| {
            let rows = conn.query("SELECT * FROM bench").unwrap();
            assert_eq!(rows.len() as i64, SEED_ROWS);
        });
    });

    group.finish();
}

// ─── 2. Point lookup ───────────────────────────────────────────────────

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
            let rows: Vec<(i64,)> = stmt
                .query_map(rusqlite::params![id], |row| Ok((row.get(0).unwrap(),)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 1);
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        let mut id = 1_i64;
        b.iter(|| {
            let rows = conn
                .query(&format!("SELECT * FROM bench WHERE id = {id}"))
                .unwrap();
            assert_eq!(rows.len(), 1);
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.finish();
}

// ─── 3. Range scan ─────────────────────────────────────────────────────

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
            let rows: Vec<(i64,)> = stmt
                .query_map(rusqlite::params![100, 200], |row| {
                    Ok((row.get(0).unwrap(),))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 100);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        b.iter(|| {
            let rows = conn
                .query("SELECT * FROM bench WHERE id >= 100 AND id < 200")
                .unwrap();
            assert_eq!(rows.len(), 100);
        });
    });

    group.finish();
}

// ─── 4. Single-row insert ──────────────────────────────────────────────

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
                conn.execute("INSERT INTO bench VALUES (1, 'test_name', 42)", [])
                    .unwrap();
                let count: i64 = conn
                    .prepare("SELECT COUNT(*) FROM bench")
                    .unwrap()
                    .query_row([], |r| r.get(0))
                    .unwrap();
                assert_eq!(count, 1);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute("INSERT INTO bench VALUES (1, 'test_name', 42)")
                    .unwrap();
                let rows = conn.query("SELECT COUNT(*) FROM bench").unwrap();
                assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ─── 5. Batch insert (1000 rows in one transaction) ────────────────────

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
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT").unwrap();
                let count: i64 = conn
                    .prepare("SELECT COUNT(*) FROM bench")
                    .unwrap()
                    .query_row([], |r| r.get(0))
                    .unwrap();
                assert_eq!(count, 1000);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                apply_pragmas_fsqlite(&conn);
                conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute("BEGIN").unwrap();
                for i in 1..=1000_i64 {
                    conn.execute(&format!(
                        "INSERT INTO bench VALUES ({i}, 'name_{i}', {})",
                        i * 7,
                    ))
                    .unwrap();
                }
                conn.execute("COMMIT").unwrap();
                let rows = conn.query("SELECT COUNT(*) FROM bench").unwrap();
                assert_eq!(rows[0].values()[0], SqliteValue::Integer(1000));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── 6. Single-row update ──────────────────────────────────────────────

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
            stmt.execute(rusqlite::params![id * 13, id]).unwrap();
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        let mut id = 1_i64;
        b.iter(|| {
            conn.execute(&format!(
                "UPDATE bench SET score = {} WHERE id = {id}",
                id * 13,
            ))
            .unwrap();
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.finish();
}

// ─── 7. Single-row delete ──────────────────────────────────────────────

fn bench_single_row_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("op_single_row_delete");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    // Each iteration needs a fresh table since we delete from it.
    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = setup_csqlite_seeded();
                conn
            },
            |conn| {
                conn.execute("DELETE FROM bench WHERE id = 500", [])
                    .unwrap();
                let count: i64 = conn
                    .prepare("SELECT COUNT(*) FROM bench")
                    .unwrap()
                    .query_row([], |r| r.get(0))
                    .unwrap();
                assert_eq!(count, SEED_ROWS - 1);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || setup_fsqlite_seeded(),
            |conn| {
                conn.execute("DELETE FROM bench WHERE id = 500").unwrap();
                let rows = conn.query("SELECT COUNT(*) FROM bench").unwrap();
                assert_eq!(rows[0].values()[0], SqliteValue::Integer(SEED_ROWS - 1));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── 8. 2-way equi-join ───────────────────────────────────────────────

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
            let rows: Vec<(i64,)> = stmt
                .query_map([], |row| Ok((row.get(0).unwrap(),)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(!rows.is_empty());
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_with_join_table();
        b.iter(|| {
            let rows = conn
                .query(
                    "SELECT bench.id, bench.name, bench2.label \
                     FROM bench INNER JOIN bench2 ON bench.id = bench2.bench_id",
                )
                .unwrap();
            assert!(!rows.is_empty());
        });
    });

    group.finish();
}

// ─── 9. Aggregation (COUNT/SUM/AVG) ────────────────────────────────────

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
            assert!(sum > 0);
            assert!(avg > 0.0);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_seeded();
        b.iter(|| {
            let rows = conn
                .query("SELECT COUNT(*), SUM(score), AVG(score) FROM bench")
                .unwrap();
            assert_eq!(rows.len(), 1);
            let vals = rows[0].values();
            // COUNT should equal SEED_ROWS.
            assert_eq!(vals[0], SqliteValue::Integer(SEED_ROWS));
        });
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
        bench_aggregation
);
criterion_main!(operation_baselines);
