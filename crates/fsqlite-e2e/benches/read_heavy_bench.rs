//! Benchmark: read-heavy SELECT workload (WHERE/aggregates/ORDER BY/LIMIT).
//!
//! Bead: bd-72im
//!
//! Measures query performance across multiple SELECT patterns on both
//! FrankenSQLite and C SQLite.  Each benchmark group populates a table with
//! 1,000 rows, then repeatedly executes a specific query pattern.
//!
//! Patterns:
//! 1. Point lookup (`WHERE id = ?`)
//! 2. Range scan (`WHERE id BETWEEN ? AND ?`)
//! 3. Full-table aggregate (`SELECT COUNT(*)`)
//! 4. GROUP BY aggregate
//! 5. ORDER BY + LIMIT

use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
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

fn setup_csqlite() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    apply_pragmas_csqlite(&conn);
    conn.execute_batch(
        "CREATE TABLE bench (\
             id INTEGER PRIMARY KEY,\
             name TEXT,\
             category TEXT,\
             score INTEGER\
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

fn setup_fsqlite() -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(":memory:").unwrap();
    apply_pragmas_fsqlite(&conn);
    conn.execute(
        "CREATE TABLE bench (\
             id INTEGER PRIMARY KEY,\
             name TEXT,\
             category TEXT,\
             score INTEGER\
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

// ─── Criterion config ───────────────────────────────────────────────────

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── 1. Point lookup ────────────────────────────────────────────────────

fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_point_lookup");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite();
        let mut stmt = conn.prepare("SELECT * FROM bench WHERE id = ?1").unwrap();
        let mut id = 1_i64;
        b.iter(|| {
            let rows: Vec<(i64, String)> = stmt
                .query_map(rusqlite::params![id], |row| {
                    Ok((row.get(0).unwrap(), row.get(1).unwrap()))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 1);
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = conn.prepare("SELECT * FROM bench WHERE id = ?1").unwrap();
        let mut id = 1_i64;
        b.iter(|| {
            let _row = stmt
                .query_row_with_params(&[SqliteValue::Integer(id)])
                .unwrap();
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.finish();
}

// ─── 2. Range scan ──────────────────────────────────────────────────────

fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_range_scan_50");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(50));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite();
        let mut stmt = conn
            .prepare("SELECT * FROM bench WHERE id >= ?1 AND id < ?2")
            .unwrap();
        b.iter(|| {
            let rows: Vec<(i64,)> = stmt
                .query_map(rusqlite::params![200, 250], |row| {
                    Ok((row.get(0).unwrap(),))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 50);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = conn
            .prepare("SELECT * FROM bench WHERE id >= ?1 AND id < ?2")
            .unwrap();
        b.iter(|| {
            let rows = stmt
                .query_with_params(&[SqliteValue::Integer(200), SqliteValue::Integer(250)])
                .unwrap();
            assert_eq!(rows.len(), 50);
        });
    });

    group.finish();
}

// ─── 3. Full-table aggregate ────────────────────────────────────────────

fn bench_full_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_count_star");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite();
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
        b.iter(|| {
            let count: i64 = stmt.query_row([], |r| r.get(0)).unwrap();
            assert_eq!(count, SEED_ROWS);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
        b.iter(|| {
            let row = stmt.query_row().unwrap();
            assert_eq!(row.values()[0], SqliteValue::Integer(SEED_ROWS));
        });
    });

    group.finish();
}

// ─── 4. GROUP BY aggregate ──────────────────────────────────────────────

fn bench_group_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_group_by");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite();
        let mut stmt = conn
            .prepare("SELECT category, COUNT(*), SUM(score) FROM bench GROUP BY category")
            .unwrap();
        b.iter(|| {
            let rows: Vec<(String, i64, i64)> = stmt
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
            assert_eq!(rows.len(), 10);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = conn
            .prepare("SELECT category, COUNT(*), SUM(score) FROM bench GROUP BY category")
            .unwrap();
        b.iter(|| {
            let rows = stmt.query().unwrap();
            assert_eq!(rows.len(), 10);
        });
    });

    group.finish();
}

// ─── 5. ORDER BY + LIMIT ───────────────────────────────────────────────

fn bench_order_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_order_limit_10");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(10));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite();
        let mut stmt = conn
            .prepare("SELECT * FROM bench ORDER BY score DESC LIMIT 10")
            .unwrap();
        b.iter(|| {
            let rows: Vec<(i64,)> = stmt
                .query_map([], |row| Ok((row.get(0).unwrap(),)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 10);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = conn
            .prepare("SELECT * FROM bench ORDER BY score DESC LIMIT 10")
            .unwrap();
        b.iter(|| {
            let rows = stmt.query().unwrap();
            assert_eq!(rows.len(), 10);
        });
    });

    group.finish();
}

criterion_group!(
    name = read_heavy;
    config = criterion_config();
    targets =
        bench_point_lookup,
        bench_range_scan,
        bench_full_count,
        bench_group_by,
        bench_order_limit
);
criterion_main!(read_heavy);
