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
//! 6. Correlated `EXISTS` subquery
//! 7. `IN (SELECT ...)` subquery
//! 8. Recursive CTE

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use fsqlite_types::value::SqliteValue;

const SEED_ROWS: i64 = 1000;
const COUNT_SEED_ROWS: i64 = 10_000;
const SUBQUERY_ROWS: i64 = 10_000;
const RECURSIVE_CTE_LIMIT: i64 = 1_000;
const RECURSIVE_CTE_SUM: i64 = 500_500;

type MaterializedBenchRow = (i64, String, String, i64);

fn expected_score_sum(row_count: i64) -> i64 {
    7 * row_count * (row_count + 1) / 2
}

fn fsqlite_integer(row: &fsqlite::Row, index: usize, context: &str) -> i64 {
    match row.get(index) {
        Some(SqliteValue::Integer(value)) => *value,
        value => panic!("{context} column {index} was not an integer: {value:?}"),
    }
}

fn fsqlite_text(row: &fsqlite::Row, index: usize, context: &str) -> String {
    match row.get(index) {
        Some(SqliteValue::Text(value)) => value.as_str().to_owned(),
        value => panic!("{context} column {index} was not text: {value:?}"),
    }
}

fn materialize_fsqlite_bench_row(row: &fsqlite::Row, context: &str) -> MaterializedBenchRow {
    (
        fsqlite_integer(row, 0, context),
        fsqlite_text(row, 1, context),
        fsqlite_text(row, 2, context),
        fsqlite_integer(row, 3, context),
    )
}

// ─── PRAGMA helpers ─────────────────────────────────────────────────────

fn apply_pragmas_csqlite(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "PRAGMA page_size = 4096;\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
    )
    .expect("apply C SQLite read benchmark PRAGMAs");
}

fn apply_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        fsqlite_e2e::block_on(conn.execute(pragma)).unwrap_or_else(|error| {
            panic!("failed to apply FrankenSQLite read benchmark PRAGMA `{pragma}`: {error:?}")
        });
    }
}

// ─── Setup helpers ──────────────────────────────────────────────────────

fn setup_csqlite_with_rows(row_count: i64) -> rusqlite::Connection {
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
        for i in 1..=row_count {
            stmt.execute(rusqlite::params![i]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

fn setup_csqlite() -> rusqlite::Connection {
    setup_csqlite_with_rows(SEED_ROWS)
}

fn setup_fsqlite_with_rows(row_count: i64) -> fsqlite::Connection {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
        .expect("open FrankenSQLite read benchmark database");
    apply_pragmas_fsqlite(&conn);
    fsqlite_e2e::block_on(conn.execute(
        "CREATE TABLE bench (\
             id INTEGER PRIMARY KEY,\
             name TEXT,\
             category TEXT,\
             score INTEGER\
         )",
    ))
    .expect("create FrankenSQLite read benchmark table");
    fsqlite_e2e::block_on(conn.execute("BEGIN"))
        .expect("begin FrankenSQLite read benchmark seed transaction");
    for i in 1..=row_count {
        fsqlite_e2e::block_on(conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, 'name_{i}', 'cat_{}', {})",
            i % 10,
            i * 7,
        )))
        .unwrap_or_else(|error| {
            panic!("insert FrankenSQLite read benchmark seed row {i}: {error:?}")
        });
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT"))
        .expect("commit FrankenSQLite read benchmark seed transaction");
    conn
}

fn setup_fsqlite() -> fsqlite::Connection {
    setup_fsqlite_with_rows(SEED_ROWS)
}

fn setup_csqlite_subquery() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    let category_count = (SUBQUERY_ROWS / 20).max(5);
    apply_pragmas_csqlite(&conn);
    conn.execute_batch(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category_id INTEGER);\
         CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT);",
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut category_stmt = conn
            .prepare("INSERT INTO categories VALUES (?1, ('cat_' || ?1))")
            .unwrap();
        for i in 1..=category_count {
            category_stmt.execute(rusqlite::params![i]).unwrap();
        }
        let mut product_stmt = conn
            .prepare(
                "INSERT INTO products VALUES (?1, ('prod_' || ?1), (?1 * 3.14), ((?1 % ?2) + 1))",
            )
            .unwrap();
        for i in 1..=SUBQUERY_ROWS {
            product_stmt
                .execute(rusqlite::params![i, category_count])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn.execute_batch("CREATE INDEX idx_prod_cat ON products(category_id);")
        .unwrap();
    conn
}

fn setup_fsqlite_subquery() -> fsqlite::Connection {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
        .expect("open FrankenSQLite subquery benchmark database");
    let category_count = (SUBQUERY_ROWS / 20).max(5);
    apply_pragmas_fsqlite(&conn);
    fsqlite_e2e::block_on(conn.execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category_id INTEGER)",
    ))
    .expect("create FrankenSQLite products benchmark table");
    fsqlite_e2e::block_on(
        conn.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)"),
    )
    .expect("create FrankenSQLite categories benchmark table");
    fsqlite_e2e::block_on(conn.execute("BEGIN"))
        .expect("begin FrankenSQLite subquery seed transaction");
    for i in 1..=category_count {
        fsqlite_e2e::block_on(
            conn.execute(&format!("INSERT INTO categories VALUES ({i}, 'cat_{i}')")),
        )
        .unwrap_or_else(|error| panic!("insert FrankenSQLite category seed row {i}: {error:?}"));
    }
    for i in 1..=SUBQUERY_ROWS {
        let category_id = (i % category_count) + 1;
        let price = i as f64 * 3.14;
        fsqlite_e2e::block_on(conn.execute(&format!(
            "INSERT INTO products VALUES ({i}, 'prod_{i}', {price}, {category_id})"
        )))
        .unwrap_or_else(|error| panic!("insert FrankenSQLite product seed row {i}: {error:?}"));
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT"))
        .expect("commit FrankenSQLite subquery seed transaction");
    fsqlite_e2e::block_on(conn.execute("CREATE INDEX idx_prod_cat ON products(category_id)"))
        .expect("create FrankenSQLite product-category index");
    conn
}

// ─── Criterion config ───────────────────────────────────────────────────

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── 1. Point lookup ────────────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
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
            let rows: Vec<MaterializedBenchRow> = stmt
                .query_map(rusqlite::params![id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .expect("query C SQLite point lookup")
                .collect::<Result<Vec<_>, _>>()
                .expect("materialize C SQLite point lookup");
            assert_eq!(rows.len(), 1);
            black_box(rows);
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = fsqlite_e2e::block_on(conn.prepare("SELECT * FROM bench WHERE id = ?1"))
            .expect("prepare FrankenSQLite point lookup");
        let mut id = 1_i64;
        b.iter(|| {
            let row =
                fsqlite_e2e::block_on(stmt.query_row_with_params(&[SqliteValue::Integer(id)]))
                    .expect("query FrankenSQLite point lookup");
            let rows = vec![materialize_fsqlite_bench_row(
                &row,
                "FrankenSQLite point lookup",
            )];
            assert_eq!(rows.len(), 1);
            black_box(rows);
            id = (id % SEED_ROWS) + 1;
        });
    });

    group.finish();
}

// ─── 2. Range scan ──────────────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
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
            let rows: Vec<MaterializedBenchRow> = stmt
                .query_map(rusqlite::params![200, 250], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .expect("query C SQLite range scan")
                .collect::<Result<Vec<_>, _>>()
                .expect("materialize C SQLite range scan");
            assert_eq!(rows.len(), 50);
            black_box(rows);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt =
            fsqlite_e2e::block_on(conn.prepare("SELECT * FROM bench WHERE id >= ?1 AND id < ?2"))
                .expect("prepare FrankenSQLite range scan");
        b.iter(|| {
            let rows = fsqlite_e2e::block_on(
                stmt.query_with_params(&[SqliteValue::Integer(200), SqliteValue::Integer(250)]),
            )
            .expect("query FrankenSQLite range scan")
            .iter()
            .map(|row| materialize_fsqlite_bench_row(row, "FrankenSQLite range scan"))
            .collect::<Vec<_>>();
            assert_eq!(rows.len(), 50);
            black_box(rows);
        });
    });

    group.finish();
}

// ─── 3. Full-table aggregate ────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_full_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_count_star");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite();
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
        b.iter(|| {
            let count: i64 = stmt
                .query_row([], |row| row.get(0))
                .expect("query C SQLite COUNT(*)");
            assert_eq!(count, SEED_ROWS);
            black_box(count);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = fsqlite_e2e::block_on(conn.prepare("SELECT COUNT(*) FROM bench"))
            .expect("prepare FrankenSQLite COUNT(*)");
        b.iter(|| {
            let row =
                fsqlite_e2e::block_on(stmt.query_row()).expect("query FrankenSQLite COUNT(*)");
            let count = fsqlite_integer(&row, 0, "FrankenSQLite COUNT(*)");
            assert_eq!(count, SEED_ROWS);
            black_box(count);
        });
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_full_count_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_count_star_10000");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(15));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_with_rows(COUNT_SEED_ROWS);
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM bench").unwrap();
        b.iter(|| {
            let count: i64 = stmt
                .query_row([], |row| row.get(0))
                .expect("query C SQLite large COUNT(*)");
            assert_eq!(count, COUNT_SEED_ROWS);
            black_box(count);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_with_rows(COUNT_SEED_ROWS);
        let stmt = fsqlite_e2e::block_on(conn.prepare("SELECT COUNT(*) FROM bench"))
            .expect("prepare FrankenSQLite large COUNT(*)");
        b.iter(|| {
            let row = fsqlite_e2e::block_on(stmt.query_row())
                .expect("query FrankenSQLite large COUNT(*)");
            let count = fsqlite_integer(&row, 0, "FrankenSQLite large COUNT(*)");
            assert_eq!(count, COUNT_SEED_ROWS);
            black_box(count);
        });
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_count_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_count_range_50");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(50));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite();
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2")
            .unwrap();
        b.iter(|| {
            let count: i64 = stmt
                .query_row(rusqlite::params![200, 250], |r| r.get(0))
                .expect("query C SQLite range COUNT(*)");
            assert_eq!(count, 50);
            black_box(count);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = fsqlite_e2e::block_on(
            conn.prepare("SELECT COUNT(*) FROM bench WHERE id >= ?1 AND id < ?2"),
        )
        .expect("prepare FrankenSQLite range COUNT(*)");
        b.iter(|| {
            let row =
                fsqlite_e2e::block_on(stmt.query_row_with_params(&[
                    SqliteValue::Integer(200),
                    SqliteValue::Integer(250),
                ]))
                .expect("query FrankenSQLite range COUNT(*)");
            let count = fsqlite_integer(&row, 0, "FrankenSQLite range COUNT(*)");
            assert_eq!(count, 50);
            black_box(count);
        });
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_count_sum_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_count_sum_aggregate");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite();
        let mut stmt = conn
            .prepare("SELECT COUNT(*), SUM(score) FROM bench")
            .unwrap();
        b.iter(|| {
            let (count, sum): (i64, i64) = stmt
                .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
                .expect("query C SQLite COUNT/SUM aggregate");
            assert_eq!(count, SEED_ROWS);
            assert_eq!(sum, expected_score_sum(SEED_ROWS));
            black_box((count, sum));
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = fsqlite_e2e::block_on(conn.prepare("SELECT COUNT(*), SUM(score) FROM bench"))
            .expect("prepare FrankenSQLite COUNT/SUM aggregate");
        b.iter(|| {
            let row = fsqlite_e2e::block_on(stmt.query_row())
                .expect("query FrankenSQLite COUNT/SUM aggregate");
            let aggregate = (
                fsqlite_integer(&row, 0, "FrankenSQLite COUNT/SUM aggregate"),
                fsqlite_integer(&row, 1, "FrankenSQLite COUNT/SUM aggregate"),
            );
            assert_eq!(aggregate.0, SEED_ROWS);
            assert_eq!(aggregate.1, expected_score_sum(SEED_ROWS));
            black_box(aggregate);
        });
    });

    group.finish();
}

// ─── 4. GROUP BY aggregate ──────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
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
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .expect("query C SQLite GROUP BY aggregate")
                .collect::<Result<Vec<_>, _>>()
                .expect("materialize C SQLite GROUP BY aggregate");
            assert_eq!(rows.len(), 10);
            black_box(rows);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt = fsqlite_e2e::block_on(
            conn.prepare("SELECT category, COUNT(*), SUM(score) FROM bench GROUP BY category"),
        )
        .expect("prepare FrankenSQLite GROUP BY aggregate");
        b.iter(|| {
            let rows = fsqlite_e2e::block_on(stmt.query())
                .expect("query FrankenSQLite GROUP BY aggregate")
                .iter()
                .map(|row| {
                    (
                        fsqlite_text(row, 0, "FrankenSQLite GROUP BY aggregate"),
                        fsqlite_integer(row, 1, "FrankenSQLite GROUP BY aggregate"),
                        fsqlite_integer(row, 2, "FrankenSQLite GROUP BY aggregate"),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 10);
            black_box(rows);
        });
    });

    group.finish();
}

// ─── 5. ORDER BY + LIMIT ───────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
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
            let rows: Vec<MaterializedBenchRow> = stmt
                .query_map([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .expect("query C SQLite ORDER BY/LIMIT")
                .collect::<Result<Vec<_>, _>>()
                .expect("materialize C SQLite ORDER BY/LIMIT");
            assert_eq!(rows.len(), 10);
            black_box(rows);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite();
        let stmt =
            fsqlite_e2e::block_on(conn.prepare("SELECT * FROM bench ORDER BY score DESC LIMIT 10"))
                .expect("prepare FrankenSQLite ORDER BY/LIMIT");
        b.iter(|| {
            let rows = fsqlite_e2e::block_on(stmt.query())
                .expect("query FrankenSQLite ORDER BY/LIMIT")
                .iter()
                .map(|row| materialize_fsqlite_bench_row(row, "FrankenSQLite ORDER BY/LIMIT"))
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 10);
            black_box(rows);
        });
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_exists_subquery(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_exists_subquery_count");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    let category_count = (SUBQUERY_ROWS / 20).max(5);
    let half = category_count / 2;
    let expected_count = SUBQUERY_ROWS / 2;
    let sql = format!(
        "SELECT COUNT(*) FROM products p WHERE EXISTS (SELECT 1 FROM categories c WHERE c.id = p.category_id AND c.id <= {half})"
    );

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_subquery();
        let mut stmt = conn.prepare(&sql).unwrap();
        b.iter(|| {
            let count: i64 = stmt
                .query_row([], |row| row.get(0))
                .expect("query C SQLite EXISTS count");
            assert_eq!(count, expected_count);
            black_box(count);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_subquery();
        let stmt =
            fsqlite_e2e::block_on(conn.prepare(&sql)).expect("prepare FrankenSQLite EXISTS count");
        b.iter(|| {
            let row =
                fsqlite_e2e::block_on(stmt.query_row()).expect("query FrankenSQLite EXISTS count");
            let count = fsqlite_integer(&row, 0, "FrankenSQLite EXISTS count");
            assert_eq!(count, expected_count);
            black_box(count);
        });
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_in_subquery(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_in_subquery_count");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    let expected_count = 100_i64;
    let sql = "SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id <= 5)";

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_subquery();
        let mut stmt = conn.prepare(sql).unwrap();
        b.iter(|| {
            let count: i64 = stmt
                .query_row([], |row| row.get(0))
                .expect("query C SQLite IN-subquery count");
            assert_eq!(count, expected_count);
            black_box(count);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_subquery();
        let stmt = fsqlite_e2e::block_on(conn.prepare(sql))
            .expect("prepare FrankenSQLite IN-subquery count");
        b.iter(|| {
            let row = fsqlite_e2e::block_on(stmt.query_row())
                .expect("query FrankenSQLite IN-subquery count");
            let count = fsqlite_integer(&row, 0, "FrankenSQLite IN-subquery count");
            assert_eq!(count, expected_count);
            black_box(count);
        });
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_recursive_cte(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_recursive_cte_sum_1000");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(RECURSIVE_CTE_LIMIT as u64));

    let sql = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 1000) SELECT SUM(x) FROM cnt";

    group.bench_function("csqlite", |b| {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        b.iter(|| {
            let sum: i64 = stmt
                .query_row([], |row| row.get(0))
                .expect("query C SQLite recursive CTE sum");
            assert_eq!(sum, RECURSIVE_CTE_SUM);
            black_box(sum);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
            .expect("open FrankenSQLite recursive CTE benchmark database");
        let stmt = fsqlite_e2e::block_on(conn.prepare(sql))
            .expect("prepare FrankenSQLite recursive CTE sum");
        b.iter(|| {
            let row = fsqlite_e2e::block_on(stmt.query_row())
                .expect("query FrankenSQLite recursive CTE sum");
            let sum = fsqlite_integer(&row, 0, "FrankenSQLite recursive CTE sum");
            assert_eq!(sum, RECURSIVE_CTE_SUM);
            black_box(sum);
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
        bench_full_count_large,
        bench_count_range,
        bench_count_sum_aggregate,
        bench_group_by,
        bench_order_limit,
        bench_exists_subquery,
        bench_in_subquery,
        bench_recursive_cte
);
criterion_main!(read_heavy);
