//! Pipeline stage micro-benchmarks (bd-6eyrg.6).
//!
//! Isolates each stage of the SQL execution pipeline to identify bottlenecks:
//! - Prepare (parse + compile): `conn.prepare(sql)`
//! - Execute-only: `stmt.query()` on already-prepared statement
//! - Full pipeline: `conn.query(sql)` (prepare + execute combined)
//! - Point lookup (B-tree seek): `SELECT ... WHERE id = ?`
//! - Full table scan: `SELECT ... ORDER BY id`
//!
//! Each benchmark runs both FrankenSQLite and C SQLite (rusqlite) side by side.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use fsqlite_types::SqliteValue;
use tempfile::NamedTempFile;

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

const SEED_ROWS: i64 = 1000;

fn setup_fsqlite() -> fsqlite::Connection {
    let conn = fsqlite::Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..SEED_ROWS {
        conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, {}, 'label_{i:04}')",
            i * 17 + 31
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn
}

fn setup_csqlite() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, label TEXT);")
        .unwrap();
    conn.execute_batch("BEGIN;").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO bench VALUES (?1, ?2, ?3)")
            .unwrap();
        for i in 0..SEED_ROWS {
            stmt.execute(rusqlite::params![i, i * 17 + 31, format!("label_{i:04}")])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT;").unwrap();
    conn
}

fn setup_fsqlite_file_backed() -> (fsqlite::Connection, NamedTempFile) {
    let database = NamedTempFile::new().unwrap();
    let path = database.path().to_string_lossy().into_owned();
    let conn = fsqlite::Connection::open(path).unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let insert = conn
        .prepare("INSERT INTO bench VALUES (?1, ?2, ?3)")
        .unwrap();
    for id in 0..SEED_ROWS {
        insert
            .execute_with_params(&[
                SqliteValue::Integer(id),
                SqliteValue::Integer(id * 17 + 31),
                SqliteValue::Text(format!("label_{id:04}").into()),
            ])
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("BEGIN").unwrap();
    (conn, database)
}

// ─── Prepare-only: parse + compile, no execution ─────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_prepare_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/prepare_only");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let sql = "SELECT id, val, label FROM bench WHERE val > 100 AND id < 500 ORDER BY val";

    group.bench_function("fsqlite", |b| {
        b.iter(|| {
            let _stmt = fconn.prepare(sql).unwrap();
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let _stmt = cconn.prepare(sql).unwrap();
        });
    });

    group.finish();
}

// ─── Execute-only: pre-prepared statement, just run ──────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_execute_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/execute_only");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let f_stmt = fconn
        .prepare("SELECT id, val FROM bench WHERE id = 500")
        .unwrap();
    let mut c_stmt = cconn
        .prepare("SELECT id, val FROM bench WHERE id = 500")
        .unwrap();

    group.bench_function("fsqlite", |b| {
        b.iter(|| {
            let rows = f_stmt.query().unwrap();
            assert_eq!(rows.len(), 1);
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let mut rows = c_stmt.query([]).unwrap();
            let row = rows.next().unwrap().unwrap();
            let _id: i64 = row.get(0).unwrap();
        });
    });

    group.finish();
}

// ─── Full pipeline: conn.query() = prepare + execute ─────────────────

// BENCH-META: engine=csqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
fn bench_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/full");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let sql_point = "SELECT id, val, label FROM bench WHERE id = 500";

    group.bench_function("fsqlite/point", |b| {
        b.iter(|| {
            let rows = fconn.query(sql_point).unwrap();
            assert_eq!(rows.len(), 1);
        });
    });

    group.bench_function("csqlite/point", |b| {
        b.iter(|| {
            let mut stmt = cconn.prepare(sql_point).unwrap();
            let count = stmt.query_map([], |_r| Ok(())).unwrap().count();
            assert_eq!(count, 1);
        });
    });

    group.finish();
}

// ─── B-tree seek: point lookups across key space ─────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_btree_seek(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/btree_seek");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let keys: Vec<i64> = (0..50).map(|i| i * 20).collect();

    group.bench_function("fsqlite", |b| {
        b.iter(|| {
            for &key in &keys {
                let rows = fconn
                    .query(&format!("SELECT val FROM bench WHERE id = {key}"))
                    .unwrap();
                assert_eq!(rows.len(), 1);
            }
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let mut stmt = cconn
                .prepare("SELECT val FROM bench WHERE id = ?1")
                .unwrap();
            for &key in &keys {
                let val: i64 = stmt
                    .query_row(rusqlite::params![key], |r| r.get(0))
                    .unwrap();
                std::hint::black_box(val);
            }
        });
    });

    group.finish();
}

// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=file, concurrency=sequential
fn bench_btree_seek_file_clustered_in(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/btree_seek_file_clustered_in");

    let (conn, _database) = setup_fsqlite_file_backed();
    let stmt = conn
        .prepare(
            "SELECT id FROM bench WHERE id IN (
                480, 481, 482, 483, 484, 485, 486, 487,
                488, 489, 490, 491, 492, 493, 494, 495,
                496, 497, 498, 499, 500, 501, 502, 503,
                504, 505, 506, 507, 508, 509, 510, 511
            )",
        )
        .unwrap();
    let expected_rows = stmt.query().unwrap();
    let expected_ids: Vec<i64> = expected_rows
        .iter()
        .filter_map(|row| match row.values().first() {
            Some(SqliteValue::Integer(id)) => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(expected_rows.len(), 32);
    assert_eq!(expected_ids.len(), 32);
    assert_eq!(expected_ids.iter().sum::<i64>(), 15_856);

    group.bench_function("fsqlite", |b| {
        b.iter(|| {
            let rows = stmt.query().unwrap();
            assert_eq!(rows.len(), 32);
            black_box(rows);
        });
    });

    group.finish();
}

// ─── Full table scan ─────────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/full_scan");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let sql = "SELECT id, val, label FROM bench ORDER BY id";

    group.bench_function("fsqlite", |b| {
        b.iter(|| {
            let rows = fconn.query(sql).unwrap();
            assert_eq!(rows.len(), SEED_ROWS as usize);
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let mut stmt = cconn.prepare(sql).unwrap();
            let count = stmt.query_map([], |_r| Ok(())).unwrap().count();
            assert_eq!(count, SEED_ROWS as usize);
        });
    });

    group.finish();
}

// ─── Aggregate pipeline ──────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/aggregate");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let sql = "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM bench";

    group.bench_function("fsqlite", |b| {
        b.iter(|| {
            let rows = fconn.query(sql).unwrap();
            assert_eq!(rows.len(), 1);
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let mut stmt = cconn.prepare(sql).unwrap();
            let count = stmt.query_map([], |_r| Ok(())).unwrap().count();
            assert_eq!(count, 1);
        });
    });

    group.finish();
}

// ─── Insert pipeline (single row, autocommit) ───────────────────────

// BENCH-META: engine=csqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
fn bench_insert_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/insert_single");

    group.bench_function("fsqlite", |b| {
        let conn = fsqlite::Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE insert_bench (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        let mut counter = 0i64;
        b.iter(|| {
            counter += 1;
            conn.execute(&format!(
                "INSERT INTO insert_bench VALUES ({counter}, {counter})"
            ))
            .unwrap();
        });
    });

    group.bench_function("csqlite", |b| {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE insert_bench (id INTEGER PRIMARY KEY, val INTEGER);")
            .unwrap();
        let mut counter = 0i64;
        b.iter(|| {
            counter += 1;
            conn.execute(
                "INSERT INTO insert_bench VALUES (?1, ?2)",
                rusqlite::params![counter, counter],
            )
            .unwrap();
        });
    });

    group.finish();
}

criterion_group! {
    name = pipeline_stages;
    config = criterion_config();
    targets =
        bench_prepare_only,
        bench_execute_only,
        bench_full_pipeline,
        bench_btree_seek,
        bench_btree_seek_file_clustered_in,
        bench_full_scan,
        bench_aggregate,
        bench_insert_single,
}

criterion_main!(pipeline_stages);
