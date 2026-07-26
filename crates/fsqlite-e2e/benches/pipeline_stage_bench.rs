//! Pipeline stage micro-benchmarks (bd-6eyrg.6).
//!
//! Isolates honest, named stages of the SQL execution pipeline:
//! - FrankenSQLite lexer + parser diagnostic (not comparable with C SQLite)
//! - Cold statement lifecycle: parse + rewrite + plan + compile + handle drop,
//!   with no cache reuse
//! - Execute-only: `stmt.query()` on an already-prepared statement
//! - Cold full pipeline: parse + rewrite + plan + compile + execute + materialize
//! - Point lookup (B-tree seek): `SELECT ... WHERE id = ?`
//! - Full table scan: `SELECT ... ORDER BY id`
//!
//! Paired benchmarks materialize equivalent outputs on both engines. There is
//! deliberately no "plan-only" ratio: neither public API exposes an equivalent
//! planning-only boundary. Planning is therefore included in the explicitly
//! labelled cold-statement-lifecycle and cold-full-pipeline measurements.
//! `EXPLAIN QUERY PLAN` would also execute and format engine-specific diagnostic
//! rows, so presenting it as plan-only would be misleading.
//!
//! FrankenSQLite's paired samples poll each async operation through
//! `fsqlite_e2e::block_on`, whose thread-local runtime is reused across calls.
//! Those ratios therefore measure caller-visible work through this synchronous
//! benchmark bridge, not scheduler-free in-runtime engine stages.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use fsqlite_parser::Parser;
use fsqlite_types::SqliteValue;
use tempfile::NamedTempFile;

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

const SEED_ROWS: i64 = 1000;

type MaterializedRow = (i64, i64, String);

fn fsqlite_integer(row: &fsqlite::Row, index: usize, context: &str) -> i64 {
    match row.get(index) {
        Some(SqliteValue::Integer(value)) => *value,
        value => panic!("{context} column {index} was not an integer: {value:?}"),
    }
}

fn fsqlite_float(row: &fsqlite::Row, index: usize, context: &str) -> f64 {
    match row.get(index) {
        Some(SqliteValue::Float(value)) => *value,
        value => panic!("{context} column {index} was not a float: {value:?}"),
    }
}

fn fsqlite_text(row: &fsqlite::Row, index: usize, context: &str) -> String {
    match row.get(index) {
        Some(SqliteValue::Text(value)) => value.as_str().to_owned(),
        value => panic!("{context} column {index} was not text: {value:?}"),
    }
}

fn materialize_fsqlite_row(row: &fsqlite::Row, context: &str) -> MaterializedRow {
    (
        fsqlite_integer(row, 0, context),
        fsqlite_integer(row, 1, context),
        fsqlite_text(row, 2, context),
    )
}

fn cold_prepare_sql(nonce: i64) -> String {
    format!(
        "SELECT id, val, label FROM bench \
         WHERE val > 100 AND id < 500 AND {nonce} = {nonce} \
         ORDER BY val"
    )
}

fn cold_full_pipeline_sql(nonce: i64) -> String {
    format!(
        "SELECT id, val, label FROM bench \
         WHERE id = 500 AND {nonce} = {nonce}"
    )
}

fn setup_fsqlite() -> fsqlite::Connection {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
        .expect("open FrankenSQLite pipeline benchmark database");
    fsqlite_e2e::block_on(
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)"),
    )
    .expect("create FrankenSQLite pipeline benchmark table");
    fsqlite_e2e::block_on(conn.execute("BEGIN"))
        .expect("begin FrankenSQLite pipeline seed transaction");
    for i in 0..SEED_ROWS {
        fsqlite_e2e::block_on(conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, {}, 'label_{i:04}')",
            i * 17 + 31
        )))
        .unwrap_or_else(|error| panic!("insert FrankenSQLite pipeline seed row {i}: {error:?}"));
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT"))
        .expect("commit FrankenSQLite pipeline seed transaction");
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
    let database = NamedTempFile::new().expect("create FrankenSQLite benchmark database file");
    let path = database.path().to_string_lossy().into_owned();
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path))
        .expect("open FrankenSQLite file-backed pipeline benchmark database");
    fsqlite_e2e::block_on(
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)"),
    )
    .expect("create FrankenSQLite file-backed pipeline benchmark table");
    fsqlite_e2e::block_on(conn.execute("BEGIN"))
        .expect("begin FrankenSQLite file-backed seed transaction");
    let insert = fsqlite_e2e::block_on(conn.prepare("INSERT INTO bench VALUES (?1, ?2, ?3)"))
        .expect("prepare FrankenSQLite file-backed seed INSERT");
    for id in 0..SEED_ROWS {
        let inserted = fsqlite_e2e::block_on(insert.execute_with_params(&[
            SqliteValue::Integer(id),
            SqliteValue::Integer(id * 17 + 31),
            SqliteValue::Text(format!("label_{id:04}").into()),
        ]))
        .unwrap_or_else(|error| {
            panic!("insert FrankenSQLite file-backed seed row {id}: {error:?}")
        });
        assert_eq!(inserted, 1, "FrankenSQLite seed INSERT affected rows");
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT"))
        .expect("commit FrankenSQLite file-backed seed transaction");
    fsqlite_e2e::block_on(conn.execute("BEGIN"))
        .expect("begin FrankenSQLite file-backed read transaction");
    (conn, database)
}

fn setup_csqlite_file_backed() -> (rusqlite::Connection, NamedTempFile) {
    let database = NamedTempFile::new().unwrap();
    let conn = rusqlite::Connection::open(database.path()).unwrap();
    conn.execute_batch(
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, label TEXT); BEGIN;",
    )
    .unwrap();
    {
        let mut insert = conn
            .prepare("INSERT INTO bench VALUES (?1, ?2, ?3)")
            .unwrap();
        for id in 0..SEED_ROWS {
            insert
                .execute(rusqlite::params![
                    id,
                    id * 17 + 31,
                    format!("label_{id:04}")
                ])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT; BEGIN;").unwrap();
    (conn, database)
}

// ─── Frontend diagnostic: lexer + parser, FrankenSQLite only ─────────

/// Non-comparable diagnostic.
///
/// `rusqlite` does not expose C SQLite's lexer/parser as a standalone public
/// boundary, so this result must never enter a cross-engine ratio.
fn bench_frontend_lex_parse_diagnostic(c: &mut Criterion) {
    let sql = "SELECT id, val, label FROM bench WHERE val > 100 AND id < 500 ORDER BY val";

    c.bench_function(
        "pipeline/diagnostic_non_comparable/frankensqlite_frontend_lex_parse",
        |b| {
            b.iter(|| {
                let mut parser = Parser::from_sql(black_box(sql));
                let statement = parser
                    .parse_statement()
                    .expect("parse FrankenSQLite frontend diagnostic statement");
                black_box(statement);
            });
        },
    );
}

// ─── Cold statement lifecycle: prepare + handle drop, no execution ──

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_prepare_only(c: &mut Criterion) {
    // The semantically inert nonce forces a cache miss in FrankenSQLite. The
    // timed caller operation is prepare-then-drop on both APIs. Their internal
    // cleanup differs (C SQLite finalizes while FrankenSQLite may retain a
    // cache template), and that public lifecycle behavior is deliberately
    // included rather than mislabelled as an intrinsic compiler-only stage.
    let mut group = c.benchmark_group("pipeline/caller_visible_cold_prepare_drop");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();
    let mut f_nonce = 0_i64;
    let mut c_nonce = 0_i64;

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                f_nonce += 1;
                cold_prepare_sql(f_nonce)
            },
            |sql| {
                let statement = fsqlite_e2e::block_on(fconn.prepare(&sql))
                    .expect("cold-prepare FrankenSQLite statement");
                black_box(statement);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                c_nonce += 1;
                cold_prepare_sql(c_nonce)
            },
            |sql| {
                let statement = cconn
                    .prepare(&sql)
                    .expect("cold-prepare C SQLite statement");
                black_box(statement);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ─── Execute-only: pre-prepared statement, just run ──────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_execute_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/execute_prepared_point");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let f_stmt = fsqlite_e2e::block_on(fconn.prepare("SELECT id, val FROM bench WHERE id = 500"))
        .expect("prepare FrankenSQLite execute-only point lookup");
    let mut c_stmt = cconn
        .prepare("SELECT id, val FROM bench WHERE id = 500")
        .expect("prepare C SQLite execute-only point lookup");

    group.bench_function("frankensqlite", |b| {
        b.iter(|| {
            let row = fsqlite_e2e::block_on(f_stmt.query_row())
                .expect("execute FrankenSQLite prepared point lookup");
            let result = (
                fsqlite_integer(&row, 0, "FrankenSQLite prepared point lookup"),
                fsqlite_integer(&row, 1, "FrankenSQLite prepared point lookup"),
            );
            assert_eq!(result, (500, 8_531));
            black_box(result);
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let result: (i64, i64) = c_stmt
                .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
                .expect("execute C SQLite prepared point lookup");
            assert_eq!(result, (500, 8_531));
            black_box(result);
        });
    });

    group.finish();
}

// ─── Cold full pipeline: parse + rewrite + plan + compile + execute ──

// BENCH-META: engine=csqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
fn bench_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/caller_visible_cold_query_materialize");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();
    let mut f_nonce = 0_i64;
    let mut c_nonce = 0_i64;

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                f_nonce += 1;
                cold_full_pipeline_sql(f_nonce)
            },
            |sql| {
                let rows = fsqlite_e2e::block_on(fconn.query(&sql))
                    .expect("run FrankenSQLite cold full-pipeline point lookup")
                    .iter()
                    .map(|row| {
                        materialize_fsqlite_row(
                            row,
                            "FrankenSQLite cold full-pipeline point lookup",
                        )
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    rows,
                    vec![(500, 8_531, "label_0500".to_owned())],
                    "FrankenSQLite cold full-pipeline result"
                );
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                c_nonce += 1;
                cold_full_pipeline_sql(c_nonce)
            },
            |sql| {
                let mut statement = cconn
                    .prepare(&sql)
                    .expect("cold-prepare C SQLite full-pipeline point lookup");
                let rows = statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                    .expect("query C SQLite cold full-pipeline point lookup")
                    .collect::<Result<Vec<MaterializedRow>, _>>()
                    .expect("materialize C SQLite cold full-pipeline point lookup");
                assert_eq!(
                    rows,
                    vec![(500, 8_531, "label_0500".to_owned())],
                    "C SQLite cold full-pipeline result"
                );
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ─── B-tree seek: point lookups across key space ─────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_btree_seek(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/execute_prepared_btree_seek_50");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let keys: Vec<i64> = (0..50).map(|i| i * 20).collect();
    let expected_values = keys.iter().map(|key| key * 17 + 31).collect::<Vec<_>>();
    let f_stmt = fsqlite_e2e::block_on(fconn.prepare("SELECT val FROM bench WHERE id = ?1"))
        .expect("prepare FrankenSQLite B-tree seek");
    let mut c_stmt = cconn
        .prepare("SELECT val FROM bench WHERE id = ?1")
        .expect("prepare C SQLite B-tree seek");

    group.bench_function("frankensqlite", |b| {
        b.iter(|| {
            let mut values = Vec::with_capacity(keys.len());
            for &key in &keys {
                let row = fsqlite_e2e::block_on(
                    f_stmt.query_row_with_params(&[SqliteValue::Integer(key)]),
                )
                .expect("execute FrankenSQLite B-tree seek");
                values.push(fsqlite_integer(&row, 0, "FrankenSQLite B-tree seek"));
            }
            assert_eq!(values, expected_values);
            black_box(values);
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let mut values = Vec::with_capacity(keys.len());
            for &key in &keys {
                let val: i64 = c_stmt
                    .query_row(rusqlite::params![key], |r| r.get(0))
                    .expect("execute C SQLite B-tree seek");
                values.push(val);
            }
            assert_eq!(values, expected_values);
            black_box(values);
        });
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=file, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=file, concurrency=sequential
fn bench_btree_seek_file_clustered_in(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/btree_seek_file_clustered_in");

    let (fconn, _fdatabase) = setup_fsqlite_file_backed();
    let (cconn, _cdatabase) = setup_csqlite_file_backed();
    let sql = "SELECT id FROM bench WHERE id IN (
                480, 481, 482, 483, 484, 485, 486, 487,
                488, 489, 490, 491, 492, 493, 494, 495,
                496, 497, 498, 499, 500, 501, 502, 503,
                504, 505, 506, 507, 508, 509, 510, 511
            )";
    let f_stmt = fsqlite_e2e::block_on(fconn.prepare(sql))
        .expect("prepare FrankenSQLite clustered IN lookup");
    let mut c_stmt = cconn
        .prepare(sql)
        .expect("prepare C SQLite clustered IN lookup");

    let expected_rows =
        fsqlite_e2e::block_on(f_stmt.query()).expect("query FrankenSQLite clustered IN validation");
    let mut f_expected_ids: Vec<i64> = expected_rows
        .iter()
        .map(|row| fsqlite_integer(row, 0, "FrankenSQLite clustered IN validation"))
        .collect();
    f_expected_ids.sort_unstable();
    let mut c_expected_ids = c_stmt
        .query_map([], |row| row.get::<_, i64>(0))
        .expect("query C SQLite clustered IN validation")
        .collect::<Result<Vec<_>, _>>()
        .expect("materialize C SQLite clustered IN validation");
    c_expected_ids.sort_unstable();
    let exact_expected_ids = (480..=511).collect::<Vec<_>>();
    assert_eq!(f_expected_ids, exact_expected_ids);
    assert_eq!(c_expected_ids, exact_expected_ids);
    assert_eq!(f_expected_ids.iter().sum::<i64>(), 15_856);

    group.bench_function("frankensqlite", |b| {
        b.iter(|| {
            let ids = fsqlite_e2e::block_on(f_stmt.query())
                .expect("execute FrankenSQLite clustered IN lookup")
                .iter()
                .map(|row| fsqlite_integer(row, 0, "FrankenSQLite clustered IN lookup"))
                .collect::<Vec<_>>();
            assert_eq!(ids.len(), 32);
            black_box(ids);
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let ids = c_stmt
                .query_map([], |row| row.get::<_, i64>(0))
                .expect("execute C SQLite clustered IN lookup")
                .collect::<Result<Vec<_>, _>>()
                .expect("materialize C SQLite clustered IN lookup");
            assert_eq!(ids.len(), 32);
            black_box(ids);
        });
    });

    group.finish();
}

// ─── Full table scan ─────────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/execute_prepared_full_scan");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let sql = "SELECT id, val, label FROM bench ORDER BY id";
    let f_stmt =
        fsqlite_e2e::block_on(fconn.prepare(sql)).expect("prepare FrankenSQLite full-table scan");
    let mut c_stmt = cconn
        .prepare(sql)
        .expect("prepare C SQLite full-table scan");
    let expected_rows = (0..SEED_ROWS)
        .map(|id| (id, id * 17 + 31, format!("label_{id:04}")))
        .collect::<Vec<_>>();
    let f_validation = fsqlite_e2e::block_on(f_stmt.query())
        .expect("validate FrankenSQLite full-table scan")
        .iter()
        .map(|row| materialize_fsqlite_row(row, "FrankenSQLite full-table scan validation"))
        .collect::<Vec<_>>();
    let c_validation = c_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .expect("validate C SQLite full-table scan")
        .collect::<Result<Vec<MaterializedRow>, _>>()
        .expect("materialize C SQLite full-table scan validation");
    assert_eq!(f_validation, expected_rows);
    assert_eq!(c_validation, expected_rows);

    group.bench_function("frankensqlite", |b| {
        b.iter(|| {
            let rows = fsqlite_e2e::block_on(f_stmt.query())
                .expect("execute FrankenSQLite full-table scan")
                .iter()
                .map(|row| materialize_fsqlite_row(row, "FrankenSQLite full-table scan"))
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), SEED_ROWS as usize);
            black_box(rows);
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let rows = c_stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .expect("execute C SQLite full-table scan")
                .collect::<Result<Vec<MaterializedRow>, _>>()
                .expect("materialize C SQLite full-table scan");
            assert_eq!(rows.len(), SEED_ROWS as usize);
            black_box(rows);
        });
    });

    group.finish();
}

// ─── Aggregate pipeline ──────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/execute_prepared_aggregate");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let sql = "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM bench";
    let f_stmt =
        fsqlite_e2e::block_on(fconn.prepare(sql)).expect("prepare FrankenSQLite aggregate");
    let mut c_stmt = cconn.prepare(sql).expect("prepare C SQLite aggregate");
    let expected_aggregate = (SEED_ROWS, 8_522_500_i64, 8_522.5_f64, 31_i64, 17_014_i64);

    group.bench_function("frankensqlite", |b| {
        b.iter(|| {
            let row = fsqlite_e2e::block_on(f_stmt.query_row())
                .expect("execute FrankenSQLite prepared aggregate");
            let aggregate = (
                fsqlite_integer(&row, 0, "FrankenSQLite aggregate"),
                fsqlite_integer(&row, 1, "FrankenSQLite aggregate"),
                fsqlite_float(&row, 2, "FrankenSQLite aggregate"),
                fsqlite_integer(&row, 3, "FrankenSQLite aggregate"),
                fsqlite_integer(&row, 4, "FrankenSQLite aggregate"),
            );
            assert_eq!(aggregate, expected_aggregate);
            black_box(aggregate);
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let aggregate: (i64, i64, f64, i64, i64) = c_stmt
                .query_row([], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                })
                .expect("execute C SQLite prepared aggregate");
            assert_eq!(aggregate, expected_aggregate);
            black_box(aggregate);
        });
    });

    group.finish();
}

// ─── Insert pipeline (single row, autocommit) ───────────────────────

// BENCH-META: engine=csqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=ad_hoc, storage=memory, concurrency=sequential
fn bench_insert_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/caller_visible_cold_autocommit_insert_empty_table");

    group.bench_function("frankensqlite", |b| {
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:"))
            .expect("open FrankenSQLite INSERT pipeline database");
        fsqlite_e2e::block_on(
            conn.execute("CREATE TABLE insert_bench (id INTEGER PRIMARY KEY, val INTEGER)"),
        )
        .expect("create FrankenSQLite INSERT pipeline table");
        let delete = fsqlite_e2e::block_on(conn.prepare("DELETE FROM insert_bench"))
            .expect("prepare FrankenSQLite INSERT benchmark reset");
        let mut counter = 0i64;
        b.iter_batched(
            || {
                let deleted = fsqlite_e2e::block_on(delete.execute())
                    .expect("reset FrankenSQLite INSERT benchmark table");
                assert!(deleted <= 1, "FrankenSQLite reset removed {deleted} rows");
                counter += 1;
                format!("INSERT INTO insert_bench VALUES ({counter}, {counter})")
            },
            |sql| {
                let inserted = fsqlite_e2e::block_on(conn.execute(&sql))
                    .expect("execute FrankenSQLite ad-hoc INSERT");
                assert_eq!(inserted, 1);
                black_box(inserted);
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("csqlite", |b| {
        let conn =
            rusqlite::Connection::open_in_memory().expect("open C SQLite INSERT pipeline database");
        conn.execute_batch("CREATE TABLE insert_bench (id INTEGER PRIMARY KEY, val INTEGER);")
            .expect("create C SQLite INSERT pipeline table");
        let mut delete = conn
            .prepare("DELETE FROM insert_bench")
            .expect("prepare C SQLite INSERT benchmark reset");
        let mut counter = 0i64;
        b.iter_batched(
            || {
                let deleted = delete
                    .execute([])
                    .expect("reset C SQLite INSERT benchmark table");
                assert!(deleted <= 1, "C SQLite reset removed {deleted} rows");
                counter += 1;
                format!("INSERT INTO insert_bench VALUES ({counter}, {counter})")
            },
            |sql| {
                let inserted = conn
                    .execute(&sql, [])
                    .expect("execute C SQLite ad-hoc INSERT");
                assert_eq!(inserted, 1);
                black_box(inserted);
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group! {
    name = pipeline_stages;
    config = criterion_config();
    targets =
        bench_frontend_lex_parse_diagnostic,
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
