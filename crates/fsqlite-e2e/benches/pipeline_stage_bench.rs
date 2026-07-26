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

#[cfg(feature = "bench-internals")]
use fsqlite_btree::cursor::{
    delete_leaf_search_hint_hits_for_bench, reset_delete_leaf_search_hint_hits_for_bench,
    reset_table_seek_mru_short_circuit_hits_for_bench, set_delete_leaf_search_hint_for_bench,
    set_table_seek_mru_short_circuit_for_bench, table_seek_mru_short_circuit_hits_for_bench,
};
#[cfg(feature = "bench-internals")]
use fsqlite_core::connection::{
    prepared_direct_update_fixed_real_hits_for_bench,
    prepared_direct_update_lazy_scratch_hits_for_bench,
    reset_prepared_direct_update_fixed_real_hits_for_bench,
    reset_prepared_direct_update_lazy_scratch_hits_for_bench,
    set_prepared_direct_update_fixed_real_for_bench,
    set_prepared_direct_update_lazy_scratch_for_bench,
};
#[cfg(feature = "bench-internals")]
use fsqlite_types::value::{
    reset_small_text_direct_trait_hits_for_bench, set_small_text_direct_traits_for_bench,
    small_text_direct_trait_hits_for_bench,
};
#[cfg(feature = "bench-internals")]
use sha2::{Digest, Sha256};
#[cfg(feature = "bench-internals")]
use std::time::Instant;

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

const SEED_ROWS: i64 = 1000;

const CLUSTERED_IN_SQL: &str = "SELECT id FROM bench WHERE id IN (
                480, 481, 482, 483, 484, 485, 486, 487,
                488, 489, 490, 491, 492, 493, 494, 495,
                496, 497, 498, 499, 500, 501, 502, 503,
                504, 505, 506, 507, 508, 509, 510, 511
            )";

const CONTRACT_ROUNDS: usize = 41;
#[cfg(feature = "bench-internals")]
const CONTRACT_EXECS_PER_REPLICATE: usize = 128;
#[cfg(feature = "bench-internals")]
const CONTRACT_MIN_OF: usize = 3;
#[cfg(feature = "bench-internals")]
const CONTRACT_BOOTSTRAP_REPS: usize = 10_000;
#[cfg(feature = "bench-internals")]
const CONTRACT_CHECKSUM_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
#[cfg(feature = "bench-internals")]
const CONTRACT_CHECKSUM_PRIME: u64 = 0x0000_0100_0000_01b3;

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

fn setup_fsqlite_text_indexed_file_backed() -> (fsqlite::Connection, NamedTempFile) {
    let (connection, database) = setup_fsqlite_file_backed();
    fsqlite_e2e::block_on(connection.execute("ROLLBACK"))
        .expect("fixture transaction should roll back before index creation");
    fsqlite_e2e::block_on(connection.execute("CREATE INDEX bench_label_idx ON bench(label)"))
        .expect("fixture label index should be created");
    fsqlite_e2e::block_on(connection.execute("BEGIN"))
        .expect("fixture transaction should restart after index creation");
    (connection, database)
}

#[cfg(feature = "bench-internals")]
fn setup_fsqlite_real_file_backed() -> (fsqlite::Connection, NamedTempFile) {
    let database = NamedTempFile::new().unwrap();
    let path = database.path().to_string_lossy().into_owned();
    let connection = fsqlite_e2e::block_on(fsqlite::Connection::open(path)).unwrap();
    fsqlite_e2e::block_on(
        connection.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val REAL, label TEXT)"),
    )
    .unwrap();
    fsqlite_e2e::block_on(connection.execute("BEGIN")).unwrap();
    let insert =
        fsqlite_e2e::block_on(connection.prepare("INSERT INTO bench VALUES (?1, ?2, ?3)")).unwrap();
    for rowid in 0..SEED_ROWS {
        fsqlite_e2e::block_on(insert.execute_with_params(&[
            SqliteValue::Integer(rowid),
            SqliteValue::Float((rowid as f64) + 0.25),
            SqliteValue::Text(format!("label_{rowid:04}").into()),
        ]))
        .unwrap();
    }
    fsqlite_e2e::block_on(connection.execute("COMMIT")).unwrap();
    fsqlite_e2e::block_on(connection.execute("BEGIN")).unwrap();
    (connection, database)
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

struct ContractArmSample {
    elapsed_ns: u128,
    checksum: u64,
}

#[cfg(feature = "bench-internals")]
#[derive(Debug)]
struct ContractPairedStats {
    p50_a_ns: u128,
    p50_b_ns: u128,
    ratio_p50: f64,
    ratio_ci95: (f64, f64),
    cv_pct: f64,
    mad: f64,
    checksum_a: u64,
    checksum_b: u64,
}

#[cfg(feature = "bench-internals")]
fn contract_self_identity() -> String {
    let Ok(path) = std::env::current_exe() else {
        return "unavailable current_exe".to_owned();
    };
    let Ok(bytes) = std::fs::read(&path) else {
        return format!("unavailable read_error {}", path.display());
    };
    let digest = Sha256::digest(&bytes);
    format!(
        "{} ({} bytes) {}",
        fsqlite_e2e::bytes_to_lower_hex(digest),
        bytes.len(),
        path.display()
    )
}

#[cfg(feature = "bench-internals")]
fn contract_source_identity(path: &str) -> String {
    let Ok(bytes) = std::fs::read(path) else {
        return format!("unavailable:{path}");
    };
    let digest = Sha256::digest(&bytes);
    format!(
        "{}:{}:{}",
        path,
        fsqlite_e2e::bytes_to_lower_hex(digest),
        bytes.len()
    )
}

#[cfg(feature = "bench-internals")]
fn contract_report_source_identities() {
    println!(
        "bench_source_sha256 {} {} {} {}",
        contract_source_identity("crates/fsqlite-e2e/benches/pipeline_stage_bench.rs"),
        contract_source_identity("crates/fsqlite-btree/src/cursor.rs"),
        contract_source_identity("crates/fsqlite-types/src/value.rs"),
        contract_source_identity("crates/fsqlite-core/src/connection.rs")
    );
}

#[cfg(feature = "bench-internals")]
fn contract_mix_checksum(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(CONTRACT_CHECKSUM_PRIME)
}

#[cfg(feature = "bench-internals")]
fn contract_rows_checksum(rows: &[fsqlite::Row]) -> u64 {
    let mut ids = rows
        .iter()
        .map(|row| match row.values().first().cloned() {
            Some(SqliteValue::Integer(id)) => id,
            value => panic!("clustered IN row must contain an integer id, got {value:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    let mut checksum = contract_mix_checksum(
        CONTRACT_CHECKSUM_OFFSET,
        u64::try_from(ids.len()).expect("row count fits in u64"),
    );
    for id in ids {
        checksum = contract_mix_checksum(checksum, u64::from_ne_bytes(id.to_ne_bytes()));
    }
    checksum
}

#[cfg(feature = "bench-internals")]
fn contract_time_once(
    statement: &fsqlite::PreparedStatement<'_>,
    candidate_enabled: bool,
) -> ContractArmSample {
    set_table_seek_mru_short_circuit_for_bench(candidate_enabled);
    let mut checksum = CONTRACT_CHECKSUM_OFFSET;
    let started = Instant::now();
    for _ in 0..CONTRACT_EXECS_PER_REPLICATE {
        let rows = fsqlite_e2e::block_on(statement.query())
            .expect("clustered IN resurrection query should execute");
        assert_eq!(rows.len(), 32);
        checksum = contract_mix_checksum(checksum, contract_rows_checksum(&rows));
        black_box(rows);
    }
    ContractArmSample {
        elapsed_ns: started.elapsed().as_nanos(),
        checksum,
    }
}

#[cfg(feature = "bench-internals")]
fn contract_time_min_of(
    statement: &fsqlite::PreparedStatement<'_>,
    candidate_enabled: bool,
) -> ContractArmSample {
    let mut best: Option<ContractArmSample> = None;
    let mut expected_checksum = None;
    for _ in 0..CONTRACT_MIN_OF {
        let sample = contract_time_once(statement, candidate_enabled);
        if let Some(expected) = expected_checksum {
            assert_eq!(
                sample.checksum, expected,
                "inner replicates must produce identical output"
            );
        } else {
            expected_checksum = Some(sample.checksum);
        }
        if best
            .as_ref()
            .is_none_or(|current| sample.elapsed_ns < current.elapsed_ns)
        {
            best = Some(sample);
        }
    }
    best.expect("min-of contract requires at least one replicate")
}

#[cfg(feature = "bench-internals")]
fn contract_median_u128(values: &mut [u128]) -> u128 {
    assert!(!values.is_empty(), "median requires samples");
    values.sort_unstable();
    let upper = values.len() / 2;
    if values.len() % 2 == 0 {
        let lower_value = values[upper - 1];
        lower_value + (values[upper] - lower_value) / 2
    } else {
        values[upper]
    }
}

#[cfg(feature = "bench-internals")]
fn contract_median_f64(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty(), "median requires samples");
    values.sort_by(f64::total_cmp);
    let upper = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[upper - 1] + values[upper]) / 2.0
    } else {
        values[upper]
    }
}

#[cfg(feature = "bench-internals")]
fn contract_bootstrap_median_ci95(ratios: &[f64]) -> (f64, f64) {
    assert!(!ratios.is_empty(), "bootstrap requires samples");
    let mut state = 0x7a25_2026_5eed_cafe_u64;
    let mut bootstrap_medians = Vec::with_capacity(CONTRACT_BOOTSTRAP_REPS);
    let mut resample = vec![0.0; ratios.len()];
    let len_u64 = u64::try_from(ratios.len()).expect("sample count fits in u64");

    for _ in 0..CONTRACT_BOOTSTRAP_REPS {
        for value in &mut resample {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let index = usize::try_from(state % len_u64).expect("index fits in usize");
            *value = ratios[index];
        }
        bootstrap_medians.push(contract_median_f64(&mut resample));
    }

    bootstrap_medians.sort_by(f64::total_cmp);
    let low = CONTRACT_BOOTSTRAP_REPS * 25 / 1_000;
    let high = (CONTRACT_BOOTSTRAP_REPS * 975 / 1_000).min(CONTRACT_BOOTSTRAP_REPS - 1);
    (bootstrap_medians[low], bootstrap_medians[high])
}

#[cfg(feature = "bench-internals")]
#[allow(clippy::cast_precision_loss)]
fn contract_paired(
    statement_a: &fsqlite::PreparedStatement<'_>,
    statement_b: &fsqlite::PreparedStatement<'_>,
    candidate_b_enabled: bool,
) -> ContractPairedStats {
    contract_paired_samples(
        || contract_time_min_of(statement_a, false),
        || contract_time_min_of(statement_b, candidate_b_enabled),
    )
}

#[cfg(feature = "bench-internals")]
#[allow(clippy::cast_precision_loss)]
fn contract_paired_samples(
    mut sample_a: impl FnMut() -> ContractArmSample,
    mut sample_b: impl FnMut() -> ContractArmSample,
) -> ContractPairedStats {
    let mut times_a = Vec::with_capacity(CONTRACT_ROUNDS);
    let mut times_b = Vec::with_capacity(CONTRACT_ROUNDS);
    let mut ratios = Vec::with_capacity(CONTRACT_ROUNDS);
    let mut checksum_a = CONTRACT_CHECKSUM_OFFSET;
    let mut checksum_b = CONTRACT_CHECKSUM_OFFSET;

    for round in 0..CONTRACT_ROUNDS {
        let (arm_a, arm_b) = if round % 2 == 0 {
            (sample_a(), sample_b())
        } else {
            let arm_b = sample_b();
            let arm_a = sample_a();
            (arm_a, arm_b)
        };
        times_a.push(arm_a.elapsed_ns);
        times_b.push(arm_b.elapsed_ns);
        ratios.push((arm_a.elapsed_ns as f64) / (arm_b.elapsed_ns.max(1) as f64));
        checksum_a = contract_mix_checksum(checksum_a, arm_a.checksum);
        checksum_b = contract_mix_checksum(checksum_b, arm_b.checksum);
    }

    let ratio_ci95 = contract_bootstrap_median_ci95(&ratios);
    let ratio_p50 = contract_median_f64(&mut ratios);
    let mean = ratios.iter().sum::<f64>() / (ratios.len() as f64);
    let variance = ratios
        .iter()
        .map(|ratio| (ratio - mean).powi(2))
        .sum::<f64>()
        / ((ratios.len() - 1) as f64);
    let cv_pct = if mean == 0.0 {
        0.0
    } else {
        variance.sqrt() / mean.abs() * 100.0
    };
    let mut deviations = ratios
        .iter()
        .map(|ratio| (ratio - ratio_p50).abs())
        .collect::<Vec<_>>();
    let mad = contract_median_f64(&mut deviations);

    ContractPairedStats {
        p50_a_ns: contract_median_u128(&mut times_a),
        p50_b_ns: contract_median_u128(&mut times_b),
        ratio_p50,
        ratio_ci95,
        cv_pct,
        mad,
        checksum_a,
        checksum_b,
    }
}

#[cfg(feature = "bench-internals")]
fn contract_time_delete_once(
    connection: &fsqlite::Connection,
    candidate_enabled: bool,
) -> ContractArmSample {
    set_delete_leaf_search_hint_for_bench(candidate_enabled);
    let statement = fsqlite_e2e::block_on(connection.prepare("DELETE FROM bench WHERE id = ?1"))
        .expect("resurrection DELETE statement should prepare");
    let mut elapsed_ns = 0u128;
    let mut checksum = CONTRACT_CHECKSUM_OFFSET;

    for _ in 0..CONTRACT_EXECS_PER_REPLICATE {
        let started = Instant::now();
        let mut affected = 0usize;
        for rowid in 480..512 {
            affected += fsqlite_e2e::block_on(
                statement.execute_with_params(&[SqliteValue::Integer(rowid)]),
            )
            .expect("resurrection DELETE should execute");
        }
        elapsed_ns += started.elapsed().as_nanos();
        assert_eq!(affected, 32, "each DELETE batch must remove 32 rows");
        checksum = contract_mix_checksum(
            checksum,
            u64::try_from(affected).expect("affected row count fits in u64"),
        );
        fsqlite_e2e::block_on(connection.execute("ROLLBACK"))
            .expect("resurrection DELETE batch should roll back");
        fsqlite_e2e::block_on(connection.execute("BEGIN"))
            .expect("resurrection DELETE batch should restart its transaction");
    }

    ContractArmSample {
        elapsed_ns,
        checksum,
    }
}

#[cfg(feature = "bench-internals")]
fn contract_time_delete_min_of(
    connection: &fsqlite::Connection,
    candidate_enabled: bool,
) -> ContractArmSample {
    let mut best: Option<ContractArmSample> = None;
    let mut expected_checksum = None;
    for _ in 0..CONTRACT_MIN_OF {
        let sample = contract_time_delete_once(connection, candidate_enabled);
        if let Some(expected) = expected_checksum {
            assert_eq!(
                sample.checksum, expected,
                "DELETE inner replicates must produce identical output"
            );
        } else {
            expected_checksum = Some(sample.checksum);
        }
        if best
            .as_ref()
            .is_none_or(|current| sample.elapsed_ns < current.elapsed_ns)
        {
            best = Some(sample);
        }
    }
    best.expect("DELETE min-of contract requires at least one replicate")
}

#[cfg(feature = "bench-internals")]
fn contract_time_small_text_once(
    connection: &fsqlite::Connection,
    candidate_enabled: bool,
) -> ContractArmSample {
    set_small_text_direct_traits_for_bench(candidate_enabled);
    let statement =
        fsqlite_e2e::block_on(connection.prepare("SELECT id FROM bench WHERE label = ?1"))
            .expect("indexed TEXT resurrection statement should prepare");
    let mut checksum = CONTRACT_CHECKSUM_OFFSET;
    let started = Instant::now();
    for _ in 0..CONTRACT_EXECS_PER_REPLICATE {
        for rowid in 480..512 {
            let parameter = SqliteValue::Text(format!("label_{rowid:04}").into());
            let rows = fsqlite_e2e::block_on(statement.query_with_params(&[parameter]))
                .expect("indexed TEXT resurrection query should execute");
            assert_eq!(rows.len(), 1, "indexed TEXT probe must find one row");
            checksum = contract_mix_checksum(checksum, contract_rows_checksum(&rows));
            black_box(rows);
        }
    }
    ContractArmSample {
        elapsed_ns: started.elapsed().as_nanos(),
        checksum,
    }
}

#[cfg(feature = "bench-internals")]
fn contract_time_small_text_min_of(
    connection: &fsqlite::Connection,
    candidate_enabled: bool,
) -> ContractArmSample {
    let mut best: Option<ContractArmSample> = None;
    let mut expected_checksum = None;
    for _ in 0..CONTRACT_MIN_OF {
        let sample = contract_time_small_text_once(connection, candidate_enabled);
        if let Some(expected) = expected_checksum {
            assert_eq!(
                sample.checksum, expected,
                "SmallText inner replicates must produce identical output"
            );
        } else {
            expected_checksum = Some(sample.checksum);
        }
        if best
            .as_ref()
            .is_none_or(|current| sample.elapsed_ns < current.elapsed_ns)
        {
            best = Some(sample);
        }
    }
    best.expect("SmallText min-of contract requires at least one replicate")
}

#[cfg(feature = "bench-internals")]
fn contract_time_update_once(
    connection: &fsqlite::Connection,
    fixed_real_enabled: bool,
    lazy_scratch_enabled: bool,
    updates_per_transaction: usize,
) -> ContractArmSample {
    set_prepared_direct_update_fixed_real_for_bench(fixed_real_enabled);
    set_prepared_direct_update_lazy_scratch_for_bench(lazy_scratch_enabled);
    let update =
        fsqlite_e2e::block_on(connection.prepare("UPDATE bench SET val = ?1 WHERE id = ?2"))
            .expect("fixed-width REAL resurrection UPDATE should prepare");
    let mut elapsed_ns = 0u128;
    let mut checksum = CONTRACT_CHECKSUM_OFFSET;

    for _ in 0..CONTRACT_EXECS_PER_REPLICATE {
        let first_rowid = if updates_per_transaction == 1 {
            500
        } else {
            480
        };
        let started = Instant::now();
        let mut affected = 0usize;
        for offset in 0..updates_per_transaction {
            let rowid = first_rowid + i64::try_from(offset).expect("UPDATE offset fits in i64");
            let next_value = (rowid as f64) + 0.75;
            affected += fsqlite_e2e::block_on(update.execute_with_params(&[
                SqliteValue::Float(next_value),
                SqliteValue::Integer(rowid),
            ]))
            .expect("fixed-width REAL resurrection UPDATE should execute");
        }
        fsqlite_e2e::block_on(connection.execute("COMMIT"))
            .expect("timed UPDATE transaction should commit");
        elapsed_ns += started.elapsed().as_nanos();
        assert_eq!(
            affected, updates_per_transaction,
            "each UPDATE transaction must affect every target row"
        );

        let last_rowid = first_rowid
            + i64::try_from(updates_per_transaction - 1).expect("UPDATE count fits in i64");
        let rows = fsqlite_e2e::block_on(
            connection.query(&format!("SELECT val FROM bench WHERE id = {last_rowid}")),
        )
        .expect("committed UPDATE value should be readable");
        let expected = (last_rowid as f64) + 0.75;
        assert_eq!(
            rows.first().and_then(|row| row.values().first()),
            Some(&SqliteValue::Float(expected)),
            "committed UPDATE must preserve the exact REAL value"
        );
        checksum = contract_mix_checksum(
            checksum,
            u64::try_from(affected).expect("affected row count fits in u64"),
        );
        checksum = contract_mix_checksum(checksum, expected.to_bits());

        set_prepared_direct_update_fixed_real_for_bench(false);
        set_prepared_direct_update_lazy_scratch_for_bench(false);
        fsqlite_e2e::block_on(connection.execute("BEGIN"))
            .expect("restore transaction should begin");
        for offset in 0..updates_per_transaction {
            let rowid = first_rowid + i64::try_from(offset).expect("UPDATE offset fits in i64");
            let original_value = (rowid as f64) + 0.25;
            fsqlite_e2e::block_on(update.execute_with_params(&[
                SqliteValue::Float(original_value),
                SqliteValue::Integer(rowid),
            ]))
            .expect("fixture restore UPDATE should execute");
        }
        fsqlite_e2e::block_on(connection.execute("COMMIT"))
            .expect("fixture restore transaction should commit");
        fsqlite_e2e::block_on(connection.execute("BEGIN"))
            .expect("next timed UPDATE transaction should begin");
        set_prepared_direct_update_fixed_real_for_bench(fixed_real_enabled);
        set_prepared_direct_update_lazy_scratch_for_bench(lazy_scratch_enabled);
    }

    ContractArmSample {
        elapsed_ns,
        checksum,
    }
}

#[cfg(feature = "bench-internals")]
fn contract_time_update_min_of(
    connection: &fsqlite::Connection,
    fixed_real_enabled: bool,
    lazy_scratch_enabled: bool,
    updates_per_transaction: usize,
) -> ContractArmSample {
    let mut best: Option<ContractArmSample> = None;
    let mut expected_checksum = None;
    for _ in 0..CONTRACT_MIN_OF {
        let sample = contract_time_update_once(
            connection,
            fixed_real_enabled,
            lazy_scratch_enabled,
            updates_per_transaction,
        );
        if let Some(expected) = expected_checksum {
            assert_eq!(
                sample.checksum, expected,
                "UPDATE inner replicates must produce identical output"
            );
        } else {
            expected_checksum = Some(sample.checksum);
        }
        if best
            .as_ref()
            .is_none_or(|current| sample.elapsed_ns < current.elapsed_ns)
        {
            best = Some(sample);
        }
    }
    best.expect("UPDATE min-of contract requires at least one replicate")
}

#[cfg(feature = "bench-internals")]
#[allow(clippy::cast_precision_loss)]
fn contract_micros_per_query(nanos: u128) -> f64 {
    (nanos as f64) / (CONTRACT_EXECS_PER_REPLICATE as f64) / 1_000.0
}

#[cfg(feature = "bench-internals")]
fn contract_report(null: &ContractPairedStats, claim: &ContractPairedStats) {
    assert_eq!(
        null.checksum_a, null.checksum_b,
        "A/A output checksum mismatch"
    );
    assert_eq!(
        claim.checksum_a, claim.checksum_b,
        "A/B output checksum mismatch"
    );
    let null_radius = (null.ratio_ci95.0 - 1.0)
        .abs()
        .max((null.ratio_ci95.1 - 1.0).abs());
    let claim_effect = (claim.ratio_p50 - 1.0).abs();
    let margin = if null_radius == 0.0 {
        f64::INFINITY
    } else {
        claim_effect / null_radius
    };
    let outside_null_ci =
        claim.ratio_p50 < null.ratio_ci95.0 || claim.ratio_p50 > null.ratio_ci95.1;
    let decisive = outside_null_ci && margin >= 2.0 && claim_effect >= 0.01;
    let verdict = if !decisive {
        "INCONCLUSIVE"
    } else if claim.ratio_p50 > 1.0 {
        "KEEP"
    } else {
        "REJECT"
    };

    println!(
        "null_a_a ratio_median={:.6} ci95=[{:.6},{:.6}] cv_pct={:.3} mad={:.6} p50_a_us={:.3} p50_b_us={:.3} checksum_a={:016x} checksum_b={:016x}",
        null.ratio_p50,
        null.ratio_ci95.0,
        null.ratio_ci95.1,
        null.cv_pct,
        null.mad,
        contract_micros_per_query(null.p50_a_ns),
        contract_micros_per_query(null.p50_b_ns),
        null.checksum_a,
        null.checksum_b
    );
    println!(
        "claim_baseline_candidate ratio_median={:.6} ci95=[{:.6},{:.6}] cv_pct={:.3} mad={:.6} baseline_p50_us={:.3} candidate_p50_us={:.3} checksum_baseline={:016x} checksum_candidate={:016x}",
        claim.ratio_p50,
        claim.ratio_ci95.0,
        claim.ratio_ci95.1,
        claim.cv_pct,
        claim.mad,
        contract_micros_per_query(claim.p50_a_ns),
        contract_micros_per_query(claim.p50_b_ns),
        claim.checksum_a,
        claim.checksum_b
    );
    println!(
        "median_ci_gate={verdict} rule=null_ci95_2x_margin cv_gate=never null_radius={null_radius:.6} claim_margin={margin:.3} min_decidable_gain={:.6} max_decidable_regression={:.6}",
        1.0 + 2.0 * null_radius,
        1.0 - 2.0 * null_radius
    );
}

#[cfg(feature = "bench-internals")]
fn run_seek_cache_resurrection_contract() {
    println!("bench_elf_sha256={}", contract_self_identity());
    contract_report_source_identities();
    println!(
        "case=table_seek_cache_mru_short_circuit rows={SEED_ROWS} execs_per_replicate={CONTRACT_EXECS_PER_REPLICATE} min_of={CONTRACT_MIN_OF} rounds={CONTRACT_ROUNDS} bootstrap_reps={CONTRACT_BOOTSTRAP_REPS}"
    );

    set_table_seek_mru_short_circuit_for_bench(false);
    let (null_a_conn, _null_a_database) = setup_fsqlite_file_backed();
    let (null_b_conn, _null_b_database) = setup_fsqlite_file_backed();
    let (baseline_conn, _baseline_database) = setup_fsqlite_file_backed();
    let (candidate_conn, _candidate_database) = setup_fsqlite_file_backed();
    let null_a_statement = fsqlite_e2e::block_on(null_a_conn.prepare(CLUSTERED_IN_SQL)).unwrap();
    let null_b_statement = fsqlite_e2e::block_on(null_b_conn.prepare(CLUSTERED_IN_SQL)).unwrap();
    let baseline_statement =
        fsqlite_e2e::block_on(baseline_conn.prepare(CLUSTERED_IN_SQL)).unwrap();
    let candidate_statement =
        fsqlite_e2e::block_on(candidate_conn.prepare(CLUSTERED_IN_SQL)).unwrap();

    let _ = black_box(contract_time_once(&null_a_statement, false));
    let _ = black_box(contract_time_once(&null_b_statement, false));
    let _ = black_box(contract_time_once(&baseline_statement, false));
    let _ = black_box(contract_time_once(&candidate_statement, true));

    reset_table_seek_mru_short_circuit_hits_for_bench();
    let null = contract_paired(&null_a_statement, &null_b_statement, false);
    let claim = contract_paired(&baseline_statement, &candidate_statement, true);
    let candidate_hits = table_seek_mru_short_circuit_hits_for_bench();
    set_table_seek_mru_short_circuit_for_bench(false);

    assert!(
        candidate_hits > 0,
        "candidate branch must execute before its timing can be interpreted"
    );
    contract_report(&null, &claim);
    println!("candidate_short_circuit_hits={candidate_hits}");
}

#[cfg(feature = "bench-internals")]
fn run_delete_search_hint_resurrection_contract() {
    println!("bench_elf_sha256={}", contract_self_identity());
    contract_report_source_identities();
    println!(
        "case=same_leaf_delete_search_hint rows={SEED_ROWS} deletes_per_batch=32 batches_per_replicate={CONTRACT_EXECS_PER_REPLICATE} min_of={CONTRACT_MIN_OF} rounds={CONTRACT_ROUNDS} bootstrap_reps={CONTRACT_BOOTSTRAP_REPS}"
    );

    set_delete_leaf_search_hint_for_bench(false);
    let (null_a_conn, _null_a_database) = setup_fsqlite_file_backed();
    let (null_b_conn, _null_b_database) = setup_fsqlite_file_backed();
    let (baseline_conn, _baseline_database) = setup_fsqlite_file_backed();
    let (candidate_conn, _candidate_database) = setup_fsqlite_file_backed();

    let _ = black_box(contract_time_delete_once(&null_a_conn, false));
    let _ = black_box(contract_time_delete_once(&null_b_conn, false));
    let _ = black_box(contract_time_delete_once(&baseline_conn, false));
    let _ = black_box(contract_time_delete_once(&candidate_conn, true));

    reset_delete_leaf_search_hint_hits_for_bench();
    let null = contract_paired_samples(
        || contract_time_delete_min_of(&null_a_conn, false),
        || contract_time_delete_min_of(&null_b_conn, false),
    );
    let claim = contract_paired_samples(
        || contract_time_delete_min_of(&baseline_conn, false),
        || contract_time_delete_min_of(&candidate_conn, true),
    );
    let candidate_hits = delete_leaf_search_hint_hits_for_bench();
    set_delete_leaf_search_hint_for_bench(false);

    assert!(
        candidate_hits > 0,
        "DELETE search-hint candidate must execute before its timing can be interpreted"
    );
    contract_report(&null, &claim);
    println!("candidate_search_hint_hits={candidate_hits}");
}

#[cfg(feature = "bench-internals")]
fn run_small_text_traits_resurrection_contract() {
    println!("bench_elf_sha256={}", contract_self_identity());
    contract_report_source_identities();
    println!(
        "case=small_text_direct_traits rows={SEED_ROWS} indexed_text_probes_per_batch=32 batches_per_replicate={CONTRACT_EXECS_PER_REPLICATE} min_of={CONTRACT_MIN_OF} rounds={CONTRACT_ROUNDS} bootstrap_reps={CONTRACT_BOOTSTRAP_REPS}"
    );

    set_small_text_direct_traits_for_bench(false);
    let (null_a_conn, _null_a_database) = setup_fsqlite_text_indexed_file_backed();
    let (null_b_conn, _null_b_database) = setup_fsqlite_text_indexed_file_backed();
    let (baseline_conn, _baseline_database) = setup_fsqlite_text_indexed_file_backed();
    let (candidate_conn, _candidate_database) = setup_fsqlite_text_indexed_file_backed();

    let _ = black_box(contract_time_small_text_once(&null_a_conn, false));
    let _ = black_box(contract_time_small_text_once(&null_b_conn, false));
    let _ = black_box(contract_time_small_text_once(&baseline_conn, false));
    let _ = black_box(contract_time_small_text_once(&candidate_conn, true));

    reset_small_text_direct_trait_hits_for_bench();
    let null = contract_paired_samples(
        || contract_time_small_text_min_of(&null_a_conn, false),
        || contract_time_small_text_min_of(&null_b_conn, false),
    );
    let claim = contract_paired_samples(
        || contract_time_small_text_min_of(&baseline_conn, false),
        || contract_time_small_text_min_of(&candidate_conn, true),
    );
    let candidate_hits = small_text_direct_trait_hits_for_bench();
    set_small_text_direct_traits_for_bench(false);

    assert!(
        candidate_hits > 0,
        "SmallText trait candidate must execute before its timing can be interpreted"
    );
    contract_report(&null, &claim);
    println!("candidate_direct_trait_hits={candidate_hits}");
}

#[cfg(feature = "bench-internals")]
fn run_fixed_real_update_resurrection_contract() {
    println!("bench_elf_sha256={}", contract_self_identity());
    contract_report_source_identities();
    println!(
        "case=direct_update_fixed_width_real rows={SEED_ROWS} updates_per_transaction=32 transactions_per_replicate={CONTRACT_EXECS_PER_REPLICATE} min_of={CONTRACT_MIN_OF} rounds={CONTRACT_ROUNDS} bootstrap_reps={CONTRACT_BOOTSTRAP_REPS}"
    );

    set_prepared_direct_update_fixed_real_for_bench(false);
    set_prepared_direct_update_lazy_scratch_for_bench(false);
    let (null_a_conn, _null_a_database) = setup_fsqlite_real_file_backed();
    let (null_b_conn, _null_b_database) = setup_fsqlite_real_file_backed();
    let (baseline_conn, _baseline_database) = setup_fsqlite_real_file_backed();
    let (candidate_conn, _candidate_database) = setup_fsqlite_real_file_backed();

    let _ = black_box(contract_time_update_once(&null_a_conn, false, false, 32));
    let _ = black_box(contract_time_update_once(&null_b_conn, false, false, 32));
    let _ = black_box(contract_time_update_once(&baseline_conn, false, false, 32));
    let _ = black_box(contract_time_update_once(&candidate_conn, true, false, 32));

    reset_prepared_direct_update_fixed_real_hits_for_bench();
    let null = contract_paired_samples(
        || contract_time_update_min_of(&null_a_conn, false, false, 32),
        || contract_time_update_min_of(&null_b_conn, false, false, 32),
    );
    let claim = contract_paired_samples(
        || contract_time_update_min_of(&baseline_conn, false, false, 32),
        || contract_time_update_min_of(&candidate_conn, true, false, 32),
    );
    let candidate_hits = prepared_direct_update_fixed_real_hits_for_bench();
    set_prepared_direct_update_fixed_real_for_bench(true);
    set_prepared_direct_update_lazy_scratch_for_bench(false);

    assert!(
        candidate_hits > 0,
        "fixed-width REAL candidate must execute before its timing can be interpreted"
    );
    contract_report(&null, &claim);
    println!("candidate_fixed_real_hits={candidate_hits}");
}

#[cfg(feature = "bench-internals")]
fn run_lazy_update_scratch_resurrection_contract() {
    println!("bench_elf_sha256={}", contract_self_identity());
    contract_report_source_identities();
    println!(
        "case=direct_update_lazy_decoded_scratch rows={SEED_ROWS} updates_per_transaction=1 transactions_per_replicate={CONTRACT_EXECS_PER_REPLICATE} min_of={CONTRACT_MIN_OF} rounds={CONTRACT_ROUNDS} bootstrap_reps={CONTRACT_BOOTSTRAP_REPS}"
    );

    set_prepared_direct_update_fixed_real_for_bench(true);
    set_prepared_direct_update_lazy_scratch_for_bench(false);
    let (null_a_conn, _null_a_database) = setup_fsqlite_real_file_backed();
    let (null_b_conn, _null_b_database) = setup_fsqlite_real_file_backed();
    let (baseline_conn, _baseline_database) = setup_fsqlite_real_file_backed();
    let (candidate_conn, _candidate_database) = setup_fsqlite_real_file_backed();

    let _ = black_box(contract_time_update_once(&null_a_conn, true, false, 1));
    let _ = black_box(contract_time_update_once(&null_b_conn, true, false, 1));
    let _ = black_box(contract_time_update_once(&baseline_conn, true, false, 1));
    let _ = black_box(contract_time_update_once(&candidate_conn, true, true, 1));

    reset_prepared_direct_update_lazy_scratch_hits_for_bench();
    let null = contract_paired_samples(
        || contract_time_update_min_of(&null_a_conn, true, false, 1),
        || contract_time_update_min_of(&null_b_conn, true, false, 1),
    );
    let claim = contract_paired_samples(
        || contract_time_update_min_of(&baseline_conn, true, false, 1),
        || contract_time_update_min_of(&candidate_conn, true, true, 1),
    );
    let candidate_hits = prepared_direct_update_lazy_scratch_hits_for_bench();
    set_prepared_direct_update_fixed_real_for_bench(true);
    set_prepared_direct_update_lazy_scratch_for_bench(false);

    assert!(
        candidate_hits > 0,
        "lazy decoded-scratch candidate must execute before its timing can be interpreted"
    );
    contract_report(&null, &claim);
    println!("candidate_lazy_scratch_hits={candidate_hits}");
}

fn main() {
    #[cfg(feature = "bench-internals")]
    if std::env::var_os("FSQLITE_LEDGER_RESURRECTION_LAZY_UPDATE_SCRATCH").is_some()
        || std::env::args_os()
            .any(|argument| argument == "--ledger-resurrection-lazy-update-scratch")
    {
        run_lazy_update_scratch_resurrection_contract();
        return;
    }

    #[cfg(feature = "bench-internals")]
    if std::env::var_os("FSQLITE_LEDGER_RESURRECTION_FIXED_REAL_UPDATE").is_some()
        || std::env::args_os().any(|argument| argument == "--ledger-resurrection-fixed-real-update")
    {
        run_fixed_real_update_resurrection_contract();
        return;
    }

    #[cfg(feature = "bench-internals")]
    if std::env::var_os("FSQLITE_LEDGER_RESURRECTION_SMALL_TEXT").is_some()
        || std::env::args_os().any(|argument| argument == "--ledger-resurrection-smalltext")
    {
        run_small_text_traits_resurrection_contract();
        return;
    }

    #[cfg(feature = "bench-internals")]
    if std::env::var_os("FSQLITE_LEDGER_RESURRECTION_DELETE").is_some()
        || std::env::args_os().any(|argument| argument == "--ledger-resurrection-delete")
    {
        run_delete_search_hint_resurrection_contract();
        return;
    }

    #[cfg(feature = "bench-internals")]
    if std::env::var_os("FSQLITE_LEDGER_RESURRECTION").is_some()
        || std::env::args_os().any(|argument| argument == "--ledger-resurrection")
    {
        run_seek_cache_resurrection_contract();
        return;
    }

    pipeline_stages();
    Criterion::default().configure_from_args().final_summary();
}
