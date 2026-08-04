#![recursion_limit = "512"]
// bd-mnlk2 / bd-zavyn: the hoisted timed bodies await fsqlite-core's
// deliberately large, deeply nested engine futures inside one runtime entry
// per sample; boxing them would put an allocation inside the timed window.
#![allow(clippy::large_futures)]

//! General end-to-end benchmark suite.
//!
//! Benchmark discipline: when the rusqlite side uses prepared statements or
//! stable parameterized SQL, the FrankenSQLite side must stay on the same
//! statement-lifecycle mode. Otherwise the benchmark mostly measures avoidable
//! parse/compile churn rather than engine behavior.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fsqlite::SqliteValue;

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

const SUBQUERY_ROWS_100K: i64 = 100_000;

fn apply_subquery_pragmas_csqlite(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "PRAGMA page_size = 4096;\
         PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA cache_size = -64000;",
    )
    .expect("failed to apply C SQLite subquery benchmark pragmas");
}

fn apply_subquery_pragmas_fsqlite(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA page_size = 4096;",
        "PRAGMA journal_mode = WAL;",
        "PRAGMA synchronous = NORMAL;",
        "PRAGMA cache_size = -64000;",
    ] {
        fsqlite_e2e::block_on(conn.execute(pragma)).unwrap_or_else(|error| {
            panic!("failed to apply FrankenSQLite subquery benchmark pragma `{pragma}`: {error}")
        });
    }
}

fn setup_csqlite_exists_regression_bench(row_count: i64) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    apply_subquery_pragmas_csqlite(&conn);
    conn.execute_batch(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);\
         CREATE TABLE product_flags (product_id INTEGER PRIMARY KEY, active INTEGER NOT NULL);",
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut product_stmt = conn
            .prepare("INSERT INTO products VALUES (?1, ('prod_' || ?1), (?1 * 3.14))")
            .unwrap();
        for i in 1..=row_count {
            product_stmt.execute(rusqlite::params![i]).unwrap();
        }

        let mut flag_stmt = conn
            .prepare("INSERT INTO product_flags VALUES (?1, 1)")
            .unwrap();
        for i in (1..=row_count).step_by(2) {
            flag_stmt.execute(rusqlite::params![i]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

fn setup_fsqlite_exists_regression_bench(row_count: i64) -> fsqlite::Connection {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
    apply_subquery_pragmas_fsqlite(&conn);
    fsqlite_e2e::block_on(
        conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)"),
    )
    .unwrap();
    fsqlite_e2e::block_on(conn.execute(
        "CREATE TABLE product_flags (product_id INTEGER PRIMARY KEY, active INTEGER NOT NULL)",
    ))
    .unwrap();
    fsqlite_e2e::block_on(conn.execute("BEGIN")).unwrap();
    for i in 1..=row_count {
        let price = i as f64 * 3.14;
        fsqlite_e2e::block_on(conn.execute(&format!(
            "INSERT INTO products VALUES ({i}, 'prod_{i}', {price})"
        )))
        .unwrap();
    }
    for i in (1..=row_count).step_by(2) {
        fsqlite_e2e::block_on(conn.execute(&format!("INSERT INTO product_flags VALUES ({i}, 1)")))
            .unwrap();
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT")).unwrap();
    conn
}

fn setup_csqlite_in_regression_bench(row_count: i64) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    apply_subquery_pragmas_csqlite(&conn);
    conn.execute_batch(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);\
         CREATE TABLE selected_ids (id INTEGER PRIMARY KEY);",
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut product_stmt = conn
            .prepare("INSERT INTO products VALUES (?1, ('prod_' || ?1), (?1 * 3.14))")
            .unwrap();
        let mut selected_stmt = conn
            .prepare("INSERT INTO selected_ids VALUES (?1)")
            .unwrap();
        for i in 1..=row_count {
            product_stmt.execute(rusqlite::params![i]).unwrap();
            selected_stmt.execute(rusqlite::params![i]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

fn setup_fsqlite_in_regression_bench(row_count: i64) -> fsqlite::Connection {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
    apply_subquery_pragmas_fsqlite(&conn);
    fsqlite_e2e::block_on(
        conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)"),
    )
    .unwrap();
    fsqlite_e2e::block_on(conn.execute("CREATE TABLE selected_ids (id INTEGER PRIMARY KEY)"))
        .unwrap();
    fsqlite_e2e::block_on(conn.execute("BEGIN")).unwrap();
    for i in 1..=row_count {
        let price = i as f64 * 3.14;
        fsqlite_e2e::block_on(conn.execute(&format!(
            "INSERT INTO products VALUES ({i}, 'prod_{i}', {price})"
        )))
        .unwrap();
        fsqlite_e2e::block_on(conn.execute(&format!("INSERT INTO selected_ids VALUES ({i})")))
            .unwrap();
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT")).unwrap();
    conn
}

// ─── Sequential INSERT benchmark (100 rows, autocommit per row) ────────
//
// Important: this benchmark intentionally does NOT wrap the loop in
// BEGIN/COMMIT. Each statement therefore pays the connection's normal
// autocommit resolve path, so this group is the right place to look at
// per-row autocommit overhead rather than batched insert throughput.

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_sequential_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_inserts_100");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
                    .unwrap();
                conn
            },
            |conn| {
                let mut stmt = conn.prepare("INSERT INTO t VALUES (?1, 'val');").unwrap();
                for i in 0..100_i64 {
                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(
                    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"),
                )
                .unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let stmt = conn
                        .prepare("INSERT INTO t VALUES (?1, 'val');")
                        .await
                        .unwrap();
                    for i in 0..100_i64 {
                        black_box(
                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── Sequential INSERT benchmark (100 rows, one explicit transaction) ──
//
// This is the batched counterpart to `sequential_inserts_100`. The statement
// execution loop stays identical, but commit work is paid once at the end
// instead of once per row.

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_sequential_inserts_single_txn(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_inserts_100_single_txn");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
                    .unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN").unwrap();
                let mut stmt = conn.prepare("INSERT INTO t VALUES (?1, 'val');").unwrap();
                for i in 0..100_i64 {
                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                }
                conn.execute_batch("COMMIT").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(
                    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"),
                )
                .unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN").await.unwrap();
                    let stmt = conn
                        .prepare("INSERT INTO t VALUES (?1, 'val');")
                        .await
                        .unwrap();
                    for i in 0..100_i64 {
                        black_box(
                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    conn.execute("COMMIT").await.unwrap();
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── Bulk INSERT benchmark (1000 rows) ──────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_bulk_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_inserts_1000");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL);",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN;").unwrap();
                let mut stmt = conn
                    .prepare("INSERT INTO t VALUES (?1, ('name_' || ?1), (?1 * 1.1));")
                    .unwrap();
                for i in 0..1000_i64 {
                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                }
                conn.execute_batch("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(
                    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL);"),
                )
                .unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN;").await.unwrap();
                    let stmt = conn
                        .prepare("INSERT INTO t VALUES (?1, ('name_' || ?1), (?1 * 1.1));")
                        .await
                        .unwrap();
                    for i in 0..1000_i64 {
                        black_box(
                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    conn.execute("COMMIT;").await.unwrap();
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── SELECT query benchmark ─────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_select_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_after_100_rows");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("csqlite", |b| {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
            .unwrap();
        {
            let mut stmt = conn
                .prepare("INSERT INTO t VALUES (?1, ('v' || ?1));")
                .unwrap();
            for i in 0..100_i64 {
                stmt.execute(rusqlite::params![i]).unwrap();
            }
        }

        let mut stmt = conn
            .prepare("SELECT * FROM t WHERE id >= 25 AND id < 75")
            .unwrap();
        b.iter(|| {
            let rows: Vec<(i64, String)> = stmt
                .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            black_box(&rows);
            assert_eq!(rows.len(), 50);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
        fsqlite_e2e::block_on(conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"))
            .unwrap();
        {
            let stmt =
                fsqlite_e2e::block_on(conn.prepare("INSERT INTO t VALUES (?1, ('v' || ?1));"))
                    .unwrap();
            for i in 0..100_i64 {
                fsqlite_e2e::block_on(stmt.execute_with_params(&[SqliteValue::Integer(i)]))
                    .unwrap();
            }
        }
        let stmt =
            fsqlite_e2e::block_on(conn.prepare("SELECT * FROM t WHERE id >= 25 AND id < 75"))
                .unwrap();

        b.iter(|| {
            let rows = fsqlite_e2e::block_on(stmt.query()).unwrap();
            black_box(&rows);
            assert_eq!(rows.len(), 50);
        });
    });

    group.finish();
}

// ─── Mixed DML benchmark ────────────────────────────────────────────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_mixed_dml(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_dml");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, n INTEGER);")
                    .unwrap();
                {
                    let mut stmt = conn
                        .prepare("INSERT INTO t VALUES (?1, ('val_' || ?1), (?1 * 10));")
                        .unwrap();
                    for i in 0..200_i64 {
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                }
                conn
            },
            |conn| {
                {
                    let mut stmt = conn
                        .prepare("UPDATE t SET n = (?1 * 100) WHERE id = ?1;")
                        .unwrap();
                    for i in 0..50_i64 {
                        black_box(stmt.execute(rusqlite::params![i]).unwrap());
                    }
                }
                {
                    let mut stmt = conn.prepare("DELETE FROM t WHERE id = ?1;").unwrap();
                    for i in 150..200_i64 {
                        black_box(stmt.execute(rusqlite::params![i]).unwrap());
                    }
                }
                {
                    let mut stmt = conn
                        .prepare("INSERT INTO t VALUES (?1, ('new_' || ?1), ?1);")
                        .unwrap();
                    for i in 200..250_i64 {
                        black_box(stmt.execute(rusqlite::params![i]).unwrap());
                    }
                }
                let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
                let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
                black_box(count);
                assert_eq!(count, 200);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(
                    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, n INTEGER);"),
                )
                .unwrap();
                let insert_stmt = fsqlite_e2e::block_on(
                    conn.prepare("INSERT INTO t VALUES (?1, ('val_' || ?1), (?1 * 10));"),
                )
                .unwrap();
                for i in 0..200_i64 {
                    fsqlite_e2e::block_on(
                        insert_stmt.execute_with_params(&[SqliteValue::Integer(i)]),
                    )
                    .unwrap();
                }
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let update_stmt = conn
                        .prepare("UPDATE t SET n = (?1 * 100) WHERE id = ?1;")
                        .await
                        .unwrap();
                    for i in 0..50_i64 {
                        black_box(
                            update_stmt
                                .execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    let delete_stmt = conn.prepare("DELETE FROM t WHERE id = ?1;").await.unwrap();
                    for i in 150..200_i64 {
                        black_box(
                            delete_stmt
                                .execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    let insert_stmt = conn
                        .prepare("INSERT INTO t VALUES (?1, ('new_' || ?1), ?1);")
                        .await
                        .unwrap();
                    for i in 200..250_i64 {
                        black_box(
                            insert_stmt
                                .execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    let stmt = conn.prepare("SELECT count(*) FROM t").await.unwrap();
                    let row = stmt.query_row().await.unwrap();
                    black_box(&row);
                    let count = &row.values()[0];
                    assert_eq!(*count, SqliteValue::Integer(200));
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── bd-1dus: Sequential write throughput (10K rows, 3 variants) ────────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_write_throughput_autocommit(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_10k_autocommit");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);",
                )
                .unwrap();
                conn
            },
            |conn| {
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137));")
                    .unwrap();
                for i in 0..10_000_i64 {
                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);",
                ))
                .unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let stmt = conn
                        .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137));")
                        .await
                        .unwrap();
                    for i in 0..10_000_i64 {
                        black_box(
                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_write_throughput_batched(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_10k_batched_1k");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);",
                )
                .unwrap();
                conn
            },
            |conn| {
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137));")
                    .unwrap();
                for batch in 0..10 {
                    conn.execute_batch("BEGIN;").unwrap();
                    let base = batch * 1000;
                    for i in base..base + 1000 {
                        black_box(stmt.execute(rusqlite::params![i]).unwrap());
                    }
                    conn.execute_batch("COMMIT;").unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);",
                ))
                .unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let stmt = conn
                        .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137));")
                        .await
                        .unwrap();
                    for batch in 0..10 {
                        conn.execute("BEGIN;").await.unwrap();
                        let base = batch as i64 * 1000;
                        for i in base..base + 1000 {
                            black_box(
                                stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                    .await
                                    .unwrap(),
                            );
                        }
                        conn.execute("COMMIT;").await.unwrap();
                    }
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_write_throughput_single_txn(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_10k_single_txn");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN;").unwrap();
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137));")
                    .unwrap();
                for i in 0..10_000_i64 {
                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                }
                conn.execute_batch("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);",
                ))
                .unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN;").await.unwrap();
                    let stmt = conn
                        .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 0.137));")
                        .await
                        .unwrap();
                    for i in 0..10_000_i64 {
                        black_box(
                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    conn.execute("COMMIT;").await.unwrap();
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── bd-72im: Read-heavy SELECT workload (WHERE/aggregates/GROUP BY) ──

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_read_heavy_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_heavy_select_1k");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("csqlite", |b| {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, tag TEXT, val INTEGER);",
        )
        .unwrap();
        {
            let mut stmt = conn
                .prepare(
                    "INSERT INTO t VALUES (?1, ('name_' || ?1), \
                     ('tag_' || (?1 % 10)), (?1 * 7));",
                )
                .unwrap();
            for i in 0..1000_i64 {
                stmt.execute(rusqlite::params![i]).unwrap();
            }
        }

        let mut q_range = conn
            .prepare("SELECT * FROM t WHERE id BETWEEN 100 AND 200")
            .unwrap();
        let mut q_agg = conn
            .prepare("SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM t")
            .unwrap();
        let mut q_group = conn
            .prepare("SELECT tag, COUNT(*) FROM t GROUP BY tag")
            .unwrap();
        let mut q_compound = conn
            .prepare("SELECT * FROM t WHERE val > 3500 AND val < 5000")
            .unwrap();

        b.iter(|| {
            let rows: Vec<(i64, String)> = q_range
                .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            black_box(&rows);
            assert_eq!(rows.len(), 101);

            let count: i64 = q_agg.query_row([], |row| row.get(0)).unwrap();
            black_box(count);
            assert_eq!(count, 1000);

            let groups: Vec<(String, i64)> = q_group
                .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            black_box(&groups);
            assert_eq!(groups.len(), 10);

            let rows: Vec<i64> = q_compound
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            black_box(&rows);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
        fsqlite_e2e::block_on(
            conn.execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, tag TEXT, val INTEGER);",
            ),
        )
        .unwrap();
        {
            let stmt = fsqlite_e2e::block_on(conn.prepare(
                "INSERT INTO t VALUES (?1, ('name_' || ?1), \
                     ('tag_' || (?1 % 10)), (?1 * 7));",
            ))
            .unwrap();
            for i in 0..1000_i64 {
                fsqlite_e2e::block_on(stmt.execute_with_params(&[SqliteValue::Integer(i)]))
                    .unwrap();
            }
        }
        let q_range =
            fsqlite_e2e::block_on(conn.prepare("SELECT * FROM t WHERE id BETWEEN 100 AND 200"))
                .unwrap();
        let q_agg = fsqlite_e2e::block_on(
            conn.prepare("SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM t"),
        )
        .unwrap();
        let q_group =
            fsqlite_e2e::block_on(conn.prepare("SELECT tag, COUNT(*) FROM t GROUP BY tag"))
                .unwrap();
        let q_compound =
            fsqlite_e2e::block_on(conn.prepare("SELECT * FROM t WHERE val > 3500 AND val < 5000"))
                .unwrap();

        b.iter(|| {
            // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
            fsqlite_e2e::block_on(async {
                let rows = q_range.query().await.unwrap();
                black_box(&rows);
                assert_eq!(rows.len(), 101);

                let agg = q_agg.query().await.unwrap();
                black_box(&agg);
                assert_eq!(agg.len(), 1);

                let groups = q_group.query().await.unwrap();
                black_box(&groups);
                assert_eq!(groups.len(), 10);

                let rows = q_compound.query().await.unwrap();
                black_box(&rows);
            });
        });
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_exists_subquery_100k(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_exists_subquery_count_100k");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(1));

    let expected_count = SUBQUERY_ROWS_100K / 2;
    let sql = "SELECT COUNT(*) FROM products p WHERE EXISTS (SELECT 1 FROM product_flags f WHERE f.product_id = p.id)";

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_exists_regression_bench(SUBQUERY_ROWS_100K);
        let mut stmt = conn.prepare(sql).unwrap();
        b.iter(|| {
            let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
            black_box(count);
            assert_eq!(count, expected_count);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_exists_regression_bench(SUBQUERY_ROWS_100K);
        let stmt = fsqlite_e2e::block_on(conn.prepare(sql)).unwrap();
        b.iter(|| {
            let row = fsqlite_e2e::block_on(stmt.query_row()).unwrap();
            black_box(&row);
            assert_eq!(row.values()[0], SqliteValue::Integer(expected_count));
        });
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_in_subquery_100k(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_in_subquery_count_100k");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(1));

    let expected_count = SUBQUERY_ROWS_100K;
    let sql = "SELECT COUNT(*) FROM products WHERE id IN (SELECT id FROM selected_ids)";

    group.bench_function("csqlite", |b| {
        let conn = setup_csqlite_in_regression_bench(SUBQUERY_ROWS_100K);
        let mut stmt = conn.prepare(sql).unwrap();
        b.iter(|| {
            let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
            black_box(count);
            assert_eq!(count, expected_count);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = setup_fsqlite_in_regression_bench(SUBQUERY_ROWS_100K);
        let stmt = fsqlite_e2e::block_on(conn.prepare(sql)).unwrap();
        b.iter(|| {
            let row = fsqlite_e2e::block_on(stmt.query_row()).unwrap();
            black_box(&row);
            assert_eq!(row.values()[0], SqliteValue::Integer(expected_count));
        });
    });

    group.finish();
}

// ─── bd-1fez: Mixed OLTP workload (80% read / 20% write) ─────────────

#[allow(clippy::too_many_lines)]
// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_mixed_oltp(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_oltp_80r_20w");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);",
                )
                .unwrap();
                {
                    let mut stmt = conn
                        .prepare("INSERT INTO t VALUES (?1, ('name_' || ?1), (?1 * 3));")
                        .unwrap();
                    for i in 0..500_i64 {
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                }
                conn
            },
            |conn| {
                let mut next_id = 500_i64;
                for op in 0_i64..100 {
                    if op % 5 == 0 {
                        if op % 10 == 0 {
                            black_box(
                                conn.execute(
                                    "INSERT INTO t VALUES (?1, 'new', ?1)",
                                    rusqlite::params![next_id],
                                )
                                .unwrap(),
                            );
                            next_id += 1;
                        } else {
                            black_box(
                                conn.execute(
                                    "UPDATE t SET val = ?1 WHERE id = ?2",
                                    rusqlite::params![op * 100, op],
                                )
                                .unwrap(),
                            );
                        }
                    } else {
                        let target = (op * 5) % 500;
                        let value: i64 = conn
                            .query_row(
                                "SELECT val FROM t WHERE id = ?1",
                                rusqlite::params![target],
                                |row| row.get(0),
                            )
                            .unwrap();
                        black_box(value);
                    }
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(
                    conn.execute(
                        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);",
                    ),
                )
                .unwrap();
                let insert_stmt = fsqlite_e2e::block_on(
                    conn.prepare("INSERT INTO t VALUES (?1, ('name_' || ?1), (?1 * 3));"),
                )
                .unwrap();
                for i in 0..500_i64 {
                    fsqlite_e2e::block_on(
                        insert_stmt.execute_with_params(&[SqliteValue::Integer(i)]),
                    )
                    .unwrap();
                }
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    let mut next_id = 500_i64;
                    for op in 0..100_i64 {
                        if op % 5 == 0 {
                            if op % 10 == 0 {
                                black_box(
                                    conn.execute_with_params(
                                        "INSERT INTO t VALUES (?1, 'new', ?1)",
                                        &[SqliteValue::Integer(next_id)],
                                    )
                                    .await
                                    .unwrap(),
                                );
                                next_id += 1;
                            } else {
                                black_box(
                                    conn.execute_with_params(
                                        "UPDATE t SET val = ?1 WHERE id = ?2",
                                        &[SqliteValue::Integer(op * 100), SqliteValue::Integer(op)],
                                    )
                                    .await
                                    .unwrap(),
                                );
                            }
                        } else {
                            let target = (op * 5) % 500;
                            let row = conn
                                .query_row_with_params(
                                    "SELECT val FROM t WHERE id = ?1",
                                    &[SqliteValue::Integer(target)],
                                )
                                .await
                                .unwrap();
                            black_box(&row);
                            assert_eq!(row.values().len(), 1);
                        }
                    }
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── bd-6d9v: Large transaction (100K / 1M row single-txn insert) ────

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_large_txn_100k(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_txn_100k");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, val INTEGER);",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN;").unwrap();
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 3));")
                    .unwrap();
                for i in 0..100_000_i64 {
                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                }
                conn.execute_batch("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, val INTEGER);",
                ))
                .unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN;").await.unwrap();
                    let stmt = conn
                        .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 3));")
                        .await
                        .unwrap();
                    for i in 0..100_000_i64 {
                        black_box(
                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    conn.execute("COMMIT;").await.unwrap();
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=sequential
fn bench_large_txn_1m(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_txn_1m");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    group.bench_function("csqlite", |b| {
        b.iter_batched(
            || {
                let conn = rusqlite::Connection::open_in_memory().unwrap();
                conn.execute_batch(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, val INTEGER);",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute_batch("BEGIN;").unwrap();
                let mut stmt = conn
                    .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 3));")
                    .unwrap();
                for i in 0..1_000_000_i64 {
                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                }
                conn.execute_batch("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
                fsqlite_e2e::block_on(conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, val INTEGER);",
                ))
                .unwrap();
                conn
            },
            |conn| {
                // bd-mnlk2 / bd-zavyn: one runtime entry per timed sample.
                fsqlite_e2e::block_on(async {
                    conn.execute("BEGIN;").await.unwrap();
                    let stmt = conn
                        .prepare("INSERT INTO bench VALUES (?1, ('data_' || ?1), (?1 * 3));")
                        .await
                        .unwrap();
                    for i in 0..1_000_000_i64 {
                        black_box(
                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                .await
                                .unwrap(),
                        );
                    }
                    conn.execute("COMMIT;").await.unwrap();
                });
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── bd-3rze: Concurrent write throughput (1/2/4/8 threads) ──────────
//
// Each thread creates its own in-memory database and inserts 1000 rows
// in a single transaction.  FrankenSQLite's Connection uses Rc (not Send),
// so connections must be created inside each thread.
// This is an independent-database scaling control, not shared-database
// concurrent-writer evidence.

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=memory, concurrency=concurrent
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=memory, concurrency=concurrent
fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_write_throughput");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    for &threads in &[1_usize, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("csqlite", threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    std::thread::scope(|s| {
                        for _ in 0..threads {
                            s.spawn(|| {
                                let conn = rusqlite::Connection::open_in_memory().unwrap();
                                conn.execute_batch(
                                    "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);",
                                )
                                .unwrap();
                                conn.execute_batch("BEGIN;").unwrap();
                                let mut stmt = conn
                                    .prepare("INSERT INTO t VALUES (?1, ('v' || ?1));")
                                    .unwrap();
                                for i in 0..1000_i64 {
                                    black_box(stmt.execute(rusqlite::params![i]).unwrap());
                                }
                                conn.execute_batch("COMMIT;").unwrap();
                            });
                        }
                    });
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("frankensqlite", threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    std::thread::scope(|s| {
                        for _ in 0..threads {
                            // bd-mnlk2 / bd-zavyn: one runtime entry per
                            // thread transaction — a per-row block_on paid
                            // ~333 ns x 1000 on the FrankenSQLite arm only.
                            s.spawn(|| {
                                fsqlite_e2e::block_on(async {
                                    let conn = fsqlite::Connection::open(":memory:").await.unwrap();
                                    conn.execute(
                                        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);",
                                    )
                                    .await
                                    .unwrap();
                                    conn.execute("BEGIN;").await.unwrap();
                                    let stmt = conn
                                        .prepare("INSERT INTO t VALUES (?1, ('v' || ?1));")
                                        .await
                                        .unwrap();
                                    for i in 0..1000_i64 {
                                        black_box(
                                            stmt.execute_with_params(&[SqliteValue::Integer(i)])
                                                .await
                                                .unwrap(),
                                        );
                                    }
                                    conn.execute("COMMIT;").await.unwrap();
                                });
                            });
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = criterion_config();
    targets =
        bench_sequential_inserts,
        bench_sequential_inserts_single_txn,
        bench_bulk_inserts,
        bench_select_queries,
        bench_mixed_dml,
        bench_write_throughput_autocommit,
        bench_write_throughput_batched,
        bench_write_throughput_single_txn,
        bench_read_heavy_select,
        bench_exists_subquery_100k,
        bench_in_subquery_100k,
        bench_mixed_oltp,
        bench_large_txn_100k,
        bench_large_txn_1m,
        bench_concurrent_writes
);
criterion_main!(benches);
