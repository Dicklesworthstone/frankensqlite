use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};

fn criterion_config() -> Criterion {
    Criterion::default().configure_from_args()
}

// ─── Sequential INSERT benchmark (100 rows) ────────────────────────────

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
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
                    .unwrap();
                conn
            },
            |conn| {
                for i in 0..100 {
                    conn.execute_with_params(
                        "INSERT INTO t VALUES (?, 'val');",
                        &[fsqlite_types::value::SqliteValue::Integer(i)],
                    )
                    .unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── Bulk INSERT benchmark (1000 rows) ──────────────────────────────────

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
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value REAL);")
                    .unwrap();
                conn
            },
            |conn| {
                conn.execute("BEGIN;").unwrap();
                for i in 0..1000 {
                    let val = f64::from(i) * 1.1;
                    conn.execute(&format!("INSERT INTO t VALUES ({i}, 'name_{i}', {val});"))
                        .unwrap();
                }
                conn.execute("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── SELECT query benchmark ─────────────────────────────────────────────

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
            assert_eq!(rows.len(), 50);
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = fsqlite::Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
            .unwrap();
        for i in 0..100 {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}');"))
                .unwrap();
        }

        b.iter(|| {
            let rows = conn
                .query("SELECT * FROM t WHERE id >= 25 AND id < 75;")
                .unwrap();
            assert_eq!(rows.len(), 50);
        });
    });

    group.finish();
}

// ─── Mixed DML benchmark ────────────────────────────────────────────────

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
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                }
                {
                    let mut stmt = conn.prepare("DELETE FROM t WHERE id = ?1;").unwrap();
                    for i in 150..200_i64 {
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                }
                {
                    let mut stmt = conn
                        .prepare("INSERT INTO t VALUES (?1, ('new_' || ?1), ?1);")
                        .unwrap();
                    for i in 200..250_i64 {
                        stmt.execute(rusqlite::params![i]).unwrap();
                    }
                }
                let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
                let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
                assert_eq!(count, 200);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, n INTEGER);")
                    .unwrap();
                for i in 0..200 {
                    conn.execute(&format!(
                        "INSERT INTO t VALUES ({i}, 'val_{i}', {});",
                        i * 10
                    ))
                    .unwrap();
                }
                conn
            },
            |conn| {
                for i in 0..50 {
                    conn.execute(&format!("UPDATE t SET n = {} WHERE id = {i};", i * 100))
                        .unwrap();
                }
                for i in 150..200 {
                    conn.execute(&format!("DELETE FROM t WHERE id = {i};"))
                        .unwrap();
                }
                for i in 200..250 {
                    conn.execute(&format!("INSERT INTO t VALUES ({i}, 'new_{i}', {i});"))
                        .unwrap();
                }
                let rows = conn.query("SELECT count(*) FROM t;").unwrap();
                let count = &rows[0].values()[0];
                assert_eq!(*count, fsqlite_types::value::SqliteValue::Integer(200));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── bd-1dus: Sequential write throughput (10K rows, 3 variants) ────────

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
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);")
                    .unwrap();
                conn
            },
            |conn| {
                for i in 0..10_000 {
                    let val = f64::from(i) * 0.137;
                    conn.execute(&format!(
                        "INSERT INTO bench VALUES ({i}, 'data_{i}', {val});"
                    ))
                    .unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

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
                        stmt.execute(rusqlite::params![i]).unwrap();
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
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);")
                    .unwrap();
                conn
            },
            |conn| {
                for batch in 0..10 {
                    conn.execute("BEGIN;").unwrap();
                    let base = batch * 1000;
                    for i in base..base + 1000 {
                        let val = f64::from(i) * 0.137;
                        conn.execute(&format!(
                            "INSERT INTO bench VALUES ({i}, 'data_{i}', {val});"
                        ))
                        .unwrap();
                    }
                    conn.execute("COMMIT;").unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

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
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, value REAL);")
                    .unwrap();
                conn
            },
            |conn| {
                conn.execute("BEGIN;").unwrap();
                for i in 0..10_000 {
                    let val = f64::from(i) * 0.137;
                    conn.execute(&format!(
                        "INSERT INTO bench VALUES ({i}, 'data_{i}', {val});"
                    ))
                    .unwrap();
                }
                conn.execute("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── bd-72im: Read-heavy SELECT workload (WHERE/aggregates/GROUP BY) ──

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
            assert_eq!(rows.len(), 101);

            let count: i64 = q_agg.query_row([], |row| row.get(0)).unwrap();
            assert_eq!(count, 1000);

            let groups: Vec<(String, i64)> = q_group
                .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(groups.len(), 10);

            let _rows: Vec<i64> = q_compound
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        });
    });

    group.bench_function("frankensqlite", |b| {
        let conn = fsqlite::Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, tag TEXT, val INTEGER);")
            .unwrap();
        for i in 0..1000 {
            let tag_idx = i % 10;
            let val = i * 7;
            conn.execute(&format!(
                "INSERT INTO t VALUES ({i}, 'name_{i}', 'tag_{tag_idx}', {val});"
            ))
            .unwrap();
        }

        b.iter(|| {
            let rows = conn
                .query("SELECT * FROM t WHERE id BETWEEN 100 AND 200;")
                .unwrap();
            assert_eq!(rows.len(), 101);

            let agg = conn
                .query("SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM t;")
                .unwrap();
            assert_eq!(agg.len(), 1);

            let groups = conn
                .query("SELECT tag, COUNT(*) FROM t GROUP BY tag;")
                .unwrap();
            assert_eq!(groups.len(), 10);

            let _ = conn
                .query("SELECT * FROM t WHERE val > 3500 AND val < 5000;")
                .unwrap();
        });
    });

    group.finish();
}

// ─── bd-1fez: Mixed OLTP workload (80% read / 20% write) ─────────────

#[allow(clippy::too_many_lines)]
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
                            conn.execute(
                                "INSERT INTO t VALUES (?1, 'new', ?1)",
                                rusqlite::params![next_id],
                            )
                            .unwrap();
                            next_id += 1;
                        } else {
                            conn.execute(
                                "UPDATE t SET val = ?1 WHERE id = ?2",
                                rusqlite::params![op * 100, op],
                            )
                            .unwrap();
                        }
                    } else {
                        let target = (op * 5) % 500;
                        let _: i64 = conn
                            .query_row(
                                "SELECT val FROM t WHERE id = ?1",
                                rusqlite::params![target],
                                |row| row.get(0),
                            )
                            .unwrap();
                    }
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER);")
                    .unwrap();
                for i in 0..500 {
                    let val = i * 3;
                    conn.execute(&format!("INSERT INTO t VALUES ({i}, 'name_{i}', {val});"))
                        .unwrap();
                }
                conn
            },
            |conn| {
                let mut next_id = 500;
                for op in 0..100 {
                    if op % 5 == 0 {
                        if op % 10 == 0 {
                            conn.execute(&format!(
                                "INSERT INTO t VALUES ({next_id}, 'new_{next_id}', {next_id});"
                            ))
                            .unwrap();
                            next_id += 1;
                        } else {
                            conn.execute(&format!(
                                "UPDATE t SET val = {} WHERE id = {op};",
                                op * 100
                            ))
                            .unwrap();
                        }
                    } else {
                        let target = (op * 5) % 500;
                        let rows = conn
                            .query(&format!("SELECT val FROM t WHERE id = {target};"))
                            .unwrap();
                        assert_eq!(rows.len(), 1);
                    }
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ─── bd-6d9v: Large transaction (100K / 1M row single-txn insert) ────

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
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, val INTEGER);",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute("BEGIN;").unwrap();
                for i in 0..100_000 {
                    let val = i * 3;
                    conn.execute(&format!(
                        "INSERT INTO bench VALUES ({i}, 'data_{i}', {val});"
                    ))
                    .unwrap();
                }
                conn.execute("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

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
                    stmt.execute(rusqlite::params![i]).unwrap();
                }
                conn.execute_batch("COMMIT;").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("frankensqlite", |b| {
        b.iter_batched(
            || {
                let conn = fsqlite::Connection::open(":memory:").unwrap();
                conn.execute(
                    "CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT, val INTEGER);",
                )
                .unwrap();
                conn
            },
            |conn| {
                conn.execute("BEGIN;").unwrap();
                for i in 0..1_000_000 {
                    let val = i * 3;
                    conn.execute(&format!(
                        "INSERT INTO bench VALUES ({i}, 'data_{i}', {val});"
                    ))
                    .unwrap();
                }
                conn.execute("COMMIT;").unwrap();
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
                                    stmt.execute(rusqlite::params![i]).unwrap();
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
                            s.spawn(|| {
                                let conn = fsqlite::Connection::open(":memory:").unwrap();
                                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
                                    .unwrap();
                                conn.execute("BEGIN;").unwrap();
                                for i in 0..1000 {
                                    conn.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}');"))
                                        .unwrap();
                                }
                                conn.execute("COMMIT;").unwrap();
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
        bench_bulk_inserts,
        bench_select_queries,
        bench_mixed_dml,
        bench_write_throughput_autocommit,
        bench_write_throughput_batched,
        bench_write_throughput_single_txn,
        bench_read_heavy_select,
        bench_mixed_oltp,
        bench_large_txn_100k,
        bench_large_txn_1m,
        bench_concurrent_writes
);
criterion_main!(benches);
