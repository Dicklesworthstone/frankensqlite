use std::env;
use std::hint::black_box;
use std::time::Instant;

use asupersync::runtime::{Runtime, RuntimeBuilder};
use fsqlite_core::connection::{Connection, hot_path_profile_snapshot, reset_hot_path_profile};
use fsqlite_types::SqliteValue;
use tempfile::NamedTempFile;

const INSERT_SQL: &str = "INSERT INTO bench (id, payload) VALUES (?1, ?2)";
const SELECT_COUNT_SUM_SQL: &str = "SELECT COUNT(*), SUM(score) FROM select_bench";
const SELECT_COVERING_INDEX_SQL: &str = "SELECT name FROM select_bench WHERE name = ?1";
const SELECT_INDEXED_EQUALITY_SQL: &str = "SELECT * FROM select_bench WHERE name = ?1";
const COUNT_INDEXED_ROWID_PROBE_SQL: &str =
    "SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id <= 5)";
const PARAM_NULL_PREDICATE_MIX_SQL: &str =
    "SELECT CASE WHEN ?1 IS NOT NULL THEN (?2 + ?3) ELSE ?4 END";

/// One runtime for the whole benchmark binary: built once in `main`, before any measured
/// region, so no runtime construction is ever attributed to a timed loop.
fn benchmark_runtime() -> Runtime {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("prepared-cache-hot-paths benchmark runtime should build")
}

/// Pragmas that shape the fixture connection. These MUST be awaited: `execute` returns a
/// future, so `let _ = conn.execute(pragma)` would drop it unpolled and silently apply
/// nothing (bd-fd1ra).
const FIXTURE_PRAGMAS: [&str; 5] = [
    "PRAGMA page_size = 4096;",
    "PRAGMA journal_mode = WAL;",
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA cache_size = -64000;",
    "PRAGMA fsqlite_capture_time_travel_snapshots=false;",
];

async fn apply_fixture_pragmas(conn: &Connection) {
    for pragma in FIXTURE_PRAGMAS {
        conn.execute(pragma).await.ok();
    }
}

async fn open_mt_mvcc_prepare_conn() -> (Connection, NamedTempFile) {
    let tmp = NamedTempFile::new().expect("tempfile");
    let path = tmp
        .path()
        .to_str()
        .expect("tempfile path must be utf-8")
        .to_owned();
    let conn = Connection::open(path).await.expect("open connection");
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, payload TEXT);")
        .await
        .expect("create table");
    conn.execute("BEGIN;").await.expect("begin transaction");
    (conn, tmp)
}

async fn bench_mt_mvcc_prepare_hit(iterations: u64) -> f64 {
    let (conn, _tmp) = open_mt_mvcc_prepare_conn().await;
    let warmed = conn.prepare(INSERT_SQL).await.expect("warm prepare");
    black_box(&warmed);

    let start = Instant::now();
    for _ in 0..iterations {
        let stmt = conn
            .prepare(black_box(INSERT_SQL))
            .await
            .expect("prepare hit");
        black_box(stmt);
    }
    start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64
}

async fn bench_mt_mvcc_prepare_then_execute_cycle(iterations: u64) -> f64 {
    let (conn, _tmp) = open_mt_mvcc_prepare_conn().await;
    let warmed = conn.prepare(INSERT_SQL).await.expect("warm prepare");
    let warmed_params = [
        SqliteValue::Integer(0),
        SqliteValue::Text(String::from("warmup").into()),
    ];
    warmed
        .execute_with_params(&warmed_params)
        .await
        .expect("warm execute");
    black_box(&warmed);

    let start = Instant::now();
    for row_id in 1..=iterations {
        let stmt = conn
            .prepare(black_box(INSERT_SQL))
            .await
            .expect("prepare hit");
        let params = [
            SqliteValue::Integer(i64::try_from(row_id).expect("row id fits i64")),
            SqliteValue::Text(format!("payload_{row_id}").into()),
        ];
        let inserted = stmt.execute_with_params(&params).await.expect("execute");
        black_box(inserted);
    }
    start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64
}

async fn open_prepared_select_fast_path_conn_with_count(count: i64) -> Connection {
    let conn = Connection::open(":memory:")
        .await
        .expect("open memory connection");
    apply_fixture_pragmas(&conn).await;
    conn.execute(
        "CREATE TABLE select_bench (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER NOT NULL
        );",
    )
    .await
    .expect("create select bench table");
    conn.execute("BEGIN;")
        .await
        .expect("begin select bench seed");
    let insert = conn
        .prepare("INSERT INTO select_bench VALUES (?1, ?2, ?3)")
        .await
        .expect("prepare select bench insert");
    for id in 1..=count {
        insert
            .execute_with_params(&[
                SqliteValue::Integer(id),
                SqliteValue::Text(format!("name_{id}").into()),
                SqliteValue::Integer(id * 7),
            ])
            .await
            .expect("seed select bench row");
    }
    conn.execute("COMMIT;")
        .await
        .expect("commit select bench seed");
    conn.execute("CREATE INDEX select_bench_name ON select_bench(name);")
        .await
        .expect("create select bench index");
    conn
}

async fn open_prepared_select_fast_path_conn() -> Connection {
    open_prepared_select_fast_path_conn_with_count(64).await
}

async fn bench_prepared_select_fast_path_pair(iterations: u64) -> f64 {
    let conn = open_prepared_select_fast_path_conn().await;
    let count_sum = conn
        .prepare(SELECT_COUNT_SUM_SQL)
        .await
        .expect("prepare count/sum");
    let covering_index = conn
        .prepare(SELECT_COVERING_INDEX_SQL)
        .await
        .expect("prepare covering indexed equality");
    let probe = [SqliteValue::Text("name_32".into())];
    black_box(count_sum.query_row().await.expect("warm count/sum"));
    black_box(
        covering_index
            .query_with_params(&probe)
            .await
            .expect("warm covering indexed equality"),
    );

    let start = Instant::now();
    for _ in 0..iterations {
        black_box(count_sum.query_row().await.expect("count/sum fast path"));
        black_box(
            covering_index
                .query_with_params(&probe)
                .await
                .expect("covering indexed equality fast path"),
        );
    }
    start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64
}

async fn bench_prepared_indexed_equality_query(iterations: u64, count: i64) -> f64 {
    let conn = open_prepared_select_fast_path_conn_with_count(count).await;
    let indexed_equality = conn
        .prepare(SELECT_INDEXED_EQUALITY_SQL)
        .await
        .expect("prepare indexed equality");
    let probe = [SqliteValue::Text(format!("name_{}", count / 2).into())];
    black_box(
        indexed_equality
            .query_with_params(&probe)
            .await
            .expect("warm indexed equality"),
    );
    black_box(
        indexed_equality
            .query_with_params(&probe)
            .await
            .expect("warm cached indexed equality"),
    );

    let start = Instant::now();
    let mut row_count = 0_usize;
    for _ in 0..iterations {
        let rows = indexed_equality
            .query_with_params(&probe)
            .await
            .expect("indexed equality fast path");
        row_count = row_count.saturating_add(rows.len());
        black_box(rows);
    }
    black_box(row_count);
    start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64
}

async fn open_e2e_read_indexed_equality_shape_conn(count: i64) -> Connection {
    let conn = Connection::open(":memory:")
        .await
        .expect("open memory connection");
    apply_fixture_pragmas(&conn).await;
    conn.execute(
        "CREATE TABLE bench (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL NOT NULL
        );",
    )
    .await
    .expect("create e2e-shaped bench table");
    conn.execute("BEGIN;").await.expect("begin e2e-shaped seed");
    let insert = conn
        .prepare("INSERT INTO bench VALUES (?1, ('user_' || ?1), (?1 * 0.137))")
        .await
        .expect("prepare e2e-shaped insert");
    for id in 0..count {
        insert
            .execute_with_params(&[SqliteValue::Integer(id)])
            .await
            .expect("seed e2e-shaped row");
    }
    conn.execute("COMMIT;")
        .await
        .expect("commit e2e-shaped seed");
    conn.execute("CREATE INDEX idx_name ON bench(name);")
        .await
        .expect("create e2e-shaped name index");
    conn
}

async fn bench_prepared_indexed_equality_e2e_shape_query(
    iterations: u64,
    count: i64,
) -> (f64, u64) {
    let conn = open_e2e_read_indexed_equality_shape_conn(count).await;
    let indexed_equality = conn
        .prepare("SELECT * FROM bench WHERE name = ?1")
        .await
        .expect("prepare e2e-shaped indexed equality");
    let probe = [SqliteValue::Text(format!("user_{}", count / 2).into())];
    black_box(
        indexed_equality
            .query_with_params(&probe)
            .await
            .expect("warm e2e-shaped indexed equality"),
    );
    black_box(
        indexed_equality
            .query_with_params(&probe)
            .await
            .expect("warm cached e2e-shaped indexed equality"),
    );

    reset_hot_path_profile();
    let start = Instant::now();
    let mut row_count = 0_usize;
    for _ in 0..iterations {
        let rows = indexed_equality
            .query_with_params(&probe)
            .await
            .expect("e2e-shaped indexed equality fast path");
        row_count = row_count.saturating_add(rows.len());
        black_box(rows);
    }
    black_box(row_count);
    let elapsed_ns = start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64;
    let profile = hot_path_profile_snapshot();
    (elapsed_ns, profile.direct_indexed_equality_query_hits)
}

async fn open_prepared_count_indexed_rowid_probe_conn(count: i64) -> Connection {
    let conn = Connection::open(":memory:")
        .await
        .expect("open memory connection");
    apply_fixture_pragmas(&conn).await;
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL,
            category_id INTEGER
        );",
    )
    .await
    .expect("create products table");
    conn.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT);")
        .await
        .expect("create categories table");
    conn.execute("BEGIN;")
        .await
        .expect("begin fixture transaction");
    let cat_count = (count / 20).max(5);
    for id in 1..=cat_count {
        conn.execute(&format!("INSERT INTO categories VALUES ({id}, 'cat_{id}')"))
            .await
            .expect("seed category row");
    }
    for id in 1..=count {
        let cid = (id % cat_count) + 1;
        let price = id as f64 * 3.14;
        conn.execute(&format!(
            "INSERT INTO products VALUES ({id}, 'prod_{id}', {price}, {cid})"
        ))
        .await
        .expect("seed product row");
    }
    conn.execute("COMMIT;")
        .await
        .expect("commit fixture transaction");
    conn.execute("CREATE INDEX idx_prod_cat ON products(category_id);")
        .await
        .expect("create products category index");
    conn
}

async fn bench_prepared_count_indexed_rowid_probe_query_row(iterations: u64, count: i64) -> f64 {
    let conn = open_prepared_count_indexed_rowid_probe_conn(count).await;
    let stmt = conn
        .prepare(COUNT_INDEXED_ROWID_PROBE_SQL)
        .await
        .expect("prepare count indexed rowid probe");
    black_box(
        stmt.query_row()
            .await
            .expect("warm count indexed rowid probe"),
    );
    black_box(
        stmt.query_row()
            .await
            .expect("warm cached count indexed rowid probe"),
    );

    let start = Instant::now();
    let mut count_sum = 0_i64;
    for _ in 0..iterations {
        let row = stmt
            .query_row()
            .await
            .expect("count indexed rowid probe fast path");
        let Some(SqliteValue::Integer(count)) = row.get(0) else {
            eprintln!("count indexed rowid probe returned non-integer row: {row:?}");
            std::process::exit(3);
        };
        count_sum = count_sum.saturating_add(*count);
    }
    black_box(count_sum);
    start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64
}

async fn bench_prepared_param_null_predicate_mix(iterations: u64) -> (f64, u64) {
    let conn = Connection::open(":memory:")
        .await
        .expect("open memory connection");
    let stmt = conn
        .prepare(PARAM_NULL_PREDICATE_MIX_SQL)
        .await
        .expect("prepare parameter/null predicate mix");
    let non_null_params = [
        SqliteValue::Integer(1),
        SqliteValue::Integer(2),
        SqliteValue::Integer(3),
        SqliteValue::Integer(100),
    ];
    let null_params = [
        SqliteValue::Null,
        SqliteValue::Integer(2),
        SqliteValue::Integer(3),
        SqliteValue::Integer(100),
    ];
    black_box(
        stmt.query_with_params(&non_null_params)
            .await
            .expect("warm non-null branch"),
    );
    black_box(
        stmt.query_with_params(&null_params)
            .await
            .expect("warm null branch"),
    );

    let start = Instant::now();
    let mut checksum = 0_u64;
    let mut use_non_null_params = true;
    for iteration in 0..iterations {
        let params = if use_non_null_params {
            &non_null_params
        } else {
            &null_params
        };
        let rows = stmt
            .query_with_params(params)
            .await
            .expect("parameter/null predicate query");
        let row = rows
            .first()
            .expect("parameter/null predicate should return one row");
        let Some(SqliteValue::Integer(value)) = row.get(0) else {
            eprintln!("parameter/null predicate returned non-integer row: {row:?}");
            std::process::exit(3);
        };
        checksum = checksum.wrapping_add(
            u64::try_from(*value).expect("benchmark result is positive") * (iteration + 1),
        );
        black_box(rows);
        use_non_null_params = !use_non_null_params;
    }
    let elapsed_ns = start.elapsed().as_secs_f64() * 1_000_000_000.0 / iterations as f64;
    (elapsed_ns, checksum)
}

async fn parse_iterations() -> u64 {
    let mut args = env::args().skip(1);
    let mut iterations = 2_000_000_u64;
    let mut filter = None;
    while let Some(arg) = args.next() {
        if arg == "--iterations" {
            if let Some(value) = args.next() {
                match value.parse() {
                    Ok(parsed) => iterations = parsed,
                    Err(_) => {
                        eprintln!("invalid --iterations value: {value}");
                        std::process::exit(2);
                    }
                }
            }
        } else if arg == "--filter" {
            filter = args.next();
        }
    }
    if let Some(filter) = filter {
        match filter.as_str() {
            "prepare_hit" => {
                let prepare_hit_ns = bench_mt_mvcc_prepare_hit(iterations).await;
                println!(
                    "prepared_cache_hot_paths mt_mvcc_prepare_hit_ns_per_op={prepare_hit_ns:.2} iterations={iterations}"
                );
                std::process::exit(0);
            }
            "prepare_execute" => {
                let prepare_execute_ns =
                    bench_mt_mvcc_prepare_then_execute_cycle(iterations.min(200_000)).await;
                println!(
                    "prepared_cache_hot_paths mt_mvcc_prepare_then_execute_cycle_ns_per_op={prepare_execute_ns:.2} iterations={}",
                    iterations.min(200_000)
                );
                std::process::exit(0);
            }
            "select_fast_paths" => {
                let select_fast_paths_ns =
                    bench_prepared_select_fast_path_pair(iterations.min(200_000)).await;
                println!(
                    "prepared_cache_hot_paths select_count_sum_plus_covering_index_ns_per_pair={select_fast_paths_ns:.2} iterations={}",
                    iterations.min(200_000)
                );
                std::process::exit(0);
            }
            "count_indexed_rowid_probe" => {
                let count_indexed_rowid_probe_ns =
                    bench_prepared_count_indexed_rowid_probe_query_row(
                        iterations.min(200_000),
                        1_000,
                    )
                    .await;
                println!(
                    "prepared_cache_hot_paths count_indexed_rowid_probe_query_row_ns_per_op={count_indexed_rowid_probe_ns:.2} rows=1000 iterations={}",
                    iterations.min(200_000)
                );
                std::process::exit(0);
            }
            "indexed_equality_100k" => {
                let indexed_equality_ns =
                    bench_prepared_indexed_equality_query(iterations.min(200_000), 100_000).await;
                println!(
                    "prepared_cache_hot_paths indexed_equality_query_ns_per_op={indexed_equality_ns:.2} rows=100000 iterations={}",
                    iterations.min(200_000)
                );
                std::process::exit(0);
            }
            "indexed_equality_e2e_shape_100k" => {
                let iterations = iterations.min(200_000);
                let (indexed_equality_ns, direct_hits) =
                    bench_prepared_indexed_equality_e2e_shape_query(iterations, 100_000).await;
                println!(
                    "prepared_cache_hot_paths indexed_equality_e2e_shape_query_ns_per_op={indexed_equality_ns:.2} rows=100000 iterations={iterations} direct_indexed_equality_hits={direct_hits}"
                );
                std::process::exit(0);
            }
            "param_null_predicate_mix" => {
                let iterations = iterations.min(200_000);
                let (param_null_predicate_ns, checksum) =
                    bench_prepared_param_null_predicate_mix(iterations).await;
                println!(
                    "prepared_cache_hot_paths param_null_predicate_mix_query_ns_per_op={param_null_predicate_ns:.2} iterations={iterations} checksum={checksum}"
                );
                std::process::exit(0);
            }
            _ => {
                eprintln!("invalid --filter value: {filter}");
                std::process::exit(2);
            }
        }
    }
    iterations
}

fn main() {
    // ONE runtime for the entire benchmark, built before any measured region.
    let runtime = benchmark_runtime();
    runtime.block_on(run());
}

async fn run() {
    let iterations = parse_iterations().await;
    let prepare_hit_ns = bench_mt_mvcc_prepare_hit(iterations).await;
    let prepare_execute_ns =
        bench_mt_mvcc_prepare_then_execute_cycle(iterations.min(200_000)).await;
    let select_fast_paths_ns = bench_prepared_select_fast_path_pair(iterations.min(200_000)).await;
    let count_indexed_rowid_probe_ns =
        bench_prepared_count_indexed_rowid_probe_query_row(iterations.min(200_000), 1_000).await;
    let (param_null_predicate_ns, checksum) =
        bench_prepared_param_null_predicate_mix(iterations.min(200_000)).await;

    println!(
        "prepared_cache_hot_paths mt_mvcc_prepare_hit_ns_per_op={prepare_hit_ns:.2} iterations={iterations}"
    );
    println!(
        "prepared_cache_hot_paths mt_mvcc_prepare_then_execute_cycle_ns_per_op={prepare_execute_ns:.2} iterations={}",
        iterations.min(200_000)
    );
    println!(
        "prepared_cache_hot_paths select_count_sum_plus_covering_index_ns_per_pair={select_fast_paths_ns:.2} iterations={}",
        iterations.min(200_000)
    );
    println!(
        "prepared_cache_hot_paths count_indexed_rowid_probe_query_row_ns_per_op={count_indexed_rowid_probe_ns:.2} rows=1000 iterations={}",
        iterations.min(200_000)
    );
    println!(
        "prepared_cache_hot_paths param_null_predicate_mix_query_ns_per_op={param_null_predicate_ns:.2} iterations={} checksum={checksum}",
        iterations.min(200_000)
    );
}
