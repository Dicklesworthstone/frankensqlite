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
#[cfg(feature = "bench-internals")]
use std::time::Instant;

use criterion::{Criterion, criterion_group};
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
use fsqlite_types::SqliteValue;
#[cfg(feature = "bench-internals")]
use fsqlite_types::value::{
    reset_small_text_direct_trait_hits_for_bench, set_small_text_direct_traits_for_bench,
    small_text_direct_trait_hits_for_bench,
};
#[cfg(feature = "bench-internals")]
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

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

#[cfg(feature = "bench-internals")]
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

fn setup_fsqlite() -> fsqlite::Connection {
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
    fsqlite_e2e::block_on(
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)"),
    )
    .unwrap();
    fsqlite_e2e::block_on(conn.execute("BEGIN")).unwrap();
    for i in 0..SEED_ROWS {
        fsqlite_e2e::block_on(conn.execute(&format!(
            "INSERT INTO bench VALUES ({i}, {}, 'label_{i:04}')",
            i * 17 + 31
        )))
        .unwrap();
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT")).unwrap();
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
    let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(path)).unwrap();
    fsqlite_e2e::block_on(
        conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)"),
    )
    .unwrap();
    fsqlite_e2e::block_on(conn.execute("BEGIN")).unwrap();
    let insert =
        fsqlite_e2e::block_on(conn.prepare("INSERT INTO bench VALUES (?1, ?2, ?3)")).unwrap();
    for id in 0..SEED_ROWS {
        fsqlite_e2e::block_on(insert.execute_with_params(&[
            SqliteValue::Integer(id),
            SqliteValue::Integer(id * 17 + 31),
            SqliteValue::Text(format!("label_{id:04}").into()),
        ]))
        .unwrap();
    }
    fsqlite_e2e::block_on(conn.execute("COMMIT")).unwrap();
    fsqlite_e2e::block_on(conn.execute("BEGIN")).unwrap();
    (conn, database)
}

#[cfg(feature = "bench-internals")]
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
            let _stmt = fsqlite_e2e::block_on(fconn.prepare(sql)).unwrap();
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

    let f_stmt =
        fsqlite_e2e::block_on(fconn.prepare("SELECT id, val FROM bench WHERE id = 500")).unwrap();
    let mut c_stmt = cconn
        .prepare("SELECT id, val FROM bench WHERE id = 500")
        .unwrap();

    group.bench_function("fsqlite", |b| {
        b.iter(|| {
            let rows = fsqlite_e2e::block_on(f_stmt.query()).unwrap();
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
            let rows = fsqlite_e2e::block_on(fconn.query(sql_point)).unwrap();
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
                let rows = fsqlite_e2e::block_on(
                    fconn.query(&format!("SELECT val FROM bench WHERE id = {key}")),
                )
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

// BENCH-META: engine=csqlite, lifecycle=prepared, storage=file, concurrency=sequential
// BENCH-META: engine=frankensqlite, lifecycle=prepared, storage=file, concurrency=sequential
fn bench_btree_seek_file_clustered_in(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/btree_seek_file_clustered_in");

    let (fconn, _fdatabase) = setup_fsqlite_file_backed();
    let (cconn, _cdatabase) = setup_csqlite_file_backed();
    let f_stmt = fsqlite_e2e::block_on(fconn.prepare(CLUSTERED_IN_SQL)).unwrap();
    let mut c_stmt = cconn.prepare(CLUSTERED_IN_SQL).unwrap();

    let expected_rows = fsqlite_e2e::block_on(f_stmt.query()).unwrap();
    let mut f_expected_ids: Vec<i64> = expected_rows
        .iter()
        .filter_map(|row| match row.values().first().cloned() {
            Some(SqliteValue::Integer(id)) => Some(id),
            _ => None,
        })
        .collect();
    f_expected_ids.sort_unstable();
    let mut c_expected_ids = c_stmt
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    c_expected_ids.sort_unstable();
    let exact_expected_ids = (480..=511).collect::<Vec<_>>();
    assert_eq!(f_expected_ids, exact_expected_ids);
    assert_eq!(c_expected_ids, exact_expected_ids);
    assert_eq!(f_expected_ids.iter().sum::<i64>(), 15_856);

    group.bench_function("fsqlite", |b| {
        b.iter(|| {
            let rows = fsqlite_e2e::block_on(f_stmt.query()).unwrap();
            assert_eq!(rows.len(), 32);
            black_box(rows);
        });
    });

    group.bench_function("csqlite", |b| {
        b.iter(|| {
            let ids = c_stmt
                .query_map([], |row| row.get::<_, i64>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
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
    let mut group = c.benchmark_group("pipeline/full_scan");

    let fconn = setup_fsqlite();
    let cconn = setup_csqlite();

    let sql = "SELECT id, val, label FROM bench ORDER BY id";

    group.bench_function("fsqlite", |b| {
        b.iter(|| {
            let rows = fsqlite_e2e::block_on(fconn.query(sql)).unwrap();
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
            let rows = fsqlite_e2e::block_on(fconn.query(sql)).unwrap();
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
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
        fsqlite_e2e::block_on(
            conn.execute("CREATE TABLE insert_bench (id INTEGER PRIMARY KEY, val INTEGER)"),
        )
        .unwrap();
        let mut counter = 0i64;
        b.iter(|| {
            counter += 1;
            fsqlite_e2e::block_on(conn.execute(&format!(
                "INSERT INTO insert_bench VALUES ({counter}, {counter})"
            )))
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

#[cfg(feature = "bench-internals")]
#[derive(Clone, Copy, Debug)]
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
        if best.is_none_or(|current| sample.elapsed_ns < current.elapsed_ns) {
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
        if best.is_none_or(|current| sample.elapsed_ns < current.elapsed_ns) {
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
        if best.is_none_or(|current| sample.elapsed_ns < current.elapsed_ns) {
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
        if best.is_none_or(|current| sample.elapsed_ns < current.elapsed_ns) {
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
