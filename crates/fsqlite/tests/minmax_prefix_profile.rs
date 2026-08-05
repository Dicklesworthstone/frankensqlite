//! Contract remeasurement for `bd-minmax-prefix-seek-4fuo6`.
//!
//! The production lever shipped in `69b7a5d6`: `MIN(b)`/`MAX(b) WHERE a=?` over
//! `(a,b)` seeks the prefix extremum rather than scanning the full group. This
//! executable compares that route with the byte-identical `NOT INDEXED` scan.
//! It implements the campaign contract directly:
//!
//! - its first stdout line hashes the ELF that is actually executing;
//! - every claim is preceded by an interleaved same-invocation A/A null;
//! - the decision gate is the bootstrap CI of the median per-round ratio, never
//!   coefficient of variation (CV is provenance only).
//!
//! Run:
//! `RCH_REQUIRE_REMOTE=1 rch exec -- cargo bench --profile release-perf
//! -p fsqlite --bench minmax_prefix_contract_bench`

#![allow(clippy::cast_precision_loss)]

use std::fmt::Write as FmtWrite;
use std::hint::black_box;
use std::io::Write as IoWrite;
use std::time::Instant;

use fsqlite::{Connection, PreparedStatement, Row, SqliteValue};
use sha2::{Digest, Sha256};

const SEED_ROWS: i64 = 20_000;
const GROUP_COUNT: i64 = 20;
const TARGET_GROUP: i64 = 7;
const CONTRACT_ROUNDS: usize = 41;
const CONTRACT_MIN_OF: usize = 3;
const CONTRACT_BOOTSTRAP_REPS: usize = 10_000;
const CONTRACT_MIN_SAMPLE_NS: u128 = 2_000_000;
const CONTRACT_MAX_ITERS: usize = 1 << 20;
const CHECKSUM_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const CHECKSUM_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug)]
struct ArmSample {
    nanos_per_query: f64,
    checksum: u64,
}

#[derive(Debug)]
struct PairedStats {
    p50_a_ns: f64,
    p50_b_ns: f64,
    ratio_p50: f64,
    ratio_ci95: (f64, f64),
    cv_pct: f64,
    mad: f64,
    checksum_a: u64,
    checksum_b: u64,
    iterations_a: usize,
    iterations_b: usize,
}

#[derive(Clone, Copy)]
struct ContractCase {
    name: &'static str,
    baseline_sql: &'static str,
    candidate_sql: &'static str,
    candidate_seek_opcode: &'static str,
}

fn bytes_to_lower_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

fn file_identity(path: &str) -> String {
    let Ok(bytes) = std::fs::read(path) else {
        return format!("unavailable:{path}");
    };
    let digest = Sha256::digest(&bytes);
    format!("{path}:{}:{}", bytes_to_lower_hex(&digest), bytes.len())
}

fn self_identity() -> String {
    let Ok(path) = std::env::current_exe() else {
        return "unavailable current_exe".to_owned();
    };
    let Ok(bytes) = std::fs::read(&path) else {
        return format!("unavailable read_error {}", path.display());
    };
    let digest = Sha256::digest(&bytes);
    format!(
        "{} ({} bytes) {}",
        bytes_to_lower_hex(&digest),
        bytes.len(),
        path.display()
    )
}

fn mix_checksum(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(CHECKSUM_PRIME)
}

fn rows_checksum(rows: &[Row]) -> u64 {
    assert_eq!(rows.len(), 1, "MIN/MAX must return exactly one row");
    assert_eq!(
        rows[0].values().len(),
        1,
        "MIN/MAX must return exactly one column"
    );
    match rows[0].values().first() {
        Some(SqliteValue::Integer(value)) => {
            mix_checksum(CHECKSUM_OFFSET, u64::from_ne_bytes(value.to_ne_bytes()))
        }
        value => panic!("MIN/MAX fixture must return an integer, got {value:?}"),
    }
}

async fn setup_fixture() -> Connection {
    let connection = Connection::open(":memory:")
        .await
        .expect("open independent in-memory fixture");
    connection
        .execute("PRAGMA fsqlite.stmt_microbatch = OFF")
        .await
        .expect("disable statement carry for the measurement fixture");
    connection
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
        .await
        .expect("create measurement table");
    connection
        .execute("CREATE INDEX idx_ab ON t(a, b)")
        .await
        .expect("create composite measurement index");
    connection
        .execute("BEGIN")
        .await
        .expect("begin fixture load");

    const INSERTS_PER_BATCH: i64 = 500;
    let mut first = 1_i64;
    while first <= SEED_ROWS {
        let last = (first + INSERTS_PER_BATCH - 1).min(SEED_ROWS);
        let mut batch = String::with_capacity(
            usize::try_from(last - first + 1).expect("batch length fits in usize") * 64,
        );
        for id in first..=last {
            let a = id % GROUP_COUNT;
            let b = (id.wrapping_mul(2_654_435_761) >> 8) & 0xffff;
            writeln!(&mut batch, "INSERT INTO t VALUES ({id}, {a}, {b});")
                .expect("writing to a String cannot fail");
        }
        connection
            .execute_batch(&batch)
            .await
            .expect("load fixture batch");
        first = last + 1;
    }

    connection
        .execute("COMMIT")
        .await
        .expect("commit fixture load");
    connection
}

async fn opcode_names(connection: &Connection, sql: &str) -> Vec<String> {
    connection
        .query(&format!("EXPLAIN {sql}"))
        .await
        .expect("EXPLAIN contract query")
        .iter()
        .filter_map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(opcode)) => Some(opcode.to_string()),
            _ => None,
        })
        .collect()
}

async fn time_once(statement: &PreparedStatement<'_>, iterations: usize) -> ArmSample {
    let mut expected_checksum = None;
    let started = Instant::now();
    for _ in 0..iterations {
        let rows = statement
            .query()
            .await
            .expect("MIN/MAX measurement query should execute");
        let checksum = rows_checksum(&rows);
        if let Some(expected) = expected_checksum {
            assert_eq!(checksum, expected, "query output changed inside one sample");
        } else {
            expected_checksum = Some(checksum);
        }
        black_box(rows);
    }
    ArmSample {
        nanos_per_query: started.elapsed().as_nanos() as f64 / iterations as f64,
        checksum: expected_checksum.expect("a sample executes at least once"),
    }
}

async fn calibrate_iterations(statement: &PreparedStatement<'_>) -> usize {
    let mut iterations = 1_usize;
    loop {
        let sample = time_once(statement, iterations).await;
        let elapsed_ns = sample.nanos_per_query * iterations as f64;
        if elapsed_ns >= CONTRACT_MIN_SAMPLE_NS as f64 || iterations >= CONTRACT_MAX_ITERS {
            return iterations;
        }
        iterations = (iterations * 2).min(CONTRACT_MAX_ITERS);
    }
}

async fn time_min_of(statement: &PreparedStatement<'_>, iterations: usize) -> ArmSample {
    let mut best: Option<ArmSample> = None;
    let mut expected_checksum = None;
    for _ in 0..CONTRACT_MIN_OF {
        let sample = time_once(statement, iterations).await;
        if let Some(expected) = expected_checksum {
            assert_eq!(
                sample.checksum, expected,
                "inner replicates must return identical output"
            );
        } else {
            expected_checksum = Some(sample.checksum);
        }
        if best.is_none_or(|current| sample.nanos_per_query < current.nanos_per_query) {
            best = Some(sample);
        }
    }
    best.expect("min-of contract requires at least one replicate")
}

fn median(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty(), "median requires samples");
    values.sort_by(f64::total_cmp);
    let upper = values.len() / 2;
    if values.len() % 2 == 0 {
        f64::midpoint(values[upper - 1], values[upper])
    } else {
        values[upper]
    }
}

fn bootstrap_median_ci95(ratios: &[f64]) -> (f64, f64) {
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
        bootstrap_medians.push(median(&mut resample));
    }

    bootstrap_medians.sort_by(f64::total_cmp);
    let low = CONTRACT_BOOTSTRAP_REPS * 25 / 1_000;
    let high = (CONTRACT_BOOTSTRAP_REPS * 975 / 1_000).min(CONTRACT_BOOTSTRAP_REPS - 1);
    (bootstrap_medians[low], bootstrap_medians[high])
}

async fn paired(
    statement_a: &PreparedStatement<'_>,
    statement_b: &PreparedStatement<'_>,
) -> PairedStats {
    let iterations_a = calibrate_iterations(statement_a).await;
    let iterations_b = calibrate_iterations(statement_b).await;
    let _ = black_box(time_once(statement_a, iterations_a).await);
    let _ = black_box(time_once(statement_b, iterations_b).await);

    let mut times_a = Vec::with_capacity(CONTRACT_ROUNDS);
    let mut times_b = Vec::with_capacity(CONTRACT_ROUNDS);
    let mut ratios = Vec::with_capacity(CONTRACT_ROUNDS);
    let mut checksum_a = CHECKSUM_OFFSET;
    let mut checksum_b = CHECKSUM_OFFSET;

    for round in 0..CONTRACT_ROUNDS {
        let (arm_a, arm_b) = if round % 2 == 0 {
            (
                time_min_of(statement_a, iterations_a).await,
                time_min_of(statement_b, iterations_b).await,
            )
        } else {
            let arm_b = time_min_of(statement_b, iterations_b).await;
            let arm_a = time_min_of(statement_a, iterations_a).await;
            (arm_a, arm_b)
        };
        times_a.push(arm_a.nanos_per_query);
        times_b.push(arm_b.nanos_per_query);
        ratios.push(arm_a.nanos_per_query / arm_b.nanos_per_query.max(f64::EPSILON));
        checksum_a = mix_checksum(checksum_a, arm_a.checksum);
        checksum_b = mix_checksum(checksum_b, arm_b.checksum);
    }

    let ratio_ci95 = bootstrap_median_ci95(&ratios);
    let ratio_p50 = median(&mut ratios);
    let mean = ratios.iter().sum::<f64>() / ratios.len() as f64;
    let variance = ratios
        .iter()
        .map(|ratio| (ratio - mean).powi(2))
        .sum::<f64>()
        / (ratios.len() - 1) as f64;
    let cv_pct = if mean == 0.0 {
        0.0
    } else {
        variance.sqrt() / mean.abs() * 100.0
    };
    let mut deviations = ratios
        .iter()
        .map(|ratio| (ratio - ratio_p50).abs())
        .collect::<Vec<_>>();
    let mad = median(&mut deviations);

    PairedStats {
        p50_a_ns: median(&mut times_a),
        p50_b_ns: median(&mut times_b),
        ratio_p50,
        ratio_ci95,
        cv_pct,
        mad,
        checksum_a,
        checksum_b,
        iterations_a,
        iterations_b,
    }
}

fn report(case_name: &str, null: &PairedStats, claim: &PairedStats) {
    assert_eq!(
        null.checksum_a, null.checksum_b,
        "A/A output checksum mismatch"
    );
    assert_eq!(
        claim.checksum_a, claim.checksum_b,
        "scan/seek output checksum mismatch"
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
    let decisive_effect = (2.0 * null_radius).max(0.01);
    let min_decidable_gain = 1.0 + decisive_effect;
    let max_decidable_regression = 1.0 - decisive_effect;
    let verdict = if claim.ratio_ci95.0 > min_decidable_gain {
        "CONFIRMED_KEEP"
    } else if claim.ratio_ci95.1 < max_decidable_regression {
        "REGRESSION"
    } else {
        "INCONCLUSIVE"
    };

    println!(
        "case={case_name} null_a_a ratio_median={:.6} ci95=[{:.6},{:.6}] cv_pct={:.3} mad={:.6} p50_a_us={:.3} p50_b_us={:.3} iterations_a={} iterations_b={} checksum_a={:016x} checksum_b={:016x}",
        null.ratio_p50,
        null.ratio_ci95.0,
        null.ratio_ci95.1,
        null.cv_pct,
        null.mad,
        null.p50_a_ns / 1_000.0,
        null.p50_b_ns / 1_000.0,
        null.iterations_a,
        null.iterations_b,
        null.checksum_a,
        null.checksum_b
    );
    println!(
        "case={case_name} claim_scan_seek ratio_median={:.6} ci95=[{:.6},{:.6}] cv_pct={:.3} mad={:.6} scan_p50_us={:.3} seek_p50_us={:.3} iterations_scan={} iterations_seek={} checksum_scan={:016x} checksum_seek={:016x}",
        claim.ratio_p50,
        claim.ratio_ci95.0,
        claim.ratio_ci95.1,
        claim.cv_pct,
        claim.mad,
        claim.p50_a_ns / 1_000.0,
        claim.p50_b_ns / 1_000.0,
        claim.iterations_a,
        claim.iterations_b,
        claim.checksum_a,
        claim.checksum_b
    );
    println!(
        "case={case_name} median_ci_gate={verdict} rule=null_ci95_2x_margin cv_gate=never null_radius={null_radius:.6} claim_margin={margin:.3} min_decidable_gain={:.6} max_decidable_regression={:.6}",
        min_decidable_gain, max_decidable_regression
    );
}

async fn run_contract() {
    println!(
        "contract rows={SEED_ROWS} groups={GROUP_COUNT} rows_per_group={} target_group={TARGET_GROUP} min_sample_ms={:.1} min_of={CONTRACT_MIN_OF} rounds={CONTRACT_ROUNDS} bootstrap_reps={CONTRACT_BOOTSTRAP_REPS}",
        SEED_ROWS / GROUP_COUNT,
        CONTRACT_MIN_SAMPLE_NS as f64 / 1_000_000.0
    );

    let null_a_connection = setup_fixture().await;
    let null_b_connection = setup_fixture().await;
    let baseline_connection = setup_fixture().await;
    let candidate_connection = setup_fixture().await;

    let cases = [
        ContractCase {
            name: "max_b_prefix_seek",
            baseline_sql: "SELECT MAX(b) FROM t NOT INDEXED WHERE a = 7",
            candidate_sql: "SELECT MAX(b) FROM t WHERE a = 7",
            candidate_seek_opcode: "SeekLE",
        },
        ContractCase {
            name: "min_b_prefix_seek",
            baseline_sql: "SELECT MIN(b) FROM t NOT INDEXED WHERE a = 7",
            candidate_sql: "SELECT MIN(b) FROM t WHERE a = 7",
            candidate_seek_opcode: "SeekGE",
        },
    ];

    for case in cases {
        let baseline_opcodes = opcode_names(&baseline_connection, case.baseline_sql).await;
        let candidate_opcodes = opcode_names(&candidate_connection, case.candidate_sql).await;
        assert!(
            !baseline_opcodes
                .iter()
                .any(|opcode| opcode == case.candidate_seek_opcode),
            "NOT INDEXED baseline unexpectedly contains {}: {baseline_opcodes:?}",
            case.candidate_seek_opcode
        );
        assert!(
            candidate_opcodes
                .iter()
                .any(|opcode| opcode == case.candidate_seek_opcode),
            "candidate must contain {}: {candidate_opcodes:?}",
            case.candidate_seek_opcode
        );
        println!(
            "case={} opcode_gate=pass baseline_scan=true candidate_seek={}",
            case.name, case.candidate_seek_opcode
        );

        let null_a = null_a_connection
            .prepare(case.baseline_sql)
            .await
            .expect("prepare first A/A scan arm");
        let null_b = null_b_connection
            .prepare(case.baseline_sql)
            .await
            .expect("prepare second A/A scan arm");
        let baseline = baseline_connection
            .prepare(case.baseline_sql)
            .await
            .expect("prepare claim scan arm");
        let candidate = candidate_connection
            .prepare(case.candidate_sql)
            .await
            .expect("prepare claim seek arm");

        let null = paired(&null_a, &null_b).await;
        let claim = paired(&baseline, &candidate).await;
        report(case.name, &null, &claim);
    }
}

pub(crate) fn run_entrypoint() {
    {
        let stdout = std::io::stdout();
        let mut lock = stdout.lock();
        writeln!(lock, "bench_elf_sha256={}", self_identity()).expect("write executable identity");
        lock.flush().expect("flush executable identity");
    }
    println!(
        "bench_source_sha256 {} {}",
        file_identity(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/minmax_prefix_profile.rs"
        )),
        file_identity(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../fsqlite-vdbe/src/codegen.rs"
        ))
    );

    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("build minmax contract runtime");
    runtime.block_on(run_contract());
}

#[cfg(test)]
#[test]
#[ignore = "contract profiler; run the custom release-perf bench target"]
fn minmax_prefix_contract() {
    run_entrypoint();
}
