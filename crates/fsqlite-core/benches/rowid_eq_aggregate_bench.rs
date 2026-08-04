//! bd-2dgf5 A/B: aggregate over a rowid-equality predicate — seek vs full scan.
//!
//! `SELECT SUM(v) FROM t WHERE id = <int literal>` now seeks the single row by rowid
//! (O(log n)); the same query with `NOT INDEXED` declines the seek and full-scans (O(n)).
//! The differential oracle test `rowid_eq_aggregate_matches_sqlite` proves both return
//! identical values, so this isolates the access path.
//!
//! Substrate: ONE self-identifying binary, with scan and seek interleaved WITHIN each
//! measured round and their order alternating by round. A same-invocation A/A NULL CONTROL
//! (scan vs scan) measures the harness floor before the real scan/seek claim. The bound
//! literal VARIES every execution so the retained autocommit count/sum cache (bd-czzlp)
//! cannot serve the answer. Gate on the bootstrap 95% CI of the MEDIAN per-round ratio,
//! never on CV; CV remains provenance only.

use std::future::Future;
use std::hint::black_box;
use std::time::Instant;

use asupersync::runtime::{Runtime, RuntimeBuilder};
use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::SqliteValue;
use sha2::{Digest, Sha256};

const ROWS: i64 = 20_000;
const EXECS_PER_SAMPLE: usize = 64;
const SAMPLES: usize = 60;
const BOOTSTRAP_REPS: usize = 10_000;
const CHECKSUM_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const CHECKSUM_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug)]
struct ArmSample {
    elapsed_ns: u128,
    checksum: u64,
}

#[derive(Debug)]
struct PairedStats {
    p50_a_ns: u128,
    p50_b_ns: u128,
    ratio_p50: f64,
    ratio_ci95: (f64, f64),
    cv_pct: f64,
    mad: f64,
    checksum_a: u64,
    checksum_b: u64,
}

type TimedArm = fn(&Connection, bool, i64) -> ArmSample;

fn block_on<F: Future>(future: F) -> F::Output {
    thread_local! {
        static RUNTIME: Runtime = RuntimeBuilder::current_thread()
            .build()
            .expect("build benchmark runtime");
    }
    RUNTIME.with(|runtime| runtime.block_on(future))
}

fn self_identity() -> String {
    let Ok(path) = std::env::current_exe() else {
        return "unavailable current_exe".to_owned();
    };
    let Ok(bytes) = std::fs::read(&path) else {
        return format!("unavailable read_error {}", path.display());
    };
    let digest = Sha256::digest(&bytes);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut digest_hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        digest_hex.push(char::from(HEX[usize::from(byte >> 4)]));
        digest_hex.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    format!("{digest_hex} ({} bytes) {}", bytes.len(), path.display())
}

fn source_identity(path: &str) -> String {
    let Ok(bytes) = std::fs::read(path) else {
        return format!("unavailable:{path}");
    };
    let digest = Sha256::digest(&bytes);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut digest_hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        digest_hex.push(char::from(HEX[usize::from(byte >> 4)]));
        digest_hex.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    format!("{path}:{digest_hex}:{}", bytes.len())
}

fn mix_checksum(state: u64, value: u64) -> u64 {
    (state ^ value).wrapping_mul(CHECKSUM_PRIME)
}

fn mix_checksum_bytes(mut state: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        state = mix_checksum(state, u64::from(*byte));
    }
    state
}

fn row_checksum(row: &Row) -> u64 {
    let mut state = CHECKSUM_OFFSET;
    state = mix_checksum(
        state,
        u64::try_from(row.values().len()).expect("column count fits in u64"),
    );
    for value in row.values() {
        state = match value {
            SqliteValue::Null => mix_checksum(state, 0),
            SqliteValue::Integer(value) => mix_checksum(
                mix_checksum(state, 1),
                u64::from_ne_bytes(value.to_ne_bytes()),
            ),
            SqliteValue::Float(value) => mix_checksum(mix_checksum(state, 2), value.to_bits()),
            SqliteValue::Text(value) => {
                mix_checksum_bytes(mix_checksum(state, 3), value.as_ref().as_bytes())
            }
            SqliteValue::Blob(value) => mix_checksum_bytes(mix_checksum(state, 4), value.as_ref()),
        };
    }
    state
}

fn rows_checksum(mut state: u64, rows: &[Row]) -> u64 {
    state = mix_checksum(
        state,
        u64::try_from(rows.len()).expect("row count fits in u64"),
    );
    let mut row_checksums = rows.iter().map(row_checksum).collect::<Vec<_>>();
    // These benchmark queries intentionally omit ORDER BY. Hash the result as
    // a multiset so an access-path-dependent but SQL-valid row order cannot
    // masquerade as a correctness mismatch.
    row_checksums.sort_unstable();
    for row_checksum in row_checksums {
        state = mix_checksum(state, row_checksum);
    }
    state
}

fn setup() -> Connection {
    let conn = block_on(Connection::open(":memory:")).expect("open");
    block_on(conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v REAL);"))
        .expect("create");
    block_on(conn.execute("BEGIN;")).expect("begin");
    for i in 1..=ROWS {
        // k == id (unique, INTEGER affinity) so an IN-list of a few values is selective and
        // has a secondary index to seek; the id-based equality/range arms are unaffected.
        block_on(conn.execute(&format!("INSERT INTO t VALUES ({i}, {i}, {i}.5);")))
            .expect("insert");
    }
    block_on(conn.execute("COMMIT;")).expect("commit");
    block_on(conn.execute("CREATE INDEX idx_t_k ON t(k);")).expect("create index");
    conn
}

/// Run one arm over `EXECS_PER_SAMPLE` distinct literals and return elapsed nanoseconds.
/// `not_indexed` selects the scan arm; the literal cycles so no cache hit can serve it.
fn time_arm(conn: &Connection, not_indexed: bool, base: i64) -> ArmSample {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let mut checksum = CHECKSUM_OFFSET;
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let id = 1 + ((base + j as i64) % ROWS);
        let sql = format!("SELECT SUM(v) FROM t{hint} WHERE id = {id}");
        let rows = block_on(conn.query(black_box(&sql))).expect("query");
        checksum = rows_checksum(checksum, &rows);
        black_box(&rows);
    }
    ArmSample {
        elapsed_ns: start.elapsed().as_nanos(),
        checksum,
    }
}

/// Range arm: `SUM(v) WHERE id <= <upper>` over a selective upper bound. The bounded scan
/// visits `[1, upper]` and stops; `NOT INDEXED` full-scans all `ROWS`. Upper varies per exec.
fn time_range_arm(conn: &Connection, not_indexed: bool, base: i64) -> ArmSample {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let mut checksum = CHECKSUM_OFFSET;
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        // Keep the range selective (~100 rows) so the bounded scan's early-exit dominates.
        let upper = 50 + ((base + j as i64) % 100);
        let sql = format!("SELECT SUM(v) FROM t{hint} WHERE id <= {upper}");
        let rows = block_on(conn.query(black_box(&sql))).expect("query");
        checksum = rows_checksum(checksum, &rows);
        black_box(&rows);
    }
    ArmSample {
        elapsed_ns: start.elapsed().as_nanos(),
        checksum,
    }
}

/// IN-list arm: `SUM(v) WHERE k IN (a,b,c)` on the INTEGER index `idx_t_k`. The seek visits
/// three duplicate runs of one row each; `NOT INDEXED` full-scans all `ROWS`. Values vary.
fn time_in_arm(conn: &Connection, not_indexed: bool, base: i64) -> ArmSample {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let mut checksum = CHECKSUM_OFFSET;
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let a = 1 + ((base + j as i64) % ROWS);
        let b = 1 + ((base + j as i64 + 1) % ROWS);
        let c = 1 + ((base + j as i64 + 2) % ROWS);
        let sql = format!("SELECT SUM(v) FROM t{hint} WHERE k IN ({a}, {b}, {c})");
        let rows = block_on(conn.query(black_box(&sql))).expect("query");
        checksum = rows_checksum(checksum, &rows);
        black_box(&rows);
    }
    ArmSample {
        elapsed_ns: start.elapsed().as_nanos(),
        checksum,
    }
}

/// Non-aggregate IN-list arm: `SELECT id, v WHERE k IN (a,b,c)`, per-value index seek + row
/// projection vs `NOT INDEXED` full-scan. Values vary. Distinct from the aggregate arm: this
/// exercises the `codegen_select_index_in_scan` ResultRow path, not accumulate.
fn time_nonagg_in_arm(conn: &Connection, not_indexed: bool, base: i64) -> ArmSample {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let mut checksum = CHECKSUM_OFFSET;
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let a = 1 + ((base + j as i64) % ROWS);
        let b = 1 + ((base + j as i64 + 1) % ROWS);
        let c = 1 + ((base + j as i64 + 2) % ROWS);
        let sql = format!("SELECT id, v FROM t{hint} WHERE k IN ({a}, {b}, {c})");
        let rows = block_on(conn.query(black_box(&sql))).expect("query");
        checksum = rows_checksum(checksum, &rows);
        black_box(&rows);
    }
    ArmSample {
        elapsed_ns: start.elapsed().as_nanos(),
        checksum,
    }
}

/// Rowid IN-list arm: `SELECT id, v WHERE id IN (a,b,c)`, one SeekRowid per value vs a
/// `NOT INDEXED` full scan. Values vary. Exercises `codegen_select_rowid_in_scan`.
fn time_rowid_in_arm(conn: &Connection, not_indexed: bool, base: i64) -> ArmSample {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let mut checksum = CHECKSUM_OFFSET;
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let a = 1 + ((base + j as i64) % ROWS);
        let b = 1 + ((base + j as i64 + 7) % ROWS);
        let c = 1 + ((base + j as i64 + 13) % ROWS);
        let sql = format!("SELECT id, v FROM t{hint} WHERE id IN ({a}, {b}, {c})");
        let rows = block_on(conn.query(black_box(&sql))).expect("query");
        checksum = rows_checksum(checksum, &rows);
        black_box(&rows);
    }
    ArmSample {
        elapsed_ns: start.elapsed().as_nanos(),
        checksum,
    }
}

/// OR-of-equalities arm: `SELECT id, v WHERE k = a OR k = b OR k = c`, normalized to a
/// per-value index seek vs a `NOT INDEXED` scan. Proves the OR->IN normalization fires.
fn time_or_arm(conn: &Connection, not_indexed: bool, base: i64) -> ArmSample {
    let hint = if not_indexed { " NOT INDEXED" } else { "" };
    let mut checksum = CHECKSUM_OFFSET;
    let start = Instant::now();
    for j in 0..EXECS_PER_SAMPLE {
        let a = 1 + ((base + j as i64) % ROWS);
        let b = 1 + ((base + j as i64 + 1) % ROWS);
        let c = 1 + ((base + j as i64 + 2) % ROWS);
        let sql = format!("SELECT id, v FROM t{hint} WHERE k = {a} OR k = {b} OR k = {c}");
        let rows = block_on(conn.query(black_box(&sql))).expect("query");
        checksum = rows_checksum(checksum, &rows);
        black_box(&rows);
    }
    ArmSample {
        elapsed_ns: start.elapsed().as_nanos(),
        checksum,
    }
}

fn median_u128(values: &mut [u128]) -> u128 {
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

fn median_f64(values: &mut [f64]) -> f64 {
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
    let mut bootstrap_medians = Vec::with_capacity(BOOTSTRAP_REPS);
    let mut resample = vec![0.0; ratios.len()];
    let len_u64 = u64::try_from(ratios.len()).expect("sample count fits in u64");

    for _ in 0..BOOTSTRAP_REPS {
        for value in &mut resample {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let index = usize::try_from(state % len_u64).expect("index fits in usize");
            *value = ratios[index];
        }
        bootstrap_medians.push(median_f64(&mut resample));
    }

    bootstrap_medians.sort_by(f64::total_cmp);
    let low = BOOTSTRAP_REPS * 25 / 1_000;
    let high = (BOOTSTRAP_REPS * 975 / 1_000).min(BOOTSTRAP_REPS - 1);
    (bootstrap_medians[low], bootstrap_medians[high])
}

#[allow(clippy::cast_precision_loss)]
fn paired(
    conn: &Connection,
    timed_arm: TimedArm,
    arm_a_not_indexed: bool,
    arm_b_not_indexed: bool,
) -> PairedStats {
    let mut times_a = Vec::with_capacity(SAMPLES);
    let mut times_b = Vec::with_capacity(SAMPLES);
    let mut ratios = Vec::with_capacity(SAMPLES);
    let mut checksum_a = CHECKSUM_OFFSET;
    let mut checksum_b = CHECKSUM_OFFSET;

    for round in 0..SAMPLES {
        let round_i64 = i64::try_from(round).expect("round fits in i64");
        let executions_i64 = i64::try_from(EXECS_PER_SAMPLE).expect("execution count fits in i64");
        let base = round_i64 * executions_i64;
        let (arm_a, arm_b) = if round % 2 == 0 {
            (
                timed_arm(conn, arm_a_not_indexed, base),
                timed_arm(conn, arm_b_not_indexed, base),
            )
        } else {
            let arm_b = timed_arm(conn, arm_b_not_indexed, base);
            let arm_a = timed_arm(conn, arm_a_not_indexed, base);
            (arm_a, arm_b)
        };
        times_a.push(arm_a.elapsed_ns);
        times_b.push(arm_b.elapsed_ns);
        ratios.push((arm_a.elapsed_ns as f64) / (arm_b.elapsed_ns.max(1) as f64));
        checksum_a = mix_checksum(checksum_a, arm_a.checksum);
        checksum_b = mix_checksum(checksum_b, arm_b.checksum);
    }

    let ratio_ci95 = bootstrap_median_ci95(&ratios);
    let ratio_p50 = median_f64(&mut ratios);
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
    let mad = median_f64(&mut deviations);

    PairedStats {
        p50_a_ns: median_u128(&mut times_a),
        p50_b_ns: median_u128(&mut times_b),
        ratio_p50,
        ratio_ci95,
        cv_pct,
        mad,
        checksum_a,
        checksum_b,
    }
}

#[allow(clippy::cast_precision_loss)]
fn micros_per_query(nanos: u128) -> f64 {
    (nanos as f64) / (EXECS_PER_SAMPLE as f64) / 1_000.0
}

fn report_case(label: &str, null: &PairedStats, claim: &PairedStats) {
    assert_eq!(
        null.checksum_a, null.checksum_b,
        "{label} A/A output checksum mismatch"
    );
    assert_eq!(
        claim.checksum_a, claim.checksum_b,
        "{label} A/B output checksum mismatch"
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
    let decidable = outside_null_ci && margin >= 2.0 && claim_effect >= 0.01;

    println!("case={label}");
    println!(
        "null_a_a ratio_median={:.6} ci95=[{:.6},{:.6}] cv_pct={:.3} mad={:.6} p50_a_us={:.3} p50_b_us={:.3} checksum_a={:016x} checksum_b={:016x}",
        null.ratio_p50,
        null.ratio_ci95.0,
        null.ratio_ci95.1,
        null.cv_pct,
        null.mad,
        micros_per_query(null.p50_a_ns),
        micros_per_query(null.p50_b_ns),
        null.checksum_a,
        null.checksum_b
    );
    println!(
        "claim_scan_seek ratio_median={:.6} ci95=[{:.6},{:.6}] cv_pct={:.3} mad={:.6} scan_p50_us={:.3} seek_p50_us={:.3} checksum_scan={:016x} checksum_seek={:016x}",
        claim.ratio_p50,
        claim.ratio_ci95.0,
        claim.ratio_ci95.1,
        claim.cv_pct,
        claim.mad,
        micros_per_query(claim.p50_a_ns),
        micros_per_query(claim.p50_b_ns),
        claim.checksum_a,
        claim.checksum_b
    );
    println!(
        "median_ci_gate={} rule=null_ci95_2x_margin cv_gate=never null_radius={:.6} claim_margin={:.3} min_decidable_ratio={:.6}",
        if decidable {
            "DECIDABLE"
        } else {
            "INCONCLUSIVE"
        },
        null_radius,
        margin,
        2.0_f64.mul_add(null_radius, 1.0)
    );
}

fn run_case(conn: &Connection, label: &str, timed_arm: TimedArm) {
    let _ = black_box(timed_arm(conn, true, 0));
    let _ = black_box(timed_arm(conn, false, 0));
    let null = paired(conn, timed_arm, true, true);
    let claim = paired(conn, timed_arm, true, false);
    report_case(label, &null, &claim);
}

fn main() {
    println!("bench_elf_sha256={}", self_identity());
    println!(
        "bench_source_sha256 {} {}",
        source_identity("crates/fsqlite-core/benches/rowid_eq_aggregate_bench.rs"),
        source_identity("crates/fsqlite-core/src/connection.rs")
    );
    let conn = setup();
    println!(
        "rows={ROWS} execs_per_sample={EXECS_PER_SAMPLE} samples={SAMPLES} bootstrap_reps={BOOTSTRAP_REPS}"
    );

    run_case(&conn, "rowid_eq_aggregate", time_arm);
    run_case(&conn, "rowid_range_aggregate", time_range_arm);
    run_case(&conn, "index_in_aggregate", time_in_arm);
    run_case(&conn, "index_in_nonaggregate", time_nonagg_in_arm);
    run_case(&conn, "rowid_in_nonaggregate", time_rowid_in_arm);
    run_case(&conn, "or_equalities_nonaggregate", time_or_arm);
}
