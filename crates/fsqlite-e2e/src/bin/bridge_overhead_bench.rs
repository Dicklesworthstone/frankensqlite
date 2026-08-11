//! `bridge-overhead-bench` — bd-rp4rt (BRIDGE lane of bd-dqdoe Gate-0 step 4).
//!
//! ONE binary, four arms, SAME literal-INSERT workload, at HEAD:
//!
//! * **arm `single_entry` (a):** one `fsqlite_e2e::block_on` around the whole
//!   timed workload (BEGIN + N inserts + COMMIT). Exactly 1 bridge entry per
//!   sample, so its elapsed-vs-N slope is the per-row engine cost with zero
//!   marginal bridge entries.
//! * **arm `per_op_bridge` (b):** the current production-consumer shape — one
//!   `fsqlite_e2e::block_on` per statement on the raw async `Connection`
//!   (thread-local reused runtime; the exact bridge named by bd-zavyn /
//!   bd-mnlk2). N+2 bridge entries per sample.
//! * **arm `worker_facade` (c):** the public worker-based `AsyncConnection`
//!   through its synchronous facade (`open_sync` / `execute_sync`). N+2
//!   facade entries per sample.
//! * **arm `ready_control` (d):** N+2 `block_on(ready(()))` entries with NO
//!   database work — the pure runtime-entry slope.
//!
//! Every bridge/facade entry is counted by an in-binary atomic counter and the
//! EXACT per-sample count is recorded; elapsed is regressed (per-arm OLS plus
//! paired overhead OLS against the bridge-call delta, with fixed-seed
//! pairs-bootstrap CIs) against those exact counts. Arm order is randomized
//! per (round, level) by a fixed-seed LCG. DIAGNOSTIC unless the run receipt
//! shows a quiet pinned host; the regression coefficients are the deliverable,
//! not absolute latency.

// bd-mnlk2 / bd-zavyn: timed bodies await fsqlite-core's deliberately large
// engine futures inside one runtime entry; boxing them would put an
// allocation inside the timed window.
#![allow(clippy::large_futures)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use sha2::Digest as _;

/// Exact count of synchronous entries into async execution (thread-local
/// runtime `block_on` bridge entries AND worker-facade `_sync` entries).
static BRIDGE_ENTRIES: AtomicU64 = AtomicU64::new(0);

fn bridged_block_on<F: std::future::Future>(future: F) -> F::Output {
    BRIDGE_ENTRIES.fetch_add(1, Ordering::Relaxed);
    fsqlite_e2e::block_on(future)
}

const LEVELS: [usize; 5] = [100, 200, 400, 800, 1600];
const ROUNDS: usize = 20;
const WARMUP_ROUNDS: usize = 2;
const LCG_SEED: u64 = 0x5dee_ce66_d1ce_b00c;
const BOOTSTRAP_RESAMPLES: usize = 2000;

const ARM_NAMES: [&str; 4] = [
    "single_entry",
    "per_op_bridge",
    "worker_facade",
    "ready_control",
];

struct Lcg(u64);

impl Lcg {
    fn next_u64(&mut self) -> u64 {
        // Numerical Recipes LCG constants; determinism matters, quality does not.
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            #[allow(clippy::cast_possible_truncation)]
            let j = (self.next_u64() % (i as u64 + 1)) as usize;
            items.swap(i, j);
        }
    }
}

#[derive(Clone, Copy)]
struct Sample {
    arm: usize,
    rows: usize,
    bridge_calls: u64,
    elapsed_ns: f64,
}

fn open_raw_connection() -> fsqlite::Connection {
    bridged_block_on(fsqlite::Connection::open(":memory:")).expect("open raw :memory:")
}

fn raw_create_schema(conn: &fsqlite::Connection) {
    bridged_block_on(conn.execute("CREATE TABLE bridge_probe (v INTEGER)"))
        .expect("create bridge_probe");
}

fn raw_verify_count(conn: &fsqlite::Connection, expected: usize) {
    let rows = bridged_block_on(conn.query("SELECT COUNT(*) FROM bridge_probe"))
        .expect("count bridge_probe");
    let got = match rows.first().and_then(|row| row.get(0)) {
        Some(fsqlite::SqliteValue::Integer(v)) => *v,
        other => panic!("expected integer count, got {other:?}"),
    };
    assert_eq!(got, i64::try_from(expected).expect("row count fits i64"));
}

/// Arm (a): one bridge entry around the entire timed workload.
#[allow(clippy::cast_precision_loss)]
fn run_single_entry(rows: usize) -> Sample {
    let conn = open_raw_connection();
    raw_create_schema(&conn);
    let before = BRIDGE_ENTRIES.load(Ordering::Relaxed);
    let start = Instant::now();
    bridged_block_on(async {
        conn.execute("BEGIN").await.expect("begin");
        for i in 0..rows {
            conn.execute(&format!("INSERT INTO bridge_probe VALUES ({i})"))
                .await
                .expect("insert");
        }
        conn.execute("COMMIT").await.expect("commit");
    });
    let elapsed = start.elapsed();
    let calls = BRIDGE_ENTRIES.load(Ordering::Relaxed) - before;
    raw_verify_count(&conn, rows);
    Sample {
        arm: 0,
        rows,
        bridge_calls: calls,
        elapsed_ns: elapsed.as_nanos() as f64,
    }
}

/// Arm (b): the current per-operation `block_on` bridge.
#[allow(clippy::cast_precision_loss)]
fn run_per_op_bridge(rows: usize) -> Sample {
    let conn = open_raw_connection();
    raw_create_schema(&conn);
    let before = BRIDGE_ENTRIES.load(Ordering::Relaxed);
    let start = Instant::now();
    bridged_block_on(conn.execute("BEGIN")).expect("begin");
    for i in 0..rows {
        bridged_block_on(conn.execute(&format!("INSERT INTO bridge_probe VALUES ({i})")))
            .expect("insert");
    }
    bridged_block_on(conn.execute("COMMIT")).expect("commit");
    let elapsed = start.elapsed();
    let calls = BRIDGE_ENTRIES.load(Ordering::Relaxed) - before;
    raw_verify_count(&conn, rows);
    Sample {
        arm: 1,
        rows,
        bridge_calls: calls,
        elapsed_ns: elapsed.as_nanos() as f64,
    }
}

/// Arm (c): worker-based `AsyncConnection` through the sync facade.
#[allow(clippy::cast_precision_loss)]
fn run_worker_facade(rows: usize) -> Sample {
    let mut conn = fsqlite::AsyncConnection::open_sync(":memory:").expect("open worker :memory:");
    BRIDGE_ENTRIES.fetch_add(1, Ordering::Relaxed);
    conn.execute_sync("CREATE TABLE bridge_probe (v INTEGER)")
        .expect("create bridge_probe");
    BRIDGE_ENTRIES.fetch_add(1, Ordering::Relaxed);
    let before = BRIDGE_ENTRIES.load(Ordering::Relaxed);
    let start = Instant::now();
    conn.execute_sync("BEGIN").expect("begin");
    BRIDGE_ENTRIES.fetch_add(1, Ordering::Relaxed);
    for i in 0..rows {
        conn.execute_sync(&format!("INSERT INTO bridge_probe VALUES ({i})"))
            .expect("insert");
        BRIDGE_ENTRIES.fetch_add(1, Ordering::Relaxed);
    }
    conn.execute_sync("COMMIT").expect("commit");
    BRIDGE_ENTRIES.fetch_add(1, Ordering::Relaxed);
    let elapsed = start.elapsed();
    let calls = BRIDGE_ENTRIES.load(Ordering::Relaxed) - before;
    let counted = conn
        .query_row_sync("SELECT COUNT(*) FROM bridge_probe")
        .expect("count bridge_probe");
    match counted.get(0) {
        Some(fsqlite::SqliteValue::Integer(v)) => {
            assert_eq!(*v, i64::try_from(rows).expect("row count fits i64"));
        }
        other => panic!("expected integer count, got {other:?}"),
    }
    conn.close_sync().expect("close worker connection");
    Sample {
        arm: 2,
        rows,
        bridge_calls: calls,
        elapsed_ns: elapsed.as_nanos() as f64,
    }
}

/// Arm (d): pure runtime-entry control — same entry count, no database work.
#[allow(clippy::cast_precision_loss)]
fn run_ready_control(rows: usize) -> Sample {
    let before = BRIDGE_ENTRIES.load(Ordering::Relaxed);
    let start = Instant::now();
    for _ in 0..rows + 2 {
        bridged_block_on(std::future::ready(()));
    }
    let elapsed = start.elapsed();
    let calls = BRIDGE_ENTRIES.load(Ordering::Relaxed) - before;
    Sample {
        arm: 3,
        rows,
        bridge_calls: calls,
        elapsed_ns: elapsed.as_nanos() as f64,
    }
}

fn run_arm(arm: usize, rows: usize) -> Sample {
    match arm {
        0 => run_single_entry(rows),
        1 => run_per_op_bridge(rows),
        2 => run_worker_facade(rows),
        3 => run_ready_control(rows),
        _ => unreachable!("unknown arm"),
    }
}

/// OLS y = intercept + slope * x. Returns (slope, intercept, r_squared).
fn ols(points: &[(f64, f64)]) -> (f64, f64, f64) {
    #[allow(clippy::cast_precision_loss)]
    let n = points.len() as f64;
    let mean_x = points.iter().map(|p| p.0).sum::<f64>() / n;
    let mean_y = points.iter().map(|p| p.1).sum::<f64>() / n;
    let sxx: f64 = points.iter().map(|p| (p.0 - mean_x).powi(2)).sum();
    let sxy: f64 = points.iter().map(|p| (p.0 - mean_x) * (p.1 - mean_y)).sum();
    let slope = sxy / sxx;
    let intercept = slope.mul_add(-mean_x, mean_y);
    let ss_tot: f64 = points.iter().map(|p| (p.1 - mean_y).powi(2)).sum();
    let ss_res: f64 = points
        .iter()
        .map(|p| (p.1 - slope.mul_add(p.0, intercept)).powi(2))
        .sum();
    let r_squared = if ss_tot > 0.0 {
        1.0 - ss_res / ss_tot
    } else {
        f64::NAN
    };
    (slope, intercept, r_squared)
}

/// Fixed-seed pairs bootstrap on the OLS slope: percentile 2.5/97.5 interval.
fn bootstrap_slope_ci(points: &[(f64, f64)], seed_salt: u64) -> (f64, f64) {
    let mut rng = Lcg(LCG_SEED ^ seed_salt);
    let mut slopes = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    let mut resample = Vec::with_capacity(points.len());
    for _ in 0..BOOTSTRAP_RESAMPLES {
        resample.clear();
        for _ in 0..points.len() {
            #[allow(clippy::cast_possible_truncation)]
            let idx = (rng.next_u64() % points.len() as u64) as usize;
            resample.push(points[idx]);
        }
        let (slope, _, _) = ols(&resample);
        if slope.is_finite() {
            slopes.push(slope);
        }
    }
    slopes.sort_by(f64::total_cmp);
    let lo = slopes[(slopes.len() * 25) / 1000];
    let hi = slopes[(slopes.len() * 975) / 1000];
    (lo, hi)
}

fn median(values: &mut [f64]) -> f64 {
    assert!(!values.is_empty(), "median requires at least one sample");
    values.sort_by(f64::total_cmp);
    let upper = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[upper - 1], values[upper])
    } else {
        values[upper]
    }
}

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_owned())
}

fn cpus_allowed_list() -> Option<String> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find(|line| line.starts_with("Cpus_allowed_list:"))
        .map(|line| line.split(':').nth(1).unwrap_or("").trim().to_owned())
}

fn provenance() -> serde_json::Value {
    let exe = std::env::current_exe().ok();
    let (elf_sha256, elf_bytes) = exe.as_ref().map_or_else(
        || ("unavailable".to_owned(), 0),
        |path| {
            std::fs::read(path).map_or_else(
                |_| ("unreadable".to_owned(), 0),
                |bytes| {
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(&bytes);
                    let hex: String = hasher
                        .finalize()
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect();
                    (hex, bytes.len())
                },
            )
        },
    );
    serde_json::json!({
        "bead": "bd-rp4rt",
        "parent_bead": "bd-dqdoe",
        "build_git_sha": env!("FSQLITE_BENCH_BUILD_GIT_SHA"),
        "build_git_branch": env!("FSQLITE_BENCH_BUILD_GIT_BRANCH"),
        "build_git_dirty": env!("FSQLITE_BENCH_BUILD_GIT_DIRTY"),
        "build_profile": env!("FSQLITE_BENCH_BUILD_PROFILE"),
        "build_opt_level": env!("FSQLITE_BENCH_BUILD_OPT_LEVEL"),
        "elf_sha256": elf_sha256,
        "elf_bytes": elf_bytes,
        "exe_path": exe.as_deref().map(|p| p.display().to_string()),
        "hostname": read_trimmed("/etc/hostname"),
        "cpus_allowed_list": cpus_allowed_list(),
        "loadavg_start": read_trimmed("/proc/loadavg"),
        "levels": LEVELS,
        "rounds": ROUNDS,
        "warmup_rounds": WARMUP_ROUNDS,
        "lcg_seed": format!("{LCG_SEED:#x}"),
        "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
        "bridge_api": "fsqlite_e2e::block_on (thread-local reused asupersync Runtime) / AsyncConnection *_sync facade",
    })
}

#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
fn main() {
    let artifact_path = std::env::args().nth(1);
    let prov = provenance();
    println!(
        "provenance: {}",
        serde_json::to_string(&prov).expect("serialize provenance")
    );

    // Warm-up: untimed full sweep so first-touch allocator/page effects land
    // outside the recorded samples, symmetrically for every arm.
    for _ in 0..WARMUP_ROUNDS {
        for rows in [LEVELS[0], LEVELS[LEVELS.len() - 1]] {
            for arm in 0..ARM_NAMES.len() {
                let _ = run_arm(arm, rows);
            }
        }
    }

    let mut rng = Lcg(LCG_SEED);
    let mut samples: Vec<Sample> = Vec::with_capacity(ROUNDS * LEVELS.len() * ARM_NAMES.len());
    for round in 0..ROUNDS {
        for &rows in &LEVELS {
            let mut order = [0usize, 1, 2, 3];
            rng.shuffle(&mut order);
            for &arm in &order {
                samples.push(run_arm(arm, rows));
            }
        }
        println!(
            "round={} complete loadavg={}",
            round + 1,
            read_trimmed("/proc/loadavg").unwrap_or_default()
        );
    }

    // Per-arm regressions. Arm 0 regresses on rows (bridge calls are constant
    // at 1); the per-entry arms regress on the EXACT recorded bridge count.
    let mut arm_reports = Vec::new();
    for (arm, name) in ARM_NAMES.iter().enumerate() {
        let arm_samples: Vec<&Sample> = samples.iter().filter(|s| s.arm == arm).collect();
        let points: Vec<(f64, f64)> = arm_samples
            .iter()
            .map(|s| {
                let x = if arm == 0 {
                    s.rows as f64
                } else {
                    s.bridge_calls as f64
                };
                (x, s.elapsed_ns)
            })
            .collect();
        let (slope, intercept, r2) = ols(&points);
        let (lo, hi) = bootstrap_slope_ci(&points, arm as u64 + 1);
        let regressor = if arm == 0 { "rows" } else { "bridge_calls" };
        println!(
            "arm={name} regressor={regressor} slope_ns={slope:.1} ci95=[{lo:.1},{hi:.1}] \
             intercept_ns={intercept:.1} r2={r2:.5} n={}",
            points.len()
        );
        arm_reports.push(serde_json::json!({
            "arm": name,
            "regressor": regressor,
            "slope_ns_per_unit": slope,
            "slope_ci95": [lo, hi],
            "intercept_ns": intercept,
            "r_squared": r2,
            "n_samples": points.len(),
        }));
    }

    // Paired overhead regressions: for each (round, level) pairing order the
    // samples were interleaved within, take arm-minus-single_entry deltas and
    // regress on the exact bridge-call delta. Pairing is by (round, level)
    // position: samples are grouped 4-at-a-time per (round, level).
    let mut overhead_reports = Vec::new();
    for (arm, name) in ARM_NAMES.iter().enumerate().skip(1) {
        let mut points = Vec::new();
        for chunk in samples.chunks(ARM_NAMES.len()) {
            let base = chunk
                .iter()
                .find(|s| s.arm == 0)
                .expect("single_entry present in every (round, level) block");
            let other = chunk
                .iter()
                .find(|s| s.arm == arm)
                .expect("arm present in every (round, level) block");
            points.push((
                other.bridge_calls as f64 - base.bridge_calls as f64,
                other.elapsed_ns - base.elapsed_ns,
            ));
        }
        let (slope, intercept, r2) = ols(&points);
        let (lo, hi) = bootstrap_slope_ci(&points, 0x100 + arm as u64);
        println!(
            "overhead {name}-minus-single_entry: slope_ns_per_extra_bridge_call={slope:.1} \
             ci95=[{lo:.1},{hi:.1}] intercept_ns={intercept:.1} r2={r2:.5} pairs={}",
            points.len()
        );
        overhead_reports.push(serde_json::json!({
            "contrast": format!("{name}_minus_single_entry"),
            "slope_ns_per_extra_bridge_call": slope,
            "slope_ci95": [lo, hi],
            "intercept_ns": intercept,
            "r_squared": r2,
            "n_pairs": points.len(),
        }));
    }

    // Per-(arm, level) medians for the record.
    let mut medians = Vec::new();
    for (arm, name) in ARM_NAMES.iter().enumerate() {
        for &rows in &LEVELS {
            let mut vals: Vec<f64> = samples
                .iter()
                .filter(|s| s.arm == arm && s.rows == rows)
                .map(|s| s.elapsed_ns)
                .collect();
            let med = median(&mut vals);
            medians.push(serde_json::json!({
                "arm": name, "rows": rows, "median_elapsed_ns": med,
            }));
        }
    }

    let artifact = serde_json::json!({
        "provenance": prov,
        "loadavg_end": read_trimmed("/proc/loadavg"),
        "arm_regressions": arm_reports,
        "overhead_regressions": overhead_reports,
        "per_level_medians": medians,
        "samples": samples.iter().map(|s| serde_json::json!({
            "arm": ARM_NAMES[s.arm],
            "rows": s.rows,
            "bridge_calls": s.bridge_calls,
            "elapsed_ns": s.elapsed_ns,
        })).collect::<Vec<_>>(),
    });
    if let Some(path) = artifact_path {
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&artifact).expect("serialize artifact"),
        )
        .expect("write artifact");
        println!("artifact_written={path}");
    }
    println!(
        "interpretation: ready_control slope = pure runtime-entry cost per bridge call; \
         per_op_bridge-minus-single_entry slope = marginal cost of moving one statement's \
         execution from inside one runtime entry to its own entry (entry + scheduler/future \
         interaction); worker_facade-minus-single_entry additionally carries SQL cloning, \
         channel transit, and worker scheduling. Coefficients only — no engine attribution."
    );
}
