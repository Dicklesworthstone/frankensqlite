//! Focused before/after benchmark for the AAC-P6 statement micro-batcher.
//!
//! Runs the exact `INSERTThroughput — Single Transaction — tiny_1col` 100-row
//! scenario from `comprehensive_bench` in the same process with the
//! micro-batcher in four configurations: off, default 16-row epoch, 64-row
//! epoch, and 256-row epoch. Reports rust-hot-cache-warm timings side by side
//! so the only changing variable is the micro-batcher itself.

// bd-mnlk2 / bd-zavyn: the hoisted timed bodies await fsqlite-core's
// deliberately large, deeply nested engine futures inside one runtime entry
// per sample; boxing them would put an allocation inside the timed window.
#![allow(clippy::large_futures)]

use std::time::Instant;

const WARMUP: usize = 4;
const ITERS: usize = 60;
const ROW_COUNTS: &[usize] = &[100, 1_000, 10_000];

#[derive(Clone, Copy)]
struct Config {
    label: &'static str,
    enabled: bool,
    max_r: Option<u32>,
}

const CONFIGS: &[Config] = &[
    Config {
        label: "OFF",
        enabled: false,
        max_r: None,
    },
    Config {
        label: "r=16",
        enabled: true,
        max_r: Some(16),
    },
    Config {
        label: "r=64",
        enabled: true,
        max_r: Some(64),
    },
    Config {
        label: "r=256",
        enabled: true,
        max_r: Some(256),
    },
];

fn measure(cfg: Config, rows: usize) -> (Vec<u64>, u64) {
    let mut samples = Vec::with_capacity(ITERS);
    let mut total_hits = 0u64;
    for run in 0..(WARMUP + ITERS) {
        let conn = fsqlite_e2e::block_on(fsqlite::Connection::open(":memory:")).unwrap();
        let pragma = if cfg.enabled {
            "PRAGMA fsqlite.stmt_microbatch = ON;"
        } else {
            "PRAGMA fsqlite.stmt_microbatch = OFF;"
        };
        fsqlite_e2e::block_on(conn.execute(pragma)).unwrap();
        if let Some(r) = cfg.max_r {
            fsqlite_e2e::block_on(
                conn.execute(&format!("PRAGMA fsqlite.stmt_microbatch_max_r = {r};")),
            )
            .unwrap();
        }
        fsqlite_e2e::block_on(conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY)")).unwrap();
        fsqlite_e2e::block_on(conn.execute("BEGIN")).unwrap();
        let stmt = fsqlite_e2e::block_on(conn.prepare("INSERT INTO bench VALUES (?1)")).unwrap();
        // bd-mnlk2 / bd-zavyn: one runtime entry for the whole timed
        // insert+commit window, so the sample measures the engine rather
        // than a ~333 ns bridge entry per row.
        let start = Instant::now();
        fsqlite_e2e::block_on(async {
            #[allow(clippy::cast_possible_wrap)]
            for i in 0..rows as i64 {
                stmt.execute_with_params(&[fsqlite::SqliteValue::Integer(i)])
                    .await
                    .unwrap();
            }
            conn.execute("COMMIT").await.unwrap();
        });
        let elapsed = start.elapsed();
        drop(stmt);
        if cfg.enabled {
            total_hits += core_microbatch_hits(&conn);
        }
        if run >= WARMUP {
            samples.push(elapsed.as_nanos() as u64);
        }
    }
    samples.sort_unstable();
    (samples, total_hits)
}

fn core_microbatch_hits(conn: &fsqlite::Connection) -> u64 {
    let rows = fsqlite_e2e::block_on(conn.query("PRAGMA fsqlite.stmt_microbatch_stats;"))
        .unwrap_or_default();
    if let Some(row) = rows.first() {
        if let Some(fsqlite::SqliteValue::Text(text)) = row.values().first() {
            for field in text.split_whitespace() {
                if let Some(val) = field.strip_prefix("hits=") {
                    return val.parse().unwrap_or(0);
                }
            }
        }
    }
    0
}

fn summary(samples: &[u64]) -> (u64, u64, u64) {
    let min = *samples.first().unwrap();
    let median = samples[samples.len() / 2];
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let p90 = samples[(samples.len() as f64 * 0.9) as usize];
    (min, median, p90)
}

fn main() {
    println!(
        "microbatch_bench: warmup={WARMUP} iters={ITERS} scenario=tiny_1col single-txn INSERT"
    );
    println!("  each data point reports (min/median/p90) of per-cycle wall time (us)");
    for &rows in ROW_COUNTS {
        println!("\n=== {rows} rows ===");
        let mut baseline_median: Option<u64> = None;
        for &cfg in CONFIGS {
            let (samples, hits) = measure(cfg, rows);
            let (min, med, p90) = summary(&samples);
            if cfg.label == "OFF" {
                baseline_median = Some(med);
            }
            let speedup = baseline_median.map_or(1.0, |b| b as f64 / med as f64);
            let hits_per_run = hits / (WARMUP + ITERS) as u64;
            println!(
                "  {:<6} | min={:7.1}us med={:7.1}us p90={:7.1}us | speedup={:5.3}x hits/run={:>6}",
                cfg.label,
                min as f64 / 1000.0,
                med as f64 / 1000.0,
                p90 as f64 / 1000.0,
                speedup,
                hits_per_run
            );
        }
    }
}
