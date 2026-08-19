//! bd-pirr5 (GH#371): WITHOUT-ROWID teardown must be RSS-bounded.
//!
//! Dropping / tearing down a large WITHOUT-ROWID table (rows live in the PK
//! index b-tree) must free pages in bounded batches, not materialize the whole
//! row set — a 9.7 GB archive OOMed on a 128 GB host during a base-schema
//! migration because peak memory tracked total table size.
//!
//! This harness builds a WITHOUT-ROWID table of a configurable size, samples
//! process RSS from a background thread while the DROP runs, and asserts the
//! teardown's peak RSS growth stays bounded (independent of table size).
//!
//! Size is env-tunable so the same test can act as a fast CI guard or a large
//! local repro: FSQLITE_PIRR5_ROWS (default 30000), FSQLITE_PIRR5_VALUE_BYTES
//! (default 4096). Default ~= 120 MB of payload.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use fsqlite_core::connection::Connection;

fn read_vm_kb(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap_or(0);
        }
    }
    0
}

fn vm_rss_kb() -> u64 {
    read_vm_kb("VmRSS:")
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
fn bd_pirr5_without_rowid_drop_is_rss_bounded() {
    asupersync::test_utils::run_test(|| async {
        let rows = env_usize("FSQLITE_PIRR5_ROWS", 30_000);
        let value_bytes = env_usize("FSQLITE_PIRR5_VALUE_BYTES", 4096);
        let approx_table_bytes = (rows * (value_bytes + 32)) as u64;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("pirr5.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        // WITHOUT ROWID: rows are stored in the PK-index b-tree (the teardown
        // target). A large TEXT value forces overflow chains, mirroring the
        // legacy internal-content fts_messages shape.
        conn.execute("CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;")
            .await
            .unwrap();

        let value: String = "x".repeat(value_bytes);
        // Insert inside a single explicit transaction for throughput, but commit
        // periodically so the INSERT side itself does not dominate memory (we are
        // measuring the DROP, not the build).
        let batch = 2_000usize;
        let mut i = 0usize;
        while i < rows {
            conn.execute("BEGIN;").await.unwrap();
            let end = (i + batch).min(rows);
            while i < end {
                conn.execute(&format!("INSERT INTO t(k, v) VALUES ({i}, '{value}');"))
                    .await
                    .unwrap();
                i += 1;
            }
            conn.execute("COMMIT;").await.unwrap();
        }

        // Reopen so no INSERT-time in-memory state inflates the baseline; the
        // teardown then reads the table purely from disk (the migration shape).
        conn.close().await.unwrap();
        let conn = Connection::open(&db_str).await.unwrap();

        let baseline_rss = vm_rss_kb();

        // Background sampler: record peak RSS while the DROP runs.
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(baseline_rss));
        let sampler = {
            let stop = Arc::clone(&stop);
            let peak = Arc::clone(&peak);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let rss = vm_rss_kb();
                    peak.fetch_max(rss, Ordering::Relaxed);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                peak.fetch_max(vm_rss_kb(), Ordering::Relaxed);
            })
        };

        conn.execute("DROP TABLE t;").await.unwrap();

        stop.store(true, Ordering::Relaxed);
        sampler.join().unwrap();

        let peak_rss = peak.load(Ordering::Relaxed);
        let growth_kb = peak_rss.saturating_sub(baseline_rss);
        let table_kb = approx_table_bytes / 1024;

        eprintln!(
            "BD_PIRR5 rows={rows} value_bytes={value_bytes} approx_table_kb={table_kb} \
             baseline_rss_kb={baseline_rss} peak_rss_kb={peak_rss} drop_growth_kb={growth_kb}"
        );

        conn.close().await.unwrap();

        // Bounded teardown: the DROP's peak RSS growth must NOT scale with table
        // size. Allow a generous fixed working-set cap (128 MB) that is
        // independent of `rows` — a materializing teardown blows past it as the
        // table grows (~table_kb of growth), a batched one stays flat.
        let cap_kb = 128 * 1024;
        assert!(
            growth_kb < cap_kb,
            "bd-pirr5: WITHOUT-ROWID DROP teardown grew RSS by {growth_kb} kB \
             (~table size {table_kb} kB) — must stay under {cap_kb} kB working-set bound"
        );
    });
}
