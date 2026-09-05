//! Page cache correctness, latency, and collision handling E2E tests (bd-aztlm).
//!
//! Tests the ShardedPageCache (with FastPageArray fast path) via both direct
//! cache API and through real SQL execution, verifying correctness under
//! insert/get/remove/clear cycles, concurrent reads, and collision scenarios.
//!
//! ## Scenarios
//!
//! | ID | Name                          | Description                                          |
//! |----|-------------------------------|------------------------------------------------------|
//! | Q1 | basic_insert_get              | Insert 100 pages, get each by pgno, verify data      |
//! | Q2 | collision_chain               | Force collisions in same shard, verify all retrievable|
//! | Q3 | resize_beyond_initial         | Insert beyond initial capacity, verify all present    |
//! | Q4 | remove_and_reclaim            | Insert then remove, verify space reclaimed            |
//! | Q5 | lookup_latency                | Measure ns/lookup, must be < 500ns for cached pages   |
//! | Q6 | concurrent_reads              | 8 threads reading same cache simultaneously           |
//! | Q7 | e2e_insert_10k_oracle         | 10K row INSERT, verify cache serves correct pages     |
//! | Q8 | e2e_concurrent_writers        | 4 concurrent writers, verify no data loss             |
//! | Q9 | cache_budget_live_effect     | SQL cache settings change real residency and evictions |
//! | Q10 | cache_budget_prepared       | Prepared setters and actual page-size conversion       |
//! | Q11 | cache_budget_transactions   | Oversized write transactions commit and roll back      |
//! | Q12 | cache_budget_connections    | Independent budgets, overlapping transactions, close   |
//! | Q13 | cache_budget_schemas        | ATTACH and TEMP settings stay isolated                 |
//! | Q14 | cache_budget_default_reopen | Header-backed defaults take effect and survive reopen  |
//! | Q15 | cache_budget_readonly       | Failed header writes still change the runtime budget   |
//! | Q16 | cache_budget_default_abort  | Main and ATTACH rollback restore only persisted defaults |
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_aztlm_page_cache_e2e -- --nocapture --test-threads=1
//! ```

#![allow(clippy::cast_precision_loss)]
#![recursion_limit = "512"]

use fsqlite_pager::ShardedPageCache;
use fsqlite_types::{PageNumber, PageSize};
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::time::Instant;

const BEAD_ID: &str = "bd-aztlm";
const REPLAY_CMD: &str =
    "cargo test -p fsqlite-e2e --test bd_aztlm_page_cache_e2e -- --nocapture --test-threads=1";

fn emit_log(test_name: &str, phase: &str, data: serde_json::Value) {
    eprintln!(
        "PAGE_CACHE_E2E:{}",
        json!({
            "bead_id": BEAD_ID,
            "test": test_name,
            "phase": phase,
            "replay_command": REPLAY_CMD,
            "data": data,
        })
    );
}

fn page_pattern(page_no: u32) -> u8 {
    (page_no.wrapping_mul(37).wrapping_add(11) & 0xFF) as u8
}

fn fill_page(cache: &ShardedPageCache, pgno: PageNumber) {
    let pattern = page_pattern(pgno.get());
    loop {
        match cache.insert_fresh(pgno, |data| {
            data.fill(pattern);
            data[..4].copy_from_slice(&pgno.get().to_le_bytes());
        }) {
            Ok(()) => return,
            Err(fsqlite_error::FrankenError::OutOfMemory) => {
                assert!(cache.evict_any(), "must be able to evict when OOM");
            }
            Err(e) => panic!("insert_fresh failed for page {}: {e}", pgno.get()),
        }
    }
}

fn verify_page(data: &[u8], pgno: PageNumber) {
    let expected_header = pgno.get().to_le_bytes();
    assert_eq!(
        &data[..4],
        &expected_header,
        "page {} header mismatch",
        pgno.get()
    );
    let expected_pattern = page_pattern(pgno.get());
    assert_eq!(
        data[4],
        expected_pattern,
        "page {} pattern byte mismatch",
        pgno.get()
    );
}

// ─── Q1: Basic insert + get ──────────────────────────────────────────

#[test]
fn q1_basic_insert_get() {
    let tn = "q1_basic_insert_get";
    let page_count = 100u32;
    emit_log(tn, "start", json!({"pages": page_count}));

    let cache = ShardedPageCache::new(PageSize::DEFAULT);

    for i in 1..=page_count {
        let pgno = PageNumber::new(i).unwrap();
        fill_page(&cache, pgno);
    }

    assert_eq!(cache.len(), page_count as usize);

    let mut mismatches = 0u64;
    for i in 1..=page_count {
        let pgno = PageNumber::new(i).unwrap();
        let found = cache.with_page(pgno, |data| {
            let header = u32::from_le_bytes(data[..4].try_into().unwrap());
            if header != i {
                mismatches += 1;
            }
            let pattern = page_pattern(i);
            if data[4] != pattern {
                mismatches += 1;
            }
        });
        assert!(found.is_some(), "page {i} not found in cache");
    }

    let snap = cache.metrics_lightweight_snapshot();
    emit_log(
        tn,
        "result",
        json!({
            "pages": page_count,
            "mismatches": mismatches,
            "cache_len": cache.len(),
            "hits": snap.hits,
            "misses": snap.misses,
        }),
    );

    assert_eq!(mismatches, 0, "[Q1] data mismatch in basic insert/get");
}

// ─── Q2: Collision chain ─────────────────────────────────────────────

#[test]
fn q2_collision_chain() {
    let tn = "q2_collision_chain";
    emit_log(tn, "start", json!({}));

    let shard_count = 4usize;
    let cache = ShardedPageCache::with_max_buffers_and_shards(PageSize::DEFAULT, 512, shard_count);

    // Insert pages that will hash to the same shard (sequential pages mod shard_count)
    let pages_per_shard = 50u32;
    let total_pages = pages_per_shard * shard_count as u32;

    for i in 1..=total_pages {
        let pgno = PageNumber::new(i).unwrap();
        fill_page(&cache, pgno);
    }

    // Verify every page is retrievable
    let mut found_count = 0u64;
    let mut mismatches = 0u64;
    for i in 1..=total_pages {
        let pgno = PageNumber::new(i).unwrap();
        if let Some(data) = cache.get_copy(pgno) {
            found_count += 1;
            let header = u32::from_le_bytes(data[..4].try_into().unwrap());
            if header != i {
                mismatches += 1;
            }
        }
    }

    let dist = cache.shard_distribution();
    emit_log(
        tn,
        "result",
        json!({
            "total_pages": total_pages,
            "found": found_count,
            "mismatches": mismatches,
            "shard_distribution": dist,
        }),
    );

    assert_eq!(
        found_count,
        u64::from(total_pages),
        "[Q2] some pages not found"
    );
    assert_eq!(mismatches, 0, "[Q2] data corruption in collision chain");
}

// ─── Q3: Resize beyond initial capacity ──────────────────────────────

#[test]
fn q3_resize_beyond_initial() {
    let tn = "q3_resize";
    emit_log(tn, "start", json!({}));

    let cache = ShardedPageCache::with_max_buffers(PageSize::DEFAULT, 2048);

    let page_count = 1500u32;
    for i in 1..=page_count {
        let pgno = PageNumber::new(i).unwrap();
        fill_page(&cache, pgno);
    }

    assert!(
        cache.len() >= page_count as usize,
        "[Q3] cache should hold all {page_count} pages, got {}",
        cache.len()
    );

    let mut verified = 0u64;
    for i in 1..=page_count {
        let pgno = PageNumber::new(i).unwrap();
        if cache.contains(pgno) {
            cache.with_page(pgno, |data| {
                verify_page(data, pgno);
            });
            verified += 1;
        }
    }

    emit_log(
        tn,
        "result",
        json!({
            "inserted": page_count,
            "verified": verified,
            "cache_len": cache.len(),
        }),
    );

    assert_eq!(
        verified,
        u64::from(page_count),
        "[Q3] not all pages verified after resize"
    );
}

// ─── Q4: Remove and reclaim ──────────────────────────────────────────

#[test]
fn q4_remove_and_reclaim() {
    let tn = "q4_remove_reclaim";
    emit_log(tn, "start", json!({}));

    let cache = ShardedPageCache::new(PageSize::DEFAULT);

    let page_count = 50u32;
    for i in 1..=page_count {
        let pgno = PageNumber::new(i).unwrap();
        fill_page(&cache, pgno);
    }

    let len_before = cache.len();
    assert_eq!(len_before, page_count as usize);

    // Remove even-numbered pages
    let mut removed = 0u32;
    for i in (2..=page_count).step_by(2) {
        let pgno = PageNumber::new(i).unwrap();
        if cache.evict(pgno) {
            removed += 1;
        }
    }

    let len_after = cache.len();

    // Verify odd pages still present, even pages gone
    let mut odd_ok = 0u32;
    let mut even_gone = 0u32;
    for i in 1..=page_count {
        let pgno = PageNumber::new(i).unwrap();
        if i % 2 == 1 {
            if cache.contains(pgno) {
                odd_ok += 1;
            }
        } else if !cache.contains(pgno) {
            even_gone += 1;
        }
    }

    emit_log(
        tn,
        "result",
        json!({
            "inserted": page_count,
            "removed": removed,
            "len_before": len_before,
            "len_after": len_after,
            "odd_present": odd_ok,
            "even_removed": even_gone,
        }),
    );

    assert_eq!(removed, page_count / 2, "[Q4] removal count mismatch");
    assert_eq!(
        len_after,
        (page_count - removed) as usize,
        "[Q4] cache len after removal"
    );
    assert_eq!(
        odd_ok,
        page_count.div_ceil(2),
        "[Q4] odd pages should remain"
    );
    assert_eq!(even_gone, page_count / 2, "[Q4] even pages should be gone");
}

// ─── Q5: Lookup latency ─────────────────────────────────────────────

#[test]
fn q5_lookup_latency() {
    let tn = "q5_latency";
    emit_log(tn, "start", json!({}));

    let cache = ShardedPageCache::new(PageSize::DEFAULT);

    let page_count = 200u32;
    for i in 1..=page_count {
        let pgno = PageNumber::new(i).unwrap();
        fill_page(&cache, pgno);
    }

    // Warm: access each page once
    for i in 1..=page_count {
        let pgno = PageNumber::new(i).unwrap();
        let _ = cache.contains(pgno);
    }

    let lookup_count = 10_000u64;
    let start = Instant::now();
    for round in 0..lookup_count {
        let i = (round as u32 % page_count) + 1;
        let pgno = PageNumber::new(i).unwrap();
        let _ = std::hint::black_box(cache.contains(pgno));
    }
    let elapsed = start.elapsed();
    let avg_ns = elapsed.as_nanos() as f64 / lookup_count as f64;

    let snap = cache.metrics_lightweight_snapshot();
    emit_log(
        tn,
        "result",
        json!({
            "lookup_count": lookup_count,
            "elapsed_ns": elapsed.as_nanos() as u64,
            "avg_ns_per_lookup": avg_ns,
            "hits": snap.hits,
            "misses": snap.misses,
        }),
    );

    assert!(
        avg_ns < 500.0,
        "[Q5] avg lookup {avg_ns:.1}ns exceeds 500ns threshold"
    );
}

// ─── Q6: Concurrent reads ────────────────────────────────────────────

#[test]
fn q6_concurrent_reads() {
    let tn = "q6_concurrent_reads";
    let thread_count = 8usize;
    let page_count = 200u32;
    emit_log(
        tn,
        "start",
        json!({"threads": thread_count, "pages": page_count}),
    );

    let cache = Arc::new(ShardedPageCache::new(PageSize::DEFAULT));

    // Pre-populate
    for i in 1..=page_count {
        let pgno = PageNumber::new(i).unwrap();
        fill_page(&cache, pgno);
    }

    let barrier = Arc::new(Barrier::new(thread_count));
    let reads_per_thread = 5_000u64;

    let handles: Vec<_> = (0..thread_count)
        .map(|tid| {
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let mut hits = 0u64;
                let mut misses = 0u64;
                let mut mismatches = 0u64;

                for r in 0..reads_per_thread {
                    let i = ((r as u32 + tid as u32 * 7) % page_count) + 1;
                    let pgno = PageNumber::new(i).unwrap();
                    match cache.get_copy(pgno) {
                        Some(data) => {
                            hits += 1;
                            let header = u32::from_le_bytes(data[..4].try_into().unwrap());
                            if header != i {
                                mismatches += 1;
                            }
                        }
                        None => misses += 1,
                    }
                }

                (hits, misses, mismatches)
            })
        })
        .collect();

    let mut total_hits = 0u64;
    let mut total_misses = 0u64;
    let mut total_mismatches = 0u64;
    for h in handles {
        let (hits, misses, mm) = h.join().unwrap();
        total_hits += hits;
        total_misses += misses;
        total_mismatches += mm;
    }

    emit_log(
        tn,
        "result",
        json!({
            "threads": thread_count,
            "total_reads": reads_per_thread * thread_count as u64,
            "hits": total_hits,
            "misses": total_misses,
            "mismatches": total_mismatches,
        }),
    );

    assert_eq!(
        total_mismatches, 0,
        "[Q6] data corruption under concurrent reads"
    );
    assert!(total_hits > 0, "[Q6] expected at least some cache hits");
}

// ─── Q7: 10K INSERT E2E oracle comparison ────────────────────────────

#[test]
fn q7_e2e_insert_10k_oracle() {
    asupersync::test_utils::run_test(|| async {
        let tn = "q7_insert_10k_oracle";
        let row_count = 10_000i64;
        emit_log(tn, "start", json!({"rows": row_count}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        fconn
            .execute("CREATE TABLE cache_test (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)")
            .await
            .unwrap();
        cconn
            .execute_batch(
                "CREATE TABLE cache_test (id INTEGER PRIMARY KEY, val INTEGER, label TEXT);",
            )
            .unwrap();

        let insert_start = Instant::now();
        fconn.execute("BEGIN").await.unwrap();
        cconn.execute_batch("BEGIN;").unwrap();
        for i in 0..row_count {
            let val = i * 13 + 7;
            let label = format!("cache_{i:06}");
            fconn
                .execute(&format!(
                    "INSERT INTO cache_test VALUES ({i}, {val}, '{label}')"
                ))
                .await
                .unwrap();
            cconn
                .execute(
                    "INSERT INTO cache_test VALUES (?1, ?2, ?3)",
                    rusqlite::params![i, val, label],
                )
                .unwrap();
        }
        fconn.execute("COMMIT").await.unwrap();
        cconn.execute_batch("COMMIT;").unwrap();
        let insert_ns = insert_start.elapsed().as_nanos() as u64;

        // Full scan — exercises page cache read path
        let scan_start = Instant::now();
        let f_rows = fconn
            .query("SELECT id, val, label FROM cache_test ORDER BY id")
            .await
            .unwrap();
        let scan_ns = scan_start.elapsed().as_nanos() as u64;

        let c_rows: Vec<(i64, i64, String)> = {
            let mut stmt = cconn
                .prepare("SELECT id, val, label FROM cache_test ORDER BY id")
                .unwrap();
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };

        assert_eq!(f_rows.len(), c_rows.len(), "[Q7] row count mismatch");

        let mut mismatches = 0u64;
        for (i, (f_row, c_row)) in f_rows.iter().zip(c_rows.iter()).enumerate() {
            let f_vals = f_row.values();
            let f_id = match &f_vals[0] {
                fsqlite_types::value::SqliteValue::Integer(n) => *n,
                other => panic!("row {i}: unexpected id: {other:?}"),
            };
            let f_val = match &f_vals[1] {
                fsqlite_types::value::SqliteValue::Integer(n) => *n,
                other => panic!("row {i}: unexpected val: {other:?}"),
            };
            let f_label = match &f_vals[2] {
                fsqlite_types::value::SqliteValue::Text(s) => s.as_str().to_owned(),
                other => panic!("row {i}: unexpected label: {other:?}"),
            };

            if f_id != c_row.0 || f_val != c_row.1 || f_label != c_row.2 {
                mismatches += 1;
            }
        }

        emit_log(
            tn,
            "result",
            json!({
                "rows": row_count,
                "insert_ns": insert_ns,
                "scan_ns": scan_ns,
                "mismatches": mismatches,
            }),
        );

        assert_eq!(mismatches, 0, "[Q7] {mismatches} mismatches in 10K oracle");
    });
}

// ─── Q8: 4 concurrent writers, no data loss ──────────────────────────

#[test]
fn q8_e2e_concurrent_writers() {
    asupersync::test_utils::run_test(|| async {
        let tn = "q8_concurrent_writers";
        let thread_count = 4usize;
        let rows_per_thread = 500i64;
        emit_log(
            tn,
            "start",
            json!({"threads": thread_count, "rows_per_thread": rows_per_thread}),
        );

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q8.db");
        let path_str = db_path.to_str().unwrap().to_owned();

        // Setup: create table on a fresh connection
        {
            let conn = fsqlite::Connection::open(&path_str).await.unwrap();
            conn.execute(
                "CREATE TABLE writers (tid INTEGER, seq INTEGER, val INTEGER, PRIMARY KEY (tid, seq))",
            )
            .await
            .unwrap();
        }

        let barrier = Arc::new(Barrier::new(thread_count));
        let path_arc = Arc::new(path_str.clone());

        let handles: Vec<_> = (0..thread_count)
            .map(|tid| {
                let barrier = Arc::clone(&barrier);
                let path = Arc::clone(&path_arc);
                std::thread::spawn(move || {
                    let mut written = 0i64;
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(path.as_str()).await.unwrap();
                        barrier.wait();

                        let batch = 50i64;
                        let mut seq = 0i64;
                        while seq < rows_per_thread {
                            let end = (seq + batch).min(rows_per_thread);
                            let max_retries = 50;
                            let mut attempt = 0;
                            loop {
                                attempt += 1;
                                conn.execute("BEGIN").await.unwrap();
                                let mut batch_ok = true;
                                for s in seq..end {
                                    let val = tid as i64 * 10000 + s;
                                    if conn
                                        .execute(&format!(
                                            "INSERT INTO writers VALUES ({tid}, {s}, {val})"
                                        ))
                                        .await
                                        .is_err()
                                    {
                                        batch_ok = false;
                                        break;
                                    }
                                }
                                if batch_ok {
                                    match conn.execute("COMMIT").await {
                                        Ok(_) => {
                                            written += end - seq;
                                            break;
                                        }
                                        Err(_) => {
                                            drop(conn.execute("ROLLBACK").await);
                                        }
                                    }
                                } else {
                                    drop(conn.execute("ROLLBACK").await);
                                }
                                assert!(
                                    attempt < max_retries,
                                    "thread {tid} exceeded {max_retries} retries at seq={seq}"
                                );
                                std::thread::sleep(std::time::Duration::from_millis(
                                    1 + (attempt as u64 * tid as u64) % 5,
                                ));
                            }
                            seq = end;
                        }
                    });

                    written
                })
            })
            .collect();

        let mut total_written = 0i64;
        for h in handles {
            total_written += h.join().unwrap();
        }

        // Verify: open fresh connection, count rows, check no duplicates
        let verify_conn = fsqlite::Connection::open(&path_str).await.unwrap();
        let count_rows = verify_conn
            .query("SELECT COUNT(*) FROM writers")
            .await
            .unwrap();
        let actual_count = match &count_rows[0].values()[0] {
            fsqlite_types::value::SqliteValue::Integer(n) => *n,
            other => panic!("unexpected count: {other:?}"),
        };

        let expected = thread_count as i64 * rows_per_thread;

        // Also verify against csqlite
        let cconn = rusqlite::Connection::open(db_path.to_str().unwrap()).unwrap();
        let c_count: i64 = cconn
            .query_row("SELECT COUNT(*) FROM writers", [], |r| r.get(0))
            .unwrap();

        emit_log(
            tn,
            "result",
            json!({
                "threads": thread_count,
                "rows_per_thread": rows_per_thread,
                "total_written": total_written,
                "fsqlite_count": actual_count,
                "csqlite_count": c_count,
                "expected": expected,
            }),
        );

        assert_eq!(
            actual_count, expected,
            "[Q8] fsqlite row count: expected {expected}, got {actual_count}"
        );
        assert_eq!(
            c_count, expected,
            "[Q8] csqlite verification: expected {expected}, got {c_count}"
        );
    });
}

// bd-dwjnq.2: these keepers use the public SQL facade and real file-backed
// pages. The budget is clean cache residency, not transaction staging, free
// pool buffers, returned SQL rows, TEMP storage, or a process-memory ceiling.
const CACHE_BUDGET_SELECT: &str = "SELECT id, payload FROM cache_budget ORDER BY id";
type CacheBudgetRows = Vec<(i64, String)>;

struct CacheBudgetRun {
    test: &'static str,
    run_id: String,
    revision: String,
}

impl CacheBudgetRun {
    fn new(test: &'static str) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let revision = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .stderr(std::process::Stdio::inherit())
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map_or_else(
                || {
                    std::env::var("FSQLITE_TEST_SOURCE_REVISION")
                        .unwrap_or_else(|_| "unavailable; see verifier source manifest".to_owned())
                },
                |value| value.trim().to_owned(),
            );
        Self {
            test,
            run_id: format!("bd-dwjnq.2-{}-{timestamp}-{test}", std::process::id()),
            revision,
        }
    }

    fn record(&self, phase: &str, conn: &fsqlite::Connection, data: serde_json::Value) {
        let stats = conn.memory_stats().unwrap();
        let peak = conn.page_cache_peak_snapshot().unwrap();
        eprintln!(
            "PAGE_CACHE_E2E:{}",
            json!({
                "bead_id": "bd-dwjnq.2",
                "run_id": self.run_id,
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis(),
                "test": self.test,
                "phase": phase,
                "event_type": "cache_budget_observation",
                "source_revision": self.revision,
                "source_note": "working-tree tests; verifier must also record source diff",
                "platform": std::env::consts::OS,
                "architecture": std::env::consts::ARCH,
                "sqlite_oracle_version": rusqlite::version(),
                "replay_command": REPLAY_CMD,
                "page_size_bytes": stats.page_size_bytes,
                "cache_page_budget": stats.page_cache.cache_page_budget,
                "cached_pages": stats.page_cache.cached_pages,
                "resident_bytes": stats.page_cache_used_bytes(),
                "buffer_pool_capacity": stats.page_cache.pool_capacity,
                "evictions": stats.page_cache.evictions,
                "admits": stats.page_cache.admits,
                "hits": stats.page_cache.hits,
                "misses": stats.page_cache.misses,
                "dirty_ratio_pct": stats.page_cache.dirty_ratio_pct,
                "peak_cached_pages": peak.peak_cached_pages,
                "peak_exact": peak.exact,
                "data": data,
            })
        );
    }

    fn assert_quiescent_budget(&self, phase: &str, conn: &fsqlite::Connection, pages: usize) {
        self.record(phase, conn, json!({"expected_budget_pages": pages}));
        let stats = conn.memory_stats().unwrap();
        assert!(
            stats.page_cache.cached_pages <= pages,
            "{phase}: completed operation retained {} pages with budget {pages}",
            stats.page_cache.cached_pages
        );
        assert_eq!(stats.page_cache.cache_page_budget, pages, "{phase}");
        assert!(stats.page_cache_used_bytes() <= pages.saturating_mul(stats.page_size_bytes));
    }
}

fn cache_budget_oracle_rows(path: &std::path::Path) -> CacheBudgetRows {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.prepare(CACHE_BUDGET_SELECT)
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn seed_cache_budget_database(path: &std::path::Path, page_size: usize) -> CacheBudgetRows {
    let mut conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(&format!(
        "PRAGMA page_size={page_size};\
         CREATE TABLE cache_budget(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);"
    ))
    .unwrap();
    let tx = conn.transaction().unwrap();
    for id in 1_i64..=64 {
        let payload = format!("{id:04}:{}", "p".repeat(page_size / 2));
        tx.execute(
            "INSERT INTO cache_budget VALUES (?1, ?2)",
            rusqlite::params![id, payload],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let actual_page_size: usize = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap();
    let page_count: usize = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    assert_eq!(actual_page_size, page_size);
    assert!(page_count > 8, "fixture must exceed the test budget");
    conn.close().unwrap();
    cache_budget_oracle_rows(path)
}

async fn cache_budget_integer(conn: &fsqlite::Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.unwrap();
    assert_eq!(rows.len(), 1, "{sql}");
    match rows[0].values() {
        [fsqlite_types::value::SqliteValue::Integer(value)] => *value,
        other => panic!("{sql}: expected one integer, got {other:?}"),
    }
}

async fn cache_budget_assert_rows(
    conn: &fsqlite::Connection,
    sql: &str,
    expected: &[(i64, String)],
) {
    use fsqlite_types::value::SqliteValue;
    let rows = conn.query(sql).await.unwrap();
    let actual: CacheBudgetRows = rows
        .iter()
        .map(|row| match row.values() {
            [SqliteValue::Integer(id), SqliteValue::Text(payload)] => (*id, payload.to_string()),
            other => panic!("{sql}: unexpected row {other:?}"),
        })
        .collect();
    assert_eq!(actual.as_slice(), expected, "{sql}");
}

fn cache_budget_pages(setting: i64, actual_page_size: usize) -> usize {
    if setting < 0 {
        usize::try_from(setting.unsigned_abs() * 1024 / u64::try_from(actual_page_size).unwrap())
            .unwrap()
    } else {
        usize::try_from(setting).unwrap()
    }
}

#[test]
fn q9_cache_budget_live_effect() {
    asupersync::test_utils::run_test(|| async {
        let run = CacheBudgetRun::new("q9_cache_budget_live_effect");
        let dir = tempfile::tempdir().unwrap();
        for page_size in [4096, 8192] {
            let path = dir.path().join(format!("budget-{page_size}.db"));
            let expected = seed_cache_budget_database(&path, page_size);
            let conn = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            let oracle = rusqlite::Connection::open_in_memory().unwrap();
            let default_setting: i64 = oracle
                .query_row("PRAGMA cache_size", [], |r| r.get(0))
                .unwrap();
            assert_eq!(conn.memory_stats().unwrap().page_size_bytes, page_size);
            run.assert_quiescent_budget(
                "bootstrap_default",
                &conn,
                cache_budget_pages(default_setting, page_size),
            );
            for requested in [
                "8",
                "-32",
                "0",
                "2147483647",
                "2147483648",
                "-2147483648",
                "-2147483649",
                "9223372036854775807",
                "-9223372036854775808",
                "9223372036854775808",
                "1e3",
                "'123abc'",
                "abc",
            ] {
                conn.execute("PRAGMA cache_size=256").await.unwrap();
                cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &expected).await;
                let before = conn.memory_stats().unwrap();
                run.record("warmed", &conn, json!({"next_setting": requested}));
                assert!(
                    before.page_cache.cached_pages > 8,
                    "warm-up must create eviction work"
                );
                let setter = format!("PRAGMA cache_size={requested}");
                oracle.execute_batch(&setter).unwrap();
                let normalized: i64 = oracle
                    .query_row("PRAGMA cache_size", [], |r| r.get(0))
                    .unwrap();
                let budget = cache_budget_pages(normalized, page_size);
                conn.execute(&setter).await.unwrap();
                assert_eq!(
                    cache_budget_integer(&conn, "PRAGMA cache_size").await,
                    normalized
                );
                run.assert_quiescent_budget("after_setter", &conn, budget);
                let after = conn.memory_stats().unwrap();
                assert_eq!(
                    after.page_cache.pool_capacity,
                    before.page_cache.pool_capacity
                );
                if budget < before.page_cache.cached_pages {
                    assert!(after.page_cache.evictions > before.page_cache.evictions);
                }
                conn.reset_page_cache_peak_residency().unwrap();
                cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &expected).await;
                run.assert_quiescent_budget("after_full_scan", &conn, budget);
            }
            conn.close().await.unwrap();
            assert_eq!(cache_budget_oracle_rows(&path), expected);
        }
    });
}

#[test]
fn q10_cache_budget_prepared_and_actual_page_size() {
    asupersync::test_utils::run_test(|| async {
        let run = CacheBudgetRun::new("q10_cache_budget_prepared_and_actual_page_size");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prepared.db");
        let expected = seed_cache_budget_database(&path, 8192);
        let conn = fsqlite::Connection::open(path.to_str().unwrap())
            .await
            .unwrap();
        conn.execute("PRAGMA cache_size=256").await.unwrap();
        cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &expected).await;
        let before = conn.memory_stats().unwrap();
        assert!(before.page_cache.cached_pages > 8);
        {
            let setter = conn.prepare("PRAGMA cache_size=8").await.unwrap();
            setter.execute().await.unwrap();
        }
        run.assert_quiescent_budget("prepared_setter", &conn, 8);
        assert!(conn.memory_stats().unwrap().page_cache.evictions > before.page_cache.evictions);
        assert_eq!(cache_budget_integer(&conn, "PRAGMA cache_size").await, 8);

        // Requesting a different page size on an existing file does not change
        // its physical page size without rebuilding it. KiB uses the real pager.
        conn.execute("PRAGMA page_size=4096").await.unwrap();
        assert_eq!(conn.memory_stats().unwrap().page_size_bytes, 8192);
        conn.execute("PRAGMA cache_size=-32").await.unwrap();
        run.assert_quiescent_budget("negative_uses_actual_page_size", &conn, 4);
        conn.execute("PRAGMA page_size=16384").await.unwrap();
        run.assert_quiescent_budget("converted_page_count_stays_frozen", &conn, 4);
        cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &expected).await;
        run.assert_quiescent_budget("prepared_scan", &conn, 4);
        conn.close().await.unwrap();
        assert_eq!(cache_budget_oracle_rows(&path), expected);
    });
}

#[test]
fn q11_cache_budget_oversized_transactions() {
    asupersync::test_utils::run_test(|| async {
        use fsqlite_types::value::SqliteValue;
        let run = CacheBudgetRun::new("q11_cache_budget_oversized_transactions");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("transactions.db");
        let initial = seed_cache_budget_database(&path, 4096);
        let mut committed: CacheBudgetRows = initial
            .iter()
            .map(|(id, text)| (*id, format!("{text}:committed")))
            .collect();
        let conn = fsqlite::Connection::open(path.to_str().unwrap())
            .await
            .unwrap();
        conn.execute("PRAGMA cache_size=256").await.unwrap();
        conn.execute("BEGIN").await.unwrap();
        let updated = conn
            .execute("UPDATE cache_budget SET payload=payload || ':committed'")
            .await
            .unwrap();
        assert_eq!(updated, initial.len());
        let mut inserted = 0;
        for (id, payload) in &initial {
            let new_id = id + 100;
            inserted += conn
                .execute_with_params(
                    "INSERT INTO cache_budget VALUES (?1, ?2)",
                    &[
                        SqliteValue::Integer(new_id),
                        SqliteValue::Text(payload.clone().into()),
                    ],
                )
                .await
                .unwrap();
            committed.push((new_id, payload.clone()));
        }
        assert_eq!(inserted, initial.len());
        conn.execute("PRAGMA cache_size=8").await.unwrap();
        run.record(
            "dirty_transaction_shrink",
            &conn,
            json!({"updated_rows": updated, "inserted_rows": inserted}),
        );
        conn.execute("COMMIT").await.unwrap();
        run.assert_quiescent_budget("after_commit", &conn, 8);
        cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &committed).await;
        run.assert_quiescent_budget("committed_scan", &conn, 8);

        conn.execute("PRAGMA cache_size=256").await.unwrap();
        conn.execute("BEGIN").await.unwrap();
        let updated = conn
            .execute("UPDATE cache_budget SET payload=payload || ':must-not-survive'")
            .await
            .unwrap();
        let deleted = conn
            .execute("DELETE FROM cache_budget WHERE id <= 16")
            .await
            .unwrap();
        assert_eq!(updated, committed.len());
        assert_eq!(deleted, 16);
        conn.execute("PRAGMA cache_size=0").await.unwrap();
        run.record(
            "dirty_transaction_zero",
            &conn,
            json!({"updated_rows": updated, "deleted_rows": deleted}),
        );
        conn.execute("ROLLBACK").await.unwrap();
        run.assert_quiescent_budget("after_rollback", &conn, 0);
        cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &committed).await;
        run.assert_quiescent_budget("rolled_back_scan", &conn, 0);
        conn.close().await.unwrap();
        assert_eq!(cache_budget_oracle_rows(&path), committed);

        let reopened = fsqlite::Connection::open(path.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(
            cache_budget_integer(&reopened, "PRAGMA cache_size").await,
            -2000
        );
        run.assert_quiescent_budget("connection_setting_did_not_persist", &reopened, 500);
        cache_budget_assert_rows(&reopened, CACHE_BUDGET_SELECT, &committed).await;
        reopened.close().await.unwrap();
    });
}

#[test]
fn q12_cache_budget_independent_connections_and_close_orders() {
    asupersync::test_utils::run_test(|| async {
        let run = CacheBudgetRun::new("q12_cache_budget_independent_connections_and_close_orders");
        let dir = tempfile::tempdir().unwrap();
        for close_small_first in [true, false] {
            let path = dir
                .path()
                .join(format!("connections-{close_small_first}.db"));
            let initial = seed_cache_budget_database(&path, 4096);
            let mut committed = initial.clone();
            committed.last_mut().unwrap().1.push_str(":writer");
            let small = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            let large = fsqlite::Connection::open(path.to_str().unwrap())
                .await
                .unwrap();
            small.execute("PRAGMA cache_size=8").await.unwrap();
            large.execute("PRAGMA cache_size=128").await.unwrap();
            cache_budget_assert_rows(&small, CACHE_BUDGET_SELECT, &initial).await;
            cache_budget_assert_rows(&large, CACHE_BUDGET_SELECT, &initial).await;
            run.assert_quiescent_budget("small_before_overlap", &small, 8);
            run.assert_quiescent_budget("large_before_overlap", &large, 128);
            assert!(large.memory_stats().unwrap().page_cache.cached_pages > 8);

            // The transactions really overlap. No serialized-mode opt-out,
            // retry loop, or synthetic cache stands in for the public SQL path.
            small.execute("BEGIN").await.unwrap();
            cache_budget_assert_rows(&small, CACHE_BUDGET_SELECT, &initial).await;
            large.execute("BEGIN").await.unwrap();
            large
                .execute("UPDATE cache_budget SET payload=payload || ':writer' WHERE id=64")
                .await
                .unwrap();
            large.execute("COMMIT").await.unwrap();
            cache_budget_assert_rows(&small, CACHE_BUDGET_SELECT, &initial).await;
            run.record(
                "reader_snapshot_after_writer_commit",
                &small,
                json!({"close_small_first": close_small_first}),
            );
            small.execute("ROLLBACK").await.unwrap();
            cache_budget_assert_rows(&small, CACHE_BUDGET_SELECT, &committed).await;
            cache_budget_assert_rows(&large, CACHE_BUDGET_SELECT, &committed).await;
            run.assert_quiescent_budget("small_after_overlap", &small, 8);
            run.assert_quiescent_budget("large_after_overlap", &large, 128);
            assert_eq!(cache_budget_integer(&small, "PRAGMA cache_size").await, 8);
            assert_eq!(cache_budget_integer(&large, "PRAGMA cache_size").await, 128);

            if close_small_first {
                small.close().await.unwrap();
                cache_budget_assert_rows(&large, CACHE_BUDGET_SELECT, &committed).await;
                run.assert_quiescent_budget("small_closed_first", &large, 128);
                assert!(large.memory_stats().unwrap().page_cache.cached_pages > 8);
                large.close().await.unwrap();
            } else {
                large.close().await.unwrap();
                cache_budget_assert_rows(&small, CACHE_BUDGET_SELECT, &committed).await;
                run.assert_quiescent_budget("large_closed_first", &small, 8);
                small.close().await.unwrap();
            }
            assert_eq!(cache_budget_oracle_rows(&path), committed);
        }
    });
}

#[test]
fn q13_cache_budget_attach_and_temp_isolation() {
    asupersync::test_utils::run_test(|| async {
        let run = CacheBudgetRun::new("q13_cache_budget_attach_and_temp_isolation");
        let dir = tempfile::tempdir().unwrap();
        let main_path = dir.path().join("main.db");
        let aux_path = dir.path().join("aux.db");
        let main_rows = seed_cache_budget_database(&main_path, 4096);
        let aux_rows = seed_cache_budget_database(&aux_path, 8192);
        let conn = fsqlite::Connection::open(main_path.to_str().unwrap())
            .await
            .unwrap();
        conn.execute(&format!(
            "ATTACH DATABASE '{}' AS aux",
            aux_path.to_str().unwrap().replace('\'', "''")
        ))
        .await
        .unwrap();
        conn.execute("PRAGMA main.cache_size=16").await.unwrap();
        cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &main_rows).await;
        conn.execute("PRAGMA aux.cache_size=-32").await.unwrap();
        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA aux.page_size").await,
            8192
        );
        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA aux.cache_size").await,
            -32
        );
        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA main.cache_size").await,
            16
        );
        run.assert_quiescent_budget("attached_setter_main_unchanged", &conn, 16);
        cache_budget_assert_rows(
            &conn,
            "SELECT id, payload FROM aux.cache_budget ORDER BY id",
            &aux_rows,
        )
        .await;

        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA temp.cache_size").await,
            0
        );
        conn.execute("CREATE TEMP TABLE temp_rows(id INTEGER PRIMARY KEY, payload TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO temp_rows VALUES (1, 'temporary'), (2, 'still here')")
            .await
            .unwrap();
        conn.execute("PRAGMA temp.cache_size=1").await.unwrap();
        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA temp.cache_size").await,
            1
        );
        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA main.cache_size").await,
            16
        );
        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA aux.cache_size").await,
            -32
        );
        run.assert_quiescent_budget("temp_setter_main_unchanged", &conn, 16);
        conn.execute("PRAGMA main.cache_size=8").await.unwrap();
        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA temp.cache_size").await,
            1
        );
        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA aux.cache_size").await,
            -32
        );
        cache_budget_assert_rows(
            &conn,
            "SELECT id, payload FROM temp_rows ORDER BY id",
            &[(1, "temporary".to_owned()), (2, "still here".to_owned())],
        )
        .await;
        cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &main_rows).await;
        run.assert_quiescent_budget("all_schema_rows_preserved", &conn, 8);
        conn.execute("DETACH DATABASE aux").await.unwrap();
        cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &main_rows).await;
        run.assert_quiescent_budget("after_detach", &conn, 8);
        conn.close().await.unwrap();
        assert_eq!(cache_budget_oracle_rows(&main_path), main_rows);
        assert_eq!(cache_budget_oracle_rows(&aux_path), aux_rows);
    });
}

#[test]
fn q14_cache_budget_default_survives_reopen() {
    asupersync::test_utils::run_test(|| async {
        let run = CacheBudgetRun::new("q14_cache_budget_default_survives_reopen");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("default.db");
        let expected = seed_cache_budget_database(&path, 4096);
        let conn = fsqlite::Connection::open(path.to_str().unwrap())
            .await
            .unwrap();
        let numeric_oracle = rusqlite::Connection::open_in_memory().unwrap();
        for requested in [
            "-2147483648",
            "'123abc'",
            "1e3",
            "9223372036854775808",
            "abc",
        ] {
            let setter = format!("PRAGMA default_cache_size={requested}");
            numeric_oracle.execute_batch(&setter).unwrap();
            let runtime: i64 = numeric_oracle
                .query_row("PRAGMA cache_size", [], |r| r.get(0))
                .unwrap();
            let persisted: i64 = numeric_oracle
                .query_row("PRAGMA default_cache_size", [], |r| r.get(0))
                .unwrap();
            conn.execute(&setter).await.unwrap();
            assert_eq!(
                cache_budget_integer(&conn, "PRAGMA cache_size").await,
                runtime
            );
            assert_eq!(
                cache_budget_integer(&conn, "PRAGMA default_cache_size").await,
                persisted
            );
            run.record(
                "default_numeric_oracle",
                &conn,
                json!({
                    "requested": requested, "runtime": runtime, "persisted": persisted,
                }),
            );
            cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &expected).await;
            run.assert_quiescent_budget(
                "default_numeric_scan",
                &conn,
                cache_budget_pages(runtime, 4096),
            );
        }
        numeric_oracle.close().unwrap();
        conn.execute("PRAGMA cache_size=256").await.unwrap();
        cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &expected).await;
        let before = conn.memory_stats().unwrap();
        assert!(before.page_cache.cached_pages > 8);
        conn.execute("PRAGMA default_cache_size=-8").await.unwrap();
        assert_eq!(
            cache_budget_integer(&conn, "PRAGMA default_cache_size").await,
            8
        );
        assert_eq!(cache_budget_integer(&conn, "PRAGMA cache_size").await, 8);
        run.assert_quiescent_budget("default_applies_to_live_pager", &conn, 8);
        assert!(conn.memory_stats().unwrap().page_cache.evictions > before.page_cache.evictions);
        cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &expected).await;
        run.assert_quiescent_budget("default_scan", &conn, 8);
        conn.close().await.unwrap();

        let oracle = rusqlite::Connection::open(&path).unwrap();
        let stock_default: i64 = oracle
            .query_row("PRAGMA default_cache_size", [], |r| r.get(0))
            .unwrap();
        let stock_current: i64 = oracle
            .query_row("PRAGMA cache_size", [], |r| r.get(0))
            .unwrap();
        assert_eq!(stock_default, 8);
        assert_eq!(stock_current, 8);
        oracle.close().unwrap();
        let reopened = fsqlite::Connection::open(path.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(
            cache_budget_integer(&reopened, "PRAGMA cache_size").await,
            stock_current
        );
        assert_eq!(
            cache_budget_integer(&reopened, "PRAGMA default_cache_size").await,
            stock_default
        );
        run.assert_quiescent_budget("persisted_default_bootstrap", &reopened, 8);
        cache_budget_assert_rows(&reopened, CACHE_BUDGET_SELECT, &expected).await;
        run.assert_quiescent_budget("persisted_default_scan", &reopened, 8);
        reopened.close().await.unwrap();
        assert_eq!(cache_budget_oracle_rows(&path), expected);
    });
}

#[test]
fn q15_cache_budget_readonly_header_failure_keeps_runtime_change() {
    asupersync::test_utils::run_test(|| async {
        let run =
            CacheBudgetRun::new("q15_cache_budget_readonly_header_failure_keeps_runtime_change");
        let dir = tempfile::tempdir().unwrap();
        for readonly_file in [false, true] {
            let path = dir.path().join(format!("readonly-{readonly_file}.db"));
            let oracle_path = dir
                .path()
                .join(format!("oracle-readonly-{readonly_file}.db"));
            let expected = seed_cache_budget_database(&path, 4096);
            assert_eq!(seed_cache_budget_database(&oracle_path, 4096), expected);
            let conn = if readonly_file {
                fsqlite::Connection::open_schema_only(path.to_str().unwrap())
                    .await
                    .unwrap()
            } else {
                fsqlite::Connection::open(path.to_str().unwrap())
                    .await
                    .unwrap()
            };
            let oracle = if readonly_file {
                rusqlite::Connection::open_with_flags(
                    &oracle_path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .unwrap()
            } else {
                rusqlite::Connection::open(&oracle_path).unwrap()
            };
            conn.execute("PRAGMA cache_size=8").await.unwrap();
            oracle.execute_batch("PRAGMA cache_size=8").unwrap();
            if !readonly_file {
                conn.execute("PRAGMA query_only=ON").await.unwrap();
                oracle.execute_batch("PRAGMA query_only=ON").unwrap();
            }
            let original_default: i64 = oracle
                .query_row("PRAGMA default_cache_size", [], |r| r.get(0))
                .unwrap();
            let stock_error = oracle
                .execute_batch("PRAGMA default_cache_size=32")
                .unwrap_err();
            assert_eq!(
                stock_error.sqlite_error_code(),
                Some(rusqlite::ErrorCode::ReadOnly)
            );
            let actual_error = conn
                .execute("PRAGMA default_cache_size=32")
                .await
                .unwrap_err();
            run.record(
                "readonly_header_error",
                &conn,
                json!({
                    "readonly_file": readonly_file,
                    "stock_error": stock_error.to_string(),
                    "actual_error": actual_error.to_string(),
                }),
            );
            assert!(matches!(
                actual_error,
                fsqlite_error::FrankenError::ReadOnly
            ));
            let stock_runtime: i64 = oracle
                .query_row("PRAGMA cache_size", [], |r| r.get(0))
                .unwrap();
            assert_eq!(stock_runtime, 32);
            assert_eq!(
                cache_budget_integer(&conn, "PRAGMA cache_size").await,
                stock_runtime
            );
            assert_eq!(
                cache_budget_integer(&conn, "PRAGMA default_cache_size").await,
                original_default
            );
            assert_eq!(
                oracle
                    .query_row("PRAGMA default_cache_size", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                original_default
            );
            cache_budget_assert_rows(&conn, CACHE_BUDGET_SELECT, &expected).await;
            run.assert_quiescent_budget("readonly_runtime_budget_really_grew", &conn, 32);
            assert!(conn.memory_stats().unwrap().page_cache.cached_pages > 8);
            conn.close().await.unwrap();
            oracle.close().unwrap();
            assert_eq!(cache_budget_oracle_rows(&path), expected);
        }
    });
}

#[test]
fn q16_cache_budget_rollback_restores_header_not_runtime() {
    asupersync::test_utils::run_test(|| async {
        let run = CacheBudgetRun::new("q16_cache_budget_rollback_restores_header_not_runtime");
        let dir = tempfile::tempdir().unwrap();
        for schema in ["main", "aux"] {
            let main_path = dir.path().join(format!("rollback-{schema}.db"));
            let oracle_main = dir.path().join(format!("oracle-rollback-{schema}.db"));
            let expected = seed_cache_budget_database(&main_path, 4096);
            assert_eq!(seed_cache_budget_database(&oracle_main, 4096), expected);
            let conn = fsqlite::Connection::open(main_path.to_str().unwrap())
                .await
                .unwrap();
            let oracle = rusqlite::Connection::open(&oracle_main).unwrap();
            let affected_path = if schema == "aux" {
                let aux = dir.path().join("rollback-aux-child.db");
                let oracle_aux = dir.path().join("oracle-rollback-aux-child.db");
                assert_eq!(seed_cache_budget_database(&aux, 4096), expected);
                assert_eq!(seed_cache_budget_database(&oracle_aux, 4096), expected);
                conn.execute(&format!(
                    "ATTACH '{}' AS aux",
                    aux.to_str().unwrap().replace('\'', "''")
                ))
                .await
                .unwrap();
                oracle
                    .execute("ATTACH ?1 AS aux", [oracle_aux.to_str().unwrap()])
                    .unwrap();
                aux
            } else {
                main_path.clone()
            };
            let runtime_query = format!("PRAGMA {schema}.cache_size");
            let default_query = format!("PRAGMA {schema}.default_cache_size");
            let original_default: i64 = oracle.query_row(&default_query, [], |r| r.get(0)).unwrap();
            conn.execute("PRAGMA main.cache_size=8").await.unwrap();
            let setup = format!("PRAGMA {schema}.cache_size=8");
            conn.execute(&setup).await.unwrap();
            oracle.execute_batch(&setup).unwrap();
            conn.execute("BEGIN").await.unwrap();
            oracle.execute_batch("BEGIN").unwrap();
            let setter = format!("PRAGMA {schema}.default_cache_size=32");
            conn.execute(&setter).await.unwrap();
            oracle.execute_batch(&setter).unwrap();
            assert_eq!(cache_budget_integer(&conn, &default_query).await, 32);
            conn.execute("ROLLBACK").await.unwrap();
            oracle.execute_batch("ROLLBACK").unwrap();
            let stock_runtime: i64 = oracle.query_row(&runtime_query, [], |r| r.get(0)).unwrap();
            let stock_default: i64 = oracle.query_row(&default_query, [], |r| r.get(0)).unwrap();
            assert_eq!(stock_runtime, 32);
            assert_eq!(stock_default, original_default);
            assert_eq!(
                cache_budget_integer(&conn, &runtime_query).await,
                stock_runtime
            );
            assert_eq!(
                cache_budget_integer(&conn, &default_query).await,
                stock_default
            );
            cache_budget_assert_rows(
                &conn,
                &format!("SELECT id, payload FROM {schema}.cache_budget ORDER BY id"),
                &expected,
            )
            .await;
            run.record(
                "default_header_rolled_back",
                &conn,
                json!({
                    "schema": schema, "runtime_setting": stock_runtime,
                    "restored_default": stock_default,
                }),
            );
            run.assert_quiescent_budget(
                "after_default_rollback",
                &conn,
                if schema == "main" { 32 } else { 8 },
            );
            conn.close().await.unwrap();
            oracle.close().unwrap();
            let reopened = rusqlite::Connection::open(&affected_path).unwrap();
            assert_eq!(
                reopened
                    .query_row("PRAGMA default_cache_size", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                original_default
            );
            reopened.close().unwrap();
            assert_eq!(cache_budget_oracle_rows(&affected_path), expected);
        }
    });
}
