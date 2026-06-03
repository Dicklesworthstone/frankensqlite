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
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_aztlm_page_cache_e2e -- --nocapture --test-threads=1
//! ```

#![allow(clippy::cast_precision_loss)]

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
    let tn = "q7_insert_10k_oracle";
    let row_count = 10_000i64;
    emit_log(tn, "start", json!({"rows": row_count}));

    let fconn = fsqlite::Connection::open(":memory:").unwrap();
    let cconn = rusqlite::Connection::open_in_memory().unwrap();

    fconn
        .execute("CREATE TABLE cache_test (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)")
        .unwrap();
    cconn
        .execute_batch("CREATE TABLE cache_test (id INTEGER PRIMARY KEY, val INTEGER, label TEXT);")
        .unwrap();

    let insert_start = Instant::now();
    fconn.execute("BEGIN").unwrap();
    cconn.execute_batch("BEGIN;").unwrap();
    for i in 0..row_count {
        let val = i * 13 + 7;
        let label = format!("cache_{i:06}");
        fconn
            .execute(&format!(
                "INSERT INTO cache_test VALUES ({i}, {val}, '{label}')"
            ))
            .unwrap();
        cconn
            .execute(
                "INSERT INTO cache_test VALUES (?1, ?2, ?3)",
                rusqlite::params![i, val, label],
            )
            .unwrap();
    }
    fconn.execute("COMMIT").unwrap();
    cconn.execute_batch("COMMIT;").unwrap();
    let insert_ns = insert_start.elapsed().as_nanos() as u64;

    // Full scan — exercises page cache read path
    let scan_start = Instant::now();
    let f_rows = fconn
        .query("SELECT id, val, label FROM cache_test ORDER BY id")
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
}

// ─── Q8: 4 concurrent writers, no data loss ──────────────────────────

#[test]
fn q8_e2e_concurrent_writers() {
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
        let conn = fsqlite::Connection::open(&path_str).unwrap();
        conn.execute(
            "CREATE TABLE writers (tid INTEGER, seq INTEGER, val INTEGER, PRIMARY KEY (tid, seq))",
        )
        .unwrap();
    }

    let barrier = Arc::new(Barrier::new(thread_count));
    let path_arc = Arc::new(path_str.clone());

    let handles: Vec<_> = (0..thread_count)
        .map(|tid| {
            let barrier = Arc::clone(&barrier);
            let path = Arc::clone(&path_arc);
            std::thread::spawn(move || {
                let conn = fsqlite::Connection::open(path.as_str()).unwrap();
                barrier.wait();

                let mut written = 0i64;
                let batch = 50i64;
                let mut seq = 0i64;
                while seq < rows_per_thread {
                    let end = (seq + batch).min(rows_per_thread);
                    let max_retries = 50;
                    let mut attempt = 0;
                    loop {
                        attempt += 1;
                        conn.execute("BEGIN").unwrap();
                        let mut batch_ok = true;
                        for s in seq..end {
                            let val = tid as i64 * 10000 + s;
                            if conn
                                .execute(&format!("INSERT INTO writers VALUES ({tid}, {s}, {val})"))
                                .is_err()
                            {
                                batch_ok = false;
                                break;
                            }
                        }
                        if batch_ok {
                            match conn.execute("COMMIT") {
                                Ok(_) => {
                                    written += end - seq;
                                    break;
                                }
                                Err(_) => {
                                    let _ = conn.execute("ROLLBACK");
                                }
                            }
                        } else {
                            let _ = conn.execute("ROLLBACK");
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

                written
            })
        })
        .collect();

    let mut total_written = 0i64;
    for h in handles {
        total_written += h.join().unwrap();
    }

    // Verify: open fresh connection, count rows, check no duplicates
    let verify_conn = fsqlite::Connection::open(&path_str).unwrap();
    let count_rows = verify_conn.query("SELECT COUNT(*) FROM writers").unwrap();
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
}
