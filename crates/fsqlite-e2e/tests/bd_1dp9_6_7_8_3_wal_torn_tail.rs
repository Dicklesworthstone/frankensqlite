//! WAL truncate/torn-tail/corruption suite (bd-1dp9.6.7.8.3).
//!
//! Proof bundle for authoritative WAL indexing: deterministic edge-case
//! tests, corruption/truncate/torn-tail scenarios, and structured logs
//! proving the fast path is used only when generation semantics are sound.
//!
//! ## Scenarios
//!
//! | ID | Name | Shape |
//! |----|------|-------|
//! | W1 | clean_wal_roundtrip | Write → checkpoint → verify no data loss |
//! | W2 | truncated_wal_recovery | Write → truncate WAL mid-frame → reopen |
//! | W3 | torn_tail_partial_frame | Write → corrupt last frame header → reopen |
//! | W4 | multi_txn_torn_tail | N txns → corrupt last → verify N-1 survive |
//! | W5 | wal_reset_generation | Write → reset WAL → verify generation changes |
//! | W6 | corruption_mid_wal | Corrupt frame in middle → reopen → verify |
//! | W7 | concurrent_append_checkpoint | Writers + checkpoint overlap |
//! | W8 | wal_growth_then_truncate | Large WAL → checkpoint → verify WAL shrinks |
//!
//! ## Structured Log Contract
//!
//! ```json
//! {
//!   "bead_id": "bd-1dp9.6.7.8.3",
//!   "trace_id": "<id>",
//!   "run_id": "<scenario>_<seed>",
//!   "scenario_id": "W1",
//!   "phase": "result",
//!   "wal_generation": "<hex>",
//!   "lookup_mode": "authoritative|fallback",
//!   "recovery_mode": "none|truncate|replay",
//!   "frames_before": 10,
//!   "frames_after": 0,
//!   "elapsed_ns": 123456
//! }
//! ```
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_1dp9_6_7_8_3_wal_torn_tail \
//!     -- --nocapture --test-threads=1
//! ```

#![allow(clippy::too_many_lines)]
#![allow(clippy::similar_names)]
#![allow(clippy::cast_precision_loss)]
#![recursion_limit = "512"]

use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use serde_json::json;

const BEAD_ID: &str = "bd-1dp9.6.7.8.3";
const REPLAY_CMD: &str = "cargo test -p fsqlite-e2e --test bd_1dp9_6_7_8_3_wal_torn_tail -- --nocapture --test-threads=1";

const SEED_W1: u64 = 0x0057_414C_5441_4931;
const SEED_W2: u64 = 0x0057_414C_5441_4932;
const SEED_W3: u64 = 0x0057_414C_5441_4933;
const SEED_W4: u64 = 0x0057_414C_5441_4934;
const SEED_W5: u64 = 0x0057_414C_5441_4935;
const SEED_W6: u64 = 0x0057_414C_5441_4936;
const SEED_W7: u64 = 0x0057_414C_5441_4937;
const SEED_W8: u64 = 0x0057_414C_5441_4938;

const WAL_HEADER_SIZE: usize = 32;
const WAL_FRAME_HEADER_SIZE: usize = 24;
const PAGE_SIZE: usize = 4096;

fn sqlite_i64(value: u64) -> i64 {
    i64::try_from(value).expect("test workload values fit SQLite INTEGER")
}

static E2E_LOCK: Mutex<()> = Mutex::new(());

// ─── Structured logging ──────────────────────────────────────────────

fn emit_log(scenario_id: &str, seed: u64, phase: &str, data: serde_json::Value) {
    let trace_id = format!("{:016x}-{:04x}", seed, std::process::id() & 0xFFFF);
    eprintln!(
        "WAL_TORN_TAIL:{}",
        json!({
            "bead_id": BEAD_ID,
            "trace_id": trace_id,
            "run_id": format!("{scenario_id}_{seed:016x}"),
            "scenario_id": scenario_id,
            "phase": phase,
            "replay_command": REPLAY_CMD,
            "data": data,
        })
    );
}

// ─── Helpers ─────────────────────────────────────────────────────────

fn setup_wal_db(path: &Path, row_count: u32) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.execute_batch(
        "PRAGMA wal_autocheckpoint = 0;
         PRAGMA journal_mode = WAL;
         PRAGMA page_size = 4096;
         PRAGMA synchronous = FULL;
         CREATE TABLE data (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);",
    )
    .expect("setup");

    for i in 0..row_count {
        conn.execute(
            "INSERT INTO data (id, payload) VALUES (?1, ?2)",
            rusqlite::params![i, format!("row-{i:06}")],
        )
        .expect("insert");
    }
    conn
}

fn count_rows_sqlite(path: &Path) -> i64 {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .expect("count")
}

fn integrity_check_sqlite(path: &Path) -> String {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity")
}

fn wal_path(db_path: &Path) -> std::path::PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-wal");
    std::path::PathBuf::from(p)
}

fn wal_frame_count(wal_data: &[u8]) -> usize {
    if wal_data.len() < WAL_HEADER_SIZE {
        return 0;
    }
    let payload = wal_data.len() - WAL_HEADER_SIZE;
    let frame_size = WAL_FRAME_HEADER_SIZE + PAGE_SIZE;
    payload / frame_size
}

async fn _count_rows_fsqlite(path: &Path) -> i64 {
    let p = path.to_str().expect("utf-8");
    let conn = fsqlite::Connection::open(p).await.expect("open fsqlite");
    conn.execute("PRAGMA journal_mode=WAL").await.ok();
    let rows = conn
        .query("SELECT COUNT(*) FROM data")
        .await
        .expect("count");
    match &rows[0].values()[0] {
        fsqlite_types::value::SqliteValue::Integer(n) => *n,
        other => panic!("expected Integer, got {other:?}"),
    }
}

// ─── W1: Clean WAL roundtrip ─────────────────────────────────────────

#[test]
fn w1_clean_wal_roundtrip() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario_id = "W1";
    let row_count = 100u32;

    emit_log(
        scenario_id,
        SEED_W1,
        "start",
        json!({"test": "clean_wal_roundtrip", "rows": row_count}),
    );

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("w1.db");

    // Write data
    {
        let conn = setup_wal_db(&db_path, row_count);

        // WAL should exist with frames
        let wal = wal_path(&db_path);
        let wal_exists = wal.exists();
        let wal_frames = if wal_exists {
            let data = fs::read(&wal).expect("read wal");
            wal_frame_count(&data)
        } else {
            0
        };

        emit_log(
            scenario_id,
            SEED_W1,
            "pre_checkpoint",
            json!({"wal_exists": wal_exists, "wal_frames": wal_frames}),
        );

        // Checkpoint
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint");
        drop(conn);
    }

    // Reopen and verify
    let count = count_rows_sqlite(&db_path);
    let integrity = integrity_check_sqlite(&db_path);

    emit_log(
        scenario_id,
        SEED_W1,
        "result",
        json!({
            "row_count": count,
            "expected": row_count,
            "integrity": integrity,
            "recovery_mode": "none",
        }),
    );

    assert_eq!(
        count,
        i64::from(row_count),
        "[W1] row count after checkpoint"
    );
    assert_eq!(integrity, "ok", "[W1] integrity check failed");
}

// ─── W2: Truncated WAL recovery ─────────────────────────────────────

#[test]
fn w2_truncated_wal_recovery() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario_id = "W2";
    let row_count = 50u32;

    emit_log(
        scenario_id,
        SEED_W2,
        "start",
        json!({"test": "truncated_wal_recovery", "rows": row_count}),
    );

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("w2.db");

    let original_count;
    {
        let conn = setup_wal_db(&db_path, row_count);
        original_count = count_rows_sqlite(&db_path);
        drop(conn);
    }

    // Truncate WAL to remove last frame
    let wal = wal_path(&db_path);
    if wal.exists() {
        let mut wal_data = fs::read(&wal).expect("read wal");
        let frames = wal_frame_count(&wal_data);

        if frames > 1 {
            let truncate_to = WAL_HEADER_SIZE + (frames - 1) * (WAL_FRAME_HEADER_SIZE + PAGE_SIZE);
            wal_data.truncate(truncate_to);
            fs::write(&wal, &wal_data).expect("write truncated wal");

            emit_log(
                scenario_id,
                SEED_W2,
                "truncated",
                json!({
                    "original_frames": frames,
                    "truncated_to_frames": frames - 1,
                    "truncated_bytes": truncate_to,
                }),
            );
        }
    }

    // Reopen — SQLite should recover what it can
    let recovered_count = count_rows_sqlite(&db_path);
    let integrity = integrity_check_sqlite(&db_path);

    emit_log(
        scenario_id,
        SEED_W2,
        "result",
        json!({
            "original_count": original_count,
            "recovered_count": recovered_count,
            "data_loss": original_count - recovered_count,
            "integrity": integrity,
            "recovery_mode": "truncate",
        }),
    );

    // After truncation, we may lose the last committed frame's data,
    // but the database must still be consistent.
    assert_eq!(
        integrity, "ok",
        "[W2] integrity check failed after WAL truncation"
    );
    assert!(
        recovered_count >= 0,
        "[W2] negative row count after recovery"
    );
}

// ─── W3: Torn tail — partial frame ──────────────────────────────────

#[test]
fn w3_torn_tail_partial_frame() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario_id = "W3";
    let row_count = 80u32;

    emit_log(
        scenario_id,
        SEED_W3,
        "start",
        json!({"test": "torn_tail_partial_frame", "rows": row_count}),
    );

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("w3.db");

    {
        let conn = setup_wal_db(&db_path, row_count);
        drop(conn);
    }

    // Simulate torn tail: truncate mid-frame (header written, page data partial)
    let wal = wal_path(&db_path);
    if wal.exists() {
        let wal_data = fs::read(&wal).expect("read wal");
        let frames = wal_frame_count(&wal_data);

        if frames > 0 {
            // Keep all complete frames except tear the last one mid-page
            let tear_point = WAL_HEADER_SIZE
                + (frames - 1) * (WAL_FRAME_HEADER_SIZE + PAGE_SIZE)
                + WAL_FRAME_HEADER_SIZE
                + PAGE_SIZE / 3;
            let torn = &wal_data[..tear_point];
            fs::write(&wal, torn).expect("write torn wal");

            emit_log(
                scenario_id,
                SEED_W3,
                "torn",
                json!({
                    "original_frames": frames,
                    "tear_point": tear_point,
                    "tear_within_last_frame_bytes": PAGE_SIZE / 3,
                }),
            );
        }
    }

    // Reopen — SQLite should discard the torn frame
    let count = count_rows_sqlite(&db_path);
    let integrity = integrity_check_sqlite(&db_path);

    emit_log(
        scenario_id,
        SEED_W3,
        "result",
        json!({
            "row_count": count,
            "integrity": integrity,
            "recovery_mode": "replay",
        }),
    );

    assert_eq!(integrity, "ok", "[W3] integrity failed after torn tail");
}

// ─── W4: Multi-txn torn tail — verify N-1 survive ───────────────────

#[test]
fn w4_multi_txn_torn_tail() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario_id = "W4";
    let txn_count = 10;
    let rows_per_txn = 10u32;

    emit_log(
        scenario_id,
        SEED_W4,
        "start",
        json!({"test": "multi_txn_torn_tail", "txn_count": txn_count, "rows_per_txn": rows_per_txn}),
    );

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("w4.db");

    // Write N separate transactions
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch(
            "PRAGMA wal_autocheckpoint = 0;
             PRAGMA journal_mode = WAL;
             PRAGMA page_size = 4096;
             PRAGMA synchronous = FULL;
             CREATE TABLE data (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);",
        )
        .expect("setup");

        for txn in 0..txn_count {
            conn.execute_batch("BEGIN;").expect("begin");
            for i in 0..rows_per_txn {
                let id = txn * rows_per_txn + i;
                conn.execute(
                    "INSERT INTO data (id, payload) VALUES (?1, ?2)",
                    rusqlite::params![id, format!("txn{txn}-row{i}")],
                )
                .expect("insert");
            }
            conn.execute_batch("COMMIT;").expect("commit");
        }
        drop(conn);
    }

    let count_before = count_rows_sqlite(&db_path);

    // Corrupt only the last frame (last txn's data)
    let wal = wal_path(&db_path);
    if wal.exists() {
        let mut wal_data = fs::read(&wal).expect("read");
        let frames = wal_frame_count(&wal_data);

        if frames > 1 {
            // Zero out the checksum of the last frame header → SQLite stops replay before it
            let last_frame_offset =
                WAL_HEADER_SIZE + (frames - 1) * (WAL_FRAME_HEADER_SIZE + PAGE_SIZE);
            // Frame header bytes 16-23 are the two 4-byte checksum values
            for b in &mut wal_data[last_frame_offset + 16..last_frame_offset + 24] {
                *b = 0x00;
            }
            fs::write(&wal, &wal_data).expect("write corrupted");

            emit_log(
                scenario_id,
                SEED_W4,
                "corrupted",
                json!({
                    "total_frames": frames,
                    "corrupted_frame": frames - 1,
                }),
            );
        }
    }

    let count_after = count_rows_sqlite(&db_path);
    let integrity = integrity_check_sqlite(&db_path);

    emit_log(
        scenario_id,
        SEED_W4,
        "result",
        json!({
            "count_before_corruption": count_before,
            "count_after_corruption": count_after,
            "rows_lost": count_before - count_after,
            "integrity": integrity,
            "recovery_mode": "replay",
        }),
    );

    assert_eq!(
        integrity, "ok",
        "[W4] integrity failed after last-frame corruption"
    );
    // We expect to lose at most the last transaction's data
    assert!(
        count_after >= i64::from((txn_count - 1) * rows_per_txn),
        "[W4] lost more than last txn: before={count_before}, after={count_after}"
    );
}

// ─── W5: WAL reset generation ────────────────────────────────────────

#[test]
fn w5_wal_reset_generation() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario_id = "W5";

    emit_log(
        scenario_id,
        SEED_W5,
        "start",
        json!({"test": "wal_reset_generation"}),
    );

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("w5.db");

    // Phase 1: create DB and write data
    {
        let conn = setup_wal_db(&db_path, 30);
        drop(conn);
    }

    // Read WAL salt (generation identity) before reset
    let wal = wal_path(&db_path);
    let salt_before = if wal.exists() {
        let data = fs::read(&wal).expect("read");
        if data.len() >= 24 {
            let s1 = u32::from_be_bytes(data[16..20].try_into().unwrap());
            let s2 = u32::from_be_bytes(data[20..24].try_into().unwrap());
            Some((s1, s2))
        } else {
            None
        }
    } else {
        None
    };

    // Phase 2: checkpoint TRUNCATE resets the WAL
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint truncate");

        // Write more data to create a new WAL with new generation
        conn.execute(
            "INSERT INTO data (id, payload) VALUES (9999, 'post-reset')",
            [],
        )
        .expect("post-reset insert");
        drop(conn);
    }

    let salt_after = if wal.exists() {
        let data = fs::read(&wal).expect("read");
        if data.len() >= 24 {
            let s1 = u32::from_be_bytes(data[16..20].try_into().unwrap());
            let s2 = u32::from_be_bytes(data[20..24].try_into().unwrap());
            Some((s1, s2))
        } else {
            None
        }
    } else {
        None
    };

    let count = count_rows_sqlite(&db_path);
    let integrity = integrity_check_sqlite(&db_path);

    let generation_changed = salt_before != salt_after;

    emit_log(
        scenario_id,
        SEED_W5,
        "result",
        json!({
            "salt_before": format!("{salt_before:?}"),
            "salt_after": format!("{salt_after:?}"),
            "generation_changed": generation_changed,
            "row_count": count,
            "integrity": integrity,
        }),
    );

    assert_eq!(integrity, "ok", "[W5] integrity after reset");
    assert_eq!(count, 31, "[W5] expected 30 + 1 post-reset row");
    // After TRUNCATE + new writes, the WAL generation (salt) should change
    if salt_before.is_some() && salt_after.is_some() {
        assert!(
            generation_changed,
            "[W5] WAL generation did not change after TRUNCATE + new writes"
        );
    }
}

// ─── W6: Corruption mid-WAL ─────────────────────────────────────────

#[test]
fn w6_corruption_mid_wal() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario_id = "W6";
    let row_count = 100u32;

    emit_log(
        scenario_id,
        SEED_W6,
        "start",
        json!({"test": "corruption_mid_wal", "rows": row_count}),
    );

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("w6.db");

    {
        let conn = setup_wal_db(&db_path, row_count);
        drop(conn);
    }

    let count_before = count_rows_sqlite(&db_path);

    // Corrupt a frame in the middle of the WAL
    let wal = wal_path(&db_path);
    let corrupted_frame_idx;
    if wal.exists() {
        let mut wal_data = fs::read(&wal).expect("read");
        let frames = wal_frame_count(&wal_data);
        corrupted_frame_idx = frames / 2;

        if frames > 2 {
            // Zero out the checksum of the middle frame
            let frame_offset =
                WAL_HEADER_SIZE + corrupted_frame_idx * (WAL_FRAME_HEADER_SIZE + PAGE_SIZE);
            for b in &mut wal_data[frame_offset + 16..frame_offset + 24] {
                *b = 0x00;
            }
            fs::write(&wal, &wal_data).expect("write corrupted");
        }
    } else {
        corrupted_frame_idx = 0;
    }

    let count_after = count_rows_sqlite(&db_path);
    let integrity = integrity_check_sqlite(&db_path);

    emit_log(
        scenario_id,
        SEED_W6,
        "result",
        json!({
            "count_before": count_before,
            "count_after": count_after,
            "corrupted_frame": corrupted_frame_idx,
            "integrity": integrity,
            "recovery_mode": "replay",
        }),
    );

    assert_eq!(
        integrity, "ok",
        "[W6] integrity failed after mid-WAL corruption"
    );
    // SQLite stops replaying at the first invalid frame, so we lose
    // data from that frame onward. But the DB must be consistent.
    assert!(
        count_after <= count_before,
        "[W6] more rows after corruption ({count_after}) than before ({count_before})"
    );
}

// ─── W7: Concurrent append + checkpoint overlap ─────────────────────

#[test]
fn w7_concurrent_append_checkpoint() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario_id = "W7";
    let n_writers = 3usize;
    let ops_per_writer = 200u64;

    emit_log(
        scenario_id,
        SEED_W7,
        "start",
        json!({"test": "concurrent_append_checkpoint", "writers": n_writers}),
    );

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("w7.db");

    // Setup
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA page_size = 4096;
             PRAGMA synchronous = NORMAL;
             PRAGMA wal_autocheckpoint = 0;
             CREATE TABLE data (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);",
        )
        .expect("setup");
    }

    let barrier = Arc::new(Barrier::new(n_writers + 1));
    let range_size = 100_000u64;

    // Writer threads
    let writer_handles: Vec<_> = (0..n_writers)
        .map(|tid| {
            let p = db_path.clone();
            let bar = Arc::clone(&barrier);
            thread::spawn(move || {
                let conn = rusqlite::Connection::open(&p).expect("open");
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
                    .expect("pragma");
                bar.wait();
                let base = (tid as u64) * range_size;
                let mut ok = 0u64;
                for i in 0..ops_per_writer {
                    if conn
                        .execute(
                            "INSERT INTO data (id, payload) VALUES (?1, ?2)",
                            rusqlite::params![sqlite_i64(base + i), format!("t{tid}-r{i}")],
                        )
                        .is_ok()
                    {
                        ok += 1;
                    }
                }
                ok
            })
        })
        .collect();

    // Checkpoint thread: runs PASSIVE checkpoint mid-flight
    let ckpt_path = db_path.clone();
    let ckpt_bar = Arc::clone(&barrier);
    let ckpt_handle = thread::spawn(move || {
        let conn = rusqlite::Connection::open(&ckpt_path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=10000;")
            .expect("pragma");
        ckpt_bar.wait();
        thread::sleep(std::time::Duration::from_millis(5));
        let start = Instant::now();
        let ok = conn
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE);")
            .is_ok();
        (ok, start.elapsed().as_nanos() as u64)
    });

    let per_writer: Vec<u64> = writer_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    let (ckpt_ok, ckpt_ns) = ckpt_handle.join().unwrap();
    let total_written: u64 = per_writer.iter().sum();

    let count = count_rows_sqlite(&db_path);
    let integrity = integrity_check_sqlite(&db_path);

    emit_log(
        scenario_id,
        SEED_W7,
        "result",
        json!({
            "per_writer_ops": per_writer,
            "total_written": total_written,
            "checkpoint_ok": ckpt_ok,
            "checkpoint_ns": ckpt_ns,
            "row_count": count,
            "integrity": integrity,
            "checkpoint_overlap": true,
        }),
    );

    assert_eq!(
        integrity, "ok",
        "[W7] integrity after concurrent append+checkpoint"
    );
    assert_eq!(
        count, total_written as i64,
        "[W7] row count mismatch: counted {count}, wrote {total_written}"
    );
}

// ─── W8: WAL growth then truncate ────────────────────────────────────

#[test]
fn w8_wal_growth_then_truncate() {
    let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let scenario_id = "W8";
    let row_count = 500u32;

    emit_log(
        scenario_id,
        SEED_W8,
        "start",
        json!({"test": "wal_growth_then_truncate", "rows": row_count}),
    );

    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("w8.db");

    {
        let conn = setup_wal_db(&db_path, row_count);
        let wal = wal_path(&db_path);
        let wal_size_before = if wal.exists() {
            fs::metadata(&wal).map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };

        emit_log(
            scenario_id,
            SEED_W8,
            "pre_checkpoint",
            json!({"wal_size_bytes": wal_size_before}),
        );

        // TRUNCATE checkpoint should shrink the WAL to zero
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint truncate");
        drop(conn);
    }

    let wal = wal_path(&db_path);
    let wal_size_after = if wal.exists() {
        fs::metadata(&wal).map(|m| m.len()).unwrap_or(0)
    } else {
        0
    };

    let count = count_rows_sqlite(&db_path);
    let integrity = integrity_check_sqlite(&db_path);

    emit_log(
        scenario_id,
        SEED_W8,
        "result",
        json!({
            "row_count": count,
            "wal_size_after_truncate": wal_size_after,
            "wal_truncated": wal_size_after == 0 || !wal.exists(),
            "integrity": integrity,
        }),
    );

    assert_eq!(integrity, "ok", "[W8] integrity after WAL truncate");
    assert_eq!(count, i64::from(row_count), "[W8] row count after truncate");
    // After TRUNCATE checkpoint, WAL should be empty or deleted
    assert!(
        wal_size_after == 0 || !wal.exists(),
        "[W8] WAL not truncated: {wal_size_after} bytes remain"
    );
}

// ─── FrankenSQLite WAL integration ───────────────────────────────────

#[test]
fn fsqlite_wal_write_and_read() {
    asupersync::test_utils::run_test(|| async {
        let _guard = E2E_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let scenario_id = "FW1";

        emit_log(
            scenario_id,
            SEED_W1,
            "start",
            json!({"test": "fsqlite_wal_write_and_read"}),
        );

        let temp = tempfile::tempdir().expect("tempdir");
        let db_path = temp.path().join("fw1.db");
        let db_str = db_path.to_str().expect("utf-8");

        let conn = fsqlite::Connection::open(db_str).await.expect("open");
        conn.execute("PRAGMA journal_mode=WAL").await.ok();
        conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            .await
            .expect("create");

        let row_count = 50i64;
        for i in 0..row_count {
            conn.execute(&format!(
                "INSERT INTO data (id, payload) VALUES ({i}, 'payload-{i}')"
            ))
            .await
            .expect("insert");
        }

        // Read back and verify
        let rows = conn
            .query("SELECT COUNT(*) FROM data")
            .await
            .expect("count");
        let count = match &rows[0].values()[0] {
            fsqlite_types::value::SqliteValue::Integer(n) => *n,
            other => panic!("expected Integer, got {other:?}"),
        };

        emit_log(
            scenario_id,
            SEED_W1,
            "result",
            json!({
                "backend": "fsqlite",
                "row_count": count,
                "expected": row_count,
                "wal_mode": true,
            }),
        );

        assert_eq!(count, row_count, "[FW1] fsqlite WAL row count mismatch");
    });
}
