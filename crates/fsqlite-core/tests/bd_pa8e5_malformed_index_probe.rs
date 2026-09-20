//! bd-pa8e5 — when a retried CREATE INDEX leaves a malformed index, WHICH side
//! diverged: the durable database, or this connection's local schema?
//!
//! Arming DDL for the autocommit transient retry made `CREATE INDEX` produce
//! "database disk image is malformed: index probe_ix payload is not a valid
//! SQLite record" in 1 of 3 runs. Two explanations fit that symptom and they
//! need opposite fixes:
//!
//!   * the sqlite_master row COMMITTED but its root page was never populated —
//!     a durable defect, visible to any reader; or
//!   * the row never committed and only OUR connection's local schema believes
//!     the index exists — connection-local staleness, which is what bd-xvv8f's
//!     `force_full_schema_reload_once` is supposed to repair on the autocommit
//!     DDL rollback path.
//!
//! So at the first malformed report, ask both sides the same question. A fresh
//! stock `rusqlite` connection reads only committed durable state; our
//! connection answers from its own schema. The pair of answers settles it.
//!
//! This reproduces only with DDL armed for retry, which is NOT the shipped
//! behaviour — bd-orwh0 deliberately left DDL out of the allowlist for this
//! reason. On shipped main this probe is expected to report no malformed index
//! at all; that is a correct result, not a failure to reproduce.
//!
//!   cargo test -p fsqlite-core --test bd_pa8e5_malformed_index_probe -- --ignored --nocapture

use fsqlite_core::connection::Connection;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const BUSY_TIMEOUT_MS: u64 = 4_000;
const RUN_SECS: u64 = 6;

/// What a fresh stock connection — i.e. committed durable state alone — can see.
fn stock_view(db: &Path) -> String {
    let Ok(stock) = rusqlite::Connection::open(db) else {
        return "stock could not open the database".to_owned();
    };
    let master = stock
        .query_row(
            "SELECT type || ' ' || name || ' root=' || rootpage FROM sqlite_master WHERE name='probe_ix'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap_or_else(|error| format!("<no sqlite_master row: {error}>"));
    // Read EVERY integrity_check row, not just the first. stock reports one row
    // per problem and "ok" alone when there are none, so taking row 0 turns a
    // detailed corruption report into the single useless line
    // "*** in database main ***". Knowing WHICH object and page stock objects to
    // is the whole value of asking it.
    let integrity = match stock.prepare("PRAGMA integrity_check") {
        Ok(mut stmt) => match stmt.query_map([], |row| row.get::<_, String>(0)) {
            Ok(rows) => {
                let all: Vec<String> = rows.filter_map(Result::ok).collect();
                if all.len() == 1 && all[0] == "ok" {
                    "ok".to_owned()
                } else {
                    let shown: Vec<&str> =
                        all.iter().take(8).map(String::as_str).collect();
                    format!("{} rows: {}", all.len(), shown.join(" / "))
                }
            }
            Err(error) => format!("<query failed: {error}>"),
        },
        Err(error) => format!("<prepare failed: {error}>"),
    };
    format!("sqlite_master: {master} | integrity_check: {integrity}")
}

#[test]
#[ignore = "bd-pa8e5 diagnosis probe; reproduces only with DDL armed for retry"]
fn which_side_diverged_when_the_index_goes_malformed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("pa8e5.db");
    let path = db_path.to_str().expect("utf-8 path").to_owned();

    {
        let p = path.clone();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&p).await.expect("open seed");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .await
                .expect("ddl");
            let payload = "d".repeat(1_500);
            for i in 0..1_200 {
                conn.execute(&format!("INSERT INTO t(id,v) VALUES({i},'{payload}')"))
                    .await
                    .expect("seed insert");
            }
            conn.close().await.expect("close seed");
        });
    }

    let stop = Arc::new(AtomicBool::new(false));
    let checkpointer = {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open checkpointer");
                conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                    .await
                    .expect("busy_timeout");
                while !stop.load(Ordering::Relaxed) {
                    let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;
                    let _ = conn.execute("INSERT INTO t(v) VALUES('c')").await;
                }
                conn.close().await.expect("close checkpointer");
            });
        })
    };

    {
        let p = path.clone();
        let db = db_path.clone();
        let stop = Arc::clone(&stop);
        asupersync::test_utils::run_test(|| async move {
            let conn = Connection::open(&p).await.expect("open prober");
            conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                .await
                .expect("busy_timeout");

            // Two PHASES, matching the keeper that originally produced the
            // malformed index. Alternating the two statements instead lets the
            // checkpointer starve both, and neither ever completes -- an earlier
            // revision of this probe did exactly that and reported nothing.
            let (mut created, mut analyzed, mut malformed) = (0u64, 0u64, 0u64);

            // Phase 1: build the index under contention, repeatedly.
            let deadline = Instant::now() + Duration::from_secs(RUN_SECS);
            while Instant::now() < deadline {
                if conn
                    .execute("CREATE INDEX IF NOT EXISTS probe_ix ON t(v)")
                    .await
                    .is_ok()
                {
                    created += 1;
                }
            }
            println!("phase 1 done: CREATE INDEX ok={created}");

            // Phase 2: read it back. ANALYZE is what surfaced the bad payload.
            let deadline = Instant::now() + Duration::from_secs(RUN_SECS);
            while Instant::now() < deadline {
                match conn.execute("ANALYZE").await {
                    Ok(_) => analyzed += 1,
                    Err(error) => {
                        let text = format!("{error}");
                        if text.contains("malformed") {
                            malformed += 1;
                            if malformed == 1 {
                                println!("\n>>> FIRST MALFORMED REPORT: {text}");
                                println!(">>> OUR connection's view:");
                                match conn
                                    .query("SELECT type, name, rootpage FROM sqlite_master WHERE name='probe_ix'")
                                    .await
                                {
                                    Ok(rows) => println!("      rows={}  {rows:?}", rows.len()),
                                    Err(error) => println!("      query failed: {error}"),
                                }
                                println!(">>> A FRESH STOCK connection's view (committed state only):");
                                println!("      {}", stock_view(&db));
                            }
                        }
                    }
                }
            }
            stop.store(true, Ordering::Relaxed);
            println!(
                "\nCREATE INDEX ok={created}  ANALYZE ok={analyzed}  malformed={malformed}"
            );
            if malformed == 0 {
                println!(
                    "No malformed index. On shipped main this is the EXPECTED result: bd-orwh0 \
                     leaves DDL out of the retry allowlist precisely to avoid this path."
                );
            }
            println!(">>> stock view at end: {}", stock_view(&db));
            conn.close().await.expect("close prober");
        });
    }

    checkpointer.join().expect("checkpointer thread");
}
