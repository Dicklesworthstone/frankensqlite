//! bd-pa8e5 — is the "corruption" real, or an artifact of reading the database
//! with stock SQLite while our own connections are still open?
//!
//! Every probe so far called stock's integrity_check while a FrankenSQLite
//! connection was still live on the same file, IN THE SAME PROCESS. bd-1nq3j
//! established that this exact configuration is unsupported: POSIX fcntl locks
//! are per process, so stock cannot see our shared WAL-index dead-man-switch
//! hold, concludes it is the last connection, and checkpoints and unlinks the
//! companions. A stock connection doing that mid-flight could perfectly well be
//! CAUSING the inconsistency it then reports.
//!
//! So this runs the identical workload and asks the question the right way
//! round: close every FrankenSQLite connection first, await the closes, and only
//! then let stock open the file at all.
//!
//! Both verdicts are printed:
//!   DIRTY  stock reads while our connections are still open (what earlier
//!          probes did, kept here only for comparison)
//!   CLEAN  stock reads after every connection has been closed
//!
//! If DIRTY reports corruption and CLEAN reports ok, the finding is an artifact
//! of the unsupported in-process configuration, not a durability defect — and
//! bd-pa8e5's P0 framing is wrong. Prints; asserts nothing.
//!
//!   cargo test -p fsqlite-core --test bd_pa8e5_clean_close_probe -- --ignored --nocapture

use fsqlite_core::connection::Connection;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const BUSY_TIMEOUT_MS: u64 = 4_000;
const PHASE_SECS: u64 = 6;

fn stock_verdict(db: &Path) -> String {
    let Ok(stock) = rusqlite::Connection::open(db) else {
        return "<stock could not open>".to_owned();
    };
    match stock.prepare("PRAGMA integrity_check") {
        Ok(mut stmt) => match stmt.query_map([], |row| row.get::<_, String>(0)) {
            Ok(rows) => {
                let all: Vec<String> = rows.filter_map(Result::ok).collect();
                if all.len() == 1 && all[0] == "ok" {
                    "ok".to_owned()
                } else {
                    format!("{} rows: {}", all.len(), all.join(" / "))
                }
            }
            Err(error) => format!("<query failed: {error}>"),
        },
        Err(error) => format!("<prepare failed: {error}>"),
    }
}

#[test]
#[ignore = "bd-pa8e5 confound check; compares dirty-read and clean-close verdicts"]
fn does_the_corruption_survive_a_clean_close() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("cleanclose.db");
    let path = db.to_str().expect("utf-8 path").to_owned();

    {
        let p = path.clone();
        asupersync::test_utils::run_test(|| async move {
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
    let antagonist = {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open antagonist");
                conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                    .await
                    .expect("busy_timeout");
                while !stop.load(Ordering::Relaxed) {
                    let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;
                    let _ = conn.execute("INSERT INTO t(v) VALUES('c')").await;
                }
                // Close properly, so the CLEAN verdict below is not measuring a
                // connection that was simply abandoned.
                conn.close().await.expect("close antagonist");
            });
        })
    };

    // The prober: build the index under load, then read it back, exactly as the
    // workload that first reported corruption did.
    let dirty_verdict = {
        let p = path.clone();
        let db = db.clone();
        let stop = Arc::clone(&stop);
        let verdict = Arc::new(std::sync::Mutex::new(String::new()));
        let out = Arc::clone(&verdict);
        asupersync::test_utils::run_test(|| async move {
            let conn = Connection::open(&p).await.expect("open prober");
            conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                .await
                .expect("busy_timeout");

            let deadline = Instant::now() + Duration::from_secs(PHASE_SECS);
            let mut created = 0u64;
            while Instant::now() < deadline {
                if conn
                    .execute("CREATE INDEX IF NOT EXISTS probe_ix ON t(v)")
                    .await
                    .is_ok()
                {
                    created += 1;
                }
            }
            let deadline = Instant::now() + Duration::from_secs(PHASE_SECS);
            let (mut analyzed, mut malformed) = (0u64, 0u64);
            while Instant::now() < deadline {
                match conn.execute("ANALYZE").await {
                    Ok(_) => analyzed += 1,
                    Err(error) if format!("{error}").contains("malformed") => malformed += 1,
                    Err(_) => {}
                }
            }
            println!("  prober: CREATE INDEX ok={created} ANALYZE ok={analyzed} malformed={malformed}");

            // DIRTY read: stock opens while we are still open. This is what every
            // earlier probe measured.
            *out.lock().expect("verdict") = stock_verdict(&db);

            stop.store(true, Ordering::Relaxed);
            conn.close().await.expect("close prober");
        });
        let v = verdict.lock().expect("verdict").clone();
        v
    };

    stop.store(true, Ordering::Relaxed);
    antagonist.join().expect("antagonist thread");

    // CLEAN read: every FrankenSQLite connection is now closed and awaited.
    let clean_verdict = stock_verdict(&db);

    println!("  DIRTY (stock read while our connections were open): {dirty_verdict}");
    println!("  CLEAN (stock read after every connection closed):   {clean_verdict}");
    println!(
        "  If DIRTY is corrupt and CLEAN is ok, the finding is an artifact of the \
         unsupported in-process stock configuration (bd-1nq3j), not a durability defect."
    );
}
