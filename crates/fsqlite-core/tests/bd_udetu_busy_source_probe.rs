//! bd-udetu — WHICH layer raises the instant `database is busy`?
//!
//! The checkpoint probe established the trigger: while a `wal_checkpoint(TRUNCATE)`
//! holds exclusive access, `BEGIN CONCURRENT` is refused in 0 ms even though
//! `busy_timeout=10000` is set and demonstrably works on other paths. It did not
//! establish where that refusal is raised, and `FrankenError::Busy` has 400+
//! construction sites across the workspace, so reading is not a viable way to
//! find it.
//!
//! This narrows it at runtime: same contention, but stop at the FIRST refusal and
//! leave the engine's own tracing switched on around it. Run with a filter, e.g.
//!
//!   RUST_LOG=fsqlite_core=trace,fsqlite_pager=trace,fsqlite_wal=trace,fsqlite_vfs=trace \
//!   cargo test -p fsqlite-core --test bd_udetu_busy_source_probe -- --ignored --nocapture
//!
//! The lines immediately preceding "FIRST REFUSAL" name the layer.

use fsqlite_core::connection::Connection;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

#[test]
#[ignore = "bd-udetu source-localisation probe; run explicitly with RUST_LOG"]
fn locate_the_instant_busy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("udetu_src.db");
    let path = path.to_str().expect("utf-8").to_owned();

    {
        let p = path.clone();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&p).await.expect("open seed");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute("PRAGMA busy_timeout=10000").await.expect("bt");
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .await
                .expect("ddl");
            let payload = "d".repeat(1500);
            for i in 0..1200 {
                conn.execute(&format!("INSERT INTO t(id,v) VALUES({i},'{payload}')"))
                    .await
                    .expect("seed");
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
                let conn = Connection::open(&p).await.expect("open ckpt");
                conn.execute("PRAGMA busy_timeout=10000").await.expect("bt");
                while !stop.load(Ordering::Relaxed) {
                    let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;
                    let _ = conn.execute("INSERT INTO t(v) VALUES('x')").await;
                }
                conn.close().await.expect("close ckpt");
            });
        })
    };

    {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&p).await.expect("open writer");
            conn.execute("PRAGMA busy_timeout=10000").await.expect("bt");
            let deadline = Instant::now() + std::time::Duration::from_secs(30);
            let mut attempts = 0u64;
            while Instant::now() < deadline {
                attempts += 1;
                let started = Instant::now();
                match conn.execute("BEGIN CONCURRENT").await {
                    Ok(_) => {
                        let _ = conn.execute("COMMIT").await;
                    }
                    Err(error) => {
                        println!(
                            "\n>>> FIRST REFUSAL after {attempts} attempts, {}ms: {error}",
                            started.elapsed().as_millis()
                        );
                        println!(
                            ">>> error_code={:?} is_transient={}",
                            error.error_code(),
                            error.is_transient()
                        );
                        if conn.in_transaction() {
                            let _ = conn.execute("ROLLBACK").await;
                        }
                        break;
                    }
                }
            }
            stop.store(true, Ordering::Relaxed);
            conn.close().await.expect("close writer");
        });
    }

    checkpointer.join().expect("checkpointer");
}
