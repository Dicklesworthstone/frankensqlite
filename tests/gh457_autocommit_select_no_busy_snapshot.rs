#![recursion_limit = "512"]

//! GH#457 / bd-reality-check-1022-llk6g.23: an autocommit SELECT must not
//! return BUSY because a writer published a commit while the read was
//! starting. Stock WAL readers never see BUSY in this steady state (only the
//! rare BUSY_RECOVERY). fsqlite answered `database is busy (snapshot conflict
//! on pages: pager publication advanced between metadata bind and reload
//! transaction)` whenever a peer commit landed between the reader's
//! publication bind and its reload transaction, with the default
//! busy_timeout of 0.
//!
//! Readers here use both ad-hoc and prepared SELECTs against one database that
//! a writer thread keeps committing to. An explicit read transaction must keep
//! its snapshot while the writer runs.

#![cfg(feature = "native")]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const READERS: usize = 4;
const RUN: Duration = Duration::from_secs(3);

fn count_of(rows: &[fsqlite_core::connection::Row]) -> i64 {
    match rows.first().map(|row| row.values()) {
        Some([SqliteValue::Integer(count)]) => *count,
        other => panic!("unexpected count row {other:?}"),
    }
}

#[test]
fn autocommit_selects_never_see_busy_while_a_writer_commits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir
        .path()
        .join("gh457.db")
        .to_str()
        .expect("utf8")
        .to_owned();
    asupersync::test_utils::run_test(|| {
        let path = path.clone();
        async move {
            let conn = Connection::open(path).await.expect("seed open");
            conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
                .await
                .expect("schema");
            conn.close().await.expect("seed close");
        }
    });

    let stop = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let busy = Arc::new(Mutex::new(Vec::<String>::new()));

    let writer = {
        let (path, stop, commits) = (path.clone(), Arc::clone(&stop), Arc::clone(&commits));
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async move {
                let conn = Connection::open(path).await.expect("writer open");
                conn.execute("PRAGMA busy_timeout=5000")
                    .await
                    .expect("writer timeout");
                while !stop.load(Ordering::Relaxed) {
                    conn.execute("INSERT INTO t(v) VALUES (randomblob(64))")
                        .await
                        .expect("writer insert");
                    commits.fetch_add(1, Ordering::Relaxed);
                }
                conn.close().await.expect("writer close");
            });
        })
    };

    let readers: Vec<_> = (0..READERS)
        .map(|reader| {
            let (path, stop, reads, busy) = (
                path.clone(),
                Arc::clone(&stop),
                Arc::clone(&reads),
                Arc::clone(&busy),
            );
            std::thread::spawn(move || {
                asupersync::test_utils::run_test(|| async move {
                    // Default busy_timeout (0), as in stock: readers never wait.
                    let conn = Connection::open(path).await.expect("reader open");
                    let prepared = conn
                        .prepare("SELECT count(*) FROM t")
                        .await
                        .expect("prepare");
                    let mut last = 0;
                    while !stop.load(Ordering::Relaxed) {
                        let outcome = if reader % 2 == 0 {
                            conn.query("SELECT count(*) FROM t").await
                        } else {
                            prepared.query().await
                        };
                        match outcome {
                            Ok(rows) => {
                                let count = count_of(&rows);
                                assert!(count >= last, "reader {reader}: count went back");
                                last = count;
                                reads.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(error) if error.is_transient() => {
                                busy.lock()
                                    .unwrap()
                                    .push(format!("reader {reader}: {error}"));
                            }
                            Err(error) => panic!("reader {reader}: {error}"),
                        }
                    }
                    drop(prepared);
                    conn.close().await.expect("reader close");
                });
            })
        })
        .collect();

    // An explicit read transaction keeps its snapshot while the writer runs.
    let snapshot_path = path.clone();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(snapshot_path)
            .await
            .expect("snapshot open");
        conn.execute("PRAGMA busy_timeout=5000")
            .await
            .expect("snapshot timeout");
        let started = Instant::now();
        while started.elapsed() < RUN {
            conn.execute("BEGIN").await.expect("begin");
            let first = count_of(
                &conn
                    .query("SELECT count(*) FROM t")
                    .await
                    .expect("first read"),
            );
            std::thread::sleep(Duration::from_millis(20));
            let second = count_of(
                &conn
                    .query("SELECT count(*) FROM t")
                    .await
                    .expect("second read"),
            );
            conn.execute("COMMIT").await.expect("commit");
            assert_eq!(
                first, second,
                "an explicit read transaction keeps its snapshot"
            );
        }
        conn.close().await.expect("snapshot close");
    });

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread");
    for reader in readers {
        reader.join().expect("reader thread");
    }
    let busy = busy.lock().unwrap();
    eprintln!(
        "event=gh457_autocommit_reads commits={} reads={} busy={} sample={:?}",
        commits.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
        busy.len(),
        busy.iter().take(5).collect::<Vec<_>>()
    );
    assert!(commits.load(Ordering::Relaxed) > 0, "the writer committed");
    assert!(reads.load(Ordering::Relaxed) > 0, "the readers read");
    assert!(
        busy.is_empty(),
        "autocommit SELECTs returned BUSY: {busy:?}"
    );
}
