//! bd-pa8e5 — is it CREATE INDEX that corrupts the index, or ordinary writes to
//! an indexed table while a checkpoint runs?
//!
//! The previous probe hammered `CREATE INDEX IF NOT EXISTS` in a loop beside a
//! checkpointer and left a database that stock SQLite's own integrity_check
//! calls corrupt, in 3 of 4 runs, on unmodified main. But almost all of those
//! 15_000-odd "successes" were IF NOT EXISTS no-ops after the first real
//! creation — so the loop may be incidental. Once the index exists, every
//! INSERT the checkpointer does has to maintain it, and THAT is the ordinary
//! workload worth suspecting.
//!
//! So separate the two. Each case creates the index exactly once, quietly, then
//! runs a different antagonist, then asks stock SQLite whether the database is
//! intact:
//!
//!   A  index + concurrent INSERTs, NO checkpointing
//!   B  index + concurrent INSERTs + TRUNCATE checkpoints
//!   C  no index + concurrent INSERTs + TRUNCATE checkpoints   (control)
//!
//! If B is corrupt and A and C are clean, the defect is index maintenance
//! racing a checkpoint, and CREATE INDEX has nothing to do with it. Prints;
//! asserts nothing, so it reports what happens rather than what I expect.
//!
//!   cargo test -p fsqlite-core --test bd_pa8e5_index_maintenance_probe -- --ignored --nocapture

use fsqlite_core::connection::Connection;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const BUSY_TIMEOUT_MS: u64 = 4_000;
const RUN_SECS: u64 = 8;

/// Ask stock SQLite — which reads only committed durable state — whether the
/// database is intact. Returns the first integrity_check row.
fn stock_integrity(db: &Path) -> String {
    let Ok(stock) = rusqlite::Connection::open(db) else {
        return "<stock could not open>".to_owned();
    };
    stock
        .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
        .unwrap_or_else(|error| format!("<integrity_check failed: {error}>"))
}

fn seed(path: &str, with_index: bool) {
    let path = path.to_owned();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(&path).await.expect("open seed");
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
        if with_index {
            // Exactly once, with no antagonist running. Whatever this probe
            // finds afterwards cannot be blamed on a contended CREATE INDEX.
            conn.execute("CREATE INDEX probe_ix ON t(v)")
                .await
                .expect("create index");
        }
        conn.close().await.expect("close seed");
    });
}

/// Run `writers` INSERT loops for RUN_SECS, optionally with a TRUNCATE
/// checkpointer beside them, then report stock's verdict.
fn exercise(label: &str, db: &Path, with_index: bool, checkpointing: bool) {
    let path = db.to_str().expect("utf-8 path").to_owned();
    seed(&path, with_index);
    println!("\n== {label}");
    println!("  after seed: {}", stock_integrity(db));

    let stop = Arc::new(AtomicBool::new(false));
    let checkpointer = checkpointing.then(|| {
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
                    // Pace the antagonist. An unthrottled TRUNCATE loop starves
                    // the writer completely now that busy_timeout is honoured:
                    // a single INSERT eats the whole run waiting it out, and the
                    // probe measures nothing. An earlier revision did exactly
                    // that -- 0 inserts in 8 seconds.
                    asupersync::time::sleep(
                        asupersync::time::wall_now(),
                        Duration::from_millis(3),
                    )
                    .await;
                }
                conn.close().await.expect("close checkpointer");
            });
        })
    });

    let writer = {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open writer");
                conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                    .await
                    .expect("busy_timeout");
                let deadline = Instant::now() + Duration::from_secs(RUN_SECS);
                let (mut ok, mut err) = (0u64, 0u64);
                while Instant::now() < deadline {
                    if conn
                        .execute("INSERT INTO t(v) VALUES('w')")
                        .await
                        .is_ok()
                    {
                        ok += 1;
                    } else {
                        err += 1;
                    }
                }
                stop.store(true, Ordering::Relaxed);
                println!("  writer: {ok} inserts ok, {err} refused");
                if ok < 100 {
                    println!("  (too few inserts to conclude anything from this case)");
                }
                conn.close().await.expect("close writer");
            });
        })
    };

    writer.join().expect("writer thread");
    stop.store(true, Ordering::Relaxed);
    if let Some(handle) = checkpointer {
        handle.join().expect("checkpointer thread");
    }
    println!("  STOCK VERDICT: {}", stock_integrity(db));
}

/// Case D, the realistic one: a single CREATE INDEX issued WHILE the table is
/// being written and checkpointed — a migration running against a live app.
/// Cases A/B/C build the index during a quiet seed, so none of them cover it.
#[test]
#[ignore = "bd-pa8e5 isolation probe; prints stock integrity_check verdicts"]
fn one_create_index_while_the_table_is_live() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("d.db");
    let path = db.to_str().expect("utf-8 path").to_owned();
    seed(&path, false);
    println!("\n== D  ONE CREATE INDEX issued under concurrent writes + checkpoints");
    println!("  after seed: {}", stock_integrity(&db));

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
                    asupersync::time::sleep(asupersync::time::wall_now(), Duration::from_millis(3))
                        .await;
                }
                conn.close().await.expect("close checkpointer");
            });
        })
    };
    let writer = {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(&p).await.expect("open writer");
                conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                    .await
                    .expect("busy_timeout");
                let mut ok = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if conn.execute("INSERT INTO t(v) VALUES('w')").await.is_ok() {
                        ok += 1;
                    }
                }
                println!("  writer: {ok} inserts ok");
                conn.close().await.expect("close writer");
            });
        })
    };

    {
        let p = path.clone();
        let stop = Arc::clone(&stop);
        asupersync::test_utils::run_test(|| async move {
            let conn = Connection::open(&p).await.expect("open migrator");
            conn.execute(&format!("PRAGMA busy_timeout={BUSY_TIMEOUT_MS}"))
                .await
                .expect("busy_timeout");
            // Let the antagonists get going, then issue the migration ONCE.
            asupersync::time::sleep(asupersync::time::wall_now(), Duration::from_millis(500)).await;
            let outcome = conn.execute("CREATE INDEX probe_ix ON t(v)").await;
            println!("  single CREATE INDEX: {outcome:?}");
            asupersync::time::sleep(asupersync::time::wall_now(), Duration::from_secs(3)).await;
            stop.store(true, Ordering::Relaxed);
            conn.close().await.expect("close migrator");
        });
    }
    writer.join().expect("writer thread");
    checkpointer.join().expect("checkpointer thread");
    println!("  STOCK VERDICT: {}", stock_integrity(&db));
}

#[test]
#[ignore = "bd-pa8e5 isolation probe; prints stock integrity_check verdicts"]
fn does_index_maintenance_race_the_checkpoint() {
    let dir = tempfile::tempdir().expect("tempdir");
    exercise(
        "A  indexed table, concurrent INSERTs, NO checkpointing",
        &dir.path().join("a.db"),
        true,
        false,
    );
    exercise(
        "B  indexed table, concurrent INSERTs, TRUNCATE checkpoints",
        &dir.path().join("b.db"),
        true,
        true,
    );
    exercise(
        "C  NO index, concurrent INSERTs, TRUNCATE checkpoints (control)",
        &dir.path().join("c.db"),
        false,
        true,
    );
    println!(
        "\nB corrupt with A and C clean means index maintenance racing a checkpoint, \
         and CREATE INDEX is incidental."
    );
}
