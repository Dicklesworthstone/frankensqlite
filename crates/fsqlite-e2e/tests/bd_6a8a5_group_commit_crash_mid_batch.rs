#![recursion_limit = "512"]

//! bd-6a8a5: a crash in the middle of a group-commit batch.
//!
//! Concurrent writers commit short `synchronous=FULL` transactions on one
//! file, so the group-commit queue flushes several of them per WAL append
//! and sync. After every acknowledged commit, the live files are copied, and
//! each copy has its WAL cut where a crash can cut it: at every frame
//! boundary, and inside a frame (a torn write). Each image is reopened by
//! FrankenSQLite and, separately, by stock SQLite. Every transaction must be
//! either fully present or fully absent, each writer's surviving
//! transactions must be a prefix of the ones it committed (a later commit
//! never survives an earlier one), both engines must read the same rows, and
//! the uncut image must hold every acknowledged transaction.
//!
//! Each writer owns a table seeded (and checkpointed) before the run, and a
//! transaction rewrites `ROWS_PER_TXN` of its seeded rows with a same-length
//! committed payload. No transaction grows the file: inserting fresh
//! 1500-byte rows extended it in nearly every transaction, the writers then
//! conflicted on the page count and committed one at a time, and every run
//! flushed 120 single-transaction epochs.
//!
//! The run must have flushed at least one epoch of two or more
//! transactions, or the cuts would never land inside a batch.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};

const WRITERS: usize = 6;
const TXNS_PER_WRITER: i64 = 20;
const ROWS_PER_TXN: i64 = 3;
/// Large enough that a transaction's rows span more than one leaf page, so
/// a cut can also land between two frames of one transaction.
const PAYLOAD_BYTES: usize = 1_500;
const PAGE_SIZE: usize = 4_096;
const WAL_HEADER_BYTES: usize = 32;
const WAL_FRAME_BYTES: usize = 24 + PAGE_SIZE;
const SIDECAR_SUFFIXES: [&str; 4] = ["", "-wal-fec", "-wal-cert", "-wal-cert-head"];

fn table(writer: usize) -> String {
    format!("w{writer}")
}

fn fill(stem: &str) -> String {
    stem.repeat(PAYLOAD_BYTES / stem.len() + 1)[..PAYLOAD_BYTES].to_owned()
}

/// The payload a row holds before the run.
fn seed(writer: usize, id: i64) -> String {
    fill(&format!("w{writer}-row{id}-seed-"))
}

/// The payload a committed transaction writes into the row.
fn payload(writer: usize, id: i64) -> String {
    fill(&format!("w{writer}-row{id}-"))
}

fn is_retryable(error: &fsqlite::FrankenError) -> bool {
    matches!(
        error,
        fsqlite::FrankenError::Busy
            | fsqlite::FrankenError::BusyRecovery
            | fsqlite::FrankenError::BusySnapshot { .. }
            | fsqlite::FrankenError::DatabaseLocked { .. }
            | fsqlite::FrankenError::WriteConflict { .. }
            | fsqlite::FrankenError::SerializationFailure { .. }
    )
}

async fn configure(conn: &fsqlite::Connection) {
    for pragma in [
        "PRAGMA journal_mode=WAL;",
        "PRAGMA synchronous=FULL;",
        "PRAGMA busy_timeout=10000;",
        "PRAGMA wal_autocheckpoint=0;",
    ] {
        conn.execute(pragma)
            .await
            .unwrap_or_else(|error| panic!("{pragma}: {error}"));
    }
}

/// One writer: `TXNS_PER_WRITER` transactions, each rewriting `ROWS_PER_TXN`
/// seeded rows of its table, retried whole on a retryable refusal. Every
/// writer starts each round's transaction at the same `round` barrier, so
/// their commits reach the group-commit queue together. Waits at `copied`
/// until the main thread has copied the live files, so no close can
/// checkpoint first. Returns how many attempts were retried.
fn run_writer(
    path: String,
    writer: usize,
    round: Arc<Barrier>,
    done: Arc<Barrier>,
    copied: Arc<Barrier>,
) -> u32 {
    fsqlite_e2e::block_on(async {
        let conn = fsqlite::Connection::open(path)
            .await
            .expect("open writer connection");
        configure(&conn).await;
        let update = format!("UPDATE {} SET payload = ?2 WHERE id = ?1", table(writer));
        let mut retries = 0_u32;
        for txn in 0..TXNS_PER_WRITER {
            round.wait();
            let mut attempts = 0_u32;
            loop {
                attempts += 1;
                assert!(
                    attempts < 10_000,
                    "writer {writer} txn {txn} never committed"
                );
                if attempts > 1 {
                    retries += 1;
                }
                if let Err(error) = conn.execute("BEGIN").await {
                    assert!(is_retryable(&error), "BEGIN: {error}");
                    continue;
                }
                let mut failed = None;
                for k in 0..ROWS_PER_TXN {
                    let id = txn * ROWS_PER_TXN + k;
                    let params = [
                        fsqlite::SqliteValue::Integer(id),
                        fsqlite::SqliteValue::Text(payload(writer, id).into()),
                    ];
                    match conn.execute_with_params(&update, &params).await {
                        Ok(changed) => {
                            assert_eq!(changed, 1, "writer {writer} row {id}: rows updated");
                        }
                        Err(error) => {
                            failed = Some(error);
                            break;
                        }
                    }
                }
                let result = match failed {
                    Some(error) => Err(error),
                    None => conn.execute("COMMIT").await.map(|_| ()),
                };
                match result {
                    Ok(()) => break,
                    Err(error) => {
                        assert!(is_retryable(&error), "writer {writer} txn {txn}: {error}");
                        if conn.in_transaction() {
                            conn.execute("ROLLBACK").await.expect("rollback");
                        }
                    }
                }
            }
        }
        done.wait();
        copied.wait();
        conn.close().await.expect("close writer connection");
        retries
    })
}

fn copy_live_files(live_db: &Path, image_dir: &Path, wal: &[u8]) -> PathBuf {
    std::fs::create_dir_all(image_dir).expect("create image dir");
    let image_db = image_dir.join("live.db");
    for suffix in SIDECAR_SUFFIXES {
        let from = PathBuf::from(format!("{}{suffix}", live_db.display()));
        if from.exists() {
            std::fs::copy(&from, format!("{}{suffix}", image_db.display()))
                .expect("copy live file");
        }
    }
    std::fs::write(format!("{}-wal", image_db.display()), wal).expect("write cut WAL");
    image_db
}

/// Per writer, the transactions present in the image: every one fully
/// present (all `ROWS_PER_TXN` of its rows hold the committed payload) or
/// fully absent (all of them still hold the seed).
type Survivors = BTreeMap<usize, Vec<i64>>;

fn survivors_from_rows(writer: usize, rows: &[(i64, String)], cut: &str) -> Vec<i64> {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let seeded: Vec<i64> = (0..TXNS_PER_WRITER * ROWS_PER_TXN).collect();
    assert_eq!(ids, seeded, "{cut}: writer {writer}'s seeded rows");
    let mut by_txn = BTreeMap::<i64, i64>::new();
    for (id, text) in rows {
        let committed = text == &payload(writer, *id);
        assert!(
            committed || text == &seed(writer, *id),
            "{cut}: writer {writer} row {id} holds neither its seed nor its committed payload"
        );
        if committed {
            *by_txn.entry(id / ROWS_PER_TXN).or_default() += 1;
        }
    }
    for (txn, count) in &by_txn {
        assert_eq!(
            *count, ROWS_PER_TXN,
            "{cut}: writer {writer} transaction {txn} is partially present ({count} of {ROWS_PER_TXN} rows)"
        );
    }
    let txns: Vec<i64> = by_txn.into_keys().collect();
    let expected_prefix: Vec<i64> = (0..i64::try_from(txns.len()).expect("fits")).collect();
    assert_eq!(
        txns, expected_prefix,
        "{cut}: writer {writer}'s surviving transactions are not a prefix of its commits"
    );
    txns
}

fn fsqlite_survivors(image_db: &Path, cut: &str) -> Survivors {
    fsqlite_e2e::block_on(async {
        let conn = fsqlite::Connection::open(image_db.display().to_string())
            .await
            .unwrap_or_else(|error| panic!("{cut}: FrankenSQLite reopen: {error}"));
        let mut survivors = Survivors::new();
        for writer in 0..WRITERS {
            let sql = format!("SELECT id, payload FROM {} ORDER BY id", table(writer));
            let rows = conn
                .query(&sql)
                .await
                .unwrap_or_else(|error| panic!("{cut}: {sql}: {error}"));
            let rows: Vec<(i64, String)> = rows
                .iter()
                .map(|row| {
                    let id = row
                        .get(0)
                        .and_then(fsqlite::SqliteValue::as_integer)
                        .expect("integer id");
                    let text = match row.get(1) {
                        Some(fsqlite::SqliteValue::Text(text)) => text.as_ref().to_owned(),
                        other => panic!("{cut}: non-text payload {other:?}"),
                    };
                    (id, text)
                })
                .collect();
            let label = format!("{cut} [FrankenSQLite]");
            survivors.insert(writer, survivors_from_rows(writer, &rows, &label));
        }
        let check = conn
            .query("PRAGMA integrity_check;")
            .await
            .unwrap_or_else(|error| panic!("{cut}: integrity_check: {error}"));
        assert!(
            matches!(check[0].get(0), Some(fsqlite::SqliteValue::Text(text)) if text.as_ref() == "ok"),
            "{cut}: FrankenSQLite integrity_check {:?}",
            check[0].get(0)
        );
        conn.close().await.expect("close image");
        survivors
    })
}

fn stock_survivors(image_db: &Path, cut: &str) -> Survivors {
    let conn = rusqlite::Connection::open(image_db).expect("stock open");
    let mut survivors = Survivors::new();
    for writer in 0..WRITERS {
        let sql = format!("SELECT id, payload FROM {} ORDER BY id", table(writer));
        let mut stmt = conn.prepare(&sql).expect("stock prepare");
        let rows: Vec<(i64, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("stock query")
            .collect::<Result<_, _>>()
            .expect("stock rows");
        let label = format!("{cut} [stock]");
        survivors.insert(writer, survivors_from_rows(writer, &rows, &label));
    }
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("stock integrity_check");
    assert_eq!(integrity, "ok", "{cut}: stock integrity_check");
    survivors
}

#[test]
fn every_transaction_of_a_torn_group_commit_batch_is_all_or_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live_dir = dir.path().join("live");
    std::fs::create_dir_all(&live_dir).expect("live dir");
    let live_db = live_dir.join("live.db");
    let path = live_db.display().to_string();

    fsqlite_e2e::block_on(async {
        let conn = fsqlite::Connection::open(path.clone())
            .await
            .expect("open schema connection");
        conn.execute("PRAGMA page_size=4096;")
            .await
            .expect("page_size");
        configure(&conn).await;
        for writer in 0..WRITERS {
            conn.execute(&format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);",
                table(writer)
            ))
            .await
            .expect("create table");
            let insert = format!(
                "INSERT INTO {} (id, payload) VALUES (?1, ?2)",
                table(writer)
            );
            conn.execute("BEGIN").await.expect("begin seed");
            for id in 0..TXNS_PER_WRITER * ROWS_PER_TXN {
                let params = [
                    fsqlite::SqliteValue::Integer(id),
                    fsqlite::SqliteValue::Text(seed(writer, id).into()),
                ];
                conn.execute_with_params(&insert, &params)
                    .await
                    .expect("seed row");
            }
            conn.execute("COMMIT").await.expect("commit seed");
        }
        // The seeded tables live in the database file, so every cut of the
        // run's WAL still has them.
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("checkpoint the seed");
        conn.close().await.expect("close schema connection");
    });

    fsqlite_wal::GLOBAL_CONSOLIDATION_METRICS.reset();
    let round = Arc::new(Barrier::new(WRITERS));
    let done = Arc::new(Barrier::new(WRITERS + 1));
    let copied = Arc::new(Barrier::new(WRITERS + 1));
    let handles: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let (path, round, done, copied) = (
                path.clone(),
                Arc::clone(&round),
                Arc::clone(&done),
                Arc::clone(&copied),
            );
            std::thread::spawn(move || run_writer(path, writer, round, done, copied))
        })
        .collect();
    done.wait();

    // Every commit is acknowledged; copy before any connection closes.
    let wal = std::fs::read(format!("{path}-wal")).expect("read live WAL");
    let base = dir.path().join("base");
    copy_live_files(&live_db, &base, &wal);
    copied.wait();
    let retries: u32 = handles
        .into_iter()
        .map(|handle| handle.join().expect("writer thread"))
        .sum();

    let histogram = fsqlite_wal::GLOBAL_CONSOLIDATION_METRICS
        .snapshot()
        .flush_batch_size_histogram;
    let multi_member_epochs: u64 = histogram[1..].iter().sum();
    eprintln!(
        "bd-6a8a5 crash-mid-batch: epoch batch-size histogram {histogram:?}, {retries} retried attempts"
    );
    assert!(
        multi_member_epochs > 0,
        "no epoch flushed two or more transactions ({histogram:?}): no cut can land inside a batch"
    );

    assert!(
        wal.len() > WAL_HEADER_BYTES,
        "the run left frames in the WAL"
    );
    let frames = (wal.len() - WAL_HEADER_BYTES) / WAL_FRAME_BYTES;
    // Each frame's (page number, commit db_size): a commit marker is a
    // nonzero db_size, and only an epoch's last frame may carry one.
    let (whole_frames, _) = wal[WAL_HEADER_BYTES..].as_chunks::<WAL_FRAME_BYTES>();
    let headers: Vec<(u32, u32)> = whole_frames
        .iter()
        .map(|frame| {
            let word = |at: usize| {
                u32::from_be_bytes(frame[at..at + 4].try_into().expect("four header bytes"))
            };
            (word(0), word(4))
        })
        .collect();
    let commit_frames = headers.iter().filter(|(_, db_size)| *db_size != 0).count();
    let sidecars: Vec<&str> = SIDECAR_SUFFIXES
        .into_iter()
        .filter(|suffix| {
            PathBuf::from(format!("{}{suffix}", base.join("live.db").display())).exists()
        })
        .collect();
    eprintln!(
        "bd-6a8a5 crash-mid-batch: {frames} frames, {commit_frames} commit-marked, first frames {:?}, sidecars {sidecars:?}",
        &headers[..headers.len().min(12)]
    );

    // Every frame boundary: a sampled subset can skip every cut inside a
    // transaction (with two frames per transaction, an even stride lands
    // only between transactions).
    let mut previous: Option<Survivors> = None;
    for frame in 0..=frames {
        let end = WAL_HEADER_BYTES + frame * WAL_FRAME_BYTES;
        for (kind, len) in [("boundary", end), ("torn", end + WAL_FRAME_BYTES / 2)] {
            if len > wal.len() {
                continue;
            }
            let cut = format!("cut after {frame} of {frames} frames ({kind})");
            let fsqlite_dir = dir.path().join(format!("f{frame}-{kind}"));
            let stock_dir = dir.path().join(format!("s{frame}-{kind}"));
            let fsqlite_image = copy_live_files(&base.join("live.db"), &fsqlite_dir, &wal[..len]);
            let stock_image = copy_live_files(&base.join("live.db"), &stock_dir, &wal[..len]);
            let ours = fsqlite_survivors(&fsqlite_image, &cut);
            let theirs = stock_survivors(&stock_image, &cut);
            // Each image is read once; drop it so the run's scratch stays at
            // two images rather than one pair per cut.
            std::fs::remove_dir_all(&fsqlite_dir).expect("drop the FrankenSQLite image");
            std::fs::remove_dir_all(&stock_dir).expect("drop the stock image");
            assert_eq!(
                ours, theirs,
                "{cut}: FrankenSQLite and stock read different rows"
            );
            if let Some(previous) = &previous {
                for writer in 0..WRITERS {
                    assert!(
                        ours[&writer].len() >= previous[&writer].len(),
                        "{cut}: writer {writer} lost a transaction a shorter cut kept"
                    );
                }
            }
            if kind == "boundary" {
                previous = Some(ours.clone());
            }
            if frame == frames && kind == "boundary" {
                for writer in 0..WRITERS {
                    assert_eq!(
                        i64::try_from(ours[&writer].len()).expect("fits"),
                        TXNS_PER_WRITER,
                        "{cut}: writer {writer} lost an acknowledged FULL commit"
                    );
                }
            }
        }
    }
}
