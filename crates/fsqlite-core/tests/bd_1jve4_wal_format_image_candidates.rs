//! bd-1jve4: whole-image publication and the bounded structural snapshot
//! validate a caller's WAL-format candidate image (header bytes 18/19 = 2/2,
//! no companions) without creating `-wal`/`-shm` beside it.
//!
//! Before the fix both opened the candidate through the ordinary
//! identity-bound read-only open, which (bd-cf0be, as stock's does) creates an
//! empty `-wal` and a 32 KiB `-shm` for a WAL-format file. The bounded APIs
//! then refused the WAL-format header and left that pair beside the caller's
//! image; the plain publication read the candidate and then refused it at
//! commit, because of the `-shm` its own validation created ("VACUUM candidate
//! is not self-contained"), the self-inflicted refusal bd-gfx4q fixed for
//! VACUUM. Seen on c8112ae72 for a stock 3.54.1 candidate and an fsqlite one.
//!
//! Negative: a candidate whose `-wal` holds frames is still refused before
//! any open, and the refusal creates nothing.

use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

/// Every file in `dir` whose name starts with `stem`, with its size.
fn files_beside(dir: &Path, stem: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().to_string_lossy().into_owned();
            name.starts_with(stem).then(|| {
                let len = entry.metadata().map_or(u64::MAX, |meta| meta.len());
                format!("{name}:{len}")
            })
        })
        .collect();
    names.sort();
    names
}

/// The files beside `candidate` other than the candidate itself.
fn companions(candidate: &Path) -> Vec<String> {
    let dir = candidate.parent().expect("candidate dir");
    let name = candidate
        .file_name()
        .and_then(|name| name.to_str())
        .expect("candidate name");
    files_beside(dir, name)
        .into_iter()
        .filter(|entry| !entry.starts_with(&format!("{name}:")))
        .collect()
}

const ROWS: &str = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
                    INSERT INTO t(b) VALUES ('one'), ('two'), ('three');
                    CREATE INDEX t_b ON t(b);";

/// A WAL-format image written by stock SQLite: WAL mode, checkpointed, and
/// cleanly closed, so no companion remains.
fn stock_wal_image(dir: &Path) -> Vec<u8> {
    let path = dir.join("stock-made.db");
    let stock = rusqlite::Connection::open(&path).expect("stock open");
    let mode: String = stock
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .expect("stock WAL");
    assert_eq!(mode, "wal");
    stock.execute_batch(ROWS).expect("stock rows");
    let _: i64 = stock
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
        .expect("stock checkpoint");
    stock.close().expect("stock close");
    std::fs::read(&path).expect("stock image")
}

/// A WAL-format image written by fsqlite (its main file; the rows are in it
/// after the close's checkpoint).
async fn fsqlite_wal_image(dir: &Path) -> Vec<u8> {
    let path = dir.join("frank-made.db");
    let conn = Connection::open(path.to_string_lossy().into_owned())
        .await
        .expect("fsqlite open");
    conn.execute("PRAGMA journal_mode=WAL")
        .await
        .expect("fsqlite WAL");
    for statement in ROWS.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        conn.execute(statement).await.expect("fsqlite rows");
    }
    conn.close().await.expect("fsqlite close");
    std::fs::read(&path).expect("fsqlite image")
}

/// The rows of `t` as stock reads them from `path`.
fn stock_rows(path: &Path) -> Vec<(i64, String)> {
    let stock = rusqlite::Connection::open(path).expect("stock open");
    let mut statement = stock
        .prepare("SELECT a, b FROM t ORDER BY a")
        .expect("stock prepare");
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("stock query")
        .collect::<Result<_, _>>()
        .expect("stock rows")
}

async fn fsqlite_count(conn: &Connection) -> i64 {
    let rows = conn.query("SELECT count(*) FROM t").await.expect("count");
    match rows.first().map(|row| &row.values()[0]) {
        Some(SqliteValue::Integer(count)) => *count,
        other => panic!("count(*) returned {other:?}"),
    }
}

/// A bounded API either validates the WAL-format image or refuses its header
/// as an unsupported shape; it never refuses for any other reason here.
fn assert_bounded_outcome(context: &str, outcome: &Result<i64, FrankenError>) {
    match outcome {
        Ok(rows) => assert_eq!(*rows, 3, "{context}: rows read through the snapshot"),
        Err(FrankenError::NotImplemented(detail)) => assert!(
            detail.contains("not rollback/DELETE mode"),
            "{context}: refused {detail}"
        ),
        Err(error) => panic!("{context}: {error:?}"),
    }
}

async fn run_candidate(dir: &Path, producer: &str, image: &[u8]) {
    let expected_rows = {
        let oracle = dir.join(format!("{producer}-oracle.db"));
        std::fs::write(&oracle, image).expect("oracle copy");
        stock_rows(&oracle)
    };
    assert_eq!(expected_rows.len(), 3, "{producer}: stock reads the image");
    let owner_path = dir.join(format!("{producer}-owner.db"));
    let owner = Connection::open(owner_path.to_string_lossy().into_owned())
        .await
        .expect("owner open");
    owner
        .execute("CREATE TABLE o(x)")
        .await
        .expect("owner table");

    // The bounded structural snapshot.
    let candidate = dir.join(format!("{producer}-snapshot.db"));
    std::fs::write(&candidate, image).expect("candidate");
    let receipt = owner
        .inspect_self_contained_image_receipt(&candidate)
        .await
        .expect("receipt");
    let outcome = match owner
        .begin_bounded_structural_snapshot(&receipt, &candidate, 256)
        .await
    {
        Ok(snapshot) => {
            let rows = fsqlite_count(snapshot.connection()).await;
            snapshot.finish().await.map(|()| rows)
        }
        Err(error) => Err(error),
    };
    let context = format!("{producer} bounded snapshot");
    assert_bounded_outcome(&context, &outcome);
    assert_eq!(companions(&candidate), Vec::<String>::new(), "{context}");
    assert_eq!(std::fs::read(&candidate).expect("read"), image, "{context}");

    // The bounded publication, abandoned.
    let candidate = dir.join(format!("{producer}-bounded.db"));
    std::fs::write(&candidate, image).expect("candidate");
    let receipt = owner
        .inspect_self_contained_image_receipt(&candidate)
        .await
        .expect("receipt");
    let source = owner
        .capture_database_image_receipt()
        .await
        .expect("source receipt");
    let outcome = match owner
        .begin_bounded_database_image_publication(&source, &receipt, &candidate, 256)
        .await
    {
        Ok(pending) => {
            let rows = fsqlite_count(pending.candidate()).await;
            pending.abandon().await.map(|()| rows)
        }
        Err(error) => Err(error),
    };
    let context = format!("{producer} bounded publication");
    assert_bounded_outcome(&context, &outcome);
    assert_eq!(companions(&candidate), Vec::<String>::new(), "{context}");
    assert_eq!(std::fs::read(&candidate).expect("read"), image, "{context}");

    // The plain publication commits the image.
    let candidate = dir.join(format!("{producer}-published.db"));
    std::fs::write(&candidate, image).expect("candidate");
    let receipt = owner
        .inspect_self_contained_image_receipt(&candidate)
        .await
        .expect("receipt");
    let source = owner
        .capture_database_image_receipt()
        .await
        .expect("source receipt");
    let context = format!("{producer} publication");
    let pending = owner
        .begin_database_image_publication(&source, &receipt, &candidate)
        .await
        .unwrap_or_else(|error| panic!("{context}: begin: {error:?}"));
    assert_eq!(fsqlite_count(pending.candidate()).await, 3, "{context}");
    if let Err(error) = pending.commit().await {
        panic!(
            "{context}: commit: {error:?}; beside the candidate: {:?}",
            companions(&candidate)
        );
    }
    assert_eq!(companions(&candidate), Vec::<String>::new(), "{context}");
    assert_eq!(fsqlite_count(&owner).await, 3, "{context}: published rows");
    owner.close().await.expect("owner close");
    assert_eq!(
        stock_rows(&owner_path),
        expected_rows,
        "{context}: the published database against stock's read of the image"
    );
    let integrity: String = rusqlite::Connection::open(&owner_path)
        .expect("stock open")
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("stock integrity");
    assert_eq!(integrity, "ok", "{context}");
}

#[test]
fn wal_format_candidates_validate_and_publish_without_creating_companions() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let stock = stock_wal_image(dir.path());
        assert_eq!((stock[18], stock[19]), (2, 2), "a WAL-format header");
        run_candidate(dir.path(), "stock", &stock).await;
        let frank = fsqlite_wal_image(dir.path()).await;
        assert_eq!((frank[18], frank[19]), (2, 2), "a WAL-format header");
        run_candidate(dir.path(), "fsqlite", &frank).await;
    });
}

/// The negative a validator that skipped the self-contained guard would
/// fail: a WAL-format candidate whose `-wal` still holds stock's committed
/// frames is refused at its receipt, before any open, and the refusal creates
/// no `-shm`.
#[test]
fn a_wal_format_candidate_with_frames_in_its_wal_is_still_refused() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("stock-live.db");
        let stock = rusqlite::Connection::open(&source).expect("stock open");
        stock
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .expect("stock WAL");
        stock.execute_batch(ROWS).expect("stock rows");
        let candidate: PathBuf = dir.path().join("with-frames.db");
        std::fs::copy(&source, &candidate).expect("copy main");
        let mut source_wal = source.clone().into_os_string();
        source_wal.push("-wal");
        let mut candidate_wal = candidate.clone().into_os_string();
        candidate_wal.push("-wal");
        std::fs::copy(&source_wal, &candidate_wal).expect("copy -wal");
        drop(stock);
        assert!(
            std::fs::metadata(&candidate_wal).expect("wal").len() > 32,
            "the copied -wal holds frames"
        );
        let before = companions(&candidate);
        let owner = Connection::open(dir.path().join("owner.db").to_string_lossy().into_owned())
            .await
            .expect("owner open");
        let refused = owner
            .inspect_self_contained_image_receipt(&candidate)
            .await
            .err();
        assert!(
            matches!(&refused, Some(FrankenError::DatabaseCorrupt { detail }) if detail.contains("not self-contained")),
            "a candidate whose -wal holds frames must be refused, got {refused:?}"
        );
        assert_eq!(
            companions(&candidate),
            before,
            "the refusal creates nothing"
        );
        owner.close().await.expect("owner close");
    });
}
