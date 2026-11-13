#![recursion_limit = "512"]

//! bd-qyekq: a `synchronous=FULL` commit fsyncs only the WAL, as stock SQLite
//! does. The `-wal-cert` certificate record that precedes each commit marker
//! is no longer fsynced on its own, so a power loss may keep the durable WAL
//! commit while dropping any suffix of the sidecar (the state
//! `synchronous=NORMAL` could already leave behind).
//!
//! These crash images copy the live files of a database after FULL commits
//! (the WAL, as fsynced, plus the main file), then cut the sidecar the way
//! an unsynced suffix can be lost: removed, emptied, truncated to an earlier
//! record boundary, or truncated mid-record. Every acknowledged row must
//! survive the reopen, the database must keep accepting writes, and stock
//! SQLite must read the same rows from the result.

use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const ROWS: i64 = 40;

fn integer(conn_rows: &[fsqlite_core::connection::Row]) -> i64 {
    match conn_rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected an integer, got {other:?}"),
    }
}

async fn scalar(conn: &Connection, sql: &str) -> i64 {
    integer(&conn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")))
}

/// Record end offsets of the certificate sidecar, oldest first. Each record
/// ends in a little-endian u32 footer holding its own length.
fn certificate_record_ends(bytes: &[u8]) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut end = bytes.len();
    while end > 0 {
        assert!(end >= 4, "sidecar ends inside a length footer");
        let footer: [u8; 4] = bytes[end - 4..end].try_into().expect("4-byte footer");
        let len = usize::try_from(u32::from_le_bytes(footer)).expect("record length fits usize");
        assert!(len > 4 && len <= end, "invalid record length {len} ending at {end}");
        ends.push(end);
        end -= len;
    }
    ends.reverse();
    ends
}

#[derive(Debug, Clone, Copy)]
enum SidecarLoss {
    Intact,
    Removed,
    Emptied,
    /// Keep the first half of the records.
    RecordPrefix,
    /// Keep the first half of the records plus a torn piece of the next.
    TornRecord,
}

fn crash_image(source_dir: &Path, image_dir: &Path, loss: SidecarLoss) -> PathBuf {
    std::fs::create_dir_all(image_dir).expect("create crash image dir");
    for suffix in ["", "-wal", "-wal-fec", "-wal-cert-head"] {
        let from = source_dir.join(format!("live.db{suffix}"));
        if from.exists() {
            std::fs::copy(&from, image_dir.join(format!("live.db{suffix}")))
                .expect("copy live file into crash image");
        }
    }
    let sidecar = std::fs::read(source_dir.join("live.db-wal-cert")).expect("read live sidecar");
    let ends = certificate_record_ends(&sidecar);
    assert!(
        i64::try_from(ends.len()).expect("record count fits i64") > ROWS,
        "every autocommit INSERT appends one certificate record (got {})",
        ends.len()
    );
    let half = ends[ends.len() / 2 - 1];
    let kept: Option<&[u8]> = match loss {
        SidecarLoss::Intact => Some(&sidecar),
        SidecarLoss::Removed => None,
        SidecarLoss::Emptied => Some(&[]),
        SidecarLoss::RecordPrefix => Some(&sidecar[..half]),
        SidecarLoss::TornRecord => Some(&sidecar[..half + 7]),
    };
    if let Some(kept) = kept {
        std::fs::write(image_dir.join("live.db-wal-cert"), kept).expect("write cut sidecar");
    }
    image_dir.join("live.db")
}

#[test]
fn full_commits_survive_every_loss_of_the_unsynced_certificate_suffix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    std::fs::create_dir_all(&live).expect("live dir");
    let db = live.join("live.db");

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db.to_str().expect("utf-8 path"))
            .await
            .expect("open live database");
        conn.query("PRAGMA journal_mode = WAL;").await.expect("WAL");
        conn.execute("PRAGMA synchronous = FULL;").await.expect("FULL");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
            .await
            .expect("create table");
        for i in 1..=ROWS {
            conn.execute(&format!("INSERT INTO t(v) VALUES ('row-{i}');"))
                .await
                .expect("autocommit insert");
        }
        assert_eq!(scalar(&conn, "SELECT count(*) FROM t;").await, ROWS);

        for loss in [
            SidecarLoss::Intact,
            SidecarLoss::Removed,
            SidecarLoss::Emptied,
            SidecarLoss::RecordPrefix,
            SidecarLoss::TornRecord,
        ] {
            let image = crash_image(&live, &dir.path().join(format!("{loss:?}")), loss);
            let reopened = Connection::open(image.to_str().expect("utf-8 path"))
                .await
                .unwrap_or_else(|e| panic!("{loss:?}: reopen crash image: {e}"));
            assert_eq!(
                scalar(&reopened, "SELECT count(*) FROM t;").await,
                ROWS,
                "{loss:?}: every FULL-acknowledged row must survive"
            );
            assert_eq!(scalar(&reopened, "SELECT max(id) FROM t;").await, ROWS, "{loss:?}");
            let check = reopened
                .query("PRAGMA integrity_check;")
                .await
                .unwrap_or_else(|e| panic!("{loss:?}: integrity_check: {e}"));
            assert!(
                matches!(&check[0].values()[0], SqliteValue::Text(s) if s.as_ref() == "ok"),
                "{loss:?}: integrity_check returned {:?}",
                check[0].values()
            );
            reopened
                .execute("INSERT INTO t(v) VALUES ('after-reopen');")
                .await
                .unwrap_or_else(|e| panic!("{loss:?}: write after reopen: {e}"));
            assert_eq!(
                scalar(&reopened, "SELECT count(*) FROM t;").await,
                ROWS + 1,
                "{loss:?}"
            );
            reopened
                .close()
                .await
                .unwrap_or_else(|e| panic!("{loss:?}: close: {e}"));

            let stock = rusqlite::Connection::open(&image).expect("stock open");
            let count: i64 = stock
                .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
                .expect("stock count");
            assert_eq!(count, ROWS + 1, "{loss:?}: stock row count");
            let integrity: String = stock
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                .expect("stock integrity_check");
            assert_eq!(integrity, "ok", "{loss:?}: stock integrity_check");
        }
        conn.close().await.expect("close live connection");
    });
}
