#![recursion_limit = "512"]

//! bd-ih8ak — a held certificate append descriptor must not trust its tail
//! past another writer's append.
//!
//! Every commit appends one record to the `-wal-cert` sidecar. A backend keeps
//! the sidecar's write descriptor between commits and skips the scan for a
//! safe append boundary when the sidecar still ends where the backend's own
//! last record ended. Two connections that alternate commits each see the
//! sidecar grow behind their back: a backend that appended at its remembered
//! offset regardless would overwrite the other connection's record, and
//! nothing a reader does would notice, because a commit without its record is
//! read as an uncertified tail commit.
//!
//! So this reads the sidecar itself: after any interleaving of commits from
//! two connections it must hold exactly one record per commit, in order, their
//! frame intervals back to back, ending at the WAL's last frame.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_wal::parallel_wal::ParallelWalDurableCertificateRecord;

const WAL_HEADER_BYTES: u64 = 32;
const WAL_FRAME_BYTES: u64 = 24 + 4096;

async fn open(path: &str) -> Connection {
    let conn = Connection::open(path).await.expect("open");
    conn.execute("PRAGMA busy_timeout=10000")
        .await
        .expect("busy_timeout");
    // No checkpoint may reset the WAL generation while the test counts
    // records: the first append of a new generation compacts the sidecar.
    conn.execute("PRAGMA wal_autocheckpoint=0")
        .await
        .expect("wal_autocheckpoint");
    conn
}

/// Every record of the sidecar, oldest first, as `(first frame, last frame)`.
fn sidecar_intervals(db_path: &str) -> Vec<(u64, u64)> {
    let bytes = std::fs::read(format!("{db_path}-wal-cert")).expect("read sidecar");
    let footer = ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE;
    let mut end = bytes.len();
    let mut intervals = Vec::new();
    while end > 0 {
        assert!(end >= footer, "sidecar ends inside a length footer at {end}");
        let length = u32::from_le_bytes(bytes[end - footer..end].try_into().expect("footer"));
        let length = usize::try_from(length).expect("record length");
        assert!(
            length >= footer && length <= end,
            "record ending at {end} declares {length} bytes"
        );
        let record = ParallelWalDurableCertificateRecord::from_bytes(&bytes[end - length..end])
            .unwrap_or_else(|error| panic!("record ending at {end} does not decode: {error}"));
        intervals.push((record.wal_frame_start, record.wal_frame_end));
        end -= length;
    }
    intervals.reverse();
    intervals
}

fn wal_frame_count(db_path: &str) -> u64 {
    let size = std::fs::metadata(format!("{db_path}-wal"))
        .expect("stat wal")
        .len();
    (size - WAL_HEADER_BYTES) / WAL_FRAME_BYTES
}

fn assert_one_record_per_commit(db_path: &str, expected_records: usize, context: &str) {
    let intervals = sidecar_intervals(db_path);
    assert_eq!(
        intervals.len(),
        expected_records,
        "{context}: one sidecar record per commit, got {intervals:?}"
    );
    for pair in intervals.windows(2) {
        assert_eq!(
            pair[1].0,
            pair[0].1 + 1,
            "{context}: records cover the WAL back to back, got {intervals:?}"
        );
    }
    assert_eq!(
        intervals.last().map(|interval| interval.1),
        Some(wal_frame_count(db_path)),
        "{context}: the newest record ends at the WAL's last frame"
    );
}

#[test]
fn alternating_writers_leave_one_certificate_record_per_commit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cert.db");
        let path = path.to_str().expect("utf-8 path").to_owned();

        let a = open(&path).await;
        a.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, who TEXT)")
            .await
            .expect("ddl");
        a.execute("INSERT INTO t(who) VALUES ('a')")
            .await
            .expect("first row");
        let b = open(&path).await;

        let mut records = sidecar_intervals(&path).len();
        assert!(records >= 1, "the setup commits left records");
        assert_one_record_per_commit(&path, records, "after setup");

        // Runs of one writer (its own tail stays valid) broken by the other
        // (the tail it remembered is no longer the sidecar's end).
        let schedule = "aabababbbaabbaaabab";
        for (step, who) in schedule.chars().enumerate() {
            let conn = if who == 'a' { &a } else { &b };
            conn.execute(&format!("INSERT INTO t(who) VALUES ('{who}')"))
                .await
                .unwrap_or_else(|error| panic!("step {step} writer {who}: {error}"));
            records += 1;
            assert_one_record_per_commit(&path, records, &format!("step {step} writer {who}"));
        }

        let rows = a.query("SELECT count(*) FROM t").await.expect("count");
        assert_eq!(
            rows[0].values(),
            &[SqliteValue::Integer(
                1 + i64::try_from(schedule.len()).expect("schedule length")
            )],
            "every commit's row is there"
        );
    });
}
