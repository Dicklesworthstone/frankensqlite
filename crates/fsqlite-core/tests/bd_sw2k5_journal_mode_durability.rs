//! bd-sw2k5 AC#1 — rollback-journal cleanup durability per `PRAGMA journal_mode`.
//!
//! The post-commit cleanup differs by mode: `delete` unlinks the journal file,
//! `truncate` empties it (keeps a zero-length file), `persist` zeroes its header
//! (keeps the file). This asserts the two clearly-distinguishable outcomes
//! (truncate keeps an empty file; delete removes it) on the file-backed rollback
//! path.
//!
//! VERIFICATION CAVEAT (bd-sw2k5): these assume `journal_mode=truncate/delete`
//! routes a write through the rollback-journal commit path (`commit_journal`),
//! producing a `<db>-journal` file. If concurrent-mode/MVCC keeps writes on the
//! WAL path even in a non-WAL journal_mode, no `-journal` is produced and the
//! rollback cleanup modes are inert — the first build must confirm which holds
//! (and, if inert, that is itself the finding to record, not a code defect).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn commit_one_write(db_str: &str, mode: &str) {
    let conn = Connection::open(db_str).await.unwrap();
    conn.execute(&format!("PRAGMA journal_mode={mode};"))
        .await
        .unwrap();
    conn.execute("CREATE TABLE t(x);").await.unwrap();
    conn.execute("INSERT INTO t VALUES(1),(2),(3);")
        .await
        .unwrap();
    conn.close().await.unwrap();
}

#[test]
fn journal_mode_truncate_keeps_empty_journal_bd_sw2k5() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("t.db");
        let journal = dir.path().join("t.db-journal");
        commit_one_write(&db.to_string_lossy(), "truncate").await;
        assert!(
            journal.exists(),
            "truncate mode keeps the rollback journal file"
        );
        assert_eq!(
            std::fs::metadata(&journal).unwrap().len(),
            0,
            "truncate mode empties the journal after commit"
        );
    });
}

#[test]
fn journal_mode_delete_removes_journal_bd_sw2k5() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("d.db");
        let journal = dir.path().join("d.db-journal");
        commit_one_write(&db.to_string_lossy(), "delete").await;
        assert!(
            !journal.exists(),
            "delete mode unlinks the rollback journal after commit"
        );
    });
}

#[test]
fn journal_mode_persist_keeps_nonempty_journal_bd_sw2k5() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("p.db");
        let journal = dir.path().join("p.db-journal");
        commit_one_write(&db.to_string_lossy(), "persist").await;
        // persist mode zeroes the journal header (making it non-hot) but keeps
        // the file, so it stays non-empty — the three-way distinction from
        // truncate (present, empty) and delete (absent).
        assert!(
            journal.exists(),
            "persist mode keeps the rollback journal file"
        );
        assert!(
            std::fs::metadata(&journal).unwrap().len() > 0,
            "persist mode retains the journal body (only the magic header is zeroed)"
        );
        // The leading JOURNAL magic must be zeroed so the retained journal is
        // not mistaken for a hot journal on the next open.
        let head = std::fs::read(&journal).unwrap();
        assert!(
            head.iter().take(8).all(|b| *b == 0),
            "persist mode must zero the journal magic so it reads as non-hot"
        );
    });
}

/// AC#3 (reopen safety): after a commit in each mode, a retained journal
/// (truncate leaves an empty file, persist a header-zeroed one) must be non-hot,
/// so a fresh connection sees the committed rows — never a spurious rollback
/// recovery. True mid-transaction power-loss simulation needs the fault-injection
/// harness (follow-up); this locks the reopen half of the AC#3 contract.
#[test]
fn journal_mode_committed_rows_survive_reopen_bd_sw2k5() {
    asupersync::test_utils::run_test(|| async {
        for mode in ["delete", "truncate", "persist"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("r.db").to_string_lossy().into_owned();
            commit_one_write(&db, mode).await;
            let conn = Connection::open(&db).await.unwrap();
            let rows = conn.query("SELECT count(*) FROM t;").await.unwrap();
            assert_eq!(
                rows[0].values()[0],
                SqliteValue::Integer(3),
                "mode={mode}: committed rows must survive a reopen (journal non-hot)"
            );
        }
    });
}

/// bd-sw2k5 regression (cross-review defect, repro commit 6d8726a1a): under
/// `journal_mode=truncate/persist` the post-commit journal is KEPT (0-byte or
/// magic-zeroed). Frank re-opens the `<db>-journal` per transaction, so the
/// SECOND write transaction on the SAME connection must REUSE that retained
/// non-hot journal instead of failing a fail-if-exists (`EXCLUSIVE`) create.
/// Two sequential autocommit INSERTs on one connection exercise exactly that;
/// `delete` (which unlinks its journal) is the control. The single-commit
/// durability tests above never issue a second write, so they miss this.
#[test]
fn journal_mode_sequential_writes_reuse_journal_bd_sw2k5() {
    asupersync::test_utils::run_test(|| async {
        for mode in ["truncate", "persist", "delete"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("s.db").to_string_lossy().into_owned();
            let conn = Connection::open(&db).await.unwrap();
            conn.execute(&format!("PRAGMA journal_mode={mode};"))
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(x);").await.unwrap();
            // First write transaction: creates the journal, then cleans it per
            // mode (truncate=empty, persist=magic-zeroed, delete=unlinked).
            conn.execute("INSERT INTO t VALUES(1);")
                .await
                .unwrap_or_else(|e| panic!("mode={mode}: first INSERT must commit: {e}"));
            // Second write transaction on the SAME connection: must reuse (or
            // recreate) the journal without a fail-if-exists collision on the
            // retained non-hot leftover.
            conn.execute("INSERT INTO t VALUES(2);")
                .await
                .unwrap_or_else(|e| {
                    panic!("mode={mode}: second INSERT must commit (journal reuse): {e}")
                });
            let rows = conn.query("SELECT count(*) FROM t;").await.unwrap();
            assert_eq!(
                rows[0].values()[0],
                SqliteValue::Integer(2),
                "mode={mode}: both sequential writes on one connection must be durable"
            );
            conn.close().await.unwrap();
        }
    });
}
