#![recursion_limit = "512"]

//! bd-qgh42 repro attempt: a fresh open failing 'unable to open database file'
//! after an external DELETE grows the freelist of a VACUUM-canonicalized DB.
//!
//! The filed repro needs the archive-reconstruct file shape (FrankenConnection
//! + canonical passes). This probe approximates "canonical passes" with an
//! explicit VACUUM, then grows the freelist via a large DELETE from a separate
//! connection, then reopens. If the open-time freelist validation over-rejects
//! the legitimate state, this reopen errors.

use fsqlite_core::connection::Connection;

async fn open(path: &str) -> Result<Connection, fsqlite_error::FrankenError> {
    Connection::open(path).await
}

// Ignored: a heavy (4000-row + VACUUM) manual probe that currently PASSES — a
// plain VACUUM-then-DELETE freelist growth reopens fine, so it does NOT reproduce
// bd-qgh42. Kept as a negative-repro record narrowing the trigger to the archive
// reconstruct's specific canonical-pass output. Un-ignore + adapt once the exact
// reconstruct file shape is reproducible in-tree.
#[test]
#[ignore = "bd-qgh42 negative repro probe (heavy); plain VACUUM/DELETE reopens fine"]
fn fresh_open_after_delete_grows_freelist_of_vacuumed_db_bd_qgh42() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("qgh42.db");
        let path = path.to_str().expect("utf8 path").to_owned();

        // 1. Build a DB with enough rows to span many pages.
        {
            let c = open(&path).await.expect("open fresh");
            c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, blob TEXT)")
                .await
                .expect("create");
            for i in 0..4000 {
                c.execute(&format!(
                    "INSERT INTO t (id, blob) VALUES ({i}, '{}')",
                    "x".repeat(200)
                ))
                .await
                .unwrap_or_else(|e| panic!("insert {i}: {e:?}"));
            }
        }

        // 2. Canonicalize with VACUUM (approximates the reconstruct passes).
        {
            let c = open(&path).await.expect("reopen for vacuum");
            c.execute("VACUUM").await.expect("vacuum");
        }

        // 3. A separate connection deletes most rows, growing the freelist.
        {
            let c = open(&path).await.expect("reopen for delete");
            c.execute("DELETE FROM t WHERE id >= 100")
                .await
                .expect("delete");
        }

        // 4. A brand-new open must succeed (stock SQLite opens this fine).
        let reopened = open(&path).await;
        assert!(
            reopened.is_ok(),
            "fresh open after freelist-growing DELETE must succeed (bd-qgh42), got {:?}",
            reopened.err()
        );
        let c = reopened.unwrap();
        let rows = c.query("SELECT count(*) FROM t").await.expect("count");
        assert_eq!(rows.len(), 1, "count query returns one row");
    });
}
