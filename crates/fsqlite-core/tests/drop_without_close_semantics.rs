// Integration tests are their own crate root and do not inherit the lib's
// `#![recursion_limit]`; the async engine futures nest deeply enough that the
// trait solver overflows the default 128. Match the 512 used elsewhere.
#![recursion_limit = "512"]

//! bd-t80zm: characterize `Connection::drop` semantics for a connection dropped
//! WITHOUT an awaited `close()`.
//!
//! Because pager/VFS I/O is async and `Drop::drop` cannot await (and this crate
//! never builds its own runtime — the `Cx` flows down from the consumer), Drop
//! cannot roll back the ordinary write transaction on disk or run a WAL
//! checkpoint. What it CAN — and does — do synchronously (see
//! `impl Drop for Connection`, connection.rs, and bd-b4mwn commit 710933145) is
//! release the SHARED resources that would otherwise wedge sibling connections:
//! it aborts a live BEGIN CONCURRENT session (releasing its page locks in the
//! shared per-path lock table, freeing the registry slot, and advancing the GC
//! horizon) and cancels the connection's region tasks. Durable rollback of an
//! ordinary transaction's uncommitted bytes stays deferred to WAL recovery,
//! which is not data loss: only committed bytes are durable in the WAL.
//!
//! These are BLACK-BOX tests (the numeric registry/lock-table accessors are
//! private): each drops a connection that holds an open, uncommitted write and
//! then asserts a fresh sibling connection on the same file is NOT wedged and
//! that committed data survived. A short `busy_timeout` on the sibling makes a
//! lock/session leak (the pre-bd-b4mwn regression) fail fast instead of
//! blocking, and clean committed rows prove the drop did not corrupt the file.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Read a single INTEGER cell from the first row of `sql`.
async fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect("query ok");
    match &rows.first().expect("one row").values()[0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("expected integer, got {other:?} for `{sql}`"),
    }
}

/// An ordinary (non-CONCURRENT) write transaction abandoned by dropping the
/// connection must not wedge the single-writer path for a sibling, and the
/// connection's previously-committed rows must remain durable.
#[test]
fn drop_without_close_frees_ordinary_writer_for_siblings() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("ordinary.db").to_string_lossy().into_owned();

        {
            let a = Connection::open(&db).await.expect("open A");
            a.execute("PRAGMA journal_mode=WAL;").await.expect("wal");
            a.execute("PRAGMA synchronous=NORMAL;").await.expect("sync");
            a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
                .await
                .expect("create");
            // Durable committed row (autocommit).
            a.execute("INSERT INTO t (id, v) VALUES (1, 10);")
                .await
                .expect("commit row");
            // Open, UNCOMMITTED ordinary write transaction — the "dirty" state.
            a.execute("BEGIN;").await.expect("begin");
            a.execute("INSERT INTO t (id, v) VALUES (2, 20);")
                .await
                .expect("dirty insert");
            // Drop A here WITHOUT awaiting close(): the open txn is abandoned.
            drop(a);
        }

        // A sibling opens the same file with a short busy_timeout: if A's
        // abandoned writer had leaked the write lock, this write would park and
        // return Busy within the timeout rather than committing.
        let b = Connection::open(&db).await.expect("open B");
        b.execute("PRAGMA busy_timeout=750;").await.expect("busy");
        b.execute("PRAGMA journal_mode=WAL;").await.expect("wal B");
        let wrote = b.execute("INSERT INTO t (id, v) VALUES (3, 30);").await;
        assert!(
            wrote.is_ok(),
            "sibling write after an abandoned ordinary txn drop must not wedge: {wrote:?}"
        );

        // A's committed row survived the drop; A's uncommitted row (id=2) was
        // never made durable, so recovery left it out.
        assert_eq!(scalar_i64(&b, "SELECT v FROM t WHERE id = 1;").await, 10);
        assert_eq!(scalar_i64(&b, "SELECT COUNT(*) FROM t WHERE id = 2;").await, 0);
        assert_eq!(scalar_i64(&b, "SELECT v FROM t WHERE id = 3;").await, 30);
    });
}

/// A live BEGIN CONCURRENT session abandoned by dropping the connection must
/// release its shared page locks and registry slot synchronously (bd-b4mwn), so
/// a sibling's conflicting concurrent write on the same page commits promptly
/// instead of parking on `page_lock_busy` for the full busy_timeout.
#[test]
fn drop_without_close_frees_concurrent_session_page_locks() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("concurrent.db").to_string_lossy().into_owned();

        {
            let a = Connection::open(&db).await.expect("open A");
            a.execute("PRAGMA journal_mode=WAL;").await.expect("wal");
            a.execute("PRAGMA synchronous=NORMAL;").await.expect("sync");
            a.execute("PRAGMA fsqlite.concurrent_mode=ON;")
                .await
                .expect("concurrent mode");
            a.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v INTEGER);")
                .await
                .expect("create");
            a.execute("INSERT INTO kv (k, v) VALUES (1, 1);")
                .await
                .expect("seed committed");
            // Open, UNCOMMITTED concurrent write holding a page lock on kv.
            a.execute_batch("BEGIN CONCURRENT; UPDATE kv SET v = 100 WHERE k = 1;")
                .await
                .expect("open concurrent write");
            // Drop A here WITHOUT close(): the concurrent session is abandoned.
            drop(a);
        }

        let b = Connection::open(&db).await.expect("open B");
        b.execute("PRAGMA busy_timeout=750;").await.expect("busy");
        b.execute("PRAGMA journal_mode=WAL;").await.expect("wal B");
        b.execute("PRAGMA fsqlite.concurrent_mode=ON;")
            .await
            .expect("concurrent mode B");
        // Conflicting concurrent write on the same page A had locked: it must
        // commit (A's page lock was released on drop), not time out Busy.
        let committed = b
            .execute_batch("BEGIN CONCURRENT; UPDATE kv SET v = 200 WHERE k = 1; COMMIT;")
            .await;
        assert!(
            committed.is_ok(),
            "sibling concurrent write after an abandoned session drop must not wedge on \
             a leaked page lock: {committed:?}"
        );
        assert_eq!(scalar_i64(&b, "SELECT v FROM kv WHERE k = 1;").await, 200);
    });
}
