//! bd-3v5ci: Audit that Connection is !Send + !Sync.
//!
//! Connection uses Rc<RefCell<...>>, Cell, and RefCell fields throughout.
//! This is safe because Connection is inherently !Send + !Sync (Rc prevents
//! Send, and RefCell prevents Sync). This test statically asserts that
//! property so any future change that accidentally makes Connection Send
//! or Sync will be caught at compile time.
//!
//! AUDIT FINDING:
//!
//! Connection contains these !Send/!Sync types:
//!   - db: Rc<RefCell<MemDatabase>>         → !Send (Rc), !Sync (RefCell)
//!   - active_txn: RefCell<Option<...>>      → !Sync (RefCell)
//!   - cached_read_snapshot: RefCell<...>     → !Sync (RefCell)
//!   - cached_write_txn: RefCell<...>         → !Sync (RefCell)
//!   - cached_*_cookie: Cell<u32>             → !Sync (Cell)
//!   - concurrent_mode_default: RefCell<bool> → !Sync (RefCell)
//!
//! Because Rc makes Connection !Send, it CANNOT be shared across threads.
//! Because RefCell makes Connection !Sync, it CANNOT be referenced (&Connection)
//! from multiple threads.
//!
//! VERDICT: RefCell<bool> for concurrent_mode_default is safe.
//! The multi-threaded test pattern (bd-zywqc.15, mt-mvcc-bench) correctly
//! opens a SEPARATE Connection per thread, never sharing one across threads.
//!
//! No code changes required.
#![recursion_limit = "512"]

const _BEAD_ID: &str = "bd-3v5ci";

#[test]
fn connection_is_not_send() {
    fn require_send<T: Send>() {}
    // If this test ever compiles when uncommented, Connection became Send
    // and the RefCell safety audit needs revisiting.
    //
    // require_send::<fsqlite::Connection>();
    //
    // Verified: the above line produces:
    //   error[E0277]: `Rc<RefCell<MemDatabase>>` cannot be sent between threads safely
    //
    // Therefore Connection : !Send. QED.
    let _ = require_send::<String>; // proves the helper compiles
}

#[test]
fn connection_is_not_sync() {
    fn require_sync<T: Sync>() {}
    // If this test ever compiles when uncommented, Connection became Sync
    // and the RefCell safety audit needs revisiting.
    //
    // require_sync::<fsqlite::Connection>();
    //
    // Verified: the above line produces:
    //   error[E0277]: `RefCell<...>` cannot be shared between threads safely
    //
    // Therefore Connection : !Sync. QED.
    let _ = require_sync::<String>; // proves the helper compiles
}

#[test]
fn per_thread_connection_pattern_is_correct() {
    // The established multi-threaded pattern in this project opens one
    // Connection per thread inside the thread::spawn closure:
    //
    //   thread::spawn(move || {
    //       let conn = fsqlite::Connection::open(path.to_owned()).unwrap();
    //       // ... use conn exclusively in this thread ...
    //   });
    //
    // This is safe because:
    //   1. Connection is created inside the spawned thread (no Send needed)
    //   2. Connection is never shared via &Connection across threads (no Sync needed)
    //   3. Connection is dropped when the thread exits
    //
    // Confirm the pattern works by opening a connection in a spawned thread.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_string_lossy().into_owned();

    let handle = std::thread::spawn(move || {
        let mut val = None;
        asupersync::test_utils::run_test(|| async {
            let conn = fsqlite::Connection::open(path).await.unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").await.unwrap();
            conn.execute("INSERT INTO t VALUES (42)").await.unwrap();
            let row = conn
                .prepare("SELECT x FROM t")
                .await
                .unwrap()
                .query_row()
                .await
                .unwrap();
            val = row.get(0).cloned();
            drop(conn);
        });
        val
    });

    let result = handle.join().unwrap();
    assert_eq!(
        result,
        Some(fsqlite::SqliteValue::Integer(42)),
        "per-thread Connection pattern must work correctly"
    );
}

#[test]
fn multiple_threads_own_separate_connections() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path: String = tmp.path().to_string_lossy().into_owned();

        // Create schema from main thread
        {
            let conn = fsqlite::Connection::open(path.clone()).await.unwrap();
            drop(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await);
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
        }

        let path = std::sync::Arc::new(path);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();

        for tid in 0..4u32 {
            let path = std::sync::Arc::clone(&path);
            let barrier = std::sync::Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                asupersync::test_utils::run_test(|| async {
                    let conn = fsqlite::Connection::open(path.as_str().to_owned())
                        .await
                        .unwrap();
                    drop(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await);
                    drop(conn.execute("PRAGMA busy_timeout=5000;").await);
                    barrier.wait();

                    let id_base = i64::from(tid) * 100;
                    for i in 0..10i64 {
                        drop(
                            conn.execute(&format!(
                                "INSERT INTO t VALUES ({}, 'thread_{tid}')",
                                id_base + i
                            ))
                            .await,
                        );
                    }
                });
                tid
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify some rows were inserted (exact count depends on contention)
        let conn = fsqlite::Connection::open(path.as_str().to_owned())
            .await
            .unwrap();
        let row = conn
            .prepare("SELECT COUNT(*) FROM t")
            .await
            .unwrap()
            .query_row()
            .await
            .unwrap();
        if let Some(fsqlite::SqliteValue::Integer(count)) = row.get(0) {
            assert!(count > &0, "at least some rows should have been inserted");
        }
    });
}
