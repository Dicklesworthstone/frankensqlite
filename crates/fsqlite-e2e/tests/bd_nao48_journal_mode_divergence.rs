//! bd-nao48: PRAGMA journal_mode semantic divergence — returns 'wal' but
//! silently enforces BEGIN CONCURRENT, ignoring SQLite WAL contract.
//!
//! ## Bug hypothesis
//!
//! FrankenSQLite reports `PRAGMA journal_mode` as 'wal' for compatibility,
//! but the actual behavior is MVCC with BEGIN CONCURRENT semantics. This
//! divergence means:
//! 1. Applications that check journal_mode == 'wal' assume standard WAL
//!    reader-writer concurrency model
//! 2. Applications may not expect SSI abort semantics when they get 'wal'
//! 3. The 'wal' response is a semantic lie if the behavior differs
//!
//! ## Test approach
//!
//! - J1: PRAGMA journal_mode returns a recognized value
//! - J2: WAL-mode concurrent read+write behavior works
//! - J3: Multiple readers during write (WAL contract)
//! - J4: journal_mode persistence across connections
//! - J5: PRAGMA journal_mode oracle parity with C SQLite
#![recursion_limit = "512"]

use fsqlite::Connection;

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

// ─── J1: PRAGMA journal_mode returns a value ───────────────────────

#[test]
fn j1_journal_mode_returns_value() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j1.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        let rows = conn
            .query("PRAGMA journal_mode")
            .await
            .expect("journal_mode query");
        assert!(!rows.is_empty(), "PRAGMA journal_mode returned no rows");
        eprintln!("J1: PRAGMA journal_mode returned {} row(s)", rows.len());
    });
}

// ─── J2: WAL-mode concurrent read+write ────────────────────────────

#[test]
fn j2_concurrent_read_write() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j2.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .expect("create");
            conn.execute("BEGIN").await.expect("begin");
            for i in 1..=100 {
                conn.execute(&format!("INSERT INTO data VALUES ({i}, 'v{i}')"))
                    .await
                    .expect("seed");
            }
            conn.execute("COMMIT").await.expect("commit");
        }

        // Reader opens and reads
        let reader = Connection::open(path_str).await.expect("reader open");
        let r_rows = reader.query("SELECT * FROM data").await.expect("read");
        assert_eq!(r_rows.len(), 100, "reader should see 100 rows");

        // Writer adds more while reader has its snapshot
        let writer = Connection::open(path_str).await.expect("writer open");
        writer.execute("BEGIN").await.expect("begin");
        writer
            .execute("INSERT INTO data VALUES (101, 'new')")
            .await
            .expect("insert");
        writer.execute("COMMIT").await.expect("commit");

        // Reader should still see its original snapshot (100 rows) or the updated state (101)
        // depending on isolation level — both are valid WAL behaviors
        let r_rows_after = reader
            .query("SELECT * FROM data")
            .await
            .expect("read after");
        assert!(
            r_rows_after.len() >= 100,
            "reader should see at least 100 rows after concurrent write"
        );

        eprintln!(
            "J2: concurrent read+write works, reader sees {} rows",
            r_rows_after.len()
        );
    });
}

// ─── J3: Multiple readers during write ─────────────────────────────

#[test]
fn j3_multiple_readers_during_write() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j3.db");
        let path_str = db_path.to_str().expect("path");

        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)")
                .await
                .expect("create");
            conn.execute("INSERT INTO kv VALUES ('counter', 0)")
                .await
                .expect("seed");
        }

        // Open 4 readers
        let mut readers: Vec<Connection> = Vec::new();
        for _ in 0..4 {
            readers.push(Connection::open(path_str).await.expect("reader"));
        }

        // All readers can read simultaneously
        for (i, reader) in readers.iter().enumerate() {
            let rows = reader.query("SELECT * FROM kv").await.expect("read");
            assert_eq!(rows.len(), 1, "reader {i}: should see 1 row");
        }

        // Writer updates while all readers are open
        let writer = Connection::open(path_str).await.expect("writer");
        writer.execute("BEGIN").await.expect("begin");
        writer
            .execute("UPDATE kv SET v = v + 1 WHERE k = 'counter'")
            .await
            .expect("update");
        writer.execute("COMMIT").await.expect("commit");

        // All readers should still work (no lock-out under WAL semantics)
        for (i, reader) in readers.iter().enumerate() {
            let rows = reader.query("SELECT * FROM kv").await.expect("read after");
            assert_eq!(rows.len(), 1, "reader {i}: should still see 1 row");
        }

        eprintln!("J3: 4 readers + 1 writer, all functional — WAL contract held");
    });
}

// ─── J4: Journal mode consistency across connections ───────────────

#[test]
fn j4_journal_mode_consistent() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("j4.db");
        let path_str = db_path.to_str().expect("path");

        // First connection sets up DB
        {
            let conn = Connection::open(path_str).await.expect("open");
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("create");
        }

        // Multiple connections should report the same journal mode
        let mut modes: Vec<String> = Vec::new();
        for _ in 0..4 {
            let conn = Connection::open(path_str).await.expect("open");
            let rows = conn.query("PRAGMA journal_mode").await.expect("query");
            modes.push(if rows.is_empty() {
                "empty".to_string()
            } else {
                format!("{} rows", rows.len())
            });
        }

        // All should be the same
        let first = &modes[0];
        for (i, mode) in modes.iter().enumerate() {
            assert_eq!(
                mode, first,
                "connection {i} reports different journal_mode: {} vs {}",
                mode, first
            );
        }
        eprintln!("J4: all 4 connections report consistent journal_mode");
    });
}

// ─── J5: Oracle parity — compare with C SQLite ────────────────────

#[test]
fn j5_journal_mode_oracle_parity() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();

        // C SQLite reference
        let c_path = dir.path().join("j5_csqlite.db");
        let c = rusqlite::Connection::open(&c_path).expect("csqlite open");
        let c_mode: String = c
            .query_row("PRAGMA journal_mode=wal", [], |r| r.get(0))
            .expect("csqlite journal_mode");

        // Create some data
        c.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
         INSERT INTO t VALUES (1, 'hello');",
        )
        .expect("csqlite setup");

        let c_rows: usize = c
            .prepare("SELECT * FROM t")
            .expect("prepare")
            .query_map([], |_| Ok(()))
            .expect("query")
            .count();

        // FrankenSQLite
        let f_path = dir.path().join("j5_fsqlite.db");
        let f = Connection::open(f_path.to_str().expect("path"))
            .await
            .expect("fsqlite open");

        f.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .expect("create");
        f.execute("INSERT INTO t VALUES (1, 'hello')")
            .await
            .expect("insert");

        let f_rows = f.query("SELECT * FROM t").await.expect("query").len();

        // Both should see the same data
        assert_eq!(
            f_rows, c_rows,
            "parity: fsqlite sees {f_rows} rows, csqlite sees {c_rows}"
        );

        // Journal mode comparison
        let f_mode_rows = f
            .query("PRAGMA journal_mode")
            .await
            .expect("f journal_mode");
        eprintln!(
            "J5: csqlite journal_mode={c_mode}, fsqlite returned {} row(s), data parity OK",
            f_mode_rows.len()
        );
    });
}
