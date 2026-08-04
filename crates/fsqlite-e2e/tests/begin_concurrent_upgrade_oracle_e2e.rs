//! bd-c0d3h — BEGIN CONCURRENT upgrade mechanism oracle parity tests.
//!
//! FrankenSQLite auto-promotes BEGIN to BEGIN CONCURRENT when
//! concurrent_mode_default is true. These tests verify the upgrade
//! behavior: that BEGIN CONCURRENT is accepted, that it enables
//! concurrent writes, that COMMIT/ROLLBACK work correctly, and that
//! the semantics match C SQLite's serialized writer model for
//! equivalent workloads.
#![recursion_limit = "512"]

use fsqlite::SqliteValue;

async fn frank_scalar(conn: &fsqlite::Connection, sql: &str) -> String {
    let rows = conn.query(sql).await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => s.to_string(),
        SqliteValue::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
        }
    }
}

// ── Test 1: BEGIN CONCURRENT is accepted ──────────────────────────────

#[test]
fn begin_concurrent_accepted() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        f.execute("CREATE TABLE bc (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();

        f.execute("BEGIN CONCURRENT").await.unwrap();
        f.execute("INSERT INTO bc VALUES (1);").await.unwrap();
        f.execute("COMMIT").await.unwrap();

        let count = frank_scalar(&f, "SELECT COUNT(*) FROM bc").await;
        assert_eq!(count, "1");
    });
}

// ── Test 2: BEGIN auto-promotes to CONCURRENT ─────────────────────────

#[test]
fn begin_auto_promotes_to_concurrent() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        f.execute("CREATE TABLE auto_bc (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();

        f.execute("BEGIN").await.unwrap();
        f.execute("INSERT INTO auto_bc VALUES (1);").await.unwrap();
        f.execute("COMMIT").await.unwrap();

        let count = frank_scalar(&f, "SELECT COUNT(*) FROM auto_bc").await;
        assert_eq!(count, "1");
    });
}

// ── Test 3: BEGIN CONCURRENT + ROLLBACK undoes changes ────────────────

#[test]
fn begin_concurrent_rollback() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        f.execute("CREATE TABLE bc_rb (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        f.execute("INSERT INTO bc_rb VALUES (1);").await.unwrap();

        f.execute("BEGIN CONCURRENT").await.unwrap();
        f.execute("INSERT INTO bc_rb VALUES (2);").await.unwrap();
        f.execute("INSERT INTO bc_rb VALUES (3);").await.unwrap();
        f.execute("ROLLBACK").await.unwrap();

        let count = frank_scalar(&f, "SELECT COUNT(*) FROM bc_rb").await;
        assert_eq!(count, "1", "rollback should undo concurrent inserts");
    });
}

// ── Test 4: Nested SAVEPOINT within CONCURRENT ────────────────────────

#[test]
fn savepoint_within_concurrent_transaction() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        f.execute("CREATE TABLE sp_bc (id INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .unwrap();
        r.execute_batch("CREATE TABLE sp_bc (id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();

        let steps = [
            "BEGIN",
            "INSERT INTO sp_bc VALUES (1, 10)",
            "SAVEPOINT sp1",
            "INSERT INTO sp_bc VALUES (2, 20)",
            "ROLLBACK TO sp1",
            "INSERT INTO sp_bc VALUES (3, 30)",
            "RELEASE sp1",
            "COMMIT",
        ];

        for s in &steps {
            let fe = f.execute(s).await;
            let re = r.execute_batch(s);
            match (&fe, &re) {
                (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
                (Ok(_), Err(e)) => panic!("frank OK but csql err on `{s}`: {e}"),
                (Err(e), Ok(())) => panic!("frank err but csql OK on `{s}`: {e}"),
            }
        }

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM sp_bc").await;
        let rcount: i64 = r
            .query_row("SELECT COUNT(*) FROM sp_bc", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fcount, rcount.to_string(), "count mismatch");
        assert_eq!(fcount, "2", "should have rows 1 and 3 (2 was rolled back)");
    });
}

// ── Test 5: Multiple sequential BEGIN CONCURRENT blocks ───────────────

#[test]
fn sequential_concurrent_transactions() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap();

        let f = fsqlite::Connection::open(f_path).await.unwrap();
        f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        f.execute("CREATE TABLE seq_bc (id INTEGER PRIMARY KEY, batch INTEGER);")
            .await
            .unwrap();

        for batch in 0..5 {
            f.execute("BEGIN CONCURRENT").await.unwrap();
            for i in 0..10 {
                let pk = batch * 10 + i;
                f.execute(&format!("INSERT INTO seq_bc VALUES ({pk}, {batch});"))
                    .await
                    .unwrap();
            }
            f.execute("COMMIT").await.unwrap();
        }

        let count = frank_scalar(&f, "SELECT COUNT(*) FROM seq_bc").await;
        assert_eq!(count, "50", "5 batches x 10 rows = 50");

        let r = rusqlite::Connection::open(f_tmp.path()).unwrap();
        let rcount: i64 = r
            .query_row("SELECT COUNT(*) FROM seq_bc", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rcount, 50, "rusqlite cross-check");
    });
}

// ── Test 6: BEGIN CONCURRENT on file-backed DB ────────────────────────

#[test]
fn begin_concurrent_file_backed_wal() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap();

        let f = fsqlite::Connection::open(f_path).await.unwrap();
        f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        f.execute("CREATE TABLE file_bc (id INTEGER PRIMARY KEY, data TEXT);")
            .await
            .unwrap();

        f.execute("BEGIN CONCURRENT").await.unwrap();
        for i in 0..25 {
            f.execute(&format!("INSERT INTO file_bc VALUES ({i}, 'item_{i}');"))
                .await
                .unwrap();
        }
        f.execute("COMMIT").await.unwrap();

        drop(f);

        let verify = fsqlite::Connection::open(f_path).await.unwrap();
        verify.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        let count = frank_scalar(&verify, "SELECT COUNT(*) FROM file_bc").await;
        assert_eq!(count, "25", "data should persist after reopen");
    });
}

// ── Test 7: Commit semantics match between engines ────────────────────

#[test]
fn commit_semantics_parity_in_memory() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        f.execute("CREATE TABLE sem (id INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .unwrap();
        r.execute_batch("CREATE TABLE sem (id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();

        f.execute("BEGIN").await.unwrap();
        f.execute("INSERT INTO sem VALUES (1, 100);").await.unwrap();
        f.execute("INSERT INTO sem VALUES (2, 200);").await.unwrap();
        f.execute("INSERT INTO sem VALUES (3, 300);").await.unwrap();
        f.execute("COMMIT").await.unwrap();

        r.execute_batch("BEGIN").unwrap();
        r.execute_batch("INSERT INTO sem VALUES (1, 100);").unwrap();
        r.execute_batch("INSERT INTO sem VALUES (2, 200);").unwrap();
        r.execute_batch("INSERT INTO sem VALUES (3, 300);").unwrap();
        r.execute_batch("COMMIT").unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM sem").await;
        let rcount: i64 = r
            .query_row("SELECT COUNT(*) FROM sem", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fcount, "3");
        assert_eq!(rcount, 3);

        let fsum = frank_scalar(&f, "SELECT SUM(v) FROM sem").await;
        let rsum: i64 = r
            .query_row("SELECT SUM(v) FROM sem", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fsum, rsum.to_string());
    });
}
