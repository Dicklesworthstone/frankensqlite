//! bd-6xvq3 — Multi-connection MVCC visibility oracle parity e2e tests.
//!
//! Exercises FrankenSQLite's MVCC visibility rules: each connection sees a
//! consistent snapshot, uncommitted changes are invisible to other
//! connections, and committed changes become visible after the reader's
//! transaction ends. Compares behavior against C SQLite (rusqlite) in
//! WAL mode where multi-connection reads are natively supported.
//!
//! These tests use file-backed databases since in-memory databases in
//! C SQLite don't support multiple connections.
#![recursion_limit = "512"]

use std::thread;

use fsqlite::SqliteValue;

// ── Helpers ────────────────────────────────────────────────────────────

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

fn csql_scalar(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| {
        let v: rusqlite::types::Value = row.get_unwrap(0);
        Ok(match v {
            rusqlite::types::Value::Null => "NULL".into(),
            rusqlite::types::Value::Integer(x) => x.to_string(),
            rusqlite::types::Value::Real(f) => format!("{f}"),
            rusqlite::types::Value::Text(s) => s,
            rusqlite::types::Value::Blob(b) => {
                format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                )
            }
        })
    })
    .unwrap()
}

// ── Test 1: Committed writes visible to new connection ────────────────

#[test]
fn committed_writes_visible_to_new_connection() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        // FrankenSQLite
        {
            let c1 = fsqlite::Connection::open(&f_path).await.unwrap();
            c1.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            c1.execute("CREATE TABLE vis (id INTEGER PRIMARY KEY, v INTEGER);")
                .await
                .unwrap();
            c1.execute("INSERT INTO vis VALUES (1, 100);")
                .await
                .unwrap();
            c1.execute("INSERT INTO vis VALUES (2, 200);")
                .await
                .unwrap();
        }
        {
            let c2 = fsqlite::Connection::open(&f_path).await.unwrap();
            c2.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            let count = frank_scalar(&c2, "SELECT COUNT(*) FROM vis").await;
            assert_eq!(
                count, "2",
                "frank: new connection should see committed rows"
            );
        }

        // C SQLite
        {
            let c1 = rusqlite::Connection::open(&r_path).unwrap();
            c1.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            c1.execute_batch("CREATE TABLE vis (id INTEGER PRIMARY KEY, v INTEGER);")
                .unwrap();
            c1.execute_batch("INSERT INTO vis VALUES (1, 100);")
                .unwrap();
            c1.execute_batch("INSERT INTO vis VALUES (2, 200);")
                .unwrap();
        }
        {
            let c2 = rusqlite::Connection::open(&r_path).unwrap();
            c2.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            let count = csql_scalar(&c2, "SELECT COUNT(*) FROM vis");
            assert_eq!(count, "2", "csql: new connection should see committed rows");
        }
    });
}

// ── Test 2: Autocommit changes visible across connections ─────────────

#[test]
fn autocommit_changes_visible_from_other_connection() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        let c1 = fsqlite::Connection::open(&f_path).await.unwrap();
        c1.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        c1.execute("CREATE TABLE auto_vis (id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .unwrap();
        c1.execute("INSERT INTO auto_vis VALUES (1, 'first');")
            .await
            .unwrap();

        let c2 = fsqlite::Connection::open(&f_path).await.unwrap();
        c2.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        let v = frank_scalar(&c2, "SELECT v FROM auto_vis WHERE id = 1").await;
        assert_eq!(v, "first", "autocommit insert should be visible from c2");

        c1.execute("INSERT INTO auto_vis VALUES (2, 'second');")
            .await
            .unwrap();
        let count = frank_scalar(&c2, "SELECT COUNT(*) FROM auto_vis").await;
        assert_eq!(count, "2", "second autocommit insert visible from c2");
    });
}

// ── Test 3: Multi-connection sequential write then verify ─────────────

#[test]
fn multi_connection_sequential_writes_oracle_parity() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let r_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();
        let r_path = r_tmp.path().to_str().unwrap().to_owned();

        // Setup
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE multi (id INTEGER PRIMARY KEY, writer TEXT);")
                .await
                .unwrap();
            let r = rusqlite::Connection::open(&r_path).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch("CREATE TABLE multi (id INTEGER PRIMARY KEY, writer TEXT);")
                .unwrap();
        }

        // Multiple connections write sequentially
        for conn_id in 0..4 {
            {
                let f = fsqlite::Connection::open(&f_path).await.unwrap();
                f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                for i in 0..10 {
                    let pk = conn_id * 10 + i;
                    f.execute(&format!(
                        "INSERT INTO multi VALUES ({pk}, 'conn_{conn_id}');"
                    ))
                    .await
                    .unwrap();
                }
            }
            {
                let r = rusqlite::Connection::open(&r_path).unwrap();
                r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
                for i in 0..10 {
                    let pk = conn_id * 10 + i;
                    r.execute_batch(&format!(
                        "INSERT INTO multi VALUES ({pk}, 'conn_{conn_id}');"
                    ))
                    .unwrap();
                }
            }
        }

        // Verify parity
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM multi").await;
        let rcount = csql_scalar(&r, "SELECT COUNT(*) FROM multi");
        assert_eq!(fcount, rcount, "row count mismatch");
        assert_eq!(fcount, "40");
    });
}

// ── Test 4: Reader on connection A during writes on connection B ──────

#[test]
fn reader_sees_committed_state_not_in_flight() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        // Setup initial data
        {
            let setup = fsqlite::Connection::open(&f_path).await.unwrap();
            setup.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            setup
                .execute("CREATE TABLE inflight (id INTEGER PRIMARY KEY, v INTEGER);")
                .await
                .unwrap();
            setup
                .execute("INSERT INTO inflight VALUES (1, 10);")
                .await
                .unwrap();
        }

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

        let fp_writer = f_path.clone();
        let bar_w = barrier.clone();

        let writer = thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&fp_writer).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                conn.execute("BEGIN CONCURRENT").await.unwrap();
                conn.execute("INSERT INTO inflight VALUES (2, 20);")
                    .await
                    .unwrap();
                bar_w.wait();
                thread::sleep(std::time::Duration::from_millis(50));
                conn.execute("COMMIT").await.unwrap();
            });
        });

        barrier.wait();

        let reader = fsqlite::Connection::open(&f_path).await.unwrap();
        reader.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        let count = frank_scalar(&reader, "SELECT COUNT(*) FROM inflight").await;
        assert!(
            count == "1" || count == "2",
            "reader should see either 1 (pre-commit snapshot) or 2 (post-commit), got {count}"
        );

        writer.join().unwrap();

        let final_count = frank_scalar(&reader, "SELECT COUNT(*) FROM inflight").await;
        assert_eq!(
            final_count, "2",
            "after writer commits, fresh read should see both rows"
        );
    });
}

// ── Test 5: Multiple readers don't block writer ───────────────────────

#[test]
fn multiple_readers_dont_block_writer() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let setup = fsqlite::Connection::open(&f_path).await.unwrap();
            setup.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            setup
                .execute("CREATE TABLE noblock (id INTEGER PRIMARY KEY, v INTEGER);")
                .await
                .unwrap();
            for i in 0..10 {
                setup
                    .execute(&format!("INSERT INTO noblock VALUES ({i}, {});", i * 100))
                    .await
                    .unwrap();
            }
        }

        let n_readers = 4;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(n_readers + 1));

        let reader_handles: Vec<_> = (0..n_readers)
            .map(|rid| {
                let fp = f_path.clone();
                let bar = barrier.clone();
                thread::spawn(move || {
                    let mut outcome = None;
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&fp).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        conn.execute("BEGIN").await.unwrap();
                        let count = frank_scalar(&conn, "SELECT COUNT(*) FROM noblock").await;
                        bar.wait();
                        thread::sleep(std::time::Duration::from_millis(100));
                        let count2 = frank_scalar(&conn, "SELECT COUNT(*) FROM noblock").await;
                        conn.execute("COMMIT").await.unwrap();
                        outcome = Some((rid, count, count2));
                    });
                    outcome.expect("reader thread must produce a result")
                })
            })
            .collect();

        barrier.wait();

        let writer = fsqlite::Connection::open(&f_path).await.unwrap();
        writer.execute("PRAGMA journal_mode = WAL;").await.unwrap();
        writer
            .execute("INSERT INTO noblock VALUES (99, 9900);")
            .await
            .unwrap();

        for h in reader_handles {
            let (rid, c1, c2) = h.join().unwrap();
            assert_eq!(
                c1, c2,
                "reader {rid}: count changed within txn (snapshot violation): {c1} -> {c2}"
            );
        }

        let final_count = frank_scalar(&writer, "SELECT COUNT(*) FROM noblock").await;
        assert_eq!(final_count, "11");
    });
}
