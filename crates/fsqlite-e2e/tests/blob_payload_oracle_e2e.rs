//! bd-1ugxp — BLOB and large payload concurrent writer oracle parity
//! e2e tests.
//!
//! Exercises FrankenSQLite's handling of:
//!   - BLOB storage and retrieval parity
//!   - Large text payloads (overflow pages)
//!   - Mixed TEXT/BLOB/INTEGER type storage
//!   - Concurrent writes with large payloads
//!   - ZEROBLOB handling
//!   - BLOB comparison and ordering
#![recursion_limit = "512"]

use std::sync::{Arc, Barrier};
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

async fn frank_rows(conn: &fsqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank query `{sql}`: {e}"));
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
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
                })
                .collect()
        })
        .collect()
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

fn rusqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("csql prepare `{sql}`: {e}"));
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
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
            });
        }
        Ok(out)
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

// ── Test 1: BLOB storage and retrieval parity ────────────────────────

#[test]
fn blob_storage_retrieval_parity() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        f.execute("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB);")
            .await
            .unwrap();
        r.execute_batch("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB);")
            .unwrap();

        let blobs = [
            "X'DEADBEEF'",
            "X'00'",
            "X''",
            "X'0102030405060708090A0B0C0D0E0F'",
            "X'FF'",
            "X'CAFEBABE'",
        ];

        for (i, blob) in blobs.iter().enumerate() {
            let sql = format!("INSERT INTO blobs VALUES ({i}, {blob});");
            f.execute(&sql).await.unwrap();
            r.execute_batch(&sql).unwrap();
        }

        let fdata = frank_rows(&f, "SELECT id, data FROM blobs ORDER BY id").await;
        let rdata = rusqlite_rows(&r, "SELECT id, data FROM blobs ORDER BY id");
        assert_eq!(fdata, rdata, "blob storage/retrieval mismatch");
    });
}

// ── Test 2: Large text payloads (overflow pages) ─────────────────────

#[test]
fn large_text_payload_overflow_parity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("large_text.db");
        let r_path = dir.path().join("large_text_r.db");
        let f_str = f_path.to_str().unwrap();
        let r_str = r_path.to_str().unwrap();

        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE big_text (id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .unwrap();
            let r = rusqlite::Connection::open(r_str).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch("CREATE TABLE big_text (id INTEGER PRIMARY KEY, payload TEXT);")
                .unwrap();

            // Insert rows with increasing payload sizes
            for i in 0..10 {
                let size = (i + 1) * 500;
                let payload: String = (0..size)
                    .map(|j| char::from(b'A' + (j % 26) as u8))
                    .collect();
                let sql = format!("INSERT INTO big_text VALUES ({i}, '{payload}');");
                f.execute(&sql).await.unwrap();
                r.execute_batch(&sql).unwrap();
            }
        }

        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let r = rusqlite::Connection::open(r_str).unwrap();

        let fcount = frank_scalar(&f, "SELECT COUNT(*) FROM big_text").await;
        let rcount = csql_scalar(&r, "SELECT COUNT(*) FROM big_text");
        assert_eq!(fcount, rcount);
        assert_eq!(fcount, "10");

        // Verify payload lengths match
        let flens = frank_rows(&f, "SELECT id, LENGTH(payload) FROM big_text ORDER BY id").await;
        let rlens = rusqlite_rows(&r, "SELECT id, LENGTH(payload) FROM big_text ORDER BY id");
        assert_eq!(flens, rlens, "payload length mismatch");

        // Verify actual content of largest payload
        let fmax = frank_scalar(&f, "SELECT payload FROM big_text WHERE id = 9").await;
        let rmax = csql_scalar(&r, "SELECT payload FROM big_text WHERE id = 9");
        assert_eq!(fmax, rmax, "largest payload content mismatch");
        assert_eq!(fmax.len(), 5000, "largest payload should be 5000 chars");
    });
}

// ── Test 3: Mixed types in same table ────────────────────────────────

#[test]
fn mixed_types_same_table_parity() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        f.execute("CREATE TABLE mixed (id INTEGER PRIMARY KEY, val);")
            .await
            .unwrap();
        r.execute_batch("CREATE TABLE mixed (id INTEGER PRIMARY KEY, val);")
            .unwrap();

        let inserts = [
            "INSERT INTO mixed VALUES (1, 42);",
            "INSERT INTO mixed VALUES (2, 'hello');",
            "INSERT INTO mixed VALUES (3, 3.14);",
            "INSERT INTO mixed VALUES (4, X'BEEF');",
            "INSERT INTO mixed VALUES (5, NULL);",
            "INSERT INTO mixed VALUES (6, 0);",
            "INSERT INTO mixed VALUES (7, '');",
            "INSERT INTO mixed VALUES (8, 9999999999999);",
        ];

        for s in &inserts {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }

        let fdata = frank_rows(&f, "SELECT id, val, typeof(val) FROM mixed ORDER BY id").await;
        let rdata = rusqlite_rows(&r, "SELECT id, val, typeof(val) FROM mixed ORDER BY id");
        assert_eq!(fdata, rdata, "mixed type storage mismatch");
    });
}

// ── Test 4: Concurrent writes with large payloads ────────────────────

#[test]
fn concurrent_writes_large_payloads() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE big_conc (id INTEGER PRIMARY KEY, tid INTEGER, payload TEXT);")
                .await
                .unwrap();
        }

        let n_threads = 4usize;
        let rows_per_thread = 10usize;
        let payload_size = 2000usize;
        let barrier = Arc::new(Barrier::new(n_threads));

        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let p = f_path.clone();
                let bar = barrier.clone();
                thread::spawn(move || {
                    asupersync::test_utils::run_test(|| async {
                        let conn = fsqlite::Connection::open(&p).await.unwrap();
                        conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                        bar.wait();
                        let payload: String = (0..payload_size)
                            .map(|j| char::from(b'a' + ((tid + j) % 26) as u8))
                            .collect();
                        for i in 0..rows_per_thread {
                            let pk = tid * rows_per_thread + i;
                            let mut attempts = 0u32;
                            loop {
                                if conn.execute("BEGIN CONCURRENT").await.is_err() {
                                    attempts += 1;
                                    assert!(attempts < 200);
                                    thread::sleep(std::time::Duration::from_millis(1));
                                    continue;
                                }
                                let sql = format!(
                                    "INSERT INTO big_conc VALUES ({pk}, {tid}, '{payload}');"
                                );
                                if conn.execute(&sql).await.is_err() {
                                    drop(conn.execute("ROLLBACK").await);
                                    attempts += 1;
                                    assert!(attempts < 200);
                                    thread::sleep(std::time::Duration::from_millis(1));
                                    continue;
                                }
                                match conn.execute("COMMIT").await {
                                    Ok(_) => break,
                                    Err(_) => {
                                        drop(conn.execute("ROLLBACK").await);
                                        attempts += 1;
                                        assert!(attempts < 200);
                                        thread::sleep(std::time::Duration::from_millis(1));
                                    }
                                }
                            }
                        }
                    });
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let count = frank_scalar(&f, "SELECT COUNT(*) FROM big_conc").await;
        assert_eq!(
            count,
            (n_threads * rows_per_thread).to_string(),
            "all rows should be present"
        );

        // Verify payload integrity
        for tid in 0..n_threads {
            let expected_payload: String = (0..payload_size)
                .map(|j| char::from(b'a' + ((tid + j) % 26) as u8))
                .collect();
            let pk = tid * rows_per_thread;
            let actual =
                frank_scalar(&f, &format!("SELECT payload FROM big_conc WHERE id = {pk}")).await;
            assert_eq!(
                actual, expected_payload,
                "payload corrupted for thread {tid}"
            );
        }
    });
}

// ── Test 5: ZEROBLOB parity ──────────────────────────────────────────

#[test]
fn zeroblob_parity() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        f.execute("CREATE TABLE zb (id INTEGER PRIMARY KEY, data BLOB);")
            .await
            .unwrap();
        r.execute_batch("CREATE TABLE zb (id INTEGER PRIMARY KEY, data BLOB);")
            .unwrap();

        let sizes = [0, 1, 10, 100, 1000];
        for (i, size) in sizes.iter().enumerate() {
            let sql = format!("INSERT INTO zb VALUES ({i}, ZEROBLOB({size}));");
            f.execute(&sql).await.unwrap();
            r.execute_batch(&sql).unwrap();
        }

        let flens = frank_rows(&f, "SELECT id, LENGTH(data) FROM zb ORDER BY id").await;
        let rlens = rusqlite_rows(&r, "SELECT id, LENGTH(data) FROM zb ORDER BY id");
        assert_eq!(flens, rlens, "zeroblob length mismatch");

        // ZEROBLOB content should be all zeros
        let fhex = frank_scalar(&f, "SELECT HEX(data) FROM zb WHERE id = 2").await;
        let rhex = csql_scalar(&r, "SELECT HEX(data) FROM zb WHERE id = 2");
        assert_eq!(fhex, rhex, "zeroblob(10) hex mismatch");
        assert_eq!(fhex, "00000000000000000000", "10 zero bytes");
    });
}

// ── Test 6: Large batch with varying payload sizes ───────────────────

#[test]
fn large_batch_varying_payload_sizes() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("vary_payload.db");
        let r_path = dir.path().join("vary_payload_r.db");
        let f_str = f_path.to_str().unwrap();
        let r_str = r_path.to_str().unwrap();

        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE vary (id INTEGER PRIMARY KEY, size_cat TEXT, payload TEXT);")
                .await
                .unwrap();
            let r = rusqlite::Connection::open(r_str).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch(
                "CREATE TABLE vary (id INTEGER PRIMARY KEY, size_cat TEXT, payload TEXT);",
            )
            .unwrap();

            let sizes = [10, 100, 500, 1000, 2000, 4000];
            for (i, &size) in sizes.iter().enumerate() {
                let payload: String = (0..size)
                    .map(|j| char::from(b'A' + (j % 26) as u8))
                    .collect();
                let cat = if size <= 100 {
                    "small"
                } else if size <= 1000 {
                    "medium"
                } else {
                    "large"
                };
                let sql = format!("INSERT INTO vary VALUES ({i}, '{cat}', '{payload}');");
                f.execute(&sql).await.unwrap();
                r.execute_batch(&sql).unwrap();
            }
        }

        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let r = rusqlite::Connection::open(r_str).unwrap();

        let fdata = frank_rows(
            &f,
            "SELECT id, size_cat, LENGTH(payload) FROM vary ORDER BY id",
        )
        .await;
        let rdata = rusqlite_rows(
            &r,
            "SELECT id, size_cat, LENGTH(payload) FROM vary ORDER BY id",
        );
        assert_eq!(fdata, rdata, "varying payload data mismatch");
    });
}

// ── Test 7: BLOB comparison and typeof parity ────────────────────────

#[test]
fn blob_comparison_typeof_parity() {
    asupersync::test_utils::run_test(|| async {
        let f = fsqlite::Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        f.execute("CREATE TABLE bcmp (id INTEGER PRIMARY KEY, data BLOB);")
            .await
            .unwrap();
        r.execute_batch("CREATE TABLE bcmp (id INTEGER PRIMARY KEY, data BLOB);")
            .unwrap();

        let inserts = [
            "INSERT INTO bcmp VALUES (1, X'0102');",
            "INSERT INTO bcmp VALUES (2, X'0103');",
            "INSERT INTO bcmp VALUES (3, X'01');",
            "INSERT INTO bcmp VALUES (4, X'010200');",
            "INSERT INTO bcmp VALUES (5, X'FF');",
        ];

        for s in &inserts {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }

        // Ordering
        let forder = frank_rows(&f, "SELECT id FROM bcmp ORDER BY data").await;
        let rorder = rusqlite_rows(&r, "SELECT id FROM bcmp ORDER BY data");
        assert_eq!(forder, rorder, "blob ordering mismatch");

        // Typeof
        let ftypes = frank_rows(&f, "SELECT id, typeof(data) FROM bcmp ORDER BY id").await;
        let rtypes = rusqlite_rows(&r, "SELECT id, typeof(data) FROM bcmp ORDER BY id");
        assert_eq!(ftypes, rtypes, "blob typeof mismatch");

        // Comparison
        let fgt = frank_scalar(&f, "SELECT COUNT(*) FROM bcmp WHERE data > X'0102'").await;
        let rgt = csql_scalar(&r, "SELECT COUNT(*) FROM bcmp WHERE data > X'0102'");
        assert_eq!(fgt, rgt, "blob > comparison mismatch");
    });
}
