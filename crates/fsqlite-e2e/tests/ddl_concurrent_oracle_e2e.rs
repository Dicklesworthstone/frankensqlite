//! bd-sj069 — DDL under concurrent load and connection lifecycle oracle
//! parity e2e tests.
//!
//! Exercises FrankenSQLite's behavior when DDL (CREATE TABLE, CREATE INDEX,
//! ALTER TABLE, DROP TABLE) happens alongside active readers/writers:
//!   - Schema changes visible to new connections after commit
//!   - CREATE INDEX on populated table parity
//!   - ALTER TABLE ADD COLUMN + concurrent readers
//!   - Connection lifecycle: open/close/reopen cycles
//!   - Rapid open/close doesn't leak or corrupt
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

// ── Test 1: CREATE TABLE visible to new connection ───────────────────

#[test]
fn create_table_visible_to_new_connection() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("ddl_vis.db");
        let r_path = dir.path().join("ddl_vis_r.db");
        let f_str = f_path.to_str().unwrap();
        let r_str = r_path.to_str().unwrap();

        // Create tables on connection 1
        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);")
                .await
                .unwrap();
            f.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, num INTEGER);")
                .await
                .unwrap();
            f.execute("INSERT INTO t1 VALUES (1, 'hello');")
                .await
                .unwrap();
            f.execute("INSERT INTO t2 VALUES (1, 42);").await.unwrap();

            let r = rusqlite::Connection::open(r_str).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);")
                .unwrap();
            r.execute_batch("CREATE TABLE t2 (id INTEGER PRIMARY KEY, num INTEGER);")
                .unwrap();
            r.execute_batch("INSERT INTO t1 VALUES (1, 'hello');")
                .unwrap();
            r.execute_batch("INSERT INTO t2 VALUES (1, 42);").unwrap();
        }

        // New connection should see both tables
        let f2 = fsqlite::Connection::open(f_str).await.unwrap();
        let r2 = rusqlite::Connection::open(r_str).unwrap();

        let ft1 = frank_scalar(&f2, "SELECT val FROM t1 WHERE id = 1").await;
        let rt1 = csql_scalar(&r2, "SELECT val FROM t1 WHERE id = 1");
        assert_eq!(ft1, rt1);
        assert_eq!(ft1, "hello");

        let ft2 = frank_scalar(&f2, "SELECT num FROM t2 WHERE id = 1").await;
        let rt2 = csql_scalar(&r2, "SELECT num FROM t2 WHERE id = 1");
        assert_eq!(ft2, rt2);
        assert_eq!(ft2, "42");
    });
}

// ── Test 2: CREATE INDEX on populated table parity ───────────────────

#[test]
fn create_index_on_populated_table_parity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("idx_pop.db");
        let r_path = dir.path().join("idx_pop_r.db");
        let f_str = f_path.to_str().unwrap();
        let r_str = r_path.to_str().unwrap();

        let setup = [
            "PRAGMA journal_mode = WAL;",
            "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, price INTEGER);",
        ];
        let inserts: Vec<String> = (0..100)
            .map(|i| {
                let cat = match i % 4 {
                    0 => "electronics",
                    1 => "books",
                    2 => "clothing",
                    _ => "food",
                };
                format!("INSERT INTO items VALUES ({i}, '{cat}', {});", i * 10 + 5)
            })
            .collect();

        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            let r = rusqlite::Connection::open(r_str).unwrap();
            for s in &setup {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
            for s in &inserts {
                f.execute(s).await.unwrap();
                r.execute_batch(s).unwrap();
            }
            // Now create indexes on populated table
            f.execute("CREATE INDEX idx_cat ON items(category);")
                .await
                .unwrap();
            f.execute("CREATE INDEX idx_price ON items(price);")
                .await
                .unwrap();
            r.execute_batch("CREATE INDEX idx_cat ON items(category);")
                .unwrap();
            r.execute_batch("CREATE INDEX idx_price ON items(price);")
                .unwrap();
        }

        // Queries that benefit from indexes
        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let r = rusqlite::Connection::open(r_str).unwrap();

        let fcat = frank_scalar(&f, "SELECT COUNT(*) FROM items WHERE category = 'books'").await;
        let rcat = csql_scalar(&r, "SELECT COUNT(*) FROM items WHERE category = 'books'");
        assert_eq!(fcat, rcat, "category count mismatch");
        assert_eq!(fcat, "25");

        let fprice = frank_scalar(
            &f,
            "SELECT COUNT(*) FROM items WHERE price BETWEEN 100 AND 500",
        )
        .await;
        let rprice = csql_scalar(
            &r,
            "SELECT COUNT(*) FROM items WHERE price BETWEEN 100 AND 500",
        );
        assert_eq!(fprice, rprice, "price range count mismatch");

        let fdata = frank_rows(
            &f,
            "SELECT id, category, price FROM items WHERE category = 'electronics' ORDER BY price LIMIT 5",
        )
        .await;
        let rdata = rusqlite_rows(
            &r,
            "SELECT id, category, price FROM items WHERE category = 'electronics' ORDER BY price LIMIT 5",
        );
        assert_eq!(fdata, rdata, "indexed query data mismatch");
    });
}

// ── Test 3: ALTER TABLE ADD COLUMN parity ────────────────────────────

#[test]
fn alter_table_add_column_parity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("alter.db");
        let r_path = dir.path().join("alter_r.db");
        let f_str = f_path.to_str().unwrap();
        let r_str = r_path.to_str().unwrap();

        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE evolve (id INTEGER PRIMARY KEY, name TEXT);")
                .await
                .unwrap();
            f.execute("INSERT INTO evolve VALUES (1, 'alice'), (2, 'bob');")
                .await
                .unwrap();

            let r = rusqlite::Connection::open(r_str).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch("CREATE TABLE evolve (id INTEGER PRIMARY KEY, name TEXT);")
                .unwrap();
            r.execute_batch("INSERT INTO evolve VALUES (1, 'alice'), (2, 'bob');")
                .unwrap();
        }

        // Add column
        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("ALTER TABLE evolve ADD COLUMN age INTEGER DEFAULT 0;")
                .await
                .unwrap();
            f.execute("UPDATE evolve SET age = 30 WHERE name = 'alice';")
                .await
                .unwrap();
            f.execute("INSERT INTO evolve VALUES (3, 'carol', 25);")
                .await
                .unwrap();

            let r = rusqlite::Connection::open(r_str).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch("ALTER TABLE evolve ADD COLUMN age INTEGER DEFAULT 0;")
                .unwrap();
            r.execute_batch("UPDATE evolve SET age = 30 WHERE name = 'alice';")
                .unwrap();
            r.execute_batch("INSERT INTO evolve VALUES (3, 'carol', 25);")
                .unwrap();
        }

        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let r = rusqlite::Connection::open(r_str).unwrap();

        let fdata = frank_rows(&f, "SELECT id, name, age FROM evolve ORDER BY id").await;
        let rdata = rusqlite_rows(&r, "SELECT id, name, age FROM evolve ORDER BY id");
        assert_eq!(fdata, rdata, "ALTER TABLE ADD COLUMN data mismatch");
        assert_eq!(fdata[0], vec!["1", "alice", "30"]);
        assert_eq!(fdata[1], vec!["2", "bob", "0"]);
        assert_eq!(fdata[2], vec!["3", "carol", "25"]);
    });
}

// ── Test 4: Rapid open/close cycles don't corrupt ────────────────────

#[test]
fn rapid_open_close_cycles_no_corruption() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("lifecycle.db");
        let f_str = f_path.to_str().unwrap();

        // Initial setup
        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE life (id INTEGER PRIMARY KEY, seq INTEGER);")
                .await
                .unwrap();
        }

        // Rapid open/write/close cycles
        for cycle in 0..20 {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute(&format!("INSERT INTO life VALUES ({cycle}, {cycle});"))
                .await
                .unwrap();
        }

        // Verify all data is present
        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let count = frank_scalar(&f, "SELECT COUNT(*) FROM life").await;
        assert_eq!(count, "20", "all 20 open/close cycles should have written");

        let data = frank_rows(&f, "SELECT id, seq FROM life ORDER BY id").await;
        for (i, row) in data.iter().enumerate() {
            assert_eq!(row, &vec![i.to_string(), i.to_string()], "row {i} mismatch");
        }
    });
}

// ── Test 5: Concurrent readers during DDL ────────────────────────────

#[test]
fn concurrent_readers_during_ddl() {
    asupersync::test_utils::run_test(|| async {
        let f_tmp = tempfile::NamedTempFile::new().unwrap();
        let f_path = f_tmp.path().to_str().unwrap().to_owned();

        // Setup initial data
        {
            let f = fsqlite::Connection::open(&f_path).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE base (id INTEGER PRIMARY KEY, val INTEGER);")
                .await
                .unwrap();
            for i in 0..50 {
                f.execute(&format!("INSERT INTO base VALUES ({i}, {});", i * 10))
                    .await
                    .unwrap();
            }
        }

        let barrier = Arc::new(Barrier::new(3));

        // Reader 1: reads base table continuously
        let fp1 = f_path.clone();
        let bar1 = barrier.clone();
        let reader1 = thread::spawn(move || {
            let mut reads = 0u32;
            asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&fp1).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                bar1.wait();
                for _ in 0..10 {
                    conn.execute("BEGIN").await.unwrap();
                    let count = frank_scalar(&conn, "SELECT COUNT(*) FROM base").await;
                    let n: i64 = count.parse().unwrap();
                    assert!(n >= 50, "reader1 should see at least 50 rows, got {n}");
                    conn.execute("COMMIT").await.unwrap();
                    reads += 1;
                    thread::sleep(std::time::Duration::from_millis(10));
                }
            });
            reads
        });

        // Reader 2: reads base table continuously
        let fp2 = f_path.clone();
        let bar2 = barrier.clone();
        let reader2 = thread::spawn(move || {
            let mut reads = 0u32;
            asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&fp2).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                bar2.wait();
                for _ in 0..10 {
                    conn.execute("BEGIN").await.unwrap();
                    let sum = frank_scalar(&conn, "SELECT SUM(val) FROM base").await;
                    let s: i64 = sum.parse().unwrap();
                    assert!(s > 0, "reader2 should see positive sum");
                    conn.execute("COMMIT").await.unwrap();
                    reads += 1;
                    thread::sleep(std::time::Duration::from_millis(10));
                }
            });
            reads
        });

        // DDL thread: adds data and creates index
        let fp_ddl = f_path.clone();
        let bar_ddl = barrier.clone();
        let ddl_thread = thread::spawn(move || {
            asupersync::test_utils::run_test(|| async {
                let conn = fsqlite::Connection::open(&fp_ddl).await.unwrap();
                conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
                bar_ddl.wait();
                for i in 50..75 {
                    conn.execute(&format!("INSERT INTO base VALUES ({i}, {});", i * 10))
                        .await
                        .unwrap();
                }
                conn.execute("CREATE INDEX idx_base_val ON base(val);")
                    .await
                    .unwrap();
            });
        });

        let r1 = reader1.join().unwrap();
        let r2 = reader2.join().unwrap();
        ddl_thread.join().unwrap();

        assert!(r1 > 0, "reader1 should have completed reads");
        assert!(r2 > 0, "reader2 should have completed reads");

        // Final verification
        let f = fsqlite::Connection::open(&f_path).await.unwrap();
        let count = frank_scalar(&f, "SELECT COUNT(*) FROM base").await;
        assert_eq!(count, "75", "should have 75 rows total");
    });
}

// ── Test 6: DROP TABLE + CREATE TABLE same name ──────────────────────

#[test]
fn drop_create_same_name_parity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("drop_create.db");
        let r_path = dir.path().join("drop_create_r.db");
        let f_str = f_path.to_str().unwrap();
        let r_str = r_path.to_str().unwrap();

        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            f.execute("CREATE TABLE reborn (id INTEGER PRIMARY KEY, v1 TEXT);")
                .await
                .unwrap();
            f.execute("INSERT INTO reborn VALUES (1, 'old');")
                .await
                .unwrap();
            f.execute("DROP TABLE reborn;").await.unwrap();
            f.execute("CREATE TABLE reborn (id INTEGER PRIMARY KEY, v1 TEXT, v2 INTEGER);")
                .await
                .unwrap();
            f.execute("INSERT INTO reborn VALUES (1, 'new', 42);")
                .await
                .unwrap();

            let r = rusqlite::Connection::open(r_str).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            r.execute_batch("CREATE TABLE reborn (id INTEGER PRIMARY KEY, v1 TEXT);")
                .unwrap();
            r.execute_batch("INSERT INTO reborn VALUES (1, 'old');")
                .unwrap();
            r.execute_batch("DROP TABLE reborn;").unwrap();
            r.execute_batch("CREATE TABLE reborn (id INTEGER PRIMARY KEY, v1 TEXT, v2 INTEGER);")
                .unwrap();
            r.execute_batch("INSERT INTO reborn VALUES (1, 'new', 42);")
                .unwrap();
        }

        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let r = rusqlite::Connection::open(r_str).unwrap();

        let fdata = frank_rows(&f, "SELECT id, v1, v2 FROM reborn").await;
        let rdata = rusqlite_rows(&r, "SELECT id, v1, v2 FROM reborn");
        assert_eq!(fdata, rdata, "drop/create same name data mismatch");
        assert_eq!(fdata[0], vec!["1", "new", "42"]);
    });
}

// ── Test 7: Many tables created sequentially ─────────────────────────

#[test]
fn many_tables_sequential_parity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("many_tbl.db");
        let r_path = dir.path().join("many_tbl_r.db");
        let f_str = f_path.to_str().unwrap();
        let r_str = r_path.to_str().unwrap();

        let n_tables = 20;

        {
            let f = fsqlite::Connection::open(f_str).await.unwrap();
            f.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            let r = rusqlite::Connection::open(r_str).unwrap();
            r.execute_batch("PRAGMA journal_mode=WAL;").unwrap();

            for t in 0..n_tables {
                let create = format!("CREATE TABLE tbl_{t} (id INTEGER PRIMARY KEY, data TEXT);");
                f.execute(&create).await.unwrap();
                r.execute_batch(&create).unwrap();
                for i in 0..5 {
                    let ins = format!("INSERT INTO tbl_{t} VALUES ({i}, 'tbl{t}_row{i}');");
                    f.execute(&ins).await.unwrap();
                    r.execute_batch(&ins).unwrap();
                }
            }
        }

        // Reopen and verify
        let f = fsqlite::Connection::open(f_str).await.unwrap();
        let r = rusqlite::Connection::open(r_str).unwrap();

        for t in 0..n_tables {
            let fcount = frank_scalar(&f, &format!("SELECT COUNT(*) FROM tbl_{t}")).await;
            let rcount = csql_scalar(&r, &format!("SELECT COUNT(*) FROM tbl_{t}"));
            assert_eq!(fcount, rcount, "table tbl_{t} count mismatch");
            assert_eq!(fcount, "5");
        }
    });
}
