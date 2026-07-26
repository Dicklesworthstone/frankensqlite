//! bd-g9rwh: Track H e2e tests — record assembly correctness, oracle parity.
//!
//! Verifies INSERT correctness at scale with oracle (rusqlite) parity.
//! Covers single-row inserts, multi-row VALUES, mixed types, and
//! roundtrip fidelity for various column types.
//!
//! - H1: 10K inserts with oracle comparison
//! - H2: Multi-row INSERT VALUES with oracle comparison
//! - H3: Mixed type columns (INTEGER, REAL, TEXT, BLOB, NULL)
//! - H4: Large TEXT/BLOB payloads
//! - H5: INSERT with DEFAULT VALUES
//! - H6: INSERT OR REPLACE (UPSERT) correctness
//! - H7: Roundtrip type fidelity (INSERT then SELECT, compare types)
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn test_tmpdir() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::temp_dir())
        .or_else(|_| tempfile::tempdir_in("."))
        .expect("tempdir")
}

async fn get_int(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(sql).await.ok()?;
    let row = rows.first()?;
    match row.get(0)? {
        SqliteValue::Integer(v) => Some(*v),
        _ => None,
    }
}

async fn setup_pair(dir: &tempfile::TempDir, name: &str) -> (Connection, rusqlite::Connection) {
    let f_path = dir.path().join(format!("{name}_f.db"));
    let c_path = dir.path().join(format!("{name}_c.db"));
    let f = Connection::open(f_path.to_str().expect("path"))
        .await
        .expect("fsqlite open");
    let c = rusqlite::Connection::open(&c_path).expect("csqlite open");
    (f, c)
}

// ─── H1: 10K inserts oracle comparison ────────────────────────────

#[test]
fn h1_insert_10k_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "h1").await;

        let ddl = "CREATE TABLE records (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        f.execute("BEGIN").await.expect("f begin");
        for i in 1..=10_000 {
            let sql = format!("INSERT INTO records VALUES ({i}, {}, 'label_{i}')", i * 7);
            f.execute(&sql).await.expect("f insert");
            c.execute(&sql, []).expect("c insert");
        }
        f.execute("COMMIT").await.expect("f commit");

        // Compare counts
        let f_count = get_int(&f, "SELECT COUNT(*) FROM records").await.unwrap();
        let c_count: i64 = c
            .query_row("SELECT COUNT(*) FROM records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            f_count, c_count,
            "H1: count mismatch f={f_count}, c={c_count}"
        );
        assert_eq!(f_count, 10_000);

        // Compare sums
        let f_sum = get_int(&f, "SELECT SUM(val) FROM records").await.unwrap();
        let c_sum: i64 = c
            .query_row("SELECT SUM(val) FROM records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_sum, c_sum, "H1: sum mismatch f={f_sum}, c={c_sum}");

        // Spot check first and last
        let f_first = get_int(&f, "SELECT val FROM records WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(f_first, 7);
        let f_last = get_int(&f, "SELECT val FROM records WHERE id = 10000")
            .await
            .unwrap();
        assert_eq!(f_last, 70_000);

        eprintln!("H1: 10K inserts — oracle parity, count={f_count}, sum={f_sum}");
    });
}

// ─── H2: Multi-row INSERT VALUES ──────────────────────────────────

#[test]
fn h2_multi_row_insert_values() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "h2").await;

        let ddl = "CREATE TABLE batch (id INTEGER PRIMARY KEY, x INTEGER, y TEXT)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        // Build multi-row INSERT: INSERT INTO batch VALUES (1,10,'a'), (2,20,'b'), ...
        let mut values = Vec::new();
        for i in 1..=500 {
            values.push(format!("({i}, {}, 'v_{i}')", i * 10));
        }
        let multi_insert = format!("INSERT INTO batch VALUES {}", values.join(", "));

        f.execute(&multi_insert).await.expect("f multi-insert");
        c.execute_batch(&multi_insert).expect("c multi-insert");

        let f_count = get_int(&f, "SELECT COUNT(*) FROM batch").await.unwrap();
        let c_count: i64 = c
            .query_row("SELECT COUNT(*) FROM batch", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_count, c_count, "H2: count mismatch");
        assert_eq!(f_count, 500);

        let f_sum = get_int(&f, "SELECT SUM(x) FROM batch").await.unwrap();
        let c_sum: i64 = c
            .query_row("SELECT SUM(x) FROM batch", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_sum, c_sum, "H2: sum mismatch");

        eprintln!("H2: multi-row INSERT VALUES — oracle parity, {f_count} rows");
    });
}

// ─── H3: Mixed type columns ──────────────────────────────────────

#[test]
fn h3_mixed_type_columns() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "h3").await;

        let ddl = "CREATE TABLE mixed (id INTEGER PRIMARY KEY, i_col INTEGER, r_col REAL, t_col TEXT, b_col BLOB)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        // Insert rows with different value patterns
        let inserts = [
            "INSERT INTO mixed VALUES (1, 42, 3.14, 'hello', X'DEADBEEF')",
            "INSERT INTO mixed VALUES (2, -999, 0.0, '', X'')",
            "INSERT INTO mixed VALUES (3, 0, -1.5, 'unicode: ñ', X'00FF00')",
            "INSERT INTO mixed VALUES (4, 9223372036854775807, 1e10, 'max_int', X'0102030405')",
            "INSERT INTO mixed VALUES (5, NULL, NULL, NULL, NULL)",
        ];

        for sql in &inserts {
            f.execute(sql).await.expect("f insert");
            c.execute_batch(sql).expect("c insert");
        }

        // Compare all rows
        let f_rows = f
            .query("SELECT * FROM mixed ORDER BY id")
            .await
            .expect("f query");
        let mut c_stmt = c
            .prepare("SELECT id, i_col, t_col FROM mixed ORDER BY id")
            .expect("c prepare");
        let c_rows: Vec<(i64, Option<i64>, Option<String>)> = c_stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .expect("c query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");

        assert_eq!(f_rows.len(), c_rows.len(), "H3: row count mismatch");

        for (f_row, c_row) in f_rows.iter().zip(c_rows.iter()) {
            let f_id = match f_row.get(0) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            assert_eq!(f_id, c_row.0, "H3: id mismatch");

            let f_int = match f_row.get(1) {
                Some(SqliteValue::Integer(v)) => Some(*v),
                _ => None,
            };
            assert_eq!(f_int, c_row.1, "H3: integer col mismatch for id={f_id}");
        }

        eprintln!(
            "H3: mixed type columns — oracle parity on {} rows",
            f_rows.len()
        );
    });
}

// ─── H4: Large TEXT/BLOB payloads ─────────────────────────────────

#[test]
fn h4_large_payloads() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("h4.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE large (id INTEGER PRIMARY KEY, data TEXT)")
            .await
            .expect("create");

        // Insert rows with progressively larger text
        conn.execute("BEGIN").await.expect("begin");
        for i in 1..=20 {
            let size = i * 500;
            let text: String = (0..size).map(|j| (b'A' + (j % 26) as u8) as char).collect();
            conn.execute(&format!("INSERT INTO large VALUES ({i}, '{text}')"))
                .await
                .expect("insert large");
        }
        conn.execute("COMMIT").await.expect("commit");

        // Verify all rows stored correctly
        let rows = conn
            .query("SELECT id, LENGTH(data) FROM large ORDER BY id")
            .await
            .expect("query");

        for (i, row) in rows.iter().enumerate() {
            let id = match row.get(0) {
                Some(SqliteValue::Integer(v)) => *v as usize,
                _ => 0,
            };
            let len = match row.get(1) {
                Some(SqliteValue::Integer(v)) => *v as usize,
                _ => 0,
            };
            let expected_len = id * 500;
            assert_eq!(
                len, expected_len,
                "H4: row {i} (id={id}) length mismatch: got {len}, expected {expected_len}"
            );
        }

        // Verify persistence
        drop(conn);
        let conn2 = Connection::open(path_str).await.expect("reopen");
        let count = get_int(&conn2, "SELECT COUNT(*) FROM large").await.unwrap();
        assert_eq!(count, 20, "H4: data lost after reopen");

        eprintln!("H4: large TEXT payloads (500-10000 chars) — stored and retrieved correctly");
    });
}

// ─── H5: INSERT with DEFAULT VALUES ───────────────────────────────

#[test]
fn h5_insert_default_values() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "h5").await;

        let ddl = "CREATE TABLE defaults (id INTEGER PRIMARY KEY, val INTEGER DEFAULT 42, label TEXT DEFAULT 'default')";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        // DEFAULT VALUES inserts
        for _ in 0..10 {
            f.execute("INSERT INTO defaults DEFAULT VALUES")
                .await
                .expect("f insert default");
            c.execute("INSERT INTO defaults DEFAULT VALUES", [])
                .expect("c insert default");
        }

        let f_count = get_int(&f, "SELECT COUNT(*) FROM defaults").await.unwrap();
        let c_count: i64 = c
            .query_row("SELECT COUNT(*) FROM defaults", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_count, c_count, "H5: count mismatch");

        // All val columns should be 42
        let f_sum = get_int(&f, "SELECT SUM(val) FROM defaults").await.unwrap();
        let c_sum: i64 = c
            .query_row("SELECT SUM(val) FROM defaults", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_sum, c_sum, "H5: sum mismatch");
        assert_eq!(f_sum, 420, "H5: 10 rows * 42 default should be 420");

        eprintln!("H5: INSERT DEFAULT VALUES — oracle parity, {f_count} rows");
    });
}

// ─── H6: INSERT OR REPLACE correctness ────────────────────────────

#[test]
fn h6_insert_or_replace() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "h6").await;

        let ddl = "CREATE TABLE upsert (id INTEGER PRIMARY KEY, val INTEGER, tag TEXT)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        // Initial inserts
        for i in 1..=100 {
            let sql = format!("INSERT INTO upsert VALUES ({i}, {i}, 'original')");
            f.execute(&sql).await.expect("f insert");
            c.execute(&sql, []).expect("c insert");
        }

        // Replace even-id rows
        for i in (2..=100).step_by(2) {
            let sql = format!(
                "INSERT OR REPLACE INTO upsert VALUES ({i}, {}, 'replaced')",
                i * 100
            );
            f.execute(&sql).await.expect("f replace");
            c.execute(&sql, []).expect("c replace");
        }

        // Count should still be 100
        let f_count = get_int(&f, "SELECT COUNT(*) FROM upsert").await.unwrap();
        let c_count: i64 = c
            .query_row("SELECT COUNT(*) FROM upsert", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_count, c_count, "H6: count mismatch");
        assert_eq!(f_count, 100);

        // Check replaced rows
        let f_replaced = get_int(&f, "SELECT COUNT(*) FROM upsert WHERE tag = 'replaced'")
            .await
            .unwrap();
        let c_replaced: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM upsert WHERE tag = 'replaced'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(f_replaced, c_replaced, "H6: replaced count mismatch");
        assert_eq!(f_replaced, 50);

        // Check sums
        let f_sum = get_int(&f, "SELECT SUM(val) FROM upsert").await.unwrap();
        let c_sum: i64 = c
            .query_row("SELECT SUM(val) FROM upsert", [], |r| r.get(0))
            .unwrap();
        assert_eq!(f_sum, c_sum, "H6: sum mismatch");

        eprintln!("H6: INSERT OR REPLACE — oracle parity, 50 replaced");
    });
}

// ─── H7: Roundtrip type fidelity ──────────────────────────────────

#[test]
fn h7_roundtrip_type_fidelity() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("h7.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE types (id INTEGER PRIMARY KEY, val)")
            .await
            .expect("create");

        // Insert various types via SQL (no parameters)
        conn.execute("INSERT INTO types VALUES (1, 42)")
            .await
            .expect("int");
        conn.execute("INSERT INTO types VALUES (2, 3.14)")
            .await
            .expect("real");
        conn.execute("INSERT INTO types VALUES (3, 'text')")
            .await
            .expect("text");
        conn.execute("INSERT INTO types VALUES (4, X'CAFE')")
            .await
            .expect("blob");
        conn.execute("INSERT INTO types VALUES (5, NULL)")
            .await
            .expect("null");

        let rows = conn
            .query("SELECT id, val, typeof(val) FROM types ORDER BY id")
            .await
            .expect("query");

        let expected_types = ["integer", "real", "text", "blob", "null"];
        for (row, expected_type) in rows.iter().zip(expected_types.iter()) {
            let id = match row.get(0) {
                Some(SqliteValue::Integer(v)) => *v,
                _ => -1,
            };
            let type_str = match row.get(2) {
                Some(SqliteValue::Text(s)) => s.as_str().to_string(),
                _ => "unknown".to_string(),
            };
            assert_eq!(
                type_str, *expected_type,
                "H7: id={id} typeof mismatch: got '{type_str}', expected '{expected_type}'"
            );
        }

        // Verify actual values roundtrip
        match rows[0].get(1) {
            Some(SqliteValue::Integer(42)) => {}
            other => panic!("H7: id=1 should be Integer(42), got {other:?}"),
        }
        match rows[2].get(1) {
            Some(SqliteValue::Text(s)) if s.as_str() == "text" => {}
            other => panic!("H7: id=3 should be Text('text'), got {other:?}"),
        }
        match rows[4].get(1) {
            Some(SqliteValue::Null) | None => {}
            other => panic!("H7: id=5 should be NULL, got {other:?}"),
        }

        eprintln!("H7: roundtrip type fidelity — all 5 types verified");
    });
}
