//! bd-gv5ak: Track O e2e tests — serialization correctness across
//! various integer sizes, mixed types, wide tables, and edge cases.
//!
//! Verifies that data roundtrips correctly through the serialization
//! layer by inserting values of specific sizes and types, then reading
//! them back with oracle (rusqlite) parity.
//!
//! - S1: Integer size boundaries (i8, i16, i32, i64 range edges)
//! - S2: Mixed NULLs and non-NULLs
//! - S3: Wide table (100 columns)
//! - S4: Large integer sequences (10K rows, various sizes)
//! - S5: REAL precision roundtrip
//! - S6: TEXT encoding edge cases (empty, unicode, long)
//! - S7: BLOB roundtrip (empty, small, medium)
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
        .expect("f open");
    let c = rusqlite::Connection::open(&c_path).expect("c open");
    (f, c)
}

// ─── S1: Integer size boundaries ──────────────────────────────────

#[test]
fn s1_integer_size_boundaries() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "s1").await;

        let ddl = "CREATE TABLE ints (id INTEGER PRIMARY KEY, val INTEGER)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        // Boundary values for different integer encodings
        let values: Vec<i64> = vec![
            0,
            1,
            -1,
            127,
            -128, // i8 boundaries
            128,
            -129,
            32767,
            -32768, // i16 boundaries
            32768,
            -32769,
            8388607,
            -8388608, // i24 boundaries
            2147483647,
            -2147483648, // i32 boundaries
            2147483648,
            -2147483649,
            140737488355327, // i48 boundary
            i64::MAX,
            i64::MIN, // i64 boundaries
        ];

        f.execute("BEGIN").await.expect("f begin");
        for (i, &val) in values.iter().enumerate() {
            let id = (i + 1) as i64;
            let sql = format!("INSERT INTO ints VALUES ({id}, {val})");
            f.execute(&sql).await.expect("f insert");
            c.execute(&sql, []).expect("c insert");
        }
        f.execute("COMMIT").await.expect("f commit");

        // Verify each value roundtrips correctly
        for (i, &expected) in values.iter().enumerate() {
            let id = (i + 1) as i64;
            let f_val = get_int(&f, &format!("SELECT val FROM ints WHERE id = {id}")).await;
            let c_val: i64 = c
                .query_row(&format!("SELECT val FROM ints WHERE id = {id}"), [], |r| {
                    r.get(0)
                })
                .expect("c query");

            assert_eq!(
                f_val,
                Some(c_val),
                "S1: id={id} val={expected} — f={f_val:?}, c={c_val}"
            );
            assert_eq!(f_val, Some(expected), "S1: id={id} roundtrip failed");
        }

        eprintln!(
            "S1: {} integer boundary values roundtrip correctly",
            values.len()
        );
    });
}

// ─── S2: Mixed NULLs and non-NULLs ───────────────────────────────

#[test]
fn s2_mixed_nulls() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "s2").await;

        let ddl = "CREATE TABLE nulls (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d BLOB)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        let rows_sql = [
            "INSERT INTO nulls VALUES (1, 42, 'hello', 3.14, X'FF')",
            "INSERT INTO nulls VALUES (2, NULL, 'world', NULL, X'00')",
            "INSERT INTO nulls VALUES (3, 99, NULL, 2.72, NULL)",
            "INSERT INTO nulls VALUES (4, NULL, NULL, NULL, NULL)",
            "INSERT INTO nulls VALUES (5, 0, '', 0.0, X'')",
        ];

        for sql in &rows_sql {
            f.execute(sql).await.expect("f insert");
            c.execute_batch(sql).expect("c insert");
        }

        // Verify NULL counts match
        let null_checks = [
            ("SELECT COUNT(*) FROM nulls WHERE a IS NULL", "a-null"),
            ("SELECT COUNT(*) FROM nulls WHERE b IS NULL", "b-null"),
            ("SELECT COUNT(*) FROM nulls WHERE c IS NULL", "c-null"),
            ("SELECT COUNT(*) FROM nulls WHERE d IS NULL", "d-null"),
            (
                "SELECT COUNT(*) FROM nulls WHERE a IS NOT NULL",
                "a-notnull",
            ),
        ];

        for (sql, label) in &null_checks {
            let f_val = get_int(&f, sql).await.unwrap();
            let c_val: i64 = c.query_row(sql, [], |r| r.get(0)).unwrap();
            assert_eq!(f_val, c_val, "S2: {label} mismatch f={f_val}, c={c_val}");
        }

        eprintln!("S2: mixed NULL patterns — oracle parity confirmed");
    });
}

// ─── S3: Wide table (100 columns) ─────────────────────────────────

#[test]
fn s3_wide_table() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("s3.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");

        // Create table with 100 INTEGER columns
        let cols: Vec<String> = (0..100).map(|i| format!("c{i} INTEGER")).collect();
        let ddl = format!(
            "CREATE TABLE wide (id INTEGER PRIMARY KEY, {})",
            cols.join(", ")
        );
        conn.execute(&ddl).await.expect("create wide");

        // Insert a row with all 100 columns set
        let vals: Vec<String> = (0..100).map(|i| format!("{}", i * 7)).collect();
        let insert = format!("INSERT INTO wide VALUES (1, {})", vals.join(", "));
        conn.execute(&insert).await.expect("insert wide");

        // Read back and verify
        let rows = conn
            .query("SELECT * FROM wide WHERE id = 1")
            .await
            .expect("query");
        assert_eq!(rows.len(), 1, "S3: should have 1 row");

        let row = &rows[0];
        // Column 0 is id (=1), columns 1-100 are c0-c99
        for i in 0..100 {
            let col_idx = i + 1;
            let expected = (i * 7) as i64;
            match row.get(col_idx) {
                Some(SqliteValue::Integer(v)) => {
                    assert_eq!(
                        *v, expected,
                        "S3: column c{i} (idx {col_idx}) expected {expected}, got {v}"
                    );
                }
                other => panic!("S3: column c{i} wrong type: {other:?}"),
            }
        }

        eprintln!("S3: 100-column wide table — all values roundtrip correctly");
    });
}

// ─── S4: Large integer sequence ───────────────────────────────────

#[test]
fn s4_large_integer_sequence() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "s4").await;

        let ddl =
            "CREATE TABLE seq (id INTEGER PRIMARY KEY, small INTEGER, medium INTEGER, big INTEGER)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        f.execute("BEGIN").await.expect("f begin");
        for i in 1..=10_000i64 {
            let small = i % 127;
            let medium = i * 257;
            let big = i * i * 1_000_000;
            let sql = format!("INSERT INTO seq VALUES ({i}, {small}, {medium}, {big})");
            f.execute(&sql).await.expect("f insert");
            c.execute(&sql, []).expect("c insert");
        }
        f.execute("COMMIT").await.expect("f commit");

        // Compare aggregates
        let checks = [
            "SELECT SUM(small) FROM seq",
            "SELECT SUM(medium) FROM seq",
            "SELECT MIN(big) FROM seq",
            "SELECT MAX(big) FROM seq",
            "SELECT COUNT(DISTINCT small) FROM seq",
        ];

        for sql in &checks {
            let f_val = get_int(&f, sql).await;
            let c_val: Option<i64> = c
                .prepare(sql)
                .ok()
                .and_then(|mut s| s.query_row([], |r| r.get(0)).ok());
            assert_eq!(
                f_val, c_val,
                "S4: mismatch for {sql} — f={f_val:?}, c={c_val:?}"
            );
        }

        eprintln!("S4: 10K rows with mixed integer sizes — oracle parity");
    });
}

// ─── S5: REAL precision roundtrip ─────────────────────────────────

#[test]
fn s5_real_precision() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let db_path = dir.path().join("s5.db");
        let path_str = db_path.to_str().expect("path");

        let conn = Connection::open(path_str).await.expect("open");
        conn.execute("CREATE TABLE reals (id INTEGER PRIMARY KEY, val REAL)")
            .await
            .expect("create");

        let values = [
            0.0,
            1.0,
            -1.0,
            3.141592653589793,
            2.718281828459045,
            1e-10,
            1e10,
            1e-300,
            1e300,
            f64::MIN_POSITIVE,
        ];

        for (i, &val) in values.iter().enumerate() {
            let id = (i + 1) as i64;
            conn.execute(&format!("INSERT INTO reals VALUES ({id}, {val})"))
                .await
                .expect("insert");
        }

        for (i, &expected) in values.iter().enumerate() {
            let id = (i + 1) as i64;
            let rows = conn
                .query(&format!("SELECT val FROM reals WHERE id = {id}"))
                .await
                .expect("query");
            match rows[0].get(0) {
                Some(SqliteValue::Float(v)) => {
                    assert!(
                        (*v - expected).abs() < 1e-10 * expected.abs().max(1.0),
                        "S5: id={id} precision loss: got {v}, expected {expected}"
                    );
                }
                Some(SqliteValue::Integer(v))
                    if expected == 0.0 || expected == 1.0 || expected == -1.0 =>
                {
                    let v_f = *v as f64;
                    assert!(
                        (v_f - expected).abs() < 1e-10,
                        "S5: id={id} stored as int {v}, expected float {expected}"
                    );
                }
                other => panic!("S5: id={id} wrong type: {other:?}"),
            }
        }

        eprintln!("S5: {} REAL values roundtrip with precision", values.len());
    });
}

// ─── S6: TEXT encoding edge cases ─────────────────────────────────

#[test]
fn s6_text_encoding_edges() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "s6").await;

        let ddl = "CREATE TABLE texts (id INTEGER PRIMARY KEY, val TEXT)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        let test_strings = [
            ("empty", ""),
            ("single", "x"),
            ("ascii", "hello world"),
            ("unicode_accents", "café résumé"),
            ("unicode_cjk", "日本語テスト"),
            ("unicode_emoji_desc", "star"),
            ("spaces", "   leading and trailing   "),
            ("newlines", "line1\nline2\nline3"),
            ("tabs", "col1\tcol2\tcol3"),
        ];

        for (i, (_label, val)) in test_strings.iter().enumerate() {
            let id = (i + 1) as i64;
            // Use single quotes, escape any internal quotes
            let escaped = val.replace('\'', "''");
            let sql = format!("INSERT INTO texts VALUES ({id}, '{escaped}')");
            f.execute(&sql).await.expect("f insert");
            c.execute_batch(&sql).expect("c insert");
        }

        // Compare lengths
        for (i, (label, _)) in test_strings.iter().enumerate() {
            let id = (i + 1) as i64;
            let sql = format!("SELECT LENGTH(val) FROM texts WHERE id = {id}");
            let f_len = get_int(&f, &sql).await;
            let c_len: Option<i64> = c
                .prepare(&sql)
                .ok()
                .and_then(|mut s| s.query_row([], |r| r.get(0)).ok());
            assert_eq!(
                f_len, c_len,
                "S6: {label} length mismatch f={f_len:?}, c={c_len:?}"
            );
        }

        eprintln!(
            "S6: {} text encoding edge cases — oracle parity",
            test_strings.len()
        );
    });
}

// ─── S7: BLOB roundtrip ──────────────────────────────────────────

#[test]
fn s7_blob_roundtrip() {
    asupersync::test_utils::run_test(|| async {
        let dir = test_tmpdir();
        let (f, c) = setup_pair(&dir, "s7").await;

        let ddl = "CREATE TABLE blobs (id INTEGER PRIMARY KEY, val BLOB)";
        f.execute(ddl).await.expect("f create");
        c.execute_batch(ddl).expect("c create");

        let test_blobs = [
            ("empty", "X''"),
            ("single_byte", "X'FF'"),
            ("deadbeef", "X'DEADBEEF'"),
            ("zeros", "X'0000000000'"),
            ("sequence", "X'0102030405060708090A0B0C0D0E0F10'"),
        ];

        for (i, (_label, hex)) in test_blobs.iter().enumerate() {
            let id = (i + 1) as i64;
            let sql = format!("INSERT INTO blobs VALUES ({id}, {hex})");
            f.execute(&sql).await.expect("f insert");
            c.execute_batch(&sql).expect("c insert");
        }

        // Compare lengths
        for (i, (label, _)) in test_blobs.iter().enumerate() {
            let id = (i + 1) as i64;
            let sql = format!("SELECT LENGTH(val) FROM blobs WHERE id = {id}");
            let f_len = get_int(&f, &sql).await;
            let c_len: Option<i64> = c
                .prepare(&sql)
                .ok()
                .and_then(|mut s| s.query_row([], |r| r.get(0)).ok());
            assert_eq!(
                f_len, c_len,
                "S7: {label} blob length mismatch f={f_len:?}, c={c_len:?}"
            );
        }

        // Compare hex() output
        for (i, (label, _)) in test_blobs.iter().enumerate() {
            let id = (i + 1) as i64;
            let sql = format!("SELECT HEX(val) FROM blobs WHERE id = {id}");
            let f_rows = f.query(&sql).await.expect("f query");
            let f_hex = match f_rows[0].get(0) {
                Some(SqliteValue::Text(s)) => s.as_str().to_string(),
                _ => "?".to_string(),
            };
            let c_hex: String = c.query_row(&sql, [], |r| r.get(0)).expect("c hex");
            assert_eq!(
                f_hex, c_hex,
                "S7: {label} HEX mismatch f={f_hex}, c={c_hex}"
            );
        }

        eprintln!(
            "S7: {} blob roundtrip values — oracle parity",
            test_blobs.len()
        );
    });
}
