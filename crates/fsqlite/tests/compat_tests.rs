//! Integration tests for the frankensqlite compat adapter layer.
//!
//! Bead: coding_agent_session_search-15tra
//!
//! These tests exercise the full compat API surface against live in-memory
//! databases, complementing the inline unit tests in each submodule.

use fsqlite::Connection;
use fsqlite::compat::*;
use fsqlite::params;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;
use rusqlite::Connection as RusqliteConnection;

// ===========================================================================
// 1. PARAMS MACRO
// ===========================================================================

#[test]
fn params_macro_empty_produces_empty_slice() {
    let p = params![];
    assert!(p.is_empty());
}

#[test]
fn params_macro_mixed_types_correct_values() {
    let p = params![1_i64, "hello", 3.14_f64];
    assert_eq!(p.len(), 3);
    assert_eq!(p[0].as_sqlite_value(), &SqliteValue::Integer(1));
    assert_eq!(p[1].as_sqlite_value(), &SqliteValue::Text("hello".into()));
    assert_eq!(p[2].as_sqlite_value(), &SqliteValue::Float(3.14));
}

#[test]
fn params_macro_none_produces_null() {
    let p = params![None::<i64>];
    assert_eq!(p.len(), 1);
    assert_eq!(p[0].as_sqlite_value(), &SqliteValue::Null);
}

#[test]
fn params_macro_bool_true_and_false() {
    let p = params![true, false];
    assert_eq!(p[0].as_sqlite_value(), &SqliteValue::Integer(1));
    assert_eq!(p[1].as_sqlite_value(), &SqliteValue::Integer(0));
}

#[test]
fn params_macro_blob() {
    let p = params![vec![1_u8, 2, 3]];
    assert_eq!(
        p[0].as_sqlite_value(),
        &SqliteValue::Blob(vec![1, 2, 3].into())
    );
}

#[test]
fn params_macro_trailing_comma() {
    let p = params![1_i64, 2_i64,];
    assert_eq!(p.len(), 2);
}

// ===========================================================================
// 2. FROM IMPLS / PARAMVALUE
// ===========================================================================

#[test]
fn param_value_from_bool() {
    assert_eq!(ParamValue::from(true).into_inner(), SqliteValue::Integer(1));
    assert_eq!(
        ParamValue::from(false).into_inner(),
        SqliteValue::Integer(0)
    );
}

#[test]
fn param_value_from_option_some_and_none() {
    let some: ParamValue = Some(42_i64).into();
    assert_eq!(some.into_inner(), SqliteValue::Integer(42));

    let none: ParamValue = None::<i64>.into();
    assert_eq!(none.into_inner(), SqliteValue::Null);
}

#[test]
fn param_value_from_u32() {
    let p: ParamValue = 42_u32.into();
    assert_eq!(p.into_inner(), SqliteValue::Integer(42));
}

#[test]
fn param_value_from_u64_valid() {
    let p: ParamValue = 100_u64.into();
    assert_eq!(p.into_inner(), SqliteValue::Integer(100));
}

#[test]
fn param_value_from_u64_overflow_preserves_exact_value() {
    let p: ParamValue = u64::MAX.into();
    assert_eq!(
        p.into_inner(),
        SqliteValue::Text(u64::MAX.to_string().into())
    );
}

#[test]
fn param_value_from_usize() {
    let p: ParamValue = 99_usize.into();
    assert_eq!(p.into_inner(), SqliteValue::Integer(99));
}

#[test]
fn param_value_from_string_and_str() {
    let p: ParamValue = "hello".into();
    assert_eq!(p.into_inner(), SqliteValue::Text("hello".into()));

    let p: ParamValue = String::from("world").into();
    assert_eq!(p.into_inner(), SqliteValue::Text("world".into()));
}

#[test]
fn param_value_from_byte_slice() {
    let data: &[u8] = &[0xDE, 0xAD];
    let p: ParamValue = data.into();
    assert_eq!(p.into_inner(), SqliteValue::Blob(vec![0xDE, 0xAD].into()));
}

// ===========================================================================
// 3. TYPED ROW EXTRACTION (RowExt)
// ===========================================================================

#[test]
fn row_get_typed_integer() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (42)").unwrap();
    let row = conn.query_row("SELECT val FROM t").unwrap();
    let v: i64 = row.get_typed(0).unwrap();
    assert_eq!(v, 42);
}

#[test]
fn row_get_typed_string() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val TEXT)").unwrap();
    conn.execute("INSERT INTO t VALUES ('hello')").unwrap();
    let row = conn.query_row("SELECT val FROM t").unwrap();
    let v: String = row.get_typed(0).unwrap();
    assert_eq!(v, "hello");
}

#[test]
fn row_get_typed_option_null() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val TEXT)").unwrap();
    conn.execute_with_params("INSERT INTO t VALUES (?1)", &[SqliteValue::Null])
        .unwrap();
    let row = conn.query_row("SELECT val FROM t").unwrap();
    let v: Option<String> = row.get_typed(0).unwrap();
    assert!(v.is_none());
}

#[test]
fn row_get_typed_f64_from_integer_coercion() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (7)").unwrap();
    let row = conn.query_row("SELECT val FROM t").unwrap();
    let v: f64 = row.get_typed(0).unwrap();
    assert!((v - 7.0).abs() < f64::EPSILON);
}

#[test]
fn row_get_typed_bool() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let row = conn.query_row("SELECT val FROM t").unwrap();
    let v: bool = row.get_typed(0).unwrap();
    assert!(v);
}

// ===========================================================================
// 4. QUERY WITH CLOSURE (ConnectionExt)
// ===========================================================================

#[test]
fn query_row_map_returns_closure_result() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

    let name: String = conn
        .query_row_map("SELECT id, name FROM t", &[], |row| row.get_typed(1))
        .unwrap();
    assert_eq!(name, "alice");
}

#[test]
fn query_row_map_with_params() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();

    let name: String = conn
        .query_row_map("SELECT name FROM t WHERE id = ?1", params![2_i64], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(name, "bob");
}

#[test]
fn query_row_map_empty_returns_error() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER)").unwrap();

    let result: Result<i64, _> =
        conn.query_row_map("SELECT id FROM t WHERE id = 999", &[], |row| {
            row.get_typed(0)
        });
    assert!(matches!(result, Err(FrankenError::QueryReturnedNoRows)));
}

#[test]
fn query_map_collect_returns_vec() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (10)").unwrap();
    conn.execute("INSERT INTO t VALUES (20)").unwrap();
    conn.execute("INSERT INTO t VALUES (30)").unwrap();

    let vals: Vec<i64> = conn
        .query_map_collect("SELECT val FROM t ORDER BY val", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(vals, vec![10, 20, 30]);
}

#[test]
fn query_map_collect_empty_returns_empty_vec() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER)").unwrap();

    let vals: Vec<i64> = conn
        .query_map_collect("SELECT id FROM t WHERE id > 999", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert!(vals.is_empty());
}

#[test]
fn execute_params_inserts_rows() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER, val TEXT)")
        .unwrap();

    let changed = conn
        .execute_compat("INSERT INTO t VALUES (?1, ?2)", params![1_i64, "hello"])
        .unwrap();
    assert_eq!(changed, 1);

    let row = conn.query_row("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(row.get(0).unwrap(), &SqliteValue::Text("hello".into()));
}

// ===========================================================================
// 5. EXECUTE_BATCH
// ===========================================================================

#[test]
fn execute_batch_multi_statement() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute_batch(
        "CREATE TABLE a (id INTEGER PRIMARY KEY);
         CREATE TABLE b (id INTEGER PRIMARY KEY);
         INSERT INTO a VALUES (1);
         INSERT INTO b VALUES (2);",
    )
    .unwrap();

    let rows_a = conn.query("SELECT COUNT(*) FROM a").unwrap();
    assert_eq!(rows_a[0].get(0).unwrap(), &SqliteValue::Integer(1));

    let rows_b = conn.query("SELECT COUNT(*) FROM b").unwrap();
    assert_eq!(rows_b[0].get(0).unwrap(), &SqliteValue::Integer(1));
}

#[test]
fn execute_batch_empty_string_is_noop() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute_batch("").unwrap();
}

#[test]
fn execute_batch_single_statement_without_semicolon() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute_batch("CREATE TABLE t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let rows = conn.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1));
}

#[test]
fn execute_batch_with_comments() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute_batch(
        "-- This is a comment
         CREATE TABLE t(x INTEGER);
         /* Block comment */
         INSERT INTO t VALUES (42);",
    )
    .unwrap();

    let rows = conn.query("SELECT x FROM t").unwrap();
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(42));
}

#[test]
fn execute_batch_pragma_blocks() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA cache_size=-4000;
         PRAGMA synchronous=NORMAL;",
    )
    .unwrap();
    // If no error, PRAGMAs were applied successfully.
}

// ===========================================================================
// 6. TRANSACTION
// ===========================================================================

#[test]
fn transaction_commit_persists_data() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val TEXT)").unwrap();

    {
        let mut tx = conn.transaction().unwrap();
        tx.execute("INSERT INTO t VALUES ('committed')").unwrap();
        tx.commit().unwrap();
    }

    let rows = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("committed".into())
    );
}

#[test]
fn transaction_drop_without_commit_rolls_back() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val TEXT)").unwrap();

    {
        let tx = conn.transaction().unwrap();
        tx.execute("INSERT INTO t VALUES ('dropped')").unwrap();
        // tx dropped without commit
    }

    let rows = conn.query("SELECT val FROM t").unwrap();
    assert!(rows.is_empty());
}

#[test]
fn transaction_explicit_rollback() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val TEXT)").unwrap();

    let mut tx = conn.transaction().unwrap();
    tx.execute("INSERT INTO t VALUES ('rolled_back')").unwrap();
    tx.rollback().unwrap();

    let rows = conn.query("SELECT val FROM t").unwrap();
    assert!(rows.is_empty());
}

#[test]
fn transaction_execute_with_params() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER, val TEXT)")
        .unwrap();

    {
        let mut tx = conn.transaction().unwrap();
        tx.execute_with_params(
            "INSERT INTO t VALUES (?1, ?2)",
            &[SqliteValue::Integer(1), SqliteValue::Text("in_tx".into())],
        )
        .unwrap();
        tx.commit().unwrap();
    }

    let rows = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("in_tx".into()));
}

#[test]
fn transaction_query_within() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (42)").unwrap();

    let mut tx = conn.transaction().unwrap();
    let rows = tx.query("SELECT val FROM t").unwrap();
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(42));
    tx.commit().unwrap();
}

#[test]
fn transaction_execute_params_compat() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER, val TEXT)")
        .unwrap();

    let mut tx = conn.transaction().unwrap();
    tx.execute_compat(
        "INSERT INTO t VALUES (?1, ?2)",
        params![1_i64, "via_params"],
    )
    .unwrap();
    tx.commit().unwrap();

    let row = conn.query_row("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(row.get(0).unwrap(), &SqliteValue::Text("via_params".into()));
}

#[test]
fn transaction_failed_commit_keeps_transaction_open_for_explicit_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("compat_failed_commit_keeps_tx_open.db");
    let db = db_path.to_string_lossy().to_string();

    {
        let conn = Connection::open(&db).unwrap();
        conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 0);").unwrap();
    }

    let conn_a = Connection::open(&db).unwrap();
    let conn_b = Connection::open(&db).unwrap();
    for conn in [&conn_a, &conn_b] {
        conn.execute("PRAGMA busy_timeout=5000;").unwrap();
        conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").unwrap();
    }

    let mut tx_a = conn_a.transaction().unwrap();
    let mut tx_b = conn_b.transaction().unwrap();

    tx_a.execute("UPDATE t SET v = v + 1 WHERE id = 1;")
        .unwrap();

    let loser_err = match tx_b.execute("UPDATE t SET v = v + 1 WHERE id = 1;") {
        Ok(changes) => {
            assert_eq!(changes, 1);
            tx_a.commit().unwrap();
            tx_b.commit().expect_err(
                "second compat transaction should fail transiently once the stale snapshot resolves",
            )
        }
        Err(err) => {
            tx_a.commit().unwrap();
            err
        }
    };

    assert!(
        loser_err.is_transient(),
        "losing compat transaction should fail transiently, got {loser_err:?}"
    );
    assert!(
        conn_b.in_transaction(),
        "failed compat commit must leave the underlying transaction open for caller-directed recovery"
    );

    tx_b.rollback().unwrap();
    assert!(
        !conn_b.in_transaction(),
        "explicit rollback should close the failed compat transaction"
    );

    let row = conn_a.query_row("SELECT v FROM t WHERE id = 1").unwrap();
    assert_eq!(row.get(0).unwrap(), &SqliteValue::Integer(1));
}

// ===========================================================================
// 7. OPTIONAL EXTENSION
// ===========================================================================

#[test]
fn optional_ok_becomes_some() {
    let result: Result<i64, FrankenError> = Ok(42);
    assert_eq!(result.optional().unwrap(), Some(42));
}

#[test]
fn optional_no_rows_becomes_none() {
    let result: Result<i64, FrankenError> = Err(FrankenError::QueryReturnedNoRows);
    assert_eq!(result.optional().unwrap(), None);
}

#[test]
fn optional_other_error_passes_through() {
    let result: Result<i64, FrankenError> = Err(FrankenError::SyntaxError {
        token: "bad sql".to_string(),
    });
    assert!(result.optional().is_err());
}

#[test]
fn optional_integration_with_query_row() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER)").unwrap();

    // Missing row → None via optional
    let row = conn
        .query_row_map("SELECT id FROM t WHERE id = 999", &[], |r| {
            r.get_typed::<i64>(0)
        })
        .optional()
        .unwrap();
    assert!(row.is_none());

    // Existing row → Some
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let row = conn
        .query_row_map("SELECT id FROM t WHERE id = 1", &[], |r| {
            r.get_typed::<i64>(0)
        })
        .optional()
        .unwrap();
    assert_eq!(row, Some(1));
}

// ===========================================================================
// 8. OPEN FLAGS
// ===========================================================================

#[test]
fn default_flags_contain_read_write_and_create() {
    let flags = OpenFlags::default_flags();
    assert!(flags.contains(OpenFlags::SQLITE_OPEN_READ_WRITE));
    assert!(flags.contains(OpenFlags::SQLITE_OPEN_CREATE));
}

#[test]
fn bitor_combines_flags() {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    assert!(flags.contains(OpenFlags::SQLITE_OPEN_READ_WRITE));
    assert!(flags.contains(OpenFlags::SQLITE_OPEN_CREATE));
}

#[test]
fn open_with_flags_in_memory() {
    let conn = open_with_flags(":memory:", OpenFlags::default_flags()).unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let rows = conn.query("SELECT x FROM t").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn open_with_flags_read_write_creates_db() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let path_str = path.to_str().unwrap();

    let conn = open_with_flags(
        path_str,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )
    .unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
    drop(conn);

    assert!(path.exists(), "database file should be created");
}

#[test]
fn open_with_flags_read_write_without_create_does_not_create_missing_db() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("missing.db");

    let error = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
        .expect_err("READ_WRITE without CREATE must reject a missing database");

    assert!(matches!(error, FrankenError::CannotOpen { .. }));
    assert!(!path.exists(), "failed open must not create the database");
}

#[test]
fn open_with_flags_read_write_without_create_preserves_empty_db() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("empty.db");
    std::fs::write(&path, []).unwrap();
    let before = std::fs::read(&path).unwrap();

    let error = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
        .expect_err("READ_WRITE without CREATE must not initialize an empty database");

    assert!(matches!(error, FrankenError::CannotOpen { .. }));
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[test]
fn open_with_flags_read_write_without_create_preserves_malformed_db() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("malformed.db");
    let before = b"not a sqlite database".to_vec();
    std::fs::write(&path, &before).unwrap();

    let error = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
        .expect_err("READ_WRITE without CREATE must reject a malformed database");

    assert!(matches!(error, FrankenError::DatabaseCorrupt { .. }));
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[test]
fn open_with_flags_read_write_without_create_opens_existing_valid_db() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("existing.db");
    {
        let seed = RusqliteConnection::open(&path).unwrap();
        seed.execute("CREATE TABLE t(x INTEGER)", []).unwrap();
        seed.execute("INSERT INTO t VALUES (1)", []).unwrap();
    }

    let conn = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
        .expect("READ_WRITE without CREATE should open an existing valid database");
    let row = conn.query_row("SELECT x FROM t").unwrap();
    assert_eq!(row.get(0), Some(&SqliteValue::Integer(1)));
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
}

#[test]
fn open_with_flags_read_only_supports_datetime_builtin() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("readonly_datetime.db");
    let path_str = path.to_str().unwrap();

    let writable = open_with_flags(
        path_str,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )
    .unwrap();
    writable.execute("CREATE TABLE t(x INTEGER)").unwrap();
    writable.execute("INSERT INTO t VALUES (1)").unwrap();
    drop(writable);

    let readonly = open_with_flags(path_str, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let row = readonly.query_row("SELECT datetime('now')").unwrap();
    let value: String = row.get_typed(0).unwrap();
    assert!(
        !value.is_empty(),
        "datetime('now') should return a non-empty timestamp on read-only compat connections"
    );
}

#[test]
fn open_with_flags_accepts_common_sqlite_ancillary_flags_like_rusqlite() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("ancillary_compat.db");
    let path_str = path.to_str().unwrap();

    let oracle = RusqliteConnection::open_with_flags(
        path_str,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
            | rusqlite::OpenFlags::SQLITE_OPEN_URI
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
            | rusqlite::OpenFlags::SQLITE_OPEN_PRIVATE_CACHE,
    )
    .unwrap();
    oracle.execute("CREATE TABLE t(x INTEGER)", []).unwrap();
    drop(oracle);

    let conn = open_with_flags(
        path_str,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE,
    )
    .unwrap();
    let row = conn
        .query_row("SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 't'")
        .unwrap();
    let table_name: String = row.get_typed(0).unwrap();
    assert_eq!(table_name, "t");
}

// ===========================================================================
// 9. PARAMS_FROM_ITER
// ===========================================================================

#[test]
fn params_from_iter_vec_of_i64() {
    let values = params_from_iter(vec![1_i64, 2, 3]);
    assert_eq!(values.len(), 3);
    assert_eq!(values[0], SqliteValue::Integer(1));
    assert_eq!(values[1], SqliteValue::Integer(2));
    assert_eq!(values[2], SqliteValue::Integer(3));
}

#[test]
fn params_from_iter_empty() {
    let values = params_from_iter(std::iter::empty::<i64>());
    assert!(values.is_empty());
}

#[test]
fn param_slice_to_values_converts_correctly() {
    let p = params![42_i64, "text"];
    let values = param_slice_to_values(p);
    assert_eq!(values[0], SqliteValue::Integer(42));
    assert_eq!(values[1], SqliteValue::Text("text".into()));
}

#[test]
fn param_slice_to_values_with_overflow_preserves_exact_value() {
    let p = [ParamValue::from(u64::MAX)];
    let values = param_slice_to_values(&p);
    assert_eq!(values[0], SqliteValue::Text(u64::MAX.to_string().into()));
}

// ===========================================================================
// 10. END-TO-END: Full round-trip with compat layer
// ===========================================================================

#[test]
fn full_compat_round_trip() {
    let conn = Connection::open(":memory:").unwrap();

    // Schema setup via batch
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            active INTEGER DEFAULT 1
         );",
    )
    .unwrap();

    // Insert via execute_params
    conn.execute_compat(
        "INSERT INTO users (id, name, email, active) VALUES (?1, ?2, ?3, ?4)",
        params![1_i64, "Alice", "alice@example.com", true],
    )
    .unwrap();

    conn.execute_compat(
        "INSERT INTO users (id, name, email, active) VALUES (?1, ?2, ?3, ?4)",
        params![2_i64, "Bob", None::<String>, false],
    )
    .unwrap();

    // Query via query_row_map
    let name: String = conn
        .query_row_map(
            "SELECT name FROM users WHERE id = ?1",
            params![1_i64],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(name, "Alice");

    // Query via query_map_collect
    let names: Vec<String> = conn
        .query_map_collect(
            "SELECT name FROM users WHERE active = ?1 ORDER BY name",
            params![true],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(names, vec!["Alice"]);

    // Optional for missing row
    let missing = conn
        .query_row_map(
            "SELECT name FROM users WHERE id = ?1",
            params![999_i64],
            |row| row.get_typed::<String>(0),
        )
        .optional()
        .unwrap();
    assert!(missing.is_none());

    // NULL handling
    let email: Option<String> = conn
        .query_row_map(
            "SELECT email FROM users WHERE id = ?1",
            params![2_i64],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert!(email.is_none());

    // Transaction: insert + rollback
    {
        let tx = conn.transaction().unwrap();
        tx.execute_compat(
            "INSERT INTO users (id, name) VALUES (?1, ?2)",
            params![3_i64, "Charlie"],
        )
        .unwrap();
        // drop without commit → rollback
    }

    let count: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM users", &[], |row| row.get_typed(0))
        .unwrap();
    assert_eq!(count, 2, "Charlie should have been rolled back");

    // Transaction: insert + commit
    {
        let mut tx = conn.transaction().unwrap();
        tx.execute_compat(
            "INSERT INTO users (id, name) VALUES (?1, ?2)",
            params![3_i64, "Charlie"],
        )
        .unwrap();
        tx.commit().unwrap();
    }

    let count: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM users", &[], |row| row.get_typed(0))
        .unwrap();
    assert_eq!(count, 3, "Charlie should be committed");
}

#[test]
fn multi_row_upsert_with_foreign_keys_uses_fallback_without_failing() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute_batch(
        "PRAGMA foreign_keys = ON;
         CREATE TABLE parent (
             id INTEGER PRIMARY KEY
         );
         CREATE TABLE child (
             id INTEGER PRIMARY KEY,
             parent_id INTEGER NOT NULL REFERENCES parent(id),
             value INTEGER NOT NULL
         );
         INSERT INTO parent (id) VALUES (1), (2);
         INSERT INTO child (id, parent_id, value) VALUES (1, 1, 10);",
    )
    .unwrap();

    conn.execute_compat(
        "INSERT INTO child (id, parent_id, value) VALUES (?1, ?2, ?3), (?4, ?5, ?6)
         ON CONFLICT(id) DO UPDATE SET value = child.value + excluded.value",
        params![1_i64, 1_i64, 5_i64, 2_i64, 2_i64, 7_i64],
    )
    .unwrap();

    let rows: Vec<(i64, i64)> = conn
        .query_map_collect(
            "SELECT id, value FROM child ORDER BY id",
            params![],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(rows, vec![(1, 15), (2, 7)]);
}

#[test]
fn upsert_do_update_after_leaf_split_does_not_double_free_page() {
    fn padded_json(len: usize) -> String {
        format!("{{\"d\":\"{}\"}}", "x".repeat(len - 8))
    }

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("upsert_leaf_split.db");
    let db = db_path.to_str().unwrap();
    let shape_class = format!("roofline-v6:1:run={}:op=1", "a".repeat(64));
    let machine: std::sync::Arc<[u8]> = vec![0_u8; 40].into();
    let updated_kernel = "simd-axpy-f64";
    let kernels = [updated_kernel, "simd-dot-f64", "simd-sum-f64"];
    let measured_lengths = [1_265_usize, 1_264, 1_265];

    {
        let conn = Connection::open(db).unwrap();
        conn.execute(
            "CREATE TABLE tune(
                kernel TEXT NOT NULL,
                shape_class TEXT NOT NULL,
                machine BLOB NOT NULL,
                params TEXT NOT NULL,
                measured TEXT NOT NULL,
                PRIMARY KEY(kernel, shape_class, machine)
            ) STRICT",
        )
        .unwrap();

        let insert = conn
            .prepare(
                "INSERT INTO tune(kernel, shape_class, machine, params, measured)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(kernel, shape_class, machine) DO NOTHING",
            )
            .unwrap();
        for (kernel, measured_len) in kernels.iter().zip(measured_lengths) {
            insert
                .execute_with_params(&[
                    SqliteValue::Text((*kernel).into()),
                    SqliteValue::Text(shape_class.clone().into()),
                    SqliteValue::Blob(std::sync::Arc::clone(&machine)),
                    SqliteValue::Text(padded_json(574).into()),
                    SqliteValue::Text(padded_json(measured_len).into()),
                ])
                .unwrap();
        }

        let update = conn
            .prepare(
                "INSERT INTO tune(kernel, shape_class, machine, params, measured)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(kernel, shape_class, machine)
                 DO UPDATE SET params = excluded.params, measured = excluded.measured",
            )
            .unwrap();
        assert_eq!(
            update
                .execute_with_params(&[
                    SqliteValue::Text(updated_kernel.into()),
                    SqliteValue::Text(shape_class.clone().into()),
                    SqliteValue::Blob(std::sync::Arc::clone(&machine)),
                    SqliteValue::Text(padded_json(574).into()),
                    SqliteValue::Text("{}".into()),
                ])
                .unwrap(),
            1
        );

        let rows = conn
            .query("SELECT kernel, measured FROM tune ORDER BY kernel")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get(1), Some(&SqliteValue::Text("{}".into())));
    }

    let stock = RusqliteConnection::open(&db_path).unwrap();
    let integrity: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    let row_count: i64 = stock
        .query_row("SELECT COUNT(*) FROM tune", [], |row| row.get(0))
        .unwrap();
    assert_eq!(row_count, 3);

    let mut statement = stock
        .prepare(
            "SELECT kernel, shape_class, machine, params, measured
             FROM tune ORDER BY kernel",
        )
        .unwrap();
    let stock_rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected_rows = kernels
        .iter()
        .zip(measured_lengths)
        .map(|(kernel, measured_len)| {
            (
                (*kernel).to_owned(),
                shape_class.clone(),
                machine.as_ref().to_vec(),
                padded_json(574),
                if *kernel == updated_kernel {
                    "{}".to_owned()
                } else {
                    padded_json(measured_len)
                },
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(stock_rows, expected_rows);
}

// ===========================================================================
// 11. RUSQLITE PARITY (golden tests)
// ===========================================================================

/// Execute the same SQL operations via both rusqlite and frankensqlite,
/// compare results for parity.
mod rusqlite_parity {
    use super::*;
    use fsqlite_func::ScalarFunction;

    fn assert_parity(
        label: &str,
        rusqlite_result: Vec<Vec<String>>,
        franken_result: Vec<Vec<String>>,
    ) {
        assert_eq!(
            rusqlite_result.len(),
            franken_result.len(),
            "{label}: row count mismatch ({} vs {})",
            rusqlite_result.len(),
            franken_result.len()
        );
        for (i, (r, f)) in rusqlite_result.iter().zip(&franken_result).enumerate() {
            assert_eq!(r, f, "{label}: row {i} mismatch");
        }
    }

    fn sqlite_val_to_string(val: &SqliteValue) -> String {
        match val {
            SqliteValue::Null => "NULL".to_string(),
            SqliteValue::Integer(i) => i.to_string(),
            SqliteValue::Float(f) => format!("{f}"),
            SqliteValue::Text(s) => s.to_string(),
            SqliteValue::Blob(b) => format!("{b:?}"),
        }
    }

    fn rusqlite_query_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
        let mut stmt = conn.prepare(sql).unwrap();
        let column_count = stmt.column_count();
        stmt.query_map([], |row| {
            let mut values = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                let value = match row.get_ref(idx).unwrap() {
                    rusqlite::types::ValueRef::Null => "NULL".to_string(),
                    rusqlite::types::ValueRef::Integer(i) => i.to_string(),
                    rusqlite::types::ValueRef::Real(f) => format!("{f}"),
                    rusqlite::types::ValueRef::Text(bytes) => {
                        String::from_utf8_lossy(bytes).into_owned()
                    }
                    rusqlite::types::ValueRef::Blob(bytes) => format!("{bytes:?}"),
                };
                values.push(value);
            }
            Ok(values)
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
    }

    fn franken_query_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
        conn.query(sql)
            .unwrap()
            .iter()
            .map(|row| row.values().iter().map(sqlite_val_to_string).collect())
            .collect()
    }

    fn assert_query_parity(
        label: &str,
        rconn: &rusqlite::Connection,
        fconn: &Connection,
        sql: &str,
    ) {
        assert_parity(
            label,
            rusqlite_query_rows(rconn, sql),
            franken_query_rows(fconn, sql),
        );
    }

    fn setup_rusqlite() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE msgs (
                id INTEGER PRIMARY KEY,
                agent TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT,
                ts INTEGER NOT NULL
             );
             INSERT INTO msgs VALUES (1, 'claude', 'user', 'fix the auth bug', 1700000000);
             INSERT INTO msgs VALUES (2, 'claude', 'assistant', 'I found the issue', 1700000001);
             INSERT INTO msgs VALUES (3, 'codex', 'user', 'add a feature', 1700000002);
             INSERT INTO msgs VALUES (4, 'codex', 'assistant', NULL, 1700000003);
             INSERT INTO msgs VALUES (5, 'claude', 'user', 'deploy to prod', 1700000004);",
        )
        .unwrap();
        conn
    }

    fn setup_franken() -> Connection {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute_batch(
            "CREATE TABLE msgs (
                id INTEGER PRIMARY KEY,
                agent TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT,
                ts INTEGER NOT NULL
             );
             INSERT INTO msgs VALUES (1, 'claude', 'user', 'fix the auth bug', 1700000000);
             INSERT INTO msgs VALUES (2, 'claude', 'assistant', 'I found the issue', 1700000001);
             INSERT INTO msgs VALUES (3, 'codex', 'user', 'add a feature', 1700000002);
             INSERT INTO msgs VALUES (4, 'codex', 'assistant', NULL, 1700000003);
             INSERT INTO msgs VALUES (5, 'claude', 'user', 'deploy to prod', 1700000004);",
        )
        .unwrap();
        conn
    }

    struct ForceMatch;

    impl ScalarFunction for ForceMatch {
        fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
            let [pattern, value] = args else {
                return Err(FrankenError::FunctionError(
                    "match() expects exactly two arguments".to_owned(),
                ));
            };
            if matches!(pattern, SqliteValue::Null) || matches!(value, SqliteValue::Null) {
                return Ok(SqliteValue::Null);
            }
            let pattern = pattern.to_text();
            Ok(SqliteValue::Integer(i64::from(
                pattern == "force-real-path",
            )))
        }

        fn num_args(&self) -> i32 {
            2
        }

        fn name(&self) -> &str {
            "match"
        }
    }

    #[test]
    fn parity_select_all() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        let r: Vec<Vec<String>> = rconn
            .prepare("SELECT id, agent FROM msgs ORDER BY id")
            .unwrap()
            .query_map([], |row| {
                Ok(vec![
                    row.get::<_, i64>(0).unwrap().to_string(),
                    row.get::<_, String>(1).unwrap(),
                ])
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        let f: Vec<Vec<String>> = fconn
            .query("SELECT id, agent FROM msgs ORDER BY id")
            .unwrap()
            .iter()
            .map(|row| {
                vec![
                    sqlite_val_to_string(row.get(0).unwrap()),
                    sqlite_val_to_string(row.get(1).unwrap()),
                ]
            })
            .collect();

        assert_parity("SELECT_ALL", r, f);
    }

    #[test]
    fn parity_where_clause() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        let r: Vec<Vec<String>> = rconn
            .prepare("SELECT id, content FROM msgs WHERE agent = 'claude' ORDER BY id")
            .unwrap()
            .query_map([], |row| {
                Ok(vec![
                    row.get::<_, i64>(0).unwrap().to_string(),
                    row.get::<_, String>(1).unwrap(),
                ])
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        let f: Vec<Vec<String>> = fconn
            .query("SELECT id, content FROM msgs WHERE agent = 'claude' ORDER BY id")
            .unwrap()
            .iter()
            .map(|row| {
                vec![
                    sqlite_val_to_string(row.get(0).unwrap()),
                    sqlite_val_to_string(row.get(1).unwrap()),
                ]
            })
            .collect();

        assert_parity("WHERE_CLAUSE", r, f);
    }

    #[test]
    fn parity_count_aggregate() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        let r_count: i64 = rconn
            .query_row("SELECT COUNT(*) FROM msgs", [], |row| row.get(0))
            .unwrap();
        let f_rows = fconn.query("SELECT COUNT(*) FROM msgs").unwrap();
        let f_count = match f_rows[0].get(0).unwrap() {
            SqliteValue::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        };
        assert_eq!(r_count, f_count, "COUNT parity");
    }

    #[test]
    fn parity_group_by() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        let r: Vec<Vec<String>> = rconn
            .prepare("SELECT agent, COUNT(*) as cnt FROM msgs GROUP BY agent ORDER BY agent")
            .unwrap()
            .query_map([], |row| {
                Ok(vec![
                    row.get::<_, String>(0).unwrap(),
                    row.get::<_, i64>(1).unwrap().to_string(),
                ])
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        let f: Vec<Vec<String>> = fconn
            .query("SELECT agent, COUNT(*) as cnt FROM msgs GROUP BY agent ORDER BY agent")
            .unwrap()
            .iter()
            .map(|row| {
                vec![
                    sqlite_val_to_string(row.get(0).unwrap()),
                    sqlite_val_to_string(row.get(1).unwrap()),
                ]
            })
            .collect();

        assert_parity("GROUP_BY", r, f);
    }

    #[test]
    fn parity_null_handling() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        // IS NULL
        let r_null: i64 = rconn
            .query_row(
                "SELECT COUNT(*) FROM msgs WHERE content IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let f_rows = fconn
            .query("SELECT COUNT(*) FROM msgs WHERE content IS NULL")
            .unwrap();
        let f_null = match f_rows[0].get(0).unwrap() {
            SqliteValue::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        };
        assert_eq!(r_null, f_null, "IS NULL parity");
    }

    #[test]
    fn parity_like_operator() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        let r_count: i64 = rconn
            .query_row(
                "SELECT COUNT(*) FROM msgs WHERE content LIKE '%bug%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let f_rows = fconn
            .query("SELECT COUNT(*) FROM msgs WHERE content LIKE '%bug%'")
            .unwrap();
        let f_count = match f_rows[0].get(0).unwrap() {
            SqliteValue::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        };
        assert_eq!(r_count, f_count, "LIKE parity");
    }

    #[test]
    fn parity_update() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        rconn
            .execute("UPDATE msgs SET content = 'updated' WHERE id = 1", [])
            .unwrap();
        fconn
            .execute("UPDATE msgs SET content = 'updated' WHERE id = 1")
            .unwrap();

        let r_val: String = rconn
            .query_row("SELECT content FROM msgs WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        let f_rows = fconn
            .query("SELECT content FROM msgs WHERE id = 1")
            .unwrap();
        let f_val = sqlite_val_to_string(f_rows[0].get(0).unwrap());

        assert_eq!(r_val, f_val, "UPDATE parity");
    }

    #[test]
    fn parity_delete() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        rconn.execute("DELETE FROM msgs WHERE id = 3", []).unwrap();
        fconn.execute("DELETE FROM msgs WHERE id = 3").unwrap();

        let r_count: i64 = rconn
            .query_row("SELECT COUNT(*) FROM msgs", [], |row| row.get(0))
            .unwrap();
        let f_rows = fconn.query("SELECT COUNT(*) FROM msgs").unwrap();
        let f_count = match f_rows[0].get(0).unwrap() {
            SqliteValue::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        };
        assert_eq!(r_count, f_count, "DELETE parity");
    }

    #[test]
    fn parity_subquery() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        let r_count: i64 = rconn
            .query_row(
                "SELECT COUNT(*) FROM msgs WHERE ts > (SELECT AVG(ts) FROM msgs)",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let f_rows = fconn
            .query("SELECT COUNT(*) FROM msgs WHERE ts > (SELECT AVG(ts) FROM msgs)")
            .unwrap();
        let f_count = match f_rows[0].get(0).unwrap() {
            SqliteValue::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        };
        assert_eq!(r_count, f_count, "SUBQUERY parity");
    }

    #[test]
    fn parity_coalesce() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        let r: Vec<Vec<String>> = rconn
            .prepare("SELECT id, COALESCE(content, '<empty>') FROM msgs ORDER BY id")
            .unwrap()
            .query_map([], |row| {
                Ok(vec![
                    row.get::<_, i64>(0).unwrap().to_string(),
                    row.get::<_, String>(1).unwrap(),
                ])
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        let f: Vec<Vec<String>> = fconn
            .query("SELECT id, COALESCE(content, '<empty>') FROM msgs ORDER BY id")
            .unwrap()
            .iter()
            .map(|row| {
                vec![
                    sqlite_val_to_string(row.get(0).unwrap()),
                    sqlite_val_to_string(row.get(1).unwrap()),
                ]
            })
            .collect();

        assert_parity("COALESCE", r, f);
    }

    #[test]
    fn parity_compound_select_set_operators() {
        let rconn = setup_rusqlite();
        let fconn = setup_franken();

        assert_query_parity(
            "COMPOUND_UNION_DISTINCT",
            &rconn,
            &fconn,
            "SELECT agent FROM msgs WHERE role = 'user'
             UNION
             SELECT agent FROM msgs WHERE content IS NULL
             ORDER BY agent",
        );
        assert_query_parity(
            "COMPOUND_UNION_ALL_MULTIPLICITY",
            &rconn,
            &fconn,
            "SELECT agent FROM msgs WHERE role = 'user'
             UNION ALL
             SELECT agent FROM msgs WHERE agent = 'codex'
             ORDER BY agent",
        );
        assert_query_parity(
            "COMPOUND_INTERSECT",
            &rconn,
            &fconn,
            "SELECT agent FROM msgs WHERE role = 'user'
             INTERSECT
             SELECT agent FROM msgs WHERE content IS NOT NULL
             ORDER BY agent",
        );
        assert_query_parity(
            "COMPOUND_EXCEPT",
            &rconn,
            &fconn,
            "SELECT agent FROM msgs
             EXCEPT
             SELECT agent FROM msgs WHERE content IS NULL
             ORDER BY agent",
        );
    }

    #[test]
    fn parity_match_udf_uses_registered_scalar_path() {
        let fconn = setup_franken();
        fconn.register_scalar_function(ForceMatch);

        assert_query_parity(
            "MATCH_UDF_REAL_PATH",
            &rusqlite_with_forced_match(),
            &fconn,
            "SELECT id FROM msgs WHERE content MATCH 'force-real-path' ORDER BY id",
        );
    }

    #[test]
    fn parity_prepared_match_udf_uses_registered_scalar_path() {
        let fconn = setup_franken();
        fconn.register_scalar_function(ForceMatch);

        let stmt = fconn
            .prepare("SELECT id FROM msgs WHERE content MATCH ?1 ORDER BY id")
            .unwrap();
        let rows = stmt
            .query_with_params(&[SqliteValue::Text("force-real-path".into())])
            .unwrap();
        let ids: Vec<String> = rows
            .iter()
            .map(|row| sqlite_val_to_string(row.get(0).unwrap()))
            .collect();
        assert_eq!(ids, vec!["1", "2", "3", "5"]);
    }

    fn rusqlite_with_forced_match() -> rusqlite::Connection {
        let conn = setup_rusqlite();
        conn.create_scalar_function(
            "match",
            2,
            rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
            |ctx| {
                let pattern = ctx.get::<String>(0)?;
                let value = ctx.get::<Option<String>>(1)?;
                Ok(i64::from(pattern == "force-real-path" && value.is_some()))
            },
        )
        .unwrap();
        conn
    }

    #[test]
    fn parity_sqlite_created_integer_primary_key_rows_do_not_shift_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("projects.db");

        {
            let conn = RusqliteConnection::open(&db_path).unwrap();
            conn.execute_batch(
                "
                CREATE TABLE projects(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL UNIQUE,
                    human_key TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX idx_projects_human_key ON projects(human_key);
                CREATE INDEX idx_projects_created_id_desc ON projects(created_at DESC, id DESC);
                ",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO projects(slug, human_key, created_at) VALUES (?1, ?2, ?3)",
                rusqlite::params!["slug-001", "/path/001", 1_773_076_744_605_941_i64],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO projects(slug, human_key, created_at) VALUES (?1, ?2, ?3)",
                rusqlite::params!["slug-002", "/path/002", 1_773_076_744_605_942_i64],
            )
            .unwrap();
        }

        let conn = Connection::open(db_path.to_str().unwrap()).unwrap();
        let rows = conn
            .query("SELECT id, slug, human_key, created_at FROM projects ORDER BY id")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values(),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("slug-001".into()),
                SqliteValue::Text("/path/001".into()),
                SqliteValue::Integer(1_773_076_744_605_941),
            ]
        );
        assert_eq!(
            rows[1].values(),
            vec![
                SqliteValue::Integer(2),
                SqliteValue::Text("slug-002".into()),
                SqliteValue::Text("/path/002".into()),
                SqliteValue::Integer(1_773_076_744_605_942),
            ]
        );

        let row = conn
            .query_row(
                "SELECT id, slug, human_key, created_at FROM projects WHERE slug = 'slug-002'",
            )
            .unwrap();
        assert_eq!(
            row.values(),
            vec![
                SqliteValue::Integer(2),
                SqliteValue::Text("slug-002".into()),
                SqliteValue::Text("/path/002".into()),
                SqliteValue::Integer(1_773_076_744_605_942),
            ]
        );
    }

    #[test]
    fn parity_sqlite_created_cursor_state_vscdb_reads_with_frankensqlite() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.vscdb");
        let composer_json = r#"{"createdAt":1700000000000,"tabs":[{"bubbles":[{"type":"user","text":"How do I sort a Vec?"},{"type":"ai","text":"Use .sort()."}]}]}"#;
        let legacy_json = r#"{"kind":"legacy"}"#;

        {
            let conn = RusqliteConnection::open(&db_path).unwrap();
            conn.execute_batch(
                "
                PRAGMA journal_mode=WAL;
                CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT);
                ",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO cursorDiskKV(key, value) VALUES (?1, ?2)",
                rusqlite::params!["composerData:comp-001", composer_json],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO ItemTable(key, value) VALUES (?1, ?2)",
                rusqlite::params!["workbench.panel.aichat.view.aichat.chatdata", legacy_json],
            )
            .unwrap();
        }

        let conn = Connection::open(db_path.to_str().unwrap()).unwrap();
        let composer_rows = conn
            .query_with_params(
                "SELECT key, value FROM cursorDiskKV WHERE key >= ?1 AND key < ?2 ORDER BY key",
                &[
                    SqliteValue::Text("composerData:".into()),
                    SqliteValue::Text("composerData;".into()),
                ],
            )
            .unwrap();
        assert_eq!(composer_rows.len(), 1);
        assert_eq!(
            composer_rows[0].values(),
            vec![
                SqliteValue::Text("composerData:comp-001".into()),
                SqliteValue::Text(composer_json.into()),
            ]
        );

        let legacy_rows = conn
            .query(
                "SELECT key, value FROM ItemTable WHERE key LIKE '%aichat%chatdata%' OR key LIKE '%composer%' ORDER BY key",
            )
            .unwrap();
        assert_eq!(legacy_rows.len(), 1);
        assert_eq!(
            legacy_rows[0].values(),
            vec![
                SqliteValue::Text("workbench.panel.aichat.view.aichat.chatdata".into()),
                SqliteValue::Text(legacy_json.into()),
            ]
        );
    }
}
