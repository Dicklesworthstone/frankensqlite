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

#[cfg(all(feature = "native", any(unix, windows)))]
#[derive(Debug, PartialEq, Eq)]
struct FileSnapshot {
    bytes: Vec<u8>,
    modified: std::time::SystemTime,
}

#[cfg(all(feature = "native", any(unix, windows)))]
fn snapshot_directory_files(
    directory: &std::path::Path,
) -> std::collections::BTreeMap<std::ffi::OsString, FileSnapshot> {
    std::fs::read_dir(directory)
        .expect("list artifact directory")
        .map(|entry| {
            let entry = entry.expect("read artifact entry");
            let path = entry.path();
            let metadata = entry.metadata().expect("read artifact metadata");
            assert!(
                metadata.is_file(),
                "unexpected non-file artifact: {}",
                path.display()
            );
            (
                entry.file_name(),
                FileSnapshot {
                    bytes: std::fs::read(&path).expect("read artifact bytes"),
                    modified: metadata
                        .modified()
                        .expect("read artifact modification time"),
                },
            )
        })
        .collect()
}

#[cfg(all(feature = "native", any(unix, windows)))]
fn suffixed_path(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut suffixed = path.as_os_str().to_owned();
    suffixed.push(suffix);
    std::path::PathBuf::from(suffixed)
}

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
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val INTEGER)").await.unwrap();
        conn.execute("INSERT INTO t VALUES (42)").await.unwrap();
        let row = conn.query_row("SELECT val FROM t").await.unwrap();
        let v: i64 = row.get_typed(0).unwrap();
        assert_eq!(v, 42);
    });
}

#[test]
fn row_get_typed_string() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val TEXT)").await.unwrap();
        conn.execute("INSERT INTO t VALUES ('hello')")
            .await
            .unwrap();
        let row = conn.query_row("SELECT val FROM t").await.unwrap();
        let v: String = row.get_typed(0).unwrap();
        assert_eq!(v, "hello");
    });
}

#[test]
fn row_get_typed_option_null() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val TEXT)").await.unwrap();
        conn.execute_with_params("INSERT INTO t VALUES (?1)", &[SqliteValue::Null])
            .await
            .unwrap();
        let row = conn.query_row("SELECT val FROM t").await.unwrap();
        let v: Option<String> = row.get_typed(0).unwrap();
        assert!(v.is_none());
    });
}

#[test]
fn row_get_typed_f64_from_integer_coercion() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val INTEGER)").await.unwrap();
        conn.execute("INSERT INTO t VALUES (7)").await.unwrap();
        let row = conn.query_row("SELECT val FROM t").await.unwrap();
        let v: f64 = row.get_typed(0).unwrap();
        assert!((v - 7.0).abs() < f64::EPSILON);
    });
}

#[test]
fn row_get_typed_bool() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val INTEGER)").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let row = conn.query_row("SELECT val FROM t").await.unwrap();
        let v: bool = row.get_typed(0).unwrap();
        assert!(v);
    });
}

// ===========================================================================
// 4. QUERY WITH CLOSURE (ConnectionExt)
// ===========================================================================

#[test]
fn query_row_map_returns_closure_result() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER, name TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice')")
            .await
            .unwrap();

        let name: String = conn
            .query_row_map("SELECT id, name FROM t", &[], |row| row.get_typed(1))
            .await
            .unwrap();
        assert_eq!(name, "alice");
    });
}

#[test]
fn query_row_map_with_params() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER, name TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alice')")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (2, 'bob')")
            .await
            .unwrap();

        let name: String = conn
            .query_row_map("SELECT name FROM t WHERE id = ?1", params![2_i64], |row| {
                row.get_typed(0)
            })
            .await
            .unwrap();
        assert_eq!(name, "bob");
    });
}

#[test]
fn query_row_map_empty_returns_error() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER)").await.unwrap();

        let result: Result<i64, _> = conn
            .query_row_map("SELECT id FROM t WHERE id = 999", &[], |row| {
                row.get_typed(0)
            })
            .await;
        assert!(matches!(result, Err(FrankenError::QueryReturnedNoRows)));
    });
}

#[test]
fn query_map_collect_returns_vec() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val INTEGER)").await.unwrap();
        conn.execute("INSERT INTO t VALUES (10)").await.unwrap();
        conn.execute("INSERT INTO t VALUES (20)").await.unwrap();
        conn.execute("INSERT INTO t VALUES (30)").await.unwrap();

        let vals: Vec<i64> = conn
            .query_map_collect("SELECT val FROM t ORDER BY val", &[], |row| {
                row.get_typed(0)
            })
            .await
            .unwrap();
        assert_eq!(vals, vec![10, 20, 30]);
    });
}

#[test]
fn query_map_collect_empty_returns_empty_vec() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER)").await.unwrap();

        let vals: Vec<i64> = conn
            .query_map_collect("SELECT id FROM t WHERE id > 999", &[], |row| {
                row.get_typed(0)
            })
            .await
            .unwrap();
        assert!(vals.is_empty());
    });
}

#[test]
fn execute_params_inserts_rows() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER, val TEXT)")
            .await
            .unwrap();

        let changed = conn
            .execute_compat("INSERT INTO t VALUES (?1, ?2)", params![1_i64, "hello"])
            .await
            .unwrap();
        assert_eq!(changed, 1);

        let row = conn
            .query_row("SELECT val FROM t WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(row.get(0).unwrap(), &SqliteValue::Text("hello".into()));
    });
}

// ===========================================================================
// 5. EXECUTE_BATCH
// ===========================================================================

#[test]
fn execute_batch_multi_statement() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch(
            "CREATE TABLE a (id INTEGER PRIMARY KEY);
         CREATE TABLE b (id INTEGER PRIMARY KEY);
         INSERT INTO a VALUES (1);
         INSERT INTO b VALUES (2);",
        )
        .await
        .unwrap();

        let rows_a = conn.query("SELECT COUNT(*) FROM a").await.unwrap();
        assert_eq!(rows_a[0].get(0).unwrap(), &SqliteValue::Integer(1));

        let rows_b = conn.query("SELECT COUNT(*) FROM b").await.unwrap();
        assert_eq!(rows_b[0].get(0).unwrap(), &SqliteValue::Integer(1));
    });
}

#[test]
fn execute_batch_empty_string_is_noop() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch("").await.unwrap();
    });
}

#[test]
fn execute_batch_single_statement_without_semicolon() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch("CREATE TABLE t(x INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let rows = conn.query("SELECT COUNT(*) FROM t").await.unwrap();
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1));
    });
}

#[test]
fn execute_batch_with_comments() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch(
            "-- This is a comment
         CREATE TABLE t(x INTEGER);
         /* Block comment */
         INSERT INTO t VALUES (42);",
        )
        .await
        .unwrap();

        let rows = conn.query("SELECT x FROM t").await.unwrap();
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(42));
    });
}

#[test]
fn execute_batch_pragma_blocks() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
         PRAGMA cache_size=-4000;
         PRAGMA synchronous=NORMAL;",
        )
        .await
        .unwrap();
        // If no error, PRAGMAs were applied successfully.
    });
}

// ===========================================================================
// 6. TRANSACTION
// ===========================================================================

#[test]
fn transaction_commit_persists_data() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val TEXT)").await.unwrap();

        {
            let mut tx = conn.transaction().await.unwrap();
            tx.execute("INSERT INTO t VALUES ('committed')")
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }

        let rows = conn.query("SELECT val FROM t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(0).unwrap(),
            &SqliteValue::Text("committed".into())
        );
    });
}

#[test]
fn transaction_drop_without_commit_rolls_back() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val TEXT)").await.unwrap();

        {
            let tx = conn.transaction().await.unwrap();
            tx.execute("INSERT INTO t VALUES ('dropped')")
                .await
                .unwrap();
            // tx dropped without commit
        }

        let rows = conn.query("SELECT val FROM t").await.unwrap();
        assert!(rows.is_empty());
    });
}

#[test]
fn transaction_explicit_rollback() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val TEXT)").await.unwrap();

        let mut tx = conn.transaction().await.unwrap();
        tx.execute("INSERT INTO t VALUES ('rolled_back')")
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        let rows = conn.query("SELECT val FROM t").await.unwrap();
        assert!(rows.is_empty());
    });
}

#[test]
fn transaction_execute_with_params() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER, val TEXT)")
            .await
            .unwrap();

        {
            let mut tx = conn.transaction().await.unwrap();
            tx.execute_with_params(
                "INSERT INTO t VALUES (?1, ?2)",
                &[SqliteValue::Integer(1), SqliteValue::Text("in_tx".into())],
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }

        let rows = conn.query("SELECT val FROM t WHERE id = 1").await.unwrap();
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Text("in_tx".into()));
    });
}

#[test]
fn transaction_query_within() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(val INTEGER)").await.unwrap();
        conn.execute("INSERT INTO t VALUES (42)").await.unwrap();

        let mut tx = conn.transaction().await.unwrap();
        let rows = tx.query("SELECT val FROM t").await.unwrap();
        assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(42));
        tx.commit().await.unwrap();
    });
}

#[test]
fn transaction_execute_params_compat() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER, val TEXT)")
            .await
            .unwrap();

        let mut tx = conn.transaction().await.unwrap();
        tx.execute_compat(
            "INSERT INTO t VALUES (?1, ?2)",
            params![1_i64, "via_params"],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let row = conn
            .query_row("SELECT val FROM t WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(row.get(0).unwrap(), &SqliteValue::Text("via_params".into()));
    });
}

#[test]
fn transaction_failed_commit_keeps_transaction_open_for_explicit_rollback() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("compat_failed_commit_keeps_tx_open.db");
        let db = db_path.to_string_lossy().to_string();

        {
            let conn = Connection::open(&db).await.unwrap();
            conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 0);").await.unwrap();
        }

        let conn_a = Connection::open(&db).await.unwrap();
        let conn_b = Connection::open(&db).await.unwrap();
        for conn in [&conn_a, &conn_b] {
            conn.execute("PRAGMA busy_timeout=5000;").await.unwrap();
            conn.execute("PRAGMA fsqlite.concurrent_mode=ON;")
                .await
                .unwrap();
        }

        let mut tx_a = conn_a.transaction().await.unwrap();
        let mut tx_b = conn_b.transaction().await.unwrap();

        tx_a.execute("UPDATE t SET v = v + 1 WHERE id = 1;")
            .await
            .unwrap();

        let loser_err = match tx_b.execute("UPDATE t SET v = v + 1 WHERE id = 1;").await {
            Ok(changes) => {
                assert_eq!(changes, 1);
                tx_a.commit().await.unwrap();
                tx_b.commit().await.expect_err(
                "second compat transaction should fail transiently once the stale snapshot resolves",
            )
            }
            Err(err) => {
                tx_a.commit().await.unwrap();
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

        tx_b.rollback().await.unwrap();
        assert!(
            !conn_b.in_transaction(),
            "explicit rollback should close the failed compat transaction"
        );

        let row = conn_a
            .query_row("SELECT v FROM t WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(row.get(0).unwrap(), &SqliteValue::Integer(1));
    });
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
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER)").await.unwrap();

        // Missing row → None via optional
        let row = conn
            .query_row_map("SELECT id FROM t WHERE id = 999", &[], |r| {
                r.get_typed::<i64>(0)
            })
            .await
            .optional()
            .unwrap();
        assert!(row.is_none());

        // Existing row → Some
        conn.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let row = conn
            .query_row_map("SELECT id FROM t WHERE id = 1", &[], |r| {
                r.get_typed::<i64>(0)
            })
            .await
            .optional()
            .unwrap();
        assert_eq!(row, Some(1));
    });
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
    asupersync::test_utils::run_test(|| async {
        let conn = open_with_flags(":memory:", OpenFlags::default_flags())
            .await
            .unwrap();
        conn.execute("CREATE TABLE t(x INTEGER)").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let rows = conn.query("SELECT x FROM t").await.unwrap();
        assert_eq!(rows.len(), 1);
    });
}

#[test]
fn open_with_flags_read_write_creates_db() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let path_str = path.to_str().unwrap();

        let conn = open_with_flags(
            path_str,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .await
        .unwrap();
        conn.execute("CREATE TABLE t(x INTEGER)").await.unwrap();
        drop(conn);

        assert!(path.exists(), "database file should be created");
    });
}

#[test]
fn open_with_flags_read_write_without_create_does_not_create_missing_db() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("missing.db");

        let error = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
            .await
            .expect_err("READ_WRITE without CREATE must reject a missing database");

        assert!(matches!(error, FrankenError::CannotOpen { .. }));
        assert!(!path.exists(), "failed open must not create the database");
    });
}

#[test]
fn open_with_flags_read_write_without_create_preserves_empty_db() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("empty.db");
        std::fs::write(&path, []).unwrap();
        let before = std::fs::read(&path).unwrap();

        let error = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
            .await
            .expect_err("READ_WRITE without CREATE must not initialize an empty database");

        assert!(matches!(error, FrankenError::CannotOpen { .. }));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    });
}

#[test]
fn open_with_flags_read_write_without_create_preserves_malformed_db() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("malformed.db");
        let before = b"not a sqlite database".to_vec();
        std::fs::write(&path, &before).unwrap();

        let error = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
            .await
            .expect_err("READ_WRITE without CREATE must reject a malformed database");

        assert!(matches!(error, FrankenError::DatabaseCorrupt { .. }));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    });
}

#[test]
fn open_with_flags_read_write_without_create_opens_existing_valid_db() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("existing.db");
        {
            let seed = RusqliteConnection::open(&path).unwrap();
            seed.execute("CREATE TABLE t(x INTEGER)", []).unwrap();
            seed.execute("INSERT INTO t VALUES (1)", []).unwrap();
        }

        let conn = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
            .await
            .expect("READ_WRITE without CREATE should open an existing valid database");
        let row = conn.query_row("SELECT x FROM t").await.unwrap();
        assert_eq!(row.get(0), Some(&SqliteValue::Integer(1)));
        conn.execute("INSERT INTO t VALUES (2)").await.unwrap();
    });
}

#[test]
fn open_with_flags_read_only_supports_datetime_builtin() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("readonly_datetime.db");
        let path_str = path.to_str().unwrap();

        let writable = open_with_flags(
            path_str,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .await
        .unwrap();
        writable.execute("CREATE TABLE t(x INTEGER)").await.unwrap();
        writable.execute("INSERT INTO t VALUES (1)").await.unwrap();
        drop(writable);

        let readonly = open_with_flags(path_str, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .await
            .unwrap();
        let row = readonly.query_row("SELECT datetime('now')").await.unwrap();
        let value: String = row.get_typed(0).unwrap();
        assert!(
            !value.is_empty(),
            "datetime('now') should return a non-empty timestamp on read-only compat connections"
        );
    });
}

#[cfg(all(feature = "native", any(unix, windows)))]
#[test]
fn open_with_flags_read_only_query_preserves_every_database_artifact() {
    asupersync::test_utils::run_test(|| async {
        use std::fs::{File, FileTimes};
        use std::time::{Duration, UNIX_EPOCH};

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("readonly_artifact_stability.db");
        let path_str = path.to_str().unwrap();

        let writable = open_with_flags(
            path_str,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .await
        .expect("create FrankenSQLite generation");
        writable
            .execute_batch(
                "CREATE TABLE artifact_probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO artifact_probe(value) VALUES ('preserved');",
            )
            .await
            .expect("seed WAL-backed row");
        drop(writable);

        let namespace_gate = suffixed_path(&path, "-fsqlite-ns-gate");
        let namespace_use = suffixed_path(&path, "-fsqlite-ns-use");
        let wal = suffixed_path(&path, "-wal");
        assert!(
            namespace_gate.exists(),
            "writer must publish the namespace gate"
        );
        assert!(
            namespace_use.exists(),
            "writer must publish the namespace identity"
        );
        assert!(
            wal.exists(),
            "fixture must retain a WAL companion for readback"
        );

        let sentinel_modified = UNIX_EPOCH + Duration::from_secs(946_684_800);
        File::options()
            .write(true)
            .open(&namespace_use)
            .expect("open namespace identity for timestamp sentinel")
            .set_times(FileTimes::new().set_modified(sentinel_modified))
            .expect("set namespace identity timestamp sentinel");
        let before = snapshot_directory_files(dir.path());

        let readonly = open_with_flags(path_str, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .await
            .expect("open existing generation read-only");
        let row = readonly
            .query_row("SELECT id, value FROM artifact_probe")
            .await
            .expect("query the WAL-backed row");
        assert_eq!(row.get(0), Some(&SqliteValue::Integer(1)));
        assert_eq!(
            row.get(1),
            Some(&SqliteValue::Text("preserved".to_owned().into()))
        );
        drop(readonly);

        assert_eq!(
            snapshot_directory_files(dir.path()),
            before,
            "read-only open/query must preserve the complete DB, namespace, WAL, and companion file set byte-for-byte without touching modification times"
        );
    });
}

#[cfg(all(feature = "native", any(unix, windows)))]
#[test]
fn open_with_flags_read_only_opens_stock_database_without_touching_it() {
    asupersync::test_utils::run_test(|| async {
        // GH #140 (partial): a stock SQLite database that FrankenSQLite has
        // never opened carries no namespace records, so strictly read-only
        // admission cannot join an existing generation. The open must still
        // succeed (via the Shared-admission fallback) and must not modify the
        // database file itself; the namespace sidecars it creates are the
        // documented residual gap tracked in #140.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("readonly_missing_namespace.db");
        let path_str = path.to_str().unwrap();
        let external = RusqliteConnection::open(path_str).expect("create external SQLite database");
        external
            .execute_batch(
                "CREATE TABLE external_probe(value INTEGER NOT NULL);
             INSERT INTO external_probe VALUES (7);",
            )
            .expect("seed external SQLite database");
        drop(external);
        let db_bytes_before = std::fs::read(&path).expect("snapshot stock database bytes");

        let readonly = open_with_flags(path_str, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .await
            .expect("stock database must remain openable read-only via fallback admission");
        let row = readonly
            .query_row("SELECT value FROM external_probe")
            .await
            .expect("query the stock-database row");
        assert_eq!(row.get(0), Some(&SqliteValue::Integer(7)));
        drop(readonly);

        assert_eq!(
            std::fs::read(&path).expect("re-read stock database bytes"),
            db_bytes_before,
            "read-only open must not modify the stock database file"
        );
        for suffix in ["-wal", "-shm", "-journal"] {
            assert!(
                !suffixed_path(&path, suffix).exists(),
                "read-only open must not create a {suffix} companion"
            );
        }
    });
}

#[test]
fn open_with_flags_accepts_common_sqlite_ancillary_flags_like_rusqlite() {
    asupersync::test_utils::run_test(|| async {
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
        .await
        .unwrap();
        let row = conn
            .query_row("SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 't'")
            .await
            .unwrap();
        let table_name: String = row.get_typed(0).unwrap();
        assert_eq!(table_name, "t");
    });
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
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // Schema setup via batch
        conn.execute_batch(
            "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            active INTEGER DEFAULT 1
         );",
        )
        .await
        .unwrap();

        // Insert via execute_params
        conn.execute_compat(
            "INSERT INTO users (id, name, email, active) VALUES (?1, ?2, ?3, ?4)",
            params![1_i64, "Alice", "alice@example.com", true],
        )
        .await
        .unwrap();

        conn.execute_compat(
            "INSERT INTO users (id, name, email, active) VALUES (?1, ?2, ?3, ?4)",
            params![2_i64, "Bob", None::<String>, false],
        )
        .await
        .unwrap();

        // Query via query_row_map
        let name: String = conn
            .query_row_map(
                "SELECT name FROM users WHERE id = ?1",
                params![1_i64],
                |row| row.get_typed(0),
            )
            .await
            .unwrap();
        assert_eq!(name, "Alice");

        // Query via query_map_collect
        let names: Vec<String> = conn
            .query_map_collect(
                "SELECT name FROM users WHERE active = ?1 ORDER BY name",
                params![true],
                |row| row.get_typed(0),
            )
            .await
            .unwrap();
        assert_eq!(names, vec!["Alice"]);

        // Optional for missing row
        let missing = conn
            .query_row_map(
                "SELECT name FROM users WHERE id = ?1",
                params![999_i64],
                |row| row.get_typed::<String>(0),
            )
            .await
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
            .await
            .unwrap();
        assert!(email.is_none());

        // Transaction: insert + rollback
        {
            let tx = conn.transaction().await.unwrap();
            tx.execute_compat(
                "INSERT INTO users (id, name) VALUES (?1, ?2)",
                params![3_i64, "Charlie"],
            )
            .await
            .unwrap();
            // drop without commit → rollback
        }

        let count: i64 = conn
            .query_row_map("SELECT COUNT(*) FROM users", &[], |row| row.get_typed(0))
            .await
            .unwrap();
        assert_eq!(count, 2, "Charlie should have been rolled back");

        // Transaction: insert + commit
        {
            let mut tx = conn.transaction().await.unwrap();
            tx.execute_compat(
                "INSERT INTO users (id, name) VALUES (?1, ?2)",
                params![3_i64, "Charlie"],
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
        }

        let count: i64 = conn
            .query_row_map("SELECT COUNT(*) FROM users", &[], |row| row.get_typed(0))
            .await
            .unwrap();
        assert_eq!(count, 3, "Charlie should be committed");
    });
}

#[test]
fn multi_row_upsert_with_foreign_keys_uses_fallback_without_failing() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
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
        .await
        .unwrap();

        conn.execute_compat(
            "INSERT INTO child (id, parent_id, value) VALUES (?1, ?2, ?3), (?4, ?5, ?6)
         ON CONFLICT(id) DO UPDATE SET value = child.value + excluded.value",
            params![1_i64, 1_i64, 5_i64, 2_i64, 2_i64, 7_i64],
        )
        .await
        .unwrap();

        let rows: Vec<(i64, i64)> = conn
            .query_map_collect(
                "SELECT id, value FROM child ORDER BY id",
                params![],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(rows, vec![(1, 15), (2, 7)]);
    });
}

#[test]
fn upsert_do_update_after_leaf_split_does_not_double_free_page() {
    asupersync::test_utils::run_test(|| async {
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
            let conn = Connection::open(db).await.unwrap();
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
            .await
            .unwrap();

            let insert = conn
                .prepare(
                    "INSERT INTO tune(kernel, shape_class, machine, params, measured)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(kernel, shape_class, machine) DO NOTHING",
                )
                .await
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
                    .await
                    .unwrap();
            }

            let update = conn
                .prepare(
                    "INSERT INTO tune(kernel, shape_class, machine, params, measured)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(kernel, shape_class, machine)
                 DO UPDATE SET params = excluded.params, measured = excluded.measured",
                )
                .await
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
                    .await
                    .unwrap(),
                1
            );

            let rows = conn
                .query("SELECT kernel, measured FROM tune ORDER BY kernel")
                .await
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
    });
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

    async fn franken_query_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
        conn.query(sql)
            .await
            .unwrap()
            .iter()
            .map(|row| row.values().iter().map(sqlite_val_to_string).collect())
            .collect()
    }

    async fn assert_query_parity(
        label: &str,
        rconn: &rusqlite::Connection,
        fconn: &Connection,
        sql: &str,
    ) {
        assert_parity(
            label,
            rusqlite_query_rows(rconn, sql),
            franken_query_rows(fconn, sql).await,
        );
    }

    async fn assert_direct_and_prepared_query_parity(
        label: &str,
        rconn: &rusqlite::Connection,
        fconn: &Connection,
        sql: &str,
    ) {
        let expected = rusqlite_query_rows(rconn, sql);
        assert_parity(
            &format!("{label}_DIRECT"),
            expected.clone(),
            franken_query_rows(fconn, sql).await,
        );

        let statement = fconn
            .prepare(sql)
            .await
            .unwrap_or_else(|error| panic!("{label}: prepare failed: {error}"));
        let prepared = statement
            .query()
            .await
            .unwrap_or_else(|error| panic!("{label}: prepared query failed: {error}"))
            .iter()
            .map(|row| row.values().iter().map(sqlite_val_to_string).collect())
            .collect();
        assert_parity(&format!("{label}_PREPARED"), expected, prepared);
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

    async fn setup_franken() -> Connection {
        let conn = Connection::open(":memory:").await.unwrap();
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
        .await
        .unwrap();
        conn
    }

    #[test]
    fn parity_comparison_collation_precedence() {
        asupersync::test_utils::run_test(|| async {
            let rconn = RusqliteConnection::open_in_memory().unwrap();
            let fconn = Connection::open(":memory:").await.unwrap();
            let schema = "
                CREATE TABLE comparison_semantics (
                    left_nocase TEXT COLLATE NOCASE,
                    right_plain TEXT,
                    left_binary TEXT,
                    right_nocase TEXT COLLATE NOCASE
                );
                INSERT INTO comparison_semantics
                VALUES ('a ', 'a', 'a', 'A');
            ";
            rconn.execute_batch(schema).unwrap();
            fconn.execute_batch(schema).await.unwrap();

            // Explicit COLLATE on the right outranks a declaration on the left.
            // Without an explicit COLLATE, the plain left column still contributes
            // its implicit BINARY declaration and outranks right-side NOCASE.
            assert_direct_and_prepared_query_parity(
                "COMPARISON_COLLATION_PRECEDENCE",
                &rconn,
                &fconn,
                "SELECT
                     left_nocase = right_plain COLLATE RTRIM,
                     left_nocase IS right_plain COLLATE RTRIM,
                     CASE left_nocase
                         WHEN right_plain COLLATE RTRIM THEN 1 ELSE 0
                     END,
                     left_binary = right_nocase,
                     left_binary IS right_nocase,
                     CASE left_binary WHEN right_nocase THEN 1 ELSE 0 END
                 FROM comparison_semantics",
            )
            .await;
        });
    }

    #[test]
    fn parity_upsert_expression_collation_and_affinity() {
        asupersync::test_utils::run_test(|| async {
            const SCHEMA_AND_ROW: &str = "
                CREATE TABLE upsert_semantics (
                    id INTEGER PRIMARY KEY,
                    left_nocase TEXT COLLATE NOCASE,
                    right_plain TEXT,
                    left_plain TEXT,
                    right_nocase TEXT COLLATE NOCASE,
                    numeric_value INTEGER,
                    numeric_text TEXT,
                    eq_explicit INTEGER,
                    is_explicit INTEGER,
                    case_explicit INTEGER,
                    between_explicit INTEGER,
                    in_singleton INTEGER,
                    implicit_binary INTEGER,
                    numeric_equal INTEGER
                );
                INSERT INTO upsert_semantics VALUES (
                    1, 'a ', 'unused', 'a', 'unused', 1, 'unused',
                    0, 0, 0, 0, 0, 0, 0
                );
            ";
            const UPSERT: &str = "
                INSERT INTO upsert_semantics (
                    id, left_nocase, right_plain, left_plain,
                    right_nocase, numeric_value, numeric_text
                ) VALUES (1, 'unused', 'a', 'unused', 'A', 0, '1')
                ON CONFLICT(id) DO UPDATE SET
                    eq_explicit =
                        left_nocase = excluded.right_plain COLLATE RTRIM,
                    is_explicit =
                        left_nocase IS excluded.right_plain COLLATE RTRIM,
                    case_explicit = CASE left_nocase
                        WHEN excluded.right_plain COLLATE RTRIM THEN 1 ELSE 0
                    END,
                    between_explicit =
                        'B' BETWEEN 'a' COLLATE NOCASE AND 'b' COLLATE NOCASE,
                    in_singleton = 'A' IN ('a' COLLATE NOCASE),
                    implicit_binary = left_plain = excluded.right_nocase,
                    numeric_equal = numeric_value = excluded.numeric_text
            ";
            const RESULT: &str = "
                SELECT eq_explicit, is_explicit, case_explicit,
                       between_explicit, in_singleton, implicit_binary,
                       numeric_equal
                FROM upsert_semantics
            ";

            for prepared in [false, true] {
                let rconn = RusqliteConnection::open_in_memory().unwrap();
                let fconn = Connection::open(":memory:").await.unwrap();
                rconn.execute_batch(SCHEMA_AND_ROW).unwrap();
                fconn.execute_batch(SCHEMA_AND_ROW).await.unwrap();
                rconn.execute_batch(UPSERT).unwrap();

                if prepared {
                    fconn
                        .prepare(UPSERT)
                        .await
                        .unwrap()
                        .execute()
                        .await
                        .unwrap();
                } else {
                    fconn.execute(UPSERT).await.unwrap();
                }

                assert_parity(
                    if prepared {
                        "UPSERT_EXPRESSION_COLLATION_AFFINITY_PREPARED"
                    } else {
                        "UPSERT_EXPRESSION_COLLATION_AFFINITY_DIRECT"
                    },
                    rusqlite_query_rows(&rconn, RESULT),
                    franken_query_rows(&fconn, RESULT).await,
                );
            }
        });
    }

    #[test]
    fn parity_excluded_register_and_is_affinity_semantics() {
        asupersync::test_utils::run_test(|| async {
            const SCHEMA: &str = "
                CREATE TABLE excluded_semantics (
                    id INTEGER PRIMARY KEY,
                    existing_nocase TEXT COLLATE NOCASE,
                    ex_nocase TEXT COLLATE NOCASE,
                    ex_binary TEXT,
                    ex_integer INTEGER,
                    result_eq_collation INTEGER,
                    result_is_affinity INTEGER,
                    result_case_collation INTEGER,
                    result_case_affinity INTEGER,
                    result_between_collation INTEGER,
                    result_between_affinity INTEGER,
                    result_in_collation INTEGER,
                    result_in_affinity INTEGER,
                    result_explicit_collation INTEGER,
                    result_cast_affinity INTEGER,
                    result_nullif_register INTEGER,
                    result_nullif_explicit INTEGER
                );
                INSERT INTO excluded_semantics (
                    id, existing_nocase, ex_nocase, ex_binary, ex_integer
                ) VALUES (1, 'a', 'old', 'old', 0);

                CREATE TABLE rowid_semantics (
                    id INTEGER PRIMARY KEY,
                    incoming_text TEXT,
                    result INTEGER
                );
                INSERT INTO rowid_semantics VALUES (1, 'old', 0);
            ";
            const EXCLUDED_UPSERT: &str = "
                INSERT INTO excluded_semantics (
                    id, existing_nocase, ex_nocase, ex_binary, ex_integer
                ) VALUES (1, 'unused', 'A', 'A', 1)
                ON CONFLICT(id) DO UPDATE SET
                    result_eq_collation = excluded.ex_nocase = 'a',
                    result_is_affinity = excluded.ex_integer IS '01',
                    result_case_collation = CASE excluded.ex_binary
                        WHEN existing_nocase THEN 1 ELSE 0 END,
                    result_case_affinity = CASE excluded.ex_integer
                        WHEN '01' THEN 1 ELSE 0 END,
                    result_between_collation = excluded.ex_binary
                        BETWEEN existing_nocase AND existing_nocase,
                    result_between_affinity = excluded.ex_integer
                        BETWEEN '01' AND '01',
                    result_in_collation = excluded.ex_nocase IN ('a'),
                    result_in_affinity = excluded.ex_integer IN ('01'),
                    result_explicit_collation =
                        excluded.ex_nocase COLLATE NOCASE = 'a',
                    result_cast_affinity =
                        CAST(excluded.ex_integer AS INTEGER) IS '01',
                    result_nullif_register =
                        nullif(excluded.ex_nocase, 'a') IS NULL,
                    result_nullif_explicit =
                        nullif(excluded.ex_nocase COLLATE NOCASE, 'a') IS NULL
            ";
            const EXCLUDED_RESULT: &str = "
                SELECT result_eq_collation, result_is_affinity,
                       result_case_collation, result_case_affinity,
                       result_between_collation, result_between_affinity,
                       result_in_collation, result_in_affinity,
                       result_explicit_collation, result_cast_affinity,
                       result_nullif_register, result_nullif_explicit
                FROM excluded_semantics
            ";
            const ROWID_UPSERT: &str = "
                INSERT INTO rowid_semantics(id, incoming_text) VALUES (1, '01')
                ON CONFLICT(id) DO UPDATE SET
                    result = rowid = excluded.incoming_text
            ";

            for prepared in [false, true] {
                let rconn = RusqliteConnection::open_in_memory().unwrap();
                let fconn = Connection::open(":memory:").await.unwrap();
                rconn.execute_batch(SCHEMA).unwrap();
                fconn.execute_batch(SCHEMA).await.unwrap();
                rconn.execute_batch(EXCLUDED_UPSERT).unwrap();
                rconn.execute_batch(ROWID_UPSERT).unwrap();

                if prepared {
                    fconn
                        .prepare(EXCLUDED_UPSERT)
                        .await
                        .unwrap()
                        .execute()
                        .await
                        .unwrap();
                    fconn
                        .prepare(ROWID_UPSERT)
                        .await
                        .unwrap()
                        .execute()
                        .await
                        .unwrap();
                } else {
                    fconn.execute(EXCLUDED_UPSERT).await.unwrap();
                    fconn.execute(ROWID_UPSERT).await.unwrap();
                }

                let expected = rusqlite_query_rows(&rconn, EXCLUDED_RESULT);
                assert_eq!(
                    expected,
                    vec![vec![
                        "0".to_owned(),
                        "0".to_owned(),
                        "1".to_owned(),
                        "0".to_owned(),
                        "1".to_owned(),
                        "0".to_owned(),
                        "0".to_owned(),
                        "0".to_owned(),
                        "1".to_owned(),
                        "1".to_owned(),
                        "0".to_owned(),
                        "1".to_owned(),
                    ]],
                    "SQLite oracle for excluded-register metadata changed",
                );
                assert_parity(
                    if prepared {
                        "EXCLUDED_REGISTER_METADATA_PREPARED"
                    } else {
                        "EXCLUDED_REGISTER_METADATA_DIRECT"
                    },
                    expected,
                    franken_query_rows(&fconn, EXCLUDED_RESULT).await,
                );
                assert_query_parity(
                    if prepared {
                        "UPSERT_HIDDEN_ROWID_AFFINITY_PREPARED"
                    } else {
                        "UPSERT_HIDDEN_ROWID_AFFINITY_DIRECT"
                    },
                    &rconn,
                    &fconn,
                    "SELECT result FROM rowid_semantics",
                )
                .await;
            }

            let rconn = RusqliteConnection::open_in_memory().unwrap();
            let fconn = Connection::open(":memory:").await.unwrap();
            const IS_SCHEMA: &str = "
                CREATE TABLE comparison_values (integer_value INTEGER, text_value TEXT);
                INSERT INTO comparison_values VALUES (1, '01');
                CREATE TABLE outer_values (id INTEGER, value INTEGER);
                INSERT INTO outer_values VALUES (1, 1);
                CREATE TABLE inner_values (id INTEGER, value BLOB);
                INSERT INTO inner_values VALUES (1, '01');
                CREATE TABLE inner_nulls (value BLOB);
                INSERT INTO inner_nulls VALUES (NULL);
                CREATE TABLE outer_rowids (id INTEGER);
                INSERT INTO outer_rowids(rowid, id) VALUES (9, 1);
                CREATE TABLE inner_rowids (value TEXT);
                INSERT INTO inner_rowids(rowid, value) VALUES (1, 'x');
            ";
            rconn.execute_batch(IS_SCHEMA).unwrap();
            fconn.execute_batch(IS_SCHEMA).await.unwrap();

            assert_direct_and_prepared_query_parity(
                "IS_COMPARISON_AFFINITY",
                &rconn,
                &fconn,
                "SELECT integer_value IS text_value FROM comparison_values",
            )
            .await;
            assert_direct_and_prepared_query_parity(
                "NESTED_COLLATE_PRESERVES_AFFINITY",
                &rconn,
                &fconn,
                "SELECT (integer_value COLLATE NOCASE COLLATE RTRIM) IS '01'
                 FROM comparison_values",
            )
            .await;
            assert_direct_and_prepared_query_parity(
                "ROWID_AFFINITY_AND_UNARY_PLUS",
                &rconn,
                &fconn,
                "SELECT rowid = '01', +rowid = '01',
                        (rowid COLLATE NOCASE COLLATE RTRIM) IS '01'
                 FROM inner_rowids",
            )
            .await;
            assert_direct_and_prepared_query_parity(
                "CORRELATED_INNER_BLOB_SHADOWS_OUTER_INTEGER_AFFINITY",
                &rconn,
                &fconn,
                "SELECT
                     (SELECT value IS 1 FROM inner_values
                      WHERE inner_values.id = outer_values.id),
                     (SELECT value = 1 FROM inner_values
                      WHERE inner_values.id = outer_values.id)
                 FROM outer_values",
            )
            .await;
            assert_direct_and_prepared_query_parity(
                "CORRELATED_INNER_HIDDEN_ROWID_SCOPE_AND_AFFINITY",
                &rconn,
                &fconn,
                "SELECT
                     (SELECT rowid FROM inner_rowids
                      WHERE inner_rowids.rowid = outer_rowids.id),
                     (SELECT rowid IS '01' FROM inner_rowids
                      WHERE inner_rowids.rowid = outer_rowids.id)
                 FROM outer_rowids",
            )
            .await;
            assert_direct_and_prepared_query_parity(
                "CORRELATED_SIMPLE_CASE_NULL_NEVER_MATCHES",
                &rconn,
                &fconn,
                "SELECT
                     (SELECT CASE value WHEN NULL THEN 1 ELSE 0 END
                      FROM inner_nulls
                      WHERE inner_nulls.rowid = outer_values.id),
                     (SELECT CASE WHEN value THEN 1 ELSE 0 END
                      FROM inner_nulls
                      WHERE inner_nulls.rowid = outer_values.id)
                 FROM outer_values",
            )
            .await;
        });
    }

    #[test]
    fn parity_collating_scalars_stop_at_first_binary_column() {
        asupersync::test_utils::run_test(|| async {
            let rconn = RusqliteConnection::open_in_memory().unwrap();
            let fconn = Connection::open(":memory:").await.unwrap();
            let schema = "
                CREATE TABLE scalar_semantics (
                    first_plain TEXT,
                    equal_nocase TEXT,
                    order_nocase TEXT
                );
                INSERT INTO scalar_semantics VALUES ('a', 'A', 'B');
            ";
            rconn.execute_batch(schema).unwrap();
            fconn.execute_batch(schema).await.unwrap();

            // A bare column defines BINARY even when its schema metadata omits an
            // explicit COLLATE clause. Literals do not, so their later NOCASE
            // argument remains the selected collation in the control expressions.
            assert_direct_and_prepared_query_parity(
                "COLLATING_SCALAR_FIRST_ARGUMENT",
                &rconn,
                &fconn,
                "SELECT
                     nullif(first_plain, equal_nocase COLLATE NOCASE),
                     min(first_plain, order_nocase COLLATE NOCASE),
                     max(first_plain, order_nocase COLLATE NOCASE),
                     nullif('a', 'A' COLLATE NOCASE),
                     min('a', 'B' COLLATE NOCASE),
                     max('a', 'B' COLLATE NOCASE)
                 FROM scalar_semantics",
            )
            .await;
        });
    }

    #[test]
    fn parity_in_list_rhs_collation_rules() {
        asupersync::test_utils::run_test(|| async {
            let rconn = RusqliteConnection::open_in_memory().unwrap();
            let fconn = Connection::open(":memory:").await.unwrap();
            let schema = "
                CREATE TABLE in_rhs (value TEXT COLLATE NOCASE);
                INSERT INTO in_rhs VALUES ('a');
            ";
            rconn.execute_batch(schema).unwrap();
            fconn.execute_batch(schema).await.unwrap();

            // SQLite's one-element constant-list rewrite admits the RHS COLLATE.
            // Multi-item and row-dependent IN lists instead compare using only
            // the LHS collation; explicit LHS BINARY/NOCASE still wins a tie.
            assert_direct_and_prepared_query_parity(
                "IN_LIST_RHS_COLLATION",
                &rconn,
                &fconn,
                "SELECT
                     'A' IN ('a' COLLATE NOCASE),
                     'A' IN ('a' COLLATE NOCASE, 'x'),
                     'A' IN (value),
                     'A' IN (value COLLATE NOCASE),
                     ('A' COLLATE NOCASE) IN (value COLLATE BINARY),
                     ('A' COLLATE BINARY) IN ('a' COLLATE NOCASE)
                 FROM in_rhs",
            )
            .await;
        });
    }

    #[test]
    fn parity_correlated_fallback_collation_and_null_ordering() {
        asupersync::test_utils::run_test(|| async {
            let rconn = RusqliteConnection::open_in_memory().unwrap();
            let fconn = Connection::open(":memory:").await.unwrap();
            let schema = "
                CREATE TABLE outer_bounds (id INTEGER PRIMARY KEY, probe TEXT);
                CREATE TABLE inner_bounds (
                    id INTEGER,
                    low_value TEXT COLLATE NOCASE,
                    high_value TEXT COLLATE RTRIM
                );
                INSERT INTO outer_bounds VALUES (1, 'B'), (2, 'b ');
                INSERT INTO inner_bounds VALUES (1, 'a', 'z'), (2, 'a', 'b');

                CREATE TABLE outer_members (probe TEXT, exact_probe TEXT);
                CREATE TABLE inner_members (value TEXT);
                INSERT INTO outer_members VALUES ('A', 'a');
                INSERT INTO inner_members VALUES ('a');
            ";
            rconn.execute_batch(schema).unwrap();
            fconn.execute_batch(schema).await.unwrap();

            // Concatenation removes the outer column's declared collation, so the
            // low and high comparisons independently inherit their RHS columns.
            assert_direct_and_prepared_query_parity(
                "CORRELATED_BETWEEN_COLLATION",
                &rconn,
                &fconn,
                "SELECT o.id,
                        (SELECT count(*)
                         FROM inner_bounds AS i
                         WHERE i.id = o.id
                           AND (o.probe || '') BETWEEN i.low_value AND i.high_value)
                 FROM outer_bounds AS o
                 ORDER BY o.id",
            )
            .await;

            // Keep the NULL before the matching IN element: a correct IN loop
            // remembers it but continues looking for a definitive match.
            assert_direct_and_prepared_query_parity(
                "CORRELATED_IN_NULLIF_COLLATION",
                &rconn,
                &fconn,
                "SELECT
                     (SELECT count(*) FROM inner_members AS i
                      WHERE (o.probe COLLATE NOCASE) IN (i.value)),
                     (SELECT count(*) FROM inner_members AS i
                      WHERE o.exact_probe IN (NULL, i.value)),
                     (SELECT count(*) FROM inner_members AS i
                      WHERE nullif(o.probe COLLATE NOCASE, i.value) IS NULL),
                     (SELECT count(*) FROM inner_members AS i
                      WHERE (o.probe || '') IN ('a' COLLATE NOCASE))
                 FROM outer_members AS o",
            )
            .await;
        });
    }

    #[test]
    fn parity_like_and_glob_propagate_pattern_collation_first() {
        asupersync::test_utils::run_test(|| async {
            let rconn = RusqliteConnection::open_in_memory().unwrap();
            let fconn = Connection::open(":memory:").await.unwrap();
            let schema = "
                CREATE TABLE pattern_inputs (source TEXT);
                INSERT INTO pattern_inputs VALUES ('abc');
            ";
            rconn.execute_batch(schema).unwrap();
            fconn.execute_batch(schema).await.unwrap();

            // LIKE/GLOB lower to function-call argument order (pattern, source),
            // so the pattern's explicit collation propagates through CAST into
            // the enclosing comparison. RTRIM and NOCASE distinguish the winner.
            assert_direct_and_prepared_query_parity(
                "PATTERN_OPERATOR_COLLATION_PROPAGATION",
                &rconn,
                &fconn,
                "SELECT
                     CAST((source COLLATE NOCASE GLOB ('*' COLLATE RTRIM)) AS TEXT) < '1 ',
                     CAST((source COLLATE RTRIM GLOB ('*' COLLATE NOCASE)) AS TEXT) < '1 ',
                     CAST((source COLLATE NOCASE LIKE ('%' COLLATE RTRIM)) AS TEXT) < '1 ',
                     CAST((source COLLATE RTRIM LIKE ('%' COLLATE NOCASE)) AS TEXT) < '1 '
                 FROM pattern_inputs",
            )
            .await;
        });
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
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

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
                .await
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
        });
    }

    #[test]
    fn parity_where_clause() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

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
                .await
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
        });
    }

    #[test]
    fn parity_count_aggregate() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

            let r_count: i64 = rconn
                .query_row("SELECT COUNT(*) FROM msgs", [], |row| row.get(0))
                .unwrap();
            let f_rows = fconn.query("SELECT COUNT(*) FROM msgs").await.unwrap();
            let f_count = match f_rows[0].get(0).unwrap() {
                SqliteValue::Integer(i) => *i,
                other => panic!("expected Integer, got {other:?}"),
            };
            assert_eq!(r_count, f_count, "COUNT parity");
        });
    }

    #[test]
    fn parity_group_by() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

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
                .await
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
        });
    }

    #[test]
    fn parity_null_handling() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

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
                .await
                .unwrap();
            let f_null = match f_rows[0].get(0).unwrap() {
                SqliteValue::Integer(i) => *i,
                other => panic!("expected Integer, got {other:?}"),
            };
            assert_eq!(r_null, f_null, "IS NULL parity");
        });
    }

    #[test]
    fn parity_like_operator() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

            let r_count: i64 = rconn
                .query_row(
                    "SELECT COUNT(*) FROM msgs WHERE content LIKE '%bug%'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let f_rows = fconn
                .query("SELECT COUNT(*) FROM msgs WHERE content LIKE '%bug%'")
                .await
                .unwrap();
            let f_count = match f_rows[0].get(0).unwrap() {
                SqliteValue::Integer(i) => *i,
                other => panic!("expected Integer, got {other:?}"),
            };
            assert_eq!(r_count, f_count, "LIKE parity");
        });
    }

    #[test]
    fn parity_update() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

            rconn
                .execute("UPDATE msgs SET content = 'updated' WHERE id = 1", [])
                .unwrap();
            fconn
                .execute("UPDATE msgs SET content = 'updated' WHERE id = 1")
                .await
                .unwrap();

            let r_val: String = rconn
                .query_row("SELECT content FROM msgs WHERE id = 1", [], |row| {
                    row.get(0)
                })
                .unwrap();
            let f_rows = fconn
                .query("SELECT content FROM msgs WHERE id = 1")
                .await
                .unwrap();
            let f_val = sqlite_val_to_string(f_rows[0].get(0).unwrap());

            assert_eq!(r_val, f_val, "UPDATE parity");
        });
    }

    #[test]
    fn parity_delete() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

            rconn.execute("DELETE FROM msgs WHERE id = 3", []).unwrap();
            fconn
                .execute("DELETE FROM msgs WHERE id = 3")
                .await
                .unwrap();

            let r_count: i64 = rconn
                .query_row("SELECT COUNT(*) FROM msgs", [], |row| row.get(0))
                .unwrap();
            let f_rows = fconn.query("SELECT COUNT(*) FROM msgs").await.unwrap();
            let f_count = match f_rows[0].get(0).unwrap() {
                SqliteValue::Integer(i) => *i,
                other => panic!("expected Integer, got {other:?}"),
            };
            assert_eq!(r_count, f_count, "DELETE parity");
        });
    }

    #[test]
    fn parity_subquery() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

            let r_count: i64 = rconn
                .query_row(
                    "SELECT COUNT(*) FROM msgs WHERE ts > (SELECT AVG(ts) FROM msgs)",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let f_rows = fconn
                .query("SELECT COUNT(*) FROM msgs WHERE ts > (SELECT AVG(ts) FROM msgs)")
                .await
                .unwrap();
            let f_count = match f_rows[0].get(0).unwrap() {
                SqliteValue::Integer(i) => *i,
                other => panic!("expected Integer, got {other:?}"),
            };
            assert_eq!(r_count, f_count, "SUBQUERY parity");
        });
    }

    #[test]
    fn parity_coalesce() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

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
                .await
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
        });
    }

    #[test]
    fn parity_compound_select_set_operators() {
        asupersync::test_utils::run_test(|| async {
            let rconn = setup_rusqlite();
            let fconn = setup_franken().await;

            assert_query_parity(
                "COMPOUND_UNION_DISTINCT",
                &rconn,
                &fconn,
                "SELECT agent FROM msgs WHERE role = 'user'
             UNION
             SELECT agent FROM msgs WHERE content IS NULL
             ORDER BY agent",
            )
            .await;
            assert_query_parity(
                "COMPOUND_UNION_ALL_MULTIPLICITY",
                &rconn,
                &fconn,
                "SELECT agent FROM msgs WHERE role = 'user'
             UNION ALL
             SELECT agent FROM msgs WHERE agent = 'codex'
             ORDER BY agent",
            )
            .await;
            assert_query_parity(
                "COMPOUND_INTERSECT",
                &rconn,
                &fconn,
                "SELECT agent FROM msgs WHERE role = 'user'
             INTERSECT
             SELECT agent FROM msgs WHERE content IS NOT NULL
             ORDER BY agent",
            )
            .await;
            assert_query_parity(
                "COMPOUND_EXCEPT",
                &rconn,
                &fconn,
                "SELECT agent FROM msgs
             EXCEPT
             SELECT agent FROM msgs WHERE content IS NULL
             ORDER BY agent",
            )
            .await;
        });
    }

    #[test]
    fn parity_match_udf_uses_registered_scalar_path() {
        asupersync::test_utils::run_test(|| async {
            let fconn = setup_franken().await;
            fconn.register_deterministic_scalar_function(ForceMatch);

            assert_query_parity(
                "MATCH_UDF_REAL_PATH",
                &rusqlite_with_forced_match(),
                &fconn,
                "SELECT id FROM msgs WHERE content MATCH 'force-real-path' ORDER BY id",
            )
            .await;
        });
    }

    #[test]
    fn parity_prepared_match_udf_uses_registered_scalar_path() {
        asupersync::test_utils::run_test(|| async {
            let fconn = setup_franken().await;
            fconn.register_deterministic_scalar_function(ForceMatch);

            let stmt = fconn
                .prepare("SELECT id FROM msgs WHERE content MATCH ?1 ORDER BY id")
                .await
                .unwrap();
            let rows = stmt
                .query_with_params(&[SqliteValue::Text("force-real-path".into())])
                .await
                .unwrap();
            let ids: Vec<String> = rows
                .iter()
                .map(|row| sqlite_val_to_string(row.get(0).unwrap()))
                .collect();
            assert_eq!(ids, vec!["1", "2", "3", "5"]);
        });
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
        asupersync::test_utils::run_test(|| async {
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

            let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();
            let rows = conn
                .query("SELECT id, slug, human_key, created_at FROM projects ORDER BY id")
                .await
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
                .await
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
        });
    }

    #[test]
    fn parity_sqlite_created_cursor_state_vscdb_reads_with_frankensqlite() {
        asupersync::test_utils::run_test(|| async {
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

            let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();
            let composer_rows = conn
                .query_with_params(
                    "SELECT key, value FROM cursorDiskKV WHERE key >= ?1 AND key < ?2 ORDER BY key",
                    &[
                        SqliteValue::Text("composerData:".into()),
                        SqliteValue::Text("composerData;".into()),
                    ],
                )
                .await
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
            .await
            .unwrap();
            assert_eq!(legacy_rows.len(), 1);
            assert_eq!(
                legacy_rows[0].values(),
                vec![
                    SqliteValue::Text("workbench.panel.aichat.view.aichat.chatdata".into()),
                    SqliteValue::Text(legacy_json.into()),
                ]
            );
        });
    }
}
