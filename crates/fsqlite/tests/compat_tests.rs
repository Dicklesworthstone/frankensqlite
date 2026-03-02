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
    assert_eq!(
        p[1].as_sqlite_value(),
        &SqliteValue::Text("hello".to_string())
    );
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
    assert_eq!(p[0].as_sqlite_value(), &SqliteValue::Blob(vec![1, 2, 3]));
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
fn param_value_from_u64_overflow_clamps() {
    let p: ParamValue = u64::MAX.into();
    assert_eq!(p.into_inner(), SqliteValue::Integer(i64::MAX));
}

#[test]
fn param_value_from_usize() {
    let p: ParamValue = 99_usize.into();
    assert_eq!(p.into_inner(), SqliteValue::Integer(99));
}

#[test]
fn param_value_from_string_and_str() {
    let p: ParamValue = "hello".into();
    assert_eq!(p.into_inner(), SqliteValue::Text("hello".to_string()));

    let p: ParamValue = String::from("world").into();
    assert_eq!(p.into_inner(), SqliteValue::Text("world".to_string()));
}

#[test]
fn param_value_from_byte_slice() {
    let data: &[u8] = &[0xDE, 0xAD];
    let p: ParamValue = data.into();
    assert_eq!(p.into_inner(), SqliteValue::Blob(vec![0xDE, 0xAD]));
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
        .execute_params("INSERT INTO t VALUES (?1, ?2)", params![1_i64, "hello"])
        .unwrap();
    assert_eq!(changed, 1);

    let row = conn.query_row("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(row.get(0).unwrap(), &SqliteValue::Text("hello".to_string()));
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
        let tx = conn.transaction().unwrap();
        tx.execute("INSERT INTO t VALUES ('committed')").unwrap();
        tx.commit().unwrap();
    }

    let rows = conn.query("SELECT val FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("committed".to_string())
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

    let tx = conn.transaction().unwrap();
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
        let tx = conn.transaction().unwrap();
        tx.execute_with_params(
            "INSERT INTO t VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(1),
                SqliteValue::Text("in_tx".to_string()),
            ],
        )
        .unwrap();
        tx.commit().unwrap();
    }

    let rows = conn.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqliteValue::Text("in_tx".to_string())
    );
}

#[test]
fn transaction_query_within() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(val INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (42)").unwrap();

    let tx = conn.transaction().unwrap();
    let rows = tx.query("SELECT val FROM t").unwrap();
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(42));
    tx.commit().unwrap();
}

#[test]
fn transaction_execute_params_compat() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER, val TEXT)")
        .unwrap();

    let tx = conn.transaction().unwrap();
    tx.execute_params(
        "INSERT INTO t VALUES (?1, ?2)",
        params![1_i64, "via_params"],
    )
    .unwrap();
    tx.commit().unwrap();

    let row = conn.query_row("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(
        row.get(0).unwrap(),
        &SqliteValue::Text("via_params".to_string())
    );
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
    assert_eq!(values[1], SqliteValue::Text("text".to_string()));
}

#[test]
fn param_slice_to_values_with_overflow_clamps() {
    let p = [ParamValue::from(u64::MAX)];
    let values = param_slice_to_values(&p);
    assert_eq!(values[0], SqliteValue::Integer(i64::MAX));
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
    conn.execute_params(
        "INSERT INTO users (id, name, email, active) VALUES (?1, ?2, ?3, ?4)",
        params![1_i64, "Alice", "alice@example.com", true],
    )
    .unwrap();

    conn.execute_params(
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
        tx.execute_params(
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
        let tx = conn.transaction().unwrap();
        tx.execute_params(
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

// ===========================================================================
// 11. RUSQLITE PARITY (golden tests)
// ===========================================================================

/// Execute the same SQL operations via both rusqlite and frankensqlite,
/// compare results for parity.
mod rusqlite_parity {
    use super::*;

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
            SqliteValue::Text(s) => s.clone(),
            SqliteValue::Blob(b) => format!("{b:?}"),
        }
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
}
