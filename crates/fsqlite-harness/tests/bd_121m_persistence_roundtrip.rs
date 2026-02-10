//! bd-121m: Persistence round-trip tests for `Connection`.
//!
//! Validates that the SQL-dump persistence mechanism in `Connection` correctly
//! writes table schema + row data on mutation and reloads it on reopen.
//! These tests use `tempfile` to avoid test-to-test interference.

#![allow(clippy::approx_constant)]

use fsqlite::Connection;
use fsqlite_types::value::SqliteValue;

// ── Basic round-trip ────────────────────────────────────────────────────────

#[test]
fn test_persistence_create_insert_reopen_select() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    // Phase 1: create table + insert row, then drop connection.
    {
        let conn = Connection::open(&path_str).unwrap();
        conn.execute("CREATE TABLE t (a INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (42);").unwrap();
    }

    // Phase 2: reopen and verify data survived.
    {
        let conn = Connection::open(&path_str).unwrap();
        let rows = conn.query("SELECT a FROM t;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(42)));
    }
}

#[test]
fn test_persistence_multiple_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("multi.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    {
        let conn = Connection::open(&path_str).unwrap();
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE scores (user_id INTEGER, score REAL);")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'alice');")
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'bob');")
            .unwrap();
        conn.execute("INSERT INTO scores VALUES (1, 95.5);")
            .unwrap();
        conn.execute("INSERT INTO scores VALUES (2, 87.3);")
            .unwrap();
    }

    {
        let conn = Connection::open(&path_str).unwrap();
        let users = conn.query("SELECT id, name FROM users;").unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].get(0), Some(&SqliteValue::Integer(1)));
        assert_eq!(
            users[0].get(1),
            Some(&SqliteValue::Text("alice".to_owned()))
        );

        let scores = conn.query("SELECT user_id, score FROM scores;").unwrap();
        assert_eq!(scores.len(), 2);
        assert_eq!(scores[0].get(0), Some(&SqliteValue::Integer(1)));
        assert_eq!(scores[0].get(1), Some(&SqliteValue::Float(95.5)));
    }
}

// ── Value type coverage ─────────────────────────────────────────────────────

#[test]
fn test_persistence_all_value_types() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("types.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    {
        let conn = Connection::open(&path_str).unwrap();
        conn.execute("CREATE TABLE types (i INTEGER, r REAL, t TEXT, b BLOB, n INTEGER);")
            .unwrap();
        conn.execute("INSERT INTO types VALUES (42, 3.14, 'hello world', X'DEADBEEF', NULL);")
            .unwrap();
    }

    {
        let conn = Connection::open(&path_str).unwrap();
        let rows = conn.query("SELECT i, r, t, b, n FROM types;").unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.get(0), Some(&SqliteValue::Integer(42)));
        assert_eq!(row.get(1), Some(&SqliteValue::Float(3.14)));
        assert_eq!(
            row.get(2),
            Some(&SqliteValue::Text("hello world".to_owned()))
        );
        assert_eq!(
            row.get(3),
            Some(&SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]))
        );
        assert_eq!(row.get(4), Some(&SqliteValue::Null));
    }
}

#[test]
fn test_persistence_text_with_single_quotes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("quotes.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    {
        let conn = Connection::open(&path_str).unwrap();
        conn.execute("CREATE TABLE q (val TEXT);").unwrap();
        conn.execute("INSERT INTO q VALUES ('it''s a test');")
            .unwrap();
    }

    {
        let conn = Connection::open(&path_str).unwrap();
        let rows = conn.query("SELECT val FROM q;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(0),
            Some(&SqliteValue::Text("it's a test".to_owned()))
        );
    }
}

// ── Memory path ─────────────────────────────────────────────────────────────

#[test]
fn test_persistence_memory_path_no_file() {
    let dir = tempfile::tempdir().unwrap();

    {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
    }

    // No file should have been created anywhere in the temp dir.
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        entries.is_empty(),
        "memory connection should not create disk files"
    );
}

// ── Empty database ──────────────────────────────────────────────────────────

#[test]
fn test_persistence_empty_database() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("empty.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    // Create and immediately drop — no tables, so persistence file should be
    // empty or absent.
    {
        let _conn = Connection::open(&path_str).unwrap();
    }

    // Reopen should succeed without errors.
    {
        let conn = Connection::open(&path_str).unwrap();
        // Expression-only SELECT still works.
        let rows = conn.query("SELECT 1 + 1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(2)));
    }
}

// ── Transaction rollback ────────────────────────────────────────────────────

#[test]
fn test_persistence_rollback_not_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("rollback.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    {
        let conn = Connection::open(&path_str).unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();

        // Begin transaction, insert, then rollback.
        conn.execute("BEGIN;").unwrap();
        conn.execute("INSERT INTO t VALUES (999);").unwrap();
        conn.execute("ROLLBACK;").unwrap();
    }

    // Reopen: only the pre-transaction row should be present.
    {
        let conn = Connection::open(&path_str).unwrap();
        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 1, "rolled-back row should not be persisted");
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
    }
}

// ── UPDATE / DELETE persistence ─────────────────────────────────────────────

#[test]
fn test_persistence_update_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("update.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    {
        let conn = Connection::open(&path_str).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'before');").unwrap();
        conn.execute("UPDATE t SET val = 'after' WHERE id = 1;")
            .unwrap();
    }

    {
        let conn = Connection::open(&path_str).unwrap();
        let rows = conn.query("SELECT val FROM t WHERE id = 1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Text("after".to_owned())));
    }
}

#[test]
fn test_persistence_delete_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("delete.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    {
        let conn = Connection::open(&path_str).unwrap();
        conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
        conn.execute("INSERT INTO t VALUES (1);").unwrap();
        conn.execute("INSERT INTO t VALUES (2);").unwrap();
        conn.execute("INSERT INTO t VALUES (3);").unwrap();
        conn.execute("DELETE FROM t WHERE x = 2;").unwrap();
    }

    {
        let conn = Connection::open(&path_str).unwrap();
        let rows = conn.query("SELECT x FROM t;").unwrap();
        assert_eq!(rows.len(), 2, "deleted row should not be persisted");
        let mut vals: Vec<i64> = Vec::new();
        for row in &rows {
            if let Some(SqliteValue::Integer(n)) = row.get(0) {
                vals.push(*n);
            }
        }
        assert!(vals.contains(&1));
        assert!(vals.contains(&3));
        assert!(!vals.contains(&2));
    }
}

// ── bd-1702: reserved-word column names ─────────────────────────────────────

#[test]
fn test_persistence_reserved_word_column_unquoted_key() {
    // bd-1702: unquoted KEY as column name must survive persistence round-trip.
    // KEY is a non-reserved keyword in SQL, so it should work without quoting.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("kw.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    {
        let conn = Connection::open(&path_str).unwrap();
        conn.execute("CREATE TABLE meta (key TEXT, val TEXT);")
            .unwrap();
        conn.execute("INSERT INTO meta VALUES ('version', '1.0');")
            .unwrap();
        conn.execute("UPDATE meta SET val = '2.0' WHERE key = 'version';")
            .unwrap();
    }

    {
        let conn = Connection::open(&path_str).unwrap();
        let rows = conn.query("SELECT key, val FROM meta;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(0),
            Some(&SqliteValue::Text("version".to_owned()))
        );
        assert_eq!(rows[0].get(1), Some(&SqliteValue::Text("2.0".to_owned())));
    }
}

// ── E2E combined test ───────────────────────────────────────────────────────

#[test]
fn test_e2e_bd_121m_persistence_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("e2e.db");
    let path_str = db_path.to_str().unwrap().to_owned();

    // Phase 1: populate database with multiple tables and value types.
    {
        let conn = Connection::open(&path_str).unwrap();
        conn.execute("CREATE TABLE meta (\"key\" TEXT, val TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE data (id INTEGER, score REAL);")
            .unwrap();

        conn.execute("INSERT INTO meta VALUES ('version', '1.0');")
            .unwrap();
        conn.execute("INSERT INTO meta VALUES ('author', 'test');")
            .unwrap();
        conn.execute("INSERT INTO data VALUES (1, 99.9);").unwrap();
        conn.execute("INSERT INTO data VALUES (2, 88.8);").unwrap();

        // Modify: update one row, delete another.
        conn.execute("UPDATE meta SET val = '2.0' WHERE \"key\" = 'version';")
            .unwrap();
        conn.execute("DELETE FROM data WHERE id = 1;").unwrap();
    }

    // Phase 2: verify state after reopen.
    {
        let conn = Connection::open(&path_str).unwrap();

        let meta = conn.query("SELECT \"key\", val FROM meta;").unwrap();
        assert_eq!(meta.len(), 2);
        // Find the version row.
        let version_key = SqliteValue::Text("version".to_owned());
        let version_row = meta
            .iter()
            .find(|r: &&fsqlite::Row| r.get(0) == Some(&version_key))
            .expect("version row should exist");
        assert_eq!(
            version_row.get(1),
            Some(&SqliteValue::Text("2.0".to_owned())),
            "updated value should persist"
        );

        let data = conn.query("SELECT id, score FROM data;").unwrap();
        assert_eq!(data.len(), 1, "deleted row should not be persisted");
        assert_eq!(data[0].get(0), Some(&SqliteValue::Integer(2)));
        assert_eq!(data[0].get(1), Some(&SqliteValue::Float(88.8)));
    }

    // Phase 3: reopen again to verify no corruption from the second open.
    {
        let conn = Connection::open(&path_str).unwrap();
        let meta = conn.query("SELECT \"key\", val FROM meta;").unwrap();
        assert_eq!(meta.len(), 2);
        let data = conn.query("SELECT id FROM data;").unwrap();
        assert_eq!(data.len(), 1);
    }
}
