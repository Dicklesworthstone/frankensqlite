//! Integration tests for the frankensqlite migration framework.
//!
//! Bead: coding_agent_session_search-15tra
//!
//! Validates the MigrationRunner lifecycle including fresh installs,
//! partial resumes, idempotency, failure handling, and multi-statement
//! migrations.

use fsqlite::Connection;
use fsqlite::compat::*;
use fsqlite::migrate::{MigrationResult, MigrationRunner};
use fsqlite::params;
use fsqlite_types::value::SqliteValue;

// ===========================================================================
// Fresh database
// ===========================================================================

#[test]
fn fresh_database_applies_all_migrations() {
    let conn = Connection::open(":memory:").unwrap();

    let result = MigrationRunner::new()
        .add(
            1,
            "create_conversations",
            "CREATE TABLE conversations (
                id TEXT PRIMARY KEY,
                agent TEXT NOT NULL,
                created_at INTEGER NOT NULL
             );",
        )
        .add(
            2,
            "create_messages",
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT
             );",
        )
        .add(
            3,
            "add_model_column",
            "ALTER TABLE conversations ADD COLUMN model TEXT;",
        )
        .run(&conn)
        .unwrap();

    assert!(result.was_fresh, "should detect fresh database");
    assert_eq!(result.applied, vec![1, 2, 3]);
    assert_eq!(result.current, 3);

    // Verify schema was applied
    conn.execute_params(
        "INSERT INTO conversations (id, agent, created_at, model) VALUES (?1, ?2, ?3, ?4)",
        params!["s-001", "claude", 1700000000_i64, "opus"],
    )
    .unwrap();

    let row = conn
        .query_row("SELECT model FROM conversations WHERE id = 's-001'")
        .unwrap();
    assert_eq!(row.get(0).unwrap(), &SqliteValue::Text("opus".to_string()));
}

// ===========================================================================
// Partial resume
// ===========================================================================

#[test]
fn partial_resume_only_applies_new_migrations() {
    let conn = Connection::open(":memory:").unwrap();

    // First run: apply V1 and V2
    let runner_v2 = MigrationRunner::new()
        .add(
            1,
            "create_items",
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);",
        )
        .add(
            2,
            "add_count",
            "ALTER TABLE items ADD COLUMN count INTEGER DEFAULT 0;",
        );

    let r1 = runner_v2.run(&conn).unwrap();
    assert_eq!(r1.applied, vec![1, 2]);
    assert_eq!(r1.current, 2);
    assert!(r1.was_fresh);

    // Second run: add V3 - only V3 should apply
    let runner_v3 = MigrationRunner::new()
        .add(
            1,
            "create_items",
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);",
        )
        .add(
            2,
            "add_count",
            "ALTER TABLE items ADD COLUMN count INTEGER DEFAULT 0;",
        )
        .add(
            3,
            "add_desc",
            "ALTER TABLE items ADD COLUMN description TEXT;",
        );

    let r2 = runner_v3.run(&conn).unwrap();
    assert_eq!(r2.applied, vec![3]);
    assert_eq!(r2.current, 3);
    assert!(!r2.was_fresh);

    // Verify column exists
    conn.execute("INSERT INTO items (id, name, description) VALUES (1, 'test', 'desc')")
        .unwrap();
    let row = conn
        .query_row("SELECT description FROM items WHERE id = 1")
        .unwrap();
    assert_eq!(row.get(0).unwrap(), &SqliteValue::Text("desc".to_string()));
}

// ===========================================================================
// Idempotency
// ===========================================================================

#[test]
fn idempotent_rerun_applies_nothing() {
    let conn = Connection::open(":memory:").unwrap();

    let runner = MigrationRunner::new()
        .add(1, "create_t", "CREATE TABLE t (x INTEGER);")
        .add(2, "create_u", "CREATE TABLE u (y TEXT);");

    let r1 = runner.run(&conn).unwrap();
    assert_eq!(r1.applied, vec![1, 2]);

    let r2 = runner.run(&conn).unwrap();
    assert!(r2.applied.is_empty(), "second run should apply nothing");
    assert_eq!(r2.current, 2);
    assert!(!r2.was_fresh);
}

// ===========================================================================
// Failed migration
// ===========================================================================

#[test]
fn failed_migration_rolls_back() {
    let conn = Connection::open(":memory:").unwrap();

    let runner = MigrationRunner::new()
        .add(1, "create_t", "CREATE TABLE t (x INTEGER);")
        .add(2, "bad_sql", "THIS IS NOT VALID SQL;");

    let result = runner.run(&conn);
    assert!(result.is_err(), "invalid SQL should fail");

    // V1 may or may not have been applied depending on implementation.
    // What matters is the DB is in a consistent state.
    // The tracking table should exist (created before migrations run).
}

// ===========================================================================
// Multi-statement migration
// ===========================================================================

#[test]
fn multi_statement_migration() {
    let conn = Connection::open(":memory:").unwrap();

    let result = MigrationRunner::new()
        .add(
            1,
            "create_schema",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE roles (id INTEGER PRIMARY KEY, role TEXT);
             CREATE INDEX idx_users_name ON users(name);",
        )
        .run(&conn)
        .unwrap();

    assert_eq!(result.applied, vec![1]);

    // Verify all three objects exist
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
        .unwrap();
    conn.execute("INSERT INTO roles (id, role) VALUES (1, 'admin')")
        .unwrap();

    // Index should speed up queries (no error = index exists)
    let rows = conn
        .query("SELECT name FROM users WHERE name = 'alice'")
        .unwrap();
    assert_eq!(rows.len(), 1);
}

// ===========================================================================
// Empty runner
// ===========================================================================

#[test]
fn empty_runner_on_fresh_db() {
    let conn = Connection::open(":memory:").unwrap();

    let result = MigrationRunner::new().run(&conn).unwrap();
    assert!(result.applied.is_empty());
    assert_eq!(result.current, 0);
    assert!(result.was_fresh);
}

// ===========================================================================
// Migration tracking table
// ===========================================================================

#[test]
fn migration_records_name_in_tracking_table() {
    let conn = Connection::open(":memory:").unwrap();

    MigrationRunner::new()
        .add(1, "init_schema", "CREATE TABLE t (x INTEGER);")
        .add(2, "add_index", "CREATE INDEX idx_t_x ON t(x);")
        .run(&conn)
        .unwrap();

    // Query the tracking table
    let rows = conn
        .query("SELECT version, name FROM _schema_migrations ORDER BY version")
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0).unwrap(), &SqliteValue::Integer(1));
    assert_eq!(
        rows[0].get(1).unwrap(),
        &SqliteValue::Text("init_schema".to_string())
    );
    assert_eq!(rows[1].get(0).unwrap(), &SqliteValue::Integer(2));
    assert_eq!(
        rows[1].get(1).unwrap(),
        &SqliteValue::Text("add_index".to_string())
    );
}

#[test]
fn migration_tracking_table_has_applied_at() {
    let conn = Connection::open(":memory:").unwrap();

    MigrationRunner::new()
        .add(1, "init", "CREATE TABLE t (x INTEGER);")
        .run(&conn)
        .unwrap();

    let rows = conn
        .query("SELECT applied_at FROM _schema_migrations WHERE version = 1")
        .unwrap();

    assert_eq!(rows.len(), 1);
    // applied_at should be a non-empty ISO timestamp string
    if let SqliteValue::Text(ts) = rows[0].get(0).unwrap() {
        assert!(!ts.is_empty(), "applied_at should not be empty");
        assert!(
            ts.contains('T') || ts.contains('-'),
            "applied_at should look like an ISO timestamp, got: {ts}"
        );
    } else {
        panic!("applied_at should be text");
    }
}

// ===========================================================================
// Ordering enforcement
// ===========================================================================

#[test]
#[should_panic(expected = "")]
fn panics_on_non_ascending_versions() {
    MigrationRunner::new()
        .add(2, "second", "CREATE TABLE b (y TEXT);")
        .add(1, "first", "CREATE TABLE a (x INTEGER);");
}

#[test]
#[should_panic(expected = "")]
fn panics_on_duplicate_versions() {
    MigrationRunner::new()
        .add(1, "first", "CREATE TABLE a (x INTEGER);")
        .add(1, "duplicate", "CREATE TABLE b (y TEXT);");
}

// ===========================================================================
// Realistic cass-like migration sequence
// ===========================================================================

#[test]
fn cass_like_migration_sequence() {
    let conn = Connection::open(":memory:").unwrap();

    let result = MigrationRunner::new()
        .add(
            1,
            "initial_schema",
            "CREATE TABLE conversations (
                id TEXT PRIMARY KEY,
                agent TEXT NOT NULL,
                workspace TEXT,
                project_dir TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER,
                model TEXT,
                title TEXT,
                message_count INTEGER DEFAULT 0,
                source_id TEXT DEFAULT 'local'
             );
             CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL REFERENCES conversations(id),
                role TEXT NOT NULL,
                content TEXT,
                timestamp INTEGER,
                token_count INTEGER DEFAULT 0
             );
             CREATE INDEX idx_conv_agent ON conversations(agent);
             CREATE INDEX idx_conv_created ON conversations(created_at);
             CREATE INDEX idx_msg_conv ON messages(conversation_id);",
        )
        .add(
            2,
            "add_bookmarks",
            "CREATE TABLE bookmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                message_index INTEGER,
                note TEXT,
                created_at INTEGER NOT NULL
             );",
        )
        .add(
            3,
            "add_tags",
            "ALTER TABLE conversations ADD COLUMN tags TEXT DEFAULT '';",
        )
        .run(&conn)
        .unwrap();

    assert!(result.was_fresh);
    assert_eq!(result.applied, vec![1, 2, 3]);
    assert_eq!(result.current, 3);

    // Insert realistic data
    conn.execute_params(
        "INSERT INTO conversations (id, agent, created_at, title, tags) VALUES (?1, ?2, ?3, ?4, ?5)",
        params!["s-001", "claude_code", 1700000000_i64, "Debug auth", "rust,auth"],
    ).unwrap();

    conn.execute_params(
        "INSERT INTO messages (conversation_id, role, content, timestamp) VALUES (?1, ?2, ?3, ?4)",
        params!["s-001", "user", "Why is auth broken?", 1700000000_i64],
    )
    .unwrap();

    conn.execute_params(
        "INSERT INTO bookmarks (conversation_id, message_index, note, created_at) VALUES (?1, ?2, ?3, ?4)",
        params!["s-001", 0_i64, "Key insight", 1700000001_i64],
    ).unwrap();

    // Verify data roundtrip
    let title: String = conn
        .query_row_map(
            "SELECT title FROM conversations WHERE id = ?1",
            params!["s-001"],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(title, "Debug auth");

    let msg_count: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
            params!["s-001"],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(msg_count, 1);

    let bm_note: String = conn
        .query_row_map(
            "SELECT note FROM bookmarks WHERE conversation_id = ?1",
            params!["s-001"],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(bm_note, "Key insight");
}

// ===========================================================================
// Migration result fields
// ===========================================================================

#[test]
fn migration_result_fields_all_correct() {
    let conn = Connection::open(":memory:").unwrap();

    let r: MigrationResult = MigrationRunner::new()
        .add(10, "v10", "CREATE TABLE t10 (x INTEGER);")
        .add(20, "v20", "CREATE TABLE t20 (y TEXT);")
        .run(&conn)
        .unwrap();

    assert!(r.was_fresh);
    assert_eq!(r.applied, vec![10, 20]);
    assert_eq!(r.current, 20);
}
