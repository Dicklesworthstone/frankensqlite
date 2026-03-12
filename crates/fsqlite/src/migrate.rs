//! Lightweight schema migration framework for FrankenSQLite.
//!
//! Provides a [`MigrationRunner`] that manages versioned schema migrations
//! using a `_schema_migrations` tracking table. Each migration is applied
//! in a transaction with automatic rollback on failure.
//!
//! # Example
//!
//! ```rust,no_run
//! use fsqlite::Connection;
//! use fsqlite::migrate::MigrationRunner;
//!
//! let conn = Connection::open("my.db").unwrap();
//! let result = MigrationRunner::new()
//!     .add(1, "create_users", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
//!     .add(2, "add_email", "ALTER TABLE users ADD COLUMN email TEXT;")
//!     .run(&conn)
//!     .unwrap();
//!
//! assert_eq!(result.current, 2);
//! ```

use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

use crate::Connection;

/// A single schema migration with a version number, descriptive name, and SQL to execute.
#[derive(Debug, Clone)]
pub struct Migration {
    /// Monotonically increasing version identifier.
    pub version: i64,
    /// Human-readable migration name (e.g., "create_users_table").
    pub name: &'static str,
    /// SQL statements to execute, separated by semicolons.
    pub up_sql: &'static str,
}

/// Result of running migrations.
#[derive(Debug, Clone)]
pub struct MigrationResult {
    /// Versions that were applied during this run.
    pub applied: Vec<i64>,
    /// The current schema version after running.
    pub current: i64,
    /// True if the database had no prior migrations (fresh install).
    pub was_fresh: bool,
}

/// Builds and executes an ordered set of schema migrations against a [`Connection`].
///
/// Migrations are tracked in a `_schema_migrations` table that records each
/// applied version and its timestamp. Only migrations newer than the most
/// recent applied version are executed.
#[derive(Debug, Clone)]
pub struct MigrationRunner {
    migrations: Vec<Migration>,
}

impl MigrationRunner {
    /// Creates a new empty runner.
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
        }
    }

    /// Adds a migration. Migrations must be added in ascending version order.
    ///
    /// # Panics
    ///
    /// Panics if `version` is not strictly greater than the last added migration's version.
    pub fn add(mut self, version: i64, name: &'static str, sql: &'static str) -> Self {
        if let Some(last) = self.migrations.last() {
            assert!(
                version > last.version,
                "migration version {version} must be greater than previous version {}",
                last.version
            );
        }
        self.migrations.push(Migration {
            version,
            name,
            up_sql: sql,
        });
        self
    }

    /// Runs all pending migrations against the given connection.
    ///
    /// Creates the `_schema_migrations` tracking table if it does not exist.
    /// Determines the current schema version, then applies each migration
    /// whose version exceeds the current version, in order.
    ///
    /// Each migration runs inside a transaction: if any statement fails,
    /// the entire migration is rolled back and the error is returned.
    ///
    /// The runner re-checks each version from inside an `IMMEDIATE`
    /// transaction so that concurrent initializers on the same database
    /// serialize instead of racing to apply the same migration.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError` if any SQL statement fails or the tracking
    /// table cannot be created/queried.
    pub fn run(&self, conn: &Connection) -> Result<MigrationResult, FrankenError> {
        // Ensure the tracking table exists.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _schema_migrations (\
                version INTEGER PRIMARY KEY, \
                name TEXT NOT NULL, \
                applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))\
            );",
        )?;

        // Read the current maximum version.
        let initial_version = Self::read_current_version(conn)?;
        let was_fresh = initial_version == 0;
        let mut applied = Vec::new();

        for migration in &self.migrations {
            if Self::version_is_applied(conn, migration.version)? {
                continue;
            }

            if Self::apply_one(conn, migration)? {
                applied.push(migration.version);
            }
        }
        let current_version = Self::read_current_version(conn)?;

        Ok(MigrationResult {
            applied,
            current: current_version,
            was_fresh,
        })
    }

    /// Reads `MAX(version)` from `_schema_migrations`, returning 0 if empty.
    fn read_current_version(conn: &Connection) -> Result<i64, FrankenError> {
        let rows = conn.query("SELECT MAX(version) FROM _schema_migrations;")?;
        if let Some(row) = rows.first() {
            match row.get(0) {
                Some(SqliteValue::Integer(v)) => Ok(*v),
                _ => Ok(0),
            }
        } else {
            Ok(0)
        }
    }

    fn version_is_applied(conn: &Connection, version: i64) -> Result<bool, FrankenError> {
        let rows = conn.query_with_params(
            "SELECT 1 FROM _schema_migrations WHERE version = ?1 LIMIT 1;",
            &[SqliteValue::Integer(version)],
        )?;
        Ok(!rows.is_empty())
    }

    /// Applies a single migration inside a BEGIN IMMEDIATE/COMMIT transaction.
    /// On failure, issues ROLLBACK before propagating the error.
    ///
    /// Returns `true` when this connection actually applied the migration and
    /// `false` when another connection finished it first.
    fn apply_one(conn: &Connection, migration: &Migration) -> Result<bool, FrankenError> {
        conn.execute("BEGIN IMMEDIATE;")?;
        let result = (|| -> Result<bool, FrankenError> {
            if Self::version_is_applied(conn, migration.version)? {
                conn.execute("COMMIT;")?;
                return Ok(false);
            }

            Self::apply_one_inner(conn, migration)?;
            conn.execute("COMMIT;")?;
            Ok(true)
        })();

        match result {
            Ok(applied) => Ok(applied),
            Err(err) => {
                // Best-effort rollback; ignore rollback errors since
                // the original error is more informative.
                let _ = conn.execute("ROLLBACK;");
                Err(err)
            }
        }
    }

    /// Executes migration SQL and records the version, without transaction management.
    fn apply_one_inner(conn: &Connection, migration: &Migration) -> Result<(), FrankenError> {
        use crate::compat::BatchExt;
        conn.execute_batch(migration.up_sql)?;
        conn.execute_with_params(
            "INSERT INTO _schema_migrations (version, name) VALUES (?1, ?2);",
            &[
                SqliteValue::Integer(migration.version),
                SqliteValue::Text(migration.name.to_owned()),
            ],
        )?;
        Ok(())
    }
}

impl Default for MigrationRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn mem_conn() -> Connection {
        Connection::open(":memory:").expect("in-memory connection should open")
    }

    #[test]
    fn fresh_database_applies_all_migrations() {
        let conn = mem_conn();
        let result = MigrationRunner::new()
            .add(
                1,
                "create_items",
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            )
            .add(
                2,
                "add_description",
                "ALTER TABLE items ADD COLUMN description TEXT",
            )
            .run(&conn)
            .unwrap();

        assert!(result.was_fresh);
        assert_eq!(result.applied, vec![1, 2]);
        assert_eq!(result.current, 2);

        // Verify the table exists and has both columns.
        conn.execute("INSERT INTO items (id, name, description) VALUES (1, 'test', 'desc');")
            .unwrap();
        let rows = conn
            .query("SELECT id, name, description FROM items;")
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn partial_resume_only_applies_new_migrations() {
        let conn = mem_conn();

        // Apply V1 only.
        let r1 = MigrationRunner::new()
            .add(
                1,
                "create_items",
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            )
            .run(&conn)
            .unwrap();

        assert!(r1.was_fresh);
        assert_eq!(r1.applied, vec![1]);
        assert_eq!(r1.current, 1);

        // Now run with V1 + V2 — only V2 should apply.
        let r2 = MigrationRunner::new()
            .add(
                1,
                "create_items",
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            )
            .add(
                2,
                "add_description",
                "ALTER TABLE items ADD COLUMN description TEXT",
            )
            .run(&conn)
            .unwrap();

        assert!(!r2.was_fresh);
        assert_eq!(r2.applied, vec![2]);
        assert_eq!(r2.current, 2);
    }

    #[test]
    fn idempotent_rerun_applies_nothing() {
        let conn = mem_conn();
        let runner = MigrationRunner::new().add(
            1,
            "create_items",
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        );

        let r1 = runner.run(&conn).unwrap();
        assert_eq!(r1.applied, vec![1]);

        let r2 = runner.run(&conn).unwrap();
        assert!(r2.applied.is_empty());
        assert_eq!(r2.current, 1);
        assert!(!r2.was_fresh);
    }

    #[test]
    fn failed_migration_rolls_back() {
        let conn = mem_conn();
        let runner = MigrationRunner::new()
            .add(
                1,
                "create_items",
                "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            )
            .add(
                2,
                "bad_migration",
                "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            ); // duplicate

        let err = runner.run(&conn);
        // V1 should have succeeded, V2 should have failed.
        // Since V1 committed before V2 started, V1 is permanent.
        assert!(err.is_err());
        assert!(
            !conn.in_transaction(),
            "failed migration should not leave an open transaction behind"
        );

        // V1 should be recorded.
        let runner2 = MigrationRunner::new().add(
            1,
            "create_items",
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        );
        let r2 = runner2.run(&conn).unwrap();
        assert!(!r2.was_fresh);
        assert_eq!(r2.current, 1);
        assert!(r2.applied.is_empty());
    }

    #[test]
    fn multi_statement_migration() {
        let conn = mem_conn();
        let result = MigrationRunner::new()
            .add(
                1,
                "create_schema",
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL); \
                 CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT NOT NULL)",
            )
            .run(&conn)
            .unwrap();

        assert_eq!(result.applied, vec![1]);

        // Both tables should exist.
        conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice');")
            .unwrap();
        conn.execute("INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'hello');")
            .unwrap();
    }

    #[test]
    fn empty_runner_on_fresh_db() {
        let conn = mem_conn();
        let result = MigrationRunner::new().run(&conn).unwrap();

        assert!(result.was_fresh);
        assert!(result.applied.is_empty());
        assert_eq!(result.current, 0);
    }

    #[test]
    fn migration_records_name_in_tracking_table() {
        let conn = mem_conn();
        MigrationRunner::new()
            .add(
                1,
                "initial_schema",
                "CREATE TABLE t1 (id INTEGER PRIMARY KEY)",
            )
            .add(2, "add_index", "CREATE INDEX idx_t1 ON t1(id)")
            .run(&conn)
            .unwrap();

        let rows = conn
            .query("SELECT version, name FROM _schema_migrations ORDER BY version;")
            .unwrap();
        assert_eq!(rows.len(), 2);

        match rows[0].get(0) {
            Some(SqliteValue::Integer(1)) => {}
            other => panic!("expected Integer(1), got {other:?}"),
        }
        match rows[0].get(1) {
            Some(SqliteValue::Text(s)) if s == "initial_schema" => {}
            other => panic!("expected Text('initial_schema'), got {other:?}"),
        }
        match rows[1].get(0) {
            Some(SqliteValue::Integer(2)) => {}
            other => panic!("expected Integer(2), got {other:?}"),
        }
        match rows[1].get(1) {
            Some(SqliteValue::Text(s)) if s == "add_index" => {}
            other => panic!("expected Text('add_index'), got {other:?}"),
        }
    }

    #[test]
    fn concurrent_apply_one_serializes_same_version() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("migration_apply_one_race.db");
        let db_path_str = db_path.to_string_lossy().to_string();
        let migration = Migration {
            version: 1,
            name: "create_items",
            up_sql: "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        };

        {
            let conn = Connection::open(&db_path_str).unwrap();
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _schema_migrations (\
                    version INTEGER PRIMARY KEY, \
                    name TEXT NOT NULL, \
                    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))\
                );",
            )
            .unwrap();
        }

        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let db_path_str = db_path_str.clone();
                let barrier = Arc::clone(&barrier);
                let migration = migration.clone();
                thread::spawn(move || {
                    let conn = Connection::open(&db_path_str).unwrap();
                    assert_eq!(MigrationRunner::read_current_version(&conn).unwrap(), 0);
                    barrier.wait();
                    MigrationRunner::apply_one(&conn, &migration).unwrap()
                })
            })
            .collect();

        let mut applied_count = 0;
        let mut skipped_count = 0;
        for handle in handles {
            if handle.join().unwrap() {
                applied_count += 1;
            } else {
                skipped_count += 1;
            }
        }

        assert_eq!(applied_count, 1);
        assert_eq!(skipped_count, 1);

        let conn = Connection::open(&db_path_str).unwrap();
        let rows = conn
            .query("SELECT version, name FROM _schema_migrations ORDER BY version;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
        assert_eq!(
            rows[0].get(1),
            Some(&SqliteValue::Text("create_items".to_owned()))
        );
    }

    #[test]
    fn apply_one_runs_missing_lower_version_even_if_higher_version_exists() {
        let conn = mem_conn();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _schema_migrations (\
                version INTEGER PRIMARY KEY, \
                name TEXT NOT NULL, \
                applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))\
            );",
        )
        .unwrap();
        conn.execute_with_params(
            "INSERT INTO _schema_migrations(version, name) VALUES (?1, ?2);",
            &[
                SqliteValue::Integer(2),
                SqliteValue::Text("already_applied".to_owned()),
            ],
        )
        .unwrap();

        let migration = Migration {
            version: 1,
            name: "outdated",
            up_sql: "CREATE TABLE should_not_exist (id INTEGER PRIMARY KEY);",
        };

        let applied = MigrationRunner::apply_one(&conn, &migration).unwrap();
        assert!(applied);
        assert!(
            !conn
                .query("SELECT name FROM sqlite_master WHERE name = 'should_not_exist';")
                .unwrap()
                .is_empty(),
            "missing lower-version migration should still run even if a higher version row already exists",
        );
        let versions = conn
            .query("SELECT version FROM _schema_migrations ORDER BY version;")
            .unwrap();
        assert_eq!(
            versions
                .iter()
                .map(|row| row.get(0).unwrap().to_integer())
                .collect::<Vec<_>>(),
            vec![1, 2],
            "runner must preserve non-contiguous/mixed-binary migration histories instead of treating MAX(version) as authoritative",
        );
    }

    #[test]
    fn run_applies_missing_lower_version_even_if_higher_version_exists() {
        let conn = mem_conn();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _schema_migrations (\
                version INTEGER PRIMARY KEY, \
                name TEXT NOT NULL\
            );",
        )
        .unwrap();
        conn.execute("INSERT INTO _schema_migrations(version, name) VALUES (2, 'second');")
            .unwrap();

        let result = MigrationRunner::new()
            .add(
                1,
                "create_sparse",
                "CREATE TABLE sparse_fixed (id INTEGER PRIMARY KEY);",
            )
            .add(
                2,
                "noop_second",
                "CREATE TABLE should_not_run (id INTEGER PRIMARY KEY);",
            )
            .run(&conn)
            .unwrap();

        assert_eq!(result.applied, vec![1]);
        assert_eq!(result.current, 2);
        assert!(!result.was_fresh);
        assert!(
            !conn
                .query("SELECT name FROM sqlite_master WHERE name = 'sparse_fixed';")
                .unwrap()
                .is_empty(),
            "public runner should repair sparse histories by applying the missing lower migration",
        );
        assert!(
            conn.query("SELECT name FROM sqlite_master WHERE name = 'should_not_run';")
                .unwrap()
                .is_empty(),
            "already-applied higher migration must stay skipped",
        );
    }

    #[test]
    #[should_panic(expected = "must be greater than")]
    fn panics_on_non_ascending_versions() {
        MigrationRunner::new()
            .add(2, "second", "SELECT 1")
            .add(1, "first", "SELECT 1");
    }

    #[test]
    #[should_panic(expected = "must be greater than")]
    fn panics_on_duplicate_versions() {
        MigrationRunner::new()
            .add(1, "first", "SELECT 1")
            .add(1, "duplicate", "SELECT 1");
    }
}
