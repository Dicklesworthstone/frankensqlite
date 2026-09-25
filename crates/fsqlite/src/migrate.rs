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
//! # async fn example() {
//! let conn = Connection::open("my.db").await.unwrap();
//! let result = MigrationRunner::new()
//!     .add(1, "create_users", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
//!     .add(2, "add_email", "ALTER TABLE users ADD COLUMN email TEXT;")
//!     .run(&conn)
//!     .await
//!     .unwrap();
//!
//! assert_eq!(result.current, 2);
//! # }
//! ```

use fsqlite_ast::Statement;
use fsqlite_error::FrankenError;
use fsqlite_parser::Parser;
use fsqlite_types::value::SqliteValue;
use std::time::{Duration, Instant};

use crate::Connection;

const MIGRATION_BUSY_RETRY_BACKOFF: Duration = Duration::from_millis(2);
const MIGRATION_BUSY_RETRY_TIMEOUT: Duration = Duration::from_secs(1);

// Own only transactions admitted by this migration attempt. The guard must
// exist before BEGIN is polled: admission and finalization can both suspend.
// As with compat::Transaction, Drop records an obligation rather than running
// an executor or attempting asynchronous rollback from a destructor.
struct MigrationAttempt<'a> {
    conn: &'a Connection,
    settled: bool,
}

impl<'a> MigrationAttempt<'a> {
    fn new(conn: &'a Connection) -> Result<Self, FrankenError> {
        if conn.in_transaction() {
            return Err(FrankenError::NestedTransaction);
        }
        Ok(Self {
            conn,
            settled: false,
        })
    }
}

impl Drop for MigrationAttempt<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.conn.mark_transaction_cleanup_required();
        }
    }
}

/// A single schema migration with a version number, descriptive name, and SQL to execute.
#[derive(Debug, Clone)]
pub struct Migration {
    /// Monotonically increasing version identifier.
    pub version: i64,
    /// Human-readable migration name (e.g., "create_users_table").
    pub name: &'static str,
    /// SQL statements to execute, separated by semicolons.
    ///
    /// The runner owns the outer transaction. BEGIN, COMMIT/END, and ROLLBACK
    /// without TO are rejected before any statement in this migration runs.
    /// Nested SAVEPOINT, ROLLBACK TO, and RELEASE remain supported.
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
/// Migrations are tracked in `main._schema_migrations`, independently of any
/// temporary table with the same name. Each missing version is applied, including
/// gaps below the maximum recorded version in a mixed-binary migration history.
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
    /// Creates the `main._schema_migrations` tracking table if it does not
    /// exist, then applies each missing version in order.
    ///
    /// Each migration runs inside a transaction: if any statement fails,
    /// the entire migration is rolled back and the error is returned.
    /// The complete pending migration is parsed before executing its first
    /// statement, so transaction-control SQL cannot commit a partial migration.
    /// Already-applied migrations are not revalidated or executed.
    ///
    /// The runner re-checks each version from inside an `IMMEDIATE`
    /// transaction so that concurrent initializers on the same database
    /// serialize instead of racing to apply the same migration.
    ///
    /// The connection must be idle and must not be used through another alias
    /// until this future completes. An existing caller transaction is refused
    /// without modifying it. Dropping an in-flight attempt records mandatory
    /// deferred rollback before the connection's next SQL entry point.
    ///
    /// # Errors
    ///
    /// Returns `FrankenError` if any SQL statement fails or the tracking
    /// table cannot be created/queried. A pending migration that attempts to
    /// own the outer transaction returns `FrankenError::FunctionError` with
    /// the migration version and name; malformed SQL returns `ParseError`.
    pub async fn run(&self, conn: &Connection) -> Result<MigrationResult, FrankenError> {
        // Check before even creating metadata: a failed nested BEGIN must not
        // enter our rollback path and discard the caller's unrelated writes.
        if conn.in_transaction() {
            return Err(FrankenError::NestedTransaction);
        }

        // Always bind metadata to the durable main database, not a TEMP shadow.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS main._schema_migrations (\
                version INTEGER PRIMARY KEY, \
                name TEXT NOT NULL, \
                applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))\
            );",
        )
        .await?;

        // Read the current maximum version.
        let initial_version = Self::read_current_version(conn).await?;
        let was_fresh = initial_version == 0;
        let mut applied = Vec::new();

        for migration in &self.migrations {
            if Self::version_is_applied(conn, migration.version).await? {
                continue;
            }

            if Self::apply_one(conn, migration).await? {
                applied.push(migration.version);
            }
        }
        let current_version = Self::read_current_version(conn).await?;

        Ok(MigrationResult {
            applied,
            current: current_version,
            was_fresh,
        })
    }

    /// Reads `MAX(version)` from `main._schema_migrations`, returning 0 if empty.
    async fn read_current_version(conn: &Connection) -> Result<i64, FrankenError> {
        let started = Instant::now();
        loop {
            match conn
                .query("SELECT MAX(version) FROM main._schema_migrations;")
                .await
            {
                Ok(rows) => {
                    return if let Some(row) = rows.first() {
                        match row.get(0) {
                            Some(SqliteValue::Integer(v)) => Ok(*v),
                            _ => Ok(0),
                        }
                    } else {
                        Ok(0)
                    };
                }
                Err(error) if Self::should_retry_busy(conn, &error, started) => {
                    Self::busy_retry_backoff().await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn version_is_applied(conn: &Connection, version: i64) -> Result<bool, FrankenError> {
        let started = Instant::now();
        loop {
            match conn
                .query_with_params(
                    "SELECT 1 FROM main._schema_migrations WHERE version = ?1 LIMIT 1;",
                    &[SqliteValue::Integer(version)],
                )
                .await
            {
                Ok(rows) => return Ok(!rows.is_empty()),
                Err(error) if Self::should_retry_busy(conn, &error, started) => {
                    Self::busy_retry_backoff().await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn should_retry_busy(conn: &Connection, error: &FrankenError, started: Instant) -> bool {
        !conn.in_transaction()
            && started.elapsed() < MIGRATION_BUSY_RETRY_TIMEOUT
            && matches!(
                error,
                FrankenError::Busy | FrankenError::BusyRecovery | FrankenError::BusySnapshot { .. }
            )
    }

    // `asupersync` is an optional dependency (enabled by `native` / `async-api`).
    // Under `--no-default-features` it is not linked, so fall back to a blocking
    // sleep — the same std backoff this path used before it became async. The
    // migration busy-retry path is cold and the backoff is ~2ms, so briefly
    // blocking is acceptable and keeps the crate building without a runtime.
    #[cfg_attr(
        not(any(feature = "native", feature = "async-api")),
        allow(clippy::unused_async)
    )]
    async fn busy_retry_backoff() {
        #[cfg(any(feature = "native", feature = "async-api"))]
        asupersync::time::sleep(asupersync::time::wall_now(), MIGRATION_BUSY_RETRY_BACKOFF).await;
        #[cfg(not(any(feature = "native", feature = "async-api")))]
        std::thread::sleep(MIGRATION_BUSY_RETRY_BACKOFF);
    }

    /// Applies a single migration inside a BEGIN IMMEDIATE/COMMIT transaction.
    /// On failure, issues ROLLBACK before propagating the error.
    ///
    /// Returns `true` when this connection actually applied the migration and
    /// `false` when another connection finished it first.
    async fn apply_one(conn: &Connection, migration: &Migration) -> Result<bool, FrankenError> {
        let started = Instant::now();
        loop {
            match Self::apply_one_once(conn, migration).await {
                Err(error) if Self::should_retry_busy(conn, &error, started) => {
                    // BusySnapshot invalidates the transaction's publication
                    // image. `apply_one_once` has rolled the whole transaction
                    // back, so retry from BEGIN rather than replaying a
                    // statement inside the stale transaction.
                    Self::busy_retry_backoff().await;
                }
                other => return other,
            }
        }
    }

    async fn apply_one_once(
        conn: &Connection,
        migration: &Migration,
    ) -> Result<bool, FrankenError> {
        let mut attempt = MigrationAttempt::new(conn)?;
        let result: Result<bool, FrankenError> = async {
            conn.execute("BEGIN IMMEDIATE;").await?;
            if Self::version_is_applied(conn, migration.version).await? {
                conn.execute("COMMIT;").await?;
                return Ok(false);
            }

            Self::apply_one_inner(conn, migration).await?;
            conn.execute("COMMIT;").await?;
            Ok(true)
        }
        .await;

        let result = match result {
            Ok(applied) => Ok(applied),
            Err(error) => Err(Self::rollback_failed_attempt(conn, error).await),
        };
        // Leave the guard armed if cleanup failed with an active transaction.
        // It also stays armed when this future is dropped at any await above.
        attempt.settled = !conn.in_transaction();
        result
    }

    /// End a failed migration attempt before the caller decides whether the
    /// whole transaction can be retried. `BEGIN IMMEDIATE` can fail after the
    /// connection has entered explicit-transaction state, and a rollback can
    /// report `BusyRecovery` after it has nevertheless cleared that state.
    async fn rollback_failed_attempt(
        conn: &Connection,
        primary_error: FrankenError,
    ) -> FrankenError {
        if !conn.in_transaction() {
            return primary_error;
        }

        match conn.execute("ROLLBACK;").await {
            Ok(_) => primary_error,
            Err(rollback_error)
                if matches!(rollback_error, FrankenError::BusyRecovery)
                    && !conn.in_transaction() =>
            {
                primary_error
            }
            Err(rollback_error) => rollback_error,
        }
    }

    fn validate_migration_sql(migration: &Migration) -> Result<(), FrankenError> {
        let (statements, errors) = Parser::from_sql(migration.up_sql).parse_all();
        if let Some(error) = errors.first() {
            return Err(FrankenError::ParseError {
                offset: error.span.start as usize,
                detail: error.to_string(),
            });
        }
        // Inspect complete statements, not semicolon splitting or keyword
        // searches: triggers, comments and quoted values can contain BEGIN/END.
        // Inner savepoints cannot commit the runner's enclosing BEGIN.
        let changes_outer_transaction = statements.iter().any(|statement| match statement {
            Statement::Begin(_) | Statement::Commit => true,
            Statement::Rollback(rollback) => rollback.to_savepoint.is_none(),
            _ => false,
        });
        if changes_outer_transaction {
            return Err(FrankenError::FunctionError(format!(
                "migration {} ({}) contains transaction-control SQL: \
                 the runner owns BEGIN, COMMIT/END and full ROLLBACK",
                migration.version, migration.name,
            )));
        }
        Ok(())
    }

    /// Executes migration SQL and records the version, without transaction management.
    async fn apply_one_inner(conn: &Connection, migration: &Migration) -> Result<(), FrankenError> {
        // Validate the entire batch before any of its effects. Checking only
        // conn.in_transaction() afterwards is too late to undo a SQL COMMIT.
        Self::validate_migration_sql(migration)?;
        conn.execute_batch(migration.up_sql).await?;
        conn.execute_with_params(
            "INSERT INTO main._schema_migrations (version, name) VALUES (?1, ?2);",
            &[
                SqliteValue::Integer(migration.version),
                SqliteValue::Text(migration.name.into()),
            ],
        )
        .await?;
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

    async fn mem_conn() -> Connection {
        Connection::open(":memory:")
            .await
            .expect("in-memory connection should open")
    }

    #[test]
    fn run_refuses_caller_transaction_without_creating_metadata_or_losing_writes() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            conn.execute("CREATE TABLE caller_data(value INTEGER)")
                .await
                .unwrap();
            conn.execute("BEGIN").await.unwrap();
            conn.execute("INSERT INTO caller_data VALUES (42)")
                .await
                .unwrap();

            for runner in [
                MigrationRunner::new(),
                MigrationRunner::new().add(1, "must_not_run", "CREATE TABLE forbidden(id INTEGER)"),
            ] {
                let error = runner.run(&conn).await.unwrap_err();
                assert!(matches!(error, FrankenError::NestedTransaction));
                assert!(conn.in_transaction());
                assert!(
                    conn.query(
                        "SELECT name FROM sqlite_master WHERE name IN ('_schema_migrations', 'forbidden')",
                    )
                    .await
                    .unwrap()
                    .is_empty()
                );
                assert_eq!(
                    conn.query_row("SELECT value FROM caller_data")
                        .await
                        .unwrap()
                        .get(0),
                    Some(&SqliteValue::Integer(42))
                );
            }
            conn.execute("COMMIT").await.unwrap();
            assert_eq!(
                conn.query("SELECT value FROM caller_data")
                    .await
                    .unwrap()
                    .len(),
                1
            );
        });
    }

    #[test]
    fn apply_one_once_refuses_nested_begin_without_rolling_back_caller() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            conn.execute("CREATE TABLE caller_data(value INTEGER)")
                .await
                .unwrap();
            conn.execute("BEGIN").await.unwrap();
            conn.execute("INSERT INTO caller_data VALUES (7)")
                .await
                .unwrap();
            let migration = Migration {
                version: 1,
                name: "unused",
                up_sql: "SELECT 1",
            };
            let error = MigrationRunner::apply_one_once(&conn, &migration)
                .await
                .unwrap_err();
            assert!(matches!(error, FrankenError::NestedTransaction));
            assert!(conn.in_transaction());
            conn.execute("COMMIT").await.unwrap();
            assert_eq!(
                conn.query_row("SELECT value FROM caller_data")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Integer(7))
            );
        });
    }

    #[test]
    fn abandoned_attempt_rolls_back_schema_and_data_before_next_sql() {
        asupersync::test_utils::run_test(|| async {
            use std::cell::Cell;
            use std::future::{Future, pending, poll_fn};
            use std::task::Poll;

            let conn = mem_conn().await;
            conn.execute("CREATE TABLE data(value INTEGER)")
                .await
                .unwrap();
            let parked = Cell::new(false);
            // Exercise the same owner used by apply_one_once, with a
            // deterministic suspension after real transactional DDL and DML.
            let mut attempt = Box::pin(async {
                let _owner = MigrationAttempt::new(&conn).unwrap();
                conn.execute("BEGIN IMMEDIATE").await.unwrap();
                conn.execute("CREATE TABLE abandoned(id INTEGER)")
                    .await
                    .unwrap();
                conn.execute("INSERT INTO data VALUES (1)").await.unwrap();
                parked.set(true);
                pending::<()>().await;
            });
            poll_fn(|cx| {
                assert!(attempt.as_mut().poll(cx).is_pending());
                if parked.get() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            assert!(conn.in_transaction());
            drop(attempt);

            assert!(
                conn.query("SELECT value FROM data")
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(!conn.in_transaction());
            assert!(
                conn.query("SELECT name FROM sqlite_master WHERE name = 'abandoned'")
                    .await
                    .unwrap()
                    .is_empty()
            );
            conn.execute("INSERT INTO data VALUES (2)").await.unwrap();
            assert_eq!(
                conn.query_row("SELECT value FROM data")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Integer(2))
            );
        });
    }

    #[test]
    fn settled_migration_does_not_mark_a_later_caller_transaction_for_cleanup() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            MigrationRunner::new()
                .add(1, "create_data", "CREATE TABLE data(value INTEGER)")
                .run(&conn)
                .await
                .unwrap();
            conn.execute("BEGIN").await.unwrap();
            conn.execute("INSERT INTO data VALUES (99)").await.unwrap();
            assert!(conn.in_transaction());
            conn.execute("COMMIT").await.unwrap();
            assert_eq!(
                conn.query_row("SELECT value FROM data")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Integer(99))
            );
        });
    }

    #[test]
    fn transaction_control_is_rejected_before_any_migration_statement_runs() {
        asupersync::test_utils::run_test(|| async {
            for sql in [
                "CREATE TABLE escaped(value INTEGER); COMMIT; INSERT INTO escaped VALUES (1);",
                "CREATE TABLE escaped(value INTEGER); eNd TrAnSaCtIoN; INSERT INTO escaped VALUES (1);",
                "CREATE TABLE escaped(value INTEGER); ROLLBACK; CREATE TABLE escaped(value INTEGER);",
                "CREATE TABLE escaped(value INTEGER); /* nested */ BEGIN IMMEDIATE;",
                "CREATE TABLE escaped(value INTEGER); -- premature publication\nCOMMIT; BEGIN;",
            ] {
                let conn = mem_conn().await;
                let error = MigrationRunner::new()
                    .add(1, "escape_attempt", sql)
                    .run(&conn)
                    .await
                    .unwrap_err();
                assert!(
                    matches!(error, FrankenError::FunctionError(ref message)
                        if message.contains("transaction-control SQL")),
                    "unexpected failure for {sql}: {error:?}"
                );
                assert!(!conn.in_transaction());
                assert!(
                    conn.query("SELECT name FROM sqlite_master WHERE name = 'escaped'")
                        .await
                        .unwrap()
                        .is_empty(),
                    "no statement may run before the complete batch passes preflight: {sql}"
                );
                assert!(
                    conn.query("SELECT version FROM main._schema_migrations")
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
        });
    }

    #[test]
    fn nested_savepoints_remain_inside_the_migration_transaction() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            let result = MigrationRunner::new()
                .add(
                    1,
                    "savepoint_body",
                    "CREATE TABLE data(value INTEGER); \
                     INSERT INTO data VALUES (1); \
                     SAVEPOINT inner_step; \
                     INSERT INTO data VALUES (2); \
                     ROLLBACK TO inner_step; \
                     RELEASE inner_step; \
                     INSERT INTO data VALUES (3);",
                )
                .run(&conn)
                .await
                .unwrap();
            assert_eq!(result.applied, vec![1]);
            assert!(!conn.in_transaction());
            let rows = conn
                .query("SELECT value FROM data ORDER BY value")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
            assert_eq!(rows[1].get(0), Some(&SqliteValue::Integer(3)));
        });
    }

    #[test]
    fn trigger_bodies_comments_and_quoted_keywords_are_not_transaction_control() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            MigrationRunner::new()
                .add(
                    1,
                    "trigger_body",
                    "CREATE TABLE data(value INTEGER); \
                     CREATE TABLE audit(value TEXT); \
                     /* COMMIT; ROLLBACK; BEGIN; */ \
                     CREATE TRIGGER record_insert AFTER INSERT ON data BEGIN \
                         INSERT INTO audit VALUES ('BEGIN; COMMIT; ROLLBACK; END;'); \
                     END; \
                     INSERT INTO data VALUES (1);",
                )
                .run(&conn)
                .await
                .unwrap();
            assert_eq!(
                conn.query_row("SELECT value FROM audit")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Text("BEGIN; COMMIT; ROLLBACK; END;".into()))
            );
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn malformed_tail_is_rejected_before_migration_writes() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            let error = MigrationRunner::new()
                .add(
                    1,
                    "bad_tail",
                    "CREATE TABLE escaped(value INTEGER); INSERT INTO",
                )
                .run(&conn)
                .await
                .unwrap_err();
            assert!(matches!(error, FrankenError::ParseError { .. }));
            assert!(!conn.in_transaction());
            assert!(
                conn.query("SELECT name FROM sqlite_master WHERE name = 'escaped'")
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                conn.query("SELECT version FROM main._schema_migrations")
                    .await
                    .unwrap()
                    .is_empty()
            );
        });
    }

    #[test]
    fn rejected_migration_preserves_previously_committed_migrations() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            let error = MigrationRunner::new()
                .add(
                    1,
                    "baseline",
                    "CREATE TABLE baseline(value INTEGER); INSERT INTO baseline VALUES (9);",
                )
                .add(
                    2,
                    "premature_commit",
                    "CREATE TABLE escaped(value INTEGER); COMMIT;",
                )
                .run(&conn)
                .await
                .unwrap_err();
            assert!(matches!(error, FrankenError::FunctionError(_)));
            assert_eq!(
                conn.query_row("SELECT value FROM baseline")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Integer(9))
            );
            let versions = conn
                .query("SELECT version FROM main._schema_migrations")
                .await
                .unwrap();
            assert_eq!(versions.len(), 1);
            assert_eq!(versions[0].get(0), Some(&SqliteValue::Integer(1)));
            assert!(
                conn.query("SELECT name FROM sqlite_master WHERE name = 'escaped'")
                    .await
                    .unwrap()
                    .is_empty()
            );
        });
    }

    #[test]
    fn temp_tracking_table_cannot_shadow_durable_migration_history() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            conn.execute_batch(
                "CREATE TEMP TABLE _schema_migrations(version INTEGER PRIMARY KEY, name TEXT); \
                 INSERT INTO temp._schema_migrations VALUES (1, 'temporary_shadow');",
            )
            .await
            .unwrap();
            let runner = MigrationRunner::new().add(
                1,
                "durable",
                "CREATE TABLE durable_data(value INTEGER)",
            );
            let first = runner.run(&conn).await.unwrap();
            assert!(first.was_fresh);
            assert_eq!(first.applied, vec![1]);
            assert_eq!(first.current, 1);
            assert_eq!(
                conn.query_row("SELECT name FROM main._schema_migrations")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Text("durable".into()))
            );
            assert_eq!(
                conn.query_row("SELECT name FROM temp._schema_migrations")
                    .await
                    .unwrap()
                    .get(0),
                Some(&SqliteValue::Text("temporary_shadow".into()))
            );
            conn.execute("INSERT INTO durable_data VALUES (1)")
                .await
                .unwrap();
            let second = runner.run(&conn).await.unwrap();
            assert!(second.applied.is_empty());
            assert!(!second.was_fresh);
        });
    }

    #[test]
    fn already_applied_migration_sql_is_not_revalidated() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            MigrationRunner::new()
                .add(1, "baseline", "SELECT 1")
                .run(&conn)
                .await
                .unwrap();
            let result = MigrationRunner::new()
                .add(1, "historical", "COMMIT")
                .add(2, "next", "SELECT 2")
                .run(&conn)
                .await
                .unwrap();
            assert_eq!(result.applied, vec![2]);
            assert_eq!(result.current, 2);
            assert!(!conn.in_transaction());
        });
    }

    #[test]
    fn fresh_database_applies_all_migrations() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
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
                .await
                .unwrap();

            assert!(result.was_fresh);
            assert_eq!(result.applied, vec![1, 2]);
            assert_eq!(result.current, 2);

            // Verify the table exists and has both columns.
            conn.execute("INSERT INTO items (id, name, description) VALUES (1, 'test', 'desc');")
                .await
                .unwrap();
            let rows = conn
                .query("SELECT id, name, description FROM items;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        });
    }

    #[test]
    fn partial_resume_only_applies_new_migrations() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;

            // Apply V1 only.
            let r1 = MigrationRunner::new()
                .add(
                    1,
                    "create_items",
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                )
                .run(&conn)
                .await
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
                .await
                .unwrap();

            assert!(!r2.was_fresh);
            assert_eq!(r2.applied, vec![2]);
            assert_eq!(r2.current, 2);
        });
    }

    #[test]
    fn idempotent_rerun_applies_nothing() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            let runner = MigrationRunner::new().add(
                1,
                "create_items",
                "CREATE TABLE items (id INTEGER PRIMARY KEY)",
            );

            let r1 = runner.run(&conn).await.unwrap();
            assert_eq!(r1.applied, vec![1]);

            let r2 = runner.run(&conn).await.unwrap();
            assert!(r2.applied.is_empty());
            assert_eq!(r2.current, 1);
            assert!(!r2.was_fresh);
        });
    }

    #[test]
    fn failed_migration_rolls_back() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
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

            let err = runner.run(&conn).await;
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
            let r2 = runner2.run(&conn).await.unwrap();
            assert!(!r2.was_fresh);
            assert_eq!(r2.current, 1);
            assert!(r2.applied.is_empty());
        });
    }

    #[test]
    fn multi_statement_migration() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            let result = MigrationRunner::new()
                .add(
                    1,
                    "create_schema",
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL); \
                 CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT NOT NULL)",
                )
                .run(&conn)
                .await
                .unwrap();

            assert_eq!(result.applied, vec![1]);

            // Both tables should exist.
            conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice');")
                .await
                .unwrap();
            conn.execute("INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'hello');")
                .await
                .unwrap();
        });
    }

    #[test]
    fn empty_runner_on_fresh_db() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            let result = MigrationRunner::new().run(&conn).await.unwrap();

            assert!(result.was_fresh);
            assert!(result.applied.is_empty());
            assert_eq!(result.current, 0);
        });
    }

    #[test]
    fn migration_records_name_in_tracking_table() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            MigrationRunner::new()
                .add(
                    1,
                    "initial_schema",
                    "CREATE TABLE t1 (id INTEGER PRIMARY KEY)",
                )
                .add(2, "add_index", "CREATE INDEX idx_t1 ON t1(id)")
                .run(&conn)
                .await
                .unwrap();

            let rows = conn
                .query("SELECT version, name FROM _schema_migrations ORDER BY version;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);

            match rows[0].get(0) {
                Some(SqliteValue::Integer(1)) => {}
                other => panic!("expected Integer(1), got {other:?}"),
            }
            match rows[0].get(1) {
                Some(SqliteValue::Text(s)) if &**s == "initial_schema" => {}
                other => panic!("expected Text('initial_schema'), got {other:?}"),
            }
            match rows[1].get(0) {
                Some(SqliteValue::Integer(2)) => {}
                other => panic!("expected Integer(2), got {other:?}"),
            }
            match rows[1].get(1) {
                Some(SqliteValue::Text(s)) if &**s == "add_index" => {}
                other => panic!("expected Text('add_index'), got {other:?}"),
            }
        });
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

        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&db_path_str).await.unwrap();
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _schema_migrations (\
                    version INTEGER PRIMARY KEY, \
                    name TEXT NOT NULL, \
                    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))\
                );",
            )
            .await
            .unwrap();
            conn.close().await.unwrap();
        });

        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let db_path_str = db_path_str.clone();
                let barrier = Arc::clone(&barrier);
                let migration = migration.clone();
                // Each OS thread drives its own runtime: `Connection` is
                // !Send + !Sync, so the connection must be opened, used, and
                // dropped entirely inside the thread that owns it.
                thread::spawn(move || {
                    let mut applied = false;
                    asupersync::test_utils::run_test(|| async {
                        let conn = Connection::open(&db_path_str).await.unwrap();
                        assert_eq!(
                            MigrationRunner::read_current_version(&conn).await.unwrap(),
                            0
                        );
                        barrier.wait();
                        let apply_result = MigrationRunner::apply_one(&conn, &migration).await;
                        let in_transaction_after_apply = conn.in_transaction();
                        conn.close().await.unwrap();
                        applied = apply_result.unwrap_or_else(|error| {
                            panic!(
                                "concurrent migration failed: {error:?}; \
                                 in_transaction_after_apply={in_transaction_after_apply}"
                            )
                        });
                    });
                    applied
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

        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&db_path_str).await.unwrap();
            let rows = conn
                .query("SELECT version, name FROM _schema_migrations ORDER BY version;")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
            assert_eq!(
                rows[0].get(1),
                Some(&SqliteValue::Text("create_items".into()))
            );
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn apply_one_runs_missing_lower_version_even_if_higher_version_exists() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _schema_migrations (\
                    version INTEGER PRIMARY KEY, \
                    name TEXT NOT NULL, \
                    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))\
                );",
            )
            .await
            .unwrap();
            conn.execute_with_params(
                "INSERT INTO _schema_migrations(version, name) VALUES (?1, ?2);",
                &[
                    SqliteValue::Integer(2),
                    SqliteValue::Text("already_applied".into()),
                ],
            )
            .await
            .unwrap();

            let migration = Migration {
                version: 1,
                name: "outdated",
                up_sql: "CREATE TABLE should_not_exist (id INTEGER PRIMARY KEY);",
            };

            let applied = MigrationRunner::apply_one(&conn, &migration).await.unwrap();
            assert!(applied);
            assert!(
                !conn
                    .query("SELECT name FROM sqlite_master WHERE name = 'should_not_exist';")
                    .await
                    .unwrap()
                    .is_empty(),
                "missing lower-version migration should still run even if a higher version row already exists",
            );
            let versions = conn
                .query("SELECT version FROM _schema_migrations ORDER BY version;")
                .await
                .unwrap();
            assert_eq!(
                versions
                    .iter()
                    .map(|row| row.get(0).unwrap().to_integer())
                    .collect::<Vec<_>>(),
                vec![1, 2],
                "runner must preserve non-contiguous/mixed-binary migration histories instead of treating MAX(version) as authoritative",
            );
        });
    }

    #[test]
    fn run_applies_missing_lower_version_even_if_higher_version_exists() {
        asupersync::test_utils::run_test(|| async {
            let conn = mem_conn().await;
            conn.execute(
                "CREATE TABLE IF NOT EXISTS _schema_migrations (\
                    version INTEGER PRIMARY KEY, \
                    name TEXT NOT NULL\
                );",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO _schema_migrations(version, name) VALUES (2, 'second');")
                .await
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
                .await
                .unwrap();

            assert_eq!(result.applied, vec![1]);
            assert_eq!(result.current, 2);
            assert!(!result.was_fresh);
            assert!(
                !conn
                    .query("SELECT name FROM sqlite_master WHERE name = 'sparse_fixed';")
                    .await
                    .unwrap()
                    .is_empty(),
                "public runner should repair sparse histories by applying the missing lower migration",
            );
            assert!(
                conn.query("SELECT name FROM sqlite_master WHERE name = 'should_not_run';")
                    .await
                    .unwrap()
                    .is_empty(),
                "already-applied higher migration must stay skipped",
            );
        });
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
