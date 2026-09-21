//! Transaction wrapper analogous to `rusqlite::Transaction`.
//!
//! Scoped transactions that should be finalized by awaiting `commit()` or
//! `rollback()`. As in `rusqlite`, an abandoned transaction does not become
//! visible: dropping without an awaited finalizer records a rollback
//! obligation on the connection, which the next SQL entry point discharges
//! before it executes anything else (see [`Drop`] on [`Transaction`]).
//!
//! The rollback is therefore *guaranteed* but *deferred* -- it completes at
//! the next statement rather than inside `Drop`, because `Drop::drop` cannot
//! await and this crate never builds its own runtime.

use std::{cell::Cell, future::Future};

use fsqlite_ast::Statement;
use fsqlite_error::FrankenError;
use fsqlite_parser::Parser;
use fsqlite_types::value::SqliteValue;

use crate::{Connection, Row};

use super::params::ParamValue;

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
mod retry;
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use retry::{RetryPolicy, RetryStopReason, TransactionRetryError, TransactionRetryExt};

/// Scoped transaction wrapper. Finalize by awaiting `commit()` or
/// `rollback()`; dropping without either rolls back (deferred to the next
/// statement — see [`Drop`]).
///
/// # Examples
///
/// ```ignore
/// use fsqlite::compat::TransactionExt;
///
/// let mut tx = conn.transaction().await?;
/// tx.execute("INSERT INTO users (name) VALUES ('alice')").await?;
/// tx.commit().await?; // Without this, the INSERT is rolled back.
/// ```
pub struct Transaction<'a> {
    conn: &'a Connection,
    finalized: Cell<bool>,
    // Only the retry owner may end a replayable transaction. Ordinary scoped
    // transactions keep their existing SQL transaction-control behavior.
    allow_sql_transaction_control: bool,
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    retryable_abort: Cell<bool>,
}

impl<'a> Transaction<'a> {
    async fn new(conn: &'a Connection) -> Result<Self, FrankenError> {
        Self::new_with_begin(conn, conn.begin_transaction()).await
    }

    // Keep admission and its rollback ownership in one path. Tests can suspend
    // or fail admission after real SQL without adding engine-global hooks.
    async fn new_with_begin(
        conn: &'a Connection,
        begin: impl Future<Output = Result<(), FrankenError>>,
    ) -> Result<Self, FrankenError> {
        // Core begin_transaction settles a dropped wrapper before BEGIN. Do
        // that same settlement before our ownership check, or a replacement
        // transaction would spuriously fail NestedTransaction after Drop.
        // An empty batch settles obligations but executes no SQL statements,
        // invokes no row callbacks, and does not acquire a read snapshot.
        conn.execute_batch("").await?;
        if conn.in_transaction() {
            return Err(FrankenError::NestedTransaction);
        }

        // Guard ownership BEFORE polling BEGIN: admission may leave an active
        // transaction and then fail, or this future may be dropped while it is
        // Pending. A live caller's transaction was excluded above.
        let transaction = Self {
            conn,
            finalized: Cell::new(false),
            allow_sql_transaction_control: true,
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            retryable_abort: Cell::new(false),
        };
        transaction.observe_transaction_state(begin.await)?;
        transaction.ensure_active()?;
        Ok(transaction)
    }

    fn ensure_active(&self) -> Result<(), FrankenError> {
        if self.finalized.get() {
            return Err(FrankenError::NoActiveTransaction);
        }
        if !self.conn.in_transaction() {
            self.finalized.set(true);
            return Err(FrankenError::NoActiveTransaction);
        }
        Ok(())
    }

    fn ensure_sql_allowed(&self, sql: &str) -> Result<(), FrankenError> {
        self.ensure_active()?;
        if self.allow_sql_transaction_control {
            return Ok(());
        }

        // A final in_transaction() check cannot detect COMMIT; BEGIN in a
        // single batch, or undo writes already committed before a later error.
        // Validate the complete input before invoking ANY engine entry point.
        // Parsing, rather than token/semicolon matching, preserves trigger
        // bodies, quoted keywords and ROLLBACK TO nested savepoints.
        let (statements, errors) = Parser::from_sql(sql).parse_all();
        if let Some(error) = errors.first() {
            return Err(FrankenError::ParseError {
                offset: error.span.start as usize,
                detail: error.to_string(),
            });
        }
        if statements.iter().any(|statement| match statement {
            Statement::Begin(_) | Statement::Commit => true,
            Statement::Rollback(rollback) => rollback.to_savepoint.is_none(),
            _ => false,
        }) {
            return Err(FrankenError::FunctionError(
                "transaction retry callback cannot execute BEGIN, COMMIT/END or full ROLLBACK"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn observe_transaction_state<T>(
        &self,
        result: Result<T, FrankenError>,
    ) -> Result<T, FrankenError> {
        if !self.conn.in_transaction() {
            self.finalized.set(true);
            // Only an observed transient engine failure certifies a retryable
            // abort. Successful SQL COMMIT/ROLLBACK must never gain that
            // status merely because the callback later returns Busy.
            #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
            self.retryable_abort
                .set(result.as_ref().is_err_and(FrankenError::is_transient));
        }
        result
    }

    /// Commit the transaction.
    ///
    /// If `COMMIT` fails, the transaction remains active so the caller can
    /// inspect the error and choose whether to retry or roll back.
    pub async fn commit(&mut self) -> Result<(), FrankenError> {
        self.ensure_active()?;
        let result = self.conn.commit_transaction().await;
        self.observe_transaction_state(result)
    }

    /// Rollback the transaction explicitly.
    ///
    /// If `ROLLBACK` fails, the transaction remains active and drop will make a
    /// best-effort rollback later.
    pub async fn rollback(&mut self) -> Result<(), FrankenError> {
        self.ensure_active()?;
        let result = self.conn.rollback_transaction().await;
        self.observe_transaction_state(result)
    }

    /// Execute a SQL statement within this transaction.
    pub async fn execute(&self, sql: &str) -> Result<usize, FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let result = self.conn.execute(sql).await;
        self.observe_transaction_state(result)
    }

    /// Execute a SQL statement with parameters within this transaction.
    pub async fn execute_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let result = self.conn.execute_with_params(sql, params).await;
        self.observe_transaction_state(result)
    }

    /// Execute a SQL statement with parameters, skipping the internal
    /// statement savepoint when the transaction itself is the rollback
    /// boundary for a prevalidated write batch.
    pub async fn execute_with_params_skip_statement_savepoint(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let result = self
            .conn
            .execute_with_params_skip_statement_savepoint_in_explicit_txn(sql, params)
            .await;
        self.observe_transaction_state(result)
    }

    /// Execute a SQL statement with `ParamValue` parameters.
    pub async fn execute_compat(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<usize, FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let result = self.conn.execute_with_params(sql, &values).await;
        self.observe_transaction_state(result)
    }

    /// Query within this transaction.
    pub async fn query(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let result = self.conn.query(sql).await;
        self.observe_transaction_state(result)
    }

    /// Query with parameters within this transaction.
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let result = self.conn.query_with_params(sql, params).await;
        self.observe_transaction_state(result)
    }

    /// Query with `ParamValue` parameters within this transaction.
    pub async fn query_params(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<Vec<Row>, FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let result = self.conn.query_with_params(sql, &values).await;
        self.observe_transaction_state(result)
    }

    /// Query returning exactly one row within this transaction.
    pub async fn query_row(&self, sql: &str) -> Result<Row, FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let result = self.conn.query_row(sql).await;
        self.observe_transaction_state(result)
    }

    /// Query returning exactly one row with parameters within this transaction.
    pub async fn query_row_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let result = self.conn.query_row_with_params(sql, params).await;
        self.observe_transaction_state(result)
    }

    /// Execute a query that returns exactly one row, mapping it with `f`.
    ///
    /// Analogous to `ConnectionExt::query_row_map` but within a transaction.
    pub async fn query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        f: F,
    ) -> Result<T, FrankenError>
    where
        F: FnOnce(&Row) -> Result<T, FrankenError>,
    {
        self.ensure_sql_allowed(sql)?;
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let result = self.conn.query_row_with_params(sql, &values).await;
        let row = self.observe_transaction_state(result)?;
        f(&row)
    }

    /// Execute a query and collect all rows into a `Vec<T>` via mapping closure.
    ///
    /// Analogous to `ConnectionExt::query_map_collect` but within a transaction.
    pub async fn query_map_collect<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        mut f: F,
    ) -> Result<Vec<T>, FrankenError>
    where
        F: FnMut(&Row) -> Result<T, FrankenError>,
    {
        self.ensure_sql_allowed(sql)?;
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let mut mapped = Vec::new();
        let result = self
            .conn
            .query_with_params_for_each(sql, &values, |row| {
                mapped.push(f(row)?);
                Ok(())
            })
            .await;
        self.observe_transaction_state(result)?;
        Ok(mapped)
    }

    /// Execute a string containing multiple SQL statements separated by
    /// semicolons, within this transaction.
    ///
    /// Analogous to `BatchExt::execute_batch` but within a transaction.
    pub async fn execute_batch(&self, sql: &str) -> Result<(), FrankenError> {
        self.ensure_sql_allowed(sql)?;
        let result = Connection::execute_batch(self.conn, sql).await;
        self.observe_transaction_state(result)
    }

    /// Get `last_insert_rowid()` within this transaction.
    pub fn last_insert_rowid(&self) -> Result<i64, FrankenError> {
        self.ensure_active()?;
        Ok(self.conn.last_insert_rowid())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // `rollback_transaction` is `async` and `Drop::drop` cannot await. This
        // crate never builds its own runtime (the `Cx` flows down from the
        // consumer), so the rollback cannot be *finished* here.
        //
        // It can still be *guaranteed*. We record the obligation on the
        // connection; the next SQL entry point discharges it by rolling back
        // before it runs anything else. That preserves the observable
        // rusqlite contract -- an abandoned transaction's writes are never
        // visible to a later statement -- without blocking in `Drop` and
        // without owning a runtime.
        if !self.finalized.get() {
            self.conn.mark_transaction_cleanup_required();
            tracing::debug!(
                target: "fsqlite::compat",
                event = "transaction_drop_without_finalize",
                msg = "Transaction dropped without an awaited commit()/rollback(); \
                       it will be rolled back before the next statement runs"
            );
        }
    }
}

/// Extension trait for creating transactions from a `Connection`.
pub trait TransactionExt {
    /// Begin a new transaction.
    ///
    /// The returned `Transaction` must be finalized by awaiting `commit()` or
    /// `rollback()`. Dropping it records a mandatory rollback obligation on
    /// the connection; the next SQL entry point completes that rollback before
    /// executing the caller's statement. The same obligation protects failed
    /// or abandoned BEGIN admission, before a wrapper has been returned.
    ///
    /// A previous abandoned wrapper is settled first. A still-live caller
    /// transaction is refused without rolling it back. Do not use the
    /// connection through another alias while construction is pending.
    fn transaction(&self) -> impl Future<Output = Result<Transaction<'_>, FrankenError>>;
}

impl TransactionExt for Connection {
    async fn transaction(&self) -> Result<Transaction<'_>, FrankenError> {
        Transaction::new(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::RowExt;

    #[test]
    fn replayable_sql_preflight_covers_every_execute_and_query_entry_point() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE data(value INTEGER)").await.unwrap();
            let mut tx = conn.transaction().await.unwrap();
            tx.allow_sql_transaction_control = false;
            tx.execute("INSERT INTO data VALUES (1)").await.unwrap();
            let sql = "INSERT INTO data VALUES (2); COMMIT; BEGIN; SELECT 3";
            let callbacks = Cell::new(0);
            macro_rules! refused {
                ($operation:expr) => {
                    assert!(matches!(
                        $operation.await,
                        Err(FrankenError::FunctionError(message))
                            if message.contains("transaction retry callback")
                    ));
                };
            }
            refused!(tx.execute(sql));
            refused!(tx.execute_with_params(sql, &[]));
            refused!(tx.execute_with_params_skip_statement_savepoint(sql, &[]));
            refused!(tx.execute_compat(sql, &[]));
            refused!(tx.execute_batch(sql));
            refused!(tx.query(sql));
            refused!(tx.query_with_params(sql, &[]));
            refused!(tx.query_params(sql, &[]));
            refused!(tx.query_row(sql));
            refused!(tx.query_row_with_params(sql, &[]));
            refused!(tx.query_row_map(sql, &[], |_| {
                callbacks.set(callbacks.get() + 1);
                Ok(())
            }));
            refused!(tx.query_map_collect(sql, &[], |_| {
                callbacks.set(callbacks.get() + 1);
                Ok(())
            }));
            assert_eq!(callbacks.get(), 0);
            assert!(!tx.finalized.get());
            assert!(conn.in_transaction());
            let rows = tx.query("SELECT value FROM data").await.unwrap();
            assert_eq!(rows.len(), 1, "no prefix of a refused batch may run");
            assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
            tx.rollback().await.unwrap();
            assert!(conn.query("SELECT value FROM data").await.unwrap().is_empty());
        });
    }

    #[test]
    fn replayable_preflight_preserves_savepoints_triggers_and_quoted_keywords() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let mut tx = conn.transaction().await.unwrap();
            tx.allow_sql_transaction_control = false;
            tx.execute_batch(
                "CREATE TABLE data(value TEXT); CREATE TABLE audit(value TEXT); \
                 CREATE TRIGGER log_insert AFTER INSERT ON data BEGIN \
                     INSERT INTO audit VALUES ('COMMIT; BEGIN; ROLLBACK'); END; \
                 SAVEPOINT inner_scope; INSERT INTO data VALUES ('discard'); \
                 ROLLBACK TO inner_scope; RELEASE inner_scope; \
                 /* COMMIT */ INSERT INTO data VALUES ('keep');",
            )
            .await
            .unwrap();
            assert_eq!(tx.query("SELECT value FROM data").await.unwrap().len(), 1);
            assert_eq!(tx.query("SELECT value FROM audit").await.unwrap().len(), 1);
            tx.commit().await.unwrap();
            assert_eq!(
                conn.query_row("SELECT value FROM data").await.unwrap().get(0),
                Some(&SqliteValue::Text("keep".into()))
            );
        });
    }

    #[test]
    fn new_transaction_settles_previous_abandoned_scope() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE data(value INTEGER)").await.unwrap();
            let previous = conn.transaction().await.unwrap();
            previous.execute("INSERT INTO data VALUES (1)").await.unwrap();
            drop(previous);
            assert!(conn.in_transaction(), "Drop records rather than executes rollback");

            // No intervening query: transaction() itself must settle the old
            // scope rather than treating the abandoned transaction as live.
            let mut replacement = conn.transaction().await.unwrap();
            assert!(replacement.query("SELECT value FROM data").await.unwrap().is_empty());
            replacement.execute("INSERT INTO data VALUES (2)").await.unwrap();
            replacement.commit().await.unwrap();
            assert_eq!(
                conn.query_row("SELECT value FROM data").await.unwrap().get(0),
                Some(&SqliteValue::Integer(2))
            );
        });
    }

    #[test]
    fn nested_transaction_refusal_preserves_live_wrapper() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE data(value INTEGER)").await.unwrap();
            let mut caller = conn.transaction().await.unwrap();
            caller.execute("INSERT INTO data VALUES (7)").await.unwrap();
            let refused = conn.transaction().await;
            assert!(matches!(refused, Err(FrankenError::NestedTransaction)));
            assert!(conn.in_transaction());
            assert_eq!(
                caller.query_row("SELECT value FROM data").await.unwrap().get(0),
                Some(&SqliteValue::Integer(7))
            );
            caller.commit().await.unwrap();
            assert_eq!(conn.query("SELECT value FROM data").await.unwrap().len(), 1);
        });
    }

    #[test]
    fn failed_admission_retires_partial_transaction_before_next_begin() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE data(value INTEGER)").await.unwrap();
            let refused = Transaction::new_with_begin(&conn, async {
                conn.begin_transaction().await?;
                conn.execute("INSERT INTO data VALUES (1)").await?;
                Err(FrankenError::Busy)
            })
            .await;
            assert!(matches!(refused, Err(FrankenError::Busy)));
            assert!(conn.in_transaction(), "failed admission must leave an owned cleanup obligation");

            let mut replacement = conn.transaction().await.unwrap();
            assert!(replacement.query("SELECT value FROM data").await.unwrap().is_empty());
            replacement.execute("INSERT INTO data VALUES (2)").await.unwrap();
            replacement.commit().await.unwrap();
            assert_eq!(
                conn.query_row("SELECT value FROM data").await.unwrap().get(0),
                Some(&SqliteValue::Integer(2))
            );
        });
    }

    #[test]
    fn dropped_admission_future_retires_partial_transaction() {
        asupersync::test_utils::run_test(|| async {
            use std::future::{pending, poll_fn};
            use std::task::Poll;

            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE data(value INTEGER)").await.unwrap();
            let parked = Cell::new(false);
            let mut admission = Box::pin(Transaction::new_with_begin(&conn, async {
                conn.begin_transaction().await?;
                conn.execute("INSERT INTO data VALUES (1)").await?;
                parked.set(true);
                pending::<Result<(), FrankenError>>().await
            }));
            poll_fn(|cx| {
                assert!(admission.as_mut().poll(cx).is_pending());
                if parked.get() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            assert!(conn.in_transaction());
            drop(admission);

            assert!(conn.query("SELECT value FROM data").await.unwrap().is_empty());
            assert!(!conn.in_transaction());
            let mut replacement = conn.transaction().await.unwrap();
            replacement.execute("INSERT INTO data VALUES (2)").await.unwrap();
            replacement.commit().await.unwrap();
            assert_eq!(
                conn.query_row("SELECT value FROM data").await.unwrap().get(0),
                Some(&SqliteValue::Integer(2))
            );
        });
    }

    #[test]
    fn admission_error_before_begin_does_not_poison_later_scope() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let refused = Transaction::new_with_begin(
                &conn,
                std::future::ready(Err(FrankenError::BusyRecovery)),
            )
            .await;
            assert!(matches!(refused, Err(FrankenError::BusyRecovery)));
            assert!(!conn.in_transaction());
            let mut replacement = conn.transaction().await.unwrap();
            replacement.execute("CREATE TABLE data(value INTEGER)").await.unwrap();
            replacement.execute("INSERT INTO data VALUES (3)").await.unwrap();
            replacement.commit().await.unwrap();
            assert_eq!(
                conn.query_row("SELECT value FROM data").await.unwrap().get(0),
                Some(&SqliteValue::Integer(3))
            );
        });
    }

    #[test]
    fn successful_admission_requires_an_active_transaction() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            let refused = Transaction::new_with_begin(&conn, std::future::ready(Ok(()))).await;
            assert!(matches!(refused, Err(FrankenError::NoActiveTransaction)));
            assert!(!conn.in_transaction());
            let mut replacement = conn.transaction().await.unwrap();
            replacement.execute("CREATE TABLE data(value INTEGER)").await.unwrap();
            replacement.commit().await.unwrap();
            assert!(conn.query("SELECT value FROM data").await.unwrap().is_empty());
        });
    }

    #[test]
    fn transaction_commit() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();

            let mut tx = conn.transaction().await.unwrap();
            tx.execute("INSERT INTO t (val) VALUES ('committed')")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            let rows = conn.query("SELECT val FROM t").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get_typed::<String>(0).unwrap(), "committed");
        });
    }

    #[test]
    fn finalized_transaction_rejects_later_operations() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();

            let mut tx = conn.transaction().await.unwrap();
            tx.execute("INSERT INTO t (val) VALUES ('committed')")
                .await
                .unwrap();
            tx.commit().await.unwrap();

            let error = tx
                .execute("INSERT INTO t (val) VALUES ('must_not_autocommit')")
                .await
                .expect_err("a finalized transaction wrapper must reject later statements");
            assert!(matches!(error, FrankenError::NoActiveTransaction));

            let rows = conn.query("SELECT val FROM t ORDER BY id").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get_typed::<String>(0).unwrap(), "committed");
        });
    }

    #[test]
    fn transaction_rejects_operations_after_sql_ends_underlying_scope() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();

            let tx = conn.transaction().await.unwrap();
            tx.execute("COMMIT").await.unwrap();

            let mut replacement = conn.transaction().await.unwrap();

            let error = tx
                .execute("INSERT INTO t (val) VALUES ('must_not_autocommit')")
                .await
                .expect_err("a wrapper must reject statements after SQL ends its transaction");
            assert!(matches!(error, FrankenError::NoActiveTransaction));
            drop(tx);

            replacement
                .execute("INSERT INTO t (val) VALUES ('replacement_transaction')")
                .await
                .unwrap();
            replacement.commit().await.unwrap();
            let rows = conn.query("SELECT val FROM t ORDER BY id").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get_typed::<String>(0).unwrap(),
                "replacement_transaction"
            );
        });
    }

    /// Dropping a `Transaction` records a deferred rollback obligation because
    /// `Drop::drop` cannot await and this crate never builds its own runtime.
    /// The next SQL entry point must settle that obligation before it executes,
    /// so abandoned writes are never visible to that later statement.
    #[test]
    fn transaction_drop_rolls_back_before_next_statement() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();

            {
                let tx = conn.transaction().await.unwrap();
                tx.execute("INSERT INTO t (val) VALUES ('not_rolled_back')")
                    .await
                    .unwrap();
                // Dropped without commit()/rollback(): the connection records a
                // rollback obligation for the next SQL entry point.
            }

            let rows = conn.query("SELECT val FROM t").await.unwrap();
            assert!(
                rows.is_empty(),
                "the next statement must roll back an abandoned transaction before it reads"
            );
            assert!(
                !conn.in_transaction(),
                "settling the deferred rollback must leave the connection idle"
            );
        });
    }

    #[test]
    fn transaction_explicit_rollback() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();

            let mut tx = conn.transaction().await.unwrap();
            tx.execute("INSERT INTO t (val) VALUES ('rolled_back')")
                .await
                .unwrap();
            tx.rollback().await.unwrap();

            let rows = conn.query("SELECT val FROM t").await.unwrap();
            assert!(rows.is_empty());
        });
    }
}
