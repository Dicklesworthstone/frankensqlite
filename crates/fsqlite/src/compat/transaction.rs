//! Transaction wrapper analogous to `rusqlite::Transaction`.
//!
//! Scoped transactions are created from `&mut Connection` and consumed by
//! `commit(self)` or `rollback(self)`, matching `rusqlite`'s ownership model.
//! Dropping an unfinished wrapper records a generation-qualified rollback
//! obligation. The next data-plane operation settles that obligation before
//! executing, because `Drop::drop` cannot await and this crate never creates a
//! hidden runtime.

use std::future::Future;

use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

use crate::{Connection, Row};

use super::params::ParamValue;

/// Scoped transaction wrapper. Finalize by awaiting `commit()` or
/// `rollback()`; dropping without either rolls back (deferred to the next
/// statement — see [`Drop`]).
///
/// # Examples
///
/// ```ignore
/// use fsqlite::compat::TransactionExt;
///
/// let mut conn = Connection::open(":memory:").await?;
/// let mut tx = conn.transaction().await?;
/// tx.execute("INSERT INTO users (name) VALUES ('alice')").await?;
/// tx.commit().await?; // Without this, the INSERT is rolled back.
/// ```
pub struct Transaction<'a> {
    conn: &'a mut Connection,
    generation: u64,
    finalized: bool,
}

impl<'a> Transaction<'a> {
    async fn new(conn: &'a mut Connection) -> Result<Self, FrankenError> {
        let generation = conn.begin_transaction().await?;
        Ok(Self {
            conn,
            generation,
            finalized: false,
        })
    }

    /// Commit the transaction.
    ///
    /// A failed commit consumes the wrapper and its `Drop` records a rollback
    /// obligation for this exact transaction generation.
    pub async fn commit(mut self) -> Result<(), FrankenError> {
        self.conn
            .commit_transaction_generation(self.generation)
            .await?;
        self.finalized = true;
        Ok(())
    }

    /// Rollback the transaction explicitly.
    ///
    /// A failed rollback consumes the wrapper and leaves its exact generation
    /// pending or fail-closed, depending on how far cleanup progressed.
    pub async fn rollback(mut self) -> Result<(), FrankenError> {
        self.conn
            .rollback_transaction_generation(self.generation)
            .await?;
        self.finalized = true;
        Ok(())
    }

    /// Execute a SQL statement within this transaction.
    pub async fn execute(&mut self, sql: &str) -> Result<usize, FrankenError> {
        self.conn
            .scoped_transaction_execute_with_params(self.generation, sql, &[], false)
            .await
    }

    /// Execute a SQL statement with parameters within this transaction.
    pub async fn execute_with_params(
        &mut self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        self.conn
            .scoped_transaction_execute_with_params(self.generation, sql, params, false)
            .await
    }

    /// Execute a SQL statement with parameters, skipping the internal
    /// statement savepoint when the transaction itself is the rollback
    /// boundary for a prevalidated write batch.
    pub async fn execute_with_params_skip_statement_savepoint(
        &mut self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        self.conn
            .scoped_transaction_execute_with_params(self.generation, sql, params, true)
            .await
    }

    /// Execute a SQL statement with `ParamValue` parameters.
    pub async fn execute_compat(
        &mut self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<usize, FrankenError> {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        self.conn
            .scoped_transaction_execute_with_params(self.generation, sql, &values, false)
            .await
    }

    /// Query within this transaction.
    pub async fn query(&mut self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        self.conn
            .scoped_transaction_query_with_params(self.generation, sql, &[])
            .await
    }

    /// Query with parameters within this transaction.
    pub async fn query_with_params(
        &mut self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError> {
        self.conn
            .scoped_transaction_query_with_params(self.generation, sql, params)
            .await
    }

    /// Query with `ParamValue` parameters within this transaction.
    pub async fn query_params(
        &mut self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<Vec<Row>, FrankenError> {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        self.conn
            .scoped_transaction_query_with_params(self.generation, sql, &values)
            .await
    }

    /// Query returning exactly one row within this transaction.
    pub async fn query_row(&mut self, sql: &str) -> Result<Row, FrankenError> {
        self.conn
            .scoped_transaction_query_row_with_params(self.generation, sql, &[])
            .await
    }

    /// Query returning exactly one row with parameters within this transaction.
    pub async fn query_row_with_params(
        &mut self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError> {
        self.conn
            .scoped_transaction_query_row_with_params(self.generation, sql, params)
            .await
    }

    /// Execute a query that returns exactly one row, mapping it with `f`.
    ///
    /// Analogous to `ConnectionExt::query_row_map` but within a transaction.
    pub async fn query_row_map<T, F>(
        &mut self,
        sql: &str,
        params: &[ParamValue],
        f: F,
    ) -> Result<T, FrankenError>
    where
        F: FnOnce(&Row) -> Result<T, FrankenError>,
    {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let row = self
            .conn
            .scoped_transaction_query_row_with_params(self.generation, sql, &values)
            .await?;
        f(&row)
    }

    /// Execute a query and collect all rows into a `Vec<T>` via mapping closure.
    ///
    /// Analogous to `ConnectionExt::query_map_collect` but within a transaction.
    pub async fn query_map_collect<T, F>(
        &mut self,
        sql: &str,
        params: &[ParamValue],
        mut f: F,
    ) -> Result<Vec<T>, FrankenError>
    where
        F: FnMut(&Row) -> Result<T, FrankenError>,
    {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        self.conn
            .scoped_transaction_query_with_params(self.generation, sql, &values)
            .await?
            .iter()
            .map(&mut f)
            .collect()
    }

    /// Execute a string containing multiple SQL statements separated by
    /// semicolons, within this transaction.
    ///
    /// Analogous to `BatchExt::execute_batch` but within a transaction.
    pub async fn execute_batch(&mut self, sql: &str) -> Result<(), FrankenError> {
        self.conn
            .scoped_transaction_execute_with_params(self.generation, sql, &[], false)
            .await
            .map(drop)
    }

    /// Get `last_insert_rowid()` within this transaction.
    pub fn last_insert_rowid(&self) -> Result<i64, FrankenError> {
        self.conn
            .scoped_transaction_last_insert_rowid(self.generation)
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
        if !self.finalized {
            self.conn.mark_transaction_cleanup_required(self.generation);
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
    /// executing the caller's statement.
    fn transaction(&mut self) -> impl Future<Output = Result<Transaction<'_>, FrankenError>>;
}

impl TransactionExt for Connection {
    async fn transaction(&mut self) -> Result<Transaction<'_>, FrankenError> {
        Transaction::new(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::RowExt;

    #[test]
    fn transaction_commit() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
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

    /// Dropping a `Transaction` records a deferred rollback obligation because
    /// `Drop::drop` cannot await and this crate never builds its own runtime.
    /// The next SQL entry point must settle that obligation before it executes,
    /// so abandoned writes are never visible to that later statement.
    #[test]
    fn transaction_drop_rolls_back_before_next_statement() {
        asupersync::test_utils::run_test(|| async {
            let mut conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();

            {
                let mut tx = conn.transaction().await.unwrap();
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
            let mut conn = Connection::open(":memory:").await.unwrap();
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
