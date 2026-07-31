//! Transaction wrapper analogous to `rusqlite::Transaction`.
//!
//! Scoped transactions that should be finalized by awaiting `commit()` or
//! `rollback()`. As in `rusqlite`, an abandoned transaction does not become
//! visible: dropping without an awaited finalizer records a rollback
//! obligation on the connection, which the next SQL entry point discharges
//! before it executes anything else (see [`Drop`] on [`Transaction`]).
//!
//! The rollback is therefore *mandatory* but *deferred*: while the connection
//! remains alive, the next data-bearing async entry point or awaited close
//! settles it before admitting more work. Cancellation republishes the exact
//! cleanup receipt for retry; a returned terminal rollback error poisons the
//! connection so later work remains fail-closed. Dropping the connection itself
//! cannot await cleanup because this crate never builds its own runtime.

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
/// let mut tx = conn.transaction().await?;
/// tx.execute("INSERT INTO users (name) VALUES ('alice')").await?;
/// tx.commit().await?; // Without this, the INSERT is rolled back.
/// ```
pub struct Transaction<'a> {
    conn: &'a Connection,
    finalized: bool,
}

impl<'a> Transaction<'a> {
    async fn new(conn: &'a Connection) -> Result<Self, FrankenError> {
        conn.begin_transaction().await?;
        Ok(Self {
            conn,
            finalized: false,
        })
    }

    /// Commit the transaction.
    ///
    /// If `COMMIT` fails, the transaction remains active so the caller can
    /// inspect the error and choose whether to retry or roll back.
    pub async fn commit(&mut self) -> Result<(), FrankenError> {
        self.conn.commit_transaction().await?;
        self.finalized = true;
        Ok(())
    }

    /// Rollback the transaction explicitly.
    ///
    /// Cancellation leaves a retryable cleanup receipt on the connection. A
    /// returned terminal rollback failure poisons the connection; later
    /// data-bearing operations are rejected rather than retrying a
    /// partly-consumed rollback.
    pub async fn rollback(&mut self) -> Result<(), FrankenError> {
        self.conn.rollback_transaction().await?;
        self.finalized = true;
        Ok(())
    }

    /// Execute a SQL statement within this transaction.
    pub async fn execute(&self, sql: &str) -> Result<usize, FrankenError> {
        self.conn.execute(sql).await
    }

    /// Execute a SQL statement with parameters within this transaction.
    pub async fn execute_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        self.conn.execute_with_params(sql, params).await
    }

    /// Execute a SQL statement with parameters, skipping the internal
    /// statement savepoint when the transaction itself is the rollback
    /// boundary for a prevalidated write batch.
    pub async fn execute_with_params_skip_statement_savepoint(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        self.conn
            .execute_with_params_skip_statement_savepoint_in_explicit_txn(sql, params)
            .await
    }

    /// Execute a SQL statement with `ParamValue` parameters.
    pub async fn execute_compat(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<usize, FrankenError> {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        self.conn.execute_with_params(sql, &values).await
    }

    /// Query within this transaction.
    pub async fn query(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        self.conn.query(sql).await
    }

    /// Query with parameters within this transaction.
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError> {
        self.conn.query_with_params(sql, params).await
    }

    /// Query with `ParamValue` parameters within this transaction.
    pub async fn query_params(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<Vec<Row>, FrankenError> {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        self.conn.query_with_params(sql, &values).await
    }

    /// Query returning exactly one row within this transaction.
    pub async fn query_row(&self, sql: &str) -> Result<Row, FrankenError> {
        self.conn.query_row(sql).await
    }

    /// Query returning exactly one row with parameters within this transaction.
    pub async fn query_row_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError> {
        self.conn.query_row_with_params(sql, params).await
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
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let row = self.conn.query_row_with_params(sql, &values).await?;
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
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let mut mapped = Vec::new();
        self.conn
            .query_with_params_for_each(sql, &values, |row| {
                mapped.push(f(row)?);
                Ok(())
            })
            .await?;
        Ok(mapped)
    }

    /// Execute a string containing multiple SQL statements separated by
    /// semicolons, within this transaction.
    ///
    /// Analogous to `BatchExt::execute_batch` but within a transaction.
    pub async fn execute_batch(&self, sql: &str) -> Result<(), FrankenError> {
        Connection::execute_batch(self.conn, sql).await
    }

    /// Get `last_insert_rowid()` within this transaction.
    pub fn last_insert_rowid(&self) -> Result<i64, FrankenError> {
        Ok(self.conn.last_insert_rowid())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // `rollback_transaction` is `async` and `Drop::drop` cannot await. This
        // crate never builds its own runtime (the `Cx` flows down from the
        // consumer), so the rollback cannot be *finished* here.
        //
        // Record a mandatory connection-owned obligation. While the
        // connection remains alive, its next data-bearing async entry point or
        // awaited close settles rollback before admitting more work.
        // Cancellation republishes the receipt; a terminal failure poisons the
        // connection. Dropping the connection itself cannot await that work.
        if !self.finalized {
            self.conn.mark_transaction_cleanup_required();
            tracing::debug!(
                target: "fsqlite::compat",
                event = "transaction_drop_without_finalize",
                msg = "Transaction dropped without an awaited commit()/rollback(); \
                       it will be rolled back before the next data-bearing operation runs"
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
    /// the connection; the next data-bearing async entry point or awaited close
    /// completes that rollback before admitting more work. A terminal rollback
    /// failure poisons the connection.
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
