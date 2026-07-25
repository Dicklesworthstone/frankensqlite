//! Transaction wrapper analogous to `rusqlite::Transaction`.
//!
//! Scoped transactions that must be finalized by awaiting `commit()` or
//! `rollback()`. Unlike `rusqlite`, dropping does NOT roll back: `Drop::drop`
//! cannot await and this crate never builds its own runtime (see [`Drop`] on
//! [`Transaction`]).

use std::future::Future;

use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

use crate::{Connection, Row};

use super::params::ParamValue;

/// Scoped transaction wrapper. Must be finalized by awaiting `commit()` or
/// `rollback()` — dropping it leaves the transaction open (see [`Drop`]).
///
/// # Examples
///
/// ```ignore
/// use fsqlite::compat::TransactionExt;
///
/// let mut tx = conn.transaction().await?;
/// tx.execute("INSERT INTO users (name) VALUES ('alice')").await?;
/// tx.commit().await?; // Required: dropping `tx` does NOT roll back.
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
    /// If `ROLLBACK` fails, the transaction remains active and drop will make a
    /// best-effort rollback later.
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
        // `rollback_transaction` is `async` in the asupersync storage stack and
        // `Drop::drop` cannot await. This crate never builds its own runtime
        // (the `Cx` flows down from the consumer), so we cannot block here to
        // finish the rollback.
        //
        // Consequence: dropping a `Transaction` without an awaited
        // `commit()`/`rollback()` leaves the transaction open on the connection
        // rather than rolling it back. Callers MUST await one of them.
        if !self.finalized {
            tracing::warn!(
                target: "fsqlite::compat",
                event = "transaction_drop_without_finalize",
                msg = "Transaction dropped without an awaited commit()/rollback(); \
                       the transaction is left open on the connection"
            );
        }
    }
}

/// Extension trait for creating transactions from a `Connection`.
pub trait TransactionExt {
    /// Begin a new transaction.
    ///
    /// The returned `Transaction` must be finalized by awaiting `commit()` or
    /// `rollback()`; dropping it does NOT roll back (see `Drop`).
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

    /// Dropping a `Transaction` does NOT roll back — `Drop::drop` cannot await
    /// and this crate never builds its own runtime. The write therefore stays
    /// in the still-open transaction and remains visible to the owning
    /// connection (read-your-own-write); only an awaited `rollback()` undoes it.
    #[test]
    fn transaction_drop_leaves_txn_open() {
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
                // Dropped without commit()/rollback(): the transaction is left
                // open on `conn` rather than rolled back.
            }

            let rows = conn.query("SELECT val FROM t").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get_typed::<String>(0).unwrap(),
                "not_rolled_back",
                "dropping a Transaction must not roll back the open transaction"
            );

            // The connection is still inside the transaction; finalize it so
            // the test leaves no dangling writer state behind.
            conn.rollback_transaction().await.unwrap();
            let rows = conn.query("SELECT val FROM t").await.unwrap();
            assert!(rows.is_empty(), "explicit rollback must undo the write");
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
