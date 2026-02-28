//! Transaction wrapper analogous to `rusqlite::Transaction`.
//!
//! Provides RAII-style transactions: auto-rollback on drop unless explicitly
//! committed.

use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

use crate::{Connection, Row};

use super::params::ParamValue;

/// RAII transaction wrapper. If dropped without calling `commit()`, the
/// transaction is automatically rolled back.
///
/// # Examples
///
/// ```ignore
/// use fsqlite::compat::TransactionExt;
///
/// let tx = conn.transaction()?;
/// tx.execute("INSERT INTO users (name) VALUES ('alice')")?;
/// tx.commit()?; // Without this, the insert is rolled back on drop.
/// ```
pub struct Transaction<'a> {
    conn: &'a Connection,
    committed: bool,
}

impl<'a> Transaction<'a> {
    fn new(conn: &'a Connection) -> Result<Self, FrankenError> {
        conn.execute("BEGIN")?;
        Ok(Self {
            conn,
            committed: false,
        })
    }

    /// Commit the transaction.
    pub fn commit(mut self) -> Result<(), FrankenError> {
        self.conn.execute("COMMIT")?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction explicitly.
    pub fn rollback(mut self) -> Result<(), FrankenError> {
        self.conn.execute("ROLLBACK")?;
        self.committed = true; // Prevent double-rollback in drop
        Ok(())
    }

    /// Execute a SQL statement within this transaction.
    pub fn execute(&self, sql: &str) -> Result<usize, FrankenError> {
        self.conn.execute(sql)
    }

    /// Execute a SQL statement with parameters within this transaction.
    pub fn execute_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        self.conn.execute_with_params(sql, params)
    }

    /// Execute a SQL statement with `ParamValue` parameters.
    pub fn execute_params(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<usize, FrankenError> {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        self.conn.execute_with_params(sql, &values)
    }

    /// Query within this transaction.
    pub fn query(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        self.conn.query(sql)
    }

    /// Query with parameters within this transaction.
    pub fn query_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError> {
        self.conn.query_with_params(sql, params)
    }

    /// Query returning exactly one row within this transaction.
    pub fn query_row(&self, sql: &str) -> Result<Row, FrankenError> {
        self.conn.query_row(sql)
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // Best-effort rollback; ignore errors since we're in drop.
            let _ = self.conn.execute("ROLLBACK");
        }
    }
}

/// Extension trait for creating transactions from a `Connection`.
pub trait TransactionExt {
    /// Begin a new transaction. The returned `Transaction` auto-rolls back on
    /// drop unless `commit()` is called.
    fn transaction(&self) -> Result<Transaction<'_>, FrankenError>;
}

impl TransactionExt for Connection {
    fn transaction(&self) -> Result<Transaction<'_>, FrankenError> {
        Transaction::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::RowExt;

    #[test]
    fn transaction_commit() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        let tx = conn.transaction().unwrap();
        tx.execute("INSERT INTO t (val) VALUES ('committed')")
            .unwrap();
        tx.commit().unwrap();

        let rows = conn.query("SELECT val FROM t").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_typed::<String>(0).unwrap(), "committed");
    }

    #[test]
    fn transaction_rollback_on_drop() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        {
            let tx = conn.transaction().unwrap();
            tx.execute("INSERT INTO t (val) VALUES ('rolled_back')")
                .unwrap();
            // tx dropped without commit → auto-rollback
        }

        let rows = conn.query("SELECT val FROM t").unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn transaction_explicit_rollback() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        let tx = conn.transaction().unwrap();
        tx.execute("INSERT INTO t (val) VALUES ('rolled_back')")
            .unwrap();
        tx.rollback().unwrap();

        let rows = conn.query("SELECT val FROM t").unwrap();
        assert!(rows.is_empty());
    }
}
