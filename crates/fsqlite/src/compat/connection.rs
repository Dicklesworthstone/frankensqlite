//! Connection extension traits for rusqlite-style query patterns.

use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

use crate::{Connection, Row};

use super::params::ParamValue;

/// Extension trait adding rusqlite-style query methods to `Connection`.
///
/// These wrap fsqlite's `query_with_params` / `query_row_with_params` to
/// accept a mapping closure, matching the ergonomics of `rusqlite::Connection`.
pub trait ConnectionExt {
    /// Execute a query that returns exactly one row, mapping it with `f`.
    ///
    /// Returns `FrankenError::QueryReturnedNoRows` if no rows match.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use fsqlite::compat::{ConnectionExt, RowExt, params};
    ///
    /// let count: i64 = conn.query_row_map(
    ///     "SELECT count(*) FROM users WHERE active = ?1",
    ///     params![true],
    ///     |row| row.get_typed(0),
    /// )?;
    /// ```
    fn query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        f: F,
    ) -> Result<T, FrankenError>
    where
        F: FnOnce(&Row) -> Result<T, FrankenError>;

    /// Execute a query and collect all rows into a `Vec<T>` via mapping closure.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use fsqlite::compat::{ConnectionExt, RowExt, params};
    ///
    /// let names: Vec<String> = conn.query_map_collect(
    ///     "SELECT name FROM users WHERE active = ?1",
    ///     params![true],
    ///     |row| row.get_typed(0),
    /// )?;
    /// ```
    fn query_map_collect<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        f: F,
    ) -> Result<Vec<T>, FrankenError>
    where
        F: FnMut(&Row) -> Result<T, FrankenError>;

    /// Execute a SQL statement with `ParamValue` parameters, returning affected row count.
    fn execute_params(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<usize, FrankenError>;
}

impl ConnectionExt for Connection {
    fn query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        f: F,
    ) -> Result<T, FrankenError>
    where
        F: FnOnce(&Row) -> Result<T, FrankenError>,
    {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let row = self.query_row_with_params(sql, &values)?;
        f(&row)
    }

    fn query_map_collect<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        mut f: F,
    ) -> Result<Vec<T>, FrankenError>
    where
        F: FnMut(&Row) -> Result<T, FrankenError>,
    {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let rows = self.query_with_params(sql, &values)?;
        rows.iter().map(|row| f(row)).collect()
    }

    fn execute_params(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<usize, FrankenError> {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        self.execute_with_params(sql, &values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::RowExt;

    #[test]
    fn query_row_map_returns_value() {
        let conn = Connection::open(":memory:").unwrap();
        let result: i64 = conn
            .query_row_map("SELECT 42", &[], |row| row.get_typed(0))
            .unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn query_row_map_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let p = [ParamValue::from(10_i64), ParamValue::from(32_i64)];
        let result: i64 = conn
            .query_row_map("SELECT ?1 + ?2", &p, |row| row.get_typed(0))
            .unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn query_map_collect_returns_vec() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('a')").unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('b')").unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('c')").unwrap();

        let results: Vec<String> = conn
            .query_map_collect("SELECT val FROM t ORDER BY id", &[], |row| {
                row.get_typed(0)
            })
            .unwrap();
        assert_eq!(results, vec!["a", "b", "c"]);
    }

    #[test]
    fn execute_params_with_values() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        let p = [ParamValue::from(1_i64), ParamValue::from("alice")];
        let affected = conn.execute_params("INSERT INTO t VALUES (?1, ?2)", &p).unwrap();
        assert_eq!(affected, 1);
    }
}
