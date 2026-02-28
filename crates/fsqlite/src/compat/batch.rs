//! `execute_batch` support, analogous to `rusqlite::Connection::execute_batch`.

use fsqlite_error::FrankenError;

use crate::Connection;

/// Extension trait for executing multiple SQL statements in a batch.
pub trait BatchExt {
    /// Execute a string containing multiple SQL statements separated by
    /// semicolons. Each statement is executed in order; an error in any
    /// statement stops execution and returns that error.
    ///
    /// This is the fsqlite equivalent of `rusqlite::Connection::execute_batch`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use fsqlite::compat::BatchExt;
    ///
    /// conn.execute_batch("
    ///     PRAGMA journal_mode = WAL;
    ///     CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);
    ///     CREATE INDEX IF NOT EXISTS idx_name ON users(name);
    /// ")?;
    /// ```
    fn execute_batch(&self, sql: &str) -> Result<(), FrankenError>;
}

impl BatchExt for Connection {
    fn execute_batch(&self, sql: &str) -> Result<(), FrankenError> {
        for stmt in sql.split(';') {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            // Add semicolon back for the parser.
            let full_stmt = format!("{trimmed};");
            self.execute(&full_stmt)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::RowExt;

    #[test]
    fn execute_batch_creates_tables() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute_batch(
            "
            CREATE TABLE a (id INTEGER PRIMARY KEY);
            CREATE TABLE b (id INTEGER PRIMARY KEY);
            INSERT INTO a (id) VALUES (1);
            INSERT INTO b (id) VALUES (2);
        ",
        )
        .unwrap();

        let a: i64 = conn
            .query_row("SELECT id FROM a")
            .map(|row| row.get_typed::<i64>(0).unwrap())
            .unwrap();
        assert_eq!(a, 1);

        let b: i64 = conn
            .query_row("SELECT id FROM b")
            .map(|row| row.get_typed::<i64>(0).unwrap())
            .unwrap();
        assert_eq!(b, 2);
    }

    #[test]
    fn execute_batch_empty_string() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute_batch("").unwrap();
        conn.execute_batch("   ;  ;  ").unwrap();
    }
}
