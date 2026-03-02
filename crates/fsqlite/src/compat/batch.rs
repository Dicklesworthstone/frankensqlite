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
        for stmt in split_sql_statements(sql) {
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

/// Split SQL text into individual statements on unquoted semicolons.
///
/// Respects single-quoted string literals (`'...'`) including escaped quotes
/// (`''`), double-quoted identifiers, and SQL comments. This prevents
/// incorrect splitting on semicolons embedded in strings or comments.
fn split_sql_statements(sql: &str) -> Vec<&str> {
    let mut stmts = Vec::new();
    let mut start = 0;
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                // Skip past the closing single quote (handle '' escapes).
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        // '' is an escaped quote, continue scanning.
                        if i < bytes.len() && bytes[i] == b'\'' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                // Skip past the closing double quote (identifiers).
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'"' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                // Line comment: skip to end of line.
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                // Block comment: skip to closing */.
                i += 2;
                while i + 1 < bytes.len() {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b';' => {
                stmts.push(&sql[start..i]);
                i += 1;
                start = i;
            }
            _ => {
                i += 1;
            }
        }
    }

    // Remaining text after the last semicolon.
    if start < sql.len() {
        stmts.push(&sql[start..]);
    }

    stmts
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

    #[test]
    fn execute_batch_semicolons_in_string_literals() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute_batch(
            "
            CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
            INSERT INTO t (val) VALUES ('hello;world');
            INSERT INTO t (val) VALUES ('a''b;c''d');
        ",
        )
        .unwrap();

        let rows = conn.query("SELECT val FROM t ORDER BY id").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get_typed::<String>(0).unwrap(), "hello;world");
        assert_eq!(rows[1].get_typed::<String>(0).unwrap(), "a'b;c'd");
    }

    #[test]
    fn split_sql_statements_basic() {
        let stmts = split_sql_statements("SELECT 1; SELECT 2;");
        // Two statements + trailing empty (which execute_batch skips).
        assert!(stmts.len() >= 2);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1].trim(), "SELECT 2");
    }

    #[test]
    fn split_sql_statements_quoted_semicolons() {
        let stmts = split_sql_statements("INSERT INTO t VALUES ('a;b'); SELECT 1;");
        assert!(stmts.len() >= 2);
        assert_eq!(stmts[0], "INSERT INTO t VALUES ('a;b')");
        assert_eq!(stmts[1].trim(), "SELECT 1");
    }

    #[test]
    fn split_sql_statements_comments() {
        let stmts = split_sql_statements("SELECT 1; -- comment with ; inside\nSELECT 2;");
        assert_eq!(stmts[0], "SELECT 1");
        assert!(stmts[1].contains("SELECT 2"));
    }
}
