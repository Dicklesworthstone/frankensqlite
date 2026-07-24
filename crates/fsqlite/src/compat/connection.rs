//! Connection extension traits for rusqlite-style query patterns.

use std::future::Future;

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
    /// Returns `FrankenError::QueryReturnedNoRows` if no rows match and
    /// `FrankenError::QueryReturnedMultipleRows` if more than one row matches.
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
    ) -> impl Future<Output = Result<T, FrankenError>>
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
    ) -> impl Future<Output = Result<Vec<T>, FrankenError>>
    where
        F: FnMut(&Row) -> Result<T, FrankenError>;

    /// Execute a SQL statement with `ParamValue` parameters, returning affected row count.
    fn execute_compat(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> impl Future<Output = Result<usize, FrankenError>>;
}

impl ConnectionExt for Connection {
    async fn query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        f: F,
    ) -> Result<T, FrankenError>
    where
        F: FnOnce(&Row) -> Result<T, FrankenError>,
    {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        let row = self.query_row_with_params(sql, &values).await?;
        f(&row)
    }

    async fn query_map_collect<T, F>(
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
        self.query_with_params_for_each(sql, &values, |row| {
            mapped.push(f(row)?);
            Ok(())
        })
        .await?;
        Ok(mapped)
    }

    async fn execute_compat(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<usize, FrankenError> {
        let values: Vec<SqliteValue> = params.iter().map(|p| p.0.clone()).collect();
        self.execute_with_params(sql, &values).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::RowExt;
    use crate::compat::{OpenFlags, open_with_flags};
    use rusqlite::params;

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
            .query_map_collect("SELECT val FROM t ORDER BY id", &[], |row| row.get_typed(0))
            .unwrap();
        assert_eq!(results, vec!["a", "b", "c"]);
    }

    #[test]
    fn query_map_collect_supports_side_effect_only_row_processing() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('a')").unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('b')").unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('c')").unwrap();

        let mut seen = Vec::new();
        let results: Vec<()> = conn
            .query_map_collect("SELECT val FROM t ORDER BY id", &[], |row| {
                seen.push(row.get_typed::<String>(0)?);
                Ok(())
            })
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(seen, vec!["a", "b", "c"]);
    }

    #[test]
    fn query_map_collect_supports_explain_statements() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('a')").unwrap();
        conn.execute("INSERT INTO t (val) VALUES ('b')").unwrap();

        let opcodes: Vec<String> = conn
            .query_map_collect(
                "EXPLAIN SELECT val FROM t WHERE id = ?1",
                &[ParamValue::from(1_i64)],
                |row| row.get_typed(1),
            )
            .unwrap();

        assert!(!opcodes.is_empty());
        assert!(opcodes.iter().any(|opcode| opcode == "OpenRead"));
    }

    #[test]
    fn execute_params_with_values() {
        let conn = Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        let p = [ParamValue::from(1_i64), ParamValue::from("alice")];
        let affected = conn
            .execute_compat("INSERT INTO t VALUES (?1, ?2)", &p)
            .unwrap();
        assert_eq!(affected, 1);
    }

    #[test]
    fn query_map_collect_composite_unique_index_returns_only_matching_duplicate_run() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let db_path = dir.path().join("messages.db");

        {
            let mut conn = rusqlite::Connection::open(&db_path).expect("open sqlite db");
            conn.execute_batch(
                "CREATE TABLE messages (
                    id INTEGER PRIMARY KEY,
                    conversation_id INTEGER NOT NULL,
                    idx INTEGER NOT NULL,
                    role TEXT,
                    author TEXT,
                    created_at INTEGER,
                    content TEXT,
                    UNIQUE(conversation_id, idx)
                );",
            )
            .expect("create schema");

            let tx = conn.transaction().expect("begin tx");
            tx.execute(
                "INSERT INTO messages (id, conversation_id, idx, role, author, created_at, content)
                 VALUES (1, 1, 0, 'user', 'u', 1000, 'first')",
                [],
            )
            .expect("insert first");
            tx.execute(
                "INSERT INTO messages (id, conversation_id, idx, role, author, created_at, content)
                 VALUES (2, 1, 1, 'assistant', 'a', 1001, 'second')",
                [],
            )
            .expect("insert second");

            for (next_id, conversation_id) in (3_i64..).zip(2_i64..=25_000_i64) {
                tx.execute(
                    "INSERT INTO messages (id, conversation_id, idx, role, author, created_at, content)
                     VALUES (?1, ?2, 0, 'assistant', 'bulk', ?3, ?4)",
                    params![
                        next_id,
                        conversation_id,
                        1_700_000_000_i64 + conversation_id,
                        format!("bulk-{conversation_id}")
                    ],
                )
                .expect("insert bulk row");
            }

            tx.commit().expect("commit fixture");
        }

        let conn = Connection::open(db_path.to_str().expect("utf8 path")).expect("open fsqlite db");
        let rows: Vec<(i64, i64, String)> = conn
            .query_map_collect(
                "SELECT id, idx, content
                 FROM messages INDEXED BY sqlite_autoindex_messages_1
                 WHERE conversation_id = ?1
                 ORDER BY idx",
                &[ParamValue::from(1_i64)],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
            )
            .expect("query composite unique index");

        assert_eq!(
            rows,
            vec![(1, 0, "first".to_owned()), (2, 1, "second".to_owned()),],
            "indexed equality scan should stay within the conversation_id=1 duplicate run",
        );

        let readonly = open_with_flags(
            db_path.to_str().expect("utf8 path"),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("open readonly fsqlite db");
        let readonly_rows: Vec<(i64, i64, String)> = readonly
            .query_map_collect(
                "SELECT id, idx, content
                 FROM messages INDEXED BY sqlite_autoindex_messages_1
                 WHERE conversation_id = ?1
                 ORDER BY idx",
                &[ParamValue::from(1_i64)],
                |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
            )
            .expect("query composite unique index via readonly path");

        assert_eq!(
            readonly_rows,
            vec![(1, 0, "first".to_owned()), (2, 1, "second".to_owned()),],
            "readonly indexed equality scan should stay within the conversation_id=1 duplicate run",
        );
    }

    #[test]
    #[ignore = "machine-local cass repro; run with FSQLITE_REAL_DB=/path/to/agent_search.db"]
    fn query_map_collect_real_cass_db_repro() {
        let db_path = std::env::var("FSQLITE_REAL_DB").expect("FSQLITE_REAL_DB must be set");
        let conn = open_with_flags(&db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open readonly real cass db");
        let query = "SELECT id, idx, content
                 FROM messages INDEXED BY sqlite_autoindex_messages_1
                 WHERE conversation_id = ?1
                 ORDER BY idx
                 LIMIT 20";
        let stmt = conn.prepare(query).expect("prepare real cass query");
        eprintln!("real_cass_query_explain:\n{}", stmt.explain());
        let rows: Vec<(i64, i64, String)> = conn
            .query_map_collect(query, &[ParamValue::from(1_i64)], |row| {
                Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?))
            })
            .expect("query real cass db");

        assert_eq!(
            rows.len(),
            2,
            "conversation_id=1 should only have two rows in the canonical cass db"
        );
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[1].0, 2);
    }

    #[test]
    #[ignore = "machine-local cass repro; run with FSQLITE_REAL_DB=/path/to/agent_search.db"]
    fn query_rowid_lookup_real_cass_db_repro() {
        let db_path = std::env::var("FSQLITE_REAL_DB").expect("FSQLITE_REAL_DB must be set");
        let conn = open_with_flags(&db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open readonly real cass db");
        let query = "SELECT id, conversation_id, idx, content
                 FROM messages
                 WHERE id = ?1";
        let stmt = conn.prepare(query).expect("prepare real cass rowid query");
        eprintln!("real_cass_rowid_query_explain:\n{}", stmt.explain());
        let rows: Vec<(i64, i64, i64, String)> = conn
            .query_map_collect(query, &[ParamValue::from(1_i64)], |row| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed(1)?,
                    row.get_typed(2)?,
                    row.get_typed(3)?,
                ))
            })
            .expect("query real cass db by rowid");

        assert_eq!(rows, vec![(1, 1, 0, "hello".to_owned())]);
    }
}
