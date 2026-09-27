//! Regression for cass GH #503: an archive written before the cass #434 writer
//! fix stores an FTS5 shadow table (`fts_messages_config`) as a rowid table that
//! declares `k PRIMARY KEY` but has no `sqlite_autoindex_fts_messages_config_1`
//! row in `sqlite_master`. Catalog binding refused every open of such a file
//! ("sqlite_master is missing implicit autoindex slot 1 for table
//! `fts_messages_config`"), including the deferred-FTS5 open that exists to
//! repair derived FTS5 shadows, so cass could not repair it in place.
//!
//! The deferred-FTS5 open now binds a missing implicit autoindex of an FTS5
//! shadow table as absent; the repair drops and recreates the shadow. Ordinary
//! opens stay strict, and so does every non-shadow table.
//!
//! Fixtures (tests/fixtures/gh503/, 32 KiB each) were written by stock SQLite
//! 3.x through Python's sqlite3 module, as the legacy catalog cannot be produced
//! through FrankenSQLite (writable_schema writes are unsupported):
//!
//! ```python
//! c.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY, title TEXT)")
//! # 3 rows: alpha, bravo, charlie
//! c.execute("CREATE VIRTUAL TABLE fts_messages USING fts5(content)")  # 2 rows
//! # legacy_fts_shadow_autoindex.db:
//! c.execute("ALTER TABLE fts_messages_config RENAME TO zz_legacy_config_src")
//! c.execute("CREATE TABLE fts_messages_config (k, v)")
//! c.execute("INSERT INTO fts_messages_config SELECT k, v FROM zz_legacy_config_src")
//! # legacy_plain_table_autoindex.db instead:
//! c.execute("CREATE TABLE legacy_meta (k, v)"); c.execute("INSERT INTO legacy_meta VALUES ('schema', 1)")
//! c.execute("PRAGMA writable_schema=ON")
//! c.execute("UPDATE sqlite_master SET sql='CREATE TABLE <target> (k PRIMARY KEY, v)' WHERE name='<target>'")
//! ```
//!
//! Run: `cargo test -p fsqlite --features fts5 --test cass503_legacy_fts_shadow_autoindex`

#![cfg(feature = "fts5")]

use std::path::{Path, PathBuf};

use fsqlite::{Connection, SqliteValue};

fn fixture_copy(name: &str) -> (tempfile::TempDir, String) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/gh503")
        .join(name);
    let dir = tempfile::TempDir::new().unwrap();
    let target: PathBuf = dir.path().join(name);
    std::fs::copy(&source, &target)
        .unwrap_or_else(|error| panic!("copy fixture {}: {error}", source.display()));
    let path = target.to_str().unwrap().to_owned();
    (dir, path)
}

async fn single_integer(conn: &Connection, sql: &str) -> i64 {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    match rows.first().and_then(|row| row.values().first().cloned()) {
        Some(SqliteValue::Integer(value)) => value,
        other => panic!("{sql}: expected one integer, got {other:?}"),
    }
}

async fn text_rows(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| match row.values().first() {
            Some(SqliteValue::Text(text)) => text.to_string(),
            other => panic!("{sql}: expected text, got {other:?}"),
        })
        .collect()
}

const MISSING_SLOT: &str = "sqlite_master is missing implicit autoindex slot 1 for table";

/// The rendered error of an open that must fail.
fn refusal<T, E: std::fmt::Display>(result: Result<T, E>, label: &str) -> String {
    match result {
        Ok(_) => panic!("{label} open must refuse this catalog"),
        Err(error) => format!("{error:#}"),
    }
}

#[test]
fn deferred_fts5_open_repairs_a_legacy_shadow_without_its_autoindex_row() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, path) = fixture_copy("legacy_fts_shadow_autoindex.db");

        // Ordinary opens stay strict: the missing autoindex row is refused.
        let message = refusal(Connection::open(&path).await, "ordinary");
        assert!(
            message.contains(MISSING_SLOT) && message.contains("fts_messages_config"),
            "unexpected ordinary-open error: {message}"
        );

        // The deferred-FTS5 repair open binds the missing shadow autoindex as
        // absent and serves the canonical rows.
        let repair = Connection::open_existing_schema_only_deferred_fts5(&path)
            .await
            .unwrap_or_else(|error| panic!("deferred-FTS5 repair open must succeed: {error:#}"));
        assert_eq!(
            single_integer(&repair, "SELECT COUNT(*) FROM conversations").await,
            3
        );

        // The cass repair sequence: drop the FTS5 table, recreate it, refill it.
        repair
            .execute("DROP TABLE IF EXISTS fts_messages")
            .await
            .expect("dropping the legacy FTS5 table");
        repair
            .execute("CREATE VIRTUAL TABLE fts_messages USING fts5(content)")
            .await
            .expect("recreating the FTS5 table");
        repair
            .execute_with_params(
                "INSERT INTO fts_messages(content) VALUES (?1)",
                &[SqliteValue::Text("rebuilt shadow row".into())],
            )
            .await
            .expect("refilling the FTS5 table");
        drop(repair);

        // After the repair an ordinary open succeeds, the catalog is canonical,
        // the file is consistent, and the canonical rows are untouched.
        let conn = Connection::open(&path)
            .await
            .unwrap_or_else(|error| panic!("ordinary reopen after repair: {error:#}"));
        let config_sql = text_rows(
            &conn,
            "SELECT sql FROM sqlite_master WHERE name = 'fts_messages_config'",
        )
        .await;
        assert_eq!(config_sql.len(), 1, "one config shadow: {config_sql:?}");
        assert!(
            config_sql[0].to_ascii_uppercase().contains("WITHOUT ROWID"),
            "the recreated config shadow must be canonical: {}",
            config_sql[0]
        );
        assert_eq!(
            text_rows(&conn, "PRAGMA integrity_check").await,
            vec!["ok".to_owned()]
        );
        assert_eq!(
            text_rows(&conn, "SELECT title FROM conversations ORDER BY id").await,
            vec!["alpha".to_owned(), "bravo".to_owned(), "charlie".to_owned()]
        );
        assert_eq!(
            single_integer(
                &conn,
                "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH 'rebuilt'"
            )
            .await,
            1
        );
    });
}

#[test]
fn deferred_fts5_open_still_refuses_a_missing_autoindex_on_an_ordinary_table() {
    asupersync::test_utils::run_test(|| async {
        let (_dir, path) = fixture_copy("legacy_plain_table_autoindex.db");
        let refusals = [
            (
                "ordinary",
                refusal(Connection::open(&path).await, "ordinary"),
            ),
            (
                "deferred-FTS5 repair",
                refusal(
                    Connection::open_existing_schema_only_deferred_fts5(&path).await,
                    "deferred-FTS5 repair",
                ),
            ),
        ];
        for (label, message) in refusals {
            assert!(
                message.contains(MISSING_SLOT) && message.contains("legacy_meta"),
                "{label} open: unexpected error: {message}"
            );
        }
    });
}
