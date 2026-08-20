//! bd-alter-table-on-view-wrong-message-qnw6g: ALTER TABLE targeting a VIEW must
//! report the stock VIEW-specific SQLITE_ERROR message (the object exists, it just
//! isn't alterable), not "no such table" from the tables-only schema lookup.
//! Messages match stock sqlite3 3.46.1 verbatim.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;

async fn alter_error_message(conn: &Connection, sql: &str) -> String {
    match conn.execute(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`"),
        Err(FrankenError::FunctionError(message)) => message,
        Err(other) => panic!("`{sql}` expected FunctionError (SQLITE_ERROR), got {other:?}"),
    }
}

#[test]
fn bd_qnw6g_alter_table_on_view_reports_view_message() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIEW v AS SELECT 1 AS a;")
            .await
            .unwrap();

        assert_eq!(
            alter_error_message(&conn, "ALTER TABLE v RENAME TO w;").await,
            "view v may not be altered"
        );
        assert_eq!(
            alter_error_message(&conn, "ALTER TABLE v ADD COLUMN x;").await,
            "Cannot add a column to a view"
        );
        assert_eq!(
            alter_error_message(&conn, "ALTER TABLE v RENAME COLUMN a TO b;").await,
            "view v may not be altered"
        );
        assert_eq!(
            alter_error_message(&conn, "ALTER TABLE v DROP COLUMN a;").await,
            "view v may not be altered"
        );

        // A genuinely-missing table still reports "no such table" (regression guard).
        match conn.execute("ALTER TABLE nope RENAME TO other;").await {
            Err(FrankenError::NoSuchTable { name }) => assert_eq!(name, "nope"),
            other => panic!("missing table must still be NoSuchTable, got {other:?}"),
        }
    });
}
