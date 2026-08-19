//! GH #215: SQLite forbids `ALTER TABLE <fts5> RENAME COLUMN` — stock errors
//! "cannot rename columns of virtual table". FrankenSQLite previously ACCEPTED
//! it, silently rewriting the connection's shadow column list. This asserts the
//! reject, cross-checked against stock SQLite (rusqlite, which ships fts5).
//!
//! bd-bzd19 L13 extends the guard to the other two column-DDL forms stock also
//! forbids on a virtual table: `ADD COLUMN` (stock: "virtual tables may not be
//! altered") and `DROP COLUMN` (stock: "cannot drop column from virtual table").

use fsqlite_core::connection::Connection;

/// The `name` column list stock/`frank` reports for `ft` via `table_info`.
async fn frank_fts5_columns(conn: &Connection) -> Vec<String> {
    conn.query("SELECT name FROM pragma_table_info('ft')")
        .await
        .expect("table_info")
        .iter()
        .map(|row| match &row.values()[0] {
            fsqlite_types::value::SqliteValue::Text(s) => s.as_ref().to_owned(),
            other => panic!("expected text col name, got {other:?}"),
        })
        .collect()
}

#[test]
fn fts5_rename_column_is_rejected_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE ft USING fts5(a, b)")
            .await
            .expect("create fts5");

        let result = conn.execute("ALTER TABLE ft RENAME COLUMN a TO c").await;
        assert!(
            result.is_err(),
            "RENAME COLUMN on an FTS5 vtab must be rejected (GH #215), got Ok"
        );
        let msg = result.unwrap_err().to_string().to_ascii_lowercase();
        assert!(
            msg.contains("virtual table"),
            "error should mention 'virtual table', got: {msg}"
        );

        // The table's columns must be unchanged after the rejected DDL.
        let cols: Vec<String> = conn
            .query("SELECT name FROM pragma_table_info('ft')")
            .await
            .expect("table_info")
            .iter()
            .map(|row| match &row.values()[0] {
                fsqlite_types::value::SqliteValue::Text(s) => s.as_ref().to_owned(),
                other => panic!("expected text col name, got {other:?}"),
            })
            .collect();
        assert!(
            cols.iter().any(|c| c == "a") && !cols.iter().any(|c| c == "c"),
            "columns must be unchanged after a rejected RENAME COLUMN, got {cols:?}"
        );

        // Cross-engine: stock SQLite rejects the identical DDL.
        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch("CREATE VIRTUAL TABLE ft USING fts5(a, b)")
            .unwrap();
        let stock = r.execute_batch("ALTER TABLE ft RENAME COLUMN a TO c");
        assert!(
            stock.is_err(),
            "stock SQLite must also reject RENAME COLUMN on an fts5 vtab"
        );
    });
}

#[test]
fn fts5_add_column_is_rejected_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE ft USING fts5(a, b)")
            .await
            .expect("create fts5");

        let result = conn.execute("ALTER TABLE ft ADD COLUMN c").await;
        assert!(
            result.is_err(),
            "ADD COLUMN on an FTS5 vtab must be rejected (bd-bzd19 L13), got Ok"
        );
        // Stock: "virtual tables may not be altered" — contains "virtual table".
        let msg = result.unwrap_err().to_string().to_ascii_lowercase();
        assert!(
            msg.contains("virtual table"),
            "error should mention 'virtual table', got: {msg}"
        );

        let cols = frank_fts5_columns(&conn).await;
        assert!(
            cols.iter().any(|c| c == "a")
                && cols.iter().any(|c| c == "b")
                && !cols.iter().any(|c| c == "c"),
            "columns must be unchanged after a rejected ADD COLUMN, got {cols:?}"
        );

        // Cross-engine: stock SQLite rejects the identical DDL.
        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch("CREATE VIRTUAL TABLE ft USING fts5(a, b)")
            .unwrap();
        assert!(
            r.execute_batch("ALTER TABLE ft ADD COLUMN c").is_err(),
            "stock SQLite must also reject ADD COLUMN on an fts5 vtab"
        );
    });
}

#[test]
fn fts5_drop_column_is_rejected_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE ft USING fts5(a, b)")
            .await
            .expect("create fts5");

        let result = conn.execute("ALTER TABLE ft DROP COLUMN a").await;
        assert!(
            result.is_err(),
            "DROP COLUMN on an FTS5 vtab must be rejected (bd-bzd19 L13), got Ok"
        );
        // Stock: "cannot drop column from virtual table \"ft\"".
        let msg = result.unwrap_err().to_string().to_ascii_lowercase();
        assert!(
            msg.contains("virtual table"),
            "error should mention 'virtual table', got: {msg}"
        );

        let cols = frank_fts5_columns(&conn).await;
        assert!(
            cols.iter().any(|c| c == "a") && cols.iter().any(|c| c == "b"),
            "columns must be unchanged after a rejected DROP COLUMN, got {cols:?}"
        );

        // Cross-engine: stock SQLite rejects the identical DDL.
        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch("CREATE VIRTUAL TABLE ft USING fts5(a, b)")
            .unwrap();
        assert!(
            r.execute_batch("ALTER TABLE ft DROP COLUMN a").is_err(),
            "stock SQLite must also reject DROP COLUMN on an fts5 vtab"
        );
    });
}
