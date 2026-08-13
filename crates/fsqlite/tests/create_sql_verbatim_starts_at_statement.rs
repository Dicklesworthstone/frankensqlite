//! bd-lgolw: the verbatim-stored CREATE text must begin at the statement's
//! first token. A leading `-- comment` submitted with the CREATE (a single
//! parsed statement) was persisted into sqlite_master, and canonical SQLite
//! then failed the WHOLE schema load with "malformed database schema
//! (products)" — observed downstream as mcp_agent_mail_rust's schema init
//! producing databases unreadable by stock sqlite3.

use fsqlite::{Connection, SqliteValue};

fn assert_stored_products_sql_starts_at_create(batch: &'static str) {
    asupersync::test_utils::run_test(move || async move {
        let path = std::env::temp_dir().join(format!(
            "lgolw-verbatim-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let conn = Connection::open(path.to_str().unwrap()).await.expect("open");
        conn.execute(batch).await.expect("execute");
        let rows = conn
            .query("SELECT sql FROM sqlite_master WHERE name = 'products';")
            .await
            .expect("read sqlite_master");
        assert_eq!(rows.len(), 1, "products row must exist");
        let sql = match &rows[0].values()[0] {
            SqliteValue::Text(s) => s.clone(),
            other => panic!("unexpected sql value: {other:?}"),
        };
        assert!(
            sql.starts_with("CREATE TABLE"),
            "stored schema sql must begin at the CREATE token (bd-lgolw): {sql:?}"
        );
        assert!(
            sql.trim_end().ends_with(')'),
            "stored schema sql must stay complete: {sql:?}"
        );
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn leading_line_comment_is_not_persisted_into_schema_sql() {
    assert_stored_products_sql_starts_at_create(
        "-- Products table\n\
         CREATE TABLE IF NOT EXISTS products (\n    \
         id INTEGER PRIMARY KEY AUTOINCREMENT,\n    \
         name TEXT NOT NULL UNIQUE\n);\n",
    );
}

#[test]
fn leading_block_comment_is_not_persisted_into_schema_sql() {
    assert_stored_products_sql_starts_at_create(
        "/* schema preamble */ CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);",
    );
}
