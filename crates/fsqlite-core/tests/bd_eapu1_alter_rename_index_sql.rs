//! bd-eapu1: ALTER TABLE RENAME COLUMN/TABLE must store the affected index's
//! `sqlite_master.sql` in SQLite's minimal form (quote only when needed, no
//! implicit ASC), not a canonical always-quoted re-render. Oracle: sqlite3.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn index_sql(conn: &Connection, name: &str) -> String {
    let r = conn
        .query(&format!("SELECT sql FROM sqlite_master WHERE name = '{name}';"))
        .await
        .unwrap();
    match &r[0].values()[0] {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected text sql, got {other:?}"),
    }
}

/// RENAME COLUMN re-renders the affected index minimally: no quotes on simple
/// identifiers, no implicit ASC. Stock: `CREATE INDEX ix ON t(c)`.
#[test]
fn rename_column_keeps_index_sql_minimal_bd_eapu1() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a, b);").await.unwrap();
        conn.execute("CREATE INDEX ix ON t(b);").await.unwrap();
        conn.execute("ALTER TABLE t RENAME COLUMN b TO c;").await.unwrap();
        assert_eq!(index_sql(&conn, "ix").await, "CREATE INDEX ix ON t(c)");
    });
}

/// RENAME TABLE likewise re-renders the index with the new table name, minimal.
#[test]
fn rename_table_keeps_index_sql_minimal_bd_eapu1() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a, b);").await.unwrap();
        conn.execute("CREATE INDEX ix ON t(b);").await.unwrap();
        conn.execute("ALTER TABLE t RENAME TO t2;").await.unwrap();
        assert_eq!(index_sql(&conn, "ix").await, "CREATE INDEX ix ON t2(b)");
    });
}

/// DESC is preserved (only implicit ASC is dropped) and a keyword column name
/// is still quoted where SQLite requires it.
#[test]
fn rename_preserves_desc_and_quotes_keyword_bd_eapu1() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a, b);").await.unwrap();
        conn.execute("CREATE INDEX ix ON t(b DESC);").await.unwrap();
        conn.execute("ALTER TABLE t RENAME COLUMN b TO \"order\";")
            .await
            .unwrap();
        assert_eq!(
            index_sql(&conn, "ix").await,
            "CREATE INDEX ix ON t(\"order\" DESC)"
        );
    });
}
