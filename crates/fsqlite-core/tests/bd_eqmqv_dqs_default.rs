//! bd-eqmqv (DQS shape 5): a double-quoted column DEFAULT is a string literal
//! under DQS-ON, not a (non-constant) column reference.
//!
//! `CREATE TABLE t(a TEXT DEFAULT "def")` parses the DEFAULT as Expr::Column and
//! the constant-DEFAULT validator rejected it ("default value ... is not
//! constant"). A proactive splice rewrites the double-quoted DEFAULT-value token
//! to a string literal before validation. Oracle: sqlite3 3.46.1 (DQS-on).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn one_text(conn: &Connection, sql: &str) -> SqliteValue {
    conn.query(sql).await.unwrap()[0].values()[0].clone()
}

#[test]
fn bd_eqmqv_double_quoted_default_is_string_literal() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // CREATE TABLE with a double-quoted DEFAULT → string literal 'def'.
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT DEFAULT \"def\");")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(id) VALUES(1);").await.unwrap();
        assert_eq!(
            one_text(&conn, "SELECT a FROM t WHERE id=1;").await,
            SqliteValue::Text("def".into())
        );

        // Parenthesized double-quoted DEFAULT too.
        conn.execute("CREATE TABLE p(id INTEGER PRIMARY KEY, a TEXT DEFAULT (\"paren\"));")
            .await
            .unwrap();
        conn.execute("INSERT INTO p(id) VALUES(1);").await.unwrap();
        assert_eq!(
            one_text(&conn, "SELECT a FROM p WHERE id=1;").await,
            SqliteValue::Text("paren".into())
        );

        // ALTER TABLE ADD COLUMN with a double-quoted DEFAULT.
        conn.execute("ALTER TABLE t ADD COLUMN b TEXT DEFAULT \"added\";")
            .await
            .unwrap();
        assert_eq!(
            one_text(&conn, "SELECT b FROM t WHERE id=1;").await,
            SqliteValue::Text("added".into())
        );

        // Regression: ordinary constant DEFAULTs are unaffected.
        conn.execute(
            "CREATE TABLE q(id INTEGER PRIMARY KEY, n INT DEFAULT 42, s TEXT DEFAULT 'lit');",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO q(id) VALUES(1);").await.unwrap();
        let row = conn.query("SELECT n, s FROM q;").await.unwrap();
        assert_eq!(row[0].values()[0], SqliteValue::Integer(42));
        assert_eq!(row[0].values()[1], SqliteValue::Text("lit".into()));

        conn.close().await.unwrap();
    });
}
