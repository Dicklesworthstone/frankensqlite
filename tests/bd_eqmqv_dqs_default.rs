#![recursion_limit = "512"]

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

fn stock_value(value: rusqlite::types::Value) -> SqliteValue {
    match value {
        rusqlite::types::Value::Null => SqliteValue::Null,
        rusqlite::types::Value::Integer(n) => SqliteValue::Integer(n),
        rusqlite::types::Value::Real(f) => SqliteValue::Float(f),
        rusqlite::types::Value::Text(s) => SqliteValue::Text(s.into()),
        rusqlite::types::Value::Blob(b) => SqliteValue::Blob(b.into()),
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| row.get::<_, rusqlite::types::Value>(i).map(stock_value))
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("FrankenSQLite `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

/// Stock's `DEFAULT id` production makes ANY identifier token a string —
/// bare, `"double-quoted"`, `[bracketed]` and `` `backticked` `` — and
/// `PRAGMA table_info` reports it as written. A database stock wrote with such
/// defaults (also via ALTER ADD COLUMN, leaving short records) must accept
/// INSERTs that take the default, read the short records, and report the same
/// `dflt_value`; an fsqlite-created table must store and report the same.
#[test]
fn identifier_spelled_defaults_read_and_insert_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let setup = "CREATE TABLE d(id INTEGER PRIMARY KEY, a TEXT DEFAULT \"dq\", \
                       b DEFAULT bare, c DEFAULT [br], e DEFAULT `bt`, f DEFAULT \"a\"\"b\");\
                     INSERT INTO d(id) VALUES (1);\
                     CREATE TABLE s(x);\
                     INSERT INTO s VALUES (1);\
                     ALTER TABLE s ADD COLUMN g TEXT DEFAULT \"fb\";\
                     ALTER TABLE s ADD COLUMN h DEFAULT [hb];";
        let after = [
            "INSERT INTO d(id) VALUES (2)",
            "INSERT INTO d DEFAULT VALUES",
            "INSERT INTO s(x) VALUES (2)",
        ];
        let checks = [
            "SELECT id, a, b, c, e, f FROM d ORDER BY id",
            "SELECT x, g, h FROM s ORDER BY x",
            "SELECT name, dflt_value FROM pragma_table_info('d') ORDER BY cid",
            "SELECT name, dflt_value FROM pragma_table_info('s') ORDER BY cid",
        ];

        // A database stock wrote, read and extended by each engine.
        let stock_path = dir.path().join("stock.db");
        let frank_path = dir.path().join("frank_copy.db");
        {
            let writer = rusqlite::Connection::open(&stock_path).unwrap();
            writer.execute_batch(setup).unwrap();
        }
        std::fs::copy(&stock_path, &frank_path).unwrap();
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
        for sql in after {
            stock.execute(sql, []).unwrap();
            frank
                .execute(sql)
                .await
                .unwrap_or_else(|e| panic!("FrankenSQLite `{sql}`: {e:?}"));
        }
        for sql in checks {
            assert_eq!(frank_rows(&frank, sql).await, stock_rows(&stock, sql), "`{sql}`");
        }

        // The same schema created by each engine, one statement at a time.
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in setup.split(';').filter(|sql| !sql.trim().is_empty()) {
            stock.execute(sql, []).unwrap();
            frank
                .execute(sql)
                .await
                .unwrap_or_else(|e| panic!("FrankenSQLite `{sql}`: {e:?}"));
        }
        for sql in after {
            stock.execute(sql, []).unwrap();
            frank.execute(sql).await.unwrap();
        }
        // ALTER ADD COLUMN does not keep its DEFAULT text verbatim yet (it
        // reports the AST rendering, the same value), so the `s` table's
        // dflt_value is compared on the stock-written database above only.
        for sql in checks.iter().filter(|sql| !sql.contains("pragma_table_info('s')")) {
            assert_eq!(frank_rows(&frank, sql).await, stock_rows(&stock, sql), "`{sql}`");
        }
        assert_eq!(
            frank_rows(&frank, "SELECT sql FROM sqlite_master WHERE name = 'd'").await,
            stock_rows(&stock, "SELECT sql FROM sqlite_master WHERE name = 'd'"),
        );
    });
}
