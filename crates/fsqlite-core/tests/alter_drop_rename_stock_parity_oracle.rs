#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.28: `ALTER TABLE` behaviour against stock.
//!
//! - `DROP COLUMN` of a column a trigger or view uses is refused, as stock
//!   refuses it. The bead's report that stock allows the trigger case came
//!   from a shell with `legacy_alter_table=1`. The trigger message differs in
//!   one documented way: stock repeats the reference as written (`new.c`),
//!   FrankenSQLite names the column (`c`).
//! - `RENAME TO` / `RENAME COLUMN` leave views, triggers, indexes and foreign
//!   keys working as stock does. The rewritten `sqlite_schema` text differs
//!   (README Limitations), so only behaviour is compared.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const DROP_PREFIX_TRIGGER: &str = "error in trigger tr after drop column: no such column: ";

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Blob(b) => format!("{b:?}"),
                })
                .collect()
        })
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock {sql}: {e}"));
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        (0..columns)
            .map(|i| {
                Ok(match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                    rusqlite::types::ValueRef::Integer(n) => n.to_string(),
                    rusqlite::types::ValueRef::Real(f) => format!("{f}"),
                    rusqlite::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    rusqlite::types::ValueRef::Blob(b) => format!("{b:?}"),
                })
            })
            .collect()
    })
    .unwrap_or_else(|e| panic!("stock {sql}: {e}"))
    .map(Result::unwrap)
    .collect()
}

fn stock_error_message(error: rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(msg)) => msg,
        other => other.to_string(),
    }
}

async fn open_pair(schema: &str) -> (Connection, rusqlite::Connection) {
    let frank = Connection::open(":memory:").await.unwrap();
    frank.execute_batch(schema).await.unwrap();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock.execute_batch(schema).unwrap();
    (frank, stock)
}

async fn assert_same_rows(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    assert_eq!(frank_rows(frank, sql).await, stock_rows(stock, sql), "{sql}");
}

#[test]
fn drop_column_used_by_trigger_or_view_is_refused_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let (frank, stock) = open_pair(
            "CREATE TABLE t(a, b, c); CREATE TABLE log(x);\
             CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.c); END;\
             CREATE TABLE u(a, b, c); CREATE VIEW v AS SELECT c FROM u;",
        )
        .await;

        let drop_trigger_column = "ALTER TABLE t DROP COLUMN c";
        let frank_error = frank
            .execute(drop_trigger_column)
            .await
            .expect_err("frank must refuse")
            .to_string();
        let stock_error = stock_error_message(
            stock
                .execute_batch(drop_trigger_column)
                .expect_err("stock refuses"),
        );
        assert_eq!(stock_error, format!("{DROP_PREFIX_TRIGGER}new.c"));
        assert_eq!(frank_error, format!("{DROP_PREFIX_TRIGGER}c"));

        let drop_view_column = "ALTER TABLE u DROP COLUMN c";
        let frank_error = frank.execute(drop_view_column).await.err().map(|e| e.to_string());
        let stock_error = stock.execute_batch(drop_view_column).err().map(stock_error_message);
        assert_eq!(
            frank_error.as_deref(),
            Some("error in view v after drop column: no such column: c")
        );
        assert_eq!(frank_error, stock_error);

        // Both refusals leave the tables, trigger and view intact; a column
        // nothing uses still drops.
        for sql in [
            "ALTER TABLE t DROP COLUMN b",
            "ALTER TABLE u DROP COLUMN a",
            "INSERT INTO t VALUES (1, 3)",
            "INSERT INTO u VALUES (2, 4)",
        ] {
            frank.execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            stock.execute_batch(sql).unwrap();
        }
        for sql in [
            "SELECT * FROM t",
            "SELECT * FROM log",
            "SELECT * FROM u",
            "SELECT * FROM v",
            "SELECT name, type FROM pragma_table_info('t')",
            "SELECT name, type FROM pragma_table_info('u')",
        ] {
            assert_same_rows(&frank, &stock, sql).await;
        }
    });
}

#[test]
fn rename_keeps_dependents_working_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let (frank, stock) = open_pair(
            "PRAGMA foreign_keys = OFF;\
             CREATE TABLE t4 (  x  INT , y TEXT  /* c */ , z REFERENCES t4(x) ) ;\
             CREATE TABLE child(a REFERENCES t4(x));\
             CREATE INDEX i4 ON t4 ( y );\
             CREATE VIEW v4 AS SELECT x, y FROM t4 WHERE t4.y > '';\
             CREATE TRIGGER tr4 AFTER INSERT ON t4 BEGIN INSERT INTO child VALUES (new.x); END;\
             CREATE TABLE \"q q\"(x);",
        )
        .await;
        for sql in [
            "ALTER TABLE t4 RENAME TO t5",
            "ALTER TABLE t5 RENAME COLUMN y TO yy",
            "ALTER TABLE \"q q\" RENAME TO [w w]",
            "INSERT INTO t5 VALUES (1, 'one', NULL)",
            "INSERT INTO t5 VALUES (2, 'two', 1)",
            "INSERT INTO \"w w\" VALUES (9)",
        ] {
            frank.execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            stock.execute_batch(sql).unwrap();
        }
        for sql in [
            "SELECT type, name, tbl_name FROM sqlite_schema ORDER BY name",
            "SELECT * FROM v4 ORDER BY x",
            "SELECT * FROM child ORDER BY a",
            "SELECT * FROM \"w w\"",
            "SELECT \"table\", \"from\", \"to\" FROM pragma_foreign_key_list('t5')",
            "SELECT \"table\", \"from\", \"to\" FROM pragma_foreign_key_list('child')",
            "SELECT name FROM pragma_index_info('i4')",
            "SELECT name FROM pragma_table_info('t5')",
        ] {
            assert_same_rows(&frank, &stock, sql).await;
        }
    });
}
