#![recursion_limit = "512"]
//! bd-7b1wh: in a trigger, a reference to the rowid has INTEGER affinity, as
//! stock gives it: the INTEGER PRIMARY KEY column by its name, or `rowid`,
//! `_rowid_` and `oid`. So `NEW.id = '1'` is 1 where fsqlite gave 0. Any other
//! column's NEW / OLD reference has no affinity (`NEW.k = '5'` stays 0 for an
//! INTEGER column), and neither has a key that is not the rowid (`id INT
//! PRIMARY KEY`, a WITHOUT ROWID table's INTEGER PRIMARY KEY).
//!
//! Every logged row, on `:memory:` and on a file, with the DML run directly and
//! prepared, matches bundled SQLite 3.54.1. A statement that fires a trigger is
//! replayed row by row by the connection, which strict mode refuses whatever
//! the expressions are, so this keeper runs without it.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).expect("stock verify prepares");
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock verify runs")
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite verify `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect()
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, s TEXT)",
    "CREATE TABLE lg(tag TEXT, c1, c2, c3, c4, c5, c6)",
    "CREATE TRIGGER t_bi BEFORE INSERT ON t BEGIN \
       INSERT INTO lg VALUES ('bi', NEW.id = '1', NEW.rowid = '1.0', typeof(NEW.id), \
         NEW.k = '5', NEW.id IN ('1', '2'), NEW.id BETWEEN '0' AND '1'); END",
    "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN \
       INSERT INTO lg VALUES ('ai', NEW.id = '1', NEW.rowid = '1', NEW._rowid_ = '1.0', \
         NEW.oid = ' 1', NEW.id = 1.0, '1' = NEW.id); \
       INSERT INTO lg VALUES ('ai-neg', NEW.k = '5', NEW.s = 5, NEW.k = 5, \
         typeof(NEW.id), typeof(NEW.rowid), NEW.id = NEW.k); \
       UPDATE lg SET c6 = 'body-where' WHERE tag = 'ai' AND NEW.id = '1'; END",
    "CREATE TRIGGER t_aw AFTER INSERT ON t WHEN NEW.id = '1' BEGIN \
       INSERT INTO lg VALUES ('when-id', NEW.id, 0, 0, 0, 0, 0); END",
    "CREATE TRIGGER t_awr AFTER INSERT ON t WHEN NEW.rowid = '2.0' BEGIN \
       INSERT INTO lg VALUES ('when-rowid', NEW.id, 0, 0, 0, 0, 0); END",
    "CREATE TRIGGER t_awk AFTER INSERT ON t WHEN NEW.k = '5' BEGIN \
       INSERT INTO lg VALUES ('neg-when-k', NEW.id, 0, 0, 0, 0, 0); END",
    "CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN \
       INSERT INTO lg VALUES ('au', OLD.id = '1', NEW.id = '3', OLD.rowid = '1', \
         NEW.oid = '3.0', NEW.k = '6', OLD._rowid_ < '2'); END",
    "CREATE TRIGGER t_ad AFTER DELETE ON t WHEN OLD.id = '3' BEGIN \
       INSERT INTO lg VALUES ('ad', OLD.id = '3', OLD.oid = '3.0', OLD.k = '6', \
         OLD.rowid IN ('3'), 0, 0); END",
    // Not the rowid: an INT PRIMARY KEY, and a WITHOUT ROWID table's key.
    "CREATE TABLE n(id INT PRIMARY KEY, k)",
    "CREATE TRIGGER n_ai AFTER INSERT ON n BEGIN \
       INSERT INTO lg VALUES ('int-pk', NEW.id = '1', NEW.rowid = '1', NEW.oid = '1', 0, 0, 0); END",
    "CREATE TABLE w(id INTEGER PRIMARY KEY, k) WITHOUT ROWID",
    "CREATE TRIGGER w_ai AFTER INSERT ON w BEGIN \
       INSERT INTO lg VALUES ('without-rowid', NEW.id = '1', NEW.k = '2', 0, 0, 0, 0); END",
    // No INTEGER PRIMARY KEY: the rowid aliases alone.
    "CREATE TABLE r(a, b)",
    "CREATE TRIGGER r_ai AFTER INSERT ON r BEGIN \
       INSERT INTO lg VALUES ('rowid-only', NEW.rowid = '1', NEW._rowid_ = '1.0', \
         NEW.oid = ' 1', NEW.a = '1', 0, 0); END",
    // A column named rowid is that column, not the rowid.
    "CREATE TABLE s(rowid TEXT, v)",
    "CREATE TRIGGER s_ai AFTER INSERT ON s BEGIN \
       INSERT INTO lg VALUES ('shadow', NEW.rowid = 1, NEW.rowid = '1', NEW.oid = '1', 0, 0, 0); END",
];

const DML: &[&str] = &[
    "INSERT INTO t VALUES (1, 5, 'a')",
    "INSERT INTO t VALUES (2, 7, 'b')",
    "UPDATE t SET id = 3, k = 6 WHERE id = 1",
    "DELETE FROM t WHERE id = 3",
    "INSERT INTO n VALUES (1, 2)",
    "INSERT INTO w VALUES (1, 2)",
    "INSERT INTO r VALUES (1, 2)",
    "INSERT INTO s VALUES ('1', 2)",
];

#[test]
fn trigger_rowid_references_have_integer_affinity() {
    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SETUP.iter().chain(DML) {
            stock.execute_batch(sql).unwrap();
        }
        let verify = "SELECT * FROM lg ORDER BY rowid";
        let expected = stock_rows(&stock, verify);
        // Not vacuous: stock's NEW.id = '1' is 1, its NEW.k = '5' is 0, and
        // its WHEN NEW.id = '1' fires.
        let row = |tag: &str| {
            expected
                .iter()
                .find(|row| row[0] == tag)
                .unwrap_or_else(|| panic!("stock logs no {tag}: {expected:?}"))
        };
        assert_eq!(row("'ai'")[1..3], ["1", "1"], "{expected:?}");
        assert_eq!(row("'ai-neg'")[1], "0", "{expected:?}");
        row("'when-id'");
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("rowid.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for sql in DML {
                    if prepared {
                        conn.prepare(sql).await.unwrap().execute().await.unwrap();
                    } else {
                        conn.execute(sql).await.unwrap();
                    }
                }
                assert_eq!(
                    fsqlite_rows(&conn, verify).await,
                    expected,
                    "file_backed={file_backed} prepared={prepared}"
                );
            }
        }
    });
}
