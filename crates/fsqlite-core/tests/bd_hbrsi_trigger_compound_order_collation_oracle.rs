//! bd-hbrsi: inside a trigger body, a compound SELECT sorts and compares by
//! the leftmost arm whose column has a collation (stock's
//! multiSelectCollSeq), and a NEW reference has its column's. The binder
//! turned NEW.a into a value with no collation, so the compound fell back to
//! BINARY. bd-90jrv's residual fix wraps such a result column in its
//! column's collation.
//!
//! The shapes are UNION ALL ordered by position and by name, UNION,
//! INTERSECT, EXCEPT, the NEW arm first, and an RTRIM column. Mixed-case and
//! trailing-space data make the collation decide. Every logged result is
//! compared with bundled SQLite 3.54.1 on `:memory:` and on a file.
//!
//! Negatives: an explicit `ORDER BY 1 COLLATE BINARY` stays BINARY, and a
//! SELECT that is not compound stays as it was.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f:?}"),
        SqliteValue::Text(s) => format!("text:{s:?}"),
        SqliteValue::Blob(b) => format!("blob:{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f:?}"),
        rusqlite::types::Value::Text(s) => format!("text:{s:?}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:02x?}"),
    }
}

const SETUP: &str = "
    CREATE TABLE p(a TEXT COLLATE NOCASE, r TEXT COLLATE RTRIM);
    CREATE TABLE lg(tag, x);
    CREATE TRIGGER pt AFTER INSERT ON p BEGIN
      INSERT INTO lg SELECT 'union-all-by-position', group_concat(x, ',')
        FROM (SELECT 'B' AS x UNION ALL SELECT NEW.a ORDER BY 1);
      INSERT INTO lg SELECT 'union-all-by-name', group_concat(x, ',')
        FROM (SELECT 'B' AS x UNION ALL SELECT NEW.a ORDER BY x);
      INSERT INTO lg SELECT 'union', group_concat(x, ',')
        FROM (SELECT 'ABC' AS x UNION SELECT NEW.a UNION SELECT 'b' ORDER BY 1);
      INSERT INTO lg SELECT 'intersect', group_concat(x, ',')
        FROM (SELECT 'ABC' AS x INTERSECT SELECT NEW.a ORDER BY 1);
      INSERT INTO lg SELECT 'except', group_concat(x, ',')
        FROM (SELECT 'ABC' AS x UNION SELECT 'd' EXCEPT SELECT NEW.a ORDER BY 1);
      INSERT INTO lg SELECT 'new-arm-first', group_concat(x, ',')
        FROM (SELECT NEW.a AS x UNION ALL SELECT 'B' ORDER BY 1);
      INSERT INTO lg SELECT 'rtrim', group_concat(x, '|')
        FROM (SELECT 'q ' AS x UNION SELECT NEW.r ORDER BY 1);
      INSERT INTO lg SELECT 'explicit-binary', group_concat(x, ',')
        FROM (SELECT 'B' AS x UNION ALL SELECT NEW.a ORDER BY 1 COLLATE BINARY);
      INSERT INTO lg SELECT 'not-compound', group_concat(x, ',')
        FROM (SELECT NEW.a AS x ORDER BY 1);
    END;
";

const FIRE: &[&str] = &[
    "INSERT INTO p VALUES ('abc', 'q')",
    "INSERT INTO p VALUES ('ABC', 'q  ')",
];

const CHECK: &str = "SELECT tag, x FROM lg ORDER BY rowid";

#[test]
fn trigger_compound_select_orders_by_the_new_columns_collation() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock.execute_batch(SETUP).unwrap();
    for statement in FIRE {
        stock.execute_batch(statement).unwrap();
    }
    let mut prepared = stock.prepare(CHECK).unwrap();
    let expected: Vec<Vec<String>> = prepared
        .query_map([], |row| {
            Ok((0..2)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .and_then(Iterator::collect)
        .unwrap();
    // Not vacuous: NOCASE sorts 'abc' before 'B', BINARY after it.
    assert_eq!(
        expected[0],
        vec![
            "text:\"union-all-by-position\"".to_owned(),
            "text:\"abc,B\"".to_owned()
        ]
    );
    assert_eq!(
        expected[7],
        vec![
            "text:\"explicit-binary\"".to_owned(),
            "text:\"B,abc\"".to_owned()
        ]
    );
    asupersync::test_utils::run_test(|| async move {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path().join("hbrsi.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let conn = Connection::open(&path).await.unwrap();
            conn.execute_batch(SETUP).await.unwrap();
            for statement in FIRE {
                conn.execute(statement)
                    .await
                    .unwrap_or_else(|e| panic!("file_backed={file_backed}: {statement}: {e}"));
            }
            let got: Vec<Vec<String>> = conn
                .query(CHECK)
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect();
            assert_eq!(got, expected, "file_backed={file_backed}");
            conn.close().await.unwrap();
        }
    });
}
