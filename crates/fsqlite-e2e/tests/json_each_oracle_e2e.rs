//! bd-4gv8m — Oracle-parity e2e: json_each / json_tree TVFs vs rusqlite.
//!
//! `json_each(json[, path])` is a table-valued function that yields one row per
//! immediate child of a JSON array/object, exposing `key`, `value`, `type`,
//! etc.; `json_tree` walks the whole document recursively. These verify the
//! row shapes (array index keys, object field keys, value storage class, the
//! `type` label), the path argument, aggregation over the produced rows, and a
//! json_tree recursive walk. Compared against rusqlite (bundled SQLite ~3.46).
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn json_each_over_array() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT key, value, type FROM json_each('[10,20,30]') ORDER BY key",
                "SELECT count(*) FROM json_each('[1,2,3,4,5]')", // 5
                "SELECT sum(value) FROM json_each('[1,2,3,4]')", // 10
            ],
            "json_each_over_array",
        )
        .await;
    });
}

#[test]
fn json_each_over_object() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT key, value, type FROM json_each('{\"a\":1,\"b\":\"x\",\"c\":2.5}') ORDER BY key",
                "SELECT key FROM json_each('{\"z\":1,\"a\":2,\"m\":3}') ORDER BY key", // a,m,z
            ],
            "json_each_over_object",
        )
        .await;
    });
}

#[test]
fn json_each_with_path() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT value FROM json_each('{\"items\":[5,6,7]}', '$.items') ORDER BY value", // 5,6,7
                "SELECT sum(value) FROM json_each('{\"nums\":[10,20,30]}', '$.nums')", // 60
            ],
            "json_each_with_path",
        )
        .await;
    });
}

#[test]
fn json_each_value_types() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // Mixed element types -> the `type` label per element.
                "SELECT type FROM json_each('[1, 2.5, \"s\", true, false, null]') ORDER BY key",
                "SELECT value FROM json_each('[1, 2.5, \"s\", null]') ORDER BY key",
            ],
            "json_each_value_types",
        )
        .await;
    });
}

#[test]
fn json_tree_recursive_walk() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // json_tree walks nested structure; count all nodes.
                "SELECT count(*) FROM json_tree('{\"a\":1,\"b\":[2,3]}')",
                // Sum of the integer leaves via json_tree.
                "SELECT sum(value) FROM json_tree('{\"a\":1,\"b\":[2,3]}') WHERE type = 'integer'", // 6
            ],
            "json_tree_recursive_walk",
        )
        .await;
    });
}
