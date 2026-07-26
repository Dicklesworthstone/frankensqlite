//! bd-4ceg2 — Oracle-parity e2e: JSON path syntax vs rusqlite.
//!
//! json_function_oracle_e2e covers simple `$.key` extraction; this exercises the
//! richer JSONPath dialect SQLite accepts: nested `$.a.b.c`, array indexing
//! `$[n]`, the last-element extension `$[#-1]`, dotted/mixed `$.items[1].name`,
//! quoted keys `$."weird key"`, the whole-document `$` path, multi-path
//! `json_extract(x, '$.a', '$.b')` returning a JSON array, `json_array_length`
//! with a path, `json_set`/`json_remove` with array-index paths, and
//! `json_patch` (RFC 7386 merge incl. null-removal). Uses the json_extract
//! function form (works in table-less SELECT). Each scenario compares against
//! rusqlite; the more exotic path features are isolated per test.
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
fn jsonpath_nested_and_array_index() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT json_extract('{\"a\":{\"b\":{\"c\":42}}}', '$.a.b.c')", // 42
                "SELECT json_extract('[10,20,30,40]', '$[0]')",                 // 10
                "SELECT json_extract('[10,20,30,40]', '$[3]')",                 // 40
                "SELECT json_extract('{\"items\":[{\"name\":\"x\"},{\"name\":\"y\"}]}', '$.items[1].name')", // 'y'
            ],
            "jsonpath_nested_and_array_index",
        )
        .await;
    });
}

#[test]
fn jsonpath_last_element_hash() {
    // SQLite's `#` = array length; `$[#-1]` is the last element.
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT json_extract('[10,20,30]', '$[#-1]')", // 30
                "SELECT json_extract('[10,20,30]', '$[#-2]')", // 20
            ],
            "jsonpath_last_element_hash",
        )
        .await;
    });
}

#[test]
fn jsonpath_quoted_key() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT json_extract('{\"weird key\":5}', '$.\"weird key\"')", // 5
                "SELECT json_extract('{\"a.b\":7}', '$.\"a.b\"')",             // 7 (dotted key)
            ],
            "jsonpath_quoted_key",
        )
        .await;
    });
}

#[test]
fn jsonpath_whole_document() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT json_extract('{\"a\":1}', '$')",         // '{"a":1}'
                "SELECT json_extract('[1,2,3]', '$')",           // '[1,2,3]'
                "SELECT typeof(json_extract('{\"a\":1}', '$'))", // text
            ],
            "jsonpath_whole_document",
        )
        .await;
    });
}

#[test]
fn jsonpath_multiple_paths_returns_array() {
    // Two-or-more paths -> json_extract returns a JSON array of the results.
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT json_extract('{\"a\":1,\"b\":2,\"c\":3}', '$.a', '$.c')", // '[1,3]'
                "SELECT json_extract('{\"a\":\"x\",\"b\":\"y\"}', '$.a', '$.b')", // '["x","y"]'
            ],
            "jsonpath_multiple_paths_returns_array",
        )
        .await;
    });
}

#[test]
fn jsonpath_array_length_with_path() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT json_array_length('{\"a\":[1,2,3,4,5]}', '$.a')", // 5
                "SELECT json_array_length('{\"a\":{\"b\":[1,2]}}', '$.a.b')", // 2
                "SELECT json_array_length('{\"a\":5}', '$.a')",           // 0 (not an array)
            ],
            "jsonpath_array_length_with_path",
        )
        .await;
    });
}

#[test]
fn jsonpath_set_remove_array_index() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT json_set('[1,2,3]', '$[1]', 99)", // '[1,99,3]'
                "SELECT json_remove('[1,2,3]', '$[0]')",  // '[2,3]'
                "SELECT json_remove('{\"a\":1,\"b\":2}', '$.a')", // '{"b":2}'
            ],
            "jsonpath_set_remove_array_index",
        )
        .await;
    });
}

#[test]
fn jsonpath_patch_merge() {
    // RFC 7386 merge patch: overwrite/add keys, and a null value deletes a key.
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT json_patch('{\"a\":1,\"b\":2}', '{\"b\":20,\"c\":30}')", // {"a":1,"b":20,"c":30}
                "SELECT json_patch('{\"a\":1,\"b\":2}', '{\"b\":null}')",        // {"a":1}
            ],
            "jsonpath_patch_merge",
        )
        .await;
    });
}
