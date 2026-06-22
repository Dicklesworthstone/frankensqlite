//! bd-140mh — Oracle-parity e2e: JSON1 scalar functions vs rusqlite.
//!
//! SQLite's JSON1 functions have precise, easy-to-miss semantics: `json()`
//! minifies and validates, `json_valid`/`json_type` classify, `json_extract`
//! returns the extracted value with its SQL storage class (and NULL for missing
//! paths), the `->` operator returns a JSON representation while `->>` returns
//! the SQL text/value, and the array/object builders quote per the input type.
//! Each scenario compares results against rusqlite's bundled JSON1; operators
//! and mutators (set/insert/replace/remove) live in their own functions so any
//! divergence isolates cleanly.

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

fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).map_err(|e| e.to_string())?;
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

fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s);
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q), sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"));
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"));
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
fn json_minify_and_validate() {
    scenario(
        &[],
        &[
            // json() validates and minifies (strips insignificant whitespace).
            "SELECT json(' { \"a\" : 1 , \"b\" : [ 2 , 3 ] } ')",
            "SELECT json('[1,2,3]')",
            "SELECT json('\"hello\"')",
        ],
        "json_minify_and_validate",
    );
}

#[test]
fn json_valid_classification() {
    scenario(
        &[],
        &[
            "SELECT json_valid('{\"a\":1}')", // 1
            "SELECT json_valid('[1,2,3]')",   // 1
            "SELECT json_valid('not json')",  // 0
            "SELECT json_valid('{\"a\":}')",  // 0
            "SELECT json_valid('123')",       // 1 (a bare number is valid JSON)
            "SELECT json_valid('null')",      // 1
        ],
        "json_valid_classification",
    );
}

#[test]
fn json_type_classification() {
    scenario(
        &[],
        &[
            "SELECT json_type('{\"a\":1}')",            // object
            "SELECT json_type('[1,2]')",                // array
            "SELECT json_type('123')",                  // integer
            "SELECT json_type('1.5')",                  // real
            "SELECT json_type('\"hi\"')",               // text
            "SELECT json_type('true')",                 // true
            "SELECT json_type('false')",                // false
            "SELECT json_type('null')",                 // null
            "SELECT json_type('{\"a\":[1,2]}', '$.a')", // array (path form)
        ],
        "json_type_classification",
    );
}

#[test]
fn json_extract_paths_and_storage_class() {
    scenario(
        &[],
        &[
            "SELECT json_extract('{\"a\":1,\"b\":\"x\"}', '$.a')", // 1 (integer)
            "SELECT json_extract('{\"a\":1,\"b\":\"x\"}', '$.b')", // x (text)
            "SELECT json_extract('[10,20,30]', '$[1]')",           // 20
            "SELECT json_extract('{\"a\":{\"b\":2}}', '$.a.b')",   // 2
            "SELECT json_extract('{\"a\":1}', '$.missing')",       // NULL
            "SELECT typeof(json_extract('{\"a\":1}', '$.a'))",     // integer
            "SELECT typeof(json_extract('{\"a\":\"x\"}', '$.a'))", // text
            "SELECT typeof(json_extract('{\"a\":1.5}', '$.a'))",   // real
        ],
        "json_extract_paths_and_storage_class",
    );
}

#[test]
fn json_array_and_object_builders() {
    scenario(
        &[],
        &[
            "SELECT json_array(1, 2, 'x', NULL)",    // [1,2,"x",null]
            "SELECT json_array(1.5, -3, 'a b')",     // [1.5,-3,"a b"]
            "SELECT json_object('a', 1, 'b', 'x')",  // {"a":1,"b":"x"}
            "SELECT json_object('n', NULL, 'k', 2)", // {"n":null,"k":2}
            "SELECT json_array_length('[1,2,3,4]')", // 4
            "SELECT json_array_length('[]')",        // 0
            "SELECT json_array_length('{\"a\":[1,2,3]}', '$.a')", // 3
        ],
        "json_array_and_object_builders",
    );
}

#[test]
fn json_quote_function() {
    scenario(
        &[],
        &[
            "SELECT json_quote('hello')", // "hello"
            "SELECT json_quote(3.14)",    // 3.14
            "SELECT json_quote(42)",      // 42
        ],
        "json_quote_function",
    );
}

/// `->` yields a JSON value (quoted text stays quoted); `->>` yields the SQL
/// text/value (quotes stripped). SQLite 3.38+. These operators parse into a
/// distinct `Expr::JsonAccess` node which the full VDBE codegen path lowers
/// (see the in-table-context test below), but the connection.rs `emit_expr`
/// path used for table-less constant SELECTs has no `JsonAccess` arm and errors.
/// Tracked in bd-m87j8.
#[test]
fn json_arrow_operators() {
    scenario(
        &[],
        &[
            "SELECT '{\"a\":1}' -> '$.a'",      // JSON 1
            "SELECT '{\"a\":1}' ->> '$.a'",     // SQL 1
            "SELECT '{\"a\":\"x\"}' -> '$.a'",  // JSON "x" (quoted)
            "SELECT '{\"a\":\"x\"}' ->> '$.a'", // SQL x (unquoted)
            "SELECT '[10,20]' -> 1",            // JSON 20 (bare int = array index)
            "SELECT '[10,20]' ->> 1",           // SQL 20
        ],
        "json_arrow_operators",
    );
}

#[test]
fn json_mutators_set_insert_replace_remove() {
    // Mutators differ in whether they create vs. only-overwrite vs. only-update.
    // Isolated so a divergence here doesn't taint the readers above.
    scenario(
        &[],
        &[
            // set: create or overwrite.
            "SELECT json_set('{\"a\":1}', '$.a', 99)", // {"a":99}
            "SELECT json_set('{\"a\":1}', '$.b', 2)",  // {"a":1,"b":2}
            // insert: only create (no overwrite of existing).
            "SELECT json_insert('{\"a\":1}', '$.a', 99)", // unchanged {"a":1}
            "SELECT json_insert('{\"a\":1}', '$.b', 2)",  // {"a":1,"b":2}
            // replace: only overwrite (no create).
            "SELECT json_replace('{\"a\":1}', '$.a', 99)", // {"a":99}
            "SELECT json_replace('{\"a\":1}', '$.b', 2)",  // unchanged {"a":1}
            // remove: delete a path.
            "SELECT json_remove('{\"a\":1,\"b\":2}', '$.a')", // {"b":2}
            "SELECT json_remove('[1,2,3]', '$[0]')",          // [2,3]
        ],
        "json_mutators_set_insert_replace_remove",
    );
}

#[test]
fn json_in_table_context() {
    scenario(
        &[
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)",
            "INSERT INTO docs VALUES (1,'{\"name\":\"ann\",\"age\":30}'), \
             (2,'{\"name\":\"bob\",\"age\":25}'), (3,'{\"name\":\"cy\",\"age\":40}')",
        ],
        &[
            "SELECT id, json_extract(body, '$.name') FROM docs ORDER BY id",
            "SELECT json_extract(body, '$.name') FROM docs \
             WHERE json_extract(body, '$.age') > 28 ORDER BY id",
            "SELECT id FROM docs ORDER BY json_extract(body, '$.age')",
        ],
        "json_in_table_context",
    );
}

/// Companion to `json_arrow_operators`: the `->` / `->>` operators DO work when
/// the expression flows through the full VDBE codegen path (a query with a FROM
/// clause), confirming the gap is specific to connection.rs `emit_expr`
/// (bd-m87j8), not the operators themselves.
#[test]
fn json_arrow_operators_in_table_context() {
    scenario(
        &[
            "CREATE TABLE d (id INTEGER PRIMARY KEY, body TEXT)",
            "INSERT INTO d VALUES (1,'{\"a\":1,\"s\":\"x\"}'),(2,'[10,20]')",
        ],
        &[
            "SELECT body -> '$.a' FROM d WHERE id = 1",  // JSON 1
            "SELECT body ->> '$.a' FROM d WHERE id = 1", // SQL 1
            "SELECT body -> '$.s' FROM d WHERE id = 1",  // JSON "x"
            "SELECT body ->> '$.s' FROM d WHERE id = 1", // SQL x
            "SELECT body -> 1 FROM d WHERE id = 2",      // JSON 20
            "SELECT body ->> 1 FROM d WHERE id = 2",     // SQL 20
        ],
        "json_arrow_operators_in_table_context",
    );
}
