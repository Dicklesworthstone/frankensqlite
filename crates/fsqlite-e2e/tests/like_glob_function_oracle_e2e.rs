//! bd-7hrze — Oracle-parity e2e: like()/glob() function forms vs rusqlite.
//!
//! Besides the `X LIKE Y` / `X GLOB Y` operators (covered by like_glob_oracle),
//! SQLite exposes them as functions with the arguments REVERSED:
//! `X LIKE Y` == `like(Y, X)` and `X GLOB Y` == `glob(Y, X)` — the pattern is the
//! first argument. `like(pattern, str, escape)` is the 3-argument ESCAPE form.
//! These verify the function forms (incl. case rules, char classes, ESCAPE, NULL
//! arguments, and use in a WHERE filter) against rusqlite.

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

fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q), sqlite_rows(r, q)) {
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

fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&f, &r, queries, label);
}

#[test]
fn like_function_two_arg() {
    assert_scalar(
        &[
            "SELECT like('a%', 'abc')",  // pattern first -> 1
            "SELECT like('A%', 'abc')",  // LIKE is ASCII case-insensitive -> 1
            "SELECT like('z%', 'abc')",  // 0
            "SELECT like('%c', 'abc')",  // 1
            "SELECT like('a_c', 'abc')", // _ matches one -> 1
            "SELECT like('a%', NULL)",   // NULL arg -> NULL
            "SELECT like(NULL, 'abc')",  // NULL pattern -> NULL
        ],
        "like_function_two_arg",
    );
}

#[test]
fn like_function_three_arg_escape() {
    assert_scalar(
        &[
            // Escape '\' makes '\%' match a literal percent.
            "SELECT like('a\\%c', 'a%c', '\\')",   // 1
            "SELECT like('a\\%c', 'abc', '\\')",   // 0 (literal % doesn't match b)
            "SELECT like('100\\%', '100%', '\\')", // 1
        ],
        "like_function_three_arg_escape",
    );
}

#[test]
fn glob_function_two_arg() {
    assert_scalar(
        &[
            "SELECT glob('a*', 'abc')",     // 1
            "SELECT glob('A*', 'abc')",     // GLOB case-sensitive -> 0
            "SELECT glob('a?c', 'abc')",    // ? matches one -> 1
            "SELECT glob('[a-c]*', 'bcd')", // char class -> 1
            "SELECT glob('[^a]*', 'bcd')",  // negated class -> 1
            "SELECT glob('a*', NULL)",      // NULL -> NULL
        ],
        "glob_function_two_arg",
    );
}

#[test]
fn like_glob_functions_in_where() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO t VALUES (1,'apple'),(2,'Apricot'),(3,'banana'),(4,'avocado')",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    check(
        &f,
        &r,
        &[
            // like() is case-insensitive -> apple, Apricot, avocado.
            "SELECT id FROM t WHERE like('a%', name) ORDER BY id", // 1,2,4
            // glob() is case-sensitive -> apple, avocado (not Apricot).
            "SELECT id FROM t WHERE glob('a*', name) ORDER BY id", // 1,4
        ],
        "like_glob_functions_in_where",
    );
}
