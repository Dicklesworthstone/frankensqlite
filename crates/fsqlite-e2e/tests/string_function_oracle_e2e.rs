//! bd-bxtse — Oracle-parity e2e: string functions vs rusqlite.
//!
//! Covers string built-ins not exercised by scalar_function_oracle_e2e: the
//! ASCII-ONLY `upper`/`lower` (SQLite leaves non-ASCII bytes untouched — a trap
//! for a full-Unicode reimplementation), `ltrim`/`rtrim`/`trim` with default
//! whitespace and a custom trim set, `replace` edge cases (replace-all, replace
//! with empty, search-not-found, empty-search no-op, NULL passthrough), the
//! 3.44 `concat` / `concat_ws` (NULL-as-empty and NULL-skip-no-double-separator),
//! `unhex` (3.41) + `octet_length` (3.43) vs `length` (bytes vs characters),
//! `format` (the printf alias), and the `string_agg` aggregate. Each scenario
//! compares results against rusqlite (bundled SQLite ~3.46).

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

fn assert_parity(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
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
fn str_upper_lower_ascii_only() {
    assert_parity(
        &[
            "SELECT upper('hello'), lower('HELLO')",
            "SELECT upper('abc123xyz'), lower('ABC123XYZ')",
            // SQLite upper/lower only touch ASCII letters; non-ASCII bytes pass through.
            "SELECT upper('café')", // 'CAFé' (é unchanged)
            "SELECT lower('CAFÉ')", // 'cafÉ' (É unchanged)
            "SELECT upper(NULL), lower(NULL)",
        ],
        "str_upper_lower_ascii_only",
    );
}

#[test]
fn str_trim_variants_and_custom_set() {
    assert_parity(
        &[
            "SELECT '[' || ltrim('  hi  ') || ']'", // '[hi  ]'
            "SELECT '[' || rtrim('  hi  ') || ']'", // '[  hi]'
            "SELECT '[' || trim('  hi  ') || ']'",  // '[hi]'
            "SELECT ltrim('xxhello', 'x')",         // 'hello'
            "SELECT rtrim('helloyy', 'y')",         // 'hello'
            "SELECT trim('xyhelloxy', 'xy')",       // 'hello'
            "SELECT trim('aaa', 'a')",              // '' (empty)
        ],
        "str_trim_variants_and_custom_set",
    );
}

#[test]
fn str_replace_edges() {
    assert_parity(
        &[
            "SELECT replace('aaa', 'a', 'bb')",  // 'bbbbbb'
            "SELECT replace('hello', 'l', '')",  // 'heo'
            "SELECT replace('hello', 'z', 'Q')", // 'hello' (not found)
            "SELECT replace('abc', '', 'X')",    // 'abc' (empty search -> unchanged)
            "SELECT replace(NULL, 'a', 'b')",    // NULL
        ],
        "str_replace_edges",
    );
}

#[test]
fn str_concat_and_concat_ws() {
    assert_parity(
        &[
            "SELECT concat('a', 'b', 'c')",          // 'abc'
            "SELECT concat('a', NULL, 'c')",         // 'ac' (NULL -> empty)
            "SELECT concat(1, 2.5, 'x')",            // '12.5x'
            "SELECT concat_ws('-', 'a', 'b', 'c')",  // 'a-b-c'
            "SELECT concat_ws('-', 'a', NULL, 'c')", // 'a-c' (NULL skipped, no '--')
            "SELECT concat_ws(',', 1, 2, 3)",        // '1,2,3'
        ],
        "str_concat_and_concat_ws",
    );
}

#[test]
fn str_unhex_and_octet_length() {
    assert_parity(
        &[
            "SELECT unhex('48656C6C6F')",   // blob 'Hello' -> X'48656C6C6F'
            "SELECT typeof(unhex('41'))",   // blob
            "SELECT unhex('xyz')",          // NULL (invalid hex)
            "SELECT octet_length('hello')", // 5
            "SELECT octet_length('café')",  // 5 (UTF-8 bytes; é = 2)
            "SELECT length('café')",        // 4 (characters)
        ],
        "str_unhex_and_octet_length",
    );
}

#[test]
fn str_format_alias() {
    assert_parity(
        &[
            "SELECT format('%d-%s', 5, 'x')", // '5-x'
            "SELECT format('%.2f', 3.14159)", // '3.14'
            "SELECT format('%05d', 42)",      // '00042'
        ],
        "str_format_alias",
    );
}

#[test]
fn str_string_agg_aggregate() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val TEXT)",
        "INSERT INTO t VALUES (1,'a','x'),(2,'a','y'),(3,'b','z'),(4,'a',NULL)",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    let mut mismatches = Vec::new();
    for q in [
        "SELECT string_agg(val, '|') FROM t WHERE grp='a'", // 'x|y' (NULL skipped)
        "SELECT grp, string_agg(val, ',') FROM t GROUP BY grp ORDER BY grp",
    ] {
        match (frank_rows(&f, q), sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (a, b) => mismatches.push(format!("{q}\n  frank: {a:?}\n  csql:  {b:?}")),
        }
    }
    assert!(
        mismatches.is_empty(),
        "str_string_agg_aggregate: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
