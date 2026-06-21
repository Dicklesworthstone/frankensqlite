//! bd-ihw09 — Oracle-parity e2e: advanced printf specifiers vs rusqlite.
//!
//! scalar_function_oracle covers %d/%x/%f/%s/%05d/%+d/%10.3f and one %q; this
//! adds the SQLite-specific quoting specifiers `%q` (double single quotes),
//! `%Q` (quote + render NULL as the literal NULL) and `%w` (double double-quotes
//! for identifiers), plus the numeric specifiers `%o`/`%e`/`%g`/`%c`/`%i`/`%u`
//! and width/precision/flag handling (`%-Nd` left-justify, `% d` space flag,
//! `%.Ns` string precision, `%*d` dynamic width). Each group is isolated so a
//! divergence (e.g. a specifier not implemented, or a different NULL rendering)
//! is clean.

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

fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q), sqlite_rows(&r, q)) {
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
fn printf_quote_q() {
    assert_scalar(
        &[
            "SELECT printf('%q', 'O''Brien')", // O''Brien
            "SELECT printf('%q', 'plain')",    // plain
            "SELECT printf('[%q]', '')",       // [] (empty)
        ],
        "printf_quote_q",
    );
}

#[test]
fn printf_quote_q_and_w() {
    assert_scalar(
        &[
            "SELECT printf('%Q', 'hi')",       // 'hi' (with surrounding quotes)
            "SELECT printf('%Q', 'O''Brien')", // 'O''Brien'
            "SELECT printf('%Q', NULL)",       // NULL (the literal, unquoted)
            "SELECT printf('%w', 'a\"b')",     // a""b (identifier escaping)
        ],
        "printf_quote_Q_and_w",
    );
}

#[test]
fn printf_numeric_specifiers() {
    assert_scalar(
        &[
            "SELECT printf('%o', 8)",         // '10' (octal)
            "SELECT printf('%i', 42)",        // '42'
            "SELECT printf('%e', 12345.678)", // scientific
            "SELECT printf('%g', 0.0001)",    // '0.0001'
            "SELECT printf('%g', 1000000.0)", // '1e+06'
        ],
        "printf_numeric_specifiers",
    );
}

/// bd-jvnwt: %u is unimplemented (emitted literally) and %c uses codepoint
/// semantics (65 -> 'A') instead of SQLite's first-char-of-text (65 -> '6').
#[test]
fn printf_u_and_c() {
    assert_scalar(
        &[
            "SELECT printf('%u', 42)", // expect '42'
            "SELECT printf('%c', 65)", // SQLite -> '6'
        ],
        "printf_u_and_c",
    );
}

#[test]
fn printf_width_precision_flags() {
    assert_scalar(
        &[
            "SELECT printf('[%-10d]', 42)",      // left-justify
            "SELECT printf('[% d]', 42)",        // space flag
            "SELECT printf('[%5.2f]', 3.14159)", // ' 3.14'
            "SELECT printf('%.3s', 'abcdef')",   // 'abc' (string precision)
        ],
        "printf_width_precision_flags",
    );
}

/// bd-jvnwt: the `*` dynamic-width form is unimplemented; the whole conversion
/// is emitted literally instead of consuming the next arg as the width.
#[test]
fn printf_dynamic_width() {
    assert_scalar(
        &["SELECT printf('[%*d]', 5, 42)"], // expect '[   42]'
        "printf_dynamic_width",
    );
}
