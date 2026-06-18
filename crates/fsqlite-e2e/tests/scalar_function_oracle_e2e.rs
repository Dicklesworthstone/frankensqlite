//! bd-74kqr — Oracle-parity e2e: scalar string/numeric built-in functions vs
//! rusqlite (real SQLite).
//!
//! Covers the corner-heavy edges where clean-room implementations drift:
//! substr negative/out-of-range indices, length on multibyte UTF-8 vs blobs,
//! trim with custom character sets, instr/replace semantics, hex/quote/char/
//! unicode encodings, printf format specifiers, and abs/round/coalesce/nullif/
//! typeof. All inputs are fixed and deterministic.

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

/// Compare a batch of table-less scalar queries against rusqlite.
fn assert_parity(queries: &[&str], label: &str) {
    let fconn = Connection::open(":memory:").expect("open frank");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&fconn, q), sqlite_rows(&rconn, q)) {
            (Ok(f), Ok(s)) if f == s => {}
            (Ok(f), Ok(s)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {f:?}\n  csql:  {s:?}"));
            }
            (Err(fe), Ok(s)) => {
                mismatches.push(format!(
                    "FRANK_ERR: {q}\n  frank: ERROR({fe})\n  csql:  {s:?}"
                ));
            }
            (Ok(f), Err(se)) => {
                mismatches.push(format!(
                    "CSQL_ERR: {q}\n  frank: {f:?}\n  csql: ERROR({se})"
                ));
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
fn scalar_substr_edges() {
    assert_parity(
        &[
            "SELECT substr('hello world', 1, 5)",
            "SELECT substr('hello world', 7)",
            "SELECT substr('hello', -3, 2)", // negative start counts from end
            "SELECT substr('hello', -3)",
            "SELECT substr('hello', 0, 3)",   // start 0
            "SELECT substr('hello', 2, -1)",  // negative length
            "SELECT substr('hello', 10, 3)",  // start beyond end
            "SELECT substr('hello', 3, 100)", // length beyond end
            "SELECT substr('héllo', 1, 2)",   // multibyte: chars not bytes
            "SELECT substring('hello world', 7, 5)",
        ],
        "scalar_substr_edges",
    );
}

#[test]
fn scalar_length_text_vs_blob() {
    assert_parity(
        &[
            "SELECT length('hello')",
            "SELECT length('héllo')", // 5 chars (not 6 bytes)
            "SELECT length('')",
            "SELECT length(X'00010203')", // blob: byte count = 4
            "SELECT length(12345)",       // integer -> text length
            "SELECT length(NULL)",
            "SELECT length('日本語')", // 3 chars
        ],
        "scalar_length_text_vs_blob",
    );
}

#[test]
fn scalar_instr_replace() {
    assert_parity(
        &[
            "SELECT instr('hello world', 'o')",
            "SELECT instr('hello world', 'world')",
            "SELECT instr('hello', 'z')", // 0 = not found
            "SELECT instr('hello', '')",  // empty needle -> 1
            "SELECT replace('aaa', 'a', 'bb')",
            "SELECT replace('hello', 'l', '')",
            "SELECT replace('hello', '', 'x')", // empty pattern -> unchanged
            "SELECT replace('abcabc', 'bc', 'X')",
        ],
        "scalar_instr_replace",
    );
}

#[test]
fn scalar_trim_variants() {
    assert_parity(
        &[
            "SELECT trim('   hi   ')",
            "SELECT ltrim('   hi   ')",
            "SELECT rtrim('   hi   ')",
            "SELECT trim('xxhixx', 'x')",
            "SELECT ltrim('xxhixx', 'x')",
            "SELECT rtrim('xxhixx', 'x')",
            "SELECT trim('abcba', 'ab')", // char-set trim
            "SELECT trim('', ' ')",
            "SELECT trim('   ')",
        ],
        "scalar_trim_variants",
    );
}

#[test]
fn scalar_case_and_concat() {
    assert_parity(
        &[
            "SELECT upper('Hello World')",
            "SELECT lower('Hello World')",
            "SELECT upper('héllo')", // ASCII-only fold
            "SELECT 'a' || 'b' || 'c'",
            "SELECT 1 || 2 || 3",
            "SELECT 'x' || NULL", // NULL propagates
            "SELECT 'val=' || 42 || '!' ",
        ],
        "scalar_case_and_concat",
    );
}

#[test]
fn scalar_hex_quote_char_unicode() {
    assert_parity(
        &[
            "SELECT hex('abc')",
            "SELECT hex(X'00FF10')",
            "SELECT hex(255)",
            "SELECT quote('it''s')",
            "SELECT quote(42)",
            "SELECT quote(3.5)",
            "SELECT quote(NULL)",
            "SELECT quote(X'4142')",
            "SELECT char(72, 105)", // 'Hi'
            "SELECT unicode('A')",
            "SELECT unicode('é')",
        ],
        "scalar_hex_quote_char_unicode",
    );
}

#[test]
fn scalar_printf_specifiers() {
    assert_parity(
        &[
            "SELECT printf('%d', 42)",
            "SELECT printf('%05d', 42)",
            "SELECT printf('%x', 255)",
            "SELECT printf('%.2f', 3.14159)",
            "SELECT printf('%s-%s', 'a', 'b')",
            "SELECT printf('%+d', 7)",
            "SELECT printf('%10.3f', 2.5)",
            "SELECT printf('%%')",
            "SELECT printf('%q', 'O''Brien')", // SQLite quoting specifier
        ],
        "scalar_printf_specifiers",
    );
}

/// `printf('%c', <int>)` diverges from SQLite: frank renders the codepoint
/// (C-printf `%c`, 65 -> 'A') while SQLite emits the first character of the
/// argument's text form ('65' -> '6'). Tracked in bd-47mu0.
#[test]
fn scalar_printf_c_specifier_divergence() {
    assert_parity(
        &["SELECT printf('%c', 65)", "SELECT printf('%c', 9731)"],
        "scalar_printf_c_specifier_divergence",
    );
}

#[test]
fn scalar_numeric_and_null_funcs() {
    assert_parity(
        &[
            "SELECT abs(-5)",
            "SELECT abs(-5.5)",
            "SELECT abs(NULL)",
            "SELECT round(2.5)", // half-away-from-zero -> 3.0
            "SELECT round(3.14159, 2)",
            "SELECT round(-2.5)",
            "SELECT round(2.4)",
            "SELECT coalesce(NULL, NULL, 3, 4)",
            "SELECT ifnull(NULL, 'fallback')",
            "SELECT nullif(5, 5)",
            "SELECT nullif(5, 6)",
            "SELECT typeof(1), typeof(1.5), typeof('x'), typeof(NULL), typeof(X'00')",
            "SELECT max(3, 1, 4, 1, 5), min(3, 1, 4, 1, 5)",
            "SELECT sign(-7), sign(0), sign(12)",
        ],
        "scalar_numeric_and_null_funcs",
    );
}
