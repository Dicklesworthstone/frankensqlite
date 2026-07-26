//! bd-rgzj0 — Oracle-parity e2e: printf %x/%X hex specifiers vs rusqlite.
//!
//! scalar_function_oracle_e2e only spot-checks `printf('%x', 255)`. This pins the
//! lowercase/uppercase split, the alternate-form `#` flag (which adds the `0x`
//! / `0X` prefix for non-zero values), zero-pad width, and the two's-complement
//! rendering of a negative integer (a full 64-bit hex string).

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
fn printf_hex_case_and_padding() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT printf('%X', 255)",   // 'FF'  (uppercase)
                "SELECT printf('%X', 65535)", // 'FFFF'
                "SELECT printf('%x', 0)",     // '0'
                "SELECT printf('%X', 0)",     // '0'
                "SELECT printf('%08x', 255)", // '000000ff' (zero-pad to width 8)
                "SELECT printf('%5x', 10)",   // '    a'  (space-pad to width 5)
            ],
            "printf_hex_case_and_padding",
        )
        .await;
    });
}

#[test]
#[ignore = "bd-w54bm: printf %#x / %#X alternate-form flag unrecognized; frank emits the format text literally instead of the 0x/0X prefix"]
fn printf_hex_alternate_form_flag() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT printf('%#x', 255)", // SQLite: '0xff'   frank: '%#x'
                "SELECT printf('%#X', 255)", // SQLite: '0XFF'   frank: '%#X'
            ],
            "printf_hex_alternate_form_flag",
        )
        .await;
    });
}

#[test]
fn printf_hex_negative_two_complement() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // SQLite renders negative integers as a full 64-bit two's-complement
                // hex string.
                "SELECT printf('%x', -1)",   // 'ffffffffffffffff'
                "SELECT printf('%X', -1)",   // 'FFFFFFFFFFFFFFFF'
                "SELECT printf('%x', -256)", // 'ffffffffffffff00'
            ],
            "printf_hex_negative_two_complement",
        )
        .await;
    });
}
