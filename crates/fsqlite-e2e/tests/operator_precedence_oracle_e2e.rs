//! bd-sot8e — Oracle-parity e2e: operator precedence vs rusqlite.
//!
//! numeric_edge_oracle tests arithmetic/bitwise values; this tests how operators
//! BIND relative to one another. SQLite's precedence (tightest first): `||`,
//! then unary `- + ~`, then `* / %`, then binary `+ -`, then `<< >> & |`, then
//! the comparisons `< <= > >=`, then `= == != <> IS IN LIKE ...`, then `NOT`,
//! then `AND`, then `OR`. The classic traps are `||` binding tighter than `+`
//! (so `'x' || 1 + 2` is `('x'||1)+2`), `AND` binding tighter than `OR`, and `&`
//! binding tighter than `|` while `+` binds tighter than both. Compared against
//! rusqlite as constant expressions.

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
fn precedence_arithmetic() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 2 + 3 * 4",     // 14 (* over +)
                "SELECT (2 + 3) * 4",   // 20
                "SELECT 2 * 3 + 4 * 5", // 26
                "SELECT 10 - 2 - 3",    // 5 (left assoc)
                "SELECT 20 / 4 / 5",    // 1 (left assoc)
                "SELECT 5 % 3 * 2",     // 4 (% same level as *, left assoc)
            ],
            "precedence_arithmetic",
        )
        .await;
    });
}

#[test]
fn precedence_unary_minus_and_bitnot() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT -2 + 3",   // 1
                "SELECT -2 * 3",   // -6
                "SELECT 2 - -3",   // 5
                "SELECT ~5",       // -6
                "SELECT ~0",       // -1
                "SELECT -(2 + 3)", // -5
            ],
            "precedence_unary_minus_and_bitnot",
        )
        .await;
    });
}

#[test]
fn precedence_bitwise_vs_arithmetic() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 | 2 & 3",  // & over | -> 1 | (2&3=2) = 3
                "SELECT 4 + 2 & 6",  // + over & -> (4+2)&6 = 6
                "SELECT 1 << 2 + 1", // + over << -> 1 << 3 = 8
                "SELECT 6 & 4 | 1",  // (6&4=4) | 1 = 5
            ],
            "precedence_bitwise_vs_arithmetic",
        )
        .await;
    });
}

#[test]
fn precedence_concat_tightest() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 'a' || 'b' || 'c'", // 'abc'
                // || binds tighter than + : ('x'||1)+2 -> 'x1'+2 -> 0+2 -> 2
                "SELECT 'x' || 1 + 2",
                // || tighter than = : ('a'||'b')='ab' -> 1
                "SELECT 'a' || 'b' = 'ab'",
            ],
            "precedence_concat_tightest",
        )
        .await;
    });
}

#[test]
fn precedence_boolean_and_over_or() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 = 1 AND 2 = 2 OR 3 = 4", // (1 AND 1) OR 0 -> 1
                "SELECT 1 = 1 OR 2 = 3 AND 4 = 5", // 1 OR (0 AND 0) -> 1 (AND tighter)
                "SELECT 0 = 1 AND 1 = 1 OR 1 = 1", // (0 AND 1) OR 1 -> 1
                "SELECT NOT 1 = 2",                // NOT (1=2) -> 1
                "SELECT NOT 0 AND 1",              // (NOT 0) AND 1 -> 1
            ],
            "precedence_boolean_and_over_or",
        )
        .await;
    });
}

#[test]
fn precedence_comparison_vs_arithmetic() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 2 + 3 < 4 + 2",       // (5 < 6) -> 1
                "SELECT 2 * 3 = 6",           // (6 = 6) -> 1
                "SELECT 1 + 1 = 2 AND 3 > 1", // (2=2) AND (3>1) -> 1
            ],
            "precedence_comparison_vs_arithmetic",
        )
        .await;
    });
}
