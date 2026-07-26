//! bd-8raic — Oracle-parity e2e: integer/numeric arithmetic edges vs rusqlite.
//!
//! SQLite has specific rules where reimplementations drift: integer +/-/* that
//! overflows i64 is promoted to REAL; integer division truncates toward zero;
//! `/` and `%` by zero yield NULL; `%` takes the sign of the dividend; hex
//! literals (0x...) parse as integers; an integer literal too large for i64
//! becomes REAL; bitwise &|<<>>~ operate on 64-bit integers. Result f64 values
//! are rendered with Rust's formatter on BOTH sides, so the comparison is on
//! the computed value, not on SQLite's text rendering.

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

async fn assert_scalar_parity(queries: &[&str], label: &str) {
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
fn numeric_integer_division_and_modulo() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 7 / 2",   // 3 (truncates toward zero)
                "SELECT -7 / 2",  // -3
                "SELECT 7 / -2",  // -3
                "SELECT -7 / -2", // 3
                "SELECT 7 % 3",   // 1
                "SELECT -7 % 3",  // -1 (sign of dividend)
                "SELECT 7 % -3",  // 1
                "SELECT -7 % -3", // -1
                "SELECT typeof(7 / 2), typeof(7.0 / 2)",
                "SELECT 7.0 / 2", // 3.5
                "SELECT 10 % 4",  // 2
            ],
            "numeric_integer_division_and_modulo",
        )
        .await;
    });
}

#[test]
fn numeric_division_by_zero_is_null() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 1 / 0",
                "SELECT 1.0 / 0",
                "SELECT 1 / 0.0",
                "SELECT 5 % 0",
                "SELECT 5.0 % 0",
                "SELECT 0 / 0",
                "SELECT typeof(1 / 0)",
            ],
            "numeric_division_by_zero_is_null",
        )
        .await;
    });
}

#[test]
fn numeric_overflow_promotes_to_real() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                // i64 max + 1 overflows -> REAL.
                "SELECT 9223372036854775807 + 1",
                "SELECT typeof(9223372036854775807 + 1)",
                // Large multiplication overflows -> REAL.
                "SELECT 9223372036854775807 * 2",
                "SELECT typeof(9223372036854775807 * 2)",
                "SELECT 3037000500 * 3037000500", // ~ i64 max boundary
                // Subtraction underflow.
                "SELECT -9223372036854775807 - 2",
                "SELECT typeof(-9223372036854775807 - 2)",
                // No overflow stays INTEGER.
                "SELECT typeof(1000000 * 1000000)",
            ],
            "numeric_overflow_promotes_to_real",
        )
        .await;
    });
}

#[test]
fn numeric_large_literals_and_hex() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 9223372036854775807", // i64 max
                "SELECT typeof(9223372036854775807)",
                // Literal too large for i64 -> REAL.
                "SELECT 9223372036854775808",
                "SELECT typeof(9223372036854775808)",
                // Hex literals parse as integers.
                "SELECT 0xFF",
                "SELECT 0x10",
                "SELECT 0xFFFFFFFF",
                "SELECT typeof(0xFF)",
                "SELECT 0x7FFFFFFFFFFFFFFF", // i64 max in hex
            ],
            "numeric_large_literals_and_hex",
        )
        .await;
    });
}

#[test]
fn numeric_bitwise_operators() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 12 & 10",    // 8
                "SELECT 12 | 10",    // 14
                "SELECT 12 << 2",    // 48
                "SELECT 48 >> 2",    // 12
                "SELECT ~0",         // -1
                "SELECT ~5",         // -6
                "SELECT -1 & 255",   // 255
                "SELECT 1 << 62",    // 4611686018427387904
                "SELECT 255 & 0x0F", // 15
                // Bitwise coerces operands to integer.
                "SELECT '6' & 3", // 2
                "SELECT 5.9 | 0", // 5 (real truncated to int for bitwise)
            ],
            "numeric_bitwise_operators",
        )
        .await;
    });
}

#[test]
fn numeric_unary_and_abs_edges() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT -(-5)",
                "SELECT - -5",
                "SELECT +5",
                "SELECT abs(-9223372036854775807)",
                // abs of i64-min overflows -> SQLite raises an error on both engines.
                "SELECT abs(-2147483648)",
                "SELECT -9223372036854775808", // unary minus on the literal
                "SELECT typeof(-9223372036854775808)",
                "SELECT 5 - -3",
                "SELECT 2 * -3",
            ],
            "numeric_unary_and_abs_edges",
        )
        .await;
    });
}

#[test]
fn numeric_mixed_int_real_arithmetic() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT typeof(1 + 1), 1 + 1",
                "SELECT typeof(1 + 1.0), 1 + 1.0",
                "SELECT typeof(2 * 3.5), 2 * 3.5",
                "SELECT 10 / 3.0",
                "SELECT 10.0 / 4",
                // Comparison across int/real.
                "SELECT 2 = 2.0",
                "SELECT 2 < 2.5",
                "SELECT 3.0 > 2",
                // Real modulo.
                "SELECT 7.5 % 2",
                "SELECT typeof(7.5 % 2)",
            ],
            "numeric_mixed_int_real_arithmetic",
        )
        .await;
    });
}
