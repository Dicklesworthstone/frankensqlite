//! bd-4d0j4 — Oracle-parity e2e: typeof() of expression results vs rusqlite.
//!
//! Other oracle files use `typeof()` as a spot assertion; this one systematically
//! pins the *storage class an expression produces*, which is a frequent
//! divergence source. SQLite's rules: integer `/` integer stays integer (2, not
//! 2.5), but any real operand promotes the whole expression to real; `round()`
//! always returns real even with zero digits; concatenation (`||`) and the string
//! functions are always text; comparison / boolean / IN / BETWEEN results are
//! always integer (0/1), except that a NULL operand makes the result NULL;
//! `avg`/`total` are always real while `sum` keeps integer unless a real appears;
//! and `count` is always integer. Constant-expression typeofs are run table-less;
//! the aggregate/column cases use a fixed table.

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

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
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

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&f, &r, queries, label).await;
}

#[test]
fn typeof_of_arithmetic() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT typeof(1 + 1)",   // integer
                "SELECT typeof(1 + 1.0)", // real (real operand promotes)
                "SELECT typeof(5 / 2)",   // integer (truncating int division)
                "SELECT typeof(5.0 / 2)", // real
                "SELECT typeof(5 / 2.0)", // real
                "SELECT typeof(5 % 2)",   // integer
                "SELECT typeof(2 * 3)",   // integer
                "SELECT typeof(2 * 3.0)", // real
                "SELECT typeof(-3)",      // integer
                "SELECT typeof(-3.0)",    // real
                "SELECT typeof(+7)",      // integer
                "SELECT typeof(0x10)",    // integer (hex literal)
                "SELECT typeof(1e3)",     // real (exponent literal)
                "SELECT typeof(3.0)",     // real
            ],
            "typeof_of_arithmetic",
        )
        .await;
    });
}

#[test]
fn typeof_of_logic_and_comparison() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT typeof('a' || 'b')",        // text
                "SELECT typeof(1 || 2)",            // text (concat always text)
                "SELECT typeof(1.5 || '')",         // text
                "SELECT typeof(1 = 1)",             // integer
                "SELECT typeof(1 < 2)",             // integer
                "SELECT typeof(1 <> 2)",            // integer
                "SELECT typeof('a' IS NULL)",       // integer
                "SELECT typeof(NULL IS NULL)",      // integer
                "SELECT typeof(NULL = NULL)",       // null (NULL operand)
                "SELECT typeof(5 < NULL)",          // null
                "SELECT typeof(NOT 1)",             // integer
                "SELECT typeof(NOT NULL)",          // null
                "SELECT typeof(1 AND 0)",           // integer
                "SELECT typeof(1 AND NULL)",        // null
                "SELECT typeof(0 AND NULL)",        // integer (short-circuits to 0)
                "SELECT typeof(1 OR NULL)",         // integer (short-circuits to 1)
                "SELECT typeof(0 OR NULL)",         // null
                "SELECT typeof(5 IN (1,2,3))",      // integer
                "SELECT typeof(2 BETWEEN 1 AND 3)", // integer
            ],
            "typeof_of_logic_and_comparison",
        )
        .await;
    });
}

#[test]
fn typeof_of_functions_and_cast() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT typeof(abs(-5))",              // integer
                "SELECT typeof(abs(-5.0))",            // real
                "SELECT typeof(round(3.7))",           // real (round always real)
                "SELECT typeof(round(3.14159, 2))",    // real
                "SELECT typeof(length('abc'))",        // integer
                "SELECT typeof(upper('a'))",           // text
                "SELECT typeof(substr('abc',1,1))",    // text
                "SELECT typeof(hex(X'41'))",           // text
                "SELECT typeof(coalesce(NULL, 1))",    // integer
                "SELECT typeof(coalesce(NULL, 1.0))",  // real
                "SELECT typeof(coalesce(NULL, 'x'))",  // text
                "SELECT typeof(nullif(1, 2))",         // integer
                "SELECT typeof(nullif(1, 1))",         // null (equal -> NULL)
                "SELECT typeof(CAST(1 AS REAL))",      // real
                "SELECT typeof(CAST(1.9 AS INTEGER))", // integer
                "SELECT typeof(CAST(1 AS TEXT))",      // text
                "SELECT typeof(CAST('x' AS BLOB))",    // blob
                "SELECT typeof(X'00')",                // blob
                "SELECT typeof(NULL)",                 // null
                "SELECT typeof(iif(1, 2, 'x'))",       // integer (taken branch)
                "SELECT typeof(iif(0, 2, 'x'))",       // text
            ],
            "typeof_of_functions_and_cast",
        )
        .await;
    });
}

#[test]
fn typeof_of_aggregates_and_columns() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER, rr REAL, tx TEXT)",
            "INSERT INTO t VALUES (1, 10, 1.5, 'a')",
            "INSERT INTO t VALUES (2, 20, 2.5, 'b')",
            "INSERT INTO t VALUES (3, 30, 3.5, 'c')",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                "SELECT typeof(sum(i)) FROM t",               // integer (sum of ints)
                "SELECT typeof(sum(rr)) FROM t",              // real
                "SELECT typeof(avg(i)) FROM t",               // real (avg always real)
                "SELECT typeof(total(i)) FROM t",             // real (total always real)
                "SELECT typeof(count(*)) FROM t",             // integer
                "SELECT typeof(count(i)) FROM t",             // integer
                "SELECT typeof(max(i)) FROM t",               // integer
                "SELECT typeof(min(rr)) FROM t",              // real
                "SELECT typeof(max(tx)) FROM t",              // text
                "SELECT typeof(group_concat(tx)) FROM t",     // text
                "SELECT typeof(sum(i)) FROM t WHERE i > 100", // null (no rows -> sum is NULL)
                "SELECT typeof(count(*)) FROM t WHERE i > 100", // integer (count is 0)
                // per-row column storage class
                "SELECT typeof(i), typeof(rr), typeof(tx) FROM t ORDER BY id",
            ],
            "typeof_of_aggregates_and_columns",
        )
        .await;
    });
}

#[test]
fn typeof_of_integer_overflow() {
    asupersync::test_utils::run_test(|| async {
        // INTEGER + that overflows i64 promotes the result to REAL in SQLite.
        assert_scalar(
            &[
                "SELECT typeof(9223372036854775807 + 1)",  // real
                "SELECT typeof(9223372036854775807 * 2)",  // real
                "SELECT typeof(-9223372036854775808 - 1)", // real
            ],
            "typeof_of_integer_overflow",
        )
        .await;
    });
}
