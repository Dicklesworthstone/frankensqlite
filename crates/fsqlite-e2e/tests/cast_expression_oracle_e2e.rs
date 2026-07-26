//! bd-3gn69 — Oracle-parity e2e: CAST() expression semantics vs rusqlite.
//!
//! `CAST(expr AS type)` has its own conversion rules distinct from column
//! affinity: text->integer reads the longest valid integer prefix (and yields 0
//! on no prefix), real->integer truncates toward zero, AS TEXT renders the SQL
//! text form, AS BLOB reinterprets the text encoding's bytes, AS NUMERIC applies
//! numeric affinity (integer when exact, else real), and CAST(NULL AS anything)
//! stays NULL. Each scenario asserts per-statement agreement with rusqlite, then
//! compares query results; risky/edge cases live in their own functions so a
//! divergence isolates cleanly.
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

async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
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
fn cast_text_to_integer_prefix() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                "SELECT CAST('123' AS INTEGER)",
                "SELECT CAST('123abc' AS INTEGER)", // longest integer prefix -> 123
                "SELECT CAST('   42  ' AS INTEGER)", // leading whitespace tolerated
                "SELECT CAST('abc' AS INTEGER)",    // no prefix -> 0
                "SELECT CAST('-17xyz' AS INTEGER)", // signed prefix -> -17
                "SELECT CAST('+5' AS INTEGER)",     // explicit plus -> 5
                "SELECT CAST('3.99' AS INTEGER)",   // stops at '.' -> 3
                "SELECT CAST('1e3' AS INTEGER)",    // stops at 'e' -> 1
                "SELECT typeof(CAST('123' AS INTEGER))",
            ],
            "cast_text_to_integer_prefix",
        )
        .await;
    });
}

#[test]
fn cast_real_to_integer_truncates_toward_zero() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                "SELECT CAST(3.9 AS INTEGER)",  // 3
                "SELECT CAST(-3.9 AS INTEGER)", // -3 (toward zero, not floor)
                "SELECT CAST(3.2 AS INTEGER)",  // 3
                "SELECT CAST(-0.5 AS INTEGER)", // 0
                "SELECT CAST(2.0 AS INTEGER)",  // 2
            ],
            "cast_real_to_integer_truncates_toward_zero",
        )
        .await;
    });
}

#[test]
fn cast_to_real() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                "SELECT CAST('3.14' AS REAL)",
                "SELECT CAST(42 AS REAL), typeof(CAST(42 AS REAL))", // 42.0, real
                "SELECT CAST('2.5e2' AS REAL)",                      // 250.0
                "SELECT CAST('abc' AS REAL)",                        // 0.0
                "SELECT CAST('7xyz' AS REAL)",                       // 7.0 (prefix)
            ],
            "cast_to_real",
        )
        .await;
    });
}

#[test]
fn cast_to_numeric_affinity() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                // NUMERIC keeps integer when exact, else real.
                "SELECT CAST(100 AS NUMERIC), typeof(CAST(100 AS NUMERIC))",
                "SELECT CAST(2.0 AS NUMERIC), typeof(CAST(2.0 AS NUMERIC))",
                "SELECT CAST('3.0' AS NUMERIC), typeof(CAST('3.0' AS NUMERIC))",
                "SELECT CAST('3.5' AS NUMERIC), typeof(CAST('3.5' AS NUMERIC))",
                "SELECT CAST('42' AS NUMERIC), typeof(CAST('42' AS NUMERIC))",
            ],
            "cast_to_numeric_affinity",
        )
        .await;
    });
}

#[test]
fn cast_to_text() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                "SELECT CAST(123 AS TEXT), typeof(CAST(123 AS TEXT))", // '123', text
                "SELECT CAST(3.14 AS TEXT)",                           // '3.14'
                "SELECT CAST(-0.5 AS TEXT)",                           // '-0.5'
                "SELECT CAST(3.0 AS TEXT)",                            // '3.0'
            ],
            "cast_to_text",
        )
        .await;
    });
}

#[test]
fn cast_to_blob() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                // AS BLOB reinterprets the text-encoding bytes.
                "SELECT CAST('abc' AS BLOB), typeof(CAST('abc' AS BLOB))",
                "SELECT hex(CAST('abc' AS BLOB))", // '616263'
                "SELECT CAST(123 AS BLOB)",        // bytes of '123' -> X'313233'
            ],
            "cast_to_blob",
        )
        .await;
    });
}

#[test]
fn cast_null_passthrough() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                "SELECT CAST(NULL AS INTEGER), CAST(NULL AS REAL), CAST(NULL AS TEXT), \
                 CAST(NULL AS BLOB), CAST(NULL AS NUMERIC)",
                "SELECT typeof(CAST(NULL AS INTEGER)), typeof(CAST(NULL AS TEXT)), \
                 typeof(CAST(NULL AS BLOB))",
            ],
            "cast_null_passthrough",
        )
        .await;
    });
}

#[test]
fn cast_in_table_context() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)",
                "INSERT INTO t VALUES (1,'10'),(2,'20x'),(3,'abc'),(4,'7.9')",
            ],
            &[
                // CAST applied per-row in projection and predicate.
                "SELECT id, CAST(s AS INTEGER) FROM t ORDER BY id",
                "SELECT id FROM t WHERE CAST(s AS INTEGER) >= 10 ORDER BY id",
                "SELECT sum(CAST(s AS INTEGER)) FROM t",
            ],
            "cast_in_table_context",
        )
        .await;
    });
}

#[test]
fn cast_integer_text_overflow() {
    // Out-of-i64-range integer text: SQLite clamps / falls to the boundary or
    // real. Isolated so a divergence here doesn't taint the rest of the suite.
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[],
            &[
                "SELECT CAST('9999999999999999999999' AS INTEGER)",
                "SELECT CAST('-9999999999999999999999' AS INTEGER)",
            ],
            "cast_integer_text_overflow",
        )
        .await;
    });
}
