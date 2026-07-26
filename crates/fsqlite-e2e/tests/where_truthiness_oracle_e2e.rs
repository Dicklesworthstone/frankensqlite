//! bd-wu6aq — Oracle-parity e2e: WHERE-clause truthiness coercion vs rusqlite.
//!
//! A bare value in a WHERE clause is tested for truth by numeric coercion: a
//! non-zero number is true; NULL is not true; a TEXT value is coerced to a
//! number first, so '5' -> 5 -> true, '0' -> 0 -> false, 'abc' -> 0 -> false,
//! and '3abc' -> 3 -> true (numeric prefix). These verify that coercion against
//! rusqlite, both as constant `SELECT 1 WHERE <v>` (1 row or 0) and as a filter
//! over a table column of mixed storage classes.

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
fn where_numeric_truthiness() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 WHERE 1",    // [1]
                "SELECT 1 WHERE 0",    // []
                "SELECT 1 WHERE -3",   // [1] (non-zero)
                "SELECT 1 WHERE 0.5",  // [1]
                "SELECT 1 WHERE 0.0",  // []
                "SELECT 1 WHERE NULL", // [] (NULL is not true)
            ],
            "where_numeric_truthiness",
        )
        .await;
    });
}

#[test]
fn where_text_truthiness_via_numeric_coercion() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 WHERE '5'",    // [1] -> 5
                "SELECT 1 WHERE '0'",    // []  -> 0
                "SELECT 1 WHERE 'abc'",  // []  -> 0
                "SELECT 1 WHERE '3abc'", // [1] -> 3 (numeric prefix)
                "SELECT 1 WHERE '0abc'", // []  -> 0
                "SELECT 1 WHERE '-2'",   // [1] -> -2 (non-zero)
                "SELECT 1 WHERE ''",     // []  -> 0
            ],
            "where_text_truthiness_via_numeric_coercion",
        )
        .await;
    });
}

#[test]
fn where_truthiness_combined_with_boolean_ops() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 1 WHERE 'abc' OR 1",  // [1]
                "SELECT 1 WHERE '5' AND 'x'", // [] (5 is true, 'x'->0 false)
                "SELECT 1 WHERE NOT 'abc'",   // [1] (NOT false -> true)
                "SELECT 1 WHERE NOT '5'",     // [] (NOT true)
            ],
            "where_truthiness_combined_with_boolean_ops",
        )
        .await;
    });
}

#[test]
fn where_truthiness_over_table_column() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v)",
            "INSERT INTO t VALUES (1,5),(2,0),(3,'7'),(4,'abc'),(5,NULL),(6,0.0),(7,'0')",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                // Truthiness per row: 5 true, 0 false, '7'->7 true, 'abc'->0 false,
                // NULL false, 0.0 false, '0'->0 false. -> ids 1,3.
                "SELECT id FROM t WHERE v ORDER BY id",
                // NOT v inverts (NULL stays excluded by NOT NULL = NULL).
                "SELECT id FROM t WHERE NOT v ORDER BY id", // 2,4,6,7
            ],
            "where_truthiness_over_table_column",
        )
        .await;
    });
}
