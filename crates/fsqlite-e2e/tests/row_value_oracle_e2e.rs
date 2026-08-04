//! bd-46b2s — Oracle-parity e2e: row-value (tuple) comparisons vs rusqlite.
//!
//! SQLite supports row values: `(a,b) = (x,y)`, `(a,b) IN ((1,2),(3,4))`,
//! lexicographic ordering `(a,b) < (c,d)`, and row values on the left of an
//! `IN (SELECT ...)`. Equality is element-wise; ordering compares left-to-right
//! and stops at the first unequal element; NULL inside a comparison follows the
//! usual three-valued logic (an undetermined element yields NULL, not false).
//! Each scenario asserts per-statement agreement with rusqlite, then compares
//! query results; ORDER BY pins row order.

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

const DATA: [&str; 2] = [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
    "INSERT INTO t VALUES (1,1,1),(2,1,2),(3,2,1),(4,2,2),(5,1,NULL),(6,NULL,9)",
];

#[test]
fn rowvalue_equality() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &DATA,
            &[
                // Element-wise equality.
                "SELECT id FROM t WHERE (a,b) = (1,2) ORDER BY id", // 2
                "SELECT id FROM t WHERE (a,b) = (2,1) ORDER BY id", // 3
                "SELECT id FROM t WHERE (a,b) <> (1,1) ORDER BY id", // 2,3,4 (NULL rows excluded)
                // A NULL element makes equality undetermined (NULL), so id 5/6 drop out.
                "SELECT id FROM t WHERE (a,b) = (1,NULL) ORDER BY id",
            ],
            "rowvalue_equality",
        )
        .await;
    });
}

#[test]
fn rowvalue_in_list() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &DATA,
            &[
                "SELECT id FROM t WHERE (a,b) IN ((1,1),(2,2)) ORDER BY id", // 1,4
                "SELECT id FROM t WHERE (a,b) IN ((1,2),(2,1),(9,9)) ORDER BY id", // 2,3
                "SELECT id FROM t WHERE (a,b) NOT IN ((1,1),(1,2)) ORDER BY id",
            ],
            "rowvalue_in_list",
        )
        .await;
    });
}

#[test]
fn rowvalue_lexicographic_order() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &DATA,
            &[
                // (a,b) < (2,1): a<2, or (a=2 and b<1). Rows with no NULL element.
                "SELECT id FROM t WHERE (a,b) < (2,1) ORDER BY id", // 1,2
                "SELECT id FROM t WHERE (a,b) >= (2,1) ORDER BY id", // 3,4
                "SELECT id FROM t WHERE (a,b) <= (1,2) ORDER BY id", // 1,2
                // Constant tuple comparisons.
                "SELECT (1,2) < (1,3)", // 1
                "SELECT (1,2) < (1,1)", // 0
                "SELECT (2,0) < (1,9)", // 0 (first element decides)
            ],
            "rowvalue_lexicographic_order",
        )
        .await;
    });
}

#[test]
fn rowvalue_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1,1,1),(2,1,2),(3,2,1),(4,2,2)",
                "CREATE TABLE allow (a INTEGER, b INTEGER)",
                "INSERT INTO allow VALUES (1,2),(2,1)",
            ],
            &[
                // Row value on the left of IN (SELECT ...).
                "SELECT id FROM t WHERE (a,b) IN (SELECT a,b FROM allow) ORDER BY id", // 2,3
                "SELECT id FROM t WHERE (a,b) NOT IN (SELECT a,b FROM allow) ORDER BY id", // 1,4
            ],
            "rowvalue_in_subquery",
        )
        .await;
    });
}

#[test]
fn rowvalue_null_in_in_list() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &DATA,
            &[
                // NULL in the search row -> IN yields NULL/false per the NOT-IN trap.
                "SELECT id FROM t WHERE (a,b) IN ((1,NULL),(2,2)) ORDER BY id",
                // Constant row-value equality with NULL element is NULL (no row).
                "SELECT 1 WHERE (1,NULL) = (1,2)",
                "SELECT 1 WHERE (1,2) = (1,2)",
            ],
            "rowvalue_null_in_in_list",
        )
        .await;
    });
}
