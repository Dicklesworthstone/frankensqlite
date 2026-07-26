//! bd-d6vup — Oracle-parity e2e: SELECT DISTINCT vs rusqlite.
//!
//! DISTINCT dedups whole result rows using the same equality SQLite uses for
//! GROUP BY: all NULLs are considered equal (so they collapse to one row),
//! numeric values that compare equal across storage classes collapse (1 and 1.0
//! are the same group), text is distinct from a numeric of the same spelling,
//! and a column's declared collation governs text dedup (a NOCASE column treats
//! 'A' and 'a' as one). `SELECT DISTINCT a` and `SELECT a ... GROUP BY a` must
//! agree. These check all of that against rusqlite; the mixed-storage-class and
//! NOCASE cases are isolated so a divergence is clean.
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

async fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in stmts {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

#[test]
fn distinct_basic_and_null_collapse() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1,1),(2,1),(3,2),(4,2),(5,NULL),(6,NULL),(7,3)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // All NULLs collapse to a single distinct row; ORDER BY puts it first.
                "SELECT DISTINCT v FROM t ORDER BY v",
                // The distinct set has 4 rows (incl. NULL)...
                "SELECT count(*) FROM (SELECT DISTINCT v FROM t)",
                // ...but count(DISTINCT v) excludes NULL -> 3.
                "SELECT count(DISTINCT v) FROM t",
            ],
            "distinct_basic_and_null_collapse",
        )
        .await;
    });
}

#[test]
fn distinct_multi_column_combinations() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (a INTEGER, b TEXT)",
            "INSERT INTO t VALUES (1,'x'),(1,'x'),(1,'y'),(2,'x'),(2,NULL),(2,NULL)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Distinct (a,b) pairs; the (2,NULL) duplicate collapses.
                "SELECT DISTINCT a, b FROM t ORDER BY a, b",
                "SELECT count(*) FROM (SELECT DISTINCT a, b FROM t)", // 4
            ],
            "distinct_multi_column_combinations",
        )
        .await;
    });
}

#[test]
fn distinct_equals_group_by() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT)",
            "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a'),(4,'c'),(5,'b'),(6,NULL)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT DISTINCT g FROM t ORDER BY g",
                "SELECT g FROM t GROUP BY g ORDER BY g",
                "SELECT DISTINCT g FROM t ORDER BY g LIMIT 2",
                "SELECT DISTINCT length(g) FROM t ORDER BY 1", // distinct over an expression
            ],
            "distinct_equals_group_by",
        )
        .await;
    });
}

#[test]
fn distinct_mixed_storage_class() {
    // 1 (int) and 1.0 (real) compare equal -> collapse; '1' (text) is distinct.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (v)",
            "INSERT INTO t VALUES (1),(1.0),('1'),(1),(2),(2.0)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT count(*) FROM (SELECT DISTINCT v FROM t)", // 3: {1==1.0}, '1', {2==2.0}
                "SELECT DISTINCT v FROM t ORDER BY v",
            ],
            "distinct_mixed_storage_class",
        )
        .await;
    });
}

#[test]
fn distinct_respects_nocase_collation() {
    // A NOCASE column dedups case-insensitively.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (s TEXT COLLATE NOCASE)",
            "INSERT INTO t VALUES ('Apple'),('apple'),('BANANA'),('banana'),('Cherry')",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT count(*) FROM (SELECT DISTINCT s FROM t)", // 3
                "SELECT DISTINCT s FROM t ORDER BY s",
            ],
            "distinct_respects_nocase_collation",
        )
        .await;
    });
}
