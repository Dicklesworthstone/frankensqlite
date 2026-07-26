//! bd-hlcs8 — Oracle-parity e2e: the VALUES table-value constructor vs rusqlite.
//!
//! `VALUES (...),(...)` is a row source in its own right. Covers a bare VALUES
//! statement, `SELECT ... FROM (VALUES ...)`, the default `columnN` column
//! names, VALUES bound in a CTE with an explicit column list, VALUES as the
//! right side of `IN`, VALUES joined with a base table, ORDER BY / LIMIT over a
//! VALUES source, mixed per-row value types, and INSERT...SELECT from VALUES.
//! Fixed/deterministic data; row order pinned with ORDER BY.

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
    check(&f, &r, queries, label).await;
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

#[test]
fn values_bare_statement() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "VALUES (1),(2),(3)",
                "VALUES (1,'a'),(2,'b')",
                "VALUES (42)",
            ],
            "values_bare_statement",
        )
        .await;
    });
}

#[test]
fn values_from_subquery_and_column_names() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT * FROM (VALUES (1,2),(3,4),(5,6)) ORDER BY 1",
                // Default column names are column1, column2, ...
                "SELECT column1, column2 FROM (VALUES (10,20),(30,40)) ORDER BY column1",
                "SELECT column1 FROM (VALUES (3),(1),(2)) ORDER BY column1",
                "SELECT sum(column1) FROM (VALUES (1),(2),(3),(4))",
            ],
            "values_from_subquery_and_column_names",
        )
        .await;
    });
}

#[test]
fn values_in_cte() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "WITH t(a, b) AS (VALUES (1,'x'),(2,'y'),(3,'z')) SELECT a, b FROM t ORDER BY a",
                "WITH t(a, b) AS (VALUES (1,10),(2,20)) SELECT sum(b) FROM t",
                // Reference a VALUES-CTE from another CTE.
                "WITH nums(n) AS (VALUES (1),(2),(3),(4)), evens(n) AS (SELECT n FROM nums WHERE n % 2 = 0) \
                 SELECT n FROM evens ORDER BY n",
            ],
            "values_in_cte",
        )
        .await;
    });
}

#[test]
fn values_in_predicate() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                // VALUES as the subquery on the right of IN.
                "SELECT id FROM t WHERE v IN (VALUES (20),(40)) ORDER BY id",
                "SELECT id FROM t WHERE v NOT IN (VALUES (10),(30)) ORDER BY id",
            ],
            "values_in_predicate",
        )
        .await;
    });
}

#[test]
fn values_joined_with_table() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                // Join a VALUES-derived table (with explicit aliases) to a base table.
                "SELECT t.name, vt.label FROM t JOIN (VALUES (1,'one'),(2,'two')) AS vt(id, label) \
                 ON t.id = vt.id ORDER BY t.id",
            ],
            "values_joined_with_table",
        )
        .await;
    });
}

#[test]
fn values_order_limit_and_mixed_types() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT column1 FROM (VALUES (3),(1),(4),(1),(5),(9)) ORDER BY column1 LIMIT 3",
                "SELECT column1 FROM (VALUES (3),(1),(4),(1),(5)) ORDER BY column1 DESC LIMIT 2 OFFSET 1",
                // Mixed per-row value types keep their storage class.
                "SELECT typeof(column1), column1 FROM (VALUES (1),(2.5),('x'),(NULL)) ORDER BY column1",
            ],
            "values_order_limit_and_mixed_types",
        )
        .await;
    });
}

#[test]
fn values_insert_select() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE dst (a INTEGER, b TEXT)",
            "INSERT INTO dst SELECT * FROM (VALUES (1,'x'),(2,'y'),(3,'z'))",
        ] {
            f.execute(s)
                .await
                .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
            r.execute_batch(s)
                .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
        }
        check(
            &f,
            &r,
            &["SELECT a, b FROM dst ORDER BY a"],
            "values_insert_select",
        )
        .await;
    });
}
