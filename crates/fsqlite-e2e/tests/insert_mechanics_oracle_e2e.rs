//! bd-qjxgt — Oracle-parity e2e: INSERT statement mechanics vs rusqlite.
//!
//! Focuses on the column-list and row-source mechanics of INSERT (distinct from
//! the conflict/DEFAULT/rowid families): an explicit column list that reorders
//! or omits columns (omitted ones take their DEFAULT or NULL), `DEFAULT VALUES`,
//! multi-row VALUES preserving per-value storage class, `INSERT ... SELECT` with
//! projection transforms / WHERE filtering / a GROUP BY aggregate source, a
//! SELECT source feeding a subset column list, expressions inside VALUES, and
//! column-count mismatches (which must error on both engines). Each scenario
//! asserts per-statement agreement, then compares the resulting table state
//! ordered by rowid/key.
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
fn insert_column_list_reorder_subset_and_default_values() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (a INTEGER, b TEXT, c INTEGER DEFAULT 99)",
                "INSERT INTO t (b, a) VALUES ('x', 1)", // reordered; c -> default 99
                "INSERT INTO t (a) VALUES (2)",         // subset; b -> NULL, c -> 99
                "INSERT INTO t DEFAULT VALUES",         // a NULL, b NULL, c 99
            ],
            &["SELECT a, b, c FROM t ORDER BY rowid"],
            "insert_column_list_reorder_subset_and_default_values",
        )
        .await;
    });
}

#[test]
fn insert_multi_row_preserves_storage_class() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v)",
                "INSERT INTO t VALUES (1,100),(2,2.5),(3,'text'),(4,NULL),(5,x'AB')",
            ],
            &["SELECT id, typeof(v), v FROM t ORDER BY id"],
            "insert_multi_row_preserves_storage_class",
        )
        .await;
    });
}

#[test]
fn insert_select_transform_and_filter() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE src (id INTEGER PRIMARY KEY, n INTEGER)",
                "INSERT INTO src VALUES (1,1),(2,2),(3,3),(4,4),(5,5)",
                "CREATE TABLE dst (doubled INTEGER, label TEXT)",
                "INSERT INTO dst SELECT n*2, 'n=' || n FROM src WHERE n % 2 = 1",
            ],
            &["SELECT doubled, label FROM dst ORDER BY doubled"], // (2,n=1),(6,n=3),(10,n=5)
            "insert_select_transform_and_filter",
        )
        .await;
    });
}

#[test]
fn insert_select_into_subset_column_list() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE dst (a INTEGER, b INTEGER, c TEXT DEFAULT 'def')",
                "CREATE TABLE src (x INTEGER)",
                "INSERT INTO src VALUES (10),(20)",
                "INSERT INTO dst (a) SELECT x FROM src",
            ],
            &["SELECT a, b, c FROM dst ORDER BY a"], // (10,NULL,def),(20,NULL,def)
            "insert_select_into_subset_column_list",
        )
        .await;
    });
}

#[test]
fn insert_select_from_aggregate_source() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE src (g TEXT, v INTEGER)",
                "INSERT INTO src VALUES ('a',1),('a',2),('b',3)",
                "CREATE TABLE agg (g TEXT, total INTEGER)",
                "INSERT INTO agg SELECT g, sum(v) FROM src GROUP BY g",
            ],
            &["SELECT g, total FROM agg ORDER BY g"], // (a,3),(b,3)
            "insert_select_from_aggregate_source",
        )
        .await;
    });
}

#[test]
fn insert_expression_in_values() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, computed INTEGER)",
                "INSERT INTO t VALUES (1, 2+3*4), (2, abs(-7)), (3, length('hello'))",
            ],
            &["SELECT id, computed FROM t ORDER BY id"], // (1,14),(2,7),(3,5)
            "insert_expression_in_values",
        )
        .await;
    });
}

#[test]
fn insert_column_count_mismatch_errors() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1)", // too few -> error on both
                "INSERT INTO t (a, b) VALUES (1,2,3)", // too many -> error on both
                "INSERT INTO t VALUES (1, 2)", // ok
            ],
            &["SELECT a, b FROM t ORDER BY rowid"], // only the valid row
            "insert_column_count_mismatch_errors",
        )
        .await;
    });
}
