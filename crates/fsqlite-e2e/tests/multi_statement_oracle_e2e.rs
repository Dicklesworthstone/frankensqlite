//! bd-rwv52 — Oracle-parity e2e: multi-statement (semicolon batch) execution.
//!
//! A single `execute` call may carry several `;`-separated statements; rusqlite
//! runs them all via `execute_batch`. These verify FrankenSQLite's `execute`
//! does the same: a CREATE followed by several INSERTs, a full DDL+DML+UPDATE
//! +DELETE batch, a batch with SQL comments / extra whitespace / a trailing
//! semicolon, and a batch creating multiple tables. The whole batch is passed in
//! one `execute` call (not split by the harness); the resulting state is then
//! compared against rusqlite. A divergence (e.g. only the first statement runs)
//! is the finding.
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

/// Run a whole multi-statement batch in ONE execute call on each engine, assert
/// success/failure agreement, then compare the query results.
async fn batch(batch_sql: &str, queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    let fe = f.execute(batch_sql).await;
    let re = r.execute_batch(batch_sql);
    match (&fe, &re) {
        (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
        (Ok(_), Err(e)) => panic!("{label}: batch\n  frank: OK\n  csql:  ERROR({e})"),
        (Err(e), Ok(())) => panic!("{label}: batch\n  frank: ERROR({e})\n  csql:  OK"),
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
fn multi_stmt_create_then_inserts() {
    asupersync::test_utils::run_test(|| async {
        batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT); \
             INSERT INTO t VALUES (1,'a'); \
             INSERT INTO t VALUES (2,'b'); \
             INSERT INTO t VALUES (3,'c');",
            &[
                "SELECT id, v FROM t ORDER BY id", // 3 rows
                "SELECT count(*) FROM t",          // 3
            ],
            "multi_stmt_create_then_inserts",
        )
        .await;
    });
}

#[test]
fn multi_stmt_ddl_dml_update_delete() {
    asupersync::test_utils::run_test(|| async {
        batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER); \
             INSERT INTO t VALUES (1,10),(2,20),(3,30); \
             UPDATE t SET v = v * 2 WHERE id >= 2; \
             DELETE FROM t WHERE id = 1;",
            &["SELECT id, v FROM t ORDER BY id"], // (2,40),(3,60)
            "multi_stmt_ddl_dml_update_delete",
        )
        .await;
    });
}

#[test]
fn multi_stmt_with_comments_and_whitespace() {
    asupersync::test_utils::run_test(|| async {
        batch(
            "CREATE TABLE t (id INTEGER);\n\
             -- a line comment\n\
             INSERT INTO t VALUES (1);\n\
             /* block comment */ INSERT INTO t VALUES (2);\n\
             INSERT INTO t VALUES (3);\n",
            &["SELECT id FROM t ORDER BY id", "SELECT count(*) FROM t"], // 1,2,3 ; 3
            "multi_stmt_with_comments_and_whitespace",
        )
        .await;
    });
}

#[test]
fn multi_stmt_multiple_creates() {
    asupersync::test_utils::run_test(|| async {
        batch(
            "CREATE TABLE a (x INTEGER); \
             CREATE TABLE b (y TEXT); \
             INSERT INTO a VALUES (1),(2); \
             INSERT INTO b VALUES ('p'),('q');",
            &[
                "SELECT x FROM a ORDER BY x", // 1,2
                "SELECT y FROM b ORDER BY y", // 'p','q'
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('a','b')", // 2
            ],
            "multi_stmt_multiple_creates",
        )
        .await;
    });
}

#[test]
fn multi_stmt_insert_select_chain() {
    asupersync::test_utils::run_test(|| async {
        batch(
            "CREATE TABLE src (n INTEGER); \
             INSERT INTO src VALUES (1),(2),(3),(4); \
             CREATE TABLE evens (n INTEGER); \
             INSERT INTO evens SELECT n FROM src WHERE n % 2 = 0;",
            &["SELECT n FROM evens ORDER BY n"], // 2,4
            "multi_stmt_insert_select_chain",
        )
        .await;
    });
}
