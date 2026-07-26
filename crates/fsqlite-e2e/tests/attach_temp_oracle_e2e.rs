//! bd-ghfn2 — Oracle-parity e2e: ATTACH / TEMP databases vs rusqlite.
//!
//! Covers TEMP tables (and a TEMP table shadowing a same-named main table),
//! `ATTACH DATABASE ':memory:' AS aux`, schema/inserts in the attached db,
//! cross-database queries and joins, the `main.` / `aux.` schema qualifiers,
//! and DETACH. Each scenario asserts per-statement success/failure agreement
//! between FrankenSQLite and rusqlite (a rejected ATTACH/TEMP statement is
//! itself a finding), then compares query results.
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
fn temp_table_basic() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TEMP TABLE tt (id INTEGER PRIMARY KEY, v TEXT)",
                "INSERT INTO tt VALUES (1,'a'),(2,'b')",
                "CREATE TEMPORARY TABLE tt2 (x INTEGER)",
                "INSERT INTO tt2 VALUES (10),(20),(30)",
            ],
            &[
                "SELECT id, v FROM tt ORDER BY id",
                "SELECT sum(x) FROM tt2",
                "SELECT count(*) FROM temp.tt",
            ],
            "temp_table_basic",
        )
        .await;
    });
}

#[test]
fn temp_table_shadows_main() {
    // A TEMP table shadows a same-named table in main within the connection.
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, src TEXT)",
                "INSERT INTO t VALUES (1,'main'),(2,'main')",
                "CREATE TEMP TABLE t (id INTEGER PRIMARY KEY, src TEXT)",
                "INSERT INTO t VALUES (9,'temp')",
            ],
            &[
                // Unqualified `t` resolves to the TEMP table.
                "SELECT id, src FROM t ORDER BY id",
                // Explicit main. reaches the shadowed base table.
                "SELECT id, src FROM main.t ORDER BY id",
            ],
            "temp_table_shadows_main",
        )
        .await;
    });
}

#[test]
fn attach_memory_db_and_query() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "ATTACH DATABASE ':memory:' AS aux",
                "CREATE TABLE aux.t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO aux.t VALUES (1,100),(2,200)",
                "CREATE TABLE main.t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO main.t VALUES (1,1),(2,2),(3,3)",
            ],
            &[
                "SELECT id, v FROM aux.t ORDER BY id",
                "SELECT id, v FROM main.t ORDER BY id",
                "SELECT sum(v) FROM aux.t",
                // Unqualified resolves to main when only main has the table... both have `t`,
                // so qualify to stay unambiguous:
                "SELECT count(*) FROM aux.t",
            ],
            "attach_memory_db_and_query",
        )
        .await;
    });
}

#[test]
fn attach_cross_db_join() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "ATTACH DATABASE ':memory:' AS aux",
                "CREATE TABLE main.orders (id INTEGER PRIMARY KEY, cust INTEGER, amt INTEGER)",
                "CREATE TABLE aux.customers (id INTEGER PRIMARY KEY, name TEXT)",
                "INSERT INTO main.orders VALUES (1,10,100),(2,20,200),(3,10,50)",
                "INSERT INTO aux.customers VALUES (10,'ann'),(20,'bob')",
            ],
            &[
                // Join across the two attached databases.
                "SELECT c.name, sum(o.amt) FROM main.orders o JOIN aux.customers c ON o.cust = c.id \
                 GROUP BY c.id ORDER BY c.name",
                "SELECT o.id FROM main.orders o WHERE o.cust IN (SELECT id FROM aux.customers) ORDER BY o.id",
            ],
            "attach_cross_db_join",
        )
        .await;
    });
}

#[test]
fn attach_detach() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "ATTACH DATABASE ':memory:' AS aux",
                "CREATE TABLE aux.t (x INTEGER)",
                "INSERT INTO aux.t VALUES (1),(2)",
                "DETACH DATABASE aux",
            ],
            &[
                // After DETACH, aux.t is gone -> error on both.
                "SELECT * FROM aux.t",
            ],
            "attach_detach",
        )
        .await;
    });
}
