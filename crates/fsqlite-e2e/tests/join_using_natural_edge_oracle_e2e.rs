//! bd-8km51 — Oracle-parity e2e: JOIN USING / NATURAL edge cases vs rusqlite.
//!
//! join_types_oracle covers the USING/NATURAL coalescing happy paths. Two corners
//! are pinned here: (1) `USING (col)` where `col` is not present in BOTH tables is
//! an error ("cannot join using column ... not present in both tables"); and
//! (2) a NATURAL JOIN between tables with NO common column names is NOT an error —
//! it degrades to a cross join (every combination). Compared against rusqlite.
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

async fn engines() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in [
        "CREATE TABLE a (id INTEGER, x TEXT)",
        "INSERT INTO a VALUES (1,'ax1'),(2,'ax2')",
        "CREATE TABLE b (id INTEGER, y TEXT)",
        "INSERT INTO b VALUES (1,'by1'),(3,'by3')",
        "CREATE TABLE c (p INTEGER)",
        "INSERT INTO c VALUES (10),(20)",
        "CREATE TABLE d (q INTEGER)",
        "INSERT INTO d VALUES (100),(200)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

async fn check(queries: &[&str], label: &str) {
    let (f, r) = engines().await;
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
fn join_using_valid_column_ok() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                // 'id' is in both -> coalesced join (id=1 matches)
                "SELECT * FROM a JOIN b USING (id) ORDER BY id", // (1,'ax1','by1')
            ],
            "join_using_valid_column_ok",
        )
        .await;
    });
}

#[test]
fn join_using_nonexistent_column_rejected() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                "SELECT * FROM a JOIN b USING (nope)", // in neither table
                "SELECT * FROM a JOIN b USING (x)",    // only in a, not b
            ],
            "join_using_nonexistent_column_rejected",
        )
        .await;
    });
}

#[test]
fn natural_join_no_common_columns_is_cross_join() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                // c and d share no column names -> NATURAL JOIN degrades to cross join
                "SELECT p, q FROM c NATURAL JOIN d ORDER BY p, q",
                // (10,100),(10,200),(20,100),(20,200)
                "SELECT count(*) FROM c NATURAL JOIN d", // 4
            ],
            "natural_join_no_common_columns_is_cross_join",
        )
        .await;
    });
}
