//! bd-22j8i — Oracle-parity e2e: UPDATE SET cross-column evaluation vs rusqlite.
//!
//! In an UPDATE, every SET right-hand-side is evaluated against the ORIGINAL
//! (pre-update) row, not the partially-updated one. So `SET a=b, b=a` swaps the
//! two columns, and `SET a=a+1, b=a` gives `b` the OLD value of `a` (not a+1).
//! dml_update_delete_oracle covers self-reference and independent multi-column
//! SETs; this targets the cross-column dependency case that a naive
//! left-to-right apply would get wrong. Each scenario asserts per-statement
//! agreement with rusqlite, then compares the resulting rows.

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
fn update_set_swaps_columns() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1,10,20),(2,30,40)",
                "UPDATE t SET a = b, b = a", // swap each row using original values
            ],
            &["SELECT id, a, b FROM t ORDER BY id"], // (1,20,10),(2,40,30)
            "update_set_swaps_columns",
        )
        .await;
    });
}

#[test]
fn update_set_rhs_uses_original_row() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1,10,0)",
                // b must get the OLD a (10), not the new a (11).
                "UPDATE t SET a = a + 1, b = a",
            ],
            &["SELECT id, a, b FROM t ORDER BY id"], // (1,11,10)
            "update_set_rhs_uses_original_row",
        )
        .await;
    });
}

#[test]
fn update_set_multi_column_cross_refs() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)",
                "INSERT INTO t VALUES (1,1,2,3)",
                // All RHS use the original (1,2,3): a=b+c=5, b=a+c=4, c=a+b=3.
                "UPDATE t SET a = b + c, b = a + c, c = a + b",
            ],
            &["SELECT id, a, b, c FROM t ORDER BY id"], // (1,5,4,3)
            "update_set_multi_column_cross_refs",
        )
        .await;
    });
}

#[test]
fn update_set_constant_offset_from_old() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1,5,0),(2,7,0)",
                // b derives from the original a even though a is also being changed.
                "UPDATE t SET a = a * 10, b = a + 100",
            ],
            &["SELECT id, a, b FROM t ORDER BY id"], // (1,50,105),(2,70,107)
            "update_set_constant_offset_from_old",
        )
        .await;
    });
}
