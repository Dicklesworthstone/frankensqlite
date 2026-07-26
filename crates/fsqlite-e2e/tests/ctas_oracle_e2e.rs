//! bd-rt9zz — Oracle-parity e2e: CREATE TABLE AS SELECT (CTAS) vs rusqlite.
//!
//! CTAS builds a new table from a query result: the column names come from the
//! SELECT's result columns/aliases, the data is copied in, and — importantly —
//! NONE of the source's PRIMARY KEY / UNIQUE / other constraints carry over (the
//! new table is unconstrained). Covered: a basic copy, alias/expression column
//! names, an aggregate source, WHERE/ORDER BY/LIMIT in the source, an empty
//! result that still creates the columns, and the no-constraint property
//! (duplicate "id" values are allowed in the CTAS result). Each scenario asserts
//! per-statement agreement with rusqlite, then compares the resulting rows.
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

const SRC: [&str; 2] = [
    "CREATE TABLE src (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)",
    "INSERT INTO src VALUES (1,'a',10),(2,'a',20),(3,'b',30)",
];

#[test]
fn ctas_basic_copy() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                SRC[0],
                SRC[1],
                "CREATE TABLE dst AS SELECT id, g, v FROM src",
            ],
            &[
                "SELECT id, g, v FROM dst ORDER BY id", // same 3 rows
                "SELECT count(*) FROM dst",             // 3
            ],
            "ctas_basic_copy",
        )
        .await;
    });
}

#[test]
fn ctas_alias_and_expression_columns() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                SRC[0],
                SRC[1],
                "CREATE TABLE calc AS SELECT id, v * 2 AS doubled, g || '!' AS tag FROM src",
            ],
            &["SELECT id, doubled, tag FROM calc ORDER BY id"], // (1,20,'a!'),(2,40,'a!'),(3,60,'b!')
            "ctas_alias_and_expression_columns",
        )
        .await;
    });
}

#[test]
fn ctas_from_aggregate() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                SRC[0],
                SRC[1],
                "CREATE TABLE summary AS SELECT g, sum(v) AS total, count(*) AS cnt FROM src GROUP BY g",
            ],
            &["SELECT g, total, cnt FROM summary ORDER BY g"], // ('a',30,2),('b',30,1)
            "ctas_from_aggregate",
        )
        .await;
    });
}

#[test]
fn ctas_with_where_orderby_limit() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                SRC[0],
                SRC[1],
                "CREATE TABLE top AS SELECT id, v FROM src WHERE v >= 20 ORDER BY v DESC LIMIT 2",
            ],
            &["SELECT id, v FROM top ORDER BY v DESC"], // (3,30),(2,20)
            "ctas_with_where_orderby_limit",
        )
        .await;
    });
}

#[test]
fn ctas_empty_result_keeps_columns() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                SRC[0],
                SRC[1],
                "CREATE TABLE empty_t AS SELECT id, v FROM src WHERE v > 1000",
                // The table exists with the two columns; a fresh insert works.
                "INSERT INTO empty_t (id, v) VALUES (1, 5)",
            ],
            &["SELECT id, v FROM empty_t ORDER BY id"], // just (1,5)
            "ctas_empty_result_keeps_columns",
        )
        .await;
    });
}

#[test]
fn ctas_carries_no_primary_key_constraint() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                SRC[0],
                SRC[1],
                // The CTAS result has NO primary key, so a duplicate id is allowed.
                "CREATE TABLE nodup AS SELECT id FROM src",
                "INSERT INTO nodup VALUES (1)", // duplicate of existing id=1 -> allowed
            ],
            &[
                "SELECT id FROM nodup ORDER BY id", // 1,1,2,3
                "SELECT count(*) FROM nodup",       // 4
            ],
            "ctas_carries_no_primary_key_constraint",
        )
        .await;
    });
}
