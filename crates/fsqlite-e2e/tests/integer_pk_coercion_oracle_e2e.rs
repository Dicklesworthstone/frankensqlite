//! bd-m8wsi — Oracle-parity e2e: INTEGER PRIMARY KEY value coercion vs rusqlite.
//!
//! The INTEGER PRIMARY KEY (the rowid alias) is special: a value stored there
//! must be (or coerce to) an integer. `'10'` -> 10 and `20.0` -> 20 are accepted
//! via INTEGER affinity; `NULL` auto-assigns the next rowid; but `5.5` or `'abc'`
//! (which cannot be an integer rowid) raise "datatype mismatch". This is
//! STRICTER than ordinary INTEGER affinity on a non-PK column, which happily
//! stores 5.5 as a real and 'abc' as text. These verify both behaviours against
//! rusqlite.
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
fn integer_pk_coerces_text_real_and_null() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
                "INSERT INTO t VALUES ('10','a')", // '10' -> 10
                "INSERT INTO t VALUES (20.0,'b')", // 20.0 -> 20
                "INSERT INTO t VALUES (NULL,'c')", // auto -> 21
            ],
            &[
                "SELECT id, typeof(id), v FROM t ORDER BY id", // (10,integer,a),(20,integer,b),(21,integer,c)
            ],
            "integer_pk_coerces_text_real_and_null",
        )
        .await;
    });
}

#[test]
fn integer_pk_rejects_non_integer() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
                "INSERT INTO t VALUES (1,'ok')",
                "INSERT INTO t VALUES (5.5,'x')", // not an integer rowid -> datatype mismatch
                "INSERT INTO t VALUES ('abc','y')", // not coercible -> datatype mismatch
                "INSERT INTO t VALUES (2,'ok2')",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // (1,'ok'),(2,'ok2')
            "integer_pk_rejects_non_integer",
        )
        .await;
    });
}

#[test]
fn integer_affinity_non_pk_is_lenient() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                // A plain INTEGER column (not the rowid) keeps non-integer values.
                "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)",
                "INSERT INTO t VALUES (1, 5.5)",   // stays real 5.5
                "INSERT INTO t VALUES (2, 'abc')", // stays text 'abc'
                "INSERT INTO t VALUES (3, '7')",   // coerces '7' -> 7
                "INSERT INTO t VALUES (4, 9.0)",   // 9.0 -> integer 9 (no fractional part)
            ],
            &["SELECT id, typeof(n), n FROM t ORDER BY id"], // (1,real,5.5),(2,text,abc),(3,integer,7),(4,integer,9)
            "integer_affinity_non_pk_is_lenient",
        )
        .await;
    });
}
