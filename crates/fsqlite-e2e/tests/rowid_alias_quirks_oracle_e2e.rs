//! bd-h40wr — Oracle-parity e2e: rowid-alias quirks vs rusqlite.
//!
//! rowid_oracle covers the happy path where `INTEGER PRIMARY KEY` aliases the
//! rowid. This file pins two famous SQLite quirks that nothing else tests:
//!   * `INTEGER PRIMARY KEY DESC` is NOT a rowid alias — the trailing `DESC`
//!     turns it into an ordinary indexed column, so it and `rowid` diverge and
//!     (per the next quirk) it accepts NULLs.
//!   * In a rowid table, a PRIMARY KEY column that is not the integer rowid
//!     alias does NOT imply NOT NULL (a long-standing compatibility bug SQLite
//!     preserves): NULLs — even multiple — are allowed. INTEGER PRIMARY KEY
//!     (the alias) is the exception and rejects NULL by auto-assigning a rowid.
//!
//! Fixed, deterministic data; each statement's success/failure is compared too.

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
fn integer_pk_is_rowid_alias() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
                "INSERT INTO t(v) VALUES ('a')", // id auto = 1
                "INSERT INTO t(id, v) VALUES (100, 'b')",
                "INSERT INTO t(id, v) VALUES (NULL, 'c')", // NULL alias -> auto = 101
            ],
            &[
                "SELECT id, rowid, v FROM t ORDER BY id", // (1,1,a),(100,100,b),(101,101,c)
                "SELECT count(*) FROM t WHERE id = rowid", // 3 (alias)
                "SELECT typeof(id) FROM t ORDER BY id",   // integer x3
            ],
            "integer_pk_is_rowid_alias",
        )
        .await;
    });
}

#[test]
fn integer_pk_desc_is_not_rowid_alias() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY DESC, v TEXT)",
                "INSERT INTO t(id, v) VALUES (5, 'a')", // hidden rowid = 1
                "INSERT INTO t(id, v) VALUES (10, 'b')", // hidden rowid = 2
                // not the alias, so NULL is allowed and stays NULL
                "INSERT INTO t(id, v) VALUES (NULL, 'c')", // hidden rowid = 3, id = NULL
            ],
            &[
                // id and rowid diverge: rowid 1,2,3 vs id 5,10,NULL
                "SELECT id, rowid, v FROM t ORDER BY rowid",
                "SELECT count(*) FROM t WHERE id <> rowid", // 2 (the 5 and 10 rows)
                "SELECT v FROM t WHERE id IS NULL",         // 'c'
                "SELECT id FROM t ORDER BY id",             // NULL, 5, 10
            ],
            "integer_pk_desc_is_not_rowid_alias",
        )
        .await;
    });
}

#[test]
fn nonalias_primary_key_allows_nulls() {
    // A TEXT PRIMARY KEY in a rowid table does NOT imply NOT NULL, and the
    // unique index treats NULLs as distinct, so multiple NULLs are accepted.
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (a INTEGER, b TEXT PRIMARY KEY)",
                "INSERT INTO t VALUES (1, 'x')",
                "INSERT INTO t VALUES (2, NULL)", // NULL PK allowed
                "INSERT INTO t VALUES (3, NULL)", // second NULL PK also allowed
                "INSERT INTO t VALUES (4, 'x')",  // duplicate non-NULL -> UNIQUE error
            ],
            &[
                "SELECT count(*) FROM t",                 // 3 (the dup failed)
                "SELECT count(*) FROM t WHERE b IS NULL", // 2
                "SELECT a FROM t WHERE b = 'x'",          // 1
            ],
            "nonalias_primary_key_allows_nulls",
        )
        .await;
    });
}
