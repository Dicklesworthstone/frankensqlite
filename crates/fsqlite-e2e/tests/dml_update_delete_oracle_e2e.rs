//! bd-qhml0 — Oracle-parity e2e: UPDATE / DELETE statement semantics vs rusqlite.
//!
//! Covers the SQL-level semantics of mutation statements (autocommit, the
//! BEGIN-CONCURRENT-promoted default path): self-referencing SET expressions
//! (`x = x + 1`), multi-column SET, CASE in SET, UPDATE with no WHERE (all
//! rows), a correlated subquery in SET, `UPDATE ... FROM` (SQLite 3.33+),
//! rewriting the INTEGER PRIMARY KEY / rowid, and DELETE with WHERE, with a
//! subquery predicate, and with no WHERE. Each scenario runs the DDL/DML
//! asserting per-statement agreement with rusqlite, then compares the resulting
//! table state. `UPDATE ... FROM` is isolated so a divergence there is clean.

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

fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).map_err(|e| e.to_string())?;
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

fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s);
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q), sqlite_rows(&r, q)) {
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
fn update_self_ref_and_multi_column() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
            "INSERT INTO t VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')",
            "UPDATE t SET a = a + 5 WHERE id = 2",
            "UPDATE t SET b = 'Z', a = a * 2 WHERE id = 3",
        ],
        &["SELECT id, a, b FROM t ORDER BY id"], // (1,10,x),(2,25,y),(3,60,Z)
        "update_self_ref_and_multi_column",
    );
}

#[test]
fn update_all_rows_and_case() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4)",
            "UPDATE t SET n = n * n", // no WHERE -> all rows
            "UPDATE t SET n = CASE WHEN n > 5 THEN n ELSE 0 END",
        ],
        &["SELECT id, n FROM t ORDER BY id"], // 0,0,9,16
        "update_all_rows_and_case",
    );
}

#[test]
fn update_correlated_subquery_set() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, v INTEGER)",
            "INSERT INTO t VALUES (1,'a',1),(2,'a',2),(3,'b',10),(4,'b',20)",
            // Set each row's v to the per-category max (correlated to the same table).
            "UPDATE t SET v = (SELECT max(v) FROM t t2 WHERE t2.cat = t.cat)",
        ],
        &["SELECT id, cat, v FROM t ORDER BY id"], // a->2, b->20
        "update_correlated_subquery_set",
    );
}

#[test]
fn update_rewrites_integer_primary_key() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
            "INSERT INTO t VALUES (1,10),(2,20)",
            "UPDATE t SET id = 5 WHERE id = 1",
        ],
        &["SELECT id, a FROM t ORDER BY id"], // (2,20),(5,10)
        "update_rewrites_integer_primary_key",
    );
}

#[test]
fn update_multicol_set_row_value_literals() {
    // bd-cz9o6: multi-column SET (a, b) = (e1, e2) with a parenthesized
    // row-value of scalar expressions (SQLite 3.15+).
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "INSERT INTO t VALUES (1,10,100),(2,20,200),(3,30,300)",
            "UPDATE t SET (a, b) = (99, 999) WHERE id = 1",
            // RHS elements may be arbitrary scalar expressions, incl. the row's
            // own columns (read of the pre-update value, SQLite semantics).
            "UPDATE t SET (a, b) = (b + 1, a + 1) WHERE id = 2",
        ],
        &["SELECT id, a, b FROM t ORDER BY id"], // (1,99,999),(2,201,21),(3,30,300)
        "update_multicol_set_row_value_literals",
    );
}

#[test]
fn update_multicol_set_subquery_scalar() {
    // bd-6utze: row-value SET fed by an (uncorrelated) scalar subquery.
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1,0),(2,0),(3,0)",
            "INSERT INTO src VALUES (1,11),(2,22),(3,33)",
            "UPDATE t SET (v) = (SELECT max(v) FROM src) WHERE id = 1",
        ],
        &["SELECT id, v FROM t ORDER BY id"], // (1,33),(2,0),(3,0)
        "update_multicol_set_subquery_scalar",
    );
}

#[test]
fn update_multicol_set_subquery_correlated() {
    // bd-6utze: single-column row-value SET fed by a correlated subquery,
    // plus a no-match row (subquery returns no rows -> NULL).
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1,0),(2,0),(3,0)",
            "INSERT INTO src VALUES (1,11),(2,22)",
            "UPDATE t SET (v) = (SELECT v FROM src WHERE src.id = t.id)",
        ],
        &["SELECT id, v FROM t ORDER BY id"], // (1,11),(2,22),(3,NULL)
        "update_multicol_set_subquery_correlated",
    );
}

#[test]
fn update_multicol2_set_subquery_correlated() {
    // bd-6utze: two-column row-value SET fed by a correlated subquery.
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "INSERT INTO t VALUES (1,0,0),(2,0,0),(3,0,0)",
            "INSERT INTO src VALUES (1,11,111),(2,22,222)",
            "UPDATE t SET (a, b) = (SELECT a, b FROM src WHERE src.id = t.id)",
        ],
        &["SELECT id, a, b FROM t ORDER BY id"], // (1,11,111),(2,22,222),(3,NULL,NULL)
        "update_multicol2_set_subquery_correlated",
    );
}

#[test]
fn update_multicol2_set_subquery_uncorrelated() {
    // bd-6utze: two-column row-value SET fed by an uncorrelated row subquery
    // (cannot fold to a scalar -> stays a Subquery; both targets drawn from the
    // same matched row).
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "INSERT INTO t VALUES (1,0,0),(2,0,0)",
            "INSERT INTO src VALUES (7,77,777),(8,88,888)",
            "UPDATE t SET (a, b) = (SELECT a, b FROM src WHERE src.id = 7) WHERE id = 1",
        ],
        &["SELECT id, a, b FROM t ORDER BY id"], // (1,77,777),(2,0,0)
        "update_multicol2_set_subquery_uncorrelated",
    );
}

#[test]
fn update_from_subquery_source() {
    // bd-kkfok: UPDATE ... FROM a (flattenable) subquery source. Note `val`
    // exists on BOTH t and src, so `s.val` must resolve to the source (not the
    // target) after flattening.
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO t VALUES (1,0),(2,0),(3,0)",
            "INSERT INTO src VALUES (1,100),(2,200)",
            "UPDATE t SET val = s.val FROM (SELECT id, val FROM src) s WHERE t.id = s.id",
        ],
        &["SELECT id, val FROM t ORDER BY id"], // (1,100),(2,200),(3,0)
        "update_from_subquery_source",
    );
}

#[test]
fn update_from_subquery_with_inner_where() {
    // bd-kkfok: the subquery's own WHERE must be merged (and alias-qualified)
    // into the join so only those source rows participate.
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO t VALUES (1,0),(2,0),(3,0)",
            "INSERT INTO src VALUES (1,100),(2,200),(3,300)",
            // Only src rows with val >= 200 contribute; row 1 keeps its old value.
            "UPDATE t SET val = s.val FROM (SELECT id, val FROM src WHERE val >= 200) s WHERE t.id = s.id",
        ],
        &["SELECT id, val FROM t ORDER BY id"], // (1,0),(2,200),(3,300)
        "update_from_subquery_with_inner_where",
    );
}

#[test]
fn update_from_subquery_star() {
    // bd-kkfok: `SELECT *` subquery source (passthrough columns).
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO t VALUES (1,0),(2,0)",
            "INSERT INTO src VALUES (1,11),(2,22)",
            "UPDATE t SET val = s.val FROM (SELECT * FROM src) s WHERE t.id = s.id",
        ],
        &["SELECT id, val FROM t ORDER BY id"], // (1,11),(2,22)
        "update_from_subquery_star",
    );
}

#[test]
fn update_from_source_table() {
    // UPDATE ... FROM (SQLite 3.33+). Unmatched rows keep their old value.
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)",
            "INSERT INTO t VALUES (1,0),(2,0),(3,0)",
            "INSERT INTO src VALUES (1,100),(2,200)",
            "UPDATE t SET val = src.val FROM src WHERE t.id = src.id",
        ],
        &["SELECT id, val FROM t ORDER BY id"], // (1,100),(2,200),(3,0)
        "update_from_source_table",
    );
}

#[test]
fn delete_where_predicate() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
            "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)",
            "DELETE FROM t WHERE a > 25",
        ],
        &[
            "SELECT id, a FROM t ORDER BY id", // (1,10),(2,20)
            "SELECT count(*) FROM t",          // 2
        ],
        "delete_where_predicate",
    );
}

#[test]
fn delete_with_subquery_predicate() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TABLE keep (v INTEGER)",
            "INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4)",
            "INSERT INTO keep VALUES (2),(4)",
            "DELETE FROM t WHERE v NOT IN (SELECT v FROM keep)",
        ],
        &["SELECT id, v FROM t ORDER BY id"], // (2,2),(4,4)
        "delete_with_subquery_predicate",
    );
}

#[test]
fn delete_all_rows() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
            "INSERT INTO t VALUES (1,1),(2,2),(3,3)",
            "DELETE FROM t",
        ],
        &[
            "SELECT count(*) FROM t", // 0
            // A fresh insert after a full delete reuses rowid space per SQLite rules.
            "SELECT max(id) FROM t", // NULL
        ],
        "delete_all_rows",
    );
}
