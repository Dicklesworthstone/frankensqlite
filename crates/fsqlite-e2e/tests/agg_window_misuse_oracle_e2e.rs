//! bd-w1393 — Oracle-parity e2e: aggregate/window misuse error parity.
//!
//! SQLite rejects several constructs at prepare time: an aggregate in WHERE
//! ("misuse of aggregate"), a nested aggregate (`sum(count(*))`), and a window
//! function anywhere other than the SELECT list or ORDER BY (WHERE / HAVING /
//! GROUP BY). A reimplementation can easily be too permissive and run these
//! anyway. These check that frank errors exactly where rusqlite errors, with
//! valid-aggregate / valid-window controls confirming the legitimate forms still
//! work. The shared comparison treats (Err,Err) as agreement and flags an
//! engine that diverges (one errors, the other succeeds).

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

fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q), sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR (frank rejected, csql accepted): {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => mismatches.push(format!(
                "CSQL_ERR (frank accepted, csql rejected): {q}\n  frank: {a:?}\n  csql: ERROR({e})"
            )),
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

fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, g TEXT)",
        "INSERT INTO t VALUES (1,10,'a'),(2,20,'a'),(3,30,'b'),(4,5,'b')",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

/// bd-fuxgg: frank runs an aggregate placed in WHERE instead of rejecting it.
#[test]
fn aggregate_in_where_is_rejected() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            "SELECT * FROM t WHERE sum(x) > 5", // misuse of aggregate -> error both
            "SELECT * FROM t WHERE count(*) > 1", // error both
            "SELECT * FROM t WHERE max(x) = x", // error both
        ],
        "aggregate_in_where_is_rejected",
    );
}

/// bd-fuxgg: frank runs a nested aggregate (returns NULL) instead of rejecting.
#[test]
fn nested_aggregate_is_rejected() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            "SELECT sum(count(*)) FROM t", // nested aggregate -> error both
            "SELECT max(avg(x)) FROM t GROUP BY g", // error both
        ],
        "nested_aggregate_is_rejected",
    );
}

/// frank correctly rejects a window function in WHERE (matches SQLite).
#[test]
fn window_function_in_where_is_rejected() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &["SELECT x FROM t WHERE row_number() OVER () = 1"],
        "window_function_in_where_is_rejected",
    );
}

/// bd-fuxgg: frank accepts a window function in HAVING / GROUP BY (SQLite rejects
/// it; window functions are only allowed in SELECT and ORDER BY).
#[test]
fn window_function_in_having_or_group_by_is_rejected() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            "SELECT g FROM t GROUP BY g HAVING row_number() OVER () > 0",
            "SELECT x FROM t GROUP BY row_number() OVER ()",
        ],
        "window_function_in_having_or_group_by_is_rejected",
    );
}

#[test]
fn valid_aggregate_and_window_controls() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            // The legitimate forms must still succeed and match.
            "SELECT sum(x), count(*), max(x) FROM t",
            "SELECT g, sum(x) FROM t GROUP BY g HAVING sum(x) > 25 ORDER BY g",
            "SELECT id, row_number() OVER (ORDER BY x) FROM t ORDER BY id",
            "SELECT id, x FROM t ORDER BY count(*) OVER () , id", // window in ORDER BY is allowed
        ],
        "valid_aggregate_and_window_controls",
    );
}
