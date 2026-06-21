//! bd-2pcbz — Oracle-parity e2e: VALUES as a compound-SELECT operand vs rusqlite.
//!
//! values_constructor_oracle covers VALUES as a standalone row source and in
//! INSERT. SQLite also accepts a `VALUES (...)` clause as either side of a
//! compound (set) operator — it behaves exactly like a SELECT producing those
//! rows. This file pins `SELECT ... UNION VALUES ...`, `VALUES ... UNION SELECT
//! ...`, UNION ALL / INTERSECT / EXCEPT between VALUES clauses, multi-column
//! VALUES in a compound, and a compound-of-VALUES wrapped in a derived table. A
//! trailing `ORDER BY` (by ordinal) pins row order. Compared against rusqlite.

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

fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
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

// NOTE on the wrapping: SQLite attaches ORDER BY/LIMIT to a trailing SELECT term
// of a compound. When the last term is a VALUES clause there is no SELECT to carry
// it, so `... UNION VALUES (...) ORDER BY 1` is a SYNTAX ERROR in SQLite. To order a
// VALUES-terminated compound we wrap it in a subquery and order the outer SELECT.
// (frank is too lenient and accepts the unwrapped form — see bd-tp6ia, pinned by
// order_by_after_values_terminated_compound below.)

#[test]
fn select_union_values() {
    assert_scalar(
        &[
            "SELECT * FROM (SELECT 1 UNION VALUES (2),(3)) ORDER BY 1", // 1,2,3
            "SELECT * FROM (SELECT 2 UNION VALUES (2),(3)) ORDER BY 1", // 2,3 (dedup)
            "SELECT * FROM (SELECT 5 UNION ALL VALUES (5),(6)) ORDER BY 1", // 5,5,6
        ],
        "select_union_values",
    );
}

#[test]
fn values_union_select() {
    assert_scalar(
        &[
            "VALUES (1),(2) UNION SELECT 3 ORDER BY 1", // 1,2,3
            "VALUES (1),(2) UNION SELECT 2 ORDER BY 1", // 1,2 (dedup)
            "VALUES (3),(1),(2) UNION SELECT 0 ORDER BY 1 DESC", // 3,2,1,0
        ],
        "values_union_select",
    );
}

#[test]
fn values_set_operators_between_values() {
    assert_scalar(
        &[
            "SELECT * FROM (VALUES (1),(2),(2) UNION ALL VALUES (3)) ORDER BY 1", // 1,2,2,3
            "SELECT * FROM (VALUES (1),(2),(3) INTERSECT VALUES (2),(3),(4)) ORDER BY 1", // 2,3
            "SELECT * FROM (VALUES (1),(2),(3) EXCEPT VALUES (2)) ORDER BY 1",    // 1,3
            // chained, equal precedence left-to-right
            "SELECT * FROM (VALUES (1),(2) UNION VALUES (3) EXCEPT VALUES (2)) ORDER BY 1", // 1,3
        ],
        "values_set_operators_between_values",
    );
}

#[test]
fn values_compound_multicolumn_and_derived() {
    assert_scalar(
        &[
            // multi-column VALUES alongside a SELECT in a compound (wrapped to order)
            "SELECT * FROM (SELECT 1, 'a' UNION VALUES (2,'b'),(3,'c')) ORDER BY 1", // (1,a),(2,b),(3,c)
            // compound-of-VALUES wrapped in a derived table, then filtered
            "SELECT col FROM (VALUES (1),(2),(3) UNION VALUES (4)) AS v(col) \
             WHERE col % 2 = 0 ORDER BY col", // 2,4
            // count over a compound of VALUES used as a subquery row source
            "SELECT count(*) FROM (VALUES (1),(2) UNION ALL VALUES (2),(3))", // 4
        ],
        "values_compound_multicolumn_and_derived",
    );
}

#[test]
fn order_by_after_values_terminated_compound() {
    // These are SYNTAX ERRORS in SQLite (the compound's last term is VALUES).
    // frank accepts them and applies the ORDER BY, so the engines disagree.
    assert_scalar(
        &[
            "SELECT 1 UNION VALUES (2),(3) ORDER BY 1",
            "VALUES (1),(2),(3) INTERSECT VALUES (2),(3),(4) ORDER BY 1",
        ],
        "order_by_after_values_terminated_compound",
    );
}
