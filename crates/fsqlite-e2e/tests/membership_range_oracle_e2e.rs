//! bd-rhsgw — Oracle-parity e2e: IN value-list + BETWEEN operators vs rusqlite.
//!
//! The value-list `x IN (...)` (distinct from the IN-subquery form covered by
//! subquery_oracle) and `x BETWEEN a AND b` carry the usual three-valued-logic
//! traps: a NULL in the IN list makes a non-membership result NULL rather than
//! false (and the matching `NOT IN` trap), the left expression's affinity is
//! applied to the list/bound values, BETWEEN is inclusive on both ends and is
//! equivalent to `x >= a AND x <= b` (so reversed bounds yield no match and a
//! NULL bound yields NULL), and BETWEEN works over text by collation order.
//! Each scenario compares against rusqlite; the empty `IN ()` list is isolated.

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
    check(&f, &r, queries, label);
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

fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, s TEXT)",
        "INSERT INTO t VALUES (1,1,'apple'),(2,3,'banana'),(3,5,'cherry'),(4,7,'date'),(5,NULL,'elder')",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn in_list_scalar_logic() {
    assert_scalar(
        &[
            "SELECT 2 IN (1,2,3), 5 IN (1,2,3)", // 1, 0
            // NULL in list: a non-match becomes NULL, a match stays 1.
            "SELECT 5 IN (1,2,NULL)",     // NULL
            "SELECT 2 IN (1,2,NULL)",     // 1
            "SELECT 5 NOT IN (1,2,NULL)", // NULL
            "SELECT 2 NOT IN (1,2,NULL)", // 0
        ],
        "in_list_scalar_logic",
    );
}

#[test]
fn in_list_over_table() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            "SELECT id FROM t WHERE n IN (1,3,5) ORDER BY id", // 1,2,3
            "SELECT id FROM t WHERE n NOT IN (1,3) ORDER BY id", // 3,4 (NULL row excluded)
            "SELECT id FROM t WHERE s IN ('apple','cherry') ORDER BY id", // 1,3
        ],
        "in_list_over_table",
    );
}

#[test]
fn in_list_text_column_vs_numeric_list() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            // TEXT column vs numeric list: no value matches -> 0 (works on both).
            "SELECT count(*) FROM t WHERE s IN (1,2,3)", // 0
        ],
        "in_list_text_column_vs_numeric_list",
    );
}

/// bd-56aj2: `x IN (list)` ignores the left operand's affinity. An INTEGER
/// column compared against a text-numeric list (`n IN ('1','5')`) coerces the
/// list to integers in SQLite (matches), but frank applies no coercion and
/// returns no rows. BETWEEN with the same coercion works (see
/// `between_over_table_and_text`).
#[test]
fn in_list_applies_affinity() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            "SELECT id FROM t WHERE n IN ('1','5') ORDER BY id", // expect 1,3
        ],
        "in_list_applies_affinity",
    );
}

#[test]
fn between_scalar_logic() {
    assert_scalar(
        &[
            // Inclusive on both ends.
            "SELECT 3 BETWEEN 1 AND 5, 5 BETWEEN 1 AND 5, 1 BETWEEN 1 AND 5", // 1,1,1
            "SELECT 6 BETWEEN 1 AND 5, 0 BETWEEN 1 AND 5",                    // 0,0
            // Reversed bounds -> never matches (x>=5 AND x<=1).
            "SELECT 3 BETWEEN 5 AND 1", // 0
            // NULL bounds / value -> NULL.
            "SELECT 3 BETWEEN NULL AND 5, 3 BETWEEN 1 AND NULL, NULL BETWEEN 1 AND 5",
            "SELECT 3 NOT BETWEEN 1 AND 5, 6 NOT BETWEEN 1 AND 5", // 0,1
        ],
        "between_scalar_logic",
    );
}

#[test]
fn between_over_table_and_text() {
    let (f, r) = data();
    check(
        &f,
        &r,
        &[
            "SELECT id FROM t WHERE n BETWEEN 2 AND 5 ORDER BY id", // 2,3
            "SELECT id FROM t WHERE n NOT BETWEEN 2 AND 5 ORDER BY id", // 1,4 (NULL excluded)
            // Text BETWEEN by collation order.
            "SELECT id FROM t WHERE s BETWEEN 'b' AND 'd' ORDER BY id", // 2,3
            // Affinity: integer column with text bounds.
            "SELECT id FROM t WHERE n BETWEEN '2' AND '5' ORDER BY id", // 2,3
        ],
        "between_over_table_and_text",
    );
}

#[test]
fn in_empty_list() {
    // SQLite accepts the empty list: x IN () -> 0, x NOT IN () -> 1, always.
    assert_scalar(
        &[
            "SELECT 1 IN ()",
            "SELECT 1 NOT IN ()",
            "SELECT NULL IN ()",     // 0 even for NULL (no candidates)
            "SELECT NULL NOT IN ()", // 1
        ],
        "in_empty_list",
    );
}
