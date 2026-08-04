//! bd-dajw9 — Oracle-parity e2e: EXISTS semantics vs rusqlite.
//!
//! correlated_subquery_oracle / subquery_oracle cover basic correlated EXISTS in
//! WHERE; this file pins the *semantics* that are easy to get wrong:
//!   * EXISTS only tests row presence — the subquery's SELECT list is irrelevant
//!     (`SELECT 1`, `SELECT NULL`, `SELECT *`, multi-column all behave the same).
//!   * EXISTS is a scalar yielding integer 0/1, usable in the SELECT list / CASE.
//!   * `LIMIT 0` inside the subquery makes it produce no rows, so EXISTS is 0.
//!   * a subquery whose rows are all-NULL still "exists" (presence, not value).
//!   * single-level `NOT EXISTS` set difference.
//!   * the relational-division idiom (double `NOT EXISTS`) — included to pin the
//!     real-world impact of the known triple-nest correlation bug bd-zvk68.
//! Deterministic fixed data.
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

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
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

async fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("setup `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("setup `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    (f, r)
}

const BASE: &[&str] = &[
    "CREATE TABLE t (v INTEGER)",
    "INSERT INTO t VALUES (1),(2),(3)",
    "CREATE TABLE empty (x INTEGER)",
    "CREATE TABLE nulls (v INTEGER)",
    "INSERT INTO nulls VALUES (NULL),(NULL)",
];

#[test]
fn exists_ignores_projected_value() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(BASE).await;
        check(
            &f,
            &r,
            &[
                "SELECT EXISTS(SELECT 1 FROM t)",         // 1
                "SELECT EXISTS(SELECT NULL FROM t)",      // 1 (NULL projection still exists)
                "SELECT EXISTS(SELECT * FROM t)",         // 1
                "SELECT EXISTS(SELECT 1, 2, 3 FROM t)",   // 1 (multi-column irrelevant)
                "SELECT EXISTS(SELECT 1 FROM t WHERE 0)", // 0 (no rows)
                "SELECT EXISTS(SELECT 1 FROM empty)",     // 0 (empty table)
                "SELECT NOT EXISTS(SELECT 1 FROM empty)", // 1
                // existence holds even when every row's column is NULL
                "SELECT EXISTS(SELECT v FROM nulls)", // 1
            ],
            "exists_ignores_projected_value",
        )
        .await;
    });
}

#[test]
fn exists_as_scalar_value() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(BASE).await;
        check(
            &f,
            &r,
            &[
                "SELECT typeof(EXISTS(SELECT 1 FROM t))", // integer
                "SELECT EXISTS(SELECT 1 FROM empty) + EXISTS(SELECT 1 FROM t)", // 0 + 1 = 1
                "SELECT CASE WHEN EXISTS(SELECT 1 FROM t) THEN 'yes' ELSE 'no' END", // 'yes'
                "SELECT CASE WHEN EXISTS(SELECT 1 FROM empty) THEN 'yes' ELSE 'no' END", // 'no'
                // EXISTS in the SELECT list, one column per row of an outer table.
                "SELECT v, EXISTS(SELECT 1 FROM nulls) FROM t ORDER BY v", // each row -> (v,1)
            ],
            "exists_as_scalar_value",
        )
        .await;
    });
}

#[test]
fn exists_limit_inside_subquery() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(BASE).await;
        check(
            &f,
            &r,
            &[
                "SELECT EXISTS(SELECT 1 FROM t LIMIT 1)",            // 1
                "SELECT EXISTS(SELECT 1 FROM t LIMIT 0)",            // 0 (LIMIT 0 -> no rows)
                "SELECT EXISTS(SELECT 1 FROM t ORDER BY v LIMIT 2)", // 1 (still >=1 row)
            ],
            "exists_limit_inside_subquery",
        )
        .await;
    });
}

#[test]
fn not_exists_single_level_set_difference() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE customers (id INTEGER PRIMARY KEY)",
            "INSERT INTO customers VALUES (1),(2),(3)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, cid INTEGER)",
            "INSERT INTO orders VALUES (10,1),(11,1),(12,3)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // customers with at least one order
                "SELECT id FROM customers c WHERE EXISTS \
                 (SELECT 1 FROM orders o WHERE o.cid = c.id) ORDER BY id", // 1,3
                // customers with no orders (set difference)
                "SELECT id FROM customers c WHERE NOT EXISTS \
                 (SELECT 1 FROM orders o WHERE o.cid = c.id) ORDER BY id", // 2
            ],
            "not_exists_single_level_set_difference",
        )
        .await;
    });
}

#[test]
fn relational_division_double_not_exists() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE students (sid INTEGER PRIMARY KEY)",
            "INSERT INTO students VALUES (1),(2),(3)",
            "CREATE TABLE courses (cid INTEGER PRIMARY KEY)",
            "INSERT INTO courses VALUES (10),(20)",
            "CREATE TABLE took (sid INTEGER, cid INTEGER)",
            // student 1 took both; student 2 took only 10; student 3 took both
            "INSERT INTO took VALUES (1,10),(1,20),(2,10),(3,10),(3,20)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // students who took EVERY course -> 1, 3
                "SELECT sid FROM students s WHERE NOT EXISTS (\
                   SELECT cid FROM courses c WHERE NOT EXISTS (\
                     SELECT 1 FROM took t WHERE t.sid = s.sid AND t.cid = c.cid)) ORDER BY sid",
            ],
            "relational_division_double_not_exists",
        )
        .await;
    });
}
