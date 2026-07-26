//! bd-41jsw — Oracle-parity e2e: CHECK constraints vs rusqlite (real SQLite).
//!
//! A CHECK constraint rejects a row only when its expression evaluates to FALSE;
//! NULL/UNKNOWN passes. Covers column and table CHECKs, the NULL-passes rule,
//! multiple CHECKs, expression/function CHECKs (length/typeof/IN), named
//! constraints, AND/OR combinations, and CHECK enforcement on UPDATE (a failed
//! UPDATE leaves the row unchanged). DML is autocommit; each scenario runs on
//! both engines asserting per-statement agreement, then compares state.
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
fn check_column_violation() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0))",
                "INSERT INTO t VALUES (1, 5)",   // ok
                "INSERT INTO t VALUES (2, -1)",  // CHECK fails -> error on both
                "INSERT INTO t VALUES (3, 0)",   // CHECK fails (0 not > 0)
                "INSERT INTO t VALUES (4, 100)", // ok
            ],
            &["SELECT id, v FROM t ORDER BY id"], // only 1 and 4
            "check_column_violation",
        )
        .await;
    });
}

#[test]
fn check_null_passes() {
    // CHECK rejects only FALSE; a NULL operand makes the predicate NULL -> pass.
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0))",
                "INSERT INTO t VALUES (1, 5)",
                "INSERT INTO t(id) VALUES (2)", // v defaults to NULL -> CHECK passes
                "INSERT INTO t VALUES (3, NULL)", // explicit NULL -> passes
            ],
            &["SELECT id, v FROM t ORDER BY id"], // all three present
            "check_null_passes",
        )
        .await;
    });
}

#[test]
fn check_table_level() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, lo INTEGER, hi INTEGER, CHECK (lo <= hi))",
                "INSERT INTO t VALUES (1, 1, 10)", // ok
                "INSERT INTO t VALUES (2, 5, 5)",  // ok (equal)
                "INSERT INTO t VALUES (3, 10, 1)", // fails
            ],
            &["SELECT id, lo, hi FROM t ORDER BY id"],
            "check_table_level",
        )
        .await;
    });
}

#[test]
fn check_multiple_constraints() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, \
               age INTEGER CHECK (age >= 0), \
               score INTEGER CHECK (score BETWEEN 0 AND 100))",
                "INSERT INTO t VALUES (1, 25, 80)", // ok
                "INSERT INTO t VALUES (2, -1, 80)", // age fails
                "INSERT INTO t VALUES (3, 25, 150)", // score fails
                "INSERT INTO t VALUES (4, 0, 0)",  // ok (boundaries)
            ],
            &["SELECT id, age, score FROM t ORDER BY id"], // 1 and 4
            "check_multiple_constraints",
        )
        .await;
    });
}

#[test]
fn check_with_expression_and_function() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, \
               name TEXT CHECK (length(name) >= 3), \
               kind TEXT CHECK (kind IN ('a','b','c')), \
               n INTEGER CHECK (typeof(n) = 'integer'))",
                "INSERT INTO t VALUES (1, 'abc', 'a', 10)", // ok
                "INSERT INTO t VALUES (2, 'ab', 'a', 10)",  // name too short
                "INSERT INTO t VALUES (3, 'abcd', 'x', 10)", // kind not in set
                "INSERT INTO t VALUES (4, 'abcd', 'b', 20)", // ok
            ],
            &["SELECT id, name, kind, n FROM t ORDER BY id"], // 1 and 4
            "check_with_expression_and_function",
        )
        .await;
    });
}

#[test]
fn check_named_and_boolean_combo() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, \
               v INTEGER CONSTRAINT v_range CHECK (v > 0 AND v < 100), \
               flag INTEGER CONSTRAINT flag_bool CHECK (flag = 0 OR flag = 1))",
                "INSERT INTO t VALUES (1, 50, 1)",  // ok
                "INSERT INTO t VALUES (2, 150, 0)", // v out of range
                "INSERT INTO t VALUES (3, 50, 2)",  // flag invalid
                "INSERT INTO t VALUES (4, 1, 0)",   // ok
            ],
            &["SELECT id, v, flag FROM t ORDER BY id"], // 1 and 4
            "check_named_and_boolean_combo",
        )
        .await;
    });
}

#[test]
fn check_enforced_on_update() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER CHECK (v >= 0))",
                "INSERT INTO t VALUES (1, 10),(2, 20)",
                "UPDATE t SET v = -5 WHERE id = 1", // violates CHECK -> fails, row unchanged
                "UPDATE t SET v = 99 WHERE id = 2", // ok
            ],
            &["SELECT id, v FROM t ORDER BY id"], // (1,10) unchanged, (2,99)
            "check_enforced_on_update",
        )
        .await;
    });
}
