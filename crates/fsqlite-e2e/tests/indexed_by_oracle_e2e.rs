//! bd-easri — Oracle-parity e2e: INDEXED BY / NOT INDEXED hints vs rusqlite.
//!
//! `FROM t INDEXED BY idx` forces a specific index; `FROM t NOT INDEXED` forbids
//! all indexes (full scan). The hints must NOT change the result set — only the
//! plan — so a forced or forbidden index still returns the right rows. Naming a
//! non-existent index is an error on both engines. The hints also apply to
//! UPDATE/DELETE. Each scenario asserts per-statement agreement with rusqlite,
//! then compares query results; the "forced index that cannot satisfy the query"
//! case is isolated because its error behaviour is the subtlest.
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

async fn assert_query_error_parity(stmts: &[&str], cases: &[(&str, &str)], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for sql in stmts {
        f.execute(sql).await.expect("frank setup statement");
        r.execute_batch(sql).expect("rusqlite setup statement");
    }

    for (sql, expected) in cases {
        let frank_error = frank_rows(&f, sql)
            .await
            .expect_err("FrankenSQLite must reject the invalid plan query");
        let sqlite_error =
            sqlite_rows(&r, sql).expect_err("C SQLite must reject the invalid plan query");
        assert!(
            frank_error.contains(expected),
            "{label}: unexpected FrankenSQLite error for `{sql}`: {frank_error}"
        );
        assert!(
            sqlite_error.contains(expected),
            "{label}: unexpected C SQLite error for `{sql}`: {sqlite_error}"
        );
    }

    f.close().await.expect("close frank");
}

const T: [&str; 3] = [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "CREATE INDEX idx_a ON t(a)",
    "INSERT INTO t VALUES (1,10,'x'),(2,20,'y'),(3,20,'z'),(4,30,'w')",
];

#[test]
fn indexed_by_equality_and_range() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &T,
            &[
                "SELECT id FROM t INDEXED BY idx_a WHERE a = 20 ORDER BY id", // 2,3
                "SELECT id FROM t INDEXED BY idx_a WHERE a > 15 ORDER BY id", // 2,3,4
                "SELECT count(*) FROM t INDEXED BY idx_a WHERE a <= 20",      // 3
            ],
            "indexed_by_equality_and_range",
        )
        .await;
    });
}

#[test]
fn not_indexed_forces_scan_same_results() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &T,
            &[
                "SELECT id FROM t NOT INDEXED WHERE a = 20 ORDER BY id", // 2,3 (scan)
                "SELECT id FROM t NOT INDEXED WHERE id = 1",             // 1
                "SELECT id FROM t NOT INDEXED WHERE a > 15 ORDER BY id", // 2,3,4
            ],
            "not_indexed_forces_scan_same_results",
        )
        .await;
    });
}

/// bd-pw68x: SQLite rejects `INDEXED BY <unknown index>` with "no such index",
/// but frank silently ignores the hint and runs the query (returns [2,3]). The
/// forced-index name is not validated against the schema.
#[test]
fn indexed_by_nonexistent_index_errors() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &T,
            &[
                // Naming an index that does not exist errors on both engines.
                "SELECT id FROM t INDEXED BY no_such_index WHERE a = 20",
            ],
            "indexed_by_nonexistent_index_errors",
        )
        .await;
    });
}

/// hfdt-3b8zl: both engines refuse `EXPLAIN [QUERY PLAN]` when the inner
/// SELECT names a missing `INDEXED BY` index — C SQLite fails at prepare
/// time, so the plan surfaces must not silently describe a hint-ignoring
/// plan.
#[test]
fn explain_query_plan_missing_indexed_by_errors() {
    asupersync::test_utils::run_test(|| async {
        assert_query_error_parity(
            &T,
            &[
                (
                    "EXPLAIN QUERY PLAN SELECT id FROM t INDEXED BY no_such_index WHERE a = 20",
                    "no such index: no_such_index",
                ),
                (
                    "EXPLAIN SELECT id FROM t INDEXED BY no_such_index WHERE a = 20",
                    "no such index: no_such_index",
                ),
                (
                    "EXPLAIN QUERY PLAN UPDATE t INDEXED BY no_such_index SET b = 'changed' WHERE a = 20",
                    "no such index: no_such_index",
                ),
                (
                    "EXPLAIN QUERY PLAN DELETE FROM t INDEXED BY no_such_index WHERE a = 20",
                    "no such index: no_such_index",
                ),
                (
                    "EXPLAIN QUERY PLAN SELECT id FROM missing_table INDEXED BY no_such_index",
                    "no such table: missing_table",
                ),
            ],
            "explain_query_plan_missing_indexed_by_errors",
        )
        .await;
    });
}

#[test]
fn indexed_by_in_update_and_delete() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
                "CREATE INDEX idx_a ON t(a)",
                "INSERT INTO t VALUES (1,10,'x'),(2,20,'y'),(3,20,'z'),(4,30,'w')",
                "UPDATE t INDEXED BY idx_a SET b = 'Z' WHERE a = 30",
                "DELETE FROM t INDEXED BY idx_a WHERE a = 10",
            ],
            &["SELECT id, a, b FROM t ORDER BY id"], // (2,20,y),(3,20,z),(4,30,Z)
            "indexed_by_in_update_and_delete",
        )
        .await;
    });
}

/// A forced index whose column is not constrained by the WHERE: both engines
/// agree (SQLite performs a full scan of idx_a rather than erroring), so the
/// query returns id=1 on both.
#[test]
fn indexed_by_unconstrained_column_full_scan() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &T,
            &[
                // idx_a is on (a) but the only constraint is on id; both allow it.
                "SELECT id FROM t INDEXED BY idx_a WHERE id = 1",
            ],
            "indexed_by_unconstrained_column_full_scan",
        )
        .await;
    });
}
