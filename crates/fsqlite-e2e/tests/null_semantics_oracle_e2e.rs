//! bd-hav7q — Oracle-parity e2e: NULL semantics / three-valued logic + CASE
//! vs rusqlite (real SQLite).
//!
//! SQLite's three-valued logic is a dense divergence source: `x = NULL` is
//! never true, `NOT IN (.., NULL)` returns NO rows (the classic trap), `IS`/
//! `IS NOT` are the only NULL-safe comparisons, CASE treats a NULL WHEN as
//! "not matched", NULL propagates through arithmetic/concat, `AND`/`OR` follow
//! Kleene logic (`0 AND NULL = 0`, `1 OR NULL = 1`), and aggregates/DISTINCT/
//! GROUP BY each have specific NULL rules. All inputs are fixed.

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

/// Scalar (table-less) parity batch.
async fn assert_scalar_parity(queries: &[&str], label: &str) {
    let fconn = Connection::open(":memory:").await.expect("open frank");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&fconn, &rconn, queries, label).await;
}

async fn check(fconn: &Connection, rconn: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(fconn, q).await, sqlite_rows(rconn, q)) {
            (Ok(f), Ok(s)) if f == s => {}
            (Ok(f), Ok(s)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {f:?}\n  csql:  {s:?}"));
            }
            (Err(fe), Ok(s)) => {
                mismatches.push(format!(
                    "FRANK_ERR: {q}\n  frank: ERROR({fe})\n  csql:  {s:?}"
                ));
            }
            (Ok(f), Err(se)) => {
                mismatches.push(format!(
                    "CSQL_ERR: {q}\n  frank: {f:?}\n  csql: ERROR({se})"
                ));
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
    let fconn = Connection::open(":memory:").await.expect("open frank");
    let rconn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        fconn
            .execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        rconn
            .execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (fconn, rconn)
}

fn data_table() -> [&'static str; 2] {
    [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO t VALUES (1,10),(2,20),(3,NULL),(4,30),(5,NULL)",
    ]
}

#[test]
fn null_equality_is_never_true() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 1 WHERE NULL = NULL",        // empty
                "SELECT 1 WHERE 1 = NULL",           // empty
                "SELECT NULL = NULL",                // NULL
                "SELECT 1 = NULL",                   // NULL
                "SELECT NULL <> NULL",               // NULL
                "SELECT NULL IS NULL",               // 1
                "SELECT NULL IS NOT NULL",           // 0
                "SELECT 1 IS NULL",                  // 0
                "SELECT NULL IS 1",                  // 0
                "SELECT NULL IS NULL AND 1 IS NULL", // 0
            ],
            "null_equality_is_never_true",
        )
        .await;
    });
}

#[test]
fn not_in_with_null_trap() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&data_table()).await;
        check(
            &f,
            &r,
            &[
                // NOT IN containing NULL -> no rows (classic SQL trap).
                "SELECT id FROM t WHERE v NOT IN (10, NULL) ORDER BY id",
                // IN containing NULL -> matches non-null members; others are NULL (falsy).
                "SELECT id FROM t WHERE v IN (10, NULL) ORDER BY id",
                // NOT IN without NULL behaves normally (NULL v never matches).
                "SELECT id FROM t WHERE v NOT IN (10, 20) ORDER BY id",
                "SELECT id FROM t WHERE v IN (10, 30) ORDER BY id",
                // Scalar NOT IN with NULL -> NULL.
                "SELECT 5 NOT IN (1, 2, NULL)",
                "SELECT 5 IN (1, 2, NULL)",
            ],
            "not_in_with_null_trap",
        )
        .await;
    });
}

#[test]
fn case_three_valued() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                // NULL WHEN condition is not true -> ELSE.
                "SELECT CASE WHEN NULL THEN 'a' ELSE 'b' END",
                "SELECT CASE WHEN 0 THEN 'a' WHEN NULL THEN 'b' ELSE 'c' END",
                "SELECT CASE WHEN 1 THEN 'a' ELSE 'b' END",
                // No ELSE and no match -> NULL.
                "SELECT CASE WHEN NULL THEN 'a' END",
                // Simple CASE: operand = NULL never matches a WHEN, even WHEN NULL.
                "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no' END",
                "SELECT CASE NULL WHEN 1 THEN 'one' ELSE 'no' END",
                "SELECT CASE 1 WHEN 1 THEN 'one' ELSE 'no' END",
                // Result can be NULL.
                "SELECT CASE WHEN 1 THEN NULL ELSE 'b' END",
            ],
            "case_three_valued",
        )
        .await;
    });
}

#[test]
fn coalesce_ifnull_nullif() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT coalesce(NULL, NULL, 7, 8)",
                "SELECT coalesce(NULL, NULL, NULL)",
                "SELECT ifnull(NULL, 'x')",
                "SELECT ifnull(5, 'x')",
                "SELECT nullif('a', 'a')",           // NULL
                "SELECT nullif('a', 'b')",           // 'a'
                "SELECT nullif(NULL, 1)",            // NULL
                "SELECT coalesce(nullif(3, 3), 99)", // 99
            ],
            "coalesce_ifnull_nullif",
        )
        .await;
    });
}

#[test]
fn null_in_arithmetic_and_concat() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 1 + NULL",
                "SELECT NULL * 0",
                "SELECT NULL / 2",
                "SELECT NULL % 3",
                "SELECT -NULL",
                "SELECT 'a' || NULL || 'b'",
                "SELECT NULL < 1",
                "SELECT NULL > 1",
                "SELECT NULL BETWEEN 1 AND 10",
                "SELECT 5 BETWEEN NULL AND 10",
                "SELECT abs(NULL)",
            ],
            "null_in_arithmetic_and_concat",
        )
        .await;
    });
}

#[test]
fn null_kleene_boolean_logic() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar_parity(
            &[
                "SELECT 0 AND NULL",    // 0 (false dominates)
                "SELECT 1 AND NULL",    // NULL
                "SELECT NULL AND 0",    // 0
                "SELECT NULL AND 1",    // NULL
                "SELECT 1 OR NULL",     // 1 (true dominates)
                "SELECT 0 OR NULL",     // NULL
                "SELECT NULL OR 0",     // NULL
                "SELECT NOT NULL",      // NULL
                "SELECT NULL AND NULL", // NULL
                "SELECT NULL OR NULL",  // NULL
            ],
            "null_kleene_boolean_logic",
        )
        .await;
    });
}

#[test]
fn null_aggregate_distinct_groupby() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&data_table()).await;
        check(
            &f,
            &r,
            &[
                // count(*) counts all rows; count(col) skips NULL.
                "SELECT count(*), count(v) FROM t",
                // sum skips NULL; sum of all-NULL is NULL; total of all-NULL is 0.0.
                "SELECT sum(v), avg(v), total(v) FROM t",
                "SELECT sum(v) FROM t WHERE v IS NULL",
                "SELECT total(v) FROM t WHERE v IS NULL",
                // DISTINCT treats NULLs as equal -> a single NULL.
                "SELECT count(DISTINCT v) FROM t",
                "SELECT v FROM t GROUP BY v ORDER BY v",
                // GROUP BY collapses the two NULL rows into one group.
                "SELECT v, count(*) FROM t GROUP BY v ORDER BY v",
                // max/min ignore NULLs.
                "SELECT max(v), min(v) FROM t",
            ],
            "null_aggregate_distinct_groupby",
        )
        .await;
    });
}

#[test]
fn null_exists_and_correlated() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO a VALUES (1,10),(2,NULL),(3,30)",
            "INSERT INTO b VALUES (1,10),(2,NULL)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // EXISTS is two-valued (true/false), unaffected by NULL rows.
                "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.v = a.v) ORDER BY id",
                // NOT EXISTS.
                "SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.v = a.v) ORDER BY id",
                // Correlated NOT IN against a column containing NULL -> trap (no rows).
                "SELECT id FROM a WHERE v NOT IN (SELECT v FROM b) ORDER BY id",
                // IN against subquery with NULL.
                "SELECT id FROM a WHERE v IN (SELECT v FROM b) ORDER BY id",
            ],
            "null_exists_and_correlated",
        )
        .await;
    });
}
