//! bd-8zpl9 — Oracle-parity e2e: CASE expression semantics vs rusqlite.
//!
//! CASE has two forms with distinct rules: the simple form `CASE x WHEN v ...`
//! compares `x = v` (so `WHEN NULL` NEVER matches — and a NULL operand never
//! matches anything), while the searched form `CASE WHEN cond ...` treats a
//! non-true (false OR NULL) condition as "skip". With no matching branch and no
//! ELSE the result is NULL. Branches are evaluated in order and the first match
//! wins (later branches are not consulted). The result's storage class comes
//! from the selected branch. These verify all of that against rusqlite, plus
//! CASE used in SELECT / WHERE / ORDER BY / GROUP BY / aggregate positions.
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

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&f, &r, queries, label).await;
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

#[test]
fn case_simple_form() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'c' END", // 'b'
                "SELECT CASE 5 WHEN 1 THEN 'a' WHEN 2 THEN 'b' END", // NULL (no match)
                "SELECT CASE 5 WHEN 1 THEN 'a' ELSE 'other' END",    // 'other'
                "SELECT CASE 'x' WHEN 'x' THEN 1 ELSE 0 END",        // 1
            ],
            "case_simple_form",
        )
        .await;
    });
}

#[test]
fn case_searched_form() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT CASE WHEN 1 > 2 THEN 'a' WHEN 2 > 1 THEN 'b' ELSE 'c' END", // 'b'
                // 0 is false, NULL is not true -> ELSE.
                "SELECT CASE WHEN 0 THEN 'a' WHEN NULL THEN 'b' ELSE 'c' END", // 'c'
                "SELECT CASE WHEN 1 THEN 'yes' END",                           // 'yes'
            ],
            "case_searched_form",
        )
        .await;
    });
}

#[test]
fn case_when_null_never_matches() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // Simple CASE uses x = v; NULL = anything is never true.
                "SELECT CASE NULL WHEN NULL THEN 'matched' ELSE 'no' END", // 'no'
                "SELECT CASE 1 WHEN NULL THEN 'a' ELSE 'b' END",           // 'b'
                "SELECT CASE NULL WHEN 1 THEN 'a' ELSE 'b' END",           // 'b'
            ],
            "case_when_null_never_matches",
        )
        .await;
    });
}

#[test]
fn case_no_else_yields_null() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT CASE WHEN 1 > 2 THEN 'a' END",               // NULL
                "SELECT typeof(CASE WHEN 0 THEN 1 END)",             // 'null'
                "SELECT CASE 9 WHEN 1 THEN 'a' WHEN 2 THEN 'b' END", // NULL
            ],
            "case_no_else_yields_null",
        )
        .await;
    });
}

#[test]
fn case_first_match_wins() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // Overlapping conditions: first true branch is selected.
                "SELECT CASE WHEN 20 > 0 THEN 'pos' WHEN 20 > 10 THEN 'big' ELSE 'neg' END", // 'pos'
                "SELECT CASE 1 WHEN 1 THEN 'one' WHEN 1 THEN 'also-one' END", // 'one'
            ],
            "case_first_match_wins",
        )
        .await;
    });
}

#[test]
fn case_branch_type_coercion() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT CASE WHEN 1 THEN 1 ELSE 'x' END", // 1 (integer branch chosen)
                "SELECT typeof(CASE WHEN 1 THEN 1 ELSE 2.5 END)", // integer
                "SELECT CASE WHEN 0 THEN 1 ELSE 2.5 END", // 2.5
                "SELECT typeof(CASE WHEN 0 THEN 1 ELSE 2.5 END)", // real
            ],
            "case_branch_type_coercion",
        )
        .await;
    });
}

#[test]
fn case_in_query_positions() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO t VALUES (1,2),(2,7),(3,4),(4,11),(5,6)",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                // Projection.
                "SELECT id, CASE WHEN n % 2 = 0 THEN 'even' ELSE 'odd' END FROM t ORDER BY id",
                // WHERE.
                "SELECT id FROM t WHERE CASE WHEN n > 5 THEN 1 ELSE 0 END = 1 ORDER BY id",
                // GROUP BY on a CASE alias.
                "SELECT CASE WHEN n > 5 THEN 'hi' ELSE 'lo' END AS bucket, count(*) \
                 FROM t GROUP BY bucket ORDER BY bucket",
                // Conditional aggregate.
                "SELECT sum(CASE WHEN n > 5 THEN n ELSE 0 END) FROM t",
                // ORDER BY a CASE key.
                "SELECT id FROM t ORDER BY CASE WHEN n % 2 = 0 THEN 0 ELSE 1 END, id",
            ],
            "case_in_query_positions",
        )
        .await;
    });
}
