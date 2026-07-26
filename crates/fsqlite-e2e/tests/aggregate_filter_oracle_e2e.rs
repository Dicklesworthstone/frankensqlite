//! bd-2ub34 — Oracle-parity e2e: aggregate FILTER(WHERE ...) clause vs rusqlite.
//!
//! The `agg(...) FILTER (WHERE cond)` clause (SQLite 3.30+) restricts which rows
//! an aggregate sees, independent of the query's own WHERE. Covered: a
//! conditional sum/count beside the unfiltered one, FILTER combined with
//! GROUP BY, several FILTERs with different predicates in one query,
//! FILTER + DISTINCT, FILTER on a window aggregate (`... FILTER (...) OVER (...)`),
//! and an empty filter (sum -> NULL, count -> 0). Each scenario compares results
//! against rusqlite (bundled SQLite ~3.46); the windowed-FILTER case is isolated.
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

async fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, x INTEGER)",
        "INSERT INTO t VALUES (1,'a',5),(2,'a',-3),(3,'b',20),(4,'b',-10),(5,'a',15),(6,'b',NULL)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn filter_conditional_vs_unfiltered() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // positives 5,20,15 -> 40; total non-null 27.
                "SELECT sum(x) FILTER (WHERE x > 0), sum(x) FROM t",
                // group 'a' rows -> 3; all rows -> 6.
                "SELECT count(*) FILTER (WHERE g = 'a'), count(*) FROM t",
                // count(x) skips NULL anyway; FILTER narrows further.
                "SELECT count(x) FILTER (WHERE x IS NOT NULL) FROM t", // 5
            ],
            "filter_conditional_vs_unfiltered",
        )
        .await;
    });
}

#[test]
fn filter_with_group_by() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // a: x>10 -> only 15 (sum 15, count 1); b: x>10 -> only 20 (sum 20, count 1).
                "SELECT g, sum(x) FILTER (WHERE x > 10), count(*) FILTER (WHERE x > 10) \
                 FROM t GROUP BY g ORDER BY g",
            ],
            "filter_with_group_by",
        )
        .await;
    });
}

#[test]
fn filter_multiple_predicates() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // pos = 40, neg = -13.
                "SELECT sum(x) FILTER (WHERE x > 0) AS pos, sum(x) FILTER (WHERE x < 0) AS neg FROM t",
            ],
            "filter_multiple_predicates",
        )
        .await;
    });
}

#[test]
fn filter_with_distinct() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // rows with x>0: g in {a,b} -> distinct count 2.
                "SELECT count(DISTINCT g) FILTER (WHERE x > 0) FROM t",
            ],
            "filter_with_distinct",
        )
        .await;
    });
}

#[test]
fn filter_empty_set() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // No row matches -> sum NULL, count 0.
                "SELECT sum(x) FILTER (WHERE x > 1000), count(*) FILTER (WHERE x > 1000) FROM t",
            ],
            "filter_empty_set",
        )
        .await;
    });
}

#[test]
fn filter_over_window() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // Cumulative sum of positive x by id: 5,5,25,25,40,40.
                "SELECT id, sum(x) FILTER (WHERE x > 0) OVER (ORDER BY id) FROM t ORDER BY id",
            ],
            "filter_over_window",
        )
        .await;
    });
}
