//! bd-bo23l — Oracle-parity e2e: GROUP BY / HAVING edge cases vs rusqlite.
//!
//! Covers the corners where grouping diverges: HAVING with no GROUP BY (a
//! whole-table aggregate filter), GROUP BY by expression / output ordinal /
//! alias, NULL grouping (all NULLs collapse to one group), grouping by a
//! constant (single group), HAVING that references an aggregate not in the
//! SELECT list, count(DISTINCT) in HAVING, and SQLite's "bare column tracks the
//! min/max row" special case. All data is fixed and deterministic, with stable
//! tiebreakers so row order is comparable.
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

fn sales_table() -> [&'static str; 2] {
    [
        "CREATE TABLE s (id INTEGER PRIMARY KEY, dept TEXT, amt INTEGER)",
        "INSERT INTO s VALUES \
         (1,'eng',100),(2,'eng',200),(3,'sales',50),(4,'sales',300),\
         (5,'sales',150),(6,'hr',NULL),(7,'hr',80),(8,'eng',NULL)",
    ]
}

#[test]
fn having_without_group_by() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&sales_table()).await;
        check(
            &f,
            &r,
            &[
                // Whole-table aggregate + HAVING (no GROUP BY): all-or-nothing.
                "SELECT count(*) FROM s HAVING count(*) > 2",
                "SELECT count(*) FROM s HAVING count(*) > 100",
                "SELECT sum(amt) FROM s HAVING sum(amt) > 1000",
                "SELECT count(*), sum(amt) FROM s HAVING min(amt) >= 0",
            ],
            "having_without_group_by",
        )
        .await;
    });
}

#[test]
fn group_by_expression_ordinal_alias() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&sales_table()).await;
        check(
            &f,
            &r,
            &[
                // GROUP BY expression.
                "SELECT amt % 100 AS bucket, count(*) FROM s WHERE amt IS NOT NULL GROUP BY amt % 100 ORDER BY bucket",
                // GROUP BY output ordinal.
                "SELECT dept, count(*) FROM s GROUP BY 1 ORDER BY dept",
                // GROUP BY alias.
                "SELECT dept AS d, sum(amt) FROM s GROUP BY d ORDER BY d",
                // GROUP BY a constant -> single group.
                "SELECT count(*), sum(amt) FROM s GROUP BY 1=1",
            ],
            "group_by_expression_ordinal_alias",
        )
        .await;
    });
}

#[test]
fn group_by_null_grouping() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&sales_table()).await;
        check(
            &f,
            &r,
            &[
                // NULL amt rows form a single group (NULL groups together).
                "SELECT amt, count(*) FROM s GROUP BY amt ORDER BY amt",
                // dept group with NULL members: count(*) vs count(amt).
                "SELECT dept, count(*), count(amt), sum(amt) FROM s GROUP BY dept ORDER BY dept",
                // avg/total over a group containing NULLs.
                "SELECT dept, avg(amt), total(amt) FROM s GROUP BY dept ORDER BY dept",
            ],
            "group_by_null_grouping",
        )
        .await;
    });
}

#[test]
fn having_references_and_distinct() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&sales_table()).await;
        check(
            &f,
            &r,
            &[
                // HAVING references an aggregate not in the SELECT list.
                "SELECT dept FROM s GROUP BY dept HAVING sum(amt) > 250 ORDER BY dept",
                // HAVING with count and a non-aggregate-derived condition.
                "SELECT dept, count(*) FROM s GROUP BY dept HAVING count(*) >= 2 ORDER BY dept",
                // count(DISTINCT) in HAVING.
                "SELECT dept FROM s GROUP BY dept HAVING count(DISTINCT amt) >= 2 ORDER BY dept",
                // GROUP BY + HAVING + ORDER BY on an aggregate.
                "SELECT dept, sum(amt) AS total FROM s GROUP BY dept HAVING total > 0 ORDER BY total DESC, dept",
            ],
            "having_references_and_distinct",
        )
        .await;
    });
}

#[test]
fn bare_column_tracks_min_max_row() {
    asupersync::test_utils::run_test(|| async {
        // SQLite special case: when a query has a single min()/max() and bare
        // (un-aggregated) columns, those columns are taken from the row that
        // produced the min/max. Ties are resolved deterministically here by making
        // the extreme value unique within each group.
        let (f, r) = setup(&[
            "CREATE TABLE e (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER, name TEXT)",
            "INSERT INTO e VALUES \
             (1,'eng',100,'ann'),(2,'eng',300,'bob'),(3,'eng',200,'cy'),\
             (4,'sales',150,'dee'),(5,'sales',90,'eve')",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Per-dept: name/id come from the max-salary row.
                "SELECT dept, max(salary), name, id FROM e GROUP BY dept ORDER BY dept",
                // Per-dept: name/id come from the min-salary row.
                "SELECT dept, min(salary), name, id FROM e GROUP BY dept ORDER BY dept",
                // Whole-table max with bare columns.
                "SELECT max(salary), name, id FROM e",
            ],
            "bare_column_tracks_min_max_row",
        )
        .await;
    });
}

#[test]
fn group_by_multi_key_and_having_combined() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, n INTEGER)",
            "INSERT INTO t VALUES \
             (1,1,'x',5),(2,1,'x',7),(3,1,'y',2),(4,2,'x',9),(5,2,'y',1),(6,2,'y',4)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT a, b, count(*), sum(n) FROM t GROUP BY a, b ORDER BY a, b",
                "SELECT a, b, sum(n) AS sn FROM t GROUP BY a, b HAVING sn >= 5 ORDER BY a, b",
                // GROUP BY mixing ordinal and name.
                "SELECT a, b, count(*) FROM t GROUP BY 1, b ORDER BY a, b",
            ],
            "group_by_multi_key_and_having_combined",
        )
        .await;
    });
}
