//! bd-jr6ax — Oracle-parity e2e: JOIN types vs rusqlite (real SQLite).
//!
//! Covers the corners where joins diverge: LEFT JOIN unmatched-row NULLs and
//! the ON-vs-WHERE distinction (a predicate on the right table in WHERE turns a
//! LEFT JOIN effectively inner, but in ON it does not), CROSS join cardinality,
//! self-joins, multi-table joins, and especially NATURAL JOIN / JOIN USING —
//! where the common column is coalesced and appears exactly once, in a specific
//! position in `SELECT *`. All data is fixed and deterministic.
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

/// Two tables sharing an `id` column for NATURAL/USING joins.
fn two_tables() -> [&'static str; 4] {
    [
        "CREATE TABLE a (id INTEGER, av TEXT)",
        "CREATE TABLE b (id INTEGER, bv TEXT)",
        "INSERT INTO a VALUES (1,'a1'),(2,'a2'),(3,'a3')",
        "INSERT INTO b VALUES (1,'b1'),(2,'b2'),(4,'b4')",
    ]
}

#[test]
fn join_inner_left_cross() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                "SELECT a.id, a.av, b.bv FROM a JOIN b ON a.id = b.id ORDER BY a.id",
                // LEFT JOIN: unmatched a.id=3 yields NULL bv.
                "SELECT a.id, a.av, b.bv FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id",
                // CROSS JOIN: 3 x 3 = 9 rows.
                "SELECT count(*) FROM a CROSS JOIN b",
                "SELECT a.id, b.id FROM a CROSS JOIN b ORDER BY a.id, b.id",
            ],
            "join_inner_left_cross",
        )
        .await;
    });
}

#[test]
fn join_left_on_vs_where() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                // Predicate in ON: rows preserved, match suppressed -> NULL bv.
                "SELECT a.id, b.bv FROM a LEFT JOIN b ON a.id = b.id AND b.bv = 'b1' ORDER BY a.id",
                // Same predicate in WHERE: filters out the NULL-extended rows (inner).
                "SELECT a.id, b.bv FROM a LEFT JOIN b ON a.id = b.id WHERE b.bv = 'b1' ORDER BY a.id",
                // WHERE IS NULL on right surfaces the unmatched left rows (anti-join).
                "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE b.id IS NULL ORDER BY a.id",
            ],
            "join_left_on_vs_where",
        )
        .await;
    });
}

#[test]
fn join_using_coalesces_column() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                // USING: the joined `id` appears once. SELECT * column ORDER matters:
                // coalesced join column first, then a's rest, then b's rest.
                "SELECT * FROM a JOIN b USING(id) ORDER BY id",
                // Explicit unqualified `id` is unambiguous under USING.
                "SELECT id, av, bv FROM a JOIN b USING(id) ORDER BY id",
                // LEFT JOIN USING: unmatched still yields one id column.
                "SELECT * FROM a LEFT JOIN b USING(id) ORDER BY id",
                "SELECT id FROM a LEFT JOIN b USING(id) ORDER BY id",
            ],
            "join_using_coalesces_column",
        )
        .await;
    });
}

#[test]
fn join_natural_coalesces_common_columns() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&two_tables()).await;
        check(
            &f,
            &r,
            &[
                // NATURAL JOIN on the common `id` column; appears once.
                "SELECT * FROM a NATURAL JOIN b ORDER BY id",
                "SELECT id, av, bv FROM a NATURAL JOIN b ORDER BY id",
                // NATURAL LEFT JOIN keeps unmatched left rows.
                "SELECT * FROM a NATURAL LEFT JOIN b ORDER BY id",
                "SELECT count(*) FROM a NATURAL JOIN b",
            ],
            "join_natural_coalesces_common_columns",
        )
        .await;
    });
}

#[test]
fn join_using_multi_column() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE x (k1 INTEGER, k2 INTEGER, xv TEXT)",
            "CREATE TABLE y (k1 INTEGER, k2 INTEGER, yv TEXT)",
            "INSERT INTO x VALUES (1,1,'x11'),(1,2,'x12'),(2,1,'x21')",
            "INSERT INTO y VALUES (1,1,'y11'),(1,2,'y12'),(2,2,'y22')",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Multi-column USING coalesces both keys (each appears once).
                "SELECT * FROM x JOIN y USING(k1, k2) ORDER BY k1, k2",
                "SELECT k1, k2, xv, yv FROM x JOIN y USING(k1, k2) ORDER BY k1, k2",
                "SELECT count(*) FROM x NATURAL JOIN y",
            ],
            "join_using_multi_column",
        )
        .await;
    });
}

#[test]
fn join_self_and_multi_table() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, mgr INTEGER)",
            "INSERT INTO emp VALUES (1,'ceo',NULL),(2,'alice',1),(3,'bob',1),(4,'carol',2)",
            "CREATE TABLE dept (eid INTEGER, dname TEXT)",
            "INSERT INTO dept VALUES (2,'eng'),(3,'sales'),(4,'eng')",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Self-join: employee -> manager name.
                "SELECT e.name, m.name FROM emp e JOIN emp m ON e.mgr = m.id ORDER BY e.id",
                // Self-join with LEFT to include the manager-less CEO.
                "SELECT e.name, m.name FROM emp e LEFT JOIN emp m ON e.mgr = m.id ORDER BY e.id",
                // Three-table join.
                "SELECT e.name, d.dname, m.name AS mgr FROM emp e \
                 JOIN dept d ON d.eid = e.id JOIN emp m ON e.mgr = m.id ORDER BY e.id",
            ],
            "join_self_and_multi_table",
        )
        .await;
    });
}

#[test]
fn join_aggregate_over_left_join() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE cust (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE ord (id INTEGER PRIMARY KEY, cust_id INTEGER, amt INTEGER)",
            "INSERT INTO cust VALUES (1,'a'),(2,'b'),(3,'c')",
            "INSERT INTO ord VALUES (10,1,100),(11,1,50),(12,2,200)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // LEFT JOIN + GROUP BY: customer c with no orders -> 0 count, NULL sum.
                "SELECT cu.name, count(o.id), sum(o.amt) FROM cust cu \
                 LEFT JOIN ord o ON o.cust_id = cu.id GROUP BY cu.id ORDER BY cu.id",
                // COALESCE the NULL sum to 0.
                "SELECT cu.name, coalesce(sum(o.amt), 0) FROM cust cu \
                 LEFT JOIN ord o ON o.cust_id = cu.id GROUP BY cu.id ORDER BY cu.id",
            ],
            "join_aggregate_over_left_join",
        )
        .await;
    });
}
