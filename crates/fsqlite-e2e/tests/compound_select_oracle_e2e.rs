//! bd-mtmlm — Oracle-parity e2e: compound SELECT vs rusqlite (real SQLite).
//!
//! Covers UNION (dedup) vs UNION ALL (keep), INTERSECT and EXCEPT set
//! semantics, NULL treated as equal in dedup, value/type behavior across
//! branches (rows keep their storage class; dedup uses type ordering), chained
//! compounds (equal precedence, left-to-right), ORDER BY on a compound by output
//! position and by the first SELECT's column name, and column-count mismatch
//! being an error. Result order is pinned with an outer/trailing ORDER BY since
//! a bare compound's order is otherwise unspecified.
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
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"));
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"));
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

fn ab() -> [&'static str; 4] {
    [
        "CREATE TABLE a (x INTEGER)",
        "CREATE TABLE b (x INTEGER)",
        "INSERT INTO a VALUES (1),(2),(2),(3)",
        "INSERT INTO b VALUES (2),(3),(4),(4)",
    ]
}

#[test]
fn compound_union_and_union_all() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&ab()).await;
        check(
            &f,
            &r,
            &[
                // UNION dedups across both branches.
                "SELECT x FROM a UNION SELECT x FROM b ORDER BY x",
                // UNION ALL keeps every row.
                "SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x",
                "SELECT count(*) FROM (SELECT x FROM a UNION SELECT x FROM b)",
                "SELECT count(*) FROM (SELECT x FROM a UNION ALL SELECT x FROM b)",
            ],
            "compound_union_and_union_all",
        )
        .await;
    });
}

#[test]
fn compound_intersect_except() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&ab()).await;
        check(
            &f,
            &r,
            &[
                "SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x", // 2,3
                "SELECT x FROM a EXCEPT SELECT x FROM b ORDER BY x",    // 1
                "SELECT x FROM b EXCEPT SELECT x FROM a ORDER BY x",    // 4
                // INTERSECT/EXCEPT also dedup their inputs.
                "SELECT count(*) FROM (SELECT x FROM a INTERSECT SELECT x FROM b)",
            ],
            "compound_intersect_except",
        )
        .await;
    });
}

#[test]
fn compound_null_dedup() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (x INTEGER)",
            "CREATE TABLE b (x INTEGER)",
            "INSERT INTO a VALUES (1),(NULL),(NULL)",
            "INSERT INTO b VALUES (NULL),(2)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // NULLs are coalesced to a single NULL by UNION/INTERSECT/EXCEPT.
                "SELECT x FROM a UNION SELECT x FROM b ORDER BY x",
                "SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x", // NULL
                "SELECT count(*) FROM (SELECT x FROM a UNION SELECT x FROM b)",
            ],
            "compound_null_dedup",
        )
        .await;
    });
}

#[test]
fn compound_type_reconciliation() {
    // Rows keep their own storage class; dedup/order use cross-type ordering.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (x)",
            "CREATE TABLE b (x)",
            "INSERT INTO a VALUES (1),(2)",
            "INSERT INTO b VALUES ('2'),('a')",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // integer 2 and text '2' are distinct values -> not deduped.
                "SELECT typeof(x), x FROM a UNION SELECT typeof(x), x FROM b ORDER BY x, 1",
                "SELECT count(*) FROM (SELECT x FROM a UNION SELECT x FROM b)",
                // Mixed int/text literal compound.
                "SELECT typeof(c) FROM (SELECT 1 AS c UNION ALL SELECT 'x' AS c) ORDER BY c",
            ],
            "compound_type_reconciliation",
        )
        .await;
    });
}

#[test]
fn compound_chained_precedence() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (x INTEGER)",
            "CREATE TABLE b (x INTEGER)",
            "CREATE TABLE c (x INTEGER)",
            "INSERT INTO a VALUES (1),(2),(3)",
            "INSERT INTO b VALUES (3),(4)",
            "INSERT INTO c VALUES (2),(4),(5)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // Equal precedence, left-to-right: (a UNION b) INTERSECT c.
                "SELECT x FROM a UNION SELECT x FROM b INTERSECT SELECT x FROM c ORDER BY x",
                // (a UNION ALL b) EXCEPT c.
                "SELECT x FROM a UNION ALL SELECT x FROM b EXCEPT SELECT x FROM c ORDER BY x",
                // Triple UNION.
                "SELECT x FROM a UNION SELECT x FROM b UNION SELECT x FROM c ORDER BY x",
            ],
            "compound_chained_precedence",
        )
        .await;
    });
}

#[test]
fn compound_order_by_position_and_name() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (id INTEGER, label TEXT)",
            "CREATE TABLE b (id INTEGER, label TEXT)",
            "INSERT INTO a VALUES (3,'c'),(1,'a')",
            "INSERT INTO b VALUES (2,'b'),(1,'a')",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // ORDER BY output position.
                "SELECT id, label FROM a UNION SELECT id, label FROM b ORDER BY 1",
                "SELECT id, label FROM a UNION SELECT id, label FROM b ORDER BY 2 DESC, 1",
                // ORDER BY the first SELECT's column name.
                "SELECT id, label FROM a UNION ALL SELECT id, label FROM b ORDER BY id, label",
            ],
            "compound_order_by_position_and_name",
        )
        .await;
    });
}

#[test]
fn compound_matching_arity() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (x INTEGER, y INTEGER)",
            "INSERT INTO a VALUES (1,2),(3,4)",
        ])
        .await;
        check(
            &f,
            &r,
            &["SELECT x, y FROM a UNION SELECT y, x FROM a ORDER BY 1, 2"],
            "compound_matching_arity",
        )
        .await;
    });
}

/// A compound whose branches have different result-column counts must be a
/// prepare-time error; frank accepts it instead. Tracked in bd-jvzc4.
#[test]
fn compound_column_count_mismatch_errors() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (x INTEGER, y INTEGER)",
            "INSERT INTO a VALUES (1,2)",
        ])
        .await;
        let q = "SELECT x FROM a UNION SELECT x, y FROM a";
        let fe = frank_rows(&f, q).await;
        let re = sqlite_rows(&r, q);
        assert!(
            fe.is_err() && re.is_err(),
            "arity-mismatch compound: frank ok={:?}, csql ok={:?} (both must error)",
            fe.is_ok(),
            re.is_ok()
        );
    });
}
