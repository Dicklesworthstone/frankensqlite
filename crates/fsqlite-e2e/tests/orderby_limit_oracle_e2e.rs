//! bd-l3px2 — Oracle-parity e2e: ORDER BY / LIMIT / OFFSET edge cases vs rusqlite.
//!
//! Classic divergence territory: SQLite's default NULL ordering (NULLs sort
//! first in ASC, last in DESC), explicit `NULLS FIRST`/`NULLS LAST`, the
//! "negative LIMIT means unlimited" rule, `OFFSET` without `LIMIT` (via
//! `LIMIT -1 OFFSET n`), ordering by output-column ordinal / alias / arbitrary
//! expression, multi-key ordering with mixed directions, and LIMIT/OFFSET on
//! compound (UNION) selects. All data is fixed and deterministic.

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

async fn assert_parity(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    queries: &[&str],
    label: &str,
) {
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

fn nullable_table() -> [&'static str; 2] {
    [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, s TEXT)",
        "INSERT INTO t VALUES (1,30,'c'),(2,NULL,'a'),(3,10,NULL),(4,20,'b'),(5,NULL,'d'),(6,10,'a')",
    ]
}

#[test]
fn orderby_default_null_position() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&nullable_table()).await;
        assert_parity(
            &f,
            &r,
            &[
                // Default: NULLs first on ASC, last on DESC.
                "SELECT id, v FROM t ORDER BY v",
                "SELECT id, v FROM t ORDER BY v ASC, id",
                "SELECT id, v FROM t ORDER BY v DESC, id",
                "SELECT id, s FROM t ORDER BY s, id",
                "SELECT id, s FROM t ORDER BY s DESC, id",
            ],
            "orderby_default_null_position",
        )
        .await;
    });
}

#[test]
fn orderby_explicit_nulls_first_last() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&nullable_table()).await;
        assert_parity(
            &f,
            &r,
            &[
                "SELECT id, v FROM t ORDER BY v ASC NULLS LAST, id",
                "SELECT id, v FROM t ORDER BY v DESC NULLS FIRST, id",
                "SELECT id, v FROM t ORDER BY v NULLS LAST, id",
                "SELECT id, s FROM t ORDER BY s ASC NULLS LAST, id",
            ],
            "orderby_explicit_nulls_first_last",
        )
        .await;
    });
}

#[test]
fn limit_negative_means_unlimited() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&nullable_table()).await;
        assert_parity(
            &f,
            &r,
            &[
                // Negative LIMIT == no limit in SQLite.
                "SELECT id FROM t ORDER BY id LIMIT -1",
                "SELECT id FROM t ORDER BY id LIMIT -5",
                // LIMIT 0 returns nothing.
                "SELECT id FROM t ORDER BY id LIMIT 0",
                // OFFSET with negative LIMIT still applies the offset.
                "SELECT id FROM t ORDER BY id LIMIT -1 OFFSET 2",
            ],
            "limit_negative_means_unlimited",
        )
        .await;
    });
}

#[test]
fn limit_offset_bounds() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&nullable_table()).await;
        assert_parity(
            &f,
            &r,
            &[
                "SELECT id FROM t ORDER BY id LIMIT 3",
                "SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2",
                "SELECT id FROM t ORDER BY id LIMIT 100", // beyond row count
                "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 100", // offset beyond rows
                "SELECT id FROM t ORDER BY id LIMIT 3, 2", // LIMIT offset, count form
            ],
            "limit_offset_bounds",
        )
        .await;
    });
}

#[test]
fn orderby_by_ordinal_alias_expression() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&nullable_table()).await;
        assert_parity(
            &f,
            &r,
            &[
                // Order by output-column ordinal.
                "SELECT v, id FROM t ORDER BY 1, 2",
                // Order by output alias.
                "SELECT v AS val, id FROM t ORDER BY val, id",
                // Order by arbitrary expression.
                "SELECT id, v FROM t ORDER BY v * -1, id",
                "SELECT id, v FROM t ORDER BY (v IS NULL), v, id",
                // Mixed-direction multi-key.
                "SELECT id, v, s FROM t ORDER BY v ASC, s DESC, id",
            ],
            "orderby_by_ordinal_alias_expression",
        )
        .await;
    });
}

#[test]
fn orderby_limit_on_compound_select() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE a (x INTEGER)",
            "CREATE TABLE b (x INTEGER)",
            "INSERT INTO a VALUES (3),(1),(2),(2)",
            "INSERT INTO b VALUES (5),(2),(4)",
        ])
        .await;
        assert_parity(
            &f,
            &r,
            &[
                // UNION dedups; ORDER BY + LIMIT apply to the whole compound.
                "SELECT x FROM a UNION SELECT x FROM b ORDER BY x LIMIT 3",
                "SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x DESC LIMIT 4",
                "SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x",
                "SELECT x FROM a EXCEPT SELECT x FROM b ORDER BY x",
                "SELECT x FROM a UNION SELECT x FROM b ORDER BY x LIMIT 2 OFFSET 1",
            ],
            "orderby_limit_on_compound_select",
        )
        .await;
    });
}

#[test]
fn orderby_limit_with_duplicates_stable() {
    // Ties broken by a unique key so the comparison is deterministic across
    // engines (SQLite's sort is not guaranteed stable without a tiebreaker).
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, grp INTEGER, v INTEGER)",
            "INSERT INTO t VALUES (1,1,10),(2,1,10),(3,2,10),(4,2,5),(5,1,5),(6,3,5)",
        ])
        .await;
        assert_parity(
            &f,
            &r,
            &[
                "SELECT id, grp, v FROM t ORDER BY v, grp, id LIMIT 4",
                "SELECT id FROM t ORDER BY v DESC, id DESC LIMIT 3 OFFSET 1",
                "SELECT grp, count(*) FROM t GROUP BY grp ORDER BY count(*) DESC, grp",
            ],
            "orderby_limit_with_duplicates_stable",
        )
        .await;
    });
}
