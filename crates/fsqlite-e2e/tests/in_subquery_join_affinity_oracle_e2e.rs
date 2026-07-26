//! bd-2fvgo — Oracle-parity e2e: IN-subquery & JOIN-ON comparison affinity.
//!
//! Extends the comparison-affinity probes (bd-56aj2 IN-list, bd-w4r25 CASE) to
//! two more contexts. `x IN (SELECT ...)` applies the affinity of the left
//! expression to the comparison, so `INTEGER_col IN (SELECT text_numeric)`
//! coerces the text values to integers and matches. A JOIN `ON a.int = b.text`
//! comparison applies NUMERIC affinity to the text side (one operand numeric,
//! the other text). These verify both against rusqlite, with same-type controls.
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
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)",
        "INSERT INTO t VALUES (1,5),(2,10),(3,15)",
        "CREATE TABLE codes (c TEXT)",
        "INSERT INTO codes VALUES ('5'),('15'),('99')",
        "CREATE TABLE t2 (tid INTEGER PRIMARY KEY, code TEXT)",
        "INSERT INTO t2 VALUES (1,'5'),(2,'10')",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn in_subquery_same_type_control() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // Same-type IN subquery (control): n IN (n>7) -> 10,15.
                "SELECT id FROM t WHERE n IN (SELECT n FROM t WHERE n > 7) ORDER BY id",
            ],
            "in_subquery_same_type_control",
        )
        .await;
    });
}

#[test]
fn join_on_cross_affinity_key() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // ON int = text -> NUMERIC applied to the text side: 5='5', 10='10'.
                "SELECT t.id, t2.code FROM t JOIN t2 ON t.n = t2.code ORDER BY t.id", // (1,'5'),(2,'10')
            ],
            "join_on_cross_affinity_key",
        )
        .await;
    });
}

/// bd-56aj2 (IN-subquery extension): `INTEGER_col IN (SELECT text_numeric)` does
/// not coerce the subquery values, so nothing matches (and NOT IN matches all).
/// Same affinity gap as the IN value-list form; sibling of simple-CASE bd-w4r25.
/// (Distinct from bd-zvk68 — this subquery is non-correlated and same-type
/// IN-subqueries work.)
#[test]
fn in_subquery_applies_lhs_affinity() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // n (INTEGER) IN (text-numeric subquery): '5'->5, '15'->15 match -> 1,3.
                "SELECT id FROM t WHERE n IN (SELECT c FROM codes) ORDER BY id",
                // NOT IN: only n=10 is absent from {5,15,99} -> 2.
                "SELECT id FROM t WHERE n NOT IN (SELECT c FROM codes) ORDER BY id",
            ],
            "in_subquery_applies_lhs_affinity",
        )
        .await;
    });
}
