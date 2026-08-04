//! bd-qs9kh — Oracle-parity e2e: group_concat/string_agg ordering & DISTINCT.
//!
//! aggregate_function_oracle_e2e covers group_concat separator / NULL-skip /
//! grouping; this covers the ordered-aggregate form `group_concat(x ORDER BY y)`
//! (SQLite 3.44+) — ascending, descending, ordered by a different column, with a
//! custom separator, the `string_agg(x, sep ORDER BY y)` alias, and
//! `group_concat(DISTINCT x)`. Ordered concatenation makes the output
//! deterministic (the plain form follows rowid order). Each scenario compares
//! against rusqlite; the ordered-aggregate cases are isolated so a divergence
//! (e.g. the ORDER-BY-in-aggregate syntax being unsupported) is clean.
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
        "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, v INTEGER, name TEXT)",
        "INSERT INTO t VALUES (1,'a',3,'cara'),(2,'a',1,'ann'),(3,'a',2,'bob'),(4,'b',5,'eve'),(5,'b',5,'dan'),(6,'b',4,'fay')",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn group_concat_plain_and_grouped() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // Plain form follows rowid order: grp a is 3,1,2.
                "SELECT group_concat(v) FROM t WHERE grp = 'a'", // '3,1,2'
                "SELECT grp, group_concat(v) FROM t GROUP BY grp ORDER BY grp",
            ],
            "group_concat_plain_and_grouped",
        )
        .await;
    });
}

#[test]
fn group_concat_distinct() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // grp b values 5,5,4 -> distinct.
                "SELECT group_concat(DISTINCT v) FROM t WHERE grp = 'b'",
            ],
            "group_concat_distinct",
        )
        .await;
    });
}

/// Ordered aggregates `group_concat(x ORDER BY y)` (SQLite 3.44+). Isolated so a
/// divergence (including the ORDER-BY-in-aggregate syntax being unsupported)
/// does not taint the plain-form coverage above.
#[test]
fn group_concat_ordered() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                "SELECT group_concat(v ORDER BY v) FROM t WHERE grp = 'a'", // '1,2,3'
                "SELECT group_concat(v ORDER BY v DESC) FROM t WHERE grp = 'a'", // '3,2,1'
                "SELECT group_concat(v, '|' ORDER BY v) FROM t WHERE grp = 'a'", // '1|2|3'
                // Concatenate one column ordered by another (names by salary v).
                "SELECT group_concat(name ORDER BY v) FROM t WHERE grp = 'a'", // 'ann,bob,cara'
                // Grouped + ordered.
                "SELECT grp, group_concat(v ORDER BY v) FROM t GROUP BY grp ORDER BY grp",
            ],
            "group_concat_ordered",
        )
        .await;
    });
}

#[test]
fn string_agg_ordered() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                "SELECT string_agg(v, '-' ORDER BY v) FROM t WHERE grp = 'a'", // '1-2-3'
            ],
            "string_agg_ordered",
        )
        .await;
    });
}
