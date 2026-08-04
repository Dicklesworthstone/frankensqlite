//! bd-cugzr — Oracle-parity e2e: DELETE / multi-row RETURNING vs rusqlite.
//!
//! returning_oracle_e2e covers INSERT and single-row UPDATE RETURNING; this adds
//! DELETE ... RETURNING (the deleted rows, via *, columns, and expressions), a
//! DELETE-all RETURNING, and multi-row UPDATE RETURNING (which must report the
//! NEW values for every affected row). RETURNING output order is unspecified in
//! SQLite, so result sets are sorted before comparison.
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

/// Run a RETURNING statement and collect its rows, sorted for order-insensitive
/// comparison.
async fn frank_returning_sorted(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect();
    out.sort();
    Ok(out)
}

fn sqlite_returning_sorted(
    conn: &rusqlite::Connection,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    let mut out = stmt
        .query_map([], |row| {
            let mut r = Vec::with_capacity(n);
            for i in 0..n {
                let v: rusqlite::types::Value = row.get_unwrap(i);
                r.push(match v {
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
            Ok(r)
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    out.sort();
    Ok(out)
}

/// Set up identical tables, run `mutation` (a RETURNING statement) on each engine
/// comparing its returned rows, then compare a follow-up state query.
async fn returning_case(setup: &[&str], mutation: &str, state_query: &str, label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in setup {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("{label} frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("{label} rusqlite `{s}`: {e}"));
    }
    let fr = frank_returning_sorted(&f, mutation).await;
    let rr = sqlite_returning_sorted(&r, mutation);
    match (&fr, &rr) {
        (Ok(a), Ok(b)) => assert_eq!(a, b, "{label}: RETURNING rows differ\n  `{mutation}`"),
        (Err(e), Ok(b)) => panic!("{label}: `{mutation}`\n  frank ERROR({e})\n  csql {b:?}"),
        (Ok(a), Err(e)) => panic!("{label}: `{mutation}`\n  frank {a:?}\n  csql ERROR({e})"),
        (Err(_), Err(_)) => {}
    }
    // Also confirm the resulting table state matches.
    let fs = frank_returning_sorted(&f, state_query).await;
    let rs = sqlite_returning_sorted(&r, state_query);
    assert_eq!(
        fs.ok(),
        rs.ok(),
        "{label}: post-mutation state differs `{state_query}`"
    );
}

const T: [&str; 2] = [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, label TEXT)",
    "INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c'),(4,40,'d')",
];

#[test]
fn delete_returning_star() {
    asupersync::test_utils::run_test(|| async {
        returning_case(
            &T,
            "DELETE FROM t WHERE v >= 30 RETURNING *", // returns deleted rows 3,4
            "SELECT id, v, label FROM t ORDER BY id",  // 1,2 remain
            "delete_returning_star",
        )
        .await;
    });
}

#[test]
fn delete_returning_columns_and_expr() {
    asupersync::test_utils::run_test(|| async {
        returning_case(
            &T,
            "DELETE FROM t WHERE id = 2 RETURNING id, v * 2 AS dbl, label", // (2,40,'b')
            "SELECT count(*) FROM t",                                       // 3
            "delete_returning_columns_and_expr",
        )
        .await;
    });
}

#[test]
fn delete_all_returning() {
    asupersync::test_utils::run_test(|| async {
        returning_case(
            &T,
            "DELETE FROM t RETURNING id", // 1,2,3,4
            "SELECT count(*) FROM t",     // 0
            "delete_all_returning",
        )
        .await;
    });
}

#[test]
fn update_returning_multi_row_new_values() {
    asupersync::test_utils::run_test(|| async {
        returning_case(
            &T,
            "UPDATE t SET v = v + 1 WHERE v >= 20 RETURNING id, v", // (2,21),(3,31),(4,41)
            "SELECT id, v FROM t ORDER BY id",
            "update_returning_multi_row_new_values",
        )
        .await;
    });
}
