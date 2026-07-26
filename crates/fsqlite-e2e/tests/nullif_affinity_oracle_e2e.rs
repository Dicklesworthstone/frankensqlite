//! bd-3ewai — Oracle-parity e2e: NULLIF comparison affinity vs rusqlite.
//!
//! `nullif(x, y)` returns NULL when `x = y` (else x), and that equality applies
//! affinity exactly like a plain `=`: `nullif(intcol, '5')` coerces '5' to 5, so
//! a row with n=5 yields NULL. This checks whether NULLIF goes through the
//! affinity-applying `=` path (bd-525y0, works) or skips it like the IN-list /
//! simple-CASE emits (bd-56aj2 / bd-w4r25). Compared against rusqlite, with
//! same-type and int-vs-real controls.

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
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, s TEXT)",
        "INSERT INTO t VALUES (1,5,'5'),(2,10,'x')",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn nullif_same_type_and_numeric_controls() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT nullif(5, 5)",    // NULL
                "SELECT nullif(5, 6)",    // 5
                "SELECT nullif(5, 5.0)",  // NULL (numeric int vs real)
                "SELECT nullif('a','a')", // NULL
                "SELECT nullif(NULL, 1)", // NULL
                "SELECT nullif('5', 5)",  // bare literals, no affinity -> not equal -> '5'
            ],
            "nullif_same_type_and_numeric_controls",
        )
        .await;
    });
}

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&f, &r, queries, label).await;
}

#[test]
fn nullif_integer_column_vs_text_numeric() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // n has INTEGER affinity: nullif(5,'5') -> coerces '5'->5 -> equal -> NULL.
                "SELECT id, nullif(n, '5') FROM t ORDER BY id", // (1,NULL),(2,10)
            ],
            "nullif_integer_column_vs_text_numeric",
        )
        .await;
    });
}

#[test]
fn nullif_text_column_vs_numeric() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // s has TEXT affinity: nullif(s,5) -> coerces 5->'5' -> id1 (s='5') -> NULL.
                "SELECT id, nullif(s, 5) FROM t ORDER BY id", // (1,NULL),(2,'x')
            ],
            "nullif_text_column_vs_numeric",
        )
        .await;
    });
}
