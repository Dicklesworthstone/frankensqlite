//! bd-525y0 — Oracle-parity e2e: comparison affinity vs rusqlite.
//!
//! When SQLite compares two operands it applies affinity per its rules:
//!  - a column with INTEGER/REAL/NUMERIC affinity vs a literal applies that
//!    affinity to the literal (`intcol = '5'` coerces '5'->5);
//!  - a column with TEXT affinity applies TEXT to the other side (`textcol = 5`
//!    coerces 5->'5');
//!  - two bare literals get NO affinity (`'5' = 5` is false; storage classes
//!    differ);
//!  - column-vs-column: if one side is numeric-affinity and the other TEXT/NONE,
//!    NUMERIC is applied to the other;
//!  - cross-storage-class comparisons order NULL < numbers < text < blob.
//! These verify all of that against rusqlite. (Distinct from the persistence
//! reopen test in affinity_persistence_oracle_e2e — this is pure WHERE/compare.)
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

async fn data() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, s TEXT)",
        "INSERT INTO t VALUES (1,5,'5'),(2,10,'10'),(3,42,'x')",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn cmp_integer_column_vs_text_numeric_literal() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // INTEGER affinity applied to the text literal: '5'->5.
                "SELECT id FROM t WHERE n = '5' ORDER BY id", // 1
                "SELECT id FROM t WHERE n > '9' ORDER BY id", // 2,3
                "SELECT id FROM t WHERE n < '11' ORDER BY id", // 1,2
            ],
            "cmp_integer_column_vs_text_numeric_literal",
        )
        .await;
    });
}

#[test]
fn cmp_text_column_vs_numeric_literal() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // TEXT affinity applied to the integer literal: 5->'5'.
                "SELECT id FROM t WHERE s = 5 ORDER BY id", // 1
                "SELECT id FROM t WHERE s = 10 ORDER BY id", // 2
            ],
            "cmp_text_column_vs_numeric_literal",
        )
        .await;
    });
}

#[test]
fn cmp_bare_literals_get_no_affinity() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT '5' = 5",     // 0 (text vs int, no affinity)
                "SELECT 5 = 5.0",     // 1 (numeric)
                "SELECT '5' = '5'",   // 1
                "SELECT 5 = '5.0'",   // 0 (no affinity)
                "SELECT '5' = '5.0'", // 0 (distinct text)
            ],
            "cmp_bare_literals_get_no_affinity",
        )
        .await;
    });
}

#[test]
fn cmp_storage_class_ordering() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // NULL < numbers < text < blob.
                "SELECT 1 < 'a', 'a' < X'00'",  // 1, 1
                "SELECT 1 < X'00', 100 < 'a'",  // 1, 1
                "SELECT NULL < 1, NULL = NULL", // NULL, NULL
                // Numeric vs lexical ordering contrast.
                "SELECT 2 < 10, '2' < '10'", // 1, 0 (lexical: '2' > '1')
            ],
            "cmp_storage_class_ordering",
        )
        .await;
    });
}

#[test]
fn cmp_column_vs_column_numeric_applied() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = data().await;
        check(
            &f,
            &r,
            &[
                // INTEGER col = TEXT col -> NUMERIC applied to the text side.
                // id1: 5 = '5'->5 match; id2: 10='10'->10 match; id3: 42='x' (stays text) no.
                "SELECT id FROM t WHERE n = s ORDER BY id", // 1,2
            ],
            "cmp_column_vs_column_numeric_applied",
        )
        .await;
    });
}

#[test]
fn cmp_mixed_storage_class_order_by() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE m (id INTEGER PRIMARY KEY, v)",
            "INSERT INTO m VALUES (1,'text'),(2,42),(3,NULL),(4,3.5),(5,X'AB'),(6,'apple')",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        check(
            &f,
            &r,
            &[
                // ORDER BY a no-affinity column sorts by storage class then value.
                "SELECT id, typeof(v) FROM m ORDER BY v, id",
            ],
            "cmp_mixed_storage_class_order_by",
        )
        .await;
    });
}
