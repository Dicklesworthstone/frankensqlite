//! bd-kyvrd — Oracle-parity e2e: BLOB I/O & semantics vs rusqlite (real SQLite).
//!
//! Covers blob literals `X'..'`, zeroblob(n), storage round-trip, hex/length/
//! substr/quote on blobs, the storage-class sort order (blobs sort after text),
//! byte-wise blob comparison, blobs in WHERE / DISTINCT / GROUP BY, CAST to/from
//! blob, the empty blob, and `||` concatenation involving blobs. All inputs are
//! fixed and deterministic; outputs render as X'..' hex on both sides.
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

async fn assert_scalar(queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    check(&f, &r, queries, label).await;
}

#[test]
fn blob_literals_and_functions() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT X'48656C6C6F'",               // 'Hello' bytes
                "SELECT typeof(X'00FF')",             // 'blob'
                "SELECT length(X'00010203')",         // 4
                "SELECT hex(X'DEADBEEF')",            // 'DEADBEEF'
                "SELECT quote(X'4142')",              // X'4142'
                "SELECT X''",                         // empty blob
                "SELECT length(X'')",                 // 0
                "SELECT substr(X'0102030405', 2, 2)", // X'0203'
                "SELECT hex(zeroblob(3))",            // '000000'
                "SELECT length(zeroblob(5))",         // 5
            ],
            "blob_literals_and_functions",
        )
        .await;
    });
}

#[test]
fn blob_storage_roundtrip() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)",
            "INSERT INTO t VALUES (1, X'01020304'),(2, X''),(3, X'FF'),(4, NULL)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT id, data, typeof(data), length(data) FROM t ORDER BY id",
                "SELECT id FROM t WHERE data = X'FF'",
                "SELECT id FROM t WHERE data IS NULL",
                "SELECT hex(data) FROM t WHERE id = 1",
            ],
            "blob_storage_roundtrip",
        )
        .await;
    });
}

#[test]
fn blob_ordering_storage_class() {
    // Storage-class order: NULL < numbers < text < blob. Among blobs, memcmp.
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v)",
            "INSERT INTO t(v) VALUES (NULL),(42),('text'),(X'00'),(X'FF'),(X'0102'),(3.5)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT typeof(v), v FROM t ORDER BY v, id",
                "SELECT typeof(v) FROM t ORDER BY v DESC, id",
                // Blob byte-wise comparison.
                "SELECT X'01' < X'02'",
                "SELECT X'0102' < X'02'",     // first byte 01 < 02
                "SELECT X'0102' < X'010203'", // prefix < longer
                "SELECT X'41' = X'41'",
            ],
            "blob_ordering_storage_class",
        )
        .await;
    });
}

#[test]
fn blob_distinct_and_group_by() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, b BLOB)",
            "INSERT INTO t VALUES (1,X'01'),(2,X'01'),(3,X'02'),(4,X'02'),(5,X'03')",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT DISTINCT b FROM t ORDER BY b",
                "SELECT count(DISTINCT b) FROM t",
                "SELECT b, count(*) FROM t GROUP BY b ORDER BY b",
            ],
            "blob_distinct_and_group_by",
        )
        .await;
    });
}

#[test]
fn blob_cast() {
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                // CAST text to blob keeps the bytes; hex shows them.
                "SELECT hex(CAST('AB' AS BLOB))",   // '4142'
                "SELECT typeof(CAST('x' AS BLOB))", // 'blob'
                // CAST a blob to text interprets bytes as the string.
                "SELECT CAST(X'414243' AS TEXT)", // 'ABC'
                // CAST a blob of digits to integer (via text).
                "SELECT CAST(X'313233' AS INTEGER)", // 123
                "SELECT CAST(X'312E35' AS REAL)",    // 1.5
                // CAST integer to blob.
                "SELECT typeof(CAST(65 AS BLOB)), hex(CAST(65 AS BLOB))",
            ],
            "blob_cast",
        )
        .await;
    });
}

#[test]
fn blob_concatenation() {
    // `||` semantics with blob operands (let the oracle define the result).
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT typeof(X'41' || X'42'), hex(CAST(X'41' || X'42' AS BLOB))",
                "SELECT X'41' || X'42'",
                "SELECT 'pre' || X'4142'",
                "SELECT typeof(X'00' || 'x')",
            ],
            "blob_concatenation",
        )
        .await;
    });
}
