//! bd-8pudk — Oracle-parity e2e: cross-storage-class ordering vs rusqlite.
//!
//! comparison_affinity_oracle tests affinity-driven *coercion* (a TEXT column
//! compared against an integer literal gets numeric affinity applied). This file
//! tests the orthogonal rule that governs comparisons when the two operands have
//! genuinely DIFFERENT storage classes and NO affinity forces a conversion:
//! SQLite ranks storage classes `NULL < {INTEGER,REAL} < TEXT < BLOB`, with
//! INTEGER and REAL compared by numeric value within the numeric class, TEXT by
//! the column collation (BINARY here), and BLOB by memcmp. A column declared with
//! no datatype has BLOB (no) affinity, so it stores each value as-inserted and
//! exposes this ranking directly in `ORDER BY`, `<`/`>`, `MIN`/`MAX`, and `WHERE`
//! boundary filters. All values are fixed and render deterministically.

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

/// Seed a no-affinity column with one value of each storage class. Insertion
/// order is deliberately scrambled so `ORDER BY` does real work.
const MIXED_SEED: &[&str] = &[
    "CREATE TABLE t (x)", // no datatype -> BLOB (no) affinity
    "INSERT INTO t VALUES ('banana')",
    "INSERT INTO t VALUES (10)",
    "INSERT INTO t VALUES (X'0102')",
    "INSERT INTO t VALUES (NULL)",
    "INSERT INTO t VALUES (2.5)",
    "INSERT INTO t VALUES ('apple')",
    "INSERT INTO t VALUES (X'00')",
    "INSERT INTO t VALUES (5)",
];

async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        let frank = frank_rows(&f, q).await;
        match (frank, sqlite_rows(&r, q)) {
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
    let mut mismatches = Vec::new();
    for q in queries {
        let frank = frank_rows(&f, q).await;
        match (frank, sqlite_rows(&r, q)) {
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

#[test]
fn sort_order_across_storage_classes() {
    // ASC: NULL first, then numbers numerically, then TEXT (binary), then BLOB.
    // DESC: exactly reversed.
    asupersync::test_utils::run_test(|| async {
        scenario(
            MIXED_SEED,
            &[
                // NULL, 2.5, 5, 10, 'apple', 'banana', X'00', X'0102'
                "SELECT x FROM t ORDER BY x",
                // X'0102', X'00', 'banana', 'apple', 10, 5, 2.5, NULL
                "SELECT x FROM t ORDER BY x DESC",
                // typeof in canonical order confirms class boundaries
                "SELECT typeof(x) FROM t ORDER BY x",
            ],
            "sort_order_across_storage_classes",
        )
        .await;
    });
}

#[test]
fn cross_class_constant_comparisons() {
    // Any number ranks below any text, any text below any blob; NULL compares
    // to NULL on any operand. INTEGER vs REAL compares numerically.
    asupersync::test_utils::run_test(|| async {
        assert_scalar(
            &[
                "SELECT 5 < 'apple'",               // number < text -> 1
                "SELECT 'apple' < X'00'",           // text < blob   -> 1
                "SELECT 100 > 'abc'",               // number > text -> 0
                "SELECT X'00' > 'zzz'",             // blob > text   -> 1
                "SELECT 2.5 < 10",                  // numeric       -> 1
                "SELECT 5 < 5.0",                   // equal numerically -> 0
                "SELECT 9223372036854775807 < 'a'", // any int < any text -> 1
                "SELECT NULL < 5",                  // NULL operand  -> NULL
                "SELECT 5 < NULL",                  // NULL operand  -> NULL
                "SELECT X'01' < X'02'",             // memcmp        -> 1
                "SELECT X'0100' > X'01'",           // longer prefix > shorter -> 1
            ],
            "cross_class_constant_comparisons",
        )
        .await;
    });
}

#[test]
fn min_max_count_across_classes() {
    // Aggregates ignore NULL; MIN/MAX honour the cross-class ranking, so MIN is
    // the smallest number and MAX is the largest blob.
    asupersync::test_utils::run_test(|| async {
        scenario(
            MIXED_SEED,
            &[
                "SELECT min(x) FROM t",            // 2.5 (smallest non-null number)
                "SELECT max(x) FROM t",            // X'0102' (largest blob)
                "SELECT count(x) FROM t",          // 7 (NULL excluded)
                "SELECT count(*) FROM t",          // 8
                "SELECT count(DISTINCT x) FROM t", // 7
                "SELECT typeof(min(x)), typeof(max(x)) FROM t", // real | blob
            ],
            "min_max_count_across_classes",
        )
        .await;
    });
}

#[test]
fn where_filter_crosses_class_boundary() {
    // A boundary literal selects everything that ranks above/below it across
    // class lines, not just same-class values.
    asupersync::test_utils::run_test(|| async {
        scenario(
            MIXED_SEED,
            &[
                // > 5: 10 (number), both texts, both blobs (all rank above an int)
                "SELECT x FROM t WHERE x > 5 ORDER BY x",
                // < 'apple': all three numbers (numbers rank below text); no text/blob
                "SELECT x FROM t WHERE x < 'apple' ORDER BY x",
                // > X'00': only the larger blob (everything else ranks below blobs)
                "SELECT x FROM t WHERE x > X'00' ORDER BY x",
                // class predicate via typeof
                "SELECT x FROM t WHERE typeof(x) = 'integer' ORDER BY x", // 5, 10
                "SELECT x FROM t WHERE x IS NULL",                        // NULL
            ],
            "where_filter_crosses_class_boundary",
        )
        .await;
    });
}
