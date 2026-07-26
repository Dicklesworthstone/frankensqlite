//! bd-dao4z — Oracle-parity e2e: declared-type-name -> column affinity.
//!
//! SQLite derives a column's affinity from its declared type name via five rules
//! applied in order: (1) contains "INT" -> INTEGER; (2) contains "CHAR"/"CLOB"/
//! "TEXT" -> TEXT; (3) contains "BLOB" or no type -> BLOB; (4) contains "REAL"/
//! "FLOA"/"DOUB" -> REAL; (5) otherwise -> NUMERIC. The "otherwise" bucket is the
//! easy one to mismap (STRING, BOOLEAN, DATE, DATETIME all -> NUMERIC, not TEXT/
//! INTEGER). This probes each class by inserting an integer and a text value and
//! reading `typeof` of the stored result (which reveals the applied affinity),
//! comparing against rusqlite.

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

/// Build a table whose columns have the given declared types, insert one row of
/// integer 42 and one of text '42', and compare typeof() of each column on each
/// row. The (int-probe typeof, text-probe typeof) pair reveals the affinity.
async fn affinity_probe(types: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let cols: Vec<String> = types
        .iter()
        .enumerate()
        .map(|(i, ty)| format!("c{i} {ty}"))
        .collect();
    let create = format!(
        "CREATE TABLE t (rid INTEGER PRIMARY KEY, {})",
        cols.join(", ")
    );
    let names: Vec<String> = (0..types.len()).map(|i| format!("c{i}")).collect();
    let int_vals = vec!["42"; types.len()].join(",");
    let txt_vals = vec!["'42'"; types.len()].join(",");
    for s in [
        create.as_str(),
        &format!("INSERT INTO t VALUES (1, {int_vals})"),
        &format!("INSERT INTO t VALUES (2, {txt_vals})"),
    ] {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    let typeofs: Vec<String> = names.iter().map(|n| format!("typeof({n})")).collect();
    let q = format!("SELECT rid, {} FROM t ORDER BY rid", typeofs.join(", "));
    check(&f, &r, &[&q], label).await;
}

#[test]
fn affinity_integer_class() {
    asupersync::test_utils::run_test(|| async {
        // Anything containing "INT" -> INTEGER affinity.
        affinity_probe(
            &[
                "INTEGER",
                "INT",
                "BIGINT",
                "INT2",
                "INT8",
                "TINYINT",
                "MEDIUMINT",
                "UNSIGNED BIG INT",
            ],
            "affinity_integer_class",
        )
        .await;
    });
}

#[test]
fn affinity_text_class() {
    asupersync::test_utils::run_test(|| async {
        // Contains "CHAR"/"CLOB"/"TEXT" -> TEXT affinity.
        affinity_probe(
            &[
                "TEXT",
                "CLOB",
                "CHARACTER(20)",
                "VARCHAR(255)",
                "NCHAR(10)",
                "NVARCHAR(8)",
                "VARYING CHARACTER(5)",
            ],
            "affinity_text_class",
        )
        .await;
    });
}

#[test]
fn affinity_real_class() {
    asupersync::test_utils::run_test(|| async {
        // Contains "REAL"/"FLOA"/"DOUB" -> REAL affinity.
        affinity_probe(
            &["REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT"],
            "affinity_real_class",
        )
        .await;
    });
}

#[test]
fn affinity_blob_class() {
    asupersync::test_utils::run_test(|| async {
        // Contains "BLOB" or no declared type -> BLOB affinity (no coercion).
        affinity_probe(&["BLOB", ""], "affinity_blob_class").await;
    });
}

#[test]
fn affinity_numeric_class() {
    asupersync::test_utils::run_test(|| async {
        // The "otherwise" bucket -> NUMERIC. STRING/BOOLEAN/DATE/DATETIME are the
        // ones most likely to be mismapped to TEXT/INTEGER.
        affinity_probe(
            &[
                "NUMERIC",
                "DECIMAL(10,5)",
                "BOOLEAN",
                "DATE",
                "DATETIME",
                "STRING",
            ],
            "affinity_numeric_class",
        )
        .await;
    });
}
