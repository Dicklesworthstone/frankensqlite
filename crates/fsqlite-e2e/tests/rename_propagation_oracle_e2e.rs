//! bd-pyh85 — Oracle-parity e2e: ALTER TABLE RENAME propagation vs rusqlite.
//!
//! Since SQLite 3.25, RENAME TABLE / RENAME COLUMN rewrites references in
//! dependent schema objects: foreign keys, indexes, views, triggers, and CHECK
//! constraints. These verify (against rusqlite) that the dependents keep working
//! after a rename — proving the references were rewritten, not just that the
//! rename itself succeeded. DML is autocommit; scenarios assert per-statement
//! agreement then compare query results.

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
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
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
fn rename_table_basic() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE old_t (id INTEGER PRIMARY KEY, v TEXT)",
                "INSERT INTO old_t VALUES (1,'a'),(2,'b')",
                "ALTER TABLE old_t RENAME TO new_t",
                "INSERT INTO new_t VALUES (3,'c')",
            ],
            &[
                "SELECT id, v FROM new_t ORDER BY id",
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='old_t'",
                "SELECT name FROM sqlite_master WHERE type='table' AND name='new_t'",
            ],
            "rename_table_basic",
        )
        .await;
    });
}

#[test]
fn rename_column_basic_and_index() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT, qty INTEGER)",
                "CREATE INDEX idx_old ON t(old_name)",
                "INSERT INTO t VALUES (1,'a',10),(2,'b',20)",
                "ALTER TABLE t RENAME COLUMN old_name TO new_name",
            ],
            &[
                "SELECT id, new_name, qty FROM t ORDER BY id",
                // The index over the renamed column must still resolve.
                "PRAGMA index_info(idx_old)",
                "SELECT id FROM t WHERE new_name = 'b'",
            ],
            "rename_column_basic_and_index",
        )
        .await;
    });
}

#[test]
fn rename_table_rewrites_fk_reference() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
                "INSERT INTO parent VALUES (1),(2)",
                "INSERT INTO child VALUES (10,1)",
                // Rename the parent: the child's FK reference must be rewritten.
                "ALTER TABLE parent RENAME TO parent2",
                "INSERT INTO child VALUES (11,2)", // still valid against parent2
                "INSERT INTO child VALUES (12,99)", // still enforced -> violation error
            ],
            &[
                "SELECT id, pid FROM child ORDER BY id",
                "SELECT count(*) FROM parent2",
            ],
            "rename_table_rewrites_fk_reference",
        )
        .await;
    });
}

#[test]
fn rename_column_rewrites_view() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)",
                "CREATE VIEW v AS SELECT id, amount FROM t WHERE amount > 5",
                "INSERT INTO t VALUES (1,10),(2,3),(3,20)",
                // Renaming a column referenced by a view must rewrite the view body.
                "ALTER TABLE t RENAME COLUMN amount TO amt",
            ],
            &[
                "SELECT id, amount FROM v ORDER BY id", // view still exposes 'amount' alias
                "SELECT id, amt FROM t ORDER BY id",
            ],
            "rename_column_rewrites_view",
        )
        .await;
    });
}

#[test]
fn rename_column_rewrites_trigger() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)",
                "CREATE TABLE log (v INTEGER)",
                "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log(v) VALUES (NEW.val); END",
                // Rename the column the trigger references via NEW.val.
                "ALTER TABLE t RENAME COLUMN val TO value",
                "INSERT INTO t VALUES (1, 42),(2, 7)",
            ],
            &["SELECT v FROM log ORDER BY v"], // trigger still logs the renamed column's value
            "rename_column_rewrites_trigger",
        )
        .await;
    });
}

#[test]
fn rename_column_rewrites_check() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER CHECK (qty >= 0))",
                "INSERT INTO t VALUES (1, 5)",
                "ALTER TABLE t RENAME COLUMN qty TO quantity",
                "INSERT INTO t VALUES (2, 10)", // ok
                "INSERT INTO t VALUES (3, -1)", // CHECK (now on 'quantity') still enforced -> error
            ],
            &["SELECT id, quantity FROM t ORDER BY id"], // 1 and 2
            "rename_column_rewrites_check",
        )
        .await;
    });
}
