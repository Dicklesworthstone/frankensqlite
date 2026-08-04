//! bd-uelpe — Oracle-parity e2e: foreign-key action matrix vs rusqlite.
//!
//! foreign_key_oracle covers CASCADE / SET NULL / RESTRICT / self-referential;
//! this fills the remaining action matrix: `ON DELETE SET DEFAULT` (child takes
//! its column DEFAULT), `ON UPDATE SET NULL`, `ON UPDATE RESTRICT` (blocks a
//! parent-key change while children exist), a composite multi-column FK with
//! CASCADE, and a three-level cascade chain (deleting the root cascades through
//! two FK levels). All DML is autocommit; each scenario asserts per-statement
//! agreement with rusqlite, then compares the resulting rows.
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

/// bd-a4ki6: ON DELETE SET DEFAULT is parsed but not applied — frank lumps
/// SetDefault with NoAction|Restrict (connection.rs:38695), so the parent delete
/// is rejected with "FOREIGN KEY constraint failed" instead of setting the
/// child's FK column to its DEFAULT.
#[test]
fn fk_on_delete_set_default() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 99 REFERENCES parent(id) ON DELETE SET DEFAULT)",
                "INSERT INTO parent VALUES (1),(2),(99)",
                "INSERT INTO child VALUES (10,1),(11,2)",
                "DELETE FROM parent WHERE id = 1",
            ],
            &["SELECT id, pid FROM child ORDER BY id"], // (10,99),(11,2)
            "fk_on_delete_set_default",
        )
        .await;
    });
}

#[test]
fn fk_on_update_set_null() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE SET NULL)",
                "INSERT INTO parent VALUES (1),(2)",
                "INSERT INTO child VALUES (10,1),(11,2)",
                "UPDATE parent SET id = 5 WHERE id = 1",
            ],
            &["SELECT id, pid FROM child ORDER BY id"], // (10,NULL),(11,2)
            "fk_on_update_set_null",
        )
        .await;
    });
}

#[test]
fn fk_on_update_restrict_blocks() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE RESTRICT)",
                "INSERT INTO parent VALUES (1),(2)",
                "INSERT INTO child VALUES (10,1)",
                "UPDATE parent SET id = 5 WHERE id = 1", // blocked (child refs it) -> error both
                "UPDATE parent SET id = 7 WHERE id = 2", // ok (no child)
            ],
            &[
                "SELECT id FROM parent ORDER BY id",     // (1,7)
                "SELECT id, pid FROM child ORDER BY id", // (10,1)
            ],
            "fk_on_update_restrict_blocks",
        )
        .await;
    });
}

#[test]
fn fk_composite_multicolumn_cascade() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE parent (a INTEGER, b INTEGER, PRIMARY KEY(a,b))",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pa INTEGER, pb INTEGER, \
                 FOREIGN KEY(pa,pb) REFERENCES parent(a,b) ON DELETE CASCADE)",
                "INSERT INTO parent VALUES (1,1),(1,2),(2,1)",
                "INSERT INTO child VALUES (10,1,1),(11,1,2),(12,2,1)",
                "DELETE FROM parent WHERE a=1 AND b=1",
            ],
            &["SELECT id, pa, pb FROM child ORDER BY id"], // (11,1,2),(12,2,1)
            "fk_composite_multicolumn_cascade",
        )
        .await;
    });
}

#[test]
fn fk_three_level_cascade_chain() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE a (id INTEGER PRIMARY KEY)",
                "CREATE TABLE b (id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id) ON DELETE CASCADE)",
                "CREATE TABLE c (id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id) ON DELETE CASCADE)",
                "INSERT INTO a VALUES (1),(2)",
                "INSERT INTO b VALUES (10,1),(11,2)",
                "INSERT INTO c VALUES (100,10),(101,11)",
                "DELETE FROM a WHERE id = 1",
            ],
            &[
                "SELECT id FROM b ORDER BY id", // (11)  -- b10 cascaded
                "SELECT id FROM c ORDER BY id", // (101) -- c100 cascaded via b10
            ],
            "fk_three_level_cascade_chain",
        )
        .await;
    });
}

/// bd-a4ki6: ON UPDATE SET DEFAULT fails identically to the ON DELETE variant.
#[test]
fn fk_on_update_set_default() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 99 REFERENCES parent(id) ON UPDATE SET DEFAULT)",
                "INSERT INTO parent VALUES (1),(2),(99)",
                "INSERT INTO child VALUES (10,1)",
                "UPDATE parent SET id = 5 WHERE id = 1",
            ],
            &["SELECT id, pid FROM child ORDER BY id"], // expect (10,99)
            "fk_on_update_set_default",
        )
        .await;
    });
}
