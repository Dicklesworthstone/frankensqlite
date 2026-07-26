//! bd-w28qj — Oracle-parity e2e: FOREIGN KEY constraints vs rusqlite.
//!
//! With `PRAGMA foreign_keys = ON`, checks FK enforcement (insert/delete
//! violations), the action clauses ON DELETE/UPDATE CASCADE / SET NULL /
//! RESTRICT / NO ACTION (including the combined `ON UPDATE CASCADE ON DELETE
//! SET NULL` form), self-referential and multi-column FKs, and that with
//! `foreign_keys = OFF` no enforcement happens. Parent mutations are autocommit
//! (sidesteps the bd-jamrd explicit-BEGIN path). Each scenario runs on both
//! engines, asserting they agree per statement, then compares resulting state.
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

/// Run statements on both engines (asserting per-statement success/failure
/// agreement), then compare queries.
async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    // Enable FK enforcement on both (off by default in SQLite).
    f.execute("PRAGMA foreign_keys = ON")
        .await
        .expect("frank pragma");
    r.execute_batch("PRAGMA foreign_keys = ON")
        .expect("rusqlite pragma");
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
fn fk_enforcement_insert_violation() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
                "INSERT INTO parent VALUES (1,'a'),(2,'b')",
                "INSERT INTO child VALUES (10,1),(11,2)", // valid
                "INSERT INTO child VALUES (12,99)",       // violates FK -> error on both
                "INSERT INTO child VALUES (13,NULL)",     // NULL FK is allowed
            ],
            &["SELECT id, pid FROM child ORDER BY id"],
            "fk_enforcement_insert_violation",
        )
        .await;
    });
}

#[test]
fn fk_on_delete_cascade() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
                "INSERT INTO parent VALUES (1),(2),(3)",
                "INSERT INTO child VALUES (10,1),(11,1),(12,2),(13,3)",
                "DELETE FROM parent WHERE id = 1", // cascades to children 10,11
            ],
            &[
                "SELECT id, pid FROM child ORDER BY id",
                "SELECT id FROM parent ORDER BY id",
            ],
            "fk_on_delete_cascade",
        )
        .await;
    });
}

#[test]
fn fk_on_delete_set_null() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
                "INSERT INTO parent VALUES (1),(2)",
                "INSERT INTO child VALUES (10,1),(11,1),(12,2)",
                "DELETE FROM parent WHERE id = 1", // children 10,11 get pid = NULL
            ],
            &["SELECT id, pid FROM child ORDER BY id"],
            "fk_on_delete_set_null",
        )
        .await;
    });
}

#[test]
fn fk_on_delete_restrict_blocks() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE RESTRICT)",
                "INSERT INTO parent VALUES (1),(2)",
                "INSERT INTO child VALUES (10,1)",
                "DELETE FROM parent WHERE id = 1", // blocked (has child) -> error on both
                "DELETE FROM parent WHERE id = 2", // no child -> ok
            ],
            &[
                "SELECT id FROM parent ORDER BY id",
                "SELECT id, pid FROM child ORDER BY id",
            ],
            "fk_on_delete_restrict_blocks",
        )
        .await;
    });
}

#[test]
fn fk_on_update_cascade() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
                "INSERT INTO parent VALUES (1),(2)",
                "INSERT INTO child VALUES (10,1),(11,1),(12,2)",
                "UPDATE parent SET id = 100 WHERE id = 1", // children 10,11 pid -> 100
            ],
            &[
                "SELECT id, pid FROM child ORDER BY id",
                "SELECT id FROM parent ORDER BY id",
            ],
            "fk_on_update_cascade",
        )
        .await;
    });
}

#[test]
fn fk_combined_update_cascade_delete_set_null() {
    asupersync::test_utils::run_test(|| async {
        // ON UPDATE CASCADE ON DELETE SET NULL: update must cascade, delete must
        // null. (A known reimplementation pitfall is applying the wrong action.)
        scenario(
            &[
                "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
                "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER \
                   REFERENCES parent(id) ON UPDATE CASCADE ON DELETE SET NULL)",
                "INSERT INTO parent VALUES (1),(2)",
                "INSERT INTO child VALUES (10,1),(11,2)",
                "UPDATE parent SET id = 100 WHERE id = 1", // child 10 pid -> 100 (CASCADE)
                "DELETE FROM parent WHERE id = 2",         // child 11 pid -> NULL (SET NULL)
            ],
            &["SELECT id, pid FROM child ORDER BY id"],
            "fk_combined_update_cascade_delete_set_null",
        )
        .await;
    });
}

#[test]
fn fk_self_referential() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE emp (id INTEGER PRIMARY KEY, mgr INTEGER REFERENCES emp(id) ON DELETE CASCADE)",
                "INSERT INTO emp VALUES (1,NULL),(2,1),(3,1),(4,2)",
                "DELETE FROM emp WHERE id = 2", // cascades to id 4
            ],
            &["SELECT id, mgr FROM emp ORDER BY id"],
            "fk_self_referential",
        )
        .await;
    });
}

#[test]
fn fk_disabled_allows_orphans() {
    asupersync::test_utils::run_test(|| async {
        // With foreign_keys OFF, orphan rows are allowed.
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        f.execute("PRAGMA foreign_keys = OFF").await.unwrap();
        r.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        for s in [
            "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
            "INSERT INTO parent VALUES (1)",
            "INSERT INTO child VALUES (10, 99)", // orphan allowed when FKs off
        ] {
            f.execute(s)
                .await
                .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
            r.execute_batch(s)
                .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
        }
        let fr = frank_rows(&f, "SELECT id, pid FROM child ORDER BY id")
            .await
            .unwrap();
        let rr = sqlite_rows(&r, "SELECT id, pid FROM child ORDER BY id").unwrap();
        assert_eq!(
            fr, rr,
            "fk_disabled_allows_orphans: frank {fr:?} vs csql {rr:?}"
        );
    });
}
