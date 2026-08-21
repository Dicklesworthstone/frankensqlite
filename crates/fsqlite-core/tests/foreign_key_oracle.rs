//! Differential oracle: foreign-key enforcement + referential actions vs
//! rusqlite (bundled SQLite 3.53), with `PRAGMA foreign_keys=ON`. A probe sweep
//! found this surface stock-correct across 10 cases; this keeper locks it in.
//!
//! Error-agnostic strategy: run DML where some statements violate a constraint
//! (rejected by both engines; the driver ignores the per-statement error) then
//! SELECT the surviving state. Covers ON DELETE CASCADE/SET NULL/RESTRICT, ON
//! UPDATE CASCADE, insert-time violation, NULL child (never constrained),
//! composite FK, self-referential FK, a multi-level cascade chain, and a
//! DEFERRABLE INITIALLY DEFERRED FK satisfied before COMMIT.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// FK enforcement is OFF by default on both engines — enable it, then run setup.
async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let _ = f.execute("PRAGMA foreign_keys=ON").await;
    let _ = r.execute_batch("PRAGMA foreign_keys=ON");
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(fr, rr, "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

#[test]
fn on_delete_actions() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE p(id INT PRIMARY KEY)",
              "CREATE TABLE c(id INT, pid INT REFERENCES p(id) ON DELETE CASCADE)",
              "INSERT INTO p VALUES (1),(2)",
              "INSERT INTO c VALUES (10,1),(11,1),(12,2)",
              "DELETE FROM p WHERE id = 1"],
            "SELECT id, pid FROM c ORDER BY id",
            "ON DELETE CASCADE removes children",
        ).await;
        agree(
            &["CREATE TABLE p(id INT PRIMARY KEY)",
              "CREATE TABLE c(id INT, pid INT REFERENCES p(id) ON DELETE SET NULL)",
              "INSERT INTO p VALUES (1)",
              "INSERT INTO c VALUES (10,1),(11,1)",
              "DELETE FROM p WHERE id = 1"],
            "SELECT id, pid FROM c ORDER BY id",
            "ON DELETE SET NULL nulls the child key",
        ).await;
        agree(
            &["CREATE TABLE p(id INT PRIMARY KEY)",
              "CREATE TABLE c(id INT, pid INT REFERENCES p(id) ON DELETE RESTRICT)",
              "INSERT INTO p VALUES (1),(2)",
              "INSERT INTO c VALUES (10,1)",
              "DELETE FROM p WHERE id = 1",
              "DELETE FROM p WHERE id = 2"],
            "SELECT id FROM p ORDER BY id",
            "ON DELETE RESTRICT blocks a referenced parent, allows an unreferenced one",
        ).await;
    });
}

#[test]
fn on_update_cascade() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE p(id INT PRIMARY KEY)",
              "CREATE TABLE c(id INT, pid INT REFERENCES p(id) ON UPDATE CASCADE)",
              "INSERT INTO p VALUES (1)",
              "INSERT INTO c VALUES (10,1),(11,1)",
              "UPDATE p SET id = 99 WHERE id = 1"],
            "SELECT id, pid FROM c ORDER BY id",
            "ON UPDATE CASCADE propagates the new key",
        ).await;
    });
}

#[test]
fn insert_violation_and_null_child() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE p(id INT PRIMARY KEY)",
              "CREATE TABLE c(id INT, pid INT REFERENCES p(id))",
              "INSERT INTO p VALUES (1)",
              "INSERT INTO c VALUES (10,1)",
              "INSERT INTO c VALUES (11,999)"],
            "SELECT id, pid FROM c ORDER BY id",
            "insert referencing a missing parent is rejected",
        ).await;
        agree(
            &["CREATE TABLE p(id INT PRIMARY KEY)",
              "CREATE TABLE c(id INT, pid INT REFERENCES p(id))",
              "INSERT INTO p VALUES (1)",
              "INSERT INTO c VALUES (10,NULL),(11,1)"],
            "SELECT id, pid FROM c ORDER BY id",
            "a NULL child key is never constrained",
        ).await;
    });
}

#[test]
fn composite_and_self_referential() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE p(a INT, b INT, PRIMARY KEY(a,b))",
              "CREATE TABLE c(x INT, a INT, b INT, FOREIGN KEY(a,b) REFERENCES p(a,b))",
              "INSERT INTO p VALUES (1,2),(3,4)",
              "INSERT INTO c VALUES (10,1,2)",
              "INSERT INTO c VALUES (11,1,9)"],
            "SELECT x,a,b FROM c ORDER BY x",
            "composite FK requires the full key to match",
        ).await;
        agree(
            &["CREATE TABLE emp(id INT PRIMARY KEY, mgr INT REFERENCES emp(id) ON DELETE SET NULL)",
              "INSERT INTO emp VALUES (1,NULL),(2,1),(3,1)",
              "DELETE FROM emp WHERE id = 1"],
            "SELECT id, mgr FROM emp ORDER BY id",
            "self-referential FK with ON DELETE SET NULL",
        ).await;
    });
}

#[test]
fn cascade_chain_and_deferred() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE gp(id INT PRIMARY KEY)",
              "CREATE TABLE p(id INT PRIMARY KEY, gpid INT REFERENCES gp(id) ON DELETE CASCADE)",
              "CREATE TABLE c(id INT, pid INT REFERENCES p(id) ON DELETE CASCADE)",
              "INSERT INTO gp VALUES (1)",
              "INSERT INTO p VALUES (10,1)",
              "INSERT INTO c VALUES (100,10),(101,10)",
              "DELETE FROM gp WHERE id = 1"],
            "SELECT (SELECT count(*) FROM gp), (SELECT count(*) FROM p), (SELECT count(*) FROM c)",
            "multi-level cascade empties the whole chain",
        ).await;
        agree(
            &["CREATE TABLE p(id INT PRIMARY KEY)",
              "CREATE TABLE c(id INT, pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
              "BEGIN",
              "INSERT INTO c VALUES (10,1)",
              "INSERT INTO p VALUES (1)",
              "COMMIT"],
            "SELECT id, pid FROM c ORDER BY id",
            "DEFERRABLE INITIALLY DEFERRED tolerates a transient dangling ref until COMMIT",
        ).await;
    });
}
