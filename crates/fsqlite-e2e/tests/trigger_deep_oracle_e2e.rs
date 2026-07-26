//! bd-qdrpm — Oracle-parity e2e: deeper trigger semantics vs rusqlite.
//!
//! Extends trigger_semantics_oracle_e2e (AFTER audit / BEFORE INSERT / WHEN /
//! RAISE / INSTEAD OF insert / multiple) into the corners it omits: BEFORE
//! UPDATE and BEFORE DELETE with OLD/NEW, `AFTER UPDATE OF <col>` firing only
//! when that column is in the SET list, OLD/NEW reference resolution across all
//! three events, INSTEAD OF UPDATE and INSTEAD OF DELETE redirecting a view to
//! its base table, and a cross-table trigger cascade (insert into a -> b -> c).
//! Each scenario asserts per-statement agreement with rusqlite, then compares
//! the resulting table/log state. DML is autocommit (the BEGIN-CONCURRENT path).

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
fn trigger_before_update_and_delete_old_new() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE TABLE log (seq INTEGER PRIMARY KEY, event TEXT, oldv INTEGER, newv INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "CREATE TRIGGER t_bu BEFORE UPDATE ON t \
                 BEGIN INSERT INTO log(event,oldv,newv) VALUES ('upd', OLD.v, NEW.v); END",
                "CREATE TRIGGER t_bd BEFORE DELETE ON t \
                 BEGIN INSERT INTO log(event,oldv,newv) VALUES ('del', OLD.v, NULL); END",
                "UPDATE t SET v = 99 WHERE id = 1",
                "DELETE FROM t WHERE id = 2",
            ],
            &[
                "SELECT event, oldv, newv FROM log ORDER BY seq", // ('upd',10,99),('del',20,NULL)
                "SELECT id, v FROM t ORDER BY id",                // (1,99)
            ],
            "trigger_before_update_and_delete_old_new",
        )
        .await;
    });
}

#[test]
fn trigger_update_of_specific_column() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "CREATE TABLE log (seq INTEGER PRIMARY KEY, msg TEXT)",
                "INSERT INTO t VALUES (1,1,1)",
                "CREATE TRIGGER t_ua AFTER UPDATE OF a ON t \
                 BEGIN INSERT INTO log(msg) VALUES ('a changed'); END",
                "UPDATE t SET b = 100 WHERE id = 1", // a not in SET -> no fire
                "UPDATE t SET a = 5 WHERE id = 1",   // fires
                "UPDATE t SET a = 5, b = 200 WHERE id = 1", // fires (a is in SET list)
            ],
            &[
                "SELECT count(*) FROM log",           // 2
                "SELECT id, a, b FROM t ORDER BY id", // (1,5,200)
            ],
            "trigger_update_of_specific_column",
        )
        .await;
    });
}

#[test]
fn trigger_old_new_across_all_events() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "CREATE TABLE audit (seq INTEGER PRIMARY KEY, op TEXT, oldv INTEGER, newv INTEGER)",
                "CREATE TRIGGER ai AFTER INSERT ON t \
                 BEGIN INSERT INTO audit(op,oldv,newv) VALUES ('I', NULL, NEW.v); END",
                "CREATE TRIGGER au AFTER UPDATE ON t \
                 BEGIN INSERT INTO audit(op,oldv,newv) VALUES ('U', OLD.v, NEW.v); END",
                "CREATE TRIGGER ad AFTER DELETE ON t \
                 BEGIN INSERT INTO audit(op,oldv,newv) VALUES ('D', OLD.v, NULL); END",
                "INSERT INTO t VALUES (1,10)",
                "UPDATE t SET v = 20 WHERE id = 1",
                "DELETE FROM t WHERE id = 1",
            ],
            &[
                // ('I',NULL,10),('U',10,20),('D',20,NULL)
                "SELECT op, oldv, newv FROM audit ORDER BY seq",
            ],
            "trigger_old_new_across_all_events",
        )
        .await;
    });
}

/// bd-ffkpv: `CREATE TRIGGER ... INSTEAD OF {INSERT|UPDATE|DELETE} ON <view>` is
/// rejected with "no such table: <view>" — INSTEAD OF view triggers are
/// unsupported. Confirmed here for UPDATE and DELETE (the bead's existing
/// INSTEAD-OF-insert test covers INSERT).
#[test]
fn trigger_instead_of_view_update_and_delete() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
                "CREATE VIEW vt AS SELECT id, v FROM t",
                "CREATE TRIGGER vt_upd INSTEAD OF UPDATE ON vt \
                 BEGIN UPDATE t SET v = NEW.v WHERE id = OLD.id; END",
                "CREATE TRIGGER vt_del INSTEAD OF DELETE ON vt \
                 BEGIN DELETE FROM t WHERE id = OLD.id; END",
                "UPDATE vt SET v = 999 WHERE id = 2",
                "DELETE FROM vt WHERE id = 3",
            ],
            &[
                "SELECT id, v FROM t ORDER BY id",  // (1,10),(2,999)
                "SELECT id, v FROM vt ORDER BY id", // same via the view
            ],
            "trigger_instead_of_view_update_and_delete",
        )
        .await;
    });
}

#[test]
fn trigger_cross_table_cascade() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE a (id INTEGER PRIMARY KEY, x INTEGER)",
                "CREATE TABLE b (id INTEGER PRIMARY KEY, x INTEGER)",
                "CREATE TABLE c (id INTEGER PRIMARY KEY, x INTEGER)",
                "CREATE TRIGGER a_ai AFTER INSERT ON a BEGIN INSERT INTO b(x) VALUES (NEW.x * 2); END",
                "CREATE TRIGGER b_ai AFTER INSERT ON b BEGIN INSERT INTO c(x) VALUES (NEW.x + 1); END",
                "INSERT INTO a(x) VALUES (5)",
            ],
            &[
                "SELECT x FROM b", // 10
                "SELECT x FROM c", // 11 (cascade a->b->c)
            ],
            "trigger_cross_table_cascade",
        )
        .await;
    });
}
