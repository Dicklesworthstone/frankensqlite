//! bd-w3emn — Oracle-parity e2e: recursive_triggers pragma vs rusqlite.
//!
//! By default `PRAGMA recursive_triggers` is OFF: a trigger whose body modifies
//! its own table does NOT re-invoke itself, so a self-referential cascade goes
//! exactly one level deep. With `recursive_triggers = ON` the trigger re-fires
//! and the cascade runs to completion. The discriminator: an
//! `AFTER DELETE ON t BEGIN DELETE FROM t WHERE parent = OLD.id; END` over a tree
//! — OFF deletes only the direct children of the root; ON deletes the whole
//! subtree. These verify both modes plus the pragma get/set, against rusqlite.

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

// Tree: 1 is root; 2 and 4 are children of 1; 3 is child of 2.
const TREE: [&str; 3] = [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, parent INTEGER)",
    "CREATE TRIGGER del_kids AFTER DELETE ON t BEGIN DELETE FROM t WHERE parent = OLD.id; END",
    "INSERT INTO t VALUES (1,NULL),(2,1),(3,2),(4,1)",
];

#[test]
fn recursive_triggers_default_off_one_level() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &{
                let mut v = vec!["PRAGMA recursive_triggers = OFF"];
                v.extend_from_slice(&TREE);
                v.push("DELETE FROM t WHERE id = 1");
                v
            },
            // OFF: deleting 1 fires the trigger once -> removes direct children 2,4;
            // those deletes do NOT re-fire, so 3 (child of 2) survives.
            &["SELECT id FROM t ORDER BY id"], // [3]
            "recursive_triggers_default_off_one_level",
        )
        .await;
    });
}

#[test]
fn recursive_triggers_on_full_cascade() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &{
                let mut v = vec!["PRAGMA recursive_triggers = ON"];
                v.extend_from_slice(&TREE);
                v.push("DELETE FROM t WHERE id = 1");
                v
            },
            // ON: the cascade re-fires through the whole subtree -> all gone.
            &["SELECT count(*) FROM t"], // 0
            "recursive_triggers_on_full_cascade",
        )
        .await;
    });
}

#[test]
fn recursive_triggers_pragma_roundtrip() {
    asupersync::test_utils::run_test(|| async {
        // Default is OFF (0); turning it on reads back 1.
        scenario(&[], &["PRAGMA recursive_triggers"], "rt_default").await;
        scenario(
            &["PRAGMA recursive_triggers = ON"],
            &["PRAGMA recursive_triggers"], // 1
            "rt_on",
        )
        .await;
    });
}
