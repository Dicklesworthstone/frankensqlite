//! bd-y7old — Oracle-parity e2e: ON CONFLICT DO UPDATE must fire BEFORE/AFTER
//! UPDATE triggers (with OLD/NEW), NOT the AFTER INSERT trigger, when a row
//! conflicts and resolves to the update path.
//!
//! Stock SQLite semantics (empirically confirmed against sqlite3 3.46.1):
//!   * A conflicting UPSERT row still fires BEFORE INSERT (the row is attempted
//!     as an insert first), then fires BEFORE UPDATE + AFTER UPDATE with
//!     OLD = the existing row and NEW = the post-assignment row.
//!   * AFTER INSERT does NOT fire on the conflict/update path.
//!   * A fresh (non-conflicting) UPSERT row fires BEFORE INSERT + AFTER INSERT
//!     and no update triggers.
//!   * A conflicting row whose `DO UPDATE ... WHERE` evaluates false fires only
//!     BEFORE INSERT (the update is a no-op; no update triggers, no AFTER
//!     INSERT).
//!
//! Each scenario replays the identical script against frank and rusqlite and
//! asserts the recorded trigger log and the final table state agree.

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

/// Trigger-log preamble: a table `t`, a `log` table, and one trigger per
/// timing/event that records `<tag> old=<v> new=<v>` (INSERT rows have no OLD).
fn preamble() -> Vec<&'static str> {
    vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE TABLE log (seq INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT)",
        "INSERT INTO t VALUES (1,10)",
        "CREATE TRIGGER bi BEFORE INSERT ON t BEGIN INSERT INTO log(msg) VALUES('BI new='||new.v); END",
        "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO log(msg) VALUES('AI new='||new.v); END",
        "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN INSERT INTO log(msg) VALUES('BU old='||old.v||' new='||new.v); END",
        "CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO log(msg) VALUES('AU old='||old.v||' new='||new.v); END",
    ]
}

#[test]
fn upsert_conflict_fires_before_and_after_update_not_after_insert() {
    asupersync::test_utils::run_test(|| async {
        let mut stmts = preamble();
        // id=1 conflicts -> BI, BU(10->99), AU(10->99); NO AI.
        stmts.push("INSERT INTO t(id,v) VALUES(1,99) ON CONFLICT(id) DO UPDATE SET v=excluded.v");
        scenario(
            &stmts,
            &[
                "SELECT msg FROM log ORDER BY seq",
                "SELECT id, v FROM t ORDER BY id",
            ],
            "upsert_conflict_fires_before_and_after_update_not_after_insert",
        )
        .await;
    });
}

#[test]
fn upsert_fresh_row_fires_insert_triggers_only() {
    asupersync::test_utils::run_test(|| async {
        let mut stmts = preamble();
        // id=2 is fresh -> BI, AI; no update triggers.
        stmts.push("INSERT INTO t(id,v) VALUES(2,20) ON CONFLICT(id) DO UPDATE SET v=excluded.v");
        scenario(
            &stmts,
            &[
                "SELECT msg FROM log ORDER BY seq",
                "SELECT id, v FROM t ORDER BY id",
            ],
            "upsert_fresh_row_fires_insert_triggers_only",
        )
        .await;
    });
}

#[test]
fn upsert_conflict_where_false_fires_only_before_insert() {
    asupersync::test_utils::run_test(|| async {
        let mut stmts = preamble();
        // id=1 conflicts but WHERE is false -> only BI (update is a no-op).
        stmts.push(
            "INSERT INTO t(id,v) VALUES(1,7) ON CONFLICT(id) DO UPDATE SET v=excluded.v WHERE 1=0",
        );
        scenario(
            &stmts,
            &[
                "SELECT msg FROM log ORDER BY seq",
                "SELECT id, v FROM t ORDER BY id",
            ],
            "upsert_conflict_where_false_fires_only_before_insert",
        )
        .await;
    });
}

#[test]
fn upsert_counter_idiom_update_triggers_see_combined_new() {
    asupersync::test_utils::run_test(|| async {
        let mut stmts = preamble();
        // n = existing v + excluded v = 10 + 5 = 15; update triggers see new=15.
        stmts.push("INSERT INTO t(id,v) VALUES(1,5) ON CONFLICT(id) DO UPDATE SET v=v+excluded.v");
        scenario(
            &stmts,
            &[
                "SELECT msg FROM log ORDER BY seq",
                "SELECT id, v FROM t ORDER BY id",
            ],
            "upsert_counter_idiom_update_triggers_see_combined_new",
        )
        .await;
    });
}

#[test]
fn upsert_multi_row_mixed_insert_and_update_triggers() {
    asupersync::test_utils::run_test(|| async {
        let mut stmts = preamble();
        // Row 1 conflicts (BI, BU, AU), rows 2/3 fresh (BI, AI each).
        stmts.push(
            "INSERT INTO t(id,v) VALUES(1,100),(2,200),(3,300) ON CONFLICT(id) DO UPDATE SET v=excluded.v",
        );
        scenario(
            &stmts,
            &[
                "SELECT msg FROM log ORDER BY seq",
                "SELECT id, v FROM t ORDER BY id",
            ],
            "upsert_multi_row_mixed_insert_and_update_triggers",
        )
        .await;
    });
}

// bd-y7old — a conflicting DO UPDATE on a WITHOUT ROWID table (explicit target on
// the PK) must also fire BEFORE/AFTER UPDATE, exercising the plan helper's
// `is_rowid_table == false` branch and the counter idiom.
#[test]
fn upsert_without_rowid_conflict_fires_update_triggers() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE TABLE log (seq INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT)",
                "INSERT INTO t VALUES ('a',10)",
                "CREATE TRIGGER bi BEFORE INSERT ON t BEGIN INSERT INTO log(msg) VALUES('BI new='||new.v); END",
                "CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO log(msg) VALUES('AI new='||new.v); END",
                "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN INSERT INTO log(msg) VALUES('BU old='||old.v||' new='||new.v); END",
                "CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO log(msg) VALUES('AU old='||old.v||' new='||new.v); END",
                // k='a' conflicts -> BI, BU(10->109), AU(10->109); NO AI.
                "INSERT INTO t(k,v) VALUES('a',99) ON CONFLICT(k) DO UPDATE SET v=v+excluded.v",
            ],
            &[
                "SELECT msg FROM log ORDER BY seq",
                "SELECT k, v FROM t ORDER BY k",
            ],
            "upsert_without_rowid_conflict_fires_update_triggers",
        )
        .await;
    });
}
