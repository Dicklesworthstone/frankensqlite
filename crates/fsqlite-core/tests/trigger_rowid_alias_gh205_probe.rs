#![recursion_limit = "512"]

//! GH #205/#216 (bd-gh-trigger-rowid-alias / trigger-pseudo-row-rowid) HEAD
//! probe: NEW.rowid / OLD.rowid / NEW.oid / NEW._rowid_ in a trigger body must
//! resolve to the affected row's actual rowid, even for a table WITHOUT an
//! explicit INTEGER PRIMARY KEY. Differential-vs-rusqlite: the trigger logs the
//! rowid into a side table; we compare the log contents.

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

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let fr: Vec<Vec<String>> = fconn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}")).iter().map(|r| r.values().iter().map(tag_f).collect()).collect();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let rr: Vec<Vec<String>> = st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(fr, rr, "trigger rowid-alias log mismatch on `{sql}`");
}

async fn run_both(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) {
    for s in stmts {
        fconn.execute(s).await.unwrap_or_else(|e| panic!("frank exec `{s}`: {e:?}"));
        rconn.execute_batch(s).unwrap_or_else(|e| panic!("csql exec `{s}`: {e}"));
    }
}

#[test]
#[ignore = "bd-gh-trigger-rowid-alias (GH #205): NEW.rowid/OLD.rowid are NULL on a \
no-INTEGER-PRIMARY-KEY table because TriggerFrame has no old_rowid/new_rowid and the actual \
affected rowid is not captured at the row-collection points (collect_delete_trigger_rows / \
collect_insert_trigger_rows) nor threaded through fire_*_triggers -> make_trigger_frame. \
Un-ignore when that pipeline lands."]
fn trigger_rowid_alias_no_ipk_gh205() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        run_both(&f, &r, &[
            "CREATE TABLE t (a TEXT)",
            "CREATE TABLE log (op TEXT, rid INTEGER)",
            "CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO log VALUES ('ins', NEW.rowid); END",
            "CREATE TRIGGER td AFTER DELETE ON t BEGIN INSERT INTO log VALUES ('del', OLD.rowid); END",
            "CREATE TRIGGER tu AFTER UPDATE ON t BEGIN INSERT INTO log VALUES ('upd', OLD.rowid); END",
            "INSERT INTO t VALUES ('x')",
            "INSERT INTO t VALUES ('y')",
            "DELETE FROM t WHERE rowid = 1",
            "UPDATE t SET a = 'z' WHERE rowid = 2",
        ]).await;
        assert_agree(&f, &r, "SELECT op, rid FROM log ORDER BY _rowid_").await;
    });
}

#[test]
#[ignore = "bd-gh-trigger-pseudo-row-rowid (GH #216): NEW.oid / NEW._rowid_ share the same \
missing-rowid-capture root cause as GH #205 on a no-INTEGER-PRIMARY-KEY table. Un-ignore with #205."]
fn trigger_rowid_alias_oid_underscore_gh216() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        run_both(&f, &r, &[
            "CREATE TABLE t (a TEXT)",
            "CREATE TABLE log (op TEXT, rid INTEGER)",
            // NEW.oid and NEW._rowid_ are the same hidden rowid.
            "CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO log VALUES ('oid', NEW.oid); INSERT INTO log VALUES ('urid', NEW._rowid_); END",
            "INSERT INTO t VALUES ('x')",
            "INSERT INTO t VALUES ('y')",
        ]).await;
        assert_agree(&f, &r, "SELECT op, rid FROM log ORDER BY rid, op").await;
    });
}

#[test]
fn trigger_rowid_alias_ipk_control_gh205() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Control: an INTEGER PRIMARY KEY table (rowid alias) should already work.
        run_both(&f, &r, &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)",
            "CREATE TABLE log (op TEXT, rid INTEGER)",
            "CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO log VALUES ('ins', NEW.rowid); END",
            "INSERT INTO t VALUES (5, 'x')",
            "INSERT INTO t VALUES (9, 'y')",
        ]).await;
        assert_agree(&f, &r, "SELECT op, rid FROM log ORDER BY rid").await;
    });
}
