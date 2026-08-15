#![recursion_limit = "512"]

//! GH #186 (bd-gh-autoincrement-sequence) HEAD probe: an INSERT OR IGNORE that
//! allocates an AUTOINCREMENT rowid but discards the row on a UNIQUE conflict
//! must still advance sqlite_sequence (stock sqlite3), so the next insert skips
//! the burned rowid. Differential vs rusqlite.

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
    assert_eq!(fr, rr, "autoincrement seq mismatch on `{sql}`");
}

async fn run_both(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) {
    for s in stmts {
        let _ = fconn.execute(s).await;
        let _ = rconn.execute_batch(s);
    }
}

#[test]
#[ignore = "bd-gh-autoincrement-sequence (GH #186): an INSERT OR IGNORE conflict allocates an \
AUTOINCREMENT rowid (NewRowid, burning it) but inserts nothing, so the connection's sequence \
write-back — which only sees affected=0 / last_insert_rowid / table_max_rowid_in_txn — never \
records the burned rowid and sqlite_sequence does not advance. Fix map: capture the max allocated \
AUTOINCREMENT rowid program-scoped in the VDBE engine's NewRowid handler (engine.rs ~10504, \
RowIdMode::AutoIncrement, keyed by root_page), expose it like last_insert_rowid, surface it through \
execute_table_program_with_db (connection.rs ~111474/111680), and have \
refresh_autoincrement_sequence_after_insert use max(current, allocated_high_water). Un-ignore then."]
fn autoincrement_or_ignore_advances_seq_gh186() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        run_both(&f, &r, &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v UNIQUE)",
            "INSERT INTO t(v) VALUES ('a')",
            "INSERT OR IGNORE INTO t(v) VALUES ('a')", // conflict: row ignored, rowid 2 burned
        ]).await;
        // sqlite3: seq advanced to 2 after the ignored insert.
        assert_agree(&f, &r, "SELECT seq FROM sqlite_sequence WHERE name='t'").await;
        run_both(&f, &r, &["INSERT INTO t(v) VALUES ('b')"]).await;
        // Next insert gets id 3 (2 was burned), seq -> 3.
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT seq FROM sqlite_sequence WHERE name='t'").await;
    });
}

#[test]
fn autoincrement_plain_sequence_control_gh186() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Control: no conflict — the sequence tracks the max inserted rowid.
        run_both(&f, &r, &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v UNIQUE)",
            "INSERT INTO t(v) VALUES ('a')",
            "INSERT INTO t(v) VALUES ('b')",
            "INSERT INTO t(id, v) VALUES (10, 'c')",
        ]).await;
        assert_agree(&f, &r, "SELECT seq FROM sqlite_sequence WHERE name='t'").await;
        run_both(&f, &r, &["INSERT INTO t(v) VALUES ('d')"]).await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
    });
}
