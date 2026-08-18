#![recursion_limit = "512"]

//! bd-elcjy (GH#147 fresh-table subcase): a table whose first row is inserted
//! AFTER a SAVEPOINT is absent from the concurrent rowid allocator's savepoint
//! mark, so ROLLBACK TO must still rewind its tip — the next insert reuses id 1,
//! matching stock SQLite. Differential vs rusqlite (frank runs concurrent mode
//! by default; the oracle is single-writer, and both must agree).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
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
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
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
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn agree(setup: &[&str], query: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, query).await;
    let rr = rq(&r, query);
    assert_eq!(fr, rr, "{msg}\n  frank={fr:?}\n  stock={rr:?}");
}

#[test]
fn fresh_autoincrement_table_reuses_id_1_after_rollback_to() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                "BEGIN",
                "SAVEPOINT sp",
                "INSERT INTO t(v) VALUES ('a')",
                "ROLLBACK TO sp",
                "INSERT INTO t(v) VALUES ('b')",
                "COMMIT",
            ],
            "SELECT id, v FROM t ORDER BY id",
            "GH#147/bd-elcjy: fresh AUTOINCREMENT table must reuse id 1 after ROLLBACK TO",
        )
        .await;
    });
}

#[test]
fn fresh_rowid_table_reuses_id_1_after_rollback_to() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
                "BEGIN",
                "SAVEPOINT sp",
                "INSERT INTO t(v) VALUES ('a')",
                "ROLLBACK TO sp",
                "INSERT INTO t(v) VALUES ('b')",
                "COMMIT",
            ],
            "SELECT id, v FROM t ORDER BY id",
            "GH#147/bd-elcjy: fresh rowid table must reuse id 1 after ROLLBACK TO",
        )
        .await;
    });
}

// Non-regression: an id allocated BEFORE the savepoint is retained, and the
// rolled-back tail after it is reclaimed (the original #147 shape).
#[test]
fn pre_savepoint_id_retained_post_savepoint_tail_reclaimed() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                "BEGIN",
                "INSERT INTO t(v) VALUES ('a')",
                "SAVEPOINT sp",
                "INSERT INTO t(v) VALUES ('b')",
                "ROLLBACK TO sp",
                "INSERT INTO t(v) VALUES ('c')",
                "COMMIT",
            ],
            "SELECT id, v FROM t ORDER BY id",
            "GH#147: id 1 kept, rolled-back id 2 reclaimed for 'c'",
        )
        .await;
    });
}
