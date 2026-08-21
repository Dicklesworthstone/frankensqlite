//! Differential oracle: UPSERT (`INSERT ... ON CONFLICT`) + `INSERT OR
//! IGNORE/REPLACE` vs rusqlite (bundled SQLite 3.53). A probe sweep found this
//! write path stock-correct across 12 cases; this keeper locks it in.
//!
//! Key semantics asserted: DO NOTHING skips a conflicting row; DO UPDATE can
//! read both the existing row and `excluded.` (the would-be-inserted values); a
//! DO UPDATE `WHERE` that evaluates false leaves the row unchanged (no error);
//! a conflict target may be omitted; a second UNIQUE constraint can be the
//! target; multi-row upserts apply per row; OR IGNORE skips, OR REPLACE
//! delete-then-inserts.

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

async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(fr, rr, "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

#[test]
fn do_nothing() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(k INT UNIQUE, v INT)",
              "INSERT INTO t VALUES (1,10)",
              "INSERT INTO t VALUES (1,99) ON CONFLICT(k) DO NOTHING",
              "INSERT INTO t VALUES (2,20) ON CONFLICT(k) DO NOTHING"],
            "SELECT k,v FROM t ORDER BY k",
            "DO NOTHING skips a conflict, keeps a non-conflict",
        ).await;
        agree(
            &["CREATE TABLE t(k INT UNIQUE, v INT)",
              "INSERT INTO t VALUES (1,10)",
              "INSERT INTO t VALUES (2,20) ON CONFLICT(k) DO NOTHING"],
            "SELECT k,v FROM t ORDER BY k",
            "DO NOTHING with no conflict is a normal insert",
        ).await;
    });
}

#[test]
fn do_update_from_excluded() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(k INT PRIMARY KEY, v INT)",
              "INSERT INTO t VALUES (1,10)",
              "INSERT INTO t VALUES (1,99) ON CONFLICT(k) DO UPDATE SET v = excluded.v"],
            "SELECT k,v FROM t ORDER BY k",
            "DO UPDATE SET v = excluded.v",
        ).await;
        agree(
            &["CREATE TABLE t(k INT PRIMARY KEY, v INT)",
              "INSERT INTO t VALUES (1,10)",
              "INSERT INTO t VALUES (1,5) ON CONFLICT(k) DO UPDATE SET v = v + excluded.v"],
            "SELECT k,v FROM t ORDER BY k",
            "DO UPDATE reading both existing v and excluded.v",
        ).await;
        agree(
            &["CREATE TABLE t(k INT PRIMARY KEY, v INT)",
              "INSERT INTO t VALUES (1,10)",
              "INSERT INTO t VALUES (1,99) ON CONFLICT DO UPDATE SET v = excluded.v"],
            "SELECT k,v FROM t ORDER BY k",
            "DO UPDATE with no explicit conflict target",
        ).await;
    });
}

#[test]
fn do_update_conditional_where() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(k INT PRIMARY KEY, v INT)",
              "INSERT INTO t VALUES (1,10)",
              "INSERT INTO t VALUES (1,99) ON CONFLICT(k) DO UPDATE SET v = excluded.v WHERE v > 100"],
            "SELECT k,v FROM t ORDER BY k",
            "DO UPDATE WHERE false leaves the row unchanged",
        ).await;
        agree(
            &["CREATE TABLE t(k INT PRIMARY KEY, v INT)",
              "INSERT INTO t VALUES (1,10)",
              "INSERT INTO t VALUES (1,99) ON CONFLICT(k) DO UPDATE SET v = excluded.v WHERE v < 100"],
            "SELECT k,v FROM t ORDER BY k",
            "DO UPDATE WHERE true applies the update",
        ).await;
    });
}

#[test]
fn do_update_multirow() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(k INT PRIMARY KEY, v INT)",
              "INSERT INTO t VALUES (1,10),(2,20)",
              "INSERT INTO t VALUES (1,100),(3,30),(2,200) ON CONFLICT(k) DO UPDATE SET v = excluded.v"],
            "SELECT k,v FROM t ORDER BY k",
            "multi-row upsert: conflicts update, new rows insert",
        ).await;
        agree(
            &["CREATE TABLE t(k INT PRIMARY KEY, v INT)",
              "INSERT INTO t VALUES (1,1),(2,2),(3,3)",
              "INSERT INTO t VALUES (2,20) ON CONFLICT(k) DO UPDATE SET v = excluded.v"],
            "SELECT count(*), sum(v) FROM t",
            "upsert row count/aggregate unchanged in size",
        ).await;
    });
}

#[test]
fn insert_or_ignore_or_replace() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(k INT UNIQUE, v INT)",
              "INSERT INTO t VALUES (1,10)",
              "INSERT OR IGNORE INTO t VALUES (1,99),(2,20)"],
            "SELECT k,v FROM t ORDER BY k",
            "INSERT OR IGNORE skips the conflict",
        ).await;
        agree(
            &["CREATE TABLE t(k INT PRIMARY KEY, v INT)",
              "INSERT INTO t VALUES (1,10)",
              "INSERT OR REPLACE INTO t VALUES (1,99),(2,20)"],
            "SELECT k,v FROM t ORDER BY k",
            "INSERT OR REPLACE delete-then-inserts",
        ).await;
    });
}

#[test]
fn upsert_on_second_unique() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INT PRIMARY KEY, email TEXT UNIQUE, hits INT)",
              "INSERT INTO t VALUES (1,'a@x',1)",
              "INSERT INTO t VALUES (2,'a@x',1) ON CONFLICT(email) DO UPDATE SET hits = hits + 1"],
            "SELECT id,email,hits FROM t ORDER BY id",
            "upsert targeting a secondary UNIQUE constraint",
        ).await;
    });
}
