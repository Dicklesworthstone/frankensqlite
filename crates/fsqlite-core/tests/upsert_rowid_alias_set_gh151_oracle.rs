#![recursion_limit = "512"]

//! GH #151 (bd-gh-upsert-rowid-alias-set): confirmation keeper. `ON CONFLICT DO
//! UPDATE SET` may reassign the INTEGER-PRIMARY-KEY (rowid alias) — including to
//! `excluded.id` — and the row must move to the new rowid. Fixed at HEAD by
//! 4c1aa7896 (reinsert-at-new-rowid); this pins it vs rusqlite.

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
    let f: Result<Vec<Vec<String>>, ()> = match fconn.query(sql).await {
        Ok(rows) => { let mut v: Vec<Vec<String>> = rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(); v.sort(); Ok(v) }
        Err(_) => Err(()),
    };
    let r: Result<Vec<Vec<String>>, ()> = (|| {
        let mut st = rconn.prepare(sql).map_err(|_| ())?;
        let n = st.column_count();
        let mut rows: Vec<Vec<String>> = st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).map_err(|_| ())?.collect::<Result<Vec<_>, _>>().map_err(|_| ())?;
        rows.sort(); Ok(rows)
    })();
    match (&f, &r) {
        (Ok(fr), Ok(rr)) => assert_eq!(fr, rr, "row mismatch on `{sql}`"),
        (Err(()), Err(())) => {}
        _ => panic!("error-vs-rows divergence on `{sql}`\n  frank: {f:?}\n  csql:  {r:?}"),
    }
}

#[test]
fn upsert_do_update_reassigns_rowid_alias_to_excluded_gh151() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, n INT)",
            "INSERT INTO t VALUES (1, 'a@b.c', 10)",
            "INSERT INTO t VALUES (99, 'a@b.c', 20) ON CONFLICT(email) DO UPDATE SET id = excluded.id, n = excluded.n",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // The row moved to rowid/id 99 with the new n; rowid tracks the alias.
        assert_agree(&f, &r, "SELECT rowid, id, email, n FROM t").await;
    });
}

#[test]
fn upsert_do_update_bumps_rowid_alias_expression_gh151() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT UNIQUE, n INT)",
            "INSERT INTO t VALUES (5, 'x', 1)",
            "INSERT INTO t VALUES (5, 'x', 2) ON CONFLICT(id) DO UPDATE SET id = id + 100, n = n + 1",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        assert_agree(&f, &r, "SELECT rowid, id, k, n FROM t").await;
    });
}
