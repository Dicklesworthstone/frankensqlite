#![recursion_limit = "512"]

//! bd-hhh47: `UPDATE t ... FROM (SELECT * FROM s ...) AS s` failed with
//! "circular reference: s" where SQLite accepts it. A FROM subquery that
//! cannot be flattened is hoisted into a CTE, and the CTE was named after the
//! subquery's alias, so a body reading a table of that name referenced the CTE
//! itself. The same CTE also shadowed a real table of the alias's name in the
//! rest of the statement. Each shape is compared with rusqlite, in memory and
//! file-backed.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{b:?}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
    "CREATE TABLE s(seq INTEGER PRIMARY KEY, tid, nv)",
    "INSERT INTO s(tid, nv) VALUES (1,'x'),(1,'y'),(2,'z'),(3,'w'),(3,'q')",
    "CREATE TABLE s2(tid, nv)",
    "INSERT INTO s2 VALUES (1,'m'),(2,'n'),(2,'o')",
    "CREATE TABLE log(msg)",
];

/// Each UPDATE runs on a fresh copy of `t`; the rows it leaves are compared.
const UPDATES: &[&str] = &[
    "UPDATE t SET v = s.nv FROM (SELECT * FROM s ORDER BY seq DESC) AS s WHERE s.tid = t.id",
    "UPDATE t SET v = s.nv FROM (SELECT * FROM s ORDER BY seq) AS s WHERE s.tid = t.id",
    "UPDATE t SET v = s.total FROM (SELECT tid, count(*) AS total FROM s GROUP BY tid) AS s \
     WHERE s.tid = t.id",
    "UPDATE t SET v = s.nv || ':' || (SELECT count(*) FROM s) \
     FROM (SELECT tid, max(nv) AS nv FROM s2 GROUP BY tid) AS s WHERE s.tid = t.id",
    "UPDATE t SET v = (SELECT group_concat(nv) FROM s WHERE s.tid = t.id) \
     FROM (SELECT DISTINCT tid FROM s2) AS s WHERE s.tid = t.id",
    "UPDATE t SET v = nv FROM (SELECT tid, nv FROM s ORDER BY seq LIMIT 3) WHERE tid = t.id",
    "UPDATE t SET v = s.nv FROM (SELECT * FROM s ORDER BY seq DESC) AS s \
     JOIN s2 ON s2.tid = s.tid WHERE s.tid = t.id",
    "UPDATE t SET v = t2.nv FROM (SELECT * FROM t JOIN s ON s.tid = t.id) AS t2 \
     WHERE t2.id = t.id AND t2.seq % 2 = 1",
];

fn rows_r(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .expect("rusqlite query")
    .map(|r| r.unwrap())
    .collect()
}

async fn rows_f(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

const RESET: &[&str] = &[
    "DELETE FROM t",
    "DELETE FROM log",
    "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')",
];

async fn check_all(f: &Connection, r: &rusqlite::Connection, with_trigger: bool) {
    for update in UPDATES {
        for sql in RESET {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        let label = if with_trigger { "with trigger" } else { "plain" };
        f.execute(update)
            .await
            .unwrap_or_else(|e| panic!("franken ({label}) `{update}`: {e:?}"));
        r.execute(update, []).unwrap();
        for check in ["SELECT id, v FROM t ORDER BY id", "SELECT msg FROM log ORDER BY rowid"] {
            assert_eq!(
                rows_f(f, check).await,
                rows_r(r, check),
                "({label}) after `{update}`: `{check}`"
            );
        }
    }
}

#[test]
fn update_from_subquery_alias_does_not_shadow_a_table() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_hhh47.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            check_all(&f, &r, false).await;
            let trigger = "CREATE TRIGGER t_upd AFTER UPDATE ON t BEGIN \
                           INSERT INTO log VALUES (OLD.id || ':' || OLD.v || '->' || NEW.v); END";
            f.execute(trigger).await.unwrap();
            r.execute(trigger, []).unwrap();
            check_all(&f, &r, true).await;
        });
    }
}
