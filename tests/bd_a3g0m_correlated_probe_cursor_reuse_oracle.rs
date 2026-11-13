#![recursion_limit = "512"]

//! bd-a3g0m: a correlated subquery opens and closes its cursors once per
//! outer row, and `PRAGMA foreign_key_check` runs one such anti-join per
//! foreign key. Each `OpenRead` rebuilt its storage cursor (root page and
//! page 1 reads, header parse, b-tree cursor allocation), about 10 us per
//! child row against stock's 1.5 us. Until an execution opens a writable
//! cursor, a closed read cursor now stays parked in its slot for the next
//! `OpenRead` of the same root, and an `OpenWrite` drops the parked cursors.
//! The check also reports a table's violations by rowid, then foreign-key id,
//! as SQLite does.
//!
//! These queries reopen subquery cursors many times, share subquery cursor
//! numbers between different tables, and write the table a subquery reads,
//! ad hoc and prepared; each is compared with rusqlite, in memory and
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
    "PRAGMA foreign_keys = OFF",
    "CREATE TABLE p(id INTEGER PRIMARY KEY, code TEXT UNIQUE, v)",
    "CREATE TABLE c(id INTEGER PRIMARY KEY, pid REFERENCES p(id), pcode TEXT REFERENCES p(code), w)",
    "CREATE TABLE q(id INTEGER PRIMARY KEY, k INTEGER, v)",
    "CREATE INDEX q_k ON q(k)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300) \
     INSERT INTO p SELECT i, 'code' || i, i % 7 FROM n",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 600) \
     INSERT INTO c SELECT i, CASE WHEN i % 37 = 0 THEN i + 1000 ELSE (i * 7) % 300 + 1 END, \
     CASE WHEN i % 41 = 0 THEN 'nope' || i ELSE 'code' || ((i * 11) % 300 + 1) END, i % 5 FROM n",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200) \
     INSERT INTO q SELECT i, (i * 13) % 50, i % 3 FROM n",
];

const QUERIES: &[&str] = &[
    "PRAGMA foreign_key_check",
    "PRAGMA foreign_key_check(c)",
    "SELECT count(*) FROM c WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.id = c.pid)",
    "SELECT count(*) FROM c WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.code = c.pcode)",
    "SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM p WHERE p.id = c.pid AND p.v = 3) ORDER BY c.id",
    // Two subqueries on different tables share the subquery cursor numbers.
    "SELECT count(*) FROM c WHERE EXISTS (SELECT 1 FROM p WHERE p.id = c.pid) \
     AND NOT EXISTS (SELECT 1 FROM q WHERE q.k = c.w)",
    "SELECT c.id, (SELECT p.v FROM p WHERE p.id = c.pid), \
     (SELECT count(*) FROM q WHERE q.k = c.w) FROM c WHERE c.id % 50 = 0 ORDER BY c.id",
    // Nested correlated subqueries.
    "SELECT count(*) FROM c WHERE EXISTS (SELECT 1 FROM p WHERE p.id = c.pid \
     AND EXISTS (SELECT 1 FROM q WHERE q.k = p.v))",
    // A subquery over the table the outer query scans.
    "SELECT q.id FROM q WHERE NOT EXISTS (SELECT 1 FROM q AS q2 WHERE q2.k = q.k AND q2.id < q.id) \
     ORDER BY q.id",
];

/// Writes whose subqueries read the table being written, checked through the
/// rows they leave behind.
const WRITES: &[(&str, &str)] = &[
    (
        "UPDATE q SET v = (SELECT count(*) FROM q AS q2 WHERE q2.k < q.k) WHERE id % 3 = 0",
        "SELECT id, k, v FROM q ORDER BY id",
    ),
    (
        "DELETE FROM q WHERE EXISTS (SELECT 1 FROM q AS q2 WHERE q2.k = q.k + 1 AND q2.id > q.id)",
        "SELECT id, k, v FROM q ORDER BY id",
    ),
    (
        "INSERT INTO q(k, v) SELECT k + 100, v FROM q WHERE NOT EXISTS \
         (SELECT 1 FROM q AS q2 WHERE q2.k = q.k + 100)",
        "SELECT id, k, v FROM q ORDER BY id",
    ),
    (
        "DELETE FROM c WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.id = c.pid)",
        "SELECT count(*), sum(id) FROM c",
    ),
];

async fn rows_f(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

async fn rows_prepared(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("franken prepare `{sql}`: {e:?}"))
        .query()
        .await
        .unwrap_or_else(|e| panic!("franken prepared query `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

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

#[test]
fn correlated_probes_that_reopen_cursors_match_sqlite() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_a3g0m.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            for sql in QUERIES {
                let stock = rows_r(&r, sql);
                assert_eq!(rows_f(&f, sql).await, stock, "query `{sql}`");
                assert_eq!(rows_prepared(&f, sql).await, stock, "prepared `{sql}`");
            }
            for (write, check) in WRITES {
                f.execute(write)
                    .await
                    .unwrap_or_else(|e| panic!("franken `{write}`: {e:?}"));
                r.execute(write, []).unwrap();
                assert_eq!(rows_f(&f, check).await, rows_r(&r, check), "after `{write}`");
            }
        });
    }
}
