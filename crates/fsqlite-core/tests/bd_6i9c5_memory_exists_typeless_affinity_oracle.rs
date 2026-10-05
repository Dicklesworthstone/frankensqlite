#![recursion_limit = "512"]

//! bd-6i9c5: a typeless column has BLOB affinity, so comparing it with a
//! TEXT-affinity expression converts neither side: integer 6 in `b.aid` does
//! not equal `CAST(6 AS TEXT)`. The in-memory correlated EXISTS probe built its
//! comparison context without marking the probe table's typeless columns as
//! declared, so they counted as having no affinity and the CAST's TEXT
//! affinity applied to them: `EXISTS (SELECT 1 FROM b WHERE b.aid =
//! CAST(a.id AS TEXT))` matched a.id = 6 in `:memory:` but not on disk.
//!
//! Each query is compared with rusqlite, ad hoc and prepared, in memory and
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
    "CREATE TABLE a(id INTEGER PRIMARY KEY, t TEXT, n INTEGER, x)",
    "INSERT INTO a VALUES (1,'1',1,1),(2,'2','2','2'),(3,'01',3,' 3'),(4,NULL,NULL,NULL),\
     (5,'x',5,x'35'),(6,'2.0',6,2.0)",
    "CREATE TABLE b(id INTEGER PRIMARY KEY, aid, at TEXT, an INTEGER)",
    "INSERT INTO b VALUES (10,1,'1',1),(11,'2','2','2'),(12,' 3','03',3),(13,2.0,'2.0',2),\
     (14,NULL,NULL,NULL),(15,'1','1',1),(16,6,'6',6)",
    "CREATE INDEX b_aid ON b(aid)",
    "CREATE TABLE c(id INTEGER PRIMARY KEY, v)",
    "INSERT INTO c VALUES (1,6),(2,'6'),(3,6.0),(4,x'36')",
];

const QUERIES: &[&str] = &[
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = CAST(a.id AS TEXT)) ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE CAST(a.id AS TEXT) = b.aid) ORDER BY a.id",
    "SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.aid = CAST(a.id AS TEXT)) ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = CAST(a.n AS TEXT)) ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = CAST(a.x AS TEXT)) ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = CAST(a.id AS TEXT) AND b.id > 0) \
     ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = CAST(a.t AS INTEGER)) ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.at = a.id) ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = a.t) ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.v = CAST(a.id AS TEXT)) ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.v = CAST(a.id AS REAL)) ORDER BY a.id",
    "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM c WHERE c.v = CAST(a.id AS BLOB)) ORDER BY a.id",
    "SELECT a.id, (SELECT group_concat(b.id) FROM b WHERE b.aid = CAST(a.id AS TEXT)) FROM a \
     ORDER BY a.id",
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
fn correlated_exists_compares_typeless_columns_without_conversion() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_6i9c5.db")
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
            for sql in QUERIES {
                let stock = rows_r(&r, sql);
                assert_eq!(rows_f(&f, sql).await, stock, "query `{sql}`");
                assert_eq!(rows_prepared(&f, sql).await, stock, "prepared `{sql}`");
            }
        });
    }
}
