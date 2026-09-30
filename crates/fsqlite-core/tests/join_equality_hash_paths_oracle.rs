#![recursion_limit = "512"]

//! Join equalities that now reach the hash join:
//! - WHERE equalities of an all-inner implicit join (`FROM a, b WHERE
//!   a.x = b.y`), which used to build the full cross product;
//! - ON equalities between a right-side column and an expression over the
//!   left side (`t.id = u.id * 2`), which used to run the nested loop.
//!
//! A file-backed database compiles the single-equality form into a rowid or
//! index seek on the key expression instead.
//!
//! Each case is compared with rusqlite, in memory and file-backed, with and
//! without indexes on the lookup columns. The fixture covers:
//! - INTEGER, REAL, TEXT, NOCASE and untyped columns;
//! - NULL keys;
//! - text keys that look numeric;
//! - LEFT joins and a LEFT/inner mix (WHERE pushdown stays off);
//! - duplicate matches, and extra ON and WHERE terms.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{}", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{}", b.len()),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let frows = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"));
    let ff: Vec<Vec<String>> = frows
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut stmt = rconn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    let rr: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect())
        })
        .expect("rusqlite query")
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(ff, rr, "mismatch on `{sql}`");
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d, n TEXT COLLATE NOCASE)",
    "INSERT INTO t VALUES (1,1,'1',1.0,'1','A'),(2,2,'2',2.5,2,'b'),(3,NULL,'x',3.0,NULL,'C'),\
     (4,4,'4',4.0,4.0,'d'),(5,5,'05',5.0,'5','e'),(6,6,'6',6.0,x'36','F'),(10,10,'10',10.0,10,'aa'),\
     (12,2,'2',12.0,'2','B')",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, r REAL, d)",
    "INSERT INTO u VALUES (1,1,'1',0.5,1),(2,2,'2',1.0,'2'),(3,3,'3',1.5,NULL),(4,NULL,NULL,2.0,4),\
     (5,5,'5',2.5,'05'),(6,6,'a',3.0,6.0),(7,7,'7',5.0,'x')",
    "CREATE TABLE w(k INTEGER, v TEXT)",
    "INSERT INTO w VALUES (1,'one'),(2,'two'),(4,'four'),(4,'FOUR'),(NULL,'null')",
];

const QUERIES: &[&str] = &[
    // Right column = expression over the left side.
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id * 2 ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id + 0.0 ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.r * 2 ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.b = u.id || '' ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.b = u.a + 0 ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.b || '' ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.d = u.id + 0 ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.d = u.d || '' ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.n = u.b || '' ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.a = -u.a + 2 * u.a ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.id = u.id * 2 ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a + 0 AND t.c > 2 ORDER BY 1, 2",
    "SELECT u.id, w.v FROM u JOIN w ON w.k = u.id + 0 ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id * 2 AND t.c <> 4.0 ORDER BY 1, 2",
    "SELECT count(*), sum(t.c) FROM u JOIN t ON t.id = u.id + 0",
    "SELECT u.b, count(*) FROM u JOIN t ON t.a = u.a + 0 GROUP BY u.b ORDER BY 1",
    "SELECT a.id, b.id FROM t a JOIN t b ON b.id = a.a * 2 ORDER BY 1, 2",
    // Inexact rowid keys (1.5, 'a') match nothing rather than a truncated rowid.
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.r * 1 ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.b || '' ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.id = u.d || '' ORDER BY 1, 2",
    // Implicit joins through WHERE equalities.
    "SELECT u.id, t.id FROM u, t WHERE t.id = u.id ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u, t WHERE t.b = u.b ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u, t WHERE t.n = u.b ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u, t WHERE t.d = u.d ORDER BY 1, 2",
    "SELECT u.id, t.id, w.v FROM u, t, w WHERE t.id = u.id AND w.k = t.a ORDER BY 1, 2, 3",
    "SELECT u.id, t.id, w.v FROM u, t, w WHERE w.k = u.id AND t.id = w.k ORDER BY 1, 2, 3",
    "SELECT u.id, t.id FROM u, t WHERE t.id = u.id OR t.a = 10 ORDER BY 1, 2",
    "SELECT u.id, t.id FROM u, t WHERE t.id = u.id AND u.r > 1 ORDER BY 1, 2",
    "SELECT a.id, b.id FROM t a, t b WHERE a.id = b.a ORDER BY 1, 2",
    // Outer joins keep their WHERE out of the ON.
    "SELECT u.id, t.id, w.v FROM u JOIN t ON t.id = u.id LEFT JOIN w ON w.k = t.a \
     WHERE w.v IS NULL OR t.a = u.a ORDER BY 1, 2, 3",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a WHERE t.id = u.id ORDER BY 1, 2",
];

/// Indexes that turn the key-expression joins into index lookups on the
/// compiled (file-backed) path.
const INDEXES: &[&str] = &[
    "CREATE INDEX t_a ON t(a)",
    "CREATE INDEX t_b ON t(b)",
    "CREATE INDEX t_n ON t(n)",
    "CREATE INDEX t_d ON t(d)",
];

#[test]
fn join_equalities_on_hash_paths_match_sqlite() {
    for (path, indexed) in [
        (None, false),
        (Some("join_equality_hash_paths.db"), false),
        (None, true),
        (Some("join_equality_hash_paths_indexed.db"), true),
    ] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = path.map_or_else(
                || ":memory:".to_owned(),
                |name| dir.path().join(name).to_string_lossy().into_owned(),
            );
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in QUERIES {
                assert_agree(&f, &r, sql).await;
            }
        });
    }
}
