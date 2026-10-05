#![recursion_limit = "512"]

//! bd-k8ebx: a numeric join probe into a typeless index also matches the
//! index's TEXT keys that NUMERIC affinity makes equal (`'2'`, `' 2'`,
//! `'2.0'`; bd-kr6hf). The join lookup found them by walking every TEXT key
//! of the index on every probe, so a 100k-row probe side paid the whole TEXT
//! region per row. The TEXT keys are now indexed once per statement by their
//! numeric value, and each probe seeks that map.
//!
//! Rows are compared with rusqlite, in memory and file-backed; the plan is
//! checked to build the map.

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
    // `k` is typeless and indexed; it holds numbers, reals, NULL, BLOBs and
    // TEXT, some of which NUMERIC affinity turns into numbers.
    "CREATE TABLE p(id INTEGER PRIMARY KEY, k, v)",
    "INSERT INTO p VALUES (1, 2, 'int2'), (2, '2', 'text2'), (3, ' 2', 'sp2'), (4, '2.0', 'r2'), \
     (5, 2.0, 'real2'), (6, '2e0', 'e2'), (7, '+2', 'plus2'), (8, '2abc', 'junk'), \
     (9, 'abc', 'abc'), (10, 3.5, 'real35'), (11, '3.5', 'text35'), (12, NULL, 'null'), \
     (13, x'32', 'blob2'), (14, '  7  ', 'sp7'), (15, '9223372036854775807', 'max'), \
     (16, '1e400', 'inf'), (17, '-0', 'negzero'), (18, '0', 'zero'), (19, 7, 'int7'), \
     (20, '2', 'text2b')",
    "CREATE INDEX p_k ON p(k)",
    "CREATE TABLE c(id INTEGER PRIMARY KEY, fk INTEGER, fr REAL, ft TEXT, fu)",
    "INSERT INTO c VALUES (1, 2, 2.0, '2', 2), (2, 7, 7.0, ' 7', '7'), (3, 0, 0.0, '0', 0), \
     (4, 9223372036854775807, 3.5, '3.5', 3.5), (5, NULL, NULL, NULL, NULL), (6, 4, 4.5, 'zz', x'32'), \
     (7, -0, -0.0, '-0', '2'), (8, 2, 1e400, '2.0', 2.0)",
];

const QUERIES: &[&str] = &[
    "SELECT c.id, p.id FROM c LEFT JOIN p ON p.k = c.fk ORDER BY c.id, p.id",
    "SELECT c.id, p.id FROM c LEFT JOIN p ON p.k = c.fr ORDER BY c.id, p.id",
    "SELECT c.id, p.id FROM c LEFT JOIN p ON p.k = c.ft ORDER BY c.id, p.id",
    "SELECT c.id, p.id FROM c LEFT JOIN p ON p.k = c.fu ORDER BY c.id, p.id",
    "SELECT c.id, p.id FROM c JOIN p ON p.k = c.fk ORDER BY c.id, p.id",
    "SELECT c.id, p.v, p.k FROM c LEFT JOIN p ON p.k = c.fk ORDER BY c.id, p.v",
    "SELECT count(*), sum(p.id), count(p.id) FROM c LEFT JOIN p ON p.k = c.fk",
    "SELECT count(*), count(p.id) FROM c LEFT JOIN p ON p.k = c.fk WHERE p.v IS NULL OR p.v <> 'text2'",
];

async fn rows_f(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
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
fn numeric_probes_match_numeric_text_keys_through_the_map() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_k8ebx.db")
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
                assert_eq!(rows_f(&f, sql).await, rows_r(&r, sql), "query `{sql}`");
            }
            if file_backed {
                // The lookup builds the map once instead of walking TEXT keys
                // per probe.
                let explain = f
                    .prepare("SELECT c.id, p.id FROM c LEFT JOIN p ON p.k = c.fk")
                    .await
                    .unwrap()
                    .explain();
                assert!(explain.contains("OpenAutoindex"), "{explain}");
            }
        });
    }
}
