#![recursion_limit = "512"]

//! bd-gtjlv: an equality scan with a residual (`a = 1 AND k = 'x' AND id > 0`)
//! on a composite index `(a, k)` probed only the leading column and walked
//! every `a = 1` entry, filtering `k` per row, partial index or not. Key terms
//! after the leading one that the WHERE pins with an exact `column = literal`
//! (no conversion implied, BINARY ordering) now join the probe prefix.
//!
//! Results are compared with rusqlite, in memory and file-backed, including
//! shapes whose second term must stay out of the prefix; the probe width is
//! read from EXPLAIN.

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
    "CREATE TABLE p(id INTEGER PRIMARY KEY, a INTEGER, k TEXT, v)",
    "CREATE INDEX p_ak ON p(a, k) WHERE k IS NOT NULL",
    "CREATE TABLE n(id INTEGER PRIMARY KEY, a INTEGER, k TEXT, v)",
    "CREATE INDEX n_ak ON n(a, k)",
    "CREATE TABLE m(id INTEGER PRIMARY KEY, a INTEGER, k TEXT COLLATE NOCASE, x, v)",
    "CREATE INDEX m_ak ON m(a, k)",
    "CREATE INDEX m_ax ON m(a, x)",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 400) \
     INSERT INTO p SELECT i, i % 4, CASE WHEN i % 10 = 0 THEN NULL ELSE 'k' || (i % 50) END, i FROM s",
    "INSERT INTO n SELECT * FROM p",
    "INSERT INTO n VALUES (1001, 1, '7', 'text7'), (1002, 1, 7, 'int7'), (1003, 1, 'K1', 'upper')",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 200) \
     INSERT INTO m SELECT i, i % 3, CASE i % 4 WHEN 0 THEN 'k1' WHEN 1 THEN 'K1' ELSE 'k2' END, \
     CASE i % 5 WHEN 0 THEN 7 WHEN 1 THEN '7' ELSE 'x' END, i FROM s",
];

/// `(query, probe fields expected for its index scan)`. Rows are compared as
/// sorted sets (no ORDER BY or rowid predicate, so the index scan is the plan).
const QUERIES: &[(&str, usize)] = &[
    // Exact second terms join the prefix (partial and plain composite).
    ("SELECT id, v FROM p WHERE a = 1 AND k = 'k21' AND v > 0", 2),
    ("SELECT id, v FROM p WHERE a = 1 AND k = 'nope' AND v > 0", 2),
    ("SELECT id, v FROM p WHERE a = 9 AND k = 'k21' AND v > 0", 2),
    ("SELECT id, v FROM p WHERE k = 'k21' AND a = 1 AND v > 100", 2),
    ("SELECT id, v FROM p WHERE a = 1 AND 'k21' = k AND v > 0", 2),
    ("SELECT id, v FROM n WHERE a = 1 AND k = 'k21' AND v > 0", 2),
    ("SELECT id, v FROM n WHERE a = 1 AND k = '7' AND v IS NOT NULL", 2),
    // An integer literal against TEXT `k` converts: the leading term alone.
    ("SELECT id, v FROM n WHERE a = 1 AND k = 7 AND v IS NOT NULL", 1),
    // NOCASE and typeless second terms stay out of the prefix.
    ("SELECT id, v FROM m WHERE a = 1 AND k = 'k1' AND v > 0", 1),
    ("SELECT id, v FROM m WHERE a = 1 AND x = 7 AND v > 0", 1),
    ("SELECT id, v FROM m WHERE a = 1 AND x = '7' AND v > 0", 1),
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

/// The field count of the record the index `SeekGE` probes with.
fn seek_probe_fields(explain: &str) -> Option<usize> {
    let ops: Vec<Vec<&str>> = explain
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>())
        .filter(|fields| fields.len() >= 5 && fields[0].parse::<usize>().is_ok())
        .collect();
    // `SeekGE cursor jump record`, `MakeRecord first count record`.
    let seek = ops.iter().position(|op| op[1] == "SeekGE")?;
    let record_reg = ops[seek][4];
    ops[..seek]
        .iter()
        .rev()
        .find(|op| op[1] == "MakeRecord" && op[4] == record_reg)
        .and_then(|op| op[3].parse().ok())
}

#[test]
fn composite_equality_scans_seek_the_pinned_prefix() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_gtjlv.db")
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
            for (sql, fields) in QUERIES {
                let mut franken = rows_f(&f, sql).await;
                let mut stock = rows_r(&r, sql);
                franken.sort();
                stock.sort();
                assert_eq!(franken, stock, "query `{sql}`");
                let explain = f.prepare(sql).await.unwrap().explain();
                assert_eq!(
                    seek_probe_fields(&explain),
                    Some(*fields),
                    "probe width of `{sql}`:\n{explain}"
                );
            }
        });
    }
}
