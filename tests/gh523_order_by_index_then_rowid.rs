#![recursion_limit = "512"]

//! GH#523: `SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = ?1 ORDER BY i, id LIMIT ?2`
//! (`UNIQUE(c, i)`, `id INTEGER PRIMARY KEY`) scanned the whole table and sorted it, once per
//! page. An index on a rowid table ends every entry with the rowid, ascending, so a last ORDER BY
//! term naming the rowid right after the index's key terms is already in index order: stock seeks
//! the index and does not sort. And when the hinted index cannot give the ORDER BY, it still seeks
//! the equality and sorts, rather than dropping the hint for a table scan.
//!
//! Results are compared in order with stock SQLite (rusqlite, bundled) on `:memory:` and on a
//! file, through `query()`, `prepare().query()` and bound parameters. Every ORDER BY here is total.
//! Covered: NULL index keys (equal keys, so the rowid alone orders them), a DESC index key in
//! both walk directions, mixed directions that must still sort, a hidden rowid, an index that
//! names the rowid alias as a key column, a rowid term before the index is exhausted, OFFSET, and
//! an output alias. The EXPLAIN of the issue's shape must seek the index with no sorter and no
//! table Rewind.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE m (id INTEGER PRIMARY KEY, c INTEGER, i INTEGER, t TEXT, UNIQUE (c, i))",
    "WITH RECURSIVE s(k) AS (SELECT 1 UNION ALL SELECT k + 1 FROM s WHERE k < 300) \
     INSERT INTO m SELECT (k * 7919) % 1000 + 1, k % 4, (k * 37) % 101, 't' || (k % 9) FROM s",
    "INSERT INTO m VALUES (5000, 1, NULL, 'n1'), (4000, 1, NULL, 'n2'), (6000, 1, NULL, 'n3'), \
     (4500, 2, NULL, 'n4')",
    "CREATE TABLE n (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, v TEXT)",
    "CREATE INDEX n_ab ON n (a, b DESC)",
    "WITH RECURSIVE s(k) AS (SELECT 1 UNION ALL SELECT k + 1 FROM s WHERE k < 400) \
     INSERT INTO n SELECT (k * 104729) % 2000 + 1, k % 5, k % 3, 'v' || k FROM s",
    "CREATE TABLE h (a INTEGER, b TEXT)",
    "CREATE INDEX h_a ON h (a)",
    "WITH RECURSIVE s(k) AS (SELECT 1 UNION ALL SELECT k + 1 FROM s WHERE k < 120) \
     INSERT INTO h SELECT k % 6, 'b' || k FROM s",
    "DELETE FROM h WHERE rowid % 7 = 0",
    "CREATE TABLE k (id INTEGER PRIMARY KEY, c INTEGER, v TEXT)",
    "CREATE INDEX k_cid ON k (c, id)",
    "INSERT INTO k VALUES (9, 1, 'a'), (3, 1, 'b'), (7, 2, 'c'), (1, 1, 'd'), (8, 1, 'e')",
];

const QUERIES: &[&str] = &[
    // The issue's shape, hinted and not, both walk directions, OFFSET, an alias.
    "SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = 1 ORDER BY i, id LIMIT 5",
    "SELECT id, i FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = 1 ORDER BY i, id",
    "SELECT id, i FROM m WHERE c = 1 ORDER BY i, id",
    "SELECT id, i FROM m WHERE c = 1 ORDER BY i DESC, id DESC",
    "SELECT id, i, t FROM m WHERE c = 2 ORDER BY c, i, id LIMIT 4 OFFSET 2",
    "SELECT id AS x, i FROM m WHERE c = 1 ORDER BY i, x LIMIT 6",
    "SELECT id, t FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = 3 ORDER BY i, rowid",
    "SELECT c, i, id FROM m INDEXED BY sqlite_autoindex_m_1 ORDER BY c, i, id LIMIT 8",
    "SELECT c, i, id FROM m ORDER BY c DESC, i DESC, id DESC LIMIT 8",
    // Mixed directions against the index: the rowid term must still be sorted.
    "SELECT id, i FROM m WHERE c = 1 ORDER BY i, id DESC",
    "SELECT id, i FROM m WHERE c = 1 ORDER BY i DESC, id",
    // A hinted index the ORDER BY cannot use seeks the equality and sorts.
    "SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = 1 ORDER BY t, id",
    "SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = 2 AND t > 't4' ORDER BY id DESC",
    // A DESC key term: a forward walk is (b DESC, rowid ASC), a reverse one the opposite.
    "SELECT id, b FROM n WHERE a = 3 ORDER BY b DESC, id",
    "SELECT id, b FROM n WHERE a = 3 ORDER BY b, id DESC",
    "SELECT id, b FROM n WHERE a = 3 ORDER BY b DESC, id DESC",
    "SELECT id, b FROM n WHERE a = 3 AND b = 1 ORDER BY id DESC",
    "SELECT id, b FROM n INDEXED BY n_ab WHERE a = 2 ORDER BY b DESC, id LIMIT 7",
    // The index is not exhausted: the rowid term alone cannot follow it.
    "SELECT id, b FROM n WHERE a = 4 ORDER BY id",
    // A hidden rowid.
    "SELECT rowid, b FROM h WHERE a = 2 ORDER BY rowid DESC",
    "SELECT rowid, b FROM h INDEXED BY h_a WHERE a = 5 ORDER BY a, _rowid_",
    "SELECT oid, a FROM h ORDER BY a, oid LIMIT 9",
    // An index naming the rowid alias as a key column.
    "SELECT v FROM k WHERE c = 1 ORDER BY id",
    "SELECT v FROM k INDEXED BY k_cid WHERE c = 1 ORDER BY id DESC",
];

const PARAM_QUERIES: &[(&str, &[SqliteValue])] = &[
    (
        "SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = ?1 ORDER BY i, id LIMIT ?2",
        &[SqliteValue::Integer(1), SqliteValue::Integer(5)],
    ),
    (
        "SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = ?1 ORDER BY i, id LIMIT ?2",
        &[SqliteValue::Float(2.0), SqliteValue::Integer(50)],
    ),
    (
        "SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = ?1 ORDER BY i, id LIMIT ?2",
        &[SqliteValue::Null, SqliteValue::Integer(5)],
    ),
    (
        "SELECT id FROM m WHERE c = ?1 ORDER BY i DESC, id DESC",
        &[SqliteValue::Integer(3)],
    ),
    (
        "SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = ?1 ORDER BY t, id",
        &[SqliteValue::Integer(0)],
    ),
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn to_rusqlite(v: &SqliteValue) -> rusqlite::types::Value {
    match v {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let mut statement = r.prepare(sql).expect("stock prepare");
    let n = statement.column_count();
    let params: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
    statement
        .query_map(rusqlite::params_from_iter(params), |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock query")
}

fn frank_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

async fn explain_rows(f: &Connection, sql: &str) -> Vec<(String, String)> {
    f.query(sql)
        .await
        .expect("explain")
        .iter()
        .map(|row| {
            let values = row.values();
            let text = |i: usize| match values.get(i) {
                Some(SqliteValue::Text(s)) => s.to_string(),
                _ => String::new(),
            };
            (text(1), text(5))
        })
        .collect()
}

async fn run(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("gh523.db").to_str().expect("utf-8 path").to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in SETUP {
        f.execute(sql).await.expect("frank setup");
        r.execute(sql, []).expect("stock setup");
    }
    let mut failures = Vec::new();
    for sql in QUERIES {
        let stock = stock_rows(&r, sql, &[]);
        assert!(stock.len() > 1, "`{sql}` must return several rows to test an order");
        let direct = f.query(sql).await.map(|rows| frank_rows(&rows));
        if direct.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (query): frank {direct:?} vs stock {stock:?}"
            ));
        }
        let prepared = match f.prepare(sql).await {
            Ok(statement) => statement.query().await.map(|rows| frank_rows(&rows)),
            Err(e) => Err(e),
        };
        if prepared.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` (prepare): frank {prepared:?} vs stock {stock:?}"
            ));
        }
    }
    for (sql, params) in PARAM_QUERIES {
        let stock = stock_rows(&r, sql, params);
        let direct = f
            .query_with_params(sql, params)
            .await
            .map(|rows| frank_rows(&rows));
        if direct.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` {params:?} (query_with_params): frank \
                 {direct:?} vs stock {stock:?}"
            ));
        }
        let prepared = match f.prepare(sql).await {
            Ok(statement) => statement
                .query_with_params(params)
                .await
                .map(|rows| frank_rows(&rows)),
            Err(e) => Err(e),
        };
        if prepared.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` {params:?} (prepare): frank {prepared:?} vs \
                 stock {stock:?}"
            ));
        }
    }
    // (program, sorts): each opens and seeks the autoindex and never rewinds; only the second,
    // whose ORDER BY the index cannot give, sorts.
    for (sql, sorts) in [
        (
            "EXPLAIN SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = 1 \
             ORDER BY i, id LIMIT 5",
            false,
        ),
        (
            "EXPLAIN SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = 1 ORDER BY t",
            true,
        ),
        ("EXPLAIN SELECT id FROM m WHERE c = 1 ORDER BY i DESC, id DESC", false),
    ] {
        let program = explain_rows(&f, sql).await;
        let opens_index = program
            .iter()
            .any(|(op, p4)| op == "OpenRead" && p4.contains("sqlite_autoindex_m_1"));
        let seeks = program.iter().any(|(op, _)| op.starts_with("Seek"));
        let sorted = program.iter().any(|(op, _)| op == "SorterSort");
        let rewinds = program.iter().any(|(op, _)| op == "Rewind");
        if !opens_index || !seeks || rewinds || sorted != sorts {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` should seek sqlite_autoindex_m_1 with \
                 sorts={sorts} and no Rewind: {program:?}"
            ));
        }
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn order_by_index_columns_then_rowid_seeks_without_sorting() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            failures.extend(run(file_backed).await);
        }
        assert!(
            failures.is_empty(),
            "{} failures:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
