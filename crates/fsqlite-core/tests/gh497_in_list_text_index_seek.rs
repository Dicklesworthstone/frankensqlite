#![recursion_limit = "512"]

//! GH#497 (bd-5qu8n): `col IN ('a', 'b', …)` on a TEXT column with an ordinary index seeks the
//! index once per distinct value, as `col = 'a'` does, instead of scanning the whole table.
//! Results are compared with stock SQLite (rusqlite, bundled) as sorted row sets (no ORDER BY, so
//! row order is unspecified), on `:memory:` and on a file, through `query()` and
//! `prepare().query()`; the EXPLAIN of the BINARY TEXT shape must open and seek the index.
//! Also covered: an integer IN list over a composite index whose trailing key column holds NULL
//! (the probe is a one-field prefix there), and shapes that must keep the full scan (a NOCASE
//! column) yet still return stock's rows.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE r9_t (id TEXT PRIMARY KEY, k TEXT NOT NULL, v TEXT)",
    "CREATE INDEX r9_t_k ON r9_t (k)",
    "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 199) \
     INSERT INTO r9_t SELECT 'id' || i, 'k' || (i % 20), 'v' || i FROM n",
    "INSERT INTO r9_t VALUES ('e1', '', 'empty'), ('u1', 'ключ', 'unicode')",
    "CREATE TABLE c (a INT, b INT, v TEXT)",
    "CREATE INDEX c_ab ON c (a, b)",
    "INSERT INTO c VALUES (1, NULL, 'n1'), (1, 2, 'x'), (2, NULL, 'n2'), (3, 4, 'y'), (1, NULL, 'n3')",
    "CREATE TABLE nc (k TEXT COLLATE NOCASE, v TEXT)",
    "CREATE INDEX nc_k ON nc (k)",
    "INSERT INTO nc VALUES ('A', 'upper'), ('a', 'lower'), ('b', 'other')",
];

const QUERIES: &[&str] = &[
    "SELECT v FROM r9_t WHERE k IN ('k1')",
    "SELECT v FROM r9_t WHERE k IN ('k1', 'k2')",
    "SELECT v FROM r9_t WHERE k IN ('k2', 'k1', 'k2')",
    "SELECT v FROM r9_t WHERE k IN ('zz')",
    "SELECT v FROM r9_t WHERE k IN ('', 'ключ')",
    "SELECT id, v FROM r9_t WHERE k IN ('k3', 'k4') AND v LIKE 'v1%'",
    "SELECT * FROM r9_t WHERE k IN ('k5')",
    "SELECT v FROM c WHERE a IN (1, 2)",
    "SELECT v FROM c WHERE a IN (1) AND b IS NULL",
    "SELECT v FROM nc WHERE k IN ('A')",
    "SELECT v FROM nc WHERE k IN ('a', 'B')",
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

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = r.prepare(sql).expect("stock prepare");
    let n = statement.column_count();
    let rows = statement
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock query");
    sorted(rows)
}

fn frank_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    sorted(
        rows.iter()
            .map(|row| row.values().iter().map(tag_f).collect())
            .collect(),
    )
}

async fn run(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("gh497.db").to_str().expect("utf-8 path").to_owned()
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
        let stock = stock_rows(&r, sql);
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
    // The index is opened and sought, as for `k = 'k1'`: no table Rewind.
    for sql in [
        "EXPLAIN SELECT v FROM r9_t WHERE k IN ('k1')",
        "EXPLAIN SELECT v FROM r9_t WHERE k IN ('k1', 'k2')",
    ] {
        let opcodes: Vec<String> = f
            .query(sql)
            .await
            .expect("explain")
            .iter()
            .filter_map(|row| match row.values().get(1) {
                Some(SqliteValue::Text(op)) => Some(op.to_string()),
                _ => None,
            })
            .collect();
        if !opcodes.iter().any(|op| op == "SeekGE") || opcodes.iter().any(|op| op == "Rewind") {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` does not seek the index: {opcodes:?}"
            ));
        }
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn text_in_list_seeks_the_index_and_matches_stock() {
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
