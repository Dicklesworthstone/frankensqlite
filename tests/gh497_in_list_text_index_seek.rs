#![recursion_limit = "512"]

//! GH#497 (bd-5qu8n): `col IN ('a', 'b', …)` on a TEXT column with an ordinary index seeks the
//! index once per distinct value, as `col = 'a'` does, instead of scanning the whole table.
//! Results are compared with stock SQLite (rusqlite, bundled) as sorted row sets (no ORDER BY, so
//! row order is unspecified), on `:memory:` and on a file, through `query()` and
//! `prepare().query()`; the EXPLAIN of the BINARY TEXT shape must open and seek the index.
//! Also covered: an integer IN list over a composite index whose trailing key column holds NULL
//! (the probe is a one-field prefix there), and shapes that must keep the full scan (a NOCASE
//! column) yet still return stock's rows.
//!
//! The issue's impact case, a parameter list `key IN (?1, ?2, ...)`, seeks too: members take the
//! column's affinity (an integer 5 finds TEXT '5', REAL 2.5 and text '1' on an INTEGER column
//! behave as stock), NULLs and duplicates drop out, and a residual conjunct still filters.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE r9_t (id TEXT PRIMARY KEY, k TEXT NOT NULL, v TEXT)",
    "CREATE INDEX r9_t_k ON r9_t (k)",
    "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 199) \
     INSERT INTO r9_t SELECT 'id' || i, 'k' || (i % 20), 'v' || i FROM n",
    "INSERT INTO r9_t VALUES ('e1', '', 'empty'), ('u1', 'ключ', 'unicode')",
    "INSERT INTO r9_t VALUES ('e5', 5, 'five'), ('e6', '5.0', 'five point zero')",
    "CREATE TABLE c (a INT, b INT, v TEXT)",
    "CREATE INDEX c_ab ON c (a, b)",
    "INSERT INTO c VALUES (1, NULL, 'n1'), (1, 2, 'x'), (2, NULL, 'n2'), (3, 4, 'y'), (1, NULL, 'n3')",
    "INSERT INTO c VALUES (2.5, 7, 'real'), (-1, 0, 'neg'), ('txt', 1, 'text')",
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
    // Lists the literal paths decline: mixed classes, numbers on TEXT, text on INTEGER.
    "SELECT v FROM r9_t WHERE k IN ('k1', 5)",
    "SELECT v FROM r9_t WHERE k IN (5, 5.0, '5')",
    "SELECT v FROM c WHERE a IN ('1', 2.0)",
    "SELECT v FROM c WHERE a IN (-1, 1)",
];

/// GH#497's impact case: `key IN (?1, ?2, ...)`, a batched key lookup. Each member takes the
/// column's affinity; NULLs and duplicates are dropped.
const PARAM_QUERIES: &[(&str, &[SqliteValue])] = &[
    ("SELECT v FROM r9_t WHERE k IN (?1)", &[SqliteValue::Integer(5)]),
    ("SELECT v FROM r9_t WHERE k IN (?1, ?2)", &[SqliteValue::Integer(5), SqliteValue::Float(5.0)]),
    ("SELECT v FROM r9_t WHERE k IN (?1, ?2)", &[SqliteValue::Null, SqliteValue::Integer(5)]),
    ("SELECT v FROM r9_t WHERE k IN (?1, ?1, ?2)", &[SqliteValue::Null, SqliteValue::Null]),
    ("SELECT id, v FROM r9_t WHERE k IN ('k3', ?1) AND v LIKE 'v1%'", &[SqliteValue::Integer(5)]),
    ("SELECT v FROM r9_t WHERE v <> ?1 AND k IN (?2, ?3)", &[SqliteValue::Integer(0), SqliteValue::Integer(5), SqliteValue::Float(5.0)]),
    ("SELECT v FROM c WHERE a IN (?1, ?2)", &[SqliteValue::Integer(1), SqliteValue::Integer(3)]),
    ("SELECT v FROM c WHERE a IN (?1, ?2)", &[SqliteValue::Float(1.0), SqliteValue::Float(2.5)]),
    ("SELECT v FROM c WHERE a IN (?1) AND b IS NULL", &[SqliteValue::Integer(1)]),
    ("SELECT v FROM nc WHERE k IN (?1)", &[SqliteValue::Null]),
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
    let rows = statement
        .query_map(rusqlite::params_from_iter(params), |row| {
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
        let stock = stock_rows(&r, sql, &[]);
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
    // The index is opened and sought, as for `k = 'k1'`: the table (cursor 0) is never
    // rewound. A parameter list rewinds only its probe set.
    for sql in [
        "EXPLAIN SELECT v FROM r9_t WHERE k IN ('k1')",
        "EXPLAIN SELECT v FROM r9_t WHERE k IN ('k1', 'k2')",
        "EXPLAIN SELECT v FROM r9_t WHERE k IN (?1)",
        "EXPLAIN SELECT v FROM r9_t WHERE k IN (?1, ?2, ?3)",
        "EXPLAIN SELECT v FROM c WHERE a IN (?1, ?2)",
    ] {
        let program: Vec<(String, i64)> = f
            .query(sql)
            .await
            .expect("explain")
            .iter()
            .filter_map(|row| match (row.values().get(1), row.values().get(2)) {
                (Some(SqliteValue::Text(op)), Some(SqliteValue::Integer(p1))) => {
                    Some((op.to_string(), *p1))
                }
                _ => None,
            })
            .collect();
        let seeks = program.iter().any(|(op, _)| op == "SeekGE");
        let rewinds_table = program.iter().any(|(op, p1)| op == "Rewind" && *p1 == 0);
        if !seeks || rewinds_table {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` does not seek the index: {program:?}"
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
