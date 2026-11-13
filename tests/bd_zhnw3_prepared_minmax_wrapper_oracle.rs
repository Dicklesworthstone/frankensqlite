#![recursion_limit = "512"]

//! bd-zhnw3: a prepared whole-table aggregate whose bare column sits inside a
//! min()/max() call's wrapper expression (`max(a) || ':' || b`) read that
//! column as NULL, and a prepared DISTINCT min()/max() took its bare columns
//! from the first row instead of the extremum row. Ad hoc execution was
//! already right. bd-6lijo (69c5554e9) routes these prepared shapes to the
//! same interpreter as ad hoc; this keeps them there. Every shape is compared
//! with rusqlite (bundled) ad hoc, prepared, and with bound parameters, in
//! memory and file-backed.
//!
//! Not covered: a DISTINCT duplicate right after the extremum row, which
//! supplies the bare columns in stock (its skip register keeps the previous
//! row's value); fsqlite keeps the first row there, ad hoc and prepared alike
//! (bd-d4lv6).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE t(a, b, c)",
    "INSERT INTO t VALUES (3,'b1',1),(9,'b6',2),(1,'b2',1),(9,'b7',2),(NULL,'bn',3),(5,'b5',3),(2,'b3',1)",
    "CREATE TABLE e(a, b)",
];

const QUERIES: &[&str] = &[
    "SELECT max(a) || ':' || b FROM t",
    "SELECT min(a) || ':' || b FROM t",
    "SELECT b || max(a) FROM t WHERE c < 3",
    "SELECT min(a) + 0, b FROM t",
    "SELECT coalesce(max(a), -1) AS m, b FROM t",
    "SELECT max(a) || ':' || b, c FROM t WHERE c <> 2",
    "SELECT max(a) || ':' || b FROM e",
    "SELECT max(DISTINCT a), b FROM t",
    "SELECT min(DISTINCT c), b FROM t",
    "SELECT max(DISTINCT a) || b FROM t",
    "SELECT min(DISTINCT a), b, c FROM t WHERE a > 1",
];

const PARAM_QUERIES: &[(&str, &[SqliteValue])] = &[
    ("SELECT max(a) || ':' || b FROM t WHERE c = ?1", &[SqliteValue::Integer(1)]),
    ("SELECT max(a) + ?1, b FROM t", &[SqliteValue::Integer(100)]),
    ("SELECT max(DISTINCT a), b FROM t WHERE c <= ?1", &[SqliteValue::Integer(1)]),
];

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

async fn run(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("zhnw3.db").to_str().expect("utf-8 path").to_owned()
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
    let queries = QUERIES
        .iter()
        .map(|sql| (*sql, &[][..]))
        .chain(PARAM_QUERIES.iter().copied());
    for (sql, params) in queries {
        let stock = stock_rows(&r, sql, params);
        let direct = f
            .query_with_params(sql, params)
            .await
            .map(|rows| frank_rows(&rows));
        if direct.as_ref().ok() != Some(&stock) {
            failures.push(format!(
                "[file_backed={file_backed}] `{sql}` {params:?} (ad hoc): frank {direct:?} vs \
                 stock {stock:?}"
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
                "[file_backed={file_backed}] `{sql}` {params:?} (prepared): frank {prepared:?} \
                 vs stock {stock:?}"
            ));
        }
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn prepared_minmax_wrapper_and_distinct_bare_columns_match_stock() {
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
