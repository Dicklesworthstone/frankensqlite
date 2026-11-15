#![recursion_limit = "512"]

//! bd-kj6tg: a subquery whose FROM is a table-valued function must resolve a
//! bare name the function does not define to the enclosing query, as stock
//! does (`(SELECT count(*) FROM json_each(o.j) WHERE value > n)` reads o.n),
//! and must keep every name the function DOES define inner, its hidden
//! columns and rowid included. The outer table here has columns named after
//! those (key, value, id, json, root, arg, schema, start, name), so an
//! inventory that is too small binds them outward, and an opaque one (the
//! old behavior) refuses `no such column` where stock answers.
//!
//! Oracle: rusqlite's bundled SQLite 3.54.1, on `:memory:` and on a file.
//! It has `json_each`, `json_tree` and the pragma functions but not
//! `generate_series`, whose case is checked by hand below against series.c's
//! declaration (`value, start HIDDEN, stop HIDDEN, step HIDDEN`).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SEED: &[&str] = &[
    "CREATE TABLE o(id INTEGER PRIMARY KEY, n INTEGER, j TEXT, key TEXT, value TEXT, json TEXT, \
     root TEXT, arg TEXT, schema TEXT, start INTEGER, name TEXT)",
    r#"INSERT INTO o VALUES (1, 2, '[10,20,30]', 'ok1', 'ov1', 'oj1', 'or1', 'oa1', 'os1', 100, 'on1'), (2, 3, '{"a":1,"b":2}', 'ok2', 'ov2', 'oj2', 'or2', 'oa2', 'os2', 200, 'on2')"#,
    "CREATE TABLE p(x INTEGER PRIMARY KEY, y TEXT)",
    "CREATE INDEX pi ON p(y)",
    "INSERT INTO p VALUES (1, 'p1'), (2, 'p2')",
];

const QUERIES: &[&str] = &[
    // A bare outer name in WHERE, in the select list, and in an aggregate.
    "SELECT id, (SELECT count(*) FROM json_each(o.j) WHERE value > n) FROM o ORDER BY id",
    "SELECT id, (SELECT max(value) || name FROM json_each(o.j)) FROM o ORDER BY id",
    "SELECT id, (SELECT n + count(*) FROM json_tree(o.j)) FROM o ORDER BY id",
    "SELECT id, (SELECT group_concat(fullkey) FROM json_tree(o.j) \
     WHERE parent IS NOT NULL AND n > 1) FROM o ORDER BY id",
    // The function's own columns stay inner: id, key and value are json_each's.
    "SELECT id, (SELECT group_concat(key) FROM json_each(o.j) WHERE id >= 0) FROM o ORDER BY id",
    "SELECT id, (SELECT group_concat(value) FROM json_each(o.j) WHERE key IS NOT NULL) \
     FROM o ORDER BY id",
    "SELECT id, (SELECT count(*) FROM json_each(o.j) WHERE type = 'integer' AND n > 0) \
     FROM o ORDER BY id",
    // Aliased, and joined to a table on either side.
    "SELECT id, (SELECT count(*) FROM json_each(o.j) AS je WHERE je.value > n) FROM o ORDER BY id",
    "SELECT id, (SELECT count(*) FROM p JOIN json_each(o.j) ON p.x = json_each.key + 1 \
     WHERE n > 1) FROM o ORDER BY id",
    "SELECT id, (SELECT count(*) FROM json_each(o.j), p WHERE y = 'p1' AND name = 'on1') \
     FROM o ORDER BY id",
    "SELECT id, (SELECT x FROM p, json_each(o.j) WHERE n > 100) FROM o ORDER BY id",
    // EXISTS, and an IN subquery in WHERE.
    "SELECT id, EXISTS (SELECT 1 FROM json_each(o.j) WHERE value = n * 10) FROM o ORDER BY id",
    "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM json_each(o.j) WHERE value = n * 10) \
     ORDER BY id",
    "SELECT id FROM o WHERE n IN (SELECT value FROM json_each('[2,5]') WHERE j IS NOT NULL) \
     ORDER BY id",
    // Pragma functions: their columns stay inner, bare outer names resolve out.
    "SELECT id, (SELECT group_concat(name) FROM pragma_table_info('p') WHERE cid < n) \
     FROM o ORDER BY id",
    "SELECT id, (SELECT group_concat(name) FROM pragma_table_info('p') WHERE start > 0) \
     FROM o ORDER BY id",
    "SELECT id, (SELECT count(*) FROM pragma_index_list('p') WHERE seq < n) FROM o ORDER BY id",
    "SELECT id, (SELECT group_concat(cid || name, ',') FROM pragma_index_info('pi') WHERE n > 0) \
     FROM o ORDER BY id",
    // Pragma functions without arg/schema columns: those names are the outer ones.
    "SELECT id, (SELECT arg FROM pragma_database_list) FROM o ORDER BY id",
    "SELECT id, (SELECT schema FROM pragma_database_list) FROM o ORDER BY id",
    "SELECT id, (SELECT arg || name FROM pragma_compile_options LIMIT 1) FROM o ORDER BY id",
    // `*` over the function does not carry its hidden columns: a derived
    // table's json is the outer one, and so is a value it does not project.
    "SELECT id, (SELECT count(*) FROM (SELECT * FROM json_each(o.j)) WHERE json IS NULL) \
     FROM o ORDER BY id",
    "SELECT id, (SELECT count(*) FROM (SELECT * FROM json_each(o.j)) WHERE value > n) \
     FROM o ORDER BY id",
    "SELECT id, (SELECT count(*) FROM (SELECT key FROM json_each(o.j)) WHERE value IS NOT NULL) \
     FROM o ORDER BY id",
];

/// A function's HIDDEN columns stay inner too: stock reads json_each's `json`
/// and `root` and pragma_table_info's `arg` and `schema` here, never the outer
/// table's columns of those names. FrankenSQLite does not implement every
/// hidden column yet, so it may refuse "no such column: <that name>"; what
/// it must never do is bind the outer column (`'oj1'`, `'oa1'`, ...), which an
/// inventory of the visible columns alone would.
const HIDDEN: &[(&str, &str)] = &[
    (
        "SELECT id, (SELECT group_concat(json) FROM json_each(o.j)) FROM o ORDER BY id",
        "json",
    ),
    (
        "SELECT id, (SELECT group_concat(key || ':' || value) FROM json_each(o.j) \
         WHERE root = '$') FROM o ORDER BY id",
        "root",
    ),
    (
        "SELECT id, (SELECT group_concat(root) FROM json_tree(o.j) WHERE id = 0 OR n = 0) \
         FROM o ORDER BY id",
        "root",
    ),
    (
        "SELECT id, (SELECT group_concat(arg) FROM pragma_table_info('p')) FROM o ORDER BY id",
        "arg",
    ),
    (
        "SELECT id, (SELECT group_concat(schema) FROM pragma_table_info('p')) FROM o ORDER BY id",
        "schema",
    ),
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}r"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}r"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock rejects `{sql}`: {e}"));
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<std::result::Result<Vec<_>, _>>()
    .unwrap()
}

async fn fsqlite_outcome(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    fsqlite_outcome(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite rejects `{sql}`: {e}"))
}

async fn compare_all(label: &str, conn: &Connection, stock: &rusqlite::Connection) {
    for sql in SEED {
        conn.execute(sql).await.expect("seed fsqlite");
        stock.execute_batch(sql).expect("seed stock");
    }
    let mut diffs = Vec::new();
    for sql in QUERIES {
        let expected = stock_rows(stock, sql);
        assert!(!expected.is_empty(), "the oracle returns rows for `{sql}`");
        let ours = fsqlite_outcome(conn, sql).await;
        if ours.as_ref() != Ok(&expected) {
            diffs.push(format!(
                "  [{label}] `{sql}`\n     frank= {ours:?}\n     stock= {expected:?}"
            ));
        }
    }
    for (sql, hidden) in HIDDEN {
        let expected = stock_rows(stock, sql);
        assert!(!expected.is_empty(), "the oracle returns rows for `{sql}`");
        let ours = fsqlite_outcome(conn, sql).await;
        let refused_on_the_hidden_column = ours
            .as_ref()
            .is_err_and(|e| e.contains("no such column") && e.contains(hidden));
        if ours.as_ref() != Ok(&expected) && !refused_on_the_hidden_column {
            diffs.push(format!(
                "  [{label}] `{sql}` (hidden `{hidden}`)\n     frank= {ours:?}\n     stock= {expected:?}"
            ));
        }
    }
    assert!(
        diffs.is_empty(),
        "{} divergence(s) from rusqlite:\n{}",
        diffs.len(),
        diffs.join("\n")
    );
}

#[test]
fn table_function_subquery_reads_outer_columns_like_stock_in_memory() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open fsqlite");
        let stock = rusqlite::Connection::open_in_memory().expect("open stock");
        compare_all("memory", &conn, &stock).await;
    });
}

#[test]
fn table_function_subquery_reads_outer_columns_like_stock_on_file() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let ours = dir.path().join("fsqlite.db");
        let theirs = dir.path().join("stock.db");
        let conn = Connection::open(ours.to_str().expect("utf-8 temp path"))
            .await
            .expect("open fsqlite file");
        let stock = rusqlite::Connection::open(&theirs).expect("open stock file");
        compare_all("file", &conn, &stock).await;
        conn.close().await.expect("close fsqlite");
    });
}

/// generate_series has no oracle in the bundled 3.54.1. series.c declares
/// `value, start HIDDEN, stop HIDDEN, step HIDDEN`, so `start` and `step` are
/// the function's own (1 and 1 here), and `n` is the outer column.
#[test]
fn generate_series_subquery_keeps_its_hidden_columns_inner() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open fsqlite");
        for sql in SEED {
            conn.execute(sql).await.expect("seed fsqlite");
        }
        let cases: &[(&str, &[&[&str]])] = &[
            (
                "SELECT id, (SELECT count(*) FROM generate_series(1, 5) WHERE value <= n) \
                 FROM o ORDER BY id",
                &[&["1", "2"], &["2", "3"]],
            ),
            (
                "SELECT id, (SELECT group_concat(start) FROM generate_series(1, 2)) \
                 FROM o ORDER BY id",
                &[&["1", "'1,1'"], &["2", "'1,1'"]],
            ),
            (
                "SELECT id, (SELECT group_concat(step) FROM generate_series(1, 2) WHERE n > 0) \
                 FROM o ORDER BY id",
                &[&["1", "'1,1'"], &["2", "'1,1'"]],
            ),
        ];
        for (sql, expected) in cases {
            let expected: Vec<Vec<String>> = expected
                .iter()
                .map(|row| row.iter().map(|cell| (*cell).to_owned()).collect())
                .collect();
            assert_eq!(fsqlite_rows(&conn, sql).await, expected, "`{sql}`");
        }
    });
}
