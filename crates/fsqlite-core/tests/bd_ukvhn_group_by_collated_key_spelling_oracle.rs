#![recursion_limit = "512"]

//! bd-ukvhn: a GROUP BY key over a NOCASE or RTRIM column has several
//! spellings within one group (`'Name-0'`, `'name-0'`, `'NAME-0'`). Stock
//! prints the spelling of the row its bare columns come from: the row of the
//! group's single min()/max() when the query has one (SQLite copies the
//! accumulator columns, the key among them, only on a row where min()/max()
//! does not skip), otherwise the group's first row. fsqlite printed the
//! first row's spelling even beside a min()/max(), because the bare-column
//! row choice counted a GROUP BY key as no bare column at all.
//!
//! Both input orders are covered (`f` and `w` hold the same rows in opposite
//! rowid order), with a min()/max() and without one, FILTER and DISTINCT
//! forms, an expression of the key, a key with an explicit COLLATE, the
//! grouped join path, and the negative cases: DISTINCT keeps the first
//! spelling, a query without min()/max() keeps the first row, and a BINARY key
//! has one spelling.
//!
//! Every statement is compared, rows in order, with rusqlite's bundled SQLite
//! 3.54.1, in memory and in a file, ad hoc and prepared. In memory with
//! parity-cert strict on, a statement must answer as stock or be refused:
//! strict mode refuses the grouped evaluator these keys run on.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

type Rows = Result<Vec<Vec<String>>, String>;

fn tag_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}

fn stock_err(e: rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqlInputError { msg, .. }
        | rusqlite::Error::SqliteFailure(_, Some(msg)) => msg,
        other => other.to_string(),
    }
}

fn rq(conn: &rusqlite::Connection, sql: &str) -> Rows {
    let mut st = conn.prepare(sql).map_err(stock_err)?;
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(stock_err)?
    .collect::<Result<Vec<_>, _>>()
    .map_err(stock_err)
}

/// `f` and `w` hold the same rows, `w` in the opposite rowid order. Within each
/// NOCASE group of `s` (and RTRIM group of `r`) the spellings differ, and the
/// min(n) and max(n) rows are neither always first nor always last.
const SCHEMA: &str = "\
CREATE TABLE f(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE, n INTEGER, r TEXT COLLATE RTRIM, b TEXT);\
INSERT INTO f VALUES (1, 'Name-0', 1, 'a ', 'p'), (2, 'name-0', 3, 'a', 'q'), \
  (3, 'NAME-0', 2, 'a  ', 'r'), (4, 'b', 4, 'z', 's'), (5, 'B', 5, 'z ', 't'), \
  (6, 'x', 7, 'y', 'u'), (7, 'X', 6, 'y ', 'v');\
CREATE TABLE w(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE, n INTEGER, r TEXT COLLATE RTRIM, b TEXT);\
INSERT INTO w SELECT 8 - id, s, n, r, b FROM f;\
CREATE TABLE j(k TEXT, x INTEGER);\
INSERT INTO j VALUES ('NAME-0', 10), ('b', 20), ('X', 30);";

/// `@T@` is replaced by `f` and by `w`.
const STATEMENTS: &[&str] = &[
    "SELECT s, count(*), max(n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, count(*), min(n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, max(n), b FROM @T@ GROUP BY s ORDER BY s",
    "SELECT max(n), s FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s || '!', max(n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT '[' || r || ']', max(n) FROM @T@ GROUP BY r ORDER BY r",
    "SELECT '[' || r || ']', min(n), b FROM @T@ GROUP BY r ORDER BY r",
    "SELECT s, max(n) FROM @T@ WHERE n > 1 GROUP BY s ORDER BY s",
    "SELECT s, max(n) FROM @T@ GROUP BY s HAVING max(n) > 0 ORDER BY s",
    "SELECT s FROM @T@ GROUP BY s HAVING min(n) > 0 ORDER BY s",
    "SELECT s, max(n) FILTER (WHERE n < 3) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, max(DISTINCT n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s COLLATE NOCASE, max(n) FROM @T@ GROUP BY s COLLATE NOCASE ORDER BY 1",
    "SELECT b, max(n) FROM @T@ GROUP BY b COLLATE NOCASE ORDER BY 1",
    "SELECT s, max(n) FROM @T@ GROUP BY s ORDER BY max(n)",
    "SELECT s, max(n), max(n) + 1 FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, r, max(n) FROM @T@ GROUP BY s, r ORDER BY s, r",
    "SELECT @T@.s, max(@T@.n) FROM @T@ JOIN j ON j.k = @T@.s GROUP BY @T@.s ORDER BY 2",
    "SELECT @T@.s, max(j.x) FROM @T@ JOIN j ON j.k = @T@.s GROUP BY @T@.s ORDER BY 2",
    // Without a min()/max(), or with two, the key keeps the first row.
    "SELECT s, count(*) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, sum(n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, b FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, n FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, max(n), min(n) FROM @T@ GROUP BY s ORDER BY s",
    // DISTINCT keeps the first spelling; a BINARY key has one spelling.
    "SELECT DISTINCT s FROM @T@ ORDER BY s",
    "SELECT DISTINCT r FROM @T@ ORDER BY r",
    "SELECT b, max(n) FROM @T@ GROUP BY b ORDER BY b",
];

fn same_outcome(frank: &Rows, stock: &Rows) -> bool {
    match (frank, stock) {
        (Ok(f), Ok(s)) => f == s,
        (Err(f), Err(s)) => f.contains(s.as_str()),
        _ => false,
    }
}

async fn compare(file_backed: bool, strict: bool) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = if file_backed {
        (
            Connection::open(dir.path().join("f.db").to_str().unwrap())
                .await
                .unwrap(),
            rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
        )
    } else {
        (
            Connection::open(":memory:").await.unwrap(),
            rusqlite::Connection::open_in_memory().unwrap(),
        )
    };
    f.execute_batch(SCHEMA).await.unwrap();
    r.execute_batch(SCHEMA).unwrap();
    if strict {
        f.execute("PRAGMA fsqlite.parity_cert_strict = ON")
            .await
            .unwrap();
    }
    let label = match (file_backed, strict) {
        (true, _) => "file",
        (false, false) => "memory",
        (false, true) => "memory, strict",
    };
    let mut diffs = Vec::new();
    let mut statements = 0usize;
    for template in STATEMENTS {
        for table in ["f", "w"] {
            let sql = template.replace("@T@", table);
            statements += 1;
            let rr = rq(&r, &sql);
            let adhoc = f
                .query(&sql)
                .await
                .map(|rows| tag_rows(&rows))
                .map_err(|e| e.to_string());
            let prepared = match f.prepare(&sql).await {
                Ok(stmt) => stmt
                    .query()
                    .await
                    .map(|rows| tag_rows(&rows))
                    .map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            for (path, fr) in [("ad hoc", adhoc), ("prepared", prepared)] {
                // Strict parity-cert mode refuses the grouped evaluator that
                // collated keys run on; it must answer as stock or refuse.
                let refused = strict
                    && fr.as_ref().is_err_and(|error| {
                        error.contains("in-memory fallback disabled in strict parity-cert mode")
                    });
                if !refused && !same_outcome(&fr, &rr) {
                    diffs.push(format!(
                        "  [{label}, {path}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            }
        }
    }
    f.close().await.unwrap();
    (diffs, statements)
}

#[test]
fn collated_group_by_key_spelling_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        let mut statements = 0usize;
        for (file_backed, strict) in [(false, false), (false, true), (true, false)] {
            let (more, count) = compare(file_backed, strict).await;
            diffs.extend(more);
            statements += count;
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {statements} statement(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
