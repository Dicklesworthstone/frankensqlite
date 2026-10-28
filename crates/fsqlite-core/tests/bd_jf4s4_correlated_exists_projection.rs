#![recursion_limit = "512"]

//! bd-jf4s4: a correlated EXISTS outside the WHERE clause (a result column,
//! NOT EXISTS, inside CASE, ORDER BY) compiled to `emit_exists_subquery`,
//! which scans one base table for a row that passes the WHERE. A subquery it
//! cannot answer that way returned a wrong 0 or 1 for every outer row: a
//! join (its tables dropped, their columns NULL), a view, a derived table, a
//! CTE, a compound, a table-valued function, GROUP BY ... HAVING, an OFFSET,
//! or a LIMIT that is not a literal. Those now answer per outer row through
//! the correlated-subquery fallback. The shapes the scan does answer stay
//! compiled, and are compared too. The OFFSET forms (past the end, negative,
//! over a join, bound to a parameter) cover bd-zje20.
//!
//! Every statement is compared, rows in order, with rusqlite's bundled
//! SQLite, in memory and in a file, ad hoc and prepared.

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

/// Stock's error message. For an error found while preparing, rusqlite's
/// Display appends the SQL text and the error offset; keep the message alone.
fn stock_err(e: rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqlInputError { msg, .. }
        | rusqlite::Error::SqliteFailure(_, Some(msg)) => msg,
        other => other.to_string(),
    }
}

fn rq(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> Rows {
    let mut st = conn.prepare(sql).map_err(stock_err)?;
    let n = st.column_count();
    st.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(stock_err)?
    .collect::<Result<Vec<_>, _>>()
    .map_err(stock_err)
}

const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
CREATE INDEX t_g ON t(g);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 60) \
  SELECT i, 'v' || (i % 7), i % 9, i % 5 FROM s;\
INSERT INTO k WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 6) \
  SELECT i, 'k' || i FROM s;\
CREATE VIEW vt AS SELECT id, g FROM t WHERE n > 1;";

/// `@E@` is replaced by each EXISTS subquery below; the outer rows are fixed.
const OUTER: &[&str] = &[
    "SELECT o.id, @E@ FROM t AS o WHERE o.id IN (6, 7, 14, 45)",
    "SELECT o.id, NOT @E@ FROM t AS o WHERE o.id IN (6, 7, 14, 45)",
    "SELECT o.id, CASE WHEN @E@ THEN 'y' ELSE 'n' END FROM t AS o WHERE o.id IN (6, 7, 14, 45)",
    "SELECT o.id FROM t AS o WHERE o.id IN (6, 7, 14, 45) ORDER BY @E@, o.id DESC",
    "SELECT o.id FROM t AS o WHERE o.id IN (6, 7, 14, 45) AND @E@",
];

const EXISTS: &[&str] = &[
    // A join, tested on its joined table, its outer table, or both.
    "EXISTS (SELECT 1 FROM t JOIN k ON t.g = k.g WHERE k.label <> 'k3' AND t.id <= o.id)",
    "EXISTS (SELECT 1 FROM t JOIN k ON t.g = k.g WHERE k.g <= o.id)",
    "EXISTS (SELECT 1 FROM t JOIN k ON t.g = k.g WHERE k.g >= 0 AND t.id = o.id)",
    "EXISTS (SELECT 1 FROM t JOIN k ON t.g = k.g WHERE t.n > 1 AND t.id <= o.id)",
    "EXISTS (SELECT 1 FROM t AS x LEFT JOIN k ON x.g = k.g WHERE k.g IS NULL AND x.id <= o.id)",
    "EXISTS (SELECT 1 FROM k JOIN t ON t.g = k.g WHERE k.label = 'k' || o.n AND t.id > o.id)",
    // OFFSET and LIMIT forms.
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT 1 OFFSET 10)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT -1 OFFSET 6)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT 1 - 1)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT 0)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT 3)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT 1 OFFSET 0)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT 1 OFFSET 100)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT 1 OFFSET -3)",
    "EXISTS (SELECT 1 FROM t JOIN k ON t.g = k.g WHERE t.id <= o.id LIMIT 1 OFFSET 8)",
    // Sources other than one base table.
    "EXISTS (SELECT 1 FROM vt WHERE vt.id = o.id)",
    "EXISTS (SELECT 1 FROM (SELECT id FROM t WHERE n > 1) AS d WHERE d.id = o.id)",
    "EXISTS (WITH c AS (SELECT id FROM t WHERE n > 1) SELECT 1 FROM c WHERE c.id = o.id)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id = o.id + 100 \
     UNION ALL SELECT 1 FROM k WHERE k.g = o.g)",
    "EXISTS (SELECT 1 FROM json_each('[2, 7, 45]') WHERE value = o.id)",
    // Grouping.
    "EXISTS (SELECT x.g FROM t AS x WHERE x.id <= o.id GROUP BY x.g HAVING count(*) > 1)",
    "EXISTS (SELECT count(*) FROM t AS x WHERE x.id > o.id + 100 HAVING count(*) > 0)",
    "EXISTS (SELECT x.g FROM t AS x WHERE x.id <= o.id GROUP BY x.g)",
    // Shapes the compiled scan answers: a rowid probe, an index probe, a scan.
    "EXISTS (SELECT 1 FROM t AS x WHERE x.id = o.id + 1 AND x.n <> o.n)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.g = o.g AND x.id <> o.id)",
    "EXISTS (SELECT 1 FROM t AS x WHERE x.n = o.n AND x.id > o.id + 20)",
    "EXISTS (SELECT 1 FROM k WHERE k.g = o.g)",
];

/// EXISTS subqueries with bound parameters (bd-zje20), and their values.
const EXISTS_WITH_PARAMS: &[(&str, &[i64])] = &[
    (
        "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT 1 OFFSET ?1)",
        &[10],
    ),
    (
        "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT ?1 OFFSET ?2)",
        &[1, -2],
    ),
    (
        "EXISTS (SELECT 1 FROM t AS x WHERE x.id <= o.id LIMIT ?1)",
        &[0],
    ),
    (
        "EXISTS (SELECT 1 FROM t JOIN k ON t.g = k.g WHERE k.g <> ?1 AND t.id <= o.id)",
        &[3],
    ),
    (
        "EXISTS (SELECT 1 FROM t AS x WHERE x.g = o.g AND x.id > o.id + ?1)",
        &[20],
    ),
];

async fn compare(file_backed: bool) -> (Vec<String>, usize) {
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
    let label = if file_backed { "file" } else { "memory" };
    let mut diffs = Vec::new();
    let mut statements = 0usize;
    let cases = EXISTS
        .iter()
        .map(|exists| (*exists, &[][..]))
        .chain(EXISTS_WITH_PARAMS.iter().copied());
    for (exists, params) in cases {
        let values: Vec<SqliteValue> = params.iter().copied().map(SqliteValue::Integer).collect();
        for outer in OUTER {
            let sql = outer.replace("@E@", exists);
            statements += 1;
            let rr = rq(&r, &sql, params);
            let adhoc = if values.is_empty() {
                f.query(&sql).await
            } else {
                f.query_with_params(&sql, &values).await
            }
            .map(|rows| tag_rows(&rows))
            .map_err(|e| e.to_string());
            let prepared = match f.prepare(&sql).await {
                Ok(stmt) => if values.is_empty() {
                    stmt.query().await
                } else {
                    stmt.query_with_params(&values).await
                }
                .map(|rows| tag_rows(&rows))
                .map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            for (path, fr) in [("ad hoc", adhoc), ("prepared", prepared)] {
                if fr != rr {
                    diffs.push(format!(
                        "  [{label}, {path}] `{sql}` {params:?}\n     \
                         frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            }
        }
    }
    f.close().await.unwrap();
    (diffs, statements)
}

#[test]
fn correlated_exists_outside_where_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut statements) = compare(false).await;
        let (file_diffs, file_statements) = compare(true).await;
        diffs.extend(file_diffs);
        statements += file_statements;
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {statements} statement(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
