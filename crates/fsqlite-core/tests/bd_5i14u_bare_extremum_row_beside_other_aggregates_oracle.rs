#![recursion_limit = "512"]

//! bd-5i14u: with a min()/max() in the query, stock takes every bare column
//! (one outside any aggregate) from the row holding the extremum, whatever
//! other aggregates sit beside it: SQLite's min/max step skips the
//! accumulator load on a row that is not a new extremum, and sum(), avg(),
//! group_concat() and the rest never do. fsqlite took the group's first row
//! as soon as any aggregate other than min/max/count was present, on the
//! grouped interpreter paths (single table and join, :memory: and file).
//!
//! The shapes: GROUP BY with sum / avg / total / group_concat /
//! json_group_array / DISTINCT / FILTER beside the extremum, a bare column in
//! a mixed expression, HAVING and ORDER BY reading a bare column, a scalar
//! subquery in the output, a FROM subquery, the grouped join, and whole-table
//! queries. The negatives a naive fix fails:
//! - group_concat / json_group_array order: the fix must not reorder rows to
//!   put the extremum row first (it did that before, for count only);
//! - two extremum aggregates: the last one decides;
//! - no extremum aggregate: the first row;
//! - NULLs: a NULL row before the first value supplies the bare columns, so
//!   an all-NULL group reports its LAST row; ties keep the first extremum row.
//!
//! `g` and `w` hold the same rows, `w` in the opposite rowid order. Every
//! statement is compared, rows in order, with rusqlite's bundled SQLite
//! 3.54.1, in memory and in a file, ad hoc and prepared.

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

/// Within groups a and b the max(n) row is neither first nor last, and its
/// `s` sorts the groups the other way round from their first rows' `s`
/// (`ORDER BY s` and `HAVING s LIKE 'm%'` tell them apart). Group c holds a
/// NULL before two tied 7s; group d is all NULL. `r` makes REAL sums whose
/// value depends on the summation.
const SCHEMA: &str = "\
CREATE TABLE g(id INTEGER PRIMARY KEY, k TEXT, s TEXT, n INTEGER, r REAL);\
INSERT INTO g VALUES (1, 'a', 'zz-first-a', 3, 0.1), (2, 'a', 'mm-max-a', 9, 0.2), \
  (3, 'a', 'yy-last-a', 5, 0.3), (4, 'b', 'aa-first-b', 1, 1e16), \
  (5, 'b', 'pp-max-b', 4, 1.0), (6, 'b', 'bb-last-b', 2, -1e16), \
  (7, 'c', 'cc-null', NULL, NULL), (8, 'c', 'cc-seven-1', 7, 2.5), \
  (9, 'c', 'cc-seven-2', 7, 3.5), (10, 'd', 'dd-null-1', NULL, NULL), \
  (11, 'd', 'dd-null-2', NULL, NULL);\
CREATE TABLE w(id INTEGER PRIMARY KEY, k TEXT, s TEXT, n INTEGER, r REAL);\
INSERT INTO w SELECT 12 - id, k, s, n, r FROM g;\
CREATE TABLE h(id INTEGER PRIMARY KEY, k TEXT, m INTEGER);\
INSERT INTO h VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'c', 40), (5, 'd', 50);";

/// `@T@` is replaced by `g` and by `w`.
const STATEMENTS: &[&str] = &[
    "SELECT k, s, max(n), sum(n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, min(n), avg(n), total(n), count(n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, max(n), group_concat(n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, min(n), json_group_array(n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, max(n), group_concat(s, '/') FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, max(n), sum(r), avg(r) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, max(DISTINCT n), sum(DISTINCT n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, max(n), sum(n) FILTER (WHERE n < 9) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, max(n) FILTER (WHERE n < 9), sum(n) FROM @T@ GROUP BY k ORDER BY k",
    // A bare column inside a mixed expression, beside an order-sensitive
    // aggregate in the same expression.
    "SELECT k, max(n) || ':' || s, sum(n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s || ':' || group_concat(n, '/'), max(n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, max(n), max(n) + sum(n) FROM @T@ GROUP BY k ORDER BY k",
    // HAVING, ORDER BY, a scalar subquery and a FROM subquery read the same row.
    "SELECT k, s, max(n), group_concat(n) FROM @T@ GROUP BY k HAVING s LIKE 'm%' ORDER BY k",
    "SELECT k, max(n), sum(n) FROM @T@ GROUP BY k ORDER BY s",
    "SELECT k, s, max(n), sum(n), (SELECT count(*) FROM h WHERE h.k = @T@.k AND h.m > @T@.n) \
     FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, max(n), sum(n) FROM (SELECT * FROM @T@) GROUP BY k ORDER BY k",
    // The grouped join, with an order-sensitive aggregate over joined rows.
    "SELECT @T@.k, @T@.s, max(@T@.n), sum(h.m) FROM @T@ JOIN h ON @T@.k = h.k \
     GROUP BY @T@.k ORDER BY @T@.k",
    "SELECT @T@.k, @T@.s, max(@T@.n), group_concat(@T@.n || '-' || h.m) FROM @T@ \
     JOIN h ON @T@.k = h.k GROUP BY @T@.k ORDER BY @T@.k",
    "SELECT @T@.k, @T@.s, min(h.m), group_concat(@T@.n) FROM @T@ JOIN h ON @T@.k = h.k \
     GROUP BY @T@.k ORDER BY @T@.k",
    // Whole-table queries.
    "SELECT max(n), sum(n), (SELECT s) FROM @T@",
    "SELECT s, max(n), sum(n) FROM @T@",
    "SELECT s, min(n), group_concat(s) FROM @T@ WHERE n > 1",
    "SELECT @T@.s, max(@T@.n), sum(h.m) FROM @T@ JOIN h ON @T@.k = h.k",
    // Negatives: two extremum aggregates (the last decides), none (first row).
    "SELECT k, s, max(n), min(n), sum(n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, min(n), max(n), group_concat(n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, sum(n), group_concat(n) FROM @T@ GROUP BY k ORDER BY k",
    "SELECT k, s, avg(n) FROM @T@ GROUP BY k ORDER BY k",
];

fn same_outcome(frank: &Rows, stock: &Rows) -> bool {
    match (frank, stock) {
        (Ok(f), Ok(s)) => f == s,
        (Err(f), Err(s)) => f.contains(s.as_str()),
        _ => false,
    }
}

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
    for template in STATEMENTS {
        for table in ["g", "w"] {
            let sql = template.replace("@T@", table);
            statements += 1;
            let rr = rq(&r, &sql);
            assert!(rr.is_ok(), "stock rejects `{sql}`: {rr:?}");
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
                if !same_outcome(&fr, &rr) {
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
fn bare_columns_beside_one_extremum_and_other_aggregates_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        let mut statements = 0usize;
        for file_backed in [false, true] {
            let (more, count) = compare(file_backed).await;
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
