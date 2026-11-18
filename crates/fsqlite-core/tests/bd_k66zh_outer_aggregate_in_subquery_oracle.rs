#![recursion_limit = "512"]

//! bd-k66zh: an aggregate call inside a subquery whose arguments reference
//! only an enclosing query's columns belongs to that enclosing query (SQLite's
//! resolve.c, TK_AGG_FUNCTION). It makes the enclosing query an aggregate and
//! is computed over that query's rows or groups; the subquery reads its value.
//! The owner is the innermost query whose FROM one of the call's columns names,
//! walking outward from the call. A call that names an inner column (alone or
//! beside outer ones) stays inner, and a call that names no column at all
//! (`count(*)`) stays where it is written. fsqlite used to evaluate every such
//! call inside the subquery, once per outer row: `SELECT (SELECT sum(n)) FROM
//! g` returned three rows (1, 2, 3) where stock returns one (6).
//!
//! The negative cases are the ones a naive "hoist every aggregate that mentions
//! an outer column" fix gets wrong: a call over the subquery's own columns, a
//! call mixing inner and outer columns, a doubly nested call that belongs to the
//! middle query, a window call, and the positions where stock rejects the call
//! (WHERE, ON, GROUP BY, the ORDER BY of a query that is not an aggregate, a
//! non-aggregate subquery's WHERE, inside another aggregate's argument, beside a
//! window function).
//!
//! Every statement is compared, rows in order, with rusqlite's bundled SQLite
//! 3.54.1, in memory and in a file, ad hoc and prepared. An error must be an
//! error on both sides, with stock's message inside fsqlite's.

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

/// `g` and `p` are the bead's tables. In `m` the max(n) row is neither the
/// first nor the last, and one n is NULL, so a bare column read from the wrong
/// row, or count(n) read as count(*), shows.
const SCHEMA: &str = "\
CREATE TABLE g(id INTEGER PRIMARY KEY, s TEXT, n INTEGER);\
INSERT INTO g VALUES (1, 'x', 1), (2, 'y', 2), (3, 'z', 3);\
CREATE TABLE p(k TEXT, v INTEGER);\
INSERT INTO p VALUES ('a', 1), ('a', 2), ('b', 3);\
CREATE TABLE m(id INTEGER PRIMARY KEY, s TEXT, n INTEGER);\
INSERT INTO m VALUES (1, 'x', 2), (2, 'y', 3), (3, 'z', 1), (4, 'w', NULL);\
CREATE VIEW vw AS SELECT (SELECT sum(n)) AS x FROM g;";

const STATEMENTS: &[&str] = &[
    // The bead's six shapes.
    "SELECT (SELECT sum(n)) FROM g",
    "SELECT (SELECT sum(n) FROM (SELECT 1)) FROM g",
    "SELECT (SELECT count(*) FROM json_each(json_group_array(s))) FROM g",
    "SELECT (SELECT count(v)) FROM p",
    "SELECT k, (SELECT max(v)) FROM p GROUP BY k ORDER BY k",
    "SELECT k FROM p GROUP BY k HAVING (SELECT sum(v)) > 2 ORDER BY k",
    // A call over the subquery's own columns stays inner.
    "SELECT id, (SELECT max(n) FROM g AS h WHERE h.id = g.id) FROM g ORDER BY id",
    "SELECT (SELECT sum(n) FROM g AS a) FROM g AS b",
    "SELECT (SELECT count(*)) FROM g",
    // A call mixing inner and outer columns stays inner.
    "SELECT id, (SELECT max(h.n + g.n) FROM g AS h) FROM g ORDER BY id",
    "SELECT id, (SELECT sum(h.n) FROM g AS h WHERE h.n <= g.n) FROM g ORDER BY id",
    // Doubly nested: the innermost query whose FROM the call names owns it.
    "SELECT (SELECT (SELECT sum(g.n)) FROM p) FROM g",
    "SELECT (SELECT (SELECT sum(p.v)) FROM p) FROM g",
    "SELECT (SELECT (SELECT sum(g.n) FROM p AS p2) FROM p LIMIT 1) FROM g",
    "SELECT k, (SELECT (SELECT sum(v + g.n)) FROM g) FROM p GROUP BY k ORDER BY k",
    "SELECT k, (SELECT (SELECT sum(v)) FROM g) FROM p GROUP BY k ORDER BY k",
    // A window call is never moved.
    "SELECT (SELECT sum(n) OVER ()) FROM g",
    // More owner shapes.
    "SELECT (SELECT sum(n) FROM p) FROM g",
    "SELECT (SELECT sum(n) FROM p WHERE 0) FROM g",
    "SELECT id, (SELECT sum(n) + id) FROM g",
    "SELECT (SELECT sum(n)), count(*) FROM g",
    "SELECT (SELECT sum(n) FILTER (WHERE n > 1)) FROM g",
    "SELECT (SELECT group_concat(s, '-')) FROM g",
    "SELECT (SELECT max(n)) FROM g WHERE n < 3",
    "SELECT (SELECT sum(n)) FROM g WHERE 0",
    "SELECT (SELECT sum(n) FROM p GROUP BY k ORDER BY k) FROM g",
    "SELECT (SELECT count(*) FROM p WHERE v < sum(g.n)) FROM g",
    "SELECT (SELECT sum(n)) FROM g GROUP BY s ORDER BY s",
    "SELECT EXISTS (SELECT sum(n)) FROM g",
    "SELECT 6 IN (SELECT sum(n)) FROM g",
    "SELECT count(*) FROM g ORDER BY (SELECT sum(n))",
    "SELECT (SELECT x FROM (SELECT sum(n) AS x)) FROM g",
    "SELECT (SELECT sum(n) FROM p AS q WHERE q.v = g.n) FROM g",
    "SELECT k, (SELECT sum(v) + v) FROM p GROUP BY k ORDER BY k",
    "SELECT (SELECT sum(n)) AS t FROM g ORDER BY t",
    "SELECT x FROM (SELECT (SELECT sum(n)) AS x FROM g)",
    "SELECT x FROM (SELECT (SELECT sum(n)) AS x FROM g) WHERE x > 0",
    "SELECT DISTINCT (SELECT sum(n) > 0) FROM g GROUP BY s",
    "SELECT (SELECT sum(g.n) FROM g AS h) FROM g",
    // A moved min()/max() picks the row bare columns are read from.
    "SELECT (SELECT max(n)), s FROM m",
    "SELECT (SELECT max(n)), (SELECT s) FROM m",
    "SELECT (SELECT max(n) || s) FROM m",
    "SELECT k, v, (SELECT max(v)) FROM p GROUP BY k ORDER BY k",
    "SELECT (SELECT max(n)), s FROM m WHERE n IS NOT NULL",
    "SELECT (SELECT min(n)), (SELECT s || id) FROM m",
    // Calls in the subquery's own clauses.
    "SELECT (SELECT sum(n) ORDER BY 1) FROM g",
    "SELECT (SELECT k FROM p ORDER BY sum(g.n)) FROM g",
    "SELECT (SELECT count(*) FROM p GROUP BY sum(g.n)) FROM g",
    "SELECT (SELECT k FROM p GROUP BY k HAVING sum(g.n) > 5 ORDER BY k) FROM g",
    // Other aggregates and sources.
    "SELECT (SELECT avg(n)), (SELECT min(s)), (SELECT total(id)) FROM m",
    "SELECT (SELECT count(n)), (SELECT count(*)) FROM m",
    "SELECT (SELECT sum(DISTINCT n % 2)) FROM m",
    "SELECT (SELECT sum(a.n * b.n)) FROM g AS a, g AS b",
    "SELECT (SELECT sum(n)) FROM g AS a JOIN p ON a.id = 1",
    "SELECT k, (SELECT group_concat(v)) FROM p GROUP BY k ORDER BY k",
    "SELECT (SELECT sum(n) FROM g AS h WHERE h.id = (SELECT max(g.id))) FROM g",
    "SELECT (SELECT json_group_array(s)) FROM g",
    "SELECT count(*) FROM g HAVING (SELECT sum(n)) > 5",
    "SELECT count(*) FROM g HAVING (SELECT sum(n)) > 6",
    "WITH c AS (SELECT (SELECT sum(n)) AS x FROM g) SELECT x FROM c",
    "SELECT x FROM vw",
    "SELECT (SELECT sum(n)) FROM g UNION ALL SELECT (SELECT count(v)) FROM p",
    "SELECT k, (SELECT count(*) FROM p AS q WHERE q.k = p.k), (SELECT count(*)) \
     FROM p GROUP BY k ORDER BY k",
    "SELECT k, (SELECT count(*) FROM g WHERE g.n < max(p.v)) FROM p GROUP BY k ORDER BY k",
    "SELECT count(*) FROM g GROUP BY s HAVING (SELECT max(n)) > 1",
    "SELECT * FROM g WHERE n > (SELECT avg(v) FROM p)",
    // bd-p42bj: an owned call whose arguments hold subqueries over the owning
    // core's row, evaluated per row of the group.
    "SELECT (SELECT sum((SELECT n))) FROM g",
    "SELECT (SELECT count((SELECT n + 1))), (SELECT max((SELECT n * 2))) FROM g",
    "SELECT (SELECT sum((SELECT n) + 1)) FROM g",
    "SELECT (SELECT group_concat((SELECT s), '-')) FROM g",
    "SELECT n % 2, (SELECT sum((SELECT n))) FROM g GROUP BY n % 2 ORDER BY 1",
    "SELECT k FROM p GROUP BY k HAVING (SELECT sum((SELECT v))) > 2 ORDER BY k",
    "SELECT (SELECT sum((SELECT count(*) FROM p WHERE p.v <= g.n))) FROM g",
    "SELECT (SELECT sum(EXISTS (SELECT 1 FROM p WHERE p.v = g.n * 2))) FROM g",
    "SELECT (SELECT sum(n IN (SELECT v FROM p WHERE k = 'a'))) FROM g",
    "SELECT (SELECT (SELECT max((SELECT n * 10)))) FROM g",
    "SELECT (SELECT count(DISTINCT (SELECT n % 2))) FROM g",
    "SELECT (SELECT sum((SELECT n))) FROM g WHERE n > 5",
    "SELECT k, (SELECT sum((SELECT v)) + count(*)) FROM p GROUP BY k ORDER BY k",
    "SELECT (SELECT sum((SELECT n FROM g AS h WHERE h.id = g.id))) FROM g",
    // ... and calls that stay the subquery's own: one per outer row.
    "SELECT (SELECT total((SELECT NULL))) FROM g",
    "SELECT (SELECT sum((SELECT n)) FROM g AS h) FROM g",
    // Positions stock rejects.
    "SELECT count(*) FROM g WHERE (SELECT sum(n)) > 1",
    "SELECT id FROM g ORDER BY (SELECT sum(n))",
    "SELECT count(*) FROM g GROUP BY (SELECT sum(n))",
    "SELECT (SELECT value FROM json_each(json_group_array(s))) FROM g",
    "SELECT k FROM p HAVING (SELECT sum(v)) > 2",
    "SELECT count(*) FROM g JOIN p ON (SELECT sum(n)) > 0",
    "SELECT count(*) FROM g WHERE n IN (SELECT sum(n))",
    "SELECT * FROM g WHERE g.n IN (SELECT max(g.n) FROM p)",
    "SELECT * FROM g WHERE n > (SELECT avg(n) FROM p)",
    "SELECT (SELECT 1 FROM p WHERE sum(g.n) > 0) FROM g",
    "SELECT (SELECT 1 FROM p JOIN p AS q ON sum(g.n) > 0) FROM g",
    "SELECT sum((SELECT sum(n))) FROM g",
    "SELECT (SELECT sum(n)), row_number() OVER () FROM g",
];

/// Statements with bound parameters, and their values.
const STATEMENTS_WITH_PARAMS: &[(&str, &[i64])] = &[
    ("SELECT (SELECT sum(n) + ?1) FROM g", &[10]),
    ("SELECT k, (SELECT max(v) * ?1) FROM p GROUP BY k ORDER BY k", &[3]),
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
    let cases = STATEMENTS
        .iter()
        .map(|sql| (*sql, &[][..]))
        .chain(STATEMENTS_WITH_PARAMS.iter().copied());
    for (sql, params) in cases {
        let values: Vec<SqliteValue> = params.iter().copied().map(SqliteValue::Integer).collect();
        statements += 1;
        let rr = rq(&r, sql, params);
        let adhoc = if values.is_empty() {
            f.query(sql).await
        } else {
            f.query_with_params(sql, &values).await
        }
        .map(|rows| tag_rows(&rows))
        .map_err(|e| e.to_string());
        let prepared = match f.prepare(sql).await {
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
            if !same_outcome(&fr, &rr) {
                diffs.push(format!(
                    "  [{label}, {path}] `{sql}` {params:?}\n     \
                     frank= {fr:?}\n     stock= {rr:?}"
                ));
            }
        }
    }
    f.close().await.unwrap();
    (diffs, statements)
}

/// UPDATE and DELETE are no aggregates: stock rejects a subquery call that
/// reads only the statement's own columns, and leaves the table as it was
/// (run per row, the first DELETE removed every row). A call over the
/// subquery's own rows, a correlated one, and a RETURNING subquery still run.
/// Applied in order to one database; the tables are compared after each.
const DML: &[&str] = &[
    "DELETE FROM g WHERE id = (SELECT max(id))",
    "UPDATE g SET n = (SELECT sum(n)) WHERE id = 1",
    "DELETE FROM g WHERE EXISTS (SELECT 1 FROM p WHERE max(g.id) > 0)",
    "DELETE FROM g WHERE EXISTS (SELECT count(*) FROM p HAVING max(g.id) > 2)",
    "UPDATE g SET n = n + 1 WHERE n < (SELECT count(*) FROM p WHERE v < max(g.n))",
    "UPDATE g SET n = 0 FROM p WHERE p.v = (SELECT max(g.n)) AND p.k = 'b'",
    "DELETE FROM g WHERE id = 1 RETURNING (SELECT max(id))",
    "DELETE FROM g WHERE id = (SELECT max(id) FROM g AS h)",
    "UPDATE g SET n = (SELECT max(v) FROM p) WHERE id = 2",
    "UPDATE g SET s = (SELECT count(*) FROM p WHERE p.v <= g.n)",
];

async fn compare_dml(file_backed: bool, prepared: bool) -> Vec<String> {
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
    let label = format!(
        "{}, {}",
        if file_backed { "file" } else { "memory" },
        if prepared { "prepared" } else { "ad hoc" }
    );
    let mut diffs = Vec::new();
    for sql in DML {
        let rr = rq(&r, sql, &[]);
        // A prepared DML statement without RETURNING runs through execute().
        let fr = if prepared {
            match f.prepare(sql).await {
                Ok(stmt) if sql.contains("RETURNING") => stmt.query().await.map(|rows| tag_rows(&rows)),
                Ok(stmt) => stmt.execute().await.map(|_| Vec::new()),
                Err(e) => Err(e),
            }
        } else {
            f.query(sql).await.map(|rows| tag_rows(&rows))
        }
        .map_err(|e| e.to_string());
        if !same_outcome(&fr, &rr) {
            diffs.push(format!(
                "  [{label}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"
            ));
        }
        for check in ["SELECT * FROM g ORDER BY id", "SELECT * FROM p ORDER BY k, v"] {
            let fr = f
                .query(check)
                .await
                .map(|rows| tag_rows(&rows))
                .map_err(|e| e.to_string());
            let rr = rq(&r, check, &[]);
            if fr != rr {
                diffs.push(format!(
                    "  [{label}] after `{sql}`: `{check}`\n     frank= {fr:?}\n     stock= {rr:?}"
                ));
            }
        }
    }
    f.close().await.unwrap();
    diffs
}

#[test]
fn outer_column_aggregates_in_dml_subqueries_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                diffs.extend(compare_dml(file_backed, prepared).await);
            }
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

#[test]
fn outer_column_aggregates_in_subqueries_match_stock() {
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
