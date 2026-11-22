//! bd-pn6az: stock SQLite's flattener splices each subquery and CTE reference
//! it can flatten into the enclosing FROM list, a copy per reference, and
//! refuses a list that passes 200 terms ("too many FROM clause terms, max:
//! 200"); its join planner refuses a SELECT of more than 64 terms ("at most
//! 64 tables in a join"). fsqlite answered both: a CTE joined to itself at 7
//! and 8 levels (128 and 256 terms) and a plain 65-table join returned a row.
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, directly and prepared. Each outcome (its rows, or its error
//! text) must equal stock's, and is pinned to stock's.
//!
//! Negatives a naive count fails: 64 tables still answer; bodies stock does
//! not flatten (DISTINCT, an outer join, LIMIT) count one term, so their
//! 8-level chains answer; a 64-table subquery held by a LEFT JOIN answers.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let result = (|| -> rusqlite::Result<Vec<Vec<String>>> {
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows = Vec::new();
        let mut cursor = statement.query([])?;
        while let Some(row) = cursor.next()? {
            rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect(),
            );
        }
        Ok(rows)
    })();
    match result {
        Ok(rows) => format!("rows={rows:?}"),
        Err(
            rusqlite::Error::SqliteFailure(_, Some(message))
            | rusqlite::Error::SqlInputError { msg: message, .. },
        ) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement) => statement.query().await,
            Err(error) => Err(error),
        }
    } else {
        conn.query(sql).await
    };
    match result {
        Ok(rows) => {
            let rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect();
            format!("rows={rows:?}")
        }
        Err(error) => format!("error: {error}"),
    }
}

/// `WITH c0 AS (SELECT 1 AS x), ci AS (<body of c(i-1)>) ... SELECT count(*) FROM ck`.
fn chain(levels: usize, hint: &str, body: &dyn Fn(usize) -> String) -> String {
    let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
    ctes.extend((1..=levels).map(|i| format!("c{i} AS {hint}({})", body(i - 1))));
    format!("WITH {} SELECT count(*) FROM c{levels}", ctes.join(", "))
}

fn self_join(p: usize) -> String {
    format!("SELECT a.x FROM c{p} AS a, c{p} AS b")
}

/// `n` copies of `o`, comma-joined, aliased `{prefix}1` ...
fn items(prefix: &str, n: usize) -> String {
    let items: Vec<String> = (1..=n).map(|i| format!("o AS {prefix}{i}")).collect();
    items.join(", ")
}

const ONE: &str = "rows=[[\"1\"]]";
const JOIN_64: &str = "error: at most 64 tables in a join";
const TERMS_200: &str = "error: too many FROM clause terms, max: 200";

fn statements() -> Vec<(String, &'static str)> {
    let mut statements: Vec<(String, &'static str)> = vec![
        (chain(5, "", &self_join), ONE),
        (chain(6, "", &self_join), ONE),
        (chain(7, "", &self_join), JOIN_64),
        (chain(8, "", &self_join), TERMS_200),
        (
            chain(9, "", &self_join),
            "error: VIEWs and/or subqueries nested too deep",
        ),
        (format!("SELECT count(*) FROM {}", items("o", 64)), ONE),
        (format!("SELECT count(*) FROM {}", items("o", 65)), JOIN_64),
        (
            format!("SELECT (SELECT count(*) FROM {})", items("o", 65)),
            JOIN_64,
        ),
        (
            format!(
                "SELECT count(*) FROM (SELECT 1 FROM {}), (SELECT 1 FROM {})",
                items("a", 40),
                items("b", 30)
            ),
            JOIN_64,
        ),
        (
            format!(
                "SELECT count(*) FROM o AS z LEFT JOIN (SELECT 1 FROM {}) ON 1",
                items("a", 64)
            ),
            ONE,
        ),
    ];
    let derived = |p: usize| format!("SELECT x FROM (SELECT a.x FROM c{p} AS a, c{p} AS b)");
    let using = |p: usize| format!("SELECT a.x FROM c{p} AS a JOIN c{p} AS b USING (x)");
    let ordered = |p: usize| format!("SELECT a.x FROM c{p} AS a, c{p} AS b ORDER BY 1");
    let distinct = |p: usize| format!("SELECT DISTINCT a.x FROM c{p} AS a, c{p} AS b");
    let left = |p: usize| format!("SELECT a.x FROM c{p} AS a LEFT JOIN c{p} AS b ON 1");
    let limited = |p: usize| format!("SELECT a.x FROM c{p} AS a, c{p} AS b LIMIT 5");
    statements.extend([
        (chain(7, "", &derived), JOIN_64),
        (chain(8, "", &derived), TERMS_200),
        (chain(7, "", &using), JOIN_64),
        (chain(7, "", &ordered), JOIN_64),
        (chain(7, "NOT MATERIALIZED ", &self_join), JOIN_64),
        (chain(8, "", &distinct), ONE),
        (chain(8, "", &left), ONE),
        (chain(8, "", &limited), ONE),
    ]);
    statements
}

#[test]
fn join_term_limits_match_stock() {
    let statements = statements();
    let setup = ["CREATE TABLE o(z)", "INSERT INTO o VALUES (1)"];
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<String> = statements
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    // Not vacuous: stock gives each pinned outcome.
    for ((sql, pinned), stock_gave) in statements.iter().zip(&expected) {
        assert_eq!(stock_gave, pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_pn6az.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in setup {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, _), expected) in statements.iter().zip(&expected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    if &got != expected {
                        mismatches.push(format!(
                            "file_backed={file_backed} prepared={prepared} `{sql}`:\n    \
                             got  {got}\n    want {expected}"
                        ));
                    }
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} outcome(s) differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
