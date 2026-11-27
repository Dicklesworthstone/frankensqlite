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

fn tvf_items(prefix: &str, n: usize) -> String {
    let items: Vec<String> = (1..=n)
        .map(|i| format!("json_each('[1]') AS {prefix}{i}"))
        .collect();
    items.join(", ")
}

const NESTED_TOO_DEEP: &str = "error: VIEWs and/or subqueries nested too deep";

#[test]
fn bd_b8cfq_views_tvf_functions_and_materialized_cte_limits_match_stock() {
    let mut setup = vec![
        "CREATE TABLE o(z)".to_owned(),
        "INSERT INTO o VALUES (1)".to_owned(),
        // View doubling chain: v0 = 1 term, v_k = 2^k flattened terms.
        "CREATE VIEW v0 AS SELECT z AS x FROM o".to_owned(),
    ];
    for i in 1..=8 {
        setup.push(format!(
            "CREATE VIEW v{i} AS SELECT a.x FROM v{p} AS a, v{p} AS b",
            p = i - 1
        ));
    }
    // Non-flattenable view chains (DISTINCT, LEFT JOIN, LIMIT).
    setup.push("CREATE VIEW vd0 AS SELECT z AS x FROM o".to_owned());
    setup.push("CREATE VIEW vj0 AS SELECT z AS x FROM o".to_owned());
    setup.push("CREATE VIEW vl0 AS SELECT z AS x FROM o".to_owned());
    for i in 1..=8 {
        let p = i - 1;
        setup.push(format!(
            "CREATE VIEW vd{i} AS SELECT DISTINCT a.x FROM vd{p} AS a, vd{p} AS b"
        ));
        setup.push(format!(
            "CREATE VIEW vj{i} AS SELECT a.x FROM vj{p} AS a LEFT JOIN vj{p} AS b ON 1"
        ));
        setup.push(format!(
            "CREATE VIEW vl{i} AS SELECT a.x FROM vl{p} AS a, vl{p} AS b LIMIT 5"
        ));
    }
    // Plain wide views and TVF-rooted view chain.
    setup.push(format!("CREATE VIEW v30 AS SELECT 1 AS x FROM {}", items("a", 30)));
    setup.push(format!("CREATE VIEW v40 AS SELECT 1 AS x FROM {}", items("b", 40)));
    setup.push(format!("CREATE VIEW v63 AS SELECT 1 AS x FROM {}", items("c", 63)));
    setup.push(format!("CREATE VIEW v64 AS SELECT 1 AS x FROM {}", items("d", 64)));
    setup.push(format!("CREATE VIEW v65 AS SELECT 1 AS x FROM {}", items("e", 65)));
    setup.push("CREATE VIEW v_shadow AS SELECT 1 AS x FROM v65".to_owned());
    setup.push("CREATE TEMP VIEW v_shadow AS SELECT 1 AS x FROM o".to_owned());
    setup.push("CREATE VIEW vt0 AS SELECT value AS x FROM json_each('[1]')".to_owned());
    for i in 1..=8 {
        setup.push(format!(
            "CREATE VIEW vt{i} AS SELECT a.x FROM vt{p} AS a, vt{p} AS b",
            p = i - 1
        ));
    }
    // View with scalar function in result column (stock flattens).
    setup.push("CREATE VIEW vf0 AS SELECT z AS x FROM o".to_owned());
    for i in 1..=8 {
        setup.push(format!(
            "CREATE VIEW vf{i} AS SELECT abs(a.x) AS x FROM vf{p} AS a, vf{p} AS b",
            p = i - 1
        ));
    }
    // Attached database with view chains on both sides of the limits.
    setup.push("ATTACH ':memory:' AS aux".to_owned());
    setup.push("CREATE TABLE aux.ao(z)".to_owned());
    setup.push("INSERT INTO aux.ao VALUES (1)".to_owned());
    setup.push("CREATE VIEW aux.av0 AS SELECT z AS x FROM ao".to_owned());
    for i in 1..=8 {
        setup.push(format!(
            "CREATE VIEW aux.av{i} AS SELECT a.x FROM av{p} AS a, av{p} AS b",
            p = i - 1
        ));
    }

    let scalar_fn = |p: usize| format!("SELECT abs(a.x) AS x FROM c{p} AS a, c{p} AS b");
    let agg_fn = |p: usize| format!("SELECT min(a.x) AS x FROM c{p} AS a, c{p} AS b");
    let window_fn = |p: usize| format!("SELECT max(a.x) OVER () AS x FROM c{p} AS a, c{p} AS b");
    let tvf_cte_chain = |levels: usize| {
        let mut ctes = vec!["c0 AS (SELECT value AS x FROM json_each('[1]'))".to_owned()];
        ctes.extend((1..=levels).map(|i| format!("c{i} AS ({})", self_join(i - 1))));
        format!("WITH {} SELECT count(*) FROM c{levels}", ctes.join(", "))
    };
    let mat_base_chain = |levels: usize| {
        let mut ctes = vec!["c0 AS MATERIALIZED (SELECT 1 AS x)".to_owned()];
        ctes.extend((1..=levels).map(|i| format!("c{i} AS ({})", self_join(i - 1))));
        format!("WITH {} SELECT count(*) FROM c{levels}", ctes.join(", "))
    };
    let unmat_into_mat = |levels: usize| {
        let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
        for i in 1..levels {
            ctes.push(format!("c{i} AS ({})", self_join(i - 1)));
        }
        ctes.push(format!(
            "c{levels} AS MATERIALIZED ({})",
            self_join(levels - 1)
        ));
        format!("WITH {} SELECT count(*) FROM c{levels}", ctes.join(", "))
    };

    let statements: Vec<(String, &'static str)> = vec![
        // 1. View chains and wide views on both sides of 64 and 200.
        ("SELECT count(*) FROM v6".to_owned(), ONE),
        ("SELECT count(*) FROM v7".to_owned(), JOIN_64),
        ("SELECT count(*) FROM v8".to_owned(), TERMS_200),
        ("SELECT count(*) FROM vd8".to_owned(), ONE),
        ("SELECT count(*) FROM vj8".to_owned(), ONE),
        ("SELECT count(*) FROM vl8".to_owned(), ONE),
        ("SELECT count(*) FROM v64".to_owned(), ONE),
        ("SELECT count(*) FROM v65".to_owned(), JOIN_64),
        ("SELECT count(*) FROM v40, v30".to_owned(), JOIN_64),
        ("SELECT count(*) FROM o, v63".to_owned(), ONE),
        ("SELECT count(*) FROM o, v64".to_owned(), JOIN_64),
        ("SELECT count(*) FROM o LEFT JOIN v64 ON 1".to_owned(), ONE),
        ("SELECT (SELECT count(*) FROM v65)".to_owned(), JOIN_64),
        // CTE shadows unqualified view; main.v7 still resolves the view.
        (
            "WITH v7 AS (SELECT 1 AS x) SELECT count(*) FROM v7".to_owned(),
            ONE,
        ),
        (
            "WITH v7 AS (SELECT 1 AS x) SELECT count(*) FROM main.v7".to_owned(),
            JOIN_64,
        ),
        // TEMP view shadows MAIN view; main.v_shadow still resolves MAIN.
        ("SELECT count(*) FROM v_shadow".to_owned(), ONE),
        ("SELECT count(*) FROM main.v_shadow".to_owned(), JOIN_64),
        // Attached schema view chains.
        ("SELECT count(*) FROM aux.av6".to_owned(), ONE),
        ("SELECT count(*) FROM aux.av7".to_owned(), JOIN_64),
        ("SELECT count(*) FROM aux.av8".to_owned(), TERMS_200),
        // View referenced through a CTE.
        (
            "WITH c AS (SELECT a.x FROM v5 AS a, v5 AS b) SELECT count(*) FROM c".to_owned(),
            ONE,
        ),
        (
            "WITH c AS (SELECT a.x FROM v5 AS a, v5 AS b, o) SELECT count(*) FROM c".to_owned(),
            JOIN_64,
        ),
        // 2. Scalar vs aggregate vs window functions in result columns.
        (chain(6, "", &scalar_fn), ONE),
        (chain(7, "", &scalar_fn), JOIN_64),
        (chain(8, "", &scalar_fn), TERMS_200),
        (chain(8, "", &agg_fn), ONE),
        (chain(8, "", &window_fn), ONE),
        ("SELECT count(*) FROM vf6".to_owned(), ONE),
        ("SELECT count(*) FROM vf7".to_owned(), JOIN_64),
        ("SELECT count(*) FROM vf8".to_owned(), TERMS_200),
        // 3. Table-valued functions (json_each) directly, in CTEs, in views, and in TVF args.
        (format!("SELECT count(*) FROM {}", tvf_items("j", 64)), ONE),
        (
            format!("SELECT count(*) FROM {}", tvf_items("j", 65)),
            JOIN_64,
        ),
        ("SELECT count(*) FROM json_each('[1]'), v63".to_owned(), ONE),
        (
            "SELECT count(*) FROM json_each('[1]'), v64".to_owned(),
            JOIN_64,
        ),
        (tvf_cte_chain(6), ONE),
        (tvf_cte_chain(7), JOIN_64),
        (tvf_cte_chain(8), TERMS_200),
        ("SELECT count(*) FROM vt6".to_owned(), ONE),
        ("SELECT count(*) FROM vt7".to_owned(), JOIN_64),
        ("SELECT count(*) FROM vt8".to_owned(), TERMS_200),
        (
            format!(
                "SELECT count(*) FROM json_each((SELECT '[1]' FROM {}))",
                items("t", 64)
            ),
            ONE,
        ),
        (
            format!(
                "SELECT count(*) FROM json_each((SELECT '[1]' FROM {}))",
                items("t", 65)
            ),
            JOIN_64,
        ),
        // 4. MATERIALIZED CTE chains on both sides of every limit.
        (chain(6, "MATERIALIZED ", &self_join), ONE),
        (chain(7, "MATERIALIZED ", &self_join), ONE),
        (chain(8, "MATERIALIZED ", &self_join), ONE),
        (chain(9, "MATERIALIZED ", &self_join), NESTED_TOO_DEEP),
        (mat_base_chain(6), ONE),
        (mat_base_chain(7), JOIN_64),
        (mat_base_chain(8), TERMS_200),
        (unmat_into_mat(6), ONE),
        (unmat_into_mat(7), JOIN_64),
        (unmat_into_mat(8), TERMS_200),
        (
            "WITH m AS MATERIALIZED (SELECT a.x FROM v5 AS a, v5 AS b) SELECT count(*) FROM m"
                .to_owned(),
            ONE,
        ),
        (
            "WITH m AS MATERIALIZED (SELECT a.x FROM v5 AS a, v5 AS b, o) SELECT count(*) FROM m"
                .to_owned(),
            JOIN_64,
        ),
        (
            format!(
                "WITH m AS MATERIALIZED (SELECT 1 AS x FROM {}) SELECT count(*) FROM m",
                items("m", 64)
            ),
            ONE,
        ),
        (
            format!(
                "WITH m AS MATERIALIZED (SELECT 1 AS x FROM {}) SELECT count(*) FROM m",
                items("m", 65)
            ),
            JOIN_64,
        ),
        (
            format!(
                "SELECT count(*) FROM (WITH c AS (SELECT 1 AS x FROM {}) SELECT x FROM c), (SELECT 1 AS x FROM {})",
                items("wa", 40),
                items("wb", 30)
            ),
            JOIN_64,
        ),
    ];

    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in &setup {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<String> = statements
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    for ((sql, pinned), stock_gave) in statements.iter().zip(&expected) {
        assert_eq!(stock_gave, pinned, "stock on `{sql}`");
    }

    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path()
                        .join("bd_b8cfq.db")
                        .to_str()
                        .unwrap()
                        .to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in &setup {
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
