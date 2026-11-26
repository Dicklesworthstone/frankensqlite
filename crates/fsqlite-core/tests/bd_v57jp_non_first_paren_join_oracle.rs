//! bd-v57jp: a parenthesized join as a non-first FROM item must execute with
//! stock SQLite 3.54.1 semantics (materialized nested FROM subquery rather than
//! flattening), model stock's extra cursor in the 1000-cursor CTE expansion
//! budget, and report stock's recursive-reference errors (`circular reference` /
//! `multiple recursive references`) when a recursive CTE self-reference sits
//! inside a non-first parenthesized join.
//!
//! Every query runs on live bundled SQLite 3.54.1 (`rusqlite`) and on fsqlite,
//! on `:memory:` and on a file-backed database, executed directly and prepared.
//! Negative control: leading (first-item) parenthesized joins, including a
//! recursive CTE arm that opens with a parenthesized join, continue to answer
//! identically to stock.

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
    let result = (|| -> rusqlite::Result<(Vec<String>, Vec<Vec<String>>)> {
        let mut statement = conn.prepare(sql)?;
        let col_names: Vec<String> = statement.column_names().map(str::to_owned).collect();
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
        Ok((col_names, rows))
    })();
    match result {
        Ok((col_names, rows)) => format!("cols={col_names:?} rows={rows:?}"),
        Err(
            rusqlite::Error::SqliteFailure(_, Some(message))
            | rusqlite::Error::SqlInputError { msg: message, .. },
        ) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

fn stock_rows_only(conn: &rusqlite::Connection, sql: &str) -> String {
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

async fn fsqlite_rows_only(conn: &Connection, sql: &str, prepared: bool) -> String {
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

const SETUP_DDL: &[&str] = &[
    "CREATE TABLE o(z INTEGER)",
    "INSERT INTO o VALUES (1), (2)",
    "CREATE TABLE p(y INTEGER, z INTEGER)",
    "INSERT INTO p VALUES (10, 1), (20, 2)",
    "CREATE TABLE q(y INTEGER, w TEXT)",
    "INSERT INTO q VALUES (10, 'a'), (30, 'b')",
    "CREATE TABLE r(w TEXT, tag TEXT)",
    "INSERT INTO r VALUES ('a', 'alpha'), ('c', 'gamma')",
    // Non-leading USING column order probe: `y` is second in `p2`, not first.
    "CREATE TABLE p2(a TEXT, y INTEGER, z INTEGER)",
    "INSERT INTO p2 VALUES ('first', 10, 1), ('second', 20, 2)",
    // Collation + affinity preservation inside a non-first parenthesized join.
    "CREATE TABLE nc(k TEXT COLLATE NOCASE, z INTEGER)",
    "INSERT INTO nc VALUES ('AbC', 1)",
    "CREATE TABLE typeless(v, z INTEGER)",
    "INSERT INTO typeless VALUES ('01', 1)",
    "CREATE TABLE tx(v TEXT, z INTEGER)",
    "INSERT INTO tx VALUES ('1', 1)",
    // Single-row seed for cursor-budget tests.
    "CREATE TABLE base(z INTEGER)",
    "INSERT INTO base VALUES (1)",
];

fn setup_stock(conn: &rusqlite::Connection) {
    for stmt in SETUP_DDL {
        conn.execute(stmt, []).unwrap();
    }
}

async fn setup_fsqlite(conn: &Connection) {
    for stmt in SETUP_DDL {
        conn.execute(stmt).await.unwrap();
    }
}

#[test]
fn non_first_paren_join_shapes_match_stock() {
    let cases: &[(&str, &str)] = &[
        // 1. Inner JOIN with inner USING: USING dedups y inside the parens only.
        (
            "inner_using_star",
            "SELECT * FROM o JOIN (p JOIN q USING (y)) ON o.z = p.z ORDER BY o.z",
        ),
        (
            "inner_using_qualified_and_bare",
            "SELECT o.z, y, p.y, q.y, q.w FROM o JOIN (p JOIN q USING (y)) ON o.z = p.z ORDER BY o.z",
        ),
        (
            "inner_using_table_stars",
            "SELECT p.*, q.* FROM o JOIN (p JOIN q USING (y)) ON o.z = p.z ORDER BY o.z",
        ),
        // Non-leading USING column position inside a non-first parenthesized join.
        (
            "inner_using_nonleading_col_order",
            "SELECT * FROM o JOIN (p2 JOIN q USING (y)) ON o.z = p2.z ORDER BY o.z",
        ),
        (
            "inner_using_nonleading_table_star",
            "SELECT p2.*, q.* FROM o JOIN (p2 JOIN q USING (y)) ON o.z = p2.z ORDER BY o.z",
        ),
        // 2. LEFT JOIN over (p JOIN q): unmatched p row must null-extend BOTH p and q.
        (
            "left_over_inner_join",
            "SELECT * FROM o LEFT JOIN (p JOIN q ON p.y = q.y) ON o.z = p.z ORDER BY o.z",
        ),
        // 3. Inner JOIN over (p LEFT JOIN q USING (y)): q.y is NULL when q doesn't match,
        //    while bare y / p.y stays non-NULL and SELECT * omits duplicate q.y.
        (
            "inner_over_left_using",
            "SELECT *, p.y, q.y FROM o JOIN (p LEFT JOIN q USING (y)) ON o.z = p.z ORDER BY o.z",
        ),
        // 4. Inner/LEFT JOIN over (p RIGHT JOIN q USING (y)) and (p FULL JOIN q USING (y)).
        (
            "inner_over_right_using",
            "SELECT *, y, p.y, q.y FROM o JOIN (p RIGHT JOIN q USING (y)) ON o.z = 1 ORDER BY y",
        ),
        (
            "inner_over_full_using",
            "SELECT *, y, p.y, q.y FROM o JOIN (p FULL JOIN q USING (y)) ON o.z = 1 ORDER BY y",
        ),
        (
            "left_over_full_using_on_bare_y",
            "SELECT o.z, y, p.y, q.y, q.w FROM o LEFT JOIN (p FULL JOIN q USING (y)) ON o.z * 10 = y ORDER BY o.z, y",
        ),
        // 5. Outer USING joined against an inner USING.
        (
            "outer_using_over_inner_using",
            "SELECT * FROM o JOIN (p JOIN q USING (y)) USING (z) ORDER BY z",
        ),
        (
            "outer_using_y_over_inner_full_using_y",
            "SELECT * FROM (SELECT 10 AS y UNION ALL SELECT 30 AS y) AS s \
             JOIN (p FULL JOIN q USING (y)) USING (y) ORDER BY y",
        ),
        // 6. Comma + CROSS JOIN, double parens, single-item parens, three-deep nest.
        (
            "comma_cross_join",
            "SELECT * FROM o, (p CROSS JOIN q) WHERE p.y = q.y ORDER BY o.z",
        ),
        (
            "double_parens",
            "SELECT * FROM o JOIN ((p JOIN q USING (y))) ON o.z = p.z ORDER BY o.z",
        ),
        (
            "single_item_parens",
            "SELECT * FROM o JOIN (p) ON o.z = p.z ORDER BY o.z",
        ),
        (
            "three_deep_nest",
            "SELECT * FROM o JOIN (p JOIN (q LEFT JOIN r ON q.w = r.w) ON p.y = q.y) ON o.z = p.z ORDER BY o.z",
        ),
        (
            "leading_paren_inside_nonfirst_paren",
            "SELECT * FROM o JOIN ((p JOIN q USING (y)) LEFT JOIN r ON q.w = r.w) ON o.z = p.z ORDER BY o.z",
        ),
        // 7. Aliases and rowid inside parens.
        (
            "aliases_inside_parens",
            "SELECT o.z, p1.y, q1.w FROM o JOIN (p AS p1 JOIN q AS q1 ON p1.y = q1.y) ON o.z = p1.z ORDER BY o.z",
        ),
        (
            "rowid_inside_parens",
            "SELECT o.z, p.rowid, q.rowid FROM o LEFT JOIN (p JOIN q ON p.y = q.y) ON o.z = p.z ORDER BY o.z",
        ),
        // 8. Collation and typeless/TEXT affinity preservation inside parens.
        (
            "nocase_collation_inside_parens",
            "SELECT o.z, nc.k = 'abc' FROM o JOIN (nc JOIN p ON nc.z = p.z) ON o.z = nc.z",
        ),
        (
            "typeless_affinity_inside_parens",
            "SELECT o.z, typeless.v = tx.v FROM o JOIN (typeless JOIN tx ON typeless.z = tx.z) ON o.z = typeless.z",
        ),
        // 9. Inside scalar subquery, EXISTS, IN, GROUP BY aggregate, window, CTE body, recursive arm.
        (
            "inside_scalar_subquery",
            "SELECT z, (SELECT q.w FROM o AS o2 JOIN (p JOIN q USING (y)) ON o2.z = p.z WHERE o2.z = o.z) \
             FROM o ORDER BY z",
        ),
        (
            "inside_exists_subquery",
            "SELECT z FROM o WHERE EXISTS (SELECT 1 FROM o AS o2 JOIN (p JOIN q USING (y)) ON o2.z = p.z WHERE o2.z = o.z) ORDER BY z",
        ),
        (
            "inside_in_subquery",
            "SELECT z FROM o WHERE z IN (SELECT p.z FROM o AS o2 JOIN (p JOIN q USING (y)) ON o2.z = p.z) ORDER BY z",
        ),
        (
            "with_group_by_aggregate",
            "SELECT o.z, count(p.y), sum(p.y) FROM o LEFT JOIN (p JOIN q ON p.y = q.y) ON o.z = p.z GROUP BY o.z ORDER BY o.z",
        ),
        (
            "with_window_function",
            "SELECT o.z, p.y, sum(p.y) OVER (ORDER BY o.z) FROM o LEFT JOIN (p JOIN q ON p.y = q.y) ON o.z = p.z ORDER BY o.z",
        ),
        (
            "inside_cte_body",
            "WITH w AS (SELECT o.z, y, q.w FROM o LEFT JOIN (p JOIN q USING (y)) ON o.z = p.z) \
             SELECT * FROM w ORDER BY z",
        ),
        (
            "inside_recursive_arm_valid",
            "WITH RECURSIVE c(x) AS ( \
                 SELECT 1 \
                 UNION ALL \
                 SELECT c.x + 1 FROM c JOIN (p JOIN q USING (y)) ON c.x = p.z WHERE c.x < 3 \
             ) SELECT x FROM c ORDER BY x",
        ),
        // 10. Recursive CTE error shapes (circular reference / multiple recursive references / legal single-item paren).
        (
            "recursive_nonfirst_paren_circular",
            "WITH RECURSIVE c(x) AS ( \
                 SELECT 1 \
                 UNION ALL \
                 SELECT x + 1 FROM o JOIN (c JOIN p) WHERE x < 3 \
             ) SELECT x FROM c",
        ),
        (
            "recursive_nonfirst_paren_multiple",
            "WITH RECURSIVE c(x) AS ( \
                 SELECT 1 \
                 UNION ALL \
                 SELECT c.x + 1 FROM c JOIN (c AS c2 JOIN p) WHERE c.x < 3 \
             ) SELECT x FROM c",
        ),
        (
            "recursive_nonfirst_single_item_paren_legal",
            "WITH RECURSIVE c(x) AS ( \
                 SELECT 1 \
                 UNION ALL \
                 SELECT x + 1 FROM o JOIN (c) WHERE o.z = 1 AND x < 3 \
             ) SELECT x FROM c ORDER BY x",
        ),
        // 11. Negative controls: leading (first-item) parenthesized join and recursive arm opening with paren join.
        (
            "negative_leading_paren_join",
            "SELECT * FROM (p JOIN q USING (y)) JOIN o ON o.z = p.z ORDER BY o.z",
        ),
        (
            "negative_leading_paren_recursive_arm",
            "WITH RECURSIVE c(x) AS ( \
                 SELECT 1 \
                 UNION ALL \
                 SELECT x + 1 FROM (c JOIN o) WHERE o.z = 1 AND x < 3 \
             ) SELECT x FROM c ORDER BY x",
        ),
    ];

    let stock = rusqlite::Connection::open_in_memory().unwrap();
    setup_stock(&stock);

    let expected: Vec<(&str, &str, String, String)> = cases
        .iter()
        .map(|&(name, sql)| (name, sql, stock_rows_only(&stock, sql), stock_outcome(&stock, sql)))
        .collect();

    // Pin the exact stock error messages for the recursive shapes so the oracle
    // cannot pass vacuously.
    for (name, _, stock_rows, _) in &expected {
        if *name == "recursive_nonfirst_paren_circular" {
            assert_eq!(stock_rows, "error: circular reference: c", "stock pin for {name}");
        } else if *name == "recursive_nonfirst_paren_multiple" {
            assert_eq!(
                stock_rows, "error: multiple recursive references: c",
                "stock pin for {name}"
            );
        } else {
            assert!(
                stock_rows.starts_with("rows="),
                "expected live stock to answer {name}, got: {stock_rows}"
            );
        }
    }

    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_v57jp.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                setup_fsqlite(&conn).await;

                for (name, sql, expected_rows, stock_debug) in &expected {
                    let got = fsqlite_rows_only(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected_rows,
                        "case={name} file_backed={file_backed} prepared={prepared}\nsql: {sql}\nstock full: {stock_debug}"
                    );
                }
            }
        }
    });
}

fn cte_chain(n: usize, outer: &str) -> String {
    let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_owned()];
    for i in 1..n {
        ctes.push(format!("c{i} AS (SELECT x FROM c{})", i - 1));
    }
    format!(
        "WITH {} {}",
        ctes.join(", "),
        outer.replace("{last}", &format!("c{}", n - 1))
    )
}

#[test]
fn non_first_paren_join_cursor_budget_matches_stock() {
    const TOO_DEEP: &str = "error: VIEWs and/or subqueries nested too deep";
    const ONE: &str = "rows=[[\"1\"]]";

    // Stock assigns +1 extra cursor for each non-first multi-item ParenJoin
    // (SF_NestedFrom subquery), 0 extra for a leading ParenJoin, and 0 extra
    // for redundant parens around a single item.
    let non_first = "SELECT x FROM base JOIN ({last} JOIN base AS b2)";
    let double_wrapped = "SELECT x FROM base JOIN (({last} JOIN base AS b2))";
    let two_nested = "SELECT x FROM base JOIN (base AS b2 JOIN ({last} JOIN base AS b3))";
    let leading = "SELECT x FROM ({last} JOIN base AS b2) JOIN base";
    let single_item = "SELECT x FROM base JOIN ({last})";

    let cases: Vec<(&str, String, &str)> = vec![
        ("non_first_below", cte_chain(996, non_first), ONE),
        ("non_first_at_limit", cte_chain(997, non_first), TOO_DEEP),
        ("double_wrapped_below", cte_chain(996, double_wrapped), ONE),
        ("double_wrapped_at_limit", cte_chain(997, double_wrapped), TOO_DEEP),
        ("two_nested_below", cte_chain(994, two_nested), ONE),
        ("two_nested_at_limit", cte_chain(995, two_nested), TOO_DEEP),
        ("leading_below", cte_chain(997, leading), ONE),
        ("leading_at_limit", cte_chain(998, leading), TOO_DEEP),
        ("single_item_below", cte_chain(998, single_item), ONE),
        ("single_item_at_limit", cte_chain(999, single_item), TOO_DEEP),
    ];

    let stock = rusqlite::Connection::open_in_memory().unwrap();
    setup_stock(&stock);

    let expected: Vec<String> = cases
        .iter()
        .map(|(name, sql, pinned)| {
            let stock_gave = stock_rows_only(&stock, sql);
            assert_eq!(&stock_gave, pinned, "live stock pin failed for {name}");
            stock_gave
        })
        .collect();

    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_v57jp_budget.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                setup_fsqlite(&conn).await;

                for ((name, sql, _), expected_outcome) in cases.iter().zip(&expected) {
                    let got = fsqlite_rows_only(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected_outcome,
                        "budget case={name} file_backed={file_backed} prepared={prepared}"
                    );
                }
            }
        }
    });
}

#[test]
fn nested_paren_join_using_star_and_tvf_match_stock() {
    let setup: &[&str] = &[
        "CREATE TABLE tj(key INTEGER, val TEXT)",
        "INSERT INTO tj VALUES (1, 'x'), (0, 'y')",
        "CREATE TABLE tjv(key INTEGER, value INTEGER)",
        "INSERT INTO tjv VALUES (1, 20), (0, 99)",
        "CREATE TABLE t1(k INTEGER, j INTEGER, v1 TEXT)",
        "INSERT INTO t1 VALUES (1, 2, 'a'), (9, 90, 'x')",
        "CREATE TABLE t2(k INTEGER, j INTEGER, v2 TEXT)",
        "INSERT INTO t2 VALUES (1, 5, 'b')",
        "CREATE TABLE t3(j INTEGER, v3 TEXT)",
        "INSERT INTO t3 VALUES (5, 'c'), (7, 'd')",
        "CREATE TABLE s0(a INTEGER)",
        "INSERT INTO s0 VALUES (1)",
        "CREATE TABLE s1(a INTEGER, j INTEGER)",
        "INSERT INTO s1 VALUES (1, 10)",
        "CREATE TABLE s2(j INTEGER, x TEXT)",
        "INSERT INTO s2 VALUES (10, 'p'), (11, 'p2')",
        "CREATE TABLE s3(a INTEGER, j INTEGER)",
        "INSERT INTO s3 VALUES (1, 20)",
        "CREATE TABLE s4(j INTEGER, y TEXT)",
        "INSERT INTO s4 VALUES (20, 'q'), (22, 'q2')",
        "CREATE TABLE c1(k INTEGER, j TEXT)",
        "INSERT INTO c1 VALUES (1, 'z')",
        "CREATE TABLE c2(k INTEGER, j TEXT COLLATE NOCASE)",
        "INSERT INTO c2 VALUES (1, 'b'), (1, 'A')",
        "CREATE TABLE c3(j TEXT, v INTEGER)",
        "INSERT INTO c3 VALUES ('b', 1), ('A', 2)",
        "CREATE TABLE f1(k INTEGER, a TEXT)",
        "INSERT INTO f1 VALUES (1, 'a')",
        "CREATE TABLE f2(k INTEGER, b TEXT)",
        "INSERT INTO f2 VALUES (2, 'b')",
        "CREATE TABLE f3(k INTEGER, c TEXT)",
        "INSERT INTO f3 VALUES (3, 'c')",
        "CREATE TABLE d1(a INTEGER, b INTEGER, c INTEGER)",
        "INSERT INTO d1 VALUES (1, 2, 3)",
        "CREATE TABLE d2(a INTEGER, b INTEGER)",
        "INSERT INTO d2 VALUES (1, 20)",
        "CREATE TABLE d3(b INTEGER, c INTEGER)",
        "INSERT INTO d3 VALUES (20, 30)",
        "CREATE TABLE d4(c INTEGER, d INTEGER)",
        "INSERT INTO d4 VALUES (30, 40)",
        "CREATE VIEW v_nested_using AS SELECT * FROM t1 JOIN (t2 JOIN t3 USING(j)) USING(k)",
    ];

    // Exact-row parity cases (including error parity for ambiguous bare refs).
    let exact_cases: &[(&str, &str)] = &[
        // 1. Inner USING(j) with outer table also carrying column j (different values!).
        (
            "inner_using_j_outer_using_k_star",
            "SELECT * FROM t1 JOIN (t2 JOIN t3 USING(j)) USING(k)",
        ),
        (
            "inner_using_j_outer_on_star",
            "SELECT * FROM t1 JOIN (t2 JOIN t3 USING(j)) ON t1.k = t2.k",
        ),
        (
            "inner_full_using_j_outer_left_using_k_star",
            "SELECT * FROM t1 LEFT JOIN (t2 FULL JOIN t3 USING(j)) USING(k) ORDER BY k",
        ),
        (
            "inner_using_j_subquery_and_view",
            "SELECT * FROM (SELECT * FROM t1 JOIN (t2 JOIN t3 USING(j)) USING(k))",
        ),
        ("inner_using_j_view", "SELECT * FROM v_nested_using"),
        (
            "inner_using_j_distinct_orderby_pos",
            "SELECT DISTINCT * FROM t1 JOIN (t2 JOIN t3 USING(j)) USING(k) ORDER BY 2, 4",
        ),
        // 2. Triple-nested disjoint USING columns.
        (
            "triple_nested_disjoint_using",
            "SELECT * FROM d1 JOIN (d2 JOIN (d3 JOIN d4 USING(c)) USING(b)) USING(a)",
        ),
        // 3. Two sibling ParenJoins both having inner USING(j) (must keep distinct values 10 vs 20!).
        (
            "two_sibling_paren_joins_same_using_col",
            "SELECT * FROM s0 \
             JOIN (s1 JOIN s2 USING(j)) ON s0.a = s1.a \
             JOIN (s3 JOIN s4 USING(j)) ON s0.a = s3.a",
        ),
        (
            "two_sibling_paren_joins_full_same_using_col",
            "SELECT * FROM s0 \
             JOIN (s1 FULL JOIN s2 USING(j)) ON s0.a = 1 \
             JOIN (s3 FULL JOIN s4 USING(j)) ON s0.a = 1 \
             ORDER BY 2, 5",
        ),
        // 4. GROUP BY, ungrouped aggregate, WINDOW, and GROUP BY + WINDOW over nested USING *.
        (
            "groupby_paren_using_star",
            "SELECT *, count(*) FROM t1 JOIN (t2 JOIN t3 USING(j)) USING(k) GROUP BY t1.k",
        ),
        (
            "ungrouped_agg_paren_using_star",
            "SELECT *, count(*) FROM t1 JOIN (t2 JOIN t3 USING(j)) USING(k)",
        ),
        (
            "window_paren_using_star",
            "SELECT *, row_number() OVER () FROM t1 JOIN (t2 JOIN t3 USING(j)) USING(k)",
        ),
        (
            "groupby_window_paren_using_star",
            "SELECT *, count(*), row_number() OVER () FROM t1 JOIN (t2 JOIN t3 USING(j)) USING(k) GROUP BY t1.k",
        ),
        (
            "groupby_two_sibling_parens",
            "SELECT *, count(*) FROM s0 \
             JOIN (s1 JOIN s2 USING(j)) ON s0.a = s1.a \
             JOIN (s3 JOIN s4 USING(j)) ON s0.a = s3.a \
             GROUP BY s0.a",
        ),
        (
            "window_two_sibling_parens",
            "SELECT *, row_number() OVER () FROM s0 \
             JOIN (s1 JOIN s2 USING(j)) ON s0.a = s1.a \
             JOIN (s3 JOIN s4 USING(j)) ON s0.a = s3.a",
        ),
        // 5. FULL JOIN USING + GROUP BY must project the coalesced USING key in SELECT *.
        (
            "full_join_using_groupby_star",
            "SELECT *, count(*) FROM f1 FULL JOIN f2 USING(k) GROUP BY k ORDER BY k",
        ),
        // 6. Positional ORDER BY on synthetic inner USING slot preserves NOCASE collation.
        (
            "collation_on_inner_using_star_orderby",
            "SELECT * FROM c1 JOIN (c2 JOIN c3 USING(j)) USING(k) ORDER BY 3",
        ),
        // 7. TVF inside nested ParenJoin with USING (projecting all SQLite-defined columns except opaque id).
        (
            "bd_qe8hc_tvf_using_key_projected",
            "SELECT key, val, value, type, atom, parent, fullkey, path \
             FROM tj JOIN (json_each('[10,20]') a JOIN (SELECT 1 AS key) b USING(key)) USING(key)",
        ),
        (
            "bd_qe8hc_tvf_using_key_left",
            "SELECT key, val, value, type, atom, parent, fullkey, path \
             FROM tj LEFT JOIN (json_each('[10,20]') a JOIN (SELECT 1 AS key) b USING(key)) USING(key) ORDER BY key",
        ),
        // 8. Negative controls: genuinely ambiguous bare refs MUST still error like stock.
        (
            "negative_bare_ambiguous_j",
            "SELECT j FROM t1 JOIN (t2 JOIN t3 USING(j)) USING(k)",
        ),
        (
            "negative_full_using_then_on_ambiguous_star",
            "SELECT * FROM f1 FULL JOIN f2 USING(k) JOIN f3 ON 1=1",
        ),
        (
            "negative_full_using_then_on_groupby_ambiguous_star",
            "SELECT *, count(*) FROM f1 FULL JOIN f2 USING(k) JOIN f3 ON 1=1 GROUP BY f3.c",
        ),
        (
            "negative_full_using_then_on_window_ambiguous_star",
            "SELECT *, row_number() OVER () FROM f1 FULL JOIN f2 USING(k) JOIN f3 ON 1=1",
        ),
    ];

    // Exact bd-nested-paren-join-using-qe8hc SELECT * TVF cases (where json_each.id is an
    // opaque implementation-defined integer, so we verify exact equality on every other column
    // and check that the id slot is an integer with matching column count/names).
    let tvf_star_cases: &[(&str, &str, usize)] = &[
        (
            "bd_qe8hc_exact_repro_star",
            "SELECT * FROM tj JOIN (json_each('[10,20]') a JOIN (SELECT 1 AS key) b USING(key)) USING(key)",
            5, // `id` column index in `key, val, value, type, atom, id, parent, fullkey, path`
        ),
        (
            "bd_qe8hc_tvf_inner_using_value_outer_using_key_star",
            "SELECT * FROM tjv JOIN (json_each('[10,20]') a JOIN (SELECT 20 AS value) b USING(value)) USING(key)",
            5, // `id` column index in `key, value, value, type, atom, id, parent, fullkey, path`
        ),
    ];

    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for stmt in setup {
        stock.execute(stmt, []).unwrap();
    }

    let expected_exact: Vec<(&str, &str, String, String)> = exact_cases
        .iter()
        .map(|&(name, sql)| (name, sql, stock_rows_only(&stock, sql), stock_outcome(&stock, sql)))
        .collect();
    for (name, _, stock_rows, _) in &expected_exact {
        if name.starts_with("negative_") {
            assert!(
                stock_rows.contains("ambiguous column name"),
                "expected stock ambiguity error for {name}, got {stock_rows}"
            );
        } else {
            assert!(
                stock_rows.starts_with("rows="),
                "expected stock rows for {name}, got {stock_rows}"
            );
        }
    }

    struct TvfStarExpectation<'a> {
        name: &'a str,
        sql: &'a str,
        id_col: usize,
        cols: Vec<String>,
        rows: Vec<Vec<String>>,
    }

    let expected_tvf_star: Vec<TvfStarExpectation<'_>> = tvf_star_cases
        .iter()
        .map(|&(name, sql, id_col)| {
            let mut stmt = stock.prepare(sql).unwrap();
            let col_names: Vec<String> = stmt.column_names().map(str::to_owned).collect();
            assert_eq!(col_names.get(id_col).map(String::as_str), Some("id"));
            let ncols = stmt.column_count();
            let mut rows = Vec::new();
            let mut cursor = stmt.query([]).unwrap();
            while let Some(row) = cursor.next().unwrap() {
                let mut vals: Vec<String> = (0..ncols)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect();
                assert!(
                    vals[id_col].parse::<i64>().is_ok(),
                    "stock json_each id should be integer for {name}, got {}",
                    vals[id_col]
                );
                vals[id_col] = "<id>".to_owned();
                rows.push(vals);
            }
            assert!(!rows.is_empty(), "stock returned empty rows for {name}");
            TvfStarExpectation {
                name,
                sql,
                id_col,
                cols: col_names,
                rows,
            }
        })
        .collect();

    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_qe8hc.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for stmt in setup {
                    conn.execute(stmt).await.unwrap();
                }

                for (name, sql, expected_rows, stock_debug) in &expected_exact {
                    let got = fsqlite_rows_only(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected_rows,
                        "case={name} file_backed={file_backed} prepared={prepared}\nsql: {sql}\nstock full: {stock_debug}"
                    );
                }

                for exp in &expected_tvf_star {
                    let rows = if prepared {
                        let stmt = conn.prepare(exp.sql).await.unwrap();
                        assert_eq!(
                            stmt.column_names(),
                            exp.cols.as_slice(),
                            "col_names mismatch for {} file_backed={file_backed}",
                            exp.name
                        );
                        stmt.query().await.unwrap()
                    } else {
                        conn.query(exp.sql).await.unwrap()
                    };
                    let got_rows: Vec<Vec<String>> = rows
                        .iter()
                        .map(|row| {
                            let mut vals: Vec<String> =
                                row.values().iter().map(render_fsqlite).collect();
                            assert!(
                                vals[exp.id_col].parse::<i64>().is_ok(),
                                "fsqlite json_each id should be integer for {}, got {}",
                                exp.name,
                                vals[exp.id_col]
                            );
                            vals[exp.id_col] = "<id>".to_owned();
                            vals
                        })
                        .collect();
                    assert_eq!(
                        &got_rows, &exp.rows,
                        "tvf star case={} file_backed={file_backed} prepared={prepared}\nsql: {}",
                        exp.name, exp.sql
                    );
                }
            }
        }
    });
}
