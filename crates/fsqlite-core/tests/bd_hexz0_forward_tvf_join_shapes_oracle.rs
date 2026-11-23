//! bd-hexz0: table-valued functions listed before the FROM source their
//! arguments read across USING, NATURAL, explicit CROSS JOIN, and outer-join
//! boundaries, compared live against bundled SQLite 3.54.1 (`rusqlite`).
//!
//! Covers:
//! - USING / NATURAL forward-reference reordering (including `*` column order
//!   and bare USING columns in SELECT, WHERE, GROUP BY, ORDER BY),
//! - SQLite 3.54.1's prepare-time refusal
//!   `"table-function argument references tables to its right"` when a forward
//!   reference crosses a LEFT, RIGHT, or FULL JOIN (including `JT_LTORJ` and
//!   `JT_RIGHT`, plus a forward reference into a parenthesized join),
//! - Explicit `CROSS JOIN` preserving written order (yielding empty results
//!   for `json_each` rather than reordering like a comma join), while allowing
//!   inner joins *after* a `CROSS JOIN` to reorder among themselves,
//! - Legal outer-join placements (outer join after the TVF + its dependency,
//!   or an inner source after a `LEFT JOIN`-RHS TVF),
//! - Negative controls: comma join still reorders, and normal left-to-right
//!   `CROSS JOIN` / `LEFT JOIN` TVFs continue to return rows identically to
//!   stock.
//!
//! Every query runs on `:memory:` and a file-backed database, executed directly
//! and prepared.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP_DDL: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, tags TEXT)",
    "INSERT INTO t VALUES (1,'[1,2]'),(2,'[3]')",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, x INTEGER)",
    "INSERT INTO u VALUES (1,7),(2,8)",
    "CREATE TABLE v(id INTEGER PRIMARY KEY, label TEXT)",
    "INSERT INTO v VALUES (1,'one')",
];

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

type QueryResult = Result<Vec<Vec<String>>, String>;

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> QueryResult {
    let mut statement = conn.prepare(sql).map_err(|e| e.to_string())?;
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

async fn fsqlite_rows(conn: &Connection, sql: &str, prepared: bool) -> QueryResult {
    let rows = if prepared {
        let statement = conn.prepare(sql).await.map_err(|e| e.to_string())?;
        statement.query().await.map_err(|e| e.to_string())?
    } else {
        conn.query(sql).await.map_err(|e| e.to_string())?
    };
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect())
}

const RIGHT_REF_ERR: &str = "table-function argument references tables to its right";
const NO_SOLUTION_ERR: &str = "no query solution";

// (case_name, sql, expected_category)
// Categories prevent vacuous passes:
//   "rows"        => stock returns >= 1 row
//   "empty"       => stock returns Ok([])
//   "error"       => stock returns Err(RIGHT_REF_ERR)
//   "no_solution" => stock returns Err(NO_SOLUTION_ERR)
const CASES: &[(&str, &str, &str)] = &[
    // 1. USING and NATURAL forward-reference reordering.
    (
        "using_star",
        "SELECT * FROM json_each(t.tags) AS j JOIN t USING (id)",
        "rows",
    ),
    (
        "using_bare_id_select_where_order",
        "SELECT id, j.value, t.tags FROM json_each(t.tags) AS j JOIN t USING (id) WHERE id = 1 ORDER BY id",
        "rows",
    ),
    (
        "using_group_by_bare_id",
        "SELECT id, count(*), sum(j.value) FROM json_each(t.tags) AS j JOIN t USING (id) GROUP BY id ORDER BY id",
        "rows",
    ),
    (
        "natural_join_value",
        "SELECT j.value FROM json_each(t.tags) AS j NATURAL JOIN t",
        "rows",
    ),
    (
        "natural_join_star",
        "SELECT * FROM json_each(t.tags) AS j NATURAL JOIN t",
        "rows",
    ),
    // 2. Outer-join forward references refused by stock 3.54.1.
    (
        "left_join_direct_ref",
        "SELECT j.value, t.id FROM json_each(t.tags) AS j LEFT JOIN t",
        "error",
    ),
    (
        "left_join_elsewhere_comma_ref",
        "SELECT j.value, t.id FROM json_each(t.tags) AS j LEFT JOIN u ON 1, t",
        "error",
    ),
    (
        "right_join_direct_ref",
        "SELECT j.value, t.id FROM json_each(t.tags) AS j RIGHT JOIN t ON 1",
        "error",
    ),
    (
        "full_join_direct_ref",
        "SELECT j.value, t.id FROM json_each(t.tags) AS j FULL JOIN t ON 1",
        "error",
    ),
    (
        "inner_then_right_join_ltorj_ref",
        "SELECT j.value, t.id FROM json_each(t.tags) AS j JOIN t ON 1 RIGHT JOIN u ON 1",
        "error",
    ),
    (
        "rhs_of_right_join_then_inner_ref",
        "SELECT j.value, t.id FROM u RIGHT JOIN json_each(t.tags) AS j ON 1 JOIN t ON 1",
        "error",
    ),
    (
        "left_join_paren_join_ref",
        "SELECT j.value, t.id FROM json_each(t.tags) AS j LEFT JOIN (t JOIN u ON u.id = t.id) ON 1",
        "error",
    ),
    (
        "left_join_ref_inside_subquery",
        "SELECT * FROM (SELECT j.value, t.id FROM json_each(t.tags) AS j LEFT JOIN t)",
        "error",
    ),
    // 3. Explicit CROSS JOIN locks order (unlike comma `,`): empty rows, not error.
    (
        "cross_join_locks_order",
        "SELECT j.value, t.id FROM json_each(t.tags) AS j CROSS JOIN t",
        "empty",
    ),
    (
        "cross_join_blocks_following_comma",
        "SELECT j.value, t.id FROM json_each(t.tags) AS j CROSS JOIN u, t",
        "no_solution",
    ),
    (
        "inner_run_after_cross_join_still_reorders",
        "SELECT j.value, t.id, u.x FROM u CROSS JOIN json_each(t.tags) AS j, t ORDER BY u.id, t.id, j.value",
        "rows",
    ),
    // 4. Legal outer-join placements.
    (
        "outer_join_after_tvf_and_source",
        "SELECT j.value, t.id, u.x FROM json_each(t.tags) AS j, t LEFT JOIN u ON u.id = t.id ORDER BY t.id, j.value",
        "rows",
    ),
    (
        "using_then_left_join_after",
        "SELECT j.value, t.id, u.x FROM json_each(t.tags) AS j JOIN t USING (id) LEFT JOIN u ON u.id = t.id ORDER BY t.id, j.value",
        "rows",
    ),
    (
        "outer_join_on_reads_tvf",
        "SELECT j.value, t.id, u.x FROM json_each(t.tags) AS j, t LEFT JOIN u ON u.id = j.value ORDER BY t.id, j.value",
        "rows",
    ),
    (
        "left_join_rhs_tvf_reading_later_inner",
        "SELECT j.value, t.id, u.x FROM u LEFT JOIN json_each(t.tags) AS j ON 1 JOIN t ON 1 ORDER BY u.id, t.id, j.value",
        "rows",
    ),
    (
        "inner_run_after_left_join",
        "SELECT j.value, t.id, v.label FROM u LEFT JOIN v ON v.id = u.id, json_each(t.tags) AS j, t ORDER BY u.id, t.id, j.value",
        "rows",
    ),
    // 5. Negative controls: comma reorders; normal left-to-right CROSS/LEFT JOIN work.
    (
        "negative_comma_reorders",
        "SELECT j.value, t.id FROM json_each(t.tags) AS j, t ORDER BY t.id, j.value",
        "rows",
    ),
    (
        "negative_left_to_right_cross_join",
        "SELECT j.value, t.id FROM t CROSS JOIN json_each(t.tags) AS j ORDER BY t.id, j.value",
        "rows",
    ),
    (
        "negative_left_to_right_left_join",
        "SELECT j.value, t.id FROM t LEFT JOIN json_each(t.tags) AS j ON 1 ORDER BY t.id, j.value",
        "rows",
    ),
];

#[test]
fn forward_table_function_join_shapes_match_stock_3_54_1() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for stmt in SETUP_DDL {
        stock.execute(stmt, []).unwrap();
    }

    let expected: Vec<(&str, &str, QueryResult)> = CASES
        .iter()
        .map(|&(name, sql, category)| {
            let res = stock_rows(&stock, sql);
            match category {
                "rows" => assert!(
                    res.as_ref().is_ok_and(|rows| !rows.is_empty()),
                    "stock pin for {name} expected non-empty rows, got {res:?}"
                ),
                "empty" => assert_eq!(
                    res,
                    Ok(Vec::new()),
                    "stock pin for {name} expected empty rows"
                ),
                "error" => assert_eq!(
                    res,
                    Err(RIGHT_REF_ERR.to_owned()),
                    "stock pin for {name} expected {RIGHT_REF_ERR:?}"
                ),
                "no_solution" => assert_eq!(
                    res,
                    Err(NO_SOLUTION_ERR.to_owned()),
                    "stock pin for {name} expected {NO_SOLUTION_ERR:?}"
                ),
                other => panic!("unknown category {other}"),
            }
            (name, sql, res)
        })
        .collect();

    // Pin column names on USING / NATURAL `SELECT *` against stock.
    let star_col_checks = [
        "SELECT * FROM json_each(t.tags) AS j JOIN t USING (id)",
        "SELECT * FROM json_each(t.tags) AS j NATURAL JOIN t",
    ];
    let expected_star_cols: Vec<Vec<String>> = star_col_checks
        .iter()
        .map(|sql| {
            stock
                .prepare(sql)
                .unwrap()
                .column_names()
                .map(str::to_owned)
                .collect()
        })
        .collect();

    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_hexz0.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for stmt in SETUP_DDL {
                    conn.execute(stmt).await.unwrap();
                }

                for (name, sql, expected_res) in &expected {
                    let got = fsqlite_rows(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected_res,
                        "case={name} file_backed={file_backed} prepared={prepared}\nsql: {sql}"
                    );
                }

                if prepared {
                    for (sql, expected_cols) in star_col_checks.iter().zip(&expected_star_cols) {
                        let stmt = conn.prepare(sql).await.unwrap();
                        assert_eq!(
                            stmt.column_names(),
                            expected_cols.as_slice(),
                            "column names mismatch file_backed={file_backed} sql: {sql}"
                        );
                    }
                }
                conn.close().await.unwrap();
            }
        }
    });
}
