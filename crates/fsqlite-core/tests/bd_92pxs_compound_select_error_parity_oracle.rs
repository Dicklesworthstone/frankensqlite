//! bd-92pxs: compound select error-text and timing parity against live bundled
//! SQLite 3.54.1 (`rusqlite`).
//!
//! Covers two classes of compound-SELECT diagnostics:
//! 1. Unknown columns in any compound arm (first, middle, last; FROM-less,
//!    table-backed, `VALUES`, `WHERE`, `GROUP BY`, `HAVING`, `LIMIT`, `OFFSET`,
//!    qualified `t.col`, behind `LIMIT 0` / `LIMIT 1` / bound `LIMIT ?`, inside
//!    subqueries / derived tables / CTEs / `INSERT` / `CREATE TABLE AS` /
//!    `CREATE VIEW`), asserting both exact error text AND prepare-vs-step timing.
//! 2. `ORDER BY` / `LIMIT` clauses placed before `UNION` / `UNION ALL` /
//!    `INTERSECT` / `EXCEPT` on any non-final arm (top-level, subquery, derived
//!    table, `EXISTS`, CTE, `INSERT ... SELECT`, `CREATE VIEW`, `CREATE TRIGGER`),
//!    plus negative controls verifying syntax errors in the RHS or trailing
//!    tokens, `VALUES` + `ORDER BY`/`LIMIT`, bare top-level parens, DQS ON/OFF,
//!    and valid compound queries still match stock 3.54.1 identically.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use rusqlite::config::DbConfig;

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

fn stock_message(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message))
        | rusqlite::Error::SqlInputError { msg: message, .. } => message.clone(),
        other => other.to_string(),
    }
}

fn stock_outcome(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> String {
    let mut statement = match conn.prepare(sql) {
        Ok(statement) => statement,
        Err(error) => return format!("prepare error: {}", stock_message(&error)),
    };
    for (index, value) in params.iter().enumerate() {
        statement.raw_bind_parameter(index + 1, value).unwrap();
    }
    let columns = statement.column_count();
    let mut cursor = statement.raw_query();
    let mut rows = Vec::new();
    loop {
        match cursor.next() {
            Ok(Some(row)) => rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect::<Vec<_>>(),
            ),
            Ok(None) => return format!("rows={rows:?}"),
            Err(error) => return format!("step error: {}", stock_message(&error)),
        }
    }
}

fn render_rows(rows: &[fsqlite_core::connection::Row]) -> String {
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect();
    format!("rows={rows:?}")
}

async fn fsqlite_outcome(conn: &Connection, sql: &str, params: &[i64], prepared: bool) -> String {
    let values: Vec<SqliteValue> = params.iter().map(|v| SqliteValue::Integer(*v)).collect();
    if !prepared {
        let result = if values.is_empty() {
            conn.query(sql).await
        } else {
            conn.query_with_params(sql, &values).await
        };
        return match result {
            Ok(rows) => render_rows(&rows),
            Err(error) => format!("error: {error}"),
        };
    }
    let statement = match conn.prepare(sql).await {
        Ok(statement) => statement,
        Err(error) => return format!("prepare error: {error}"),
    };
    let result = if values.is_empty() {
        statement.query().await
    } else {
        statement.query_with_params(&values).await
    };
    match result {
        Ok(rows) => render_rows(&rows),
        Err(error) => format!("step error: {error}"),
    }
}

fn strip_phase(outcome: &str) -> &str {
    outcome
        .strip_prefix("prepare ")
        .or_else(|| outcome.strip_prefix("step "))
        .unwrap_or(outcome)
}

async fn open_pair(
    file_backed: bool,
) -> (
    Option<tempfile::TempDir>,
    rusqlite::Connection,
    Connection,
) {
    let setup = "\
        CREATE TABLE s(v INTEGER, w TEXT); \
        INSERT INTO s VALUES (1, 'a'), (2, 'b'); \
        CREATE TABLE t(a INTEGER);";
    if file_backed {
        let dir = tempfile::tempdir().unwrap();
        let stock_path = dir.path().join("stock.db");
        let fsqlite_path = dir.path().join("fsqlite.db");
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        stock.execute_batch(setup).unwrap();
        let fsqlite = Connection::open(fsqlite_path.to_str().unwrap())
            .await
            .unwrap();
        fsqlite.execute_batch(setup).await.unwrap();
        (Some(dir), stock, fsqlite)
    } else {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        stock.execute_batch(setup).unwrap();
        let fsqlite = Connection::open(":memory:").await.unwrap();
        fsqlite.execute_batch(setup).await.unwrap();
        (None, stock, fsqlite)
    }
}

async fn check_cases(
    cases: &[(&str, &str, &[i64], &str)],
    dqs: Option<bool>,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for file_backed in [false, true] {
        let mode = if file_backed { "file" } else { "mem" };
        let (_dir, stock, fsqlite) = open_pair(file_backed).await;
        if let Some(enabled) = dqs {
            stock
                .set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DML, enabled)
                .unwrap();
            let pragma = if enabled {
                "PRAGMA fsqlite.dqs = ON"
            } else {
                "PRAGMA fsqlite.dqs = OFF"
            };
            fsqlite.execute(pragma).await.unwrap();
        }
        for &(label, sql, params, expected_stock) in cases {
            let stock_res = stock_outcome(&stock, sql, params);
            assert_eq!(
                stock_res, expected_stock,
                "[{mode}/{label}] bundled sqlite3 oracle drift for `{sql}`"
            );

            let prep_res = fsqlite_outcome(&fsqlite, sql, params, true).await;
            if prep_res != stock_res {
                mismatches.push(format!(
                    "[{mode}/{label}/prepared] `{sql}` params={params:?}\n  stock:   {stock_res}\n  fsqlite: {prep_res}"
                ));
            }

            let direct_res = fsqlite_outcome(&fsqlite, sql, params, false).await;
            let expected_direct = strip_phase(&stock_res);
            if direct_res != expected_direct {
                mismatches.push(format!(
                    "[{mode}/{label}/direct] `{sql}` params={params:?}\n  stock:   {expected_direct}\n  fsqlite: {direct_res}"
                ));
            }
        }
    }
    mismatches
}

#[test]
fn unknown_columns_in_every_compound_arm_match_stock_text_and_timing() {
    let cases: &[(&str, &str, &[i64], &str)] = &[
        (
            "headline_union_all_limit_1",
            "SELECT 1 UNION ALL SELECT nosuchcol LIMIT 1",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "union_all_limit_0",
            "SELECT 1 UNION ALL SELECT nosuchcol LIMIT 0",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "union_all_bound_limit_0",
            "SELECT 1 UNION ALL SELECT nosuchcol LIMIT ?",
            &[0],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "union_all_bound_limit_1",
            "SELECT 1 UNION ALL SELECT nosuchcol LIMIT ?",
            &[1],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "first_arm_bad_second_ok",
            "SELECT nosuchcol UNION SELECT 1",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "two_bad_arms_rtl",
            "SELECT bad1 UNION SELECT bad2",
            &[],
            "prepare error: no such column: bad2",
        ),
        (
            "three_bad_arms_rtl",
            "SELECT bad1 UNION SELECT bad2 UNION SELECT bad3",
            &[],
            "prepare error: no such column: bad3",
        ),
        (
            "first_and_third_bad_rtl",
            "SELECT bad1 UNION SELECT 2 UNION SELECT bad3",
            &[],
            "prepare error: no such column: bad3",
        ),
        (
            "first_and_second_bad_third_ok",
            "SELECT bad1 UNION SELECT bad2 UNION SELECT 3",
            &[],
            "prepare error: no such column: bad2",
        ),
        (
            "intersect_bad_rhs",
            "SELECT 1 INTERSECT SELECT nosuchcol",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "except_bad_rhs",
            "SELECT 1 EXCEPT SELECT nosuchcol",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "arity_vs_unknown_rhs",
            "SELECT 1, 2 UNION SELECT nosuchcol",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "arity_vs_unknown_lhs",
            "SELECT nosuchcol UNION SELECT 1, 2",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "values_union_unknown",
            "VALUES (1) UNION SELECT nosuchcol",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "select_union_values_unknown",
            "SELECT 1 UNION VALUES (nosuchcol)",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "qualified_unknown_fromless",
            "SELECT 1 UNION SELECT t.nosuchcol",
            &[],
            "prepare error: no such column: t.nosuchcol",
        ),
        (
            "table_arm2_unknown_limit_1",
            "SELECT v FROM s UNION ALL SELECT nosuchcol FROM s LIMIT 1",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "table_arm2_where_unknown_limit_1",
            "SELECT v FROM s UNION ALL SELECT v FROM s WHERE nosuchcol = 1 LIMIT 1",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "table_arm2_groupby_unknown_limit_1",
            "SELECT v FROM s UNION ALL SELECT v FROM s GROUP BY nosuchcol LIMIT 1",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "table_arm2_having_unknown_limit_1",
            "SELECT v FROM s UNION ALL SELECT v FROM s GROUP BY v HAVING nosuchcol > 0 LIMIT 1",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "limit_unknown_beats_arms",
            "SELECT bad1 UNION SELECT bad2 LIMIT bad_lim",
            &[],
            "prepare error: no such column: bad_lim",
        ),
        (
            "offset_unknown_beats_arms",
            "SELECT 1 UNION SELECT bad2 LIMIT 1 OFFSET bad_off",
            &[],
            "prepare error: no such column: bad_off",
        ),
        (
            "compound_order_unknown",
            "SELECT 1 UNION SELECT 2 ORDER BY nosuchcol",
            &[],
            "prepare error: 1st ORDER BY term does not match any column in the result set",
        ),
        (
            "arm_unknown_beats_compound_order",
            "SELECT 1 UNION SELECT bad2 ORDER BY bad_ord",
            &[],
            "prepare error: no such column: bad2",
        ),
        (
            "scalar_subquery_compound_unknown",
            "SELECT (SELECT 1 UNION ALL SELECT nosuchcol LIMIT 1)",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "derived_table_compound_unknown",
            "SELECT * FROM (SELECT 1 UNION ALL SELECT nosuchcol LIMIT 1)",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "cte_compound_unknown",
            "WITH c AS (SELECT 1 UNION ALL SELECT nosuchcol LIMIT 1) SELECT * FROM c",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "insert_compound_unknown",
            "INSERT INTO t SELECT 1 UNION ALL SELECT nosuchcol LIMIT 1",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "ctas_compound_unknown",
            "CREATE TABLE ct AS SELECT 1 UNION ALL SELECT nosuchcol LIMIT 1",
            &[],
            "prepare error: no such column: nosuchcol",
        ),
    ];

    let mismatches = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .unwrap()
        .block_on(check_cases(cases, None));
    assert!(
        mismatches.is_empty(),
        "unknown-column compound parity mismatches ({}):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn order_by_and_limit_before_compound_ops_match_stock_text_and_timing() {
    let cases: &[(&str, &str, &[i64], &str)] = &[
        (
            "top_level_order_before_union",
            "SELECT v FROM s ORDER BY v DESC UNION SELECT 1",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "derived_order_before_union",
            "SELECT * FROM (SELECT v FROM s ORDER BY v DESC UNION SELECT 1)",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "scalar_subquery_order_before_union",
            "SELECT (SELECT v FROM s ORDER BY v DESC UNION SELECT 1)",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "exists_order_before_union",
            "SELECT EXISTS(SELECT v FROM s ORDER BY v DESC UNION SELECT 1)",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "cte_order_before_union",
            "WITH c AS (SELECT v FROM s ORDER BY v DESC UNION SELECT 1) SELECT * FROM c",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "order_before_union_all",
            "SELECT 1 ORDER BY 1 UNION ALL SELECT 2",
            &[],
            "prepare error: ORDER BY clause should come after UNION ALL not before",
        ),
        (
            "order_before_intersect",
            "SELECT 1 ORDER BY 1 INTERSECT SELECT 2",
            &[],
            "prepare error: ORDER BY clause should come after INTERSECT not before",
        ),
        (
            "order_before_except",
            "SELECT 1 ORDER BY 1 EXCEPT SELECT 2",
            &[],
            "prepare error: ORDER BY clause should come after EXCEPT not before",
        ),
        (
            "limit_before_union",
            "SELECT v FROM s LIMIT 1 UNION SELECT 1",
            &[],
            "prepare error: LIMIT clause should come after UNION not before",
        ),
        (
            "limit_offset_before_union_all",
            "SELECT 1 LIMIT 1 OFFSET 0 UNION ALL SELECT 2",
            &[],
            "prepare error: LIMIT clause should come after UNION ALL not before",
        ),
        (
            "limit_before_intersect",
            "SELECT 1 LIMIT 1 INTERSECT SELECT 2",
            &[],
            "prepare error: LIMIT clause should come after INTERSECT not before",
        ),
        (
            "limit_comma_before_except",
            "SELECT 1 LIMIT 1, 2 EXCEPT SELECT 2",
            &[],
            "prepare error: LIMIT clause should come after EXCEPT not before",
        ),
        (
            "order_and_limit_before_union_prefers_order",
            "SELECT v FROM s ORDER BY v LIMIT 1 UNION SELECT 1",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "order_and_limit_before_except_prefers_order",
            "SELECT 1 ORDER BY 1 LIMIT 1 EXCEPT SELECT 2",
            &[],
            "prepare error: ORDER BY clause should come after EXCEPT not before",
        ),
        (
            "middle_arm_order_before_union",
            "SELECT 1 UNION SELECT 2 ORDER BY 1 UNION SELECT 3",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "middle_arm_limit_before_intersect",
            "SELECT 1 UNION SELECT 2 LIMIT 1 INTERSECT SELECT 3",
            &[],
            "prepare error: LIMIT clause should come after INTERSECT not before",
        ),
        (
            "rightmost_misplaced_clause_wins_limit",
            "SELECT 1 ORDER BY 1 UNION SELECT 2 LIMIT 1 INTERSECT SELECT 3",
            &[],
            "prepare error: LIMIT clause should come after INTERSECT not before",
        ),
        (
            "rightmost_misplaced_clause_wins_order",
            "SELECT 1 LIMIT 1 UNION SELECT 2 ORDER BY 1 INTERSECT SELECT 3",
            &[],
            "prepare error: ORDER BY clause should come after INTERSECT not before",
        ),
        (
            "first_arm_order_carried_past_plain_second_arm",
            "SELECT 1 ORDER BY 1 UNION SELECT 2 INTERSECT SELECT 3",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "order_before_union_values",
            "SELECT 1 ORDER BY 1 UNION VALUES (2)",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "values_then_select_order_before_union",
            "VALUES (1) UNION SELECT 2 ORDER BY 1 UNION SELECT 3",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "misplaced_clause_precedes_column_resolution",
            "SELECT 1 ORDER BY 1 UNION SELECT nosuchcol",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "insert_select_order_before_union",
            "INSERT INTO t SELECT 1 ORDER BY 1 UNION SELECT 2",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "create_view_order_before_union",
            "CREATE VIEW v_bad AS SELECT 1 ORDER BY 1 UNION SELECT 2",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
        (
            "create_trigger_order_before_union",
            "CREATE TRIGGER tr_bad AFTER INSERT ON t BEGIN SELECT 1 ORDER BY 1 UNION SELECT 2; END",
            &[],
            "prepare error: ORDER BY clause should come after UNION not before",
        ),
    ];

    let mismatches = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .unwrap()
        .block_on(check_cases(cases, None));
    assert!(
        mismatches.is_empty(),
        "ORDER BY / LIMIT before compound parity mismatches ({}):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn compound_select_negative_controls_and_dqs_match_stock() {
    let syntax_and_valid_cases: &[(&str, &str, &[i64], &str)] = &[
        (
            "order_union_semi_is_syntax_error",
            "SELECT 1 ORDER BY 1 UNION;",
            &[],
            "prepare error: near \";\": syntax error",
        ),
        (
            "order_union_select_semi_is_syntax_error",
            "SELECT 1 ORDER BY 1 UNION SELECT;",
            &[],
            "prepare error: near \";\": syntax error",
        ),
        (
            "order_union_select_from_semi_is_syntax_error",
            "SELECT 1 ORDER BY 1 UNION SELECT 2 FROM;",
            &[],
            "prepare error: near \";\": syntax error",
        ),
        (
            "order_union_bad_token_is_syntax_error",
            "SELECT 1 ORDER BY 1 UNION 123;",
            &[],
            "prepare error: near \"123\": syntax error",
        ),
        (
            "order_union_eof_is_incomplete_input",
            "SELECT 1 ORDER BY 1 UNION",
            &[],
            "prepare error: incomplete input",
        ),
        (
            "order_union_select_eof_is_incomplete_input",
            "SELECT 1 ORDER BY 1 UNION SELECT",
            &[],
            "prepare error: incomplete input",
        ),
        (
            "order_union_ok_then_bad_intersect_is_syntax_error",
            "SELECT 1 ORDER BY 1 UNION SELECT 2 INTERSECT;",
            &[],
            "prepare error: near \";\": syntax error",
        ),
        (
            "order_union_extra_rparen_is_syntax_error",
            "SELECT 1 ORDER BY 1 UNION SELECT 2);",
            &[],
            "prepare error: near \")\": syntax error",
        ),
        (
            "order_union_trailing_limit_order_is_syntax_error",
            "SELECT 1 ORDER BY 1 UNION SELECT 2 LIMIT 1 ORDER BY 1;",
            &[],
            "prepare error: near \"ORDER\": syntax error",
        ),
        (
            "values_order_before_union_is_syntax_error",
            "VALUES (1) ORDER BY 1 UNION SELECT 2;",
            &[],
            "prepare error: near \"ORDER\": syntax error",
        ),
        (
            "select_union_values_order_before_union_is_syntax_error",
            "SELECT 1 UNION VALUES (2) ORDER BY 1 UNION SELECT 3;",
            &[],
            "prepare error: near \"ORDER\": syntax error",
        ),
        (
            "values_limit_before_union_is_syntax_error",
            "VALUES (1) LIMIT 1 UNION SELECT 2;",
            &[],
            "prepare error: near \"LIMIT\": syntax error",
        ),
        (
            "bare_top_level_paren_select_is_syntax_error",
            "(SELECT v FROM s ORDER BY v DESC UNION SELECT 1);",
            &[],
            "prepare error: near \"(\": syntax error",
        ),
        (
            "valid_compound_order_and_limit",
            "SELECT v FROM s UNION SELECT 1 ORDER BY 1 DESC LIMIT 2",
            &[],
            "rows=[[\"2\"], [\"1\"]]",
        ),
        (
            "valid_compound_limit_offset",
            "SELECT 1 UNION ALL SELECT 2 ORDER BY 1 DESC LIMIT 1 OFFSET 1",
            &[],
            "rows=[[\"1\"]]",
        ),
        (
            "valid_intersect_order_limit",
            "SELECT v FROM s INTERSECT SELECT 2 ORDER BY v LIMIT 1",
            &[],
            "rows=[[\"2\"]]",
        ),
        (
            "valid_except_order_limit",
            "SELECT v FROM s EXCEPT SELECT 1 ORDER BY v LIMIT 1",
            &[],
            "rows=[[\"2\"]]",
        ),
        (
            "valid_limit_0_does_not_evaluate_overflow",
            "SELECT v FROM s UNION ALL SELECT abs(-9223372036854775808) LIMIT 0",
            &[],
            "rows=[]",
        ),
    ];

    let dqs_on_cases: &[(&str, &str, &[i64], &str)] = &[
        (
            "dqs_on_union_lit",
            r#"SELECT 1 UNION SELECT "dqs_lit""#,
            &[],
            "rows=[[\"1\"], [\"'dqs_lit'\"]]",
        ),
        (
            "dqs_on_union_all_limit_1",
            r#"SELECT 1 UNION ALL SELECT "dqs_lit" LIMIT 1"#,
            &[],
            "rows=[[\"1\"]]",
        ),
        (
            "dqs_on_both_arms",
            r#"SELECT "a" UNION SELECT "b""#,
            &[],
            "rows=[[\"'a'\"], [\"'b'\"]]",
        ),
        (
            "dqs_on_both_arms_limit_0",
            r#"SELECT "a" UNION ALL SELECT "b" LIMIT 0"#,
            &[],
            "rows=[]",
        ),
        (
            "dqs_on_table_arm2_limit_1",
            r#"SELECT v FROM s UNION ALL SELECT "lit" FROM s LIMIT 1"#,
            &[],
            "rows=[[\"1\"]]",
        ),
        (
            "dqs_on_mixed_with_unquoted_unknown_rhs",
            r#"SELECT "a" UNION SELECT nosuchcol"#,
            &[],
            "prepare error: no such column: nosuchcol",
        ),
        (
            "dqs_on_mixed_with_unquoted_unknown_lhs",
            r#"SELECT nosuchcol UNION SELECT "b""#,
            &[],
            "prepare error: no such column: nosuchcol",
        ),
    ];

    let dqs_off_cases: &[(&str, &str, &str, &str)] = &[
        (
            "dqs_off_union_lit_errors",
            r#"SELECT 1 UNION SELECT "dqs_lit""#,
            "prepare error: no such column: \"dqs_lit\" - should this be a string literal in single-quotes?",
            "prepare error: no such column: dqs_lit",
        ),
        (
            "dqs_off_union_all_limit_1_errors",
            r#"SELECT 1 UNION ALL SELECT "dqs_lit" LIMIT 1"#,
            "prepare error: no such column: \"dqs_lit\" - should this be a string literal in single-quotes?",
            "prepare error: no such column: dqs_lit",
        ),
    ];

    let rt = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .unwrap();
    let mut mismatches = rt.block_on(check_cases(syntax_and_valid_cases, None));
    mismatches.extend(rt.block_on(check_cases(dqs_on_cases, Some(true))));
    rt.block_on(async {
        for file_backed in [false, true] {
            let mode = if file_backed { "file" } else { "mem" };
            let (_dir, stock, fsqlite) = open_pair(file_backed).await;
            stock
                .set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DML, false)
                .unwrap();
            fsqlite.execute("PRAGMA fsqlite.dqs = OFF").await.unwrap();
            for &(label, sql, expected_stock, expected_fsqlite) in dqs_off_cases {
                let stock_res = stock_outcome(&stock, sql, &[]);
                assert_eq!(
                    stock_res, expected_stock,
                    "[{mode}/{label}] bundled sqlite3 oracle drift for `{sql}`"
                );
                let prep_res = fsqlite_outcome(&fsqlite, sql, &[], true).await;
                if prep_res != expected_fsqlite {
                    mismatches.push(format!(
                        "[{mode}/{label}/prepared] `{sql}`\n  expected: {expected_fsqlite}\n  fsqlite:  {prep_res}"
                    ));
                }
                let direct_res = fsqlite_outcome(&fsqlite, sql, &[], false).await;
                let expected_direct = strip_phase(expected_fsqlite);
                if direct_res != expected_direct {
                    mismatches.push(format!(
                        "[{mode}/{label}/direct] `{sql}`\n  expected: {expected_direct}\n  fsqlite:  {direct_res}"
                    ));
                }
            }
        }
    });

    // View deferral control: CREATE VIEW accepts a compound with an unknown
    // column, and selecting from the view reports `no such column` at prepare.
    rt.block_on(async {
        for file_backed in [false, true] {
            let mode = if file_backed { "file" } else { "mem" };
            let (_dir, stock, fsqlite) = open_pair(file_backed).await;
            let create_sql = "CREATE VIEW v_bad_col AS SELECT 1 UNION ALL SELECT nosuchcol LIMIT 1";
            assert_eq!(stock_outcome(&stock, create_sql, &[]), "rows=[]");
            assert_eq!(
                fsqlite_outcome(&fsqlite, create_sql, &[], true).await,
                "rows=[]",
                "[{mode}/view_create] CREATE VIEW should defer unknown column validation"
            );
            let select_sql = "SELECT * FROM v_bad_col";
            let stock_res = stock_outcome(&stock, select_sql, &[]);
            assert_eq!(stock_res, "prepare error: no such column: nosuchcol");
            let prep_res = fsqlite_outcome(&fsqlite, select_sql, &[], true).await;
            if prep_res != stock_res {
                mismatches.push(format!(
                    "[{mode}/view_select/prepared] `{select_sql}`\n  stock:   {stock_res}\n  fsqlite: {prep_res}"
                ));
            }
        }
    });

    assert!(
        mismatches.is_empty(),
        "negative-control / DQS compound parity mismatches ({}):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
