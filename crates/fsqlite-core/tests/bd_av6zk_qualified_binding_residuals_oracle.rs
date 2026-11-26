#![recursion_limit = "512"]

//! bd-av6zk: residual divergences of the bd-at0bx three-part column binder
//! (`crates/fsqlite-core/src/connection/qualified_column_binding.rs`) against
//! live bundled SQLite 3.54.1 (`rusqlite`).
//!
//! Covers all three items in `bd-av6zk`:
//! 1. Missing column of views with unaliased computed columns, table-valued
//!    functions (`json_each` / `json_tree`), and two-part missing columns
//!    alongside same-named `main`/`temp` tables — matching stock's exact error
//!    message and never leaking synthetic `name@db` aliases or
//!    `internal error: column not found:`.
//! 2. Error precedence in statements with multiple errors: stock reports the
//!    first error in walk order (`no such column: nosuch`, `no such function:
//!    nosuchfn`) rather than reporting a later three-part column error first.
//! 3. `sqlite_master` / `sqlite_schema` / `sqlite_temp_master` /
//!    `sqlite_temp_schema` database qualifiers across `main`, `temp`, and
//!    attached schemas.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Rows(Vec<Vec<String>>),
    Err(String),
}

fn fmt_fsqlite_val(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(i) => format!("I:{i}"),
        SqliteValue::Float(f) => format!("R:{f:?}"),
        SqliteValue::Text(s) => format!("T:{s}"),
        SqliteValue::Blob(b) => format!("B:{b:?}"),
    }
}

fn fmt_rusqlite_val(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(i) => format!("I:{i}"),
        rusqlite::types::Value::Real(f) => format!("R:{f:?}"),
        rusqlite::types::Value::Text(s) => format!("T:{s}"),
        rusqlite::types::Value::Blob(b) => format!("B:{b:?}"),
    }
}

fn normalize_error(msg: &str) -> String {
    // Strip any SQLite prefix like "Error code ...: " or trailing " in ... at offset N"
    // while preserving the full message so synthetic aliases (`name@db`) and
    // `internal error:` leaks ALWAYS cause a visible mismatch.
    let trimmed = msg.trim();
    if let Some(idx) = trimmed.find(" in ")
        && trimmed[idx..].contains(" at offset ")
    {
        return trimmed[..idx].trim().to_owned();
    }
    trimmed.to_owned()
}

fn stock_query(conn: &rusqlite::Connection, sql: &str) -> Outcome {
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => return Outcome::Err(normalize_error(&e.to_string())),
    };
    let col_count = stmt.column_count();
    let rows_iter = match stmt.query_map([], |row| {
        let mut cols = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val: rusqlite::types::Value = row.get(i)?;
            cols.push(fmt_rusqlite_val(&val));
        }
        Ok(cols)
    }) {
        Ok(iter) => iter,
        Err(e) => return Outcome::Err(normalize_error(&e.to_string())),
    };
    let mut out = Vec::new();
    for r in rows_iter {
        match r {
            Ok(row) => out.push(row),
            Err(e) => return Outcome::Err(normalize_error(&e.to_string())),
        }
    }
    Outcome::Rows(out)
}

async fn frank_query_direct(conn: &Connection, sql: &str) -> Outcome {
    match conn.query(sql).await {
        Ok(rows) => Outcome::Rows(
            rows.into_iter()
                .map(|r| r.values().iter().map(fmt_fsqlite_val).collect())
                .collect(),
        ),
        Err(e) => Outcome::Err(normalize_error(&e.to_string())),
    }
}

async fn frank_query_prepared(conn: &Connection, sql: &str) -> Outcome {
    let stmt = match conn.prepare(sql).await {
        Ok(s) => s,
        Err(e) => return Outcome::Err(normalize_error(&e.to_string())),
    };
    match stmt.query().await {
        Ok(rows) => Outcome::Rows(
            rows.into_iter()
                .map(|r| r.values().iter().map(fmt_fsqlite_val).collect())
                .collect(),
        ),
        Err(e) => Outcome::Err(normalize_error(&e.to_string())),
    }
}

const SETUP_SQL: &str = "\
CREATE TABLE t(x INTEGER);
INSERT INTO t VALUES (10);
CREATE TABLE users(name TEXT);
INSERT INTO users VALUES ('m');
CREATE TEMP TABLE users(name TEXT);
INSERT INTO temp.users VALUES ('t');
CREATE VIEW vo AS SELECT x + 1, x FROM t;
CREATE TEMP VIEW vo AS SELECT 1 AS y;
ATTACH ':memory:' AS aux;
CREATE TABLE aux.a(k INTEGER);
INSERT INTO aux.a VALUES (42);
";

const CASES: &[(&str, &str)] = &[
    // ── Item 1: view with unaliased computed column, TVF, and two-part missing column ──
    (
        "view_computed_join_missing_col_no_synthetic_alias_leak",
        "SELECT main.vo.nosuch, temp.vo.y FROM main.vo JOIN temp.vo",
    ),
    (
        "view_computed_single_missing_col_keeps_schema_qualifier",
        "SELECT main.vo.nosuch FROM main.vo",
    ),
    (
        "view_computed_join_valid_cols_control",
        "SELECT main.vo.x, temp.vo.y FROM main.vo JOIN temp.vo",
    ),
    (
        "view_computed_aliased_missing_col",
        "SELECT main.v.nosuch FROM main.vo AS v",
    ),
    (
        "view_computed_aliased_wrong_table_name",
        "SELECT main.vo.nosuch FROM main.vo AS v",
    ),
    (
        "tvf_json_each_missing_col",
        "SELECT main.json_each.nosuch FROM json_each('[1]')",
    ),
    (
        "tvf_json_each_valid_value_control",
        "SELECT main.json_each.value FROM json_each('[1]')",
    ),
    (
        "tvf_json_each_valid_rowid_control",
        "SELECT main.json_each.rowid FROM json_each('[1]')",
    ),
    (
        "tvf_json_tree_missing_col",
        "SELECT main.json_tree.nosuch FROM json_tree('[1]')",
    ),
    (
        "tvf_json_tree_valid_value_control",
        "SELECT main.json_tree.value FROM json_tree('[1]')",
    ),
    (
        "two_part_missing_col_with_same_named_main_and_temp_tables",
        "SELECT main.users.name, users.nosuch FROM main.users JOIN temp.users",
    ),
    (
        "three_part_missing_col_main_with_same_named_tables",
        "SELECT main.users.name, main.users.nosuch FROM main.users JOIN temp.users",
    ),
    (
        "three_part_missing_col_temp_with_same_named_tables",
        "SELECT main.users.name, temp.users.nosuch FROM main.users JOIN temp.users",
    ),
    (
        "three_part_valid_join_same_named_tables_control",
        "SELECT main.users.name, temp.users.name FROM main.users JOIN temp.users",
    ),
    // ── Item 2: walk-order error precedence with three-part errors ──
    (
        "precedence_bare_nosuch_before_three_part",
        "SELECT nosuch, temp.t.x FROM t",
    ),
    (
        "precedence_three_part_before_bare_nosuch_control",
        "SELECT temp.t.x, nosuch FROM t",
    ),
    (
        "precedence_unknown_fn_before_three_part",
        "SELECT nosuchfn(1), temp.t.x FROM t",
    ),
    (
        "precedence_three_part_before_unknown_fn_control",
        "SELECT temp.t.x, nosuchfn(1) FROM t",
    ),
    (
        "precedence_two_part_nosuch_before_three_part",
        "SELECT t.nosuch, temp.t.x FROM t",
    ),
    (
        "precedence_three_part_before_two_part_nosuch_control",
        "SELECT temp.t.x, t.nosuch FROM t",
    ),
    (
        "precedence_where_nosuch_before_three_part",
        "SELECT x FROM t WHERE nosuch = 1 AND temp.t.x = 1",
    ),
    (
        "precedence_where_three_part_before_nosuch_control",
        "SELECT x FROM t WHERE temp.t.x = 1 AND nosuch = 1",
    ),
    (
        "precedence_valid_select_alias_in_where_still_reports_three_part",
        "SELECT x AS a FROM t WHERE a = 1 AND temp.t.x = 1",
    ),
    (
        "precedence_valid_select_alias_in_order_by_still_reports_three_part",
        "SELECT x AS a FROM t ORDER BY a, temp.t.x",
    ),
    (
        "precedence_valid_builtin_fn_before_three_part_control",
        "SELECT abs(x), temp.t.x FROM t",
    ),
    (
        "precedence_valid_window_fn_before_three_part_control",
        "SELECT row_number() OVER (), temp.t.x FROM t",
    ),
    // ── Item 3: sqlite_master / sqlite_schema / sqlite_temp_master / sqlite_temp_schema ──
    (
        "sqlite_master_wrong_temp_qualifier",
        "SELECT temp.sqlite_master.name FROM sqlite_master",
    ),
    (
        "sqlite_schema_wrong_temp_qualifier",
        "SELECT temp.sqlite_schema.name FROM sqlite_schema",
    ),
    (
        "sqlite_temp_master_wrong_main_qualifier",
        "SELECT main.sqlite_temp_master.name FROM sqlite_temp_master",
    ),
    (
        "sqlite_temp_schema_wrong_main_qualifier",
        "SELECT main.sqlite_temp_schema.name FROM sqlite_temp_schema",
    ),
    (
        "sqlite_master_valid_main_qualifier_control",
        "SELECT main.sqlite_master.name FROM sqlite_master WHERE main.sqlite_master.type = 'table' AND main.sqlite_master.name = 't'",
    ),
    (
        "sqlite_temp_master_valid_temp_qualifier_control",
        "SELECT temp.sqlite_temp_master.name FROM sqlite_temp_master WHERE temp.sqlite_temp_master.type = 'table' AND temp.sqlite_temp_master.name = 'users'",
    ),
    (
        "sqlite_master_missing_column",
        "SELECT main.sqlite_master.nosuch FROM sqlite_master",
    ),
    (
        "temp_sqlite_master_valid_temp_qualifier_control",
        "SELECT temp.sqlite_master.name FROM temp.sqlite_master WHERE temp.sqlite_master.type = 'table' AND temp.sqlite_master.name = 'users'",
    ),
    (
        "sqlite_master_join_main_and_temp_control",
        "SELECT main.sqlite_master.name, temp.sqlite_master.name FROM main.sqlite_master JOIN temp.sqlite_master ON main.sqlite_master.name = 't' AND temp.sqlite_master.name = 'users'",
    ),
    (
        "sqlite_master_valid_rowid_control",
        "SELECT main.sqlite_master.rowid FROM sqlite_master WHERE name = 't'",
    ),
    (
        "aux_sqlite_master_valid_qualifier_control",
        "SELECT aux.sqlite_master.name FROM aux.sqlite_master WHERE aux.sqlite_master.name = 'a'",
    ),
    (
        "aux_sqlite_master_wrong_main_qualifier",
        "SELECT main.sqlite_master.name FROM aux.sqlite_master",
    ),
    (
        "main_sqlite_master_wrong_aux_qualifier",
        "SELECT aux.sqlite_master.name FROM sqlite_master",
    ),
];

async fn run_suite(label: &str, fconn: &Connection, rconn: &rusqlite::Connection) {
    fconn.execute_batch(SETUP_SQL).await.expect("frank setup");
    rconn.execute_batch(SETUP_SQL).expect("stock setup");

    let mut diffs = Vec::new();
    for &(name, sql) in CASES {
        let stock = stock_query(rconn, sql);
        let direct = frank_query_direct(fconn, sql).await;
        let prepared = frank_query_prepared(fconn, sql).await;
        for (mode, actual) in [("direct", &direct), ("prepared", &prepared)] {
            if let Outcome::Err(msg) = actual
                && (msg.contains('@') || msg.contains("internal error:"))
            {
                diffs.push(format!(
                    "[{label} {mode}] `{name}` LEAKED synthetic alias or internal error:\n  sql:   {sql}\n  stock: {stock:?}\n  frank: {actual:?}"
                ));
                continue;
            }
            if actual != &stock {
                diffs.push(format!(
                    "[{label} {mode}] `{name}`:\n  sql:   {sql}\n  stock: {stock:?}\n  frank: {actual:?}"
                ));
            }
        }
    }
    assert!(
        diffs.is_empty(),
        "[{label}] {} mismatch(es) across {} cases:\n{}",
        diffs.len(),
        CASES.len(),
        diffs.join("\n")
    );
}

#[test]
fn qualified_column_binding_residuals_match_stock_3541() {
    asupersync::test_utils::run_test(|| async {
        // 1. :memory:
        let fmem = Connection::open(":memory:").await.expect("fmem");
        let rmem = rusqlite::Connection::open_in_memory().expect("rmem");
        run_suite("memory", &fmem, &rmem).await;

        // 2. File-backed
        let dir = tempfile::tempdir().expect("tempdir");
        let fpath = dir.path().join("frank.db");
        let rpath = dir.path().join("stock.db");
        let ffile = Connection::open(fpath.to_str().expect("utf8"))
            .await
            .expect("ffile");
        let rfile = rusqlite::Connection::open(&rpath).expect("rfile");
        run_suite("file", &ffile, &rfile).await;
    });
}
