//! Differential probe: classify `CodegenError::Unsupported` sites in
//! fsqlite-vdbe/src/codegen.rs as GENUINE feature gaps vs. legit errors vs.
//! constructs that fall back to the interpreter and work.
//!
//! For each candidate we run the SAME SQL on FrankenSQLite and on rusqlite
//! (real SQLite) and classify:
//!   GAP   frank ERR, rusqlite OK   -> real construct we reject (bead-worthy)
//!   ok    both succeed             -> handled (codegen rejects but eval falls back)
//!   legit both error               -> correct rejection
//!   loose frank OK, rusqlite ERR   -> we are too permissive (different issue)
//!
//! Diagnostic harness, not an assertion suite. Run with:
//!   cargo test -p fsqlite-core --test codegen_gap_probe -- --nocapture

use fsqlite_core::connection::Connection;

fn frank_run(setup: &[&str], sql: &str, is_query: bool) -> Result<(), String> {
    let conn = Connection::open(":memory:").map_err(|e| format!("open: {e}"))?;
    for s in setup {
        conn.execute(s).map_err(|e| format!("setup `{s}`: {e}"))?;
    }
    if is_query {
        conn.query(sql).map(|_| ()).map_err(|e| e.to_string())
    } else {
        conn.execute(sql).map(|_| ()).map_err(|e| e.to_string())
    }
}

fn sqlite_run(setup: &[&str], sql: &str, is_query: bool) -> Result<(), String> {
    let conn = rusqlite::Connection::open_in_memory().map_err(|e| format!("open: {e}"))?;
    for s in setup {
        conn.execute_batch(s)
            .map_err(|e| format!("setup `{s}`: {e}"))?;
    }
    if is_query {
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let n = stmt.column_count();
        stmt.query_map([], |row| {
            for i in 0..n {
                let _: rusqlite::types::Value = row.get_unwrap(i);
            }
            Ok(())
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map(|_| ())
        .map_err(|e| e.to_string())
    } else {
        conn.execute_batch(sql).map_err(|e| e.to_string())
    }
}

fn classify(label: &str, setup: &[&str], sql: &str, is_query: bool) {
    let f = frank_run(setup, sql, is_query);
    let s = sqlite_run(setup, sql, is_query);
    match (&f, &s) {
        (Err(fe), Ok(())) => println!("GAP   {label}: {sql}\n        frank => {fe}"),
        (Ok(()), Ok(())) => println!("ok    {label}"),
        (Err(_), Err(_)) => println!("legit {label}"),
        (Ok(()), Err(se)) => println!("loose {label}: rusqlite rejects => {se}"),
    }
}

// Diagnostic harness, not a pass/fail gate: it prints a classification per
// candidate and never asserts (so it does not break when a gap is implemented).
// Run on demand with:
//   cargo test -p fsqlite-core --test codegen_gap_probe -- --ignored --nocapture
// Confirmed genuine gaps from this probe are tracked as
// bd-cz9o6, bd-6utze, bd-kkfok, bd-p0xje (all UPDATE-codegen).
#[test]
#[ignore = "diagnostic triage harness; run with --ignored --nocapture"]
fn codegen_gap_probe() {
    let t = &[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)",
        "INSERT INTO t VALUES (1,10,100,'x'),(2,20,200,'y'),(3,30,300,'z')",
    ][..];
    let ts = &[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER, w INTEGER)",
        "INSERT INTO t VALUES (1,0),(2,0),(3,0)",
        "INSERT INTO src VALUES (1,11,111),(2,22,222),(3,33,333)",
    ][..];

    // ---- multi-column SET (UPDATE) — SQLite 3.15+ ----
    classify(
        "update_multicol_set_literals",
        t,
        "UPDATE t SET (a, b) = (99, 999) WHERE id = 1",
        false,
    );
    classify(
        "update_multicol_set_subquery",
        ts,
        "UPDATE t SET (v) = (SELECT max(v) FROM src) WHERE id = 1",
        false,
    );
    classify(
        "update_multicol_set_rowvalue_subquery",
        ts,
        "UPDATE t SET (v) = (SELECT v FROM src WHERE src.id = t.id) WHERE id = 2",
        false,
    );

    // ---- UPDATE ... FROM — SQLite 3.33+ ----
    classify(
        "update_from_subquery",
        ts,
        "UPDATE t SET v = s.v FROM (SELECT id, v FROM src) s WHERE t.id = s.id",
        false,
    );
    classify(
        "update_from_join",
        ts,
        "UPDATE t SET v = src.w FROM src JOIN (SELECT 1 AS k) k ON 1=1 WHERE t.id = src.id",
        false,
    );

    // ---- UPDATE / DELETE with ORDER BY / LIMIT ----
    // (Whether rusqlite accepts depends on SQLITE_ENABLE_UPDATE_DELETE_LIMIT;
    //  the differential classifies it correctly either way.)
    classify(
        "delete_limit",
        t,
        "DELETE FROM t WHERE a > 0 LIMIT 1",
        false,
    );
    classify(
        "delete_order_by_limit",
        t,
        "DELETE FROM t ORDER BY id LIMIT 1",
        false,
    );
    classify(
        "update_limit",
        t,
        "UPDATE t SET a = a + 1 WHERE b > 0 LIMIT 1",
        false,
    );
    classify(
        "update_order_by_limit",
        t,
        "UPDATE t SET a = a + 1 ORDER BY id LIMIT 1",
        false,
    );

    // ---- CREATE TABLE complex DEFAULT expressions ----
    classify(
        "create_default_fncall",
        &[],
        "CREATE TABLE x (a INTEGER DEFAULT (abs(-5)))",
        false,
    );
    classify(
        "create_default_arith",
        &[],
        "CREATE TABLE x (a INTEGER DEFAULT (2 + 3 * 4))",
        false,
    );

    // ---- assorted constructs that may or may not fall back ----
    classify(
        "insert_default_values",
        &["CREATE TABLE x (a INTEGER DEFAULT 7, b TEXT DEFAULT 'q')"],
        "INSERT INTO x DEFAULT VALUES",
        false,
    );
    classify(
        "update_set_from_self_join",
        ts,
        "UPDATE t SET v = (SELECT w FROM src WHERE src.id = t.id)",
        false,
    );
    classify(
        "delete_using_subselect_limit",
        t,
        "DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY a DESC LIMIT 1)",
        false,
    );

    // ---- second batch: more UPDATE / INSERT variants ----
    // aliased UPDATE target (SQLite 3.33+)
    classify(
        "update_aliased_target",
        t,
        "UPDATE t AS x SET a = 1 WHERE x.id = 1",
        false,
    );
    // UPDATE ... FROM with table alias
    classify(
        "update_from_table_alias",
        ts,
        "UPDATE t SET v = s.v FROM src AS s WHERE t.id = s.id",
        false,
    );
    // UPDATE ... FROM comma-joined multiple tables
    classify(
        "update_from_comma_tables",
        ts,
        "UPDATE t SET v = src.v FROM src, (SELECT 1) WHERE t.id = src.id",
        false,
    );
    // multi-target (2-col) row-value SET from correlated subquery
    classify(
        "update_multicol2_subquery",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "INSERT INTO t VALUES (1,0,0),(2,0,0)",
            "INSERT INTO src VALUES (1,7,8),(2,9,10)",
        ],
        "UPDATE t SET (a, b) = (SELECT a, b FROM src WHERE src.id = t.id)",
        false,
    );
    // INSERT ... SELECT with ORDER BY / LIMIT
    classify(
        "insert_select_order_limit",
        &[
            "CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TABLE dst (id INTEGER, v INTEGER)",
            "INSERT INTO src VALUES (1,30),(2,10),(3,20)",
        ],
        "INSERT INTO dst SELECT id, v FROM src ORDER BY v DESC LIMIT 2",
        false,
    );
    // INSERT ... SELECT compound (UNION)
    classify(
        "insert_select_union",
        &[
            "CREATE TABLE a (x INTEGER)",
            "CREATE TABLE b (x INTEGER)",
            "CREATE TABLE dst (x INTEGER)",
            "INSERT INTO a VALUES (1),(2)",
            "INSERT INTO b VALUES (3),(4)",
        ],
        "INSERT INTO dst SELECT x FROM a UNION SELECT x FROM b",
        false,
    );
    // UPSERT targeting a non-PK unique index with DO UPDATE
    classify(
        "upsert_do_update_unique",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT UNIQUE, n INTEGER)",
            "INSERT INTO t VALUES (1,'a',1)",
        ],
        "INSERT INTO t (id,k,n) VALUES (2,'a',5) ON CONFLICT(k) DO UPDATE SET n = n + excluded.n",
        false,
    );
    // RETURNING on UPDATE (SQLite 3.35+)
    classify(
        "update_returning",
        t,
        "UPDATE t SET a = a + 1 WHERE id = 1 RETURNING id, a",
        true,
    );
    // RETURNING on DELETE
    classify(
        "delete_returning",
        t,
        "DELETE FROM t WHERE id = 1 RETURNING id, a",
        true,
    );
}

// Second diagnostic harness: broader SELECT / aggregate / window / compound /
// table-valued constructs (areas the first probe does not exercise). Same GAP /
// ok / legit / loose classification. Run with:
//   cargo test -p fsqlite-core --test codegen_gap_probe -- --ignored --nocapture
#[test]
#[ignore = "diagnostic triage harness; run with --ignored --nocapture"]
fn codegen_gap_probe_select_window_aggregate() {
    let t = &[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, g INTEGER, v INTEGER)",
        "INSERT INTO t VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,2,50)",
    ][..];

    // ---- aggregate FILTER clause (SQLite 3.30+) ----
    classify(
        "agg_filter_clause",
        t,
        "SELECT g, count(*) FILTER (WHERE v > 20) AS c FROM t GROUP BY g ORDER BY g",
        true,
    );
    classify(
        "agg_filter_sum",
        t,
        "SELECT sum(v) FILTER (WHERE v < 40) AS s FROM t",
        true,
    );

    // ---- window frame variants ----
    classify(
        "window_range_frame",
        t,
        "SELECT v, sum(v) OVER (ORDER BY v RANGE BETWEEN 15 PRECEDING AND 15 FOLLOWING) FROM t ORDER BY v",
        true,
    );
    classify(
        "window_groups_frame",
        t,
        "SELECT v, sum(v) OVER (ORDER BY v GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY v",
        true,
    );
    classify(
        "window_exclude_current",
        t,
        "SELECT v, sum(v) OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t ORDER BY v",
        true,
    );
    classify(
        "window_named_window_clause",
        t,
        "SELECT v, row_number() OVER w FROM t WINDOW w AS (ORDER BY v) ORDER BY v",
        true,
    );
    classify(
        "window_filter_on_window_agg",
        t,
        "SELECT v, sum(v) FILTER (WHERE v > 15) OVER (ORDER BY v) FROM t ORDER BY v",
        true,
    );

    // ---- VALUES as a relation / top-level ----
    classify("values_top_level", &[], "VALUES (1, 2), (3, 4)", true);
    classify(
        "select_from_values",
        &[],
        "SELECT column1, column2 FROM (VALUES (1, 'a'), (2, 'b')) ORDER BY column1",
        true,
    );

    // ---- compound SELECT edges ----
    classify(
        "compound_order_by_ordinal",
        t,
        "SELECT v FROM t UNION SELECT v + 1000 FROM t ORDER BY 1 DESC LIMIT 3",
        true,
    );
    classify(
        "compound_limit_on_values",
        &[],
        "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 LIMIT 2",
        true,
    );

    // ---- correlated scalar subquery in projection ----
    classify(
        "correlated_scalar_in_select",
        t,
        "SELECT id, (SELECT count(*) FROM t t2 WHERE t2.g = t.g) AS gc FROM t ORDER BY id",
        true,
    );

    // ---- DISTINCT aggregate + GROUP BY HAVING ----
    classify(
        "group_by_having_count_distinct",
        t,
        "SELECT g, count(DISTINCT v) AS d FROM t GROUP BY g HAVING count(DISTINCT v) > 1 ORDER BY g",
        true,
    );

    // ---- json table-valued + scalar ----
    classify(
        "json_each_table_valued",
        &[],
        "SELECT value FROM json_each('[10,20,30]') ORDER BY value",
        true,
    );

    // ---- SELECT with LIMIT and OFFSET expression ----
    classify(
        "select_limit_offset_expr",
        t,
        "SELECT v FROM t ORDER BY v LIMIT 2 + 1 OFFSET 1",
        true,
    );
}
