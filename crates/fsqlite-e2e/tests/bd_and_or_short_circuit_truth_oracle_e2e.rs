//! bd-and-or-short-circuit-value-jump-gaps-dkswh (GAP-1/2) — Oracle-parity e2e.
//!
//! In TRUTH contexts (WHERE clause, CASE-WHEN condition), stock sqlite3
//! short-circuits AND/OR left-to-right and never evaluates the skipped operand.
//! A would-be-skipped operand that ERRORS must therefore NOT make the whole
//! query error — stock returns the row.
//!
//! Two families of repro, deliberately separated to isolate the failing organ:
//!   * `_subq_` cases use `EXISTS(SELECT 1 FROM json_each(x))` — a CORRELATED
//!     subquery, which routes SELECTs through the connection.rs semantic
//!     fallback (`execute_correlated_subquery_where_fallback`), NOT codegen.
//!   * `_scalar_` cases use `json_extract(x,'$.a')` — a plain scalar function on
//!     the in-scope column, no subquery, so the query stays in VDBE codegen
//!     (`emit_where_filter` / `emit_case_expr`). These isolate the codegen
//!     truth-context AND/OR short-circuit behavior.
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

const DDL: [&str; 2] = [
    "CREATE TABLE t(x TEXT)",
    "INSERT INTO t VALUES ('{}'), ('bare')",
];

async fn setup_mem() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in DDL {
        f.execute(s).await.unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s).unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

// ── Original bead repro: correlated EXISTS(json_each(x)) — connection.rs path ──

/// GAP-1 (subquery form): WHERE OR short-circuit with a correlated EXISTS.
///
/// HANDBACK (Option B, connection.rs — NOT codegen). This SELECT never reaches
/// VDBE codegen: `select_correlated_exists_where_requires_fallback` routes it to
/// `execute_correlated_subquery_where_fallback` -> `execute_join_select`, whose
/// WHERE filter (connection.rs ~line 78924) formerly called the EAGER
/// `eval_expr_with_subqueries` on the whole predicate rather than the
/// short-circuiting `eval_expr_truthiness`. Two connection.rs defects stack here:
///   1. FIXED: the WHERE filter now routes through `eval_expr_truthiness`, so
///      AND/OR short-circuit left-to-right. The `x='bare'` row (left disjunct
///      TRUE) is let through without evaluating the erroring EXISTS.
///   2. STILL OPEN (blocks this all-or-nothing single-query keeper): an
///      UNQUALIFIED outer column `x` is not substituted into the `json_each(x)`
///      table-valued-function argument in the correlated-EXISTS WHERE fallback,
///      so the nested `SELECT 1 FROM json_each(x)` fails with
///      "column not found: x". This fires on the `x='{}'` row, which genuinely
///      must evaluate the EXISTS — short-circuit alone does not fix it. A
///      QUALIFIED ref (`json_each(t.x)`) already substitutes correctly, so the
///      lever is the unqualified-outer-ref binding in the TVF-arg substitution
///      (see `probe_unqualified_external_ref` in
///      `substitute_outer_refs_in_select_with_schema_context`).
/// Fix belongs in connection.rs (IvoryFortress's lane); ignored until gap (2) lands.
#[test]
#[ignore = "bd-and-or-short-circuit-value-jump-gaps-dkswh: fix(1) short-circuit LANDED (execute_join_select WHERE routes through eval_expr_truthiness; the 'bare' row now passes). Still red on x='{}': gap(2) unqualified outer col `x` is not substituted into the json_each(x) TVF arg in the correlated-EXISTS WHERE fallback (errors \"column not found: x\"); qualified t.x already works. Un-ignore once gap(2) lands."]
fn where_or_subq_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup_mem().await;
        check(
            &f,
            &r,
            &["SELECT x FROM t WHERE x IS NOT '{}' OR EXISTS(SELECT 1 FROM json_each(x)) ORDER BY x"],
            "where_or_subq_short_circuits",
        )
        .await;
    });
}

/// GAP-2 (subquery form): CASE-WHEN condition OR short-circuit with a correlated EXISTS.
#[test]
fn case_or_subq_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup_mem().await;
        check(
            &f,
            &r,
            &["SELECT CASE WHEN (x IS NOT '{}' OR EXISTS(SELECT 1 FROM json_each(x))) THEN 1 ELSE 0 END AS c FROM t ORDER BY x"],
            "case_or_subq_short_circuits",
        )
        .await;
    });
}

// ── Codegen-path isolation: scalar json_extract(x,'$.a'), no subquery ──────────
// json_extract('bare', '$.a') raises "malformed JSON" in stock and fsqlite;
// json_extract('{}', '$.a') is NULL in both. So the ONLY way the row x='bare'
// survives (OR) or is filtered without error (AND) is genuine short-circuit.

/// GAP-1 (codegen WHERE): left TRUE must skip the erroring right disjunct.
#[test]
fn where_or_scalar_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup_mem().await;
        check(
            &f,
            &r,
            &["SELECT x FROM t WHERE x IS NOT '{}' OR json_extract(x,'$.a') IS NOT NULL ORDER BY x"],
            "where_or_scalar_short_circuits",
        )
        .await;
    });
}

/// GAP-1 (codegen WHERE, AND polarity): left FALSE must skip the erroring right conjunct.
#[test]
fn where_and_scalar_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup_mem().await;
        check(
            &f,
            &r,
            &["SELECT x FROM t WHERE x = '{}' AND json_extract(x,'$.a') IS NOT NULL ORDER BY x"],
            "where_and_scalar_short_circuits",
        )
        .await;
    });
}

/// GAP-2 (codegen CASE-WHEN): left TRUE must skip the erroring right disjunct.
#[test]
fn case_or_scalar_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup_mem().await;
        check(
            &f,
            &r,
            &["SELECT CASE WHEN (x IS NOT '{}' OR json_extract(x,'$.a') IS NOT NULL) THEN 1 ELSE 0 END AS c FROM t ORDER BY x"],
            "case_or_scalar_short_circuits",
        )
        .await;
    });
}

/// GAP-2 (codegen CASE-WHEN, AND polarity): left FALSE must skip the erroring right conjunct.
#[test]
fn case_and_scalar_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup_mem().await;
        check(
            &f,
            &r,
            &["SELECT CASE WHEN (x = '{}' AND json_extract(x,'$.a') IS NOT NULL) THEN 1 ELSE 0 END AS c FROM t ORDER BY x"],
            "case_and_scalar_short_circuits",
        )
        .await;
    });
}

/// GAP-2 (codegen ON CONFLICT DO UPDATE searched-CASE-WHEN): the searched-CASE in
/// an upsert SET clause is a TRUTH context too — left TRUE must skip the erroring
/// right disjunct (emit_upsert_expr path, distinct from emit_case_expr).
#[test]
fn upsert_case_or_scalar_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let ddl = [
            "CREATE TABLE u(id INTEGER PRIMARY KEY, x TEXT, note TEXT)",
            "INSERT INTO u VALUES (1, '{}', 'orig')",
            "INSERT INTO u(id, x, note) VALUES (1, 'bare', 'ignored') \
             ON CONFLICT(id) DO UPDATE SET note = \
             CASE WHEN (excluded.x IS NOT '{}' OR json_extract(excluded.x,'$.a') IS NOT NULL) \
             THEN 'matched' ELSE 'nomatch' END",
        ];
        for s in ddl {
            f.execute(s).await.unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
            r.execute_batch(s).unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
        }
        check(
            &f,
            &r,
            &["SELECT note FROM u WHERE id = 1"],
            "upsert_case_or_scalar_short_circuits",
        )
        .await;
    });
}

// ── GAP-3 / root-cause-A: `0 AND E` folds BEFORE the eager subquery hoist ───────
// An UNCORRELATED subquery `(SELECT count(*) FROM json_each('bare'))` errors if
// evaluated. It was eager-executed at PREPARE time (connection.rs rewrite_in_expr
// Exists/Subquery arms) BEFORE the planner GAP-3 fold, so `0 AND E` / `E AND 0`
// errored in the FROM-bearing result-column / HAVING / nested-comparison paths
// where stock sqlite3 3.46.1 folds to FALSE and never evaluates E (verified). The
// fix folds `0 AND E` -> FALSE at the top of rewrite_in_expr, one phase earlier,
// so the dead operand's subquery is never hoisted. FALSE absorbs AND in every
// context/position/polarity.
#[test]
fn and_false_folds_before_uncorrelated_subquery_hoist() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup_mem().await;
        check(
            &f,
            &r,
            &[
                // value context, FROM-bearing (defeats the no-FROM fast path):
                "SELECT (0 AND (SELECT count(*) FROM json_each('bare'))) FROM t ORDER BY x",
                // literal 0 on the RIGHT arm:
                "SELECT ((SELECT count(*) FROM json_each('bare')) AND 0) FROM t ORDER BY x",
                // HAVING truth context:
                "SELECT count(*) FROM t HAVING 0 AND (SELECT count(*) FROM json_each('bare'))",
                // nested inside a comparison operand — still folds (context-independent):
                "SELECT x FROM t WHERE (0 AND (SELECT count(*) FROM json_each('bare'))) = 0 ORDER BY x",
                // under NOT — polarity-independent:
                "SELECT x FROM t WHERE NOT(0 AND (SELECT count(*) FROM json_each('bare'))) ORDER BY x",
            ],
            "and_false_folds_before_uncorrelated_subquery_hoist",
        )
        .await;
    });
}

/// Guard against OVER-folding: only the *integer literal 0* AND-absorbs. Every
/// other value-context form stays EAGER and must ERROR exactly as stock does
/// (`check` treats both-error as parity, so a spurious fold surfaces as an
/// Ok-vs-Err mismatch). `1 OR E` in a VALUE context is eager in stock too — the
/// TRUE-absorbs-OR short-circuit is truth-context-only and is NOT yet implemented
/// here (tracked separately); it must keep erroring, not silently fold.
#[test]
fn and_or_fold_does_not_over_fold_value_context() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup_mem().await;
        check(
            &f,
            &r,
            &[
                "SELECT 1 OR (SELECT count(*) FROM json_each('bare'))",
                "SELECT 1 AND (SELECT count(*) FROM json_each('bare'))",
                "SELECT 0.0 AND (SELECT count(*) FROM json_each('bare'))",
                "SELECT '0' AND (SELECT count(*) FROM json_each('bare'))",
            ],
            "and_or_fold_does_not_over_fold_value_context",
        )
        .await;
    });
}
