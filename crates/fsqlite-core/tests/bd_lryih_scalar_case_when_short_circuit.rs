#![recursion_limit = "512"]

//! bd-lryih (scalar residual) — FROM-less pure-scalar searched-CASE `WHEN`
//! conditions must short-circuit AND/OR left-to-right, exactly like stock
//! sqlite3 and like the FROM-bearing VDBE codegen path.
//!
//! A pure-scalar FROM-less `SELECT` (no subquery / WHERE / ORDER BY) routes
//! through `compile_expression_select` → `emit_case_expr` (connection.rs). Its
//! searched-CASE branch evaluated the whole WHEN condition eagerly, so
//! `CASE WHEN 1 OR <erroring> …` errored where stock short-circuits `1 OR …`
//! to TRUE and never evaluates the erroring operand.
//!
//! Companion to facet-1 (interpreter path `eval_expr_with_subqueries`,
//! 5a7b00e1b) and to the FROM-bearing codegen path
//! (`emit_searched_case_when_condition`) which already short-circuits. This
//! keeper isolates the FROM-less scalar expression-compiler organ.
//!
//! Oracle: stock rusqlite (3.46.x). Each probe must AGREE with stock — same
//! value on success, or both error. The guards prove we do NOT over-fold:
//! `0 OR E` and `1 AND E` must still evaluate `E` (and error) in both engines.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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
    let out = stmt
        .query_map([], |row| {
            let mut cols = Vec::with_capacity(n);
            for i in 0..n {
                let v: rusqlite::types::Value = row.get_unwrap(i);
                cols.push(match v {
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
            Ok(cols)
        })
        .map_err(|e| e.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;
    Ok(out)
}

#[test]
fn bd_lryih_scalar_case_when_short_circuit_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.expect("open fsqlite");
        let oracle = rusqlite::Connection::open_in_memory().expect("open rusqlite");

        // (sql, must_error) — must_error is the stock-verified expectation, and
        // both engines must agree with it.
        let probes: &[(&str, bool)] = &[
            // TRUE-absorbs-OR: `1 OR E` short-circuits → E never evaluated → THEN.
            (
                "SELECT CASE WHEN 1 OR json_extract('bare','$.a') THEN 1 ELSE 0 END;",
                false,
            ),
            // Guard: `0 OR E` must still evaluate E (no short-circuit) → error.
            (
                "SELECT CASE WHEN 0 OR json_extract('bare','$.a') THEN 1 ELSE 0 END;",
                true,
            ),
            // Guard: `1 AND E` must still evaluate E (AND does not skip on true left) → error.
            (
                "SELECT CASE WHEN 1 AND json_extract('bare','$.a') THEN 1 ELSE 0 END;",
                true,
            ),
            // Nested: left OR already true short-circuits the erroring right.
            (
                "SELECT CASE WHEN (1 OR json_extract('bare','$.a')) OR json_extract('nope','$.b') THEN 7 ELSE 0 END;",
                false,
            ),
            // Ordinary multi-WHEN CASE still selects the right arm.
            (
                "SELECT CASE WHEN 0 THEN 'a' WHEN 1 THEN 'b' ELSE 'c' END;",
                false,
            ),
            // ELSE fall-through unaffected.
            ("SELECT CASE WHEN 0 THEN 'x' ELSE 'y' END;", false),
        ];

        for (sql, must_error) in probes {
            let f = frank_rows(&frank, sql).await;
            let s = sqlite_rows(&oracle, sql);

            assert_eq!(
                f.is_err(),
                *must_error,
                "fsqlite error-state mismatch vs expectation for `{sql}`: got {f:?}"
            );
            assert_eq!(
                s.is_err(),
                *must_error,
                "stock oracle error-state mismatch vs expectation for `{sql}`: got {s:?} \
                 (if this fires, the expectation is wrong, not the engine)"
            );
            if !*must_error {
                assert_eq!(
                    f.as_ref().ok(),
                    s.as_ref().ok(),
                    "fsqlite vs stock value divergence for `{sql}`"
                );
            }
        }

        frank.close().await.expect("close fsqlite");
    });
}

const GROUPED_CASE_SETUP: &str = "
    CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE, n INTEGER, truth INTEGER);
    CREATE TABLE u(id INTEGER PRIMARY KEY);
    CREATE TABLE q(id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE, n INTEGER, bad TEXT);
    INSERT INTO t VALUES(1, 'Alpha', 1, 1), (2, 'beta', 2, 0), (3, 'Gamma', 3, NULL);
    INSERT INTO u VALUES(1), (2), (3);
    INSERT INTO q VALUES(1, 'Alpha', 1, 'bare'), (2, 'beta', 2, 'bare'), (3, 'Gamma', 3, 'bare');
";

#[test]
fn bd_k5isc_grouped_join_case_subqueries_are_lazy_and_preserve_metadata() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temporary database directory");
        let disk_path = dir.path().join("grouped-case.db");
        for path in [":memory:", disk_path.to_str().expect("UTF-8 database path")] {
            let frank = Connection::open(path).await.expect("open fsqlite");
            let oracle = rusqlite::Connection::open_in_memory().expect("open rusqlite");
            frank
                .execute_batch(GROUPED_CASE_SETUP)
                .await
                .expect("seed fsqlite");
            oracle
                .execute_batch(GROUPED_CASE_SETUP)
                .expect("seed oracle");

            // A correlated scalar subquery forces the grouped join's row
            // expression evaluator. Every row would fail if it were executed.
            const E: &str = "(SELECT json_extract(q.bad, '$.a') FROM q WHERE q.id = t.id)";
            let probes = [
                (format!("CASE WHEN t.id > 0 THEN t.n ELSE {E} END"), false),
                (format!("CASE WHEN t.id < 0 THEN {E} ELSE t.n END"), false),
                (
                    format!("CASE WHEN t.id > 0 THEN 1 WHEN {E} THEN 9 ELSE 0 END"),
                    false,
                ),
                (
                    format!("CASE WHEN t.id > 0 OR {E} THEN 1 ELSE 0 END"),
                    false,
                ),
                (
                    format!("CASE WHEN t.id < 0 AND {E} THEN 9 ELSE 1 END"),
                    false,
                ),
                (
                    format!(
                        "CASE WHEN t.id > 0 OR (CASE WHEN {E} THEN 1 ELSE 0 END) THEN 1 ELSE 0 END"
                    ),
                    false,
                ),
                (
                    format!(
                        "CASE WHEN t.id < 0 AND (CASE WHEN {E} THEN 1 ELSE 0 END) THEN 9 ELSE 1 END"
                    ),
                    false,
                ),
                (format!("CASE WHEN NULL AND {E} THEN 9 ELSE 1 END"), false),
                (
                    format!("CASE WHEN NOT (t.id < 0 AND {E}) THEN 1 ELSE 0 END"),
                    false,
                ),
                (format!("CASE WHEN t.id < 0 THEN {E} END"), false),
                (
                    format!(
                        "CASE WHEN t.id > 0 THEN CASE WHEN t.truth THEN 1 ELSE t.n END ELSE {E} END"
                    ),
                    false,
                ),
                // Required conditions and selected results must still error.
                (format!("CASE WHEN t.id < 0 OR {E} THEN 1 ELSE 0 END"), true),
                (
                    format!("CASE WHEN t.id > 0 AND {E} THEN 1 ELSE 0 END"),
                    true,
                ),
                (format!("CASE WHEN t.id > 0 THEN {E} ELSE 0 END"), true),
                (format!("CASE WHEN t.id < 0 THEN 0 ELSE {E} END"), true),
                // Simple CASE uses equality, including declared and scalar
                // subquery donor affinity/collation, rather than truthiness.
                (
                    format!("CASE t.n WHEN CAST(t.n AS TEXT) THEN 1 WHEN {E} THEN 9 ELSE 0 END"),
                    false,
                ),
                (
                    format!(
                        "CASE (SELECT q.n FROM q WHERE q.id = t.id) WHEN CAST(t.n AS TEXT) THEN 1 WHEN {E} THEN 9 ELSE 0 END"
                    ),
                    false,
                ),
                (
                    format!(
                        "CASE CAST(t.n AS TEXT) WHEN (SELECT q.n FROM q WHERE q.id = t.id) THEN 1 WHEN {E} THEN 9 ELSE 0 END"
                    ),
                    false,
                ),
                (
                    format!(
                        "CASE (SELECT q.n FROM q WHERE q.id = t.id) COLLATE BINARY WHEN printf('%d', t.n) THEN 1 WHEN {E} THEN 9 ELSE 0 END"
                    ),
                    false,
                ),
                (
                    format!("CASE t.a WHEN upper(t.a) THEN 1 WHEN {E} THEN 9 ELSE 0 END"),
                    false,
                ),
                // Scalar-subquery collation is opaque; only an outer explicit
                // COLLATE makes this comparison use NOCASE.
                (
                    format!(
                        "CASE (SELECT q.a FROM q WHERE q.id = t.id) WHEN upper(t.a) THEN 1 WHEN {E} THEN 9 ELSE 0 END"
                    ),
                    true,
                ),
                (
                    format!(
                        "CASE (SELECT q.a FROM q WHERE q.id = t.id) COLLATE NOCASE WHEN upper(t.a) THEN 1 WHEN {E} THEN 9 ELSE 0 END"
                    ),
                    false,
                ),
                (
                    format!(
                        "CASE t.n WHEN (SELECT NULL FROM q WHERE q.id = t.id) THEN {E} ELSE 1 END"
                    ),
                    false,
                ),
                (
                    "CASE NULL WHEN (SELECT q.n FROM q WHERE q.id = t.id) THEN 9 ELSE 1 END"
                        .to_owned(),
                    false,
                ),
                // A NULL base never matches, but SQLite still evaluates WHEN.
                (format!("CASE NULL WHEN {E} THEN 9 ELSE 1 END"), true),
                // A chosen column/CAST must not donate its affinity or
                // declared collation through the surrounding CASE result.
                (
                    format!(
                        "(CASE WHEN t.id > 0 THEN CAST(t.n AS INTEGER) ELSE {E} END) = printf('%d', t.n)"
                    ),
                    false,
                ),
                (
                    format!("(CASE WHEN t.id > 0 THEN t.a ELSE {E} END) = upper(t.a)"),
                    false,
                ),
                // Explicit collations retain their original leftmost
                // precedence, even when they occur in an unused branch.
                (
                    format!(
                        "(CASE WHEN t.id > 0 THEN t.a ELSE {E} COLLATE NOCASE END) = upper(t.a)"
                    ),
                    false,
                ),
                (
                    format!(
                        "(CASE WHEN t.id > 0 THEN t.a ELSE {E} COLLATE BINARY END) = upper(t.a) COLLATE NOCASE"
                    ),
                    false,
                ),
                (
                    format!(
                        "(CASE WHEN t.id > 0 THEN t.a COLLATE NOCASE ELSE {E} COLLATE BINARY END) = upper(t.a)"
                    ),
                    false,
                ),
            ];

            for (expression, must_error) in probes {
                let sql = format!(
                    "SELECT t.truth, sum({expression}) FROM t JOIN u USING(id) \
                     GROUP BY t.truth ORDER BY t.truth"
                );
                let expected = sqlite_rows(&oracle, &sql);
                let actual = frank_rows(&frank, &sql).await;
                assert_eq!(
                    expected.is_err(),
                    must_error,
                    "stock control: {sql}: {expected:?}"
                );
                assert_eq!(actual.is_err(), must_error, "{path}: {sql}: {actual:?}");
                if !must_error {
                    assert_eq!(actual, expected, "{path}: {sql}");
                }
            }

            let sql = "SELECT sum(CASE WHEN t.n > ?1 THEN t.n \
                       ELSE (SELECT q.n * 10 FROM q WHERE q.id = t.id) END) \
                       FROM t JOIN u USING(id)";
            for threshold in [0, 1, 3] {
                let expected = oracle
                    .query_row(sql, [threshold], |row| row.get::<_, i64>(0))
                    .expect("parameterized stock CASE");
                let actual = frank
                    .query_with_params(sql, &[SqliteValue::Integer(threshold)])
                    .await
                    .expect("parameterized grouped CASE");
                assert_eq!(actual.len(), 1, "{path}: threshold={threshold}");
                assert_eq!(
                    actual[0].values(),
                    &[SqliteValue::Integer(expected)],
                    "{path}: threshold={threshold}"
                );
            }

            // Deferred evaluation must not bypass prepare-time validation.
            for dead_branch in [
                "(SELECT missing FROM q WHERE q.id = t.id)",
                "(SELECT q.n FROM missing_table q WHERE q.id = t.id)",
                "(SELECT q.n, q.a FROM q WHERE q.id = t.id)",
                "(SELECT abs(q.n, q.n) FROM q WHERE q.id = t.id)",
                "(SELECT missing_function(q.n) FROM q WHERE q.id = t.id)",
            ] {
                let sql = format!(
                    "SELECT sum(CASE WHEN t.id > 0 THEN 1 ELSE {dead_branch} END) \
                     FROM t JOIN u USING(id) GROUP BY t.truth"
                );
                assert!(sqlite_rows(&oracle, &sql).is_err(), "stock control: {sql}");
                assert!(frank_rows(&frank, &sql).await.is_err(), "{path}: {sql}");
            }

            // FULL JOIN's unqualified USING column is the coalesced value,
            // including a right-only row, within both WHEN and the subquery.
            frank
                .execute("INSERT INTO u VALUES(4)")
                .await
                .expect("right-only row");
            oracle
                .execute("INSERT INTO u VALUES(4)", [])
                .expect("oracle right-only row");
            let sql = "SELECT sum(CASE WHEN id > 0 THEN (SELECT id) ELSE 0 END) \
                       FROM t FULL JOIN u USING(id)";
            assert_eq!(
                frank_rows(&frank, sql).await,
                sqlite_rows(&oracle, sql),
                "{path}: {sql}"
            );
            frank.close().await.expect("close fsqlite");
        }
    });
}

#[test]
fn bd_k5isc_case_with_group_aggregates_keeps_group_context() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temporary database directory");
        let disk_path = dir.path().join("group-case-context.db");
        for path in [":memory:", disk_path.to_str().expect("UTF-8 database path")] {
            let frank = Connection::open(path).await.expect("open fsqlite");
            let oracle = rusqlite::Connection::open_in_memory().expect("open rusqlite");
            frank.execute_batch(GROUPED_CASE_SETUP).await.unwrap();
            oracle.execute_batch(GROUPED_CASE_SETUP).unwrap();

            // These CASE expressions surround aggregates. Their conditions
            // must see the whole group, not the representative input row.
            // The correlated result is constant across possible donor rows.
            for sql in [
                "SELECT CASE WHEN sum(t.n)>4 \
                 THEN (SELECT 7 FROM q WHERE q.id=t.id) ELSE 0 END \
                 FROM t JOIN u USING(id)",
                "SELECT CASE sum(t.n) WHEN 6 \
                 THEN (SELECT 7 FROM q WHERE q.id=t.id) ELSE 0 END \
                 FROM t JOIN u USING(id)",
                "SELECT CASE 6 WHEN sum(t.n) \
                 THEN (SELECT 7 FROM q WHERE q.id=t.id) ELSE 0 END \
                 FROM t JOIN u USING(id)",
                "SELECT CASE WHEN sum(t.n)>4 \
                 THEN max(t.n)+(SELECT 7 FROM q WHERE q.id=t.id) ELSE 0 END \
                 FROM t JOIN u USING(id)",
                "SELECT CASE count(*) WHEN 0 THEN (SELECT coalesce(t.id,42)) ELSE 0 END \
                 FROM t JOIN u USING(id) WHERE t.id<0",
                "SELECT sum(t.n) FROM t JOIN u USING(id) \
                 HAVING CASE WHEN sum(t.n)>4 \
                 THEN EXISTS(SELECT 1 FROM q WHERE q.id=t.id) ELSE 0 END",
                "SELECT t.truth,sum(t.n) FROM t JOIN u USING(id) GROUP BY t.truth \
                 ORDER BY CASE sum(t.n) WHEN 3 \
                 THEN (SELECT q.n FROM q WHERE q.id=t.id) ELSE 10+sum(t.n) END",
            ] {
                let expected = sqlite_rows(&oracle, sql).expect("stock group CASE");
                let actual = frank_rows(&frank, sql)
                    .await
                    .unwrap_or_else(|error| panic!("{path}: {sql}: {error}"));
                assert_eq!(actual, expected, "{path}: {sql}");
            }
            frank.close().await.expect("close fsqlite");
        }
    });
}

#[test]
fn bd_k5isc_attached_dml_case_waits_for_target_row_binding() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temporary database directory");
        let disk_path = dir.path().join("attached-case-context.db");
        for path in [":memory:", disk_path.to_str().expect("UTF-8 database path")] {
            let frank = Connection::open(path).await.expect("open fsqlite");
            let oracle = rusqlite::Connection::open_in_memory().expect("open rusqlite");
            let setup = "CREATE TABLE source(n INTEGER); INSERT INTO source VALUES(10); \
                         ATTACH ':memory:' AS aux; \
                         CREATE TABLE aux.target(id INTEGER PRIMARY KEY,x INTEGER); \
                         INSERT INTO aux.target VALUES(-1,0),(1,0),(2,0),(3,0);";
            frank
                .execute_batch(setup)
                .await
                .expect("seed attached fsqlite");
            oracle.execute_batch(setup).expect("seed attached oracle");

            // Only the independent main-side subqueries can be evaluated
            // before delegation. CASE's id/x belong to each attached row.
            for sql in [
                "UPDATE aux.target SET x=CASE WHEN id>0 \
                 THEN (SELECT n FROM main.source) ELSE -1 END",
                "UPDATE aux.target SET x=CASE id WHEN 2 \
                 THEN (SELECT n+20 FROM main.source) ELSE x+1 END",
                "UPDATE aux.target SET x=x+100 WHERE CASE WHEN id>1 \
                 THEN id IN (SELECT n/5 FROM main.source) ELSE 0 END",
                "DELETE FROM aux.target WHERE CASE id WHEN 1 \
                 THEN EXISTS(SELECT 1 FROM main.source WHERE n=10) ELSE 0 END",
                "DELETE FROM aux.target WHERE CASE WHEN id<0 \
                 THEN (SELECT 1 FROM main.source) ELSE 0 END",
            ] {
                oracle.execute(sql, []).expect("stock attached CASE");
                frank
                    .execute(sql)
                    .await
                    .unwrap_or_else(|error| panic!("{path}: {sql}: {error}"));
                let query = "SELECT id,x FROM aux.target ORDER BY id";
                assert_eq!(
                    frank_rows(&frank, query).await.expect("read attached rows"),
                    sqlite_rows(&oracle, query).expect("read stock attached rows"),
                    "{path}: after {sql}"
                );
            }
            frank.close().await.expect("close fsqlite");
        }
    });
}

#[test]
fn bd_k5isc_fromless_simple_case_preserves_subquery_donor_metadata() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.expect("open fsqlite");
        let oracle = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        for sql in [
            "SELECT CASE (SELECT CAST(1 AS INTEGER)) WHEN '1' THEN 7 ELSE 0 END",
            "SELECT CASE (SELECT CAST(1 AS INTEGER)) COLLATE BINARY \
             WHEN '1' THEN 7 ELSE 0 END",
            // Nested CASE results mask the chosen operand's affinity. Both
            // unused erroring queries must remain unevaluated as well.
            "SELECT CASE (CASE WHEN 1 THEN (SELECT CAST(1 AS INTEGER)) \
             ELSE (SELECT json_extract('bare','$.a')) END) \
             WHEN (CASE WHEN 1 THEN '1' ELSE (SELECT json_extract('bare','$.a')) END) \
             THEN 99 WHEN (SELECT 1) THEN 7 ELSE 0 END",
        ] {
            let expected = sqlite_rows(&oracle, sql).expect("stock donor CASE");
            assert_eq!(expected, vec![vec!["7".to_owned()]], "stock control: {sql}");
            assert_eq!(
                frank_rows(&frank, sql)
                    .await
                    .unwrap_or_else(|error| panic!("{sql}: {error}")),
                expected,
                "{sql}"
            );
        }
        frank.close().await.expect("close fsqlite");
    });
}

struct CountingCaseIdentity {
    name: &'static str,
    calls: Arc<AtomicUsize>,
}

impl fsqlite_func::ScalarFunction for CountingCaseIdentity {
    fn name(&self) -> &str {
        self.name
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(args[0].clone())
    }
}

#[test]
fn bd_k5isc_case_evaluates_scalar_functions_once_and_skips_unused_cte_probes() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open fsqlite");
        conn.execute_batch(GROUPED_CASE_SETUP)
            .await
            .expect("seed fsqlite");
        let operand_calls = Arc::new(AtomicUsize::new(0));
        let when_calls = Arc::new(AtomicUsize::new(0));
        let result_calls = Arc::new(AtomicUsize::new(0));
        let probe_calls = Arc::new(AtomicUsize::new(0));
        for (name, calls) in [
            ("case_operand", &operand_calls),
            ("case_when", &when_calls),
            ("case_result", &result_calls),
            ("case_probe", &probe_calls),
        ] {
            conn.register_nondeterministic_scalar_function(CountingCaseIdentity {
                name,
                calls: Arc::clone(calls),
            });
        }

        let sql = "SELECT sum(CASE case_operand(t.n) \
                   WHEN case_when(0) THEN case_result(99) \
                   WHEN case_when((SELECT q.n FROM q WHERE q.id = t.id)) THEN case_result(t.n) \
                   ELSE case_result(99) END) FROM t JOIN u USING(id)";
        assert_eq!(
            frank_rows(&conn, sql).await.expect("counted CASE"),
            vec![vec!["6".to_owned()]]
        );
        assert_eq!(
            operand_calls.load(Ordering::Relaxed),
            3,
            "one base evaluation per row"
        );
        assert_eq!(
            when_calls.load(Ordering::Relaxed),
            6,
            "two reached WHEN values per row"
        );
        assert_eq!(
            result_calls.load(Ordering::Relaxed),
            3,
            "one chosen result per row"
        );

        // The production shape in bd-k5isc: a grouped join whose later CASE
        // arm probes a CTE correlated to the current joined row.
        for (condition, probes_expected) in [("t.id > 0", false), ("t.id < 0", true)] {
            probe_calls.store(0, Ordering::Relaxed);
            let sql = format!(
                "WITH links AS (SELECT id FROM q UNION SELECT id FROM u) \
                 SELECT sum(CASE WHEN {condition} THEN 1 \
                 WHEN EXISTS(SELECT 1 FROM links WHERE links.id = t.id AND case_probe(links.id) > 0) \
                 THEN 1 ELSE 0 END) FROM t JOIN u USING(id) GROUP BY t.truth ORDER BY t.truth"
            );
            assert_eq!(
                frank_rows(&conn, &sql).await.expect("correlated CTE CASE"),
                vec![
                    vec!["1".to_owned()],
                    vec!["1".to_owned()],
                    vec!["1".to_owned()]
                ]
            );
            assert_eq!(
                probe_calls.load(Ordering::Relaxed) > 0,
                probes_expected,
                "{sql}"
            );
        }
        conn.close().await.expect("close fsqlite");
    });
}
