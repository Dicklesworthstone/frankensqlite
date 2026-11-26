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

const CONTACT_STATS_SCHEMA: &str = "
    CREATE TABLE agents (id INTEGER PRIMARY KEY, contact_policy TEXT);
    CREATE TABLE messages (id INTEGER PRIMARY KEY, sender_id INTEGER, created_ts INTEGER NOT NULL);
    CREATE TABLE message_recipients (message_id INTEGER NOT NULL, agent_id INTEGER NOT NULL);
    CREATE TABLE agent_links (
        id INTEGER PRIMARY KEY,
        a_agent_id INTEGER,
        b_agent_id INTEGER,
        status TEXT,
        expires_ts INTEGER
    );
";

const CONTACT_STATS_NOW: i64 = 1_760_000_000_000_000;
const CONTACT_STATS_WINDOW_START: i64 = CONTACT_STATS_NOW - 86_400_000_000;
const CONTACT_STATS_INSIDE: i64 = CONTACT_STATS_NOW - 3_600_000_000;
const CONTACT_STATS_OUTSIDE: i64 = CONTACT_STATS_NOW - 2 * 86_400_000_000;

fn contact_stats_seed_sql(extra_links: usize) -> String {
    use std::fmt::Write as _;
    let mut sql = String::from(CONTACT_STATS_SCHEMA);
    sql.push_str(
        "INSERT INTO agents (id, contact_policy) VALUES \
         (1, 'auto'), (2, 'open'), (3, 'contacts_only'), (4, 'block_all'), \
         (5, 'contacts_only'), (6, 'contacts_only'), (7, 'contacts_only'), (8, NULL), \
         (10, 'auto'), (11, 'auto'), (12, 'auto');",
    );
    let _ = write!(
        sql,
        "INSERT INTO agent_links (id, a_agent_id, b_agent_id, status, expires_ts) VALUES \
         (1, 10, 3, 'approved', 0), \
         (2, 5, 11, 'accepted', NULL), \
         (3, 10, 6, 'pending', 0), \
         (4, 11, 6, 'approved', {}), \
         (5, 12, 6, 'approved', {}), \
         (6, 10, 7, 'rejected', 0);",
        CONTACT_STATS_NOW - 1_000,
        CONTACT_STATS_NOW + 1_000_000_000,
    );
    for idx in 0..extra_links {
        let id = 100 + idx;
        let a = 1_000 + idx;
        let b = 2_000 + idx;
        let _ = write!(
            sql,
            "INSERT INTO agent_links (id, a_agent_id, b_agent_id, status, expires_ts) \
             VALUES ({id}, {a}, {b}, 'approved', 0);"
        );
    }
    let _ = write!(
        sql,
        "INSERT INTO messages (id, sender_id, created_ts) VALUES \
         (1, 10, {inside}), (2, 11, {inside}), (3, 12, {inside}), \
         (4, 3, {inside}), (5, 4, {inside}), (6, 10, {outside}), (7, 11, {inside});\
         INSERT INTO message_recipients (message_id, agent_id) VALUES \
         (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), \
         (2, 3), (2, 5), (2, 6), \
         (3, 6), (3, 4), \
         (4, 3), (5, 4), (6, 3), (7, 1);",
        inside = CONTACT_STATS_INSIDE,
        outside = CONTACT_STATS_OUTSIDE,
    );
    sql
}

fn legacy_contact_stats_sql(cte_tag_expr: Option<&str>) -> String {
    let link_expires_expr = "COALESCE(al.expires_ts, 0)";
    let message_created_expr = "COALESCE(m.created_ts, 0)";
    let extra_select = cte_tag_expr
        .map(|expr| format!(", {expr} AS tag"))
        .unwrap_or_default();
    format!(
        "WITH approved_links AS ( \
            SELECT a_agent_id AS sender_id, b_agent_id AS recipient_id{extra_select} \
            FROM agent_links al \
            WHERE status IN ('approved', 'accepted') \
              AND ({link_expires_expr} <= 0 OR {link_expires_expr} > {now}) \
            UNION \
            SELECT b_agent_id AS sender_id, a_agent_id AS recipient_id{extra_select} \
            FROM agent_links al \
            WHERE status IN ('approved', 'accepted') \
              AND ({link_expires_expr} <= 0 OR {link_expires_expr} > {now}) \
         ) \
         SELECT \
            mr.agent_id AS agent_id, \
            SUM(CASE \
                    WHEN recipient.contact_policy IN ('open', 'auto') THEN 1 \
                    WHEN m.sender_id = mr.agent_id THEN 1 \
                    WHEN recipient.contact_policy = 'contacts_only' AND EXISTS ( \
                        SELECT 1 FROM approved_links link \
                        WHERE link.sender_id = m.sender_id \
                          AND link.recipient_id = mr.agent_id \
                    ) THEN 1 \
                    ELSE 0 END) AS respected_count, \
            SUM(CASE \
                    WHEN recipient.contact_policy = 'contacts_only' \
                     AND m.sender_id != mr.agent_id \
                     AND NOT EXISTS ( \
                        SELECT 1 FROM approved_links link \
                        WHERE link.sender_id = m.sender_id \
                          AND link.recipient_id = mr.agent_id \
                    ) THEN 1 \
                    WHEN recipient.contact_policy = 'block_all' \
                     AND m.sender_id != mr.agent_id \
                    THEN 1 \
                    ELSE 0 END) AS violation_count \
         FROM message_recipients mr \
         JOIN messages m ON m.id = mr.message_id \
         JOIN agents recipient ON recipient.id = mr.agent_id \
         WHERE mr.agent_id IN (1, 2, 3, 4, 5, 6, 7, 8) \
           AND {message_created_expr} >= {window_start} \
         GROUP BY mr.agent_id \
         ORDER BY mr.agent_id",
        now = CONTACT_STATS_NOW,
        window_start = CONTACT_STATS_WINDOW_START,
    )
}

#[test]
fn bd_k5isc_legacy_contact_stats_exact_oracle_and_cte_single_eval() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temporary directory");
        let disk_path = dir.path().join("contact-stats.sqlite3");
        let disk_path_str = disk_path.to_str().expect("UTF-8 path").to_owned();
        let seed_sql = contact_stats_seed_sql(80);
        let query_sql = legacy_contact_stats_sql(None);
        let counted_sql = legacy_contact_stats_sql(Some("cte_eval_counter(al.id)"));

        let oracle = rusqlite::Connection::open_in_memory().expect("open oracle");
        oracle.execute_batch(&seed_sql).expect("seed oracle");
        let expected = sqlite_rows(&oracle, &query_sql).expect("oracle legacy contact stats");
        assert_eq!(
            expected,
            vec![
                vec!["1", "2", "0"],
                vec!["2", "1", "0"],
                vec!["3", "2", "1"],
                vec!["4", "1", "2"],
                vec!["5", "1", "1"],
                vec!["6", "1", "2"],
                vec!["7", "0", "1"],
                vec!["8", "0", "0"],
            ]
            .into_iter()
            .map(|r| r.into_iter().map(str::to_owned).collect::<Vec<_>>())
            .collect::<Vec<_>>()
        );

        for (label, path, reopen) in [
            ("memory", ":memory:", false),
            ("file-fresh-write", disk_path_str.as_str(), false),
            ("file-reopened", disk_path_str.as_str(), true),
        ] {
            let conn = Connection::open(path).await.expect("open fsqlite");
            if !reopen {
                if path != ":memory:" {
                    conn.execute("PRAGMA journal_mode=WAL")
                        .await
                        .expect("enable WAL");
                }
                conn.execute_batch(&seed_sql).await.expect("seed fsqlite");
            }
            let direct = frank_rows(&conn, &query_sql)
                .await
                .unwrap_or_else(|err| panic!("{label} direct failed: {err}"));
            assert_eq!(direct, expected, "{label} direct mismatch vs stock 3.54.1");

            let prepared = conn.prepare(&query_sql).await.expect("prepare query");
            let prepared_rows: Vec<Vec<String>> = prepared
                .query()
                .await
                .unwrap_or_else(|err| panic!("{label} prepared failed: {err}"))
                .iter()
                .map(|row| row.values().iter().map(render_frank).collect())
                .collect();
            assert_eq!(
                prepared_rows, expected,
                "{label} prepared mismatch vs stock 3.54.1"
            );

            // Prove the UNION CTE body executes ONCE per statement (2 arms x 83 qualifying
            // rows = 166 UDF calls), never re-evaluated per correlated probe row.
            let cte_calls = Arc::new(AtomicUsize::new(0));
            conn.register_nondeterministic_scalar_function(CountingCaseIdentity {
                name: "cte_eval_counter",
                calls: Arc::clone(&cte_calls),
            });
            let counted = frank_rows(&conn, &counted_sql)
                .await
                .unwrap_or_else(|err| panic!("{label} counted failed: {err}"));
            assert_eq!(counted, expected, "{label} counted mismatch vs stock");
            assert_eq!(
                cte_calls.load(Ordering::Relaxed),
                166,
                "{label}: CTE body must evaluate once per statement (2 UNION arms x 83 matching rows)"
            );
            conn.close().await.expect("close fsqlite");
        }
    });
}

#[test]
fn bd_k5isc_multi_column_exists_affinity_collation_and_shadowing_oracle() {
    asupersync::test_utils::run_test(|| async {
        let setup = "
            CREATE TABLE outer_rows (
                id INTEGER PRIMARY KEY,
                grp INTEGER,
                policy TEXT,
                k_int INTEGER,
                k_txt TEXT,
                k_nc TEXT COLLATE NOCASE,
                k_rt TEXT COLLATE RTRIM,
                k_raw
            );
            CREATE TABLE join_dim (
                id INTEGER PRIMARY KEY,
                tag TEXT
            );
            CREATE TABLE pair_links (
                id INTEGER PRIMARY KEY,
                p_int INTEGER,
                p_txt TEXT,
                p_nc TEXT COLLATE NOCASE,
                p_rt TEXT COLLATE RTRIM,
                p_raw
            );
            INSERT INTO join_dim VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h');
            INSERT INTO outer_rows VALUES
                (1, 1, 'contacts_only', 1, 'Alpha', 'Alpha', 'pad  ', 1),
                (2, 1, 'contacts_only', 2, '2',     'BETA',  'pad',   '2'),
                (3, 1, 'contacts_only', 9007199254740993, 'hi', 'Hi', 'x', 9007199254740993),
                (4, 2, 'contacts_only', NULL, 'Alpha', 'Alpha', 'pad', NULL),
                (5, 2, 'contacts_only', 1, NULL, 'Alpha', 'pad', 1),
                (6, 2, 'open',          999, 'missing', 'missing', 'missing', 999),
                (7, 3, 'contacts_only', 0, 'zero', 'ZERO', 'z   ', -0.0),
                (8, 3, 'contacts_only', 5, '5', 'FIVE', 'f', 5);
            INSERT INTO pair_links VALUES
                (1, 1, 'Alpha', 'alpha', 'pad', 1),
                (2, 2, '2',     'beta',  'pad   ', 2),
                (3, 9007199254740992, 'hi', 'hi', 'x', 9007199254740992.0),
                (4, 0, 'zero',  'zero',  'z', 0.0),
                (5, 5, '5',     'five',  'f', '5'),
                (6, 1, NULL,    'alpha', 'pad', 1),
                (7, NULL, 'Alpha', 'alpha', 'pad', NULL);
        ";
        // Pad pair_links past EXISTS_DIRECT_PROBE_MAX_LINEAR_ROWS (64) to exercise
        // memo hash-set promotion in both GROUP BY joins and non-grouped joins.
        let mut full_setup = String::from(setup);
        for i in 10..95 {
            use std::fmt::Write as _;
            let _ = write!(
                full_setup,
                "INSERT INTO pair_links VALUES ({i}, {k}, 'pad_{i}', 'pad_{i}', 'pad_{i}', {k});",
                k = 1000 + i
            );
        }

        let queries = [
            // 2-col integer+text over UNION CTE inside GROUP BY CASE (EXISTS + NOT EXISTS).
            "WITH links AS (
                SELECT p_int AS s, p_txt AS r FROM pair_links
                UNION
                SELECT p_int + 10000 AS s, p_txt AS r FROM pair_links
             )
             SELECT o.grp,
                    SUM(CASE WHEN o.policy = 'open' THEN 1
                             WHEN EXISTS (SELECT 1 FROM links l WHERE l.s = o.k_int AND l.r = o.k_txt) THEN 1
                             ELSE 0 END),
                    SUM(CASE WHEN o.policy = 'contacts_only'
                              AND NOT EXISTS (SELECT 1 FROM links l WHERE o.k_int = l.s AND l.r = o.k_txt) THEN 1
                             ELSE 0 END)
             FROM outer_rows o JOIN join_dim d ON d.id = o.id
             GROUP BY o.grp ORDER BY o.grp",
            // 2-col with NOCASE + RTRIM collations and reversed operand order.
            "SELECT o.grp,
                    SUM(CASE WHEN EXISTS (
                        SELECT 1 FROM pair_links l
                        WHERE l.p_nc = o.k_nc AND o.k_rt = l.p_rt
                    ) THEN 1 ELSE 0 END)
             FROM outer_rows o JOIN join_dim d ON d.id = o.id
             GROUP BY o.grp ORDER BY o.grp",
            // GH#428 typeless (BLOB affinity) vs TEXT column must NOT coerce int 2 to '2'.
            "SELECT o.id,
                    EXISTS (
                        SELECT 1 FROM pair_links l
                        WHERE l.p_raw = o.k_txt AND l.p_int = o.k_int
                    ),
                    EXISTS (
                        SELECT 1 FROM pair_links l
                        WHERE l.p_raw = o.k_raw AND l.p_int = o.k_int
                    )
             FROM outer_rows o JOIN join_dim d ON d.id = o.id
             ORDER BY o.id",
            // Duplicate inner column in conjunctive AND: l.p_int = o.k_int AND l.p_int = o.grp.
            "SELECT o.grp,
                    SUM(CASE WHEN EXISTS (
                        SELECT 1 FROM pair_links l
                        WHERE l.p_int = o.k_int AND l.p_int = o.grp
                    ) THEN 1 ELSE 0 END)
             FROM outer_rows o JOIN join_dim d ON d.id = o.id
             GROUP BY o.grp ORDER BY o.grp",
            // Precision-preserving int64 vs f64 near 2^53 and -0.0 vs +0.0.
            "SELECT o.id,
                    EXISTS (
                        SELECT 1 FROM pair_links l
                        WHERE l.p_raw = o.k_raw AND l.p_txt = o.k_txt
                    )
             FROM outer_rows o JOIN join_dim d ON d.id = o.id
             WHERE o.id IN (3, 7, 8)
             ORDER BY o.id",
            // CTE shadowing persistent base table name `pair_links`.
            "WITH pair_links AS (
                SELECT 1 AS p_int, 'Alpha' AS p_txt
                UNION
                SELECT 5 AS p_int, '5' AS p_txt
             )
             SELECT o.grp,
                    SUM(CASE WHEN EXISTS (
                        SELECT 1 FROM pair_links l
                        WHERE l.p_int = o.k_int AND l.p_txt = o.k_txt
                    ) THEN 1 ELSE 0 END)
             FROM outer_rows o JOIN join_dim d ON d.id = o.id
             GROUP BY o.grp ORDER BY o.grp",
            // Non-grouped JOIN WHERE with 2-col correlated EXISTS over CTE.
            "WITH links AS (SELECT p_int AS s, p_txt AS r FROM pair_links)
             SELECT o.id FROM outer_rows o JOIN join_dim d ON d.id = o.id
             WHERE EXISTS (SELECT 1 FROM links l WHERE l.s = o.k_int AND l.r = o.k_txt)
             ORDER BY o.id",
        ];

        let dir = tempfile::tempdir().expect("tempdir");
        let disk_path = dir.path().join("multi-col-exists.sqlite3");
        let disk_path_str = disk_path.to_str().expect("UTF-8 path").to_owned();

        let oracle = rusqlite::Connection::open_in_memory().expect("open oracle");
        oracle.execute_batch(&full_setup).expect("seed oracle");

        for (label, path, reopen) in [
            ("memory", ":memory:", false),
            ("file-fresh", disk_path_str.as_str(), false),
            ("file-reopened", disk_path_str.as_str(), true),
        ] {
            let frank = Connection::open(path).await.expect("open fsqlite");
            if !reopen {
                if path != ":memory:" {
                    frank.execute("PRAGMA journal_mode=WAL").await.unwrap();
                }
                frank.execute_batch(&full_setup).await.expect("seed fsqlite");
            }
            for sql in queries {
                let expected = sqlite_rows(&oracle, sql).expect("oracle query");
                let actual = frank_rows(&frank, sql)
                    .await
                    .unwrap_or_else(|err| panic!("{label}: {sql}: {err}"));
                assert_eq!(actual, expected, "{label} direct mismatch for:\n{sql}");

                let stmt = frank.prepare(sql).await.expect("prepare");
                let prep_rows: Vec<Vec<String>> = stmt
                    .query()
                    .await
                    .unwrap_or_else(|err| panic!("{label} prepared: {sql}: {err}"))
                    .iter()
                    .map(|row| row.values().iter().map(render_frank).collect())
                    .collect();
                assert_eq!(prep_rows, expected, "{label} prepared mismatch for:\n{sql}");
            }
            frank.close().await.expect("close fsqlite");
        }
    });
}

#[test]
#[ignore = "manual paired benchmark for bd-k5isc vs stock rusqlite 3.54.1; run with -- --ignored --nocapture"]
fn bd_k5isc_contact_stats_benchmark_vs_stock_sqlite() {
    use std::time::Instant;

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let frank_path = dir.path().join("frank-bench.sqlite3");
        let stock_path = dir.path().join("stock-bench.sqlite3");
        let frank_path_str = frank_path.to_str().expect("UTF-8").to_owned();

        let seed_sql = contact_stats_seed_sql(200);
        let index_sql = "
            CREATE INDEX idx_message_recipients_agent ON message_recipients(agent_id);
            CREATE INDEX idx_message_recipients_agent_message ON message_recipients(agent_id, message_id);
            CREATE INDEX idx_messages_created_ts ON messages(created_ts);
        ";

        // Seed FrankenSQLite file-backed WAL DB (9 doublings = 8,192 in-window grouped deliveries,
        // 4,608 of which hit contacts_only EXISTS + NOT EXISTS over a 406-row UNION CTE).
        {
            let frank = Connection::open(&frank_path_str).await.expect("open frank");
            frank.execute("PRAGMA journal_mode=WAL").await.unwrap();
            frank.execute_batch(&seed_sql).await.unwrap();
            frank.execute_batch(index_sql).await.unwrap();
            frank.execute("BEGIN").await.unwrap();
            for generation in 0..9 {
                let offset = 7_i64 << generation;
                frank
                    .execute(&format!(
                        "INSERT INTO messages (id, sender_id, created_ts) \
                         SELECT id + {offset}, sender_id, created_ts FROM messages WHERE id <= {offset}"
                    ))
                    .await
                    .unwrap();
                frank
                    .execute(&format!(
                        "INSERT INTO message_recipients (message_id, agent_id) \
                         SELECT message_id + {offset}, agent_id FROM message_recipients WHERE message_id <= {offset}"
                    ))
                    .await
                    .unwrap();
            }
            frank.execute("COMMIT").await.unwrap();
            frank.close().await.unwrap();
        }

        // Seed stock rusqlite 3.54.1 identically on disk.
        let oracle = rusqlite::Connection::open(&stock_path).expect("open stock");
        oracle.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        oracle.execute_batch(&seed_sql).unwrap();
        oracle.execute_batch(index_sql).unwrap();
        oracle.execute_batch("BEGIN;").unwrap();
        for generation in 0..9 {
            let offset = 7_i64 << generation;
            oracle
                .execute(
                    "INSERT INTO messages (id, sender_id, created_ts) \
                     SELECT id + ?1, sender_id, created_ts FROM messages WHERE id <= ?1",
                    [offset],
                )
                .unwrap();
            oracle
                .execute(
                    "INSERT INTO message_recipients (message_id, agent_id) \
                     SELECT message_id + ?1, agent_id FROM message_recipients WHERE message_id <= ?1",
                    [offset],
                )
                .unwrap();
        }
        oracle.execute_batch("COMMIT;").unwrap();

        let query_sql = legacy_contact_stats_sql(None);
        let expected = sqlite_rows(&oracle, &query_sql).expect("stock query");

        // Reopen FrankenSQLite fresh (cold memdb_rows_loaded, real production shape!).
        let frank = Connection::open(&frank_path_str).await.expect("reopen frank");
        let actual = frank_rows(&frank, &query_sql).await.expect("frank query");
        assert_eq!(actual, expected, "benchmark query output mismatch vs stock");

        // Warmup 1 each, then 5 interleaved samples.
        let _ = sqlite_rows(&oracle, &query_sql).unwrap();
        let _ = frank_rows(&frank, &query_sql).await.unwrap();

        let mut stock_ms = Vec::with_capacity(5);
        let mut frank_ms = Vec::with_capacity(5);
        for i in 0..5 {
            if i % 2 == 0 {
                let t0 = Instant::now();
                let r = frank_rows(&frank, &query_sql).await.unwrap();
                frank_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(r, expected);

                let t1 = Instant::now();
                let s = sqlite_rows(&oracle, &query_sql).unwrap();
                stock_ms.push(t1.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(s, expected);
            } else {
                let t1 = Instant::now();
                let s = sqlite_rows(&oracle, &query_sql).unwrap();
                stock_ms.push(t1.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(s, expected);

                let t0 = Instant::now();
                let r = frank_rows(&frank, &query_sql).await.unwrap();
                frank_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(r, expected);
            }
        }
        stock_ms.sort_by(f64::total_cmp);
        frank_ms.sort_by(f64::total_cmp);
        eprintln!(
            "bd-k5isc paired bench (8,192 grouped rows, 4,608 contacts_only EXISTS+NOT EXISTS, 406 CTE rows, reopened file-backed WAL): \
             stock_3.54.1 median={:.2}ms (all={stock_ms:.2?}) | frankensqlite median={:.2}ms (all={frank_ms:.2?}) | ratio={:.2}x",
            stock_ms[2],
            frank_ms[2],
            frank_ms[2] / stock_ms[2].max(1e-6),
        );
        frank.close().await.unwrap();
    });
}
