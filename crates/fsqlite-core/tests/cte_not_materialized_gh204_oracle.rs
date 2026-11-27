#![recursion_limit = "512"]

//! GH #204 (bd-gh-cte-materialization-hints-sn0yh): a `NOT MATERIALIZED`
//! non-recursive CTE must be inlined at each reference so its body re-evaluates
//! per use (matching SQLite), instead of being materialized once into a transient
//! table. Frank previously materialized every CTE regardless of the hint, so two
//! references to a `NOT MATERIALIZED (SELECT random())` CTE observed the same
//! value (bug: result `1`) instead of two independent draws (correct: `0`).
//!
//! rusqlite (stock SQLite 3.46.x) is the oracle. Because a *deterministic* body
//! makes materialize-once and inline observationally identical (the hint is a
//! pure planning knob for deterministic queries), the distinguishing probes use
//! `random()`; the remaining probes assert the inlining transform preserves
//! results across aliasing, joins, and IN/EXISTS subqueries.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
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

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank error on `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Assert frank and rusqlite agree on `sql` (order-insensitive).
async fn assert_agree(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let mut fr = frank_rows(f, sql).await;
    let mut rr = sqlite_rows(r, sql);
    fr.sort();
    rr.sort();
    assert_eq!(fr, rr, "oracle mismatch on `{sql}`");
}

fn setup_r() -> rusqlite::Connection {
    let r = rusqlite::Connection::open_in_memory().unwrap();
    r.execute_batch("CREATE TABLE t(a INTEGER); INSERT INTO t VALUES (1),(2),(3);")
        .unwrap();
    r
}

async fn setup_f() -> Connection {
    let f = Connection::open(":memory:").await.unwrap();
    f.execute("CREATE TABLE t(a INTEGER)").await.unwrap();
    f.execute("INSERT INTO t VALUES (1),(2),(3)").await.unwrap();
    f
}

/// The exact GH#204 repro: two references to a `NOT MATERIALIZED (SELECT
/// random())` CTE must re-evaluate independently, so `=` is 0 and `<>` is 1.
/// Runs several times: frank and rusqlite use independent RNG streams, so the
/// ~1-in-2^64 self-tie in either engine is vanishingly unlikely across the loop.
#[test]
fn not_materialized_random_reevaluates_gh204() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for _ in 0..8 {
            // Equality: independent draws => 0 (the bug produced 1).
            assert_agree(
                &f,
                &r,
                "WITH c AS NOT MATERIALIZED (SELECT random() AS r) \
                 SELECT (SELECT r FROM c)=(SELECT r FROM c)",
            )
            .await;
            // Inequality: independent draws => 1.
            assert_agree(
                &f,
                &r,
                "WITH c AS NOT MATERIALIZED (SELECT random() AS r) \
                 SELECT (SELECT r FROM c)<>(SELECT r FROM c)",
            )
            .await;
        }
        // Also pin the semantic expectation directly (not just oracle parity).
        for _ in 0..8 {
            let eq = frank_rows(
                &f,
                "WITH c AS NOT MATERIALIZED (SELECT random() AS r) \
                 SELECT (SELECT r FROM c)=(SELECT r FROM c)",
            )
            .await;
            assert_eq!(
                eq,
                vec![vec!["0".to_owned()]],
                "NOT MATERIALIZED `=` must be 0"
            );
        }
    });
}

/// MATERIALIZED control: computed once, so both references observe the same
/// value => `=` is 1. Must be unchanged by the GH#204 fix.
#[test]
fn materialized_random_computed_once_gh204() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for _ in 0..8 {
            assert_agree(
                &f,
                &r,
                "WITH c AS MATERIALIZED (SELECT random() AS r) \
                 SELECT (SELECT r FROM c)=(SELECT r FROM c)",
            )
            .await;
        }
        let eq = frank_rows(
            &f,
            "WITH c AS MATERIALIZED (SELECT random() AS r) \
             SELECT (SELECT r FROM c)=(SELECT r FROM c)",
        )
        .await;
        assert_eq!(eq, vec![vec!["1".to_owned()]], "MATERIALIZED `=` must be 1");
    });
}

/// Default (no hint) control: a CTE referenced more than once is materialized by
/// SQLite's default heuristic, so `=` is 1. The fix does not touch the default
/// path; assert frank matches the oracle either way.
#[test]
fn default_hint_random_control_gh204() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for _ in 0..8 {
            assert_agree(
                &f,
                &r,
                "WITH c AS (SELECT random() AS r) \
                 SELECT (SELECT r FROM c)=(SELECT r FROM c)",
            )
            .await;
        }
    });
}

/// Column aliasing: `c(x)` renames the body's single output column; the inlined
/// derived table must carry that alias so `SELECT x` resolves.
#[test]
fn not_materialized_column_alias_gh204() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        assert_agree(
            &f,
            &r,
            "WITH c(x) AS NOT MATERIALIZED (SELECT 5) SELECT x FROM c",
        )
        .await;
        // Multi-column aliasing.
        assert_agree(
            &f,
            &r,
            "WITH c(x, y) AS NOT MATERIALIZED (SELECT 5, 6) SELECT y, x FROM c",
        )
        .await;
        // The pinned value.
        let rows = frank_rows(
            &f,
            "WITH c(x) AS NOT MATERIALIZED (SELECT 5) SELECT x FROM c",
        )
        .await;
        assert_eq!(rows, vec![vec!["5".to_owned()]]);
    });
}

/// A `NOT MATERIALIZED` CTE referenced from a JOIN (once, and twice as a
/// self-join) inlines each FROM reference to a derived table; results must match
/// the oracle.
#[test]
fn not_materialized_in_join_gh204() {
    asupersync::test_utils::run_test(|| async {
        let f = setup_f().await;
        let r = setup_r();
        assert_agree(
            &f,
            &r,
            "WITH c AS NOT MATERIALIZED (SELECT a FROM t WHERE a >= 2) \
             SELECT t.a, c.a FROM t JOIN c ON t.a = c.a ORDER BY t.a",
        )
        .await;
        // Self-join: the CTE is inlined at two FROM positions.
        assert_agree(
            &f,
            &r,
            "WITH c AS NOT MATERIALIZED (SELECT a FROM t WHERE a <= 2) \
             SELECT x.a, y.a FROM c x JOIN c y ON x.a <= y.a ORDER BY x.a, y.a",
        )
        .await;
    });
}

/// A `NOT MATERIALIZED` CTE referenced inside a `WHERE ... IN (SELECT ... FROM c)`
/// subquery and inside `EXISTS` must have its inner `FROM c` inlined.
#[test]
fn not_materialized_in_where_subquery_gh204() {
    asupersync::test_utils::run_test(|| async {
        let f = setup_f().await;
        let r = setup_r();
        assert_agree(
            &f,
            &r,
            "WITH c AS NOT MATERIALIZED (SELECT a FROM t WHERE a >= 2) \
             SELECT a FROM t WHERE a IN (SELECT a FROM c) ORDER BY a",
        )
        .await;
        assert_agree(
            &f,
            &r,
            "WITH c AS NOT MATERIALIZED (SELECT a FROM t WHERE a >= 2) \
             SELECT a FROM t WHERE EXISTS (SELECT 1 FROM c WHERE c.a = t.a) ORDER BY a",
        )
        .await;
    });
}

/// A `NOT MATERIALIZED` CTE that references another (materialized) CTE: the
/// inlined body still reads the materialized sibling. Deterministic, so it is an
/// oracle-parity correctness check.
#[test]
fn not_materialized_references_other_cte_gh204() {
    asupersync::test_utils::run_test(|| async {
        let f = setup_f().await;
        let r = setup_r();
        assert_agree(
            &f,
            &r,
            "WITH base AS MATERIALIZED (SELECT a FROM t WHERE a >= 2), \
                  scaled AS NOT MATERIALIZED (SELECT a * 10 AS m FROM base) \
             SELECT m FROM scaled ORDER BY m",
        )
        .await;
    });
}

/// bd-wpiq6 M4: a CHAIN of NOT MATERIALIZED CTEs (`b`'s body reads `a`) must
/// inline transitively, so `a`'s non-deterministic body re-evaluates at each of
/// `b`'s reference sites. Frank previously inlined `b` carrying a bare `FROM a`,
/// leaving `a` a single-evaluation shared CTE, so the two `(SELECT r FROM b)`
/// observed one `random()` draw (`=` returned 1 where stock re-evaluates to 0).
#[test]
fn not_materialized_chained_random_reevaluates_wpiq6() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        const SQL: &str = "WITH a AS NOT MATERIALIZED (SELECT random() AS r), \
                                b AS NOT MATERIALIZED (SELECT r FROM a) \
                           SELECT (SELECT r FROM b)=(SELECT r FROM b)";
        for _ in 0..8 {
            assert_agree(&f, &r, SQL).await;
            let eq = frank_rows(&f, SQL).await;
            assert_eq!(
                eq,
                vec![vec!["0".to_owned()]],
                "chained NOT MATERIALIZED `=` must be 0 (independent draws)"
            );
        }
    });
}

/// bd-wpiq6 M8: a row-value comparison where one operand is `SELECT *` over an
/// outer CTE must not falsely raise the scalar-arity error. The width check
/// computed the `SELECT *` operand's arity with an EMPTY CTE scope
/// (under-counting it to 1), so its explicit multi-column peer was
/// mis-classified as a scalar subquery and rejected ("sub-select returns 2
/// columns - expected 1"). The prepare resolver now threads the visible CTE
/// scope and the codegen pre-pass fails open on wildcard operands.
///
/// This asserts the specific width verdict is gone. (Frank cannot yet EXECUTE a
/// row-value comparison whose operands are subqueries — a separate interpreted
/// evaluator gap tracked elsewhere — so this checks the arity error is absent
/// rather than full oracle parity.)
#[test]
fn scalar_subquery_width_sees_outer_cte_wpiq6() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        for sql in [
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT * FROM c) = (SELECT a, b FROM c)",
            "WITH c(a,b) AS (SELECT 1,2) SELECT (SELECT a, b FROM c) = (SELECT * FROM c)",
        ] {
            if let Err(e) = f.query(sql).await {
                let msg = format!("{e:?}");
                assert!(
                    !msg.contains("expected 1"),
                    "the `SELECT *` operand's width must resolve the outer CTE, not \
                     falsely fire the scalar-arity error on `{sql}`: {msg}"
                );
            }
        }
    });
}

/// bd-9tcne regression: chained NOT MATERIALIZED pre-expansion must honor lexical
/// shadowing. CTE `b`'s body opens its OWN `WITH a` that redeclares the outer
/// `a`, so `x FROM a` inside `b` binds the LOCAL a (=2), not the outer (=1). The
/// pre-expansion previously substituted the outer `a` into `b`'s body ignoring
/// the shadowing WITH, yielding 1 where stock yields 2.
#[test]
fn not_materialized_pre_expansion_honors_lexical_shadowing_9tcne() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Shadowed: local `a` (=2) hides the outer `a` (=1) inside `b`.
        assert_agree(
            &f,
            &r,
            "WITH a AS NOT MATERIALIZED (SELECT 1 AS x), \
                  b AS NOT MATERIALIZED (WITH a AS (SELECT 2 AS x) SELECT x FROM a) \
             SELECT x FROM b",
        )
        .await;
        // Control: no inner redeclaration — `b` sees the outer `a` (=1).
        assert_agree(
            &f,
            &r,
            "WITH a AS NOT MATERIALIZED (SELECT 1 AS x), \
                  b AS NOT MATERIALIZED (SELECT x FROM a) \
             SELECT x FROM b",
        )
        .await;
    });
}

/// bd-ljuk0: a column-listed CTE or view renames only the columns exposed to
/// outer queries; its body's ORDER BY / WHERE / GROUP BY / HAVING must resolve
/// the body's own result aliases and reject the outer column-list names, while
/// outer queries see only the column-list names and reject the body's aliases.
#[test]
fn bd_ljuk0_column_listed_cte_and_view_body_order_by_matches_stock() {
    fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(_, Some(msg))
            | rusqlite::Error::SqlInputError { msg, .. } => msg,
            other => other.to_string(),
        })?;
        let n = stmt.column_count();
        stmt.query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .map_err(|e| match e {
            rusqlite::Error::SqliteFailure(_, Some(msg))
            | rusqlite::Error::SqlInputError { msg, .. } => msg,
            other => other.to_string(),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| match e {
            rusqlite::Error::SqliteFailure(_, Some(msg))
            | rusqlite::Error::SqlInputError { msg, .. } => msg,
            other => other.to_string(),
        })
    }

    async fn frank_outcome(
        conn: &Connection,
        sql: &str,
        prepared: bool,
    ) -> Result<Vec<Vec<String>>, String> {
        let rows = if prepared {
            match conn.prepare(sql).await {
                Ok(stmt) => stmt.query().await,
                Err(e) => Err(e),
            }
        } else {
            conn.query(sql).await
        };
        rows.map(|rs| {
            rs.iter()
                .map(|r| r.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
    }

    const SETUP: &[&str] = &[
        "CREATE TABLE s(p INT, q INT)",
        "INSERT INTO s VALUES (7, 8), (3, 4), (9, 2), (7, 10)",
        "CREATE VIEW v_ord(c1, c2) AS SELECT p AS x, q AS y FROM s WHERE p > 5 ORDER BY y DESC",
        "CREATE VIEW v_where(c1, c2) AS SELECT p AS x, q AS y FROM s WHERE y > 5",
        "CREATE VIEW v_bad_ord(c1, c2) AS SELECT p AS x, q AS y FROM s ORDER BY c2 DESC",
        "CREATE VIEW v_bad_where(c1, c2) AS SELECT p AS x, q AS y FROM s WHERE c2 > 5",
    ];

    const QUERIES: &[&str] = &[
        // Exact bd-ljuk0 repro (single-use unhinted CTE + LIMIT 1 + body ORDER BY alias).
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s WHERE p > 5 ORDER BY y DESC) SELECT c1 FROM w LIMIT 1",
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s WHERE p > 5 ORDER BY y DESC) SELECT c1, c2 FROM w",
        // Explicit NOT MATERIALIZED with column list + body ORDER BY / WHERE alias.
        "WITH w(c1, c2) AS NOT MATERIALIZED (SELECT p AS x, q AS y FROM s WHERE p > 5 ORDER BY y DESC) SELECT c1, c2 FROM w LIMIT 2",
        "WITH w(c1, c2) AS NOT MATERIALIZED (SELECT p AS x, q AS y FROM s WHERE y > 5 ORDER BY y DESC) SELECT c1, c2 FROM w",
        // Column-listed CTE body ordered by ordinal and by expression.
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s ORDER BY 2 DESC, 1 ASC) SELECT c1, c2 FROM w LIMIT 2",
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s ORDER BY q + p DESC, q ASC) SELECT c1, c2 FROM w LIMIT 3",
        // Column-listed CTE with GROUP BY / HAVING referencing body alias.
        "WITH w(c1, c2) AS (SELECT p AS x, sum(q) AS y FROM s GROUP BY x HAVING y >= 10 ORDER BY y DESC) SELECT c1, c2 FROM w LIMIT 2",
        // Recursive CTE with a column list (ordinal and body alias in ORDER BY).
        "WITH RECURSIVE r(c1, c2) AS (SELECT 1 AS a, 10 AS b UNION ALL SELECT c1 + 1, c2 - 2 FROM r WHERE c1 < 4 ORDER BY 2 DESC) SELECT c1, c2 FROM r",
        "WITH RECURSIVE r(c1, c2) AS (SELECT 1 AS a, 10 AS b UNION ALL SELECT c1 + 1, c2 - 2 FROM r WHERE c1 < 4 ORDER BY b DESC) SELECT c1, c2 FROM r",
        // Compound body with ORDER BY body alias and ordinal.
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s WHERE p > 5 UNION ALL SELECT 1, 99 ORDER BY y DESC) SELECT c1, c2 FROM w LIMIT 3",
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s WHERE p > 5 UNION ALL SELECT 1, 99 ORDER BY 2 DESC) SELECT c1, c2 FROM w LIMIT 3",
        // View with a column list + body ORDER BY / WHERE alias.
        "SELECT c1, c2 FROM v_ord LIMIT 2",
        "SELECT c1, c2 FROM v_where ORDER BY c2 DESC",
        // Negative cases: outer query cannot see body's alias (`y`).
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s WHERE p > 5 ORDER BY y DESC) SELECT y FROM w LIMIT 1",
        "WITH w(c1, c2) AS NOT MATERIALIZED (SELECT p AS x, q AS y FROM s WHERE p > 5 ORDER BY y DESC) SELECT y FROM w",
        "SELECT y FROM v_ord",
        "SELECT y FROM v_where",
        // Negative cases: body cannot see CTE / view column-list names (`c2`).
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s WHERE p > 5 ORDER BY c2 DESC) SELECT c1 FROM w LIMIT 1",
        "WITH w(c1, c2) AS NOT MATERIALIZED (SELECT p AS x, q AS y FROM s WHERE p > 5 ORDER BY c2 DESC) SELECT c1 FROM w",
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s WHERE c2 > 5) SELECT c1 FROM w LIMIT 1",
        "WITH w(c1, c2) AS (SELECT p AS x, q AS y FROM s WHERE p > 5 UNION ALL SELECT 1, 99 ORDER BY c2 DESC) SELECT c1 FROM w",
        "WITH RECURSIVE r(c1, c2) AS (SELECT 1 AS a, 10 AS b UNION ALL SELECT c1 + 1, c2 - 2 FROM r WHERE c1 < 4 ORDER BY c2 DESC) SELECT c1 FROM r",
        "SELECT c1 FROM v_bad_ord",
        "SELECT c1 FROM v_bad_where",
    ];

    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = if file_backed {
                let fpath = dir.path().join("f_ljuk0.db");
                let rpath = dir.path().join("r_ljuk0.db");
                (
                    Connection::open(fpath.to_str().unwrap()).await.unwrap(),
                    rusqlite::Connection::open(rpath).unwrap(),
                )
            } else {
                (
                    Connection::open(":memory:").await.unwrap(),
                    rusqlite::Connection::open_in_memory().unwrap(),
                )
            };
            let label = if file_backed { "file" } else { "memory" };
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            for sql in QUERIES {
                let stock = stock_outcome(&r, sql);
                for prepared in [false, true] {
                    let frank = frank_outcome(&f, sql, prepared).await;
                    if frank != stock {
                        diffs.push(format!(
                            "[{label} prepared={prepared}] `{sql}`\n  frank={frank:?}\n  stock={stock:?}"
                        ));
                    }
                }
            }
            // NOT MATERIALIZED with column list + ORDER BY body alias must still
            // re-evaluate random() independently per reference.
            let nm_rand = "WITH w(c1, c2) AS NOT MATERIALIZED \
                           (SELECT random() AS x, q AS y FROM s WHERE p > 7 ORDER BY y DESC) \
                           SELECT (SELECT c1 FROM w LIMIT 1) = (SELECT c1 FROM w LIMIT 1)";
            let got = frank_rows(&f, nm_rand).await;
            assert_eq!(
                got,
                vec![vec!["0".to_owned()]],
                "[{label}] NOT MATERIALIZED column-listed CTE with ORDER BY must re-evaluate per use"
            );
            f.close().await.unwrap();
        }
        assert!(
            diffs.is_empty(),
            "bd-ljuk0 {} divergence(s) from rusqlite 3.54.1:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
