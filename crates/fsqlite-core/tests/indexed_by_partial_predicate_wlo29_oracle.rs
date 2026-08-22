//! bd-wlo29: the `INDEXED BY` partial-index cover check (GH#173 / 102bbdb07)
//! was wrong in BOTH directions. This keeper pins the corrected behavior
//! against the stock `sqlite3` CLI (3.46.1); every reject/allow verdict below
//! was copied verbatim from its output.
//!
//! * H3 — the check was TOO STRICT: it required a *syntactic* conjunct match,
//!   so the common idiom (a partial index `WHERE b IS NOT NULL` used with a
//!   query `WHERE b = 5`) wrongly errored "no query solution". Stock SQLite's
//!   `sqlite3ExprImpliesExpr` allows a null-rejecting comparison to cover an
//!   `IS NOT NULL`, and an OR whose branch is covered — but NOT arithmetic
//!   implication (`b > 5` still does not cover `b > 0`).
//!
//! * H4 — the check was TOO LOOSE: it pooled OUTER-join ON conjuncts globally,
//!   so a forced partial scan on a LEFT JOIN's *preserved* (left) table was
//!   judged "covered" by an ON term and honored — silently dropping the
//!   preserved rows. An outer-join ON term may only cover the partial index of
//!   that join's unpreserved (right) table.
//!
//! * L10 — the check only ran on top-level SELECT sources; UPDATE/DELETE
//!   targets and derived-table (subquery) sources skipped it entirely.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn conn(ddl: &[&str]) -> Connection {
    let conn = Connection::open(":memory:").await.unwrap();
    for stmt in ddl {
        conn.execute(stmt)
            .await
            .unwrap_or_else(|e| panic!("DDL `{stmt}` must succeed: {e:?}"));
    }
    conn
}

async fn reject_query(conn: &Connection, sql: &str) {
    let err = conn
        .query(sql)
        .await
        .expect_err(&format!("`{sql}` must be rejected (no query solution)"));
    let msg = err.to_string();
    assert!(
        msg.contains("no query solution"),
        "`{sql}` rejected with the wrong message: {msg}"
    );
}

async fn allow_query(conn: &Connection, sql: &str) {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` must be allowed, got: {e:?}"));
}

async fn reject_exec(conn: &Connection, sql: &str) {
    let err = conn
        .execute(sql)
        .await
        .expect_err(&format!("`{sql}` must be rejected (no query solution)"));
    let msg = err.to_string();
    assert!(
        msg.contains("no query solution"),
        "`{sql}` rejected with the wrong message: {msg}"
    );
}

async fn allow_exec(conn: &Connection, sql: &str) {
    conn.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` must be allowed, got: {e:?}"));
}

// ------------------------------------------------------------------ H3 -----

#[test]
fn h3_null_rejecting_comparison_covers_is_not_null() {
    asupersync::test_utils::run_test(|| async {
        let c = conn(&[
            "CREATE TABLE t(a, b);",
            "CREATE INDEX ni ON t(a) WHERE b IS NOT NULL;",
        ])
        .await;
        // b=5 is NULL-rejecting on b, so it implies `b IS NOT NULL` -> allowed.
        allow_query(&c, "SELECT a FROM t INDEXED BY ni WHERE b = 5;").await;
        // A bare `WHERE b` (truthy) also rejects NULL -> allowed.
        allow_query(&c, "SELECT a FROM t INDEXED BY ni WHERE b;").await;
        // `b IS NULL` is NULL-tolerant and does NOT cover `b IS NOT NULL`.
        reject_query(&c, "SELECT a FROM t INDEXED BY ni WHERE b IS NULL;").await;
    });
}

#[test]
fn h3_null_propagating_operand_and_or_cover_is_not_null_vi8lh() {
    // bd-vi8lh (b): a comparison whose operand is a NULL-propagating expression
    // of `b` (arithmetic / bitwise / concat / unary) still rejects NULL on `b`
    // and covers `b IS NOT NULL`; an OR of null-rejecting terms covers too.
    // CAST, functions, coalesce, and an OR with a non-rejecting arm do NOT cover
    // (all verified vs sqlite3 3.46.1 — the recognizer must not over-reach).
    asupersync::test_utils::run_test(|| async {
        let c = conn(&[
            "CREATE TABLE t(a, b);",
            "CREATE INDEX ni ON t(a) WHERE b IS NOT NULL;",
        ])
        .await;
        // NULL-propagating operands cover `b IS NOT NULL`.
        allow_query(&c, "SELECT a FROM t INDEXED BY ni WHERE b + 0 = 5;").await;
        allow_query(&c, "SELECT a FROM t INDEXED BY ni WHERE -b = -5;").await;
        allow_query(&c, "SELECT a FROM t INDEXED BY ni WHERE b || '' = '5';").await;
        allow_query(&c, "SELECT a FROM t INDEXED BY ni WHERE b & 1 = 1;").await;
        // An OR of null-rejecting arms covers.
        allow_query(&c, "SELECT a FROM t INDEXED BY ni WHERE b = 1 OR b = 5;").await;
        // Non-propagating operands do NOT cover (stock is conservative).
        reject_query(
            &c,
            "SELECT a FROM t INDEXED BY ni WHERE cast(b AS text) = '5';",
        )
        .await;
        reject_query(
            &c,
            "SELECT a FROM t INDEXED BY ni WHERE coalesce(b, 0) = 5;",
        )
        .await;
        reject_query(&c, "SELECT a FROM t INDEXED BY ni WHERE abs(b) = 5;").await;
        // An OR with a non-rejecting arm does NOT cover.
        reject_query(&c, "SELECT a FROM t INDEXED BY ni WHERE b = 1 OR a = 9;").await;
    });
}

#[test]
fn h3_or_branch_covers_or_predicate() {
    asupersync::test_utils::run_test(|| async {
        let c = conn(&[
            "CREATE TABLE t(a, b);",
            "CREATE INDEX o ON t(a) WHERE b = 1 OR b = 2;",
        ])
        .await;
        // A query term covering one OR-branch covers the whole OR predicate.
        allow_query(&c, "SELECT a FROM t INDEXED BY o WHERE b = 1;").await;
        allow_query(&c, "SELECT a FROM t INDEXED BY o WHERE b = 2;").await;
        // A term matching neither branch is not covered.
        reject_query(&c, "SELECT a FROM t INDEXED BY o WHERE b = 3;").await;
    });
}

#[test]
fn h3_arithmetic_implication_is_still_not_a_cover() {
    asupersync::test_utils::run_test(|| async {
        let c = conn(&[
            "CREATE TABLE t(a, b);",
            "CREATE INDEX g ON t(a) WHERE b > 0;",
        ])
        .await;
        // `b > 5` logically implies `b > 0` but stock SQLite still REJECTS it:
        // the cover check is not arithmetic implication.
        reject_query(&c, "SELECT a FROM t INDEXED BY g WHERE b > 5;").await;
        allow_query(&c, "SELECT a FROM t INDEXED BY g WHERE b > 0;").await;
    });
}

// ------------------------------------------------------------------ H4 -----

#[test]
fn h4_outer_on_does_not_cover_preserved_left_table() {
    asupersync::test_utils::run_test(|| async {
        let c = conn(&[
            "CREATE TABLE t1(a, b);",
            "CREATE TABLE t2(x, y);",
            "CREATE INDEX p1 ON t1(a) WHERE b = 1;",
        ])
        .await;
        // t1 is the preserved (left) table of the LEFT JOIN; its rows are
        // emitted regardless of the ON clause, so the ON term `t1.b = 1` may
        // NOT cover t1's partial index. Honoring the partial scan would drop
        // preserved rows -> stock rejects.
        reject_query(
            &c,
            "SELECT * FROM t1 INDEXED BY p1 LEFT JOIN t2 ON t1.b = 1;",
        )
        .await;
    });
}

#[test]
fn h4_outer_on_covers_unpreserved_right_table() {
    asupersync::test_utils::run_test(|| async {
        let c = conn(&[
            "CREATE TABLE t1(a, b);",
            "CREATE TABLE t2(x, y);",
            "CREATE INDEX p2 ON t2(x) WHERE y = 1;",
        ])
        .await;
        // t2 is the unpreserved (right) table; its own ON term `t2.y = 1` DOES
        // constrain its scan, so it covers t2's partial index -> allowed.
        allow_query(
            &c,
            "SELECT * FROM t1 LEFT JOIN t2 INDEXED BY p2 ON t2.y = 1;",
        )
        .await;
    });
}

// ----------------------------------------------------------------- L10 -----

#[test]
fn l10_update_target_partial_index_cover() {
    asupersync::test_utils::run_test(|| async {
        let c = conn(&[
            "CREATE TABLE t(a, b);",
            "CREATE INDEX ni ON t(a) WHERE b IS NOT NULL;",
        ])
        .await;
        allow_exec(&c, "UPDATE t INDEXED BY ni SET a = 1 WHERE b = 5;").await;
        reject_exec(&c, "UPDATE t INDEXED BY ni SET a = 1 WHERE a = 1;").await;
    });
}

#[test]
fn l10_delete_target_partial_index_cover() {
    asupersync::test_utils::run_test(|| async {
        let c = conn(&[
            "CREATE TABLE t(a, b);",
            "CREATE INDEX g ON t(a) WHERE b > 0;",
        ])
        .await;
        allow_exec(&c, "DELETE FROM t INDEXED BY g WHERE b > 0;").await;
        reject_exec(&c, "DELETE FROM t INDEXED BY g WHERE a = 1;").await;
    });
}

#[test]
fn l10_derived_table_source_is_checked() {
    asupersync::test_utils::run_test(|| async {
        let c = conn(&[
            "CREATE TABLE t(a, b);",
            "CREATE INDEX g ON t(a) WHERE b > 0;",
        ])
        .await;
        // The forced partial index lives on a subquery (derived-table) source.
        allow_query(
            &c,
            "SELECT * FROM (SELECT a FROM t INDEXED BY g WHERE b > 0);",
        )
        .await;
        reject_query(
            &c,
            "SELECT * FROM (SELECT a FROM t INDEXED BY g WHERE a = 1);",
        )
        .await;
    });
}

// ------------------------------------------------------------------ (a) -----
// bd-vi8lh (a): a LEFT-JOIN right table STRENGTH-REDUCES to INNER when a WHERE
// term is null-rejecting on one of its columns; the WHERE terms then cover its
// forced partial index (stock's LEFT->INNER reduction). SAFETY: the check is
// prepare-only accept/reject; execution still filters null-extended rows, so an
// accepted case is verified ROW-FOR-ROW against the stock oracle (sqlite3
// 3.46.1), not just allow/reject — an accept that returned wrong rows would be a
// separate execution bug.

async fn int_rows(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` must be allowed, got: {e:?}"))
        .iter()
        .map(|row| match &row.values()[0] {
            SqliteValue::Integer(n) => *n,
            other => panic!("`{sql}` expected integer rows, got {other:?}"),
        })
        .collect()
}

#[test]
fn h4_left_join_strength_reduction_cover_vi8lh() {
    asupersync::test_utils::run_test(|| async {
        // a(i)=1,2,3; t(j,b)=(1,5); partial index pi ON t(j) WHERE b IS NOT NULL.
        let c = conn(&[
            "CREATE TABLE a(i);",
            "CREATE TABLE t(j, b);",
            "INSERT INTO a(i) VALUES (1),(2),(3);",
            "INSERT INTO t(j, b) VALUES (1, 5);",
            "CREATE INDEX pi ON t(j) WHERE b IS NOT NULL;",
        ])
        .await;

        // WHERE t.b = 5 is null-rejecting on t.b -> LEFT->INNER; the WHERE term
        // covers pi's `b IS NOT NULL`. Execution filters a.i=2,3 (they null-extend
        // and t.b=5 is not true). Stock: ACCEPT, rows=[1].
        assert_eq!(
            int_rows(
                &c,
                "SELECT a.i FROM a LEFT JOIN t INDEXED BY pi ON a.i = t.j WHERE t.b = 5 ORDER BY a.i;",
            )
            .await,
            vec![1],
            "WHERE t.b=5 strength-reduces and returns stock's [1]",
        );
        // A NULL-propagating operand (t.b + 0 = 5) also strength-reduces (reuses
        // facet (b) expr_null_propagates_from). Stock: ACCEPT, rows=[1].
        assert_eq!(
            int_rows(
                &c,
                "SELECT a.i FROM a LEFT JOIN t INDEXED BY pi ON a.i = t.j WHERE t.b + 0 = 5 ORDER BY a.i;",
            )
            .await,
            vec![1],
            "WHERE t.b+0=5 (null-propagating) strength-reduces and returns [1]",
        );

        // t.b IS NULL is NULL-tolerant -> NO strength reduction. Stock: REJECT.
        reject_query(
            &c,
            "SELECT a.i FROM a LEFT JOIN t INDEXED BY pi ON a.i = t.j WHERE t.b IS NULL;",
        )
        .await;
        // A WHERE term on the OTHER table (a.i > 0) does not strength-reduce t.
        // Stock: REJECT.
        reject_query(
            &c,
            "SELECT a.i FROM a LEFT JOIN t INDEXED BY pi ON a.i = t.j WHERE a.i > 0;",
        )
        .await;
        // Pure LEFT join, ON-only: `a.i=t.j` does not cover `b IS NOT NULL`, and
        // no WHERE term strength-reduces. Stock: REJECT.
        reject_query(
            &c,
            "SELECT a.i FROM a LEFT JOIN t INDEXED BY pi ON a.i = t.j;",
        )
        .await;

        // CONTROL (must NOT regress): the null-rejecting term lives in the ON, so
        // it covers pi for the UNPRESERVED right table via the existing outer-ON
        // path (no strength reduction needed); the LEFT join stays PRESERVED so
        // all a rows survive. Stock: ACCEPT, rows=[1,2,3].
        assert_eq!(
            int_rows(
                &c,
                "SELECT a.i FROM a LEFT JOIN t INDEXED BY pi ON a.i = t.j AND t.b = 5 ORDER BY a.i;",
            )
            .await,
            vec![1, 2, 3],
            "ON-covers control: LEFT preserved, all rows [1,2,3] (no regression)",
        );
    });
}
