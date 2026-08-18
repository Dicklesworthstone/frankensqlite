//! GH #173 (bd-qfgsa): `SELECT ... INDEXED BY <idx>` where `<idx>` is a PARTIAL
//! index must be REJECTED at prepare with "no query solution" UNLESS the query's
//! WHERE (or a join ON clause) *syntactically* covers every conjunct of the
//! index's partial predicate.
//!
//! The contract below is verified against the stock `sqlite3` CLI (3.46.1) —
//! the expected reject/allow verdicts are copied verbatim from its behavior:
//!
//! ```text
//! CREATE TABLE t(a,b,c,d); CREATE INDEX idx ON t(a) WHERE b>0;
//! SELECT a,b FROM t INDEXED BY idx WHERE a=1;              -- ERROR: no query solution
//! SELECT a,b FROM t INDEXED BY idx WHERE a=1 AND b>0;      -- OK
//! SELECT a,b FROM t INDEXED BY idx WHERE a=1 AND b>5;      -- ERROR: no query solution
//! SELECT a,b FROM t INDEXED BY idx WHERE a=1 AND b>0 AND c=2; -- OK
//! ```
//!
//! The critical case is `b>5`: even though it *logically implies* `b>0`, stock
//! SQLite REJECTS it. SQLite performs a SYNTACTIC conjunct-cover check, NOT
//! general logical implication.

use fsqlite_core::connection::Connection;

/// Open an in-memory connection and run the partial/plain index DDL.
async fn conn_with_indexes() -> Connection {
    let conn = Connection::open(":memory:").await.unwrap();
    for ddl in [
        "CREATE TABLE t(a, b, c, d);",
        // Partial index whose predicate is a single conjunct `b > 0`.
        "CREATE INDEX idx ON t(a) WHERE b > 0;",
        // Partial index whose predicate is two conjuncts `b > 0 AND c < 10`.
        "CREATE INDEX idx2 ON t(a) WHERE b > 0 AND c < 10;",
        // Partial index whose predicate is an equality `d = 5`.
        "CREATE INDEX idx_eq ON t(a) WHERE d = 5;",
        // A plain (non-partial) index on the same table.
        "CREATE INDEX idx_plain ON t(a);",
    ] {
        conn.execute(ddl)
            .await
            .unwrap_or_else(|e| panic!("DDL `{ddl}` must succeed: {e:?}"));
    }
    conn
}

/// A query that stock SQLite REJECTS at prepare with "no query solution".
async fn assert_rejected(conn: &Connection, sql: &str) {
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

/// A query that stock SQLite ALLOWS (prepares/executes successfully).
async fn assert_allowed(conn: &Connection, sql: &str) {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` must be allowed, but was rejected: {e:?}"));
}

#[test]
fn gh173_forced_partial_index_requires_syntactic_predicate_cover() {
    asupersync::test_utils::run_test(|| async {
        let conn = conn_with_indexes().await;

        // ---- single-conjunct partial predicate `b > 0` (index `idx`) ----

        // No `b > 0` term at all -> reject.
        assert_rejected(&conn, "SELECT a, b FROM t INDEXED BY idx WHERE a = 1;").await;
        // No WHERE clause at all -> reject.
        assert_rejected(&conn, "SELECT a, b FROM t INDEXED BY idx;").await;
        // Exact partial predicate present -> allow.
        assert_allowed(&conn, "SELECT a, b FROM t INDEXED BY idx WHERE a = 1 AND b > 0;").await;
        // CRITICAL: `b > 5` logically implies `b > 0` but SQLite still REJECTS
        // (syntactic match, not implication).
        assert_rejected(&conn, "SELECT a, b FROM t INDEXED BY idx WHERE a = 1 AND b > 5;").await;
        // Superset of conjuncts (extra `c = 2`) still contains `b > 0` -> allow.
        assert_allowed(
            &conn,
            "SELECT a, b FROM t INDEXED BY idx WHERE a = 1 AND b > 0 AND c = 2;",
        )
        .await;
        // Conjunct order does not matter -> allow.
        assert_allowed(&conn, "SELECT a, b FROM t INDEXED BY idx WHERE b > 0 AND a = 1;").await;
        // A table-qualified predicate covers the bare stored predicate -> allow.
        assert_allowed(&conn, "SELECT a, b FROM t INDEXED BY idx WHERE a = 1 AND t.b > 0;").await;
        // A commuted comparison (`0 < b`) covers `b > 0` -> allow.
        assert_allowed(&conn, "SELECT a, b FROM t INDEXED BY idx WHERE a = 1 AND 0 < b;").await;

        // ---- two-conjunct partial predicate `b > 0 AND c < 10` (index `idx2`) ----

        // Only one of the two conjuncts present -> reject.
        assert_rejected(&conn, "SELECT a FROM t INDEXED BY idx2 WHERE a = 1 AND b > 0;").await;
        // Both conjuncts present (any order) -> allow.
        assert_allowed(
            &conn,
            "SELECT a FROM t INDEXED BY idx2 WHERE a = 1 AND b > 0 AND c < 10;",
        )
        .await;
        assert_allowed(
            &conn,
            "SELECT a FROM t INDEXED BY idx2 WHERE c < 10 AND b > 0 AND a = 1;",
        )
        .await;

        // ---- equality partial predicate `d = 5` (index `idx_eq`) ----

        assert_rejected(&conn, "SELECT a FROM t INDEXED BY idx_eq WHERE a = 1;").await;
        assert_allowed(&conn, "SELECT a FROM t INDEXED BY idx_eq WHERE d = 5;").await;
        assert_allowed(&conn, "SELECT a FROM t INDEXED BY idx_eq WHERE a = 1 AND d = 5;").await;

        // ---- CONTROL: a NON-partial forced index is usable with any WHERE ----

        assert_allowed(&conn, "SELECT a FROM t INDEXED BY idx_plain WHERE a = 1;").await;
        assert_allowed(&conn, "SELECT a FROM t INDEXED BY idx_plain;").await;
        assert_allowed(&conn, "SELECT a FROM t INDEXED BY idx_plain WHERE b > 5;").await;
    });
}
