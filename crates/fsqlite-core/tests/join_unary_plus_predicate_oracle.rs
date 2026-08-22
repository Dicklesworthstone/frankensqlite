//! bd-g54oq regression guard: unary plus (`+expr`) in a JOIN predicate.
//!
//! `eval_join_expr` (the fallback join executor) once handled UnaryOp
//! Negate/Not/BitNot and let `UnaryOp::Plus` fall through to NULL. Since the
//! parser emits `UnaryOp::Plus` (and `+expr` is the classic SQLite idiom to
//! defeat index usage in a predicate), a join-fallback predicate like
//! `+t.id = u.id` evaluated to NULL — never truthy — and silently dropped
//! EVERY row. The arm is now `UnaryOp::Plus => val` (a no-op, matching C
//! SQLite); this keeper pins that against the rusqlite oracle across a few
//! predicate shapes so the fallthrough cannot regress.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

const SEED_STMTS: &[&str] = &[
    "CREATE TABLE t (id INTEGER, v TEXT)",
    "CREATE TABLE u (id INTEGER, w TEXT)",
    "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')",
    "INSERT INTO u VALUES (2,'x'),(3,'y'),(4,'z')",
];

/// Each query joins t and u with a predicate that uses unary plus. The bug
/// yielded an empty result for all of them; the oracle yields the real join.
const QUERIES: &[&str] = &[
    // Unary plus on the left of an equijoin ON.
    "SELECT t.v, u.w FROM t JOIN u ON +t.id = u.id ORDER BY t.id",
    // Unary plus on the right.
    "SELECT t.v, u.w FROM t JOIN u ON t.id = +u.id ORDER BY t.id",
    // Unary plus inside a comma-join WHERE predicate.
    "SELECT t.v, u.w FROM t, u WHERE +t.id = u.id ORDER BY t.id",
    // Unary plus in a range predicate (expression join shape).
    "SELECT t.v, u.w FROM t JOIN u ON +t.id >= u.id ORDER BY t.id, u.id",
];

fn oracle_rows(sql: &str) -> Vec<(String, String)> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for stmt in SEED_STMTS {
        conn.execute(stmt, []).unwrap();
    }
    let mut stmt = conn.prepare(sql).unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    rows
}

#[test]
fn unary_plus_join_predicate_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for stmt in SEED_STMTS {
            conn.execute(stmt).await.expect("seed");
        }

        for sql in QUERIES {
            let expected = oracle_rows(sql);
            // The bug's signature: `expected` is non-empty but the buggy engine
            // returned nothing. Assert the oracle actually exercises rows so a
            // future schema change can't make this vacuously pass.
            assert!(
                !expected.is_empty(),
                "oracle premise: `{sql}` must return rows"
            );

            let rows = conn
                .query(sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
            let got: Vec<(String, String)> = rows
                .iter()
                .map(|r| {
                    let cell = |i: usize| match r.values()[i] {
                        SqliteValue::Text(ref s) => s.as_ref().to_owned(),
                        ref other => panic!("`{sql}` col {i} not text: {other:?}"),
                    };
                    (cell(0), cell(1))
                })
                .collect();

            assert_eq!(
                got, expected,
                "unary-plus join predicate `{sql}` diverged from the C SQLite oracle"
            );
        }
    });
}
