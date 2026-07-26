//! Integration tests for PLANNER-3: the `try_prepare_simple_join_rows` wire
//! that routes multi-table FROM clauses through
//! `order_join_inputs_with_hints`.
//!
//! This is the *gate-only* stage of the wire: the planner is consulted for
//! every inner-only multi-table FROM, but the 5 parallel structures in
//! `prepare_simple_join_select_rows_with_scanner` are NOT yet reshaped. These
//! tests guard correctness — any future reshape must keep them green.
//!
//! Coverage:
//! 1. **small-joins-big**: ANALYZE populates sqlite_stat1 with a small build
//!    side and a big probe side. The query must return the same result set
//!    regardless of source order, and regardless of whether a reorder would
//!    be chosen internally.
//! 2. **LEFT JOIN fall-through**: when any join is LEFT, the planner wire
//!    must skip the reorder query entirely (verified behaviorally — result
//!    correctness is preserved).
//!
//! These tests exercise the SELECT path end-to-end, so a broken reshape
//! (once landed) will show up as wrong result sets.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Build a canonicalized (sorted) string representation of a result set for
/// order-insensitive comparison.
fn canonicalize(rows: Vec<Vec<SqliteValue>>) -> Vec<String> {
    let mut out: Vec<String> = rows
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|v| format!("{:?}", v))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    out.sort();
    out
}

async fn select_all(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .expect("query succeeds")
        .iter()
        .map(|r| r.values().to_vec())
        .collect()
}

#[test]
fn inner_join_small_and_big_returns_same_rows_regardless_of_source_order() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t_small (id INTEGER PRIMARY KEY, tag TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE t_big (id INTEGER PRIMARY KEY, ref_id INTEGER, v INTEGER);")
            .await
            .unwrap();

        // 10 rows in t_small, 10_000 rows in t_big. Each t_big row points at a
        // t_small row via ref_id (1..=10). This ensures that a proper inner join
        // returns 10_000 rows (one per t_big row, matched to the corresponding
        // t_small row).
        conn.execute("BEGIN;").await.unwrap();
        for i in 1..=10i64 {
            conn.execute_with_params(
                "INSERT INTO t_small(id, tag) VALUES (?1, ?2);",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("tag{}", i).into()),
                ],
            )
            .await
            .unwrap();
        }
        for i in 1..=10_000i64 {
            let ref_id = ((i - 1) % 10) + 1;
            conn.execute_with_params(
                "INSERT INTO t_big(id, ref_id, v) VALUES (?1, ?2, ?3);",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Integer(ref_id),
                    SqliteValue::Integer(i * 2),
                ],
            )
            .await
            .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();
        conn.execute("ANALYZE;").await.unwrap();

        // Query A: big joined against small (planner would prefer small-build,
        // big-probe; but the reshape is gated off so this executes in source
        // order).
        let rows_big_first = select_all(
            &conn,
            "SELECT t_big.id, t_small.tag FROM t_big JOIN t_small ON t_big.ref_id = t_small.id;",
        )
        .await;

        // Query B: same query with tables in reversed source order. With the
        // reshape landed these would take the same underlying execution path;
        // without it, they just both execute in their source order but must
        // return the same result set.
        let rows_small_first = select_all(
            &conn,
            "SELECT t_big.id, t_small.tag FROM t_small JOIN t_big ON t_big.ref_id = t_small.id;",
        )
        .await;

        assert_eq!(rows_big_first.len(), 10_000, "every t_big row must join");
        assert_eq!(
            canonicalize(rows_big_first),
            canonicalize(rows_small_first),
            "inner-join result set must be order-invariant regardless of FROM-clause source order"
        );
    });
}

#[test]
fn left_join_falls_through_planner_gate_without_reshape() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t_small (id INTEGER PRIMARY KEY, tag TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE t_big (id INTEGER PRIMARY KEY, ref_id INTEGER);")
            .await
            .unwrap();

        // 5 small rows, 20 big rows with half having ref_id matching a small row
        // (id 1..=5) and half with ref_id = 999 (no match — relies on LEFT JOIN
        // NULL-padding).
        conn.execute("BEGIN;").await.unwrap();
        for i in 1..=5i64 {
            conn.execute_with_params(
                "INSERT INTO t_small(id, tag) VALUES (?1, ?2);",
                &[
                    SqliteValue::Integer(i),
                    SqliteValue::Text(format!("s{}", i).into()),
                ],
            )
            .await
            .unwrap();
        }
        for i in 1..=20i64 {
            let ref_id = if i <= 10 { ((i - 1) % 5) + 1 } else { 999 };
            conn.execute_with_params(
                "INSERT INTO t_big(id, ref_id) VALUES (?1, ?2);",
                &[SqliteValue::Integer(i), SqliteValue::Integer(ref_id)],
            )
            .await
            .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();
        conn.execute("ANALYZE;").await.unwrap();

        // LEFT JOIN: every t_big row must be present. 10 rows match a t_small
        // tag; 10 rows have NULL tag. Source order must NOT be reordered by the
        // planner because LEFT JOIN is non-commutative.
        let rows = select_all(
        &conn,
        "SELECT t_big.id, t_small.tag FROM t_big LEFT JOIN t_small ON t_big.ref_id = t_small.id ORDER BY t_big.id;",
    )
    .await;

        assert_eq!(rows.len(), 20, "LEFT JOIN preserves all left-side rows");

        // First 10 rows should have a non-NULL tag; last 10 should be NULL.
        for (idx, row) in rows.iter().enumerate() {
            let id_in = row.first().and_then(|v| match v {
                SqliteValue::Integer(i) => Some(*i),
                _ => None,
            });
            let tag = row.get(1).cloned().unwrap_or(SqliteValue::Null);
            let expected_id = (idx as i64) + 1;
            assert_eq!(id_in, Some(expected_id), "row id {} unexpected", idx);
            if expected_id <= 10 {
                assert!(
                    matches!(tag, SqliteValue::Text(_)),
                    "row {} (id {}) should have a matched tag, got {:?}",
                    idx,
                    expected_id,
                    tag
                );
            } else {
                assert!(
                    matches!(tag, SqliteValue::Null),
                    "row {} (id {}) should have NULL tag (LEFT JOIN padding), got {:?}",
                    idx,
                    expected_id,
                    tag
                );
            }
        }
    });
}

#[test]
fn inner_join_without_analyze_preserves_source_order_result() {
    // When no ANALYZE has been run, sqlite_stat1 is empty and
    // `order_join_inputs_with_hints` returns identity. The query must
    // still execute correctly — this guards against the planner call
    // mis-handling an empty-stats case.
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO a VALUES (1, 100), (2, 200);")
            .await
            .unwrap();
        conn.execute("INSERT INTO b VALUES (10, 1), (11, 2), (12, 1);")
            .await
            .unwrap();
        // NOTE: intentionally no ANALYZE.

        let rows = select_all(
            &conn,
            "SELECT b.id, a.v FROM b JOIN a ON b.a_id = a.id ORDER BY b.id;",
        )
        .await;
        assert_eq!(rows.len(), 3);
        // Just check first row round-trips correctly.
        let first = &rows[0];
        assert!(matches!(first[0], SqliteValue::Integer(10)));
        assert!(matches!(first[1], SqliteValue::Integer(100)));
    });
}
