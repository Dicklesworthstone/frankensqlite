//! bd-xmpma: an OR-absorbing / AND-absorbing WHERE containing an ERRORING
//! uncorrelated subquery must fold at compile time (stock semantics) so a bare
//! aggregate (count(*)) returns a single correctly-aggregated row, not per-row
//! NULLs. Regression: frank routed count(*) FROM t WHERE (erroring subq) OR TRUE
//! to the per-row execute_join_select fallback (which short-circuits the WHERE
//! but does NOT aggregate) → three NULL rows instead of the count 3.
//!
//! Oracle (sqlite3 3.46.1) confirmed:
//!   (SELECT 1 FROM json_each('bad')) OR TRUE  → count 3 (subq absorbed, unrun)
//!   (SELECT 1 FROM json_each('bad')) OR 1      → count 3
//!   (SELECT 1 FROM json_each('bad')) AND 0     → count 0 (absorbed false)
//!   (SELECT 1 FROM json_each('bad'))           → ERROR (subq is evaluated)

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn count_scalar(rows: &[fsqlite_core::connection::Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer count, got {other:?}"),
    }
}

#[test]
fn bd_xmpma_aggregate_absorbing_where_with_erroring_subquery_folds() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a, b);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1,5),(2,10),(3,15);")
            .await
            .unwrap();

        // OR-absorbing: the erroring uncorrelated subquery is discarded by the
        // compile-time fold, WHERE is always true, count(*) = 3 (single row).
        assert_eq!(
            count_scalar(
                &conn
                    .query("SELECT count(*) FROM t WHERE (SELECT 1 FROM json_each('bad')) OR TRUE;")
                    .await
                    .unwrap()
            ),
            3,
            "bd-xmpma: (erroring subq) OR TRUE must absorb to a single count of 3, not per-row NULLs"
        );
        assert_eq!(
            count_scalar(
                &conn
                    .query("SELECT count(*) FROM t WHERE (SELECT 1 FROM json_each('bad')) OR 1;")
                    .await
                    .unwrap()
            ),
            3,
            "OR 1 absorbs identically to OR TRUE"
        );

        // AND-absorbing: WHERE is always false, count(*) = 0; subq not evaluated.
        assert_eq!(
            count_scalar(
                &conn
                    .query("SELECT count(*) FROM t WHERE (SELECT 1 FROM json_each('bad')) AND 0;")
                    .await
                    .unwrap()
            ),
            0,
            "(erroring subq) AND 0 must absorb to count 0"
        );

        // Control: a VALID subquery in the same absorbing position still folds to 3.
        assert_eq!(
            count_scalar(
                &conn
                    .query("SELECT count(*) FROM t WHERE (SELECT 1) OR TRUE;")
                    .await
                    .unwrap()
            ),
            3,
            "valid subquery under OR TRUE also folds to 3"
        );

        // Control: NON-aggregate projection under the same WHERE is unaffected
        // (already correct pre-fix) — must still return the three rows.
        let rows = conn
            .query("SELECT a FROM t WHERE (SELECT 1 FROM json_each('bad')) OR TRUE ORDER BY a;")
            .await
            .unwrap();
        let vals: Vec<i64> = rows
            .iter()
            .map(|r| match r.values()[0] {
                SqliteValue::Integer(n) => n,
                ref o => panic!("expected int, got {o:?}"),
            })
            .collect();
        assert_eq!(
            vals,
            vec![1, 2, 3],
            "non-aggregate scan under OR TRUE unaffected"
        );

        // Control: a bare erroring subquery WHERE (NOT absorbed) still surfaces
        // the error rather than being silently swallowed to NULL.
        assert!(
            conn.query("SELECT count(*) FROM t WHERE (SELECT 1 FROM json_each('bad'));")
                .await
                .is_err(),
            "an unabsorbed erroring subquery in WHERE must surface its error"
        );
    });
}
