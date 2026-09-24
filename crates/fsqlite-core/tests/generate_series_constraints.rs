#![recursion_limit = "512"]

//! SQL guards for the built-in series cursor and its xBestIndex/xFilter plan.
//! Large-domain seek bounds are tested directly in fsqlite-ext-misc so that
//! a broken planner cannot make this SQL target scan an unbounded sequence.
#![cfg(feature = "ext-misc")]

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

async fn values(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.query(sql).await.unwrap().iter().map(|row| match row.get(0) {
        Some(SqliteValue::Integer(value)) => *value,
        other => panic!("expected integer for {sql}: {other:?}"),
    }).collect()
}

#[test]
fn series_bounds_preserve_hidden_arguments_and_requested_order() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn.query(
            "SELECT value, start, stop, step FROM generate_series(2, 31, 7) \
             WHERE value > 7 AND value <= 25 ORDER BY value DESC",
        ).await.unwrap();
        assert_eq!(rows.len(), 3);
        for (row, expected) in rows.iter().zip([23, 16, 9]) {
            for (column, expected) in [expected, 2, 31, 7].into_iter().enumerate() {
                assert_eq!(row.get(column), Some(&SqliteValue::Integer(expected)));
            }
        }
        assert_eq!(values(&conn,
            "SELECT value FROM generate_series(20, -10, -3) \
             WHERE value >= -2 AND value < 10 ORDER BY value ASC",
        ).await, [-1, 2, 5, 8]);
    });
}

#[test]
fn series_null_arguments_and_conflicting_bounds_produce_no_rows() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for sql in [
            "SELECT value FROM generate_series(NULL, 4)",
            "SELECT value FROM generate_series(1, NULL)",
            "SELECT value FROM generate_series(1, 4, NULL)",
            "SELECT value FROM generate_series(1, 9) WHERE value > 7 AND value < 3",
            "SELECT value FROM generate_series(1, 9) WHERE value = NULL",
            "SELECT value FROM generate_series(1, 9) WHERE value = 2.5",
        ] {
            assert!(conn.query(sql).await.unwrap().is_empty(), "{sql}");
        }
        assert_eq!(values(&conn, "SELECT value FROM generate_series(1, 3, 0)").await, [1, 2, 3]);
    });
}

#[test]
fn series_fractional_and_large_integer_predicates_keep_exact_members() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        assert_eq!(values(&conn,
            "SELECT value FROM generate_series(-5, 5, 2) \
             WHERE value > -0.25 AND value <= 3.75 ORDER BY value DESC",
        ).await, [3, 1]);
        assert_eq!(values(&conn,
            "SELECT value FROM generate_series(9007199254740990, 9007199254740998) \
             WHERE value = 9007199254740993",
        ).await, [9_007_199_254_740_993]);
        assert_eq!(values(&conn,
            "SELECT value FROM generate_series(9223372036854775805, 9223372036854775807) \
             WHERE value > 9223372036854775806",
        ).await, [i64::MAX]);
    });
}

#[test]
fn series_join_and_aggregate_keep_sql_semantics() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE wanted(value INTEGER)").await.unwrap();
        conn.execute("INSERT INTO wanted VALUES (3), (7), (8)").await.unwrap();
        assert_eq!(values(&conn,
            "SELECT g.value FROM generate_series(1, 11, 2) AS g \
             JOIN wanted AS w ON g.value = w.value ORDER BY g.value DESC",
        ).await, [7, 3]);
        assert_eq!(values(&conn,
            "SELECT sum(value) FROM generate_series(1, 100) WHERE value >= 90",
        ).await, [1045]);
    });
}
