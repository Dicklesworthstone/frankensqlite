#![recursion_limit = "512"]

//! SQL guards for the built-in series cursor and its xBestIndex/xFilter plan.
//! Large-domain seek bounds are tested directly in fsqlite-ext-misc so that
//! a broken planner cannot make this SQL target scan an unbounded sequence.
#![cfg(feature = "ext-misc")]

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

async fn values(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.query(sql)
        .await
        .unwrap()
        .iter()
        .map(|row| match row.get(0) {
            Some(SqliteValue::Integer(value)) => *value,
            other => panic!("expected integer for {sql}: {other:?}"),
        })
        .collect()
}

#[test]
fn series_bounds_preserve_hidden_arguments_and_requested_order() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn
            .query(
                "SELECT value, start, stop, step FROM generate_series(2, 31, 7) \
             WHERE value > 7 AND value <= 25 ORDER BY value DESC",
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        for (row, expected) in rows.iter().zip([23, 16, 9]) {
            for (column, expected) in [expected, 2, 31, 7].into_iter().enumerate() {
                assert_eq!(row.get(column), Some(&SqliteValue::Integer(expected)));
            }
        }
        assert_eq!(
            values(
                &conn,
                "SELECT value FROM generate_series(20, -10, -3) \
             WHERE value >= -2 AND value < 10 ORDER BY value ASC",
            )
            .await,
            [-1, 2, 5, 8]
        );
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
        assert_eq!(
            values(&conn, "SELECT value FROM generate_series(1, 3, 0)").await,
            [1, 2, 3]
        );
    });
}

#[test]
fn series_fractional_and_large_integer_predicates_keep_exact_members() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        assert_eq!(
            values(
                &conn,
                "SELECT value FROM generate_series(-5, 5, 2) \
             WHERE value > -0.25 AND value <= 3.75 ORDER BY value DESC",
            )
            .await,
            [3, 1]
        );
        assert_eq!(
            values(
                &conn,
                "SELECT value FROM generate_series(9007199254740990, 9007199254740998) \
             WHERE value = 9007199254740993",
            )
            .await,
            [9_007_199_254_740_993]
        );
        assert_eq!(
            values(
                &conn,
                "SELECT value FROM generate_series(9223372036854775805, 9223372036854775807) \
             WHERE value > 9223372036854775806",
            )
            .await,
            [i64::MAX]
        );
    });
}

#[test]
fn series_join_and_aggregate_keep_sql_semantics() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE wanted(value INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO wanted VALUES (3), (7), (8)")
            .await
            .unwrap();
        assert_eq!(
            values(
                &conn,
                "SELECT g.value FROM generate_series(1, 11, 2) AS g \
             JOIN wanted AS w ON g.value = w.value ORDER BY g.value DESC",
            )
            .await,
            [7, 3]
        );
        assert_eq!(
            values(
                &conn,
                "SELECT sum(value) FROM generate_series(1, 100) WHERE value >= 90",
            )
            .await,
            [1045]
        );
    });
}

async fn aggregate_rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

/// GH#530's original reproductions must consume every Cartesian tuple. The
/// separately run benchmark checks memory/time; these assert exact SQL results.
#[test]
fn cartesian_series_aggregates_cover_the_issue_and_product_shapes() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for (sql, expected) in [
            (
                "SELECT count(*) FROM generate_series(1,1000)a,generate_series(1,1000)b",
                1_000_000,
            ),
            (
                "SELECT sum((a.value-1)*1000+b.value) \
                 FROM generate_series(1,1000)a,generate_series(1,1000)b",
                500_000_500_000,
            ),
            (
                "SELECT sum(a.value*b.value) FROM generate_series(1,7)a \
                 CROSS JOIN generate_series(2,10,2)b",
                840,
            ),
            (
                "SELECT sum(100*a.value+10*b.value+c.value) FROM generate_series(1,2)a \
                 JOIN generate_series(1,3)b CROSS JOIN generate_series(1,4)c",
                4140,
            ),
        ] {
            assert_eq!(values(&conn, sql).await, [expected], "{sql}");
        }
    });
}

#[test]
fn cartesian_series_aggregates_preserve_hidden_slots_and_parameters() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        assert_eq!(
            aggregate_rows(
                &conn,
                "SELECT sum(a.value),sum(a.start),sum(a.stop),sum(a.step),sum(a.rowid), \
                 sum(b.value),sum(b.start),sum(b.stop),sum(b.step),sum(b.rowid) \
                 FROM generate_series(2,8,3)a,generate_series(11,13,2)b",
            )
            .await,
            vec![
                vec![30, 12, 48, 18, 30, 72, 66, 78, 12, 72]
                    .into_iter()
                    .map(SqliteValue::Integer)
                    .collect::<Vec<_>>()
            ]
        );
        let sql = "SELECT count(*),sum(a.value*b.value) FROM generate_series(1,?1)a, \
                   generate_series(1,?2)b";
        let prepared = conn.prepare(sql).await.unwrap();
        for bound in [7_i64, 0, 1, 9, 0] {
            let params = [SqliteValue::Integer(bound), SqliteValue::Integer(3)];
            let expected = vec![
                SqliteValue::Integer(bound * 3),
                if bound == 0 {
                    SqliteValue::Null
                } else {
                    SqliteValue::Integer(bound * (bound + 1) * 3)
                },
            ];
            let direct = conn.query_with_params(sql, &params).await.unwrap();
            let reused = prepared.query_with_params(&params).await.unwrap();
            assert_eq!(direct.len(), 1);
            assert_eq!(reused.len(), 1);
            assert_eq!(direct[0].values(), expected.as_slice(), "bound={bound}");
            assert_eq!(reused[0].values(), expected.as_slice(), "bound={bound}");
        }
    });
}

#[test]
fn cartesian_series_empty_inputs_nulls_and_overflow_keep_aggregate_semantics() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for sources in [
            "generate_series(1,0)a,generate_series(1,3)b",
            "generate_series(1,3)a,generate_series(1,0)b",
            "generate_series(1,3)a,generate_series(NULL,3)b",
            "generate_series(1,3)a,generate_series(1,0)b,generate_series(1,2)c",
        ] {
            let sql = format!(
                "SELECT count(*),count(a.value),sum(a.value),avg(a.value),min(a.value),max(a.value) \
                 FROM {sources}"
            );
            assert_eq!(
                aggregate_rows(&conn, &sql).await,
                vec![vec![
                    SqliteValue::Integer(0),
                    SqliteValue::Integer(0),
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Null,
                    SqliteValue::Null,
                ]],
                "{sql}"
            );
            assert!(
                aggregate_rows(&conn, &format!("{sql} GROUP BY a.value"))
                    .await
                    .is_empty()
            );
        }
        assert_eq!(
            aggregate_rows(
                &conn,
                "SELECT count(*),count(NULL),sum(NULL),avg(NULL),min(NULL),max(NULL) \
                 FROM generate_series(1,2)a,generate_series(1,3)b",
            )
            .await,
            vec![vec![
                SqliteValue::Integer(6),
                SqliteValue::Integer(0),
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Null,
            ]]
        );
        let overflow = "SELECT sum(9223372036854775807) FROM generate_series(1,1)a, \
                        generate_series(1,2)b";
        let error = conn.query(overflow).await.unwrap_err().to_string();
        assert!(error.contains("overflow"), "{error}");
        // Failed accumulation must not leak state into a subsequent execution.
        assert_eq!(
            values(
                &conn,
                "SELECT sum(a.value*b.value) FROM generate_series(1,2)a,generate_series(1,3)b"
            )
            .await,
            [18]
        );
    });
}

#[test]
fn cartesian_series_validates_later_function_arity_before_an_empty_product() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for stop in [0, 1] {
            for arguments in ["", "1,2,3,4"] {
                let sql = format!(
                    "SELECT count(*) FROM generate_series(1,{stop})a \
                     CROSS JOIN generate_series({arguments})b"
                );
                let error = conn.query(&sql).await.unwrap_err().to_string();
                assert!(error.contains("generate_series"), "{sql}: {error}");
            }
        }
    });
}

#[test]
fn cartesian_series_limits_apply_to_final_aggregate_rows_in_internal_callers() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let from = "FROM generate_series(1,2)a,generate_series(1,3)b";
        for suffix in ["LIMIT 0", "LIMIT 1 OFFSET 1", "LIMIT -1 OFFSET 1"] {
            let sql = format!("SELECT count(*) {from} {suffix}");
            assert!(aggregate_rows(&conn, &sql).await.is_empty(), "{sql}");
        }
        conn.execute("CREATE TABLE aggregate_out(n INTEGER)")
            .await
            .unwrap();
        for suffix in ["LIMIT 0", "LIMIT 1 OFFSET 1"] {
            conn.execute(&format!(
                "WITH unused AS (VALUES(1)) INSERT INTO aggregate_out \
                 SELECT count(*) {from} {suffix}"
            ))
            .await
            .unwrap();
        }
        assert_eq!(
            values(&conn, "SELECT count(*) FROM aggregate_out").await,
            [0]
        );
        conn.execute(&format!(
            "WITH unused AS (VALUES(1)) INSERT INTO aggregate_out SELECT count(*) {from} LIMIT 1"
        ))
        .await
        .unwrap();
        assert_eq!(values(&conn, "SELECT n FROM aggregate_out").await, [6]);
    });
}

struct CountSeriesArgument(Arc<AtomicUsize>);

impl fsqlite_func::ScalarFunction for CountSeriesArgument {
    fn name(&self) -> &str {
        "count_series_argument"
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(args[0].clone())
    }
}

#[test]
fn cartesian_series_custom_arguments_keep_existing_callback_behavior() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        conn.register_nondeterministic_scalar_function(CountSeriesArgument(Arc::clone(&calls)));
        // Application functions retain the general executor. This assertion
        // preserves its existing callback frequency; it is not a stock oracle
        // for how often SQLite reevaluates an inner table-function argument.
        let sql = "SELECT count(*) FROM generate_series(1,count_series_argument(2))a, \
                   generate_series(1,count_series_argument(3))b";
        assert_eq!(values(&conn, sql).await, [6]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        calls.store(0, Ordering::Relaxed);
        assert_eq!(
            values(
                &conn,
                "SELECT sum(count_series_argument(a.value+b.value)) FROM generate_series(1,2)a, \
             generate_series(1,3)b",
            )
            .await,
            [21]
        );
        assert_eq!(calls.load(Ordering::Relaxed), 6);
    });
}

#[cfg(feature = "native")]
#[test]
fn cartesian_series_observes_operation_deadline_after_source_scans_finish() {
    use asupersync::time::{TimeSource, TimerDriver, TimerDriverHandle};
    use asupersync::types::{RegionId, TaskId, Time};
    use std::sync::atomic::AtomicU64;

    // Advance only when the operation observes its clock. The deadline is
    // independent of host speed and thread scheduling, so it cannot expire
    // while a loaded test runner is paused between SQL operations.
    #[derive(Debug, Default)]
    struct TickClock(AtomicU64);

    impl TimeSource for TickClock {
        fn now(&self) -> Time {
            Time::from_nanos(self.0.fetch_add(1, Ordering::Relaxed))
        }
    }

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for third_stop in [1, 100] {
            let clock = Arc::new(TickClock::default());
            let driver =
                TimerDriverHandle::new(Arc::new(TimerDriver::with_clock(Arc::clone(&clock))));
            let native = asupersync::Cx::new_with_drivers(
                RegionId::new_for_test(0, 1),
                TaskId::new_for_test(0, 1),
                asupersync::Budget::new().with_deadline(Time::from_nanos(1024)),
                None,
                None,
                None,
                Some(driver),
                None,
            );
            let operation = conn.root_cx().create_child();
            operation.set_native_cx(native);
            let binding = conn.bind_operation_cx(&operation);
            let sql = format!(
                "SELECT count(*) FROM generate_series(1,100)a, \
                 generate_series(1,100)b,generate_series(1,{third_stop})c"
            );
            let result = conn.query(&sql).await;
            if third_stop == 1 {
                // Both executions scan three sources shorter than the
                // cursor's checkpoint interval. The small product proves
                // the deadline allows all source scans and SQL setup.
                let rows = result.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(10_000)));
                assert!(clock.0.load(Ordering::Relaxed) < 1024);
            } else {
                // The same source-scan checkpoint count leaves the larger
                // product to exhaust the budget during tuple enumeration.
                assert!(
                    matches!(result, Err(fsqlite_error::FrankenError::Interrupt)),
                    "Cartesian enumeration must observe its deadline: {result:?}"
                );
                assert!(clock.0.load(Ordering::Relaxed) >= 1024);
            }
            drop(binding);
        }
        // Operation cancellation must not poison the connection root.
        assert_eq!(
            values(&conn, "SELECT count(*) FROM generate_series(1,3)").await,
            [3]
        );
    });
}
