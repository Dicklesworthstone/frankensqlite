//! Live SQL coverage for the generate_series xBestIndex/xFilter contract.
//! Named instances exercise hidden WHERE arguments; table-function syntax
//! retains its positional invocation protocol. These tests do not assert that
//! the Connection planner forwards ORDER BY metadata or avoids materialization.
#![cfg(feature = "misc")]
// Connection futures are intentionally !Send and deeply composed, just as in
// the public facade. This test drives them on the caller's current-thread runtime.
#![allow(clippy::future_not_send, clippy::large_futures)]

use fsqlite::{Connection, SqliteValue};

async fn named_series() -> Connection {
    let connection = Connection::open(":memory:").await.unwrap();
    connection
        .execute("CREATE VIRTUAL TABLE seq USING generate_series")
        .await
        .unwrap();
    connection
}

async fn integer_rows(connection: &Connection, sql: &str, width: usize) -> Vec<Vec<i64>> {
    connection
        .query(sql)
        .await
        .unwrap()
        .iter()
        .map(|row| {
            (0..width)
                .map(|column| match row.get(column) {
                    Some(SqliteValue::Integer(value)) => *value,
                    other => panic!("expected integer column {column} for {sql}, got {other:?}"),
                })
                .collect()
        })
        .collect()
}

#[test]
fn hidden_constraints_bind_by_column_and_preserve_original_parameters() {
    asupersync::test_utils::run_test(|| async {
        let connection = named_series().await;
        let rows = integer_rows(
            &connection,
            "SELECT value, start, stop, step FROM seq \
             WHERE stop=11 AND value>=4 AND step=2 AND start=1 AND value<=8 ORDER BY value",
            4,
        )
        .await;
        assert_eq!(rows, vec![vec![5, 1, 11, 2], vec![7, 1, 11, 2]]);
        let rows = integer_rows(
            &connection,
            "SELECT value FROM seq WHERE start=2 AND step=0 AND stop=4 ORDER BY value",
            1,
        )
        .await;
        assert_eq!(rows, vec![vec![2], vec![3], vec![4]]);
    });
}

#[test]
fn duplicate_hidden_and_unconsumed_value_constraints_remain_effective() {
    asupersync::test_utils::run_test(|| async {
        let connection = named_series().await;
        assert!(
            connection
                .query("SELECT value FROM seq WHERE start=1 AND start=3 AND stop=7")
                .await
                .unwrap()
                .is_empty()
        );
        let rows = integer_rows(
            &connection,
            "SELECT value FROM seq WHERE start=1 AND stop=9 AND step=2 \
             AND value>=2 AND value>=5 AND value<>7 ORDER BY value",
            1,
        )
        .await;
        assert_eq!(rows, vec![vec![5], vec![9]]);
        let rows = integer_rows(&connection,
            "SELECT value FROM seq WHERE start=1 AND stop=9 AND step=2 AND 4<value AND 8>value ORDER BY value", 1).await;
        assert_eq!(rows, vec![vec![5], vec![7]]);
    });
}

#[test]
fn reverse_sequences_and_rowid_bounds_keep_their_step_alignment() {
    asupersync::test_utils::run_test(|| async {
        let connection = named_series().await;
        let rows = integer_rows(
            &connection,
            "SELECT value, start, stop, step FROM seq WHERE start=10 AND stop=-10 AND step=-3 \
             AND value>-4.5 AND value<=6.25 ORDER BY value",
            4,
        )
        .await;
        assert_eq!(
            rows,
            vec![
                vec![-2, 10, -10, -3],
                vec![1, 10, -10, -3],
                vec![4, 10, -10, -3]
            ]
        );
        let rows = integer_rows(
            &connection,
            "SELECT value FROM seq WHERE start=10 AND stop=-10 AND step=-3 \
             AND rowid>=1 AND rowid<=7 ORDER BY value DESC",
            1,
        )
        .await;
        assert_eq!(rows, vec![vec![7], vec![4], vec![1]]);
    });
}

#[test]
fn real_predicates_preserve_fractional_and_large_integer_boundaries() {
    asupersync::test_utils::run_test(|| async {
        let connection = named_series().await;
        let rows = integer_rows(&connection,
            "SELECT value FROM seq WHERE start=-5 AND stop=5 AND value>-0.25 AND value<2.75 ORDER BY value", 1).await;
        assert_eq!(rows, vec![vec![0], vec![1], vec![2]]);
        assert!(
            connection
                .query("SELECT value FROM seq WHERE start=0 AND stop=5 AND value=2.5")
                .await
                .unwrap()
                .is_empty()
        );
        let rows = integer_rows(
            &connection,
            "SELECT value FROM seq WHERE start=9007199254740991 AND stop=9007199254740995 \
             AND value>9007199254740992.0 ORDER BY value",
            1,
        )
        .await;
        assert_eq!(
            rows,
            vec![
                vec![9_007_199_254_740_993],
                vec![9_007_199_254_740_994],
                vec![9_007_199_254_740_995]
            ]
        );
    });
}

#[test]
fn empty_and_failed_scans_do_not_contaminate_later_scans() {
    asupersync::test_utils::run_test(|| async {
        let connection = named_series().await;
        for sql in [
            "SELECT value FROM seq WHERE start=NULL AND stop=5",
            "SELECT value FROM seq WHERE start=1 AND stop=NULL",
            "SELECT value FROM seq WHERE start=1 AND stop=5 AND step=NULL",
            "SELECT value FROM seq WHERE start=1 AND stop=5 AND value>NULL",
            "SELECT value FROM generate_series(NULL,5)",
            "SELECT value FROM generate_series(1,NULL)",
            "SELECT value FROM generate_series(1,5,NULL)",
        ] {
            assert!(connection.query(sql).await.unwrap().is_empty(), "{sql}");
        }
        assert!(
            connection
                .query("SELECT value FROM seq WHERE stop=5")
                .await
                .is_err()
        );
        assert_eq!(
            integer_rows(
                &connection,
                "SELECT value FROM seq WHERE start=2 AND stop=3 ORDER BY value",
                1
            )
            .await,
            vec![vec![2], vec![3]]
        );
        assert_eq!(
            integer_rows(
                &connection,
                "SELECT value FROM generate_series(2,4) ORDER BY value",
                1
            )
            .await,
            vec![vec![2], vec![3], vec![4]]
        );
    });
}

#[test]
fn constrained_series_aggregates_and_limits_use_only_matching_rows() {
    asupersync::test_utils::run_test(|| async {
        let connection = named_series().await;
        assert_eq!(integer_rows(&connection,
            "SELECT count(*), sum(value) FROM seq WHERE start=1 AND stop=9 AND step=2 AND value>2 AND value<8", 2).await,
            vec![vec![3,15]]);
        assert_eq!(integer_rows(&connection,
            "SELECT value FROM seq WHERE start=1 AND stop=9 ORDER BY value DESC LIMIT 3 OFFSET 2", 1).await,
            vec![vec![7],vec![6],vec![5]]);
        assert_eq!(
            integer_rows(
                &connection,
                "SELECT value FROM generate_series(9,1,-2) ORDER BY value",
                1
            )
            .await,
            vec![vec![1], vec![3], vec![5], vec![7], vec![9]]
        );
    });
}
