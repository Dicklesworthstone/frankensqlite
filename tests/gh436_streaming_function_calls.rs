//! GH#436: function calls in the streaming table-function aggregate lane are
//! resolved once per query. Results must match stock SQLite, and a function
//! the connection registers must still override the builtin of that name.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_error::Result;
use fsqlite_func::ScalarFunction;
use fsqlite_types::value::SqliteValue;

/// Replaces the builtin `abs(X)`: always 42.
struct FortyTwoAbs;

impl ScalarFunction for FortyTwoAbs {
    fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
        Ok(SqliteValue::Integer(42))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "abs"
    }
}

async fn rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

#[test]
fn streaming_aggregate_function_calls_match_sqlite_and_honor_overrides() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        // Expected values from sqlite3 over the same 1..1000 series.
        assert_eq!(
            rows(
                &conn,
                "SELECT sum(round(log10(value))), sum(abs(-value)) FROM generate_series(1, 1000)"
            )
            .await,
            vec![vec![SqliteValue::Float(2650.0), SqliteValue::Integer(500_500)]]
        );
        assert_eq!(
            rows(
                &conn,
                "SELECT round(log10(value)) AS k, sum(value) FROM generate_series(1, 1000) \
                 GROUP BY k ORDER BY k"
            )
            .await,
            vec![
                vec![SqliteValue::Float(0.0), SqliteValue::Integer(6)],
                vec![SqliteValue::Float(1.0), SqliteValue::Integer(490)],
                vec![SqliteValue::Float(2.0), SqliteValue::Integer(49_590)],
                vec![SqliteValue::Float(3.0), SqliteValue::Integer(450_414)],
            ]
        );
        // SQLite's N == 0 rounding and int64 N clamp.
        assert_eq!(
            rows(
                &conn,
                "SELECT round(0.49999999999999994), round(1.23456, 4294967298), \
                 round(1.23456, -4294967295)"
            )
            .await,
            vec![vec![
                SqliteValue::Float(1.0),
                SqliteValue::Float(1.23456),
                SqliteValue::Float(1.0),
            ]]
        );
        // A registered function overrides the builtin inside the lane too.
        conn.register_deterministic_scalar_function(FortyTwoAbs);
        assert_eq!(
            rows(&conn, "SELECT sum(abs(value)) FROM generate_series(1, 10)").await,
            vec![vec![SqliteValue::Integer(420)]]
        );
        assert_eq!(
            rows(
                &conn,
                "SELECT abs(value) AS k, count(*) FROM generate_series(1, 10) GROUP BY k"
            )
            .await,
            vec![vec![SqliteValue::Integer(42), SqliteValue::Integer(10)]]
        );
        conn.close().await.expect("close");
    });
}
