#![recursion_limit = "512"]

//! GH #529: DISTINCT aggregates over a table-valued FROM source
//! (`generate_series`, `json_each`) run on the core's materialized aggregate
//! fallback, which checked every argument against all earlier distinct ones.
//! The cost was quadratic: `sum(DISTINCT round(value/1.5))` over 50,000 rows
//! took 2.8 s, and doubling the rows quadrupled the time. Every DISTINCT shape
//! of that fallback now deduplicates by sorting, keeping first occurrences in
//! scan order.
//!
//! The oracle is rusqlite's bundled SQLite, which has `json_each` but not
//! `generate_series`. The stock comparisons therefore use `json_each`, and the
//! growth bound uses `generate_series`, whose answers are closed-form.

use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_error::Result;
use fsqlite_func::AggregateFunction;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}r"),
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
        rusqlite::types::Value::Real(f) => format!("{f}r"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut st = rconn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock rejects `{sql}`: {e}"));
    let n = st.column_count();
    let expected: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let actual: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect();
    assert_eq!(actual, expected, "DISTINCT aggregate mismatch on `{sql}`");
}

/// Equal numbers of both storage classes, collation-equal text, and NULLs, in
/// an order where first occurrence and sorted order differ.
const MIXED: &str = r#"'["b", 1, "a", "A", 1.0, "a ", "1", null, 2, "B", "b", null, 2.5, 1, "A", 2.5, "a"]'"#;
const NUMBERS: &str = "'[3, 1, 2.0, 2, 3.5, 1, null, 7, 3.5, null, 3, 0, -4, -4.0]'";

#[test]
fn distinct_aggregates_over_table_functions_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let queries = [
            // Built-in aggregates, BINARY.
            format!(
                "SELECT count(DISTINCT value), sum(DISTINCT value), total(DISTINCT value), \
                 avg(DISTINCT value), min(DISTINCT value), max(DISTINCT value) \
                 FROM json_each({NUMBERS})"
            ),
            format!("SELECT count(DISTINCT value), group_concat(DISTINCT value) FROM json_each({MIXED})"),
            format!("SELECT json_group_array(DISTINCT value) FROM json_each({MIXED})"),
            // Collated arguments.
            format!(
                "SELECT count(DISTINCT value COLLATE NOCASE), \
                 group_concat(DISTINCT value COLLATE NOCASE) FROM json_each({MIXED})"
            ),
            format!(
                "SELECT count(DISTINCT value COLLATE RTRIM), \
                 group_concat(DISTINCT value COLLATE RTRIM) FROM json_each({MIXED})"
            ),
            format!("SELECT json_group_array(DISTINCT value COLLATE NOCASE) FROM json_each({MIXED})"),
            // In-aggregate ORDER BY, order-sensitive and not.
            format!("SELECT group_concat(DISTINCT value ORDER BY value DESC) FROM json_each({MIXED})"),
            format!(
                "SELECT group_concat(DISTINCT value COLLATE NOCASE ORDER BY value) \
                 FROM json_each({MIXED})"
            ),
            format!("SELECT sum(DISTINCT value ORDER BY value), count(DISTINCT value ORDER BY key DESC) FROM json_each({NUMBERS})"),
            // FILTER, then GROUP BY.
            format!(
                "SELECT count(DISTINCT value) FILTER (WHERE key % 2 = 0), \
                 group_concat(DISTINCT value) FILTER (WHERE key > 3) FROM json_each({MIXED})"
            ),
            format!(
                "SELECT type, count(DISTINCT value), group_concat(DISTINCT value), \
                 json_group_array(DISTINCT value) FROM json_each({MIXED}) GROUP BY type ORDER BY type"
            ),
            // Two table functions joined, with and without USING.
            format!(
                "SELECT count(DISTINCT a.value), group_concat(DISTINCT a.value), \
                 group_concat(DISTINCT a.value ORDER BY a.value) \
                 FROM json_each({MIXED}) AS a JOIN json_each('[1, 2]') AS b ON b.value = 1"
            ),
            format!(
                "SELECT a.type, count(DISTINCT a.value), sum(DISTINCT a.value ORDER BY a.key) \
                 FROM json_each({NUMBERS}) AS a JOIN json_each('[1, 2]') AS b USING (type) \
                 GROUP BY a.type ORDER BY a.type"
            ),
        ];
        for sql in &queries {
            assert_agree(&f, &r, sql).await;
        }
    });
}

/// Appends each value it is stepped with, so the argument rows a DISTINCT
/// application aggregate receives, and their order, are visible.
struct StepOrder;

impl AggregateFunction for StepOrder {
    type State = Vec<String>;

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> Result<()> {
        state.push(match &args[0] {
            SqliteValue::Null => "NULL".to_owned(),
            SqliteValue::Text(text) => format!("'{text}'"),
            value => value.to_text(),
        });
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> Result<SqliteValue> {
        Ok(SqliteValue::Text(state.join("|").into()))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "step_order"
    }
}

#[test]
fn distinct_application_aggregate_steps_first_occurrences_in_scan_order() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        f.register_aggregate_function(StepOrder);
        let step_order = |sql: String| {
            let f = &f;
            async move {
                let rows = f.query(&sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
                tag_f(&rows[0].values()[0])
            }
        };
        // 1.0 equals the earlier 1; the text '1' does not. NULLs form one class.
        assert_eq!(
            step_order(format!("SELECT step_order(DISTINCT value) FROM json_each({MIXED})")).await,
            "''b'|1|'a'|'A'|'a '|'1'|NULL|2|'B'|2.5'"
        );
        assert_eq!(
            step_order(format!(
                "SELECT step_order(DISTINCT value COLLATE NOCASE) FROM json_each({MIXED})"
            ))
            .await,
            "''b'|1|'a'|'a '|'1'|NULL|2|2.5'"
        );
        // The step order follows the in-aggregate ORDER BY; DISTINCT keeps
        // the first row of each class in scan order (2.0, not the later 2),
        // as stock's group_concat(DISTINCT value ORDER BY key DESC) does.
        assert_eq!(
            step_order(format!(
                "SELECT step_order(DISTINCT value ORDER BY key DESC) FROM json_each({NUMBERS})"
            ))
            .await,
            "'-4|0|7|NULL|3.5|2.0|1|3'"
        );
    });
}

fn digits(n: u64) -> u64 {
    n.checked_ilog10().map_or(1, |log| u64::from(log) + 1)
}

/// The DISTINCT aggregates of `generate_series(1, n)` and their closed forms.
async fn timed_distinct_aggregates(conn: &Connection, n: u64) -> Duration {
    let sql = format!(
        "SELECT sum(DISTINCT value), count(DISTINCT value % 1000), \
         length(group_concat(DISTINCT value)), length(json_group_array(DISTINCT value)), \
         count(DISTINCT CAST(value AS TEXT) COLLATE NOCASE), \
         length(group_concat(DISTINCT value ORDER BY value DESC)) \
         FROM generate_series(1, {n})"
    );
    let concat_len = (1..=n).map(digits).sum::<u64>() + n - 1;
    let expected = [
        n * (n + 1) / 2,
        n.min(1000),
        concat_len,
        concat_len + 2,
        n,
        concat_len,
    ]
    .map(|v| SqliteValue::Integer(i64::try_from(v).unwrap()).to_text());
    let started = Instant::now();
    let rows = conn.query(&sql).await.unwrap();
    let elapsed = started.elapsed();
    let actual: Vec<String> = rows[0].values().iter().map(SqliteValue::to_text).collect();
    assert_eq!(actual, expected, "n = {n}");
    elapsed
}

#[test]
fn distinct_aggregate_cost_grows_with_input_not_its_square() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        // 8x the rows: about 8x the time when deduplication sorts, 64x when it
        // scans every earlier value. Best of three absorbs scheduler noise.
        let (small, large) = (5_000, 40_000);
        let mut best = [Duration::MAX; 2];
        for _ in 0..3 {
            for (slot, n) in [small, large].into_iter().enumerate() {
                best[slot] = best[slot].min(timed_distinct_aggregates(&conn, n).await);
            }
        }
        let ratio = best[1].as_secs_f64() / best[0].as_secs_f64().max(1e-6);
        eprintln!(
            "GH529 best n={small}: {:?}, n={large}: {:?}, ratio {ratio:.1}",
            best[0], best[1]
        );
        assert!(
            ratio < 24.0,
            "8x the rows cost {ratio:.1}x the time ({:?} -> {:?}): DISTINCT deduplication is \
             superlinear",
            best[0],
            best[1]
        );
    });
}
