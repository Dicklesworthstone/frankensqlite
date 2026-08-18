#![recursion_limit = "512"]

//! bd-uobkz: the FROM-less HAVING evaluator, the last AND/OR short-circuit gap
//! of the dkswh umbrella. Two facets, both differential vs rusqlite 3.46.1:
//!
//!  1. `SELECT 1 GROUP BY 1 HAVING (0 AND E)` — stock folds the exact-integer-0
//!     AND to FALSE at parse time and NEVER evaluates the (dead) subquery E, so
//!     no group passes and the result is empty. frank previously pre-inlined E
//!     via inline_subqueries_in_expr (no fold) and surfaced E's error.
//!  2. `SELECT 1 HAVING E` (FROM-less, no GROUP BY, no aggregate) — stock
//!     rejects at prepare with "HAVING clause on a non-aggregate query". frank
//!     previously either errored "HAVING is not supported in this connection
//!     path" (no-subquery) or silently ignored the HAVING and returned the row
//!     (subquery path).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
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
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

/// Both engines return the same rows for `sql`.
async fn assert_rows_agree(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let fr: Vec<Vec<String>> = f
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut st = r
        .prepare(sql)
        .unwrap_or_else(|e| panic!("rusqlite prepare `{sql}`: {e:?}"));
    let n = st.column_count();
    let rr: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(fr, rr, "row divergence on `{sql}`");
}

/// Both engines REJECT `sql`; frank's message contains `frank_needle`.
async fn assert_both_reject(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
    frank_needle: &str,
) {
    let fe = f
        .query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("frank accepted `{sql}` but must reject"));
    let msg = format!("{fe:?}").to_ascii_lowercase();
    assert!(
        msg.contains(frank_needle),
        "frank `{sql}`: expected error containing `{frank_needle}`, got {fe:?}"
    );
    assert!(r.prepare(sql).is_err(), "rusqlite must also reject `{sql}`");
}

/// Facet 1: grouped FROM-less HAVING folds `0 AND E` to FALSE without touching E.
#[test]
fn grouped_having_zero_and_subquery_folds_gh_uobkz() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // `json_each('bare')` errors if ever evaluated; the fold must prevent it.
        assert_rows_agree(
            &f,
            &r,
            "SELECT 1 GROUP BY 1 HAVING (0 AND (SELECT count(*) FROM json_each('bare')))",
        )
        .await;
        // A live (non-folded) grouped HAVING still evaluates normally.
        assert_rows_agree(&f, &r, "SELECT 1 GROUP BY 1 HAVING (1 AND 1)").await;
        assert_rows_agree(&f, &r, "SELECT 1 GROUP BY 1 HAVING (0 OR 1)").await;
    });
}

/// Facet 2: FROM-less HAVING with no GROUP BY / no aggregate is rejected.
#[test]
fn fromless_having_non_aggregate_rejected_gh_uobkz() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // No subquery (previously errored "not supported in this connection path").
        assert_both_reject(&f, &r, "SELECT 1 HAVING (0 AND 1)", "non-aggregate").await;
        // With subquery (previously silently returned the row, ignoring HAVING).
        assert_both_reject(
            &f,
            &r,
            "SELECT 1 HAVING (0 AND (SELECT count(*) FROM json_each('bare')))",
            "non-aggregate",
        )
        .await;
        // A plain non-zero HAVING with no aggregate is equally illegal.
        assert_both_reject(&f, &r, "SELECT 1 HAVING 1", "non-aggregate").await;
    });
}

/// Control: an implicit-aggregate HAVING remains valid.
#[test]
fn implicit_aggregate_having_still_valid_gh_uobkz() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        assert_rows_agree(&f, &r, "SELECT count(*) HAVING count(*) > 0").await;
        assert_rows_agree(&f, &r, "SELECT count(*) HAVING count(*) > 5").await;
    });
}
