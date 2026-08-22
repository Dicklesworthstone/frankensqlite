//! Differential oracle: aggregate-function edge cases vs rusqlite (bundled
//! SQLite 3.53). A probe sweep found this surface stock-correct across 18 cases;
//! this keeper locks it in.
//!
//! Covers count(*) vs count(col) vs count(DISTINCT), sum/total/avg NULL-skipping
//! and empty-set behaviour (sum/avg/min/max -> NULL, total -> 0.0), min/max
//! NULL-skipping and mixed storage-class ordering, group_concat (default and
//! custom separator, in-aggregate ORDER BY, DISTINCT, empty -> NULL), the FILTER
//! clause, grouped aggregates, and the result affinities (sum of mixed -> real,
//! avg always real).

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

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(
        fr, rr,
        "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

/// x with NULLs + duplicates across two groups.
const V: &[&str] = &[
    "CREATE TABLE v(x INT, g TEXT)",
    "INSERT INTO v VALUES (3,'a'),(1,'a'),(NULL,'a'),(2,'b'),(2,'b'),(NULL,'b')",
];

#[test]
fn count_sum_avg_null_semantics() {
    asupersync::test_utils::run_test(|| async {
        agree(
            V,
            "SELECT count(*), count(x), count(DISTINCT x) FROM v",
            "count variants",
        )
        .await;
        agree(V, "SELECT sum(x), total(x) FROM v", "sum/total skip NULL").await;
        agree(
            &["CREATE TABLE v(x INT)"],
            "SELECT sum(x), total(x), count(x) FROM v",
            "empty: sum NULL, total 0.0",
        )
        .await;
        agree(
            &[
                "CREATE TABLE v(x INT)",
                "INSERT INTO v VALUES (NULL),(NULL)",
            ],
            "SELECT sum(x), total(x) FROM v",
            "all-NULL: sum NULL, total 0.0",
        )
        .await;
        agree(V, "SELECT avg(x) FROM v", "avg skips NULL").await;
        agree(
            &["CREATE TABLE v(x INT)", "INSERT INTO v VALUES (2),(4)"],
            "SELECT avg(x), typeof(avg(x)) FROM v",
            "avg is always real",
        )
        .await;
    });
}

#[test]
fn min_max() {
    asupersync::test_utils::run_test(|| async {
        agree(V, "SELECT min(x), max(x) FROM v", "min/max skip NULL").await;
        agree(
            &["CREATE TABLE v(x INT)"],
            "SELECT min(x), max(x) FROM v",
            "min/max over empty are NULL",
        )
        .await;
        agree(
            &[
                "CREATE TABLE v(x)",
                "INSERT INTO v VALUES (5),('abc'),(2.5),(X'ff')",
            ],
            "SELECT quote(max(x)), quote(min(x)) FROM v",
            "min/max across storage classes",
        )
        .await;
    });
}

#[test]
fn group_concat_variants() {
    asupersync::test_utils::run_test(|| async {
        agree(
            V,
            "SELECT group_concat(x) FROM (SELECT x FROM v ORDER BY x)",
            "default separator, skips NULL",
        )
        .await;
        agree(
            V,
            "SELECT group_concat(x, '|') FROM (SELECT x FROM v ORDER BY x)",
            "custom separator",
        )
        .await;
        agree(
            V,
            "SELECT group_concat(x ORDER BY x DESC) FROM v",
            "in-aggregate ORDER BY",
        )
        .await;
        agree(
            V,
            "SELECT group_concat(DISTINCT x) FROM (SELECT x FROM v WHERE x IS NOT NULL ORDER BY x)",
            "DISTINCT",
        )
        .await;
        agree(
            &["CREATE TABLE v(x INT)"],
            "SELECT group_concat(x) FROM v",
            "empty -> NULL",
        )
        .await;
    });
}

#[test]
fn grouped_and_filter_and_affinity() {
    asupersync::test_utils::run_test(|| async {
        agree(
            V,
            "SELECT g, sum(x), count(x), count(*) FROM v GROUP BY g ORDER BY g",
            "grouped sum/count",
        )
        .await;
        agree(
            V,
            "SELECT g, count(DISTINCT x) FROM v GROUP BY g ORDER BY g",
            "grouped count(DISTINCT)",
        )
        .await;
        agree(
            V,
            "SELECT sum(x) FILTER (WHERE x > 1), count(*) FILTER (WHERE x IS NOT NULL) FROM v",
            "FILTER clause",
        )
        .await;
        agree(
            &["CREATE TABLE v(x)", "INSERT INTO v VALUES (1),(2.5),(3)"],
            "SELECT sum(x), typeof(sum(x)) FROM v",
            "sum of mixed -> real",
        )
        .await;
    });
}
