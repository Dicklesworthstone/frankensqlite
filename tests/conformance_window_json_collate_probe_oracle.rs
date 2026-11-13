#![recursion_limit = "512"]

//! Differential conformance probe vs rusqlite across previously-unprobed
//! surfaces: window frames (ROWS/RANGE), named windows, window functions
//! (ntile/lag/lead/nth_value/first_value), JSON -> / ->> operators, and COLLATE
//! in comparison/IN contexts. Lands as a regression keeper; any RED is filed.

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
    match f.query(sql).await {
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
async fn agree(setup: &[&str], query: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, query).await;
    let rr = rq(&r, query);
    assert_eq!(fr, rr, "{msg}\n  frank={fr:?}\n  stock={rr:?}");
}

const T: &[&str] = &[
    "CREATE TABLE t(id INT, g INT, v INT)",
    "INSERT INTO t VALUES (1,1,10),(2,1,20),(3,1,30),(4,2,40),(5,2,50)",
];

#[test]
fn window_rows_frame_sliding() {
    asupersync::test_utils::run_test(|| async {
        agree(T, "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY id",
            "window ROWS 1 PRECEDING..1 FOLLOWING").await;
    });
}

#[test]
fn window_range_unbounded() {
    asupersync::test_utils::run_test(|| async {
        agree(T, "SELECT id, avg(v) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t ORDER BY id",
            "window RANGE UNBOUNDED..CURRENT").await;
    });
}

#[test]
fn window_partitioned() {
    asupersync::test_utils::run_test(|| async {
        agree(
            T,
            "SELECT id, g, sum(v) OVER (PARTITION BY g ORDER BY id) FROM t ORDER BY id",
            "window PARTITION BY g running sum",
        )
        .await;
    });
}

#[test]
fn named_window() {
    asupersync::test_utils::run_test(|| async {
        agree(T, "SELECT id, count(*) OVER w, max(v) OVER w FROM t WINDOW w AS (ORDER BY id) ORDER BY id",
            "named window shared by two aggregates").await;
    });
}

#[test]
fn window_ntile_lag_lead_nthvalue() {
    asupersync::test_utils::run_test(|| async {
        agree(
            T,
            "SELECT id, ntile(2) OVER (ORDER BY id), lag(v,2,-1) OVER (ORDER BY id), \
                    lead(v) OVER (ORDER BY id), nth_value(v,3) OVER (ORDER BY id), \
                    first_value(v) OVER (ORDER BY id) FROM t ORDER BY id",
            "ntile/lag/lead/nth_value/first_value",
        )
        .await;
    });
}

#[test]
fn json_arrow_operators() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT '{\"a\":{\"b\":5}}' ->> '$.a.b', '[1,2,3]' ->> 1, \
                    json('{\"a\":{\"b\":5}}') -> '$.a' ->> '$.b', '{\"x\":\"hi\"}' ->> '$.x'",
            "JSON -> and ->> operators (chained/path/index)",
        )
        .await;
    });
}

#[test]
fn json_each_and_functions() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT (SELECT json_group_array(value) FROM json_each('[1,2,3]')), \
                    json_array_length('[1,2,3,4]'), \
                    (SELECT sum(value) FROM json_each('[10,20,30]'))",
            "json_group_array / json_array_length / json_each sum",
        )
        .await;
    });
}

#[test]
fn collate_nocase_contexts() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE c(s TEXT COLLATE NOCASE)",
                "INSERT INTO c VALUES ('Apple'),('BANANA'),('cherry')",
            ],
            "SELECT s FROM c WHERE s IN ('apple','CHERRY') ORDER BY s",
            "column COLLATE NOCASE drives IN + ORDER BY",
        )
        .await;
    });
}

#[test]
fn collate_expression_forms() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT 'ABC'='abc' COLLATE NOCASE, 'x' COLLATE NOCASE IN ('X','y'), \
                    'straße' = 'STRASSE' COLLATE NOCASE",
            "COLLATE in equality / IN / non-ASCII",
        )
        .await;
    });
}
