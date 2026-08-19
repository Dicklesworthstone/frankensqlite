#![recursion_limit = "512"]

//! bd-bzd19 L8 + row-value probes — HAVING IN-list bare/aggregate columns and
//! row-value nested subqueries, differential vs rusqlite (bundled SQLite).

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
    "CREATE TABLE t(g INT, v INT)",
    "INSERT INTO t VALUES (1,10),(1,20),(2,5),(3,7),(3,8)",
];

#[test]
fn having_bare_column_in_list() {
    asupersync::test_utils::run_test(|| async {
        agree(
            T,
            "SELECT g, count(*) FROM t GROUP BY g HAVING g IN (1,3) ORDER BY g",
            "L8: bare column in HAVING IN-list",
        )
        .await;
    });
}

#[test]
fn having_aggregate_in_list() {
    asupersync::test_utils::run_test(|| async {
        agree(
            T,
            "SELECT g, count(*) c FROM t GROUP BY g HAVING count(*) IN (2) ORDER BY g",
            "L8: aggregate in HAVING IN-list",
        )
        .await;
    });
}

#[test]
fn having_bare_column_not_in_list() {
    asupersync::test_utils::run_test(|| async {
        agree(
            T,
            "SELECT g, count(*) FROM t GROUP BY g HAVING g NOT IN (2) ORDER BY g",
            "L8: bare column NOT IN HAVING IN-list",
        )
        .await;
    });
}

#[test]
fn having_bare_column_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        agree(
            T,
            "SELECT g FROM t GROUP BY g HAVING g IN (SELECT v FROM t WHERE v < 8) ORDER BY g",
            "L8: bare column IN (subquery) in HAVING",
        )
        .await;
    });
}

#[test]
fn having_mixed_agg_and_bare() {
    asupersync::test_utils::run_test(|| async {
        agree(
            T,
            "SELECT g, sum(v) s FROM t GROUP BY g HAVING g IN (1,2,3) AND sum(v) > 10 ORDER BY g",
            "L8: HAVING mixes a bare-column IN-list with an aggregate predicate",
        )
        .await;
    });
}

#[test]
fn row_value_in_nested_subquery() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE a(x INT, y INT)",
                "CREATE TABLE b(x INT, y INT)",
                "INSERT INTO a VALUES (1,2),(3,4),(5,6)",
                "INSERT INTO b VALUES (1,2),(9,9)",
            ],
            "SELECT x, y FROM a WHERE (x, y) IN (SELECT x, y FROM b) ORDER BY x",
            "row-value IN (subquery)",
        )
        .await;
    });
}
