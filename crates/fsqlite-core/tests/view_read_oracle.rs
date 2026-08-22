//! Differential oracle: read-side view semantics vs rusqlite (bundled SQLite
//! 3.53). A probe sweep found this surface stock-correct across 14 cases; this
//! keeper locks it in. (INSTEAD OF view DML is covered elsewhere.)
//!
//! Covers simple projection views, aliased/expression columns, a filtering
//! view, an aggregate (GROUP BY) view, a UNION view, a nested view (view over a
//! view), a joined view, a CASE-expression view, a view whose column is a
//! correlated subquery, outer WHERE push-down / ORDER BY+LIMIT / aggregate over
//! a view, joining a view to a table, and re-aggregating an aggregate view.

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

/// Base data + a battery of views, set up on both engines.
fn setup() -> Vec<&'static str> {
    vec![
        "CREATE TABLE t(id INT, g TEXT, v INT)",
        "INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',5),(5,'c',NULL)",
        "CREATE TABLE u(id INT, label TEXT)",
        "INSERT INTO u VALUES (1,'one'),(3,'three')",
        "CREATE VIEW vsimple AS SELECT id, v FROM t",
        "CREATE VIEW valias AS SELECT id AS k, v*2 AS doubled FROM t",
        "CREATE VIEW vagg AS SELECT g, sum(v) AS total, count(*) AS n FROM t GROUP BY g",
        "CREATE VIEW vfilter AS SELECT id, v FROM t WHERE v > 10",
        "CREATE VIEW vunion AS SELECT id FROM t WHERE g='a' UNION SELECT id FROM t WHERE v > 25",
        "CREATE VIEW vnested AS SELECT k, doubled FROM valias WHERE doubled > 20",
        "CREATE VIEW vjoin AS SELECT t.id, t.v, u.label FROM t JOIN u ON u.id=t.id",
        "CREATE VIEW vexpr AS SELECT id, CASE WHEN v IS NULL THEN 'none' WHEN v>=20 THEN 'hi' ELSE 'lo' END AS bucket FROM t",
        "CREATE VIEW vsub AS SELECT id, (SELECT count(*) FROM t t2 WHERE t2.v > t.v) AS higher FROM t",
    ]
}

async fn agree(sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup() {
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

#[test]
fn projection_alias_filter() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT id, v FROM vsimple ORDER BY id", "simple view").await;
        agree(
            "SELECT k, doubled FROM valias ORDER BY k",
            "aliased + expression columns",
        )
        .await;
        agree("SELECT id, v FROM vfilter ORDER BY id", "filtering view").await;
        agree(
            "SELECT id, bucket FROM vexpr ORDER BY id",
            "CASE-expression view",
        )
        .await;
    });
}

#[test]
fn aggregate_and_union_views() {
    asupersync::test_utils::run_test(|| async {
        agree("SELECT g, total, n FROM vagg ORDER BY g", "aggregate view").await;
        agree("SELECT id FROM vunion ORDER BY id", "UNION view").await;
        agree(
            "SELECT count(*) AS groups, sum(total) AS grand FROM vagg",
            "re-aggregate an aggregate view",
        )
        .await;
    });
}

#[test]
fn outer_clauses_over_view() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT id FROM vsimple WHERE v >= 20 ORDER BY id",
            "outer WHERE push-down",
        )
        .await;
        agree(
            "SELECT id, v FROM vsimple ORDER BY v DESC LIMIT 2",
            "outer ORDER BY + LIMIT",
        )
        .await;
        agree(
            "SELECT count(*), max(v) FROM vsimple",
            "aggregate over a view",
        )
        .await;
    });
}

#[test]
fn nested_joined_subquery_views() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT k, doubled FROM vnested ORDER BY k",
            "nested view over a view",
        )
        .await;
        agree("SELECT id, v, label FROM vjoin ORDER BY id", "joined view").await;
        agree(
            "SELECT id, higher FROM vsub ORDER BY id",
            "view with a correlated-subquery column",
        )
        .await;
        agree("SELECT vsimple.id, vsimple.v, u.label FROM vsimple JOIN u ON u.id=vsimple.id ORDER BY vsimple.id", "join a view to a table").await;
    });
}
