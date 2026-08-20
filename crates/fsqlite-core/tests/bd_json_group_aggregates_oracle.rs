//! bd-json-group-aggregates-pdvx5: JSON1 aggregate functions `json_group_array`
//! and `json_group_object` were declared in the extension's doc comment as
//! provided but never actually registered (`register_json_scalars` wired only
//! scalars), so any query using them failed with
//! `no such function: json_group_array`. This keeper is a differential oracle
//! against rusqlite (bundled SQLite 3.53): it asserts frank now produces the
//! same JSON aggregate text stock SQLite does.
//!
//! Ordering note: a bare aggregate over a table scan visits rows in an
//! unspecified order. To keep the comparison deterministic on both engines we
//! feed the aggregate from an `ORDER BY` subquery — the documented SQLite idiom
//! for imposing a stable order on `json_group_array`/`json_group_object`.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
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
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Set up identical schema on both engines, run one SELECT, assert agreement.
async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        r.execute(s, []).unwrap();
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(fr, rr, "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

#[test]
fn json_group_array_integers() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x INTEGER)", "INSERT INTO t VALUES (1),(2),(3)"],
            "SELECT json_group_array(x) FROM (SELECT x FROM t ORDER BY x)",
            "json_group_array over integers must equal [1,2,3]",
        )
        .await;
    });
}

#[test]
fn json_group_array_text() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('a'),('b'),('c')"],
            "SELECT json_group_array(x) FROM (SELECT x FROM t ORDER BY x)",
            "json_group_array over text must JSON-quote each element",
        )
        .await;
    });
}

#[test]
fn json_group_array_with_null() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(id INTEGER, x INTEGER)", "INSERT INTO t VALUES (1,1),(2,NULL),(3,3)"],
            "SELECT json_group_array(x) FROM (SELECT id, x FROM t ORDER BY id)",
            "json_group_array must emit JSON null for SQL NULL",
        )
        .await;
    });
}

#[test]
fn json_group_object_text_keys() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(k TEXT, v INTEGER)", "INSERT INTO t VALUES ('a',1),('b',2)"],
            "SELECT json_group_object(k, v) FROM (SELECT k, v FROM t ORDER BY k)",
            "json_group_object must build {\"a\":1,\"b\":2}",
        )
        .await;
    });
}

#[test]
fn json_group_object_text_values() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(k TEXT, v TEXT)", "INSERT INTO t VALUES ('x','hi'),('y','bye')"],
            "SELECT json_group_object(k, v) FROM (SELECT k, v FROM t ORDER BY k)",
            "json_group_object must JSON-quote string values",
        )
        .await;
    });
}

#[test]
fn json_group_array_grouped() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(g INTEGER, x INTEGER)",
                "INSERT INTO t VALUES (1,10),(1,11),(2,20),(2,21),(2,22)",
            ],
            "SELECT g, json_group_array(x) FROM (SELECT g, x FROM t ORDER BY g, x) GROUP BY g ORDER BY g",
            "GROUP BY json_group_array must produce one array per group",
        )
        .await;
    });
}

#[test]
fn json_group_array_empty_set() {
    asupersync::test_utils::run_test(|| async {
        // Aggregate over zero rows: stock returns a single NULL row. Whatever
        // stock does, frank must match (self-verifying via the differential).
        agree(
            &["CREATE TABLE t(x INTEGER)"],
            "SELECT json_group_array(x) FROM t",
            "json_group_array over empty set must match stock",
        )
        .await;
    });
}

#[test]
fn json_group_array_result_is_valid_json() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x INTEGER)", "INSERT INTO t VALUES (1),(2),(3)"],
            "SELECT json_valid(json_group_array(x)) FROM t",
            "json_group_array output must itself be valid JSON",
        )
        .await;
    });
}

// The two cases below deliberately omit the ORDER BY subquery so the aggregate
// folds a bare table scan. That form takes the interpreted group-aggregate path
// (not the VDBE lowering the subquery cases exercise), so these guard the
// `is_current_aggregate_fn` classifier being JSON-aware: without it the query
// is (mis)planned as a per-row scalar scan and returns one NULL per row instead
// of a single folded array/object. Row visit order is rowid order on both
// engines for a single heap scan, so the comparison stays deterministic.

#[test]
fn json_group_array_bare_scan_interpreted_path() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x INTEGER)", "INSERT INTO t VALUES (1),(2),(3)"],
            "SELECT json_group_array(x) FROM t",
            "bare json_group_array must fold to one row (interpreted path)",
        )
        .await;
    });
}

#[test]
fn json_group_object_bare_scan_interpreted_path() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(k TEXT, v INTEGER)", "INSERT INTO t VALUES ('a',1),('b',2)"],
            "SELECT json_group_object(k, v) FROM t",
            "bare json_group_object must fold to one row (interpreted path)",
        )
        .await;
    });
}
