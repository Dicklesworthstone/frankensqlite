#![recursion_limit = "512"]
//! bd-insert-select-returning-param-77yt5: `INSERT ... SELECT ... RETURNING`
//! must number `?` placeholders GLOBALLY, including a `?` in the RETURNING clause.
//! The per-row replay (`build_insert_select_replay_sql`) rebuilds the statement as
//! `INSERT INTO t VALUES(?1,?2) RETURNING <returning>` and runs it with only the
//! inserted row's column values as params, so a RETURNING `?` (which refers to the
//! original statement's bind params) previously bound out of range. Param-bound
//! differential vs rusqlite (bundled SQLite).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn iv(n: i64) -> SqliteValue {
    SqliteValue::Integer(n)
}
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
async fn fq_p(f: &Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    match f.query_with_params(sql, params).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq_p(r: &rusqlite::Connection, sql: &str, vals: &[i64]) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map(rusqlite::params_from_iter(vals.iter().copied()), |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}
/// Run the same param-bound statement on both engines and assert the projected
/// rows agree.
async fn agree_p(setup: &[&str], sql: &str, fparams: &[SqliteValue], rvals: &[i64], msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        r.execute(s, []).unwrap();
    }
    let fr = fq_p(&f, sql, fparams).await;
    let rr = rq_p(&r, sql, rvals);
    assert_eq!(fr, rr, "{msg}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

#[test]
fn insert_select_returning_param_numbers_globally() {
    asupersync::test_utils::run_test(|| async {
        // SELECT ?=5, ?=6 (source), RETURNING id, a, ?=7. The RETURNING ? is param#3,
        // not a per-scope #1 (which previously bound out of range in the replay).
        agree_p(
            &["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)"],
            "INSERT INTO t(id, a) SELECT ?, ? RETURNING id, a, ?",
            &[iv(5), iv(6), iv(7)],
            &[5, 6, 7],
            "INSERT...SELECT RETURNING ? must be param#3",
        )
        .await;
    });
}

#[test]
fn insert_select_returning_no_param_unaffected() {
    asupersync::test_utils::run_test(|| async {
        agree_p(
            &["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)"],
            "INSERT INTO t(id, a) SELECT ?, ? RETURNING id, a",
            &[iv(8), iv(9)],
            &[8, 9],
            "INSERT...SELECT RETURNING without ? unaffected",
        )
        .await;
    });
}

#[test]
fn insert_select_source_subquery_and_returning_param() {
    asupersync::test_utils::run_test(|| async {
        // (SELECT ?) in the SELECT source consumes a slot; RETURNING ? = param#3.
        agree_p(
            &["CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)"],
            "INSERT INTO t(id, a) SELECT ?, (SELECT ?) RETURNING a, ?",
            &[iv(2), iv(30), iv(77)],
            &[2, 30, 77],
            "INSERT...SELECT source subquery ? + RETURNING ? global numbering",
        )
        .await;
    });
}

#[test]
fn insert_select_multirow_source_returning_param() {
    asupersync::test_utils::run_test(|| async {
        // Multi-row source table; WHERE y > ?=0 (param#1), RETURNING ?=99 (param#2)
        // projected per inserted row.
        agree_p(
            &[
                "CREATE TABLE src(x INTEGER, y INTEGER)",
                "INSERT INTO src VALUES (10, 1), (20, 2)",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
            ],
            "INSERT INTO t(id, a) SELECT x, y FROM src WHERE y > ? RETURNING id, a, ?",
            &[iv(0), iv(99)],
            &[0, 99],
            "INSERT...SELECT multi-row source with RETURNING ?",
        )
        .await;
    });
}
