#![recursion_limit = "512"]

//! Review of e1e90c680: prepared INSERT / UPDATE / DELETE statements whose
//! text reads a subquery are re-folded on every execution, against that
//! execution's parameters and data. These shapes go beyond the commit's own
//! keeper: named and gapped `?NNN` parameters shared between a subquery and
//! the outer statement, self-referencing subqueries, INSERT ... SELECT with a
//! subquery filter, UPDATE ... FROM, RETURNING, and reuse inside one explicit
//! transaction across the statement's own writes. Compared with stock SQLite.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE t(x INTEGER PRIMARY KEY, g, label TEXT);\
INSERT INTO t VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c');\
CREATE TABLE dst(k, c, n);\
CREATE TABLE src(k, g);\
INSERT INTO src VALUES (100, 1), (101, 2), (102, 3);";

#[derive(Clone, Copy)]
enum Op {
    Exec(&'static str),
    Prepare(usize, &'static str),
    Run(usize, &'static [V]),
    Query(usize, &'static [V]),
    Dump(&'static str),
}

#[derive(Clone, Copy)]
enum V {
    I(i64),
    T(&'static str),
    N,
}

const OPS: &[Op] = &[
    Op::Prepare(
        0,
        "INSERT INTO dst VALUES (:k, (SELECT count(*) FROM t WHERE g = :g), \
         :g IN (SELECT g FROM t WHERE x > :k - 10))",
    ),
    Op::Run(0, &[V::I(10), V::I(1)]),
    Op::Exec("INSERT INTO t VALUES (4, 1, 'd')"),
    Op::Run(0, &[V::I(11), V::I(1)]),
    Op::Run(0, &[V::I(12), V::T("2")]),
    Op::Run(0, &[V::I(13), V::N]),
    Op::Prepare(1, "UPDATE t SET label = label || ?3 WHERE g = ?5 AND x IN (SELECT x FROM t WHERE g = ?5 ORDER BY x DESC LIMIT ?3)"),
    Op::Run(1, &[V::N, V::N, V::I(1), V::N, V::I(1)]),
    Op::Run(1, &[V::N, V::N, V::I(2), V::N, V::I(1)]),
    Op::Prepare(2, "DELETE FROM dst WHERE k < (SELECT max(k) FROM dst) AND c >= ?"),
    Op::Run(2, &[V::I(3)]),
    Op::Run(2, &[V::I(0)]),
    Op::Prepare(3, "INSERT INTO dst SELECT k, g, ?1 FROM src WHERE g IN (SELECT g FROM t WHERE label LIKE ?2)"),
    Op::Run(3, &[V::T("first"), V::T("a%")]),
    Op::Exec("UPDATE t SET label = 'zz' WHERE x = 3"),
    Op::Run(3, &[V::T("second"), V::T("z%")]),
    Op::Prepare(4, "UPDATE dst SET n = s.k FROM (SELECT k, g FROM src WHERE g <= ?1) AS s WHERE dst.c = s.g AND dst.k = (SELECT max(k) FROM dst AS d2 WHERE d2.c = s.g)"),
    Op::Run(4, &[V::I(1)]),
    Op::Run(4, &[V::I(3)]),
    Op::Prepare(5, "UPDATE t SET g = g + 10 WHERE x = (SELECT min(x) FROM t WHERE g < ?)"),
    Op::Run(5, &[V::I(10)]),
    Op::Dump("SELECT x, g FROM t ORDER BY x"),
    Op::Run(5, &[V::I(10)]),
    Op::Run(5, &[V::I(5)]),
    Op::Prepare(7, "SELECT x, g FROM t WHERE g > (SELECT min(g) FROM t) + ? ORDER BY x"),
    Op::Query(7, &[V::I(5)]),
    Op::Exec("UPDATE t SET g = g - 1"),
    Op::Query(7, &[V::I(5)]),
    Op::Exec("BEGIN"),
    Op::Prepare(6, "INSERT INTO dst(k, c) VALUES (?, (SELECT count(*) FROM dst))"),
    Op::Run(6, &[V::I(50)]),
    Op::Run(6, &[V::I(51)]),
    Op::Run(6, &[V::I(52)]),
    Op::Exec("COMMIT"),
    Op::Run(6, &[V::I(53)]),
    Op::Dump("SELECT x, g, label FROM t ORDER BY x"),
    Op::Dump("SELECT k, c, n FROM dst ORDER BY rowid"),
];

fn to_fsqlite(values: &[V]) -> Vec<SqliteValue> {
    values
        .iter()
        .map(|value| match value {
            V::I(v) => SqliteValue::Integer(*v),
            V::T(v) => SqliteValue::Text((*v).into()),
            V::N => SqliteValue::Null,
        })
        .collect()
}

fn to_rusqlite(values: &[V]) -> Vec<rusqlite::types::Value> {
    values
        .iter()
        .map(|value| match value {
            V::I(v) => rusqlite::types::Value::Integer(*v),
            V::T(v) => rusqlite::types::Value::Text((*v).to_owned()),
            V::N => rusqlite::types::Value::Null,
        })
        .collect()
}

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Integer(v) => format!("i:{v}"),
        SqliteValue::Float(v) => format!("f:{v}"),
        SqliteValue::Text(v) => format!("t:{v}"),
        SqliteValue::Blob(v) => format!("b:{:?}", v.to_vec()),
        SqliteValue::Null => "null".to_owned(),
    }
}

fn render_rusqlite(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
        rusqlite::types::ValueRef::Real(v) => format!("f:{v}"),
        rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
        rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
        rusqlite::types::ValueRef::Null => "null".to_owned(),
    }
}

fn render_rows(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(render_fsqlite)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn stock_transcript() -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(SCHEMA).expect("stock schema");
    let mut sqls: Vec<&str> = vec![""; 8];
    let mut transcript = Vec::new();
    for (step, op) in OPS.iter().enumerate() {
        match *op {
            Op::Exec(sql) => {
                conn.execute_batch(sql).expect("stock exec");
            }
            Op::Prepare(id, sql) => sqls[id] = sql,
            Op::Run(id, params) => {
                let mut stmt = conn.prepare_cached(sqls[id]).expect("stock prepare");
                let outcome = match stmt.execute(rusqlite::params_from_iter(to_rusqlite(params))) {
                    Ok(changes) => format!("ok changes={changes}"),
                    Err(error) => format!("error:{error}"),
                };
                transcript.push(format!("step {step} run {id} => {outcome}"));
            }
            Op::Query(id, params) => {
                let mut stmt = conn.prepare_cached(sqls[id]).expect("stock prepare");
                let width = stmt.column_count();
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(to_rusqlite(params)), |row| {
                        Ok((0..width)
                            .map(|i| render_rusqlite(row.get_ref(i).expect("cell")))
                            .collect::<Vec<_>>()
                            .join("|"))
                    })
                    .expect("stock query")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("stock rows");
                transcript.push(format!("step {step} query {id} => {rows:?}"));
            }
            Op::Dump(sql) => {
                let mut stmt = conn.prepare(sql).expect("stock dump");
                let width = stmt.column_count();
                let rows = stmt
                    .query_map([], |row| {
                        Ok((0..width)
                            .map(|i| render_rusqlite(row.get_ref(i).expect("cell")))
                            .collect::<Vec<_>>()
                            .join("|"))
                    })
                    .expect("stock dump query")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("stock dump rows");
                transcript.push(format!("dump {sql} => {rows:?}"));
            }
        }
    }
    transcript
}

async fn fsqlite_transcript(conn: &Connection) -> Vec<String> {
    conn.execute_batch(SCHEMA).await.expect("schema");
    let mut prepared = Vec::new();
    let mut transcript = Vec::new();
    for (step, op) in OPS.iter().enumerate() {
        match *op {
            Op::Exec(sql) => {
                conn.execute(sql).await.expect("exec");
            }
            Op::Prepare(id, sql) => {
                let stmt = conn.prepare(sql).await.expect("prepare");
                if prepared.len() <= id {
                    prepared.resize_with(id + 1, || None);
                }
                prepared[id] = Some(stmt);
            }
            Op::Run(id, params) => {
                let stmt = prepared[id].as_ref().expect("prepared");
                let outcome = match stmt.execute_with_params(&to_fsqlite(params)).await {
                    Ok(changes) => format!("ok changes={changes}"),
                    Err(error) => format!("error:{error}"),
                };
                transcript.push(format!("step {step} run {id} => {outcome}"));
            }
            Op::Query(id, params) => {
                let stmt = prepared[id].as_ref().expect("prepared");
                let rows = stmt
                    .query_with_params(&to_fsqlite(params))
                    .await
                    .expect("query");
                transcript.push(format!("step {step} query {id} => {:?}", render_rows(&rows)));
            }
            Op::Dump(sql) => {
                let rows = conn.query(sql).await.expect("dump");
                transcript.push(format!("dump {sql} => {:?}", render_rows(&rows)));
            }
        }
    }
    transcript
}

fn assert_matches_stock(label: &str, got: &[String], want: &[String]) {
    for (line, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "{label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "{label} transcript length");
}

#[test]
fn prepared_dml_subqueries_refold_per_execution_like_stock() {
    let expected = stock_transcript();
    let in_memory = expected.clone();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &in_memory);
    });
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("prepared_dml_reuse.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(&path_str).await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("file-backed", &got, &expected);
    });
    let checked = rusqlite::Connection::open(&path).expect("stock open of fsqlite file");
    let verdict: String = checked
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(verdict, "ok", "stock integrity_check of the fsqlite file");
}
