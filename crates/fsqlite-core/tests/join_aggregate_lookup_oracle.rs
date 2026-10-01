#![recursion_limit = "512"]

//! Implicit aggregates over a single-lookup join (bd-41o3t):
//! `SELECT count(*), sum(t.c) FROM u JOIN t ON t.a = u.a` on a file-backed
//! database compiles into a rowid or index lookup loop that feeds each match
//! to AggStep, instead of materializing every joined row first. count(*)
//! over an index lookup reads only the index.
//!
//! Each query is compared with rusqlite, with and without indexes on the
//! lookup columns. The cases cover INNER and LEFT joins, either FROM order,
//! expression keys, WHERE and extra ON terms, typed and untyped keys,
//! collated columns, an empty table, and a bound parameter. Shapes the lookup
//! loop does not handle (OR in ON, DISTINCT aggregates) must still answer
//! correctly through the general path.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{}", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{}", b.len()),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d, n TEXT COLLATE NOCASE)",
    "INSERT INTO t VALUES (1,1,'1',1.0,'1','A'),(2,2,'2',2.5,2,'b'),(3,NULL,'x',3.0,NULL,'C'),\
     (4,4,'4',4.0,4.0,'d'),(5,5,'05',5.0,'5','e'),(6,6,'6',6.0,x'36','F'),(10,10,'10',10.0,10,'aa'),\
     (12,2,'2',12.0,'2','B')",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, r REAL, d)",
    "INSERT INTO u VALUES (1,1,'1',0.5,1),(2,2,'2',1.0,'2'),(3,3,'3',1.5,NULL),(4,NULL,NULL,2.0,4),\
     (5,5,'5',2.5,'05'),(6,6,'a',3.0,6.0),(7,7,'7',5.0,'x')",
    "CREATE TABLE e(id INTEGER PRIMARY KEY, a INTEGER)",
];

const INDEXES: &[&str] = &[
    "CREATE INDEX t_a ON t(a)",
    "CREATE INDEX t_b ON t(b)",
    "CREATE INDEX t_n ON t(n)",
    "CREATE INDEX t_d ON t(d)",
    "CREATE INDEX e_a ON e(a)",
];

const QUERIES: &[&str] = &[
    "SELECT count(*) FROM u JOIN t ON t.a = u.a",
    "SELECT count(*) FROM t JOIN u ON t.a = u.a",
    "SELECT count(*) FROM u JOIN t ON t.id = u.id",
    "SELECT count(*) FROM u JOIN t ON t.id = u.a * 2",
    "SELECT count(*), sum(t.c), total(t.c), avg(t.c), min(t.c), max(t.c) FROM u JOIN t ON t.a = u.a",
    "SELECT count(t.b), count(u.b), min(t.b), max(u.b) FROM u JOIN t ON t.a = u.a",
    "SELECT sum(t.c * u.r), sum(t.a + u.a), max(t.b || u.b) FROM u JOIN t ON t.a = u.a",
    "SELECT count(*), count(t.id), sum(t.a) FROM u LEFT JOIN t ON t.a = u.a",
    "SELECT count(*), count(t.id), sum(t.c) FROM u LEFT JOIN t ON t.id = u.a + 3",
    "SELECT count(*) FROM u LEFT JOIN t ON t.a = u.a WHERE t.id IS NULL",
    "SELECT count(*), sum(u.r) FROM u JOIN t ON t.a = u.a WHERE u.r > 1",
    "SELECT count(*) FROM u JOIN t ON t.a = u.a WHERE t.c > 3",
    "SELECT count(*) FROM u JOIN t ON t.b = u.b",
    "SELECT count(*) FROM u JOIN t ON t.d = u.d",
    "SELECT count(*), min(t.n), max(t.n) FROM u JOIN t ON t.n = u.b",
    "SELECT min(t.n), max(t.n) FROM u JOIN t ON t.a = u.a",
    "SELECT count(*), sum(u.a), avg(u.a) FROM e JOIN u ON e.a = u.a",
    "SELECT count(*), sum(e.a) FROM u LEFT JOIN e ON e.a = u.a",
    "SELECT count(*) FROM u JOIN t ON t.id = u.b || ''",
    "SELECT count(*) FROM u JOIN t ON t.id = u.r * 2",
    "SELECT sum(t.id), max(u.id) FROM u JOIN t ON t.a = u.a AND t.c > 2",
    "SELECT count(*) FROM u JOIN t ON t.a = u.a OR t.id = u.id",
    "SELECT count(DISTINCT t.a) FROM u JOIN t ON t.a = u.a",
    "SELECT count(*) FROM u AS x JOIN t AS y ON y.a = x.a",
    "SELECT count(*) FROM t a JOIN t b ON b.a = a.id",
    "SELECT sum(t.rowid), count(u.rowid) FROM u JOIN t ON t.a = u.a",
];

async fn assert_agree(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
    rparams: &[&dyn rusqlite::ToSql],
) {
    let ff: Vec<Vec<String>> = fconn
        .query_with_params(sql, params)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut stmt = rconn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    let rr: Vec<Vec<String>> = stmt
        .query_map(rparams, |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect())
        })
        .expect("rusqlite query")
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(ff, rr, "mismatch on `{sql}`");
}

async fn opcodes(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .filter_map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(op)) => Some(op.to_string()),
            _ => None,
        })
        .collect()
}

/// bd-24pa2: an aggregate over a join that names a column no source has
/// reports SQLite's `no such column`, not an internal error.
#[test]
fn join_aggregate_unknown_column_reports_no_such_column() {
    for path in [None, Some("join_aggregate_unknown_column.db")] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = path.map_or_else(
                || ":memory:".to_owned(),
                |name| dir.path().join(name).to_string_lossy().into_owned(),
            );
            let f = Connection::open(&target).await.unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
            }
            for sql in [
                "SELECT count(*), sum(zz.c) FROM u JOIN e ON e.a = u.a",
                "SELECT u.b, max(zz.c) FROM u JOIN t ON t.a = u.a GROUP BY u.b",
            ] {
                let err = f.query(sql).await.expect_err(sql).to_string();
                assert!(
                    err.contains("no such column: zz.c") && !err.contains("internal"),
                    "`{sql}`: {err}"
                );
            }
        });
    }
}

#[test]
fn join_aggregates_over_lookups_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_aggregate_lookup.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in QUERIES {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            assert_agree(
                &f,
                &r,
                "SELECT count(*), sum(t.c) FROM u JOIN t ON t.a = u.a WHERE t.c > ?1",
                &[SqliteValue::Float(2.0)],
                &[&2.0_f64],
            )
            .await;

            if indexed {
                let ops = opcodes(&f, "SELECT count(*) FROM u JOIN t ON t.a = u.a").await;
                assert!(ops.iter().any(|op| op == "AggStep"), "{ops:?}");
                assert!(
                    !ops.iter().any(|op| op == "SeekRowid"),
                    "count(*) over an index lookup reads only the index: {ops:?}"
                );
                let ops = opcodes(&f, "SELECT sum(t.c) FROM u JOIN t ON t.a = u.a").await;
                assert!(ops.iter().any(|op| op == "SeekRowid"), "{ops:?}");
            }
        });
    }
}
