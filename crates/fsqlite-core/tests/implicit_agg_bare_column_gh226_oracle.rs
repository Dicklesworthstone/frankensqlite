#![recursion_limit = "512"]

//! GH #226 (bd-gh-implicit-agg-bare-column-row): a bare (non-aggregate) column
//! in an aggregate query with no GROUP BY takes the value from the FIRST
//! scanned row (stock sqlite3), not the last. fsqlite previously captured the
//! last scanned row because emit_aggregate_accumulate_body stored the bare
//! expression on every row; a first-row flag now gates the store. The separate
//! min()/max() bare-column tracking must be undisturbed. rusqlite is the oracle.

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

/// Aggregate queries here return exactly one row; compare it directly.
async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let fr: Vec<Vec<String>> = fconn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}")).iter().map(|r| r.values().iter().map(tag_f).collect()).collect();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let rr: Vec<Vec<String>> = st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(fr, rr, "aggregate bare-column mismatch on `{sql}`");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection, rows: &str) {
    for s in ["CREATE TABLE t (x INTEGER, y TEXT)", &format!("INSERT INTO t VALUES {rows}")] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn implicit_agg_bare_column_first_row_gh226() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Scan order (rowid order) drives the "first row" — values are NOT sorted.
        seed(&f, &r, "(3,'c'),(1,'a'),(2,'b')").await;
        // sum/count/avg/group_concat + a bare column -> the FIRST scanned row.
        assert_agree(&f, &r, "SELECT sum(x), x FROM t").await;
        assert_agree(&f, &r, "SELECT count(*), x, y FROM t").await;
        assert_agree(&f, &r, "SELECT avg(x), x FROM t").await;
        assert_agree(&f, &r, "SELECT group_concat(x), y FROM t").await;
    });
}

#[test]
fn implicit_agg_bare_column_first_matching_row_where_gh226() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r, "(3,'c'),(1,'a'),(2,'b')").await;
        // With a WHERE, the bare column is the first row that PASSES the filter.
        assert_agree(&f, &r, "SELECT sum(x), x, y FROM t WHERE x > 1").await;
        assert_agree(&f, &r, "SELECT count(*), y FROM t WHERE x >= 2").await;
    });
}

#[test]
fn implicit_agg_minmax_bare_column_undisturbed_gh226() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r, "(3,'c'),(1,'a'),(2,'b')").await;
        // min()/max() carry their OWN row's bare column (a separate, correct
        // path) — the first-row flag must not change it.
        assert_agree(&f, &r, "SELECT max(x), y FROM t").await;
        assert_agree(&f, &r, "SELECT min(x), y FROM t").await;
        assert_agree(&f, &r, "SELECT max(x), x, y FROM t").await;
    });
}

#[test]
fn implicit_agg_empty_and_single_gh226() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r, "(5,'e')").await;
        // Single row: bare column is that row.
        assert_agree(&f, &r, "SELECT sum(x), x, y FROM t").await;
        // Empty scan (all filtered): aggregate yields COUNT=0/SUM=NULL, bare NULL.
        assert_agree(&f, &r, "SELECT sum(x), x, y FROM t WHERE x > 100").await;
    });
}
