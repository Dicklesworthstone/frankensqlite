#![recursion_limit = "512"]

//! GH #225 (bd-gh-having-bare-column-resolution) HEAD probe: HAVING without
//! GROUP BY (and unprojected columns under GROUP BY) must resolve a bare column
//! to a real row's value, not NULL. Differential-vs-rusqlite probe to establish
//! exactly what still diverges at HEAD before implementing.

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

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut fr: Vec<Vec<String>> = fconn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}")).iter().map(|r| r.values().iter().map(tag_f).collect()).collect();
    fr.sort();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let mut rr: Vec<Vec<String>> = st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    rr.sort();
    assert_eq!(fr, rr, "HAVING bare-column mismatch on `{sql}`");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in ["CREATE TABLE t (x INTEGER)", "INSERT INTO t VALUES (1),(2),(3)"] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn having_bare_column_implicit_gh225() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Implicit aggregate: bare x in HAVING is the (first) scanned row's x=1.
        assert_agree(&f, &r, "SELECT sum(x) FROM t HAVING x=1").await;      // -> 6
        assert_agree(&f, &r, "SELECT sum(x) FROM t HAVING x IS NULL").await; // -> no row
        assert_agree(&f, &r, "SELECT sum(x) FROM t HAVING x=2").await;       // -> no row
    });
}

#[test]
fn having_bare_column_group_by_unprojected_gh225() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // GROUP BY on x (the group key) but x is not projected.
        assert_agree(&f, &r, "SELECT sum(x) FROM t GROUP BY x HAVING x=1").await; // -> 1
        assert_agree(&f, &r, "SELECT sum(x) FROM t GROUP BY x HAVING x>1").await; // -> 2, 3
        // Projected control (already works).
        assert_agree(&f, &r, "SELECT x, sum(x) FROM t GROUP BY x HAVING x=1").await;
    });
}
