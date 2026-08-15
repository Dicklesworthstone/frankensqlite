#![recursion_limit = "512"]

//! GH #248 (bd-gh-window-order-nulls): a window function whose ORDER BY carries
//! an explicit NULLS FIRST/LAST must sort the partition accordingly, so the
//! frame (cumulative SUM/COUNT) and rank (row_number) reflect that ordering.
//! Resolved by the window-path rewrite (win_order_by carries nulls_order,
//! cmp_in_aggregate_order_keys honors term.nulls); this keeper pins the repro.
//! rusqlite is the oracle. Rows are compared per-id (stable outer ORDER BY id)
//! so the test targets the WINDOW VALUES, not the emitted row order.

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

/// Compare in emitted order (both queries carry a deterministic outer ORDER BY).
async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let fr: Vec<Vec<String>> = fconn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}")).iter().map(|r| r.values().iter().map(tag_f).collect()).collect();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let rr: Vec<Vec<String>> = st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(fr, rr, "window NULLS-order mismatch on `{sql}`");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO t VALUES (1,3),(2,NULL),(3,1),(4,NULL),(5,2)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn window_cumulative_sum_nulls_last_gh248() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Cumulative SUM over ORDER BY v NULLS LAST: NULL rows sort last and pick
        // up the running total (6), rather than sorting first with SUM=NULL.
        assert_agree(&f, &r,
            "SELECT id, v, SUM(v) OVER (ORDER BY v NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t ORDER BY id").await;
        assert_agree(&f, &r,
            "SELECT id, v, COUNT(v) OVER (ORDER BY v NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t ORDER BY id").await;
    });
}

#[test]
fn window_row_number_nulls_last_gh248() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // row_number() over ORDER BY v NULLS LAST -> NULL rows ranked 4,5.
        assert_agree(&f, &r,
            "SELECT id, v, row_number() OVER (ORDER BY v NULLS LAST) FROM t ORDER BY id").await;
        assert_agree(&f, &r,
            "SELECT id, v, rank() OVER (ORDER BY v NULLS LAST) FROM t ORDER BY id").await;
    });
}

#[test]
fn window_order_desc_nulls_first_gh248() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // DESC NULLS FIRST -> NULL rows ranked first.
        assert_agree(&f, &r,
            "SELECT id, v, row_number() OVER (ORDER BY v DESC NULLS FIRST) FROM t ORDER BY id").await;
        // Default DESC (NULLS LAST) as a contrast.
        assert_agree(&f, &r,
            "SELECT id, v, row_number() OVER (ORDER BY v DESC) FROM t ORDER BY id").await;
    });
}
