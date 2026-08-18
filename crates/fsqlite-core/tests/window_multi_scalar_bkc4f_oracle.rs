#![recursion_limit = "512"]

//! bd-bkc4f: a scalar output expression combining TWO OR MORE window functions
//! was broken.  `replace_window_with_placeholder` only replaced the FIRST
//! window call it found with a single shared placeholder, leaving any further
//! `FunctionCall{over:Some}` residuals in the AST — which then evaluated as a
//! scalar (`NULL`) or hit a name-based collation lookup missing `BINARY`
//! ("no such collation sequence: BINARY").  The placeholder machinery is now
//! multi-slot: every window call becomes a distinct `__win_result_N__`
//! placeholder, each is computed, and all placeholders are substituted before
//! the residual scalar expression is evaluated per row.
//!
//! rusqlite is the oracle.  All queries carry a deterministic outer ORDER BY so
//! rows are compared in emitted order.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        }
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => {
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        }
    }
}

/// Compare in emitted order (both queries carry a deterministic outer ORDER BY).
async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let fr: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank failed `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let rr: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(fr, rr, "multi-window scalar mismatch on `{sql}`");
}

async fn seed_t(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, price INTEGER)",
        "INSERT INTO t VALUES (1,30),(2,10),(3,50)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

async fn seed_s(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "CREATE TABLE s (cat TEXT, amt INTEGER)",
        "INSERT INTO s VALUES ('a',10),('a',20),('b',5),('b',15),('b',30)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn two_windows_plus_bkc4f() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed_t(&f, &r).await;
        // Headline #1: sum + count, both frame-less. Oracle: 93 every row.
        assert_agree(
            &f,
            &r,
            "SELECT sum(price) OVER () + count(*) OVER () FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn two_windows_minus_order_by_binary_collation_bkc4f() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed_t(&f, &r).await;
        // Headline #2: the residual OVER(ORDER BY) previously triggered the
        // spurious "no such collation sequence: BINARY". Oracle: 0 / 20 / 40.
        assert_agree(
            &f,
            &r,
            "SELECT max(price) OVER (ORDER BY id) - min(price) OVER (ORDER BY id) \
             FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn two_windows_concat_different_funcs_bkc4f() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed_t(&f, &r).await;
        // Headline #3: `||` of two different ranking functions. Oracle: 1-1 / 2-2 / 3-3.
        assert_agree(
            &f,
            &r,
            "SELECT row_number() OVER (ORDER BY id) || '-' || rank() OVER (ORDER BY id) \
             FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn three_windows_in_one_expression_bkc4f() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed_t(&f, &r).await;
        // Three windows summed. Oracle: 90 + 3 + 50 = 143.
        assert_agree(
            &f,
            &r,
            "SELECT sum(price) OVER () + count(*) OVER () + max(price) OVER () \
             FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn mixed_window_and_non_window_terms_bkc4f() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed_t(&f, &r).await;
        // Window + plain column. Oracle: 91 / 92 / 93.
        assert_agree(
            &f,
            &r,
            "SELECT sum(price) OVER () + id FROM t ORDER BY id",
        )
        .await;
        // Unary-negated window + window. Oracle: -87.
        assert_agree(
            &f,
            &r,
            "SELECT -sum(price) OVER () + count(*) OVER () FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn partitioned_two_windows_bkc4f() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed_t(&f, &r).await;
        // Each window carries its own PARTITION BY. Oracle: 82 / 11 / 82.
        assert_agree(
            &f,
            &r,
            "SELECT sum(price) OVER (PARTITION BY price>20) \
             + count(*) OVER (PARTITION BY price>20) FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn case_with_two_windows_bkc4f() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed_t(&f, &r).await;
        // Windows buried in both a CASE condition and its branches. Oracle: 90 / 3 / 3.
        assert_agree(
            &f,
            &r,
            "SELECT CASE WHEN row_number() OVER (ORDER BY id) = 1 \
             THEN sum(price) OVER () ELSE count(*) OVER () END FROM t ORDER BY id",
        )
        .await;
    });
}

#[test]
fn single_window_in_expression_control_bkc4f() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed_t(&f, &r).await;
        // Control: a SINGLE window inside an expression must still work
        // (one window -> one placeholder). Oracle: 91.
        assert_agree(
            &f,
            &r,
            "SELECT sum(price) OVER () + 1 FROM t ORDER BY id",
        )
        .await;
        // Control: a bare single window (the ColKind::Window fast path). Oracle: 90.
        assert_agree(&f, &r, "SELECT sum(price) OVER () FROM t ORDER BY id").await;
    });
}

#[test]
fn group_by_two_windows_gbw_path_bkc4f() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed_s(&f, &r).await;
        // GROUP BY + double window over grouped aggregates (GbwColKind path).
        // Oracle: 82 / 82.
        assert_agree(
            &f,
            &r,
            "SELECT cat, sum(sum(amt)) OVER () + count(*) OVER () \
             FROM s GROUP BY cat ORDER BY cat",
        )
        .await;
        // GROUP BY + double window with ORDER BY (the BINARY residual case).
        // Oracle: 0 / 20.
        assert_agree(
            &f,
            &r,
            "SELECT cat, max(sum(amt)) OVER (ORDER BY cat) - min(sum(amt)) OVER (ORDER BY cat) \
             FROM s GROUP BY cat ORDER BY cat",
        )
        .await;
    });
}
