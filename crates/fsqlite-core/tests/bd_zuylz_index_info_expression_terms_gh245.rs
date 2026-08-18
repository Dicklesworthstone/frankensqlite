#![recursion_limit = "512"]

//! GH #245/#246 (bd-zuylz): PRAGMA index_info / index_xinfo must report
//! expression-index key terms with SQLite's cid=-2 / NULL-name sentinel, and
//! must not drop the whole index when it carries an expression term.
//!
//! rusqlite (real SQLite 3.46.1) is the oracle. The differential compares the
//! full structured row set (seqno, cid, name-nullability, and for xinfo the
//! desc/coll/key columns), not display text.

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

async fn fq(fconn: &Connection, sql: &str) -> Vec<Vec<String>> {
    fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(rconn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = rconn.prepare(sql).unwrap();
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

/// index_info / index_xinfo rows are already in seqno order on both engines;
/// compare them verbatim (do NOT sort — seqno ordering is part of the contract).
async fn assert_agree_ordered(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let fr = fq(fconn, sql).await;
    let rr = rq(rconn, sql);
    assert_eq!(fr, rr, "row-set mismatch on `{sql}`");
    assert!(!fr.is_empty(), "`{sql}` unexpectedly returned no rows");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, extra TEXT)",
        // Pure expression index.
        "CREATE INDEX idx_expr ON t(lower(name))",
        // Plain single-column index (regression guard).
        "CREATE INDEX idx_plain ON t(name)",
        // Mixed: plain column followed by an expression term.
        "CREATE INDEX idx_mixed ON t(extra, lower(name))",
        // Multi-plain-column index (regression guard).
        "CREATE INDEX idx_multi ON t(name, extra)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn index_info_and_xinfo_expression_terms_match_stock_gh245() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;

        for idx in ["idx_expr", "idx_plain", "idx_mixed", "idx_multi"] {
            assert_agree_ordered(&f, &r, &format!("PRAGMA index_info('{idx}')")).await;
            assert_agree_ordered(&f, &r, &format!("PRAGMA index_xinfo('{idx}')")).await;
        }
    });
}
