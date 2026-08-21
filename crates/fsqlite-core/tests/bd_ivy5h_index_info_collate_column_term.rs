#![recursion_limit = "512"]

//! bd-ivy5h: PRAGMA index_info / index_xinfo must resolve a COLLATE-decorated
//! bare column key term (e.g. `b COLLATE NOCASE`) to the underlying table column
//! (cid>=0 + name), NOT report it as an anonymous expression (cid=-2 / NULL).
//! The collation is surfaced separately by index_xinfo's `coll` column.
//!
//! The bug only surfaced inside an *expression-mode* index (one carrying at
//! least one genuine expression term, which stringifies every term into
//! key_expressions), so the COLLATE column arrived as its full SQL fragment.
//! Follow-up to bd-zuylz (GH#245/#246). rusqlite (real SQLite) is the oracle.

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

/// index_info / index_xinfo rows are in seqno order on both engines; compare
/// verbatim (seqno ordering is part of the contract).
async fn assert_agree_ordered(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let fr = fq(fconn, sql).await;
    let rr = rq(rconn, sql);
    assert_eq!(fr, rr, "row-set mismatch on `{sql}`");
    assert!(!fr.is_empty(), "`{sql}` unexpectedly returned no rows");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b TEXT)",
        // THE BUG: a COLLATE-decorated column followed by a genuine expression.
        // The expression term forces expression-mode storage, so the COLLATE
        // column is stringified as `b COLLATE NOCASE` and used to be misreported
        // as cid=-2/NULL instead of column b.
        "CREATE INDEX idx_collate_expr ON t(b COLLATE NOCASE, a + 1)",
        // COLLATE column in the middle of a mixed index (both neighbours differ).
        "CREATE INDEX idx_collate_mixed ON t(a, b COLLATE RTRIM, lower(b))",
        // Pure COLLATE-column index (all-simple path — regression guard).
        "CREATE INDEX idx_collate_plain ON t(b COLLATE NOCASE)",
        // Pure expression index stays cid=-2 (regression guard).
        "CREATE INDEX idx_expr_only ON t(a + 1)",
        // Plain multi-column index (regression guard).
        "CREATE INDEX idx_multi ON t(a, b)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn index_info_collate_column_terms_match_stock_bd_ivy5h() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;

        for idx in [
            "idx_collate_expr",
            "idx_collate_mixed",
            "idx_collate_plain",
            "idx_expr_only",
            "idx_multi",
        ] {
            assert_agree_ordered(&f, &r, &format!("PRAGMA index_info('{idx}')")).await;
            assert_agree_ordered(&f, &r, &format!("PRAGMA index_xinfo('{idx}')")).await;
        }
    });
}
