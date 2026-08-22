#![recursion_limit = "512"]

//! GH #272 (bd-gh-strict-affinity-coercion): a STRICT column applies its
//! declared-type affinity coercion to a losslessly-convertible value instead of
//! rejecting on raw storage class — a STRICT TEXT column stores INTEGER/REAL as
//! text, a STRICT INTEGER column stores an integer-valued REAL as an integer.
//! Non-lossless conversions (1.5 into INTEGER, BLOB into TEXT) still error.
//! rusqlite is the oracle for the accept/reject decision AND the stored
//! value/typeof.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

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

/// Run each statement on both engines; record a divergence if they disagree on ok/err.
async fn apply_checked(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    stmts: &[&str],
) -> Vec<String> {
    let mut d = Vec::new();
    for s in stmts {
        let f = fconn.execute(s).await;
        let r = rconn.execute_batch(s);
        match (f, r) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => d.push(format!("STMT_DIVERGE: {s}\n  frank: OK\n  csql: ERR({e})")),
            (Err(e), Ok(())) => d.push(format!("STMT_DIVERGE: {s}\n  frank: ERR({e})\n  csql: OK")),
        }
    }
    d
}

async fn assert_agree_query(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut fr: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap()
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect();
    fr.sort();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let mut rr: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rr.sort();
    assert_eq!(fr, rr, "stored value/typeof mismatch on `{sql}`");
}

#[test]
fn strict_text_accepts_numeric_gh272() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE t (a TEXT) STRICT"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // INTEGER and REAL coerce to text; a BLOB is not convertible -> both error.
        let d = apply_checked(
            &f,
            &r,
            &[
                "INSERT INTO t VALUES (1)",
                "INSERT INTO t VALUES (1.5)",
                "INSERT INTO t VALUES (X'DEAD')",
            ],
        )
        .await;
        assert!(
            d.is_empty(),
            "GH#272 STRICT TEXT divergence:\n{}",
            d.join("\n")
        );
        assert_agree_query(&f, &r, "SELECT a, typeof(a) FROM t ORDER BY a").await;
    });
}

#[test]
fn strict_integer_accepts_lossless_real_gh272() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE t (a INTEGER) STRICT"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // 3.0 losslessly stores as integer 3; 3.14 is fractional -> both error.
        let d = apply_checked(
            &f,
            &r,
            &[
                "INSERT INTO t VALUES (3.0)",
                "INSERT INTO t VALUES (3.14)",
                "INSERT INTO t VALUES (-7.0)",
            ],
        )
        .await;
        assert!(
            d.is_empty(),
            "GH#272 STRICT INTEGER divergence:\n{}",
            d.join("\n")
        );
        assert_agree_query(&f, &r, "SELECT a, typeof(a) FROM t ORDER BY a").await;
    });
}

#[test]
fn strict_multi_column_text_affinity_gh272() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE t (a INTEGER, b REAL, c TEXT) STRICT"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // Text '42' -> integer, '1.5' -> real, integer 9 -> text '9' (all lossless).
        let d = apply_checked(&f, &r, &["INSERT INTO t VALUES ('42', '1.5', 9)"]).await;
        assert!(
            d.is_empty(),
            "GH#272 STRICT multi-col divergence:\n{}",
            d.join("\n")
        );
        assert_agree_query(
            &f,
            &r,
            "SELECT a, typeof(a), b, typeof(b), c, typeof(c) FROM t",
        )
        .await;
    });
}

#[test]
fn strict_update_integer_into_text_gh272() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT) STRICT",
            "INSERT INTO t VALUES (1, 'x')",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // UPDATE with an integer input coerces to text on both engines.
        let d = apply_checked(&f, &r, &["UPDATE t SET a = 42 WHERE id = 1"]).await;
        assert!(
            d.is_empty(),
            "GH#272 STRICT UPDATE divergence:\n{}",
            d.join("\n")
        );
        assert_agree_query(&f, &r, "SELECT a, typeof(a) FROM t").await;
    });
}
