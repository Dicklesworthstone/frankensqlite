#![recursion_limit = "512"]

//! GH #162 / #164 (bd-gh-strict-typecheck-coercion): every enumerated STRICT
//! type-check coercion case, verified differentially against rusqlite. These
//! were resolved by the GH #163 (STRICT INTEGER/REAL accept lossless TEXT) and
//! GH #272 (STRICT TEXT accept INTEGER/REAL; STRICT INTEGER accept lossless
//! REAL) fixes to `Value::validate_strict`; this keeper pins the full case
//! battery the two issues named, including the whitespace-tolerant / '42.0'
//! text parses and the cases stock sqlite3 still rejects.

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

async fn apply_checked(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) -> Vec<String> {
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
    let mut fr: Vec<Vec<String>> = fconn.query(sql).await.unwrap().iter().map(|r| r.values().iter().map(tag_f).collect()).collect();
    fr.sort();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let mut rr: Vec<Vec<String>> = st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    rr.sort();
    assert_eq!(fr, rr, "stored value/typeof mismatch on `{sql}`");
}

#[test]
fn strict_integer_accepts_lossless_text_and_real_gh162_164() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE t (a INT) STRICT"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // ACCEPTED on the oracle: well-formed integer text, '42.0' text (folds to
        // integer), whitespace-padded text, and an exact-integer REAL (#164).
        let d = apply_checked(&f, &r, &[
            "INSERT INTO t VALUES ('42')",
            "INSERT INTO t VALUES ('42.0')",
            "INSERT INTO t VALUES ('  42  ')",
            "INSERT INTO t VALUES (42.0)",
        ]).await;
        assert!(d.is_empty(), "GH#162/#164 STRICT INTEGER accept divergence:\n{}", d.join("\n"));
        assert_agree_query(&f, &r, "SELECT a, typeof(a) FROM t").await;
    });
}

#[test]
fn strict_integer_rejects_nonlossless_gh162_164() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE t (a INT) STRICT"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // REJECTED on the oracle: trailing-garbage text, fractional REAL, and a
        // magnitude past i64 that coerces to REAL (not a lossless integer).
        let d = apply_checked(&f, &r, &[
            "INSERT INTO t VALUES ('42x')",
            "INSERT INTO t VALUES (42.5)",
            "INSERT INTO t VALUES ('9223372036854775808')",
        ]).await;
        assert!(d.is_empty(), "GH#162/#164 STRICT INTEGER reject divergence:\n{}", d.join("\n"));
        assert_agree_query(&f, &r, "SELECT a, typeof(a) FROM t").await;
    });
}

#[test]
fn strict_text_and_real_cross_type_gh162_164() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE tt (a TEXT) STRICT", "CREATE TABLE tr (a REAL) STRICT", "CREATE TABLE tb (a BLOB) STRICT"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let d = apply_checked(&f, &r, &[
            "INSERT INTO tt VALUES (42)",     // integer -> text '42'
            "INSERT INTO tr VALUES ('1.5')",  // text -> real 1.5
            "INSERT INTO tb VALUES ('hi')",   // text into BLOB -> reject
        ]).await;
        assert!(d.is_empty(), "GH#162/#164 cross-type divergence:\n{}", d.join("\n"));
        assert_agree_query(&f, &r, "SELECT a, typeof(a) FROM tt").await;
        assert_agree_query(&f, &r, "SELECT a, typeof(a) FROM tr").await;
    });
}
