#![recursion_limit = "512"]

//! GH #163 (bd-gh-strict-text-coercion): a STRICT INTEGER/REAL column must accept
//! a TEXT value that losslessly converts to the column's type (stock sqlite3
//! STRICT), and reject text that does not. rusqlite is the oracle for both the
//! accept/reject decision and the stored value/typeof.

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

/// Run a statement on both engines; record a divergence if they disagree on ok/err.
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
    assert_eq!(fr, rr, "value mismatch on `{sql}`");
}

#[test]
fn strict_integer_column_text_coercion_gh163() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE ti (a INTEGER) STRICT", "INSERT INTO ti VALUES (7)"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // '7' losslessly integer -> accept; '1.5' and 'abc' -> reject on both.
        let d = apply_checked(&f, &r, &[
            "INSERT INTO ti VALUES ('7')",
            "INSERT INTO ti VALUES ('1.5')",
            "INSERT INTO ti VALUES ('abc')",
        ]).await;
        assert!(d.is_empty(), "GH#163 STRICT INTEGER divergence:\n{}", d.join("\n"));
        assert_agree_query(&f, &r, "SELECT a, typeof(a) FROM ti ORDER BY a").await;
    });
}

#[test]
fn strict_real_column_text_coercion_gh163() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in ["CREATE TABLE tr (a REAL) STRICT", "INSERT INTO tr VALUES (2.5)"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // '1.5' -> real; '9' -> promotes to 9.0; 'xyz' -> reject.
        let d = apply_checked(&f, &r, &[
            "INSERT INTO tr VALUES ('1.5')",
            "INSERT INTO tr VALUES ('9')",
            "INSERT INTO tr VALUES ('xyz')",
        ]).await;
        assert!(d.is_empty(), "GH#163 STRICT REAL divergence:\n{}", d.join("\n"));
        assert_agree_query(&f, &r, "SELECT a, typeof(a) FROM tr ORDER BY a").await;
    });
}
