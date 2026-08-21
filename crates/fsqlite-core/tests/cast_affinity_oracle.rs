//! Differential oracle: CAST semantics + type-affinity coercion in comparisons
//! vs rusqlite (bundled SQLite 3.53). A probe sweep found this surface
//! stock-correct across 21 cases; this keeper locks it in.
//!
//! Notable semantics asserted: CAST(text AS INTEGER) parses a leading numeric
//! prefix (else 0), CAST(real AS INTEGER) truncates toward zero, NUMERIC
//! affinity stores an integral string as INTEGER; comparison applies column
//! affinity (int col vs text literal coerces the literal; text col vs numeric
//! literal coerces the literal) while a bare literal-vs-literal comparison does
//! NOT coerce; ordering follows the storage-class rank NULL < num < text < blob.

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

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(fr, rr, "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

#[test]
fn cast_text_to_integer() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT CAST('123abc' AS INTEGER)", "leading numeric prefix -> 123").await;
        agree(&[], "SELECT CAST('abc' AS INTEGER)", "non-numeric -> 0").await;
        agree(&[], "SELECT CAST('  42  ' AS INTEGER)", "leading/trailing whitespace").await;
        agree(&[], "SELECT CAST('-7xyz' AS INTEGER)", "sign + leading numeric").await;
        agree(&[], "SELECT CAST('1e3' AS INTEGER)", "integer cast stops at 'e' -> 1").await;
    });
}

#[test]
fn cast_real_to_integer_truncates() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT CAST(3.99 AS INTEGER)", "truncate toward zero (positive)").await;
        agree(&[], "SELECT CAST(-3.99 AS INTEGER)", "truncate toward zero (negative)").await;
    });
}

#[test]
fn cast_to_real_and_text() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT CAST('3.14xyz' AS REAL)", "text -> real with trailing garbage").await;
        agree(&[], "SELECT CAST('2.5e2' AS REAL)", "text -> real with exponent").await;
        agree(&[], "SELECT CAST(123 AS TEXT)", "int -> text").await;
        agree(&[], "SELECT CAST(2.5 AS TEXT)", "real -> text").await;
    });
}

#[test]
fn cast_null_and_numeric() {
    asupersync::test_utils::run_test(|| async {
        agree(&[], "SELECT CAST(NULL AS INTEGER), CAST(NULL AS TEXT)", "CAST NULL stays NULL").await;
        agree(&[], "SELECT CAST('42.0' AS NUMERIC)", "NUMERIC of integral value -> integer").await;
        agree(&[], "SELECT CAST('42.5' AS NUMERIC)", "NUMERIC of fractional value -> real").await;
    });
}

#[test]
fn comparison_affinity() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(x INTEGER)", "INSERT INTO t VALUES (5),(10)"],
            "SELECT x FROM t WHERE x = '5'",
            "int column vs text literal: literal coerced to int",
        ).await;
        agree(
            &["CREATE TABLE t(s TEXT)", "INSERT INTO t VALUES ('5'),('05'),('5.0')"],
            "SELECT s FROM t WHERE s = 5 ORDER BY s",
            "text column vs numeric literal: literal coerced to text",
        ).await;
        agree(
            &["CREATE TABLE t(n NUMERIC)", "INSERT INTO t VALUES ('007'),('3.14'),('abc')"],
            "SELECT n, typeof(n) FROM t ORDER BY rowid",
            "NUMERIC affinity stores integral string as integer",
        ).await;
        agree(&[], "SELECT 5 = '5', '5' = 5, 5 < '5', 'abc' < 5",
              "bare literal-vs-literal comparison applies no affinity").await;
    });
}

#[test]
fn storage_class_ordering() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(b BLOB)", "INSERT INTO t VALUES (X'01'),(X'00'),(5),('x')"],
            "SELECT quote(b) FROM t ORDER BY b",
            "blob column ORDER BY spans storage classes",
        ).await;
        agree(
            &["CREATE TABLE t(v)", "INSERT INTO t VALUES (NULL),(1),(2.5),('a'),(X'ff')"],
            "SELECT typeof(v) FROM t ORDER BY v",
            "ORDER BY rank: NULL < numeric < text < blob",
        ).await;
        agree(
            &["CREATE TABLE t(s TEXT)", "INSERT INTO t VALUES ('10'),('9'),('100')"],
            "SELECT s FROM t WHERE CAST(s AS INTEGER) > 9 ORDER BY CAST(s AS INTEGER)",
            "explicit CAST in WHERE/ORDER BY",
        ).await;
    });
}
