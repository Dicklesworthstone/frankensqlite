#![recursion_limit = "512"]

//! STRICT-table type-enforcement leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over STRICT tables — declared-type coercion (INT<->INTEGER, REAL
//! widening, TEXT), lossless-integer acceptance (1.0 -> 1) vs rejection (1.5,
//! 'abc'), ANY columns preserving storage class, and typeof of stored values.
//! Insert success is compared via row count; stored types via typeof. Pass =
//! coverage keeper; a mismatch is a leaf divergence.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else { return vec![vec!["ERR".to_owned()]] };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect::<Vec<_>>())
    }) {
        Ok(rows) => rows.collect::<Result<Vec<_>, _>>().unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn strict_tables_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let mut check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        ex(&f, &r, "CREATE TABLE s(i INTEGER, r REAL, t TEXT, b BLOB, a ANY) STRICT").await;

        // valid rows
        ex(&f, &r, "INSERT INTO s VALUES (1, 2.5, 'x', x'0102', 'anything')").await;
        ex(&f, &r, "INSERT INTO s VALUES (10, 3, 'y', x'03', 42)").await;   // r=3 widened, a=int 42
        ex(&f, &r, "INSERT INTO s VALUES ('50', 4, 'z', x'04', 2.5)").await; // i='50' -> lossless int?
        ex(&f, &r, "INSERT INTO s VALUES (7.0, 5, 'w', x'05', x'aa')").await; // i=7.0 -> lossless 7
        check("valid rows typeof", fq(&f, "SELECT typeof(i),typeof(r),typeof(t),typeof(b),typeof(a) FROM s ORDER BY rowid").await, rq(&r, "SELECT typeof(i),typeof(r),typeof(t),typeof(b),typeof(a) FROM s ORDER BY rowid"), &mut diffs);
        check("valid rows values", fq(&f, "SELECT i,r,a FROM s ORDER BY rowid").await, rq(&r, "SELECT i,r,a FROM s ORDER BY rowid"), &mut diffs);

        // rejections (each must fail on both -> row count unchanged at 4)
        ex(&f, &r, "INSERT INTO s VALUES ('abc', 1.0, 't', x'00', 1)").await;  // 'abc' not int
        ex(&f, &r, "INSERT INTO s VALUES (1.5, 1.0, 't', x'00', 1)").await;    // 1.5 not lossless int
        ex(&f, &r, "INSERT INTO s VALUES (1, 'nope', 't', x'00', 1)").await;   // 'nope' not real
        ex(&f, &r, "INSERT INTO s VALUES (1, 1.0, 1, x'00', 1)").await;        // int into TEXT (strict)
        ex(&f, &r, "INSERT INTO s VALUES (1, 1.0, 't', 'notblob', 1)").await;  // text into BLOB
        check("rejections count", fq(&f, "SELECT count(*) FROM s").await, rq(&r, "SELECT count(*) FROM s"), &mut diffs);

        // STRICT with NULL (allowed unless NOT NULL)
        ex(&f, &r, "INSERT INTO s VALUES (NULL, NULL, NULL, NULL, NULL)").await;
        check("nulls allowed", fq(&f, "SELECT count(*) FROM s WHERE i IS NULL").await, rq(&r, "SELECT count(*) FROM s WHERE i IS NULL"), &mut diffs);

        // STRICT table with a bare (no declared type) column must be rejected at CREATE
        ex(&f, &r, "CREATE TABLE bad(x, y INTEGER) STRICT").await;
        check("bare-column strict rejected", fq(&f, "SELECT count(*) FROM sqlite_master WHERE name='bad'").await, rq(&r, "SELECT count(*) FROM sqlite_master WHERE name='bad'"), &mut diffs);

        assert!(diffs.is_empty(), "{} STRICT-table divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
