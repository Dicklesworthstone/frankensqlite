#![recursion_limit = "512"]

//! CAST expression-semantics leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the SQLite CAST rules — CAST(x AS INTEGER) truncating a REAL
//! toward zero and parsing a text prefix (leading spaces + digits, stopping at
//! the first non-numeric), CAST(x AS REAL) incl scientific notation, CAST(x AS
//! TEXT) of ints/reals/blobs, CAST(x AS BLOB) (identity of text bytes), CAST(x
//! AS NUMERIC) choosing INT-vs-REAL, out-of-range text/real -> INTEGER clamping,
//! CAST of NULL (stays NULL), and typeof() after each CAST. Scalar results with
//! int/real distinction compared. Pass = coverage keeper; a mismatch is a leaf.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f:?}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f:?}"),
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

#[test]
fn cast_expression_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // CAST AS INTEGER: real truncates toward zero
            "SELECT CAST(3.9 AS INTEGER), CAST(-3.9 AS INTEGER), CAST(3.1 AS INTEGER), CAST(-0.5 AS INTEGER)",
            // CAST AS INTEGER of text: parse a leading numeric prefix, else 0
            "SELECT CAST('123abc' AS INTEGER), CAST('  42' AS INTEGER), CAST('abc' AS INTEGER), CAST('' AS INTEGER)",
            "SELECT CAST('-17xyz' AS INTEGER), CAST('+9' AS INTEGER), CAST('  -3.7q' AS INTEGER)",
            // CAST AS INTEGER of a numeric-looking text with exponent (integer parse stops before 'e')
            "SELECT CAST('12e3' AS INTEGER), CAST('0x10' AS INTEGER)",
            // out-of-range text/real -> INTEGER clamps to i64 bounds
            "SELECT CAST('99999999999999999999' AS INTEGER), CAST(9e18 AS INTEGER), CAST(1e19 AS INTEGER), CAST(-1e19 AS INTEGER)",
            // CAST AS REAL: ints, text with exponent, leading text
            "SELECT CAST(5 AS REAL), CAST('3.14' AS REAL), CAST('1e3' AS REAL), CAST('2.5abc' AS REAL), CAST('xyz' AS REAL)",
            // CAST AS TEXT of int/real/blob
            "SELECT CAST(42 AS TEXT), CAST(3.5 AS TEXT), CAST(-7 AS TEXT)",
            "SELECT CAST(x'414243' AS TEXT)",              // blob bytes 'ABC' -> text
            // CAST AS BLOB (text/int/real -> their text-encoding bytes)
            "SELECT CAST('hi' AS BLOB), CAST(42 AS BLOB), CAST(3.5 AS BLOB)",
            // CAST AS NUMERIC: integral-valued text -> INTEGER, else REAL
            "SELECT CAST('42' AS NUMERIC), CAST('42.0' AS NUMERIC), CAST('42.5' AS NUMERIC), CAST('1e2' AS NUMERIC)",
            "SELECT CAST(3.0 AS NUMERIC), CAST(3.5 AS NUMERIC)",
            // typeof after CASTs
            "SELECT typeof(CAST(1 AS REAL)), typeof(CAST(1.5 AS INTEGER)), typeof(CAST(1 AS TEXT)), typeof(CAST('x' AS BLOB))",
            "SELECT typeof(CAST('42' AS NUMERIC)), typeof(CAST('42.5' AS NUMERIC)), typeof(CAST('42.0' AS NUMERIC))",
            // CAST of NULL is NULL for every target type
            "SELECT CAST(NULL AS INTEGER), CAST(NULL AS REAL), CAST(NULL AS TEXT), CAST(NULL AS BLOB), CAST(NULL AS NUMERIC)",
            "SELECT typeof(CAST(NULL AS INTEGER))",
            // round-trip: REAL -> TEXT -> REAL preserves value
            "SELECT CAST(CAST(3.14159 AS TEXT) AS REAL)",
            // CAST in a WHERE / comparison context
            "SELECT CAST('10' AS INTEGER) = 10, CAST(10 AS TEXT) = '10', CAST(2.0 AS INTEGER) = 2",
            // hex/large integer literal cast
            "SELECT CAST(0xff AS TEXT), CAST(0xff AS REAL)",
            // whitespace-only and sign-only text
            "SELECT CAST('   ' AS INTEGER), CAST('-' AS INTEGER), CAST('.' AS REAL), CAST('.5' AS REAL)",
        ];

        let mut diffs = Vec::new();
        for q in exprs {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(diffs.is_empty(), "{} CAST-semantics divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
