#![recursion_limit = "512"]

//! Differential oracle sweep (pane af49, 2026-08-20): frank vs rusqlite over the
//! SQLite 3.35+ math function family (SQLITE_ENABLE_MATH_FUNCTIONS — the project
//! builds rusqlite with it via .cargo/config LIBSQLITE3_FLAGS). Exactly-rounded
//! ops (abs/sqrt/floor/ceil/trunc/round/mod/sign/pi) compared bit-exact;
//! transcendentals (sin/cos/tan/exp/log/pow/…) normalized via in-SQL
//! `round(...,10)` to neutralize cross-libm last-ULP noise; domain errors
//! (sqrt(-1), ln(0), acos(2), …) compared for NULL/value handling. Pass =
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

async fn fval(conn: &Connection, sql: &str) -> String {
    let rows = conn.query(sql).await.unwrap_or_else(|e| panic!("frank `{sql}`: {e:?}"));
    assert_eq!(rows.len(), 1, "frank `{sql}` returned {} rows", rows.len());
    tag_f(&rows[0].values()[0])
}
fn rval(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| {
        Ok(tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)))
    })
    .unwrap_or_else(|e| panic!("rusqlite `{sql}`: {e:?}"))
}

#[test]
fn math_functions_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // exactly-rounded / integer-domain — compared bit-exact
            "SELECT abs(-5), abs(-5.5), abs(9223372036854775807)",
            "SELECT floor(2.7), ceil(2.1), trunc(2.9), trunc(-2.9)",
            "SELECT round(2.567,2), round(2.5), round(3.5), round(-2.5)",
            "SELECT sign(-3), sign(0), sign(3.2), sign(0.0)",
            "SELECT mod(7,3), mod(-7,3), mod(7.5,2), mod(7,0)",
            "SELECT typeof(sqrt(4)), typeof(abs(5)), typeof(ceil(2.5)), typeof(floor(3))",
            // transcendentals — round(...,10) neutralizes cross-libm ULP noise
            "SELECT round(sqrt(2),10), round(sqrt(16),10)",
            "SELECT round(sin(1),10), round(cos(0),10), round(tan(1),10)",
            "SELECT round(asin(0.5),10), round(acos(0.5),10), round(atan(1),10)",
            "SELECT round(atan2(1,1),10)",
            "SELECT round(exp(1),10), round(exp(0),10)",
            "SELECT round(ln(2.718281828459045),10), round(log(100),10)",
            "SELECT round(log10(1000),10), round(log2(8),10), round(log(2,8),10)",
            "SELECT round(pow(2,10),10), round(pow(2,0.5),10), round(power(9,0.5),10)",
            "SELECT round(sinh(1),10), round(cosh(1),10), round(tanh(1),10)",
            "SELECT round(radians(180),10), round(degrees(3.141592653589793),10)",
            "SELECT round(pi(),10)",
            "SELECT round(pow(2,-2),10), round(pow(0,0),10), round(pow(-2,3),10)",
            // domain edges — NULL vs value handling
            "SELECT sqrt(-1), sqrt(0), sqrt(-0.0)",
            "SELECT ln(0), ln(-1), log(0), log(-5)",
            "SELECT acos(2), asin(2), acos(-2)",
            "SELECT pow(-1,0.5), pow(0,-1)",
            "SELECT ceil(NULL), sqrt(NULL), pow(2,NULL), atan2(NULL,1)",
            "SELECT abs(NULL), round(NULL,2), mod(NULL,3)",
            // interplay with integer/real typing
            "SELECT typeof(pow(2,3)), typeof(sqrt(9)), typeof(round(2.5,0))",
            "SELECT min(sqrt(2), sqrt(3)), max(pow(2,2), pow(2,3))",
        ];

        let mut diffs = Vec::new();
        for e in exprs {
            let fv = fval(&f, e).await;
            let rv = rval(&r, e);
            if fv != rv {
                diffs.push(format!("  `{e}`\n     frank= {fv}\n     stock= {rv}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} math-function divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
