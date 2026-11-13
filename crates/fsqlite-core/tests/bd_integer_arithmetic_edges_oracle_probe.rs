#![recursion_limit = "512"]

//! Integer/numeric arithmetic-edge leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the corners of SQLite integer math — signed 64-bit overflow
//! promoting to REAL (e.g. 9223372036854775807 + 1), integer division
//! truncation-toward-zero with negative operands, modulo sign (result takes the
//! dividend's sign), division/modulo by zero yielding NULL, unary minus on
//! i64::MIN, hex-literal parsing, bitwise AND/OR/XOR/NOT and shifts (including
//! negative and large shift counts), and INT vs REAL result typing. Scalar
//! results compared via a value tag that distinguishes int from real.
//! Pass = coverage keeper; a mismatch is a leaf divergence.

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
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else {
        return vec![vec!["ERR".to_owned()]];
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}

#[test]
fn integer_arithmetic_edges_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // signed 64-bit overflow -> promotes to REAL
            "SELECT 9223372036854775807 + 1",
            "SELECT 9223372036854775807 * 2",
            "SELECT -9223372036854775808 - 1",
            "SELECT 9223372036854775807 + 0", // stays int at the boundary
            // integer division truncates toward zero
            "SELECT 7 / 2, -7 / 2, 7 / -2, -7 / -2",
            "SELECT 1 / 2, -1 / 2",
            // modulo takes the dividend's sign
            "SELECT 7 % 3, -7 % 3, 7 % -3, -7 % -3",
            "SELECT 5 % 2, 5 % -2, -5 % 2",
            // division / modulo by zero -> NULL
            "SELECT 1 / 0, 1 % 0, 0 / 0, 5.0 / 0",
            // unary minus on i64::MIN -> overflow to REAL
            "SELECT -(-9223372036854775808)",
            "SELECT abs(-9223372036854775808)",
            // mixing int and real promotes to real
            "SELECT 3 + 0.0, 3 / 2.0, 10 * 1.0",
            // hex literals
            "SELECT 0xff, 0x10, 0xFFFFFFFF, 0x7fffffffffffffff",
            "SELECT 0xffffffffffffffff", // -1 as signed 64-bit
            // bitwise ops
            "SELECT 5 & 3, 5 | 2",
            "SELECT ~0, ~5, ~-1",
            "SELECT 1 << 4, 256 >> 2, 1 << 63",
            "SELECT -8 >> 1, -8 << 1",
            "SELECT 5 & NULL, 5 | NULL, NULL << 2", // NULL propagation
            // large / negative shift counts (SQLite: >=64 or negative -> 0 or sign-based)
            "SELECT 1 << 64, 1 << 100, 1 << -1, 255 >> 64",
            // comparison result typing (0/1 integers)
            "SELECT 2 > 1, 1 = 1, 3 <> 3, NULL = NULL, 1 < NULL",
            // string-to-number coercion in arithmetic
            "SELECT '5' + 3, '5abc' + 3, 'abc' + 3, '  12 ' + 0",
            "SELECT '3.14' * 2, '0x10' + 0",
            // round-half behavior and REAL formatting
            "SELECT 1.0/3.0, 2.0/3.0",
            // typeof across the boundary
            "SELECT typeof(9223372036854775807 + 1), typeof(9223372036854775807), typeof(1/2), typeof(1/2.0)",
        ];

        let mut diffs = Vec::new();
        for q in exprs {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} integer-arithmetic divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// `%` over operands read from rows, so it runs in the scan loop instead of
/// being folded: two INTEGER registers take the loop's integer path
/// (bd-w4pr1.13.20), every other pair `sql_rem`. The rows put each storage
/// class on both sides, a zero divisor, `i64::MIN % -1`, and result types
/// that change from row to row in the same result register (INTEGER after
/// REAL and NULL), compared with their `typeof` against stock.
#[test]
fn remainder_over_scanned_rows_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let setup = [
            "CREATE TABLE p(id INTEGER PRIMARY KEY, x, y, s TEXT)",
            "INSERT INTO p(x, y, s) VALUES \
             (7, 3, 'a'), (-7, 3, 'b'), (7, -3, 'c'), (-7, -3, 'd'), (5, 0, 'e'), \
             (0, 5, 'f'), (-9223372036854775808, -1, 'g'), (-9223372036854775808, 7, 'h'), \
             (9223372036854775807, -2, 'i'), (7.5, 2, 'j'), (8, 3, 'k'), (7, 2.5, 'l'), \
             (NULL, 3, 'm'), (9, 4, 'n'), ('17', 5, 'o'), (17, '5', 'p'), \
             (3, NULL, 'r'), (X'3132', 5, 's'), (1e20, 7, 't'), (12, 5, 'u'), (12, 0.0, 'v')",
        ];
        for statement in setup {
            f.execute(statement).await.unwrap();
            r.execute_batch(statement).unwrap();
        }
        let queries = [
            "SELECT id, x % y, typeof(x % y) FROM p ORDER BY id",
            "SELECT id, s, x % 3, typeof(x % 3), y % x FROM p ORDER BY id",
            "SELECT id FROM p WHERE x % y = 1 ORDER BY id",
            "SELECT id FROM p WHERE x % y = 0 ORDER BY id",
            "SELECT id FROM p WHERE id % 3 = 0 ORDER BY id",
            "SELECT count(*), sum(id % 7), sum(rowid % 4), total(x % y) FROM p",
            "SELECT id, rowid, typeof(rowid), rowid % 5 FROM p ORDER BY id",
        ];
        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} remainder divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
