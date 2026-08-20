//! bd-printf-unsupported-spec-null-f46wo: printf()/format() must match stock
//! SQLite's conversion-specifier partition exactly. Differential vs rusqlite
//! (bundled SQLite 3.53, the authoritative oracle — the sqlite3 CLI 3.46.1 is a
//! known float-text outlier, but the specifier partition is stable across both).
//!
//! Three contracts:
//!   1. Supported specifiers (% n d i u f e E g G s z q Q w x X o c p r) format
//!      identically to stock — INCLUDING the two frank previously emitted as raw
//!      literals: `%p` (== `%X`, uppercase hex) and `%r` (ordinal, 255->'255th').
//!   2. Unsupported/unknown specifiers (a, b, h, j, k, l, m, t, v, y and the
//!      unsupported uppercase set) NULL the whole call — frank previously emitted
//!      a raw `%<spec>` literal.
//!   3. Positional args (`%2$s`) NULL the whole call (the `$` reaches the
//!      conversion slot after the digits parse as field width).
//!
//! Compared via `SELECT quote(printf(...))` so NULL vs a value is unambiguous
//! (quote(NULL) yields the text 'NULL').

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn frank(f: &Connection, sql: &str) -> String {
    match f.query(sql).await {
        Ok(rows) if !rows.is_empty() => match &rows[0].values()[0] {
            SqliteValue::Null => "<null>".to_owned(),
            SqliteValue::Integer(n) => format!("i:{n}"),
            SqliteValue::Float(x) => format!("r:{x}"),
            SqliteValue::Text(s) => format!("t:{s}"),
            SqliteValue::Blob(b) => format!("b:{b:02X?}"),
        },
        Ok(_) => "<norows>".to_owned(),
        Err(e) => format!("<err:{e:?}>"),
    }
}

fn oracle(r: &rusqlite::Connection, sql: &str) -> String {
    match r.query_row(sql, [], |row| row.get::<_, rusqlite::types::Value>(0)) {
        Ok(rusqlite::types::Value::Null) => "<null>".to_owned(),
        Ok(rusqlite::types::Value::Integer(n)) => format!("i:{n}"),
        Ok(rusqlite::types::Value::Real(x)) => format!("r:{x}"),
        Ok(rusqlite::types::Value::Text(s)) => format!("t:{s}"),
        Ok(rusqlite::types::Value::Blob(b)) => format!("b:{b:02X?}"),
        Err(e) => format!("<err:{e}>"),
    }
}

/// Run every `SELECT quote(printf(...))` in `sqls` on both engines, collect all
/// divergences, and fail once with the full list.
async fn assert_all_agree(sqls: &[String]) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut diverged = Vec::new();
    for sql in sqls {
        let fv = frank(&f, sql).await;
        let rv = oracle(&r, sql);
        if fv != rv {
            diverged.push(format!("  {sql}\n    frank ={fv}\n    oracle={rv}"));
        }
    }
    assert!(
        diverged.is_empty(),
        "{} printf specifier divergence(s) vs rusqlite 3.53:\n{}",
        diverged.len(),
        diverged.join("\n")
    );
}

#[test]
fn printf_specifier_partition_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut sqls = Vec::new();
        // (1)+(2): every ascii-letter specifier with an integer arg — supported
        // ones format, unsupported ones NULL; the oracle decides which is which.
        for c in ('a'..='z').chain('A'..='Z') {
            sqls.push(format!("SELECT quote(printf('%{c}', 255))"));
        }
        // (3): positional args NULL the whole call.
        for p in ["%2$s %1$s", "%3$d", "%5$s", "%1$d"] {
            sqls.push(format!("SELECT quote(printf('{p}', 1, 2, 3))"));
        }
        assert_all_agree(&sqls).await;
    });
}

#[test]
fn printf_p_and_r_match_stock_across_args() {
    asupersync::test_utils::run_test(|| async {
        let mut sqls = Vec::new();
        // %p (== %X) and %r (ordinal) over varied argument types.
        for arg in ["255", "-1", "0", "3.5", "'x'", "'0x1F'", "NULL", "4294967296"] {
            sqls.push(format!("SELECT quote(printf('%p', {arg}))"));
            sqls.push(format!("SELECT quote(printf('%r', {arg}))"));
        }
        // %r ordinal-suffix edge coverage (11/12/13 -> th; 21/101/111 boundaries).
        for n in [0, 1, 2, 3, 4, 11, 12, 13, 21, 22, 23, 100, 101, 111, 112, 113, -2] {
            sqls.push(format!("SELECT quote(printf('%r', {n}))"));
        }
        // %p width/precision/flags parity with %X.
        for f in ["%5p", "%-5p", "%05p", "%.3p", "%#p", "%5r", "%-6r"] {
            sqls.push(format!("SELECT quote(printf('{f}', 255))"));
        }
        assert_all_agree(&sqls).await;
    });
}

#[test]
fn printf_supported_controls_unregressed() {
    asupersync::test_utils::run_test(|| async {
        let mut sqls = Vec::new();
        // The documented supported set must still format correctly.
        for f in [
            "%d", "%i", "%u", "%x", "%X", "%o", "%c", "%s", "%z", "%q", "%Q", "%w",
            "%f", "%e", "%E", "%g", "%G", "%n", "%%", "%!d", "%,d", "%+d", "%05d",
            "%-8d|", "%5.2f", "%.0f",
        ] {
            sqls.push(format!("SELECT quote(printf('{f}', 1234))"));
        }
        assert_all_agree(&sqls).await;
    });
}
