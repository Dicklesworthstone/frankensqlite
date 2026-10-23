#![recursion_limit = "512"]

//! bd-printf-unsupported-spec-null-f46wo: printf()/format() must match stock
//! SQLite's conversion-specifier partition exactly. Differential vs rusqlite
//! (bundled SQLite 3.53, the authoritative oracle — the sqlite3 CLI 3.46.1 is a
//! known float-text outlier, but the specifier partition is stable across both).
//!
//! Four contracts:
//!   1. Supported specifiers (% n d i u f e E g G s z q Q w x X o c p r) format
//!      identically to stock — INCLUDING the two frank previously emitted as raw
//!      literals: `%p` (== `%X`, uppercase hex) and `%r` (ordinal, 255->'255th').
//!   2. Unsupported/unknown specifiers (a, b, h, j, k, m, t, v, y and the
//!      unsupported uppercase set) stop formatting: stock keeps the text
//!      accumulated before them, and an empty accumulation is NULL — frank
//!      previously emitted a raw `%<spec>` literal, then NULLed even a prefix.
//!   3. Positional args (`%2$s`) behave the same way (the `$` reaches the
//!      conversion slot after the digits parse as field width).
//!   4. `l` and `ll` are length modifiers (`%lld` == `%d`), not conversions.
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
        for arg in [
            "255",
            "-1",
            "0",
            "3.5",
            "'x'",
            "'0x1F'",
            "NULL",
            "4294967296",
        ] {
            sqls.push(format!("SELECT quote(printf('%p', {arg}))"));
            sqls.push(format!("SELECT quote(printf('%r', {arg}))"));
        }
        // %r ordinal-suffix edge coverage (11/12/13 -> th; 21/101/111 boundaries).
        for n in [
            0, 1, 2, 3, 4, 11, 12, 13, 21, 22, 23, 100, 101, 111, 112, 113, -2,
        ] {
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
            "%d", "%i", "%u", "%x", "%X", "%o", "%c", "%s", "%z", "%q", "%Q", "%w", "%f", "%e",
            "%E", "%g", "%G", "%n", "%%", "%!d", "%,d", "%+d", "%05d", "%-8d|", "%5.2f", "%.0f",
        ] {
            sqls.push(format!("SELECT quote(printf('{f}', 1234))"));
        }
        assert_all_agree(&sqls).await;
    });
}

#[test]
fn printf_length_modifier_is_a_no_op_like_stock() {
    asupersync::test_utils::run_test(|| async {
        // (4): for every supported conversion, flag/width/precision prefix and
        // argument type, `%<spec>l<conv>` and `%<spec>ll<conv>` must render
        // exactly like `%<spec><conv>` — on stock (sanity check of the premise)
        // and on frank. Frank-vs-stock parity of the unmodified forms is the job
        // of the tests above, so a pre-existing divergence there cannot make
        // this property test fail for an unrelated reason.
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut failures = Vec::new();
        let mut checked = 0;
        for conv in ["d", "i", "u", "x", "X", "o", "c", "s", "q", "f", "e", "g"] {
            for spec in ["", "5", "-5", "05", ".3", ",", "+"] {
                for arg in ["9223372036854775807", "-1", "42", "3.5", "'txt'"] {
                    let plain = format!("SELECT quote(printf('<%{spec}{conv}>', {arg}))");
                    let frank_plain = frank(&f, &plain).await;
                    let stock_plain = oracle(&r, &plain);
                    for modifier in ["l", "ll"] {
                        let sql =
                            format!("SELECT quote(printf('<%{spec}{modifier}{conv}>', {arg}))");
                        checked += 1;
                        let stock = oracle(&r, &sql);
                        if stock != stock_plain {
                            failures.push(format!(
                                "  premise: {sql}\n    stock={stock} vs {stock_plain}"
                            ));
                        }
                        let fv = frank(&f, &sql).await;
                        if fv != frank_plain {
                            failures.push(format!(
                                "  {sql}\n    frank={fv} but without the modifier {frank_plain}"
                            ));
                        }
                    }
                }
            }
        }
        f.close().await.unwrap();
        eprintln!(
            "event=printf_length_modifier_property checked={checked} failures={}",
            failures.len()
        );
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    });
}

#[test]
fn printf_length_modifiers_and_text_before_unknown_spec_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut sqls: Vec<String> = [
            "SELECT quote(printf('%lld', 9223372036854775807))",
            "SELECT quote(printf('%ld|%li|%lu|%llu', 5, -5, -1, 42))",
            "SELECT quote(printf('%llx|%lX|%llo', 255, 255, 8))",
            "SELECT quote(printf('%5ld|%-5lld|%05ld|%.3ld|%,lld', 42, 42, 42, 7, 1234567))",
            "SELECT quote(printf('%lf|%lle|%lg|%ls|%lq', 1.5, 2.5, 3.5, 'x', 'it''s'))",
            "SELECT quote(format('%lld rows', 3))",
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        // (2)+(3): text accumulated before an unknown or positional conversion
        // is kept; only an empty accumulation is NULL.
        for p in ["x%a", "ab%d|%y|tail", "a%2$s", "%d%k%d", "pre %lly", "%a"] {
            sqls.push(format!("SELECT quote(printf('{p}', 5, 6))"));
        }
        assert_all_agree(&sqls).await;
    });
}
