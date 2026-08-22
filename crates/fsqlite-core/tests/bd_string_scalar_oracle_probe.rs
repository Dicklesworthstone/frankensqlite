#![recursion_limit = "512"]

//! String scalar-function leaf-hunt (pane af49, 2026-08-22): frank vs rusqlite
//! over substr (negative/zero/oob indices, 2-arg + 3-arg), replace (incl empty
//! needle), instr, trim/ltrim/rtrim (default + custom char set), printf/format
//! specifiers (%d %s %x %o %e %g %c %% width/precision/flags), char(), unicode(),
//! hex(), unhex(), quote() over text/int/real/NULL/blob, upper/lower (ASCII),
//! length vs octet-ish, and || concat coercion. Compared via quote() so TEXT and
//! BLOB round-trip apples-to-apples. Pass = coverage keeper; a mismatch is a leaf.

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

#[test]
fn string_scalar_functions_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        // compare a scalar expr across engines via quote() so text/blob are exact
        macro_rules! q {
            ($d:expr, $expr:expr) => {{
                let sql = concat!("SELECT quote(", $expr, ")");
                let fr = fq(&f, sql).await;
                let rr = rq(&r, sql);
                if fr != rr {
                    $d.push(format!("  [{}]\n     frank= {:?}\n     stock= {:?}", $expr, fr, rr));
                }
            }};
        }

        // --- substr: negative / zero / out-of-range indices ---
        q!(diffs, "substr('abcdef', 2, 3)");
        q!(diffs, "substr('abcdef', -2)");
        q!(diffs, "substr('abcdef', -2, 1)");
        q!(diffs, "substr('abcdef', -3, 2)");
        q!(diffs, "substr('abcdef', 0)");
        q!(diffs, "substr('abcdef', 0, 2)");     // count includes the phantom pos-0
        q!(diffs, "substr('abcdef', 2, -1)");    // negative length -> chars before start
        q!(diffs, "substr('abcdef', 4, -2)");
        q!(diffs, "substr('abcdef', 10)");
        q!(diffs, "substr('abcdef', 2, 100)");
        q!(diffs, "substr('héllo', 2, 2)");       // multibyte char indexing

        // --- replace (incl empty needle -> stock returns original) ---
        q!(diffs, "replace('aXbXc', 'X', '-')");
        q!(diffs, "replace('aXbXc', 'X', '')");
        q!(diffs, "replace('abc', '', 'Z')");
        q!(diffs, "replace('aaa', 'a', 'aa')");

        // --- instr ---
        q!(diffs, "instr('abcabc', 'bc')");
        q!(diffs, "instr('abcabc', 'x')");
        q!(diffs, "instr('abc', '')");

        // --- trim / ltrim / rtrim, default + custom set ---
        q!(diffs, "trim('  hi  ')");
        q!(diffs, "ltrim('xxhi', 'x')");
        q!(diffs, "rtrim('hixx', 'x')");
        q!(diffs, "trim('xyhixy', 'xy')");
        q!(diffs, "trim('abchi cba', 'abc')");

        // --- printf / format specifiers ---
        q!(diffs, "printf('%d', 42)");
        q!(diffs, "printf('%5d', 42)");
        q!(diffs, "printf('%-5d|', 42)");
        q!(diffs, "printf('%05d', 42)");
        q!(diffs, "printf('%+d', 42)");
        q!(diffs, "printf('%x', 255)");
        q!(diffs, "printf('%o', 8)");
        q!(diffs, "printf('%.2f', 3.14159)");
        q!(diffs, "printf('%e', 12345.678)");
        q!(diffs, "printf('%g', 0.0001)");
        q!(diffs, "printf('%s-%s', 'a', 'b')");
        q!(diffs, "printf('%c', 65)");
        q!(diffs, "printf('%%')");
        // NOTE: printf('%!.20g', 0.1) is a KNOWN, intentionally-unasserted diff —
        // frank emits 0.100000000000000005 (18 sig digits) vs stock's
        // 0.1000000000000000056 (19). This is the REAL->text digit-count artifact
        // (see bd-280m3 WONTFIX / the float-text oracle-version trap), not a leaf.

        // --- char / unicode ---
        q!(diffs, "char(72, 105)");
        q!(diffs, "unicode('A')");
        q!(diffs, "unicode('é')");

        // --- hex / unhex / quote of blobs ---
        q!(diffs, "hex('abc')");
        q!(diffs, "hex(x'0aff')");
        q!(diffs, "unhex('414243')");
        q!(diffs, "unhex('zzz')");                 // invalid -> NULL
        q!(diffs, "quote(x'00ff10')");
        q!(diffs, "quote('it''s')");
        q!(diffs, "quote(NULL)");
        q!(diffs, "quote(42)");

        // --- upper/lower (SQLite is ASCII-only for these) ---
        q!(diffs, "upper('abcé')");
        q!(diffs, "lower('ABCÉ')");

        // --- length (chars) ---
        q!(diffs, "length('héllo')");
        q!(diffs, "length(x'0102')");

        // --- concat coercion ---
        q!(diffs, "'x' || 5 || 'y'");
        q!(diffs, "1 || 2");
        q!(diffs, "'a' || NULL");

        assert!(diffs.is_empty(), "{} string-scalar divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
