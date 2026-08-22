//! Differential oracle: string + printf functions vs rusqlite (bundled SQLite
//! 3.53). A probe sweep found this surface stock-correct across 22 cases; this
//! keeper locks it in.
//!
//! Covers substr with 1-based / zero / negative start, negative length, and
//! past-end (empty); instr, replace, trim/ltrim/rtrim (default and charset),
//! upper/lower, length, hex, quote, char, unicode; printf integer specifiers
//! with width/left/zero padding, hex/octal, string specifiers with width, an
//! explicit-precision float (`%.2f`/`%.0f`/`%e`), `%%`, `%c`; `||` NULL
//! propagation; and the result affinities via typeof.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
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
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn agree(sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(
        fr, rr,
        "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

#[test]
fn substr_indexing() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT substr('hello', 2, 3), substr('hello', 2), substr('hello', 2, 99)",
            "substr basic + open-ended + past-end length",
        )
        .await;
        agree("SELECT substr('hello', 0, 3)", "substr with start 0").await;
        agree(
            "SELECT substr('hello', -3, 2)",
            "substr with negative start (from end)",
        )
        .await;
        agree(
            "SELECT substr('hello', 4, -2)",
            "substr with negative length",
        )
        .await;
        agree(
            "SELECT substr('hi', 5, 3)",
            "substr starting past the end is empty",
        )
        .await;
    });
}

#[test]
fn instr_replace_trim() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT instr('hello world', 'o'), instr('hello', 'z'), instr('abcabc', 'bc')",
            "instr",
        )
        .await;
        agree(
            "SELECT replace('aXbXc', 'X', '-'), replace('aaa', 'a', 'bb'), replace('x', 'y', 'z')",
            "replace",
        )
        .await;
        agree(
            "SELECT '['||trim('  hi  ')||']', '['||ltrim('  hi')||']', '['||rtrim('hi  ')||']'",
            "trim/ltrim/rtrim default",
        )
        .await;
        agree(
            "SELECT trim('xxhixx', 'x'), ltrim('...hi', '.'), rtrim('hi!!!', '!')",
            "trim/ltrim/rtrim with charset",
        )
        .await;
    });
}

#[test]
fn case_length_hex_quote_char() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT upper('Hello123'), lower('Hello123')",
            "upper/lower ASCII",
        )
        .await;
        agree(
            "SELECT length('hello'), length(''), length('cafe')",
            "length in characters",
        )
        .await;
        agree("SELECT hex('AB'), hex(255)", "hex of text and integer").await;
        agree(
            "SELECT quote('a''b'), quote(42), quote(NULL), quote(3.5)",
            "quote of text/int/null/real",
        )
        .await;
        agree(
            "SELECT char(72, 105), unicode('A'), unicode('z')",
            "char/unicode",
        )
        .await;
    });
}

#[test]
fn printf_specifiers() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT printf('%d|%5d|%-5d|%05d', 42, 42, 42, 42)",
            "printf integer width/left/zero padding",
        )
        .await;
        agree("SELECT printf('%x %X %o', 255, 255, 8)", "printf hex/octal").await;
        agree(
            "SELECT printf('[%s][%10s][%-10s]', 'hi', 'hi', 'hi')",
            "printf string width",
        )
        .await;
        agree(
            "SELECT printf('%.2f %.0f %e', 3.14159, 2.5, 12345.0)",
            "printf explicit-precision float",
        )
        .await;
        agree("SELECT printf('100%% of %d', 5)", "printf percent literal").await;
        agree("SELECT printf('%c%c', 72, 105)", "printf %c").await;
    });
}

#[test]
fn concat_and_typeof() {
    asupersync::test_utils::run_test(|| async {
        agree(
            "SELECT 'a' || NULL, 'a' || 'b' || 'c'",
            "|| NULL propagation",
        )
        .await;
        agree(
            "SELECT typeof(substr('x',1)), typeof(length('x')), typeof(printf('%d',1))",
            "result affinities",
        )
        .await;
    });
}
