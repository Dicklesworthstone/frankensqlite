#![recursion_limit = "512"]

//! String/text scalar-function edge leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the fiddly corners of SQLite's text builtins — substr with
//! 1-based, negative (from-end), zero, and overrun indices; the 2-arg substr
//! (to end); replace including empty-needle and overlapping; trim/ltrim/rtrim
//! with a custom trim-character set; instr (incl empty needle and not-found);
//! char() / unicode(); hex() / unhex() (incl odd-length and invalid); quote()
//! over text/blob/null/real; length vs octet_length on multibyte; and the
//! likely()/unlikely()/likelihood() no-op wrappers. Scalar results compared.
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
fn string_function_edges_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // substr: 1-based indexing
            "SELECT substr('abcdef', 1, 3), substr('abcdef', 2, 3), substr('abcdef', 4)",
            // substr negative start = counts from the end
            "SELECT substr('abcdef', -2), substr('abcdef', -3, 2), substr('abcdef', -10, 3)",
            // substr zero / overrun / zero-length
            "SELECT substr('abcdef', 0, 3), substr('abcdef', 3, 0), substr('abcdef', 3, 100)",
            // substr negative length (window extends leftward from start)
            "SELECT substr('abcdef', 4, -2), substr('abcdef', -2, -2)",
            // substr on empty and past-end
            "SELECT substr('', 1, 3), substr('abc', 10, 2)",
            // replace: normal, empty needle (no-op), full replacement
            "SELECT replace('aXbXc', 'X', '-'), replace('abc', '', 'Z'), replace('aaa', 'a', 'bb')",
            "SELECT replace('mississippi', 'iss', 'IS')",
            // trim / ltrim / rtrim with default whitespace
            "SELECT '['||trim('  hi  ')||']', '['||ltrim('  hi  ')||']', '['||rtrim('  hi  ')||']'",
            // trim with a custom character set
            "SELECT trim('xxhixx','x'), ltrim('xyxhi','xy'), rtrim('hixyx','xy')",
            "SELECT trim('abcba','ab')",
            // instr: found, not-found, empty needle, needle at start
            "SELECT instr('abcabc','bc'), instr('abcabc','z'), instr('abc',''), instr('abc','a')",
            // instr with a blob? keep to text; instr of substring past
            "SELECT instr('hello world','o'), instr('hello world','world')",
            // char() builds a string from unicode code points
            "SELECT char(72,105), char(0x263A), char(65,66,67)",
            // unicode() returns the code point of the first char
            "SELECT unicode('A'), unicode('abc'), unicode('☺')",
            // hex() over text and integer-derived blobs
            "SELECT hex('abc'), hex(''), hex(x'00ff10')",
            // unhex() (3.41+) valid, odd-length -> NULL, invalid -> NULL
            "SELECT unhex('616263'), unhex('6'), unhex('zz'), unhex('')",
            // quote() over the storage classes
            "SELECT quote('a''b'), quote(NULL), quote(42), quote(3.5), quote(x'00ff')",
            // length vs multibyte; length counts characters, not bytes
            "SELECT length('abc'), length('☺'), length('a☺b'), length('')",
            // upper / lower (ASCII only in SQLite core)
            "SELECT upper('aBc'), lower('AbC'), upper('café')",
            // printf-ish via format() alias (3.38+): basic
            "SELECT format('%d-%s', 7, 'x'), format('%.2f', 3.14159)",
            // likely / unlikely / likelihood are no-op passthroughs
            "SELECT likely(5), unlikely('x'), likelihood(9, 0.5)",
            // concatenation with NULL yields NULL
            "SELECT 'a' || NULL, NULL || 'b', 'a' || 1 || 'b'",
            // typeof over string function outputs
            "SELECT typeof(substr('abc',1,1)), typeof(hex('a')), typeof(length('a'))",
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
            "{} string-function divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
