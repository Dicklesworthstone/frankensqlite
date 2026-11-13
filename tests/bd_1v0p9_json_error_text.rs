#![recursion_limit = "512"]

//! bd-1v0p9: the JSON functions report stock's error texts and follow stock's
//! argument rules. Malformed input is "malformed JSON", an unparseable PATH is
//! "bad JSON path: 'p'" (a number used as a PATH is read as its text), and
//! json_set/json_insert/json_replace take any odd argument count (one returns
//! the input) and skip a pair whose PATH is NULL. Every statement's result or
//! error text is compared with rusqlite's bundled SQLite.

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

async fn fq(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|r| r.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

fn rq(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut st = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

const QUERIES: &[&str] = &[
    // Malformed input, text and blob, through every kind of consumer.
    "SELECT json('{')",
    "SELECT json('')",
    "SELECT json('[1,]')",
    "SELECT jsonb('{')",
    "SELECT json(x'7b')",
    "SELECT json(x'ff')",
    "SELECT json_extract('{', '$')",
    "SELECT jsonb_extract('{', '$')",
    "SELECT json_type('{', '$')",
    "SELECT json_array_length('x')",
    "SELECT json_patch('{', '{}')",
    "SELECT json_patch('{}', '{')",
    "SELECT json_pretty('{')",
    "SELECT json_set('{', '$.a', 1)",
    "SELECT json_quote(json('{'))",
    "SELECT '{' -> '$.a'",
    "SELECT count(*) FROM json_each('{')",
    "SELECT json_group_array(json('{')) FROM (SELECT 1)",
    // Unparseable paths.
    "SELECT json_extract('[1]', 'x')",
    "SELECT json_extract('[1]', '$[')",
    "SELECT json_extract('[1]', '$.')",
    "SELECT json_extract('[1]', '$.\"a')",
    "SELECT json_extract('[1]', '$[1')",
    "SELECT json_extract('[1]', '$a')",
    "SELECT json_extract('[1]', 'it''s')",
    "SELECT json_extract('{\"a\":1}', '$.a', 'y')",
    "SELECT json_set('{}', 'x', 1)",
    "SELECT json_remove('[1]', 'x')",
    "SELECT json_replace('[1]', '$[x]', 1)",
    "SELECT json_array_length('[1]', 'x')",
    "SELECT '[1]' ->> '$x'",
    "SELECT count(*) FROM json_tree('[1]', 'x')",
    "SELECT count(*) FROM json_each('[1]', 'x')",
    // A number as a PATH is read as its text; a BLOB as its bytes.
    "SELECT json_extract('[1]', 5)",
    "SELECT json_extract('[1]', 1.5)",
    "SELECT json_extract('{\"a\":1}', x'242e61')",
    "SELECT json_set('{}', 5, 1)",
    "SELECT json_remove('[1]', 5)",
    "SELECT json_type('[1]', 5)",
    "SELECT json_array_length('[1]', 5)",
    "SELECT count(*) FROM json_each('[1]', 5)",
    // An array subscript is ASCII digits; one too large for any array finds
    // nothing.
    "SELECT json_extract('[1]', '$[+0]')",
    "SELECT json_extract('[1,2]', '$[#-+1]')",
    "SELECT json_extract('[1,2]', '$[ 1]')",
    "SELECT json_extract('[1,2]', '$[01]')",
    "SELECT json_extract('[1]', '$[99999999999999999999999]')",
    "SELECT json_extract('[1,2]', '$[#-99999999999999999999999]')",
    "SELECT json_set('[1]', '$[99999999999999999999999]', 2)",
    // `$[#-0]` is the slot `$[#]` names.
    "SELECT json_extract('[1]', '$[#-0]')",
    "SELECT json_extract('[1]', '$[#-0]', '$[0]')",
    "SELECT json_type('[1]', '$[#-0]')",
    "SELECT json_set('[1]', '$[#-0]', 2)",
    "SELECT json_insert('[1]', '$[#-0]', 2)",
    // The -> and ->> abbreviations.
    "SELECT '[5,6]' -> '[1]'",
    "SELECT '{\"1.5\":7}' -> 1.5",
    "SELECT '{\"a\":1}' -> x'61'",
    "SELECT '[5,6]' -> -1",
    "SELECT '{\"a b\":1}' ->> 'a b'",
    // json_set / json_insert / json_replace arity and NULL paths.
    "SELECT json_set(' { } ')",
    "SELECT hex(jsonb_set(' {} '))",
    "SELECT json_insert(' [ 1 ] ')",
    "SELECT json_replace(' [1] ')",
    "SELECT json_insert('{}', '$.a')",
    "SELECT jsonb_set('{}', '$.a')",
    "SELECT json_set('{}', '$.a', 1, '$.b')",
    "SELECT json_set(NULL, 1)",
    "SELECT json_set(NULL)",
    "SELECT json_set()",
    "SELECT json_set('{}', NULL, 1)",
    "SELECT json_set('{}', '$.a', 1, NULL, 2)",
    "SELECT json_replace('{\"a\":1}', NULL, 2, '$.a', 3)",
    "SELECT json(json_set('{}', NULL, 1, '$.b', json('[2]')))",
    // Zero-argument calls of the variadic functions.
    "SELECT json_extract()",
    "SELECT json_extract('[1]')",
    "SELECT json_remove()",
    "SELECT json_remove(' [ 1 ] ')",
    "SELECT json_remove('[1,2]', NULL)",
    // json_object labels and arity.
    "SELECT json_object('a')",
    "SELECT jsonb_object('a')",
    "SELECT json_object(1, 2)",
    "SELECT jsonb_object(1, 2)",
    "SELECT json_object(NULL, 2)",
    // json_group_object reads its label as text and skips a NULL label.
    "SELECT json_group_object(k, v) FROM (SELECT 1 AS k, 2 AS v)",
    "SELECT json_group_object(k, v) FROM (SELECT NULL AS k, 2 AS v)",
    "SELECT json_group_object(k, v) FROM \
     (SELECT 1 AS k, 2 AS v UNION ALL SELECT NULL, 3 UNION ALL SELECT 'a', 4)",
    "SELECT json_group_object(k, v) FROM (SELECT 1.5 AS k, 'x' AS v)",
    // json_valid FLAGS.
    "SELECT json_valid('{', 99)",
    "SELECT json_valid('{', 0)",
    "SELECT json_valid('{', -1)",
    "SELECT json_valid('{', 300)",
    "SELECT json_valid('{}', 15)",
];

#[test]
fn json_errors_and_argument_rules_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let mut diffs = Vec::new();
        for q in QUERIES {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} of {} JSON error/argument divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            QUERIES.len(),
            diffs.join("\n")
        );
    });
}
