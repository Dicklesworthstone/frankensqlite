#![recursion_limit = "512"]

//! bd-5ul61: json_array_insert, jsonb_array_insert, jsonb_group_array and
//! jsonb_group_object exist and behave as in stock. json_array_insert inserts
//! before the array element a PATH names (appending at the slot past the end),
//! and a PATH ending in an object label is "not an array element" once the
//! lookup reaches an object or a member it would create. The jsonb_ aggregates
//! build what their json_ forms build and return it as JSONB, keeping duplicate
//! labels, as jsonb_object now does too. Every statement's result or error text
//! is compared with rusqlite's bundled SQLite.

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

/// Stock's error message. For an error found while preparing, rusqlite's
/// Display appends the SQL text and the error offset; keep the message alone.
fn stock_err(e: rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqlInputError { msg, .. } => msg,
        other => other.to_string(),
    }
}

fn rq(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut st = conn.prepare(sql).map_err(stock_err)?;
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

const SETUP: &[&str] = &[
    "CREATE TABLE t(k INTEGER, v)",
    "INSERT INTO t VALUES (1, 'a'), (1, 'b'), (2, NULL), (2, 3.5), (3, '[1]'), (3, 7)",
    "CREATE TABLE e(id INTEGER PRIMARY KEY, j TEXT, p TEXT)",
    "INSERT INTO e VALUES (1, '[1,2]', '$[1]'), (2, '{\"a\":[1]}', '$.a[#]'), \
     (3, '[1,2]', '$[9]'), (4, '[[0]]', '$[0][0]')",
];

const QUERIES: &[&str] = &[
    // Insert before the indexed element; append at the slot past the end.
    "SELECT json_array_insert('[1,2]', '$[0]', 9)",
    "SELECT json_array_insert('[1,2]', '$[1]', 9)",
    "SELECT json_array_insert('[1,2]', '$[2]', 9)",
    "SELECT json_array_insert('[1,2]', '$[3]', 9)",
    "SELECT json_array_insert('[1,2]', '$[#]', 9)",
    "SELECT json_array_insert('[1,2]', '$[#-0]', 9)",
    "SELECT json_array_insert('[1,2]', '$[#-1]', 9)",
    "SELECT json_array_insert('[1,2]', '$[#-2]', 9)",
    "SELECT json_array_insert('[1,2]', '$[#-3]', 9)",
    "SELECT json_array_insert('[]', '$[0]', 9)",
    "SELECT json_array_insert('[[1],2]', '$[0][0]', 9)",
    "SELECT json_array_insert('[[1],2]', '$[0][1]', 9)",
    "SELECT json_array_insert('{\"a\":[1]}', '$.a[0]', 9)",
    "SELECT json_array_insert('{\"a\":[1]}', '$.a[#]', 9)",
    "SELECT json_array_insert('[1,2]', '$', 9)",
    "SELECT json_array_insert('{}', '$', 9)",
    "SELECT json_array_insert('5', '$[0]', 9)",
    // Several pairs, applied in order; a NULL PATH skips its pair.
    "SELECT json_array_insert('[1,2]', '$[0]', 9, '$[0]', 8)",
    "SELECT json_array_insert('[1,2]', '$[1]', 9, NULL, 8, '$[#]', 7)",
    "SELECT json_array_insert('[1,2]', NULL, 9)",
    // The VALUE: SQL types, and JSON values embedded rather than quoted.
    "SELECT json_array_insert('[1,2]', '$[1]', json('[3]'))",
    "SELECT json_array_insert('[1,2]', '$[1]', json_object('x', 1))",
    "SELECT json_array_insert('[1,2]', '$[1]', '[3]')",
    "SELECT json_array_insert('[1,2]', '$[1]', NULL)",
    "SELECT json_array_insert('[1,2]', '$[1]', 1.5)",
    "SELECT json_array_insert('[1,2]', '$[1]', jsonb('{\"y\":2}'))",
    // A PATH ending in an object label.
    "SELECT json_array_insert('{\"a\":1}', '$.a', 9)",
    "SELECT json_array_insert('{}', '$.a', 9)",
    "SELECT json_array_insert('{\"a\":{}}', '$.a.b', 9)",
    "SELECT json_array_insert('{}', '$.x.a', 9)",
    "SELECT json_array_insert('{\"a\":1}', '$.a.b', 9)",
    "SELECT json_array_insert('{\"a\":null}', '$.a.b', 9)",
    "SELECT json_array_insert('[1]', '$.a', 9)",
    "SELECT json_array_insert('[1]', '$[0].a', 9)",
    "SELECT json_array_insert('[{}]', '$[0].a', 9)",
    "SELECT json_array_insert('[{}]', '$[1].a', 9)",
    "SELECT json_array_insert('[{}]', '$[5].a', 9)",
    "SELECT json_array_insert('[{}]', '$[#].a', 9)",
    "SELECT json_array_insert('[{}]', '$[#-1].a', 9)",
    "SELECT json_array_insert('[{}]', '$[#-2].a', 9)",
    "SELECT json_array_insert('{\"q''t\":{}}', '$.\"q''t\".z', 9)",
    "SELECT json_array_insert('[1]', '$[0]', 1, '$.a', 2)",
    // Missing parents are built when the PATH ends in a subscript.
    "SELECT json_array_insert('{}', '$.a[0]', 9)",
    "SELECT json_array_insert('{}', '$.a[1]', 9)",
    "SELECT json_array_insert('{}', '$.a[#]', 9)",
    "SELECT json_array_insert('{}', '$.a.b[0]', 9)",
    "SELECT json_array_insert('[]', '$[0][0]', 9)",
    "SELECT json_array_insert('[]', '$[1][0]', 9)",
    "SELECT json_array_insert('{\"a\":null}', '$.a[0]', 9)",
    "SELECT json_array_insert('{\"a\":1}', '$.a[0]', 9)",
    "SELECT json_array_insert('{\"a\":{}}', '$.a[0]', 9)",
    // Arity, NULL and malformed input, bad paths.
    "SELECT json_array_insert('[1]', '$[0]')",
    "SELECT jsonb_array_insert('[1]', '$[0]')",
    "SELECT json_array_insert('[1]')",
    "SELECT json_array_insert()",
    "SELECT json_array_insert(NULL, '$[0]', 1)",
    "SELECT json_array_insert('[1', '$[0]', 1)",
    "SELECT json_array_insert('[1]', 'x', 1)",
    "SELECT json_array_insert('[1]', 1, 1)",
    // The jsonb_ form, and the JSON result subtype of the json_ form.
    // (A SQL text VALUE is left to json() here: stock stores it in JSONB as
    // TEXTRAW, as for every jsonb_ edit function, and FrankenSQLite as TEXT.)
    "SELECT jsonb_array_insert('[1,2]', '$[1]', 9)",
    "SELECT hex(jsonb_array_insert('[1,[2]]', '$[1][0]', 9.5))",
    "SELECT json(jsonb_array_insert('[1,2]', '$[1]', 'x'))",
    "SELECT json(jsonb_array_insert('{\"a\":[1]}', '$.a[0]', 9))",
    "SELECT json_array_insert(jsonb('[1,2]'), '$[1]', 9)",
    "SELECT json_array(json_array_insert('[1]', '$[0]', 0))",
    "SELECT json_array(jsonb_array_insert('[1]', '$[0]', 0))",
    "SELECT typeof(json_array_insert('[1]', '$[0]', 0)), \
     typeof(jsonb_array_insert('[1]', '$[0]', 0))",
    "SELECT id, json_array_insert(j, p, 9) FROM e ORDER BY id",
    // jsonb_group_array.
    "SELECT hex(jsonb_group_array(x)) FROM (SELECT 1 x UNION ALL SELECT 'a')",
    "SELECT jsonb_group_array(x) FROM (SELECT 1 x UNION ALL SELECT 'a')",
    "SELECT json(jsonb_group_array(x)) FROM \
     (SELECT 1 x UNION ALL SELECT NULL UNION ALL SELECT 2.5)",
    "SELECT hex(jsonb_group_array(x)) FROM (SELECT 1 x WHERE 0)",
    "SELECT hex(jsonb_group_array(json('[1]'))) FROM (SELECT 1)",
    "SELECT hex(jsonb_group_array(jsonb('[1]'))) FROM (SELECT 1)",
    "SELECT jsonb_group_array(1)",
    "SELECT typeof(jsonb_group_array(1))",
    "SELECT json_array(jsonb_group_array(1))",
    "SELECT json(jsonb_group_array(DISTINCT x)) FROM (SELECT 1 x UNION ALL SELECT 1)",
    "SELECT json(jsonb_group_array(x ORDER BY x DESC)) FROM (SELECT 1 x UNION ALL SELECT 2)",
    "SELECT jsonb_group_array(1, 2)",
    "SELECT jsonb_group_array()",
    "SELECT hex(jsonb_group_array(v)) FROM t",
    "SELECT k, hex(jsonb_group_array(v)) FROM t GROUP BY k ORDER BY k",
    "SELECT k, json(jsonb_group_array(v)) FROM t GROUP BY k HAVING count(*) > 1 ORDER BY k",
    "SELECT t.k, hex(jsonb_group_array(u.v)) FROM t JOIN t u ON u.rowid = t.rowid \
     GROUP BY t.k ORDER BY t.k",
    "SELECT hex(jsonb_group_array(u.v)) FROM t JOIN t u ON u.rowid = t.rowid",
    // jsonb_group_object.
    "SELECT hex(jsonb_group_object('k', 2)) FROM (SELECT 1)",
    "SELECT json(jsonb_group_object(k, v)) FROM (SELECT 'a' k, 1 v \
     UNION ALL SELECT 'b', NULL UNION ALL SELECT NULL, 3 UNION ALL SELECT 4, 5)",
    "SELECT hex(jsonb_group_object(k, v)) FROM (SELECT 'a' k, 1 v WHERE 0)",
    "SELECT hex(jsonb_group_object('a', json('{\"b\":1}')))",
    "SELECT hex(jsonb_group_object(k, v)) FROM (SELECT 'a' k, 1 v UNION ALL SELECT 'a', 2)",
    "SELECT jsonb_group_object('a')",
    "SELECT typeof(jsonb_group_object('a', 1))",
    "SELECT hex(jsonb_group_object(v, k)) FROM t",
    "SELECT k, hex(jsonb_group_object(v, k)) FROM t GROUP BY k ORDER BY k",
    "SELECT t.k, hex(jsonb_group_object(u.v, u.k)) FROM t JOIN t u ON u.rowid = t.rowid \
     GROUP BY t.k ORDER BY t.k",
    // The grouped join evaluator reads a JSON-valued json_group_object's label
    // the way the aggregate does: as text, skipping a NULL label.
    "SELECT json_group_object(t.k, json_quote(u.v)) FROM t JOIN t u ON u.rowid = t.rowid",
    "SELECT json_group_object(nullif(t.k, 2), json_array(u.v)) FROM t \
     JOIN t u ON u.rowid = t.rowid",
    "SELECT hex(jsonb_group_object(nullif(t.k, 2), json_array(u.v))) FROM t \
     JOIN t u ON u.rowid = t.rowid",
    // jsonb_object, which builds its object the same way, keeps a duplicate
    // label too.
    "SELECT hex(jsonb_object('a', 1, 'a', 2))",
    "SELECT hex(jsonb_object('a', json('[1]'), 'a', 'x', 'b', NULL))",
    "SELECT hex(jsonb_object('k', 9e999, 'j', -9e999, 'i', 1.5))",
    "SELECT hex(jsonb_object())",
    "SELECT jsonb_object('a')",
    "SELECT jsonb_object(1, 2)",
];

#[test]
fn json_array_insert_and_jsonb_group_aggregates_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SETUP {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }

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
            "{} of {} divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            QUERIES.len(),
            diffs.join("\n")
        );
    });
}
