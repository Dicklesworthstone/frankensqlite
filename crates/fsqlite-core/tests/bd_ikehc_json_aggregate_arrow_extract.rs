#![recursion_limit = "512"]

//! bd-ikehc: the JSON aggregates embed the JSON that `x -> path` returns, and
//! a json_extract() result that is an array or object, as stock does (both
//! carry the JSON subtype; `->>` and a json_extract() scalar or string do
//! not). Covers plain and GROUP BY aggregates, FILTER, DISTINCT, in-aggregate
//! ORDER BY, the jsonb_ forms, and windows (plain, nested in an expression,
//! over GROUP BY, FROM-less). Other aggregates over the same arguments are
//! checked too, as the JSON-subtype routing must leave them alone. Every
//! statement's result or error text is compared with rusqlite's bundled SQLite.

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
    "CREATE TABLE d(id INTEGER PRIMARY KEY, g TEXT, doc TEXT)",
    "INSERT INTO d VALUES \
     (1, 'a', '{\"tags\":[\"x\",\"y\"],\"n\":1,\"s\":\"[5]\",\"o\":{\"k\":1},\"b\":true}'), \
     (2, 'a', '{\"tags\":[],\"n\":2.5,\"s\":\"str\",\"o\":{},\"b\":false}'), \
     (3, 'b', '{\"tags\":[\"z\"],\"n\":null,\"o\":[1,2]}'), \
     (4, 'b', '{\"tags\":[\"x\",\"y\"],\"n\":-7,\"s\":\"\",\"o\":{\"k\":1}}'), \
     (5, 'c', '[1,{\"a\":2}]')",
    "CREATE TABLE bad(id INTEGER PRIMARY KEY, doc TEXT)",
    "INSERT INTO bad VALUES (1, '[1]'), (2, 'not json')",
];

const QUERIES: &[&str] = &[
    // `->` always yields JSON: containers, numbers, strings, booleans, null.
    "SELECT json_group_array(doc -> '$.tags') FROM d",
    "SELECT json_group_array(doc -> '$.n') FROM d",
    "SELECT json_group_array(doc -> '$.s') FROM d",
    "SELECT json_group_array(doc -> '$.b') FROM d",
    "SELECT json_group_array(doc -> '$.o') FROM d",
    "SELECT json_group_array(doc -> '$[1]') FROM d",
    "SELECT json_group_array(doc -> 'tags') FROM d",
    "SELECT json_group_array(doc -> '$') FROM d WHERE id IN (2, 5)",
    // `->>` yields SQL values.
    "SELECT json_group_array(doc ->> '$.tags') FROM d",
    "SELECT json_group_array(doc ->> '$.s') FROM d",
    "SELECT json_group_array(doc ->> '$.b') FROM d",
    // json_extract(): a container is JSON; a scalar or string is an SQL value;
    // several paths make an array.
    "SELECT json_group_array(json_extract(doc, '$.tags')) FROM d",
    "SELECT json_group_array(json_extract(doc, '$.o')) FROM d",
    "SELECT json_group_array(json_extract(doc, '$.n')) FROM d",
    "SELECT json_group_array(json_extract(doc, '$.s')) FROM d",
    "SELECT json_group_array(json_extract(doc, '$.b')) FROM d",
    "SELECT json_group_array(json_extract(doc, '$')) FROM d WHERE id IN (2, 5)",
    "SELECT json_group_array(json_extract(doc, '$.n', '$.s')) FROM d",
    "SELECT json_group_array(json_extract(doc, '$.tags[0]')) FROM d",
    // json_group_object values.
    "SELECT json_group_object(id, doc -> '$.o') FROM d",
    "SELECT json_group_object(id, doc -> '$.s') FROM d",
    "SELECT json_group_object(g, json_extract(doc, '$.o')) FROM d",
    "SELECT json_group_object(id, json_extract(doc, '$.s')) FROM d",
    "SELECT json_group_object(doc ->> '$.s', doc -> '$.n') FROM d",
    // GROUP BY.
    "SELECT g, json_group_array(doc -> '$.tags') FROM d GROUP BY g ORDER BY g",
    "SELECT g, json_group_array(json_extract(doc, '$.tags')) FROM d GROUP BY g ORDER BY g",
    "SELECT g, json_group_array(json_extract(doc, '$.s')) FROM d GROUP BY g ORDER BY g",
    "SELECT g, json_group_object(id, json_extract(doc, '$.o')) FROM d GROUP BY g ORDER BY g",
    "SELECT g, json_group_object(id, doc -> '$.b') FROM d GROUP BY g ORDER BY g",
    "SELECT g, count(*), json_group_array(json_extract(doc, '$.o')), \
     sum(json_extract(doc, '$.n')) FROM d GROUP BY g ORDER BY g",
    "SELECT g, json_group_array(json_extract(doc, '$.o')) FROM d GROUP BY g \
     HAVING count(*) > 1 ORDER BY g",
    // FILTER, DISTINCT, ORDER BY inside the aggregate.
    "SELECT json_group_array(json_extract(doc, '$.tags')) FILTER (WHERE id > 1) FROM d",
    "SELECT json_group_array(DISTINCT json_extract(doc, '$.o')) FROM d",
    "SELECT json_group_array(DISTINCT doc -> '$.tags') FROM d",
    "SELECT json_group_array(doc -> '$.n' ORDER BY id DESC) FROM d",
    "SELECT json_group_array(json_extract(doc, '$.o') ORDER BY g DESC, id) FROM d",
    "SELECT g, json_group_object(id, json_extract(doc, '$.tags') ORDER BY id DESC) \
     FROM d GROUP BY g ORDER BY g",
    // The jsonb_ forms.
    "SELECT hex(jsonb_group_array(doc -> '$.tags')) FROM d",
    "SELECT hex(jsonb_group_array(json_extract(doc, '$.o'))) FROM d",
    "SELECT hex(jsonb_group_array(json_extract(doc, '$.s'))) FROM d",
    "SELECT hex(jsonb_group_object(id, json_extract(doc, '$.o'))) FROM d",
    "SELECT g, hex(jsonb_group_object(id, doc -> '$.n')) FROM d GROUP BY g ORDER BY g",
    // `->` in a window.
    "SELECT id, json_group_array(doc -> '$.tags') OVER (ORDER BY id ROWS 1 PRECEDING) \
     FROM d ORDER BY id",
    "SELECT id, json_group_object(id, doc -> '$.o') OVER (PARTITION BY g) FROM d ORDER BY id",
    "SELECT id, json_group_array(doc ->> '$.s') OVER (ORDER BY id) FROM d ORDER BY id",
    // json_extract() in a window: containers embedded, scalars and strings as
    // SQL values, in every window engine.
    "SELECT id, json_group_array(json_extract(doc, '$.tags')) OVER (ORDER BY id \
     ROWS 1 PRECEDING) FROM d ORDER BY id",
    "SELECT id, json_group_array(json_extract(doc, '$.s')) OVER (ORDER BY id) FROM d ORDER BY id",
    "SELECT id, json_group_array(json_extract(doc, '$.n')) OVER (PARTITION BY g) \
     FROM d ORDER BY id",
    "SELECT id, json_group_array(json_extract(doc, '$.b')) OVER () FROM d ORDER BY id",
    "SELECT id, json_group_array(json_extract(doc, '$.n', '$.o')) OVER (ORDER BY id) \
     FROM d ORDER BY id",
    "SELECT id, json_group_object(id, json_extract(doc, '$.o')) OVER (ORDER BY id \
     ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM d ORDER BY id",
    "SELECT id, json_group_object(json_extract(doc, '$.s'), id) OVER (ORDER BY id) \
     FROM d ORDER BY id",
    "SELECT id, hex(jsonb_group_array(json_extract(doc, '$.o')) OVER (ORDER BY id)) \
     FROM d ORDER BY id",
    "SELECT id, hex(jsonb_group_array(json_extract(doc, '$.s')) OVER (ORDER BY id)) \
     FROM d ORDER BY id",
    "SELECT id, json_extract(json_group_array(json_extract(doc, '$.tags')) \
     OVER (ORDER BY id), '$[0][0]') FROM d ORDER BY id",
    "SELECT g, json_group_array(json_extract(max(doc), '$.o')) OVER (ORDER BY g) \
     FROM d GROUP BY g ORDER BY g",
    "SELECT json_group_array(json_extract('{\"a\":[1]}', '$.a')) OVER ()",
    "SELECT json_group_array(json_extract('{\"a\":\"[1]\"}', '$.a')) OVER ()",
    "SELECT json_group_array(json_extract(doc, '$')) OVER () FROM bad",
    // Other aggregates over the same arguments.
    "SELECT g, sum(json_extract(doc, '$.n')), max(doc -> '$.n'), \
     group_concat(json_extract(doc, '$.o')) FROM d GROUP BY g ORDER BY g",
    "SELECT count(doc -> '$.tags'), count(json_extract(doc, '$.s')) FROM d",
    "SELECT g, count(DISTINCT json_extract(doc, '$.o')) FROM d GROUP BY g ORDER BY g",
    // Errors.
    "SELECT json_group_array(json_extract(doc, '$')) FROM bad",
    "SELECT json_group_array(doc -> '$') FROM bad",
    "SELECT json_group_array(json_extract(doc, 'x')) FROM d",
];

#[test]
fn json_aggregates_embed_arrow_and_json_extract_containers_like_stock() {
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
