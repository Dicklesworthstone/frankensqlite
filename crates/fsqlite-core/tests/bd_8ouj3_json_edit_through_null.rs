#![recursion_limit = "512"]

//! bd-8ouj3: json_set / json_insert / json_replace (and the jsonb_ forms) never
//! build structure through a JSON null, or any scalar, in the middle of a
//! PATH. A step into one finds nothing, so that pair changes nothing; missing
//! object members and the slot past an array's end are still built. Every
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

const SETUP: &[&str] = &[
    "CREATE TABLE d(id INTEGER PRIMARY KEY, j TEXT, p TEXT)",
    "INSERT INTO d VALUES \
     (1, '{\"a\":null}', '$.a.b.c'), \
     (2, '[null]', '$[0][0][0]'), \
     (3, '{\"a\":null}', '$.a[0].c'), \
     (4, '{}', '$.a.b.c'), \
     (5, '{\"a\":1}', '$.a.b')",
];

const QUERIES: &[&str] = &[
    // The bead's shapes.
    "SELECT json_set('{\"a\":null}', '$.a.b.c', 1)",
    "SELECT json_insert('[null]', '$[0][0][0]', 1)",
    "SELECT json_set('{\"a\":null}', '$.a[0].c', 1)",
    // A null at each depth of the path, for each edit function.
    "SELECT json_set('null', '$', 1)",
    "SELECT json_set('null', '$.a', 1)",
    "SELECT json_set('null', '$[0]', 1)",
    "SELECT json_set('null', '$.a.b', 1)",
    "SELECT json_set('{\"a\":null}', '$.a', 1)",
    "SELECT json_set('{\"a\":null}', '$.a.b', 1)",
    "SELECT json_set('{\"a\":null}', '$.a[0]', 1)",
    "SELECT json_set('{\"a\":null}', '$.a[#]', 1)",
    "SELECT json_set('{\"a\":{\"b\":null}}', '$.a.b.c', 1)",
    "SELECT json_set('{\"a\":{\"b\":null}}', '$.a.b.c.d', 1)",
    "SELECT json_set('{\"a\":{\"b\":null}}', '$.a.b[0][1]', 1)",
    "SELECT json_set('[null]', '$[0][0]', 1)",
    "SELECT json_set('[null]', '$[0].x', 1)",
    "SELECT json_set('[null]', '$[#-1].x.y', 1)",
    "SELECT json_set('[1,[null]]', '$[1][0].x', 1)",
    "SELECT json_insert('null', '$.a', 1)",
    "SELECT json_insert('{\"a\":null}', '$.a', 1)",
    "SELECT json_insert('{\"a\":null}', '$.a.b', 1)",
    "SELECT json_insert('{\"a\":null}', '$.a.b.c', 1)",
    "SELECT json_insert('{\"a\":[null]}', '$.a[0].b', 1)",
    "SELECT json_insert('{\"a\":[null]}', '$.a[1].b', 1)",
    "SELECT json_insert('[null]', '$[0][#]', 1)",
    "SELECT json_replace('{\"a\":null}', '$.a', 1)",
    "SELECT json_replace('{\"a\":null}', '$.a.b', 1)",
    "SELECT json_replace('{\"a\":null}', '$.a.b.c', 1)",
    "SELECT json_replace('[null]', '$[0][0]', 1)",
    // The jsonb_ forms, as JSON text and as bytes.
    "SELECT json(jsonb_set('{\"a\":null}', '$.a.b.c', 1))",
    "SELECT json(jsonb_set('{\"a\":null}', '$.a[0].c', 1))",
    "SELECT json(jsonb_insert('[null]', '$[0][0][0]', 1))",
    "SELECT json(jsonb_replace('{\"a\":null}', '$.a.b', 1))",
    "SELECT hex(jsonb_set('{\"a\":null}', '$.a.b.c', 1))",
    "SELECT hex(jsonb_insert('[null]', '$[0][0][0]', 1))",
    // Other scalars in the middle of a path.
    "SELECT json_set('{\"a\":1}', '$.a.b.c', 1)",
    "SELECT json_set('{\"a\":\"s\"}', '$.a[0]', 1)",
    "SELECT json_set('{\"a\":true}', '$.a.b', 1)",
    "SELECT json_insert('[false]', '$[0][0]', 1)",
    "SELECT json_set('{\"a\":[]}', '$.a.b', 1)",
    "SELECT json_set('{\"a\":{}}', '$.a[0]', 1)",
    // A blocked pair leaves the other pairs to apply.
    "SELECT json_set('{\"a\":null}', '$.a.b.c', 1, '$.d', 2)",
    "SELECT json_set('{\"a\":null}', '$.d', 2, '$.a.b', 1, '$.a', 3)",
    "SELECT json_insert('[null]', '$[0][0]', 1, '$[#]', 2)",
    // Missing members and the end slot are still built.
    "SELECT json_set('{}', '$.a.b.c', 1)",
    "SELECT json_set('{}', '$.a[0].c', 1)",
    "SELECT json_set('{}', '$.a[1]', 1)",
    "SELECT json_set('{}', '$.a[#].b', 1)",
    "SELECT json_set('[]', '$[0][0]', 1)",
    "SELECT json_set('[]', '$[1][0]', 1)",
    "SELECT json_insert('{}', '$.a[0][0]', 1)",
    "SELECT json_set('{\"a\":{}}', '$.a.b.c', 1)",
    // Values computed per row.
    "SELECT id, json_set(j, p, 1) FROM d ORDER BY id",
    "SELECT id, json_insert(j, p, 1) FROM d ORDER BY id",
    "SELECT id, json(jsonb_set(j, p, 1)) FROM d ORDER BY id",
];

#[test]
fn json_edits_never_build_through_a_null_like_stock() {
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
            "{} of {} JSON edit divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            QUERIES.len(),
            diffs.join("\n")
        );
    });
}
