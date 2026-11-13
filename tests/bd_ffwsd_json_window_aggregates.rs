#![recursion_limit = "512"]

//! bd-ffwsd: json_group_array, json_group_object, jsonb_group_array and
//! jsonb_group_object work as window functions, as in stock: every frame kind
//! (ROWS, RANGE, GROUPS; unbounded, running and sliding; EXCLUDE), PARTITION
//! BY, FILTER, NULL labels sliding through a frame, JSON-subtyped arguments
//! embedded rather than quoted, FROM-less and GROUP BY windows, and window
//! calls nested in expressions. Every statement's result or error text is
//! compared with rusqlite's bundled SQLite.
//!
//! Not covered here: `->` and json_extract() arguments (bd-ikehc, which the
//! plain aggregates share), a window result passed to a JSON constructor
//! (bd-0pj4x), and json()/json_extract() over duplicate labels (llk6g.14).

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
    "CREATE TABLE w(id INTEGER PRIMARY KEY, g TEXT, k, v, j TEXT)",
    "INSERT INTO w VALUES \
     (1, 'a', 'x', 1, '[1]'), \
     (2, 'a', 'y', 'two', '{\"a\":1}'), \
     (3, 'a', NULL, 3.5, '2'), \
     (4, 'b', 'x', NULL, 'null'), \
     (5, 'b', 7, '[1]', '\"s\"'), \
     (6, 'b', 'x', 6, 'true'), \
     (7, 'c', NULL, NULL, '[]'), \
     (8, 'c', 'q\"t', 'q\"t', '{}')",
];

const QUERIES: &[&str] = &[
    // Whole partition, running, and sliding ROWS frames.
    "SELECT id, json_group_array(v) OVER () FROM w ORDER BY id",
    "SELECT id, json_group_array(v) OVER (ORDER BY id) FROM w ORDER BY id",
    "SELECT id, json_group_array(v) OVER (ORDER BY id DESC) FROM w ORDER BY id",
    "SELECT id, json_group_array(v) OVER (ORDER BY id ROWS 1 PRECEDING) FROM w ORDER BY id",
    "SELECT id, json_group_array(v) OVER (ORDER BY id \
     ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w ORDER BY id",
    "SELECT id, json_group_array(v) OVER (ORDER BY id \
     ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w ORDER BY id",
    "SELECT id, json_group_array(v) OVER (ORDER BY id \
     ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM w ORDER BY id",
    "SELECT id, json_group_array(v) OVER (ORDER BY id \
     ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM w ORDER BY id",
    // RANGE and GROUPS frames, with peers.
    "SELECT id, json_group_array(id) OVER (ORDER BY g) FROM w ORDER BY id",
    "SELECT id, json_group_array(id) OVER (ORDER BY id \
     RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w ORDER BY id",
    "SELECT id, json_group_array(id) OVER (ORDER BY g \
     GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
    "SELECT id, json_group_array(id) OVER (ORDER BY g \
     RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w ORDER BY id",
    // EXCLUDE.
    "SELECT id, json_group_array(id) OVER (ORDER BY g \
     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) \
     FROM w ORDER BY id",
    "SELECT id, json_group_array(id) OVER (ORDER BY g \
     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) \
     FROM w ORDER BY id",
    "SELECT id, json_group_array(id) OVER (ORDER BY g \
     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) \
     FROM w ORDER BY id",
    "SELECT id, json_group_object(k, id) OVER (ORDER BY g \
     ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM w ORDER BY id",
    // PARTITION BY.
    "SELECT id, json_group_array(v) OVER (PARTITION BY g) FROM w ORDER BY id",
    "SELECT id, json_group_array(v) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id",
    "SELECT id, json_group_array(v) OVER (PARTITION BY g ORDER BY id \
     ROWS 1 PRECEDING) FROM w ORDER BY id",
    "SELECT id, json_group_object(k, v) OVER (PARTITION BY g ORDER BY id) \
     FROM w ORDER BY id",
    // FILTER.
    "SELECT id, json_group_array(v) FILTER (WHERE v IS NOT NULL) OVER (ORDER BY id) \
     FROM w ORDER BY id",
    "SELECT id, json_group_array(v) FILTER (WHERE id % 2 = 0) OVER (ORDER BY id \
     ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w ORDER BY id",
    "SELECT id, json_group_object(k, v) FILTER (WHERE id > 2) OVER (ORDER BY id \
     ROWS 2 PRECEDING) FROM w ORDER BY id",
    // json_group_object: NULL labels (skipped, also while sliding through a
    // frame), non-text labels, duplicate labels.
    "SELECT id, json_group_object(k, v) OVER () FROM w ORDER BY id",
    "SELECT id, json_group_object(k, v) OVER (ORDER BY id) FROM w ORDER BY id",
    "SELECT id, json_group_object(k, v) OVER (ORDER BY id ROWS 1 PRECEDING) \
     FROM w ORDER BY id",
    "SELECT id, json_group_object(k, v) OVER (ORDER BY id \
     ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w ORDER BY id",
    "SELECT id, json_group_object(k, v) OVER (ORDER BY id \
     ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM w ORDER BY id",
    "SELECT id, json_group_object(id, k) OVER (ORDER BY id ROWS 2 PRECEDING) \
     FROM w ORDER BY id",
    "SELECT id, json_group_object(g, id) OVER (ORDER BY g \
     RANGE CURRENT ROW) FROM w ORDER BY id",
    "SELECT id, json_group_object(CASE WHEN id % 3 = 0 THEN NULL ELSE 'L' END, id) \
     OVER (ORDER BY id ROWS 2 PRECEDING) FROM w ORDER BY id",
    // JSON-subtyped arguments are embedded, not quoted.
    "SELECT id, json_group_array(json(j)) OVER (ORDER BY id) FROM w ORDER BY id",
    "SELECT id, json_group_array(json(j)) OVER (ORDER BY id ROWS 1 PRECEDING) \
     FROM w ORDER BY id",
    "SELECT id, json_group_array(j) OVER (ORDER BY id ROWS 1 PRECEDING) \
     FROM w ORDER BY id",
    "SELECT id, json_group_array(json_object('id', id, 'v', v)) OVER (PARTITION BY g) \
     FROM w ORDER BY id",
    "SELECT id, json_group_array(json_array(id, k)) OVER (ORDER BY id \
     ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM w ORDER BY id",
    "SELECT id, json_group_object(g, json(j)) OVER (ORDER BY id ROWS 1 PRECEDING) \
     FROM w ORDER BY id",
    "SELECT id, json_group_object(k, json_array(id)) OVER (PARTITION BY g ORDER BY id) \
     FROM w ORDER BY id",
    "SELECT id, json_group_array(jsonb(j)) OVER (ORDER BY id ROWS 1 PRECEDING) \
     FROM w ORDER BY id",
    // The jsonb_ forms return JSONB.
    "SELECT id, hex(jsonb_group_array(v) OVER (ORDER BY id)) FROM w ORDER BY id",
    "SELECT id, hex(jsonb_group_array(v) OVER (ORDER BY id ROWS 1 PRECEDING)) \
     FROM w ORDER BY id",
    "SELECT id, hex(jsonb_group_object(k, v) OVER (ORDER BY id ROWS 1 PRECEDING)) \
     FROM w ORDER BY id",
    "SELECT id, hex(jsonb_group_object(k, v) OVER (PARTITION BY g)) FROM w ORDER BY id",
    "SELECT id, json(jsonb_group_array(v) OVER (ORDER BY id \
     ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)) FROM w ORDER BY id",
    "SELECT id, hex(jsonb_group_object(k, v) OVER (ORDER BY id \
     ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)) FROM w ORDER BY id",
    "SELECT id, hex(jsonb_group_array(json(j)) OVER (ORDER BY id ROWS 1 PRECEDING)) \
     FROM w ORDER BY id",
    "SELECT id, hex(jsonb_group_object(g, json_object('id', id)) OVER (ORDER BY id)) \
     FROM w ORDER BY id",
    "SELECT id, json(jsonb_group_object(id, json_object('g', g)) OVER (ORDER BY id \
     ROWS 2 PRECEDING)) FROM w ORDER BY id",
    // Named windows, several windows in one SELECT, other window functions.
    "SELECT id, json_group_array(v) OVER win, json_group_object(k, id) OVER win \
     FROM w WINDOW win AS (ORDER BY id ROWS 1 PRECEDING) ORDER BY id",
    "SELECT id, row_number() OVER (ORDER BY id DESC), \
     json_group_array(id) OVER (PARTITION BY g ORDER BY id), \
     group_concat(id) OVER (ORDER BY id ROWS 1 PRECEDING) FROM w ORDER BY id",
    // Window calls nested in expressions.
    "SELECT id, json_array_length(json_group_array(v) OVER (ORDER BY id)) FROM w ORDER BY id",
    "SELECT id, 'x' || json_group_array(id) OVER (ORDER BY id ROWS 1 PRECEDING) \
     FROM w ORDER BY id",
    "SELECT id, (json_group_array(id) OVER (ORDER BY id)) -> '$[0]' FROM w ORDER BY id",
    "SELECT id, json_extract(json_group_object(id, g) OVER (PARTITION BY g), '$.4') \
     FROM w ORDER BY id",
    "SELECT * FROM (SELECT id, json_group_array(v) OVER (ORDER BY id) AS a FROM w) \
     WHERE id > 5 ORDER BY id",
    "SELECT id, a FROM (SELECT id, json_group_object(k, id) OVER (ORDER BY id) AS a \
     FROM w) ORDER BY a DESC, id LIMIT 3",
    "WITH c AS (SELECT id, json_group_array(id) OVER (ORDER BY id ROWS 1 PRECEDING) AS a \
     FROM w) SELECT a FROM c WHERE id IN (2, 8) ORDER BY id",
    // GROUP BY, then a window over the groups.
    "SELECT g, json_group_array(count(*)) OVER (ORDER BY g) FROM w GROUP BY g ORDER BY g",
    "SELECT g, json_group_object(g, sum(id)) OVER (ORDER BY g ROWS 1 PRECEDING) \
     FROM w GROUP BY g ORDER BY g",
    "SELECT g, json_group_array(json_group_array(id)) OVER (ORDER BY g) \
     FROM w GROUP BY g ORDER BY g",
    "SELECT g, json_group_array(json_array(g, count(*))) OVER (ORDER BY g) \
     FROM w GROUP BY g ORDER BY g",
    "SELECT g, hex(jsonb_group_array(max(id)) OVER (ORDER BY g DESC)) \
     FROM w GROUP BY g ORDER BY g",
    // A join as the window's input.
    "SELECT a.id, b.id, json_group_array(b.id) OVER (PARTITION BY a.id ORDER BY b.id) \
     FROM w a JOIN w b ON a.g = b.g WHERE a.id < 4 ORDER BY a.id, b.id",
    // FROM-less.
    "SELECT json_group_array(1) OVER ()",
    "SELECT json_group_array(NULL) OVER ()",
    "SELECT json_group_array(json('[1,2]')) OVER ()",
    "SELECT json_group_array('[1,2]') OVER ()",
    "SELECT json_group_object('a', 1) OVER ()",
    "SELECT json_group_object(NULL, 1) OVER ()",
    "SELECT json_group_object('a', json_object('b', 2)) OVER ()",
    "SELECT hex(jsonb_group_array(2) OVER ())",
    "SELECT hex(jsonb_group_object('a', 2) OVER ())",
    "SELECT json_group_array(1) FILTER (WHERE 0) OVER ()",
    "SELECT hex(jsonb_group_object('a', 1) FILTER (WHERE 0) OVER ())",
    "SELECT json_group_array(1) OVER (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)",
    // Empty input.
    "SELECT json_group_array(v) OVER () FROM w WHERE 0",
    // Errors.
    "SELECT json_group_array(1, 2) OVER ()",
    "SELECT json_group_array() OVER ()",
    "SELECT json_group_object(1) OVER ()",
    "SELECT jsonb_group_object(1, 2, 3) OVER ()",
    "SELECT json_group_array(v, 1) OVER () FROM w",
    "SELECT json_group_object(k, json('[1')) OVER () FROM w",
];

#[test]
fn json_aggregates_as_window_functions_match_stock() {
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
