#![recursion_limit = "512"]

//! bd-oui0e: json_each / json_tree over a NULL input return no rows, as in
//! stock's `jsonEachFilter` (a NULL input or NULL PATH ends the scan before any
//! row). fsqlite raised "json table-valued input must be TEXT or BLOB JSON",
//! so `FROM t, json_each(t.tags)` failed on any table with a NULL in `tags`.
//! Every query is compared with rusqlite's bundled SQLite on the same data.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, tags TEXT)",
    "INSERT INTO t VALUES (1,'[1,2]'),(2,NULL),(3,'[3]')",
];

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
        .map_err(|e| format!("{e:?}"))
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

#[test]
fn json_each_and_json_tree_skip_null_input_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SETUP {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }

        let queries = [
            // The lateral forms over a table with a NULL `tags` row.
            "SELECT t.id, j.value FROM t, json_each(t.tags) AS j ORDER BY t.id, j.key",
            "SELECT t.id, j.value FROM t JOIN json_each(t.tags) AS j ORDER BY t.id, j.key",
            "SELECT t.id, j.value FROM t LEFT JOIN json_each(t.tags) AS j ORDER BY t.id, j.key",
            "SELECT t.id, count(j.value) FROM t LEFT JOIN json_each(t.tags) AS j \
             GROUP BY t.id ORDER BY t.id",
            "SELECT t.id, j.fullkey FROM t, json_tree(t.tags) AS j \
             WHERE j.atom IS NOT NULL ORDER BY t.id, j.fullkey",
            "SELECT id, (SELECT count(*) FROM json_each(t.tags)) FROM t ORDER BY id",
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM json_each(t.tags) WHERE value = 3)",
            // A NULL input, before the PATH is looked at.
            "SELECT count(*) FROM json_each(NULL)",
            "SELECT count(*) FROM json_tree(NULL)",
            "SELECT count(*) FROM json_each(NULL, 'not a path')",
            "SELECT count(*) FROM json_each(jsonb(NULL))",
            // A NULL PATH.
            "SELECT count(*) FROM json_each('[1,2]', NULL)",
            "SELECT count(*) FROM json_tree('{\"a\":1}', NULL)",
            // No input argument.
            "SELECT count(*) FROM json_each()",
            "SELECT count(*) FROM json_tree()",
            // Unchanged: a present input and PATH still scan.
            "SELECT key, value FROM json_each('{\"a\":[5,6]}', '$.a')",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} json_each/json_tree NULL-input divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );

        // The input is parsed before the NULL PATH ends the scan, so malformed
        // input still errors in both.
        let malformed = "SELECT count(*) FROM json_each('{', NULL)";
        assert!(rq(&r, malformed).is_err(), "stock rejects `{malformed}`");
        assert!(
            fq(&f, malformed).await.is_err(),
            "fsqlite must reject `{malformed}` like stock"
        );
    });
}
