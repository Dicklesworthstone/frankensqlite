#![recursion_limit = "512"]

//! A table-valued function first in FROM whose argument holds a subquery
//! (`FROM json_each((SELECT tags FROM t WHERE id = 1))`) returned no rows: the
//! join fallback treats any subquery argument as a possible LATERAL reference
//! (bd-tfwym) and skips pre-materializing it, but only joined sources are
//! evaluated per left row. The first source has no left sibling to read, so
//! it is always materialized now. rusqlite's bundled SQLite (which has
//! `json_each` but not `generate_series`) is the oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}r"),
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
        rusqlite::types::Value::Real(f) => format!("{f}r"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut st = rconn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock rejects `{sql}`: {e}"));
    let n = st.column_count();
    let expected: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert!(!expected.is_empty(), "the oracle returns rows for `{sql}`");
    let actual: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect();
    assert_eq!(actual, expected, "table-function result mismatch on `{sql}`");
}

const SEED: [&str; 2] = [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, tags TEXT)",
    r#"INSERT INTO t VALUES (1, '["b", "a", "b", 3]'), (2, '[]'), (3, '[10, 20]')"#,
];

#[test]
fn first_table_function_with_a_subquery_argument_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SEED {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        for sql in [
            // Projection, DISTINCT, ORDER BY, LIMIT.
            "SELECT key, value, type FROM json_each((SELECT tags FROM t WHERE id = 1))",
            "SELECT DISTINCT value FROM json_each((SELECT tags FROM t WHERE id = 1))",
            "SELECT value FROM json_each((SELECT tags FROM t WHERE id = 1)) ORDER BY key DESC LIMIT 3",
            "SELECT value FROM json_each((SELECT '[1, 2, 2]'))",
            // Subqueries nested in a larger argument expression.
            "SELECT value FROM json_each(json_array((SELECT max(id) FROM t), 'x'))",
            "SELECT value FROM json_each(coalesce((SELECT tags FROM t WHERE id = 9), '[7]'))",
            "SELECT value FROM json_each((SELECT tags FROM t WHERE id = 1), '$[1]')",
            // Aggregates the streaming table-function path does not take.
            "SELECT count(DISTINCT value), group_concat(value), total(value) \
             FROM json_each((SELECT tags FROM t WHERE id = 1))",
            "SELECT count(value) FILTER (WHERE type = 'text'), json_group_array(value) \
             FROM json_each((SELECT tags FROM t WHERE id = 1))",
            "SELECT type, count(*) FROM json_each((SELECT tags FROM t WHERE id = 1)) \
             GROUP BY type ORDER BY type",
            // The first source joined to a table and to another table function.
            "SELECT j.value, t.id FROM json_each((SELECT tags FROM t WHERE id = 3)) AS j \
             JOIN t ON t.id <= 2 ORDER BY j.key, t.id",
            "SELECT a.value, b.value FROM json_each((SELECT tags FROM t WHERE id = 3)) AS a, \
             json_each('[1, 2]') AS b ORDER BY a.key, b.key",
            // Inside a correlated scalar subquery and an EXISTS, and an outer
            // column argument without a subquery.
            "SELECT id, (SELECT count(*) FROM json_each(t.tags)) FROM t ORDER BY id",
            "SELECT id, (SELECT group_concat(value, '|') FROM json_each((SELECT u.tags FROM t AS u \
             WHERE u.id = t.id))) FROM t ORDER BY id",
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM json_each((SELECT tags FROM t AS u \
             WHERE u.id = t.id)) WHERE value = 'a') ORDER BY id",
            "SELECT count(*) FROM t WHERE id IN (SELECT value FROM json_each((SELECT '[1, 3]')))",
        ] {
            assert_agree(&f, &r, sql).await;
        }
    });
}

#[test]
fn first_generate_series_with_subquery_bounds_produces_its_rows() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        for sql in SEED {
            f.execute(sql).await.unwrap();
        }
        let text = |sql: &'static str| {
            let f = &f;
            async move {
                let rows = f.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
                rows.iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(tag_f)
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .collect::<Vec<_>>()
                    .join(";")
            }
        };
        assert_eq!(
            text("SELECT value FROM generate_series((SELECT min(id) FROM t), (SELECT max(id) FROM t))")
                .await,
            "1;2;3"
        );
        assert_eq!(
            text("SELECT sum(DISTINCT value % 2), group_concat(value) FROM generate_series(1, (SELECT 4))")
                .await,
            "1,'1,2,3,4'"
        );
    });
}
