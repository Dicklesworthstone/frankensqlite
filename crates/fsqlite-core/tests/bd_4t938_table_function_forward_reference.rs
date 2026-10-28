#![recursion_limit = "512"]

//! bd-4t938: a table-valued function listed before the FROM source its
//! arguments read. Stock's planner runs the function after that source
//! (`FROM json_each(t.tags) AS j, t` scans `t` first); fsqlite's join fallback
//! ran sources in FROM order and failed with "column not found: tags". When an
//! outer join pins the source after the function, stock runs the function
//! without the argument and json_each returns no rows.
//!
//! Each query is compared with rusqlite's bundled SQLite on the same data:
//! rows as a sorted multiset when the statement has no ORDER BY, in order
//! otherwise. `SELECT *` also compares the result column names. json_each's
//! `id` differs from stock for arrays longer than one element (bd-kenqt), so
//! the `*` queries keep `j.key = 0` rows only.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, tags TEXT)",
    "INSERT INTO t VALUES (1,'[1,2]'),(2,'[3]'),(3,'[]')",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, n INTEGER)",
    "INSERT INTO u VALUES (1,2),(2,1),(3,0)",
];

/// Statements without ORDER BY; rows are compared as sorted multisets.
const UNORDERED: &[&str] = &[
    // Comma join.
    "SELECT j.value, t.id FROM json_each(t.tags) AS j, t",
    // JOIN ... ON, with the ON term reading either side.
    "SELECT j.value, t.id FROM json_each(t.tags) AS j JOIN t ON t.id >= 1",
    "SELECT j.value, t.id FROM json_each(t.tags) AS j INNER JOIN t ON j.value > 1",
    // Two functions chained ahead of the table, in either order. (rusqlite's
    // bundled SQLite has no generate_series, so the chain is json_each.)
    "SELECT j.value, k.value, t.id \
     FROM json_each(t.tags) AS j, json_each(json_array(j.value, -j.value)) AS k, t",
    "SELECT j.value, k.value, t.id \
     FROM json_each(json_array(j.value, -j.value)) AS k, json_each(t.tags) AS j, t",
    // Three sources, the function reading the last one.
    "SELECT g.value, u.id FROM json_each(json_array(u.n, u.n + 1)) AS g, t, u \
     WHERE t.id = u.id",
    "SELECT j.value, t.id FROM json_each(t.tags) AS j, t, u WHERE u.id = t.id AND u.n > 1",
    // A bare argument column, a derived table, json_tree, an aggregate.
    "SELECT j.value FROM json_each(tags) AS j, t WHERE j.value >= 2",
    "SELECT j.value, x.id FROM json_each(x.tags) AS j, (SELECT id, tags FROM t) AS x",
    "SELECT j.fullkey, t.id FROM json_tree(t.tags) AS j, t WHERE j.atom IS NOT NULL",
    "SELECT count(*), sum(j.value) FROM json_each(t.tags) AS j, t",
    // `*` and `tbl.*` keep FROM order.
    "SELECT * FROM json_each(t.tags) AS j, t WHERE j.key = 0",
    "SELECT j.*, t.id FROM json_each(t.tags) AS j, t WHERE j.key = 0",
    "SELECT t.*, j.value FROM json_each(t.tags) AS j, t",
    // An outer join pins `t` after the function: stock returns no rows.
    "SELECT j.value, t.id FROM json_each(t.tags) AS j LEFT JOIN t",
];

/// Statements with ORDER BY; rows are compared in order.
const ORDERED: &[&str] = &[
    "SELECT t.id, count(*) FROM json_each(t.tags) AS j, t GROUP BY t.id ORDER BY t.id",
    "SELECT * FROM json_each(t.tags) AS j, t WHERE j.key = 0 ORDER BY 9 DESC, 2",
    "SELECT DISTINCT t.id FROM json_each(t.tags) AS j, t ORDER BY 1",
    "SELECT j.value, t.id FROM json_each(t.tags) AS j, t ORDER BY j.value DESC LIMIT 2",
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
fn table_function_before_the_source_it_reads_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SETUP {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }

        let mut diffs = Vec::new();
        for (queries, sorted) in [(UNORDERED, true), (ORDERED, false)] {
            for q in queries {
                let mut fr = fq(&f, q).await;
                let mut rr = rq(&r, q);
                if sorted {
                    for rows in [&mut fr, &mut rr].into_iter().flatten() {
                        rows.sort();
                    }
                }
                if fr != rr {
                    diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
                }
            }
        }
        assert!(
            diffs.is_empty(),
            "{} forward-reference divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );

        // Every query above returns rows from stock except the LEFT JOIN
        // shape, so a blanket empty result cannot pass.
        let comma = "SELECT j.value, t.id FROM json_each(t.tags) AS j, t";
        assert_eq!(fq(&f, comma).await.unwrap().len(), 3, "{comma}");

        let star = "SELECT * FROM json_each(t.tags) AS j, t";
        let frank_names = f.prepare(star).await.unwrap().column_names().clone();
        let stock_names: Vec<String> = r
            .prepare(star)
            .unwrap()
            .column_names()
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        assert_eq!(frank_names, stock_names, "{star} column names");
    });
}
