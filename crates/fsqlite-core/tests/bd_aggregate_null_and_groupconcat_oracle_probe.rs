#![recursion_limit = "512"]

//! Aggregate NULL-handling + group_concat/total leaf-hunt (pane af49,
//! 2026-08-21): frank vs rusqlite over the fiddly numeric/NULL corners of
//! aggregates — sum() of all-NULL -> NULL while total() -> 0.0, sum() over
//! integers staying INTEGER vs overflowing to REAL, total() always REAL,
//! avg() ignoring NULLs and returning REAL, count(*) vs count(col) vs
//! count(DISTINCT col) with NULLs, min()/max() skipping NULL, group_concat with
//! the default comma and a custom separator (NULLs skipped), and empty-group
//! aggregates. Grouped result sets ordered; group_concat member order is stable
//! by rowid on both engines here. Pass = coverage keeper; a mismatch is a leaf.

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
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else { return vec![vec!["ERR".to_owned()]] };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect::<Vec<_>>())
    }) {
        Ok(rows) => rows.collect::<Result<Vec<_>, _>>().unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}

#[test]
fn aggregate_null_and_groupconcat_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, g TEXT, n INTEGER, x REAL, s TEXT)",
            "INSERT INTO t VALUES \
              (1,'a',10,1.5,'p'),\
              (2,'a',NULL,2.5,NULL),\
              (3,'a',30,NULL,'q'),\
              (4,'b',NULL,NULL,NULL),\
              (5,'b',5,5.0,'r'),\
              (6,'c',NULL,NULL,NULL)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // sum vs total over a fully-NULL group ('c'): sum->NULL, total->0.0
            "SELECT g, sum(n), total(n) FROM t GROUP BY g ORDER BY g",
            // avg ignores NULLs, returns REAL; count(n) skips NULL; count(*) all
            "SELECT g, avg(n), count(n), count(*) FROM t GROUP BY g ORDER BY g",
            // min/max skip NULLs
            "SELECT g, min(n), max(n), min(x), max(x) FROM t GROUP BY g ORDER BY g",
            // group_concat default comma separator, NULLs skipped
            "SELECT g, group_concat(s) FROM t GROUP BY g ORDER BY g",
            // group_concat with a custom separator
            "SELECT g, group_concat(s, '|') FROM t GROUP BY g ORDER BY g",
            // group_concat over the integer column (NULLs skipped, ints texted)
            "SELECT g, group_concat(n) FROM t GROUP BY g ORDER BY g",
            // count(DISTINCT) with NULLs (NULL not counted)
            "SELECT count(DISTINCT g), count(DISTINCT n), count(DISTINCT x) FROM t",
            // whole-table aggregates (no GROUP BY)
            "SELECT sum(n), total(n), avg(n), min(n), max(n), count(n), count(*) FROM t",
            // sum staying INTEGER when all operands are integers
            "SELECT typeof(sum(n)), typeof(total(n)), typeof(avg(n)), typeof(count(*)) FROM t",
            // sum overflow -> REAL promotion
            "SELECT sum(v) FROM (VALUES (9223372036854775807),(9223372036854775807)) AS z(v)",
            "SELECT typeof(sum(v)) FROM (VALUES (9223372036854775807),(1)) AS z(v)",
            // empty aggregate (no rows) -> sum NULL, count 0, total 0.0
            "SELECT sum(n), total(n), count(*), avg(n), max(n) FROM t WHERE g='zzz'",
            // group_concat with DISTINCT
            "SELECT group_concat(DISTINCT g) FROM t",
            // HAVING referencing an aggregate over NULLs
            "SELECT g, count(n) AS cn FROM t GROUP BY g HAVING count(n) >= 1 ORDER BY g",
            // sum of REAL column with NULLs
            "SELECT g, sum(x), total(x) FROM t GROUP BY g ORDER BY g",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(diffs.is_empty(), "{} aggregate/group_concat divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
