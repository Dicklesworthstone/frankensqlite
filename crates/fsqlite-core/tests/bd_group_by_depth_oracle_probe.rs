#![recursion_limit = "512"]

//! GROUP BY depth leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite over
//! GROUP BY corners — grouping by an EXPRESSION, the SQLite-specific rule that a
//! bare (non-aggregated) column in a query with a single min()/max() takes its
//! value from the row that produced that extremum, HAVING referencing an
//! aggregate and an output alias, GROUP BY by output ordinal, GROUP BY with a
//! NOCASE collation, multi-column grouping, and grouping with NULLs (all NULLs
//! form one group). Only the min/max-associated bare column is asserted (a bare
//! column with NO min/max is implementation-defined, so it is NOT probed).
//! Ordered result sets compared. Pass = coverage keeper; a mismatch is a leaf.

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
fn group_by_depth_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, g TEXT, v INTEGER, tag TEXT)",
            "INSERT INTO t VALUES \
              (1,'a',30,'a-hi'),(2,'a',10,'a-lo'),(3,'a',20,'a-mid'),\
              (4,'b',50,'b-hi'),(5,'b',40,'b-mid'),\
              (6,'c',NULL,'c-null'),(7,'c',5,'c-lo')",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // min/max bare-column rule: `tag` and `id` come from the max(v) row per group
            "SELECT g, max(v), id, tag FROM t GROUP BY g ORDER BY g",
            "SELECT g, min(v), id, tag FROM t GROUP BY g ORDER BY g",
            // the extremum row selection across the whole table (no GROUP BY)
            "SELECT max(v), id, tag FROM t",
            "SELECT min(v), id, tag FROM t",
            // grouping by an expression (first letter — here g already is, so use substr on tag)
            "SELECT substr(tag,1,1) AS k, count(*) FROM t GROUP BY substr(tag,1,1) ORDER BY k",
            // group by output ordinal
            "SELECT g, count(*), sum(v) FROM t GROUP BY 1 ORDER BY 1",
            // group by expression that is a computed bucket
            "SELECT v/10 AS bucket, count(*) FROM t WHERE v IS NOT NULL GROUP BY v/10 ORDER BY bucket",
            // HAVING referencing an aggregate
            "SELECT g, count(*) AS n FROM t GROUP BY g HAVING count(*) >= 2 ORDER BY g",
            // HAVING referencing an aggregate alias
            "SELECT g, sum(v) AS s FROM t GROUP BY g HAVING sum(v) > 25 ORDER BY g",
            // HAVING with a non-aggregate group condition
            "SELECT g, count(*) FROM t GROUP BY g HAVING g <> 'a' ORDER BY g",
            // NULLs form a single group
            "SELECT v, count(*) FROM t GROUP BY v ORDER BY v",
            // multi-column grouping
            "SELECT g, (v IS NULL) AS isnull, count(*) FROM t GROUP BY g, (v IS NULL) ORDER BY g, isnull",
            // GROUP BY with a NOCASE collation folds case
            "SELECT count(DISTINCT g) FROM (SELECT 'A' AS g UNION ALL SELECT 'a' UNION ALL SELECT 'B')",
            // min(g) is deterministic (BINARY) per NOCASE group; bare `g` would be implementation-defined
            "SELECT min(g) AS mg, count(*) FROM (SELECT 'A' AS g UNION ALL SELECT 'a' UNION ALL SELECT 'B') GROUP BY g COLLATE NOCASE ORDER BY mg",
            // aggregate over an empty group set (WHERE excludes all) -> one all-NULL/zero row
            "SELECT count(*), sum(v), max(v) FROM t WHERE g='zzz'",
            // GROUP BY over empty -> no rows
            "SELECT g, count(*) FROM t WHERE g='zzz' GROUP BY g",
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
            "{} GROUP BY-depth divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
