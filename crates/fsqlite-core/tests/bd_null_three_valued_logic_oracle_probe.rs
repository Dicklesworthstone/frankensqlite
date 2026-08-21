#![recursion_limit = "512"]

//! NULL / three-valued-logic leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite
//! over SQL's NULL semantics — IN / NOT IN with a NULL in the value list (the
//! classic `x NOT IN (1,2,NULL)` -> NULL, never false, trap), NULL on the left of
//! IN/NOT IN, three-valued AND/OR truth tables, IS / IS NOT (NULL-comparable) vs
//! `=`/`<>` (NULL-propagating), WHERE rows whose predicate is NULL being excluded,
//! coalesce / ifnull / nullif, CASE with a NULL/absent match, and aggregates over
//! NULLs. Scalar / small result sets compared. Pass = coverage keeper.

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
fn null_three_valued_logic_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1,10),(2,20),(3,NULL),(4,30)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let exprs = [
            // IN / NOT IN with a NULL in the list
            "SELECT 10 IN (10,20,NULL), 99 IN (10,20,NULL), 10 NOT IN (10,20,NULL), 99 NOT IN (10,20,NULL)",
            // NULL on the left of IN / NOT IN
            "SELECT NULL IN (1,2,3), NULL NOT IN (1,2,3), NULL IN (1,2,NULL)",
            // empty-ish and single-value IN
            "SELECT 5 IN (5), 5 IN (6), 5 NOT IN (6)",
            // three-valued AND truth table
            "SELECT (NULL AND 0), (NULL AND 1), (0 AND NULL), (1 AND NULL), (NULL AND NULL)",
            // three-valued OR truth table
            "SELECT (NULL OR 0), (NULL OR 1), (0 OR NULL), (1 OR NULL), (NULL OR NULL)",
            // NOT NULL
            "SELECT (NOT NULL), (NOT 0), (NOT 1)",
            // comparison with NULL yields NULL
            "SELECT (NULL = NULL), (NULL <> NULL), (NULL = 1), (1 < NULL), (NULL > 0)",
            // IS / IS NOT treat NULL as comparable (yield 0/1)
            "SELECT (NULL IS NULL), (NULL IS NOT NULL), (1 IS NULL), (1 IS 1), (1 IS NOT 2), (NULL IS 1)",
            // coalesce / ifnull / nullif
            "SELECT coalesce(NULL,NULL,3,4), coalesce(NULL,NULL), ifnull(NULL,7), ifnull(5,7), nullif(5,5), nullif(5,6)",
            // CASE: a NULL WHEN condition is not true -> falls through
            "SELECT CASE WHEN NULL THEN 'a' ELSE 'b' END, CASE WHEN 1 THEN 'a' ELSE 'b' END, CASE WHEN NULL THEN 'a' END",
            // searched CASE matching NULL value via equality (never matches) vs simple CASE
            "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no' END",
            // concatenation / arithmetic with NULL -> NULL
            "SELECT (NULL || 'x'), (NULL + 1), (NULL * 0)",
            // WHERE excludes rows whose predicate is NULL (v=NULL row dropped)
            "SELECT id FROM t WHERE v > 15 ORDER BY id",
            "SELECT id FROM t WHERE v <> 10 ORDER BY id",
            "SELECT id FROM t WHERE NOT (v = 10) ORDER BY id",
            "SELECT id FROM t WHERE v IS NULL ORDER BY id",
            "SELECT id FROM t WHERE v IN (10,30) ORDER BY id",
            "SELECT id FROM t WHERE v NOT IN (10,30) ORDER BY id",
            "SELECT id FROM t WHERE v NOT IN (10,30,NULL) ORDER BY id",   // NULL in list -> no rows
            // correlated NOT IN with a NULL-bearing subquery -> empty
            "SELECT id FROM t WHERE id NOT IN (SELECT v FROM t) ORDER BY id",
            // aggregates over NULLs
            "SELECT count(v), count(*), total(v), sum(v), avg(v) FROM t",
            // NULL ordering: NULLs sort first by default asc
            "SELECT v FROM t ORDER BY v",
            "SELECT v FROM t ORDER BY v DESC",
            "SELECT v FROM t ORDER BY v NULLS LAST",
        ];

        let mut diffs = Vec::new();
        for q in exprs {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(diffs.is_empty(), "{} NULL/3VL divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
