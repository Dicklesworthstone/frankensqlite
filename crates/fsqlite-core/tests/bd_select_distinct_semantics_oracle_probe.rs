#![recursion_limit = "512"]

//! SELECT DISTINCT semantics leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over DISTINCT — single- and multi-column DISTINCT, NULL treated as
//! equal-to-NULL for dedup purposes (so duplicate NULL rows collapse), DISTINCT
//! over an expression, DISTINCT combined with ORDER BY and LIMIT/OFFSET, the
//! DISTINCT-vs-GROUP BY equivalence, affinity-driven distinctness (int 1 vs
//! text '1' vs real 1.0 are distinct storage classes), and DISTINCT over a
//! join. Ordered result sets compared. Pass = coverage keeper.

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
fn select_distinct_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(g TEXT, n INTEGER, x REAL)",
            "INSERT INTO t VALUES \
              ('a',1,1.0),('a',1,1.0),('a',2,2.0),\
              ('b',NULL,NULL),('b',NULL,NULL),('b',3,3.5),\
              (NULL,1,NULL),(NULL,1,NULL)",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // single-column DISTINCT with a NULL group (NULLs collapse to one)
            "SELECT DISTINCT g FROM t ORDER BY g",
            "SELECT DISTINCT n FROM t ORDER BY n",
            // multi-column DISTINCT: (g,n) pairs, NULLs equal for dedup
            "SELECT DISTINCT g, n FROM t ORDER BY g, n",
            // DISTINCT over all three including NULL-bearing rows
            "SELECT DISTINCT g, n, x FROM t ORDER BY g, n, x",
            // DISTINCT over an expression
            "SELECT DISTINCT n * 2 AS d FROM t ORDER BY d",
            "SELECT DISTINCT g || '/' || COALESCE(n,-1) FROM t ORDER BY 1",
            // DISTINCT + ORDER BY DESC + LIMIT/OFFSET
            "SELECT DISTINCT n FROM t ORDER BY n DESC LIMIT 2",
            "SELECT DISTINCT g FROM t ORDER BY g LIMIT 2 OFFSET 1",
            // count of distinct rows
            "SELECT count(*) FROM (SELECT DISTINCT g, n FROM t)",
            // DISTINCT-vs-GROUP BY equivalence
            "SELECT g, n FROM t GROUP BY g, n ORDER BY g, n",
            // affinity: mixed storage classes stay distinct (int 1 vs text '1' vs real 1.0);
            // CTE column-list alias is the SQLite-supported positional-rename form.
            "WITH z(v) AS (VALUES (1),('1'),(1.0),(NULL),(NULL)) SELECT DISTINCT v FROM z ORDER BY typeof(v), v",
            // DISTINCT over a join
            "SELECT DISTINCT t1.g FROM t t1 JOIN t t2 ON t1.g=t2.g ORDER BY t1.g",
            // DISTINCT with a WHERE filter
            "SELECT DISTINCT n FROM t WHERE g='a' ORDER BY n",
            // DISTINCT *
            "SELECT DISTINCT * FROM t ORDER BY g, n, x",
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
            "{} DISTINCT divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// bd-w4pr1.13.15: a DISTINCT scan and an IN probe ask an ephemeral index
/// "is this key present" once per row, and the engine answers a repeated
/// probe record from a per-cursor memo instead of seeking again. The answers
/// must not depend on that: these cases repeat keys right after their first
/// appearance, mix storage classes that compare equal (1 and 1.0) with ones
/// that do not ('1', x'31'), probe NULLs, exceed the memo's size, keep
/// inserting until the memo pauses and then repeat old keys, and rebuild a
/// correlated IN set of the same size with different members for the next
/// outer row (a memo that outlived its set would answer from the old one).
#[test]
fn repeated_membership_probes_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE m(id INTEGER PRIMARY KEY, k, n TEXT COLLATE NOCASE, i INTEGER, g INTEGER)",
            // 720 rows cycling through 12 key spellings, in an order that
            // repeats a key at once and again much later.
            "WITH RECURSIVE c(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM c WHERE x < 719) \
             INSERT INTO m SELECT x + 1, \
               CASE (x * 7 + x / 12) % 12 WHEN 0 THEN 1 WHEN 1 THEN 1.0 WHEN 2 THEN '1' \
                 WHEN 3 THEN NULL WHEN 4 THEN x'31' WHEN 5 THEN 2 WHEN 6 THEN 2.5 WHEN 7 THEN 'a' \
                 WHEN 8 THEN 'A' WHEN 9 THEN NULL WHEN 10 THEN 3 ELSE 'a ' END, \
               CASE x % 5 WHEN 0 THEN 'Ann' WHEN 1 THEN 'ann' WHEN 2 THEN 'BOB' WHEN 3 THEN NULL ELSE 'bob' END, \
               x % 40, x % 3 FROM c",
            "CREATE TABLE s(g INTEGER, y)",
            "INSERT INTO s VALUES (0,1),(0,2),(0,3),(1,5),(1,6),(1,7),(2,5),(2,NULL),(2,9)",
            "CREATE TABLE big(id INTEGER PRIMARY KEY, x INTEGER)",
            // 40,000 distinct values, then 20,000 rows that repeat them.
            "WITH RECURSIVE c(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM c WHERE v < 60000) \
             INSERT INTO big SELECT v, CASE WHEN v <= 40000 THEN v ELSE (v * 7919) % 40000 + 1 END FROM c",
        ] {
            f.execute(s).await.unwrap();
            r.execute(s, []).unwrap();
        }

        let queries = [
            // DISTINCT in scan order (no ORDER BY) and ordered.
            "SELECT DISTINCT k FROM m",
            "SELECT DISTINCT n FROM m",
            "SELECT DISTINCT k, n FROM m ORDER BY typeof(k), k, n",
            "SELECT DISTINCT i % 4, typeof(k) FROM m",
            "SELECT count(*) FROM (SELECT DISTINCT k FROM m)",
            "SELECT count(*) FROM (SELECT DISTINCT n COLLATE BINARY FROM m)",
            "SELECT DISTINCT i FROM m WHERE i < 30 LIMIT 7 OFFSET 3",
            // More distinct keys than the memo holds; inserts until it pauses.
            "SELECT count(*), sum(x), min(x), max(x) FROM (SELECT DISTINCT x FROM big)",
            "SELECT count(*) FROM (SELECT DISTINCT x % 9000 FROM big)",
            // IN over a literal list long enough to be materialized.
            "SELECT count(*), sum(id) FROM m WHERE i IN (1, 3, 5, 7, 9, 11, 13, 15, 17, 39)",
            "SELECT count(*), sum(id) FROM m WHERE i NOT IN (1, 3, 5, 7, 9, 11, 13, 15, 17, 39)",
            "SELECT count(*), sum(id) FROM m WHERE i IN (1, 3, 5, 7, 9, 11, 13, 15, NULL, 39)",
            "SELECT count(*), sum(id) FROM m WHERE i NOT IN (1, 3, 5, 7, 9, 11, 13, 15, NULL, 39)",
            "SELECT count(*), sum(id) FROM m WHERE i IN ('1', '3', 5.0, 7, '9x', 11, 13, 15, 17, 39.5)",
            "SELECT count(*), sum(id) FROM m WHERE k IN (1, '1', 2.5, 'a', 'zz', 4, 5, 6, 7, 8)",
            "SELECT count(*), sum(id) FROM m WHERE k NOT IN (1, '1', 2.5, 'a', 'zz', 4, 5, 6, 7, 8)",
            "SELECT count(*), sum(id) FROM m WHERE n IN ('ANN', 'x', 'y', 'z', 'w', 'v', 'u', 't', 'q')",
            "SELECT id, i IN (1, 3, 5, 7, 9, 11, 13, 15, NULL, 39) FROM m WHERE id <= 45 ORDER BY id",
            // IN over a subquery, uncorrelated and correlated. Groups 0 and 1
            // have three members each and share none; group 2 has a NULL.
            "SELECT count(*), sum(id) FROM m WHERE i IN (SELECT y FROM s)",
            "SELECT count(*), sum(id) FROM m WHERE i NOT IN (SELECT y FROM s WHERE y IS NOT NULL)",
            "SELECT count(*), sum(id) FROM m WHERE i NOT IN (SELECT y FROM s)",
            "SELECT count(*), sum(id) FROM m WHERE i IN (SELECT y FROM s WHERE s.g = m.g)",
            "SELECT count(*), sum(id) FROM m WHERE i NOT IN (SELECT y FROM s WHERE s.g = m.g)",
            "SELECT id, g, i, i IN (SELECT y FROM s WHERE s.g = m.g) FROM m WHERE i IN (1, 5, 9) \
             AND id <= 130 ORDER BY id",
        ];

        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            assert_ne!(rr, vec![vec!["ERR".to_owned()]], "stock rejects `{q}`");
            if fr != rr {
                let shown = |rows: &[Vec<String>]| format!("{:?}", &rows[..rows.len().min(12)]);
                diffs.push(format!(
                    "  `{q}`\n     frank= {}\n     stock= {}",
                    shown(&fr),
                    shown(&rr)
                ));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} membership divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
