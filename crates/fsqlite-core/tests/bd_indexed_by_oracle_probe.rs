#![recursion_limit = "512"]

//! INDEXED BY / NOT INDEXED leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite
//! over the index-hint syntax in SELECT / UPDATE / DELETE — a forced index,
//! NOT INDEXED (full scan), a non-existent index (error on both), and an index
//! the WHERE cannot use (error on both). Results / final state compared; a
//! parse-or-exec rejection of the hint by frank would surface as a divergence.
//! Pass = coverage keeper; a mismatch is a leaf divergence.

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
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn indexed_by_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT)",
            "CREATE INDEX ik ON t(k)",
            "INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,20,'c'),(4,30,'d'),(5,10,'e')",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        // forced index — same results as without the hint
        check(
            "indexed by",
            fq(
                &f,
                "SELECT id,v FROM t INDEXED BY ik WHERE k=20 ORDER BY id",
            )
            .await,
            rq(
                &r,
                "SELECT id,v FROM t INDEXED BY ik WHERE k=20 ORDER BY id",
            ),
            &mut diffs,
        );
        // NOT INDEXED — full scan, same results
        check(
            "not indexed",
            fq(&f, "SELECT id,v FROM t NOT INDEXED WHERE k=10 ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t NOT INDEXED WHERE k=10 ORDER BY id"),
            &mut diffs,
        );
        // hint with a join
        check("indexed by in join", fq(&f, "SELECT a.id,b.id FROM t a INDEXED BY ik JOIN t b ON a.k=b.k WHERE a.id<b.id ORDER BY a.id,b.id").await,
              rq(&r, "SELECT a.id,b.id FROM t a INDEXED BY ik JOIN t b ON a.k=b.k WHERE a.id<b.id ORDER BY a.id,b.id"), &mut diffs);
        // non-existent index -> error on both
        check(
            "no such index",
            fq(&f, "SELECT id FROM t INDEXED BY nope WHERE k=10").await,
            rq(&r, "SELECT id FROM t INDEXED BY nope WHERE k=10"),
            &mut diffs,
        );
        // an index the WHERE cannot use -> error on both (INDEXED BY forces it)
        check(
            "index not usable",
            fq(&f, "SELECT id FROM t INDEXED BY ik WHERE v='a'").await,
            rq(&r, "SELECT id FROM t INDEXED BY ik WHERE v='a'"),
            &mut diffs,
        );

        // DELETE / UPDATE with INDEXED BY
        ex(&f, &r, "UPDATE t INDEXED BY ik SET v='X' WHERE k=30").await;
        check(
            "update indexed by",
            fq(&f, "SELECT id,v FROM t WHERE k=30 ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t WHERE k=30 ORDER BY id"),
            &mut diffs,
        );
        ex(&f, &r, "DELETE FROM t NOT INDEXED WHERE k=10").await;
        check(
            "delete not indexed",
            fq(&f, "SELECT count(*) FROM t").await,
            rq(&r, "SELECT count(*) FROM t"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} INDEXED BY divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
        f.close().await.expect("close FrankenSQLite oracle");
    });
}

#[test]
fn gh381_composite_equality_prefix_serves_order_by_without_sorter() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE quotes(quote_id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, observed_at TEXT NOT NULL, payload TEXT)",
            "CREATE INDEX idx_quotes_symbol ON quotes(symbol ASC, observed_at DESC)",
            "CREATE INDEX idx_quotes_observed_at ON quotes(observed_at)",
            "INSERT INTO quotes VALUES (1,'ABC','2026-01-01','a'),(2,'ABC','2026-01-03','b'),(3,'ABC','2026-01-02','c'),(4,'XYZ','2026-01-04','d')",
        ] {
            ex(&f, &r, sql).await;
        }

        for query in [
            "SELECT quote_id FROM quotes WHERE symbol='ABC' ORDER BY observed_at DESC LIMIT 2",
            "SELECT quote_id FROM quotes WHERE symbol='ABC' ORDER BY observed_at ASC LIMIT 2",
            "SELECT quote_id FROM quotes INDEXED BY idx_quotes_symbol WHERE symbol='ABC' ORDER BY observed_at DESC LIMIT 2",
        ] {
            assert_eq!(fq(&f, query).await, rq(&r, query), "row divergence for {query}");

            let details = fq(&f, &format!("EXPLAIN QUERY PLAN {query}")).await;
            let flattened = details
                .iter()
                .flatten()
                .cloned()
                .collect::<Vec<_>>()
                .join("\n");
            let has_composite_search = details.iter().flatten().any(|field| {
                field.contains("SEARCH") && field.contains("idx_quotes_symbol")
            });
            assert!(
                has_composite_search,
                "the equality prefix must search the composite index for {query}, got {details:?}"
            );
            assert!(
                !flattened.contains("TEMP B-TREE"),
                "the ordered index must eliminate the sorter for {query}, got {details:?}"
            );
        }

        let not_indexed = "SELECT quote_id FROM quotes NOT INDEXED WHERE symbol='ABC' ORDER BY observed_at DESC LIMIT 2";
        assert_eq!(
            fq(&f, not_indexed).await,
            rq(&r, not_indexed),
            "NOT INDEXED row divergence"
        );
        let not_indexed_details = fq(&f, &format!("EXPLAIN QUERY PLAN {not_indexed}")).await;
        let not_indexed_plan = not_indexed_details
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !not_indexed_plan.contains("idx_quotes_symbol"),
            "NOT INDEXED must forbid the composite index, got {not_indexed_details:?}"
        );
        assert!(
            not_indexed_plan.contains("TEMP B-TREE"),
            "NOT INDEXED must retain the ORDER BY sorter, got {not_indexed_details:?}"
        );

        f.close().await.expect("close FrankenSQLite GH#381 oracle");
    });
}
