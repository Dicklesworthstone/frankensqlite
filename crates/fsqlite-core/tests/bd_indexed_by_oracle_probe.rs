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

#[test]
fn gh523_indexed_by_order_by_trailing_rowid_and_missing_hint() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE m(id INTEGER PRIMARY KEY, c INTEGER, i INTEGER, t TEXT, UNIQUE(c, i))",
            "INSERT INTO m VALUES (1, 10, 3, 'a'), (2, 10, 1, 'b'), (3, 10, 2, 'c'), (4, 20, 1, 'd')",
        ] {
            ex(&f, &r, sql).await;
        }

        let query = "SELECT id FROM m INDEXED BY sqlite_autoindex_m_1 WHERE c = ?1 ORDER BY i, id LIMIT ?2";
        let eqp = fq(&f, &format!("EXPLAIN QUERY PLAN {query}")).await;
        let eqp_text = eqp
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            eqp_text.contains("SEARCH m USING COVERING INDEX sqlite_autoindex_m_1 (c=?)"),
            "GH#523: expected covering index search without sorter, got {eqp:?}"
        );
        assert!(
            !eqp_text.contains("TEMP B-TREE"),
            "GH#523: trailing rowid ORDER BY term must not add a sorter, got {eqp:?}"
        );

        let stmt = f.prepare(query).await.unwrap();
        let rows = stmt
            .query_with_params(&[
                fsqlite_types::value::SqliteValue::Integer(10),
                fsqlite_types::value::SqliteValue::Integer(2),
            ])
            .await
            .unwrap();
        let ids: Vec<i64> = rows
            .iter()
            .map(|row| match row.values()[0] {
                fsqlite_types::value::SqliteValue::Integer(v) => v,
                ref other => panic!("expected integer id, got {other:?}"),
            })
            .collect();
        assert_eq!(ids, vec![2, 3]);

        // Part 2: missing hinted index must fail at prepare/query_with_params/query
        // in both ORDER BY i and ORDER BY i, id shapes.
        f.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, c INTEGER, i INTEGER, t TEXT)")
            .await
            .unwrap();
        for bad_sql in [
            "SELECT id FROM u INDEXED BY sqlite_autoindex_u_1 WHERE c = ?1 ORDER BY i LIMIT ?2",
            "SELECT id FROM u INDEXED BY sqlite_autoindex_u_1 WHERE c = ?1 ORDER BY i, id LIMIT ?2",
        ] {
            let prep_err = match f.prepare(bad_sql).await {
                Ok(_) => panic!("prepare must reject missing INDEXED BY for {bad_sql}"),
                Err(err) => err,
            };
            assert!(
                prep_err.to_string().contains("no such index: sqlite_autoindex_u_1"),
                "expected no such index error on prepare for {bad_sql}, got {prep_err:?}"
            );
            let qwp_err = f
                .query_with_params(
                    bad_sql,
                    &[
                        fsqlite_types::value::SqliteValue::Integer(10),
                        fsqlite_types::value::SqliteValue::Integer(2),
                    ],
                )
                .await
                .expect_err("query_with_params must reject missing INDEXED BY");
            assert!(
                qwp_err.to_string().contains("no such index: sqlite_autoindex_u_1"),
                "expected no such index error on query_with_params for {bad_sql}, got {qwp_err:?}"
            );
        }

        f.close().await.expect("close FrankenSQLite GH#523 oracle");
    });
}

#[test]
fn gh526_before_update_trigger_when_correlated_exists_prefix_seek() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        for sql in [
            "CREATE TABLE t (
                id TEXT PRIMARY KEY,
                c1 TEXT NOT NULL, c2 TEXT NOT NULL, c3 TEXT NOT NULL, c4 TEXT NOT NULL,
                c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT NOT NULL,
                c9 TEXT, c10 TEXT, c11 INTEGER, c12 TEXT, c13 TEXT, c14 TEXT, c15 TEXT
            )",
            "CREATE INDEX x1 ON t (c3, c4, c8 DESC, c1)",
            "CREATE INDEX x2 ON t (c1, c8 DESC)",
            "CREATE TRIGGER trg_when BEFORE UPDATE ON t
             WHEN EXISTS (
                 SELECT 1 FROM t AS v
                 WHERE v.id IS NOT OLD.id AND v.c14 IS NOT NULL
                   AND v.c1 = NEW.c1 AND v.c2 = NEW.c2 AND v.c3 = NEW.c3 AND v.c4 = NEW.c4
                   AND v.c5 IS NEW.c5 AND v.c6 IS NEW.c6 AND v.c7 IS NEW.c7
                   AND v.c8 = NEW.c8
                   AND v.c9 IS NEW.c9 AND v.c10 IS NEW.c10 AND v.c11 IS NEW.c11
                   AND v.c12 IS NEW.c12 AND v.c13 IS NEW.c13
                 ORDER BY v.c3, v.c4, v.c8 DESC LIMIT 1
             )
             BEGIN
                 SELECT RAISE(ABORT, 'dup');
             END",
        ] {
            f.execute(sql).await.unwrap();
        }

        f.execute("BEGIN").await.unwrap();
        let insert = f
            .prepare(
                "INSERT INTO t VALUES (?1,'s1','p1','us-gaap',?2,'U','FY','10-K',?3,'f1','2025-02-01',0,'main',NULL,?4,'r1')",
            )
            .await
            .unwrap();
        // bd-kwar3: one tag per row. With `i % 200` tags, rows i and i + 1400
        // agreed on every compared column, so the first UPDATE raised 'dup'
        // (stock does too) before the timing bound was ever reached.
        for i in 0..2_000_i64 {
            insert
                .execute_with_params(&[
                    fsqlite_types::value::SqliteValue::Text(format!("id{i}").into()),
                    fsqlite_types::value::SqliteValue::Text(format!("tag{i}").into()),
                    fsqlite_types::value::SqliteValue::Text(format!("2024-12-{:02}", (i % 28) + 1).into()),
                    fsqlite_types::value::SqliteValue::Text(format!("h{i}").into()),
                ])
                .await
                .unwrap();
        }
        f.execute("COMMIT").await.unwrap();

        let started = std::time::Instant::now();
        f.execute("BEGIN").await.unwrap();
        let update = f
            .prepare("UPDATE t SET c14 = ?1 WHERE id = ?2")
            .await
            .unwrap();
        for i in 0..64_i64 {
            update
                .execute_with_params(&[
                    fsqlite_types::value::SqliteValue::Text(format!("nh{i}").into()),
                    fsqlite_types::value::SqliteValue::Text(format!("id{i}").into()),
                ])
                .await
                .unwrap();
        }
        f.execute("COMMIT").await.unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "GH#526: BEFORE UPDATE WHEN EXISTS took {elapsed:?} for 64 updates over 2,000 rows"
        );

        // Also verify duplicate detection still triggers RAISE(ABORT, 'dup') with SQLITE_CONSTRAINT_TRIGGER.
        f.execute("INSERT INTO t VALUES ('dup1','s1','p1','us-gaap','tag0','U','FY','10-K','2024-12-01','f1','2025-02-01',0,'main',NULL,NULL,'r1')")
            .await
            .unwrap();
        let err = f
            .execute("UPDATE t SET c14 = 'h_dup' WHERE id = 'dup1'")
            .await
            .expect_err("duplicate update must fire BEFORE UPDATE WHEN EXISTS trigger");
        assert_eq!(err.to_string(), "dup");
        assert_eq!(err.error_code(), fsqlite_error::ErrorCode::Constraint);
        assert_eq!(err.extended_error_code(), 19 | (7 << 8));

        f.close().await.expect("close FrankenSQLite GH#526 oracle");
    });
}
