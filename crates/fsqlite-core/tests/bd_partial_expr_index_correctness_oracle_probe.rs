#![recursion_limit = "512"]

//! Partial/expression index correctness leaf-hunt (pane af49, 2026-08-21): frank
//! vs rusqlite over index-maintenance correctness — a partial index
//! (`WHERE status='active'`) whose membership must track UPDATEs that move rows
//! in/out of the predicate; an expression index (`lower(name)`) used by a
//! matching WHERE; a multi-column index under equality/range; and DELETE/UPDATE
//! churn that must not leave stale index entries. Because an index must never
//! change query RESULTS (only the plan), every indexed query is compared to the
//! rusqlite oracle (which is the ground truth) — a divergence means frank's
//! index read returned wrong rows. Pass = coverage keeper.

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
fn bd_81jud_partial_index_must_cover_the_query() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("facts.db");
        for filename in [":memory:", path.to_str().unwrap()] {
            let f = Connection::open(filename).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let setup = [
                "CREATE TABLE facts(id INTEGER PRIMARY KEY, g INTEGER, h INTEGER, value REAL)",
                "INSERT INTO facts VALUES
                   (1,1,0,-1.0),(2,1,0,-3.0),(3,1,20,5.0),(4,2,0,7.0),(5,1,NULL,100.0)",
            ];
            for sql in setup {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            let self_join = "SELECT b.id FROM facts AS a JOIN facts AS b ON a.g=b.g AND a.h=b.h
                             WHERE a.id=1 ORDER BY b.id";
            let expected_join = vec![vec!["int:1".to_owned()], vec!["int:2".to_owned()]];
            assert_eq!(rq(&r, self_join), expected_join, "stock {filename}");
            let partial_index = "CREATE INDEX partial_facts ON facts(g,h) WHERE h > 10";
            // A point read for the first alias must leave the second alias's
            // scan unrestricted, with or without an index on the table.
            assert_eq!(fq(&f, self_join).await, expected_join, "{filename} without index");
            f.execute(partial_index).await.unwrap();
            r.execute_batch(partial_index).unwrap();
            assert_eq!(rq(&r, self_join), expected_join, "stock {filename} with index");
            assert_eq!(fq(&f, self_join).await, expected_join, "{filename} with index");

            for (predicate, count, sum, uses_partial) in [
                ("g = 1 AND h = 0", 2, -4.0, false),
                ("g = 1 AND h >= 0", 3, 1.0, false),
                ("g = 1 AND h > 10", 1, 5.0, true),
            ] {
                let sql = format!("SELECT count(*), sum(value) FROM main.facts WHERE {predicate}");
                let expected = vec![vec![format!("int:{count}"), format!("real:{sum}")]];
                assert_eq!(rq(&r, &sql), expected, "stock {filename}: {sql}");
                assert_eq!(fq(&f, &sql).await, expected, "{filename}: {sql}");
                let prepared = f.prepare(&sql).await.unwrap();
                let rows = prepared.query().await.unwrap();
                assert_eq!(rows.len(), 1, "prepared {filename}: {sql}");
                assert_eq!(
                    rows[0].values(),
                    &[SqliteValue::Integer(count), SqliteValue::Float(sum)],
                    "prepared {filename}: {sql}"
                );
                let eqp = format!("EXPLAIN QUERY PLAN {sql}");
                for plan in [fq(&f, &eqp).await, rq(&r, &eqp)] {
                    assert_eq!(
                        plan.iter().flatten().any(|value| value.contains("partial_facts")),
                        uses_partial,
                        "{filename}: {eqp}: {plan:?}"
                    );
                }
            }

            let sql = "SELECT id FROM main.facts WHERE g = ?1 AND h >= ?2 ORDER BY id";
            let prepared = f.prepare(sql).await.unwrap();
            let mut stock = r.prepare(sql).unwrap();
            for (floor, expected) in [(0, vec![1, 2, 3]), (10, vec![3]), (21, vec![])] {
                let stock_rows = stock
                    .query_map([1, floor], |row| row.get::<_, i64>(0))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                assert_eq!(stock_rows, expected, "stock bound {floor}");
                let subject_rows = prepared
                    .query_with_params(&[SqliteValue::Integer(1), SqliteValue::Integer(floor)])
                    .await
                    .unwrap();
                assert_eq!(subject_rows.len(), expected.len(), "{filename}, bound {floor}");
                for (row, id) in subject_rows.iter().zip(expected) {
                    assert_eq!(row.values(), &[SqliteValue::Integer(id)], "{filename}");
                }
            }
            for (sql, values) in [
                ("SELECT id FROM facts WHERE g = 1 AND h IN (0,20) ORDER BY id", vec![vec![1], vec![2], vec![3]]),
                ("SELECT id FROM facts WHERE g = 1 AND h >= 0 ORDER BY h DESC, id", vec![vec![3], vec![1], vec![2]]),
                ("SELECT min(h), max(h) FROM facts WHERE g = 1 AND h >= 0", vec![vec![0, 20]]),
            ] {
                let expected: Vec<Vec<String>> = values
                    .into_iter()
                    .map(|row| row.into_iter().map(|value| format!("int:{value}")).collect())
                    .collect();
                assert_eq!(rq(&r, sql), expected, "stock {filename}: {sql}");
                assert_eq!(fq(&f, sql).await, expected, "{filename}: {sql}");
            }
            for (sql, changed) in [
                ("UPDATE facts SET value = 8 WHERE g = 1 AND h = 0", 2),
                ("DELETE FROM facts WHERE g = 1 AND h = 0", 2),
            ] {
                assert_eq!(r.execute(sql, []).unwrap(), changed, "stock {filename}: {sql}");
                assert_eq!(f.execute(sql).await.unwrap(), changed, "{filename}: {sql}");
            }
            let sql = "SELECT id FROM facts ORDER BY id";
            let expected = vec![vec!["int:3".to_owned()], vec!["int:4".to_owned()], vec!["int:5".to_owned()]];
            assert_eq!(rq(&r, sql), expected, "stock after writes");
            assert_eq!(fq(&f, sql).await, expected, "{filename} after writes");
            f.close().await.unwrap();
        }
    });
}

#[test]
fn partial_expr_index_correctness_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE u(id INTEGER PRIMARY KEY, name TEXT, status TEXT, score INTEGER)",
            "CREATE INDEX ix_active ON u(name) WHERE status='active'",
            "CREATE INDEX ix_lower_name ON u(lower(name))",
            "CREATE INDEX ix_score_status ON u(score, status)",
            "INSERT INTO u VALUES \
              (1,'Alice','active',10),(2,'bob','inactive',20),(3,'Carol','active',30),\
              (4,'dave','active',40),(5,'Eve','inactive',50),(6,'alice','active',15)",
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

        // partial index: query matching the partial predicate
        check(
            "partial idx active",
            fq(
                &f,
                "SELECT id,name FROM u WHERE status='active' AND name='Alice' ORDER BY id",
            )
            .await,
            rq(
                &r,
                "SELECT id,name FROM u WHERE status='active' AND name='Alice' ORDER BY id",
            ),
            &mut diffs,
        );
        check(
            "partial idx all active",
            fq(
                &f,
                "SELECT id,name FROM u WHERE status='active' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT id,name FROM u WHERE status='active' ORDER BY name",
            ),
            &mut diffs,
        );
        // expression index: WHERE lower(name)=...
        check(
            "expr idx lower",
            fq(
                &f,
                "SELECT id,name FROM u WHERE lower(name)='alice' ORDER BY id",
            )
            .await,
            rq(
                &r,
                "SELECT id,name FROM u WHERE lower(name)='alice' ORDER BY id",
            ),
            &mut diffs,
        );
        // multi-column index equality + range
        check(
            "multicol idx range",
            fq(
                &f,
                "SELECT id,score FROM u WHERE score>=20 AND score<=40 ORDER BY score",
            )
            .await,
            rq(
                &r,
                "SELECT id,score FROM u WHERE score>=20 AND score<=40 ORDER BY score",
            ),
            &mut diffs,
        );
        check(
            "multicol idx eq",
            fq(
                &f,
                "SELECT id FROM u WHERE score=30 AND status='active' ORDER BY id",
            )
            .await,
            rq(
                &r,
                "SELECT id FROM u WHERE score=30 AND status='active' ORDER BY id",
            ),
            &mut diffs,
        );

        // ── UPDATE moving a row OUT of the partial predicate (status active->inactive) ──
        ex(&f, &r, "UPDATE u SET status='inactive' WHERE id=3").await;
        check(
            "partial after move-out",
            fq(
                &f,
                "SELECT id,name FROM u WHERE status='active' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT id,name FROM u WHERE status='active' ORDER BY name",
            ),
            &mut diffs,
        );
        // ── UPDATE moving a row INTO the partial predicate (inactive->active) ──
        ex(&f, &r, "UPDATE u SET status='active' WHERE id=2").await;
        check(
            "partial after move-in",
            fq(
                &f,
                "SELECT id,name FROM u WHERE status='active' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT id,name FROM u WHERE status='active' ORDER BY name",
            ),
            &mut diffs,
        );
        // ── UPDATE the indexed expression's base column (rename) ──
        ex(&f, &r, "UPDATE u SET name='ALICE' WHERE id=1").await;
        check(
            "expr idx after rename",
            fq(
                &f,
                "SELECT id,name FROM u WHERE lower(name)='alice' ORDER BY id",
            )
            .await,
            rq(
                &r,
                "SELECT id,name FROM u WHERE lower(name)='alice' ORDER BY id",
            ),
            &mut diffs,
        );
        // ── DELETE a row and confirm no stale index entry ──
        ex(&f, &r, "DELETE FROM u WHERE id=4").await;
        check(
            "partial after delete",
            fq(
                &f,
                "SELECT id,name FROM u WHERE status='active' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT id,name FROM u WHERE status='active' ORDER BY name",
            ),
            &mut diffs,
        );
        check(
            "expr idx after delete",
            fq(&f, "SELECT id FROM u WHERE lower(name)='dave'").await,
            rq(&r, "SELECT id FROM u WHERE lower(name)='dave'"),
            &mut diffs,
        );
        // ── UPDATE the score used by the multi-column index ──
        ex(&f, &r, "UPDATE u SET score=score+100 WHERE status='active'").await;
        check(
            "multicol after score bump",
            fq(&f, "SELECT id,score FROM u WHERE score>=100 ORDER BY score").await,
            rq(&r, "SELECT id,score FROM u WHERE score>=100 ORDER BY score"),
            &mut diffs,
        );

        // full-scan cross-check: the same predicates without any index-implied filter must agree too
        check(
            "full state",
            fq(&f, "SELECT id,name,status,score FROM u ORDER BY id").await,
            rq(&r, "SELECT id,name,status,score FROM u ORDER BY id"),
            &mut diffs,
        );
        // count via the partial predicate
        check(
            "partial count",
            fq(&f, "SELECT count(*) FROM u WHERE status='active'").await,
            rq(&r, "SELECT count(*) FROM u WHERE status='active'"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} partial/expr-index divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
