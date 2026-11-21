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
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    })
    .unwrap_or_else(|e| panic!("{sql}: {e}"))
    .collect::<Result<Vec<_>, _>>()
    .unwrap_or_else(|e| panic!("{sql}: {e}"))
}
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    f.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    r.execute(sql, []).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[test]
fn forced_partial_index_binds_the_visible_self_join_source() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aliases.db");
        for filename in [":memory:", path.to_str().unwrap()] {
            let f = Connection::open(filename).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO t VALUES(1,0),(2,1),(3,2)",
                "CREATE INDEX pos ON t(id) WHERE t.a>0",
            ] {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            for join in ["JOIN", "LEFT JOIN"] {
                for (source, valid) in [("x", true), ("t", false)] {
                    let sql = format!(
                        "SELECT x.id FROM t AS x INDEXED BY pos {join} t ON t.id=2
                                       WHERE {source}.a>0 ORDER BY x.id"
                    );
                    if valid {
                        let expected = vec![vec!["int:2"], vec!["int:3"]];
                        assert_eq!(rq(&r, &sql), expected);
                        assert_eq!(fq(&f, &sql).await, expected);
                        let prepared = f.prepare(&sql).await.unwrap();
                        let ids: Vec<_> = prepared
                            .query()
                            .await
                            .unwrap()
                            .iter()
                            .map(|row| row.values()[0].clone())
                            .collect();
                        assert_eq!(ids, [SqliteValue::Integer(2), SqliteValue::Integer(3)]);
                    } else {
                        assert!(
                            r.prepare(&sql)
                                .unwrap_err()
                                .to_string()
                                .contains("no query solution")
                        );
                        assert!(
                            f.prepare(&sql)
                                .await
                                .unwrap_err()
                                .to_string()
                                .contains("no query solution"),
                            "{sql}"
                        );
                        assert!(
                            f.query(&sql)
                                .await
                                .unwrap_err()
                                .to_string()
                                .contains("no query solution"),
                            "{sql}"
                        );
                    }
                }
            }
            f.close().await.unwrap();
        }
    });
}

#[test]
fn collated_boolean_truth_tests_preserve_rows_and_index_membership() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truth.db");
        for filename in [":memory:", path.to_str().unwrap()] {
            let f = Connection::open(filename).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE t(id INTEGER PRIMARY KEY,a,k,marked INTEGER DEFAULT 0)",
                "INSERT INTO t(id,a,k) VALUES (1,NULL,1),(2,0,1),(3,1,1),(4,2,1),(5,'no',1),(6,'2x',1)",
                "CREATE INDEX present ON t(k) WHERE a IS NOT NULL",
            ] {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            for (predicate, ids) in [
                ("a IS (TRUE COLLATE NOCASE)", vec![3, 4, 6]),
                ("a IS ((FALSE COLLATE RTRIM) COLLATE NOCASE)", vec![2, 5]),
                ("a IS NOT (TRUE COLLATE RTRIM)", vec![1, 2, 5]),
                ("a IS NOT (FALSE COLLATE NOCASE)", vec![1, 3, 4, 6]),
                ("(TRUE COLLATE NOCASE) IS a", vec![3]),
                ("a IS (1 COLLATE NOCASE)", vec![3]),
            ] {
                let sql = format!("SELECT id FROM t WHERE {predicate} ORDER BY id");
                let expected: Vec<_> = ids.iter().map(|id| vec![format!("int:{id}")]).collect();
                assert_eq!(rq(&r, &sql), expected, "stock: {sql}");
                assert_eq!(fq(&f, &sql).await, expected, "{filename}: {sql}");
                let prepared = f.prepare(&sql).await.unwrap();
                let got: Vec<_> = prepared
                    .query()
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values()[0].clone())
                    .collect();
                assert_eq!(
                    got,
                    ids.into_iter()
                        .map(SqliteValue::Integer)
                        .collect::<Vec<_>>()
                );
            }
            for (pred, valid) in [
                ("a IS (TRUE COLLATE NOCASE)", true),
                ("a IS FALSE", true),
                ("a", true),
                ("NOT a", true),
                ("a IS NOT TRUE", false),
                ("TRUE IS a", false),
                ("a IS 1", false),
            ] {
                let sql =
                    format!("SELECT id FROM t INDEXED BY present WHERE k=1 AND {pred} ORDER BY id");
                if valid {
                    assert_eq!(fq(&f, &sql).await, rq(&r, &sql));
                } else {
                    assert!(
                        r.prepare(&sql)
                            .unwrap_err()
                            .to_string()
                            .contains("no query solution")
                    );
                    assert!(
                        f.prepare(&sql)
                            .await
                            .unwrap_err()
                            .to_string()
                            .contains("no query solution"),
                        "{sql}"
                    );
                }
                let eqp = format!("EXPLAIN QUERY PLAN SELECT id FROM t WHERE k=1 AND {pred}");
                for plan in [rq(&r, &eqp), fq(&f, &eqp).await] {
                    assert_eq!(
                        plan.iter().flatten().any(|s| s.contains("present")),
                        valid,
                        "{eqp}: {plan:?}"
                    );
                }
            }
            for (sql, expected) in [
                (
                    "SELECT 2 IS (TRUE COLLATE nocase), 2 IS (+TRUE), 2 IS (1=1), NULL IS NOT (FALSE COLLATE rtrim)",
                    vec![vec!["int:1", "int:0", "int:0", "int:1"]],
                ),
                (
                    "SELECT 2 IS (TRUE COLLATE absent_collation)",
                    vec![vec!["int:1"]],
                ),
                (
                    "SELECT k FROM t GROUP BY k HAVING count(*) IS (TRUE COLLATE rtrim)",
                    vec![vec!["int:1"]],
                ),
                (
                    "SELECT x.id FROM t AS x JOIN t AS y ON y.id=3 WHERE x.a IS (TRUE COLLATE nocase) ORDER BY x.id",
                    vec![vec!["int:3"], vec!["int:4"], vec!["int:6"]],
                ),
            ] {
                assert_eq!(rq(&r, sql), expected, "{sql}");
                assert_eq!(fq(&f, sql).await, expected, "{sql}");
            }
            for sql in [
                "SELECT (2 IS (TRUE COLLATE absent_collation))=1",
                "SELECT 'a' COLLATE absent_collation = 'A'",
            ] {
                assert!(
                    r.prepare(sql)
                        .unwrap_err()
                        .to_string()
                        .contains("no such collation sequence")
                );
                assert!(
                    f.query(sql)
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains("no such collation sequence"),
                    "{sql}"
                );
            }
            let bound = "SELECT id FROM t WHERE a IS (?1 COLLATE nocase) ORDER BY id";
            let plan = f.prepare(bound).await.unwrap();
            for (param, id) in [(SqliteValue::Integer(1), 3), (SqliteValue::Null, 1)] {
                let rows = plan.query_with_params(&[param]).await.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values()[0], SqliteValue::Integer(id));
            }
            drop(plan);
            let create = "CREATE INDEX truth ON t(id) WHERE a IS (TRUE COLLATE NOCASE)";
            ex(&f, &r, create).await;
            let sql = "UPDATE t SET marked=1 WHERE a IS (TRUE COLLATE NOCASE)";
            assert_eq!(r.execute(sql, []).unwrap(), 3);
            assert_eq!(f.execute(sql).await.unwrap(), 3);
            let move_row = "UPDATE t SET a='text' WHERE id=4";
            assert_eq!(r.execute(move_row, []).unwrap(), 1);
            assert_eq!(f.execute(move_row).await.unwrap(), 1);
            f.close().await.unwrap();
            if filename != ":memory:" {
                let reopened = rusqlite::Connection::open(filename).unwrap();
                assert_eq!(
                    rq(&reopened, "PRAGMA integrity_check"),
                    vec![vec!["text:ok"]]
                );
                assert_eq!(
                    rq(
                        &reopened,
                        "SELECT id FROM t INDEXED BY truth WHERE a IS (TRUE COLLATE NOCASE) ORDER BY id"
                    ),
                    vec![vec!["int:3"], vec!["int:6"]]
                );
                assert_eq!(
                    rq(&reopened, "SELECT id FROM t WHERE marked=1 ORDER BY id"),
                    vec![vec!["int:3"], vec!["int:4"], vec!["int:6"]]
                );
            }
        }
    });
}

#[test]
fn forced_partial_index_respects_comparison_collation() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("collations.db");
        for filename in [":memory:", file.to_str().unwrap()] {
            let f = Connection::open(filename).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT COLLATE NOCASE,
                                c TEXT COLLATE NOCASE, dirty INTEGER DEFAULT 0)",
                "INSERT INTO t(id,a,b,c) VALUES(1,'A','a','A'),(2,'b','b','b'),(3,'x','y','z')",
                "CREATE INDEX ab ON t(id) WHERE a=b",
                "CREATE INDEX bc ON t(id) WHERE b=c",
                "CREATE INDEX explicit_pair ON t(id) WHERE a COLLATE NOCASE=b COLLATE BINARY",
                "CREATE INDEX different ON t(id) WHERE b!=c",
                "CREATE INDEX distinct_pair ON t(id) WHERE b IS NOT c",
                "CREATE INDEX truthy ON t(id) WHERE id IS TRUE",
            ] {
                r.execute_batch(sql).unwrap();
                f.execute(sql).await.unwrap();
            }

            // Stock rejects the write before touching any rows, even though the
            // unforced predicate really does match both 1 ('A'/'a') and 2.
            for sql in [
                "UPDATE t INDEXED BY ab SET dirty=1 WHERE b=a",
                "DELETE FROM t INDEXED BY explicit_pair WHERE b COLLATE BINARY=a COLLATE NOCASE",
            ] {
                let oracle = r.execute(sql, []).unwrap_err();
                assert!(oracle.to_string().contains("no query solution"), "{sql}: {oracle}");
                let err = f.execute(sql).await.unwrap_err();
                assert!(err.to_string().contains("no query solution"), "{sql}: {err}");
                let err = f.prepare(sql).await.expect_err("invalid write prepared");
                assert!(err.to_string().contains("no query solution"), "{sql}: {err}");
            }
            for sql in [
                "SELECT id FROM t AS q INDEXED BY ab WHERE q.b=q.a",
                "EXPLAIN QUERY PLAN SELECT id FROM t INDEXED BY ab WHERE b=a",
                "EXPLAIN QUERY PLAN UPDATE t INDEXED BY ab SET dirty=2 WHERE b=a",
                "SELECT id FROM t INDEXED BY different WHERE c!=b",
                "SELECT id FROM t INDEXED BY distinct_pair WHERE c IS NOT b",
                "SELECT id FROM t INDEXED BY truthy WHERE TRUE IS id",
            ] {
                let oracle = r.prepare(sql).unwrap_err();
                assert!(oracle.to_string().contains("no query solution"), "{sql}: {oracle}");
                let err = f.query(sql).await.unwrap_err();
                assert!(err.to_string().contains("no query solution"), "{sql}: {err}");
            }
            for (sql, ids) in [
                ("SELECT id FROM t WHERE b=a ORDER BY id", vec![1, 2]),
                ("SELECT id FROM t AS q INDEXED BY ab WHERE q.a=q.b ORDER BY id", vec![2]),
                ("SELECT id FROM t INDEXED BY bc WHERE c=b ORDER BY id", vec![1, 2]),
                ("SELECT id FROM t INDEXED BY ab WHERE (a=b) COLLATE RTRIM ORDER BY id", vec![2]),
                ("SELECT id FROM t INDEXED BY ab WHERE (a=b AND dirty=0) COLLATE RTRIM ORDER BY id", vec![2]),
                ("SELECT id FROM t INDEXED BY explicit_pair WHERE a COLLATE NOCASE=b COLLATE BINARY ORDER BY id", vec![1, 2]),
                ("SELECT id FROM t INDEXED BY truthy WHERE id IS TRUE ORDER BY id", vec![1, 2, 3]),
                ("SELECT id FROM t WHERE dirty=0 ORDER BY id", vec![1, 2, 3]),
            ] {
                let expected: Vec<Vec<String>> = ids.iter()
                    .map(|id| vec![format!("int:{id}")])
                    .collect();
                assert_eq!(rq(&r, sql), expected, "stock: {sql}");
                assert_eq!(fq(&f, sql).await, expected, "{filename}: {sql}");
            }
            let lookup = "SELECT id FROM t INDEXED BY bc WHERE (c=b) COLLATE RTRIM AND id>=?1 ORDER BY id";
            let plan = f.prepare(lookup).await.unwrap();
            let mut stock_plan = r.prepare(lookup).unwrap();
            for (floor, ids) in [(1, vec![1, 2]), (2, vec![2]), (3, vec![])] {
                let stock_ids = stock_plan.query_map([floor], |row| row.get::<_, i64>(0))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(stock_ids, ids);
                let rows = plan.query_with_params(&[SqliteValue::Integer(floor)]).await.unwrap();
                assert_eq!(rows.len(), ids.len());
                for (row, id) in rows.iter().zip(ids) {
                    assert_eq!(row.values(), &[SqliteValue::Integer(id)]);
                }
            }
            drop(plan);
            let update = "UPDATE t INDEXED BY bc SET dirty=1 WHERE (c=b) COLLATE RTRIM";
            assert_eq!(r.execute(update, []).unwrap(), 2);
            assert_eq!(f.execute(update).await.unwrap(), 2);
            f.close().await.unwrap();
            if filename != ":memory:" {
                let reopened = rusqlite::Connection::open(filename).unwrap();
                let sql = "SELECT id FROM t WHERE dirty=1 ORDER BY id";
                assert_eq!(rq(&reopened, sql), vec![vec!["int:1"], vec!["int:2"]]);
            }
        }
    });
}

#[test]
fn forced_partial_index_membership_with_an_empty_set() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("membership.db");
        for filename in [":memory:", path.to_str().unwrap()] {
            let f = Connection::open(filename).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, k TEXT)",
                "INSERT INTO t VALUES (1,NULL,'x'),(2,5,'y'),(3,9,'x')",
                "CREATE INDEX present ON t(k) WHERE a IS NOT NULL",
                "CREATE TABLE excluded(val INTEGER)",
            ] {
                r.execute_batch(sql).unwrap();
                f.execute(sql).await.unwrap();
            }
            for predicate in ["a IN ()", "a NOT IN ()", "a NOT IN excluded",
                              "a NOT IN (SELECT val FROM excluded)"] {
                let delete = format!("DELETE FROM t INDEXED BY present WHERE {predicate}");
                let stock = r.execute(&delete, []).unwrap_err();
                assert!(stock.to_string().contains("no query solution"), "{delete}: {stock}");
                let err = f.execute(&delete).await.unwrap_err();
                assert!(err.to_string().contains("no query solution"), "{delete}: {err}");
            }
            let all = "SELECT id FROM t WHERE a NOT IN excluded ORDER BY id";
            let expected = vec![vec!["int:1"], vec!["int:2"], vec!["int:3"]];
            assert_eq!(rq(&r, all), expected);
            assert_eq!(fq(&f, all).await, expected);

            for (membership, uses_partial) in [("IN ()", false), ("IN (9)", true), ("NOT IN ()", false)] {
                let eqp = format!("EXPLAIN QUERY PLAN SELECT id FROM t WHERE k='x' AND a {membership}");
                for plan in [rq(&r, &eqp), fq(&f, &eqp).await] {
                    assert_eq!(plan.iter().flatten().any(|s| s.contains("present")),
                               uses_partial, "{filename}: {eqp}: {plan:?}");
                }
            }

            // The same prepared IN-subquery can run when the set is empty
            // and after it is populated. Neither case can return a NULL lhs.
            let lookup = "SELECT id FROM t INDEXED BY present WHERE a IN excluded";
            let plan = f.prepare(lookup).await.unwrap();
            assert!(rq(&r, lookup).is_empty());
            assert!(plan.query().await.unwrap().is_empty());
            r.execute("INSERT INTO excluded VALUES(5)", []).unwrap();
            f.execute("INSERT INTO excluded VALUES(5)").await.unwrap();
            assert_eq!(rq(&r, lookup), vec![vec!["int:2"]]);
            let rows = plan.query().await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(2)]);
            drop(plan);
            for sql in [
                "SELECT id FROM t INDEXED BY present WHERE a NOT IN (5,7)",
                "SELECT id FROM t INDEXED BY present WHERE a IS NOT NULL AND a NOT IN excluded",
            ] {
                assert_eq!(rq(&r, sql), vec![vec!["int:3"]]);
                assert_eq!(fq(&f, sql).await, vec![vec!["int:3"]]);
            }
            let delete = "DELETE FROM t INDEXED BY present WHERE a NOT IN (5,7)";
            assert_eq!(r.execute(delete, []).unwrap(), 1);
            assert_eq!(f.execute(delete).await.unwrap(), 1);
            let remaining = "SELECT id FROM t ORDER BY id";
            assert_eq!(rq(&r, remaining), vec![vec!["int:1"], vec!["int:2"]]);
            assert_eq!(fq(&f, remaining).await, rq(&r, remaining));
            f.close().await.unwrap();
        }
    });
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
