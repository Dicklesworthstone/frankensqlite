#![recursion_limit = "512"]

//! Confirmation keepers for three GH-triage beads found already-fixed at HEAD
//! but lacking exact coverage:
//!   * GH #234 (bd-gh-having-agg-filter): aggregate FILTER honored in a grouped
//!     HAVING term.
//!   * GH #235 (bd-gh-having-agg-filter-match): aggregate FILTER honored in a
//!     HAVING term with no GROUP BY.
//!   * GH #269 (bd-gh-groupby-fn-resolution): GROUP BY of an unknown function
//!     (e.g. ROLLUP/CUBE, unsupported by SQLite) raises "no such function".
//! rusqlite is the oracle; these pin the fixed behavior so it cannot regress.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
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
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let f: Result<Vec<Vec<String>>, ()> = match fconn.query(sql).await {
        Ok(rows) => {
            let mut v: Vec<Vec<String>> = rows
                .iter()
                .map(|r| r.values().iter().map(tag_f).collect())
                .collect();
            v.sort();
            Ok(v)
        }
        Err(_) => Err(()),
    };
    let r: Result<Vec<Vec<String>>, ()> = (|| {
        let mut st = rconn.prepare(sql).map_err(|_| ())?;
        let n = st.column_count();
        let mut rows: Vec<Vec<String>> = st
            .query_map([], |row| {
                Ok((0..n)
                    .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect())
            })
            .map_err(|_| ())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ())?;
        rows.sort();
        Ok(rows)
    })();
    match (&f, &r) {
        (Ok(fr), Ok(rr)) => assert_eq!(fr, rr, "row mismatch on `{sql}`"),
        (Err(()), Err(())) => {}
        _ => panic!("error-vs-rows divergence on `{sql}`\n  frank: {f:?}\n  csql:  {r:?}"),
    }
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "CREATE TABLE t (g INTEGER, x INTEGER)",
        "INSERT INTO t VALUES (1, 5), (1, -2), (2, 10), (2, -3), (2, 4)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn having_aggregate_filter_grouped_gh234() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // FILTER restricts the aggregate inside the HAVING term (per group).
        assert_agree(
            &f,
            &r,
            "SELECT g FROM t GROUP BY g HAVING sum(x) FILTER (WHERE x > 0) > 6 ORDER BY g",
        )
        .await;
        assert_agree(&f, &r, "SELECT g, count(*) FILTER (WHERE x > 0) AS pos FROM t GROUP BY g HAVING count(*) FILTER (WHERE x > 0) = 1 ORDER BY g").await;
    });
}

#[test]
fn having_aggregate_filter_no_group_by_gh235() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Implicit single group: FILTER applies inside the HAVING aggregate.
        assert_agree(
            &f,
            &r,
            "SELECT sum(x) FROM t HAVING count(*) FILTER (WHERE x > 0) = 3",
        )
        .await;
        assert_agree(
            &f,
            &r,
            "SELECT sum(x) FROM t HAVING sum(x) FILTER (WHERE x > 0) > 100",
        )
        .await;
    });
}

#[test]
fn group_by_unknown_function_errors_gh269() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // ROLLUP/CUBE are not SQLite functions: a GROUP BY that calls one must
        // raise "no such function", not silently accept it.
        assert_agree(&f, &r, "SELECT g, count(*) FROM t GROUP BY rollup(g)").await;
        assert_agree(&f, &r, "SELECT g FROM t GROUP BY totally_unknown_fn(g)").await;
    });
}

/// bd-4uurt: the prepared file path lowers HAVING to VDBE bytecode, while
/// direct queries and in-memory grouping use the connection evaluator.
/// Compare actual rows across both paths, including NULL-safe equality and
/// SQLite's distinct boolean-test semantics for IS TRUE / IS FALSE.
#[test]
fn grouped_having_is_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("having.db");
            let f = Connection::open(if file_backed {
                path.to_str().unwrap()
            } else {
                ":memory:"
            })
            .await
            .unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE t(a, b, v)",
                "INSERT INTO t VALUES (1,2,10),(1,2,11),(1,3,12),(2,2,13),\
                 (NULL,2,NULL),(0,2,0),('abc',2,NULL),(3,NULL,NULL)",
            ] {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            for predicate in [
                "a IS 2",
                "a IS NOT 2",
                "a IS NULL",
                "a IS NOT NULL",
                "a IS b",
                "a IS NOT b",
                "a IS TRUE",
                "a IS FALSE",
                "a IS NOT TRUE",
                "a IS NOT FALSE",
                "count(*) IS 2",
                "count(*) IS NOT 2",
                "sum(v) IS NULL",
                "sum(v) IS NOT NULL",
                "sum(v) IS TRUE",
                "sum(v) IS FALSE",
                "sum(v) IS NOT TRUE",
                "sum(v) IS NOT FALSE",
                "(a IS 2 OR a IS NULL) AND count(*) IS 1",
                "NOT (a IS 2)",
                "NOT (sum(v) IS NULL)",
                // Controls: ordinary equality keeps SQL NULL propagation.
                "a = 2",
                "a <> 2",
                "a = NULL",
                "count(*) = 2",
                "sum(v) <> 0",
            ] {
                let sql = format!(
                    "SELECT a,b,count(*),sum(v) FROM t GROUP BY a,b \
                     HAVING {predicate} ORDER BY a,b"
                );
                let mut statement = r.prepare(&sql).unwrap();
                let expected: Vec<Vec<String>> = statement
                    .query_map([], |row| {
                        (0..4)
                            .map(|i| row.get::<_, rusqlite::types::Value>(i).map(|v| tag_r(&v)))
                            .collect()
                    })
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
                let direct = f.query(&sql).await.unwrap();
                assert_eq!(
                    direct
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                        .collect::<Vec<_>>(),
                    expected,
                    "direct, file={file_backed}: {sql}"
                );
                let prepared = f.prepare(&sql).await.unwrap();
                assert_eq!(
                    prepared
                        .query()
                        .await
                        .unwrap()
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                        .collect::<Vec<_>>(),
                    expected,
                    "prepared, file={file_backed}: {sql}"
                );
            }
            f.close().await.unwrap();
        }
    });
}

/// Strict mode refuses the row-mirror fallback, so these assertions prove
/// both direct and prepared queries actually run against file-backed storage.
#[test]
fn prepared_file_grouped_having_is_uses_storage() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("having-strict.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a INTEGER, b TEXT, v INTEGER)",
            "INSERT INTO t VALUES (1,'A',10),(1,'A',11),(2,'a',0),(NULL,NULL,NULL)",
        ] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        f.execute("PRAGMA fsqlite.parity_cert_strict = ON")
            .await
            .unwrap();
        for predicate in [
            "a IS 2",
            "a IS NOT 2",
            "a IS NULL",
            "a IS NOT NULL",
            "count(*) IS 2",
            "count(*) IS NOT 2",
            "sum(v) IS NULL",
            "sum(v) IS NOT NULL",
            "a IS '2'",
            "b IS 'a' COLLATE NOCASE",
            "b COLLATE NOCASE IS 'a'",
            "NOT (a IS 2)",
            "A IS 2",
            "a = 2",
            "a = '2'",
        ] {
            let sql = format!(
                "SELECT a,b,count(*),sum(v) FROM t GROUP BY a,b HAVING {predicate} ORDER BY a,b"
            );
            let mut statement = r.prepare(&sql).unwrap();
            let expected: Vec<Vec<String>> = statement
                .query_map([], |row| {
                    (0..4)
                        .map(|i| row.get::<_, rusqlite::types::Value>(i).map(|v| tag_r(&v)))
                        .collect()
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            let prepared = f.prepare(&sql).await.unwrap();
            let actual = prepared.query().await.unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
                expected,
                "strict prepared: {sql}"
            );
            let direct = f.query(&sql).await.unwrap();
            assert_eq!(
                direct
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
                expected,
                "strict direct: {sql}"
            );
        }
        f.close().await.unwrap();
    });
}

/// Sharing the direct/prepared admission decision must not expose operands
/// that the grouped bytecode cannot resolve, or conflate aggregate variants.
#[test]
fn grouped_having_is_keeps_complex_operands_on_the_semantic_path() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("having-operands.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a INTEGER, b INTEGER, v INTEGER)",
            "INSERT INTO t VALUES (1,1,5),(1,1,5),(2,3,0),(2,3,NULL),(3,2,9)",
        ] {
            f.execute(sql).await.unwrap();
            r.execute_batch(sql).unwrap();
        }
        for sql in [
            "SELECT count(*) FROM t GROUP BY a HAVING a IS 2 ORDER BY a",
            "SELECT a,count(*) FROM t GROUP BY a HAVING a+1 IS 3 ORDER BY a",
            "SELECT a,count(*) AS n FROM t GROUP BY a HAVING n+1 IS 3 ORDER BY a",
            "SELECT a,sum(v),sum(DISTINCT v) FROM t GROUP BY a \
             HAVING sum(DISTINCT v) IS 5 ORDER BY a",
            "SELECT a,count(*) AS b FROM t GROUP BY a HAVING b IS 1 ORDER BY a",
            "SELECT a,count(*) AS n FROM t GROUP BY a HAVING n IS 2 ORDER BY a",
            "SELECT q.a,count(*) FROM t AS q GROUP BY q.a HAVING q.a IS '2' ORDER BY q.a",
            "SELECT a AS k,count(*) FROM t GROUP BY a HAVING k IS 2 ORDER BY a",
        ] {
            let mut statement = r.prepare(sql).unwrap();
            let columns = statement.column_count();
            let expected: Vec<Vec<String>> = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|i| row.get::<_, rusqlite::types::Value>(i).map(|v| tag_r(&v)))
                        .collect()
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            let prepared = f.prepare(sql).await.unwrap();
            for (mode, rows) in [
                ("direct", f.query(sql).await.unwrap()),
                ("prepared", prepared.query().await.unwrap()),
            ] {
                assert_eq!(
                    rows.iter()
                        .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                        .collect::<Vec<_>>(),
                    expected,
                    "{mode}: {sql}"
                );
            }
        }
        f.close().await.unwrap();
    });
}
