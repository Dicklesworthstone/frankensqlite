#![recursion_limit = "512"]

//! bd-ukvhn: a GROUP BY key over a NOCASE or RTRIM column has several
//! spellings within one group (`'Name-0'`, `'name-0'`, `'NAME-0'`). Stock
//! prints the spelling of the row its bare columns come from: the row of the
//! group's single min()/max() when the query has one (SQLite copies the
//! accumulator columns, the key among them, only on a row where min()/max()
//! does not skip), otherwise the group's first row. fsqlite printed the
//! first row's spelling even beside a min()/max(), because the bare-column
//! row choice counted a GROUP BY key as no bare column at all.
//!
//! Both input orders are covered (`f` and `w` hold the same rows in opposite
//! rowid order), with a min()/max() and without one, FILTER and DISTINCT
//! forms, an expression of the key, a key with an explicit COLLATE, the
//! grouped join path, and the negative cases: DISTINCT keeps the first
//! spelling, a query without min()/max() keeps the first row, and a BINARY key
//! has one spelling.
//!
//! Every statement is compared, rows in order, with rusqlite's bundled SQLite
//! 3.54.1, in memory and in a file, ad hoc and prepared. In memory with
//! parity-cert strict on, a statement must answer as stock or be refused:
//! strict mode refuses the grouped evaluator these keys run on.
//!
//! bd-36aed holds the same rule for an untyped key whose group mixes INTEGER
//! and REAL spellings (3 and 3.0), on the VDBE GROUP BY program: see
//! `mixed_integer_real_group_by_key_spelling_matches_stock_on_a_strict_file_database`.

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

type Rows = Result<Vec<Vec<String>>, String>;

fn tag_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}

fn stock_err(e: rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqlInputError { msg, .. }
        | rusqlite::Error::SqliteFailure(_, Some(msg)) => msg,
        other => other.to_string(),
    }
}

fn rq(conn: &rusqlite::Connection, sql: &str) -> Rows {
    let mut st = conn.prepare(sql).map_err(stock_err)?;
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(stock_err)?
    .collect::<Result<Vec<_>, _>>()
    .map_err(stock_err)
}

/// `f` and `w` hold the same rows, `w` in the opposite rowid order. Within each
/// NOCASE group of `s` (and RTRIM group of `r`) the spellings differ, and the
/// min(n) and max(n) rows are neither always first nor always last.
const SCHEMA: &str = "\
CREATE TABLE f(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE, n INTEGER, r TEXT COLLATE RTRIM, b TEXT);\
INSERT INTO f VALUES (1, 'Name-0', 1, 'a ', 'p'), (2, 'name-0', 3, 'a', 'q'), \
  (3, 'NAME-0', 2, 'a  ', 'r'), (4, 'b', 4, 'z', 's'), (5, 'B', 5, 'z ', 't'), \
  (6, 'x', 7, 'y', 'u'), (7, 'X', 6, 'y ', 'v');\
CREATE TABLE w(id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE, n INTEGER, r TEXT COLLATE RTRIM, b TEXT);\
INSERT INTO w SELECT 8 - id, s, n, r, b FROM f;\
CREATE TABLE j(k TEXT, x INTEGER);\
INSERT INTO j VALUES ('NAME-0', 10), ('b', 20), ('X', 30);";

/// `@T@` is replaced by `f` and by `w`.
const STATEMENTS: &[&str] = &[
    "SELECT s, count(*), max(n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, count(*), min(n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, max(n), b FROM @T@ GROUP BY s ORDER BY s",
    "SELECT max(n), s FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s || '!', max(n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT '[' || r || ']', max(n) FROM @T@ GROUP BY r ORDER BY r",
    "SELECT '[' || r || ']', min(n), b FROM @T@ GROUP BY r ORDER BY r",
    "SELECT s, max(n) FROM @T@ WHERE n > 1 GROUP BY s ORDER BY s",
    "SELECT s, max(n) FROM @T@ GROUP BY s HAVING max(n) > 0 ORDER BY s",
    "SELECT s FROM @T@ GROUP BY s HAVING min(n) > 0 ORDER BY s",
    "SELECT s, max(n) FILTER (WHERE n < 3) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, max(DISTINCT n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s COLLATE NOCASE, max(n) FROM @T@ GROUP BY s COLLATE NOCASE ORDER BY 1",
    "SELECT b, max(n) FROM @T@ GROUP BY b COLLATE NOCASE ORDER BY 1",
    "SELECT s, max(n) FROM @T@ GROUP BY s ORDER BY max(n)",
    "SELECT s, max(n), max(n) + 1 FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, r, max(n) FROM @T@ GROUP BY s, r ORDER BY s, r",
    "SELECT @T@.s, max(@T@.n) FROM @T@ JOIN j ON j.k = @T@.s GROUP BY @T@.s ORDER BY 2",
    "SELECT @T@.s, max(j.x) FROM @T@ JOIN j ON j.k = @T@.s GROUP BY @T@.s ORDER BY 2",
    // Without a min()/max(), or with two, the key keeps the first row.
    "SELECT s, count(*) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, count(*) FROM @T@ GROUP BY s ORDER BY n + 0 DESC",
    "SELECT s, count(*) FROM @T@ GROUP BY s ORDER BY (SELECT @T@.n) DESC",
    "SELECT s, sum(n) FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, b FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, n FROM @T@ GROUP BY s ORDER BY s",
    "SELECT s, max(n), min(n) FROM @T@ GROUP BY s ORDER BY s",
    // DISTINCT keeps the first spelling; a BINARY key has one spelling.
    "SELECT DISTINCT s FROM @T@ ORDER BY s",
    "SELECT DISTINCT r FROM @T@ ORDER BY r",
    "SELECT b, max(n) FROM @T@ GROUP BY b ORDER BY b",
];

fn same_outcome(frank: &Rows, stock: &Rows) -> bool {
    match (frank, stock) {
        (Ok(f), Ok(s)) => f == s,
        (Err(f), Err(s)) => f.contains(s.as_str()),
        _ => false,
    }
}

async fn compare(file_backed: bool, strict: bool) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = if file_backed {
        (
            Connection::open(dir.path().join("f.db").to_str().unwrap())
                .await
                .unwrap(),
            rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
        )
    } else {
        (
            Connection::open(":memory:").await.unwrap(),
            rusqlite::Connection::open_in_memory().unwrap(),
        )
    };
    f.execute_batch(SCHEMA).await.unwrap();
    r.execute_batch(SCHEMA).unwrap();
    if strict {
        f.execute("PRAGMA fsqlite.parity_cert_strict = ON")
            .await
            .unwrap();
    }
    let label = match (file_backed, strict) {
        (true, _) => "file",
        (false, false) => "memory",
        (false, true) => "memory, strict",
    };
    let mut diffs = Vec::new();
    let mut statements = 0usize;
    for template in STATEMENTS {
        for table in ["f", "w"] {
            let sql = template.replace("@T@", table);
            statements += 1;
            let rr = rq(&r, &sql);
            let adhoc = f
                .query(&sql)
                .await
                .map(|rows| tag_rows(&rows))
                .map_err(|e| e.to_string());
            let prepared = match f.prepare(&sql).await {
                Ok(stmt) => stmt
                    .query()
                    .await
                    .map(|rows| tag_rows(&rows))
                    .map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            for (path, fr) in [("ad hoc", adhoc), ("prepared", prepared)] {
                // Strict parity-cert mode refuses the grouped evaluator that
                // collated keys run on; it must answer as stock or refuse.
                let refused = strict
                    && fr.as_ref().is_err_and(|error| {
                        error.contains("in-memory fallback disabled in strict parity-cert mode")
                    });
                if !refused && !same_outcome(&fr, &rr) {
                    diffs.push(format!(
                        "  [{label}, {path}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            }
        }
    }
    f.close().await.unwrap();
    (diffs, statements)
}

/// bd-36aed: one GROUP BY group of an untyped column can hold an INTEGER and
/// a REAL spelling of one value (3 and 3.0, -0.0 and 0, 5.0 and 5). Stock
/// prints the key as the row its bare columns come from spells it: the
/// group's first row, or the row the last min()/max() picks (ties keep the
/// earlier row; a FILTERed one's rejected row counts only as the group's
/// first). The VDBE sorted plan printed the group's last row.
///
/// Not covered, and not claimed: a DISTINCT min()/max() beside such a key.
/// Stock lets a row repeating the DISTINCT value keep the choice the row
/// before it made; the program keeps the group's first row there, as the
/// aggregate without GROUP BY does (bd-6hoc8), and differs from stock when
/// the extremum's row spells the key otherwise (`max(DISTINCT n)` over
/// `(5.0, 1), (5, 2)`: stock 5, fsqlite 5.0).
///
/// `m` and `w` hold the same rows in opposite rowid order and `mi` holds
/// `w`'s rows under an index on the key, which the plan walks instead of
/// sorting. On a file database with parity-cert strict on, each statement of
/// the first list runs the GROUP BY program and must answer as stock, ad hoc
/// and prepared. Arithmetic on the aggregate in HAVING exercises the strict
/// fallback refusal. The second list compares ordinary execution with stock.
const MIXED_SPELLING_SCHEMA: &str = "\
CREATE TABLE m(id INTEGER PRIMARY KEY, a, b, n INTEGER);\
INSERT INTO m VALUES (1, 3, 1, 10), (2, 3.0, 1.0, 5), (3, 3, 1, 7), (4, 5.0, 2, 1), \
  (5, 5, 2.0, 2), (6, -0.0, 1, 4), (7, 0, 1, 3), (8, 2.5, 1, 1), (9, NULL, 1, 1), \
  (10, NULL, 1.0, 2), (11, 'x', 1, 1), (12, 7, 1, 9), (13, 7.0, 1, 9), (14, 8.0, 1, NULL), \
  (15, 8, 1.0, 3), (16, 8.0, 1, 3), (17, 9, 1, 6), (18, 9.0, 1.0, 6), (19, 9, 1, 2), \
  (20, x'01', 1, 1);\
CREATE TABLE w(id INTEGER PRIMARY KEY, a, b, n INTEGER);\
INSERT INTO w SELECT 21 - id, a, b, n FROM m;\
CREATE TABLE mi(id INTEGER PRIMARY KEY, a, b, n INTEGER);\
INSERT INTO mi SELECT * FROM w;\
CREATE INDEX mi_a ON mi(a);";

/// `@T@` is replaced by `m` and by `w`.
const MIXED_SPELLING_STRICT: &[&str] = &[
    "SELECT a, count(*) FROM @T@ GROUP BY a HAVING count(*) > 1 ORDER BY a",
    "SELECT a, count(*) FROM @T@ GROUP BY a HAVING count(*) + 0 > 1 ORDER BY a",
    "SELECT a, count(*) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, count(*), sum(n) FROM @T@ GROUP BY a",
    "SELECT a, max(n) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, min(n) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, min(n), max(n) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, max(n), min(n) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT max(n), a, count(*) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, count(DISTINCT n), sum(DISTINCT n) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, max(n) FILTER (WHERE n > 2) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, max(n) FILTER (WHERE n > 2), min(n) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, count(*) FROM @T@ WHERE n > 1 GROUP BY a ORDER BY a",
    "SELECT a, count(*) FROM @T@ GROUP BY a ORDER BY a LIMIT 3 OFFSET 2",
    "SELECT a, b, count(*), max(n) FROM @T@ GROUP BY a, b ORDER BY a, b",
    "SELECT b, a, sum(n), avg(n) FROM @T@ GROUP BY b, a ORDER BY b, a",
    "SELECT a, group_concat(n) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, count(*) FROM mi GROUP BY a",
    "SELECT a, max(n) FROM mi GROUP BY a LIMIT 100",
    "SELECT a, min(n) FROM mi GROUP BY a ORDER BY a LIMIT 100",
];

/// `@T@` is replaced by `m` and by `w`.
///
/// Not covered, and not claimed: a max() beside the key in a shape strict
/// mode refuses (`SELECT a + 0, max(n) ... GROUP BY a`,
/// `... ORDER BY max(n), a`). Those run on the connection's grouped
/// evaluator, which prints a tied or later extremum row's spelling (8.0 where
/// stock prints 8), the same with or without the fix to the program.
const MIXED_SPELLING_STRICT_OFF: &[&str] = &[
    "SELECT a, count(*) FROM @T@ GROUP BY a HAVING count(*) + 0 > 1 ORDER BY a",
    "SELECT a, count(*) FROM @T@ GROUP BY a HAVING count(*) > 1 ORDER BY a",
    "SELECT typeof(a), a, count(*) FROM @T@ GROUP BY a ORDER BY a",
    "SELECT a, total(n), count(n) FROM @T@ GROUP BY a ORDER BY a DESC",
];

#[test]
fn mixed_integer_real_group_by_key_spelling_matches_stock_on_a_strict_file_database() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f = Connection::open(dir.path().join("f.db").to_str().unwrap())
            .await
            .unwrap();
        let r = rusqlite::Connection::open(dir.path().join("r.db")).unwrap();
        f.execute_batch(MIXED_SPELLING_SCHEMA).await.unwrap();
        r.execute_batch(MIXED_SPELLING_SCHEMA).unwrap();
        f.execute("PRAGMA fsqlite.parity_cert_strict = ON")
            .await
            .unwrap();
        let error = f
            .query("SELECT a, total(n), count(n) FROM m GROUP BY a ORDER BY a DESC")
            .await
            .expect_err("DESC ORDER BY over GROUP BY needs the grouped evaluator");
        assert!(
            error
                .to_string()
                .contains("decision_reason=group_by_fallback"),
            "{error}"
        );
        let mut diffs = Vec::new();
        let mut statements = 0usize;
        for (strict, templates) in [
            (true, MIXED_SPELLING_STRICT),
            (false, MIXED_SPELLING_STRICT_OFF),
        ] {
            let pragma = if strict {
                "PRAGMA fsqlite.parity_cert_strict = ON"
            } else {
                "PRAGMA fsqlite.parity_cert_strict = OFF"
            };
            f.execute(pragma).await.unwrap();
            let label = if strict { "strict file" } else { "file" };
            for template in templates {
                let tables: &[&str] = if template.contains("@T@") {
                    &["m", "w"]
                } else {
                    &[""]
                };
                for table in tables {
                    let sql = template.replace("@T@", table);
                    statements += 1;
                    let rr = rq(&r, &sql);
                    let adhoc = f
                        .query(&sql)
                        .await
                        .map(|rows| tag_rows(&rows))
                        .map_err(|e| e.to_string());
                    let prepared = match f.prepare(&sql).await {
                        Ok(stmt) => stmt
                            .query()
                            .await
                            .map(|rows| tag_rows(&rows))
                            .map_err(|e| e.to_string()),
                        Err(e) => Err(e.to_string()),
                    };
                    for (path, fr) in [("ad hoc", adhoc), ("prepared", prepared)] {
                        if !same_outcome(&fr, &rr) {
                            diffs.push(format!(
                                "  [{label}, {path}] `{sql}`\n     frank= {fr:?}\n     stock= {rr:?}"
                            ));
                        }
                    }
                }
            }
        }
        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {statements} statement(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

#[test]
#[cfg(feature = "ext-json")]
fn mixed_numeric_group_keys_follow_the_extremum_row() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let setup = "CREATE TABLE data(k, score); INSERT INTO data VALUES(1,0),(1.0,1);";
        stock.execute_batch(setup).unwrap();
        let path = dir.path().join("groups.db");
        for db in [":memory:", path.to_str().unwrap()] {
            let frank = Connection::open(db).await.unwrap();
            frank.execute_batch(setup).await.unwrap();
            for sql in [
                "SELECT a.value, max(a.type='real') FROM json_each('[1,1.0]') a, json_each('[0]') b GROUP BY a.value",
                "SELECT a.value, min(a.type='real') FROM json_each('[1.0,1]') a, json_each('[0]') b GROUP BY a.value",
                "SELECT a.value, count(*) FROM json_each('[1,1.0]') a, json_each('[0]') b GROUP BY a.value",
                "SELECT a.value, max(a.type='real') FROM json_each('[1,1.0]') a GROUP BY a.value",
                "SELECT typeof(k), quote(k), max(score) FROM data GROUP BY k",
                "SELECT typeof(k), max(score) FROM data GROUP BY k HAVING typeof(k)='real'",
                "SELECT k, max(score), max(-score) FROM data GROUP BY k",
                "SELECT k, max(-score), max(score) FROM data GROUP BY k",
                "SELECT typeof(k), quote(k), max(score) FROM data CROSS JOIN json_each('[0]') GROUP BY k",
                "SELECT k, count(*) FROM data CROSS JOIN json_each('[0]') GROUP BY k",
                "SELECT k, max(score) FROM data CROSS JOIN json_each('[0]') WHERE 0 GROUP BY k",
                "SELECT k, max(score) FILTER (WHERE score=0) FROM data CROSS JOIN json_each('[0]') GROUP BY k",
                "SELECT k, max(DISTINCT score) FROM data CROSS JOIN json_each('[0]') GROUP BY k",
            ] {
                let expected = rq(&stock, sql).unwrap();
                let direct = frank.query(sql).await.unwrap();
                assert_eq!(tag_rows(&direct), expected, "{db}, ad hoc: {sql}");
                let prepared = frank.prepare(sql).await.unwrap();
                assert_eq!(
                    tag_rows(&prepared.query().await.unwrap()),
                    expected,
                    "{db}, prepared: {sql}"
                );
            }
            frank.close().await.unwrap();
        }
    });
}

#[test]
fn grouped_queries_use_each_tracked_argument_once() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    struct NextValue(Arc<AtomicI64>);
    impl fsqlite_func::ScalarFunction for NextValue {
        fn name(&self) -> &str {
            "next_value"
        }
        fn num_args(&self) -> i32 {
            0
        }
        fn invoke(&self, _: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(
                self.0.fetch_add(1, Ordering::Relaxed) + 1,
            ))
        }
    }

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("volatile.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let db = Connection::open(path).await.unwrap();
            db.execute_batch(
                "CREATE TABLE t(k); INSERT INTO t VALUES(1),(1.0);
                 CREATE TABLE u(n); INSERT INTO u VALUES(2);",
            )
            .await
            .unwrap();
            let calls = Arc::new(AtomicI64::new(0));
            db.register_nondeterministic_scalar_function(NextValue(Arc::clone(&calls)));
            for source in ["t", "t CROSS JOIN u"] {
                for template in [
                    "SELECT k,max(next_value()),max(next_value()) FROM @SOURCE@ GROUP BY k",
                    "SELECT k,max(next_value())+0,max(next_value()) FROM @SOURCE@ GROUP BY k",
                    "SELECT k,max(next_value())+0,count(*) FROM @SOURCE@ GROUP BY k
                     HAVING max(next_value())=2 ORDER BY max(next_value())+1",
                ] {
                    let sql = template.replace("@SOURCE@", source);
                    for prepared in [false, true] {
                        calls.store(0, Ordering::Relaxed);
                        let rows = if prepared {
                            db.prepare(&sql).await.unwrap().query().await.unwrap()
                        } else {
                            db.query(&sql).await.unwrap()
                        };
                        assert_eq!(
                            tag_rows(&rows),
                            vec![vec!["real:1", "int:2", "int:2"]],
                            "{path}, prepared={prepared}: {sql}"
                        );
                        assert_eq!(
                            calls.load(Ordering::Relaxed),
                            2,
                            "{path}, prepared={prepared}: {sql}"
                        );
                    }
                }
            }
            if path != ":memory:" {
                db.execute("PRAGMA fsqlite.parity_cert_strict=ON").await.unwrap();
                let sql = "SELECT k, sum(next_value()+k) FROM t AS src GROUP BY k
                           HAVING sum(next_value()+src.K) IS NOT NULL";
                for prepared in [false, true] {
                    calls.store(0, Ordering::Relaxed);
                    let rows = if prepared {
                        db.prepare(sql).await.unwrap().query().await.unwrap()
                    } else {
                        db.query(sql).await.unwrap()
                    };
                    assert_eq!(rows.len(), 1);
                    assert_eq!(tag_f(&rows[0].values()[1]), "real:5");
                    assert_eq!(calls.load(Ordering::Relaxed), 2, "prepared={prepared}");
                }
            }
            db.close().await.unwrap();
        }
    });
}

#[test]
fn compiled_grouped_aggregates_share_only_identical_calls() {
    asupersync::test_utils::run_test(|| async {
        let db = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let setup = "CREATE TABLE g(k); INSERT INTO g VALUES (1),(1.0);";
        db.execute_batch(setup).await.unwrap();
        stock.execute_batch(setup).unwrap();
        let steps =
            |rows: &[Vec<String>]| rows.iter().filter(|row| row[1] == "text:AggStep").count();
        for (columns, count) in [
            ("max(random()), MAX(RANDOM())", 1),
            ("sum(random()), SUM(RANDOM())", 1),
            ("max(random()), min(random())", 2),
            ("max(DISTINCT random()), max(random())", 2),
            ("max((SELECT random())), max((SELECT random()))", 2),
            (
                "max(random()) FILTER (WHERE k>0), max(random()) FILTER (WHERE k<0)",
                2,
            ),
        ] {
            let sql = format!("EXPLAIN SELECT k,{columns} FROM g GROUP BY k");
            let stock_steps = steps(&rq(&stock, &sql).unwrap());
            assert_eq!(stock_steps, count, "{sql}");
            assert_eq!(
                steps(&tag_rows(&db.query(&sql).await.unwrap())),
                stock_steps,
                "{sql}"
            );
        }
        let sql = "EXPLAIN SELECT k,sum(random()+k) FROM g AS src GROUP BY k
                   HAVING sum(random()+src.K) IS NOT NULL";
        let stock_steps = steps(&rq(&stock, sql).unwrap());
        assert_eq!(stock_steps, 1);
        assert_eq!(steps(&tag_rows(&db.query(sql).await.unwrap())), stock_steps);

        let sql = "SELECT max(?), max(?) FROM g GROUP BY k";
        let params = [SqliteValue::Integer(3), SqliteValue::Integer(7)];
        let expected = stock
            .query_row(sql, [3, 7], |row| {
                Ok(vec![tag_r(&row.get(0)?), tag_r(&row.get(1)?)])
            })
            .unwrap();
        let direct = db.query_with_params(sql, &params).await.unwrap();
        let bound = db
            .prepare(sql)
            .await
            .unwrap()
            .query_with_params(&params)
            .await
            .unwrap();
        assert_eq!(tag_rows(&direct), vec![expected.clone()]);
        assert_eq!(tag_rows(&bound), vec![expected]);
        db.close().await.unwrap();
    });
}

#[test]
fn collated_group_by_key_spelling_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        let mut statements = 0usize;
        for (file_backed, strict) in [(false, false), (false, true), (true, false)] {
            let (more, count) = compare(file_backed, strict).await;
            diffs.extend(more);
            statements += count;
        }
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {statements} statement(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
