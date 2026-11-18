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
/// the first list runs the GROUP BY program (strict refuses the grouped
/// evaluator; a HAVING is the control that it does) and must answer as stock,
/// ad hoc and prepared. The second list holds shapes strict mode refuses there
/// (a HAVING, typeof() of the key, a DESC order), compared with strict off.
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
/// mode refuses (`... HAVING max(n) > 2`, `SELECT a + 0, max(n) ... GROUP BY
/// a`, `... ORDER BY max(n), a`). Those run on the connection's grouped
/// evaluator, which prints a tied or later extremum row's spelling (8.0 where
/// stock prints 8), the same with or without the fix to the program.
const MIXED_SPELLING_STRICT_OFF: &[&str] = &[
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
        assert!(
            f.query("SELECT a, count(*) FROM m GROUP BY a HAVING count(*) > 1")
                .await
                .is_err(),
            "strict mode must refuse the grouped evaluator, which a HAVING takes; otherwise \
             the strict list need not run the GROUP BY program"
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
