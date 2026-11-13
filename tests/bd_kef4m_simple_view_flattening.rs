#![recursion_limit = "512"]

//! bd-kef4m: every SELECT that read a view copied the view's whole result
//! into a temp table first, so `SELECT * FROM v WHERE id = ?` cost a scan of
//! the view's table, and a correlated subquery over a view paid that copy
//! once per outer row (20,000 rows: two minutes). A simple view (one table,
//! columns or plain expressions, an optional WHERE) read alone is now
//! flattened into the query, as stock flattens it, through views of views.
//! A flattened per-row EXISTS probe reads `WHERE id = <v> AND ... LIMIT 1`,
//! and the VDBE scanned the whole table for a rowid equality with another
//! term and a LIMIT; that shape now seeks the row. An UPDATE reading a view
//! in SET reads each row's values with `... FROM <target> WHERE rowid = ?`,
//! which the generic join fallback answered by loading the whole target; a
//! WHERE pinning the rowid now reads that row alone.
//!
//! Every query is compared, rows in order, with rusqlite's bundled SQLite, in
//! memory and in a file, ad hoc and prepared: views with a column list,
//! expression columns, aliases, a NOCASE column, TEXT affinity, rowid, views
//! of views, chains at and past the 16 views one statement flattens, TEMP
//! views and a TEMP table shadowing the view's table, the
//! views that must stay materialized, correlated subqueries over views, names
//! the view does not expose (an error, both sides), rowid equalities with
//! a residual and LIMIT/OFFSET, and pinned-rowid reads (the alias, `rowid`,
//! `oid`, `_rowid_`, text and real keys, a declared `rowid` column). UPDATE
//! and DELETE reading views, a join or the target, with a trigger, compare
//! every table after. Two cost tests pin which shapes flatten and that the
//! rowid shape seeks.

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

/// Rows, or `None` for an error: only whether a query fails is compared,
/// not the wording of its message.
type Rows = Option<Vec<Vec<String>>>;

fn tag_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}

fn rq(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> Rows {
    let mut st = conn.prepare(sql).ok()?;
    let n = st.column_count();
    st.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .ok()?
    .collect::<Result<Vec<_>, _>>()
    .ok()
}

async fn fq(conn: &Connection, sql: &str, params: &[i64], prepared: bool) -> Rows {
    let values: Vec<SqliteValue> = params.iter().copied().map(SqliteValue::Integer).collect();
    let rows = if prepared {
        let stmt = conn.prepare(sql).await.ok()?;
        if values.is_empty() {
            stmt.query().await
        } else {
            stmt.query_with_params(&values).await
        }
    } else if values.is_empty() {
        conn.query(sql).await
    } else {
        conn.query_with_params(sql, &values).await
    };
    rows.ok().map(|rows| tag_rows(&rows))
}

const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT COLLATE NOCASE, g INTEGER, n INTEGER, s TEXT);\
CREATE INDEX t_g ON t(g);\
INSERT INTO t WITH RECURSIVE q(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM q WHERE i < 60) \
  SELECT i, CASE WHEN i % 4 = 0 THEN 'V' ELSE 'v' END || (i % 7), \
    CASE WHEN i % 11 = 0 THEN NULL ELSE i % 9 END, i % 5, \
    CASE WHEN i % 3 = 0 THEN '7' ELSE i END FROM q;\
CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER, n INTEGER);\
INSERT INTO u SELECT id, g, n FROM t WHERE id <= 30;\
CREATE VIEW v_cols AS SELECT id, g, v FROM t WHERE n > 1;\
CREATE VIEW v_star AS SELECT * FROM t WHERE g IS NOT NULL;\
CREATE VIEW v_named(k, grp) AS SELECT id, g FROM t;\
CREATE VIEW v_expr AS SELECT id, g + 1 AS g1, v || '!' AS bang, CAST(n AS TEXT) AS nt FROM t;\
CREATE VIEW v_alias AS SELECT x.id AS id, x.v AS label, x.s AS s FROM t AS x WHERE x.g < 5;\
CREATE VIEW v_of_v AS SELECT id, g FROM v_cols WHERE g <> 3;\
CREATE VIEW v_of_alias AS SELECT a.id, a.label FROM v_alias AS a WHERE a.id % 2 = 0;\
CREATE VIEW v_rowid AS SELECT rowid AS r, n FROM t;\
CREATE VIEW v_case AS SELECT id, CASE WHEN g > 4 THEN 'hi' ELSE 'lo' END AS band FROM t;\
CREATE VIEW v_text AS SELECT id, s FROM t;\
CREATE VIEW v_agg AS SELECT g, count(*) AS c FROM t GROUP BY g;\
CREATE VIEW v_distinct AS SELECT DISTINCT g FROM t;\
CREATE VIEW v_limit AS SELECT id, g FROM t ORDER BY id DESC LIMIT 5;\
CREATE VIEW v_join AS SELECT t.id, u.n FROM t JOIN u ON u.id = t.id;\
CREATE VIEW v_union AS SELECT id FROM t WHERE id < 3 UNION ALL SELECT id FROM u WHERE id > 28;\
CREATE VIEW v_dup AS SELECT id, id FROM t;\
CREATE TEMP VIEW tv AS SELECT id, n FROM t WHERE g = 1;\
CREATE TABLE w(rowid TEXT, x INTEGER);\
INSERT INTO w VALUES ('3', 1), ('1', 2), ('2', 3);";

const QUERIES: &[(&str, &[i64])] = &[
    // One table, plain columns, a WHERE.
    ("SELECT * FROM v_cols WHERE id = 7", &[]),
    ("SELECT * FROM v_cols WHERE id = 5", &[]),
    ("SELECT g, v FROM v_cols WHERE g = 3 ORDER BY id", &[]),
    ("SELECT count(*) FROM v_cols WHERE v = 'v3'", &[]),
    (
        "SELECT id FROM v_cols WHERE v_cols.g > 6 ORDER BY id DESC LIMIT 3",
        &[],
    ),
    (
        "SELECT id FROM v_cols AS c WHERE c.g BETWEEN 2 AND 4 ORDER BY id LIMIT 4 OFFSET 2",
        &[],
    ),
    ("SELECT DISTINCT g FROM v_cols ORDER BY g", &[]),
    (
        "SELECT max(id), min(g), total(g) FROM v_cols WHERE v LIKE 'v%'",
        &[],
    ),
    ("SELECT *, id * 2 FROM v_cols WHERE id = 9", &[]),
    ("SELECT v_cols.* FROM v_cols WHERE id = 9", &[]),
    ("SELECT * FROM V_COLS WHERE ID = 9", &[]),
    ("SELECT g AS id FROM v_cols ORDER BY id LIMIT 4", &[]),
    ("SELECT * FROM v_cols WHERE id = ?1", &[7]),
    (
        "SELECT id FROM v_cols WHERE g = ? AND id > ? ORDER BY id",
        &[3, 10],
    ),
    // `*`, a column list, expressions, aliases.
    (
        "SELECT * FROM v_star WHERE id IN (3, 9, 22) ORDER BY id",
        &[],
    ),
    ("SELECT id, s FROM v_star WHERE s = 7 ORDER BY id", &[]),
    ("SELECT k, grp FROM v_named WHERE k < 5 ORDER BY k", &[]),
    ("SELECT * FROM v_named WHERE grp IS NULL ORDER BY k", &[]),
    (
        "SELECT v_named.k FROM v_named WHERE v_named.grp = 2 ORDER BY 1",
        &[],
    ),
    ("SELECT * FROM v_named ORDER BY grp DESC, k LIMIT 3", &[]),
    ("SELECT * FROM v_expr WHERE id = 12", &[]),
    ("SELECT id, g1 FROM v_expr WHERE g1 = 5 ORDER BY id", &[]),
    ("SELECT bang FROM v_expr WHERE nt = '3' ORDER BY id", &[]),
    ("SELECT count(*) FROM v_expr WHERE nt = 3", &[]),
    ("SELECT * FROM v_alias WHERE id > 50 ORDER BY id", &[]),
    ("SELECT id FROM v_alias WHERE label = 'v1' ORDER BY id", &[]),
    (
        "SELECT id, band FROM v_case WHERE band = 'hi' ORDER BY id LIMIT 5",
        &[],
    ),
    ("SELECT id FROM v_text WHERE s = 7 ORDER BY id", &[]),
    ("SELECT id FROM v_text WHERE s > 50 ORDER BY id", &[]),
    ("SELECT r, n FROM v_rowid WHERE r = 33", &[]),
    // Views of views; a TEMP view.
    ("SELECT * FROM v_of_v WHERE id < 20 ORDER BY id", &[]),
    ("SELECT count(*), sum(g) FROM v_of_v", &[]),
    ("SELECT * FROM v_of_alias WHERE id > 40 ORDER BY id", &[]),
    ("SELECT * FROM tv WHERE id > 10 ORDER BY id", &[]),
    // Views that stay materialized, and outer shapes that keep them so.
    ("SELECT * FROM v_agg WHERE g = 2", &[]),
    ("SELECT * FROM v_distinct ORDER BY g", &[]),
    ("SELECT * FROM v_limit WHERE g > 2 ORDER BY id", &[]),
    ("SELECT * FROM v_join WHERE id = 6", &[]),
    ("SELECT * FROM v_union ORDER BY id", &[]),
    ("SELECT * FROM v_dup WHERE id = 4", &[]),
    (
        "SELECT band, count(*) FROM v_case GROUP BY band ORDER BY band",
        &[],
    ),
    (
        "SELECT v_cols.id, u.n FROM v_cols JOIN u ON u.id = v_cols.id WHERE v_cols.g = 2 ORDER BY 1",
        &[],
    ),
    (
        "SELECT id FROM v_cols WHERE id IN (SELECT id FROM u WHERE n = 3) ORDER BY id",
        &[],
    ),
    // Correlated subqueries over views.
    (
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM v_cols WHERE v_cols.id = u.id \
         AND v_cols.g = 3) ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id, (SELECT count(*) FROM v_cols WHERE v_cols.g = u.g) FROM u ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id, (SELECT k FROM v_named WHERE grp = u.g ORDER BY k DESC LIMIT 1) FROM u \
         ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id FROM u WHERE u.id IN (SELECT id FROM v_of_v) ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id FROM u WHERE NOT EXISTS (SELECT 1 FROM v_expr WHERE v_expr.id = u.id \
         AND v_expr.g1 > 5) ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id, EXISTS (SELECT 1 FROM v_alias WHERE v_alias.id = u.id + ?1) FROM u \
         ORDER BY u.id",
        &[2],
    ),
    // Correlated subqueries over a join or a derived table, whose bound
    // equalities now seek.
    (
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM t AS x JOIN t AS y ON y.id = x.id \
         WHERE x.id = u.id AND y.g = 3) ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id, (SELECT count(*) FROM t AS x JOIN u AS y ON y.g = x.g WHERE x.id = u.id) \
         FROM u ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM t JOIN t AS y ON y.id = t.id + 1 \
         WHERE t.id = u.id AND y.n = u.n) ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM t AS x JOIN t AS y ON y.id = x.id \
         WHERE x.s = u.id) ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM (SELECT id, g FROM t WHERE n > 1) AS d \
         WHERE d.id = u.id AND d.g = 3) ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id, (SELECT d.g FROM (SELECT id, g FROM t) AS d WHERE d.id = u.id + 1) \
         FROM u ORDER BY u.id",
        &[],
    ),
    (
        "SELECT u.id, (SELECT x.v FROM t AS x WHERE x.g = u.g ORDER BY x.id DESC LIMIT 1 \
         OFFSET 1) FROM u ORDER BY u.id",
        &[],
    ),
    // A rowid equality with a residual and a LIMIT.
    ("SELECT g FROM t WHERE id = 7 AND g = 7 LIMIT 1", &[]),
    ("SELECT g FROM t WHERE id = 7 AND g = 3 LIMIT 1", &[]),
    (
        "SELECT * FROM t WHERE id = 8 AND n > 1 LIMIT 5 OFFSET 0",
        &[],
    ),
    (
        "SELECT * FROM t WHERE id = 8 AND n > 1 LIMIT 1 OFFSET 1",
        &[],
    ),
    ("SELECT id FROM t WHERE id = 8 AND n > 1 LIMIT 0", &[]),
    ("SELECT id FROM t WHERE id = 8 AND n > 1 LIMIT -1", &[]),
    ("SELECT v FROM t WHERE g = 7 AND id = 16 LIMIT 1", &[]),
    ("SELECT v FROM t WHERE id = '16' AND g = 7 LIMIT 1", &[]),
    ("SELECT v FROM t WHERE id = 16.5 AND g = 7 LIMIT 1", &[]),
    ("SELECT v FROM t WHERE id = NULL AND g = 7 LIMIT 1", &[]),
    ("SELECT v FROM t WHERE id = 99 AND g = 7 LIMIT 1", &[]),
    ("SELECT v FROM t WHERE id = ?1 AND g = ?2 LIMIT 1", &[16, 7]),
    (
        "SELECT v FROM t WHERE id = ?1 AND v = 'v2' LIMIT ?2",
        &[16, 1],
    ),
    (
        "SELECT v FROM t WHERE id = ? AND g = ? LIMIT ?",
        &[16, 7, 1],
    ),
    (
        "SELECT v FROM t WHERE id = ? AND g = ? LIMIT 1 OFFSET ?",
        &[16, 7, 0],
    ),
    // A correlated subquery in the result over a table whose rowid the WHERE
    // pins: the fallback reads that row alone.
    (
        "SELECT u.n, (SELECT count(*) FROM v_cols WHERE v_cols.g = u.g) FROM u WHERE u.id = 7",
        &[],
    ),
    (
        "SELECT (SELECT count(*) FROM v_cols WHERE v_cols.g = u.g) FROM u WHERE 7 = u.rowid",
        &[],
    ),
    (
        "SELECT (SELECT max(id) FROM v_cols WHERE v_cols.g = u.g) FROM u \
         WHERE u.oid = ?1 AND u.n > 1",
        &[8],
    ),
    (
        "SELECT (SELECT max(id) FROM v_cols WHERE v_cols.g = u.g) FROM u \
         WHERE u._rowid_ = ?1 AND u.n > 1",
        &[10],
    ),
    (
        "SELECT (SELECT 1 FROM v_cols WHERE v_cols.g = u.g LIMIT 1) FROM u WHERE id = 999",
        &[],
    ),
    (
        "SELECT (SELECT count(*) FROM v_cols WHERE v_cols.g = u.g) FROM u WHERE u.id = '7'",
        &[],
    ),
    (
        "SELECT (SELECT count(*) FROM v_cols WHERE v_cols.g = u.g) FROM u WHERE u.id = 7.0",
        &[],
    ),
    (
        "SELECT u.id, (SELECT count(*) FROM v_cols WHERE v_cols.g = u.g) FROM u \
         WHERE u.id = 7 OR u.id = 9 ORDER BY 1",
        &[],
    ),
    (
        "SELECT w.x, (SELECT count(*) FROM v_cols WHERE v_cols.g = w.x) FROM w \
         WHERE w.rowid = 2",
        &[],
    ),
    (
        "SELECT w.rowid, (SELECT count(*) FROM v_cols WHERE v_cols.g = w.x) FROM w \
         WHERE w._rowid_ = 2",
        &[],
    ),
    // A chain of views at and past the most one statement flattens.
    ("SELECT * FROM c15 WHERE id = 5", &[]),
    ("SELECT * FROM c16 WHERE id = 5", &[]),
    (
        "SELECT u.id, (SELECT c16.n FROM c16 WHERE c16.id = u.id) FROM u WHERE u.id < 4",
        &[],
    ),
    // Names the view does not expose.
    ("SELECT n FROM v_cols", &[]),
    ("SELECT v_cols.n FROM v_cols", &[]),
    ("SELECT t.id FROM v_cols", &[]),
    ("SELECT id FROM v_cols WHERE n = 2", &[]),
    ("SELECT x FROM v_expr WHERE id = 1", &[]),
];

/// Read with a TEMP table named `t` shadowing the views' table: a MAIN view
/// reads main.t, as stock does.
const SHADOWED: &[&str] = &[
    "SELECT * FROM v_cols WHERE id = 7",
    "SELECT * FROM v_of_v WHERE id < 12 ORDER BY id",
    "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM v_cols WHERE v_cols.id = u.id) ORDER BY u.id",
];
const SHADOW: &str = "CREATE TEMP TABLE t(id INTEGER PRIMARY KEY, v, g, n, s);\
INSERT INTO temp.t VALUES (7, 'temp', 99, 9, 'x'), (8, 'temp', 98, 9, 'x');";

async fn open_pair(
    file_backed: bool,
    dir: &tempfile::TempDir,
) -> (Connection, rusqlite::Connection) {
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
    let chain = view_chain();
    for schema in [SCHEMA, chain.as_str()] {
        f.execute_batch(schema).await.unwrap();
        r.execute_batch(schema).unwrap();
    }
    (f, r)
}

/// `c0` .. `c16`, each `c<i>` a simple view of `c<i - 1>` adding 1 to `n`:
/// reading `c15` flattens 16 views, the most one statement flattens;
/// reading `c16` (17 views) materializes, so a deep chain never becomes one
/// expression as deep as the chain (bd-5haia's 2 MiB stack bound).
fn view_chain() -> String {
    let mut chain = "CREATE VIEW c0 AS SELECT id, n FROM t;".to_owned();
    for level in 1..=16 {
        chain.push_str(&format!(
            "CREATE VIEW c{level} AS SELECT id, n + 1 AS n FROM c{};",
            level - 1
        ));
    }
    chain
}

async fn compare(file_backed: bool) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = open_pair(file_backed, &dir).await;
    let label = if file_backed { "file" } else { "memory" };
    let mut diffs = Vec::new();
    let mut runs = 0usize;
    let shadowed = SHADOWED.iter().map(|sql| (*sql, &[][..], true));
    for (sql, params, shadow) in QUERIES
        .iter()
        .map(|(sql, params)| (*sql, *params, false))
        .chain(shadowed)
    {
        if shadow {
            f.execute_batch(SHADOW).await.unwrap();
            r.execute_batch(SHADOW).unwrap();
        }
        let rr = rq(&r, sql, params);
        for prepared in [false, true] {
            runs += 1;
            let fr = fq(&f, sql, params, prepared).await;
            if fr != rr {
                diffs.push(format!(
                    "  [{label}, prepared={prepared}, shadowed={shadow}] `{sql}` {params:?}\n     \
                     frank= {fr:?}\n     stock= {rr:?}"
                ));
            }
        }
        if shadow {
            f.execute("DROP TABLE temp.t").await.unwrap();
            r.execute("DROP TABLE temp.t", []).unwrap();
        }
    }
    f.close().await.unwrap();
    (diffs, runs)
}

#[test]
fn simple_views_flatten_and_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut runs) = compare(false).await;
        let (file_diffs, file_runs) = compare(true).await;
        diffs.extend(file_diffs);
        runs += file_runs;
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {runs} query run(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// UPDATE and DELETE whose subqueries read views, a join or the target
/// itself; with an UPDATE trigger; on a table whose column named `rowid`
/// shadows the rowid. Each per-row SET read pins the row's rowid.
const DML: &[&str] = &[
    "UPDATE u SET n = (SELECT count(*) FROM v_cols WHERE v_cols.id = u.id)",
    "UPDATE u SET n = (SELECT grp FROM v_named WHERE k = u.id + 1), \
     g = (SELECT count(*) FROM v_of_v WHERE v_of_v.g = u.g)",
    "UPDATE u SET n = (SELECT count(*) FROM v_cols WHERE v_cols.g = u.g) WHERE u.id > 10",
    "UPDATE u SET n = n + (SELECT count(*) FROM v_cols WHERE v_cols.id <= u.n)",
    "UPDATE u SET n = (SELECT count(*) FROM u AS y WHERE y.n < u.n)",
    "UPDATE u SET g = (SELECT max(x.id) FROM t AS x JOIN u AS y ON y.id = x.id WHERE x.g = u.g)",
    "DELETE FROM u WHERE EXISTS (SELECT 1 FROM v_cols WHERE v_cols.id = u.id AND v_cols.g = 3)",
    "DELETE FROM u WHERE (SELECT count(*) FROM v_expr WHERE v_expr.g1 = u.g + 1) > 6",
    "UPDATE w SET x = (SELECT count(*) FROM v_cols WHERE v_cols.g = w.x)",
    "CREATE TABLE ulog(id, n);\
     CREATE TRIGGER u_au AFTER UPDATE ON u BEGIN INSERT INTO ulog VALUES (new.id, new.n); END;\
     UPDATE u SET n = (SELECT count(*) FROM v_cols WHERE v_cols.g = u.g) WHERE u.id % 4 = 1",
];

/// Each DML statement on a fresh pair: whether it fails, then every table it
/// can change, rows in order.
async fn compare_dml(file_backed: bool) -> (Vec<String>, usize) {
    let label = if file_backed { "file" } else { "memory" };
    let mut diffs = Vec::new();
    for sql in DML {
        let dir = tempfile::tempdir().unwrap();
        let (f, r) = open_pair(file_backed, &dir).await;
        let f_ok = f.execute_batch(sql).await.is_ok();
        let r_ok = r.execute_batch(sql).is_ok();
        if f_ok != r_ok {
            diffs.push(format!(
                "  [{label}] `{sql}`: frank ok={f_ok}, stock ok={r_ok}"
            ));
        }
        for read in [
            "SELECT * FROM u ORDER BY id",
            "SELECT _rowid_, * FROM w ORDER BY _rowid_",
            "SELECT * FROM ulog ORDER BY rowid",
        ] {
            let (fr, rr) = (fq(&f, read, &[], false).await, rq(&r, read, &[]));
            if fr != rr {
                diffs.push(format!(
                    "  [{label}] `{sql}` then `{read}`\n     frank= {fr:?}\n     stock= {rr:?}"
                ));
            }
        }
        f.close().await.unwrap();
    }
    (diffs, DML.len())
}

#[test]
fn dml_reading_views_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut runs) = compare_dml(false).await;
        let (file_diffs, file_runs) = compare_dml(true).await;
        diffs.extend(file_diffs);
        runs += file_runs;
        assert!(
            diffs.is_empty(),
            "{} divergence(s) over {runs} statement run(s) from rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

#[test]
fn only_views_that_do_not_flatten_are_materialized() {
    const MATERIALIZED: &str = "view_materialization";
    let flattened = [
        "SELECT * FROM v_cols WHERE id = 7",
        "SELECT * FROM v_named WHERE k = 3",
        "SELECT * FROM v_expr WHERE id = 12",
        "SELECT * FROM v_of_v WHERE id < 20",
        "SELECT * FROM v_of_alias WHERE id > 40",
        "SELECT * FROM tv WHERE id > 10",
        "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM v_cols WHERE v_cols.id = u.id \
         AND v_cols.g = 3)",
        "SELECT u.id, (SELECT count(*) FROM v_cols WHERE v_cols.g = u.g) FROM u",
        "SELECT * FROM c15 WHERE id = 5",
    ];
    let materialized = [
        "SELECT * FROM c16 WHERE id = 5",
        "SELECT * FROM v_agg WHERE g = 2",
        "SELECT * FROM v_distinct",
        "SELECT * FROM v_limit WHERE g > 2",
        "SELECT * FROM v_join WHERE id = 6",
        "SELECT * FROM v_dup WHERE id = 4",
        "SELECT band, count(*) FROM v_case GROUP BY band",
        "SELECT v_cols.id FROM v_cols JOIN u ON u.id = v_cols.id",
    ];
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let (f, _r) = open_pair(false, &dir).await;
        f.set_fallback_capture_enabled(true);
        let mut wrong = Vec::new();
        for (sql, expect_materialized) in flattened
            .iter()
            .map(|sql| (sql, false))
            .chain(materialized.iter().map(|sql| (sql, true)))
        {
            f.reset_fallback_decision_evidence();
            f.query(sql).await.unwrap();
            let went_materialized = f
                .fallback_decision_snapshot()
                .decisions
                .iter()
                .any(|decision| decision.decision_reason == MATERIALIZED);
            if went_materialized != expect_materialized {
                wrong.push(format!(
                    "  `{sql}`: materialized = {went_materialized}, expected {expect_materialized}"
                ));
            }
        }
        f.close().await.unwrap();
        assert!(wrong.is_empty(), "routing:\n{}", wrong.join("\n"));
    });
}

#[test]
fn rowid_equality_with_a_residual_and_limit_seeks() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute_batch(SCHEMA).await.unwrap();
        let mut wrong = Vec::new();
        for sql in [
            "SELECT g FROM t WHERE id = 7 AND g = 3 LIMIT 1",
            "SELECT 1 FROM t WHERE n > 1 AND id = 7 AND g = 3 LIMIT 1",
            "SELECT * FROM t WHERE id = ?1 AND n > 1 LIMIT 2 OFFSET 1",
        ] {
            let opcodes: Vec<String> = conn
                .query(&format!("EXPLAIN {sql}"))
                .await
                .unwrap()
                .iter()
                .map(|row| match &row.values()[1] {
                    SqliteValue::Text(opcode) => opcode.to_string(),
                    other => format!("{other:?}"),
                })
                .collect();
            if !opcodes.iter().any(|opcode| opcode == "SeekRowid")
                || opcodes.iter().any(|opcode| opcode == "Rewind")
            {
                wrong.push(format!("  `{sql}`: {opcodes:?}"));
            }
        }
        conn.close().await.unwrap();
        assert!(
            wrong.is_empty(),
            "expected a rowid seek, no scan:\n{}",
            wrong.join("\n")
        );
    });
}
