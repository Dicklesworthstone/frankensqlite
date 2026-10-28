#![recursion_limit = "512"]

//! bd-h7hox: UPDATE and DELETE compiled every correlated subquery in SET and
//! WHERE with `emit_scalar_subquery` / `emit_exists_subquery`, which scan one
//! base table. Any other shape (a join, a view, a derived table, a CTE, a
//! compound, a table-valued function, GROUP BY, HAVING, LIMIT 0, an OFFSET, a
//! no-FROM subquery with a WHERE, a correlated IN in SET) changed the wrong
//! rows or wrote wrong values, silently. Such a WHERE is now resolved to row
//! locators through the SELECT path, and such a SET is evaluated per row
//! through it, each row read just before it changes. With UPDATE triggers or
//! foreign keys on, any correlated SET subquery failed ("eval_join_expr:
//! unsupported expression type"); those take the per-row evaluation too.
//!
//! Every statement runs alone on a fresh target and is compared with
//! rusqlite's bundled SQLite: the change count, the target's rows, the rows
//! its triggers logged and its child table. Targets: a rowid table, a WITHOUT
//! ROWID table, a table with a hidden rowid, a rowid table with UPDATE /
//! DELETE triggers, and a parent with a cascading child under foreign keys.
//! In memory and in a file, ad hoc and prepared.

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

fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap()
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}

const SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
CREATE INDEX t_g ON t(g);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 60) \
  SELECT i, 'v' || (i % 7), i % 9, i % 5 FROM s;\
INSERT INTO k WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 6) \
  SELECT i, 'k' || i FROM s;\
CREATE VIEW vt AS SELECT id, g FROM t WHERE n > 1;\
CREATE TABLE log(id, old_n, new_n);";

/// Each target is rebuilt, holding a copy of `t`, before every statement:
/// its table, then what follows the copy (a child table `uc`, and for the
/// foreign-key target the pragma).
const TARGETS: &[(&str, &str, &str)] = &[
    (
        "rowid",
        "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);",
        PLAIN_CHILD,
    ),
    (
        "without rowid",
        "CREATE TABLE u(id INTEGER NOT NULL PRIMARY KEY, v TEXT, g INTEGER, n INTEGER) \
         WITHOUT ROWID;",
        PLAIN_CHILD,
    ),
    (
        "hidden rowid",
        "CREATE TABLE u(id INTEGER, v TEXT, g INTEGER, n INTEGER);",
        PLAIN_CHILD,
    ),
    (
        "triggers",
        "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
         CREATE TRIGGER u_au AFTER UPDATE ON u BEGIN \
           INSERT INTO log VALUES (old.id, old.n, new.n); END;\
         CREATE TRIGGER u_bd BEFORE DELETE ON u BEGIN \
           INSERT INTO log VALUES (old.id, old.n, NULL); END;",
        PLAIN_CHILD,
    ),
    (
        "foreign keys",
        "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);",
        "CREATE TABLE uc(uid INTEGER REFERENCES u(id) ON DELETE CASCADE ON UPDATE CASCADE, \
           note TEXT);\
         INSERT INTO uc SELECT id, 'c' || id FROM t WHERE id % 4 = 0 ORDER BY id;\
         PRAGMA foreign_keys = ON;",
    ),
];

const PLAIN_CHILD: &str = "CREATE TABLE uc(uid INTEGER, note TEXT);\
     INSERT INTO uc SELECT id, 'c' || id FROM t WHERE id % 4 = 0 ORDER BY id;";

const STATEMENTS: &[(&str, &[i64])] = &[
    // SET subqueries the compiled scan cannot answer.
    (
        "UPDATE u SET n = EXISTS (SELECT 1 FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label <> 'k3' AND x.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = EXISTS (SELECT 1 FROM t AS x WHERE x.id <= u.id LIMIT 1 OFFSET 10)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label <> 'k3' AND x.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.n = u.n GROUP BY x.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g HAVING count(*) > 6)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT x.id FROM t AS x WHERE x.g = u.g LIMIT 0)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g LIMIT 0)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT x.id FROM t AS x WHERE x.g = u.g LIMIT 1 OFFSET 2)",
        &[],
    ),
    ("UPDATE u SET n = (SELECT 5 WHERE u.n > 2)", &[]),
    ("UPDATE u SET n = EXISTS (SELECT 1 WHERE u.n > 2)", &[]),
    (
        "UPDATE u SET n = (SELECT count(*) FROM (SELECT * FROM t WHERE n > 1) AS x \
         WHERE x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (WITH c AS (SELECT * FROM t WHERE n > 1) \
         SELECT count(*) FROM c WHERE c.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT x.id FROM t AS x WHERE x.id = u.id + 100 \
         UNION ALL SELECT -u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM json_each('[1,2,3,4]') AS j \
         WHERE j.value < u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g \
         AND EXISTS (SELECT 1 FROM vt WHERE vt.id = x.id))",
        &[],
    ),
    (
        "UPDATE u SET n = u.id IN (SELECT x.id FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label = 'k4' AND x.n = u.n)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.g = u.g), \
         v = (SELECT k.label FROM k JOIN t AS x ON x.g = k.g WHERE x.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.g = u.g) \
         WHERE EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id)",
        &[],
    ),
    // The subquery reads the target: each row sees the rows changed before it.
    (
        "UPDATE u SET n = (SELECT count(*) FROM u AS x JOIN k ON x.g = k.g \
         WHERE x.n >= 2 AND x.id < u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM u AS x JOIN k ON x.g = k.g WHERE x.n = u.n)",
        &[],
    ),
    // UPDATE ... FROM evaluates its SET once, before any row changes.
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.g = k.g) + k.g \
         FROM k WHERE k.g = u.g AND k.label <> 'k2'",
        &[],
    ),
    // SET subqueries the compiled scan answers.
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = EXISTS (SELECT 1 FROM t AS x WHERE x.g = u.g AND x.id > u.id + 40)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT x.n FROM t AS x WHERE x.id = u.id + 3)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT x.id FROM t AS x WHERE x.g = u.g \
         ORDER BY x.id DESC LIMIT 1 OFFSET 1)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(DISTINCT x.g) FROM t AS x WHERE x.n = u.n)",
        &[],
    ),
    // WHERE subqueries the compiled scan cannot answer.
    (
        "UPDATE u SET n = -1 WHERE EXISTS (SELECT 1 FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label = 'k2' AND x.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = -1 WHERE EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id AND vt.g = 3)",
        &[],
    ),
    (
        "UPDATE u SET n = -1 WHERE EXISTS (SELECT 1 FROM t AS x WHERE x.id <= u.id \
         LIMIT 1 OFFSET 50)",
        &[],
    ),
    (
        "UPDATE u SET n = -1 WHERE (SELECT count(*) FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label = 'k2' AND x.id = u.id) > 0",
        &[],
    ),
    (
        "DELETE FROM u WHERE EXISTS (SELECT 1 FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label = 'k2' AND x.id = u.id)",
        &[],
    ),
    (
        "DELETE FROM u WHERE EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id AND vt.g = 3)",
        &[],
    ),
    (
        "DELETE FROM u WHERE EXISTS (SELECT 1 FROM t AS x WHERE x.id <= u.id LIMIT 1 OFFSET 50)",
        &[],
    ),
    (
        "DELETE FROM u WHERE NOT EXISTS (SELECT 1 FROM t AS x JOIN k ON x.g = k.g \
         WHERE x.id = u.id)",
        &[],
    ),
    (
        "DELETE FROM u WHERE (SELECT count(*) FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label = 'k2' AND x.id = u.id) > 0",
        &[],
    ),
    (
        "DELETE FROM u WHERE (SELECT count(*) FROM vt WHERE vt.id = u.id) = 0",
        &[],
    ),
    (
        "DELETE FROM u WHERE (SELECT count(*) FROM t AS x WHERE x.g = u.g \
         HAVING count(*) > 6) IS NOT NULL",
        &[],
    ),
    (
        "DELETE FROM u WHERE EXISTS (SELECT 1 FROM t AS x WHERE x.id = u.id \
         AND EXISTS (SELECT 1 FROM vt WHERE vt.id = x.id))",
        &[],
    ),
    (
        "DELETE FROM u WHERE EXISTS (SELECT 1 FROM (SELECT id FROM t WHERE n > 2) AS x \
         WHERE x.id = u.id)",
        &[],
    ),
    (
        "DELETE FROM u WHERE u.id < 5 OR EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id \
         AND vt.g = 3)",
        &[],
    ),
    ("DELETE FROM u WHERE EXISTS (SELECT 1 WHERE u.n > 2)", &[]),
    // DELETE decides every row before removing any.
    (
        "DELETE FROM u WHERE EXISTS (SELECT 1 FROM u AS x JOIN k ON x.g = k.g \
         WHERE x.id = u.id - 1)",
        &[],
    ),
    // WHERE subqueries that already compiled or were already routed.
    (
        "DELETE FROM u WHERE EXISTS (SELECT 1 FROM t AS x WHERE x.g = u.g \
         AND x.id > u.id + 40)",
        &[],
    ),
    (
        "UPDATE u SET n = -1 WHERE u.id IN (SELECT x.id FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label = 'k4' AND x.n = u.n)",
        &[],
    ),
    // One row, and a parent key a child row cascades from.
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g) WHERE id = 8",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.g = u.g) WHERE id = 8",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt) WHERE id < 9",
        &[],
    ),
    (
        "UPDATE u SET id = id + 1000 WHERE EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id \
         AND vt.g = 3)",
        &[],
    ),
    (
        "UPDATE u SET id = id + (SELECT count(*) FROM t AS x JOIN k ON x.g = k.g \
         WHERE x.id = u.id) * 1000",
        &[],
    ),
    // Bound parameters, numbered and anonymous.
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.g = u.g AND vt.id > ?1) \
         WHERE u.id > ?2",
        &[10, 20],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.g = u.g AND vt.id > ?) \
         WHERE u.id > ?",
        &[10, 20],
    ),
    (
        "DELETE FROM u WHERE EXISTS (SELECT 1 FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.g = ?1 AND x.id = u.id) AND u.id < ?2",
        &[2, 40],
    ),
    (
        "UPDATE u SET n = ?2 + EXISTS (SELECT 1 FROM t AS x WHERE x.id <= u.id \
         LIMIT 1 OFFSET ?1)",
        &[10, 100],
    ),
];

// rusqlite's bundled SQLite enforces foreign keys by default; fsqlite does not.
const RESET: &str = "PRAGMA foreign_keys = OFF; DROP TABLE IF EXISTS uc; DROP TABLE IF EXISTS u; \
                     DELETE FROM log;";
const FILL: &str = "INSERT INTO u SELECT id, v, g, n FROM t ORDER BY id;";
const CHECKS: &[&str] = &[
    "SELECT * FROM u ORDER BY id",
    "SELECT * FROM log ORDER BY rowid",
    "SELECT * FROM uc ORDER BY rowid",
];

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
    f.execute_batch(SCHEMA).await.unwrap();
    r.execute_batch(SCHEMA).unwrap();
    (f, r)
}

async fn compare(file_backed: bool) -> (Vec<String>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (f, r) = open_pair(file_backed, &dir).await;
    let db = if file_backed { "file" } else { "memory" };
    let mut diffs = Vec::new();
    let mut runs = 0usize;
    for (target, ddl, after_fill) in TARGETS {
        for (sql, params) in STATEMENTS {
            let values: Vec<SqliteValue> =
                params.iter().copied().map(SqliteValue::Integer).collect();
            for prepared in [false, true] {
                runs += 1;
                for setup in [RESET, *ddl, FILL, *after_fill] {
                    f.execute_batch(setup).await.unwrap();
                    r.execute_batch(setup).unwrap();
                }
                let stock = r
                    .execute(sql, rusqlite::params_from_iter(params.iter()))
                    .map_err(|e| e.to_string());
                let frank = match (prepared, values.is_empty()) {
                    (false, true) => f.execute(sql).await,
                    (false, false) => f.execute_with_params(sql, &values).await,
                    (true, _) => match f.prepare(sql).await {
                        Ok(stmt) if values.is_empty() => stmt.execute().await,
                        Ok(stmt) => stmt.execute_with_params(&values).await,
                        Err(e) => Err(e),
                    },
                }
                .map_err(|e| e.to_string());
                let path = format!("[{db}, {target}, prepared={prepared}] `{sql}` {params:?}");
                let changes_match = match (&frank, &stock) {
                    (Ok(frank), Ok(stock)) => frank == stock,
                    (Err(_), Err(_)) => true,
                    _ => false,
                };
                if !changes_match {
                    diffs.push(format!(
                        "  {path}\n     changes: frank= {frank:?} stock= {stock:?}"
                    ));
                }
                for check in CHECKS {
                    let (fr, rr) = (fq(&f, check).await, rq(&r, check));
                    if fr != rr {
                        diffs.push(format!(
                            "  {path}\n     `{check}`:\n     frank= {fr:?}\n     stock= {rr:?}"
                        ));
                    }
                }
            }
        }
    }
    f.close().await.unwrap();
    (diffs, runs)
}

#[test]
fn dml_correlated_subqueries_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let (mut diffs, mut runs) = compare(false).await;
        let (file_diffs, file_runs) = compare(true).await;
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

/// A SET subquery the compiled scan answers keeps the compiled UPDATE; only
/// one it cannot answer is evaluated row by row.
#[test]
fn only_uncompiled_set_subqueries_run_row_by_row() {
    const ROW_BY_ROW: &str = "update_row_by_row_correlated_set_fallback";
    let compiled = [
        "UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g)",
        "UPDATE u SET n = EXISTS (SELECT 1 FROM t AS x WHERE x.g = u.g AND x.id > u.id + 40)",
        "UPDATE u SET n = (SELECT x.n FROM t AS x WHERE x.id = u.id + 3)",
        "UPDATE u SET n = (SELECT 5)",
        "UPDATE u SET n = -1 WHERE EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id)",
    ];
    let row_by_row = [
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.id = u.id)",
        "UPDATE u SET n = EXISTS (SELECT 1 FROM t AS x JOIN k ON x.g = k.g WHERE x.id = u.id)",
        "UPDATE u SET n = (SELECT 5 WHERE u.n > 2)",
    ];
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let (f, _r) = open_pair(false, &dir).await;
        f.set_fallback_capture_enabled(true);
        let mut wrong = Vec::new();
        for (sql, expect_row_by_row) in compiled
            .iter()
            .map(|sql| (sql, false))
            .chain(row_by_row.iter().map(|sql| (sql, true)))
        {
            for setup in [RESET, TARGETS[0].1, FILL, TARGETS[0].2] {
                f.execute_batch(setup).await.unwrap();
            }
            f.reset_fallback_decision_evidence();
            f.execute(sql).await.unwrap();
            let went_row_by_row = f
                .fallback_decision_snapshot()
                .decisions
                .iter()
                .any(|decision| decision.decision_reason == ROW_BY_ROW);
            if went_row_by_row != expect_row_by_row {
                wrong.push(format!(
                    "  `{sql}`: row by row = {went_row_by_row}, expected {expect_row_by_row}"
                ));
            }
        }
        f.close().await.unwrap();
        assert!(wrong.is_empty(), "routing:\n{}", wrong.join("\n"));
    });
}
