#![recursion_limit = "512"]

//! bd-hga35: UPDATE and DELETE compiled every correlated subquery in
//! RETURNING with `emit_scalar_subquery` / `emit_exists_subquery`, which scan
//! one base table, so a subquery over a join, a view, an OFFSET and the other
//! bd-h7hox shapes returned 0 or NULL. Such a statement now runs row by row
//! and reads each row's RETURNING through the SELECT path: an UPDATE once the
//! row has changed, where stock evaluates RETURNING (so a subquery sees the
//! rows changed so far, this one included), a DELETE just before the row is
//! removed.
//!
//! Every statement runs alone on a fresh target and is compared with
//! rusqlite's bundled SQLite: the RETURNING rows in order, `changes()`, the
//! target's rows and the rows its triggers logged. Targets: a rowid table, a
//! WITHOUT ROWID table, a table with a hidden rowid, a rowid table with a
//! BEFORE UPDATE trigger, and a parent with a cascading child under foreign
//! keys. In memory and in a file, ad hoc and prepared.
//!
//! Not covered (the statement keeps the compiled lane, still wrong for these
//! subqueries): an UPDATE with AFTER UPDATE triggers, whose RETURNING stock
//! evaluates before them; a DELETE with DELETE triggers; an UPDATE that
//! assigns the row key.

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

/// Rows, or the error: only whether a statement fails is compared, not the
/// wording of its message.
type Rows = Result<Vec<Vec<String>>, ()>;

fn rq(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> Rows {
    let mut st = conn.prepare(sql).map_err(drop)?;
    let n = st.column_count();
    st.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(drop)?
    .collect::<Result<Vec<_>, _>>()
    .map_err(drop)
}

async fn fq(conn: &Connection, sql: &str, params: &[i64], prepared: bool) -> Rows {
    let values: Vec<SqliteValue> = params.iter().copied().map(SqliteValue::Integer).collect();
    let rows = if prepared {
        match conn.prepare(sql).await {
            Ok(stmt) if values.is_empty() => stmt.query().await,
            Ok(stmt) => stmt.query_with_params(&values).await,
            Err(error) => Err(error),
        }
    } else if values.is_empty() {
        conn.query(sql).await
    } else {
        conn.query_with_params(sql, &values).await
    };
    rows.map_err(drop).map(|rows| {
        rows.iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect()
    })
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
/// its table, then what follows the copy (a child table `uc`, a view of the
/// target, and for the foreign-key target the pragma).
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
        "before trigger",
        "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
         CREATE TRIGGER u_bu BEFORE UPDATE ON u BEGIN \
           INSERT INTO log VALUES (old.id, old.n, new.n); END;",
        PLAIN_CHILD,
    ),
    (
        "foreign keys",
        "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);",
        "CREATE TABLE uc(uid INTEGER REFERENCES u(id) ON DELETE CASCADE ON UPDATE CASCADE, \
           note TEXT);\
         INSERT INTO uc SELECT id, 'c' || id FROM t WHERE id % 4 = 0 ORDER BY id;\
         CREATE VIEW vu AS SELECT id, n FROM u WHERE n >= 0;\
         CREATE UNIQUE INDEX u_vg ON u(v, g);\
         PRAGMA foreign_keys = ON;",
    ),
];

const PLAIN_CHILD: &str = "CREATE TABLE uc(uid INTEGER, note TEXT);\
     INSERT INTO uc SELECT id, 'c' || id FROM t WHERE id % 4 = 0 ORDER BY id;\
     CREATE VIEW vu AS SELECT id, n FROM u WHERE n >= 0;\
     CREATE UNIQUE INDEX u_vg ON u(v, g);";

const STATEMENTS: &[(&str, &[i64])] = &[
    // The bead's statements.
    (
        "UPDATE u SET n = 0 WHERE id % 11 = 0 RETURNING id, \
         (SELECT count(*) FROM vt WHERE vt.g = u.g), \
         EXISTS (SELECT 1 FROM t AS x JOIN k ON x.g = k.g WHERE x.id = u.id)",
        &[],
    ),
    (
        "DELETE FROM u WHERE id % 13 = 0 RETURNING id, \
         (SELECT count(*) FROM t AS x JOIN k ON x.g = k.g WHERE k.label <> 'k3' \
           AND x.id = u.id), \
         EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id)",
        &[],
    ),
    // Joins, views, a derived table, an OFFSET, nested subqueries, `*`.
    (
        "UPDATE u SET n = n + 1 WHERE id < 8 RETURNING *, \
         (SELECT max(x.id) FROM t AS x JOIN k ON k.g = x.g WHERE x.n = u.n)",
        &[],
    ),
    (
        "UPDATE u SET v = v || '!' WHERE g = 4 RETURNING v, \
         (SELECT d.g FROM (SELECT id, g FROM t WHERE n > 1) AS d WHERE d.id = u.id + 1)",
        &[],
    ),
    (
        "UPDATE u SET n = -n WHERE id BETWEEN 20 AND 26 RETURNING id, n, \
         (SELECT x.id FROM t AS x WHERE x.g = u.g ORDER BY x.id LIMIT 1 OFFSET 2)",
        &[],
    ),
    (
        "UPDATE u SET n = 7 WHERE id IN (3, 30, 33) RETURNING id, \
         (SELECT count(*) FROM t AS x WHERE x.g = u.g \
           AND EXISTS (SELECT 1 FROM vt WHERE vt.id = x.id))",
        &[],
    ),
    (
        "DELETE FROM u WHERE g = 2 RETURNING *, \
         (SELECT k.label FROM vt JOIN k ON k.g = vt.g WHERE vt.id = u.id)",
        &[],
    ),
    (
        "DELETE FROM u WHERE id > 50 RETURNING id, \
         (SELECT count(*) FROM (SELECT id FROM t WHERE n > 2) AS x WHERE x.id < u.id)",
        &[],
    ),
    // RETURNING reading the target through a view after each row changes:
    // stock sees the rows changed so far, this one included.
    (
        "UPDATE u SET n = n + 100 WHERE id IN (2, 3, 4) RETURNING id, \
         (SELECT count(*) FROM vu WHERE vu.n >= 100 AND vu.id <= u.id)",
        &[],
    ),
    // A plain RETURNING keeps the compiled lane; no rows; WHERE routed too.
    (
        "UPDATE u SET n = n + 1 WHERE id < 4 RETURNING id, n, \
         (SELECT count(*) FROM t AS x WHERE x.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = 1 WHERE id > 1000 RETURNING id, \
         (SELECT count(*) FROM vt WHERE vt.g = u.g)",
        &[],
    ),
    (
        "DELETE FROM u WHERE EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id AND vt.g = 3) \
         RETURNING id, (SELECT count(*) FROM vt WHERE vt.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.id = u.id) WHERE id < 10 \
         RETURNING id, n, EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id + 1)",
        &[],
    ),
    // Bound parameters, numbered and anonymous, in SET, WHERE and RETURNING.
    (
        "UPDATE u SET n = ?1 WHERE id < ?2 RETURNING id, \
         (SELECT count(*) FROM vt WHERE vt.g = u.g + ?3)",
        &[9, 6, 1],
    ),
    (
        "UPDATE u SET n = ? WHERE id < ? RETURNING id, \
         (SELECT count(*) FROM vt WHERE vt.g = u.g + ?)",
        &[9, 6, 1],
    ),
    (
        "DELETE FROM u WHERE id < ?1 RETURNING id, \
         (SELECT count(*) FROM t AS x JOIN k ON x.g = k.g WHERE k.g = ?2 AND x.id = u.id)",
        &[12, 3],
    ),
    // A row OR IGNORE skips returns nothing (`u_vg` is unique on (v, g)).
    (
        "UPDATE OR IGNORE u SET v = 'v1', g = 1 WHERE id < 12 RETURNING id, v, \
         (SELECT count(*) FROM vt WHERE vt.g = u.g)",
        &[],
    ),
    // UPDATE FROM: a RETURNING subquery may read the FROM table's name in
    // its own FROM; RETURNING itself cannot name the FROM table.
    (
        "UPDATE u SET n = k.g + 1 FROM k WHERE k.g = u.g AND u.id < 20 RETURNING id, n, \
         (SELECT count(*) FROM vt WHERE vt.g = u.g)",
        &[],
    ),
    (
        "UPDATE u SET n = 0 FROM k WHERE k.g = u.g AND u.id < 9 RETURNING id, \
         (SELECT k.label FROM k JOIN vt ON vt.g = k.g WHERE vt.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = 0 FROM k WHERE k.g = u.g AND u.id < 5 RETURNING id, k.label, \
         (SELECT count(*) FROM vt WHERE vt.g = u.g)",
        &[],
    ),
    // RETURNING cannot name the target's alias: an error, nothing changed.
    (
        "UPDATE u AS a SET n = 0 WHERE id < 5 RETURNING a.id, \
         (SELECT count(*) FROM vt WHERE vt.g = a.g)",
        &[],
    ),
];

// rusqlite's bundled SQLite enforces foreign keys by default; fsqlite does not.
const RESET: &str = "PRAGMA foreign_keys = OFF; DROP VIEW IF EXISTS vu; DROP TABLE IF EXISTS uc; \
                     DROP TABLE IF EXISTS u; DELETE FROM log;";
const FILL: &str = "INSERT INTO u SELECT id, v, g, n FROM t ORDER BY id;";
const CHECKS: &[&str] = &[
    "SELECT changes()",
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
            for prepared in [false, true] {
                runs += 1;
                for setup in [RESET, *ddl, FILL, *after_fill] {
                    f.execute_batch(setup).await.unwrap();
                    r.execute_batch(setup).unwrap();
                }
                let path = format!("[{db}, {target}, prepared={prepared}] `{sql}` {params:?}");
                let (fr, rr) = (fq(&f, sql, params, prepared).await, rq(&r, sql, params));
                if fr != rr {
                    diffs.push(format!(
                        "  {path}\n     RETURNING:\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
                let failed = rr.is_err();
                for check in CHECKS {
                    // bd-ulgsj: a statement that fails to prepare resets
                    // changes() to 0 where stock keeps the previous count.
                    if failed && *check == "SELECT changes()" {
                        continue;
                    }
                    let (fr, rr) = (fq(&f, check, &[], false).await, rq(&r, check, &[]));
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
fn returning_correlated_subqueries_match_stock() {
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

/// Only RETURNING the compiled scan cannot answer runs row by row.
#[test]
fn only_uncompiled_returning_subqueries_run_row_by_row() {
    const ROW_BY_ROW: [&str; 2] = [
        "update_row_by_row_correlated_returning_fallback",
        "delete_row_by_row_correlated_returning_fallback",
    ];
    let routed = [
        "UPDATE u SET n = 0 WHERE id % 11 = 0 RETURNING id, \
         (SELECT count(*) FROM vt WHERE vt.g = u.g)",
        "DELETE FROM u WHERE id % 13 = 0 RETURNING id, \
         EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id)",
    ];
    let compiled = [
        "UPDATE u SET n = n + 1 WHERE id < 4 RETURNING id, n, \
         (SELECT count(*) FROM t AS x WHERE x.g = u.g)",
        "DELETE FROM u WHERE id < 4 RETURNING id",
        "UPDATE u SET n = 0 WHERE id < 4 RETURNING id, (SELECT count(*) FROM vt)",
    ];
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let (f, _r) = open_pair(false, &dir).await;
        f.set_fallback_capture_enabled(true);
        let mut wrong = Vec::new();
        for (sql, expect_routed) in routed
            .iter()
            .map(|sql| (sql, true))
            .chain(compiled.iter().map(|sql| (sql, false)))
        {
            for setup in [RESET, TARGETS[0].1, FILL, PLAIN_CHILD] {
                f.execute_batch(setup).await.unwrap();
            }
            f.reset_fallback_decision_evidence();
            f.query(sql).await.unwrap();
            let went_routed = f
                .fallback_decision_snapshot()
                .decisions
                .iter()
                .any(|decision| ROW_BY_ROW.contains(&decision.decision_reason.as_str()));
            if went_routed != expect_routed {
                wrong.push(format!(
                    "  `{sql}`: row by row = {went_routed}, expected {expect_routed}"
                ));
            }
        }
        f.close().await.unwrap();
        assert!(wrong.is_empty(), "routing:\n{}", wrong.join("\n"));
    });
}
