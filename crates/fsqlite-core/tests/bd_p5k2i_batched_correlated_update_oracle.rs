#![recursion_limit = "512"]

//! bd-p5k2i: an UPDATE whose SET clause contains an uncompiled correlated
//! subquery (over a view, join, derived table, CTE, etc.) used to run one
//! SELECT per target row (`DmlRowReplay::UpdateEvaluated`) and then one full
//! dispatched UPDATE per row. When none of the SET expressions read the target
//! table (directly or transitively through views/CTEs/virtual tables), and the
//! table has no UPDATE triggers, active foreign-key actions, replace-victim
//! callbacks, correlated RETURNING, or `OR FAIL`, compute every target row's
//! locator and SET values in ONE SELECT over the target table and apply them
//! with one precompiled single-row UPDATE program.
//!
//! Covers:
//! 1. Differential correctness against live bundled `rusqlite` across memory
//!    and file-backed DBs, ad hoc and prepared (including re-execution with
//!    distinct bound parameters), rowid / IPK / hidden-rowid / shadowed-rowid /
//!    WITHOUT ROWID / indexed / RETURNING / explicit-txn rollback shapes.
//! 2. Planted negatives: self-reading SET subqueries (direct, through a view,
//!    through a view-of-view, through a CTE), BEFORE/AFTER UPDATE triggers,
//!    `PRAGMA foreign_keys = ON`, and `UPDATE OR FAIL` must stay off the
//!    batched path and match stock (with proof that one-shot SELECT differs
//!    from stock on the self-reading shapes).
//! 3. Same-invocation 20,000-row A/B against live bundled `rusqlite` for both
//!    view and join correlated-SET UPDATE shapes.

use std::time::Instant;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const ROW_BY_ROW_REASON: &str = "update_row_by_row_correlated_set_fallback";
const BATCHED_REASON: &str = "update_batched_correlated_set_select";

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

fn logged_reason(conn: &Connection, reason: &str) -> bool {
    conn.fallback_decision_snapshot()
        .decisions
        .iter()
        .any(|decision| decision.decision_reason == reason)
}

const BASE_SCHEMA: &str = "\
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
CREATE INDEX t_g ON t(g);\
INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 60) \
  SELECT i, 'v' || (i % 7), i % 9, i % 5 FROM s;\
INSERT INTO k WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 6) \
  SELECT i, 'k' || i FROM s;\
CREATE VIEW vt AS SELECT id, g FROM t WHERE n > 1;\
CREATE VIEW v_of_v AS SELECT id, g FROM vt WHERE g <> 3;\
CREATE VIEW v_agg AS SELECT g, count(*) AS cnt FROM t GROUP BY g;";

const RESET_TARGET: &str = "\
PRAGMA foreign_keys = OFF;\
DROP VIEW IF EXISTS vvu;\
DROP VIEW IF EXISTS vu;\
DROP TABLE IF EXISTS uc;\
DROP TABLE IF EXISTS ulog;\
DROP TABLE IF EXISTS u;";

const FILL_U: &str = "INSERT INTO u SELECT id, v, g, n FROM t ORDER BY id;";

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
    f.execute_batch(BASE_SCHEMA).await.unwrap();
    r.execute_batch(BASE_SCHEMA).unwrap();
    (f, r)
}

const ELIGIBLE_TARGETS: &[(&str, &str)] = &[
    (
        "rowid_ipk",
        "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
         CREATE INDEX u_n ON u(n);",
    ),
    (
        "hidden_rowid",
        "CREATE TABLE u(id INTEGER, v TEXT, g INTEGER, n INTEGER);\
         CREATE INDEX u_n ON u(n);",
    ),
    (
        "without_rowid",
        "CREATE TABLE u(id INTEGER NOT NULL, v TEXT, g INTEGER NOT NULL, n INTEGER, \
         PRIMARY KEY(g, id)) WITHOUT ROWID;\
         CREATE INDEX u_n ON u(n);",
    ),
];

const ELIGIBLE_STATEMENTS: &[(&str, &[i64])] = &[
    // View and view-of-view correlated SET subqueries.
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM v_of_v WHERE v_of_v.g = u.g)",
        &[],
    ),
    // Non-flattenable aggregate view.
    (
        "UPDATE u SET n = COALESCE((SELECT cnt FROM v_agg WHERE v_agg.g = u.g), -1)",
        &[],
    ),
    // Multi-table join in SET subquery (scalar and EXISTS).
    (
        "UPDATE u SET n = (SELECT count(*) FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label <> 'k3' AND x.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = EXISTS (SELECT 1 FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label <> 'k3' AND x.id = u.id)",
        &[],
    ),
    // Derived table, CTE inside subquery, compound, json_each, correlated IN.
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
        "UPDATE u SET n = u.id IN (SELECT x.id FROM t AS x JOIN k ON x.g = k.g \
         WHERE k.label = 'k4' AND x.n = u.n)",
        &[],
    ),
    // Multiple SET assignments + target alias.
    (
        "UPDATE u AS tgt SET n = (SELECT count(*) FROM vt WHERE vt.g = tgt.g), \
         v = COALESCE((SELECT k.label FROM k JOIN t AS x ON x.g = k.g WHERE x.id = tgt.id), 'none')",
        &[],
    ),
    // Duplicate target column (last wins against pre-update row).
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.id = u.id), \
         n = n + (SELECT count(*) FROM v_of_v WHERE v_of_v.g = u.g)",
        &[],
    ),
    // WHERE with correlated view subquery + 0-row WHERE + 1-row WHERE.
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.g = u.g) \
         WHERE EXISTS (SELECT 1 FROM vt WHERE vt.id = u.id)",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.id = u.id) WHERE u.id = 8",
        &[],
    ),
    (
        "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.id = u.id) WHERE u.id > 9999",
        &[],
    ),
    // Bound parameters: numbered, anonymous, reordered.
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
        "UPDATE u SET n = ?2 + (SELECT count(*) FROM vt WHERE vt.g = u.g AND vt.id > ?1) \
         WHERE u.id <= ?1",
        &[15, 100],
    ),
    // Primary-key mutation without OR REPLACE.
    (
        "UPDATE u SET id = id + (SELECT count(*) FROM t AS x JOIN k ON x.g = k.g \
         WHERE x.id = u.id) * 1000",
        &[],
    ),
];

#[test]
fn batched_correlated_set_update_matches_stock_memory_and_file() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        let mut runs = 0usize;

        for file_backed in [false, true] {
            let db = if file_backed { "file" } else { "memory" };
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = open_pair(file_backed, &dir).await;
            f.set_fallback_capture_enabled(true);

            for (target, ddl) in ELIGIBLE_TARGETS {
                for (sql, params) in ELIGIBLE_STATEMENTS {
                    let values: Vec<SqliteValue> =
                        params.iter().copied().map(SqliteValue::Integer).collect();
                    for prepared in [false, true] {
                        runs += 1;
                        for setup in [RESET_TARGET, *ddl, FILL_U] {
                            f.execute_batch(setup).await.unwrap();
                            r.execute_batch(setup).unwrap();
                        }
                        f.reset_fallback_decision_evidence();
                        let stock = r
                            .execute(sql, rusqlite::params_from_iter(params.iter()))
                            .unwrap();
                        let frank = match (prepared, values.is_empty()) {
                            (false, true) => f.execute(sql).await.unwrap(),
                            (false, false) => f.execute_with_params(sql, &values).await.unwrap(),
                            (true, true) => f.prepare(sql).await.unwrap().execute().await.unwrap(),
                            (true, false) => f
                                .prepare(sql)
                                .await
                                .unwrap()
                                .execute_with_params(&values)
                                .await
                                .unwrap(),
                        };
                        let ctx = format!("[{db}, {target}, prepared={prepared}] `{sql}` {params:?}");
                        if frank != stock {
                            diffs.push(format!("{ctx}: changes frank={frank} stock={stock}"));
                        }
                        if !logged_reason(&f, BATCHED_REASON) {
                            diffs.push(format!(
                                "{ctx}: expected `{BATCHED_REASON}` fallback marker, got {:?}",
                                f.fallback_decision_snapshot().decisions
                            ));
                        }
                        if !logged_reason(&f, ROW_BY_ROW_REASON) {
                            diffs.push(format!(
                                "{ctx}: expected `{ROW_BY_ROW_REASON}` compatibility marker"
                            ));
                        }
                        for check in [
                            "SELECT id, v, g, n FROM u ORDER BY id",
                            "SELECT n, count(*) FROM u GROUP BY n ORDER BY n",
                        ] {
                            let (fr, rr) = (fq(&f, check).await, rq(&r, check));
                            if fr != rr {
                                diffs.push(format!(
                                    "{ctx}: `{check}` diverged:\n  frank={fr:?}\n  stock={rr:?}"
                                ));
                            }
                        }
                    }
                }
            }

            // Prepared re-execution with distinct parameter bindings on the SAME prepared statement.
            for setup in [RESET_TARGET, ELIGIBLE_TARGETS[0].1, FILL_U] {
                f.execute_batch(setup).await.unwrap();
                r.execute_batch(setup).unwrap();
            }
            let reuse_sql = "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.g = u.g AND vt.id > ?1) \
                             WHERE u.id BETWEEN ?2 AND ?3";
            let f_stmt = f.prepare(reuse_sql).await.unwrap();
            let mut r_stmt = r.prepare(reuse_sql).unwrap();
            for (p1, p2, p3) in [(5_i64, 1_i64, 20_i64), (25, 21, 45), (40, 46, 60)] {
                runs += 1;
                f.reset_fallback_decision_evidence();
                let f_changes = f_stmt
                    .execute_with_params(&[
                        SqliteValue::Integer(p1),
                        SqliteValue::Integer(p2),
                        SqliteValue::Integer(p3),
                    ])
                    .await
                    .unwrap();
                let r_changes = r_stmt.execute(rusqlite::params![p1, p2, p3]).unwrap();
                if f_changes != r_changes || !logged_reason(&f, BATCHED_REASON) {
                    diffs.push(format!(
                        "[{db}, prepared-reuse ({p1},{p2},{p3})]: changes {f_changes}/{r_changes}, batched={}",
                        logged_reason(&f, BATCHED_REASON)
                    ));
                }
                let check = "SELECT id, v, g, n FROM u ORDER BY id";
                if fq(&f, check).await != rq(&r, check) {
                    diffs.push(format!("[{db}, prepared-reuse ({p1},{p2},{p3})]: rows diverged"));
                }
            }
            drop(r_stmt);

            // Shadowed rowid / _rowid_ / oid table + RETURNING + explicit-txn UNIQUE rollback.
            let extra_setup = "\
                DROP TABLE IF EXISTS shadowed;\
                CREATE TABLE shadowed(rowid INT, id INTEGER PRIMARY KEY, _rowid_ INT, oid INT, n INT);\
                INSERT INTO shadowed SELECT 99, id, 99, 99, n FROM t WHERE id <= 20;";
            f.execute_batch(extra_setup).await.unwrap();
            r.execute_batch(extra_setup).unwrap();
            let shadowed_sql =
                "UPDATE shadowed SET n = (SELECT count(*) FROM vt WHERE vt.id = shadowed.id) \
                 WHERE shadowed.id <= ?1 RETURNING id, n";
            f.reset_fallback_decision_evidence();
            let f_ret: Vec<Vec<String>> = f
                .prepare(shadowed_sql)
                .await
                .unwrap()
                .query_with_params(&[SqliteValue::Integer(10)])
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect();
            let mut r_ret_stmt = r.prepare(shadowed_sql).unwrap();
            let r_ret: Vec<Vec<String>> = r_ret_stmt
                .query_map([10_i64], |row| {
                    Ok(vec![
                        tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(0)),
                        tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(1)),
                    ])
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            drop(r_ret_stmt);
            runs += 1;
            if f_ret != r_ret || !logged_reason(&f, BATCHED_REASON) {
                diffs.push(format!(
                    "[{db}, shadowed+RETURNING]: f_ret={f_ret:?} r_ret={r_ret:?} batched={}",
                    logged_reason(&f, BATCHED_REASON)
                ));
            }

            // Explicit transaction statement rollback on UNIQUE conflict (default ABORT).
            let uniq_setup = "\
                DROP TABLE IF EXISTS uniq_u;\
                CREATE TABLE uniq_u(id INTEGER PRIMARY KEY, ucol INTEGER UNIQUE, g INTEGER);\
                INSERT INTO uniq_u VALUES (1, 100, 1), (2, 200, 1), (3, 300, 2);";
            f.execute_batch(uniq_setup).await.unwrap();
            r.execute_batch(uniq_setup).unwrap();
            f.execute("BEGIN").await.unwrap();
            r.execute("BEGIN", []).unwrap();
            let conflict_sql =
                "UPDATE uniq_u SET ucol = (SELECT count(*) FROM vt WHERE vt.g = uniq_u.g)";
            f.reset_fallback_decision_evidence();
            let f_err = f.execute(conflict_sql).await.is_err();
            let r_err = r.execute(conflict_sql, []).is_err();
            runs += 1;
            if f_err != r_err || !f_err || !logged_reason(&f, BATCHED_REASON) {
                diffs.push(format!(
                    "[{db}, txn-abort]: f_err={f_err} r_err={r_err} batched={}",
                    logged_reason(&f, BATCHED_REASON)
                ));
            }
            if fq(&f, "SELECT * FROM uniq_u ORDER BY id").await
                != rq(&r, "SELECT * FROM uniq_u ORDER BY id")
            {
                diffs.push(format!("[{db}, txn-abort]: partial write leaked after ABORT"));
            }
            f.execute("COMMIT").await.unwrap();
            r.execute("COMMIT", []).unwrap();

            f.close().await.unwrap();
        }

        assert!(
            diffs.is_empty(),
            "{} divergence(s) across {runs} runs:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

#[test]
fn planted_negatives_stay_row_by_row_and_match_stock() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let db = if file_backed { "file" } else { "memory" };
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = open_pair(file_backed, &dir).await;
            f.set_fallback_capture_enabled(true);

            // 1. Self-reading SET subqueries: direct, view of target, view of view, CTE.
            //    Each is verified to produce a DIFFERENT table under one-shot SELECT
            //    than stock SQLite's progressive row-by-row semantics.
            let self_read_cases: &[(&str, &str, &str)] = &[
                (
                    "direct_join_self_read",
                    "UPDATE u SET n = (SELECT count(*) FROM u AS x JOIN k ON x.g = k.g \
                     WHERE x.n >= 2 AND x.id < u.id)",
                    "SELECT u.id, (SELECT count(*) FROM u AS x JOIN k ON x.g = k.g \
                     WHERE x.n >= 2 AND x.id < u.id) FROM u ORDER BY u.id",
                ),
                (
                    "view_of_target_self_read",
                    "UPDATE u SET n = (SELECT count(*) FROM vu WHERE vu.n >= 2 AND vu.id < u.id)",
                    "SELECT u.id, (SELECT count(*) FROM vu WHERE vu.n >= 2 AND vu.id < u.id) \
                     FROM u ORDER BY u.id",
                ),
                (
                    "view_of_view_of_target_self_read",
                    "UPDATE u SET n = (SELECT count(*) FROM vvu WHERE vvu.n >= 2 AND vvu.id < u.id)",
                    "SELECT u.id, (SELECT count(*) FROM vvu WHERE vvu.n >= 2 AND vvu.id < u.id) \
                     FROM u ORDER BY u.id",
                ),
                (
                    "cte_in_set_self_read",
                    "UPDATE u SET n = (WITH c AS (SELECT * FROM u) \
                     SELECT count(*) FROM c WHERE c.n >= 2 AND c.id < u.id)",
                    "SELECT u.id, (WITH c AS (SELECT * FROM u) \
                     SELECT count(*) FROM c WHERE c.n >= 2 AND c.id < u.id) \
                     FROM u ORDER BY u.id",
                ),
            ];

            for &(label, update_sql, oneshot_select_sql) in self_read_cases {
                let setup = format!(
                    "{RESET_TARGET}\
                     CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
                     {FILL_U}\
                     CREATE VIEW vu AS SELECT id, g, n FROM u;\
                     CREATE VIEW vvu AS SELECT id, g, n FROM vu;"
                );
                f.execute_batch(&setup).await.unwrap();
                r.execute_batch(&setup).unwrap();

                // Prove the planted negative is discriminating: one-shot SELECT differs from stock UPDATE.
                let oneshot_snapshot = rq(&r, oneshot_select_sql);

                f.reset_fallback_decision_evidence();
                let f_changes = f.execute(update_sql).await.unwrap();
                let r_changes = r.execute(update_sql, []).unwrap();
                assert_eq!(f_changes, r_changes, "[{db}, {label}] change count");

                let stock_after = rq(&r, "SELECT id, n FROM u ORDER BY id");
                let frank_after = fq(&f, "SELECT id, n FROM u ORDER BY id").await;
                assert_ne!(
                    stock_after, oneshot_snapshot,
                    "[{db}, {label}] planted negative must distinguish one-shot SELECT from progressive UPDATE"
                );
                assert_eq!(frank_after, stock_after, "[{db}, {label}] rows must match stock");
                assert!(
                    !logged_reason(&f, BATCHED_REASON),
                    "[{db}, {label}] self-reading SET must NOT take `{BATCHED_REASON}`"
                );
                assert!(
                    logged_reason(&f, ROW_BY_ROW_REASON),
                    "[{db}, {label}] self-reading SET must stay on `{ROW_BY_ROW_REASON}`"
                );
            }

            // 2. BEFORE and AFTER UPDATE triggers must stay off the batched path.
            for (label, trig_ddl) in [
                (
                    "before_trigger",
                    "CREATE TABLE ulog(id, old_n, new_n);\
                     CREATE TRIGGER u_bu BEFORE UPDATE ON u BEGIN \
                       INSERT INTO ulog VALUES (old.id, old.n, new.n); END;",
                ),
                (
                    "after_trigger",
                    "CREATE TABLE ulog(id, old_n, new_n);\
                     CREATE TRIGGER u_au AFTER UPDATE ON u BEGIN \
                       INSERT INTO ulog VALUES (old.id, old.n, new.n); END;",
                ),
            ] {
                let setup = format!(
                    "{RESET_TARGET}\
                     CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
                     {FILL_U}{trig_ddl}"
                );
                f.execute_batch(&setup).await.unwrap();
                r.execute_batch(&setup).unwrap();
                let sql = "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.id = u.id)";
                f.reset_fallback_decision_evidence();
                assert_eq!(
                    f.execute(sql).await.unwrap(),
                    r.execute(sql, []).unwrap(),
                    "[{db}, {label}]"
                );
                assert!(
                    !logged_reason(&f, BATCHED_REASON),
                    "[{db}, {label}] trigger table must NOT take `{BATCHED_REASON}`"
                );
                assert_eq!(
                    fq(&f, "SELECT * FROM u ORDER BY id").await,
                    rq(&r, "SELECT * FROM u ORDER BY id"),
                    "[{db}, {label}] u rows"
                );
                assert_eq!(
                    fq(&f, "SELECT * FROM ulog ORDER BY rowid").await,
                    rq(&r, "SELECT * FROM ulog ORDER BY rowid"),
                    "[{db}, {label}] ulog rows"
                );
            }

            // 3. PRAGMA foreign_keys = ON must stay off the batched path.
            {
                let setup = format!(
                    "{RESET_TARGET}\
                     CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
                     {FILL_U}\
                     CREATE TABLE uc(uid INTEGER REFERENCES u(id) ON UPDATE CASCADE, note TEXT);\
                     INSERT INTO uc SELECT id, 'c' || id FROM t WHERE id % 4 = 0 ORDER BY id;\
                     PRAGMA foreign_keys = ON;"
                );
                f.execute_batch(&setup).await.unwrap();
                r.execute_batch(&setup).unwrap();
                let sql = "UPDATE u SET id = id + (SELECT count(*) FROM vt WHERE vt.id = u.id) * 1000";
                f.reset_fallback_decision_evidence();
                assert_eq!(
                    f.execute(sql).await.unwrap(),
                    r.execute(sql, []).unwrap(),
                    "[{db}, fk_cascade]"
                );
                assert!(
                    !logged_reason(&f, BATCHED_REASON),
                    "[{db}, fk_cascade] FK-enabled UPDATE must NOT take `{BATCHED_REASON}`"
                );
                assert_eq!(
                    fq(&f, "SELECT * FROM u ORDER BY id").await,
                    rq(&r, "SELECT * FROM u ORDER BY id"),
                    "[{db}, fk_cascade] u"
                );
                assert_eq!(
                    fq(&f, "SELECT * FROM uc ORDER BY rowid").await,
                    rq(&r, "SELECT * FROM uc ORDER BY rowid"),
                    "[{db}, fk_cascade] uc"
                );
            }

            // 4. UPDATE OR FAIL must stay off the batched path and preserve prior rows on constraint error.
            {
                let setup = "\
                    PRAGMA foreign_keys = OFF;\
                    DROP TABLE IF EXISTS fail_u;\
                    CREATE TABLE fail_u(id INTEGER PRIMARY KEY, n INTEGER CHECK(n < 5));\
                    INSERT INTO fail_u VALUES (1, 0), (2, 0), (3, 0);";
                f.execute_batch(setup).await.unwrap();
                r.execute_batch(setup).unwrap();
                let sql = "UPDATE OR FAIL fail_u SET n = CASE WHEN fail_u.id = 2 THEN 99 \
                           ELSE (SELECT count(*) FROM vt WHERE vt.id = fail_u.id) END";
                f.reset_fallback_decision_evidence();
                assert!(f.execute(sql).await.is_err(), "[{db}, or_fail] fsqlite error");
                assert!(r.execute(sql, []).is_err(), "[{db}, or_fail] stock error");
                assert!(
                    !logged_reason(&f, BATCHED_REASON),
                    "[{db}, or_fail] OR FAIL must NOT take `{BATCHED_REASON}`"
                );
                assert_eq!(
                    fq(&f, "SELECT * FROM fail_u ORDER BY id").await,
                    rq(&r, "SELECT * FROM fail_u ORDER BY id"),
                    "[{db}, or_fail] partial rows"
                );
            }

            // 5. Supported single-table compiled correlated UPDATE must take NEITHER fallback.
            {
                let setup = format!(
                    "{RESET_TARGET}\
                     CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT, g INTEGER, n INTEGER);\
                     {FILL_U}"
                );
                f.execute_batch(&setup).await.unwrap();
                f.reset_fallback_decision_evidence();
                f.execute("UPDATE u SET n = (SELECT count(*) FROM t AS x WHERE x.g = u.g)")
                    .await
                    .unwrap();
                assert!(!logged_reason(&f, BATCHED_REASON), "[{db}, compiled]");
                assert!(!logged_reason(&f, ROW_BY_ROW_REASON), "[{db}, compiled]");
            }

            f.close().await.unwrap();
        }
    });
}

#[test]
fn same_invocation_20k_row_ab_vs_stock() {
    const N_ROWS: i64 = 20_000;
    let setup = format!(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER, n INTEGER);\
         CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
         CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER, n INTEGER);\
         INSERT INTO k WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 8) \
           SELECT i, 'k' || i FROM s;\
         INSERT INTO t WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < {N_ROWS}) \
           SELECT i, i % 9, i % 5 FROM s;\
         INSERT INTO u SELECT id, g, n FROM t;\
         CREATE VIEW vt AS SELECT id, g FROM t WHERE n > 1;"
    );

    let workloads: &[(&str, &str)] = &[
        (
            "view_20k",
            "UPDATE u SET n = (SELECT count(*) FROM vt WHERE vt.id = u.id)",
        ),
        (
            "join_20k",
            "UPDATE u SET n = (SELECT count(*) FROM t AS x JOIN k ON x.g = k.g \
             WHERE k.label <> 'k3' AND x.id = u.id)",
        ),
    ];

    asupersync::test_utils::run_test(|| async {
        for &(label, sql) in workloads {
            let f = Connection::open(":memory:").await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            f.execute_batch(&setup).await.unwrap();
            r.execute_batch(&setup).unwrap();
            f.set_fallback_capture_enabled(true);
            f.reset_fallback_decision_evidence();

            let r_start = Instant::now();
            let r_changes = r.execute(sql, []).unwrap();
            let r_elapsed = r_start.elapsed();

            let f_start = Instant::now();
            let f_changes = f.execute(sql).await.unwrap();
            let f_elapsed = f_start.elapsed();

            let ratio = f_elapsed.as_secs_f64() / r_elapsed.as_secs_f64().max(1e-9);
            let took_batched = logged_reason(&f, BATCHED_REASON);
            println!(
                "\nBD_P5K2I_AB label={label} rows={N_ROWS} batched={took_batched} \
                 fsqlite_ms={:.2} stock_ms={:.2} ratio={ratio:.2}x",
                f_elapsed.as_secs_f64() * 1000.0,
                r_elapsed.as_secs_f64() * 1000.0,
            );

            assert_eq!(f_changes, r_changes, "{label} change count");
            let check = "SELECT count(*), sum(n), sum(id * n) FROM u";
            assert_eq!(fq(&f, check).await, rq(&r, check), "{label} summary parity");
            assert_eq!(
                fq(&f, "SELECT id, n FROM u WHERE id % 997 = 0 ORDER BY id").await,
                rq(&r, "SELECT id, n FROM u WHERE id % 997 = 0 ORDER BY id"),
                "{label} sampled rows parity"
            );
            assert!(
                took_batched,
                "{label} must run on `{BATCHED_REASON}` single-SELECT path"
            );

            f.close().await.unwrap();
        }
    });
}
