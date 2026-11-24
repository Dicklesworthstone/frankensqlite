#![recursion_limit = "512"]

//! GH #542: Failed INSERT (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK, NOT NULL)
//! and UPSERT (`ON CONFLICT DO UPDATE` / `DO NOTHING`) / `INSERT OR IGNORE`
//! must match C SQLite (`rusqlite`) on `last_insert_rowid()`, `changes()`, and
//! `total_changes()` across non-rowid-alias PRIMARY KEY tables, INTEGER PRIMARY
//! KEY tables, multi-row inserts, RETURNING, triggers, schema conflict clauses,
//! deferred vs immediate foreign keys, AUTOINCREMENT, WITHOUT ROWID, and TEMP
//! tables — both in-memory and file-backed, via `execute`,
//! `execute_with_params`, and prepared statements.

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

async fn f_query(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|r| r.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

fn r_query(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let cols = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            Ok((0..cols)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

#[derive(Debug, PartialEq, Eq)]
struct StepObs {
    ok: bool,
    sql_last_insert_rowid: i64,
    api_last_insert_rowid: i64,
    changes: i64,
    total_changes: i64,
}

async fn f_counters(conn: &Connection) -> (i64, i64, i64, i64) {
    let rows = conn
        .query("SELECT last_insert_rowid(), changes(), total_changes()")
        .await
        .expect("fsqlite counter query should succeed");
    let vals = rows[0].values();
    let sql_lir = match vals[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected int last_insert_rowid, got {other:?}"),
    };
    let changes = match vals[1] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected int changes, got {other:?}"),
    };
    let total_changes = match vals[2] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected int total_changes, got {other:?}"),
    };
    (sql_lir, conn.last_insert_rowid(), changes, total_changes)
}

fn r_counters(conn: &rusqlite::Connection) -> (i64, i64, i64, i64) {
    let (sql_lir, changes, total_changes): (i64, i64, i64) = conn
        .query_row(
            "SELECT last_insert_rowid(), changes(), total_changes()",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("rusqlite counter query should succeed");
    (sql_lir, conn.last_insert_rowid(), changes, total_changes)
}

async fn step_exec(
    f: &Connection,
    r: &rusqlite::Connection,
    label: &str,
    sql: &str,
    diffs: &mut Vec<String>,
) {
    let f_ok = f.execute(sql).await.is_ok();
    let r_ok = r.execute(sql, []).is_ok();
    let (f_sql_lir, f_api_lir, f_chg, f_tot) = f_counters(f).await;
    let (r_sql_lir, r_api_lir, r_chg, r_tot) = r_counters(r);
    let f_obs = StepObs {
        ok: f_ok,
        sql_last_insert_rowid: f_sql_lir,
        api_last_insert_rowid: f_api_lir,
        changes: f_chg,
        total_changes: f_tot,
    };
    let r_obs = StepObs {
        ok: r_ok,
        sql_last_insert_rowid: r_sql_lir,
        api_last_insert_rowid: r_api_lir,
        changes: r_chg,
        total_changes: r_tot,
    };
    if f_obs != r_obs {
        diffs.push(format!(
            "  [{label}] sql={sql}\n    frank={f_obs:?}\n    stock={r_obs:?}"
        ));
    }
}

fn to_rusqlite_value(v: &SqliteValue) -> rusqlite::types::Value {
    match v {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

async fn step_exec_params(
    f: &Connection,
    r: &rusqlite::Connection,
    label: &str,
    sql: &str,
    params: &[SqliteValue],
    use_prepared: bool,
    diffs: &mut Vec<String>,
) {
    let f_ok = if use_prepared {
        let stmt = f.prepare(sql).await.expect("fsqlite prepare");
        stmt.execute_with_params(params).await.is_ok()
    } else {
        f.execute_with_params(sql, params).await.is_ok()
    };
    let r_vals: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite_value).collect();
    let r_ok = r
        .execute(sql, rusqlite::params_from_iter(r_vals.iter()))
        .is_ok();
    let (f_sql_lir, f_api_lir, f_chg, f_tot) = f_counters(f).await;
    let (r_sql_lir, r_api_lir, r_chg, r_tot) = r_counters(r);
    let f_obs = StepObs {
        ok: f_ok,
        sql_last_insert_rowid: f_sql_lir,
        api_last_insert_rowid: f_api_lir,
        changes: f_chg,
        total_changes: f_tot,
    };
    let r_obs = StepObs {
        ok: r_ok,
        sql_last_insert_rowid: r_sql_lir,
        api_last_insert_rowid: r_api_lir,
        changes: r_chg,
        total_changes: r_tot,
    };
    if f_obs != r_obs {
        diffs.push(format!(
            "  [{label}] sql={sql} params={params:?} prepared={use_prepared}\n    frank={f_obs:?}\n    stock={r_obs:?}"
        ));
    }
}

async fn step_query(
    f: &Connection,
    r: &rusqlite::Connection,
    label: &str,
    sql: &str,
    diffs: &mut Vec<String>,
) {
    let f_rows = f_query(f, sql).await;
    let r_rows = r_query(r, sql);
    let f_ok = f_rows.is_ok();
    let r_ok = r_rows.is_ok();
    if f_ok != r_ok || (f_ok && f_rows.as_ref().ok() != r_rows.as_ref().ok()) {
        diffs.push(format!(
            "  [{label} rows] sql={sql}\n    frank={f_rows:?}\n    stock={r_rows:?}"
        ));
    }
    let (f_sql_lir, f_api_lir, f_chg, f_tot) = f_counters(f).await;
    let (r_sql_lir, r_api_lir, r_chg, r_tot) = r_counters(r);
    let f_obs = StepObs {
        ok: f_ok,
        sql_last_insert_rowid: f_sql_lir,
        api_last_insert_rowid: f_api_lir,
        changes: f_chg,
        total_changes: f_tot,
    };
    let r_obs = StepObs {
        ok: r_ok,
        sql_last_insert_rowid: r_sql_lir,
        api_last_insert_rowid: r_api_lir,
        changes: r_chg,
        total_changes: r_tot,
    };
    if f_obs != r_obs {
        diffs.push(format!(
            "  [{label} counters] sql={sql}\n    frank={f_obs:?}\n    stock={r_obs:?}"
        ));
    }
}

async fn run_issue_542_core_script(f: &Connection, r: &rusqlite::Connection, mode: &str) {
    let mut diffs = Vec::new();

    step_exec(
        f,
        r,
        &format!("{mode}/create"),
        "CREATE TABLE t (id BIGINT NOT NULL, val TEXT, PRIMARY KEY (id))",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/insert_12"),
        "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),\
         (7,'g'),(8,'h'),(9,'i'),(10,'j'),(11,'k'),(12,'l')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/delete_12"),
        "DELETE FROM t WHERE id = 12",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/dup_pk_literal"),
        "INSERT INTO t VALUES (11, 'dup')",
        &mut diffs,
    )
    .await;
    step_exec_params(
        f,
        r,
        &format!("{mode}/dup_pk_params"),
        "INSERT INTO t VALUES (?1, ?2)",
        &[SqliteValue::Integer(11), SqliteValue::from("dup2")],
        false,
        &mut diffs,
    )
    .await;
    step_exec_params(
        f,
        r,
        &format!("{mode}/dup_pk_prepared"),
        "INSERT INTO t VALUES (?1, ?2)",
        &[SqliteValue::Integer(11), SqliteValue::from("dup3")],
        true,
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/upsert_do_nothing_conflict"),
        "INSERT INTO t VALUES (11, 'dup') ON CONFLICT(id) DO NOTHING",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/upsert_do_update_conflict"),
        "INSERT INTO t VALUES (11, 'updated') ON CONFLICT(id) DO UPDATE SET val = excluded.val",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/insert_or_ignore_conflict"),
        "INSERT OR IGNORE INTO t VALUES (11, 'dup')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/upsert_do_update_insert_reuse"),
        "INSERT INTO t VALUES (13, 'ok') ON CONFLICT(id) DO UPDATE SET val = excluded.val",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/upsert_do_update_insert_new"),
        "INSERT INTO t VALUES (14, 'ok2') ON CONFLICT(id) DO UPDATE SET val = excluded.val",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/dup_pk_no_prior_delete"),
        "INSERT INTO t VALUES (11, 'dup_again')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/upsert_do_update_where_false"),
        "INSERT INTO t VALUES (11, 'skip') ON CONFLICT(id) DO UPDATE SET val = excluded.val WHERE 0",
        &mut diffs,
    )
    .await;

    // Multi-row failures on non-rowid-alias PK table
    step_exec(
        f,
        r,
        &format!("{mode}/multirow_abort"),
        "INSERT INTO t VALUES (20, 'ok'), (11, 'dup')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/begin_for_rollback"),
        "BEGIN",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/multirow_rollback"),
        "INSERT OR ROLLBACK INTO t VALUES (21, 'ok'), (11, 'dup')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/multirow_fail"),
        "INSERT OR FAIL INTO t VALUES (22, 'ok'), (11, 'dup')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/multirow_ignore"),
        "INSERT OR IGNORE INTO t VALUES (23, 'ok'), (11, 'dup'), (24, 'ok2')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/insert_or_replace"),
        "INSERT OR REPLACE INTO t VALUES (11, 'replaced')",
        &mut diffs,
    )
    .await;

    // INSERT ... RETURNING paths
    step_query(
        f,
        r,
        &format!("{mode}/insert_returning_ok"),
        "INSERT INTO t VALUES (30, 'ret') RETURNING id, val",
        &mut diffs,
    )
    .await;
    step_query(
        f,
        r,
        &format!("{mode}/insert_returning_fail"),
        "INSERT INTO t VALUES (30, 'ret_dup') RETURNING id, val",
        &mut diffs,
    )
    .await;
    step_query(
        f,
        r,
        &format!("{mode}/upsert_do_update_returning"),
        "INSERT INTO t VALUES (30, 'ret_upd') ON CONFLICT(id) DO UPDATE SET val = excluded.val RETURNING id, val",
        &mut diffs,
    )
    .await;
    step_query(
        f,
        r,
        &format!("{mode}/upsert_do_nothing_returning"),
        "INSERT INTO t VALUES (30, 'ret_ign') ON CONFLICT(id) DO NOTHING RETURNING id, val",
        &mut diffs,
    )
    .await;

    assert!(
        diffs.is_empty(),
        "GH #542 core parity diffs ({mode}):\n{}",
        diffs.join("\n")
    );
}

#[test]
fn gh542_issue_repro_and_upsert_matrix_memory_and_file() {
    asupersync::test_utils::run_test(|| async {
        let f_mem = Connection::open(":memory:").await.unwrap();
        let r_mem = rusqlite::Connection::open_in_memory().unwrap();
        run_issue_542_core_script(&f_mem, &r_mem, "memory").await;
        f_mem.close().await.unwrap();

        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("f.db");
        let r_path = dir.path().join("r.db");
        let f_file = Connection::open(f_path.to_str().unwrap()).await.unwrap();
        let r_file = rusqlite::Connection::open(&r_path).unwrap();
        run_issue_542_core_script(&f_file, &r_file, "file").await;
        f_file.close().await.unwrap();
    });
}

async fn run_sqlmodel_differential_and_fk_script(
    f: &Connection,
    r: &rusqlite::Connection,
    mode: &str,
) {
    let mut diffs = Vec::new();

    step_exec(
        f,
        r,
        &format!("{mode}/fk_on"),
        "PRAGMA foreign_keys = ON",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/create_teams"),
        "CREATE TABLE diff_teams (\
            id BIGINT NOT NULL, \
            name TEXT NOT NULL UNIQUE, \
            motto TEXT, \
            PRIMARY KEY (id)\
         )",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/create_players"),
        "CREATE TABLE diff_players (\
            id BIGINT NOT NULL, \
            team_id BIGINT NOT NULL REFERENCES diff_teams(id), \
            name TEXT NOT NULL, \
            score INTEGER NOT NULL, \
            active INTEGER NOT NULL, \
            weight REAL, \
            PRIMARY KEY (id)\
         )",
        &mut diffs,
    )
    .await;

    for (id, name, motto) in [
        (1, "red", SqliteValue::Null),
        (2, "blue", SqliteValue::from("go blue")),
        (3, "green", SqliteValue::Null),
    ] {
        step_exec_params(
            f,
            r,
            &format!("{mode}/insert_team_{id}"),
            "INSERT INTO diff_teams (id, name, motto) VALUES (?1, ?2, ?3)",
            &[SqliteValue::Integer(id), SqliteValue::from(name), motto],
            false,
            &mut diffs,
        )
        .await;
    }

    step_exec(
        f,
        r,
        &format!("{mode}/bulk_players"),
        "INSERT INTO diff_players (id, team_id, name, score, active, weight) VALUES \
         (1,1,'p1',10,0,1.5),(2,2,'p2',20,1,3.0),(3,3,'p3',30,0,4.5),(4,1,'p4',40,1,NULL),\
         (5,2,'p5',50,0,7.5),(6,3,'p6',60,1,9.0),(7,1,'p7',70,0,10.5),(8,2,'p8',80,1,NULL),\
         (9,3,'p9',90,0,13.5),(10,1,'p10',100,1,15.0),(11,2,'p11',110,0,16.5),(12,3,'p12',120,1,NULL)",
        &mut diffs,
    )
    .await;

    step_exec_params(
        f,
        r,
        &format!("{mode}/dup_pk_team"),
        "INSERT INTO diff_teams (id, name, motto) VALUES (?1, ?2, ?3)",
        &[
            SqliteValue::Integer(1),
            SqliteValue::from("again"),
            SqliteValue::Null,
        ],
        false,
        &mut diffs,
    )
    .await;

    step_exec_params(
        f,
        r,
        &format!("{mode}/dup_unique_team"),
        "INSERT INTO diff_teams (id, name, motto) VALUES (?1, ?2, ?3)",
        &[
            SqliteValue::Integer(9),
            SqliteValue::from("red"),
            SqliteValue::Null,
        ],
        false,
        &mut diffs,
    )
    .await;

    // Foreign key violation on diff_players (unprepared, params, prepared!)
    step_exec(
        f,
        r,
        &format!("{mode}/fk_violation_literal"),
        "INSERT INTO diff_players (id, team_id, name, score, active, weight) \
         VALUES (998, 42, 'orphan_lit', 0, 1, NULL)",
        &mut diffs,
    )
    .await;
    step_exec_params(
        f,
        r,
        &format!("{mode}/fk_violation_params"),
        "INSERT INTO diff_players (id, team_id, name, score, active, weight) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        &[
            SqliteValue::Integer(999),
            SqliteValue::Integer(42),
            SqliteValue::from("orphan"),
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Null,
        ],
        false,
        &mut diffs,
    )
    .await;
    step_exec_params(
        f,
        r,
        &format!("{mode}/fk_violation_prepared"),
        "INSERT INTO diff_players (id, team_id, name, score, active, weight) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        &[
            SqliteValue::Integer(1000),
            SqliteValue::Integer(42),
            SqliteValue::from("orphan_prep"),
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Null,
        ],
        true,
        &mut diffs,
    )
    .await;

    // Exact sqlmodel_rust UPSERT probes after constraint violations
    step_exec_params(
        f,
        r,
        &format!("{mode}/upsert_do_update"),
        "INSERT INTO diff_teams (id, name, motto) VALUES (?1, ?2, ?3) \
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, motto = EXCLUDED.motto",
        &[
            SqliteValue::Integer(1),
            SqliteValue::from("crimson"),
            SqliteValue::from("still red"),
        ],
        false,
        &mut diffs,
    )
    .await;
    step_exec_params(
        f,
        r,
        &format!("{mode}/upsert_do_nothing"),
        "INSERT INTO diff_teams (id, name, motto) VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING",
        &[
            SqliteValue::Integer(2),
            SqliteValue::from("ignored"),
            SqliteValue::Null,
        ],
        false,
        &mut diffs,
    )
    .await;

    // Next valid insert after rejected FK inserts must reuse rowid 13 cleanly
    step_exec_params(
        f,
        r,
        &format!("{mode}/valid_player_after_fk_fails"),
        "INSERT INTO diff_players (id, team_id, name, score, active, weight) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        &[
            SqliteValue::Integer(51),
            SqliteValue::Integer(2),
            SqliteValue::from("fifty-one"),
            SqliteValue::Integer(7),
            SqliteValue::Integer(1),
            SqliteValue::Null,
        ],
        false,
        &mut diffs,
    )
    .await;
    step_query(
        f,
        r,
        &format!("{mode}/rowid_of_player_51"),
        "SELECT rowid, id FROM diff_players WHERE id = 51",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/delete_player_51"),
        "DELETE FROM diff_players WHERE id = 51",
        &mut diffs,
    )
    .await;

    // Multi-row FK violation: row 1 succeeds, row 2 fails FK
    step_exec(
        f,
        r,
        &format!("{mode}/multirow_fk_violation"),
        "INSERT INTO diff_players (id, team_id, name, score, active, weight) VALUES \
         (1001, 1, 'good', 1, 1, NULL), (1002, 42, 'bad', 2, 1, NULL)",
        &mut diffs,
    )
    .await;

    // UPSERT non-conflicting insert arm that fails FK (usesStmtJournal = 1 -> updates rowid)
    step_exec(
        f,
        r,
        &format!("{mode}/upsert_insert_fk_fail"),
        "INSERT INTO diff_players (id, team_id, name, score, active, weight) \
         VALUES (1003, 42, 'bad_upsert', 1, 1, NULL) \
         ON CONFLICT(id) DO UPDATE SET name = excluded.name",
        &mut diffs,
    )
    .await;

    // UPSERT DO NOTHING non-conflicting insert arm that fails FK (usesStmtJournal = 0 -> unchanged)
    step_exec(
        f,
        r,
        &format!("{mode}/upsert_do_nothing_fk_fail"),
        "INSERT INTO diff_players (id, team_id, name, score, active, weight) \
         VALUES (1005, 42, 'bad_do_nothing', 1, 1, NULL) ON CONFLICT DO NOTHING",
        &mut diffs,
    )
    .await;

    // INSERT OR REPLACE on parent team failing inbound FK
    step_exec(
        f,
        r,
        &format!("{mode}/replace_parent_fk_fail"),
        "INSERT OR REPLACE INTO diff_teams (id, name, motto) VALUES (1, 'crimson2', 'fk_blocked')",
        &mut diffs,
    )
    .await;

    // Explicit transaction FK failure (prepared + unprepared)
    step_exec(f, r, &format!("{mode}/tx_begin"), "BEGIN", &mut diffs).await;
    step_exec_params(
        f,
        r,
        &format!("{mode}/tx_fk_fail_prepared"),
        "INSERT INTO diff_players (id, team_id, name, score, active, weight) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        &[
            SqliteValue::Integer(1004),
            SqliteValue::Integer(42),
            SqliteValue::from("tx_orphan"),
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Null,
        ],
        true,
        &mut diffs,
    )
    .await;
    step_exec(f, r, &format!("{mode}/tx_rollback"), "ROLLBACK", &mut diffs).await;

    // INTEGER PRIMARY KEY (rowid alias) with FK, UNIQUE, CHECK, NOT NULL
    step_exec(
        f,
        r,
        &format!("{mode}/create_ipk"),
        "CREATE TABLE ipk (\
            id INTEGER PRIMARY KEY, \
            team_id BIGINT REFERENCES diff_teams(id), \
            u TEXT UNIQUE, \
            c INT CHECK(c > 0), \
            nn TEXT NOT NULL\
         )",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/ipk_ok"),
        "INSERT INTO ipk VALUES (100, 1, 'a', 1, 'x')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/ipk_dup_pk"),
        "INSERT INTO ipk VALUES (100, 1, 'b', 1, 'x')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/ipk_dup_unique"),
        "INSERT INTO ipk VALUES (200, 1, 'a', 1, 'x')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/ipk_fk_fail"),
        "INSERT INTO ipk VALUES (300, 999, 'c', 1, 'x')",
        &mut diffs,
    )
    .await;
    step_exec_params(
        f,
        r,
        &format!("{mode}/ipk_fk_fail_prepared"),
        "INSERT INTO ipk VALUES (?1, ?2, ?3, ?4, ?5)",
        &[
            SqliteValue::Integer(301),
            SqliteValue::Integer(999),
            SqliteValue::from("c2"),
            SqliteValue::Integer(1),
            SqliteValue::from("x"),
        ],
        true,
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/ipk_check_fail"),
        "INSERT INTO ipk VALUES (400, 1, 'd', -1, 'x')",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/ipk_notnull_fail"),
        "INSERT INTO ipk VALUES (401, 1, 'e', 1, NULL)",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/ipk_upsert_conflict"),
        "INSERT INTO ipk VALUES (100, 1, 'z', 1, 'x') ON CONFLICT(id) DO UPDATE SET u = 'updated'",
        &mut diffs,
    )
    .await;
    step_exec(
        f,
        r,
        &format!("{mode}/ipk_upsert_insert"),
        "INSERT INTO ipk VALUES (500, 1, 'f', 1, 'x') ON CONFLICT(id) DO UPDATE SET u = 'updated'",
        &mut diffs,
    )
    .await;

    assert!(
        diffs.is_empty(),
        "GH #542 sqlmodel/FK parity diffs ({mode}):\n{}",
        diffs.join("\n")
    );
}

#[test]
fn gh542_sqlmodel_differential_and_fk_matrix_memory_and_file() {
    asupersync::test_utils::run_test(|| async {
        let f_mem = Connection::open(":memory:").await.unwrap();
        let r_mem = rusqlite::Connection::open_in_memory().unwrap();
        run_sqlmodel_differential_and_fk_script(&f_mem, &r_mem, "memory").await;
        f_mem.close().await.unwrap();

        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("f_fk.db");
        let r_path = dir.path().join("r_fk.db");
        let f_file = Connection::open(f_path.to_str().unwrap()).await.unwrap();
        let r_file = rusqlite::Connection::open(&r_path).unwrap();
        run_sqlmodel_differential_and_fk_script(&f_file, &r_file, "file").await;
        f_file.close().await.unwrap();
    });
}

#[test]
fn gh542_fk_statement_journal_and_deferred_edge_cases() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();

        step_exec(&f, &r, "fk_on", "PRAGMA foreign_keys = ON", &mut diffs).await;
        step_exec(
            &f,
            &r,
            "create_p",
            "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY)",
            &mut diffs,
        )
        .await;
        step_exec(&f, &r, "ins_p", "INSERT INTO p VALUES (1)", &mut diffs).await;
        step_exec(
            &f,
            &r,
            "create_marker",
            "CREATE TABLE marker (id INTEGER PRIMARY KEY)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "create_c",
            "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES p(id))",
            &mut diffs,
        )
        .await;

        // Single-row conflict clauses: ABORT, ROLLBACK, FAIL, IGNORE preserve; REPLACE updates.
        for (tag, sql) in [
            ("or_abort", "INSERT OR ABORT INTO c VALUES (2, 999)"),
            ("or_rollback", "INSERT OR ROLLBACK INTO c VALUES (2, 999)"),
            ("or_fail", "INSERT OR FAIL INTO c VALUES (2, 999)"),
            ("or_ignore", "INSERT OR IGNORE INTO c VALUES (2, 999)"),
            ("or_replace", "INSERT OR REPLACE INTO c VALUES (2, 999)"),
            (
                "do_nothing",
                "INSERT INTO c VALUES (2, 999) ON CONFLICT DO NOTHING",
            ),
            (
                "do_nothing_target",
                "INSERT INTO c VALUES (2, 999) ON CONFLICT(id) DO NOTHING",
            ),
            (
                "do_update_insert_arm",
                "INSERT INTO c VALUES (2, 999) ON CONFLICT(id) DO UPDATE SET pid = excluded.pid",
            ),
            ("values_subquery", "INSERT INTO c VALUES (2, (SELECT 999))"),
            (
                "with_single_values",
                "WITH x(a) AS (VALUES(1)) INSERT INTO c VALUES (2, 999)",
            ),
            ("insert_select", "INSERT INTO c SELECT 2, 999"),
        ] {
            step_exec(
                &f,
                &r,
                &format!("mark_{tag}"),
                "INSERT INTO marker VALUES (NULL)",
                &mut diffs,
            )
            .await;
            step_exec(&f, &r, tag, sql, &mut diffs).await;
        }

        // RETURNING on FK failure updates rowid (usesStmtJournal = 1 in stock)
        step_exec(
            &f,
            &r,
            "mark_returning_fk",
            "INSERT INTO marker VALUES (200)",
            &mut diffs,
        )
        .await;
        step_query(
            &f,
            &r,
            "returning_fk_fail",
            "INSERT INTO c VALUES (2, 999) RETURNING id",
            &mut diffs,
        )
        .await;

        // DEFAULT VALUES FK failure preserves rowid
        step_exec(
            &f,
            &r,
            "create_c_def",
            "CREATE TABLE c_def (id BIGINT NOT NULL DEFAULT 10 PRIMARY KEY, pid BIGINT NOT NULL DEFAULT 999 REFERENCES p(id))",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "mark_def",
            "INSERT INTO marker VALUES (201)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "default_values_fk_fail",
            "INSERT INTO c_def DEFAULT VALUES",
            &mut diffs,
        )
        .await;

        // Self-referential single-row FK: valid self-ref succeeds, invalid preserves rowid
        step_exec(
            &f,
            &r,
            "create_node",
            "CREATE TABLE node (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES node(id))",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "node_root",
            "INSERT INTO node VALUES (1, NULL)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "node_self_ref_ok",
            "INSERT INTO node VALUES (2, 2)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "node_self_ref_bad",
            "INSERT INTO node VALUES (3, 999)",
            &mut diffs,
        )
        .await;

        // Schema-level ON CONFLICT REPLACE vs NOT NULL ON CONFLICT REPLACE vs statement OR ABORT override
        step_exec(
            &f,
            &r,
            "create_c_pk_rep",
            "CREATE TABLE c_pk_rep (id BIGINT NOT NULL PRIMARY KEY ON CONFLICT REPLACE, pid BIGINT NOT NULL REFERENCES p(id))",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "create_c_u_rep",
            "CREATE TABLE c_u_rep (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES p(id), u TEXT UNIQUE ON CONFLICT REPLACE)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "create_c_ipk_rep",
            "CREATE TABLE c_ipk_rep (id INTEGER PRIMARY KEY ON CONFLICT REPLACE, pid BIGINT NOT NULL REFERENCES p(id))",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "create_c_nn_rep",
            "CREATE TABLE c_nn_rep (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES p(id), v TEXT NOT NULL ON CONFLICT REPLACE DEFAULT 'd')",
            &mut diffs,
        )
        .await;

        for (tag, mark, sql) in [
            (
                "schema_pk_replace",
                210,
                "INSERT INTO c_pk_rep VALUES (2, 999)",
            ),
            (
                "schema_pk_replace_override_abort",
                211,
                "INSERT OR ABORT INTO c_pk_rep VALUES (2, 999)",
            ),
            (
                "schema_unique_replace",
                212,
                "INSERT INTO c_u_rep VALUES (2, 999, 'a')",
            ),
            (
                "schema_ipk_replace",
                213,
                "INSERT INTO c_ipk_rep VALUES (2, 999)",
            ),
            (
                "schema_ipk_replace_override_abort",
                214,
                "INSERT OR ABORT INTO c_ipk_rep VALUES (2, 999)",
            ),
            (
                "schema_notnull_replace",
                215,
                "INSERT INTO c_nn_rep VALUES (2, 999, NULL)",
            ),
        ] {
            step_exec(
                &f,
                &r,
                &format!("mark_{tag}"),
                &format!("INSERT INTO marker VALUES ({mark})"),
                &mut diffs,
            )
            .await;
            step_exec(&f, &r, tag, sql, &mut diffs).await;
        }

        // Deferred FK (declared first) + Immediate FK (declared second):
        // immediate FK takes precedence (preserves rowid); deferred-only in autocommit updates rowid.
        step_exec(
            &f,
            &r,
            "create_c_mixed_fk",
            "CREATE TABLE c_mixed_fk (\
                id BIGINT NOT NULL PRIMARY KEY, \
                pid_def BIGINT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED, \
                pid_imm BIGINT REFERENCES p(id)\
             )",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "mark_mixed_imm",
            "INSERT INTO marker VALUES (220)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "mixed_imm_only_fail",
            "INSERT INTO c_mixed_fk VALUES (1, 1, 999)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "mark_mixed_both",
            "INSERT INTO marker VALUES (221)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "mixed_both_fail_imm_wins",
            "INSERT INTO c_mixed_fk VALUES (2, 999, 999)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "mark_mixed_def",
            "INSERT INTO marker VALUES (222)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "mixed_def_only_autocommit_fail",
            "INSERT INTO c_mixed_fk VALUES (3, 999, 1)",
            &mut diffs,
        )
        .await;

        // Prepared statement prepared with foreign_keys=OFF, then executed after foreign_keys=ON
        step_exec(&f, &r, "fk_off_prep", "PRAGMA foreign_keys = OFF", &mut diffs).await;
        let f_stmt = f
            .prepare("INSERT INTO c VALUES (?1, ?2)")
            .await
            .expect("prepare with fk off");
        step_exec(&f, &r, "fk_on_prep", "PRAGMA foreign_keys = ON", &mut diffs).await;
        step_exec(
            &f,
            &r,
            "mark_prep_toggle",
            "INSERT INTO marker VALUES (230)",
            &mut diffs,
        )
        .await;
        let f_ok = f_stmt
            .execute_with_params(&[SqliteValue::Integer(50), SqliteValue::Integer(999)])
            .await
            .is_ok();
        let r_ok = r
            .execute("INSERT INTO c VALUES (?1, ?2)", [50_i64, 999_i64])
            .is_ok();
        let f_obs = f_counters(&f).await;
        let r_obs = r_counters(&r);
        if f_ok != r_ok || f_obs != r_obs {
            diffs.push(format!(
                "  [prep_before_fk_on] f_ok={f_ok} f={f_obs:?} vs r_ok={r_ok} r={r_obs:?}"
            ));
        }

        // Prepared statement prepared BEFORE CREATE TRIGGER, then re-executed after CREATE TRIGGER
        step_exec(
            &f,
            &r,
            "add_bi_trigger",
            "CREATE TRIGGER c_bi BEFORE INSERT ON c BEGIN SELECT 1; END",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "mark_prep_after_trigger",
            "INSERT INTO marker VALUES (231)",
            &mut diffs,
        )
        .await;
        let f_ok2 = f_stmt
            .execute_with_params(&[SqliteValue::Integer(51), SqliteValue::Integer(999)])
            .await
            .is_ok();
        let r_ok2 = r
            .execute("INSERT INTO c VALUES (?1, ?2)", [51_i64, 999_i64])
            .is_ok();
        let f_obs2 = f_counters(&f).await;
        let r_obs2 = r_counters(&r);
        if f_ok2 != r_ok2 || f_obs2 != r_obs2 {
            diffs.push(format!(
                "  [prep_after_create_trigger] f_ok={f_ok2} f={f_obs2:?} vs r_ok={r_ok2} r={r_obs2:?}"
            ));
        }

        // DO UPDATE on plain non-FK column preserves rowid; touching child FK,
        // parent FK, or firing BEFORE/AFTER UPDATE trigger updates rowid.
        step_exec(
            &f,
            &r,
            "create_c_du",
            "CREATE TABLE c_du (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES p(id), u TEXT UNIQUE, val TEXT NOT NULL CHECK(val != 'bad'))",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "create_gc_du",
            "CREATE TABLE gc_du (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT REFERENCES c_du(id))",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "create_c_du_trig",
            "CREATE TABLE c_du_trig (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES p(id), val TEXT)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "add_au_trigger",
            "CREATE TRIGGER c_du_au AFTER UPDATE ON c_du_trig BEGIN SELECT 1; END",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "create_c_du_col_trig",
            "CREATE TABLE c_du_col_trig (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES p(id), a TEXT, b TEXT)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "add_bu_of_a_trigger",
            "CREATE TRIGGER c_du_bu_a BEFORE UPDATE OF a ON c_du_col_trig BEGIN SELECT 1; END",
            &mut diffs,
        )
        .await;
        for (tag, mark, sql) in [
            (
                "du_plain_col",
                240,
                "INSERT INTO c_du VALUES (10, 999, 'a', 'x') ON CONFLICT(id) DO UPDATE SET val = excluded.val",
            ),
            (
                "du_unique_col",
                241,
                "INSERT INTO c_du VALUES (10, 999, 'a', 'x') ON CONFLICT(id) DO UPDATE SET u = excluded.u",
            ),
            (
                "du_child_fk_col",
                242,
                "INSERT INTO c_du VALUES (10, 999, 'a', 'x') ON CONFLICT(id) DO UPDATE SET pid = excluded.pid",
            ),
            (
                "du_parent_fk_col",
                243,
                "INSERT INTO c_du VALUES (10, 999, 'a', 'x') ON CONFLICT(id) DO UPDATE SET id = excluded.id",
            ),
            (
                "du_after_update_trigger",
                244,
                "INSERT INTO c_du_trig VALUES (10, 999, 'x') ON CONFLICT(id) DO UPDATE SET val = excluded.val",
            ),
            (
                "du_update_of_nonmatching_col",
                245,
                "INSERT INTO c_du_col_trig VALUES (10, 999, 'x', 'y') ON CONFLICT(id) DO UPDATE SET b = excluded.b",
            ),
            (
                "du_update_of_matching_col",
                246,
                "INSERT INTO c_du_col_trig VALUES (10, 999, 'x', 'y') ON CONFLICT(id) DO UPDATE SET a = excluded.a",
            ),
        ] {
            step_exec(
                &f,
                &r,
                &format!("mark_{tag}"),
                &format!("INSERT INTO marker VALUES ({mark})"),
                &mut diffs,
            )
            .await;
            step_exec(&f, &r, tag, sql, &mut diffs).await;
        }

        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "GH #542 FK statement-journal/deferred edge-case diffs:\n{}",
            diffs.join("\n")
        );
    });
}

#[test]
fn gh542_triggers_autoincrement_without_rowid_temp_parity() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();

        step_exec(&f, &r, "fk_on", "PRAGMA foreign_keys = ON", &mut diffs).await;
        step_exec(
            &f,
            &r,
            "create_parent",
            "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY)",
            &mut diffs,
        )
        .await;
        step_exec(&f, &r, "ins_p1", "INSERT INTO p VALUES (1)", &mut diffs).await;

        step_exec(
            &f,
            &r,
            "create_log",
            "CREATE TABLE tlog (id INTEGER PRIMARY KEY, msg TEXT)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "create_trigt",
            "CREATE TABLE trigt (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES p(id), val TEXT)",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "bi_abort",
            "CREATE TRIGGER bi_abort BEFORE INSERT ON trigt WHEN NEW.val = 'bi_abort' \
             BEGIN SELECT RAISE(ABORT, 'bi'); END",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "bi_ignore",
            "CREATE TRIGGER bi_ignore BEFORE INSERT ON trigt WHEN NEW.val = 'bi_ignore' \
             BEGIN SELECT RAISE(IGNORE); END",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "ai_abort",
            "CREATE TRIGGER ai_abort AFTER INSERT ON trigt WHEN NEW.val = 'ai_abort' \
             BEGIN SELECT RAISE(ABORT, 'ai'); END",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "ai_log",
            "CREATE TRIGGER ai_log AFTER INSERT ON trigt WHEN NEW.val = 'log' \
             BEGIN INSERT INTO tlog VALUES (999, NEW.val); END",
            &mut diffs,
        )
        .await;

        step_exec(
            &f,
            &r,
            "trigt_ok",
            "INSERT INTO trigt VALUES (1, 1, 'ok')",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "trigt_bi_abort",
            "INSERT INTO trigt VALUES (2, 1, 'bi_abort')",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "trigt_bi_ignore",
            "INSERT INTO trigt VALUES (3, 1, 'bi_ignore')",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "trigt_ai_abort",
            "INSERT INTO trigt VALUES (4, 1, 'ai_abort')",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "trigt_fk_fail_with_after_trigger",
            "INSERT INTO trigt VALUES (5, 999, 'log')",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "trigt_log_ok",
            "INSERT INTO trigt VALUES (6, 1, 'log')",
            &mut diffs,
        )
        .await;

        // AUTOINCREMENT & WITHOUT ROWID & TEMP
        step_exec(
            &f,
            &r,
            "create_ai",
            "CREATE TABLE ai (id INTEGER PRIMARY KEY AUTOINCREMENT, u TEXT UNIQUE)",
            &mut diffs,
        )
        .await;
        step_exec(&f, &r, "ai_1", "INSERT INTO ai(u) VALUES ('a')", &mut diffs).await;
        step_exec(&f, &r, "ai_dup", "INSERT INTO ai(u) VALUES ('a')", &mut diffs).await;
        step_exec(
            &f,
            &r,
            "create_wor",
            "CREATE TABLE wor (id INT PRIMARY KEY, v TEXT) WITHOUT ROWID",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "wor_ok",
            "INSERT INTO wor VALUES (42, 'x')",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "wor_dup",
            "INSERT INTO wor VALUES (42, 'y')",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "create_temp",
            "CREATE TEMP TABLE tmp (id BIGINT NOT NULL, val TEXT, PRIMARY KEY (id))",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "tmp_ok",
            "INSERT INTO tmp VALUES (1, 'a'), (2, 'b')",
            &mut diffs,
        )
        .await;
        step_exec(
            &f,
            &r,
            "tmp_dup",
            "INSERT INTO tmp VALUES (1, 'dup')",
            &mut diffs,
        )
        .await;

        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "GH #542 triggers/autoinc/without-rowid/temp parity diffs:\n{}",
            diffs.join("\n")
        );
    });
}



#[test]
fn gh542_edge1_temp_parent_child_and_shadowing() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        for (label, sql) in [
            ("fk_on", "PRAGMA foreign_keys = ON"),
            ("create_main_p", "CREATE TABLE p (id INTEGER PRIMARY KEY, k INT UNIQUE)"),
            ("create_main_c", "CREATE TABLE c (id INTEGER PRIMARY KEY, pk INT REFERENCES p(k))"),
            ("seed_main_p", "INSERT INTO p VALUES (1, 10)"),
            ("seed_main_c", "INSERT INTO c VALUES (50, 10)"),
            // TEMP parent + TEMP child referencing TEMP parent(k)
            ("create_tp", "CREATE TEMP TABLE tp (id INTEGER PRIMARY KEY, k INT UNIQUE)"),
            ("create_tc", "CREATE TEMP TABLE tc (id INTEGER PRIMARY KEY, pk INT REFERENCES tp(k))"),
            ("seed_tp", "INSERT INTO tp VALUES (1, 100)"),
            ("seed_tc", "INSERT INTO tc VALUES (70, 100)"),
            // Single-row immediate FK failure on TEMP child
            ("tc_imm_fail", "INSERT INTO tc(pk) VALUES (999)"),
            // Upsert on TEMP parent modifying referenced key k (child tc references 100)
            (
                "tp_upsert_mod_pk",
                "INSERT INTO tp(id, k) VALUES (1, 100) ON CONFLICT(id) DO UPDATE SET k = 200",
            ),
            // TEMP table `p` shadowing `main.p`, with NO temp child referencing it,
            // plus an immediate FK to `tp(k)` so we can observe failing insert behavior.
            (
                "create_shadow_p",
                "CREATE TEMP TABLE p (id INTEGER PRIMARY KEY, k INT UNIQUE, tpk INT REFERENCES tp(k))",
            ),
            ("seed_shadow_p", "INSERT INTO p VALUES (1, 10, 100)"),
            // Failing insert into TEMP `p` with DO UPDATE SET k = 99 (main.c references main.p(k), NOT temp.p(k))
            (
                "shadow_p_upsert_fk_fail",
                "INSERT INTO p(id, k, tpk) VALUES (2, 20, 999) ON CONFLICT(id) DO UPDATE SET k = 99",
            ),
            // Explicit main.p upsert modifying referenced parent key k (main.c references main.p(k))
            (
                "main_p_upsert_mod_pk",
                "INSERT INTO main.p(id, k) VALUES (1, 10) ON CONFLICT(id) DO UPDATE SET k = 20",
            ),
            // A BEFORE INSERT trigger on main.p must not change TEMP p's immediate-FK preservation
            (
                "main_p_before_trig",
                "CREATE TRIGGER tr_main_p BEFORE INSERT ON main.p BEGIN SELECT 1; END",
            ),
            (
                "shadow_p_imm_fk_fail_with_main_trig",
                "INSERT INTO p(id, k, tpk) VALUES (3, 30, 999)",
            ),
        ] {
            step_exec(&f, &r, label, sql, &mut diffs).await;
        }
        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "Edge 1 (TEMP parent/child + shadowing) diffs:\n{}",
            diffs.join("\n")
        );
    });
}

#[test]
fn gh542_edge2_composite_pk_not_null_check_on_conflict_replace() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        for (label, sql) in [
            ("fk_on", "PRAGMA foreign_keys = ON"),
            ("create_p", "CREATE TABLE p (id INTEGER PRIMARY KEY)"),
            ("seed_p", "INSERT INTO p VALUES (1)"),
            (
                "create_comp_pk",
                "CREATE TABLE c_pk (a INT, b INT, pid INT REFERENCES p(id), PRIMARY KEY (a, b) ON CONFLICT REPLACE)",
            ),
            ("seed_comp_pk", "INSERT INTO c_pk VALUES (1, 1, 1)"),
            ("comp_pk_fk_fail_new", "INSERT INTO c_pk VALUES (2, 2, 999)"),
            ("comp_pk_fk_fail_dup", "INSERT INTO c_pk VALUES (1, 1, 999)"),
            (
                "create_nn_rep",
                "CREATE TABLE c_nn (id INTEGER PRIMARY KEY, x INT NOT NULL ON CONFLICT REPLACE DEFAULT 7, pid INT REFERENCES p(id))",
            ),
            ("seed_nn", "INSERT INTO c_nn(id, x, pid) VALUES (10, 5, 1)"),
            ("nn_fk_fail_nonnull", "INSERT INTO c_nn(x, pid) VALUES (5, 999)"),
            ("nn_fk_fail_null", "INSERT INTO c_nn(x, pid) VALUES (NULL, 999)"),
            (
                "create_chk",
                "CREATE TABLE c_chk (id INTEGER PRIMARY KEY, x INT CHECK(x > 0), pid INT REFERENCES p(id))",
            ),
            ("seed_chk", "INSERT INTO c_chk(id, x, pid) VALUES (20, 5, 1)"),
            ("chk_fk_fail_valid_x", "INSERT INTO c_chk(x, pid) VALUES (5, 999)"),
            ("chk_fk_fail_bad_x", "INSERT INTO c_chk(x, pid) VALUES (-1, 999)"),
        ] {
            step_exec(&f, &r, label, sql, &mut diffs).await;
        }
        for tbl in ["c_pk", "c_nn", "c_chk"] {
            let q = format!("SELECT count(*) FROM {tbl}");
            assert_eq!(
                f_query(&f, &q).await,
                Ok(vec![vec!["int:1".to_owned()]]),
                "table {tbl} must exist with 1 seeded row"
            );
            assert_eq!(r_query(&r, &q), Ok(vec![vec!["int:1".to_owned()]]));
        }
        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "Edge 2 (composite PK / NOT NULL / CHECK ON CONFLICT REPLACE) diffs:\n{}",
            diffs.join("\n")
        );
    });
}

#[test]
fn gh542_edge3_cte_and_scalar_subquery_values() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        for (label, sql) in [
            ("fk_on", "PRAGMA foreign_keys = ON"),
            ("create_p", "CREATE TABLE p (id INTEGER PRIMARY KEY)"),
            ("create_c", "CREATE TABLE c (id INTEGER PRIMARY KEY, v INT, pid INT REFERENCES p(id))"),
            ("seed_p", "INSERT INTO p VALUES (1)"),
            ("seed_c", "INSERT INTO c VALUES (10, 1, 1)"),
            (
                "cte_values_subquery_v",
                "WITH cte AS (SELECT 1) INSERT INTO c(v, pid) VALUES ((SELECT * FROM cte), 999)",
            ),
            (
                "cte_values_subquery_pid",
                "WITH cte AS (SELECT 999) INSERT INTO c(v, pid) VALUES (1, (SELECT * FROM cte))",
            ),
            (
                "scalar_subquery_values",
                "INSERT INTO c(v, pid) VALUES (1, (SELECT 999))",
            ),
            (
                "cte_insert_select",
                "WITH cte AS (SELECT 1 AS v, 999 AS pid) INSERT INTO c(v, pid) SELECT v, pid FROM cte",
            ),
        ] {
            step_exec(&f, &r, label, sql, &mut diffs).await;
        }
        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "Edge 3 (CTE + scalar subquery VALUES) diffs:\n{}",
            diffs.join("\n")
        );
    });
}

#[test]
fn gh542_edge4_pragma_defer_foreign_keys_autocommit_and_txn() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        for (label, sql) in [
            ("fk_on", "PRAGMA foreign_keys = ON"),
            ("create_p", "CREATE TABLE p (id INTEGER PRIMARY KEY)"),
            ("create_c", "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT REFERENCES p(id))"),
            ("seed_p", "INSERT INTO p VALUES (1)"),
            ("seed_c", "INSERT INTO c VALUES (10, 1)"),
            // Autocommit with defer_foreign_keys = ON
            ("defer_on_auto", "PRAGMA defer_foreign_keys = ON"),
            ("auto_fk_fail_with_defer", "INSERT INTO c(pid) VALUES (999)"),
            ("auto_fk_fail_after_first", "INSERT INTO c(pid) VALUES (999)"),
            // Reset baseline and test inside BEGIN transaction
            ("seed_c_2", "INSERT INTO c VALUES (20, 1)"),
            ("begin_txn", "BEGIN"),
            ("defer_on_txn", "PRAGMA defer_foreign_keys = ON"),
            ("txn_fk_insert", "INSERT INTO c(pid) VALUES (999)"),
            ("txn_commit_fail", "COMMIT"),
            ("txn_rollback", "ROLLBACK"),
            // After rollback, defer_foreign_keys should auto-reset to OFF
            ("post_rollback_imm_fail", "INSERT INTO c(pid) VALUES (999)"),
        ] {
            step_exec(&f, &r, label, sql, &mut diffs).await;
        }
        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "Edge 4 (PRAGMA defer_foreign_keys autocommit vs txn) diffs:\n{}",
            diffs.join("\n")
        );
    });
}

#[test]
fn gh542_edge5_prepared_statement_across_create_drop_trigger() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        for (label, sql) in [
            ("fk_on", "PRAGMA foreign_keys = ON"),
            ("create_p", "CREATE TABLE p (id INTEGER PRIMARY KEY)"),
            ("create_c", "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT REFERENCES p(id))"),
            ("seed_p", "INSERT INTO p VALUES (1)"),
            ("seed_c", "INSERT INTO c VALUES (10, 1)"),
        ] {
            step_exec(&f, &r, label, sql, &mut diffs).await;
        }

        let sql = "INSERT INTO c(pid) VALUES (?1)";
        let f_stmt = f.prepare(sql).await.expect("fsqlite prepare");
        let mut r_stmt = r.prepare(sql).expect("rusqlite prepare");

        let exec_pair = async |f_conn: &Connection,
                               r_conn: &rusqlite::Connection,
                               f_s: &fsqlite_core::connection::PreparedStatement,
                               r_s: &mut rusqlite::Statement<'_>,
                               label: &str,
                               pid: i64,
                               diffs: &mut Vec<String>| {
            let f_ok = f_s
                .execute_with_params(&[SqliteValue::Integer(pid)])
                .await
                .is_ok();
            let r_ok = r_s.execute(rusqlite::params![pid]).is_ok();
            let (f_sql_lir, f_api_lir, f_chg, f_tot) = f_counters(f_conn).await;
            let (r_sql_lir, r_api_lir, r_chg, r_tot) = r_counters(r_conn);
            let f_obs = StepObs {
                ok: f_ok,
                sql_last_insert_rowid: f_sql_lir,
                api_last_insert_rowid: f_api_lir,
                changes: f_chg,
                total_changes: f_tot,
            };
            let r_obs = StepObs {
                ok: r_ok,
                sql_last_insert_rowid: r_sql_lir,
                api_last_insert_rowid: r_api_lir,
                changes: r_chg,
                total_changes: r_tot,
            };
            if f_obs != r_obs {
                diffs.push(format!(
                    "  [{label}] prepared pid={pid}\n    frank={f_obs:?}\n    stock={r_obs:?}"
                ));
            }
        };

        exec_pair(&f, &r, &f_stmt, &mut r_stmt, "prep_before_trig", 999, &mut diffs).await;

        step_exec(
            &f,
            &r,
            "add_before_trig",
            "CREATE TRIGGER tr_before BEFORE INSERT ON c BEGIN SELECT 1; END",
            &mut diffs,
        )
        .await;
        exec_pair(&f, &r, &f_stmt, &mut r_stmt, "prep_with_before_trig", 999, &mut diffs).await;

        step_exec(&f, &r, "drop_before_trig", "DROP TRIGGER tr_before", &mut diffs).await;
        step_exec(&f, &r, "reseed_c", "INSERT INTO c VALUES (20, 1)", &mut diffs).await;
        step_exec(
            &f,
            &r,
            "add_after_trig",
            "CREATE TRIGGER tr_after AFTER INSERT ON c BEGIN SELECT 1; END",
            &mut diffs,
        )
        .await;
        exec_pair(&f, &r, &f_stmt, &mut r_stmt, "prep_with_after_trig", 999, &mut diffs).await;

        step_exec(&f, &r, "drop_after_trig", "DROP TRIGGER tr_after", &mut diffs).await;
        step_exec(&f, &r, "reseed_c_2", "INSERT INTO c VALUES (30, 1)", &mut diffs).await;
        exec_pair(&f, &r, &f_stmt, &mut r_stmt, "prep_after_drop_trig", 999, &mut diffs).await;

        drop(r_stmt);
        drop(f_stmt);
        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "Edge 5 (prepared stmt across CREATE/DROP TRIGGER) diffs:\n{}",
            diffs.join("\n")
        );
    });
}

#[test]
fn gh542_edge6_insert_or_conflict_clauses() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        for (label, sql) in [
            ("fk_on", "PRAGMA foreign_keys = ON"),
            ("create_p", "CREATE TABLE p (id INTEGER PRIMARY KEY)"),
            ("create_c", "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT REFERENCES p(id))"),
            (
                "create_crepl",
                "CREATE TABLE crepl (id INTEGER PRIMARY KEY, u INT UNIQUE ON CONFLICT REPLACE, pid INT REFERENCES p(id))",
            ),
            ("seed_p", "INSERT INTO p VALUES (1)"),
            ("seed_c", "INSERT INTO c VALUES (10, 1)"),
            ("auto_or_ignore", "INSERT OR IGNORE INTO c(pid) VALUES (999)"),
            ("auto_or_fail", "INSERT OR FAIL INTO c(pid) VALUES (999)"),
            ("auto_or_rollback", "INSERT OR ROLLBACK INTO c(pid) VALUES (999)"),
            ("auto_or_abort", "INSERT OR ABORT INTO c(pid) VALUES (999)"),
            // Explicit OR ABORT on a table with UNIQUE ON CONFLICT REPLACE
            ("seed_crepl", "INSERT INTO crepl VALUES (15, 1, 1)"),
            ("crepl_plain_fk_fail", "INSERT INTO crepl(u, pid) VALUES (2, 999)"),
            ("seed_crepl_2", "INSERT INTO crepl VALUES (16, 3, 1)"),
            ("crepl_or_abort_fk_fail", "INSERT OR ABORT INTO crepl(u, pid) VALUES (4, 999)"),
            // Inside explicit transaction
            ("txn_begin_1", "BEGIN"),
            ("txn_seed", "INSERT INTO c VALUES (20, 1)"),
            ("txn_or_ignore", "INSERT OR IGNORE INTO c(pid) VALUES (999)"),
            ("txn_or_fail", "INSERT OR FAIL INTO c(pid) VALUES (999)"),
            ("txn_or_abort", "INSERT OR ABORT INTO c(pid) VALUES (999)"),
            ("txn_or_rollback", "INSERT OR ROLLBACK INTO c(pid) VALUES (999)"),
            // Verify whether the transaction is still open or was rolled back by INSERT OR ROLLBACK
            ("post_or_rollback_rollback", "ROLLBACK"),
        ] {
            step_exec(&f, &r, label, sql, &mut diffs).await;
        }
        let fq = f_query(&f, "SELECT id FROM c ORDER BY id").await;
        let rq = r_query(&r, "SELECT id FROM c ORDER BY id");
        assert_eq!(fq, rq, "rows after INSERT OR ROLLBACK must match stock");
        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "Edge 6 (INSERT OR IGNORE/FAIL/ROLLBACK/ABORT) diffs:\n{}",
            diffs.join("\n")
        );
    });
}

#[test]
fn gh542_edge7_mixed_deferred_and_immediate_fks() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        for (label, sql) in [
            ("fk_on", "PRAGMA foreign_keys = ON"),
            ("create_p1", "CREATE TABLE p1 (id INTEGER PRIMARY KEY)"),
            ("create_p2", "CREATE TABLE p2 (id INTEGER PRIMARY KEY)"),
            ("seed_p1", "INSERT INTO p1 VALUES (1)"),
            ("seed_p2", "INSERT INTO p2 VALUES (1)"),
            (
                "create_def_first",
                "CREATE TABLE m_df (id INTEGER PRIMARY KEY, d INT REFERENCES p1(id) DEFERRABLE INITIALLY DEFERRED, i INT REFERENCES p2(id))",
            ),
            (
                "create_imm_first",
                "CREATE TABLE m_if (id INTEGER PRIMARY KEY, i INT REFERENCES p2(id), d INT REFERENCES p1(id) DEFERRABLE INITIALLY DEFERRED)",
            ),
            ("seed_df", "INSERT INTO m_df VALUES (10, 1, 1)"),
            ("df_both_fail_auto", "INSERT INTO m_df(d, i) VALUES (999, 999)"),
            ("df_imm_only_auto", "INSERT INTO m_df(d, i) VALUES (1, 999)"),
            ("df_def_only_auto", "INSERT INTO m_df(d, i) VALUES (999, 1)"),
            ("seed_if", "INSERT INTO m_if VALUES (30, 1, 1)"),
            ("if_both_fail_auto", "INSERT INTO m_if(i, d) VALUES (999, 999)"),
            ("if_imm_only_auto", "INSERT INTO m_if(i, d) VALUES (999, 1)"),
            ("if_def_only_auto", "INSERT INTO m_if(i, d) VALUES (1, 999)"),
            ("txn_begin", "BEGIN"),
            ("txn_seed_df", "INSERT INTO m_df VALUES (50, 1, 1)"),
            ("txn_df_both_fail", "INSERT INTO m_df(d, i) VALUES (999, 999)"),
            ("txn_df_imm_only", "INSERT INTO m_df(d, i) VALUES (1, 999)"),
            ("txn_df_def_only", "INSERT INTO m_df(d, i) VALUES (999, 1)"),
            ("txn_commit_fail", "COMMIT"),
            ("txn_rollback", "ROLLBACK"),
        ] {
            step_exec(&f, &r, label, sql, &mut diffs).await;
        }
        f.close().await.unwrap();
        assert!(
            diffs.is_empty(),
            "Edge 7 (mixed deferred + immediate FKs) diffs:\n{}",
            diffs.join("\n")
        );
    });
}
