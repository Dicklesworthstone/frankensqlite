//! bd-q3hu3: `UPDATE ... SET a=?, b=? WHERE k1=? AND k2=?` on a composite-PK
//! table matched 0 rows when the WHERE clause carries 2+ bound parameters.
//!
//! Reproduction (from mcp_agent_mail_rust, fsqlite 0.3.4): the SET clause
//! consumes placeholder slots ?1/?2, so the WHERE-clause equality probes read
//! slots ?3/?4. If the UPDATE WHERE seek reads the wrong slot (e.g. the SET
//! values 200/100 instead of the key values 1/2), it seeks a non-existent key
//! and silently matches nothing. SELECT with the same params works; a fully
//! literal WHERE works; a single-param WHERE works — only 2+ params in the
//! UPDATE's WHERE regress. Verified against C SQLite via prepared execution.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
fn to_rusqlite(v: &SqliteValue) -> rusqlite::types::Value {
    match v {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}
async fn frank_state(c: &Connection, sql: &str, ncols: usize) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c
        .query(sql)
        .await
        .unwrap()
        .iter()
        .map(|row| row.values().iter().take(ncols).map(render).collect())
        .collect();
    r.sort();
    r
}
fn sqlite_state(c: &rusqlite::Connection, sql: &str, ncols: usize) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let mut r: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..ncols)
                .map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(x) => x.to_string(),
                    rusqlite::types::Value::Real(f) => format!("{f:?}"),
                    rusqlite::types::Value::Text(s) => format!("'{s}'"),
                    rusqlite::types::Value::Blob(b) => format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    ),
                })
                .collect::<Vec<_>>())
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    r.sort();
    r
}

/// Run a parameterized DML on both engines, assert equal rows-affected AND
/// equal resulting table state.
async fn check(
    ddl: &[&str],
    seed: &[&str],
    dml: &str,
    params: &[SqliteValue],
    select_state: &str,
    ncols: usize,
) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ddl.iter().chain(seed.iter()) {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    let f_affected = f
        .execute_with_params(dml, params)
        .await
        .unwrap_or_else(|e| panic!("frank `{dml}` params {params:?}: {e}"));
    let rparams: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
    let r_affected = r
        .execute(dml, rusqlite::params_from_iter(rparams.iter()))
        .unwrap();
    assert_eq!(
        f_affected, r_affected,
        "rows-affected diverged after `{dml}` params {params:?}: frank={f_affected} sqlite={r_affected}"
    );
    assert_eq!(
        frank_state(&f, select_state, ncols).await,
        sqlite_state(&r, select_state, ncols),
        "table state diverged after `{dml}` params {params:?}"
    );
}

const MR_DDL: &[&str] = &["CREATE TABLE message_recipients (
        message_id INTEGER NOT NULL,
        agent_id   INTEGER NOT NULL,
        read_ts    INTEGER,
        ack_ts     INTEGER,
        PRIMARY KEY (message_id, agent_id)
    );"];
const MR_SEED: &[&str] = &[
    "INSERT INTO message_recipients VALUES (1, 2, NULL, NULL);",
    "INSERT INTO message_recipients VALUES (1, 3, NULL, NULL);",
    "INSERT INTO message_recipients VALUES (5, 2, NULL, NULL);",
];
const MR_SELECT: &str = "SELECT message_id, agent_id, read_ts, ack_ts FROM message_recipients";

#[test]
fn update_two_where_params_composite_pk_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // The exact mcp_agent_mail_rust shape: 2 SET params (slots 1,2) then
        // 2 WHERE params (slots 3,4). Binds [200,100,1,2] -> must match (1,2).
        check(
            MR_DDL,
            MR_SEED,
            "UPDATE message_recipients SET read_ts=?, ack_ts=? WHERE message_id=? AND agent_id=?",
            &[
                SqliteValue::Integer(200),
                SqliteValue::Integer(100),
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
            ],
            MR_SELECT,
            4,
        )
        .await;
    });
}

#[test]
fn update_two_where_params_numbered_placeholders_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // Same shape with explicit numbered placeholders.
        check(
            MR_DDL,
            MR_SEED,
            "UPDATE message_recipients SET read_ts=?1, ack_ts=?2 WHERE message_id=?3 AND agent_id=?4",
            &[
                SqliteValue::Integer(200),
                SqliteValue::Integer(100),
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
            ],
            MR_SELECT,
            4,
        )
        .await;
    });
}

#[test]
fn update_two_where_params_no_set_params_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // Literal SET, 2 WHERE params in slots 1,2 (no SET offset). Isolates
        // whether the bug is the offset or the two-EQ-key seek itself.
        check(
            MR_DDL,
            MR_SEED,
            "UPDATE message_recipients SET read_ts=200, ack_ts=100 WHERE message_id=? AND agent_id=?",
            &[SqliteValue::Integer(1), SqliteValue::Integer(2)],
            MR_SELECT,
            4,
        )
        .await;
    });
}

#[test]
fn update_two_where_params_no_pk_index_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // Same 2-SET + 2-WHERE-param shape but on a plain (rowid) table with no
        // composite PK / index — forces the full-scan WHERE path.
        check(
            &["CREATE TABLE mr2 (message_id INTEGER, agent_id INTEGER, read_ts INTEGER, ack_ts INTEGER);"],
            &[
                "INSERT INTO mr2 VALUES (1, 2, NULL, NULL);",
                "INSERT INTO mr2 VALUES (1, 3, NULL, NULL);",
                "INSERT INTO mr2 VALUES (5, 2, NULL, NULL);",
            ],
            "UPDATE mr2 SET read_ts=?, ack_ts=? WHERE message_id=? AND agent_id=?",
            &[
                SqliteValue::Integer(200),
                SqliteValue::Integer(100),
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
            ],
            "SELECT message_id, agent_id, read_ts, ack_ts FROM mr2",
            4,
        )
        .await;
    });
}

#[test]
fn delete_two_where_params_composite_pk_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // DELETE with 2 WHERE params (no SET offset) — control that the
        // two-EQ-key seek is correct on its own.
        check(
            MR_DDL,
            MR_SEED,
            "DELETE FROM message_recipients WHERE message_id=? AND agent_id=?",
            &[SqliteValue::Integer(1), SqliteValue::Integer(2)],
            MR_SELECT,
            4,
        )
        .await;
    });
}

// ── WITHOUT ROWID composite PK ──────────────────────────────────────────
// The mcp_agent_mail_rust schema declares its composite-PK tables WITHOUT
// ROWID, which routes UPDATE through `codegen_update_without_rowid` — a
// distinct two-pass path. Pass 1 (WHERE, emitted first) and Pass 2 (SET,
// emitted second) must still number anon placeholders in SQL-text order
// (SET first). This is the actual regressed path in bd-q3hu3.
const MRW_DDL: &[&str] = &["CREATE TABLE mrw (
        message_id INTEGER NOT NULL,
        agent_id   INTEGER NOT NULL,
        read_ts    INTEGER,
        ack_ts     INTEGER,
        PRIMARY KEY (message_id, agent_id)
    ) WITHOUT ROWID;"];
const MRW_SEED: &[&str] = &[
    "INSERT INTO mrw VALUES (1, 2, NULL, NULL);",
    "INSERT INTO mrw VALUES (1, 3, NULL, NULL);",
    "INSERT INTO mrw VALUES (5, 2, NULL, NULL);",
];
const MRW_SELECT: &str = "SELECT message_id, agent_id, read_ts, ack_ts FROM mrw";

#[test]
fn update_without_rowid_two_set_two_where_params_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // 2 SET params (slots 1,2) then 2 WHERE params (slots 3,4). Binds
        // [200,100,1,2] -> must match (1,2). The bug numbered WHERE as slots
        // 1,2 (reading 200,100) and seeked a non-existent key -> 0 rows.
        check(
            MRW_DDL,
            MRW_SEED,
            "UPDATE mrw SET read_ts=?, ack_ts=? WHERE message_id=? AND agent_id=?",
            &[
                SqliteValue::Integer(200),
                SqliteValue::Integer(100),
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
            ],
            MRW_SELECT,
            4,
        )
        .await;
    });
}

#[test]
fn update_without_rowid_numbered_placeholders_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        check(
            MRW_DDL,
            MRW_SEED,
            "UPDATE mrw SET read_ts=?1, ack_ts=?2 WHERE message_id=?3 AND agent_id=?4",
            &[
                SqliteValue::Integer(200),
                SqliteValue::Integer(100),
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
            ],
            MRW_SELECT,
            4,
        )
        .await;
    });
}

#[test]
fn update_without_rowid_one_set_one_where_param_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // 1 SET param (slot 1) + 1 WHERE param (slot 2): the minimal offset
        // that still crosses the Pass-1/Pass-2 boundary.
        check(
            MRW_DDL,
            MRW_SEED,
            "UPDATE mrw SET read_ts=? WHERE message_id=5 AND agent_id=?",
            &[SqliteValue::Integer(200), SqliteValue::Integer(2)],
            MRW_SELECT,
            4,
        )
        .await;
    });
}

#[test]
fn update_without_rowid_literal_set_two_where_params_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // Literal SET (no offset), 2 WHERE params in slots 1,2 — the control
        // that stays correct even with the bug (no Pass-2 placeholders).
        check(
            MRW_DDL,
            MRW_SEED,
            "UPDATE mrw SET read_ts=200, ack_ts=100 WHERE message_id=? AND agent_id=?",
            &[SqliteValue::Integer(1), SqliteValue::Integer(2)],
            MRW_SELECT,
            4,
        )
        .await;
    });
}

#[test]
fn delete_without_rowid_two_where_params_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        // DELETE WITHOUT ROWID (no SET) — must remain byte-identical/correct.
        check(
            MRW_DDL,
            MRW_SEED,
            "DELETE FROM mrw WHERE message_id=? AND agent_id=?",
            &[SqliteValue::Integer(1), SqliteValue::Integer(2)],
            MRW_SELECT,
            4,
        )
        .await;
    });
}
