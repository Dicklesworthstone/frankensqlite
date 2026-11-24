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
//!
//! GH#541 also covers a TEXT PRIMARY KEY after another bound WHERE term:
//! the index probe must keep the key's SQL-order slot even when emitted first.
//! The same holds for an INTEGER PRIMARY KEY or `rowid` term, whose rowid
//! seek is emitted first too.
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

const TEXT_PK_STATE: &str = "SELECT k, a, v FROM t";

fn text_params(values: &[&str]) -> Vec<SqliteValue> {
    values
        .iter()
        .map(|value| SqliteValue::Text((*value).into()))
        .collect()
}

async fn text_pk_databases(wal: bool) -> (tempfile::TempDir, Connection, rusqlite::Connection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("frank.db");
    let f = Connection::open(if wal {
        path.to_str().unwrap()
    } else {
        ":memory:"
    })
    .await
    .unwrap();
    let r = if wal {
        rusqlite::Connection::open(dir.path().join("stock.db")).unwrap()
    } else {
        rusqlite::Connection::open_in_memory().unwrap()
    };
    if wal {
        assert_eq!(
            f.query_row("PRAGMA journal_mode = WAL")
                .await
                .unwrap()
                .values(),
            &[SqliteValue::Text("wal".into())]
        );
        assert_eq!(
            r.query_row("PRAGMA journal_mode = WAL", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "wal"
        );
    }
    let ddl = "CREATE TABLE t (k TEXT PRIMARY KEY, a TEXT NOT NULL, v TEXT)";
    f.execute(ddl).await.unwrap();
    r.execute_batch(ddl).unwrap();
    (dir, f, r)
}

async fn reset_text_pk(f: &Connection, r: &rusqlite::Connection) {
    // Wrong-slot values are real keys too, so comparing the whole table also
    // catches writes to decoys rather than only detecting missing writes.
    for sql in [
        "DELETE FROM t",
        "INSERT INTO t VALUES ('k1','a1','old'), ('k2','a2','old'), \
         ('a1','a1','old'), ('a2','a2','old'), ('x','a1','old'), ('old','a1','old')",
    ] {
        f.execute(sql).await.unwrap();
        r.execute_batch(sql).unwrap();
    }
}

async fn assert_where_count(
    f: &Connection,
    r: &rusqlite::Connection,
    predicate: &str,
    params: &[SqliteValue],
    expected: usize,
) {
    let sql = format!("SELECT count(*) FROM t WHERE {predicate}");
    let rparams: Vec<_> = params.iter().map(to_rusqlite).collect();
    let stock: i64 = r
        .query_row(&sql, rusqlite::params_from_iter(&rparams), |row| row.get(0))
        .unwrap();
    assert_eq!(stock, expected as i64, "stock count: {sql}, {params:?}");
    assert_eq!(
        f.query_row_with_params(&sql, params)
            .await
            .unwrap()
            .values(),
        &[SqliteValue::Integer(stock)],
        "read count: {sql}, {params:?}"
    );
}

async fn assert_write(
    f: &Connection,
    r: &rusqlite::Connection,
    state: &str,
    sql: &str,
    params: &[SqliteValue],
    expected: usize,
    prepared: Option<&fsqlite::PreparedStatement<'_>>,
) {
    let affected = if let Some(statement) = prepared {
        statement.execute_with_params(params).await
    } else {
        f.execute_with_params(sql, params).await
    }
    .unwrap_or_else(|error| panic!("{sql}, {params:?}: {error}"));
    let rparams: Vec<_> = params.iter().map(to_rusqlite).collect();
    assert_eq!(
        r.execute(sql, rusqlite::params_from_iter(&rparams))
            .unwrap(),
        expected,
        "stock write: {sql}, {params:?}"
    );
    assert_eq!(affected, expected, "write count: {sql}, {params:?}");
    assert_eq!(
        frank_state(f, state, 3).await,
        sqlite_state(r, state, 3),
        "table state: {sql}, {params:?}"
    );
}

async fn text_pk_write_matrix(wal: bool) {
    let (dir, f, r) = text_pk_databases(wal).await;
    let predicates: &[(&str, &[&str])] = &[
        ("a = ? AND k = ?", &["a1", "k1"]),
        ("k = ? AND a = ?", &["k1", "a1"]),
        ("k = ? AND a = ? AND v = ?", &["k1", "a1", "old"]),
        ("a = ? AND k = ? AND v = ?", &["a1", "k1", "old"]),
        ("a = ? AND v = ? AND k = ?", &["a1", "old", "k1"]),
        ("a = :a AND k = :k AND a = :a", &["a1", "k1"]),
        ("k = ?", &["k1"]),
        ("a = 'a1' AND k = 'k1'", &[]),
    ];
    for (predicate, values) in predicates {
        let where_params = text_params(values);
        for (prefix, bind_set) in [
            ("UPDATE t SET v = ?", true),
            ("UPDATE t SET v = 'x'", false),
            ("DELETE FROM t", false),
        ] {
            let sql = format!("{prefix} WHERE {predicate}");
            let mut params = if bind_set {
                text_params(&["x"])
            } else {
                Vec::new()
            };
            params.extend_from_slice(&where_params);
            for use_prepared in [false, true] {
                reset_text_pk(&f, &r).await;
                assert_where_count(&f, &r, predicate, &where_params, 1).await;
                let prepared = if use_prepared {
                    Some(f.prepare(&sql).await.unwrap())
                } else {
                    None
                };
                assert_write(&f, &r, TEXT_PK_STATE, &sql, &params, 1, prepared.as_ref()).await;
            }
        }
    }

    // Numbered gaps and named reuse must survive too. The SELECT uses its
    // own SQL-order bindings; an UPDATE's SET may precede its WHERE slots.
    let explicit_cases: &[(&str, &[&str], &str, &[&str])] = &[
        (
            "UPDATE t SET v = ?1 WHERE a = ?2 AND k = ?3",
            &["x", "a1", "k1"],
            "a = ? AND k = ?",
            &["a1", "k1"],
        ),
        (
            "UPDATE t SET v = ?1 WHERE k = ?2 AND a = ?3 AND v = ?4",
            &["x", "k1", "a1", "old"],
            "k = ?1 AND a = ?2 AND v = ?3",
            &["k1", "a1", "old"],
        ),
        (
            "UPDATE t SET v = ?1 WHERE a = ?2 AND k = ?3 AND v = ?4",
            &["x", "a1", "k1", "old"],
            "a = ?1 AND k = ?2 AND v = ?3",
            &["a1", "k1", "old"],
        ),
        (
            "UPDATE t SET v = ?1 WHERE a = ?2 AND v = ?3 AND k = ?4",
            &["x", "a1", "old", "k1"],
            "a = ?1 AND v = ?2 AND k = ?3",
            &["a1", "old", "k1"],
        ),
        (
            "UPDATE t SET v = ?3 WHERE a = ?1 AND k = ?2",
            &["a1", "k1", "x"],
            "a = ?1 AND k = ?2",
            &["a1", "k1"],
        ),
        (
            "UPDATE t SET v = ?3 WHERE a = ? AND k = ?",
            &["unused-1", "unused-2", "x", "a1", "k1"],
            "a = ? AND k = ?",
            &["a1", "k1"],
        ),
        (
            "UPDATE t SET v = :value WHERE a = @a AND k = $key AND :value = :value",
            &["x", "a1", "k1"],
            "a = @a AND k = $key AND :value = :value",
            &["a1", "k1", "x"],
        ),
        (
            "DELETE FROM t WHERE a = ?2 AND k = ?1",
            &["k1", "a1"],
            "a = ?2 AND k = ?1",
            &["k1", "a1"],
        ),
        (
            "DELETE FROM t WHERE k = ?1 AND a = ?2 AND v = ?3",
            &["k1", "a1", "old"],
            "k = ?1 AND a = ?2 AND v = ?3",
            &["k1", "a1", "old"],
        ),
        (
            "DELETE FROM t WHERE a = ?1 AND k = ?2 AND v = ?3",
            &["a1", "k1", "old"],
            "a = ?1 AND k = ?2 AND v = ?3",
            &["a1", "k1", "old"],
        ),
        (
            "DELETE FROM t WHERE a = ?1 AND v = ?2 AND k = ?3",
            &["a1", "old", "k1"],
            "a = ?1 AND v = ?2 AND k = ?3",
            &["a1", "old", "k1"],
        ),
        (
            "DELETE FROM t WHERE a = ?2 AND k = ?",
            &["unused", "a1", "k1"],
            "a = ?2 AND k = ?",
            &["unused", "a1", "k1"],
        ),
    ];
    for (sql, values, predicate, where_values) in explicit_cases {
        let params = text_params(values);
        let where_params = text_params(where_values);
        for use_prepared in [false, true] {
            reset_text_pk(&f, &r).await;
            assert_where_count(&f, &r, predicate, &where_params, 1).await;
            let prepared = if use_prepared {
                Some(f.prepare(sql).await.unwrap())
            } else {
                None
            };
            assert_write(&f, &r, TEXT_PK_STATE, sql, &params, 1, prepared.as_ref()).await;
        }
    }
    if wal {
        // Keep both the last DELETE and an UPDATE visible to the reopen check.
        let where_params = text_params(&["a2", "k2"]);
        assert_where_count(&f, &r, "a = ? AND k = ?", &where_params, 1).await;
        assert_write(
            &f,
            &r,
            TEXT_PK_STATE,
            "UPDATE t SET v = ? WHERE a = ? AND k = ?",
            &text_params(&["persisted", "a2", "k2"]),
            1,
            None,
        )
        .await;
    }
    f.close().await.unwrap();
    if wal {
        let reopened = Connection::open(dir.path().join("frank.db").to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(
            frank_state(&reopened, TEXT_PK_STATE, 3).await,
            sqlite_state(&r, TEXT_PK_STATE, 3),
            "parameterized writes must persist across reopen"
        );
        reopened.close().await.unwrap();
    }
}

#[test]
fn gh541_text_primary_key_parameter_order_memory() {
    asupersync::test_utils::run_test(|| text_pk_write_matrix(false));
}

#[test]
fn gh541_text_primary_key_parameter_order_wal() {
    asupersync::test_utils::run_test(|| text_pk_write_matrix(true));
}

const ROWID_STATE: &str = "SELECT rowid, a, v FROM t";

/// GH#541's rowid seeks. Through 0.4.4, an INTEGER PRIMARY KEY or `rowid`
/// equality written after another anonymous `?` was bound to the wrong
/// parameter slot: `UPDATE ... WHERE a = ? AND k = ?` changed 0 rows. The
/// decoy (3, 7) swaps the target (7, 3)'s key and `a`, so a swapped binding
/// shows up as a write to the wrong row rather than as a correct count.
async fn rowid_write_matrix(ddl: &str, seed: &str, keys: &[&str]) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    f.execute(ddl).await.unwrap();
    r.execute_batch(ddl).unwrap();
    // The target row's key and `a` values; v is 'old' on every row.
    let (k, a, v) = (
        SqliteValue::Integer(7),
        SqliteValue::Integer(3),
        SqliteValue::Text("old".into()),
    );
    for key in keys {
        // Each key-second shape sits beside its key-first twin.
        let predicates = [
            (format!("a = ? AND {key} = ?"), vec![a.clone(), k.clone()]),
            (format!("{key} = ? AND a = ?"), vec![k.clone(), a.clone()]),
            (
                format!("a = ? AND v = ? AND {key} = ?"),
                vec![a.clone(), v.clone(), k.clone()],
            ),
            (
                format!("{key} = ? AND v = ? AND a = ?"),
                vec![k.clone(), v.clone(), a.clone()],
            ),
            (format!("{key} = ?"), vec![k.clone()]),
        ];
        for (predicate, where_params) in &predicates {
            for (prefix, bind_set) in [
                ("UPDATE t SET v = ?", true),
                ("UPDATE t SET v = 'x'", false),
                ("DELETE FROM t", false),
            ] {
                let sql = format!("{prefix} WHERE {predicate}");
                let mut params = if bind_set {
                    text_params(&["x"])
                } else {
                    Vec::new()
                };
                params.extend_from_slice(where_params);
                for use_prepared in [false, true] {
                    for reset in ["DELETE FROM t", seed] {
                        f.execute(reset).await.unwrap();
                        r.execute_batch(reset).unwrap();
                    }
                    assert_where_count(&f, &r, predicate, where_params, 1).await;
                    let prepared = if use_prepared {
                        Some(f.prepare(&sql).await.unwrap())
                    } else {
                        None
                    };
                    assert_write(&f, &r, ROWID_STATE, &sql, &params, 1, prepared.as_ref()).await;
                }
            }
        }
    }
    f.close().await.unwrap();
}

#[test]
fn gh541_integer_primary_key_parameter_order() {
    asupersync::test_utils::run_test(|| {
        rowid_write_matrix(
            "CREATE TABLE t (k INTEGER PRIMARY KEY, a INTEGER NOT NULL, v TEXT)",
            "INSERT INTO t (k, a, v) VALUES (7, 3, 'old'), (3, 7, 'old')",
            &["k", "rowid"],
        )
    });
}

#[test]
fn gh541_rowid_parameter_order() {
    asupersync::test_utils::run_test(|| {
        rowid_write_matrix(
            "CREATE TABLE t (a INTEGER NOT NULL, v TEXT)",
            "INSERT INTO t (rowid, a, v) VALUES (7, 3, 'old'), (3, 7, 'old')",
            &["rowid"],
        )
    });
}

#[test]
fn gh541_prepared_text_primary_key_rebinds_after_misses_and_schema_change() {
    asupersync::test_utils::run_test(|| async {
        for wal in [false, true] {
            let (_dir, f, r) = text_pk_databases(wal).await;
            for deleting in [false, true] {
                reset_text_pk(&f, &r).await;
                let sql = if deleting {
                    "DELETE FROM t WHERE a = ? AND k = ?"
                } else {
                    "UPDATE t SET v = ? WHERE a = ? AND k = ?"
                };
                let prepared = f.prepare(sql).await.unwrap();
                for (step, (a, key, expected)) in [
                    (Some("a1"), Some("missing"), 0),
                    (Some("wrong"), Some("k1"), 0),
                    (Some("a1"), None, 0),
                    (None, Some("k1"), 0),
                    (Some("a1"), Some("k1"), 1),
                    (Some("a2"), Some("k2"), 1),
                ]
                .into_iter()
                .enumerate()
                {
                    if step == 5 {
                        // Reprepare the retained handle from its original SQL,
                        // without changing the target table's available indexes.
                        let ddl = format!("CREATE TABLE reprepare_{deleting} (n INTEGER)");
                        f.execute(&ddl).await.unwrap();
                        r.execute_batch(&ddl).unwrap();
                    }
                    let where_params: Vec<_> = [a, key]
                        .into_iter()
                        .map(|value| {
                            value.map_or(SqliteValue::Null, |text| SqliteValue::Text(text.into()))
                        })
                        .collect();
                    assert_where_count(&f, &r, "a = ? AND k = ?", &where_params, expected).await;
                    let mut params = if deleting {
                        Vec::new()
                    } else {
                        vec![SqliteValue::Text(format!("new-{step}").into())]
                    };
                    params.extend_from_slice(&where_params);
                    assert_write(
                        &f,
                        &r,
                        TEXT_PK_STATE,
                        sql,
                        &params,
                        expected,
                        Some(&prepared),
                    )
                    .await;
                }
            }
            f.close().await.unwrap();
        }
    });
}
