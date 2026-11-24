//! bd-732ft: a ROLLBACK after a CTE-fed INSERT into a table with a trigger
//! emptied a TEMP table (`:memory:`) or detached it (file: "OpenRead failed:
//! TEMP root page 2147483646 is not attached").
//!
//! Cause: an earlier statement whose trigger wrote through storage cursors left
//! the row mirror marked dirty, so the next CTE-fed INSERT's materialization
//! refresh, inside the transaction, rebuilt the `MemDatabase` from the pager.
//! The rebuild copies TEMP and materialized tables over, but published a fresh
//! undo log that held only its own construction. BEGIN, savepoint and statement
//! undo tokens point into the old log, so ROLLBACK and ROLLBACK TO undid the
//! rebuild (deleting the copied TEMP tables) instead of the transaction's TEMP
//! writes, and a failing statement could no longer unwind its own TEMP writes.
//! Since 31fe23f50 (bd-cmsax) both reload swap sites hand the live undo log,
//! statement marks and transaction flag to the rebuilt image
//! (`MemDatabase::hand_undo_state_to`).
//!
//! The scenarios: the bead's repro and a plain trigger-firing insert before the
//! transaction; TEMP writes inside the transaction vanish and TEMP rows from
//! before it survive; ROLLBACK TO a savepoint; a unique TEMP index and TEMP
//! triggers; a statement failing after the rebuild inside a transaction; and,
//! against a hand-over that leaks, a COMMIT of the rebuilt transaction followed
//! by later transactions' ROLLBACKs. (A failing CTE-fed autocommit INSERT that
//! keeps its TEMP triggers' writes with no rebuild at all is bd-cmsax's own
//! case, covered by bd_cmsax_cte_autocommit_temp_trigger_unwind_oracle.)
//!
//! Each scenario runs step by step against bundled SQLite 3.54.1 on `:memory:`
//! and on a file: every statement succeeds or fails on both, and every SELECT
//! returns the same rows.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

/// A step's outcome: the rows of a SELECT, an empty row set for any other
/// statement that succeeded, or the error text.
type Outcome = Result<Vec<Vec<String>>, String>;

fn is_query(sql: &str) -> bool {
    sql.trim_start().to_ascii_uppercase().starts_with("SELECT")
}

fn stock_step(conn: &rusqlite::Connection, sql: &str) -> Outcome {
    if !is_query(sql) {
        return conn
            .execute_batch(sql)
            .map(|()| Vec::new())
            .map_err(|e| e.to_string());
    }
    let mut statement = conn.prepare(sql).map_err(|e| e.to_string())?;
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .map_err(|e| e.to_string())
}

async fn fsqlite_step(conn: &Connection, sql: &str) -> Outcome {
    if !is_query(sql) {
        return conn
            .execute(sql)
            .await
            .map(|_| Vec::new())
            .map_err(|e| e.to_string());
    }
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

/// The bead's schema: a TEMP table with a row from before any transaction and
/// a main table whose trigger writes a second main table.
const BEAD_SCHEMA: &[&str] = &[
    "CREATE TEMP TABLE scratch(x)",
    "INSERT INTO scratch VALUES (1)",
    "CREATE TABLE t3(id INTEGER PRIMARY KEY, n INTEGER)",
    "CREATE TABLE l3(id, c)",
    "CREATE TRIGGER t3i AFTER INSERT ON t3 BEGIN INSERT INTO l3 VALUES (NEW.id, 0); END",
];

/// Adds a unique TEMP index, a TEMP trigger on the TEMP table and a TEMP
/// trigger on the main table, both writing a TEMP log.
const TEMP_OBJECTS_SCHEMA: &[&str] = &[
    "CREATE TEMP TABLE scratch(x)",
    "INSERT INTO scratch VALUES (1)",
    "CREATE UNIQUE INDEX temp.scratch_ux ON scratch(x)",
    "CREATE TEMP TABLE temp_log(v)",
    "CREATE TEMP TRIGGER scratch_log AFTER INSERT ON scratch BEGIN \
       INSERT INTO temp_log VALUES (NEW.x); END",
    "CREATE TABLE t3(id INTEGER PRIMARY KEY, n INTEGER)",
    "CREATE TABLE l3(id, c)",
    "CREATE TRIGGER t3i AFTER INSERT ON t3 BEGIN INSERT INTO l3 VALUES (NEW.id, 0); END",
    "CREATE TEMP TRIGGER t3_temp AFTER INSERT ON t3 BEGIN \
       INSERT INTO temp_log VALUES (-NEW.id); END",
];

/// The autocommit CTE-fed insert that leaves the row mirror dirty.
const CTE_INSERT_1_20: &str = "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s \
    WHERE i < 20) INSERT INTO t3 SELECT i, i FROM s";

/// The in-transaction CTE-fed insert whose materialization rebuilds the mirror.
const CTE_INSERT_21_25: &str = "WITH RECURSIVE s(i) AS (SELECT 21 UNION ALL SELECT i + 1 \
    FROM s WHERE i < 25) INSERT INTO t3 SELECT i, i FROM s";

/// The same insert failing on its last row (id 1 exists), after its triggers
/// wrote the first four rows' TEMP log entries.
const CTE_INSERT_FAILING: &str = "WITH RECURSIVE s(i) AS (SELECT 21 UNION ALL SELECT i + 1 \
    FROM s WHERE i < 25) INSERT INTO t3 SELECT CASE WHEN i = 25 THEN 1 ELSE i END, i FROM s";

const MAIN_VERIFY: &[&str] = &[
    "SELECT count(*), sum(id), sum(n) FROM t3",
    "SELECT count(*), sum(id) FROM l3",
];

const TEMP_OBJECTS_VERIFY: &[&str] = &[
    "SELECT x FROM scratch ORDER BY x",
    "SELECT rowid, v FROM temp_log ORDER BY rowid",
    "SELECT type, name, tbl_name FROM sqlite_temp_master ORDER BY name",
];

struct Scenario {
    name: &'static str,
    schema: &'static [&'static str],
    steps: Vec<&'static str>,
}

fn scenarios() -> Vec<Scenario> {
    let with_main_verify = |mut steps: Vec<&'static str>| {
        steps.extend_from_slice(MAIN_VERIFY);
        steps
    };
    let with_temp_verify = |mut steps: Vec<&'static str>| {
        steps.extend_from_slice(TEMP_OBJECTS_VERIFY);
        steps.extend_from_slice(MAIN_VERIFY);
        steps
    };
    vec![
        // The bead's repro.
        Scenario {
            name: "bead_repro",
            schema: BEAD_SCHEMA,
            steps: with_main_verify(vec![
                CTE_INSERT_1_20,
                "BEGIN",
                CTE_INSERT_21_25,
                "ROLLBACK",
                "SELECT count(*) FROM scratch",
            ]),
        },
        // Any earlier trigger-firing write leaves the mirror dirty, not only a
        // CTE-fed one.
        Scenario {
            name: "plain_trigger_insert_before_txn",
            schema: BEAD_SCHEMA,
            steps: with_main_verify(vec![
                "INSERT INTO t3 VALUES (100, 100)",
                "BEGIN",
                CTE_INSERT_21_25,
                "ROLLBACK",
                "SELECT x FROM scratch ORDER BY x",
            ]),
        },
        // TEMP writes inside the transaction, before and after the rebuild,
        // vanish; the row from before it survives its in-transaction UPDATE.
        Scenario {
            name: "txn_temp_writes_vanish",
            schema: BEAD_SCHEMA,
            steps: with_main_verify(vec![
                CTE_INSERT_1_20,
                "BEGIN",
                "INSERT INTO scratch VALUES (2)",
                "UPDATE scratch SET x = x + 100 WHERE x = 1",
                CTE_INSERT_21_25,
                "INSERT INTO scratch VALUES (3)",
                "DELETE FROM scratch WHERE x = 2",
                "SELECT x FROM scratch ORDER BY x",
                "ROLLBACK",
                "SELECT rowid, x FROM scratch ORDER BY rowid",
                "INSERT INTO scratch VALUES (4)",
                "SELECT rowid, x FROM scratch ORDER BY rowid",
            ]),
        },
        // ROLLBACK TO a savepoint taken before the rebuild keeps the writes
        // before the savepoint, and the transaction commits.
        Scenario {
            name: "rollback_to_savepoint",
            schema: BEAD_SCHEMA,
            steps: with_main_verify(vec![
                CTE_INSERT_1_20,
                "BEGIN",
                "INSERT INTO scratch VALUES (2)",
                "SAVEPOINT a",
                "INSERT INTO scratch VALUES (3)",
                CTE_INSERT_21_25,
                "INSERT INTO scratch VALUES (4)",
                "ROLLBACK TO a",
                "SELECT x FROM scratch ORDER BY x",
                "INSERT INTO scratch VALUES (5)",
                "COMMIT",
                "SELECT x FROM scratch ORDER BY x",
            ]),
        },
        // A unique TEMP index and TEMP triggers survive the ROLLBACK: the
        // index still refuses a row from before the transaction and accepts
        // one only the rolled-back transaction had.
        Scenario {
            name: "temp_index_and_triggers",
            schema: TEMP_OBJECTS_SCHEMA,
            steps: with_temp_verify(vec![
                CTE_INSERT_1_20,
                "BEGIN",
                "INSERT INTO scratch VALUES (2)",
                CTE_INSERT_21_25,
                "INSERT INTO scratch VALUES (3)",
                "ROLLBACK",
                "INSERT INTO scratch VALUES (1)",
                "INSERT INTO scratch VALUES (2)",
                "INSERT INTO t3 VALUES (30, 30)",
            ]),
        },
        // A statement that fails after the rebuild unwinds its own TEMP writes
        // and keeps the transaction's earlier ones, which then commit.
        Scenario {
            name: "failed_statement_in_txn",
            schema: TEMP_OBJECTS_SCHEMA,
            steps: with_temp_verify(vec![
                CTE_INSERT_1_20,
                "BEGIN",
                "INSERT INTO scratch VALUES (2)",
                CTE_INSERT_FAILING,
                "INSERT INTO scratch VALUES (3)",
                "COMMIT",
            ]),
        },
        // The handed-over log ends with its transaction: a later ROLLBACK
        // undoes only the later transaction, after a COMMIT of the rebuilt
        // one and again after a second rebuild in the later transaction.
        Scenario {
            name: "commit_after_rebuild_then_later_rollback",
            schema: TEMP_OBJECTS_SCHEMA,
            steps: with_temp_verify(vec![
                CTE_INSERT_1_20,
                "BEGIN",
                "INSERT INTO scratch VALUES (2)",
                CTE_INSERT_21_25,
                "INSERT INTO scratch VALUES (3)",
                "COMMIT",
                "BEGIN",
                "INSERT INTO scratch VALUES (4)",
                "DELETE FROM scratch WHERE x = 1",
                "ROLLBACK",
                "SELECT x FROM scratch ORDER BY x",
                "SELECT count(*), sum(v) FROM temp_log",
                "INSERT INTO t3 VALUES (200, 200)",
                "BEGIN",
                "INSERT INTO scratch VALUES (5)",
                "WITH RECURSIVE s(i) AS (SELECT 26 UNION ALL SELECT i + 1 FROM s WHERE i < 30) \
                 INSERT INTO t3 SELECT i, i FROM s",
                "ROLLBACK",
                "INSERT INTO scratch VALUES (3)",
            ]),
        },
    ]
}

fn database_path(dir: &tempfile::TempDir, engine: &str, file_backed: bool) -> String {
    if file_backed {
        dir.path()
            .join(format!("{engine}.db"))
            .to_str()
            .unwrap()
            .to_owned()
    } else {
        ":memory:".to_owned()
    }
}

#[test]
fn temp_state_after_an_in_transaction_mirror_rebuild_matches_stock() {
    let scenarios = scenarios();
    // Not vacuous: on stock the bead's TEMP row survives the ROLLBACK, the
    // failing insert fails, and the TEMP triggers logged the committed rows.
    {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in BEAD_SCHEMA {
            stock.execute_batch(sql).unwrap();
        }
        let bead = &scenarios[0];
        let outcomes: Vec<_> = bead.steps.iter().map(|sql| stock_step(&stock, sql)).collect();
        assert_eq!(outcomes[4], Ok(vec![vec!["1".to_owned()]]), "{outcomes:?}");
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in TEMP_OBJECTS_SCHEMA {
            stock.execute_batch(sql).unwrap();
        }
        let failed = &scenarios[5];
        let outcomes: Vec<_> = failed
            .steps
            .iter()
            .map(|sql| stock_step(&stock, sql))
            .collect();
        assert!(outcomes[3].is_err(), "{outcomes:?}");
        assert_eq!(
            outcomes[6],
            Ok(vec![
                vec!["1".to_owned()],
                vec!["2".to_owned()],
                vec!["3".to_owned()]
            ]),
            "{outcomes:?}"
        );
    }
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for scenario in &scenarios {
            for file_backed in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let stock =
                    rusqlite::Connection::open(database_path(&dir, "stock", file_backed)).unwrap();
                let conn = Connection::open(&database_path(&dir, "fsqlite", file_backed))
                    .await
                    .unwrap();
                for sql in scenario.schema {
                    stock.execute_batch(sql).unwrap();
                    conn.execute(sql)
                        .await
                        .unwrap_or_else(|e| panic!("{}: schema `{sql}`: {e}", scenario.name));
                }
                for (index, sql) in scenario.steps.iter().enumerate() {
                    let want = stock_step(&stock, sql);
                    let got = fsqlite_step(&conn, sql).await;
                    let same = match (&want, &got) {
                        (Ok(want), Ok(got)) => want == got,
                        (Err(_), Err(_)) => true,
                        _ => false,
                    };
                    if !same {
                        mismatches.push(format!(
                            "{} (file_backed={file_backed}) step {index} `{sql}`: \
                             stock {want:?}, fsqlite {got:?}",
                            scenario.name
                        ));
                    }
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "bd-732ft: {} step(s) differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
