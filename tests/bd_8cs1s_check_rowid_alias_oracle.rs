#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! bd-8cs1s: a CHECK constraint that names a hidden rowid alias (`rowid`,
//! `_rowid_`, `oid`) must be enforced by FrankenSQLite writes exactly as stock
//! SQLite enforces it.
//!
//! Defect: the write paths evaluate CHECK against the new row's image held in
//! registers, and that image carried no rowid, so every rowid alias in a CHECK
//! expression evaluated to NULL. A NULL CHECK result passes, so
//! `CREATE TABLE ck(a, CHECK(rowid > 1)); INSERT INTO ck VALUES (1)` was
//! accepted where stock raises "CHECK constraint failed: rowid > 1".
//!
//! Every expectation comes from a live stock-SQLite (rusqlite, bundled)
//! oracle. The oracle file runs setup and steps in stock; the subject file
//! runs setup in stock or FrankenSQLite (`Origin`) and the steps in
//! FrankenSQLite. Each step's outcome (success, or the exact stock error
//! message) must match, stock must read the same rows back from the subject
//! file, and both engines' integrity_check must be `ok`. The failing-step
//! lists pinned below are asserted against the oracle first, so they are
//! oracle-verified rather than hand-made expectations.

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[derive(Clone, Copy, Debug)]
enum Origin {
    /// Stock SQLite creates the schema and the seed rows.
    Stock,
    /// FrankenSQLite creates the schema and the seed rows.
    Fsqlite,
}

struct Scenario {
    label: String,
    /// Schema and seed rows, run by whichever engine `Origin` names.
    setup: Vec<String>,
    /// Statements FrankenSQLite runs against the subject file.
    steps: Vec<String>,
    /// Queries stock uses to read every table back.
    dumps: Vec<String>,
    /// Oracle-verified `(step index, stock error)` for every failing step.
    expected_failures: Vec<(usize, String)>,
}

type Outcome = Result<(), String>;
type Dump = Vec<Vec<String>>;

fn stock_value(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Null => "NULL".to_owned(),
        rusqlite::types::ValueRef::Integer(n) => n.to_string(),
        rusqlite::types::ValueRef::Real(r) => format!("{r:?}"),
        rusqlite::types::ValueRef::Text(text) => {
            format!("'{}'", String::from_utf8_lossy(text))
        }
        rusqlite::types::ValueRef::Blob(blob) => format!("x'{blob:02x?}'"),
    }
}

fn stock_dump(db: &rusqlite::Connection, sql: &str) -> Dump {
    let mut stmt = db
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock prepare `{sql}`: {e}"));
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| row.get_ref(i).map(stock_value))
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .unwrap_or_else(|e| panic!("stock query `{sql}`: {e}"))
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap_or_else(|e| panic!("stock rows `{sql}`: {e}"))
}

fn stock_integrity(db: &rusqlite::Connection) -> Vec<String> {
    db.prepare("PRAGMA integrity_check")
        .expect("prepare stock integrity_check")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("run stock integrity_check")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect stock integrity_check")
}

fn stock_run_all(path: &Path, statements: &[String]) {
    let db = rusqlite::Connection::open(path).expect("open with stock SQLite");
    for sql in statements {
        db.execute_batch(sql)
            .unwrap_or_else(|e| panic!("stock `{sql}`: {e}"));
    }
}

fn stock_outcomes(path: &Path, statements: &[String]) -> Vec<Outcome> {
    let db = rusqlite::Connection::open(path).expect("open with stock SQLite");
    statements
        .iter()
        .map(|sql| db.execute_batch(sql).map_err(|e| e.to_string()))
        .collect()
}

async fn fsqlite_open(path: &Path, label: &str) -> Connection {
    Connection::open(path.to_str().expect("utf-8 temp path"))
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite open: {e}"))
}

async fn fsqlite_run_all(path: &Path, statements: &[String], label: &str) {
    let conn = fsqlite_open(path, label).await;
    for sql in statements {
        conn.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("[{label}] fsqlite `{sql}`: {e}"));
    }
    conn.close()
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite close: {e}"));
}

async fn fsqlite_outcomes(path: &Path, statements: &[String], label: &str) -> Vec<Outcome> {
    let conn = fsqlite_open(path, label).await;
    let mut outcomes = Vec::with_capacity(statements.len());
    for sql in statements {
        outcomes.push(
            conn.execute(sql)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
        );
    }
    conn.close()
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite close: {e}"));
    outcomes
}

async fn fsqlite_integrity(path: &Path, label: &str) -> Vec<String> {
    let conn = fsqlite_open(path, label).await;
    let rows = conn
        .query("PRAGMA integrity_check")
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite integrity_check: {e}"))
        .iter()
        .map(|row| match &row.values()[0] {
            SqliteValue::Text(text) => text.to_string(),
            other => format!("{other:?}"),
        })
        .collect();
    conn.close()
        .await
        .unwrap_or_else(|e| panic!("[{label}] fsqlite close after integrity_check: {e}"));
    rows
}

/// Runs `scenario` on a stock oracle file, checks the pinned failures against
/// it, then replays the steps through FrankenSQLite on a stock-created and on
/// a FrankenSQLite-created subject file. Returns the oracle's table dumps.
async fn assert_check_steps_match_stock(scenario: &Scenario) -> Vec<Dump> {
    let label = scenario.label.as_str();
    let dir = tempfile::tempdir().expect("tempdir");
    let oracle_path = dir.path().join("oracle.db");

    stock_run_all(&oracle_path, &scenario.setup);
    let oracle_outcomes = stock_outcomes(&oracle_path, &scenario.steps);
    let oracle_failures: Vec<(usize, String)> = oracle_outcomes
        .iter()
        .enumerate()
        .filter_map(|(i, outcome)| outcome.as_ref().err().map(|e| (i, e.clone())))
        .collect();
    assert_eq!(
        oracle_failures, scenario.expected_failures,
        "[{label}] premise: the stock oracle rejects exactly the pinned steps"
    );
    let (oracle_dumps, oracle_integrity) = {
        let db = rusqlite::Connection::open(&oracle_path).expect("open oracle");
        let dumps: Vec<Dump> = scenario
            .dumps
            .iter()
            .map(|sql| stock_dump(&db, sql))
            .collect();
        (dumps, stock_integrity(&db))
    };
    assert_eq!(
        oracle_integrity,
        vec!["ok".to_owned()],
        "[{label}] premise: the stock oracle must be intact"
    );

    for origin in [Origin::Stock, Origin::Fsqlite] {
        let run = format!("{label}/{origin:?}");
        let subject_path = dir.path().join(format!("subject_{origin:?}.db"));
        match origin {
            Origin::Stock => stock_run_all(&subject_path, &scenario.setup),
            Origin::Fsqlite => fsqlite_run_all(&subject_path, &scenario.setup, &run).await,
        }
        let outcomes = fsqlite_outcomes(&subject_path, &scenario.steps, &run).await;
        for ((sql, expected), outcome) in scenario.steps.iter().zip(&oracle_outcomes).zip(&outcomes)
        {
            match (expected, outcome) {
                (Ok(()), Ok(())) => {}
                (Err(stock), Err(ours)) => assert!(
                    ours.contains(stock.as_str()),
                    "[{run}] `{sql}`: stock error {stock:?}, FrankenSQLite error {ours:?}"
                ),
                _ => panic!("[{run}] `{sql}`: stock {expected:?}, FrankenSQLite {outcome:?}"),
            }
        }

        {
            let db = rusqlite::Connection::open(&subject_path).expect("stock open subject");
            assert_eq!(
                stock_integrity(&db),
                vec!["ok".to_owned()],
                "[{run}] stock integrity_check after FrankenSQLite writes"
            );
            for (sql, oracle_dump) in scenario.dumps.iter().zip(&oracle_dumps) {
                assert_eq!(
                    &stock_dump(&db, sql),
                    oracle_dump,
                    "[{run}] `{sql}` after FrankenSQLite writes differs from the stock oracle"
                );
            }
        }
        assert_eq!(
            fsqlite_integrity(&subject_path, &run).await,
            vec!["ok".to_owned()],
            "[{run}] FrankenSQLite integrity_check after its own writes"
        );
    }

    oracle_dumps
}

fn strings(statements: &[&str]) -> Vec<String> {
    statements.iter().map(|sql| (*sql).to_owned()).collect()
}

fn failures(pins: &[(usize, &str)]) -> Vec<(usize, String)> {
    pins.iter()
        .map(|(i, message)| (*i, (*message).to_owned()))
        .collect()
}

fn row(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// The reported repro, plus a named CHECK (its name is the error message).
#[test]
fn bd_8cs1s_check_on_rowid_rejects_the_reported_insert() {
    asupersync::test_utils::run_test(|| async {
        let scenario = Scenario {
            label: "repro".to_owned(),
            setup: strings(&[
                "CREATE TABLE ck(a, CHECK(rowid > 1))",
                "CREATE TABLE n(a, CONSTRAINT pos CHECK(oid > 1))",
            ]),
            steps: strings(&[
                "INSERT INTO ck VALUES (1)",
                "INSERT INTO ck VALUES (2)",
                "INSERT INTO ck(rowid, a) VALUES (5, 3)",
                "INSERT INTO ck VALUES (4)",
                "INSERT INTO n VALUES (1)",
                "INSERT INTO n(rowid, a) VALUES (2, 1)",
                "UPDATE n SET rowid = 1",
            ]),
            dumps: strings(&[
                "SELECT rowid, a FROM ck ORDER BY rowid",
                "SELECT rowid, a FROM n ORDER BY rowid",
            ]),
            expected_failures: failures(&[
                (0, "CHECK constraint failed: rowid > 1"),
                (1, "CHECK constraint failed: rowid > 1"),
                (4, "CHECK constraint failed: pos"),
                (6, "CHECK constraint failed: pos"),
            ]),
        };
        let dumps = assert_check_steps_match_stock(&scenario).await;
        assert_eq!(dumps[0], vec![row(&["5", "3"]), row(&["6", "4"])]);
        assert_eq!(dumps[1], vec![row(&["2", "1"])]);
    });
}

/// Every write shape against a table without an INTEGER PRIMARY KEY, once per
/// alias spelling: VALUES (single, multi-row, auto rowid), INSERT ... SELECT
/// with and without FROM, OR IGNORE, DEFAULT VALUES, UPDATE (each alias as the
/// assignment target, OR IGNORE, UPDATE ... FROM), UPSERT DO UPDATE / DO
/// NOTHING (the attempted row is checked with its newly allocated rowid before
/// conflict resolution), REPLACE and INSERT OR REPLACE.
#[test]
fn bd_8cs1s_check_on_rowid_alias_matches_stock_on_every_write_path() {
    asupersync::test_utils::run_test(|| async {
        for alias in ["rowid", "_rowid_", "oid", "OID", "t.rowid"] {
            let check = format!("{alias} > 1 AND {alias} < 100");
            let message = format!("CHECK constraint failed: {check}");
            let scenario = Scenario {
                label: format!("hidden_{alias}"),
                setup: vec![
                    format!("CREATE TABLE t(k UNIQUE, v, CHECK({check}))"),
                    "INSERT INTO t(rowid, k, v) VALUES (10, 'seed', 0)".to_owned(),
                ],
                steps: strings(&[
                    "INSERT INTO t(rowid, k, v) VALUES (1, 'a', 1)",
                    "INSERT INTO t(rowid, k, v) VALUES (100, 'a', 1)",
                    "INSERT INTO t(k, v) VALUES ('a', 1)",
                    "INSERT INTO t(rowid, k, v) VALUES (2, 'b', 2), (3, 'c', 3)",
                    "INSERT INTO t(rowid, k, v) VALUES (4, 'd', 4), (0, 'e', 5)",
                    "INSERT INTO t(k, v) SELECT k || 'x', v + 10 FROM t \
                     WHERE v BETWEEN 1 AND 3 ORDER BY rowid",
                    "INSERT INTO t(rowid, k, v) SELECT -rowid, k || 'y', v FROM t WHERE k = 'a'",
                    "INSERT INTO t(rowid, k, v) SELECT 50, 'sel', 9",
                    "INSERT INTO t(rowid, k, v) SELECT 1, 'sel1', 9",
                    "INSERT OR IGNORE INTO t(rowid, k, v) VALUES (1, 'i1', 0), (60, 'i60', 0)",
                    "UPDATE t SET rowid = rowid - 10 WHERE k = 'a'",
                    "UPDATE t SET rowid = 20 WHERE k = 'a'",
                    "UPDATE t SET v = v + 1 WHERE v > 0",
                    "UPDATE OR IGNORE t SET rowid = 1, v = -1 WHERE k = 'b'",
                    "UPDATE t SET oid = 150 WHERE k = 'b'",
                    "UPDATE t SET _rowid_ = 30 WHERE k = 'b'",
                    "UPDATE t SET rowid = s.r FROM (SELECT 1 AS r) AS s WHERE t.k = 'c'",
                    "UPDATE t SET rowid = s.r FROM (SELECT 40 AS r) AS s WHERE t.k = 'c'",
                    "INSERT INTO t(rowid, k, v) VALUES (5, 'a', 7) \
                     ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                    "INSERT INTO t(rowid, k, v) VALUES (0, 'a', 7) \
                     ON CONFLICT(k) DO UPDATE SET v = 8",
                    "INSERT INTO t(k, v) VALUES ('a', 9) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                    "INSERT INTO t(k, v) VALUES ('new', 9) ON CONFLICT(k) DO NOTHING",
                    "REPLACE INTO t(rowid, k, v) VALUES (1, 'b', 0)",
                    "REPLACE INTO t(rowid, k, v) VALUES (25, 'b', 0)",
                    "INSERT OR REPLACE INTO t(k, v) VALUES ('c', 0)",
                    "INSERT INTO t(rowid, k, v) VALUES (99, 'z', 0)",
                    "INSERT INTO t DEFAULT VALUES",
                    "INSERT INTO t(k, v) VALUES ('a', 1) ON CONFLICT(k) DO UPDATE SET v = 0",
                    "INSERT OR REPLACE INTO t(k, v) VALUES ('z', 1)",
                    "INSERT OR IGNORE INTO t(k, v) VALUES ('q', 1)",
                ]),
                dumps: strings(&["SELECT rowid, k, v FROM t ORDER BY rowid"]),
                expected_failures: [0, 1, 4, 6, 8, 10, 14, 16, 19, 22, 26, 27, 28]
                    .into_iter()
                    .map(|i| (i, message.clone()))
                    .collect(),
            };
            let dumps = assert_check_steps_match_stock(&scenario).await;
            assert_eq!(
                dumps[0]
                    .iter()
                    .map(|row| row[0].clone())
                    .collect::<Vec<_>>(),
                row(&[
                    "10", "12", "13", "14", "20", "25", "50", "60", "61", "62", "99"
                ]),
                "[hidden_{alias}] oracle pin: only rowids inside (1, 100) were written"
            );
        }
    });
}

/// A declared column shadows only the alias it is named after: `rowid` below
/// is an ordinary column while `oid` and `_rowid_` still name the hidden
/// rowid, and the other way round for a declared `oid`.
#[test]
fn bd_8cs1s_declared_rowid_column_shadows_only_its_own_alias_in_check() {
    asupersync::test_utils::run_test(|| async {
        let scenario = Scenario {
            label: "shadow".to_owned(),
            setup: strings(&[
                "CREATE TABLE s(rowid INTEGER, a, CHECK(rowid > 0), CHECK(oid > 1), \
                 CHECK(_rowid_ < 100))",
                "CREATE TABLE o(oid INTEGER, a, CHECK(oid > 0), CHECK(rowid > 1))",
            ]),
            steps: strings(&[
                "INSERT INTO s(rowid, a) VALUES (5, 1)",
                "INSERT INTO s(oid, rowid, a) VALUES (5, 5, 1)",
                "INSERT INTO s(oid, rowid, a) VALUES (6, 0, 1)",
                "INSERT INTO s(_rowid_, rowid, a) VALUES (100, 1, 1)",
                "INSERT INTO s(rowid, a) VALUES (1, 2)",
                "UPDATE s SET rowid = -1 WHERE a = 2",
                "UPDATE s SET oid = 1 WHERE a = 2",
                "UPDATE s SET rowid = 9, oid = 7 WHERE a = 2",
                "UPDATE s SET _rowid_ = 100 WHERE a = 1",
                "INSERT INTO s(rowid, a) SELECT rowid + 1, a + 10 FROM s ORDER BY oid",
                "INSERT INTO s DEFAULT VALUES",
                "INSERT INTO o(oid, a) VALUES (5, 1)",
                "INSERT INTO o(rowid, oid, a) VALUES (2, 0, 1)",
                "INSERT INTO o(rowid, oid, a) VALUES (2, 3, 1)",
                "UPDATE o SET _rowid_ = 1",
                "UPDATE o SET oid = -oid",
                "UPDATE o SET rowid = 8, oid = 4",
            ]),
            dumps: strings(&[
                "SELECT _rowid_, rowid, a FROM s ORDER BY _rowid_",
                "SELECT rowid, oid, a FROM o ORDER BY rowid",
            ]),
            expected_failures: failures(&[
                (0, "CHECK constraint failed: oid > 1"),
                (2, "CHECK constraint failed: rowid > 0"),
                (3, "CHECK constraint failed: _rowid_ < 100"),
                (5, "CHECK constraint failed: rowid > 0"),
                (6, "CHECK constraint failed: oid > 1"),
                (8, "CHECK constraint failed: _rowid_ < 100"),
                (11, "CHECK constraint failed: rowid > 1"),
                (12, "CHECK constraint failed: oid > 0"),
                (14, "CHECK constraint failed: rowid > 1"),
                (15, "CHECK constraint failed: oid > 0"),
            ]),
        };
        let dumps = assert_check_steps_match_stock(&scenario).await;
        assert_eq!(
            dumps[0],
            vec![
                row(&["5", "5", "1"]),
                row(&["7", "9", "2"]),
                row(&["8", "6", "11"]),
                row(&["9", "10", "12"]),
                row(&["10", "NULL", "NULL"]),
            ]
        );
        assert_eq!(dumps[1], vec![row(&["8", "4", "1"])]);
    });
}

/// INTEGER PRIMARY KEY tables: every alias names the same key as `id`, so an
/// explicit, NULL, auto-allocated or rewritten id is what the CHECK sees,
/// including the DO UPDATE half of an UPSERT that rewrites `id`.
#[test]
fn bd_8cs1s_check_on_rowid_alias_of_integer_primary_key_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let scenario = Scenario {
            label: "ipk".to_owned(),
            setup: strings(&[
                "CREATE TABLE p(id INTEGER PRIMARY KEY, k UNIQUE, v, CHECK(rowid <> 2), \
                 CHECK(oid < 100), CHECK(_rowid_ > -5))",
            ]),
            steps: strings(&[
                "INSERT INTO p VALUES (2, 'a', 0)",
                "INSERT INTO p(k, v) VALUES ('a', 0)",
                "INSERT INTO p(k, v) VALUES ('b', 0)",
                "INSERT INTO p(rowid, k, v) VALUES (3, 'b', 0)",
                "INSERT INTO p(id, k, v) VALUES (NULL, 'c', 0)",
                "INSERT INTO p(id, k, v) VALUES (-5, 'n', 0)",
                "UPDATE p SET id = 2 WHERE k = 'c'",
                "UPDATE p SET id = 150 WHERE k = 'c'",
                "UPDATE p SET rowid = -9 WHERE k = 'c'",
                "UPDATE p SET oid = 50 WHERE k = 'c'",
                "UPDATE p SET id = s.r FROM (SELECT 2 AS r) AS s WHERE p.k = 'b'",
                "INSERT INTO p(k, v) VALUES ('c', 1) ON CONFLICT(k) DO UPDATE SET id = 2",
                "INSERT INTO p(k, v) VALUES ('c', 1) ON CONFLICT(k) DO UPDATE SET id = 60",
                "INSERT INTO p(k, v) VALUES ('c', 1) ON CONFLICT(k) DO UPDATE SET v = v + 1",
                "REPLACE INTO p VALUES (2, 'a', 9)",
                "REPLACE INTO p VALUES (7, 'a', 9)",
                "UPDATE p SET id = id + 1 WHERE k = 'b'",
                "INSERT INTO p(id, k, v) SELECT 99, 'sel', 0",
                "INSERT INTO p(k, v) VALUES ('auto', 0)",
                "INSERT INTO p(k, v) VALUES ('c', 5) ON CONFLICT(k) DO UPDATE SET v = 5",
                "INSERT INTO p DEFAULT VALUES",
            ]),
            dumps: strings(&["SELECT id, k, v FROM p ORDER BY id"]),
            expected_failures: failures(&[
                (0, "CHECK constraint failed: rowid <> 2"),
                (2, "CHECK constraint failed: rowid <> 2"),
                (5, "CHECK constraint failed: _rowid_ > -5"),
                (6, "CHECK constraint failed: rowid <> 2"),
                (7, "CHECK constraint failed: oid < 100"),
                (8, "CHECK constraint failed: _rowid_ > -5"),
                (10, "CHECK constraint failed: rowid <> 2"),
                (11, "CHECK constraint failed: rowid <> 2"),
                (14, "CHECK constraint failed: rowid <> 2"),
                (18, "CHECK constraint failed: oid < 100"),
                (19, "CHECK constraint failed: oid < 100"),
                (20, "CHECK constraint failed: oid < 100"),
            ]),
        };
        let dumps = assert_check_steps_match_stock(&scenario).await;
        assert_eq!(
            dumps[0],
            vec![
                row(&["4", "'b'", "0"]),
                row(&["7", "'a'", "9"]),
                row(&["60", "'c'", "1"]),
                row(&["99", "'sel'", "0"]),
            ]
        );
    });
}
