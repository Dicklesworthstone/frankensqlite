#![recursion_limit = "512"]

//! GH#448: a DELETE stopped by its BEFORE trigger's `RAISE(FAIL)` must report
//! the rows it deleted before the failure in `changes()`, as stock does.
//!
//! Every combination of statement (DELETE, UPDATE, INSERT ... VALUES,
//! INSERT ... SELECT), trigger timing, RAISE action, failing row, trigger-body
//! writes and transaction state runs against stock SQLite (rusqlite), and so
//! does every conflict algorithm on a UNIQUE or PRIMARY KEY conflict partway
//! through a multi-row statement, with and without a trigger on the table.
//! After the statement each side reports `changes()`, `total_changes()`,
//! `last_insert_rowid()`, the table and the trigger's log, and again after
//! COMMIT. Stock's rules, which these cases pin:
//! - `changes()` counts the statement's own rows that survive: those before
//!   the failing row under FAIL (plus the failing row itself when an AFTER
//!   trigger raised), none under ABORT or ROLLBACK;
//! - `total_changes()` counts every trigger-body write that ran, even ones an
//!   ABORT then rolled back, plus the statement's surviving rows;
//! - `last_insert_rowid()` is never restored by a failure: it keeps the rowid
//!   of the last row the statement inserted, even one ABORT rolled back;
//! - `RAISE(ROLLBACK, msg)` reports `msg`, and in autocommit acts as ABORT.

use std::fmt::Write as _;

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

/// One scenario: setup statements, the statement under test, and the
/// statements that observe its effects.
struct Script {
    label: String,
    setup: Vec<String>,
    statement: String,
    after: Vec<&'static str>,
}

fn observations(explicit_transaction: bool) -> Vec<&'static str> {
    let mut after = vec![
        "SELECT changes()",
        "SELECT total_changes()",
        "SELECT last_insert_rowid()",
        "SELECT k, v FROM t ORDER BY k",
        "SELECT k, tag FROM log ORDER BY id",
    ];
    if explicit_transaction {
        after.extend([
            "COMMIT",
            "SELECT changes()",
            "SELECT total_changes()",
            "SELECT last_insert_rowid()",
            "SELECT k, v FROM t ORDER BY k",
            "SELECT count(*) FROM log",
        ]);
    }
    after
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    Delete,
    Update,
    InsertValues,
    InsertSelect,
}

#[derive(Clone, Copy, Debug)]
struct RaiseCase {
    kind: Kind,
    timing: &'static str,
    action: &'static str,
    /// The row (1-based, in key order) whose trigger raises.
    fail_at: i64,
    /// Rows the trigger body writes to `log` before it raises.
    log_writes: usize,
    /// Inside BEGIN, after an earlier INSERT in the same transaction.
    explicit_transaction: bool,
}

impl RaiseCase {
    fn script(self) -> Script {
        let (event, row_ref, key_base, statement) = match self.kind {
            Kind::Delete => ("DELETE", "OLD", 0, "DELETE FROM t WHERE k <= 5"),
            Kind::Update => ("UPDATE", "NEW", 0, "UPDATE t SET v = v + 1 WHERE k <= 5"),
            Kind::InsertValues => (
                "INSERT",
                "NEW",
                10,
                "INSERT INTO t VALUES (11, 0), (12, 0), (13, 0), (14, 0), (15, 0)",
            ),
            Kind::InsertSelect => (
                "INSERT",
                "NEW",
                10,
                "INSERT INTO t SELECT k, v FROM src ORDER BY k",
            ),
        };
        let raise = if self.action == "IGNORE" {
            "RAISE(IGNORE)".to_owned()
        } else {
            format!("RAISE({}, 'stop')", self.action)
        };
        let mut writes = String::new();
        for write in 0..self.log_writes {
            let _ = write!(
                writes,
                "INSERT INTO log(k, tag) VALUES ({row_ref}.k, 'w{write}'); "
            );
        }
        let mut setup = vec![
            "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER)".to_owned(),
            "CREATE TABLE log(id INTEGER PRIMARY KEY, k, tag)".to_owned(),
            "CREATE TABLE src(k INTEGER PRIMARY KEY, v INTEGER)".to_owned(),
            "INSERT INTO t VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)".to_owned(),
            "INSERT INTO src VALUES (11, 0), (12, 0), (13, 0), (14, 0), (15, 0)".to_owned(),
            format!(
                "CREATE TRIGGER tr {} {event} ON t BEGIN {writes}\
                 SELECT CASE WHEN {row_ref}.k = {} THEN {raise} END; END",
                self.timing,
                key_base + self.fail_at,
            ),
        ];
        if self.explicit_transaction {
            setup.push("BEGIN".to_owned());
            setup.push("INSERT INTO t VALUES (9, 0)".to_owned());
        }
        Script {
            label: format!("{self:?}"),
            setup,
            statement: statement.to_owned(),
            after: observations(self.explicit_transaction),
        }
    }
}

fn raise_scripts() -> Vec<Script> {
    let mut scripts = Vec::new();
    for kind in [
        Kind::Delete,
        Kind::Update,
        Kind::InsertValues,
        Kind::InsertSelect,
    ] {
        for timing in ["BEFORE", "AFTER"] {
            for action in ["FAIL", "ABORT", "ROLLBACK", "IGNORE"] {
                for fail_at in [1, 3] {
                    for log_writes in [0, 2] {
                        for explicit_transaction in [true, false] {
                            let case = RaiseCase {
                                kind,
                                timing,
                                action,
                                fail_at,
                                log_writes,
                                explicit_transaction,
                            };
                            scripts.push(case.script());
                        }
                    }
                }
            }
        }
    }
    scripts
}

/// A multi-row statement whose third row conflicts with an existing row.
fn conflict_scripts() -> Vec<Script> {
    let statements = [
        "INSERT <or>INTO t VALUES (21, 0, 21), (22, 0, 22), (1, 0, 31), (23, 0, 23)",
        "INSERT <or>INTO t SELECT k + 20, v, u + 20 FROM t WHERE k < 3 \
         UNION ALL SELECT 2, 0, 32 UNION ALL SELECT 24, 0, 24",
        "UPDATE <or>t SET u = CASE WHEN k = 3 THEN 5 ELSE u + 10 END WHERE k <= 5",
    ];
    let mut scripts = Vec::new();
    for statement in statements {
        for action in ["", "ABORT", "FAIL", "ROLLBACK", "IGNORE", "REPLACE"] {
            for with_trigger in [false, true] {
                for explicit_transaction in [true, false] {
                    let or = if action.is_empty() {
                        String::new()
                    } else {
                        format!("OR {action} ")
                    };
                    let mut setup = vec![
                        "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER, u INTEGER UNIQUE)"
                            .to_owned(),
                        "CREATE TABLE log(id INTEGER PRIMARY KEY, k, tag)".to_owned(),
                        "INSERT INTO t VALUES (1, 0, 1), (2, 0, 2), (3, 0, 3), (4, 0, 4), \
                         (5, 0, 5)"
                            .to_owned(),
                    ];
                    if with_trigger {
                        setup.push(
                            "CREATE TRIGGER ti AFTER INSERT ON t BEGIN \
                             INSERT INTO log(k, tag) VALUES (NEW.k, 'i'); END"
                                .to_owned(),
                        );
                        setup.push(
                            "CREATE TRIGGER tu AFTER UPDATE ON t BEGIN \
                             INSERT INTO log(k, tag) VALUES (NEW.k, 'u'); END"
                                .to_owned(),
                        );
                    }
                    if explicit_transaction {
                        setup.push("BEGIN".to_owned());
                        setup.push("INSERT INTO t VALUES (9, 0, 9)".to_owned());
                    }
                    scripts.push(Script {
                        label: format!(
                            "conflict `{statement}` action={action:?} \
                             with_trigger={with_trigger} \
                             explicit_transaction={explicit_transaction}"
                        ),
                        setup,
                        statement: statement.replace("<or>", &or),
                        after: observations(explicit_transaction),
                    });
                }
            }
        }
    }
    scripts
}

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Integer(v) => format!("i:{v}"),
        SqliteValue::Float(v) => format!("f:{v}"),
        SqliteValue::Text(v) => format!("t:{v}"),
        SqliteValue::Blob(v) => format!("b:{:?}", v.to_vec()),
        SqliteValue::Null => "null".to_owned(),
    }
}

fn render_rusqlite(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
        rusqlite::types::ValueRef::Real(v) => format!("f:{v}"),
        rusqlite::types::ValueRef::Text(v) => format!("t:{}", String::from_utf8_lossy(v)),
        rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
        rusqlite::types::ValueRef::Null => "null".to_owned(),
    }
}

fn render_rows(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(render_fsqlite)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn stock_query(conn: &rusqlite::Connection, sql: &str) -> String {
    let run = || -> rusqlite::Result<Vec<String>> {
        let mut stmt = conn.prepare(sql)?;
        let width = stmt.column_count();
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(
                (0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
        Ok(out)
    };
    match run() {
        Ok(rows) => format!("{sql} => ok {rows:?}"),
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => {
            format!("{sql} => error:{message}")
        }
        Err(error) => format!("{sql} => error:{error}"),
    }
}

async fn fsqlite_query(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => format!("{sql} => ok {:?}", render_rows(&rows)),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

fn stock_transcript(script: &Script) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    let mut transcript: Vec<String> = script
        .setup
        .iter()
        .map(|sql| stock_query(&conn, sql))
        .collect();
    transcript.push(stock_query(&conn, "SELECT total_changes()"));
    transcript.push(stock_query(&conn, &script.statement));
    transcript.extend(script.after.iter().map(|sql| stock_query(&conn, sql)));
    transcript
}

async fn fsqlite_transcript(script: &Script, path: &str) -> Vec<String> {
    let conn = Connection::open(path).await.expect("fsqlite open");
    let mut transcript = Vec::new();
    for sql in &script.setup {
        transcript.push(fsqlite_query(&conn, sql).await);
    }
    transcript.push(fsqlite_query(&conn, "SELECT total_changes()").await);
    transcript.push(fsqlite_query(&conn, &script.statement).await);
    for sql in &script.after {
        transcript.push(fsqlite_query(&conn, sql).await);
    }
    conn.close().await.expect("fsqlite close");
    transcript
}

/// The first line where the transcripts differ, if any.
fn first_difference(got: &[String], want: &[String]) -> Option<String> {
    got.iter()
        .zip(want)
        .find(|(got, want)| got != want)
        .map(|(got, want)| format!("\n    fsqlite: {got}\n    stock:   {want}"))
        .or_else(|| {
            (got.len() != want.len())
                .then(|| format!("\n    {} lines vs stock's {}", got.len(), want.len()))
        })
}

fn assert_scripts_match_stock(scripts: Vec<Script>, file_backed: bool) {
    let expected: Vec<Vec<String>> = scripts.iter().map(stock_transcript).collect();
    let dir = tempfile::tempdir().expect("temp dir");
    let dir_path = dir.path().to_owned();
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for (index, (script, want)) in scripts.iter().zip(&expected).enumerate() {
            let path = if file_backed {
                dir_path
                    .join(format!("case{index}.db"))
                    .to_str()
                    .expect("utf-8 path")
                    .to_owned()
            } else {
                ":memory:".to_owned()
            };
            let got = fsqlite_transcript(script, &path).await;
            if let Some(difference) = first_difference(&got, want) {
                mismatches.push(format!("{}:{difference}", script.label));
            }
        }
        for mismatch in &mismatches {
            eprintln!("event=gh448_mismatch file_backed={file_backed} {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} cases differ from stock (file_backed={file_backed}); first: {}",
            mismatches.len(),
            scripts.len(),
            mismatches[0]
        );
    });
}

#[test]
fn gh448_issue_reproduction_matches_stock() {
    let setup = [
        "CREATE TABLE t(k INTEGER PRIMARY KEY)",
        "CREATE TABLE log(k)",
        "INSERT INTO t VALUES(1),(2),(3)",
        "CREATE TRIGGER td BEFORE DELETE ON t BEGIN INSERT INTO log VALUES(OLD.k); \
         SELECT CASE WHEN OLD.k=2 THEN RAISE(FAIL,'stop') END; END",
        "BEGIN",
        "INSERT INTO t VALUES(4)",
        "DELETE FROM t WHERE k<=3",
        "SELECT changes()",
        "SELECT k FROM t ORDER BY k",
        "SELECT k FROM log ORDER BY rowid",
        "COMMIT",
        "SELECT k FROM t ORDER BY k",
    ];
    let stock = rusqlite::Connection::open_in_memory().expect("stock open");
    let want: Vec<String> = setup.iter().map(|sql| stock_query(&stock, sql)).collect();
    assert_eq!(want[7], "SELECT changes() => ok [\"i:1\"]", "the stock oracle");
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("fsqlite open");
        let mut got = Vec::new();
        for sql in setup {
            got.push(fsqlite_query(&conn, sql).await);
        }
        assert_eq!(got, want);
    });
}

#[test]
fn trigger_raise_change_counters_match_stock_in_memory() {
    assert_scripts_match_stock(raise_scripts(), false);
}

#[test]
fn trigger_raise_change_counters_match_stock_file_backed() {
    assert_scripts_match_stock(raise_scripts(), true);
}

#[test]
fn conflict_change_counters_match_stock_in_memory() {
    assert_scripts_match_stock(conflict_scripts(), false);
}

#[test]
fn conflict_change_counters_match_stock_file_backed() {
    assert_scripts_match_stock(conflict_scripts(), true);
}
