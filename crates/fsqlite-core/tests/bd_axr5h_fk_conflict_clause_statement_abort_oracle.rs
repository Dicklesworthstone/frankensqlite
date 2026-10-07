#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! bd-axr5h: a FOREIGN KEY violation always resolves with ABORT semantics.
//!
//! Stock SQLite ignores the statement's conflict clause for FK errors:
//! `sqlite3VdbeCheckFk` and the immediate-FK `OP_Halt` both set
//! `errorAction = OE_Abort`, so `INSERT OR ROLLBACK` (or OR FAIL, IGNORE,
//! REPLACE) that hits a missing parent only rolls the failing statement back.
//! An explicit transaction stays open with its earlier work intact, and COMMIT
//! still succeeds. FrankenSQLite used to apply `OR ROLLBACK` to every
//! constraint-class error, so an FK failure ended the whole transaction.
//!
//! Every expectation is a live stock-SQLite oracle (bundled rusqlite): each
//! case runs the same statements in stock and in FrankenSQLite (in-memory,
//! file-backed, and prepared-statement lanes) and compares, per step, the error
//! class and message, then the transaction state, the table contents, the
//! outcome of a trailing COMMIT, and the table contents after it.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[derive(Clone, Copy, Debug)]
enum Lane {
    /// `Connection::open(":memory:")`, every step through `execute`.
    Memory,
    /// A file-backed database, every step through `execute`.
    File,
    /// `Connection::open(":memory:")`, every DML step through `prepare` +
    /// `execute` (transaction control, which `prepare` does not take, through
    /// `execute`).
    PreparedMemory,
}

fn is_dml(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    ["INSERT", "UPDATE", "DELETE", "REPLACE"]
        .iter()
        .any(|keyword| head.starts_with(keyword))
}

const LANES: [Lane; 3] = [Lane::Memory, Lane::File, Lane::PreparedMemory];

/// The observable result of one statement: success, or the primary SQLite
/// result code and the error message.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    Err { code: i32, message: String },
}

type Rows = Vec<Vec<String>>;

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    steps: Vec<(String, Outcome)>,
    in_transaction: bool,
    rows: Vec<(String, Rows)>,
    /// A `COMMIT` issued after the steps: succeeds only when the transaction
    /// the steps left behind is still open and committable.
    commit: Outcome,
    rows_after_commit: Vec<(String, Rows)>,
}

struct Case {
    name: String,
    setup: Vec<String>,
    steps: Vec<String>,
    queries: Vec<String>,
}

fn tag_f(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

fn tag_r(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

fn stock_outcome(result: rusqlite::Result<()>) -> Outcome {
    match result {
        Ok(()) => Outcome::Ok,
        Err(rusqlite::Error::SqliteFailure(error, message)) => Outcome::Err {
            code: error.extended_code & 0xff,
            message: message.unwrap_or_default(),
        },
        Err(other) => panic!("unexpected stock error shape: {other:?}"),
    }
}

fn stock_rows(db: &rusqlite::Connection, sql: &str) -> Rows {
    let mut statement = db
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock prepare `{sql}`: {e}"));
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .unwrap_or_else(|e| panic!("stock query `{sql}`: {e}"))
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap_or_else(|e| panic!("stock rows `{sql}`: {e}"))
}

fn stock_observe(case: &Case) -> Observation {
    let db = rusqlite::Connection::open_in_memory().expect("open stock oracle");
    for sql in &case.setup {
        db.execute_batch(sql)
            .unwrap_or_else(|e| panic!("[{}] stock setup `{sql}`: {e}", case.name));
    }
    let steps = case
        .steps
        .iter()
        .map(|sql| (sql.clone(), stock_outcome(db.execute_batch(sql))))
        .collect();
    let in_transaction = !db.is_autocommit();
    let rows = case
        .queries
        .iter()
        .map(|q| (q.clone(), stock_rows(&db, q)))
        .collect();
    let commit = stock_outcome(db.execute_batch("COMMIT"));
    let rows_after_commit = case
        .queries
        .iter()
        .map(|q| (q.clone(), stock_rows(&db, q)))
        .collect();
    Observation {
        steps,
        in_transaction,
        rows,
        commit,
        rows_after_commit,
    }
}

fn frank_outcome<T>(result: fsqlite_error::Result<T>) -> Outcome {
    match result {
        Ok(_) => Outcome::Ok,
        Err(error) => Outcome::Err {
            code: error.error_code() as i32,
            message: error.to_string(),
        },
    }
}

async fn frank_step(conn: &Connection, lane: Lane, sql: &str) -> Outcome {
    match lane {
        Lane::Memory | Lane::File => frank_outcome(conn.execute(sql).await),
        Lane::PreparedMemory if !is_dml(sql) => frank_outcome(conn.execute(sql).await),
        Lane::PreparedMemory => match conn.prepare(sql).await {
            Ok(statement) => frank_outcome(statement.execute().await),
            Err(error) => frank_outcome::<()>(Err(error)),
        },
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Rows {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite query `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

async fn frank_observe(case: &Case, lane: Lane) -> Observation {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = match lane {
        Lane::Memory | Lane::PreparedMemory => ":memory:".to_owned(),
        Lane::File => dir.path().join("case.db").to_string_lossy().into_owned(),
    };
    let conn = Connection::open(path).await.expect("open fsqlite");
    for sql in &case.setup {
        conn.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("[{}] fsqlite setup `{sql}`: {e}", case.name));
    }
    let mut steps = Vec::with_capacity(case.steps.len());
    for sql in &case.steps {
        steps.push((sql.clone(), frank_step(&conn, lane, sql).await));
    }
    let in_transaction = conn.in_transaction();
    let mut rows = Vec::with_capacity(case.queries.len());
    for q in &case.queries {
        rows.push((q.clone(), frank_rows(&conn, q).await));
    }
    let commit = frank_outcome(conn.execute("COMMIT").await);
    let mut rows_after_commit = Vec::with_capacity(case.queries.len());
    for q in &case.queries {
        rows_after_commit.push((q.clone(), frank_rows(&conn, q).await));
    }
    Observation {
        steps,
        in_transaction,
        rows,
        commit,
        rows_after_commit,
    }
}

/// Run every case in stock and in every FrankenSQLite lane; return one line
/// per divergence.
fn divergences(cases: &[Case]) -> Vec<String> {
    let mut diffs = Vec::new();
    asupersync::test_utils::run_test(|| async {
        for case in cases {
            let stock = stock_observe(case);
            for lane in LANES {
                let frank = frank_observe(case, lane).await;
                if frank != stock {
                    diffs.push(format!(
                        "[{} / {lane:?}]\n    fsqlite = {frank:?}\n    stock   = {stock:?}",
                        case.name
                    ));
                }
            }
        }
    });
    diffs
}

fn assert_matches_stock(label: &str, cases: &[Case]) {
    let diffs = divergences(cases);
    assert!(
        diffs.is_empty(),
        "{label}: {} divergence(s) from stock SQLite:\n{}",
        diffs.len(),
        diffs.join("\n")
    );
}

/// Every statement-level conflict clause, plus none at all.
const MODES: [&str; 6] = [
    "OR ROLLBACK",
    "OR ABORT",
    "OR FAIL",
    "OR IGNORE",
    "OR REPLACE",
    "",
];

fn fk_schema(deferred: bool) -> Vec<String> {
    let deferrable = if deferred {
        " DEFERRABLE INITIALLY DEFERRED"
    } else {
        ""
    };
    vec![
        "PRAGMA foreign_keys = ON".to_owned(),
        "CREATE TABLE p(id INTEGER PRIMARY KEY)".to_owned(),
        format!("CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id){deferrable})"),
        "INSERT INTO p VALUES (1), (2)".to_owned(),
    ]
}

fn fk_queries() -> Vec<String> {
    vec![
        "SELECT id, pid FROM c ORDER BY id".to_owned(),
        "SELECT id FROM p ORDER BY id".to_owned(),
    ]
}

fn case(name: String, setup: Vec<String>, steps: &[String], queries: Vec<String>) -> Case {
    Case {
        name,
        setup,
        steps: steps.to_vec(),
        queries,
    }
}

/// The reported repro: inside BEGIN, a one-row `INSERT OR ROLLBACK` naming a
/// missing parent. Stock aborts only the statement; the transaction stays
/// open, a later statement still runs, and COMMIT keeps the earlier row.
#[test]
fn bd_axr5h_insert_or_rollback_fk_violation_keeps_explicit_transaction_open() {
    let steps = [
        "BEGIN".to_owned(),
        "INSERT INTO c VALUES (10, 1)".to_owned(),
        "INSERT OR ROLLBACK INTO c VALUES (11, 99)".to_owned(),
        "INSERT INTO c VALUES (12, 2)".to_owned(),
    ];
    let cases = [case(
        "begin; insert or rollback missing parent; continue".to_owned(),
        fk_schema(false),
        &steps,
        fk_queries(),
    )];
    assert_matches_stock("bd-axr5h reported repro", &cases);
}

/// Every conflict clause on every DML shape that can hit an immediate FK
/// violation inside an explicit transaction: single-row VALUES, multi-row
/// VALUES (the violating row is in the middle), INSERT ... SELECT, and UPDATE.
#[test]
fn bd_axr5h_fk_violation_ignores_statement_conflict_clause_in_explicit_transaction() {
    let mut cases = Vec::new();
    for mode in MODES {
        let shapes = [
            (
                "single-row",
                format!("INSERT {mode} INTO c VALUES (11, 99)"),
            ),
            (
                "multi-row",
                format!("INSERT {mode} INTO c VALUES (11, 1), (12, 99), (13, 2)"),
            ),
            (
                "insert-select",
                format!(
                    "INSERT {mode} INTO c SELECT id + 20, CASE id WHEN 2 THEN 99 ELSE id END \
                     FROM p ORDER BY id"
                ),
            ),
            (
                "update",
                format!("UPDATE {mode} c SET pid = pid + 98 WHERE id = 10"),
            ),
        ];
        for (shape, statement) in shapes {
            let steps = [
                "BEGIN".to_owned(),
                "INSERT INTO c VALUES (10, 1)".to_owned(),
                statement,
                "INSERT INTO c VALUES (14, 2)".to_owned(),
            ];
            cases.push(case(
                format!("explicit txn, {shape}, `{mode}`"),
                fk_schema(false),
                &steps,
                fk_queries(),
            ));
        }
    }
    assert_matches_stock("FK violation inside BEGIN", &cases);
}

/// The same statements in autocommit mode: stock rolls back the implicit
/// transaction (nothing of the failing statement survives) under every clause,
/// including OR FAIL, because the FK error's action is ABORT.
#[test]
fn bd_axr5h_fk_violation_ignores_statement_conflict_clause_in_autocommit() {
    let mut cases = Vec::new();
    for mode in MODES {
        for (shape, statement) in [
            (
                "single-row",
                format!("INSERT {mode} INTO c VALUES (11, 99)"),
            ),
            (
                "multi-row",
                format!("INSERT {mode} INTO c VALUES (11, 1), (12, 99), (13, 2)"),
            ),
            (
                "update",
                format!("UPDATE {mode} c SET pid = pid + 98 WHERE id = 10"),
            ),
        ] {
            let steps = [
                "INSERT INTO c VALUES (10, 1)".to_owned(),
                statement,
                "INSERT INTO c VALUES (14, 2)".to_owned(),
            ];
            cases.push(case(
                format!("autocommit, {shape}, `{mode}`"),
                fk_schema(false),
                &steps,
                fk_queries(),
            ));
        }
    }
    assert_matches_stock("FK violation in autocommit", &cases);
}

/// Controls that must keep working: `OR ROLLBACK` on a genuine UNIQUE / NOT
/// NULL conflict still ends the explicit transaction, and a deferred FK
/// violation under `OR ROLLBACK` is not a statement error at all (it surfaces
/// at COMMIT, which fails and leaves the transaction open).
#[test]
fn bd_axr5h_or_rollback_controls_match_stock() {
    let mut cases = Vec::new();
    let unique_steps = [
        "BEGIN".to_owned(),
        "INSERT INTO c VALUES (10, 1)".to_owned(),
        "INSERT OR ROLLBACK INTO c VALUES (11, 2), (10, 1)".to_owned(),
    ];
    cases.push(case(
        "explicit txn, OR ROLLBACK on a PRIMARY KEY conflict".to_owned(),
        fk_schema(false),
        &unique_steps,
        fk_queries(),
    ));
    let mut not_null_setup = fk_schema(false);
    not_null_setup.push("CREATE TABLE n(id INTEGER PRIMARY KEY, v NOT NULL)".to_owned());
    let not_null_steps = [
        "BEGIN".to_owned(),
        "INSERT INTO c VALUES (10, 1)".to_owned(),
        "INSERT OR ROLLBACK INTO n VALUES (1, 1), (2, NULL)".to_owned(),
    ];
    let mut not_null_queries = fk_queries();
    not_null_queries.push("SELECT id, v FROM n ORDER BY id".to_owned());
    cases.push(case(
        "explicit txn, OR ROLLBACK on a NOT NULL conflict".to_owned(),
        not_null_setup,
        &not_null_steps,
        not_null_queries,
    ));
    let deferred_steps = [
        "BEGIN".to_owned(),
        "INSERT INTO c VALUES (10, 1)".to_owned(),
        "INSERT OR ROLLBACK INTO c VALUES (11, 99)".to_owned(),
    ];
    cases.push(case(
        "explicit txn, OR ROLLBACK with a deferred FK".to_owned(),
        fk_schema(true),
        &deferred_steps,
        fk_queries(),
    ));
    let fk_then_unique = [
        "BEGIN".to_owned(),
        "INSERT INTO c VALUES (10, 1)".to_owned(),
        "INSERT OR ROLLBACK INTO c VALUES (11, 99)".to_owned(),
        "INSERT OR ROLLBACK INTO c VALUES (12, 2), (10, 2)".to_owned(),
    ];
    cases.push(case(
        "explicit txn, FK abort then a PRIMARY KEY OR ROLLBACK".to_owned(),
        fk_schema(false),
        &fk_then_unique,
        fk_queries(),
    ));
    assert_matches_stock("OR ROLLBACK controls", &cases);
}
