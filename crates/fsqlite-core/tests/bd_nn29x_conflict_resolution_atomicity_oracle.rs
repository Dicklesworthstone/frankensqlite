#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! bd-nn29x: which rows a failing statement keeps is decided by the conflict
//! action of the failure itself, not by the statement's `OR` clause.
//!
//! In stock SQLite every halting error carries its own action
//! (`Vdbe.errorAction`): a constraint's `OP_Halt` carries the resolved
//! algorithm of that constraint (statement `OR` clause, else the column or
//! table `ON CONFLICT` clause, else ABORT); everything else (a STRICT datatype
//! error, an FK violation, `RAISE(ABORT)`, a constraint in an UPSERT `DO
//! UPDATE`, which is always coded with ABORT) leaves the default ABORT. Inside
//! a trigger, a non-default outer `OR` clause replaces the trigger statement's
//! own clause. At the statement boundary FAIL keeps the rows written so far,
//! ABORT undoes the statement, and ROLLBACK ends the transaction.
//!
//! Every expectation is a live stock-SQLite oracle (bundled rusqlite): each
//! case runs the same statements in stock and in FrankenSQLite (in-memory,
//! file-backed, and prepared-statement lanes) and compares, per step, the error
//! class and message, then the transaction state, the table contents, the
//! outcome of a trailing COMMIT, and the table contents after it.
//!
//! Known stock artifact deliberately left out: stock opens a statement journal
//! only when its code generator saw an ABORT-capable constraint
//! (`usesStmtJournal = isMultiWrite && mayAbort`). Without one, an error whose
//! action is ABORT but which is not such a constraint (a STRICT datatype
//! error, an integer overflow) cannot undo the statement inside an explicit
//! transaction, so stock keeps its partial rows there. FrankenSQLite always
//! undoes the statement. The explicit-transaction STRICT cases below therefore
//! use statements for which stock does open a statement journal.

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

/// Run every case in stock and in every FrankenSQLite lane, passing both
/// observations through `normalize`; return one line per divergence.
fn divergences(cases: &[Case], normalize: fn(&mut Observation)) -> Vec<String> {
    let mut diffs = Vec::new();
    asupersync::test_utils::run_test(|| async {
        for case in cases {
            let mut stock = stock_observe(case);
            normalize(&mut stock);
            for lane in LANES {
                let mut frank = frank_observe(case, lane).await;
                normalize(&mut frank);
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

fn assert_no_divergences(label: &str, diffs: &[String]) {
    assert!(
        diffs.is_empty(),
        "{label}: {} divergence(s) from stock SQLite:\n{}",
        diffs.len(),
        diffs.join("\n")
    );
}

fn assert_matches_stock(label: &str, cases: &[Case]) {
    assert_no_divergences(label, &divergences(cases, |_| {}));
}

/// The message every `RAISE()` in these cases carries.
const RAISE_MESSAGE: &str = "boom";

/// Compare everything except the primary result code of a `RAISE()` error:
/// stock reports SQLITE_CONSTRAINT (19) for it and FrankenSQLite reports
/// SQLITE_ERROR (1). That code is a separate divergence from the rows,
/// transaction state and message this bead is about, so a `RAISE()` step
/// still has to fail with exactly the stock message.
fn assert_matches_stock_except_raise_result_code(label: &str, cases: &[Case]) {
    fn forget_raise_result_code(observation: &mut Observation) {
        for (_, outcome) in &mut observation.steps {
            if let Outcome::Err { code, message } = outcome
                && message == RAISE_MESSAGE
            {
                *code = 0;
            }
        }
    }
    assert_no_divergences(label, &divergences(cases, forget_raise_result_code));
}

/// Every statement-level conflict clause, plus none at all.
const MODES: [&str; 6] = [
    "OR FAIL",
    "OR ABORT",
    "OR ROLLBACK",
    "OR IGNORE",
    "OR REPLACE",
    "",
];

/// Every constraint-level `ON CONFLICT` algorithm.
const ALGORITHMS: [&str; 5] = ["FAIL", "ABORT", "ROLLBACK", "IGNORE", "REPLACE"];

#[derive(Clone, Copy, Debug)]
enum Txn {
    /// The statement runs in autocommit mode.
    Autocommit,
    /// `BEGIN` and one earlier row precede the statement.
    Explicit,
}

const TXNS: [Txn; 2] = [Txn::Autocommit, Txn::Explicit];

/// `statement` alone in autocommit mode, or preceded by `BEGIN` and `prior`
/// in an explicit transaction.
fn in_txn(txn: Txn, prior: &str, statement: String) -> Vec<String> {
    match txn {
        Txn::Autocommit => vec![statement],
        Txn::Explicit => vec!["BEGIN".to_owned(), prior.to_owned(), statement],
    }
}

fn strs(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

/// Bead case 1: a STRICT datatype error is not a uniqueness / NOT NULL
/// conflict, so the statement's `OR` clause does not apply and the statement
/// aborts. In autocommit mode stock keeps none of the statement's rows, for
/// INSERT and UPDATE alike.
#[test]
fn bd_nn29x_strict_datatype_error_aborts_statement_in_autocommit() {
    let mut cases = Vec::new();
    for mode in MODES {
        cases.push(Case {
            name: format!("autocommit STRICT multi-row INSERT `{mode}`"),
            setup: strs(&["CREATE TABLE s(x INTEGER) STRICT"]),
            steps: vec![format!("INSERT {mode} INTO s VALUES (1), ('bad'), (3)")],
            queries: strs(&["SELECT x FROM s ORDER BY rowid"]),
        });
        cases.push(Case {
            name: format!("autocommit STRICT UPDATE `{mode}`"),
            setup: strs(&[
                "CREATE TABLE s(x INTEGER) STRICT",
                "INSERT INTO s VALUES (1), (2), (3)",
            ]),
            steps: vec![format!(
                "UPDATE {mode} s SET x = CASE x WHEN 2 THEN 'bad' ELSE x + 10 END"
            )],
            queries: strs(&["SELECT x FROM s ORDER BY rowid"]),
        });
    }
    assert_matches_stock("STRICT datatype error in autocommit", &cases);
}

/// The same STRICT failure inside an explicit transaction, in statements for
/// which stock opens a statement journal (see the module note): the statement
/// is undone, the transaction stays open, and COMMIT keeps the earlier row.
#[test]
fn bd_nn29x_strict_datatype_error_aborts_statement_in_explicit_transaction() {
    let mut cases = Vec::new();
    for mode in MODES {
        cases.push(Case {
            name: format!("explicit txn STRICT + RAISE(ABORT) trigger `{mode}`"),
            setup: strs(&[
                "CREATE TABLE s(x INTEGER) STRICT",
                "CREATE TRIGGER s_guard AFTER INSERT ON s WHEN NEW.x < 0 \
                 BEGIN SELECT RAISE(ABORT, 'negative'); END",
            ]),
            steps: in_txn(
                Txn::Explicit,
                "INSERT INTO s VALUES (0)",
                format!("INSERT {mode} INTO s VALUES (1), ('bad'), (3)"),
            ),
            queries: strs(&["SELECT x FROM s ORDER BY rowid"]),
        });
    }
    for mode in ["OR ABORT", ""] {
        cases.push(Case {
            name: format!("explicit txn STRICT NOT NULL column `{mode}`"),
            setup: strs(&["CREATE TABLE s(x INTEGER NOT NULL) STRICT"]),
            steps: in_txn(
                Txn::Explicit,
                "INSERT INTO s VALUES (0)",
                format!("INSERT {mode} INTO s VALUES (1), ('bad'), (3)"),
            ),
            queries: strs(&["SELECT x FROM s ORDER BY rowid"]),
        });
    }
    assert_matches_stock("STRICT datatype error inside BEGIN", &cases);
}

/// Bead cases 2 and 3: a NOT NULL column's own `ON CONFLICT` clause decides
/// the action when the statement has no `OR` clause; a statement `OR` clause
/// overrides it. FAIL keeps the earlier rows of a multi-row INSERT (or the
/// rows an UPDATE already changed), ROLLBACK ends the explicit transaction.
/// The column has no DEFAULT, so REPLACE resolves to ABORT.
#[test]
fn bd_nn29x_not_null_column_conflict_clause_matches_stock() {
    let mut cases = Vec::new();
    for algorithm in ALGORITHMS {
        let table = format!(
            "CREATE TABLE n(id INTEGER PRIMARY KEY, v INTEGER NOT NULL ON CONFLICT {algorithm})"
        );
        for mode in ["", "OR FAIL", "OR ABORT", "OR ROLLBACK", "OR IGNORE"] {
            for txn in TXNS {
                cases.push(Case {
                    name: format!("NOT NULL ON CONFLICT {algorithm}, INSERT `{mode}`, {txn:?}"),
                    setup: vec![table.clone()],
                    steps: in_txn(
                        txn,
                        "INSERT INTO n VALUES (1, 1)",
                        format!("INSERT {mode} INTO n VALUES (2, 2), (3, NULL), (4, 4)"),
                    ),
                    queries: strs(&["SELECT id, v FROM n ORDER BY id"]),
                });
                cases.push(Case {
                    name: format!("NOT NULL ON CONFLICT {algorithm}, UPDATE `{mode}`, {txn:?}"),
                    setup: vec![
                        table.clone(),
                        "INSERT INTO n VALUES (2, 2), (3, 3), (4, 4)".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO n VALUES (1, 1)",
                        format!(
                            "UPDATE {mode} n SET v = CASE id WHEN 3 THEN NULL ELSE v + 10 END \
                             WHERE id >= 2"
                        ),
                    ),
                    queries: strs(&["SELECT id, v FROM n ORDER BY id"]),
                });
            }
        }
    }
    assert_matches_stock("NOT NULL ON CONFLICT", &cases);
}

/// Controls: the same rule for UNIQUE and INTEGER PRIMARY KEY constraints that
/// declare their own `ON CONFLICT` algorithm, under every statement clause.
#[test]
fn bd_nn29x_unique_and_primary_key_conflict_clause_matches_stock() {
    let mut cases = Vec::new();
    for algorithm in ALGORITHMS {
        for mode in MODES {
            for txn in TXNS {
                cases.push(Case {
                    name: format!("UNIQUE ON CONFLICT {algorithm}, INSERT `{mode}`, {txn:?}"),
                    setup: vec![
                        format!(
                            "CREATE TABLE u(id INTEGER PRIMARY KEY, v INTEGER UNIQUE ON CONFLICT {algorithm})"
                        ),
                        "INSERT INTO u VALUES (1, 1)".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO u VALUES (5, 5)",
                        format!("INSERT {mode} INTO u VALUES (2, 2), (3, 1), (4, 4)"),
                    ),
                    queries: strs(&["SELECT id, v FROM u ORDER BY id"]),
                });
                cases.push(Case {
                    name: format!(
                        "INTEGER PRIMARY KEY ON CONFLICT {algorithm}, INSERT `{mode}`, {txn:?}"
                    ),
                    setup: vec![
                        format!(
                            "CREATE TABLE k(id INTEGER PRIMARY KEY ON CONFLICT {algorithm}, v TEXT)"
                        ),
                        "INSERT INTO k VALUES (1, 'a')".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO k VALUES (5, 'e')",
                        format!("INSERT {mode} INTO k VALUES (2, 'b'), (1, 'dup'), (3, 'c')"),
                    ),
                    queries: strs(&["SELECT id, v FROM k ORDER BY id"]),
                });
            }
        }
    }
    assert_matches_stock("UNIQUE / PRIMARY KEY ON CONFLICT", &cases);
}

/// Bead case 4a: an UPSERT `DO UPDATE` is always coded with ABORT, so a
/// constraint failure inside it undoes the whole statement whatever the outer
/// clause says (OR FAIL keeps nothing; OR ROLLBACK keeps the transaction). A
/// constraint failure in the INSERT half still follows the outer clause.
#[test]
fn bd_nn29x_upsert_do_update_failure_aborts_statement() {
    let mut cases = Vec::new();
    for mode in MODES {
        for txn in TXNS {
            cases.push(Case {
                name: format!("DO UPDATE NOT NULL failure `{mode}`, {txn:?}"),
                setup: strs(&[
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
                    "INSERT INTO t VALUES (5, 5)",
                ]),
                steps: in_txn(
                    txn,
                    "INSERT INTO t VALUES (0, 0)",
                    format!(
                        "INSERT {mode} INTO t VALUES (1, 1), (5, 6), (2, 2) \
                         ON CONFLICT(id) DO UPDATE SET v = NULL"
                    ),
                ),
                queries: strs(&["SELECT id, v FROM t ORDER BY id"]),
            });
            cases.push(Case {
                name: format!("DO UPDATE UNIQUE failure `{mode}`, {txn:?}"),
                setup: strs(&[
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT UNIQUE)",
                    "INSERT INTO t VALUES (5, 'a'), (6, 'b')",
                ]),
                steps: in_txn(
                    txn,
                    "INSERT INTO t VALUES (0, 'z')",
                    format!(
                        "INSERT {mode} INTO t VALUES (1, 'x'), (5, 'y'), (2, 'w') \
                         ON CONFLICT(id) DO UPDATE SET u = 'b'"
                    ),
                ),
                queries: strs(&["SELECT id, u FROM t ORDER BY id"]),
            });
        }
    }
    for mode in ["OR FAIL", "OR ABORT", ""] {
        for txn in TXNS {
            cases.push(Case {
                name: format!("UPSERT insert-half NOT NULL failure `{mode}`, {txn:?}"),
                setup: strs(&[
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
                    "INSERT INTO t VALUES (5, 5)",
                ]),
                steps: in_txn(
                    txn,
                    "INSERT INTO t VALUES (0, 0)",
                    format!(
                        "INSERT {mode} INTO t VALUES (1, 1), (5, 6), (2, NULL) \
                         ON CONFLICT(id) DO UPDATE SET v = excluded.v + 100"
                    ),
                ),
                queries: strs(&["SELECT id, v FROM t ORDER BY id"]),
            });
        }
    }
    for mode in ["OR FAIL", "OR ROLLBACK", "OR REPLACE", ""] {
        for txn in TXNS {
            cases.push(Case {
                name: format!("WITHOUT ROWID DO UPDATE NOT NULL failure `{mode}`, {txn:?}"),
                setup: strs(&[
                    "CREATE TABLE w(id INTEGER PRIMARY KEY, v INTEGER NOT NULL) WITHOUT ROWID",
                    "INSERT INTO w VALUES (5, 5)",
                ]),
                steps: in_txn(
                    txn,
                    "INSERT INTO w VALUES (0, 0)",
                    format!(
                        "INSERT {mode} INTO w VALUES (1, 1), (5, 6), (2, 2) \
                         ON CONFLICT(id) DO UPDATE SET v = NULL"
                    ),
                ),
                queries: strs(&["SELECT id, v FROM w ORDER BY id"]),
            });
        }
    }
    assert_matches_stock("UPSERT DO UPDATE failure", &cases);
}

/// Bead case 4b: `RAISE(ABORT)` in a trigger undoes the whole statement under
/// every outer clause, OR FAIL included; `RAISE(FAIL)` keeps the work done so
/// far, also under every clause. BEFORE and AFTER triggers on INSERT VALUES,
/// INSERT ... SELECT and UPDATE, plus the `CASE WHEN ... THEN RAISE()` form.
/// The RAISE result code is excluded (see
/// `assert_matches_stock_except_raise_result_code`).
#[test]
fn bd_nn29x_trigger_raise_action_overrides_statement_clause() {
    let mut cases = Vec::new();
    for mode in MODES {
        for txn in TXNS {
            cases.push(Case {
                name: format!("AFTER INSERT CASE-form RAISE(ABORT) `{mode}`, {txn:?}"),
                setup: strs(&[
                    "CREATE TABLE t(x INTEGER PRIMARY KEY)",
                    "CREATE TABLE log(x)",
                    "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
                     INSERT INTO log VALUES (NEW.x); \
                     SELECT CASE WHEN NEW.x = 2 THEN RAISE(ABORT, 'boom') END; END",
                ]),
                steps: in_txn(
                    txn,
                    "INSERT INTO t VALUES (0)",
                    format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                ),
                queries: strs(&[
                    "SELECT x FROM t ORDER BY x",
                    "SELECT x FROM log ORDER BY rowid",
                ]),
            });
        }
    }
    for raise in ["ABORT", "FAIL"] {
        for timing in ["BEFORE", "AFTER"] {
            for mode in MODES {
                for txn in TXNS {
                    cases.push(Case {
                        name: format!("{timing} INSERT RAISE({raise}) `{mode}`, {txn:?}"),
                        setup: vec![
                            "CREATE TABLE t(x INTEGER PRIMARY KEY)".to_owned(),
                            "CREATE TABLE log(x)".to_owned(),
                            format!(
                                "CREATE TRIGGER tr {timing} INSERT ON t BEGIN \
                                 INSERT INTO log VALUES (NEW.x); \
                                 SELECT RAISE({raise}, 'boom') WHERE NEW.x = 2; END"
                            ),
                        ],
                        steps: in_txn(
                            txn,
                            "INSERT INTO t VALUES (0)",
                            format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                        ),
                        queries: strs(&[
                            "SELECT x FROM t ORDER BY x",
                            "SELECT x FROM log ORDER BY rowid",
                        ]),
                    });
                    cases.push(Case {
                        name: format!(
                            "{timing} INSERT RAISE({raise}) INSERT ... SELECT `{mode}`, {txn:?}"
                        ),
                        setup: vec![
                            "CREATE TABLE t(x INTEGER PRIMARY KEY)".to_owned(),
                            "CREATE TABLE log(x)".to_owned(),
                            "CREATE TABLE src(x)".to_owned(),
                            "INSERT INTO src VALUES (1), (2), (3)".to_owned(),
                            format!(
                                "CREATE TRIGGER tr {timing} INSERT ON t BEGIN \
                                 INSERT INTO log VALUES (NEW.x); \
                                 SELECT RAISE({raise}, 'boom') WHERE NEW.x = 2; END"
                            ),
                        ],
                        steps: in_txn(
                            txn,
                            "INSERT INTO t VALUES (0)",
                            format!("INSERT {mode} INTO t SELECT x FROM src ORDER BY x"),
                        ),
                        queries: strs(&[
                            "SELECT x FROM t ORDER BY x",
                            "SELECT x FROM log ORDER BY rowid",
                        ]),
                    });
                    cases.push(Case {
                        name: format!("{timing} UPDATE RAISE({raise}) `{mode}`, {txn:?}"),
                        setup: vec![
                            "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER)".to_owned(),
                            "CREATE TABLE log(x)".to_owned(),
                            "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)".to_owned(),
                            format!(
                                "CREATE TRIGGER tr {timing} UPDATE ON t BEGIN \
                                 INSERT INTO log VALUES (NEW.x); \
                                 SELECT RAISE({raise}, 'boom') WHERE NEW.x = 12; END"
                            ),
                        ],
                        steps: in_txn(
                            txn,
                            "INSERT INTO t VALUES (0, 0)",
                            format!("UPDATE {mode} t SET x = x + 10 WHERE id > 0"),
                        ),
                        queries: strs(&[
                            "SELECT id, x FROM t ORDER BY id",
                            "SELECT x FROM log ORDER BY rowid",
                        ]),
                    });
                }
            }
        }
    }
    assert_matches_stock_except_raise_result_code("trigger RAISE", &cases);
}

/// Bead case 5: a trigger statement's own `OR` clause applies only when the
/// outer statement has none; a non-default outer clause replaces it. So with
/// an outer `INSERT OR FAIL ... VALUES (1), (2), (3)` an AFTER INSERT trigger's
/// failing `INSERT OR ABORT` resolves with FAIL and keeps outer rows 1 and 2.
#[test]
fn bd_nn29x_trigger_statement_conflict_clause_follows_outer_clause() {
    let mut cases = Vec::new();
    for inner in ["OR ABORT", "OR FAIL", "OR IGNORE", "OR ROLLBACK", ""] {
        for mode in MODES {
            for txn in TXNS {
                cases.push(Case {
                    name: format!("trigger `INSERT {inner}` under outer `{mode}`, {txn:?}"),
                    setup: vec![
                        "CREATE TABLE t(x INTEGER PRIMARY KEY)".to_owned(),
                        "CREATE TABLE log(x INTEGER UNIQUE)".to_owned(),
                        "INSERT INTO log VALUES (2)".to_owned(),
                        format!(
                            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
                             INSERT {inner} INTO log VALUES (NEW.x); END"
                        ),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO t VALUES (0)",
                        format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                    ),
                    queries: strs(&["SELECT x FROM t ORDER BY x", "SELECT x FROM log ORDER BY x"]),
                });
            }
        }
    }
    // The clause a trigger passes on is the firing statement's: a DELETE has
    // none (so the triggers it fires keep their own clauses), and an UPSERT's
    // DO UPDATE fires its UPDATE triggers under ABORT.
    for mode in ["OR FAIL", "OR IGNORE", ""] {
        for txn in TXNS {
            cases.push(Case {
                name: format!("DELETE in a trigger under outer `{mode}`, {txn:?}"),
                setup: strs(&[
                    "CREATE TABLE t(x INTEGER PRIMARY KEY)",
                    "CREATE TABLE d(x INTEGER PRIMARY KEY)",
                    "CREATE TABLE log(x INTEGER UNIQUE)",
                    "INSERT INTO log VALUES (2)",
                    "INSERT INTO d VALUES (1), (2), (3)",
                    "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
                     DELETE FROM d WHERE x = NEW.x; END",
                    "CREATE TRIGGER td AFTER DELETE ON d BEGIN \
                     INSERT INTO log VALUES (OLD.x); END",
                ]),
                steps: in_txn(
                    txn,
                    "INSERT INTO t VALUES (0)",
                    format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                ),
                queries: strs(&[
                    "SELECT x FROM t ORDER BY x",
                    "SELECT x FROM d ORDER BY x",
                    "SELECT x FROM log ORDER BY x",
                ]),
            });
            cases.push(Case {
                name: format!("DO UPDATE-fired trigger under outer `{mode}`, {txn:?}"),
                setup: strs(&[
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                    "CREATE TABLE log(x INTEGER UNIQUE)",
                    "INSERT INTO log VALUES (6)",
                    "INSERT INTO t VALUES (5, 5)",
                    "CREATE TRIGGER tu AFTER UPDATE ON t BEGIN \
                     INSERT INTO log VALUES (NEW.v); END",
                ]),
                steps: in_txn(
                    txn,
                    "INSERT INTO t VALUES (0, 0)",
                    format!(
                        "INSERT {mode} INTO t VALUES (1, 1), (5, 6), (2, 2) \
                         ON CONFLICT(id) DO UPDATE SET v = excluded.v"
                    ),
                ),
                queries: strs(&[
                    "SELECT id, v FROM t ORDER BY id",
                    "SELECT x FROM log ORDER BY x",
                ]),
            });
        }
    }
    assert_matches_stock("trigger statement conflict clause", &cases);
}

/// FK actions that rewrite child rows (ON UPDATE CASCADE, SET NULL, SET
/// DEFAULT) are coded by stock as an UPDATE trigger step under OE_Abort
/// (`sqlite3FkActions` -> `sqlite3CodeRowTriggerDirect(.., OE_Abort, ..)`).
/// So a child constraint the action violates aborts the statement whatever
/// the child column's own ON CONFLICT clause or the outer statement's clause
/// says, and the child's UPDATE triggers run their INSERT/UPDATE statements
/// under ABORT. Once a failure carries its own algorithm, an action run
/// without that override applied the child column's FAIL / ROLLBACK /
/// IGNORE / REPLACE instead.
#[test]
fn bd_nn29x_fk_action_child_update_resolves_with_abort() {
    let mut cases = Vec::new();
    for algorithm in ALGORITHMS {
        for txn in TXNS {
            for mode in MODES {
                cases.push(Case {
                    name: format!(
                        "ON UPDATE SET NULL into child NOT NULL ON CONFLICT {algorithm}, \
                         outer `UPDATE {mode}`, {txn:?}"
                    ),
                    setup: vec![
                        "PRAGMA foreign_keys = ON".to_owned(),
                        "CREATE TABLE p(id INTEGER PRIMARY KEY, tag TEXT)".to_owned(),
                        format!(
                            "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER NOT NULL \
                             ON CONFLICT {algorithm} REFERENCES p(id) ON UPDATE SET NULL)"
                        ),
                        "INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c')".to_owned(),
                        "INSERT INTO c VALUES (1, 1), (2, 2), (3, 3)".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO p VALUES (50, 'pre')",
                        format!("UPDATE {mode} p SET id = id + 100 WHERE id < 50"),
                    ),
                    queries: strs(&[
                        "SELECT id, tag FROM p ORDER BY id",
                        "SELECT id, pid FROM c ORDER BY id",
                    ]),
                });
            }
            cases.push(Case {
                name: format!(
                    "ON DELETE SET DEFAULT into child UNIQUE ON CONFLICT {algorithm}, {txn:?}"
                ),
                setup: vec![
                    "PRAGMA foreign_keys = ON".to_owned(),
                    "CREATE TABLE p(id INTEGER PRIMARY KEY)".to_owned(),
                    format!(
                        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 0 \
                         UNIQUE ON CONFLICT {algorithm} REFERENCES p(id) ON DELETE SET DEFAULT)"
                    ),
                    "INSERT INTO p VALUES (0), (1), (2), (3)".to_owned(),
                    "INSERT INTO c VALUES (10, 0), (11, 1), (12, 2)".to_owned(),
                ],
                steps: in_txn(
                    txn,
                    "INSERT INTO p VALUES (50)",
                    "DELETE FROM p WHERE id IN (1, 3)".to_owned(),
                ),
                queries: strs(&[
                    "SELECT id FROM p ORDER BY id",
                    "SELECT id, pid FROM c ORDER BY id",
                ]),
            });
        }
    }
    for txn in TXNS {
        for mode in MODES {
            cases.push(Case {
                name: format!("ON UPDATE CASCADE into child CHECK, outer `UPDATE {mode}`, {txn:?}"),
                setup: strs(&[
                    "PRAGMA foreign_keys = ON",
                    "CREATE TABLE p(id INTEGER PRIMARY KEY)",
                    "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) \
                     ON UPDATE CASCADE CHECK (pid < 103))",
                    "INSERT INTO p VALUES (1), (2), (3)",
                    "INSERT INTO c VALUES (1, 1), (2, 2), (3, 3)",
                ]),
                steps: in_txn(
                    txn,
                    "INSERT INTO p VALUES (50)",
                    format!("UPDATE {mode} p SET id = id + 100 WHERE id < 50"),
                ),
                queries: strs(&[
                    "SELECT id FROM p ORDER BY id",
                    "SELECT id, pid FROM c ORDER BY id",
                ]),
            });
            for inner in ["OR FAIL", "OR IGNORE", "OR ROLLBACK", "OR REPLACE", ""] {
                cases.push(Case {
                    name: format!(
                        "ON UPDATE CASCADE child trigger `INSERT {inner}`, outer `UPDATE {mode}`, {txn:?}"
                    ),
                    setup: vec![
                        "PRAGMA foreign_keys = ON".to_owned(),
                        "CREATE TABLE p(id INTEGER PRIMARY KEY)".to_owned(),
                        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) \
                         ON UPDATE CASCADE)"
                            .to_owned(),
                        "CREATE TABLE log(x INTEGER UNIQUE)".to_owned(),
                        "INSERT INTO log VALUES (102)".to_owned(),
                        "INSERT INTO p VALUES (1), (2), (3)".to_owned(),
                        "INSERT INTO c VALUES (1, 1), (2, 2), (3, 3)".to_owned(),
                        format!(
                            "CREATE TRIGGER ct AFTER UPDATE ON c BEGIN \
                             INSERT {inner} INTO log VALUES (NEW.pid); END"
                        ),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO p VALUES (50)",
                        format!("UPDATE {mode} p SET id = id + 100 WHERE id < 50"),
                    ),
                    queries: strs(&[
                        "SELECT id FROM p ORDER BY id",
                        "SELECT id, pid FROM c ORDER BY id",
                        "SELECT x FROM log ORDER BY x",
                    ]),
                });
            }
        }
    }
    assert_matches_stock("FK action child UPDATE", &cases);
}

/// A WITHOUT ROWID UPDATE that resolves a later row's UNIQUE / PRIMARY KEY
/// conflict with FAIL keeps the rows it already rewrote, as stock does. The
/// WITHOUT ROWID rewrite used to raise that conflict with an `IdxInsert`,
/// whose conflict path deletes every index entry inserted since the last
/// table `Insert`; a WITHOUT ROWID program has no `Insert`, so the earlier
/// rows' new entries were deleted after their old entries were already gone,
/// and those rows vanished.
#[test]
fn bd_nn29x_without_rowid_update_fail_keeps_rewritten_rows() {
    let mut cases = Vec::new();
    for txn in TXNS {
        for (mode, column_clause) in [
            ("OR FAIL", ""),
            ("OR FAIL", " ON CONFLICT ABORT"),
            ("OR FAIL", " ON CONFLICT REPLACE"),
            ("", " ON CONFLICT FAIL"),
            ("OR ABORT", " ON CONFLICT FAIL"),
            ("", " ON CONFLICT ROLLBACK"),
        ] {
            cases.push(Case {
                name: format!("WITHOUT ROWID UNIQUE{column_clause}, `UPDATE {mode}`, {txn:?}"),
                setup: vec![
                    format!(
                        "CREATE TABLE w(id INTEGER PRIMARY KEY, v INTEGER UNIQUE{column_clause}) \
                         WITHOUT ROWID"
                    ),
                    "INSERT INTO w VALUES (2, 2), (3, 3), (4, 4), (10, 13)".to_owned(),
                ],
                steps: in_txn(
                    txn,
                    "INSERT INTO w VALUES (1, 1)",
                    format!("UPDATE {mode} w SET v = v + 10 WHERE id BETWEEN 2 AND 4"),
                ),
                queries: strs(&[
                    "SELECT id, v FROM w ORDER BY id",
                    "SELECT id FROM w WHERE v > 0 ORDER BY v",
                ]),
            });
            cases.push(Case {
                name: format!(
                    "WITHOUT ROWID UNIQUE{column_clause}, `UPDATE {mode}` ... FROM, {txn:?}"
                ),
                setup: vec![
                    format!(
                        "CREATE TABLE w(id INTEGER PRIMARY KEY, v INTEGER UNIQUE{column_clause}) \
                         WITHOUT ROWID"
                    ),
                    "CREATE TABLE src(id INTEGER PRIMARY KEY, nv INTEGER)".to_owned(),
                    "INSERT INTO w VALUES (2, 2), (3, 3), (4, 4), (10, 13)".to_owned(),
                    "INSERT INTO src VALUES (2, 12), (3, 13), (4, 14)".to_owned(),
                ],
                steps: in_txn(
                    txn,
                    "INSERT INTO w VALUES (1, 1)",
                    format!("UPDATE {mode} w SET v = src.nv FROM src WHERE src.id = w.id"),
                ),
                queries: strs(&[
                    "SELECT id, v FROM w ORDER BY id",
                    "SELECT id FROM w WHERE v > 0 ORDER BY v",
                ]),
            });
        }
        // The primary key moves onto another row's key.
        cases.push(Case {
            name: format!("WITHOUT ROWID TEXT PRIMARY KEY, `UPDATE OR FAIL`, {txn:?}"),
            setup: strs(&[
                "CREATE TABLE w(k TEXT PRIMARY KEY, v) WITHOUT ROWID",
                "INSERT INTO w VALUES ('b', 1), ('c', 2), ('d', 3), ('cc', 9)",
            ]),
            steps: in_txn(
                txn,
                "INSERT INTO w VALUES ('z', 0)",
                "UPDATE OR FAIL w SET k = k || k WHERE k IN ('b', 'c', 'd')".to_owned(),
            ),
            queries: strs(&["SELECT k, v FROM w ORDER BY k"]),
        });
    }
    assert_matches_stock("WITHOUT ROWID UPDATE FAIL", &cases);
}
