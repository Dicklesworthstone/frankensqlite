#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! bd-nn29x / bd-axr5h review keepers: wider stock-oracle coverage of the
//! per-failure conflict algorithm and of the trigger `OR`-clause inheritance
//! than `bd_nn29x_conflict_resolution_atomicity_oracle` has.
//!
//! - Trigger bodies: UPDATE-fired triggers, BEFORE triggers, body UPDATE
//!   statements, inner `OR REPLACE`, two trigger levels (the clause flows from
//!   the outermost statement that has one; a DELETE in between passes none),
//!   NOT NULL columns with their own algorithm inside a body, and
//!   RAISE(IGNORE / FAIL / ABORT / ROLLBACK) next to an explicit-clause body
//!   INSERT.
//! - Constraint clauses: NOT NULL / UNIQUE in WITHOUT ROWID tables, rowid
//!   TEXT PRIMARY KEY, table-level UNIQUE(a, b), UPDATE of UNIQUE and INTEGER
//!   PRIMARY KEY, CHECK under every statement clause.
//! - UPDATE of a NOT NULL ON CONFLICT IGNORE column skips the row (BEFORE
//!   trigger fired, AFTER trigger not, index and integrity intact), in every
//!   UPDATE lane.
//! - Deferred FKs under every clause; an immediate FK violation counted before
//!   a constraint failure whose own algorithm is FAIL or ROLLBACK.
//! - The recorded per-failure algorithm is never applied to a later error
//!   with the same message, across `execute`, `prepare` + `execute` (fresh
//!   and reused handles) and `Connection::query`; savepoints.
//! - Implicit REPLACE victim DELETE triggers inherit REPLACE even when a
//!   primary-key or UNIQUE constraint chose it, for body INSERT and UPDATE.
//!
//! Every expectation is a live stock-SQLite oracle (bundled rusqlite): each
//! case runs the same statements in stock and in FrankenSQLite and compares,
//! per step, the primary result code and message, then the transaction state,
//! the table contents, the outcome of a trailing COMMIT and the table contents
//! after it.
//!
//! Left out, each a pre-existing divergence tracked by its own bead: a column
//! `ON CONFLICT` on a WITHOUT ROWID PRIMARY KEY (it resolves with the statement
//! clause only); TEMP tables;
//! the RAISE() result code and RAISE(ROLLBACK)'s autocommit error; the timing
//! of REPLACE victim deletion relative to FK actions; and
//! `OR REPLACE` into a NOT NULL column with a DEFAULT (bd-m3jt4).

use std::collections::HashMap;

use fsqlite_core::connection::{Connection, PreparedStatement};
use fsqlite_types::value::SqliteValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lane {
    /// `Connection::open(":memory:")`, every step through `execute`.
    Memory,
    /// A file-backed database, every step through `execute`.
    File,
    /// Every DML step through a fresh `prepare` + `execute`.
    Prepared,
    /// Every DML step through `prepare` + `execute`, reusing the handle when
    /// the same SQL text runs again.
    PreparedReuse,
    /// Every DML step through `Connection::query`.
    Query,
}

const BASIC_LANES: [Lane; 3] = [Lane::Memory, Lane::File, Lane::Prepared];
const ALL_LANES: [Lane; 5] = [
    Lane::Memory,
    Lane::File,
    Lane::Prepared,
    Lane::PreparedReuse,
    Lane::Query,
];

fn is_dml(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    ["INSERT", "UPDATE", "DELETE", "REPLACE"]
        .iter()
        .any(|keyword| head.starts_with(keyword))
}

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
    /// A `COMMIT` issued after the steps.
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

async fn frank_step<'c>(
    conn: &'c Connection,
    lane: Lane,
    cache: &mut HashMap<String, PreparedStatement<'c>>,
    sql: &str,
) -> Outcome {
    if !is_dml(sql) {
        return frank_outcome(conn.execute(sql).await);
    }
    match lane {
        Lane::Memory | Lane::File => frank_outcome(conn.execute(sql).await),
        Lane::Prepared => match conn.prepare(sql).await {
            Ok(statement) => frank_outcome(statement.execute().await),
            Err(error) => frank_outcome::<()>(Err(error)),
        },
        Lane::PreparedReuse => {
            if !cache.contains_key(sql) {
                match conn.prepare(sql).await {
                    Ok(statement) => {
                        cache.insert(sql.to_owned(), statement);
                    }
                    Err(error) => return frank_outcome::<()>(Err(error)),
                }
            }
            frank_outcome(cache[sql].execute().await)
        }
        Lane::Query => frank_outcome(conn.query(sql).await),
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
        Lane::File => dir.path().join("case.db").to_string_lossy().into_owned(),
        _ => ":memory:".to_owned(),
    };
    let conn = Connection::open(path).await.expect("open fsqlite");
    for sql in &case.setup {
        conn.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("[{}] fsqlite setup `{sql}`: {e}", case.name));
    }
    let mut cache = HashMap::new();
    let mut steps = Vec::with_capacity(case.steps.len());
    for sql in &case.steps {
        steps.push((sql.clone(), frank_step(&conn, lane, &mut cache, sql).await));
    }
    drop(cache);
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

/// Run every case in stock and in every listed lane, passing both
/// observations through `normalize`; fail with one entry per divergence.
fn assert_matches_stock(
    label: &str,
    cases: &[Case],
    lanes: &[Lane],
    normalize: fn(&mut Observation),
) {
    let mut diffs = Vec::new();
    asupersync::test_utils::run_test(|| async {
        for case in cases {
            let mut stock = stock_observe(case);
            normalize(&mut stock);
            for &lane in lanes {
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
    assert!(
        diffs.is_empty(),
        "{label}: {} divergence(s) from stock SQLite over {} case(s):\n{}",
        diffs.len(),
        cases.len(),
        diffs.join("\n")
    );
}

fn no_normalize(_: &mut Observation) {}

/// Every statement-level conflict clause, plus none at all.
const MODES: [&str; 6] = [
    "OR FAIL",
    "OR ABORT",
    "OR ROLLBACK",
    "OR IGNORE",
    "OR REPLACE",
    "",
];
/// Every clause a trigger body statement can carry, plus none at all.
const INNER: [&str; 6] = [
    "OR ABORT",
    "OR FAIL",
    "OR IGNORE",
    "OR ROLLBACK",
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

fn in_txn(txn: Txn, prior: &str, statement: String) -> Vec<String> {
    match txn {
        Txn::Autocommit => vec![statement],
        Txn::Explicit => vec!["BEGIN".to_owned(), prior.to_owned(), statement],
    }
}

fn strs(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

// ---------------------------------------------------------------------------
// Trigger clause inheritance
// ---------------------------------------------------------------------------

/// An UPDATE-fired AFTER trigger whose body INSERT carries every explicit
/// clause, under every outer UPDATE clause.
#[test]
fn review_update_fired_trigger_body_insert_clause() {
    let mut cases = Vec::new();
    for inner in INNER {
        for mode in MODES {
            for txn in TXNS {
                cases.push(Case {
                    name: format!(
                        "AFTER UPDATE body `INSERT {inner}` under `UPDATE {mode}`, {txn:?}"
                    ),
                    setup: vec![
                        "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER)".to_owned(),
                        "CREATE TABLE log(x INTEGER UNIQUE, n INTEGER)".to_owned(),
                        "INSERT INTO log VALUES (12, 0)".to_owned(),
                        "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)".to_owned(),
                        format!(
                            "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN \
                             INSERT {inner} INTO log VALUES (NEW.x, NEW.id); END"
                        ),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO t VALUES (0, 0)",
                        format!("UPDATE {mode} t SET x = x + 10 WHERE id > 0"),
                    ),
                    queries: strs(&[
                        "SELECT id, x FROM t ORDER BY id",
                        "SELECT x, n FROM log ORDER BY x",
                    ]),
                });
            }
        }
    }
    assert_matches_stock(
        "UPDATE-fired trigger body INSERT clause",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

/// An INSERT-fired trigger whose body is an UPDATE with every explicit clause.
#[test]
fn review_trigger_body_update_statement_clause() {
    let mut cases = Vec::new();
    for inner in INNER {
        for mode in MODES {
            for txn in TXNS {
                cases.push(Case {
                    name: format!(
                        "AFTER INSERT body `UPDATE {inner}` under `INSERT {mode}`, {txn:?}"
                    ),
                    setup: vec![
                        "CREATE TABLE t(x INTEGER PRIMARY KEY)".to_owned(),
                        "CREATE TABLE u(id INTEGER PRIMARY KEY, v INTEGER UNIQUE)".to_owned(),
                        "INSERT INTO u VALUES (1, 100), (2, 2)".to_owned(),
                        format!(
                            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
                             UPDATE {inner} u SET v = NEW.x WHERE id = 1; END"
                        ),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO t VALUES (0)",
                        format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                    ),
                    queries: strs(&[
                        "SELECT x FROM t ORDER BY x",
                        "SELECT id, v FROM u ORDER BY id",
                    ]),
                });
            }
        }
    }
    assert_matches_stock(
        "trigger body UPDATE clause",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

/// A BEFORE INSERT trigger whose body INSERT carries every explicit clause.
#[test]
fn review_before_insert_trigger_body_clause() {
    let mut cases = Vec::new();
    for inner in INNER {
        for mode in MODES {
            for txn in TXNS {
                cases.push(Case {
                    name: format!(
                        "BEFORE INSERT body `INSERT {inner}` under `INSERT {mode}`, {txn:?}"
                    ),
                    setup: vec![
                        "CREATE TABLE t(x INTEGER PRIMARY KEY)".to_owned(),
                        "CREATE TABLE log(x INTEGER UNIQUE, n INTEGER)".to_owned(),
                        "INSERT INTO log VALUES (2, 0)".to_owned(),
                        format!(
                            "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN \
                             INSERT {inner} INTO log VALUES (NEW.x, NEW.x * 10); END"
                        ),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO t VALUES (0)",
                        format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                    ),
                    queries: strs(&[
                        "SELECT x FROM t ORDER BY x",
                        "SELECT x, n FROM log ORDER BY x",
                    ]),
                });
            }
        }
    }
    assert_matches_stock(
        "BEFORE INSERT trigger body clause",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

/// Two trigger levels: the clause flows from the outermost statement that has
/// one; a DELETE in between passes none, and its trigger's own clause flows on.
#[test]
fn review_nested_trigger_clause_inheritance() {
    let mut cases = Vec::new();
    let three_tables = [
        "CREATE TABLE t(x INTEGER PRIMARY KEY)",
        "CREATE TABLE u(x INTEGER PRIMARY KEY)",
        "CREATE TABLE v(x INTEGER UNIQUE)",
        "INSERT INTO v VALUES (2)",
    ];
    let three_queries = [
        "SELECT x FROM t ORDER BY x",
        "SELECT x FROM u ORDER BY x",
        "SELECT x FROM v ORDER BY x",
    ];
    for inner in INNER {
        for mode in MODES {
            for txn in TXNS {
                let mut setup = strs(&three_tables);
                setup.push(
                    "CREATE TRIGGER tt AFTER INSERT ON t BEGIN INSERT INTO u VALUES (NEW.x); END"
                        .to_owned(),
                );
                setup.push(format!(
                    "CREATE TRIGGER tu AFTER INSERT ON u BEGIN \
                     INSERT {inner} INTO v VALUES (NEW.x); END"
                ));
                cases.push(Case {
                    name: format!(
                        "t -> u -> v: outer `INSERT {mode}`, v body `INSERT {inner}`, {txn:?}"
                    ),
                    setup,
                    steps: in_txn(
                        txn,
                        "INSERT INTO t VALUES (0)",
                        format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                    ),
                    queries: strs(&three_queries),
                });
            }
        }
        for txn in TXNS {
            let mut setup = strs(&three_tables);
            setup.push(format!(
                "CREATE TRIGGER tt AFTER INSERT ON t BEGIN \
                 INSERT {inner} INTO u VALUES (NEW.x); END"
            ));
            setup.push(
                "CREATE TRIGGER tu AFTER INSERT ON u BEGIN INSERT INTO v VALUES (NEW.x); END"
                    .to_owned(),
            );
            cases.push(Case {
                name: format!(
                    "t -> u -> v: outer default, middle `INSERT {inner}`, v body default, {txn:?}"
                ),
                setup,
                steps: in_txn(
                    txn,
                    "INSERT INTO t VALUES (0)",
                    "INSERT INTO t VALUES (1), (2), (3)".to_owned(),
                ),
                queries: strs(&three_queries),
            });
            let mut setup = strs(&three_tables);
            setup.push("INSERT INTO t VALUES (1), (2), (3)".to_owned());
            setup.push(format!(
                "CREATE TRIGGER tt AFTER DELETE ON t BEGIN \
                 INSERT {inner} INTO u VALUES (OLD.x); END"
            ));
            setup.push(
                "CREATE TRIGGER tu AFTER INSERT ON u BEGIN INSERT INTO v VALUES (NEW.x); END"
                    .to_owned(),
            );
            cases.push(Case {
                name: format!("DELETE t -> `INSERT {inner}` u -> default v, {txn:?}"),
                setup,
                steps: in_txn(
                    txn,
                    "INSERT INTO t VALUES (0)",
                    "DELETE FROM t WHERE x > 0".to_owned(),
                ),
                queries: strs(&three_queries),
            });
        }
        for mode in MODES {
            cases.push(Case {
                name: format!("`INSERT {mode}` t -> DELETE d -> d body `INSERT {inner}` log"),
                setup: vec![
                    "CREATE TABLE t(x INTEGER PRIMARY KEY)".to_owned(),
                    "CREATE TABLE d(x INTEGER PRIMARY KEY)".to_owned(),
                    "CREATE TABLE log(x INTEGER UNIQUE)".to_owned(),
                    "INSERT INTO log VALUES (2)".to_owned(),
                    "INSERT INTO d VALUES (1), (2), (3)".to_owned(),
                    "CREATE TRIGGER tr AFTER INSERT ON t BEGIN DELETE FROM d WHERE x = NEW.x; END"
                        .to_owned(),
                    format!(
                        "CREATE TRIGGER td AFTER DELETE ON d BEGIN \
                         INSERT {inner} INTO log VALUES (OLD.x); END"
                    ),
                ],
                steps: vec![
                    "BEGIN".to_owned(),
                    "INSERT INTO t VALUES (0)".to_owned(),
                    format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                ],
                queries: strs(&[
                    "SELECT x FROM t ORDER BY x",
                    "SELECT x FROM d ORDER BY x",
                    "SELECT x FROM log ORDER BY x",
                ]),
            });
        }
    }
    assert_matches_stock(
        "nested trigger clause inheritance",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

/// bd-ygcuj: the implicit DELETE uses REPLACE whether the statement or the
/// violated constraint selected it. Both INSERT and UPDATE statements in
/// its trigger body inherit that algorithm, overriding their own clauses.
#[test]
fn review_replace_victim_trigger_inherits_constraint_algorithm() {
    let mut cases = Vec::new();
    for (constraint, schema, replacement) in [
        (
            "primary key",
            "CREATE TABLE t(id INTEGER PRIMARY KEY ON CONFLICT REPLACE, k INTEGER, v TEXT)",
            "INSERT INTO t VALUES (1, 11, 'new-a'), (2, 22, 'new-b')",
        ),
        (
            "unique",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER UNIQUE ON CONFLICT REPLACE, v TEXT)",
            "INSERT INTO t VALUES (4, 11, 'new-a'), (5, 22, 'new-b')",
        ),
    ] {
        for timing in ["BEFORE", "AFTER"] {
            for inner in INNER {
                for body_is_update in [false, true] {
                    let body = if body_is_update {
                        format!(
                            "UPDATE {inner} log SET x = OLD.id, marker = OLD.v WHERE id = 0"
                        )
                    } else {
                        format!(
                            "INSERT {inner} INTO log VALUES (OLD.id, OLD.id, OLD.v)"
                        )
                    };
                    for txn in TXNS {
                        cases.push(Case {
                            name: format!(
                                "{constraint} REPLACE, {timing} DELETE `{body}`, {txn:?}"
                            ),
                            setup: vec![
                                "PRAGMA recursive_triggers = ON".to_owned(),
                                schema.to_owned(),
                                "CREATE TABLE log(id INTEGER PRIMARY KEY, x INTEGER UNIQUE, \
                                 marker TEXT)"
                                    .to_owned(),
                                "INSERT INTO log VALUES (0, 0, 'zero'), (2, 2, 'original')"
                                    .to_owned(),
                                "INSERT INTO t VALUES (1, 11, 'a'), (2, 22, 'b'), (3, 33, 'c')"
                                    .to_owned(),
                                format!(
                                    "CREATE TRIGGER td {timing} DELETE ON t BEGIN {body}; END"
                                ),
                            ],
                            steps: in_txn(
                                txn,
                                "INSERT INTO t VALUES (9, 99, 'prior')",
                                replacement.to_owned(),
                            ),
                            queries: strs(&[
                                "SELECT id, k, v FROM t ORDER BY id",
                                "SELECT id, x, marker FROM log ORDER BY id",
                                "PRAGMA integrity_check",
                            ]),
                        });
                    }
                }
            }
        }
    }
    assert_matches_stock(
        "REPLACE victim DELETE trigger conflict inheritance",
        &cases,
        &ALL_LANES,
        no_normalize,
    );
}

/// The victim's clause ends when its DELETE triggers return. The outer
/// INSERT's own AFTER trigger and later statements retain their clauses.
#[test]
fn review_replace_victim_conflict_override_stays_local() {
    let cases = TXNS.map(|txn| Case {
        name: format!("REPLACE victim override is local, {txn:?}"),
        setup: strs(&[
            "PRAGMA recursive_triggers = ON",
            "CREATE TABLE t(id INTEGER PRIMARY KEY ON CONFLICT REPLACE, v TEXT)",
            "CREATE TABLE deleted(id INTEGER UNIQUE, v TEXT)",
            "CREATE TABLE inserted(id INTEGER UNIQUE)",
            "INSERT INTO t VALUES (1, 'old')",
            "INSERT INTO deleted VALUES (1, 'before')",
            "INSERT INTO inserted VALUES (1)",
            "CREATE TRIGGER td AFTER DELETE ON t BEGIN \
             INSERT OR ABORT INTO deleted VALUES (OLD.id, OLD.v); END",
            "CREATE TRIGGER ti AFTER INSERT ON t BEGIN \
             INSERT INTO inserted VALUES (NEW.id); END",
        ]),
        steps: in_txn(
            txn,
            "INSERT INTO t VALUES (2, 'prior')",
            "INSERT INTO t VALUES (1, 'new')".to_owned(),
        ),
        queries: strs(&[
            "SELECT id, v FROM t ORDER BY id",
            "SELECT id, v FROM deleted ORDER BY id",
            "SELECT id FROM inserted ORDER BY id",
            "PRAGMA integrity_check",
        ]),
    });
    assert_matches_stock(
        "REPLACE victim clause does not escape into the outer INSERT trigger",
        &cases,
        &ALL_LANES,
        no_normalize,
    );
}

/// RAISE(IGNORE / FAIL / ABORT / ROLLBACK) after an explicit-clause body
/// INSERT, directly and one trigger level down, under every outer clause.
#[test]
fn review_trigger_raise_matrix() {
    const RAISE_MESSAGE: &str = "review-raise";
    let mut cases = Vec::new();
    for raise in ["IGNORE", "FAIL", "ABORT", "ROLLBACK"] {
        let raise_expr = if raise == "IGNORE" {
            "RAISE(IGNORE)".to_owned()
        } else {
            format!("RAISE({raise}, '{RAISE_MESSAGE}')")
        };
        for timing in ["BEFORE", "AFTER"] {
            for mode in MODES {
                for txn in TXNS {
                    cases.push(Case {
                        name: format!(
                            "{timing} INSERT {raise_expr} after INSERT OR FAIL, outer `{mode}`, {txn:?}"
                        ),
                        setup: vec![
                            "CREATE TABLE t(x INTEGER PRIMARY KEY)".to_owned(),
                            "CREATE TABLE log(x INTEGER UNIQUE)".to_owned(),
                            format!(
                                "CREATE TRIGGER tr {timing} INSERT ON t BEGIN \
                                 INSERT OR FAIL INTO log VALUES (NEW.x); \
                                 SELECT {raise_expr} WHERE NEW.x = 2; \
                                 INSERT INTO log VALUES (NEW.x + 100); END"
                            ),
                        ],
                        steps: in_txn(
                            txn,
                            "INSERT INTO t VALUES (0)",
                            format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                        ),
                        queries: strs(&["SELECT x FROM t ORDER BY x", "SELECT x FROM log ORDER BY x"]),
                    });
                    cases.push(Case {
                        name: format!("nested {timing} {raise_expr}, outer `{mode}`, {txn:?}"),
                        setup: vec![
                            "CREATE TABLE t(x INTEGER PRIMARY KEY)".to_owned(),
                            "CREATE TABLE u(x INTEGER PRIMARY KEY)".to_owned(),
                            "CREATE TRIGGER tt AFTER INSERT ON t BEGIN \
                             INSERT INTO u VALUES (NEW.x); END"
                                .to_owned(),
                            format!(
                                "CREATE TRIGGER tu {timing} INSERT ON u BEGIN \
                                 SELECT {raise_expr} WHERE NEW.x = 2; END"
                            ),
                        ],
                        steps: in_txn(
                            txn,
                            "INSERT INTO t VALUES (0)",
                            format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                        ),
                        queries: strs(&[
                            "SELECT x FROM t ORDER BY x",
                            "SELECT x FROM u ORDER BY x",
                        ]),
                    });
                }
            }
        }
    }
    assert_matches_stock(
        "trigger RAISE matrix",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

/// A trigger body INSERT that hits a NOT NULL column with its own algorithm,
/// under every outer clause.
#[test]
fn review_trigger_body_not_null_column_clause() {
    let mut cases = Vec::new();
    for algorithm in ALGORITHMS {
        for inner in ["", "OR FAIL"] {
            for mode in MODES {
                for txn in TXNS {
                    cases.push(Case {
                        name: format!(
                            "body `INSERT {inner}` into NOT NULL ON CONFLICT {algorithm}, \
                             outer `{mode}`, {txn:?}"
                        ),
                        setup: vec![
                            "CREATE TABLE t(x INTEGER PRIMARY KEY)".to_owned(),
                            format!(
                                "CREATE TABLE nn(id INTEGER PRIMARY KEY, \
                                 v INTEGER NOT NULL ON CONFLICT {algorithm})"
                            ),
                            format!(
                                "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
                                 INSERT {inner} INTO nn VALUES (NEW.x, \
                                 CASE WHEN NEW.x = 2 THEN NULL ELSE NEW.x END); END"
                            ),
                        ],
                        steps: in_txn(
                            txn,
                            "INSERT INTO t VALUES (0)",
                            format!("INSERT {mode} INTO t VALUES (1), (2), (3)"),
                        ),
                        queries: strs(&[
                            "SELECT x FROM t ORDER BY x",
                            "SELECT id, v FROM nn ORDER BY id",
                        ]),
                    });
                }
            }
        }
    }
    assert_matches_stock(
        "trigger body NOT NULL column clause",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

// ---------------------------------------------------------------------------
// Column- and table-level ON CONFLICT clauses
// ---------------------------------------------------------------------------

/// NOT NULL / UNIQUE in WITHOUT ROWID tables, rowid TEXT PRIMARY KEY,
/// table-level UNIQUE(a, b), UPDATE of UNIQUE and of INTEGER PRIMARY KEY;
/// every statement clause; autocommit and BEGIN.
#[test]
fn review_column_and_table_level_conflict_clauses() {
    let mut cases = Vec::new();
    for algorithm in ALGORITHMS {
        for mode in MODES {
            // The constraint's own clause decides only when the statement
            // has none; ABORT is the default either way.
            let own_clause_decides = mode.is_empty() && algorithm != "ABORT";
            for txn in TXNS {
                let tag = format!("{algorithm}, `{mode}`, {txn:?}");
                cases.push(Case {
                    name: format!("WITHOUT ROWID NOT NULL ON CONFLICT {tag}, INSERT"),
                    setup: vec![format!(
                        "CREATE TABLE w(id INTEGER PRIMARY KEY, \
                         v INTEGER NOT NULL ON CONFLICT {algorithm}) WITHOUT ROWID"
                    )],
                    steps: in_txn(
                        txn,
                        "INSERT INTO w VALUES (1, 1)",
                        format!("INSERT {mode} INTO w VALUES (2, 2), (3, NULL), (4, 4)"),
                    ),
                    queries: strs(&["SELECT id, v FROM w ORDER BY id"]),
                });
                cases.push(Case {
                    name: format!("WITHOUT ROWID NOT NULL ON CONFLICT {tag}, UPDATE"),
                    setup: vec![
                        format!(
                            "CREATE TABLE w(id INTEGER PRIMARY KEY, \
                             v INTEGER NOT NULL ON CONFLICT {algorithm}) WITHOUT ROWID"
                        ),
                        "INSERT INTO w VALUES (2, 2), (3, 3), (4, 4)".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO w VALUES (1, 1)",
                        format!(
                            "UPDATE {mode} w SET v = CASE id WHEN 3 THEN NULL ELSE v + 10 END \
                             WHERE id >= 2"
                        ),
                    ),
                    queries: strs(&["SELECT id, v FROM w ORDER BY id"]),
                });
                cases.push(Case {
                    name: format!("WITHOUT ROWID UNIQUE ON CONFLICT {tag}, INSERT"),
                    setup: vec![
                        format!(
                            "CREATE TABLE w(id INTEGER PRIMARY KEY, \
                             v INTEGER UNIQUE ON CONFLICT {algorithm}) WITHOUT ROWID"
                        ),
                        "INSERT INTO w VALUES (10, 1)".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO w VALUES (5, 5)",
                        format!("INSERT {mode} INTO w VALUES (2, 2), (3, 1), (4, 4)"),
                    ),
                    queries: strs(&["SELECT id, v FROM w ORDER BY id"]),
                });
                cases.push(Case {
                    name: format!("WITHOUT ROWID UNIQUE ON CONFLICT {tag}, UPDATE"),
                    setup: vec![
                        format!(
                            "CREATE TABLE w(id INTEGER PRIMARY KEY, \
                             v INTEGER UNIQUE ON CONFLICT {algorithm}) WITHOUT ROWID"
                        ),
                        "INSERT INTO w VALUES (2, 2), (3, 3), (4, 4), (10, 13)".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO w VALUES (1, 1)",
                        format!("UPDATE {mode} w SET v = v + 10 WHERE id BETWEEN 2 AND 4"),
                    ),
                    queries: strs(&["SELECT id, v FROM w ORDER BY id"]),
                });
                // Pre-existing: a WITHOUT ROWID PRIMARY KEY's own clause is
                // not applied; only the statement clause is.
                if !own_clause_decides {
                    cases.push(Case {
                        name: format!("WITHOUT ROWID TEXT PRIMARY KEY ON CONFLICT {tag}, UPDATE"),
                        setup: vec![
                            format!(
                                "CREATE TABLE w(k TEXT PRIMARY KEY ON CONFLICT {algorithm}, v) \
                                 WITHOUT ROWID"
                            ),
                            "INSERT INTO w VALUES ('b', 1), ('c', 2), ('d', 3), ('cc', 9)"
                                .to_owned(),
                        ],
                        steps: in_txn(
                            txn,
                            "INSERT INTO w VALUES ('z', 0)",
                            format!("UPDATE {mode} w SET k = k || k WHERE k IN ('b', 'c', 'd')"),
                        ),
                        queries: strs(&["SELECT k, v FROM w ORDER BY k"]),
                    });
                }
                cases.push(Case {
                    name: format!("rowid TEXT PRIMARY KEY ON CONFLICT {tag}, INSERT"),
                    setup: vec![
                        format!("CREATE TABLE r(k TEXT PRIMARY KEY ON CONFLICT {algorithm}, v)"),
                        "INSERT INTO r VALUES ('a', 1)".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO r VALUES ('z', 0)",
                        format!("INSERT {mode} INTO r VALUES ('b', 2), ('a', 3), ('c', 4)"),
                    ),
                    queries: strs(&["SELECT k, v FROM r ORDER BY k"]),
                });
                cases.push(Case {
                    name: format!("rowid UNIQUE ON CONFLICT {tag}, UPDATE"),
                    setup: vec![
                        format!(
                            "CREATE TABLE u(id INTEGER PRIMARY KEY, \
                             v INTEGER UNIQUE ON CONFLICT {algorithm})"
                        ),
                        "INSERT INTO u VALUES (2, 2), (3, 3), (4, 4), (10, 13)".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO u VALUES (1, 1)",
                        format!("UPDATE {mode} u SET v = v + 10 WHERE id BETWEEN 2 AND 4"),
                    ),
                    queries: strs(&["SELECT id, v FROM u ORDER BY id"]),
                });
                for (operation, statement) in [
                    (
                        "UPDATE",
                        format!("UPDATE {mode} k SET id = id + 10 WHERE id BETWEEN 2 AND 4"),
                    ),
                    (
                        "UPDATE FROM",
                        format!(
                            "UPDATE {mode} k SET id = moves.new_id FROM moves \
                             WHERE k.id = moves.old_id"
                        ),
                    ),
                ] {
                    cases.push(Case {
                        name: format!(
                            "rowid INTEGER PRIMARY KEY ON CONFLICT {tag}, {operation}"
                        ),
                        setup: vec![
                            format!(
                                "CREATE TABLE k(id INTEGER PRIMARY KEY ON CONFLICT {algorithm}, \
                                 v TEXT)"
                            ),
                            "INSERT INTO k VALUES (2, 'b'), (3, 'c'), (4, 'd'), (13, 'x')"
                                .to_owned(),
                            "CREATE TABLE moves(old_id INTEGER PRIMARY KEY, new_id INTEGER)"
                                .to_owned(),
                            "INSERT INTO moves VALUES (2, 12), (3, 13), (4, 14)".to_owned(),
                        ],
                        steps: in_txn(txn, "INSERT INTO k VALUES (1, 'a')", statement),
                        queries: strs(&["SELECT id, v FROM k ORDER BY id"]),
                    });
                }
                cases.push(Case {
                    name: format!("table-level UNIQUE(a, b) ON CONFLICT {tag}, INSERT"),
                    setup: vec![
                        format!("CREATE TABLE tl(a, b, c, UNIQUE(a, b) ON CONFLICT {algorithm})"),
                        "INSERT INTO tl VALUES (1, 1, 'old')".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO tl VALUES (9, 9, 'pre')",
                        format!(
                            "INSERT {mode} INTO tl VALUES (2, 2, 'x'), (1, 1, 'dup'), (3, 3, 'y')"
                        ),
                    ),
                    queries: strs(&["SELECT a, b, c FROM tl ORDER BY a, b"]),
                });
            }
        }
    }
    assert_matches_stock(
        "column / table level ON CONFLICT",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

/// bd-q51j8: RETURNING exposes only writes that survive the chosen schema or
/// statement action, including an untargeted constraint after an UPSERT probe.
#[test]
fn review_schema_ignore_returning_rows() {
    let primary_key_statements = [
        "INSERT INTO k DEFAULT VALUES RETURNING id, v",
        "INSERT INTO k VALUES (12, 'new12'), (13, 'new13'), (14, 'new14') RETURNING id, v",
        "INSERT INTO k SELECT id, v FROM src RETURNING id, v",
        "INSERT INTO k SELECT 13, 'new13' RETURNING id, v",
        "INSERT INTO k SELECT 12, 'new12' RETURNING id, v",
        "INSERT INTO k VALUES (12, 'new12'), (13, 'new13'), (14, 'new14') \
         ON CONFLICT(v) DO NOTHING RETURNING id, v",
        "INSERT INTO k VALUES (13, 'skip'), (100, 'x'), (14, 'new14') \
         ON CONFLICT(v) DO UPDATE SET v = excluded.v RETURNING id, v",
        "INSERT OR REPLACE INTO k VALUES (12, 'new12'), (13, 'new13'), (14, 'new14') \
         RETURNING id, v",
        "UPDATE k SET id = id + 10 WHERE id BETWEEN 2 AND 4 RETURNING id, v",
        "UPDATE k SET id = moves.new_id FROM moves WHERE k.id = moves.old_id RETURNING id, v",
    ];
    let unique_statements = [
        "INSERT INTO k DEFAULT VALUES RETURNING id, v",
        "INSERT INTO k VALUES (12, 'new12'), (14, 'x'), (15, 'new15') RETURNING id, v",
        "UPDATE k SET v = 'x' WHERE id IN (2, 3) RETURNING id, v",
        "UPDATE k SET v = 'x' FROM moves WHERE k.id = moves.old_id RETURNING id, v",
    ];
    asupersync::test_utils::run_test(|| async {
        for (schema, statements) in [
            (
                "CREATE TABLE k(id INTEGER PRIMARY KEY ON CONFLICT IGNORE DEFAULT 13, v TEXT UNIQUE)",
                primary_key_statements.as_slice(),
            ),
            (
                "CREATE TABLE k(id INTEGER DEFAULT 13, v TEXT UNIQUE, \
                 PRIMARY KEY(id) ON CONFLICT IGNORE)",
                primary_key_statements.as_slice(),
            ),
            (
                "CREATE TABLE k(id INTEGER PRIMARY KEY, v TEXT UNIQUE ON CONFLICT IGNORE DEFAULT 'x')",
                unique_statements.as_slice(),
            ),
        ] {
            for sql in statements {
                for persistent in [false, true] {
                    for prepared in [false, true] {
                        let stock = rusqlite::Connection::open_in_memory().expect("stock");
                        let dir = tempfile::tempdir().expect("tempdir");
                        let path = if persistent {
                            dir.path().join("returning.db").to_string_lossy().into_owned()
                        } else {
                            ":memory:".to_owned()
                        };
                        let conn = Connection::open(path).await.expect("fsqlite");
                        for setup in [
                            schema,
                            "INSERT INTO k VALUES (2, 'b'), (3, 'c'), (4, 'd'), (13, 'x')",
                            "CREATE TABLE src(id INTEGER, v TEXT)",
                            "INSERT INTO src VALUES (12, 'new12'), (13, 'new13'), (14, 'new14')",
                            "CREATE TABLE moves(old_id INTEGER PRIMARY KEY, new_id INTEGER)",
                            "INSERT INTO moves VALUES (2, 12), (3, 13), (4, 14)",
                        ] {
                            stock.execute_batch(setup).expect("stock setup");
                            conn.execute(setup).await.expect("fsqlite setup");
                        }

                        let expected = stock_rows(&stock, sql);
                        let returned = if prepared {
                            let statement = conn.prepare(sql).await.expect("prepare RETURNING");
                            statement.query().await
                        } else {
                            conn.query(sql).await
                        }
                        .unwrap_or_else(|error| panic!("{schema}; {sql}: {error}"));
                        let observed: Rows = returned
                            .iter()
                            .map(|row| row.values().iter().map(tag_f).collect())
                            .collect();
                        assert_eq!(
                            observed, expected,
                            "{schema}; {sql}; persistent={persistent}, prepared={prepared}"
                        );
                        for query in [
                            "SELECT changes()",
                            "SELECT id, v FROM k ORDER BY id",
                            "SELECT id, v FROM k WHERE v >= 'new' ORDER BY v",
                            "PRAGMA integrity_check",
                        ] {
                            assert_eq!(
                                frank_rows(&conn, query).await,
                                stock_rows(&stock, query),
                                "{schema}; {sql}; {query}; persistent={persistent}, prepared={prepared}"
                            );
                        }
                    }
                }
            }
        }
    });
}

/// bd-ipk-default-used-as-rowid-yniw8: a rowid alias does not inherit its
/// declared DEFAULT when omitted. All INSERT sources must request a fresh
/// rowid, while explicit keys and defaults on ordinary columns remain intact.
#[test]
fn review_omitted_rowid_alias_ignores_defaults() {
    asupersync::test_utils::run_test(|| async {
        for schema in [
            "CREATE TABLE k(id INTEGER PRIMARY KEY DEFAULT 13, v TEXT DEFAULT 'ordinary default')",
            "CREATE TABLE k(id INTEGER DEFAULT 13, v TEXT DEFAULT 'ordinary default', PRIMARY KEY(id))",
            "CREATE TABLE k(id INTEGER PRIMARY KEY AUTOINCREMENT DEFAULT 13, v TEXT DEFAULT 'ordinary default')",
            "CREATE TABLE k(id INTEGER PRIMARY KEY DEFAULT 'not an integer', v TEXT DEFAULT 'ordinary default')",
        ] {
            for persistent in [false, true] {
                for prepared in [false, true] {
                    let stock = rusqlite::Connection::open_in_memory().expect("stock");
                    let dir = tempfile::tempdir().expect("tempdir");
                    let path = if persistent {
                        dir.path().join("ipk-default.db").to_string_lossy().into_owned()
                    } else {
                        ":memory:".to_owned()
                    };
                    let conn = Connection::open(path).await.expect("fsqlite");
                    for setup in [
                        schema,
                        "CREATE TABLE src(v TEXT)",
                        "INSERT INTO src VALUES ('source one'), ('source two')",
                    ] {
                        stock.execute_batch(setup).expect("stock setup");
                        conn.execute(setup).await.expect("fsqlite setup");
                    }
                    for sql in [
                        "INSERT INTO k DEFAULT VALUES RETURNING id, v",
                        "INSERT INTO k(v) VALUES ('values') RETURNING id, v",
                        "INSERT INTO k(v) SELECT 'constant select' RETURNING id, v",
                        "INSERT INTO k(v) SELECT v FROM src RETURNING id, v",
                        "INSERT INTO k(id, v) VALUES (NULL, 'explicit null') RETURNING id, v",
                        "INSERT INTO k(rowid, v) VALUES (100, 'explicit rowid') RETURNING id, v",
                        "INSERT INTO k DEFAULT VALUES RETURNING id, v",
                        "DELETE FROM k WHERE id = (SELECT max(id) FROM k) RETURNING id, v",
                        "INSERT INTO k(v) VALUES ('after delete') RETURNING id, v",
                    ] {
                        let expected = stock_rows(&stock, sql);
                        let returned = if prepared {
                            conn.prepare(sql)
                                .await
                                .expect("prepare rowid-default statement")
                                .query()
                                .await
                        } else {
                            conn.query(sql).await
                        }
                        .unwrap_or_else(|error| panic!("{schema}; {sql}: {error}"));
                        let observed: Rows = returned
                            .iter()
                            .map(|row| row.values().iter().map(tag_f).collect())
                            .collect();
                        assert_eq!(
                            observed, expected,
                            "{schema}; {sql}; persistent={persistent}, prepared={prepared}"
                        );
                        for query in [
                            "SELECT changes(), last_insert_rowid()",
                            "SELECT id, v FROM k ORDER BY id",
                            "PRAGMA integrity_check",
                        ] {
                            assert_eq!(
                                frank_rows(&conn, query).await,
                                stock_rows(&stock, query),
                                "{schema}; {sql}; {query}; persistent={persistent}, prepared={prepared}"
                            );
                        }
                    }
                }
            }
        }
    });
}

/// The trigger path builds a candidate NEW row before bytecode runs. It must
/// also leave an omitted rowid alias unset, even if its schema has a DEFAULT.
#[test]
fn review_rowid_alias_default_is_not_a_trigger_new_value() {
    asupersync::test_utils::run_test(|| async {
        for persistent in [false, true] {
            for prepared in [false, true] {
                let stock = rusqlite::Connection::open_in_memory().expect("stock");
                let dir = tempfile::tempdir().expect("tempdir");
                let path = if persistent {
                    dir.path().join("trigger-ipk-default.db").to_string_lossy().into_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(path).await.expect("fsqlite");
                for setup in [
                    "CREATE TABLE k(id INTEGER PRIMARY KEY DEFAULT 13, v TEXT DEFAULT 'ordinary default')",
                    "CREATE TABLE audit(id INTEGER, v TEXT)",
                    "CREATE TABLE src(v TEXT)",
                    "INSERT INTO src VALUES ('source row')",
                    "CREATE TRIGGER k_before BEFORE INSERT ON k WHEN NEW.id = 13 \
                     BEGIN SELECT RAISE(ABORT, 'rowid DEFAULT leaked into NEW'); END",
                    "CREATE TRIGGER k_after AFTER INSERT ON k \
                     BEGIN INSERT INTO audit VALUES (NEW.id, NEW.v); END",
                ] {
                    stock.execute_batch(setup).expect("stock setup");
                    conn.execute(setup).await.expect("fsqlite setup");
                }
                for sql in [
                    "INSERT INTO k DEFAULT VALUES RETURNING id, v",
                    "INSERT INTO k(v) VALUES ('values') RETURNING id, v",
                    "INSERT INTO k(v) SELECT 'constant select' RETURNING id, v",
                    "INSERT INTO k(v) SELECT v FROM src RETURNING id, v",
                    "INSERT INTO k(id, v) VALUES (NULL, 'explicit null') RETURNING id, v",
                    "INSERT INTO k(rowid, v) VALUES (42, 'explicit rowid') RETURNING id, v",
                ] {
                    let expected = stock_rows(&stock, sql);
                    let returned = if prepared {
                        conn.prepare(sql)
                            .await
                            .expect("prepare triggered default insert")
                            .query()
                            .await
                    } else {
                        conn.query(sql).await
                    }
                    .unwrap_or_else(|error| panic!("{sql}: {error}"));
                    let observed: Rows = returned
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect();
                    assert_eq!(
                        observed, expected,
                        "{sql}; persistent={persistent}, prepared={prepared}"
                    );
                    for query in [
                        "SELECT id, v FROM k ORDER BY id",
                        "SELECT id, v FROM audit ORDER BY id",
                    ] {
                        assert_eq!(
                            frank_rows(&conn, query).await,
                            stock_rows(&stock, query),
                            "{sql}; {query}; persistent={persistent}, prepared={prepared}"
                        );
                    }
                }
            }
        }
    });
}

/// An INTEGER PRIMARY KEY is not a rowid alias in a WITHOUT ROWID table,
/// nor when declared with column-level DESC or with the type name INT.
/// Suppressing its DEFAULT there would lose actual primary-key values.
#[test]
fn review_non_alias_primary_key_preserves_defaults() {
    asupersync::test_utils::run_test(|| async {
        for schema in [
            "CREATE TABLE k(id INTEGER PRIMARY KEY DEFAULT 13, v TEXT DEFAULT 'ordinary default') WITHOUT ROWID",
            "CREATE TABLE k(id INTEGER PRIMARY KEY DESC DEFAULT 13, v TEXT DEFAULT 'ordinary default')",
            "CREATE TABLE k(id INT PRIMARY KEY DEFAULT 13, v TEXT DEFAULT 'ordinary default')",
        ] {
            for persistent in [false, true] {
                for prepared in [false, true] {
                    let stock = rusqlite::Connection::open_in_memory().expect("stock");
                    let dir = tempfile::tempdir().expect("tempdir");
                    let path = if persistent {
                        dir.path().join("pk-default.db").to_string_lossy().into_owned()
                    } else {
                        ":memory:".to_owned()
                    };
                    let conn = Connection::open(path).await.expect("fsqlite");
                    for setup in [
                        schema,
                        "CREATE TABLE src(v TEXT)",
                        "INSERT INTO src VALUES ('source one')",
                    ] {
                        stock.execute_batch(setup).expect("stock setup");
                        conn.execute(setup).await.expect("fsqlite setup");
                    }
                    for sql in [
                        "INSERT INTO k DEFAULT VALUES RETURNING id, v",
                        "INSERT OR REPLACE INTO k(v) VALUES ('values') RETURNING id, v",
                        "INSERT OR REPLACE INTO k(v) SELECT 'constant select' RETURNING id, v",
                        "INSERT OR REPLACE INTO k(v) SELECT v FROM src RETURNING id, v",
                    ] {
                        let expected = stock_rows(&stock, sql);
                        let returned = if prepared {
                            conn.prepare(sql)
                                .await
                                .expect("prepare ordinary-default statement")
                                .query()
                                .await
                        } else {
                            conn.query(sql).await
                        }
                        .unwrap_or_else(|error| panic!("{schema}; {sql}: {error}"));
                        let observed: Rows = returned
                            .iter()
                            .map(|row| row.values().iter().map(tag_f).collect())
                            .collect();
                        assert_eq!(
                            observed, expected,
                            "{schema}; {sql}; persistent={persistent}, prepared={prepared}"
                        );
                        assert_eq!(
                            frank_rows(&conn, "SELECT id, v FROM k ORDER BY id").await,
                            stock_rows(&stock, "SELECT id, v FROM k ORDER BY id"),
                            "{schema}; {sql}; persistent={persistent}, prepared={prepared}"
                        );
                    }
                }
            }
        }
    });
}

/// bd-1rkcr: a NULL column with its own IGNORE skips the candidate before
/// CHECK; a non-NULL row violating CHECK must still use the statement action.
#[test]
fn review_without_rowid_not_null_ignore_and_check_order() {
    let mut cases = Vec::new();
    for mode in MODES {
        for txn in TXNS {
            for nullable in ["NULL", "3"] {
                let source_rows = format!("(2, 2, 1), (3, {nullable}, -1), (4, 4, 1)");
                for source in [
                    format!("VALUES {source_rows}"),
                    "SELECT id, nn, checked FROM src".to_owned(),
                ] {
                    cases.push(Case {
                        name: format!(
                            "WITHOUT ROWID NOT NULL IGNORE, CHECK, `{mode}`, {txn:?}, {source}"
                        ),
                        setup: vec![
                            "CREATE TABLE w(id INTEGER PRIMARY KEY, \
                             nn INTEGER NOT NULL ON CONFLICT IGNORE, \
                             checked INTEGER CHECK(checked > 0)) WITHOUT ROWID"
                                .to_owned(),
                            "CREATE TABLE src(id INTEGER, nn INTEGER, checked INTEGER)".to_owned(),
                            format!("INSERT INTO src VALUES {source_rows}"),
                        ],
                        steps: in_txn(
                            txn,
                            "INSERT INTO w VALUES (1, 1, 1)",
                            format!("INSERT {mode} INTO w {source}"),
                        ),
                        queries: strs(&[
                            "SELECT id, nn, checked FROM w ORDER BY id",
                            "PRAGMA integrity_check",
                        ]),
                    });
                }
            }
        }
    }
    assert_matches_stock(
        "WITHOUT ROWID NOT NULL IGNORE and CHECK precedence",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

/// CHECK constraints have no clause of their own: the statement clause (else
/// ABORT) decides; IGNORE skips the row, REPLACE acts as ABORT.
#[test]
fn review_check_constraint_statement_clause() {
    let mut cases = Vec::new();
    for mode in MODES {
        for txn in TXNS {
            for without_rowid in ["", " WITHOUT ROWID"] {
                cases.push(Case {
                    name: format!("CHECK INSERT `{mode}`{without_rowid}, {txn:?}"),
                    setup: vec![format!(
                        "CREATE TABLE c(id INTEGER PRIMARY KEY, x INTEGER CHECK (x < 10)){without_rowid}"
                    )],
                    steps: in_txn(
                        txn,
                        "INSERT INTO c VALUES (1, 1)",
                        format!("INSERT {mode} INTO c VALUES (2, 2), (3, 30), (4, 4)"),
                    ),
                    queries: strs(&["SELECT id, x FROM c ORDER BY id"]),
                });
                cases.push(Case {
                    name: format!("CHECK UPDATE `{mode}`{without_rowid}, {txn:?}"),
                    setup: vec![
                        format!(
                            "CREATE TABLE c(id INTEGER PRIMARY KEY, x INTEGER CHECK (x < 10)){without_rowid}"
                        ),
                        "INSERT INTO c VALUES (2, 2), (3, 3), (4, 4)".to_owned(),
                    ],
                    steps: in_txn(
                        txn,
                        "INSERT INTO c VALUES (1, 1)",
                        format!(
                            "UPDATE {mode} c SET x = CASE id WHEN 3 THEN 30 ELSE x + 1 END \
                             WHERE id >= 2"
                        ),
                    ),
                    queries: strs(&["SELECT id, x FROM c ORDER BY id"]),
                });
            }
        }
    }
    assert_matches_stock(
        "CHECK constraint statement clause",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

// ---------------------------------------------------------------------------
// UPDATE: NOT NULL ON CONFLICT IGNORE skips the row
// ---------------------------------------------------------------------------

/// The skipped row fires its BEFORE trigger but not its AFTER trigger, and
/// leaves the index and the change count consistent, in the plain and the
/// UPDATE ... FROM lanes of rowid and WITHOUT ROWID tables. A NULL into a
/// neighbouring ABORT column still fails, and IGNORE on the earlier column
/// wins when both are NULL.
#[test]
fn review_update_not_null_on_conflict_ignore_skips_row() {
    let mut cases = Vec::new();
    for mode in MODES {
        for without_rowid in ["", " WITHOUT ROWID"] {
            let setup = vec![
                format!(
                    "CREATE TABLE n(id INTEGER PRIMARY KEY, v INTEGER NOT NULL ON CONFLICT IGNORE, \
                     w INTEGER NOT NULL DEFAULT 7){without_rowid}"
                ),
                "CREATE INDEX n_v ON n(v)".to_owned(),
                "CREATE TABLE src(id INTEGER PRIMARY KEY, nv)".to_owned(),
                "CREATE TABLE log(tag, id, v)".to_owned(),
                "INSERT INTO n VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3)".to_owned(),
                "INSERT INTO src VALUES (1, 11), (2, NULL), (3, 13)".to_owned(),
                "CREATE TRIGGER nb BEFORE UPDATE ON n BEGIN \
                 INSERT INTO log VALUES ('before', OLD.id, NEW.v); END"
                    .to_owned(),
                "CREATE TRIGGER na AFTER UPDATE ON n BEGIN \
                 INSERT INTO log VALUES ('after', OLD.id, NEW.v); END"
                    .to_owned(),
            ];
            let queries = strs(&[
                "SELECT id, v, w FROM n ORDER BY id",
                "SELECT tag, id, v FROM log ORDER BY rowid",
                "SELECT id FROM n WHERE v IS NOT NULL ORDER BY v",
                "PRAGMA integrity_check",
            ]);
            cases.push(Case {
                name: format!(
                    "UPDATE `{mode}` NULL into NOT NULL ON CONFLICT IGNORE{without_rowid}"
                ),
                setup: setup.clone(),
                steps: vec![format!(
                    "UPDATE {mode} n SET v = CASE id WHEN 2 THEN NULL ELSE v + 10 END"
                )],
                queries: queries.clone(),
            });
            cases.push(Case {
                name: format!(
                    "UPDATE FROM `{mode}` NULL into NOT NULL ON CONFLICT IGNORE{without_rowid}"
                ),
                setup: setup.clone(),
                steps: vec![format!(
                    "UPDATE {mode} n SET v = src.nv FROM src WHERE src.id = n.id"
                )],
                queries: queries.clone(),
            });
            // `OR REPLACE` into the NOT NULL DEFAULT column is bd-m3jt4.
            if mode != "OR REPLACE" {
                cases.push(Case {
                    name: format!(
                        "UPDATE `{mode}` NULL into ABORT column next to IGNORE column{without_rowid}"
                    ),
                    setup: setup.clone(),
                    steps: vec![
                        "BEGIN".to_owned(),
                        format!("UPDATE {mode} n SET w = CASE id WHEN 2 THEN NULL ELSE w + 10 END"),
                    ],
                    queries: queries.clone(),
                });
            }
            cases.push(Case {
                name: format!("UPDATE `{mode}` NULL into IGNORE then ABORT column{without_rowid}"),
                setup: setup.clone(),
                steps: vec![
                    "BEGIN".to_owned(),
                    format!(
                        "UPDATE {mode} n SET v = CASE id WHEN 2 THEN NULL ELSE v END, \
                         w = CASE id WHEN 2 THEN NULL ELSE w END"
                    ),
                ],
                queries,
            });
        }
    }
    assert_matches_stock(
        "UPDATE NOT NULL ON CONFLICT IGNORE",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

// ---------------------------------------------------------------------------
// FOREIGN KEY errors next to conflict clauses
// ---------------------------------------------------------------------------

/// Deferred FKs under every clause: the statement succeeds, COMMIT fails.
#[test]
fn review_deferred_fk_under_every_clause() {
    let mut cases = Vec::new();
    let schema = [
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE p(id INTEGER PRIMARY KEY)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) \
         DEFERRABLE INITIALLY DEFERRED)",
        "INSERT INTO p VALUES (1)",
    ];
    for mode in MODES {
        for txn in TXNS {
            cases.push(Case {
                name: format!("deferred FK INSERT `{mode}`, {txn:?}"),
                setup: strs(&schema),
                steps: in_txn(
                    txn,
                    "INSERT INTO c VALUES (10, 1)",
                    format!("INSERT {mode} INTO c VALUES (1, 1), (2, 99), (3, 1)"),
                ),
                queries: strs(&["SELECT id, pid FROM c ORDER BY id"]),
            });
            let mut setup = strs(&schema);
            setup.push("INSERT INTO c VALUES (1, 1), (2, 1), (3, 1)".to_owned());
            cases.push(Case {
                name: format!("deferred FK UPDATE `{mode}`, {txn:?}"),
                setup,
                steps: in_txn(
                    txn,
                    "INSERT INTO c VALUES (10, 1)",
                    format!(
                        "UPDATE {mode} c SET pid = CASE id WHEN 2 THEN 99 ELSE pid END \
                         WHERE id < 10"
                    ),
                ),
                queries: strs(&["SELECT id, pid FROM c ORDER BY id"]),
            });
        }
    }
    assert_matches_stock("deferred FK", &cases, &BASIC_LANES, no_normalize);
}

/// An immediate FK violation counted by an earlier row, then a constraint
/// failure whose own algorithm is FAIL or ROLLBACK: stock re-checks the FK
/// counter when a FAIL statement stops, and that FK error (ABORT) wins.
#[test]
fn review_fk_violation_then_column_level_failure() {
    let mut cases = Vec::new();
    for algorithm in ALGORITHMS {
        for mode in ["", "OR FAIL", "OR ROLLBACK", "OR ABORT"] {
            for txn in TXNS {
                for (constraint, bad_row) in [("UNIQUE", "(3, 5, 1)"), ("NOT NULL", "(3, NULL, 1)")]
                {
                    let mut setup = vec![
                        "PRAGMA foreign_keys = ON".to_owned(),
                        "CREATE TABLE p(id INTEGER PRIMARY KEY)".to_owned(),
                        format!(
                            "CREATE TABLE c(id INTEGER PRIMARY KEY, \
                             x INTEGER {constraint} ON CONFLICT {algorithm}, \
                             pid INTEGER REFERENCES p(id))"
                        ),
                        "INSERT INTO p VALUES (1)".to_owned(),
                    ];
                    if constraint == "UNIQUE" {
                        setup.push("INSERT INTO c VALUES (100, 5, 1)".to_owned());
                    }
                    cases.push(Case {
                        name: format!(
                            "orphan row then {constraint} ON CONFLICT {algorithm}, `{mode}`, {txn:?}"
                        ),
                        setup,
                        steps: in_txn(
                            txn,
                            "INSERT INTO c VALUES (10, 10, 1)",
                            format!(
                                "INSERT {mode} INTO c VALUES (1, 1, 1), (2, 2, 99), {bad_row}, (4, 4, 1)"
                            ),
                        ),
                        queries: strs(&["SELECT id, x, pid FROM c ORDER BY id"]),
                    });
                }
            }
        }
    }
    assert_matches_stock(
        "FK then column-level failure",
        &cases,
        &BASIC_LANES,
        no_normalize,
    );
}

// ---------------------------------------------------------------------------
// The recorded algorithm is per failure; entry points; savepoints
// ---------------------------------------------------------------------------

/// A failure recorded with FAIL is never applied to a later error with the
/// same message (and vice versa), whichever entry point runs the statements.
#[test]
fn review_recorded_algorithm_is_per_failure() {
    let mut cases = vec![
        Case {
            name: "column FAIL, then OR ABORT failures with the same message".to_owned(),
            setup: strs(&[
                "CREATE TABLE u(id INTEGER PRIMARY KEY, v INTEGER UNIQUE ON CONFLICT FAIL)",
            ]),
            steps: strs(&[
                "BEGIN",
                "INSERT INTO u VALUES (1, 1), (2, 1)",
                "INSERT OR ABORT INTO u VALUES (7, 7), (8, 1)",
                "INSERT OR ABORT INTO u VALUES (9, 9), (10, 1) RETURNING id",
                "UPDATE OR ABORT u SET v = 1 WHERE id = 7",
                "INSERT INTO u VALUES (11, 11), (12, 1)",
            ]),
            queries: strs(&["SELECT id, v FROM u ORDER BY id"]),
        },
        Case {
            name: "OR ABORT, then column FAIL failures with the same message".to_owned(),
            setup: strs(&[
                "CREATE TABLE u(id INTEGER PRIMARY KEY, v INTEGER UNIQUE ON CONFLICT FAIL)",
            ]),
            steps: strs(&[
                "BEGIN",
                "INSERT INTO u VALUES (1, 1)",
                "INSERT OR ABORT INTO u VALUES (7, 7), (8, 1)",
                "INSERT INTO u VALUES (2, 2), (3, 1)",
                "INSERT INTO u VALUES (4, 4), (5, 1) RETURNING id",
            ]),
            queries: strs(&["SELECT id, v FROM u ORDER BY id"]),
        },
        Case {
            name: "trigger failure with FAIL, then outer ABORT on the same constraint".to_owned(),
            setup: strs(&[
                "CREATE TABLE log(x INTEGER UNIQUE ON CONFLICT FAIL)",
                "CREATE TABLE t(x INTEGER PRIMARY KEY)",
                "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES (NEW.x); END",
                "INSERT INTO log VALUES (2)",
            ]),
            steps: strs(&[
                "BEGIN",
                "INSERT INTO t VALUES (1), (2), (3)",
                "INSERT OR ABORT INTO log VALUES (50), (2)",
                "INSERT INTO t VALUES (4), (5)",
                "INSERT INTO log VALUES (60), (2)",
            ]),
            queries: strs(&["SELECT x FROM t ORDER BY x", "SELECT x FROM log ORDER BY x"]),
        },
        Case {
            name: "one prepared statement failing, succeeding and failing again".to_owned(),
            setup: strs(&[
                "CREATE TABLE u(id INTEGER PRIMARY KEY, v INTEGER UNIQUE ON CONFLICT FAIL)",
                "CREATE TABLE src(v)",
                "INSERT INTO src VALUES (5)",
            ]),
            steps: strs(&[
                "BEGIN",
                "INSERT INTO u SELECT (SELECT coalesce(max(id), 0) + 1 FROM u), v FROM src",
                "INSERT INTO u SELECT (SELECT coalesce(max(id), 0) + 1 FROM u), v FROM src",
                "INSERT OR ABORT INTO u VALUES (50, 50), (51, 5)",
                "UPDATE src SET v = v + 1",
                "INSERT INTO u SELECT (SELECT coalesce(max(id), 0) + 1 FROM u), v FROM src",
                "INSERT INTO u SELECT (SELECT coalesce(max(id), 0) + 1 FROM u), v FROM src",
            ]),
            queries: strs(&["SELECT id, v FROM u ORDER BY id"]),
        },
    ];
    for algorithm in ALGORITHMS {
        let table = format!(
            "CREATE TABLE n(id INTEGER PRIMARY KEY, v INTEGER NOT NULL ON CONFLICT {algorithm})"
        );
        cases.push(Case {
            name: format!("SAVEPOINT as BEGIN, NOT NULL ON CONFLICT {algorithm}"),
            setup: vec![table.clone()],
            steps: strs(&[
                "SAVEPOINT a",
                "INSERT INTO n VALUES (1, 1)",
                "INSERT INTO n VALUES (2, 2), (3, NULL), (4, 4)",
            ]),
            queries: strs(&["SELECT id, v FROM n ORDER BY id"]),
        });
        cases.push(Case {
            name: format!("BEGIN; SAVEPOINT; NOT NULL ON CONFLICT {algorithm}; ROLLBACK TO"),
            setup: vec![table.clone()],
            steps: strs(&[
                "BEGIN",
                "INSERT INTO n VALUES (1, 1)",
                "SAVEPOINT a",
                "INSERT INTO n VALUES (5, 5)",
                "INSERT INTO n VALUES (2, 2), (3, NULL), (4, 4)",
                "ROLLBACK TO a",
                "INSERT INTO n VALUES (6, 6)",
            ]),
            queries: strs(&["SELECT id, v FROM n ORDER BY id"]),
        });
        cases.push(Case {
            name: format!("BEGIN; SAVEPOINT; NOT NULL ON CONFLICT {algorithm}; RELEASE"),
            setup: vec![table],
            steps: strs(&[
                "BEGIN",
                "INSERT INTO n VALUES (1, 1)",
                "SAVEPOINT a",
                "INSERT INTO n VALUES (2, 2), (3, NULL), (4, 4)",
                "RELEASE a",
            ]),
            queries: strs(&["SELECT id, v FROM n ORDER BY id"]),
        });
    }
    assert_matches_stock(
        "recorded algorithm per failure",
        &cases,
        &ALL_LANES,
        no_normalize,
    );
}
