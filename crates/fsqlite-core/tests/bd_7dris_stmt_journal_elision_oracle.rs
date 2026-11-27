#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! bd-7dris: statement-journal elision parity (`usesStmtJournal = isMultiWrite && mayAbort`).
//!
//! Inside an explicit transaction (`BEGIN` or `SAVEPOINT`), stock SQLite opens
//! a statement journal only when its code generator sets `mayAbort` (for
//! example: an ABORT-capable `NOT NULL`/`CHECK`/`UNIQUE`/`WITHOUT ROWID PK`
//! constraint, an `OP_MustBeInt` on an explicit `INTEGER PRIMARY KEY` /
//! `rowid` / `LIMIT`, a non-inline SQL function call, an immediate foreign key
//! with `PRAGMA foreign_keys = ON`, or a trigger / view / `UPSERT DO UPDATE`
//! that itself `mayAbort`).
//!
//! When `usesStmtJournal == 0`, an `OE_Abort` error raised by an opcode that
//! does not call `sqlite3MayAbort()` (notably `OP_TypeCheck` on `STRICT`
//! tables) has no statement journal to roll back: earlier rows written by the
//! statement remain in the open transaction, `changes()` is reset to `0`,
//! `last_insert_rowid()` keeps the last inserted rowid, and the transaction
//! stays open so a later `COMMIT` commits those rows and a later `ROLLBACK` or
//! `ROLLBACK TO sp` undoes them. In autocommit mode the entire implicit
//! transaction rolls back so no partial rows survive.
//!
//! Every case in this suite runs against a live bundled stock SQLite oracle
//! (`rusqlite`) and compares per-step outcomes, `in_transaction`, `changes()`,
//! `total_changes()`, `last_insert_rowid()`, table rows, `PRAGMA
//! integrity_check`, and the outcome + rows after the finishing `COMMIT` or
//! `ROLLBACK` across `:memory:`, file-backed, and prepared-statement lanes.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[derive(Clone, Copy, Debug)]
enum Lane {
    Memory,
    File,
    PreparedMemory,
    PreparedFile,
}

const LANES: [Lane; 4] = [
    Lane::Memory,
    Lane::File,
    Lane::PreparedMemory,
    Lane::PreparedFile,
];

fn is_dml(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    ["INSERT", "UPDATE", "DELETE", "REPLACE", "WITH"]
        .iter()
        .any(|keyword| head.starts_with(keyword))
}

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
    changes: i64,
    total_changes: i64,
    last_insert_rowid: i64,
    rows: Vec<(String, Rows)>,
    integrity_check: Rows,
    finish: Vec<(String, Outcome)>,
    rows_after_finish: Vec<(String, Rows)>,
}

struct Case {
    name: String,
    setup: Vec<String>,
    steps: Vec<String>,
    queries: Vec<String>,
    finish: Vec<String>,
}

impl Case {
    fn new(
        name: impl Into<String>,
        setup: &[&str],
        steps: &[&str],
        queries: &[&str],
        finish: &[&str],
    ) -> Self {
        Self {
            name: name.into(),
            setup: setup.iter().map(|s| (*s).to_owned()).collect(),
            steps: steps.iter().map(|s| (*s).to_owned()).collect(),
            queries: queries.iter().map(|s| (*s).to_owned()).collect(),
            finish: finish.iter().map(|s| (*s).to_owned()).collect(),
        }
    }
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

fn stock_i64(db: &rusqlite::Connection, sql: &str) -> i64 {
    db.query_row(sql, [], |row| row.get(0))
        .unwrap_or_else(|e| panic!("stock scalar `{sql}`: {e}"))
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
    let changes = stock_i64(&db, "SELECT changes()");
    let total_changes = stock_i64(&db, "SELECT total_changes()");
    let last_insert_rowid = stock_i64(&db, "SELECT last_insert_rowid()");
    let rows = case
        .queries
        .iter()
        .map(|q| (q.clone(), stock_rows(&db, q)))
        .collect();
    let integrity_check = stock_rows(&db, "PRAGMA integrity_check");
    let finish = case
        .finish
        .iter()
        .map(|sql| (sql.clone(), stock_outcome(db.execute_batch(sql))))
        .collect();
    let rows_after_finish = case
        .queries
        .iter()
        .map(|q| (q.clone(), stock_rows(&db, q)))
        .collect();
    Observation {
        steps,
        in_transaction,
        changes,
        total_changes,
        last_insert_rowid,
        rows,
        integrity_check,
        finish,
        rows_after_finish,
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
        Lane::PreparedMemory | Lane::PreparedFile if !is_dml(sql) => {
            frank_outcome(conn.execute(sql).await)
        }
        Lane::PreparedMemory | Lane::PreparedFile => match conn.prepare(sql).await {
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

async fn frank_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite scalar `{sql}`: {e}"));
    match rows.first().and_then(|r| r.values().first()) {
        Some(SqliteValue::Integer(n)) => *n,
        other => panic!("expected integer from `{sql}`, got {other:?}"),
    }
}

async fn frank_observe(case: &Case, lane: Lane) -> Observation {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = match lane {
        Lane::Memory | Lane::PreparedMemory => ":memory:".to_owned(),
        Lane::File | Lane::PreparedFile => {
            dir.path().join("case.db").to_string_lossy().into_owned()
        }
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
    let changes = frank_i64(&conn, "SELECT changes()").await;
    let total_changes = frank_i64(&conn, "SELECT total_changes()").await;
    let last_insert_rowid = frank_i64(&conn, "SELECT last_insert_rowid()").await;
    let mut rows = Vec::with_capacity(case.queries.len());
    for q in &case.queries {
        rows.push((q.clone(), frank_rows(&conn, q).await));
    }
    let integrity_check = frank_rows(&conn, "PRAGMA integrity_check").await;
    let mut finish = Vec::with_capacity(case.finish.len());
    for sql in &case.finish {
        finish.push((sql.clone(), frank_outcome(conn.execute(sql).await)));
    }
    let mut rows_after_finish = Vec::with_capacity(case.queries.len());
    for q in &case.queries {
        rows_after_finish.push((q.clone(), frank_rows(&conn, q).await));
    }
    Observation {
        steps,
        in_transaction,
        changes,
        total_changes,
        last_insert_rowid,
        rows,
        integrity_check,
        finish,
        rows_after_finish,
    }
}

fn assert_matches_stock(cases: &[Case]) {
    let mut diffs = Vec::new();
    asupersync::test_utils::run_test(|| async {
        for case in cases {
            let stock = stock_observe(case);
            for lane in LANES {
                let frank = frank_observe(case, lane).await;
                if frank != stock {
                    diffs.push(format!(
                        "[{} / {lane:?}]\n    fsqlite = {frank:#?}\n    stock   = {stock:#?}",
                        case.name
                    ));
                }
            }
        }
    });
    assert!(
        diffs.is_empty(),
        "bd-7dris divergences against bundled stock SQLite {}:\n{}",
        rusqlite::version(),
        diffs.join("\n")
    );
}

#[test]
fn strict_insert_and_update_elide_stmt_journal_across_conflict_clauses_and_txn_modes() {
    let clauses = [
        ("default", ""),
        ("or_abort", "OR ABORT "),
        ("or_fail", "OR FAIL "),
        ("or_rollback", "OR ROLLBACK "),
        ("or_ignore", "OR IGNORE "),
        ("or_replace", "OR REPLACE "),
    ];
    let mut cases = Vec::new();

    for (label, clause) in clauses {
        let insert_sql = format!("INSERT {clause}INTO s VALUES (1), (2), ('bad'), (4)");
        cases.push(Case::new(
            format!("strict_insert_begin_commit_{label}"),
            &["CREATE TABLE s(x INT) STRICT"],
            &["BEGIN", &insert_sql],
            &["SELECT rowid, x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ));
        cases.push(Case::new(
            format!("strict_insert_begin_rollback_{label}"),
            &["CREATE TABLE s(x INT) STRICT"],
            &["BEGIN", &insert_sql],
            &["SELECT rowid, x FROM s ORDER BY rowid"],
            &["ROLLBACK"],
        ));
        cases.push(Case::new(
            format!("strict_insert_savepoint_rollback_to_{label}"),
            &["CREATE TABLE s(x INT) STRICT"],
            &["SAVEPOINT sp", &insert_sql],
            &["SELECT rowid, x FROM s ORDER BY rowid"],
            &["ROLLBACK TO sp", "RELEASE sp"],
        ));
        cases.push(Case::new(
            format!("strict_insert_autocommit_{label}"),
            &["CREATE TABLE s(x INT) STRICT"],
            &[&insert_sql],
            &["SELECT rowid, x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ));

        let update_sql = format!(
            "UPDATE {clause}s SET x = CASE id WHEN 1 THEN 100 WHEN 2 THEN 'bad' ELSE 300 END"
        );
        cases.push(Case::new(
            format!("strict_update_begin_commit_{label}"),
            &[
                "CREATE TABLE s(id INT, x INT) STRICT",
                "CREATE INDEX idx_sx ON s(x)",
                "INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)",
            ],
            &["BEGIN", &update_sql],
            &[
                "SELECT id, x FROM s ORDER BY id",
                "SELECT id FROM s WHERE x = 100 ORDER BY id",
                "SELECT id FROM s WHERE x = 20 ORDER BY id",
            ],
            &["COMMIT"],
        ));
        cases.push(Case::new(
            format!("strict_update_autocommit_{label}"),
            &[
                "CREATE TABLE s(id INT, x INT) STRICT",
                "CREATE INDEX idx_sx ON s(x)",
                "INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)",
            ],
            &[&update_sql],
            &[
                "SELECT id, x FROM s ORDER BY id",
                "SELECT id FROM s WHERE x = 10 ORDER BY id",
            ],
            &["COMMIT"],
        ));
    }

    assert_matches_stock(&cases);
}

#[test]
fn strict_insert_select_cte_temp_and_without_rowid_parity() {
    let mut batch_seed = vec![
        "CREATE TABLE src(k INT, v ANY)".to_owned(),
        "CREATE TABLE s(x INT) STRICT".to_owned(),
    ];
    for i in 1..=35 {
        batch_seed.push(format!("INSERT INTO src VALUES ({i}, {i})"));
    }
    batch_seed.push("INSERT INTO src VALUES (36, 'bad')".to_owned());
    batch_seed.push("INSERT INTO src VALUES (37, 37)".to_owned());
    let batch_seed_refs: Vec<&str> = batch_seed.iter().map(String::as_str).collect();

    let cases = vec![
        Case::new(
            "insert_select_small_elides_stmt_journal",
            &[
                "CREATE TABLE src(k INT, v ANY)",
                "INSERT INTO src VALUES (1, 10), (2, 20), (3, 'bad'), (4, 40)",
                "CREATE TABLE s(x INT) STRICT",
            ],
            &["BEGIN", "INSERT INTO s SELECT v FROM src ORDER BY k"],
            &["SELECT rowid, x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "insert_select_batch32_elides_stmt_journal",
            &batch_seed_refs,
            &["BEGIN", "INSERT INTO s SELECT v FROM src ORDER BY k"],
            &["SELECT count(*), min(x), max(x) FROM s"],
            &["COMMIT"],
        ),
        Case::new(
            "insert_select_with_const_limit_elides",
            &[
                "CREATE TABLE src(k INT, v ANY)",
                "INSERT INTO src VALUES (1, 10), (2, 'bad')",
                "CREATE TABLE s(x INT) STRICT",
            ],
            &["BEGIN", "INSERT INTO s SELECT v FROM src ORDER BY k LIMIT 5"],
            &["SELECT rowid, x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "insert_select_with_expr_limit_sets_may_abort",
            &[
                "CREATE TABLE src(k INT, v ANY)",
                "INSERT INTO src VALUES (1, 10), (2, 'bad')",
                "CREATE TABLE s(x INT) STRICT",
            ],
            &[
                "BEGIN",
                "INSERT INTO s SELECT v FROM src ORDER BY k LIMIT 2 + 3",
            ],
            &["SELECT rowid, x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "insert_select_with_offset_sets_may_abort",
            &[
                "CREATE TABLE src(k INT, v ANY)",
                "INSERT INTO src VALUES (1, 10), (2, 'bad')",
                "CREATE TABLE s(x INT) STRICT",
            ],
            &[
                "BEGIN",
                "INSERT INTO s SELECT v FROM src ORDER BY k LIMIT 5 OFFSET 0",
            ],
            &["SELECT rowid, x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "insert_cte_elides_stmt_journal",
            &["CREATE TABLE s(x INT) STRICT"],
            &[
                "BEGIN",
                "WITH cte(v) AS (VALUES (1), (2), ('bad')) INSERT INTO s SELECT v FROM cte",
            ],
            &["SELECT rowid, x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "temp_strict_table_elides_stmt_journal",
            &["CREATE TEMP TABLE ts(x INT) STRICT"],
            &["BEGIN", "INSERT INTO ts VALUES (1), (2), ('bad')"],
            &["SELECT rowid, x FROM ts ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "temp_strict_table_rollback_undoes_kept_rows",
            &["CREATE TEMP TABLE ts(x INT) STRICT"],
            &["BEGIN", "INSERT INTO ts VALUES (1), (2), ('bad')"],
            &["SELECT rowid, x FROM ts ORDER BY rowid"],
            &["ROLLBACK"],
        ),
        Case::new(
            "without_rowid_strict_insert_default_abort_undoes",
            &["CREATE TABLE wr(k INT PRIMARY KEY, x INT) WITHOUT ROWID, STRICT"],
            &["BEGIN", "INSERT INTO wr VALUES (1, 10), (2, 'bad')"],
            &["SELECT k, x FROM wr ORDER BY k"],
            &["COMMIT"],
        ),
        Case::new(
            "without_rowid_strict_insert_or_fail_elides",
            &["CREATE TABLE wr(k INT PRIMARY KEY, x INT) WITHOUT ROWID, STRICT"],
            &["BEGIN", "INSERT OR FAIL INTO wr VALUES (1, 10), (2, 'bad')"],
            &["SELECT k, x FROM wr ORDER BY k"],
            &["COMMIT"],
        ),
        Case::new(
            "without_rowid_strict_insert_or_ignore_elides",
            &["CREATE TABLE wr(k INT PRIMARY KEY, x INT) WITHOUT ROWID, STRICT"],
            &["BEGIN", "INSERT OR IGNORE INTO wr VALUES (1, 10), (2, 'bad')"],
            &["SELECT k, x FROM wr ORDER BY k"],
            &["COMMIT"],
        ),
        Case::new(
            "without_rowid_strict_update_non_pk_elides",
            &[
                "CREATE TABLE wr(k INT PRIMARY KEY, x INT) WITHOUT ROWID, STRICT",
                "INSERT INTO wr VALUES (1, 10), (2, 20)",
            ],
            &[
                "BEGIN",
                "UPDATE wr SET x = CASE k WHEN 1 THEN 100 ELSE 'bad' END",
            ],
            &["SELECT k, x FROM wr ORDER BY k"],
            &["COMMIT"],
        ),
        Case::new(
            "without_rowid_strict_update_pk_default_abort_undoes",
            &[
                "CREATE TABLE wr(k INT PRIMARY KEY, x INT) WITHOUT ROWID, STRICT",
                "INSERT INTO wr VALUES (1, 10), (2, 20)",
            ],
            &[
                "BEGIN",
                "UPDATE wr SET k = k + 10, x = CASE k WHEN 1 THEN 100 ELSE 'bad' END",
            ],
            &["SELECT k, x FROM wr ORDER BY k"],
            &["COMMIT"],
        ),
    ];

    assert_matches_stock(&cases);
}

#[test]
fn may_abort_constraints_ipk_and_expressions_match_stock() {
    let cases = vec![
        // NOT NULL combinations
        Case::new(
            "not_null_default_abort_undoes",
            &["CREATE TABLE s(x INT NOT NULL) STRICT"],
            &["BEGIN", "INSERT INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "not_null_on_conflict_fail_elides",
            &["CREATE TABLE s(x INT NOT NULL ON CONFLICT FAIL) STRICT"],
            &["BEGIN", "INSERT INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "not_null_or_fail_elides",
            &["CREATE TABLE s(x INT NOT NULL) STRICT"],
            &["BEGIN", "INSERT OR FAIL INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "not_null_or_replace_without_default_undoes",
            &["CREATE TABLE s(x INT NOT NULL) STRICT"],
            &["BEGIN", "INSERT OR REPLACE INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "not_null_or_replace_with_default_undoes_via_second_pass",
            &["CREATE TABLE s(x INT NOT NULL DEFAULT 0) STRICT"],
            &["BEGIN", "INSERT OR REPLACE INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "update_not_touching_not_null_elides",
            &[
                "CREATE TABLE s(id INT, a INT NOT NULL, x INT) STRICT",
                "INSERT INTO s VALUES (1, 1, 10), (2, 2, 20)",
            ],
            &[
                "BEGIN",
                "UPDATE s SET x = CASE id WHEN 1 THEN 100 ELSE 'bad' END",
            ],
            &["SELECT id, a, x FROM s ORDER BY id"],
            &["COMMIT"],
        ),
        // UNIQUE combinations
        Case::new(
            "unique_default_abort_undoes",
            &["CREATE TABLE s(x INT UNIQUE) STRICT"],
            &["BEGIN", "INSERT INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "unique_or_fail_elides",
            &["CREATE TABLE s(x INT UNIQUE) STRICT"],
            &["BEGIN", "INSERT OR FAIL INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "unique_or_ignore_elides",
            &["CREATE TABLE s(x INT UNIQUE) STRICT"],
            &["BEGIN", "INSERT OR IGNORE INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "unique_or_replace_without_triggers_elides",
            &["CREATE TABLE s(x INT UNIQUE) STRICT"],
            &["BEGIN", "INSERT OR REPLACE INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        // INTEGER PRIMARY KEY (OP_MustBeInt)
        Case::new(
            "ipk_omitted_elides",
            &["CREATE TABLE s(id INTEGER PRIMARY KEY, x INT) STRICT"],
            &["BEGIN", "INSERT INTO s(x) VALUES (10), ('bad')"],
            &["SELECT id, x FROM s ORDER BY id"],
            &["COMMIT"],
        ),
        Case::new(
            "ipk_supplied_undoes_even_with_or_fail",
            &["CREATE TABLE s(id INTEGER PRIMARY KEY, x INT) STRICT"],
            &["BEGIN", "INSERT OR FAIL INTO s(id, x) VALUES (1, 10), (2, 'bad')"],
            &["SELECT id, x FROM s ORDER BY id"],
            &["COMMIT"],
        ),
        Case::new(
            "ipk_supplied_undoes_even_with_or_ignore",
            &["CREATE TABLE s(id INTEGER PRIMARY KEY, x INT) STRICT"],
            &["BEGIN", "INSERT OR IGNORE INTO s(id, x) VALUES (1, 10), (2, 'bad')"],
            &["SELECT id, x FROM s ORDER BY id"],
            &["COMMIT"],
        ),
        Case::new(
            "ipk_update_non_pk_elides",
            &[
                "CREATE TABLE s(id INTEGER PRIMARY KEY, x INT) STRICT",
                "INSERT INTO s VALUES (1, 10), (2, 20)",
            ],
            &[
                "BEGIN",
                "UPDATE s SET x = CASE id WHEN 1 THEN 100 ELSE 'bad' END",
            ],
            &["SELECT id, x FROM s ORDER BY id"],
            &["COMMIT"],
        ),
        Case::new(
            "ipk_update_pk_undoes_even_with_or_ignore",
            &[
                "CREATE TABLE s(id INTEGER PRIMARY KEY, x INT) STRICT",
                "INSERT INTO s VALUES (1, 10), (2, 20)",
            ],
            &[
                "BEGIN",
                "UPDATE OR IGNORE s SET id = id + 10, x = CASE id WHEN 1 THEN 100 ELSE 'bad' END",
            ],
            &["SELECT id, x FROM s ORDER BY id"],
            &["COMMIT"],
        ),
        // CHECK constraints
        Case::new(
            "check_default_abort_undoes",
            &["CREATE TABLE s(x INT CHECK(x != 999)) STRICT"],
            &["BEGIN", "INSERT INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "check_or_replace_degrades_to_abort_undoes",
            &["CREATE TABLE s(x INT CHECK(x != 999)) STRICT"],
            &["BEGIN", "INSERT OR REPLACE INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "check_or_fail_elides",
            &["CREATE TABLE s(x INT CHECK(x != 999)) STRICT"],
            &["BEGIN", "INSERT OR FAIL INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "check_ignored_by_pragma_elides",
            &[
                "CREATE TABLE s(x INT CHECK(x != 999)) STRICT",
                "PRAGMA ignore_check_constraints = ON",
            ],
            &["BEGIN", "INSERT INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "check_calling_function_undoes_even_with_or_fail",
            &["CREATE TABLE s(x INT CHECK(abs(x) < 1000)) STRICT"],
            &["BEGIN", "INSERT OR FAIL INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "update_not_touching_checked_column_elides",
            &[
                "CREATE TABLE s(id INT, a INT CHECK(a > 0), x INT) STRICT",
                "INSERT INTO s VALUES (1, 1, 10), (2, 2, 20)",
            ],
            &[
                "BEGIN",
                "UPDATE s SET x = CASE id WHEN 1 THEN 100 ELSE 'bad' END",
            ],
            &["SELECT id, a, x FROM s ORDER BY id"],
            &["COMMIT"],
        ),
        // SQL functions vs inline builtins and opcodes
        Case::new(
            "sql_function_abs_sets_may_abort",
            &["CREATE TABLE s(x INT) STRICT"],
            &["BEGIN", "INSERT INTO s VALUES (abs(1)), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "inline_builtin_coalesce_elides",
            &["CREATE TABLE s(x INT) STRICT"],
            &["BEGIN", "INSERT INTO s VALUES (coalesce(NULL, 1)), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "inline_builtin_ifnull_elides",
            &["CREATE TABLE s(x INT) STRICT"],
            &["BEGIN", "INSERT INTO s VALUES (ifnull(NULL, 1)), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "inline_builtin_iif_elides",
            &["CREATE TABLE s(x INT) STRICT"],
            &["BEGIN", "INSERT INTO s VALUES (iif(1, 1, 2)), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "arithmetic_opcode_elides",
            &["CREATE TABLE s(x INT) STRICT"],
            &["BEGIN", "INSERT INTO s VALUES (1 + 2), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "like_operator_sets_may_abort",
            &[
                "CREATE TABLE src(k INT, v ANY, tag TEXT)",
                "INSERT INTO src VALUES (1, 10, 'a'), (2, 'bad', 'a')",
                "CREATE TABLE s(x INT) STRICT",
            ],
            &[
                "BEGIN",
                "INSERT INTO s SELECT v FROM src WHERE tag LIKE 'a%' ORDER BY k",
            ],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
    ];

    assert_matches_stock(&cases);
}

#[test]
fn triggers_and_foreign_keys_stmt_journal_elision_match_stock() {
    let cases = vec![
        Case::new(
            "plain_after_insert_trigger_elides_keeps_both_tables",
            &[
                "CREATE TABLE s(x INT) STRICT",
                "CREATE TABLE log(y INT)",
                "CREATE TRIGGER tr AFTER INSERT ON s BEGIN INSERT INTO log VALUES (NEW.x); END",
            ],
            &["BEGIN", "INSERT INTO s VALUES (1), ('bad')"],
            &[
                "SELECT x FROM s ORDER BY rowid",
                "SELECT y FROM log ORDER BY rowid",
            ],
            &["COMMIT"],
        ),
        Case::new(
            "after_insert_trigger_with_not_null_body_sets_may_abort",
            &[
                "CREATE TABLE s(x INT) STRICT",
                "CREATE TABLE log(y INT NOT NULL)",
                "CREATE TRIGGER tr AFTER INSERT ON s BEGIN INSERT INTO log VALUES (NEW.x); END",
            ],
            &["BEGIN", "INSERT INTO s VALUES (1), ('bad')"],
            &[
                "SELECT x FROM s ORDER BY rowid",
                "SELECT y FROM log ORDER BY rowid",
            ],
            &["COMMIT"],
        ),
        Case::new(
            "after_insert_trigger_inherits_or_fail_elides",
            &[
                "CREATE TABLE s(x INT) STRICT",
                "CREATE TABLE log(y INT NOT NULL)",
                "CREATE TRIGGER tr AFTER INSERT ON s BEGIN INSERT INTO log VALUES (NEW.x); END",
            ],
            &["BEGIN", "INSERT OR FAIL INTO s VALUES (1), ('bad')"],
            &[
                "SELECT x FROM s ORDER BY rowid",
                "SELECT y FROM log ORDER BY rowid",
            ],
            &["COMMIT"],
        ),
        Case::new(
            "after_insert_trigger_raise_abort_sets_may_abort",
            &[
                "CREATE TABLE s(x INT) STRICT",
                "CREATE TRIGGER tr AFTER INSERT ON s BEGIN SELECT RAISE(ABORT, 'nope') WHERE NEW.x = 999; END",
            ],
            &["BEGIN", "INSERT INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "after_insert_trigger_raise_fail_elides",
            &[
                "CREATE TABLE s(x INT) STRICT",
                "CREATE TRIGGER tr AFTER INSERT ON s BEGIN SELECT RAISE(FAIL, 'nope') WHERE NEW.x = 999; END",
            ],
            &["BEGIN", "INSERT INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "unique_or_replace_delete_trigger_recursive_off_elides",
            &[
                "PRAGMA recursive_triggers = OFF",
                "CREATE TABLE s(x INT UNIQUE) STRICT",
                "CREATE TABLE log(y INT)",
                "CREATE TRIGGER tr AFTER DELETE ON s BEGIN INSERT INTO log VALUES (OLD.x); END",
            ],
            &["BEGIN", "INSERT OR REPLACE INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "unique_or_replace_delete_trigger_recursive_on_sets_may_abort",
            &[
                "PRAGMA recursive_triggers = ON",
                "CREATE TABLE s(x INT UNIQUE) STRICT",
                "CREATE TABLE log(y INT)",
                "CREATE TRIGGER tr AFTER DELETE ON s BEGIN INSERT INTO log VALUES (OLD.x); END",
            ],
            &["BEGIN", "INSERT OR REPLACE INTO s VALUES (1), ('bad')"],
            &["SELECT x FROM s ORDER BY rowid"],
            &["COMMIT"],
        ),
        // Foreign keys
        Case::new(
            "immediate_fk_on_sets_may_abort",
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE p(id INT PRIMARY KEY) STRICT",
                "INSERT INTO p VALUES (1)",
                "CREATE TABLE c(pid INT REFERENCES p(id)) STRICT",
            ],
            &["BEGIN", "INSERT INTO c VALUES (1), ('bad')"],
            &["SELECT pid FROM c ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "immediate_fk_off_elides",
            &[
                "PRAGMA foreign_keys = OFF",
                "CREATE TABLE p(id INT PRIMARY KEY) STRICT",
                "INSERT INTO p VALUES (1)",
                "CREATE TABLE c(pid INT REFERENCES p(id)) STRICT",
            ],
            &["BEGIN", "INSERT INTO c VALUES (1), ('bad')"],
            &["SELECT pid FROM c ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "deferred_fk_on_elides_and_fails_commit",
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE p(id INT PRIMARY KEY) STRICT",
                "CREATE TABLE c(pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED) STRICT",
            ],
            &["BEGIN", "INSERT INTO c VALUES (999), ('bad')"],
            &["SELECT pid FROM c ORDER BY rowid"],
            &["COMMIT"],
        ),
        Case::new(
            "deferred_fk_on_elides_and_commits_after_parent_inserted",
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE p(id INT PRIMARY KEY) STRICT",
                "CREATE TABLE c(pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED) STRICT",
            ],
            &["BEGIN", "INSERT INTO c VALUES (999), ('bad')"],
            &["SELECT pid FROM c ORDER BY rowid"],
            &["INSERT INTO p VALUES (999)", "COMMIT"],
        ),
        Case::new(
            "deferred_fk_update_on_elides_and_fails_commit",
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE p(id INT PRIMARY KEY) STRICT",
                "INSERT INTO p VALUES (1), (2)",
                "CREATE TABLE c(k INT, pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED) STRICT",
                "INSERT INTO c VALUES (1, 1), (2, 2)",
            ],
            &[
                "BEGIN",
                "UPDATE c SET pid = CASE k WHEN 1 THEN 999 ELSE 'bad' END",
            ],
            &["SELECT k, pid FROM c ORDER BY k"],
            &["COMMIT"],
        ),
        Case::new(
            "self_referencing_on_delete_cascade_strict_does_not_recurse_infinitely",
            &[
                "PRAGMA foreign_keys = ON",
                "CREATE TABLE p(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE, x INT) STRICT",
                "INSERT INTO p VALUES (1, 1, 10), (2, 1, 20)",
            ],
            &[
                "BEGIN",
                "UPDATE p SET x = CASE id WHEN 1 THEN 100 ELSE 'bad' END",
            ],
            &["SELECT id, pid, x FROM p ORDER BY id"],
            &["COMMIT"],
        ),
    ];

    assert_matches_stock(&cases);
}

#[test]
fn prepared_statement_reuse_tracks_dynamic_pragma_changes_against_stock() {
    asupersync::test_utils::run_test(|| async {
        // 1. PRAGMA foreign_keys toggled between executions of the SAME prepared statement.
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let frank = Connection::open(":memory:").await.unwrap();
        let setup = [
            "CREATE TABLE p(id INT PRIMARY KEY) STRICT",
            "INSERT INTO p VALUES (1)",
            "CREATE TABLE c(pid INT REFERENCES p(id)) STRICT",
            "CREATE TABLE chk(x INT CHECK(x != 999)) STRICT",
            "CREATE TABLE uq(x INT UNIQUE) STRICT",
            "CREATE TABLE log(y INT)",
            "CREATE TRIGGER tr_del AFTER DELETE ON uq BEGIN INSERT INTO log VALUES (OLD.x); END",
        ];
        for sql in setup {
            stock.execute_batch(sql).unwrap();
            frank.execute(sql).await.unwrap();
        }

        let fk_sql = "INSERT INTO c VALUES (?1), (?2)";
        let chk_sql = "INSERT INTO chk VALUES (?1), (?2)";
        let rec_sql = "INSERT OR REPLACE INTO uq VALUES (?1), (?2)";

        let mut stock_fk = stock.prepare(fk_sql).unwrap();
        let frank_fk = frank.prepare(fk_sql).await.unwrap();
        for (pragma, expect_kept) in [
            ("PRAGMA foreign_keys = ON", false),
            ("PRAGMA foreign_keys = OFF", true),
            ("PRAGMA foreign_keys = ON", false),
        ] {
            stock.execute_batch(pragma).unwrap();
            frank.execute(pragma).await.unwrap();
            stock.execute_batch("DELETE FROM c; BEGIN;").unwrap();
            frank.execute("DELETE FROM c").await.unwrap();
            frank.execute("BEGIN").await.unwrap();

            let s_err = stock_fk
                .execute(rusqlite::params![1_i64, "bad"])
                .expect_err("stock datatype error");
            let f_err = frank_fk
                .execute_with_params(&[SqliteValue::Integer(1), SqliteValue::Text("bad".into())])
                .await
                .expect_err("fsqlite datatype error");
            assert_eq!(
                stock_outcome(Err(s_err)),
                frank_outcome::<usize>(Err(f_err)),
                "fk reuse mismatch for {pragma}"
            );
            let s_rows = stock_rows(&stock, "SELECT pid FROM c ORDER BY rowid");
            let f_rows = frank_rows(&frank, "SELECT pid FROM c ORDER BY rowid").await;
            assert_eq!(f_rows, s_rows, "fk rows mismatch for {pragma}");
            assert_eq!(
                !f_rows.is_empty(),
                expect_kept,
                "unexpected kept state for {pragma}"
            );
            stock.execute_batch("COMMIT").unwrap();
            frank.execute("COMMIT").await.unwrap();
        }

        let mut stock_chk = stock.prepare(chk_sql).unwrap();
        let frank_chk = frank.prepare(chk_sql).await.unwrap();
        for (pragma, expect_kept) in [
            ("PRAGMA ignore_check_constraints = OFF", false),
            ("PRAGMA ignore_check_constraints = ON", true),
            ("PRAGMA ignore_check_constraints = OFF", false),
        ] {
            stock.execute_batch(pragma).unwrap();
            frank.execute(pragma).await.unwrap();
            stock.execute_batch("DELETE FROM chk; BEGIN;").unwrap();
            frank.execute("DELETE FROM chk").await.unwrap();
            frank.execute("BEGIN").await.unwrap();

            let s_err = stock_chk
                .execute(rusqlite::params![1_i64, "bad"])
                .expect_err("stock datatype error");
            let f_err = frank_chk
                .execute_with_params(&[SqliteValue::Integer(1), SqliteValue::Text("bad".into())])
                .await
                .expect_err("fsqlite datatype error");
            assert_eq!(
                stock_outcome(Err(s_err)),
                frank_outcome::<usize>(Err(f_err)),
                "chk reuse mismatch for {pragma}"
            );
            let s_rows = stock_rows(&stock, "SELECT x FROM chk ORDER BY rowid");
            let f_rows = frank_rows(&frank, "SELECT x FROM chk ORDER BY rowid").await;
            assert_eq!(f_rows, s_rows, "chk rows mismatch for {pragma}");
            assert_eq!(
                !f_rows.is_empty(),
                expect_kept,
                "unexpected kept state for {pragma}"
            );
            stock.execute_batch("COMMIT").unwrap();
            frank.execute("COMMIT").await.unwrap();
        }

        let mut stock_rec = stock.prepare(rec_sql).unwrap();
        let frank_rec = frank.prepare(rec_sql).await.unwrap();
        for (pragma, expect_kept) in [
            ("PRAGMA recursive_triggers = OFF", true),
            ("PRAGMA recursive_triggers = ON", false),
            ("PRAGMA recursive_triggers = OFF", true),
        ] {
            stock.execute_batch(pragma).unwrap();
            frank.execute(pragma).await.unwrap();
            stock.execute_batch("DELETE FROM uq; BEGIN;").unwrap();
            frank.execute("DELETE FROM uq").await.unwrap();
            frank.execute("BEGIN").await.unwrap();

            let s_err = stock_rec
                .execute(rusqlite::params![1_i64, "bad"])
                .expect_err("stock datatype error");
            let f_err = frank_rec
                .execute_with_params(&[SqliteValue::Integer(1), SqliteValue::Text("bad".into())])
                .await
                .expect_err("fsqlite datatype error");
            assert_eq!(
                stock_outcome(Err(s_err)),
                frank_outcome::<usize>(Err(f_err)),
                "rec reuse mismatch for {pragma}"
            );
            let s_rows = stock_rows(&stock, "SELECT x FROM uq ORDER BY rowid");
            let f_rows = frank_rows(&frank, "SELECT x FROM uq ORDER BY rowid").await;
            assert_eq!(f_rows, s_rows, "rec rows mismatch for {pragma}");
            assert_eq!(
                !f_rows.is_empty(),
                expect_kept,
                "unexpected kept state for {pragma}"
            );
            stock.execute_batch("COMMIT").unwrap();
            frank.execute("COMMIT").await.unwrap();
        }
    });
}
