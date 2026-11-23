#![recursion_limit = "512"]

//! Bead .33: after a failed INSERT the next automatic rowid is
//! `max(rowid) + 1`, as in stock SQLite (rusqlite), including when the failed
//! INSERT is the connection's first write to the table.
//!
//! The failing statement's row lands in the table b-tree before its index
//! entry hits the UNIQUE (or PRIMARY KEY) conflict. The shared rowid
//! allocator's rewind for a table first allocated in the aborted transaction
//! targeted a floor already raised past that row, so the rowid stayed burned
//! and the next INSERT got `max(rowid) + 2`. An `OR FAIL` statement keeps its
//! earlier rows and runs no rewind, so the rowid of its failing row stayed
//! burned whatever constraint failed, AUTOINCREMENT included. Each case seeds a
//! table, runs a failing statement and then plain inserts, and compares every
//! outcome and the final rows with stock. A case runs in memory, in a file
//! FrankenSQLite seeded with another connection, and in a file stock seeded;
//! stock then checks the file.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

/// (name, schema and seed, statements after the open)
const CASES: &[(&str, &[&str], &[&str])] = &[
    (
        "unique",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &["INSERT INTO u VALUES (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "partial_unique",
        &[
            "CREATE TABLE u(value INTEGER)",
            "CREATE UNIQUE INDEX u_value ON u(value) WHERE rowid > 0",
            "INSERT INTO u VALUES (7)",
        ],
        &["INSERT INTO u VALUES (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "text_primary_key",
        &["CREATE TABLE u(value TEXT PRIMARY KEY)", "INSERT INTO u VALUES ('a')"],
        &["INSERT INTO u VALUES ('a')", "INSERT INTO u VALUES ('b')"],
    ),
    (
        "multi_row_abort",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &["INSERT INTO u VALUES (8), (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "multi_row_fail_keeps_its_first_row",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &["INSERT OR FAIL INTO u VALUES (8), (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "multi_row_fail_not_null",
        &[
            "CREATE TABLE u(value UNIQUE, n NOT NULL CHECK (n >= 0))",
            "INSERT INTO u VALUES (7, 1)",
        ],
        &["INSERT OR FAIL INTO u VALUES (8, 1), (9, NULL)", "INSERT INTO u VALUES (30, 1)"],
    ),
    (
        "multi_row_fail_check",
        &[
            "CREATE TABLE u(value UNIQUE, n NOT NULL CHECK (n >= 0))",
            "INSERT INTO u VALUES (7, 1)",
        ],
        &["INSERT OR FAIL INTO u VALUES (8, 1), (9, -1)", "INSERT INTO u VALUES (30, 1)"],
    ),
    (
        "column_on_conflict_fail",
        &["CREATE TABLE u(value UNIQUE ON CONFLICT FAIL)", "INSERT INTO u VALUES (7)"],
        &["INSERT INTO u VALUES (8), (7)", "INSERT INTO u VALUES (30)"],
    ),
    (
        "fail_insert_select",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "INSERT OR FAIL INTO u SELECT 8 UNION ALL SELECT 7",
            "INSERT INTO u VALUES (30)",
        ],
    ),
    (
        "fail_in_explicit_transaction",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "BEGIN",
            "INSERT OR FAIL INTO u VALUES (8), (7)",
            "INSERT INTO u VALUES (30)",
            "COMMIT",
        ],
    ),
    (
        // A failed statement never runs stock's sqlite_sequence update, so
        // its rowids are not burned even under AUTOINCREMENT.
        "autoincrement_fail",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, value UNIQUE, n NOT NULL)",
            "INSERT INTO u(value, n) VALUES (7, 1)",
        ],
        &[
            "INSERT OR FAIL INTO u(value, n) VALUES (8, 1), (7, 1)",
            "INSERT OR FAIL INTO u(value, n) VALUES (9, 1), (10, NULL)",
            "INSERT INTO u(value, n) VALUES (30, 1)",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        // A statement that succeeds keeps an AUTOINCREMENT rowid it burned.
        "autoincrement_ignore_burns",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, value UNIQUE)",
            "INSERT INTO u(value) VALUES (7)",
        ],
        &[
            "INSERT OR IGNORE INTO u(value) VALUES (7)",
            "INSERT INTO u(value) VALUES (30)",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        "two_failures",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "INSERT INTO u VALUES (7)",
            "INSERT INTO u VALUES (7)",
            "INSERT INTO u VALUES (30)",
        ],
    ),
    (
        "success_then_failure",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "INSERT INTO u VALUES (9)",
            "INSERT INTO u VALUES (7)",
            "INSERT INTO u VALUES (30)",
        ],
    ),
    (
        "explicit_transaction",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "BEGIN",
            "INSERT INTO u VALUES (7)",
            "INSERT INTO u VALUES (30)",
            "COMMIT",
        ],
    ),
    (
        "rolled_back_transaction",
        &["CREATE TABLE u(value UNIQUE)", "INSERT INTO u VALUES (7)"],
        &[
            "BEGIN",
            "INSERT INTO u VALUES (8)",
            "INSERT INTO u VALUES (7)",
            "ROLLBACK",
            "INSERT INTO u VALUES (30)",
        ],
    ),
    (
        "autoincrement",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, value UNIQUE)",
            "INSERT INTO u(value) VALUES (7)",
        ],
        &["INSERT INTO u(value) VALUES (7)", "INSERT INTO u(value) VALUES (30)"],
    ),
    // bd-u65rh / bd-failed-insert-rowid-floor-l53w4: failed explicit-rowid
    // inserts must not leave the shared allocator pinned above max(rowid).
    (
        "explicit_unique_autocommit",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "INSERT INTO u VALUES (7, 'a')",
            "INSERT INTO u(v) VALUES ('b')",
        ],
    ),
    (
        "explicit_unique_explicit_txn",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "BEGIN",
            "INSERT INTO u VALUES (7, 'a')",
            "INSERT INTO u(v) VALUES ('b')",
            "COMMIT",
        ],
    ),
    (
        "explicit_multi_row_unique_explicit_txn",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "BEGIN",
            "INSERT INTO u VALUES (90001, 'b'), (90002, 'a')",
            "INSERT INTO u(v) VALUES ('c')",
            "COMMIT",
        ],
    ),
    (
        "explicit_autoincrement_unique_autocommit",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "INSERT INTO u VALUES (90001, 'a')",
            "INSERT INTO u(v) VALUES ('b')",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        "explicit_autoincrement_unique_explicit_txn",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "BEGIN",
            "INSERT INTO u VALUES (90001, 'b'), (90002, 'a')",
            "INSERT INTO u(v) VALUES ('c')",
            "COMMIT",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        "explicit_or_fail_keeps_first_rewinds_second",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "BEGIN",
            "INSERT OR FAIL INTO u VALUES (10, 'b'), (90001, 'a')",
            "INSERT INTO u(v) VALUES ('c')",
            "COMMIT",
        ],
    ),
    (
        "explicit_autoincrement_or_fail_keeps_first_rewinds_second",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "BEGIN",
            "INSERT OR FAIL INTO u VALUES (10, 'b'), (90001, 'a')",
            "INSERT INTO u(v) VALUES ('c')",
            "COMMIT",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        "explicit_ignore_normal_does_not_burn",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "BEGIN",
            "INSERT OR IGNORE INTO u VALUES (90001, 'a')",
            "INSERT INTO u(v) VALUES ('b')",
            "COMMIT",
        ],
    ),
    (
        "explicit_ignore_autoincrement_burns",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "BEGIN",
            "INSERT OR IGNORE INTO u VALUES (90001, 'a')",
            "INSERT INTO u(v) VALUES ('b')",
            "COMMIT",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        "explicit_savepoint_and_rollback",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "BEGIN",
            "SAVEPOINT sp",
            "INSERT INTO u VALUES (95000, 'b')",
            "ROLLBACK TO sp",
            "INSERT INTO u(v) VALUES ('c')",
            "RELEASE sp",
            "COMMIT",
            "BEGIN",
            "INSERT INTO u VALUES (96000, 'd')",
            "ROLLBACK",
            "INSERT INTO u(v) VALUES ('e')",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        "explicit_max_rowid_fail_does_not_exhaust",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a')",
        ],
        &[
            "BEGIN",
            "INSERT INTO u VALUES (9223372036854775807, 'a')",
            "INSERT INTO u(v) VALUES ('b')",
            "COMMIT",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
    (
        "explicit_update_rowid_fail",
        &[
            "CREATE TABLE u(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT UNIQUE)",
            "INSERT INTO u VALUES (1, 'a'), (2, 'b')",
        ],
        &[
            "BEGIN",
            "UPDATE u SET id = 90001, v = 'a' WHERE id = 2",
            "INSERT INTO u(v) VALUES ('c')",
            "COMMIT",
            "SELECT name, seq FROM sqlite_sequence",
        ],
    ),
];

const ROWS: &str = "SELECT rowid, * FROM u ORDER BY rowid";

/// Who wrote the seed rows the statements run against.
#[derive(Clone, Copy, Debug)]
enum Seed {
    /// The same in-memory connection.
    Memory,
    /// Another FrankenSQLite connection, closed before the open.
    Fsqlite,
    /// Stock.
    Stock,
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
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => format!("{sql} => error:{message}"),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

async fn fsqlite_query(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => format!("{sql} => ok {:?}", render_rows(&rows)),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

async fn fsqlite_execute(conn: &Connection, sql: &str) -> String {
    match conn.execute(sql).await {
        Ok(_) => format!("{sql} => ok []"),
        Err(error) => format!("{sql} => error:{error}"),
    }
}

/// Every statement's outcome and the final rows, in one stock connection
/// after the seed one closed.
fn stock_transcript(path: &str, seed: &[&str], steps: &[&str]) -> Vec<String> {
    {
        let conn = rusqlite::Connection::open(path).expect("stock open");
        for sql in seed {
            conn.execute_batch(sql).expect("stock seed");
        }
    }
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    let mut transcript: Vec<String> = steps.iter().map(|sql| stock_query(&conn, sql)).collect();
    transcript.push(stock_query(&conn, ROWS));
    transcript
}

async fn fsqlite_transcript(path: &str, seed_by: Seed, seed: &[&str], steps: &[&str]) -> Vec<String> {
    let conn = match seed_by {
        Seed::Memory => {
            let conn = Connection::open(":memory:").await.expect("fsqlite open");
            for sql in seed {
                conn.execute(sql).await.expect("fsqlite seed");
            }
            conn
        }
        Seed::Fsqlite => {
            let seeder = Connection::open(path).await.expect("fsqlite open");
            for sql in seed {
                seeder.execute(sql).await.expect("fsqlite seed");
            }
            seeder.close().await.expect("fsqlite close");
            Connection::open(path).await.expect("fsqlite reopen")
        }
        Seed::Stock => {
            let stock = rusqlite::Connection::open(path).expect("stock open");
            for sql in seed {
                stock.execute_batch(sql).expect("stock seed");
            }
            drop(stock);
            Connection::open(path).await.expect("fsqlite open")
        }
    };
    let mut transcript = Vec::new();
    for sql in steps {
        transcript.push(if sql.starts_with("SELECT") {
            fsqlite_query(&conn, sql).await
        } else {
            fsqlite_execute(&conn, sql).await
        });
    }
    transcript.push(fsqlite_query(&conn, ROWS).await);
    conn.close().await.expect("fsqlite close");
    transcript
}

#[test]
fn next_rowid_after_a_failed_insert_matches_stock() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dir_path = dir.path().to_owned();
    let path_of = |name: &str| {
        dir_path
            .join(format!("{name}.db"))
            .to_str()
            .expect("utf-8 path")
            .to_owned()
    };
    let expected: Vec<Vec<String>> = CASES
        .iter()
        .map(|(name, seed, steps)| stock_transcript(&path_of(&format!("stock_{name}")), seed, steps))
        .collect();
    // The oracle itself: the insert after the failure lands at max(rowid) + 1.
    // (So does AUTOINCREMENT's: the failed statement's sqlite_sequence update
    // rolls back with it.)
    assert!(
        expected[0].last().is_some_and(|rows| rows.ends_with("[\"i:1|i:7\", \"i:2|i:30\"]")),
        "stock: {:#?}",
        expected[0]
    );
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for ((name, seed, steps), want) in CASES.iter().zip(&expected) {
            for seed_by in [Seed::Memory, Seed::Fsqlite, Seed::Stock] {
                let path = path_of(&format!("{name}_{seed_by:?}"));
                let got = fsqlite_transcript(&path, seed_by, seed, steps).await;
                if &got != want {
                    mismatches.push(format!(
                        "{name} ({seed_by:?})\n  fsqlite: {got:#?}\n  stock:   {want:#?}"
                    ));
                }
                if !matches!(seed_by, Seed::Memory) {
                    let stock = rusqlite::Connection::open(&path).expect("stock reopen");
                    let check = stock_query(&stock, "PRAGMA integrity_check");
                    let rows = stock_query(&stock, ROWS);
                    if check != "PRAGMA integrity_check => ok [\"t:ok\"]"
                        || Some(&rows) != want.last()
                    {
                        mismatches.push(format!(
                            "{name} ({seed_by:?}): stock reading the fsqlite file\n  {check}\n  {rows}"
                        ));
                    }
                }
            }
        }
        for mismatch in &mismatches {
            eprintln!("event=failed_insert_rowid_mismatch {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} mismatches; first: {}",
            mismatches.len(),
            mismatches[0]
        );
    });
}

fn stock_outcome_transcript(path: &str, seed: &[&str], steps: &[&str]) -> Vec<String> {
    {
        let conn = rusqlite::Connection::open(path).expect("stock open");
        conn.execute_batch("PRAGMA foreign_keys = ON;")
            .expect("stock fk");
        for sql in seed {
            conn.execute_batch(sql).expect("stock seed");
        }
    }
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    conn.execute_batch("PRAGMA foreign_keys = ON;")
        .expect("stock fk");
    steps
        .iter()
        .map(|sql| {
            let trimmed = sql.trim_start();
            if trimmed.starts_with("SELECT") {
                stock_query(&conn, sql)
            } else {
                match conn.execute(sql, []) {
                    Ok(_) => format!("{sql} => ok"),
                    Err(_) => format!("{sql} => err"),
                }
            }
        })
        .collect()
}

async fn fsqlite_outcome_transcript(
    path: &str,
    seed_by: Seed,
    seed: &[&str],
    steps: &[&str],
) -> Vec<String> {
    let conn = match seed_by {
        Seed::Memory => {
            let conn = Connection::open(":memory:").await.expect("fsqlite open");
            conn.execute("PRAGMA foreign_keys = ON")
                .await
                .expect("fsqlite fk");
            for sql in seed {
                conn.execute(sql).await.expect("fsqlite seed");
            }
            conn
        }
        Seed::Fsqlite => {
            let seeder = Connection::open(path).await.expect("fsqlite open");
            seeder
                .execute("PRAGMA foreign_keys = ON")
                .await
                .expect("fsqlite fk");
            for sql in seed {
                seeder.execute(sql).await.expect("fsqlite seed");
            }
            seeder.close().await.expect("fsqlite close");
            let conn = Connection::open(path).await.expect("fsqlite reopen");
            conn.execute("PRAGMA foreign_keys = ON")
                .await
                .expect("fsqlite fk");
            conn
        }
        Seed::Stock => {
            let stock = rusqlite::Connection::open(path).expect("stock open");
            stock
                .execute_batch("PRAGMA foreign_keys = ON;")
                .expect("stock fk");
            for sql in seed {
                stock.execute_batch(sql).expect("stock seed");
            }
            drop(stock);
            let conn = Connection::open(path).await.expect("fsqlite open");
            conn.execute("PRAGMA foreign_keys = ON")
                .await
                .expect("fsqlite fk");
            conn
        }
    };
    let mut out = Vec::with_capacity(steps.len());
    for sql in steps {
        let trimmed = sql.trim_start();
        if trimmed.starts_with("SELECT") {
            out.push(fsqlite_query(&conn, sql).await);
        } else {
            match conn.execute(sql).await {
                Ok(_) => out.push(format!("{sql} => ok")),
                Err(_) => out.push(format!("{sql} => err")),
            }
        }
    }
    conn.close().await.expect("fsqlite close");
    out
}

/// bd-u65rh & bd-failed-insert-rowid-floor-l53w4: exact canonical repros
/// covering multi-row datatype mismatch, INSERT...SELECT replay path, and
/// FOREIGN KEY violations on explicit rowids in autocommit and explicit
/// transactions, both `:memory:` and file-backed.
#[test]
fn explicit_rowid_failed_statements_match_stock_across_modes() {
    const EXPLICIT_CASES: &[(&str, &[&str], &[&str])] = &[
        (
            "bd_u65rh_datatype_mismatch_autocommit",
            &[
                "CREATE TABLE n(a INT, b TEXT)",
                "INSERT INTO n VALUES (1, 'a'), (2, 'b')",
            ],
            &[
                "INSERT INTO n(rowid, a) VALUES (90001, 1), (90002, 2), ('abc', 5)",
                "INSERT INTO n(b) VALUES ('q')",
                "SELECT last_insert_rowid()",
                "SELECT rowid, a, b FROM n ORDER BY rowid",
            ],
        ),
        (
            "bd_u65rh_datatype_mismatch_explicit_txn",
            &[
                "CREATE TABLE n(a INT, b TEXT)",
                "INSERT INTO n VALUES (1, 'a'), (2, 'b')",
            ],
            &[
                "BEGIN",
                "INSERT INTO n(rowid, a) VALUES (90001, 1), (90002, 2), ('abc', 5)",
                "INSERT INTO n(b) VALUES ('q')",
                "SELECT last_insert_rowid()",
                "COMMIT",
                "SELECT rowid, a, b FROM n ORDER BY rowid",
            ],
        ),
        (
            "bd_u65rh_insert_select_replay_autocommit",
            &[
                "CREATE TABLE s(x INT, y TEXT)",
                "INSERT INTO s VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g')",
                "CREATE TABLE n(a INT UNIQUE, b TEXT)",
                "INSERT INTO n VALUES (5, 'five')",
            ],
            &[
                "INSERT INTO n(rowid, a) SELECT x + 90000, x FROM s WHERE x < 8",
                "INSERT INTO n(b) SELECT y FROM s WHERE x <= 2",
                "SELECT last_insert_rowid()",
                "SELECT rowid, a, b FROM n ORDER BY rowid",
            ],
        ),
        (
            "bd_u65rh_insert_select_replay_explicit_txn",
            &[
                "CREATE TABLE s(x INT, y TEXT)",
                "INSERT INTO s VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g')",
                "CREATE TABLE n(a INT UNIQUE, b TEXT)",
                "INSERT INTO n VALUES (5, 'five')",
            ],
            &[
                "BEGIN",
                "INSERT INTO n(rowid, a) SELECT x + 90000, x FROM s WHERE x < 8",
                "INSERT INTO n(b) SELECT y FROM s WHERE x <= 2",
                "SELECT last_insert_rowid()",
                "COMMIT",
                "SELECT rowid, a, b FROM n ORDER BY rowid",
            ],
        ),
        (
            "bd_l53w4_fk_autocommit",
            &[
                "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
                "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
                "INSERT INTO parent VALUES (1)",
                "INSERT INTO child VALUES (1, 1)",
            ],
            &[
                "INSERT INTO child VALUES (2, 999)",
                "INSERT INTO child(pid) VALUES (1)",
                "SELECT group_concat(id) FROM child",
            ],
        ),
        (
            "bd_l53w4_fk_explicit_txn",
            &[
                "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
                "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
                "INSERT INTO parent VALUES (1)",
                "INSERT INTO child VALUES (1, 1)",
            ],
            &[
                "BEGIN",
                "INSERT INTO child VALUES (90001, 999)",
                "INSERT INTO child(pid) VALUES (1)",
                "COMMIT",
                "SELECT group_concat(id) FROM child",
            ],
        ),
        (
            "bd_l53w4_fk_autoincrement_autocommit_and_txn",
            &[
                "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
                "CREATE TABLE child(id INTEGER PRIMARY KEY AUTOINCREMENT, pid INTEGER REFERENCES parent(id))",
                "INSERT INTO parent VALUES (1)",
                "INSERT INTO child VALUES (1, 1)",
            ],
            &[
                "INSERT INTO child VALUES (90001, 999)",
                "INSERT INTO child(pid) VALUES (1)",
                "BEGIN",
                "INSERT INTO child VALUES (90002, 999)",
                "INSERT INTO child(pid) VALUES (1)",
                "COMMIT",
                "SELECT group_concat(id) FROM child",
                "SELECT name, seq FROM sqlite_sequence",
            ],
        ),
        (
            "bd_u65rh_multi_row_not_null_explicit_txn",
            &[
                "CREATE TABLE n(a INT NOT NULL, b TEXT)",
                "INSERT INTO n VALUES (1, 'a'), (2, 'b')",
            ],
            &[
                "BEGIN",
                "INSERT INTO n(rowid, a, b) VALUES (90001, 1, 'x'), (90002, 2, 'y'), (90003, NULL, 'z')",
                "INSERT INTO n(a, b) VALUES (3, 'q')",
                "SELECT last_insert_rowid()",
                "COMMIT",
                "SELECT rowid, a, b FROM n ORDER BY rowid",
            ],
        ),
        // bd-w4pr1.13: a later row of a multi-row explicit statement (a run of
        // the statement's first floor bump) discarded by a UNIQUE conflict
        // gives its rowid back, under OR IGNORE and under OR FAIL.
        (
            "bd_w4pr1_13_run_row_or_ignore_explicit_txn",
            &[
                "CREATE TABLE n(a INT UNIQUE, b TEXT)",
                "INSERT INTO n VALUES (7, 'seven')",
            ],
            &[
                "BEGIN",
                "INSERT OR IGNORE INTO n(rowid, a) VALUES (90001, 1), (90002, 2), (90003, 7)",
                "INSERT INTO n(b) VALUES ('q')",
                "SELECT last_insert_rowid()",
                "COMMIT",
                "SELECT rowid, a, b FROM n ORDER BY rowid",
            ],
        ),
        (
            "bd_w4pr1_13_run_row_or_fail_explicit_txn",
            &[
                "CREATE TABLE n(a INT UNIQUE, b TEXT)",
                "INSERT INTO n VALUES (7, 'seven')",
            ],
            &[
                "BEGIN",
                "INSERT OR FAIL INTO n(rowid, a) VALUES (90001, 1), (90002, 2), (90003, 7), (90004, 4)",
                "INSERT INTO n(b) VALUES ('q')",
                "SELECT last_insert_rowid()",
                "COMMIT",
                "SELECT rowid, a, b FROM n ORDER BY rowid",
            ],
        ),
    ];

    let dir = tempfile::tempdir().expect("temp dir");
    let dir_path = dir.path().to_owned();
    let path_of = |name: &str| {
        dir_path
            .join(format!("{name}.db"))
            .to_str()
            .expect("utf-8 path")
            .to_owned()
    };
    let expected: Vec<Vec<String>> = EXPLICIT_CASES
        .iter()
        .map(|(name, seed, steps)| {
            stock_outcome_transcript(&path_of(&format!("stock_{name}")), seed, steps)
        })
        .collect();

    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for ((name, seed, steps), want) in EXPLICIT_CASES.iter().zip(&expected) {
            for seed_by in [Seed::Memory, Seed::Fsqlite, Seed::Stock] {
                let path = path_of(&format!("{name}_{seed_by:?}"));
                let got = fsqlite_outcome_transcript(&path, seed_by, seed, steps).await;
                if &got != want {
                    mismatches.push(format!(
                        "{name} ({seed_by:?})\n  fsqlite: {got:#?}\n  stock:   {want:#?}"
                    ));
                }
                if !matches!(seed_by, Seed::Memory) {
                    let stock = rusqlite::Connection::open(&path).expect("stock reopen");
                    let check = stock_query(&stock, "PRAGMA integrity_check");
                    if check != "PRAGMA integrity_check => ok [\"t:ok\"]" {
                        mismatches.push(format!(
                            "{name} ({seed_by:?}): stock integrity_check failed: {check}"
                        ));
                    }
                }
            }
        }
        for mismatch in &mismatches {
            eprintln!("event=explicit_rowid_mismatch {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} explicit-rowid mismatches; first: {}",
            mismatches.len(),
            mismatches[0]
        );
    });
}

/// bd-u65rh concurrency guard: if a concurrent peer inserts on the same table
/// (on a disjoint B-tree leaf) after this session's explicit-rowid bump,
/// rolling back this session's statement or transaction MUST keep the gap and
/// never overwrite or collide with the peer's committed row.
#[test]
fn explicit_rowid_rollback_keeps_gap_when_concurrent_peer_intervenes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("explicit_peer_gap.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async move {
        let a = Connection::open(&path_str).await.expect("open a");
        a.execute_batch(
            "PRAGMA journal_mode=WAL;\
             CREATE TABLE u(id INTEGER PRIMARY KEY, pad TEXT NOT NULL);",
        )
        .await
        .expect("schema");
        // Seed enough padded rows with even rowids so the table spans many
        // B-tree leaf pages; rowid 5 lives on the leftmost leaf while explicit
        // rowid 90001 lands on the rightmost leaf (disjoint MVCC page locks).
        let pad = "p".repeat(240);
        a.execute("BEGIN").await.expect("seed begin");
        for i in 1..=200_i64 {
            a.execute(&format!("INSERT INTO u VALUES ({}, '{pad}')", i * 2))
                .await
                .expect("seed insert");
        }
        a.execute("COMMIT").await.expect("seed commit");

        let b = Connection::open(&path_str).await.expect("open b");
        a.execute("PRAGMA busy_timeout=2000").await.expect("a busy");
        b.execute("PRAGMA busy_timeout=2000").await.expect("b busy");

        // Use `RETURNING id` so every write runs on the VDBE +
        // ConcurrentRowIdAllocator lane (plain single-row INSERT takes the
        // direct-simple B-tree cursor lane and bypasses the shared allocator).
        //
        // Connection A bumps the allocator tip with explicit rowid 90001 on
        // the rightmost leaf inside an explicit transaction. While A holds that
        // uncommitted bump, peer B inserts explicit rowid 5 on the disjoint
        // leftmost leaf and commits. Then A's second statement bumps to 95001
        // and fails NOT NULL, and A rolls back its whole transaction. Because
        // B intervened on the same table, A's rollback keeps the gap at 90002
        // (while A's statement rewind already reclaimed 95001 back to 90002).
        a.execute("BEGIN CONCURRENT").await.expect("a begin");
        b.execute("BEGIN CONCURRENT").await.expect("b begin");

        a.query(&format!("INSERT INTO u VALUES (90001, '{pad}') RETURNING id"))
            .await
            .expect("a explicit insert 90001");
        b.query(&format!("INSERT INTO u VALUES (5, '{pad}') RETURNING id"))
            .await
            .expect("b disjoint explicit insert 5");
        b.execute("COMMIT").await.expect("b commit 5");

        // A's second statement bumps to 95001 and then fails NOT NULL.
        assert!(
            a.query(&format!(
                "INSERT INTO u VALUES (95001, '{pad}'), (95002, NULL) RETURNING id"
            ))
            .await
            .is_err(),
            "NOT NULL must fail A's second statement"
        );
        // Rolling back A's entire transaction must not rewind past B's commit:
        // A's statement-level rewind reclaimed 95001 back to 90002, while A's
        // transaction-level rewind sees B's intervening write and preserves
        // 90002 as the tip (keeping the gap).
        a.execute("ROLLBACK").await.expect("a rollback");

        // Start a fresh transaction where B also holds an active write claim
        // on the leftmost leaf so quiescent lowering cannot mask the allocator
        // tip, then verify A allocates from the preserved tip (90002).
        b.execute("BEGIN CONCURRENT").await.expect("b begin 2");
        b.query(&format!("INSERT INTO u VALUES (7, '{pad}') RETURNING id"))
            .await
            .expect("b disjoint explicit insert 7");
        a.execute("BEGIN CONCURRENT").await.expect("a begin 2");
        let a_rows = a
            .query(&format!("INSERT INTO u(pad) VALUES ('{pad}') RETURNING id"))
            .await
            .expect("a implicit insert after peer intervention");
        let a_id: i64 = match a_rows.first().and_then(|row| row.get(0)) {
            Some(SqliteValue::Integer(id)) => *id,
            other => panic!("unexpected a RETURNING id: {other:?}"),
        };
        assert_eq!(
            a_id, 90002,
            "peer intervention after A's explicit bump 90001 must keep the gap at 90002"
        );
        a.execute("COMMIT").await.expect("a commit 2");
        b.execute("COMMIT").await.expect("b commit 2");

        let count: i64 = match a
            .query_row("SELECT count(*) FROM u")
            .await
            .expect("count")
            .get(0)
        {
            Some(SqliteValue::Integer(n)) => *n,
            other => panic!("unexpected count: {other:?}"),
        };
        assert_eq!(count, 203, "200 seeded + B(5, 7) + A(90002) = 203 rows");

        a.close().await.expect("close a");
        b.close().await.expect("close b");
    });

    let stock = rusqlite::Connection::open(&path).expect("stock open");
    let check = stock_query(&stock, "PRAGMA integrity_check");
    assert_eq!(check, "PRAGMA integrity_check => ok [\"t:ok\"]");
}

/// bd-w4pr1.13: the peer-gap case above with multi-row statements, the shape
/// of a bulk INSERT, whose later rows the allocator records as one run of the
/// statement's first bump. Connection A's first statement appends explicit
/// rowids 90001..90100 on the rightmost leaves (raising the floor on every
/// row, so to 90101); peer B inserts explicit rowid 5 on the leftmost leaf
/// (disjoint page locks; a short row, so the full leaf takes it without a
/// split of the root A holds) and commits; A's second statement appends
/// 95001..95009 and fails NOT NULL on 95010, and its rewind gives back that
/// whole run, to 90101; A's ROLLBACK then keeps the gap, because B intervened.
/// A's next implicit rowid is therefore 90101: not 90002 (a floor raised by
/// each statement's first row only), nor 95010 (a run not given back).
#[test]
fn explicit_rowid_runs_raise_every_row_and_rewind_whole_beside_a_concurrent_peer() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("explicit_run_floor.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async move {
        let a = Connection::open(&path_str).await.expect("open a");
        a.execute_batch(
            "PRAGMA journal_mode=WAL;\
             CREATE TABLE u(id INTEGER PRIMARY KEY, pad TEXT NOT NULL);",
        )
        .await
        .expect("schema");
        let pad = "p".repeat(240);
        a.execute("BEGIN").await.expect("seed begin");
        for i in 1..=200_i64 {
            a.execute(&format!("INSERT INTO u VALUES ({}, '{pad}')", i * 2))
                .await
                .expect("seed insert");
        }
        a.execute("COMMIT").await.expect("seed commit");

        let b = Connection::open(&path_str).await.expect("open b");
        a.execute("PRAGMA busy_timeout=2000").await.expect("a busy");
        b.execute("PRAGMA busy_timeout=2000").await.expect("b busy");

        a.execute("BEGIN CONCURRENT").await.expect("a begin");
        b.execute("BEGIN CONCURRENT").await.expect("b begin");
        a.execute(&format!(
            "INSERT INTO u SELECT value, '{pad}' FROM generate_series(90001, 90100)"
        ))
        .await
        .expect("a explicit run 90001..90100");
        b.query("INSERT INTO u VALUES (5, 'b') RETURNING id")
            .await
            .expect("b disjoint explicit insert 5");
        b.execute("COMMIT").await.expect("b commit 5");
        assert!(
            a.execute(&format!(
                "INSERT INTO u SELECT value, CASE WHEN value = 95010 THEN NULL ELSE '{pad}' END \
                 FROM generate_series(95001, 95010)"
            ))
            .await
            .is_err(),
            "NOT NULL must fail A's second statement"
        );
        a.execute("ROLLBACK").await.expect("a rollback");

        // B holds a claim on the table, so no quiescent lowering hides the tip.
        b.execute("BEGIN CONCURRENT").await.expect("b begin 2");
        b.query("INSERT INTO u VALUES (7, 'b') RETURNING id")
            .await
            .expect("b disjoint explicit insert 7");
        a.execute("BEGIN CONCURRENT").await.expect("a begin 2");
        let a_rows = a
            .query(&format!("INSERT INTO u(pad) VALUES ('{pad}') RETURNING id"))
            .await
            .expect("a implicit insert after the runs");
        let a_id: i64 = match a_rows.first().and_then(|row| row.get(0)) {
            Some(SqliteValue::Integer(id)) => *id,
            other => panic!("unexpected a RETURNING id: {other:?}"),
        };
        assert_eq!(
            a_id, 90101,
            "every row of the first run raised the floor, and the failed second run was given back whole"
        );
        a.execute("COMMIT").await.expect("a commit 2");
        b.execute("COMMIT").await.expect("b commit 2");
        a.close().await.expect("close a");
        b.close().await.expect("close b");
    });

    let stock = rusqlite::Connection::open(&path).expect("stock open");
    assert_eq!(
        stock_query(&stock, "SELECT count(*), count(DISTINCT id), max(id) FROM u"),
        "SELECT count(*), count(DISTINCT id), max(id) FROM u => ok [\"i:203|i:203|i:90101\"]"
    );
    let check = stock_query(&stock, "PRAGMA integrity_check");
    assert_eq!(check, "PRAGMA integrity_check => ok [\"t:ok\"]");
}
