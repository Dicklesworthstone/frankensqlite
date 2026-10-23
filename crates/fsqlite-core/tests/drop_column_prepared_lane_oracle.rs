#![recursion_limit = "512"]

//! After `ALTER TABLE ... DROP COLUMN`, every read and write sees the table
//! without the column, as in stock SQLite (rusqlite).
//!
//! DROP COLUMN rewrote the stored records but left the MemDatabase row mirror
//! holding the old ones, and the prepared lane (`query_with_params`,
//! `execute_with_params`) reads that mirror: `SELECT * FROM t` still returned
//! the dropped column's values (`alter_table_oracle::rename_table_and_drop_column`).
//! Each case runs its statements through the prepared lane and through
//! `query`/`execute`, in memory and in a file, reading the table before the
//! DROP so the mirror is loaded, and compares every outcome with stock. The
//! file is read again after a reopen, and stock checks it.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const T: &[&str] = &[
    "CREATE TABLE t(a INT, b INT, c INT)",
    "INSERT INTO t VALUES (1, 2, 3), (4, 5, 6)",
];

/// (name, schema and seed, statements)
const CASES: &[(&str, &[&str], &[&str])] = &[
    (
        "drop_middle",
        T,
        &[
            "SELECT * FROM t ORDER BY a",
            "ALTER TABLE t DROP COLUMN b",
            "SELECT * FROM t ORDER BY a",
            "SELECT c FROM t WHERE a = 4",
        ],
    ),
    (
        "drop_last",
        T,
        &[
            "SELECT * FROM t ORDER BY a",
            "ALTER TABLE t DROP COLUMN c",
            "SELECT * FROM t ORDER BY a",
        ],
    ),
    (
        "insert_after_drop",
        T,
        &[
            "SELECT * FROM t ORDER BY a",
            "ALTER TABLE t DROP COLUMN b",
            "INSERT INTO t VALUES (7, 9)",
            "SELECT * FROM t ORDER BY a",
        ],
    ),
    (
        "update_after_drop",
        T,
        &[
            "SELECT * FROM t ORDER BY a",
            "ALTER TABLE t DROP COLUMN b",
            "UPDATE t SET c = c + 10 WHERE a = 1",
            "SELECT * FROM t ORDER BY a",
        ],
    ),
    (
        "delete_after_drop",
        T,
        &[
            "SELECT * FROM t ORDER BY a",
            "ALTER TABLE t DROP COLUMN b",
            "DELETE FROM t WHERE c = 6",
            "SELECT count(*), sum(c) FROM t",
        ],
    ),
    (
        "indexed_column_kept",
        &[
            "CREATE TABLE t(a INT, b INT, c INT)",
            "CREATE INDEX t_c ON t(c)",
            "INSERT INTO t VALUES (1, 2, 3), (4, 5, 6)",
        ],
        &[
            "SELECT a FROM t WHERE c = 6",
            "ALTER TABLE t DROP COLUMN b",
            "SELECT a, c FROM t WHERE c = 6",
            "SELECT * FROM t ORDER BY c DESC",
        ],
    ),
    (
        "rowid_alias_table",
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
            "INSERT INTO t VALUES (1, 'x', 'y'), (2, 'p', 'q')",
        ],
        &[
            "SELECT * FROM t ORDER BY id",
            "ALTER TABLE t DROP COLUMN a",
            "SELECT * FROM t ORDER BY id",
            "INSERT INTO t(b) VALUES ('z')",
            "SELECT rowid, * FROM t ORDER BY id",
        ],
    ),
    (
        "without_rowid",
        &[
            "CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT) WITHOUT ROWID",
            "INSERT INTO t VALUES (1, 2, 3), (4, 5, 6)",
        ],
        &[
            "SELECT * FROM t ORDER BY a",
            "ALTER TABLE t DROP COLUMN b",
            "SELECT * FROM t ORDER BY a",
            "SELECT c FROM t WHERE a = 4",
        ],
    ),
    (
        "join_after_drop",
        &[
            "CREATE TABLE t(a INT, b INT, c INT)",
            "INSERT INTO t VALUES (1, 2, 3), (4, 5, 6)",
            "CREATE TABLE u(a INT, label TEXT)",
            "INSERT INTO u VALUES (1, 'one'), (4, 'four')",
        ],
        &[
            "SELECT t.*, u.label FROM t JOIN u USING (a) ORDER BY a",
            "ALTER TABLE t DROP COLUMN b",
            "SELECT t.*, u.label FROM t JOIN u USING (a) ORDER BY a",
        ],
    ),
    (
        "committed_transaction",
        T,
        &[
            "SELECT * FROM t ORDER BY a",
            "BEGIN",
            "ALTER TABLE t DROP COLUMN b",
            "SELECT * FROM t ORDER BY a",
            "COMMIT",
            "SELECT * FROM t ORDER BY a",
        ],
    ),
    (
        "rolled_back_transaction",
        T,
        &[
            "SELECT * FROM t ORDER BY a",
            "BEGIN",
            "ALTER TABLE t DROP COLUMN b",
            "SELECT * FROM t ORDER BY a",
            "ROLLBACK",
            "SELECT * FROM t ORDER BY a",
        ],
    ),
    (
        "add_then_drop",
        T,
        &[
            "SELECT * FROM t ORDER BY a",
            "ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'x'",
            "SELECT * FROM t ORDER BY a",
            "ALTER TABLE t DROP COLUMN a",
            "SELECT * FROM t ORDER BY c",
        ],
    ),
];

const FINAL_READ: &str = "SELECT * FROM t ORDER BY 1";

/// Which public entry points run the statements.
#[derive(Clone, Copy, Debug)]
enum Lane {
    /// `query_with_params` / `execute_with_params`.
    Prepared,
    /// `query` / `execute`.
    Interpreted,
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

async fn fsqlite_run(conn: &Connection, lane: Lane, sql: &str) -> String {
    if sql.starts_with("SELECT") {
        let rows = match lane {
            Lane::Prepared => conn.query_with_params(sql, &[]).await,
            Lane::Interpreted => conn.query(sql).await,
        };
        match rows {
            Ok(rows) => format!("{sql} => ok {:?}", render_rows(&rows)),
            Err(error) => format!("{sql} => error:{error}"),
        }
    } else {
        let outcome = match lane {
            Lane::Prepared => conn.execute_with_params(sql, &[]).await,
            Lane::Interpreted => conn.execute(sql).await,
        };
        match outcome {
            Ok(_) => format!("{sql} => ok []"),
            Err(error) => format!("{sql} => error:{error}"),
        }
    }
}

/// Every statement's outcome in one stock connection, then the table as a
/// fresh connection reads it.
fn stock_expected(path: &str, seed: &[&str], steps: &[&str]) -> (Vec<String>, String) {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    for sql in seed {
        conn.execute_batch(sql).expect("stock seed");
    }
    let transcript = steps.iter().map(|sql| stock_query(&conn, sql)).collect();
    drop(conn);
    let reopened = rusqlite::Connection::open(path).expect("stock reopen");
    (transcript, stock_query(&reopened, FINAL_READ))
}

#[test]
fn reads_and_writes_after_drop_column_match_stock() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dir_path = dir.path().to_owned();
    let path_of = |name: &str| {
        dir_path
            .join(format!("{name}.db"))
            .to_str()
            .expect("utf-8 path")
            .to_owned()
    };
    let expected: Vec<(Vec<String>, String)> = CASES
        .iter()
        .map(|(name, seed, steps)| stock_expected(&path_of(&format!("stock_{name}")), seed, steps))
        .collect();
    // The oracle itself: the dropped column is gone.
    assert_eq!(
        expected[0].0[2],
        "SELECT * FROM t ORDER BY a => ok [\"i:1|i:3\", \"i:4|i:6\"]",
        "stock: {:#?}",
        expected[0].0
    );
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for ((name, seed, steps), (want, want_final)) in CASES.iter().zip(&expected) {
            for lane in [Lane::Prepared, Lane::Interpreted] {
                for in_file in [false, true] {
                    let path = path_of(&format!("{name}_{lane:?}"));
                    let label = format!("{name} ({lane:?}, {})", if in_file { "file" } else { "memory" });
                    let conn = Connection::open(if in_file { path.as_str() } else { ":memory:" })
                        .await
                        .expect("fsqlite open");
                    for sql in *seed {
                        conn.execute(sql).await.expect("fsqlite seed");
                    }
                    let mut got = Vec::new();
                    for sql in *steps {
                        got.push(fsqlite_run(&conn, lane, sql).await);
                    }
                    conn.close().await.expect("fsqlite close");
                    if &got != want {
                        mismatches.push(format!("{label}\n  fsqlite: {got:#?}\n  stock:   {want:#?}"));
                    }
                    if !in_file {
                        continue;
                    }
                    let reopened = Connection::open(&path).await.expect("fsqlite reopen");
                    let got_final = fsqlite_run(&reopened, lane, FINAL_READ).await;
                    reopened.close().await.expect("fsqlite close");
                    let stock = rusqlite::Connection::open(&path).expect("stock open");
                    let stock_final = stock_query(&stock, FINAL_READ);
                    let check = stock_query(&stock, "PRAGMA integrity_check");
                    if &got_final != want_final
                        || &stock_final != want_final
                        || check != "PRAGMA integrity_check => ok [\"t:ok\"]"
                    {
                        mismatches.push(format!(
                            "{label} after a reopen\n  fsqlite: {got_final}\n  stock reading the file: \
                             {stock_final}\n  {check}\n  want: {want_final}"
                        ));
                    }
                }
            }
        }
        for mismatch in &mismatches {
            eprintln!("event=drop_column_lane_mismatch {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} mismatches; first: {}",
            mismatches.len(),
            mismatches[0]
        );
    });
}
