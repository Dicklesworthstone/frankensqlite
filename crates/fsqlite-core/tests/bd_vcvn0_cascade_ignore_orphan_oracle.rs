#![recursion_limit = "512"]

//! bd-vcvn0: an FK action whose DELETE or UPDATE of a child row is skipped by
//! a BEFORE trigger's RAISE(IGNORE) leaves that row pointing at a parent key
//! that is gone. Stock fails the statement with "FOREIGN KEY constraint
//! failed" unless the key has a parent again by the statement's end (or, for
//! a deferred key, by COMMIT). Main at 7d09e6417 let the statement succeed and
//! left the orphan: `PRAGMA foreign_key_check` then reported `c|11|p|0`.
//!
//! Oracle: bundled stock SQLite runs each script on a file; FrankenSQLite runs
//! it in memory and on a file, which stock then reads and checks.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

struct Script {
    name: &'static str,
    /// `{action}` is replaced by the child key's action clause.
    action: &'static str,
    setup: &'static [&'static str],
    steps: &'static [&'static str],
}

const CHILD: &str = "CREATE TABLE c(id INTEGER PRIMARY KEY, pid REFERENCES p(id) {action})";
const SEED: &[&str] = &[
    "PRAGMA foreign_keys = ON",
    "CREATE TABLE p(id INTEGER PRIMARY KEY)",
    CHILD,
    "INSERT INTO p VALUES (1), (2)",
    "INSERT INTO c VALUES (10, 1), (11, 1), (20, 2)",
];
const IGNORE_DELETE: &str =
    "CREATE TRIGGER t BEFORE DELETE ON c WHEN OLD.id = 11 BEGIN SELECT RAISE(IGNORE); END";
const IGNORE_UPDATE: &str =
    "CREATE TRIGGER t BEFORE UPDATE ON c WHEN OLD.id = 11 BEGIN SELECT RAISE(IGNORE); END";
const PARENTS: &str = "SELECT group_concat(id) FROM (SELECT id FROM p ORDER BY id)";
const CHILDREN: &str = "SELECT group_concat(id || ':' || ifnull(pid, 'null')) \
                        FROM (SELECT id, pid FROM c ORDER BY id)";
const FK_CHECK: &str = "PRAGMA foreign_key_check";

const SCRIPTS: &[Script] = &[
    Script {
        name: "delete_cascade_ignored",
        action: "ON DELETE CASCADE",
        setup: &[IGNORE_DELETE],
        steps: &["DELETE FROM p WHERE id = 1", PARENTS, CHILDREN, FK_CHECK],
    },
    Script {
        name: "delete_set_null_ignored",
        action: "ON DELETE SET NULL",
        setup: &[IGNORE_UPDATE],
        steps: &["DELETE FROM p WHERE id = 1", PARENTS, CHILDREN, FK_CHECK],
    },
    Script {
        name: "delete_set_default_ignored",
        action: "ON DELETE SET DEFAULT",
        setup: &[IGNORE_UPDATE],
        steps: &["DELETE FROM p WHERE id = 1", PARENTS, CHILDREN, FK_CHECK],
    },
    Script {
        name: "update_cascade_ignored",
        action: "ON UPDATE CASCADE",
        setup: &[IGNORE_UPDATE],
        steps: &["UPDATE p SET id = 5 WHERE id = 1", PARENTS, CHILDREN, FK_CHECK],
    },
    Script {
        name: "update_set_null_ignored",
        action: "ON UPDATE SET NULL",
        setup: &[IGNORE_UPDATE],
        steps: &["UPDATE p SET id = 5 WHERE id = 1", PARENTS, CHILDREN, FK_CHECK],
    },
    Script {
        name: "multi_row_delete",
        action: "ON DELETE CASCADE",
        setup: &[IGNORE_DELETE],
        steps: &["DELETE FROM p", PARENTS, CHILDREN, FK_CHECK],
    },
    Script {
        name: "grandchild_ignored",
        action: "ON DELETE CASCADE",
        setup: &[
            "CREATE TABLE g(id INTEGER PRIMARY KEY, cid REFERENCES c(id) ON DELETE CASCADE)",
            "INSERT INTO g VALUES (100, 10), (101, 11)",
            "CREATE TRIGGER tg BEFORE DELETE ON g WHEN OLD.id = 101 \
             BEGIN SELECT RAISE(IGNORE); END",
        ],
        steps: &[
            "DELETE FROM p WHERE id = 1",
            PARENTS,
            CHILDREN,
            "SELECT group_concat(id) FROM (SELECT id FROM g ORDER BY id)",
            FK_CHECK,
        ],
    },
    // The parent key exists again by the statement's end.
    Script {
        name: "parent_reinserted_by_trigger",
        action: "ON DELETE CASCADE",
        setup: &[
            IGNORE_DELETE,
            "CREATE TRIGGER pd AFTER DELETE ON p BEGIN INSERT INTO p VALUES (OLD.id); END",
        ],
        steps: &["DELETE FROM p WHERE id = 1", PARENTS, CHILDREN, FK_CHECK],
    },
    // The trigger ignores a row the action does not touch.
    Script {
        name: "ignore_of_unrelated_row",
        action: "ON DELETE CASCADE",
        setup: &[
            "CREATE TRIGGER t BEFORE DELETE ON c WHEN OLD.id = 20 \
             BEGIN SELECT RAISE(IGNORE); END",
        ],
        steps: &["DELETE FROM p WHERE id = 1", PARENTS, CHILDREN, FK_CHECK],
    },
    // An orphan written while foreign_keys was OFF does not fail a statement
    // that leaves no new one.
    Script {
        name: "existing_orphan_elsewhere",
        action: "ON DELETE CASCADE",
        setup: &[
            IGNORE_DELETE,
            "PRAGMA foreign_keys = OFF",
            "INSERT INTO c VALUES (30, 99)",
            "PRAGMA foreign_keys = ON",
        ],
        steps: &["DELETE FROM p WHERE id = 2", PARENTS, CHILDREN, FK_CHECK],
    },
    Script {
        name: "deferred_commit_fails",
        action: "ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED",
        setup: &[IGNORE_DELETE],
        steps: &[
            "BEGIN",
            "DELETE FROM p WHERE id = 1",
            "COMMIT",
            "ROLLBACK",
            PARENTS,
            CHILDREN,
            FK_CHECK,
        ],
    },
    Script {
        name: "deferred_parent_restored",
        action: "ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED",
        setup: &[IGNORE_DELETE],
        steps: &[
            "BEGIN",
            "DELETE FROM p WHERE id = 1",
            "INSERT INTO p VALUES (1)",
            "COMMIT",
            PARENTS,
            CHILDREN,
            FK_CHECK,
        ],
    },
];

fn render_stock(value: rusqlite::types::ValueRef<'_>) -> String {
    match value {
        rusqlite::types::ValueRef::Null => "NULL".to_owned(),
        rusqlite::types::ValueRef::Integer(n) => n.to_string(),
        rusqlite::types::ValueRef::Real(f) => format!("{f:.9e}"),
        rusqlite::types::ValueRef::Text(text) => String::from_utf8_lossy(text).into_owned(),
        rusqlite::types::ValueRef::Blob(bytes) => format!("{bytes:?}"),
    }
}

fn stock_step(conn: &rusqlite::Connection, sql: &str) -> String {
    let run = || -> rusqlite::Result<Vec<String>> {
        let mut statement = conn.prepare(sql)?;
        let width = statement.column_count();
        let mut rows = statement.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let mut cells = Vec::with_capacity(width);
            for index in 0..width {
                cells.push(render_stock(row.get_ref(index)?));
            }
            out.push(cells.join("|"));
        }
        Ok(out)
    };
    match run() {
        Ok(rows) => format!("ok {rows:?}"),
        Err(error) => format!("error {error}"),
    }
}

fn render_rows(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f:.9e}"),
                    SqliteValue::Text(text) => text.to_string(),
                    SqliteValue::Blob(bytes) => format!("{bytes:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn fsqlite_step(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => format!("ok {:?}", render_rows(&rows)),
        Err(error) => format!("error {error}"),
    }
}

fn setup_sql(script: &Script) -> Vec<String> {
    SEED.iter()
        .map(|sql| sql.replace("{action}", script.action))
        .chain(script.setup.iter().map(|sql| (*sql).to_owned()))
        .collect()
}

fn transcript(steps: &[&str], outcomes: Vec<String>) -> Vec<String> {
    steps
        .iter()
        .zip(outcomes)
        .map(|(sql, outcome)| format!("{sql} -> {outcome}"))
        .collect()
}

fn compare(label: &str, got: &[String], want: &[String], mismatches: &mut Vec<String>) {
    if got != want {
        mismatches.push(format!("{label}\n  fsqlite: {got:#?}\n  stock:   {want:#?}"));
    }
}

#[test]
fn fk_actions_skipped_by_raise_ignore_match_stock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut expected = Vec::new();
    for script in SCRIPTS {
        let path = dir.path().join(format!("{}-stock.db", script.name));
        let stock = rusqlite::Connection::open(&path).expect("stock open");
        for sql in setup_sql(script) {
            stock.execute_batch(&sql).expect("stock setup");
        }
        let outcomes = script.steps.iter().map(|sql| stock_step(&stock, sql)).collect();
        expected.push(transcript(script.steps, outcomes));
    }
    // The oracle itself: stock refuses the orphaning DELETE.
    assert_eq!(
        expected[0][0], "DELETE FROM p WHERE id = 1 -> error FOREIGN KEY constraint failed",
        "stock: {:#?}",
        expected[0]
    );
    let dir_path = dir.path().to_owned();
    asupersync::test_utils::run_test(|| async move {
        let mut mismatches = Vec::new();
        for (script, want) in SCRIPTS.iter().zip(&expected) {
            let conn = Connection::open(":memory:").await.expect("open");
            for sql in setup_sql(script) {
                conn.execute(&sql).await.expect("setup");
            }
            let mut outcomes = Vec::new();
            for sql in script.steps {
                outcomes.push(fsqlite_step(&conn, sql).await);
            }
            let got = transcript(script.steps, outcomes);
            compare(&format!("{} (memory)", script.name), &got, want, &mut mismatches);
            conn.close().await.expect("close");

            let path = dir_path.join(format!("{}-fsqlite.db", script.name));
            let path_text = path.to_str().expect("utf-8 path");
            let conn = Connection::open(path_text).await.expect("open file");
            for sql in setup_sql(script) {
                conn.execute(&sql).await.expect("file setup");
            }
            let mut outcomes = Vec::new();
            for sql in script.steps {
                outcomes.push(fsqlite_step(&conn, sql).await);
            }
            let got = transcript(script.steps, outcomes);
            compare(&format!("{} (file)", script.name), &got, want, &mut mismatches);
            conn.close().await.expect("close file");

            let stock = rusqlite::Connection::open(&path).expect("stock reads the file");
            let checks = [PARENTS, CHILDREN, FK_CHECK, "PRAGMA integrity_check"];
            let got: Vec<String> = checks.iter().map(|sql| stock_step(&stock, sql)).collect();
            let want_checks: Vec<String> = [PARENTS, CHILDREN, FK_CHECK]
                .iter()
                .map(|sql| {
                    want.iter()
                        .rev()
                        .find_map(|line| line.strip_prefix(&format!("{sql} -> ")))
                        .unwrap_or("missing")
                        .to_owned()
                })
                .chain(std::iter::once("ok [\"ok\"]".to_owned()))
                .collect();
            compare(
                &format!("{} (stock reading the fsqlite file)", script.name),
                &got,
                &want_checks,
                &mut mismatches,
            );
        }
        for mismatch in &mismatches {
            eprintln!("event=fk_action_ignore_mismatch {mismatch}");
        }
        assert!(
            mismatches.is_empty(),
            "{} mismatches; first: {}",
            mismatches.len(),
            mismatches[0]
        );
    });
}

/// Inside an explicit transaction, stock reports the error but keeps the
/// statement's changes: it opens no statement journal for a CASCADE or SET
/// NULL key (fkey.c skips sqlite3MayAbort, trusting the action to repair any
/// violation), so the parent row stays deleted and the orphan stays. That
/// leaves the very violation the error reports. FrankenSQLite rolls the
/// statement back, so the transaction holds no orphan.
#[test]
fn fk_action_skipped_inside_a_transaction_rolls_the_statement_back() {
    const SCRIPT: Script = Script {
        name: "explicit_transaction",
        action: "ON DELETE CASCADE",
        setup: &[IGNORE_DELETE],
        steps: &[],
    };
    let stock = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in setup_sql(&SCRIPT) {
        stock.execute_batch(&sql).expect("stock setup");
    }
    let stock_outcome: Vec<String> = [
        "BEGIN",
        "INSERT INTO p VALUES (3)",
        "DELETE FROM p WHERE id = 1",
        PARENTS,
        CHILDREN,
        FK_CHECK,
    ]
    .iter()
    .map(|sql| stock_step(&stock, sql))
    .collect();
    assert_eq!(
        stock_outcome[2..],
        [
            "error FOREIGN KEY constraint failed".to_owned(),
            "ok [\"2,3\"]".to_owned(),
            "ok [\"11:1,20:2\"]".to_owned(),
            "ok [\"c|11|p|0\"]".to_owned(),
        ],
        "stock's partial statement changed: {stock_outcome:#?}"
    );
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        for sql in setup_sql(&SCRIPT) {
            conn.execute(&sql).await.expect("setup");
        }
        let mut outcome = Vec::new();
        for sql in [
            "BEGIN",
            "INSERT INTO p VALUES (3)",
            "DELETE FROM p WHERE id = 1",
            PARENTS,
            CHILDREN,
            FK_CHECK,
            "COMMIT",
            PARENTS,
            CHILDREN,
        ] {
            outcome.push(fsqlite_step(&conn, sql).await);
        }
        assert_eq!(
            outcome,
            [
                "ok []",
                "ok []",
                "error FOREIGN KEY constraint failed",
                "ok [\"1,2,3\"]",
                "ok [\"10:1,11:1,20:2\"]",
                "ok []",
                "ok []",
                "ok [\"1,2,3\"]",
                "ok [\"10:1,11:1,20:2\"]",
            ],
            "the failed DELETE is rolled back and the INSERT before it kept"
        );
        conn.close().await.expect("close");
    });
}
