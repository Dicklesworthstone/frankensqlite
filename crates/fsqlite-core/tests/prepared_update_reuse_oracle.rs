#![recursion_limit = "512"]

//! Review of a31225e8b (bd-9ag5r in-place UPDATE), 36b2bbf09 (bd-c1vth exact
//! index restore), 2b6215390 / 826037063 (UPDATE ... FROM with triggers) and
//! f41221f85 (parameterized replay): the per-row state those commits keep in
//! the VDBE (a deferred delete, the index entries an UPDATE removed, replay
//! parameters numbered after the statement's own) must not leak from one
//! execution of a PREPARED statement into the next — including executions that
//! fail partway through a multi-row statement, or skip rows on a conflict.
//!
//! Each scenario runs the same sequence of executions through one fsqlite
//! prepared statement and through one rusqlite statement on separate
//! file-backed databases, comparing every execution's outcome and the final
//! contents, then stock `integrity_check` on the fsqlite file.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use rusqlite::types::Value;

fn to_stock(value: &SqliteValue) -> Value {
    match value {
        SqliteValue::Null => Value::Null,
        SqliteValue::Integer(v) => Value::Integer(*v),
        SqliteValue::Float(v) => Value::Real(*v),
        SqliteValue::Text(v) => Value::Text(v.to_string()),
        SqliteValue::Blob(v) => Value::Blob(v.to_vec()),
    }
}

fn render(value: &Value) -> String {
    match value {
        Value::Null => "N".to_owned(),
        Value::Integer(v) => format!("I{v}"),
        Value::Real(v) => format!("R{v}"),
        Value::Text(v) => format!("T{v}"),
        Value::Blob(v) => format!("B{v:?}"),
    }
}

/// Outcome of one execution: the change count, or the error class. Messages
/// are compared only by whether the statement failed; the row contents and
/// counters around them are what this test is about.
fn outcome<E: std::fmt::Display>(result: Result<usize, E>) -> String {
    match result {
        Ok(n) => format!("ok {n}"),
        Err(error) => {
            let text = error.to_string();
            if text.contains("UNIQUE") {
                "err UNIQUE".to_owned()
            } else if text.contains("CHECK") {
                "err CHECK".to_owned()
            } else if text.contains("NOT NULL") {
                "err NOT NULL".to_owned()
            } else {
                format!("err {text}")
            }
        }
    }
}

struct Scenario {
    name: &'static str,
    setup: &'static str,
    statement: &'static str,
    executions: Vec<Vec<SqliteValue>>,
    dump: &'static [&'static str],
}

fn int(v: i64) -> SqliteValue {
    SqliteValue::Integer(v)
}

fn scenarios() -> Vec<Scenario> {
    let mut seed = String::new();
    for i in 1..=400 {
        seed.push_str(&format!(
            "INSERT INTO u VALUES({i}, {}, {}, '{}');\n",
            i * 10,
            i % 37,
            "p".repeat(20 + i % 60)
        ));
    }
    let seed: &'static str = Box::leak(seed.into_boxed_str());
    let with_seed = |head: &str, tail: &str| -> &'static str {
        Box::leak(format!("{head}\nBEGIN;\n{seed}COMMIT;\n{tail}").into_boxed_str())
    };
    vec![
        Scenario {
            // bd-c1vth restore + 9ag5r in-place path, OR IGNORE skipping some
            // rows of each execution.
            name: "or_ignore_reused",
            setup: with_seed(
                "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c, d);\n\
                 CREATE INDEX u_c ON u(c);\nCREATE INDEX u_cd ON u(c, d);\n\
                 CREATE INDEX u_expr ON u(c + a);\nCREATE INDEX u_part ON u(d) WHERE c > 10;",
                "",
            ),
            statement: "UPDATE OR IGNORE u SET b = b + ?1, c = c + 1 WHERE a BETWEEN ?2 AND ?3",
            executions: vec![
                vec![int(10), int(1), int(50)],
                vec![int(0), int(1), int(400)],
                vec![int(-10), int(30), int(90)],
                vec![int(10), int(100), int(200)],
                vec![int(5), int(1), int(400)],
            ],
            dump: &["SELECT a, b, c, d FROM u ORDER BY a"],
        },
        Scenario {
            // A multi-row UPDATE that fails partway (CHECK on a later row),
            // then succeeds: no deferred delete or restore list may leak.
            name: "abort_midway_then_reuse",
            setup: with_seed(
                "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c CHECK (c < ?), d);",
                "",
            )
            .replace("CHECK (c < ?)", "CHECK (c < 60)")
            .leak(),
            statement: "UPDATE u SET c = c + ?1, d = d || 'x' WHERE a <= ?2",
            executions: vec![
                vec![int(1), int(400)],
                vec![int(30), int(400)],
                vec![int(2), int(50)],
                vec![int(25), int(10)],
                vec![int(-5), int(400)],
            ],
            dump: &["SELECT a, b, c, d FROM u ORDER BY a"],
        },
        Scenario {
            // OR REPLACE deleting other rows between executions.
            name: "or_replace_reused",
            setup: with_seed(
                "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c, d);\nCREATE INDEX u_c ON u(c);",
                "",
            ),
            statement: "UPDATE OR REPLACE u SET b = ?1 WHERE a = ?2",
            executions: vec![
                vec![int(20), int(1)],
                vec![int(30), int(5)],
                vec![int(990), int(7)],
                vec![int(70), int(990)],
                vec![int(80), int(9)],
            ],
            dump: &["SELECT a, b, c, d FROM u ORDER BY a"],
        },
        Scenario {
            // UPDATE ... FROM with a trigger (replay path), parameters, repeated
            // FROM matches, reused.
            name: "update_from_trigger_reused",
            setup: with_seed(
                "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c, d);\nCREATE INDEX u_c ON u(c);\n\
                 CREATE TABLE src(k, nc);\nCREATE TABLE log(a, oldc, newc);\n\
                 CREATE TRIGGER u_au AFTER UPDATE ON u BEGIN INSERT INTO log VALUES(NEW.a, OLD.c, NEW.c); END;",
                "INSERT INTO src VALUES (1, 5), (1, 7), (2, 11), (3, 13), (3, 17), (4, 19);",
            ),
            statement: "UPDATE OR IGNORE u SET c = c + v.nc, b = b + ?3 FROM src AS v WHERE v.k = ?1 AND u.a % 37 = ?2",
            executions: vec![
                vec![int(1), int(3), int(0)],
                vec![int(3), int(3), int(1)],
                vec![int(2), int(5), int(10)],
                vec![int(4), int(0), int(-10)],
                vec![int(1), int(3), int(0)],
            ],
            dump: &[
                "SELECT a, b, c, d FROM u ORDER BY a",
                "SELECT a, oldc, newc FROM log ORDER BY rowid",
            ],
        },
        Scenario {
            // UPDATE ... FROM without triggers (two-pass VDBE lane) on a
            // WITHOUT ROWID table, reused.
            name: "update_from_without_rowid_reused",
            setup: with_seed(
                "CREATE TABLE u0(a INTEGER PRIMARY KEY, b UNIQUE, c, d);",
                "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c, d) WITHOUT ROWID;\n\
                 CREATE INDEX u_c ON u(c);\nINSERT INTO u SELECT * FROM u0;\n\
                 CREATE TABLE src(k, nc);\nINSERT INTO src VALUES (1, 5), (1, 7), (2, 11), (3, 13);",
            )
            .replace("INSERT INTO u VALUES", "INSERT INTO u0 VALUES")
            .leak(),
            statement: "UPDATE u SET c = c + v.nc FROM src AS v WHERE v.k = ?1 AND u.c = ?2",
            executions: vec![
                vec![int(1), int(3)],
                vec![int(1), int(15)],
                vec![int(2), int(10)],
                vec![int(3), int(21)],
            ],
            dump: &["SELECT a, b, c, d FROM u ORDER BY a"],
        },
    ]
}

fn stock_run(path: &std::path::Path, scenario: &Scenario) -> (Vec<String>, Vec<String>) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    conn.execute_batch(scenario.setup).unwrap();
    let mut outcomes = Vec::new();
    {
        let mut stmt = conn.prepare(scenario.statement).unwrap();
        for params in &scenario.executions {
            let params: Vec<Value> = params.iter().map(to_stock).collect();
            let before = conn.total_changes();
            let result = stmt.execute(rusqlite::params_from_iter(params.iter()));
            let delta = i64::try_from(conn.total_changes() - before).unwrap();
            outcomes.push(format!("{} total+{delta}", outcome(result)));
        }
    }
    let mut dump = Vec::new();
    for query in scenario.dump {
        let mut stmt = conn.prepare(query).unwrap();
        let width = stmt.column_count();
        let mut rows = stmt.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            dump.push(
                (0..width)
                    .map(|i| render(&row.get::<_, Value>(i).unwrap()))
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
    }
    (outcomes, dump)
}

async fn total_changes(conn: &Connection) -> i64 {
    let rows = conn.query("SELECT total_changes()").await.unwrap();
    match rows[0].values()[0] {
        SqliteValue::Integer(v) => v,
        ref other => panic!("total_changes() = {other:?}"),
    }
}

async fn fsqlite_run(path: &str, scenario: &Scenario) -> (Vec<String>, Vec<String>) {
    let conn = Connection::open(path.to_owned()).await.unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL;")
        .await
        .unwrap();
    conn.execute_batch(scenario.setup).await.unwrap();
    let mut outcomes = Vec::new();
    {
        let stmt = conn.prepare(scenario.statement).await.unwrap();
        for params in &scenario.executions {
            let before = total_changes(&conn).await;
            let result = stmt.execute_with_params(params).await;
            let delta = total_changes(&conn).await - before;
            outcomes.push(format!("{} total+{delta}", outcome(result)));
        }
    }
    let mut dump = Vec::new();
    for query in scenario.dump {
        for row in conn.query(query).await.unwrap() {
            dump.push(
                row.values()
                    .iter()
                    .map(|v| render(&to_stock(v)))
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
    }
    conn.close().await.unwrap();
    (outcomes, dump)
}

#[test]
fn reused_prepared_updates_match_stock() {
    let dir = tempfile::tempdir().unwrap();
    let mut failures = Vec::new();
    for scenario in scenarios() {
        let stock_path = dir.path().join(format!("{}-stock.db", scenario.name));
        let fsqlite_path = dir.path().join(format!("{}-fsqlite.db", scenario.name));
        let want = stock_run(&stock_path, &scenario);
        let path = fsqlite_path.to_str().unwrap().to_owned();
        let mut got = (Vec::new(), Vec::new());
        asupersync::test_utils::run_test(|| async { got = fsqlite_run(&path, &scenario).await });
        if got.0 != want.0 {
            failures.push(format!(
                "{}: outcomes\n  fsqlite {:?}\n  stock   {:?}",
                scenario.name, got.0, want.0
            ));
        }
        if got.1 != want.1 {
            let first = got
                .1
                .iter()
                .zip(&want.1)
                .position(|(g, w)| g != w)
                .unwrap_or_else(|| got.1.len().min(want.1.len()));
            failures.push(format!(
                "{}: contents differ ({} vs {} rows), first at {first}: fsqlite {:?} stock {:?}",
                scenario.name,
                got.1.len(),
                want.1.len(),
                got.1.get(first),
                want.1.get(first)
            ));
        }
        let stock = rusqlite::Connection::open(&fsqlite_path).unwrap();
        let check: String = stock
            .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
            .unwrap();
        if check != "ok" {
            failures.push(format!("{}: integrity_check {check}", scenario.name));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
