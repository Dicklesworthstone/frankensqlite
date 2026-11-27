#![recursion_limit = "512"]

//! bd-u7sv8: trigger parity gaps against stock SQLite.
//!
//! - A trigger's NEW row is assembled from the statement's values (an
//!   INSERT's row, or the OLD row with the SET applied), so its generated
//!   columns read NULL in INSERT triggers and kept the OLD values in UPDATE
//!   triggers. Stock computes them from the new base columns, VIRTUAL and
//!   STORED alike, before any trigger reads NEW.
//!
//! Every case is compared against stock SQLite (rusqlite), in memory and
//! file-backed.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
CREATE TABLE g(id INTEGER PRIMARY KEY, a INTEGER, s TEXT, \
    b INTEGER GENERATED ALWAYS AS (a * 10) VIRTUAL, \
    c TEXT GENERATED ALWAYS AS ('c' || a) STORED, \
    d GENERATED ALWAYS AS (b + 1) VIRTUAL, \
    e TEXT GENERATED ALWAYS AS (a + 0.5) STORED, \
    j GENERATED ALWAYS AS (json_object('s', s, 'b', b)) VIRTUAL);\
CREATE TABLE log(seq INTEGER PRIMARY KEY, msg);\
CREATE TRIGGER g_bi BEFORE INSERT ON g BEGIN INSERT INTO log(msg) VALUES \
    ('bi ' || quote(NEW.a) || ' ' || quote(NEW.b) || ' ' || quote(NEW.c) || ' ' || quote(NEW.d) \
     || ' ' || quote(NEW.e) || ' ' || NEW.j); END;\
CREATE TRIGGER g_ai AFTER INSERT ON g BEGIN INSERT INTO log(msg) VALUES \
    ('ai ' || quote(NEW.a) || ' ' || quote(NEW.b) || ' ' || quote(NEW.c) || ' ' || quote(NEW.d) \
     || ' ' || quote(NEW.e) || ' ' || NEW.j); END;\
CREATE TRIGGER g_bu BEFORE UPDATE ON g BEGIN INSERT INTO log(msg) VALUES \
    ('bu ' || quote(OLD.b) || '>' || quote(NEW.b) || ' ' || quote(OLD.c) || '>' || quote(NEW.c) \
     || ' ' || quote(NEW.d) || ' ' || quote(NEW.e) || ' ' || NEW.j); END;\
CREATE TRIGGER g_au AFTER UPDATE ON g BEGIN INSERT INTO log(msg) VALUES \
    ('au ' || quote(OLD.b) || '>' || quote(NEW.b) || ' ' || quote(OLD.c) || '>' || quote(NEW.c) \
     || ' ' || quote(NEW.d) || ' ' || quote(NEW.e) || ' ' || NEW.j); END;\
CREATE TRIGGER g_ad AFTER DELETE ON g BEGIN INSERT INTO log(msg) VALUES \
    ('ad ' || quote(OLD.b) || ' ' || quote(OLD.c) || ' ' || quote(OLD.d) || ' ' || OLD.j); END;\
CREATE TRIGGER g_bu_when BEFORE UPDATE OF a ON g WHEN NEW.b > 1000 BEGIN \
    INSERT INTO log(msg) VALUES ('when ' || NEW.id || ' ' || NEW.b); END;\
CREATE TABLE src(k INTEGER PRIMARY KEY, delta INTEGER);\
INSERT INTO src VALUES (1, 5), (3, 7);";

const STATEMENTS: &[&str] = &[
    "INSERT INTO g(id, a, s) VALUES (1, 5, 'x')",
    "INSERT INTO g(id, a, s) VALUES (2, 6, 'y'), (3, 7, 'z')",
    "INSERT INTO g(id, a, s) SELECT k + 10, delta, 'src' FROM src",
    "UPDATE g SET a = a + 1 WHERE id = 1",
    "UPDATE g SET a = a + 100",
    "UPDATE g SET s = s || '!' WHERE id = 2",
    "UPDATE g SET a = g.a + src.delta FROM src WHERE src.k = g.id",
    "INSERT INTO g(id, a, s) VALUES (2, 9, 'dup') ON CONFLICT(id) DO UPDATE SET a = excluded.a * 2",
    "INSERT OR REPLACE INTO g(id, a, s) VALUES (3, 1, 'replaced')",
    "DELETE FROM g WHERE id = 2",
    "DELETE FROM g WHERE id > 10",
];

const DUMPS: &[&str] = &[
    "SELECT seq, msg FROM log ORDER BY seq",
    "SELECT id, a, s, b, c, d, e, j FROM g ORDER BY id",
];

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

fn stock_transcript() -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(SCHEMA).expect("stock schema");
    let query = |sql: &str| -> Result<Vec<String>, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let width = stmt.column_count();
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            out.push(
                (0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
        Ok(out)
    };
    let mut transcript = Vec::new();
    for sql in STATEMENTS {
        let outcome = match query(sql) {
            Ok(rows) => format!("ok {rows:?}"),
            Err(message) => format!("error:{message}"),
        };
        transcript.push(format!("{sql} => {outcome}"));
    }
    for sql in DUMPS {
        transcript.push(format!("{sql} => {:?}", query(sql).expect("stock dump")));
    }
    transcript
}

async fn fsqlite_transcript(conn: &Connection) -> Vec<String> {
    conn.execute_batch(SCHEMA).await.expect("schema");
    let mut transcript = Vec::new();
    for sql in STATEMENTS {
        let outcome = match conn.query(sql).await {
            Ok(rows) => format!("ok {:?}", render_rows(&rows)),
            Err(error) => format!("error:{error}"),
        };
        transcript.push(format!("{sql} => {outcome}"));
    }
    for sql in DUMPS {
        let rows = conn.query(sql).await.expect("dump");
        transcript.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    transcript
}

fn assert_matches_stock(label: &str, got: &[String], want: &[String]) {
    for (line, (got, want)) in got.iter().zip(want).enumerate() {
        assert_eq!(got, want, "bd-u7sv8 {label} parity line {line}");
    }
    assert_eq!(got.len(), want.len(), "bd-u7sv8 {label} transcript length");
}

#[test]
fn trigger_new_generated_columns_match_stock_in_memory() {
    let expected = stock_transcript();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(":memory:").await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("in-memory", &got, &expected);
    });
}

#[test]
fn trigger_new_generated_columns_match_stock_file_backed() {
    let expected = stock_transcript();
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("u7sv8.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async move {
        let conn = Connection::open(&path_str).await.expect("open");
        let got = fsqlite_transcript(&conn).await;
        assert_matches_stock("file-backed", &got, &expected);
    });
    let checked = rusqlite::Connection::open(&path).expect("stock open of fsqlite file");
    let verdict: String = checked
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(verdict, "ok", "stock integrity_check of the fsqlite file");
}

struct Scenario {
    name: &'static str,
    setup: &'static str,
    statements: &'static [&'static str],
    dumps: &'static [&'static str],
}

const RETURNING_AND_TRIGGER_SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "insert_correlated_returning_rowid",
        setup: "\
            CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
            INSERT INTO k VALUES (1, 'one'), (2, 'two'), (3, 'three');\
            CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER);\
            CREATE VIEW vk AS SELECT g, label FROM k WHERE g > 0;",
        statements: &[
            "INSERT INTO u VALUES (10, 1), (20, 2) RETURNING id, (SELECT label FROM k WHERE k.g = u.g)",
            "INSERT INTO u VALUES (30, 3) RETURNING id, (SELECT label FROM vk WHERE vk.g = u.g)",
            "INSERT INTO u VALUES (40, 1), (50, 2) RETURNING id, (SELECT count(*) FROM u AS x WHERE x.id <= u.id)",
            "INSERT INTO u SELECT g + 100, g FROM k ORDER BY g RETURNING id, (SELECT label FROM k AS x WHERE x.g = u.g)",
        ],
        dumps: &["SELECT id, g FROM u ORDER BY id"],
    },
    Scenario {
        name: "insert_correlated_returning_without_rowid",
        setup: "\
            CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
            INSERT INTO k VALUES (1, 'one'), (2, 'two');\
            CREATE TABLE u(id INTEGER NOT NULL PRIMARY KEY, g INTEGER) WITHOUT ROWID;",
        statements: &[
            "INSERT INTO u VALUES (10, 1), (20, 2) RETURNING id, (SELECT label FROM k WHERE k.g = u.g), (SELECT count(*) FROM u AS x WHERE x.id <= u.id)",
        ],
        dumps: &["SELECT id, g FROM u ORDER BY id"],
    },
    Scenario {
        name: "insert_before_after_trigger_returning_subqueries",
        setup: "\
            CREATE TABLE log(x INTEGER);\
            CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER);\
            CREATE TRIGGER bi BEFORE INSERT ON u BEGIN INSERT INTO log VALUES (NEW.id); END;\
            CREATE TRIGGER ai AFTER INSERT ON u BEGIN INSERT INTO log VALUES (-NEW.id); END;",
        statements: &[
            "INSERT INTO u VALUES (10, 1), (20, 2) RETURNING id, (SELECT count(*) FROM log), (SELECT count(*) FROM log WHERE log.x = u.id)",
            "INSERT INTO u VALUES (30, 3), (40, 4) RETURNING id, (SELECT total_changes()), (SELECT last_insert_rowid())",
        ],
        dumps: &[
            "SELECT id, g FROM u ORDER BY id",
            "SELECT x FROM log ORDER BY rowid",
        ],
    },
    Scenario {
        name: "insert_no_trigger_fromless_stateful_returning_subqueries",
        setup: "CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER);",
        statements: &[
            "INSERT INTO u VALUES (10, 1), (20, 2) RETURNING id, (SELECT last_insert_rowid()), (SELECT total_changes()), (SELECT changes())",
            "INSERT INTO u VALUES (30, 3), (40, 4) RETURNING id, last_insert_rowid(), total_changes(), changes()",
            "INSERT OR IGNORE INTO u VALUES (10, 99), (50, 5) RETURNING id, last_insert_rowid()",
            "INSERT INTO u VALUES (10, 88) ON CONFLICT(id) DO UPDATE SET g = 88 RETURNING id, last_insert_rowid()",
        ],
        dumps: &["SELECT id, g FROM u ORDER BY id"],
    },
    Scenario {
        name: "insert_upsert_and_replace_returning_subqueries_with_triggers",
        setup: "\
            CREATE TABLE log(tag TEXT, val INTEGER);\
            CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
            INSERT INTO k VALUES (1, 'one'), (2, 'two'), (9, 'nine');\
            CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER);\
            INSERT INTO u VALUES (10, 1), (20, 2);\
            CREATE TRIGGER bi BEFORE INSERT ON u BEGIN INSERT INTO log VALUES ('bi', NEW.g); END;\
            CREATE TRIGGER bu BEFORE UPDATE ON u BEGIN INSERT INTO log VALUES ('bu', NEW.g); END;\
            CREATE TRIGGER bd BEFORE DELETE ON u BEGIN INSERT INTO log VALUES ('bd', OLD.g); END;",
        statements: &[
            "INSERT INTO u VALUES (10, 2) ON CONFLICT(id) DO UPDATE SET g = 9 RETURNING id, (SELECT label FROM k WHERE k.g = u.g), (SELECT count(*) FROM log)",
            "INSERT OR REPLACE INTO u VALUES (20, 1) RETURNING id, (SELECT label FROM k WHERE k.g = u.g), (SELECT count(*) FROM log)",
        ],
        dumps: &[
            "SELECT id, g FROM u ORDER BY id",
            "SELECT tag, val FROM log ORDER BY rowid",
        ],
    },
    Scenario {
        name: "insert_generated_column_correlated_and_before_trigger_returning",
        setup: "\
            CREATE TABLE log(x INTEGER);\
            CREATE TABLE k(g INTEGER PRIMARY KEY, label TEXT);\
            INSERT INTO k VALUES (50, 'fifty'), (60, 'sixty');\
            CREATE TABLE g(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 10) VIRTUAL, c TEXT GENERATED ALWAYS AS ('c' || a) STORED);\
            CREATE TRIGGER bi BEFORE INSERT ON g BEGIN INSERT INTO log VALUES (NEW.b); END;",
        statements: &[
            "INSERT INTO g(id, a) VALUES (1, 5), (2, 6) RETURNING id, b, c, (SELECT label FROM k WHERE k.g = g.b), (SELECT count(*) FROM log)",
        ],
        dumps: &[
            "SELECT id, a, b, c FROM g ORDER BY id",
            "SELECT x FROM log ORDER BY rowid",
        ],
    },
    Scenario {
        name: "update_and_delete_before_triggers_returning_subqueries_and_with_cte",
        setup: "\
            CREATE TABLE log(x INTEGER);\
            CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER);\
            INSERT INTO u VALUES (10, 1), (20, 2), (30, 3);\
            CREATE TRIGGER bu BEFORE UPDATE ON u BEGIN INSERT INTO log VALUES (NEW.id); END;\
            CREATE TRIGGER bd BEFORE DELETE ON u BEGIN INSERT INTO log VALUES (-OLD.id); END;",
        statements: &[
            "UPDATE u SET g = g + 1 WHERE id IN (10, 20) RETURNING id, (SELECT total_changes()), (SELECT last_insert_rowid()), (SELECT count(*) FROM log)",
            "WITH c(x) AS (SELECT 100) UPDATE u SET g = (SELECT x FROM c) WHERE id IN (10, 20) RETURNING id, (SELECT count(*) FROM log)",
            "WITH c(x) AS (SELECT 20) DELETE FROM u WHERE id <= (SELECT x FROM c) RETURNING id, (SELECT count(*) FROM log)",
            "DELETE FROM u WHERE id = 30 RETURNING id, (SELECT count(*) FROM log), (SELECT total_changes())",
        ],
        dumps: &[
            "SELECT id, g FROM u ORDER BY id",
            "SELECT x FROM log ORDER BY rowid",
        ],
    },
    Scenario {
        name: "delete_before_and_after_trigger_last_insert_rowid",
        setup: "\
            CREATE TABLE aux(a INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);\
            CREATE TABLE log(tag TEXT, lid INTEGER);\
            CREATE TABLE u(id INTEGER PRIMARY KEY, g INTEGER);\
            INSERT INTO u VALUES (10, 1), (20, 2), (30, 3);\
            CREATE TRIGGER bd BEFORE DELETE ON u BEGIN \
                INSERT INTO log VALUES ('bd1', last_insert_rowid()); \
                INSERT INTO aux(v) VALUES ('bd' || OLD.id); \
                INSERT INTO log VALUES ('bd2', last_insert_rowid()); \
            END;\
            CREATE TRIGGER ad AFTER DELETE ON u BEGIN \
                INSERT INTO log VALUES ('ad1', last_insert_rowid()); \
                INSERT INTO aux(v) VALUES ('ad' || OLD.id); \
                INSERT INTO log VALUES ('ad2', last_insert_rowid()); \
            END;",
        statements: &[
            "DELETE FROM u WHERE id = 10",
            "DELETE FROM u WHERE id IN (20, 30)",
        ],
        dumps: &[
            "SELECT tag, lid FROM log ORDER BY rowid",
            "SELECT a, v FROM aux ORDER BY a",
            "SELECT last_insert_rowid()",
        ],
    },
];

fn run_stock_scenario(scenario: &Scenario) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(scenario.setup).expect("stock setup");
    let query = |sql: &str| -> Result<Vec<String>, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let width = stmt.column_count();
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            out.push(
                (0..width)
                    .map(|i| render_rusqlite(row.get_ref(i).expect("stock cell")))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
        Ok(out)
    };
    let mut out = Vec::new();
    for sql in scenario.statements {
        let res = match query(sql) {
            Ok(rows) => format!("ok {rows:?}"),
            Err(err) => format!("err:{err}"),
        };
        out.push(format!("{sql} => {res}"));
    }
    for sql in scenario.dumps {
        out.push(format!("{sql} => {:?}", query(sql).expect("stock dump")));
    }
    out
}

async fn run_fsqlite_scenario(
    conn: &Connection,
    scenario: &Scenario,
    prepared: bool,
) -> Vec<String> {
    conn.execute_batch(scenario.setup)
        .await
        .expect("fsqlite setup");
    let mut out = Vec::new();
    for sql in scenario.statements {
        let result = if prepared {
            match conn.prepare(sql).await {
                Ok(stmt) => stmt.query().await,
                Err(err) => Err(err),
            }
        } else {
            conn.query(sql).await
        };
        let res = match result {
            Ok(rows) => format!("ok {:?}", render_rows(&rows)),
            Err(err) => format!("err:{err}"),
        };
        out.push(format!("{sql} => {res}"));
    }
    for sql in scenario.dumps {
        let rows = conn.query(sql).await.expect("fsqlite dump");
        out.push(format!("{sql} => {:?}", render_rows(&rows)));
    }
    out
}

#[test]
fn trigger_and_returning_subquery_matrix_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut diffs = Vec::new();
        for scenario in RETURNING_AND_TRIGGER_SCENARIOS {
            let expected = run_stock_scenario(scenario);
            for file_backed in [false, true] {
                let mode = if file_backed { "file" } else { "mem" };
                for prepared in [false, true] {
                    let dir = file_backed.then(|| tempfile::tempdir().expect("tempdir"));
                    let conn = if let Some(ref d) = dir {
                        let p = d.path().join(format!("{}.db", scenario.name));
                        Connection::open(p.to_str().unwrap()).await.expect("open")
                    } else {
                        Connection::open(":memory:").await.expect("open")
                    };
                    let got = run_fsqlite_scenario(&conn, scenario, prepared).await;
                    if got != expected {
                        diffs.push(format!(
                            "[{}/{mode}/prepared={prepared}]\n  stock:   {expected:?}\n  fsqlite: {got:?}",
                            scenario.name
                        ));
                    }
                    conn.close().await.expect("close");
                }
            }
        }
        assert!(
            diffs.is_empty(),
            "bd-u7sv8 trigger/RETURNING subquery mismatches ({}):\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
