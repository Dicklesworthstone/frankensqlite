//! bd-w78tx: stock SQLite compiles every trigger a DML statement can fire
//! while it prepares the statement, whatever the trigger's WHEN clause, and
//! refuses the statement when a trigger's WHEN or body names a relation that
//! does not exist: `no such table: main.missing`. One broken trigger refuses
//! every statement that could fire it, before any row changes. fsqlite failed
//! only when the broken trigger actually fired, so it answered statements
//! stock refuses.
//!
//! Each scenario runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, with the probe statements run directly and prepared. Every
//! probe's outcome (its rows, or its error text) must equal stock's, and the
//! probes that matter are also pinned to stock's outcome so the comparison
//! cannot pass with both engines doing the same wrong thing.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

/// Prepare `sql` on stock and step it to the end: its rows, or the error of
/// the prepare or of a step.
fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let result = (|| -> rusqlite::Result<Vec<Vec<String>>> {
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows = Vec::new();
        let mut cursor = statement.query([])?;
        while let Some(row) = cursor.next()? {
            rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect(),
            );
        }
        Ok(rows)
    })();
    match result {
        Ok(rows) => format!("rows={rows:?}"),
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

/// `sql` on fsqlite, run directly (`Connection::query`) or prepared first
/// (a prepared SELECT is queried, a prepared DML statement executed). The
/// probes' DDL and PRAGMA steps always run directly.
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let preparable = ["SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE"]
        .iter()
        .any(|keyword| sql.starts_with(keyword));
    let result = if prepared && preparable {
        match conn.prepare(sql).await {
            Ok(statement) if sql.starts_with("SELECT") => statement.query().await,
            Ok(statement) => statement.execute().await.map(|_| Vec::new()),
            Err(error) => Err(error),
        }
    } else {
        conn.query(sql).await
    };
    match result {
        Ok(rows) => {
            let rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect();
            format!("rows={rows:?}")
        }
        Err(error) => format!("error: {error}"),
    }
}

/// Setup statements, then probes, each with the outcome stock must give.
struct Scenario {
    name: &'static str,
    setup: &'static [&'static str],
    probes: &'static [(&'static str, &'static str)],
}

const BASE: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b UNIQUE)",
    "CREATE TABLE lg(x)",
    "CREATE TABLE other(z)",
];

const NO_ROWS: &str = "rows=[]";

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "the bead's shape: a WHEN that is false for the row, beside a valid trigger",
        setup: &[
            "CREATE TRIGGER t_ok AFTER INSERT ON t BEGIN INSERT INTO lg VALUES (NEW.a); END",
            "CREATE TRIGGER t_bad AFTER INSERT ON t WHEN NEW.a = 99 \
             BEGIN INSERT INTO missing VALUES (NEW.a); END",
        ],
        probes: &[
            (
                "INSERT INTO t VALUES (1, 2)",
                "error: no such table: main.missing",
            ),
            ("SELECT count(*) FROM t", "rows=[[\"0\"]]"),
            ("SELECT count(*) FROM lg", "rows=[[\"0\"]]"),
            // Negatives: the other events have no broken trigger.
            ("UPDATE t SET b = 3", NO_ROWS),
            ("DELETE FROM t", NO_ROWS),
            (
                "INSERT INTO t SELECT 5, 6 WHERE 0",
                "error: no such table: main.missing",
            ),
            // Once the table exists the statement runs, as in stock.
            ("CREATE TABLE missing(x)", NO_ROWS),
            ("INSERT INTO t VALUES (3, 4)", NO_ROWS),
            ("SELECT a, b FROM t", "rows=[[\"3\", \"4\"]]"),
            ("SELECT count(*) FROM lg", "rows=[[\"1\"]]"),
            ("DROP TABLE missing", NO_ROWS),
            (
                "INSERT INTO t VALUES (5, 6)",
                "error: no such table: main.missing",
            ),
        ],
    },
    Scenario {
        name: "UPDATE and DELETE triggers, and UPDATE OF",
        setup: &[
            "INSERT INTO t VALUES (1, 1)",
            "CREATE TRIGGER t_upd AFTER UPDATE OF a ON t WHEN 0 BEGIN UPDATE mu SET x = 1; END",
            "CREATE TRIGGER t_del BEFORE DELETE ON t WHEN 0 BEGIN DELETE FROM md; END",
        ],
        probes: &[
            ("UPDATE t SET b = 5", NO_ROWS),
            ("UPDATE t SET a = 5", "error: no such table: main.mu"),
            ("UPDATE t SET b = 6, a = 7", "error: no such table: main.mu"),
            ("DELETE FROM t WHERE a = 9", "error: no such table: main.md"),
            ("INSERT INTO t VALUES (2, 2)", NO_ROWS),
            (
                "SELECT a, b FROM t ORDER BY a",
                "rows=[[\"1\", \"5\"], [\"2\", \"2\"]]",
            ),
        ],
    },
    Scenario {
        name: "stock's order: BEFORE triggers first, then the newest",
        setup: &[
            "CREATE TRIGGER r1 BEFORE INSERT ON t WHEN 0 BEGIN INSERT INTO m1 VALUES (1); END",
            "CREATE TRIGGER r2 AFTER INSERT ON t WHEN 0 BEGIN INSERT INTO m2 VALUES (1); END",
            "CREATE TRIGGER r3 AFTER INSERT ON t WHEN 0 BEGIN INSERT INTO m3 VALUES (1); END",
            "CREATE TRIGGER r4 AFTER DELETE ON t WHEN 0 BEGIN INSERT INTO m4 VALUES (1); END",
            "CREATE TRIGGER r5 AFTER DELETE ON t WHEN 0 BEGIN INSERT INTO m5 VALUES (1); END",
        ],
        probes: &[
            (
                "INSERT INTO t VALUES (1, 1)",
                "error: no such table: main.m1",
            ),
            ("DELETE FROM t", "error: no such table: main.m5"),
        ],
    },
    Scenario {
        name: "a WHEN and the statements of a body",
        setup: &[
            "CREATE TRIGGER r AFTER INSERT ON t WHEN (SELECT count(*) FROM mw) \
             BEGIN INSERT INTO mb VALUES (1); END",
            "CREATE TRIGGER s AFTER UPDATE ON t WHEN 0 \
             BEGIN INSERT INTO lg VALUES (1); INSERT INTO s2 VALUES (1); END",
            "CREATE TRIGGER d AFTER DELETE ON t WHEN 0 \
             BEGIN UPDATE lg SET x = 1 WHERE x IN (SELECT q FROM mq); END",
        ],
        probes: &[
            (
                "INSERT INTO t VALUES (1, 1)",
                "error: no such table: main.mw",
            ),
            ("UPDATE t SET b = 2", "error: no such table: main.s2"),
            ("DELETE FROM t", "error: no such table: main.mq"),
        ],
    },
    Scenario {
        name: "relations in a body's sources and subqueries",
        setup: &[
            "CREATE TRIGGER r AFTER INSERT ON t WHEN 0 BEGIN INSERT INTO lg SELECT x FROM ms; END",
            "CREATE TRIGGER s AFTER UPDATE ON t WHEN 0 \
             BEGIN SELECT 1 WHERE EXISTS (SELECT 1 FROM me); END",
            "CREATE TRIGGER d AFTER DELETE ON t WHEN 0 \
             BEGIN UPDATE lg SET x = (SELECT y FROM my); END",
        ],
        probes: &[
            (
                "INSERT INTO t VALUES (1, 1)",
                "error: no such table: main.ms",
            ),
            ("UPDATE t SET b = 2", "error: no such table: main.me"),
            ("DELETE FROM t", "error: no such table: main.my"),
        ],
    },
    Scenario {
        name: "a persistent trigger resolves in MAIN only; a TEMP trigger sees TEMP",
        setup: &[
            "CREATE TEMP TABLE tmp(q)",
            "CREATE TEMP VIEW tv AS SELECT 1 AS q",
            "CREATE TRIGGER r AFTER INSERT ON t WHEN 0 BEGIN INSERT INTO tmp VALUES (NEW.a); END",
            "CREATE TRIGGER s AFTER UPDATE ON t WHEN 0 BEGIN INSERT INTO lg SELECT q FROM tv; END",
            "CREATE TEMP TRIGGER d AFTER DELETE ON t BEGIN INSERT INTO tmp VALUES (OLD.a); END",
            "CREATE TEMP TABLE tt(a)",
            "CREATE TEMP TRIGGER tt_bad AFTER INSERT ON tt WHEN 0 \
             BEGIN INSERT INTO missing VALUES (1); END",
        ],
        probes: &[
            (
                "INSERT INTO t VALUES (1, 1)",
                "error: no such table: main.tmp",
            ),
            ("UPDATE t SET b = 2", "error: no such table: main.tv"),
            ("DELETE FROM t", NO_ROWS),
            ("INSERT INTO tt VALUES (1)", "error: no such table: missing"),
        ],
    },
    Scenario {
        name: "an upsert's DO UPDATE compiles the UPDATE triggers",
        setup: &[
            "INSERT INTO t VALUES (6, 6)",
            "CREATE TRIGGER r AFTER UPDATE ON t WHEN 0 BEGIN INSERT INTO missing VALUES (1); END",
        ],
        probes: &[
            (
                "INSERT INTO t VALUES (6, 6) ON CONFLICT(a) DO NOTHING",
                NO_ROWS,
            ),
            (
                "INSERT INTO t VALUES (6, 6) ON CONFLICT(a) DO UPDATE SET b = 7",
                "error: no such table: main.missing",
            ),
            ("INSERT INTO t VALUES (7, 7)", NO_ROWS),
            (
                "SELECT a, b FROM t ORDER BY a",
                "rows=[[\"6\", \"6\"], [\"7\", \"7\"]]",
            ),
        ],
    },
    Scenario {
        name: "a REPLACE compiles the DELETE triggers only under recursive_triggers",
        setup: &[
            "INSERT INTO t VALUES (6, 6)",
            "CREATE TRIGGER r AFTER DELETE ON t WHEN 0 BEGIN INSERT INTO missing VALUES (1); END",
        ],
        probes: &[
            ("INSERT OR REPLACE INTO t VALUES (6, 9)", NO_ROWS),
            ("REPLACE INTO t VALUES (7, 9)", NO_ROWS),
            ("SELECT a, b FROM t ORDER BY a", "rows=[[\"7\", \"9\"]]"),
            ("PRAGMA recursive_triggers = 1", NO_ROWS),
            (
                "INSERT OR REPLACE INTO t VALUES (7, 10)",
                "error: no such table: main.missing",
            ),
            ("SELECT a, b FROM t ORDER BY a", "rows=[[\"7\", \"9\"]]"),
        ],
    },
    Scenario {
        name: "an INSTEAD OF trigger on a view",
        setup: &[
            "CREATE VIEW v AS SELECT a, b FROM t",
            "CREATE TRIGGER r INSTEAD OF INSERT ON v WHEN 0 \
             BEGIN INSERT INTO missing VALUES (NEW.a); END",
        ],
        probes: &[
            (
                "INSERT INTO v VALUES (10, 14)",
                "error: no such table: main.missing",
            ),
            ("SELECT count(*) FROM t", "rows=[[\"0\"]]"),
        ],
    },
    Scenario {
        name: "negatives: bodies stock compiles",
        setup: &[
            "CREATE VIEW lv AS SELECT x FROM lg",
            "CREATE TRIGGER r1 BEFORE INSERT ON t WHEN NEW.a = 99 \
             BEGIN SELECT RAISE(ABORT, 'no 99'); END",
            "CREATE TRIGGER r2 AFTER INSERT ON t \
             BEGIN INSERT INTO lg WITH c AS (SELECT NEW.a AS v) SELECT v FROM c; END",
            "CREATE TRIGGER r3 AFTER INSERT ON t \
             BEGIN INSERT INTO lg WITH missing AS (SELECT 1) SELECT * FROM missing; END",
            "CREATE TRIGGER r4 AFTER INSERT ON t \
             BEGIN INSERT INTO lg SELECT value FROM json_each('[1,2]'); END",
            "CREATE TRIGGER r5 AFTER INSERT ON t \
             BEGIN INSERT INTO lg SELECT count(*) FROM pragma_table_info('t'); END",
            "CREATE TRIGGER r6 AFTER INSERT ON t \
             BEGIN INSERT INTO lg SELECT count(*) FROM sqlite_master; END",
            "CREATE TRIGGER r7 AFTER INSERT ON t BEGIN INSERT INTO lg SELECT count(*) FROM lv; END",
            "CREATE TRIGGER r8 AFTER INSERT ON other WHEN 0 BEGIN INSERT INTO missing VALUES (1); END",
        ],
        probes: &[
            ("INSERT INTO t VALUES (1, 1)", NO_ROWS),
            ("INSERT INTO t VALUES (99, 2)", "error: no 99"),
            ("SELECT count(*) FROM t", "rows=[[\"1\"]]"),
            ("SELECT count(*) FROM lg", "rows=[[\"7\"]]"),
            (
                "INSERT INTO other VALUES (1)",
                "error: no such table: main.missing",
            ),
            ("SELECT count(*) FROM other", "rows=[[\"0\"]]"),
        ],
    },
];

#[test]
fn triggers_naming_missing_relations_refuse_their_statements_like_stock() {
    let mut expected: Vec<Vec<String>> = Vec::new();
    for scenario in SCENARIOS {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in BASE.iter().chain(scenario.setup) {
            stock
                .execute_batch(sql)
                .unwrap_or_else(|error| panic!("{}: stock setup `{sql}`: {error}", scenario.name));
        }
        let outcomes: Vec<String> = scenario
            .probes
            .iter()
            .map(|(sql, _)| stock_outcome(&stock, sql))
            .collect();
        // Not vacuous: stock gives each pinned outcome.
        for ((sql, pinned), stock_gave) in scenario.probes.iter().zip(&outcomes) {
            assert_eq!(stock_gave, pinned, "{}: stock on `{sql}`", scenario.name);
        }
        expected.push(outcomes);
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                for (scenario, expected) in SCENARIOS.iter().zip(&expected) {
                    let dir = tempfile::tempdir().unwrap();
                    let path = if file_backed {
                        dir.path().join("bd_w78tx.db").to_str().unwrap().to_owned()
                    } else {
                        ":memory:".to_owned()
                    };
                    let conn = Connection::open(&path).await.unwrap();
                    for sql in BASE.iter().chain(scenario.setup) {
                        conn.execute(sql).await.unwrap_or_else(|error| {
                            panic!("{}: fsqlite setup `{sql}`: {error}", scenario.name)
                        });
                    }
                    for ((sql, _), expected) in scenario.probes.iter().zip(expected) {
                        let got = fsqlite_outcome(&conn, sql, prepared).await;
                        assert_eq!(
                            &got, expected,
                            "{} (file_backed={file_backed} prepared={prepared}): `{sql}`",
                            scenario.name
                        );
                    }
                }
            }
        }
    });
}
