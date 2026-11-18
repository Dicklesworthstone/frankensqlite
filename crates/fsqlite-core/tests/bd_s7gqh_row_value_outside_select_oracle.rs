#![recursion_limit = "512"]
//! bd-s7gqh: row-value comparisons outside a plain SELECT.
//!
//! Codegen read a row value as NULL, so `UPDATE ... WHERE (a, n) = ('abc', 1)`
//! changed no row, `DELETE ... WHERE (a, n) IS (NULL, 1)` deleted every row
//! (`NULL IS NULL`), and INSERT VALUES, UPDATE SET, RETURNING, upsert, trigger
//! bodies, CHECK constraints and generated columns all got NULL. Misuse
//! (`UPDATE t SET n = (1, 2)`, a comparison of a pair with a triple) ran
//! silently instead of failing with stock's `row value misused`.
//!
//! Every statement here runs on bundled SQLite 3.54.1 and on fsqlite, on
//! `:memory:` and on a file, executed directly and prepared, with the
//! interpreter fallbacks refused (strict), except in the one scenario whose
//! shapes only those fallbacks answer. Each statement's outcome (its
//! `changes()` and RETURNING rows, or its error text) and the final contents
//! of every table must equal stock's.

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

fn stock_param(value: &SqliteValue) -> rusqlite::types::Value {
    match value {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

fn stock_error(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message))
        | rusqlite::Error::SqlInputError { msg: message, .. } => message.clone(),
        other => other.to_string(),
    }
}

/// One statement and the values it binds.
struct Step {
    sql: &'static str,
    params: Vec<SqliteValue>,
}

fn steps(sqls: &[&'static str]) -> Vec<Step> {
    sqls.iter()
        .map(|sql| Step {
            sql,
            params: Vec::new(),
        })
        .collect()
}

fn returns_rows(sql: &str) -> bool {
    sql.contains("RETURNING") || sql.trim_start().starts_with("SELECT")
}

/// What one statement did: its RETURNING (or SELECT) rows, sorted, and
/// `changes()`, or its error text.
fn stock_outcome(conn: &rusqlite::Connection, step: &Step) -> String {
    let params: Vec<rusqlite::types::Value> = step.params.iter().map(stock_param).collect();
    let result = (|| -> rusqlite::Result<String> {
        let mut statement = conn.prepare(step.sql)?;
        let columns = statement.column_count();
        let mut rows: Vec<Vec<String>> = Vec::new();
        if returns_rows(step.sql) {
            let mut cursor = statement.query(rusqlite::params_from_iter(params.iter()))?;
            while let Some(row) = cursor.next()? {
                rows.push(
                    (0..columns)
                        .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                        .collect(),
                );
            }
        } else {
            statement.execute(rusqlite::params_from_iter(params.iter()))?;
        }
        drop(statement);
        rows.sort();
        let changes: i64 = conn.query_row("SELECT changes()", [], |row| row.get(0))?;
        Ok(format!("changes={changes} rows={rows:?}"))
    })();
    result.unwrap_or_else(|error| format!("error: {}", stock_error(&error)))
}

async fn fsqlite_outcome(conn: &Connection, step: &Step, prepared: bool) -> String {
    let rows_wanted = returns_rows(step.sql);
    let result: Result<Vec<Vec<String>>, String> = async {
        let rows = if prepared {
            let statement = conn.prepare(step.sql).await.map_err(|e| e.to_string())?;
            if rows_wanted {
                statement
                    .query_with_params(&step.params)
                    .await
                    .map_err(|e| e.to_string())?
            } else {
                statement
                    .execute_with_params(&step.params)
                    .await
                    .map_err(|e| e.to_string())?;
                Vec::new()
            }
        } else if rows_wanted {
            conn.query_with_params(step.sql, &step.params)
                .await
                .map_err(|e| e.to_string())?
        } else {
            conn.execute_with_params(step.sql, &step.params)
                .await
                .map_err(|e| e.to_string())?;
            Vec::new()
        };
        Ok(rows
            .iter()
            .map(|row| row.values().iter().map(render_fsqlite).collect())
            .collect())
    }
    .await;
    match result {
        Ok(mut rows) => {
            rows.sort();
            let changes = conn
                .query("SELECT changes()")
                .await
                .expect("changes()")
                .first()
                .map(|row| render_fsqlite(&row.values()[0]))
                .unwrap_or_default();
            format!("changes={changes} rows={rows:?}")
        }
        Err(error) => format!("error: {error}"),
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).expect("stock verify prepares");
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock verify runs")
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite verify `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect()
}

/// Stock's outcome of every step and the final contents of every table.
struct StockRun {
    outcomes: Vec<String>,
    tables: Vec<Vec<Vec<String>>>,
}

fn run_stock(setup: &[&str], steps: &[Step], verify: &[&str]) -> StockRun {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        conn.execute_batch(sql)
            .unwrap_or_else(|e| panic!("stock setup `{sql}`: {e}"));
    }
    let outcomes = steps
        .iter()
        .map(|step| stock_outcome(&conn, step))
        .collect();
    let tables = verify.iter().map(|sql| stock_rows(&conn, sql)).collect();
    StockRun { outcomes, tables }
}

/// Run the scenario on fsqlite in every mode and compare it with stock. With
/// `strict`, the steps run with the interpreter fallbacks refused, so each one
/// is answered by compiled code.
async fn assert_matches_stock(
    name: &str,
    setup: &[&str],
    steps: &[Step],
    verify: &[&str],
    strict: bool,
) -> StockRun {
    let stock = run_stock(setup, steps, verify);
    for file_backed in [false, true] {
        for prepared in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path().join("rowvalue.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let mode =
                format!("{name}: file_backed={file_backed} prepared={prepared} strict={strict}");
            let conn = Connection::open(&path).await.unwrap();
            for sql in setup {
                conn.execute(sql)
                    .await
                    .unwrap_or_else(|e| panic!("{mode}: setup `{sql}`: {e}"));
            }
            conn.set_reject_mem_fallback(strict);
            conn.set_strict_mem_fallback_rejection(strict);
            for (step, expected) in steps.iter().zip(&stock.outcomes) {
                let got = fsqlite_outcome(&conn, step, prepared).await;
                assert_eq!(&got, expected, "{mode}: `{}` {:?}", step.sql, step.params);
            }
            conn.set_strict_mem_fallback_rejection(false);
            conn.set_reject_mem_fallback(false);
            for (sql, expected) in verify.iter().zip(&stock.tables) {
                assert_eq!(&fsqlite_rows(&conn, sql).await, expected, "{mode}: `{sql}`");
            }
        }
    }
    stock
}

const TABLE: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, n INTEGER, hit TEXT DEFAULT '')",
    "INSERT INTO t(id, a, n) VALUES (1, 'abc', 1), (2, 'abc', 2), (3, 'abc', NULL), \
       (4, 'xyz', 1), (5, NULL, 1), (6, 'abd', 0), (7, '1', 7)",
    "CREATE TABLE u(k INTEGER PRIMARY KEY, x TEXT, y INTEGER)",
    "INSERT INTO u VALUES (1, 'abc', 2), (2, 'xyz', 1), (3, 'q', 9)",
];

const VERIFY: &[&str] = &["SELECT * FROM t ORDER BY id", "SELECT * FROM u ORDER BY k"];

/// Each operator marks the rows it matches; DELETE, INSERT VALUES, UPDATE SET
/// and RETURNING follow. INSERT ... SELECT is in
/// `row_value_interpreter_paths_match_stock`.
#[test]
fn row_value_dml_matches_stock() {
    const DML: &[&str] = &[
        "UPDATE t SET hit = hit || 'eq ' WHERE (a, n) = ('abc', 1)",
        "UPDATE t SET hit = hit || 'ne ' WHERE (a, n) <> ('abc', 1)",
        "UPDATE t SET hit = hit || 'lt ' WHERE (a, n) < ('abd', 0)",
        "UPDATE t SET hit = hit || 'le ' WHERE (a, n) <= ('abc', 1)",
        "UPDATE t SET hit = hit || 'gt ' WHERE (a, n) > ('abc', 1)",
        "UPDATE t SET hit = hit || 'ge ' WHERE (a, n) >= ('abd', 0)",
        "UPDATE t SET hit = hit || 'is ' WHERE (a, n) IS ('abc', NULL)",
        "UPDATE t SET hit = hit || 'isnot ' WHERE (a, n) IS NOT (NULL, 1)",
        "UPDATE t SET hit = hit || 'in ' WHERE (a, n) IN (('abc', 2), ('xyz', 1))",
        "UPDATE t SET hit = hit || 'notin ' WHERE (a, n) NOT IN (('abc', 2), ('zzz', 1))",
        "UPDATE t SET hit = hit || 'insub ' WHERE (a, n) IN (SELECT x, y FROM u)",
        "UPDATE t SET hit = hit || 'btw ' WHERE (a, n) BETWEEN ('abc', 2) AND ('abd', 0)",
        "UPDATE t SET hit = hit || 'nbtw ' WHERE (a, n) NOT BETWEEN ('abc', 2) AND ('abd', 0)",
        "UPDATE t SET hit = hit || 'subeq ' WHERE (a, n) = (SELECT x, y FROM u WHERE k = 2)",
        "UPDATE t SET hit = hit || 'subgt ' WHERE (a, n) > (SELECT x, y FROM u ORDER BY k LIMIT 1)",
        "UPDATE t SET hit = hit || 'corr ' WHERE EXISTS (SELECT 1 FROM u WHERE (u.x, u.y) = (t.a, t.n))",
        // Mixed affinity: n is INTEGER, so '1' converts; a is TEXT.
        "UPDATE t SET hit = hit || 'aff ' WHERE (n, a) = ('1', 'abc')",
        "UPDATE t SET hit = hit || 'aff2 ' WHERE (a, n) = (1, '7')",
        // The scalar spelling of the first statement, unchanged.
        "UPDATE t SET hit = hit || 'scalar ' WHERE a = 'abc' AND n = 1",
        "UPDATE t SET hit = hit || 'set ', n = n WHERE ((a, n) < ('abc', 2)) = 1",
        "UPDATE t SET (a, n) = ('p', 50) WHERE (a, n) = ('xyz', 1)",
        "UPDATE t SET hit = (a, n) = ('abc', 2) WHERE id IN (1, 2)",
        "UPDATE t SET n = n + 10 WHERE (a, n) >= ('abd', 0) AND (a, n) < ('p', 0) \
           RETURNING id, (a, n) = ('abd', 10), (a, n) < ('abd', 11)",
        "INSERT INTO t(id, a, n, hit) VALUES (8, 'v', 1, (1, 2) = (1, 2)), \
           (9, 'w', 2, (1, NULL) < (1, 3)), (10, 'x', 3, (2, 'a') > (1, 'b'))",
        "INSERT INTO t(id, a, n) VALUES (20, 'r', NULL) RETURNING id, (a, n) IS ('r', NULL)",
        // `(a, n) IS (NULL, 1)` matches row 5 only; read as NULL IS NULL it
        // matches every row.
        "DELETE FROM t WHERE (a, n) IS (NULL, 1)",
        "DELETE FROM t WHERE (a, n) > ('w', 0) AND (a, n) < ('x', 9) RETURNING id, a, n",
        "UPDATE u SET y = y + 1 WHERE (x, y) <> ('q', 9)",
        "DELETE FROM u WHERE (x, y) = (SELECT a, n FROM t WHERE id = 2)",
    ];
    let dml_steps = steps(DML);
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock("dml", TABLE, &dml_steps, VERIFY, true).await;
        // Not vacuous: the IS delete removes exactly one row in stock, and
        // the lexicographic `<` matches rows a member-wise AND would not.
        let is_delete = DML.iter().position(|s| s.contains("IS (NULL, 1)")).unwrap();
        assert_eq!(stock.outcomes[is_delete], "changes=1 rows=[]");
        let member_wise = run_stock(
            TABLE,
            &steps(&["UPDATE t SET hit = 'm' WHERE a < 'abd' AND n < 0"]),
            &[],
        );
        assert_eq!(member_wise.outcomes[0], "changes=0 rows=[]");
        assert_ne!(
            stock.outcomes[2], member_wise.outcomes[0],
            "lt: {:?}",
            stock.outcomes
        );
    });
}

/// Upsert DO UPDATE SET and its WHERE.
#[test]
fn row_value_upsert_matches_stock() {
    const SETUP: &[&str] = &[
        "CREATE TABLE p(k INTEGER PRIMARY KEY, a TEXT, b INTEGER)",
        "INSERT INTO p VALUES (1, 'p', 2), (2, 'q', NULL)",
    ];
    const UPSERT: &[&str] = &[
        "INSERT INTO p VALUES (1, 'p', 3) ON CONFLICT(k) DO UPDATE \
           SET b = ((excluded.a, excluded.b) = ('p', 3)) + 10 WHERE (p.a, p.b) = ('p', 2)",
        "INSERT INTO p VALUES (1, 'p', 4) ON CONFLICT(k) DO UPDATE \
           SET b = 99 WHERE (p.a, p.b) = ('p', 2)",
        "INSERT INTO p VALUES (2, 'z', 5) ON CONFLICT(k) DO UPDATE \
           SET (a, b) = (excluded.a, excluded.b) WHERE (p.a, p.b) IS ('q', NULL)",
        "INSERT INTO p VALUES (2, 'y', 6) ON CONFLICT(k) DO UPDATE \
           SET b = (excluded.a, excluded.b) < (p.a, p.b) WHERE (excluded.a, excluded.b) <> (p.a, p.b) \
           RETURNING k, a, b, (a, b) = ('z', 1)",
    ];
    let steps = steps(UPSERT);
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock(
            "upsert",
            SETUP,
            &steps,
            &["SELECT * FROM p ORDER BY k"],
            true,
        )
        .await;
        assert_eq!(stock.tables[0][0], ["1", "'p'", "11"], "{:?}", stock.tables);
    });
}

/// Row values in trigger bodies (every DML shape) and in WHEN clauses. A
/// statement that fires a trigger is replayed row by row by the connection
/// (`update_row_by_row_trigger_or_fk_fallback` and its INSERT / DELETE twins),
/// which strict mode refuses whatever the expressions are, so this scenario
/// runs with it off.
#[test]
fn row_value_triggers_match_stock() {
    const SETUP: &[&str] = &[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, n INTEGER)",
        "CREATE TABLE lg(tag TEXT, v)",
        "CREATE TABLE q(k INTEGER PRIMARY KEY, a TEXT, b)",
        "INSERT INTO lg VALUES ('mark', 1), ('gone', NULL), ('keep', NULL)",
        "CREATE TRIGGER t_body AFTER INSERT ON t BEGIN \
           INSERT INTO lg VALUES ('body-eq', (NEW.a, NEW.n) = ('v', 1)); \
           INSERT INTO lg VALUES ('body-lt', (NEW.a, NEW.n) < ('v', 2)); \
           UPDATE lg SET v = v + 100 WHERE (tag, v) = ('mark', 1) AND (NEW.a, NEW.n) = ('v', 1); \
           DELETE FROM lg WHERE (tag, v) IS ('gone', NULL) AND (NEW.a, NEW.n) IS ('w', NULL); \
           INSERT INTO q VALUES (1, NEW.a, NEW.n) ON CONFLICT(k) DO UPDATE \
             SET b = (excluded.a, excluded.b) > (q.a, q.b) WHERE (q.a, q.b) <> (excluded.a, excluded.b); \
         END",
        "CREATE TRIGGER t_when_is AFTER INSERT ON t WHEN (NEW.a, NEW.n) IS ('w', NULL) BEGIN \
           INSERT INTO lg VALUES ('when-is', NEW.id); END",
        "CREATE TRIGGER t_when_btw AFTER INSERT ON t \
           WHEN (NEW.a, NEW.n) BETWEEN ('v', 0) AND ('v', 9) BEGIN \
           INSERT INTO lg VALUES ('when-between', NEW.id); END",
        "CREATE TRIGGER t_when_lt AFTER INSERT ON t WHEN (NEW.a, NEW.n) < ('w', 0) BEGIN \
           INSERT INTO lg VALUES ('when-lt', NEW.id); END",
        "CREATE TRIGGER t_when_in AFTER INSERT ON t WHEN (NEW.a, NEW.n) IN (('x', 3), ('v', 1)) BEGIN \
           INSERT INTO lg VALUES ('when-in', NEW.id); END",
        "CREATE TRIGGER t_upd AFTER UPDATE ON t WHEN (OLD.a, OLD.n) <> (NEW.a, NEW.n) BEGIN \
           INSERT INTO lg VALUES ('upd', (OLD.a, OLD.n) < (NEW.a, NEW.n)); \
           UPDATE lg SET v = 'old-new' WHERE (tag, OLD.n) = ('keep', 1); END",
        "CREATE TRIGGER t_del BEFORE DELETE ON t WHEN (OLD.a, OLD.n) >= ('w', 0) BEGIN \
           INSERT INTO lg VALUES ('del', OLD.id); END",
    ];
    const DML: &[&str] = &[
        "INSERT INTO t VALUES (1, 'v', 1)",
        "INSERT INTO t VALUES (2, 'w', NULL)",
        "INSERT INTO t VALUES (3, 'x', 3)",
        "INSERT INTO t VALUES (4, 'a', 0)",
        "UPDATE t SET n = n + 1 WHERE (a, n) <= ('w', 0)",
        "UPDATE t SET a = a WHERE (a, n) = ('x', 3)",
        "DELETE FROM t WHERE (a, n) > ('v', 9)",
    ];
    let steps = steps(DML);
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock(
            "triggers",
            SETUP,
            &steps,
            &[
                "SELECT * FROM t ORDER BY id",
                "SELECT tag, v FROM lg ORDER BY rowid",
                "SELECT * FROM q ORDER BY k",
            ],
            false,
        )
        .await;
        let log: Vec<&str> = stock.tables[1].iter().map(|row| row[0].as_str()).collect();
        for tag in [
            "'when-is'",
            "'when-between'",
            "'when-lt'",
            "'when-in'",
            "'upd'",
            "'del'",
        ] {
            assert!(log.contains(&tag), "stock log lacks {tag}: {log:?}");
        }
        assert!(
            !log.contains(&"'gone'"),
            "stock deletes the gone row: {log:?}"
        );
    });
}

/// Misuse keeps stock's error, and the statement changes nothing.
#[test]
fn row_value_misuse_matches_stock() {
    const MISUSE: &[&str] = &[
        "UPDATE t SET hit = 'x' WHERE (a, n) = ('abc', 1, 2)",
        "DELETE FROM t WHERE (a, n) = 1",
        "UPDATE t SET n = (1, 2)",
        "INSERT INTO t(a, n) VALUES ((1, 2), 3)",
        "DELETE FROM t WHERE (a, n) = (SELECT x FROM u)",
        "DELETE FROM t WHERE n = (SELECT x, y FROM u)",
        "UPDATE t SET hit = 'x' WHERE (a, n) BETWEEN ('a', 0) AND 5",
        "UPDATE t SET hit = ((a, n), 1) = ((a, n), 1)",
        "UPDATE t SET hit = 'x' WHERE (a, n) IS NULL",
        "UPDATE t SET hit = 'x' WHERE 1 IN (1, (a, n))",
        "UPDATE t SET hit = 'x' WHERE (nope, n) = (1, 2, 3)",
        "UPDATE t SET hit = 'x' WHERE id = 1 RETURNING (a, n)",
        "INSERT INTO t(a, n) SELECT x, y FROM u WHERE (x, y) < (1, 2, 3)",
        "SELECT a FROM t WHERE (a, n) = ('abc', 2, 3)",
        "SELECT a FROM t ORDER BY (a, n)",
        // The well-formed spellings next to them still run.
        "UPDATE t SET hit = 'ok' WHERE (a, n) = ('abc', 1)",
        "DELETE FROM t WHERE (a, n) = (SELECT x, y FROM u WHERE k = 3)",
    ];
    let steps = steps(MISUSE);
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock("misuse", TABLE, &steps, VERIFY, true).await;
        for (sql, outcome) in MISUSE.iter().zip(&stock.outcomes).take(MISUSE.len() - 2) {
            assert!(
                outcome.starts_with("error: "),
                "stock accepts `{sql}`: {outcome}"
            );
        }
    });
}

/// Bound parameters as row-value members, executed directly and prepared.
#[test]
fn row_value_bound_parameters_match_stock() {
    let text = |s: &str| SqliteValue::Text(s.into());
    let steps = vec![
        Step {
            sql: "UPDATE t SET hit = hit || 'p1 ' WHERE (a, n) = (?1, ?2)",
            params: vec![text("abc"), SqliteValue::Integer(2)],
        },
        Step {
            sql: "UPDATE t SET hit = hit || 'p2 ' WHERE (a, n) > (?, ?)",
            params: vec![text("abc"), SqliteValue::Integer(1)],
        },
        Step {
            sql: "UPDATE t SET hit = hit || 'p3 ' WHERE (a, n) IS (?1, ?2)",
            params: vec![text("abc"), SqliteValue::Null],
        },
        Step {
            sql: "SELECT id FROM t WHERE (a, id) > (?1, ?2) ORDER BY a, id LIMIT 3",
            params: vec![text("abc"), SqliteValue::Integer(1)],
        },
        Step {
            sql: "DELETE FROM t WHERE (a, n) IS (?1, ?2) RETURNING id",
            params: vec![SqliteValue::Null, SqliteValue::Integer(1)],
        },
        Step {
            sql: "INSERT INTO t(id, a, n, hit) VALUES (30, ?1, ?2, (?1, ?2) = ('k', 3))",
            params: vec![text("k"), SqliteValue::Integer(3)],
        },
    ];
    asupersync::test_utils::run_test(|| async {
        assert_matches_stock("params", TABLE, &steps, VERIFY, true).await;
    });
}

/// Shapes the connection answers itself rather than compiled code: INSERT ...
/// SELECT, from another table, from its own target and with no FROM in a
/// trigger body (`insert_select_row_by_row_fallback`), and a grouped HAVING
/// (`group_by_fallback`). Strict mode refuses those paths whatever the
/// expressions are, so this scenario runs with it off.
#[test]
fn row_value_interpreter_paths_match_stock() {
    const SETUP: &[&str] = &[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, n INTEGER, hit TEXT DEFAULT '')",
        "INSERT INTO t(id, a, n) VALUES (1, 'abc', 1), (2, 'abc', 2), (3, 'abc', NULL), \
           (4, 'xyz', 1), (5, NULL, 1), (6, 'abd', 0)",
        "CREATE TABLE u(k INTEGER PRIMARY KEY, x TEXT, y INTEGER)",
        "INSERT INTO u VALUES (1, 'abc', 2), (2, 'xyz', 1), (3, 'q', 9)",
        "CREATE TABLE lg(tag TEXT, v)",
        "CREATE TABLE src(a TEXT, n INTEGER)",
        "CREATE TRIGGER src_body AFTER INSERT ON src BEGIN \
           INSERT INTO lg SELECT 'body-sel', (NEW.a, NEW.n) <> ('v', 1); \
           INSERT INTO lg SELECT 'body-sel-lt', (NEW.a, NEW.n) < ('v', 2) \
             WHERE (NEW.a, NEW.n) IS NOT (NULL, NULL); \
         END",
    ];
    const STEPS: &[&str] = &[
        "INSERT INTO t(a, n, hit) SELECT x || '+', y, (x, y) >= ('abc', 2) FROM u \
           WHERE (x, y) < ('xyz', 5) ORDER BY k",
        "INSERT INTO t(a, n, hit) SELECT a || '+', n, (a, n) >= ('abc', 2) FROM t \
           WHERE (a, n) < ('abd', 0) ORDER BY id",
        "INSERT INTO src VALUES ('v', 1)",
        "INSERT INTO src VALUES ('u', 9)",
        "SELECT a, count(*) FROM t GROUP BY a HAVING (a, count(*)) = ('abc', 3)",
        "SELECT a, count(*) FROM t GROUP BY a HAVING (a, max(n)) > ('abc', 1) ORDER BY a",
    ];
    let steps = steps(STEPS);
    asupersync::test_utils::run_test(|| async {
        assert_matches_stock(
            "interpreter",
            SETUP,
            &steps,
            &[
                "SELECT * FROM t ORDER BY id",
                "SELECT tag, v FROM lg ORDER BY rowid",
            ],
            false,
        )
        .await;
    });
}

/// CHECK constraints and generated columns, STORED and VIRTUAL.
#[test]
fn row_value_schema_expressions_match_stock() {
    const SETUP: &[&str] = &[
        "CREATE TABLE c(x, y, CHECK ((x, y) <> (0, 0)))",
        "CREATE TABLE g(x, y, s AS ((x, y) < (1, 5)) STORED, w AS ((x, y) = (1, 2)) VIRTUAL)",
    ];
    const DML: &[&str] = &[
        "INSERT INTO c VALUES (1, 0)",
        "INSERT INTO c VALUES (0, 0)",
        "INSERT INTO c VALUES (0, NULL)",
        "UPDATE c SET x = 0 WHERE x = 1",
        "INSERT INTO g(x, y) VALUES (1, 2), (2, 0), (1, NULL)",
        "UPDATE g SET y = 9 WHERE x = 2",
        "SELECT x, y, s, w FROM g WHERE w = 1",
    ];
    let steps = steps(DML);
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock(
            "schema",
            SETUP,
            &steps,
            &[
                "SELECT * FROM c ORDER BY rowid",
                "SELECT x, y, s, w FROM g ORDER BY rowid",
            ],
            true,
        )
        .await;
        assert!(
            stock.outcomes[1].starts_with("error: CHECK constraint failed"),
            "{:?}",
            stock.outcomes
        );
    });
}
