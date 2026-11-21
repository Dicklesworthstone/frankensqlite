#![recursion_limit = "512"]
//! bd-65jog: the row-value residuals left after bd-s7gqh.
//!
//! - A simple CASE with a row-value operand read the row value as NULL, so
//!   `CASE (a, n) WHEN ('abc', 1) THEN ...` matched no row (a wrong answer), and
//!   with literal operands it was "not implemented".
//! - `SET (a, n) = (1, 2, 3)` and its relatives reported "not implemented: ..."
//!   or "sub-select returns 2 columns - expected 1"; stock says
//!   "2 columns assigned 3 values", and for a row-value or scalar source it says
//!   so in its parser: before name resolution, and at CREATE TRIGGER.
//! - A nested row value in a top-level WHERE / ON `=` or `IS`, which stock's
//!   WHERE analysis splits into one term per element, was "row value misused".
//! - A partial index whose WHERE has a row value was refused at CREATE INDEX.
//! - A row-value DEFAULT was refused as "not constant".
//! - A compound's column-count error said "UNION" for UNION ALL, and named the
//!   operator where stock reports a one-row VALUES arm.
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared (DDL always directly: fsqlite's
//! `prepare` takes DML only). Each statement's outcome (its rows, its
//! `changes()` for DML, or its error text) and the final contents of the
//! listed queries must
//! equal stock's. File-backed runs also reopen the database and re-run the
//! listed queries, so stored schema text is read back through a fresh schema.

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

fn stock_error(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message))
        | rusqlite::Error::SqlInputError { msg: message, .. } => message.clone(),
        other => other.to_string(),
    }
}

fn is_ddl(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    head.starts_with("CREATE") || head.starts_with("ALTER") || head.starts_with("DROP")
}

/// `changes()` is a statement's own outcome only after INSERT, UPDATE or
/// DELETE. After a SELECT or DDL it still reports an earlier statement, and
/// fsqlite resets it on a statement that fails to prepare where stock keeps
/// it (a separate divergence, outside this bead), so it is recorded for DML
/// only.
fn is_dml(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    head.starts_with("INSERT") || head.starts_with("UPDATE") || head.starts_with("DELETE")
}

fn returns_rows(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    sql.contains("RETURNING")
        || head.starts_with("SELECT")
        || head.starts_with("WITH")
        || head.starts_with("VALUES")
        || head.starts_with("PRAGMA")
}

/// What one statement did: its rows, in order, and `changes()`, or its error
/// text.
fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let result = (|| -> rusqlite::Result<String> {
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows: Vec<Vec<String>> = Vec::new();
        if returns_rows(sql) {
            let mut cursor = statement.query([])?;
            while let Some(row) = cursor.next()? {
                rows.push(
                    (0..columns)
                        .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                        .collect(),
                );
            }
        } else {
            statement.execute([])?;
        }
        drop(statement);
        if !is_dml(sql) {
            return Ok(format!("rows={rows:?}"));
        }
        let changes: i64 = conn.query_row("SELECT changes()", [], |row| row.get(0))?;
        Ok(format!("changes={changes} rows={rows:?}"))
    })();
    result.unwrap_or_else(|error| format!("error: {}", stock_error(&error)))
}

async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let rows_wanted = returns_rows(sql);
    let result: Result<Vec<Vec<String>>, String> = async {
        let rows = if prepared && !is_ddl(sql) {
            let statement = conn.prepare(sql).await.map_err(|e| e.to_string())?;
            if rows_wanted {
                statement.query().await.map_err(|e| e.to_string())?
            } else {
                statement.execute().await.map_err(|e| e.to_string())?;
                Vec::new()
            }
        } else if rows_wanted {
            conn.query(sql).await.map_err(|e| e.to_string())?
        } else {
            conn.execute(sql).await.map_err(|e| e.to_string())?;
            Vec::new()
        };
        Ok(rows
            .iter()
            .map(|row| row.values().iter().map(render_fsqlite).collect())
            .collect())
    }
    .await;
    match result {
        Ok(rows) if !is_dml(sql) => format!("rows={rows:?}"),
        Ok(rows) => {
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
    let mut statement = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("stock verify `{sql}` prepares: {e}"));
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .unwrap_or_else(|e| panic!("stock verify `{sql}` runs: {e}"))
}

async fn fsqlite_rows(conn: &Connection, sql: &str, mode: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("{mode}: fsqlite verify `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect()
}

/// Stock's outcome of every step and the final result of every verify query.
struct StockRun {
    outcomes: Vec<String>,
    tables: Vec<Vec<Vec<String>>>,
}

impl StockRun {
    /// Stock's outcome of the step whose text contains `needle`.
    fn outcome_of(&self, steps: &[&str], needle: &str) -> &str {
        let index = steps
            .iter()
            .position(|sql| sql.contains(needle))
            .unwrap_or_else(|| panic!("no step contains `{needle}`"));
        &self.outcomes[index]
    }
}

fn run_stock(setup: &[&str], steps: &[&str], verify: &[&str]) -> StockRun {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        conn.execute_batch(sql)
            .unwrap_or_else(|e| panic!("stock setup `{sql}`: {e}"));
    }
    let outcomes = steps.iter().map(|sql| stock_outcome(&conn, sql)).collect();
    let tables = verify.iter().map(|sql| stock_rows(&conn, sql)).collect();
    StockRun { outcomes, tables }
}

/// Run the scenario on fsqlite in every mode and compare it with stock.
async fn assert_matches_stock(
    name: &str,
    setup: &[&str],
    steps: &[&str],
    verify: &[&str],
) -> StockRun {
    let stock = run_stock(setup, steps, verify);
    for file_backed in [false, true] {
        for prepared in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path().join("bd_65jog.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let mode = format!("{name}: file_backed={file_backed} prepared={prepared}");
            let conn = Connection::open(&path).await.unwrap();
            for sql in setup {
                conn.execute(sql)
                    .await
                    .unwrap_or_else(|e| panic!("{mode}: setup `{sql}`: {e}"));
            }
            for (sql, expected) in steps.iter().zip(&stock.outcomes) {
                let got = fsqlite_outcome(&conn, sql, prepared).await;
                assert_eq!(&got, expected, "{mode}: `{sql}`");
            }
            for (sql, expected) in verify.iter().zip(&stock.tables) {
                assert_eq!(&fsqlite_rows(&conn, sql, &mode).await, expected, "{mode}: `{sql}`");
            }
            if file_backed {
                conn.close().await.unwrap();
                let reopened = Connection::open(&path).await.unwrap();
                let mode = format!("{mode} reopened");
                for (sql, expected) in verify.iter().zip(&stock.tables) {
                    assert_eq!(
                        &fsqlite_rows(&reopened, sql, &mode).await,
                        expected,
                        "{mode}: `{sql}`"
                    );
                }
                reopened.close().await.unwrap();
            }
        }
    }
    stock
}

const TABLE: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, n INTEGER, c TEXT DEFAULT '')",
    "INSERT INTO t(id, a, n) VALUES (1, 'abc', 1), (2, 'x', 2), (3, NULL, 3), (4, 'ABC', 1), \
       (5, 'x', NULL)",
    "CREATE TABLE s(k INTEGER PRIMARY KEY, x TEXT, y INTEGER)",
    "INSERT INTO s VALUES (1, 'x', 2), (2, 'abc', 1)",
];

/// A simple CASE whose operand is a row value compares it with each WHEN
/// element by element (stock's codeVectorCompare).
#[test]
fn case_with_row_value_operand_matches_stock() {
    const STEPS: &[&str] = &[
        "SELECT CASE (1, 2) WHEN (1, 2) THEN 1 END",
        "SELECT CASE (1, 2) WHEN (1, 3) THEN 1 WHEN (1, 2) THEN 2 ELSE 3 END",
        "SELECT CASE (1, NULL) WHEN (1, NULL) THEN 1 ELSE 0 END",
        "SELECT id, CASE (a, n) WHEN ('abc', 1) THEN 'hit' WHEN ('x', 2) THEN 'x2' \
           ELSE 'miss' END FROM t ORDER BY id",
        "SELECT id FROM t WHERE CASE (a, n) WHEN ('x', 2) THEN 1 ELSE 0 END ORDER BY id",
        "SELECT id, CASE (a COLLATE NOCASE, n) WHEN ('abc', 1) THEN 'nc' ELSE 'no' END \
           FROM t ORDER BY id",
        "SELECT CASE (SELECT x, y FROM s WHERE k = 1) WHEN ('x', 2) THEN 'sub' END",
        "SELECT id, CASE (a, n) WHEN (SELECT x, y FROM s WHERE k = 2) THEN 'sub2' ELSE '-' END \
           FROM t ORDER BY id",
        "SELECT id, CASE (a, n) WHEN (SELECT x, y FROM s WHERE s.k = t.id) THEN 'corr' \
           ELSE '-' END FROM t ORDER BY id",
        "UPDATE t SET c = CASE (a, n) WHEN ('abc', 1) THEN 'u1' WHEN ('x', NULL) THEN 'u5' \
           ELSE 'u' END",
        "CREATE TABLE lg(x)",
        "CREATE TABLE out(v)",
        "CREATE TRIGGER lg_ai AFTER INSERT ON lg BEGIN \
           INSERT INTO out VALUES (CASE (NEW.x, 1) WHEN (5, 1) THEN 'five' ELSE 'other' END); END",
        "INSERT INTO lg VALUES (5), (6)",
        "CREATE VIEW v AS SELECT CASE (1, 2) WHEN (1, 2) THEN 'v' END AS k",
        "SELECT * FROM v",
        // Misuse stays misuse: sizes must agree, and a row value is no result.
        "SELECT CASE (1, 2) WHEN 1 THEN 1 END",
        "SELECT CASE 1 WHEN (1, 2) THEN 1 END",
        "SELECT CASE (1, 2) WHEN (1, 2, 3) THEN 1 END",
        "SELECT CASE (1, 2) WHEN (1, 2) THEN (1, 2) END",
        "SELECT CASE ((1, 2), 3) WHEN ((1, 2), 3) THEN 1 END",
        "SELECT CASE (a, n) WHEN ('abc', 1) THEN 1 ELSE (a, n) END FROM t",
    ];
    const VERIFY: &[&str] = &["SELECT * FROM t ORDER BY id", "SELECT * FROM out ORDER BY rowid"];
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock("case", TABLE, STEPS, VERIFY).await;
        // Not vacuous: a row value read as NULL matches no WHEN.
        assert!(
            stock
                .outcome_of(STEPS, "WHEN ('x', 2) THEN 'x2'")
                .contains("'hit'"),
            "{:?}",
            stock.outcomes
        );
        assert_eq!(
            stock.tables[1],
            vec![vec!["'five'".to_owned()], vec!["'other'".to_owned()]]
        );
        assert!(
            stock
                .outcome_of(STEPS, "WHEN 1 THEN 1 END")
                .contains("row value misused")
        );
    });
}

/// `SET (columns) = source`: stock's "N columns assigned M values", before
/// name resolution for a row-value or scalar source.
#[test]
fn set_column_list_arity_errors_match_stock() {
    const SETUP: &[&str] = &[
        "CREATE TABLE u(a UNIQUE, n, c)",
        "INSERT INTO u VALUES ('abc', 1, 2)",
        "CREATE TABLE w(x, y)",
        "INSERT INTO w VALUES (5, 6)",
    ];
    const STEPS: &[&str] = &[
        "UPDATE u SET (a, n) = (1, 2, 3)",
        "UPDATE u SET (a, n) = 1",
        "UPDATE u SET (a, n, c) = (1, 2)",
        "UPDATE u SET (n) = (1, 2)",
        // A row-value or scalar source is checked as it is parsed, so its
        // error wins over every name the statement resolves later.
        "UPDATE nosuch SET (a, n) = (1, 2, 3)",
        "UPDATE u SET (a, nope) = (1, 2, 3)",
        "UPDATE u SET c = nope, (a, n) = (1, 2, 3)",
        "UPDATE u SET (a, n) = (1, 2, 3) WHERE nope = 1",
        "UPDATE u SET (a, n) = (1, 2, 3) RETURNING nope",
        "INSERT INTO u VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, c) = (1, 2, 3)",
        "INSERT INTO u VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, c) = 5",
        "INSERT INTO u VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, nope) = (1, 2, 3)",
        "INSERT INTO u VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, c) = (1, 2, 3) \
           WHERE nope = 1",
        // A subquery source is checked after its names resolve.
        "UPDATE u SET (a, n) = (SELECT 1, 2, 3)",
        "UPDATE u SET (n) = (SELECT 1, 2)",
        "UPDATE u SET (a, n) = (SELECT 1)",
        "UPDATE u SET (n, c) = (SELECT x, y, x FROM w)",
        "UPDATE nosuch SET (a, n) = (SELECT 1, 2, 3)",
        "UPDATE u SET (a, nope) = (SELECT 1, 2, 3)",
        // A trigger body is parsed at CREATE TRIGGER.
        "CREATE TABLE lg(x)",
        "CREATE TRIGGER lg_bad AFTER INSERT ON lg BEGIN UPDATE u SET (a, n) = (1, 2, 3); END",
        "SELECT count(*) FROM sqlite_master WHERE type = 'trigger'",
        // What does work keeps working.
        "UPDATE u SET (n, c) = (SELECT x, y FROM w)",
        "UPDATE u SET (n) = (SELECT 7)",
        "UPDATE u SET (a, n) = ('q', n + 1)",
        "INSERT INTO u VALUES ('q', 9, 9) ON CONFLICT(a) DO UPDATE SET (n) = 70",
        "INSERT INTO u VALUES ('q', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, c) = (excluded.n, 'e')",
        // Negatives: a row value for one column, or inside the source.
        "UPDATE u SET n = (1, 2)",
        "UPDATE u SET (a, n) = ((1, 2), 3)",
    ];
    const VERIFY: &[&str] = &["SELECT * FROM u ORDER BY rowid"];
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock("set", SETUP, STEPS, VERIFY).await;
        assert_eq!(
            stock.outcome_of(STEPS, "UPDATE nosuch SET (a, n) = (1, 2, 3)"),
            "error: 2 columns assigned 3 values"
        );
        assert_eq!(
            stock.outcome_of(STEPS, "UPDATE nosuch SET (a, n) = (SELECT"),
            "error: no such table: nosuch"
        );
        assert!(
            stock
                .outcome_of(STEPS, "SELECT count(*) FROM sqlite_master")
                .ends_with("rows=[[\"0\"]]")
        );
        assert_eq!(stock.tables[0], vec![vec!["'q'", "9", "'e'"]]);
    });
}

/// Stock's WHERE analysis splits a top-level AND conjunct `X = Y` / `X IS Y`
/// of row values into one term per element, recursively, so an element may
/// itself be a row value there, and only there.
#[test]
fn nested_row_value_where_terms_match_stock() {
    const STEPS: &[&str] = &[
        "SELECT id FROM t WHERE ((a, n), 1) = ((a, n), 1) ORDER BY id",
        "SELECT id FROM t WHERE ((a, n), 1) IS ((a, n), 1) ORDER BY id",
        "SELECT id FROM t WHERE ((a, n), 1) = (('x', 2), 1)",
        "SELECT id FROM t WHERE id > 0 AND ((a, n), 1) = (('x', 2), 1) AND n IS NOT NULL",
        "SELECT id FROM t WHERE (((a, n), 1), id) = ((('x', 2), 1), 2)",
        "SELECT id FROM t WHERE (a, (n, 1)) = ('x', (2, 1))",
        "SELECT id FROM t WHERE ((SELECT x, y FROM s WHERE k = 1), 1) = (('x', 2), 1)",
        "SELECT x FROM (SELECT 1 AS x) WHERE ((x, 1), 2) = ((1, 1), 2)",
        "WITH c(x) AS (SELECT 1) SELECT x FROM c WHERE ((x, 1), 2) = ((1, 1), 2)",
        "SELECT 1 WHERE EXISTS (SELECT 1 FROM t WHERE ((a, n), 1) = (('x', 2), 1))",
        "SELECT (SELECT id FROM t WHERE ((a, n), 1) = (('x', 2), 1))",
        "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE ((a, n), 1) = (('abc', 1), 1))",
        "SELECT count(*) FROM t AS p JOIN t AS q ON ((p.a, p.n), 1) = ((q.a, q.n), 1)",
        "SELECT count(*) FROM t AS p LEFT JOIN t AS q ON ((p.a, p.n), 1) = ((q.a, q.n), 1)",
        "UPDATE t SET c = 'u' WHERE ((a, n), 1) = (('x', 2), 1)",
        "DELETE FROM t WHERE ((a, n), id) IS ((NULL, 3), 3)",
        // Outside a top-level `=` / `IS` conjunct of WHERE or ON, stock codes
        // the comparison as written, and a nested row value is misuse.
        "SELECT id FROM t WHERE ((a, n), 1) = (('x', 2), 1) OR id = 9",
        "SELECT id FROM t WHERE NOT ((a, n), 1) = (('x', 2), 1)",
        "SELECT id FROM t WHERE ((a, n), 1) <> (('x', 2), 1)",
        "SELECT id FROM t WHERE ((a, n), 1) IS NOT (('x', 2), 1)",
        "SELECT id FROM t WHERE ((a, n), 1) < (('x', 2), 1)",
        "SELECT ((1, 2), 1) = ((1, 2), 1)",
        "SELECT id FROM t WHERE (((a, n), 1) = (('x', 2), 1)) = 1",
        "SELECT id FROM t WHERE ((a, n), 1) = ((a), 1)",
        "SELECT CASE WHEN ((1, 1), 1) = ((1, 1), 1) THEN 1 END",
        "CREATE TABLE p(k INTEGER PRIMARY KEY, v)",
        "INSERT INTO p VALUES (1, 1)",
        "INSERT INTO p VALUES (1, 5) ON CONFLICT(k) DO UPDATE SET v = 9 \
           WHERE ((excluded.v, 1), 2) = ((5, 1), 2)",
        "SELECT id, ((a, n), 1) = (('x', 2), 1) FROM t",
    ];
    const VERIFY: &[&str] = &["SELECT * FROM t ORDER BY id", "SELECT * FROM p"];
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock("nested", TABLE, STEPS, VERIFY).await;
        // The third step, the first whose text is exactly this comparison.
        assert!(
            stock.outcomes[2].ends_with("rows=[[\"2\"]]"),
            "{:?}",
            stock.outcomes
        );
        assert!(
            stock
                .outcome_of(STEPS, "OR id = 9")
                .contains("row value misused")
        );
        assert!(stock.tables[0].len() == 4, "{:?}", stock.tables[0]);
    });
}

/// A partial index whose WHERE compares row values: created, maintained,
/// used by INDEXED BY, and read back after a reopen.
#[test]
fn partial_index_row_value_predicate_matches_stock() {
    const STEPS: &[&str] = &[
        "CREATE INDEX t_eq ON t(id) WHERE (a, n) = ('x', 2)",
        "CREATE INDEX t_lt ON t(n) WHERE (a, n) < ('b', 0)",
        "SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name",
        "INSERT INTO t(id, a, n) VALUES (6, 'x', 2), (7, 'ABD', 4), (8, 'x', 3)",
        "UPDATE t SET n = 2 WHERE id = 8",
        "UPDATE t SET a = 'y' WHERE id = 6",
        "DELETE FROM t WHERE id = 1",
        "SELECT id FROM t INDEXED BY t_eq WHERE (a, n) = ('x', 2) ORDER BY id",
        // bd-wpfqs: stock's implication test compares the row-value predicate
        // term whole, so the scalar spelling, a reordered or a commuted row
        // value refuses ("no query solution"), while the row value as written
        // answers, also with another term beside it and inside a derived
        // table or an IN subquery.
        "SELECT id FROM t INDEXED BY t_eq WHERE a = 'x' AND n = 2 ORDER BY id",
        "SELECT id FROM t INDEXED BY t_eq WHERE n = 2 AND a = 'x' ORDER BY id",
        "SELECT id FROM t INDEXED BY t_eq WHERE (n, a) = (2, 'x') ORDER BY id",
        "SELECT id FROM t INDEXED BY t_eq WHERE ('x', 2) = (a, n) ORDER BY id",
        "SELECT n FROM t INDEXED BY t_lt WHERE a < 'b' OR (a = 'b' AND n < 0) ORDER BY n",
        "SELECT id FROM t INDEXED BY t_eq WHERE (a, n) = ('x', 2) AND id > 0 ORDER BY id",
        "SELECT * FROM (SELECT id FROM t INDEXED BY t_eq WHERE (a, n) = ('x', 2)) ORDER BY 1",
        "SELECT id FROM t WHERE id IN (SELECT id FROM t INDEXED BY t_eq WHERE (a, n) = ('x', 2)) \
         ORDER BY id",
        "SELECT * FROM (SELECT id FROM t INDEXED BY t_eq WHERE a = 'x' AND n = 2)",
        // A scalar predicate term is implied by a row value's expansion.
        "CREATE INDEX t_sc ON t(n) WHERE a = 'x'",
        "SELECT id FROM t INDEXED BY t_sc WHERE (a, n) = ('x', 2) ORDER BY id",
        "DROP INDEX t_sc",
        "SELECT n FROM t INDEXED BY t_lt WHERE (a, n) < ('b', 0) ORDER BY n",
        "PRAGMA integrity_check",
        // Misuse, and a row-value IN list, which stock rewrites to a subquery.
        "CREATE INDEX t_bad1 ON t(id) WHERE (a, n) = ('x', 2, 3)",
        "CREATE INDEX t_bad2 ON t(id) WHERE ((a, n), 1) = (('x', 2), 1)",
        "CREATE INDEX t_bad3 ON t(id) WHERE (a, n) IN (('x', 2))",
        "CREATE INDEX t_bad4 ON t(id) WHERE (a, n)",
    ];
    const VERIFY: &[&str] = &[
        "SELECT id FROM t INDEXED BY t_eq WHERE (a, n) = ('x', 2) ORDER BY id",
        "SELECT n FROM t INDEXED BY t_lt WHERE (a, n) < ('b', 0) ORDER BY n",
        "SELECT * FROM t ORDER BY id",
        "PRAGMA integrity_check",
    ];
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock("partial", TABLE, STEPS, VERIFY).await;
        assert_eq!(stock.tables[0], vec![vec!["2"], vec!["8"]]);
        // Not vacuous (bd-wpfqs): stock refuses the scalar spelling and
        // answers the row value as written, here and in a derived table.
        assert!(
            stock
                .outcome_of(STEPS, "t_eq WHERE a = 'x' AND n = 2 ORDER BY id")
                .contains("no query solution")
        );
        assert!(
            stock
                .outcome_of(STEPS, "t_eq WHERE a = 'x' AND n = 2)")
                .contains("no query solution")
        );
        assert!(
            stock
                .outcome_of(STEPS, "t_eq WHERE (a, n) = ('x', 2) AND id > 0")
                .ends_with("rows=[[\"2\"], [\"8\"]]")
        );
        assert!(
            stock
                .outcome_of(STEPS, "t_eq WHERE (a, n) = ('x', 2)) ORDER BY 1")
                .ends_with("rows=[[\"2\"], [\"8\"]]")
        );
        assert!(
            !stock.outcomes[0].starts_with("error") && !stock.outcomes[1].starts_with("error"),
            "{:?}",
            stock.outcomes
        );
    });
}

/// A DEFAULT built from row values is constant, as stock's
/// sqlite3ExprIsConstantOrFunction reads it.
#[test]
fn row_value_default_matches_stock() {
    const STEPS: &[&str] = &[
        "CREATE TABLE d(k INTEGER PRIMARY KEY, v DEFAULT ((1, 2) < (1, 3)), \
           w DEFAULT ((1, 2) = (1, 2)), z DEFAULT ((1, 'a') IS (1, 'a')), \
           b DEFAULT ((1, 2) BETWEEN (0, 0) AND (5, 5)), \
           e DEFAULT (CASE (1, 2) WHEN (1, 2) THEN 'y' END))",
        "INSERT INTO d(k) VALUES (1)",
        "INSERT INTO d DEFAULT VALUES",
        // Accepted as constant at CREATE; misuse fails when the default is
        // used, as stock's.
        "CREATE TABLE d2(k, v DEFAULT ((1, 2) < (1, 3, 4)))",
        "INSERT INTO d2(k) VALUES (1)",
        "CREATE TABLE d3(k, v DEFAULT ((1, 2)))",
        "INSERT INTO d3(k) VALUES (1)",
        "INSERT INTO d3(k, v) VALUES (1, 2)",
        "CREATE TABLE d4(k, v DEFAULT (((1, 2), 3) = ((1, 2), 3)))",
        "INSERT INTO d4(k) VALUES (1)",
        // Not constant: a column, and a row-value IN list (a subquery to stock).
        "CREATE TABLE d5(k, v DEFAULT ((1, k) = (1, 2)))",
        "CREATE TABLE d6(k, v DEFAULT ((1, 2) IN ((1, 2))))",
        // ALTER ADD COLUMN: stock refuses an expression default on a table
        // with rows, and takes it on an empty one.
        "CREATE TABLE full(a)",
        "INSERT INTO full VALUES (1)",
        "ALTER TABLE full ADD COLUMN g DEFAULT ((1, 2) < (1, 3))",
        "CREATE TABLE empty(a)",
        "ALTER TABLE empty ADD COLUMN g DEFAULT ((1, 2) < (1, 3))",
        "INSERT INTO empty(a) VALUES (1)",
    ];
    const VERIFY: &[&str] = &[
        "SELECT * FROM d ORDER BY k",
        "SELECT * FROM d3",
        "SELECT * FROM full",
        "SELECT * FROM empty",
        "SELECT name FROM sqlite_master ORDER BY name",
    ];
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock("default", &[], STEPS, VERIFY).await;
        assert!(!stock.outcomes[0].starts_with("error"), "{:?}", stock.outcomes);
        assert_eq!(
            stock.outcome_of(STEPS, "INSERT INTO d2(k)"),
            "error: row value misused"
        );
        assert_eq!(
            stock.outcome_of(STEPS, "CREATE TABLE d6"),
            "error: default value of column [v] is not constant"
        );
        assert_eq!(stock.tables[3], vec![vec!["1", "1"]]);
    });
}

/// A compound's column-count error names the operator between the two arms
/// whose widths differ, or a one-row VALUES arm on its right.
#[test]
fn compound_arity_error_text_matches_stock() {
    const STEPS: &[&str] = &[
        "SELECT 1 UNION ALL SELECT 2, 3",
        "SELECT 1 UNION SELECT 2, 3",
        "SELECT 1 INTERSECT SELECT 2, 3",
        "SELECT 1 EXCEPT SELECT 2, 3",
        "SELECT 1 UNION ALL SELECT 2 UNION SELECT 3, 4",
        "SELECT 1, 2 UNION SELECT 3, 4 UNION ALL SELECT 5",
        "SELECT 1 INTERSECT SELECT 2 UNION ALL SELECT 3, 4",
        "SELECT 'union', 1 WHERE (1, 2) = (1, 2) UNION ALL SELECT 2 WHERE (1, 2) < (1, 1)",
        "SELECT * FROM (SELECT 1 UNION ALL SELECT 2, 3)",
        "SELECT 1 UNION ALL SELECT 2, 3 ORDER BY 1",
        "VALUES (1, 2) UNION ALL SELECT 1",
        "VALUES (1), (2) UNION ALL SELECT 1, 2",
        // A one-row VALUES on the right of the mismatch.
        "SELECT 1 UNION VALUES (1, 2)",
        "SELECT 1 EXCEPT VALUES (1, 2)",
        "SELECT 1 UNION ALL SELECT 2, 3 UNION ALL VALUES (1)",
        "SELECT (SELECT 1 UNION ALL VALUES (1, 2))",
        // A multi-row VALUES there is named by its operator.
        "SELECT 1 UNION ALL VALUES (1, 2), (3, 4)",
        "SELECT 1 UNION VALUES (1, 2), (3, 4)",
        "SELECT 1 UNION ALL VALUES (1), (2, 3)",
        // Equal widths are no error.
        "SELECT 1, 2 UNION ALL VALUES (3, 4) ORDER BY 1",
    ];
    asupersync::test_utils::run_test(|| async {
        let stock = assert_matches_stock("compound", &[], STEPS, &[]).await;
        assert_eq!(
            stock.outcomes[0],
            "error: SELECTs to the left and right of UNION ALL do not have the same number of \
             result columns"
        );
        assert_eq!(
            stock.outcome_of(STEPS, "SELECT 1 UNION VALUES (1, 2)"),
            "error: all VALUES must have the same number of terms"
        );
    });
}
