//! bd-nf30k: stock SQLite compiles every trigger a DML statement can fire
//! while it prepares the statement, whatever the trigger's WHEN clause, and
//! refuses the statement when one does not compile. bd-w78tx covered a
//! relation that does not exist; this covers the other names compiling
//! resolves: a column, `NEW.x` or `OLD.x`, a function or its arity, an
//! INSERT's column or value count, a misplaced aggregate. fsqlite raised
//! those only when the trigger fired, so with `WHEN 0` it ran statements
//! stock refuses.
//!
//! Compiling a trigger also compiles what it fires: the triggers its body
//! statements fire, and with `PRAGMA foreign_keys = 1` the ON DELETE / ON
//! UPDATE actions a DELETE or UPDATE takes on child tables, which fire the
//! child's triggers. There fsqlite changed data stock refuses to change:
//! `DELETE FROM par WHERE a = 1` deleted the parent row (and cascaded) while
//! a child's DELETE trigger named `OLD.nope`; stock deletes nothing.
//!
//! Each scenario runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, with the probe statements run directly and prepared. Every
//! probe's outcome (its rows, or its error text) must equal stock's, and is
//! pinned to stock's so the comparison cannot pass vacuously.

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
        Err(
            rusqlite::Error::SqliteFailure(_, Some(message))
            | rusqlite::Error::SqlInputError { msg: message, .. },
        ) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

/// `sql` on fsqlite, run directly (`Connection::query`) or prepared first
/// (a prepared SELECT is queried, a prepared DML statement executed, DML on
/// a view included since bd-x5eqm). DDL and PRAGMA probes always run
/// directly.
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let preparable = ["SELECT", "INSERT", "UPDATE", "DELETE"]
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

const NO_ROWS: &str = "rows=[]";
const INSERT: &str = "INSERT INTO t VALUES (1, 1)";
const DROP: &str = "DROP TRIGGER tr";

const NAMES_SETUP: &[&str] = &[
    "CREATE TABLE t(a, b)",
    "CREATE TABLE lg(m)",
    "CREATE TABLE o(z, w)",
    "CREATE TABLE p(z)",
];

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "a WHEN 0 trigger whose body does not compile refuses the INSERT",
        setup: NAMES_SETUP,
        probes: &[
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (NEW.zz); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: NEW.zz"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (OLD.a); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: OLD.a"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (new.Zz); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: new.Zz"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (abs(1, 2)); END",
                NO_ROWS,
            ),
            (INSERT, "error: wrong number of arguments to function abs()"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (lower(NEW.a, 1)); END",
                NO_ROWS,
            ),
            (
                INSERT,
                "error: wrong number of arguments to function lower()",
            ),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (nofunc(1)); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such function: nofunc"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN UPDATE lg SET m = (SELECT y FROM o); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: y"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 BEGIN UPDATE lg SET nope = 1; END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: nope"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN UPDATE o SET (z, nope) = (1, 2); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: nope"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN UPDATE lg AS l SET m = l.nope; END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: l.nope"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN DELETE FROM lg WHERE nope = 1; END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: nope"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN DELETE FROM lg WHERE m IN (SELECT nope FROM o); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: nope"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN DELETE FROM lg WHERE count(*) > 1; END",
                NO_ROWS,
            ),
            (INSERT, "error: misuse of aggregate function count()"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg(nope) VALUES (1); END",
                NO_ROWS,
            ),
            (INSERT, "error: table lg has no column named nope"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (1, 2); END",
                NO_ROWS,
            ),
            (
                INSERT,
                "error: table lg has 1 columns but 2 values were supplied",
            ),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 BEGIN INSERT INTO lg SELECT 1, 2; END",
                NO_ROWS,
            ),
            (
                INSERT,
                "error: table lg has 1 columns but 2 values were supplied",
            ),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg(m) VALUES (1, 2); END",
                NO_ROWS,
            ),
            (INSERT, "error: 2 values for 1 columns"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg SELECT count(*) FROM o GROUP BY count(*); END",
                NO_ROWS,
            ),
            (
                INSERT,
                "error: aggregate functions are not allowed in the GROUP BY clause",
            ),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg SELECT o.nope FROM o; END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: o.nope"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg SELECT z FROM o, p; END",
                NO_ROWS,
            ),
            (INSERT, "error: ambiguous column name: z"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 BEGIN SELECT nope FROM o; END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: nope"),
            (DROP, NO_ROWS),
            // A WHEN is compiled whatever it is, before the body.
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.zz = 1 \
                 BEGIN INSERT INTO lg VALUES (1); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: NEW.zz"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.yy \
                 BEGIN INSERT INTO lg VALUES (NEW.zz); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: NEW.yy"),
            (DROP, NO_ROWS),
            // Body statements in order, each whole: a column before a later
            // statement's missing relation, and the other way round.
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (NEW.zz); INSERT INTO missing VALUES (1); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: NEW.zz"),
            (DROP, NO_ROWS),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO missing VALUES (1); INSERT INTO lg VALUES (NEW.zz); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such table: main.missing"),
            (DROP, NO_ROWS),
            // Triggers in stock's order: BEFORE newest first, then AFTER.
            (
                "CREATE TRIGGER t1 BEFORE INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (NEW.b1); END",
                NO_ROWS,
            ),
            (
                "CREATE TRIGGER t2 AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (NEW.a2); END",
                NO_ROWS,
            ),
            (
                "CREATE TRIGGER t3 BEFORE INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (NEW.b3); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: NEW.b3"),
            ("DROP TRIGGER t3", NO_ROWS),
            (INSERT, "error: no such column: NEW.b1"),
            ("DROP TRIGGER t1", NO_ROWS),
            (INSERT, "error: no such column: NEW.a2"),
            ("DROP TRIGGER t2", NO_ROWS),
            // A recursive BEFORE trigger and a TEMP trigger.
            (
                "CREATE TRIGGER tr BEFORE INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO t VALUES (NEW.a, NEW.zz); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: NEW.zz"),
            (DROP, NO_ROWS),
            (
                "CREATE TEMP TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (NEW.zz); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: NEW.zz"),
            (DROP, NO_ROWS),
            // The triggers a body statement fires are compiled too.
            (
                "CREATE TRIGGER tl AFTER INSERT ON lg \
                 BEGIN INSERT INTO o VALUES (NEW.nope, 1); END",
                NO_ROWS,
            ),
            (
                "CREATE TRIGGER tr AFTER INSERT ON t WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (NEW.a); END",
                NO_ROWS,
            ),
            (INSERT, "error: no such column: NEW.nope"),
            ("DROP TRIGGER tl", NO_ROWS),
            (INSERT, NO_ROWS),
            (DROP, NO_ROWS),
            // Nothing was written but the one row the valid trigger allowed.
            ("SELECT count(*) FROM t", "rows=[[\"1\"]]"),
            ("SELECT count(*) FROM lg", "rows=[[\"0\"]]"),
        ],
    },
    Scenario {
        name: "foreign-key actions compile the child triggers they fire",
        setup: &[
            "CREATE TABLE lg(m)",
            "CREATE TABLE par(a PRIMARY KEY, n)",
            "CREATE TABLE child(pa REFERENCES par(a) ON DELETE CASCADE ON UPDATE CASCADE)",
            "CREATE TABLE nul(pa REFERENCES par(a) ON DELETE SET NULL)",
            "CREATE TABLE plain(pa REFERENCES par(a))",
            "INSERT INTO par VALUES (1, 'x'), (2, 'y'), (3, 'z')",
            "INSERT INTO child VALUES (1)",
            "INSERT INTO nul VALUES (2)",
        ],
        probes: &[
            ("PRAGMA foreign_keys = 1", NO_ROWS),
            // The bead's data divergence: stock deletes nothing.
            (
                "CREATE TRIGGER ct AFTER DELETE ON child \
                 BEGIN INSERT INTO lg VALUES (OLD.nope); END",
                NO_ROWS,
            ),
            (
                "DELETE FROM par WHERE a = 1",
                "error: no such column: OLD.nope",
            ),
            (
                "SELECT a FROM par ORDER BY a",
                "rows=[[\"1\"], [\"2\"], [\"3\"]]",
            ),
            ("SELECT pa FROM child", "rows=[[\"1\"]]"),
            // Negative: an UPDATE of no key column takes no action.
            ("UPDATE par SET n = 'w' WHERE a = 1", NO_ROWS),
            ("SELECT n FROM par WHERE a = 1", "rows=[[\"'w'\"]]"),
            ("DROP TRIGGER ct", NO_ROWS),
            (
                "CREATE TRIGGER ct AFTER DELETE ON child BEGIN INSERT INTO missing VALUES (1); END",
                NO_ROWS,
            ),
            (
                "DELETE FROM par WHERE a = 1",
                "error: no such table: main.missing",
            ),
            ("DROP TRIGGER ct", NO_ROWS),
            // ON UPDATE CASCADE fires the child's UPDATE triggers; a DELETE
            // cascades as a DELETE, which they do not see.
            (
                "CREATE TRIGGER ct AFTER UPDATE ON child \
                 BEGIN INSERT INTO lg VALUES (NEW.nope); END",
                NO_ROWS,
            ),
            (
                "UPDATE par SET a = 10 WHERE a = 1",
                "error: no such column: NEW.nope",
            ),
            ("UPDATE par SET n = 'v' WHERE a = 1", NO_ROWS),
            ("DELETE FROM par WHERE a = 1", NO_ROWS),
            (
                "SELECT a, n FROM par ORDER BY a",
                "rows=[[\"2\", \"'y'\"], [\"3\", \"'z'\"]]",
            ),
            ("SELECT count(*) FROM child", "rows=[[\"0\"]]"),
            ("DROP TRIGGER ct", NO_ROWS),
            // ON DELETE SET NULL fires the child's UPDATE OF its key.
            (
                "CREATE TRIGGER nt AFTER UPDATE OF pa ON nul \
                 BEGIN INSERT INTO lg VALUES (OLD.nope); END",
                NO_ROWS,
            ),
            (
                "DELETE FROM par WHERE a = 2",
                "error: no such column: OLD.nope",
            ),
            ("SELECT pa FROM nul", "rows=[[\"2\"]]"),
            ("DROP TRIGGER nt", NO_ROWS),
            // Negatives: NO ACTION takes no action, and with foreign_keys
            // off no action is taken.
            (
                "CREATE TRIGGER pt AFTER DELETE ON plain \
                 BEGIN INSERT INTO lg VALUES (OLD.nope); END",
                NO_ROWS,
            ),
            (
                "CREATE TRIGGER pu AFTER UPDATE ON plain \
                 BEGIN INSERT INTO lg VALUES (OLD.nope); END",
                NO_ROWS,
            ),
            ("DELETE FROM par WHERE a = 3", NO_ROWS),
            ("SELECT a FROM par ORDER BY a", "rows=[[\"2\"]]"),
            ("DROP TRIGGER pt", NO_ROWS),
            ("DROP TRIGGER pu", NO_ROWS),
            (
                "CREATE TRIGGER nt AFTER UPDATE ON nul BEGIN INSERT INTO lg VALUES (OLD.nope); END",
                NO_ROWS,
            ),
            ("PRAGMA foreign_keys = 0", NO_ROWS),
            ("DELETE FROM par WHERE a = 2", NO_ROWS),
            ("SELECT count(*) FROM par", "rows=[[\"0\"]]"),
            ("SELECT pa FROM nul", "rows=[[\"2\"]]"),
            ("PRAGMA foreign_keys = 1", NO_ROWS),
            ("DROP TRIGGER nt", NO_ROWS),
            // A self-referencing cascade is compiled once.
            (
                "CREATE TABLE tree(id PRIMARY KEY, up REFERENCES tree(id) ON DELETE CASCADE)",
                NO_ROWS,
            ),
            ("INSERT INTO tree VALUES (1, NULL), (2, 1)", NO_ROWS),
            (
                "CREATE TRIGGER tt AFTER DELETE ON tree WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (OLD.id); END",
                NO_ROWS,
            ),
            ("DELETE FROM tree WHERE id = 1", NO_ROWS),
            ("SELECT count(*) FROM tree", "rows=[[\"0\"]]"),
            ("DROP TRIGGER tt", NO_ROWS),
            ("INSERT INTO tree VALUES (1, NULL), (2, 1)", NO_ROWS),
            (
                "CREATE TRIGGER tt AFTER DELETE ON tree WHEN 0 \
                 BEGIN INSERT INTO lg VALUES (OLD.nope); END",
                NO_ROWS,
            ),
            (
                "DELETE FROM tree WHERE id = 1",
                "error: no such column: OLD.nope",
            ),
            ("SELECT count(*) FROM tree", "rows=[[\"2\"]]"),
            ("SELECT count(*) FROM lg", "rows=[[\"0\"]]"),
        ],
    },
    Scenario {
        name: "valid triggers stock compiles still let their statements run",
        setup: &[
            "CREATE TABLE t(a, b)",
            "CREATE TABLE lg(m)",
            "CREATE TABLE o(z, w)",
            "CREATE TABLE g(a, b AS (a * 2))",
            "CREATE TABLE q(id INTEGER PRIMARY KEY, v UNIQUE)",
            "CREATE VIEW vt AS SELECT a, b FROM t",
            "CREATE TRIGGER v1 BEFORE INSERT ON t WHEN 0 \
             BEGIN SELECT RAISE(ABORT, 'no') WHERE NEW.a > 1; SELECT RAISE(IGNORE); END",
            "CREATE TRIGGER v2 AFTER INSERT ON t WHEN 0 \
             BEGIN INSERT INTO lg VALUES (NEW.rowid); INSERT INTO lg VALUES (NEW._rowid_ + NEW.oid); END",
            "CREATE TRIGGER v3 AFTER DELETE ON t WHEN 0 \
             BEGIN INSERT INTO lg VALUES (OLD.a + OLD.rowid); END",
            "CREATE TRIGGER v4 AFTER UPDATE ON t WHEN 0 BEGIN \
             INSERT INTO lg SELECT z FROM o WHERE o.w = NEW.b ORDER BY z LIMIT 1; \
             UPDATE lg SET m = o.z FROM o WHERE o.w = OLD.a; \
             INSERT INTO lg SELECT k FROM (SELECT NEW.a AS k); \
             DELETE FROM lg WHERE m IN (SELECT a FROM t WHERE t.b = NEW.b); \
             UPDATE lg SET m = (SELECT count(*) FROM o) WHERE m = OLD.b; \
             INSERT INTO lg SELECT max(z) FROM o GROUP BY w HAVING count(*) > NEW.a; END",
            "CREATE TRIGGER v5 AFTER INSERT ON t WHEN 0 BEGIN INSERT INTO q(id, v) \
             VALUES (NEW.a, NEW.b) ON CONFLICT(v) DO UPDATE SET id = excluded.id + q.id; END",
            "CREATE TRIGGER v6 AFTER UPDATE OF b ON t WHEN 0 \
             BEGIN INSERT INTO lg VALUES (NEW.nope); END",
            "CREATE TRIGGER v7 INSTEAD OF INSERT ON vt BEGIN INSERT INTO t VALUES (NEW.a, NEW.b); END",
            "CREATE TRIGGER v8 AFTER INSERT ON t \
             WHEN (SELECT count(*) FROM o WHERE z = NEW.a) > 100 BEGIN INSERT INTO lg VALUES (1); END",
            "CREATE TRIGGER v9 AFTER INSERT ON t WHEN 0 BEGIN \
             UPDATE lg SET rowid = rowid + 1 WHERE oid = NEW.rowid; \
             INSERT INTO lg(rowid, m) VALUES (NEW.a, NEW.b); END",
            "CREATE TRIGGER v10 AFTER INSERT ON t WHEN 0 \
             BEGIN INSERT INTO g VALUES (NEW.a); INSERT INTO g(a) SELECT NEW.b; END",
            "CREATE TRIGGER v11 AFTER INSERT ON t WHEN 0 BEGIN \
             INSERT INTO lg SELECT row_number() OVER (ORDER BY z) FROM o; \
             INSERT INTO lg SELECT value FROM json_each('[1]'); END",
            "CREATE TRIGGER v12 AFTER INSERT ON o WHEN 0 BEGIN INSERT INTO lg VALUES (NEW.zz); END",
            "CREATE TRIGGER v13 AFTER INSERT ON t WHEN 0 \
             BEGIN INSERT INTO lg SELECT x FROM (SELECT 1 AS x) AS new; END",
        ],
        probes: &[
            ("INSERT INTO t VALUES (1, 1)", NO_ROWS),
            ("INSERT INTO vt VALUES (3, 3)", NO_ROWS),
            ("UPDATE t SET a = 5 WHERE a = 1", NO_ROWS),
            ("DELETE FROM t WHERE a = 3", NO_ROWS),
            ("SELECT a, b FROM t ORDER BY a", "rows=[[\"5\", \"1\"]]"),
            // The UPDATE OF b trigger is compiled only for an UPDATE of b,
            // and another table's trigger only for that table.
            (
                "UPDATE t SET b = 5 WHERE a = 5",
                "error: no such column: NEW.nope",
            ),
            (
                "INSERT INTO o VALUES (1, 1)",
                "error: no such column: NEW.zz",
            ),
            ("SELECT count(*) FROM lg", "rows=[[\"0\"]]"),
        ],
    },
    Scenario {
        name: "the body statements stock compiles, in WHEN 0 triggers",
        setup: &[
            "CREATE TABLE t(a, b, \"order\" TEXT, d DEFAULT 5)",
            "CREATE TABLE t2(x, y)",
            "CREATE TABLE q(id INTEGER PRIMARY KEY, v UNIQUE, w)",
            "CREATE TABLE g(a, b AS (a * 2) STORED, c AS (a + 1))",
            "CREATE TABLE wr(k PRIMARY KEY, v) WITHOUT ROWID",
            "CREATE TRIGGER f1 AFTER INSERT ON t WHEN 0 BEGIN \
             UPDATE t2 AS z SET y = (SELECT count(*) FROM t2 AS w WHERE w.x = z.x) + NEW.a \
               WHERE z.x = NEW.b; \
             INSERT INTO t2 SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > NEW.a ORDER BY 1; \
             INSERT INTO t2 SELECT row_number() OVER (ORDER BY a), sum(b) OVER () FROM t; \
             SELECT CASE WHEN NEW.a < 0 THEN RAISE(ABORT, 'neg') END; \
             INSERT INTO t2 SELECT NEW.a, (SELECT count(*) FROM t2 WHERE t2.x = NEW.a); \
             UPDATE t2 SET x = x + 1 WHERE rowid = NEW.rowid; \
             INSERT INTO q(rowid, v, w) VALUES (NEW.a, NEW.b, NEW.\"order\"); \
             INSERT INTO q(v, w) VALUES (NEW.a, 1) \
               ON CONFLICT(v) DO UPDATE SET w = excluded.w + q.w WHERE q.id > 0; \
             INSERT OR REPLACE INTO q VALUES (NEW.a, NEW.b, NEW.d); \
             UPDATE t SET b = b + 1 WHERE a = NEW.a; \
             INSERT INTO t2 SELECT a, b FROM t WHERE a = NEW.a; \
             INSERT INTO g(a) VALUES (NEW.a); \
             INSERT INTO g VALUES (NEW.a); \
             INSERT INTO wr VALUES (NEW.a, NEW.b); \
             UPDATE wr SET v = NEW.b WHERE k = NEW.a; \
             INSERT INTO t2 VALUES (json_extract('{\"a\":1}', '$.a'), \
               coalesce(NEW.a, typeof(NEW.b), strftime('%Y', 'now'))); \
             INSERT INTO t2 SELECT value, key FROM json_each('[1,2]') WHERE value > NEW.a; \
             INSERT INTO t2 VALUES (NEW.a IN (SELECT x FROM t2), \
               EXISTS (SELECT 1 FROM t WHERE t.a = NEW.a)); \
             UPDATE t2 SET (x, y) = (NEW.a, NEW.b) WHERE x IS NULL; \
             UPDATE t2 SET (x, y) = (SELECT a, b FROM t WHERE t.a = NEW.a) WHERE x = 1; \
             INSERT INTO t2 SELECT * FROM t2 WHERE x = NEW.a; \
             INSERT INTO t2(y, x) SELECT b, a FROM t LIMIT 1; \
             DELETE FROM t2 WHERE x IN (SELECT a FROM t WHERE b = NEW.b) AND y IS NULL; END",
            "CREATE TRIGGER f2 BEFORE UPDATE OF a ON t WHEN 0 BEGIN \
             UPDATE t2 SET x = OLD.a WHERE x = NEW.a; \
             INSERT INTO t2 VALUES (OLD.rowid, NEW.rowid); \
             SELECT RAISE(IGNORE) WHERE OLD.\"order\" = NEW.\"order\"; END",
            "CREATE TRIGGER f3 AFTER DELETE ON t WHEN 0 BEGIN \
             INSERT INTO t2 VALUES (OLD.a, OLD.d); DELETE FROM t2 WHERE x = OLD.a; END",
            "CREATE VIEW v AS SELECT a, b FROM t",
            "CREATE TRIGGER f4 INSTEAD OF UPDATE ON v BEGIN \
             UPDATE t SET b = NEW.b WHERE a = OLD.a; END",
            // A double-quoted name that resolves to nothing is a string in
            // stock (SQLITE_DQS), so this body compiles.
            "CREATE TRIGGER f6 AFTER INSERT ON q WHEN 0 BEGIN \
             INSERT INTO t2 SELECT \"order\", d FROM t; INSERT INTO t2 VALUES (\"hello\", 1); END",
        ],
        probes: &[
            ("INSERT INTO t VALUES (1, 2, 'x', 3)", NO_ROWS),
            ("UPDATE t SET a = 5", NO_ROWS),
            ("DELETE FROM t WHERE a = 99", NO_ROWS),
            ("UPDATE v SET b = 9 WHERE a = 5", NO_ROWS),
            ("SELECT a, b FROM t", "rows=[[\"5\", \"9\"]]"),
            ("INSERT INTO q(v) VALUES (100)", NO_ROWS),
            ("SELECT count(*) FROM t2", "rows=[[\"0\"]]"),
        ],
    },
];

#[test]
fn trigger_bodies_that_do_not_compile_refuse_their_statements_like_stock() {
    let mut expected: Vec<Vec<String>> = Vec::new();
    for scenario in SCENARIOS {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in scenario.setup {
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
                        dir.path().join("bd_nf30k.db").to_str().unwrap().to_owned()
                    } else {
                        ":memory:".to_owned()
                    };
                    let conn = Connection::open(&path).await.unwrap();
                    for sql in scenario.setup {
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
