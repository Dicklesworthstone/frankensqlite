//! bd-nnnd8: `Connection::prepare` refused every statement but SELECT,
//! INSERT, UPDATE, DELETE and PRAGMA ("prepare() supports SELECT, INSERT,
//! UPDATE, DELETE, and PRAGMA statements only"). Stock's
//! `sqlite3_prepare_v2` prepares DDL, ATTACH and DETACH, transaction control,
//! savepoints, VACUUM, ANALYZE and REINDEX, and stepping the statement runs
//! it once. A wrapper or migration runner that prepares every statement
//! failed on its first CREATE TABLE.
//!
//! Stock raises most DDL errors while it prepares (sqlite3StartTable,
//! sqlite3CreateIndex, sqlite3BeginTrigger, sqlite3DropTable and the ALTER
//! TABLE entry points run in the parser), and the rest at the step:
//! transaction control, savepoints, ATTACH and DETACH. A statement held
//! across a schema change is re-prepared at its step, so its error is the
//! re-prepare's.
//!
//! Each scenario runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file. fsqlite runs every probe three ways: directly
//! (`Connection::execute` / `query`), prepared then queried, and prepared
//! then executed. A prepared probe's outcome tells a prepare error from a
//! step error, and must equal stock's; a direct probe's error must equal
//! stock's error text. Every stock outcome is pinned so the comparison
//! cannot pass vacuously.
//!
//! Negative cases a naive fix fails:
//! - accepting every statement at prepare and failing at the step: a
//!   prepared CREATE of a taken name, or a DROP of a missing object, must
//!   fail at prepare, as stock's does;
//! - running the statement at prepare: a statement prepared and dropped
//!   without a step must change nothing (no table, no index, no attached
//!   database, no open transaction or savepoint);
//! - caching a prepare-time verdict: a held statement whose name a later
//!   statement takes, or whose table a later statement drops, must fail at
//!   its step with the re-prepare's error, and a stepped CREATE must fail
//!   when stepped again;
//! - rendering the statement from its AST: the stored `sqlite_master.sql`
//!   keeps the text as written.

use fsqlite_core::connection::{Connection, PreparedStatement};
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

fn stock_message(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message))
        | rusqlite::Error::SqlInputError { msg: message, .. } => message.clone(),
        other => other.to_string(),
    }
}

/// One probe statement and how it is driven.
#[derive(Clone, Copy)]
enum Probe {
    /// Prepare, then step to the end.
    Run(&'static str),
    /// Prepare, step to the end, then step the same statement again.
    StepTwice(&'static str),
    /// Prepare, then drop the statement without a step.
    PrepareOnly(&'static str),
    /// Prepare, run `between` directly, then step the held statement.
    Held {
        sql: &'static str,
        between: &'static [&'static str],
    },
}

impl Probe {
    const fn sql(self) -> &'static str {
        match self {
            Self::Run(sql)
            | Self::StepTwice(sql)
            | Self::PrepareOnly(sql)
            | Self::Held { sql, .. } => sql,
        }
    }
}

fn returns_rows(sql: &str) -> bool {
    sql.starts_with("SELECT") || sql.starts_with("PRAGMA")
}

// ---------------------------------------------------------------- stock --

fn stock_step(statement: &mut rusqlite::Statement<'_>) -> String {
    let columns = statement.column_count();
    let result = (|| -> rusqlite::Result<Vec<Vec<String>>> {
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
        Err(error) => format!("step error: {}", stock_message(&error)),
    }
}

fn stock_run(conn: &rusqlite::Connection, sql: &str) -> String {
    match conn.prepare(sql) {
        Ok(mut statement) => stock_step(&mut statement),
        Err(error) => format!("prepare error: {}", stock_message(&error)),
    }
}

fn stock_outcome(conn: &rusqlite::Connection, probe: Probe) -> String {
    let sql = probe.sql();
    let mut statement = match conn.prepare(sql) {
        Ok(statement) => statement,
        Err(error) => return format!("prepare error: {}", stock_message(&error)),
    };
    match probe {
        Probe::Run(_) => stock_step(&mut statement),
        Probe::StepTwice(_) => {
            let first = stock_step(&mut statement);
            let second = stock_step(&mut statement);
            format!("{first} | {second}")
        }
        Probe::PrepareOnly(_) => {
            drop(statement);
            "prepared".to_owned()
        }
        Probe::Held { between, .. } => {
            let mut parts = vec!["held".to_owned()];
            parts.extend(between.iter().map(|sql| stock_run(conn, sql)));
            parts.push(stock_step(&mut statement));
            parts.join(" | ")
        }
    }
}

// -------------------------------------------------------------- fsqlite --

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// `Connection::execute`, or `Connection::query` for a row-returning probe.
    Direct,
    /// `Connection::prepare`, then `PreparedStatement::query` (`execute` for
    /// a DML probe).
    PreparedQuery,
    /// `Connection::prepare`, then `PreparedStatement::execute`, or `query`
    /// for a row-returning probe.
    PreparedExecute,
}

fn render_fsqlite_rows(rows: &[fsqlite_core::connection::Row]) -> String {
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect();
    format!("rows={rows:?}")
}

async fn fsqlite_direct(conn: &Connection, sql: &str) -> String {
    let result = if returns_rows(sql) {
        conn.query(sql).await
    } else {
        conn.execute(sql).await.map(|_| Vec::new())
    };
    match result {
        Ok(rows) => render_fsqlite_rows(&rows),
        Err(error) => format!("error: {error}"),
    }
}

/// A DML probe here is setup between schema statements: a prepared INSERT,
/// UPDATE or DELETE without RETURNING is executed in both prepared modes.
fn is_dml(sql: &str) -> bool {
    ["INSERT", "UPDATE", "DELETE"]
        .iter()
        .any(|keyword| sql.starts_with(keyword))
}

async fn fsqlite_step(statement: &PreparedStatement<'_>, sql: &str, mode: Mode) -> String {
    let result = if (mode == Mode::PreparedExecute && !returns_rows(sql)) || is_dml(sql) {
        statement.execute().await.map(|_| Vec::new())
    } else {
        statement.query().await
    };
    match result {
        Ok(rows) => render_fsqlite_rows(&rows),
        Err(error) => format!("step error: {error}"),
    }
}

async fn fsqlite_outcome(conn: &Connection, probe: Probe, mode: Mode) -> Option<String> {
    let sql = probe.sql();
    if mode == Mode::Direct {
        return match probe {
            Probe::Run(_) => Some(fsqlite_direct(conn, sql).await),
            Probe::StepTwice(_) => {
                let first = fsqlite_direct(conn, sql).await;
                let second = fsqlite_direct(conn, sql).await;
                Some(format!("{first} | {second}"))
            }
            // Nothing to run directly.
            Probe::PrepareOnly(_) => None,
            Probe::Held { between, .. } => {
                let mut parts = vec!["held".to_owned()];
                for sql in between {
                    parts.push(fsqlite_direct(conn, sql).await);
                }
                parts.push(fsqlite_direct(conn, sql).await);
                Some(parts.join(" | "))
            }
        };
    }
    let statement = match conn.prepare(sql).await {
        Ok(statement) => statement,
        Err(error) => return Some(format!("prepare error: {error}")),
    };
    Some(match probe {
        Probe::Run(_) => fsqlite_step(&statement, sql, mode).await,
        Probe::StepTwice(_) => {
            let first = fsqlite_step(&statement, sql, mode).await;
            let second = fsqlite_step(&statement, sql, mode).await;
            format!("{first} | {second}")
        }
        Probe::PrepareOnly(_) => {
            drop(statement);
            "prepared".to_owned()
        }
        Probe::Held { between, .. } => {
            // Every `between` statement succeeds on stock, so its outcome
            // carries no error to place at a prepare or a step.
            let mut parts = vec!["held".to_owned()];
            for sql in between {
                parts.push(fsqlite_direct(conn, sql).await);
            }
            parts.push(fsqlite_step(&statement, sql, mode).await);
            parts.join(" | ")
        }
    })
}

/// A prepared outcome as a direct run reports it: one error, whether stock
/// raised it at the prepare or at the step.
fn collapse_timing(outcome: &str) -> String {
    outcome
        .replace("prepare error: ", "error: ")
        .replace("step error: ", "error: ")
}

// ------------------------------------------------------------ scenarios --

struct Scenario {
    name: &'static str,
    setup: &'static [&'static str],
    probes: &'static [(Probe, &'static str)],
}

const NO_ROWS: &str = "rows=[]";

const T_SETUP: &[&str] = &[
    "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
    "INSERT INTO t VALUES (1, 'x'), (2, 'y')",
];

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "every schema statement kind prepares and lands as execute() lands it",
        setup: T_SETUP,
        probes: &[
            (Probe::Run("CREATE TABLE u(a, b DEFAULT 'd')"), NO_ROWS),
            (
                Probe::Run("CREATE  TABLE  w ( k  INTEGER  PRIMARY KEY , v )"),
                NO_ROWS,
            ),
            // The stored text is the statement as written.
            (Probe::Run("CREATE  TABLE  sp ( k  INTEGER , v )"), NO_ROWS),
            (Probe::Run("CREATE  INDEX  isp  ON  sp ( v )"), NO_ROWS),
            (
                Probe::Run("CREATE  VIEW  vsp  AS  SELECT  k  FROM  sp"),
                NO_ROWS,
            ),
            (
                Probe::Run(
                    "SELECT name, sql FROM sqlite_master WHERE name IN ('sp', 'isp', 'vsp') \
                     ORDER BY name",
                ),
                "rows=[[\"'isp'\", \"'CREATE INDEX isp  ON  sp ( v )'\"], \
                 [\"'sp'\", \"'CREATE TABLE sp ( k  INTEGER , v )'\"], \
                 [\"'vsp'\", \"'CREATE VIEW vsp  AS  SELECT  k  FROM  sp'\"]]",
            ),
            (Probe::Run("DROP VIEW vsp"), NO_ROWS),
            (Probe::Run("DROP TABLE sp"), NO_ROWS),
            (Probe::Run("CREATE TABLE IF NOT EXISTS u(z)"), NO_ROWS),
            (Probe::Run("CREATE INDEX ib ON t(b)"), NO_ROWS),
            (
                Probe::Run("CREATE UNIQUE INDEX IF NOT EXISTS ib ON t(b)"),
                NO_ROWS,
            ),
            (
                Probe::Run("CREATE VIEW v AS SELECT a, b FROM t WHERE a > 1"),
                NO_ROWS,
            ),
            (
                Probe::Run(
                    "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u(a) VALUES (NEW.a); END",
                ),
                NO_ROWS,
            ),
            (Probe::Run("INSERT INTO t VALUES (3, 'z')"), NO_ROWS),
            (
                Probe::Run("ALTER TABLE w ADD COLUMN c INTEGER DEFAULT 7"),
                NO_ROWS,
            ),
            (Probe::Run("ALTER TABLE w RENAME COLUMN c TO c2"), NO_ROWS),
            (Probe::Run("ALTER TABLE w RENAME TO w2"), NO_ROWS),
            (Probe::Run("ALTER TABLE w2 DROP COLUMN v"), NO_ROWS),
            (
                Probe::Run("CREATE TABLE ctas AS SELECT a, b FROM t WHERE a < 3"),
                NO_ROWS,
            ),
            (Probe::Run("ANALYZE"), NO_ROWS),
            (Probe::Run("ANALYZE t"), NO_ROWS),
            (Probe::Run("REINDEX"), NO_ROWS),
            (Probe::Run("REINDEX ib"), NO_ROWS),
            (Probe::Run("VACUUM"), NO_ROWS),
            // The text RENAME COLUMN, RENAME TO and DROP COLUMN leave for w2
            // differs from stock's on direct execution too (fsqlite renders it
            // anew), so w2's text is left out; its columns are read below.
            (
                Probe::Run(
                    "SELECT type, name, tbl_name, sql FROM sqlite_master \
                     WHERE name NOT LIKE 'sqlite_stat%' AND name <> 'w2' ORDER BY type, name",
                ),
                "rows=[[\"'index'\", \"'ib'\", \"'t'\", \"'CREATE INDEX ib ON t(b)'\"], \
                 [\"'table'\", \"'ctas'\", \"'ctas'\", \"'CREATE TABLE ctas(a INT,b TEXT)'\"], \
                 [\"'table'\", \"'t'\", \"'t'\", \"'CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)'\"], \
                 [\"'table'\", \"'u'\", \"'u'\", \"'CREATE TABLE u(a, b DEFAULT 'd')'\"], \
                 [\"'trigger'\", \"'tr'\", \"'t'\", \
                 \"'CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u(a) VALUES (NEW.a); END'\"], \
                 [\"'view'\", \"'v'\", \"'v'\", \"'CREATE VIEW v AS SELECT a, b FROM t WHERE a > 1'\"]]",
            ),
            (
                Probe::Run("SELECT type, name, tbl_name FROM sqlite_master WHERE name = 'w2'"),
                "rows=[[\"'table'\", \"'w2'\", \"'w2'\"]]",
            ),
            (
                Probe::Run("SELECT * FROM u ORDER BY a"),
                "rows=[[\"3\", \"'d'\"]]",
            ),
            (
                Probe::Run("SELECT * FROM v"),
                "rows=[[\"2\", \"'y'\"], [\"3\", \"'z'\"]]",
            ),
            (
                Probe::Run("SELECT * FROM ctas ORDER BY a"),
                "rows=[[\"1\", \"'x'\"], [\"2\", \"'y'\"]]",
            ),
            (Probe::Run("INSERT INTO w2(k) VALUES (10)"), NO_ROWS),
            (Probe::Run("SELECT * FROM w2"), "rows=[[\"10\", \"7\"]]"),
            (Probe::Run("DROP TRIGGER tr"), NO_ROWS),
            (Probe::Run("DROP VIEW v"), NO_ROWS),
            (Probe::Run("DROP INDEX ib"), NO_ROWS),
            (Probe::Run("DROP TABLE ctas"), NO_ROWS),
            (
                Probe::Run(
                    "SELECT type, name FROM sqlite_master \
                     WHERE name NOT LIKE 'sqlite_stat%' ORDER BY type, name",
                ),
                "rows=[[\"'table'\", \"'t'\"], [\"'table'\", \"'u'\"], [\"'table'\", \"'w2'\"]]",
            ),
        ],
    },
    Scenario {
        name: "a schema statement stock refuses while preparing fails at prepare",
        setup: &[
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
            "INSERT INTO t VALUES (1, 'x'), (2, 'y')",
            "CREATE INDEX ib ON t(b)",
            "CREATE VIEW v AS SELECT a, b FROM t",
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END",
        ],
        probes: &[
            (
                Probe::Run("CREATE TABLE t(z)"),
                "prepare error: table t already exists",
            ),
            (
                Probe::Run("CREATE TABLE T(z)"),
                "prepare error: table T already exists",
            ),
            (
                Probe::Run("CREATE TABLE main.t(z)"),
                "prepare error: table t already exists",
            ),
            (
                Probe::Run("CREATE TABLE v(z)"),
                "prepare error: view v already exists",
            ),
            // An index's name is taken too, IF NOT EXISTS or not.
            (
                Probe::Run("CREATE TABLE ib(z)"),
                "prepare error: there is already an index named ib",
            ),
            (
                Probe::Run("CREATE TABLE IF NOT EXISTS ib(z)"),
                "prepare error: there is already an index named ib",
            ),
            (
                Probe::Run("CREATE VIEW IF NOT EXISTS ib AS SELECT 1"),
                "prepare error: there is already an index named ib",
            ),
            (
                Probe::Run("CREATE VIEW t AS SELECT 1"),
                "prepare error: table t already exists",
            ),
            (
                Probe::Run("CREATE VIEW v AS SELECT 1"),
                "prepare error: view v already exists",
            ),
            (
                Probe::Run("CREATE INDEX ib ON t(b)"),
                "prepare error: index ib already exists",
            ),
            (
                Probe::Run("CREATE INDEX t ON t(b)"),
                "prepare error: there is already a table named t",
            ),
            (
                Probe::Run("CREATE INDEX v ON t(b)"),
                "prepare error: there is already a table named v",
            ),
            (
                Probe::Run("CREATE INDEX ix ON missing(b)"),
                "prepare error: no such table: main.missing",
            ),
            (
                Probe::Run("CREATE INDEX ix ON t(nosuch)"),
                "prepare error: no such column: nosuch",
            ),
            (
                Probe::Run("CREATE INDEX ix ON v(a)"),
                "prepare error: views may not be indexed",
            ),
            (
                Probe::Run("CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END"),
                "prepare error: trigger tr already exists",
            ),
            (
                Probe::Run("CREATE TRIGGER tr2 AFTER INSERT ON missing BEGIN SELECT 1; END"),
                "prepare error: no such table: main.missing",
            ),
            (
                Probe::Run("CREATE TRIGGER tr2 INSTEAD OF INSERT ON t BEGIN SELECT 1; END"),
                "prepare error: cannot create INSTEAD OF trigger on table: t",
            ),
            (
                Probe::Run("CREATE TRIGGER tr2 BEFORE INSERT ON v BEGIN SELECT 1; END"),
                "prepare error: cannot create BEFORE trigger on view: v",
            ),
            (
                Probe::Run("CREATE TABLE sqlite_x(a)"),
                "prepare error: object name reserved for internal use: sqlite_x",
            ),
            (
                Probe::Run("CREATE INDEX sqlite_i ON t(b)"),
                "prepare error: object name reserved for internal use: sqlite_i",
            ),
            (
                Probe::Run("CREATE VIEW sqlite_v AS SELECT 1"),
                "prepare error: object name reserved for internal use: sqlite_v",
            ),
            (
                Probe::Run("CREATE TEMP TABLE main.q(a)"),
                "prepare error: temporary table name must be unqualified",
            ),
            (
                Probe::Run("CREATE TABLE aux.q(a)"),
                "prepare error: unknown database aux",
            ),
            (
                Probe::Run("CREATE VIEW aux.vv AS SELECT 1"),
                "prepare error: unknown database aux",
            ),
            (Probe::Run("CREATE TABLE IF NOT EXISTS t(z)"), NO_ROWS),
            (Probe::Run("CREATE TABLE IF NOT EXISTS v(z)"), NO_ROWS),
            (
                Probe::Run("CREATE VIEW IF NOT EXISTS v AS SELECT 1"),
                NO_ROWS,
            ),
            (Probe::Run("CREATE INDEX IF NOT EXISTS ib ON t(b)"), NO_ROWS),
            (
                Probe::Run("CREATE INDEX IF NOT EXISTS t ON t(b)"),
                "prepare error: there is already a table named t",
            ),
            (
                Probe::Run("CREATE TRIGGER IF NOT EXISTS tr AFTER INSERT ON t BEGIN SELECT 1; END"),
                NO_ROWS,
            ),
            (
                Probe::Run("DROP TABLE missing"),
                "prepare error: no such table: missing",
            ),
            (
                Probe::Run("DROP INDEX missing"),
                "prepare error: no such index: missing",
            ),
            (
                Probe::Run("DROP VIEW missing"),
                "prepare error: no such view: missing",
            ),
            (
                Probe::Run("DROP TRIGGER missing"),
                "prepare error: no such trigger: missing",
            ),
            (
                Probe::Run("DROP TABLE main.missing"),
                "prepare error: no such table: main.missing",
            ),
            (
                Probe::Run("DROP VIEW t"),
                "prepare error: use DROP TABLE to delete table t",
            ),
            (
                Probe::Run("DROP TABLE v"),
                "prepare error: use DROP VIEW to delete view v",
            ),
            (Probe::Run("DROP TABLE IF EXISTS missing"), NO_ROWS),
            (
                Probe::Run("ALTER TABLE missing ADD COLUMN c"),
                "prepare error: no such table: missing",
            ),
            (
                Probe::Run("ALTER TABLE missing RENAME TO m2"),
                "prepare error: no such table: missing",
            ),
            (
                Probe::Run("ALTER TABLE t ADD COLUMN b"),
                "prepare error: duplicate column name: b",
            ),
            (
                Probe::Run("ALTER TABLE t RENAME TO v"),
                "prepare error: there is already another table or index with this name: v",
            ),
            (
                Probe::Run("ANALYZE missing"),
                "prepare error: no such table: missing",
            ),
            (
                Probe::Run("REINDEX missing"),
                "prepare error: unable to identify the object to be reindexed",
            ),
            // These are refused only when the statement runs.
            (
                Probe::Run("DETACH nosuch"),
                "step error: no such database: nosuch",
            ),
            (
                Probe::Run("COMMIT"),
                "step error: cannot commit - no transaction is active",
            ),
            (
                Probe::Run("ROLLBACK"),
                "step error: cannot rollback - no transaction is active",
            ),
            (
                Probe::Run("RELEASE nosuch"),
                "step error: no such savepoint: nosuch",
            ),
            (
                Probe::Run("ROLLBACK TO nosuch"),
                "step error: no such savepoint: nosuch",
            ),
            // A MAIN index's name is free in the TEMP namespace.
            (Probe::Run("CREATE TEMP TABLE ib(z)"), NO_ROWS),
            (
                Probe::Run("SELECT type, name, tbl_name FROM sqlite_master ORDER BY name"),
                "rows=[[\"'index'\", \"'ib'\", \"'t'\"], [\"'table'\", \"'t'\", \"'t'\"], \
                 [\"'trigger'\", \"'tr'\", \"'t'\"], [\"'view'\", \"'v'\", \"'v'\"]]",
            ),
        ],
    },
    Scenario {
        name: "a re-stepped or held statement re-prepares at its step",
        setup: T_SETUP,
        probes: &[
            (
                Probe::StepTwice("CREATE TABLE t2(a)"),
                "rows=[] | step error: table t2 already exists",
            ),
            (
                Probe::StepTwice("CREATE TABLE IF NOT EXISTS t3(a)"),
                "rows=[] | rows=[]",
            ),
            (
                Probe::StepTwice("CREATE INDEX i2 ON t2(a)"),
                "rows=[] | step error: index i2 already exists",
            ),
            (
                Probe::StepTwice("DROP TABLE t3"),
                "rows=[] | step error: no such table: t3",
            ),
            (
                Probe::StepTwice("BEGIN"),
                "rows=[] | step error: cannot start a transaction within a transaction",
            ),
            (Probe::Run("COMMIT"), NO_ROWS),
            (Probe::StepTwice("ANALYZE"), "rows=[] | rows=[]"),
            (Probe::StepTwice("REINDEX"), "rows=[] | rows=[]"),
            (
                Probe::Held {
                    sql: "CREATE TABLE t4(a)",
                    between: &["CREATE TABLE t4(z)"],
                },
                "held | rows=[] | step error: table t4 already exists",
            ),
            (
                Probe::Held {
                    sql: "CREATE TABLE IF NOT EXISTS t5(a)",
                    between: &["CREATE TABLE t5(z)"],
                },
                "held | rows=[] | rows=[]",
            ),
            (
                Probe::Held {
                    sql: "DROP TABLE t2",
                    between: &["DROP TABLE t2"],
                },
                "held | rows=[] | step error: no such table: t2",
            ),
            (
                Probe::Held {
                    sql: "CREATE INDEX i9 ON t(b)",
                    between: &["CREATE TABLE tmpx(a)"],
                },
                "held | rows=[] | rows=[]",
            ),
            (
                Probe::Held {
                    sql: "ALTER TABLE t4 ADD COLUMN c",
                    between: &["ALTER TABLE t4 ADD COLUMN c"],
                },
                "held | rows=[] | step error: duplicate column name: c",
            ),
            (
                Probe::Held {
                    sql: "CREATE TRIGGER trx AFTER INSERT ON t4 BEGIN SELECT 1; END",
                    between: &["DROP TABLE t4"],
                },
                "held | rows=[] | step error: no such table: main.t4",
            ),
            (
                Probe::Held {
                    sql: "COMMIT",
                    between: &["BEGIN", "CREATE TABLE inx(a)"],
                },
                "held | rows=[] | rows=[] | rows=[]",
            ),
            (
                Probe::Held {
                    sql: "CREATE INDEX i8 ON t(b)",
                    between: &["DROP TABLE t"],
                },
                "held | rows=[] | step error: no such table: main.t",
            ),
            (
                Probe::Run(
                    "SELECT type, name, tbl_name, sql FROM sqlite_master \
                     WHERE name NOT LIKE 'sqlite_stat%' ORDER BY name",
                ),
                "rows=[[\"'table'\", \"'inx'\", \"'inx'\", \"'CREATE TABLE inx(a)'\"], \
                 [\"'table'\", \"'t5'\", \"'t5'\", \"'CREATE TABLE t5(z)'\"], \
                 [\"'table'\", \"'tmpx'\", \"'tmpx'\", \"'CREATE TABLE tmpx(a)'\"]]",
            ),
        ],
    },
    Scenario {
        name: "a statement prepared and dropped without a step changes nothing",
        setup: &[
            "CREATE TABLE p(a, b)",
            "INSERT INTO p VALUES (1, 2)",
            "CREATE INDEX pb ON p(b)",
        ],
        probes: &[
            (Probe::PrepareOnly("CREATE TABLE z(a)"), "prepared"),
            (Probe::PrepareOnly("DROP TABLE p"), "prepared"),
            (Probe::PrepareOnly("DROP INDEX pb"), "prepared"),
            (Probe::PrepareOnly("ALTER TABLE p ADD COLUMN c"), "prepared"),
            (Probe::PrepareOnly("ALTER TABLE p RENAME TO p9"), "prepared"),
            (Probe::PrepareOnly("CREATE INDEX pc ON p(a)"), "prepared"),
            (Probe::PrepareOnly("CREATE VIEW pv AS SELECT 1"), "prepared"),
            (
                Probe::PrepareOnly("CREATE TRIGGER ptr AFTER INSERT ON p BEGIN SELECT 1; END"),
                "prepared",
            ),
            (Probe::PrepareOnly("ATTACH ':memory:' AS aux"), "prepared"),
            (Probe::PrepareOnly("BEGIN"), "prepared"),
            (Probe::PrepareOnly("SAVEPOINT sp"), "prepared"),
            (Probe::PrepareOnly("VACUUM"), "prepared"),
            (Probe::PrepareOnly("ANALYZE"), "prepared"),
            (
                Probe::Run("SELECT type, name, sql FROM sqlite_master ORDER BY name"),
                "rows=[[\"'table'\", \"'p'\", \"'CREATE TABLE p(a, b)'\"], \
                 [\"'index'\", \"'pb'\", \"'CREATE INDEX pb ON p(b)'\"]]",
            ),
            (Probe::Run("SELECT * FROM p"), "rows=[[\"1\", \"2\"]]"),
            (
                Probe::Run("DETACH aux"),
                "step error: no such database: aux",
            ),
            (
                Probe::Run("COMMIT"),
                "step error: cannot commit - no transaction is active",
            ),
            (
                Probe::Run("RELEASE sp"),
                "step error: no such savepoint: sp",
            ),
        ],
    },
    Scenario {
        name: "transaction control, savepoints, ATTACH and DETACH prepare and run",
        setup: &["CREATE TABLE s(a, b)"],
        probes: &[
            (Probe::Run("BEGIN"), NO_ROWS),
            (Probe::Run("INSERT INTO s VALUES (1, 'x')"), NO_ROWS),
            (Probe::Run("SAVEPOINT s1"), NO_ROWS),
            (Probe::Run("INSERT INTO s VALUES (2, 'y')"), NO_ROWS),
            (Probe::Run("ROLLBACK TO s1"), NO_ROWS),
            (Probe::Run("RELEASE s1"), NO_ROWS),
            (Probe::Run("COMMIT"), NO_ROWS),
            (Probe::Run("SELECT * FROM s"), "rows=[[\"1\", \"'x'\"]]"),
            (Probe::Run("BEGIN IMMEDIATE"), NO_ROWS),
            (Probe::Run("INSERT INTO s VALUES (3, 'z')"), NO_ROWS),
            (Probe::Run("ROLLBACK"), NO_ROWS),
            (Probe::Run("SELECT count(*) FROM s"), "rows=[[\"1\"]]"),
            (Probe::Run("SAVEPOINT outer_sp"), NO_ROWS),
            (Probe::Run("INSERT INTO s VALUES (4, 'w')"), NO_ROWS),
            (Probe::Run("RELEASE outer_sp"), NO_ROWS),
            (Probe::Run("SELECT count(*) FROM s"), "rows=[[\"2\"]]"),
            (Probe::Run("BEGIN"), NO_ROWS),
            (
                Probe::Run("BEGIN"),
                "step error: cannot start a transaction within a transaction",
            ),
            (Probe::Run("END"), NO_ROWS),
            (
                Probe::Run("END"),
                "step error: cannot commit - no transaction is active",
            ),
            (Probe::StepTwice("SAVEPOINT sv"), "rows=[] | rows=[]"),
            (Probe::Run("RELEASE sv"), NO_ROWS),
            (Probe::Run("RELEASE sv"), NO_ROWS),
            (
                Probe::Run("RELEASE sv"),
                "step error: no such savepoint: sv",
            ),
            (Probe::Run("ATTACH ':memory:' AS aux"), NO_ROWS),
            (Probe::Run("CREATE TABLE aux.q(a)"), NO_ROWS),
            (Probe::Run("INSERT INTO aux.q VALUES (5)"), NO_ROWS),
            (Probe::Run("SELECT * FROM aux.q"), "rows=[[\"5\"]]"),
            (
                Probe::Run("ATTACH ':memory:' AS aux"),
                "step error: database aux is already in use",
            ),
            (Probe::Run("DETACH aux"), NO_ROWS),
            (
                Probe::Run("DETACH aux"),
                "step error: no such database: aux",
            ),
            (
                Probe::Run("SELECT * FROM s ORDER BY a"),
                "rows=[[\"1\", \"'x'\"], [\"4\", \"'w'\"]]",
            ),
        ],
    },
];

#[test]
fn schema_statements_prepare_and_step_like_stock() {
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
            .map(|(probe, _)| stock_outcome(&stock, *probe))
            .collect();
        // Not vacuous: stock gives each pinned outcome.
        for ((probe, pinned), stock_gave) in scenario.probes.iter().zip(&outcomes) {
            assert_eq!(
                stock_gave,
                pinned,
                "{}: stock on `{}`",
                scenario.name,
                probe.sql()
            );
        }
        expected.push(outcomes);
    }
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for file_backed in [false, true] {
            for mode in [Mode::Direct, Mode::PreparedQuery, Mode::PreparedExecute] {
                for (scenario, expected) in SCENARIOS.iter().zip(&expected) {
                    let dir = tempfile::tempdir().unwrap();
                    let path = if file_backed {
                        dir.path().join("bd_nnnd8.db").to_str().unwrap().to_owned()
                    } else {
                        ":memory:".to_owned()
                    };
                    let conn = Connection::open(&path).await.unwrap();
                    for sql in scenario.setup {
                        conn.execute(sql).await.unwrap_or_else(|error| {
                            panic!("{}: fsqlite setup `{sql}`: {error}", scenario.name)
                        });
                    }
                    for ((probe, _), expected) in scenario.probes.iter().zip(expected) {
                        let Some(got) = fsqlite_outcome(&conn, *probe, mode).await else {
                            continue;
                        };
                        let want = if mode == Mode::Direct {
                            collapse_timing(expected)
                        } else {
                            expected.clone()
                        };
                        if got != want {
                            mismatches.push(format!(
                                "{} (file_backed={file_backed} mode={mode:?}) `{}`:\n    \
                                 got  {got}\n    want {want}",
                                scenario.name,
                                probe.sql()
                            ));
                        }
                    }
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} outcome(s) differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
