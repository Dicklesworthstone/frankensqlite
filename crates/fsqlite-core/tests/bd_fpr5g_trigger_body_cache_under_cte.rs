#![recursion_limit = "512"]
//! bd-fpr5g: a statement with a WITH clause compiled its trigger bodies again
//! for every firing row. The statement turns the compiled-program cache off
//! for its own scope (its CTE names resolve to temporary roots), and the
//! trigger scope kept that bypass, although a trigger body cannot see the
//! caller's CTEs (they are hidden for the trigger's duration) and binds
//! NEW / OLD as parameters, so its text identifies its program. The trigger
//! scope now reuses compiled programs.
//!
//! The keepers pin the compile count of a CTE-fed trigger insert (a constant
//! handful, not two per firing row) and, against bundled SQLite 3.54.1 on
//! `:memory:` and on a file, that the reuse does not leak a scope: a CTE named
//! like a body table shadows it for the outer statement only, programs cached
//! inside a WITH scope serve a plain statement afterwards and the other way
//! round, and a body that names only a caller's CTE fails as stock fails. The
//! hot-path profile counters are process-global, so the tests run one at a
//! time. A statement that fires a trigger is replayed row by row, which strict
//! mode refuses, so these run without it.
//!
//! Lever 2 shares the trigger definitions (`Arc`) between the connection's
//! list, its savepoint snapshots and each firing, and keeps only trigger names
//! on the recursion stack. The last two keepers hold what that touches: an
//! ALTER TABLE RENAME / RENAME COLUMN rolled back (or rolled back to a
//! savepoint) leaves the trigger as it was, a committed one rewrites it, and
//! `recursive_triggers` OFF stops only the trigger re-entering itself, while ON
//! lets it recurse.
//!
//! Lever 3b runs a bound INSERT, UPDATE or DELETE through its compiled program
//! after its first firing (the kernel in connection/insert_batch.rs, whose lib
//! tests count the kernel runs). The last keeper holds what that kernel must
//! give up or keep: a trigger on its target, foreign keys turned on, a failing
//! firing, conflict overrides, rollback, and a TEMP target.
//!
//! Lever RC (retained cursors) keeps the cursor a trigger body's program
//! displaces from the engine's slot as a spare, which the next open of its
//! root takes back (fsqlite-vdbe's lib test pins that). Its keeper writes the
//! bodies' tables and the firing table from everywhere a spare could go stale
//! under: outside the bodies, from the firing table's own trigger with
//! recursion off and on, after a DROP / CREATE / CREATE INDEX, and after a
//! savepoint rollback and a failed statement.
//!
//! Lever 5 runs the outer INSERT ... SELECT replay of a materialized SELECT,
//! and of a stream while temp roots exist, through GH#516's compiled row
//! program; its keeper replays across a chunk boundary, through RAISE,
//! conflict overrides, a source-writing trigger, a view read in a body and a
//! rollback.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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

fn database_path(dir: &tempfile::TempDir, file_backed: bool) -> String {
    if file_backed {
        dir.path().join("fpr5g.db").to_str().unwrap().to_owned()
    } else {
        ":memory:".to_owned()
    }
}

/// The bead's trigger: a WHEN guard, an INSERT and an UPDATE in the body.
const BEAD_SCHEMA: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, n INTEGER)",
    "CREATE TABLE lg(k, m)",
    "CREATE TABLE u(k TEXT PRIMARY KEY, c INTEGER)",
    "INSERT INTO u VALUES ('even', 0), ('odd', 0)",
    "CREATE TRIGGER ti AFTER INSERT ON t WHEN NEW.n % 7 <> 3 BEGIN \
       INSERT INTO lg VALUES (NEW.id, NEW.a = 'x' OR NEW.b = 'Y' OR NEW.n > 10); \
       UPDATE u SET c = c + 1 WHERE k = CASE WHEN NEW.n % 2 = 0 THEN 'even' ELSE 'odd' END; \
     END",
];

const BEAD_ROWS: i64 = 300;

const BEAD_INSERT: &str = "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s \
    WHERE i < 300) INSERT INTO t SELECT i, 'x' || (i % 10), 'y' || (i % 5), i FROM s";

const BEAD_VERIFY: &[&str] = &[
    "SELECT count(*), sum(id), sum(n) FROM t",
    "SELECT count(*), sum(k), sum(m) FROM lg",
    "SELECT k, c FROM u ORDER BY k",
];

/// Compilations allowed for the 300-row CTE-fed insert: a constant handful
/// for the statement, its CTE and the two body statements. Compiling the body
/// per firing row is two programs for each of the 257 firing rows.
const MAX_BEAD_COMPILATIONS: u64 = 40;

#[test]
fn cte_fed_trigger_bodies_compile_once_and_match_stock() {
    let _serial = serial();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in BEAD_SCHEMA {
        stock.execute_batch(sql).unwrap();
    }
    stock.execute_batch(BEAD_INSERT).unwrap();
    let expected: Vec<_> = BEAD_VERIFY.iter().map(|sql| stock_rows(&stock, sql)).collect();
    // Not vacuous: every row arrives and the trigger fires for all but the
    // rows with n % 7 = 3.
    let firing = (1..=BEAD_ROWS).filter(|n| n % 7 != 3).count();
    assert_eq!(expected[0][0][0], BEAD_ROWS.to_string(), "{expected:?}");
    assert_eq!(expected[1][0][0], firing.to_string(), "{expected:?}");
    asupersync::test_utils::run_test(|| async move {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let conn = Connection::open(&database_path(&dir, file_backed))
                .await
                .unwrap();
            for sql in BEAD_SCHEMA {
                conn.execute(sql).await.unwrap();
            }
            set_hot_path_profile_enabled(true);
            reset_hot_path_profile();
            let outcome = conn.execute(BEAD_INSERT).await;
            let compilations = hot_path_profile_snapshot().parser.compiled_cache_misses;
            set_hot_path_profile_enabled(false);
            outcome.unwrap_or_else(|e| panic!("file_backed={file_backed}: {e}"));
            eprintln!(
                "bd-fpr5g: file_backed={file_backed}: {firing} firing rows compiled \
                 {compilations} programs"
            );
            assert!(
                compilations <= MAX_BEAD_COMPILATIONS,
                "bd-fpr5g: file_backed={file_backed}: {compilations} compilations for \
                 {firing} firing rows; trigger bodies compile per row under a WITH scope \
                 (limit {MAX_BEAD_COMPILATIONS})"
            );
            for (sql, want) in BEAD_VERIFY.iter().zip(&expected) {
                assert_eq!(
                    &fsqlite_rows(&conn, sql).await,
                    want,
                    "file_backed={file_backed}: {sql}"
                );
            }
        }
    });
}

/// A body that reads and writes `u` while callers bind a CTE named `u`, and a
/// trigger whose body names a relation that only a caller's CTE binds.
const SCOPE_SCHEMA: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)",
    "CREATE TABLE u(k TEXT PRIMARY KEY, c INTEGER)",
    "INSERT INTO u VALUES ('even', 0), ('odd', 0)",
    "CREATE TABLE lg(id, seen, total)",
    "CREATE TRIGGER ti AFTER INSERT ON t BEGIN \
       INSERT INTO lg VALUES (NEW.id, (SELECT count(*) FROM u), (SELECT sum(c) FROM u)); \
       UPDATE u SET c = c + NEW.n WHERE k = CASE WHEN NEW.n % 2 = 0 THEN 'even' ELSE 'odd' END; \
     END",
    "CREATE TABLE q(x)",
    "CREATE TRIGGER qi AFTER INSERT ON q BEGIN \
       INSERT INTO lg SELECT NEW.x, count(*), 0 FROM only_cte; END",
];

/// Each step runs on both engines; a step either succeeds on both or fails on
/// both. Plain statements warm the cache before and after the WITH scopes.
const SCOPE_STEPS: &[&str] = &[
    "INSERT INTO t VALUES (1, 1)",
    // The outer SELECT reads the three-row CTE u; the body still sees the
    // two-row table u.
    "WITH u(k, c) AS (SELECT 'a', 100 UNION ALL SELECT 'b', 200 UNION ALL SELECT 'c', 301) \
     INSERT INTO t SELECT 10 + c / 100, c FROM u",
    // A CTE u with another shape: a body program bound to it would have no c.
    "WITH RECURSIVE u(i) AS (SELECT 20 UNION ALL SELECT i + 1 FROM u WHERE i < 24) \
     INSERT INTO t SELECT i, i FROM u",
    "INSERT INTO t VALUES (2, 3)",
    "WITH u(k, c) AS (SELECT 'z', 7) INSERT INTO t SELECT 40, c FROM u",
    // Only the caller binds only_cte: the body fails, and the statement with it.
    "WITH only_cte(v) AS (SELECT 1) INSERT INTO q SELECT v FROM only_cte",
    "INSERT INTO t VALUES (3, 4)",
];

const SCOPE_VERIFY: &[&str] = &[
    "SELECT * FROM t ORDER BY id",
    "SELECT * FROM u ORDER BY k",
    "SELECT * FROM lg ORDER BY rowid",
    "SELECT count(*) FROM q",
];

#[test]
fn caller_ctes_stay_invisible_to_cached_trigger_bodies() {
    let _serial = serial();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SCOPE_SCHEMA {
        stock.execute_batch(sql).unwrap();
    }
    let stock_outcomes: Vec<Option<String>> = SCOPE_STEPS
        .iter()
        .map(|sql| stock.execute_batch(sql).err().map(|e| e.to_string()))
        .collect();
    let expected: Vec<_> = SCOPE_VERIFY.iter().map(|sql| stock_rows(&stock, sql)).collect();
    // Not vacuous: the body under a CTE named u counted the table's two rows,
    // not the CTE's three, and the caller-only name failed.
    let row_11 = expected[2]
        .iter()
        .find(|row| row[0] == "11")
        .unwrap_or_else(|| panic!("stock logs no id 11: {expected:?}"));
    assert_eq!(row_11[1], "2", "{expected:?}");
    let only_cte = stock_outcomes[5]
        .as_deref()
        .unwrap_or_else(|| panic!("stock accepts the caller-only CTE: {stock_outcomes:?}"));
    assert!(only_cte.contains("no such table"), "{only_cte}");
    asupersync::test_utils::run_test(|| async move {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let conn = Connection::open(&database_path(&dir, file_backed))
                .await
                .unwrap();
            for sql in SCOPE_SCHEMA {
                conn.execute(sql).await.unwrap();
            }
            for (sql, stock_error) in SCOPE_STEPS.iter().zip(&stock_outcomes) {
                match (conn.execute(sql).await, stock_error) {
                    (Ok(_), None) => {}
                    (Err(error), Some(_)) => assert!(
                        error.to_string().contains("no such table"),
                        "file_backed={file_backed}: `{sql}`: {error}"
                    ),
                    (outcome, stock_error) => panic!(
                        "file_backed={file_backed}: `{sql}`: fsqlite {outcome:?}, \
                         stock {stock_error:?}"
                    ),
                }
            }
            for (sql, want) in SCOPE_VERIFY.iter().zip(&expected) {
                assert_eq!(
                    &fsqlite_rows(&conn, sql).await,
                    want,
                    "file_backed={file_backed}: {sql}"
                );
            }
        }
    });
}

/// Runs every statement on both engines; a statement returning rows is a
/// checkpoint whose rows must be stock's.
async fn assert_script_matches_stock(script: &[&str], context: &str) {
    for file_backed in [false, true] {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open(&database_path(&dir, file_backed))
            .await
            .unwrap();
        for sql in script {
            if sql.trim_start().starts_with("SELECT") {
                assert_eq!(
                    fsqlite_rows(&conn, sql).await,
                    stock_rows(&stock, sql),
                    "{context}: file_backed={file_backed}: {sql}"
                );
            } else {
                stock.execute_batch(sql).unwrap();
                conn.execute(sql)
                    .await
                    .unwrap_or_else(|e| panic!("{context}: file_backed={file_backed}: {sql}: {e}"));
            }
        }
    }
}

#[test]
fn renamed_trigger_tables_and_columns_roll_back_with_the_transaction() {
    const SCRIPT: &[&str] = &[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
        "CREATE TABLE lg(tag, v)",
        "CREATE TRIGGER ti AFTER INSERT ON t BEGIN \
           INSERT INTO lg VALUES ('ti', (SELECT count(*) FROM t) * 100 + NEW.a); END",
        "INSERT INTO t VALUES (1, 1)",
        "BEGIN",
        "ALTER TABLE t RENAME TO t2",
        "INSERT INTO t2 VALUES (2, 2)",
        // The rename reached the trigger inside the transaction.
        "SELECT name, tbl_name FROM sqlite_master WHERE type = 'trigger'",
        "SELECT * FROM lg ORDER BY rowid",
        "ROLLBACK",
        // Rolled back: the trigger is on t again and reads t.
        "INSERT INTO t VALUES (3, 3)",
        "SAVEPOINT s",
        "ALTER TABLE t RENAME COLUMN a TO b",
        "INSERT INTO t VALUES (4, 4)",
        "ROLLBACK TO s",
        "RELEASE s",
        "INSERT INTO t VALUES (5, 5)",
        "BEGIN",
        "ALTER TABLE t RENAME COLUMN a TO c",
        "INSERT INTO t VALUES (6, 6)",
        "COMMIT",
        // Committed: the trigger now reads NEW.c.
        "INSERT INTO t VALUES (7, 7)",
        "SELECT * FROM lg ORDER BY rowid",
        "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'",
    ];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        assert_script_matches_stock(SCRIPT, "rename rollback").await;
    });
}

#[test]
fn recursive_triggers_off_stops_only_self_reentry() {
    const SCRIPT: &[&str] = &[
        "CREATE TABLE r(n INTEGER)",
        "CREATE TABLE rl(tag, n)",
        "CREATE TRIGGER r_self AFTER INSERT ON r WHEN NEW.n < 4 BEGIN \
           INSERT INTO rl VALUES ('self', NEW.n); INSERT INTO r VALUES (NEW.n + 1); END",
        "CREATE TRIGGER r_sib AFTER INSERT ON r BEGIN INSERT INTO rl VALUES ('sib', NEW.n); END",
        // OFF (the default): r_self's own INSERT does not fire r_self again,
        // but it does fire r_sib.
        "INSERT INTO r VALUES (1)",
        "SELECT * FROM rl ORDER BY rowid",
        "SELECT count(*) FROM r",
        "DELETE FROM rl",
        "DELETE FROM r",
        "PRAGMA recursive_triggers = ON",
        // ON: r_self recurses until its WHEN stops it.
        "INSERT INTO r VALUES (1)",
        "SELECT * FROM rl ORDER BY rowid",
        "SELECT * FROM r ORDER BY rowid",
    ];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        assert_script_matches_stock(SCRIPT, "recursive_triggers").await;
    });
}

/// Lever 3a binds a parameter-bound body statement once, and reuses it while
/// the trigger, the statement, the firing statement's conflict override, the
/// schema and the frame's shape stay the same. Each of those changes between
/// firings here, and every result is stock's.
/// - Multi-row firings under OR IGNORE and then OR REPLACE: the override goes
///   into the bound statement, so a template must not cross from one to the
///   other.
/// - A plain INSERT whose body collides fails as a whole, as in stock. The
///   `!` statements must fail on both engines.
/// - A new column (a new frame), a dropped and recreated trigger of the same
///   name (a new body), and a renamed column the body reads.
#[test]
fn bound_trigger_bodies_follow_conflict_overrides_and_schema_changes() {
    const SCRIPT: &[&str] = &[
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT)",
        "CREATE TABLE lg(k INTEGER PRIMARY KEY, v TEXT)",
        "CREATE TABLE src(id INTEGER PRIMARY KEY, a TEXT)",
        "INSERT INTO src VALUES (1, 'a'), (4, 'b'), (7, 'c'), (10, 'd'), (13, 'e')",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
           INSERT INTO lg VALUES (NEW.id % 3, NEW.a); \
           UPDATE lg SET v = v || '+' WHERE k = NEW.id % 3; END",
        "INSERT OR IGNORE INTO t SELECT id, a FROM src WHERE id < 8",
        "SELECT * FROM lg ORDER BY k",
        "DELETE FROM t",
        "INSERT OR REPLACE INTO t SELECT id, a FROM src",
        "SELECT * FROM lg ORDER BY k",
        "!INSERT INTO t VALUES (16, 'f')",
        "SELECT * FROM t ORDER BY id",
        "ALTER TABLE t ADD COLUMN b TEXT DEFAULT 'z'",
        "INSERT INTO t VALUES (17, 'g', 'h')",
        "DROP TRIGGER tr",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
           INSERT INTO lg VALUES (NEW.id + 100, NEW.b); END",
        "INSERT INTO t VALUES (18, 'i', 'j')",
        "INSERT INTO t (id, a) VALUES (19, 'k')",
        "ALTER TABLE t RENAME COLUMN b TO bb",
        "INSERT INTO t VALUES (20, 'l', 'm')",
        "SELECT * FROM lg ORDER BY k",
        "SELECT * FROM t ORDER BY id",
    ];
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let stock = rusqlite::Connection::open_in_memory().unwrap();
            let dir = tempfile::tempdir().unwrap();
            let conn = Connection::open(&database_path(&dir, file_backed))
                .await
                .unwrap();
            for sql in SCRIPT {
                if let Some(failing) = sql.strip_prefix('!') {
                    assert!(
                        stock.execute_batch(failing).is_err(),
                        "stock must refuse `{failing}`"
                    );
                    assert!(
                        conn.execute(failing).await.is_err(),
                        "file_backed={file_backed}: fsqlite must refuse `{failing}`"
                    );
                } else if sql.starts_with("SELECT") {
                    assert_eq!(
                        fsqlite_rows(&conn, sql).await,
                        stock_rows(&stock, sql),
                        "file_backed={file_backed}: {sql}"
                    );
                } else {
                    stock.execute_batch(sql).unwrap();
                    conn.execute(sql)
                        .await
                        .unwrap_or_else(|e| panic!("file_backed={file_backed}: {sql}: {e}"));
                }
            }
        }
    });
}

/// Lever 3b runs a parameter-bound body INSERT, UPDATE or DELETE through its
/// compiled program after the statement's first firing, without the
/// dispatcher's validation and rewriting, while the program is still the one
/// the dispatcher would run and the connection's state lets the shortcut
/// stand. Every result here is stock's, on `:memory:` and on a file; the `!`
/// statements must fail on both engines. The negatives are what a kernel that
/// cached its program and ran it unconditionally gets wrong:
/// - a trigger created on the UPDATE's target must fire on every later firing
///   (the UPDATE arm's tail does not fire triggers, so that target leaves the
///   kernel);
/// - foreign keys turned on, without a schema change, must be enforced on the
///   body's UPDATE of a child row;
/// - a body UPDATE that fails on one firing refuses the whole statement and
///   leaves no row behind, and the change counters stay stock's;
/// - the firing statement's OR IGNORE / OR REPLACE reaches the body, REPLACE
///   deleting the conflicting row;
/// - work done through the kernel inside a transaction or a savepoint rolls
///   back;
/// - a TEMP trigger's unqualified target becomes a new TEMP table.
const KERNEL_SCRIPT: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)",
    "CREATE TABLE lg(k, m)",
    "CREATE TABLE u(k TEXT PRIMARY KEY, c INTEGER)",
    "CREATE TABLE ulog(k, c)",
    "CREATE TABLE par(id INTEGER PRIMARY KEY)",
    "CREATE TABLE ch(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES par(id))",
    "INSERT INTO u VALUES ('even', 0), ('odd', 0)",
    "INSERT INTO par VALUES (1), (2)",
    "INSERT INTO ch VALUES (1, 1)",
    "CREATE TRIGGER ti AFTER INSERT ON t WHEN NEW.n % 7 <> 3 BEGIN \
       INSERT INTO lg VALUES (NEW.id, NEW.n > 10); \
       UPDATE u SET c = c + 1 WHERE k = CASE WHEN NEW.n % 2 = 0 THEN 'even' ELSE 'odd' END; \
       DELETE FROM lg WHERE k = NEW.id - 20; \
       UPDATE ch SET pid = CASE WHEN NEW.n % 2 = 0 THEN 2 ELSE 1 END WHERE id = 1; \
     END",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 60) \
     INSERT INTO t SELECT i, i FROM s",
    "SELECT changes(), total_changes()",
    "SELECT * FROM lg ORDER BY rowid",
    "SELECT * FROM u ORDER BY k",
    "SELECT * FROM ch",
    // A body UPDATE that fails on its 15th firing.
    "CREATE TABLE t2(id INTEGER PRIMARY KEY, n INTEGER)",
    "CREATE TRIGGER t2i AFTER INSERT ON t2 BEGIN \
       UPDATE u SET k = CASE WHEN NEW.n = 15 THEN 'odd' ELSE k END WHERE k = 'even'; \
       INSERT INTO lg VALUES (NEW.id + 1000, -NEW.n); \
     END",
    "!WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 30) \
     INSERT INTO t2 SELECT i, i FROM s",
    "SELECT count(*) FROM t2",
    "SELECT count(*), sum(m) FROM lg",
    "SELECT * FROM u ORDER BY k",
    "SELECT total_changes()",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 30) \
     INSERT OR IGNORE INTO t2 SELECT i, i FROM s",
    "SELECT changes(), total_changes()",
    "SELECT * FROM u ORDER BY k",
    "SELECT count(*), sum(m) FROM lg",
    "DELETE FROM t2",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 30) \
     INSERT OR REPLACE INTO t2 SELECT i, i FROM s",
    "SELECT changes(), total_changes()",
    "SELECT * FROM u ORDER BY k",
    "SELECT count(*), sum(m) FROM lg",
    "UPDATE u SET k = 'even', c = 0 WHERE k = 'odd'",
    "INSERT INTO u VALUES ('odd', 0)",
    // A trigger on the body UPDATE's target.
    "CREATE TRIGGER tu AFTER UPDATE ON u BEGIN INSERT INTO ulog VALUES (NEW.k, NEW.c); END",
    "WITH RECURSIVE s(i) AS (SELECT 61 UNION ALL SELECT i + 1 FROM s WHERE i < 80) \
     INSERT INTO t SELECT i, i FROM s",
    "SELECT * FROM ulog ORDER BY rowid",
    "SELECT * FROM u ORDER BY k",
    "SELECT * FROM ch",
    // Foreign keys turned on: the body's UPDATE of ch must find its parent.
    "DELETE FROM par WHERE id = 2",
    "PRAGMA foreign_keys = ON",
    "!INSERT INTO t VALUES (82, 82)",
    "SELECT count(*) FROM t",
    "SELECT * FROM ch",
    "SELECT count(*), sum(m) FROM lg",
    "INSERT INTO t VALUES (83, 83)",
    "SELECT * FROM ch",
    "PRAGMA foreign_keys = OFF",
    "INSERT INTO par VALUES (2)",
    // Rolled back with the transaction, and to a savepoint.
    "BEGIN",
    "WITH RECURSIVE s(i) AS (SELECT 100 UNION ALL SELECT i + 1 FROM s WHERE i < 140) \
     INSERT INTO t SELECT i, i FROM s",
    "SELECT count(*), sum(m) FROM lg",
    "ROLLBACK",
    "SELECT count(*), sum(m) FROM lg",
    "SELECT * FROM u ORDER BY k",
    "BEGIN",
    "INSERT INTO t VALUES (199, 199)",
    "SAVEPOINT a",
    "WITH RECURSIVE s(i) AS (SELECT 200 UNION ALL SELECT i + 1 FROM s WHERE i < 210) \
     INSERT INTO t SELECT i, i FROM s",
    "ROLLBACK TO a",
    "RELEASE a",
    "INSERT INTO t VALUES (211, 211)",
    "COMMIT",
    "SELECT * FROM lg ORDER BY rowid DESC LIMIT 4",
    "SELECT * FROM u ORDER BY k",
    "SELECT count(*) FROM ulog",
    // A TEMP trigger whose unqualified target becomes a TEMP table.
    "CREATE TEMP TRIGGER tt AFTER INSERT ON t BEGIN INSERT INTO lg VALUES (NEW.id, 'temp'); END",
    "WITH RECURSIVE s(i) AS (SELECT 300 UNION ALL SELECT i + 1 FROM s WHERE i < 310) \
     INSERT INTO t SELECT i, i FROM s",
    "CREATE TEMP TABLE lg(k, m)",
    "WITH RECURSIVE s(i) AS (SELECT 311 UNION ALL SELECT i + 1 FROM s WHERE i < 320) \
     INSERT INTO t SELECT i, i FROM s",
    "SELECT * FROM temp.lg ORDER BY rowid",
    "SELECT count(*) FROM main.lg WHERE m = 'temp'",
    "SELECT count(*), sum(k) FROM main.lg",
    "PRAGMA integrity_check",
];

/// Runs `script` on stock and on fsqlite (`:memory:`, then a file). A step
/// marked `!` must fail on both; a SELECT or `PRAGMA integrity_check` must
/// answer as stock does; every other step must succeed on both.
async fn assert_marked_script_matches_stock(script: &[&str], context: &str) {
    for file_backed in [false, true] {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open(&database_path(&dir, file_backed))
            .await
            .unwrap();
        for sql in script {
            if let Some(failing) = sql.strip_prefix('!') {
                assert!(
                    stock.execute_batch(failing).is_err(),
                    "{context}: stock must refuse `{failing}`"
                );
                assert!(
                    conn.execute(failing).await.is_err(),
                    "{context}: file_backed={file_backed}: fsqlite must refuse `{failing}`"
                );
            } else if sql.starts_with("SELECT") || sql.starts_with("PRAGMA integrity") {
                assert_eq!(
                    fsqlite_rows(&conn, sql).await,
                    stock_rows(&stock, sql),
                    "{context}: file_backed={file_backed}: {sql}"
                );
            } else {
                stock.execute_batch(sql).unwrap();
                conn.execute(sql).await.unwrap_or_else(|e| {
                    panic!("{context}: file_backed={file_backed}: {sql}: {e}")
                });
            }
        }
    }
}

#[test]
fn kernel_run_trigger_bodies_follow_targets_pragmas_failures_and_rollback() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        assert_marked_script_matches_stock(KERNEL_SCRIPT, "kernel").await;
    });
}

/// The engine keeps a retained cursor that a program on another root
/// displaces from its slot as a spare, and the next open of that root takes it
/// back (fsqlite-vdbe's lib test pins the mechanism). Every statement below
/// that fires or follows a trigger reopens cursors the bodies displaced. A
/// spare must never write over another cursor's work, so the steps write each
/// table the bodies write from outside the bodies, write the firing table
/// from its own trigger (recursive_triggers OFF and ON, an INSERT body and a
/// DELETE of the row before), drop and create a body's table again, index
/// it, and roll back to a savepoint and a failed statement between firings,
/// inside one transaction (where spares live) and in autocommit.
const SPARE_CURSOR_SCRIPT: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT, n INTEGER)",
    "CREATE TABLE lg(k, m)",
    "CREATE TABLE u(k TEXT PRIMARY KEY, c INTEGER)",
    "INSERT INTO u VALUES ('even', 0), ('odd', 0)",
    "CREATE TRIGGER ti AFTER INSERT ON t WHEN NEW.n % 7 <> 3 BEGIN \
       INSERT INTO lg VALUES (NEW.id, NEW.a = 'x' OR NEW.b = 'Y' OR NEW.n > 10); \
       UPDATE u SET c = c + 1 WHERE k = CASE WHEN NEW.n % 2 = 0 THEN 'even' ELSE 'odd' END; \
     END",
    "BEGIN",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 400) \
     INSERT INTO t SELECT i, 'x' || (i % 10), 'y' || (i % 5), i FROM s",
    // The bodies' tables written from outside them, between firings.
    "INSERT INTO lg VALUES (-1, -1)",
    "UPDATE u SET c = c + 1000 WHERE k = 'odd'",
    "INSERT INTO t VALUES (401, 'x1', 'y1', 401)",
    "DELETE FROM lg WHERE k % 50 = 0",
    "WITH RECURSIVE s(i) AS (SELECT 402 UNION ALL SELECT i + 1 FROM s WHERE i < 700) \
     INSERT INTO t SELECT i, 'x' || (i % 10), 'y' || (i % 5), i FROM s",
    "SELECT count(*), sum(k), sum(m) FROM lg",
    "SELECT * FROM u ORDER BY k",
    "SELECT count(*), sum(id) FROM t",
    // A body's table dropped and created again (its pages may serve the new
    // table), then indexed, between firings.
    "DROP TABLE lg",
    "CREATE TABLE lg(k, m)",
    "INSERT INTO t VALUES (701, 'x', 'Y', 701)",
    "CREATE INDEX lg_k ON lg(k)",
    "WITH RECURSIVE s(i) AS (SELECT 702 UNION ALL SELECT i + 1 FROM s WHERE i < 800) \
     INSERT INTO t SELECT i, 'x' || (i % 10), 'y' || (i % 5), i FROM s",
    "SELECT count(*), sum(k), sum(m) FROM lg",
    "SELECT k FROM lg WHERE k BETWEEN 750 AND 760 ORDER BY k",
    // Rolled back to a savepoint, and a statement failed in a body, between
    // firings.
    "SAVEPOINT a",
    "WITH RECURSIVE s(i) AS (SELECT 801 UNION ALL SELECT i + 1 FROM s WHERE i < 850) \
     INSERT INTO t SELECT i, 'x' || (i % 10), 'y' || (i % 5), i FROM s",
    "ROLLBACK TO a",
    "RELEASE a",
    "CREATE TABLE ck(v INTEGER CHECK (v <> 250))",
    "CREATE TRIGGER cki AFTER INSERT ON t WHEN NEW.id > 900 BEGIN \
       INSERT INTO ck VALUES (NEW.id - 700); END",
    "!WITH RECURSIVE s(i) AS (SELECT 901 UNION ALL SELECT i + 1 FROM s WHERE i < 1000) \
     INSERT INTO t SELECT i, 'x' || (i % 10), 'y' || (i % 5), i FROM s",
    "WITH RECURSIVE s(i) AS (SELECT 1001 UNION ALL SELECT i + 1 FROM s WHERE i < 1050) \
     INSERT INTO t SELECT i, 'x' || (i % 10), 'y' || (i % 5), i FROM s",
    "COMMIT",
    "SELECT count(*), sum(k), sum(m) FROM lg",
    "SELECT * FROM u ORDER BY k",
    "SELECT count(*), sum(id), max(id) FROM t",
    "SELECT count(*), sum(v) FROM ck",
    "PRAGMA integrity_check",
    // A body that writes the firing table: an INSERT of a row past the right
    // edge (the outer rows that follow are no longer appends) and a DELETE of
    // the row before.
    "CREATE TABLE s2(id INTEGER PRIMARY KEY, n INTEGER)",
    "CREATE TABLE s2log(id, n)",
    "CREATE TRIGGER s2i AFTER INSERT ON s2 WHEN NEW.n > 0 BEGIN \
       INSERT INTO s2 VALUES (NEW.id + 100000, NEW.n - 1); \
       INSERT INTO s2log VALUES (NEW.id, NEW.n); \
       DELETE FROM s2 WHERE id = NEW.id - 1 AND NEW.id % 5 = 0; \
     END",
    "BEGIN",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 300) \
     INSERT INTO s2 SELECT i, i % 4 FROM s",
    "SELECT count(*), sum(id), sum(n) FROM s2",
    "SELECT count(*), sum(id), sum(n) FROM s2log",
    "COMMIT",
    "PRAGMA recursive_triggers = ON",
    "BEGIN",
    "WITH RECURSIVE s(i) AS (SELECT 301 UNION ALL SELECT i + 1 FROM s WHERE i < 600) \
     INSERT INTO s2 SELECT i, i % 4 FROM s",
    "COMMIT",
    "SELECT count(*), sum(id), sum(n) FROM s2",
    "SELECT count(*), sum(id), sum(n) FROM s2log",
    "SELECT id, n FROM s2 WHERE id > 300000 ORDER BY id LIMIT 5",
    "PRAGMA recursive_triggers = OFF",
    "INSERT INTO s2 VALUES (601, 3)",
    "SELECT count(*), sum(id), sum(n) FROM s2",
    "CREATE INDEX s2_n ON s2(n)",
    "BEGIN",
    "WITH RECURSIVE s(i) AS (SELECT 602 UNION ALL SELECT i + 1 FROM s WHERE i < 700) \
     INSERT INTO s2 SELECT i, i % 4 FROM s",
    "COMMIT",
    "SELECT n, count(*), sum(id) FROM s2 GROUP BY n ORDER BY n",
    "SELECT count(*), sum(id), sum(n) FROM s2log",
    "PRAGMA integrity_check",
];

/// Lever 2a gives a column with no DEFAULT its NULL without the evaluator.
/// The NEW row a trigger sees (BEFORE and AFTER, from VALUES, a SELECT,
/// DEFAULT VALUES, and the foreign-key path) must still hold every omitted
/// column's DEFAULT: a literal, an empty string, an explicit NULL, a quoted
/// text, an expression, and a column with none.
const DEFAULT_ROW_SCRIPT: &[&str] = &[
    "CREATE TABLE p(id INTEGER PRIMARY KEY)",
    "INSERT INTO p VALUES (1), (7)",
    "CREATE TABLE d(id INTEGER PRIMARY KEY, none_ INTEGER, lit INTEGER DEFAULT 7, \
       empty TEXT DEFAULT '', nul DEFAULT NULL, q TEXT DEFAULT 'q''s', e INTEGER DEFAULT (3 * 4), \
       r INTEGER DEFAULT 7 REFERENCES p(id), last_none TEXT)",
    "CREATE TABLE dl(tag, id, none_, lit, empty, nul, q, e, r, last_none, types)",
    // (A BEFORE trigger's NEW.id for a rowid not yet chosen is not this
    // lever's: the WHEN leaves that row out.)
    "CREATE TRIGGER db BEFORE INSERT ON d WHEN NEW.id > 0 BEGIN INSERT INTO dl VALUES ('before', NEW.id, \
       NEW.none_, NEW.lit, NEW.empty, NEW.nul, NEW.q, NEW.e, NEW.r, NEW.last_none, \
       typeof(NEW.none_) || typeof(NEW.empty) || typeof(NEW.nul) || typeof(NEW.last_none)); END",
    "CREATE TRIGGER da AFTER INSERT ON d BEGIN INSERT INTO dl VALUES ('after', NEW.id, \
       NEW.none_, NEW.lit, NEW.empty, NEW.nul, NEW.q, NEW.e, NEW.r, NEW.last_none, \
       typeof(NEW.none_) || typeof(NEW.empty) || typeof(NEW.nul) || typeof(NEW.last_none)); END",
    "INSERT INTO d(id) VALUES (1), (2)",
    "INSERT INTO d(id, lit, last_none) VALUES (3, 30, 'x')",
    "INSERT INTO d(id, none_, nul) SELECT 10 + id, id, 'n' FROM p",
    "INSERT INTO d DEFAULT VALUES",
    "PRAGMA foreign_keys = ON",
    "INSERT INTO d(id, e) VALUES (20, 99)",
    "!INSERT INTO d(id, r) VALUES (21, 5)",
    "SELECT * FROM dl ORDER BY rowid",
    "SELECT * FROM d ORDER BY id",
];

#[test]
fn trigger_new_rows_hold_every_omitted_columns_default() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        assert_marked_script_matches_stock(DEFAULT_ROW_SCRIPT, "default row").await;
    });
}

/// Lever 5 replays a SELECT-fed stream whose rows were materialized, and a
/// stream while temp roots exist, through the compiled row program
/// (connection/insert_batch.rs's lib tests count which streams do). Each
/// replayed row must still do what the prepared replay did: across the
/// 10,000-row chunk boundary, with BEFORE triggers that RAISE(IGNORE) or
/// RAISE(ABORT), OR IGNORE and OR REPLACE, a trigger that writes the source
/// table (within one chunk), a CTE source with a TEMP table present, a body
/// that reads a view (a materialized binding while it runs), and a rollback.
const REPLAY_SCRIPT: &[&str] = &[
    "CREATE TABLE src(i INTEGER PRIMARY KEY, n INTEGER, tag TEXT)",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 10020) \
     INSERT INTO src SELECT i, i, 'g' || (i % 3) FROM s",
    "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, tag TEXT)",
    "CREATE TABLE lg(k, m)",
    "CREATE TRIGGER tb BEFORE INSERT ON t WHEN NEW.n % 1000 = 999 BEGIN \
       SELECT RAISE(IGNORE); END",
    "CREATE TRIGGER ti AFTER INSERT ON t WHEN NEW.n % 5 = 0 BEGIN \
       INSERT INTO lg VALUES (NEW.id, NEW.n * 2); END",
    "INSERT INTO t SELECT i, n + 0, tag FROM src",
    "SELECT changes(), total_changes()",
    "SELECT count(*), sum(id), sum(n) FROM t",
    "SELECT count(*), sum(k), sum(m) FROM lg",
    "INSERT OR IGNORE INTO t SELECT i, n + 1, 'x' FROM src WHERE i % 10 = 0",
    "SELECT changes(), total_changes()",
    "INSERT OR REPLACE INTO t SELECT i, n + 2, 'r' FROM src WHERE i % 7 = 0 AND i < 500",
    "SELECT changes(), total_changes()",
    "SELECT count(*), sum(n), sum(tag = 'r') FROM t",
    "SELECT count(*), sum(m) FROM lg",
    // A trigger that writes the source table, within one chunk.
    "CREATE TABLE t2(id INTEGER PRIMARY KEY, n INTEGER, tag TEXT)",
    "CREATE TRIGGER t2i AFTER INSERT ON t2 WHEN NEW.n % 5 = 0 BEGIN \
       UPDATE src SET tag = 'seen' WHERE i = NEW.id + 1; \
       INSERT INTO src VALUES (NEW.id + 100000, 0, 'new'); END",
    "INSERT INTO t2 SELECT i, n + 0, tag FROM src WHERE i <= 500",
    "SELECT count(*), sum(tag = 'seen') FROM t2",
    "SELECT count(*), sum(tag = 'seen'), sum(tag = 'new') FROM src",
    "CREATE TRIGGER t2a BEFORE INSERT ON t2 WHEN NEW.id = 777 BEGIN \
       SELECT RAISE(ABORT, 'stop at 777'); END",
    "!INSERT INTO t2 SELECT i, n + 0, tag FROM src WHERE i > 500 AND i <= 1000",
    "SELECT count(*), sum(n) FROM t2",
    "SELECT count(*), sum(tag = 'new') FROM src",
    // A CTE source while a TEMP table exists; the body reads a view.
    "CREATE TEMP TABLE scratch(x)",
    "INSERT INTO scratch VALUES (1)",
    "CREATE TABLE t3(id INTEGER PRIMARY KEY, n INTEGER)",
    "CREATE TABLE l3(id, c)",
    "CREATE VIEW v3 AS SELECT count(*) AS c, sum(n) AS s FROM t3",
    "CREATE TRIGGER t3i AFTER INSERT ON t3 BEGIN \
       INSERT INTO l3 SELECT NEW.id, c * 1000 + s FROM v3; END",
    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 400) \
     INSERT INTO t3 SELECT i, i FROM s",
    "SELECT count(*), sum(n) FROM t3",
    "SELECT count(*), sum(c) FROM l3",
    "SELECT * FROM scratch",
    // The rollback runs without the TEMP table: on main (eb7cd85b2, before
    // this lever) a ROLLBACK after CTE-fed INSERTs into a table with a trigger
    // empties a TEMP table, a separate defect reported by trig lane round 2.
    "DROP TABLE scratch",
    "BEGIN",
    "WITH RECURSIVE s(i) AS (SELECT 401 UNION ALL SELECT i + 1 FROM s WHERE i < 600) \
     INSERT INTO t3 SELECT i, i FROM s",
    "SELECT count(*), sum(c) FROM l3",
    "ROLLBACK",
    "SELECT count(*), sum(n) FROM t3",
    "SELECT count(*), sum(c) FROM l3",
    "PRAGMA integrity_check",
];

#[test]
fn replayed_select_rows_follow_chunks_raises_conflicts_and_temp_roots() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        assert_marked_script_matches_stock(REPLAY_SCRIPT, "replay").await;
    });
}

#[test]
fn spare_cursors_follow_self_writes_recursion_schema_changes_and_rollbacks() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        assert_marked_script_matches_stock(SPARE_CURSOR_SCRIPT, "spare cursors").await;
    });
}
