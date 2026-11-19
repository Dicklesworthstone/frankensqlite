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
