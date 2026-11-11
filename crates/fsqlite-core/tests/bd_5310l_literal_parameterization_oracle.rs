#![recursion_limit = "512"]

//! bd-5310l — a literal statement run through its parameterized form must be
//! indistinguishable from the same statement run as text.
//!
//! `Connection::query` rewrites a few literal statement shapes to numbered
//! parameters so that statements differing only in their literals share one
//! parse and one compiled program (`connection/literal_params.rs`). Everything
//! here is checked three ways:
//!
//! * `recognized`: the statement as an application writes it, which takes the
//!   parameterized path;
//! * `text`: the same statement with its table written `main.<table>`, which
//!   the recognizer leaves alone, so it takes the ordinary text path;
//! * `stock`: the first form on stock SQLite (rusqlite).
//!
//! The case a careless rewrite gets wrong is in the matrix on purpose: binding
//! `'123'` as the integer it looks like, or a sign-carrying literal as its
//! magnitude, stores a different type in the BLOB and untyped columns, and
//! the stored type and value show it. Values are compared as values (a REAL
//! by its bits), not through the engines' own number formatting.
//!
//! The last part proves the mechanism is engaged at all: 120 distinct literal
//! statements of two shapes parse a handful of times on the recognized form
//! and once each on the text form.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

const AFFINITY_TABLE: &str = "CREATE TABLE m(id INTEGER PRIMARY KEY, i INTEGER, r REAL, n NUMERIC, t TEXT, b BLOB, x)";
const AFFINITY_DUMP: &str = "SELECT id, typeof(i), i, typeof(r), r, typeof(n), n, typeof(t), t, typeof(b), b, typeof(x), x FROM m ORDER BY id";
const TWO_COLUMN_DUMP: &str = "SELECT typeof(a), a, typeof(b), b FROM s ORDER BY rowid";

/// Literals of every kind the recognizer binds, plus the ones it must refuse.
const LITERALS: &[&str] = &[
    "42",
    "-7",
    "+9",
    "0",
    "-0",
    "3.5",
    "-0.25",
    "5.0",
    "-5.0",
    "1e3",
    "-1.5e-3",
    ".5",
    "0x1F",
    "-0x10",
    "9223372036854775807",
    "-9223372036854775807",
    "007",
    "- 7",
    "1234.5678",
    "'abc'",
    "'123'",
    "'-7'",
    "'1.5'",
    "'1e3'",
    "' 12 '",
    "''",
    "'it''s'",
    "'semi;colon -- and /* comment */ openers'",
    "x'00ff'",
    "x''",
    "NULL",
    // Refused by the recognizer (as are the exponent, leading-dot, hex and
    // blob spellings above), so both fsqlite forms take the text path; they
    // stay in the matrix to pin that against stock as well.
    "9223372036854775808",
    "-9223372036854775808",
];

async fn open(path: &str) -> Connection {
    let conn = Connection::open(path).await.expect("open");
    conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
    conn
}

fn text_of(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(value) => value.to_string(),
        SqliteValue::Float(value) => format!("{value:?}"),
        SqliteValue::Text(value) => value.to_string(),
        SqliteValue::Blob(value) => format!("{value:?}"),
    }
}

/// Run one statement on fsqlite: rows as text lines, or the error text.
async fn run(conn: &Connection, sql: &str) -> Result<Vec<String>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(text_of)
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect()
        })
        .map_err(|error| error.to_string())
}

/// Run one statement on stock SQLite: rows as text lines, or the error text.
fn run_stock(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut statement = conn.prepare(sql).map_err(|error| error.to_string())?;
    let columns = statement.column_count();
    let mut rows = statement.query([]).map_err(|error| error.to_string())?;
    let mut out = Vec::new();
    loop {
        let row = match rows.next() {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(error) => return Err(error.to_string()),
        };
        let mut cells = Vec::with_capacity(columns);
        for index in 0..columns {
            let value: rusqlite::types::Value = row.get(index).map_err(|error| error.to_string())?;
            cells.push(match value {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(value) => value.to_string(),
                rusqlite::types::Value::Real(value) => format!("{value:?}"),
                rusqlite::types::Value::Text(value) => value,
                rusqlite::types::Value::Blob(value) => format!("{value:?}"),
            });
        }
        out.push(cells.join("|"));
    }
    Ok(out)
}

/// The statement as the recognizer sees it, and the same statement on the
/// ordinary text path (`main.`-qualified target).
fn forms(sql: &str, table: &str) -> (String, String) {
    let needle = format!(" {table}");
    let position = sql.find(&needle).expect("statement names its table");
    let mut text_form = sql.to_owned();
    text_form.insert_str(position + 1, "main.");
    (sql.to_owned(), text_form)
}

/// How closely a group of statements is held to stock. The parameterized and
/// text paths are always compared exactly with each other.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StockCheck {
    /// Rows, error text, `changes()` and `last_insert_rowid()` equal stock's.
    Exact,
    /// Success or failure and rows equal stock's, as do the final contents.
    /// For groups whose error wording or counters this change does not touch
    /// and whose parity with stock other suites own.
    Outcome,
}

/// Run `statements` on all three arms and compare every outcome, then the
/// result of `dump`.
async fn compare(
    recognized: &Connection,
    text: &Connection,
    stock: &rusqlite::Connection,
    table: &str,
    statements: &[String],
    dump: &str,
    stock_check: StockCheck,
) {
    for sql in statements {
        let (recognized_sql, text_sql) = forms(sql, table);
        let via_parameters = run(recognized, &recognized_sql).await;
        let via_text = run(text, &text_sql).await;
        let via_stock = run_stock(stock, &recognized_sql);
        assert_eq!(
            via_parameters, via_text,
            "parameterized and text paths differ for: {sql}"
        );
        match (&via_parameters, &via_stock) {
            (Ok(rows), Ok(stock_rows)) => {
                assert_eq!(rows, stock_rows, "rows differ from stock for: {sql}");
            }
            (Err(error), Err(stock_error)) => {
                if stock_check == StockCheck::Exact {
                    assert_eq!(error, stock_error, "error text differs from stock for: {sql}");
                }
            }
            (ours, theirs) => {
                panic!("outcome differs from stock for: {sql}\n fsqlite: {ours:?}\n stock: {theirs:?}")
            }
        }
        for probe in ["SELECT changes()", "SELECT last_insert_rowid()"] {
            let ours = run(recognized, probe).await;
            assert_eq!(ours, run(text, probe).await, "{probe} after: {sql}");
            if via_parameters.is_ok() && stock_check == StockCheck::Exact {
                assert_eq!(ours, run_stock(stock, probe), "{probe} vs stock after: {sql}");
            }
        }
    }
    let ours = run(recognized, dump).await;
    assert_eq!(ours, run(text, dump).await, "final contents: parameterized vs text");
    assert_eq!(ours, run_stock(stock, dump), "final contents: fsqlite vs stock");
}

#[test]
fn literal_statements_run_identically_parameterized_as_text_and_on_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path_of = |name: &str| {
            dir.path()
                .join(name)
                .to_str()
                .expect("utf-8 path")
                .to_owned()
        };
        let recognized = open(&path_of("recognized.db")).await;
        let text = open(&path_of("text.db")).await;
        let stock = rusqlite::Connection::open_in_memory().expect("stock open");

        // --- every literal kind into every column affinity -----------------
        for conn in [&recognized, &text] {
            conn.execute(AFFINITY_TABLE).await.expect("ddl");
        }
        stock.execute_batch(AFFINITY_TABLE).expect("stock ddl");
        let inserts: Vec<String> = LITERALS
            .iter()
            .map(|literal| {
                format!(
                    "INSERT INTO m(i, r, n, t, b, x) VALUES ({literal}, {literal}, {literal}, {literal}, {literal}, {literal})"
                )
            })
            .collect();
        compare(&recognized, &text, &stock, "m", &inserts, AFFINITY_DUMP, StockCheck::Exact).await;

        // --- an explicit rowid, with and without a column list --------------
        let keyed = [
            "INSERT INTO m VALUES (1000, 1, 2.0, 3, 'four', x'05', 6)",
            "INSERT INTO m(id, t) VALUES (-5, 'negative key')",
            "INSERT INTO m(t, id) VALUES ('listed after', 2000)",
        ]
        .map(str::to_owned);
        compare(&recognized, &text, &stock, "m", &keyed, AFFINITY_DUMP, StockCheck::Exact).await;

        // --- rowid lookups, updates and deletes ----------------------------
        let by_key = [
            "SELECT t, typeof(t) FROM m WHERE id = 1000",
            "SELECT i, r, n, t, b, x FROM m WHERE id = 1000",
            "SELECT * FROM m WHERE id = -5",
            "SELECT t FROM m WHERE id = 424242",
            "SELECT t FROM m WHERE id = +2000",
            "UPDATE m SET i = i + 1, t = 'updated', x = NULL WHERE id = 1000",
            "UPDATE m SET r = r - 0.5, n = i WHERE id = 1000",
            "UPDATE m SET t = '123', b = '123', x = -7 WHERE id = 2000",
            "UPDATE m SET t = 'no such row' WHERE id = 424242",
            "DELETE FROM m WHERE id = -5",
            "DELETE FROM m WHERE id = 424242",
            "SELECT t FROM m WHERE id = -5",
        ]
        .map(str::to_owned);
        compare(&recognized, &text, &stock, "m", &by_key, AFFINITY_DUMP, StockCheck::Exact).await;

        // --- statements that fail -------------------------------------------
        let constrained = "CREATE TABLE c(id INTEGER PRIMARY KEY, u TEXT UNIQUE, nn INTEGER NOT NULL, k INTEGER CHECK (k < 10))";
        for conn in [&recognized, &text] {
            conn.execute(constrained).await.expect("ddl");
        }
        stock.execute_batch(constrained).expect("stock ddl");
        let failing = [
            "INSERT INTO c VALUES (1, 'a', 1, 1)",
            "INSERT INTO c VALUES (1, 'b', 1, 1)",
            "INSERT INTO c VALUES (2, 'a', 1, 1)",
            "INSERT INTO c(id, u) VALUES (3, 'c')",
            "INSERT INTO c VALUES (4, 'd', 1, 99)",
            "INSERT INTO c VALUES ('not a rowid', 'e', 1, 1)",
            "INSERT INTO c VALUES (5, 'f', 1)",
            "INSERT INTO c(id, nosuch) VALUES (6, 'g')",
            "INSERT INTO nosuch VALUES (1)",
            "UPDATE c SET nosuch = 1 WHERE id = 1",
            "UPDATE c SET u = 'a', k = 5 WHERE id = 1",
            "UPDATE c SET k = 99 WHERE id = 1",
            "SELECT nosuch FROM c WHERE id = 1",
            "SELECT u FROM nosuch WHERE id = 1",
            "DELETE FROM nosuch WHERE id = 1",
            "INSERT INTO c VALUES (7, 'h', 2, 3)",
        ]
        .map(str::to_owned);
        // The parameterized and text paths must report one error text; whether
        // that text equals stock's word for word is older than this change, so
        // only the outcome (error or not) and the final contents are compared.
        compare(
            &recognized,
            &text,
            &stock,
            "c",
            &failing[..8],
            "SELECT id, u, nn, k FROM c ORDER BY id",
            StockCheck::Outcome,
        )
        .await;
        for sql in &failing[8..] {
            // These name a table that does not exist, or are covered above;
            // `forms` needs the table name, so they run unqualified on both.
            let ours = run(&recognized, sql).await;
            assert_eq!(ours.is_ok(), run_stock(&stock, sql).is_ok(), "outcome vs stock: {sql}");
            let _ = run(&text, sql).await;
        }
        assert_eq!(
            run(&recognized, "SELECT id, u, nn, k FROM c ORDER BY id").await,
            run_stock(&stock, "SELECT id, u, nn, k FROM c ORDER BY id"),
            "contents after the failing statements"
        );

        // --- a schema change between two uses of one statement shape ---------
        for conn in [&recognized, &text] {
            conn.execute("CREATE TABLE s(a INTEGER, b TEXT)").await.expect("ddl");
        }
        stock.execute_batch("CREATE TABLE s(a INTEGER, b TEXT)").expect("stock ddl");
        let before = ["INSERT INTO s VALUES (1, '10')", "INSERT INTO s VALUES ('2', 20)"].map(str::to_owned);
        compare(
            &recognized,
            &text,
            &stock,
            "s",
            &before,
            TWO_COLUMN_DUMP,
            StockCheck::Exact,
        )
        .await;
        // Same statement texts, same arity, the two affinities swapped: a
        // program kept from the first table would store the old types.
        let swap = "DROP TABLE s; CREATE TABLE s(a TEXT, b INTEGER)";
        for conn in [&recognized, &text] {
            conn.execute_batch(swap).await.expect("schema change");
        }
        stock.execute_batch(swap).expect("stock schema change");
        compare(
            &recognized,
            &text,
            &stock,
            "s",
            &before,
            TWO_COLUMN_DUMP,
            StockCheck::Exact,
        )
        .await;
        assert_eq!(
            run(&recognized, "SELECT typeof(a), typeof(b) FROM s ORDER BY rowid").await,
            Ok(vec!["text|integer".to_owned(), "text|integer".to_owned()]),
            "the second table's affinities apply to the reused statement text"
        );

        // --- tables with behaviour attached -----------------------------------
        // Triggers, AUTOINCREMENT, defaults, a generated column, STRICT typing,
        // WITHOUT ROWID and foreign keys all read the inserted or assigned
        // values; a bound value must reach each of them as the literal did.
        let attached = "PRAGMA foreign_keys = ON; \
            CREATE TABLE p(id INTEGER PRIMARY KEY AUTOINCREMENT, a INTEGER DEFAULT 7, \
                b TEXT NOT NULL DEFAULT 'd', g INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL); \
            CREATE TABLE log(what TEXT, old_a, new_a, new_b); \
            CREATE TRIGGER p_ai AFTER INSERT ON p BEGIN \
                INSERT INTO log VALUES ('ins', NULL, new.a, new.b); END; \
            CREATE TRIGGER p_au AFTER UPDATE ON p BEGIN \
                INSERT INTO log VALUES ('upd', old.a, new.a, new.b); END; \
            CREATE TRIGGER p_bd BEFORE DELETE ON p BEGIN \
                INSERT INTO log VALUES ('del', old.a, NULL, old.b); END; \
            CREATE TABLE st(id INTEGER PRIMARY KEY, n INTEGER, t TEXT) STRICT; \
            CREATE TABLE wr(k INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID; \
            CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))";
        for conn in [&recognized, &text] {
            conn.execute_batch(attached).await.expect("ddl");
        }
        stock.execute_batch(attached).expect("stock ddl");
        let on_p = [
            "INSERT INTO p(a) VALUES (5)",
            "INSERT INTO p(b) VALUES ('x')",
            "INSERT INTO p(id, a, b) VALUES (10, 1, 'ten')",
            "INSERT INTO p(a, b) VALUES (NULL, 'n')",
            "INSERT INTO p(a, b) VALUES ('12', 34)",
            "UPDATE p SET a = a + 10 WHERE id = 1",
            "UPDATE p SET b = 'z', a = 3 WHERE id = 10",
            "SELECT a, b, g FROM p WHERE id = 10",
            "UPDATE p SET b = NULL WHERE id = 10",
            "DELETE FROM p WHERE id = 2",
            "INSERT INTO p(a) VALUES (6)",
        ]
        .map(str::to_owned);
        compare(
            &recognized,
            &text,
            &stock,
            "p",
            &on_p,
            "SELECT id, a, b, g FROM p ORDER BY id",
            StockCheck::Outcome,
        )
        .await;
        let on_strict = [
            "INSERT INTO st VALUES (1, 5, 'five')",
            "INSERT INTO st VALUES (2, '6', 7)",
            "INSERT INTO st VALUES (3, 'x', 't')",
            "INSERT INTO st VALUES (4, 1.5, 't')",
            "UPDATE st SET n = 'y' WHERE id = 1",
            "UPDATE st SET n = '8', t = 9 WHERE id = 1",
            "SELECT n, t FROM st WHERE id = 1",
        ]
        .map(str::to_owned);
        compare(
            &recognized,
            &text,
            &stock,
            "st",
            &on_strict,
            "SELECT id, typeof(n), n, typeof(t), t FROM st ORDER BY id",
            StockCheck::Outcome,
        )
        .await;
        let on_without_rowid = [
            "INSERT INTO wr VALUES (1, 'one')",
            "INSERT INTO wr VALUES (1, 'again')",
            "INSERT INTO wr(v, k) VALUES ('two', 2)",
            "SELECT v FROM wr WHERE k = 1",
            "UPDATE wr SET v = 'uno' WHERE k = 1",
            "DELETE FROM wr WHERE k = 2",
        ]
        .map(str::to_owned);
        compare(
            &recognized,
            &text,
            &stock,
            "wr",
            &on_without_rowid,
            "SELECT k, v FROM wr ORDER BY k",
            StockCheck::Outcome,
        )
        .await;
        let on_child = [
            "INSERT INTO child VALUES (1, 10)",
            "INSERT INTO child VALUES (2, 999)",
            // An explicit rowid: on a file database the automatic rowid after
            // a failed explicit-rowid INSERT differs from stock on main
            // already (3 where stock gives 2), on the text path too. That is
            // bd-failed-insert-rowid-floor-l53w4; when it closes, this row can
            // go back to `INSERT INTO child(pid) VALUES (1)`.
            "INSERT INTO child VALUES (3, 1)",
            "UPDATE child SET pid = 998 WHERE id = 1",
            "UPDATE child SET pid = 11 WHERE id = 1",
        ]
        .map(str::to_owned);
        compare(
            &recognized,
            &text,
            &stock,
            "child",
            &on_child,
            "SELECT id, pid FROM child ORDER BY id",
            StockCheck::Outcome,
        )
        .await;
        // The parent rows the children point at cannot go; the others can.
        let parent_deletes = [
            "DELETE FROM p WHERE id = 3",
            "DELETE FROM p WHERE id = 1",
            "DELETE FROM p WHERE id = 10",
        ]
        .map(str::to_owned);
        compare(
            &recognized,
            &text,
            &stock,
            "p",
            &parent_deletes,
            "SELECT id, a, b, g FROM p ORDER BY id",
            StockCheck::Outcome,
        )
        .await;
        for dump in [
            "SELECT what, old_a, new_a, new_b FROM log ORDER BY rowid",
            "SELECT name, seq FROM sqlite_sequence ORDER BY name",
        ] {
            let ours = run(&recognized, dump).await;
            assert_eq!(ours, run(&text, dump).await, "{dump}: parameterized vs text");
            assert_eq!(ours, run_stock(&stock, dump), "{dump}: fsqlite vs stock");
        }

        // --- a key that is not the rowid alias is left to the text path -----
        for conn in [&recognized, &text] {
            conn.execute("CREATE TABLE w(k INTEGER, v TEXT)").await.expect("ddl");
            conn.execute("CREATE INDEX w_k ON w(k)").await.expect("index");
        }
        stock
            .execute_batch("CREATE TABLE w(k INTEGER, v TEXT); CREATE INDEX w_k ON w(k)")
            .expect("stock ddl");
        let non_key = [
            "INSERT INTO w VALUES (1, 'a')",
            "INSERT INTO w VALUES (1, 'b')",
            "INSERT INTO w VALUES (2, 'c')",
            "SELECT v FROM w WHERE k = 1",
            "UPDATE w SET v = 'z' WHERE k = 1",
            "DELETE FROM w WHERE k = 2",
        ]
        .map(str::to_owned);
        compare(
            &recognized,
            &text,
            &stock,
            "w",
            &non_key,
            "SELECT k, v FROM w ORDER BY rowid",
            StockCheck::Exact,
        )
        .await;

        // --- the mechanism is engaged ----------------------------------------
        // Counters are process-global; this file has one test.
        for conn in [&recognized, &text] {
            conn.execute("CREATE TABLE e(id INTEGER PRIMARY KEY, w INTEGER, s TEXT)")
                .await
                .expect("ddl");
        }
        set_hot_path_profile_enabled(true);
        let mut misses = Vec::new();
        for (conn, target) in [(&recognized, "e"), (&text, "main.e")] {
            reset_hot_path_profile();
            for row in 0..60 {
                conn.query(&format!("INSERT INTO {target}(w, s) VALUES ({row}, 'payload-{row}')"))
                    .await
                    .expect("insert");
            }
            for row in 1..=60 {
                conn.query(&format!("SELECT w, s FROM {target} WHERE id = {row}"))
                    .await
                    .expect("select");
            }
            let parser = hot_path_profile_snapshot().parser;
            misses.push((parser.parse_cache_misses, parser.compiled_cache_misses));
        }
        set_hot_path_profile_enabled(false);
        let (recognized_misses, text_misses) = (misses[0], misses[1]);
        assert!(
            recognized_misses.0 <= 4 && recognized_misses.1 <= 4,
            "120 literal statements of two shapes parse and compile a handful of times, got {recognized_misses:?}"
        );
        assert!(
            text_misses.0 >= 120,
            "the text path parses each distinct statement, got {text_misses:?}"
        );
        assert_eq!(
            run(&recognized, "SELECT count(*), sum(w), sum(length(s)) FROM e").await,
            run(&text, "SELECT count(*), sum(w), sum(length(s)) FROM e").await,
        );
    });
}
