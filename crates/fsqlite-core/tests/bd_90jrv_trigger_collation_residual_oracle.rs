#![recursion_limit = "512"]
#![allow(
    clippy::significant_drop_tightening,
    reason = "the process-global VDBE opcode counter needs each test serialized"
)]
//! bd-90jrv residual: a trigger's NEW/OLD reference compares under its
//! column's collation as stock 3.54.1 gives it, a declared (not explicit)
//! collation.
//!
//! - A BINARY column's reference has the BINARY collation, so on the
//!   deciding side it beats a NOCASE column opposite: `NEW.b = u.w`
//!   compares BINARY. The binder turned it into a value with no collation,
//!   so u.w's NOCASE decided.
//! - Over a view, NEW.x carries the view column's collation (INSTEAD OF
//!   triggers had none).
//! - A compound SELECT sorts by the leftmost arm whose column has a
//!   collation, a bound NEW.a included.
//!
//! Every answer is compared with bundled SQLite 3.54.1 on `:memory:` and on
//! a file, with mixed-case data so the collation decides. The second test
//! pins the index seeks a naive "always wrap BINARY" fix would lose:
//! `NEW.k = big.k` and `big.k = NEW.k` in a SELECT, an UPDATE and a DELETE
//! trigger body each seek a BINARY index on `ROWS` rows, counted in executed
//! opcodes on a file-backed database.

use std::sync::{Mutex, MutexGuard};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::engine::{reset_vdbe_metrics, set_vdbe_metrics_enabled, vdbe_metrics_snapshot};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f:?}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f:?}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:02x?}"),
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).expect("stock prepares");
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock runs")
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect()
}

const SETUP: &str = "
    CREATE TABLE t(a TEXT, b TEXT);
    CREATE TABLE tn(a TEXT COLLATE NOCASE, b TEXT);
    CREATE TABLE u(w TEXT COLLATE NOCASE, k TEXT);
    CREATE INDEX uw ON u(w);
    CREATE INDEX uk ON u(k);
    INSERT INTO u VALUES ('abc', 'abc'), ('ABC', 'ABC'), ('xyz', 'xyz');
    CREATE TABLE dd(w TEXT COLLATE NOCASE);
    INSERT INTO dd VALUES ('abc'), ('ABC'), ('xyz');
    CREATE TABLE lg(tag, x);
    CREATE TRIGGER t_ins AFTER INSERT ON t BEGIN
      INSERT INTO lg SELECT 'left', count(*) FROM u WHERE NEW.b = u.w;
      INSERT INTO lg SELECT 'right', count(*) FROM u WHERE u.w = NEW.b;
      INSERT INTO lg SELECT 'left-binary-col', count(*) FROM u WHERE NEW.b = u.k;
      INSERT INTO lg SELECT 'between', count(*) FROM u WHERE NEW.b BETWEEN u.w AND u.w;
      INSERT INTO lg VALUES ('in-sub', NEW.b IN (SELECT w FROM u WHERE k = 'ABC'));
      INSERT INTO lg VALUES ('eq-sub', NEW.b = (SELECT w FROM u WHERE k = 'ABC'));
      INSERT INTO lg VALUES ('case', CASE NEW.b WHEN (SELECT w FROM u WHERE k = 'ABC')
                                     THEN 1 ELSE 0 END);
      INSERT INTO lg VALUES ('nullif', nullif(NEW.b, (SELECT w FROM u WHERE k = 'ABC')));
      INSERT INTO lg VALUES ('rowid-left', NEW.rowid = (SELECT w FROM u WHERE k = 'ABC'));
      UPDATE u SET k = k WHERE NEW.b = w;
      INSERT INTO lg VALUES ('update-left', changes());
      DELETE FROM dd WHERE NEW.b = w;
      INSERT INTO lg VALUES ('delete-left', changes());
    END;
    CREATE TRIGGER t_upd AFTER UPDATE ON t BEGIN
      INSERT INTO lg SELECT 'old-left', count(*) FROM u WHERE OLD.b = u.w;
      INSERT INTO lg SELECT 'old-right', count(*) FROM u WHERE u.w = OLD.b;
    END;
    CREATE TRIGGER tn_ins AFTER INSERT ON tn BEGIN
      INSERT INTO lg SELECT 'nocase-left', count(*) FROM u WHERE NEW.a = u.k;
      INSERT INTO lg SELECT 'nocase-right', count(*) FROM u WHERE u.k = NEW.a;
      INSERT INTO lg SELECT 'binary-left-of-binary', count(*) FROM u WHERE NEW.b = u.k;
      INSERT INTO lg SELECT 'compound', group_concat(x, ',')
        FROM (SELECT 'B' AS x UNION ALL SELECT NEW.a ORDER BY 1);
      INSERT INTO lg SELECT 'compound-distinct', count(*)
        FROM (SELECT 'ABC' AS x UNION SELECT NEW.a);
    END;
    CREATE TABLE s(a TEXT COLLATE NOCASE, n INTEGER);
    CREATE VIEW sv AS SELECT a, n FROM s;
    CREATE TRIGGER sv_ins INSTEAD OF INSERT ON sv BEGIN
      INSERT INTO lg VALUES ('view-eq', NEW.a = 'ABC');
      INSERT INTO lg VALUES ('view-in', NEW.a IN ('ABC'));
      INSERT INTO lg VALUES ('view-eq-rhs', 'ABC' = NEW.a);
    END;
";

const STATEMENTS: &[&str] = &[
    "INSERT INTO t VALUES ('x', 'abc')",
    "UPDATE t SET a = 'y'",
    "INSERT INTO tn VALUES ('abc', 'abc')",
    "INSERT INTO sv VALUES ('abc', 1)",
];

const CHECKS: &[&str] = &[
    "SELECT tag, x FROM lg ORDER BY rowid",
    "SELECT w FROM dd ORDER BY rowid",
    "SELECT w, k FROM u ORDER BY rowid",
];

#[test]
fn trigger_references_compare_under_stocks_collation() {
    let _serial = serial();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock.execute_batch(SETUP).unwrap();
    for statement in STATEMENTS {
        stock.execute_batch(statement).unwrap();
    }
    let expected: Vec<_> = CHECKS.iter().map(|sql| stock_rows(&stock, sql)).collect();
    // Not vacuous: BINARY and NOCASE give different counts on this data.
    let tag = |name: &str| -> Vec<String> {
        expected[0]
            .iter()
            .find(|row| row[0] == format!("text:{name}"))
            .cloned()
            .unwrap_or_else(|| panic!("no `{name}` row: {:?}", expected[0]))
    };
    assert_eq!(tag("left")[1], "int:1");
    assert_eq!(tag("right")[1], "int:2");
    assert_eq!(tag("update-left")[1], "int:1");
    assert_eq!(tag("delete-left")[1], "int:1");
    assert_eq!(tag("view-eq")[1], "int:1");
    assert_eq!(tag("compound")[1], "text:abc,B");
    asupersync::test_utils::run_test(|| async move {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path().join("collate.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let conn = Connection::open(&path).await.unwrap();
            conn.execute_batch(SETUP).await.unwrap();
            for statement in STATEMENTS {
                conn.execute(statement)
                    .await
                    .unwrap_or_else(|e| panic!("fsqlite `{statement}`: {e}"));
            }
            for (sql, want) in CHECKS.iter().zip(&expected) {
                let got = fsqlite_rows(&conn, sql).await;
                assert_eq!(&got, want, "file_backed={file_backed}: {sql}");
            }
            conn.close().await.unwrap();
        }
    });
}

const ROWS: u64 = 2000;

/// A seek visits a few rows (tens of opcodes); a scan of `ROWS` rows runs
/// well over ten thousand.
const SEEK_LIMIT: u64 = 2000;

fn seek_setup() -> String {
    let mut sql = format!(
        "CREATE TABLE big(k TEXT, v INTEGER);
         CREATE INDEX bigk ON big(k);
         INSERT INTO big
           WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < {ROWS})
           SELECT CASE i % 2 WHEN 0 THEN 'k' ELSE 'K' END || i, 0 FROM s;
         CREATE TABLE sink(n);"
    );
    for (name, body) in [
        (
            "sel_l",
            "INSERT INTO sink SELECT count(*) FROM big WHERE NEW.k = big.k",
        ),
        (
            "sel_r",
            "INSERT INTO sink SELECT count(*) FROM big WHERE big.k = NEW.k",
        ),
        ("upd_l", "UPDATE big SET v = v + 1 WHERE NEW.k = k"),
        ("upd_r", "UPDATE big SET v = v + 1 WHERE k = NEW.k"),
        ("del_l", "DELETE FROM big WHERE NEW.k = k AND v = 99"),
        ("del_r", "DELETE FROM big WHERE k = NEW.k AND v = 99"),
    ] {
        sql.push_str(&format!(
            "CREATE TABLE {name}(k TEXT);
             CREATE TRIGGER {name}_t AFTER INSERT ON {name} BEGIN {body}; END;"
        ));
    }
    sql
}

const SEEK_STATEMENTS: &[&str] = &[
    "INSERT INTO sel_l VALUES ('k18')",
    "INSERT INTO sel_r VALUES ('k18')",
    "INSERT INTO upd_l VALUES ('k18')",
    "INSERT INTO upd_r VALUES ('K19')",
    "INSERT INTO del_l VALUES ('k18')",
    "INSERT INTO del_r VALUES ('K19')",
];

#[test]
fn binary_trigger_probes_keep_their_index_seek() {
    let _serial = serial();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    stock.execute_batch(&seek_setup()).unwrap();
    for statement in SEEK_STATEMENTS {
        stock.execute_batch(statement).unwrap();
    }
    let checks = [
        "SELECT n FROM sink ORDER BY rowid",
        "SELECT k, v FROM big WHERE v <> 0 ORDER BY k",
    ];
    let expected: Vec<_> = checks.iter().map(|sql| stock_rows(&stock, sql)).collect();
    assert_eq!(expected[0].len(), 2, "{:?}", expected[0]);
    assert_eq!(expected[1].len(), 2, "{:?}", expected[1]);
    asupersync::test_utils::run_test(|| async move {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path().join("seek.db").to_str().unwrap().to_owned()
            } else {
                ":memory:".to_owned()
            };
            let conn = Connection::open(&path).await.unwrap();
            conn.execute_batch(&seek_setup()).await.unwrap();
            let mut report = Vec::new();
            let mut wrong = Vec::new();
            for statement in SEEK_STATEMENTS {
                set_vdbe_metrics_enabled(true);
                reset_vdbe_metrics();
                conn.execute(statement)
                    .await
                    .unwrap_or_else(|e| panic!("fsqlite `{statement}`: {e}"));
                let metrics = vdbe_metrics_snapshot();
                set_vdbe_metrics_enabled(false);
                let opcodes = metrics.opcodes_executed_total;
                let seeks: u64 = metrics
                    .opcode_execution_totals
                    .iter()
                    .filter(|entry| {
                        matches!(
                            entry.opcode.as_str(),
                            "SeekGE" | "SeekGT" | "SeekLE" | "SeekLT"
                        )
                    })
                    .map(|entry| entry.total)
                    .sum();
                report.push(format!("  {opcodes:>8} opcodes, {seeks} seeks: `{statement}`"));
                if file_backed && (opcodes > SEEK_LIMIT || seeks == 0) {
                    wrong.push(format!("  {opcodes} opcodes, {seeks} seeks: `{statement}`"));
                }
            }
            for (sql, want) in checks.iter().zip(&expected) {
                let got = fsqlite_rows(&conn, sql).await;
                assert_eq!(&got, want, "file_backed={file_backed}: {sql}");
            }
            conn.close().await.unwrap();
            eprintln!(
                "bd-90jrv: file_backed={file_backed} (seek limit {SEEK_LIMIT} opcodes):\n{}",
                report.join("\n")
            );
            assert!(
                wrong.is_empty(),
                "a trigger body's BINARY probe must seek its index:\n{}",
                wrong.join("\n")
            );
        }
    });
}
