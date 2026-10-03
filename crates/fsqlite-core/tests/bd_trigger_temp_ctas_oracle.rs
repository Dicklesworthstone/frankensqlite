#![recursion_limit = "512"]

//! Trigger / CREATE TABLE AS parity gaps, pinned against stock SQLite
//! (rusqlite), in memory and file-backed:
//!
//! - bd-y26jy: `CREATE TEMP TABLE x AS SELECT ...` (and `temp.x`) creates the
//!   table in the temp schema, so `CREATE INDEX temp.i ON x(...)` works and
//!   `temp.sqlite_master` lists it. fsqlite used to create a main table.
//! - bd-oh34i: trigger NEW values carry the column's affinity, in BEFORE and
//!   AFTER INSERT/UPDATE triggers and their WHEN clauses. fsqlite used to show
//!   `INSERT ... VALUES('1')` into an INTEGER column as the text '1', and a
//!   `WHEN NEW.k = 1` trigger never fired.
//! - bd-5pt42: a multi-row DELETE runs each row's BEFORE trigger, delete and
//!   AFTER trigger before the next row's, so trigger bodies observe the rows
//!   already deleted. fsqlite used to fire every BEFORE trigger, then delete
//!   every row, then fire every AFTER trigger.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use rusqlite::types::Value;

fn value(value: Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Integer(value) => SqliteValue::Integer(value),
        Value::Real(value) => SqliteValue::Float(value),
        Value::Text(value) => SqliteValue::from(value.as_str()),
        Value::Blob(value) => SqliteValue::Blob(value.into()),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("FrankenSQLite: `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("SQLite: `{sql}`: {e}"));
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| row.get::<_, Value>(i).map(value))
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

async fn run_both(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    frank
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("FrankenSQLite: `{sql}`: {e:?}"));
    stock
        .execute_batch(sql)
        .unwrap_or_else(|e| panic!("SQLite: `{sql}`: {e}"));
}

async fn compare(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    assert_eq!(
        frank_rows(frank, sql).await,
        stock_rows(stock, sql),
        "`{sql}` differs from SQLite"
    );
}

/// Runs `body` once against in-memory databases and once against fresh files.
fn for_each_backing<F, Fut>(body: F)
where
    F: Fn(Connection, rusqlite::Connection) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        body(frank, stock).await;

        let dir = tempfile::tempdir().unwrap();
        let frank_path = dir.path().join("frank.db");
        let stock_path = dir.path().join("stock.db");
        let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        body(frank, stock).await;
    });
}

#[test]
fn create_temp_table_as_select_lives_in_temp_schema() {
    for_each_backing(|frank, stock| async move {
        for sql in [
            "CREATE TABLE src(a INTEGER, b TEXT)",
            "INSERT INTO src VALUES (1, 'x'), (2, 'y'), (3, 'z')",
            "CREATE TEMP TABLE x AS SELECT * FROM src",
            "CREATE INDEX temp.ix ON x(a)",
            "CREATE TABLE temp.y AS SELECT a * 10 AS m FROM src WHERE a > 1",
            "CREATE TEMP TABLE IF NOT EXISTS x AS SELECT 99",
            // A main table may share the name of a temp table.
            "CREATE TABLE x AS SELECT b FROM src WHERE a = 1",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        for sql in [
            "SELECT type, name, tbl_name FROM temp.sqlite_master ORDER BY name",
            "SELECT type, name, tbl_name FROM main.sqlite_master ORDER BY name",
            "SELECT * FROM x ORDER BY 1",
            "SELECT * FROM temp.x ORDER BY a",
            "SELECT * FROM main.x",
            "SELECT * FROM y ORDER BY m",
            "SELECT typeof(a), typeof(b) FROM temp.x ORDER BY a",
        ] {
            compare(&frank, &stock, sql).await;
        }
        let duplicate = "CREATE TEMP TABLE x AS SELECT 1";
        assert!(frank.execute(duplicate).await.is_err());
        assert!(stock.execute_batch(duplicate).is_err());
    });
}

#[test]
fn trigger_new_values_carry_column_affinity() {
    for_each_backing(|frank, stock| async move {
        for sql in [
            "CREATE TABLE t(k INTEGER, r REAL, s TEXT, n NUMERIC, b BLOB, u)",
            "CREATE TABLE log(w, v, ty)",
            "CREATE TRIGGER bi BEFORE INSERT ON t BEGIN \
               INSERT INTO log VALUES('bi-k', NEW.k, typeof(NEW.k)); \
               INSERT INTO log VALUES('bi-r', NEW.r, typeof(NEW.r)); \
               INSERT INTO log VALUES('bi-s', NEW.s, typeof(NEW.s)); \
               INSERT INTO log VALUES('bi-n', NEW.n, typeof(NEW.n)); \
               INSERT INTO log VALUES('bi-b', NEW.b, typeof(NEW.b)); \
               INSERT INTO log VALUES('bi-u', NEW.u, typeof(NEW.u)); END",
            "CREATE TRIGGER ai AFTER INSERT ON t BEGIN \
               INSERT INTO log VALUES('ai-k', NEW.k, typeof(NEW.k)); \
               INSERT INTO log VALUES('ai-r', NEW.r, typeof(NEW.r)); \
               INSERT INTO log VALUES('ai-s', NEW.s, typeof(NEW.s)); END",
            "CREATE TRIGGER bw BEFORE INSERT ON t WHEN NEW.k = 1 BEGIN \
               INSERT INTO log VALUES('bw', NEW.k, typeof(NEW.k)); END",
            "CREATE TRIGGER aw AFTER INSERT ON t WHEN NEW.s = '3' BEGIN \
               INSERT INTO log VALUES('aw', NEW.s, typeof(NEW.s)); END",
            "INSERT INTO t VALUES('1', '2', 3, '4.0', 'x', '5')",
            "INSERT INTO t VALUES('abc', 'x1', 2.5, '7e0', 8, 9)",
            "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN \
               INSERT INTO log VALUES('bu-new', NEW.k, typeof(NEW.k)); \
               INSERT INTO log VALUES('bu-old', OLD.k, typeof(OLD.k)); END",
            "CREATE TRIGGER au AFTER UPDATE ON t WHEN NEW.k = 7 BEGIN \
               INSERT INTO log VALUES('au-new', NEW.k, typeof(NEW.k)); \
               INSERT INTO log VALUES('au-s', NEW.s, typeof(NEW.s)); END",
            "UPDATE t SET k = '7', s = 8 WHERE k = 1",
            "CREATE TABLE p(id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TRIGGER pbi BEFORE INSERT ON p BEGIN \
               INSERT INTO log VALUES('pbi-v', NEW.v, typeof(NEW.v)); END",
            "CREATE TRIGGER pai AFTER INSERT ON p BEGIN \
               INSERT INTO log VALUES('pai-id', NEW.id, typeof(NEW.id)); \
               INSERT INTO log VALUES('pai-v', NEW.v, typeof(NEW.v)); END",
            "INSERT INTO p VALUES('10', '11')",
            "INSERT INTO p(v) VALUES('12')",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        compare(&frank, &stock, "SELECT w, v, ty FROM log ORDER BY rowid").await;
        compare(&frank, &stock, "SELECT k, typeof(k), s, typeof(s) FROM t ORDER BY rowid").await;
    });
}

#[test]
fn multi_row_delete_interleaves_before_and_after_triggers_per_row() {
    for_each_backing(|frank, stock| async move {
        for sql in [
            "CREATE TABLE log(w, k, c, m)",
            // INTEGER PRIMARY KEY table, BEFORE and AFTER triggers.
            "CREATE TABLE t(k INTEGER PRIMARY KEY, v)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
            "CREATE TRIGGER bd BEFORE DELETE ON t BEGIN \
               INSERT INTO log VALUES('bd', OLD.k, (SELECT count(*) FROM t), \
                                      (SELECT max(k) FROM t)); END",
            "CREATE TRIGGER ad AFTER DELETE ON t BEGIN \
               INSERT INTO log VALUES('ad', OLD.k, (SELECT count(*) FROM t), \
                                      (SELECT max(k) FROM t)); END",
            "DELETE FROM t WHERE k >= 2",
            // Rowid table with a BEFORE trigger only, deleting every row.
            "CREATE TABLE u(k, v)",
            "INSERT INTO u VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "CREATE TRIGGER ubd BEFORE DELETE ON u BEGIN \
               INSERT INTO log VALUES('ubd', OLD.k, (SELECT count(*) FROM u), \
                                      (SELECT group_concat(k) FROM u)); END",
            "DELETE FROM u",
            // WITHOUT ROWID table.
            "CREATE TABLE w(k TEXT PRIMARY KEY, v) WITHOUT ROWID",
            "INSERT INTO w VALUES ('a', 1), ('b', 2), ('c', 3)",
            "CREATE TRIGGER wbd BEFORE DELETE ON w BEGIN \
               INSERT INTO log VALUES('wbd', OLD.k, (SELECT count(*) FROM w), \
                                      (SELECT group_concat(k) FROM w)); END",
            "DELETE FROM w WHERE v > 1",
            // A BEFORE trigger deleting a later target row: that row is skipped
            // (no second delete, no triggers), and changes() counts only the
            // outer statement's own deletes.
            "CREATE TABLE s(k INTEGER PRIMARY KEY)",
            "INSERT INTO s VALUES (1), (2), (3), (4)",
            "CREATE TRIGGER sbd BEFORE DELETE ON s BEGIN \
               INSERT INTO log VALUES('sbd', OLD.k, (SELECT count(*) FROM s), NULL); \
               DELETE FROM s WHERE k = OLD.k + 1; END",
            "CREATE TRIGGER sad AFTER DELETE ON s BEGIN \
               INSERT INTO log VALUES('sad', OLD.k, (SELECT count(*) FROM s), NULL); END",
            "DELETE FROM s WHERE k IN (1, 2, 3)",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        compare(&frank, &stock, "SELECT changes()").await;
        compare(&frank, &stock, "SELECT w, k, c, m FROM log ORDER BY rowid").await;
        for table in ["t", "u", "w", "s"] {
            compare(&frank, &stock, &format!("SELECT * FROM {table} ORDER BY k")).await;
        }
        // RETURNING across the row-by-row replay, and RAISE(IGNORE) skipping one row.
        for sql in [
            "CREATE TABLE r(k INTEGER PRIMARY KEY, v)",
            "INSERT INTO r VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
            "CREATE TRIGGER rbd BEFORE DELETE ON r WHEN OLD.k = 2 BEGIN \
               SELECT RAISE(IGNORE); END",
            "CREATE TRIGGER rad AFTER DELETE ON r BEGIN \
               INSERT INTO log VALUES('rad', OLD.k, (SELECT count(*) FROM r), NULL); END",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        compare(&frank, &stock, "DELETE FROM r WHERE k > 0 RETURNING k, v").await;
        compare(&frank, &stock, "SELECT changes()").await;
        compare(&frank, &stock, "SELECT * FROM r ORDER BY k").await;
        compare(&frank, &stock, "SELECT w, k, c, m FROM log WHERE w = 'rad' ORDER BY rowid").await;
    });
}
