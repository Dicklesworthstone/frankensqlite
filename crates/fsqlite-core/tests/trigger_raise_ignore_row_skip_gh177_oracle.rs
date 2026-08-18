#![recursion_limit = "512"]

//! GH #177 (bd-gh-trigger-raise-ignore-row-skip-oit9m): `RAISE(IGNORE)` fired
//! from a BEFORE DELETE / BEFORE UPDATE trigger must skip ONLY the current row
//! and continue processing the remaining rows — it must NOT abort the whole
//! statement.
//!
//! Before the fix, the interpreter DELETE/UPDATE paths treated the first
//! `RAISE(IGNORE)` as a statement-level skip (`changes() == 0`, no rows
//! touched). SQLite 3.46.1 skips per-row.
//!
//! Every expected value below was oracled directly with `sqlite3 :memory:` and
//! is pinned here differentially against the live rusqlite oracle (SQLite
//! 3.46.1). Both the surviving/updated table state AND `changes()` are
//! compared.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        }
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => {
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        }
    }
}

async fn exec_both(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    f.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("frank exec `{sql}`: {e:?}"));
    r.execute_batch(sql)
        .unwrap_or_else(|e| panic!("rusqlite exec `{sql}`: {e:?}"));
}

/// Assert both engines return the same rows for `query`, compared as a sorted
/// multiset (trigger firing order is engine-defined, so we do not pin order).
async fn assert_agree(f: &Connection, r: &rusqlite::Connection, query: &str) {
    let mut fr: Vec<Vec<String>> = f
        .query(query)
        .await
        .unwrap_or_else(|e| panic!("frank query `{query}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut st = r.prepare(query).unwrap();
    let n = st.column_count();
    let mut rr: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    fr.sort();
    rr.sort();
    assert_eq!(fr, rr, "GH#177 divergence on `{query}`");
}

/// Compare `SELECT changes()` on both engines after the most recent statement.
async fn assert_changes_agree(f: &Connection, r: &rusqlite::Connection, ctx: &str) {
    let fr = f
        .query("SELECT changes()")
        .await
        .unwrap_or_else(|e| panic!("frank changes(): {e:?}"));
    let frank_changes = match fr.first().and_then(|row| row.values().first()) {
        Some(SqliteValue::Integer(n)) => *n,
        other => panic!("frank changes() unexpected: {other:?}"),
    };
    let rusqlite_changes: i64 = r
        .query_row("SELECT changes()", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        frank_changes, rusqlite_changes,
        "GH#177 changes() divergence [{ctx}]: frank={frank_changes} rusqlite={rusqlite_changes}"
    );
}

/// Oracle-pinned expected value: frank changes() must equal `expected`.
async fn assert_frank_changes(f: &Connection, expected: i64, ctx: &str) {
    let fr = f
        .query("SELECT changes()")
        .await
        .unwrap_or_else(|e| panic!("frank changes(): {e:?}"));
    let frank_changes = match fr.first().and_then(|row| row.values().first()) {
        Some(SqliteValue::Integer(n)) => *n,
        other => panic!("frank changes() unexpected: {other:?}"),
    };
    assert_eq!(
        frank_changes, expected,
        "GH#177 fixed-oracle changes() [{ctx}]: frank={frank_changes} expected={expected}"
    );
}

const SEED: &[&str] = &["INSERT INTO t VALUES (1,-3),(2,5),(3,-7),(4,10),(5,0)"];

/// BEFORE DELETE RAISE(IGNORE) WHERE OLD.v > 0: only v<=0 rows deleted.
/// Oracle: leaves (2,5),(4,10); changes()=3.
#[test]
fn before_delete_ignore_positive_rows() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            SEED[0],
            "CREATE TRIGGER trg BEFORE DELETE ON t BEGIN SELECT RAISE(IGNORE) WHERE OLD.v > 0; END",
            "DELETE FROM t",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "delete-positive").await;
        assert_frank_changes(&f, 3, "delete-positive").await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
    });
}

/// BEFORE UPDATE RAISE(IGNORE) WHERE OLD.v > 0: only v<=0 rows updated.
/// Oracle: (1,97),(2,5),(3,93),(4,10),(5,100); changes()=3.
#[test]
fn before_update_ignore_positive_rows() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            SEED[0],
            "CREATE TRIGGER trg BEFORE UPDATE ON t BEGIN SELECT RAISE(IGNORE) WHERE OLD.v > 0; END",
            "UPDATE t SET v = v + 100",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "update-positive").await;
        assert_frank_changes(&f, 3, "update-positive").await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
    });
}

/// IGNORE the FIRST row only (id=1): rows after it must still be processed.
/// Oracle DELETE: leaves (1,-3); changes()=4.
#[test]
fn before_delete_ignore_first_row_only() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            SEED[0],
            "CREATE TRIGGER trg BEFORE DELETE ON t BEGIN SELECT RAISE(IGNORE) WHERE OLD.id = 1; END",
            "DELETE FROM t",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "delete-first").await;
        assert_frank_changes(&f, 4, "delete-first").await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
    });
}

/// IGNORE the LAST row only (id=5): earlier rows are unaffected.
/// Oracle DELETE: leaves (5,0); changes()=4.
#[test]
fn before_delete_ignore_last_row_only() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            SEED[0],
            "CREATE TRIGGER trg BEFORE DELETE ON t BEGIN SELECT RAISE(IGNORE) WHERE OLD.id = 5; END",
            "DELETE FROM t",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "delete-last").await;
        assert_frank_changes(&f, 4, "delete-last").await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
    });
}

/// IGNORE ALL rows: no-op, table unchanged, changes()=0.
#[test]
fn before_delete_ignore_all_rows() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            SEED[0],
            "CREATE TRIGGER trg BEFORE DELETE ON t BEGIN SELECT RAISE(IGNORE); END",
            "DELETE FROM t",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "delete-all").await;
        assert_frank_changes(&f, 0, "delete-all").await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
    });
}

/// IGNORE NO rows (guard never true): full DML runs, changes()=5.
#[test]
fn before_delete_ignore_no_rows() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            SEED[0],
            "CREATE TRIGGER trg BEFORE DELETE ON t BEGIN SELECT RAISE(IGNORE) WHERE 0; END",
            "DELETE FROM t",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "delete-none").await;
        assert_frank_changes(&f, 5, "delete-none").await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
    });
}

/// Table WITHOUT an INTEGER PRIMARY KEY column (implicit rowid): the row-skip
/// rewrite must key on the real rowid. Oracle DELETE: leaves 5,10; changes()=3.
#[test]
fn before_delete_ignore_no_ipk_column() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a)",
            "INSERT INTO t VALUES (-3),(5),(-7),(10),(0)",
            "CREATE TRIGGER trg BEFORE DELETE ON t BEGIN SELECT RAISE(IGNORE) WHERE OLD.a > 0; END",
            "DELETE FROM t",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "delete-no-ipk").await;
        assert_frank_changes(&f, 3, "delete-no-ipk").await;
        assert_agree(&f, &r, "SELECT a FROM t ORDER BY a").await;
    });
}

/// UPDATE on a table WITHOUT an IPK column. Oracle: (97,5,93,10,100);
/// changes()=3.
#[test]
fn before_update_ignore_no_ipk_column() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a)",
            "INSERT INTO t VALUES (-3),(5),(-7),(10),(0)",
            "CREATE TRIGGER trg BEFORE UPDATE ON t BEGIN SELECT RAISE(IGNORE) WHERE OLD.a > 0; END",
            "UPDATE t SET a = a + 100",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "update-no-ipk").await;
        assert_frank_changes(&f, 3, "update-no-ipk").await;
        assert_agree(&f, &r, "SELECT a FROM t ORDER BY a").await;
    });
}

/// AFTER DELETE trigger must fire ONLY for the non-ignored rows.
/// Oracle: remaining (2,5),(4,10); AFTER log = {1,3,5}; changes()=3.
#[test]
fn before_delete_ignore_with_after_trigger() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TABLE log(rid INTEGER)",
            SEED[0],
            "CREATE TRIGGER trg_before BEFORE DELETE ON t \
                 BEGIN SELECT RAISE(IGNORE) WHERE OLD.v > 0; END",
            "CREATE TRIGGER trg_after AFTER DELETE ON t \
                 BEGIN INSERT INTO log VALUES(OLD.id); END",
            "DELETE FROM t",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "delete-after").await;
        assert_frank_changes(&f, 3, "delete-after").await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT rid FROM log ORDER BY rid").await;
    });
}

/// AFTER UPDATE trigger must fire ONLY for the non-ignored rows.
/// Oracle: (1,97),(2,5),(3,93),(4,10),(5,100); AFTER log = {1,3,5}; changes()=3.
#[test]
fn before_update_ignore_with_after_trigger() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TABLE log(rid INTEGER)",
            SEED[0],
            "CREATE TRIGGER trg_before BEFORE UPDATE ON t \
                 BEGIN SELECT RAISE(IGNORE) WHERE OLD.v > 0; END",
            "CREATE TRIGGER trg_after AFTER UPDATE ON t \
                 BEGIN INSERT INTO log VALUES(OLD.id); END",
            "UPDATE t SET v = v + 100",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_changes_agree(&f, &r, "update-after").await;
        assert_frank_changes(&f, 3, "update-after").await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT rid FROM log ORDER BY rid").await;
    });
}
