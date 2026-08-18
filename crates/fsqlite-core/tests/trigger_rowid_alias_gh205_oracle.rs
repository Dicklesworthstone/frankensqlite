#![recursion_limit = "512"]

//! GH #205 (bd-gh-trigger-rowid-alias-8ywyb): the `rowid` / `oid` / `_rowid_`
//! aliases on NEW/OLD must resolve inside trigger bodies (and WHEN clauses) even
//! on tables that have no INTEGER PRIMARY KEY column to carry the rowid.
//!
//! The DELETE path was fixed earlier (387fcbb9d); this keeper pins the INSERT,
//! UPDATE, and INSERT OR REPLACE paths differentially against the live rusqlite
//! oracle (SQLite 3.46.1). A control table WITH an INTEGER PRIMARY KEY confirms
//! the previously-working alias-over-column path is not regressed.
//!
//! Standalone expected values (BEFORE INSERT auto-rowid == -1, etc.) were also
//! confirmed directly with `sqlite3 :memory:`.

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

/// Run a single SQL statement (or batch) against both engines.
async fn exec_both(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    f.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("frank exec `{sql}`: {e:?}"));
    r.execute_batch(sql)
        .unwrap_or_else(|e| panic!("rusqlite exec `{sql}`: {e:?}"));
}

/// Assert both engines return the same rows for `query`, compared as a multiset.
///
/// The relative firing order of multiple triggers matching one event is
/// engine-defined in SQLite, so we compare the sorted row sets rather than the
/// literal order; wrong values, wrong counts, and missing/extra rows are still
/// caught.
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
    assert_eq!(fr, rr, "GH#205 divergence on `{query}`");
}

/// Assert a frank query returns exactly `expected` (oracle-fixed values),
/// compared as a multiset (see `assert_agree`).
async fn assert_frank_eq(f: &Connection, query: &str, expected: &[&[&str]]) {
    let mut fr: Vec<Vec<String>> = f
        .query(query)
        .await
        .unwrap_or_else(|e| panic!("frank query `{query}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut exp: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|s| (*s).to_owned()).collect())
        .collect();
    fr.sort();
    exp.sort();
    assert_eq!(fr, exp, "GH#205 fixed-oracle divergence on `{query}`");
}

#[test]
fn after_insert_rowid_no_ipk() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a TEXT)",
            "CREATE TABLE log(op TEXT, rid INTEGER, oidv INTEGER, ridv INTEGER, aval TEXT)",
            "CREATE TRIGGER ai AFTER INSERT ON t BEGIN \
                 INSERT INTO log VALUES('ai', NEW.rowid, NEW.oid, NEW._rowid_, NEW.a); END",
        ] {
            exec_both(&f, &r, sql).await;
        }
        // Auto-assigned rowids, then an explicit rowid, then auto again.
        for sql in [
            "INSERT INTO t(a) VALUES('first')",
            "INSERT INTO t(a) VALUES('second')",
            "INSERT INTO t(rowid, a) VALUES(100, 'explicit')",
            "INSERT INTO t(a) VALUES('after_explicit')",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_agree(&f, &r, "SELECT op, rid, oidv, ridv, aval FROM log ORDER BY rowid").await;
        // Fixed oracle values: NEW.rowid == NEW.oid == NEW._rowid_ == actual rowid.
        assert_frank_eq(
            &f,
            "SELECT rid, oidv, ridv FROM log ORDER BY rowid",
            &[
                &["1", "1", "1"],
                &["2", "2", "2"],
                &["100", "100", "100"],
                &["101", "101", "101"],
            ],
        )
        .await;
    });
}

#[test]
fn after_delete_rowid_no_ipk_regression() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a TEXT)",
            "CREATE TABLE log(op TEXT, rid INTEGER, oidv INTEGER, ridv INTEGER, aval TEXT)",
            "CREATE TRIGGER ad AFTER DELETE ON t BEGIN \
                 INSERT INTO log VALUES('ad', OLD.rowid, OLD.oid, OLD._rowid_, OLD.a); END",
            "INSERT INTO t(a) VALUES('first')",
            "INSERT INTO t(a) VALUES('second')",
            "INSERT INTO t(rowid, a) VALUES(50, 'third')",
            "DELETE FROM t WHERE a IN ('first', 'third')",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_agree(&f, &r, "SELECT op, rid, oidv, ridv, aval FROM log ORDER BY rowid").await;
    });
}

#[test]
fn after_update_rowid_no_ipk() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a TEXT)",
            "CREATE TABLE log(op TEXT, oldrid INTEGER, newrid INTEGER, aval TEXT)",
            "CREATE TRIGGER au AFTER UPDATE ON t BEGIN \
                 INSERT INTO log VALUES('au', OLD.rowid, NEW.rowid, NEW.a); END",
            "INSERT INTO t(a) VALUES('r1')",
            "INSERT INTO t(a) VALUES('r2')",
            "INSERT INTO t(rowid, a) VALUES(9, 'r9')",
            "UPDATE t SET a = a || '-upd'",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_agree(&f, &r, "SELECT op, oldrid, newrid, aval FROM log ORDER BY rowid").await;
        // On an ordinary UPDATE the rowid is unchanged: OLD.rowid == NEW.rowid.
        assert_frank_eq(
            &f,
            "SELECT oldrid, newrid FROM log ORDER BY rowid",
            &[&["1", "1"], &["2", "2"], &["9", "9"]],
        )
        .await;
    });
}

#[test]
fn before_insert_rowid_no_ipk() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a TEXT)",
            "CREATE TABLE log(op TEXT, rid INTEGER, aval TEXT)",
            // Value: BEFORE INSERT NEW.rowid == -1 for auto rowid, explicit otherwise.
            "CREATE TRIGGER bi BEFORE INSERT ON t BEGIN \
                 INSERT INTO log VALUES('bi', NEW.rowid, NEW.a); END",
            // WHEN clause: only fires when the (not-yet-assigned) rowid is -1.
            "CREATE TRIGGER bi_when BEFORE INSERT ON t WHEN NEW.rowid = -1 BEGIN \
                 INSERT INTO log VALUES('bi_when_neg1', NEW.rowid, NEW.a); END",
        ] {
            exec_both(&f, &r, sql).await;
        }
        for sql in [
            "INSERT INTO t(a) VALUES('auto')",
            "INSERT INTO t(rowid, a) VALUES(77, 'explicit')",
            "INSERT INTO t(a) VALUES('auto2')",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_agree(&f, &r, "SELECT op, rid, aval FROM log ORDER BY rowid").await;
        // Fixed oracle: auto rows -> -1 (bi) plus a matching bi_when row;
        // the explicit row -> 77 (bi) and no bi_when row.
        assert_frank_eq(
            &f,
            "SELECT op, rid FROM log ORDER BY rowid",
            &[
                &["'bi'", "-1"],
                &["'bi_when_neg1'", "-1"],
                &["'bi'", "77"],
                &["'bi'", "-1"],
                &["'bi_when_neg1'", "-1"],
            ],
        )
        .await;
    });
}

#[test]
fn update_when_new_rowid_no_ipk() {
    // The GH report's hard-error case: a WHEN clause referencing NEW.rowid on a
    // table without an INTEGER PRIMARY KEY column.
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a TEXT)",
            "CREATE TABLE log(op TEXT, oldrid INTEGER, newrid INTEGER, aval TEXT)",
            "CREATE TRIGGER au_when AFTER UPDATE ON t WHEN NEW.rowid = 2 BEGIN \
                 INSERT INTO log VALUES('au_when_rid2', OLD.rowid, NEW.rowid, NEW.a); END",
            "CREATE TRIGGER bu_when BEFORE UPDATE ON t WHEN OLD.rowid = 1 BEGIN \
                 INSERT INTO log VALUES('bu_when_rid1', OLD.rowid, NEW.rowid, NEW.a); END",
            "INSERT INTO t(a) VALUES('one')",
            "INSERT INTO t(a) VALUES('two')",
            "INSERT INTO t(a) VALUES('three')",
            "UPDATE t SET a = a || '!'",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_agree(&f, &r, "SELECT op, oldrid, newrid, aval FROM log ORDER BY rowid").await;
        // Only the rowid==2 (AFTER) and rowid==1 (BEFORE) rows should be logged.
        assert_frank_eq(
            &f,
            "SELECT op, oldrid, newrid FROM log ORDER BY oldrid",
            &[&["'bu_when_rid1'", "1", "1"], &["'au_when_rid2'", "2", "2"]],
        )
        .await;
    });
}

#[test]
fn insert_or_replace_victim_delete_trigger_no_ipk() {
    // REPLACE conflict resolution fires DELETE triggers only when
    // recursive_triggers = ON; OLD.rowid must be the victim's rowid.
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "PRAGMA recursive_triggers = ON",
            "CREATE TABLE t(a TEXT, b TEXT UNIQUE)",
            "CREATE TABLE log(op TEXT, rid INTEGER, aval TEXT)",
            "CREATE TRIGGER bd BEFORE DELETE ON t BEGIN \
                 INSERT INTO log VALUES('bd', OLD.rowid, OLD.a); END",
            "CREATE TRIGGER ad AFTER DELETE ON t BEGIN \
                 INSERT INTO log VALUES('ad', OLD.rowid, OLD.a); END",
            "INSERT INTO t(a, b) VALUES('first', 'x')",
            "INSERT INTO t(a, b) VALUES('second', 'y')",
            // Conflicts on b='x' -> victim is rowid 1.
            "INSERT OR REPLACE INTO t(a, b) VALUES('third', 'x')",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_agree(&f, &r, "SELECT op, rid, aval FROM log ORDER BY rowid").await;
        assert_agree(&f, &r, "SELECT rowid, a, b FROM t ORDER BY rowid").await;
        // Victim rowid must be 1 (fixed oracle).
        assert_frank_eq(
            &f,
            "SELECT op, rid FROM log ORDER BY rowid",
            &[&["'bd'", "1"], &["'ad'", "1"]],
        )
        .await;
    });
}

#[test]
fn ipk_control_not_regressed() {
    // Control: a table WITH an INTEGER PRIMARY KEY. The alias resolves via the
    // snapshotted column; INSERT/UPDATE/DELETE rowid must stay correct.
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT)",
            "CREATE TABLE log(op TEXT, rid INTEGER, idv INTEGER, aval TEXT)",
            "CREATE TRIGGER ai AFTER INSERT ON t BEGIN \
                 INSERT INTO log VALUES('ai', NEW.rowid, NEW.id, NEW.a); END",
            "CREATE TRIGGER au AFTER UPDATE ON t BEGIN \
                 INSERT INTO log VALUES('au', NEW.rowid, NEW.id, NEW.a); END",
            "CREATE TRIGGER ad AFTER DELETE ON t BEGIN \
                 INSERT INTO log VALUES('ad', OLD.rowid, OLD.id, OLD.a); END",
            "INSERT INTO t(a) VALUES('auto')",
            "INSERT INTO t(id, a) VALUES(42, 'explicit')",
            "UPDATE t SET a = a || '-u' WHERE id = 1",
            "DELETE FROM t WHERE id = 42",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_agree(&f, &r, "SELECT op, rid, idv, aval FROM log ORDER BY rowid").await;
        // NEW.rowid tracks the INTEGER PRIMARY KEY column value.
        assert_frank_eq(
            &f,
            "SELECT op, rid, idv FROM log ORDER BY rowid",
            &[
                &["'ai'", "1", "1"],
                &["'ai'", "42", "42"],
                &["'au'", "1", "1"],
                &["'ad'", "42", "42"],
            ],
        )
        .await;
    });
}
