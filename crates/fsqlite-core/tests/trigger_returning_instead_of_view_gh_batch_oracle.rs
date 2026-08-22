#![recursion_limit = "512"]

//! GH #160 / #241 / #242 (bd-gh-trigger-returning): a DML statement with a
//! `RETURNING` clause that targets a VIEW with a matching `INSTEAD OF` trigger
//! must project `RETURNING` over the affected view row — the NEW row for
//! INSERT/UPDATE, the OLD row for DELETE — exactly like a table DML.
//!
//! Before the fix, `execute_instead_of_view_{insert,update,delete}` fired the
//! trigger (so the base-table side effect was applied) but returned
//! `Vec::new()`, silently dropping every RETURNING row.
//!
//! This file also pins the sibling RAISE(IGNORE) + RETURNING cases (GH #228
//! INSERT, #229 UPDATE, #230 DELETE): a per-row `RAISE(IGNORE)` in a BEFORE
//! trigger must emit RETURNING rows for exactly the non-ignored rows (the DML
//! restriction landed in GH #177; RETURNING must follow it).
//!
//! Every expected value was oracled with `sqlite3 :memory:` (3.46.1) and is
//! pinned here differentially against the live rusqlite oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
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
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
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

/// Run a `... RETURNING ...` DML on both engines and assert the emitted rows
/// match as a sorted multiset (RETURNING order is engine-defined). Runs the DML
/// via `query()` on frank (RETURNING produces rows) and `query_map` on rusqlite.
async fn assert_returning_agree(f: &Connection, r: &rusqlite::Connection, dml: &str) {
    let mut fr: Vec<Vec<String>> = f
        .query(dml)
        .await
        .unwrap_or_else(|e| panic!("frank returning `{dml}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut st = r.prepare(dml).unwrap();
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
    assert_eq!(fr, rr, "RETURNING divergence on `{dml}`");
}

/// Assert both engines return the same rows for a plain SELECT (final state).
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
    assert_eq!(fr, rr, "state divergence on `{query}`");
}

// ---------------------------------------------------------------------------
// Cluster A: INSTEAD OF view RETURNING (#160 INSERT, #241 UPDATE, #242 DELETE)
// ---------------------------------------------------------------------------

/// GH #160: INSTEAD OF INSERT view RETURNING must emit the NEW view row.
/// Oracle: `INSERT INTO v VALUES(1,'hi') RETURNING id,v` -> `1|hi`, base=`1|HI`.
#[test]
fn instead_of_insert_view_returning() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE base(id INTEGER PRIMARY KEY, v TEXT)",
            "CREATE VIEW v AS SELECT id,v FROM base",
            "CREATE TRIGGER v_ins INSTEAD OF INSERT ON v BEGIN \
             INSERT INTO base(id,v) VALUES(NEW.id,upper(NEW.v)); END",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_returning_agree(&f, &r, "INSERT INTO v VALUES(1,'hi') RETURNING id,v").await;
        assert_agree(&f, &r, "SELECT id,v FROM base ORDER BY id").await;
    });
}

/// GH #241: INSTEAD OF UPDATE view RETURNING must emit the post-SET (NEW) row.
/// Oracle: `UPDATE v SET val='z' WHERE id=1 RETURNING id,val` -> `1|z`.
#[test]
fn instead_of_update_view_returning() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO t VALUES(1,'a'),(2,'b')",
            "CREATE VIEW v AS SELECT id,val FROM t",
            "CREATE TRIGGER v_upd INSTEAD OF UPDATE ON v BEGIN \
             UPDATE t SET val=NEW.val WHERE id=NEW.id; END",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_returning_agree(&f, &r, "UPDATE v SET val='z' WHERE id=1 RETURNING id,val").await;
        assert_agree(&f, &r, "SELECT id,val FROM t ORDER BY id").await;
    });
}

/// GH #242: INSTEAD OF DELETE view RETURNING must emit the deleted (OLD) row.
/// Oracle: `DELETE FROM v WHERE a=2 RETURNING a,b` -> `2|two`.
#[test]
fn instead_of_delete_view_returning() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
            "INSERT INTO t VALUES(1,'one'),(2,'two'),(3,'three')",
            "CREATE VIEW v AS SELECT a,b FROM t",
            "CREATE TRIGGER v_del INSTEAD OF DELETE ON v BEGIN DELETE FROM t WHERE a=OLD.a; END",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_returning_agree(&f, &r, "DELETE FROM v WHERE a=2 RETURNING a,b").await;
        assert_agree(&f, &r, "SELECT a,b FROM t ORDER BY a").await;
    });
}

/// Star projection over an INSTEAD OF view RETURNING (`RETURNING *`).
#[test]
fn instead_of_view_returning_star_and_expr() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE base(id INTEGER PRIMARY KEY, v TEXT)",
            "CREATE VIEW v AS SELECT id,v FROM base",
            "CREATE TRIGGER v_ins INSTEAD OF INSERT ON v BEGIN \
             INSERT INTO base(id,v) VALUES(NEW.id,NEW.v); END",
        ] {
            exec_both(&f, &r, sql).await;
        }
        // `*` plus a computed expression referencing a bare view column.
        assert_returning_agree(
            &f,
            &r,
            "INSERT INTO v VALUES(7,'hi') RETURNING *, id+1, upper(v)",
        )
        .await;
        assert_agree(&f, &r, "SELECT id,v FROM base ORDER BY id").await;
    });
}

// ---------------------------------------------------------------------------
// Cluster B: RAISE(IGNORE) + RETURNING (#228 INSERT, #229 UPDATE, #230 DELETE)
// ---------------------------------------------------------------------------

/// GH #229: BEFORE UPDATE RAISE(IGNORE) — RETURNING emits only non-ignored rows.
/// Oracle: `UPDATE t SET v=v||'!' RETURNING id,v` -> (1,a!),(3,c!); row 2 kept.
#[test]
fn before_update_raise_ignore_returning() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')",
            "CREATE TRIGGER trg BEFORE UPDATE ON t WHEN NEW.id=2 BEGIN SELECT RAISE(IGNORE); END",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_returning_agree(&f, &r, "UPDATE t SET v=v||'!' RETURNING id,v").await;
        assert_agree(&f, &r, "SELECT id,v FROM t ORDER BY id").await;
    });
}

/// GH #230: BEFORE DELETE RAISE(IGNORE) — RETURNING emits only non-ignored rows.
/// Oracle: `DELETE FROM t RETURNING a,b` -> (1,one),(3,three); row 2 survives.
#[test]
fn before_delete_raise_ignore_returning() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
            "INSERT INTO t VALUES(1,'one'),(2,'two'),(3,'three')",
            "CREATE TRIGGER tr BEFORE DELETE ON t WHEN OLD.a=2 BEGIN SELECT RAISE(IGNORE); END",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_returning_agree(&f, &r, "DELETE FROM t RETURNING a,b").await;
        assert_agree(&f, &r, "SELECT a,b FROM t ORDER BY a").await;
    });
}

/// GH #228: BEFORE INSERT RAISE(IGNORE) — RETURNING emits only non-ignored rows.
/// Oracle: `INSERT ... (10,keep),(20,skip),(30,keep) RETURNING a,b` -> keep rows.
#[test]
fn before_insert_raise_ignore_returning() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)",
            "CREATE TRIGGER trg BEFORE INSERT ON t BEGIN SELECT RAISE(IGNORE) WHERE NEW.b='skip'; END",
        ] {
            exec_both(&f, &r, sql).await;
        }
        assert_returning_agree(
            &f,
            &r,
            "INSERT INTO t VALUES (10,'keep'),(20,'skip'),(30,'keep') RETURNING a,b",
        )
        .await;
        assert_agree(&f, &r, "SELECT a,b FROM t ORDER BY a").await;
    });
}
