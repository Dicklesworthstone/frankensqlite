#![recursion_limit = "512"]

//! GH #159 (bd-gh-update-or-ignore-returning): `UPDATE OR IGNORE ... RETURNING`
//! must NOT emit a row for an update that a uniqueness (PK/UNIQUE) conflict
//! suppresses — stock sqlite3 emits nothing for the skipped row, while the
//! (unchanged) table state stays correct. rusqlite is the oracle for BOTH the
//! RETURNING rows and the post-update table contents.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

/// Agree on the rows a query/DML-with-RETURNING produces (both engines, sorted).
async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut fr: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect();
    fr.sort();
    let mut st = rconn.prepare(sql).unwrap();
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
    rr.sort();
    assert_eq!(fr, rr, "UPDATE OR IGNORE RETURNING row mismatch on `{sql}`");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) {
    for s in stmts {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn update_or_ignore_returning_unique_conflict_emits_nothing_gh159() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (1,'a@x.com',10),(2,'b@x.com',20)",
            ],
        )
        .await;
        // Variant (a): UNIQUE conflict on `email` — RETURNING must emit nothing.
        assert_agree(
            &f,
            &r,
            "UPDATE OR IGNORE t SET email='a@x.com' WHERE id=2 RETURNING id,email,v",
        )
        .await;
        // Table state must be unchanged (row 2 keeps b@x.com) on both engines.
        assert_agree(&f, &r, "SELECT id,email,v FROM t ORDER BY id").await;
    });
}

#[test]
fn update_or_ignore_returning_pk_conflict_emits_nothing_gh159() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (1,'a@x.com',10),(2,'b@x.com',20)",
            ],
        )
        .await;
        // Variant (b): PK/rowid conflict (SET id=1 collides with row 1). SQLite
        // emits nothing (previously fsqlite wrongly emitted the CONFLICTING row).
        assert_agree(
            &f,
            &r,
            "UPDATE OR IGNORE t SET id=1 WHERE id=2 RETURNING id,email,v",
        )
        .await;
        assert_agree(&f, &r, "SELECT id,email,v FROM t ORDER BY id").await;
    });
}

#[test]
fn update_or_ignore_returning_no_conflict_still_emits_gh159() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (1,'a@x.com',10),(2,'b@x.com',20)",
            ],
        )
        .await;
        // Control: a NON-conflicting UPDATE OR IGNORE RETURNING must still emit
        // the updated row (the guard must not over-suppress).
        assert_agree(
            &f,
            &r,
            "UPDATE OR IGNORE t SET email='c@x.com', v=99 WHERE id=2 RETURNING id,email,v",
        )
        .await;
        assert_agree(&f, &r, "SELECT id,email,v FROM t ORDER BY id").await;
        // Setting a column to its OWN current value is NOT a conflict — must emit.
        assert_agree(
            &f,
            &r,
            "UPDATE OR IGNORE t SET email='a@x.com' WHERE id=1 RETURNING id,email,v",
        )
        .await;
    });
}

#[test]
fn update_or_ignore_returning_from_conflict_gh159() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (1,'a@x.com',10),(2,'b@x.com',20),(3,'c@x.com',30)",
                "CREATE TABLE src (tid INTEGER, newemail TEXT)",
                "INSERT INTO src VALUES (2,'a@x.com'),(3,'z@x.com')",
            ],
        )
        .await;
        // UPDATE ... FROM ... OR IGNORE RETURNING: row 2 conflicts (a@x.com), so
        // only the non-conflicting row 3 (z@x.com) may be emitted.
        assert_agree(&f, &r, "UPDATE OR IGNORE t SET email=src.newemail FROM src WHERE t.id=src.tid RETURNING id,email,v").await;
        assert_agree(&f, &r, "SELECT id,email,v FROM t ORDER BY id").await;
    });
}

#[test]
fn update_or_ignore_returning_without_rowid_gh159() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE w (k TEXT PRIMARY KEY, e TEXT UNIQUE, n INTEGER) WITHOUT ROWID",
                "INSERT INTO w VALUES ('p','a',1),('q','b',2)",
            ],
        )
        .await;
        // WITHOUT ROWID sibling: UNIQUE `e` conflict must emit nothing.
        assert_agree(
            &f,
            &r,
            "UPDATE OR IGNORE w SET e='a' WHERE k='q' RETURNING k,e,n",
        )
        .await;
        assert_agree(&f, &r, "SELECT k,e,n FROM w ORDER BY k").await;
    });
}
