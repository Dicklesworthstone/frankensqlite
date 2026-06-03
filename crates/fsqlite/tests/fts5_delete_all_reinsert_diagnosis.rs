//! Diagnosis + regression for frankensqlite#94: `DELETE FROM <fts5>` with no
//! WHERE clause (delete-all) followed by re-`INSERT` of the same rowids must
//! NOT raise `Sqlite(PrimaryKeyViolation)`. This exercises all three content
//! modes (stored, contentless, external-content).
//!
//! Run: `cargo test -p fsqlite --features fts5 --test fts5_delete_all_reinsert_diagnosis -- --nocapture`

#![cfg(feature = "fts5")]

use fsqlite::{Connection, SqliteValue};

fn fts_rowids(conn: &Connection, fts: &str) -> Vec<i64> {
    conn.query(&format!("SELECT rowid FROM {fts} ORDER BY rowid"))
        .expect("select fts rowids")
        .iter()
        .map(|r| match &r.values()[0] {
            SqliteValue::Integer(i) => *i,
            other => panic!("unexpected rowid value: {other:?}"),
        })
        .collect()
}

/// Stored-content FTS5: `fts5(c)`.
#[test]
fn delete_all_then_reinsert_stored() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE VIRTUAL TABLE t USING fts5(c)")
        .expect("create stored fts5");

    for i in 1..=3 {
        conn.execute_with_params(
            "INSERT INTO t(rowid, c) VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("doc {i}").into()),
            ],
        )
        .expect("seed insert");
    }

    let before = fts_rowids(&conn, "t");
    eprintln!("[stored] rowids before delete = {before:?}");
    assert_eq!(before, vec![1, 2, 3]);

    let affected = conn.execute("DELETE FROM t").expect("delete-all");
    eprintln!("[stored] DELETE FROM t affected = {affected}");

    let after = fts_rowids(&conn, "t");
    eprintln!("[stored] rowids after delete = {after:?}");
    assert!(after.is_empty(), "[stored] delete-all must clear all rows");

    for i in 1..=3 {
        let res = conn.execute_with_params(
            "INSERT INTO t(rowid, c) VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("redoc {i}").into()),
            ],
        );
        eprintln!("[stored] re-insert rowid={i} => {res:?}");
        res.expect("[stored] re-insert must not raise PrimaryKeyViolation");
    }

    let again = fts_rowids(&conn, "t");
    eprintln!("[stored] rowids after reinsert = {again:?}");
    assert_eq!(again, vec![1, 2, 3]);

    // Index must reflect the NEW content, not the deleted content.
    let hits = conn
        .query("SELECT rowid FROM t WHERE t MATCH 'redoc'")
        .expect("match redoc");
    assert_eq!(hits.len(), 3, "[stored] new content must be searchable");
    let stale = conn
        .query("SELECT rowid FROM t WHERE t MATCH 'doc'")
        .expect("match doc");
    // 'doc' is a prefix-free token only present in old content; after rebuild
    // the only token is 'redoc', so 'doc' must match nothing.
    assert_eq!(stale.len(), 0, "[stored] stale postings must be gone");
}

/// Contentless FTS5: `fts5(c, content='')`.
#[test]
fn delete_all_then_reinsert_contentless() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE VIRTUAL TABLE t USING fts5(c, content='', contentless_delete=1)")
        .expect("create contentless fts5");

    for i in 1..=3 {
        conn.execute_with_params(
            "INSERT INTO t(rowid, c) VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("doc {i}").into()),
            ],
        )
        .expect("seed insert");
    }

    let before = fts_rowids(&conn, "t");
    eprintln!("[contentless] rowids before delete = {before:?}");
    assert_eq!(before, vec![1, 2, 3]);

    let affected = conn.execute("DELETE FROM t").expect("delete-all");
    eprintln!("[contentless] DELETE FROM t affected = {affected}");

    let after = fts_rowids(&conn, "t");
    eprintln!("[contentless] rowids after delete = {after:?}");
    assert!(
        after.is_empty(),
        "[contentless] delete-all must clear all rows"
    );

    for i in 1..=3 {
        let res = conn.execute_with_params(
            "INSERT INTO t(rowid, c) VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("redoc {i}").into()),
            ],
        );
        eprintln!("[contentless] re-insert rowid={i} => {res:?}");
        res.expect("[contentless] re-insert must not raise PrimaryKeyViolation");
    }

    let again = fts_rowids(&conn, "t");
    eprintln!("[contentless] rowids after reinsert = {again:?}");
    assert_eq!(again, vec![1, 2, 3]);

    let hits = conn
        .query("SELECT rowid FROM t WHERE t MATCH 'redoc'")
        .expect("match redoc");
    assert_eq!(
        hits.len(),
        3,
        "[contentless] new content must be searchable"
    );
}

/// External-content FTS5: `fts5(c, content='base', content_rowid='id')`.
#[test]
fn delete_all_then_reinsert_external() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE TABLE base (id INTEGER PRIMARY KEY AUTOINCREMENT, command TEXT NOT NULL)")
        .expect("create base");
    conn.execute("CREATE VIRTUAL TABLE t USING fts5(command, content='base', content_rowid='id')")
        .expect("create external fts5");

    for i in 1..=3 {
        conn.execute_with_params(
            "INSERT INTO t(rowid, command) VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("doc {i}").into()),
            ],
        )
        .expect("seed insert");
    }

    let before = fts_rowids(&conn, "t");
    eprintln!("[external] rowids before delete = {before:?}");
    assert_eq!(before, vec![1, 2, 3]);

    let affected = conn.execute("DELETE FROM t").expect("delete-all");
    eprintln!("[external] DELETE FROM t affected = {affected}");

    let after = fts_rowids(&conn, "t");
    eprintln!("[external] rowids after delete = {after:?}");
    assert!(
        after.is_empty(),
        "[external] delete-all must clear all rows"
    );

    for i in 1..=3 {
        let res = conn.execute_with_params(
            "INSERT INTO t(rowid, command) VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("redoc {i}").into()),
            ],
        );
        eprintln!("[external] re-insert rowid={i} => {res:?}");
        res.expect("[external] re-insert must not raise PrimaryKeyViolation");
    }

    let again = fts_rowids(&conn, "t");
    eprintln!("[external] rowids after reinsert = {again:?}");
    assert_eq!(again, vec![1, 2, 3]);
}

/// A WHERE-clause DELETE against a stored FTS5 table must delete only the
/// matching rowid, not the whole table.
#[test]
fn delete_where_rowid_then_reinsert_stored() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE VIRTUAL TABLE t USING fts5(c)")
        .expect("create stored fts5");

    for i in 1..=3 {
        conn.execute_with_params(
            "INSERT INTO t(rowid, c) VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("doc {i}").into()),
            ],
        )
        .expect("seed insert");
    }

    let affected = conn
        .execute("DELETE FROM t WHERE rowid = 2")
        .expect("delete one");
    eprintln!("[stored-where] DELETE WHERE rowid=2 affected = {affected}");
    assert_eq!(affected, 1, "[stored-where] exactly one row deleted");

    let after = fts_rowids(&conn, "t");
    eprintln!("[stored-where] rowids after delete = {after:?}");
    assert_eq!(after, vec![1, 3], "[stored-where] only rowid 2 removed");

    // Re-insert rowid 2 must succeed now that it's gone.
    conn.execute_with_params(
        "INSERT INTO t(rowid, c) VALUES (?1, ?2)",
        &[SqliteValue::Integer(2), SqliteValue::Text("redoc 2".into())],
    )
    .expect("[stored-where] re-insert rowid 2");

    let again = fts_rowids(&conn, "t");
    assert_eq!(again, vec![1, 2, 3]);
}

/// A contentless table WITHOUT `contentless_delete=1` must reject DELETE,
/// matching SQLite, and must not corrupt the in-memory index.
#[test]
fn delete_all_rejected_on_contentless_without_contentless_delete() {
    let conn = Connection::open(":memory:").expect("open");
    conn.execute("CREATE VIRTUAL TABLE t USING fts5(c, content='')")
        .expect("create contentless fts5");

    for i in 1..=3 {
        conn.execute_with_params(
            "INSERT INTO t(rowid, c) VALUES (?1, ?2)",
            &[
                SqliteValue::Integer(i),
                SqliteValue::Text(format!("doc {i}").into()),
            ],
        )
        .expect("seed insert");
    }

    let res = conn.execute("DELETE FROM t");
    eprintln!("[contentless-no-delete] DELETE FROM t => {res:?}");
    assert!(
        res.is_err(),
        "[contentless-no-delete] DELETE must be rejected without contentless_delete=1"
    );
}
