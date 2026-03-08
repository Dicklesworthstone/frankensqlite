//! Cross-connection UPSERT visibility tests.
//!
//! Verifies that INSERT ... ON CONFLICT DO UPDATE correctly detects
//! conflicting rows committed by a *different* connection to the same
//! file-backed database.  Single-connection upserts pass; this file
//! exercises the cross-connection refresh path.

use fsqlite::Connection;
use tempfile::tempdir;

/// Helper: open a connection to the given path.
fn open(path: &str) -> Connection {
    Connection::open(path.to_owned()).expect("open connection")
}

/// Helper: extract column value as text.
fn col(rows: &[fsqlite::Row], row: usize, col: usize) -> String {
    rows[row]
        .get(col)
        .map(fsqlite_types::SqliteValue::to_text)
        .unwrap_or_default()
}

/// Baseline: single-connection upsert works (sanity check).
#[test]
fn single_conn_upsert_updates_existing_row() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("single.db").to_string_lossy().to_string();

    let c = open(&path);
    c.execute(
        "CREATE TABLE agents (id INTEGER PRIMARY KEY AUTOINCREMENT, \
         slug TEXT NOT NULL UNIQUE, name TEXT NOT NULL)",
    )
    .unwrap();

    c.execute(
        "INSERT INTO agents(slug,name) VALUES('test','Original') \
         ON CONFLICT(slug) DO UPDATE SET name=excluded.name",
    )
    .unwrap();

    c.execute(
        "INSERT INTO agents(slug,name) VALUES('test','Updated') \
         ON CONFLICT(slug) DO UPDATE SET name=excluded.name",
    )
    .unwrap();

    let rows = c
        .query("SELECT COUNT(*) FROM agents WHERE slug='test'")
        .unwrap();
    assert_eq!(
        col(&rows, 0, 0),
        "1",
        "single-conn: should have exactly 1 row"
    );

    let rows = c
        .query("SELECT name FROM agents WHERE slug='test'")
        .unwrap();
    assert_eq!(
        col(&rows, 0, 0),
        "Updated",
        "single-conn: name should be updated"
    );
}

/// Cross-connection: Connection B's upsert must see Connection A's committed row.
#[test]
fn cross_conn_upsert_detects_conflict() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cross.db").to_string_lossy().to_string();

    // Bootstrap schema.
    {
        let c = open(&path);
        c.execute(
            "CREATE TABLE agents (id INTEGER PRIMARY KEY AUTOINCREMENT, \
             slug TEXT NOT NULL UNIQUE, name TEXT NOT NULL)",
        )
        .unwrap();
    }

    // Connection A: insert a row.
    {
        let c = open(&path);
        c.execute(
            "INSERT INTO agents(slug,name) VALUES('test','ConnA') \
             ON CONFLICT(slug) DO UPDATE SET name=excluded.name",
        )
        .unwrap();

        let rows = c
            .query("SELECT COUNT(*) FROM agents WHERE slug='test'")
            .unwrap();
        assert_eq!(col(&rows, 0, 0), "1", "connA: should have 1 row");
    }

    // Connection B: same upsert — should hit ON CONFLICT, not insert a duplicate.
    {
        let c = open(&path);
        c.execute(
            "INSERT INTO agents(slug,name) VALUES('test','ConnB') \
             ON CONFLICT(slug) DO UPDATE SET name=excluded.name",
        )
        .unwrap();

        let rows = c
            .query("SELECT COUNT(*) FROM agents WHERE slug='test'")
            .unwrap();
        assert_eq!(
            col(&rows, 0, 0),
            "1",
            "cross-conn: should still have exactly 1 row, not a duplicate"
        );

        let rows = c
            .query("SELECT name FROM agents WHERE slug='test'")
            .unwrap();
        assert_eq!(
            col(&rows, 0, 0),
            "ConnB",
            "cross-conn: name should be updated to ConnB"
        );
    }
}

/// Cross-connection with DO NOTHING: duplicate should be silently skipped.
#[test]
fn cross_conn_do_nothing_skips_duplicate() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("cross_nothing.db")
        .to_string_lossy()
        .to_string();

    {
        let c = open(&path);
        c.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)")
            .unwrap();
        c.execute("INSERT INTO kv VALUES ('key1', 100)").unwrap();
    }

    {
        let c = open(&path);
        c.execute("INSERT INTO kv VALUES ('key1', 200) ON CONFLICT DO NOTHING")
            .unwrap();

        let rows = c.query("SELECT COUNT(*) FROM kv WHERE k='key1'").unwrap();
        assert_eq!(col(&rows, 0, 0), "1", "DO NOTHING: should still have 1 row");

        let rows = c.query("SELECT v FROM kv WHERE k='key1'").unwrap();
        assert_eq!(
            col(&rows, 0, 0),
            "100",
            "DO NOTHING: original value preserved"
        );
    }
}

/// Cross-connection: three sequential connections, each upserting the same key.
#[test]
fn cross_conn_three_connections_same_key() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("three.db").to_string_lossy().to_string();

    {
        let c = open(&path);
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, slug TEXT UNIQUE, val INTEGER)")
            .unwrap();
    }

    for i in 1..=3 {
        let c = open(&path);
        c.execute(&format!(
            "INSERT INTO t(slug,val) VALUES('x',{i}) \
             ON CONFLICT(slug) DO UPDATE SET val=excluded.val"
        ))
        .unwrap();

        let rows = c.query("SELECT COUNT(*) FROM t WHERE slug='x'").unwrap();
        assert_eq!(
            col(&rows, 0, 0),
            "1",
            "iteration {i}: should always have exactly 1 row"
        );

        let rows = c.query("SELECT val FROM t WHERE slug='x'").unwrap();
        assert_eq!(
            col(&rows, 0, 0),
            format!("{i}"),
            "iteration {i}: val should be {i}"
        );
    }
}
