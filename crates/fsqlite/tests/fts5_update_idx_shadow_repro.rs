//! Repro for frankensqlite#121: an in-place `UPDATE` on a table that carries
//! an external-content FTS5 index (kept in sync via an AFTER UPDATE trigger)
//! aborts with a false "database disk image is malformed" error:
//!
//! ```text
//! database disk image is malformed: table_seek called on index page
//! (type LeafIndex, page N, root N): cursor is_table flag likely incorrect
//! ```
//!
//! Root cause: the FTS5 AFTER-UPDATE trigger issues INSERT/DELETE against the
//! FTS5 shadow tables. The `%_idx` shadow is a `WITHOUT ROWID` (index-structured)
//! b-tree, but the write path opens a *table* cursor and drives it through
//! `table_seek_for_insert`, which then trips the `is_table` guard on the
//! index-structured root. The store is canonically valid; the engine mis-binds
//! the cursor.
//!
//! Run: `cargo test -p fsqlite --features fts5 --test fts5_update_idx_shadow_repro -- --nocapture`

#![cfg(feature = "fts5")]

use fsqlite::Connection;
use rusqlite::Connection as StockConnection;

fn setup(conn: &Connection) {
    conn.execute(
        "CREATE TABLE commands (\
            id INTEGER PRIMARY KEY AUTOINCREMENT,\
            command TEXT NOT NULL\
        )",
    )
    .expect("create commands");
    conn.execute(
        "CREATE VIRTUAL TABLE commands_fts USING fts5(\
            command,\
            content='commands',\
            content_rowid='id'\
        )",
    )
    .expect("create external-content fts5");
    conn.execute(
        "CREATE TRIGGER commands_fts_insert AFTER INSERT ON commands BEGIN \
            INSERT INTO commands_fts(rowid, command) VALUES (new.id, new.command); \
        END",
    )
    .expect("create insert trigger");
    // The canonical FTS5 external-content AFTER UPDATE trigger: delete the old
    // terms from the FTS index, then re-insert the new terms.
    conn.execute(
        "CREATE TRIGGER commands_fts_update AFTER UPDATE ON commands BEGIN \
            INSERT INTO commands_fts(commands_fts, rowid, command) \
                VALUES('delete', old.id, old.command); \
            INSERT INTO commands_fts(rowid, command) VALUES (new.id, new.command); \
        END",
    )
    .expect("create update trigger");
}

fn match_rowids(conn: &Connection, term: &str) -> Vec<i64> {
    conn.query(&format!(
        "SELECT rowid FROM commands_fts WHERE commands_fts MATCH '{term}' ORDER BY rowid"
    ))
    .expect("MATCH query")
    .iter()
    .map(|r| match &r.values()[0] {
        fsqlite::SqliteValue::Integer(i) => *i,
        other => panic!("unexpected rowid value: {other:?}"),
    })
    .collect()
}

/// In-memory variant (the shadow tables live at rootpage=0 in MemDatabase).
#[test]
fn update_fts_indexed_table_in_memory() {
    let conn = Connection::open(":memory:").expect("open");
    setup(&conn);

    conn.execute("INSERT INTO commands(command) VALUES ('first command')")
        .expect("1st insert");
    conn.execute("INSERT INTO commands(command) VALUES ('second command')")
        .expect("2nd insert");

    let res = conn.execute("UPDATE commands SET command = 'updated command' WHERE id = 1");
    assert!(
        res.is_ok(),
        "UPDATE on FTS5-indexed table must not abort as malformed: {res:?}"
    );

    assert_eq!(match_rowids(&conn, "updated"), vec![1]);
    assert!(match_rowids(&conn, "first").is_empty());
}

/// On-disk variant: the `%_idx` shadow is persisted as a real index-structured
/// (WITHOUT ROWID) b-tree, so the shadow write path must open an index cursor.
/// This is the reporter's scenario for frankensqlite#121.
#[test]
fn update_fts_indexed_table_on_disk() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap().to_owned();

    {
        let conn = Connection::open(&path).expect("open on-disk");
        setup(&conn);
        conn.execute("INSERT INTO commands(command) VALUES ('first command')")
            .expect("1st insert");
        conn.execute("INSERT INTO commands(command) VALUES ('second command')")
            .expect("2nd insert");

        let res = conn.execute("UPDATE commands SET command = 'updated command' WHERE id = 1");
        assert!(
            res.is_ok(),
            "UPDATE on FTS5-indexed table (same connection) must not abort as malformed: {res:?}"
        );
        assert_eq!(match_rowids(&conn, "updated"), vec![1]);
        assert!(match_rowids(&conn, "first").is_empty());
    }
}

/// On-disk variant with a close/reopen cycle before the UPDATE: the `%_idx`
/// shadow b-tree is reloaded from disk pages, so the cursor binding is driven
/// purely off the persisted page type.
#[test]
fn update_fts_indexed_table_reopen_then_update() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let path = tmp.path().to_str().unwrap().to_owned();

    {
        let conn = Connection::open(&path).expect("open on-disk");
        setup(&conn);
        conn.execute("INSERT INTO commands(command) VALUES ('first command')")
            .expect("1st insert");
        conn.execute("INSERT INTO commands(command) VALUES ('second command')")
            .expect("2nd insert");
    }

    let conn = Connection::open(&path).expect("reopen on-disk");
    let res = conn.execute("UPDATE commands SET command = 'updated command' WHERE id = 1");
    assert!(
        res.is_ok(),
        "UPDATE on FTS5-indexed table (after reopen) must not abort as malformed: {res:?}"
    );
    assert_eq!(match_rowids(&conn, "updated"), vec![1]);
    assert!(match_rowids(&conn, "first").is_empty());
}

/// The reporter's actual scenario (frankensqlite#121): the store is created by
/// STOCK SQLite, where the FTS5 `%_idx` shadow is a genuine `WITHOUT ROWID`
/// (index-structured) b-tree. Frankensqlite then opens that canonically-valid
/// file and performs an in-place UPDATE, whose AFTER-UPDATE trigger drives
/// INSERT/DELETE into the shadow tables. The `%_idx` write must open an index
/// cursor, not a table cursor.
#[test]
fn update_stock_created_fts_indexed_table_does_not_abort() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("commands.db");

    // Build the store with stock SQLite (rusqlite bundled).
    {
        let conn = StockConnection::open(&db_path).expect("open stock");
        conn.execute_batch(
            "CREATE TABLE commands(\
                id INTEGER PRIMARY KEY AUTOINCREMENT,\
                command TEXT NOT NULL\
             );\
             CREATE VIRTUAL TABLE commands_fts USING fts5(\
                command, content='commands', content_rowid='id'\
             );\
             CREATE TRIGGER commands_fts_insert AFTER INSERT ON commands BEGIN \
                INSERT INTO commands_fts(rowid, command) VALUES (new.id, new.command); \
             END;\
             CREATE TRIGGER commands_fts_update AFTER UPDATE ON commands BEGIN \
                INSERT INTO commands_fts(commands_fts, rowid, command) \
                    VALUES('delete', old.id, old.command); \
                INSERT INTO commands_fts(rowid, command) VALUES (new.id, new.command); \
             END;",
        )
        .expect("create stock schema");
        conn.execute("INSERT INTO commands(command) VALUES ('first command')", [])
            .expect("stock insert 1");
        conn.execute(
            "INSERT INTO commands(command) VALUES ('second command')",
            [],
        )
        .expect("stock insert 2");
        // Sanity: stock's own integrity_check passes.
        let ok: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .expect("integrity_check");
        assert_eq!(ok, "ok", "stock store must be canonically valid");
    }

    // Now open with frankensqlite and UPDATE.
    let conn = Connection::open(db_path.to_str().unwrap()).expect("frankensqlite open stock file");
    let res = conn.execute("UPDATE commands SET command = 'updated command' WHERE id = 1");
    assert!(
        res.is_ok(),
        "UPDATE on stock-created FTS5-indexed table must not abort as malformed: {res:?}"
    );

    assert_eq!(match_rowids(&conn, "updated"), vec![1]);
    assert!(match_rowids(&conn, "first").is_empty());
}
