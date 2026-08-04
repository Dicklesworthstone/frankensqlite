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
// Every test in this file drives full-stack engine operations through a
// non-`Send` `Connection`, so each `.await` holds a large, non-`Send` future by
// construction. Under the workspace's denied pedantic + nursery lint set that
// makes `clippy::large_futures` and `clippy::future_not_send` fire at every
// `Connection::open(..).await` here. `Box::pin` at each site would add an
// allocation to the exact code paths these regressions measure without changing
// anything they prove, so both lints are allowed at crate level instead. Both
// are required: a strict run of this target reported 6 `large_futures` and 2
// `future_not_send` errors.
#![allow(clippy::future_not_send, clippy::large_futures)]

use fsqlite::Connection;
use rusqlite::Connection as StockConnection;

async fn setup(conn: &Connection) {
    conn.execute(
        "CREATE TABLE commands (\
            id INTEGER PRIMARY KEY AUTOINCREMENT,\
            command TEXT NOT NULL\
        )",
    )
    .await
    .expect("create commands");
    conn.execute(
        "CREATE VIRTUAL TABLE commands_fts USING fts5(\
            command,\
            content='commands',\
            content_rowid='id'\
        )",
    )
    .await
    .expect("create external-content fts5");
    conn.execute(
        "CREATE TRIGGER commands_fts_insert AFTER INSERT ON commands BEGIN \
            INSERT INTO commands_fts(rowid, command) VALUES (new.id, new.command); \
        END",
    )
    .await
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
    .await
    .expect("create update trigger");
}

async fn match_rowids(conn: &Connection, term: &str) -> Vec<i64> {
    conn.query(&format!(
        "SELECT rowid FROM commands_fts WHERE commands_fts MATCH '{term}' ORDER BY rowid"
    ))
    .await
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
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        setup(&conn).await;

        conn.execute("INSERT INTO commands(command) VALUES ('first command')")
            .await
            .expect("1st insert");
        conn.execute("INSERT INTO commands(command) VALUES ('second command')")
            .await
            .expect("2nd insert");

        let res = conn
            .execute("UPDATE commands SET command = 'updated command' WHERE id = 1")
            .await;
        assert!(
            res.is_ok(),
            "UPDATE on FTS5-indexed table must not abort as malformed: {res:?}"
        );

        assert_eq!(match_rowids(&conn, "updated").await, vec![1]);
        assert!(match_rowids(&conn, "first").await.is_empty());
    });
}

/// On-disk variant: the `%_idx` shadow is persisted as a real index-structured
/// (WITHOUT ROWID) b-tree, so the shadow write path must open an index cursor.
/// This is the reporter's scenario for frankensqlite#121.
#[test]
fn update_fts_indexed_table_on_disk() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_owned();

        {
            let conn = Connection::open(&path).await.expect("open on-disk");
            setup(&conn).await;
            conn.execute("INSERT INTO commands(command) VALUES ('first command')")
                .await
                .expect("1st insert");
            conn.execute("INSERT INTO commands(command) VALUES ('second command')")
                .await
                .expect("2nd insert");

            let res = conn
                .execute("UPDATE commands SET command = 'updated command' WHERE id = 1")
                .await;
            assert!(
                res.is_ok(),
                "UPDATE on FTS5-indexed table (same connection) must not abort as malformed: {res:?}"
            );
            assert_eq!(match_rowids(&conn, "updated").await, vec![1]);
            assert!(match_rowids(&conn, "first").await.is_empty());
        }
    });
}

/// On-disk variant with a close/reopen cycle before the UPDATE: the `%_idx`
/// shadow b-tree is reloaded from disk pages, so the cursor binding is driven
/// purely off the persisted page type.
#[test]
fn update_fts_indexed_table_reopen_then_update() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_owned();

        {
            let conn = Connection::open(&path).await.expect("open on-disk");
            setup(&conn).await;
            conn.execute("INSERT INTO commands(command) VALUES ('first command')")
                .await
                .expect("1st insert");
            conn.execute("INSERT INTO commands(command) VALUES ('second command')")
                .await
                .expect("2nd insert");
        }

        let conn = Connection::open(&path).await.expect("reopen on-disk");
        let res = conn
            .execute("UPDATE commands SET command = 'updated command' WHERE id = 1")
            .await;
        assert!(
            res.is_ok(),
            "UPDATE on FTS5-indexed table (after reopen) must not abort as malformed: {res:?}"
        );
        assert_eq!(match_rowids(&conn, "updated").await, vec![1]);
        assert!(match_rowids(&conn, "first").await.is_empty());
    });
}

/// The reporter's actual scenario (frankensqlite#121): the store is created by
/// STOCK SQLite, where the FTS5 `%_idx` shadow is a genuine `WITHOUT ROWID`
/// (index-structured) b-tree. Frankensqlite then opens that canonically-valid
/// file and performs an in-place UPDATE, whose AFTER-UPDATE trigger drives
/// INSERT/DELETE into the shadow tables. The `%_idx` write must open an index
/// cursor, not a table cursor.
#[test]
fn update_stock_created_fts_indexed_table_does_not_abort() {
    asupersync::test_utils::run_test(|| async {
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
        let conn = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("frankensqlite open stock file");
        let res = conn
            .execute("UPDATE commands SET command = 'updated command' WHERE id = 1")
            .await;
        assert!(
            res.is_ok(),
            "UPDATE on stock-created FTS5-indexed table must not abort as malformed: {res:?}"
        );

        assert_eq!(match_rowids(&conn, "updated").await, vec![1]);
        assert!(match_rowids(&conn, "first").await.is_empty());
    });
}

/// GH#300 release proof: an FTS5 database *written by FrankenSQLite* must be
/// canonically valid to stock SQLite.
///
/// #300 reported the opposite direction from the tests above: FrankenSQLite
/// created `%_config` as a rowid table `(k TEXT PRIMARY KEY, v)` rather than
/// upstream's `(k PRIMARY KEY, v) WITHOUT ROWID`. The rowid form makes SQLite
/// materialize an implicit `sqlite_autoindex_<tbl>_config_1` whose entry count
/// diverges from the table, so stock `PRAGMA integrity_check` reports
/// `database disk image is malformed` on an otherwise-healthy database, and
/// even `SELECT count(*)` over the shadow fails. `%_idx` carried the same class
/// of deviation: no `PRIMARY KEY(segid, term)` and not `WITHOUT ROWID`.
///
/// This keeper pins the whole chain rather than just the DDL text, because the
/// schema string alone does not prove the on-disk b-tree is index-structured:
///
/// 1. the emitted `%_config` and `%_idx` schema carry `WITHOUT ROWID`;
/// 2. no `sqlite_autoindex_*_config_*` is materialized — the direct cause;
/// 3. the shadow is readable by stock SQLite (the reported `count(*)` failure);
/// 4. stock `PRAGMA integrity_check` returns exactly `ok`.
///
/// Assertion 4 is load-bearing: it is the exact command that failed in the
/// report, run by the exact engine that failed it. Assertions 1-3 exist so a
/// regression names its own cause instead of surfacing only as "malformed".
///
/// Deterministic: one writer, no threads, no retries. The FTS5 column set
/// mirrors the issue's upstream reference table so the shadow set has the same
/// shape that was reported broken.
#[test]
fn frankensqlite_written_fts5_is_canonical_to_stock_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_shadow_shape.db");
        let db_path_str = db_path.to_str().unwrap().to_owned();

        // Build the database entirely with FrankenSQLite.
        {
            let conn = Connection::open(&db_path_str)
                .await
                .expect("frankensqlite open for create");
            conn.execute(
                "CREATE VIRTUAL TABLE fts_messages USING fts5(\
                    content,\
                    title,\
                    tokenize='porter'\
                )",
            )
            .await
            .expect("create fts5 table");
            // Populate so every shadow carries real content: `%_data` and
            // `%_idx` only acquire segment rows once something is indexed,
            // while `%_config` is written at creation.
            conn.execute(
                "INSERT INTO fts_messages(content, title) \
                 VALUES ('the quick brown fox', 'first title')",
            )
            .await
            .expect("insert row 1");
            conn.execute(
                "INSERT INTO fts_messages(content, title) \
                 VALUES ('jumps over the lazy dog', 'second title')",
            )
            .await
            .expect("insert row 2");

            // The index must actually work before anything is asserted about
            // the file: a keeper that only proved "stock can read it" would
            // pass just as happily over an empty index.
            //
            // The MATCH is issued inline rather than through `match_rowids`,
            // which hardcodes the `commands_fts` table used by the tests above.
            // This keeper deliberately names its table `fts_messages` to mirror
            // the reference schema in GH#300.
            let matched = conn
                .query(
                    "SELECT rowid FROM fts_messages WHERE fts_messages MATCH 'quick' ORDER BY rowid",
                )
                .await
                .expect("FrankenSQLite MATCH over the freshly written FTS5 index");
            // `first()` rather than `[0]`: a row with no columns would panic on
            // direct indexing, whereas this surfaces as a clean `None` in the
            // assertion below, which also pins the rowid's type and value.
            let hits: Vec<Option<&fsqlite::SqliteValue>> =
                matched.iter().map(|row| row.values().first()).collect();
            assert_eq!(
                hits,
                vec![Some(&fsqlite::SqliteValue::Integer(1))],
                "FrankenSQLite MATCH must find the indexed row"
            );

            // Fold the WAL back so the stock reader below sees a complete image.
            //
            // The result is deliberately NOT discarded. If the checkpoint fails,
            // committed content can remain only in the `-wal` sidecar; stock
            // SQLite would then either read that sidecar or read a main file
            // missing the FTS5 writes, and `integrity_check` would be attesting
            // something other than the checkpointed main image this keeper
            // claims to prove. A failed checkpoint must fail the keeper rather
            // than silently weaken it into an opportunistic pass.
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                .await
                .expect("wal_checkpoint(TRUNCATE) must succeed before the stock integrity proof");

            // Close explicitly rather than relying on drop. Dropping the
            // connection emits "Connection dropped without explicit close()",
            // and more importantly it leaves shutdown work unawaited: the stock
            // reader below would then be racing whatever the drop path still
            // had to finish. Ordering matters here — the close follows the
            // checkpoint, so the durable image is already folded into the main
            // file before the handle goes away.
            conn.close().await.expect(
                "FrankenSQLite connection must close cleanly before the stock integrity proof",
            );
        }

        // Everything below runs on stock SQLite (rusqlite, bundled).
        let stock = StockConnection::open(&db_path).expect("stock open of FrankenSQLite file");

        // 1. Both key-structured shadows must declare WITHOUT ROWID.
        //
        //    Each schema is read with its own static-message `expect` rather
        //    than through a shared closure with a formatted fallback: a
        //    formatted `expect` argument would be constructed on every call
        //    even on success, and a `panic!` fallback reads as library-style
        //    panicking to the bug scanner. The table name lives in the static
        //    message instead, so diagnostics stay specific.
        let shadow_schema_query =
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1";

        let config_sql: String = stock
            .query_row(shadow_schema_query, ["fts_messages_config"], |row| {
                row.get(0)
            })
            .expect("stock could not read the %_config shadow schema");
        assert!(
            config_sql.to_ascii_uppercase().contains("WITHOUT ROWID"),
            "GH#300: %_config must be WITHOUT ROWID like upstream, got: {config_sql}"
        );

        let idx_sql: String = stock
            .query_row(shadow_schema_query, ["fts_messages_idx"], |row| row.get(0))
            .expect("stock could not read the %_idx shadow schema");
        assert!(
            idx_sql.to_ascii_uppercase().contains("WITHOUT ROWID"),
            "GH#300: %_idx must be WITHOUT ROWID like upstream, got: {idx_sql}"
        );
        assert!(
            idx_sql.to_ascii_uppercase().contains("PRIMARY KEY"),
            "GH#300: %_idx must declare PRIMARY KEY(segid, term), got: {idx_sql}"
        );

        // 2. No implicit autoindex over the config shadow: that object's entry
        //    count is what diverged and produced the malformed report.
        let autoindex_count: i64 = stock
            .query_row(
                "SELECT count(*) FROM sqlite_master \
                 WHERE type = 'index' AND name LIKE 'sqlite_autoindex_fts_messages_config%'",
                [],
                |row| row.get(0),
            )
            .expect("stock could not scan sqlite_master for autoindexes");
        assert_eq!(
            autoindex_count, 0,
            "GH#300: a WITHOUT ROWID %_config must not materialize sqlite_autoindex_*_config_*"
        );

        // 3. The shadow must be readable by stock SQLite; the report showed
        //    even a bare count failing with `malformed`.
        let config_rows: i64 = stock
            .query_row("SELECT count(*) FROM fts_messages_config", [], |row| {
                row.get(0)
            })
            .expect("GH#300: stock SELECT count(*) over %_config must not report malformed");
        assert!(
            config_rows > 0,
            "%_config should carry at least the format-version row, got {config_rows}"
        );

        // 4. The release proof: the exact command from the report, on the exact
        //    engine from the report.
        let verdict: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect(
                "stock integrity_check must execute over a FrankenSQLite-written FTS5 database",
            );
        assert_eq!(
            verdict, "ok",
            "GH#300: stock SQLite must report a FrankenSQLite-written FTS5 database as healthy"
        );
    });
}
