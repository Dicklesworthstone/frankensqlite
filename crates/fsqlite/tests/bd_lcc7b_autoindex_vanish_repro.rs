//! bd-lcc7b / GH#374 repro: the `sqlite_autoindex_conversations_1` master row
//! vanished on a field cass archive while `PRAGMA index_list` still reported the
//! implicit autoindex — "sqlite_master is missing implicit autoindex slot 1".
//!
//! Forensics on the 2.5 GB artifact showed:
//!  - the `conversations` CREATE TABLE row is present WITH inline
//!    `UNIQUE(source_id, agent_id, external_id)`,
//!  - NO `sqlite_autoindex_conversations_1` master row and NO autoindex btree
//!    allocated (rootpage sequence is dense with no slot for it),
//!  - a separately-created named unique index `idx_conversations_provenance` on
//!    the SAME columns occupies the rowid/rootpage the autoindex would have had.
//!
//! cass's own schema carries the note: "That ALTER path can duplicate provenance
//! autoindex state in frankensqlite when the named unique provenance index
//! already exists." So the trigger class is: inline-UNIQUE autoindex + a named
//! UNIQUE index on the identical columns + ALTER TABLE ADD COLUMN.
//!
//! These tests reproduce the candidate sequences on the current engine and
//! assert the implicit autoindex master row survives (present in sqlite_master),
//! reopen succeeds, and integrity_check is clean.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;
use tempfile::TempDir;

fn text(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        SqliteValue::Null => "<null>".to_string(),
        SqliteValue::Integer(i) => i.to_string(),
        SqliteValue::Float(r) => r.to_string(),
        SqliteValue::Blob(_) => "<blob>".to_string(),
    }
}

async fn dump_master(conn: &Connection, label: &str) -> Vec<(String, String, String)> {
    let rows = conn
        .query(
            "SELECT type, name, tbl_name FROM sqlite_master \
             WHERE tbl_name='conversations' ORDER BY rowid;",
        )
        .await
        .unwrap();
    eprintln!("--- sqlite_master (conversations) @ {label} ---");
    let mut out = Vec::new();
    for r in &rows {
        let v = r.values();
        let (t, n, tb) = (text(&v[0]), text(&v[1]), text(&v[2]));
        eprintln!("  type={t:<6} name={n:<40} tbl={tb}");
        out.push((t, n, tb));
    }
    out
}

async fn autoindex_present(conn: &Connection) -> bool {
    let rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type='index' AND name='sqlite_autoindex_conversations_1';",
        )
        .await
        .unwrap();
    matches!(rows[0].values()[0], SqliteValue::Integer(n) if n >= 1)
}

async fn integrity(conn: &Connection) -> String {
    let rows = conn.query("PRAGMA integrity_check;").await.unwrap();
    rows.iter()
        .map(|r| text(&r.values()[0]))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Simplest collision: inline UNIQUE autoindex + named UNIQUE index on the same
/// columns, then ADD COLUMN.
#[test]
fn lcc7b_inline_unique_plus_named_unique_index_then_add_column() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("collision.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = Connection::open(&p).await.unwrap();
            conn.execute(
                "CREATE TABLE conversations(
                    id INTEGER PRIMARY KEY,
                    source_id TEXT,
                    agent_id INTEGER,
                    external_id TEXT,
                    UNIQUE(source_id, agent_id, external_id)
                 );",
            )
            .await
            .unwrap();
            assert!(
                autoindex_present(&conn).await,
                "autoindex missing right after CREATE TABLE"
            );
            dump_master(&conn, "after CREATE TABLE").await;

            conn.execute(
                "CREATE UNIQUE INDEX idx_conversations_provenance \
                 ON conversations(source_id, agent_id, external_id);",
            )
            .await
            .unwrap();
            dump_master(&conn, "after CREATE UNIQUE INDEX").await;
            assert!(
                autoindex_present(&conn).await,
                "autoindex vanished after CREATE UNIQUE INDEX on same columns"
            );

            conn.execute("ALTER TABLE conversations ADD COLUMN metadata_bin BLOB;")
                .await
                .unwrap();
            dump_master(&conn, "after ADD COLUMN metadata_bin").await;
            assert!(
                autoindex_present(&conn).await,
                "REPRO: autoindex master row vanished after ADD COLUMN (index_list still reports it)"
            );

            conn.execute("ALTER TABLE conversations ADD COLUMN origin_host TEXT;")
                .await
                .unwrap();
            dump_master(&conn, "after ADD COLUMN origin_host").await;
            assert!(
                autoindex_present(&conn).await,
                "REPRO: autoindex vanished after 2nd ADD COLUMN"
            );

            eprintln!("integrity_check (in-conn): {}", integrity(&conn).await);
            conn.close().await.unwrap();
        }
        // Reopen: this is where the field DB failed ("missing implicit autoindex slot 1").
        let conn = Connection::open(&p).await.unwrap();
        let master = dump_master(&conn, "after REOPEN").await;
        eprintln!("integrity_check (reopen): {}", integrity(&conn).await);
        assert!(
            master
                .iter()
                .any(|(t, n, _)| t == "index" && n == "sqlite_autoindex_conversations_1"),
            "REPRO: sqlite_autoindex_conversations_1 missing after reopen"
        );
    });
}

/// Reopen the DB between every migration step, mimicking cass applying its
/// migrations across separate process runs (each `cass` invocation opens then
/// closes the archive). A schema reload via compat_persist happens at each open;
/// this exercises any load/reconcile interaction the single-connection tests miss.
#[test]
fn lcc7b_reopen_between_each_migration_step() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("reopen.db");
        let p = path.to_string_lossy().into_owned();

        // Run one statement in its own open/close cycle, then assert the
        // autoindex master row survives the reload.
        async fn step(p: &str, sql: &str, label: &str) {
            let conn = Connection::open(p).await.unwrap();
            conn.execute(sql).await.unwrap();
            let present = autoindex_present(&conn).await;
            dump_master(&conn, label).await;
            conn.close().await.unwrap();
            assert!(present, "REPRO: autoindex vanished during `{label}`");
            // Reopen fresh purely to re-run the compat_persist load path.
            let conn = Connection::open(p).await.unwrap();
            let present = autoindex_present(&conn).await;
            let integ = integrity(&conn).await;
            conn.close().await.unwrap();
            assert!(
                present,
                "REPRO: autoindex vanished after reopen following `{label}`"
            );
            assert_eq!(
                integ, "ok",
                "integrity_check not ok after reopen following `{label}`"
            );
        }

        // V1 base (no unique) — this open creates the DB.
        {
            let conn = Connection::open(&p).await.unwrap();
            conn.execute(
                "CREATE TABLE conversations(id INTEGER PRIMARY KEY, agent_id INTEGER, \
                 source_id TEXT, external_id TEXT, title TEXT);",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO conversations(id, agent_id, source_id, external_id) \
                          VALUES (1, 7, 'local', 'e1');",
            )
            .await
            .unwrap();
            conn.close().await.unwrap();
        }
        // V5 rebuild, each statement in its own run where feasible. The rebuild
        // block must be one connection (temp copy + drop + rename are coupled).
        {
            let conn = Connection::open(&p).await.unwrap();
            conn.execute(
                "CREATE TABLE conversations_new(id INTEGER PRIMARY KEY, agent_id INTEGER NOT NULL, \
                 source_id TEXT NOT NULL DEFAULT 'local', external_id TEXT, title TEXT, \
                 UNIQUE(source_id, agent_id, external_id));",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO conversations_new(id, agent_id, source_id, external_id, title) \
                          SELECT id, agent_id, 'local', external_id, title FROM conversations;",
            )
            .await
            .unwrap();
            conn.execute("DROP TABLE conversations;").await.unwrap();
            conn.execute("ALTER TABLE conversations_new RENAME TO conversations;")
                .await
                .unwrap();
            let present = autoindex_present(&conn).await;
            conn.close().await.unwrap();
            assert!(
                present,
                "REPRO: autoindex missing right after V5 rebuild block"
            );
            // reopen check
            let conn = Connection::open(&p).await.unwrap();
            assert!(
                autoindex_present(&conn).await,
                "REPRO: autoindex vanished after reopen post-V5"
            );
            assert_eq!(
                integrity(&conn).await,
                "ok",
                "integrity not ok after reopen post-V5"
            );
            conn.close().await.unwrap();
        }
        step(
            &p,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_provenance \
             ON conversations(source_id, agent_id, external_id);",
            "provenance index (own run)",
        )
        .await;
        step(
            &p,
            "ALTER TABLE conversations ADD COLUMN metadata_bin BLOB;",
            "add metadata_bin (own run)",
        )
        .await;
        step(
            &p,
            "ALTER TABLE conversations ADD COLUMN origin_host TEXT;",
            "add origin_host (own run)",
        )
        .await;
        step(
            &p,
            "ALTER TABLE conversations ADD COLUMN total_input_tokens INTEGER;",
            "add tokens (own run)",
        )
        .await;
    });
}

/// Faithful cass V5-style table rebuild: base table (no unique) -> new table WITH
/// inline UNIQUE -> copy -> DROP -> RENAME TO -> named UNIQUE index -> ADD COLUMNs.
#[test]
fn lcc7b_cass_v5_rebuild_then_provenance_index_then_add_columns() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("v5.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = Connection::open(&p).await.unwrap();
            // V1 base: no unique constraint on conversations.
            conn.execute(
                "CREATE TABLE conversations(
                    id INTEGER PRIMARY KEY,
                    agent_id INTEGER,
                    source_id TEXT,
                    external_id TEXT,
                    title TEXT
                 );",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO conversations(id, agent_id, source_id, external_id, title) \
                          VALUES (1, 7, 'local', 'ext-1', 't1'), (2, 7, 'local', 'ext-2', 't2');",
            )
            .await
            .unwrap();

            // V5: recreate table with UNIQUE constraint, copy, drop, rename.
            conn.execute(
                "CREATE TABLE conversations_new(
                    id INTEGER PRIMARY KEY,
                    agent_id INTEGER NOT NULL,
                    source_id TEXT NOT NULL DEFAULT 'local',
                    external_id TEXT,
                    title TEXT,
                    UNIQUE(source_id, agent_id, external_id)
                 );",
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO conversations_new(id, agent_id, source_id, external_id, title) \
                 SELECT id, agent_id, 'local', external_id, title FROM conversations;",
            )
            .await
            .unwrap();
            conn.execute("DROP TABLE conversations;").await.unwrap();
            conn.execute("ALTER TABLE conversations_new RENAME TO conversations;")
                .await
                .unwrap();
            dump_master(&conn, "after V5 RENAME TO").await;
            assert!(
                autoindex_present(&conn).await,
                "autoindex missing after V5 RENAME conversations_new -> conversations"
            );

            // Later migration: named unique index on the SAME columns.
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_provenance \
                 ON conversations(source_id, agent_id, external_id);",
            )
            .await
            .unwrap();
            dump_master(&conn, "after provenance index").await;
            assert!(
                autoindex_present(&conn).await,
                "autoindex vanished after provenance index"
            );

            // V7+ ADD COLUMN churn.
            for col in [
                "ADD COLUMN metadata_bin BLOB",
                "ADD COLUMN origin_host TEXT",
                "ADD COLUMN total_input_tokens INTEGER",
                "ADD COLUMN primary_model TEXT",
            ] {
                conn.execute(&format!("ALTER TABLE conversations {col};"))
                    .await
                    .unwrap();
                let ok = autoindex_present(&conn).await;
                dump_master(&conn, &format!("after {col}")).await;
                assert!(ok, "REPRO: autoindex master row vanished after `{col}`");
            }
            eprintln!("integrity_check (in-conn): {}", integrity(&conn).await);
            conn.close().await.unwrap();
        }
        let conn = Connection::open(&p).await.unwrap();
        let master = dump_master(&conn, "after REOPEN").await;
        eprintln!("integrity_check (reopen): {}", integrity(&conn).await);
        assert!(
            master
                .iter()
                .any(|(t, n, _)| t == "index" && n == "sqlite_autoindex_conversations_1"),
            "REPRO: sqlite_autoindex_conversations_1 missing after reopen (cass V5 path)"
        );
    });
}
