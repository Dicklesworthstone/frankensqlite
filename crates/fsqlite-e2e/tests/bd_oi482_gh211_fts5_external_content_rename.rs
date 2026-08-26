//! bd-oi482 / GitHub issue #211 — FTS5 external-content `'rebuild'` and
//! connection lifecycle across a content-table rename.
//!
//! An external-content FTS5 table `CREATE VIRTUAL TABLE ft USING fts5(body,
//! content='c', content_rowid='id')` keeps its column bodies in the SOURCE
//! table `c`. Stock SQLite does NOT rewrite the `content=` option on
//! `ALTER TABLE c RENAME TO c2`, so once `c` is gone every statement that reads
//! the FTS5 content raises the table-local error `no such table: main.c`, while
//! the rest of the database stays usable and renaming `c` back recovers.
//!
//! FrankenSQLite previously mapped the absent content table to a silent
//! `None`/empty (serving stale/empty rows and rebuilding to an empty index).
//! This keeper pins the stock-matching contract for the paths frank actually
//! round-trips today — the `'rebuild'` maintenance command and the connection
//! lifecycle — verified DIFFERENTIALLY against rusqlite (stock SQLite + FTS5):
//!   1. after `ALTER TABLE c RENAME TO c2`, `'rebuild'` errors
//!      `no such table: main.c` on BOTH engines (was: silent rebuild to empty);
//!   2. the connection stays usable (unrelated statements still succeed — the
//!      reload-poisoning face);
//!   3. a rowid-only MATCH still answers from the postings on BOTH engines;
//!   4. `ALTER TABLE c2 RENAME TO c` lets `'rebuild'` succeed again;
//!   5. reopening the database does not poison the connection bootstrap.
//!
//! NOTE (bd-c6jre / FTS5-S4.6): external-content column PROJECTION (a scan or a
//! MATCH that reads `body`) is a separate, in-progress rework — frank currently
//! returns NULL for those columns even with the content table present, so it can
//! neither serve nor error them the way stock does yet. That parity is tracked
//! there; this keeper deliberately does not assert on those shapes.
//!
//! Run: `cargo test -p fsqlite-e2e --test bd_oi482_gh211_fts5_external_content_rename`

use fsqlite::Connection;

/// Classify an execute/query error string. Stock and frank both surface
/// sqlite3_errmsg "no such table: main.c" for the absent external content
/// table `c` (match the content table `c`, exclude the FTS5 table name `ft`).
fn is_no_such_content_table(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("no such table")
        && (lower.contains("main.c") || lower.ends_with(": c"))
        && !lower.contains("ft")
}

async fn frank_exec_err(conn: &Connection, sql: &str) -> Option<String> {
    conn.execute(sql).await.err().map(|e| e.to_string())
}

fn stock_exec_err(conn: &rusqlite::Connection, sql: &str) -> Option<String> {
    conn.execute_batch(sql).err().map(|e| e.to_string())
}

/// Query returning the single first column of every row as i64 (for the
/// rowid-only MATCH shape both engines answer from postings).
async fn frank_rowids(conn: &Connection, sql: &str) -> Result<Vec<i64>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|r| match &r.values()[0] {
            fsqlite::SqliteValue::Integer(n) => *n,
            other => panic!("expected integer rowid, got {other:?}"),
        })
        .collect())
}

fn stock_rowids(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<i64>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let iter = stmt
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for r in iter {
        out.push(r.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

const SETUP: &str = "\
    CREATE TABLE c(id INTEGER PRIMARY KEY, body TEXT);\n\
    INSERT INTO c(id, body) VALUES (1, 'hello world'), (2, 'goodbye moon');\n\
    CREATE VIRTUAL TABLE ft USING fts5(body, content='c', content_rowid='id');\n\
    INSERT INTO ft(ft) VALUES('rebuild');";

const REBUILD: &str = "INSERT INTO ft(ft) VALUES('rebuild');";
const MATCH_ROWID: &str = "SELECT rowid FROM ft WHERE ft MATCH 'hello'";
const UNRELATED: &str = "SELECT 1";

#[test]
fn gh211_external_content_rename_rebuild_and_lifecycle_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let frank_path = dir.path().join("frank.db");
        let frank_str = frank_path.to_string_lossy().into_owned();

        // ---- STOCK ground truth (rusqlite, in-memory) -----------------
        let sqlite = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        sqlite.execute_batch(SETUP).expect("stock setup");
        // Baseline rebuild succeeds while `c` exists.
        assert!(
            stock_exec_err(&sqlite, REBUILD).is_none(),
            "sanity: stock rebuild succeeds while the content table exists"
        );
        sqlite
            .execute_batch("ALTER TABLE c RENAME TO c2;")
            .expect("stock rename");
        let stock_rebuild_err = stock_exec_err(&sqlite, REBUILD);
        let stock_unrelated_ok = stock_exec_err(&sqlite, UNRELATED).is_none();
        let stock_match_rowid = stock_rowids(&sqlite, MATCH_ROWID);
        sqlite
            .execute_batch("ALTER TABLE c2 RENAME TO c;")
            .expect("stock rename-back");
        let stock_rebuild_recovered = stock_exec_err(&sqlite, REBUILD);
        drop(sqlite);

        // ---- FRANK under test -----------------------------------------
        let conn = Connection::open(frank_str.clone())
            .await
            .expect("open frank");
        conn.execute(SETUP).await.expect("frank setup");
        assert!(
            frank_exec_err(&conn, REBUILD).await.is_none(),
            "frank rebuild succeeds while the content table exists"
        );
        conn.execute("ALTER TABLE c RENAME TO c2;")
            .await
            .expect("frank rename");
        let frank_rebuild_err = frank_exec_err(&conn, REBUILD).await;
        let frank_unrelated_ok = frank_exec_err(&conn, UNRELATED).await.is_none();
        let frank_match_rowid = frank_rowids(&conn, MATCH_ROWID).await;
        conn.execute("ALTER TABLE c2 RENAME TO c;")
            .await
            .expect("frank rename-back");
        let frank_rebuild_recovered = frank_exec_err(&conn, REBUILD).await;

        // ---- Differential assertions ----------------------------------
        // 1. Post-rename `'rebuild'` errors `no such table: main.c` on both.
        assert!(
            stock_rebuild_err
                .as_deref()
                .is_some_and(is_no_such_content_table),
            "sanity: stock `'rebuild'` errors `no such table: main.c` after the rename, got {stock_rebuild_err:?}"
        );
        assert!(
            frank_rebuild_err
                .as_deref()
                .is_some_and(is_no_such_content_table),
            "post-rename `'rebuild'` must error `no such table: main.c` like stock, got {frank_rebuild_err:?}"
        );
        // 2. The connection stays usable (reload-poisoning face).
        assert!(
            stock_unrelated_ok,
            "sanity: stock unrelated statement succeeds after rename"
        );
        assert!(
            frank_unrelated_ok,
            "an unrelated statement must still succeed after the rename (no connection poisoning)"
        );
        // 3. Rowid-only MATCH still answers from postings on both.
        assert_eq!(
            stock_match_rowid.as_deref().ok(),
            Some([1_i64].as_slice()),
            "sanity: stock rowid-only MATCH survives the missing content table"
        );
        assert_eq!(
            frank_match_rowid, stock_match_rowid,
            "rowid-only MATCH must match stock after the rename"
        );
        // 4. Rename-back recovers `'rebuild'` on both.
        assert!(
            stock_rebuild_recovered.is_none(),
            "sanity: stock rebuild recovers after rename-back"
        );
        assert!(
            frank_rebuild_recovered.is_none(),
            "rename-back must let `'rebuild'` succeed again, got {frank_rebuild_recovered:?}"
        );

        // ---- 5. Reopen must not poison the connection bootstrap -------
        drop(conn);
        let reopened = Connection::open(frank_str).await.expect("reopen frank");
        assert!(
            frank_exec_err(&reopened, UNRELATED).await.is_none(),
            "reopening the database must not poison the connection (SELECT 1 works)"
        );
    });
}
