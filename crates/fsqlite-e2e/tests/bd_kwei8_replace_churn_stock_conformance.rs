//! bd-kwei8 — INSERT OR REPLACE churn must leave a file canonical SQLite can
//! read *with the live WAL present* (no checkpoint first).
//!
//! PROD incident (jsm CLI, jeffreys-skills.md tickets 1982c4f5/6893fcbe):
//! `jsm sync`'s cache refresh does INSERT OR REPLACE churn of ~1KB rows keyed
//! by TEXT id. After a completed sync, stock `sqlite3` reports `database disk
//! image is malformed` on jsm.db while fsqlite itself reads the same file
//! fine. Physical signature (WAL parsed by hand): an interior table page
//! whose cell still references a leaf page that REPLACE-conflict deletion
//! drained to zero cells — the emptied leaf was never merged/detached, and
//! stock SQLite (which never persists an empty non-root leaf) rejects the
//! shape.
//!
//! Minimal trigger, found by bisection: a multi-leaf table btree (12 rows of
//! ~900-byte payloads) plus INSERT OR REPLACE of 4 keys. Plain DELETE,
//! DELETE-then-INSERT, and UPDATE of the same rows do NOT reproduce — only
//! the REPLACE conflict-resolution path leaves the drained leaf attached.
//! fsqlite's own `PRAGMA integrity_check`/`quick_check` also fail to flag the
//! shape, which is why the existing churn suites (which checkpoint before the
//! canonical cross-check, or only verify the committed main file) never saw
//! it.
#![recursion_limit = "512"]

use fsqlite_types::SqliteValue;
use tempfile::TempDir;

/// `PRAGMA quick_check` -> `Ok(())` when the single row is `"ok"`, else the
/// first reported corruption line.
async fn quick_check(conn: &fsqlite::Connection) -> Result<(), String> {
    let rows = conn
        .query("PRAGMA quick_check")
        .await
        .map_err(|e| e.to_string())?;
    match rows.first().map(|r| r.values().first().cloned()) {
        Some(Some(SqliteValue::Text(s))) => {
            let s = s.to_string();
            if s == "ok" { Ok(()) } else { Err(s) }
        }
        other => Err(format!("unexpected quick_check result: {other:?}")),
    }
}

/// Run the minimal REPLACE-churn workload and hand the resulting file —
/// including any live WAL — to canonical SQLite.
async fn replace_churn_then_stock_check(replaces: usize) {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("replace_churn.db");

    {
        let conn = fsqlite::Connection::open(path.to_string_lossy().as_ref())
            .await
            .expect("open fsqlite");
        conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY, payload TEXT)")
            .await
            .expect("create table");
        let payload_x = "x".repeat(900);
        for i in 0..12 {
            conn.execute(&format!("INSERT INTO t VALUES ('k{i:02}', '{payload_x}')"))
                .await
                .unwrap_or_else(|e| panic!("insert {i}: {e}"));
        }
        let payload_y = "y".repeat(900);
        for i in 0..replaces {
            conn.execute(&format!(
                "INSERT OR REPLACE INTO t VALUES ('k{i:02}', '{payload_y}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("replace {i}: {e}"));
        }

        // fsqlite's own view must be consistent...
        if let Err(msg) = quick_check(&conn).await {
            panic!("fsqlite quick_check after replace churn: {msg}");
        }
        let rows = conn.query("SELECT count(*) FROM t").await.expect("count");
        assert_eq!(
            rows.first().and_then(|r| r.values().first().cloned()),
            Some(SqliteValue::Integer(12)),
            "fsqlite row count after churn"
        );
    } // connection drop: passive checkpoint; a live WAL may (and does) remain

    // ...and canonical SQLite must accept the same file, live WAL and all.
    let cconn = rusqlite::Connection::open(&path).expect("open canonical");
    let ic: String = cconn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("canonical integrity_check");
    assert_eq!(
        ic, "ok",
        "canonical SQLite on fsqlite-written file (live WAL): {ic}"
    );
    let n: i64 = cconn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("canonical count");
    assert_eq!(n, 12, "canonical row count after churn");
}

/// The minimal production shape: 4 REPLACEs drain one leaf of a 4-leaf tree.
#[test]
fn replace_churn_minimal_stays_stock_readable_with_live_wal() {
    asupersync::test_utils::run_test(|| async {
        replace_churn_then_stock_check(4).await;
    });
}

/// The full jsm-sync shape: every row REPLACEd (cache refresh of all 12).
#[test]
fn replace_churn_full_refresh_stays_stock_readable_with_live_wal() {
    asupersync::test_utils::run_test(|| async {
        replace_churn_then_stock_check(12).await;
    });
}
