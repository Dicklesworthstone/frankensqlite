//! bd-sw2k5 — `PRAGMA journal_mode` round-trip for the rollback-journal modes.
//!
//! `journal_mode` accepts all of SQLite's modes; setting one returns the mode
//! that is now in effect. For the rollback-journal modes (delete / truncate /
//! persist / memory) the change is per-connection and NOT persisted, so a fresh
//! connection reports the default (`delete`) — matching stock SQLite, where only
//! `wal` survives a reopen (it is recorded in the database header).
//!
//! This guards the round-trip surface (AC #2). The per-mode durability behavior
//! (AC #1: truncate empties the journal, persist zeroes its header, memory keeps
//! it in RAM, off disables it) is the remaining pager-level work tracked on the
//! bead.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn journal_mode(conn: &Connection, set: Option<&str>) -> String {
    let sql = match set {
        Some(m) => format!("PRAGMA journal_mode={m};"),
        None => "PRAGMA journal_mode;".to_owned(),
    };
    let rows = conn.query(&sql).await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("journal_mode should be text, got {other:?}"),
    }
}

/// Each rollback-journal mode round-trips: setting it returns the same mode,
/// and a subsequent bare query still reports it within the same connection.
/// Uses a file-backed database — a `:memory:` database is pinned to `memory`
/// mode by SQLite and cannot be changed.
#[test]
fn journal_mode_rollback_modes_round_trip_bd_sw2k5() {
    asupersync::test_utils::run_test(|| async {
        for mode in ["delete", "truncate", "persist", "memory"] {
            let dir = tempfile::tempdir().unwrap();
            let db_str = dir
                .path()
                .join(format!("rt_{mode}.db"))
                .to_string_lossy()
                .into_owned();
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute("CREATE TABLE t(x);").await.unwrap();
            assert_eq!(
                journal_mode(&conn, Some(mode)).await,
                mode,
                "setting journal_mode={mode} should return {mode}"
            );
            assert_eq!(
                journal_mode(&conn, None).await,
                mode,
                "journal_mode readback should stay {mode} in-connection"
            );
        }
    });
}

/// A rollback-journal mode set on a file-backed database does NOT persist across
/// a reopen — a fresh connection reports the default `delete` (only `wal` is
/// recorded in the header and survives). Matches stock SQLite.
#[test]
fn journal_mode_rollback_mode_resets_on_reopen_bd_sw2k5() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_str = dir
            .path()
            .join("sw2k5.db")
            .to_string_lossy()
            .into_owned();

        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute("CREATE TABLE t(x);").await.unwrap();
            assert_eq!(journal_mode(&conn, Some("truncate")).await, "truncate");
            conn.close().await.unwrap();
        }

        let conn = Connection::open(&db_str).await.unwrap();
        assert_eq!(
            journal_mode(&conn, None).await,
            "delete",
            "a rollback-journal mode must reset to the default on reopen"
        );
    });
}
