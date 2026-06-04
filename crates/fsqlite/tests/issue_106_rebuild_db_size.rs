//! Regression test for GitHub issue #106.
//!
//! A single connection (no concurrency) runs a table-rebuild transaction under
//! BEGIN EXCLUSIVE that creates and reads tables whose pages are not yet
//! committed. A streaming `INSERT ... SELECT` fast path previously opened an
//! inner committed read-only pager transaction and failed with a spurious:
//!   BusySnapshot { conflicting_pages: "page N > snapshot db_size M (latest: M)" }
//!
//! The root cause is that committed-pager scan/reload paths are not valid while
//! an explicit transaction owns uncommitted DDL and page allocations.

use fsqlite::Connection;

/// The classic table-rebuild pattern used by schema migrations:
///   BEGIN EXCLUSIVE
///   DROP TABLE IF EXISTS tmp; CREATE TABLE tmp(...);
///   INSERT INTO tmp SELECT ... FROM orig;
///   DROP TABLE orig; ALTER TABLE tmp RENAME TO orig;
///   COMMIT
/// repeated until the file must grow, then read back.
#[test]
fn rebuild_then_read_no_busy_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("issue106_rebuild_then_read.db");

    let conn = Connection::open(path.to_str().unwrap()).unwrap();

    conn.execute("CREATE TABLE orig (id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();

    // Seed enough rows that the rebuild touches several pages.
    for i in 0..200 {
        conn.execute(&format!(
            "INSERT INTO orig (id, payload) VALUES ({i}, 'payload-value-number-{i}')"
        ))
        .unwrap();
    }

    // Run the rebuild pattern several times. Each rebuild allocates a fresh
    // tmp table (growing the file) and frees the old `orig` pages, exercising
    // the freelist-reuse / EOF-allocation paths that can drop the live page
    // from the committed db_size.
    for round in 0..6 {
        conn.execute("BEGIN EXCLUSIVE").unwrap();
        conn.execute("DROP TABLE IF EXISTS tmp").unwrap();
        conn.execute("CREATE TABLE tmp (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        conn.execute("INSERT INTO tmp (id, payload) SELECT id, payload FROM orig")
            .unwrap();
        conn.execute("DROP TABLE orig").unwrap();
        conn.execute("ALTER TABLE tmp RENAME TO orig").unwrap();
        conn.execute("COMMIT").unwrap();

        // The very next read must not spuriously fail with BusySnapshot.
        let rows = conn
            .query("SELECT id, payload FROM orig ORDER BY id")
            .unwrap_or_else(|e| panic!("read after rebuild round {round} failed: {e:?}"));
        assert_eq!(rows.len(), 200, "row count after rebuild round {round}");
    }
}

/// Faithful mirror of beads_rust's `apply_schema` -> `run_pre_schema_migrations`
/// -> `rebuild_issues_table` sequence (GH #106 downstream repro), but in pure
/// fsqlite. The DB runs in the default rollback-journal mode (NOT WAL — beads
/// only switches to WAL at the very end of apply_schema). The rebuild grows the
/// file under BEGIN EXCLUSIVE, recreates the table under the SAME name, then a
/// burst of CREATE INDEX / CREATE TABLE statements grows the file further. The
/// next read must not fail with BusySnapshot.
#[test]
fn beads_like_rebuild_then_index_burst() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("issue106_beads_like_rebuild.db");

    let conn = Connection::open(path.to_str().unwrap()).unwrap();

    // Legacy two-column table, like a database created by an old br version.
    conn.execute("CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT NOT NULL)")
        .unwrap();
    for i in 0..40 {
        conn.execute(&format!(
            "INSERT INTO issues (id, title) VALUES ('id-{i}', 'title number {i}')"
        ))
        .unwrap();
    }

    // ---- rebuild_issues_table: BEGIN EXCLUSIVE ... COMMIT ----
    conn.execute("PRAGMA foreign_keys = OFF").unwrap();
    conn.execute("BEGIN EXCLUSIVE").unwrap();
    conn.execute("DROP TABLE IF EXISTS issues_rebuild_tmp")
        .unwrap();
    // Canonical wide table (mirrors the 38-column issues table).
    let wide_cols: String = (0..36)
        .map(|i| format!(", col{i} TEXT DEFAULT ''"))
        .collect();
    conn.execute(&format!(
        "CREATE TABLE issues_rebuild_tmp (id TEXT PRIMARY KEY, title TEXT NOT NULL{wide_cols})"
    ))
    .unwrap();
    conn.execute("INSERT INTO issues_rebuild_tmp (id, title) SELECT id, title FROM issues")
        .unwrap();
    conn.execute("DROP TABLE issues").unwrap();
    conn.execute(&format!(
        "CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT NOT NULL{wide_cols})"
    ))
    .unwrap();
    conn.execute("INSERT INTO issues (id, title) SELECT id, title FROM issues_rebuild_tmp")
        .unwrap();
    conn.execute("DROP TABLE issues_rebuild_tmp").unwrap();
    conn.execute("COMMIT").unwrap();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();

    // ---- execute_batch(SCHEMA_SQL): index + table burst that grows the file ----
    for i in 0..16 {
        conn.execute(&format!("CREATE INDEX idx_issues_c{i} ON issues(col{i})"))
            .unwrap_or_else(|e| panic!("create index {i} failed: {e:?}"));
    }
    for i in 0..6 {
        conn.execute(&format!(
            "CREATE TABLE aux{i} (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)"
        ))
        .unwrap_or_else(|e| panic!("create aux table {i} failed: {e:?}"));
    }

    // ---- the next reads must not spuriously BusySnapshot ----
    let info = conn.query("PRAGMA table_info(issues)").unwrap();
    assert!(info.len() >= 38, "issues should have all columns");
    let cnt = conn.query("SELECT COUNT(*) FROM issues").unwrap();
    assert_eq!(cnt.len(), 1);
    let rows = conn
        .query("SELECT id, title FROM issues ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 40);
}

/// Tighter variant: a single rebuild that grows the file from a small db_size,
/// then immediately scans the rebuilt table.
#[test]
fn single_rebuild_grow_then_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("issue106_single_rebuild.db");

    let conn = Connection::open(path.to_str().unwrap()).unwrap();

    conn.execute("CREATE TABLE orig (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..50 {
        conn.execute(&format!("INSERT INTO orig (id, v) VALUES ({i}, 'x{i}')"))
            .unwrap();
    }

    conn.execute("BEGIN EXCLUSIVE").unwrap();
    conn.execute("CREATE TABLE tmp (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO tmp (id, v) SELECT id, v FROM orig")
        .unwrap();
    conn.execute("DROP TABLE orig").unwrap();
    conn.execute("ALTER TABLE tmp RENAME TO orig").unwrap();
    conn.execute("COMMIT").unwrap();

    let rows = conn.query("SELECT id, v FROM orig ORDER BY id").unwrap();
    assert_eq!(rows.len(), 50);
}
