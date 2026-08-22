//! GH #209 (bd-c6jre): `ALTER TABLE <fts5> RENAME TO <new>` must rename the
//! FTS5 virtual table together with all five of its shadow tables
//! (`_config`, `_content`, `_data`, `_docsize`, `_idx`) and keep the table
//! fully functional — exactly like stock SQLite. FrankenSQLite previously
//! rejected the rename with `Unsupported`.
//!
//! Cross-checked against stock SQLite (rusqlite, which ships fts5):
//!  (a) `MATCH` on the NEW name returns the indexed row,
//!  (b) the OLD name (and its shadows) are gone,
//!  (c) the renamed shadow tables exist in sqlite_master,
//!  (d) a fresh reopen of a file-backed DB still `MATCH`es on the new name,
//!      and stock SQLite can read the renamed image,
//!  plus a control that a non-FTS5 rename still works and that an `INSERT`
//!  after the rename still updates the index.
//!
//! Requires `--features ext-fts5`.

#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Scalar `i64` from the first column of the first row.
async fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {e}"));
    match rows.first().map(|row| row.values().first().cloned()) {
        Some(Some(SqliteValue::Integer(n))) => n,
        other => panic!("expected integer scalar from `{sql}`, got {other:?}"),
    }
}

/// Sorted set of `type='table'` names visible in frank's sqlite_master.
async fn frank_table_names(conn: &Connection) -> Vec<String> {
    let mut names: Vec<String> = conn
        .query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .await
        .expect("query sqlite_master")
        .iter()
        .map(|row| match row.values().first() {
            Some(SqliteValue::Text(s)) => s.as_ref().to_owned(),
            other => panic!("expected text name, got {other:?}"),
        })
        .collect();
    names.sort();
    names
}

/// Sorted set of `type='table'` names visible in stock SQLite's sqlite_master.
fn stock_table_names(conn: &rusqlite::Connection) -> Vec<String> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap();
    let mut names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    names.sort();
    names
}

#[test]
fn fts5_rename_table_matches_stock_gh209() {
    asupersync::test_utils::run_test(|| async {
        // --- frank ---
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE ft USING fts5(a, b)")
            .await
            .expect("create fts5");
        conn.execute("INSERT INTO ft VALUES('hello', 'world')")
            .await
            .expect("insert row");

        conn.execute("ALTER TABLE ft RENAME TO ft2")
            .await
            .expect("RENAME TO ft2 must succeed (GH #209)");

        // (a) MATCH on the new name returns the row.
        assert_eq!(
            scalar_i64(&conn, "SELECT count(*) FROM ft2 WHERE ft2 MATCH 'hello'").await,
            1,
            "MATCH on the renamed fts5 table must find the indexed row"
        );

        // (b) The old name is gone.
        let old_lookup = conn
            .query("SELECT name FROM sqlite_master WHERE name='ft' AND type='table'")
            .await
            .unwrap();
        assert!(old_lookup.is_empty(), "old vtab name `ft` must be gone");
        assert!(
            conn.query("SELECT count(*) FROM ft WHERE ft MATCH 'hello'")
                .await
                .is_err(),
            "querying the old name must fail after rename"
        );

        // (c) The renamed shadow tables exist in sqlite_master.
        let frank_names = frank_table_names(&conn).await;
        for expected in [
            "ft2",
            "ft2_config",
            "ft2_content",
            "ft2_data",
            "ft2_docsize",
            "ft2_idx",
        ] {
            assert!(
                frank_names.iter().any(|n| n == expected),
                "expected `{expected}` in sqlite_master, got {frank_names:?}"
            );
        }
        // No `ft`-prefixed leftovers survive the rename.
        assert!(
            !frank_names
                .iter()
                .any(|n| n == "ft" || n.starts_with("ft_")),
            "no old `ft`/`ft_*` entries may remain, got {frank_names:?}"
        );

        // --- stock oracle: identical operations, identical catalog shape ---
        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch("CREATE VIRTUAL TABLE ft USING fts5(a, b);")
            .unwrap();
        r.execute("INSERT INTO ft VALUES('hello', 'world')", [])
            .unwrap();
        r.execute_batch("ALTER TABLE ft RENAME TO ft2;").unwrap();
        let stock_match: i64 = r
            .query_row(
                "SELECT count(*) FROM ft2 WHERE ft2 MATCH 'hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stock_match, 1, "stock MATCH sanity after rename");

        assert_eq!(
            frank_table_names(&conn).await,
            stock_table_names(&r),
            "frank's renamed catalog (table names) must equal stock SQLite's"
        );
    });
}

#[test]
fn fts5_rename_table_insert_after_rename_updates_index_gh209() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE ft USING fts5(a, b)")
            .await
            .unwrap();
        conn.execute("INSERT INTO ft VALUES('hello', 'world')")
            .await
            .unwrap();
        conn.execute("ALTER TABLE ft RENAME TO ft2").await.unwrap();

        // INSERT through the renamed table still updates the live index.
        conn.execute("INSERT INTO ft2 VALUES('fresh', 'document')")
            .await
            .expect("insert into renamed fts5 table");

        assert_eq!(
            scalar_i64(&conn, "SELECT count(*) FROM ft2 WHERE ft2 MATCH 'fresh'").await,
            1,
            "a row inserted after rename must be searchable"
        );
        assert_eq!(
            scalar_i64(&conn, "SELECT count(*) FROM ft2 WHERE ft2 MATCH 'hello'").await,
            1,
            "the pre-rename row must still be searchable"
        );
        assert_eq!(
            scalar_i64(&conn, "SELECT count(*) FROM ft2").await,
            2,
            "both rows must be present after rename + insert"
        );
    });
}

#[test]
fn fts5_rename_table_survives_reopen_gh209() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_rename_gh209.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Create, populate, rename, then close.
        {
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute("CREATE VIRTUAL TABLE ft USING fts5(a, b)")
                .await
                .unwrap();
            conn.execute("INSERT INTO ft VALUES('hello', 'world')")
                .await
                .unwrap();
            conn.execute("INSERT INTO ft VALUES('lorem', 'ipsum')")
                .await
                .unwrap();
            conn.execute("ALTER TABLE ft RENAME TO ft2").await.unwrap();
            // MATCH works before the reopen too.
            assert_eq!(
                scalar_i64(&conn, "SELECT count(*) FROM ft2 WHERE ft2 MATCH 'hello'").await,
                1
            );
            conn.close().await.unwrap();
        }

        // (d) A fresh reopen still MATCHes on the new name (lazy shadow bind).
        {
            let conn = Connection::open(&db_str).await.unwrap();
            assert_eq!(
                scalar_i64(&conn, "SELECT count(*) FROM ft2 WHERE ft2 MATCH 'hello'").await,
                1,
                "reopened DB must MATCH on the renamed table"
            );
            assert_eq!(
                scalar_i64(&conn, "SELECT count(*) FROM ft2 WHERE ft2 MATCH 'lorem'").await,
                1,
                "reopened DB must MATCH the second row too"
            );
            // Insert after reopen keeps working against the renamed shadows.
            conn.execute("INSERT INTO ft2 VALUES('reopened', 'row')")
                .await
                .expect("insert after reopen");
            assert_eq!(
                scalar_i64(&conn, "SELECT count(*) FROM ft2 WHERE ft2 MATCH 'reopened'").await,
                1,
                "insert after reopen must update the renamed index"
            );
            conn.close().await.unwrap();
        }

        // Stock SQLite reads the renamed on-disk image and MATCHes on it.
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check on renamed image");
        let stock_match: i64 = stock
            .query_row(
                "SELECT count(*) FROM ft2 WHERE ft2 MATCH 'hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            stock_match, 1,
            "stock SQLite must MATCH on the frank-renamed table"
        );
        // The old name must not exist for stock either.
        let stock_old: i64 = stock
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='ft' AND type='table'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stock_old, 0, "old vtab name must be absent on disk");
    });
}

#[test]
fn fts5_case_only_rename_matches_stock_message_bd_3nppp() {
    // bd-3nppp: `ALTER TABLE t RENAME TO T` (case-only) on an fts5 table must be
    // REJECTED with stock sqlite3's exact message and error code, not frank's
    // internal shadow-name preflight error ("internal error: table T_data
    // already exists" / SQLITE_INTERNAL). Both oracles agree and reject with
    // SQLITE_ERROR: "there is already another table or index with this name: T"
    // (verified against sqlite3 3.46.1 CLI and bundled 3.53.2 via rusqlite).
    // Stock never lets RENAME change a name's stored case. A non-fts5 control
    // asserts the ordinary rename path matches the same stock message.
    asupersync::test_utils::run_test(|| async {
        // --- stock oracle (bundled, fts5-capable) ---
        let r = rusqlite::Connection::open_in_memory().unwrap();
        r.execute_batch("CREATE VIRTUAL TABLE t USING fts5(a, b);")
            .unwrap();
        let stock_msg = r
            .execute_batch("ALTER TABLE t RENAME TO T;")
            .expect_err("stock must reject a case-only fts5 rename")
            .to_string();
        assert_eq!(
            stock_msg, "there is already another table or index with this name: T",
            "stock oracle message drifted"
        );

        // --- frank: same op, error must match stock verbatim ---
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(a, b)")
            .await
            .expect("create fts5");
        let frank_msg = conn
            .execute("ALTER TABLE t RENAME TO T")
            .await
            .expect_err("frank must reject a case-only fts5 rename")
            .to_string();
        assert_eq!(
            frank_msg, stock_msg,
            "frank's case-only fts5 rename error must match stock verbatim"
        );

        // The vtab + its shadows must be untouched (reject, not partial-apply).
        let names = frank_table_names(&conn).await;
        for expected in ["t", "t_config", "t_content", "t_data", "t_docsize", "t_idx"] {
            assert!(
                names.iter().any(|n| n == expected),
                "fts5 table + shadows must survive the rejected rename, got {names:?}"
            );
        }
        assert!(
            !names.iter().any(|n| n == "T" || n.starts_with("T_")),
            "no upper-cased catalog entries may appear after a rejected rename, got {names:?}"
        );

        // --- non-fts5 control: ordinary case-only rename, same stock message ---
        let r2 = rusqlite::Connection::open_in_memory().unwrap();
        r2.execute_batch("CREATE TABLE p(a, b);").unwrap();
        let stock_ctrl = r2
            .execute_batch("ALTER TABLE p RENAME TO P;")
            .expect_err("stock must reject a case-only ordinary rename")
            .to_string();
        assert_eq!(
            stock_ctrl,
            "there is already another table or index with this name: P"
        );
        let conn2 = Connection::open(":memory:").await.unwrap();
        conn2.execute("CREATE TABLE p(a, b)").await.unwrap();
        let frank_ctrl = conn2
            .execute("ALTER TABLE p RENAME TO P")
            .await
            .expect_err("frank must reject a case-only ordinary rename")
            .to_string();
        assert_eq!(
            frank_ctrl, stock_ctrl,
            "frank's ordinary case-only rename error must match stock verbatim"
        );
    });
}

#[test]
fn non_fts5_rename_still_works_gh209() {
    // Regression guard: narrowing the live-vtab rename guard to FTS5 must not
    // disturb ordinary table renames.
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE plain(x INTEGER, y TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO plain VALUES(1, 'one'), (2, 'two')")
            .await
            .unwrap();
        conn.execute("ALTER TABLE plain RENAME TO plain2")
            .await
            .expect("ordinary table rename must still work");

        assert_eq!(
            scalar_i64(&conn, "SELECT count(*) FROM plain2").await,
            2,
            "renamed ordinary table must retain its rows"
        );
        assert!(
            conn.query("SELECT * FROM plain").await.is_err(),
            "old ordinary-table name must be gone"
        );
    });
}
