#![recursion_limit = "512"]

//! GH #353 (bd-5ava1): on a WITHOUT ROWID table, the auto-indexes for COMPOSITE
//! UNIQUE constraints are never maintained when the leading key column is shared
//! across rows — every inserted row is missing from every composite-UNIQUE
//! auto-index, and stock sqlite3 reports corruption. Single-column UNIQUE and
//! the PK are fine. The keeper writes the issue's EXACT reproducer with fsqlite
//! (file-backed, journal_mode=DELETE), then reopens the file with stock sqlite3
//! (rusqlite = bundled SQLite) and asserts `PRAGMA integrity_check` == 'ok'.

use fsqlite_core::connection::Connection;

/// stock sqlite3 (bundled via rusqlite) integrity_check on a file.
fn stock_integrity_check(path: &str) -> Vec<String> {
    let r = rusqlite::Connection::open(path).expect("stock sqlite3 opens the fsqlite file");
    let mut st = r.prepare("PRAGMA integrity_check").unwrap();
    st.query_map([], |row| row.get::<_, String>(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
}

/// The issue's exact schema.
const SCHEMA: &str = "CREATE TABLE members(
    intent_id       TEXT    NOT NULL,
    intent_ordinal  INTEGER NOT NULL,
    capture_ordinal INTEGER NOT NULL,
    xbrl_fact_id    TEXT    NOT NULL,
    capture_fact_id TEXT    NOT NULL,
    natural_key     TEXT    NOT NULL,
    persisted_at    TEXT    NOT NULL,
    PRIMARY KEY (intent_id, intent_ordinal),
    UNIQUE (intent_id, capture_ordinal),
    UNIQUE (intent_id, xbrl_fact_id),
    UNIQUE (capture_fact_id)
) WITHOUT ROWID";

#[test]
fn wr_composite_unique_autoindex_exact_reproducer_gh353() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("shape.sqlite").to_string_lossy().to_string();
        {
            let f = Connection::open(&db).await.unwrap();
            f.execute("PRAGMA journal_mode = DELETE").await.unwrap();
            f.execute(SCHEMA).await.unwrap();
            f.execute("CREATE INDEX idx_members_capture ON members(capture_ordinal)").await.unwrap();
            f.execute("CREATE INDEX idx_members_fact ON members(xbrl_fact_id)").await.unwrap();
            // 5 rows sharing ONE intent_id — composite UNIQUE leading column is
            // shared; only the trailing column varies (0..4).
            f.execute("BEGIN IMMEDIATE").await.unwrap();
            for i in 0..5 {
                f.execute(&format!(
                    "INSERT INTO members VALUES \
                     ('intent-A', {i}, {i}, 'xbrl-{i}', 'cap-{i}', 'nk-{i}', '2026-08-16')"
                ))
                .await
                .unwrap_or_else(|e| panic!("insert row {i}: {e:?}"));
            }
            f.execute("COMMIT").await.unwrap();
            f.close().await.expect("close so stock sqlite3 sees a consistent file");
        }
        let report = stock_integrity_check(&db);
        assert_eq!(
            report,
            vec!["ok".to_string()],
            "stock sqlite3 integrity_check on the fsqlite-written WITHOUT ROWID + composite-UNIQUE \
             table (shared leading key column) reported: {report:?}"
        );
    });
}

#[test]
fn iso_shared_leading_autocommit_wal_gh353() {
    // Isolation: shared leading composite-UNIQUE column, AUTOCOMMIT, default (WAL).
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("iso1.sqlite").to_string_lossy().to_string();
        {
            let f = Connection::open(&db).await.unwrap();
            f.execute("CREATE TABLE m (a TEXT, b INTEGER, c INTEGER, PRIMARY KEY (a, b), UNIQUE (a, c)) WITHOUT ROWID").await.unwrap();
            for i in 0..5 {
                f.execute(&format!("INSERT INTO m VALUES ('shared', {i}, {i})")).await.unwrap();
            }
            f.close().await.expect("close");
        }
        assert_eq!(stock_integrity_check(&db), vec!["ok".to_string()], "iso: shared-leading autocommit WAL");
    });
}

#[test]
fn iso_shared_leading_begin_immediate_wal_gh353() {
    // Isolation: shared leading, BEGIN IMMEDIATE, default (WAL).
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("iso2.sqlite").to_string_lossy().to_string();
        {
            let f = Connection::open(&db).await.unwrap();
            f.execute("CREATE TABLE m (a TEXT, b INTEGER, c INTEGER, PRIMARY KEY (a, b), UNIQUE (a, c)) WITHOUT ROWID").await.unwrap();
            f.execute("BEGIN IMMEDIATE").await.unwrap();
            for i in 0..5 {
                f.execute(&format!("INSERT INTO m VALUES ('shared', {i}, {i})")).await.unwrap();
            }
            f.execute("COMMIT").await.unwrap();
            f.close().await.expect("close");
        }
        assert_eq!(stock_integrity_check(&db), vec!["ok".to_string()], "iso: shared-leading BEGIN IMMEDIATE WAL");
    });
}

#[test]
fn iso_distinct_leading_begin_immediate_delete_gh353() {
    // Isolation: DISTINCT leading, BEGIN IMMEDIATE, journal_mode=DELETE.
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("iso3.sqlite").to_string_lossy().to_string();
        {
            let f = Connection::open(&db).await.unwrap();
            f.execute("PRAGMA journal_mode = DELETE").await.unwrap();
            f.execute("CREATE TABLE m (a TEXT, b INTEGER, c INTEGER, PRIMARY KEY (a, b), UNIQUE (a, c)) WITHOUT ROWID").await.unwrap();
            f.execute("BEGIN IMMEDIATE").await.unwrap();
            for i in 0..5 {
                f.execute(&format!("INSERT INTO m VALUES ('k{i}', {i}, {i})")).await.unwrap();
            }
            f.execute("COMMIT").await.unwrap();
            f.close().await.expect("close");
        }
        assert_eq!(stock_integrity_check(&db), vec!["ok".to_string()], "iso: distinct-leading BEGIN IMMEDIATE DELETE");
    });
}

#[test]
fn wr_single_col_unique_autoindex_control_gh353() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("single.sqlite").to_string_lossy().to_string();
        {
            let f = Connection::open(&db).await.unwrap();
            f.execute("PRAGMA journal_mode = DELETE").await.unwrap();
            // Control: single-column UNIQUE on a composite-PK WR table is clean.
            f.execute(
                "CREATE TABLE m (a TEXT, b INTEGER, u TEXT, PRIMARY KEY (a, b), UNIQUE (u)) WITHOUT ROWID",
            )
            .await
            .unwrap();
            f.execute("BEGIN IMMEDIATE").await.unwrap();
            for i in 0..5 {
                f.execute(&format!("INSERT INTO m VALUES ('shared', {i}, 'u-{i}')")).await.unwrap();
            }
            f.execute("COMMIT").await.unwrap();
            f.close().await.expect("close");
        }
        assert_eq!(stock_integrity_check(&db), vec!["ok".to_string()]);
    });
}
