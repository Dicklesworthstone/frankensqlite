#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! WITHOUT ROWID physical-layout keepers checked against stock SQLite:
//! PK-overlapping UNIQUE autoindexes and repeated WITHOUT ROWID migration
//! churn (create/copy/drop/rename/reindex).
//!
//! Salvaged from an unmerged codex working copy
//! (`frankensqlite_codex_fk_depth_20260728`, 2026-07-29); expectations were
//! re-verified against stock SQLite 3.46.1.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

fn row_values(row: &Row) -> Vec<SqliteValue> {
    row.values().to_vec()
}

#[test]
fn test_without_rowid_hfdt_overlapping_unique_round_trips_sparse_ordinal() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let fsql_path = dir.path().join("without_rowid_hfdt_fsql_created.db");
        let fsql_path_str = fsql_path.to_string_lossy().into_owned();

        {
            let conn = Connection::open(&fsql_path_str).await.unwrap();
            conn.execute(
                "CREATE TABLE wr (
                     id TEXT PRIMARY KEY,
                     value TEXT NOT NULL,
                     UNIQUE(id, value)
                 ) WITHOUT ROWID;
                 INSERT INTO wr VALUES ('id-b', 'beta'), ('id-a', 'alpha');",
            )
            .await
            .unwrap();
        }

        {
            let sqlite = rusqlite::Connection::open(&fsql_path).unwrap();
            let index_names = sqlite
                .prepare(
                    "SELECT name
                     FROM sqlite_schema
                     WHERE type = 'index' AND tbl_name = 'wr'
                     ORDER BY name;",
                )
                .unwrap()
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(index_names, vec!["sqlite_autoindex_wr_2".to_owned()]);
            let index_terms = sqlite
                .prepare("PRAGMA index_xinfo('sqlite_autoindex_wr_2');")
                .unwrap()
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(5)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                index_terms,
                vec![(0, "id".to_owned(), 1), (1, "value".to_owned(), 1)]
            );
            let integrity: String = sqlite
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(integrity, "ok");
            let quick: String = sqlite
                .query_row("PRAGMA quick_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(quick, "ok");
            let forced_value: String = sqlite
                .query_row(
                    "SELECT value
                     FROM wr INDEXED BY sqlite_autoindex_wr_2
                     WHERE id = 'id-a' AND value = 'alpha';",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(forced_value, "alpha");
        }

        {
            let conn = Connection::open(&fsql_path_str).await.unwrap();
            let index_names = conn
                .query("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'wr';")
                .await
                .expect("HFDT-shaped WITHOUT ROWID table should reload");
            assert_eq!(
                index_names.iter().map(row_values).collect::<Vec<_>>(),
                vec![vec![SqliteValue::Text("sqlite_autoindex_wr_2".into())]]
            );
            let index_columns = conn
                .query("PRAGMA index_info(sqlite_autoindex_wr_2);")
                .await
                .unwrap()
                .iter()
                .map(|row| match &row.values()[2] {
                    SqliteValue::Text(name) => name.to_string(),
                    other => panic!("index_info name must be TEXT: {other:?}"),
                })
                .collect::<Vec<_>>();
            let expected_columns: Vec<String> = vec!["id".to_owned(), "value".to_owned()];
            assert_eq!(index_columns, expected_columns);
            conn.execute("INSERT INTO wr VALUES ('id-c', 'gamma');")
                .await
                .unwrap();
        }

        {
            let sqlite = rusqlite::Connection::open(&fsql_path).unwrap();
            let integrity: String = sqlite
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(integrity, "ok");
            let forced_count: i64 = sqlite
                .query_row(
                    "SELECT count(*)
                     FROM wr INDEXED BY sqlite_autoindex_wr_2
                     WHERE id IN ('id-a', 'id-b', 'id-c');",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(forced_count, 3);
        }

        let stock_path = dir.path().join("without_rowid_hfdt_stock_created.db");
        let stock_path_str = stock_path.to_string_lossy().into_owned();
        {
            let sqlite = rusqlite::Connection::open(&stock_path).unwrap();
            sqlite
                .execute_batch(
                    "CREATE TABLE wr (
                         id TEXT PRIMARY KEY,
                         value TEXT NOT NULL,
                         UNIQUE(id, value)
                     ) WITHOUT ROWID;
                     INSERT INTO wr VALUES ('id-b', 'beta'), ('id-a', 'alpha');",
                )
                .unwrap();
        }
        {
            let conn = Connection::open(&stock_path_str).await.unwrap();
            let rows = conn
                .query(
                    "SELECT id, value
                     FROM wr INDEXED BY sqlite_autoindex_wr_2
                     ORDER BY id;",
                )
                .await
                .expect("FSQL must read stock's overlapping-PK secondary-index layout");
            assert_eq!(
                rows.iter().map(row_values).collect::<Vec<_>>(),
                vec![
                    vec![
                        SqliteValue::Text("id-a".into()),
                        SqliteValue::Text("alpha".into()),
                    ],
                    vec![
                        SqliteValue::Text("id-b".into()),
                        SqliteValue::Text("beta".into()),
                    ],
                ]
            );
            conn.execute("INSERT INTO wr VALUES ('id-c', 'gamma');")
                .await
                .unwrap();
        }
        {
            let sqlite = rusqlite::Connection::open(&stock_path).unwrap();
            let integrity: String = sqlite
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(integrity, "ok");
            let quick: String = sqlite
                .query_row("PRAGMA quick_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(quick, "ok");
            let forced_count: i64 = sqlite
                .query_row(
                    "SELECT count(*)
                     FROM wr INDEXED BY sqlite_autoindex_wr_2
                     WHERE id IN ('id-a', 'id-b', 'id-c');",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(forced_count, 3);
        }
    });
}

#[test]
fn without_rowid_migration_churn_keeps_every_auxiliary_root_accounted_for() {
    asupersync::test_utils::run_test(|| async {
        const ROW_COUNT: i64 = 128;
        const MIGRATION_ROUNDS: i64 = 6;

        let directory = tempfile::tempdir().expect("create DDL churn tempdir");
        let path = directory.path().join("without-rowid-ddl-churn.db");
        let path_text = path.to_string_lossy().into_owned();

        let assert_stock_page_accounting = |expected_revision: i64| {
            let oracle =
                rusqlite::Connection::open(&path).expect("open churn database with stock SQLite");
            let integrity_rows = oracle
                .prepare("PRAGMA integrity_check")
                .expect("prepare stock integrity_check")
                .query_map([], |row| row.get::<_, String>(0))
                .expect("run stock integrity_check")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect stock integrity_check");
            assert_eq!(
                integrity_rows,
                vec!["ok".to_owned()],
                "every allocated page must remain owned or present on the freelist"
            );
            let quick_rows = oracle
                .prepare("PRAGMA quick_check")
                .expect("prepare stock quick_check")
                .query_map([], |row| row.get::<_, String>(0))
                .expect("run stock quick_check")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect stock quick_check");
            assert_eq!(quick_rows, vec!["ok".to_owned()]);

            let row_count: i64 = oracle
                .query_row("SELECT count(*) FROM capture", [], |row| row.get(0))
                .expect("count migrated rows");
            let min_revision: i64 = oracle
                .query_row("SELECT min(revision) FROM capture", [], |row| row.get(0))
                .expect("read minimum migrated revision");
            let max_revision: i64 = oracle
                .query_row("SELECT max(revision) FROM capture", [], |row| row.get(0))
                .expect("read maximum migrated revision");
            assert_eq!(row_count, ROW_COUNT);
            assert_eq!(min_revision, expected_revision);
            assert_eq!(max_revision, expected_revision);

            let index_names = oracle
                .prepare(
                    "SELECT name
                     FROM sqlite_schema
                     WHERE type = 'index' AND tbl_name = 'capture'
                     ORDER BY name",
                )
                .expect("prepare stock index inventory")
                .query_map([], |row| row.get::<_, String>(0))
                .expect("read stock index inventory")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect stock index inventory");
            assert_eq!(
                index_names,
                vec![
                    "idx_capture_route".to_owned(),
                    "sqlite_autoindex_capture_2".to_owned(),
                    "sqlite_autoindex_capture_3".to_owned(),
                    "sqlite_autoindex_capture_4".to_owned(),
                ],
                "the hidden WITHOUT ROWID primary key must consume ordinal 1 without owning a separate root"
            );

            oracle
                .query_row("PRAGMA page_count", [], |row| row.get::<_, i64>(0))
                .expect("read page_count")
        };

        {
            let conn = Connection::open(&path_text)
                .await
                .expect("open initial FrankenSQLite database");
            conn.execute(
                "CREATE TABLE capture(
                     id TEXT PRIMARY KEY,
                     filing_id TEXT NOT NULL,
                     accession TEXT NOT NULL,
                     venue TEXT NOT NULL,
                     revision INTEGER NOT NULL,
                     payload TEXT,
                     UNIQUE(id, filing_id),
                     UNIQUE(accession),
                     UNIQUE(filing_id, accession)
                 ) WITHOUT ROWID;
                 CREATE INDEX idx_capture_route
                     ON capture(venue DESC, filing_id);",
            )
            .await
            .expect("create migration source schema");
            conn.execute("BEGIN")
                .await
                .expect("begin source seed transaction");
            for id in 0..ROW_COUNT {
                conn.execute_with_params(
                    "INSERT INTO capture
                         (id, filing_id, accession, venue, revision, payload)
                     VALUES (?1, ?2, ?3, ?4, 0, ?5)",
                    &[
                        SqliteValue::Text(format!("id-{id:04}").into()),
                        SqliteValue::Text(format!("filing-{id:04}").into()),
                        SqliteValue::Text(format!("accession-{id:04}").into()),
                        SqliteValue::Text(format!("venue-{}", id % 7).into()),
                        SqliteValue::Text(format!("payload-{id:04}").into()),
                    ],
                )
                .await
                .expect("seed source row");
            }
            conn.execute("COMMIT")
                .await
                .expect("commit source seed transaction");
            conn.close().await.expect("close initial database");
        }
        assert_stock_page_accounting(0);
        let mut page_counts = Vec::new();

        for migration_round in 1..=MIGRATION_ROUNDS {
            let staging_table = format!("capture_next_{migration_round}");
            let conn = Connection::open(&path_text)
                .await
                .expect("reopen database for migration round");
            conn.execute("BEGIN IMMEDIATE")
                .await
                .expect("begin migration transaction");
            conn.execute(&format!(
                "CREATE TABLE {staging_table}(
                     id TEXT PRIMARY KEY,
                     filing_id TEXT NOT NULL,
                     accession TEXT NOT NULL,
                     venue TEXT NOT NULL,
                     revision INTEGER NOT NULL,
                     payload TEXT,
                     UNIQUE(id, filing_id),
                     UNIQUE(accession),
                     UNIQUE(filing_id, accession)
                 ) WITHOUT ROWID"
            ))
            .await
            .expect("create replacement WITHOUT ROWID table");
            conn.execute(&format!(
                "INSERT INTO {staging_table}
                     (id, filing_id, accession, venue, revision, payload)
                 SELECT id, filing_id, accession, venue, revision + 1, payload
                 FROM capture"
            ))
            .await
            .expect("copy rows into replacement table");
            conn.execute("DROP INDEX idx_capture_route")
                .await
                .expect("drop prior migration index");
            conn.execute("DROP TABLE capture")
                .await
                .expect("drop prior WITHOUT ROWID table");
            conn.execute(&format!("ALTER TABLE {staging_table} RENAME TO capture"))
                .await
                .expect("publish replacement table");
            conn.execute(
                "CREATE INDEX idx_capture_route
                     ON capture(venue DESC, filing_id)",
            )
            .await
            .expect("create replacement routing index");
            conn.execute("COMMIT")
                .await
                .expect("commit migration transaction");
            conn.close().await.expect("close migrated database");

            page_counts.push(assert_stock_page_accounting(migration_round));
        }
        // Each round frees exactly what the next one allocates, so once the
        // first copy exists the file must stop growing: stock SQLite stays at
        // the same page_count for every later round. A per-round leak that
        // lands on the freelist passes integrity_check but grows the file.
        assert!(
            page_counts.windows(2).all(|pair| pair[1] <= pair[0]),
            "WITHOUT ROWID migration churn keeps growing the file: {page_counts:?}"
        );
    });
}
