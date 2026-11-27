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
fn vacuum_preserves_overlapping_without_rowid_index_fields() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        let copy = dir.path().join("copy.db");
        let stock = rusqlite::Connection::open(&source).unwrap();
        stock.execute_batch(
            "CREATE TABLE t(v TEXT,k TEXT,id INT,PRIMARY KEY(k COLLATE NOCASE DESC,id)) WITHOUT ROWID;
             CREATE INDEX partial ON t(k COLLATE NOCASE,id) WHERE v='include';
             CREATE INDEX expr ON t(k COLLATE BINARY,length(v));
             INSERT INTO t VALUES('include','a',1),('include','Z',2),('exclude','b',3);",
        ).unwrap();
        drop(stock);
        let db = Connection::open(source.to_str().unwrap()).await.unwrap();
        db.execute(&format!("VACUUM INTO '{}'", copy.display())).await.unwrap();
        db.close().await.unwrap();
        for path in [&source, &copy] {
            let stock = rusqlite::Connection::open(path).unwrap();
            let verdict: String = stock.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
            assert_eq!(verdict, "ok");
            for (sql, expected) in [
                ("SELECT group_concat(id) FROM (SELECT id FROM t INDEXED BY partial WHERE v='include' ORDER BY id)", "1,2"),
                ("SELECT group_concat(id) FROM (SELECT id FROM t INDEXED BY expr WHERE length(v)=7 ORDER BY id)", "1,2,3"),
            ] {
                let ids: String = stock.query_row(sql, [], |r| r.get(0)).unwrap();
                assert_eq!(ids, expected, "{sql}");
            }
        }
    });
}

#[test]
fn index_xinfo_uses_the_owning_tables_primary_key_declaration() {
    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let db = Connection::open(":memory:").await.unwrap();
        for sql in [
            "CREATE TABLE t(v TEXT,k TEXT,id INT,PRIMARY KEY(k COLLATE NOCASE DESC,id)) WITHOUT ROWID",
            "CREATE INDEX ix ON t(k COLLATE BINARY,v)",
            "CREATE INDEX iy ON t(id,k COLLATE NOCASE DESC)",
            "CREATE TEMP TABLE t(u TEXT,y TEXT,PRIMARY KEY(u COLLATE RTRIM)) WITHOUT ROWID",
            "CREATE INDEX temp.iz ON t(u COLLATE BINARY)",
            "SAVEPOINT s",
            "ALTER TABLE temp.t ADD COLUMN extra TEXT",
            "ROLLBACK TO s",
            "RELEASE s",
        ] {
            stock.execute_batch(sql).unwrap();
            db.execute(sql).await.unwrap();
        }
        for sql in [
            "PRAGMA main.index_xinfo(ix)", "PRAGMA main.index_xinfo(iy)",
            "PRAGMA main.index_xinfo(t)", "PRAGMA temp.index_xinfo(t)", "PRAGMA temp.index_xinfo(iz)",
            "SELECT sql FROM sqlite_temp_master WHERE type='table' AND name='t'",
            "SELECT sql FROM sqlite_temp_master WHERE type='index' AND name='iz'",
        ] {
            let mut stmt = stock.prepare(sql).unwrap();
            let n = stmt.column_count();
            let expected = stmt.query_map([], |r| {
                (0..n).map(|column| match r.get_ref(column)? {
                    rusqlite::types::ValueRef::Integer(i) => Ok(SqliteValue::Integer(i)),
                    rusqlite::types::ValueRef::Text(s) => Ok(SqliteValue::Text(std::str::from_utf8(s).unwrap().into())),
                    rusqlite::types::ValueRef::Null => Ok(SqliteValue::Null),
                    value => panic!("unexpected catalog value {value:?}"),
                }).collect::<rusqlite::Result<Vec<_>>>()
            }).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
            assert!(!expected.is_empty(), "{sql}");
            let actual = db.query(sql).await.unwrap();
            assert_eq!(actual.iter().map(row_values).collect::<Vec<_>>(), expected, "{sql}");
        }
        db.close().await.unwrap();
    });
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

fn stock_pragma_rows(stock: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = stock.prepare(sql).expect("stock prepare");
    let ncols = stmt.column_count();
    stmt.query_map([], |row| {
        (0..ncols)
            .map(|col| match row.get_ref(col)? {
                rusqlite::types::ValueRef::Integer(i) => Ok(SqliteValue::Integer(i)),
                rusqlite::types::ValueRef::Text(s) => {
                    Ok(SqliteValue::Text(std::str::from_utf8(s).unwrap().into()))
                }
                rusqlite::types::ValueRef::Null => Ok(SqliteValue::Null),
                other => panic!("unexpected pragma value {other:?}"),
            })
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .expect("stock query_map")
    .collect::<rusqlite::Result<Vec<_>>>()
    .expect("stock collect")
}

/// bd-l7z39: live stock 3.54.1 differential keeper for `PRAGMA index_xinfo`
/// and `PRAGMA index_info` across WITHOUT ROWID primary keys with table-level
/// COLLATE/DESC overrides, inline UNIQUE autoindexes, expression indexes,
/// column-level vs table-level COLLATE overrides, file-backed reopens, ATTACHed
/// schemas, and an ordinary rowid-table negative (which MUST keep cid=-1).
#[test]
fn bd_l7z39_index_xinfo_and_index_info_without_rowid_pk_collation_desc_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let stock_path = dir.path().join("stock_l7z39.db");
        let native_path = dir.path().join("native_l7z39.db");
        let stock_aux_path = dir.path().join("stock_l7z39_aux.db");
        let native_aux_path = dir.path().join("native_l7z39_aux.db");

        let ddls = [
            // Exact bd-l7z39 schema: WITHOUT ROWID PK with table-level COLLATE NOCASE DESC.
            "CREATE TABLE t(v TEXT, k TEXT, id INTEGER, PRIMARY KEY(k COLLATE NOCASE DESC, id)) WITHOUT ROWID;",
            "CREATE INDEX ix ON t(k COLLATE BINARY, v);",
            "CREATE INDEX iy ON t(id, k COLLATE NOCASE DESC);",
            "CREATE INDEX iz ON t(k COLLATE NOCASE ASC, id);",
            "CREATE INDEX iexpr ON t(lower(k), v);",
            // Column-level COLLATE RTRIM overridden by table-level PK COLLATE NOCASE DESC,
            // plus an inline UNIQUE constraint to verify autoindex locator layout.
            "CREATE TABLE t2(v TEXT UNIQUE, k TEXT COLLATE RTRIM, id INTEGER, PRIMARY KEY(k COLLATE NOCASE DESC, id)) WITHOUT ROWID;",
            "CREATE INDEX ix2 ON t2(k, v);",
            "CREATE INDEX iy2 ON t2(id, k COLLATE NOCASE DESC);",
            // Ordinary rowid-table negative: MUST still append trailing cid=-1 rowid in index_xinfo.
            "CREATE TABLE r(v TEXT, k TEXT, id INTEGER, PRIMARY KEY(k COLLATE NOCASE DESC, id));",
            "CREATE INDEX r_ix ON r(k COLLATE BINARY, v);",
            "CREATE INDEX r_iy ON r(id, k COLLATE NOCASE DESC);",
            // Seed rows (including two NULLs in t2.v so sqlite_autoindex_t2_1
            // must order by the appended PK column under SQLite's bAscKeyBug).
            "INSERT INTO t VALUES('a', 'b', 1), ('b', 'A', 2), ('c', 'c', 3);",
            "INSERT INTO t2 VALUES(NULL, 'b', 1), (NULL, 'A', 2), ('uniq', 'c', 3);",
        ];

        let probes = [
            // WITHOUT ROWID secondary indexes (index_xinfo + index_info)
            "PRAGMA index_xinfo(ix);",
            "PRAGMA index_info(ix);",
            "PRAGMA index_xinfo(iy);",
            "PRAGMA index_info(iy);",
            "PRAGMA index_xinfo(iz);",
            "PRAGMA index_info(iz);",
            "PRAGMA index_xinfo(iexpr);",
            "PRAGMA index_info(iexpr);",
            // WITHOUT ROWID PK by table name and autoindex name
            "PRAGMA index_xinfo(t);",
            "PRAGMA index_info(t);",
            "PRAGMA index_xinfo(sqlite_autoindex_t_1);",
            "PRAGMA index_info(sqlite_autoindex_t_1);",
            // Column-level vs table-level COLLATE override + inline UNIQUE autoindex
            "PRAGMA index_xinfo(ix2);",
            "PRAGMA index_info(ix2);",
            "PRAGMA index_xinfo(iy2);",
            "PRAGMA index_info(iy2);",
            "PRAGMA index_xinfo(t2);",
            "PRAGMA index_info(t2);",
            "PRAGMA index_xinfo(sqlite_autoindex_t2_1);",
            "PRAGMA index_info(sqlite_autoindex_t2_1);",
            "PRAGMA index_xinfo(sqlite_autoindex_t2_2);",
            "PRAGMA index_info(sqlite_autoindex_t2_2);",
            // Ordinary rowid-table negative
            "PRAGMA index_xinfo(r_ix);",
            "PRAGMA index_info(r_ix);",
            "PRAGMA index_xinfo(r_iy);",
            "PRAGMA index_info(r_iy);",
            "PRAGMA index_xinfo(sqlite_autoindex_r_1);",
            "PRAGMA index_info(sqlite_autoindex_r_1);",
        ];

        // Phase 1: create in file-backed databases, check live before close.
        let stock = rusqlite::Connection::open(&stock_path).expect("stock open");
        let native = Connection::open(native_path.to_str().expect("utf8"))
            .await
            .expect("native open");
        for ddl in ddls {
            stock.execute_batch(ddl).expect("stock ddl");
            native.execute(ddl).await.expect("native ddl");
        }
        // Verify rowid-table negative actually has cid=-1 while WITHOUT ROWID does NOT.
        let r_ix_xinfo = stock_pragma_rows(&stock, "PRAGMA index_xinfo(r_ix);");
        assert!(
            r_ix_xinfo
                .last()
                .is_some_and(|row| row.get(1) == Some(&SqliteValue::Integer(-1))),
            "rowid-table negative must end with cid=-1: {r_ix_xinfo:?}"
        );
        for wr_idx in ["PRAGMA index_xinfo(ix);", "PRAGMA index_xinfo(iy);"] {
            let wr_rows = stock_pragma_rows(&stock, wr_idx);
            assert!(
                wr_rows
                    .iter()
                    .all(|row| row.get(1) != Some(&SqliteValue::Integer(-1))),
                "WITHOUT ROWID {wr_idx} must never contain cid=-1: {wr_rows:?}"
            );
        }

        for sql in probes {
            let expected = stock_pragma_rows(&stock, sql);
            assert!(!expected.is_empty(), "stock returned no rows for {sql}");
            let actual: Vec<Vec<SqliteValue>> = native
                .query(sql)
                .await
                .expect("native query")
                .iter()
                .map(row_values)
                .collect();
            assert_eq!(actual, expected, "live create mismatch for {sql}");
        }
        drop(stock);
        native.close().await.expect("native close");

        // Phase 2: reopen BOTH fsqlite-created file AND stock-created file in fsqlite,
        // plus ATTACHed aux schema.
        for (label, path) in [
            ("reopen-native-file", &native_path),
            ("open-stock-file", &stock_path),
        ] {
            let stock_check = rusqlite::Connection::open(path).expect("stock reopen");
            let native_check = Connection::open(path.to_str().expect("utf8"))
                .await
                .expect("native reopen");
            for sql in probes {
                let expected = stock_pragma_rows(&stock_check, sql);
                let actual: Vec<Vec<SqliteValue>> = native_check
                    .query(sql)
                    .await
                    .expect("native reopen query")
                    .iter()
                    .map(row_values)
                    .collect();
                assert_eq!(actual, expected, "{label} mismatch for {sql}");
            }
            // Also verify PRAGMA integrity_check agrees ("ok") on stock-created
            // file opened in fsqlite AND after VACUUM INTO opened in stock!
            if label == "open-stock-file" {
                let ic: Vec<Vec<SqliteValue>> = native_check
                    .query("PRAGMA integrity_check;")
                    .await
                    .expect("native ic")
                    .iter()
                    .map(row_values)
                    .collect();
                assert_eq!(
                    ic,
                    vec![vec![SqliteValue::Text("ok".into())]],
                    "native integrity_check on stock-created file with bAscKeyBug UNIQUE autoindex"
                );
                let vacuum_copy = dir.path().join("vacuumed_from_stock.db");
                native_check
                    .execute(&format!("VACUUM INTO '{}';", vacuum_copy.display()))
                    .await
                    .expect("vacuum into");
                let stock_vacuum =
                    rusqlite::Connection::open(&vacuum_copy).expect("stock open vacuumed");
                let stock_ic = stock_pragma_rows(&stock_vacuum, "PRAGMA integrity_check;");
                assert_eq!(
                    stock_ic,
                    vec![vec![SqliteValue::Text("ok".into())]],
                    "stock integrity_check on fsqlite VACUUM INTO copy with bAscKeyBug UNIQUE autoindex"
                );
            }
            native_check.close().await.expect("close reopen");
        }

        // Phase 3: ATTACHed database + TEMP table coverage.
        let stock_outer = rusqlite::Connection::open(&stock_aux_path).expect("stock outer");
        let native_outer = Connection::open(native_aux_path.to_str().expect("utf8"))
            .await
            .expect("native outer");
        stock_outer
            .execute_batch(&format!(
                "ATTACH DATABASE '{}' AS aux; \
                 CREATE TEMP TABLE tw(v TEXT, k TEXT, id INTEGER, PRIMARY KEY(k COLLATE NOCASE DESC, id)) WITHOUT ROWID; \
                 CREATE INDEX temp.tw_ix ON tw(k COLLATE BINARY, v); \
                 CREATE INDEX temp.tw_iy ON tw(id, k COLLATE NOCASE DESC);",
                stock_path.display()
            ))
            .expect("stock attach+temp");
        native_outer
            .execute(&format!(
                "ATTACH DATABASE '{}' AS aux;",
                native_path.display()
            ))
            .await
            .expect("native attach");
        for sql in [
            "CREATE TEMP TABLE tw(v TEXT, k TEXT, id INTEGER, PRIMARY KEY(k COLLATE NOCASE DESC, id)) WITHOUT ROWID;",
            "CREATE INDEX temp.tw_ix ON tw(k COLLATE BINARY, v);",
            "CREATE INDEX temp.tw_iy ON tw(id, k COLLATE NOCASE DESC);",
        ] {
            native_outer.execute(sql).await.expect("native temp ddl");
        }
        for sql in [
            "PRAGMA aux.index_xinfo(ix);",
            "PRAGMA aux.index_info(ix);",
            "PRAGMA aux.index_xinfo(iy);",
            "PRAGMA aux.index_info(iy);",
            "PRAGMA aux.index_xinfo(r_ix);",
            "PRAGMA temp.index_xinfo(tw_ix);",
            "PRAGMA temp.index_info(tw_ix);",
            "PRAGMA temp.index_xinfo(tw_iy);",
            "PRAGMA temp.index_info(tw_iy);",
        ] {
            let expected = stock_pragma_rows(&stock_outer, sql);
            assert!(!expected.is_empty(), "stock empty for {sql}");
            let actual: Vec<Vec<SqliteValue>> = native_outer
                .query(sql)
                .await
                .expect("native attach/temp query")
                .iter()
                .map(row_values)
                .collect();
            assert_eq!(actual, expected, "attach/temp mismatch for {sql}");
        }
        native_outer.close().await.expect("close outer");
    });
}

#[test]
fn clustered_primary_key_collations_govern_writes_and_secondary_locators_after_reopen() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("clustered.db");
        let reference = rusqlite::Connection::open_in_memory().unwrap();
        let mut db = Connection::open(path.to_str().unwrap()).await.unwrap();
        for sql in [
            "CREATE TABLE t(k TEXT COLLATE NOCASE, v TEXT UNIQUE, PRIMARY KEY(k COLLATE BINARY DESC)) WITHOUT ROWID",
            "INSERT INTO t VALUES('A','one')",
            "INSERT OR REPLACE INTO t VALUES('a','two')",
            "CREATE INDEX by_column ON t(k,v)",
            "CREATE INDEX by_key ON t(k COLLATE BINARY,v)",
            "CREATE TABLE n(k TEXT, v TEXT, PRIMARY KEY(k COLLATE NOCASE)) WITHOUT ROWID",
            "INSERT INTO n VALUES('Alpha','first')",
            "INSERT OR IGNORE INTO n VALUES('alpha','ignored')",
            "CREATE TABLE r(k TEXT, id INT, v TEXT, PRIMARY KEY(k COLLATE NOCASE,k COLLATE BINARY,id,id DESC)) WITHOUT ROWID",
            "INSERT INTO r VALUES('A',1,'one'),('a',1,'two')",
            "CREATE INDEX by_repeated_key ON r(k COLLATE NOCASE)",
            "CREATE TABLE audit(k TEXT, old_v TEXT, new_v TEXT)",
            "CREATE TRIGGER t_updates AFTER UPDATE OF v ON t BEGIN INSERT INTO audit VALUES(OLD.k,OLD.v,NEW.v); END",
        ] {
            reference.execute_batch(sql).unwrap();
            db.execute(sql).await.unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
        let probes = [
            "SELECT k,v FROM t WHERE k='a' ORDER BY k COLLATE BINARY",
            "SELECT k,v FROM t INDEXED BY by_column ORDER BY k COLLATE BINARY",
            "SELECT k,v FROM t INDEXED BY by_key ORDER BY k COLLATE BINARY",
            "SELECT k,v FROM n WHERE k='alpha'",
            "SELECT k,v FROM n ORDER BY k",
            "SELECT k,id,v FROM r INDEXED BY by_repeated_key ORDER BY k COLLATE BINARY",
            "SELECT k,old_v,new_v FROM audit ORDER BY rowid",
            "PRAGMA integrity_check",
        ];
        for stage in ["created", "reopened", "updated"] {
            if stage == "reopened" {
                db.close().await.unwrap();
                db = Connection::open(path.to_str().unwrap()).await.unwrap();
            }
            if stage == "updated" {
                for sql in [
                    "UPDATE OR IGNORE t SET k='a' WHERE v='one'",
                    "UPDATE t SET v=v||'!' WHERE k='a'",
                    "UPDATE n SET k='ALPHA' WHERE v='first'",
                    "INSERT OR REPLACE INTO n VALUES('alpha','replacement')",
                    "UPDATE r SET v='updated' WHERE k COLLATE BINARY='A'",
                    "DELETE FROM r WHERE k COLLATE BINARY='a'",
                    "REINDEX by_column",
                    "REINDEX by_repeated_key",
                ] {
                    reference.execute_batch(sql).unwrap();
                    db.execute(sql).await.unwrap_or_else(|error| panic!("{sql}: {error}"));
                }
            }
            for sql in probes {
                let expected = stock_pragma_rows(&reference, sql);
                let actual = db.query(sql).await.unwrap();
                assert_eq!(actual.iter().map(row_values).collect::<Vec<_>>(), expected, "{stage}: {sql}");
            }
        }
        db.close().await.unwrap();
        let stock = rusqlite::Connection::open(path).unwrap();
        for sql in probes {
            assert_eq!(stock_pragma_rows(&stock, sql), stock_pragma_rows(&reference, sql), "native file opened by stock: {sql}");
        }
    });
}

#[test]
fn clustered_upserts_choose_the_same_victim_as_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let db = Connection::open(":memory:").await.unwrap();
        // The candidate 'x' collides with 'X' under NOCASE and with 'x '
        // under RTRIM. Updating the wrong victim still succeeds.
        for (definition, explicit_index) in [
            ("PRIMARY KEY(k COLLATE NOCASE), UNIQUE(k COLLATE RTRIM)", false),
            ("UNIQUE(k COLLATE RTRIM), PRIMARY KEY(k COLLATE NOCASE)", false),
            ("PRIMARY KEY(k COLLATE NOCASE), UNIQUE(k COLLATE RTRIM) ON CONFLICT REPLACE", false),
            ("PRIMARY KEY(k COLLATE NOCASE)", true),
        ] {
            let create = format!("CREATE TABLE t(k TEXT,v INT,{definition}) WITHOUT ROWID");
            for sql in [
                create.as_str(),
                "CREATE TABLE audit(k TEXT)",
                "CREATE TRIGGER witness AFTER UPDATE ON t BEGIN INSERT INTO audit VALUES(OLD.k); END",
            ] {
                db.execute(sql).await.unwrap();
                stock.execute_batch(sql).unwrap();
            }
            if explicit_index {
                let sql = "CREATE UNIQUE INDEX u ON t(k COLLATE RTRIM)";
                db.execute(sql).await.unwrap();
                stock.execute_batch(sql).unwrap();
            }
            for target in ["", "(k)", "(k COLLATE NOCASE)", "(k COLLATE RTRIM) WHERE 0"] {
                for action in ["DO NOTHING", "DO UPDATE SET v=v+1"] {
                    let insert = format!("INSERT INTO t VALUES('x',9) ON CONFLICT{target} {action}");
                    for sql in [
                        "DELETE FROM audit",
                        "DELETE FROM t",
                        "INSERT INTO t VALUES('X',0),('x ',0)",
                        insert.as_str(),
                    ] {
                        stock.execute_batch(sql).unwrap();
                        db.execute(sql).await.unwrap_or_else(|e| panic!("{definition} / {sql}: {e}"));
                    }
                    for probe in ["SELECT k,v FROM t ORDER BY k COLLATE BINARY", "SELECT k FROM audit"] {
                        let rows = db.query(probe).await.unwrap();
                        assert_eq!(rows.iter().map(row_values).collect::<Vec<_>>(),
                            stock_pragma_rows(&stock, probe), "{definition}: {insert}: {probe}");
                    }
                }
            }
            db.execute("DROP TABLE t").await.unwrap();
            db.execute("DROP TABLE audit").await.unwrap();
            stock.execute_batch("DROP TABLE t; DROP TABLE audit").unwrap();
        }

        let create = "CREATE TABLE r(k TEXT,j INT,PRIMARY KEY(k COLLATE NOCASE,k COLLATE RTRIM)) WITHOUT ROWID";
        for sql in [create, "INSERT INTO r VALUES('X',1)"] {
            stock.execute_batch(sql).unwrap();
            db.execute(sql).await.unwrap();
        }
        for target in [
            "k", "k,k", "k,j", "j,k", "k COLLATE RTRIM,k COLLATE NOCASE",
            "k COLLATE NOCASE,k COLLATE NOCASE", "k COLLATE BINARY,k COLLATE RTRIM",
            "k COLLATE BINARY,k", "k,unknown_col", "k,unknown_col COLLATE NOCASE",
        ] {
            let sql = format!("INSERT INTO r VALUES('X',2) ON CONFLICT({target}) DO UPDATE SET j=excluded.j");
            let expected = stock.execute_batch(&sql);
            let actual = db.execute(&sql).await;
            assert_eq!(actual.is_ok(), expected.is_ok(), "{sql}: {actual:?} vs {expected:?}");
        }
        db.close().await.unwrap();
    });
}
