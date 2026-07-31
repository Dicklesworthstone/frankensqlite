//! bd-pt5co — Oracle-parity e2e: WITHOUT ROWID DML & indexing vs rusqlite.
//!
//! rowid_oracle covers WITHOUT ROWID storage/ordering and the no-rowid-column
//! rule; this exercises mutation on that distinct storage model (the PK *is* the
//! key, there is no rowid B-tree): UPDATE/DELETE of non-key columns, UPDATE of
//! the PRIMARY KEY itself (re-keying + re-ordering), a secondary index lookup,
//! duplicate-PK conflict (error and INSERT OR REPLACE), and an INTEGER-PK
//! WITHOUT ROWID table's PK ordering. Each scenario asserts per-statement
//! agreement with rusqlite, then compares the resulting rows.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn without_rowid_update_and_delete() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('banana',1),('apple',2),('cherry',3),('date',4)",
                "UPDATE wr SET v = v * 10 WHERE k = 'apple'", // apple -> 20
                "DELETE FROM wr WHERE k = 'cherry'",
            ],
            &["SELECT k, v FROM wr ORDER BY k"], // (apple,20),(banana,1),(date,4)
            "without_rowid_update_and_delete",
        )
        .await;
    });
}

#[test]
fn without_rowid_update_primary_key() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('apple',1),('banana',2)",
                "UPDATE wr SET k = 'zebra' WHERE k = 'apple'", // re-key + re-order
            ],
            &["SELECT k, v FROM wr ORDER BY k"], // (banana,2),(zebra,1)
            "without_rowid_update_primary_key",
        )
        .await;
    });
}

#[test]
fn without_rowid_secondary_index() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX idx_v ON wr(v)",
                "INSERT INTO wr VALUES ('a',30),('b',10),('c',20),('d',10)",
            ],
            &[
                "SELECT k FROM wr WHERE v = 10 ORDER BY k",    // b,d
                "SELECT k FROM wr WHERE v > 15 ORDER BY v, k", // c(20),a(30)
                "SELECT k, v FROM wr ORDER BY v, k",
            ],
            "without_rowid_secondary_index",
        )
        .await;
    });
}

#[test]
fn without_rowid_pk_conflict_and_replace() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO wr VALUES ('apple',1),('banana',2)",
                "INSERT INTO wr VALUES ('apple',99)", // duplicate PK -> error both
                "INSERT OR REPLACE INTO wr VALUES ('apple',99)", // replaces apple
            ],
            &["SELECT k, v FROM wr ORDER BY k"], // (apple,99),(banana,2)
            "without_rowid_pk_conflict_and_replace",
        )
        .await;
    });
}

#[test]
fn without_rowid_update_or_replace_fails_before_mutation_and_preserves_integrity() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().expect("create WITHOUT ROWID regression tempdir");
        let path = directory.path().join("update-or-replace.db");
        let path_text = path.to_string_lossy().into_owned();
        let conn = Connection::open(&path_text)
            .await
            .expect("open FrankenSQLite database");

        conn.execute(
            "CREATE TABLE wr(
                 id INTEGER PRIMARY KEY,
                 u TEXT UNIQUE,
                 payload INTEGER
             ) WITHOUT ROWID",
        )
        .await
        .expect("create WITHOUT ROWID fixture");
        conn.execute("INSERT INTO wr VALUES(1,'a',10),(2,'b',20)")
            .await
            .expect("seed two distinct rows");

        let error = conn
            .execute("UPDATE OR REPLACE wr SET u='a' WHERE id=2")
            .await
            .expect_err("unsafe WITHOUT ROWID victim replacement must fail closed");
        assert!(
            error.to_string().contains("FAIL/IGNORE/REPLACE"),
            "rejection must identify the quarantined conflict family: {error}"
        );

        assert_eq!(
            frank_rows(&conn, "SELECT id, u, payload FROM wr ORDER BY id")
                .await
                .expect("read table after rejected replacement"),
            vec![
                vec!["1".to_owned(), "'a'".to_owned(), "10".to_owned()],
                vec!["2".to_owned(), "'b'".to_owned(), "20".to_owned()],
            ],
            "the rejected statement must leave both table rows unchanged"
        );
        assert_eq!(
            frank_rows(
                &conn,
                "SELECT id, u, payload FROM wr INDEXED BY sqlite_autoindex_wr_1 ORDER BY u",
            )
            .await
            .expect("force the UNIQUE secondary index after rejection"),
            vec![
                vec!["1".to_owned(), "'a'".to_owned(), "10".to_owned()],
                vec!["2".to_owned(), "'b'".to_owned(), "20".to_owned()],
            ],
            "the rejected statement must leave the UNIQUE secondary index unchanged"
        );
        assert_eq!(
            frank_rows(&conn, "PRAGMA integrity_check")
                .await
                .expect("run FrankenSQLite integrity_check"),
            vec![vec!["'ok'".to_owned()]]
        );
        conn.close().await.expect("close FrankenSQLite database");

        let oracle = rusqlite::Connection::open(&path).expect("open file with stock SQLite");
        let integrity: String = oracle
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("run stock SQLite integrity_check");
        assert_eq!(integrity, "ok");
        assert_eq!(
            sqlite_rows(
                &oracle,
                "SELECT id, u, payload FROM wr INDEXED BY sqlite_autoindex_wr_1 ORDER BY u",
            )
            .expect("read forced UNIQUE index with stock SQLite"),
            vec![
                vec!["1".to_owned(), "'a'".to_owned(), "10".to_owned()],
                vec!["2".to_owned(), "'b'".to_owned(), "20".to_owned()],
            ]
        );
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

            let page_count: i64 = oracle
                .query_row("PRAGMA page_count", [], |row| row.get(0))
                .expect("read page_count");
            let freelist_count: i64 = oracle
                .query_row("PRAGMA freelist_count", [], |row| row.get(0))
                .expect("read freelist_count");
            assert!(
                (0..=page_count).contains(&freelist_count),
                "invalid page accounting: page_count={page_count}, freelist_count={freelist_count}"
            );
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

            assert_stock_page_accounting(migration_round);
        }
    });
}

#[test]
fn without_rowid_integer_pk_ordering() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE wr (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID",
                "INSERT INTO wr VALUES (3,'c'),(1,'a'),(2,'b'),(10,'j')",
                "UPDATE wr SET v = 'B' WHERE id = 2",
                "DELETE FROM wr WHERE id = 10",
            ],
            &["SELECT id, v FROM wr ORDER BY id"], // (1,a),(2,B),(3,c)
            "without_rowid_integer_pk_ordering",
        )
        .await;
    });
}
