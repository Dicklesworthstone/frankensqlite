//! bd-gh-prepared-stmt-reprepare-yuq2p (GH #239): a prepared statement must
//! transparently re-prepare after a SAME-connection schema change, matching
//! stock SQLite (via the rusqlite oracle), instead of failing permanently with
//! `SchemaChanged`.
//!
//! Stock SQLite prepares with `sqlite3_prepare_v2`, so `sqlite3_step`
//! transparently re-prepares on `SQLITE_SCHEMA` and returns the rows (or the
//! real error if the object no longer exists). rusqlite inherits that behavior.
//!
//! Coverage:
//!   (a) ALTER TABLE ... ADD COLUMN with `SELECT *` — statement stays valid and
//!       re-projects the widened row shape.
//!   (b) ALTER TABLE ... ADD COLUMN with an explicit column list — rows unchanged.
//!   (c) CREATE INDEX — statement stays valid.
//!   (d) DROP TABLE — re-prepare fails with the REAL "no such table" error, NOT
//!       `SchemaChanged`.
//!   (e) prepared INSERT DML followed by an unrelated ADD COLUMN — the INSERT
//!       succeeds after transparent re-prepare.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

/// Normalize a Franken row into a comparable per-cell string.
fn franken_cell(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("I:{n}"),
        SqliteValue::Float(r) => format!("R:{r}"),
        SqliteValue::Text(t) => format!("T:{}", t.as_str()),
        SqliteValue::Blob(b) => format!("B:{b:?}"),
    }
}

/// Normalize a rusqlite value into the same comparable per-cell string.
fn oracle_cell(v: &rusqlite::types::Value) -> String {
    use rusqlite::types::Value;
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Integer(n) => format!("I:{n}"),
        Value::Real(r) => format!("R:{r}"),
        Value::Text(t) => format!("T:{t}"),
        Value::Blob(b) => format!("B:{b:?}"),
    }
}

fn oracle_rows(stmt: &mut rusqlite::Statement<'_>) -> Vec<Vec<String>> {
    // Read the column count from the *row* (post re-prepare), not from the
    // Statement handle, whose cached count can lag a transparent re-prepare.
    stmt.query_map([], |row| {
        let col_count = row.as_ref().column_count();
        let mut cells = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let v: rusqlite::types::Value = row.get(i)?;
            cells.push(oracle_cell(&v));
        }
        Ok(cells)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn franken_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.values().iter().map(franken_cell).collect())
        .collect()
}

async fn read_with_api(
    stmt: &fsqlite_core::connection::PreparedStatement<'_>,
    api: &str,
) -> Vec<Vec<String>> {
    match api {
        "query" => franken_rows(&stmt.query().await.unwrap()),
        "query_with_params" => franken_rows(&stmt.query_with_params(&[]).await.unwrap()),
        "query_row" => franken_rows(&[stmt.query_row().await.unwrap()]),
        "query_row_with_params" => franken_rows(&[stmt.query_row_with_params(&[]).await.unwrap()]),
        "for_each" => {
            let mut rows = Vec::new();
            stmt.query_with_params_for_each(&[], |row| {
                rows.push(row.values().iter().map(franken_cell).collect());
                Ok(())
            })
            .await
            .unwrap();
            rows
        }
        _ => panic!("unknown prepared-read API: {api}"),
    }
}

#[test]
fn schema_reprepare_releases_only_its_own_read_transaction() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for explicit in [false, true] {
                for api in ["query", "query_with_params", "query_row", "query_row_with_params", "for_each"] {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("prepared-schema-retry.db");
                    let conn = Connection::open(if file_backed { path.to_str().unwrap() } else { ":memory:" })
                        .await
                        .unwrap();
                    assert!(conn.is_concurrent_mode_default());
                    let ora = rusqlite::Connection::open_in_memory().unwrap();
                    let setup = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT); INSERT INTO t VALUES(1,'alpha');";
                    conn.execute(setup).await.unwrap();
                    ora.execute_batch(setup).unwrap();
                    let sql = "SELECT * FROM t WHERE a=1";
                    let stmt = conn.prepare(sql).await.unwrap();
                    let mut ostmt = ora.prepare(sql).unwrap();
                    assert_eq!(read_with_api(&stmt, api).await, oracle_rows(&mut ostmt));
                    if explicit {
                        conn.execute("BEGIN;").await.unwrap();
                        ora.execute_batch("BEGIN;").unwrap();
                    }
                    let alter = "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7;";
                    conn.execute(alter).await.unwrap();
                    ora.execute_batch(alter).unwrap();
                    assert_eq!(read_with_api(&stmt, api).await, oracle_rows(&mut ostmt));
                    assert_eq!(conn.in_transaction(), explicit);

                    // A leaked autocommit reader turns the following real
                    // index allocation into ReadOnly. An explicit transaction
                    // must instead retain ownership of both DDL and DML.
                    let writes = "CREATE INDEX idx_t_c ON t(c); INSERT INTO t(a,b) VALUES(2,'beta');";
                    conn.execute(writes).await.unwrap_or_else(|error| {
                        panic!("{api}, file={file_backed}, explicit={explicit}: {error:?}")
                    });
                    ora.execute_batch(writes).unwrap();
                    drop(stmt);
                    drop(ostmt);
                    if explicit {
                        conn.execute("ROLLBACK;").await.unwrap();
                        ora.execute_batch("ROLLBACK;").unwrap();
                    }
                    let final_sql = "SELECT * FROM t ORDER BY a";
                    let expected = oracle_rows(&mut ora.prepare(final_sql).unwrap());
                    assert_eq!(franken_rows(&conn.query(final_sql).await.unwrap()), expected);
                    conn.close().await.unwrap();
                    if file_backed {
                        let reopened = rusqlite::Connection::open(&path).unwrap();
                        assert_eq!(oracle_rows(&mut reopened.prepare(final_sql).unwrap()), expected);
                        let integrity: String = reopened.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
                        assert_eq!(integrity, "ok");
                    }
                    eprintln!("event=prepared_schema_retry_cleanup_verified api={api} file={file_backed} explicit={explicit}");
                }
            }
        }
    });
}

#[test]
fn add_column_select_star_reprojects_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // --- rusqlite oracle -------------------------------------------------
        let ora = rusqlite::Connection::open_in_memory().unwrap();
        ora.execute_batch(
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);\
             INSERT INTO t VALUES (1,'alpha'),(2,'beta');",
        )
        .unwrap();
        let mut ostmt = ora.prepare("SELECT * FROM t ORDER BY a").unwrap();
        // Prime once before the DDL, exactly like the Franken flow.
        let _ = oracle_rows(&mut ostmt);
        ora.execute("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7", [])
            .unwrap();
        let expected = oracle_rows(&mut ostmt);

        // --- FrankenSQLite ---------------------------------------------------
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,'alpha'),(2,'beta');")
            .await
            .unwrap();
        let stmt = conn.prepare("SELECT * FROM t ORDER BY a").await.unwrap();
        let _ = stmt.query().await.unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7;")
            .await
            .unwrap();
        let got = franken_rows(&stmt.query().await.expect("re-prepare after ADD COLUMN"));

        assert_eq!(
            got, expected,
            "SELECT * must re-project the widened row shape"
        );
        // Sanity: the new column is actually present after re-prepare.
        assert_eq!(
            got[0].len(),
            3,
            "re-prepared SELECT * must expose 3 columns"
        );
    });
}

#[test]
fn add_column_explicit_list_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        let ora = rusqlite::Connection::open_in_memory().unwrap();
        ora.execute_batch(
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);\
             INSERT INTO t VALUES (1,'alpha'),(2,'beta');",
        )
        .unwrap();
        let mut ostmt = ora.prepare("SELECT b FROM t ORDER BY a").unwrap();
        let _ = oracle_rows(&mut ostmt);
        ora.execute("ALTER TABLE t ADD COLUMN c INTEGER", [])
            .unwrap();
        let expected = oracle_rows(&mut ostmt);

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,'alpha'),(2,'beta');")
            .await
            .unwrap();
        let stmt = conn.prepare("SELECT b FROM t ORDER BY a").await.unwrap();
        let _ = stmt.query().await.unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN c INTEGER;")
            .await
            .unwrap();
        let got = franken_rows(&stmt.query().await.expect("re-prepare after ADD COLUMN"));

        assert_eq!(got, expected);
    });
}

#[test]
fn create_index_keeps_prepared_select_valid_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        let ora = rusqlite::Connection::open_in_memory().unwrap();
        ora.execute_batch(
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);\
             INSERT INTO t VALUES (1,'alpha'),(2,'beta');",
        )
        .unwrap();
        let mut ostmt = ora
            .prepare("SELECT b FROM t WHERE a >= 1 ORDER BY a")
            .unwrap();
        let _ = oracle_rows(&mut ostmt);
        ora.execute("CREATE INDEX idx_t_b ON t(b)", []).unwrap();
        let expected = oracle_rows(&mut ostmt);

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,'alpha'),(2,'beta');")
            .await
            .unwrap();
        let stmt = conn
            .prepare("SELECT b FROM t WHERE a >= 1 ORDER BY a")
            .await
            .unwrap();
        let _ = stmt.query().await.unwrap();
        conn.execute("CREATE INDEX idx_t_b ON t(b);").await.unwrap();
        let got = franken_rows(&stmt.query().await.expect("re-prepare after CREATE INDEX"));

        assert_eq!(got, expected);
    });
}

#[test]
fn drop_table_surfaces_real_error_not_schemachanged() {
    asupersync::test_utils::run_test(|| async {
        // rusqlite oracle: dropping the table makes re-execution fail with a
        // "no such table" error, NOT a silent SQLITE_SCHEMA.
        let ora = rusqlite::Connection::open_in_memory().unwrap();
        ora.execute_batch(
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);\
             INSERT INTO t VALUES (1,'alpha');",
        )
        .unwrap();
        let mut ostmt = ora.prepare("SELECT b FROM t").unwrap();
        let _ = oracle_rows(&mut ostmt);
        ora.execute("DROP TABLE t", []).unwrap();
        let oracle_result: Result<(), rusqlite::Error> = (|| {
            let mut rows = ostmt.query([])?;
            while rows.next()?.is_some() {}
            Ok(())
        })();
        let oracle_msg = format!("{:?}", oracle_result.unwrap_err()).to_lowercase();
        assert!(
            oracle_msg.contains("no such table"),
            "rusqlite oracle should report no-such-table, got: {oracle_msg}"
        );

        // FrankenSQLite must match: the REAL error, never SchemaChanged.
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,'alpha');")
            .await
            .unwrap();
        let stmt = conn.prepare("SELECT b FROM t").await.unwrap();
        let _ = stmt.query().await.unwrap();
        conn.execute("DROP TABLE t;").await.unwrap();
        let err = stmt.query().await.expect_err("dropped table must error");
        assert!(
            !matches!(err, fsqlite_error::FrankenError::SchemaChanged),
            "dropped table must surface the real error, not SchemaChanged: {err:?}"
        );
        // Franken surfaces the structured `NoSuchTable { name }` error; rusqlite
        // reports "no such table". Both name the missing relation `t`.
        let msg = format!("{err:?}").to_lowercase().replace(' ', "");
        assert!(
            msg.contains("nosuchtable") || msg.contains("no such table"),
            "Franken should report no-such-table like rusqlite, got: {err:?}"
        );
        drop(stmt);
        conn.execute("CREATE TABLE replacement(value INTEGER); INSERT INTO replacement VALUES(42);")
            .await
            .expect("failed re-prepare must not leave a read-only transaction active");
        assert_eq!(conn.query("SELECT value FROM replacement;").await.unwrap()[0].values(), &[SqliteValue::Integer(42)]);
        conn.close().await.unwrap();
    });
}

#[test]
fn prepared_insert_survives_unrelated_ddl_matches_oracle() {
    asupersync::test_utils::run_test(|| async {
        // rusqlite oracle: a prepared INSERT re-executes successfully after an
        // unrelated same-connection ADD COLUMN.
        let ora = rusqlite::Connection::open_in_memory().unwrap();
        ora.execute_batch("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);")
            .unwrap();
        let mut oins = ora.prepare("INSERT INTO t(a, b) VALUES (?1, ?2)").unwrap();
        oins.execute(rusqlite::params![1, "alpha"]).unwrap();
        ora.execute("ALTER TABLE t ADD COLUMN c INTEGER", [])
            .unwrap();
        let ora_affected = oins.execute(rusqlite::params![2, "beta"]).unwrap();
        let ora_count: i64 = ora
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ora_affected, 1);
        assert_eq!(ora_count, 2);

        // FrankenSQLite must match.
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);")
            .await
            .unwrap();
        let ins = conn
            .prepare("INSERT INTO t(a, b) VALUES (?1, ?2)")
            .await
            .unwrap();
        ins.execute_with_params(&[SqliteValue::Integer(1), SqliteValue::Text("alpha".into())])
            .await
            .unwrap();
        conn.execute("ALTER TABLE t ADD COLUMN c INTEGER;")
            .await
            .unwrap();
        let affected = ins
            .execute_with_params(&[SqliteValue::Integer(2), SqliteValue::Text("beta".into())])
            .await
            .expect("prepared INSERT must re-prepare and succeed after ADD COLUMN");
        assert_eq!(affected, 1);
        let count = conn.query("SELECT COUNT(*) FROM t;").await.unwrap();
        assert_eq!(count[0].values()[0], SqliteValue::Integer(2));
    });
}
