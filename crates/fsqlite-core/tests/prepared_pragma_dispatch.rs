use fsqlite_core::connection::{Connection, Row};
use fsqlite_error::ErrorCode;
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn format_fsqlite_rows(rows: Vec<Row>) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values().iter().map(format_fsqlite_value).collect())
        .collect()
}

fn format_fsqlite_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(number) => number.to_string(),
        SqliteValue::Float(number) => format!("{number}"),
        SqliteValue::Text(text) => format!("'{text}'"),
        SqliteValue::Blob(bytes) => format_blob(bytes),
    }
}

fn format_rusqlite_value(value: rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(number) => number.to_string(),
        rusqlite::types::Value::Real(number) => format!("{number}"),
        rusqlite::types::Value::Text(text) => format!("'{text}'"),
        rusqlite::types::Value::Blob(bytes) => format_blob(&bytes),
    }
}

fn format_blob(bytes: &[u8]) -> String {
    format!(
        "X'{}'",
        bytes
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>()
    )
}

async fn fsqlite_prepared_rows(conn: &Connection, sql: &str) -> TestResult<Vec<Vec<String>>> {
    let stmt = conn.prepare(sql).await?;
    let rows = stmt.query().await?;
    for row in &rows {
        assert_eq!(
            row.values().len(),
            stmt.column_count(),
            "prepared row width mismatch for {sql}"
        );
    }
    Ok(format_fsqlite_rows(rows))
}

fn rusqlite_rows(conn: &rusqlite::Connection, sql: &str) -> TestResult<Vec<Vec<String>>> {
    let mut stmt = conn.prepare(sql)?;
    let column_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(column_count);
            for index in 0..column_count {
                let value = row.get::<_, rusqlite::types::Value>(index)?;
                values.push(format_rusqlite_value(value));
            }
            Ok(values)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

async fn assert_prepared_pragma_query_parity(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    sql: &str,
) -> TestResult {
    let fstmt = fconn.prepare(sql).await?;
    let (rusqlite_column_count, rusqlite_column_names) = {
        let rstmt = rconn.prepare(sql)?;
        (
            rstmt.column_count(),
            rstmt
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
        )
    };
    assert_eq!(
        fstmt.column_count(),
        rusqlite_column_count,
        "column count mismatch for {sql}"
    );
    assert_eq!(
        fstmt.column_names(),
        rusqlite_column_names,
        "column names mismatch for {sql}"
    );
    let fsqlite_rows = fstmt.query().await?;
    for row in &fsqlite_rows {
        assert_eq!(
            row.values().len(),
            fstmt.column_count(),
            "prepared row width mismatch for {sql}"
        );
    }
    assert_eq!(
        format_fsqlite_rows(fsqlite_rows),
        rusqlite_rows(rconn, sql)?,
        "row mismatch for {sql}"
    );
    Ok(())
}

async fn assert_prepared_pragma_shape_parity(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    sql: &str,
) -> TestResult {
    let fstmt = fconn.prepare(sql).await?;
    let rstmt = rconn.prepare(sql)?;
    assert_eq!(
        fstmt.column_count(),
        rstmt.column_count(),
        "column count mismatch for {sql}"
    );
    assert_eq!(
        fstmt.column_names(),
        rstmt
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        "column names mismatch for {sql}"
    );
    let rows = fstmt.query().await?;
    for row in &rows {
        assert_eq!(
            row.values().len(),
            fstmt.column_count(),
            "prepared row width mismatch for {sql}"
        );
    }
    Ok(())
}

async fn assert_prepared_pragma_unordered_query_parity(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    sql: &str,
) -> TestResult {
    let fstmt = fconn.prepare(sql).await?;
    let rstmt = rconn.prepare(sql)?;
    assert_eq!(
        fstmt.column_count(),
        rstmt.column_count(),
        "column count mismatch for {sql}"
    );
    assert_eq!(
        fstmt.column_names(),
        rstmt
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        "column names mismatch for {sql}"
    );
    let rows = fstmt.query().await?;
    for row in &rows {
        assert_eq!(
            row.values().len(),
            fstmt.column_count(),
            "prepared row width mismatch for {sql}"
        );
    }
    let mut fsqlite_rows = format_fsqlite_rows(rows);
    let mut sqlite_rows = rusqlite_rows(rconn, sql)?;
    fsqlite_rows.sort();
    sqlite_rows.sort();
    assert_eq!(fsqlite_rows, sqlite_rows, "row mismatch for {sql}");
    Ok(())
}

async fn assert_prepared_pragma_execute_only_parity(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    sql: &str,
) -> TestResult {
    let fstmt = fconn.prepare(sql).await?;
    let (rusqlite_column_count, rusqlite_column_names) = {
        let rstmt = rconn.prepare(sql)?;
        (
            rstmt.column_count(),
            rstmt
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
        )
    };
    assert_eq!(
        fstmt.column_count(),
        rusqlite_column_count,
        "column count mismatch for {sql}"
    );
    assert_eq!(
        fstmt.column_names(),
        rusqlite_column_names,
        "column names mismatch for {sql}"
    );
    let query_rows = fstmt.query().await?;
    for row in &query_rows {
        assert_eq!(
            row.values().len(),
            fstmt.column_count(),
            "prepared row width mismatch for {sql}"
        );
    }
    assert!(
        query_rows.is_empty(),
        "zero-column PRAGMA query unexpectedly returned rows for {sql}"
    );
    assert_eq!(
        format_fsqlite_rows(query_rows),
        rusqlite_rows(rconn, sql)?,
        "prepared query row mismatch for {sql}"
    );
    assert_eq!(
        fconn.prepare(sql).await?.execute().await?,
        0,
        "unexpected output row for {sql}"
    );
    assert_eq!(
        format_fsqlite_rows(fconn.query(sql).await?),
        rusqlite_rows(rconn, sql)?,
        "direct row mismatch for {sql}"
    );
    Ok(())
}

async fn assert_fsqlite_pragma_shape(
    conn: &Connection,
    sql: &str,
    expected_columns: &[&str],
) -> TestResult {
    let stmt = conn.prepare(sql).await?;
    assert_eq!(
        stmt.column_count(),
        expected_columns.len(),
        "column count mismatch for {sql}"
    );
    assert_eq!(
        stmt.column_names(),
        expected_columns,
        "column names mismatch for {sql}"
    );
    let rows = stmt.query().await?;
    for row in &rows {
        assert_eq!(
            row.values().len(),
            stmt.column_count(),
            "prepared row width mismatch for {sql}"
        );
    }
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn prepared_pragma_getters_and_setters_match_rusqlite() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let fconn = Connection::open(":memory:").await?;
            let rconn = rusqlite::Connection::open_in_memory()?;
            fconn
                .execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
                .await?;
            fconn
                .execute(
                    "CREATE TABLE child(\
                         id INTEGER PRIMARY KEY, \
                         parent_id INTEGER REFERENCES parent(id)\
                     )",
                )
                .await?;
            fconn
                .execute("CREATE INDEX child_parent_idx ON child(parent_id)")
                .await?;
            rconn.execute_batch(
                "CREATE TABLE parent(id INTEGER PRIMARY KEY); \
                 CREATE TABLE child(\
                     id INTEGER PRIMARY KEY, \
                     parent_id INTEGER REFERENCES parent(id)\
                 ); \
                 CREATE INDEX child_parent_idx ON child(parent_id);",
            )?;

            for sql in ["PRAGMA user_version = 37", "PRAGMA cache_size = -4000"] {
                fconn.prepare(sql).await?.execute().await?;
                rconn.execute_batch(sql)?;
            }

            for sql in ["PRAGMA user_version", "PRAGMA cache_size"] {
                assert_eq!(
                    fsqlite_prepared_rows(&fconn, sql).await?,
                    rusqlite_rows(&rconn, sql)?
                );
            }

            for sql in [
                "PRAGMA user_version",
                "PRAGMA busy_timeout",
                "PRAGMA busy_timeout = 50",
                "PRAGMA table_info(child)",
                "PRAGMA table_info = 'child'",
                "PRAGMA main.table_info(child)",
                "PRAGMA table_info(missing_table)",
                "PRAGMA table_xinfo(child)",
                "PRAGMA table_xinfo = 'missing_table'",
                "PRAGMA main.table_xinfo(child)",
                "PRAGMA index_list(child)",
                "PRAGMA main.index_list(child)",
                "PRAGMA index_info(child_parent_idx)",
                "PRAGMA main.index_info(child_parent_idx)",
                "PRAGMA index_xinfo(child_parent_idx)",
                "PRAGMA main.index_xinfo(child_parent_idx)",
                "PRAGMA foreign_key_list(child)",
                "PRAGMA foreign_key_list = 'child'",
                "PRAGMA main.foreign_key_list(child)",
                "PRAGMA foreign_key_check",
                "PRAGMA foreign_key_check(child)",
                "PRAGMA main.foreign_key_check(child)",
                "PRAGMA wal_checkpoint(TRUNCATE)",
                "PRAGMA main.wal_checkpoint(TRUNCATE)",
                "PRAGMA definitely_not_supported",
                "PRAGMA case_sensitive_like",
            ] {
                assert_prepared_pragma_query_parity(&fconn, &rconn, sql).await?;
            }

            for sql in [
                "PRAGMA database_list",
                "PRAGMA table_list",
                "PRAGMA collation_list",
            ] {
                assert_prepared_pragma_shape_parity(&fconn, &rconn, sql).await?;
            }

            fconn
                .execute("CREATE TEMP TABLE parent(id INTEGER PRIMARY KEY, temp_marker TEXT)")
                .await?;
            fconn
                .execute(
                    "CREATE TEMP TABLE child(\
                         id INTEGER PRIMARY KEY, \
                         temp_parent_id INTEGER REFERENCES parent(id), \
                         temp_marker TEXT\
                     )",
                )
                .await?;
            fconn
                .execute("CREATE INDEX temp.child_parent_idx ON child(temp_parent_id)")
                .await?;
            // The test deliberately creates orphan rows for foreign_key_check.
            // Do not rely on SQLite's build-time default for FK enforcement.
            fconn.execute("PRAGMA foreign_keys = OFF").await?;
            fconn
                .execute("INSERT INTO temp.child(id, temp_parent_id) VALUES (12, 902)")
                .await?;
            rconn.execute_batch(
                "PRAGMA foreign_keys = OFF; \
                 CREATE TEMP TABLE parent(id INTEGER PRIMARY KEY, temp_marker TEXT); \
                 CREATE TEMP TABLE child(\
                     id INTEGER PRIMARY KEY, \
                     temp_parent_id INTEGER REFERENCES parent(id), \
                     temp_marker TEXT\
                 ); \
                 CREATE INDEX temp.child_parent_idx ON child(temp_parent_id); \
                 INSERT INTO temp.child(id, temp_parent_id) VALUES (12, 902);",
            )?;

            // The no-argument form defaults to main, while the unqualified
            // table-call form uses ordinary lookup and resolves the shadowing
            // TEMP table.
            for sql in [
                "PRAGMA foreign_key_check",
                "PRAGMA foreign_key_check(child)",
                "PRAGMA temp.foreign_key_check",
            ] {
                assert_prepared_pragma_query_parity(&fconn, &rconn, sql).await?;
            }

            for sql in [
                "PRAGMA table_info(child)",
                "PRAGMA main.table_info(child)",
                "PRAGMA temp.table_info(child)",
                "PRAGMA table_xinfo(child)",
                "PRAGMA main.table_xinfo(child)",
                "PRAGMA temp.table_xinfo(child)",
                "PRAGMA index_list(child)",
                "PRAGMA main.index_list(child)",
                "PRAGMA temp.index_list(child)",
                "PRAGMA index_info(child_parent_idx)",
                "PRAGMA main.index_info(child_parent_idx)",
                "PRAGMA temp.index_info(child_parent_idx)",
                "PRAGMA index_xinfo(child_parent_idx)",
                "PRAGMA main.index_xinfo(child_parent_idx)",
                "PRAGMA temp.index_xinfo(child_parent_idx)",
                "PRAGMA foreign_key_list(child)",
                "PRAGMA main.foreign_key_list(child)",
                "PRAGMA temp.foreign_key_list(child)",
                "PRAGMA foreign_key_check",
                "PRAGMA main.foreign_key_check(child)",
                "PRAGMA temp.foreign_key_check(child)",
            ] {
                assert_prepared_pragma_query_parity(&fconn, &rconn, sql).await?;
            }

            fconn
                .execute("CREATE TABLE wr_main(id INTEGER PRIMARY KEY) WITHOUT ROWID")
                .await?;
            fconn
                .execute("CREATE TEMP TABLE wr_temp(id INTEGER PRIMARY KEY) WITHOUT ROWID")
                .await?;
            rconn.execute_batch(
                "CREATE TABLE wr_main(id INTEGER PRIMARY KEY) WITHOUT ROWID; \
                 CREATE TEMP TABLE wr_temp(id INTEGER PRIMARY KEY) WITHOUT ROWID;",
            )?;
            for sql in [
                "PRAGMA table_list",
                "PRAGMA main.table_list",
                "PRAGMA temp.table_list",
                "PRAGMA table_list(child)",
                "PRAGMA main.table_list(child)",
                "PRAGMA temp.table_list(child)",
                "PRAGMA table_list(wr_main)",
                "PRAGMA table_list(wr_temp)",
            ] {
                assert_prepared_pragma_unordered_query_parity(&fconn, &rconn, sql).await?;
            }

            #[cfg(feature = "ext-fts5")]
            {
                fconn
                    .execute("CREATE VIRTUAL TABLE pragma_vt USING fts5(content)")
                    .await?;
                rconn.execute_batch("CREATE VIRTUAL TABLE pragma_vt USING fts5(content);")?;
                for sql in [
                    "PRAGMA table_list(pragma_vt)",
                    "PRAGMA table_list(pragma_vt_data)",
                ] {
                    assert_prepared_pragma_unordered_query_parity(&fconn, &rconn, sql).await?;
                }
            }

            fconn.execute("ATTACH ':memory:' AS aux").await?;
            fconn
                .execute("CREATE TABLE aux.attached_wr(id INTEGER PRIMARY KEY) WITHOUT ROWID")
                .await?;
            fconn
                .execute("CREATE TABLE aux.parent(id INTEGER PRIMARY KEY)")
                .await?;
            fconn
                .execute(
                    "CREATE TABLE aux.child(\
                         id INTEGER PRIMARY KEY, \
                         aux_parent_id INTEGER REFERENCES parent(id)\
                     )",
                )
                .await?;
            fconn
                .execute("CREATE INDEX aux.child_parent_idx ON child(aux_parent_id)")
                .await?;
            fconn
                .execute(
                    "CREATE TABLE aux.wr_child(\
                         id INTEGER PRIMARY KEY, \
                         aux_parent_id INTEGER REFERENCES parent(id)\
                     ) WITHOUT ROWID",
                )
                .await?;
            fconn
                .execute("INSERT INTO aux.child(id, aux_parent_id) VALUES (21, 903)")
                .await?;
            fconn
                .execute("INSERT INTO aux.wr_child(id, aux_parent_id) VALUES (22, 904)")
                .await?;
            rconn.execute_batch(
                "ATTACH ':memory:' AS aux; \
                 CREATE TABLE aux.attached_wr(id INTEGER PRIMARY KEY) WITHOUT ROWID; \
                 CREATE TABLE aux.parent(id INTEGER PRIMARY KEY); \
                 CREATE TABLE aux.child(\
                     id INTEGER PRIMARY KEY, \
                     aux_parent_id INTEGER REFERENCES parent(id)\
                 ); \
                 CREATE INDEX aux.child_parent_idx ON child(aux_parent_id); \
                 CREATE TABLE aux.wr_child(\
                     id INTEGER PRIMARY KEY, \
                     aux_parent_id INTEGER REFERENCES parent(id)\
                 ) WITHOUT ROWID; \
                 INSERT INTO aux.child(id, aux_parent_id) VALUES (21, 903); \
                 INSERT INTO aux.wr_child(id, aux_parent_id) VALUES (22, 904);",
            )?;
            for sql in [
                "PRAGMA table_list",
                "PRAGMA aux.table_list",
                "PRAGMA table_list(attached_wr)",
                "PRAGMA aux.table_list(attached_wr)",
            ] {
                assert_prepared_pragma_unordered_query_parity(&fconn, &rconn, sql).await?;
            }
            for sql in [
                "PRAGMA aux.table_info(child)",
                "PRAGMA aux.table_xinfo(child)",
                "PRAGMA aux.index_list(child)",
                "PRAGMA aux.index_info(child_parent_idx)",
                "PRAGMA aux.index_xinfo(child_parent_idx)",
                "PRAGMA aux.foreign_key_list(child)",
                "PRAGMA aux.foreign_key_check(child)",
                "PRAGMA aux.foreign_key_check(wr_child)",
                "PRAGMA aux.database_list",
            ] {
                assert_prepared_pragma_query_parity(&fconn, &rconn, sql).await?;
            }
            for sql in [
                "PRAGMA nosuch.table_info(child)",
                "PRAGMA nosuch.index_list(child)",
                "PRAGMA nosuch.foreign_key_check(child)",
                "PRAGMA nosuch.table_list",
                "PRAGMA nosuch.database_list",
            ] {
                let error = fconn
                    .prepare(sql)
                    .await?
                    .query()
                    .await
                    .expect_err("unknown schema unexpectedly fell back");
                assert_eq!(error.error_code(), ErrorCode::Error, "{sql}");
                assert!(
                    error.to_string().contains("no such database"),
                    "unexpected unknown-schema error for {sql}: {error}"
                );
                assert!(
                    rusqlite_rows(&rconn, sql).is_err(),
                    "SQLite unexpectedly accepted unknown schema for {sql}"
                );
            }

            let fsqlite_user_version = fconn.prepare("PRAGMA user_version").await?;
            let user_version_row = fsqlite_user_version.query_row().await?;
            assert_eq!(user_version_row.values(), &[SqliteValue::Integer(37)]);
            assert_eq!(
                fconn
                    .prepare("PRAGMA user_version")
                    .await?
                    .execute()
                    .await?,
                1
            );

            for sql in [
                "PRAGMA user_version = 41",
                "PRAGMA schema_version = 73",
                "PRAGMA synchronous = OFF",
                "PRAGMA encoding = 'UTF-8'",
                "PRAGMA case_sensitive_like = ON",
            ] {
                assert_prepared_pragma_execute_only_parity(&fconn, &rconn, sql).await?;
            }
            assert_prepared_pragma_query_parity(&fconn, &rconn, "PRAGMA user_version").await?;
            assert_prepared_pragma_query_parity(&fconn, &rconn, "PRAGMA schema_version").await?;
            assert_prepared_pragma_query_parity(&fconn, &rconn, "PRAGMA synchronous").await?;
            assert_eq!(
                format_fsqlite_rows(fconn.query("SELECT 'a' LIKE 'A'").await?),
                rusqlite_rows(&rconn, "SELECT 'a' LIKE 'A'")?,
                "case_sensitive_like setter did not affect LIKE behavior"
            );

            for sql in [
                "PRAGMA journal_mode = MEMORY",
                "PRAGMA wal_autocheckpoint = 17",
                "PRAGMA locking_mode = EXCLUSIVE",
                "PRAGMA secure_delete = FAST",
                "PRAGMA threads = 2",
            ] {
                assert_prepared_pragma_query_parity(&fconn, &rconn, sql).await?;
            }
            for sql in [
                "PRAGMA journal_mode",
                "PRAGMA wal_autocheckpoint",
                "PRAGMA locking_mode",
                "PRAGMA secure_delete",
                "PRAGMA threads",
            ] {
                assert_prepared_pragma_query_parity(&fconn, &rconn, sql).await?;
            }

            assert_prepared_pragma_execute_only_parity(&fconn, &rconn, "PRAGMA query_only = ON")
                .await?;
            for sql in ["PRAGMA user_version = 900", "PRAGMA application_id = 901"] {
                let stmt = fconn.prepare(sql).await?;
                assert!(
                    stmt.query().await.is_err(),
                    "query_only allowed prepared query write for {sql}"
                );
                assert!(
                    stmt.execute().await.is_err(),
                    "query_only allowed prepared execute write for {sql}"
                );
                assert!(
                    fconn.query(sql).await.is_err(),
                    "query_only allowed direct query write for {sql}"
                );
                assert!(
                    rusqlite_rows(&rconn, sql).is_err(),
                    "SQLite unexpectedly allowed query_only write for {sql}"
                );
            }
            assert_prepared_pragma_execute_only_parity(&fconn, &rconn, "PRAGMA query_only = OFF")
                .await?;
            assert_prepared_pragma_query_parity(&fconn, &rconn, "PRAGMA user_version").await?;
            assert_prepared_pragma_query_parity(&fconn, &rconn, "PRAGMA application_id").await?;

            fconn.execute("BEGIN").await?;
            rconn.execute_batch("BEGIN")?;
            assert_prepared_pragma_execute_only_parity(&fconn, &rconn, "PRAGMA foreign_keys = ON")
                .await?;
            fconn.execute("COMMIT").await?;
            rconn.execute_batch("COMMIT")?;
            assert_prepared_pragma_query_parity(&fconn, &rconn, "PRAGMA foreign_keys").await?;

            assert_fsqlite_pragma_shape(&fconn, "PRAGMA fsqlite.concurrency", &["key", "value"])
                .await?;
            assert_fsqlite_pragma_shape(&fconn, "PRAGMA fsqlite_concurrency", &["key", "value"])
                .await?;

            #[cfg(feature = "diagnostic-pragmas")]
            {
                for sql in ["PRAGMA fsqlite.conflict_log", "PRAGMA conflict_log"] {
                    assert_fsqlite_pragma_shape(&fconn, sql, &["seq", "timestamp_ns", "event"])
                        .await?;
                }
                assert_fsqlite_pragma_shape(
                    &fconn,
                    "PRAGMA fsqlite.ssi_decisions",
                    &[
                        "txn_id",
                        "txn_epoch",
                        "snapshot_seq",
                        "commit_seq",
                        "decision_type",
                        "conflicting_txns",
                        "conflict_pages",
                        "read_set_page_count",
                        "read_set_top_k_pages",
                        "read_set_bloom_fingerprint",
                        "write_set_pages",
                        "rationale",
                        "timestamp_unix_ns",
                        "decision_epoch",
                        "chain_hash",
                    ],
                )
                .await?;
                assert_fsqlite_pragma_shape(&fconn, "PRAGMA fsqlite.cache_reset", &["status"])
                    .await?;
                assert_fsqlite_pragma_shape(
                    &fconn,
                    "PRAGMA fsqlite.cache_stats",
                    &["name", "value"],
                )
                .await?;
            }

            #[cfg(not(target_arch = "wasm32"))]
            {
                assert_fsqlite_pragma_shape(
                    &fconn,
                    "PRAGMA fsqlite.raptorq_events",
                    &[
                        "seq",
                        "frame_id",
                        "symbols_lost",
                        "symbols_used",
                        "repair_success",
                        "latency_ns",
                        "budget_utilization_pct",
                        "severity_bucket",
                    ],
                )
                .await?;
                assert_fsqlite_pragma_shape(
                    &fconn,
                    "PRAGMA fsqlite_raptorq_stats",
                    &["name", "value"],
                )
                .await?;
            }

            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn prepared_file_pragmas_cover_mmap_and_wal_frames() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let fdir = tempfile::tempdir()?;
            let rdir = tempfile::tempdir()?;
            let fpath = fdir.path().join("franken.db");
            let rpath = rdir.path().join("sqlite.db");
            let fconn = Connection::open(fpath.to_string_lossy().as_ref()).await?;
            let rconn = rusqlite::Connection::open(rpath)?;

            for sql in [
                "PRAGMA journal_mode = WAL",
                "PRAGMA mmap_size = 65536",
                "PRAGMA wal_autocheckpoint = 0",
            ] {
                assert_prepared_pragma_query_parity(&fconn, &rconn, sql).await?;
            }
            assert_prepared_pragma_query_parity(&fconn, &rconn, "PRAGMA mmap_size").await?;

            fconn.execute("CREATE TABLE framed(id INTEGER)").await?;
            fconn
                .execute("INSERT INTO framed VALUES (1),(2),(3)")
                .await?;
            rconn.execute_batch(
                "CREATE TABLE framed(id INTEGER); \
                 INSERT INTO framed VALUES (1),(2),(3);",
            )?;

            let fstmt = fconn.prepare("PRAGMA wal_checkpoint(PASSIVE)").await?;
            let rstmt = rconn.prepare("PRAGMA wal_checkpoint(PASSIVE)")?;
            assert_eq!(fstmt.column_count(), rstmt.column_count());
            assert_eq!(
                fstmt.column_names(),
                rstmt
                    .column_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            );
            let frows = fstmt.query().await?;
            let rrows = rusqlite_rows(&rconn, "PRAGMA wal_checkpoint(PASSIVE)")?;
            assert_eq!(frows.len(), 1);
            assert_eq!(rrows.len(), 1);
            assert_eq!(frows[0].values().len(), 3);
            let SqliteValue::Integer(f_log_frames) = frows[0].values()[1] else {
                panic!("FrankenSQLite WAL log-frame count was not an integer");
            };
            let r_log_frames = rrows[0][1].parse::<i64>()?;
            assert!(
                f_log_frames > 0,
                "FrankenSQLite WAL checkpoint did not observe written frames"
            );
            assert!(
                r_log_frames > 0,
                "SQLite WAL checkpoint did not observe written frames"
            );

            let faux_path = fdir.path().join("aux.db");
            let raux_path = rdir.path().join("aux.db");
            let faux_path_sql = faux_path.to_string_lossy().replace('\'', "''");
            let raux_path_sql = raux_path.to_string_lossy().replace('\'', "''");
            fconn
                .execute(&format!("ATTACH DATABASE '{faux_path_sql}' AS aux"))
                .await?;
            rconn.execute_batch(&format!("ATTACH DATABASE '{raux_path_sql}' AS aux;"))?;
            for sql in [
                "PRAGMA aux.locking_mode = EXCLUSIVE",
                "PRAGMA aux.locking_mode",
                "PRAGMA aux.secure_delete = ON",
                "PRAGMA aux.secure_delete",
            ] {
                assert_prepared_pragma_query_parity(&fconn, &rconn, sql).await?;
            }
            Ok(())
        }
        .await;
    });
    outcome
}
