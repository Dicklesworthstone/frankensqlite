use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::SqliteValue;
use std::io::Write as _;

async fn table_issue_ids(conn: &Connection) -> Vec<String> {
    conn.query("SELECT id FROM issues ORDER BY rowid")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|row| {
            row.values()
                .first()
                .and_then(SqliteValue::as_text)
                .map(ToOwned::to_owned)
        })
        .collect()
}

async fn keyed_issue_lookup(conn: &Connection, issue_id: &str) -> Vec<String> {
    conn.query_with_params(
        "SELECT id FROM issues WHERE id = ?",
        &[SqliteValue::Text(issue_id.to_owned().into())],
    )
    .await
    .unwrap()
    .into_iter()
    .filter_map(|row| {
        row.values()
            .first()
            .and_then(SqliteValue::as_text)
            .map(ToOwned::to_owned)
    })
    .collect()
}

async fn query_row_issue_lookup(conn: &Connection, issue_id: &str) -> Option<String> {
    match conn
        .query_row_with_params(
            "SELECT id FROM issues WHERE id = ?",
            &[SqliteValue::Text(issue_id.to_owned().into())],
        )
        .await
    {
        Ok(row) => row
            .values()
            .first()
            .and_then(SqliteValue::as_text)
            .map(ToOwned::to_owned),
        Err(FrankenError::QueryReturnedNoRows) => None,
        Err(error) => panic!("query_row issue lookup failed for {issue_id}: {error}"),
    }
}

async fn create_beads_like_issues_table(conn: &Connection) {
    conn.execute(
        "CREATE TABLE issues (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            priority INTEGER NOT NULL DEFAULT 2,
            created_at TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT ''
        );",
    )
    .await
    .unwrap();
}

async fn rebuild_beads_like_tables(conn: &Connection) {
    conn.execute("DROP TABLE IF EXISTS issues;").await.unwrap();
    create_beads_like_issues_table(conn).await;
}

async fn seed_imported_rows(conn: &Connection, imported_count: usize) {
    for i in 0..imported_count {
        conn.execute_with_params(
            "INSERT INTO issues(id, title, status, priority, created_at, updated_at)
             VALUES (?, ?, 'open', 2, '2026-04-18T00:00:00Z', '2026-04-18T00:00:00Z')",
            &[
                SqliteValue::Text(format!("alt-import-{i:04}").into()),
                SqliteValue::Text(format!("Imported issue {i}").into()),
            ],
        )
        .await
        .unwrap();
    }
}

async fn run_rebuilt_reopen_lookup_matrix(reject_mem_fallback: bool) {
    const IMPORTED_COUNT: usize = 300;
    const FRESH_LOOP_COUNT: usize = 30;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join(if reject_mem_fallback {
        "beads-rebuild-reject-mem.db"
    } else {
        "beads-rebuild-default.db"
    });
    let db_str = db_path.to_string_lossy().into_owned();

    {
        let conn = Connection::open(db_str.clone()).await.unwrap();
        create_beads_like_issues_table(&conn).await;
        conn.execute(
            "INSERT INTO issues(id, title, status, priority, created_at, updated_at)
             VALUES
             ('alt-seed-a', 'Seed A', 'open', 2, '2026-04-18T00:00:00Z', '2026-04-18T00:00:00Z'),
             ('alt-seed-b', 'Seed B', 'open', 2, '2026-04-18T00:00:00Z', '2026-04-18T00:00:00Z');",
        )
        .await
        .unwrap();
    }

    // Mimic `br sync --import-only --rebuild`: drop data tables, recreate them,
    // import a large batch, then continue using the rebuilt DB file.
    {
        let conn = Connection::open(db_str.clone()).await.unwrap();
        rebuild_beads_like_tables(&conn).await;
        seed_imported_rows(&conn, IMPORTED_COUNT).await;
    }

    for i in 0..FRESH_LOOP_COUNT {
        let fresh_id = format!("alt-fresh-{i:04}");
        let fresh_title = format!("Fresh issue {i}");

        {
            let conn = Connection::open(db_str.clone()).await.unwrap();
            conn.execute_with_params(
                "INSERT INTO issues(id, title, status, priority, created_at, updated_at)
                 VALUES (?, ?, 'open', 2, '2026-04-18T00:00:00Z', '2026-04-18T00:00:00Z')",
                &[
                    SqliteValue::Text(fresh_id.clone().into()),
                    SqliteValue::Text(fresh_title.clone().into()),
                ],
            )
            .await
            .unwrap();
        }

        let conn = Connection::open(db_str.clone()).await.unwrap();
        conn.set_reject_mem_fallback(reject_mem_fallback);

        let all_ids = table_issue_ids(&conn).await;
        assert!(
            all_ids.iter().any(|id| id == &fresh_id),
            "full table scan could not find freshly inserted id {fresh_id} \
             after rebuild/reopen (reject_mem_fallback={reject_mem_fallback})"
        );

        let keyed_rows = keyed_issue_lookup(&conn, &fresh_id).await;
        assert_eq!(
            keyed_rows,
            vec![fresh_id.clone()],
            "indexed equality lookup diverged from full scan for {fresh_id} \
             after rebuild/reopen (reject_mem_fallback={reject_mem_fallback}). \
             full_scan_tail={:?}",
            &all_ids[all_ids.len().saturating_sub(8)..]
        );

        let query_row = query_row_issue_lookup(&conn, &fresh_id).await;
        assert_eq!(
            query_row.as_deref(),
            Some(fresh_id.as_str()),
            "query_row keyed lookup diverged for {fresh_id} after rebuild/reopen \
             (reject_mem_fallback={reject_mem_fallback}); keyed_rows={keyed_rows:?}"
        );
    }
}

#[test]
fn file_backed_rebuild_reopen_text_lookup_matches_full_scan_default_mode() {
    asupersync::test_utils::run_test(|| async {
        run_rebuilt_reopen_lookup_matrix(false).await;
    });
}

#[test]
fn file_backed_rebuild_reopen_text_lookup_matches_full_scan_reject_mem_fallback() {
    asupersync::test_utils::run_test(|| async {
        run_rebuilt_reopen_lookup_matrix(true).await;
    });
}

#[test]
fn reopen_after_same_path_file_replacement_reads_new_incarnation() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("replace-reopen.db");
        let replacement_path = dir.path().join("replacement.db");

        let old_conn = Connection::open(db_path.to_string_lossy().into_owned())
            .await
            .unwrap();
        old_conn
            .execute("CREATE TABLE marker(value TEXT NOT NULL);")
            .await
            .unwrap();
        for value in ["old-1", "old-2", "old-3"] {
            old_conn
                .execute_with_params(
                    "INSERT INTO marker(value) VALUES (?)",
                    &[SqliteValue::Text(value.into())],
                )
                .await
                .unwrap();
        }
        let old_identity = old_conn.file_identity().await.unwrap().unwrap();

        let replacement_identity = {
            let conn = Connection::open(replacement_path.to_string_lossy().into_owned())
                .await
                .unwrap();
            conn.execute("CREATE TABLE marker(value TEXT NOT NULL);")
                .await
                .unwrap();
            conn.execute("INSERT INTO marker(value) VALUES ('new-incarnation');")
                .await
                .unwrap();
            let identity = conn.file_identity().await.unwrap().unwrap();
            conn.close().await.unwrap();
            identity
        };
        assert_ne!(old_identity, replacement_identity);

        // A database-family replacement discards recovery artifacts belonging
        // to the old inode. Move them aside instead of letting an old WAL
        // legitimately replay old rows over the replacement image.
        for suffix in ["-journal", "-wal", "-shm", "-wal-fec"] {
            let mut sidecar = db_path.as_os_str().to_owned();
            sidecar.push(suffix);
            let sidecar = std::path::PathBuf::from(sidecar);
            if sidecar.exists() {
                let mut displaced = db_path.as_os_str().to_owned();
                displaced.push(format!(".pre-replacement{suffix}"));
                std::fs::rename(sidecar, std::path::PathBuf::from(displaced)).unwrap();
            }
        }

        // Match the downstream replacement flow: release the application
        // handle immediately before the atomic swap, leaving no time for a
        // path-keyed process registry to age out before the reopen.
        drop(old_conn);
        std::fs::rename(&replacement_path, &db_path).unwrap();

        let reopened = Connection::open(db_path.to_string_lossy().into_owned())
            .await
            .unwrap();
        assert_eq!(
            reopened.file_identity().await.unwrap().unwrap(),
            replacement_identity
        );
        let rows = reopened
            .query("SELECT value FROM marker ORDER BY rowid")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0].as_text(),
            Some("new-incarnation"),
            "same-path reopen must read the replacement inode"
        );
        assert_eq!(rows.len(), 1);
        reopened.close().await.unwrap();
    });
}

#[test]
fn vacuum_into_compacts_database_with_trailing_whole_page_slack() {
    asupersync::test_utils::run_test(|| async {
        const PAGE_SIZE: usize = 4096;
        const APPENDED_PAGES: usize = 64;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("trailing-slack.db");
        let compacted_path = dir.path().join("compacted.db");
        let db_str = db_path.to_string_lossy().into_owned();

        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode=DELETE;
                 CREATE TABLE marker(value TEXT NOT NULL);
                 INSERT INTO marker(value) VALUES ('survives');",
            )
            .unwrap();
        }

        let logical_len = std::fs::metadata(&db_path).unwrap().len();
        let logical_bytes = std::fs::read(&db_path).unwrap();
        let header_pages_before = u32::from_be_bytes([
            logical_bytes[28],
            logical_bytes[29],
            logical_bytes[30],
            logical_bytes[31],
        ]);
        let mut db_file = std::fs::OpenOptions::new()
            .append(true)
            .open(&db_path)
            .unwrap();
        db_file
            .write_all(&vec![0_u8; PAGE_SIZE * APPENDED_PAGES])
            .unwrap();
        db_file.flush().unwrap();
        drop(db_file);
        let bloated_len = std::fs::metadata(&db_path).unwrap().len();
        assert!(bloated_len > logical_len);
        assert_eq!(
            u64::from(header_pages_before) * PAGE_SIZE as u64,
            logical_len,
            "fixture must start with an exact logical extent"
        );
        assert_eq!(
            bloated_len,
            logical_len + (PAGE_SIZE * APPENDED_PAGES) as u64,
            "fixture must add whole-page trailing slack without changing the header"
        );

        let conn = Connection::open(db_str).await.unwrap();
        conn.execute_with_params(
            "VACUUM INTO ?1;",
            &[SqliteValue::Text(
                compacted_path.to_string_lossy().into_owned().into(),
            )],
        )
        .await
        .unwrap();

        let compacted = Connection::open(compacted_path.to_string_lossy().into_owned())
            .await
            .unwrap();
        let rows = compacted.query("SELECT value FROM marker").await.unwrap();
        assert_eq!(rows[0].values()[0].as_text(), Some("survives"));

        let bytes = std::fs::read(&compacted_path).unwrap();
        let header_pages = u32::from_be_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]);
        assert!(
            bytes.len() < bloated_len as usize,
            "VACUUM INTO must discard, not adopt, trailing slack"
        );
        assert!(
            header_pages < header_pages_before + APPENDED_PAGES as u32,
            "VACUUM INTO must not turn appended slack into logical pages"
        );
        assert_eq!(
            u64::from(header_pages) * PAGE_SIZE as u64,
            bytes.len() as u64,
            "VACUUM INTO output must omit source trailing slack"
        );
        compacted.close().await.unwrap();
        conn.close().await.unwrap();
    });
}
