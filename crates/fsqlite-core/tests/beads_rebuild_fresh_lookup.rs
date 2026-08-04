use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::SqliteValue;

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
