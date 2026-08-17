//! bd-o01lp / GH#357: `VACUUM INTO` must round-trip a contentless FTS5 table's
//! schema. The parent `CREATE VIRTUAL TABLE ... USING fts5(...)` sqlite_master
//! row is stored with rootpage=0 (no b-tree of its own); its shadow tables
//! (`%_config`, `%_data`, `%_docsize`, `%_idx`) are real b-trees. Before the
//! fix, `VACUUM INTO` copied the shadow tables but DROPPED the parent vtab
//! sqlite_master row, leaving an orphaned/undiscoverable FTS5 table in the
//! output.
//!
//! Distinct from #340 (WITHOUT ROWID roots) and #347 (non-compact output).
//!
//! Requires `--features ext-fts5` (default).

#![cfg(feature = "ext-fts5")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn bd_o01lp_vacuum_into_preserves_contentless_fts5_vtab_entry_gh357() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let source_path = dir.path().join("gh357-source.db");
        let target_path = dir.path().join("gh357-target.db");
        let source = source_path.to_string_lossy().into_owned();
        let target = target_path.to_string_lossy().into_owned();

        // Build the source exactly as the GH#357 hermetic repro does.
        let conn = Connection::open(&source).await.expect("open source");
        conn.execute("CREATE TABLE t(a TEXT);")
            .await
            .expect("create ordinary table");
        conn.execute("INSERT INTO t(a) VALUES ('ordinary-row');")
            .await
            .expect("insert ordinary row");
        conn.execute("CREATE VIRTUAL TABLE search USING fts5(body, content='');")
            .await
            .expect("create contentless fts5 vtab");
        conn.execute("INSERT INTO search(body) VALUES ('indexed-row');")
            .await
            .expect("insert indexed row");

        // Sanity: the SOURCE exposes the vtab entry (this is what the output
        // must round-trip).
        let src_master = conn
            .query(
                "SELECT name FROM sqlite_master WHERE name='search' AND type='table';",
            )
            .await
            .expect("query source sqlite_master");
        assert_eq!(
            src_master.len(),
            1,
            "source must expose the `search` vtab entry before VACUUM"
        );

        conn.execute_with_params(
            "VACUUM INTO ?1;",
            &[SqliteValue::Text(target.clone().into())],
        )
        .await
        .expect("VACUUM INTO output");
        conn.close().await.expect("close source");

        // Read the OUTPUT with the stock oracle (rusqlite), independent of
        // fsqlite's own reload logic — it reads the raw on-disk sqlite_master.
        // A bare SELECT over sqlite_master does not instantiate the vtab module,
        // so this works regardless of whether the oracle links FTS5.
        let oracle = rusqlite::Connection::open(&target_path).expect("open output with oracle");
        let master_rows: Vec<(String, String, i64, Option<String>)> = {
            let mut stmt = oracle
                .prepare(
                    "SELECT type, name, rootpage, sql FROM sqlite_master \
                     WHERE name='t' OR name LIKE 'search%' ORDER BY name;",
                )
                .expect("prepare oracle sqlite_master query");
            stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .expect("run oracle sqlite_master query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect oracle sqlite_master rows")
        };

        let names: Vec<&str> = master_rows.iter().map(|r| r.1.as_str()).collect();

        // GH#357 core assertion: the parent vtab entry survived the VACUUM.
        let search_entry = master_rows
            .iter()
            .find(|r| r.1 == "search")
            .unwrap_or_else(|| {
                panic!(
                    "GH#357: VACUUM INTO output is missing the `search` vtab entry; \
                     sqlite_master names present = {names:?}"
                )
            });
        assert_eq!(
            search_entry.0, "table",
            "the `search` vtab entry must have type='table'"
        );
        assert_eq!(
            search_entry.2, 0,
            "a virtual table's sqlite_master row must record rootpage=0"
        );
        let search_sql = search_entry
            .3
            .as_deref()
            .expect("the `search` vtab entry must carry its CREATE VIRTUAL TABLE sql");
        assert!(
            search_sql
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("CREATE VIRTUAL TABLE SEARCH"),
            "unexpected `search` vtab sql: {search_sql:?}"
        );

        // The shadow tables (real b-trees) must also be present, with positive
        // rootpages (their copied roots must stay valid).
        for shadow in [
            "search_config",
            "search_data",
            "search_docsize",
            "search_idx",
        ] {
            let entry = master_rows
                .iter()
                .find(|r| r.1 == shadow)
                .unwrap_or_else(|| panic!("output missing shadow table `{shadow}`: {names:?}"));
            assert!(
                entry.2 > 0,
                "shadow table `{shadow}` must keep a valid positive rootpage, got {}",
                entry.2
            );
        }

        // The ordinary table `t` must round-trip its row.
        assert!(
            names.contains(&"t"),
            "output missing ordinary table `t`: {names:?}"
        );
        let t_value: String = oracle
            .query_row("SELECT a FROM t;", [], |row| row.get(0))
            .expect("read ordinary row from output");
        assert_eq!(t_value, "ordinary-row", "the `t` row must round-trip");
        drop(oracle);

        // FTS5 usability: reopen the output with fsqlite (ext-fts5 default) and
        // confirm the vacuumed vtab is discoverable AND queryable via MATCH.
        // NB: a contentless FTS5 table (content='') does not store the indexed
        // column text, so reading `body` returns NULL — the retrievable proof of
        // a working index is the matched rowid, exactly as stock SQLite reports.
        let reopened = Connection::open(&target).await.expect("reopen output");
        let match_rows = reopened
            .query("SELECT rowid FROM search WHERE search MATCH 'indexed-row';")
            .await
            .expect("MATCH query against vacuumed fts5 table");
        assert_eq!(
            match_rows.len(),
            1,
            "the vacuumed contentless FTS5 table must return its indexed row via MATCH"
        );
        assert_eq!(
            match_rows[0].values()[0],
            SqliteValue::Integer(1),
            "MATCH must surface the indexed row's rowid"
        );
        reopened.close().await.expect("close reopened output");
    });
}
