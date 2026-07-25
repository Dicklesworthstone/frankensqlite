#![cfg(feature = "async-api")]

//! G0 integration regression for the async facade over the async storage stack.
//!
//! This target stays separate from the crate's legacy unit-test module so the
//! facade can retain executable evidence while that module is migrated to the
//! async `Connection` API.

use asupersync::runtime::RuntimeBuilder;
use fsqlite::{AsyncConnection, FrankenError, SqliteValue};
use fsqlite_types::cx::Cx;

#[test]
fn async_facade_drives_file_backed_storage_futures_to_completion() {
    let runtime = RuntimeBuilder::current_thread()
        .blocking_threads(2, 2)
        .build()
        .expect("test runtime should build");

    runtime.block_on(async {
        let directory = tempfile::tempdir().expect("temporary database directory");
        let database_path = directory.path().join("async-facade.db");
        let database_path = database_path.to_string_lossy().into_owned();
        let cx = Cx::new();

        let mut connection = AsyncConnection::open(&cx, database_path.clone())
            .await
            .expect("file-backed async connection should open");

        connection
            .execute_batch(
                &cx,
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
            )
            .await
            .expect("schema batch should complete");
        connection
            .execute_with_params(
                &cx,
                "INSERT INTO items (id, name) VALUES (?1, ?2)",
                &[SqliteValue::Integer(1), SqliteValue::Text("one".into())],
            )
            .await
            .expect("parameterized insert should complete");

        let rows = connection
            .query_with_params(
                &cx,
                "SELECT name FROM items WHERE id = ?1",
                &[SqliteValue::Integer(1)],
            )
            .await
            .expect("parameterized query should complete");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Text("one".into())));

        let row = connection
            .query_row_with_params(
                &cx,
                "SELECT name FROM items WHERE id = ?1",
                &[SqliteValue::Integer(1)],
            )
            .await
            .expect("parameterized row query should complete");
        assert_eq!(row.get(0), Some(&SqliteValue::Text("one".into())));

        connection
            .begin_transaction(&cx)
            .await
            .expect("transaction should begin");
        assert!(connection.in_transaction());
        connection
            .execute(&cx, "INSERT INTO items (id, name) VALUES (2, 'two')")
            .await
            .expect("transactional insert should complete");
        connection
            .rollback_transaction(&cx)
            .await
            .expect("transaction should roll back");
        assert!(!connection.in_transaction());
        assert!(
            connection
                .query(&cx, "SELECT id FROM items WHERE id = 2")
                .await
                .expect("post-rollback query should complete")
                .is_empty()
        );

        connection
            .begin_transaction(&cx)
            .await
            .expect("second transaction should begin");
        connection
            .execute(&cx, "INSERT INTO items (id, name) VALUES (3, 'three')")
            .await
            .expect("committed insert should complete");
        connection
            .commit_transaction(&cx)
            .await
            .expect("transaction should commit");
        assert!(!connection.in_transaction());

        let cancelled = Cx::new();
        cancelled.cancel();
        assert!(matches!(
            connection.query(&cancelled, "SELECT 1").await,
            Err(FrankenError::Interrupt)
        ));

        connection
            .close(&cx)
            .await
            .expect("explicit close should complete");

        let mut reopened = AsyncConnection::open(&cx, database_path)
            .await
            .expect("committed database should reopen");
        let row = reopened
            .query_row(&cx, "SELECT name FROM items WHERE id = 3")
            .await
            .expect("committed row should survive reopen");
        assert_eq!(row.get(0), Some(&SqliteValue::Text("three".into())));
        reopened
            .close(&cx)
            .await
            .expect("reopened connection should close");
    });
}
