#![cfg(feature = "async-api")]

//! G0 integration regression for the async facade over the async storage stack.
//!
//! This target stays separate from the crate's legacy unit-test module so the
//! facade can retain executable evidence while that module is migrated to the
//! async `Connection` API.

use asupersync::runtime::RuntimeBuilder;
use fsqlite::{AsyncConnection, FrankenError, SqliteValue, Transaction};
use fsqlite_types::cx::Cx;

fn assert_send<T: Send + ?Sized>(_: &T) {}

fn assert_send_sync<T: Send + Sync>() {}

fn assert_connection_futures_are_send(connection: &mut AsyncConnection, cx: &Cx) {
    assert_send(&AsyncConnection::open(cx, ":memory:"));
    assert_send(&connection.query(cx, "SELECT 1"));
    assert_send(&connection.begin_transaction(cx));
    assert_send(&connection.close(cx));
}

fn assert_transaction_futures_are_send(transaction: &mut Transaction<'_>, cx: &Cx) {
    assert_send(&transaction.prepare(cx, "SELECT 1"));
    assert_send(&transaction.query(cx, "SELECT 1"));
    assert_send(&transaction.execute(cx, "SELECT 1"));
    assert_send(&transaction.commit(cx));
    assert_send(&transaction.rollback(cx));
}

#[test]
fn async_actor_public_types_are_send_sync() {
    assert_send_sync::<AsyncConnection>();
    assert_send_sync::<Transaction<'static>>();
}

#[test]
fn async_facade_drives_file_backed_storage_futures_to_completion() {
    let runtime = RuntimeBuilder::current_thread()
        // The engine owns a separate large-stack thread. One runtime blocking
        // slot is therefore sufficient for the sequential response waiters.
        .blocking_threads(1, 1)
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
        assert_connection_futures_are_send(&mut connection, &cx);

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

        let mut transaction = connection
            .begin_transaction(&cx)
            .await
            .expect("transaction should begin");
        assert_transaction_futures_are_send(&mut transaction, &cx);
        assert!(connection.in_transaction());
        transaction
            .prepare(&cx, "INSERT INTO items (id, name) VALUES (?1, ?2)")
            .await
            .expect("transaction-scoped prepare should retain actor ownership");
        transaction
            .execute(&cx, "INSERT INTO items (id, name) VALUES (2, 'two')")
            .await
            .expect("transactional insert should complete");
        transaction
            .rollback(&cx)
            .await
            .expect("transaction should roll back");
        drop(transaction);
        assert!(!connection.in_transaction());
        assert!(
            connection
                .query(&cx, "SELECT id FROM items WHERE id = 2")
                .await
                .expect("post-rollback query should complete")
                .is_empty()
        );

        let mut transaction = connection
            .begin_transaction(&cx)
            .await
            .expect("second transaction should begin");
        transaction
            .execute(&cx, "INSERT INTO items (id, name) VALUES (3, 'three')")
            .await
            .expect("committed insert should complete");
        assert_eq!(
            transaction
                .last_insert_rowid(&cx)
                .await
                .expect("transaction-scoped row id should cross the actor boundary"),
            3
        );
        transaction
            .commit(&cx)
            .await
            .expect("transaction should commit");
        drop(transaction);
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

#[test]
fn sync_facade_owns_storage_futures_on_its_worker() {
    let directory = tempfile::tempdir().expect("temporary database directory");
    let database_path = directory.path().join("sync-facade.db");
    let database_path = database_path.to_string_lossy().into_owned();
    let mut connection =
        AsyncConnection::open_sync(database_path).expect("file-backed sync connection should open");

    connection
        .execute_batch_sync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
             INSERT INTO items(name) VALUES ('before');",
        )
        .expect("schema and seed batch should complete");
    connection
        .prepare_sync("SELECT id, name FROM items WHERE id = ?1")
        .expect("statement validation should complete on the worker");

    assert_eq!(
        connection
            .execute_with_params_sync(
                "INSERT INTO items(name) VALUES (?1)",
                &[SqliteValue::Text("after".into())],
            )
            .expect("parameterized insert should complete"),
        1
    );
    assert_eq!(
        connection
            .last_insert_rowid_sync()
            .expect("last inserted row id should cross the worker boundary"),
        2
    );

    let row = connection
        .query_row_with_params_sync(
            "SELECT id, name FROM items WHERE id = ?1",
            &[SqliteValue::Integer(2)],
        )
        .expect("parameterized row query should complete");
    assert_eq!(row.get(0), Some(&SqliteValue::Integer(2)));
    assert_eq!(row.get(1), Some(&SqliteValue::Text("after".into())));

    let mut streamed_ids = Vec::new();
    connection
        .query_with_params_for_each_sync(
            "SELECT id FROM items WHERE id >= ?1 ORDER BY id",
            &[SqliteValue::Integer(1)],
            |row| {
                streamed_ids.push(row.get(0).cloned());
                Ok(())
            },
        )
        .expect("bounded row stream should complete");
    assert_eq!(
        streamed_ids,
        vec![Some(SqliteValue::Integer(1)), Some(SqliteValue::Integer(2))]
    );

    let mut transaction = connection
        .begin_transaction_sync()
        .expect("batch transaction should begin");
    transaction
        .prepare_sync("SELECT id FROM items WHERE id >= ?1")
        .expect("transaction-scoped prepare should retain actor ownership");
    let mut pre_batch_ids = Vec::new();
    transaction
        .query_with_params_for_each_sync(
            "SELECT id FROM items WHERE id >= ?1 ORDER BY id",
            &[SqliteValue::Integer(1)],
            |row| {
                pre_batch_ids.push(row.get(0).cloned());
                Ok(())
            },
        )
        .expect("transaction-scoped bounded stream should complete");
    assert_eq!(
        pre_batch_ids,
        vec![Some(SqliteValue::Integer(1)), Some(SqliteValue::Integer(2))]
    );
    assert_eq!(
        transaction
            .execute_many_with_params_sync(
                "INSERT INTO items(name) VALUES (?1)",
                &[
                    vec![SqliteValue::Text("batch-a".into())],
                    vec![SqliteValue::Text("batch-b".into())],
                ],
            )
            .expect("batched parameter sets should complete"),
        2
    );
    assert_eq!(
        transaction
            .last_insert_rowid_sync()
            .expect("transaction-scoped row id should cross the actor boundary"),
        4
    );
    transaction
        .commit_sync()
        .expect("batch transaction should commit");
    drop(transaction);
    let batch_names = connection
        .query_sync("SELECT name FROM items WHERE id >= 3 ORDER BY id")
        .expect("committed batch should be queryable");
    assert_eq!(
        batch_names
            .iter()
            .map(|row| row.get(0).cloned())
            .collect::<Vec<_>>(),
        vec![
            Some(SqliteValue::Text("batch-a".into())),
            Some(SqliteValue::Text("batch-b".into())),
        ]
    );

    let mut transaction = connection
        .begin_transaction_sync()
        .expect("failing batch transaction should begin");
    assert!(
        transaction
            .execute_many_with_params_sync(
                "INSERT INTO items(id, name) VALUES (?1, ?2)",
                &[
                    vec![
                        SqliteValue::Integer(10),
                        SqliteValue::Text("pending".into()),
                    ],
                    vec![
                        SqliteValue::Integer(1),
                        SqliteValue::Text("duplicate".into()),
                    ],
                ],
            )
            .is_err()
    );
    assert!(connection.in_transaction());
    transaction
        .rollback_sync()
        .expect("caller should roll back a failed batch");
    drop(transaction);
    assert!(
        connection
            .query_sync("SELECT id FROM items WHERE id = 10")
            .expect("rolled-back batch should be queryable")
            .is_empty()
    );

    let mut transaction = connection
        .begin_transaction_sync()
        .expect("transaction should begin");
    assert!(connection.in_transaction());
    transaction
        .execute_sync("DELETE FROM items")
        .expect("transactional delete should complete");
    transaction
        .rollback_sync()
        .expect("transaction should roll back");
    drop(transaction);
    assert!(!connection.in_transaction());
    assert_eq!(
        connection
            .query_sync("SELECT id FROM items")
            .expect("post-rollback query should complete")
            .len(),
        4
    );

    connection
        .close_sync()
        .expect("explicit sync close should complete");
    assert!(connection.query_sync("SELECT 1").is_err());
}
