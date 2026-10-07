//! View-expansion keepers.
//!
//! - Self-referential and mutually recursive views are rejected with SQLite's
//!   "circularly defined" error on a deliberately small (1 MiB) native stack,
//!   and the connection stays usable afterwards.
//! - A trigger body that reads through a view with a correlated scalar
//!   subquery on `NEW` sees exactly the row that fired it.
//!
//! Salvaged from the July 2026 codex phase-C working copy
//! (`frankensqlite_codex_phasec_clean_20260727`, connection.rs unit tests).
//! That copy also asserted a 128-level acyclic view chain on the same 1 MiB
//! stack; acyclic chains are covered by `bd_5haia_view_chain_depth.rs`
//! (bd-5haia), which runs them 128 and 1000 views deep on a 2 MiB thread.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn view_cycles_are_rejected_on_a_small_stack_and_connection_stays_usable() {
    std::thread::Builder::new()
        .name("view-cycle-small-stack".to_owned())
        .stack_size(1024 * 1024)
        .spawn(|| {
            let dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
            tracing::dispatcher::with_default(&dispatch, || {
                let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
                    .build()
                    .expect("view-cycle runtime should build");
                // Drive one engine future at a time so the test's own async
                // state machine does not embed every child future at once.
                let conn = runtime.block_on(Connection::open(":memory:")).unwrap();

                runtime
                    .block_on(conn.execute("CREATE VIEW self_cycle AS SELECT * FROM self_cycle;"))
                    .unwrap();
                let error = runtime
                    .block_on(conn.query("SELECT * FROM self_cycle;"))
                    .expect_err("a self-referential view must be rejected");
                assert!(
                    error
                        .to_string()
                        .contains("view self_cycle is circularly defined"),
                    "unexpected self-cycle error: {error}"
                );
                runtime
                    .block_on(conn.query("SELECT 1;"))
                    .expect("connection must remain usable after rejecting a view cycle");
                runtime
                    .block_on(conn.execute("DROP VIEW self_cycle;"))
                    .unwrap();

                runtime
                    .block_on(conn.execute("CREATE VIEW mutual_a AS SELECT * FROM mutual_b;"))
                    .unwrap();
                runtime
                    .block_on(conn.execute("CREATE VIEW mutual_b AS SELECT * FROM mutual_a;"))
                    .unwrap();
                let error = runtime
                    .block_on(conn.query("SELECT * FROM mutual_a;"))
                    .expect_err("mutually recursive views must be rejected");
                assert!(
                    error
                        .to_string()
                        .contains("view mutual_a is circularly defined"),
                    "unexpected mutual-cycle error: {error}"
                );
                let rows = runtime
                    .block_on(conn.query("SELECT 41 + 1;"))
                    .expect("cycle rejection must not poison later statements");
                assert_eq!(rows[0].values()[0], SqliteValue::Integer(42));
                runtime
                    .block_on(conn.execute("DROP VIEW mutual_a;"))
                    .unwrap();
                runtime
                    .block_on(conn.execute("DROP VIEW mutual_b;"))
                    .unwrap();
            });
        })
        .expect("spawn 1 MiB view-cycle thread")
        .join()
        .expect("view-cycle thread panicked");
}

#[test]
fn trigger_body_reads_through_view_with_correlated_new_subquery() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE source (id INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE log (id INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE VIEW source_view AS SELECT id FROM source;")
            .await
            .unwrap();
        conn.execute(
            "CREATE TRIGGER source_log AFTER INSERT ON source BEGIN
               INSERT INTO log
               SELECT id FROM source_view
               WHERE id = (SELECT NEW.id);
             END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO source VALUES (7);")
            .await
            .unwrap();
        // A second row makes the correlated filter observable: stock SQLite
        // logs [7, 9]; ignoring `(SELECT NEW.id)` would log [7, 7, 9].
        conn.execute("INSERT INTO source VALUES (9);")
            .await
            .unwrap();
        let rows = conn
            .query("SELECT id FROM log ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.values()[0].clone())
                .collect::<Vec<_>>(),
            vec![SqliteValue::Integer(7), SqliteValue::Integer(9)]
        );
    });
}
