#![recursion_limit = "512"]

//! Foreign-key edge semantics pinned against stock SQLite (rusqlite).
//!
//! Each case drives the public `Connection` API and, where the original probe
//! did, repeats the same script on rusqlite as the semantic control:
//!
//! - a failed deferred COMMIT keeps the transaction and its obligation, and a
//!   later repair lets the same transaction commit (child-side and
//!   parent-side NO ACTION, DELETE and UPDATE);
//! - savepoint and statement rollback discard a recorded deferred obligation;
//! - RESTRICT stays statement-immediate for both UPDATE and DELETE even when
//!   declared DEFERRABLE INITIALLY DEFERRED;
//! - post-mutation FK rejection restores the base DELETE/UPDATE rows;
//! - a nested ON DELETE SET DEFAULT whose default parent a trigger just
//!   deleted in the same statement is re-probed, fails, and rolls back the
//!   whole statement;
//! - FK violations and runtime trigger failures ignore the OR FAIL
//!   partial-write policy and roll back the whole statement;
//! - self-referential actions that rewrite or remove the row match SQLite;
//! - shorthand `REFERENCES parent` uses the parent's declared PRIMARY KEY
//!   (non-IPK and composite WITHOUT ROWID);
//! - a parent deleted by a nested trigger is re-probed, never served from a
//!   statement-scoped FK cache;
//! - a full ROLLBACK discards deferred obligations.
//!
//! Salvaged from the July 2026 codex phase-C working copy
//! (`frankensqlite_codex_phasec_clean_20260727`, connection.rs unit tests);
//! assertions on that copy's private connection fields were dropped.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

fn rows_as_values(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(|row| row.values().to_vec()).collect()
}

async fn scalar(conn: &Connection, sql: &str) -> SqliteValue {
    conn.query(sql).await.unwrap()[0].values()[0].clone()
}

#[test]
fn deferred_child_fk_checks_final_state_after_delete_or_repair_like_sqlite() {
    asupersync::test_utils::run_test(|| async {
        const PARENT_SQL: &str = "CREATE TABLE parent (id INTEGER PRIMARY KEY);";
        const CHILD_SQL: &str = "CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
        );";

        for (label, repair_sql, expected_child_count) in [
            ("delete orphan", "DELETE FROM child WHERE id = 10;", 0_i64),
            (
                "re-parent orphan",
                "UPDATE child SET parent_id = 1 WHERE id = 10;",
                1_i64,
            ),
            (
                "set orphan key to NULL",
                "UPDATE child SET parent_id = NULL WHERE id = 10;",
                1_i64,
            ),
        ] {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
            conn.execute(PARENT_SQL).await.unwrap();
            conn.execute(CHILD_SQL).await.unwrap();
            conn.execute("INSERT INTO parent VALUES (1);")
                .await
                .unwrap();
            conn.execute("BEGIN;").await.unwrap();
            conn.execute("INSERT INTO child VALUES (10, 99);")
                .await
                .unwrap_or_else(|error| {
                    panic!("{label}: deferred child violation must reach COMMIT: {error}")
                });
            for attempt in 1..=2 {
                let error = conn
                    .execute("COMMIT;")
                    .await
                    .expect_err("unrepaired child-side deferred FK must reject COMMIT");
                assert!(
                    matches!(error, FrankenError::ForeignKeyViolation),
                    "{label}: unexpected COMMIT error: {error:?}"
                );
                assert!(
                    conn.in_transaction(),
                    "{label}: failed child-side COMMIT #{attempt} must keep the transaction active",
                );
            }
            conn.execute(repair_sql)
                .await
                .unwrap_or_else(|error| panic!("{label}: repair failed: {error}"));
            conn.execute("COMMIT;")
                .await
                .unwrap_or_else(|error| panic!("{label}: final-state repair must commit: {error}"));
            assert!(!conn.in_transaction(), "{label}");
            assert_eq!(
                scalar(&conn, "SELECT COUNT(*) FROM child;").await,
                SqliteValue::Integer(expected_child_count),
                "{label}: FrankenSQLite final state",
            );

            let sqlite = rusqlite::Connection::open_in_memory().unwrap();
            sqlite.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            sqlite.execute_batch(PARENT_SQL).unwrap();
            sqlite.execute_batch(CHILD_SQL).unwrap();
            sqlite
                .execute("INSERT INTO parent VALUES (1);", [])
                .unwrap();
            sqlite.execute_batch("BEGIN;").unwrap();
            sqlite
                .execute("INSERT INTO child VALUES (10, 99);", [])
                .unwrap();
            sqlite
                .execute_batch("COMMIT;")
                .expect_err("SQLite must reject the first unrepaired child-side COMMIT");
            sqlite
                .execute_batch("COMMIT;")
                .expect_err("SQLite must retain child-side deferred obligations for a retry");
            sqlite.execute(repair_sql, []).unwrap();
            sqlite.execute_batch("COMMIT;").unwrap();
            let sqlite_child_count: i64 = sqlite
                .query_row("SELECT COUNT(*) FROM child;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                sqlite_child_count, expected_child_count,
                "{label}: SQLite control"
            );
        }
    });
}

#[test]
fn deferred_no_action_parent_delete_rechecks_after_failed_commit_and_child_repair() {
    asupersync::test_utils::run_test(|| async {
        const PARENT_SQL: &str = "CREATE TABLE parent (id INTEGER PRIMARY KEY);";
        const CHILD_SQL: &str = "CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES parent(id)
                ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
        );";

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute(PARENT_SQL).await.unwrap();
        conn.execute(CHILD_SQL).await.unwrap();
        conn.execute("INSERT INTO parent VALUES (1);")
            .await
            .unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1);")
            .await
            .unwrap();

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("DELETE FROM parent WHERE id = 1;")
            .await
            .expect("deferred NO ACTION must allow the parent DELETE until COMMIT");
        for attempt in 1..=2 {
            let error = conn
                .execute("COMMIT;")
                .await
                .expect_err("unrepaired deferred NO ACTION must reject COMMIT");
            assert!(matches!(error, FrankenError::ForeignKeyViolation));
            assert!(
                conn.in_transaction(),
                "failed COMMIT #{attempt} must keep the transaction active"
            );
        }

        conn.execute("DELETE FROM child WHERE id = 10;")
            .await
            .expect("removing the remaining child must repair the deferred relationship");
        conn.execute("COMMIT;")
            .await
            .expect("repairing the child must make the retrying COMMIT succeed");
        assert!(
            conn.query("SELECT * FROM parent;")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(conn.query("SELECT * FROM child;").await.unwrap().is_empty());

        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        sqlite.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        sqlite.execute_batch(PARENT_SQL).unwrap();
        sqlite.execute_batch(CHILD_SQL).unwrap();
        sqlite
            .execute("INSERT INTO parent VALUES (1);", [])
            .unwrap();
        sqlite
            .execute("INSERT INTO child VALUES (10, 1);", [])
            .unwrap();
        sqlite.execute_batch("BEGIN;").unwrap();
        sqlite
            .execute("DELETE FROM parent WHERE id = 1;", [])
            .unwrap();
        sqlite
            .execute_batch("COMMIT;")
            .expect_err("SQLite must reject the first unrepaired COMMIT");
        sqlite
            .execute_batch("COMMIT;")
            .expect_err("SQLite must retain the deferred obligation for a retry");
        sqlite
            .execute("DELETE FROM child WHERE id = 10;", [])
            .unwrap();
        sqlite.execute_batch("COMMIT;").unwrap();
        let counts: (i64, i64) = sqlite
            .query_row(
                "SELECT (SELECT COUNT(*) FROM parent), (SELECT COUNT(*) FROM child);",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (0, 0));
    });
}

#[test]
fn deferred_no_action_parent_update_rechecks_after_failed_commit_and_child_repair() {
    asupersync::test_utils::run_test(|| async {
        const PARENT_SQL: &str = "CREATE TABLE parent (id INTEGER PRIMARY KEY);";
        const CHILD_SQL: &str = "CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES parent(id)
                ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED
        );";

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute(PARENT_SQL).await.unwrap();
        conn.execute(CHILD_SQL).await.unwrap();
        conn.execute("INSERT INTO parent VALUES (1);")
            .await
            .unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1);")
            .await
            .unwrap();

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("UPDATE parent SET id = 2 WHERE id = 1;")
            .await
            .expect("deferred NO ACTION must allow the parent UPDATE until COMMIT");
        assert_eq!(
            scalar(&conn, "SELECT id FROM parent;").await,
            SqliteValue::Integer(2)
        );
        assert_eq!(
            scalar(&conn, "SELECT parent_id FROM child;").await,
            SqliteValue::Integer(1)
        );
        for attempt in 1..=2 {
            let error = conn
                .execute("COMMIT;")
                .await
                .expect_err("unrepaired deferred NO ACTION must reject COMMIT");
            assert!(matches!(error, FrankenError::ForeignKeyViolation));
            assert!(
                conn.in_transaction(),
                "failed COMMIT #{attempt} must keep the transaction active"
            );
        }

        conn.execute("UPDATE child SET parent_id = 2 WHERE id = 10;")
            .await
            .expect("the application must be able to repair a failed deferred FK");
        conn.execute("COMMIT;")
            .await
            .expect("repairing the child must make the retrying COMMIT succeed");
        let joined = conn
            .query(
                "SELECT child.id, child.parent_id
                 FROM child JOIN parent ON parent.id = child.parent_id;",
            )
            .await
            .unwrap();
        assert_eq!(joined.len(), 1);

        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        sqlite.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        sqlite.execute_batch(PARENT_SQL).unwrap();
        sqlite.execute_batch(CHILD_SQL).unwrap();
        sqlite
            .execute("INSERT INTO parent VALUES (1);", [])
            .unwrap();
        sqlite
            .execute("INSERT INTO child VALUES (10, 1);", [])
            .unwrap();
        sqlite.execute_batch("BEGIN;").unwrap();
        sqlite
            .execute("UPDATE parent SET id = 2 WHERE id = 1;", [])
            .unwrap();
        sqlite
            .execute_batch("COMMIT;")
            .expect_err("SQLite must reject the first unrepaired COMMIT");
        sqlite
            .execute_batch("COMMIT;")
            .expect_err("SQLite must retain the deferred obligation for a retry");
        sqlite
            .execute("UPDATE child SET parent_id = 2 WHERE id = 10;", [])
            .unwrap();
        sqlite.execute_batch("COMMIT;").unwrap();
        let sqlite_rows: i64 = sqlite
            .query_row(
                "SELECT COUNT(*) FROM child JOIN parent ON parent.id = child.parent_id;",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sqlite_rows, 1);
    });
}

#[test]
fn deferred_parent_action_obligation_rolls_back_with_named_and_statement_savepoints() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parent(id)
                    ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
            );",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO parent VALUES (1);")
            .await
            .unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1);")
            .await
            .unwrap();

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("SAVEPOINT deferred_parent_action;")
            .await
            .unwrap();
        conn.execute("DELETE FROM parent WHERE id = 1;")
            .await
            .unwrap();
        conn.execute("ROLLBACK TO deferred_parent_action;")
            .await
            .unwrap();
        conn.execute("COMMIT;")
            .await
            .expect("rollback to the named savepoint must discard its deferred action obligation");

        conn.execute("BEGIN;").await.unwrap();
        conn.execute(
            "CREATE TRIGGER parent_abort_after_update
             AFTER UPDATE ON parent
             BEGIN SELECT RAISE(ABORT, 'stop'); END;",
        )
        .await
        .unwrap();
        let error = conn
            .execute("UPDATE parent SET id = 2 WHERE id = 1;")
            .await
            .expect_err("the trigger must roll the statement back after recording its obligation");
        assert!(
            matches!(error, FrankenError::FunctionError(_)),
            "unexpected RAISE(ABORT) error: {error:?}"
        );
        conn.execute("COMMIT;")
            .await
            .expect("a rolled-back statement must not poison a later COMMIT");
        assert_eq!(
            scalar(&conn, "SELECT id FROM parent;").await,
            SqliteValue::Integer(1)
        );
    });
}

#[test]
fn deferred_parent_update_allows_restoring_the_original_key_before_commit() {
    asupersync::test_utils::run_test(|| async {
        const PARENT_SQL: &str = "CREATE TABLE parent (id INTEGER PRIMARY KEY);";
        const CHILD_SQL: &str = "CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES parent(id)
                ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED
        );";

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute(PARENT_SQL).await.unwrap();
        conn.execute(CHILD_SQL).await.unwrap();
        conn.execute("INSERT INTO parent VALUES (1);")
            .await
            .unwrap();
        conn.execute("INSERT INTO child VALUES (10, 1);")
            .await
            .unwrap();
        conn.execute("BEGIN;").await.unwrap();
        conn.execute("UPDATE parent SET id = 2 WHERE id = 1;")
            .await
            .unwrap();
        conn.execute("UPDATE parent SET id = 1 WHERE id = 2;")
            .await
            .unwrap();
        conn.execute("COMMIT;")
            .await
            .expect("restoring the original parent key must satisfy final FK state");
        assert_eq!(
            scalar(&conn, "SELECT parent_id FROM child;").await,
            SqliteValue::Integer(1)
        );

        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        sqlite.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        sqlite.execute_batch(PARENT_SQL).unwrap();
        sqlite.execute_batch(CHILD_SQL).unwrap();
        sqlite
            .execute("INSERT INTO parent VALUES (1);", [])
            .unwrap();
        sqlite
            .execute("INSERT INTO child VALUES (10, 1);", [])
            .unwrap();
        sqlite.execute_batch("BEGIN;").unwrap();
        sqlite
            .execute("UPDATE parent SET id = 2 WHERE id = 1;", [])
            .unwrap();
        sqlite
            .execute("UPDATE parent SET id = 1 WHERE id = 2;", [])
            .unwrap();
        sqlite.execute_batch("COMMIT;").unwrap();
        let sqlite_child_parent: i64 = sqlite
            .query_row("SELECT parent_id FROM child;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sqlite_child_parent, 1);
    });
}

#[test]
fn deferred_restrict_parent_actions_remain_statement_immediate_like_sqlite() {
    asupersync::test_utils::run_test(|| async {
        for (label, action_clause, statement) in [
            (
                "UPDATE",
                "ON UPDATE RESTRICT",
                "UPDATE parent SET id = 2 WHERE id = 1;",
            ),
            (
                "DELETE",
                "ON DELETE RESTRICT",
                "DELETE FROM parent WHERE id = 1;",
            ),
        ] {
            let parent_count_sql = "SELECT COUNT(*) FROM parent WHERE id = 1;";
            let child_sql = format!(
                "CREATE TABLE child (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER REFERENCES parent(id)
                        {action_clause} DEFERRABLE INITIALLY DEFERRED
                );"
            );

            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
                .await
                .unwrap();
            conn.execute(&child_sql).await.unwrap();
            conn.execute("INSERT INTO parent VALUES (1);")
                .await
                .unwrap();
            conn.execute("INSERT INTO child VALUES (10, 1);")
                .await
                .unwrap();
            conn.execute("BEGIN;").await.unwrap();
            let error = conn
                .execute(statement)
                .await
                .expect_err("RESTRICT must reject at statement time even when deferred");
            assert!(
                matches!(error, FrankenError::ForeignKeyViolation),
                "{label}: unexpected error {error:?}"
            );
            assert_eq!(
                scalar(&conn, parent_count_sql).await,
                SqliteValue::Integer(1),
                "{label}: failed statement must restore the parent row",
            );
            conn.execute("ROLLBACK;").await.unwrap();

            let sqlite = rusqlite::Connection::open_in_memory().unwrap();
            sqlite.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            sqlite
                .execute_batch("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
                .unwrap();
            sqlite.execute_batch(&child_sql).unwrap();
            sqlite
                .execute("INSERT INTO parent VALUES (1);", [])
                .unwrap();
            sqlite
                .execute("INSERT INTO child VALUES (10, 1);", [])
                .unwrap();
            sqlite.execute_batch("BEGIN;").unwrap();
            sqlite
                .execute(statement, [])
                .expect_err("SQLite RESTRICT must reject at statement time");
            let sqlite_parent_count: i64 = sqlite
                .query_row(parent_count_sql, [], |row| row.get(0))
                .unwrap();
            assert_eq!(sqlite_parent_count, 1, "{label}: SQLite control");
            sqlite.execute_batch("ROLLBACK;").unwrap();
        }
    });
}

#[test]
fn delete_with_residual_external_fk_reference_rolls_back_base_mutation() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute(
            "CREATE TABLE node (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES node(id) ON DELETE RESTRICT
            );",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO node VALUES (1, 1), (2, 1);")
            .await
            .unwrap();

        let error = conn
            .execute("DELETE FROM node WHERE id = 1;")
            .await
            .expect_err("the remaining external child must block the delete");
        assert!(matches!(error, FrankenError::ForeignKeyViolation));
        let rows = conn
            .query("SELECT id, parent FROM node ORDER BY id;")
            .await
            .unwrap();
        assert_eq!(
            rows_as_values(&rows),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Integer(1)],
                vec![SqliteValue::Integer(2), SqliteValue::Integer(1)],
            ],
            "post-delete FK rejection must restore the base delete",
        );
    });
}

#[test]
fn update_with_residual_external_fk_reference_rolls_back_base_mutation() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute(
            "CREATE TABLE node (
                id INTEGER PRIMARY KEY,
                parent INTEGER REFERENCES node(id) ON UPDATE RESTRICT
            );",
        )
        .await
        .unwrap();
        conn.execute("CREATE TABLE prior_work (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute("INSERT INTO node VALUES (1, 1), (2, 1);")
            .await
            .unwrap();

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO prior_work VALUES (7);")
            .await
            .unwrap();
        let error = conn
            .execute("UPDATE node SET id = 3, parent = 3 WHERE id = 1;")
            .await
            .expect_err("the residual external child must block the update");
        assert!(matches!(error, FrankenError::ForeignKeyViolation));
        assert!(conn.in_transaction());
        let rows = conn
            .query("SELECT id, parent FROM node ORDER BY id;")
            .await
            .unwrap();
        assert_eq!(
            rows_as_values(&rows),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Integer(1)],
                vec![SqliteValue::Integer(2), SqliteValue::Integer(1)],
            ],
            "post-mutation FK rejection must roll the base UPDATE back",
        );
        assert_eq!(
            scalar(&conn, "SELECT id FROM prior_work;").await,
            SqliteValue::Integer(7),
            "the outer transaction's older work must survive",
        );
        conn.execute("COMMIT;").await.unwrap();
    });
}

#[test]
fn fk_action_rechecks_parent_despite_live_statement_scoped_cache() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER DEFAULT 999
                    REFERENCES parent(id) ON DELETE SET DEFAULT
            );",
        )
        .await
        .unwrap();
        conn.execute(
            "CREATE TRIGGER child_after_insert_removes_parent
             AFTER INSERT ON child
             BEGIN
                DELETE FROM parent WHERE id = NEW.parent_id;
             END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO parent VALUES (999);")
            .await
            .unwrap();

        // INSERT ... SELECT may install a statement-scoped positive FK cache
        // for child(parent_id=999). Its AFTER trigger then removes that parent
        // and invokes SET DEFAULT back to 999. The nested action must issue a
        // fresh probe, not reuse the now-stale cache entry.
        let error = conn
            .execute(
                "INSERT INTO child (id, parent_id)
                 SELECT 1, id FROM parent WHERE id = 999;",
            )
            .await
            .expect_err("nested SET DEFAULT must reject the deleted cached parent");
        assert!(
            matches!(error, FrankenError::ForeignKeyViolation),
            "unexpected error {error:?}"
        );
        assert!(
            conn.query("SELECT * FROM child;").await.unwrap().is_empty(),
            "the outer INSERT and nested action must roll back together"
        );
        assert_eq!(
            scalar(&conn, "SELECT id FROM parent;").await,
            SqliteValue::Integer(999)
        );
        // FK enforcement must remain active after the nested failure.
        let error = conn
            .execute("INSERT INTO child VALUES (2, 12345);")
            .await
            .expect_err("FK enforcement must survive the nested action failure");
        assert!(matches!(error, FrankenError::ForeignKeyViolation));
    });
}

#[test]
fn insert_or_fail_fk_violation_rolls_back_statement_in_autocommit_and_explicit_txn() {
    asupersync::test_utils::run_test(|| async {
        async fn seeded() -> Connection {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
            conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE child (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER REFERENCES parent(id)
                );",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO parent VALUES (1);")
                .await
                .unwrap();
            conn
        }

        let autocommit = seeded().await;
        let error = autocommit
            .execute("INSERT OR FAIL INTO child VALUES (1, 1), (2, 999);")
            .await
            .expect_err("FK violations ignore the OR FAIL preserve-rows policy");
        assert!(matches!(error, FrankenError::ForeignKeyViolation));
        assert!(
            autocommit
                .query("SELECT * FROM child;")
                .await
                .unwrap()
                .is_empty(),
            "the valid first row must roll back with the FK failure"
        );

        let explicit = seeded().await;
        explicit.execute("BEGIN;").await.unwrap();
        explicit
            .execute("INSERT INTO child VALUES (10, 1);")
            .await
            .unwrap();
        let error = explicit
            .execute("INSERT OR FAIL INTO child VALUES (11, 1), (12, 999);")
            .await
            .expect_err("FK violations must roll back the current OR FAIL statement");
        assert!(matches!(error, FrankenError::ForeignKeyViolation));
        assert!(explicit.in_transaction());
        let rows = explicit
            .query("SELECT id, parent_id FROM child ORDER BY id;")
            .await
            .unwrap();
        assert_eq!(
            rows_as_values(&rows),
            vec![vec![SqliteValue::Integer(10), SqliteValue::Integer(1)]],
            "the transaction's older row survives, but neither row from the failing statement does",
        );
        explicit.execute("COMMIT;").await.unwrap();

        // Stock SQLite control for the autocommit script.
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        sqlite
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE parent (id INTEGER PRIMARY KEY);
                 CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id));
                 INSERT INTO parent VALUES (1);",
            )
            .unwrap();
        sqlite
            .execute("INSERT OR FAIL INTO child VALUES (1, 1), (2, 999);", [])
            .expect_err("SQLite rejects the FK violation");
        let count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM child;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            count, 0,
            "SQLite control: OR FAIL keeps no row on FK failure"
        );
    });
}

#[test]
fn insert_or_fail_runtime_trigger_error_rolls_back_in_autocommit_and_explicit_txn() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE protected (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE prior_work (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TRIGGER protected_after_insert AFTER INSERT ON protected BEGIN
                SELECT RAISE(ABORT, 'runtime trigger failure');
            END;",
        )
        .await
        .unwrap();

        let error = conn
            .execute("INSERT OR FAIL INTO protected VALUES (1), (2);")
            .await
            .expect_err("runtime failures cannot inherit OR FAIL partial-write semantics");
        assert!(
            matches!(error, FrankenError::FunctionError(_)),
            "unexpected error {error:?}"
        );
        assert!(
            conn.query("SELECT * FROM protected;")
                .await
                .unwrap()
                .is_empty()
        );

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO prior_work VALUES (1);")
            .await
            .unwrap();
        let error = conn
            .execute("INSERT OR FAIL INTO protected VALUES (3), (4);")
            .await
            .expect_err("runtime failure must roll back only the current statement");
        assert!(matches!(error, FrankenError::FunctionError(_)));
        assert!(conn.in_transaction());
        assert!(
            conn.query("SELECT * FROM protected;")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            scalar(&conn, "SELECT id FROM prior_work;").await,
            SqliteValue::Integer(1)
        );
        conn.execute("COMMIT;").await.unwrap();
    });
}

/// Self-referential rows whose own reference is rewritten or removed by the
/// action. (Self-delete under NO ACTION/RESTRICT and self-update under
/// RESTRICT are not covered here: stock SQLite accepts them, FrankenSQLite
/// currently reports a FOREIGN KEY violation.)
#[test]
fn self_referential_fk_actions_that_rewrite_the_row_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        for (label, action, statement, expected) in [
            (
                "ON DELETE CASCADE",
                "ON DELETE CASCADE",
                "DELETE FROM node WHERE id = 1;",
                Vec::<(i64, i64)>::new(),
            ),
            (
                "ON DELETE SET NULL",
                "ON DELETE SET NULL",
                "DELETE FROM node WHERE id = 1;",
                Vec::new(),
            ),
            (
                "default NO ACTION update",
                "",
                "UPDATE node SET id = 2, parent = 2 WHERE id = 1;",
                vec![(2, 2)],
            ),
            (
                "ON UPDATE CASCADE",
                "ON UPDATE CASCADE",
                "UPDATE node SET id = 2, parent = 2 WHERE id = 1;",
                vec![(2, 2)],
            ),
            (
                "ON UPDATE SET NULL",
                "ON UPDATE SET NULL",
                "UPDATE node SET id = 2, parent = 2 WHERE id = 1;",
                vec![(2, 2)],
            ),
        ] {
            let schema = format!(
                "CREATE TABLE node (
                    id INTEGER PRIMARY KEY,
                    parent INTEGER REFERENCES node(id) {action}
                );"
            );

            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
            conn.execute(&schema).await.unwrap();
            conn.execute("INSERT INTO node VALUES (1, 1);")
                .await
                .unwrap();
            conn.execute(statement)
                .await
                .unwrap_or_else(|error| panic!("{label}: statement must succeed: {error}"));
            let rows = conn
                .query("SELECT id, parent FROM node ORDER BY id;")
                .await
                .unwrap();
            let expected_values: Vec<Vec<SqliteValue>> = expected
                .iter()
                .map(|(id, parent)| vec![SqliteValue::Integer(*id), SqliteValue::Integer(*parent)])
                .collect();
            assert_eq!(
                rows_as_values(&rows),
                expected_values,
                "{label}: FrankenSQLite"
            );

            let sqlite = rusqlite::Connection::open_in_memory().unwrap();
            sqlite.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            sqlite.execute_batch(&schema).unwrap();
            sqlite
                .execute("INSERT INTO node VALUES (1, 1);", [])
                .unwrap();
            sqlite
                .execute(statement, [])
                .unwrap_or_else(|error| panic!("{label}: SQLite statement failed: {error}"));
            let sqlite_rows: Vec<(i64, i64)> = sqlite
                .prepare("SELECT id, parent FROM node ORDER BY id;")
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert_eq!(sqlite_rows, expected, "{label}: SQLite control");
        }
    });
}

#[test]
fn shorthand_fk_uses_declared_non_ipk_and_without_rowid_primary_keys_like_sqlite() {
    asupersync::test_utils::run_test(|| async {
        for (
            label,
            parent_sql,
            child_sql,
            seed_sql,
            update_sql,
            child_state_sql,
            expected,
            orphan_sql,
        ) in [
            (
                "single non-IPK TEXT PRIMARY KEY",
                "CREATE TABLE parent (key TEXT PRIMARY KEY, payload TEXT);",
                "CREATE TABLE child (
                    id INTEGER PRIMARY KEY,
                    parent_key TEXT REFERENCES parent ON UPDATE CASCADE
                );",
                "INSERT INTO parent VALUES ('old', 'payload');
                 INSERT INTO child VALUES (1, 'old');",
                "UPDATE parent SET key = 'new' WHERE key = 'old';",
                "SELECT parent_key FROM child WHERE id = 1;",
                vec!["new"],
                "INSERT INTO child VALUES (2, 'orphan');",
            ),
            (
                "composite WITHOUT ROWID PRIMARY KEY",
                "CREATE TABLE parent (
                    tenant TEXT,
                    item TEXT,
                    PRIMARY KEY(tenant, item)
                ) WITHOUT ROWID;",
                "CREATE TABLE child (
                    id INTEGER PRIMARY KEY,
                    tenant TEXT,
                    item TEXT,
                    FOREIGN KEY(tenant, item) REFERENCES parent ON UPDATE CASCADE
                );",
                "INSERT INTO parent VALUES ('tenant-old', 'item-old');
                 INSERT INTO child VALUES (1, 'tenant-old', 'item-old');",
                "UPDATE parent
                 SET tenant = 'tenant-new', item = 'item-new'
                 WHERE tenant = 'tenant-old' AND item = 'item-old';",
                "SELECT tenant, item FROM child WHERE id = 1;",
                vec!["tenant-new", "item-new"],
                "INSERT INTO child VALUES (2, 'tenant-orphan', 'item-orphan');",
            ),
        ] {
            let expected_values: Vec<SqliteValue> = expected
                .iter()
                .map(|value| SqliteValue::Text((*value).into()))
                .collect();

            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
            conn.execute(parent_sql)
                .await
                .unwrap_or_else(|error| panic!("{label}: parent DDL failed: {error}"));
            conn.execute(child_sql)
                .await
                .unwrap_or_else(|error| panic!("{label}: child DDL failed: {error}"));
            conn.execute_batch(seed_sql)
                .await
                .unwrap_or_else(|error| panic!("{label}: seed failed: {error}"));
            conn.execute(update_sql).await.unwrap_or_else(|error| {
                panic!("{label}: shorthand parent-key cascade failed: {error}")
            });
            assert_eq!(
                conn.query(child_state_sql).await.unwrap()[0].values(),
                expected_values.as_slice(),
                "{label}: parent action must use the declared shorthand key",
            );
            let error = conn
                .execute(orphan_sql)
                .await
                .expect_err("the resolved shorthand key must reject an orphan");
            assert!(
                matches!(error, FrankenError::ForeignKeyViolation),
                "{label}: unexpected FrankenSQLite orphan error: {error:?}",
            );

            let sqlite = rusqlite::Connection::open_in_memory().unwrap();
            sqlite.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
            sqlite.execute_batch(parent_sql).unwrap();
            sqlite.execute_batch(child_sql).unwrap();
            sqlite.execute_batch(seed_sql).unwrap();
            sqlite.execute_batch(update_sql).unwrap();
            let sqlite_state: Vec<String> = sqlite
                .prepare(child_state_sql)
                .unwrap()
                .query_row([], |row| {
                    (0..row.as_ref().column_count())
                        .map(|index| row.get::<_, String>(index))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap();
            assert_eq!(sqlite_state, expected, "{label}: SQLite control");
            let error = sqlite
                .execute(orphan_sql, [])
                .expect_err("SQLite must reject the same orphan");
            assert!(
                error.to_string().contains("FOREIGN KEY constraint failed"),
                "{label}: unexpected SQLite orphan error: {error}",
            );
        }
    });
}

#[test]
fn statement_scoped_fk_cache_invalidates_after_trigger_parent_mutation() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE src (seq INTEGER, parent_id INTEGER);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TABLE child (
                seq INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parent(id)
            );",
        )
        .await
        .unwrap();
        conn.execute("CREATE TABLE prior_work (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TRIGGER child_after_insert_mutates_parent
             AFTER INSERT ON child
             WHEN NEW.seq = 1
             BEGIN
                DELETE FROM child WHERE seq = NEW.seq;
                DELETE FROM parent WHERE id = NEW.parent_id;
             END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO parent VALUES (1);")
            .await
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 1), (2, 1);")
            .await
            .unwrap();

        let error = conn
            .execute("INSERT INTO child SELECT seq, parent_id FROM src ORDER BY seq;")
            .await
            .expect_err("the second replay row must recheck the deleted parent");
        assert!(matches!(error, FrankenError::ForeignKeyViolation));
        assert!(
            conn.query("SELECT * FROM child;").await.unwrap().is_empty(),
            "autocommit rollback must remove both replay effects",
        );
        assert_eq!(
            scalar(&conn, "SELECT id FROM parent;").await,
            SqliteValue::Integer(1),
            "autocommit rollback must restore the parent deleted by row one",
        );

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO prior_work VALUES (1);")
            .await
            .unwrap();
        let error = conn
            .execute("INSERT INTO child SELECT seq, parent_id FROM src ORDER BY seq;")
            .await
            .expect_err("the explicit transaction replay must also recheck row two");
        assert!(matches!(error, FrankenError::ForeignKeyViolation));
        assert!(conn.in_transaction());
        assert!(
            conn.query("SELECT * FROM child;").await.unwrap().is_empty(),
            "the failing replay statement must leave no child rows",
        );
        assert_eq!(
            scalar(&conn, "SELECT id FROM parent;").await,
            SqliteValue::Integer(1),
            "the failing explicit statement must restore the parent",
        );
        assert_eq!(
            scalar(&conn, "SELECT id FROM prior_work;").await,
            SqliteValue::Integer(1),
            "the explicit transaction's earlier write must survive",
        );
        conn.execute("COMMIT;").await.unwrap();
    });
}

#[test]
fn full_rollback_discards_deferred_fk_obligations() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
            );",
        )
        .await
        .unwrap();

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO child VALUES (1, 99);")
            .await
            .expect("the deferred violation belongs to the active transaction");
        conn.execute("ROLLBACK;").await.unwrap();
        assert!(!conn.in_transaction());

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO parent VALUES (1);")
            .await
            .unwrap();
        conn.execute("COMMIT;")
            .await
            .expect("a rolled-back transaction's deferred obligation must not leak forward");
        assert!(conn.query("SELECT * FROM child;").await.unwrap().is_empty());
    });
}
