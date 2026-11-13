#![recursion_limit = "512"]
#![allow(clippy::too_many_lines)]

//! Stock-SQLite keepers for statement semantics that interleave foreign-key
//! actions, triggers, UPSERT, RETURNING and conflict resolution.
//!
//! Salvaged from an unmerged codex working copy
//! (`frankensqlite_codex_fk_depth_20260728`, 2026-07-29). Every scenario either
//! asserts against a rusqlite (C SQLite) oracle inside the test or encodes
//! behavior re-verified against stock SQLite 3.46.1. Only cases that already
//! pass on main were kept; diverging cases are tracked separately.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

fn row_values(row: &Row) -> Vec<SqliteValue> {
    row.values().to_vec()
}

fn assert_oracle_fk_integrity(oracle: &rusqlite::Connection) {
    let mut statement = oracle.prepare("PRAGMA foreign_key_check;").unwrap();
    let mut violations = statement.query([]).unwrap();
    assert!(
        violations.next().unwrap().is_none(),
        "C SQLite oracle should retain foreign-key integrity"
    );
}

fn run_quiet_autocommit_test<F, Fut>(make: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("quiet autocommit test runtime should build");
    runtime.block_on(make());
}

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

fn fk_cascade_trigger_schema(deferred: bool) -> String {
    let deferral = if deferred {
        "DEFERRABLE INITIALLY DEFERRED"
    } else {
        ""
    };
    format!(
        "PRAGMA foreign_keys = ON;
         CREATE TABLE parent (id INTEGER PRIMARY KEY);
         CREATE TABLE child (
             id INTEGER PRIMARY KEY,
             parent_id INTEGER REFERENCES parent(id)
                 ON DELETE CASCADE ON UPDATE CASCADE
         );
         CREATE TABLE ref_parent (id INTEGER PRIMARY KEY);
         CREATE TABLE guarded (
             id INTEGER PRIMARY KEY,
             parent_id INTEGER REFERENCES ref_parent(id) {deferral}
         );
         CREATE TABLE audit (id INTEGER PRIMARY KEY);
         INSERT INTO parent VALUES (1);
         INSERT INTO child VALUES (10, 1);
         INSERT INTO ref_parent VALUES (1);
         INSERT INTO guarded VALUES (10, 1);"
    )
}

fn assert_oracle_fk_error(error: &rusqlite::Error) {
    assert!(
        matches!(
            error,
            rusqlite::Error::SqliteFailure(code, _)
                if code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY
        ),
        "expected the stock foreign-key constraint error, got {error}"
    );
}

async fn assert_fk_cascade_rows_match(
    conn: &Connection,
    oracle: &rusqlite::Connection,
    context: &str,
) {
    for sql in [
        "SELECT id FROM parent ORDER BY id",
        "SELECT id, parent_id FROM child ORDER BY id",
        "SELECT id FROM ref_parent ORDER BY id",
        "SELECT id, parent_id FROM guarded ORDER BY id",
        "SELECT id FROM audit ORDER BY id",
    ] {
        let mut statement = oracle.prepare(sql).unwrap();
        let columns = statement.column_count();
        let expected = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|column| row.get::<_, i64>(column).map(SqliteValue::Integer))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let actual = conn
            .query(sql)
            .await
            .unwrap()
            .iter()
            .map(row_values)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{context}: {sql}");
    }
}

/// bd-vcvn0: a generated FK action may suppress its own child-side checks,
/// but user trigger statements inside that action still own FK obligations.
#[test]
fn fk_cascade_trigger_immediate_checks_match_stock() {
    asupersync::test_utils::run_test(|| async {
        for (event, mutation) in [
            ("DELETE", "DELETE FROM parent WHERE id = 1"),
            ("UPDATE", "UPDATE parent SET id = 2 WHERE id = 1"),
        ] {
            for body in [
                "INSERT INTO guarded VALUES (20, 999)",
                "UPDATE guarded SET parent_id = 999 WHERE id = 10",
            ] {
                for explicit in [false, true] {
                    let context = format!("{event} cascade / {body} / explicit={explicit}");
                    let schema = format!(
                        "{} CREATE TRIGGER child_action AFTER {event} ON child BEGIN
                             INSERT INTO audit VALUES (20);
                             {body};
                         END;",
                        fk_cascade_trigger_schema(false)
                    );
                    let oracle = rusqlite::Connection::open(":memory:").unwrap();
                    oracle.execute_batch(&schema).unwrap();
                    let conn = Connection::open(":memory:").await.unwrap();
                    conn.execute(&schema).await.unwrap();
                    if explicit {
                        let prior = "BEGIN; INSERT INTO audit VALUES (99);";
                        oracle.execute_batch(prior).unwrap();
                        conn.execute(prior).await.unwrap();
                    }

                    let oracle_error = oracle.execute(mutation, []).unwrap_err();
                    assert_oracle_fk_error(&oracle_error);
                    let error = conn
                        .execute(mutation)
                        .await
                        .expect_err("cascade-fired trigger must reject a missing parent");
                    assert!(
                        matches!(error, FrankenError::ForeignKeyViolation),
                        "{context}: {error}"
                    );
                    assert_eq!(conn.in_transaction(), explicit, "{context}");
                    assert_eq!(oracle.is_autocommit(), !explicit, "{context}");
                    assert_fk_cascade_rows_match(&conn, &oracle, &context).await;
                    assert_oracle_fk_integrity(&oracle);
                    assert!(
                        conn.query("PRAGMA foreign_key_check")
                            .await
                            .unwrap()
                            .is_empty()
                    );

                    // Error unwinding must restore ordinary enforcement too.
                    let invalid = "INSERT INTO guarded VALUES (30, 12345)";
                    assert_oracle_fk_error(&oracle.execute(invalid, []).unwrap_err());
                    assert!(matches!(
                        conn.execute(invalid).await,
                        Err(FrankenError::ForeignKeyViolation)
                    ));

                    // The same trigger must succeed once its parent exists.
                    oracle
                        .execute("INSERT INTO ref_parent VALUES (999)", [])
                        .unwrap();
                    conn.execute("INSERT INTO ref_parent VALUES (999)")
                        .await
                        .unwrap();
                    let expected_changes = oracle.execute(mutation, []).unwrap();
                    assert_eq!(conn.execute(mutation).await.unwrap(), expected_changes);
                    if explicit {
                        oracle.execute_batch("COMMIT").unwrap();
                        conn.execute("COMMIT").await.unwrap();
                    }
                    assert_fk_cascade_rows_match(&conn, &oracle, &context).await;
                    assert_oracle_fk_integrity(&oracle);
                    assert!(
                        conn.query("PRAGMA foreign_key_check")
                            .await
                            .unwrap()
                            .is_empty()
                    );
                    conn.close().await.unwrap();
                }
            }
        }
    });
}

#[test]
fn fk_cascade_trigger_deferred_checks_survive_failed_commit() {
    asupersync::test_utils::run_test(|| async {
        let schema = format!(
            "{} CREATE TRIGGER child_action AFTER DELETE ON child BEGIN
                 INSERT INTO audit VALUES (20);
                 INSERT INTO guarded VALUES (20, 999);
             END;",
            fk_cascade_trigger_schema(true)
        );
        let oracle = rusqlite::Connection::open(":memory:").unwrap();
        oracle.execute_batch(&schema).unwrap();
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(&schema).await.unwrap();
        let transaction = "BEGIN; INSERT INTO audit VALUES (99); DELETE FROM parent WHERE id = 1;";
        oracle.execute_batch(transaction).unwrap();
        conn.execute(transaction).await.unwrap();
        assert_fk_cascade_rows_match(&conn, &oracle, "deferred statement effects").await;

        assert_oracle_fk_error(&oracle.execute_batch("COMMIT").unwrap_err());
        assert!(matches!(
            conn.execute("COMMIT").await,
            Err(FrankenError::ForeignKeyViolation)
        ));
        assert!(!oracle.is_autocommit());
        assert!(conn.in_transaction());
        assert_fk_cascade_rows_match(&conn, &oracle, "failed COMMIT retains transaction").await;

        let repair = "INSERT INTO ref_parent VALUES (999); COMMIT;";
        oracle.execute_batch(repair).unwrap();
        conn.execute(repair).await.unwrap();
        assert_fk_cascade_rows_match(&conn, &oracle, "repaired deferred obligation").await;
        assert_oracle_fk_integrity(&oracle);
        assert!(
            conn.query("PRAGMA foreign_key_check")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!conn.in_transaction());
        conn.close().await.unwrap();
    });
}

#[test]
fn fk_cascade_trigger_nested_program_checks_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let schema = format!(
            "{} CREATE VIEW guarded_view AS SELECT id, parent_id FROM guarded;
             CREATE TRIGGER guarded_view_insert INSTEAD OF INSERT ON guarded_view BEGIN
                 INSERT INTO guarded VALUES (NEW.id, NEW.parent_id);
             END;
             CREATE TRIGGER audit_action AFTER INSERT ON audit BEGIN
                 DELETE FROM parent WHERE id = 1;
             END;
             CREATE TRIGGER child_action AFTER DELETE ON child BEGIN
                 INSERT INTO guarded_view VALUES (20, 999);
             END;",
            fk_cascade_trigger_schema(false)
        );
        let oracle = rusqlite::Connection::open(":memory:").unwrap();
        oracle.execute_batch(&schema).unwrap();
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(&schema).await.unwrap();
        let mutation = "INSERT INTO audit VALUES (20)";
        assert_oracle_fk_error(&oracle.execute(mutation, []).unwrap_err());
        assert!(matches!(
            conn.execute(mutation).await,
            Err(FrankenError::ForeignKeyViolation)
        ));
        assert_fk_cascade_rows_match(&conn, &oracle, "trigger/cascade/trigger rollback").await;

        oracle
            .execute("INSERT INTO ref_parent VALUES (999)", [])
            .unwrap();
        conn.execute("INSERT INTO ref_parent VALUES (999)")
            .await
            .unwrap();
        assert_eq!(
            conn.execute(mutation).await.unwrap(),
            oracle.execute(mutation, []).unwrap()
        );
        assert_fk_cascade_rows_match(&conn, &oracle, "trigger/cascade/trigger success").await;
        assert_oracle_fk_integrity(&oracle);
        assert!(
            conn.query("PRAGMA foreign_key_check")
                .await
                .unwrap()
                .is_empty()
        );
        conn.close().await.unwrap();
    });
}

#[test]
fn fk_cascade_trigger_can_reference_new_parent_key() {
    asupersync::test_utils::run_test(|| async {
        let schema = format!(
            "{} CREATE TRIGGER child_action AFTER UPDATE ON child BEGIN
                 INSERT INTO child VALUES (20, NEW.parent_id);
             END;",
            fk_cascade_trigger_schema(false)
        );
        let oracle = rusqlite::Connection::open(":memory:").unwrap();
        oracle.execute_batch(&schema).unwrap();
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(&schema).await.unwrap();
        let mutation = "UPDATE parent SET id = 2 WHERE id = 1";
        assert_eq!(
            conn.execute(mutation).await.unwrap(),
            oracle.execute(mutation, []).unwrap()
        );
        assert_fk_cascade_rows_match(&conn, &oracle, "trigger sees new parent key").await;
        assert_oracle_fk_integrity(&oracle);
        assert!(
            conn.query("PRAGMA foreign_key_check")
                .await
                .unwrap()
                .is_empty()
        );
        conn.close().await.unwrap();
    });
}

fn render_rusqlite(v: rusqlite::types::ValueRef<'_>) -> String {
    match v {
        rusqlite::types::ValueRef::Null => "NULL".to_owned(),
        rusqlite::types::ValueRef::Integer(n) => n.to_string(),
        rusqlite::types::ValueRef::Real(f) => format!("{f}"),
        rusqlite::types::ValueRef::Text(s) => {
            format!("'{}'", String::from_utf8_lossy(s))
        }
        rusqlite::types::ValueRef::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

#[test]
fn test_insert_or_fail_foreign_key_error_aborts_entire_insert_select() {
    asupersync::test_utils::run_test(|| async {
        let oracle = rusqlite::Connection::open(":memory:").unwrap();
        oracle
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE parent (id INTEGER PRIMARY KEY);
                 CREATE TABLE source (id INTEGER PRIMARY KEY, parent_id INTEGER);
                 CREATE TABLE child (
                     id INTEGER PRIMARY KEY,
                     parent_id INTEGER REFERENCES parent(id)
                 );
                 CREATE TABLE marker (id INTEGER);
                 CREATE TRIGGER child_before_insert
                 BEFORE INSERT ON child
                 BEGIN
                     INSERT INTO marker VALUES (NEW.id);
                 END;
                 INSERT INTO parent VALUES (1);
                 INSERT INTO source VALUES (1, 1), (2, 999);",
            )
            .unwrap();
        oracle
            .execute(
                "INSERT OR FAIL INTO child
                 SELECT id, parent_id FROM source ORDER BY id;",
                [],
            )
            .expect_err("C SQLite must ABORT OR FAIL on a foreign-key violation");
        let oracle_child_count: i64 = oracle
            .query_row("SELECT COUNT(*) FROM child;", [], |row| row.get(0))
            .unwrap();
        let oracle_marker_count: i64 = oracle
            .query_row("SELECT COUNT(*) FROM marker;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(oracle_child_count, 0);
        assert_eq!(oracle_marker_count, 0);
        assert_oracle_fk_integrity(&oracle);

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE parent (id INTEGER PRIMARY KEY);
             CREATE TABLE source (id INTEGER PRIMARY KEY, parent_id INTEGER);
             CREATE TABLE child (
                 id INTEGER PRIMARY KEY,
                 parent_id INTEGER REFERENCES parent(id)
             );
             CREATE TABLE marker (id INTEGER);
             CREATE TRIGGER child_before_insert
             BEFORE INSERT ON child
             BEGIN
                 INSERT INTO marker VALUES (NEW.id);
             END;
             INSERT INTO parent VALUES (1);
             INSERT INTO source VALUES (1, 1), (2, 999);",
        )
        .await
        .unwrap();

        let insert = "INSERT OR FAIL INTO child
                      SELECT id, parent_id FROM source ORDER BY id;";
        let error = conn
            .execute(insert)
            .await
            .expect_err("foreign-key failure must abort the whole autocommit statement");
        assert!(matches!(error, FrankenError::ForeignKeyViolation));
        assert!(!conn.in_transaction());
        let child_count = conn.query("SELECT COUNT(*) FROM child;").await.unwrap();
        let marker_count = conn.query("SELECT COUNT(*) FROM marker;").await.unwrap();
        assert_eq!(child_count[0].values()[0], SqliteValue::Integer(0));
        assert_eq!(marker_count[0].values()[0], SqliteValue::Integer(0));

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO marker VALUES (99);")
            .await
            .unwrap();
        let error = conn
            .execute(insert)
            .await
            .expect_err("foreign-key failure must abort only the explicit-txn statement");
        assert!(matches!(error, FrankenError::ForeignKeyViolation));
        assert!(conn.in_transaction());
        let child_count = conn.query("SELECT COUNT(*) FROM child;").await.unwrap();
        let marker_count = conn.query("SELECT COUNT(*) FROM marker;").await.unwrap();
        assert_eq!(child_count[0].values()[0], SqliteValue::Integer(0));
        assert_eq!(
            marker_count[0].values()[0],
            SqliteValue::Integer(1),
            "the prior transaction write must survive while trigger effects roll back"
        );
        let violations = conn.query("PRAGMA foreign_key_check;").await.unwrap();
        assert!(violations.is_empty());
        conn.execute("COMMIT;").await.unwrap();
    });
}

#[test]
fn test_fk_multirow_update_interleaves_parent_rows_and_child_triggers() {
    asupersync::test_utils::run_test(|| async {
        let schema = "
            PRAGMA foreign_keys = ON;
            CREATE TABLE parent (id INTEGER PRIMARY KEY);
            CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL
                    REFERENCES parent(id) ON UPDATE CASCADE
            );
            CREATE TABLE audit (v INTEGER NOT NULL);
            CREATE TRIGGER child_u_exists
            BEFORE UPDATE ON child
            WHEN OLD.parent_id = 1
            BEGIN
                INSERT INTO audit
                VALUES (EXISTS(SELECT 1 FROM parent WHERE id = 2));
            END;
            INSERT INTO parent VALUES (1), (2);
            INSERT INTO child VALUES (10, 1), (20, 2);
        ";

        let oracle = rusqlite::Connection::open(":memory:").unwrap();
        oracle.execute_batch(schema).unwrap();
        assert_eq!(
            oracle
                .execute("UPDATE parent SET id = id + 10;", [])
                .unwrap(),
            2
        );
        assert_eq!(
            oracle
                .query_row("SELECT v FROM audit;", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1,
            "C SQLite cascades row 1 while parent row 2 still has its old key"
        );
        assert_oracle_fk_integrity(&oracle);

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(schema).await.unwrap();
        assert_eq!(
            conn.execute("UPDATE parent SET id = id + 10;")
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row("SELECT v FROM audit;")
                .await
                .unwrap()
                .values()[0],
            SqliteValue::Integer(1),
            "each parent UPDATE must complete its cascade and child trigger before the next parent row mutates"
        );
        assert!(
            conn.query("PRAGMA foreign_key_check;")
                .await
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn test_fk_interleaved_update_preserves_params_and_shadowed_rowid_locator() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "
            PRAGMA foreign_keys = ON;
            CREATE TABLE parent (
                rowid TEXT NOT NULL,
                id INTEGER PRIMARY KEY,
                marker INTEGER NOT NULL
            );
            CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL
                    REFERENCES parent(id) ON UPDATE CASCADE
            );
            INSERT INTO parent VALUES ('one', 1, 0), ('two', 2, 0);
            INSERT INTO child VALUES (10, 1), (20, 2);
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            conn.execute_with_params(
                "UPDATE parent SET id = id + ?1 WHERE marker >= ?2;",
                &[SqliteValue::Integer(10), SqliteValue::Integer(0)],
            )
            .await
            .unwrap(),
            2
        );
        assert_eq!(
            conn.query("SELECT rowid, id FROM parent ORDER BY id;")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![SqliteValue::Text("one".into()), SqliteValue::Integer(11)],
                vec![SqliteValue::Text("two".into()), SqliteValue::Integer(12)],
            ],
            "the declared `rowid` TEXT column shadows the row locator and must keep its values"
        );
        assert_eq!(
            conn.query("SELECT parent_id FROM child ORDER BY id;")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![SqliteValue::Integer(11)],
                vec![SqliteValue::Integer(12)],
            ]
        );
    });
}

#[test]
fn test_fk_interleaved_update_counts_parent_rows_once_and_preserves_insert_rowid() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "
            PRAGMA foreign_keys = ON;
            CREATE TABLE parent (id INTEGER PRIMARY KEY);
            CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL
                    REFERENCES parent(id) ON UPDATE CASCADE
            );
            CREATE TABLE audit (id INTEGER PRIMARY KEY, parent_id INTEGER);
            CREATE TABLE marker (id INTEGER PRIMARY KEY);
            CREATE TRIGGER child_u_audit
            AFTER UPDATE ON child
            BEGIN
                INSERT INTO audit(parent_id) VALUES (NEW.parent_id);
            END;
            INSERT INTO parent VALUES (1), (2);
            INSERT INTO child VALUES (10, 1), (20, 2);
            INSERT INTO marker VALUES (42);
            ",
        )
        .await
        .unwrap();
        let before = conn
            .query_row("SELECT total_changes(), last_insert_rowid();")
            .await
            .unwrap()
            .values()
            .to_vec();
        let before_total = match &before[0] {
            SqliteValue::Integer(value) => *value,
            _ => unreachable!("total_changes() must be an integer"),
        };
        // Stock SQLite 3.46.1 reports 42 (the marker row) here and after the
        // UPDATE.
        assert_eq!(before[1], SqliteValue::Integer(42));

        assert_eq!(
            conn.execute("UPDATE parent SET id = id + 10;")
                .await
                .unwrap(),
            2
        );
        let after = conn
            .query_row("SELECT changes(), total_changes(), last_insert_rowid();")
            .await
            .unwrap();
        assert_eq!(after.values()[0], SqliteValue::Integer(2));
        assert_eq!(
            after.values()[1],
            SqliteValue::Integer(before_total + 6),
            "two parent updates, two FK child updates, and two trigger inserts must each be counted once"
        );
        assert_eq!(
            after.values()[2],
            before[1],
            "generated FK actions and their triggers must not leak last_insert_rowid() into the parent statement"
        );
    });
}

#[test]
fn test_interleaved_update_replays_fixed_targets_against_fresh_row_images() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "
            CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER NOT NULL);
            CREATE TRIGGER mutate_future_row
            BEFORE UPDATE ON t
            WHEN OLD.id = 1
            BEGIN
                UPDATE t SET x = 999 WHERE id = 2;
            END;
            INSERT INTO t VALUES (1, 1), (2, 2);
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            conn.execute("UPDATE t SET x = x + 100 WHERE x < 10;")
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            conn.query("SELECT id, x FROM t ORDER BY id;")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Integer(101)],
                vec![SqliteValue::Integer(2), SqliteValue::Integer(1099)],
            ],
            "target membership is fixed before mutation, but each row's SET expression must read its current image"
        );
    });
}

#[test]
fn test_constraint_abort_after_trigger_write_publishes_auxiliary_total_only() {
    asupersync::test_utils::run_test(|| async {
        let schema = "
            CREATE TABLE t (
                id INTEGER PRIMARY KEY,
                x INTEGER NOT NULL UNIQUE
            );
            CREATE TABLE audit (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL
            );
            CREATE TABLE marker (id INTEGER PRIMARY KEY);
            INSERT INTO t VALUES (1, 1), (2, 2);
            INSERT INTO marker VALUES (42);
            CREATE TRIGGER log_before_update
            BEFORE UPDATE ON t
            BEGIN
                INSERT INTO audit(parent_id) VALUES (OLD.id);
            END;
        ";

        let oracle = rusqlite::Connection::open(":memory:").unwrap();
        oracle.execute_batch(schema).unwrap();
        oracle.execute_batch("BEGIN;").unwrap();
        let oracle_before = oracle
            .query_row("SELECT total_changes(), last_insert_rowid();", [], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap();
        oracle
            .execute("UPDATE t SET x = 2 WHERE id = 1;", [])
            .expect_err("C SQLite UNIQUE constraint must abort the statement");
        let oracle_after = oracle
            .query_row(
                "SELECT changes(), total_changes(), last_insert_rowid();",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(oracle_after.0, 0);
        assert_eq!(oracle_after.1, oracle_before.0 + 1);
        assert_eq!(oracle_after.2, oracle_before.1);
        assert!(!oracle.is_autocommit());

        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(schema).await.unwrap();
        conn.execute("BEGIN;").await.unwrap();
        let before = conn
            .query_row("SELECT total_changes(), last_insert_rowid();")
            .await
            .unwrap()
            .values()
            .to_vec();
        let before_total = before[0]
            .as_integer()
            .expect("total_changes() must be an integer");
        assert_eq!(before[1], SqliteValue::Integer(oracle_before.1));
        assert_eq!(oracle_before.1, 42);

        conn.execute("UPDATE t SET x = 2 WHERE id = 1;")
            .await
            .expect_err("UNIQUE constraint must abort the statement");
        assert_eq!(
            conn.query("SELECT id, x FROM t ORDER BY id;")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Integer(1)],
                vec![SqliteValue::Integer(2), SqliteValue::Integer(2)],
            ]
        );
        assert!(
            conn.query("SELECT id FROM audit;")
                .await
                .unwrap()
                .is_empty()
        );
        let after = conn
            .query_row("SELECT changes(), total_changes(), last_insert_rowid();")
            .await
            .unwrap();
        assert_eq!(after.values()[0], SqliteValue::Integer(0));
        assert_eq!(after.values()[1], SqliteValue::Integer(before_total + 1));
        assert_eq!(after.values()[2], before[1]);
        assert!(conn.in_transaction());
    });
}

#[test]
fn test_outer_fail_does_not_override_trigger_body_abort_across_fk_cascade() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA foreign_keys = ON;").await.unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parent(id) ON UPDATE CASCADE
            );",
        )
        .await
        .unwrap();
        conn.execute("CREATE TABLE log (value INTEGER UNIQUE);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE marker (value INTEGER);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TRIGGER child_au AFTER UPDATE ON child
             BEGIN
                 INSERT OR ABORT INTO log VALUES (1);
             END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO parent VALUES (1);")
            .await
            .unwrap();
        conn.execute("INSERT INTO child VALUES (1, 1), (2, 1);")
            .await
            .unwrap();

        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO marker VALUES (1);")
            .await
            .unwrap();
        let error = conn
            .execute("UPDATE OR FAIL parent SET id = 11 WHERE id = 1;")
            .await
            .expect_err("the child trigger's OR ABORT must win inside the FK cascade");
        assert!(
            matches!(error, FrankenError::UniqueViolation { .. })
                || error.to_string().contains("UNIQUE constraint failed"),
            "the test must reach the duplicate in the child trigger, got {error}"
        );

        assert!(
            conn.in_transaction(),
            "ABORT must roll back only the statement, not the explicit transaction"
        );
        let parent_rows = conn
            .query("SELECT id FROM parent ORDER BY id;")
            .await
            .unwrap();
        assert_eq!(
            parent_rows.iter().map(row_values).collect::<Vec<_>>(),
            vec![vec![SqliteValue::Integer(1)]],
        );
        let child_rows = conn
            .query("SELECT id, parent_id FROM child ORDER BY id;")
            .await
            .unwrap();
        assert_eq!(
            child_rows.iter().map(row_values).collect::<Vec<_>>(),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Integer(1)],
                vec![SqliteValue::Integer(2), SqliteValue::Integer(1)],
            ],
        );
        assert!(
            conn.query("SELECT value FROM log;")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            conn.query("SELECT value FROM marker;").await.unwrap().len(),
            1,
            "the explicit transaction's earlier marker must remain"
        );
        conn.execute("ROLLBACK;").await.unwrap();
    });
}

#[test]
fn test_upsert_unique_target_routes_after_update_old_and_new_images() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE dst(id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE log(old_name TEXT, new_name TEXT);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TRIGGER dst_au AFTER UPDATE OF name ON dst BEGIN \
             INSERT INTO log VALUES (OLD.name, NEW.name); END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO dst VALUES (1, 'a@example.test', 'old');")
            .await
            .unwrap();

        let affected = conn
            .execute(
                "INSERT INTO dst VALUES (99, 'a@example.test', 'new') \
                 ON CONFLICT(email) DO UPDATE SET name = excluded.name;",
            )
            .await
            .unwrap();
        assert_eq!(affected, 1);
        assert_eq!(
            conn.query_row("SELECT id, email, name FROM dst;")
                .await
                .unwrap()
                .values(),
            &[
                SqliteValue::Integer(1),
                SqliteValue::Text("a@example.test".into()),
                SqliteValue::Text("new".into()),
            ],
        );
        assert_eq!(
            conn.query_row("SELECT old_name, new_name FROM log;")
                .await
                .unwrap()
                .values(),
            &[
                SqliteValue::Text("old".into()),
                SqliteValue::Text("new".into()),
            ],
        );

        conn.execute("DELETE FROM log;").await.unwrap();
        let skipped = conn
            .execute(
                "INSERT INTO dst VALUES (99, 'a@example.test', 'ignored') \
                 ON CONFLICT(email) DO UPDATE SET name = excluded.name WHERE 0;",
            )
            .await
            .unwrap();
        assert_eq!(skipped, 0);
        assert!(conn.query("SELECT * FROM log;").await.unwrap().is_empty());
        assert_eq!(
            conn.query_row("SELECT name FROM dst;")
                .await
                .unwrap()
                .values(),
            &[SqliteValue::Text("new".into())],
        );
    });
}

#[test]
fn test_multirow_upsert_after_update_trigger_observes_each_completed_row_in_order() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE dst(id INTEGER PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TABLE log(id INTEGER, visible_rows INTEGER, old_name TEXT, new_name TEXT);",
        )
        .await
        .unwrap();
        conn.execute(
            "CREATE TRIGGER dst_au AFTER UPDATE ON dst BEGIN \
             INSERT INTO log SELECT NEW.id, count(*), OLD.name, NEW.name FROM dst; END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO dst VALUES (1, 'old-one'), (2, 'old-two');")
            .await
            .unwrap();

        let affected = conn
            .execute(
                "INSERT INTO dst VALUES (1, 'new-one'), (3, 'three'), (2, 'new-two') \
                 ON CONFLICT(id) DO UPDATE SET name = excluded.name;",
            )
            .await
            .unwrap();
        assert_eq!(affected, 3);
        assert_eq!(
            conn.query("SELECT id, visible_rows, old_name, new_name FROM log ORDER BY rowid;")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(2),
                    SqliteValue::Text("old-one".into()),
                    SqliteValue::Text("new-one".into()),
                ],
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(3),
                    SqliteValue::Text("old-two".into()),
                    SqliteValue::Text("new-two".into()),
                ],
            ],
            "the first AFTER UPDATE must run before the intervening INSERT row",
        );
    });
}

#[test]
fn test_multirow_upsert_returning_replays_triggers_in_row_order() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE dst(id INTEGER PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE log(kind TEXT, id INTEGER, visible_rows INTEGER);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TRIGGER dst_ai AFTER INSERT ON dst BEGIN \
             INSERT INTO log SELECT 'insert', NEW.id, count(*) FROM dst; END;",
        )
        .await
        .unwrap();
        conn.execute(
            "CREATE TRIGGER dst_au AFTER UPDATE ON dst BEGIN \
             INSERT INTO log SELECT 'update', NEW.id, count(*) FROM dst; END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO dst VALUES (1, 'old');")
            .await
            .unwrap();
        conn.execute("DELETE FROM log;").await.unwrap();

        let rows = conn
            .query(
                "INSERT INTO dst VALUES (1, 'new'), (2, 'two') \
                 ON CONFLICT(id) DO UPDATE SET name = excluded.name \
                 RETURNING id, name;",
            )
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(row_values).collect::<Vec<_>>(),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("new".into())],
                vec![SqliteValue::Integer(2), SqliteValue::Text("two".into())],
            ],
        );
        assert_eq!(
            conn.query("SELECT kind, id, visible_rows FROM log ORDER BY rowid;")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    SqliteValue::Text("update".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(1),
                ],
                vec![
                    SqliteValue::Text("insert".into()),
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(2),
                ],
            ],
            "each trigger must run after its row mutation and before the next source row",
        );
    });
}

#[test]
fn test_multirow_upsert_returning_skips_raise_ignore_source_row_in_order() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE dst(id INTEGER PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE log(id INTEGER);").await.unwrap();
        conn.execute(
            "CREATE TRIGGER dst_bi BEFORE INSERT ON dst \
             WHEN NEW.id = 2 BEGIN SELECT RAISE(IGNORE); END;",
        )
        .await
        .unwrap();
        conn.execute(
            "CREATE TRIGGER dst_ai AFTER INSERT ON dst BEGIN \
             INSERT INTO log VALUES (NEW.id); END;",
        )
        .await
        .unwrap();

        let rows = conn
            .query(
                "INSERT INTO dst VALUES (1, 'one'), (2, 'two'), (3, 'three') \
                 ON CONFLICT(id) DO UPDATE SET name = excluded.name \
                 RETURNING id, name;",
            )
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(row_values).collect::<Vec<_>>(),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("one".into())],
                vec![SqliteValue::Integer(3), SqliteValue::Text("three".into()),],
            ],
        );
        assert_eq!(
            conn.query("SELECT id FROM log ORDER BY rowid;")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![SqliteValue::Integer(1)], vec![SqliteValue::Integer(3)],],
        );
    });
}

#[test]
fn test_trigger_exit_restores_changes_and_last_insert_rowid_around_upsert() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE dst(id INTEGER PRIMARY KEY, value TEXT UNIQUE);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE trigger_log(value TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE sentinel(value TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO dst VALUES (1, 'old');")
            .await
            .unwrap();
        conn.execute("INSERT INTO sentinel(rowid, value) VALUES (777, 'sentinel');")
            .await
            .unwrap();
        conn.execute(
            "CREATE TRIGGER dst_ai AFTER INSERT ON dst BEGIN \
             INSERT INTO trigger_log VALUES (NEW.value); END;",
        )
        .await
        .unwrap();
        conn.execute(
            "CREATE TRIGGER dst_au AFTER UPDATE ON dst BEGIN \
             INSERT INTO trigger_log VALUES (NEW.value); END;",
        )
        .await
        .unwrap();

        let total_before_update = conn
            .query_row("SELECT total_changes();")
            .await
            .unwrap()
            .values()[0]
            .as_integer()
            .unwrap();
        conn.execute(
            "INSERT INTO dst VALUES (99, 'old') \
             ON CONFLICT(value) DO UPDATE SET value = excluded.value;",
        )
        .await
        .unwrap();
        let total_after_update = conn
            .query_row("SELECT total_changes();")
            .await
            .unwrap()
            .values()[0]
            .as_integer()
            .unwrap();
        assert_eq!(
            total_after_update - total_before_update,
            2,
            "the outer UPDATE and trigger-body INSERT both count toward total_changes()",
        );
        assert_eq!(
            conn.query_row("SELECT changes(), last_insert_rowid();")
                .await
                .unwrap()
                .values(),
            &[SqliteValue::Integer(1), SqliteValue::Integer(777)],
        );

        let total_before_insert = conn
            .query_row("SELECT total_changes();")
            .await
            .unwrap()
            .values()[0]
            .as_integer()
            .unwrap();
        conn.execute(
            "INSERT INTO dst VALUES (100, 'new') \
             ON CONFLICT(value) DO UPDATE SET value = excluded.value;",
        )
        .await
        .unwrap();
        let total_after_insert = conn
            .query_row("SELECT total_changes();")
            .await
            .unwrap()
            .values()[0]
            .as_integer()
            .unwrap();
        assert_eq!(
            total_after_insert - total_before_insert,
            2,
            "the outer INSERT and trigger-body INSERT both count toward total_changes()",
        );
        assert_eq!(
            conn.query_row("SELECT changes(), last_insert_rowid();")
                .await
                .unwrap()
                .values(),
            &[SqliteValue::Integer(1), SqliteValue::Integer(100)],
        );
    });
}

#[test]
fn test_upsert_update_arm_always_aborts_secondary_constraints() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "CREATE TABLE dst(\
             id INTEGER PRIMARY KEY, \
             unique_value TEXT UNIQUE, \
             required_value TEXT NOT NULL);",
        )
        .await
        .unwrap();
        conn.execute("CREATE TABLE log(id INTEGER);").await.unwrap();
        conn.execute(
            "CREATE TRIGGER dst_au AFTER UPDATE ON dst BEGIN \
             INSERT INTO log VALUES (NEW.id); END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO dst VALUES (1, 'one', 'required-one');")
            .await
            .unwrap();
        conn.execute("INSERT INTO dst VALUES (2, 'two', 'required-two');")
            .await
            .unwrap();

        for insert_prefix in ["INSERT", "INSERT OR IGNORE", "INSERT OR REPLACE"] {
            let unique_error = conn
                .execute(&format!(
                    "{insert_prefix} INTO dst VALUES (1, 'attempt', 'required') \
                     ON CONFLICT(id) DO UPDATE SET unique_value = 'two';"
                ))
                .await
                .expect_err("DO UPDATE must ABORT on a second UNIQUE conflict");
            assert!(
                unique_error
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("unique"),
                "unexpected error for {insert_prefix}: {unique_error}",
            );

            let not_null_error = conn
                .execute(&format!(
                    "{insert_prefix} INTO dst VALUES (1, 'attempt', 'required') \
                     ON CONFLICT(id) DO UPDATE SET required_value = NULL;"
                ))
                .await
                .expect_err("DO UPDATE must ABORT on NOT NULL");
            assert!(
                not_null_error
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("not null"),
                "unexpected error for {insert_prefix}: {not_null_error}",
            );
        }

        assert_eq!(
            conn.query("SELECT id, unique_value, required_value FROM dst ORDER BY id;")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Text("one".into()),
                    SqliteValue::Text("required-one".into()),
                ],
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Text("two".into()),
                    SqliteValue::Text("required-two".into()),
                ],
            ],
        );
        assert!(conn.query("SELECT * FROM log;").await.unwrap().is_empty());
    });
}

#[test]
fn test_insert_select_upsert_replay_uses_nested_prepared_dispatch() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE src(id INTEGER, name TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE dst(id INTEGER PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE log(kind TEXT, id INTEGER, old_name TEXT, new_name TEXT);")
            .await
            .unwrap();
        conn.execute(
            "CREATE TRIGGER dst_ai AFTER INSERT ON dst BEGIN \
             INSERT INTO log VALUES ('insert', NEW.id, NULL, NEW.name); END;",
        )
        .await
        .unwrap();
        conn.execute(
            "CREATE TRIGGER dst_au AFTER UPDATE ON dst BEGIN \
             INSERT INTO log VALUES ('update', NEW.id, OLD.name, NEW.name); END;",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO dst VALUES (1, 'old');")
            .await
            .unwrap();
        conn.execute("DELETE FROM log;").await.unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'new'), (2, 'two');")
            .await
            .unwrap();

        let affected = conn
            .execute(
                "INSERT INTO dst(id, name) \
                 SELECT id, name FROM src WHERE 1 \
                 ON CONFLICT(id) DO UPDATE SET name = excluded.name;",
            )
            .await
            .unwrap();
        assert_eq!(affected, 2);

        let rows = conn
            .query("SELECT id, name FROM dst ORDER BY id;")
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(row_values).collect::<Vec<_>>(),
            vec![
                vec![SqliteValue::Integer(1), SqliteValue::Text("new".into())],
                vec![SqliteValue::Integer(2), SqliteValue::Text("two".into())],
            ],
        );
        let log = conn
            .query("SELECT kind, id, old_name, new_name FROM log ORDER BY id;")
            .await
            .unwrap();
        assert_eq!(
            log.iter().map(row_values).collect::<Vec<_>>(),
            vec![
                vec![
                    SqliteValue::Text("update".into()),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("old".into()),
                    SqliteValue::Text("new".into()),
                ],
                vec![
                    SqliteValue::Text("insert".into()),
                    SqliteValue::Integer(2),
                    SqliteValue::Null,
                    SqliteValue::Text("two".into()),
                ],
            ],
        );
    });
}

#[test]
fn test_select_table_star_from_explicit_main_table() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("ATTACH DATABASE ':memory:' AS aux;")
            .await
            .unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE aux.t (id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'main-row');")
            .await
            .unwrap();
        conn.execute("INSERT INTO aux.t VALUES (1, 'aux-row');")
            .await
            .unwrap();

        let rows = conn
            .query("SELECT t.* FROM main.t ORDER BY id;")
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("main-row".into())
            ]]
        );
    });
}

#[test]
fn test_attached_insert_returning_star() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("ATTACH DATABASE ':memory:' AS aux;")
            .await
            .unwrap();
        conn.execute("CREATE TABLE aux.t (id INTEGER PRIMARY KEY, present INTEGER);")
            .await
            .unwrap();

        let rows = conn
            .query("INSERT INTO aux.t VALUES (1, 2) RETURNING *;")
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![SqliteValue::Integer(1), SqliteValue::Integer(2)]]
        );
    });
}

#[test]
fn test_attached_insert_with_cte_returning_star() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("ATTACH DATABASE ':memory:' AS aux;")
            .await
            .unwrap();
        conn.execute("CREATE TABLE aux.t (id INTEGER PRIMARY KEY, present INTEGER);")
            .await
            .unwrap();

        let rows = conn
            .query(
                "WITH src(id, present) AS (VALUES (1, 2))
             INSERT INTO aux.t SELECT id, present FROM src
             RETURNING *;",
            )
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![SqliteValue::Integer(1), SqliteValue::Integer(2)]]
        );
    });
}

#[test]
fn test_attached_update_returning_star() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("ATTACH DATABASE ':memory:' AS aux;")
            .await
            .unwrap();
        conn.execute("CREATE TABLE aux.t (id INTEGER PRIMARY KEY, present INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO aux.t VALUES (1, 0);")
            .await
            .unwrap();

        let rows = conn
            .query("UPDATE aux.t SET present = 2 WHERE id = 1 RETURNING *;")
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![SqliteValue::Integer(1), SqliteValue::Integer(2)]]
        );
    });
}

#[test]
fn test_attached_delete_returning_star() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("ATTACH DATABASE ':memory:' AS aux;")
            .await
            .unwrap();
        conn.execute("CREATE TABLE aux.t (id INTEGER PRIMARY KEY, present INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO aux.t VALUES (1, 10), (2, 20);")
            .await
            .unwrap();

        let rows = conn
            .query("DELETE FROM aux.t WHERE id = 1 RETURNING *;")
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![SqliteValue::Integer(1), SqliteValue::Integer(10)]]
        );

        let persisted = conn.query("SELECT id, present FROM aux.t;").await.unwrap();
        assert_eq!(
            persisted
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![SqliteValue::Integer(2), SqliteValue::Integer(20)]]
        );
    });
}

#[test]
fn test_join_select_alias_table_star_preserves_rhs_columns() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE lhs (id INTEGER, left_value TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE rhs (id INTEGER, right_value TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO lhs VALUES (1, 'lhs');")
            .await
            .unwrap();
        conn.execute("INSERT INTO rhs VALUES (1, 'rhs');")
            .await
            .unwrap();

        let rows = conn
            .query("SELECT r.* FROM lhs JOIN main.rhs AS r USING(id);")
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("rhs".into())
            ]]
        );
    });
}

#[test]
fn returned_explicit_parameter_batch_error_preserves_earlier_parameters() {
    run_quiet_autocommit_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        let parameter_sets = vec![
            vec![SqliteValue::Integer(1), SqliteValue::Text("kept".into())],
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Text("duplicate".into()),
            ],
        ];

        let error = conn
            .execute_many_with_params_skip_statement_savepoint_in_explicit_txn(
                "INSERT INTO t VALUES (?1, ?2)",
                &parameter_sets,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("UNIQUE")
                || error.to_string().contains("constraint")
                || error.to_string().contains("primary"),
            "second parameter must report its ordinary database error: {error}"
        );
        let rows = conn.query("SELECT id, value FROM t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[0].values()[1], SqliteValue::Text("kept".into()));
        assert!(
            conn.in_transaction(),
            "a returned batch error preserves the documented caller-owned transaction"
        );
        conn.execute("ROLLBACK").await.unwrap();
    });
}

#[test]
fn test_aggregate_order_by_and_filter_placeholders_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let setup = "CREATE TABLE t(v TEXT, k INTEGER, flag INTEGER);
                     INSERT INTO t VALUES ('a', 1, 1), ('b', 2, 0), ('c', 3, 1), ('d', 4, 1);";
        let sql = "SELECT group_concat(v || ? ORDER BY k * ?2, :sort) FILTER (WHERE flag = @keep), $tail FROM t";
        let frank = Connection::open(":memory:").await.unwrap();
        for statement in setup.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            frank.execute(statement).await.unwrap();
        }
        let rows = frank
            .query_with_params(
                sql,
                &[
                    SqliteValue::Text("-".into()),
                    SqliteValue::Integer(-1),
                    SqliteValue::Integer(0),
                    SqliteValue::Integer(1),
                    SqliteValue::Text("end".into()),
                ],
            )
            .await
            .expect("placeholders inside aggregate ORDER BY / FILTER must bind");
        let frank_values = rows
            .iter()
            .map(|row| row.values().iter().map(render_frank).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let oracle = rusqlite::Connection::open(":memory:").unwrap();
        oracle.execute_batch(setup).unwrap();
        let oracle_values = oracle
            .query_row(sql, rusqlite::params!["-", -1, 0, 1, "end"], |row| {
                Ok(vec![
                    format!("'{}'", row.get::<_, String>(0)?),
                    format!("'{}'", row.get::<_, String>(1)?),
                ])
            })
            .unwrap();
        assert_eq!(frank_values, vec![oracle_values]);
    });
}

#[test]
fn test_bd_dojku_comma_less_table_constraints_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        let ddl = "CREATE TABLE t(a INT, b INT, PRIMARY KEY(a) CHECK(b > 0) UNIQUE(b));";
        frank.execute(ddl).await.unwrap();
        oracle.execute_batch(ddl).unwrap();

        frank.execute("INSERT INTO t VALUES (1, 10);").await.unwrap();
        oracle.execute("INSERT INTO t VALUES (1, 10);", []).unwrap();

        for bad_sql in [
            "INSERT INTO t VALUES (1, 20);",
            "INSERT INTO t VALUES (2, 10);",
            "INSERT INTO t VALUES (2, -1);",
        ] {
            assert!(
                frank.execute(bad_sql).await.is_err(),
                "expected constraint failure for {bad_sql}"
            );
            assert!(
                oracle.execute(bad_sql, []).is_err(),
                "expected stock failure for {bad_sql}"
            );
        }

        // Named constraint propagation across comma-less table constraints until next comma
        let named_ddl = "CREATE TABLE t_named(a INT, b INT, c INT, CONSTRAINT c1 CHECK(a > 0) CHECK(b > 0), CHECK(c > 0));";
        frank.execute(named_ddl).await.unwrap();
        oracle.execute_batch(named_ddl).unwrap();

        let frank_err = frank
            .execute("INSERT INTO t_named VALUES (1, -1, 1);")
            .await
            .unwrap_err()
            .to_string();
        let oracle_err = oracle
            .execute("INSERT INTO t_named VALUES (1, -1, 1);", [])
            .unwrap_err()
            .to_string();
        assert_eq!(frank_err, oracle_err);

        let frank_err_un = frank
            .execute("INSERT INTO t_named VALUES (1, 1, -1);")
            .await
            .unwrap_err()
            .to_string();
        let oracle_err_un = oracle
            .execute("INSERT INTO t_named VALUES (1, 1, -1);", [])
            .unwrap_err()
            .to_string();
        assert_eq!(frank_err_un, oracle_err_un);
    });
}

#[test]
fn test_bd_vcvn0_fk_cascade_and_trigger_enforce_child_fk() {
    asupersync::test_utils::run_test(|| async {
        let setup = [
            "PRAGMA foreign_keys = ON;",
            "CREATE TABLE p1(id INTEGER PRIMARY KEY);",
            "CREATE TABLE p2(id INTEGER PRIMARY KEY);",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, a INTEGER REFERENCES p1(id) ON DELETE SET DEFAULT, b INTEGER REFERENCES p2(id));",
            "INSERT INTO p1 VALUES (1);",
            "INSERT INTO p2 VALUES (10);",
            "INSERT INTO c VALUES (100, 1, 10);",
            "CREATE TRIGGER c_au AFTER UPDATE ON c BEGIN UPDATE c SET b = 999 WHERE id = NEW.id; END;",
        ];
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();
        for stmt in setup {
            frank.execute(stmt).await.unwrap();
            oracle.execute_batch(stmt).unwrap();
        }

        let frank_err = frank.execute("DELETE FROM p1 WHERE id = 1;").await;
        let oracle_err = oracle.execute("DELETE FROM p1 WHERE id = 1;", []);
        assert!(
            frank_err.is_err() && oracle_err.is_err(),
            "trigger inside FK cascade setting invalid FK must fail: frank={frank_err:?} oracle={oracle_err:?}"
        );
    });
}

#[test]
fn test_bd_01rla_self_referential_fk_delete_and_update_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        for action in ["RESTRICT", "NO ACTION", "CASCADE", "SET NULL"] {
            let frank = Connection::open(":memory:").await.unwrap();
            let oracle = rusqlite::Connection::open(":memory:").unwrap();
            let setup = [
                "PRAGMA foreign_keys = ON;",
                &format!(
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, p INTEGER REFERENCES t(id) ON DELETE {action} ON UPDATE {action});"
                ),
                "INSERT INTO t VALUES (1, 1);",
            ];
            for stmt in &setup {
                frank.execute(stmt).await.unwrap();
                oracle.execute_batch(stmt).unwrap();
            }

            let update_sql = "UPDATE t SET id = 2, p = 2 WHERE id = 1;";
            let frank_upd = frank.execute(update_sql).await.map(|_| ());
            let oracle_upd = oracle.execute(update_sql, []).map(|_| ());
            assert_eq!(
                frank_upd.is_ok(),
                oracle_upd.is_ok(),
                "self-ref UPDATE mismatch for {action}: frank={frank_upd:?} oracle={oracle_upd:?}"
            );

            let frank_rows: Vec<Vec<String>> = frank
                .query("SELECT id, p FROM t ORDER BY id;")
                .await
                .unwrap()
                .iter()
                .map(|r| r.values().iter().map(render_frank).collect())
                .collect();
            let mut stmt = oracle.prepare("SELECT id, p FROM t ORDER BY id;").unwrap();
            let oracle_rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    Ok(vec![
                        render_rusqlite(row.get_ref(0)?),
                        render_rusqlite(row.get_ref(1)?),
                    ])
                })
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(
                frank_rows, oracle_rows,
                "post-UPDATE rows mismatch for {action}"
            );

            let delete_sql = "DELETE FROM t;";
            let frank_del = frank.execute(delete_sql).await.map(|_| ());
            let oracle_del = oracle.execute(delete_sql, []).map(|_| ());
            assert_eq!(
                frank_del.is_ok(),
                oracle_del.is_ok(),
                "self-ref DELETE mismatch for {action}: frank={frank_del:?} oracle={oracle_del:?}"
            );
        }
    });
}

fn rusqlite_err_msg(err: &rusqlite::Error) -> String {
    match err {
        rusqlite::Error::SqliteFailure(_, Some(msg))
        | rusqlite::Error::SqlInputError { msg, .. } => msg.clone(),
        other => other.to_string(),
    }
}

#[test]
fn test_bd_kn6ah_bd_vtbcf_bd_wt3mn_ddl_column_resolution_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        frank.execute("CREATE TABLE base(a INT);").await.unwrap();
        oracle.execute("CREATE TABLE base(a INT);", []).unwrap();

        let error_cases = [
            // bd-kn6ah: generated columns naming rowid/_rowid_/oid
            "CREATE TABLE gv(a, b AS (rowid * 2));",
            "CREATE TABLE gs(a, b AS (rowid * 2) STORED);",
            "CREATE TABLE gc(a, b AS (oid * 2), CHECK(b > 2));",
            // bd-vtbcf: qualified column reference in generated columns
            "CREATE TABLE g(a, b AS (main.g.a * 2));",
            "CREATE TABLE g2(a, b AS (temp.g2.a * 2));",
            "CREATE TABLE g3(a, b AS (g3.a * 2));",
            // bd-wt3mn: CHECK referencing unknown column or WITHOUT ROWID rowid alias
            "CREATE TABLE w(a PRIMARY KEY, b, CHECK(rowid > 1)) WITHOUT ROWID;",
            "CREATE TABLE w2(a PRIMARY KEY, b, CHECK(_rowid_ > 1)) WITHOUT ROWID;",
            "CREATE TABLE w3(a PRIMARY KEY, b, CHECK(oid > 1)) WITHOUT ROWID;",
            "CREATE TABLE ns(a, CHECK(nosuch > 1));",
            // ALTER TABLE ADD COLUMN equivalents
            "ALTER TABLE base ADD COLUMN b AS (base.a);",
            "ALTER TABLE base ADD COLUMN b AS (rowid);",
            "ALTER TABLE base ADD COLUMN b CHECK (nosuch > 0);",
        ];

        for sql in error_cases {
            let frank_err = frank
                .execute(sql)
                .await
                .expect_err(&format!("expected error for {sql}"))
                .to_string();
            let oracle_err = rusqlite_err_msg(
                &oracle
                    .execute(sql, [])
                    .expect_err(&format!("expected stock error for {sql}")),
            );
            assert_eq!(frank_err, oracle_err, "error message mismatch for {sql}");
        }

        // WITHOUT ROWID table that explicitly declares a `rowid` column allows `CHECK(rowid > 1)`
        let valid_without_rowid =
            "CREATE TABLE w_ok(a PRIMARY KEY, rowid INT, CHECK(rowid > 1)) WITHOUT ROWID;";
        frank.execute(valid_without_rowid).await.unwrap();
        oracle.execute(valid_without_rowid, []).unwrap();
    });
}

#[test]
fn test_bd_whs71_alter_table_add_column_check_validates_existing_rows() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        for stmt in [
            "CREATE TABLE a(x);",
            "INSERT INTO a(rowid, x) VALUES (1, 1), (5, 5);",
        ] {
            frank.execute(stmt).await.unwrap();
            oracle.execute(stmt, []).unwrap();
        }

        for bad_alter in [
            "ALTER TABLE a ADD COLUMN y CHECK(x > 3);",
            "ALTER TABLE a ADD COLUMN z DEFAULT 0 CHECK(z = 1);",
            "ALTER TABLE a ADD COLUMN y CHECK(rowid > 3);",
        ] {
            let frank_err = frank
                .execute(bad_alter)
                .await
                .expect_err(&format!("expected failure for {bad_alter}"))
                .to_string();
            let oracle_err = oracle
                .execute(bad_alter, [])
                .expect_err(&format!("expected stock failure for {bad_alter}"))
                .to_string();
            assert_eq!(frank_err, oracle_err, "mismatch on {bad_alter}");
        }

        // Schema must remain unchanged (only column `x`), and a valid ADD COLUMN CHECK must succeed.
        frank
            .execute("ALTER TABLE a ADD COLUMN y DEFAULT 10 CHECK(y > 5 AND x > 0);")
            .await
            .unwrap();
        oracle
            .execute(
                "ALTER TABLE a ADD COLUMN y DEFAULT 10 CHECK(y > 5 AND x > 0);",
                [],
            )
            .unwrap();

        let frank_rows: Vec<Vec<String>> = frank
            .query("SELECT rowid, x, y FROM a ORDER BY rowid;")
            .await
            .unwrap()
            .iter()
            .map(|r| r.values().iter().map(render_frank).collect())
            .collect();
        let mut stmt = oracle
            .prepare("SELECT rowid, x, y FROM a ORDER BY rowid;")
            .unwrap();
        let oracle_rows: Vec<Vec<String>> = stmt
            .query_map([], |row| {
                Ok(vec![
                    render_rusqlite(row.get_ref(0)?),
                    render_rusqlite(row.get_ref(1)?),
                    render_rusqlite(row.get_ref(2)?),
                ])
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(frank_rows, oracle_rows);
    });
}

#[test]
fn test_bd_kld68_create_index_on_virtual_table_or_view_errors_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        for stmt in [
            "CREATE VIRTUAL TABLE f USING fts5(x);",
            "CREATE TABLE base(x);",
            "CREATE VIEW v AS SELECT x FROM base;",
        ] {
            frank.execute(stmt).await.unwrap();
            oracle.execute(stmt, []).unwrap();
        }

        for bad_index in ["CREATE INDEX i_f ON f(x);", "CREATE INDEX i_v ON v(x);"] {
            let frank_err = frank.execute(bad_index).await.unwrap_err().to_string();
            let oracle_err = oracle.execute(bad_index, []).unwrap_err().to_string();
            assert_eq!(frank_err, oracle_err, "mismatch on {bad_index}");
        }
    });
}

#[test]
fn test_bd_29sgd_pragma_index_xinfo_on_without_rowid_table_name() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        let ddl = "CREATE TABLE w(a, b, PRIMARY KEY(a)) WITHOUT ROWID;";
        frank.execute(ddl).await.unwrap();
        oracle.execute(ddl, []).unwrap();

        for sql in [
            "PRAGMA index_xinfo('w');",
            "PRAGMA index_info('w');",
            "SELECT * FROM pragma_index_xinfo('w');",
        ] {
            let frank_rows: Vec<Vec<String>> = frank
                .query(sql)
                .await
                .unwrap()
                .iter()
                .map(|r| r.values().iter().map(render_frank).collect())
                .collect();
            let mut stmt = oracle.prepare(sql).unwrap();
            let col_count = stmt.column_count();
            let oracle_rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::with_capacity(col_count);
                    for i in 0..col_count {
                        vals.push(render_rusqlite(row.get_ref(i)?));
                    }
                    Ok(vals)
                })
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(frank_rows, oracle_rows, "mismatch on {sql}");
        }
    });
}

#[test]
fn test_bd_mw9m3_pragma_table_info_on_circular_view_errors_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        for ddl in [
            "CREATE VIEW c AS SELECT * FROM c;",
            "CREATE VIEW ma AS SELECT * FROM mb;",
            "CREATE VIEW mb AS SELECT * FROM ma;",
        ] {
            frank.execute(ddl).await.unwrap();
            oracle.execute(ddl, []).unwrap();
        }

        for sql in [
            "PRAGMA table_info(c);",
            "PRAGMA table_info(ma);",
            "SELECT * FROM pragma_table_info('c');",
        ] {
            let frank_err = frank.query(sql).await.unwrap_err().to_string();
            let oracle_err = oracle
                .prepare(sql)
                .and_then(|mut s| {
                    let rows: rusqlite::Result<Vec<i64>> = s.query_map([], |r| r.get(0))?.collect();
                    rows
                })
                .unwrap_err()
                .to_string();
            assert_eq!(frank_err, oracle_err, "mismatch on {sql}");
        }
    });
}

#[test]
fn test_bd_073qp_and_bd_84b60_duplicate_relation_column_names_renamed() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        for ddl in [
            "CREATE TABLE t(x INT);",
            "INSERT INTO t VALUES (10);",
            "CREATE VIEW vd AS SELECT x, x FROM t;",
            "CREATE TABLE users(name TEXT);",
            "CREATE TEMP TABLE users(name TEXT);",
            "CREATE TEMP VIEW vv2 AS SELECT main.users.name, temp.users.name FROM main.users JOIN temp.users;",
        ] {
            frank.execute(ddl).await.unwrap();
            oracle.execute(ddl, []).unwrap();
        }

        for sql in [
            "SELECT * FROM (SELECT 1 AS x, 2 AS x);",
            "SELECT * FROM vd;",
            "WITH c AS (SELECT 1 AS x, 2 AS x) SELECT * FROM c;",
        ] {
            let frank_stmt = frank.prepare(sql).await.unwrap();
            let oracle_stmt = oracle.prepare(sql).unwrap();
            let oracle_cols: Vec<String> = oracle_stmt
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            assert_eq!(frank_stmt.column_names(), oracle_cols.as_slice(), "column names mismatch on {sql}");
        }

        let info_sql = "SELECT name FROM pragma_table_info('vv2');";
        let frank_names: Vec<String> = frank
            .query(info_sql)
            .await
            .unwrap()
            .iter()
            .map(|r| render_frank(&r.values()[0]))
            .collect();
        let mut stmt = oracle.prepare(info_sql).unwrap();
        let oracle_names: Vec<String> = stmt
            .query_map([], |r| Ok(render_rusqlite(r.get_ref(0)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(frank_names, oracle_names);
    });
}

#[test]
fn test_bd_3j81a_create_trigger_on_schema_qualified_table_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        for ddl in [
            "CREATE TABLE t(x INT);",
            "CREATE TABLE logm(a INT);",
            "CREATE TRIGGER mtr3 AFTER INSERT ON main.t BEGIN INSERT INTO logm VALUES (NEW.x * 10); END;",
            "INSERT INTO t VALUES (3);",
        ] {
            frank.execute(ddl).await.unwrap();
            oracle.execute(ddl, []).unwrap();
        }

        let frank_val = frank
            .query_row("SELECT a FROM logm;")
            .await
            .unwrap()
            .values()[0]
            .as_integer()
            .unwrap();
        let oracle_val: i64 = oracle
            .query_row("SELECT a FROM logm;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(frank_val, 30);
        assert_eq!(frank_val, oracle_val);

        for bad_sql in [
            "CREATE TRIGGER mtr_bad AFTER INSERT ON temp.t BEGIN SELECT 1; END;",
            "CREATE TEMP TRIGGER main.tr_bad AFTER INSERT ON t BEGIN SELECT 1; END;",
        ] {
            let frank_err = frank.execute(bad_sql).await.unwrap_err().to_string();
            let oracle_err = rusqlite_err_msg(&oracle.execute(bad_sql, []).unwrap_err());
            assert_eq!(frank_err, oracle_err, "mismatch on {bad_sql}");
        }
    });
}

#[test]
fn test_bd_xrzce_rowid_aliases_in_upsert_set_and_returning_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        for ddl in [
            "CREATE TABLE q(k TEXT UNIQUE, v INT);",
            "INSERT INTO q(rowid, k, v) VALUES (10, 'a', 1);",
            "INSERT INTO q VALUES ('a', 2) ON CONFLICT(k) DO UPDATE SET rowid = -5, v = excluded.v;",
        ] {
            frank.execute(ddl).await.unwrap();
            oracle.execute(ddl, []).unwrap();
        }

        let sql = "SELECT rowid, k, v FROM q;";
        let frank_row: Vec<String> = frank
            .query_row(sql)
            .await
            .unwrap()
            .values()
            .iter()
            .map(render_frank)
            .collect();
        let oracle_row: Vec<String> = oracle
            .query_row(sql, [], |r| {
                Ok(vec![
                    render_rusqlite(r.get_ref(0)?),
                    render_rusqlite(r.get_ref(1)?),
                    render_rusqlite(r.get_ref(2)?),
                ])
            })
            .unwrap();
        assert_eq!(frank_row, oracle_row);

        let bad_returning = "INSERT INTO q AS x VALUES ('b', 3) RETURNING x.rowid;";
        let frank_err = frank.query(bad_returning).await.unwrap_err().to_string();
        let oracle_err = rusqlite_err_msg(
            &oracle
                .prepare(bad_returning)
                .and_then(|mut s| {
                    let rows: rusqlite::Result<Vec<i64>> = s.query_map([], |r| r.get(0))?.collect();
                    rows
                })
                .unwrap_err(),
        );
        assert_eq!(frank_err, oracle_err);
    });
}

#[test]
fn test_bd_257yl_unqualified_declared_rowid_in_multi_table_from_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        for ddl in [
            "CREATE TABLE r(rowid INTEGER, v INT);",
            "CREATE TABLE s(x INT, y INT);",
            "CREATE TABLE o(oid INT, _rowid_ INT, v INT);",
            "INSERT INTO r(_rowid_, rowid, v) VALUES (10, 100, 1), (20, 200, 2);",
            "INSERT INTO s(rowid, x, y) VALUES (30, 1, 11), (40, 2, 22);",
            "INSERT INTO o(rowid, oid, _rowid_, v) VALUES (50, 500, 5000, 1), (60, 600, 6000, 2);",
        ] {
            frank.execute(ddl).await.unwrap();
            oracle.execute(ddl, []).unwrap();
        }

        for sql in [
            "SELECT rowid, v FROM r JOIN s ON r.v = s.x ORDER BY v;",
            "SELECT rowid, x FROM r, s WHERE r.v = s.x ORDER BY x;",
            "SELECT count(*) FROM r JOIN s ON rowid > 0;",
            "SELECT rowid FROM r JOIN o ON r.v = o.v ORDER BY r.v;",
        ] {
            let frank_rows: Vec<Vec<String>> = frank
                .query(sql)
                .await
                .unwrap()
                .iter()
                .map(|r| r.values().iter().map(render_frank).collect())
                .collect();
            let mut stmt = oracle.prepare(sql).unwrap();
            let col_count = stmt.column_count();
            let oracle_rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::with_capacity(col_count);
                    for i in 0..col_count {
                        vals.push(render_rusqlite(row.get_ref(i)?));
                    }
                    Ok(vals)
                })
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(frank_rows, oracle_rows, "mismatch on {sql}");
        }
    });
}

#[test]
fn test_bd_h09hh_vacuum_partial_index_with_declared_column_collation() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("vac_partial_collation.db");
        let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();
        for sql in [
            "CREATE TABLE c(v TEXT COLLATE NOCASE);",
            "CREATE INDEX c_col ON c(v) WHERE v = 'abc';",
            "INSERT INTO c VALUES ('ABC'), ('abc'), ('x');",
            "VACUUM;",
        ] {
            conn.execute(sql).await.unwrap();
        }
        let rows: Vec<String> = conn
            .query("SELECT v FROM c INDEXED BY c_col WHERE v = 'abc' ORDER BY rowid;")
            .await
            .unwrap()
            .iter()
            .map(|r| render_frank(&r.values()[0]))
            .collect();
        assert_eq!(rows, vec!["'ABC'".to_owned(), "'abc'".to_owned()]);

        let oracle = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = oracle
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
    });
}

#[test]
fn test_bd_d4lv6_minmax_distinct_bare_column_duplicate_after_loading_row() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let oracle = rusqlite::Connection::open(":memory:").unwrap();

        for ddl in [
            "CREATE TABLE t(a INT, b TEXT, c INT);",
            "INSERT INTO t VALUES (9, 'b6', 2), (5, 'b1', 1), (9, 'b7', 2), (NULL, 'bn', 3), (5, 'b5', 3);",
            "CREATE TABLE t2(a INT, b TEXT, f INT);",
            "INSERT INTO t2 VALUES (3, 'b3a', 1), (9, 'b9', 1), (3, 'b3b', 1);",
        ] {
            frank.execute(ddl).await.unwrap();
            oracle.execute(ddl, []).unwrap();
        }

        for sql in [
            "SELECT max(DISTINCT a), b FROM t WHERE c >= 2;",
            "SELECT max(DISTINCT a), b FROM t;",
            "SELECT max(DISTINCT a), b FROM t2;",
            "SELECT max(DISTINCT a) FILTER (WHERE f), b FROM t2;",
        ] {
            let frank_row: Vec<String> = frank
                .query_row(sql)
                .await
                .unwrap()
                .values()
                .iter()
                .map(render_frank)
                .collect();
            let stmt_frank = frank.prepare(sql).await.unwrap();
            let prep_row: Vec<String> = stmt_frank
                .query_row()
                .await
                .unwrap()
                .values()
                .iter()
                .map(render_frank)
                .collect();
            let oracle_row: Vec<String> = oracle
                .query_row(sql, [], |r| {
                    Ok(vec![
                        render_rusqlite(r.get_ref(0)?),
                        render_rusqlite(r.get_ref(1)?),
                    ])
                })
                .unwrap();
            assert_eq!(frank_row, oracle_row, "ad-hoc mismatch on {sql}");
            assert_eq!(prep_row, oracle_row, "prepared mismatch on {sql}");
        }
    });
}

#[test]
fn test_bd_3syf5_check_error_text_grouped_exprs_and_constraint_name_carryover() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let frank_path = dir.path().join("frank_3syf5.db");
        let oracle_path = dir.path().join("oracle_3syf5.db");

        let frank = Connection::open(frank_path.to_str().unwrap())
            .await
            .unwrap();
        let oracle = rusqlite::Connection::open(&oracle_path).unwrap();

        for ddl in [
            "CREATE TABLE g(a, b, CHECK((a) AND (b)));",
            "CREATE TABLE g2(a CHECK((a > 0)));",
            "CREATE TABLE c1_tbl(a CONSTRAINT c1 CHECK(a > 0) CHECK(a < 10));",
            "CREATE TABLE nn_tbl(a CONSTRAINT nn NOT NULL CHECK(a > 0));",
            "CREATE TABLE c2_tbl(a, b, CONSTRAINT c2 CHECK(a > 0) CHECK(b < 10));",
        ] {
            frank.execute(ddl).await.unwrap();
            oracle.execute(ddl, []).unwrap();
        }

        let probes = [
            "INSERT INTO g VALUES (0, 1);",
            "INSERT INTO g2 VALUES (0);",
            "INSERT INTO c1_tbl VALUES (20);",
            "INSERT INTO nn_tbl VALUES (0);",
            "INSERT INTO c2_tbl VALUES (1, 20);",
        ];

        for sql in probes {
            let f_err = frank.execute(sql).await.unwrap_err().to_string();
            let o_err = rusqlite_err_msg(&oracle.execute(sql, []).unwrap_err());
            assert_eq!(f_err, o_err, "live DDL CHECK error mismatch for {sql}");
        }
        drop(frank);
        drop(oracle);

        // Verify reopened databases and stock-created databases produce the exact same text.
        let frank_reopen = Connection::open(frank_path.to_str().unwrap())
            .await
            .unwrap();
        let frank_from_stock = Connection::open(oracle_path.to_str().unwrap())
            .await
            .unwrap();
        let oracle_reopen = rusqlite::Connection::open(&oracle_path).unwrap();

        for sql in probes {
            let o_err = rusqlite_err_msg(&oracle_reopen.execute(sql, []).unwrap_err());
            let fr_err = frank_reopen.execute(sql).await.unwrap_err().to_string();
            let fs_err = frank_from_stock.execute(sql).await.unwrap_err().to_string();
            assert_eq!(fr_err, o_err, "reopened CHECK error mismatch for {sql}");
            assert_eq!(
                fs_err, o_err,
                "stock-created CHECK error mismatch for {sql}"
            );
        }
    });
}

