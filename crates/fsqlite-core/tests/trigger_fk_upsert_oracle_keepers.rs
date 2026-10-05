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
