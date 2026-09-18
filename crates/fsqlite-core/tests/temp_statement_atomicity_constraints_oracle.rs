//! bd-c36qv — widen TEMP statement atomicity beyond NOT NULL.
//!
//! bd-5bq6u proved and fixed statement atomicity on the MemDatabase lane, but
//! every shape it measured aborted on a **NOT NULL** violation. CHECK and UNIQUE
//! reach the abort by different routes, and a partially applied multi-row DELETE
//! was never probed at all. bd-ndm28 then fixed the transaction-scope half, so
//! both scopes are now live and both deserve guarding here.
//!
//! Every case runs on a TEMP table and on a main-schema table. The main-schema
//! arm is the control: the pager's implicit transaction makes it atomic, so if
//! that arm ever fails the test is wrong rather than the engine.
//!
//! Stock SQLite is the oracle throughout — these assert engine-vs-stock equality
//! rather than a hand-written expectation.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

/// Compare `sql` across both engines.
async fn assert_same(conn: &Connection, stock: &rusqlite::Connection, sql: &str, ctx: &str) {
    let expected = stock
        .prepare(sql)
        .unwrap()
        .query_map([], |row| {
            Ok((0..row.as_ref().column_count())
                .map(|i| row.get::<_, i64>(i).unwrap_or(-1))
                .collect::<Vec<_>>())
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let actual = conn
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("{ctx}: read: {error}"))
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Integer(i) => *i,
                    _ => -1,
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{ctx}");
}

/// Run one statement through both engines and require they agree on success.
async fn both(conn: &Connection, stock: &rusqlite::Connection, sql: &str, ctx: &str) -> bool {
    let ours = conn.execute(sql).await;
    let theirs = stock.execute_batch(sql);
    assert_eq!(
        ours.is_ok(),
        theirs.is_ok(),
        "{ctx}: sql={sql}: ours={ours:?} stock={theirs:?}"
    );
    ours.is_ok()
}

/// A CHECK violation on a later row must discard the whole statement.
#[test]
fn temp_failed_statement_on_check_violation_is_atomic_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            for in_txn in [false, true] {
                let keyword = if temporary { "TEMP " } else { "" };
                let ctx = format!("temporary={temporary}, in_txn={in_txn}");
                let conn = Connection::open(":memory:").await.unwrap();
                let stock = rusqlite::Connection::open_in_memory().unwrap();
                let ddl = format!(
                    "CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, v INTEGER CHECK (v <> 13))"
                );
                stock.execute_batch(&ddl).unwrap();
                conn.execute(&ddl).await.unwrap();
                both(&conn, &stock, "INSERT INTO g(v) VALUES(5)", &ctx).await;
                if in_txn {
                    both(&conn, &stock, "BEGIN", &ctx).await;
                }
                // Row 1 (19) is accepted, row 2 (13) trips the CHECK.
                both(&conn, &stock, "INSERT INTO g(v) VALUES(19),(13)", &ctx).await;
                if in_txn {
                    both(&conn, &stock, "COMMIT", &ctx).await;
                }
                assert_same(&conn, &stock, "SELECT id,v FROM g ORDER BY id", &ctx).await;
            }
        }
    });
}

/// A UNIQUE violation on a later row must discard the whole statement.
#[test]
fn temp_failed_statement_on_unique_violation_is_atomic_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            for in_txn in [false, true] {
                let keyword = if temporary { "TEMP " } else { "" };
                let ctx = format!("temporary={temporary}, in_txn={in_txn}");
                let conn = Connection::open(":memory:").await.unwrap();
                let stock = rusqlite::Connection::open_in_memory().unwrap();
                let ddl = format!(
                    "CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, u INTEGER UNIQUE, v INTEGER)"
                );
                stock.execute_batch(&ddl).unwrap();
                conn.execute(&ddl).await.unwrap();
                both(&conn, &stock, "INSERT INTO g(u,v) VALUES(1,5)", &ctx).await;
                if in_txn {
                    both(&conn, &stock, "BEGIN", &ctx).await;
                }
                // Row 1 (u=2) is accepted, row 2 collides with the existing u=1.
                both(&conn, &stock, "INSERT INTO g(u,v) VALUES(2,7),(1,9)", &ctx).await;
                if in_txn {
                    both(&conn, &stock, "COMMIT", &ctx).await;
                }
                assert_same(&conn, &stock, "SELECT id,u,v FROM g ORDER BY id", &ctx).await;
            }
        }
    });
}

/// A multi-row UPDATE that trips a CHECK partway must restore every prior value,
/// not merely stop. This is the UPDATE analogue of the bd-5bq6u INSERT shapes.
#[test]
fn temp_failed_multirow_update_on_check_is_atomic_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let keyword = if temporary { "TEMP " } else { "" };
            let ctx = format!("temporary={temporary}");
            let conn = Connection::open(":memory:").await.unwrap();
            let stock = rusqlite::Connection::open_in_memory().unwrap();
            let ddl = format!(
                "CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, v INTEGER CHECK (v < 500))"
            );
            stock.execute_batch(&ddl).unwrap();
            conn.execute(&ddl).await.unwrap();
            for sql in [
                "INSERT INTO g(v) VALUES(1)",
                "INSERT INTO g(v) VALUES(2)",
                "INSERT INTO g(v) VALUES(400)",
            ] {
                both(&conn, &stock, sql, &ctx).await;
            }
            // id 1 and 2 pass the CHECK after +100; id 3 becomes 500 and fails.
            both(&conn, &stock, "UPDATE g SET v = v + 100", &ctx).await;
            assert_same(&conn, &stock, "SELECT id,v FROM g ORDER BY id", &ctx).await;
        }
    });
}

/// A multi-row DELETE aborted partway by a trigger must put every row back.
/// DELETE was never probed by bd-5bq6u, and it exercises `MemDbUndoOp::DeleteRow`
/// rather than the UpsertRow path the INSERT/UPDATE shapes cover.
#[test]
fn temp_failed_multirow_delete_is_atomic_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let keyword = if temporary { "TEMP " } else { "" };
            let ctx = format!("temporary={temporary}");
            let conn = Connection::open(":memory:").await.unwrap();
            let stock = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                format!("CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, v INTEGER)"),
                format!(
                    "CREATE {keyword}TRIGGER guard BEFORE DELETE ON g WHEN OLD.id = 3 \
                     BEGIN SELECT RAISE(ABORT, 'keep id 3'); END"
                ),
                "INSERT INTO g(v) VALUES(10)".to_owned(),
                "INSERT INTO g(v) VALUES(20)".to_owned(),
                "INSERT INTO g(v) VALUES(30)".to_owned(),
                "INSERT INTO g(v) VALUES(40)".to_owned(),
            ] {
                both(&conn, &stock, &sql, &ctx).await;
            }
            // Deletes id 1 and 2, then the trigger aborts on id 3. Stock rolls the
            // whole statement back, so all four rows must still be present.
            both(&conn, &stock, "DELETE FROM g", &ctx).await;
            assert_same(&conn, &stock, "SELECT id,v FROM g ORDER BY id", &ctx).await;
        }
    });
}
