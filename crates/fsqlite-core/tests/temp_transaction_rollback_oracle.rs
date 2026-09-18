//! bd-c36qv — does an explicit `ROLLBACK` (and `ROLLBACK TO SAVEPOINT`) undo
//! mutations to a **TEMP** table?
//!
//! bd-5bq6u fixed *statement* atomicity on the MemDatabase lane by wiring
//! MemDatabase's existing undo log to a statement boundary
//! (`begin_statement`/`end_statement`). Transaction scope is a different
//! question and was deliberately left unmeasured there.
//!
//! The reason it is genuinely in doubt, rather than obviously fine:
//! `Connection::snapshot` already captures `undo_version()` into `DbSnapshot`
//! and `restore_snapshot_state` already calls `rollback_to()`. Both were
//! completely inert while `undo_enabled` was false — which is precisely why
//! bd-5bq6u existed. Now that the log records, that path may work for free.
//!
//! But `end_statement` truncates the undo log when the OUTERMOST statement
//! succeeds, and inside an explicit transaction every top-level statement *is*
//! outermost. So each statement's records may be discarded at statement end,
//! leaving a later `ROLLBACK` with nothing to unwind. That is the specific
//! failure this file is written to detect.
//!
//! Each case runs twice: `temporary=false` is the control (the pager owns those
//! tables, so it must pass), `temporary=true` is the question.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

/// Read `SELECT id, v FROM g ORDER BY id` from both engines and compare.
async fn assert_same_rows(conn: &Connection, stock: &rusqlite::Connection, ctx: &str) {
    let sql = "SELECT id, v FROM g ORDER BY id";
    let expected = stock
        .prepare(sql)
        .unwrap()
        .query_map([], |row| {
            Ok(vec![
                SqliteValue::Integer(row.get(0)?),
                SqliteValue::Integer(row.get(1)?),
            ])
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let actual = conn
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("{ctx}: read: {error}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{ctx}");
}

/// Open both engines with `g` seeded to a single row `(1, 5)`.
async fn seeded(temporary: bool) -> (Connection, rusqlite::Connection) {
    let keyword = if temporary { "TEMP " } else { "" };
    let conn = Connection::open(":memory:").await.unwrap();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    let ddl = format!("CREATE {keyword}TABLE g(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)");
    stock.execute_batch(&ddl).unwrap();
    conn.execute(&ddl).await.unwrap();
    for sql in ["INSERT INTO g(v) VALUES(5)"] {
        stock.execute_batch(sql).unwrap();
        conn.execute(sql).await.unwrap();
    }
    (conn, stock)
}

/// Run the same SQL through both engines, requiring both to agree on success.
async fn both(conn: &Connection, stock: &rusqlite::Connection, sql: &str, ctx: &str) {
    let ours = conn.execute(sql).await;
    let theirs = stock.execute_batch(sql);
    assert_eq!(
        ours.is_ok(),
        theirs.is_ok(),
        "{ctx}: sql={sql}: ours={ours:?} stock={theirs:?}"
    );
}

/// `BEGIN; INSERT; ROLLBACK` must leave the table as it was.
#[test]
fn temp_rollback_undoes_insert_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let ctx = format!("temporary={temporary}");
            let (conn, stock) = seeded(temporary).await;
            for sql in ["BEGIN", "INSERT INTO g(v) VALUES(7)", "ROLLBACK"] {
                both(&conn, &stock, sql, &ctx).await;
            }
            assert_same_rows(&conn, &stock, &format!("{ctx}: after ROLLBACK of an INSERT")).await;
        }
    });
}

/// `BEGIN; UPDATE; ROLLBACK` must restore the prior values, not just row count.
#[test]
fn temp_rollback_undoes_update_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let ctx = format!("temporary={temporary}");
            let (conn, stock) = seeded(temporary).await;
            for sql in ["BEGIN", "UPDATE g SET v = 999 WHERE id = 1", "ROLLBACK"] {
                both(&conn, &stock, sql, &ctx).await;
            }
            assert_same_rows(&conn, &stock, &format!("{ctx}: after ROLLBACK of an UPDATE")).await;
        }
    });
}

/// `BEGIN; DELETE; ROLLBACK` must bring the row back.
#[test]
fn temp_rollback_undoes_delete_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let ctx = format!("temporary={temporary}");
            let (conn, stock) = seeded(temporary).await;
            for sql in ["BEGIN", "DELETE FROM g WHERE id = 1", "ROLLBACK"] {
                both(&conn, &stock, sql, &ctx).await;
            }
            assert_same_rows(&conn, &stock, &format!("{ctx}: after ROLLBACK of a DELETE")).await;
        }
    });
}

/// Several statements inside one transaction: ROLLBACK must undo all of them,
/// which is the case most likely to expose per-statement log truncation.
#[test]
fn temp_rollback_undoes_multiple_statements_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let ctx = format!("temporary={temporary}");
            let (conn, stock) = seeded(temporary).await;
            for sql in [
                "BEGIN",
                "INSERT INTO g(v) VALUES(7)",
                "INSERT INTO g(v) VALUES(9)",
                "UPDATE g SET v = 111 WHERE id = 1",
                "DELETE FROM g WHERE id = 2",
                "ROLLBACK",
            ] {
                both(&conn, &stock, sql, &ctx).await;
            }
            assert_same_rows(
                &conn,
                &stock,
                &format!("{ctx}: after ROLLBACK of four statements"),
            )
            .await;
        }
    });
}

/// `COMMIT` must keep the work — the mirror image, so a rollback that simply
/// discarded everything could not pass both this and the cases above.
#[test]
fn temp_commit_keeps_transaction_work_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let ctx = format!("temporary={temporary}");
            let (conn, stock) = seeded(temporary).await;
            for sql in [
                "BEGIN",
                "INSERT INTO g(v) VALUES(7)",
                "UPDATE g SET v = 55 WHERE id = 1",
                "COMMIT",
            ] {
                both(&conn, &stock, sql, &ctx).await;
            }
            assert_same_rows(&conn, &stock, &format!("{ctx}: after COMMIT")).await;
        }
    });
}

/// `SAVEPOINT` / `ROLLBACK TO` needs the same undo information.
#[test]
fn temp_rollback_to_savepoint_undoes_inner_work_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for temporary in [false, true] {
            let ctx = format!("temporary={temporary}");
            let (conn, stock) = seeded(temporary).await;
            for sql in [
                "BEGIN",
                "INSERT INTO g(v) VALUES(7)",
                "SAVEPOINT sp",
                "INSERT INTO g(v) VALUES(9)",
                "UPDATE g SET v = 222 WHERE id = 1",
                "ROLLBACK TO sp",
                "COMMIT",
            ] {
                both(&conn, &stock, sql, &ctx).await;
            }
            // The pre-savepoint INSERT survives; everything after it is undone.
            assert_same_rows(&conn, &stock, &format!("{ctx}: after ROLLBACK TO sp")).await;
        }
    });
}
