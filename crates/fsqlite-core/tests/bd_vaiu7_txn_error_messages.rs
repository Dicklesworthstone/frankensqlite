// Keeper for bd-txn-savepoint-errors-internal-vs-error-vaiu7: user-facing
// transaction- and savepoint-control errors must match SQLite's messages
// VERBATIM under SQLITE_ERROR — never wrapped as an Internal error (which would
// prefix "internal error:" and report SQLITE_INTERNAL).
// Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;

async fn err_of(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect_err("statement should have been rejected")
        .to_string()
}

fn assert_stock(msg: &str, expected: &str) {
    assert_eq!(msg, expected, "message must match stock verbatim");
    assert!(
        !msg.starts_with("internal error:"),
        "must not be wrapped as an Internal error (was: {msg:?})"
    );
}

#[test]
fn txn_savepoint_error_messages_vaiu7() {
    asupersync::test_utils::run_test(|| async {
        // BEGIN within an active transaction.
        assert_stock(
            &err_of(&["BEGIN"], "BEGIN").await,
            "cannot start a transaction within a transaction",
        );
        // COMMIT with no active transaction.
        assert_stock(
            &err_of(&[], "COMMIT").await,
            "cannot commit - no transaction is active",
        );
        // ROLLBACK with no active transaction.
        assert_stock(
            &err_of(&[], "ROLLBACK").await,
            "cannot rollback - no transaction is active",
        );
        // ROLLBACK TO / RELEASE a savepoint that does not exist.
        assert_stock(
            &err_of(&["BEGIN"], "ROLLBACK TO sp").await,
            "no such savepoint: sp",
        );
        assert_stock(
            &err_of(&["BEGIN"], "RELEASE sp").await,
            "no such savepoint: sp",
        );
    });
}
