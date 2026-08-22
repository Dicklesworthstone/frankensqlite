// Keeper (bd-errmsg-parity-batch2): DROP of a system table reports stock's
// "table <name> may not be dropped" (beats IF EXISTS; DROP VIEW on sqlite_master
// gives the same). sqlite_master is always protected; sqlite_sequence is
// protected once it exists. A non-existent sqlite_ table still says "no such
// table". Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(setup: &[&str], failing: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(failing)
        .await
        .expect_err("must be rejected")
        .to_string()
}

#[test]
fn drop_system_table_protected() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            err_of(&[], "DROP TABLE sqlite_master").await,
            "table sqlite_master may not be dropped",
        );
        // Case-insensitive; canonical lowercase name in the message.
        assert_eq!(
            err_of(&[], "DROP TABLE sqlite_MASTER").await,
            "table sqlite_master may not be dropped",
        );
        // Protection beats IF EXISTS.
        assert_eq!(
            err_of(&[], "DROP TABLE IF EXISTS sqlite_master").await,
            "table sqlite_master may not be dropped",
        );
        // DROP VIEW on sqlite_master reports the same "table ... may not be dropped".
        assert_eq!(
            err_of(&[], "DROP VIEW sqlite_master").await,
            "table sqlite_master may not be dropped",
        );
        // sqlite_sequence is protected once AUTOINCREMENT has created it.
        assert_eq!(
            err_of(
                &[
                    "CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b)",
                    "INSERT INTO t(b) VALUES('x')",
                ],
                "DROP TABLE sqlite_sequence",
            )
            .await,
            "table sqlite_sequence may not be dropped",
        );
        // A non-existent sqlite_-prefixed table still says "no such table".
        assert_eq!(
            err_of(&[], "DROP TABLE sqlite_foo").await,
            "no such table: sqlite_foo",
        );
    });
}
