// Keeper for bd-s9irk #1: an INTEGER PRIMARY KEY (rowid) duplicate reports
// SQLite's "UNIQUE constraint failed: <table>.<ipk_col>" — not "PRIMARY KEY
// constraint failed". Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;

async fn err_of(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect_err("duplicate PK should be rejected")
        .to_string()
}

#[test]
fn integer_pk_dup_reports_unique_constraint_s9irk() {
    asupersync::test_utils::run_test(|| async {
        // Plain INTEGER PRIMARY KEY duplicate.
        assert_eq!(
            err_of(
                &["CREATE TABLE t(a INTEGER PRIMARY KEY, b)", "INSERT INTO t VALUES(1,'x')"],
                "INSERT INTO t VALUES(1,'y')",
            )
            .await,
            "UNIQUE constraint failed: t.a",
        );
        // INSERT OR ROLLBACK on the same conflict reports the same message.
        assert_eq!(
            err_of(
                &["CREATE TABLE u(a INTEGER PRIMARY KEY, b)", "INSERT INTO u VALUES(1,'x')"],
                "INSERT OR ROLLBACK INTO u VALUES(1,'y')",
            )
            .await,
            "UNIQUE constraint failed: u.a",
        );
    });
}
