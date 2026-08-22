// Keeper (bd-errmsg-parity-batch2): DROP TABLE/VIEW on the wrong object type
// reports stock's verbatim SQLITE_ERROR text (not "internal error: ..."), and a
// BEFORE/AFTER trigger on a view names the ACTUAL timing keyword (not the
// hardcoded "BEFORE/AFTER"). Oracle: sqlite3 3.46.1 + rusqlite 3.53.
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
fn drop_wrong_type_and_view_trigger_messages() {
    asupersync::test_utils::run_test(|| async {
        // DROP VIEW on a table / DROP TABLE on a view -> verbatim, not Internal.
        assert_eq!(
            err_of(&["CREATE TABLE t(a)"], "DROP VIEW t").await,
            "use DROP TABLE to delete table t",
        );
        assert_eq!(
            err_of(&["CREATE VIEW v AS SELECT 1 AS a"], "DROP TABLE v").await,
            "use DROP VIEW to delete view v",
        );
        // BEFORE/AFTER trigger on a view names the actual timing.
        assert_eq!(
            err_of(
                &["CREATE VIEW v AS SELECT 1 AS a"],
                "CREATE TRIGGER tr AFTER INSERT ON v BEGIN SELECT 1; END",
            )
            .await,
            "cannot create AFTER trigger on view: v",
        );
        assert_eq!(
            err_of(
                &["CREATE VIEW v AS SELECT 1 AS a"],
                "CREATE TRIGGER tr BEFORE INSERT ON v BEGIN SELECT 1; END",
            )
            .await,
            "cannot create BEFORE trigger on view: v",
        );
        // No timing keyword defaults to BEFORE.
        assert_eq!(
            err_of(
                &["CREATE VIEW v AS SELECT 1 AS a"],
                "CREATE TRIGGER tr INSERT ON v BEGIN SELECT 1; END",
            )
            .await,
            "cannot create BEFORE trigger on view: v",
        );
    });
}
