// Keeper (bd-errmsg-parity-batch2-g8v6e leaf (b)): stock SQLite reserves object
// names beginning with "sqlite_" (case-insensitive) for internal use. A user
// CREATE of a table, index, view, or trigger with such a name fails with the
// verbatim "object name reserved for internal use: <name>" — for ALL object
// kinds, not just CREATE TABLE. Oracle: rusqlite (bundled SQLite 3.53.2) +
// sqlite3 CLI 3.46.1.
use fsqlite_core::connection::Connection;

async fn ddl_err(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.expect("setup must succeed");
    }
    c.execute(sql)
        .await
        .expect_err("a reserved sqlite_ name must be rejected")
        .to_string()
}

#[test]
fn create_with_reserved_sqlite_prefix_is_rejected_for_all_object_kinds() {
    asupersync::test_utils::run_test(|| async {
        // CREATE TABLE (already landed) + case-insensitivity + exact "sqlite_".
        assert_eq!(
            ddl_err(&[], "CREATE TABLE sqlite_foo(a)").await,
            "object name reserved for internal use: sqlite_foo",
        );
        assert_eq!(
            ddl_err(&[], "CREATE TABLE SQLITE_UP(a)").await,
            "object name reserved for internal use: SQLITE_UP",
        );
        assert_eq!(
            ddl_err(&[], "CREATE TEMP TABLE sqlite_tmp(a)").await,
            "object name reserved for internal use: sqlite_tmp",
        );

        // CREATE INDEX / VIEW / TRIGGER (this leaf).
        assert_eq!(
            ddl_err(&["CREATE TABLE t(a, b)"], "CREATE INDEX sqlite_idx ON t(a)").await,
            "object name reserved for internal use: sqlite_idx",
        );
        assert_eq!(
            ddl_err(&[], "CREATE VIEW sqlite_v AS SELECT 1").await,
            "object name reserved for internal use: sqlite_v",
        );
        assert_eq!(
            ddl_err(
                &["CREATE TABLE t(a, b)"],
                "CREATE TRIGGER sqlite_trg AFTER INSERT ON t BEGIN SELECT 1; END",
            )
            .await,
            "object name reserved for internal use: sqlite_trg",
        );

        // A non-reserved name on the same paths still works.
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE t(a, b)").await.unwrap();
        c.execute("CREATE INDEX ok_idx ON t(a)").await.unwrap();
        c.execute("CREATE VIEW ok_v AS SELECT 1").await.unwrap();
        c.execute("CREATE TRIGGER ok_trg AFTER INSERT ON t BEGIN SELECT 1; END")
            .await
            .unwrap();
    });
}
