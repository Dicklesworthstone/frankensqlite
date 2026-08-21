// Keeper (bd-errmsg-parity-batch2): a user CREATE TABLE with a name beginning
// "sqlite_" (case-insensitive) is rejected with stock's "object name reserved
// for internal use: <name>" (name as-written), even under IF NOT EXISTS.
// Internal sqlite_sequence creation (AUTOINCREMENT) is unaffected.
// Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn ddl_err(ddl: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(ddl).await.expect_err("reserved name must be rejected").to_string()
}

#[test]
fn create_table_reserved_sqlite_name_rejected() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            ddl_err("CREATE TABLE sqlite_foo(a)").await,
            "object name reserved for internal use: sqlite_foo",
        );
        // Case-insensitive prefix; the reported name preserves the written case.
        assert_eq!(
            ddl_err("CREATE TABLE SQLITE_Bar(a)").await,
            "object name reserved for internal use: SQLITE_Bar",
        );
        // IF NOT EXISTS still errors.
        assert_eq!(
            ddl_err("CREATE TABLE IF NOT EXISTS sqlite_baz(a)").await,
            "object name reserved for internal use: sqlite_baz",
        );
        // Quoted name.
        assert_eq!(
            ddl_err("CREATE TABLE \"sqlite_q\"(a)").await,
            "object name reserved for internal use: sqlite_q",
        );
        // "sqlite_" alone is reserved.
        assert_eq!(
            ddl_err("CREATE TABLE sqlite_(a)").await,
            "object name reserved for internal use: sqlite_",
        );

        // Not reserved: "sqlite" without the underscore is a normal name.
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE sqlitex(a)").await.expect("sqlitex is a valid name");

        // AUTOINCREMENT still auto-creates sqlite_sequence internally (the check
        // must not block internal table creation).
        let c2 = Connection::open(":memory:").await.unwrap();
        c2.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT, b)").await.unwrap();
        c2.execute("INSERT INTO t(b) VALUES('x')").await.unwrap();
        let rows = c2
            .query_with_params("SELECT name FROM sqlite_sequence", &[])
            .await
            .expect("sqlite_sequence must exist after AUTOINCREMENT insert");
        assert_eq!(rows.len(), 1, "sqlite_sequence should have one row for table t");
    });
}
