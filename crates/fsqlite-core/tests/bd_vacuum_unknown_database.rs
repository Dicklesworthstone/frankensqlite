// Keeper (bd-errmsg-parity-batch3): VACUUM of an unknown database name reports
// stock's "unknown database <name>", not frank's old "not implemented: VACUUM
// on attached schemas". VACUUM of main / a live attached schema / no-arg all
// succeed. Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

#[test]
fn vacuum_unknown_database_reports_unknown_database() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        let err = c.execute("VACUUM nope").await.expect_err("unknown VACUUM target").to_string();
        assert_eq!(err, "unknown database nope");

        let err2 = c
            .execute("VACUUM aux INTO 'ignored.db'")
            .await
            .expect_err("unknown VACUUM INTO target")
            .to_string();
        assert_eq!(err2, "unknown database aux");

        // VACUUM main / no-arg succeed.
        c.execute("VACUUM").await.expect("bare VACUUM must succeed");
        c.execute("VACUUM main").await.expect("VACUUM main must succeed");
    });
}
