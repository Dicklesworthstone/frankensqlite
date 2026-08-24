// Keeper for bd-977wx: an INTEGER PRIMARY KEY (rowid alias) collision must be
// reported as SQLite's `UNIQUE constraint failed: <table>.<ipk>` — never the
// bare `PRIMARY KEY constraint failed`, and never wrapped as a FrankenError
// ::Internal ("internal error: VDBE halted ..."). The VDBE Insert conflict
// handler previously emitted the bare message, which fell through the typed
// error mapping and leaked as an Internal wrapper. Oracle: sqlite3 3.46.1.
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

fn assert_unique(msg: &str, expected: &str) {
    assert_eq!(msg, expected, "message must match stock verbatim");
    assert!(
        !msg.starts_with("internal error:"),
        "IPK collision must be a clean constraint error, not Internal (was: {msg:?})"
    );
    assert!(
        !msg.contains("PRIMARY KEY constraint failed"),
        "IPK collision must read as a UNIQUE violation on the IPK column (was: {msg:?})"
    );
}

#[test]
fn ipk_collision_reports_unique_constraint_977wx() {
    asupersync::test_utils::run_test(|| async {
        let base = &["CREATE TABLE t(k INTEGER PRIMARY KEY, v)", "INSERT INTO t VALUES (1,10),(2,20)"];

        // (1) plain INSERT duplicating an existing rowid/IPK.
        assert_unique(
            &err_of(base, "INSERT INTO t VALUES (2,99)").await,
            "UNIQUE constraint failed: t.k",
        );

        // (2) plain UPDATE that moves the IPK onto an existing one.
        assert_unique(
            &err_of(base, "UPDATE t SET k=1 WHERE k=2").await,
            "UNIQUE constraint failed: t.k",
        );

        // (3) INSERT OR ABORT (explicit default conflict mode) — same message.
        assert_unique(
            &err_of(base, "INSERT OR ABORT INTO t VALUES (1,88)").await,
            "UNIQUE constraint failed: t.k",
        );

        // Contrast: a secondary UNIQUE index collision keeps naming its own
        // column, unaffected by the IPK-label path.
        assert_unique(
            &err_of(
                &["CREATE TABLE u(k INTEGER PRIMARY KEY, e TEXT UNIQUE)", "INSERT INTO u VALUES (1,'a')"],
                "INSERT INTO u VALUES (2,'a')",
            )
            .await,
            "UNIQUE constraint failed: u.e",
        );
    });
}
