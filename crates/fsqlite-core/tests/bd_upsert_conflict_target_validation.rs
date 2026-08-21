// Keeper (bd-errmsg-parity-batch4): an ON CONFLICT conflict-target is validated
// for both DO NOTHING and DO UPDATE: a non-existent target column reports
// "no such column: <c>"; a target that matches no PRIMARY KEY / UNIQUE
// constraint reports "ON CONFLICT clause does not match any PRIMARY KEY or
// UNIQUE constraint". A valid PK/UNIQUE target is accepted.
// Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql).await.expect_err("bad ON CONFLICT target must be rejected").to_string()
}

async fn ok(setup: &[&str], sql: &str) {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql).await.expect("valid ON CONFLICT target must succeed");
}

#[test]
fn upsert_conflict_target_validated() {
    asupersync::test_utils::run_test(|| async {
        // Non-existent target column (DO NOTHING and DO UPDATE).
        assert_eq!(
            err_of(&["CREATE TABLE t(a PRIMARY KEY, b)"], "INSERT INTO t VALUES(1,2) ON CONFLICT(nope) DO NOTHING").await,
            "no such column: nope",
        );
        assert_eq!(
            err_of(&["CREATE TABLE t(a PRIMARY KEY, b)"], "INSERT INTO t VALUES(1,2) ON CONFLICT(nope) DO UPDATE SET b=2").await,
            "no such column: nope",
        );
        // Existing column that is not a PK/UNIQUE arbiter.
        assert_eq!(
            err_of(&["CREATE TABLE t(a, b)"], "INSERT INTO t VALUES(1,2) ON CONFLICT(a) DO NOTHING").await,
            "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint",
        );
        // Valid PRIMARY KEY / UNIQUE targets are accepted.
        ok(&["CREATE TABLE t(a PRIMARY KEY, b)"], "INSERT INTO t VALUES(1,2) ON CONFLICT(a) DO NOTHING").await;
        ok(&["CREATE TABLE t(a, b UNIQUE)"], "INSERT INTO t VALUES(1,2) ON CONFLICT(b) DO NOTHING").await;
        ok(&["CREATE TABLE t(a PRIMARY KEY, b)", "INSERT INTO t VALUES(1,1)"],
           "INSERT INTO t VALUES(1,2) ON CONFLICT(a) DO UPDATE SET b=excluded.b").await;
    });
}
