// Keeper (bd-prepare-time-validation-bypass slice 3): a bad column in a window
// function's inline OVER (PARTITION BY / ORDER BY <col>) is rejected at prepare
// with "no such column: <c>", matching stock — previously frank resolved window
// columns only at execution (empty table -> never evaluated -> no error).
// Valid window queries are unaffected. Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute("CREATE TABLE t(a, b)").await.unwrap();
    c.execute(sql).await.expect_err("bad window column must be rejected").to_string()
}

async fn ok(setup: &[&str], sql: &str) {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql).await.expect("valid window query must succeed");
}

#[test]
fn window_spec_bad_column_rejected() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            err_of("SELECT sum(a) OVER (ORDER BY nope) FROM t").await,
            "no such column: nope",
        );
        assert_eq!(
            err_of("SELECT sum(a) OVER (PARTITION BY nope) FROM t").await,
            "no such column: nope",
        );
        // Valid window queries are unaffected.
        let t: &[&str] = &["CREATE TABLE t(a, b)", "INSERT INTO t VALUES(1,2),(3,4)"];
        ok(t, "SELECT sum(a) OVER (ORDER BY a) FROM t").await;
        ok(t, "SELECT sum(a) OVER (PARTITION BY b) FROM t").await;
        ok(t, "SELECT sum(a) OVER () FROM t").await;
        ok(t, "SELECT row_number() OVER (ORDER BY a) FROM t").await;
        ok(t, "SELECT a, sum(b) OVER (PARTITION BY a ORDER BY b) FROM t").await;
    });
}
