// Keeper (bd-prepare-time-validation-bypass): an aggregate misused in WHERE now
// raises the prepare-time "misuse of aggregate…" error via execute() too, not
// only via query() — the prepared/VDBE fast lane previously skipped it, so
// execute() of the SELECT returned no error while stock (and frank's query())
// errored at prepare. Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn exec_err(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect_err("aggregate-in-WHERE misuse must error")
        .to_string()
}

async fn exec_ok(setup: &[&str], sql: &str) {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect("valid query must succeed via execute()");
}

#[test]
fn execute_aggregate_in_where_reports_misuse() {
    asupersync::test_utils::run_test(|| async {
        let t: &[&str] = &["CREATE TABLE t(x)"];
        // Aggregate SELECT -> "misuse of aggregate: NAME()".
        assert_eq!(
            exec_err(t, "SELECT max(x) FROM t WHERE max(x) > 0").await,
            "misuse of aggregate: max()",
        );
        // Non-aggregate SELECT -> "misuse of aggregate function NAME()".
        assert_eq!(
            exec_err(t, "SELECT * FROM t WHERE sum(x) > 0").await,
            "misuse of aggregate function sum()",
        );
        // Valid queries via execute() are unaffected.
        exec_ok(t, "SELECT * FROM t WHERE x > 0").await;
        exec_ok(t, "SELECT sum(x) FROM t").await;
        exec_ok(
            &["CREATE TABLE t2(x)", "INSERT INTO t2 VALUES(1),(2)"],
            "SELECT count(*) FROM t2",
        )
        .await;
    });
}
