// Keeper (bd-errmsg-parity-batch3): a known SCALAR function used with an OVER
// clause reports stock's "NAME() may not be used as a window function", not
// "no such function: NAME". An unknown name still says "no such function"; an
// aggregate or window function with OVER is valid. Oracle: sqlite3 3.46.1 +
// rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect_err("must be rejected")
        .to_string()
}

async fn ok(setup: &[&str], sql: &str) {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect("valid window query must succeed");
}

#[test]
fn scalar_used_as_window_reports_misuse() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            err_of(&[], "SELECT abs(1) OVER ()").await,
            "abs() may not be used as a window function",
        );
        assert_eq!(
            err_of(&[], "SELECT length('x') OVER ()").await,
            "length() may not be used as a window function",
        );
        // An unknown function name still reports "no such function".
        assert_eq!(
            err_of(&[], "SELECT nonexist(1) OVER ()").await,
            "no such function: nonexist",
        );
        // Aggregates and window functions are valid with OVER.
        ok(
            &["CREATE TABLE t(a)", "INSERT INTO t VALUES(1),(2)"],
            "SELECT sum(a) OVER () FROM t",
        )
        .await;
        ok(
            &["CREATE TABLE t(a)", "INSERT INTO t VALUES(1),(2)"],
            "SELECT row_number() OVER () FROM t",
        )
        .await;
    });
}
