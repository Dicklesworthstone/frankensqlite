#![recursion_limit = "512"]

// Keeper (bd-errmsg-parity-batch #1): an INSERT VALUES column/value count
// mismatch reports stock's verbatim SQLITE_ERROR text, NOT frank's old
// "not implemented: ..." (NotImplemented) wrapper. Two stock forms:
//   no target list   -> "table t has N columns but M values were supplied"
//   with target list -> "M values for N columns"
// Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn insert_err(setup: &str, insert: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(setup).await.unwrap();
    c.execute(insert)
        .await
        .expect_err("column/value count mismatch must be rejected")
        .to_string()
}

#[test]
fn insert_value_count_mismatch_reports_stock_text() {
    asupersync::test_utils::run_test(|| async {
        // No target column list -> "table t has N columns but M values were supplied".
        assert_eq!(
            insert_err("CREATE TABLE t(a, b)", "INSERT INTO t VALUES(1)").await,
            "table t has 2 columns but 1 values were supplied",
        );
        assert_eq!(
            insert_err("CREATE TABLE t(a, b)", "INSERT INTO t VALUES(1, 2, 3)").await,
            "table t has 2 columns but 3 values were supplied",
        );
        // With an explicit target list -> "M values for N columns".
        assert_eq!(
            insert_err("CREATE TABLE t(a, b)", "INSERT INTO t(a) VALUES(1, 2)").await,
            "2 values for 1 columns",
        );
        assert_eq!(
            insert_err("CREATE TABLE t(a, b)", "INSERT INTO t(a, b) VALUES(1)").await,
            "1 values for 2 columns",
        );
    });
}

fn stock_insert_err(setup: &str, insert: &str) -> String {
    let c = rusqlite::Connection::open_in_memory().unwrap();
    c.execute_batch(setup).unwrap();
    let err = c.execute_batch(insert).expect_err("stock rejects it");
    match err {
        rusqlite::Error::SqlInputError { msg, .. }
        | rusqlite::Error::SqliteFailure(_, Some(msg)) => msg,
        other => other.to_string(),
    }
}

// A target list that names every column, in or out of table order, still takes the
// short form; so do WITHOUT ROWID and generated-column tables and multi-row VALUES.
// Live oracle: bundled rusqlite.
#[test]
fn insert_value_count_mismatch_matches_stock_across_table_shapes() {
    const T: &str = "CREATE TABLE t(a, b)";
    const W: &str = "CREATE TABLE w(a PRIMARY KEY, b) WITHOUT ROWID";
    const G: &str = "CREATE TABLE g(a, b, c AS (a + b))";
    const CASES: &[(&str, &str)] = &[
        (T, "INSERT INTO t(b, a) VALUES(1)"),
        (T, "INSERT INTO t(a, b) VALUES(1), (2)"),
        (T, "INSERT INTO t VALUES(1), (2)"),
        (W, "INSERT INTO w(a) VALUES(1, 2)"),
        (W, "INSERT INTO w(a, b) VALUES(1)"),
        (W, "INSERT INTO w VALUES(1)"),
        (G, "INSERT INTO g VALUES(1)"),
        (G, "INSERT INTO g VALUES(1, 2, 3)"),
        (G, "INSERT INTO g(a, b) VALUES(1)"),
        (T, "INSERT INTO t(a, b) SELECT 1"),
        (T, "INSERT INTO t SELECT 1"),
    ];
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for (setup, insert) in CASES {
            let ours = insert_err(setup, insert).await;
            let stock = stock_insert_err(setup, insert);
            if ours != stock {
                mismatches.push(format!("{insert}\n  stock:   {stock}\n  fsqlite: {ours}"));
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    });
}
