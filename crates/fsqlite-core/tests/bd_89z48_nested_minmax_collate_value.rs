//! bd-89z48 (regression/incompleteness of 15d34d33d / bd-3radn M5; overlaps
//! bd-9vtbh).
//!
//! `15d34d33d` made the group-by extremum-ROW selection honor a `COLLATE`
//! wrapper on a nested min()/max() argument, but the extremum VALUE was still
//! computed BINARY at the sibling `eval_group_agg_join_expr` aggregate arm (it
//! called the shared aggregate registry, which compares only BINARY). So
//! `SELECT max(name COLLATE NOCASE) || price FROM p` returned a self-inconsistent
//! hybrid: the BINARY max value paired with the NOCASE-selected row's other
//! columns. The VALUE must now be selected under the same collation as the row.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn frank_scalar(conn: &Connection, sql: &str) -> String {
    let rows = conn.query(sql).await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Text(s) => s.as_ref().to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        other => panic!("scalar query returned {other:?}"),
    }
}

fn stock_scalar(setup: &str, query: &str) -> String {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(setup).unwrap();
    conn.query_row(query, [], |r| r.get::<_, String>(0))
        .unwrap()
}

/// Explicit `COLLATE NOCASE` wrapper on the nested max() argument: value and row
/// must agree under NOCASE. Rows (price, name) = (3,'m'),(1,'Z'),(2,'q'):
/// NOCASE max name is 'Z' (folds to 'z', the largest), from the price=1 row, so
/// `max(name COLLATE NOCASE) || price` = 'Z1' (stock), not the BINARY 'q1'.
#[test]
fn nested_max_collate_wrapper_value_matches_row() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE p(price INTEGER, name TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO p VALUES(3,'m'),(1,'Z'),(2,'q');")
            .await
            .unwrap();

        let got = frank_scalar(&conn, "SELECT max(name COLLATE NOCASE) || price FROM p;").await;
        let stock = stock_scalar(
            "CREATE TABLE p(price INTEGER, name TEXT); INSERT INTO p VALUES(3,'m'),(1,'Z'),(2,'q');",
            "SELECT max(name COLLATE NOCASE) || price FROM p;",
        );
        assert_eq!(stock, "Z1", "oracle sanity: stock must return Z1");
        assert_eq!(got, stock, "nested max COLLATE value/row hybrid (bd-89z48)");
    });
}

/// The bare nested max VALUE itself (no `|| price`) must be the NOCASE extremum.
#[test]
fn nested_max_collate_wrapper_bare_value_is_nocase_extremum() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE p(price INTEGER, name TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO p VALUES(3,'m'),(1,'Z'),(2,'q');")
            .await
            .unwrap();
        let got = frank_scalar(&conn, "SELECT max(name COLLATE NOCASE) FROM p;").await;
        assert_eq!(got, "Z", "NOCASE max of {{m,Z,q}} is Z");

        let got_min = frank_scalar(&conn, "SELECT min(name COLLATE NOCASE) FROM p;").await;
        assert_eq!(got_min, "m", "NOCASE min of {{m,Z,q}} is m");
    });
}

/// Declared column collation (`name TEXT COLLATE NOCASE`, no explicit wrapper)
/// must be honored the same way — folded into the same fix (bd-89z48).
#[test]
fn nested_max_declared_collation_value_matches_row() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE p2(price INTEGER, name TEXT COLLATE NOCASE);")
            .await
            .unwrap();
        conn.execute("INSERT INTO p2 VALUES(3,'m'),(1,'Z'),(2,'q');")
            .await
            .unwrap();

        let got = frank_scalar(&conn, "SELECT max(name) || price FROM p2;").await;
        let stock = stock_scalar(
            "CREATE TABLE p2(price INTEGER, name TEXT COLLATE NOCASE); INSERT INTO p2 VALUES(3,'m'),(1,'Z'),(2,'q');",
            "SELECT max(name) || price FROM p2;",
        );
        assert_eq!(
            stock, "Z1",
            "oracle sanity: declared-NOCASE stock returns Z1"
        );
        assert_eq!(
            got, stock,
            "declared-collation nested max value/row (bd-89z48)"
        );
    });
}
