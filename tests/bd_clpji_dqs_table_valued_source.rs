#![recursion_limit = "512"]

//! bd-clpji: under SQLite's default DQS setting a double-quoted token that
//! names no column is a string literal. With a table-valued source the
//! expression evaluator reports the miss as `column not found: X`, which the
//! DQS rewrite-retry did not recognize, so
//! `SELECT printf("%0200d", value) FROM generate_series(1, 2)` failed with
//! "internal error: column not found: %0200d". Oracle: sqlite3 3.46.1
//! (DQS on), as in bd_2phpu_dqs_table_context.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn values(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(|row| row.values().to_vec()).collect()
}

fn text(s: &str) -> SqliteValue {
    SqliteValue::Text(s.into())
}

#[test]
fn dqs_strings_over_table_valued_sources_are_literals() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        let rows = conn
            .query("SELECT length(printf(\"%0200d\", value)) FROM generate_series(1, 2)")
            .await
            .unwrap();
        assert_eq!(
            values(&rows),
            vec![vec![SqliteValue::Integer(200)], vec![SqliteValue::Integer(200)]]
        );

        let rows = conn
            .query("SELECT value, \"x\" || value FROM generate_series(1, 3) WHERE \"a\" < \"b\"")
            .await
            .unwrap();
        assert_eq!(
            values(&rows),
            vec![
                vec![SqliteValue::Integer(1), text("x1")],
                vec![SqliteValue::Integer(2), text("x2")],
                vec![SqliteValue::Integer(3), text("x3")],
            ]
        );

        let rows = conn
            .query("SELECT key, \"k\" || key FROM json_each('[10,20]')")
            .await
            .unwrap();
        assert_eq!(
            values(&rows),
            vec![
                vec![SqliteValue::Integer(0), text("k0")],
                vec![SqliteValue::Integer(1), text("k1")],
            ]
        );

        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO t SELECT value + 10, printf(\"%0200d\", value) \
             FROM generate_series(1, 2)",
        )
        .await
        .unwrap();
        let rows = conn
            .query("SELECT a, length(b), substr(b, 199) FROM t ORDER BY a")
            .await
            .unwrap();
        assert_eq!(
            values(&rows),
            vec![
                vec![SqliteValue::Integer(11), SqliteValue::Integer(200), text("01")],
                vec![SqliteValue::Integer(12), SqliteValue::Integer(200), text("02")],
            ]
        );

        // Only a double-quoted token falls back: a bare unknown name errors.
        assert!(
            conn.query("SELECT naem FROM generate_series(1, 2)")
                .await
                .is_err(),
            "bare unknown column must still error"
        );
    });
}
