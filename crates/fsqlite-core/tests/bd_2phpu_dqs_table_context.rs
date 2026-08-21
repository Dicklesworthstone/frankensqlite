//! bd-2phpu: the DQS ("double-quoted string") fallback must also fire for
//! double-quoted tokens that fail column resolution in a FROM-table / WHERE /
//! concat context — not only in the fromless projection contexts of bd-jcjkf.
//!
//! The VDBE column resolver reports the miss as `FunctionError("no such
//! column: X")` (bd-6mj9n error-parity); the DQS retry previously matched only
//! Internal/NotImplemented, so it never fired here (regressing bd-jcjkf's
//! FROM-table shapes too). Oracle: sqlite3 3.46.1 (DQS-on).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn bd_2phpu_dqs_fallback_in_table_where_concat_contexts() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a TEXT);").await.unwrap();
        conn.execute("INSERT INTO t VALUES('foo'),('bar');")
            .await
            .unwrap();

        // WHERE with a double-quoted non-column → string literal 'foo'.
        let r = conn
            .query("SELECT count(*) FROM t WHERE a = \"foo\";")
            .await
            .unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Integer(1));

        // Projected double-quoted non-column alongside a real column.
        let r = conn
            .query("SELECT a, \"zzz\" FROM t ORDER BY a;")
            .await
            .unwrap();
        assert_eq!(r[0].values()[1], SqliteValue::Text("zzz".into()));
        assert_eq!(r[1].values()[1], SqliteValue::Text("zzz".into()));

        // Double-quoted non-column concatenated with a real column.
        let r = conn
            .query("SELECT \"pre_\" || a FROM t ORDER BY a;")
            .await
            .unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("pre_bar".into()));
        assert_eq!(r[1].values()[0], SqliteValue::Text("pre_foo".into()));

        // A double-quoted name that DOES resolve stays a column reference.
        let r = conn.query("SELECT \"a\" FROM t ORDER BY a;").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("bar".into()));

        // Regression guard: a BARE (unquoted) unknown column must STILL error —
        // the DQS fallback applies only to double-quoted tokens.
        assert!(
            conn.query("SELECT naem FROM t;").await.is_err(),
            "bare unknown column must still error"
        );
        assert!(
            conn.query("SELECT count(*) FROM t WHERE naem = 1;")
                .await
                .is_err(),
            "bare unknown column in WHERE must still error"
        );

        conn.close().await.unwrap();
    });
}
