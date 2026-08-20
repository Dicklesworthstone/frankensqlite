//! bd-jcjkf — DQS ("double-quoted string") compat, shapes 1+2 (DQS-ON by default).
//!
//! SQLite's legacy default (SQLITE_DQS=3): a double-quoted identifier that does
//! NOT resolve to a real column/table falls back to a STRING LITERAL. A
//! double-quoted name that DOES resolve stays a column. Oracle: sqlite3 3.46.1
//! (DQS-on). Shape 3 (INSERT VALUES("x") -> silent NULL) is bd-82jdw, not here.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Shape 1: FROM-less unresolvable double-quoted refs become string literals.
#[test]
fn dqs_shape1_fromless_double_quoted_is_string() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        let r = conn.query("SELECT \"hello\";").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("hello".into()));

        let r = conn.query("SELECT \"a\" || \"b\";").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("ab".into()));

        // "x"="x" -> both fall back to the SAME string 'x' -> equal -> 1.
        let r = conn.query("SELECT \"x\"=\"x\";").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Integer(1));
    });
}

/// Shape 2: a double-quoted ref unresolvable against the FROM table becomes a
/// string literal, emitted once per row.
#[test]
fn dqs_shape2_fromhaving_unresolvable_is_string() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(c TEXT);").await.unwrap();
        conn.execute("INSERT INTO t VALUES('r1'),('r2');").await.unwrap();

        let r = conn.query("SELECT \"nope\" FROM t;").await.unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].values()[0], SqliteValue::Text("nope".into()));
        assert_eq!(r[1].values()[0], SqliteValue::Text("nope".into()));
    });
}

/// A double-quoted name that DOES resolve to a real column stays a column
/// reference — the DQS fallback must not clobber it.
#[test]
fn dqs_real_column_still_wins() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(c TEXT);").await.unwrap();
        conn.execute("INSERT INTO t VALUES('realval');").await.unwrap();

        // "c" resolves -> the column value, NOT the string 'c'.
        let r = conn.query("SELECT \"c\" FROM t;").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("realval".into()));

        // WHERE "c"="c" -> column compared to itself -> true -> the row.
        let r = conn.query("SELECT c FROM t WHERE \"c\"=\"c\";").await.unwrap();
        assert_eq!(r.len(), 1);
    });
}

/// Mixed: a resolvable double-quoted column AND an unresolvable one in the same
/// projection — per-name precision (only the failing name is rewritten).
#[test]
fn dqs_mixed_real_column_and_literal() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(c TEXT);").await.unwrap();
        conn.execute("INSERT INTO t VALUES('rv');").await.unwrap();

        let r = conn.query("SELECT \"c\", \"typo\" FROM t;").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("rv".into()));
        assert_eq!(r[0].values()[1], SqliteValue::Text("typo".into()));
    });
}

/// Regression: a BARE (unquoted) unknown column must STILL error — DQS applies
/// only to double-quoted tokens.
#[test]
fn dqs_bare_unquoted_unknown_column_still_errors() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(c TEXT);").await.unwrap();

        let r = conn.query("SELECT nope FROM t;").await;
        assert!(
            r.is_err(),
            "a bare unquoted unknown column must still error, got {r:?}"
        );
    });
}

/// A double-quoted token whose content contains a single quote must be spliced
/// with correct SQL escaping (`'` -> `''`).
#[test]
fn dqs_single_quote_in_name_is_escaped() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        let r = conn.query("SELECT \"a'b\";").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("a'b".into()));
    });
}
