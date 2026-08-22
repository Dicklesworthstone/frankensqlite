//! bd-e8jzh — `PRAGMA fsqlite.dqs` opt-out knob for the DQS-ON engine (bd-jcjkf).
//!
//! Stock SQLite exposes DQS ("double-quoted string") control as the dbconfig
//! pair `SQLITE_DBCONFIG_DQS_DDL` / `SQLITE_DBCONFIG_DQS_DML`. FrankenSQLite's
//! native equivalent is `PRAGMA fsqlite.dqs = ON|OFF` (also the bare `PRAGMA
//! dqs`), which flips the single `Connection.dqs_enabled` gate the rewrite-retry
//! engine reads. ON (default) makes an unresolvable double-quoted identifier
//! fall back to a string literal; OFF restores strict, typo-safe resolution so
//! the same identifier errors as "no such column".

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// The knob defaults ON (readback `1`) via both the namespaced and bare forms,
/// matching the stock `SQLITE_DQS=3` default the engine ships with.
#[test]
fn dqs_pragma_defaults_on_readback() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        let r = conn.query("PRAGMA fsqlite.dqs;").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Integer(1));

        // Bare form (`PRAGMA dqs`) reads the same gate.
        let r = conn.query("PRAGMA dqs;").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Integer(1));
    });
}

/// `= OFF` returns `0` and disables the fallback: an unresolvable double-quoted
/// identifier is no longer rewritten to a string literal, so it errors as a
/// missing column instead of silently succeeding.
#[test]
fn dqs_pragma_off_disables_fallback() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // Baseline (ON): the FROM-less unresolvable name is a string literal.
        let r = conn.query("SELECT \"typo\";").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("typo".into()));

        // Flip OFF; readback confirms 0.
        let r = conn.query("PRAGMA fsqlite.dqs = OFF;").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Integer(0));

        // With DQS OFF the same statement now errors (strict resolution): no
        // proactive rewrite, no error-retry rewrite — the raw "no such column"
        // propagates.
        assert!(
            conn.query("SELECT \"typo\";").await.is_err(),
            "DQS OFF must reject an unresolvable double-quoted identifier"
        );
    });
}

/// `= ON` after `= OFF` restores the fallback: the identifier once again folds
/// to a string literal. Verifies the gate is a live toggle, not a one-shot.
#[test]
fn dqs_pragma_on_restores_fallback() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        let r = conn.query("PRAGMA fsqlite.dqs = OFF;").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Integer(0));
        assert!(conn.query("SELECT \"typo\";").await.is_err());

        let r = conn.query("PRAGMA fsqlite.dqs = ON;").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Integer(1));

        // Fallback is back: unresolvable double-quoted name -> string literal.
        let r = conn.query("SELECT \"typo\";").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("typo".into()));
    });
}

/// A double-quoted name that DOES resolve to a real column stays a column under
/// BOTH settings — the knob governs only the unresolvable-name fallback, never
/// legitimate quoted-identifier resolution.
#[test]
fn dqs_pragma_off_keeps_real_column_resolution() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(c TEXT);").await.unwrap();
        conn.execute("INSERT INTO t VALUES('realval');")
            .await
            .unwrap();

        conn.query("PRAGMA fsqlite.dqs = OFF;").await.unwrap();

        // "c" resolves to the column regardless of the DQS gate.
        let r = conn.query("SELECT \"c\" FROM t;").await.unwrap();
        assert_eq!(r[0].values()[0], SqliteValue::Text("realval".into()));
    });
}
