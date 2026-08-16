//! GH #216 / #205 (bd-gh-trigger-rowid-alias): `OLD.rowid` / `NEW.rowid`
//! inside a trigger body must resolve to the affected row's rowid, even when
//! the table has no explicit `INTEGER PRIMARY KEY` alias for the rowid.
//!
//! Before the fix, `OLD.rowid` in a trigger on a rowid table without an IPK
//! alias resolved to NULL, because the trigger frame only carried the row's
//! named column values and had no channel for the implicit rowid. The DELETE
//! path now captures the deleted row's rowid (`collect_delete_trigger_rows`
//! projects `rowid, *`) and threads it into the trigger frame, so `OLD.rowid`
//! resolves to the real value.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn query_rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {e}"))
        .into_iter()
        .map(|r| r.values().to_vec())
        .collect()
}

/// AFTER DELETE trigger on a rowid table with no IPK alias: `OLD.rowid`
/// resolves to the deleted row's rowid, not NULL.
#[test]
fn trigger_old_rowid_resolves_on_delete_without_ipk_alias() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // `t` is a rowid table but has NO `INTEGER PRIMARY KEY` — the rowid is
        // implicit, so `OLD.rowid` cannot come from a named column.
        conn.execute("CREATE TABLE t(a TEXT)").await.unwrap();
        conn.execute("CREATE TABLE log(kind TEXT, rid INTEGER)")
            .await
            .unwrap();
        conn.execute(
            "CREATE TRIGGER trg AFTER DELETE ON t \
             BEGIN INSERT INTO log VALUES('del', OLD.rowid); END;",
        )
        .await
        .unwrap();

        conn.execute("INSERT INTO t(a) VALUES('x')").await.unwrap(); // rowid 1
        conn.execute("INSERT INTO t(a) VALUES('y')").await.unwrap(); // rowid 2

        conn.execute("DELETE FROM t WHERE a='y'").await.unwrap();

        let rows = query_rows(&conn, "SELECT kind, rid FROM log").await;
        assert_eq!(rows.len(), 1, "trigger should have fired exactly once");
        assert_eq!(rows[0][0], SqliteValue::Text("del".into()));
        assert_eq!(
            rows[0][1],
            SqliteValue::Integer(2),
            "OLD.rowid must resolve to the deleted row's rowid, not NULL"
        );

        conn.close().await.unwrap();
    });
}
