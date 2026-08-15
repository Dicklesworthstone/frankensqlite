//! GH #284 (bd-gh-pragma-writable-schema): `PRAGMA writable_schema = RESET`
//! must be accepted like stock SQLite, not rejected as a bad boolean.
//!
//! Before the fix, RESET errored "expected ON|OFF|TRUE|FALSE|1|0, got RESET"
//! and aborted the rest of the script. Stock SQLite 3.46.1 accepts it silently
//! (turning the toggle off and reparsing the schema) and runs the following
//! statements. We verify RESET is accepted, leaves the toggle off, and does not
//! block subsequent statements — including RESET after an explicit ON.
//!
//! (The separate GH #284 denial-diagnostic — DML on sqlite_master under
//! writable_schema=OFF must report "table sqlite_master may not be modified"
//! rather than "no such table" — is tracked as the remaining half of the bead.)

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn writable_schema(conn: &Connection) -> i64 {
    let rows = conn
        .query("PRAGMA writable_schema")
        .await
        .expect("readback PRAGMA writable_schema");
    match rows.first().map(fsqlite_core::connection::Row::values) {
        Some([SqliteValue::Integer(n), ..]) => *n,
        other => panic!("unexpected writable_schema readback: {other:?}"),
    }
}

#[test]
fn writable_schema_reset_is_accepted_and_turns_off() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a)").await.unwrap();

        // Bare RESET on a fresh connection: accepted, toggle stays off. This is
        // the core GH #284 regression — RESET used to error and abort here.
        conn.query("PRAGMA writable_schema=RESET")
            .await
            .expect("RESET must be accepted, not rejected as a bad boolean");
        assert_eq!(writable_schema(&conn).await, 0);

        // A following statement must still run (the bug aborted the script).
        let rows = conn
            .query("SELECT 1")
            .await
            .expect("subsequent SELECT runs");
        assert_eq!(
            rows.first().unwrap().values().first(),
            Some(&SqliteValue::Integer(1))
        );

        // RESET after an explicit ON turns the toggle back off.
        conn.query("PRAGMA writable_schema=ON").await.unwrap();
        assert_eq!(writable_schema(&conn).await, 1);
        conn.query("PRAGMA writable_schema=RESET").await.unwrap();
        assert_eq!(writable_schema(&conn).await, 0);

        // The keyword is case-insensitive.
        conn.query("PRAGMA writable_schema=ON").await.unwrap();
        conn.query("PRAGMA writable_schema=reset").await.unwrap();
        assert_eq!(writable_schema(&conn).await, 0);
    });
}
