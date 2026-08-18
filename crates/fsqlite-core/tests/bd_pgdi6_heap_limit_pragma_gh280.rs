#![recursion_limit = "512"]

//! bd-pgdi6 / GH #280: `PRAGMA soft_heap_limit` / `hard_heap_limit` set/get.
//!
//! The exact clamp algebra (soft clamps to hard, hard is lower-only, hard
//! lowering drags soft down, non-numeric is a no-op readback) is covered
//! comprehensively by the fsqlite-vdbe unit test `test_connection_pragma_heap_limits_gh280`
//! against the empirically-derived C SQLite 3.46.1 semantics. This integration
//! test covers what the unit test cannot: that the value actually surfaces as a
//! one-row result through `Connection::query`, and that the limit is
//! **process-global** — visible from a second, independent connection.
//!
//! Only the *soft* limit is driven here: it is advisory (no allocation side
//! effect) and can be cleared with `= 0`, so the test leaves the shared state
//! clean. The hard limit's lower-only ratchet is exercised by the unit test.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().to_vec())
        .collect()
}

async fn one_int(conn: &Connection, sql: &str) -> i64 {
    let r = rows(conn, sql).await;
    assert_eq!(r.len(), 1, "`{sql}` must emit exactly one row, got {r:?}");
    assert_eq!(r[0].len(), 1, "`{sql}` must emit one column, got {:?}", r[0]);
    match &r[0][0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("`{sql}` must emit an integer, got {other:?}"),
    }
}

#[test]
fn heap_limit_row_emission_and_process_global_gh280() {
    asupersync::test_utils::run_test(|| async {
        let a = Connection::open(":memory:").await.unwrap();

        // Fresh process starts at 0 with no hard limit in force.
        assert_eq!(one_int(&a, "PRAGMA soft_heap_limit").await, 0);

        // Assignment echoes one row = the value after mutation.
        assert_eq!(one_int(&a, "PRAGMA soft_heap_limit=12345678").await, 12_345_678);
        // Bare readback echoes one row with the same value.
        assert_eq!(one_int(&a, "PRAGMA soft_heap_limit").await, 12_345_678);

        // Process-global: a second, independent connection sees the same value.
        let b = Connection::open(":memory:").await.unwrap();
        assert_eq!(one_int(&b, "PRAGMA soft_heap_limit").await, 12_345_678);

        // While no hard limit is in force, `= 0` clears the soft limit.
        assert_eq!(one_int(&b, "PRAGMA soft_heap_limit=0").await, 0);

        // A hard limit assignment also surfaces as one integer row through the
        // connection layer (value-after-mutation; from 0 it takes hold).
        assert_eq!(one_int(&b, "PRAGMA hard_heap_limit=20000000").await, 20_000_000);
        assert_eq!(one_int(&a, "PRAGMA hard_heap_limit").await, 20_000_000);

        // With a hard limit now in force, `soft = 0` clamps *up* to the hard
        // limit instead of clearing — end-to-end proof of the clamp algebra.
        assert_eq!(one_int(&a, "PRAGMA soft_heap_limit=0").await, 20_000_000);
    });
}
