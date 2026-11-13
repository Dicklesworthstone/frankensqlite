#![recursion_limit = "512"]

//! bd-tj811: after a same-connection schema change, the first execution of a
//! held prepared statement re-prepares it, and the re-prepared program replaces
//! the old one in the handle, as stock `sqlite3_step` swaps the new VM into the
//! statement. Later executions run it directly instead of failing with
//! `SchemaChanged` and re-preparing every time, and `column_names()` /
//! `column_count()` report the re-prepared result columns.
//!
//! The hot-path counters are process-global; this binary holds one test so no
//! sibling can move them.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::SqliteValue;

fn ints(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<i64>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Integer(n) => *n,
                    other => panic!("expected integer, got {other:?}"),
                })
                .collect()
        })
        .collect()
}

async fn check(conn: &Connection, label: &str) {
    conn.execute_batch("CREATE TABLE t(a, b); INSERT INTO t VALUES (1, 2);")
        .await
        .expect("setup");
    let stmt = conn.prepare("SELECT * FROM t").await.expect("prepare");
    assert_eq!(stmt.column_names(), ["a", "b"], "{label}");
    assert_eq!(ints(&stmt.query().await.expect("query")), [[1, 2]], "{label}");

    conn.execute("ALTER TABLE t ADD COLUMN c DEFAULT 3")
        .await
        .expect("alter");

    set_hot_path_profile_enabled(true);
    reset_hot_path_profile();
    for _ in 0..5 {
        assert_eq!(
            ints(&stmt.query().await.expect("query after ALTER")),
            [[1, 2, 3]],
            "{label}: the held statement re-projects the widened row"
        );
    }
    let parser = hot_path_profile_snapshot().parser;
    set_hot_path_profile_enabled(false);
    assert_eq!(
        parser.prepared_cache_hits + parser.prepared_cache_misses,
        1,
        "{label}: five executions after one schema change re-prepare once: {parser:?}"
    );
    assert_eq!(
        stmt.column_names(),
        ["a", "b", "c"],
        "{label}: column names follow the re-prepared statement"
    );
    assert_eq!(stmt.column_count(), 3, "{label}");

    // A second schema change re-prepares the swapped-in statement again.
    conn.execute("ALTER TABLE t ADD COLUMN d DEFAULT 4")
        .await
        .expect("second alter");
    assert_eq!(
        ints(&stmt.query().await.expect("query after second ALTER")),
        [[1, 2, 3, 4]],
        "{label}"
    );
    assert_eq!(stmt.column_names(), ["a", "b", "c", "d"], "{label}");
}

#[test]
fn reprepared_statement_replaces_the_stale_one_in_its_handle() {
    asupersync::test_utils::run_test(|| async {
        let memory = Connection::open(":memory:").await.expect("open memory");
        check(&memory, "memory").await;

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("tj811.db");
        let file = Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .expect("open file");
        check(&file, "file").await;
    });
}
