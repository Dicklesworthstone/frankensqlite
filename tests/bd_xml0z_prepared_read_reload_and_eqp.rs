#![recursion_limit = "512"]

//! bd-xml0z (GH#433 follow-up): prepared point lookups that compile to VDBE
//! (composite keys, WITHOUT ROWID tables, secondary indexes) on a file-backed
//! connection reloaded the whole MemDatabase from the pager on every
//! execution — a publication bind, an extra read transaction and a schema
//! reload — because a file connection keeps its rows unloaded by design and
//! the prepared read path treated "unloaded" as "stale". This keeper asserts
//! that the reload count does not scale with executions, that peer commits
//! (rows and DDL) are still seen, and that `EXPLAIN QUERY PLAN` names every
//! equality-bound key column of a multi-column seek, as stock SQLite does.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "\
    CREATE TABLE r (tenant_id TEXT NOT NULL, id TEXT NOT NULL, party TEXT, v TEXT, \
                    PRIMARY KEY (tenant_id, id));\
    CREATE TABLE rw (tenant_id TEXT NOT NULL, id TEXT NOT NULL, party TEXT, v TEXT, \
                     PRIMARY KEY (tenant_id, id)) WITHOUT ROWID;\
    CREATE TABLE p (tenant_id TEXT NOT NULL, id TEXT NOT NULL, party TEXT, v TEXT, \
                    PRIMARY KEY (tenant_id, id));\
    CREATE INDEX p_party ON p (tenant_id, party);";

const LOOKUPS: &[&str] = &[
    "SELECT v FROM r WHERE tenant_id = 't' AND id = ?1",
    "SELECT v FROM rw WHERE tenant_id = 't' AND id = ?1",
    "SELECT v FROM p WHERE tenant_id = 't' AND party = ?1",
];

fn text(value: &str) -> SqliteValue {
    SqliteValue::Text(value.into())
}

fn single_text(rows: &[fsqlite_core::connection::Row]) -> Option<String> {
    match rows {
        [] => None,
        [row] => match &row.values()[0] {
            SqliteValue::Text(value) => Some(value.to_string()),
            other => panic!("expected text, got {other:?}"),
        },
        _ => panic!("expected at most one row, got {}", rows.len()),
    }
}

/// The lookup key for `i`: `id` for the PK shapes, `party` for the index shape.
fn key_for(sql: &str, i: usize) -> String {
    if sql.contains("party = ?1") {
        format!("party-{i:04}")
    } else {
        format!("key-{i:04}")
    }
}

async fn seed(conn: &Connection, rows: usize) {
    conn.execute_batch(SCHEMA).await.expect("schema");
    conn.execute("BEGIN").await.expect("begin");
    for i in 0..rows {
        for table in ["r", "rw", "p"] {
            conn.execute_with_params(
                &format!("INSERT INTO {table} VALUES ('t', ?1, ?2, ?3)"),
                &[
                    text(&format!("key-{i:04}")),
                    text(&format!("party-{i:04}")),
                    text(&format!("value-{i}")),
                ],
            )
            .await
            .expect("insert");
        }
    }
    conn.execute("COMMIT").await.expect("commit");
}

#[test]
fn prepared_vdbe_lookups_do_not_reload_memdb_per_execution() {
    asupersync::test_utils::run_test(|| async {
        set_hot_path_profile_enabled(true);
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("lookups.db").to_string_lossy().into_owned();
        let conn = Connection::open(&path).await.expect("open");
        seed(&conn, 300).await;

        const EXECUTIONS: usize = 200;
        for sql in LOOKUPS {
            let stmt = conn.prepare(sql).await.expect("prepare");
            // Warm once: the first read may legitimately reload.
            let warm = stmt
                .query_with_params(&[text(&key_for(sql, 7))])
                .await
                .expect("warm");
            assert_eq!(single_text(&warm).as_deref(), Some("value-7"), "{sql}");

            reset_hot_path_profile();
            for j in 0..EXECUTIONS {
                let i = (j * 37) % 300;
                let rows = stmt
                    .query_with_params(&[text(&key_for(sql, i))])
                    .await
                    .expect("lookup");
                assert_eq!(single_text(&rows), Some(format!("value-{i}")), "{sql} #{i}");
            }
            let misses = stmt
                .query_with_params(&[text("no-such-key")])
                .await
                .expect("miss");
            assert!(misses.is_empty(), "{sql}: a missing key must find nothing");
            let reloads = hot_path_profile_snapshot().memdb_pager_reloads;
            assert!(
                reloads <= 2,
                "bd-xml0z: {EXECUTIONS} prepared executions of `{sql}` reloaded the \
                 MemDatabase from the pager {reloads} times; it must not scale with \
                 executions (expected ~0)"
            );
        }
        set_hot_path_profile_enabled(false);
    });
}

#[test]
fn prepared_vdbe_lookups_still_see_peer_rows_and_ddl() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("peer.db").to_string_lossy().into_owned();
        let conn = Connection::open(&path).await.expect("open");
        seed(&conn, 50).await;
        let peer = Connection::open(&path).await.expect("open peer");

        for sql in LOOKUPS {
            let stmt = conn.prepare(sql).await.expect("prepare");
            let before = stmt
                .query_with_params(&[text(&key_for(sql, 3))])
                .await
                .expect("before");
            assert_eq!(single_text(&before).as_deref(), Some("value-3"), "{sql}");
        }

        // Peer changes a row in every table and adds new ones.
        for table in ["r", "rw", "p"] {
            peer.execute(&format!(
                "UPDATE {table} SET v = 'peer-3' WHERE tenant_id = 't' AND id = 'key-0003'"
            ))
            .await
            .expect("peer update");
            peer.execute(&format!(
                "INSERT INTO {table} VALUES ('t', 'key-9999', 'party-9999', 'peer-new')"
            ))
            .await
            .expect("peer insert");
        }
        for sql in LOOKUPS {
            let stmt = conn.prepare(sql).await.expect("prepare after peer write");
            let changed = stmt
                .query_with_params(&[text(&key_for(sql, 3))])
                .await
                .expect("changed");
            assert_eq!(single_text(&changed).as_deref(), Some("peer-3"), "{sql}");
            let added = stmt
                .query_with_params(&[text(&key_for(sql, 9999))])
                .await
                .expect("added");
            assert_eq!(single_text(&added).as_deref(), Some("peer-new"), "{sql}");
        }

        // A peer's DDL is seen by the next prepare and by unprepared reads.
        let warm = conn.prepare(LOOKUPS[0]).await.expect("prepare before ddl");
        warm.query_with_params(&[text("key-0004")])
            .await
            .expect("lookup before ddl");
        peer.execute("ALTER TABLE r ADD COLUMN extra TEXT DEFAULT 'd'")
            .await
            .expect("peer ddl");
        peer.execute("UPDATE r SET v = 'after-ddl' WHERE id = 'key-0004'")
            .await
            .expect("peer update after ddl");
        let stmt = conn.prepare(LOOKUPS[0]).await.expect("prepare after ddl");
        let after_ddl = stmt
            .query_with_params(&[text("key-0004")])
            .await
            .expect("after ddl");
        assert_eq!(single_text(&after_ddl).as_deref(), Some("after-ddl"));
        let extra = conn
            .query("SELECT extra FROM r WHERE tenant_id = 't' AND id = 'key-0004'")
            .await
            .expect("new column visible");
        assert_eq!(single_text(&extra).as_deref(), Some("d"));
    });
}

#[test]
fn explain_query_plan_names_every_equality_bound_seek_column() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("eqp.db").to_string_lossy().into_owned();
        let conn = Connection::open(&path).await.expect("open");
        seed(&conn, 20).await;
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        stock.execute_batch(SCHEMA).expect("stock schema");

        let stock_detail = |sql: &str| -> Vec<String> {
            let mut stmt = stock
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .expect("stock eqp");
            stmt.query_map([], |row| row.get::<_, String>(3))
                .expect("stock rows")
                .map(Result::unwrap)
                .collect()
        };

        for sql in [
            "SELECT v FROM r WHERE tenant_id = 't' AND id = 'x'",
            "SELECT v FROM rw WHERE tenant_id = 't' AND id = 'x'",
            "SELECT v FROM p WHERE tenant_id = 't' AND party = 'x'",
            "SELECT v FROM r WHERE id = 'x' AND tenant_id = 't'",
            "SELECT v FROM r WHERE tenant_id = 't'",
        ] {
            let rows = conn
                .query(&format!("EXPLAIN QUERY PLAN {sql}"))
                .await
                .expect("eqp");
            let ours: Vec<String> = rows
                .iter()
                .map(|row| match &row.values()[3] {
                    SqliteValue::Text(detail) => detail.to_string(),
                    other => panic!("expected text detail, got {other:?}"),
                })
                .collect();
            assert_eq!(ours, stock_detail(sql), "EXPLAIN QUERY PLAN {sql}");
        }

        // A range on the second column must not be reported as an equality.
        let rows = conn
            .query("EXPLAIN QUERY PLAN SELECT v FROM r WHERE tenant_id = 't' AND id > 'x'")
            .await
            .expect("range eqp");
        for row in &rows {
            let SqliteValue::Text(detail) = &row.values()[3] else {
                panic!("expected text detail");
            };
            assert!(
                !detail.contains("id=?"),
                "a range bound must not render as `id=?`: {detail}"
            );
        }
    });
}
