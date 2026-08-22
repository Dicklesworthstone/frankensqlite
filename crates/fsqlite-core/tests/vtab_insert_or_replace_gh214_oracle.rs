//! GH #214 (bd-gh-vtab-insert-or-replace-m7npk): `INSERT OR REPLACE` into a live
//! virtual table (R*Tree) errored with "UPSERT and conflict clauses are not
//! supported for live virtual-table INSERT" instead of performing SQLite's
//! REPLACE (delete the row conflicting on the rowid, then insert).
//!
//! SQLite oracle:
//!   CREATE VIRTUAL TABLE rt USING rtree(id, minX, maxX);
//!   INSERT INTO rt VALUES(1, 0.0, 1.0);
//!   INSERT OR REPLACE INTO rt VALUES(1, 5.0, 6.0);
//!   SELECT * FROM rt;   -->  1|5.0|6.0   (the id=1 row now has the new bbox)
//!
//! rusqlite's bundled SQLite is not guaranteed to be compiled with the rtree
//! module (`CREATE VIRTUAL TABLE ... USING rtree` may fail), so — like the
//! sibling GH #208 keeper — this test asserts against hardcoded values that
//! match SQLite's documented `INSERT OR REPLACE` semantics rather than tying to
//! a live rusqlite oracle.
//!
//! Requires `--features ext-rtree`.

#![cfg(feature = "ext-rtree")]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// All `(id, minX, maxX)` rows, ordered by id.
async fn contents(conn: &Connection) -> Vec<Vec<SqliteValue>> {
    conn.query("SELECT id, minX, maxX FROM rt ORDER BY id")
        .await
        .expect("SELECT from rt")
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn row(id: i64, min_x: f64, max_x: f64) -> Vec<SqliteValue> {
    vec![
        SqliteValue::Integer(id),
        SqliteValue::Float(min_x),
        SqliteValue::Float(max_x),
    ]
}

/// The exact repro: `INSERT OR REPLACE` of a conflicting id replaces the row.
#[test]
fn insert_or_replace_replaces_conflicting_rowid_gh214() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE rt USING rtree(id, minX, maxX)")
            .await
            .unwrap();
        conn.execute("INSERT INTO rt VALUES(1, 0.0, 1.0)")
            .await
            .unwrap();

        // Before the fix this errored NotImplemented; it must REPLACE instead.
        conn.execute("INSERT OR REPLACE INTO rt VALUES(1, 5.0, 6.0)")
            .await
            .expect("INSERT OR REPLACE must succeed for a live rtree vtab");

        // Exactly one row, carrying the new bbox.
        assert_eq!(contents(&conn).await, vec![row(1, 5.0, 6.0)]);
    });
}

/// `INSERT OR REPLACE` of a non-conflicting id is a plain insert (both rows).
#[test]
fn insert_or_replace_without_conflict_just_inserts_gh214() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE rt USING rtree(id, minX, maxX)")
            .await
            .unwrap();
        conn.execute("INSERT INTO rt VALUES(1, 0.0, 1.0)")
            .await
            .unwrap();

        conn.execute("INSERT OR REPLACE INTO rt VALUES(2, 5.0, 6.0)")
            .await
            .unwrap();

        assert_eq!(
            contents(&conn).await,
            vec![row(1, 0.0, 1.0), row(2, 5.0, 6.0)]
        );
    });
}

/// A plain `INSERT` of a duplicate id STILL errors — REPLACE must not change
/// the ordinary (no conflict clause) insert behavior.
#[test]
fn plain_insert_of_duplicate_id_still_errors_gh214() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE rt USING rtree(id, minX, maxX)")
            .await
            .unwrap();
        conn.execute("INSERT INTO rt VALUES(1, 0.0, 1.0)")
            .await
            .unwrap();

        let err = conn
            .execute("INSERT INTO rt VALUES(1, 5.0, 6.0)")
            .await
            .expect_err("plain INSERT of a duplicate rowid must still fail");
        assert!(
            err.to_string().contains("PRIMARY KEY"),
            "expected a PRIMARY KEY violation, got: {err}"
        );

        // The original row is untouched by the failed insert.
        assert_eq!(contents(&conn).await, vec![row(1, 0.0, 1.0)]);
    });
}

/// `INSERT OR REPLACE` with an auto/absent rowid (NULL id) has no conflict to
/// resolve, so it simply inserts a new row alongside the existing ones.
#[test]
fn insert_or_replace_with_auto_rowid_inserts_new_row_gh214() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE rt USING rtree(id, minX, maxX)")
            .await
            .unwrap();
        conn.execute("INSERT INTO rt VALUES(1, 0.0, 1.0)")
            .await
            .unwrap();

        // NULL id => auto-assigned rowid; this must NOT delete/replace id=1.
        conn.execute("INSERT OR REPLACE INTO rt VALUES(NULL, 7.0, 8.0)")
            .await
            .unwrap();

        let rows = contents(&conn).await;
        assert_eq!(
            rows.len(),
            2,
            "auto-rowid REPLACE must add a new row: {rows:?}"
        );
        // The pre-existing id=1 row is unchanged.
        assert_eq!(rows[0], row(1, 0.0, 1.0));
        // The new row carries the inserted bbox (its auto id is >= 2).
        assert_eq!(rows[1][1], SqliteValue::Float(7.0));
        assert_eq!(rows[1][2], SqliteValue::Float(8.0));
    });
}
