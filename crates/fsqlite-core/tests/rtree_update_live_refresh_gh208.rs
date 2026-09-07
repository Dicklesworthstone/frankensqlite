//! GH #208 (bd-gh-live-vtab-update-routing): an UPDATE on a live R*Tree virtual
//! table must be visible on the same connection.
//!
//! Before the fix the UPDATE persisted to the backing b-tree (a reopen showed
//! the new bounds) but the in-memory live RtreeVirtualTable instance serving
//! same-connection scans was never refreshed, so `SELECT` returned the stale
//! pre-update bounds while `changes()` still reported 1. The UPDATE is now
//! routed through the module with the old rowid and recomputed row image.
//!
//! Requires `--features ext-rtree`.

#![cfg(feature = "ext-rtree")]

use std::sync::atomic::{AtomicUsize, Ordering};

use fsqlite_core::connection::Connection;
use fsqlite_error::{FrankenError, Result};
use fsqlite_ext_rtree::RtreeVirtualTable;
use fsqlite_func::vtab::{IndexInfo, VirtualTable, module_factory_from};
use fsqlite_types::cx::Cx;
use fsqlite_types::value::SqliteValue;

/// A real R-tree extension whose contract permits insertion and in-place
/// mutation, but forbids deletion. SQL UPDATE must preserve that distinction
/// so modules can validate ownership against the old row before changing it.
struct UpdateOnlyRtree(RtreeVirtualTable);

static MODULE_UPDATE_CALLBACKS: AtomicUsize = AtomicUsize::new(0);
static MODULE_RESTORE_CALLBACKS: AtomicUsize = AtomicUsize::new(0);

impl VirtualTable for UpdateOnlyRtree {
    type Cursor = <RtreeVirtualTable as VirtualTable>::Cursor;

    fn connect(cx: &Cx, args: &[&str]) -> Result<Self> {
        RtreeVirtualTable::connect(cx, args).map(Self)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        self.0.best_index(info)
    }

    fn open(&self) -> Result<Self::Cursor> {
        self.0.open()
    }

    fn update(&mut self, cx: &Cx, args: &[SqliteValue]) -> Result<Option<i64>> {
        if args.len() == 1 {
            return Err(FrankenError::function_error(
                "update-only module forbids DELETE callbacks",
            ));
        }
        let result = self.0.update(cx, args);
        if result.is_ok() && args.first().is_some_and(|value| !value.is_null()) {
            MODULE_UPDATE_CALLBACKS.fetch_add(1, Ordering::Release);
        }
        result
    }

    fn restore_materialized_rows(
        &mut self,
        cx: &Cx,
        rows: &[(i64, Vec<SqliteValue>)],
    ) -> Result<bool> {
        let restored = self.0.restore_materialized_rows(cx, rows)?;
        if restored {
            MODULE_RESTORE_CALLBACKS.fetch_add(1, Ordering::Release);
        }
        Ok(restored)
    }

    fn begin(&mut self, cx: &Cx) -> Result<()> {
        self.0.begin(cx)
    }

    fn sync_txn(&mut self, cx: &Cx) -> Result<()> {
        self.0.sync_txn(cx)
    }

    fn commit(&mut self, cx: &Cx) -> Result<()> {
        self.0.commit(cx)
    }

    fn rollback(&mut self, cx: &Cx) -> Result<()> {
        self.0.rollback(cx)
    }

    fn savepoint(&mut self, cx: &Cx, level: i32) -> Result<()> {
        self.0.savepoint(cx, level)
    }

    fn release(&mut self, cx: &Cx, level: i32) -> Result<()> {
        self.0.release(cx, level)
    }

    fn rollback_to(&mut self, cx: &Cx, level: i32) -> Result<()> {
        self.0.rollback_to(cx, level)
    }
}

async fn row(conn: &Connection, sql: &str) -> Vec<SqliteValue> {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {e}"));
    rows.first()
        .unwrap_or_else(|| panic!("query `{sql}`: no rows"))
        .values()
        .to_vec()
}

#[test]
fn rtree_update_is_visible_on_the_same_connection() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE demo USING rtree(id, minx, maxx, miny, maxy)")
            .await
            .unwrap();
        conn.execute("INSERT INTO demo VALUES (1, 0.0, 1.0, 0.0, 1.0)")
            .await
            .unwrap();

        // Full UPDATE of every bound must be visible immediately (was stale).
        conn.execute("UPDATE demo SET minx=5.0, maxx=6.0, miny=5.0, maxy=6.0 WHERE id=1")
            .await
            .unwrap();
        assert_eq!(
            row(
                &conn,
                "SELECT id, minx, maxx, miny, maxy FROM demo WHERE id=1"
            )
            .await,
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(5.0),
                SqliteValue::Float(6.0),
                SqliteValue::Float(5.0),
                SqliteValue::Float(6.0),
            ],
        );

        // Partial UPDATE — only maxx changes; the rest must be preserved.
        conn.execute("UPDATE demo SET maxx=20.0 WHERE id=1")
            .await
            .unwrap();
        assert_eq!(
            row(
                &conn,
                "SELECT id, minx, maxx, miny, maxy FROM demo WHERE id=1"
            )
            .await,
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(5.0),
                SqliteValue::Float(20.0),
                SqliteValue::Float(5.0),
                SqliteValue::Float(6.0),
            ],
        );

        // The row count is unchanged (update, not insert/delete).
        assert_eq!(
            row(&conn, "SELECT count(*) FROM demo").await,
            vec![SqliteValue::Integer(1)],
        );
    });
}

#[test]
fn sql_update_preserves_old_row_for_the_module_callback() {
    asupersync::test_utils::run_test(|| async {
        MODULE_UPDATE_CALLBACKS.store(0, Ordering::Release);
        MODULE_RESTORE_CALLBACKS.store(0, Ordering::Release);
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("update-callback.db");
        let path = database.to_str().unwrap();
        let conn = Connection::open(path).await.unwrap();
        conn.register_module(
            "update_only",
            Box::new(module_factory_from::<UpdateOnlyRtree>()),
        );
        conn.execute("CREATE VIRTUAL TABLE demo USING update_only(id, minx, maxx)")
            .await
            .unwrap();
        conn.execute("INSERT INTO demo VALUES (1, 0.0, 1.0)")
            .await
            .unwrap();
        assert_eq!(
            row(&conn, "SELECT id, minx, maxx FROM demo").await,
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Float(0.0),
                SqliteValue::Float(1.0)
            ]
        );

        conn.execute("UPDATE demo SET minx=2.0, maxx=3.0 WHERE id=1")
            .await
            .expect(
                "SQL UPDATE must invoke the module UPDATE with its old row intact, never DELETE",
            );
        assert_eq!(
            MODULE_UPDATE_CALLBACKS.load(Ordering::Acquire),
            1,
            "SQL UPDATE must reach the live module, including its old-row validation"
        );
        assert_eq!(
            row(&conn, "SELECT changes()").await,
            vec![SqliteValue::Integer(1)]
        );
        let committed = vec![
            SqliteValue::Integer(1),
            SqliteValue::Float(2.0),
            SqliteValue::Float(3.0),
        ];
        assert_eq!(
            row(&conn, "SELECT id, minx, maxx FROM demo").await,
            committed
        );

        conn.execute("BEGIN").await.unwrap();
        conn.execute("SAVEPOINT before_update").await.unwrap();
        conn.execute("UPDATE demo SET maxx=9.0 WHERE id=1")
            .await
            .unwrap();
        assert_eq!(
            row(&conn, "SELECT maxx FROM demo").await,
            vec![SqliteValue::Float(9.0)]
        );
        conn.execute("ROLLBACK TO before_update").await.unwrap();
        assert_eq!(
            row(&conn, "SELECT id, minx, maxx FROM demo").await,
            committed
        );
        conn.execute("RELEASE before_update").await.unwrap();
        conn.execute("UPDATE demo SET maxx=7.0 WHERE id=1")
            .await
            .unwrap();
        conn.execute("ROLLBACK").await.unwrap();
        assert_eq!(MODULE_UPDATE_CALLBACKS.load(Ordering::Acquire), 3);
        assert_eq!(
            row(&conn, "SELECT id, minx, maxx FROM demo").await,
            committed
        );

        let error = conn
            .execute("DELETE FROM demo WHERE id=1")
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("update-only module forbids DELETE callbacks"),
            "{error}"
        );
        assert_eq!(
            row(&conn, "SELECT id, minx, maxx FROM demo").await,
            committed
        );
        conn.close().await.unwrap();

        MODULE_RESTORE_CALLBACKS.store(0, Ordering::Release);
        let reopened = Connection::open(path).await.unwrap();
        reopened.register_module(
            "update_only",
            Box::new(module_factory_from::<UpdateOnlyRtree>()),
        );
        assert_eq!(
            row(&reopened, "SELECT id, minx, maxx FROM demo").await,
            committed
        );
        assert!(MODULE_RESTORE_CALLBACKS.load(Ordering::Acquire) > 0);
        reopened
            .execute("UPDATE demo SET maxx=4.0 WHERE id=1")
            .await
            .unwrap();
        assert_eq!(MODULE_UPDATE_CALLBACKS.load(Ordering::Acquire), 4);
        assert_eq!(
            row(&reopened, "SELECT maxx FROM demo").await,
            vec![SqliteValue::Float(4.0)]
        );
        reopened.close().await.unwrap();
    });
}

#[test]
fn multirow_update_failure_restores_module_and_storage() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("update-atomicity.db");
        let path = database.to_str().unwrap();
        let conn = Connection::open(path).await.unwrap();
        conn.execute("CREATE TABLE markers(value TEXT)")
            .await
            .unwrap();
        conn.execute("CREATE VIRTUAL TABLE demo USING rtree(id, minx, maxx)")
            .await
            .unwrap();
        conn.execute("INSERT INTO demo VALUES (1, 0.0, 10.0), (2, 0.0, 1.0)")
            .await
            .unwrap();
        let before: Vec<Vec<SqliteValue>> = conn
            .query("SELECT id, minx, maxx FROM demo ORDER BY id")
            .await
            .unwrap()
            .iter()
            .map(|row| row.values().to_vec())
            .collect();
        assert_eq!(before.len(), 2);
        conn.execute("BEGIN").await.unwrap();
        conn.execute("INSERT INTO markers VALUES ('preserve')")
            .await
            .unwrap();

        // The first row permits this bound; the second rejects it. The failed
        // statement must roll back module changes without losing earlier DML.
        let error = conn.execute("UPDATE demo SET minx=2.0").await.unwrap_err();
        assert!(
            matches!(error, FrankenError::RtreeConstraint { .. }),
            "{error}"
        );
        assert_eq!(
            conn.query("SELECT id, minx, maxx FROM demo ORDER BY id")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            before
        );
        assert_eq!(
            row(&conn, "SELECT value FROM markers").await,
            vec![SqliteValue::Text("preserve".into())]
        );
        conn.execute("COMMIT").await.unwrap();
        conn.close().await.unwrap();

        let reopened = Connection::open(path).await.unwrap();
        assert_eq!(
            reopened
                .query("SELECT id, minx, maxx FROM demo ORDER BY id")
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            before
        );
        assert_eq!(
            row(&reopened, "SELECT value FROM markers").await,
            vec![SqliteValue::Text("preserve".into())]
        );
        reopened.close().await.unwrap();
    });
}
