//! Pager-backed WITH execution and ownership of statement-local CTE roots.
//!
//! CTE bindings are not TEMP-catalog objects. Root ownership, rather than a
//! table name or an ambient time-travel flag, decides which rows live in memory.

use super::{
    BoolCellRestoreGuard, CapturedTempTable, Connection, MemdbRowHydrationSuppression,
    Result,
};

/// Both guards are connection-local and unwind when an async operation is
/// dropped. Nested WITH clauses and delegation restore the prior policy.
pub(super) struct PagerBackedCteScope<'a> {
    _policy: BoolCellRestoreGuard<'a>,
    _hydration: MemdbRowHydrationSuppression<'a>,
}

impl Connection {
    pub(super) fn enter_pager_backed_cte_scope(
        &self,
        inherited: bool,
    ) -> Option<PagerBackedCteScope<'_>> {
        if !self.pager.is_file_backed()
            || self.time_travel_active.get()
            || !(inherited || self.schema_only_open || self.pager_backed_cte_scope.get())
        {
            return None;
        }
        Some(PagerBackedCteScope {
            _policy: BoolCellRestoreGuard::new(&self.pager_backed_cte_scope, true),
            _hydration: self.suppress_memdb_row_hydration(),
        })
    }

    /// Use the existing high, descending MemDatabase-only namespace. In
    /// particular, never probe or allocate a root in the main database merely
    /// to hold a CTE frontier. Do not add the binding to temp_table_names.
    pub(super) async fn allocate_statement_memdb_root(&self, columns: usize) -> Result<i32> {
        let root = self.allocate_schema_table_root(true, columns, false).await?;
        self.statement_memdb_roots.borrow_mut().insert(root);
        Ok(root)
    }

    pub(super) fn is_statement_memdb_root(&self, root: i32) -> bool {
        self.statement_memdb_roots.borrow().contains(&root)
    }

    /// Whether a specific root, independently of the persistent mirror's
    /// validity, is an authoritative connection-local row source.
    pub(super) fn is_connection_local_row_root(&self, root: i32) -> bool {
        if self.is_statement_memdb_root(root) {
            return true;
        }
        let temp_names = self.temp_table_names.borrow();
        if temp_names.is_empty() {
            return false;
        }
        self.schema.borrow().iter().any(|table| {
            table.root_page == root
                && temp_names.contains(&table.name.to_ascii_lowercase())
        })
    }

    /// A full schema refresh must preserve materialized CTEs, just as it
    /// preserves TEMP tables, but must not give a CTE TEMP-catalog ownership.
    /// Capture reverse binding order: restoration prepends each CTE, restoring
    /// nested and sibling shadowing in the same order as the live schema.
    pub(super) fn snapshot_statement_memdb_roots(&self) -> Vec<CapturedTempTable> {
        let roots = self.statement_memdb_roots.borrow();
        if roots.is_empty() {
            return Vec::new();
        }
        let schema = self.schema.borrow();
        let db = self.db.borrow();
        schema
            .iter()
            .rev()
            .filter(|table| roots.contains(&table.root_page))
            .map(|table| {
                let rows = db.get_table(table.root_page).map_or_else(Vec::new, |rows| {
                    rows.iter_rows()
                        .map(|(rowid, values)| (rowid, values.to_vec()))
                        .collect()
                });
                (table.clone(), rows)
            })
            .collect()
    }
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use super::Connection;
    use crate::connection::Row;
    use fsqlite_types::value::SqliteValue;

    #[test]
    fn bounded_cte_delegation_preserves_child_policy_and_zero_hydration() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let main_path = dir.path().join("main.db");
            let aux_path = dir.path().join("aux.db");
            {
                let main = rusqlite::Connection::open(&main_path).unwrap();
                main.execute_batch("CREATE TABLE anchor(x INTEGER)").unwrap();
                let mut aux = rusqlite::Connection::open(&aux_path).unwrap();
                aux.execute_batch("CREATE TABLE bulk(id INTEGER PRIMARY KEY, body TEXT); CREATE TABLE hot(id TEXT PRIMARY KEY, v INTEGER); INSERT INTO hot VALUES ('k042',42)").unwrap();
                let tx = aux.transaction().unwrap();
                {
                    let body = "x".repeat(2048);
                    let mut stmt = tx.prepare("INSERT INTO bulk VALUES (?1,?2)").unwrap();
                    for id in 0..4096 {
                        stmt.execute(rusqlite::params![id, &body]).unwrap();
                    }
                }
                tx.commit().unwrap();
            }
            let conn = Connection::open_existing_schema_only(main_path.to_string_lossy().as_ref()).await.unwrap();
            conn.execute(&format!("ATTACH '{}' AS aux", aux_path.to_string_lossy().replace('\'', "''"))).await.unwrap();
            let before = conn.with_attached_connection("aux", |child| {
                Ok((child.memdb_row_hydration_count(), child.schema_only_open,
                    child.pager_backed_cte_scope.get(), child.memdb_row_hydration_suppressed.get()))
            }).unwrap();
            let rows: Vec<Row> = conn.query("WITH c(k) AS (VALUES ('k042')) SELECT c.k,t.v FROM c JOIN aux.hot t ON t.id=c.k").await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values(), &[SqliteValue::Text("k042".into()), SqliteValue::Integer(42)]);
            assert!(conn.query("WITH c(k) AS (VALUES ('k042')) SELECT abs(-9223372036854775808) FROM c JOIN aux.hot t ON t.id=c.k").await.is_err());
            let after = conn.with_attached_connection("aux", |child| {
                Ok((child.memdb_row_hydration_count(), child.schema_only_open,
                    child.pager_backed_cte_scope.get(), child.memdb_row_hydration_suppressed.get()))
            }).unwrap();
            assert_eq!(after, before, "delegation must neither hydrate the child's bulk nor leak bounded policy");
            assert!(conn.statement_memdb_roots.borrow().is_empty());
            assert!(!conn.pager_backed_cte_scope.get());
            assert_eq!(conn.memdb_row_hydration_suppressed.get(), 0);
            conn.close_without_checkpoint().await.unwrap();
        });
    }
}
