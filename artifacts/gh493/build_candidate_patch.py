#!/usr/bin/env python3
"""Emit the GH493 integration diff against one exact, reviewed source blob.

This is a development-time patch generator, NOT a build script. It never edits
Rust sources, changes Cargo configuration, executes SQL, or publishes commits.
The candidate is not active merely because this file is in the repository.

Usage, from a clean checkout:
  python3 artifacts/gh493/build_candidate_patch.py > /tmp/gh493.patch
  git apply --check /tmp/gh493.patch
  git apply /tmp/gh493.patch

The SHA check deliberately refuses peer/source drift. Review and regenerate the
candidate against a changed source instead of overriding the pin.
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path

SOURCE = "crates/fsqlite-core/src/connection.rs"
BASE_BLOB = "74e4bb5ee503fd40e0104dbbaa5dadabdef90777"


@dataclass(frozen=True)
class Edit:
    name: str
    old: str
    new: str
    scope: str | None = None
    count: int = 1


EDITS: list[Edit] = []


def edit(name: str, old: str, new: str, scope: str | None = None, count: int = 1) -> None:
    EDITS.append(Edit(name, old, new, scope, count))


edit("declare CTE storage module", "mod conformal_retry;\n", "mod conformal_retry;\nmod cte_storage;\n")
edit("connection-local ownership and policy", "    next_temp_root_page: Cell<i32>,\n", """    next_temp_root_page: Cell<i32>,
    /// Statement-owned roots, separate from TEMP-catalog name ownership.
    statement_memdb_roots: RefCell<HashSet<i32>>,
    /// Bounded WITH policy, also inherited by an attached child for one call.
    pager_backed_cte_scope: Cell<bool>,
""")
edit("initialize both open families", "            next_temp_root_page: Cell::new(TEMP_MEMDB_ROOT_START),\n", """            next_temp_root_page: Cell::new(TEMP_MEMDB_ROOT_START),
            statement_memdb_roots: RefCell::new(HashSet::new()),
            pager_backed_cte_scope: Cell::new(false),
""", count=2)
edit("keep delegated prepared reads bounded", "        self.schema_only_open\n", "        self.schema_only_open || self.pager_backed_cte_scope.get()\n", "defer_memdb_row_hydration")
edit("inherit policy only for the delegated call", "        f(conn.as_ref()).await\n", """        let _cte_scope = self.pager_backed_cte_scope.get().then(|| {
            conn.enter_pager_backed_cte_scope(true)
        });
        f(conn.as_ref()).await
""", "with_attached_connection_async")

# Scope starts before attached-target enrollment, and lasts through consumer
# execution and cleanup, not merely the CTE-body materialization call.
for method in ["execute_with_ctes", "execute_delete_with_ctes", "execute_update_with_ctes", "execute_insert_with_ctes", "materialize_with_clause_snapshot"]:
    edit("bounded policy: " + method, "    ) -> Result<Vec<" + ("MaterializedTempTable" if method == "materialize_with_clause_snapshot" else "Row") + ">> {\n", "    ) -> Result<Vec<" + ("MaterializedTempTable" if method == "materialize_with_clause_snapshot" else "Row") + ">> {\n        let _cte_scope = self.enter_pager_backed_cte_scope(false);\n", method)

edit("explain bounded refresh", """        // Recursive fallback joins read their working table from MemDatabase.
        // Hydrate persistent rows before installing any CTE roots so a mixed
        // persistent/working-table join uses one complete execution image.
""", """        // Settle pending writes and refresh schema in the active snapshot.
        // The bounded WITH scope suppresses persistent row hydration; CTE
        // roots are independently authoritative, persistent roots use pager IO.
""", "materialize_with_clause")
edit("no main-file root probe for CTEs", "        self.reserve_clean_memdb_root_pages(ctes.len()).await?;\n", "", "materialize_with_clause")
edit("ordinary CTE root", "                let root_page = self.db.borrow_mut().create_table(num_columns);\n", "                let root_page = self.allocate_statement_memdb_root(num_columns).await?;\n", "materialize_with_clause")
edit("recursive frontier root", "        let root_page = self.db.borrow_mut().create_table(num_columns);\n", "        let root_page = self.allocate_statement_memdb_root(num_columns).await?;\n", "materialize_recursive_cte")
edit("imported roots have a cleanup guard while installing", """        self.reserve_clean_memdb_root_pages(temp_tables.len())
            .await?;
        let mut installed = Vec::with_capacity(temp_tables.len());
""", """        let mut installed = MaterializedTablesCleanupGuard::new(self);
""", "install_materialized_temp_tables")
edit("imported CTE root", "            let root_page = self.db.borrow_mut().create_table(temp_table.columns.len());\n", """            let root_page = self
                .allocate_statement_memdb_root(temp_table.columns.len())
                .await?;
""", "install_materialized_temp_tables")
edit("record imported root ownership", "            installed.push((temp_table.name.clone(), root_page));\n", "            installed.tables.push((temp_table.name.clone(), root_page));\n", "install_materialized_temp_tables")
edit("transfer installed cleanup ownership", "        Ok(installed)\n", "        Ok(std::mem::take(&mut installed.tables))\n", "install_materialized_temp_tables")
edit("remove exact root ownership even after a refresh", "        for (name, root_page) in temp_tables {\n", """        for (name, root_page) in temp_tables {
            self.statement_memdb_roots.borrow_mut().remove(root_page);
""", "cleanup_cte_tables")

edit("route both CTE and TEMP roots explicitly", """        let temp_names = self.temp_table_names.borrow();
        if temp_names.is_empty() {
            return HashSet::new();
        }
        let schema = self.schema.borrow();
        let mut roots = HashSet::new();
""", """        let mut roots = self.statement_memdb_roots.borrow().clone();
        let temp_names = self.temp_table_names.borrow();
        if temp_names.is_empty() {
            return roots;
        }
        let schema = self.schema.borrow();
""", "temp_storage_roots")

edit("mixed interpreted reads do not fake time travel", "        let _time_travel_guard = BoolCellRestoreGuard::new(&self.time_travel_active, true);\n", """        // A CTE frontier is not a complete persistent snapshot. In bounded
        // mode choose memory/pager per root instead of making every MemDB
        // placeholder authoritative via the time-travel override.
        let _time_travel_guard = (!self.pager_backed_cte_scope.get())
            .then(|| BoolCellRestoreGuard::new(&self.time_travel_active, true));
""", "execute_select_via_memdb_fallback")
edit("scan local roots even with an unloaded persistent mirror", """        if !self.join_mem_scan_safe() {
            return None;
        }
        let binding_name = src.local_table_binding()?;
        let (table_schema, rowid_alias_column_index) =
            self.local_join_binding_table(binding_name)?;
        let root_page = table_schema.root_page;
""", """        let binding_name = src.local_table_binding()?;
        let (table_schema, rowid_alias_column_index) =
            self.local_join_binding_table(binding_name)?;
        let root_page = table_schema.root_page;
        if !self.is_connection_local_row_root(root_page) && !self.join_mem_scan_safe() {
            return None;
        }
""", "try_scan_join_source_from_memdb")
edit("CTE outputs are not a shadowed base table's rowid alias", "        let rowid_alias_column_index = self.rowid_alias_columns.borrow().get(&name_lc).copied();\n", """        let rowid_alias_column_index = if self.is_statement_memdb_root(table.root_page) {
            None
        } else {
            self.rowid_alias_columns.borrow().get(&name_lc).copied()
        };
""", "local_join_binding_table")

# Preserve CTE tables across legitimate schema/mirror refreshes rather than
# falsely clearing the dirty flag or forbidding correctness-required refreshes.
edit("capture CTE roots independently of TEMP names", """        if self.temp_table_names.borrow().is_empty() {
            return Vec::new();
        }
""", """        let cte_tables = self.snapshot_statement_memdb_roots();
        if self.temp_table_names.borrow().is_empty() {
            return cte_tables;
        }
""", "snapshot_temp_tables")
edit("capture actual TEMP table under a same-name CTE", "                let table = schema\n                    .iter()\n                    .find(|t| t.name.eq_ignore_ascii_case(name_lc))?;\n", """                let table = schema.iter().find(|table| {
                    table.name.eq_ignore_ascii_case(name_lc)
                        && !self.is_statement_memdb_root(table.root_page)
                })?;
""", "snapshot_temp_tables")
edit("restore TEMP before prepending CTE bindings", "\n            .collect()\n", "\n            .chain(cte_tables)\n            .collect()\n", "snapshot_temp_tables")
edit("restore CTE rows without taking catalog ownership", "        for (temp_schema, rows) in captured {\n", """        for (temp_schema, rows) in captured {
            if self.is_statement_memdb_root(temp_schema.root_page) {
                let ordinary_next_root = new_db.next_root_page();
                new_db.create_table_at(temp_schema.root_page, temp_schema.columns.len());
                new_db.set_next_root_page(ordinary_next_root);
                for (rowid, values) in rows {
                    new_db.upsert_row(temp_schema.root_page, rowid, values);
                }
                // Do not park/remove a same-name MAIN relation or claim its
                // TEMP marker. The CTE shadows it only for this statement.
                new_schema.insert(0, temp_schema);
                continue;
            }
""", "restore_temp_tables_into")

# This branch has already proved that the stripped SELECT references NO local
# CTE. Re-enter the child's ordinary SELECT dispatcher, which owns its snapshot
# and flushes retained writes. Do not install irrelevant CTEs or load its file.
edit("attached-only SELECT uses its native dispatcher", """                // T1.5: the stripped query references ONLY the attached
""", """                if self.pager_backed_cte_scope.get() {
                    strip_attached_schema_from_select(&mut stripped, &target_schema);
                    return self
                        .with_attached_connection_async(&target_schema, async move |conn| {
                            conn.execute_statement(&Statement::Select(stripped), params).await
                        })
                        .await;
                }
                // T1.5: the stripped query references ONLY the attached
""", "execute_with_ctes")


# The sqlite_schema materializer also used time_travel_active as a proxy for
# its private rows. Inside a bounded WITH, catalog rows need explicit roots
# too, or a catalog/base-table join would read an empty persistent mirror.
edit("catalog rows in bounded WITH have explicit local roots", "            let root_page = self.db.borrow_mut().create_table(virtual_columns.len());\n", """            let root_page = if self.pager_backed_cte_scope.get() {
                self.allocate_statement_memdb_root(virtual_columns.len()).await?
            } else {
                self.db.borrow_mut().create_table(virtual_columns.len())
            };
""", "execute_with_materialized_sqlite_schema")
edit("catalog/base joins preserve per-root storage", "                let _time_travel_guard = BoolCellRestoreGuard::new(&self.time_travel_active, true);\n", """                let _time_travel_guard = (!self.pager_backed_cte_scope.get())
                    .then(|| BoolCellRestoreGuard::new(&self.time_travel_active, true));
""", "execute_with_materialized_sqlite_schema")


def git_blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode("ascii") + b"\0" + data).hexdigest()


def apply_edit(source: str, rule: Edit) -> str:
    lo, hi = 0, len(source)
    if rule.scope:
        declarations = list(re.finditer(
            r"(?m)^    (?:pub(?:\([^\n]*\))? )?(?:async )?fn "
            + re.escape(rule.scope) + r"\b", source))
        if len(declarations) != 1:
            raise ValueError(f"{rule.name}: expected one method {rule.scope}, found {len(declarations)}")
        lo = declarations[0].start()
        following = re.search(r"(?m)^    (?:pub(?:\([^\n]*\))? )?(?:async )?fn \w+", source[declarations[0].end():])
        if following:
            hi = declarations[0].end() + following.start()
    window = source[lo:hi]
    found = window.count(rule.old)
    if found != rule.count:
        raise ValueError(f"{rule.name}: expected {rule.count} exact anchor(s), found {found}")
    return source[:lo] + window.replace(rule.old, rule.new) + source[hi:]


def build_patch(root: Path, assets: Path) -> str:
    source_path = root / SOURCE
    raw = source_path.read_bytes()
    actual = git_blob_sha(raw)
    if actual != BASE_BLOB:
        raise ValueError(f"source drift: {SOURCE}: expected {BASE_BLOB}, got {actual}; review, do not bypass")
    before = raw.decode("utf-8")
    after = before
    for rule in EDITS:
        after = apply_edit(after, rule)
    diff = list(difflib.unified_diff(before.splitlines(keepends=True), after.splitlines(keepends=True),
                                     fromfile="a/" + SOURCE, tofile="b/" + SOURCE))
    for asset, destination in [
        ("cte_storage.rs", "crates/fsqlite-core/src/connection/cte_storage.rs"),
        ("gh493_pager_contract.rs", "crates/fsqlite-core/tests/gh493_pager_contract.rs"),
    ]:
        if (root / destination).exists():
            raise ValueError(f"refusing to overwrite existing integration destination: {destination}")
        contents = (assets / asset).read_text(encoding="utf-8")
        diff.extend(difflib.unified_diff([], contents.splitlines(keepends=True),
                                         fromfile="/dev/null", tofile="b/" + destination))
    return "".join(diff)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        patch = build_patch(args.repo, Path(__file__).resolve().parent)
    except (OSError, UnicodeError, ValueError) as error:
        print(f"GH493 candidate refused: {error}", file=sys.stderr)
        return 1
    sys.stdout.write(patch)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
